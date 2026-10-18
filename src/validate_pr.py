# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
PR validation engine for subset-scoped table governance.

Phase 1 capabilities (metadata-only, no PPE connection):
    1. Parse PR description for affected tables and validation scope.
    2. Validate declared tables exist in table_registry.yml.
    3. Filter to subset scope.
    4. Validate metadata consistency across all files.
    5. Expand lineage per scope mode.
    6. Verify all expanded tables have complete metadata.

Usage:
    python validate_pr.py \
        --metadata_path <path> \
        --pr_description_env_var SYSTEM_PULLREQUEST_DESCRIPTION \
        --validation_scope affected_only

Exit codes:
    0 = validation passed (or no-op)
    1 = validation failed
"""

import argparse
import os
import re
import sys
from typing import Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    DEFAULT_SCOPE_MODE,
    LOG_PREFIX_ERROR,
    LOG_PREFIX_INFO,
    LOG_PREFIX_SKIP,
    LOG_PREFIX_START,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
    METADATA_PATH,
    PR_LAKEHOUSE_ID_KEY,
    PR_LAKEHOUSE_WORKSPACE_ID_KEY,
    PR_NOTEBOOK_WORKSPACE_ID_KEY,
    VALID_SCOPE_MODES,
)
from metadata_parser import (
    MetadataLoadError,
    MetadataValidationError,
    SubsetScope,
    TableLineage,
    TableRegistry,
    collect_contract_warnings,
    validate_metadata_consistency,
    validate_metadata_shapes,
)

# ===========================================================================
# PR description parsing
# ===========================================================================

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


def _normalize_description_text(description: str) -> str:
    """Normalize line endings and URL-encoded newline tokens from ADO payloads."""
    if not description:
        return ""
    normalized = description
    normalized = normalized.replace("%0D%0A", "\n")
    normalized = normalized.replace("%0A", "\n")
    normalized = normalized.replace("%0D", "\n")
    normalized = normalized.replace("\r\n", "\n")
    normalized = normalized.replace("\r", "\n")
    return normalized


def _extract_pr_key_value(lines: List[str], key_name: str) -> str:
    """Extract `key: value` style items from markdown list lines in PR descriptions."""
    key_lower = key_name.strip().lower()
    for line in lines:
        stripped = line.strip()
        match = re.match(r"^[-*]\s*([A-Za-z0-9_]+)\s*:\s*(.+)$", stripped)
        if not match:
            continue
        key = match.group(1).strip().lower()
        value = match.group(2).strip()
        if key == key_lower and value:
            return value
    return ""


def parse_pr_runtime_overrides(description: str) -> Dict[str, str]:
    """
    Parse optional runtime override fields from PR description.

    Supported keys:
      - notebook_workspace_id
      - lakehouse_workspace_id
      - lakehouse_id

    Returns:
      Dict containing only valid UUID values for supported keys.
    """
    overrides: Dict[str, str] = {}
    if not description:
        return overrides

    normalized = _normalize_description_text(description)
    lines = normalized.strip().split("\n")

    raw_notebook_ws = _extract_pr_key_value(lines, PR_NOTEBOOK_WORKSPACE_ID_KEY)
    raw_lakehouse_ws = _extract_pr_key_value(lines, PR_LAKEHOUSE_WORKSPACE_ID_KEY)
    raw_lakehouse_id = _extract_pr_key_value(lines, PR_LAKEHOUSE_ID_KEY)

    if raw_notebook_ws and _UUID_RE.match(raw_notebook_ws):
        overrides[PR_NOTEBOOK_WORKSPACE_ID_KEY] = raw_notebook_ws
    if raw_lakehouse_ws and _UUID_RE.match(raw_lakehouse_ws):
        overrides[PR_LAKEHOUSE_WORKSPACE_ID_KEY] = raw_lakehouse_ws
    if raw_lakehouse_id and _UUID_RE.match(raw_lakehouse_id):
        overrides[PR_LAKEHOUSE_ID_KEY] = raw_lakehouse_id

    return overrides

def parse_pr_description(
    description: str,
) -> Tuple[List[str], str]:
    """
    Parse PR description to extract affected tables and validation scope mode.

    Expected format:
        ## Affected Tables (Subset Only)
        - table_a
        - table_b

        ## Validation Scope Mode
        Choose one:
        - [x] affected_only
        - [ ] upstream
        ...

    Returns:
        Tuple of (affected_tables, scope_mode).
    """
    affected_tables: List[str] = []
    scope_mode: str = DEFAULT_SCOPE_MODE

    if not description:
        return affected_tables, scope_mode

    # Normalize line endings — Azure DevOps may pass PR descriptions with
    # %0A / %0D%0A (URL-encoded), literal \n, \r\n, or actual newlines.
    normalized = _normalize_description_text(description)

    lines = normalized.strip().split("\n")

    # --- Extract affected tables ---
    in_tables_section = False
    for line in lines:
        stripped = line.strip()

        # Detect start of affected tables section
        if stripped.lower().startswith("## affected tables"):
            in_tables_section = True
            continue

        # Detect start of another section (end of tables section)
        if stripped.startswith("##") and in_tables_section:
            in_tables_section = False

        if in_tables_section:
            # Match lines starting with "- " (markdown list items)
            match = re.match(r"^[-*]\s+(.+)$", stripped)
            if match:
                table_name = match.group(1).strip()
                if table_name:
                    affected_tables.append(table_name)

    # --- Extract validation scope mode ---
    # Tolerant matching: case-insensitive and forgiving of missing spaces around
    # the checkbox (e.g. "-[x]upstream"). Collect *every* ticked checkbox so we
    # can surface ambiguity and never silently downgrade the scope.
    ticked_modes: List[str] = []
    for line in lines:
        stripped = line.strip()
        # Match a ticked checkbox: "- [x] mode", "-[X]mode", "* [x]  mode", etc.
        match = re.match(r"^[-*]\s*\[x\]\s*(\w+)\s*$", stripped, re.IGNORECASE)
        if match:
            ticked_modes.append(match.group(1).strip().lower())

    if len(ticked_modes) > 1:
        print(
            f"{LOG_PREFIX_WARNING} Multiple scope modes ticked: {ticked_modes}. "
            f"Using the first valid one in document order."
        )

    recognized = [m for m in ticked_modes if m in VALID_SCOPE_MODES]
    if recognized:
        scope_mode = recognized[0]
    else:
        # Any ticked-but-unknown box must be loud, never a silent downgrade.
        for unknown in ticked_modes:
            print(
                f"{LOG_PREFIX_WARNING} Unrecognized scope mode '{unknown}', "
                f"defaulting to {DEFAULT_SCOPE_MODE}."
            )

    return affected_tables, scope_mode


# ===========================================================================
# Validation logic
# ===========================================================================

def validate(
    metadata_path: str,
    affected_tables: List[str],
    scope_mode: str,
) -> Tuple[bool, List[str]]:
    """
    Run all Phase 1 validation checks.

    Returns:
        Tuple of (passed: bool, errors: list of error messages).
    """
    errors: List[str] = []

    # -----------------------------------------------------------------------
    # Step 1: Load metadata
    # -----------------------------------------------------------------------
    try:
        subset = SubsetScope(metadata_path)
        registry = TableRegistry(metadata_path)
        lineage = TableLineage(metadata_path)
    except MetadataLoadError as e:
        errors.append(f"Failed to load metadata: {e}")
        return False, errors

    # -----------------------------------------------------------------------
    # Step 1.5: Validate structural shape of metadata files (schema)
    # Runs regardless of subset.enabled so malformed metadata is caught early.
    # Accepts empty onboarding skeletons; rejects missing keys, wrong types, typos.
    # -----------------------------------------------------------------------
    shape_errors = validate_metadata_shapes(metadata_path)
    if shape_errors:
        for se in shape_errors:
            print(f"{LOG_PREFIX_ERROR} Metadata shape validation FAILED: {se}")
        errors.extend(shape_errors)
        return False, errors

    # -----------------------------------------------------------------------
    # Step 2: Check if subset validation is enabled
    # -----------------------------------------------------------------------
    if not subset.enabled:
        print(f"{LOG_PREFIX_INFO} Subset validation is disabled (subset.enabled=false). Passing.")
        return True, []

    # -----------------------------------------------------------------------
    # Step 3: Handle empty affected tables
    # -----------------------------------------------------------------------
    if not affected_tables:
        print(
            f"{LOG_PREFIX_INFO} No affected tables declared in PR description. "
            f"Table detection will rely on git diff (detect_impact.py)."
        )
        # Output empty ADO variable so downstream steps know PR declared nothing
        print("##vso[task.setvariable variable=pr_declared_tables]")
        return True, []

    print(f"{LOG_PREFIX_INFO} Declared affected tables: {affected_tables}")
    print(f"{LOG_PREFIX_INFO} Validation scope mode: {scope_mode}")

    # -----------------------------------------------------------------------
    # Step 4: Validate each declared table exists in registry
    # -----------------------------------------------------------------------
    for table in affected_tables:
        if not registry.table_exists(table):
            # Check if it's a non-subset table (warning) vs unknown table (error)
            if not subset.is_table_in_subset(table):
                print(
                    f"{LOG_PREFIX_WARNING} Declared table '{table}' is not in subset scope - "
                    f"will be filtered out (no failure)."
                )
            else:
                errors.append(
                    f"Declared table '{table}' is in subset scope but not registered "
                    f"in table_registry.yml."
                )

    # -----------------------------------------------------------------------
    # Step 5: Filter to subset scope
    # -----------------------------------------------------------------------
    subset_affected = subset.filter_tables(affected_tables)

    if not subset_affected:
        print(f"{LOG_PREFIX_SKIP} No subset tables remain after filtering. Running in no-op mode. Passing.")
        print("##vso[task.setvariable variable=pr_declared_tables]")
        return True, []

    print(f"{LOG_PREFIX_INFO} Subset-filtered tables: {subset_affected}")
    # Export the subset-filtered DECLARED tables (NOT the lineage-expanded set)
    # as an ADO variable for the downstream merge. By design: later pipeline
    # stages (freshness gate, data presence) re-derive scope themselves from
    # --scope_mode, so this handoff stays a stable, minimal source of truth
    # instead of baking one scope mode into the variable.
    print("##vso[task.setvariable variable=pr_declared_tables]{}".format(
        ",".join(sorted(subset_affected))
    ))

    # -----------------------------------------------------------------------
    # Step 6: Cross-validate metadata consistency
    # -----------------------------------------------------------------------
    consistency_errors = validate_metadata_consistency(subset, registry, lineage)
    errors.extend(consistency_errors)

    dag_errors = [e for e in consistency_errors if "not a valid DAG" in e]
    if dag_errors:
        for dag_err in dag_errors:
            print(f"{LOG_PREFIX_ERROR} Step 6 — Metadata consistency check FAILED: {dag_err}")
        print(f"{LOG_PREFIX_ERROR} BFS traversal blocked. Resolve the cycle(s) in table_lineage.yml before proceeding.")
        return False, errors

    # -----------------------------------------------------------------------
    # Step 7: Expand lineage based on scope mode.
    # `expanded` is computed for LOCAL contract-warning coverage only (Step 8)
    # and is deliberately NOT exported. Downstream stages re-expand from
    # --scope_mode, so keeping this local avoids a second, divergent source of
    # scope truth.
    # -----------------------------------------------------------------------
    subset_table_set = set(subset.tables)
    try:
        expanded = lineage.expand_scope(
            declared_tables=set(subset_affected),
            scope_mode=scope_mode,
            subset_tables=subset_table_set,
        )
    except MetadataValidationError as e:
        errors.append(str(e))
        return False, errors

    print(f"{LOG_PREFIX_INFO} Expanded validation scope ({scope_mode}): {sorted(expanded)}")

    # -----------------------------------------------------------------------
    # Step 8: Contract completeness for the expanded set.
    # Structural existence (registry membership + lineage entry) is already
    # guaranteed by Step 6 for every subset table, and `expanded` is always a
    # subset of the subset tables -- so re-checking it here would only produce
    # duplicate errors. We therefore surface per-table contract WARNINGS only.
    # New per-table contract checks belong in collect_contract_warnings().
    # -----------------------------------------------------------------------
    for warning in collect_contract_warnings(registry, expanded):
        print(f"{LOG_PREFIX_WARNING} {warning}")

    # -----------------------------------------------------------------------
    # Step 9: Data presence checks -- handled by Phase 2 pipeline step
    # See: trigger_data_presence.py (runs as AzureCLI@2 step after this)
    # -----------------------------------------------------------------------
    print(
        f"{LOG_PREFIX_INFO} Data presence checks (row count, freshness, schema) "
        f"are handled by the Phase 2 pipeline step (trigger_data_presence.py)."
    )

    # De-duplicate while preserving order -- defense against any double-reporting
    # (e.g. an overlap between the Step 4 triage and Step 6 consistency checks).
    errors = list(dict.fromkeys(errors))

    return len(errors) == 0, errors


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="PR Validation — Subset Table Governance")
    parser.add_argument(
        "--metadata_path",
        default=METADATA_PATH,
        help="Path to metadata directory.",
    )
    parser.add_argument(
        "--pr_description_env_var",
        default="SYSTEM_PULLREQUEST_DESCRIPTION",
        help="Name of environment variable containing PR description.",
    )
    parser.add_argument(
        "--pr_description",
        default=None,
        help="PR description text (alternative to env var, for local testing).",
    )
    parser.add_argument(
        "--pr_description_file",
        default=None,
        help="Path to a file containing the PR description text.",
    )
    parser.add_argument(
        "--validation_scope",
        default=None,
        help="Override validation scope mode (for pipeline variable control).",
    )
    parser.add_argument(
        "--additional_tables",
        nargs="?",
        default="",
        help="Comma-separated list of additional tables (e.g. from detect_impact git diff).",
    )
    args = parser.parse_args()

    print(f"{LOG_PREFIX_START} PR Validation Starting")
    print(f"{LOG_PREFIX_INFO} Metadata path: {args.metadata_path}")
    print()

    # -----------------------------------------------------------------------
    # Get PR description
    # -----------------------------------------------------------------------
    pr_description = args.pr_description
    if pr_description is None and args.pr_description_file:
        try:
            with open(args.pr_description_file, "r", encoding="utf-8-sig") as f:
                pr_description = f.read()
        except OSError as e:
            print(f"{LOG_PREFIX_WARNING} Could not read PR description file: {e}")
    if pr_description is None:
        pr_description = os.environ.get(args.pr_description_env_var, "")

    # Strip any stray BOM characters
    pr_description = pr_description.replace("\ufeff", "")

    # Debug: log raw PR description (first 500 chars) for troubleshooting
    print(f"{LOG_PREFIX_INFO} PR description length: {len(pr_description)} chars")
    if pr_description:
        preview = repr(pr_description[:500])
        print(f"{LOG_PREFIX_INFO} PR description preview: {preview}")

    if not pr_description:
        print(f"{LOG_PREFIX_WARNING} PR description is empty or not available.")
        print(f"{LOG_PREFIX_WARNING} Checked env var: {args.pr_description_env_var}")
        print(
            f"{LOG_PREFIX_INFO} If this is not a PR build, validation will run in no-op mode."
        )

    # -----------------------------------------------------------------------
    # Parse PR description
    # -----------------------------------------------------------------------
    affected_tables, scope_mode = parse_pr_description(pr_description)
    runtime_overrides = parse_pr_runtime_overrides(pr_description)
    if runtime_overrides:
        print(f"{LOG_PREFIX_INFO} Parsed runtime overrides from PR: {runtime_overrides}")
    else:
        print(f"{LOG_PREFIX_INFO} No runtime overrides found in PR description.")

    # Override scope mode if provided via CLI
    if args.validation_scope and args.validation_scope in VALID_SCOPE_MODES:
        scope_mode = args.validation_scope
        print(f"{LOG_PREFIX_INFO} Scope mode overridden via CLI: {scope_mode}")

    # Merge additional tables (from detect_impact git diff) with PR-declared tables
    additional = [t.strip() for t in (args.additional_tables or "").split(",") if t.strip()]
    if additional:
        print(f"{LOG_PREFIX_INFO} Additional tables from git diff: {additional}")
        merged = list(dict.fromkeys(affected_tables + additional))  # dedupe, preserve order
        affected_tables = merged
        print(f"{LOG_PREFIX_INFO} Merged affected tables (PR + git diff): {affected_tables}")

    # Echo the final resolved parse result (single explicit line) so the
    # tables + scope mode that drive validation are always traceable in logs.
    print(
        f"{LOG_PREFIX_INFO} Resolved parse result -> "
        f"tables: {affected_tables if affected_tables else '[]'} | scope_mode: {scope_mode}"
    )

    # Output scope_mode as ADO variable for downstream steps
    print("##vso[task.setvariable variable=pr_scope_mode]{}".format(scope_mode))

    # -----------------------------------------------------------------------
    # Run validation
    # -----------------------------------------------------------------------
    passed, errors = validate(args.metadata_path, affected_tables, scope_mode)

    # -----------------------------------------------------------------------
    # Report results
    # -----------------------------------------------------------------------
    print()
    if passed:
        print(f"{LOG_PREFIX_SUCCESS} PR Validation PASSED")
        sys.exit(0)
    else:
        print(f"{LOG_PREFIX_ERROR} PR Validation FAILED with {len(errors)} error(s):")
        for i, err in enumerate(errors, 1):
            print(f"    {i}. {err}")
        # Azure DevOps error logging
        for err in errors:
            print(f"##vso[task.logissue type=error]{err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
