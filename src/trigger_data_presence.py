# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Phase 2c - Data Presence Validation Orchestrator.

Builds the contract payload from table_registry.yml, triggers the
data_presence_check notebook in Fabric via the Jobs API, reads the
results JSON, and prints a structured pass/fail report.

Usage (local testing against a personal workspace):
    az login
    python trigger_data_presence.py \
        --metadata_path examples/metadata \
        --workspace_id <your-notebook-workspace-id> \
        --lakehouse_workspace_id <your-lakehouse-workspace-id> \
        --notebook_item_id <your-notebook-item-id> \
        --mode soft

Usage (CI pipeline - IDs from pipeline variables):
    python trigger_data_presence.py \
        --metadata_path $(metadata_path) \
        --workspace_id $(fabric_notebook_workspace_id) \
        --lakehouse_workspace_id $(fabric_lakehouse_workspace_id) \
        --lakehouse_id $(fabric_lakehouse_id) \
        --notebook_item_id $(data_presence_notebook_id) \
        --mode soft
"""

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    ADVANCED_CHECK_TIMEOUT_SEC,
    ALLOWED_LAKEHOUSE_WORKSPACE_IDS,
    ALLOWED_NOTEBOOK_WORKSPACE_IDS,
    DATA_PRESENCE_NOTEBOOK_ITEM_ID,
    DATA_PRESENCE_NOTEBOOK_NAME,
    DATA_PRESENCE_RESULTS_PATH,
    DATA_PRESENCE_RETENTION_MONTHS,
    DEFAULT_ADV_CHECKS_SCOPE,
    ENABLE_PR_RUNTIME_OVERRIDES,
    FABRIC_LAKEHOUSE_ID,
    FABRIC_LAKEHOUSE_WORKSPACE_ID,
    FABRIC_WORKSPACE_ID,
    JOB_TIMEOUT_SEC,
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
)
from fabric_jobs_client import FabricJobsClient, FabricJobsClientError
from freshness_validator import FreshnessValidator
from freshness_validator import summarize_results as freshness_summarize
from metadata_parser import (
    AdvancedChecks,
    ExecutionFreshness,
    MetadataLoadError,
    MetadataValidationError,
    NotebookOutputs,
    PipelineOutputs,
    SubsetScope,
    TableLineage,
    TableRegistry,
)
from validate_pr import parse_pr_runtime_overrides

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)


def _is_uuid(value: str) -> bool:
    return bool(value and _UUID_RE.match(value))


def _load_pr_description_text(pr_description_file: str, env_var_name: str) -> str:
    """Load PR description from file first, then environment variable fallback."""
    if pr_description_file:
        try:
            with open(pr_description_file, "r", encoding="utf-8-sig") as f:
                return f.read().replace("\ufeff", "")
        except OSError as e:
            print("{} Could not read PR description file '{}': {}".format(
                LOG_PREFIX_WARNING, pr_description_file, e
            ))
    env_text = os.environ.get(env_var_name, "")
    return env_text.replace("\ufeff", "")


def resolve_runtime_targets(base_targets: dict, pr_overrides: dict, explicit_overrides: dict) -> dict:
    """
    Resolve notebook/lakehouse targets with precedence and allowlist enforcement.

    Precedence:
      explicit_overrides > pr_overrides > base_targets
    """
    resolved = {
        "workspace_id": base_targets.get("workspace_id", ""),
        "lakehouse_workspace_id": base_targets.get("lakehouse_workspace_id", ""),
        "lakehouse_id": base_targets.get("lakehouse_id", ""),
    }

    if ENABLE_PR_RUNTIME_OVERRIDES and pr_overrides:
        pr_ws = pr_overrides.get(PR_NOTEBOOK_WORKSPACE_ID_KEY, "")
        if pr_ws:
            if pr_ws in ALLOWED_NOTEBOOK_WORKSPACE_IDS:
                resolved["workspace_id"] = pr_ws
                print("{} Using PR override notebook workspace_id={}".format(
                    LOG_PREFIX_INFO, pr_ws
                ))
            else:
                print(
                    "{} PR override notebook workspace '{}' rejected (not in allowlist). "
                    "Using default/base value.".format(LOG_PREFIX_WARNING, pr_ws)
                )

        pr_lh_ws = pr_overrides.get(PR_LAKEHOUSE_WORKSPACE_ID_KEY, "")
        if pr_lh_ws:
            if pr_lh_ws in ALLOWED_LAKEHOUSE_WORKSPACE_IDS:
                resolved["lakehouse_workspace_id"] = pr_lh_ws
                print("{} Using PR override lakehouse_workspace_id={}".format(
                    LOG_PREFIX_INFO, pr_lh_ws
                ))
            else:
                print(
                    "{} PR override lakehouse workspace '{}' rejected (not in allowlist). "
                    "Using default/base value.".format(LOG_PREFIX_WARNING, pr_lh_ws)
                )

        # NOTE: unlike the two workspace IDs above, lakehouse_id is intentionally
        # NOT allowlisted. It is UUID-validated upstream, the lakehouse workspace
        # it resolves against IS allowlisted, and the results file path is
        # orchestrator-controlled (random run_id). The realistic worst case is a
        # wrong lakehouse yielding an unreadable results file, which is now
        # surfaced loudly as a failure (see parse_results_payload) rather than a
        # silent pass. Residual risk accepted; revisit if lakehouse selection
        # ever becomes attacker-influenced.
        pr_lh_id = pr_overrides.get(PR_LAKEHOUSE_ID_KEY, "")
        if pr_lh_id:
            resolved["lakehouse_id"] = pr_lh_id
            print("{} Using PR override lakehouse_id={}".format(LOG_PREFIX_INFO, pr_lh_id))

    ex_ws = explicit_overrides.get("workspace_id", "")
    if ex_ws:
        resolved["workspace_id"] = ex_ws
        print("{} Using explicit override workspace_id={}".format(LOG_PREFIX_INFO, ex_ws))

    ex_lh_ws = explicit_overrides.get("lakehouse_workspace_id", "")
    if ex_lh_ws:
        resolved["lakehouse_workspace_id"] = ex_lh_ws
        print("{} Using explicit override lakehouse_workspace_id={}".format(LOG_PREFIX_INFO, ex_lh_ws))

    ex_lh_id = explicit_overrides.get("lakehouse_id", "")
    if ex_lh_id:
        resolved["lakehouse_id"] = ex_lh_id
        print("{} Using explicit override lakehouse_id={}".format(LOG_PREFIX_INFO, ex_lh_id))

    return resolved


def resolve_notebook_item_id(
    client: FabricJobsClient,
    notebook_name: str,
    fallback_notebook_id: str,
    role_label: str,
) -> str:
    """
    Resolve a notebook ID using name-first lookup with fallback ID support.

    Rules:
      - If notebook_name resolves uniquely in workspace, use resolved ID.
      - If not found and fallback_notebook_id exists, use fallback.
      - If unresolved and no fallback, raise error.
      - If name lookup is ambiguous, propagate the client error.
    """
    if notebook_name:
        resolved_id = client.resolve_notebook_id_by_name(notebook_name)
        if resolved_id:
            print("{} Resolved {} notebook by name '{}' -> {}".format(
                LOG_PREFIX_INFO,
                role_label,
                notebook_name,
                resolved_id,
            ))
            return resolved_id
        print("{} {} notebook '{}' not found in workspace; trying fallback ID.".format(
            LOG_PREFIX_WARNING,
            role_label,
            notebook_name,
        ))

    if fallback_notebook_id:
        print("{} Using fallback {} notebook ID: {}".format(
            LOG_PREFIX_INFO,
            role_label,
            fallback_notebook_id,
        ))
        return fallback_notebook_id

    raise FabricJobsClientError(
        "Could not resolve {} notebook. name='{}', fallback_id='{}'".format(
            role_label,
            notebook_name,
            fallback_notebook_id,
        )
    )


def build_tables_payload(
    registry: TableRegistry,
    table_names: list,
) -> list:
    """
    Build the tables_json payload for the notebook from table_registry.yml.

    Each entry contains the contract fields the notebook needs:
    table_name, min_rows, row_count_mode, min_rows_date_check, freshness_hours, freshness_column,
    required_columns, not_null_columns, checks.
    """
    payload = []
    for table_name in table_names:
        entry = {
            "table_name": table_name,
            "min_rows": registry.get_min_rows(table_name),
            "row_count_mode": registry.get_row_count_mode(table_name),
            "min_rows_date_check": registry.get_min_rows_date_check(table_name),
            "freshness_hours": registry.get_freshness_hours(table_name),
            "freshness_column": registry.get_freshness_column(table_name),
            "required_columns": registry.get_required_columns(table_name),
            "not_null_columns": registry.get_not_null_columns(table_name),
            "checks": registry.get_custom_checks(table_name),
        }
        payload.append(entry)
    return payload


def print_advanced_check_report(check_name: str, results: dict) -> bool:
    """
    Print a human-readable report for a single advanced check notebook result.

    Returns True if check passed, False otherwise.
    """
    overall_passed = results.get("passed", False)
    prefix = LOG_PREFIX_SUCCESS if overall_passed else LOG_PREFIX_ERROR
    print("")
    print("-" * 70)
    print("{} Advanced Check: {}".format(prefix, check_name))
    print("{} Run ID: {} | Timestamp: {}".format(
        LOG_PREFIX_INFO, results.get("run_id", "?"), results.get("timestamp", "?")))

    for check in results.get("checks", []):
        status = check.get("status", "?")
        name = check.get("name", "?")
        if status == "passed":
            detail = ""
            if "recent_avg" in check:
                detail = " (recent={}, hist={} +/- {}, bounds=[{}, {}])".format(
                    check.get("recent_avg"),
                    check.get("hist_avg"),
                    check.get("hist_stddev"),
                    check.get("lower_bound"),
                    check.get("upper_bound"),
                )
            print("  {} {}: passed{}".format(LOG_PREFIX_SUCCESS, name, detail))
        elif status == "failed":
            detail = ""
            if "recent_avg" in check:
                detail = " (recent={}, bounds=[{}, {}])".format(
                    check.get("recent_avg"),
                    check.get("lower_bound"),
                    check.get("upper_bound"),
                )
            elif "message" in check:
                detail = " ({})".format(check["message"])
            print("  {} {}: FAILED{}".format(LOG_PREFIX_ERROR, name, detail))
        elif status == "skipped":
            msg = check.get("message", "")
            print("  {} {}: skipped{}".format(
                LOG_PREFIX_SKIP, name, " - " + msg if msg else ""))
        elif status == "error":
            msg = check.get("message", "")
            print("  {} {}: ERROR{}".format(
                LOG_PREFIX_ERROR, name, " - " + msg if msg else ""))

    return overall_passed


def print_results_report(results: dict) -> bool:
    """
    Print a human-readable report from the notebook results JSON.

    Returns True if all checks passed, False otherwise.
    """
    summary = results.get("summary", {})
    tables = results.get("tables", {})

    print("")
    print("=" * 70)
    print("DATA PRESENCE VALIDATION REPORT")
    print("=" * 70)
    print("{} Run ID: {}".format(LOG_PREFIX_INFO, results.get("run_id", "?")))
    print("{} Mode: {}".format(LOG_PREFIX_INFO, results.get("mode", "?")))
    print("{} Timestamp: {}".format(LOG_PREFIX_INFO, results.get("timestamp", "?")))
    print("")

    for table_name, table_result in tables.items():
        checks = table_result.get("checks", [])
        passed = table_result.get("passed", False)
        prefix = LOG_PREFIX_SUCCESS if passed else LOG_PREFIX_ERROR
        print("{} {}".format(prefix, table_name))
        for check in checks:
            status = check.get("status", "?")
            name = check.get("name", "?")
            if status == "passed":
                detail = ""
                if "actual" in check and "expected_min" in check:
                    detail = " (actual={}, min={})".format(
                        check["actual"], check["expected_min"]
                    )
                elif "age_hours" in check:
                    detail = " (age={:.1f}h, limit={}h)".format(
                        check["age_hours"], check["limit_hours"]
                    )
                print("  {} {}: passed{}".format(LOG_PREFIX_SUCCESS, name, detail))
            elif status == "failed":
                detail = ""
                if "actual" in check and "expected_min" in check:
                    detail = " (actual={}, min={})".format(
                        check["actual"], check["expected_min"]
                    )
                elif "missing" in check:
                    detail = " (missing: {})".format(check["missing"])
                elif "message" in check:
                    detail = " ({})".format(check["message"])
                print("  {} {}: FAILED{}".format(LOG_PREFIX_ERROR, name, detail))
            elif status == "skipped":
                msg = check.get("message", "")
                print("  {} {}: skipped{}".format(
                    LOG_PREFIX_SKIP, name, " - " + msg if msg else ""
                ))
            elif status == "error":
                msg = check.get("message", "")
                print("  {} {}: ERROR{}".format(
                    LOG_PREFIX_ERROR, name, " - " + msg if msg else ""
                ))

    print("")
    print("-" * 70)
    print("{} Total: {} | Passed: {} | Failed: {} | Skipped: {} | Error: {}".format(
        LOG_PREFIX_INFO,
        summary.get("total", 0),
        summary.get("passed", 0),
        summary.get("failed", 0),
        summary.get("skipped", 0),
        summary.get("error", 0),
    ))
    print("=" * 70)

    all_passed = summary.get("failed", 0) == 0 and summary.get("error", 0) == 0
    return all_passed


def parse_results_payload(results_content):
    """Classify and parse the results payload read back from OneLake.

    The notebook job already reached "Completed", so the checks DID run
    remotely; this only interprets whether we could retrieve a verdict.

    Returns (results, status):
      - status "ok": results is the parsed dict.
      - status "missing": results_content was None (file not found/unreadable).
      - status "unparseable": results_content was not valid JSON.
    In the non-"ok" cases results is None.
    """
    if results_content is None:
        return None, "missing"
    try:
        return json.loads(results_content), "ok"
    except json.JSONDecodeError:
        return None, "unparseable"


def resolve_scope(lineage, table_names, scope_mode, subset_tables):
    """Expand the directly-affected tables to the scoped validation set.

    Uses lineage.expand_scope (the single source of truth: subset-filtered and
    correct for all four modes; affected_only returns the declared set
    verbatim). On an invalid lineage DAG (e.g. a cycle) it degrades safely to
    the directly-affected tables so a metadata error never crashes or silently
    drops checks.

    Returns (scoped_tables, error) where error is None on success or the
    stringified MetadataValidationError when the safe fallback was used.
    """
    directly_impacted = set(table_names)
    try:
        return lineage.expand_scope(directly_impacted, scope_mode, subset_tables), None
    except MetadataValidationError as e:
        return set(directly_impacted), str(e)


def main():
    parser = argparse.ArgumentParser(
        description="Phase 2 - Data Presence Validation Orchestrator"
    )
    parser.add_argument(
        "--metadata_path", default=METADATA_PATH,
        help="Path to metadata directory.",
    )
    parser.add_argument(
        "--workspace_id", default=FABRIC_WORKSPACE_ID,
        help="Fabric workspace ID.",
    )
    parser.add_argument(
        "--lakehouse_id", default=FABRIC_LAKEHOUSE_ID,
        help="Fabric Lakehouse ID.",
    )
    parser.add_argument(
        "--lakehouse_workspace_id", default=FABRIC_LAKEHOUSE_WORKSPACE_ID,
        help="Workspace ID that owns the lakehouse (for OneLake DFS read). "
             "Defaults to --workspace_id. Use when the notebook runs in a "
             "different workspace than the lakehouse.",
    )
    parser.add_argument(
        "--notebook_name", default=DATA_PRESENCE_NOTEBOOK_NAME,
        help="Fabric notebook display name for data presence validation.",
    )
    parser.add_argument(
        "--notebook_item_id", default=DATA_PRESENCE_NOTEBOOK_ITEM_ID,
        help="Fallback Fabric item ID of the data_presence_check notebook.",
    )
    parser.add_argument(
        "--mode", default="soft", choices=["soft", "hard"],
        help="Validation mode: soft (warn only) or hard (fail on violation).",
    )
    parser.add_argument(
        "--tables", default="",
        help="Comma-separated list of tables to validate. Empty = all subset tables.",
    )
    parser.add_argument(
        "--timeout_sec", type=int, default=JOB_TIMEOUT_SEC,
        help="Max seconds to wait for notebook job.",
    )
    parser.add_argument(
        "--results_path", default=DATA_PRESENCE_RESULTS_PATH,
        help="OneLake file path or directory where notebook writes results JSON.",
    )
    parser.add_argument(
        "--retention_months", type=int, default=DATA_PRESENCE_RETENTION_MONTHS,
        help="Delete result files older than this many months from the results directory.",
    )
    parser.add_argument(
        "--results_output_file", default="",
        help="Local file path to save results JSON (for ADO artifact publishing).",
    )
    parser.add_argument(
        "--adv_checks_scope", default=DEFAULT_ADV_CHECKS_SCOPE,
        choices=["all", "affected", "none"],
        help="Which advanced checks to run: 'all' (every enabled check), "
             "'affected' (only checks whose tables overlap PR-affected tables), "
             "'none' (skip advanced checks entirely).",
    )
    parser.add_argument(
        "--scope_mode", default="affected_only",
        choices=["affected_only", "upstream", "downstream", "full_lineage"],
        help="Validation scope mode from PR description. Controls whether freshness "
             "gating expands to downstream tables.",
    )
    parser.add_argument(
        "--pr_description_file", default="",
        help="Optional path to PR description text file. Used for runtime workspace overrides.",
    )
    parser.add_argument(
        "--pr_description_env_var", default="SYSTEM_PULLREQUEST_DESCRIPTION",
        help="Fallback env var containing PR description text when --pr_description_file is not provided.",
    )
    parser.add_argument(
        "--override_workspace_id", default="",
        help="Explicit override for notebook workspace ID (highest precedence).",
    )
    parser.add_argument(
        "--override_lakehouse_workspace_id", default="",
        help="Explicit override for lakehouse workspace ID (highest precedence).",
    )
    parser.add_argument(
        "--override_lakehouse_id", default="",
        help="Explicit override for lakehouse ID (highest precedence).",
    )
    args = parser.parse_args()

    base_targets = {
        "workspace_id": args.workspace_id,
        "lakehouse_workspace_id": args.lakehouse_workspace_id,
        "lakehouse_id": args.lakehouse_id,
    }
    explicit_overrides = {
        "workspace_id": args.override_workspace_id,
        "lakehouse_workspace_id": args.override_lakehouse_workspace_id,
        "lakehouse_id": args.override_lakehouse_id,
    }

    pr_desc_text = _load_pr_description_text(args.pr_description_file, args.pr_description_env_var)
    pr_overrides = parse_pr_runtime_overrides(pr_desc_text)
    if pr_overrides:
        print("{} Parsed PR runtime overrides: {}".format(LOG_PREFIX_INFO, pr_overrides))
    else:
        print("{} No PR runtime overrides provided.".format(LOG_PREFIX_INFO))

    resolved_targets = resolve_runtime_targets(base_targets, pr_overrides, explicit_overrides)
    runtime_workspace_id = resolved_targets["workspace_id"]
    runtime_lakehouse_workspace_id = resolved_targets["lakehouse_workspace_id"]
    runtime_lakehouse_id = resolved_targets["lakehouse_id"]

    if not runtime_workspace_id:
        # Phase 2 is simply not configured (no notebook workspace provided).
        # Skip cleanly with actionable guidance instead of failing later with an
        # opaque HTTP error. Mirrors the missing-notebook-identity skip below
        # (always exit 0 — "Phase 2 not set up" is not a data-quality failure).
        print(
            "{} FABRIC_WORKSPACE_ID is not set, so Phase 2 (data-presence) cannot "
            "run. Set the FABRIC_WORKSPACE_ID environment variable or pass "
            "--workspace_id (see docs/configuration-reference.md and "
            "fabric/README.md). Skipping data-presence checks.".format(LOG_PREFIX_SKIP)
        )
        sys.exit(0)
    if not _is_uuid(runtime_workspace_id):
        print("{} Invalid notebook workspace ID '{}'.".format(LOG_PREFIX_ERROR, runtime_workspace_id))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)
    if runtime_lakehouse_workspace_id and not _is_uuid(runtime_lakehouse_workspace_id):
        print("{} Invalid lakehouse workspace ID '{}'.".format(LOG_PREFIX_ERROR, runtime_lakehouse_workspace_id))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)
    if runtime_lakehouse_id and not _is_uuid(runtime_lakehouse_id):
        print("{} Invalid lakehouse ID '{}'.".format(LOG_PREFIX_ERROR, runtime_lakehouse_id))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)

    print("{} Data Presence Validation Orchestrator Starting".format(LOG_PREFIX_START))
    print("{} Workspace: {}".format(LOG_PREFIX_INFO, runtime_workspace_id))
    print("{} Lakehouse WS: {}".format(LOG_PREFIX_INFO, runtime_lakehouse_workspace_id or runtime_workspace_id))
    print("{} Lakehouse: {}".format(LOG_PREFIX_INFO, runtime_lakehouse_id))
    print("{} Notebook name: {}".format(LOG_PREFIX_INFO, args.notebook_name))
    print("{} Notebook fallback ID: {}".format(LOG_PREFIX_INFO, args.notebook_item_id or "<none>"))
    print("{} Mode:      {}".format(LOG_PREFIX_INFO, args.mode))
    print("")

    # ------------------------------------------------------------------
    # Step 1: Load metadata and determine tables to validate
    # ------------------------------------------------------------------
    try:
        subset = SubsetScope(args.metadata_path)
        registry = TableRegistry(args.metadata_path)
    except MetadataLoadError as e:
        print("{} Failed to load metadata: {}".format(LOG_PREFIX_ERROR, e))
        # Metadata load failure is infrastructure, not data quality - exit 0 in soft
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)

    if not subset.enabled:
        print("{} Subset validation disabled. Nothing to check.".format(LOG_PREFIX_SKIP))
        sys.exit(0)

    if args.tables:
        table_names = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        # No tables specified — nothing to validate.
        # This happens when neither git diff nor PR description identified any affected tables.
        print("{} No tables specified (--tables is empty). Nothing to validate.".format(LOG_PREFIX_SKIP))
        sys.exit(0)

    if not table_names:
        print("{} No tables to validate.".format(LOG_PREFIX_SKIP))
        sys.exit(0)

    print("{} Tables to validate: {}".format(LOG_PREFIX_INFO, len(table_names)))

    # ------------------------------------------------------------------
    # Step 1a: Resolve validation scope (shared by freshness gate + data presence)
    # ------------------------------------------------------------------
    # Expand the directly-affected tables to the scoped validation set ONCE,
    # via lineage.expand_scope (the single source of truth: subset-filtered,
    # handles affected_only / upstream / downstream / full_lineage). Both the
    # freshness gate (Step 1b) and the data-presence payload (Step 2) consume
    # this same set so they can never diverge. affected_only returns the
    # declared set verbatim, so this is a no-op unless a wider scope mode is
    # configured.
    directly_impacted = set(table_names)
    try:
        scope_lineage = TableLineage(args.metadata_path)
    except MetadataLoadError as e:
        print("{} Could not load lineage metadata: {}. Falling back to directly "
              "affected tables only (no scope expansion).".format(LOG_PREFIX_WARNING, e))
        scope_lineage = None

    if scope_lineage is not None:
        scoped_tables, scope_err = resolve_scope(
            scope_lineage, table_names, args.scope_mode, set(subset.tables)
        )
        if scope_err is not None:
            print("{} Could not expand validation scope ({}). Falling back to "
                  "directly affected tables only.".format(LOG_PREFIX_WARNING, scope_err[:200]))
    else:
        scoped_tables = set(directly_impacted)

    print("{} Directly impacted tables: {}".format(LOG_PREFIX_INFO, len(directly_impacted)))
    print("{} Scoped tables (mode='{}', subset-filtered): {}".format(
        LOG_PREFIX_INFO, args.scope_mode, len(scoped_tables)))
    print("{} [DEBUG] Scoped list: {}".format(LOG_PREFIX_INFO, sorted(scoped_tables)))

    # ------------------------------------------------------------------
    # Step 1b: Freshness gating (lineage-aware, before data presence)
    # ------------------------------------------------------------------
    freshness_gate_passed = True
    try:
        freshness_cfg = ExecutionFreshness(args.metadata_path)
    except MetadataLoadError:
        freshness_cfg = None

    if freshness_cfg and freshness_cfg.enabled and freshness_cfg.mode != "skip":
        print("")
        print("=" * 70)
        print("FRESHNESS GATING (mode={})".format(freshness_cfg.mode))
        print("=" * 70)

        if scope_lineage is None:
            print("{} Lineage metadata unavailable. Skipping freshness gate.".format(
                LOG_PREFIX_WARNING))
        else:
            try:
                nb_outputs = NotebookOutputs(args.metadata_path)
                pl_outputs = PipelineOutputs(args.metadata_path)
            except MetadataLoadError as e:
                print("{} Could not load output metadata: {}. Skipping freshness gate.".format(
                    LOG_PREFIX_WARNING, e
                ))
                nb_outputs = None

            if nb_outputs is not None:
                try:
                    freshness_client = FabricJobsClient(workspace_id=runtime_workspace_id)
                    validator = FreshnessValidator(
                        client=freshness_client,
                        lineage=scope_lineage,
                        notebook_outputs=nb_outputs,
                        pipeline_outputs=pl_outputs,
                        tolerance_minutes=freshness_cfg.tolerance_minutes,
                        default_max_age_hours=freshness_cfg.default_max_age_hours,
                        registry=registry,
                    )
                    freshness_results = validator.validate(
                        directly_impacted=directly_impacted,
                        tainted_tables=scoped_tables,
                        exempt_tables=set(freshness_cfg.exempt_tables),
                    )
                    freshness_gate_passed = freshness_summarize(
                        freshness_results,
                        freshness_cfg.mode,
                    )
                except Exception as e:
                    print("{} Freshness validation error: {}. Skipping freshness gate.".format(
                        LOG_PREFIX_WARNING, str(e)[:300]
                    ))
                    freshness_gate_passed = True  # Don't block on infra errors

                if not freshness_gate_passed:
                    # freshness_summarize only returns False in hard mode, so this
                    # block is reached exclusively for a hard-mode failure (stale
                    # or no_record); soft/skip always return True and never get here.
                    print("{} Freshness gate BLOCKED the pipeline (hard mode).".format(LOG_PREFIX_ERROR))
                    sys.exit(1)
    else:
        print("{} Freshness gating disabled or not configured.".format(LOG_PREFIX_SKIP))

    # ------------------------------------------------------------------
    # Step 2: Build contract payload
    # ------------------------------------------------------------------
    # Data presence validates the SCOPE-EXPANDED set from Step 1a, not just the
    # directly-affected --tables. With a wider --scope_mode this also checks row
    # presence across the lineage neighborhood; affected_only keeps it to the
    # declared tables (no change).
    payload = build_tables_payload(registry, sorted(scoped_tables))
    tables_json = json.dumps(payload, default=str)
    print("{} Payload size: {} chars for {} tables".format(
        LOG_PREFIX_INFO, len(tables_json), len(payload)
    ))

    # ------------------------------------------------------------------
    # Step 3: Validate notebook identity config
    # ------------------------------------------------------------------
    if not args.notebook_name and not args.notebook_item_id:
        print("{} Neither notebook_name nor notebook_item_id is configured. Skipping data presence checks.".format(
            LOG_PREFIX_WARNING
        ))
        sys.exit(0)

    # ------------------------------------------------------------------
    # Step 4: Trigger notebook via Fabric Jobs API
    # ------------------------------------------------------------------
    run_id = str(uuid.uuid4())[:8]
    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.results_path.lower().endswith(".json"):
        resolved_results_path = args.results_path
    else:
        resolved_results_path = args.results_path.rstrip("/") + "/data_presence_results_{}_{}.json".format(
            run_timestamp,
            run_id,
        )

    print("{} Results path: {}".format(LOG_PREFIX_INFO, resolved_results_path))

    notebook_params = {
        "tables_json": {"value": tables_json, "type": "string"},
        "results_path": {"value": resolved_results_path, "type": "string"},
        "mode": {"value": args.mode, "type": "string"},
        "run_id": {"value": run_id, "type": "string"},
        "retention_months": {"value": str(args.retention_months), "type": "string"},
    }

    try:
        client = FabricJobsClient(workspace_id=runtime_workspace_id)
    except Exception as e:
        print("{} Failed to initialize Fabric client: {}".format(LOG_PREFIX_ERROR, e))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)

    try:
        data_presence_notebook_id = resolve_notebook_item_id(
            client=client,
            notebook_name=args.notebook_name,
            fallback_notebook_id=args.notebook_item_id,
            role_label="data_presence",
        )
        status, job_id = client.run_notebook(
            notebook_item_id=data_presence_notebook_id,
            parameters=notebook_params,
            timeout_sec=args.timeout_sec,
        )
    except FabricJobsClientError as e:
        print("{} Notebook job submission failed: {}".format(LOG_PREFIX_ERROR, e))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)
    except Exception as e:
        print("{} Unexpected error during notebook execution: {}".format(
            LOG_PREFIX_ERROR, str(e)[:300]
        ))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)

    print("{} Notebook job finished: status={}, job_id={}".format(
        LOG_PREFIX_INFO, status, job_id
    ))

    # ------------------------------------------------------------------
    # Step 5: Read results JSON from OneLake
    # ------------------------------------------------------------------
    if status not in ("Completed",):
        print("{} Notebook job did not complete successfully (status={}).".format(
            LOG_PREFIX_WARNING, status
        ))
        if args.mode == "soft":
            print("{} Exiting with 0 (soft mode).".format(LOG_PREFIX_WARNING))
            sys.exit(0)
        sys.exit(1)

    lakehouse_ws = runtime_lakehouse_workspace_id if runtime_lakehouse_workspace_id else None
    results_content = None
    try:
        results_content = client.read_onelake_file(
            lakehouse_id=runtime_lakehouse_id,
            file_path=resolved_results_path,
            workspace_id=lakehouse_ws,
        )
    except Exception as e:
        print("{} Failed to read results file: {}".format(LOG_PREFIX_WARNING, e))

    # ------------------------------------------------------------------
    # Step 6: Parse and report results
    # ------------------------------------------------------------------
    # The job reached "Completed", so the checks DID run remotely. If we cannot
    # read or parse the results here we have NO verdict: validation is
    # INCOMPLETE. Treat that as a failure and fall through to the final summary
    # (which applies soft/hard semantics) instead of silently exiting 0 in soft
    # mode - a quiet exit-0 is indistinguishable from a clean pass.
    results, results_status = parse_results_payload(results_content)
    if results_status == "missing":
        print("{} DATA PRESENCE VALIDATION INCOMPLETE: job Completed but the "
              "results file was not found or unreadable - data checks could NOT "
              "be verified. Treating as a failure.".format(LOG_PREFIX_ERROR))
    elif results_status == "unparseable":
        print("{} DATA PRESENCE VALIDATION INCOMPLETE: results file is not valid "
              "JSON - data checks could NOT be verified. Treating as a "
              "failure.".format(LOG_PREFIX_ERROR))

    if results is None:
        all_passed = False
    else:
        all_passed = print_results_report(results)

    # Save results to local file if requested (for ADO artifact publishing)
    combined_results = {"data_presence": results if results is not None else {}, "advanced_checks": {}}
    if args.results_output_file:
        try:
            out_dir = os.path.dirname(args.results_output_file)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
        except OSError as e:
            print("{} Could not create results directory: {}".format(LOG_PREFIX_WARNING, e))

    # ------------------------------------------------------------------
    # Step 7: Run advanced checks (sequential)
    # ------------------------------------------------------------------
    # TODO(next version): each advanced check below submits a notebook job and
    # blocks on its completion before starting the next one. When several checks
    # are enabled this serializes their wall-clock time. This can be optimized by
    # running the checks in parallel (submit all jobs, then poll/collect results)
    # to reduce total runtime. Deferred to a future version because it adds
    # concurrency + error-handling complexity to the FabricJobsClient usage.
    adv_all_passed = True
    if args.adv_checks_scope == "none":
        print("")
        print("{} Advanced checks skipped (--adv_checks_scope=none).".format(LOG_PREFIX_SKIP))
    else:
        try:
            adv_registry = AdvancedChecks(args.metadata_path)
        except MetadataLoadError:
            adv_registry = None

        if adv_registry is not None:
            if args.adv_checks_scope == "affected":
                affected_set = set(table_names)
                checks_to_run = adv_registry.filter_by_tables(affected_set)
            else:
                checks_to_run = adv_registry.enabled_checks()

            if not checks_to_run:
                print("")
                print("{} No advanced checks to run (scope={}).".format(
                    LOG_PREFIX_SKIP, args.adv_checks_scope))
            else:
                print("")
                print("=" * 70)
                print("ADVANCED CHECKS (sequential, scope={})".format(args.adv_checks_scope))
                print("=" * 70)
                print("{} Checks to run: {}".format(
                    LOG_PREFIX_INFO, len(checks_to_run)))

                for check_def in checks_to_run:
                    check_name = check_def.get("name", "unnamed")
                    notebook_name = check_def.get("notebook_name", "")
                    notebook_id = check_def.get("notebook_id", "")
                    check_timeout = check_def.get("timeout_sec", ADVANCED_CHECK_TIMEOUT_SEC)

                    if not notebook_name and not notebook_id:
                        print("{} Advanced check '{}' skipped - no notebook_name or notebook_id configured.".format(
                            LOG_PREFIX_SKIP, check_name))
                        combined_results["advanced_checks"][check_name] = {
                            "status": "skipped",
                            "message": "notebook_name/notebook_id not configured",
                        }
                        continue

                    # Build per-check results path
                    adv_results_path = args.results_path.rstrip("/") + "/adv_check_{}_{}_{}.json".format(
                        check_name, run_timestamp, run_id)

                    adv_params = {
                        "results_path": {"value": adv_results_path, "type": "string"},
                        "mode": {"value": args.mode, "type": "string"},
                        "run_id": {"value": run_id, "type": "string"},
                    }

                    print("{} Running advanced check: {} (notebook_name='{}', fallback_id='{}')".format(
                        LOG_PREFIX_INFO, check_name, notebook_name, notebook_id))

                    try:
                        adv_notebook_id = resolve_notebook_item_id(
                            client=client,
                            notebook_name=notebook_name,
                            fallback_notebook_id=notebook_id,
                            role_label="advanced_check:{}".format(check_name),
                        )
                        adv_status, _adv_job_id = client.run_notebook(
                            notebook_item_id=adv_notebook_id,
                            parameters=adv_params,
                            timeout_sec=check_timeout,
                        )
                    except (FabricJobsClientError, Exception) as e:
                        print("{} Advanced check '{}' failed to execute: {}".format(
                            LOG_PREFIX_ERROR, check_name, str(e)[:200]))
                        combined_results["advanced_checks"][check_name] = {
                            "status": "error",
                            "message": str(e)[:200],
                        }
                        adv_all_passed = False
                        continue

                    if adv_status != "Completed":
                        print("{} Advanced check '{}' did not complete (status={}).".format(
                            LOG_PREFIX_WARNING, check_name, adv_status))
                        combined_results["advanced_checks"][check_name] = {
                            "status": "error",
                            "message": "Job status: {}".format(adv_status),
                        }
                        adv_all_passed = False
                        continue

                    # Read advanced check results
                    adv_content = None
                    try:
                        adv_content = client.read_onelake_file(
                            lakehouse_id=runtime_lakehouse_id,
                            file_path=adv_results_path,
                            workspace_id=lakehouse_ws,
                        )
                    except Exception as e:
                        print("{} Failed to read advanced check results: {}".format(
                            LOG_PREFIX_WARNING, e))

                    if adv_content is None:
                        print("{} ADVANCED CHECK INCOMPLETE: '{}' job ran but its "
                              "results file was not found or unreadable - could "
                              "NOT be verified. Treating as a failure.".format(
                                  LOG_PREFIX_ERROR, check_name))
                        combined_results["advanced_checks"][check_name] = {
                            "status": "error",
                            "message": "Results file not found",
                        }
                        adv_all_passed = False
                        continue

                    try:
                        adv_results = json.loads(adv_content)
                    except json.JSONDecodeError as e:
                        print("{} Failed to parse advanced check results: {}".format(
                            LOG_PREFIX_ERROR, e))
                        combined_results["advanced_checks"][check_name] = {
                            "status": "error",
                            "message": "JSON parse error",
                        }
                        adv_all_passed = False
                        continue

                    check_passed = print_advanced_check_report(check_name, adv_results)
                    combined_results["advanced_checks"][check_name] = adv_results
                    if not check_passed:
                        adv_all_passed = False

    # ------------------------------------------------------------------
    # Step 8: Final summary and exit
    # ------------------------------------------------------------------
    if args.results_output_file:
        try:
            with open(args.results_output_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(combined_results, indent=2, default=str))
            print("{} Results saved to: {}".format(LOG_PREFIX_INFO, args.results_output_file))
        except OSError as e:
            print("{} Could not save results locally: {}".format(LOG_PREFIX_WARNING, e))

    # Freshness is a self-contained gate: it already blocks in-place via
    # sys.exit(1) when it fails (hard mode is its only failing case), so it can
    # never reach this point as a failure. Only data-presence and advanced
    # checks decide the final verdict here.
    overall_passed = all_passed and adv_all_passed

    if overall_passed:
        print("{} All validations PASSED.".format(LOG_PREFIX_SUCCESS))
        sys.exit(0)
    else:
        if args.mode == "hard":
            print("{} Validation FAILED (hard mode).".format(LOG_PREFIX_ERROR))
            sys.exit(1)
        else:
            print("{} Validation issues detected (soft mode - not blocking).".format(
                LOG_PREFIX_WARNING
            ))
            sys.exit(0)


if __name__ == "__main__":
    main()
