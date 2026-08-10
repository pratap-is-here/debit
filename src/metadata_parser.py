# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Metadata parser for the testing framework.
Loads and validates all YAML metadata files.
Provides typed access to subset scope, table registry, and lineage.
"""

import os
from typing import Any, Dict, Iterable, List, Optional, Set

import yaml

from constants import (
    ADVANCED_CHECK_TIMEOUT_SEC,
    ADVANCED_CHECKS_FILE,
    DEFAULT_FRESHNESS_MODE,
    DEFAULT_STALENESS_MAX_AGE_HOURS,
    DEFAULT_TOLERANCE_MINUTES,
    EXECUTION_FRESHNESS_FILE,
    LOG_PREFIX_INFO,
    LOG_PREFIX_SKIP,
    LOG_PREFIX_WARNING,
    METADATA_PATH,
    NOTEBOOK_OUTPUTS_FILE,
    PIPELINE_OUTPUTS_FILE,
    SUBSET_SCOPE_FILE,
    TABLE_LINEAGE_FILE,
    TABLE_REGISTRY_FILE,
)


class MetadataLoadError(Exception):
    """Raised when a required metadata file cannot be loaded."""


class MetadataValidationError(Exception):
    """Raised when metadata content is invalid."""


def _load_yaml(file_path: str) -> dict:
    """Load a YAML file and return its contents as a dictionary."""
    if not os.path.exists(file_path):
        raise MetadataLoadError(f"Metadata file not found: {file_path}")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if data is None:
            return {}
        return data
    except yaml.YAMLError as e:
        raise MetadataLoadError(f"Failed to parse YAML file {file_path}: {e}") from e


class SubsetScope:
    """Represents the subset_scope.yml configuration."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, SUBSET_SCOPE_FILE)
        self._data = _load_yaml(self._path)
        # `or {}` coalesces a present-but-empty section (parses to None) to {} —
        # without it, `subset:` with no children would crash later .get() calls.
        self._subset = self._data.get("subset") or {}

    @property
    def enabled(self) -> bool:
        return self._subset.get("enabled", False)

    @property
    def tables(self) -> List[str]:
        return self._subset.get("tables", [])

    @property
    def notebooks(self) -> List[str]:
        return self._subset.get("notebooks", [])

    @property
    def pipelines(self) -> List[str]:
        return self._subset.get("pipelines", [])

    def is_table_in_subset(self, table_name: str) -> bool:
        """Check if a table is in the subset scope."""
        return table_name in self.tables

    def filter_tables(self, table_names: List[str]) -> List[str]:
        """Return only tables that are in the subset scope."""
        subset_tables = []
        for t in table_names:
            if self.is_table_in_subset(t):
                subset_tables.append(t)
            else:
                print(f"{LOG_PREFIX_WARNING} Table '{t}' is not in subset scope - skipped.")
        return subset_tables


class TableRegistry:
    """Represents the table_registry.yml configuration."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, TABLE_REGISTRY_FILE)
        self._data = _load_yaml(self._path)
        # `or {}` coalesces a present-but-empty `tables:` section (None) to {}.
        self._tables = self._data.get("tables") or {}

    def get_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table configuration by name."""
        return self._tables.get(table_name)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table is registered."""
        return table_name in self._tables

    def get_min_rows(self, table_name: str) -> int:
        """Get minimum row count contract for a table."""
        table = self.get_table(table_name)
        if table is None:
            return 0
        return table.get("min_rows", 0)

    def get_freshness_hours(self, table_name: str) -> int:
        """Get freshness contract for a table. Returns 0 if not set (freshness check disabled)."""
        table = self.get_table(table_name)
        if table is None:
            return 0
        return table.get("freshness_hours", 0)

    def get_execution_freshness_hours(self, table_name: str, default: int) -> int:
        """Get the max execution-age (run-time freshness) threshold for a table.

        This is distinct from ``freshness_hours`` (data-content freshness via
        ``freshness_column``): it bounds how recently the table's producing job
        must have *run*. Returns the per-table ``execution_freshness_hours``
        override when present, otherwise the supplied ``default``.
        """
        table = self.get_table(table_name)
        if table is None:
            return default
        return table.get("execution_freshness_hours", default)

    def get_required_columns(self, table_name: str) -> List[str]:
        """Get required columns for a table."""
        table = self.get_table(table_name)
        if table is None:
            return []
        schema = table.get("schema", {})
        return schema.get("required_columns", [])

    def get_freshness_column(self, table_name: str) -> str:
        """Get the column name used for freshness checks. Empty string if not set."""
        table = self.get_table(table_name)
        if table is None:
            return ""
        return table.get("freshness_column", "")

    def get_min_rows_date_check(self, table_name: str) -> Optional[int]:
        """Get business-day offset for row-count date check.
        Returns None when not set, meaning the notebook should use the latest available date.
        Returns an int (e.g. -2) to filter rows for today + offset business days.
        """
        table = self.get_table(table_name)
        if table is None:
            return None
        return table.get("min_rows_date_check", None)

    def get_row_count_mode(self, table_name: str) -> Optional[str]:
        """Get explicit row count mode.

        Allowed values:
            - "business_offset": use min_rows_date_check with freshness_column
            - "latest_available_date": use MAX(freshness_column)
            - "total": use total row count

        Returns None when not configured.
        """
        table = self.get_table(table_name)
        if table is None:
            return None
        return table.get("row_count_mode", None)

    def get_not_null_columns(self, table_name: str) -> List[str]:
        """Get columns that must not contain NULL values. Empty list if none defined."""
        table = self.get_table(table_name)
        if table is None:
            return []
        return table.get("not_null_columns", [])

    def get_custom_checks(self, table_name: str) -> List[Dict[str, Any]]:
        """Get custom check definitions for a table. Empty list if none defined."""
        table = self.get_table(table_name)
        if table is None:
            return []
        return table.get("checks", [])

    def all_table_names(self) -> List[str]:
        """Return all registered table names."""
        return list(self._tables.keys())


class TableLineage:
    """
    Represents table_lineage.yml.
    Lineage is stored downstream-only.
    Upstream is computed at runtime by graph inversion.
    """

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, TABLE_LINEAGE_FILE)
        self._data = _load_yaml(self._path)
        # `or {}` coalesces a present-but-empty `lineage:` section (None) to {} —
        # this is what lets a brand-new onboarding skeleton no-op instead of crash.
        self._lineage = self._data.get("lineage") or {}
        self._upstream_index: Optional[Dict[str, List[str]]] = None

    def get_downstream(self, table_name: str) -> List[str]:
        """Get direct downstream tables for a given table."""
        entry = self._lineage.get(table_name, {})
        # Return a copy so callers cannot mutate the parsed lineage state.
        return list(entry.get("downstream", []))

    def has_entry(self, table_name: str) -> bool:
        """Check if a table has a lineage entry."""
        return table_name in self._lineage

    def all_tables_in_lineage(self) -> Set[str]:
        """Return all tables mentioned in lineage (sources + targets)."""
        tables = set(self._lineage.keys())
        for entry in self._lineage.values():
            for ds in entry.get("downstream", []):
                tables.add(ds)
        return tables

    def _build_upstream_index(self) -> Dict[str, List[str]]:
        """
        Invert the downstream-only graph to compute upstream relationships.
        If A.downstream contains B, then B.upstream includes A.
        """
        upstream: Dict[str, List[str]] = {}
        for source, entry in self._lineage.items():
            for target in entry.get("downstream", []):
                if target not in upstream:
                    upstream[target] = []
                upstream[target].append(source)
        return upstream

    def _build_adjacency(self) -> Dict[str, List[str]]:
        """Return a de-duplicated adjacency list for all lineage nodes."""
        adjacency: Dict[str, List[str]] = {}
        for table in self.all_tables_in_lineage():
            adjacency[table] = []

        for source, entry in self._lineage.items():
            seen = set()
            for target in entry.get("downstream", []):
                if target not in seen:
                    adjacency[source].append(target)
                    seen.add(target)

        return adjacency

    def find_cycle(self) -> Optional[List[str]]:
        """
        Detect a cycle in the lineage graph.

        Returns:
            A cycle path like [A, B, C, A] when found, otherwise None.
        """
        adjacency = self._build_adjacency()
        visit_state: Dict[str, int] = {}
        stack: List[str] = []
        stack_index: Dict[str, int] = {}

        def dfs(node: str) -> Optional[List[str]]:
            visit_state[node] = 1
            stack_index[node] = len(stack)
            stack.append(node)

            for neighbor in adjacency.get(node, []):
                neighbor_state = visit_state.get(neighbor, 0)
                if neighbor_state == 0:
                    cycle = dfs(neighbor)
                    if cycle is not None:
                        return cycle
                elif neighbor_state == 1:
                    cycle_start = stack_index[neighbor]
                    return stack[cycle_start:] + [neighbor]

            stack.pop()
            stack_index.pop(node, None)
            visit_state[node] = 2
            return None

        for node in sorted(adjacency.keys()):
            if visit_state.get(node, 0) == 0:
                cycle = dfs(node)
                if cycle is not None:
                    return cycle

        return None

    def ensure_valid_dag(self) -> None:
        """Raise when lineage is not a valid DAG."""
        cycle = self.find_cycle()
        if cycle is not None:
            cycle_text = " -> ".join(cycle)
            raise MetadataValidationError(
                f"Lineage graph is not a valid DAG. Cycle detected: {cycle_text}"
            )

    def get_upstream(self, table_name: str) -> List[str]:
        """Get direct upstream tables for a given table (computed by inversion)."""
        if self._upstream_index is None:
            self._upstream_index = self._build_upstream_index()
        # Return a copy so callers cannot mutate the cached upstream index.
        return list(self._upstream_index.get(table_name, []))

    def expand_downstream(self, start_tables: Set[str], subset_tables: Set[str]) -> Set[str]:
        """
        BFS expansion of downstream tables, stopping at non-subset boundaries.
        """
        self.ensure_valid_dag()
        visited = set()
        queue = list(start_tables)
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for ds in self.get_downstream(current):
                if ds in subset_tables and ds not in visited:
                    queue.append(ds)
                else:
                    if ds not in subset_tables:
                        print(f"{LOG_PREFIX_SKIP} Downstream traversal stopped at non-subset table: '{ds}'")
        return visited

    def expand_upstream(self, start_tables: Set[str], subset_tables: Set[str]) -> Set[str]:
        """
        BFS expansion of upstream tables, stopping at non-subset boundaries.
        """
        self.ensure_valid_dag()
        visited = set()
        queue = list(start_tables)
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            for us in self.get_upstream(current):
                if us in subset_tables and us not in visited:
                    queue.append(us)
                else:
                    if us not in subset_tables:
                        print(f"{LOG_PREFIX_SKIP} Upstream traversal stopped at non-subset table: '{us}'")
        return visited

    def expand_scope(
        self,
        declared_tables: Set[str],
        scope_mode: str,
        subset_tables: Set[str]
    ) -> Set[str]:
        """
        Expand declared tables based on scope mode, constrained to subset.

        Args:
            declared_tables: Tables declared in PR.
            scope_mode: One of affected_only, upstream, downstream, full_lineage.
            subset_tables: Set of all tables in subset scope.

        Returns:
            Expanded set of tables to validate.
        """
        if scope_mode == "affected_only":
            return declared_tables

        if scope_mode == "downstream":
            return self.expand_downstream(declared_tables, subset_tables)

        if scope_mode == "upstream":
            return self.expand_upstream(declared_tables, subset_tables)

        if scope_mode == "full_lineage":
            downstream = self.expand_downstream(declared_tables, subset_tables)
            upstream = self.expand_upstream(declared_tables, subset_tables)
            return downstream | upstream

        print(f"{LOG_PREFIX_WARNING} Unknown scope mode '{scope_mode}', defaulting to affected_only.")
        return declared_tables


class NotebookOutputs:
    """Represents notebook_outputs.yml (Phase 2 readiness)."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, NOTEBOOK_OUTPUTS_FILE)
        if os.path.exists(self._path):
            self._data = _load_yaml(self._path)
        else:
            self._data = {}
        self._notebooks = self._data.get("notebook_outputs", {})

    def get_output_tables(self, notebook_path: str) -> List[str]:
        """Get output tables for a notebook."""
        entry = self._notebooks.get(notebook_path, {})
        return entry.get("tables", [])

    def find_notebook_for_table(self, table_name: str) -> Optional[str]:
        """Find the notebook that produces a given table. Returns None if not found."""
        for nb_name, entry in self._notebooks.items():
            if table_name in entry.get("tables", []):
                return nb_name
        return None


class PipelineOutputs:
    """Represents pipeline_outputs.yml (Phase 2 readiness)."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, PIPELINE_OUTPUTS_FILE)
        if os.path.exists(self._path):
            self._data = _load_yaml(self._path)
        else:
            self._data = {}
        self._pipelines = self._data.get("pipeline_outputs", {})

    def get_output_tables(self, pipeline_name: str) -> List[str]:
        """Get output tables for a pipeline."""
        entry = self._pipelines.get(pipeline_name, {})
        return entry.get("tables", [])

    def find_pipeline_for_table(self, table_name: str) -> Optional[str]:
        """Find the pipeline that produces a given table. Returns None if not found."""
        for pl_name, entry in self._pipelines.items():
            if table_name in entry.get("tables", []):
                return pl_name
        return None


class AdvancedChecks:
    """Represents advanced_checks.yml — registry of advanced check notebooks."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, ADVANCED_CHECKS_FILE)
        if os.path.exists(self._path):
            self._data = _load_yaml(self._path)
        else:
            self._data = {}
        self._checks = self._data.get("advanced_checks", [])

    def all_checks(self) -> List[Dict[str, Any]]:
        """Return all advanced check definitions."""
        return list(self._checks)

    def enabled_checks(self) -> List[Dict[str, Any]]:
        """Return only enabled advanced check definitions."""
        return [c for c in self._checks if c.get("enabled", True)]

    def get_check(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a single advanced check definition by name."""
        for c in self._checks:
            if c.get("name") == name:
                return c
        return None

    def get_tables(self, name: str) -> List[str]:
        """Get the list of tables a check reads."""
        check = self.get_check(name)
        if check is None:
            return []
        return check.get("tables", [])

    def get_notebook_name(self, name: str) -> str:
        """Get the Fabric notebook display name for a check."""
        check = self.get_check(name)
        if check is None:
            return ""
        return check.get("notebook_name", "")

    def get_notebook_id(self, name: str) -> str:
        """Get the Fabric notebook item ID fallback for a check."""
        check = self.get_check(name)
        if check is None:
            return ""
        return check.get("notebook_id", "")

    def get_timeout(self, name: str) -> int:
        """Get per-check timeout, falling back to global default."""
        check = self.get_check(name)
        if check is None:
            return ADVANCED_CHECK_TIMEOUT_SEC
        return check.get("timeout_sec", ADVANCED_CHECK_TIMEOUT_SEC)

    def filter_by_tables(self, affected_tables: Set[str]) -> List[Dict[str, Any]]:
        """Return enabled checks whose tables overlap with the affected set."""
        result = []
        for check in self.enabled_checks():
            check_tables = set(check.get("tables", []))
            if check_tables & affected_tables:
                result.append(check)
        return result


class ExecutionFreshness:
    """Represents execution_freshness.yml — freshness gating configuration."""

    def __init__(self, metadata_path: str = METADATA_PATH):
        self._path = os.path.join(metadata_path, EXECUTION_FRESHNESS_FILE)
        if os.path.exists(self._path):
            self._data = _load_yaml(self._path)
        else:
            self._data = {}
        self._gating = self._data.get("freshness_gating", {})

    @property
    def enabled(self) -> bool:
        return self._gating.get("enabled", False)

    @property
    def mode(self) -> str:
        return self._gating.get("mode", DEFAULT_FRESHNESS_MODE)

    @property
    def tolerance_minutes(self) -> int:
        return self._gating.get("tolerance_minutes", DEFAULT_TOLERANCE_MINUTES)

    @property
    def exempt_tables(self) -> List[str]:
        return self._gating.get("exempt_tables", [])

    @property
    def default_max_age_hours(self) -> int:
        """Org-wide fallback execution-freshness threshold (hours).

        Used by the freshness gate when a table has no per-table
        ``execution_freshness_hours`` override in the registry. Falls back to
        the module default when the config key is absent.
        """
        return self._gating.get("default_max_age_hours", DEFAULT_STALENESS_MAX_AGE_HOURS)



# ===========================================================================
# Metadata shape (schema) validation
# ===========================================================================

# Structural schemas for the three core metadata files. These intentionally
# validate SHAPE only (top-level keys, types, and — for subset — unknown-key
# typos). They deliberately accept empty onboarding skeletons such as a file
# containing only `lineage:` (which parses to {"lineage": None}), while still
# rejecting missing top-level keys, wrong types, and misspelled keys.

SUBSET_SCHEMA = {
    "type": "object",
    "required": ["subset"],
    "properties": {
        "subset": {
            "type": "object",
            "required": ["enabled"],
            "properties": {
                "enabled": {"type": "boolean"},
                "tables": {"type": ["array", "null"], "items": {"type": "string"}},
                "notebooks": {"type": ["array", "null"], "items": {"type": "string"}},
                "pipelines": {"type": ["array", "null"], "items": {"type": "string"}},
            },
            "additionalProperties": False,
        }
    },
    "additionalProperties": True,
}

REGISTRY_SCHEMA = {
    "type": "object",
    "required": ["tables"],
    # Per-table entry internals are intentionally left unconstrained so that the
    # rich, evolving contract fields (min_rows, schema, checks, ...) are not
    # coupled to this shape check.
    "properties": {"tables": {"type": ["object", "null"]}},
    "additionalProperties": True,
}

LINEAGE_SCHEMA = {
    "type": "object",
    "required": ["lineage"],
    "properties": {
        "lineage": {
            "type": ["object", "null"],
            "additionalProperties": {
                "type": ["object", "null"],
                "properties": {
                    "downstream": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                    }
                },
                "additionalProperties": True,
            },
        }
    },
    "additionalProperties": True,
}


def validate_metadata_shapes(metadata_path: str = METADATA_PATH) -> List[str]:
    """Validate the structural shape of the three core metadata files.

    Accepts empty onboarding skeletons (e.g. a file containing only `lineage:`)
    but rejects missing top-level keys, wrong types, and unknown keys (typos).

    Returns:
        A list of human-readable error messages. Empty list means all files
        are structurally valid.
    """
    import jsonschema  # local import: only required when shape validation runs

    checks = [
        (SUBSET_SCOPE_FILE, SUBSET_SCHEMA),
        (TABLE_REGISTRY_FILE, REGISTRY_SCHEMA),
        (TABLE_LINEAGE_FILE, LINEAGE_SCHEMA),
    ]

    errors: List[str] = []
    for filename, schema in checks:
        path = os.path.join(metadata_path, filename)
        try:
            data = _load_yaml(path)
        except MetadataLoadError as e:
            errors.append(str(e))
            continue
        validator = jsonschema.Draft7Validator(schema)
        for err in sorted(validator.iter_errors(data), key=lambda e: list(e.path)):
            location = "/".join(str(p) for p in err.path) or "<root>"
            errors.append(
                "{}: schema violation at '{}': {}".format(filename, location, err.message)
            )
    return errors


def validate_metadata_consistency(
    subset: SubsetScope,
    registry: TableRegistry,
    lineage: TableLineage
) -> List[str]:
    """
    Cross-validate all metadata files for consistency.

    Returns:
        List of error messages. Empty list means all checks passed.
    """
    errors = []

    if not subset.enabled:
        print(f"{LOG_PREFIX_INFO} Subset validation is disabled (subset.enabled=false). Skipping consistency checks.")
        return errors

    # Check 1: Every subset table must be in the registry
    for table in subset.tables:
        if not registry.table_exists(table):
            errors.append(f"Subset table '{table}' is not registered in {TABLE_REGISTRY_FILE}.")

    # Check 2: Every subset table must have a lineage entry
    for table in subset.tables:
        if not lineage.has_entry(table):
            errors.append(
                f"Subset table '{table}' has no entry in {TABLE_LINEAGE_FILE}. "
                f"Every subset table must declare downstream (even if empty list)."
            )

    # Check 3: Downstream references must point to registered tables
    for table in subset.tables:
        for ds in lineage.get_downstream(table):
            if not registry.table_exists(ds):
                errors.append(
                    f"Downstream table '{ds}' (referenced from '{table}') is not registered in {TABLE_REGISTRY_FILE}."
                )

    # Check 4: Lineage must be a valid DAG for pipeline-style dependencies
    cycle = lineage.find_cycle()
    if cycle is not None:
        errors.append(
            "Lineage graph is not a valid DAG. Cycle detected: " + " -> ".join(cycle)
        )

    return errors


def collect_contract_warnings(
    registry: TableRegistry,
    tables: Iterable[str],
) -> List[str]:
    """
    Collect non-blocking, per-table contract warnings for a set of tables.

    These are advisory only (never failures): they flag weak or internally
    inconsistent table contracts that would reduce the usefulness of the
    Phase 2 data-presence checks. This is the single, central place to add
    future per-table contract checks.

    Returns:
        A sorted, de-duplicated list of human-readable warning messages
        (empty list = no warnings).
    """
    warnings: List[str] = []
    for table in sorted(set(tables)):
        min_rows = registry.get_min_rows(table)
        required_cols = registry.get_required_columns(table)
        freshness_hours = registry.get_freshness_hours(table)
        freshness_column = registry.get_freshness_column(table)

        if min_rows == 0:
            warnings.append(
                f"Table '{table}' has min_rows=0. Consider setting a meaningful threshold."
            )
        if not required_cols:
            warnings.append(
                f"Table '{table}' has no required_columns defined. "
                f"Consider adding schema validation."
            )
        if freshness_hours > 0 and not freshness_column:
            warnings.append(
                f"Table '{table}' sets freshness_hours={freshness_hours} but no "
                f"freshness_column; the freshness check cannot run without a column."
            )
    return warnings
