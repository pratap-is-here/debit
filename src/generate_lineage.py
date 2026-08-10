# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Automatic lineage generation for subset-scoped tables.

Algorithm:
    1. Scan all .DataPipeline folders for pipeline-content.json.
    2. Extract notebookId and config_obj_name from TridentNotebook activities.
    3. Scan all .Notebook folders for .platform (logicalId) and notebook-content.py.
    4. Map notebookId -> logicalId -> notebook path -> written tables.
    5. Parse notebook source code for table write operations (Level 2).
    6. Cross-validate config_obj_name vs notebook-detected writes.
    7. Build lineage graph by analyzing read/write patterns.
    8. Filter to subset scope.
    9. Output table_lineage.yml, table_registry.yml, notebook_outputs.yml, pipeline_outputs.yml.

Usage:
    python generate_lineage.py --metadata_path <path> --notebooks_root <path> --pipelines_root <path>

    Or rely on environment variables / defaults from constants.py.
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# Allow importing from same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    ACTIVITY_TYPE_NOTEBOOK,
    DEFAULT_FRESHNESS_HOURS,
    DEFAULT_MIN_ROWS,
    LOG_PREFIX_ERROR,
    LOG_PREFIX_INFO,
    LOG_PREFIX_START,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
    METADATA_PATH,
    NOTEBOOK_CONTENT_FILE,
    NOTEBOOK_FOLDER_SUFFIX,
    NOTEBOOK_OUTPUTS_FILE,
    NOTEBOOKS_ROOT,
    PIPELINE_CONTENT_FILE,
    PIPELINE_FOLDER_SUFFIX,
    PIPELINE_OUTPUTS_FILE,
    PIPELINES_ROOT,
    PLATFORM_FILE,
    SUBSET_SCOPE_FILE,
    TABLE_LINEAGE_FILE,
    TABLE_REGISTRY_FILE,
)

# ===========================================================================
# Regex patterns for detecting table writes in notebook code
# ===========================================================================

# SQL-based write patterns
SQL_CREATE_TABLE = re.compile(
    r"""(?:CREATE|REPLACE)\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\s+"""
    r"""(?:IF\s+NOT\s+EXISTS\s+)?[`"']?(\w+(?:\.\w+)*)[`"']?""",
    re.IGNORECASE,
)
SQL_INSERT_INTO = re.compile(
    r"""INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?[`"']?(\w+(?:\.\w+)*)[`"']?""",
    re.IGNORECASE,
)
SQL_MERGE_INTO = re.compile(
    r"""MERGE\s+INTO\s+[`"']?(\w+(?:\.\w+)*)[`"']?""",
    re.IGNORECASE,
)
SQL_DROP_CREATE = re.compile(
    r"""DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[`"']?(\w+(?:\.\w+)*)[`"']?""",
    re.IGNORECASE,
)

# DataFrame write patterns (f? handles f-string prefixes like saveAsTable(f"..."))
DF_SAVE_AS_TABLE = re.compile(
    r"""\.saveAsTable\s*\(\s*f?["'](\w+(?:\.\w+)*)["']""",
    re.IGNORECASE,
)
DF_INSERT_INTO = re.compile(
    r"""\.insertInto\s*\(\s*f?["'](\w+(?:\.\w+)*)["']""",
    re.IGNORECASE,
)
DF_WRITE_TABLE = re.compile(
    r"""\.write\s*(?:\.\w+\s*\([^)]*\)\s*)*\.saveAsTable\s*\(\s*f?["'](\w+(?:\.\w+)*)["']""",
    re.IGNORECASE,
)

# SQL-based read patterns (for lineage inference)
SQL_SELECT_FROM = re.compile(
    r"""(?:FROM|JOIN)\s+[`"']?(\w+(?:\.\w+)*)[`"']?""",
    re.IGNORECASE,
)
DF_READ_TABLE = re.compile(
    r"""spark\.(?:read\.)?table\s*\(\s*f?["'](\w+(?:\.\w+)*)["']""",
    re.IGNORECASE,
)
DF_SQL_READ = re.compile(
    r"""spark\.sql\s*\(\s*(?:f?["']{1,3})(.*?)(?:["']{1,3})\s*\)""",
    re.IGNORECASE | re.DOTALL,
)

# Path-based write patterns (for file outputs like onelake paths)
PATH_WRITE = re.compile(
    r"""\.(?:save|write|csv|parquet|json)\s*\(\s*f?["'](Files/[^"']+|abfss://[^"']+)["']""",
    re.IGNORECASE,
)

# f-string DATABASE reference patterns:
#   {DATABASE['SILVER_LAYER']}.table_name -> captures (layer, table_name)
FSTRING_DB_REF = re.compile(
    r"""\{DATABASE\[['"]([\w]+)['"]\]\}\.(\w+)""",
)

# Known Python module prefixes — filter these from FROM/JOIN read detection
PYTHON_MODULE_PREFIXES = {
    "pyspark", "sklearn", "pandas", "numpy", "scipy", "matplotlib",
    "datetime", "dateutil", "typing", "collections", "os", "sys",
    "json", "re", "math", "time", "functools", "itertools",
    "notebookutils", "imblearn", "flask", "statsmodels", "pytz",
    "requests", "urllib", "abc", "copy", "io", "logging",
}

ALL_WRITE_PATTERNS = [
    SQL_CREATE_TABLE,
    SQL_INSERT_INTO,
    SQL_MERGE_INTO,
    SQL_DROP_CREATE,
    DF_SAVE_AS_TABLE,
    DF_INSERT_INTO,
    DF_WRITE_TABLE,
    PATH_WRITE,
]

ALL_READ_PATTERNS = [
    SQL_SELECT_FROM,
    DF_READ_TABLE,
]


# ===========================================================================
# Data structures
# ===========================================================================

class NotebookInfo:
    """Parsed information about a single notebook."""
    def __init__(self):
        self.folder_name: str = ""
        self.folder_path: str = ""
        self.logical_id: str = ""
        self.display_name: str = ""
        self.written_tables: Set[str] = set()
        self.read_tables: Set[str] = set()
        self.file_outputs: Set[str] = set()  # path-based outputs


class PipelineActivity:
    """A single notebook activity extracted from a pipeline."""
    def __init__(self):
        self.activity_name: str = ""
        self.notebook_id: str = ""
        self.config_obj_name: Optional[str] = None
        self.parameters: Dict[str, Any] = {}


class PipelineInfo:
    """Parsed information about a single pipeline."""
    def __init__(self):
        self.folder_name: str = ""
        self.folder_path: str = ""
        self.logical_id: str = ""
        self.display_name: str = ""
        self.notebook_activities: List[PipelineActivity] = []
        self.invoked_pipeline_ids: List[str] = []


class ActivityUnit:
    """Represents one notebook invocation with its associated read and write tables."""
    def __init__(self, label: str = ""):
        self.label = label
        self.reads: Set[str] = set()
        self.writes: Set[str] = set()


# ===========================================================================
# Parsing functions
# ===========================================================================

def parse_platform_file(platform_path: str) -> Tuple[str, str]:
    """
    Parse a .platform file to extract logicalId and displayName.

    Returns:
        Tuple of (logicalId, displayName).
    """
    with open(platform_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logical_id = data.get("config", {}).get("logicalId", "")
    display_name = data.get("metadata", {}).get("displayName", "")
    return logical_id, display_name


def _strip_comments(code: str) -> str:
    """Strip Python line comments to avoid false positives from commented-out code."""
    lines = code.split('\n')
    result = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith('#'):
            result.append('')
        else:
            result.append(line)
    return '\n'.join(result)


def _resolve_fstring_paths(code: str) -> Set[str]:
    """
    Resolve f-string file paths by finding variable assignments and substituting.
    Handles patterns like: root_container_path = f"Files/exports/sales/"
    Then: .csv(f"{root_container_path}orders/daily_sales_csv/...")
    """
    resolved: Set[str] = set()

    # Find path variable assignments: var = f?"Files/..."  or  var = "Files/..."
    path_vars: Dict[str, str] = {}
    var_pattern = re.compile(
        r"""(\w+)\s*=\s*f?["']((?:Files|abfss)[^"']*?)["']""",
    )
    for match in var_pattern.finditer(code):
        var_name = match.group(1)
        var_value = match.group(2)
        path_vars[var_name] = var_value

    # Find .csv/.parquet/.json writes using f-string with variable prefix
    fstring_write = re.compile(
        r"""\.(?:csv|parquet|json)\s*\(\s*f["']\{(\w+)\}([^"']*?)["']""",
        re.IGNORECASE,
    )
    for match in fstring_write.finditer(code):
        var_ref = match.group(1)
        suffix = match.group(2)
        if var_ref in path_vars:
            full_path = path_vars[var_ref] + suffix
            resolved.add(full_path)

    return resolved


def parse_notebook_code(content_path: str) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Parse notebook-content.py for table read/write operations.

    Returns:
        Tuple of (written_tables, read_tables, file_outputs).
    """
    written_tables: Set[str] = set()
    read_tables: Set[str] = set()
    file_outputs: Set[str] = set()

    try:
        with open(content_path, "r", encoding="utf-8") as f:
            code = f.read()
    except Exception as e:
        print(f"{LOG_PREFIX_WARNING} Could not read notebook content at '{content_path}': {e}")
        return written_tables, read_tables, file_outputs

    # Strip comments to avoid false positives from commented-out code
    code = _strip_comments(code)

    # Detect writes
    for pattern in ALL_WRITE_PATTERNS:
        for match in pattern.finditer(code):
            table_name = match.group(1).strip()
            if table_name.startswith("Files/") or table_name.startswith("abfss://"):
                file_outputs.add(table_name)
            else:
                # Exclude common false positives
                if table_name.lower() not in ("table", "true", "false", "none", "null"):
                    written_tables.add(table_name)

    # Resolve f-string variable paths (e.g., f"{root_container_path}...")
    file_outputs.update(_resolve_fstring_paths(code))

    # Detect reads from direct table references
    for pattern in ALL_READ_PATTERNS:
        for match in pattern.finditer(code):
            table_name = match.group(1).strip()
            if table_name.lower() not in ("table", "true", "false", "none", "null", "select"):
                read_tables.add(table_name)

    # Detect reads from spark.sql() calls (extract FROM/JOIN targets)
    for match in DF_SQL_READ.finditer(code):
        sql_text = match.group(1)
        for from_match in SQL_SELECT_FROM.finditer(sql_text):
            table_name = from_match.group(1).strip()
            if table_name.lower() not in ("table", "true", "false", "none", "null", "select", "dual"):
                read_tables.add(table_name)

    # Detect f-string DATABASE references: {DATABASE['SILVER_LAYER']}.table_name
    for match in FSTRING_DB_REF.finditer(code):
        layer = match.group(1).strip()
        table_name = match.group(2).strip()
        if table_name.lower() not in ("table", "true", "false", "none", "null"):
            if layer == "SILVER_LAYER":
                qualified = f"silver.{table_name}"
            elif layer == "GOLD_LAYER":
                qualified = f"gold.{table_name}"
            else:
                qualified = table_name
            read_tables.add(qualified)

    # Filter out Python module imports from reads
    filtered_reads: Set[str] = set()
    for table_name in read_tables:
        first_segment = table_name.split(".")[0].lower()
        if first_segment in PYTHON_MODULE_PREFIXES:
            continue
        filtered_reads.add(table_name)
    read_tables = filtered_reads

    return written_tables, read_tables, file_outputs


def scan_notebooks(notebooks_root: str) -> Dict[str, NotebookInfo]:
    """
    Scan all .Notebook folders and extract metadata + code analysis.

    Returns:
        Dict mapping logicalId -> NotebookInfo.
    """
    notebooks: Dict[str, NotebookInfo] = {}

    if not os.path.isdir(notebooks_root):
        print(f"{LOG_PREFIX_WARNING} Notebooks root not found: {notebooks_root}")
        return notebooks

    for item in os.listdir(notebooks_root):
        item_path = os.path.join(notebooks_root, item)
        if not os.path.isdir(item_path):
            continue
        if not item.endswith(NOTEBOOK_FOLDER_SUFFIX):
            continue

        platform_path = os.path.join(item_path, PLATFORM_FILE)
        content_path = os.path.join(item_path, NOTEBOOK_CONTENT_FILE)

        if not os.path.exists(platform_path):
            print(f"{LOG_PREFIX_WARNING} No .platform file in '{item}' - skipping.")
            continue

        nb = NotebookInfo()
        nb.folder_name = item
        nb.folder_path = item_path

        try:
            nb.logical_id, nb.display_name = parse_platform_file(platform_path)
        except Exception as e:
            print(f"{LOG_PREFIX_ERROR} Failed to parse .platform for '{item}': {e}")
            continue

        if not nb.logical_id:
            print(f"{LOG_PREFIX_WARNING} No logicalId in .platform for '{item}' — skipping.")
            continue

        if os.path.exists(content_path):
            nb.written_tables, nb.read_tables, nb.file_outputs = parse_notebook_code(content_path)
            print(
                f"{LOG_PREFIX_INFO} Notebook '{nb.display_name}': "
                f"writes={nb.written_tables}, reads={nb.read_tables}, files={nb.file_outputs}"
            )
        else:
            print(f"{LOG_PREFIX_WARNING} No {NOTEBOOK_CONTENT_FILE} in '{item}'.")

        notebooks[nb.logical_id] = nb

    print(f"{LOG_PREFIX_SUCCESS} Scanned {len(notebooks)} notebooks.")
    return notebooks


def parse_pipeline_content(content_path: str) -> Tuple[List[PipelineActivity], List[str]]:
    """
    Parse pipeline-content.json to extract notebook activities and invoked pipelines.

    Returns:
        Tuple of (notebook_activities, invoked_pipeline_ids).
    """
    notebook_activities: List[PipelineActivity] = []
    invoked_pipeline_ids: List[str] = []

    try:
        with open(content_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"{LOG_PREFIX_ERROR} Failed to parse pipeline content '{content_path}': {e}")
        return notebook_activities, invoked_pipeline_ids

    properties = data.get("properties", {})
    activities = properties.get("activities", [])

    for activity in activities:
        activity_type = activity.get("type", "")
        type_props = activity.get("typeProperties", {})

        if activity_type == ACTIVITY_TYPE_NOTEBOOK:
            pa = PipelineActivity()
            pa.activity_name = activity.get("name", "")
            pa.notebook_id = type_props.get("notebookId", "")
            pa.parameters = type_props.get("parameters", {})

            # Extract config_obj_name if present
            config_obj = pa.parameters.get("config_obj_name", {})
            if isinstance(config_obj, dict):
                value = config_obj.get("value", None)
                # Handle expression values
                if isinstance(value, dict):
                    # Expression-type value — cannot resolve statically
                    print(
                        f"{LOG_PREFIX_WARNING} config_obj_name for activity "
                        f"'{pa.activity_name}' is an expression - cannot resolve statically."
                    )
                    pa.config_obj_name = None
                elif isinstance(value, str) and value:
                    pa.config_obj_name = value
            elif isinstance(config_obj, str):
                pa.config_obj_name = config_obj

            notebook_activities.append(pa)

        elif activity_type == "InvokePipeline":
            pipeline_id = type_props.get("pipelineId", "")
            if pipeline_id:
                invoked_pipeline_ids.append(pipeline_id)

    return notebook_activities, invoked_pipeline_ids


def scan_pipelines(pipelines_root: str) -> Dict[str, PipelineInfo]:
    """
    Scan all .DataPipeline folders and extract pipeline metadata.

    Returns:
        Dict mapping logicalId -> PipelineInfo.
    """
    pipelines: Dict[str, PipelineInfo] = {}

    if not os.path.isdir(pipelines_root):
        print(f"{LOG_PREFIX_WARNING} Pipelines root not found: {pipelines_root}")
        return pipelines

    for item in os.listdir(pipelines_root):
        item_path = os.path.join(pipelines_root, item)
        if not os.path.isdir(item_path):
            continue
        if not item.endswith(PIPELINE_FOLDER_SUFFIX):
            continue

        platform_path = os.path.join(item_path, PLATFORM_FILE)
        content_path = os.path.join(item_path, PIPELINE_CONTENT_FILE)

        if not os.path.exists(platform_path):
            print(f"{LOG_PREFIX_WARNING} No .platform file in pipeline '{item}' - skipping.")
            continue
        if not os.path.exists(content_path):
            print(f"{LOG_PREFIX_WARNING} No {PIPELINE_CONTENT_FILE} in pipeline '{item}' - skipping.")
            continue

        pl = PipelineInfo()
        pl.folder_name = item
        pl.folder_path = item_path

        try:
            pl.logical_id, pl.display_name = parse_platform_file(platform_path)
        except Exception as e:
            print(f"{LOG_PREFIX_ERROR} Failed to parse .platform for pipeline '{item}': {e}")
            continue

        if not pl.logical_id:
            print(f"{LOG_PREFIX_WARNING} No logicalId in .platform for pipeline '{item}' - skipping.")
            continue

        pl.notebook_activities, pl.invoked_pipeline_ids = parse_pipeline_content(content_path)
        print(
            f"{LOG_PREFIX_INFO} Pipeline '{pl.display_name}': "
            f"{len(pl.notebook_activities)} notebook activities, "
            f"{len(pl.invoked_pipeline_ids)} invoked pipelines."
        )

        pipelines[pl.logical_id] = pl

    print(f"{LOG_PREFIX_SUCCESS} Scanned {len(pipelines)} pipelines.")
    return pipelines


# ===========================================================================
# Lineage inference engine
# ===========================================================================

def _match_table_name(candidate: str, subset_tables: Set[str]) -> Optional[str]:
    """
    Attempt to match a candidate table name against the subset tables.

    Matching strategies (in order):
        1. Exact match.
        2. Candidate matches the last segment of a qualified name
           (e.g., 'daily_sales_summary' matches 'gold.daily_sales_summary').
        3. Candidate is a path that matches a subset file output.

    Returns:
        The matched subset table name, or None.
    """
    # Exact match
    if candidate in subset_tables:
        return candidate

    # Match by last segment (handle schema.table or db.table format)
    for st in subset_tables:
        parts = st.split(".")
        if len(parts) > 1 and parts[-1] == candidate:
            return st
        # Also match if candidate has schema prefix and subset doesn't
        candidate_parts = candidate.split(".")
        if len(candidate_parts) > 1 and candidate_parts[-1] == parts[-1]:
            return st

    # Path-based match (for file outputs)
    # e.g., candidate = "Files/exports/sales/daily_sales_csv" matches subset entry
    for st in subset_tables:
        if st.startswith("Files/") and candidate.startswith("Files/"):
            # Normalize paths and compare
            norm_c = os.path.normpath(candidate).replace("\\", "/")
            norm_s = os.path.normpath(st).replace("\\", "/")
            if norm_c == norm_s:
                return st
            # Prefix match — candidate path starts with subset path (e.g., with /rundate= suffix)
            if norm_c.startswith(norm_s + "/") or norm_c.startswith(norm_s):
                return st
            if norm_s.startswith(norm_c + "/") or norm_s.startswith(norm_c):
                return st
            # Partial path match — candidate contains the key path segment
            if st.endswith(candidate.split("/")[-1]) or candidate.endswith(st.split("/")[-1]):
                return st

    return None


def build_table_mappings(
    notebooks: Dict[str, NotebookInfo],
    pipelines: Dict[str, PipelineInfo],
    subset_tables: Set[str],
) -> Tuple[
    Dict[str, Set[str]],
    Dict[str, Set[str]],
    Dict[str, Set[str]],
    Dict[str, Set[str]],
    List[str],
    List[ActivityUnit],
]:
    """
    Build the mapping of which tables each notebook/pipeline reads and writes.

    Uses 2-level detection:
        Level 1: config_obj_name from pipeline (authoritative)
        Level 2: Notebook code parsing (supplementary / cross-validation)

    Returns:
        Tuple of:
            - notebook_writes: logicalId -> set of written tables
            - notebook_reads: logicalId -> set of read tables
            - pipeline_writes: pipeline logicalId -> set of written tables
            - pipeline_reads: pipeline logicalId -> set of read tables
            - ignored_tables: list of table names excluded because not in subset
            - activity_units: list of ActivityUnit for accurate lineage inference
    """
    notebook_writes: Dict[str, Set[str]] = defaultdict(set)
    notebook_reads: Dict[str, Set[str]] = defaultdict(set)
    pipeline_writes: Dict[str, Set[str]] = defaultdict(set)
    pipeline_reads: Dict[str, Set[str]] = defaultdict(set)
    ignored_tables: List[str] = []
    activity_units: List[ActivityUnit] = []

    for pl_id, pl in pipelines.items():
        for activity in pl.notebook_activities:
            notebook_id = activity.notebook_id
            config_name = activity.config_obj_name

            # Create an activity unit for this specific notebook invocation
            unit = ActivityUnit(
                label=f"{pl.display_name}/{activity.activity_name}"
            )

            # Level 1: config_obj_name
            if config_name:
                # Attempt to match config_obj_name to subset tables
                matched = _match_table_name(config_name, subset_tables)
                if matched:
                    pipeline_writes[pl_id].add(matched)
                    unit.writes.add(matched)
                    print(
                        f"{LOG_PREFIX_SUCCESS} Level 1 match: pipeline '{pl.display_name}' "
                        f"activity '{activity.activity_name}' -> table '{matched}'"
                    )
                else:
                    ignored_tables.append(
                        f"config_obj_name '{config_name}' from pipeline "
                        f"'{pl.display_name}' activity '{activity.activity_name}'"
                    )

            # Level 2: Notebook code analysis
            if notebook_id in notebooks:
                nb = notebooks[notebook_id]

                # Written tables from notebook code
                for wt in nb.written_tables:
                    matched = _match_table_name(wt, subset_tables)
                    if matched:
                        notebook_writes[notebook_id].add(matched)
                        pipeline_writes[pl_id].add(matched)
                        unit.writes.add(matched)

                        # Cross-validate with Level 1
                        if config_name:
                            config_matched = _match_table_name(config_name, subset_tables)
                            if config_matched and config_matched == matched:
                                print(
                                    f"{LOG_PREFIX_SUCCESS} Cross-validated: config_obj_name and "
                                    f"notebook code both produce '{matched}'"
                                )
                            elif config_matched and config_matched != matched:
                                print(
                                    f"{LOG_PREFIX_WARNING} Conflict: config_obj_name='{config_name}' "
                                    f"(->'{config_matched}') vs notebook writes '{matched}'. "
                                    f"Level 1 (config_obj_name) takes priority."
                                )
                    else:
                        ignored_tables.append(
                            f"notebook write '{wt}' from notebook '{nb.display_name}'"
                        )

                # File-based outputs from notebook code
                for fo in nb.file_outputs:
                    matched = _match_table_name(fo, subset_tables)
                    if matched:
                        notebook_writes[notebook_id].add(matched)
                        pipeline_writes[pl_id].add(matched)
                        unit.writes.add(matched)
                    else:
                        ignored_tables.append(
                            f"file output '{fo}' from notebook '{nb.display_name}'"
                        )

                # Read tables from notebook code
                for rt in nb.read_tables:
                    matched = _match_table_name(rt, subset_tables)
                    if matched:
                        notebook_reads[notebook_id].add(matched)
                        pipeline_reads[pl_id].add(matched)
                        unit.reads.add(matched)
            else:
                if notebook_id:
                    print(
                        f"{LOG_PREFIX_WARNING} notebookId '{notebook_id}' in pipeline "
                        f"'{pl.display_name}' does not match any scanned notebook logicalId."
                    )

            # Only add unit if it has at least one read or write
            if unit.reads or unit.writes:
                activity_units.append(unit)

    return notebook_writes, notebook_reads, pipeline_writes, pipeline_reads, ignored_tables, activity_units


def infer_lineage(
    activity_units: List[ActivityUnit],
    subset_tables: Set[str],
) -> Dict[str, List[str]]:
    """
    Infer downstream-only lineage from activity-level read/write mappings.

    For each activity unit (one notebook invocation), if it reads table X
    and writes table Y, then X -> Y (X.downstream includes Y).

    This is more accurate than pipeline-level inference because different
    notebook activities within the same pipeline have independent read/write sets.

    Returns:
        Dict mapping table_name -> list of downstream table names.
    """
    lineage: Dict[str, List[str]] = {}

    for table in subset_tables:
        lineage[table] = []

    for unit in activity_units:
        for read_table in unit.reads:
            if read_table not in subset_tables:
                continue
            for written_table in unit.writes:
                if written_table != read_table and written_table in subset_tables:
                    if written_table not in lineage[read_table]:
                        lineage[read_table].append(written_table)
                        print(
                            f"{LOG_PREFIX_INFO} Lineage inferred: '{read_table}' -> "
                            f"'{written_table}' (via {unit.label})"
                        )

    # Sort downstream lists for deterministic output
    for table in lineage:
        lineage[table] = sorted(lineage[table])

    return lineage


# ===========================================================================
# Output generation
# ===========================================================================

def generate_table_lineage_yml(
    lineage: Dict[str, List[str]],
    output_path: str,
) -> None:
    """Generate table_lineage.yml in the format expected by Phase 1."""
    output = {"lineage": {}}
    for table in sorted(lineage.keys()):
        output["lineage"][table] = {"downstream": lineage[table]}

    file_path = os.path.join(output_path, TABLE_LINEAGE_FILE)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated by generate_lineage.py\n")
        f.write("# Do not edit manually — re-run the script to update.\n")
        f.write("# Last generated: see git commit timestamp.\n\n")
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"{LOG_PREFIX_SUCCESS} Generated {file_path}")


def generate_table_registry_yml(
    subset_tables: Set[str],
    output_path: str,
) -> None:
    """
    Generate a skeleton table_registry.yml.
    Uses default min_rows and freshness_hours.
    Required columns must be filled in manually.
    """
    registry_path = os.path.join(output_path, TABLE_REGISTRY_FILE)

    # If registry already exists, load it to preserve manual edits
    existing: Dict[str, Any] = {}
    if os.path.exists(registry_path):
        with open(registry_path, "r", encoding="utf-8") as f:
            existing_data = yaml.safe_load(f) or {}
            existing = existing_data.get("tables", {})

    output = {"tables": {}}
    for table in sorted(subset_tables):
        if table in existing:
            # Preserve existing manual configuration
            output["tables"][table] = existing[table]
        else:
            # Generate skeleton entry
            output["tables"][table] = {
                "subset": True,
                "min_rows": DEFAULT_MIN_ROWS,
                "freshness_hours": DEFAULT_FRESHNESS_HOURS,
                "schema": {
                    "required_columns": []  # Must be filled manually
                },
            }

    with open(registry_path, "w", encoding="utf-8") as f:
        f.write("# Table registry for subset validation.\n")
        f.write("# Entries with 'required_columns: []' need manual configuration.\n\n")
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"{LOG_PREFIX_SUCCESS} Generated {registry_path}")


def generate_notebook_outputs_yml(
    notebooks: Dict[str, NotebookInfo],
    notebook_writes: Dict[str, Set[str]],
    output_path: str,
) -> None:
    """Generate notebook_outputs.yml for Phase 2 readiness."""
    output = {"notebook_outputs": {}}

    for nb_id, nb in notebooks.items():
        tables = sorted(notebook_writes.get(nb_id, set()))
        if tables:
            # Use the folder name for the key
            output["notebook_outputs"][nb.folder_name] = {
                "display_name": nb.display_name,
                "logical_id": nb.logical_id,
                "tables": tables,
            }

    file_path = os.path.join(output_path, NOTEBOOK_OUTPUTS_FILE)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated notebook -> table output mapping (Phase 2 readiness).\n\n")
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"{LOG_PREFIX_SUCCESS} Generated {file_path}")


def generate_pipeline_outputs_yml(
    pipelines: Dict[str, PipelineInfo],
    pipeline_writes: Dict[str, Set[str]],
    output_path: str,
) -> None:
    """Generate pipeline_outputs.yml for Phase 2 readiness."""
    output = {"pipeline_outputs": {}}

    for pl_id, pl in pipelines.items():
        tables = sorted(pipeline_writes.get(pl_id, set()))
        if tables:
            output["pipeline_outputs"][pl.folder_name] = {
                "display_name": pl.display_name,
                "logical_id": pl.logical_id,
                "tables": tables,
            }

    file_path = os.path.join(output_path, PIPELINE_OUTPUTS_FILE)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated pipeline -> table output mapping (Phase 2 readiness).\n\n")
        yaml.dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"{LOG_PREFIX_SUCCESS} Generated {file_path}")


# ===========================================================================
# Main
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate table lineage metadata from Fabric pipelines and notebooks."
    )
    parser.add_argument(
        "--metadata_path",
        default=METADATA_PATH,
        help="Path to metadata directory.",
    )
    parser.add_argument(
        "--notebooks_root",
        default=NOTEBOOKS_ROOT,
        help="Path to root directory containing .Notebook folders.",
    )
    parser.add_argument(
        "--pipelines_root",
        default=PIPELINES_ROOT,
        help="Path to root directory containing .DataPipeline folders.",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print results without writing files.",
    )
    args = parser.parse_args()

    print(f"{LOG_PREFIX_START} Lineage Generation Starting")
    print(f"{LOG_PREFIX_INFO} Metadata path: {args.metadata_path}")
    print(f"{LOG_PREFIX_INFO} Notebooks root: {args.notebooks_root}")
    print(f"{LOG_PREFIX_INFO} Pipelines root: {args.pipelines_root}")
    print()

    # -----------------------------------------------------------------------
    # Step 1: Load subset scope
    # -----------------------------------------------------------------------
    subset_scope_path = os.path.join(args.metadata_path, SUBSET_SCOPE_FILE)
    if not os.path.exists(subset_scope_path):
        print(f"{LOG_PREFIX_ERROR} subset_scope.yml not found at '{subset_scope_path}'. Aborting.")
        sys.exit(1)

    with open(subset_scope_path, "r", encoding="utf-8") as f:
        scope_data = yaml.safe_load(f)

    subset_config = scope_data.get("subset", {})
    subset_tables: Set[str] = set(subset_config.get("tables", []))

    if not subset_tables:
        print(f"{LOG_PREFIX_ERROR} No tables defined in subset_scope.yml. Aborting.")
        sys.exit(1)

    print(f"{LOG_PREFIX_INFO} Subset tables ({len(subset_tables)}):")
    for t in sorted(subset_tables):
        print(f"    - {t}")
    print()

    # -----------------------------------------------------------------------
    # Step 2: Scan notebooks
    # -----------------------------------------------------------------------
    notebooks = scan_notebooks(args.notebooks_root)
    print()

    # -----------------------------------------------------------------------
    # Step 3: Scan pipelines
    # -----------------------------------------------------------------------
    pipelines = scan_pipelines(args.pipelines_root)
    print()

    # -----------------------------------------------------------------------
    # Step 4: Build table mappings (2-level detection)
    # -----------------------------------------------------------------------
    notebook_writes, notebook_reads, pipeline_writes, pipeline_reads, ignored, activity_units = build_table_mappings(
        notebooks, pipelines, subset_tables
    )

    print()
    if ignored:
        print(f"{LOG_PREFIX_WARNING} Ignored non-subset assets ({len(ignored)}):")
        for ig in ignored:
            print(f"    - {ig}")
        print()

    print(f"{LOG_PREFIX_INFO} Built {len(activity_units)} activity units for lineage inference.")
    print()

    # -----------------------------------------------------------------------
    # Step 5: Infer lineage (activity-level for accuracy)
    # -----------------------------------------------------------------------
    lineage = infer_lineage(
        activity_units,
        subset_tables,
    )

    print()
    print(f"{LOG_PREFIX_INFO} Final lineage graph:")
    for table in sorted(lineage.keys()):
        ds = lineage[table]
        print(f"    {table} -> {ds if ds else '[]'}")
    print()

    # -----------------------------------------------------------------------
    # Step 6: Generate output files
    # -----------------------------------------------------------------------
    if args.dry_run:
        print(f"{LOG_PREFIX_INFO} Dry run - no files written.")
    else:
        generate_table_lineage_yml(lineage, args.metadata_path)
        generate_table_registry_yml(subset_tables, args.metadata_path)
        generate_notebook_outputs_yml(notebooks, notebook_writes, args.metadata_path)
        generate_pipeline_outputs_yml(pipelines, pipeline_writes, args.metadata_path)

    print()
    print(f"{LOG_PREFIX_SUCCESS} Lineage generation complete.")


if __name__ == "__main__":
    main()
