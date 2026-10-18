# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Centralized constants for the testing framework.
All paths and configuration values are defined here.
No hardcoded paths should exist outside this file.
"""

import os

# ---------------------------------------------------------------------------
# Root paths — override via environment variables or CLI arguments
# ---------------------------------------------------------------------------
REPO_ROOT = os.environ.get(
    "REPO_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

# Default metadata directory. Out of the box this points at the bundled,
# fully synthetic example dataset (``examples/metadata``) so the tool — and its
# test suite — run with zero internal access. Point this at your own metadata
# directory (see ``config/`` for templates) via the METADATA_PATH env var or
# the ``--metadata_path`` CLI argument.
METADATA_PATH = os.environ.get(
    "METADATA_PATH",
    os.path.join(REPO_ROOT, "examples", "metadata")
)

# Roots for Fabric notebook and pipeline source folders in your repository.
# Defaults assume a simple ``<repo>/notebooks`` and ``<repo>/pipelines`` layout;
# override to match your own folder structure.
NOTEBOOKS_ROOT = os.environ.get(
    "NOTEBOOKS_ROOT",
    os.path.join(REPO_ROOT, "notebooks")
)

PIPELINES_ROOT = os.environ.get(
    "PIPELINES_ROOT",
    os.path.join(REPO_ROOT, "pipelines")
)

# ---------------------------------------------------------------------------
# Metadata file names
# ---------------------------------------------------------------------------
SUBSET_SCOPE_FILE = "subset_scope.yml"
TABLE_REGISTRY_FILE = "table_registry.yml"
TABLE_LINEAGE_FILE = "table_lineage.yml"
NOTEBOOK_OUTPUTS_FILE = "notebook_outputs.yml"
PIPELINE_OUTPUTS_FILE = "pipeline_outputs.yml"
ADVANCED_CHECKS_FILE = "advanced_checks.yml"
EXECUTION_FRESHNESS_FILE = "execution_freshness.yml"

# ---------------------------------------------------------------------------
# Freshness gating defaults
# ---------------------------------------------------------------------------
DEFAULT_FRESHNESS_MODE = "soft"       # soft = warn, hard = block, skip = disabled
DEFAULT_TOLERANCE_MINUTES = 30        # clock-skew tolerance for edge-by-edge comparison
DEFAULT_STALENESS_MAX_AGE_HOURS = 48  # staleness window for directly impacted tables

# ---------------------------------------------------------------------------
# Fabric artifact conventions
# ---------------------------------------------------------------------------
NOTEBOOK_FOLDER_SUFFIX = ".Notebook"
PIPELINE_FOLDER_SUFFIX = ".DataPipeline"
PLATFORM_FILE = ".platform"
NOTEBOOK_CONTENT_FILE = "notebook-content.py"
PIPELINE_CONTENT_FILE = "pipeline-content.json"

# ---------------------------------------------------------------------------
# Pipeline activity types
# ---------------------------------------------------------------------------
ACTIVITY_TYPE_NOTEBOOK = "TridentNotebook"
ACTIVITY_TYPE_INVOKE_PIPELINE = "InvokePipeline"

# ---------------------------------------------------------------------------
# Validation defaults
# ---------------------------------------------------------------------------
DEFAULT_MIN_ROWS = 0
DEFAULT_FRESHNESS_HOURS = 0  # 0 = freshness check disabled; set explicitly in table_registry.yml to enable

# ---------------------------------------------------------------------------
# PR template markers
# ---------------------------------------------------------------------------
PR_AFFECTED_TABLES_HEADER = "## Affected Tables"
PR_VALIDATION_SCOPE_HEADER = "## Validation Scope Mode"
VALID_SCOPE_MODES = ["affected_only", "upstream", "downstream", "full_lineage"]
DEFAULT_SCOPE_MODE = "affected_only"

# Optional PR description keys for runtime workspace overrides.
PR_NOTEBOOK_WORKSPACE_ID_KEY = "notebook_workspace_id"
PR_LAKEHOUSE_WORKSPACE_ID_KEY = "lakehouse_workspace_id"
PR_LAKEHOUSE_ID_KEY = "lakehouse_id"

# ---------------------------------------------------------------------------
# Fabric API (Phase 2 - Data Presence Validation)
# ---------------------------------------------------------------------------
# Public Microsoft Fabric / OneLake product endpoints. Override only if you use
# a sovereign / non-public cloud.
FABRIC_API_BASE_URL = os.environ.get(
    "FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com/v1"
)
ONELAKE_DFS_BASE_URL = os.environ.get(
    "ONELAKE_DFS_BASE_URL", "https://onelake.dfs.fabric.microsoft.com"
)

# Fabric workspace / lakehouse identifiers.
# REQUIRED: provide your own values via environment variables (or CLI args /
# PR-description overrides). There are intentionally no real defaults here.
# The validation notebook may live in a collaboration workspace while the
# lakehouse lives in another workspace, hence the separate IDs.
FABRIC_WORKSPACE_ID = os.environ.get("FABRIC_WORKSPACE_ID", "")
FABRIC_LAKEHOUSE_ID = os.environ.get("FABRIC_LAKEHOUSE_ID", "")
FABRIC_LAKEHOUSE_WORKSPACE_ID = os.environ.get("FABRIC_LAKEHOUSE_WORKSPACE_ID", "")

# Identity of the data-presence validation notebook in Fabric.
# Name lookup in the target workspace is attempted first, with item ID fallback.
DATA_PRESENCE_NOTEBOOK_NAME = os.environ.get(
    "DATA_PRESENCE_NOTEBOOK_NAME",
    "data_presence_check"
)
DATA_PRESENCE_NOTEBOOK_ITEM_ID = os.environ.get("DATA_PRESENCE_NOTEBOOK_ITEM_ID", "")

# Directory where the validation notebook writes run-specific results JSON files.
DATA_PRESENCE_RESULTS_PATH = os.environ.get(
    "DATA_PRESENCE_RESULTS_PATH", "Files/pr_validation/results"
)

# Retention window for notebook-written result files.
DATA_PRESENCE_RETENTION_MONTHS = 3

# Job polling configuration
JOB_POLL_INTERVAL_SEC = 15
JOB_TIMEOUT_SEC = 480  # 8 minutes max for notebook job
ADVANCED_CHECK_TIMEOUT_SEC = 600  # 10 minutes default for advanced check notebooks

# Advanced checks scope: "all" = run every check, "affected" = only checks whose tables overlap PR
DEFAULT_ADV_CHECKS_SCOPE = "all"

# Runtime workspace override controls.
# When enabled, a PR description may request a different notebook / lakehouse
# workspace, but only workspaces on the allowlists below are accepted. This
# prevents arbitrary workspace targeting from PR text.
ENABLE_PR_RUNTIME_OVERRIDES = (
    os.environ.get("ENABLE_PR_RUNTIME_OVERRIDES", "true").lower() == "true"
)


def _id_list_from_env(var_name: str) -> list:
    """Parse a comma-separated allowlist of workspace IDs from an env var."""
    raw = os.environ.get(var_name, "")
    return [v.strip() for v in raw.split(",") if v.strip()]


# Allowlists: only workspaces listed here (via the corresponding env vars) may
# be targeted by PR-provided overrides. Empty by default — configure for your
# own tenant, e.g. ALLOWED_NOTEBOOK_WORKSPACE_IDS="<id-1>,<id-2>".
ALLOWED_NOTEBOOK_WORKSPACE_IDS = _id_list_from_env("ALLOWED_NOTEBOOK_WORKSPACE_IDS")
ALLOWED_LAKEHOUSE_WORKSPACE_IDS = _id_list_from_env("ALLOWED_LAKEHOUSE_WORKSPACE_IDS")

# ---------------------------------------------------------------------------
# Logging prefixes (ASCII-safe for cp1252 Windows agents)
# ---------------------------------------------------------------------------
LOG_PREFIX_INFO = "[INFO]"
LOG_PREFIX_SUCCESS = "[PASS]"
LOG_PREFIX_WARNING = "[WARN]"
LOG_PREFIX_ERROR = "[FAIL]"
LOG_PREFIX_SKIP = "[SKIP]"
LOG_PREFIX_START = "[START]"
