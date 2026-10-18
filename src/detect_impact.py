# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Phase 2 — Automatic impact detection from git diff.

Detects which notebooks and pipelines were modified, maps them to output tables,
and merges with manually declared tables from the PR.

Usage:
    python detect_impact.py \
        --metadata_path <path> \
        --target_branch origin/develop

Note: This script is ACTIVE in the PR validation pipeline. It runs before
validate_pr.py and exports the `auto_detected_tables` variable (the git-diff
tables) that validate_pr.py consumes via --additional_tables.
"""

import argparse
import os
import subprocess
import sys
from typing import List, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    LOG_PREFIX_ERROR,
    LOG_PREFIX_INFO,
    LOG_PREFIX_START,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
    METADATA_PATH,
)
from metadata_parser import (
    NotebookOutputs,
    PipelineOutputs,
    SubsetScope,
)


def get_changed_files_from_diff(target_branch: str) -> List[str]:
    """
    Get list of changed files between HEAD and target branch using git diff.

    Returns:
        List of changed file paths relative to repo root.
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", target_branch, "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
        return files
    except subprocess.CalledProcessError as e:
        print(f"{LOG_PREFIX_ERROR} git diff failed: {e}")
        return []


def extract_changed_assets(
    changed_files: List[str],
    subset_notebooks: List[str],
    subset_pipelines: List[str],
) -> tuple:
    """
    Match changed files against subset notebook and pipeline paths.

    Returns:
        Tuple of (changed_notebook_folders, changed_pipeline_folders).
    """
    changed_notebooks: Set[str] = set()
    changed_pipelines: Set[str] = set()

    for filepath in changed_files:
        normalized = filepath.replace("\\", "/")

        # Check if file belongs to a subset notebook folder
        for nb_path in subset_notebooks:
            # nb_path example:
            # notebooks/weather_api_data_extraction.Notebook
            if normalized.startswith(nb_path + "/") or normalized == nb_path:
                folder_name = nb_path.split("/")[-1]
                changed_notebooks.add(folder_name)

        # Check if file belongs to a subset pipeline folder
        for pl_path in subset_pipelines:
            if normalized.startswith(pl_path + "/") or normalized == pl_path:
                folder_name = pl_path.split("/")[-1]
                changed_pipelines.add(folder_name)

    return changed_notebooks, changed_pipelines


def detect_impacted_tables(
    metadata_path: str,
    target_branch: str,
) -> Set[str]:
    """
    Main Phase 2 detection logic.

    Returns:
        Set of impacted table names.
    """
    subset = SubsetScope(metadata_path)
    nb_outputs = NotebookOutputs(metadata_path)
    pl_outputs = PipelineOutputs(metadata_path)

    if not subset.enabled:
        print(f"{LOG_PREFIX_INFO} Subset validation disabled. Returning empty set.")
        return set()

    # Get changed files
    changed_files = get_changed_files_from_diff(target_branch)
    print(f"{LOG_PREFIX_INFO} {len(changed_files)} files changed.")

    # Match against subset assets
    changed_notebooks, changed_pipelines = extract_changed_assets(
        changed_files, subset.notebooks, subset.pipelines
    )

    print(f"{LOG_PREFIX_INFO} Changed subset notebooks: {changed_notebooks}")
    print(f"{LOG_PREFIX_INFO} Changed subset pipelines: {changed_pipelines}")

    # Map to output tables
    impacted_tables: Set[str] = set()

    for nb_folder in changed_notebooks:
        tables = nb_outputs.get_output_tables(nb_folder)
        if tables:
            impacted_tables.update(tables)
        else:
            print(
                f"{LOG_PREFIX_WARNING} Notebook '{nb_folder}' modified but has no output "
                f"table mapping in notebook_outputs.yml."
            )

    for pl_folder in changed_pipelines:
        tables = pl_outputs.get_output_tables(pl_folder)
        if tables:
            impacted_tables.update(tables)
        else:
            print(
                f"{LOG_PREFIX_WARNING} Pipeline '{pl_folder}' modified but has no output "
                f"table mapping in pipeline_outputs.yml."
            )

    # Filter to subset tables
    subset_impacted = set()
    for t in impacted_tables:
        if subset.is_table_in_subset(t):
            subset_impacted.add(t)
        else:
            print(f"{LOG_PREFIX_WARNING} Impacted table '{t}' not in subset - ignored.")

    print(f"{LOG_PREFIX_SUCCESS} Auto-detected impacted subset tables: {sorted(subset_impacted)}")
    return subset_impacted


def main():
    parser = argparse.ArgumentParser(description="Phase 2 - Auto Impact Detection")
    parser.add_argument("--metadata_path", default=METADATA_PATH)
    parser.add_argument("--target_branch", default="origin/develop")
    args = parser.parse_args()

    print(f"{LOG_PREFIX_START} Phase 2 Impact Detection")

    tables = detect_impacted_tables(args.metadata_path, args.target_branch)

    # Output as Azure DevOps pipeline variables for downstream consumption.
    # ``##vso[task.setvariable ...]`` is a public Azure Pipelines logging
    # command; when this script runs outside Azure DevOps it is simply printed
    # to stdout and ignored. Only auto_detected_tables is exported: the
    # data-presence task expands this (merged with pr_declared_tables) to the
    # validation scope in-script via --scope_mode, so detect_impact does not
    # pre-compute a scope set here.
    #
    # ADOPTERS ON GITHUB ACTIONS / OTHER CI: the ``##vso`` line below only sets a
    # variable on Azure DevOps. To consume the impacted tables elsewhere, capture
    # this script's stdout, or write the comma-separated list to your CI's output
    # mechanism instead — e.g. on GitHub Actions append
    # ``auto_detected_tables=<list>`` to the file named by $GITHUB_OUTPUT.
    if tables:
        table_list = ",".join(sorted(tables))
        print(f"##vso[task.setvariable variable=auto_detected_tables]{table_list}")
    else:
        print(f"{LOG_PREFIX_INFO} No impacted subset tables detected from diff.")
        print("##vso[task.setvariable variable=auto_detected_tables]")


if __name__ == "__main__":
    main()
