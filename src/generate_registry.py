# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Standalone utility to generate or update table_registry.yml from subset_scope.yml.
Preserves existing manual entries. Adds skeleton entries for new tables.

Usage:
    python generate_registry.py --metadata_path <path>
"""

import argparse
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    DEFAULT_FRESHNESS_HOURS,
    DEFAULT_MIN_ROWS,
    LOG_PREFIX_INFO,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
    METADATA_PATH,
    SUBSET_SCOPE_FILE,
    TABLE_REGISTRY_FILE,
)


def main():
    parser = argparse.ArgumentParser(description="Generate/update table_registry.yml")
    parser.add_argument("--metadata_path", default=METADATA_PATH)
    args = parser.parse_args()

    # Load subset scope
    scope_path = os.path.join(args.metadata_path, SUBSET_SCOPE_FILE)
    with open(scope_path, "r", encoding="utf-8") as f:
        scope_data = yaml.safe_load(f) or {}

    subset_tables = set(scope_data.get("subset", {}).get("tables", []))

    # Load existing registry if present
    registry_path = os.path.join(args.metadata_path, TABLE_REGISTRY_FILE)
    existing = {}
    if os.path.exists(registry_path):
        with open(registry_path, "r", encoding="utf-8") as f:
            existing_data = yaml.safe_load(f) or {}
            existing = existing_data.get("tables", {})
        print(f"{LOG_PREFIX_INFO} Loaded existing registry with {len(existing)} tables.")

    # Build output
    output = {"tables": {}}
    added = 0
    preserved = 0

    for table in sorted(subset_tables):
        if table in existing:
            output["tables"][table] = existing[table]
            preserved += 1
        else:
            output["tables"][table] = {
                "min_rows": DEFAULT_MIN_ROWS,
                "freshness_hours": DEFAULT_FRESHNESS_HOURS,
                "schema": {
                    "required_columns": [],
                },
            }
            added += 1
            print(f"{LOG_PREFIX_WARNING} New skeleton entry for '{table}' - configure min_rows, freshness, columns.")

    # Preserve non-subset tables that were already in registry
    for table, config in existing.items():
        if table not in output["tables"]:
            output["tables"][table] = config

    with open(registry_path, "w", encoding="utf-8") as f:
        f.write("# Table registry for subset validation.\n")
        f.write("# Entries with 'required_columns: []' need manual configuration.\n\n")
        yaml.safe_dump(output, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print(f"{LOG_PREFIX_SUCCESS} Registry updated: {preserved} preserved, {added} added.")


if __name__ == "__main__":
    main()
