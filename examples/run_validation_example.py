#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Runnable example: validate a pull request against the bundled synthetic
metadata, with no internal or cloud access required.

This drives ``validate_pr.validate`` directly with a small set of affected
tables and prints the result for each lineage scope mode.

Run from the repository root:

    python examples/run_validation_example.py

It points the framework at ``examples/metadata`` (the same synthetic "retail
sales" dataset used by the test suite). To validate your own metadata instead,
set the METADATA_PATH environment variable or edit ``metadata_path`` below.
"""

import os
import sys

# Make the framework modules importable (src/ is added to sys.path).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import validate_pr  # noqa: E402


def main() -> int:
    metadata_path = os.environ.get(
        "METADATA_PATH", os.path.join(REPO_ROOT, "examples", "metadata")
    )

    affected_tables = ["silver.fact_orders"]

    print(f"Using metadata: {metadata_path}")
    print(f"Affected tables: {affected_tables}\n")

    overall_ok = True
    for scope_mode in ("affected_only", "upstream", "downstream", "full_lineage"):
        passed, errors = validate_pr.validate(metadata_path, affected_tables, scope_mode)
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] scope_mode={scope_mode}")
        for err in errors:
            print(f"        - {err}")
        overall_ok = overall_ok and passed

    print("\nDone.")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
