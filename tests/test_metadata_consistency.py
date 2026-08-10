# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for cross-file consistency validation (``validate_metadata_consistency``).

These guard the renumbered checks after Step D removed the registry subset flag:
  * Check 1: every subset table is registered,
  * Check 2: every subset table has a lineage entry,
  * Check 3: downstream references point to registered tables,
  * Check 4: lineage is a valid DAG (no cycles),
  * disabled subset short-circuits and returns no errors.
"""

from metadata_parser import (
    SubsetScope,
    TableLineage,
    TableRegistry,
    validate_metadata_consistency,
)


def _load(path):
    return SubsetScope(path), TableRegistry(path), TableLineage(path)


def test_valid_temp_metadata_has_no_consistency_errors(write_metadata):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
    registry = "tables:\n  a.b: {}\n"
    lineage = "lineage:\n  a.b:\n    downstream: []\n"
    path = write_metadata(subset, registry, lineage)
    assert validate_metadata_consistency(*_load(path)) == []


def test_check1_subset_table_missing_from_registry(write_metadata):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
    registry = "tables:\n  other.t: {}\n"
    lineage = "lineage:\n  a.b:\n    downstream: []\n"
    path = write_metadata(subset, registry, lineage)
    errors = validate_metadata_consistency(*_load(path))
    assert any("a.b" in e and "not registered" in e for e in errors)


def test_check2_subset_table_missing_lineage(write_metadata):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
    registry = "tables:\n  a.b: {}\n"
    lineage = "lineage:\n"
    path = write_metadata(subset, registry, lineage)
    errors = validate_metadata_consistency(*_load(path))
    assert any("a.b" in e and "no entry" in e for e in errors)


def test_check3_downstream_references_unregistered_table(write_metadata):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
    registry = "tables:\n  a.b: {}\n"
    lineage = "lineage:\n  a.b:\n    downstream:\n      - ghost.t\n"
    path = write_metadata(subset, registry, lineage)
    errors = validate_metadata_consistency(*_load(path))
    assert any("Downstream table 'ghost.t'" in e for e in errors)


def test_check4_cycle_is_detected(write_metadata):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n    - b.c\n"
    registry = "tables:\n  a.b: {}\n  b.c: {}\n"
    lineage = (
        "lineage:\n"
        "  a.b:\n    downstream:\n      - b.c\n"
        "  b.c:\n    downstream:\n      - a.b\n"
    )
    path = write_metadata(subset, registry, lineage)
    errors = validate_metadata_consistency(*_load(path))
    assert any("Cycle detected" in e for e in errors)


def test_disabled_subset_skips_consistency(write_metadata):
    # Deliberately inconsistent content, but disabled => no checks run.
    subset = "subset:\n  enabled: false\n  tables:\n    - missing.t\n"
    registry = "tables:\n"
    lineage = "lineage:\n"
    path = write_metadata(subset, registry, lineage)
    assert validate_metadata_consistency(*_load(path)) == []
