# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for Step D: subset membership has a single source of truth
(``subset_scope.yml``), and the redundant registry ``subset`` flag is gone.

Also exercises the ``validate_pr.validate`` happy path end-to-end against the
live metadata to confirm Step 1.5 (shape validation) and the consistency
checks are wired together correctly.
"""

import validate_pr
from constants import METADATA_PATH
from metadata_parser import (
    SubsetScope,
    TableLineage,
    TableRegistry,
    validate_metadata_consistency,
)


def test_registry_carries_no_subset_flag():
    registry = TableRegistry(METADATA_PATH)
    offenders = [
        name
        for name in registry.all_table_names()
        if "subset" in (registry.get_table(name) or {})
    ]
    assert offenders == [], f"registry entries still carry a 'subset' flag: {offenders}"


def test_is_subset_table_method_is_removed():
    # The redundant API must be gone so nothing can re-introduce the dual source.
    assert not hasattr(TableRegistry, "is_subset_table")


def test_real_metadata_consistency_passes():
    subset = SubsetScope(METADATA_PATH)
    registry = TableRegistry(METADATA_PATH)
    lineage = TableLineage(METADATA_PATH)
    assert validate_metadata_consistency(subset, registry, lineage) == []


def test_validate_pr_happy_path_against_real_metadata():
    subset = SubsetScope(METADATA_PATH)
    sample = subset.tables[:2]
    passed, errors = validate_pr.validate(METADATA_PATH, sample, "affected_only")
    assert passed is True
    assert errors == []


def test_validate_pr_fails_on_malformed_metadata(write_metadata):
    # A typo'd key must be caught by Step 1.5 (shape validation) before scope logic.
    bad_subset = "subset:\n  enabled: true\n  tabels:\n    - a.b\n"
    registry = "tables:\n  a.b: {}\n"
    lineage = "lineage:\n  a.b:\n    downstream: []\n"
    path = write_metadata(bad_subset, registry, lineage)
    passed, errors = validate_pr.validate(path, ["a.b"], "affected_only")
    assert passed is False
    assert any("tabels" in e for e in errors)
