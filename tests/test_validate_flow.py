# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the Step 3 (``validate_pr.validate``) hardening:

  * Step 8 no longer duplicates the structural existence checks that Step 6
    already performs; it surfaces per-table contract WARNINGS only.
  * ``collect_contract_warnings`` is the single, central home for per-table
    contract coherence warnings (min_rows, required_columns, freshness pairing).
  * The final ``errors`` list returned by ``validate`` is de-duplicated.

These run against both isolated temp metadata (via the ``write_metadata``
fixture) and the live ``metadata/`` to lock in real-world behavior.
"""

import validate_pr
from constants import METADATA_PATH
from metadata_parser import (
    SubsetScope,
    TableRegistry,
    collect_contract_warnings,
)

# ---------------------------------------------------------------------------
# collect_contract_warnings — unit level
# ---------------------------------------------------------------------------

def test_clean_contract_yields_no_warnings(write_metadata):
    registry_yaml = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 100\n"
        "    freshness_hours: 24\n"
        "    freshness_column: event_date\n"
        "    schema:\n"
        "      required_columns:\n"
        "        - id\n"
    )
    path = write_metadata(registry=registry_yaml)
    registry = TableRegistry(path)
    assert collect_contract_warnings(registry, ["a.b"]) == []


def test_min_rows_zero_warns(write_metadata):
    registry_yaml = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 0\n"
        "    freshness_hours: 24\n"
        "    freshness_column: event_date\n"
        "    schema:\n"
        "      required_columns:\n"
        "        - id\n"
    )
    path = write_metadata(registry=registry_yaml)
    warnings = collect_contract_warnings(TableRegistry(path), ["a.b"])
    assert any("min_rows=0" in w for w in warnings)


def test_missing_required_columns_warns(write_metadata):
    registry_yaml = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 10\n"
        "    freshness_hours: 24\n"
        "    freshness_column: event_date\n"
    )
    path = write_metadata(registry=registry_yaml)
    warnings = collect_contract_warnings(TableRegistry(path), ["a.b"])
    assert any("no required_columns" in w for w in warnings)


def test_freshness_hours_without_column_warns(write_metadata):
    # The new coherence check: freshness gating requested but no column to gate on.
    registry_yaml = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 10\n"
        "    freshness_hours: 48\n"
        "    schema:\n"
        "      required_columns:\n"
        "        - id\n"
    )
    path = write_metadata(registry=registry_yaml)
    warnings = collect_contract_warnings(TableRegistry(path), ["a.b"])
    assert any("freshness_hours=48" in w for w in warnings)
    assert any("freshness_column" in w for w in warnings)


def test_warnings_are_sorted_and_deduped(write_metadata):
    registry_yaml = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 0\n"
    )
    path = write_metadata(registry=registry_yaml)
    # Pass the same table twice; output must not contain duplicates.
    warnings = collect_contract_warnings(TableRegistry(path), ["a.b", "a.b"])
    assert warnings == sorted(set(warnings))


# ---------------------------------------------------------------------------
# validate() — integration level
# ---------------------------------------------------------------------------

def test_live_metadata_upstream_and_downstream_still_pass():
    # Regression: removing the redundant Step 8 checks must not change outcomes.
    subset = SubsetScope(METADATA_PATH)
    sample = subset.tables[:2]
    for mode in ("affected_only", "upstream", "downstream", "full_lineage"):
        passed, errors = validate_pr.validate(METADATA_PATH, sample, mode)
        assert passed is True, f"mode {mode} unexpectedly failed: {errors}"
        assert errors == []


def test_freshness_incoherent_table_warns_but_still_passes(write_metadata, capsys):
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
    registry = (
        "tables:\n"
        "  a.b:\n"
        "    min_rows: 10\n"
        "    freshness_hours: 48\n"          # no freshness_column on purpose
        "    schema:\n"
        "      required_columns:\n"
        "        - id\n"
    )
    lineage = "lineage:\n  a.b:\n    downstream: []\n"
    path = write_metadata(subset, registry, lineage)
    passed, errors = validate_pr.validate(path, ["a.b"], "affected_only")
    assert passed is True          # warning, never a failure
    assert errors == []
    assert "freshness_column" in capsys.readouterr().out


def test_validate_dedupes_errors(write_metadata):
    # A subset listing the same table twice makes Step 6 raise identical errors
    # twice; the final returned list must be de-duplicated.
    subset = "subset:\n  enabled: true\n  tables:\n    - a.b\n    - a.b\n"
    registry = "tables: {}\n"        # a.b deliberately NOT registered
    lineage = "lineage: {}\n"
    path = write_metadata(subset, registry, lineage)
    passed, errors = validate_pr.validate(path, ["a.b"], "affected_only")
    assert passed is False
    assert len(errors) == len(set(errors)), f"errors contain duplicates: {errors}"
