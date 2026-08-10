# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the metadata loader classes (Step A: None-coalescing resilience).

These guard the behaviour that a present-but-empty YAML section (which
``yaml.safe_load`` parses to ``None``) no longer crashes the loaders. An empty
onboarding skeleton such as ``subset:`` / ``tables:`` / ``lineage:`` must
no-op gracefully rather than raise ``AttributeError``.
"""

from constants import METADATA_PATH
from metadata_parser import SubsetScope, TableLineage, TableRegistry

EMPTY_SUBSET = "subset:\n"
EMPTY_REGISTRY = "tables:\n"
EMPTY_LINEAGE = "lineage:\n"


def test_empty_subset_section_does_not_crash(write_metadata):
    path = write_metadata(EMPTY_SUBSET, EMPTY_REGISTRY, EMPTY_LINEAGE)
    subset = SubsetScope(path)
    assert subset.enabled is False
    assert subset.tables == []
    assert subset.notebooks == []
    assert subset.pipelines == []


def test_empty_registry_section_does_not_crash(write_metadata):
    path = write_metadata(EMPTY_SUBSET, EMPTY_REGISTRY, EMPTY_LINEAGE)
    registry = TableRegistry(path)
    assert registry.all_table_names() == []
    assert registry.table_exists("anything") is False
    assert registry.get_min_rows("anything") == 0


def test_empty_lineage_section_does_not_crash(write_metadata):
    path = write_metadata(EMPTY_SUBSET, EMPTY_REGISTRY, EMPTY_LINEAGE)
    lineage = TableLineage(path)
    assert lineage.get_downstream("anything") == []
    assert lineage.has_entry("anything") is False
    assert lineage.all_tables_in_lineage() == set()


# ---------------------------------------------------------------------------
# get_downstream / get_upstream return COPIES (copy-on-return hardening): a
# caller mutating the returned list must not corrupt the loader's internal
# lineage state, so a second read returns the original, unmodified edges.
# ---------------------------------------------------------------------------

_LINEAGE_EDGE = """
lineage:
  s.a:
    downstream: [s.b]
"""


def test_get_downstream_returns_a_copy(write_metadata):
    """Mutating the list from get_downstream must not leak into the loader."""
    lineage = TableLineage(write_metadata(lineage=_LINEAGE_EDGE))
    first = lineage.get_downstream("s.a")
    assert first == ["s.b"]
    first.append("s.injected")
    # A fresh read must reflect only the original edge, not the mutation.
    assert lineage.get_downstream("s.a") == ["s.b"]


def test_get_upstream_returns_a_copy(write_metadata):
    """Mutating the list from get_upstream must not leak into the cached index."""
    lineage = TableLineage(write_metadata(lineage=_LINEAGE_EDGE))
    first = lineage.get_upstream("s.b")
    assert first == ["s.a"]
    first.append("s.injected")
    # A fresh read must reflect only the original edge, not the mutation.
    assert lineage.get_upstream("s.b") == ["s.a"]


def test_real_metadata_loads_and_subset_is_well_formed():
    """Smoke regression against the live metadata in the repo."""
    subset = SubsetScope(METADATA_PATH)
    registry = TableRegistry(METADATA_PATH)
    lineage = TableLineage(METADATA_PATH)

    assert subset.enabled is True
    assert len(subset.tables) > 0

    # Every subset table must be registered and have a lineage entry. We assert
    # the relationship (subset is a subset of the registry) rather than exact
    # counts so the test stays valid as tables are legitimately added/removed.
    registered = set(registry.all_table_names())
    assert set(subset.tables).issubset(registered)
    for table in subset.tables:
        assert lineage.has_entry(table), f"{table} missing lineage entry"


# ---------------------------------------------------------------------------
# get_execution_freshness_hours: per-table run-time freshness threshold with a
# caller-supplied default. This is the value the freshness gate uses to decide
# whether a table's producing job ran recently enough; it is independent of the
# data-content ``freshness_hours`` field.
# ---------------------------------------------------------------------------

_REGISTRY_EXEC_FRESHNESS = """
tables:
  s.with_override:
    execution_freshness_hours: 12
    min_rows: 1
  s.without_override:
    min_rows: 1
"""


def test_execution_freshness_hours_returns_override(write_metadata):
    """A table that sets execution_freshness_hours returns that value, not the default."""
    registry = TableRegistry(write_metadata(registry=_REGISTRY_EXEC_FRESHNESS))
    assert registry.get_execution_freshness_hours("s.with_override", default=48) == 12


def test_execution_freshness_hours_falls_back_to_default(write_metadata):
    """A table with no execution_freshness_hours returns the supplied default."""
    registry = TableRegistry(write_metadata(registry=_REGISTRY_EXEC_FRESHNESS))
    assert registry.get_execution_freshness_hours("s.without_override", default=48) == 48


def test_execution_freshness_hours_unregistered_table_returns_default(write_metadata):
    """An unregistered table returns the supplied default rather than crashing."""
    registry = TableRegistry(write_metadata(registry=_REGISTRY_EXEC_FRESHNESS))
    assert registry.get_execution_freshness_hours("s.not_present", default=48) == 48
