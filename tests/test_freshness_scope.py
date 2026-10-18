# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the freshness-gate scope expansion used by `trigger_data_presence.py`.

Step 1 of the combined fix moved the gate from a hand-rolled, downstream-only,
subset-UNfiltered `compute_tainted_set` branch to `TableLineage.expand_scope`,
which is the single source of truth: subset-filtered and correct for all four
scope modes. These tests lock in two things:

  * Group 1 — `TableLineage.expand_scope` returns the right set per mode
    (the engine the gate now calls).
  * Group 2 — feeding that scoped set into `FreshnessValidator.validate`
    (exactly as the gate does) makes `upstream` mode flag a changed table that
    ran BEFORE its upstream source, where the old `affected_only`-equivalent
    behavior silently missed it.
  * Group 3 — `validate`'s Check B is applied to EVERY scoped table with
    per-table `execution_freshness_hours` thresholds, so never-run tables
    (`no_record`) and too-old runs (`stale`) are caught regardless of edges —
    including the never-run upstream parent that Check A's edge comparison
    skips.

No git, network, or Fabric calls are exercised: lineage is built from temp
metadata and execution timestamps are injected via a stub.
"""

from datetime import datetime, timedelta, timezone

from freshness_validator import FreshnessValidator
from metadata_parser import TableLineage, TableRegistry


def _lineage(text, write_metadata):
    return TableLineage(write_metadata(lineage=text))


class _StubValidator(FreshnessValidator):
    """FreshnessValidator with execution timestamps injected from a dict.

    Overrides the only Fabric-touching method so `validate()` runs purely on
    in-memory lineage + a table -> datetime map. Tables absent from the map
    return None (i.e. "no execution record").
    """

    def __init__(self, lineage, timestamps, **kwargs):
        super().__init__(
            client=None,
            lineage=lineage,
            notebook_outputs=None,
            pipeline_outputs=None,
            **kwargs,
        )
        self._ts = timestamps

    def get_last_execution_time(self, table):
        return self._ts.get(table)


# ---------------------------------------------------------------------------
# Group 1 — expand_scope per mode (the engine the gate now uses)
# ---------------------------------------------------------------------------

_CHAIN = """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.c]
"""


def test_expand_scope_affected_only_returns_declared_verbatim(write_metadata):
    """affected_only never expands — the declared set is returned as-is."""
    lineage = _lineage(_CHAIN, write_metadata)
    subset = {"s.a", "s.b", "s.c"}
    assert lineage.expand_scope({"s.b"}, "affected_only", subset) == {"s.b"}


def test_expand_scope_downstream_is_subset_filtered(write_metadata):
    """downstream stops at the subset boundary (s.c excluded)."""
    lineage = _lineage(_CHAIN, write_metadata)
    subset = {"s.a", "s.b"}  # s.c intentionally outside the subset
    assert lineage.expand_scope({"s.a"}, "downstream", subset) == {"s.a", "s.b"}


def test_expand_scope_upstream_includes_ancestors(write_metadata):
    """upstream walks ancestors (the half the old gate never computed)."""
    lineage = _lineage(_CHAIN, write_metadata)
    subset = {"s.a", "s.b", "s.c"}
    assert lineage.expand_scope({"s.c"}, "upstream", subset) == {"s.a", "s.b", "s.c"}


def test_expand_scope_full_lineage_includes_both_halves(write_metadata):
    """full_lineage is the union of upstream and downstream walks."""
    lineage = _lineage(_CHAIN, write_metadata)
    subset = {"s.a", "s.b", "s.c"}
    assert lineage.expand_scope({"s.b"}, "full_lineage", subset) == {"s.a", "s.b", "s.c"}


def test_expand_scope_unknown_mode_defaults_to_affected_only(write_metadata):
    """An unrecognized mode degrades to affected_only (declared verbatim)."""
    lineage = _lineage(_CHAIN, write_metadata)
    subset = {"s.a", "s.b", "s.c"}
    assert lineage.expand_scope({"s.b"}, "sideways", subset) == {"s.b"}


# ---------------------------------------------------------------------------
# Group 2 — gate behavior: upstream scope now flags a stale changed table
# ---------------------------------------------------------------------------

# s.source feeds s.changed (s.source is upstream of s.changed).
_SOURCE_CHANGED = """
lineage:
  s.source:
    downstream: [s.changed]
"""


def _scenario(write_metadata):
    """Build the lineage + injected timestamps where the changed table ran
    BEFORE its upstream source (the classic 'forgot to re-run downstream')."""
    lineage = _lineage(_SOURCE_CHANGED, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {
        "s.source": now - timedelta(hours=1),   # source refreshed recently
        "s.changed": now - timedelta(hours=3),   # changed table is older -> stale
    }
    return lineage, now, timestamps


def test_upstream_scope_flags_changed_table_that_ran_before_source(write_metadata):
    """With upstream scope, the source enters the scoped set, so Check A
    detects the changed table ran before it -> 'stale'. This is the behavior
    the Step 1 fix unlocks."""
    lineage, now, timestamps = _scenario(write_metadata)
    subset = {"s.source", "s.changed"}
    directly_impacted = {"s.changed"}

    # Mirror the gate: expand via the single source of truth, then validate.
    scoped = lineage.expand_scope(directly_impacted, "upstream", subset)
    assert scoped == {"s.source", "s.changed"}

    validator = _StubValidator(lineage, timestamps)
    results = validator.validate(
        directly_impacted=directly_impacted,
        tainted_tables=scoped,
        now=now,
        exempt_tables=set(),
    )
    assert results["s.changed"][0] == "stale"


def test_affected_only_scope_misses_upstream_staleness(write_metadata):
    """Contrast / regression marker: with affected_only the source is NOT in
    scope, so the same out-of-order execution is NOT flagged (the changed table
    reads 'fresh'). Documents the gap that upstream mode now closes."""
    lineage, now, timestamps = _scenario(write_metadata)
    subset = {"s.source", "s.changed"}
    directly_impacted = {"s.changed"}

    scoped = lineage.expand_scope(directly_impacted, "affected_only", subset)
    assert scoped == {"s.changed"}

    validator = _StubValidator(lineage, timestamps)
    results = validator.validate(
        directly_impacted=directly_impacted,
        tainted_tables=scoped,
        now=now,
        exempt_tables=set(),
    )
    assert results["s.changed"][0] != "stale"
    assert results["s.changed"][0] == "fresh"


# ---------------------------------------------------------------------------
# Group 3 — Check B is broadened to EVERY scoped table, with per-table
# thresholds. This is the hardening that (a) catches never-run tables and
# tables that ran too long ago regardless of edges, and (b) closes the
# never-run-parent blind spot in Check A.
# ---------------------------------------------------------------------------

# A lone table with no edges, so only Check B (absolute staleness) can act.
_STANDALONE = """
lineage:
  s.solo: {}
"""


def test_check_b_flags_never_run_scoped_table_as_no_record(write_metadata):
    """A scoped table with no execution record is failed as 'no_record' by
    Check B even though it participates in no scoped edge."""
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    validator = _StubValidator(lineage, {}, default_max_age_hours=48)  # no timestamp -> never ran

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables=set(),
    )
    assert results["s.solo"][0] == "no_record"


def test_check_b_flags_too_old_scoped_table_as_stale(write_metadata):
    """A scoped table whose last run predates the default threshold is 'stale'."""
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {"s.solo": now - timedelta(hours=72)}  # 72h > default 48h
    validator = _StubValidator(lineage, timestamps, default_max_age_hours=48)

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables=set(),
    )
    assert results["s.solo"][0] == "stale"


def test_check_b_fresh_when_within_default_threshold(write_metadata):
    """A recent run (younger than the default threshold) reads 'fresh'."""
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {"s.solo": now - timedelta(hours=10)}  # 10h < default 48h
    validator = _StubValidator(lineage, timestamps, default_max_age_hours=48)

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables=set(),
    )
    assert results["s.solo"][0] == "fresh"


def test_per_table_override_relaxes_staleness(write_metadata):
    """A generous per-table execution_freshness_hours override makes a run that
    would be stale under the default read 'fresh' instead — proving the
    registry cascade (per-table -> default) is honored."""
    registry = TableRegistry(
        write_metadata(
            registry="tables:\n  s.solo:\n    execution_freshness_hours: 1000\n"
        )
    )
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {"s.solo": now - timedelta(hours=72)}  # stale under default 48h
    validator = _StubValidator(
        lineage, timestamps, default_max_age_hours=48, registry=registry
    )

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables=set(),
    )
    # 72h < 1000h override -> fresh, despite exceeding the 48h default.
    assert results["s.solo"][0] == "fresh"


def test_per_table_override_tightens_staleness(write_metadata):
    """A stricter per-table override flips an otherwise-fresh run to 'stale',
    the complementary direction of the cascade."""
    registry = TableRegistry(
        write_metadata(
            registry="tables:\n  s.solo:\n    execution_freshness_hours: 1\n"
        )
    )
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {"s.solo": now - timedelta(hours=5)}  # fresh under default 48h
    validator = _StubValidator(
        lineage, timestamps, default_max_age_hours=48, registry=registry
    )

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables=set(),
    )
    # 5h > 1h override -> stale, despite being within the 48h default.
    assert results["s.solo"][0] == "stale"


def test_exempt_table_is_never_failed_by_check_b(write_metadata):
    """An exempt table that never ran is reported 'exempt', not 'no_record' —
    exemption short-circuits both checks."""
    lineage = _lineage(_STANDALONE, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    validator = _StubValidator(lineage, {}, default_max_age_hours=48)

    results = validator.validate(
        directly_impacted=set(),
        tainted_tables={"s.solo"},
        now=now,
        exempt_tables={"s.solo"},
    )
    assert results["s.solo"][0] == "exempt"


def test_check_b_catches_never_run_parent_that_check_a_misses(write_metadata):
    """The headline blind-spot fix: a never-run upstream parent has no timestamp,
    so Check A's edge comparison skips it entirely. Check B, now applied to every
    scoped table, independently fails that parent as 'no_record' while the child
    (which did run) stays fresh."""
    lineage = _lineage(_SOURCE_CHANGED, write_metadata)
    now = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)
    timestamps = {
        # s.source intentionally absent -> never ran (Check A blind spot).
        "s.changed": now - timedelta(hours=2),
    }
    scoped = {"s.source", "s.changed"}
    validator = _StubValidator(lineage, timestamps, default_max_age_hours=48)

    results = validator.validate(
        directly_impacted={"s.changed"},
        tainted_tables=scoped,
        now=now,
        exempt_tables=set(),
    )
    assert results["s.source"][0] == "no_record"
    assert results["s.changed"][0] == "fresh"

