# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the freshness-gate verdict invariant in `summarize_results`
(used by the freshness gate in `trigger_data_presence.py`).

`summarize_results` treats both `stale` (ran too long ago or before an
upstream change) and `no_record` (never ran) as freshness *failures*. The mode
decides what a failure does:

  * `hard` is the ONLY mode that can return False (block) -- it does so whenever
    any failure is present.
  * `soft` reports failures loudly but always returns True ("warn but
    continue").
  * `skip` is a no-op gate -> always True.

This False-only-in-hard invariant is what makes the orchestrator's removal of
the vestigial `and freshness_gate_passed` term safe: whenever the gate returns
False the orchestrator has already exited (1) at the gate itself. These tests
pin that behavior so it cannot silently regress.
"""

from freshness_validator import summarize_results

# A single failing table per kind; the only thing that varies is `mode`.
_STALE = {"s.table": ("stale", "ran before upstream")}
_FRESH = {"s.table": ("fresh", "ok")}
_NO_RECORD = {"s.table": ("no_record", "no execution row")}


def test_hard_mode_stale_blocks():
    """Hard mode blocks on a stale table -> False."""
    assert summarize_results(_STALE, "hard") is False


def test_hard_mode_no_record_blocks():
    """Hard mode blocks on a never-run (no_record) table -> False.

    no_record is now a first-class failure, no longer gated behind a separate
    behavior knob."""
    assert summarize_results(_NO_RECORD, "hard") is False


def test_soft_mode_stale_passes_with_warning():
    """Soft mode warns but never blocks -> True (the 'warn but continue' path)."""
    assert summarize_results(_STALE, "soft") is True


def test_soft_mode_no_record_passes_with_warning():
    """Soft mode reports a never-run table as a failure but does not block."""
    assert summarize_results(_NO_RECORD, "soft") is True


def test_skip_mode_is_noop_pass():
    """Skip mode is a no-op gate -> always True."""
    assert summarize_results(_STALE, "skip") is True


def test_hard_mode_fresh_passes():
    """Hard mode with no failing tables passes."""
    assert summarize_results(_FRESH, "hard") is True


def test_false_is_unreachable_outside_hard_mode():
    """Invariant guaranteeing the removed `and freshness_gate_passed` term was
    dead: only mode=='hard' can ever yield False."""
    for mode in ("soft", "skip"):
        for results in (_STALE, _FRESH, _NO_RECORD):
            assert summarize_results(results, mode) is True

