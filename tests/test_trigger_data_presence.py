# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the data-presence results-payload interpretation in
`trigger_data_presence.py`.

Step 2 of the combined fix closed the "silent evasion" gap: after a notebook
job reaches "Completed", if the results file cannot be read or parsed, the
orchestrator no longer exits 0 silently in soft mode (indistinguishable from a
clean pass). Instead `parse_results_payload` classifies the outcome so the
caller can emit a loud INCOMPLETE message and treat it as a failure that flows
into the final soft/hard summary.

These tests lock the pure classification seam: a missing or unparseable payload
must NOT be classified as a verdict (results is None, status != "ok"), while a
valid payload — including an empty-but-valid object — parses to "ok".

Step 5 (A5) added `resolve_scope`: the data-presence payload is now built from
the scope-EXPANDED table set (the same set the freshness gate uses), not just
the directly-affected --tables. These tests lock that pure seam: affected_only
is a verbatim no-op, wider modes expand subset-filtered, and an invalid lineage
DAG degrades safely to the directly-affected set while reporting the error.
"""

from metadata_parser import TableLineage
from trigger_data_presence import parse_results_payload, resolve_scope


def test_missing_payload_is_classified_missing():
    """None content (file not found/unreadable) -> no verdict, 'missing'."""
    results, status = parse_results_payload(None)
    assert results is None
    assert status == "missing"


def test_invalid_json_is_classified_unparseable():
    """Non-JSON content -> no verdict, 'unparseable' (was a silent soft pass)."""
    results, status = parse_results_payload("{not valid json")
    assert results is None
    assert status == "unparseable"


def test_empty_string_is_unparseable():
    """An empty file is not valid JSON and must not read as a clean pass."""
    results, status = parse_results_payload("")
    assert results is None
    assert status == "unparseable"


def test_valid_payload_parses_ok():
    """Valid JSON returns the parsed dict with status 'ok'."""
    results, status = parse_results_payload('{"summary": {"failed": 0, "error": 0}}')
    assert status == "ok"
    assert results == {"summary": {"failed": 0, "error": 0}}


def test_empty_but_valid_object_is_ok():
    """An empty-but-valid JSON object is a real (if empty) verdict, not a failure."""
    results, status = parse_results_payload("{}")
    assert status == "ok"
    assert results == {}


def test_non_ok_status_means_no_verdict():
    """Both failure classifications yield results=None, which is what the
    orchestrator keys off to set all_passed=False (the behavior Step 2 fixed)."""
    for content in (None, "{bad", ""):
        results, status = parse_results_payload(content)
        assert results is None
        assert status != "ok"


# ---------------------------------------------------------------------------
# resolve_scope — the shared scope expansion feeding BOTH the freshness gate
# and (as of Step 5/A5) the data-presence payload.
# ---------------------------------------------------------------------------

_CHAIN = """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.c]
"""

_CYCLE = """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.a]
"""


def _lineage(text, write_metadata):
    return TableLineage(write_metadata(lineage=text))


def test_resolve_scope_affected_only_returns_declared_verbatim(write_metadata):
    """affected_only is a verbatim no-op: data presence checks only the
    directly-affected tables, unchanged from the pre-A5 behavior."""
    lineage = _lineage(_CHAIN, write_metadata)
    scoped, err = resolve_scope(lineage, ["s.a"], "affected_only", {"s.a", "s.b", "s.c"})
    assert scoped == {"s.a"}
    assert err is None


def test_resolve_scope_downstream_is_subset_filtered(write_metadata):
    """downstream expands along lineage but stops at the subset boundary
    (s.c is outside the subset and excluded)."""
    lineage = _lineage(_CHAIN, write_metadata)
    scoped, err = resolve_scope(lineage, ["s.a"], "downstream", {"s.a", "s.b"})
    assert scoped == {"s.a", "s.b"}
    assert err is None


def test_resolve_scope_downstream_includes_in_subset_descendants(write_metadata):
    """With the whole chain in subset, downstream reaches every descendant."""
    lineage = _lineage(_CHAIN, write_metadata)
    scoped, err = resolve_scope(lineage, ["s.a"], "downstream", {"s.a", "s.b", "s.c"})
    assert scoped == {"s.a", "s.b", "s.c"}
    assert err is None


def test_resolve_scope_invalid_dag_falls_back_to_directly_affected(write_metadata):
    """A cyclic lineage degrades safely: returns the directly-affected set and
    surfaces the error string (for the loud [WARNING]) instead of crashing."""
    lineage = _lineage(_CYCLE, write_metadata)
    scoped, err = resolve_scope(lineage, ["s.a"], "downstream", {"s.a", "s.b"})
    assert scoped == {"s.a"}
    assert err is not None


def test_resolve_scope_affected_only_ignores_cycle(write_metadata):
    """affected_only never walks the DAG, so even an invalid DAG is a clean
    no-op (no fallback, no error)."""
    lineage = _lineage(_CYCLE, write_metadata)
    scoped, err = resolve_scope(lineage, ["s.a"], "affected_only", {"s.a", "s.b"})
    assert scoped == {"s.a"}
    assert err is None
