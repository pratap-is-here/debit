# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the Phase 2 auto-impact-detection engine (`detect_impact.py`) and the
subset-filtered downstream walk it relies on (`TableLineage.expand_downstream`).

These lock in the two pure, deterministic pieces of the auto-detection flow:
  * `extract_changed_assets` — maps changed git-diff file paths to the subset
    notebook/pipeline folders they belong to.
  * `expand_downstream` — BFS of downstream tables from a seed set, STOPPING at
    the subset boundary. Step 4 of the combined fix retired the old, unfiltered
    `compute_tainted_set` and pointed detect_impact at this shared engine (the
    same one the freshness gate and validate_pr use), so the auto-detected
    "tainted" set is now subset-scoped and DAG-validated.

No git, network, or Fabric calls are exercised — both functions are pure.
"""

import pytest

from detect_impact import extract_changed_assets
from metadata_parser import MetadataValidationError, TableLineage

# ---------------------------------------------------------------------------
# Group A — extract_changed_assets (file path -> changed subset asset)
# ---------------------------------------------------------------------------

NB = "notebooks/weather_api_data_extraction.Notebook"
PL = "pipelines/load_weather.Pipeline"


def test_changed_file_under_notebook_folder_is_detected():
    """A file edited inside a subset notebook folder flags that notebook."""
    changed = [NB + "/notebook-content.py"]
    notebooks, pipelines = extract_changed_assets(changed, [NB], [PL])
    assert notebooks == {"weather_api_data_extraction.Notebook"}
    assert pipelines == set()


def test_changed_file_under_pipeline_folder_is_detected():
    """A file edited inside a subset pipeline folder flags that pipeline."""
    changed = [PL + "/pipeline-content.json"]
    notebooks, pipelines = extract_changed_assets(changed, [NB], [PL])
    assert pipelines == {"load_weather.Pipeline"}
    assert notebooks == set()


def test_windows_backslash_path_is_normalized():
    """Backslash-separated diff paths still match (CI may emit either separator)."""
    changed = [NB.replace("/", "\\") + "\\notebook-content.py"]
    notebooks, _ = extract_changed_assets(changed, [NB], [PL])
    assert notebooks == {"weather_api_data_extraction.Notebook"}


def test_exact_folder_path_match():
    """The folder path itself (no trailing file) matches the == branch."""
    notebooks, _ = extract_changed_assets([NB], [NB], [PL])
    assert notebooks == {"weather_api_data_extraction.Notebook"}


def test_unrelated_file_is_ignored():
    """A changed file outside any subset asset produces no matches."""
    notebooks, pipelines = extract_changed_assets(
        ["README.md", "some/other/path/file.py"], [NB], [PL]
    )
    assert notebooks == set()
    assert pipelines == set()


def test_sibling_prefix_does_not_false_match():
    """`foo.Notebook2/...` must NOT match subset `foo.Notebook` (boundary guard)."""
    changed = [NB + "2/notebook-content.py"]
    notebooks, _ = extract_changed_assets(changed, [NB], [PL])
    assert notebooks == set()


def test_multiple_files_same_notebook_dedupes():
    """Two edited files in one notebook collapse to a single folder entry."""
    changed = [NB + "/cell1.py", NB + "/cell2.py", NB + "/meta.json"]
    notebooks, _ = extract_changed_assets(changed, [NB], [PL])
    assert notebooks == {"weather_api_data_extraction.Notebook"}


# ---------------------------------------------------------------------------
# Group B — expand_downstream (subset-filtered downstream blast-radius walk)
#
# detect_impact.main() now computes its "tainted" set via
# lineage.expand_downstream(tables, subset_tables), so these lock that engine's
# contract: it follows downstream edges but stops at the subset boundary and
# validates the lineage DAG (raising on cycles).
# ---------------------------------------------------------------------------

def _lineage(text, write_metadata):
    return TableLineage(write_metadata(lineage=text))


def test_downstream_linear_chain_within_subset(write_metadata):
    """Linear A->B->C from {A}, all in subset, yields the whole chain."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.c]
""",
        write_metadata,
    )
    assert lineage.expand_downstream({"s.a"}, {"s.a", "s.b", "s.c"}) == {"s.a", "s.b", "s.c"}


def test_downstream_includes_seed_when_leaf(write_metadata):
    """A leaf seed with no downstream still returns itself."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.c]
""",
        write_metadata,
    )
    assert lineage.expand_downstream({"s.c"}, {"s.c"}) == {"s.c"}


def test_downstream_diamond_no_duplicates(write_metadata):
    """Diamond A->B, A->C, B->D, C->D from {A} returns each node once."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.b, s.c]
  s.b:
    downstream: [s.d]
  s.c:
    downstream: [s.d]
""",
        write_metadata,
    )
    subset = {"s.a", "s.b", "s.c", "s.d"}
    assert lineage.expand_downstream({"s.a"}, subset) == {"s.a", "s.b", "s.c", "s.d"}


def test_downstream_raises_on_cycle(write_metadata):
    """A cyclic graph A->B->A is not a valid DAG: expand_downstream raises.

    This is the behavior change from the retired compute_tainted_set (which was
    cycle-safe but never validated). detect_impact.main() wraps this call in a
    try/except and falls back to the directly-impacted tables so the metadata
    error never crashes detection."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.a]
""",
        write_metadata,
    )
    with pytest.raises(MetadataValidationError):
        lineage.expand_downstream({"s.a"}, {"s.a", "s.b"})


def test_downstream_multiple_seeds_union(write_metadata):
    """Multiple seeds return the union of their downstream reach."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.b]
  s.x:
    downstream: [s.y]
""",
        write_metadata,
    )
    subset = {"s.a", "s.b", "s.x", "s.y"}
    assert lineage.expand_downstream({"s.a", "s.x"}, subset) == {"s.a", "s.b", "s.x", "s.y"}


def test_downstream_stops_at_subset_boundary(write_metadata):
    """Pins the Step 4 behavior: the auto-detected tainted set is now
    subset-filtered. A downstream table outside the subset (s.c) is excluded,
    where the retired compute_tainted_set would have included it."""
    lineage = _lineage(
        """
lineage:
  s.a:
    downstream: [s.b]
  s.b:
    downstream: [s.c]
""",
        write_metadata,
    )
    subset = {"s.a", "s.b"}  # s.c intentionally outside the subset
    assert lineage.expand_downstream({"s.a"}, subset) == {"s.a", "s.b"}
