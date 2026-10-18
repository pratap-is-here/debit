# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for PR description parsing in ``validate_pr.parse_pr_description`` —
specifically the hardened scope-mode extraction:

  1. Tolerant matching: case-insensitive and forgiving of missing spaces around
     the checkbox (e.g. "-[x]upstream", "- [X] Upstream").
  2. Loud fallback: a ticked-but-unknown mode warns and defaults to
     ``affected_only`` instead of silently downgrading.
  3. Ambiguity surfacing: multiple ticked boxes warn and the first *valid*
     mode in document order wins.

Also keeps regression coverage for affected-table extraction so the parser's
two responsibilities (what / how-far) stay correct together.
"""

import validate_pr
from constants import DEFAULT_SCOPE_MODE

# ---------------------------------------------------------------------------
# Baseline / regression
# ---------------------------------------------------------------------------

def test_empty_description_returns_defaults():
    tables, mode = validate_pr.parse_pr_description("")
    assert tables == []
    assert mode == DEFAULT_SCOPE_MODE


def test_canonical_template_parses_tables_and_mode():
    desc = (
        "## Affected Tables (Subset Only)\n"
        "- silver.dim_device\n"
        "- silver.fact_reading\n"
        "\n"
        "## Validation Scope Mode\n"
        "Choose one:\n"
        "- [ ] affected_only\n"
        "- [x] upstream\n"
        "- [ ] downstream\n"
    )
    tables, mode = validate_pr.parse_pr_description(desc)
    assert tables == ["silver.dim_device", "silver.fact_reading"]
    assert mode == "upstream"


def test_no_ticked_box_keeps_default_mode():
    desc = (
        "## Validation Scope Mode\n"
        "- [ ] affected_only\n"
        "- [ ] upstream\n"
    )
    _, mode = validate_pr.parse_pr_description(desc)
    assert mode == DEFAULT_SCOPE_MODE


# ---------------------------------------------------------------------------
# Fix 1 — tolerant matching
# ---------------------------------------------------------------------------

def test_uppercase_x_is_accepted():
    _, mode = validate_pr.parse_pr_description("- [X] downstream\n")
    assert mode == "downstream"


def test_uppercase_mode_word_is_normalized():
    _, mode = validate_pr.parse_pr_description("- [x] Upstream\n")
    assert mode == "upstream"


def test_missing_space_after_dash_and_bracket_is_tolerated():
    # "-[x]upstream" — no space after the dash, none after the bracket.
    _, mode = validate_pr.parse_pr_description("-[x]upstream\n")
    assert mode == "upstream"


def test_asterisk_bullet_is_accepted():
    _, mode = validate_pr.parse_pr_description("* [x] full_lineage\n")
    assert mode == "full_lineage"


# ---------------------------------------------------------------------------
# Fix 2 — loud fallback on unrecognized mode
# ---------------------------------------------------------------------------

def test_unrecognized_mode_falls_back_to_default(capsys):
    _, mode = validate_pr.parse_pr_description("- [x] upstreem\n")  # typo
    assert mode == DEFAULT_SCOPE_MODE
    out = capsys.readouterr().out
    assert "Unrecognized scope mode 'upstreem'" in out
    assert DEFAULT_SCOPE_MODE in out


def test_recognized_mode_emits_no_unrecognized_warning(capsys):
    _, mode = validate_pr.parse_pr_description("- [x] upstream\n")
    assert mode == "upstream"
    out = capsys.readouterr().out
    assert "Unrecognized scope mode" not in out


# ---------------------------------------------------------------------------
# Fix 3 — ambiguity surfacing on multiple ticked boxes
# ---------------------------------------------------------------------------

def test_multiple_ticked_boxes_warn_and_first_valid_wins(capsys):
    desc = (
        "## Validation Scope Mode\n"
        "- [x] downstream\n"
        "- [x] upstream\n"
    )
    _, mode = validate_pr.parse_pr_description(desc)
    assert mode == "downstream"  # first valid in document order
    out = capsys.readouterr().out
    assert "Multiple scope modes ticked" in out


def test_multiple_ticked_first_invalid_then_valid_uses_valid(capsys):
    desc = (
        "- [x] garbage\n"
        "- [x] upstream\n"
    )
    _, mode = validate_pr.parse_pr_description(desc)
    # First *valid* wins even if an invalid box appears earlier; and because a
    # valid mode is chosen, no "defaulting to affected_only" warning fires.
    assert mode == "upstream"
    out = capsys.readouterr().out
    assert "Multiple scope modes ticked" in out
    assert "defaulting to" not in out
