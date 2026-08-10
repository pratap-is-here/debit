# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for the Phase 2 required-config guard in ``trigger_data_presence.main``.

These lock the adopter-facing behaviour that turns an unconfigured or malformed
Fabric workspace ID into an actionable message instead of an opaque HTTP error
later in the run:

* An empty workspace ID means "Phase 2 is not configured" -> clean skip
  (exit 0) with guidance naming FABRIC_WORKSPACE_ID / --workspace_id, mirroring
  the missing-notebook-identity skip.
* A non-empty but malformed workspace ID is a real misconfiguration -> in hard
  mode it exits non-zero with an "Invalid" message.

The guard runs before any metadata load or Fabric/network call, so these tests
stay fully offline.
"""

import os
import sys

import pytest

from trigger_data_presence import main

# Absolute path to the bundled synthetic metadata, independent of the pytest CWD.
EXAMPLES_METADATA = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples",
    "metadata",
)


def _run_main(monkeypatch, argv_tail):
    """Invoke main() with a controlled argv and return the raised SystemExit."""
    argv = ["trigger_data_presence.py", "--metadata_path", EXAMPLES_METADATA] + argv_tail
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as excinfo:
        main()
    return excinfo.value


def test_empty_workspace_id_skips_cleanly(monkeypatch, capsys):
    """Unset workspace ID -> exit 0 with actionable FABRIC_WORKSPACE_ID guidance."""
    exit_exc = _run_main(
        monkeypatch,
        ["--workspace_id", "", "--tables", "silver.fact_orders", "--mode", "soft"],
    )
    assert exit_exc.code == 0
    out = capsys.readouterr().out
    assert "FABRIC_WORKSPACE_ID is not set" in out


def test_malformed_workspace_id_fails_in_hard_mode(monkeypatch, capsys):
    """Non-empty, non-UUID workspace ID -> exit 1 in hard mode with 'Invalid'."""
    exit_exc = _run_main(
        monkeypatch,
        ["--workspace_id", "not-a-uuid", "--tables", "silver.fact_orders", "--mode", "hard"],
    )
    assert exit_exc.code == 1
    out = capsys.readouterr().out
    assert "Invalid notebook workspace ID" in out
