# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Offline integration tests for the Fabric REST/OneLake client
(`fabric_jobs_client.FabricJobsClient`).

This module previously had no coverage because every method is network I/O.
These tests exercise the real client logic end-to-end while stubbing the two
external boundaries it touches — the `requests` HTTP layer and the
`AzureCliCredential` token source — and no-op'ing `time.sleep` so the polling
loop runs instantly. Nothing here makes a real network call.

The behaviors locked in are the ones with non-trivial logic: timestamp parsing
and normalization, run-time caching, the submit→poll→terminal/timeout state
machine, OneLake status handling, name resolution, and the fail-soft network
paths (network/HTTP errors degrade to None/raise rather than crashing).
"""

from datetime import timezone

import pytest
import requests

import fabric_jobs_client as fjc
from fabric_jobs_client import FabricJobsClient, FabricJobsClientError

# ---------------------------------------------------------------------------
# Test doubles for the two external boundaries (HTTP + credential).
# ---------------------------------------------------------------------------

class FakeResponse:
    """Minimal stand-in for a requests.Response."""

    def __init__(self, status_code=200, json_data=None, text="", headers=None):
        self.status_code = status_code
        self._json = json_data
        self.text = text
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("no json body")
        return self._json


class _FakeToken:
    def __init__(self, token):
        self.token = token


class FakeCredential:
    """Stub AzureCliCredential — never touches the az CLI / network."""

    def get_token(self, *_scopes, **_kwargs):
        return _FakeToken("fake-token")


@pytest.fixture
def client(monkeypatch):
    """A FabricJobsClient whose credential is stubbed so construction and token
    acquisition are fully offline."""
    monkeypatch.setattr(fjc, "AzureCliCredential", FakeCredential)
    # Make the polling loop instant regardless of configured intervals.
    monkeypatch.setattr(fjc.time, "sleep", lambda *_a, **_k: None)
    return FabricJobsClient(workspace_id="ws-1")


def _patch_get(monkeypatch, fake_get):
    monkeypatch.setattr(fjc.requests, "get", fake_get)


def _patch_post(monkeypatch, fake_post):
    monkeypatch.setattr(fjc.requests, "post", fake_post)


# ---------------------------------------------------------------------------
# get_last_successful_run_time
# ---------------------------------------------------------------------------

def test_run_time_returns_latest_completed_and_ignores_non_completed(client, monkeypatch):
    """Picks the newest endTimeUtc among Completed instances; non-Completed
    (e.g. Failed) instances are ignored even if newer."""
    body = {
        "value": [
            {"status": "Completed", "endTimeUtc": "2026-01-01T00:00:00Z"},
            {"status": "Completed", "endTimeUtc": "2026-06-01T00:00:00Z"},
            {"status": "Failed", "endTimeUtc": "2027-01-01T00:00:00Z"},
        ]
    }
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    result = client.get_last_successful_run_time("item-1")

    assert result is not None
    assert result.year == 2026 and result.month == 6


def test_run_time_normalizes_seven_digit_fractional_seconds(client, monkeypatch):
    """Fabric returns 7-digit fractions which 3.10 fromisoformat rejects; the
    client truncates to 6 digits and parses a tz-aware datetime."""
    body = {"value": [{"status": "Completed", "endTimeUtc": "2026-04-05T18:38:42.7993478Z"}]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    result = client.get_last_successful_run_time("item-1")

    assert result is not None
    assert result.tzinfo is not None
    assert result.microsecond == 799347  # 7th digit dropped, not rounded


def test_run_time_coerces_naive_timestamp_to_utc(client, monkeypatch):
    """A timestamp with no Z/offset is treated as UTC so later tz-aware
    comparisons never crash."""
    body = {"value": [{"status": "Completed", "endTimeUtc": "2026-04-05T18:38:42"}]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    result = client.get_last_successful_run_time("item-1")

    assert result is not None
    assert result.tzinfo == timezone.utc


def test_run_time_returns_none_when_no_completed_runs(client, monkeypatch):
    """No Completed instances -> None (the caller treats this as no_record)."""
    body = {"value": [{"status": "Failed", "endTimeUtc": "2026-04-05T18:38:42Z"}]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    assert client.get_last_successful_run_time("item-1") is None


def test_run_time_is_cached_after_first_lookup(client, monkeypatch):
    """A second lookup for the same item must not issue another HTTP call —
    the per-item run-time cache serves it."""
    calls = {"n": 0}

    def fake_get(*_a, **_k):
        calls["n"] += 1
        return FakeResponse(200, json_data={"value": [
            {"status": "Completed", "endTimeUtc": "2026-06-01T00:00:00Z"}]})

    _patch_get(monkeypatch, fake_get)

    first = client.get_last_successful_run_time("item-1")
    second = client.get_last_successful_run_time("item-1")

    assert first == second
    assert calls["n"] == 1


def test_run_time_network_error_returns_none(client, monkeypatch):
    """A connection error degrades to None rather than propagating."""
    def boom(*_a, **_k):
        raise requests.exceptions.ConnectionError("down")

    _patch_get(monkeypatch, boom)

    assert client.get_last_successful_run_time("item-1") is None


def test_run_time_non_200_returns_none(client, monkeypatch):
    """A non-200 status degrades to None rather than raising."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(500, text="server error"))

    assert client.get_last_successful_run_time("item-1") is None


# ---------------------------------------------------------------------------
# run_notebook (submit -> poll state machine)
# ---------------------------------------------------------------------------

def test_run_notebook_submits_and_polls_to_completed(client, monkeypatch):
    """202 submit + Location header yields the job id; the first terminal poll
    status is returned."""
    submit = FakeResponse(
        202, headers={"Location": "https://api/x/jobs/instances/job-123"})
    _patch_post(monkeypatch, lambda *a, **k: submit)
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data={"status": "Completed"}))

    status, job_id = client.run_notebook("nb-1", parameters={"mode": "soft"})

    assert status == "Completed"
    assert job_id == "job-123"


def test_run_notebook_non_202_submit_raises(client, monkeypatch):
    """A failed submit (non-200/202) raises FabricJobsClientError."""
    _patch_post(monkeypatch, lambda *a, **k: FakeResponse(400, text="bad request"))

    with pytest.raises(FabricJobsClientError):
        client.run_notebook("nb-1")


def test_run_notebook_missing_job_id_raises(client, monkeypatch):
    """A 202 with neither a Location header nor a body id cannot be polled and
    must raise rather than poll a bogus URL."""
    _patch_post(monkeypatch, lambda *a, **k: FakeResponse(202, json_data={}, headers={}))

    with pytest.raises(FabricJobsClientError):
        client.run_notebook("nb-1")


def test_run_notebook_times_out_and_attempts_cancel(client, monkeypatch):
    """When polling never reaches a terminal state within the timeout, the
    client returns ('Timeout', id) and attempts a cancel POST."""
    posts = {"submit": 0, "cancel": 0}

    def fake_post(url, *_a, **_k):
        if url.endswith("/cancel"):
            posts["cancel"] += 1
            return FakeResponse(200)
        posts["submit"] += 1
        return FakeResponse(202, headers={"Location": "https://api/x/jobs/instances/job-9"})

    _patch_post(monkeypatch, fake_post)
    # Poll always returns a non-terminal state.
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data={"status": "Running"}))

    status, job_id = client.run_notebook("nb-1", timeout_sec=2, poll_interval_sec=1)

    assert status == "Timeout"
    assert job_id == "job-9"
    assert posts["cancel"] == 1


# ---------------------------------------------------------------------------
# read_onelake_file
# ---------------------------------------------------------------------------

def test_read_onelake_file_returns_text_on_200(client, monkeypatch):
    """200 returns the raw file body."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, text='{"ok": true}'))

    assert client.read_onelake_file("lh-1", "Files/r.json") == '{"ok": true}'


def test_read_onelake_file_returns_none_on_404(client, monkeypatch):
    """404 (file not present) returns None, not an error."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(404, text="not found"))

    assert client.read_onelake_file("lh-1", "Files/missing.json") is None


def test_read_onelake_file_returns_none_on_other_error(client, monkeypatch):
    """Any other non-200 status also degrades to None."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(503, text="unavailable"))

    assert client.read_onelake_file("lh-1", "Files/r.json") is None


# ---------------------------------------------------------------------------
# resolve_notebook_id_by_name
# ---------------------------------------------------------------------------

def test_resolve_notebook_id_matches_case_insensitively(client, monkeypatch):
    """An exact (case-insensitive) display-name match returns its id."""
    body = {"value": [
        {"id": "nb-1", "displayName": "My Notebook", "type": "Notebook"},
        {"id": "pl-1", "displayName": "My Pipeline", "type": "DataPipeline"},
    ]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    assert client.resolve_notebook_id_by_name("my notebook") == "nb-1"


def test_resolve_notebook_id_returns_none_when_no_match(client, monkeypatch):
    """No matching notebook returns None."""
    body = {"value": [{"id": "nb-1", "displayName": "Other", "type": "Notebook"}]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    assert client.resolve_notebook_id_by_name("missing") is None


def test_resolve_notebook_id_ambiguous_raises(client, monkeypatch):
    """Two notebooks sharing a display name is ambiguous and must raise rather
    than silently pick one."""
    body = {"value": [
        {"id": "nb-1", "displayName": "Dup", "type": "Notebook"},
        {"id": "nb-2", "displayName": "Dup", "type": "Notebook"},
    ]}
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(200, json_data=body))

    with pytest.raises(FabricJobsClientError):
        client.resolve_notebook_id_by_name("dup")


# ---------------------------------------------------------------------------
# smoke_test
# ---------------------------------------------------------------------------

def test_smoke_test_returns_true_on_200(client, monkeypatch):
    """A 200 workspace lookup confirms connectivity."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(
        200, json_data={"displayName": "WS", "id": "ws-1"}))

    assert client.smoke_test() is True


def test_smoke_test_raises_on_non_200(client, monkeypatch):
    """A non-200 workspace lookup raises FabricJobsClientError."""
    _patch_get(monkeypatch, lambda *a, **k: FakeResponse(403, text="forbidden"))

    with pytest.raises(FabricJobsClientError):
        client.smoke_test()
