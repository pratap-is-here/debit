# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Fabric Jobs API client for Phase 2 - Data Presence Validation.

Handles:
    - Authentication via AzureCliCredential (works with AzureCLI@2 in ADO)
    - Submitting notebook jobs via Fabric REST API
    - Polling job status until completion or timeout
    - Reading results files from OneLake DFS

Usage (smoke test):
    az login
    python fabric_jobs_client.py --workspace_id <id> --smoke_test

Usage (run notebook):
    python fabric_jobs_client.py \
        --workspace_id <id> \
        --notebook_item_id <id> \
        --parameters '{"tables_json": "...", "mode": "soft"}'
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from azure.identity import AzureCliCredential

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    FABRIC_API_BASE_URL,
    JOB_POLL_INTERVAL_SEC,
    JOB_TIMEOUT_SEC,
    LOG_PREFIX_ERROR,
    LOG_PREFIX_INFO,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
    ONELAKE_DFS_BASE_URL,
)

# Fabric API token scope
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
# OneLake (ADLS Gen2) token scope
_STORAGE_SCOPE = "https://storage.azure.com/.default"

# Regex to truncate fractional seconds to 6 digits max.
# Python 3.10 fromisoformat only handles 0, 3, or 6 fractional digits;
# Fabric API often returns 7.  e.g. "2026-04-05T18:38:42.7993478"
_FRAC_SEC_RE = re.compile(r"(\.\d{6})\d+")


class FabricJobsClientError(Exception):
    """Raised when a Fabric API call fails."""
    pass


class FabricJobsClient:
    """
    Thin client for the Fabric REST API (Jobs + OneLake).

    Token acquisition uses AzureCliCredential, which picks up the
    identity established by the AzureCLI@2 pipeline task (SPN via WIF)
    or a local 'az login' session for development.
    """

    def __init__(self, workspace_id: str):
        self._workspace_id = workspace_id
        self._credential = AzureCliCredential()
        # Tokens are cached per scope after first acquisition
        self._fabric_token: Optional[str] = None
        self._storage_token: Optional[str] = None
        # Cache: item_id -> last successful run time (or None if never run).
        # Avoids redundant API calls when multiple tables share the same producer.
        self._run_time_cache: Dict[str, Optional[datetime]] = {}

    # ------------------------------------------------------------------
    # Token helpers
    # ------------------------------------------------------------------

    def _get_fabric_token(self) -> str:
        if self._fabric_token is None:
            token = self._credential.get_token(_FABRIC_SCOPE)
            self._fabric_token = token.token
        return self._fabric_token

    def _get_storage_token(self) -> str:
        if self._storage_token is None:
            token = self._credential.get_token(_STORAGE_SCOPE)
            self._storage_token = token.token
        return self._storage_token

    def _fabric_headers(self) -> Dict[str, str]:
        return {
            "Authorization": "Bearer " + self._get_fabric_token(),
            "Content-Type": "application/json",
        }

    def _storage_headers(self) -> Dict[str, str]:
        return {
            "Authorization": "Bearer " + self._get_storage_token(),
        }

    # ------------------------------------------------------------------
    # Workspace smoke test
    # ------------------------------------------------------------------

    def smoke_test(self) -> bool:
        """
        Verify connectivity by fetching workspace metadata.
        Returns True on success, raises on failure.
        """
        url = "{}/workspaces/{}".format(FABRIC_API_BASE_URL, self._workspace_id)
        print("{} Smoke test: GET {}".format(LOG_PREFIX_INFO, url))
        resp = requests.get(url, headers=self._fabric_headers(), timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            print(
                "{} Workspace found: {} (id={})".format(
                    LOG_PREFIX_SUCCESS,
                    data.get("displayName", "?"),
                    data.get("id", "?"),
                )
            )
            return True
        raise FabricJobsClientError(
            "Smoke test failed: HTTP {} - {}".format(resp.status_code, resp.text[:500])
        )

    # ------------------------------------------------------------------
    # Execution timestamp query
    # ------------------------------------------------------------------

    def get_last_successful_run_time(
        self,
        item_id: str,
        item_type: str = "Notebook",
    ) -> Optional[datetime]:
        """
        Return the end time of the most recent successful job for a Fabric item.

        Uses GET /v1/workspaces/{ws}/items/{item}/jobs/instances and filters
        for status == "Completed", returning the latest endTimeUtc.

        Args:
            item_id: Fabric item ID (notebook or pipeline).
            item_type: "Notebook" or "DataPipeline" — used only for logging.

        Returns:
            datetime (UTC) of the last successful run, or None if none found.
        """
        if item_id in self._run_time_cache:
            cached = self._run_time_cache[item_id]
            print(
                "{} [cache] Last successful run for {} ({}): {}".format(
                    LOG_PREFIX_INFO,
                    item_id,
                    item_type,
                    cached.isoformat() if cached else None,
                )
            )
            return cached

        url = "{}/workspaces/{}/items/{}/jobs/instances".format(
            FABRIC_API_BASE_URL, self._workspace_id, item_id
        )
        print(
            "{} Querying last successful run for {} ({})".format(
                LOG_PREFIX_INFO, item_id, item_type
            )
        )

        try:
            resp = requests.get(url, headers=self._fabric_headers(), timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(
                "{} Network error querying job instances: {}".format(
                    LOG_PREFIX_WARNING, str(e)[:200]
                )
            )
            return None

        if resp.status_code != 200:
            print(
                "{} Job instances query failed: HTTP {} - {}".format(
                    LOG_PREFIX_WARNING, resp.status_code, resp.text[:300]
                )
            )
            return None

        data = resp.json()
        instances = data.get("value", data.get("items", []))

        latest: Optional[datetime] = None
        for inst in instances:
            if inst.get("status") != "Completed":
                continue
            end_str = inst.get("endTimeUtc") or inst.get("endTime")
            if not end_str:
                continue
            try:
                # Normalise: replace Z→+00:00, truncate >6 fractional digits
                # (Python 3.10 fromisoformat rejects 7-digit fractions).
                normalised = end_str.replace("Z", "+00:00")
                normalised = _FRAC_SEC_RE.sub(r"\1", normalised)
                end_dt = datetime.fromisoformat(normalised)
                # Fabric API sometimes returns naive timestamps (no Z / offset).
                # Treat them as UTC so timezone-aware comparisons never crash.
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                if latest is None or end_dt > latest:
                    latest = end_dt
            except (ValueError, TypeError):
                continue

        if latest:
            print(
                "{} Last successful run for {}: {}".format(
                    LOG_PREFIX_SUCCESS, item_id, latest.isoformat()
                )
            )
        else:
            print(
                "{} No successful runs found for {}".format(
                    LOG_PREFIX_WARNING, item_id
                )
            )

        self._run_time_cache[item_id] = latest
        return latest

    # ------------------------------------------------------------------
    # Item discovery (generic)
    # ------------------------------------------------------------------

    def resolve_item_id_by_name(
        self, display_name: str, item_type: str
    ) -> Optional[str]:
        """
        Resolve a Fabric item ID by display name and type.

        Args:
            display_name: Exact display name (case-insensitive match).
            item_type: Fabric item type, e.g. "Notebook", "DataPipeline".

        Returns:
            Item ID when exactly one match is found, None otherwise.
        """
        if not display_name:
            return None

        url = "{}/workspaces/{}/items".format(FABRIC_API_BASE_URL, self._workspace_id)
        target = display_name.strip().lower()
        target_type = item_type.strip().lower()
        matches: List[Dict[str, Any]] = []

        while url:
            try:
                resp = requests.get(url, headers=self._fabric_headers(), timeout=30)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                print(
                    "{} Network error querying item list: {}".format(
                        LOG_PREFIX_WARNING, str(e)[:200]
                    )
                )
                return None

            if resp.status_code != 200:
                print(
                    "{} Item list failed: HTTP {} - {}".format(
                        LOG_PREFIX_WARNING, resp.status_code, resp.text[:300]
                    )
                )
                return None

            data = resp.json()
            page_items = data.get("value", data.get("items", []))

            for item in page_items:
                if (
                    str(item.get("type", "")).strip().lower() == target_type
                    and str(item.get("displayName", "")).strip().lower() == target
                ):
                    matches.append(item)

            continuation_uri = data.get("continuationUri")
            continuation_token = data.get("continuationToken")
            if continuation_uri:
                url = continuation_uri
            elif continuation_token:
                base = "{}/workspaces/{}/items".format(
                    FABRIC_API_BASE_URL, self._workspace_id
                )
                sep = "&" if "?" in base else "?"
                url = "{}{}continuationToken={}".format(base, sep, continuation_token)
            else:
                url = None

        if len(matches) == 1:
            return matches[0].get("id")
        if len(matches) == 0:
            print(
                "{} No {} named '{}' found in workspace".format(
                    LOG_PREFIX_WARNING, item_type, display_name
                )
            )
        else:
            print(
                "{} Ambiguous: {} items named '{}' in workspace".format(
                    LOG_PREFIX_WARNING, len(matches), display_name
                )
            )
        return None

    # ------------------------------------------------------------------
    # Notebook discovery
    # ------------------------------------------------------------------

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """
        List notebook items in the current workspace.

        Returns:
            List of item dicts containing at least id/displayName/type.
        """
        url = "{}/workspaces/{}/items".format(FABRIC_API_BASE_URL, self._workspace_id)
        items: List[Dict[str, Any]] = []

        while url:
            resp = requests.get(url, headers=self._fabric_headers(), timeout=30)
            if resp.status_code != 200:
                raise FabricJobsClientError(
                    "Notebook list failed: HTTP {} - {}".format(resp.status_code, resp.text[:500])
                )

            data = resp.json()
            page_items = data.get("value")
            if page_items is None:
                page_items = data.get("items", [])

            for item in page_items:
                if str(item.get("type", "")).lower() == "notebook":
                    items.append(item)

            # Defensive pagination support for multiple response shapes.
            continuation_uri = data.get("continuationUri")
            continuation_token = data.get("continuationToken")
            if continuation_uri:
                url = continuation_uri
            elif continuation_token:
                url = "{}?continuationToken={}".format(
                    "{}/workspaces/{}/items".format(FABRIC_API_BASE_URL, self._workspace_id),
                    continuation_token,
                )
            else:
                url = None

        return items

    def resolve_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """
        Resolve a notebook ID by exact display name in this workspace.

        Returns:
            notebook item ID when exactly one match is found, None when no match.

        Raises:
            FabricJobsClientError when multiple notebooks share the same name.
        """
        if not notebook_name:
            return None

        notebooks = self.list_notebooks()
        matches = [
            n for n in notebooks
            if str(n.get("displayName", "")).strip().lower() == notebook_name.strip().lower()
        ]

        if len(matches) == 0:
            return None
        if len(matches) > 1:
            ids = [m.get("id", "?") for m in matches]
            raise FabricJobsClientError(
                "Notebook name '{}' is ambiguous in workspace {}. Matching IDs: {}".format(
                    notebook_name,
                    self._workspace_id,
                    ids,
                )
            )

        return matches[0].get("id")

    # ------------------------------------------------------------------
    # Notebook job submission + polling
    # ------------------------------------------------------------------

    def run_notebook(
        self,
        notebook_item_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        timeout_sec: int = JOB_TIMEOUT_SEC,
        poll_interval_sec: int = JOB_POLL_INTERVAL_SEC,
    ) -> Tuple[str, str]:
        """
        Submit a notebook job and poll until completion.

        Args:
            notebook_item_id: Fabric item ID of the notebook.
            parameters: Dict of notebook parameter overrides.
            timeout_sec: Max seconds to wait for job completion.
            poll_interval_sec: Seconds between poll requests.

        Returns:
            Tuple of (status, job_instance_id).
            status is one of: "Completed", "Failed", "Cancelled", "Timeout".
        """
        # --- Submit ---
        url = "{}/workspaces/{}/items/{}/jobs/instances?jobType=RunNotebook".format(
            FABRIC_API_BASE_URL, self._workspace_id, notebook_item_id
        )

        body: Dict[str, Any] = {}
        if parameters:
            body["executionData"] = {"parameters": parameters}

        print("{} Submitting notebook job: {}".format(LOG_PREFIX_INFO, notebook_item_id))
        try:
            resp = requests.post(
                url,
                headers=self._fabric_headers(),
                json=body if body else None,
                timeout=60,
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            raise FabricJobsClientError(
                "Network error submitting job: {}".format(str(e)[:300])
            )

        # 202 Accepted is the expected response
        if resp.status_code not in (200, 202):
            raise FabricJobsClientError(
                "Job submit failed: HTTP {} - {}".format(
                    resp.status_code, resp.text[:500]
                )
            )

        # Extract job instance ID from Location header or response body
        location = resp.headers.get("Location", "")
        job_instance_id = ""
        if location:
            # Location header format: .../jobs/instances/{jobInstanceId}
            parts = location.rstrip("/").split("/")
            job_instance_id = parts[-1] if parts else ""

        if not job_instance_id:
            # Fallback: try response body
            try:
                job_instance_id = resp.json().get("id", "")
            except Exception:
                pass

        if not job_instance_id:
            raise FabricJobsClientError(
                "Could not extract job instance ID from response."
            )

        print(
            "{} Job submitted. Instance ID: {}".format(
                LOG_PREFIX_INFO, job_instance_id
            )
        )

        # --- Poll ---
        poll_url = "{}/workspaces/{}/items/{}/jobs/instances/{}".format(
            FABRIC_API_BASE_URL, self._workspace_id, notebook_item_id, job_instance_id
        )

        elapsed = 0
        status = "Unknown"
        terminal_states = {"Completed", "Failed", "Cancelled", "Deduped"}

        while elapsed < timeout_sec:
            time.sleep(poll_interval_sec)
            elapsed += poll_interval_sec

            try:
                poll_resp = requests.get(
                    poll_url, headers=self._fabric_headers(), timeout=30
                )
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                print(
                    "{} Poll network error (will retry): {}".format(
                        LOG_PREFIX_WARNING, str(e)[:100]
                    )
                )

                continue
            if poll_resp.status_code != 200:
                print(
                    "{} Poll returned HTTP {} (will retry)".format(
                        LOG_PREFIX_WARNING, poll_resp.status_code
                    )
                )
                continue

            job_data = poll_resp.json()
            status = job_data.get("status", "Unknown")
            print(
                "{} Job status: {} (elapsed {}s)".format(
                    LOG_PREFIX_INFO, status, elapsed
                )
            )

            if status in terminal_states:
                return status, job_instance_id

        # Timeout reached - attempt to cancel
        print(
            "{} Job timed out after {}s. Attempting cancel.".format(
                LOG_PREFIX_WARNING, timeout_sec
            )
        )
        cancel_url = "{}/cancel".format(poll_url)
        try:
            requests.post(cancel_url, headers=self._fabric_headers(), timeout=15)
        except Exception:
            pass

        return "Timeout", job_instance_id

    # ------------------------------------------------------------------
    # OneLake DFS file reader
    # ------------------------------------------------------------------

    def read_onelake_file(
        self,
        lakehouse_id: str,
        file_path: str,
        workspace_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Read a file from the Lakehouse Files section via OneLake DFS API.

        Args:
            lakehouse_id: Fabric item ID of the Lakehouse.
            file_path: Relative path within the Lakehouse (e.g. Files/foo/bar.json).
            workspace_id: Optional override for the workspace that owns the lakehouse.
                          Defaults to the workspace this client was initialized with.

        Returns:
            File content as a string, or None if not found.
        """
        ws_id = workspace_id or self._workspace_id
        url = "{}/{}/{}/{}".format(
            ONELAKE_DFS_BASE_URL,
            ws_id,
            lakehouse_id,
            file_path,
        )
        print("{} Reading OneLake file: {}".format(LOG_PREFIX_INFO, file_path))
        resp = requests.get(url, headers=self._storage_headers(), timeout=60)

        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 404:
            print(
                "{} File not found: {}".format(LOG_PREFIX_WARNING, file_path)
            )
            return None

        print(
            "{} OneLake read failed: HTTP {} - {}".format(
                LOG_PREFIX_WARNING, resp.status_code, resp.text[:300]
            )
        )
        return None


# ======================================================================
# CLI entry point (for smoke testing and manual runs)
# ======================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fabric Jobs API client - smoke test and notebook runner"
    )
    parser.add_argument("--workspace_id", required=True, help="Fabric workspace ID")
    parser.add_argument("--smoke_test", action="store_true", help="Run connectivity check only")
    parser.add_argument("--notebook_item_id", default="", help="Notebook item ID to run")
    parser.add_argument("--parameters", default="{}", help="JSON string of notebook parameters")
    parser.add_argument("--timeout_sec", type=int, default=JOB_TIMEOUT_SEC, help="Job timeout")
    args = parser.parse_args()

    client = FabricJobsClient(workspace_id=args.workspace_id)

    if args.smoke_test:
        try:
            client.smoke_test()
            print("{} Smoke test passed.".format(LOG_PREFIX_SUCCESS))
            sys.exit(0)
        except FabricJobsClientError as e:
            print("{} {}".format(LOG_PREFIX_ERROR, e))
            sys.exit(1)

    if not args.notebook_item_id:
        print("{} --notebook_item_id is required when not using --smoke_test".format(LOG_PREFIX_ERROR))
        sys.exit(1)

    try:
        params = json.loads(args.parameters)
    except json.JSONDecodeError as e:
        print("{} Invalid JSON in --parameters: {}".format(LOG_PREFIX_ERROR, e))
        sys.exit(1)

    try:
        status, job_id = client.run_notebook(
            notebook_item_id=args.notebook_item_id,
            parameters=params,
            timeout_sec=args.timeout_sec,
        )
        print("{} Job finished: status={}, id={}".format(LOG_PREFIX_INFO, status, job_id))
        if status == "Completed":
            sys.exit(0)
        else:
            sys.exit(1)
    except FabricJobsClientError as e:
        print("{} {}".format(LOG_PREFIX_ERROR, e))
        sys.exit(1)


if __name__ == "__main__":
    main()
