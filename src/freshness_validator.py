# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Freshness Validator for lineage-aware test gating.

Validates that downstream data has been refreshed after upstream changes,
using two complementary checks:
    Check A: Edge-by-edge upstream vs downstream timestamps
    Check B: Absolute staleness window for every scoped table

See LINEAGE_FRESHNESS_DESIGN.md for full design rationale.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from constants import (
    LOG_PREFIX_ERROR,
    LOG_PREFIX_INFO,
    LOG_PREFIX_SUCCESS,
    LOG_PREFIX_WARNING,
)
from fabric_jobs_client import FabricJobsClient
from metadata_parser import (
    NotebookOutputs,
    PipelineOutputs,
    TableLineage,
    TableRegistry,
)

# Severity ranking for picking the worst result across diamond paths.
_STATUS_SEVERITY = {"fresh": 0, "no_record": 1, "stale": 2}


def _severity(status: str) -> int:
    return _STATUS_SEVERITY.get(status, -1)


class FreshnessValidator:
    """
    Validates lineage freshness using two complementary checks:
      Check A: Upstream vs downstream timestamps (catches forgotten downstream runs)
      Check B: Absolute staleness window over every scoped table (catches
               never-run tables and tables that ran too long ago)
    """

    def __init__(
        self,
        client: FabricJobsClient,
        lineage: TableLineage,
        notebook_outputs: NotebookOutputs,
        pipeline_outputs: PipelineOutputs,
        tolerance_minutes: int = 30,
        default_max_age_hours: int = 48,
        registry: Optional[TableRegistry] = None,
    ):
        self._client = client
        self._lineage = lineage
        self._nb_outputs = notebook_outputs
        self._pl_outputs = pipeline_outputs
        self._tolerance = timedelta(minutes=tolerance_minutes)
        self._default_max_age_hours = default_max_age_hours
        self._registry = registry
        # Cache: item name -> resolved Fabric item ID (or None if not found)
        self._item_id_cache: Dict[str, Optional[str]] = {}

    def _staleness_threshold_for(self, table: str) -> timedelta:
        """Resolve the absolute-staleness threshold for a table.

        Cascade: per-table ``execution_freshness_hours`` in table_registry.yml
        -> ``default_max_age_hours`` (org-wide config fallback). When no
        registry is wired, the default is used for every table.
        """
        if self._registry is not None:
            hours = self._registry.get_execution_freshness_hours(
                table, self._default_max_age_hours
            )
        else:
            hours = self._default_max_age_hours
        return timedelta(hours=hours)

    def _resolve_item_id(self, name: str, item_type: str) -> Optional[str]:
        """Resolve a Fabric item display name to its ID, with caching."""
        cache_key = "{}::{}".format(item_type, name)
        if cache_key not in self._item_id_cache:
            self._item_id_cache[cache_key] = self._client.resolve_item_id_by_name(
                name, item_type
            )
        return self._item_id_cache[cache_key]

    def get_last_execution_time(self, table: str) -> Optional[datetime]:
        """
        Query Fabric for the last successful execution of the
        pipeline/notebook that produces this table.

        Checks both pipeline and notebook outputs metadata, resolves
        names to Fabric item IDs, and returns the most recent timestamp.
        """
        producing_pipeline = self._pl_outputs.find_pipeline_for_table(table)
        producing_notebook = self._nb_outputs.find_notebook_for_table(table)

        print("{} [freshness] Table '{}': producing_pipeline={}, producing_notebook={}".format(
            LOG_PREFIX_INFO, table, producing_pipeline, producing_notebook
        ))

        timestamps = []

        if producing_pipeline:
            # Strip .DataPipeline suffix for display name resolution
            pl_display_name = producing_pipeline
            if pl_display_name.endswith(".DataPipeline"):
                pl_display_name = pl_display_name[: -len(".DataPipeline")]
            item_id = self._resolve_item_id(pl_display_name, "DataPipeline")
            print("{} [freshness]   Pipeline '{}' -> item_id={}".format(
                LOG_PREFIX_INFO, pl_display_name, item_id
            ))
            if item_id:
                ts = self._client.get_last_successful_run_time(
                    item_id=item_id,
                    item_type="DataPipeline",
                )
                print("{} [freshness]   Pipeline last_run={}".format(
                    LOG_PREFIX_INFO, ts.isoformat() if ts else None
                ))
                if ts:
                    timestamps.append(ts)

        if producing_notebook:
            # Strip .Notebook suffix for display name resolution
            nb_display_name = producing_notebook
            if nb_display_name.endswith(".Notebook"):
                nb_display_name = nb_display_name[: -len(".Notebook")]
            item_id = self._resolve_item_id(nb_display_name, "Notebook")
            print("{} [freshness]   Notebook '{}' -> item_id={}".format(
                LOG_PREFIX_INFO, nb_display_name, item_id
            ))
            if item_id:
                ts = self._client.get_last_successful_run_time(
                    item_id=item_id,
                    item_type="Notebook",
                )
                print("{} [freshness]   Notebook last_run={}".format(
                    LOG_PREFIX_INFO, ts.isoformat() if ts else None
                ))
                if ts:
                    timestamps.append(ts)

        return max(timestamps) if timestamps else None

    def validate(
        self,
        directly_impacted: Set[str],
        tainted_tables: Set[str],
        now: Optional[datetime] = None,
        exempt_tables: Optional[Set[str]] = None,
    ) -> Dict[str, Tuple[str, str]]:
        """
        Run both freshness checks on tainted tables.

        Args:
            directly_impacted: Tables whose producing code was changed.
            tainted_tables: Full tainted set (directly_impacted + downstream).
            now: Current UTC time (injectable for testing).
            exempt_tables: Tables to skip.

        Returns:
            Dict of table -> (status, reason)
            status: "fresh" | "stale" | "exempt" | "no_record"
        """
        if now is None:
            now = datetime.now(timezone.utc)
        if exempt_tables is None:
            exempt_tables = set()

        results: Dict[str, Tuple[str, str]] = {}

        # Collect timestamps for all tainted tables
        timestamps: Dict[str, Optional[datetime]] = {}
        for table in tainted_tables:
            if table in exempt_tables:
                results[table] = ("exempt", "Table is exempt from freshness gating")
                continue
            timestamps[table] = self.get_last_execution_time(table)

        # --- Check A: Edge-by-Edge Upstream vs Downstream Timestamps ---
        edge_results: Dict[str, Tuple[str, str]] = {}

        for parent_table in tainted_tables:
            if parent_table in exempt_tables:
                continue
            parent_ts = timestamps.get(parent_table)
            if parent_ts is None:
                continue

            for child_table in self._lineage.get_downstream(parent_table):
                # By design, edge freshness is evaluated only when BOTH endpoints
                # are in the scoped (tainted) set. A parent->child timestamp
                # comparison is only meaningful when both ends are in validation
                # scope; we do not infer staleness across an edge that leaves the
                # scope. The directly-impacted set is still covered by Check B
                # (absolute staleness) below.
                if child_table not in tainted_tables or child_table in exempt_tables:
                    continue
                child_ts = timestamps.get(child_table)

                if child_ts is None:
                    candidate = (
                        "no_record",
                        "No execution record; upstream {} was run at {}".format(
                            parent_table, parent_ts.isoformat()
                        ),
                    )
                elif child_ts >= (parent_ts - self._tolerance):
                    candidate = (
                        "fresh",
                        "Ran at {}, after upstream {} at {}".format(
                            child_ts.isoformat(),
                            parent_table,
                            parent_ts.isoformat(),
                        ),
                    )
                else:
                    candidate = (
                        "stale",
                        "Ran at {}, BEFORE upstream {} at {}".format(
                            child_ts.isoformat(),
                            parent_table,
                            parent_ts.isoformat(),
                        ),
                    )

                # For diamond dependencies: keep the worst result.
                existing = edge_results.get(child_table)
                if existing is None or _severity(candidate[0]) > _severity(existing[0]):
                    edge_results[child_table] = candidate

        results.update(edge_results)

        # --- Check B: Absolute Staleness Window ---
        # Applied to EVERY scoped (tainted) non-exempt table, not just the
        # directly-impacted ones. This independently catches never-run tables
        # (no_record) and tables that ran too long ago (stale) even when they
        # never appear as a child in a scoped Check A edge -- closing the
        # never-run-parent blind spot. Per-table thresholds come from
        # execution_freshness_hours; results merge by severity so a Check B
        # 'stale' can override a Check A 'fresh' but never the reverse.
        for table in tainted_tables:
            if table in exempt_tables:
                continue
            ts = timestamps.get(table)

            if ts is None:
                candidate = (
                    "no_record",
                    "No execution record found in Fabric",
                )
            else:
                threshold = self._staleness_threshold_for(table)
                age = now - ts
                if age > threshold:
                    candidate = (
                        "stale",
                        "Last execution was {}d {}h ago (threshold: {:.0f}h). "
                        "Re-run the producing pipeline/notebook to validate "
                        "your changes.".format(
                            age.days,
                            age.seconds // 3600,
                            threshold.total_seconds() / 3600,
                        ),
                    )
                else:
                    candidate = (
                        "fresh",
                        "Last execution at {} (within staleness window)".format(
                            ts.isoformat()
                        ),
                    )

            existing = results.get(table)
            if existing is None or _severity(candidate[0]) > _severity(existing[0]):
                results[table] = candidate

        return results


def summarize_results(
    results: Dict[str, Tuple[str, str]],
    mode: str,
) -> bool:
    """
    Print a summary of freshness validation results.

    Both ``stale`` (ran too long ago, or before an upstream change) and
    ``no_record`` (never ran) are treated as freshness *failures*. The mode
    decides whether failures block the gate:

    Args:
        results: Output from FreshnessValidator.validate()
        mode: "soft" (report failures but never block), "hard" (block on any
            failure), "skip" (no-op gate).

    Returns:
        True if the gate passes (always in soft/skip; in hard only when there
        are no failures), False only in hard mode when failures exist.
    """
    if mode == "skip":
        print("{} Freshness gating is disabled (mode=skip)".format(LOG_PREFIX_INFO))
        return True

    stale_count = 0
    fresh_count = 0
    no_record_count = 0

    for table, (status, reason) in sorted(results.items()):
        if status == "stale":
            print("{} {} - STALE: {}".format(LOG_PREFIX_ERROR, table, reason))
            stale_count += 1
        elif status == "no_record":
            print("{} {} - NO RECORD: {}".format(LOG_PREFIX_ERROR, table, reason))
            no_record_count += 1
        elif status == "exempt":
            print("{} {} - exempt".format(LOG_PREFIX_INFO, table))
        else:
            print("{} {} - fresh".format(LOG_PREFIX_SUCCESS, table))
            fresh_count += 1

    failure_count = stale_count + no_record_count

    print(
        "\n{} Freshness summary: {} fresh, {} stale, {} no-record".format(
            LOG_PREFIX_INFO, fresh_count, stale_count, no_record_count
        )
    )

    if failure_count == 0:
        print("{} Freshness gate passed".format(LOG_PREFIX_SUCCESS))
        return True

    if mode == "hard":
        print(
            "{} Freshness gate FAILED ({} stale, {} no-record in hard mode)".format(
                LOG_PREFIX_ERROR, stale_count, no_record_count
            )
        )
        return False

    # Soft mode: surface failures loudly but never block the PR.
    print(
        "{} Freshness gate found {} failure(s) ({} stale, {} no-record) "
        "but is NOT blocking (mode=soft)".format(
            LOG_PREFIX_WARNING, failure_count, stale_count, no_record_count
        )
    )
    return True
