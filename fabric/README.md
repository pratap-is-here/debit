# Phase 2: the `data_presence_check` Fabric notebook

Phase 1 of this framework (PR parsing, metadata validation, lineage scope) is
pure Python and runs anywhere with no cloud access. **Phase 2** runs the actual
data-quality checks (row count, freshness, schema, not-null, custom SQL) against
live data in a Microsoft Fabric lakehouse. Those checks execute inside this
notebook, which `src/trigger_data_presence.py` launches through the Fabric
Jobs API.

This folder ships the notebook so adopters have a working Phase 2 out of the
box. It is a **template**: it contains no real workspace, lakehouse, or table
identifiers — you point it at your own environment.

## What is here

```
fabric/data_presence_check.Notebook/
├── notebook-content.py   # the notebook source (Fabric "git integration" format)
└── .platform             # Fabric item metadata (placeholder logicalId + lakehouse)
```

## Importing it into Fabric

1. In your Fabric workspace, add this notebook (Git integration, or **Import
   notebook**). Keep the name **`data_presence_check`** — `trigger_data_presence.py`
   resolves the notebook by that name first, then by item ID.
2. **Attach your own lakehouse** as the notebook's default lakehouse. The shipped
   `.platform` / `META` block uses all-zero placeholder GUIDs (`00000000-…`) and
   the name `YourLakehouse` on purpose; Fabric rebinds these when you attach a
   real lakehouse.
3. If you rely on the optional `abfss://` write fallback, also pass the
   `lakehouse_workspace_id` and `lakehouse_id` parameters (they default to empty,
   which disables the fallback — the primary `mssparkutils.fs.put` write is used).

Then set the framework's environment variables (see
[`../docs/configuration-reference.md`](../docs/configuration-reference.md)) so
`trigger_data_presence.py` can find the workspace, lakehouse, and this notebook.

## Parameters (injected by `trigger_data_presence.py`)

| Parameter | Meaning |
|-----------|---------|
| `tables_json` | JSON array of per-table contracts to validate (see below). |
| `results_path` | Lakehouse `Files/…` path the notebook writes results JSON to. |
| `mode` | `soft` (warn only) or `hard` (raise on any failure/error). |
| `run_id` | Correlation id echoed into the results. |
| `retention_months` | How long to keep old result files (cleanup runs at the end). |
| `lakehouse_workspace_id`, `lakehouse_id` | Optional; enable the `abfss://` write fallback only. |

## Input contract: `tables_json`

Built by `trigger_data_presence.build_tables_payload()` from your
`table_registry.yml`. Each element:

```json
{
  "table_name": "silver.fact_orders",
  "min_rows": 1000,
  "row_count_mode": "latest_available_date",
  "min_rows_date_check": -1,
  "freshness_hours": 24,
  "freshness_column": "order_date",
  "required_columns": ["order_id", "customer_id", "order_date"],
  "not_null_columns": ["order_id", "customer_id"],
  "checks": [
    { "name": "amount_non_negative",
      "sql": "SELECT COUNT(1) FROM {table} WHERE order_amount < 0",
      "expect_eq": 0 }
  ]
}
```

- `row_count_mode`: `total`, `latest_available_date`, or `business_offset`. When
  omitted it is inferred from `freshness_column` / `min_rows_date_check`.
- Custom `checks` support `expect_eq`, `expect_lt`, `expect_gt`; `{table}` in
  `sql` is substituted with the table name. Not-null and custom checks apply to
  managed tables only (skipped for `Files/…` sources).

## Output contract: results JSON

Written to `results_path` and read back by `trigger_data_presence.py`:

```json
{
  "run_id": "…",
  "mode": "soft",
  "timestamp": "2026-01-01T00:00:00Z",
  "tables": {
    "silver.fact_orders": {
      "passed": true,
      "checks": [ { "name": "row_count", "status": "passed", "actual": 1200, "expected_min": 1000 } ]
    }
  },
  "summary": { "total": 1, "passed": 1, "failed": 0, "skipped": 0, "error": 0 }
}
```

Per-check `status` is one of `passed`, `failed`, `skipped`, `error`. In `hard`
mode the notebook raises if any table has a `failed` or `error` check; in `soft`
mode it only logs a warning.
