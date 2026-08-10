# Examples

This folder contains a **fully synthetic** "retail sales" dataset and runnable
examples. None of it references any real system, tenant, workspace, or data.

## Contents

| Path | Purpose |
|------|---------|
| [`metadata/`](metadata/) | A complete, internally consistent set of the seven metadata files the framework reads. This is also the **default** `METADATA_PATH`, so the framework and its tests run out of the box. |
| [`run_validation_example.py`](run_validation_example.py) | Drives `validate_pr.validate` against the synthetic metadata for every lineage scope mode. |
| [`sample_pr_description.md`](sample_pr_description.md) | A pull-request description in the format the framework parses. |

## Run it

From the repository root:

```bash
python examples/run_validation_example.py
```

Expected output is a `PASS` line for each scope mode (`affected_only`,
`upstream`, `downstream`, `full_lineage`).

## The synthetic domain

The example models a tiny medallion-style sales warehouse:

```
silver.dim_customer ─┐
                     ├─> silver.fact_orders ─┬─> gold.daily_sales_summary ─> Files/exports/sales/daily_sales_csv
silver.dim_product ──┘                       └─> gold.product_revenue_summary
silver.dim_product ──────────────────────────────> gold.product_revenue_summary
```

Use it as a template for your own metadata — see [`../config/`](../config/) for
annotated, placeholder-only templates.
