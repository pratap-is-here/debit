# Configuration templates

These are **annotated, placeholder-only templates** for the seven metadata
files the framework reads. They document every supported field but contain no
real values.

## How to use

1. Copy the files you need into your own metadata directory (e.g. `metadata/`).
2. Replace every `<placeholder>` with real values for your tables.
3. Point the framework at the directory via the `METADATA_PATH` environment
   variable or the `--metadata_path` CLI argument.

| File | Required? | Purpose |
|------|-----------|---------|
| `subset_scope.yml` | Yes | What tables / notebooks / pipelines are in scope. |
| `table_registry.yml` | Yes | Per-table data-quality contracts. |
| `table_lineage.yml` | Yes | Downstream-only dependency graph. |
| `notebook_outputs.yml` | For diff-based detection | Notebook → tables mapping. |
| `pipeline_outputs.yml` | For diff-based detection | Pipeline → tables mapping. |
| `execution_freshness.yml` | For the freshness gate | Freshness gating config. |
| `advanced_checks.yml` | Optional | Advanced check-notebook registry. |

For a complete, working set of these files see
[`../examples/metadata/`](../examples/metadata/).
