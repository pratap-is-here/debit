# Configuration reference

All configuration is **environment-driven** (see `src/constants.py`) and
**metadata-driven** (the YAML files under your `METADATA_PATH`). Nothing is
hardcoded.

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `METADATA_PATH` | `examples/metadata` | Directory containing the metadata YAML files. |
| `REPO_ROOT` | repo root | Base path for resolving notebook/pipeline folders. |
| `NOTEBOOKS_ROOT` | `<repo>/notebooks` | Root of notebook source folders. |
| `PIPELINES_ROOT` | `<repo>/pipelines` | Root of pipeline source folders. |
| `FABRIC_API_BASE_URL` | public Fabric API | Override for sovereign clouds. |
| `ONELAKE_DFS_BASE_URL` | public OneLake DFS | Override for sovereign clouds. |
| `FABRIC_WORKSPACE_ID` | _(empty)_ | Workspace hosting the validation notebook. |
| `FABRIC_LAKEHOUSE_ID` | _(empty)_ | Lakehouse item ID. |
| `FABRIC_LAKEHOUSE_WORKSPACE_ID` | _(empty)_ | Workspace hosting the lakehouse. |
| `DATA_PRESENCE_NOTEBOOK_ITEM_ID` | _(empty)_ | Fabric item ID of the data-presence notebook. |
| `DATA_PRESENCE_NOTEBOOK_NAME` | `data_presence_check` | Notebook display name resolved in the workspace (the item ID above is the fallback). |
| `DATA_PRESENCE_RESULTS_PATH` | `Files/pr_validation/results` | OneLake path where the notebook writes its results JSON. |
| `ALLOWED_NOTEBOOK_WORKSPACE_IDS` | _(empty)_ | Comma-separated allowlist for PR overrides. |
| `ALLOWED_LAKEHOUSE_WORKSPACE_IDS` | _(empty)_ | Comma-separated allowlist for PR overrides. |
| `ENABLE_PR_RUNTIME_OVERRIDES` | `true` | Allow a PR description to target a different notebook/lakehouse workspace. A requested workspace is honored **only** if it appears in the matching allowlist above; otherwise the request is ignored and the base/default workspace is used (fail-safe). Set `false` to disable PR-driven overrides entirely. |

> Some knobs are **CLI-only** (not environment variables): `--retention_months`
> (default 3), `--mode` (`soft`/`hard`), `--scope_mode`, and `--adv_checks_scope`.
> Run `python src/trigger_data_presence.py --help` for the full list.

## Metadata files

See the annotated templates in [`../config/`](../config/) and the complete
worked example in [`../examples/metadata/`](../examples/metadata/):

- `subset_scope.yml`
- `table_registry.yml`
- `table_lineage.yml`
- `notebook_outputs.yml`
- `pipeline_outputs.yml`
- `execution_freshness.yml`
- `advanced_checks.yml`

## Phase 2 (Fabric) setup

Running the live data-presence checks requires importing the
`data_presence_check` notebook into your Fabric workspace and setting the
`FABRIC_*` / `DATA_PRESENCE_NOTEBOOK_ITEM_ID` variables above. See
[`../fabric/README.md`](../fabric/README.md) for the import steps and the
notebook's input/output contract, and
[operations-and-access.md](operations-and-access.md) for the identity, Fabric
permissions, and CI wiring that Phase 2 needs.
