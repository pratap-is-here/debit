# CI/CD templates

These are **copy-and-adapt templates** that wire the validator into your CI so it
actually gates pull requests. They are intentionally **not** active from this
folder — copy the one you need into the right place and edit it for your repo.

> Not to be confused with [`../.github/workflows/ci.yml`](../.github/workflows/ci.yml),
> which tests/lints *this framework's own* code. The templates here run the
> validator against *your* data-pipeline PRs.

| Template | For | What to do |
|----------|-----|------------|
| [`github-actions-pr-validation.yml`](github-actions-pr-validation.yml) | GitHub Actions | Copy to `.github/workflows/pr-validation.yml`, then set `METADATA_PATH` and the trigger branch. |
| [`azure-pipelines.yml`](azure-pipelines.yml) | Azure DevOps | Add as a pipeline, then set it as a **Build Validation branch policy** on your integration branch. |

## Two phases

- **Phase 1 — metadata + impact detection.** Fully **offline**: runs
  `detect_impact.py` (git diff → impacted tables) and `validate_pr.py` (parse the
  PR + validate contracts). No cloud credentials required. This is the hard gate.
- **Phase 2 — data presence (optional).** Runs the live row-count / freshness /
  schema checks in Microsoft Fabric via `trigger_data_presence.py`. **Off by
  default.** Enabling it requires an Entra identity with Fabric access and CI
  auth wiring — see [`../docs/operations-and-access.md`](../docs/operations-and-access.md).

## Before you enable Phase 2

Both templates authenticate to Fabric through `AzureCliCredential`, so the CI job
must first establish an `az` session:

- **GitHub Actions:** `azure/login` (OIDC) — set repository variable
  `ENABLE_PHASE2=true` and provide `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` /
  `AZURE_SUBSCRIPTION_ID` plus the `FABRIC_*` variables.
- **Azure DevOps:** an `AzureCLI@2` task with a **Workload Identity Federation**
  service connection — set the `azure_service_connection` and `fabric_*`
  variables.

The identity needs `Item.Execute` on the notebook workspace and OneLake read on
the lakehouse. Full setup (Entra app, tenant setting, workspace roles, branch
policy) is in [`../docs/operations-and-access.md`](../docs/operations-and-access.md).
