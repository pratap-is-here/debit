# Operations & access setup

The framework code is only half of what makes the gate run. The other half is
**out-of-band setup** — an identity, Fabric access, and CI wiring — that is not,
and cannot be, expressed in code. This guide lists everything you must set up so
that an adopting team can go from "cloned the repo" to "the gate runs on our
PRs."

> **How to read this.** Where this guide refers to *"the reference deployment,"*
> it means the internal deployment this framework was extracted from. Those exact
> resources are not shared here; instead, each step tells you the **equivalent**
> you must create in your own tenant.

## What actually needs access

Phase 1 (metadata + impact detection) is **offline** — it needs nothing beyond
Python and your repo. Everything below is for **Phase 2** (live data-presence and
freshness checks in Microsoft Fabric), which is optional but is the point of the
tool for most teams.

At runtime the Phase 2 client (`fabric_jobs_client.py`) authenticates with
`AzureCliCredential` and requests two token scopes:

| Scope | Used for |
|-------|----------|
| `https://api.fabric.microsoft.com/.default` | Run the notebook via the Fabric Jobs API |
| `https://storage.azure.com/.default` | Read the results file back from OneLake (DFS API) |

Because it uses `AzureCliCredential`, **the CI job must first establish an `az`
login session** (an `AzureCLI@2` task on Azure DevOps, or `azure/login` on GitHub
Actions). A raw environment-variable service principal is not picked up.

---

## 1. Identity — Entra service principal

Create (or reuse) a **Microsoft Entra application registration**; its service
principal is the identity that runs the checks.

1. Register an app: **Azure portal → Microsoft Entra ID → App registrations →
   New registration** (or `az ad sp create-for-rbac`, see the appendix).
2. Record the **Application (client) ID**, **Directory (tenant) ID**, and the
   **Subscription ID** you will use.
3. Do **not** add admin-consent-required Fabric API permissions to the app — for
   the Jobs API, workspace role membership (Section 2) is what grants access.

*The reference deployment used a service principal reached through a Workload
Identity Federation service connection; you must create the equivalent app in
your own tenant.*

---

## 2. Fabric access

### 2.1 Allow service principals to call Fabric APIs (tenant setting)
A Fabric admin must enable service-principal API access and scope it to a
security group:

1. Create a **security group** (Entra ID → Groups, type **Security**) and add
   your app as a member.
2. **Fabric admin portal → Tenant settings → Developer settings →
   "Service principals can use Fabric APIs"** → enable, and add the security
   group. (See *Enable service principal authentication for Fabric APIs* in the
   Microsoft Fabric docs.)

### 2.2 Give the identity workspace access
Add the service principal as a member of the Fabric workspace(s) with a role of
**Contributor or higher**. Two workspaces may be involved (the framework keeps
their IDs separate):

- the workspace **hosting the `data_presence_check` notebook** (`FABRIC_WORKSPACE_ID`), and
- the workspace **hosting the lakehouse** with your data (`FABRIC_LAKEHOUSE_WORKSPACE_ID`).

A Contributor (or higher) role grants both the ability to **run the notebook**
(`Item.Execute` / `Notebook.Execute`, required by the Fabric Jobs API) and
**OneLake data access** to read the results file and the tables being validated.

### 2.3 Capacity
The workspace(s) must be assigned to a **Fabric capacity** (an F SKU or a trial
capacity) — notebooks cannot execute otherwise.

### 2.4 Create the validation notebook and capture IDs
1. Import [`../fabric/data_presence_check.Notebook`](../fabric/data_presence_check.Notebook)
   into your notebook workspace (Git integration or **Import notebook**). Keep
   the name **`data_presence_check`**.
2. Attach your lakehouse as its default lakehouse.
3. Capture these values — they become the `FABRIC_*` / `DATA_PRESENCE_*`
   settings in [configuration-reference.md](configuration-reference.md):
   - notebook **workspace ID**
   - lakehouse **workspace ID** and lakehouse **item ID**
   - the notebook **item ID** (fallback; the client resolves by name first)

### 2.5 (Optional) Advanced-check notebooks
If you use `advanced_checks.yml`, create each advanced-check notebook in the
workspace and record its `notebook_id` in that file.

---

## 3. Permissions matrix

| Who / what | Needs | Why |
|------------|-------|-----|
| Entra app (service principal) | Membership in the security group allowed to use Fabric APIs | Call the Fabric REST API at all |
| Service principal | **Contributor+** on the notebook workspace | Run the notebook (`Item.Execute`) |
| Service principal | **Contributor+** on the lakehouse workspace | OneLake read of results + validated tables |
| Notebook workspace & lakehouse workspace | Assigned to a **Fabric capacity** | Notebook execution |
| CI service connection / OIDC credential | Federated trust to the app | Establish the `az` session the client needs |
| CI build identity | Read pull requests in the repo | Fetch the PR description (Phase 1) |
| Pipeline author | Project Admin or **Edit policies** on the repo | Add the build-validation branch policy |

---

## 4. CI wiring

Copy the matching template from
[`../pipeline-templates/`](../pipeline-templates/) and wire it as follows.

### 4.A Azure DevOps (matches the reference deployment)
1. **Service connection.** Create an **Azure Resource Manager service connection
   using Workload Identity Federation**, trusting the Entra app from Section 1.
   Put its name in the pipeline's `azure_service_connection` variable.
2. **Variable group / variables.** Set `metadata_path` and the `fabric_*` IDs
   from Section 2.4 (a variable group keeps them out of the YAML).
3. **OAuth token.** Ensure **"Allow scripts to access the OAuth token"** is on —
   the pipeline reads the PR description via `System.AccessToken`.
4. **Branch policy (this is what makes it a gate).** Add the pipeline as a
   **Build Validation** policy on your integration branch:
   **Repos → Branches → (branch) → Branch policies → Build Validation → +**.
   Without this, the pipeline exists but nothing requires it on PRs. (See *Set
   and manage branch policies* in the Azure Repos docs; requires Project Admin
   or **Edit policies** permission.)
5. **PR template.** Adopt [`../.github/PULL_REQUEST_TEMPLATE.md`](../.github/PULL_REQUEST_TEMPLATE.md)
   (or your ADO equivalent) so authors declare affected tables + scope mode.

### 4.B GitHub Actions
1. **Federated credential.** On the Entra app, add a **federated identity
   credential** trusting your GitHub repo (`entity: pull_request` and/or a
   branch), so `azure/login` can use OIDC with no stored secret.
2. **Repository variables/secrets.** Set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
   `AZURE_SUBSCRIPTION_ID`, the `FABRIC_*` variables, and `ENABLE_PHASE2=true`
   to turn on the optional Phase 2 job.
3. **Branch protection (the gate).** Add a branch protection rule requiring the
   `PR Validation` workflow's status check before merge.

---

## 5. Appendix — copy-and-adapt commands

> Replace every placeholder in angle brackets. These are starting points; follow
> your org's security guidance for production identities.

### 5.1 Create the Entra app / service principal (Azure CLI)
```bash
# Creates an app + service principal and prints appId (client id) and tenant.
az ad sp create-for-rbac --name "debit-pr-validator" --skip-assignment
# Note the "appId" (AZURE_CLIENT_ID) and "tenant" (AZURE_TENANT_ID) from the output.
```

### 5.2 Add a GitHub OIDC federated credential to the app
```bash
az ad app federated-credential create \
  --id <APP_OBJECT_ID> \
  --parameters '{
    "name": "github-pr-validation",
    "issuer": "https://token.actions.githubusercontent.com",
    "subject": "repo:<org>/<repo>:pull_request",
    "audiences": ["api://AzureADTokenExchange"]
  }'
```

### 5.3 GitHub Actions login snippet (already in the template)
```yaml
permissions:
  id-token: write
  contents: read
steps:
  - uses: azure/login@v2
    with:
      client-id: ${{ secrets.AZURE_CLIENT_ID }}
      tenant-id: ${{ secrets.AZURE_TENANT_ID }}
      subscription-id: ${{ secrets.AZURE_SUBSCRIPTION_ID }}
  # trigger_data_presence.py then authenticates via AzureCliCredential.
```

### 5.4 Azure DevOps service connection
Create it in **Project settings → Service connections → New → Azure Resource
Manager → Workload Identity federation**, point it at the Entra app, and use its
name as `azure_service_connection` in `pipeline-templates/azure-pipelines.yml`.

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `[SKIP] FABRIC_WORKSPACE_ID is not set …` | Phase 2 not configured | Set `FABRIC_WORKSPACE_ID` / `--workspace_id` (Section 2.4). Expected on a Phase-1-only setup. |
| `InsufficientPrivileges` / HTTP 401/403 from the Jobs API | SP lacks Fabric API access or a workspace role | Enable the tenant SP setting (2.1) and add the SP as **Contributor+** on the workspace (2.2). |
| HTTP 401/403 reading the results file (OneLake) | SP has no OneLake access to the lakehouse | Add the SP to the **lakehouse** workspace (2.2); confirm `FABRIC_LAKEHOUSE_*` IDs. |
| Job never starts / times out | Workspace not on a capacity, or wrong notebook/workspace ID | Assign a capacity (2.3); re-check the captured IDs (2.4). |
| `Invalid notebook workspace ID '...'` | A non-empty but malformed ID was supplied | Provide a valid GUID for `FABRIC_WORKSPACE_ID` / `--workspace_id`. |
| PR description empty in the run | OAuth token not allowed, or build identity can't read PRs | Enable OAuth token access (4.A.3); grant the build identity PR read. |
| Pipeline runs but PRs merge without it | No branch policy / protection | Add the Build Validation policy (ADO, 4.A.4) or required check (GitHub, 4.B.3). |

---

For the metadata contract and env-var reference, see
[configuration-reference.md](configuration-reference.md); for the notebook's
input/output contract, see [`../fabric/README.md`](../fabric/README.md); for the
CI templates, see [`../pipeline-templates/`](../pipeline-templates/).
