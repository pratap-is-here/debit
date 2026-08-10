# Data Pipeline Change Validator

A lightweight, metadata-driven framework for validating data-platform pull
requests **before** they merge. It catches data-quality regressions early by
combining three ideas:

1. **PR validation** — parse a pull request for the tables it affects and the
   validation scope requested, then check those tables against declared
   contracts.
2. **Metadata-driven checks** — row counts, freshness, required columns,
   not-null constraints, and custom SQL assertions are declared as data
   (YAML), not code.
3. **Lineage-based scope** — a downstream-only dependency graph lets you expand
   validation from the changed tables to their upstream and/or downstream
   neighbours.

The framework targets [Microsoft Fabric](https://learn.microsoft.com/fabric/)
data engineering workflows (notebooks, pipelines, lakehouse tables) but the core
engine — PR parsing, metadata loading, consistency checks, and lineage scope —
is plain Python and has **no cloud dependency**. The entire test suite runs
fully offline.

> Status: extracted for public release. The bundled example dataset and tests
> are fully synthetic and contain no real systems, tenants, or data.

## Key concepts

| Concept | File | What it does |
|---------|------|--------------|
| Subset scope | `subset_scope.yml` | Which tables / notebooks / pipelines are governed. |
| Table registry | `table_registry.yml` | Per-table data-quality contracts. |
| Lineage | `table_lineage.yml` | Downstream-only DAG; upstream is derived at runtime. |
| Impact detection | `notebook_outputs.yml`, `pipeline_outputs.yml` | Map a git diff to impacted tables. |
| Freshness gate | `execution_freshness.yml` | Block/warn when producing jobs run stale. |
| Advanced checks | `advanced_checks.yml` | Register complex multi-table check notebooks. |

## Repository layout

```
src/        Core framework modules (plain Python).
config/     Annotated, placeholder-only metadata templates.
examples/   A complete synthetic dataset + runnable example.
tests/      Offline unit/integration test suite.
docs/       Architecture and usage documentation.
fabric/     Phase 2 data-presence notebook for Microsoft Fabric.
pipeline-templates/  Copy-and-adapt CI templates (GitHub Actions + Azure DevOps).
```

## Getting started

### Prerequisites

- Python 3.9+

### Install

```bash
python -m venv .venv
. .venv/Scripts/activate         # Windows
# source .venv/bin/activate      # macOS / Linux

pip install -r requirements.txt
pip install -r requirements-dev.txt   # only needed to run the tests
```

### Run the example

The framework ships with a synthetic "retail sales" dataset under
`examples/metadata`, which is also the default `METADATA_PATH`.

```bash
python examples/run_validation_example.py
```

You should see a `PASS` for each lineage scope mode.

### Run the tests

```bash
python -m pytest -q
```

The suite is fully offline (HTTP and credentials are stubbed; metadata is built
in temporary directories), so it runs without any internal or cloud access.

## Using it with your own data

1. Copy the templates in [`config/`](config/) into your own metadata directory.
2. Fill in your tables, contracts, and lineage (see
   [`examples/metadata/`](examples/metadata/) for a complete worked example).
3. Point the framework at your metadata and Fabric environment via environment
   variables or CLI arguments — **nothing is hardcoded**:

   | Variable | Purpose |
   |----------|---------|
   | `METADATA_PATH` | Directory containing your metadata YAML files. |
   | `FABRIC_WORKSPACE_ID` | Workspace hosting the validation notebook. |
   | `FABRIC_LAKEHOUSE_ID` / `FABRIC_LAKEHOUSE_WORKSPACE_ID` | Lakehouse identity. |
   | `DATA_PRESENCE_NOTEBOOK_ITEM_ID` | Fabric item ID of the data-presence notebook. |

   See [`src/constants.py`](src/constants.py) for the full list.

4. Run the PR validation step in your CI. Copy a ready-made pipeline from
   [`pipeline-templates/`](pipeline-templates/) (GitHub Actions or Azure
   DevOps) and follow [`docs/operations-and-access.md`](docs/operations-and-access.md)
   to wire identity, Fabric access, and the branch policy that makes it a gate.
   The framework emits Azure Pipelines logging commands (`##vso[...]`) for Azure
   DevOps; these are harmless no-ops on other CI systems.

See [`docs/`](docs/) for architecture and configuration details.

## Contributing

This project welcomes contributions and suggestions. See
[CONTRIBUTING.md](CONTRIBUTING.md) for details, including the Microsoft CLA.

This project has adopted the
[Microsoft Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## Security

Please report security issues as described in [SECURITY.md](SECURITY.md). Do not
file security vulnerabilities as public GitHub issues.

## License

Released under the [MIT License](LICENSE).

## Trademarks

This project may contain trademarks or logos for projects, products, or
services. Authorized use of Microsoft trademarks or logos is subject to and must
follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must
not cause confusion or imply Microsoft sponsorship. Any use of third-party
trademarks or logos is subject to those third parties' policies.
