# Quickstart

## 1. Install

```bash
python -m venv .venv
. .venv/Scripts/activate         # Windows
# source .venv/bin/activate      # macOS / Linux

pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## 2. Run the bundled example

```bash
python examples/run_validation_example.py
```

## 3. Run the tests (offline)

```bash
python -m pytest -q
```

## 4. Point it at your own metadata

```bash
# Windows PowerShell
$env:METADATA_PATH = "C:\path\to\your\metadata"
python src/validate_pr.py --pr_description_file examples/sample_pr_description.md
```

See [configuration-reference.md](configuration-reference.md) for the metadata
file formats and [`../config/`](../config/) for templates.

## 5. (Optional) Enable Phase 2 data-presence checks

The live checks against Microsoft Fabric run in the `data_presence_check`
notebook shipped under [`../fabric/`](../fabric/). Import it into your workspace
and set the `FABRIC_*` environment variables — see
[`../fabric/README.md`](../fabric/README.md) for the notebook contract and
[operations-and-access.md](operations-and-access.md) for the full identity,
Fabric-access, and CI-wiring setup.
