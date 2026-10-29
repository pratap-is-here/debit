# Contributing

Thank you for your interest in contributing to DEBIT (Data Engineering Build Impact Testing).

## Contribution guidelines

Contributions are welcome through GitHub issues and pull requests. Please keep
changes focused, include tests for behavior changes, and avoid committing
secrets, internal IDs, or environment-specific paths.

## Development setup

```bash
# From the repository root
python -m venv .venv
. .venv/Scripts/activate        # Windows
# source .venv/bin/activate     # macOS / Linux

pip install -r requirements.txt
pip install -r requirements-dev.txt
```

## Running the tests

The full test suite is **offline** — it requires no network, cloud, or Fabric
access:

```bash
python -m pytest -q
```

All tests must pass before a pull request is merged.

## Project conventions

- The framework modules in `src/` use **bare imports** (e.g.
  `from constants import ...`). The test suite puts `src/` on `sys.path` via
  `tests/conftest.py`; keep that convention when adding modules.
- All paths, IDs, and tunable values live in `src/constants.py` and are driven
  by environment variables or CLI arguments. **Do not hardcode** secrets,
  tenant/workspace/lakehouse IDs, internal URLs, or absolute paths.
- New per-table contract checks belong in
  `metadata_parser.collect_contract_warnings`.
- Add or update tests under `tests/` for any behavior change.

## Pull request checklist

- [ ] Tests pass locally (`python -m pytest -q`).
- [ ] No secrets, internal IDs, or internal paths introduced.
- [ ] New configuration is documented in `config/` and `docs/`.
- [ ] Public behavior changes are reflected in the README/docs.
