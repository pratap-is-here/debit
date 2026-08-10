# Contributing

Thank you for your interest in contributing to the Data Pipeline Change Validator!

## Contributor License Agreement

This project welcomes contributions and suggestions. Most contributions require
you to agree to a Contributor License Agreement (CLA) declaring that you have
the right to, and actually do, grant us the rights to use your contribution. For
details, visit [https://cla.opensource.microsoft.com](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether
you need to provide a CLA and decorate the PR appropriately (e.g., status check,
comment). Simply follow the instructions provided by the bot. You will only need
to do this once across all repositories using our CLA.

This project has adopted the
[Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the
[Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any
additional questions or comments.

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
