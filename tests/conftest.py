# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Shared pytest fixtures and import setup for the testing-framework test suite.

The framework modules use bare imports (``from constants import ...``,
``from metadata_parser import ...``), so the ``src`` directory must be on
``sys.path`` before any test module imports them. This conftest guarantees that
and provides a ``write_metadata`` factory for building isolated temp metadata.
"""

import os
import sys

import pytest

# Make the framework modules importable (they rely on the src dir being on
# sys.path, exactly as a CI runner would invoke them).
SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from constants import (  # noqa: E402  (import after sys.path setup is intentional)
    SUBSET_SCOPE_FILE,
    TABLE_LINEAGE_FILE,
    TABLE_REGISTRY_FILE,
)


@pytest.fixture
def write_metadata(tmp_path):
    """Factory fixture that writes the three core metadata files to a temp dir.

    Usage:
        path = write_metadata(subset=..., registry=..., lineage=...)

    Any argument left as ``None`` is simply not written, which is useful for
    exercising missing-file behaviour. Returns the temp metadata directory path
    that can be passed straight into the loader classes / validators.
    """

    def _write(subset=None, registry=None, lineage=None):
        files = {
            SUBSET_SCOPE_FILE: subset,
            TABLE_REGISTRY_FILE: registry,
            TABLE_LINEAGE_FILE: lineage,
        }
        for name, text in files.items():
            if text is not None:
                (tmp_path / name).write_text(text, encoding="utf-8")
        return str(tmp_path)

    return _write
