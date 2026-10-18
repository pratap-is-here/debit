# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for structural (schema) validation of the metadata files
(Step B: ``validate_metadata_shapes``).

These verify that the jsonschema-based shape check:
  * passes for the live repo metadata and for well-formed temp metadata,
  * accepts empty onboarding skeletons,
  * rejects typos (unknown keys), missing top-level keys, wrong types, and
    non-string list items — each with a clear, file-scoped message.
"""

from constants import METADATA_PATH
from metadata_parser import validate_metadata_shapes

VALID_SUBSET = "subset:\n  enabled: true\n  tables:\n    - a.b\n"
VALID_REGISTRY = "tables:\n  a.b: {}\n"
VALID_LINEAGE = "lineage:\n  a.b:\n    downstream: []\n"


def test_real_metadata_passes_shape_validation():
    assert validate_metadata_shapes(METADATA_PATH) == []


def test_valid_temp_metadata_passes(write_metadata):
    path = write_metadata(VALID_SUBSET, VALID_REGISTRY, VALID_LINEAGE)
    assert validate_metadata_shapes(path) == []


def test_empty_onboarding_skeletons_pass(write_metadata):
    # A brand-new onboarding skeleton: enabled present (required), everything else empty.
    path = write_metadata("subset:\n  enabled: false\n", "tables:\n", "lineage:\n")
    assert validate_metadata_shapes(path) == []


def test_typo_key_in_subset_is_rejected(write_metadata):
    bad_subset = "subset:\n  enabled: true\n  tabels:\n    - a.b\n"
    path = write_metadata(bad_subset, VALID_REGISTRY, VALID_LINEAGE)
    errors = validate_metadata_shapes(path)
    assert any("tabels" in e for e in errors)
    assert all("subset_scope.yml" in e for e in errors if "tabels" in e)


def test_missing_enabled_key_is_rejected(write_metadata):
    bad_subset = "subset:\n  tables:\n    - a.b\n"
    path = write_metadata(bad_subset, VALID_REGISTRY, VALID_LINEAGE)
    errors = validate_metadata_shapes(path)
    assert any("enabled" in e for e in errors)


def test_enabled_must_be_boolean(write_metadata):
    bad_subset = 'subset:\n  enabled: "yes"\n'
    path = write_metadata(bad_subset, VALID_REGISTRY, VALID_LINEAGE)
    errors = validate_metadata_shapes(path)
    assert any("enabled" in e and "boolean" in e for e in errors)


def test_missing_top_level_tables_key_is_rejected(write_metadata):
    path = write_metadata(VALID_SUBSET, "wrongkey:\n", VALID_LINEAGE)
    errors = validate_metadata_shapes(path)
    assert any("table_registry.yml" in e and "tables" in e for e in errors)


def test_non_string_downstream_is_rejected(write_metadata):
    bad_lineage = "lineage:\n  a.b:\n    downstream:\n      - 123\n"
    path = write_metadata(VALID_SUBSET, VALID_REGISTRY, bad_lineage)
    errors = validate_metadata_shapes(path)
    assert any("downstream" in e and "string" in e for e in errors)


def test_missing_file_is_reported(write_metadata):
    # Only write two of the three files; the missing one must surface an error.
    path = write_metadata(VALID_SUBSET, VALID_REGISTRY, lineage=None)
    errors = validate_metadata_shapes(path)
    assert any("table_lineage.yml" in e for e in errors)
