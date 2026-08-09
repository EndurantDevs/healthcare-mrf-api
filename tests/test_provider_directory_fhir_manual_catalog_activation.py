# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict manual-catalog boundaries used by selector-free activation."""

from __future__ import annotations

from copy import deepcopy
import json

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog


MANIFEST_PATH = manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST


def _manifest_document() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _manual_entry(manifest_by_field: dict) -> dict:
    manual_entries = [
        entry_by_field
        for entry_by_field in manifest_by_field["entries"]
        if entry_by_field.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    ]
    assert len(manual_entries) == 1
    return manual_entries[0]


def _write_manifest(tmp_path, manifest_by_field: dict):
    manifest_path = tmp_path / "provider-directory-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest_by_field, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


@pytest.mark.parametrize(
    "duplicate_field",
    ("classification", "source_ids"),
)
def test_manual_manifest_rejects_duplicate_json_members(
    tmp_path,
    duplicate_field,
):
    """Reject ambiguous authorization fields before source resolution."""

    raw_manifest = MANIFEST_PATH.read_text(encoding="utf-8")
    field_fragment = f'"{duplicate_field}":'
    field_offset = raw_manifest.index(field_fragment)
    field_value = _manifest_document()["entries"][0][duplicate_field]
    duplicate = json.dumps({duplicate_field: field_value})[1:-1] + ","
    ambiguous_manifest = (
        raw_manifest[:field_offset]
        + duplicate
        + raw_manifest[field_offset:]
    )
    manifest_path = tmp_path / "provider-directory-manifest.json"
    manifest_path.write_text(ambiguous_manifest, encoding="utf-8")

    with pytest.raises(RuntimeError, match="document_unreadable"):
        manual_catalog.reviewed_manual_census_source_id(
            manifest_path=manifest_path
        )


def test_manual_source_id_must_have_one_manifest_owner(tmp_path):
    """Reject a reviewed source ID repeated by any other catalog entry."""

    manifest_by_field = deepcopy(_manifest_document())
    source_id = _manual_entry(manifest_by_field)["source_ids"][0]
    manifest_by_field["entries"].append(
        {
            "classification": "public_metadata",
            "source_ids": [source_id],
        }
    )
    manifest_path = _write_manifest(tmp_path, manifest_by_field)

    with pytest.raises(RuntimeError, match="source_resolution_ambiguous"):
        manual_catalog.reviewed_manual_census_source_id(
            manifest_path=manifest_path
        )
    with pytest.raises(RuntimeError, match="source_resolution_ambiguous"):
        manual_catalog.reviewed_manual_census_seed_rows(
            source_id,
            manifest_path=manifest_path,
        )


@pytest.mark.parametrize(
    ("malformed_entry", "error_marker"),
    (
        (None, "entry_shape"),
        (
            {"classification": "public_metadata", "source_ids": "invalid"},
            "source_ids_invalid",
        ),
    ),
)
def test_selector_free_source_resolution_rejects_other_entry_shape(
    tmp_path,
    malformed_entry,
    error_marker,
):
    manifest_by_field = deepcopy(_manifest_document())
    manifest_by_field["entries"].append(malformed_entry)

    with pytest.raises(RuntimeError, match=error_marker):
        manual_catalog.reviewed_manual_census_source_id(
            manifest_path=_write_manifest(tmp_path, manifest_by_field)
        )
