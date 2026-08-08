# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for the reviewed manual Provider Directory catalog."""

from __future__ import annotations

import copy
import json

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog


MANIFEST_PATH = manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _manual_entry() -> dict:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    entries = [
        entry
        for entry in manifest["entries"]
        if entry.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    ]
    assert len(entries) == 1
    return entries[0]


@pytest.mark.parametrize(
    ("document_text", "expected_error"),
    (
        ("{", "document_unreadable"),
        ("[]", "document_shape"),
    ),
)
def test_manual_manifest_rejects_invalid_document_boundaries(
    tmp_path,
    document_text,
    expected_error,
):
    manifest_path = tmp_path / "provider-directory-manifest.json"
    manifest_path.write_text(document_text, encoding="utf-8")

    with pytest.raises(RuntimeError, match=expected_error):
        manual_catalog.reviewed_manual_census_seed_rows(
            "synthetic-source",
            manifest_path=manifest_path,
        )


@pytest.mark.parametrize(
    ("helper", "invalid_value", "field_name"),
    (
        (manual_catalog._strict_text, "", "display_name"),
        (manual_catalog._strict_slug, "invalid slug", "entry_id"),
    ),
)
def test_manual_catalog_rejects_invalid_text_boundaries(
    helper,
    invalid_value,
    field_name,
):
    with pytest.raises(RuntimeError, match=f"{field_name}_invalid"):
        helper(invalid_value, field_name=field_name)


@pytest.mark.parametrize(
    ("raw_entry", "expected_error"),
    (
        (None, "entry_shape"),
        (
            {
                "classification": manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION,
                "source_ids": "synthetic-source",
            },
            "source_ids_invalid",
        ),
    ),
)
def test_manual_source_resolution_rejects_malformed_entries(
    raw_entry,
    expected_error,
):
    with pytest.raises(RuntimeError, match=expected_error):
        manual_catalog._manual_entry_for_source(
            {"entries": [raw_entry]},
            "synthetic-source",
        )


@pytest.mark.parametrize(
    "canonical_base",
    ("not-a-url", "https://directory.example.org/fhir/"),
)
def test_manual_catalog_rejects_invalid_base_boundaries(canonical_base):
    with pytest.raises(RuntimeError, match="canonical_base_invalid"):
        manual_catalog._validated_base(canonical_base)


def test_manual_catalog_rejects_non_list_resource_profile():
    with pytest.raises(RuntimeError, match="resources_invalid"):
        manual_catalog._strict_resources(
            ("Organization",),
            field_name="resources",
        )


def test_manual_catalog_rejects_noncanonical_equivalent_start_url():
    canonical_base = "https://directory.example.org/fhir"

    with pytest.raises(RuntimeError, match="start_urls_invalid"):
        manual_catalog._validated_start_urls(
            {
                "Organization": (
                    "https://directory.example.org:443/fhir/Organization"
                )
            },
            canonical_base=canonical_base,
            resources=("Organization",),
        )


def test_manual_config_rejects_non_mapping_contract():
    entry = copy.deepcopy(_manual_entry())
    entry[manual_catalog.MANUAL_CURRENT_VERSION_CENSUS_FIELD] = []

    with pytest.raises(RuntimeError, match="manual_contract_shape"):
        manual_catalog._validated_manual_config(
            entry,
            canonical_base=entry["canonical_base"],
            resources=tuple(entry["resources"]),
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_error"),
    (
        (
            "provider_directory_census_cutoff",
            CUTOFF,
            "persisted_cutoff_forbidden",
        ),
        ("unexpected_field", True, "manual_contract_fields"),
        ("contract_version", 1, "contract_version_invalid"),
        (
            "continuation_strategy",
            "synthetic-unsupported",
            "continuation_strategy_invalid",
        ),
    ),
)
def test_manual_config_rejects_invalid_contract_fields(
    field_name,
    invalid_value,
    expected_error,
):
    entry = copy.deepcopy(_manual_entry())
    entry[manual_catalog.MANUAL_CURRENT_VERSION_CENSUS_FIELD][field_name] = (
        invalid_value
    )

    with pytest.raises(RuntimeError, match=expected_error):
        manual_catalog._validated_manual_config(
            entry,
            canonical_base=entry["canonical_base"],
            resources=tuple(entry["resources"]),
        )


def test_manual_entry_rejects_unexpected_field():
    entry = copy.deepcopy(_manual_entry())
    entry["unexpected_field"] = True

    with pytest.raises(RuntimeError, match="entry_fields"):
        manual_catalog._validated_manual_entry(
            entry,
            entry["source_ids"][0],
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_error"),
    (
        ("source_ids", [], "source_ids_invalid"),
        ("launch_mode", "scheduled", "launch_mode_invalid"),
        ("resource_profile", "B7", "resource_profile_invalid"),
    ),
)
def test_manual_entry_rejects_invalid_admission_fields(
    field_name,
    invalid_value,
    expected_error,
):
    entry = copy.deepcopy(_manual_entry())
    requested_source_id = entry["source_ids"][0]
    entry[field_name] = invalid_value

    with pytest.raises(RuntimeError, match=expected_error):
        manual_catalog._validated_manual_entry(entry, requested_source_id)
