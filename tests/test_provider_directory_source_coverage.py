# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Truncated Michigan search results cannot authorize full acquisition."""

import importlib
import json

import pytest

from api.provider_directory_sources import DEFAULT_MANIFEST, provider_directory_source_catalog
from process.provider_directory_publication_catalog_authority import canonical_manifest_digest
from process.provider_directory_source_coverage import (
    INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
    MICHIGAN_COVERAGE_WARNING,
    MICHIGAN_PROVIDER_DIRECTORY_BASE,
    MICHIGAN_SOURCE_ID,
)

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize("api_base", [
    INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE,
    MICHIGAN_PROVIDER_DIRECTORY_BASE,
    MICHIGAN_PROVIDER_DIRECTORY_BASE + "/",
])
@pytest.mark.parametrize("resource_type", sorted(importer.MICHIGAN_SUPPORTED_RESOURCES))
def test_stale_full_metadata_cannot_admit_michigan_acquisition(api_base, resource_type):
    source_record_by_field = {
        "source_id": "synthetic-alias",
        "api_base": api_base,
        "metadata_json": {
            "provider_directory_coverage_mode": "full",
            "provider_directory_acquisition_enabled": True,
            "provider_directory_fully_enumerable_resources": [resource_type],
        },
    }
    with pytest.raises(RuntimeError, match="upstream_search_window_incomplete"):
        importer._assert_resource_acquisition_allowed(source_record_by_field, [resource_type])


def test_michigan_is_skipped_without_aborting_healthy_sources():
    healthy_source_by_field = {
        "source_id": "synthetic-healthy",
        "api_base": "https://directory.example.test/fhir",
    }
    selected_sources, selection_metrics = importer._select_resource_import_sources(
        [healthy_source_by_field, {"source_id": MICHIGAN_SOURCE_ID,
                                 "api_base": MICHIGAN_PROVIDER_DIRECTORY_BASE}],
        valid_source_ids=None, open_only=True, include_auth_required=False,
    )
    assert selected_sources == [healthy_source_by_field]
    assert selection_metrics["source_import_skipped_blocked_source"] == 1
    assert selection_metrics[
        "source_import_skipped_blocked_source_upstream_search_window_incomplete"
    ] == 1


def _dataset_row(*, promote):
    return {
        "source_id": MICHIGAN_SOURCE_ID,
        "endpoint_id": "synthetic-endpoint",
        "source_record_json": {
            "source_id": MICHIGAN_SOURCE_ID,
            "api_base": MICHIGAN_PROVIDER_DIRECTORY_BASE,
            "metadata_json": {"provider_directory_coverage_mode": "full"},
        },
        "dataset_id": "synthetic-dataset",
        "evidence_run_id": "synthetic-root",
        "selected_resources": ["Organization"],
        "status": "validated" if promote else "published",
        "is_current": not promote,
        "current_dataset_count": 0 if promote else 1,
        "promote_on_cutover": promote,
        "dataset_hash": "a" * 64,
        "resource_count": 1,
        "validated_at": "2026-09-06T00:00:00+00:00",
        "publication_metadata_json": {"selected_resources": ["Organization"]},
    }


def test_michigan_new_publication_is_blocked_but_current_slice_is_retained():
    with pytest.raises(RuntimeError, match="artifact_coverage_blocked"):
        importer._validate_provider_directory_artifact_datasets(
            [_dataset_row(promote=True)], [MICHIGAN_SOURCE_ID],
            should_select_validated_candidates=True,
        )
    fence = importer._validate_provider_directory_artifact_datasets(
        [_dataset_row(promote=False)], [MICHIGAN_SOURCE_ID],
    )
    assert fence.datasets[0].dataset_id == "synthetic-dataset"
    assert fence.datasets[0].is_current is True
    assert fence.datasets[0].promote_on_cutover is False


@pytest.mark.parametrize(("api_base", "canonical_base"), [
    ("https://directory.example.test/fhir", MICHIGAN_PROVIDER_DIRECTORY_BASE),
    ("https://directory.example.test/fhir", MICHIGAN_PROVIDER_DIRECTORY_BASE + "/"),
    (MICHIGAN_PROVIDER_DIRECTORY_BASE, "https://directory.example.test/fhir"),
])
def test_michigan_canonical_alias_cannot_bypass_acquisition_or_promotion(api_base, canonical_base):
    source_record_by_field = {
        "source_id": "synthetic-canonical-alias", "api_base": api_base,
        "canonical_api_base": canonical_base,
        "metadata_json": {"provider_directory_coverage_mode": "full"},
    }
    start_url = importer._resource_start_url(source_record_by_field, "Organization", page_count=1)
    assert start_url.startswith(canonical_base.rstrip("/") + "/Organization?")
    with pytest.raises(RuntimeError, match="upstream_search_window_incomplete"):
        importer._assert_resource_acquisition_allowed(source_record_by_field, ["Organization"])
    for promote in (False, True):
        row = _dataset_row(promote=promote)
        row.update(source_id=source_record_by_field["source_id"], source_record_json=source_record_by_field)
        if promote:
            with pytest.raises(RuntimeError, match="artifact_coverage_blocked"):
                importer._validate_provider_directory_artifact_datasets(
                    [row], [source_record_by_field["source_id"]], should_select_validated_candidates=True,
                )
        else:
            fence = importer._validate_provider_directory_artifact_datasets(
                [row], [source_record_by_field["source_id"]],
            )
            assert fence.datasets[0].is_current is True
            assert fence.datasets[0].promote_on_cutover is False


def test_catalog_warns_without_changing_held_profile_selection_authority():
    catalog = provider_directory_source_catalog()
    manifest = json.loads(DEFAULT_MANIFEST.read_text())
    assert catalog["catalog_digest"] == canonical_manifest_digest(manifest)
    for entry in catalog["items"]:
        if entry["source_ids"] == [MICHIGAN_SOURCE_ID]:
            assert entry["runnable"] is True
            assert entry["profile_enabled"] is True
            assert entry["coverage_warning"] == MICHIGAN_COVERAGE_WARNING
            assert entry["acquisition_blocked_reason"] == "upstream_search_window_incomplete"
        else:
            assert "coverage_warning" not in entry
            assert "acquisition_blocked_reason" not in entry
