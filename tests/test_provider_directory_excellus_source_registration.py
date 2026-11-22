# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the network snapshot on its exact source and resource selection."""

import copy
import importlib
import json
from pathlib import Path

import pytest

from scripts import generate_provider_directory_support_docs as generator

importer = importlib.import_module("process.provider_directory_fhir")


ROOT = Path(__file__).resolve().parents[1]
BASE = "https://fhir.excellusbcbs.com/fhir/api"
ENTRY_ID = "excellus-provider-directory"


def _source():
    seeds = json.loads((ROOT / "specs/provider_directory_supplemental_sources.json").read_text())
    matches = [seed for seed in seeds if seed["id"] == ENTRY_ID]
    assert len(matches) == 1
    return importer._source_row_from_seed(matches[0])


def _select(source, requested):
    return importer._select_resource_import_sources(
        [source],
        valid_source_ids={source["source_id"]},
        open_only=True,
        include_auth_required=False,
        requested_resource_types=requested,
    )


def test_excellus_registration_is_exact_scoped_acquisition(monkeypatch):
    source = _source()
    manifest = generator.load_manifest(generator.DEFAULT_MANIFEST)
    entry = next(entry for entry in manifest["entries"] if entry["entry_id"] == ENTRY_ID)
    assert source["canonical_api_base"] == entry["canonical_base"] == BASE
    assert entry["source_ids"] == [source["source_id"]]
    assert entry["classification"] == "acquisition"
    assert entry["resources"] == ["Organization", "OrganizationAffiliation"]
    assert entry["resource_profile"] == "A2"
    metadata = source["metadata_json"]
    assert metadata["provider_directory_acquisition_enabled"] is True
    assert metadata["provider_directory_fully_enumerable_resources"] == []
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    assert (
        importer._endpoint_dataset_selected_resources([source], list(importer.DEFAULT_RESOURCES)) == entry["resources"]
    )
    selected, metrics = _select(source, entry["resources"])
    assert selected == [source]
    assert metrics["source_import_sources_selected"] == 1
    verification = json.loads((ROOT / "specs/provider_directory_endpoint_verification.json").read_text())
    assert verification["entries"][ENTRY_ID]["proof_state"] == "not_recorded"


@pytest.mark.parametrize(
    "named,requested",
    [
        (False, None),
        (False, ["Organization", "OrganizationAffiliation"]),
        (True, None),
        (True, ["Organization"]),
        (True, ["Practitioner"]),
    ],
)
def test_broad_raw_and_wrong_resource_requests_are_blocked(monkeypatch, named, requested):
    source = _source()
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: named)
    selected, metrics = _select(source, requested)
    assert selected == []
    assert metrics["source_import_skipped_blocked_source"] == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_id", "other-source"),
        ("api_base", "https://example.org/fhir"),
        ("canonical_api_base", "https://example.org/fhir"),
        ("snapshot", {"name": "Other", "owner_reference": "Organization/owner"}),
        ("snapshot", None),
    ],
)
def test_altered_source_identity_is_blocked(monkeypatch, field, value):
    source = copy.deepcopy(_source())
    if field == "snapshot":
        source["metadata_json"]["provider_directory_network_snapshot"] = value
    else:
        source[field] = value
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    selected, metrics = _select(source, ["Organization", "OrganizationAffiliation"])
    assert selected == []
    assert metrics["source_import_skipped_blocked_source"] == 1


def test_snapshot_selection_is_bound_to_endpoint_and_acquisition_contracts():
    source = _source()
    changed = copy.deepcopy(source)
    changed["metadata_json"]["provider_directory_network_snapshot"]["name"] = "Other"
    assert importer._resource_endpoint_signature(source) != importer._resource_endpoint_signature(changed)
    for artifact in (False, True):
        assert "provider_directory_network_snapshot" in importer._source_contract_metadata_keys(
            source["metadata_json"],
            artifact=artifact,
        )


def test_normal_catalog_exposes_an_acquisition_only_network_snapshot(monkeypatch):
    from api.provider_directory_source_worker import ROUTES_ENV
    from api.provider_directory_sources import provider_directory_source_catalog

    monkeypatch.setenv(
        ROUTES_ENV,
        json.dumps(
            [
                {
                    "root_run_id": None,
                    "source_id": _source()["source_id"],
                    "endpoint_scope": BASE,
                    "image": "ghcr.io/example/worker@sha256:" + "a" * 64,
                    "secret_name": "source-runtime",
                }
            ]
        ),
    )
    item = next(item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == ENTRY_ID)
    assert item["runnable"] is True and item["profile_enabled"] is False
    assert item["supported_resources"] == item["resources"] == ["Organization", "OrganizationAffiliation"]
    assert item["coverage_warning"] == (
        "Networks discovered in the FHIR directory and their institutional affiliations. "
        "Commercial market coverage and individual clinician membership are unverified."
    )
