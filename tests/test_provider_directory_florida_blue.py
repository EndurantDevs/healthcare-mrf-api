# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the public directory contract separate from metadata and legacy aliases."""

import dataclasses
import importlib

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_profile as profile_artifact
from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _source_http_test_session,
)
from tests.test_provider_directory_profile_selection_attestation import _computed

directory = importlib.import_module("process.provider_directory_fhir")
BASE = directory.FLORIDA_BLUE_PROVIDER_DIRECTORY_BASE
SOURCE_ID = "pdfhir_b46e16afb8f8a34bf14a8cd1"
METADATA_URL = "https://apigw.bcbsfl.com/interop/interop-developer-portal/cms/iop/v1/pd/R4/metadata"


def source():
    return directory._source_row_from_seed(directory.source_selection.configured_seed_rows()[0])


def test_real_profile_selection_remains_bound_to_reviewed_source_registry(monkeypatch):
    baseline = _computed()
    source_ids = profile_artifact.configured_profile_source_ids()
    monkeypatch.setattr(
        profile_artifact, "configured_profile_source_ids", lambda: (*source_ids, "synthetic-new-source")
    )
    assert _computed().identity_payload["profile_input_digest"] != baseline.identity_payload["profile_input_digest"]


@pytest.mark.parametrize("document", ["{}", '["invalid"]'])
def test_configured_seed_document_refuses_non_row_data(monkeypatch, document):
    monkeypatch.setattr(directory.source_selection.Path, "read_text", lambda *_args, **_kwargs: document)
    with pytest.raises(ValueError, match="provider_directory_configured_sources_invalid"):
        directory.source_selection.configured_seed_rows()


def test_seed_catalog_scope_and_page_caps(monkeypatch):
    monkeypatch.setattr(directory, "_remote_supplemental_catalogs", lambda **_kwargs: ([], {}))
    seeds, metrics = directory._seed_rows_from_supplemental_catalogs(source_query="Florida Blue")
    assert len(seeds) == metrics["catalogs"]["configured_sources"]["rows"] == 1
    row = directory._source_row_from_seed(seeds[0])
    entry = next(item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == "florida-blue")
    assert row["source_id"] == SOURCE_ID and entry["source_ids"] == [SOURCE_ID]
    assert row["canonical_api_base"] == entry["canonical_base"] == BASE
    assert entry["runnable"] and entry["profile_enabled"]
    assert row["auth_type"] == "none" and not row["requires_registration"]
    assert set(entry["resources"]) == set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
    assert directory._source_resource_fetch_order(row, entry["resources"])[:4] == [
        "PractitionerRole",
        "HealthcareService",
        "Practitioner",
        "Location",
    ]
    assert directory._source_resource_fetch_order(row, ["Organization", "PractitionerRole"]) == [
        "PractitionerRole",
        "Organization",
    ]
    for resource in entry["resources"]:
        assert directory._resource_start_url(row, resource, page_count=1000) == f"{BASE}/{resource}?_count=100"
    assert directory._resource_start_url(row, "Endpoint", page_count=100) is None
    assert "OrganizationAffiliation" not in row["metadata_json"]["provider_directory_expected_nonempty_resources"]
    assert directory._source_request_interval_seconds(row) == 0.1
    assert not directory._has_source_declared_credentialed_access(row)


@pytest.mark.parametrize("confirmed", [True, False])
def test_separate_metadata_preserves_only_explicitly_confirmed_acquisition_base(confirmed):
    row = source()
    if not confirmed:
        row["metadata_json"].pop("provider_directory_confirmed_base")
    expected = BASE if confirmed else METADATA_URL.rsplit("/", 1)[0]
    assert directory._candidate_metadata_urls(row)[0] == (expected, METADATA_URL)


def test_metadata_only_discovery_keeps_inferred_base():
    row_by_field = {"metadata_json": {"metadata_url": "https://directory.example/fhir/metadata"}}
    assert directory._candidate_metadata_urls(row_by_field)[0] == (
        "https://directory.example/fhir",
        "https://directory.example/fhir/metadata",
    )


@pytest.mark.asyncio
async def test_shared_metadata_probe_reads_directory_collection_only(monkeypatch):
    requests = []

    async def fetch(_source, url, **_kwargs):
        requests.append(url)
        if url == METADATA_URL:
            return (
                200,
                {
                    "resourceType": "CapabilityStatement",
                    "fhirVersion": "4.0.1",
                    "rest": [{"resource": [{"type": "Patient"}, {"type": "Practitioner"}]}],
                },
                None,
                1,
            )
        assert url == f"{BASE}/Practitioner?_count=1"
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    probe, _payload = await directory._probe_source(source(), timeout=5, run_id=None)
    assert probe["status"] == "valid" and probe["api_base"] == BASE
    assert requests == [METADATA_URL, f"{BASE}/Practitioner?_count=1"]


def test_opaque_cursor_bytes_and_durable_retry_identity():
    row = source()
    url = f"{BASE}/Practitioner?_count=100"
    following = f"{BASE}/?_getpages=synthetic%2Bcursor%3D&_getpagesoffset=100&_count=100&_bundletype=searchset"
    assert directory._resolved_fhir_next_url(row, url, following) == following
    assert directory._source_fetch_candidate_urls(following) == [following]
    with pytest.raises(ValueError, match="untrusted_pagination_link"):
        directory._resolved_fhir_next_url(row, url, "https://unrelated.example/fhir/?_getpages=synthetic")
    root = directory._pagination_checkpoint_context(row, [SOURCE_ID], run_id="example-root", retry_of_run_id=None)
    retry = directory._pagination_checkpoint_context(
        row, [SOURCE_ID], run_id="example-retry", retry_of_run_id="example-root", pagination_root_run_id="example-root"
    )
    assert root.canonical_api_base == retry.canonical_api_base == BASE
    assert root.source_scope_hash == retry.source_scope_hash
    assert retry.acquisition_root_run_id == root.owner_run_id == "example-root"


@pytest.mark.parametrize("checkpointing,expected", [(True, 7), (False, 1)])
def test_shared_scheduler_caps_eight_requested_chains_at_seven_resources(monkeypatch, checkpointing, expected):
    row = _parallel_resource_source()
    row.update(source())
    row["_partition_checkpoint_source_ids"] = (SOURCE_ID,)
    row["_pagination_checkpoint_context"] = dataclasses.replace(
        row["_pagination_checkpoint_context"], canonical_api_base=BASE, source_ids=(SOURCE_ID,)
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(
        resource_types=directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES,
        requested_concurrency=8,
        checkpointing_enabled=checkpointing,
    )
    with _source_http_test_session():
        assert directory._effective_resource_scan_concurrency(row, options) == expected
