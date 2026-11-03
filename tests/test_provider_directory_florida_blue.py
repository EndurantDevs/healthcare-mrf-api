# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the public directory contract separate from metadata and legacy aliases."""

import dataclasses
import importlib
from types import SimpleNamespace

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from process import provider_directory_generic_source_policy as source_policy
from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _source_http_test_session,
)

directory = importlib.import_module("process.provider_directory_fhir")
BASE = directory.FLORIDA_BLUE_PROVIDER_DIRECTORY_BASE
SOURCE_ID = "pdfhir_b46e16afb8f8a34bf14a8cd1"


def source():
    return directory._source_row_from_seed(directory.source_selection.configured_seed_rows()[0])


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
    assert not directory.capped_partition.enabled(directory, row)
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
    assert directory.source_selection.declared_probe_target(directory, row) == ("Practitioner", BASE)


@pytest.mark.asyncio
async def test_declared_collection_probe_keeps_the_confirmed_acquisition_base(monkeypatch):
    requests = []

    async def fetch(_source, url, **_kwargs):
        requests.append(url)
        assert url == f"{BASE}/Practitioner?_count=1"
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    probe, capability = await directory._probe_source(source(), timeout=5, run_id=None)
    assert probe["status"] == "valid" and probe["api_base"] == BASE
    assert capability is None and probe["resource_probe"]["search_bundle_valid"] is True
    assert requests == [f"{BASE}/Practitioner?_count=1"]


@pytest.mark.asyncio
async def test_stale_isolated_source_uses_declared_collection_without_catalog_writes(monkeypatch):
    source_by_field = {**source(), "last_validated_status": "valid_non_fhir"}
    task_map = {
        **directory.source_selection._ISOLATED_ACQUISITION_FLAGS,
        "source_ids": [SOURCE_ID],
        "resources": ["Practitioner"],
        "provider_directory_endpoint_scope": BASE,
    }
    requests = []

    async def fetch(_source, url, **_kwargs):
        directory.source_selection.require_qualification_url(directory, url)
        requests.append(url)
        assert url == f"{BASE}/Practitioner?_count=1"
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1

    async def refuse_write(*_args, **_kwargs):
        pytest.fail("qualification must not write catalog probes")

    monkeypatch.setattr(directory, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    monkeypatch.setattr(directory, "_upsert_rows", refuse_write)
    monkeypatch.setattr(directory, "_run_source_probe_batch", refuse_write)
    token = source_policy._FACTORY.set(SimpleNamespace(storage_binding=object()))
    try:
        qualified = await directory.source_selection.qualify_stale_isolated_source(
            directory, [source_by_field], task_map, timeout=3, run_id="synthetic-qualification"
        )
    finally:
        source_policy._FACTORY.reset(token)
    assert qualified == [{**source_by_field, "last_validated_status": "valid"}]
    assert source_by_field["last_validated_status"] == "valid_non_fhir"
    assert requests == [f"{BASE}/Practitioner?_count=1"]
    assert directory.source_selection.QUALIFICATION_API_BASE.get() is None


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
