# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Minnesota remains probe-only until complete enumeration is available."""

import importlib

import pytest

from api.provider_directory_sources import provider_directory_source_catalog

directory = importlib.import_module("process.provider_directory_fhir")
BASE = "https://api.bluecrossmn.com/fhir/provider"
SOURCE_ID = "pdfhir_69fc4cc6c4ce23447dc03058"


@pytest.fixture
def carrier_source():
    seed = next(row for row in directory.source_selection.configured_seed_rows() if row["api_base"] == BASE)
    return directory._source_row_from_seed(seed)


def test_probe_only_catalog_preserves_carrier_identity_and_documented_scope(carrier_source):
    entry = next(row for row in provider_directory_source_catalog()["items"] if row["entry_id"] == "bcbs-minnesota")
    assert carrier_source["source_id"] == SOURCE_ID and entry["source_ids"] == [SOURCE_ID]
    assert carrier_source["canonical_api_base"] == entry["canonical_base"] == BASE
    assert carrier_source["org_name"] == "Blue Cross and Blue Shield of Minnesota"
    assert carrier_source["plan_name"] is None
    assert carrier_source["auth_type"] == "none" and not carrier_source["requires_registration"]
    assert not directory._has_source_declared_credentialed_access(carrier_source)
    assert not entry["runnable"] and not entry["profile_enabled"]
    assert entry["resources"] == [] and entry["resource_profile"] == "NONE"
    assert set(entry["supported_resources"]) == set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)


@pytest.mark.parametrize(
    "resources",
    [[resource] for resource in directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES]
    + [list(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)],
)
def test_truncated_search_cannot_start_resource_acquisition(carrier_source, resources):
    metadata = carrier_source["metadata_json"]
    assert metadata["provider_directory_acquisition_enabled"] is False
    assert metadata["provider_directory_acquisition_blocked_reason"] == "search_results_truncated_without_continuation"
    assert directory._source_fully_enumerable_resource_types(carrier_source) == set()
    with pytest.raises(RuntimeError, match="coverage_mode_probe_only"):
        directory._assert_resource_acquisition_allowed(carrier_source, resources)


def test_bounded_probe_limits_exclude_endpoint_and_do_not_enable_checkpoints(carrier_source):
    for resource in directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES:
        assert (
            directory._resource_start_url(carrier_source, resource, page_count=1000) == f"{BASE}/{resource}?_count=500"
        )
    assert directory._resource_start_url(carrier_source, "Endpoint", page_count=100) is None
    assert directory._source_request_interval_seconds(carrier_source) == 0.1
    assert directory.source_selection.declared_probe_target(directory, carrier_source) == ("Practitioner", BASE)
    assert (
        directory._pagination_checkpoint_context(
            carrier_source, [SOURCE_ID], run_id="example-probe", retry_of_run_id=None
        )
        is None
    )


@pytest.mark.asyncio
async def test_declared_collection_probe_keeps_the_confirmed_base(monkeypatch, carrier_source):
    requests = []

    async def fetch(_source, url, **_kwargs):
        requests.append(url)
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    probe, capability = await directory._probe_source(carrier_source, timeout=5, run_id=None)
    assert probe["status"] == "valid" and probe["api_base"] == BASE
    assert capability is None and probe["resource_probe"]["search_bundle_valid"] is True
    assert requests == [f"{BASE}/Practitioner?_count=1"]
