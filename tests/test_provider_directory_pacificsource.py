# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public directory metadata uses the shared acquisition and checkpoint contracts."""

import dataclasses

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _source_http_test_session,
    importer,
)

BASE = "https://api.apim.pacificsource.com/fhir/provider/R4"
SOURCE_ID = "pdfhir_19526dc970938f7a212516fa"


def test_pacificsource_seed_matches_catalog_without_product_claims(monkeypatch):
    monkeypatch.setattr(importer, "_remote_supplemental_catalogs", lambda **_kwargs: ([], {}))
    seed_rows, metrics = importer._seed_rows_from_supplemental_catalogs(source_query="PacificSource")
    assert len(seed_rows) == 1
    source_by_field = importer._source_row_from_seed(seed_rows[0])
    entry = next(
        catalog_entry
        for catalog_entry in provider_directory_source_catalog()["items"]
        if catalog_entry["entry_id"] == "pacificsource"
    )
    assert source_by_field["source_id"] == SOURCE_ID
    assert source_by_field["canonical_api_base"] == entry["canonical_base"] == BASE
    assert entry["source_ids"] == [SOURCE_ID]
    assert entry["runnable"] and entry["profile_enabled"]
    assert entry["resources"] == list(importer.DEFAULT_RESOURCES)
    assert metrics["catalogs"]["pacificsource"]["rows"] == 1
    assert source_by_field["plan_name"] is None
    assert all(
        source_by_field[field] is None for field in ("is_medicare_advantage", "is_medicaid_mco", "is_chip", "is_qhp")
    )
    assert source_by_field["auth_type"] == "none"
    assert not source_by_field["requires_registration"] and not source_by_field["requires_api_key"]
    metadata = source_by_field["metadata_json"]
    assert metadata["provider_directory_plan_provenance_neutralized"] is True
    assert metadata["provider_directory_expected_nonempty_resources"] == [
        resource for resource in importer.DEFAULT_RESOURCES if resource != "Endpoint"
    ]
    for resource in importer.DEFAULT_RESOURCES:
        cap = {"PractitionerRole": 10, "OrganizationAffiliation": 50}.get(resource, 100)
        assert importer._source_resource_timeout(source_by_field, resource, 15) == 90
        assert (
            importer._resource_start_url(source_by_field, resource, page_count=1000)
            == f"{BASE}/{resource}?_format=json&_count={cap}"
        )
    assert importer._resource_start_url(source_by_field, "MedicationKnowledge", page_count=100) is None
    assert not importer._is_seed_row_query_match(seed_rows[0], "unrelated payer")


@pytest.mark.parametrize(
    ("org_name", "plan_name"),
    [
        ("PacificSource Medicare", "Medicare Advantage"),
        ("PacificSource Health Plans", "Medicare Advantage, Medicaid MCO"),
    ],
)
def test_pacificsource_does_not_rewrite_other_endpoint_contracts(org_name, plan_name):
    seed_by_field = {
        "org_name": org_name,
        "plan_name": plan_name,
        "api_base": "https://directory.example/pacificsource/r4",
        "source": "synthetic-catalog",
    }
    source = importer._source_row_from_seed(seed_by_field)
    assert source["source_id"] == importer._stable_source_id(seed_by_field) != SOURCE_ID
    assert source["api_base"] == seed_by_field["api_base"]
    assert source["org_name"] == org_name and source["plan_name"] == plan_name


def test_pacificsource_uses_the_existing_durable_checkpoint_scope():
    source = importer._source_row_from_seed(importer.pacificsource.seed_rows()[0])
    context = importer._pagination_checkpoint_context(source, [SOURCE_ID], run_id="run-first", retry_of_run_id=None)
    assert context is not None
    assert context.canonical_api_base == BASE and context.source_ids == (SOURCE_ID,)
    assert context.owner_run_id == context.acquisition_root_run_id == "run-first"
    assert importer._pagination_checkpoint_scope_identity(source, [SOURCE_ID]) == (BASE, context.source_scope_hash)


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, 8),
        ({"source_concurrency": 2}, 1),
        ({"checkpointing_enabled": False}, 1),
        ({"deferred_materialization": False}, 1),
    ],
)
def test_pacificsource_eight_cursor_request_retains_shared_scan_gates(monkeypatch, overrides, expected):
    source = _parallel_resource_source()
    source.update(importer._source_row_from_seed(importer.pacificsource.seed_rows()[0]))
    source["_partition_checkpoint_source_ids"] = (SOURCE_ID,)
    source["_pagination_checkpoint_context"] = dataclasses.replace(
        source["_pagination_checkpoint_context"], canonical_api_base=BASE, source_ids=(SOURCE_ID,)
    )
    _patch_resource_scan_storage(monkeypatch, True)
    options = _resource_concurrency_options(
        resource_types=tuple(importer.DEFAULT_RESOURCES), requested_concurrency=8, **overrides
    )
    with _source_http_test_session():
        assert importer._effective_resource_scan_concurrency(source, options) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_first_scan_uses_reviewed_estimates_then_published_counts(monkeypatch, published):
    from unittest.mock import AsyncMock

    from process import provider_directory_generic_source_adapters as adapters

    source_row_map = importer._source_row_from_seed(importer.pacificsource.seed_rows()[0])
    seed_counts = source_row_map["metadata_json"]["provider_directory_resource_count_estimates"]
    seed_counts.update({"MedicationKnowledge": 9999999, "invalid": -1})
    if published:
        source_row_map["_endpoint_id"] = "endpoint-example"
    monkeypatch.setattr(
        adapters,
        "current_endpoint_dataset_record",
        AsyncMock(return_value={"status": importer.ENDPOINT_DATASET_PUBLISHED, "resource_counts": {"Location": 5}}),
    )
    counts = await adapters.resource_scan_estimates(importer, source_row_map)
    if published:
        assert counts == {"Location": 5}
    else:
        assert sorted(counts, key=counts.get, reverse=True) == [
            "Practitioner",
            "Location",
            "HealthcareService",
            "PractitionerRole",
            "Organization",
            "InsurancePlan",
            "OrganizationAffiliation",
            "Endpoint",
        ]
        adapters.current_endpoint_dataset_record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_path", ["", "/Practitioner"])
async def test_direct_http_keeps_opaque_cursor_bytes_and_refuses_redirects(monkeypatch, resource_path):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from yarl import URL

    requests = []
    request_url = f"https://directory.example/fhir{resource_path}?_getpages=opaque%2Bpart+%2F%3D&_count=50"
    bundle_by_field = {"resourceType": "Bundle", "type": "searchset", "entry": []}

    @asynccontextmanager
    async def get(url, **options):
        requests.append((str(URL(url)), options))
        yield SimpleNamespace(status=200)

    monkeypatch.setattr(importer, "_read_source_http_payload", AsyncMock(return_value=bundle_by_field))
    response = await importer._fetch_json_with_source_session(SimpleNamespace(get=get), request_url, timeout=2)
    assert response[:3] == (200, bundle_by_field, None)
    assert requests[0][0] == request_url
    assert requests[0][1]["allow_redirects"] is False
