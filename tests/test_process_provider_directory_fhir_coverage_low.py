# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_fhir_coverage_low_support import (
    AsyncContext,
    GuardConnection,
    GuardEngine,
    assert_address_publication_edges,
    assert_artifact_summary_edges,
    assert_network_publication_edges,
    assert_seed_fetch_edges,
    artifact_dataset,
    bulk_identity,
    run_nonvalid_probe_batch,
)

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_reverse_lookup_control_edges(monkeypatch):
    options = importer.ScanPractitionerRoleFetchOptions(0, 0, 10, 1, None)
    request = importer._PractitionerRoleReverseLookupRequest(
        {"api_base": "https://example.test/fhir"}, {}, options, 1, None
    )
    buffer = importer._StreamedResourceRowBuffer(object, None, 1, [])
    state = importer._PractitionerRoleReverseLookupState(object, [], buffer, True, 0)

    async def seeds(*_args):
        yield "practitioner", "Practitioner", "123"

    monkeypatch.setattr(importer, "_iter_scan_practitioner_role_seed_rows", seeds)
    queue = asyncio.Queue()
    await state.produce_seed_rows(request, queue, 1)
    assert state.seed_count == 1

    cancel_options = importer.ScanPractitionerRoleFetchOptions(
        0, 0, 10, 1, None, cancel_ctx={"run_id": "run-1"}
    )
    cancel_request = importer._PractitionerRoleReverseLookupRequest(
        request.source, {}, cancel_options, 1, None
    )
    monkeypatch.setattr(importer, "_raise_if_resource_import_cancelled", AsyncMock())
    monkeypatch.setattr(importer, "_scan_practitioner_role_reverse_lookup_url", Mock(return_value="next"))
    monkeypatch.setattr(state, "has_reached_page_limit", AsyncMock(return_value=True))
    await state.fetch_seed_pages(cancel_request, ("practitioner", "Practitioner", "123"))
    importer._raise_if_resource_import_cancelled.assert_awaited_once()


def test_small_normalization_edges(monkeypatch):
    assert importer._minimal_resource_id_rows(
        [{"resource_id": " 123 "}, {"resource_id": None}]
    ) == [{"resource_id": "123"}]
    assert importer._amerihealth_caritas_plan_code(
        {"org_name": "AmeriHealth Caritas Pennsylvania"}
    ) == importer.AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS[
        importer._payer_alias_key("AmeriHealth Caritas Pennsylvania")
    ]
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT", "true")
    assert importer._is_bulk_export_enabled(None) is True
    assert importer._is_bulk_export_enabled(False) is False
    assert importer._source_fetch_candidate_urls(
        "https://example.test/fhir/Practitioner?_count=invalid"
    ) == ["https://example.test/fhir/Practitioner?_count=invalid"]
    assert importer._is_safe_bulk_https_url("https://example.test:bad/fhir") is False


def test_alohr_false_and_loop_edges(monkeypatch):
    assert importer._alohr_telecom("first@example.test", " ", "555-0100") == [
        {"system": "email", "value": "first@example.test"},
        {"system": "phone", "value": "555-0100"},
    ]
    monkeypatch.setattr(importer, "parse_fhir_resource", Mock(return_value=None))
    rows_by_model = {}
    importer._append_alohr_parsed_resource(
        rows_by_model,
        "source-1",
        {"resourceType": "Practitioner", "id": "practitioner-1"},
        run_id=None,
    )
    assert rows_by_model == {}
    monkeypatch.setattr(importer, "_append_alohr_parsed_resource", Mock())
    importer._append_alohr_organization_rows(
        rows_by_model,
        "source-1",
        {
            "orgId": "org-1",
            "contacts": [{"contacts": []}, "skip", {"contacts": []}],
        },
        run_id=None,
    )
    assert importer._append_alohr_parsed_resource.call_count == 1


def test_oauth_transport_failure_is_local(monkeypatch):
    monkeypatch.setattr(
        importer.urllib.request,
        "urlopen",
        Mock(side_effect=RuntimeError("offline")),
    )
    token = importer._fetch_oauth2_client_credentials_token_sync(
        {
            "token_url": "https://auth.example.test/token",
            "client_id": "client",
            "client_secret": "secret",
        }
    )
    assert token is None


def test_credential_options_tolerate_empty_oauth_token(monkeypatch):
    credential_spec_by_field = {
        "oauth2": {"token_url": "https://auth.example.test/token"}
    }
    monkeypatch.setattr(
        importer,
        "_credential_spec_for_source",
        Mock(return_value=credential_spec_by_field),
    )
    monkeypatch.setattr(importer, "_is_credential_allowed_for_url", Mock(return_value=True))
    monkeypatch.setattr(importer, "_fetch_oauth2_client_credentials_token_sync", Mock(return_value=None))
    options_by_field = importer._credential_request_options_for_source(
        {"source_id": "source-1"}, "https://example.test/fhir"
    )
    assert "Authorization" not in options_by_field["headers"]


@pytest.mark.asyncio
async def test_address_publication_defer_and_cleanup(monkeypatch):
    await assert_address_publication_edges(monkeypatch)


@pytest.mark.asyncio
async def test_coordinate_empty_scope_and_backfill(monkeypatch):
    clauses = importer._provider_directory_location_coordinate_scopes(
        "loc", db_schema="public", run_id=None, source_ids=None, seen_table=None
    )
    assert all("source_id = ANY" not in clause for clause in clauses)
    monkeypatch.setattr(importer, "_is_table_present", AsyncMock(return_value=True))
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value={"candidate_rows": 0}))
    assert await importer.backfill_provider_directory_location_coordinates(
        "public", run_id=None, source_ids=None
    ) == 0
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value="public.addr_key_v1"))
    assert await importer._has_address_canon_functions("public") is True


def test_locked_fence_groups_multiple_rows(monkeypatch):
    dataset = artifact_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    locked_rows = [
        {"dataset_id": "dataset-1", "endpoint_id": "endpoint-1"},
        {"dataset_id": "dataset-2", "endpoint_id": "endpoint-1"},
        {"dataset_id": "dataset-3", "endpoint_id": None},
    ]
    assertion = Mock()
    monkeypatch.setattr(importer, "_assert_locked_artifact_endpoint_state", assertion)
    monkeypatch.setattr(importer, "_is_artifact_fence_dataset_row_exact", Mock(return_value=True))
    importer._assert_locked_artifact_fence_datasets(fence, locked_rows)
    assert len(assertion.call_args.args[1]) == 2


@pytest.mark.asyncio
async def test_artifact_summary_invalid_and_missing(monkeypatch):
    await assert_artifact_summary_edges(monkeypatch)


def test_relation_aggregation_without_network():
    affiliation_key = importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY
    proof = importer._aggregate_dataset_serving_relation_proofs(
        {affiliation_key: []}, build_run_id=None
    )
    assert importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY not in proof
    assert affiliation_key in proof


@pytest.mark.asyncio
async def test_unpromoted_stages_are_not_removed(monkeypatch):
    stages = (SimpleNamespace(resume_checkpoint=None), SimpleNamespace(resume_checkpoint=None))
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        AsyncMock(side_effect=RuntimeError("promotion failed")),
    )
    remove = AsyncMock()
    monkeypatch.setattr(importer, "_remove_provider_directory_artifact_stage", remove)
    with pytest.raises(RuntimeError, match="promotion failed"):
        await importer._finalize_provider_directory_profile_stages(
            {}, stages, defer_cutover=False
        )
    remove.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_npi_empty_scope_and_missing_copy(monkeypatch):
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="UPDATE 1"))
    assert await importer.backfill_provider_directory_resource_id_npis(
        "public", run_id=None, seen_table=None
    ) == {"Practitioner": 1, "Organization": 1}

    raw_connection = SimpleNamespace()
    connection = SimpleNamespace(raw_connection=raw_connection)
    monkeypatch.setattr(importer.db, "acquire", Mock(return_value=AsyncContext(connection)))
    with pytest.raises(NotImplementedError, match="copy_records_to_table"):
        await importer._copy_mark_resource_rows_seen(
            "Practitioner", [("source-1", "resource-1")], "run-1", seen_table="seen"
        )


@pytest.mark.asyncio
async def test_network_deferred_publication_without_scope(monkeypatch):
    await assert_network_publication_edges(monkeypatch)


@pytest.mark.asyncio
async def test_network_catalog_small_edges(monkeypatch):
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=1))
    assert await importer._has_provider_directory_network_catalog_rows("public") is True
    monkeypatch.setattr(importer, "PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES", ("same",))
    monkeypatch.setattr(importer, "_network_catalog_index_name", Mock(return_value="same_index"))
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    await importer._rename_network_catalog_stage_indexes("public", "stage")
    status.assert_not_awaited()
    assert importer._provider_directory_network_catalog_scope_filter(
        "row", run_id="run-1", source_ids=None
    ).count("AND") == 1
    assert importer._location_archive_stage_table_name("stable").startswith(
        importer.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_STAGE_PREFIX
    )


def test_supplemental_catalog_failures_are_reported(monkeypatch):
    failure = Mock(side_effect=RuntimeError("offline fixture"))
    for name in (
        "_seed_rows_from_amerihealth_caritas_catalog",
        "_seed_rows_from_contra_costa_catalog",
        "_cms_sma_seed_rows",
    ):
        monkeypatch.setattr(importer, name, failure)
    for name in (
        "_reviewed_provider_directory_candidate_seed_rows",
        "_health_partners_plans_seed_rows",
        "_aetna_provider_directory_data_seed_rows",
        "_provider_directory_blocker_seed_rows",
    ):
        monkeypatch.setattr(importer, name, Mock(return_value=[]))
    rows, metrics = importer._seed_rows_from_supplemental_catalogs()
    assert rows == []
    assert all(
        "offline fixture" in metrics["catalogs"][name]["error"]
        for name in ("amerihealth_caritas", "contra_costa", "cms_sma_endpoint_directory")
    )


def test_catalog_filters_skip_unmatched_public_rows():
    cms_csv = """Provider Directory Endpoint Information,,,,
State,Provider Directory Production Base URL,Status - Drop Down List,Is the API public? (Y/N) - Drop Down List,FHIR Capability Statement link
Iowa,https://fixture.test/fhir/Practitioner,Active,Yes,
"""
    assert importer._cms_sma_endpoint_directory_seed_rows_from_csv(
        cms_csv, source_query="not-iowa", source_url="fixture.csv"
    ) == []
    amerigroup_html = """
    <table><tr>
      <td>5400</td><td>AmeriHealth Caritas District of Columbia</td>
      <td><a href="https://api-ext.amerihealthcaritas.com/5400/provider-api/swagger-ui/">API</a></td>
    </tr></table>
    """
    assert importer._amerihealth_caritas_seed_rows_from_catalog_html(
        amerigroup_html, source_query="not-amerihealth", source_url="fixture.html"
    ) == []


def test_seed_resolution_and_sync_fetch_edges(monkeypatch):
    assert_seed_fetch_edges(monkeypatch)


@pytest.mark.asyncio
async def test_async_fetch_wrappers(monkeypatch):
    expected = (200, {"resourceType": "Bundle"}, None, 1)
    to_thread = AsyncMock(return_value=expected)
    monkeypatch.setattr(importer.asyncio, "to_thread", to_thread)
    assert await importer._fetch_json("https://fixture.test/fhir", timeout=1) == expected
    assert await importer._fetch_json_with_options(
        "https://fixture.test/fhir", timeout=1, extra_headers={"X-Test": "1"}
    ) == expected
    assert to_thread.await_count == 2


def test_molina_and_quota_edges():
    current_url = f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=10"
    same_origin = f"{importer.MOLINA_PROVIDER_DIRECTORY_BASE}/Practitioner?cursorMark=next"
    assert importer._resolved_molina_next_url(current_url, same_origin) == same_origin
    invalid_duration_by_field = {
        "message": "Out of call volume quota. Quota will be replenished in 01:99:99."
    }
    assert importer._molina_quota_retry_not_before(
        {"api_base": importer.MOLINA_PROVIDER_DIRECTORY_BASE},
        403,
        invalid_duration_by_field,
        now_utc=datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC),
    ) is None


@pytest.mark.asyncio
async def test_public_resolver_edges(monkeypatch):
    resolver = importer._PublicBulkResolver()
    monkeypatch.setattr(resolver._resolver, "resolve", AsyncMock(return_value=[]))
    assert await resolver.resolve("example.test") == []
    resolver._resolver.resolve.return_value = [{"host": "8.8.8.8"}]
    assert await resolver.resolve("example.test") == [{"host": "8.8.8.8"}]
    resolver._resolver.resolve.return_value = [{"host": "not-an-ip"}]
    with pytest.raises(OSError, match="non_public_dns"):
        await resolver.resolve("example.test")


@pytest.mark.asyncio
async def test_guard_owner_and_lock_failures(monkeypatch):
    identity = bulk_identity()
    owner_connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=RuntimeError("connection lost"))
    )
    with pytest.raises(RuntimeError, match="worker_guard_lost"):
        await importer._assert_bulk_checkpoint_guard_owner(owner_connection, identity)

    lock_connection = GuardConnection(False)
    engine = GuardEngine(lock_connection)

    async def connect():
        importer.db.engine = engine

    monkeypatch.setattr(importer.db, "engine", None)
    monkeypatch.setattr(importer.db, "connect", connect)
    with pytest.raises(RuntimeError, match="worker_active"):
        async with importer._bulk_checkpoint_worker_guard(identity):
            pytest.fail("lock must fail before yielding")


@pytest.mark.asyncio
@pytest.mark.parametrize("unlock_result", [None, RuntimeError("unlock failed")])
async def test_guard_unlock_is_best_effort(monkeypatch, unlock_result):
    connection = GuardConnection(True, unlock_result)
    if isinstance(unlock_result, Exception):
        connection.scalar = AsyncMock(side_effect=[True, unlock_result])
    monkeypatch.setattr(importer.db, "engine", GuardEngine(connection))
    async with importer._bulk_checkpoint_worker_guard(bulk_identity()) as ownership_probe:
        assert callable(ownership_probe)
    assert connection.scalar.await_count == 2


@pytest.mark.asyncio
async def test_probe_batch_nonvalid_capabilities(monkeypatch):
    observed = await run_nonvalid_probe_batch(monkeypatch)
    assert observed == (40, 0, set())
