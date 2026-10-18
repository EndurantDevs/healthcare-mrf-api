# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed detail orchestration keeps one policy, source pin and real request."""

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic import Request, response
from sanic.compat import Header
from sanic.exceptions import NotFound, SanicException, ServiceUnavailable

from api import provider_detail_network_read as composer
from api.endpoint import npi as provider
from api.network_address_scope import current_network_address_scope
from api.provider_detail_network_contract import parse_managed_detail_query
from process import registry_network_catalog_read as catalog
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import RegistryNetworkCatalogError

NPI = "0000000049"
DIGEST = "a" * 64


def _install_catalog_mocks(monkeypatch, manifest, native_read):
    @asynccontextmanager
    async def catalog_snapshot(driver, generation, schema):
        yield await composer._pin(driver, generation, schema)

    monkeypatch.setattr(catalog, "registry_catalog_read_snapshot", catalog_snapshot, raising=False)
    monkeypatch.setattr(composer, "_pin", AsyncMock(return_value=manifest))
    monkeypatch.setattr(composer, "registry_schema", lambda: "sample_registry")
    monkeypatch.setattr(provider, "read_npi_detail", AsyncMock(side_effect=native_read))
    monkeypatch.setattr(
        composer,
        "read_registry_provider_plans",
        AsyncMock(
            return_value={
                "generation": "9",
                "approved_custom_revision": "4",
                "npi_data": [],
                "plan_data": [],
                "issuer_data": [],
            }
        ),
    )
    monkeypatch.setattr(
        composer,
        "resolve_registry_network_catalog_legacy",
        AsyncMock(return_value={"generation": "9", "approved_custom_revision": "4", "items": [{"network_id": 84}]}),
    )


@pytest.fixture
def detail_boundary(monkeypatch):
    """Isolate orchestration only; source and database authority are not simulated proof."""
    candidate = "11111111-1111-4111-8111-111111111111"
    manifest = PinnedNetworkServingManifest(
        9, candidate, "network_candidate_" + candidate.replace("-", ""), 1, {}, 4, "b" * 64, 123
    )
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=1))
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    state = SimpleNamespace(
        active=False,
        snapshots=[],
        seen=[],
        document={
            "npi": int(NPI),
            "address_list": [],
            "taxonomy_list": [{"healthcare_provider_taxonomy_code": "207Q00000X"}],
        },
    )

    @asynccontextmanager
    async def snapshot(database, schema, *, include_detail):
        assert database is provider.db and include_detail is True
        state.snapshots.append(schema)
        state.active = True
        try:
            yield session
        finally:
            state.active = False

    async def native_read(request, npi, *, native_args):
        assert state.active and current_network_address_scope().manifest is manifest
        state.seen.append((request, npi, native_args, current_network_address_scope()))
        return response.json(state.document)

    async def metadata(read_session, pin, npi, document, **options):
        assert state.active and read_session is session and pin is manifest and npi == NPI
        assert current_network_address_scope().manifest is manifest
        return document | {"metadata_projected": True}

    metadata_module = ModuleType("api.provider_detail_network_metadata")
    metadata_module.project_managed_detail_metadata = AsyncMock(side_effect=metadata)
    monkeypatch.setitem(sys.modules, metadata_module.__name__, metadata_module)
    monkeypatch.setattr(composer, "provider_profile_read_snapshot", snapshot)
    _install_catalog_mocks(monkeypatch, manifest, native_read)
    request = Request(
        b"/api/v1/registry/serving/providers/detail?force_address_update=true&address_limit=all",
        Header({}),
        "1.1",
        "POST",
        None,
        SimpleNamespace(config={}),
    )
    return SimpleNamespace(
        driver=driver,
        manifest=manifest,
        state=state,
        request=request,
        metadata=metadata_module.project_managed_detail_metadata,
    )


async def _read(boundary, **query_fields):
    query = parse_managed_detail_query({"npi": NPI} | query_fields)
    return await composer.read_managed_provider_detail(
        boundary.request, query, excluded_network_ids=(7,), access_scope_sha256=DIGEST
    )


@pytest.mark.asyncio
async def test_default_snapshot_preserves_known_zero_office_identity(detail_boundary):
    boundary = detail_boundary
    original_args_by_name = dict(boundary.request.args)
    result = await _read(boundary)
    assert result == {
        "generation": "9",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
        "data": {"npi": boundary.state.document | {"metadata_projected": True}},
    }
    composer._pin.assert_awaited_once_with(boundary.driver, None, "sample_registry")
    assert len(boundary.state.snapshots) == 1 and not boundary.state.active
    request, npi, args, scope = boundary.state.seen[0]
    assert request is boundary.request and npi == NPI
    assert args == {"address_limit": "5", "sync_geocode": "false", "lookup_stored_geocode": "false"}
    assert scope.network_ids is None and scope.excluded_network_ids == (7,) and scope.access_scope_sha256 == DIGEST
    assert dict(boundary.request.args) == original_args_by_name
    assert current_network_address_scope() is None
    boundary.driver.fetchval.assert_not_awaited()
    composer.read_registry_provider_plans.assert_not_awaited()


@pytest.mark.asyncio
async def test_includes_share_manifest_and_taxonomy_comes_from_exact_projected_native_detail(detail_boundary):
    boundary = detail_boundary
    result = await _read(boundary, include=["plans", "taxonomy"], network_generation="9", debug="true")
    assert result["data"]["taxonomy"] == result["data"]["npi"]["taxonomy_list"]
    assert result["data"]["plans_by_npi"] == {"npi_data": [], "plan_data": [], "issuer_data": []}
    composer._pin.assert_awaited_once_with(boundary.driver, 9, "sample_registry")
    plan_call = composer.read_registry_provider_plans.await_args
    assert plan_call.args[0] is boundary.driver and plan_call.kwargs["manifest"] is boundary.manifest
    assert plan_call.args[1].generation_id == 9 and "network_ids" not in plan_call.kwargs
    assert boundary.metadata.await_args.kwargs == {"include_sources": True, "include_evidence": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("network_ids", [[84], list(range(100, 200))])
async def test_canonical_targets_require_one_active_approved_set(detail_boundary, network_ids):
    boundary = detail_boundary
    boundary.driver.fetchval.return_value = len(network_ids)
    await _read(boundary, selector={"namespace": "canonical", "values": network_ids}, include=["plans"])
    sql, revision, selected, excluded = boundary.driver.fetchval.await_args.args
    assert revision == 4 and selected == [str(network_id) for network_id in network_ids] and excluded == ["7"]
    assert "record_kind='network'" in sql and "archived'='false'" in sql and "NOT record_key=ANY" in sql
    assert "LIMIT" not in sql and "registry_approved_record" in sql
    boundary.driver.fetchval.assert_awaited_once()
    assert boundary.state.seen[0][3].network_ids == tuple(network_ids)
    assert composer.read_registry_provider_plans.await_args.kwargs["network_ids"] == tuple(network_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1])
async def test_partial_hidden_missing_or_archived_selection_refuses_whole_detail(detail_boundary, count):
    boundary = detail_boundary
    boundary.driver.fetchval.return_value = count
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
        await _read(boundary, selector={"namespace": "canonical", "values": [7, 84]})
    provider.read_npi_detail.assert_not_awaited()
    boundary.metadata.assert_not_awaited()
    assert not boundary.state.active and current_network_address_scope() is None


@pytest.mark.asyncio
async def test_legacy_mapping_uses_exact_generation_before_native_and_selected_plans(detail_boundary):
    boundary = detail_boundary
    await _read(boundary, selector={"namespace": "checksum_network", "values": ["42"]}, include=["plans"])
    call = composer.resolve_registry_network_catalog_legacy.await_args
    assert call.kwargs["generation_id"] == 9 and call.kwargs["excluded_network_ids"] == (7,)
    assert boundary.state.seen[0][3].network_ids == (84,)
    assert composer.read_registry_provider_plans.await_args.kwargs["network_ids"] == (84,)
    composer._pin.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["ambiguous", "stale"])
async def test_legacy_ambiguity_or_stale_mapping_never_falls_back_to_native(detail_boundary, failure):
    boundary = detail_boundary
    if failure == "ambiguous":
        composer.resolve_registry_network_catalog_legacy.side_effect = RegistryNetworkCatalogError(
            "registry_network_catalog_detail_denied"
        )
    else:
        composer.resolve_registry_network_catalog_legacy.return_value["generation"] = "10"
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied|source_changed"):
        await _read(boundary, selector={"namespace": "checksum_network", "values": ["42"]})
    provider.read_npi_detail.assert_not_awaited()
    boundary.metadata.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [ServiceUnavailable("Unavailable source")])
async def test_native_http_refusal_is_preserved_without_metadata_plans_or_fallback(detail_boundary, refusal):
    boundary = detail_boundary
    provider.read_npi_detail.side_effect = refusal
    with pytest.raises(type(refusal)) as caught:
        await _read(boundary, include=["plans"])
    assert caught.value is refusal
    boundary.metadata.assert_not_awaited()
    composer.read_registry_provider_plans.assert_not_awaited()
    assert not boundary.state.active and current_network_address_scope() is None


@pytest.mark.asyncio
async def test_pin_storage_failure_is_terminal_before_native(detail_boundary):
    composer._pin.side_effect = asyncpg.QueryCanceledError("private database diagnostic")
    with pytest.raises(RegistryNetworkCatalogError, match="registry_network_catalog_unavailable") as caught:
        await _read(detail_boundary)
    assert isinstance(caught.value.__cause__, asyncpg.QueryCanceledError)
    assert "private" not in str(caught.value)
    provider.read_npi_detail.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["utf8", "identity", "taxonomy", "surrogate", "overflow", "plans_pin"])
async def test_closed_document_utf8_full_response_bounds_and_plan_pin_refuse(detail_boundary, monkeypatch, failure):
    boundary = detail_boundary
    includes = []
    if failure == "utf8":
        provider.read_npi_detail.side_effect = None
        provider.read_npi_detail.return_value = response.raw(b"\xff", content_type="application/json")
    if failure == "identity":
        boundary.state.document["npi"] = int(NPI) + 1
    if failure == "taxonomy":
        boundary.state.document["taxonomy_list"] = None
        includes = ["taxonomy"]
    if failure == "surrogate":
        boundary.metadata.side_effect = None
        boundary.metadata.return_value = {"npi": int(NPI), "name": "\ud800"}
    if failure == "overflow":
        monkeypatch.setattr(composer, "MAX_DETAIL_BYTES", 300)
        boundary.metadata.side_effect = None
        boundary.metadata.return_value = {"npi": int(NPI), "name": "é" * 200}
    if failure == "plans_pin":
        composer.read_registry_provider_plans.return_value["approved_custom_revision"] = "5"
        includes = ["plans"]
    with pytest.raises(RegistryNetworkCatalogError):
        await _read(boundary, include=includes)
    assert not boundary.state.active and current_network_address_scope() is None


@pytest.mark.asyncio
async def test_metadata_refusal_does_not_return_cached_native_detail(detail_boundary):
    refusal = ServiceUnavailable("Verified memberships unavailable")
    detail_boundary.metadata.side_effect = refusal
    with pytest.raises(ServiceUnavailable) as caught:
        await _read(detail_boundary, include=["plans"])
    assert caught.value is refusal
    provider.read_npi_detail.assert_awaited_once()
    composer.read_registry_provider_plans.assert_not_awaited()
    assert current_network_address_scope() is None and not detail_boundary.state.active


@pytest.mark.asyncio
async def test_returned_native_http_error_preserves_status_without_echoing_body(detail_boundary):
    provider.read_npi_detail.side_effect = None
    provider.read_npi_detail.return_value = response.raw(b"private diagnostic", status=404)
    with pytest.raises(SanicException) as caught:
        await _read(detail_boundary)
    assert caught.value.status_code == 404 and "private" not in str(caught.value)
    detail_boundary.metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_nonquery_objects_refuse_before_snapshot_or_native(detail_boundary):
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await composer.read_managed_provider_detail(
            detail_boundary.request, SimpleNamespace(npi=NPI), excluded_network_ids=(), access_scope_sha256=DIGEST
        )
    assert not detail_boundary.state.snapshots
    provider.read_npi_detail.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace,values", [("canonical", [84]), ("checksum_network", ["42"])])
async def test_organization_policy_is_forwarded_without_transitive_network_denial(detail_boundary, namespace, values):
    boundary = detail_boundary
    policy = RegistryOrganizationVisibility(("22222222-2222-4222-8222-222222222222",), ())
    query = parse_managed_detail_query({"npi": NPI, "selector": {"namespace": namespace, "values": values}})
    result = await composer.read_managed_provider_detail(
        boundary.request, query, excluded_network_ids=(7,), access_scope_sha256=DIGEST, organization_policy=policy
    )
    assert result["data"]["npi"]["npi"] == int(NPI)
    if namespace == "checksum_network":
        assert composer.resolve_registry_network_catalog_legacy.await_args.kwargs["organization_policy"] is policy
    else:
        assert "company" not in boundary.driver.fetchval.await_args.args[0]
        composer.resolve_registry_network_catalog_legacy.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_cancellation_releases_snapshot_and_scope_without_fallback(detail_boundary):
    provider.read_npi_detail.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await _read(detail_boundary, include=["plans"])
    assert not detail_boundary.state.active and current_network_address_scope() is None
    detail_boundary.metadata.assert_not_awaited()
    composer.read_registry_provider_plans.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_absence_retains_the_authorized_actual_manifest(detail_boundary):
    boundary = detail_boundary
    provider.read_npi_detail.side_effect = NotFound("Absent provider")
    with pytest.raises(composer.ManagedProviderDetailAbsent) as caught:
        await _read(boundary, include=["plans"])
    assert caught.value.proof == {
        "generation": "9",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
        "npi": NPI,
        "access_scope_sha256": DIGEST,
    }
    boundary.metadata.assert_not_awaited()
    composer.read_registry_provider_plans.assert_not_awaited()
    assert not boundary.state.active and current_network_address_scope() is None
