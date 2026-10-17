# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Collections share the detail pin and restore the actual request Reader binding."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic import Request, response
from sanic.compat import Header
from sanic.exceptions import SanicException

from api import provider_batch
from api import provider_collection_network_read as collection
from api import provider_detail_network_read as detail
from api.endpoint import npi as provider
from api.network_address_scope import current_network_address_scope
from api.provider_collection_network_contract import parse_managed_collection_query
from process import registry_network_catalog_read as catalog
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from process.registry_network_catalog_read import RegistryNetworkCatalogError

NPI = "0000000049"
DIGEST = "a" * 64


@pytest.fixture
def boundary(monkeypatch):
    candidate = "11111111-1111-4111-8111-111111111111"
    manifest = PinnedNetworkServingManifest(
        9, candidate, "network_candidate_" + candidate.replace("-", ""), 1, {}, 4, "b" * 64, 123
    )
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=1))
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    state = SimpleNamespace(
        active=False, catalog_active=False, snapshots=[], seen=[], document={"rows": [], "total_source": "omitted"}
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

    @asynccontextmanager
    async def catalog_snapshot(driver, generation, schema):
        state.catalog_active = True
        try:
            yield await detail._pin(driver, generation, schema)
        finally:
            state.catalog_active = False

    async def native_read(request, *, native_args):
        assert state.active and state.catalog_active and request.ctx.sa_session is session
        scope = current_network_address_scope()
        assert scope.manifest is manifest
        state.seen.append((request, native_args, scope))
        return response.json(state.document, default=str)

    async def batch_read(request, batch_params, *, native_args, session):
        assert request.ctx.sa_session is session
        await native_read(request, native_args=native_args)
        state.batch = batch_params
        return state.document

    monkeypatch.setattr(collection, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(detail, "registry_schema", lambda: "sample_registry")
    monkeypatch.setattr(detail, "_pin", AsyncMock(return_value=manifest))
    monkeypatch.setattr(catalog, "registry_catalog_read_snapshot", catalog_snapshot, raising=False)
    monkeypatch.setattr(
        detail,
        "resolve_registry_network_catalog_legacy",
        AsyncMock(return_value={"generation": "9", "approved_custom_revision": "4", "items": [{"network_id": 84}]}),
    )
    monkeypatch.setattr(provider, "list_providers", AsyncMock(side_effect=native_read))
    monkeypatch.setattr(provider, "get_near_npi", AsyncMock(side_effect=native_read))
    monkeypatch.setattr(provider_batch, "read_native_batch", AsyncMock(side_effect=batch_read))
    path = b"/api/v1/registry/serving/providers/collection?plan_network=99"
    request = Request(path, Header({}), "1.1", "POST", None, SimpleNamespace(config={}))
    return SimpleNamespace(request=request, driver=driver, session=session, manifest=manifest, state=state)


def _query(kind="list", **options):
    query_by_field = {"kind": kind, "native_query": {"include_total": "false"} if kind != "batch" else {}}
    if kind == "batch":
        query_by_field["native_batch"] = {"npis": [NPI]}
    return parse_managed_collection_query(query_by_field | options)


async def _read(boundary, query=None, **policy):
    return await collection.read_managed_provider_collection(
        boundary.request, query or _query(), excluded_network_ids=(7,), access_scope_sha256=DIGEST, **policy
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
@pytest.mark.parametrize("has_prior", [False, True])
async def test_all_native_families_share_common_scope_and_actual_reader(boundary, kind, has_prior):
    prior = object()
    if has_prior:
        boundary.request.ctx.sa_session = prior
    original_args_by_name = dict(boundary.request.args)
    result = await _read(boundary, _query(kind, network_generation="9"))
    assert result == {
        "generation": "9",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
        "data": boundary.state.document,
    }
    detail._pin.assert_awaited_once_with(boundary.driver, 9, "sample_registry")
    boundary.driver.fetchval.assert_not_awaited()
    assert len(boundary.state.snapshots) == 1 and not boundary.state.active and not boundary.state.catalog_active
    request, args, scope = boundary.state.seen[0]
    assert request is boundary.request and scope.network_ids is None
    assert scope.excluded_network_ids == (7,) and scope.access_scope_sha256 == DIGEST
    assert args.get("include_total") == ("true" if kind == "batch" else "false")
    assert "plan_network" not in args and dict(boundary.request.args) == original_args_by_name
    if kind == "batch":
        assert boundary.state.batch["npis"] == [49]
    assert getattr(boundary.request.ctx, "sa_session", None) is (prior if has_prior else None)
    assert hasattr(boundary.request.ctx, "sa_session") is has_prior
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace,values", [("canonical", [84]), ("checksum_network", ["42"])])
async def test_selected_and_retained_scope_precedes_native(boundary, namespace, values):
    policy = RegistryOrganizationVisibility(("22222222-2222-4222-8222-222222222222",), ())
    await _read(
        boundary,
        _query(selector={"namespace": namespace, "values": values}, network_generation="9"),
        organization_policy=policy,
    )
    assert boundary.state.seen[0][2].network_ids == (84,)
    detail._pin.assert_awaited_once_with(boundary.driver, 9, "sample_registry")
    if namespace == "canonical":
        assert boundary.driver.fetchval.await_args.args[1:] == (4, ["84"], ["7"])
    else:
        options = detail.resolve_registry_network_catalog_legacy.await_args.kwargs
        assert options["generation_id"] == 9 and options["organization_policy"] is policy
        assert options["manifest"] is boundary.manifest


@pytest.mark.asyncio
async def test_denied_selector_stops_before_native_and_session_binding(boundary):
    boundary.driver.fetchval.return_value = 0
    with pytest.raises(RegistryNetworkCatalogError, match="detail_denied"):
        await _read(boundary, _query(selector={"namespace": "canonical", "values": [84]}))
    assert not boundary.state.seen and not boundary.state.active
    assert not hasattr(boundary.request.ctx, "sa_session") and current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
@pytest.mark.parametrize("status", [400, 403, 404, 409, 503])
async def test_native_failures_keep_status_without_private_payload_and_restore_request(boundary, kind, status):
    prior = object()
    boundary.request.ctx.sa_session = prior
    native = {"list": provider.list_providers, "geo": provider.get_near_npi, "batch": provider_batch.read_native_batch}[
        kind
    ]
    native.side_effect = SanicException("private native diagnostic", status_code=status)
    with pytest.raises(SanicException) as caught:
        await _read(boundary, _query(kind))
    assert caught.value.status_code == status and "private" not in str(caught.value)
    assert boundary.request.ctx.sa_session is prior and current_network_address_scope() is None
    assert not boundary.state.active


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["returned", "utf8", "duplicate", "nonfinite", "scalar", "surrogate", "overflow", "database", "cancel"]
)
async def test_private_failures_and_bounds_release_all_context(boundary, monkeypatch, failure):
    if failure == "returned":
        provider.list_providers.side_effect = None
        provider.list_providers.return_value = response.raw(b"private diagnostic", status=404)
    elif failure in {"utf8", "duplicate", "nonfinite", "scalar"}:
        provider.list_providers.side_effect = None
        content = {
            "utf8": b"\xff",
            "duplicate": b'{"rows":[],"rows":[]}',
            "nonfinite": b'{"rows":NaN}',
            "scalar": b"1",
        }[failure]
        provider.list_providers.return_value = response.raw(content, content_type="application/json")
    elif failure == "surrogate":
        boundary.state.document = {"name": "\ud800"}
    elif failure == "overflow":
        monkeypatch.setattr(collection, "MAX_COLLECTION_BYTES", 250)
        boundary.state.document = {"name": "é" * 100}
    if failure == "database":
        detail._pin.side_effect = asyncpg.QueryCanceledError("private database diagnostic")
    if failure == "cancel":
        provider.list_providers.side_effect = asyncio.CancelledError()
    expected = asyncio.CancelledError if failure == "cancel" else (RegistryNetworkCatalogError, SanicException)
    with pytest.raises(expected) as caught:
        await _read(boundary)
    assert "private" not in str(caught.value)
    assert not hasattr(boundary.request.ctx, "sa_session") and not boundary.state.active
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["object", "duplicate", "selector", "batch", "raw"])
async def test_forged_dto_refuses_before_reader(boundary, forgery):
    query = _query()
    if forgery == "object":
        query = SimpleNamespace(kind="list")
    elif forgery == "duplicate":
        query = replace(query, native_query=(("q", "one"), ("q", "two")))
    elif forgery == "selector":
        query = replace(query, selector=(True,))
    elif forgery == "batch":
        query = replace(query, native_batch={"npis": [NPI]})
    else:
        query = replace(query, native_query=(("plan_network", "42"),))
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await _read(boundary, query)
    assert not boundary.state.snapshots and not boundary.state.seen


@pytest.mark.asyncio
async def test_native_cancellation_removes_absent_binding(boundary):
    provider.list_providers.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await _read(boundary)
    assert not hasattr(boundary.request.ctx, "sa_session") and current_network_address_scope() is None


@pytest.mark.asyncio
async def test_requested_generation_mismatch_refuses_before_native(boundary):
    with pytest.raises(RegistryNetworkCatalogError, match="source_changed"):
        await _read(boundary, _query(network_generation="10"))
    assert not boundary.state.seen and not boundary.state.active
    assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_batch_preserves_existing_native_json_scalar_formatting(boundary):
    boundary.state.document = {
        "items": [{"npi": 49, "status": 200, "provider": {"enumeration_date": date(2026, 1, 1)}}]
    }
    result = await _read(boundary, _query("batch"))
    assert result["data"]["items"][0]["provider"]["enumeration_date"] == "2026-01-01"


def _service_refusal(code="ptg2_provider_scope_refused"):
    return {
        "status": 422,
        "code": code,
        "message": "Bounded service scope required.",
        "fix_it": {"reason": "Choose a smaller scope.", "retry_options": []},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "ptg2_provider_scope_refused",
        "ptg2_online_work_budget_exceeded",
        "ptg2_provider_filter_scope_required",
        "ptg2_location_scope_too_broad",
        "ptg2_provider_filter_unsupported",
    ],
)
@pytest.mark.parametrize("has_prior", [False, True])
async def test_service_refusal_is_closed_and_restores_reader(boundary, monkeypatch, code, has_prior):
    from api import custom_import_provider_service_http as service

    prior = object()
    if has_prior:
        boundary.request.ctx.sa_session = prior
    refusal_by_field = _service_refusal(code)
    refusal_by_field.update(context={"diagnostic": "discarded context"}, detail="discarded detail")
    refusal_by_field["fix_it"].update(retry_options=[{"code": "discarded guidance"}], diagnostic="discarded")

    async def refused(request, *, native_args):
        assert request is boundary.request and request.ctx.sa_session is boundary.session
        assert boundary.state.active and current_network_address_scope().manifest is boundary.manifest
        assert native_args.get("code") == "99213"
        return response.json(refusal_by_field, status=422)

    monkeypatch.setattr(service, "_service_page", refused)
    with pytest.raises(collection.ProviderCollectionServiceRefusal) as caught:
        await _read(boundary, _query("by-service", native_query={"code": "99213"}))
    assert caught.value.document == _service_refusal(code)
    assert "discarded" not in str(caught.value) and "discarded" not in json.dumps(caught.value.document)
    detail._pin.assert_awaited_once()
    assert len(boundary.state.snapshots) == 1 and not boundary.state.active and not boundary.state.catalog_active
    assert getattr(boundary.request.ctx, "sa_session", None) is (prior if has_prior else None)
    assert hasattr(boundary.request.ctx, "sa_session") is has_prior
    assert current_network_address_scope() is None


@pytest.mark.parametrize(
    "changes",
    [
        {"status": True},
        {"status": "422"},
        {"status": 422.0},
        {"status": 400},
        {"code": "unknown"},
        {"code": []},
        {"message": ""},
        {"message": 12},
        {"message": "\ud800"},
        {"fix_it": []},
        {"fix_it": {}},
        {"fix_it": {"reason": "", "retry_options": []}},
        {"fix_it": {"reason": 12, "retry_options": []}},
        {"fix_it": {"reason": "\ud800", "retry_options": []}},
        {"fix_it": {"reason": "scope", "retry_options": {}}},
        {"fix_it": {"reason": "scope", "retry_options": [1]}},
        {"fix_it": {"reason": "x" * 8192, "retry_options": []}},
        {"fix_it": {"reason": "\n" * 4096, "retry_options": []}},
    ],
)
def test_service_refusal_rejects_malformed_fields(changes):
    encoded = json.dumps(_service_refusal() | changes).encode()
    with pytest.raises(SanicException) as caught:
        collection._native_document(response.raw(encoded, status=422), preserve_service_refusal=True)
    assert caught.value.status_code == 503 and "scope" not in str(caught.value)


@pytest.mark.parametrize(
    "encoded",
    [
        b"\xff",
        b"{",
        b"[]",
        b"null",
        b'{"status":422,"status":422}',
        b'{"status":NaN}',
        b"[" * 2000,
        b" " * (1024 * 1024 + 1),
    ],
)
def test_service_refusal_rejects_invalid_json_and_body_bounds(encoded):
    with pytest.raises(SanicException) as caught:
        collection._native_document(response.raw(encoded, status=422), preserve_service_refusal=True)
    assert caught.value.status_code == 503


def test_service_refusal_uses_existing_unicode_and_guidance_bounds():
    refusal_by_field = _service_refusal()
    refusal_by_field["message"] = "é" * 301
    fixed_bytes = len(json.dumps({"reason": "", "retry_options": []}, separators=(",", ":")).encode())
    refusal_by_field["fix_it"]["reason"] = "x" * (8192 - fixed_bytes)
    encoded = json.dumps(refusal_by_field).encode()
    encoded += b" " * (1024 * 1024 - len(encoded))
    with pytest.raises(collection.ProviderCollectionServiceRefusal) as caught:
        collection._native_document(response.raw(encoded, status=422), preserve_service_refusal=True)
    assert caught.value.document["message"] == "é" * 300
    assert caught.value.document["fix_it"] == refusal_by_field["fix_it"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo"])
async def test_other_native_collections_do_not_preserve_service_refusals(boundary, kind):
    native = provider.list_providers if kind == "list" else provider.get_near_npi
    native.side_effect = None
    native.return_value = response.json(_service_refusal(), status=422)
    with pytest.raises(SanicException) as caught:
        await _read(boundary, _query(kind))
    assert caught.value.status_code == 503 and not hasattr(boundary.request.ctx, "sa_session")
    assert not boundary.state.active and current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "geo", "batch", "by-service"])
async def test_unstructured_native_422_is_unavailable(boundary, monkeypatch, kind):
    from api import custom_import_provider_service_http as service

    failure = AsyncMock(side_effect=SanicException("discarded diagnostic", status_code=422))
    monkeypatch.setattr(service, "_service_page", failure)
    native = {
        "list": provider.list_providers,
        "geo": provider.get_near_npi,
        "batch": provider_batch.read_native_batch,
    }.get(kind)
    if native is not None:
        native.side_effect = failure.side_effect
    query = _query(kind, **({"native_query": {"code": "99213"}} if kind == "by-service" else {}))
    with pytest.raises(SanicException) as caught:
        await _read(boundary, query)
    assert caught.value.status_code == 503 and "discarded" not in str(caught.value)
    assert not boundary.state.active and not hasattr(boundary.request.ctx, "sa_session")
