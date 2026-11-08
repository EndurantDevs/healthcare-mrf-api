# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release collections keep native authority, policy custody and closed inputs."""

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sanic import response
from sanic.exceptions import SanicException

from api import billing_search_http, custom_import_plan_sql, provider_billing_network_read
from api import custom_import_provider_service_http as service
from api.billing_search_transport_contract import _framed_sha256
from api.endpoint import network_catalog as endpoint
from api.endpoint import npi as provider
from api.endpoint import pricing
from api.network_address_scope import current_network_address_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.provider_collection_network_contract import parse_managed_collection_query
from api.provider_collection_network_read import read_managed_provider_collection
from tests import test_billing_search_endpoint_access as signed_billing
from tests.test_billing_search_endpoint_access import (
    PLAN_RELEASE_ID,
    TRUSTED_NOW,
    _authorize,
    _keyring,
    _parameters,
    _signed_headers,
)
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import DIGEST
from tests.test_provider_collection_network_read import boundary as boundary
from tests.test_registry_provider_collection_routes import _post
from tests.test_registry_provider_collection_routes import collection_context as collection_context


@pytest.fixture(autouse=True)
def signed_tenant(monkeypatch):
    original = signed_billing._context

    def context(*args, **options):
        claims = original(*args, **options)
        claims["tenant_scope_sha256"] = _framed_sha256(
            b"HEALTHPORTA_BILLING_SEARCH_TENANT_SCOPE_V1\x00",
            json.dumps({"client_id": "sample-client"}, separators=(",", ":"), ensure_ascii=False).encode(),
        )
        return claims

    monkeypatch.setattr(signed_billing, "_context", context)


def _document(kind):
    native_by_name = {"plan_release_id": PLAN_RELEASE_ID, "limit": "1000", "cursor": "1000000004"}
    if kind == "billing-search":
        native_by_name = _parameters()
        native_by_name["billing_transport_context_sha256"] = _authorize().verified_transport.transport_context_sha256
    return {"kind": kind, "native_query": native_by_name}


async def _read(boundary, document):
    return await read_managed_provider_collection(
        boundary.request,
        parse_managed_collection_query(document),
        excluded_network_ids=(7,),
        access_scope_sha256=DIGEST,
        **({"client_id": "sample-client"} if document["kind"] == "billing-search" else {}),
    )


@pytest.mark.parametrize("kind", ["group-plan", "billing-search"])
def test_release_collection_freezes_native_options_without_service_code(kind):
    document = _document(kind)
    query = parse_managed_collection_query(document)
    assert query.kind == kind and dict(query.native_query) == document["native_query"]
    assert query.selector is None and query.native_batch is None


@pytest.mark.parametrize("kind", ["group-plan", "billing-search"])
@pytest.mark.parametrize("change", ["native_batch", "unknown", "missing_release", "invalid_release"])
def test_release_collection_rejects_hidden_authority_and_missing_release(kind, change):
    document = _document(kind)
    if change == "native_batch":
        document[change] = {"npis": ["1000000004"]}
    elif change == "unknown":
        document["native_query"]["access_scope_sha256"] = DIGEST
    elif change == "missing_release":
        del document["native_query"]["plan_release_id"]
    else:
        document["native_query"]["plan_release_id"] = "unpublished"
    with pytest.raises(ValueError):
        parse_managed_collection_query(document)


@pytest.mark.parametrize("kind", ["by-service", "group-plan", "billing-search"])
def test_pricing_collection_preserves_explicit_selector_separately_from_native(kind):
    document = _document(kind) if kind != "by-service" else {"kind": kind, "native_query": {"code": "99213"}}
    document["selector"] = {"namespace": "canonical", "values": [84, 42]}
    query = parse_managed_collection_query(document)
    assert query.selector == (42, 84)
    assert dict(query.native_query) == document["native_query"]


@pytest.mark.parametrize("field", ["limit", "cursor"])
@pytest.mark.parametrize("value", ["0", "-1", "+1", " 1", "1.0", "١", "10000000000"])
def test_group_collection_bounds_before_reader(field, value):
    document = _document("group-plan")
    document["native_query"][field] = value
    with pytest.raises(ValueError):
        parse_managed_collection_query(document)


@pytest.mark.parametrize("value", ["1001", "2000"])
def test_group_limit_cannot_exceed_native_cap(value):
    document = _document("group-plan")
    document["native_query"]["limit"] = value
    with pytest.raises(ValueError):
        parse_managed_collection_query(document)


@pytest.mark.parametrize(
    "field,value",
    [("limit", "101"), ("cursor", "unsealed"), ("code", ""), ("billing_transport_context_sha256", "A" * 64)],
)
def test_billing_collection_reuses_native_strict_parser(field, value):
    document = _document("billing-search")
    document["native_query"][field] = value
    with pytest.raises(ValueError):
        parse_managed_collection_query(document)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["group-plan", "billing-search"])
@pytest.mark.parametrize("selected", [False, True])
async def test_release_dispatch_keeps_real_request_scope_and_reader(boundary, monkeypatch, kind, selected):
    document = _document(kind)
    if selected:
        document["selector"] = {"namespace": "canonical", "values": [84]}
    original_args_by_name = dict(boundary.request.args)
    authorizations = []
    if kind == "billing-search":
        boundary.request.headers.update(_signed_headers())
        monkeypatch.setattr(billing_search_http, "_trusted_now", lambda: TRUSTED_NOW)
        monkeypatch.setattr(billing_search_http, "_transport_keyring", _keyring)
        authorize = provider_billing_network_read.authorize_managed_billing_search

        def pre_reader(request, *, native_args):
            assert not boundary.state.active and not boundary.state.snapshots
            authorizations.append(authorize(request, native_args=native_args))
            return authorizations[-1]

        monkeypatch.setattr(provider_billing_network_read, "authorize_managed_billing_search", pre_reader)

    async def native(request, *, native_args, **options):
        assert request is boundary.request and request.ctx.sa_session is boundary.session
        assert boundary.state.active and boundary.state.catalog_active
        assert current_network_address_scope().manifest is boundary.manifest
        assert current_network_address_scope().network_ids == ((84,) if selected else None)
        assert dict((name, native_args.get(name)) for name in native_args) == document["native_query"]
        assert options == ({"session": boundary.session} if kind == "billing-search" else {})
        return response.json({"items": [], "cursor": None, "native_contract": kind})

    if kind == "group-plan":
        monkeypatch.setattr(pricing, "group_plan_providers", native)
    else:
        monkeypatch.setattr(provider_billing_network_read, "read_managed_billing_search", native)
    payload_by_field = await _read(boundary, document)
    assert payload_by_field["data"] == {"items": [], "cursor": None, "native_contract": kind}
    assert payload_by_field["generation"] == "9" and len(authorizations) == (kind == "billing-search")
    assert not boundary.state.active and not boundary.state.catalog_active
    assert dict(boundary.request.args) == original_args_by_name
    assert not hasattr(boundary.request.ctx, "sa_session") and current_network_address_scope() is None


@pytest.mark.asyncio
async def test_invalid_real_billing_signature_refuses_before_any_reader(boundary, monkeypatch):
    document = _document("billing-search")
    monkeypatch.setattr(billing_search_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_search_http, "_transport_keyring", _keyring)
    with pytest.raises(SanicException) as caught:
        await _read(boundary, document)
    assert caught.value.status_code == 404
    assert not boundary.state.snapshots and not hasattr(boundary.request.ctx, "sa_session")
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["group-plan", "billing-search"])
async def test_release_kind_crosses_actual_collection_http(collection_context, boundary, monkeypatch, kind):
    seen_requests = []

    async def native(request, *, native_args, **options):
        assert request.method == "POST" and request.path.endswith("/providers/collection")
        assert request.ctx.sa_session is boundary.session and boundary.state.active
        assert current_network_address_scope().manifest is boundary.manifest
        seen_requests.append(request)
        return response.json({"items": [], "next_cursor": None})

    headers_by_name = {"Authorization": "Bearer synthetic-collection-control"}
    if kind == "billing-search":
        headers_by_name.update(_signed_headers())
        monkeypatch.setattr(billing_search_http, "_trusted_now", lambda: TRUSTED_NOW)
        monkeypatch.setattr(billing_search_http, "_transport_keyring", _keyring)
        monkeypatch.setattr(provider_billing_network_read, "read_managed_billing_search", native)
    else:
        monkeypatch.setattr(pricing, "group_plan_providers", native)
    monkeypatch.setattr(endpoint, "read_managed_provider_collection", read_managed_provider_collection)
    envelope = v2_envelope(_document(kind))
    reply = await _post(collection_context, envelope, headers=headers_by_name)
    assert reply.status == 200 and reply.json["data"] == {"items": [], "next_cursor": None}
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == envelope["access_scope_sha256"]
    assert len(seen_requests) == 1 and not hasattr(seen_requests[0].ctx, "sa_session")
    assert current_network_address_scope() is None and not boundary.state.active


@pytest.mark.asyncio
async def test_collection_http_binds_real_billing_signature_to_actual_query(collection_context, boundary, monkeypatch):
    monkeypatch.setattr(endpoint, "read_managed_provider_collection", read_managed_provider_collection)
    monkeypatch.setattr(billing_search_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_search_http, "_transport_keyring", _keyring)
    reader = AsyncMock(side_effect=AssertionError("unverified native read"))
    monkeypatch.setattr(provider_billing_network_read, "read_managed_billing_search", reader)
    document = _document("billing-search")
    document["native_query"]["code"] = "99213"
    headers_by_name = {"Authorization": "Bearer synthetic-collection-control", **_signed_headers()}
    reply = await _post(collection_context, v2_envelope(document), headers=headers_by_name)
    assert reply.status == 404 and "X-Network-Generation" not in reply.headers
    assert not boundary.state.snapshots and current_network_address_scope() is None
    reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_rejects_another_policy_client_before_reader(collection_context, boundary, monkeypatch):
    monkeypatch.setattr(endpoint, "read_managed_provider_collection", read_managed_provider_collection)
    monkeypatch.setattr(billing_search_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(billing_search_http, "_transport_keyring", _keyring)
    reader = AsyncMock(side_effect=AssertionError("cross-client native read"))
    monkeypatch.setattr(provider_billing_network_read, "read_managed_billing_search", reader)
    headers_by_name = {"Authorization": "Bearer synthetic-collection-control", **_signed_headers()}
    reply = await _post(
        collection_context,
        v2_envelope(_document("billing-search"), client_id="different-client"),
        headers=headers_by_name,
    )
    assert reply.status == 404 and "X-Network-Generation" not in reply.headers
    assert not boundary.state.snapshots and current_network_address_scope() is None
    reader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["cursor", "readiness", "cancel"])
async def test_group_failure_restores_reader_and_policy(boundary, monkeypatch, failure_kind):
    failures_by_kind = {
        "cursor": SanicException("private group failure", status_code=409),
        "readiness": PlanPricingProjectionUnavailable("private readiness failure"),
        "cancel": asyncio.CancelledError(),
    }
    failure = failures_by_kind[failure_kind]
    monkeypatch.setattr(pricing, "group_plan_providers", AsyncMock(side_effect=failure))
    from process.registry_network_catalog_read import RegistryNetworkCatalogError

    expected = {
        "cursor": SanicException,
        "readiness": RegistryNetworkCatalogError,
        "cancel": asyncio.CancelledError,
    }[failure_kind]
    with pytest.raises(expected) as caught:
        await _read(boundary, _document("group-plan"))
    if failure_kind == "cursor":
        assert caught.value.status_code == 409 and "private" not in str(caught.value)
    elif failure_kind == "readiness":
        assert str(caught.value) == "registry_network_catalog_unavailable"
    assert not boundary.state.active and not boundary.state.catalog_active
    assert not hasattr(boundary.request.ctx, "sa_session") and current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["plan_release_id", "plan_id", "plan_external_id", "source_key", "snapshot_id"])
async def test_service_selector_prepares_temp_tables_before_reader(boundary, monkeypatch, selector):
    events = []

    @asynccontextmanager
    async def prepared(*, temporary_setup):
        assert temporary_setup is custom_import_plan_sql.prepare_plan_query_tables
        assert not boundary.state.snapshots
        events.append("prepare")
        try:
            yield
        finally:
            events.append("release")

    async def native(request, *, native_args):
        assert events == ["prepare"] and boundary.state.active
        assert native_args.get(selector) == "exact-selector"
        return response.json({"items": []})

    monkeypatch.setattr(provider.db, "reader_session", prepared)
    monkeypatch.setattr(service, "_service_page", native)
    await _read(boundary, {"kind": "by-service", "native_query": {"code": "99213", selector: "exact-selector"}})
    assert events == ["prepare", "release"]
    assert not boundary.state.active and current_network_address_scope() is None
