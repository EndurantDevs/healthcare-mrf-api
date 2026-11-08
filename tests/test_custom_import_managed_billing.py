# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed billing imports retain one managed Reader and both real authorities."""

import asyncio
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import orjson
import pytest
from sanic import Sanic

from api import billing_search_gateway_transport as gateway
from api import billing_search_transport_contract as contract
from api import billing_search_verified_transport as verified_billing
from api import custom_import_billing_http as billing
from api import custom_import_provider_collection as managed
from api import custom_import_read_http as transport
from api import provider_detail_network_read as detail
from api.network_address_scope import current_network_address_scope
from api.provider_billing_network_read import require_managed_billing_client
from tests import test_billing_search_endpoint_access as native
from tests import test_custom_import_read_http as permits
from tests.custom_import_billing_support import Session, install_pipeline, signed_request
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import boundary as boundary


def _signed(*, client="sample-client", include_filter=True, damage=None):
    request = signed_request(include_filter=include_filter)
    document = json.loads(request.body)
    context = native._context(tuple(sorted(document["native_query"].items())))
    context["tenant_scope_sha256"] = contract._framed_sha256(
        b"HEALTHPORTA_BILLING_SEARCH_TENANT_SCOPE_V1\0", orjson.dumps({"client_id": client})
    )
    encoded = contract._canonical_json_bytes(context)
    signature = hmac.digest(native.KEY, gateway._signature_message(native.KEY_ID, encoded), hashlib.sha256)
    request.headers.update(
        {
            contract.BILLING_SEARCH_TRANSPORT_CONTEXT_HEADER: native._base64url(encoded),
            contract.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER: native._base64url(signature),
        }
    )
    digest = contract._framed_sha256(verified_billing._CONTEXT_DOMAIN, encoded)
    document["billing_transport_context_sha256"] = digest
    document["managed_collection"] = v2_envelope(
        {
            "kind": "billing-search",
            "native_query": document["native_query"] | {"billing_transport_context_sha256": digest},
            "network_generation": "9",
        },
        client_id=client,
        excluded_network_ids=[7],
    )
    match damage:
        case "client":
            document["managed_collection"] = v2_envelope(
                document["managed_collection"]["query"], client_id="other-client"
            )
        case "query":
            document["managed_collection"]["query"]["native_query"]["code"] = "99213"
        case "digest":
            document["managed_collection"]["query"]["native_query"]["billing_transport_context_sha256"] = "a" * 64
        case "policy":
            document["managed_collection"]["access_scope_sha256"] = "0" * 64
        case "shadow":
            document["native_query"]["billing_transport_context_sha256"] = digest
    request.body = transport._canonical_json_bytes(document)
    request.headers.update(
        permits._headers(
            body=request.body,
            path=request.path,
            contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
            body_sha256=transport.managed_provider_body_sha256,
            signature_message=transport.custom_import_provider_signature_message,
        )
    )
    if damage == "outer":
        request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "invalid"
    elif damage == "billing":
        request.headers[contract.BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER] = "invalid"
    elif damage == "target":
        document["target"]["generation_id"] += 1
        request.body = transport._canonical_json_bytes(document)
    return request


def _install(monkeypatch, boundary):
    session = Session()
    session.connection = boundary.session.connection
    install_pipeline(monkeypatch, session)

    @asynccontextmanager
    async def snapshot(*_args, **_kwargs):
        boundary.state.snapshots.append("Reader")
        session.active = True
        try:
            yield session
        finally:
            session.active = False

    @asynccontextmanager
    async def bounded(same_session, **_kwargs):
        assert same_session is session and session.active
        yield

    monkeypatch.setattr(managed, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(managed, "_bounded_read_window", bounded)
    return session


async def _post(request, *, prior=None):
    app = Sanic("managed_billing_" + uuid4().hex)
    observed_requests = []

    @app.post(billing.CUSTOM_IMPORT_BILLING_SEARCH_PATH)
    async def serve(actual):
        observed_requests.append(actual)
        if prior is not None:
            actual.ctx.sa_session = prior
        return await billing.serve_custom_import_billing_search(actual, None)

    _, reply = await app.asgi_client.post(request.path, data=request.body, headers=request.headers)
    assert getattr(observed_requests[0].ctx, "sa_session", None) is prior
    assert current_network_address_scope() is None
    return reply


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("client", ["sample-client", "sample-é"])
async def test_real_signed_billing_http_uses_one_managed_reader(boundary, monkeypatch, include_filter, client):
    session = _install(monkeypatch, boundary)
    search = billing.search_exact_billing_provider_page

    async def scoped(reader, **options):
        assert reader is session and session.active
        scope = current_network_address_scope()
        assert scope.manifest is boundary.manifest and scope.network_ids is None
        assert scope.excluded_network_ids == (7,)
        return await search(reader, **options)

    monkeypatch.setattr(billing, "search_exact_billing_provider_page", scoped)
    reply = await _post(_signed(client=client, include_filter=include_filter), prior=object())
    assert reply.status == 200, reply.body
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.json["managed_collection"]["client_id"] == client
    assert ("custom_import" in reply.json["items"][0]) is include_filter
    assert session.events == ["prepare", "search", "shape"] + (["hydrate"] if include_filter else []) + ["finality"]
    assert len(boundary.state.snapshots) == 1 and not session.active
    detail._pin.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["outer", "billing", "target", "client", "policy", "query", "digest", "shadow"])
async def test_billing_authority_damage_is_rejected_before_reader(boundary, monkeypatch, damage):
    session = _install(monkeypatch, boundary)
    reply = await _post(_signed(damage=damage))
    assert reply.status in {400, 404}, reply.body
    assert set(reply.json) == {"error"}
    assert session.events == [] and boundary.state.snapshots == []
    detail._pin.assert_not_awaited()


def test_managed_billing_unicode_tenant_matches_fixed_rust_vector(monkeypatch):
    request = _signed(client="sample-é")
    access = native._authorize(dict(json.loads(request.body)["native_query"]), request.headers)
    assert (
        access.authorization_context.tenant_scope_sha256
        == "d4eb90bc925dd30a91f3e3fa39a2e76f89cbb4f07c593f423aebf4cceea73391"
    )
    require_managed_billing_client(access, "sample-é")
    with pytest.raises(native.endpoint.BillingSearchEndpointAccessError):
        require_managed_billing_client(access, "sample-client")


@pytest.mark.asyncio
async def test_managed_billing_expired_network_releases_reader(boundary, monkeypatch):
    session = _install(monkeypatch, boundary)
    detail._pin.return_value = replace(boundary.manifest, generation_id=10)
    reply = await _post(_signed())
    assert reply.status == 409 and set(reply.json) == {"error"}
    assert "X-Network-Generation" not in reply.headers
    assert not session.active and session.events == []


@pytest.mark.asyncio
async def test_managed_billing_native_failure_discards_private_payload(boundary, monkeypatch):
    session = _install(monkeypatch, boundary)

    async def failed(*_args, **_kwargs):
        raise ValueError("synthetic private diagnostic")

    monkeypatch.setattr(billing, "search_exact_billing_provider_page", failed)
    reply = await _post(_signed())
    assert reply.status == 503 and set(reply.json) == {"error"}
    assert "synthetic private" not in reply.text and "X-Network-Generation" not in reply.headers
    assert not session.active


@pytest.mark.asyncio
async def test_managed_billing_http_cancellation_releases_reader(boundary, monkeypatch):
    session = _install(monkeypatch, boundary)
    entered = asyncio.Event()

    async def blocked(*_args, **_kwargs):
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(billing, "search_exact_billing_provider_page", blocked)
    task = asyncio.create_task(_post(_signed(), prior=object()))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not session.active and current_network_address_scope() is None
