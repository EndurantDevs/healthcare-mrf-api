# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed collection policy cannot shadow native requests or escape one Reader."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import SanicException

from api import custom_import_provider_batch as batch
from api import custom_import_provider_collection as managed
from api import custom_import_provider_http as providers
from api import custom_import_read_http as transport
from api import provider_batch
from api import provider_collection_network_read as collection
from api import provider_detail_network_read as detail
from api.network_address_scope import current_network_address_scope
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests import test_custom_import_provider_batch as batch_fixtures
from tests import test_custom_import_provider_http as list_fixtures
from tests import test_custom_import_read_http as permits
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import boundary as boundary


def _body(kind="list", **changes):
    document = json.loads(batch_fixtures._body() if kind == "batch" else list_fixtures._body())
    query_by_field = {"kind": kind, "native_query": document["native_query"]}
    if kind == "batch":
        query_by_field["native_batch"] = document["native_batch"]
    document["managed_collection"] = v2_envelope(query_by_field)
    return transport._canonical_json_bytes(document | changes)


def _parse(body, kind="list"):
    return (
        batch._parse_batch_request(body)
        if kind == "batch"
        else providers._parse_provider_request(body, allow_managed_collection=True, allow_network_binding=True)
    )


def _request(body, kind="list"):
    path = batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH if kind == "batch" else providers.CUSTOM_IMPORT_PROVIDERS_PATH
    return SimpleNamespace(
        body=body,
        headers=permits._headers(
            body=body,
            path=path,
            contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
            body_sha256=transport.managed_provider_body_sha256,
            signature_message=transport.custom_import_provider_signature_message,
        ),
        method="POST",
        path=path,
        query_string="",
        ctx=SimpleNamespace(),
    )


@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("field", list(v2_envelope()))
def test_each_policy_field_is_required(kind, field):
    document = json.loads(_body(kind))
    del document["managed_collection"][field]
    with pytest.raises(CustomImportReadRequestError):
        _parse(transport._canonical_json_bytes(document), kind)


@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("failure", ["kind", "query", "digest", "extra", "binding", "detail", "legacy"])
def test_closed_policy_exact_native_and_mode_are_required(kind, failure):
    document = json.loads(_body(kind))
    envelope = document["managed_collection"]
    match failure:
        case "kind":
            envelope["query"]["kind"] = "geo"
        case "query":
            envelope["query"]["native_query"]["state"] = "CA"
        case "digest":
            envelope["access_scope_sha256"] = "0" * 64
        case "extra":
            envelope["source"] = "untrusted"
        case "legacy":
            envelope.pop("contract_version")
        case _:
            document["network_binding" if failure == "binding" else "managed_detail"] = None
    with pytest.raises((CustomImportReadRequestError, transport.CustomImportReadTransportError)):
        _parse(transport._canonical_json_bytes(document), kind)


@pytest.mark.parametrize("field", ["address_limit", "address_offset", "include_sources", "include_evidence", "npis"])
def test_batch_shadowing_is_rejected(field):
    document = json.loads(_body("batch"))
    descriptor = document["managed_collection"]["query"]["native_batch"]
    descriptor[field] = {
        "address_limit": 6,
        "address_offset": 1,
        "include_sources": True,
        "include_evidence": True,
        "npis": ["9000000002"],
    }[field]
    with pytest.raises(CustomImportReadRequestError):
        _parse(transport._canonical_json_bytes(document), "batch")


def test_padded_batch_identity_does_not_broaden_legacy_semantics():
    document = json.loads(_body("batch"))
    for descriptor in (document["native_batch"], document["managed_collection"]["query"]["native_batch"]):
        descriptor["npis"] = ["0000000049"]
    with pytest.raises(CustomImportReadRequestError, match="not canonical"):
        _parse(transport._canonical_json_bytes(document), "batch")


@pytest.fixture
def import_boundary(boundary, monkeypatch):
    boundary.session.events = []
    monkeypatch.setattr(managed, "provider_profile_read_snapshot", collection.provider_profile_read_snapshot)

    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        assert session is boundary.session and timeout_ms > 0
        yield

    monkeypatch.setattr(managed, "_bounded_read_window", bounded)
    return boundary


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("has_prior", [False, True])
async def test_signed_collection_preserves_family_and_one_pin(
    import_boundary, monkeypatch, kind, include_filter, has_prior
):
    boundary = import_boundary
    if kind == "list":
        imported = SimpleNamespace(root_fields=(), context_fields=(), children=())
        list_fixtures._install(
            monkeypatch, boundary.session, page_rows=[{"npi": 1104212877}], imported_items={"1104212877": imported}
        )
        native = providers._provider_page
        owner, name = providers, "_provider_page"
    else:
        batch_fixtures._install(monkeypatch, boundary.session)
        native = provider_batch.read_native_batch
        owner, name = provider_batch, "read_native_batch"

    async def scoped(request, *args, **options):
        assert request.ctx.sa_session is boundary.session
        assert current_network_address_scope().manifest is boundary.manifest
        return await native(request, *args, **options)

    monkeypatch.setattr(owner, name, scoped)
    document = json.loads(_body(kind, include_filter=include_filter))
    document["managed_collection"] = v2_envelope(
        document["managed_collection"]["query"], excluded_network_ids=list(range(1, 5001))
    )
    body = transport._canonical_json_bytes(document)
    assert transport._MAX_BODY_BYTES < len(body) < managed.MAX_MANAGED_BODY_BYTES
    request = _request(body, kind)
    prior = object()
    if has_prior:
        request.ctx.sa_session = prior
    serve = batch.serve_custom_import_provider_batch if kind == "batch" else providers.serve_custom_import_providers
    reply = await serve(request, None)
    assert reply.status == 200, reply.body
    provider_payload = json.loads(reply.body)
    assert provider_payload["managed_collection"] == {
        "client_id": document["managed_collection"]["client_id"],
        "policy_revision": document["managed_collection"]["policy_revision"],
        "generation": "9",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
    }
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == document["managed_collection"]["access_scope_sha256"]
    provider_rows = (
        provider_payload["rows"]
        if kind == "list"
        else [provider_item["provider"] for provider_item in provider_payload["items"]]
    )
    assert all(("custom_import" in provider_row) is include_filter for provider_row in provider_rows)
    assert provider_payload["total"] == 1 if kind == "list" else provider_payload["pagination"]["total"] == 2
    detail._pin.assert_awaited_once()
    assert len(boundary.state.snapshots) == 1 and not boundary.state.active
    assert getattr(request.ctx, "sa_session", None) is (prior if has_prior else None)
    assert hasattr(request.ctx, "sa_session") is has_prior
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("failure", ["signature", "path", "expired", "target", "query", "too_large", "duplicate"])
async def test_signed_refusal_happens_before_reader(import_boundary, monkeypatch, kind, failure):
    permits._install_keyring(monkeypatch)
    body = _body(kind)
    request = _request(body, kind)
    match failure:
        case "signature":
            request.body = _body(kind, include_filter=not json.loads(body)["include_filter"])
        case "path":
            request.headers = _request(body, "batch" if kind == "list" else "list").headers
        case "expired" | "target":
            request.headers = permits._resigned_headers(
                body=body,
                path=request.path,
                contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
                body_sha256=transport.managed_provider_body_sha256,
                signature_message=transport.custom_import_provider_signature_message,
                **(
                    {"expires_at": permits._NOW}
                    if failure == "expired"
                    else {"target": permits._TARGET | {"generation_id": 102}}
                ),
            )
        case "query":
            request.query_string = "state=CA"
        case "duplicate":
            request.body = body[:-1] + b',"require_match":true}'
        case _:
            request.body = body + b" " * managed.MAX_MANAGED_BODY_BYTES
    serve = batch.serve_custom_import_provider_batch if kind == "batch" else providers.serve_custom_import_providers
    reply = await serve(request, None)
    assert reply.status in {400, 404}
    assert set(json.loads(reply.body)) == {"error"}
    assert not import_boundary.state.snapshots
    detail._pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("status", [400, 403, 404, 409, 503])
async def test_native_failure_status_is_sanitized_and_releases_reader(import_boundary, monkeypatch, kind, status):
    permits._install_keyring(monkeypatch)
    owner, name = (batch, "_read_batch_payload") if kind == "batch" else (providers, "_read_provider_payload")
    monkeypatch.setattr(owner, name, AsyncMock(side_effect=SanicException("private diagnostic", status_code=status)))
    request = _request(_body(kind), kind)
    serve = batch.serve_custom_import_provider_batch if kind == "batch" else providers.serve_custom_import_providers
    reply = await serve(request, None)
    assert reply.status == status and b"private diagnostic" not in reply.body
    assert set(json.loads(reply.body)) == {"error"} and len(reply.body) <= transport._MAX_RESPONSE_BYTES
    assert not hasattr(request.ctx, "sa_session") and current_network_address_scope() is None
    assert not import_boundary.state.active


@pytest.mark.parametrize("bound", ["collection", "family"])
def test_combined_response_keeps_both_bounds(monkeypatch, bound):
    parsed = _parse(_body())
    manifest = SimpleNamespace(generation_id=9, approved_custom_revision=4, manifest_sha256="b" * 64)
    monkeypatch.setattr(managed, "MAX_COLLECTION_BYTES", 600 if bound == "collection" else 2000)
    with pytest.raises(CustomImportReadUnavailableError, match="response is unavailable"):
        managed.managed_collection_response(
            {"rows": [{"value": "x" * 1000}]},
            parsed.managed_collection,
            manifest,
            maximum_bytes=600 if bound == "family" else 2000,
        )


@pytest.mark.asyncio
async def test_cancellation_restores_binding_and_releases_pin(import_boundary):
    parsed = _parse(_body())
    request = _request(_body())
    prior = object()
    request.ctx.sa_session = prior
    with pytest.raises(asyncio.CancelledError):
        async with managed.managed_import_collection_scope(request, parsed.managed_collection):
            raise asyncio.CancelledError()
    assert request.ctx.sa_session is prior and current_network_address_scope() is None
    assert not import_boundary.state.active


@pytest.mark.parametrize(
    "error,status",
    [
        ("registry_network_catalog_request_invalid", 400),
        ("registry_network_catalog_detail_denied", 404),
        ("registry_network_catalog_source_changed", 409),
        ("registry_network_catalog_unavailable", 503),
    ],
)
def test_catalog_failures_are_closed(error, status):
    reply = managed.managed_collection_error(RegistryNetworkCatalogError(error))
    assert reply.status == status and set(json.loads(reply.body)) == {"error"}


@pytest.mark.parametrize("kind", ["list", "batch"])
@pytest.mark.parametrize("flag", [None, 1, "true"])
def test_managed_transport_requires_explicit_boolean_mode(kind, flag):
    body = _body(kind)
    request = _request(body, kind)
    with pytest.raises(transport.CustomImportReadTransportError):
        transport._verify_provider_transport(
            headers=request.headers,
            body=body,
            request=_parse(body, kind),
            trusted_now=permits._NOW,
            keyring=transport._load_keyring(permits._keyring_document()),
            path=request.path,
            managed_collection=flag,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["list", "batch"])
async def test_denied_selector_precedes_native_reads(import_boundary, monkeypatch, kind):
    permits._install_keyring(monkeypatch)
    document = json.loads(_body(kind))
    document["managed_collection"]["query"]["selector"] = {"namespace": "canonical", "values": [84]}
    request = _request(transport._canonical_json_bytes(document), kind)
    import_boundary.driver.fetchval.return_value = 0
    owner, name = (batch, "_read_batch_payload") if kind == "batch" else (providers, "_read_provider_payload")
    reader = AsyncMock()
    monkeypatch.setattr(owner, name, reader)
    serve = batch.serve_custom_import_provider_batch if kind == "batch" else providers.serve_custom_import_providers
    reply = await serve(request, None)
    assert reply.status == 404 and set(json.loads(reply.body)) == {"error"}
    reader.assert_not_awaited()
    assert not hasattr(request.ctx, "sa_session") and current_network_address_scope() is None
    assert not import_boundary.state.active
