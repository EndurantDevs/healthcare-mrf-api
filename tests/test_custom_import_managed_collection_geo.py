# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual signed Geo transport binds managed policy and retained cursor identity."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic, response

from api import custom_import_provider_collection as managed
from api import custom_import_provider_geo as geo
from api import custom_import_provider_geo_cursor as cursor
from api import custom_import_read_http as transport
from api import provider_detail_network_read as detail
from api.endpoint import extension_reads
from api.network_address_scope import current_network_address_scope
from api.provider_geo_sql import native_geo_entity_scope
from process.custom_import.read_contracts import MAX_CURSOR_TTL_SECONDS
from process.custom_import.read_core import ReadFieldValue
from process.custom_import.read_cursor import _base64url_decode
from tests import test_custom_import_provider_geo_http as fixtures
from tests import test_custom_import_read_http as permits
from tests.test_custom_import_provider_collection import import_boundary as import_boundary
from tests.test_custom_import_provider_http import _Session
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import boundary as boundary


def _document(*, native_query=None, include_filter=True, generation=None):
    document = json.loads(fixtures._body(include_filter=include_filter))
    if native_query is not None:
        document["native_query"] = native_query
    query_by_field = {"kind": "geo", "native_query": document["native_query"]}
    if generation is not None:
        query_by_field["network_generation"] = str(generation)
    document["managed_collection"] = v2_envelope(query_by_field)
    return document


def _headers(body):
    return permits._headers(
        body=body,
        path=geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH,
        contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=transport.managed_provider_body_sha256,
        signature_message=transport.custom_import_provider_signature_message,
    )


async def _post(document, *, headers=None, prior=None, query_string=""):
    body = document if type(document) is bytes else transport._canonical_json_bytes(document)
    app = Sanic("managed_geo_" + uuid4().hex)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    observed_requests = []

    @app.middleware("request")
    async def bind(request):
        observed_requests.append(request)
        if prior is not None:
            request.ctx.sa_session = prior

    _, reply = await app.asgi_client.post(
        geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH + query_string, data=body, headers=headers or _headers(body)
    )
    request = observed_requests[0]
    assert getattr(request.ctx, "sa_session", None) is prior
    assert hasattr(request.ctx, "sa_session") is (prior is not None)
    assert current_network_address_scope() is None
    return reply


@pytest.fixture
def geo_boundary(import_boundary, monkeypatch):
    held = import_boundary
    held.manifest = replace(held.manifest, generation_id=17)
    detail._pin.return_value = held.manifest
    imported = SimpleNamespace(
        root_fields=(ReadFieldValue("score", "integer", "value", 7),), context_fields=(), children=()
    )
    held.observed = fixtures._install_geo_service(monkeypatch, held.session, imported_items={"1104212877": imported})
    held.bindings, held.anchors, held.requests = [], [], []

    async def page(request, *, native_args, import_context, prepare_cursor, native_scope_preparer):
        assert request.ctx.sa_session is held.session
        scope = current_network_address_scope()
        assert scope is not None and scope.manifest.generation_id in {17, 18}
        assert held.state.active and held.state.catalog_active
        assert native_args.get("include_total") == "true"
        preparer = native_scope_preparer.__self__

        async def scope_import(session, **options):
            assert session is held.session and request.ctx.sa_session is session
            assert options["prepared"] is import_context.prepared
            assert "native_eligible_geo" in str(options["native_entity_scope"])
            held.session.events.append("native_scope")
            return options["prepared"]

        monkeypatch.setattr(preparer.service, "scope_npi_entity_relation", scope_import, raising=False)
        scoped = await native_scope_preparer(native_geo_entity_scope())
        assert scoped.prepared is import_context.prepared
        held.requests.append(request)
        held.anchors.append(await fixtures._prepare_geo_cursor(prepare_cursor, held.session, native_args))
        held.bindings.append(prepare_cursor.__self__)
        provider_rows = [
            fixtures._provider(1104212877, fixtures._ADDRESS_A),
            fixtures._provider(1104212877, fixtures._ADDRESS_B),
        ]
        return fixtures._geo_reply(
            provider_rows, has_more=True, anchor=["1104212877", fixtures._ADDRESS_B], total_count=5
        )

    monkeypatch.setattr(geo, "_geo_page", page)
    return held


def _cursor_document(token):
    return json.loads(_base64url_decode(token)[:-32])


def _open(token, binding):
    return cursor.open_geo_cursor(
        token,
        secret=binding.secret,
        pinned_target=binding.pinned_target,
        query_fingerprint=binding.query_fingerprint,
        authorization_scope_sha256=binding.import_context.prepared.authorization_scope_sha256,
        trusted_now=binding.trusted_now,
        network_generation=binding.network_generation,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("has_prior", [False, True])
async def test_actual_signed_geo_has_one_reader_pin_and_exact_family(geo_boundary, include_filter, has_prior):
    document = _document(include_filter=include_filter)
    document["managed_collection"] = v2_envelope(
        document["managed_collection"]["query"], excluded_network_ids=list(range(1, 5001))
    )
    body = transport._canonical_json_bytes(document)
    assert transport._MAX_BODY_BYTES < len(body) < managed.MAX_MANAGED_BODY_BYTES
    reply = await _post(body, prior=object() if has_prior else None)
    assert reply.status == 200, reply.json
    assert reply.json["total_count"] == 5 and reply.json["has_more"] is True
    assert reply.json["result_identity"] == ["npi", "address_key"]
    assert set(reply.json) == {
        "items",
        "total_count",
        "has_more",
        "next_cursor",
        "result_identity",
        "managed_collection",
    }
    assert reply.json["managed_collection"] == {
        "client_id": document["managed_collection"]["client_id"],
        "policy_revision": document["managed_collection"]["policy_revision"],
        "generation": "17",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
    }
    assert reply.headers.get_list("X-Network-Generation") == ["17"]
    assert reply.headers.get_list("X-Network-Access-Scope") == [document["managed_collection"]["access_scope_sha256"]]
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert all(("custom_import" in provider) is include_filter for provider in reply.json["items"])
    hydration = geo_boundary.observed["hydration"]
    assert len(hydration) == int(include_filter)
    if include_filter:
        assert hydration[0]["entity_values"] == ("1104212877",)
        assert hydration[0]["prepared"] is geo_boundary.observed["prepared"][0]
        assert reply.json["items"][0]["custom_import"] == reply.json["items"][1]["custom_import"]
        assert reply.json["items"][0]["custom_import"]["root_fields"][0]["value"] == 7
    assert geo_boundary.session.events == ["resolve", "prepare", "native_scope"] + (
        ["hydrate"] if include_filter else []
    ) + ["finality"]
    detail._pin.assert_awaited_once_with(geo_boundary.driver, None, "sample_registry")
    assert len(geo_boundary.state.snapshots) == 1 and not geo_boundary.state.active
    assert _cursor_document(reply.json["next_cursor"])["version"] == 2
    assert _open(reply.json["next_cursor"], geo_boundary.bindings[0]).network_generation == 17


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "signature",
        "path",
        "digest",
        "raw_query",
        "kind",
        "policy",
        "policy_signature",
        "native_signature",
        "query_string",
    ],
)
async def test_bad_signed_geo_never_opens_reader(geo_boundary, failure):
    document = _document()
    signed = _headers(transport._canonical_json_bytes(document))
    query_string = ""
    match failure:
        case "signature":
            document["include_filter"] = False
        case "path":
            signed = permits._headers(
                body=transport._canonical_json_bytes(document),
                path="/api/v1/extensions/custom-import/providers",
                contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
                body_sha256=transport.managed_provider_body_sha256,
                signature_message=transport.custom_import_provider_signature_message,
            )
        case "digest":
            document["managed_collection"]["access_scope_sha256"] = "0" * 64
        case "raw_query":
            document["managed_collection"]["query"]["native_query"] = fixtures._native_query(radius="6")
        case "kind":
            document["managed_collection"]["query"]["kind"] = "list"
        case "policy":
            document["managed_collection"].pop("excluded_company_ids")
        case "policy_signature":
            document["managed_collection"] = v2_envelope(
                document["managed_collection"]["query"], excluded_network_ids=[7]
            )
        case "native_signature":
            document = _document(native_query=fixtures._native_query(radius="6"))
        case _:
            query_string = "?lat=0"
    reply = await _post(document, headers=signed, query_string=query_string)
    expected_status = 400 if failure in {"digest", "raw_query", "kind", "policy"} else 404
    assert reply.status == expected_status and set(reply.json) == {"error"}
    assert geo_boundary.state.snapshots == [] and geo_boundary.observed["prepared"] == []
    detail._pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_continuation_retains_generation_and_original_expiry(geo_boundary, monkeypatch):
    first = await _post(_document())
    assert first.status == 200
    first_state = _open(first.json["next_cursor"], geo_boundary.bindings[0])
    current_head = replace(geo_boundary.manifest, generation_id=18)

    async def pin(driver, generation, schema):
        assert driver is geo_boundary.driver and schema == "sample_registry"
        return geo_boundary.manifest if generation == 17 else current_head

    detail._pin.side_effect = pin
    later = transport._canonical_utc(permits._NOW)[1] + timedelta(seconds=15)
    monkeypatch.setattr(transport, "_trusted_now", lambda: later.strftime("%Y-%m-%dT%H:%M:%SZ"))
    reply = await _post(_document(native_query=fixtures._native_query(cursor=first.json["next_cursor"]), generation=17))
    assert reply.status == 200, reply.json
    continued = _open(reply.json["next_cursor"], geo_boundary.bindings[-1])
    assert (continued.issued_at, continued.expires_at) == (first_state.issued_at, first_state.expires_at)
    assert continued.expires_at == first_state.issued_at + MAX_CURSOR_TTL_SECONDS
    assert continued.network_generation == 17 and reply.headers["X-Network-Generation"] == "17"
    assert geo_boundary.anchors == [None, ("1104212877", fixtures._ADDRESS_B)]
    assert [call.args[1] for call in detail._pin.await_args_list] == [None, 17]
    assert len(geo_boundary.state.snapshots) == 2 and not geo_boundary.state.active


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["policy", "manifest", "approval", "query", "generation", "legacy"])
async def test_changed_bound_geo_cursor_refuses_without_partial_page(geo_boundary, change):
    first = await _post(_document())
    assert first.status == 200
    token = first.json["next_cursor"]
    descriptor = fixtures._native_query(cursor=token)
    document = _document(native_query=descriptor, generation=17)
    match change:
        case "policy":
            document["managed_collection"] = v2_envelope(
                document["managed_collection"]["query"], excluded_network_ids=[7]
            )
        case "manifest":
            detail._pin.return_value = replace(geo_boundary.manifest, manifest_sha256="c" * 64)
        case "approval":
            detail._pin.return_value = replace(geo_boundary.manifest, approved_custom_revision=5)
        case "query":
            document = _document(native_query=descriptor | {"radius": "6.0"}, generation=17)
        case "generation":
            detail._pin.return_value = replace(geo_boundary.manifest, generation_id=18)
            document = _document(native_query=descriptor, generation=18)
        case "legacy":
            state = replace(_open(token, geo_boundary.bindings[0]), network_generation=None)
            legacy = cursor.issue_geo_cursor(state, secret=geo_boundary.bindings[0].secret)
            document = _document(native_query=fixtures._native_query(cursor=legacy), generation=17)
    reply = await _post(document)
    assert reply.status == 409 and set(reply.json) == {"error"}
    assert len(geo_boundary.observed["hydration"]) == 1
    assert geo_boundary.session.events.count("finality") == 1
    assert not geo_boundary.state.active and not geo_boundary.state.catalog_active


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 403, 404, 409, 503])
async def test_managed_geo_keeps_sanitized_native_status(geo_boundary, monkeypatch, status):
    async def denied(*args, **options):
        return response.raw(b"private native diagnostic", status=status)

    monkeypatch.setattr(geo, "_geo_page", denied)
    reply = await _post(_document(), prior=object())
    assert reply.status == status and set(reply.json) == {"error"}
    assert b"private native diagnostic" not in reply.body
    assert len(reply.body) < transport._MAX_RESPONSE_BYTES
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert geo_boundary.observed["hydration"] == [] and not geo_boundary.state.active


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", ["collection", "family"])
async def test_managed_geo_checks_entire_presented_response_bound(geo_boundary, monkeypatch, bound):
    if bound == "collection":
        monkeypatch.setattr(managed, "MAX_COLLECTION_BYTES", 300)
    else:
        monkeypatch.setattr(geo, "_provider_response_limit", lambda *args: 300)
    reply = await _post(_document())
    assert reply.status == 503 and set(reply.json) == {"error"}
    assert len(geo_boundary.observed["hydration"]) == 1
    assert not geo_boundary.state.active and not geo_boundary.state.catalog_active


@pytest.mark.asyncio
async def test_managed_geo_cancellation_propagates_and_restores_request(geo_boundary, monkeypatch):
    async def cancelled(*args, **options):
        assert current_network_address_scope().manifest is geo_boundary.manifest
        raise asyncio.CancelledError()

    monkeypatch.setattr(geo, "_geo_page", cancelled)
    body = transport._canonical_json_bytes(_document())
    prior = object()
    request = fixtures._request(body, headers=_headers(body), ctx=SimpleNamespace(sa_session=prior))
    with pytest.raises(asyncio.CancelledError):
        await geo.serve_custom_import_provider_geo(request, None)
    assert request.ctx.sa_session is prior and current_network_address_scope() is None
    assert geo_boundary.observed["hydration"] == [] and not geo_boundary.state.active
    detail._pin.assert_awaited_once()


@pytest.mark.asyncio
async def test_unscoped_geo_endpoint_keeps_legacy_cursor_continuation(monkeypatch):
    session = _Session()
    fixtures._install_geo_service(monkeypatch, session)
    bindings = []

    async def page(request, *, native_args, prepare_cursor, **options):
        assert current_network_address_scope() is None
        await fixtures._prepare_geo_cursor(prepare_cursor, session, native_args)
        bindings.append(prepare_cursor.__self__)
        return fixtures._geo_reply(
            [fixtures._provider(1104212877, fixtures._ADDRESS_A)],
            has_more=True,
            anchor=["1104212877", fixtures._ADDRESS_A],
        )

    monkeypatch.setattr(geo, "_geo_page", page)
    first = await _post(fixtures._body(), prior=session)
    assert first.status == 200 and "managed_collection" not in first.json
    assert "X-Network-Generation" not in first.headers and "X-Network-Access-Scope" not in first.headers
    assert _cursor_document(first.json["next_cursor"])["version"] == 1
    second = await _post(
        fixtures._body(native_query=fixtures._native_query(cursor=first.json["next_cursor"])), prior=session
    )
    assert second.status == 200
    assert _cursor_document(second.json["next_cursor"])["version"] == 1
    assert (
        _open(first.json["next_cursor"], bindings[0]).expires_at
        == _open(second.json["next_cursor"], bindings[1]).expires_at
    )


@pytest.mark.asyncio
async def test_geo_body_over_managed_limit_stops_before_reader(geo_boundary):
    body = transport._canonical_json_bytes(_document())
    reply = await _post(body + b" " * managed.MAX_MANAGED_BODY_BYTES, headers=_headers(body))
    assert reply.status == 404 and set(reply.json) == {"error"}
    assert geo_boundary.state.snapshots == [] and geo_boundary.observed["prepared"] == []
    detail._pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_geo_missing_retained_generation_stops_before_import_preparation(geo_boundary):
    first = await _post(_document())
    assert first.status == 200
    detail._pin.return_value = replace(geo_boundary.manifest, generation_id=18)
    reply = await _post(_document(native_query=fixtures._native_query(cursor=first.json["next_cursor"]), generation=17))
    assert reply.status == 409 and set(reply.json) == {"error"}
    assert len(geo_boundary.observed["prepared"]) == len(geo_boundary.observed["hydration"]) == 1
    assert not geo_boundary.state.active and not geo_boundary.state.catalog_active
