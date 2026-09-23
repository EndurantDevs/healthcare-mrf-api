# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed HTTP-boundary regressions for imported provider geo pages only."""

from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sanic import Blueprint, Sanic, response
from sqlalchemy import literal, select

from api import custom_import_provider_geo as geo
from api import custom_import_provider_geo_cursor as geo_cursor
from api import custom_import_read_http as transport
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import extension_reads
from process.custom_import.read_contracts import (
    MAX_CURSOR_TTL_SECONDS,
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
    PinnedReadTarget,
)
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadFieldValue
from tests import test_custom_import_read_http as fixtures
from tests.test_custom_import_provider_http import _Session

_ADDRESS_A = "00000000-0000-4000-8000-000000000001"
_ADDRESS_B = "00000000-0000-4000-8000-000000000002"
_ADDRESS_C = "00000000-0000-4000-8000-000000000003"
_PINNED_TARGET = PinnedReadTarget(11, 101, 21, 31, "synthetic_profile")


def _native_query(**changes):
    native_query_by_name = {
        "lat": "40.0",
        "long": "-73.0",
        "radius": "5.0",
        "limit": "3",
        "include_total": "true",
        "view": "card",
    }
    native_query_by_name.update(changes)
    return native_query_by_name


def _body(**changes):
    request_document_by_name = {
        "target": fixtures._TARGET,
        "native_query": _native_query(),
        "context": [{"field_id": "region", "operator": "eq", "value": "north"}],
        "filters": [],
        "order": [{"field_id": "metric", "direction": "desc"}],
        "require_match": False,
    }
    request_document_by_name.update(changes)
    return transport._canonical_json_bytes(request_document_by_name)


def _request(body=None, **changes):
    body = _body() if body is None else body
    request = SimpleNamespace(
        body=body,
        headers=fixtures._provider_headers(body=body, path=geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH),
        method="POST",
        path=geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH,
        query_string="",
    )
    for name, value in changes.items():
        setattr(request, name, value)
    return request


def _provider(npi, address_key):
    return {"npi": npi, "address_key": address_key, "latitude": 40.25}


def _geo_reply(items, *, has_more=False, anchor=None, total_count=None):
    return response.json(
        {
            "items": items,
            "total_count": len(items) + int(has_more) if total_count is None else total_count,
            "next_cursor": None,
            "has_more": has_more,
            "result_identity": ["npi", "address_key"],
            "_custom_import_next_anchor": anchor,
        }
    )


def _install_snapshot(monkeypatch, session):
    fixtures._install_keyring(monkeypatch)

    @asynccontextmanager
    async def bounded(same_session, *, timeout_ms):
        assert same_session is session and timeout_ms > 0
        session.events.append("bounded")
        yield

    async def resolve(same_session, target):
        assert same_session is session
        session.events.append("resolve")
        return await fixtures._resolved_target(same_session, target)

    monkeypatch.setattr(geo, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", resolve)


def _install_geo_service(monkeypatch, session, *, imported_items=None, finality_failure=False):
    """Bind the synthetic import preparation and hydration to one session."""

    _install_snapshot(monkeypatch, session)
    observed_calls_by_name = {"hydration": [], "prepared": []}

    class PreparedGeoReadService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, *, authorization, target, query):
            assert same_session is session and target == _PINNED_TARGET
            assert isinstance(authorization, ExtensionReadAuthorization)
            assert type(query.require_match) is bool
            assert self.authorizer.authorize(authorization, target=target) is not None
            assert query.context_filters[0].value == "north" and query.filters == ()
            session.events.append("prepare")
            prepared = PreparedNpiEntityRelation(
                select(literal("1104212877").label("entity_value"), literal(5).label("sort_0")),
                query.order_terms or (),
                "b" * 64,
                "a" * 64,
            )
            observed_calls_by_name["prepared"].append(prepared)
            return prepared

        async def hydrate_npi_page(self, same_session, **kwargs):
            assert same_session is session
            session.events.append("hydrate")
            observed_calls_by_name["hydration"].append(kwargs)
            return imported_items or {}

    async def finality(same_session, target):
        assert same_session is session and target == _PINNED_TARGET
        session.events.append("finality")
        if finality_failure:
            raise CustomImportReadUnavailableError("synthetic ineligible generation")

    monkeypatch.setattr(geo, "CustomImportReadService", PreparedGeoReadService)
    monkeypatch.setattr(geo, "verify_published_generation", finality)
    return observed_calls_by_name


async def _prepare_geo_cursor(prepare_cursor, session, native_args):
    """Supply the normalized private geo values the native helper would bind."""

    return await prepare_cursor(
        session=session,
        address_table_sql="mrf.entity_address_unified",
        query_parameters={
            "in_lat": float(native_args.get("lat")),
            "in_long": float(native_args.get("long")),
            "radius": float(native_args.get("radius")),
        },
        taxonomy_conditions="1=1",
        extra_clause="",
        ilike_clause="",
        primary_only=True,
        use_taxonomy_filter=False,
        geo_precision_clause="precision-v1",
        geo_type_clause="type-v1",
        limit=int(native_args.get("limit")),
    )


def _cursor_binding(*, scope="a" * 64, native_args=None, trusted_now=100):
    session = object()
    prepared = PreparedNpiEntityRelation(select(literal("1104212877").label("entity_value")), (), "b" * 64, scope)
    context = ProviderImportQuery(prepared, compile_npi_entity_relation(prepared.statement), False)
    binding = geo._GeoCursorBinding(
        session,
        _PINNED_TARGET,
        context,
        geo._parse_geo_native_query(native_args or _native_query()),
        b"synthetic-geo-cursor-key-32-bytes!",
        trusted_now,
    )
    return session, binding


async def _bind_cursor(binding, session, *, geo_precision_clause="precision-v1", geo_type_clause="type-v1"):
    return await binding.prepare(
        session=session,
        address_table_sql="mrf.entity_address_unified",
        query_parameters={"in_lat": 40.0, "in_long": -73.0, "radius": 5.0},
        taxonomy_conditions="1=1",
        extra_clause="",
        ilike_clause="",
        primary_only=True,
        use_taxonomy_filter=False,
        geo_precision_clause=geo_precision_clause,
        geo_type_clause=geo_type_clause,
        limit=3,
    )


def test_geo_native_query_is_closed_bounded_and_forces_exact_total():
    args = geo._parse_geo_native_query(_native_query(limit="50"))
    assert args.get("include_total") == "true"
    assert args.get("limit") == "50"
    for document in (
        _native_query(limit="51"),
        _native_query(include_total="no"),
        _native_query(start="0"),
        _native_query(lat=40.0),
    ):
        with pytest.raises(CustomImportReadRequestError):
            geo._parse_geo_native_query(document)


@pytest.mark.asyncio
async def test_geo_transport_rejects_before_storage(monkeypatch):
    fixtures._install_keyring(monkeypatch)
    request = _request()
    request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "A" * 43

    class NoStorage:
        def in_transaction(self):
            raise AssertionError("invalid transport reached storage")

    reply = await geo.serve_custom_import_provider_geo(request, NoStorage())

    assert reply.status == 404
    assert reply.headers["cache-control"] == "private, no-store"


@pytest.mark.asyncio
async def test_geo_rejects_closed_body_route_and_query_before_storage(monkeypatch):
    fixtures._install_keyring(monkeypatch)
    canonical = _body()
    noncanonical = json.dumps(json.loads(canonical), separators=(", ", ": "), sort_keys=True).encode("ascii")
    requests = (
        _request(_body(extra="unexpected")),
        _request(noncanonical),
        _request(method="GET"),
        _request(path="/api/v1/extensions/custom-import/providers"),
        _request(query_string="native=untrusted"),
    )
    for request in requests:
        session = _Session()
        reply = await geo.serve_custom_import_provider_geo(request, session)
        assert reply.status == 404
        assert session.events == []


@pytest.mark.asyncio
async def test_geo_hydrates_unique_npis_in_one_snapshot_and_issues_private_anchor(monkeypatch):
    session = _Session()
    imported_item = SimpleNamespace(
        root_fields=(ReadFieldValue("metric", "integer", "value", 7),),
        context_fields=(ReadFieldValue("region", "string", "value", "north"),),
    )
    observed_calls_by_name = _install_geo_service(monkeypatch, session, imported_items={"1104212877": imported_item})
    observed_bindings = []

    async def page(request, *, native_args, import_context, prepare_cursor):
        assert request.path == geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH
        assert native_args.get("include_total") == "true"
        assert import_context.prepared is observed_calls_by_name["prepared"][0]
        assert await _prepare_geo_cursor(prepare_cursor, session, native_args) is None
        observed_bindings.append(prepare_cursor.__self__)
        return _geo_reply(
            [
                _provider(1104212877, _ADDRESS_A),
                _provider(1104212877, _ADDRESS_B),
                _provider("1000000000", _ADDRESS_C),
            ],
            has_more=True,
            anchor=["1000000000", _ADDRESS_C],
            total_count=4,
        )

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(), session)

    assert reply.status == 200
    response_document = json.loads(reply.body)
    assert [provider_document["npi"] for provider_document in response_document["items"]] == [
        1104212877,
        1104212877,
        "1000000000",
    ]
    assert response_document["items"][0]["custom_import"]["target"] == fixtures._TARGET
    assert response_document["items"][1]["custom_import"]["root_fields"][0]["value"] == 7
    assert response_document["items"][2]["custom_import"] is None
    assert "_custom_import_next_anchor" not in response_document
    assert isinstance(response_document["next_cursor"], str)
    assert observed_calls_by_name["hydration"][0]["entity_values"] == ("1104212877", "1000000000")
    assert observed_calls_by_name["hydration"][0]["prepared"] is observed_calls_by_name["prepared"][0]
    assert observed_bindings[0].session is session
    assert session.events == ["begin", "snapshot", "bounded", "resolve", "prepare", "hydrate", "finality", "end"]


@pytest.mark.asyncio
async def test_geo_order_only_retains_native_absence(monkeypatch):
    session = _Session()
    _install_geo_service(monkeypatch, session)

    async def page(_request, *, native_args, prepare_cursor, **_kwargs):
        assert await _prepare_geo_cursor(prepare_cursor, session, native_args) is None
        return _geo_reply([_provider(1104212877, _ADDRESS_A)])

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(_body(filters=[])), session)

    assert reply.status == 200
    assert json.loads(reply.body)["items"][0]["custom_import"] is None


@pytest.mark.asyncio
async def test_geo_missing_required_import_match_never_returns_native_row(monkeypatch):
    session = _Session()
    _install_geo_service(monkeypatch, session)

    async def page(_request, *, native_args, prepare_cursor, **_kwargs):
        assert await _prepare_geo_cursor(prepare_cursor, session, native_args) is None
        return _geo_reply([_provider(1104212877, _ADDRESS_A)])

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(_body(order=None, require_match=True)), session)

    assert reply.status == 503 and session.rolled_back
    assert b'"items"' not in reply.body


@pytest.mark.asyncio
async def test_geo_cursor_binds_scope_normalized_values_and_original_ttl():
    session, initial = _cursor_binding()
    assert await _bind_cursor(initial, session) is None
    token = initial.next_cursor(["1000000000", _ADDRESS_C])
    assert token is not None

    continuation_session, continuation = _cursor_binding(native_args=_native_query(cursor=token), trusted_now=150)
    assert await _bind_cursor(continuation, continuation_session) == ("1000000000", _ADDRESS_C)
    renewed = continuation.next_cursor(["1000000001", _ADDRESS_A])
    state = geo_cursor.open_geo_cursor(
        renewed,
        secret=b"synthetic-geo-cursor-key-32-bytes!",
        pinned_target=_PINNED_TARGET,
        query_fingerprint=continuation.query_fingerprint,
        authorization_scope_sha256="a" * 64,
        trusted_now=150,
    )
    assert (state.issued_at, state.expires_at) == (100, 100 + MAX_CURSOR_TTL_SECONDS)

    scope_session, changed_scope = _cursor_binding(scope="c" * 64, native_args=_native_query(cursor=token))
    with pytest.raises(CustomImportReadCursorError):
        await _bind_cursor(changed_scope, scope_session)
    values_session, changed_values = _cursor_binding(native_args=_native_query(cursor=token))
    with pytest.raises(CustomImportReadCursorError):
        await _bind_cursor(changed_values, values_session, geo_precision_clause="precision-v2")
    type_session, changed_type = _cursor_binding(native_args=_native_query(cursor=token))
    with pytest.raises(CustomImportReadCursorError):
        await _bind_cursor(changed_type, type_session, geo_type_clause="type-v2")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "page_factory",
    [
        pytest.param(lambda: response.json({"items": []}), id="malformed-envelope"),
        pytest.param(
            lambda: response.raw(b"x" * (transport._MAX_RESPONSE_BYTES + 1), status=200),
            id="oversized-response",
        ),
    ],
)
async def test_geo_malformed_or_unbounded_native_reply_fails_closed(monkeypatch, page_factory):
    session = _Session()
    _install_geo_service(monkeypatch, session)

    async def page(*_args, **_kwargs):
        return page_factory()

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(), session)

    assert reply.status == 503 and session.rolled_back
    assert len(reply.body) < 256
    assert "hydrate" not in session.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_address_key", "anchor_address_key"),
    [
        pytest.param(_ADDRESS_A, _ADDRESS_B, id="wrong-last-address"),
        pytest.param("not-a-uuid", "not-a-uuid", id="malformed-uuid"),
    ],
)
async def test_geo_rejects_mismatched_or_malformed_private_anchor_as_unavailable(
    monkeypatch, provider_address_key, anchor_address_key
):
    session = _Session()
    _install_geo_service(monkeypatch, session)

    async def page(_request, *, native_args, prepare_cursor, **_kwargs):
        assert await _prepare_geo_cursor(prepare_cursor, session, native_args) is None
        return _geo_reply(
            [_provider(1104212877, provider_address_key)],
            has_more=True,
            anchor=["1104212877", anchor_address_key],
        )

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(), session)

    assert reply.status == 503
    assert reply.headers["cache-control"] == "private, no-store"
    assert "hydrate" not in session.events


@pytest.mark.asyncio
async def test_geo_finality_failure_never_exposes_native_page(monkeypatch):
    session = _Session()
    _install_geo_service(monkeypatch, session, finality_failure=True)

    async def page(_request, *, native_args, prepare_cursor, **_kwargs):
        assert await _prepare_geo_cursor(prepare_cursor, session, native_args) is None
        return _geo_reply([])

    monkeypatch.setattr(geo, "_geo_page", page)
    reply = await geo.serve_custom_import_provider_geo(_request(), session)

    assert reply.status == 503 and session.rolled_back
    assert b'"items"' not in reply.body
    assert session.events[-2:] == ["finality", "end"]


@pytest.mark.asyncio
async def test_geo_external_cancellation_rolls_back_and_propagates(monkeypatch):
    session = _Session()
    _install_geo_service(monkeypatch, session)

    async def cancelled_page(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(geo, "_geo_page", cancelled_page)
    with pytest.raises(asyncio.CancelledError):
        await geo.serve_custom_import_provider_geo(_request(), session)

    assert session.events[-1] == "end"
    assert session.rolled_back
    assert "finality" not in session.events


@pytest.mark.asyncio
async def test_registered_geo_route_forwards_the_bound_session(monkeypatch):
    session = object()
    observed_calls = []

    async def serve(request, candidate_session):
        observed_calls.append((request.method, request.path, candidate_session))
        return response.json({"ok": True})

    monkeypatch.setattr(extension_reads, "serve_custom_import_provider_geo", serve)
    app = Sanic(f"custom-import-provider-geo-route-{uuid.uuid4().hex}")

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = session

    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _request, result = await app.asgi_client.post(geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH, data=_body())

    assert result.status_code == 200
    assert result.json == {"ok": True}
    assert observed_calls == [("POST", geo.CUSTOM_IMPORT_PROVIDER_GEO_PATH, session)]
