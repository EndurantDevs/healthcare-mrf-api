# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider composition cannot bypass scope, snapshot, or exact totals."""

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sanic import Blueprint, Sanic, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import literal, select

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api.endpoint import extension_reads
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadFieldValue
from tests import test_custom_import_read_http as fixtures


def _body(**changes):
    body_map = {
        "target": fixtures._TARGET,
        "native_query": {"name_like": ["Synthetic", "Example"]},
        "context": [{"field_id": "region", "operator": "eq", "value": "north"}],
        "filters": [],
        "order": [{"field_id": "metric", "direction": "desc"}],
        "require_match": False,
    }
    body_map.update(changes)
    return transport._canonical_json_bytes(body_map)


def _request(body=None, **changes):
    body = _body() if body is None else body
    request = SimpleNamespace(
        body=body,
        headers=fixtures._provider_headers(body=body, path=provider_http.CUSTOM_IMPORT_PROVIDERS_PATH),
        method="POST",
        path=provider_http.CUSTOM_IMPORT_PROVIDERS_PATH,
        query_string="",
    )
    for name, value in changes.items():
        setattr(request, name, value)
    return request


class _Session:
    def __init__(self, *, already_active=False):
        self.events = []
        self.already_active = already_active
        self.rolled_back = False

    def in_transaction(self):
        return self.already_active

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        try:
            yield self
        except BaseException:
            self.rolled_back = True
            raise
        finally:
            self.events.append("end")

    async def execute(self, statement):
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        self.events.append("snapshot")


def _install_window(monkeypatch, session):
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

    monkeypatch.setattr(provider_http, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", resolve)


def _install(
    monkeypatch,
    session,
    *,
    page_failure=None,
    finality_failure=False,
    page_rows=None,
    imported_items=None,
    require_match=False,
):
    """Bind synthetic native results and import hydration to one session."""

    _install_window(monkeypatch, session)
    expected_require_match = require_match

    class PreparedProviderReadService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, *, authorization, target, query):
            assert same_session is session
            assert self.authorizer.authorize(authorization, target=target).value == "a" * 64
            assert query.context_filters[0].value == "north" and query.filters == ()
            assert query.require_match is expected_require_match
            session.events.append("prepare")
            return PreparedNpiEntityRelation(
                select(literal("1104212877").label("entity_value"), literal(5).label("sort_0")),
                query.order_terms or (),
                "b" * 64,
                "c" * 64,
            )

        async def hydrate_npi_page(self, same_session, **kwargs):
            assert same_session is session
            assert kwargs["entity_values"] == tuple(str(provider["npi"]) for provider in page_rows or [])
            session.events.append("hydrate")
            return imported_items or {}

    async def page(request, *, native_args, import_context):
        assert request.path == provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
        assert native_args.getlist("name_like") == ["Synthetic", "Example"]
        assert native_args.get("include_total") == "true"
        assert import_context.require_match is require_match
        assert import_context.prepared.normalized_order_terms[0].direction == "desc"
        session.events.append("page")
        if page_failure is not None:
            raise page_failure
        return response.json({"rows": page_rows or [], "total": len(page_rows or [])})

    async def finality(same_session, target):
        assert same_session is session and target.dataset_id == 11
        session.events.append("finality")
        if finality_failure:
            raise CustomImportReadUnavailableError("synthetic ineligible generation")

    monkeypatch.setattr(provider_http, "CustomImportReadService", PreparedProviderReadService)
    monkeypatch.setattr(provider_http, "_provider_page", page)
    monkeypatch.setattr(provider_http, "verify_published_generation", finality)


def test_context_selection_does_not_require_imported_membership():
    parsed = provider_http._parse_provider_request(_body())
    assert parsed.context[0].field_id == "region" and parsed.filters == ()
    assert parsed.require_match is False
    assert parsed.native_args.getlist("name_like") == ["Synthetic", "Example"]
    assert parsed.native_args.get("include_total") == "true"
    filtered = provider_http._parse_provider_request(_body(order=None, require_match=True))
    assert filtered.order_terms is None and filtered.require_match is True


def test_provider_v1_body_is_not_a_provider_v2_body():
    body_document = json.loads(_body())
    del body_document["context"]

    with pytest.raises(transport.CustomImportReadTransportError):
        provider_http._parse_provider_request(transport._canonical_json_bytes(body_document))


@pytest.mark.parametrize(
    "changes",
    [
        {"require_match": 0},
        {"order": None},
        {"native_query": {"include_total": "false"}},
        {"native_query": {"view": "sitemap"}},
        {"native_query": {"count_only": "1"}},
        {"native_query": {"format": "classification"}},
        {"native_query": {"source_url": "https://example.invalid"}},
        {"native_query": {"q": ["one", "two"]}},
        {"native_query": {"name_like": []}},
        {"native_query": {"name_like": ["x"] * 17}},
        {"native_query": {"q": "x" * 2049}},
        {"native_query": {"page": 1}},
        {"filters": [{"field_id": "metric", "operator": "eq", "value": "5"}] * 3},
        {"context": [{"field_id": "region", "operator": "neq", "value": "north"}]},
        {"filters": [{"field_id": "metric", "operator": "gte", "value": "5"}]},
        {"filters": [{"field_id": "metric", "operator": "eq", "value": None}]},
    ],
)
def test_provider_page_shape_rejects_ambiguous_or_nonpage_inputs(changes):
    with pytest.raises(CustomImportReadRequestError):
        provider_http._parse_provider_request(_body(**changes))


@pytest.mark.asyncio
@pytest.mark.parametrize("page_rows", [[{"npi": "bad"}], [{"npi": True}], [{"npi": 1104212877}] * 2])
async def test_malformed_native_page_is_a_service_failure(monkeypatch, page_rows):
    session = _Session()
    _install(monkeypatch, session, page_rows=page_rows)
    reply = await provider_http.serve_custom_import_providers(_request(), session)
    assert reply.status == 503 and session.rolled_back
    assert "hydrate" not in session.events


@pytest.mark.asyncio
async def test_signed_provider_page_uses_one_snapshot_and_checks_empty_result_finality(monkeypatch):
    session = _Session()
    _install(monkeypatch, session)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == 200
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events == [
        "begin",
        "snapshot",
        "bounded",
        "resolve",
        "prepare",
        "page",
        "hydrate",
        "finality",
        "end",
    ]


@pytest.mark.asyncio
async def test_provider_hydration_preserves_order_native_values_and_absence(monkeypatch):
    session = _Session()
    imported_item = SimpleNamespace(
        root_fields=(ReadFieldValue("metric", "integer", "value", 7),),
        context_fields=(ReadFieldValue("context_metric", "decimal", "null", None),),
    )
    _install(
        monkeypatch,
        session,
        page_rows=[{"npi": 1104212877, "latitude": 40.25}, {"npi": "1000000000"}],
        imported_items={"1104212877": imported_item},
    )
    reply = await provider_http.serve_custom_import_providers(_request(), session)
    assert reply.status == 200
    payload = json.loads(reply.body)
    assert [provider["npi"] for provider in payload["rows"]] == [1104212877, "1000000000"]
    assert payload["rows"][0]["latitude"] == 40.25
    assert payload["rows"][0]["custom_import"] == {
        "target": fixtures._TARGET,
        **transport._search_item_payload(imported_item),
    }
    assert payload["rows"][1]["custom_import"] is None
    assert session.events.index("hydrate") < session.events.index("finality")


@pytest.mark.asyncio
async def test_missing_required_imported_match_cannot_return_native_provider(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, page_rows=[{"npi": 1104212877}], require_match=True)
    reply = await provider_http.serve_custom_import_providers(_request(_body(require_match=True)), session)
    assert reply.status == 503 and session.rolled_back
    assert b'"rows"' not in reply.body


@pytest.mark.asyncio
async def test_provider_response_limit_applies_after_imported_field_hydration(monkeypatch):
    session = _Session()
    oversized = SimpleNamespace(
        root_fields=(ReadFieldValue("text", "string", "value", "x" * transport._MAX_RESPONSE_BYTES),), context_fields=()
    )
    _install(monkeypatch, session, page_rows=[{"npi": 1104212877}], imported_items={"1104212877": oversized})
    reply = await provider_http.serve_custom_import_providers(_request(), session)
    assert reply.status == 503 and session.rolled_back
    assert len(reply.body) < 256


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["signature", "body", "path", "query", "target", "expired"])
async def test_provider_permit_failures_do_not_touch_database(monkeypatch, failure):
    fixtures._install_keyring(monkeypatch)
    request = _request()
    match failure:
        case "signature":
            request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "A" * 43
        case "body":
            request.body = _body(require_match=True)
        case "path":
            request.headers = fixtures._provider_headers(body=request.body, path=transport.CUSTOM_IMPORT_READ_PATH)
        case "query":
            request.query_string = "npi=1104212877"
        case "target":
            request.headers = fixtures._resigned_provider_headers(
                body=request.body, path=request.path, target={**fixtures._TARGET, "generation_id": 102}
            )
        case "expired":
            monkeypatch.setattr(transport, "_trusted_now", lambda: "2031-01-02T03:05:05Z")
    session = _Session()

    reply = await provider_http.serve_custom_import_providers(request, session)

    assert reply.status == 404
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events == []


@pytest.mark.asyncio
async def test_provider_read_never_resets_an_existing_transaction(monkeypatch):
    fixtures._install_keyring(monkeypatch)
    session = _Session(already_active=True)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == 503 and session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "status"),
    [(InvalidUsage("synthetic invalid query"), 400), (RuntimeError("synthetic backend detail"), 503)],
)
async def test_provider_failures_are_safe_and_do_not_fall_back(monkeypatch, failure, status):
    session = _Session()
    _install(monkeypatch, session, page_failure=failure)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == status and b"synthetic" not in reply.body
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events[-1] == "end"


@pytest.mark.asyncio
async def test_failed_finality_cannot_return_native_rows(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, finality_failure=True)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == 503 and b'"rows"' not in reply.body
    assert session.events[-2:] == ["finality", "end"]
    assert session.rolled_back


@pytest.mark.asyncio
async def test_provider_deadline_cancels_the_shared_transaction_without_fallback(monkeypatch):
    session = _Session()
    _install(monkeypatch, session)

    async def delayed_page(*_args, **_kwargs):
        await asyncio.sleep(1)
        pytest.fail("provider response outlived its deadline")

    monkeypatch.setattr(provider_http, "DEFAULT_READ_TIMEOUT_MS", 10)
    monkeypatch.setattr(provider_http, "_provider_page", delayed_page)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == 503 and session.rolled_back
    assert "finality" not in session.events


@pytest.mark.asyncio
async def test_external_cancellation_propagates_after_rolling_back_the_snapshot(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, page_failure=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await provider_http.serve_custom_import_providers(_request(), session)

    assert session.events[-2:] == ["page", "end"]
    assert session.rolled_back
    assert "finality" not in session.events


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 500])
async def test_oversized_or_failed_native_response_is_not_exposed(monkeypatch, status):
    session = _Session()
    _install(monkeypatch, session)

    async def invalid_page(*_args, **_kwargs):
        return response.raw(b"x" * (transport._MAX_RESPONSE_BYTES + 1), status=status)

    monkeypatch.setattr(provider_http, "_provider_page", invalid_page)

    reply = await provider_http.serve_custom_import_providers(_request(), session)

    assert reply.status == 503 and session.rolled_back
    assert len(reply.body) < 256


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_body(extra="unexpected"), id="extra-key"),
        pytest.param(
            json.dumps(json.loads(_body()), separators=(", ", ": "), sort_keys=True).encode("ascii"), id="noncanonical"
        ),
    ],
)
async def test_signed_provider_body_shape_failure_never_opens_a_transaction(monkeypatch, body):
    fixtures._install_keyring(monkeypatch)
    session = _Session()

    reply = await provider_http.serve_custom_import_providers(_request(body), session)

    assert reply.status == 404
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"", b"x" * (transport._MAX_BODY_BYTES + 1)])
async def test_unbounded_provider_body_never_opens_a_transaction(monkeypatch, body):
    fixtures._install_keyring(monkeypatch)
    session = _Session()
    request = _request()
    request.body = body

    reply = await provider_http.serve_custom_import_providers(request, session)

    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
async def test_registered_provider_route_forwards_the_bound_session(monkeypatch):
    session = object()
    observed_calls = []

    async def serve(request, candidate_session):
        observed_calls.append((request.method, request.path, candidate_session))
        return response.json({"ok": True})

    monkeypatch.setattr(extension_reads, "serve_custom_import_providers", serve)
    app = Sanic(f"custom-import-provider-route-{uuid.uuid4().hex}")

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = session

    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _request, result = await app.asgi_client.post(provider_http.CUSTOM_IMPORT_PROVIDERS_PATH, data=_body())

    assert result.status_code == 200
    assert result.json == {"ok": True}
    assert observed_calls == [("POST", provider_http.CUSTOM_IMPORT_PROVIDERS_PATH, session)]
