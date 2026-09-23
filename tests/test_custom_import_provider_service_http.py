# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider-service reads keep authorization and hydration in one snapshot."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sanic import response
from sqlalchemy import literal, select

from api import custom_import_provider_service_http as service_http
from api import custom_import_read_http as transport
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadFieldValue
from tests import test_custom_import_read_http as fixtures


def _body(**changes):
    document_map = {
        "target": fixtures._TARGET,
        "native_query": {"code_system": "CPT", "code": "99213", "limit": "2"},
        "context": [{"field_id": "service", "operator": "eq", "value": "99213"}],
        "filters": [{"field_id": "metric", "operator": "gt", "value": "5"}],
        "order": None,
        "require_match": True,
    }
    document_map.update(changes)
    return transport._canonical_json_bytes(document_map)


def _request(body=None, **changes):
    body = _body() if body is None else body
    request = SimpleNamespace(
        body=body,
        headers=fixtures._provider_headers(body=body, path=service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH),
        method="POST",
        path=service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH,
        query_string="",
    )
    for name, value in changes.items():
        setattr(request, name, value)
    return request


class _Session:
    def __init__(self):
        self.events = []
        self.rolled_back = False

    def in_transaction(self):
        return False

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


def _read_service_type(session, page_items, imported, require_match):
    """Build a synthetic authorized read service for one native page."""

    class ReadService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, **kwargs):
            assert same_session is session
            assert self.authorizer.authorize(kwargs["authorization"], target=kwargs["target"]).value == "a" * 64
            query = kwargs["query"]
            assert query.require_match is require_match and query.require_exact_context is True
            assert query.context_filters[0].value == "99213"
            if require_match:
                assert query.filters[0].value == "5"
            else:
                assert query.filters == ()
            session.events.append("prepare")
            columns = [literal("1104212877").label("entity_value")]
            if query.order_terms:
                columns.append(literal(7).label("sort_0"))
            return PreparedNpiEntityRelation(select(*columns), query.order_terms or (), "b" * 64, "c" * 64)

        async def hydrate_npi_page(self, same_session, **kwargs):
            assert same_session is session
            assert kwargs["entity_values"] == tuple(str(provider["npi"]) for provider in page_items or [])
            session.events.append("hydrate")
            if not imported:
                return {}
            return {
                "1104212877": SimpleNamespace(
                    root_fields=(ReadFieldValue("metric", "integer", "value", 7),),
                    context_fields=(),
                )
            }

    return ReadService


def _install(monkeypatch, session, *, page_items=None, imported=True, require_match=True, finality_failure=False):
    """Bind signed transport, native rows, and generation finality to one session."""
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

    async def page(request, *, native_args, import_context):
        assert request.path == service_http.CUSTOM_IMPORT_PROVIDER_SERVICE_PATH
        assert native_args.get("code_system") == "CPT" and native_args.get("code") == "99213"
        assert import_context.require_match is require_match
        session.events.append("page")
        return response.json(
            {
                "items": page_items or [],
                "pagination": {"total": len(page_items or []), "page": 1, "limit": 2, "offset": 0},
                "query": {"code_system": "CPT", "code": "99213"},
            }
        )

    async def finality(same_session, target):
        assert same_session is session and target.dataset_id == 11
        session.events.append("finality")
        if finality_failure:
            raise CustomImportReadUnavailableError("synthetic ineligible generation")

    monkeypatch.setattr(service_http, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", resolve)
    monkeypatch.setattr(
        service_http, "CustomImportReadService", _read_service_type(session, page_items, imported, require_match)
    )
    monkeypatch.setattr(service_http, "_service_page", page)
    monkeypatch.setattr(service_http, "verify_published_generation", finality)


@pytest.mark.parametrize(
    "native_query", [{"mode": "plan"}, {"plan_id": "x"}, {"code": "99213"}, {"code_system": "CPT"}]
)
def test_service_native_query_rejects_nonclaims_or_missing_code(native_query):
    with pytest.raises(CustomImportReadRequestError):
        service_http._parse_service_native_query(native_query)


@pytest.mark.asyncio
async def test_service_read_hydrates_canonical_npi_under_one_signed_snapshot(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, page_items=[{"npi": 1104212877, "provider_name": "Synthetic Provider"}])
    reply = await service_http.serve_custom_import_provider_service(_request(), session)

    assert reply.status == 200 and reply.headers["cache-control"] == "private, no-store"
    payload = json.loads(reply.body)
    assert payload["items"][0]["npi"] == "1104212877"
    assert payload["items"][0]["custom_import"] == {
        "target": fixtures._TARGET,
        "root_fields": [{"field_id": "metric", "field_type": "integer", "state": "value", "value": 7}],
        "context_fields": [],
    }
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
async def test_order_only_service_page_keeps_unmatched_provider_with_null_import(monkeypatch):
    session = _Session()
    _install(
        monkeypatch,
        session,
        page_items=[{"npi": 1104212877}, {"npi": "1000000000"}],
        require_match=False,
    )
    body = _body(filters=[], order=[{"field_id": "metric", "direction": "desc"}], require_match=False)
    reply = await service_http.serve_custom_import_provider_service(_request(body), session)
    assert reply.status == 200
    payload = json.loads(reply.body)
    assert [item["npi"] for item in payload["items"]] == ["1104212877", "1000000000"]
    assert payload["items"][0]["custom_import"] is not None
    assert payload["items"][1]["custom_import"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["signature", "path", "query", "unknown_native"])
async def test_invalid_service_permit_does_not_touch_database(monkeypatch, failure):
    fixtures._install_keyring(monkeypatch)
    request = _request()
    if failure == "signature":
        request.headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = "A" * 43
    elif failure == "path":
        request.headers = fixtures._provider_headers(body=request.body, path=transport.CUSTOM_IMPORT_READ_PATH)
    elif failure == "query":
        request.query_string = "code=99213"
    else:
        request = _request(_body(native_query={"code_system": "CPT", "code": "99213", "mode": "plan"}))
    session = _Session()
    reply = await service_http.serve_custom_import_provider_service(request, session)
    assert reply.status in {400, 403, 404} and session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "items,imported",
    [
        ([{"npi": 1104212877}], False),
        ([{"npi": "bad"}], True),
        ([{"npi": 1104212877}, {"npi": "1104212877"}], True),
    ],
)
async def test_missing_import_or_bad_native_identity_fails_closed(monkeypatch, items, imported):
    session = _Session()
    _install(monkeypatch, session, page_items=items, imported=imported)
    reply = await service_http.serve_custom_import_provider_service(_request(), session)
    assert reply.status == 503 and session.rolled_back


@pytest.mark.asyncio
async def test_generation_finality_failure_does_not_return_provider_data(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, page_items=[{"npi": 1104212877}], finality_failure=True)
    reply = await service_http.serve_custom_import_provider_service(_request(), session)
    assert reply.status == 503 and session.rolled_back
