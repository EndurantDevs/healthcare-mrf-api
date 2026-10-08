# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed provider-service reads keep authorization and hydration in one snapshot."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic import response
from sqlalchemy import literal, select

from api import custom_import_plan_sql as plan_sql
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
                    children=(
                        SimpleNamespace(collection="facts", fields=(ReadFieldValue("score", "integer", "value", 3),)),
                    ),
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
@pytest.mark.parametrize("limit", ["51", "100"])
@pytest.mark.parametrize("row_count", [0, 50, 51])
@pytest.mark.parametrize("require_match", [False, True])
async def test_complete_service_page_limit_is_rejected_before_storage(monkeypatch, limit, row_count, require_match):
    session = _Session()
    rows = [{"npi": str(1000000000 + index)} for index in range(row_count)]
    _install(monkeypatch, session, page_items=rows, imported=False, require_match=require_match)
    body = _body(
        native_query={"code_system": "CPT", "code": "99213", "limit": limit},
        filters=[{"field_id": "metric", "operator": "gt", "value": "5"}] if require_match else [],
        order=None if require_match else [{"field_id": "metric", "direction": "desc"}],
        require_match=require_match,
    )

    reply = await service_http.serve_custom_import_provider_service(_request(body), session)

    assert reply.status == 400
    assert session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", [0, 50])
async def test_order_only_service_page_limit_accepts_fifty_with_unmatched_rows(monkeypatch, row_count):
    session = _Session()
    rows = [{"npi": str(1000000000 + index)} for index in range(row_count)]
    _install(monkeypatch, session, page_items=rows, imported=False, require_match=False)
    body = _body(
        native_query={"code_system": "CPT", "code": "99213", "limit": "50"},
        filters=[],
        order=[{"field_id": "metric", "direction": "desc"}],
        require_match=False,
    )

    reply = await service_http.serve_custom_import_provider_service(_request(body), session)

    assert reply.status == 200
    payload = json.loads(reply.body)
    assert len(payload["items"]) == row_count and payload["pagination"]["total"] == row_count
    assert all(row["custom_import"] is None for row in payload["items"])
    assert session.events.index("hydrate") < session.events.index("finality")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row_count", "field_bytes", "status"),
    [(25, 300, 200), (50, 300, 200), (51, 300, 503), (25, 262144, 503)],
)
async def test_complete_service_page_keeps_per_provider_and_final_wire_bounds(
    monkeypatch, row_count, field_bytes, status
):
    session = _Session()
    provider_rows = [{"npi": str(1000000000 + index)} for index in range(row_count)]
    _install(monkeypatch, session, page_items=provider_rows, imported=False)
    imported = SimpleNamespace(
        root_fields=(),
        context_fields=(),
        children=tuple(
            SimpleNamespace(
                collection="facts",
                fields=(ReadFieldValue("text", "string", "value", "x" * field_bytes),),
            )
            for _ in range(42)
        ),
    )

    async def hydrate(self, same_session, **kwargs):
        assert same_session is session
        assert kwargs["entity_values"] == tuple(provider_document["npi"] for provider_document in provider_rows)
        session.events.append("hydrate")
        return {provider_document["npi"]: imported for provider_document in provider_rows}

    monkeypatch.setattr(service_http.CustomImportReadService, "hydrate_npi_page", hydrate)
    reply = await service_http.serve_custom_import_provider_service(_request(), session)
    assert reply.status == status
    if status == 200:
        response_document = json.loads(reply.body)
        assert transport._MAX_RESPONSE_BYTES < len(reply.body) <= (row_count + 1) * transport._MAX_RESPONSE_BYTES
        assert response_document["pagination"]["total"] == row_count
        assert [provider_document["npi"] for provider_document in response_document["items"]] == [
            provider_document["npi"] for provider_document in provider_rows
        ]
        assert all(
            len(provider_document["custom_import"]["children"]) == 42
            for provider_document in response_document["items"]
        )
        assert session.events.index("hydrate") < session.events.index("finality")
    else:
        assert session.rolled_back and len(reply.body) < 256
        if row_count > 50:
            assert "hydrate" not in session.events


@pytest.mark.asyncio
async def test_service_read_hydrates_canonical_npi_under_one_signed_snapshot(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, page_items=[{"npi": 1104212877, "provider_name": "Synthetic Provider"}])
    reply = await service_http.serve_custom_import_provider_service(_request(), session)

    assert reply.status == 200 and reply.headers["cache-control"] == "private, no-store"
    provider_payload = json.loads(reply.body)
    assert provider_payload["items"][0]["npi"] == "1104212877"
    assert provider_payload["items"][0]["custom_import"] == {
        "target": fixtures._TARGET,
        "root_fields": [{"field_id": "metric", "field_type": "integer", "state": "value", "value": 7}],
        "context_fields": [],
        "children": [
            {
                "collection": "facts",
                "fields": [{"field_id": "score", "field_type": "integer", "state": "value", "value": 3}],
            }
        ],
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
@pytest.mark.parametrize("has_plan_release", [False, True])
async def test_signed_plan_storage_precedes_read_only_snapshot(monkeypatch, has_plan_release):
    session = SimpleNamespace()
    events = []

    async def execute(statement):
        events.append(str(statement))

    async def allocate(same_session):
        assert same_session is session
        events.append("allocate temporary plan tables")

    session.execute = execute
    allocation = AsyncMock(side_effect=allocate)
    monkeypatch.setattr(plan_sql, "prepare_plan_query_tables", allocation)
    await service_http._prepare_service_snapshot(
        session, {"plan_release_id": "synthetic-release"} if has_plan_release else {}
    )
    if has_plan_release:
        assert events == [
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "allocate temporary plan tables",
            "SET TRANSACTION READ ONLY",
        ]
        allocation.assert_awaited_once_with(session)
    else:
        assert events == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"]
        allocation.assert_not_awaited()


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
        request = _request(_body(native_query={"code_system": "CPT", "code": "99213", "snapshot_id": "untrusted"}))
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


@pytest.mark.parametrize(
    "changes,expected_release,valid",
    (
        ({}, "synthetic-plan-release", True),
        ({}, None, False),
        ({"plan_release_id": "other-release"}, "synthetic-plan-release", False),
        ({"query": {"plan_release_id": "other-release"}}, "synthetic-plan-release", False),
        ({"pricing_scope": "claims"}, "synthetic-plan-release", False),
        ({"custom_import_native_entry_ids": None}, "synthetic-plan-release", False),
        ({"custom_import_native_entry_ids": ["a" * 64] * 2}, "synthetic-plan-release", False),
        ({"custom_import_native_entry_ids": ["a" * 64]}, "synthetic-plan-release", False),
        ({"custom_import_native_entry_ids": ["bad", "b" * 64]}, "synthetic-plan-release", False),
    ),
)
def test_plan_native_entry_identity_is_bound_to_signed_release(changes, expected_release, valid):
    """Repeated NPI rows require one unique native identity per signed plan entry."""

    plan_payload_by_field = {
        "items": [{"npi": 1104212877}, {"npi": "1104212877"}],
        "pagination": {"total": 2, "limit": 2, "offset": 0, "page": 1},
        "query": {"plan_release_id": "synthetic-plan-release"},
        "plan_release_id": "synthetic-plan-release",
        "pricing_scope": "plan_scoped_ptg",
        "custom_import_native_entry_ids": ["a" * 64, "b" * 64],
        **changes,
    }
    body = transport._canonical_json_bytes(plan_payload_by_field)
    if not valid:
        with pytest.raises(CustomImportReadUnavailableError):
            service_http._service_payload(body, plan_release_id=expected_release)
        return
    parsed_by_field = service_http._service_payload(body, plan_release_id=expected_release)
    assert [provider_item["npi"] for provider_item in parsed_by_field["items"]] == ["1104212877"] * 2
    assert parsed_by_field["custom_import_native_entry_ids"] == ["a" * 64, "b" * 64]
