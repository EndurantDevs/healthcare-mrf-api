# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite signed batches preserve scope, inclusion, and pre-page counts."""

import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from sanic.exceptions import InvalidUsage
from sqlalchemy import literal, select

from api import custom_import_provider_batch as batch
from api import custom_import_read_http as transport
from api import provider_batch, provider_list_sql
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import extension_reads
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadFieldValue, ReadOrderTerm
from tests import test_custom_import_read_http as fixtures
from tests.test_custom_import_provider_http import _Session


def _body(**changes):
    document_by_field = {
        "target": fixtures._TARGET,
        "native_query": {},
        "context": [],
        "filters": [],
        "order": None,
        "require_match": True,
        "include_filter": False,
        "native_batch": {
            "npis": ["9000000000", "9000000001"],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        },
    }
    document_by_field.update(changes)
    return transport._canonical_json_bytes(document_by_field)


def _request(body):
    return SimpleNamespace(
        body=body,
        headers=fixtures._provider_headers(body=body, path=batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH),
        method="POST",
        path=batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH,
        query_string="",
    )


def _install(monkeypatch, session, *, finality_failure=False):
    fixtures._install_keyring(monkeypatch)
    prepared_reads = []
    hydrated_chunks = []

    class ReadService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, *, authorization, target, query):
            assert same_session is session and self.authorizer.authorize(authorization, target=target).value == "a" * 64
            prepared = PreparedNpiEntityRelation(
                select(literal("9000000000").label("entity_value")), (), "b" * 64, "c" * 64
            )
            prepared_reads.append((query, prepared, target))
            return prepared

        async def hydrate_npi_page(self, same_session, **kwargs):
            query, prepared, target = prepared_reads[0]
            assert same_session is session and kwargs["query"] is query and kwargs["prepared"] is prepared
            assert kwargs["pinned_target"] is target and set(kwargs["entity_values"]) <= set(query.entity_values)
            hydrated_chunks.append(kwargs["entity_values"])
            item = SimpleNamespace(
                root_fields=(ReadFieldValue("score", "integer", "value", 3),), context_fields=(), children=()
            )
            return {identity: item for identity in kwargs["entity_values"]}

    @asynccontextmanager
    async def bounded(same_session, **kwargs):
        assert same_session is session and kwargs["timeout_ms"] > 0
        yield

    async def native(request, params, *, native_args, session, import_context):
        assert tuple(params["npis"]) == import_context.native_npis
        items = [{"npi": identity, "status": 200, "provider": {"npi": identity}} for identity in params["npis"]]
        return {
            "items": items,
            "requested": len(items),
            "found": len(items),
            "not_found": 0,
            "pagination": {"total": len(items), "page": 1, "offset": 0, "limit": len(items), "has_more": False},
        }

    async def finality(same_session, target):
        assert same_session is session and target is prepared_reads[0][2]
        if finality_failure:
            raise CustomImportReadUnavailableError("synthetic unavailable generation")

    monkeypatch.setattr(batch, "CustomImportReadService", ReadService)
    monkeypatch.setattr(batch, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", fixtures._resolved_target)
    monkeypatch.setattr(provider_batch, "read_native_batch", native)
    monkeypatch.setattr(batch, "verify_published_generation", finality)
    return prepared_reads, hydrated_chunks


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
async def test_signed_batch_prepares_once_and_hydrates_only_explicitly_included_payload(monkeypatch, include_filter):
    session = _Session()
    prepared, chunks = _install(monkeypatch, session)
    native_reader = provider_batch.read_native_batch

    async def native_with_payload(*args, **kwargs):
        payload = await native_reader(*args, **kwargs)
        for item in payload["items"]:
            item["provider"]["synthetic_notes"] = "x" * 3000
        return payload

    monkeypatch.setattr(provider_batch, "read_native_batch", native_with_payload)
    native_batch = json.loads(_body())["native_batch"]
    native_batch["npis"] = [str(9000000000 + index) for index in range(100)]
    reply = await batch.serve_custom_import_provider_batch(
        _request(_body(native_batch=native_batch, include_filter=include_filter)), session
    )
    assert reply.status == 200 and reply.headers["cache-control"] == "private, no-store"
    assert len(reply.body) > transport._MAX_RESPONSE_BYTES
    payload = json.loads(reply.body)
    assert len(prepared) == 1 and prepared[0][0].entity_values == tuple(native_batch["npis"])
    assert payload["found"] == 100 and payload["not_found"] == 0
    assert [len(chunk) for chunk in chunks] == ([50, 50] if include_filter else [])
    assert all(("custom_import" in item["provider"]) is include_filter for item in payload["items"])
    assert session.events == ["begin", "snapshot", "end"]


@pytest.mark.asyncio
async def test_signed_batch_finality_failure_rolls_back_without_partial_response(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, finality_failure=True)
    reply = await batch.serve_custom_import_provider_batch(_request(_body()), session)
    assert reply.status == 503 and session.rolled_back
    assert "items" not in json.loads(reply.body)


@pytest.mark.asyncio
async def test_batch_signature_cannot_replay_a_provider_page_permit(monkeypatch):
    session = _Session()
    _install(monkeypatch, session)
    request = _request(_body())
    request.headers = fixtures._provider_headers(body=request.body, path="/api/v1/extensions/custom-import/providers")
    reply = await batch.serve_custom_import_provider_batch(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.parametrize(
    "native_batch",
    [
        None,
        {},
        {"npis": ["9000000000"]},
        {
            "npis": [9000000000],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        },
        {
            "npis": ["0900000000"],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        },
    ],
)
def test_signed_batch_requires_closed_canonical_native_shape(native_batch):
    with pytest.raises((CustomImportReadRequestError, InvalidUsage)):
        batch._parse_batch_request(_body(native_batch=native_batch))


def test_signed_batch_retains_original_native_bounds():
    native_batch = json.loads(_body())["native_batch"]
    for field, value in (
        ("npis", [str(9000000000 + index) for index in range(101)]),
        ("address_limit", 21),
        ("address_offset", -1),
    ):
        with pytest.raises(InvalidUsage):
            batch._parse_batch_request(_body(native_batch={**native_batch, field: value}))


@pytest.mark.parametrize("require_match", (False, True))
def test_requested_npi_scope_is_typed_and_keeps_optional_imports(require_match):
    prepared = PreparedNpiEntityRelation(
        select(literal("9000000000").label("entity_value"), literal(3).label("sort_0")),
        (ReadOrderTerm("score", "desc", "last"),),
        "b" * 64,
        "c" * 64,
    )
    context = ProviderImportQuery(
        prepared, compile_npi_entity_relation(prepared.statement), require_match, (9000000000, 9000000001)
    )
    clause = provider_list_sql._provider_import_match_clause(context, "c.npi")
    assert clause.startswith("(c.npi) = ANY(:__native_batch_npis)")
    assert "IN (SELECT CASE" not in clause
    assert ("EXISTS" in clause) is require_match
    statement = provider_list_sql._provider_list_statement(
        "SELECT 1 WHERE " + clause, None, native_npis=context.native_npis
    )
    assert str(statement._bindparams["__native_batch_npis"].type) == "ARRAY"
    assert provider_list_sql._provider_list_parameters({}, None, native_npis=context.native_npis) == {
        "__native_batch_npis": [9000000000, 9000000001]
    }
    with pytest.raises(ValueError, match="collide"):
        provider_list_sql._provider_list_parameters({"__native_batch_npis": []}, None, native_npis=context.native_npis)


def test_openapi_signed_batch_binds_include_and_native_pagination_contract():
    document_map = yaml.safe_load(Path("doc/openapi.yaml").read_text())
    schemas_by_name = document_map["components"]["schemas"]
    provider_schema = schemas_by_name["CustomImportProviderRequest"]
    assert provider_schema["properties"]["include_filter"]["default"] is True
    assert provider_schema["properties"]["context"]["items"]["allOf"][1]["properties"]["operator"]["enum"] == ["eq"]
    request_schema = schemas_by_name["CustomImportProviderBatchRequest"]
    native_schema = request_schema["properties"]["native_batch"]
    assert native_schema["additionalProperties"] is False and set(native_schema["required"]) == batch._BATCH_KEYS
    assert native_schema["properties"]["npis"]["maxItems"] == 100
    response_schema = schemas_by_name["CustomImportProviderBatchResponse"]
    assert set(response_schema["properties"]["pagination"]["required"]) == {
        "total",
        "page",
        "offset",
        "limit",
        "has_more",
    }
    assert "custom_import" not in schemas_by_name["CustomImportProviderResult"]["required"]
    assert "/extensions/custom-import/providers/batch" in document_map["paths"]


@pytest.mark.parametrize(
    ("page_schema", "rows_key"),
    [
        ("CustomImportProviderListPage", "rows"),
        ("CustomImportProviderGeoPage", "items"),
        ("CustomImportProviderServicePage", "items"),
        ("CustomImportProviderBatchResponse", "items"),
    ],
)
def test_openapi_provider_pages_accept_absent_optional_import(page_schema, rows_key):
    schemas = yaml.safe_load(Path("doc/openapi.yaml").read_text())["components"]["schemas"]
    row_schema = schemas[page_schema]["properties"][rows_key]["items"]
    if page_schema == "CustomImportProviderBatchResponse":
        row_schema = row_schema["properties"]["provider"]
    assert row_schema == {"$ref": "#/components/schemas/CustomImportProviderResult"}
    provider = schemas["CustomImportProviderResult"]
    assert "custom_import" not in provider["required"]
    alternatives = provider["properties"]["custom_import"]["oneOf"]
    assert [option for option in alternatives if option.get("nullable")] == [
        {"type": "object", "nullable": True, "enum": [None]}
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"method": "GET"}, {"path": "/other"}, {"query_string": "page=1"}])
async def test_signed_batch_rejects_route_changes_before_opening_snapshot(monkeypatch, change):
    session = _Session()
    prepared, chunks = _install(monkeypatch, session)
    request = _request(_body())
    for name, value in change.items():
        setattr(request, name, value)
    reply = await batch.serve_custom_import_provider_batch(request, session)
    assert reply.status == 404 and session.events == []
    assert prepared == chunks == []


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [None, b"", b"[]", b" {} ", b"x" * (transport._MAX_BODY_BYTES + 1)])
async def test_signed_batch_rejects_malformed_body_before_opening_snapshot(monkeypatch, body):
    session = _Session()
    prepared, chunks = _install(monkeypatch, session)
    request = _request(_body())
    request.body = body
    reply = await batch.serve_custom_import_provider_batch(request, session)
    assert reply.status == 404 and session.events == []
    assert prepared == chunks == [] and "items" not in json.loads(reply.body)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_session", [False, True])
async def test_signed_batch_requires_a_fresh_session_after_signature_verification(monkeypatch, has_session):
    session = _Session(already_active=True)
    prepared, chunks = _install(monkeypatch, session)
    reply = await batch.serve_custom_import_provider_batch(_request(_body()), session if has_session else None)
    assert reply.status == 503 and session.events == []
    assert prepared == chunks == [] and "items" not in json.loads(reply.body)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
async def test_signed_batch_oversized_native_response_rolls_back_without_partial_items(monkeypatch, include_filter):
    session = _Session()
    prepared, chunks = _install(monkeypatch, session)
    native_reader = provider_batch.read_native_batch

    async def native_with_payload(*args, **kwargs):
        payload = await native_reader(*args, **kwargs)
        for item in payload["items"]:
            item["provider"]["synthetic_notes"] = "x" * 2048
        return payload

    monkeypatch.setattr(provider_batch, "read_native_batch", native_with_payload)
    monkeypatch.setattr(transport, "_MAX_RESPONSE_BYTES", 1024)
    reply = await batch.serve_custom_import_provider_batch(_request(_body(include_filter=include_filter)), session)
    assert reply.status == 503 and session.rolled_back
    assert session.events == ["begin", "snapshot", "end"] and len(prepared) == 1
    assert [len(chunk) for chunk in chunks] == ([2] if include_filter else [])
    assert "items" not in json.loads(reply.body)


@pytest.mark.asyncio
async def test_signed_batch_endpoint_keeps_the_request_session(monkeypatch):
    session = object()
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))
    expected = object()
    receiver = AsyncMock(return_value=expected)
    monkeypatch.setattr(extension_reads, "serve_custom_import_provider_batch", receiver)
    assert await extension_reads.providers_batch(request) is expected
    receiver.assert_awaited_once_with(request, session)


@pytest.mark.parametrize("include_filter", [False, True])
@pytest.mark.parametrize("row_count", [-1, batch.provider_http.MAX_NPI_PAGE_SIZE + 1])
def test_provider_response_rejects_invalid_counts_even_without_import_payload(include_filter, row_count):
    parsed = batch._parse_batch_request(_body(include_filter=include_filter)).provider
    with pytest.raises(CustomImportReadUnavailableError, match="page exceeds its bound"):
        batch.provider_http._provider_response_limit(parsed, row_count)
