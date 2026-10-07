# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact batch hydration stays signed, bounded and independent of native paging."""

import asyncio
import json
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic import Blueprint, Sanic, response
from sqlalchemy.dialects import postgresql

from api import custom_import_detail_batch as batch
from api import custom_import_read_http as transport
from api.endpoint import extension_reads
from process.custom_import import read_core
from process.custom_import.read_core import EntityFamilySet, ReadChild, ReadFieldValue
from tests import custom_import_grouped_support as grouped
from tests import test_custom_import_provider_query as query_fixture
from tests import test_custom_import_read_http as fixtures
from tests.openapi_route_contract_support import load_openapi_document
from tests.test_custom_import_provider_http import _Session

_VALUES = ("1000000002", "1000000001")


def _body(**changes):
    document_map = {
        "target": fixtures._TARGET,
        "entities": {"adapter_id": "npi", "values": list(_VALUES)},
        "family_entitlement": "full_family",
    }
    document_map.update(changes)
    return transport._canonical_json_bytes(document_map)


def _request(body=None, **changes):
    body = _body() if body is None else body
    request = SimpleNamespace(
        body=body,
        headers=fixtures._headers(body=body, path=batch.CUSTOM_IMPORT_DETAIL_BATCH_PATH),
        method="POST",
        path=batch.CUSTOM_IMPORT_DETAIL_BATCH_PATH,
        query_string="",
    )
    for name, value in changes.items():
        setattr(request, name, value)
    return request


def _install(monkeypatch, session, imported=None, *, failure=None, finality_failure=False):
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

    class BatchHydrationService:
        def __init__(self, *, authorizer):
            self.authorizer = authorizer

        async def prepare_npi_entity_relation(self, same_session, *, authorization, target, query):
            assert same_session is session
            assert self.authorizer.authorize(authorization, target=target).value == "a" * 64
            assert query.require_match is False and query.filters == () and query.order_terms is None
            session.events.append("prepare")
            return query

        async def hydrate_npi_page(self, same_session, **arguments):
            assert same_session is session and arguments["prepared"] == arguments["query"]
            assert arguments["entity_values"] == _VALUES
            assert arguments["full_family"] is True
            session.events.append("hydrate")
            if failure is not None:
                raise failure
            return {} if imported is None else imported

    async def finality(same_session, _target):
        assert same_session is session
        session.events.append("finality")
        if finality_failure:
            raise RuntimeError("synthetic finality failure")

    monkeypatch.setattr(batch, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", resolve)
    monkeypatch.setattr(batch, "CustomImportReadService", BatchHydrationService)
    monkeypatch.setattr(batch, "verify_published_generation", finality)


@pytest.mark.asyncio
@pytest.mark.parametrize("grouped_family", [False, True])
async def test_batch_reuses_authorized_hydration_and_preserves_order_absence_and_decimals(monkeypatch, grouped_family):
    session = _Session()
    family = SimpleNamespace(
        root_fields=(ReadFieldValue("score", "decimal", "value", Decimal("1.200000000001")),),
        context_fields=(),
        children=(ReadChild("facts", 1, (ReadFieldValue("count", "integer", "value", 7),)),),
    )
    imported = family
    request_changes_map = {}
    if grouped_family:
        imported = EntityFamilySet(
            None, "full_family", "period", 2024, (("segment_a", family),), ("segment_b",), "b" * 64, "c" * 64
        )
        request_changes_map["grouped_entity_selection"] = grouped.selection_document()
    _install(monkeypatch, session, {_VALUES[1]: imported})

    reply = await batch.serve_custom_import_detail_batch(_request(_body(**request_changes_map)), session)

    assert reply.status == 200
    provider_payload = json.loads(reply.body)
    assert set(provider_payload) == {"target", "items"} and provider_payload["target"] == fixtures._TARGET
    assert [provider_item["npi"] for provider_item in provider_payload["items"]] == list(_VALUES)
    assert provider_payload["items"][0]["custom_import"] is None
    imported_family = provider_payload["items"][1]["custom_import"]
    assert imported_family["target"] == fixtures._TARGET
    if grouped_family:
        assert imported_family["projection"] == "full_family" and imported_family["missing_group_values"] == [
            "segment_b"
        ]
        imported_family = imported_family["families"][0]
    else:
        assert set(imported_family) == {"target", "root_fields", "children"}
    assert imported_family["root_fields"][0]["value"] == "1.200000000001"
    assert imported_family["children"][0]["fields"][0]["value"] == 7
    assert reply.headers["cache-control"] == "private, no-store"
    assert session.events == ["begin", "snapshot", "bounded", "resolve", "prepare", "hydrate", "finality", "end"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"entities": {"adapter_id": "other", "values": list(_VALUES)}},
        {"entities": {"adapter_id": "npi", "values": []}},
        {"entities": {"adapter_id": "npi", "values": [str(1000000000 + index) for index in range(51)]}},
        {"entities": {"adapter_id": "npi", "values": [_VALUES[0], _VALUES[0]]}},
        {"entities": {"adapter_id": "npi", "values": [1000000000]}},
        {"entities": {"adapter_id": "npi", "values": ["１000000000"]}},
        {"entities": {"adapter_id": "npi", "values": ["bad"]}},
        {"family_entitlement": "query_projection"},
        {"entity": {"adapter_id": "npi", "value": _VALUES[0]}},
        {"native_query": {"limit": "2"}},
        {"filters": [{"field_id": "score", "operator": "gt", "value": 1}]},
        {"context": []},
    ],
)
async def test_invalid_identity_or_search_input_never_opens_a_snapshot(monkeypatch, changes):
    fixtures._install_keyring(monkeypatch)
    session = _Session()
    reply = await batch.serve_custom_import_detail_batch(_request(_body(**changes)), session)
    assert reply.status in {400, 404} and session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["identity", "path", "target", "noncanonical"])
async def test_signed_body_path_and_target_cannot_be_changed(monkeypatch, change):
    fixtures._install_keyring(monkeypatch)
    session = _Session()
    request = _request()
    if change == "identity":
        request.body = _body(entities={"adapter_id": "npi", "values": ["1000000003"]})
    elif change == "path":
        request.headers = fixtures._headers(body=request.body, path=transport.CUSTOM_IMPORT_DETAIL_PATH)
    elif change == "target":
        request.headers = fixtures._resigned_headers(
            body=request.body,
            path=batch.CUSTOM_IMPORT_DETAIL_BATCH_PATH,
            target={**fixtures._TARGET, "generation_id": 102},
        )
    else:
        request.body = json.dumps(json.loads(request.body)).encode("ascii")
    reply = await batch.serve_custom_import_detail_batch(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", ["unknown_identity", "oversized_family", "finality", "active_session"])
async def test_batch_failure_never_exposes_a_partial_result(monkeypatch, condition):
    session = _Session(already_active=condition == "active_session")
    families_by_npi = {}
    if condition == "unknown_identity":
        families_by_npi["1000000003"] = None
    elif condition == "oversized_family":
        families_by_npi[_VALUES[0]] = SimpleNamespace(
            root_fields=(ReadFieldValue("text", "string", "value", "x" * transport._MAX_RESPONSE_BYTES),),
            context_fields=(),
            children=(),
        )
    _install(monkeypatch, session, families_by_npi, finality_failure=condition == "finality")
    reply = await batch.serve_custom_import_detail_batch(_request(), session)
    assert reply.status == 503 and b'"items"' not in reply.body
    if condition == "active_session":
        assert session.events == []
    else:
        assert session.rolled_back and session.events[-1] == "end"


@pytest.mark.asyncio
async def test_external_cancellation_rolls_back_without_partial_results(monkeypatch):
    session = _Session()
    _install(monkeypatch, session, failure=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await batch.serve_custom_import_detail_batch(_request(), session)
    assert session.rolled_back and session.events[-1] == "end" and "finality" not in session.events


def test_exact_batch_accepts_the_full_finite_identity_bound():
    npi_values = [str(1000000000 + index) for index in range(50)]
    parsed, accepted_npis = batch._parse_batch_request(_body(entities={"adapter_id": "npi", "values": npi_values}))
    assert accepted_npis == tuple(npi_values) and parsed.family_entitlement == "full_family"


@pytest.mark.asyncio
async def test_batch_deadline_rolls_back_the_snapshot(monkeypatch):
    session = _Session()
    _install(monkeypatch, session)

    async def delayed_hydration(*_args):
        await asyncio.sleep(1)
        pytest.fail("batch hydration outlived its deadline")

    monkeypatch.setattr(batch, "_read_batch_payload", delayed_hydration)
    monkeypatch.setattr(batch, "DEFAULT_READ_TIMEOUT_MS", 10)
    reply = await batch.serve_custom_import_detail_batch(_request(), session)
    assert reply.status == 503 and session.rolled_back
    assert "finality" not in session.events and session.events[-1] == "end"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [{"method": "GET"}, {"query_string": "page=2"}, {"headers": {}}, {"body": b"x" * (transport._MAX_BODY_BYTES + 1)}],
)
async def test_closed_request_boundary_never_opens_a_snapshot(monkeypatch, changes):
    fixtures._install_keyring(monkeypatch)
    session = _Session()
    request = _request()
    for field_name, field_value in changes.items():
        setattr(request, field_name, field_value)
    reply = await batch.serve_custom_import_detail_batch(request, session)
    assert reply.status == 404 and session.events == []


@pytest.mark.asyncio
async def test_registered_batch_route_forwards_its_bound_session(monkeypatch):
    session = object()
    observed_calls = []

    async def serve(request, candidate):
        observed_calls.append((request.method, request.path, candidate))
        return response.json({"ok": True})

    monkeypatch.setattr(extension_reads, "serve_custom_import_detail_batch", serve)
    app = Sanic(f"exact-detail-batch-{uuid.uuid4().hex}")

    @app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = session

    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _sent, reply = await app.asgi_client.post(batch.CUSTOM_IMPORT_DETAIL_BATCH_PATH, data=_body())
    assert reply.status_code == 200 and reply.json == {"ok": True}
    assert observed_calls == [("POST", batch.CUSTOM_IMPORT_DETAIL_BATCH_PATH, session)]


@pytest.mark.asyncio
async def test_ordinary_batch_projects_complete_roots_without_expanding_query_pages(monkeypatch):
    fixtures._install_keyring(monkeypatch)
    context = query_fixture._context()
    context = replace(
        context, definition=replace(context.definition, query=replace(context.definition.query, root_fields=("npi",)))
    )
    winner = SimpleNamespace(entity_binding_id=1, context_key_sha256=b"a" * 32)
    family = SimpleNamespace(family_revision_id=2, root_record_id=3, child_count=0)
    selected_rows = ((winner, family, SimpleNamespace(root_revision_id=4), None, _VALUES[0]),)
    session = _Session()
    session.execute = AsyncMock(return_value=SimpleNamespace(all=lambda: selected_rows, first=lambda: None))

    @asynccontextmanager
    async def bounded(_session, *, timeout_ms):
        yield

    root_scalars_by_key = {
        (4, slot): SimpleNamespace(field_type="string", value_state="value", string_value=scalar_value)
        for slot, scalar_value in ((1, _VALUES[0]), (2, "Synthetic Complete"))
    }
    monkeypatch.setattr(batch, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(transport, "_resolve_pinned_target", AsyncMock(return_value=context.target))
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    monkeypatch.setattr(read_core, "_root_scalar_rows", AsyncMock(return_value=root_scalars_by_key))
    monkeypatch.setattr(read_core, "_family_child_rows", AsyncMock(return_value=()))
    monkeypatch.setattr(read_core, "_child_scalar_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(batch, "verify_published_generation", AsyncMock())
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())

    reply = await batch.serve_custom_import_detail_batch(_request(), session)
    assert reply.status == 200
    provider_items = json.loads(reply.body)["items"]
    assert [provider_item["npi"] for provider_item in provider_items] == list(_VALUES)
    assert provider_items[1]["custom_import"] is None
    full_root = provider_items[0]["custom_import"]["root_fields"]
    assert [(field["field_id"], field["value"]) for field in full_root] == [
        ("npi", _VALUES[0]),
        ("display_name", "Synthetic Complete"),
    ]
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    authorization = read_core.ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    projected = await service.hydrate_npi_page(
        session, authorization=authorization, pinned_target=context.target, prepared=prepared, entity_values=_VALUES
    )
    assert [field.field_id for field in projected[_VALUES[0]].root_fields] == ["npi"]
    assert read_core.verify_published_generation.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(("full_family", "has_distinct_families"), ((True, True), (True, False), (False, True)))
async def test_ordinary_detail_ambiguity_is_checked_before_hydration(monkeypatch, full_family, has_distinct_families):
    """Full detail rejects separate families and preserves legacy search projection."""

    context = query_fixture._context()
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    winner = SimpleNamespace(entity_binding_id=1, context_key_sha256=b"a" * 32)
    family = SimpleNamespace(family_revision_id=2, root_record_id=3, child_count=0)
    selected_rows = ((winner, family, SimpleNamespace(root_revision_id=4), None, _VALUES[0]),)
    query_result = SimpleNamespace(
        all=lambda: selected_rows, first=lambda: (_VALUES[0],) if has_distinct_families else None
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=query_result))

    @asynccontextmanager
    async def bounded(_session, *, timeout_ms):
        yield

    complete = AsyncMock(return_value=(family,))
    projected = AsyncMock(return_value=(family,))
    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    monkeypatch.setattr(read_core, "_hydrate_complete_family_entities", complete)
    monkeypatch.setattr(read_core, "_hydrate_provider_page_items", projected)
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    authorization = read_core.ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    hydration = service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=context.target,
        prepared=prepared,
        entity_values=_VALUES,
        full_family=full_family,
    )
    if full_family and has_distinct_families:
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="selected entity is not eligible"):
            await hydration
        assert session.execute.await_count == 1
        complete.assert_not_awaited()
        projected.assert_not_awaited()
        read_core.verify_published_generation.assert_not_awaited()
    else:
        assert await hydration == {_VALUES[0]: family}
        assert complete.await_count == int(full_family)
        assert projected.await_count == int(not full_family)
        assert session.execute.await_count == 1 + int(full_family)
    if full_family:
        guard_sql = str(session.execute.await_args_list[0].args[0].compile(dialect=postgresql.dialect()))
        identity = re.search(r"count\(distinct\(\(([^)]+)\)\)\)", guard_sql)
        assert identity is not None, guard_sql
        assert tuple(column.strip().rsplit(".", 1)[-1] for column in identity.group(1).split(",")) == (
            "root_record_id",
            "family_revision_id",
            "entity_binding_id",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(("full_family", "size"), ((None, 0), (0, 0), (1, 0), ("full_family", 0), (True, 51)))
async def test_complete_page_flag_and_bound_are_checked_before_storage(full_family, size):
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(read_core.CustomImportReadRequestError):
        await service.hydrate_npi_page(
            session,
            authorization=read_core.ExtensionReadAuthorization("synthetic"),
            pinned_target=query_fixture._target(),
            prepared=None,
            entity_values=tuple(str(1000000000 + index) for index in range(size)),
            full_family=full_family,
        )
    session.execute.assert_not_awaited()


def test_custom_import_extension_detail_batch_transport_contract():
    spec = load_openapi_document(Path("doc/openapi.yaml"))
    operation = spec["paths"]["/extensions/custom-import/detail/batch"]["post"]
    detail = spec["paths"]["/extensions/custom-import/detail"]["post"]

    assert "healthporta.custom-import-extension-read-transport.v1" in operation["description"]
    assert "transport.v2" not in operation["description"]
    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/CustomImportDetailBatchRequest"
    }
    assert len(operation["parameters"]) == 3
    for parameter, existing in zip(operation["parameters"], detail["parameters"], strict=True):
        for key in ("name", "in", "required", "schema"):
            assert parameter[key] == existing[key]
    assert set(operation["responses"]) == {"200", "400", "404", "503"}
    for status, response in operation["responses"].items():
        expected = "CustomImportDetailBatchResponse" if status == "200" else "CustomImportReadErrorResponse"
        assert response["headers"]["Cache-Control"] == {"$ref": "#/components/headers/CustomImportReadCacheControl"}
        assert response["content"]["application/json"]["schema"] == {"$ref": f"#/components/schemas/{expected}"}
    cache_control = spec["components"]["headers"]["CustomImportReadCacheControl"]
    assert cache_control["schema"]["enum"] == ["private, no-store"]
    assert "null item in a 200 response" in operation["responses"]["404"]["description"]


def test_custom_import_extension_detail_batch_request_contract():
    schemas = load_openapi_document(Path("doc/openapi.yaml"))["components"]["schemas"]
    request = schemas["CustomImportDetailBatchRequest"]
    properties = request["properties"]
    entities = properties["entities"]
    npi_values_schema = entities["properties"]["values"]

    assert request["additionalProperties"] is False
    assert request["required"] == ["target", "entities", "family_entitlement"]
    assert set(properties) == {
        "target",
        "entities",
        "family_entitlement",
        "grouped_entity_selection",
        "context",
        "grouped_child_query",
    }
    assert properties["target"] == {"$ref": "#/components/schemas/CustomImportReadTarget"}
    assert properties["family_entitlement"] == {
        "$ref": "#/components/schemas/CustomImportDetailRequest/properties/family_entitlement"
    }
    assert request["anyOf"] == [
        {"required": ["grouped_entity_selection"]},
        {"not": {"anyOf": [{"required": ["context"]}, {"required": ["grouped_child_query"]}]}},
    ]
    assert entities["additionalProperties"] is False
    assert entities["required"] == ["adapter_id", "values"]
    assert entities["properties"]["adapter_id"] == {"type": "string", "enum": ["npi"]}
    assert (npi_values_schema["minItems"], npi_values_schema["maxItems"], npi_values_schema["uniqueItems"]) == (
        1,
        50,
        True,
    )
    assert npi_values_schema["items"] == {"$ref": "#/components/schemas/CustomImportNpiValue"}
    npi = schemas["CustomImportNpiValue"]
    assert (npi["type"], npi["minLength"], npi["maxLength"], npi["pattern"]) == (
        "string",
        10,
        10,
        "^[0-9]{10}$",
    )
    assert re.fullmatch(npi["pattern"], "1000000001")
    assert re.fullmatch(npi["pattern"], "１００００００００１") is None
    context = properties["context"]
    assert context["maxItems"] == 3
    assert context["items"] == {"$ref": "#/components/schemas/CustomImportReadFilter"}
    for name in ("CustomImportGroupedEntitySelection", "CustomImportGroupedChildQuery"):
        assert schemas[name]["additionalProperties"] is False
        assert set(schemas[name]["required"]) == set(schemas[name]["properties"])
    assert properties["grouped_entity_selection"] == {"$ref": "#/components/schemas/CustomImportGroupedEntitySelection"}
    assert properties["grouped_child_query"] == {"$ref": "#/components/schemas/CustomImportGroupedChildQuery"}


def test_custom_import_extension_detail_batch_response_contract():
    schemas = load_openapi_document(Path("doc/openapi.yaml"))["components"]["schemas"]
    response = schemas["CustomImportDetailBatchResponse"]
    batch_items_schema = response["properties"]["items"]
    batch_item_schema = batch_items_schema["items"]
    grouped = schemas["CustomImportGroupedFullFamilySet"]

    assert response["additionalProperties"] is False
    assert response["required"] == ["target", "items"]
    assert set(response["properties"]) == {"target", "items"}
    assert response["properties"]["target"] == {"$ref": "#/components/schemas/CustomImportReadTarget"}
    assert (batch_items_schema["minItems"], batch_items_schema["maxItems"]) == (1, 50)
    assert "request order" in batch_items_schema["description"]
    assert batch_item_schema["additionalProperties"] is False
    assert batch_item_schema["required"] == ["npi", "custom_import"]
    assert set(batch_item_schema["properties"]) == {"npi", "custom_import"}
    assert batch_item_schema["properties"]["npi"] == {"$ref": "#/components/schemas/CustomImportNpiValue"}
    assert batch_item_schema["properties"]["custom_import"]["oneOf"] == [
        {"$ref": "#/components/schemas/CustomImportRootDetail"},
        {"$ref": "#/components/schemas/CustomImportGroupedFullFamilySet"},
        {"type": "object", "nullable": True, "enum": [None]},
    ]
    assert grouped["additionalProperties"] is False
    assert set(grouped["required"]) == set(grouped["properties"])
    assert grouped["properties"]["projection"] == {"type": "string", "enum": ["full_family"]}
    families = grouped["properties"]["families"]
    assert (families["minItems"], families["maxItems"]) == (1, 2)
    assert families["items"]["additionalProperties"] is False
    assert families["items"]["required"] == ["group_value", "root_fields", "children"]
    for field in ("root_fields", "children"):
        assert families["items"]["properties"][field] == {
            "$ref": f"#/components/schemas/CustomImportRootDetail/properties/{field}"
        }
