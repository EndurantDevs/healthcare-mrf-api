# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact office pagination, physical budgets and installed API boundaries."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_registry_ptg_scope as routes
from api import ptg2_v4_graph as graph
from process import registry_ptg_office_choices as choices
from process import registry_ptg_office_preparation as preparation
from process.registry_ptg_graph_payload_read import budgeted_graph_payloads
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget, RegistryPTGGraphReadError
from process.registry_ptg_office_preparation_contract import validated_office_choices_envelope
from tests.test_registry_ptg_office_preparation import BINDING, LOCATION, envelope, scope, selected_record
from tests.test_registry_ptg_office_preparation_api import _services


def request_document():
    result = envelope()
    result["selector"] = {name: result["selector"][name] for name in ("scope_id", "client_id", "scope_approval_sha256")}
    result["selector"] |= {"retained_generation_id": None, "after": None, "limit": 20}
    return result


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(source_command={}),
        lambda d: d["selector"].update(limit=True),
        lambda d: d["selector"].update(limit=21),
        lambda d: d["selector"].update(retained_generation_id=0),
        lambda d: d["selector"].update(after="cursor"),
        lambda d: d["selector"].update(retained_generation_id=True),
        lambda d: d["selector"].update(retained_generation_id=3, after="x" * 1025),
        lambda d: d["read_limits"].update(maximum_bytes=True),
        lambda d: d["selector"].update(client_id="system"),
    ],
)
def test_closed_choices_envelope_refuses_authority_and_invalid_bounds(mutation):
    document = request_document()
    mutation(document)
    with pytest.raises((ValueError, PermissionError)):
        validated_office_choices_envelope(document)


def test_cursor_binds_entire_source_command_and_generation():
    selector = request_document()["selector"]
    selector.update(retained_generation_id=3, after=choices._cursor("a" * 64, 3, BINDING))
    assert choices._after(selector, "a" * 64, 3) == BINDING
    for digest, generation in [("b" * 64, 3), ("a" * 64, 4)]:
        with pytest.raises(ValueError, match="selection_changed"):
            choices._after(selector, digest, generation)


def metadata(size=8, **extra):
    return {
        "block_hash": b"a" * 32,
        "format_version": 2,
        "object_kind": "member",
        "codec": "none",
        "raw_byte_count": size,
        "stored_byte_count": size,
        "payload_byte_count": size,
        "block_entry_count": 2,
        **extra,
    }


@pytest.mark.asyncio
async def test_payload_budget_rejects_before_any_payload_query():
    session = SimpleNamespace(execute=AsyncMock())
    budget = RegistryPTGGraphReadBudget(7, 10, 10)
    with pytest.raises(RegistryPTGGraphReadError, match="budget"):
        await budgeted_graph_payloads(session, [metadata()], "fixture", budget, "member", 100)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_payload_budget_reserves_exact_full_batch_before_fetch():
    budget = RegistryPTGGraphReadBudget(16, 10, 10)

    async def execute(statement, parameters):
        assert budget.read_bytes == 8 and budget.read_pages == 1
        assert "payload" in str(statement) and parameters["row_limit"] == 2
        return SimpleNamespace(
            mappings=lambda: SimpleNamespace(all=lambda: [{"block_hash": b"a" * 32, "payload": b"12345678"}])
        )

    rows = await budgeted_graph_payloads(
        SimpleNamespace(execute=execute), [metadata()], "fixture", budget, "member", 100
    )
    assert rows[0]["payload"] == b"12345678"


@pytest.mark.asyncio
@pytest.mark.parametrize("budgeted", [False, True])
async def test_uncached_physical_reads_authenticate_then_cache(monkeypatch, budgeted):
    """Legacy and bounded reads retain authentic bytes through a cache miss and hit."""
    from process.ptg_parts.ptg2_shared_blocks import shared_block_hash

    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(1024))
    raw_payload = b"\x02\x00\x00\x00\x07\x00\x00\x00"
    kind = "v4_npi_groups_exact_members_v1"
    block_hash = shared_block_hash(format_version=2, object_kind=kind, codec="none", payload=raw_payload)
    metadata_by_field = metadata(block_hash=block_hash, object_kind=kind)
    query_calls = []
    budget = RegistryPTGGraphReadBudget(128) if budgeted else None

    async def execute(statement, parameters):
        query_calls.append(str(statement))
        if len(query_calls) == 1:
            if budgeted:
                assert "octet_length(payload) AS payload_byte_count" in query_calls[-1]
                assert budget.read_bytes == 0
                return [metadata_by_field]
            return [{**metadata_by_field, "payload": raw_payload}]
        assert budgeted and len(query_calls) == 2 and budget.read_bytes == len(raw_payload)
        return SimpleNamespace(
            mappings=lambda: SimpleNamespace(all=lambda: [{"block_hash": block_hash, "payload": raw_payload}])
        )

    load_options_by_field = {
        "schema_name": "fixture",
        "object_kind": kind,
        "coordinates": [SimpleNamespace(block_hash=block_hash, entry_count=2)],
        "maximum_raw_bytes": 128,
    }
    with graph.v4_graph_request_scope(read_budget=budget):
        loaded_by_hash = await graph._load_physical_blocks(SimpleNamespace(execute=execute), **load_options_by_field)
        cached_by_hash = await graph._load_physical_blocks(SimpleNamespace(execute=execute), **load_options_by_field)
    assert loaded_by_hash == cached_by_hash
    assert loaded_by_hash[block_hash].payload == raw_payload
    assert graph._decode_member_page(loaded_by_hash[block_hash].payload, entry_count=2) == (2, 7)
    assert len(query_calls) == 1 + int(budgeted)
    if budgeted:
        assert budget.read_bytes == 2 * len(raw_payload) and budget.read_pages == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("codec", "zlib"),
        ("stored_byte_count", 9),
        ("payload_byte_count", 9),
        ("format_version", True),
        ("block_hash", b"a"),
    ],
)
async def test_malformed_cas_metadata_never_fetches_payload(field, value):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(RegistryPTGGraphReadError):
        await budgeted_graph_payloads(
            session, [metadata(**{field: value})], "fixture", RegistryPTGGraphReadBudget(100), "member", 100
        )
    session.execute.assert_not_awaited()


def test_cached_payload_is_charged_and_nested_scope_cannot_replace_budget(monkeypatch):
    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(1024))
    budget = RegistryPTGGraphReadBudget(3)
    cached = graph._CachedPhysicalBlock(b"a" * 32, "member", 2, b"1234")
    graph._PHYSICAL_BLOCK_CACHE.put(cached.block_hash, cached, 128)
    try:
        with graph.v4_graph_request_scope(read_budget=budget):
            with pytest.raises(RegistryPTGGraphReadError):
                graph._cached_physical_blocks({cached.block_hash: SimpleNamespace(entry_count=2)}, "member")
            with pytest.raises(graph.PTG2SharedBlockError):
                with graph.v4_graph_request_scope(read_budget=RegistryPTGGraphReadBudget(100)):
                    pytest.fail("budget replacement was accepted")
    finally:
        graph._PHYSICAL_BLOCK_CACHE = graph._ByteLRU(1024)


@pytest.mark.asyncio
async def test_actual_api_choices_auth_and_linked_service(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    app, service = _services()
    calls = []

    async def method(self, document, *, deadline):
        calls.append((self, document))
        return {"items": [], "next_cursor": None}

    monkeypatch.setattr(preparation.RegistryPTGOfficePreparationService, "office_choices", method)
    request = SimpleNamespace(app=app, headers={}, args={}, query_string="", body=b"not json")
    denied = await routes.read_registry_ptg_office_choices(request)
    assert denied.status == 401 and not calls
    request.headers = {"Authorization": "Bearer synthetic-control"}
    request.body = json.dumps(request_document()).encode()
    allowed = await routes.read_registry_ptg_office_choices(request)
    assert allowed.status == 200 and calls == [(service, request_document())]
    assert allowed.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [None, "before", "after"])
async def test_choices_service_uses_reader_and_reauthorizes_actual_command(monkeypatch, denied):
    _, service = _services()
    document = request_document()
    phases = []
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "original")))

    @asynccontextmanager
    async def transaction(sessions, *, control_schema):
        assert sessions is service.scope_service.reader_sessions
        try:
            yield session
        except BaseException:
            phases.append("rollback")
            raise
        else:
            phases.append("commit")

    command_by_field = {"actual": "published full command"}
    context = object()
    monkeypatch.setattr(preparation, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(preparation, "_source_context", AsyncMock(return_value=(context, command_by_field)))
    monkeypatch.setattr(preparation.capture, "_scope", AsyncMock(return_value=scope()))

    async def authorize(owner, envelope, actual, deadline):
        assert actual is command_by_field and envelope == document
        phase = "before" if not phases else "after"
        phases.append(phase)
        if denied == phase:
            raise PermissionError("denied")

    monkeypatch.setattr(preparation, "_authorize", authorize)

    async def read(*args):
        phases.append("read")
        return {"items": []}

    monkeypatch.setattr(choices, "read_registry_ptg_office_choices", read)
    if denied:
        with pytest.raises(PermissionError):
            await service.office_choices(document)
        assert phases == (["before", "rollback"] if denied == "before" else ["before", "read", "after", "rollback"])
    else:
        assert await service.office_choices(document) == {"items": []}
        assert phases == ["before", "read", "after", "commit"]


@pytest.mark.asyncio
async def test_filtered_page_advances_scanned_cursor_without_fabricating_offices(monkeypatch):
    record = selected_record()["record_json"]
    rows = []
    for index in range(21):
        item = copy.deepcopy(record)
        item["binding_id"] = f"22222222-2222-4222-8222-{index + 1:012d}"
        rows.append({"record_key": item["binding_id"], "record_json": item})
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows)))
    )
    serving = SimpleNamespace(generation_id=3, schema_name="retained")
    monkeypatch.setattr(choices.capture, "_driver", AsyncMock(return_value=object()))
    monkeypatch.setattr(choices, "resolve_network_serving_manifest", AsyncMock(return_value=serving))
    monkeypatch.setattr(choices, "_graph_selections", AsyncMock(return_value=({}, {})))
    result = await choices.read_registry_ptg_office_choices(
        session,
        SimpleNamespace(control_schema="control"),
        scope(),
        {"exact": "command"},
        request_document()["selector"],
        RegistryPTGGraphReadBudget(100),
    )
    assert result["items"] == [] and result["retained_generation_id"] == 3
    pin_by_field = {"after": result["next_cursor"]}
    assert choices._after(pin_by_field, result["source_command_sha256"], 3) == rows[19]["record_key"]
    assert session.execute.await_count == 1


@pytest.mark.parametrize(
    "value,nullable,expected", [(None, True, None), ("", True, ""), ("123 Example Street", False, "123 Example Street")]
)
def test_genuine_labels_preserved(value, nullable, expected):
    assert choices._label(value, 512, nullable=nullable) == expected


@pytest.mark.parametrize("value", [None, "", " " * 3, "x" * 513, "line\nbreak", "é" * 257, " padded", "private\ue000"])
def test_missing_or_unbounded_required_labels_fail_closed(value):
    with pytest.raises(ValueError, match="selection_unresolved"):
        choices._label(value, 512)


@pytest.mark.asyncio
async def test_actual_graph_discovery_batches_exact_npis_and_proves_selected_source_occurrence(monkeypatch):
    selected_by_field = selected_record()
    context = SimpleNamespace(
        source_specification=SimpleNamespace(ptg_schema_name="source"), graph_identity={"snapshot_key": 17}
    )
    record_by_field, site_by_field = choices._binding(
        {"record_key": BINDING, "record_json": selected_by_field["record_json"]}, 3
    )
    replies = [
        [{"npi_key": 9, "npi": 1234567893}],
        [
            {
                name: selected_by_field[name]
                for name in (
                    "npi_key",
                    "provider_group_key",
                    "dense_source_key",
                    "source_record_ordinal",
                    "provider_group_ref",
                )
            }
        ],
    ]
    sql_calls = []

    async def execute(statement, parameters):
        sql_calls.append((str(statement), parameters))
        rows = replies.pop(0)
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    monkeypatch.setattr(choices.source, "_physical_binding", AsyncMock(return_value=None))
    budget = RegistryPTGGraphReadBudget(1000)

    async def reverse(session, **kwargs):
        assert graph._request_io().read_budget is budget
        assert kwargs["relation"] == "npi_groups_exact" and kwargs["owner_keys"] == (9,)
        return {9: (8,)}

    monkeypatch.setattr(choices, "lookup_v4_relation_members", reverse)
    proof = AsyncMock()
    monkeypatch.setattr(choices, "verify_registry_ptg_graph_page", proof)
    selected, npi_keys = await choices._graph_selections(
        SimpleNamespace(execute=execute), context, scope(), [(record_by_field, site_by_field)], budget
    )
    assert npi_keys == {"1234567893": 9} and selected[9]["source_record_ordinal"] == 7
    assert len(sql_calls) == 2 and sql_calls[1][1]["sources"] == (0,)
    assert "ptg2_v3_snapshot_source" in sql_calls[1][0]
    page = proof.await_args.args[2]
    assert page["selected_edges"] == b"\x00\x00\x00\x08\x00\x00\x00\x09"
    assert proof.await_args.kwargs["read_budget"] is budget


@pytest.mark.asyncio
@pytest.mark.parametrize("tampered", [False, True])
async def test_actual_display_join_returns_exact_site_or_refuses_whole_page(tampered):
    site_by_field = {
        "binding_id": BINDING,
        "provider_id": "1234567893",
        "location_id": LOCATION,
        "location_key": "c" * 64,
        "address_row_sha256": "d" * 64,
    }
    selection_by_field = {
        name: selected_record()[name]
        for name in ("binding_id", "binding_revision", "dense_source_key", "source_record_ordinal")
    }
    selection_by_field["binding_id"] = BINDING
    label_by_field = {
        "binding_id": BINDING,
        "entity_name": "Example Provider",
        "npi": None if tampered else "1234567893",
        "first_line": "123 Example Street",
        "second_line": "",
        "second_line_bounded": True,
        "city_name": "Sample City",
        "state_name": "CA",
        "postal_code": "90210",
        "country_code": "US",
    }

    async def execute(statement, parameters):
        sql = str(statement)
        assert "binding.location_id=requested.location_id" in sql
        assert "address.entity_type,address.entity_id" in sql and "to_jsonb(address)" in sql
        assert json.loads(parameters["sites"]) == [site_by_field]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [label_by_field]))

    call = choices._display_items(
        SimpleNamespace(execute=execute),
        SimpleNamespace(schema_name="retained"),
        [site_by_field],
        {BINDING: selection_by_field},
    )
    if tampered:
        with pytest.raises(ValueError, match="selection_unresolved"):
            await call
    else:
        displayed_items = await call
        assert len(displayed_items) == 1 and displayed_items[0]["address"]["location_id"] == LOCATION
        assert displayed_items[0]["provider"] == {"npi": "1234567893", "display_name": "Example Provider"}
        assert displayed_items[0]["address"]["second_line"] == ""


@pytest.mark.asyncio
async def test_actual_post_route_keeps_control_auth_and_rejects_other_methods(monkeypatch):
    from uuid import uuid4

    from sanic import Sanic

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control")
    installed, service = _services()
    method = AsyncMock(return_value={"items": [], "next_cursor": None})
    monkeypatch.setattr(preparation.RegistryPTGOfficePreparationService, "office_choices", method)
    app = Sanic("synthetic_office_choices_" + uuid4().hex)
    prefix = "/control/v1/registry/ptg-source-scopes"
    app.blueprint(routes.blueprint, url_prefix=prefix)
    routes.install_registry_ptg_scope_engine(app, installed.ctx.registry_ptg_scope_engine, office_preparation=service)
    try:
        _, denied = await app.asgi_client.post(prefix + "/office-choices", json=request_document())
        assert denied.status_code == 401 and method.await_count == 0
        _, allowed = await app.asgi_client.post(
            prefix + "/office-choices", json=request_document(), headers={"Authorization": "Bearer synthetic-control"}
        )
        assert allowed.status_code == 200 and method.await_count == 1
        _, wrong_method = await app.asgi_client.get(prefix + "/office-choices")
        assert wrong_method.status_code == 405 and method.await_count == 1
    finally:
        Sanic.unregister_app(app)


@pytest.mark.asyncio
async def test_actual_map_reader_refuses_budget_before_selecting_payload():
    queries = []

    async def execute(statement, parameters):
        queries.append(str(statement))
        assert "octet_length(block.payload) AS payload_byte_count" in queries[-1]
        assert "LIMIT :read_page_limit" in queries[-1]
        return [metadata(object_kind=graph.PTG2_V4_MAP_BLOCK_KIND, coordinate_count=1)]

    with graph.v4_graph_request_scope(read_budget=RegistryPTGGraphReadBudget(1)):
        with pytest.raises(RegistryPTGGraphReadError, match="budget"):
            await graph._query_v4_map_packs(
                SimpleNamespace(execute=execute),
                schema_name="fixture",
                snapshot_key=17,
                object_kind="v4_npi_groups_exact_locators_v1",
                missing_pairs=[(0, 0)],
            )
    assert len(queries) == 1


@pytest.mark.asyncio
async def test_heavy_owner_rejects_fragment_overflow_before_building_coordinates():
    session = SimpleNamespace(execute=AsyncMock())
    with graph.v4_graph_request_scope(read_budget=RegistryPTGGraphReadBudget(100, 2, 2)):
        with pytest.raises(graph.PTG2SharedBlockError, match="read budget"):
            await graph._load_v4_heavy_bitmap_payloads(
                session,
                snapshot_key=17,
                schema_name="fixture",
                relation_manifest=object(),
                heavy_owners={9: SimpleNamespace(fragment_count=10**9)},
            )
    session.execute.assert_not_awaited()
