# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Company search guards and genuine PostgreSQL compilation without a database."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from api.endpoint import registry_management as routes
from process import registry_record_store as store


def _statement(**changes):
    arguments_by_name = {"q": "Example", "limit": 50, "offset": 0, "record_ids": None, "schema": "example_registry"}
    arguments_by_name |= changes
    query = arguments_by_name.pop("q")
    return store._company_search_statement(query, **arguments_by_name)


@pytest.mark.parametrize("q", [None, True, 1, "", "  ", "x" * 129, "é" * 65, "x\x00", "x\x85", "\tx", "\ud800"])
def test_invalid_query_refuses_before_sql(q):
    with pytest.raises(ValueError):
        _statement(q=q)


@pytest.mark.parametrize(
    "parameters",
    [
        {"limit": True},
        {"limit": 0},
        {"limit": 101},
        {"limit": "1"},
        {"offset": True},
        {"offset": -1},
        {"offset": 1000001},
        {"offset": "0"},
        {"record_ids": "all"},
        {"record_ids": ()},
        {"record_ids": [None]},
        {"record_ids": [str(UUID(int=0))]},
        {"record_ids": [str(UUID(int=1)).upper().replace("1", "A")]},
        {"record_ids": [str(UUID(int=1))] * 2},
        {"record_ids": [str(UUID(int=i + 1)) for i in range(5001)]},
    ],
)
def test_invalid_page_and_selector_refuse(parameters):
    with pytest.raises(ValueError):
        _statement(**parameters)


def test_literal_query_and_scope_compile_before_page():
    query = " A%_\\é "
    statement = _statement(q=query, limit=3, offset=7, record_ids=[str(UUID(int=1))])
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "company_id IN" in sql and "EXISTS" in sql and "jsonb_array_elements_text" in sql
    assert sql.index("WHERE") < sql.index("ORDER BY") < sql.index("LIMIT") < sql.index("OFFSET")
    assert sql.count("ILIKE") == 2 and "ESCAPE" in sql
    assert list(compiled.params.values()).count("%A\\%\\_\\\\é%") == 2
    assert [UUID(int=1)] in compiled.params.values()
    assert "A%_" not in sql


@pytest.mark.parametrize("canonical", [True, False])
def test_only_canonical_uuid_query_adds_exact_identity(canonical):
    q = "12345678-1234-5678-8123-123456789abc"
    if not canonical:
        q = q.upper()
    compiled = _statement(q=q).compile(dialect=postgresql.dialect())
    assert (UUID(q) in compiled.params.values()) is canonical
    assert "ILIKE" in str(compiled)


def test_empty_scope_stays_empty_and_large_scope_is_bounded():
    empty = _statement(record_ids=[]).compile(dialect=postgresql.dialect(), compile_kwargs={"render_postcompile": True})
    assert "company_id IN" in str(empty) and "1 != 1" in str(empty)
    ids = [str(UUID(int=i + 1)) for i in range(5000)]
    compiled = _statement(q="  " + "é" * 64 + "  ", record_ids=ids).compile(dialect=postgresql.dialect())
    assert len(next(value for value in compiled.params.values() if type(value) is list)) == 5000


@pytest.mark.asyncio
async def test_search_reuses_bulk_assertion_assembly(monkeypatch):
    statements = []
    attached_batches = []

    class Session:
        async def execute(self, statement):
            statements.append(statement)
            return SimpleNamespace(mappings=lambda: [{"company_id": UUID(int=1), "revision": 2}])

    async def attach(session, records, schema):
        attached_batches.append(records)
        records[0]["identifier_assertions"] = [{"synthetic": True}]

    monkeypatch.setattr(store, "_attach_company_assertions", attach)
    records = await store.search_registry_companies(Session(), "Example", limit=1, record_ids=[str(UUID(int=1))])
    assert len(statements) == len(attached_batches) == len(records) == 1
    assert records[0]["company_id"] == str(UUID(int=1)) and records[0]["identifier_assertions"] == [{"synthetic": True}]


def _request(document, *, args=None):
    return SimpleNamespace(
        match_info={},
        args=args or {},
        headers={"Authorization": "Bearer synthetic-company-search"},
        body=json.dumps(document).encode(),
        ctx=SimpleNamespace(sa_session=None),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra", "duplicate", "scalar", "query", "oversized"])
async def test_closed_body_refuses_before_session(monkeypatch, damage):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-company-search")
    document_by_field = {"q": "Example", "limit": 50, "offset": 0, "record_ids": None}
    request = _request(document_by_field)
    match damage:
        case "missing":
            request = _request({key: value for key, value in document_by_field.items() if key != "record_ids"})
        case "extra":
            request = _request(document_by_field | {"actor": "admin"})
        case "duplicate":
            request.body = b'{"q":"first","q":"second","limit":50,"offset":0,"record_ids":null}'
        case "scalar":
            request = _request([])
        case "query":
            request.args = {"limit": "1"}
        case _:
            request.body = b" " * 262145
    result = await routes.search_companies(request)
    assert result.status == 400 and result.headers["Cache-Control"] == "private, no-store"


@pytest.mark.asyncio
async def test_route_accepts_large_scoped_body_and_returns_closed_records(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-company-search")
    document_by_field = {
        "q": "Example",
        "limit": 1,
        "offset": 0,
        "record_ids": [str(UUID(int=i + 1)) for i in range(5000)],
    }
    calls = []

    class Session:
        @asynccontextmanager
        async def begin(self):
            yield

    async def search(session, q, **parameters):
        calls.append((q, parameters))
        return [{"company_id": str(UUID(int=1))}]

    request = _request(document_by_field)
    assert 131072 < len(request.body) < 262144
    request.ctx.sa_session = Session()
    monkeypatch.setattr(routes, "search_registry_companies", search)
    result = await routes.search_companies(request)
    assert result.status == 200 and json.loads(result.body) == {"records": [{"company_id": str(UUID(int=1))}]}
    assert calls == [("Example", {key: value for key, value in document_by_field.items() if key != "q"})]
    assert result.headers["Cache-Control"] == "private, no-store"
