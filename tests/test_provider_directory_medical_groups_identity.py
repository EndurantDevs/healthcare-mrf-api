# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-name guards precede Doctors authority, scalar and page interpretation."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import pytest

from api import provider_directory_medical_groups as groups
from api.provider_directory_entities_contract import (
    DirectoryReadError,
    parse_directory_read,
)

_KEY = bytes(range(32))
_ENTITY_ID = str(UUID(int=1))


class _Result:
    def __init__(self, session, kind, value):
        self.session, self.kind, self.value = session, kind, value
        self.closed = False

    def _decode(self):
        self.session.events.append(("decode", self.kind))
        return self.value

    def mappings(self):
        return self

    def one(self):
        return self._decode()

    def one_or_none(self):
        return self._decode()

    def all(self):
        return self._decode()

    def scalar_one(self):
        return self._decode()

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, *, active=True):
        self.active, self.events, self.results = active, [], []
        self.transaction = object()
        self.modes = ("repeatable read", "on")
        self.authority = {
            "local_lineage_id": UUID(int=2),
            "origin_lineage_id": UUID(int=3),
            "local_generation": 4,
            "origin_generation": 5,
            "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "serving_schema": "synthetic",
            "serving_relation_names": list(groups._TABLES),
        }
        self.rows, self.missing_bindings, self.entity_exists = [], False, True

    def in_transaction(self):
        return self.active

    def get_transaction(self):
        return self.transaction if self.active else None

    @asynccontextmanager
    async def begin(self):
        assert self.active is False
        self.active = True
        self.events.append(("begin", None))
        try:
            yield
        finally:
            self.active = False
            self.events.append(("end", None))

    def _response(self, sql, parameters):
        if sql.startswith("SET "):
            return "settings", None
        if "current_setting('transaction_isolation')" in sql:
            return "modes", self.modes
        if f'FROM "synthetic".{groups._AUTHORITY}' in sql:
            return "authority", self.authority
        if "WHERE (nullif" in sql:
            return "completeness", self.missing_bindings
        if "SELECT EXISTS" in sql:
            return "existence", self.entity_exists
        return "page", self.rows

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        assert "LOCK TABLE" not in sql and "pg_advisory" not in sql and "LIMIT 0" not in sql
        kind, value = self._response(sql, parameters)
        result = _Result(self, kind, value)
        self.events.append(("execute", kind))
        self.results.append(result)
        return result


@pytest.fixture(autouse=True)
def _read_context(monkeypatch):
    monkeypatch.setattr(groups, "directory_cursor_key", lambda: _KEY)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic")


def _query(*, kind="medical-groups", shape="entities", generation=None):
    suffix = "&generation_id=" + generation if generation else ""
    entity = _ENTITY_ID if shape != "entities" else None
    return parse_directory_read(
        kind,
        entity,
        shape,
        "source_id=cms-doctors&limit=2" + suffix if shape != "entity" else "source_id=cms-doctors" + suffix,
    )


@pytest.mark.parametrize("owned", [False, True])
@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
async def test_complete_inventory_guards_authority_setup_and_empty_page(monkeypatch, owned, kind):
    session = _Session(active=not owned)
    reader = groups.read_medical_groups if owned else groups.read_medical_groups_snapshot
    response = await reader(session, _query(kind=kind))
    assert response == {
        "generation_id": groups._generation_key(
            _KEY, session.authority, [("synthetic", name) for name in groups._READ_TABLES[:5]]
        ),
        "items": [],
        "next_cursor": None,
    }
    for statement_kind in ("authority", "completeness", "page"):
        assert session.events.index(("execute", statement_kind)) < session.events.index(("decode", statement_kind))
    assert session.active is (not owned)


@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
async def test_relationship_existence_and_page_use_guarded_scalars(monkeypatch, kind):
    session = _Session()
    response = await groups.read_medical_groups_snapshot(session, _query(kind=kind, shape="relationships"))
    assert response["items"] == []
    assert session.events.index(("execute", "existence")) < session.events.index(("decode", "existence"))


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off")])
async def test_borrowed_requires_native_readonly_snapshot_before_catalog(monkeypatch, modes):
    session = _Session()
    session.modes = modes
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "catalog") not in session.events


@pytest.mark.parametrize(
    "authority",
    [
        None,
        {"local_generation": 0},
        pytest.param({"published_at": datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)}, id="authority3"),
    ],
)
async def test_catalog_inventory_does_not_substitute_for_accepted_authority(monkeypatch, authority):
    session = _Session()
    session.authority = None if authority is None else {**session.authority, **authority}
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "page") not in session.events


@pytest.mark.parametrize("failure,status", [("generation", 409), ("missing-binding", 503), ("missing-entity", 404)])
async def test_existing_refusal_semantics_remain_after_guard(monkeypatch, failure, status):
    session = _Session()
    session.missing_bindings = failure == "missing-binding"
    query = _query(
        shape="entity" if failure == "missing-entity" else "entities",
        generation="gen_changed" if failure == "generation" else None,
    )
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, query)
    assert caught.value.status == status


async def test_cancel_preserves_borrowed_transaction_and_resets_scope(monkeypatch):
    session = _Session()

    execute = session.execute

    async def cancel(statement, parameters=None):
        if groups._AUTHORITY in str(statement):
            raise asyncio.CancelledError
        return await execute(statement, parameters)

    monkeypatch.setattr(session, "execute", cancel)
    with pytest.raises(asyncio.CancelledError):
        await groups.read_medical_groups_snapshot(session, _query())
    assert session.active is True


async def test_serializable_readonly_snapshot_is_supported(monkeypatch):
    session = _Session()
    session.modes = ("serializable", "on")
    assert (await groups.read_medical_groups_snapshot(session, _query()))["items"] == []


@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
@pytest.mark.parametrize(
    "recorded",
    [("another_schema", list(groups._TABLES)), ("synthetic", list(groups._TABLES[:2])), ("synthetic", None)],
)
async def test_ledger_naming_other_relations_refuses_group_and_site_reads(monkeypatch, kind, recorded):
    session = _Session()
    session.authority = {**session.authority, "serving_schema": recorded[0], "serving_relation_names": recorded[1]}
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query(kind=kind))
    assert caught.value.status == 503
    assert ("execute", "page") not in session.events


@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
async def test_ledger_written_before_named_publication_keeps_fixed_relations(monkeypatch, kind):
    session = _Session()
    session.authority = {**session.authority, "serving_schema": None, "serving_relation_names": None}
    response = await groups.read_medical_groups_snapshot(session, _query(kind=kind))
    assert response["items"] == [] and response["generation_id"].startswith("gen_")
