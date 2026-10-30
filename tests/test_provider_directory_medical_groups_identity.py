# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-name guards precede Doctors authority, scalar and page interpretation."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import network_read_execution as execution
from api import provider_directory_medical_groups as groups
from api.provider_directory_entities_contract import (
    DirectoryReadError,
    parse_directory_read,
)
from process.network_read_identity import NetworkReadIdentityError

_KEY = bytes(range(32))
_ENTITY_ID = str(UUID(int=1))


class _Result:
    def __init__(self, session, kind, value):
        self.session, self.kind, self.value = session, kind, value
        self.requires_guard = execution.has_network_read_inventory()
        self.guarded, self.closed = False, False

    def _decode(self):
        assert not self.requires_guard or self.guarded
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


def _catalog():
    return [
        {
            "name": name,
            "oid": 101 + index,
            "relkind": "r",
            "relpersistence": "p",
            "heap": True,
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "relispartition": False,
            "inherited": False,
            "rules": False,
        }
        for index, name in enumerate(groups._READ_TABLES)
    ]


class _Session:
    def __init__(self, *, active=True):
        self.active, self.events, self.results = active, [], []
        self.transaction = object()
        self.modes, self.catalog = ("repeatable read", "on"), _catalog()
        self.authority = {
            "local_lineage_id": UUID(int=2),
            "origin_lineage_id": UUID(int=3),
            "local_generation": 4,
            "origin_generation": 5,
            "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "relation_oids": [101, 102, 103],
        }
        self.rows, self.missing_bindings, self.entity_exists = [], False, True
        self.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: self.active)
        self.connection = AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(side_effect=lambda: SimpleNamespace(driver_connection=self.driver))
            )
        )

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
        if "pg_catalog.pg_class" in sql:
            assert parameters == {"names": list(groups._READ_TABLES), "schema": "synthetic"}
            return "catalog", self.catalog
        if "published_at, relation_oids" in sql:
            return "authority", self.authority
        if "WHERE (nullif" in sql:
            return "completeness", self.missing_bindings
        if "SELECT EXISTS" in sql:
            return "existence", self.entity_exists
        return "page", self.rows

    async def execute(self, statement, parameters):
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


def _verify(monkeypatch, session, *, refuse=None):
    async def verify(driver, relations, *, expected_backend_pid):
        assert driver is session.driver and expected_backend_pid == 42
        assert tuple(pin.table_name for pin in relations) == groups._READ_TABLES
        assert tuple(pin.schema_name for pin in relations) == ("synthetic",) * 6
        current = session.results[-1]
        session.events.append(("guard", current.kind))
        if current.kind == refuse:
            raise NetworkReadIdentityError("changed_relation")
        current.guarded = True

    monkeypatch.setattr(execution, "verify_bound_network_names", verify)


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
    _verify(monkeypatch, session)
    reader = groups.read_medical_groups if owned else groups.read_medical_groups_snapshot
    response = await reader(session, _query(kind=kind))
    assert response == {
        "generation_id": groups._generation_key(_KEY, session.authority, [101, 102, 103, 104, 105]),
        "items": [],
        "next_cursor": None,
    }
    for statement_kind in ("authority", "completeness", "page"):
        assert session.events.index(("execute", statement_kind)) < session.events.index(("guard", statement_kind))
        assert session.events.index(("guard", statement_kind)) < session.events.index(("decode", statement_kind))
    assert session.active is (not owned)
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
async def test_relationship_existence_and_page_use_guarded_scalars(monkeypatch, kind):
    session = _Session()
    _verify(monkeypatch, session)
    response = await groups.read_medical_groups_snapshot(session, _query(kind=kind, shape="relationships"))
    assert response["items"] == []
    assert session.events.index(("guard", "existence")) < session.events.index(("decode", "existence"))


@pytest.mark.parametrize(
    "refuse,shape",
    [("authority", "entities"), ("completeness", "entities"), ("page", "entity"), ("existence", "relationships")],
)
async def test_identity_refusal_precedes_authority_empty_or_missing_interpretation(monkeypatch, refuse, shape):
    session = _Session()
    session.entity_exists = False
    _verify(monkeypatch, session, refuse=refuse)
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query(shape=shape))
    assert caught.value.status == 503
    assert ("decode", refuse) not in session.events
    assert session.results[-1].closed is True
    assert not execution.has_network_read_inventory()


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off")])
async def test_borrowed_requires_native_readonly_snapshot_before_catalog(monkeypatch, modes):
    session = _Session()
    session.modes = modes
    _verify(monkeypatch, session)
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "catalog") not in session.events


@pytest.mark.parametrize("native_active,pid", [(False, 42), (True, True), (True, 0), (True, 2**31)])
async def test_original_native_backend_is_required_before_catalog(native_active, pid):
    session = _Session()
    session.driver = SimpleNamespace(is_in_transaction=lambda: native_active, get_server_pid=lambda: pid)
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "catalog") not in session.events


@pytest.mark.parametrize(
    "changed",
    [
        {"oid": None},
        {"oid": True},
        {"oid": 2**32},
        {"relkind": "v"},
        {"relpersistence": "u"},
        {"heap": False},
        {"relrowsecurity": True},
        {"relforcerowsecurity": True},
        {"relispartition": True},
        {"inherited": True},
        {"rules": True},
    ],
)
async def test_every_inventory_relation_requires_ordinary_native_heap(changed):
    session = _Session()
    session.catalog[-1].update(changed)
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "authority") not in session.events


@pytest.mark.parametrize("change", ["missing", "reordered", "duplicate"])
async def test_inventory_is_complete_and_ordered(change):
    session = _Session()
    if change == "missing":
        session.catalog.pop()
    elif change == "reordered":
        session.catalog.reverse()
    else:
        session.catalog[-1] = session.catalog[0]
    with pytest.raises(DirectoryReadError):
        await groups.read_medical_groups_snapshot(session, _query())
    assert ("execute", "authority") not in session.events


async def test_authority_pin_does_not_change_original_five_oid_generation(monkeypatch):
    session = _Session()
    session.catalog[-1]["oid"] = 999
    _verify(monkeypatch, session)
    response = await groups.read_medical_groups_snapshot(session, _query())
    assert response["generation_id"] == groups._generation_key(_KEY, session.authority, [101, 102, 103, 104, 105])


@pytest.mark.parametrize(
    "authority",
    [
        None,
        {"local_generation": 0},
        {"relation_oids": [101, 102, 999]},
        {"published_at": datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)},
    ],
)
async def test_catalog_inventory_does_not_substitute_for_accepted_authority(monkeypatch, authority):
    session = _Session()
    session.authority = None if authority is None else {**session.authority, **authority}
    _verify(monkeypatch, session)
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "page") not in session.events


@pytest.mark.parametrize("failure,status", [("generation", 409), ("missing-binding", 503), ("missing-entity", 404)])
async def test_existing_refusal_semantics_remain_after_guard(monkeypatch, failure, status):
    session = _Session()
    session.missing_bindings = failure == "missing-binding"
    _verify(monkeypatch, session)
    query = _query(
        shape="entity" if failure == "missing-entity" else "entities",
        generation="gen_changed" if failure == "generation" else None,
    )
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, query)
    assert caught.value.status == status
    assert not execution.has_network_read_inventory()


async def test_cancel_preserves_borrowed_transaction_and_resets_scope(monkeypatch):
    session = _Session()

    async def cancel(*_arguments, **_keywords):
        raise asyncio.CancelledError

    monkeypatch.setattr(execution, "verify_bound_network_names", cancel)
    with pytest.raises(asyncio.CancelledError):
        await groups.read_medical_groups_snapshot(session, _query())
    assert session.active is True
    assert not execution.has_network_read_inventory()


async def test_serializable_readonly_snapshot_is_supported(monkeypatch):
    session = _Session()
    session.modes = ("serializable", "on")
    _verify(monkeypatch, session)
    assert (await groups.read_medical_groups_snapshot(session, _query()))["items"] == []


async def test_missing_native_driver_interface_refuses_before_catalog():
    session = _Session()
    session.driver = object()
    with pytest.raises(DirectoryReadError) as caught:
        await groups.read_medical_groups_snapshot(session, _query())
    assert caught.value.status == 503
    assert ("execute", "catalog") not in session.events
