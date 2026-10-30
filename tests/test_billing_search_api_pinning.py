# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in billing pins keep cursor bytes and guard subsequent canonical reads.

Protocol doubles establish physical-identity wiring, not native Source authority.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api import billing_search_pagination as pagination
from api import network_read_execution as execution
from process import network_read_identity as identity
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests import test_billing_search_pagination as fixture


class _Result:
    def __init__(self, session, rows, *, payload=False):
        self.session, self.rows, self.payload = session, rows, payload
        self.before_guard, self.closed = session.guards, False

    def all(self):
        if self.payload:
            assert self.session.guards > self.before_guard
        return self.rows

    def close(self):
        self.closed = True


class _Driver:
    def __init__(self, session):
        self.session, self.pid, self.active = session, 42, True
        self.modes = ("repeatable read", "on")
        self.after_guard = None

    def get_server_pid(self):
        return self.pid

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, query, *arguments):
        if "current_setting('transaction_isolation')" in query:
            return self.modes
        assert query == identity._BOUND_NAMES_SQL
        names, oids, pid = arguments
        self.session.guards += 1
        receipt_by_field = {
            "backend_matches": self.pid == pid,
            "relation_count": len(names),
            "valid": all(self.session.current.get(name) == oid for name, oid in zip(names, oids)),
        }
        if self.after_guard:
            self.after_guard()
        return receipt_by_field


class _Session:
    def __init__(self, schema="mrf", address_oid=1001, evidence_oid=1002):
        self.info, self.transaction, self.guards = {}, object(), 0
        self.driver = _Driver(self)
        self.rows = [("entity_address_evidence", evidence_oid), ("entity_address_unified", address_oid)]
        self.current = {f'"{schema}"."{name}"': oid for name, oid in self.rows}
        self.statements, self.results = [], []
        self.after_catalog = self.after_payload = None
        self.kind, self.persistence = "r", "p"
        self.has_inheritance = self.is_partition = False

    def in_transaction(self):
        return self.driver.active

    def get_transaction(self):
        return self.transaction

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        assert "LOCK TABLE" not in sql and "pg_advisory" not in sql and "WHERE false" not in sql
        if "pg_catalog.pg_class" in sql:
            assert "c.relkind='r'" in sql
            assert "c.relpersistence IN ('p','u')" in sql and "NOT c.relispartition" in sql
            assert "pg_catalog.pg_inherits" in sql and "i.inhrelid=c.oid OR i.inhparent=c.oid" in sql
            assert parameters["names"] == list(pagination._ADDRESS_TABLES)
            is_direct_heap = (
                self.kind == "r"
                and self.persistence in {"p", "u"}
                and not self.has_inheritance
                and not self.is_partition
            )
            result = _Result(self, self.rows if is_direct_heap else [])
            if self.after_catalog:
                self.after_catalog()
        else:
            result = _Result(self, [("billing payload",)], payload=True)
            if self.after_payload:
                self.after_payload()
        self.results.append(result)
        return result


@pytest.mark.parametrize("schema", ["mrf", "billing_fixture"])
@pytest.mark.parametrize("selected", [False, True])
@pytest.mark.parametrize("source_pinned", [False, True])
async def test_api_pin_and_sealed_cursor_bytes_match_the_original_path(monkeypatch, schema, selected, source_pinned):
    monkeypatch.setattr(pagination.ptg2_serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(fixture.cursor.secrets, "token_bytes", lambda size: b"n" * size)
    selection = fixture.source_pinned_selection() if source_pinned else fixture._selection()
    options = {"contributing_bindings": selection.in_network_bindings} if selected else {}
    legacy = AsyncMock()
    legacy.scalar.return_value = (1001, 1002)
    expected = await pagination.capture_billing_search_generation_pin(legacy, selection, **options)
    session = _Session(schema)
    actual = await pagination.capture_billing_search_generation_pin(session, selection, api_read=True, **options)
    assert actual == expected
    inventory = execution._installed_read_inventory(session)
    assert {(pin.schema_name, pin.table_name, pin.table_oid) for pin in inventory.relations} == {
        (schema, "entity_address_unified", 1001),
        (schema, "entity_address_evidence", 1002),
    }
    assert inventory.connection is session.driver and inventory.backend_pid == 42
    old_binding, new_binding = fixture._cursor_binding(pin=expected), fixture._cursor_binding(pin=actual)
    assert old_binding == new_binding
    sort_key = (0, 1.25, 0, fixture.SNAPSHOT_ID, 1396271656, "address-key")
    old_cursor = pagination.seal_billing_search_page_cursor(sort_key, keyring=fixture.KEYRING, binding=old_binding)
    new_cursor = pagination.seal_billing_search_page_cursor(sort_key, keyring=fixture.KEYRING, binding=new_binding)
    assert fixture._wire_token(old_cursor, old_binding) == fixture._wire_token(new_cursor, new_binding)


@pytest.mark.parametrize("invalid", [None, 0, 1, "true", []])
async def test_read_mode_requires_an_explicit_boolean(invalid):
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(object(), fixture._selection(), api_read=invalid)


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off"), None])
async def test_api_capture_requires_a_native_read_only_snapshot(modes):
    session = _Session()
    session.driver.modes = modes
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.statements == [] and session.info == {}


async def test_serializable_read_only_snapshot_is_supported():
    session = _Session()
    session.driver.modes = ("serializable", "on")
    assert (
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
        == fixture._pin()
    )


@pytest.mark.parametrize("missing", [False, True])
async def test_the_complete_exact_address_pair_is_required(missing):
    session = _Session()
    if missing:
        session.rows.pop()
    else:
        session.rows[0] = ("other_evidence", 1002)
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.info == {}


@pytest.mark.parametrize("kind", ["v", "m", "f", "p"])
async def test_only_ordinary_address_heaps_can_be_pinned(kind):
    session = _Session()
    session.kind = kind
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.info == {}


@pytest.mark.parametrize("shape", ["inherited-parent", "inherited-child", "partition-leaf", "temporary"])
async def test_catalog_topology_cannot_add_unlisted_payload_heaps(shape):
    session = _Session()
    session.has_inheritance = shape in {"inherited-parent", "inherited-child"}
    session.is_partition = shape == "partition-leaf"
    session.persistence = "t" if shape == "temporary" else "p"
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.info == {}


async def test_an_unlogged_direct_heap_keeps_the_same_generation_bytes():
    session = _Session()
    session.persistence = "u"
    assert (
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
        == fixture._pin()
    )


@pytest.mark.parametrize("oids", [(None, 1002), (True, 1002), (0, 1002), (-1, 1002), (1001, 1001), (2**32, 1002)])
async def test_missing_or_invalid_physical_oids_never_publish_a_pin(oids):
    session = _Session(address_oid=oids[0], evidence_oid=oids[1])
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    assert session.info == {}


@pytest.mark.parametrize("phase", ["catalog", "install"])
@pytest.mark.parametrize("change", ["driver", "pid", "transaction"])
async def test_original_backend_and_root_transaction_survive_inventory_install(phase, change):
    session = _Session()

    def mutate():
        if change == "driver":
            session.driver = _Driver(session)
        elif change == "pid":
            session.driver.pid += 1
        else:
            session.transaction = object()

    if phase == "catalog":
        session.after_catalog = mutate
    else:
        session.driver.after_guard = mutate
    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)


@pytest.mark.parametrize("drift", [False, True])
async def test_subsequent_buffered_billing_rows_are_guarded_before_cache(drift):
    session, cached_rows = _Session(), []
    await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    if drift:
        session.after_payload = lambda: session.current.update({'"mrf"."entity_address_unified"': 999})
        with pytest.raises(ServiceUnavailable):
            cached_rows.extend((await execution.execute_network_read(session, "SELECT billing_payload")).all())
        assert session.results[-1].closed
    else:
        cached_rows.extend((await execution.execute_network_read(session, "SELECT billing_payload")).all())
    assert cached_rows == ([] if drift else [("billing payload",)])


@pytest.mark.parametrize("failure", ["postgres", "interface", "timeout", "closed", "cancel"])
async def test_native_probe_failures_use_the_existing_billing_error_contract(failure):
    session = _Session()
    error = {
        "postgres": asyncpg.PostgresError("native probe"),
        "interface": asyncpg.InterfaceError("native probe"),
        "timeout": TimeoutError("native probe"),
        "closed": asyncpg.InterfaceError("closed driver"),
        "cancel": asyncio.CancelledError(),
    }[failure]
    if failure == "closed":
        session.connection = AsyncMock(side_effect=error)
    else:
        session.driver.fetchrow = AsyncMock(side_effect=error)
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else PTG2ManifestArtifactError) as caught:
        await pagination.capture_billing_search_generation_pin(session, fixture._selection(), api_read=True)
    if failure == "cancel":
        assert caught.value is error
    assert session.statements == [] and session.info == {}
