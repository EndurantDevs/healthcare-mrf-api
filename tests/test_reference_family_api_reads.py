# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in claims setup preserves native checks under guarded buffered reads.

Catalog/authority leaves are explicit doubles; these tests provide no native proof.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import reference_family_reads as reads
from process import mrf_address_publication as publication
from process import network_read_identity as identity
from process import reference_family_archive as archive


class _Result:
    def __init__(self, session, value):
        self.session, self.value = session, value
        self.before_guard = session.guards
        self.closed = False

    def scalar(self):
        assert self.session.guards > self.before_guard
        return self.value

    def all(self):
        assert self.session.guards > self.before_guard
        return self.value

    def __iter__(self):
        return iter(self.all())

    def close(self):
        self.closed = True


class _Driver:
    def __init__(self, session):
        self.session, self.pid, self.active = session, 42, True
        self.modes = ("repeatable read", "on")

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
        return {
            "backend_matches": self.pid == pid,
            "relation_count": len(names),
            "valid": all(self.session.current.get(name) == oid for name, oid in zip(names, oids)),
        }


class _Session:
    def __init__(self, families):
        self.info, self.guards = {"unrelated": "preserved"}, 0
        self.transaction = object()
        self.driver = _Driver(self)
        self.oid_by_name, self.pairs_by_importer = {}, {}
        self.current, self.results, self.statements = {}, [], []
        self.is_writable = self.has_binding_drift = self.has_overlap = False
        self.after_query = None
        self.rollback = AsyncMock()
        ordinal = 100
        for importer in archive.TERMINAL_CAPABILITIES:
            pairs = []
            for name in sorted(archive.reference_family_receive_spec(importer).table_names):
                ordinal += 1
                oid = ordinal if importer in families else None
                self.oid_by_name[("mrf", name)] = oid
                if oid is not None:
                    pairs.append((name, oid))
            if pairs:
                self.pairs_by_importer[importer] = tuple(pairs)
        for schema, names in (
            ("mrf", ("code_catalog", "code_crosswalk")),
            ("hp_snapshot_retention", ("current_generation", "relation")),
        ):
            for name in names:
                ordinal += 1
                self.oid_by_name[(schema, name)] = ordinal
        self.current = {f'"{schema}"."{name}"': oid for (schema, name), oid in self.oid_by_name.items()}

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
        assert execution._SESSION_INVENTORY_KEY in self.info
        if "SELECT c.generation_id" in sql:
            pairs = self.pairs_by_importer[parameters["importer"]]
            if self.has_binding_drift:
                pairs = pairs[:-1]
            buffered = _Result(self, [("protected-generation", name, oid) for name, oid in pairs])
        elif "native_catalog" in sql:
            buffered = _Result(self, True)
        elif "left_row" in sql:
            buffered = _Result(self, self.has_overlap)
        elif "display_name" in sql:
            buffered = _Result(self, "Synthetic" if "LIMIT" in sql else [("CPT", "00001")])
        else:
            buffered = _Result(self, not self.is_writable)
        self.results.append(buffered)
        if self.after_query:
            self.after_query(sql)
        return buffered


def _reader(monkeypatch, families=("claims-pricing",)):
    session = _Session(families)

    async def oid(_session, schema, name):
        return session.oid_by_name.get((schema, name))

    async def catalog(adapter, oids):
        assert type(adapter) is reads._ClaimsAPISetup and adapter._session is session
        assert all(type(oid) is int and oid > 0 for oid in oids)
        assert await adapter.scalar("SELECT native_catalog") is True

    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(side_effect=oid))
    proof = AsyncMock(side_effect=catalog)
    monkeypatch.setattr(publication, "require_native_read_catalog", proof)
    return session, proof


@pytest.mark.parametrize("families", [(), ("claims-pricing",), ("drug-claims",), ("claims-pricing", "drug-claims")])
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_api_inventory_contains_every_installed_and_absent_name(monkeypatch, families, isolation):
    session, proof = _reader(monkeypatch, families)
    session.driver.modes = (isolation, "on")
    expected = tuple(sorted(pair for pairs in session.pairs_by_importer.values() for pair in pairs)) or None
    assert await reads.pin_claims_reader(session, api_read=True) == expected
    installed = execution._installed_read_inventory(session)
    actual_oid_by_name = {(pin.schema_name, pin.table_name): pin.table_oid for pin in installed.relations}
    actual_oid_by_name.update({coordinates: None for coordinates in installed.absent_names})
    expected_oid_by_name = {
        name: oid for name, oid in session.oid_by_name.items() if families or name[0] != "hp_snapshot_retention"
    }
    assert actual_oid_by_name == expected_oid_by_name
    assert installed.connection is session.driver and installed.backend_pid == 42
    session.rollback.assert_not_awaited()
    if families:
        assert proof.await_count == len(families) + 2
        catalog, crosswalk = reads.claims_dictionary_tables(session)
        assert "code_catalog" in str(catalog) and "code_crosswalk" in str(crosswalk)
        assert ("drug_claims_code_catalog" in str(catalog)) is ("drug-claims" in families)
    else:
        proof.assert_not_awaited()
        assert "snapshot_claims_dictionary_tables" not in session.info
        assert all(call.args[1] != "hp_snapshot_retention" for call in archive._relation_oid.await_args_list)
    assert session.info["unrelated"] == "preserved"


@pytest.mark.parametrize("invalid", [None, 0, 1, "true", []])
async def test_api_mode_requires_an_explicit_boolean(invalid):
    with pytest.raises(ValueError, match="mode"):
        await reads.pin_claims_reader(object(), api_read=invalid)


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off"), ("serializable", "off"), None])
async def test_api_requires_native_read_only_snapshot_before_discovery(monkeypatch, modes):
    session, proof = _reader(monkeypatch)
    session.driver.modes = modes
    with pytest.raises(RuntimeError, match="read-only"):
        await reads.pin_claims_reader(session, api_read=True)
    archive._relation_oid.assert_not_awaited()
    proof.assert_not_awaited()
    assert session.statements == [] and execution._SESSION_INVENTORY_KEY not in session.info


@pytest.mark.parametrize("failure", ["postgres", "interface", "timeout", "closed", "cancel"])
async def test_native_probe_failures_keep_the_reader_error_contract(monkeypatch, failure):
    session, proof = _reader(monkeypatch)
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
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError) as caught:
        await reads.pin_claims_reader(session, api_read=True)
    if failure == "cancel":
        assert caught.value is error
    archive._relation_oid.assert_not_awaited()
    proof.assert_not_awaited()
    assert session.statements == [] and execution._SESSION_INVENTORY_KEY not in session.info


@pytest.mark.parametrize("phase", ["acquire", "modes"])
async def test_claims_snapshot_cannot_adopt_a_replaced_transaction_before_discovery(monkeypatch, phase):
    session, proof = _reader(monkeypatch)
    original_connection = session.connection
    original_fetch = session.driver.fetchrow

    async def connection():
        acquired = await original_connection()
        if phase == "acquire":
            session.transaction = object()
        return acquired

    async def fetchrow(query, *arguments):
        modes = await original_fetch(query, *arguments)
        session.transaction = object()
        return modes

    session.connection = AsyncMock(side_effect=connection)
    if phase == "modes":
        session.driver.fetchrow = fetchrow
    with pytest.raises(RuntimeError, match="snapshot is unavailable"):
        await reads.pin_claims_reader(session, api_read=True)
    archive._relation_oid.assert_not_awaited()
    proof.assert_not_awaited()
    assert session.statements == [] and execution._SESSION_INVENTORY_KEY not in session.info


@pytest.mark.parametrize("change", ["driver", "pid", "transaction"])
async def test_catalog_discovery_cannot_move_the_original_snapshot(monkeypatch, change):
    session, proof = _reader(monkeypatch)
    original = reads._claims_api_inventory

    async def inventory(actual):
        result = await original(actual)
        if change == "driver":
            session.driver = _Driver(session)
        elif change == "pid":
            session.driver.pid += 1
        else:
            session.transaction = object()
        return result

    monkeypatch.setattr(reads, "_claims_api_inventory", inventory)
    with pytest.raises(RuntimeError, match="snapshot changed"):
        await reads.pin_claims_reader(session, api_read=True)
    proof.assert_not_awaited()
    assert session.statements == [] and execution._SESSION_INVENTORY_KEY not in session.info


@pytest.mark.parametrize("kind", ["incomplete", "binding", "writable", "overlap", "native"])
async def test_existing_native_refusals_precede_overlay_publication(monkeypatch, kind):
    session, proof = _reader(monkeypatch, ("claims-pricing", "drug-claims"))
    if kind == "incomplete":
        session.oid_by_name[("mrf", archive.ClaimsCodeCatalog.__tablename__)] = None
    elif kind == "native":
        proof.side_effect = RuntimeError("native catalog refused")
    else:
        setattr(
            session, {"binding": "has_binding_drift", "writable": "is_writable", "overlap": "has_overlap"}[kind], True
        )
    with pytest.raises(RuntimeError):
        await reads.pin_claims_reader(session, api_read=True)
    assert "snapshot_claims_dictionary_tables" not in session.info
    assert session.info["unrelated"] == "preserved"
    session.rollback.assert_not_awaited()


@pytest.mark.parametrize("change", ["installed", "absent", "binding"])
async def test_setup_name_drift_refuses_buffered_values_before_inspection(monkeypatch, change):
    session, _proof = _reader(monkeypatch)

    def mutate(_sql):
        if change == "installed":
            name = '"mrf"."code_catalog"'
        elif change == "binding":
            name = '"hp_snapshot_retention"."current_generation"'
        else:
            schema, table = next(name for name, oid in session.oid_by_name.items() if oid is None)
            name = f'"{schema}"."{table}"'
        session.current[name] = 999

    session.after_query = mutate
    with pytest.raises(ServiceUnavailable):
        await reads.pin_claims_reader(session, api_read=True)
    assert session.results[-1].closed and "snapshot_claims_dictionary_tables" not in session.info


@pytest.mark.parametrize("drift", [False, True])
async def test_owned_neighbor_payload_uses_the_installed_session_guard(monkeypatch, drift):
    session, _proof = _reader(monkeypatch)
    await reads.pin_claims_reader(session, api_read=True)
    before = session.guards
    if drift:
        session.after_query = lambda _sql: session.current.update({'"mrf"."code_catalog"': 999})
        with pytest.raises(ServiceUnavailable):
            await reads._query_catalog_neighbors(session, {("CPT", "00001")})
        assert session.results[-1].closed
    else:
        assert await reads._query_catalog_neighbors(session, {("CPT", "00001")}) == {("CPT", "00001")}
        assert session.guards == before + 2


async def test_setup_adapter_cannot_read_before_its_inventory_is_installed(monkeypatch):
    session, proof = _reader(monkeypatch)
    with pytest.raises(RuntimeError, match="inventory is unavailable"):
        await reads._ClaimsAPISetup(session).execute("SELECT payload")
    assert session.statements == []
    proof.assert_not_awaited()


async def test_only_scope_contains_native_checked_heaps_and_preserves_uninstalled_tables(monkeypatch):
    session, _proof = _reader(monkeypatch)
    unproved = ("mrf", "pricing_prescription")
    session.oid_by_name[unproved] = 999
    session.current['"mrf"."pricing_prescription"'] = 999
    await reads.pin_claims_reader(session, api_read=True)
    installed = execution._installed_read_inventory(session)
    assert unproved in {(pin.schema_name, pin.table_name) for pin in installed.relations}
    assert unproved not in {(pin.schema_name, pin.table_name) for pin in installed.only_relations}
    assert {pin.table_oid for pin in installed.only_relations} == {
        oid for name, oid in session.oid_by_name.items() if oid is not None and name != unproved
    }
    binding = next(sql for sql in session.statements if "SELECT c.generation_id" in sql)
    assert 'FROM ONLY "hp_snapshot_retention"."current_generation" c' in binding
    assert 'JOIN ONLY "hp_snapshot_retention"."relation" r' in binding


async def test_dictionary_overlap_uses_only_the_native_checked_mirrors(monkeypatch):
    session, _proof = _reader(monkeypatch, ("claims-pricing", "drug-claims"))
    await reads.pin_claims_reader(session, api_read=True)
    overlap_statements = [sql for sql in session.statements if "left_row" in sql]
    assert len(overlap_statements) == 2
    assert all('FROM ONLY "mrf"."claims_code_' in sql for sql in overlap_statements)
    assert all('JOIN ONLY "mrf"."drug_claims_code_' in sql for sql in overlap_statements)


async def test_native_heap_admission_preserves_overlapping_prior_inventory(monkeypatch):
    from process.network_compact_layout import CompactAddressRelation

    session, _proof = _reader(monkeypatch)
    name, oid = session.pairs_by_importer["claims-pricing"][0]
    pin = CompactAddressRelation("mrf", name, oid)
    await execution.install_network_read_inventory(session, (pin,), only_relations=(pin,))
    await reads.pin_claims_reader(session, api_read=True)
    installed = execution._installed_read_inventory(session)
    assert installed.only_relations.count(pin) == 1
    assert installed.relations.count(pin) == 2


@pytest.mark.parametrize("previous", [False, True])
@pytest.mark.parametrize("failure", [RuntimeError("catalog refused"), asyncio.CancelledError()])
async def test_failed_or_cancelled_setup_restores_the_original_inventory(monkeypatch, previous, failure):
    session, proof = _reader(monkeypatch)
    if previous:
        await execution.install_network_read_inventory(session, ())
    previous_info_by_name = dict(session.info)
    proof.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        await reads.pin_claims_reader(session, api_read=True)
    assert caught.value is failure and session.info == previous_info_by_name


@pytest.mark.parametrize("change", ["driver", "pid", "inactive"])
async def test_mode_probe_context_drift_refuses_before_any_catalog_discovery(monkeypatch, change):
    session, proof = _reader(monkeypatch)
    original = session.driver.fetchrow

    async def changed(query, *parameters):
        modes = await original(query, *parameters)
        if change == "driver":
            session.driver = _Driver(session)
        elif change == "pid":
            session.driver.pid += 1
        else:
            session.driver.active = False
        return modes

    session.driver.fetchrow = changed
    with pytest.raises(RuntimeError, match="snapshot is unavailable"):
        await reads.pin_claims_reader(session, api_read=True)
    archive._relation_oid.assert_not_awaited()
    proof.assert_not_awaited()
    assert session.statements == []
