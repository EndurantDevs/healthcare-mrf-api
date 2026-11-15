# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained read grants on PostgreSQL: generations sharing one base keep the grants the others still need."""

import importlib.util
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import network_retained_read_access as retained
from process import network_serving_read as serving
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation

pytestmark = pytest.mark.asyncio
_MIGRATION = Path(__file__).parents[1] / "alembic/versions/20261115110000_network_retained_read_grant_ledger.py"


def _ledger_ddl(schema):
    specification = importlib.util.spec_from_file_location("retained_read_grant_ledger", _MIGRATION)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration._ddl(schema)


@asynccontextmanager
async def _grant_database():
    """Create a shared base, two retained generations, a control ledger and one reader."""
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select the test database")
    token = uuid4().hex[:12]
    names = SimpleNamespace(
        control="grant_ctl_" + token,
        base="grant_base_" + token,
        older="grant_old_" + token,
        newer="grant_new_" + token,
        owner="grant_owner_" + token,
        reader="grant_reader_" + token,
    )
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        await connection.execute(f'CREATE ROLE "{names.owner}" NOLOGIN; CREATE ROLE "{names.reader}" NOLOGIN')
        await connection.execute(f'CREATE SCHEMA "{names.control}"')
        for statement in _ledger_ddl(names.control):
            await connection.execute(statement)
        for schema in (names.base, names.older, names.newer):
            await connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{names.owner}"')
        await connection.execute(f'CREATE TABLE "{names.base}".entity_address_unified(location_key text)')
        await connection.execute(f"INSERT INTO \"{names.base}\".entity_address_unified VALUES ('office')")
        for schema in (names.older, names.newer):
            await connection.execute(f'CREATE TABLE "{schema}".network_membership(location_key text)')
        for schema, table in (
            (names.base, "entity_address_unified"),
            (names.older, "network_membership"),
            (names.newer, "network_membership"),
        ):
            await connection.execute(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{names.owner}"')
        yield connection, names
    finally:
        for schema in (names.newer, names.older, names.base, names.control):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for role in (names.reader, names.owner):
            await connection.execute(f'DROP ROLE IF EXISTS "{role}"')
        await connection.close()


def _generation(names, schema, digit):
    """Describe one retained generation whose query relations include the shared base table."""
    base = CompactAddressRelation(names.base, "entity_address_unified")
    return SimpleNamespace(
        schema_name=schema,
        manifest_sha256=digit * 64,
        address_relation=base,
        candidate_relation_pins=(CompactAddressRelation(schema, "network_membership"), base),
    )


async def _change(connection, monkeypatch, names, generation, *, revoke=False):
    monkeypatch.setattr(serving, "resolve_retained_network_address_source", _resolver(generation))
    operation = retained.revoke_retained_read_access if revoke else retained.install_retained_read_access
    async with connection.transaction(isolation="repeatable_read"):
        await operation(
            connection,
            PinnedAddressSource(generation.schema_name, "entity_address_unified", generation.manifest_sha256),
            reader_role=names.reader,
            control_schema=names.control,
        )


def _resolver(generation):
    async def resolve(_connection, _source, *, control_schema):
        return generation

    return resolve


async def _readable(connection, names):
    """Report which relations the reader can actually query, using fresh unprepared statements."""
    readable_by_schema = {}
    for schema, table in (
        (names.base, "entity_address_unified"),
        (names.older, "network_membership"),
        (names.newer, "network_membership"),
    ):
        try:
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{names.reader}"')
                await connection.execute(f'SELECT count(*) FROM "{schema}"."{table}"')
            readable_by_schema[schema] = True
        except asyncpg.InsufficientPrivilegeError:
            readable_by_schema[schema] = False
    return readable_by_schema


async def _ledger_rows(connection, names):
    return await connection.fetchval(f'SELECT count(*) FROM "{names.control}".network_retained_read_grant')


async def test_revoking_one_generation_keeps_the_shared_base_for_the_other(monkeypatch):
    async with _grant_database() as (connection, names):
        older, newer = _generation(names, names.older, "a"), _generation(names, names.newer, "b")
        await _change(connection, monkeypatch, names, older)
        await _change(connection, monkeypatch, names, newer)
        await _change(connection, monkeypatch, names, newer)  # A repeated install keeps one ledger row.
        assert await _ledger_rows(connection, names) == 2
        assert await _readable(connection, names) == {names.base: True, names.older: True, names.newer: True}
        await _change(connection, monkeypatch, names, older, revoke=True)
        assert await _readable(connection, names) == {names.base: True, names.older: False, names.newer: True}
        await _change(connection, monkeypatch, names, newer, revoke=True)
        assert await _readable(connection, names) == {names.base: False, names.older: False, names.newer: False}
        assert await _ledger_rows(connection, names) == 0


async def test_revoking_a_pre_ledger_grant_keeps_what_a_recorded_generation_needs(monkeypatch):
    async with _grant_database() as (connection, names):
        older, newer = _generation(names, names.older, "a"), _generation(names, names.newer, "b")
        # A grant made before the ledger existed has native privileges but no ledger row.
        for schema, table in ((names.base, "entity_address_unified"), (names.older, "network_membership")):
            await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{names.reader}"')
            await connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{names.reader}"')
        await _change(connection, monkeypatch, names, newer)
        assert await _ledger_rows(connection, names) == 1
        await _change(connection, monkeypatch, names, older, revoke=True)
        assert await _readable(connection, names) == {names.base: True, names.older: False, names.newer: True}
        await _change(connection, monkeypatch, names, newer, revoke=True)
        assert await _readable(connection, names) == {names.base: False, names.older: False, names.newer: False}
        assert await _ledger_rows(connection, names) == 0


async def _stale_revoke(connection, monkeypatch, names, older, newer):
    """Retain the first transaction's old snapshot across a separately committed installation."""
    concurrent = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        try:
            async with connection.transaction(isolation="repeatable_read"):
                await _ledger_rows(connection, names)
                await _change(concurrent, monkeypatch, names, newer)
                await _change(connection, monkeypatch, names, older, revoke=True)
        except asyncpg.SerializationError as error:
            return error
    finally:
        await concurrent.close()
    return None


@pytest.mark.parametrize("pre_ledger", [False, True], ids=["managed", "pre_ledger"])
async def test_stale_reader_revoke_refuses_and_a_fresh_transaction_preserves_newer_grants(monkeypatch, pre_ledger):
    async with _grant_database() as (connection, names):
        older, newer = _generation(names, names.older, "a"), _generation(names, names.newer, "b")
        if pre_ledger:
            for schema, table in ((names.base, "entity_address_unified"), (names.older, "network_membership")):
                await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{names.reader}"')
                await connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{names.reader}"')
        else:
            await _change(connection, monkeypatch, names, older)
        conflict = await _stale_revoke(connection, monkeypatch, names, older, newer)
        assert conflict is not None, await _readable(connection, names)
        assert conflict.sqlstate == "40001"
        assert await _ledger_rows(connection, names) == (1 if pre_ledger else 2)
        assert await _readable(connection, names) == {names.base: True, names.older: True, names.newer: True}
        # The failed outer transaction has exited; _change starts an entirely fresh RR snapshot.
        await _change(connection, monkeypatch, names, older, revoke=True)
        assert await _ledger_rows(connection, names) == 1
        assert await _readable(connection, names) == {names.base: True, names.older: False, names.newer: True}
        await _change(connection, monkeypatch, names, newer, revoke=True)
        assert await _ledger_rows(connection, names) == 0
        assert await _readable(connection, names) == {names.base: False, names.older: False, names.newer: False}
        assert (
            await connection.fetchval(
                f'SELECT count(*) FROM "{names.control}".network_retained_read_grant_guard WHERE reader_role=$1',
                names.reader,
            )
            == 1
        )
