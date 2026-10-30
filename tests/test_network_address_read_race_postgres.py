# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native read/DDL protocol checks; these do not qualify the complete API integration."""

import asyncio
import json
from contextlib import aclosing
from types import SimpleNamespace

import pytest

from process import entity_address_snapshot_adoption as adoption
from process.network_address_projection import _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import (
    NetworkReadIdentityError,
    verify_bound_network_names,
    verify_consumed_network_relations,
)
from tests import test_network_address_artifact_postgres as native

pytestmark = pytest.mark.asyncio
_RELATION_LOCKS = (
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
)


class PinnedReadMismatch(RuntimeError):
    """An internal data result cannot be exposed under the requested physical pin."""


@pytest.fixture
async def address_race_db(monkeypatch, request):
    """Set the owned fixture's contents before its actual protected descriptor is captured."""
    prepare_source = native._prepare_source

    async def prepare_contents(connection, roles):
        schema = await prepare_source(connection, roles)
        if not request.param:
            await connection.execute(f'DELETE FROM "{schema}".entity_address_unified')
        return schema

    monkeypatch.setattr(native, "_prepare_source", prepare_contents)
    async with aclosing(native.address_db.__wrapped__()) as resources:
        fixture = await anext(resources)
        fixture.race_populated = request.param
        yield fixture


def _data_query(qualified_name):
    return (
        "SELECT pg_catalog.to_regclass($1::text)::oid::bigint AS statement_oid,"
        "coalesce(array_agg(tableoid::bigint ORDER BY location_key),'{}'::bigint[]) AS data_oids,"
        "coalesce(array_agg(location_key::text ORDER BY location_key),'{}'::text[]) AS locations "
        f"FROM {qualified_name}"
    )


async def _relation_locks(connection, oid):
    return await connection.fetchval(
        "SELECT coalesce(array_agg(mode ORDER BY mode),'{}'::text[]) FROM pg_catalog.pg_locks "
        "WHERE pid=pg_backend_pid() AND database=(SELECT oid FROM pg_catalog.pg_database "
        "WHERE datname=current_database()) AND locktype='relation' AND relation=$1::oid AND granted",
        oid,
    )


async def _consumption_guard(connection, qualified_name, oid):
    return await connection.fetchrow(
        "SELECT coalesce(pg_catalog.to_regclass($1::text)::oid::bigint=$2::bigint,false) AS name_matches,"
        "EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=pg_backend_pid() AND "
        "database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()) "
        "AND locktype='relation' AND relation=$2::bigint::oid AND granted AND mode=ANY($3::text[])) AS pin_locked",
        qualified_name,
        oid,
        list(_RELATION_LOCKS),
    )


async def _verify_physical_read(resources, table_name, oid, backend_pid):
    await verify_consumed_network_relations(
        resources.reader,
        (CompactAddressRelation(resources.schema, table_name, oid),),
        expected_backend_pid=backend_pid,
    )


def _expose_locations(receipt, valid):
    if not valid:
        raise PinnedReadMismatch("pinned_address_relation_changed")
    return receipt["locations"]


async def _prepare_replacement(resources, *, populated):
    schema = _identifier(resources.schema)
    await resources.admin.execute(
        f"CREATE TABLE {schema}.entity_address_unified_next (LIKE {schema}.entity_address_unified INCLUDING ALL)"
    )
    await resources.admin.execute(
        f'ALTER TABLE {schema}.entity_address_unified_next OWNER TO "{resources.roles["owner"]}"'
    )
    await resources.admin.execute(
        f'GRANT SELECT ON {schema}.entity_address_unified_next TO "{resources.roles["reader"]}"'
    )
    if populated:
        await resources.admin.execute(
            f"INSERT INTO {schema}.entity_address_unified_next(location_key,type,npi,checksum) "
            "VALUES('new-office','practice',1000000491,1)"
        )


async def _swap_replacement(resources, monkeypatch):
    """Use the real protected swap; only replacement content setup differs from native._rotate."""
    monkeypatch.setattr(adoption.entity_address_unified, "db", native._NativeDB(resources.publisher))
    async with resources.publisher.transaction():
        retained = await adoption._swap_sealed_stage_table(
            resources.schema,
            SimpleNamespace(__main_table__="entity_address_unified"),
            SimpleNamespace(__tablename__="entity_address_unified_next", __my_additional_indexes__=[]),
            resources.descriptor["protected_owner_oid"],
        )
        oid = await resources.publisher.fetchval(
            "SELECT to_regclass($1::text)::oid::bigint", f'"{resources.schema}".entity_address_unified'
        )
        await resources.publisher.execute(
            f'UPDATE "{resources.schema}".entity_address_result_generation '
            "SET local_generation=2,origin_generation=2,relation_oids[1]=$1",
            oid,
        )
        await resources.publisher.execute(
            f'UPDATE "{resources.schema}".address_alias_state_v1 SET generation=generation+1'
        )
    return retained


async def _warm_statement(connection, query, qualified_name, *, warm_prepared):
    if not warm_prepared:
        return None
    async with connection.transaction():
        statement = await connection.prepare(query)
        await statement.fetchrow(qualified_name)
    assert not connection.is_in_transaction()
    return statement


async def _resolved_name(resources):
    oid = resources.descriptor["table"]["oid"]
    physical = await resources.reader.fetchrow(
        "SELECT n.nspname,c.relname FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1::oid",
        oid,
    )
    assert tuple(physical) == (resources.schema, "entity_address_unified")
    qualified_name = _identifier(physical["nspname"]) + "." + _identifier(physical["relname"])
    assert not await _relation_locks(resources.reader, oid)
    return qualified_name, oid


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["old-empty", "old-populated"])
@pytest.mark.parametrize("warm_prepared", [False, True], ids=["cold", "prepared-before-rr"])
async def test_rotation_results_require_physical_pin(address_race_db, monkeypatch, warm_prepared, record_property):
    resources = address_race_db
    reader, old_oid = resources.reader, resources.descriptor["table"]["oid"]
    live_name = f'{_identifier(resources.schema)}."entity_address_unified"'
    old_locations = await resources.admin.fetch(f"SELECT location_key FROM {live_name} ORDER BY location_key")
    old_locations = [office["location_key"] for office in old_locations]
    assert old_locations == (["old-office"] if resources.race_populated else [])
    # Stage data must commit before RR starts, so the replacement is visible if the stale name is consumed.
    await _prepare_replacement(resources, populated=not old_locations)
    query = _data_query(live_name)
    statement = await _warm_statement(reader, query, live_name, warm_prepared=warm_prepared)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        resolved_name, resolved_oid = await _resolved_name(resources)
        backend_pid = reader.get_server_pid()
        assert resolved_oid == old_oid
        retained = await asyncio.wait_for(_swap_replacement(resources, monkeypatch), timeout=10)
        assert retained["relation_oid"] == old_oid
        retained_name = f"{_identifier(resources.schema)}.{_identifier(retained['retained_name'])}"
        receipt = await (
            reader.fetchrow(query, resolved_name) if statement is None else statement.fetchrow(resolved_name)
        )
        expected_replacement = [] if old_locations else ["new-office"]
        assert receipt["locations"] == expected_replacement and receipt["locations"] != old_locations
        assert receipt["statement_oid"] != old_oid
        assert all(oid == receipt["statement_oid"] for oid in receipt["data_oids"])
        post_check = await _consumption_guard(reader, resolved_name, old_oid)
        assert post_check["name_matches"] is False
        _record_race(record_property, warm_prepared, old_locations, receipt, post_check, old_oid)
        assert "AccessShareLock" in await _relation_locks(reader, receipt["statement_oid"])
        with pytest.raises(PinnedReadMismatch):
            _expose_locations(receipt, receipt["statement_oid"] == old_oid)
        with pytest.raises(PinnedReadMismatch):
            _expose_locations(receipt, post_check["pin_locked"] and post_check["name_matches"])
        with pytest.raises(NetworkReadIdentityError):
            await _verify_physical_read(resources, "entity_address_unified", old_oid, backend_pid)
        with pytest.raises(NetworkReadIdentityError):
            await verify_bound_network_names(
                reader,
                (CompactAddressRelation(resources.schema, "entity_address_unified", old_oid),),
                expected_backend_pid=backend_pid,
            )
        retained_receipt = await reader.fetchrow(_data_query(retained_name), retained_name)
        assert retained_receipt["statement_oid"] == old_oid and retained_receipt["locations"] == old_locations
        post_check = await _consumption_guard(reader, resolved_name, old_oid)
        assert post_check["pin_locked"] is True and post_check["name_matches"] is False
        with pytest.raises(PinnedReadMismatch):
            _expose_locations(receipt, post_check["pin_locked"] and post_check["name_matches"])
        pinned_check = await _consumption_guard(reader, retained_name, old_oid)
        await _verify_physical_read(resources, retained["retained_name"], old_oid, backend_pid)
        await verify_bound_network_names(
            reader,
            (CompactAddressRelation(resources.schema, retained["retained_name"], old_oid),),
            expected_backend_pid=backend_pid,
        )
        assert _expose_locations(retained_receipt, all(pinned_check.values())) == old_locations
    assert not await _relation_locks(reader, old_oid)


def _record_race(record_property, warm_prepared, old_locations, receipt, post_check, old_oid):
    record_property(
        "native_read_race",
        json.dumps(
            {
                "scope": "native-component-only",
                "warm_prepared": warm_prepared,
                "old_rows": len(old_locations),
                "replacement_rows": len(receipt["locations"]),
                "statement_oid_differs": receipt["statement_oid"] != old_oid,
                "post_name_matches": post_check["name_matches"],
                "post_pin_locked": post_check["pin_locked"],
            },
            sort_keys=True,
        ),
    )


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["empty", "populated"])
@pytest.mark.parametrize("warm_prepared", [False, True], ids=["cold", "prepared-before-rr"])
async def test_normal_read_has_automatic_relation_lock(address_race_db, warm_prepared):
    resources = address_race_db
    reader = resources.reader
    live_name = f'{_identifier(resources.schema)}."entity_address_unified"'
    query = _data_query(live_name)
    statement = await _warm_statement(reader, query, live_name, warm_prepared=warm_prepared)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        resolved_name, oid = await _resolved_name(resources)
        backend_pid = reader.get_server_pid()
        receipt = await (
            reader.fetchrow(query, resolved_name) if statement is None else statement.fetchrow(resolved_name)
        )
        post_check = await _consumption_guard(reader, resolved_name, oid)
        assert receipt["statement_oid"] == oid and all(post_check.values())
        assert "AccessShareLock" in await _relation_locks(reader, oid)
        await _verify_physical_read(resources, "entity_address_unified", oid, backend_pid)
        await verify_bound_network_names(
            reader,
            (CompactAddressRelation(resources.schema, "entity_address_unified", oid),),
            expected_backend_pid=backend_pid,
        )
        assert _expose_locations(receipt, True) == (["old-office"] if resources.race_populated else [])
    assert not await _relation_locks(reader, oid)


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["old-empty", "old-populated"])
async def test_cached_live_name_rebinds_after_rotation_before_inventory(address_race_db, monkeypatch):
    """Reverse race: a pre-rotation prepared query must consume the newly resolved live heap."""
    resources = address_race_db
    reader, old_oid = resources.reader, resources.descriptor["table"]["oid"]
    live_name = f'{_identifier(resources.schema)}."entity_address_unified"'
    statement = await _warm_statement(reader, _data_query(live_name), live_name, warm_prepared=True)
    await _prepare_replacement(resources, populated=not resources.race_populated)
    await _swap_replacement(resources, monkeypatch)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        new_oid = await reader.fetchval("SELECT to_regclass($1::text)::oid::bigint", live_name)
        assert new_oid != old_oid
        backend_pid = reader.get_server_pid()
        receipt = await statement.fetchrow(live_name)
        assert receipt["statement_oid"] == new_oid and all(oid == new_oid for oid in receipt["data_oids"])
        await _verify_physical_read(resources, "entity_address_unified", new_oid, backend_pid)
        await verify_bound_network_names(
            reader,
            (CompactAddressRelation(resources.schema, "entity_address_unified", new_oid),),
            expected_backend_pid=backend_pid,
        )
        assert receipt["locations"] == ([] if resources.race_populated else ["new-office"])


@pytest.mark.parametrize(
    "address_race_db", [False, True], indirect=True, ids=["installed-empty", "installed-populated"]
)
async def test_optional_name_installation_cannot_escape_absent_inventory(address_race_db):
    resources = address_race_db
    reader = resources.reader
    await _prepare_replacement(resources, populated=resources.race_populated)
    missing = f'{_identifier(resources.schema)}."entity_address_missing"'
    absent_names = ((resources.schema, "entity_address_missing"),)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        await reader.fetchval("SELECT count(*) FROM pg_catalog.pg_class")
        backend_pid = reader.get_server_pid()
        await verify_bound_network_names(reader, (), expected_backend_pid=backend_pid, absent_names=absent_names)
        await resources.admin.execute(
            f"ALTER TABLE {_identifier(resources.schema)}.entity_address_unified_next RENAME TO entity_address_missing"
        )
        receipt = await reader.fetchrow(_data_query(missing), missing)
        assert receipt["locations"] == (["new-office"] if resources.race_populated else [])
        with pytest.raises(NetworkReadIdentityError):
            await verify_bound_network_names(reader, (), expected_backend_pid=backend_pid, absent_names=absent_names)


def _composite_query(qualified_name):
    return (
        "WITH typed AS MATERIALIZED (SELECT jsonb_populate_record(NULL::"
        + qualified_name
        + ',\'{"location_key":"source-office"}\'::jsonb) AS office) '
        "SELECT pg_typeof(office)::oid::bigint AS type_oid,(office).location_key FROM typed"
    )


@pytest.mark.parametrize("address_race_db", [True], indirect=True)
@pytest.mark.parametrize("warm_prepared", [False, True], ids=["cold-rowtype", "cached-rowtype"])
async def test_legacy_composite_cast_does_not_prove_heap_consumption(address_race_db, monkeypatch, warm_prepared):
    """Record the unsafe live-type behavior removed from the API payload renderer."""
    resources = address_race_db
    reader, old_oid = resources.reader, resources.descriptor["table"]["oid"]
    live_name = f'{_identifier(resources.schema)}."entity_address_unified"'
    query = _composite_query(live_name)
    statement = None
    if warm_prepared:
        async with reader.transaction():
            statement = await reader.prepare(query)
            await statement.fetchrow()
    await _prepare_replacement(resources, populated=False)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        _, pinned_oid = await _resolved_name(resources)
        backend_pid = reader.get_server_pid()
        old_type = await reader.fetchval("SELECT reltype::bigint FROM pg_class WHERE oid=$1::oid", old_oid)
        await asyncio.wait_for(_swap_replacement(resources, monkeypatch), timeout=10)
        receipt = await (reader.fetchrow(query) if statement is None else statement.fetchrow())
        assert receipt["location_key"] == "source-office"
        assert (receipt["type_oid"] == old_type) is warm_prepared
        current_oid = await reader.fetchval("SELECT to_regclass($1::text)::oid::bigint", live_name)
        assert await _relation_locks(reader, current_oid) == []
        with pytest.raises(NetworkReadIdentityError):
            await verify_bound_network_names(
                reader,
                (CompactAddressRelation(resources.schema, "entity_address_unified", pinned_oid),),
                expected_backend_pid=backend_pid,
            )


@pytest.mark.parametrize("address_race_db", [True], indirect=True)
async def test_legacy_cached_type_is_outside_fixed_heap_name_contract(address_race_db, monkeypatch):
    """A matching current heap name cannot authenticate an old cached composite type."""
    resources = address_race_db
    reader = resources.reader
    live_name = f'{_identifier(resources.schema)}."entity_address_unified"'
    async with reader.transaction():
        statement = await reader.prepare(_composite_query(live_name))
        await statement.fetchrow()
    await _prepare_replacement(resources, populated=False)
    await _swap_replacement(resources, monkeypatch)
    async with reader.transaction(isolation="repeatable_read", readonly=True):
        current = await reader.fetchrow(
            "SELECT oid::bigint,reltype::bigint FROM pg_class WHERE oid=to_regclass($1::text)", live_name
        )
        receipt = await statement.fetchrow()
        assert receipt["type_oid"] != current["reltype"] and receipt["location_key"] == "source-office"
        assert await _relation_locks(reader, current["oid"]) == []
        await verify_bound_network_names(
            reader,
            (CompactAddressRelation(resources.schema, "entity_address_unified", current["oid"]),),
            expected_backend_pid=reader.get_server_pid(),
        )
