# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native immutable named reads across normal publication and prepared queries."""

import asyncio
import json
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import text

from api.network_address_scope import network_address_read_scope, scoped_address_cache_key
from api.network_compact_address import compact_address_native_sql, compact_address_read_relations
from api.network_read_execution import qualified_relation_sql
from process.network_serving_read import resolve_network_serving_manifest
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _reader,
    _seal,
)
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def address_race_db(monkeypatch, request):
    """Reuse the ordinary owned native preparation/publication lifecycle."""
    async with asynccontextmanager(compact_publication_db.__wrapped__)(monkeypatch) as fixture:
        fixture.race_populated = request.param
        fixture.schema = fixture.address.schema
        fixture.admin = fixture.address.admin
        fixture.reader = fixture.address.reader
        first = await _candidate(fixture, (42, 88) if request.param else (88, 99), 0)
        await _prepare(fixture, first)
        await _seal(fixture, first)
        await _publish(fixture, first)
        async with _reader(fixture) as (_connection, driver):
            fixture.first_pin = await resolve_network_serving_manifest(
                driver, control_schema=fixture.control_schema, api_read=True
            )
        yield fixture


def _data_query(manifest):
    relation, parameters = compact_address_native_sql(manifest.compact_layout, first_parameter=1, network_ids=(42,))
    return f"SELECT to_jsonb(office) AS office FROM {relation} office ORDER BY location_key", parameters


async def _rows(driver, query, parameters, statement=None):
    rows = await (driver.fetch(query, *parameters) if statement is None else statement.fetch(*parameters))
    return [json.loads(row["office"]) if type(row["office"]) is str else row["office"] for row in rows]


async def _relation_locks(connection, relation):
    """Observe ordinary SQL locks by name; no result is accepted on lock identity."""
    return await connection.fetchval(
        "SELECT coalesce(array_agg(l.mode ORDER BY l.mode),'{}'::text[]) FROM pg_catalog.pg_locks l "
        "JOIN pg_catalog.pg_class c ON c.oid=l.relation "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE l.pid=pg_backend_pid() AND l.locktype='relation' AND l.granted "
        "AND n.nspname=$1 AND c.relname=$2",
        relation.schema_name,
        relation.table_name,
    )


@asynccontextmanager
async def _named_reader(fixture, manifest, warm_prepared):
    query, parameters = _data_query(manifest)
    async with fixture.reader_engine.connect() as connection:
        driver = (await connection.get_raw_connection()).driver_connection
        statement = None
        if warm_prepared:
            async with connection.begin():
                await connection.execute(text("SELECT 1"))
                statement = await driver.prepare(query)
                await statement.fetch(*parameters)
        async with connection.begin():
            await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            yield driver, query, parameters, statement
        for relation in compact_address_read_relations(manifest.compact_layout):
            assert await _relation_locks(driver, relation) == []


async def _prepare_successor(fixture):
    target = await _candidate(
        fixture, (88, 99) if fixture.race_populated else (42, 99), fixture.first_pin.generation_id
    )
    await _prepare(fixture, target)
    await _seal(fixture, target)
    return target


async def _publish_concurrently(fixture, target):
    task = asyncio.create_task(_publish(fixture, target))
    try:
        return await asyncio.wait_for(task, timeout=10)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def _read_pin(fixture, generation_id=None):
    async with _reader(fixture) as (_connection, driver):
        pin = await resolve_network_serving_manifest(
            driver, generation_id=generation_id, control_schema=fixture.control_schema, api_read=True
        )
        query, parameters = _data_query(pin)
        return pin, await _rows(driver, query, parameters)


async def _assert_write_denied(connection, statement):
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(statement)


async def _assert_application_dml_denied(fixture, manifest):
    reader = fixture.address.reader
    relations = compact_address_read_relations(manifest.compact_layout)
    async with reader.transaction(isolation="repeatable_read"):
        for relation in relations:
            qualified = qualified_relation_sql(relation.schema_name, relation.table_name)
            key = "network_id" if relation.table_name == "network_slice_ref" else "location_key"
            for operation in (
                f"INSERT INTO {qualified} DEFAULT VALUES",
                f'UPDATE {qualified} SET "{key}"="{key}" WHERE false',
                f"DELETE FROM {qualified} WHERE false",
                f"TRUNCATE {qualified}",
            ):
                await _assert_write_denied(reader, operation)


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["old-empty", "old-populated"])
@pytest.mark.parametrize("warm_prepared", [False, True], ids=["cold", "prepared-before-rr"])
async def test_publication_keeps_pinned_named_generation(address_race_db, warm_prepared, record_property):
    fixture = address_race_db
    first = fixture.first_pin
    successor = await _prepare_successor(fixture)
    source_relation = qualified_relation_sql(
        first.compact_layout.base.schema_name, first.compact_layout.base.table_name
    )
    source_before = await fixture.connection.fetch(
        f"SELECT to_jsonb(office) FROM {source_relation} office ORDER BY location_key"
    )
    async with _named_reader(fixture, first, warm_prepared) as (driver, query, parameters, statement):
        assert (
            await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema, api_read=True)
            == first
        )
        original = await _rows(driver, query, parameters, statement)
        assert [office_row["location_key"] for office_row in original] == (
            ["old-office"] if fixture.race_populated else []
        )
        published = await _publish_concurrently(fixture, successor)
        assert published["generation_id"] != first.generation_id
        assert (
            await resolve_network_serving_manifest(driver, control_schema=fixture.control_schema, api_read=True)
            == first
        )
        assert await _rows(driver, query, parameters, statement) == original
    current, current_rows = await _read_pin(fixture)
    retained, retained_rows = await _read_pin(fixture, first.generation_id)
    assert current.generation_id == published["generation_id"] and current.schema_name == successor.schema_name
    assert [office_row["location_key"] for office_row in current_rows] == (
        [] if fixture.race_populated else ["old-office"]
    )
    assert retained == first and retained_rows == original
    assert (
        await fixture.connection.fetch(f"SELECT to_jsonb(office) FROM {source_relation} office ORDER BY location_key")
        == source_before
    )
    record_property(
        "native_named_publication",
        json.dumps(
            dict(warm_prepared=warm_prepared, old_rows=len(original), replacement_rows=len(current_rows)),
            sort_keys=True,
        ),
    )


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["empty", "populated"])
@pytest.mark.parametrize("warm_prepared", [False, True], ids=["cold", "prepared-before-rr"])
async def test_normal_named_read_has_automatic_relation_lock(address_race_db, warm_prepared):
    fixture = address_race_db
    manifest = fixture.first_pin
    async with _named_reader(fixture, manifest, warm_prepared) as (driver, query, parameters, statement):
        rows = await _rows(driver, query, parameters, statement)
        assert [row["location_key"] for row in rows] == (["old-office"] if fixture.race_populated else [])
        for relation in compact_address_read_relations(manifest.compact_layout):
            assert "AccessShareLock" in await _relation_locks(driver, relation)
    await _assert_application_dml_denied(fixture, manifest)
    assert (await _read_pin(fixture, manifest.generation_id))[1] == rows


@pytest.mark.parametrize("address_race_db", [False, True], indirect=True, ids=["old-empty", "old-populated"])
async def test_cached_named_query_stays_with_its_retained_generation(address_race_db):
    fixture = address_race_db
    first = fixture.first_pin
    successor = await _prepare_successor(fixture)
    query, parameters = _data_query(first)
    async with fixture.reader_engine.connect() as connection:
        async with connection.begin():
            await connection.execute(text("SELECT 1"))
            driver = (await connection.get_raw_connection()).driver_connection
            statement = await driver.prepare(query)
            original = await _rows(driver, query, parameters, statement)
        await _publish_concurrently(fixture, successor)
        async with connection.begin():
            await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            current = await resolve_network_serving_manifest(
                driver, control_schema=fixture.control_schema, api_read=True
            )
            current_query, current_parameters = _data_query(current)
            current_rows = await _rows(driver, current_query, current_parameters)
            assert current.schema_name == successor.schema_name and current != first
            assert [office_row["location_key"] for office_row in current_rows] == (
                [] if fixture.race_populated else ["old-office"]
            )
            assert await _rows(driver, query, parameters, statement) == original
            assert (
                await resolve_network_serving_manifest(
                    driver, generation_id=first.generation_id, control_schema=fixture.control_schema, api_read=True
                )
                == first
            )
            keys = []
            for manifest in (first, current):
                with network_address_read_scope(manifest, (42,)):
                    keys.append(scoped_address_cache_key("same-query"))
            assert keys[0] != keys[1]
