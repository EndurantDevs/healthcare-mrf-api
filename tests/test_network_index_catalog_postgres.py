# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A fixed deparse context restores native caller settings on every outcome."""

import asyncio
import json
import os

import asyncpg
import pytest

from process.network_index_catalog import index_catalog_statement
from tests.test_network_membership_slices_postgres import slice_db as slice_db


async def _cancel_sleeping_catalog(connection, observer, statement):
    pending_query = asyncio.create_task(connection.fetchval(index_catalog_statement(statement + ",pg_sleep(10)")))
    try:
        async with asyncio.timeout(5):
            while (
                await observer.fetchval(
                    "SELECT wait_event FROM pg_stat_activity WHERE pid=$1", connection.get_server_pid()
                )
                != "PgSleep"
            ):
                await asyncio.sleep(0.01)
        pending_query.cancel()
        await pending_query
    finally:
        if not pending_query.done():
            pending_query.cancel()
            await asyncio.gather(pending_query, return_exceptions=True)


async def _assert_catalog_cancellation(connection, statement):
    observer = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            async with connection.transaction():
                await _cancel_sleeping_catalog(connection, observer, statement)
    finally:
        await observer.close()


async def _assert_catalog_statement_failure(connection):
    statement = "SELECT 1/(length(native_path.configured_path)-10) AS index_oid"
    with pytest.raises(asyncpg.DivisionByZeroError):
        async with connection.transaction():
            await connection.fetchval(index_catalog_statement(statement))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["rows", "empty", "error", "cancel"])
async def test_index_catalog_restores_exact_path_on_every_outcome(slice_db, outcome):
    connection = slice_db.connection
    original = 'public, "$user", pg_catalog'
    async with connection.transaction():
        await connection.execute("SELECT set_config('search_path',$1,true)", original)
        statement = "SELECT 1::bigint AS index_oid,current_setting('search_path') AS path"
        if outcome in ("rows", "empty"):
            statement += " WHERE " + ("TRUE" if outcome == "rows" else "FALSE")
            index_rows = json.loads(await connection.fetchval(index_catalog_statement(statement)))
            assert index_rows == ([{"index_oid": 1, "path": "pg_catalog"}] if outcome == "rows" else [])
        elif outcome == "error":
            await _assert_catalog_statement_failure(connection)
        else:
            await _assert_catalog_cancellation(connection, statement)
        assert await connection.fetchval("SHOW search_path") == original


@pytest.mark.asyncio
async def test_legacy_catalog_keeps_its_original_deparse_context(slice_db):
    connection = slice_db.connection
    async with connection.transaction():
        await connection.execute("SET LOCAL search_path=public,pg_catalog")
        statement = "SELECT 1::bigint AS index_oid,current_setting('search_path') AS path"
        rows = json.loads(await connection.fetchval(index_catalog_statement(statement, geo_present="FALSE")))
        assert rows == [{"index_oid": 1, "path": "public, pg_catalog"}]
        assert await connection.fetchval("SHOW search_path") == "public, pg_catalog"


@pytest.mark.asyncio
@pytest.mark.parametrize("geo_flags", [(True, True), (False, True), (True, False)])
@pytest.mark.parametrize("reverse", [False, True])
async def test_index_catalog_reapplies_path_for_each_correlated_family(slice_db, geo_flags, reverse):
    connection = slice_db.connection
    statement = index_catalog_statement(
        "SELECT family.ordinal::bigint AS index_oid,current_setting('search_path') AS path",
        geo_present="family.has_geo",
    )
    async with connection.transaction():
        await connection.execute("SET LOCAL search_path=public,pg_catalog")
        values = "(2,$2::boolean),(1,$1::boolean)" if reverse else "(1,$1::boolean),(2,$2::boolean)"
        rows = await connection.fetch(
            "SELECT family.ordinal,catalog.rows FROM (VALUES " + values + ") "
            "family(ordinal,has_geo) CROSS JOIN LATERAL (" + statement + ") catalog ORDER BY family.ordinal",
            *geo_flags,
        )
        assert [json.loads(row["rows"]) for row in rows] == [
            [{"index_oid": ordinal, "path": "pg_catalog" if has_geo else "public, pg_catalog"}]
            for ordinal, has_geo in enumerate(geo_flags, 1)
        ]
        assert await connection.fetchval("SHOW search_path") == "public, pg_catalog"
