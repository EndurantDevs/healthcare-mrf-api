# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ONLY and column-access properties, separate from claims Source authority."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, exists, func, select, text, union_all
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import registry

from api.network_read_execution import direct_heap_relation_sql, execute_network_read, install_network_read_inventory
from process.network_compact_layout import CompactAddressRelation
from tests.test_plan_release_retained_reads_postgres import retained_case

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _column_reader_case(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, _reader, _before):
        parent = case.relation("direct_heap_probe")
        child = case.relation("direct_heap_child")
        await admin.execute(f"CREATE TABLE {parent}(id integer PRIMARY KEY,private_payload text)")
        await admin.execute(f"CREATE TABLE {child}(id integer NOT NULL,private_payload text)")
        await admin.execute(f"INSERT INTO {parent} VALUES(1,'parent'); INSERT INTO {child} VALUES(999,'child')")
        await admin.execute(f'GRANT SELECT(id) ON {parent} TO "{case.reader}"')
        oid = await admin.fetchval("SELECT to_regclass($1)::oid::bigint", parent)
        url = URL.create(
            "postgresql+asyncpg",
            username=case.reader,
            password=case.role_passwords[case.reader],
            host=case.config["host"],
            port=case.config["port"],
            database=case.config["database"],
        )
        engine = create_async_engine(url, isolation_level="REPEATABLE READ")
        try:
            async with async_sessionmaker(engine)() as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                pin = CompactAddressRelation(case.schema, "direct_heap_probe", oid)
                await install_network_read_inventory(session, (pin,), only_relations=(pin,))
                table = Table(
                    "direct_heap_probe", MetaData(), Column("id", Integer, primary_key=True), schema=case.schema
                )
                yield case, admin, session, table, parent, child, oid
        finally:
            await engine.dispose()


async def test_native_column_reader_excludes_child_attached_after_metadata(monkeypatch):
    async with _column_reader_case(monkeypatch) as (case, admin, session, table, parent, child, oid):
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        assert await driver.fetchval("SELECT NOT has_table_privilege(current_user,$1::oid,'SELECT')", oid)
        assert not await driver.fetchval(
            "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND relation=$1::oid AND granted)", oid
        )
        await admin.execute("SET lock_timeout='250ms'")
        await admin.execute(f"ALTER TABLE {child} INHERIT {parent}")
        alias = table.alias("other")
        statements = (
            select(table.c.id),
            select(alias.c.id),
            select(alias.alias("outer_alias").c.id),
            select(table.tablesample(func.system(100), name="sampled").c.id),
            select(select(table.c.id).cte("selected")),
            select(table.c.id).where(exists(select(alias.c.id).where(alias.c.id == table.c.id).correlate(table))),
        )
        for statement in statements:
            assert (await execute_network_read(session, statement)).scalars().all() == [1]
        combined = union_all(select(table.c.id), select(alias.c.id))
        assert (await execute_network_read(session, combined)).scalars().all() == [1, 1]
        relation = direct_heap_relation_sql(session, case.schema, "direct_heap_probe")
        async with admin.transaction():
            await admin.execute(f"LOCK TABLE {child} IN ACCESS EXCLUSIVE MODE NOWAIT")
            result = await execute_network_read(session, f"SELECT p.id FROM {relation} p ORDER BY p.id")
            assert result.scalars().all() == [1]
        assert (await session.execute(select(table.c.id).order_by(table.c.id))).scalars().all() == [1, 999]
        assert await driver.fetchval("SELECT current_user=session_user AND current_user::text=$1", case.reader)


async def test_native_hints_preserve_orm_entity_hydration(monkeypatch):
    mappings = registry()
    try:
        async with _column_reader_case(monkeypatch) as (_case, _admin, session, table, _parent, _child, _oid):

            class Provider:
                """Map only the column this genuine Reader may select."""

            mappings.map_imperatively(Provider, table)
            records = (await execute_network_read(session, select(Provider))).scalars().all()
            assert len(records) == 1 and type(records[0]) is Provider and records[0].id == 1
    finally:
        mappings.dispose()
