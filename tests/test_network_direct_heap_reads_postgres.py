# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native ONLY and column-access properties, separate from claims Source authority."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, exists, func, select, text, union_all
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import registry

from api.network_direct_heap_reads import direct_heap_read_statement
from api.network_read_execution import qualified_relation_sql
from process.entity_address_cutover_contract import postgres_sqlstate
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
                table = Table(
                    "direct_heap_probe", MetaData(), Column("id", Integer, primary_key=True), schema=case.schema
                )
                yield case, admin, session, table, parent, child
        finally:
            await engine.dispose()


async def test_native_column_reader_excludes_child_attached_after_metadata(monkeypatch):
    async with _column_reader_case(monkeypatch) as (case, admin, session, table, parent, child):
        pin = CompactAddressRelation(case.schema, "direct_heap_probe")
        with pytest.raises(DBAPIError) as denied:
            async with session.begin_nested():
                await session.execute(text(f"SELECT private_payload FROM ONLY {parent}"))
        assert postgres_sqlstate(denied.value) == "42501"
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
            assert (await session.execute(direct_heap_read_statement(statement, (pin,)))).scalars().all() == [1]
        combined = union_all(select(table.c.id), select(alias.c.id))
        assert (await session.execute(direct_heap_read_statement(combined, (pin,)))).scalars().all() == [1, 1]
        relation = "ONLY " + qualified_relation_sql(case.schema, "direct_heap_probe")
        async with admin.transaction():
            await admin.execute(f"LOCK TABLE {child} IN ACCESS EXCLUSIVE MODE NOWAIT")
            result = await session.execute(text(f"SELECT p.id FROM {relation} p ORDER BY p.id"))
            assert result.scalars().all() == [1]
        assert (await session.execute(select(table.c.id).order_by(table.c.id))).scalars().all() == [1, 999]


async def test_native_hints_preserve_orm_entity_hydration(monkeypatch):
    mappings = registry()
    try:
        async with _column_reader_case(monkeypatch) as (case, _admin, session, table, _parent, _child):

            class Provider:
                """Map only the column this genuine Reader may select."""

            mappings.map_imperatively(Provider, table)
            pin = CompactAddressRelation(case.schema, "direct_heap_probe")
            records = (await session.execute(direct_heap_read_statement(select(Provider), (pin,)))).scalars().all()
            assert len(records) == 1 and type(records[0]) is Provider and records[0].id == 1
    finally:
        mappings.dispose()
