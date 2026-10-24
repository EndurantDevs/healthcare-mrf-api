# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source-maintenance actor with SELECT-only immutable storage access."""

from contextlib import asynccontextmanager

from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from db.plan_release_storage import storage_relation


async def prepare_maintenance_actor(case):
    admin = await case.connect()
    async with admin.transaction():
        await admin.execute(f"ALTER TABLE {case.relation('ptg2_snapshot')} ADD previous_snapshot_id text")
        await admin.execute(
            f'UPDATE {case.relation("ptg2_snapshot")} SET manifest=\'{{"serving_index":{{"source_key":"synthetic_source"}}}}\'::jsonb'
        )
        for table, columns in (
            (
                "hp_plan_release_binding",
                "release_binding_id text,serving_revision_id text,role text,ordinal integer,snapshot_id text",
            ),
            ("hp_snapshot_pin", "owner_type text,owner_id text,snapshot_id text,source_key text,node_id text"),
        ):
            relation = storage_relation(case.preparation_schema, table)
            await admin.execute(f"CREATE TABLE {relation}({columns})")
            await admin.execute(f'GRANT SELECT,UPDATE ON {relation} TO "{case.reader}"')
        for table in (
            "ptg2_snapshot",
            "ptg2_current_snapshot",
            "ptg2_current_source_snapshot",
            "ptg2_current_plan_source",
            "ptg2_snapshot_pin",
        ):
            await admin.execute(f'GRANT SELECT,UPDATE ON {case.relation(table)} TO "{case.reader}"')


@asynccontextmanager
async def maintenance_session(case):
    url = URL.create(
        "postgresql+asyncpg",
        username=case.reader,
        password=case.role_passwords[case.reader],
        host=case.config["host"],
        port=case.config["port"],
        database=case.config["database"],
    )
    engine = create_async_engine(url)
    try:
        async with engine.begin() as session:
            yield session
    finally:
        await engine.dispose()


async def acquire_publication_fence(publisher):
    """Match the native Engine source fence on the same publication connection."""
    await publisher.execute("SET LOCAL lock_timeout='100ms'")
    await publisher.execute("SELECT pg_advisory_xact_lock_shared(hashtext($1))", "ptg2_source_pointer_gc_v1")
    await publisher.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ptg2_source_lifecycle_v2:synthetic_source"
    )
