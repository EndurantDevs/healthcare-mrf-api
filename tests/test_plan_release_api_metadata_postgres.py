# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-family API reads locate storage by name and hold what they read."""

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.plan_release_pricing_projection import plan_release_serving_sql
from api.plan_release_read_storage import is_retained_session_read, lock_binding_storage
from tests.plan_release_storage_postgres_support import RELEASE, _database_config
from tests.test_plan_release_retained_reads_postgres import retained_case

pytestmark = pytest.mark.asyncio


async def test_retained_api_read_holds_named_tables_and_matches_original_rows(monkeypatch):
    async with retained_case(monkeypatch) as (case, _admin, _reader, before):
        sql = plan_release_serving_sql(case.schema, include_pricing_projection=False, retained=True)
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
                assert await is_retained_session_read(session, case.schema) is True
                await lock_binding_storage(session, case.schema, retained=True)
                held = await session.execute(
                    text(
                        "SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                        "WHERE l.pid=pg_backend_pid() AND l.mode='AccessShareLock' AND l.granted "
                        "AND c.relnamespace=CAST(:schema AS regnamespace)"
                    ),
                    {"schema": case.schema},
                )
                assert {held_lock[0] for held_lock in held} >= {
                    "plan_release_snapshot_binding_legacy",
                    "plan_release_serving_revision",
                    "ptg2_snapshot",
                    "ptg2_snapshot_pin",
                    "ptg2_v3_snapshot_plan_scope",
                }
                binding_rows = await session.execute(
                    text(sql), {"plan_release_id": RELEASE, "pin_owner_type": "plan_release_serving_revision"}
                )
                assert [dict(binding_row._mapping) for binding_row in binding_rows] == [
                    dict(expected) for expected in before
                ]
        finally:
            await engine.dispose()


@pytest.mark.parametrize("suffix", ["0" * 32, "a" * 32, "", "0" * 31, "g" * 32, "0" * 32 + "_extra"])
async def test_native_storage_accepts_only_exact_owned_ci_parent(monkeypatch, suffix):
    database = "hc_network_registry_" + suffix
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database)
    if suffix in {"0" * 32, "a" * 32}:
        assert _database_config()["database"] == database
    else:
        with pytest.raises(pytest.skip.Exception, match="requires an explicit test database"):
            _database_config()
