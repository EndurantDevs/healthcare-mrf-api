# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for a logical plan in a shared snapshot."""

import os
import re

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from api.plan_release_pricing_projection import plan_release_serving_sql


@pytest.mark.asyncio
async def test_release_query_requires_exact_logical_snapshot_scope():
    database_name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.search(r"(?:^|[_-])test(?:[_-]|$)", database_name, re.I):
        pytest.skip("PostgreSQL proof requires an explicit test database")

    dsn = URL.create(
        "postgresql+asyncpg",
        username=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=database_name,
    )
    engine = create_async_engine(dsn)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                for statement in (
                    "CREATE TEMP TABLE plan_release_serving_revision (serving_revision_id text, published_at timestamptz, plan_release_id text, healthporta_plan_id text, plan_version_id text, release_month text, release_status text, expected_binding_count integer, binding_set_digest text, serving_status text, is_current boolean)",
                    "CREATE TEMP TABLE plan_release_snapshot_binding (serving_revision_id text, binding_ordinal integer, snapshot_id text, source_key text, plan_id text, plan_market_type text, role text, required boolean)",
                    "CREATE TEMP TABLE ptg2_snapshot (snapshot_id text, status text)",
                    "CREATE TEMP TABLE ptg2_snapshot_pin (owner_type text, owner_id text, snapshot_id text)",
                    "CREATE TEMP TABLE ptg2_v3_snapshot_plan_scope (snapshot_id text, plan_id text, plan_market_type text)",
                    "INSERT INTO plan_release_serving_revision VALUES ('revision', now(), 'release', 'plan', NULL, '2026-09', 'published', 1, 'digest', 'published', true)",
                    "INSERT INTO plan_release_snapshot_binding VALUES ('revision', 0, 'snapshot', 'synthetic-source', 'logical-plan', 'group', 'in_network', true)",
                    "INSERT INTO ptg2_snapshot VALUES ('snapshot', 'published')",
                    "INSERT INTO ptg2_snapshot_pin VALUES ('plan_release_serving_revision', 'revision', 'snapshot')",
                ):
                    await connection.execute(text(statement))
                query = text(plan_release_serving_sql("pg_temp", include_pricing_projection=False))
                query_parameters_by_name = {
                    "plan_release_id": "release",
                    "pin_owner_type": "plan_release_serving_revision",
                }

                async def scope_present():
                    row = (await connection.execute(query, query_parameters_by_name)).mappings().one()
                    assert row["is_pinned"] is True
                    return row["logical_scope_present"]

                assert await scope_present() is False
                await connection.execute(
                    text("INSERT INTO ptg2_v3_snapshot_plan_scope VALUES ('snapshot', 'logical-plan', 'individual')")
                )
                assert await scope_present() is False
                await connection.execute(
                    text("INSERT INTO ptg2_v3_snapshot_plan_scope VALUES ('snapshot', 'logical-plan', 'group')")
                )
                assert await scope_present() is True
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
