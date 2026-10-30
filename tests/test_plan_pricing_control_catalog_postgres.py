# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected CHECK declarations must match PostgreSQL's own canonical output."""

import os
import re
from uuid import uuid4

import asyncpg
import pytest

from db.plan_release_storage import storage_identifier
from process.plan_pricing_control_catalog import CONTROL_SHAPES, _normalized_expression


@pytest.mark.asyncio
async def test_protected_control_checks_round_trip_through_native_postgres():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.search(r"(?:^|[_-])test(?:[_-]|$)", database, re.I):
        pytest.skip("native control proof requires an explicit test database")
    connection = await asyncpg.connect(
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
        database=database,
    )
    try:
        async with connection.transaction():
            for shape in CONTROL_SHAPES.values():
                name = "native_control_check_" + uuid4().hex
                columns = [storage_identifier(column[0]) + " " + column[1] for column in shape["columns"]]
                checks = [
                    "CONSTRAINT " + storage_identifier(key) + " " + expression
                    for key, expression in shape["checks"].items()
                ]
                await connection.execute(
                    "CREATE TEMPORARY TABLE "
                    + storage_identifier(name)
                    + " ("
                    + ",".join(columns + checks)
                    + ") ON COMMIT DROP"
                )
                observed = await connection.fetch(
                    "SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint "
                    "WHERE conrelid=to_regclass($1) AND contype='c'",
                    "pg_temp." + name,
                )
                assert {
                    constraint["conname"]: _normalized_expression(constraint["definition"]) for constraint in observed
                } == {key: _normalized_expression(expression) for key, expression in shape["checks"].items()}
        assert not await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", "pg_temp." + name)
    finally:
        await connection.close(timeout=5)
