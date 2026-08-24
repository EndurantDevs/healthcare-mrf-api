# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import ptg2_shared_gc as shared_gc
from process.ptg_parts import ptg2_source_snapshot_gc as source_snapshot_gc
from process.ptg_parts import snapshot_cleanup

from tests.ptg2_shared_gc_test_support import (
    _SharedGCExecutor,
    _SourceGCProjectionExecutor,
    _hash,
    _patch_v4_abandonment_pipeline,
)
from tests.ptg2_shared_gc_postgres_support import (
    _assert_completed_v4_abandonment,
    _assert_gc_fixture_storage,
    _assert_partial_v4_abandonment,
    _cancel_after_build_pin_batch,
    _create_gc_block_schema,
    _create_gc_layout_schema,
    _create_v4_abandonment_schema,
    _drop_test_schema_and_disconnect,
    _insert_gc_block_fixture,
    _insert_gc_layout_fixture,
    _insert_v4_abandonment_fixture,
    _release_and_sweep_gc_fixture,
)


@pytest.mark.asyncio
async def test_real_postgres_v4_abandonment_resumes_after_cancellation(
    monkeypatch,
):
    """A canceled bounded cleanup keeps reachability and resumes exactly."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1")

    database = Database()
    schema_name = f"ptg2_v4_abandon_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    build_token = "a" * 32
    await database.connect()
    monkeypatch.setattr(shared_gc, "db", database)
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await _create_v4_abandonment_schema(connection, schema)
            block_hashes = await _insert_v4_abandonment_fixture(
                connection,
                schema,
                build_token=build_token,
            )

        with pytest.raises(asyncio.CancelledError):
            await shared_gc.abandon_owned_v4_layout(
                schema_name=schema_name,
                snapshot_key=77,
                build_token=build_token,
                grace_seconds=60,
                progress_callback=_cancel_after_build_pin_batch,
                options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=2),
            )
        await _assert_partial_v4_abandonment(
            database,
            schema,
            build_token=build_token,
        )

        resumed = await shared_gc.abandon_owned_v4_layout(
            schema_name=schema_name,
            snapshot_key=77,
            build_token=build_token,
            grace_seconds=60,
            options=shared_gc.PTG2V4AbandonmentOptions(batch_rows=2),
        )

        assert resumed == shared_gc.PTG2SharedLayoutGCStats(1, 3, 3)
        await _assert_completed_v4_abandonment(
            database,
            schema,
            block_hashes,
        )
    finally:
        await _drop_test_schema_and_disconnect(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_candidate_scoped_release_and_sweep_sql():
    """Exercise candidate-scoped layout release and block sweep in PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_shared_gc_test_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    block_hash = _hash(20)
    unrelated_hash = _hash(21)
    await database.connect()
    try:
        async with database.acquire() as connection:
            await connection.status(f"CREATE SCHEMA {schema}")
            await _create_gc_layout_schema(connection, schema)
            await _create_gc_block_schema(connection, schema)
            await _insert_gc_layout_fixture(connection, schema)
            await _insert_gc_block_fixture(
                connection,
                schema,
                block_hash,
                unrelated_hash,
            )
        await _release_and_sweep_gc_fixture(
            database,
            schema_name,
            schema,
            block_hash,
        )
        await _assert_gc_fixture_storage(database, schema)
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
