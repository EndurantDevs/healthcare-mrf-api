# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
import datetime as dt

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.models import MRFPayer, MRFSource, db
from process.mrf_source_discovery import _retag_sources_for_discovery_run
from process.mrf_discovery_checkpoints import (
    DatabaseDiscoveryCheckpointStore,
    DiscoverySourceBatchMismatch,
    SourceProcessResult,
    source_set_sha256,
)


OPT_IN_DSN_ENV = "HLTHPRT_MRF_DISCOVERY_CHECKPOINT_POSTGRES_DSN"
DISPOSABLE_DATABASE_PATTERN = re.compile(
    r"^ptg2_v3_lifecycle_test_[a-z0-9][a-z0-9_]{7,}$"
)
TEST_ROOT_RUN_ID = "mrf_discovery_checkpoint_test_root"


def _database_url():
    dsn = os.getenv(OPT_IN_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {OPT_IN_DSN_ENV} to run PostgreSQL checkpoint tests")
    database_url = make_url(dsn)
    if not database_url.drivername.startswith("postgresql"):
        pytest.fail(f"{OPT_IN_DSN_ENV} must use PostgreSQL")
    database_name = str(database_url.database or "")
    if not DISPOSABLE_DATABASE_PATTERN.fullmatch(database_name):
        pytest.fail(f"refusing non-disposable PostgreSQL database {database_name!r}")
    if not database_url.host or not database_url.username:
        pytest.fail(f"{OPT_IN_DSN_ENV} must include an explicit host and user")
    return database_url


async def _connect(database_url):
    return await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )


async def _clear_test_batch(database_url) -> None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    connection = await _connect(database_url)
    try:
        await connection.execute(
            f'DELETE FROM "{schema}"."mrf_discovery_batch" '
            "WHERE root_run_id = $1",
            TEST_ROOT_RUN_ID,
        )
    finally:
        await connection.close()


async def _prepare_catalog_source() -> None:
    await db.create_table(MRFPayer.__table__, checkfirst=True)
    await db.create_table(MRFSource.__table__, checkfirst=True)
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
    async with db.session() as session:
        await session.execute(
            MRFSource.__table__.delete().where(
                MRFSource.source_id == "source_retag"
            )
        )
        await session.execute(
            MRFSource.__table__.insert().values(
                source_id="source_retag",
                source_key="source-retag-test",
                display_name="Retag Test",
                status="active",
                metadata_json={"discovery_run_id": "run_parent"},
                created_at=now,
                updated_at=now,
            )
        )


async def _delete_catalog_source() -> None:
    async with db.session() as session:
        await session.execute(
            MRFSource.__table__.delete().where(
                MRFSource.source_id == "source_retag"
            )
        )


@pytest.mark.asyncio
async def test_postgres_retry_replays_only_unfinished_sources_and_fences_owner():
    """Persist retry progress while rejecting stale checkpoint owners."""

    database_url = _database_url()
    source_records = [
        {
            "source_id": "source_alpha",
            "payer_id": "payer_alpha",
            "index_url": "https://example.test/alpha.json",
        },
        {
            "source_id": "source_beta",
            "payer_id": "payer_beta",
            "index_url": "https://example.test/beta.json",
        },
    ]
    checkpoint_store = DatabaseDiscoveryCheckpointStore()
    await db.disconnect()
    await _clear_test_batch(database_url)
    try:
        frozen_records = await checkpoint_store.initialize_batch(
            TEST_ROOT_RUN_ID,
            "run_parent",
            source_records,
        )
        assert frozen_records == source_records
        assert await checkpoint_store.is_source_claimed(
            TEST_ROOT_RUN_ID, "source_alpha", "run_parent"
        )
        assert await checkpoint_store.is_source_completed(
            TEST_ROOT_RUN_ID,
            "source_alpha",
            "run_parent",
            SourceProcessResult(urls_checked=1, plans_discovered=2),
        )
        assert await checkpoint_store.is_source_claimed(
            TEST_ROOT_RUN_ID, "source_beta", "run_parent"
        )
        assert await checkpoint_store.is_source_failed(
            TEST_ROOT_RUN_ID,
            "source_beta",
            "run_parent",
            RuntimeError("temporary source failure"),
        )

        failed_summary = await checkpoint_store.summarize_batch(
            TEST_ROOT_RUN_ID, "run_parent"
        )
        assert failed_summary.completed_source_count == 1
        assert failed_summary.failed_source_count == 1
        assert failed_summary.is_complete is False

        resumed_records = await checkpoint_store.resume_batch(
            TEST_ROOT_RUN_ID,
            "run_retry",
            "run_parent",
        )
        assert resumed_records == source_records
        assert await checkpoint_store.pending_sources(TEST_ROOT_RUN_ID) == [
            source_records[1]
        ]
        assert not await checkpoint_store.is_source_completed(
            TEST_ROOT_RUN_ID,
            "source_beta",
            "run_parent",
            SourceProcessResult(),
        )
        assert await checkpoint_store.is_source_claimed(
            TEST_ROOT_RUN_ID, "source_beta", "run_retry"
        )
        assert await checkpoint_store.is_source_completed(
            TEST_ROOT_RUN_ID,
            "source_beta",
            "run_retry",
            SourceProcessResult(urls_checked=1, files_discovered=3),
        )

        completed_summary = await checkpoint_store.summarize_batch(
            TEST_ROOT_RUN_ID, "run_retry"
        )
        expected_digest = source_set_sha256(["source_alpha", "source_beta"])
        assert completed_summary.is_complete is True
        assert completed_summary.source_set_count == 2
        assert completed_summary.completed_source_count == 2
        assert completed_summary.source_set_sha256 == expected_digest
        assert completed_summary.completed_source_set_sha256 == expected_digest
        assert completed_summary.urls_checked == 2
        assert completed_summary.plans_discovered == 2
        assert completed_summary.files_discovered == 3

        with pytest.raises(
            DiscoverySourceBatchMismatch,
            match="direct child",
        ):
            await checkpoint_store.resume_batch(
                TEST_ROOT_RUN_ID,
                "run_invalid_retry",
                "run_parent",
            )
    finally:
        await db.disconnect()
        await _clear_test_batch(database_url)


@pytest.mark.asyncio
async def test_postgres_retag_updates_only_resume_metadata():
    database_url = _database_url()
    await db.disconnect()
    try:
        await _prepare_catalog_source()
        await _retag_sources_for_discovery_run(
            [
                {
                    "source_id": "source_retag",
                    "source_key": "source-retag-test",
                    "display_name": "Retag Test",
                    "status": "active",
                    "metadata_json": {"discovery_run_id": "run_parent"},
                    "created_at": "2026-07-15T12:00:00",
                    "updated_at": "2026-07-15T12:00:00",
                }
            ],
            "run_retry",
        )
        async with db.session() as session:
            source_row = (
                await session.execute(
                    MRFSource.__table__.select().where(
                        MRFSource.source_id == "source_retag"
                    )
                )
            ).mappings().one()
        assert source_row["metadata_json"]["discovery_run_id"] == "run_retry"
        assert isinstance(source_row["created_at"], dt.datetime)
        assert isinstance(source_row["updated_at"], dt.datetime)
    finally:
        await _delete_catalog_source()
        await db.disconnect()
