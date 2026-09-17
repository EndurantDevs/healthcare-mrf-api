# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional native SQL and atomic-publication proof in an owned database."""

import asyncio
import copy
import importlib
import os
import uuid
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import new_york_profile_store as module
from process import provider_profile_source_store as shared
from tests.test_new_york_profile_acquisition import source_session
from tests.test_new_york_profile_store import RUN_ID, _substitute_corroboration_receipt, captured_case


@asynccontextmanager
async def _database(monkeypatch):
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set the profile PostgreSQL DSN for native store tests")
    admin_url = make_url(dsn).set(drivername="postgresql")
    name = "ny_profile_store_test_" + uuid.uuid4().hex
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    engine, is_creation_attempted = None, False
    try:
        assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", name) == 0
        is_creation_attempted = True
        await admin.execute(f'CREATE DATABASE "{name}" TEMPLATE template0')
        engine = create_async_engine(admin_url.set(database=name, drivername="postgresql+asyncpg"))
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        assert await database.scalar("SELECT current_database()") == name
        monkeypatch.setattr(shared, "db", database)
        florida = importlib.import_module("process.florida_mqa_profile")
        monkeypatch.setattr(florida, "db", database)
        await shared.ensure_tables()
        yield database
    finally:
        try:
            if engine is not None:
                await engine.dispose()
            if is_creation_attempted:
                await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
                assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", name) == 0
                assert await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=$1", name) == 0
        finally:
            await admin.close(timeout=5)
            assert admin.is_closed()


async def _retain(database, case):
    await module.store.claim_run(case.run)
    for model, stored_rows in (
        (shared.ProviderProfileArtifact, [case.artifact]),
        (shared.ProviderProfileSourceRecord, case.records),
        (shared.ProviderProfileFact, case.facts),
    ):
        await database.insert(model.__table__).values(stored_rows).status()


async def _assert_unpublished(database):
    source_run = await module.store._read_run(RUN_ID)
    assert source_run["status"] == "running"
    assert await module.store.read_publication() is None
    fact_table = shared.ProviderProfileFact.__table__
    assert await database.first(select(fact_table).where(fact_table.c.published_at.is_not(None))) is None


async def _assert_support_substitution_cannot_publish(database, case):
    tampered = copy.deepcopy(case)
    _substitute_corroboration_receipt(tampered)
    record_table, artifact_table = (
        shared.ProviderProfileSourceRecord.__table__,
        shared.ProviderProfileArtifact.__table__,
    )
    async with database.transaction() as transaction:
        await (
            database.update(record_table)
            .where(record_table.c.record_id == tampered.records[0]["record_id"])
            .values(match_evidence=tampered.records[0]["match_evidence"])
            .status()
        )
        await (
            database.update(artifact_table)
            .where(artifact_table.c.run_id == RUN_ID)
            .values(
                **{field: tampered.artifact[field] for field in ("content_sha256", "content_bytes", "metadata_json")}
            )
            .status()
        )
        actual = await database.first(
            select(record_table).where(record_table.c.record_id == tampered.records[0]["record_id"])
        )
        assert module._hash(dict(actual._mapping)) == tampered.profiles["111111"]["record_sha256"]
        assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_records"] == 1
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        await _assert_unpublished(database)
        await transaction.rollback()


async def test_native_store_checks_actual_rows_and_rolls_back_cancelled_publication(captured_case, monkeypatch):
    case = captured_case
    async with _database(monkeypatch) as database:
        await _retain(database, case)
        counts = await module.store.retained_counts(RUN_ID)
        assert counts["acquired_profiles"] == counts["retained_source_records"] == 2
        assert counts["held_attempts"] == counts["matched_public_providers"] == 1
        assert module.store._completion_metrics(case.run, case.metrics, counts)["retained_facts"] == 2
        await _assert_support_substitution_cannot_publish(database, case)
        fact_table = shared.ProviderProfileFact.__table__
        async with database.transaction() as transaction:
            await (
                database.update(fact_table)
                .where(fact_table.c.fact_id == case.facts[0]["fact_id"])
                .values(value_json={})
                .status()
            )
            with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
                await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
            await _assert_unpublished(database)
            await transaction.rollback()
        original_complete = type(module.store)._complete_run

        async def cancelled_after_update(self, run_id, metrics):
            await original_complete(self, run_id, metrics)
            raise asyncio.CancelledError

        with monkeypatch.context() as patch:
            patch.setattr(type(module.store), "_complete_run", cancelled_after_update)
            with pytest.raises(asyncio.CancelledError):
                await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        await _assert_unpublished(database)
        published = await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        assert published["published"] is True
        assert (await module.store.read_publication())["current_run_id"] == RUN_ID
        assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_facts"] == 0
        source_table = shared.ProviderProfileSourceRecord.__table__
        unmatched = await database.first(select(source_table).where(source_table.c.license_number == "222222"))
        assert unmatched.matched_npi is None and unmatched.match_status == "identity_conflict"
        assert await database.first(select(source_table).where(source_table.c.license_number == "333333")) is None
