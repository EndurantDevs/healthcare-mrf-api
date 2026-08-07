# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for fixed synthetic seed publication."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db.models import db
import process.formulary_fhir.synthetic_seed_publisher as publisher_module
from process.formulary_fhir.repository_shared import row_mapping
from process.formulary_fhir.repository_shared import table_name
from process.formulary_fhir.synthetic_canary import (
    verify_synthetic_seed_candidate,
)
from process.formulary_fhir.synthetic_canary_contract import CANARY_ENABLED_ENV
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import (
    SEED_PUBLICATION_ENABLED_ENV,
)
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_seed_publisher import (
    SyntheticSeedPublicationError,
)
from process.formulary_fhir.synthetic_seed_publisher import publish_synthetic_seed
from tests.test_formulary_fhir_repository_postgres import _configure_database
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action
from tests.test_formulary_fhir_synthetic_canary_postgres import TABLES


async def _prepare_schema(monkeypatch, database_url, schema_name, engine):
    _configure_database(monkeypatch, database_url, schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")
    await db.disconnect()
    monkeypatch.setenv(CANARY_ENABLED_ENV, "true")
    await verify_synthetic_seed_candidate()
    monkeypatch.delenv(CANARY_ENABLED_ENV, raising=False)
    monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, "true")


async def _table_fingerprints() -> dict[str, tuple[int, str]]:
    fingerprints_by_table: dict[str, tuple[int, str]] = {}
    for table in TABLES:
        fingerprint_by_field = row_mapping(
            await db.first(
                f"SELECT count(*) AS row_count, md5(COALESCE(string_agg("
                f"to_jsonb(stored)::text, E'\\n' ORDER BY "
                f"to_jsonb(stored)::text), '')) AS content_hash FROM "
                f"{table_name(table)} AS stored;"
            )
        )
        fingerprints_by_table[table] = (
            fingerprint_by_field["row_count"],
            fingerprint_by_field["content_hash"],
        )
    return fingerprints_by_table


async def _assert_published_state(published_at) -> None:
    source_by_field = row_mapping(
        await db.first(
            f"SELECT source_id, enabled FROM "
            f"{table_name('fhir_formulary_source')};"
        )
    )
    dataset_by_field = row_mapping(
        await db.first(
            f"SELECT dataset_id, status, publish_requested, seed_eligible, "
            f"published_at FROM {table_name('fhir_formulary_dataset')};"
        )
    )
    pointer_by_field = row_mapping(
        await db.first(
            f"SELECT source_id, dataset_id, generation, published_at FROM "
            f"{table_name('fhir_formulary_current')};"
        )
    )
    expected_dataset_id = expected_evidence()["dataset_id"]
    assert source_by_field == {"source_id": CANARY_SOURCE_ID, "enabled": False}
    assert dataset_by_field == {
        "dataset_id": expected_dataset_id,
        "status": "published",
        "publish_requested": False,
        "seed_eligible": True,
        "published_at": published_at,
    }
    assert pointer_by_field == {
        "source_id": CANARY_SOURCE_ID,
        "dataset_id": expected_dataset_id,
        "generation": 1,
        "published_at": published_at,
    }


async def _assert_verified_without_pointer() -> None:
    assert await db.scalar(
        f"SELECT status FROM {table_name('fhir_formulary_dataset')};"
    ) == "verified"
    assert await db.scalar(
        f"SELECT count(*) FROM {table_name('fhir_formulary_current')};"
    ) == 0
    assert await db.scalar(
        f"SELECT enabled FROM {table_name('fhir_formulary_source')};"
    ) is False


@pytest.mark.asyncio
async def test_synthetic_seed_publish_and_replay_are_byte_stable(monkeypatch):
    """Publish generation one, then prove an exact idempotent replay."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        await _assert_verified_without_pointer()
        candidate_fingerprints = await _table_fingerprints()
        first_publication = await publish_synthetic_seed()
        first_fingerprints = await _table_fingerprints()
        await _assert_published_state(first_publication.published_at)
        immutable_tables = set(TABLES) - {
            "fhir_formulary_dataset",
            "fhir_formulary_current",
        }
        assert {
            table: first_fingerprints[table]
            for table in immutable_tables
        } == {
            table: candidate_fingerprints[table]
            for table in immutable_tables
        }
        assert candidate_fingerprints["fhir_formulary_current"][0] == 0
        assert first_fingerprints["fhir_formulary_current"][0] == 1

        replay_publication = await publish_synthetic_seed()
        replay_fingerprints = await _table_fingerprints()

        assert replay_publication == first_publication
        assert replay_fingerprints == first_fingerprints
        await _assert_published_state(first_publication.published_at)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_seed_publishers_return_one_exact_generation(monkeypatch):
    """Prove the shared source lease serializes identical publication calls."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        pool_size=5,
    )
    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)

        first_publication, second_publication = await asyncio.gather(
            publish_synthetic_seed(),
            publish_synthetic_seed(),
        )

        assert first_publication == second_publication
        assert first_publication.generation == 1
        await _assert_published_state(first_publication.published_at)
        assert await db.scalar(
            f"SELECT count(*) FROM {table_name('fhir_formulary_current')};"
        ) == 1
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_verifier_dataset_lock_cannot_deadlock_source_first_publisher(
    monkeypatch,
):
    """Prove graph recomputation never asks for source after dataset lock."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        pool_size=5,
    )
    dataset_locked = asyncio.Event()
    publisher_reached_dataset = asyncio.Event()
    original_locked_dataset = publisher_module._locked_dataset_row

    async def observed_publisher_lock(database):
        publisher_reached_dataset.set()
        return await original_locked_dataset(database)

    async def verify_while_locked():
        async with db.transaction():
            dataset_by_field = await original_locked_dataset(db)
            dataset = publisher_module._candidate_dataset(dataset_by_field)
            dataset_locked.set()
            await publisher_reached_dataset.wait()
            verification = await publisher_module._recompute_dataset_verification(
                db,
                CANARY_SOURCE_ID,
                dataset,
            )
            publisher_module._require_exact_verification(verification)

    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        verifier_task = asyncio.create_task(verify_while_locked())
        await dataset_locked.wait()
        monkeypatch.setattr(
            publisher_module,
            "_locked_dataset_row",
            observed_publisher_lock,
        )
        publisher_task = asyncio.create_task(publish_synthetic_seed())
        async with asyncio.timeout(5):
            await asyncio.gather(verifier_task, publisher_task)

        await _assert_published_state(publisher_task.result().published_at)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_cancellation_after_nested_publish_rolls_back_and_retries(
    monkeypatch,
):
    """Cancel before outer commit, prove rollback, then publish successfully."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    nested_publication_finished = asyncio.Event()
    original_postflight = publisher_module._postflight

    async def blocked_postflight(_database, _publication):
        nested_publication_finished.set()
        await asyncio.Event().wait()

    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        candidate_fingerprints = await _table_fingerprints()
        monkeypatch.setattr(
            publisher_module,
            "_postflight",
            blocked_postflight,
        )
        publication_task = asyncio.create_task(publish_synthetic_seed())
        await nested_publication_finished.wait()
        publication_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await publication_task

        await _assert_verified_without_pointer()
        assert await _table_fingerprints() == candidate_fingerprints
        monkeypatch.setattr(
            publisher_module,
            "_postflight",
            original_postflight,
        )
        publication = await publish_synthetic_seed()
        await _assert_published_state(publication.published_at)
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_pointer_drift_rejects_without_mutation(monkeypatch):
    """Reject a nonempty seed pointer without changing any stored row."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    try:
        await _prepare_schema(monkeypatch, database_url, schema_name, engine)
        inserted_count = await db.status(
            f"INSERT INTO {table_name('fhir_formulary_current')} ("
            "source_id, dataset_id, generation) VALUES ("
            ":source_id, :dataset_id, 2);",
            source_id=CANARY_SOURCE_ID,
            dataset_id=expected_evidence()["dataset_id"],
        )
        assert inserted_count == 1
        baseline_fingerprints = await _table_fingerprints()

        with pytest.raises(SyntheticSeedPublicationError) as caught:
            await publish_synthetic_seed()

        assert caught.value.code == "catalog"
        assert await _table_fingerprints() == baseline_fingerprints
        assert await db.scalar(
            f"SELECT status FROM {table_name('fhir_formulary_dataset')};"
        ) == "verified"
        assert await db.scalar(
            f"SELECT generation FROM {table_name('fhir_formulary_current')};"
        ) == 2
    finally:
        await db.disconnect()
        await _drop_schema(engine, schema_name)
        await engine.dispose()
