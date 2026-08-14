# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for restart-safe UHC admission receipts."""

from __future__ import annotations

from pathlib import Path
import uuid

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from tests.formulary_fhir_twin_admission_pg_support import ADMISSION_PATH
from tests.formulary_fhir_twin_admission_pg_support import ATTEMPT_PATH
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import FOUNDATION_PATH
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.uhc_receipt_postgres_support import assert_receipt_catalog
from tests.uhc_receipt_postgres_support import assert_receipt_immutability
from tests.uhc_receipt_postgres_support import CANDIDATE_DATASET_ID
from tests.uhc_receipt_postgres_support import seed_pending_artifacts
from tests.uhc_receipt_postgres_support import seed_source_and_twins
from tests.uhc_receipt_postgres_support import unicode_artifact_set
from tests.uhc_receipt_postgres_support import verify_root_and_insert_receipt
from tests.uhc_selected_receipt_postgres_support import selected_artifact_set
from tests.uhc_selected_receipt_postgres_support import (
    verify_selected_root_and_insert_receipt,
)


VERSIONS = Path(FOUNDATION_PATH).parent
ARTIFACT_PATH = VERSIONS / (
    "20260810030000_fhir_formulary_source_artifact.py"
)
RECEIPT_PATH = VERSIONS / (
    "20260810040000_fhir_formulary_uhc_admission_receipt.py"
)
SELECTED_RECEIPT_PATH = VERSIONS / (
    "20260814010000_fhir_formulary_uhc_selected_receipt.py"
)


async def _assert_stored_receipt(
    connection: asyncpg.Connection,
    schema_name: str,
    receipt_id: str,
    artifact_root: str,
) -> None:
    stored_receipt = await connection.fetchrow(
        f"SELECT receipt_id, candidate_dataset_id, artifact_set_sha256, "
        "file_count, plan_count, medication_membership_count FROM "
        f"{quoted(schema_name)}.fhir_formulary_uhc_admission_receipt"
    )
    assert dict(stored_receipt) == {
        "receipt_id": receipt_id,
        "candidate_dataset_id": CANDIDATE_DATASET_ID,
        "artifact_set_sha256": artifact_root,
        "file_count": 48,
        "plan_count": 2,
        "medication_membership_count": 5,
    }


async def _exercise_receipt(
    connection: asyncpg.Connection,
    schema_name: str,
) -> tuple[str, str]:
    exact_set = unicode_artifact_set()
    async with connection.transaction():
        await seed_source_and_twins(connection, schema_name)
        await seed_pending_artifacts(connection, schema_name, exact_set)
        receipt_id = await verify_root_and_insert_receipt(
            connection,
            schema_name,
            exact_set,
        )
    await assert_receipt_catalog(connection, schema_name)
    await assert_receipt_immutability(connection, schema_name)
    await _assert_stored_receipt(
        connection,
        schema_name,
        receipt_id,
        exact_set.artifact_set_sha256,
    )
    return receipt_id, exact_set.artifact_set_sha256


async def _assert_upgraded_receipt(
    connection: asyncpg.Connection,
    schema_name: str,
    receipt_id: str,
    artifact_root: str,
) -> None:
    upgraded = await connection.fetchrow(
        "SELECT receipt_id, artifact_set_sha256, file_count, expected_file_count, "
        "excluded_file_count, cardinality(selected_source_file_ids) AS selected_count, "
        f"exclusion_code FROM {quoted(schema_name)}.fhir_formulary_uhc_admission_receipt"
    )
    assert dict(upgraded) == {
        "receipt_id": receipt_id,
        "artifact_set_sha256": artifact_root,
        "file_count": 48,
        "expected_file_count": 48,
        "excluded_file_count": 0,
        "selected_count": 48,
        "exclusion_code": None,
    }


@pytest.mark.asyncio
async def test_uhc_receipt_postgres_root_identity_and_immutability(
    monkeypatch,
) -> None:
    """PostgreSQL independently binds exact 24+24 bytes to one receipt."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    legacy_migration_paths = (
        FOUNDATION_PATH,
        ATTEMPT_PATH,
        ADMISSION_PATH,
        ARTIFACT_PATH,
        RECEIPT_PATH,
    )
    migrations = tuple(
        load_migration(path, f"uhc_receipt_{index}")
        for index, path in enumerate(legacy_migration_paths)
    )
    receipt_migration = migrations[-1]
    selected_receipt_migration = load_migration(
        SELECTED_RECEIPT_PATH,
        "uhc_selected_receipt",
    )
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {quoted(schema_name)}"
            )
        for migration in migrations:
            await run_migration(engine, migration, "upgrade")
        connection = await connect(url)
        try:
            receipt_id, artifact_root = await _exercise_receipt(
                connection,
                schema_name,
            )
            await run_migration(engine, selected_receipt_migration, "upgrade")
            await _assert_upgraded_receipt(connection, schema_name, receipt_id, artifact_root)
            await assert_receipt_catalog(
                connection,
                schema_name,
                selected=True,
            )
            await run_migration(engine, selected_receipt_migration, "downgrade")
            with pytest.raises(DBAPIError, match="downgrade_blocked"):
                await run_migration(engine, receipt_migration, "downgrade")
        finally:
            await connection.close()
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_uhc_partial_receipt_postgres_round_trip_and_recovery(
    monkeypatch,
) -> None:
    """PostgreSQL admits an exact subset and keeps it stable after recovery."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    migration_paths = (
        FOUNDATION_PATH,
        ATTEMPT_PATH,
        ADMISSION_PATH,
        ARTIFACT_PATH,
        RECEIPT_PATH,
        SELECTED_RECEIPT_PATH,
    )
    migrations = tuple(
        load_migration(path, f"uhc_partial_receipt_{index}")
        for index, path in enumerate(migration_paths)
    )
    selected_receipt_migration = migrations[-1]
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {quoted(schema_name)}"
            )
        for migration in migrations:
            await run_migration(engine, migration, "upgrade")
        connection = await connect(url)
        try:
            exact_set = unicode_artifact_set()
            selected_set = selected_artifact_set(exact_set, 47)
            async with connection.transaction():
                await seed_source_and_twins(connection, schema_name)
                await seed_pending_artifacts(connection, schema_name, exact_set)
                await verify_selected_root_and_insert_receipt(
                    connection,
                    schema_name,
                    exact_set,
                    selected_set,
                )
            await assert_receipt_catalog(
                connection,
                schema_name,
                selected=True,
            )
            await assert_receipt_immutability(connection, schema_name)
            with pytest.raises(DBAPIError, match="downgrade_blocked"):
                await run_migration(
                    engine,
                    selected_receipt_migration,
                    "downgrade",
                )
        finally:
            await connection.close()
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()
