# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL lifecycle proof for the UHC Flex NPI cohort."""

from __future__ import annotations

from pathlib import Path
import uuid

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
import pytest

from tests.formulary_fhir_twin_admission_pg_support import ADMISSION_PATH
from tests.formulary_fhir_twin_admission_pg_support import ATTEMPT_PATH
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import FOUNDATION_PATH
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    assert_cohort_immutability,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    assert_invalid_sources_block_header,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    assert_stored_cohort,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    create_provider_foundation,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    insert_valid_cohort,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    seed_official_dataset,
)


VERSIONS = Path(FOUNDATION_PATH).parent
ARTIFACT_PATH = VERSIONS / (
    "20260810030000_fhir_formulary_source_artifact.py"
)
RECEIPT_PATH = VERSIONS / (
    "20260810040000_fhir_formulary_uhc_admission_receipt.py"
)
COHORT_PATH = VERSIONS / (
    "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
)


async def _apply_foundation_migrations(engine) -> object:
    migration_paths = (
        FOUNDATION_PATH,
        ATTEMPT_PATH,
        ADMISSION_PATH,
        ARTIFACT_PATH,
        RECEIPT_PATH,
    )
    for index, migration_path in enumerate(migration_paths):
        migration = load_migration(migration_path, f"uhc_flex_base_{index}")
        await run_migration(engine, migration, "upgrade")
    return load_migration(COHORT_PATH, "uhc_flex_cohort")


@pytest.mark.asyncio
async def test_uhc_flex_npi_cohort_postgres_lifecycle(monkeypatch) -> None:
    """PostgreSQL seals only an exact current official Practitioner NPI set."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(
                f"CREATE SCHEMA {quoted(schema_name)}"
            )
        cohort_migration = await _apply_foundation_migrations(engine)
        connection = await connect(url)
        try:
            await create_provider_foundation(connection, schema_name)
        finally:
            await connection.close()
        await run_migration(engine, cohort_migration, "upgrade")
        connection = await connect(url)
        try:
            await seed_official_dataset(connection, schema_name)
            await assert_invalid_sources_block_header(connection, schema_name)
            await insert_valid_cohort(connection, schema_name)
            await assert_stored_cohort(connection, schema_name)
            await assert_cohort_immutability(connection, schema_name)
            with pytest.raises(DBAPIError, match="downgrade_blocked"):
                await run_migration(engine, cohort_migration, "downgrade")
        finally:
            await connection.close()
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()
