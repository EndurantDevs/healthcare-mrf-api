# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path
import uuid

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
import pytest

from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_practitioner_twin_pg_support import (
    admit_and_replay_match,
    assert_admission_and_immutability_guards,
    assert_attempt_guards,
    assert_persisted_attempts,
    assert_swapped_roles_rejected,
    assert_unsealed_root_guard,
    burn_mismatch,
    configure_database,
    prepare_schema,
)


VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
COHORT_PATH = VERSIONS / "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
ACQUISITION_PATH = VERSIONS / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
TWIN_PATH = VERSIONS / (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission.py"
)


async def _assert_database_guards(
    url,
    schema: str,
    matched_pair,
    mismatched_pair,
    admission,
) -> None:
    """Run the direct-SQL attempt, admission, and root tamper packet."""

    connection = await connect(url)
    try:
        await assert_persisted_attempts(
            connection,
            schema,
            matched_pair,
            mismatched_pair,
        )
        exact_replay = await assert_attempt_guards(
            connection,
            schema,
            admission,
            mismatched_pair,
        )
        await assert_admission_and_immutability_guards(
            connection,
            schema,
            admission,
        )
        await assert_unsealed_root_guard(
            connection,
            schema,
            matched_pair,
            exact_replay,
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_flex_practitioner_twin_attempt_and_authority_postgres(
    monkeypatch,
) -> None:
    """Prove durable comparison, authority, replay, and database guards."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = configure_database(monkeypatch, url)
    cohort_migration = load_migration(COHORT_PATH, "flex_twin_cohort")
    acquisition_migration = load_migration(
        ACQUISITION_PATH,
        "flex_twin_acquisition",
    )
    twin_migration = load_migration(TWIN_PATH, "flex_twin_admission")
    try:
        await prepare_schema(
            engine,
            url,
            schema_name,
            cohort_migration,
            acquisition_migration,
            twin_migration,
        )
        await database.connect()
        matched_pair, admission = await admit_and_replay_match(database)
        mismatched_pair = await burn_mismatch(database)
        await _assert_database_guards(
            url,
            schema,
            matched_pair,
            mismatched_pair,
            admission,
        )
        await assert_swapped_roles_rejected(database, matched_pair)
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(engine, twin_migration, "downgrade")
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
