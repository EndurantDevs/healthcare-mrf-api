# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL claim-order proof for exact Flex Practitioner work."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.uhc_flex_practitioner_store import (
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_error,
    complete_uhc_flex_practitioner_result,
    initialize_uhc_flex_practitioner_acquisition,
    release_uhc_flex_practitioner_work,
    seal_uhc_flex_practitioner_acquisition,
)
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS
from tests.test_provider_directory_uhc_flex_practitioner_acquisition_postgres import (
    _configure_database,
    _matched,
    _prepare_schema,
    _role_identities,
    _unmatched,
    VERSIONS,
)


CONTENT_TYPE_RETRY_PATH = VERSIONS / (
    "20260824143000_uhc_flex_content_type_retry.py"
)


async def _prepare_downgraded_content_type_recovery_schema(
    engine, url, schema_name
):
    await _prepare_schema(engine, url, schema_name)
    migration = load_migration(
        CONTENT_TYPE_RETRY_PATH,
        "flex_content_type_retry",
    )
    await run_migration(engine, migration, "upgrade")
    await run_migration(engine, migration, "downgrade")
    return migration


async def _claim_exact(database, acquisition_id, requested_npi):
    claim = await claim_uhc_flex_practitioner_work(
        acquisition_id,
        requested_npi=requested_npi,
        database=database,
    )
    assert claim is not None
    return claim


async def _claim_general(database, acquisition_id, *, fresh_only=None):
    claim = await claim_uhc_flex_practitioner_work(
        acquisition_id, fresh_only=fresh_only, database=database
    )
    assert claim is not None
    return claim


@pytest.mark.asyncio
async def test_general_claim_prefers_fresh_work_but_exact_retry_stays_exact(
    monkeypatch,
) -> None:
    """Claim fresh then least-attempted work without changing exact claims."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        await _prepare_schema(engine, url, schema_name)
        await database.connect()
        baseline, _candidate = _role_identities()
        assert await initialize_uhc_flex_practitioner_acquisition(
            baseline, database=database
        ) == 1
        retried_npi, fresh_npi = MEMBER_NPIS
        for expected_attempt in (1, 2):
            retry_claim = await _claim_exact(
                database, baseline.acquisition_id, retried_npi
            )
            assert retry_claim.attempt == expected_attempt
            await release_uhc_flex_practitioner_work(
                retry_claim,
                database=database,
            )

        general_claim = await _claim_general(
            database, baseline.acquisition_id
        )
        assert (general_claim.requested_npi, general_claim.attempt) == (
            fresh_npi,
            1,
        )
        await release_uhc_flex_practitioner_work(
            general_claim,
            database=database,
        )
        tail_claim = await _claim_general(
            database,
            baseline.acquisition_id,
            fresh_only=False,
        )
        assert (tail_claim.requested_npi, tail_claim.attempt) == (
            fresh_npi,
            2,
        )
        exact_retry = await _claim_exact(
            database, baseline.acquisition_id, retried_npi
        )
        assert (exact_retry.requested_npi, exact_retry.attempt) == (
            retried_npi,
            3,
        )
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_claim_recovers_legacy_content_type_error_without_losing_results(
    monkeypatch,
) -> None:
    """Reclaim the now-retryable error while preserving completed work."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        migration = await _prepare_downgraded_content_type_recovery_schema(
            engine, url, schema_name
        )
        await database.connect()
        baseline, _candidate = _role_identities()
        assert await initialize_uhc_flex_practitioner_acquisition(
            baseline, database=database
        ) == 1
        completed_npi, failed_npi = MEMBER_NPIS
        completed_claim = await _claim_exact(
            database, baseline.acquisition_id, completed_npi
        )
        await complete_uhc_flex_practitioner_result(
            completed_claim, _matched(completed_npi), database=database
        )
        failed_claim = await _claim_exact(
            database, baseline.acquisition_id, failed_npi
        )
        assert failed_claim.attempt == 1
        await complete_uhc_flex_practitioner_error(
            failed_claim, error_code="content_type_invalid", database=database
        )
        await database.disconnect()
        await run_migration(engine, migration, "upgrade")
        await database.connect()
        recovered_claim = await claim_uhc_flex_practitioner_work(
            baseline.acquisition_id, database=database
        )
        assert recovered_claim is not None
        assert recovered_claim.requested_npi == failed_npi
        assert recovered_claim.attempt == 2
        await complete_uhc_flex_practitioner_result(
            recovered_claim, _unmatched(failed_npi), database=database
        )
        summary = await seal_uhc_flex_practitioner_acquisition(
            baseline, database=database
        )
        assert (
            summary.matched_count,
            summary.unmatched_count,
            summary.error_count,
            summary.resource_count,
        ) == (1, 1, 0, 1)
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


@pytest.mark.asyncio
async def test_claim_keeps_other_and_repeated_errors_terminal(monkeypatch) -> None:
    """Keep semantic and later-attempt media failures terminal."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        migration = await _prepare_downgraded_content_type_recovery_schema(
            engine, url, schema_name
        )
        await run_migration(engine, migration, "upgrade")
        await database.connect()
        _baseline, candidate = _role_identities()
        await initialize_uhc_flex_practitioner_acquisition(candidate, database=database)
        semantic_npi, repeated_npi = MEMBER_NPIS
        semantic_claim = await _claim_exact(
            database, candidate.acquisition_id, semantic_npi
        )
        await complete_uhc_flex_practitioner_error(
            semantic_claim, error_code="response_validation_cross_npi", database=database
        )
        first_claim = await _claim_exact(
            database, candidate.acquisition_id, repeated_npi
        )
        await release_uhc_flex_practitioner_work(first_claim, database=database)
        second_claim = await claim_uhc_flex_practitioner_work(
            candidate.acquisition_id,
            requested_npi=repeated_npi,
            database=database,
        )
        assert second_claim is not None
        assert second_claim.attempt == 2
        await complete_uhc_flex_practitioner_error(
            second_claim, error_code="content_type_invalid", database=database
        )
        assert await claim_uhc_flex_practitioner_work(
            candidate.acquisition_id, database=database
        ) is None
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
