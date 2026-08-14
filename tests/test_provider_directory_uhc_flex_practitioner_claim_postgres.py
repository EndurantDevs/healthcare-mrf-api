# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL claim-order proof for exact Flex Practitioner work."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.uhc_flex_practitioner_store import (
    claim_uhc_flex_practitioner_work,
    initialize_uhc_flex_practitioner_acquisition,
    release_uhc_flex_practitioner_work,
)
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS
from tests.test_provider_directory_uhc_flex_practitioner_acquisition_postgres import (
    _configure_database,
    _prepare_schema,
    _role_identities,
)


@pytest.mark.asyncio
async def test_general_claim_prefers_fresh_work_but_exact_retry_stays_exact(
    monkeypatch,
) -> None:
    """Claim untouched work before a retry without changing exact claims."""

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
            retry_claim = await claim_uhc_flex_practitioner_work(
                baseline.acquisition_id,
                requested_npi=retried_npi,
                database=database,
            )
            assert retry_claim is not None
            assert retry_claim.attempt == expected_attempt
            await release_uhc_flex_practitioner_work(
                retry_claim,
                database=database,
            )

        general_claim = await claim_uhc_flex_practitioner_work(
            baseline.acquisition_id,
            database=database,
        )
        assert general_claim is not None
        assert (general_claim.requested_npi, general_claim.attempt) == (
            fresh_npi,
            1,
        )
        exact_retry = await claim_uhc_flex_practitioner_work(
            baseline.acquisition_id,
            requested_npi=retried_npi,
            database=database,
        )
        assert exact_retry is not None
        assert (exact_retry.requested_npi, exact_retry.attempt) == (
            retried_npi,
            3,
        )
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
