# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path
import uuid

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
import pytest

from db.connection import Database
from process.uhc_flex_practitioner_query import (
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_store import (
    build_uhc_flex_practitioner_acquisition_identity,
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_error,
    complete_uhc_flex_practitioner_result,
    heartbeat_uhc_flex_practitioner_work,
    initialize_uhc_flex_practitioner_acquisition,
    read_uhc_flex_practitioner_resource_page,
    release_uhc_flex_practitioner_work,
    seal_uhc_flex_practitioner_acquisition,
    UHCFlexPractitionerStoreError,
)
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import cohort_fixture
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    create_provider_foundation,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    insert_valid_cohort,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    seed_official_dataset,
)


VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
COHORT_PATH = VERSIONS / "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
ACQUISITION_PATH = VERSIONS / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
RESOURCE_ORDER_REPAIR_PATH = VERSIONS / (
    "20260811130000_provider_directory_exact_practitioner_resource_order_repair.py"
)
WORK_TABLE = "provider_directory_uhc_flex_practitioner_work"
ACQUISITION_TABLE = "provider_directory_uhc_flex_practitioner_acquisition"
RESOURCE_TABLE = "provider_directory_uhc_flex_practitioner_resource"


def _matched(npi: int):
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 1,
            "entry": [{"resource": {
                "resourceType": "Practitioner",
                "id": f"synthetic-{npi}",
                "identifier": [{
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "value": str(npi),
                }],
                "communication": [{"language": {"text": "English"}}],
            }}],
        },
    )


def _unmatched(npi: int):
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {"resourceType": "Bundle", "type": "searchset", "total": 0},
    )


def _matched_mixed_case(npi: int):
    entries = [
        {
            "resource": {
                "resourceType": "Practitioner",
                "id": resource_id,
                "identifier": [
                    {
                        "system": "http://hl7.org/fhir/sid/us-npi",
                        "value": str(npi),
                    }
                ],
            }
        }
        for resource_id in ("practitioner-a", "practitioner-A")
    ]
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(entries),
            "entry": entries,
        },
    )


def _configure_database(monkeypatch, url) -> Database:
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    return Database()


async def _expire_claim(connection, schema_name: str, acquisition_id: str) -> None:
    schema = quoted(schema_name)
    trigger = "pd_uhc_flex_practitioner_work_guard"
    await connection.execute(
        f"ALTER TABLE {schema}.{WORK_TABLE} DISABLE TRIGGER {trigger}"
    )
    try:
        await connection.execute(
            f"UPDATE {schema}.{WORK_TABLE} SET lease_expires_at = "
            "clock_timestamp() - interval '1 second' "
            "WHERE acquisition_id = $1 AND status = 'leased'",
            acquisition_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {schema}.{WORK_TABLE} ENABLE ALWAYS TRIGGER {trigger}"
        )


async def _prepare_schema(engine, url, schema_name: str) -> tuple[object, object]:
    cohort_migration = load_migration(COHORT_PATH, "flex_store_cohort")
    acquisition_migration = load_migration(
        ACQUISITION_PATH,
        "flex_store_acquisition",
    )
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {quoted(schema_name)}"
        )
    connection = await connect(url)
    try:
        await create_provider_foundation(connection, schema_name)
    finally:
        await connection.close()
    await run_migration(engine, cohort_migration, "upgrade")
    connection = await connect(url)
    try:
        await seed_official_dataset(connection, schema_name)
        await insert_valid_cohort(connection, schema_name)
    finally:
        await connection.close()
    await run_migration(engine, acquisition_migration, "upgrade")
    return cohort_migration, acquisition_migration


def _role_identities():
    cohort = cohort_fixture()
    intent_id = "pdufdi_" + "1" * 48
    baseline = build_uhc_flex_practitioner_acquisition_identity(
        cohort,
        acquisition_role="baseline",
        run_id="pdufpr_" + "2" * 48,
        dataset_intent_id=intent_id,
    )
    candidate = build_uhc_flex_practitioner_acquisition_identity(
        cohort,
        acquisition_role="candidate",
        run_id="pdufpr_" + "3" * 48,
        dataset_intent_id=intent_id,
    )
    return baseline, candidate


async def _initialize_roles(database, baseline, candidate) -> None:
    assert await initialize_uhc_flex_practitioner_acquisition(
        baseline,
        database=database,
    ) == 1
    assert await initialize_uhc_flex_practitioner_acquisition(
        baseline,
        database=database,
    ) == 0
    assert await initialize_uhc_flex_practitioner_acquisition(
        candidate,
        database=database,
    ) == 1


async def _release_and_retry(database, baseline, second_npi: int) -> None:
    initial_claim = await claim_uhc_flex_practitioner_work(
        baseline.acquisition_id,
        requested_npi=second_npi,
        database=database,
    )
    assert initial_claim is not None
    await release_uhc_flex_practitioner_work(initial_claim, database=database)
    with pytest.raises(UHCFlexPractitionerStoreError) as release_error:
        await release_uhc_flex_practitioner_work(initial_claim, database=database)
    assert release_error.value.code == "lease_lost"
    retry_claim = await claim_uhc_flex_practitioner_work(
        baseline.acquisition_id,
        requested_npi=second_npi,
        database=database,
    )
    assert retry_claim is not None
    assert retry_claim.attempt == initial_claim.attempt + 1
    assert retry_claim.lease_token != initial_claim.lease_token
    await complete_uhc_flex_practitioner_result(
        retry_claim,
        _unmatched(second_npi),
        database=database,
    )


async def _complete_baseline(database, baseline):
    first_npi, second_npi = MEMBER_NPIS
    first_claim = await claim_uhc_flex_practitioner_work(
        baseline.acquisition_id,
        requested_npi=first_npi,
        database=database,
    )
    assert first_claim is not None
    await heartbeat_uhc_flex_practitioner_work(
        first_claim,
        lease_seconds=600,
        database=database,
    )
    await complete_uhc_flex_practitioner_result(
        first_claim,
        _matched(first_npi),
        database=database,
    )
    with pytest.raises(DBAPIError, match="acquisition_incomplete"):
        await seal_uhc_flex_practitioner_acquisition(baseline, database=database)
    await _release_and_retry(database, baseline, second_npi)
    summary = await seal_uhc_flex_practitioner_acquisition(
        baseline,
        database=database,
    )
    assert (
        summary.matched_count,
        summary.unmatched_count,
        summary.error_count,
        summary.resource_count,
    ) == (1, 1, 0, 1)
    assert summary.cohort_complete is True
    assert summary.endpoint_collection_complete is False
    assert summary.endpoint_complete is False


async def _complete_error_candidate(database, url, schema_name, candidate) -> None:
    first_npi, second_npi = MEMBER_NPIS
    stale_claim = await claim_uhc_flex_practitioner_work(
        candidate.acquisition_id,
        requested_npi=first_npi,
        database=database,
    )
    assert stale_claim is not None and stale_claim.attempt == 1
    connection = await connect(url)
    try:
        await _expire_claim(connection, schema_name, candidate.acquisition_id)
    finally:
        await connection.close()
    reclaimed = await claim_uhc_flex_practitioner_work(
        candidate.acquisition_id,
        requested_npi=first_npi,
        database=database,
    )
    assert reclaimed is not None and reclaimed.attempt == 2
    assert reclaimed.lease_token != stale_claim.lease_token
    with pytest.raises(UHCFlexPractitionerStoreError) as stale_error:
        await complete_uhc_flex_practitioner_result(
            stale_claim,
            _unmatched(first_npi),
            database=database,
        )
    assert stale_error.value.code == "lease_lost"
    await complete_uhc_flex_practitioner_error(
        reclaimed,
        error_code="response_validation",
        database=database,
    )
    second_claim = await claim_uhc_flex_practitioner_work(
        candidate.acquisition_id,
        requested_npi=second_npi,
        database=database,
    )
    assert second_claim is not None
    await complete_uhc_flex_practitioner_result(
        second_claim,
        _unmatched(second_npi),
        database=database,
    )
    with pytest.raises(DBAPIError, match="acquisition_incomplete"):
        await seal_uhc_flex_practitioner_acquisition(candidate, database=database)


async def _assert_manifest(database, acquisition_id: str) -> None:
    page = await read_uhc_flex_practitioner_resource_page(
        acquisition_id,
        limit=1,
        database=database,
    )
    assert len(page) == 1
    assert page[0].requested_npi == MEMBER_NPIS[0]
    assert page[0].payload_sha256 == hashlib.sha256(
        page[0].payload_json_text.encode("utf-8")
    ).hexdigest()


async def _assert_database_guards(url, schema_name: str, candidate) -> None:
    connection = await connect(url)
    try:
        schema = quoted(schema_name)
        derived_source_count = await connection.fetchval(
            f"SELECT count(*) FROM {schema}.provider_directory_source "
            "WHERE source_id = 'pdfhir_1ceb7c0986c320b7eb924881'"
        )
        assert derived_source_count == 0
        failed_header = await connection.fetchrow(
            f"SELECT status, cohort_complete, terminal_set_sha256 "
            f"FROM {schema}.{ACQUISITION_TABLE} WHERE acquisition_id = $1",
            candidate.acquisition_id,
        )
        assert tuple(failed_header) == ("building", False, None)
        statements = (
            f"UPDATE {schema}.{WORK_TABLE} SET error_code = 'drift'",
            f"DELETE FROM {schema}.{ACQUISITION_TABLE}",
            f"TRUNCATE TABLE {schema}.{RESOURCE_TABLE}",
        )
        for guarded_statement in statements:
            await assert_sqlstate(connection, "55000", guarded_statement)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_flex_practitioner_acquisition_postgres_lifecycle(monkeypatch) -> None:
    """Exercise roles, retry release, reclaim, exact seal, and payload reads."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        _, acquisition_migration = await _prepare_schema(engine, url, schema_name)
        await database.connect()
        baseline, candidate = _role_identities()
        await _initialize_roles(database, baseline, candidate)
        await _complete_baseline(database, baseline)
        await _complete_error_candidate(
            database,
            url,
            schema_name,
            candidate,
        )
        await _assert_manifest(database, baseline.acquisition_id)
        await _assert_database_guards(url, schema_name, candidate)
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(engine, acquisition_migration, "downgrade")
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
async def _complete_mixed_case_result(database, url, schema_name) -> None:
    await database.connect()
    baseline, candidate = _role_identities()
    await _initialize_roles(database, baseline, candidate)
    requested_npi = MEMBER_NPIS[0]
    claim = await claim_uhc_flex_practitioner_work(
        baseline.acquisition_id,
        requested_npi=requested_npi,
        database=database,
    )
    assert claim is not None
    query_result = _matched_mixed_case(requested_npi)
    assert query_result.resource_ids == (
        "practitioner-A",
        "practitioner-a",
    )
    await complete_uhc_flex_practitioner_result(
        claim,
        query_result,
        database=database,
    )

    connection = await connect(url)
    try:
        stored = await connection.fetchrow(
            f"SELECT status, result_sha256, resource_count "
            f"FROM {quoted(schema_name)}.{WORK_TABLE} "
            "WHERE acquisition_id = $1 AND npi = $2",
            baseline.acquisition_id,
            requested_npi,
        )
    finally:
        await connection.close()
    assert tuple(stored) == (
        "matched",
        query_result.result_sha256,
        2,
    )


@pytest.mark.asyncio
async def test_exact_practitioner_result_hash_uses_codepoint_resource_order(
    monkeypatch,
) -> None:
    """Keep PostgreSQL result hashing aligned with Python for mixed-case IDs."""

    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    try:
        await _prepare_schema(engine, url, schema_name)
        connection = await connect(url)
        try:
            schema = quoted(schema_name)
            collation_ref = f"{schema}.exact_practitioner_test_order"
            await connection.execute(
                f"CREATE COLLATION {collation_ref} "
                "(provider = icu, locale = 'en-US')"
            )
            await connection.execute(
                f"ALTER TABLE {schema}.{RESOURCE_TABLE} "
                "ALTER COLUMN resource_id TYPE varchar(64) "
                f"COLLATE {collation_ref} USING resource_id::varchar(64)"
            )
            locale_order = await connection.fetchval(
                "SELECT string_agg(resource_id, ',' ORDER BY resource_id) "
                f"FROM (VALUES ('practitioner-a' COLLATE {collation_ref}), "
                f"('practitioner-A' COLLATE {collation_ref})) "
                "AS ordering(resource_id)"
            )
        finally:
            await connection.close()
        assert locale_order == "practitioner-a,practitioner-A"
        repair_migration = load_migration(
            RESOURCE_ORDER_REPAIR_PATH,
            "exact_practitioner_resource_order_repair",
        )
        await run_migration(engine, repair_migration, "upgrade")
        connection = await connect(url)
        try:
            function_definition = await connection.fetchval(
                "SELECT pg_catalog.pg_get_functiondef("
                "pg_catalog.to_regprocedure($1));",
                (
                    f'{quoted(schema_name)}.'
                    '"guard_pd_uhc_flex_practitioner_work"()'
                ),
            )
        finally:
            await connection.close()
        assert 'ORDER BY resource.resource_id COLLATE pg_catalog."C"' in (
            function_definition
        )
        await _complete_mixed_case_result(database, url, schema_name)
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()
