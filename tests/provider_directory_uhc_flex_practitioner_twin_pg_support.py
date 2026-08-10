# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL support for sealed Practitioner twin proofs."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from db.connection import Database
from process.uhc_flex_practitioner_query import (
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_store import (
    build_uhc_flex_practitioner_acquisition_identity,
    claim_uhc_flex_practitioner_work,
    complete_uhc_flex_practitioner_result,
    initialize_uhc_flex_practitioner_acquisition,
    seal_uhc_flex_practitioner_acquisition,
)
from process.uhc_flex_practitioner_twin_store import (
    admit_uhc_flex_practitioner_twins,
    require_uhc_flex_practitioner_admission,
    UHCFlexPractitionerTwinAdmission,
    UHCFlexPractitionerTwinStoreError,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
)
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    cohort_fixture,
    create_provider_foundation,
    insert_valid_cohort,
    MEMBER_NPIS,
    seed_official_dataset,
)


ACQUISITION_TABLE = "provider_directory_uhc_flex_practitioner_acquisition"
ATTEMPT_TABLE = "provider_directory_uhc_flex_practitioner_twin_attempt"
ADMISSION_TABLE = "provider_directory_uhc_flex_practitioner_twin_admission"
PROJECTION_DATE = "2026-08-10"
_ACQUISITION_GUARD = "pd_uhc_flex_practitioner_acquisition_guard"


@dataclass(frozen=True)
class TwinSealedPair:
    baseline_id: str
    candidate_id: str
    terminal_set_sha256: str
    resource_count: int


def configure_database(monkeypatch, url) -> Database:
    """Bind the repository database wrapper to the disposable server."""

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    return Database()


def _matched_query(npi: int, variant: str = "same"):
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 1,
            "entry": [{"resource": {
                "resourceType": "Practitioner",
                "id": f"synthetic-{npi}-{variant}",
                "identifier": [{
                    "system": "http://hl7.org/fhir/sid/us-npi",
                    "value": str(npi),
                }],
            }}],
        },
    )


def _unmatched_query(npi: int):
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {"resourceType": "Bundle", "type": "searchset", "total": 0},
    )


async def _seal_acquisition(
    database: Database,
    identity,
    *,
    first_variant: str,
    is_first_match: bool = True,
):
    await initialize_uhc_flex_practitioner_acquisition(identity, database=database)
    for index, npi in enumerate(MEMBER_NPIS):
        claim = await claim_uhc_flex_practitioner_work(
            identity.acquisition_id,
            requested_npi=npi,
            database=database,
        )
        assert claim is not None
        query_result = (
            _matched_query(npi, first_variant)
            if index == 0 and is_first_match
            else _unmatched_query(npi)
        )
        await complete_uhc_flex_practitioner_result(
            claim,
            query_result,
            database=database,
        )
    return await seal_uhc_flex_practitioner_acquisition(identity, database=database)


async def sealed_pair(
    database: Database,
    *,
    operation_key: str,
    candidate_variant: str = "same",
) -> TwinSealedPair:
    """Persist and seal one derived baseline/candidate acquisition pair."""

    cohort = cohort_fixture()
    intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        cohort.cohort_id,
        PROJECTION_DATE,
        operation_key,
    )
    identities = tuple(
        build_uhc_flex_practitioner_acquisition_identity(
            cohort,
            acquisition_role=role,
            run_id=build_uhc_flex_practitioner_run_id(intent_id, role),
            dataset_intent_id=intent_id,
        )
        for role in ("baseline", "candidate")
    )
    baseline_summary = await _seal_acquisition(
        database,
        identities[0],
        first_variant="same",
    )
    candidate_summary = await _seal_acquisition(
        database,
        identities[1],
        first_variant=candidate_variant,
    )
    if candidate_variant == "same":
        assert candidate_summary.terminal_set_sha256 == baseline_summary.terminal_set_sha256
        assert candidate_summary.resource_count == baseline_summary.resource_count
    return TwinSealedPair(
        baseline_id=identities[0].acquisition_id,
        candidate_id=identities[1].acquisition_id,
        terminal_set_sha256=baseline_summary.terminal_set_sha256,
        resource_count=baseline_summary.resource_count,
    )


async def prepare_schema(
    engine,
    url,
    schema_name: str,
    cohort_migration,
    acquisition_migration,
    twin_migration,
) -> None:
    """Install the exact foundation through twin revision in one schema."""

    schema = quoted(schema_name)
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
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
    await run_migration(engine, twin_migration, "upgrade")


async def admit_and_replay_match(
    database: Database,
) -> tuple[TwinSealedPair, UHCFlexPractitionerTwinAdmission]:
    """Prove matched admission, exact replay, lookup, and date rejection."""

    matched_pair = await sealed_pair(database, operation_key="a" * 64)
    admission = await admit_uhc_flex_practitioner_twins(
        matched_pair.baseline_id,
        matched_pair.candidate_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key="a" * 64,
        database=database,
    )
    replay = await admit_uhc_flex_practitioner_twins(
        matched_pair.baseline_id,
        matched_pair.candidate_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key="a" * 64,
        database=database,
    )
    assert replay == admission
    assert admission.terminal_set_sha256 == matched_pair.terminal_set_sha256
    assert admission.resource_count == matched_pair.resource_count
    assert await require_uhc_flex_practitioner_admission(
        matched_pair.candidate_id,
        database=database,
    ) == admission
    with pytest.raises(UHCFlexPractitionerTwinStoreError) as date_error:
        await require_uhc_flex_practitioner_admission(
            matched_pair.candidate_id,
            semantic_projection_as_of="2026-08-11",
            operation_key="a" * 64,
            database=database,
        )
    assert date_error.value.code == "identity"
    return matched_pair, admission


async def burn_mismatch(database: Database) -> TwinSealedPair:
    """Prove mismatch persists once and raises only outside the transaction."""

    mismatched_pair = await sealed_pair(
        database,
        operation_key="b" * 64,
        candidate_variant="drift",
    )
    for _ in range(2):
        with pytest.raises(UHCFlexPractitionerTwinStoreError) as mismatch:
            await admit_uhc_flex_practitioner_twins(
                mismatched_pair.baseline_id,
                mismatched_pair.candidate_id,
                semantic_projection_as_of=PROJECTION_DATE,
                operation_key="b" * 64,
                database=database,
            )
        assert mismatch.value.code == "mismatch"
    return mismatched_pair


_ATTEMPT_COPY_COLUMNS = (
    "attempt_id", "attempt_contract_id", "semantic_projection_as_of",
    "operation_key", "baseline_acquisition_id", "candidate_acquisition_id",
    "cohort_id", "dataset_intent_id", "source_id", "connector_id",
    "query_contract_id", "storage_contract_id", "baseline_run_id",
    "candidate_run_id", "expected_npi_count", "baseline_terminal_set_sha256",
    "candidate_terminal_set_sha256", "baseline_resource_count",
    "candidate_resource_count", "matched",
)


def attempt_copy_sql(
    schema: str,
    attempt_id: str,
    **override_by_field: str,
) -> str:
    """Build one direct attempt replay or tamper statement."""

    selected_expressions = [
        override_by_field.get(name, name) for name in _ATTEMPT_COPY_COLUMNS
    ]
    return (
        f"INSERT INTO {schema}.{ATTEMPT_TABLE} "
        f"({', '.join(_ATTEMPT_COPY_COLUMNS)}) "
        f"SELECT {', '.join(selected_expressions)} "
        f"FROM {schema}.{ATTEMPT_TABLE} WHERE attempt_id = '{attempt_id}'"
    )


async def assert_persisted_attempts(
    connection,
    schema: str,
    matched_pair: TwinSealedPair,
    mismatched_pair: TwinSealedPair,
) -> None:
    """Require one matched and one burned mismatch, with one authority."""

    matched_attempt = await connection.fetchrow(
        f"SELECT matched FROM {schema}.{ATTEMPT_TABLE} "
        "WHERE candidate_acquisition_id = $1",
        matched_pair.candidate_id,
    )
    mismatch_attempt = await connection.fetchrow(
        f"SELECT matched FROM {schema}.{ATTEMPT_TABLE} "
        "WHERE candidate_acquisition_id = $1",
        mismatched_pair.candidate_id,
    )
    assert matched_attempt is not None and matched_attempt["matched"] is True
    assert mismatch_attempt is not None and mismatch_attempt["matched"] is False
    assert await connection.fetchval(
        f"SELECT count(*) FROM {schema}.{ATTEMPT_TABLE}"
    ) == 2
    assert await connection.fetchval(
        f"SELECT count(*) FROM {schema}.{ADMISSION_TABLE}"
    ) == 1


def _invalid_attempt_statements(
    schema: str,
    attempt_id: str,
    reused_candidate_id: str,
) -> tuple[str, ...]:
    override_maps = (
        {"semantic_projection_as_of": "DATE '2026-08-11'"},
        {"operation_key": "'c' || repeat('0', 63)"},
        {"cohort_id": "'pdufc_' || repeat('0', 48)"},
        {"dataset_intent_id": "'pdufdi_' || repeat('0', 48)"},
        {"source_id": "'source-drift'"},
        {"connector_id": "'connector-drift'"},
        {"query_contract_id": "'query-drift'"},
        {"storage_contract_id": "'storage-drift'"},
        {"expected_npi_count": "expected_npi_count + 1"},
        {"baseline_terminal_set_sha256": "repeat('0', 64)"},
        {"candidate_acquisition_id": f"'{reused_candidate_id}'"},
    )
    return tuple(
        attempt_copy_sql(schema, attempt_id, **override_by_field)
        for override_by_field in override_maps
    )


async def assert_attempt_guards(
    connection,
    schema: str,
    admission: UHCFlexPractitionerTwinAdmission,
    mismatched_pair: TwinSealedPair,
) -> str:
    """Require exact replay and reject field, root, and pair reuse."""

    exact_replay = attempt_copy_sql(
        schema,
        admission.attempt_id,
    ) + " ON CONFLICT DO NOTHING"
    assert await connection.execute(exact_replay) == "INSERT 0 0"
    invalid_statements = _invalid_attempt_statements(
        schema,
        admission.attempt_id,
        mismatched_pair.candidate_id,
    )
    for statement in invalid_statements:
        await assert_sqlstate(connection, "55000", statement)
    return exact_replay


async def assert_admission_and_immutability_guards(
    connection,
    schema: str,
    admission: UHCFlexPractitionerTwinAdmission,
) -> None:
    """Reject admission tamper plus update, delete, and truncate."""

    columns = (
        "admission_id, admission_contract_id, semantic_projection_as_of, "
        "operation_key, attempt_id, baseline_acquisition_id, "
        "candidate_acquisition_id, cohort_id, dataset_intent_id, source_id, "
        "connector_id, query_contract_id, storage_contract_id, baseline_run_id, "
        "candidate_run_id, expected_npi_count, terminal_set_sha256, "
        "resource_count, publication_authority"
    )
    tamper_statement = (
        f"INSERT INTO {schema}.{ADMISSION_TABLE} ({columns}) SELECT "
        "admission_id, admission_contract_id, semantic_projection_as_of, "
        "operation_key, attempt_id, baseline_acquisition_id, "
        "candidate_acquisition_id, cohort_id, dataset_intent_id, source_id, "
        "connector_id, query_contract_id, storage_contract_id, baseline_run_id, "
        "candidate_run_id, expected_npi_count, repeat('0', 64), "
        f"resource_count, true FROM {schema}.{ADMISSION_TABLE} "
        f"WHERE admission_id = '{admission.admission_id}'"
    )
    await assert_sqlstate(connection, "55000", tamper_statement)
    immutable_statements = (
        f"UPDATE {schema}.{ATTEMPT_TABLE} SET matched = false",
        f"DELETE FROM {schema}.{ADMISSION_TABLE}",
        f"TRUNCATE TABLE {schema}.{ADMISSION_TABLE}, {schema}.{ATTEMPT_TABLE}",
    )
    for statement in immutable_statements:
        await assert_sqlstate(connection, "55000", statement)


async def _set_candidate_building(connection, schema: str, candidate_id: str) -> None:
    await connection.execute(
        f"UPDATE {schema}.{ACQUISITION_TABLE} SET status = 'building', "
        "cohort_complete = false, pending_count = NULL, leased_count = NULL, "
        "matched_count = NULL, unmatched_count = NULL, error_count = NULL, "
        "resource_count = NULL, terminal_set_sha256 = NULL, sealed_at = NULL "
        "WHERE acquisition_id = $1",
        candidate_id,
    )


async def _restore_candidate(
    connection,
    schema: str,
    matched_pair: TwinSealedPair,
) -> None:
    await connection.execute(
        f"UPDATE {schema}.{ACQUISITION_TABLE} SET status = 'sealed', "
        "cohort_complete = true, pending_count = 0, leased_count = 0, "
        "matched_count = 1, unmatched_count = 1, error_count = 0, "
        "resource_count = $2, terminal_set_sha256 = $3, "
        "sealed_at = transaction_timestamp() WHERE acquisition_id = $1",
        matched_pair.candidate_id,
        matched_pair.resource_count,
        matched_pair.terminal_set_sha256,
    )


async def assert_unsealed_root_guard(
    connection,
    schema: str,
    matched_pair: TwinSealedPair,
    exact_replay: str,
) -> None:
    """Tamper one root into valid building state and reject its replay."""

    await connection.execute(
        f"ALTER TABLE {schema}.{ACQUISITION_TABLE} "
        f"DISABLE TRIGGER {_ACQUISITION_GUARD}"
    )
    try:
        await _set_candidate_building(
            connection,
            schema,
            matched_pair.candidate_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {schema}.{ACQUISITION_TABLE} "
            f"ENABLE ALWAYS TRIGGER {_ACQUISITION_GUARD}"
        )
    await assert_sqlstate(connection, "55000", exact_replay)
    await connection.execute(
        f"ALTER TABLE {schema}.{ACQUISITION_TABLE} "
        f"DISABLE TRIGGER {_ACQUISITION_GUARD}"
    )
    try:
        await _restore_candidate(connection, schema, matched_pair)
    finally:
        await connection.execute(
            f"ALTER TABLE {schema}.{ACQUISITION_TABLE} "
            f"ENABLE ALWAYS TRIGGER {_ACQUISITION_GUARD}"
        )


async def assert_swapped_roles_rejected(
    database: Database,
    matched_pair: TwinSealedPair,
) -> None:
    """Reject reversing exact baseline and candidate roles."""

    with pytest.raises(UHCFlexPractitionerTwinStoreError) as role_error:
        await admit_uhc_flex_practitioner_twins(
            matched_pair.candidate_id,
            matched_pair.baseline_id,
            semantic_projection_as_of=PROJECTION_DATE,
            operation_key="a" * 64,
            database=database,
        )
    assert role_error.value.code == "identity"
