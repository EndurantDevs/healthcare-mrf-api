# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import uuid

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
import pytest

from db.connection import Database
import process.uhc_flex_practitioner_publication as publication
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
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    UHCFlexPractitionerTwinStoreError,
)
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    cohort_fixture,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    create_provider_foundation,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    insert_valid_cohort,
)
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import DATASET_ID
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import MEMBER_NPIS
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import (
    seed_official_dataset,
)
from tests.provider_directory_rooted_graph_pg_support import (
    extend_publication_foundation as extend_rooted_publication_foundation,
)
from tests.provider_directory_uhc_flex_publication_pg_support import (
    extend_flex_publication_foundation as _extend_provider_foundation,
    seed_exact_publication_registry as _seed_exact_source,
)


VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
COHORT_PATH = VERSIONS / "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
ACQUISITION_PATH = VERSIONS / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
TWIN_PATH = VERSIONS / (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission.py"
)
PUBLICATION_PATH = VERSIONS / (
    "20260810080000_provider_directory_uhc_flex_practitioner_publication.py"
)
ROOTED_PUBLICATION_PATH = VERSIONS / (
    "20260811020000_provider_directory_rooted_graph_acquisition.py"
)
SINGLE_ROOT_PATH = VERSIONS / (
    "20260812030000_provider_directory_specialized_single_root_admission.py"
)
PROJECTION_DATE = "2026-08-10"
SOURCE_ID = "pdfhir_1ceb7c0986c320b7eb924881"
ENDPOINT_ID = "ad53a7446514ed65b3a8ea7ab68ceb9a1ef85bf6c04fcb882219ecb50928bab5"


def _configure_database(monkeypatch, url) -> Database:
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    return Database()


def _query_result(npi: int, matched: bool):
    entries = []
    if matched:
        entries.append(
            {
                "resource": {
                    "resourceType": "Practitioner",
                    "id": f"synthetic-{npi}",
                    "identifier": [
                        {
                            "system": "http://hl7.org/fhir/sid/us-npi",
                            "value": str(npi),
                        }
                    ],
                    "name": [{"family": "Synthetic", "given": ["Alex"]}],
                }
            }
        )
    return validate_uhc_flex_practitioner_search_bundle(
        npi,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(entries),
            "entry": entries,
        },
    )


async def _sealed_pair(
    database: Database,
    *,
    operation_key: str,
    matched: bool,
    admit: bool = True,
    cohort=None,
):
    selected_cohort = cohort if cohort is not None else cohort_fixture()
    intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        selected_cohort.cohort_id,
        PROJECTION_DATE,
        operation_key,
    )
    identities = []
    for role in ("baseline", "candidate"):
        identity = build_uhc_flex_practitioner_acquisition_identity(
            selected_cohort,
            acquisition_role=role,
            run_id=build_uhc_flex_practitioner_run_id(intent_id, role),
            dataset_intent_id=intent_id,
        )
        identities.append(identity)
        await initialize_uhc_flex_practitioner_acquisition(
            identity,
            database=database,
        )
        for npi_index, npi in enumerate(MEMBER_NPIS):
            claim = await claim_uhc_flex_practitioner_work(
                identity.acquisition_id,
                requested_npi=npi,
                database=database,
            )
            assert claim is not None
            await complete_uhc_flex_practitioner_result(
                claim,
                _query_result(npi, matched and npi_index == 0),
                database=database,
            )
        await seal_uhc_flex_practitioner_acquisition(
            identity,
            database=database,
        )
    if not admit:
        return identities[1].acquisition_id
    return await admit_uhc_flex_practitioner_twins(
        identities[0].acquisition_id,
        identities[1].acquisition_id,
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=operation_key,
        database=database,
    )


async def _prepare_publication_schema(
    engine,
    url,
    schema_name: str,
    schema: str,
    migrations: tuple,
    *,
    include_rooted_registry: bool = False,
) -> None:
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
    connection = await connect(url)
    try:
        await create_provider_foundation(connection, schema_name)
    finally:
        await connection.close()
    await run_migration(engine, migrations[0], "upgrade")
    connection = await connect(url)
    try:
        await seed_official_dataset(connection, schema_name)
        await insert_valid_cohort(connection, schema_name)
    finally:
        await connection.close()
    await run_migration(engine, migrations[1], "upgrade")
    await run_migration(engine, migrations[2], "upgrade")
    connection = await connect(url)
    try:
        await _extend_provider_foundation(connection, schema_name)
    finally:
        await connection.close()
    await run_migration(engine, migrations[3], "upgrade")
    connection = await connect(url)
    try:
        await _seed_exact_source(
            connection,
            schema_name,
            include_rooted=include_rooted_registry,
        )
    finally:
        await connection.close()


@asynccontextmanager
async def _publication_test_scope(monkeypatch):
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    schema = quoted(schema_name)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = _configure_database(monkeypatch, url)
    migrations = tuple(
        load_migration(path, f"flex_publication_{index}")
        for index, path in enumerate(
            (COHORT_PATH, ACQUISITION_PATH, TWIN_PATH, PUBLICATION_PATH)
        )
    )
    rooted_migration = load_migration(
        ROOTED_PUBLICATION_PATH,
        "flex_publication_rooted",
    )
    single_root_migration = load_migration(
        SINGLE_ROOT_PATH,
        "flex_publication_single_root",
    )
    try:
        await _prepare_publication_schema(
            engine,
            url,
            schema_name,
            schema,
            migrations,
            include_rooted_registry=True,
        )
        connection = await connect(url)
        try:
            await extend_rooted_publication_foundation(connection, schema_name)
        finally:
            await connection.close()
        await run_migration(engine, rooted_migration, "upgrade")
        await run_migration(engine, single_root_migration, "upgrade")
        await database.connect()
        yield url, schema, database, engine, migrations[3]
    finally:
        await database.disconnect()
        await drop_schema(engine, schema_name)
        await engine.dispose()


async def _assert_admission_gate(database: Database, schema: str) -> None:
    candidate_id = await _sealed_pair(
        database,
        operation_key="0" * 64,
        matched=False,
        admit=False,
    )
    with pytest.raises(UHCFlexPractitionerTwinStoreError):
        await publication.publish_uhc_flex_practitioner_dataset(
            candidate_id,
            database=database,
            batch_size=1,
        )
    assert (
        await database.scalar(
            f"SELECT count(*) FROM {schema}."
            "provider_directory_uhc_flex_practitioner_dataset"
        )
        == 0
    )


async def _publish_successive_datasets(database: Database):
    first_admission = await _sealed_pair(
        database,
        operation_key="a" * 64,
        matched=True,
    )
    first = await publication.publish_uhc_flex_practitioner_dataset(
        first_admission.candidate_acquisition_id,
        database=database,
        batch_size=1,
    )
    assert first.replayed is False
    assert first.readiness.resource_count == 1
    assert first.readiness.endpoint_collection_complete is False
    assert first.readiness.endpoint_complete is False
    replay = await publication.publish_uhc_flex_practitioner_dataset(
        first_admission.candidate_acquisition_id,
        database=database,
        batch_size=1,
    )
    assert replay.replayed is True
    assert replay.readiness == first.readiness

    empty_admission = await _sealed_pair(
        database,
        operation_key="b" * 64,
        matched=False,
    )
    empty = await publication.publish_uhc_flex_practitioner_dataset(
        empty_admission.candidate_acquisition_id,
        database=database,
        batch_size=1,
    )
    assert empty.replayed is False
    assert empty.readiness.resource_count == 0
    assert empty.readiness.previous_dataset_id == first.readiness.dataset_id
    assert (
        await publication.load_current_uhc_flex_dataset_readiness(database=database)
        == empty.readiness
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="replay is not current",
    ):
        await publication.publish_uhc_flex_practitioner_dataset(
            first_admission.candidate_acquisition_id,
            database=database,
            batch_size=1,
        )
    return first, empty


async def _assert_exact_removal(connection, schema: str, first, empty) -> None:
    statuses = await connection.fetch(
        f"SELECT dataset_id, status, is_current FROM {schema}."
        "provider_directory_uhc_flex_practitioner_dataset "
        "ORDER BY created_at, dataset_id"
    )
    assert [tuple(status_record) for status_record in statuses] == [
        (first.readiness.dataset_id, "superseded", False),
        (empty.readiness.dataset_id, "published", True),
    ]
    current_resource_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
        f"AS resource JOIN {schema}."
        "provider_directory_uhc_flex_practitioner_dataset AS header "
        "ON header.dataset_id = resource.dataset_id "
        "WHERE header.is_current IS TRUE"
    )
    assert current_resource_count == 0
    old_resource_count = await connection.fetchval(
        f"SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
        "WHERE dataset_id = $1",
        first.readiness.dataset_id,
    )
    assert old_resource_count == 1


async def _assert_official_dataset_rotation_revokes_readiness(
    connection,
    schema: str,
    dataset_id: str,
) -> None:
    ready_function = f"{schema}.provider_directory_uhc_flex_practitioner_dataset_ready"
    assert (
        await connection.fetchval(
            f"SELECT {ready_function}($1)",
            dataset_id,
        )
        is True
    )
    await connection.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET status = 'superseded', is_current = false, "
        "superseded_at = transaction_timestamp() WHERE dataset_id = $1",
        DATASET_ID,
    )
    assert (
        await connection.fetchval(
            f"SELECT {ready_function}($1)",
            dataset_id,
        )
        is False
    )


async def _assert_mutation_guards(connection, schema: str) -> None:
    guarded_statements = (
        (
            "55000",
            f"UPDATE {schema}.provider_directory_uhc_flex_practitioner_dataset "
            "SET dataset_hash = repeat('0', 64) WHERE is_current",
        ),
        (
            {"23514", "55000"},
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = '{}'::jsonb WHERE is_current",
        ),
        (
            "55000",
            f"UPDATE {schema}.provider_directory_uhc_flex_practitioner_"
            "dataset_resource SET payload_hash = repeat('0', 64)",
        ),
        (
            "55000",
            f"UPDATE {schema}.provider_directory_source SET metadata_json = "
            "jsonb_set(metadata_json, '{provider_directory_authority_id}', "
            "'\"drift\"'::jsonb) WHERE source_id = '" + SOURCE_ID + "'",
        ),
        (
            "55000",
            f"UPDATE {schema}.provider_directory_api_endpoint SET metadata_json = "
            "jsonb_set(metadata_json, '{authority_id}', '\"drift\"'::jsonb) "
            "WHERE endpoint_id = '" + ENDPOINT_ID + "'",
        ),
        (
            "55000",
            f"TRUNCATE TABLE {schema}."
            "provider_directory_uhc_flex_practitioner_dataset_resource",
        ),
        (
            "55000",
            f"TRUNCATE TABLE {schema}."
            "provider_directory_uhc_flex_practitioner_dataset CASCADE",
        ),
    )
    for expected_sqlstate, statement in guarded_statements:
        await assert_sqlstate(connection, expected_sqlstate, statement)


@pytest.mark.asyncio
async def test_flex_practitioner_publication_lifecycle_replay_and_removal(
    monkeypatch,
) -> None:
    async with _publication_test_scope(monkeypatch) as test_scope:
        url, schema, database, engine, publication_migration = test_scope
        monkeypatch.setattr(
            publication,
            "register_uhc_flex_practitioner_source",
            AsyncMock(return_value=SimpleNamespace(endpoint_id=ENDPOINT_ID)),
        )
        await _assert_admission_gate(database, schema)
        first, empty = await _publish_successive_datasets(database)
        connection = await connect(url)
        try:
            await _assert_exact_removal(connection, schema, first, empty)
            await _assert_mutation_guards(connection, schema)
            await _assert_official_dataset_rotation_revokes_readiness(
                connection,
                schema,
                empty.readiness.dataset_id,
            )
        finally:
            await connection.close()
        with pytest.raises(DBAPIError, match="downgrade_blocked"):
            await run_migration(engine, publication_migration, "downgrade")
