# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from tests.provider_directory_subset_completion_pg_setup import (
    install_subset_canonical_functions,
)
from tests.provider_directory_dataset_artifact_pg_support import (
    insert_validated_shared_dataset as _insert_validated_shared_dataset,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except (OSError, OperationalError):
        pytest.skip("Provider Directory artifact DB tests require a reachable disposable database")
    if "test" not in database_name.lower():
        pytest.skip("Provider Directory artifact DB tests require a disposable test database")


def _dataset_metadata(resources: list[str]) -> str:
    return json.dumps({"selected_resources": resources})


def _source_metadata(resources: list[str]) -> str:
    return json.dumps(
        {
            "provider_directory_supported_resources": resources,
            "provider_directory_fully_enumerable_resources": resources,
        }
    )


async def _create_artifact_tables(database: Database, schema: str) -> None:
    """Create one disposable schema with the exact resolver columns."""
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_api_endpoint "
        "(endpoint_id varchar(64) PRIMARY KEY);"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, "
        "endpoint_id varchar(64) NOT NULL, "
        "import_run_id varchar(64), "
        "acquisition_root_run_id varchar(64), "
        "previous_dataset_id varchar(96), "
        "dataset_hash varchar(64), "
        "status varchar(32) NOT NULL, "
        "is_current boolean NOT NULL, "
        "resource_count bigint NOT NULL DEFAULT 0, "
        "superseded_at timestamp, "
        "created_at timestamp, "
        "validated_at timestamp, "
        "published_at timestamp, "
        "publication_metadata_json jsonb, "
        "artifact_selection_receipt_json jsonb, "
        "publication_metadata_summary_json jsonb, "
        "publication_metadata_sha256 varchar(64), "
        "content_proof_admission_version smallint, "
        "content_proof_admission_kind varchar(32), "
        "content_proof_admission_sha256 varchar(64), "
        "content_proof_resource_types varchar(64)[], "
        "completion_proof_required_version integer, "
        "completion_proof_json jsonb, "
        "completion_proof_sha256 varchar(64)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, "
        "resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, "
        "payload_json json NOT NULL, "
        "acquired_resource_sha256 varchar(64), "
        "PRIMARY KEY (dataset_id, resource_type, resource_id)"
        ");"
    )
    await install_subset_canonical_functions(database, schema)
    await _install_admission_seal_fixture_contract(database, schema)
    await _create_artifact_scope_tables(database, schema)


async def _install_admission_seal_fixture_contract(
    database: Database,
    schema: str,
) -> None:
    """Install the current bounded receipt digest in full-schema fixtures."""

    await database.status(
        f"""
        CREATE FUNCTION {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
            metadata_summary jsonb,
            admission_version smallint,
            admission_kind text,
            proof_sha256 text,
            resource_types varchar[]
        ) RETURNS varchar
        LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $function$
            SELECT {schema}.provider_directory_subset_payload_sha256(
                jsonb_build_object(
                    'contract', 'provider-directory-admission-seal-v1',
                    'metadata_summary', metadata_summary,
                    'admission_version', admission_version,
                    'admission_kind', admission_kind,
                    'proof_sha256', proof_sha256,
                    'resource_types', to_jsonb(resource_types)
                )
            )::varchar
        $function$;
        """
    )


async def _create_artifact_scope_tables(database: Database, schema: str) -> None:
    """Install artifact scope tables after the current dataset schema."""

    for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS):
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model, schema, model.__tablename__,
            )
        )
        for statement in importer._artifact_scope_pk_sql(
            model,
            schema,
            model.__tablename__,
        ):
            await database.status(statement)
    await database.status(
        f"ALTER TABLE {schema}.provider_directory_source "
        "ADD CONSTRAINT provider_directory_source_endpoint_id_fkey "
        "FOREIGN KEY (endpoint_id) REFERENCES "
        f"{schema}.provider_directory_api_endpoint(endpoint_id) "
        "ON DELETE SET NULL;"
    )


async def _insert_fixture_sources(database: Database, schema: str) -> None:
    """Insert endpoint aliases used by the artifact race harness."""
    endpoints = ("endpoint_shared", "endpoint_unpublished", "endpoint_repoint")
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id) "
        "SELECT unnest(CAST(:endpoint_ids AS varchar[]));",
        endpoint_ids=list(endpoints),
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source ("
        "source_id, org_name, endpoint_id, requires_registration, "
        "requires_api_key, metadata_json"
        ") VALUES "
        "('source_primary', 'Primary', 'endpoint_shared', false, false, "
        "CAST(:source_metadata AS json)), "
        "('source_sibling', 'Sibling', 'endpoint_shared', false, false, "
        "CAST(:source_metadata AS json)), "
        "('source_catalog_only', 'Catalog-only', NULL, false, false, "
        "CAST(:source_metadata AS json)), "
        "('source_no_current', 'No-current', 'endpoint_unpublished', false, false, "
        "CAST(:source_metadata AS json));",
        source_metadata=_source_metadata(["Location"]),
    )


async def _insert_fixture_datasets(database: Database, schema: str) -> None:
    """Insert one current and one unpublished endpoint dataset."""
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "status, is_current, published_at, publication_metadata_json"
        ") VALUES "
        "('dataset_shared', 'endpoint_shared', 'run-shared', 'root-shared', "
        ":published_status, true, now(), CAST(:location_metadata AS json)), "
        "('dataset_unpublished', 'endpoint_unpublished', 'run-unpublished', "
        "'root-unpublished', 'incomplete', true, now(), CAST(:empty_metadata AS json));",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        location_metadata=_dataset_metadata(["Location"]),
        empty_metadata=_dataset_metadata([]),
    )


async def _insert_fixture_resources(database: Database, schema: str) -> None:
    """Insert one validated row and one unvalidated linked row."""
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES ("
        "'dataset_shared', 'Location', 'location-1', 'a' || repeat('b', 63), "
        "CAST(:payload_json AS json)"
        ");",
        payload_json=json.dumps(
            {
                "status": "active",
                "name": "Scope Clinic",
                "type_codes": ["clinic"],
                "first_line": "1 Scope Way",
                "city_name": "Austin",
                "state_code": "TX",
                "postal_code": "78701",
                "latitude": "30.2672",
                "longitude": "-97.7431",
                "telecom": [{"system": "phone", "value": "555-0100"}],
            }
        ),
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES ("
        "'dataset_shared', 'Practitioner', 'linked-practitioner-1', "
        "'c' || repeat('d', 63), CAST(:payload_json AS json)"
        ");",
        payload_json=json.dumps(
            {
                "npi": 1234567890,
                "active": True,
                "full_name": "Unvalidated Linked Practitioner",
            }
        ),
    )


async def _insert_fixture_rows(database: Database, schema: str) -> None:
    """Populate the disposable schema with one immutable dataset family."""
    await _insert_fixture_sources(database, schema)
    await _insert_fixture_datasets(database, schema)
    await _insert_fixture_resources(database, schema)


@asynccontextmanager
async def _dataset_database(monkeypatch):
    schema = f"provider_directory_dataset_artifact_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    try:
        await database.connect()
        await _require_disposable_postgres(database)
    except Exception as exc:
        await database.disconnect()
        pytest.skip(f"Postgres is not available for artifact DB tests: {exc}")
    is_schema_created = False
    try:
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await _create_artifact_tables(database, schema)
        await _insert_fixture_rows(database, schema)
        monkeypatch.setattr(importer, "db", database)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _scope_table_names(database: Database, schema: str) -> list[str]:
    rows = await database.all(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname = :schema_name "
        "AND tablename LIKE 'provider_directory%_artifact_scope_%' "
        "ORDER BY tablename;",
        schema_name=schema,
    )
    return [row._mapping["tablename"] for row in rows]


async def _insert_next_shared_dataset(database: Database, schema: str) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "status, is_current, published_at, publication_metadata_json"
        ") VALUES ('dataset_next', 'endpoint_shared', 'run-next', 'root-next', "
        ":published_status, false, now(), CAST(:metadata AS json));",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        metadata=_dataset_metadata(["Location"]),
    )


@pytest.mark.asyncio
async def test_real_postgres_dataset_scope_keeps_explicit_sources_and_cleans(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        explicit_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        all_source_fence = await importer._resolve_provider_directory_artifact_datasets(None)

        assert explicit_fence.source_ids == ["source_primary"]
        assert all_source_fence.source_ids == ["source_primary", "source_sibling"]
        artifact_scope_metrics_by_name = {}
        async with importer._provider_directory_artifact_dataset_scope(
            run_id="artifact-run",
            source_ids=["source_primary"],
            fence=explicit_fence,
            metrics=artifact_scope_metrics_by_name,
        ):
            scope_table = importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()[
                "provider_directory_location"
            ]
            scope_location_row_list = await database.all(
                f"SELECT source_id, resource_id, status, name, type_codes, "
                f"latitude, longitude, telecom, last_seen_run_id, observed_at "
                f"FROM {schema}.{scope_table} ORDER BY source_id;"
            )
            materialized_location_row_list = [
                dict(scope_location_row._mapping)
                for scope_location_row in scope_location_row_list
            ]
            assert [location_row_by_field["source_id"] for location_row_by_field in materialized_location_row_list] == [
                "source_primary",
            ]
            assert {location_row_by_field["resource_id"] for location_row_by_field in materialized_location_row_list} == {"location-1"}
            assert {location_row_by_field["status"] for location_row_by_field in materialized_location_row_list} == {"active"}
            assert {location_row_by_field["name"] for location_row_by_field in materialized_location_row_list} == {"Scope Clinic"}
            assert {tuple(location_row_by_field["type_codes"]) for location_row_by_field in materialized_location_row_list} == {("clinic",)}
            assert {location_row_by_field["latitude"] for location_row_by_field in materialized_location_row_list} == {"30.2672"}
            assert {location_row_by_field["longitude"] for location_row_by_field in materialized_location_row_list} == {"-97.7431"}
            assert {location_row_by_field["last_seen_run_id"] for location_row_by_field in materialized_location_row_list} == {"root-shared"}
            assert all(location_row_by_field["observed_at"] is not None for location_row_by_field in materialized_location_row_list)
            assert all(location_row_by_field["telecom"][0]["value"] == "555-0100" for location_row_by_field in materialized_location_row_list)
            practitioner_scope_table = (
                importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()[
                    "provider_directory_practitioner"
                ]
            )
            assert await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.{practitioner_scope_table};"
            ) == 0

        assert artifact_scope_metrics_by_name["artifact_scope_dataset_count"] == 1
        assert artifact_scope_metrics_by_name["artifact_scope_alias_count"] == 1
        assert artifact_scope_metrics_by_name["artifact_scope_dataset_rows"] == 1
        assert artifact_scope_metrics_by_name["artifact_scope_projected_rows"] == 1
        assert await _scope_table_names(database, schema) == []


@pytest.mark.asyncio
async def test_dataset_selection_projects_exact_evidence_run_fallback(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        resolve = importer._resolve_provider_directory_artifact_datasets
        root_fence = await resolve(["source_primary"])
        assert root_fence.datasets[0].evidence_run_id == "root-shared"

        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET acquisition_root_run_id = NULL "
            "WHERE dataset_id = 'dataset_shared';"
        )
        fallback_fence = await resolve(["source_primary"])
        assert fallback_fence.datasets[0].evidence_run_id == "run-shared"


@pytest.mark.asyncio
async def test_real_postgres_scope_materializes_a_proven_linked_family(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        selected_fence = (
            await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"]
            )
        )
        selected_dataset = selected_fence.datasets[0]
        retained_dataset = replace(
            selected_dataset,
            retained_resources=("Location", "Practitioner"),
        )
        retained_fence = replace(
            selected_fence,
            datasets=(retained_dataset,),
        )

        async with importer._provider_directory_artifact_dataset_scope(
            run_id="artifact-linked-run",
            source_ids=["source_primary"],
            fence=retained_fence,
            metrics={},
        ):
            practitioner_scope_table = (
                importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()[
                    "provider_directory_practitioner"
                ]
            )
            practitioner_rows = await database.all(
                f"SELECT source_id, resource_id, full_name "
                f"FROM {schema}.{practitioner_scope_table};"
            )
            assert [
                dict(practitioner_row._mapping)
                for practitioner_row in practitioner_rows
            ] == [
                {
                    "source_id": "source_primary",
                    "resource_id": "linked-practitioner-1",
                    "full_name": "Unvalidated Linked Practitioner",
                }
            ]

        assert await _scope_table_names(database, schema) == []


@pytest.mark.asyncio
async def test_real_postgres_dataset_fence_rejects_alias_repoint_and_current_change(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(["source_primary"])
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_repoint' WHERE source_id = 'source_primary';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_source_endpoint_dataset_changed",
        ):
            await importer._lock_and_verify_artifact_dataset_fence(fence)

        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' WHERE source_id = 'source_primary';"
        )
        await _insert_next_shared_dataset(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET is_current = false WHERE dataset_id = 'dataset_shared';"
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET is_current = true WHERE dataset_id = 'dataset_next';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_endpoint_dataset_current_changed",
        ):
            await importer._lock_and_verify_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_real_postgres_dataset_fence_reads_live_alias_during_artifact_scope(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )

        async with importer._provider_directory_artifact_dataset_scope(
            run_id="artifact-run",
            source_ids=["source_primary"],
            fence=fence,
        ):
            await database.status(
                f"UPDATE {schema}.provider_directory_source "
                "SET endpoint_id = 'endpoint_repoint' "
                "WHERE source_id = 'source_primary';"
            )
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="provider_directory_source_endpoint_dataset_changed",
            ):
                await importer._lock_and_verify_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_real_postgres_explicit_dataset_fence_ignores_unselected_alias_join(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' "
            "WHERE source_id = 'source_catalog_only';"
        )

        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(fence)
