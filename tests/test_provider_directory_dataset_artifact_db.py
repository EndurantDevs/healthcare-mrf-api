# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import getpass
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


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
        "status varchar(32) NOT NULL, "
        "is_current boolean NOT NULL, "
        "superseded_at timestamp, "
        "created_at timestamp, "
        "validated_at timestamp, "
        "published_at timestamp, "
        "publication_metadata_json json"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, "
        "resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, "
        "payload_json json NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_type, resource_id)"
        ");"
    )
    for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS):
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                model.__tablename__,
            )
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
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", "127.0.0.1")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5440")
    monkeypatch.setenv("HLTHPRT_DB_USER", getpass.getuser())
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "healthporta_test")
    schema = f"provider_directory_dataset_artifact_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    await database.connect()
    is_schema_created = False
    try:
        await _require_disposable_postgres(database)
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
async def test_real_postgres_dataset_scope_expands_aliases_materializes_and_cleans(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        explicit_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        all_source_fence = await importer._resolve_provider_directory_artifact_datasets(None)

        assert explicit_fence.source_ids == ["source_primary", "source_sibling"]
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
                "source_sibling",
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
        assert artifact_scope_metrics_by_name["artifact_scope_alias_count"] == 2
        assert artifact_scope_metrics_by_name["artifact_scope_dataset_rows"] == 1
        assert artifact_scope_metrics_by_name["artifact_scope_projected_rows"] == 2
        assert await _scope_table_names(database, schema) == []


@pytest.mark.asyncio
async def test_real_postgres_dataset_fence_rejects_alias_repoint_and_current_change(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(["source_primary"])
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_repoint' WHERE source_id = 'source_sibling';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_source_endpoint_dataset_changed",
        ):
            await importer._lock_and_verify_artifact_dataset_fence(fence)

        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' WHERE source_id = 'source_sibling';"
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
            match="provider_directory_source_endpoint_dataset_changed",
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
                "WHERE source_id = 'source_sibling';"
            )
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="provider_directory_source_endpoint_dataset_changed",
            ):
                await importer._lock_and_verify_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_real_postgres_dataset_fence_rejects_alias_joined_after_selection(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' "
            "WHERE source_id = 'source_catalog_only';"
        )

        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_source_endpoint_dataset_changed",
        ):
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_real_postgres_cutover_fence_blocks_dataset_promotion(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(["source_primary"])
        await _insert_next_shared_dataset(database, schema)
        promotion_database = Database()
        await promotion_database.connect()

        async def promote_current_dataset() -> None:
            async with promotion_database.transaction():
                await promotion_database.status(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset "
                    "SET is_current = false WHERE dataset_id = 'dataset_shared';"
                )
                await promotion_database.status(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset "
                    "SET is_current = true WHERE dataset_id = 'dataset_next';"
                )

        try:
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(fence)
                promotion_task = asyncio.create_task(promote_current_dataset())
                await asyncio.sleep(0.05)
                assert not promotion_task.done()
            await asyncio.wait_for(promotion_task, timeout=2)
        finally:
            await promotion_database.disconnect()

        assert await database.scalar(
            f"SELECT dataset_id FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE endpoint_id = 'endpoint_shared' AND is_current = true;"
        ) == "dataset_next"


@pytest.mark.asyncio
async def test_real_postgres_cutover_fence_blocks_alias_join(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        alias_database = Database()
        await alias_database.connect()

        async def join_endpoint_alias() -> None:
            await alias_database.status(
                f"UPDATE {schema}.provider_directory_source "
                "SET endpoint_id = 'endpoint_shared' "
                "WHERE source_id = 'source_catalog_only';"
            )

        try:
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(fence)
                alias_task = asyncio.create_task(join_endpoint_alias())
                await asyncio.sleep(0.05)
                assert not alias_task.done()
            await asyncio.wait_for(alias_task, timeout=2)
        finally:
            await alias_database.disconnect()

        assert await database.scalar(
            f"SELECT endpoint_id FROM {schema}.provider_directory_source "
            "WHERE source_id = 'source_catalog_only';"
        ) == "endpoint_shared"
