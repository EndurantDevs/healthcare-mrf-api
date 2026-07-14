# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import hashlib
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except (OSError, OperationalError):
        pytest.skip("CareSource census tests need disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("CareSource census tests need a test database")


@pytest.mark.asyncio
async def test_real_postgres_counts_exact_unique_candidate_resource_ids(monkeypatch):
    schema = f"caresource_census_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
            "dataset_id varchar(96) NOT NULL, resource_type varchar(64) NOT NULL, "
            "resource_id varchar(256) NOT NULL, payload_hash varchar(64) NOT NULL, "
            "payload_json jsonb NOT NULL, "
            "PRIMARY KEY (dataset_id, resource_type, resource_id));"
        )
        await database.status(
            f"INSERT INTO {schema}.provider_directory_dataset_resource "
            "(dataset_id, resource_type, resource_id, payload_hash, payload_json) VALUES "
            "('dataset-caresource', 'PractitionerRole', 'role-1', repeat('1', 64), '{}'::jsonb), "
            "('dataset-caresource', 'PractitionerRole', 'role-2', repeat('2', 64), '{}'::jsonb), "
            "('dataset-caresource', 'Practitioner', 'practitioner-1', repeat('3', 64), '{}'::jsonb), "
            "('other-dataset', 'PractitionerRole', 'role-3', repeat('4', 64), '{}'::jsonb);"
        )
        await database.status(
            f"INSERT INTO {schema}.provider_directory_dataset_resource "
            "(dataset_id, resource_type, resource_id, payload_hash, payload_json) VALUES "
            "('dataset-caresource', 'PractitionerRole', 'role-2', repeat('5', 64), '{}'::jsonb) "
            "ON CONFLICT (dataset_id, resource_type, resource_id) DO UPDATE "
            "SET payload_hash = EXCLUDED.payload_hash;"
        )
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setattr(importer, "db", database)
        context = importer.PaginationCheckpointContext(
            canonical_api_base=importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
            source_scope_hash="scope",
            source_ids=("caresource",),
            owner_run_id="run",
            retry_of_run_id=None,
            acquisition_root_run_id="run",
            dataset_id="dataset-caresource",
            lineage_verified=False,
        )

        unique_count = await importer._caresource_unique_candidate_count(
            context,
            "PractitionerRole",
        )

        assert unique_count == 2
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable Postgres is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _create_checkpoint_lifecycle_tables(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, "
        "acquisition_root_run_id varchar(128));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, payload_hash varchar(64) NOT NULL, "
        "payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_type, resource_id));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_pagination_checkpoint ("
        "canonical_api_base text NOT NULL, resource_type varchar(64) NOT NULL, "
        "source_scope_hash varchar(64) NOT NULL, dataset_id varchar(96), "
        "source_ids jsonb NOT NULL, acquisition_root_run_id varchar(128) NOT NULL, "
        "owner_run_id varchar(128) NOT NULL, retry_of_run_id varchar(128), "
        "start_url_hash varchar(64) NOT NULL, next_url text, state varchar(16) NOT NULL, "
        "pages_processed bigint NOT NULL, rows_processed bigint NOT NULL, "
        "recent_cursor_hashes jsonb NOT NULL, completeness_json jsonb NOT NULL, "
        "created_at timestamp NOT NULL, updated_at timestamp NOT NULL, "
        "completed_at timestamp, "
        "UNIQUE (canonical_api_base, resource_type, source_scope_hash, "
        "acquisition_root_run_id));"
    )


async def _seed_checkpoint_lifecycle_rows(database: Database, schema: str) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, acquisition_root_run_id) VALUES "
        "('dataset-caresource', 'run-original');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json) VALUES "
        "('dataset-caresource', 'PractitionerRole', 'role-old', repeat('1', 64), '{}'::jsonb), "
        "('dataset-caresource', 'Practitioner', 'practitioner-keep', repeat('2', 64), '{}'::jsonb);"
    )


def _caresource_lifecycle_checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.CARESOURCE_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="scope",
        source_ids=("caresource",),
        owner_run_id="run-retry",
        retry_of_run_id="run-original",
        acquisition_root_run_id="run-original",
        endpoint_id="endpoint-caresource",
        dataset_id="dataset-caresource",
        lineage_verified=True,
    )


async def _assert_reset_checkpoint_state(database: Database, schema: str, start_url: str) -> None:
    resource_type_records = await database.all(
        f"SELECT resource_type FROM {schema}.provider_directory_dataset_resource "
        "ORDER BY resource_type;"
    )
    assert [resource_type_record[0] for resource_type_record in resource_type_records] == [
        "Practitioner"
    ]
    checkpoint_record = await database.first(
        f"SELECT next_url, state, pages_processed, rows_processed, "
        f"completeness_json FROM {schema}.provider_directory_pagination_checkpoint;"
    )
    assert checkpoint_record is not None
    checkpoint_by_field = importer._pagination_checkpoint_row_mapping(
        checkpoint_record
    )
    assert checkpoint_by_field["next_url"] == start_url
    assert checkpoint_by_field["state"] == importer.PAGINATION_CHECKPOINT_ACTIVE
    assert checkpoint_by_field["pages_processed"] == 0
    assert checkpoint_by_field["rows_processed"] == 0
    assert checkpoint_by_field["completeness_json"] == {}


def _caresource_lifecycle_candidate(checkpoint_context):
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-caresource",
        dataset_id="dataset-caresource",
        acquisition_root_run_id="run-original",
        source_ids=("caresource",),
        selected_resources=("PractitionerRole",),
        import_run_id="run-retry",
        previous_dataset_id=None,
        checkpoint_context=checkpoint_context,
    )


@pytest.mark.asyncio
async def test_real_postgres_retains_census_until_dataset_validation(monkeypatch):
    """Retain complete CareSource census proof until candidate validation."""
    schema = f"caresource_checkpoint_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await _create_checkpoint_lifecycle_tables(database, schema)
        await _seed_checkpoint_lifecycle_rows(database, schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setattr(importer, "db", database)
        checkpoint_context = _caresource_lifecycle_checkpoint_context()
        start_url = (
            f"{importer.CARESOURCE_PROVIDER_DIRECTORY_BASE}/"
            "PractitionerRole?_count=1000"
        )

        await importer._reset_pagination_checkpoint(
            checkpoint_context,
            "PractitionerRole",
            start_url,
            hashlib.sha256(start_url.encode("utf-8")).hexdigest(),
        )
        await _assert_reset_checkpoint_state(database, schema, start_url)

        endpoint_candidate = _caresource_lifecycle_candidate(checkpoint_context)
        await importer._clear_finalized_endpoint_dataset_pagination_checkpoints(
            endpoint_candidate,
            {
                "status": importer.ENDPOINT_DATASET_INCOMPLETE,
                "published": False,
            },
        )
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.provider_directory_pagination_checkpoint;"
        ) == 1

        await importer._clear_finalized_endpoint_dataset_pagination_checkpoints(
            endpoint_candidate,
            {
                "status": importer.ENDPOINT_DATASET_VALIDATED,
                "validated": True,
                "published": False,
            },
        )
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.provider_directory_pagination_checkpoint;"
        ) == 0
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable Postgres is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()
