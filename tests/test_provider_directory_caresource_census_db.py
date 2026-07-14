# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
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
