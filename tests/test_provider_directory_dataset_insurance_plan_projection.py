# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import importlib
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


def _network_proof_by_field() -> dict[str, int | str]:
    return {
        "target_dataset_count": 1,
        "acquisition_root_run_id": "root-run",
        "insurance_plan_resource_count": 3,
        "insurance_plan_with_network_refs_count": 2,
        "zero_network_reference_plan_count": 1,
        "malformed_network_refs_payload_count": 0,
        "network_reference_value_count": 5,
        "valid_network_reference_count": 5,
        "invalid_network_reference_count": 0,
        "expected_edge_count": 3,
    }


def _projection_proof_by_field() -> dict[str, int | str]:
    return {
        "target_dataset_count": 1,
        "acquisition_root_run_id": "root-run",
        "insurance_plan_resource_count": 3,
        "plan_projection_count": 3,
        "missing_or_mismatched_plan_projection_count": 0,
        "unexpected_plan_projection_count": 0,
    }


@pytest.mark.asyncio
async def test_network_plan_builder_replaces_plan_projection_before_edges():
    operation_executor = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                _network_proof_by_field(),
                _projection_proof_by_field(),
            ]
        ),
        status=AsyncMock(
            side_effect=["DELETE 2", "INSERT 3", "DELETE 1", "INSERT 3"]
        ),
    )

    proof_by_field = await importer._build_provider_directory_dataset_network_plan(
        operation_executor,
        "dataset-1",
        build_run_id="build-run",
        expected_acquisition_root_run_id="root-run",
    )

    projection_delete_sql = operation_executor.status.await_args_list[0].args[0]
    projection_insert_sql = operation_executor.status.await_args_list[1].args[0]
    edge_delete_sql = operation_executor.status.await_args_list[2].args[0]
    assert "provider_directory_dataset_insurance_plan" in projection_delete_sql
    assert "WHERE dataset_id = :dataset_id" in projection_delete_sql
    assert "provider_directory_dataset_resource" in projection_insert_sql
    assert "resource_type = 'InsurancePlan'" in projection_insert_sql
    assert "provider_directory_dataset_network_plan" in edge_delete_sql
    assert proof_by_field["plan_projection_count"] == 3
    assert proof_by_field["replaced_plan_projection_count"] == 2
    assert proof_by_field["inserted_plan_projection_count"] == 3


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except (OSError, OperationalError):
        pytest.skip("InsurancePlan projection tests need disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("InsurancePlan projection tests need a test database")


async def _create_projection_tables(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, acquisition_root_run_id varchar(64), "
        "import_run_id varchar(64) NOT NULL);"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, payload_hash varchar(64) NOT NULL, "
        "payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_type, resource_id));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_insurance_plan ("
        "dataset_id varchar(96) NOT NULL, resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_id));"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_network_plan ("
        "dataset_id varchar(96) NOT NULL, network_resource_id varchar(256) NOT NULL, "
        "insurance_plan_resource_id varchar(256) NOT NULL, "
        "PRIMARY KEY (dataset_id, network_resource_id, insurance_plan_resource_id));"
    )


async def _seed_projection_data(database: Database, schema: str) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, acquisition_root_run_id, import_run_id) VALUES "
        "('dataset-a', 'root-a', 'import-a'), ('dataset-b', 'root-b', 'import-b');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json) VALUES "
        "('dataset-a', 'InsurancePlan', 'plan-a', repeat('a', 64), "
        "'{\"network_refs\":[\"Organization/network-a\"]}'::jsonb), "
        "('dataset-a', 'InsurancePlan', 'plan-b', repeat('b', 64), "
        "'{\"network_refs\":[]}'::jsonb);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_insurance_plan VALUES "
        "('dataset-b', 'sentinel-plan', repeat('0', 64), '{}'::jsonb);"
    )


@asynccontextmanager
async def _projection_database(monkeypatch):
    schema = f"provider_directory_plan_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await _create_projection_tables(database, schema)
        await _seed_projection_data(database, schema)
        yield database, schema
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable Postgres is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _rebuild_projection(
    database: Database,
    *,
    build_run_id: str,
) -> dict[str, object]:
    async with database.acquire() as connection:
        return await importer._build_provider_directory_dataset_network_plan(
            connection,
            "dataset-a",
            build_run_id=build_run_id,
            expected_acquisition_root_run_id="root-a",
        )


@pytest.mark.asyncio
async def test_real_postgres_rerun_replaces_only_dataset_plan_projection(monkeypatch):
    async with _projection_database(monkeypatch) as (database, schema):
        await _rebuild_projection(database, build_run_id="first-build")
        await database.status(
            f"UPDATE {schema}.provider_directory_dataset_resource "
            "SET payload_hash = repeat('f', 64), "
            "payload_json = payload_json || '{\"name\":\"Updated plan\"}'::jsonb "
            "WHERE dataset_id = 'dataset-a' AND resource_id = 'plan-a';"
        )
        rerun_proof_by_field = await _rebuild_projection(
            database,
            build_run_id="second-build",
        )
        projection_records = await database.all(
            f"SELECT resource_id, payload_hash FROM {schema}."
            "provider_directory_dataset_insurance_plan "
            "WHERE dataset_id = 'dataset-a' ORDER BY resource_id;"
        )
        sentinel_count = await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_dataset_insurance_plan "
            "WHERE dataset_id = 'dataset-b';"
        )

    projection_by_id = {
        projection_record._mapping["resource_id"]: projection_record._mapping
        for projection_record in projection_records
    }
    assert rerun_proof_by_field["replaced_plan_projection_count"] == 2
    assert rerun_proof_by_field["inserted_plan_projection_count"] == 2
    assert rerun_proof_by_field["plan_projection_count"] == 2
    assert projection_by_id["plan-a"]["payload_hash"] == "f" * 64
    assert sentinel_count == 1
