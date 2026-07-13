# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("dataset serving relation tests need disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("dataset serving relation tests need a test database")


async def _create_tables(database: Database, schema: str) -> None:
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, "
        "acquisition_root_run_id varchar(64)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, "
        "resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, "
        "payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_type, resource_id)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_network_plan ("
        "dataset_id varchar(96) NOT NULL, "
        "network_resource_id varchar(256) NOT NULL, "
        "insurance_plan_resource_id varchar(256) NOT NULL, "
        "acquisition_root_run_id varchar(64), "
        "build_run_id varchar(64), "
        "built_at timestamp NOT NULL, "
        "PRIMARY KEY (dataset_id, network_resource_id, insurance_plan_resource_id)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_affiliation_organization ("
        "dataset_id varchar(96) NOT NULL, "
        "participating_organization_resource_id varchar(256) NOT NULL, "
        "affiliation_resource_id varchar(256) NOT NULL, "
        "acquisition_root_run_id varchar(64), "
        "build_run_id varchar(64), "
        "built_at timestamp NOT NULL, "
        "PRIMARY KEY (dataset_id, participating_organization_resource_id, "
        "affiliation_resource_id)"
        ");"
    )


async def _insert_resource(
    database: Database,
    schema: str,
    dataset_id: str,
    resource_type: str,
    resource_id: str,
    payload: dict,
) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_json) "
        "VALUES (:dataset_id, :resource_type, :resource_id, "
        "CAST(:payload_json AS jsonb));",
        dataset_id=dataset_id,
        resource_type=resource_type,
        resource_id=resource_id,
        payload_json=json.dumps(payload),
    )


async def _insert_endpoint_dataset_fixtures(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, acquisition_root_run_id) VALUES "
        "('dataset-a', 'root-a'), ('dataset-b', 'root-b'), "
        "('dataset-zero', 'root-zero');"
    )


async def _insert_plan_fixtures(database: Database, schema: str) -> None:
    plan_payload_by_id = {
        "plan-a": {
            "network_refs": [
                "Organization/net-shared",
                "https://payer.test/fhir/Organization/net-two",
                "net-shared",
                "Organization/net-two",
            ]
        },
        "plan-b": {"network_refs": ["Organization/net-shared"]},
        "plan-c": {"network_refs": []},
    }
    for resource_id, plan_payload in plan_payload_by_id.items():
        await _insert_resource(
            database,
            schema,
            "dataset-a",
            "InsurancePlan",
            resource_id,
            plan_payload,
        )


async def _insert_affiliation_fixtures(
    database: Database,
    schema: str,
) -> None:
    affiliation_reference_by_id = {
        "affiliation-a": "Organization/org-a",
        "affiliation-b": (
            "https://payer.test/fhir/Organization/org-a"
        ),
        "affiliation-c": "org-b",
        "affiliation-d": "",
        "affiliation-e": None,
    }
    for resource_id, affiliation_reference in (
        affiliation_reference_by_id.items()
    ):
        await _insert_resource(
            database,
            schema,
            "dataset-a",
            "OrganizationAffiliation",
            resource_id,
            {"participating_organization_ref": affiliation_reference},
        )


async def _insert_zero_edge_fixtures(database: Database, schema: str) -> None:
    for plan_number in range(4):
        await _insert_resource(
            database,
            schema,
            "dataset-zero",
            "InsurancePlan",
            f"zero-plan-{plan_number}",
            {"network_refs": []},
        )


async def _insert_sentinel_relation_fixtures(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_network_plan "
        "VALUES ('dataset-b', 'sentinel-network', 'sentinel-plan', "
        "'root-b', 'old-build', now());"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_affiliation_organization "
        "VALUES ('dataset-b', 'sentinel-org', 'sentinel-affiliation', "
        "'root-b', 'old-build', now());"
    )


async def _insert_fixtures(database: Database, schema: str) -> None:
    await _insert_endpoint_dataset_fixtures(database, schema)
    await _insert_plan_fixtures(database, schema)
    await _insert_affiliation_fixtures(database, schema)
    await _insert_zero_edge_fixtures(database, schema)
    await _insert_sentinel_relation_fixtures(database, schema)


@asynccontextmanager
async def _dataset_database(monkeypatch):
    schema = f"provider_directory_serving_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await _create_tables(database, schema)
        await _insert_fixtures(database, schema)
        yield database, schema
    except Exception:
        if not is_schema_created:
            pytest.skip("disposable Postgres is unavailable")
        raise
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _build_dataset_a_relations(database: Database):
    async with database.acquire() as connection:
        network_proof = (
            await importer._build_provider_directory_dataset_network_plan(
                connection,
                "dataset-a",
                build_run_id="retry-child",
                expected_acquisition_root_run_id="root-a",
            )
        )
        affiliation_proof = await (
            importer
            ._build_provider_directory_dataset_affiliation_organization(
                connection,
                "dataset-a",
                build_run_id="retry-child",
                expected_acquisition_root_run_id="root-a",
            )
        )
    return network_proof, affiliation_proof


async def _dataset_a_relation_rows(database: Database, schema: str):
    network_edge_rows = await database.all(
        f"SELECT network_resource_id, insurance_plan_resource_id, "
        f"acquisition_root_run_id, build_run_id FROM {schema}."
        "provider_directory_dataset_network_plan "
        "WHERE dataset_id = 'dataset-a' ORDER BY 1, 2;"
    )
    affiliation_edge_rows = await database.all(
        f"SELECT participating_organization_resource_id, "
        f"affiliation_resource_id FROM {schema}."
        "provider_directory_dataset_affiliation_organization "
        "WHERE dataset_id = 'dataset-a' ORDER BY 1, 2;"
    )
    return network_edge_rows, affiliation_edge_rows


def _assert_network_edges(network_proof, network_edge_rows) -> None:
    assert network_proof["edge_count"] == 3
    assert network_proof["duplicate_network_reference_count"] == 2
    assert network_proof["zero_network_reference_plan_count"] == 1
    assert [tuple(edge_row)[:2] for edge_row in network_edge_rows] == [
        ("net-shared", "plan-a"),
        ("net-shared", "plan-b"),
        ("net-two", "plan-a"),
    ]
    assert all(
        tuple(edge_row)[2:] == ("root-a", "retry-child")
        for edge_row in network_edge_rows
    )


def _assert_affiliation_edges(affiliation_proof, affiliation_edge_rows) -> None:
    assert affiliation_proof["edge_count"] == 3
    assert affiliation_proof[
        "empty_participating_organization_reference_count"
    ] == 2
    assert [tuple(edge_row) for edge_row in affiliation_edge_rows] == [
        ("org-a", "affiliation-a"),
        ("org-a", "affiliation-b"),
        ("org-b", "affiliation-c"),
    ]


async def _assert_dataset_b_sentinels(database: Database, schema: str) -> None:
    assert await database.scalar(
        f"SELECT count(*) FROM {schema}."
        "provider_directory_dataset_network_plan "
        "WHERE dataset_id = 'dataset-b';"
    ) == 1
    assert await database.scalar(
        f"SELECT count(*) FROM {schema}."
        "provider_directory_dataset_affiliation_organization "
        "WHERE dataset_id = 'dataset-b';"
    ) == 1


@pytest.mark.asyncio
async def test_real_postgres_builds_normalized_relations_and_preserves_other_dataset(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        network_proof, affiliation_proof = await _build_dataset_a_relations(
            database
        )
        network_edge_rows, affiliation_edge_rows = (
            await _dataset_a_relation_rows(database, schema)
        )

        _assert_network_edges(network_proof, network_edge_rows)
        _assert_affiliation_edges(affiliation_proof, affiliation_edge_rows)
        await _assert_dataset_b_sentinels(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_accepts_zero_edges_and_rolls_back_invalid_rebuild(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        async with database.acquire() as connection:
            zero_proof = (
                await importer._build_provider_directory_dataset_network_plan(
                    connection,
                    "dataset-zero",
                    build_run_id="zero-build",
                    expected_acquisition_root_run_id="root-zero",
                )
            )
            await importer._build_provider_directory_dataset_network_plan(
                connection,
                "dataset-a",
                build_run_id="good-build",
                expected_acquisition_root_run_id="root-a",
            )
        await database.status(
            f"UPDATE {schema}.provider_directory_dataset_resource "
            "SET payload_json = "
            "'{\"network_refs\":[\"PractitionerRole/not-a-network\"]}'::jsonb "
            "WHERE dataset_id = 'dataset-a' "
            "AND resource_type = 'InsurancePlan' "
            "AND resource_id = 'plan-a';"
        )

        with pytest.raises(RuntimeError, match="invalid_references"):
            async with database.acquire() as connection:
                await importer._build_provider_directory_dataset_network_plan(
                    connection,
                    "dataset-a",
                    build_run_id="bad-build",
                    expected_acquisition_root_run_id="root-a",
                )

        assert zero_proof["complete"] is True
        assert zero_proof["insurance_plan_resource_count"] == 4
        assert zero_proof["edge_count"] == 0
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}."
            "provider_directory_dataset_network_plan "
            "WHERE dataset_id = 'dataset-a';"
        ) == 3
