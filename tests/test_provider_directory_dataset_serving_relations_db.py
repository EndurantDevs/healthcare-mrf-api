# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from contextlib import asynccontextmanager
import asyncio
import hashlib
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
    await database.status(f"CREATE TABLE {schema}.provider_directory_source (source_id varchar(64) PRIMARY KEY, org_name varchar(256) NOT NULL, endpoint_id varchar(64), canonical_api_base text, metadata_json jsonb);")
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, "
        "endpoint_id varchar(64) NOT NULL, "
        "acquisition_root_run_id varchar(64), "
        "import_run_id varchar(64) NOT NULL, "
        "previous_dataset_id varchar(96), "
        "dataset_hash varchar(64), "
        "resource_count bigint, "
        "status varchar(32) NOT NULL DEFAULT 'published', "
        "is_current boolean NOT NULL DEFAULT true, "
        "superseded_at timestamptz, "
        "publication_metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource ("
        "dataset_id varchar(96) NOT NULL, "
        "resource_type varchar(64) NOT NULL, "
        "resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, "
        "payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_type, resource_id)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_insurance_plan ("
        "dataset_id varchar(96) NOT NULL, "
        "resource_id varchar(256) NOT NULL, "
        "payload_hash varchar(64) NOT NULL, "
        "payload_json jsonb NOT NULL, "
        "PRIMARY KEY (dataset_id, resource_id)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_network_plan ("
        "dataset_id varchar(96) NOT NULL, "
        "network_resource_id varchar(256) NOT NULL, "
        "insurance_plan_resource_id varchar(256) NOT NULL, "
        "PRIMARY KEY (dataset_id, network_resource_id, insurance_plan_resource_id)"
        ");"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_affiliation_organization ("
        "dataset_id varchar(96) NOT NULL, "
        "participating_organization_resource_id varchar(256) NOT NULL, "
        "affiliation_resource_id varchar(256) NOT NULL, "
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
    payload_json = json.dumps(payload, sort_keys=True)
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json) "
        "VALUES (:dataset_id, :resource_type, :resource_id, :payload_hash, "
        "CAST(:payload_json AS jsonb));",
        dataset_id=dataset_id,
        resource_type=resource_type,
        resource_id=resource_id,
        payload_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        payload_json=payload_json,
    )

async def _insert_endpoint_dataset_fixtures(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, org_name, endpoint_id, canonical_api_base, metadata_json) VALUES "
        "('source-a', 'Standard', 'endpoint-a', 'https://standard.test', '{}'::jsonb);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, endpoint_id, acquisition_root_run_id, import_run_id) VALUES "
        "('dataset-a', 'endpoint-a', 'root-a', 'import-a'), "
        "('dataset-b', 'endpoint-b', 'root-b', 'import-b'), "
        "('dataset-zero', 'endpoint-zero', 'root-zero', 'import-zero'), "
        "('dataset-legacy', 'endpoint-legacy', NULL, 'legacy-import');"
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
    await _insert_resource(
        database,
        schema,
        "dataset-legacy",
        "InsurancePlan",
        "legacy-plan",
        {"network_refs": ["Organization/legacy-network"]},
    )

async def _insert_affiliation_fixtures(
    database: Database,
    schema: str,
) -> None:
    affiliation_reference_by_id = {
        "affiliation-a": "Organization/org-a",
        "affiliation-b": "https://payer.test/fhir/Organization/org-a",
        "affiliation-c": "org-b",
        "affiliation-d": "",
        "affiliation-e": None,
    }
    for resource_id, affiliation_reference in (
        affiliation_reference_by_id.items()
    ):
        payload = {"participating_organization_ref": affiliation_reference}
        if resource_id in {"affiliation-a", "affiliation-b"}:
            payload["organization_ref"] = "Organization/org-b"
        await _insert_resource(
            database, schema, "dataset-a", "OrganizationAffiliation",
            resource_id, payload,
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
        f"INSERT INTO {schema}.provider_directory_dataset_insurance_plan "
        "VALUES ('dataset-b', 'sentinel-plan', repeat('0', 64), '{}'::jsonb);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_network_plan "
        "VALUES ('dataset-b', 'sentinel-network', 'sentinel-plan');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_affiliation_organization "
        "VALUES ('dataset-b', 'sentinel-org', 'sentinel-affiliation');"
    )


async def _insert_fixtures(database: Database, schema: str) -> None:
    await _insert_endpoint_dataset_fixtures(database, schema)
    await _insert_plan_fixtures(database, schema)
    for resource_id in ("org-a", "org-b"):
        await _insert_resource(
            database, schema, "dataset-a", "Organization", resource_id,
            {"name": resource_id},
        )
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
        f"SELECT network_resource_id, insurance_plan_resource_id FROM {schema}."
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
    assert [tuple(edge_row) for edge_row in network_edge_rows] == [
        ("net-shared", "plan-a"),
        ("net-shared", "plan-b"),
        ("net-two", "plan-a"),
    ]


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
        "provider_directory_dataset_insurance_plan "
        "WHERE dataset_id = 'dataset-b';"
    ) == 1
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



async def _build_baseline_dataset_a_relations(database: Database, schema: str):
    async with database.acquire() as connection:
        await importer._build_provider_directory_dataset_network_plan(
            connection,
            "dataset-a",
            build_run_id="good-build",
            expected_acquisition_root_run_id="root-a",
        )
        await importer._build_provider_directory_dataset_affiliation_organization(
            connection,
            "dataset-a",
            build_run_id="good-build",
            expected_acquisition_root_run_id="root-a",
        )
    return await _dataset_a_relation_rows(database, schema)


async def _assert_dataset_a_relations_unchanged(
    database: Database,
    schema: str,
    expected_rows,
) -> None:
    assert await _dataset_a_relation_rows(database, schema) == expected_rows


@pytest.mark.asyncio
async def test_real_postgres_accepts_zero_network_edges(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, _schema_name):
        async with database.acquire() as connection:
            zero_proof = (
                await importer._build_provider_directory_dataset_network_plan(
                    connection,
                    "dataset-zero",
                    build_run_id="zero-build",
                    expected_acquisition_root_run_id="root-zero",
                )
            )

        assert zero_proof["complete"] is True
        assert zero_proof["insurance_plan_resource_count"] == 4
        assert zero_proof["edge_count"] == 0


@pytest.mark.asyncio
async def test_real_postgres_invalid_network_refs_preserve_rows(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        original_rows = await _build_baseline_dataset_a_relations(database, schema)
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

        await _assert_dataset_a_relations_unchanged(database, schema, original_rows)


@pytest.mark.asyncio
async def test_real_postgres_invalid_affiliation_refs_preserve_rows(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        original_rows = await _build_baseline_dataset_a_relations(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_dataset_resource "
            "SET payload_json = "
            "'{\"participating_organization_ref\": "
            "\"Practitioner/not-an-organization\"}'::jsonb "
            "WHERE dataset_id = 'dataset-a' "
            "AND resource_type = 'OrganizationAffiliation' "
            "AND resource_id = 'affiliation-a';"
        )

        with pytest.raises(RuntimeError, match="invalid_references"):
            async with database.acquire() as connection:
                await importer._build_provider_directory_dataset_affiliation_organization(
                    connection,
                    "dataset-a",
                    build_run_id="bad-build",
                    expected_acquisition_root_run_id="root-a",
                )

        await _assert_dataset_a_relations_unchanged(database, schema, original_rows)


@pytest.mark.asyncio
async def test_real_postgres_legacy_null_root_uses_import_run_id(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        async with database.acquire() as connection:
            proof = await importer._build_provider_directory_dataset_network_plan(
                connection,
                "dataset-legacy",
                build_run_id="legacy-build",
                expected_acquisition_root_run_id="legacy-import",
            )

        relation_rows = await database.all(
            f"SELECT network_resource_id, insurance_plan_resource_id FROM {schema}."
            "provider_directory_dataset_network_plan "
            "WHERE dataset_id = 'dataset-legacy';"
        )
        assert proof["acquisition_root_run_id"] == "legacy-import"
        assert proof["edge_count"] == 1
        assert [tuple(row) for row in relation_rows] == [
            ("legacy-network", "legacy-plan")
        ]


@pytest.mark.asyncio
async def test_real_postgres_serializes_same_dataset_relation_builds():
    database = Database()
    first_acquired = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    second_acquired = asyncio.Event()
    lock_tasks = []

    async def hold_first_lock():
        async with database.acquire() as connection:
            await importer._lock_dataset_serving_relation_build(
                connection,
                "dataset-lock-test",
            )
            first_acquired.set()
            await release_first.wait()

    async def wait_for_same_lock():
        await first_acquired.wait()
        async with database.acquire() as connection:
            second_started.set()
            await importer._lock_dataset_serving_relation_build(
                connection,
                "dataset-lock-test",
            )
            second_acquired.set()

    try:
        await database.connect()
        await _require_disposable_postgres(database)
        first_task = asyncio.create_task(hold_first_lock())
        second_task = asyncio.create_task(wait_for_same_lock())
        lock_tasks = [first_task, second_task]
        await second_started.wait()
        await asyncio.sleep(0.1)
        assert second_acquired.is_set() is False
        release_first.set()
        await asyncio.gather(first_task, second_task)
        assert second_acquired.is_set() is True
    finally:
        release_first.set()
        if lock_tasks:
            await asyncio.gather(*lock_tasks, return_exceptions=True)
        await database.disconnect()
