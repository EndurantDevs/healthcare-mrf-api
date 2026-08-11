# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import hashlib
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process.provider_directory_resource_hash import (
    legacy_resource_payload_sha256,
)


importer = importlib.import_module("process.provider_directory_fhir")


DATASET_ID = "dataset-keyset"
RESOURCE_TYPE = "PractitionerRole"
ROW_COUNT = 20_000
CURSOR_ID = "00015000"
BATCH_SIZE = 7


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("content-proof keyset test needs disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("content-proof keyset test needs a test database")


@asynccontextmanager
async def _content_proof_database(monkeypatch):
    schema = f"provider_directory_keyset_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        await database.status(
            f"""
            CREATE TABLE "{schema}".provider_directory_dataset_resource (
                dataset_id varchar(96) NOT NULL,
                resource_type varchar(64) NOT NULL,
                resource_id varchar(256) NOT NULL,
                payload_hash varchar(64) NOT NULL,
                payload_json jsonb NOT NULL,
                acquired_resource_sha256 varchar(64),
                PRIMARY KEY (dataset_id, resource_type, resource_id)
            );
            """
        )
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


async def _seed_content_rows(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id,
            payload_hash, payload_json
        )
        SELECT :dataset_id, :resource_type,
               lpad(row_number::text, 8, '0'),
               repeat('a', 64), '{{}}'::jsonb
          FROM generate_series(1, :row_count) AS row_number;
        """,
        dataset_id=DATASET_ID,
        resource_type=RESOURCE_TYPE,
        row_count=ROW_COUNT,
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id,
            payload_hash, payload_json
        )
        SELECT 'dataset-decoy', :resource_type,
               lpad(row_number::text, 8, '0'),
               repeat('b', 64), '{{}}'::jsonb
          FROM generate_series(1, :row_count) AS row_number;
        """,
        resource_type=RESOURCE_TYPE,
        row_count=ROW_COUNT,
    )
    await database.status(
        f"""
        CREATE INDEX dataset_resource_order_idx
            ON "{schema}".provider_directory_dataset_resource (
                resource_type, resource_id
            );
        """
    )
    await database.status(
        f'ANALYZE "{schema}".provider_directory_dataset_resource;'
    )


def _plan_nodes(raw_plan):
    plan_root = raw_plan[0]["Plan"]
    pending_nodes = [plan_root]
    while pending_nodes:
        plan_node = pending_nodes.pop()
        yield plan_node
        pending_nodes.extend(plan_node.get("Plans", ()))


async def _content_page_and_plan(database: Database):
    query = importer._endpoint_dataset_hash_page_sql(
        True,
        include_payload_json=True,
    ).strip().removesuffix(";")
    params_by_name = {
        "dataset_id": DATASET_ID,
        "after_resource_type": RESOURCE_TYPE,
        "after_resource_id": CURSOR_ID,
        "batch_size": BATCH_SIZE,
    }
    async with database.acquire() as connection:
        await connection.status(
            "SET plan_cache_mode = force_generic_plan;"
        )
        page_rows = await connection.all(query, **params_by_name)
        raw_plan = await connection.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query,
            **params_by_name,
        )
    return page_rows, list(_plan_nodes(raw_plan))


async def _acquired_page_and_plan(database: Database):
    query = importer._subset_acquired_page_sql(True).strip().removesuffix(";")
    params_by_name = {
        "dataset_id": DATASET_ID,
        "resource_types": [RESOURCE_TYPE],
        "after_resource_type": RESOURCE_TYPE,
        "after_resource_id": CURSOR_ID,
        "batch_size": BATCH_SIZE,
    }
    async with database.acquire() as connection:
        await connection.status(
            "SET plan_cache_mode = force_generic_plan;"
        )
        page_rows = await connection.all(query, **params_by_name)
        raw_plan = await connection.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query,
            **params_by_name,
        )
    return page_rows, list(_plan_nodes(raw_plan))


async def _seed_overlapping_proof_rows(
    database: Database,
    schema: str,
) -> list[tuple[str, str, str]]:
    target_payload_by_field = {}
    target_hash = legacy_resource_payload_sha256(
        target_payload_by_field
    )
    target_rows = [
        ("Location", "location-1", target_hash),
        ("LU:PractitionerRole:pass:1", "excluded-1", target_hash),
        ("Organization", "organization-1", target_hash),
        ("PractitionerRole", "role-1", target_hash),
        ("PractitionerRole", "role-2", target_hash),
        ("PractitionerRole", "role-3", target_hash),
    ]
    for dataset_id, payload_by_field in (
        (DATASET_ID, target_payload_by_field),
        ("dataset-decoy", {"decoy": True}),
    ):
        for resource_type, resource_id, _payload_hash in target_rows:
            await database.status(
                f"""
                INSERT INTO "{schema}".provider_directory_dataset_resource (
                    dataset_id, resource_type, resource_id,
                    payload_hash, payload_json
                ) VALUES (
                    :dataset_id, :resource_type, :resource_id,
                    :payload_hash, CAST(:payload_json AS jsonb)
                );
                """,
                dataset_id=dataset_id,
                resource_type=resource_type,
                resource_id=resource_id,
                payload_hash=legacy_resource_payload_sha256(
                    payload_by_field
                ),
                payload_json=json.dumps(
                    payload_by_field,
                    sort_keys=True,
                ),
            )
    return [
        target_row
        for target_row in target_rows
        if not target_row[0].startswith("LU:")
    ]


def _expected_proof_identity(
    target_rows: list[tuple[str, str, str]],
) -> tuple[str, dict[str, str], dict[str, int]]:
    identity_by_type: dict[str, list[str]] = {}
    ordered_identities = []
    for target_row in sorted(target_rows):
        stable_identity = importer._stable_identity_json(target_row)
        ordered_identities.append(stable_identity)
        identity_by_type.setdefault(target_row[0], []).append(
            stable_identity
        )
    dataset_hash = hashlib.sha256(
        "\n".join(ordered_identities).encode()
    ).hexdigest()
    return (
        dataset_hash,
        {
            resource_type: hashlib.sha256(
                "\n".join(identities).encode()
            ).hexdigest()
            for resource_type, identities in identity_by_type.items()
        },
        {
            resource_type: len(identities)
            for resource_type, identities in identity_by_type.items()
        },
    )


@pytest.mark.asyncio
async def test_postgres_content_proof_pages_one_dataset_without_leakage(
    monkeypatch,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        target_rows = await _seed_overlapping_proof_rows(database, schema)
        monkeypatch.setattr(
            importer,
            "ENDPOINT_DATASET_HASH_BATCH_SIZE",
            2,
        )
        proof = await importer._endpoint_dataset_content_proof(
            database,
            DATASET_ID,
            verify_payload_hashes=True,
            resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
        )

    dataset_hash, hashes_by_type, counts_by_type = (
        _expected_proof_identity(target_rows)
    )
    assert proof.dataset_hash == dataset_hash
    assert proof.resource_count == len(target_rows)
    assert proof.resource_hashes == hashes_by_type
    assert proof.resource_counts == counts_by_type


@pytest.mark.asyncio
async def test_postgres_content_proof_cursor_is_a_bounded_index_range(
    monkeypatch,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        await _seed_content_rows(database, schema)
        page_rows, plan_nodes = await _content_page_and_plan(database)

    assert [page_record.resource_id for page_record in page_rows] == [
        f"{resource_id:08d}"
        for resource_id in range(15_001, 15_001 + BATCH_SIZE)
    ]
    resource_nodes = [
        plan_node
        for plan_node in plan_nodes
        if plan_node.get("Relation Name")
        == "provider_directory_dataset_resource"
    ]
    assert resource_nodes
    assert all(
        plan_node.get("Node Type") != "Seq Scan"
        for plan_node in resource_nodes
    )
    index_conditions = " ".join(
        str(plan_node.get("Index Cond", ""))
        for plan_node in resource_nodes
    )
    rows_inspected = sum(
        int(plan_node.get("Actual Rows", 0))
        + int(plan_node.get("Rows Removed by Filter", 0))
        for plan_node in resource_nodes
    )
    assert rows_inspected < 100
    index_names = {
        str(plan_node.get("Index Name", ""))
        for plan_node in resource_nodes
    }
    assert any(index_name.endswith("_pkey") for index_name in index_names)
    assert all(
        identity_column in index_conditions
        for identity_column in ("dataset_id", "resource_type", "resource_id")
    )
    assert not any(
        "resource_id" in str(plan_node.get("Filter", ""))
        for plan_node in resource_nodes
    )


@pytest.mark.asyncio
async def test_postgres_subset_acquired_cursor_is_a_bounded_index_range(
    monkeypatch,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        await _seed_content_rows(database, schema)
        page_rows, plan_nodes = await _acquired_page_and_plan(database)

    assert [page_record.resource_id for page_record in page_rows] == [
        f"{resource_id:08d}"
        for resource_id in range(15_001, 15_001 + BATCH_SIZE)
    ]
    resource_nodes = [
        plan_node
        for plan_node in plan_nodes
        if plan_node.get("Relation Name")
        == "provider_directory_dataset_resource"
    ]
    assert resource_nodes
    assert all(
        plan_node.get("Node Type") != "Seq Scan"
        for plan_node in resource_nodes
    )
    index_conditions = " ".join(
        str(plan_node.get("Index Cond", ""))
        for plan_node in resource_nodes
    )
    rows_inspected = sum(
        int(plan_node.get("Actual Rows", 0))
        + int(plan_node.get("Rows Removed by Filter", 0))
        for plan_node in resource_nodes
    )
    assert rows_inspected < 100
    assert any(
        str(plan_node.get("Index Name", "")).endswith("_pkey")
        for plan_node in resource_nodes
    )
    assert all(
        identity_column in index_conditions
        for identity_column in ("dataset_id", "resource_type", "resource_id")
    )
