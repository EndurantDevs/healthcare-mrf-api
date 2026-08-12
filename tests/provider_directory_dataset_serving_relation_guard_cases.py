# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL guards for normalized Provider Directory serving relations."""

from __future__ import annotations

import asyncio

import pytest

from db.connection import Database
from tests.test_provider_directory_dataset_serving_relations_db import (
    _dataset_a_relation_rows,
    _dataset_database,
    _require_disposable_postgres,
    importer,
)


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
