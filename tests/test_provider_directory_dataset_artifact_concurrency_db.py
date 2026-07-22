# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused PostgreSQL concurrency proof for Provider Directory cutover locks."""

import asyncio
import importlib

import pytest

from db.connection import Database
from tests.test_provider_directory_dataset_artifact_db import _dataset_database


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_real_postgres_cutover_fence_allows_disjoint_source_seed(
    monkeypatch,
):
    """Avoid table-wide blocking while exact source and endpoint rows stay fenced."""

    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        seed_database = Database()
        await seed_database.connect()

        try:
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(fence)
                await asyncio.wait_for(
                    seed_database.status(
                        f"UPDATE {schema}.provider_directory_source "
                        "SET plan_name = 'Disjoint seed update' "
                        "WHERE source_id = 'source_catalog_only';"
                    ),
                    timeout=2,
                )
        finally:
            await seed_database.disconnect()

        updated_plan_name = await database.scalar(
            f"SELECT plan_name FROM {schema}.provider_directory_source "
            "WHERE source_id = 'source_catalog_only';"
        )
        assert updated_plan_name == "Disjoint seed update"
