# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib

import pytest

from db.connection import Database
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_next_shared_dataset,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_real_postgres_cutover_fence_blocks_dataset_promotion(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
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
