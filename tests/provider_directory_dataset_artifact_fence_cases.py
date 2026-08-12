# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL artifact-fence drift contracts."""

from __future__ import annotations

import importlib

import pytest

from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_next_shared_dataset,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_real_postgres_dataset_fence_rejects_alias_repoint_and_current_change(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(["source_primary"])
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_repoint' WHERE source_id = 'source_primary';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_source_endpoint_dataset_changed",
        ):
            await importer._lock_and_verify_artifact_dataset_fence(fence)

        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' WHERE source_id = 'source_primary';"
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
            match="provider_directory_endpoint_dataset_current_changed",
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
                "WHERE source_id = 'source_primary';"
            )
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="provider_directory_source_endpoint_dataset_changed",
            ):
                await importer._lock_and_verify_artifact_dataset_fence(fence)


@pytest.mark.asyncio
async def test_real_postgres_explicit_dataset_fence_ignores_unselected_alias_join(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET endpoint_id = 'endpoint_shared' "
            "WHERE source_id = 'source_catalog_only';"
        )

        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(fence)
