# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused PostgreSQL concurrency proof for Provider Directory cutover locks."""

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from tests.provider_directory_dataset_artifact_pg_support import (
    insert_validated_shared_dataset,
    seal_validated_dataset,
)
from tests.test_provider_directory_dataset_artifact_db import _dataset_database


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate(
    *,
    endpoint_id: str = "endpoint_candidate_other",
    source_ids: tuple[str, ...] = ("source_primary", "source_sibling"),
) -> importer.EndpointDatasetCandidate:
    return importer.EndpointDatasetCandidate(
        endpoint_id=endpoint_id,
        dataset_id="dataset_candidate_other",
        acquisition_root_run_id="root-candidate-other",
        source_ids=source_ids,
        selected_resources=("Location",),
        expected_resources=("Location",),
        import_run_id="run-candidate",
        previous_dataset_id=None,
        resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
    )


async def _prepare_candidate_finalization(database, schema: str):
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint "
        "(endpoint_id) VALUES ('endpoint_candidate_other');"
    )
    await insert_validated_shared_dataset(
        database,
        schema,
        dataset_id="dataset_candidate_other",
        root_run_id="root-candidate-other",
        seal=False,
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET endpoint_id = 'endpoint_candidate_other', "
        "previous_dataset_id = NULL, status = :acquiring_status, "
        "dataset_hash = NULL, validated_at = NULL "
        "WHERE dataset_id = 'dataset_candidate_other';",
        acquiring_status=importer.ENDPOINT_DATASET_ACQUIRING,
    )
    return await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )


async def _finalize_candidate(
    validation_database,
    candidate,
    schema: str,
    candidate_stored: asyncio.Event,
    allow_validation_commit: asyncio.Event,
) -> None:
    async with validation_database.transaction():
        await importer._lock_endpoint_dataset_for_validation(
            validation_database,
            candidate,
        )
        await importer._store_validated_endpoint_dataset(
            validation_database,
            candidate,
            None,
            "e" * 64,
            1,
            {
                "source_ids": ["source_primary", "source_sibling"],
                "selected_resources": ["Location"],
                "expected_resources": ["Location"],
            },
        )
        await seal_validated_dataset(
            validation_database,
            schema,
            "dataset_candidate_other",
        )
        candidate_stored.set()
        await allow_validation_commit.wait()


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


@pytest.mark.asyncio
async def test_real_postgres_cutover_fence_waits_for_candidate_finalization(
    monkeypatch,
):
    """Wait for a cross-endpoint candidate, then reject the stale cutover."""

    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await _prepare_candidate_finalization(database, schema)
        candidate = _candidate()
        validation_database = Database()
        await validation_database.connect()
        candidate_stored = asyncio.Event()
        allow_validation_commit = asyncio.Event()

        async def verify_cutover() -> None:
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(fence)

        validation_task = asyncio.create_task(
            _finalize_candidate(
                validation_database,
                candidate,
                schema,
                candidate_stored,
                allow_validation_commit,
            )
        )
        try:
            await asyncio.wait_for(candidate_stored.wait(), timeout=2)
            cutover_task = asyncio.create_task(verify_cutover())
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.shield(cutover_task),
                    timeout=0.1,
                )
            allow_validation_commit.set()
            await asyncio.wait_for(validation_task, timeout=2)
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="endpoint_dataset_candidate_changed",
            ):
                await asyncio.wait_for(cutover_task, timeout=2)
        finally:
            allow_validation_commit.set()
            await validation_database.disconnect()

        assert await importer._artifact_eligible_validated_ids(
            fence,
            database,
        ) == {"endpoint_candidate_other": ["dataset_candidate_other"]}


@pytest.mark.asyncio
async def test_validated_dataset_store_requires_complete_source_scope():
    connection = AsyncMock()
    connection.all.return_value = [{"source_id": "source_a"}]

    with pytest.raises(RuntimeError, match="source_changed"):
        await importer._store_validated_endpoint_dataset(
            connection,
            _candidate(source_ids=("source_a", "source_b")),
            "dataset_current",
            "d" * 64,
            2,
            {"verification": "matched"},
        )

    connection.status.assert_not_awaited()
