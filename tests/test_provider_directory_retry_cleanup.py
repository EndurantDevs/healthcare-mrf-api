# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib

import pytest

from db.connection import Database
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
)
from tests.test_provider_directory_two_phase_publication import (
    _insert_second_validated_endpoint,
    _prepare_retry_cleanup_state,
    _retry_cleanup_counts,
    importer,
)


async def _mixed_candidate_current_fence(database):
    """Select one validated candidate and one unchanged current peer."""
    await _insert_validated_shared_dataset(database, importer._schema())
    await _insert_second_validated_endpoint(database, importer._schema())
    candidate_selection = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )
    current_selection = await importer._resolve_provider_directory_artifact_datasets(
        ["source_z"],
        should_select_validated_candidates=False,
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        candidate_selection.datasets + current_selection.datasets,
        should_select_validated_candidates=True,
    )


async def _insert_mixed_retry_state(database, schema: str) -> None:
    """Create retry rows for both the candidate and its unchanged peer."""
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_pagination_checkpoint ("
        "dataset_id varchar(96) NOT NULL);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_pagination_checkpoint "
        "VALUES ('dataset_candidate'), ('dataset_z_current');"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json"
        ") VALUES "
        "('dataset_candidate', 'LU:Location:pass:2', 'candidate-pass', "
        ":candidate_hash, CAST('{}' AS json)), "
        "('dataset_z_current', 'LU:Location:pass:1', 'current-pass', "
        ":current_hash, CAST('{}' AS json));",
        candidate_hash="6" * 64,
        current_hash="5" * 64,
    )


async def _remaining_retry_dataset_ids(database, schema: str) -> list[str]:
    rows = await database.all(
        "SELECT dataset_id FROM ("
        f"SELECT dataset_id FROM {schema}.provider_directory_dataset_resource "
        "WHERE resource_type LIKE 'LU:%:pass:%' UNION ALL "
        f"SELECT dataset_id FROM {schema}.provider_directory_pagination_checkpoint"
        ") retry_state ORDER BY dataset_id;"
    )
    return [str(row[0]) for row in rows]


@contextlib.asynccontextmanager
async def _locked_candidate_dataset(schema: str):
    lock_database = Database()
    await lock_database.connect()
    try:
        async with lock_database.transaction():
            await lock_database.all(
                f"SELECT dataset_id FROM {schema}.provider_directory_endpoint_dataset "
                "WHERE dataset_id = 'dataset_candidate' FOR UPDATE;"
            )
            yield
    finally:
        await lock_database.disconnect()


@pytest.mark.asyncio
async def test_cleanup_only_clears_promoted_candidate_in_mixed_fence(monkeypatch):
    """A candidate cutover retains retry state for an unchanged current peer."""
    async with _dataset_database(monkeypatch) as (database, schema):
        publication_fence = await _mixed_candidate_current_fence(database)
        await importer._promote_provider_directory_artifact_datasets(publication_fence)
        await _insert_mixed_retry_state(database, schema)

        metrics = await importer._record_artifact_promotion_metrics(
            publication_fence,
            {},
            promoted_stage_count=1,
        )

        assert metrics["artifact_promoted_dataset_ids"] == ["dataset_candidate"]
        assert metrics["artifact_retry_state_cleared_dataset_ids"] == [
            "dataset_candidate"
        ]
        assert metrics["artifact_retry_state_pending_dataset_ids"] == []
        assert await _remaining_retry_dataset_ids(database, schema) == [
            "dataset_z_current",
            "dataset_z_current",
        ]


@pytest.mark.asyncio
async def test_cleanup_lock_contention_is_bounded_and_recoverable(monkeypatch):
    """A concurrent dataset lock leaves retry state pending without blocking."""
    async with _dataset_database(monkeypatch) as (database, schema):
        await _prepare_retry_cleanup_state(database, schema)
        async with _locked_candidate_dataset(schema):
            started_at = asyncio.get_running_loop().time()
            cleared_ids = await importer._clear_promoted_endpoint_dataset_retry_state(
                ["dataset_candidate"]
            )
            elapsed_seconds = asyncio.get_running_loop().time() - started_at
            assert cleared_ids == []
            assert elapsed_seconds < (
                importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_TRANSACTION_TIMEOUT_SECONDS
            )
            assert await _retry_cleanup_counts(database, schema) == (1, 1)

        assert await importer._clear_promoted_endpoint_dataset_retry_state(
            ["dataset_candidate"]
        ) == ["dataset_candidate"]
        assert await _retry_cleanup_counts(database, schema) == (0, 0)
