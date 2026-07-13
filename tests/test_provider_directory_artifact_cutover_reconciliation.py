# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib

import pytest

from tests.test_provider_directory_artifact_cutover import (
    _assert_artifact_relation_values,
    _create_artifact_bundle_relations,
    _keep_stage_indexes,
    _prepared_bundle_stage,
    importer,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
)


async def _reconciliation_bundle(database, schema):
    await _insert_validated_shared_dataset(database, schema)
    dataset_fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )
    await _create_artifact_bundle_relations(database, schema)
    artifact_stages = (
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_a",
            "artifact_target_a",
            _keep_stage_indexes,
        ),
        await _prepared_bundle_stage(
            schema,
            "artifact_stage_b",
            "artifact_target_b",
            _keep_stage_indexes,
        ),
    )
    return dataset_fence, artifact_stages


@contextlib.contextmanager
def _active_dataset_fence(dataset_fence):
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        dataset_fence
    )
    try:
        yield
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)


async def _assert_promoted_dataset_pointers(database, schema):
    pointer_rows = await database.all(
        f"SELECT dataset_id, status, is_current "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id IN ('dataset_shared', 'dataset_candidate') "
        "ORDER BY dataset_id;"
    )
    assert [tuple(pointer_row) for pointer_row in pointer_rows] == [
        ("dataset_candidate", importer.ENDPOINT_DATASET_PUBLISHED, True),
        ("dataset_shared", importer.ENDPOINT_DATASET_SUPERSEDED, False),
    ]


@pytest.mark.asyncio
async def test_real_postgres_artifact_bundle_accepts_timeout_after_commit(monkeypatch):
    """A lost commit acknowledgement verifies both OIDs and dataset pointers."""

    async with _dataset_database(monkeypatch) as (database, schema):
        dataset_fence, artifact_stages = await _reconciliation_bundle(
            database,
            schema,
        )
        promote_transaction = (
            importer._promote_provider_directory_artifact_bundle_transaction
        )

        async def commit_then_lose_acknowledgement(prepared_artifact_stages):
            await promote_transaction(prepared_artifact_stages)
            raise TimeoutError

        monkeypatch.setattr(
            importer,
            "_promote_provider_directory_artifact_bundle_transaction",
            commit_then_lose_acknowledgement,
        )

        with _active_dataset_fence(dataset_fence):
            await importer._promote_provider_directory_artifact_bundle(
                artifact_stages
            )

        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "new-a",
                "artifact_target_b": "new-b",
            },
        )
        for stage_table in ("artifact_stage_a", "artifact_stage_b"):
            assert await database.scalar(
                "SELECT to_regclass(:relation_name);",
                relation_name=f"{schema}.{stage_table}",
            ) is None
        await _assert_promoted_dataset_pointers(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_timeout_reconciliation_rejects_oid_only_commit(
    monkeypatch,
):
    """Matching relation OIDs are insufficient when the pointer stayed old."""
    async with _dataset_database(monkeypatch) as (database, schema):
        dataset_fence, artifact_stages = await _reconciliation_bundle(
            database,
            schema,
        )
        promote_transaction = (
            importer._promote_provider_directory_artifact_bundle_transaction
        )

        async def commit_relations_only_then_timeout(prepared_artifact_stages):
            transaction_fence_token = (
                importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(None)
            )
            try:
                await promote_transaction(prepared_artifact_stages)
            finally:
                importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(
                    transaction_fence_token
                )
            raise TimeoutError

        monkeypatch.setattr(
            importer,
            "_promote_provider_directory_artifact_bundle_transaction",
            commit_relations_only_then_timeout,
        )
        with _active_dataset_fence(dataset_fence):
            with pytest.raises(TimeoutError):
                await importer._promote_provider_directory_artifact_bundle(
                    artifact_stages
                )

        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "new-a",
                "artifact_target_b": "new-b",
            },
        )
        current_dataset_id = await database.scalar(
            f"SELECT dataset_id "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE endpoint_id = 'endpoint_shared' "
            "AND status = :published_status AND is_current = true;",
            published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        )
        assert current_dataset_id == "dataset_shared"
