# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for sealed generic candidate preference."""

from __future__ import annotations

import pytest

from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
    importer,
)
from tests.test_provider_directory_dataset_selection_receipt_db import (
    _install_receipt_candidate,
)


async def _resolve_candidate():
    """Resolve the synthetic endpoint through automatic candidate selection."""

    return await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"],
        should_select_validated_candidates=True,
    )


@pytest.mark.asyncio
async def test_normalized_sealed_candidate_supersedes_legacy_candidate(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(
            database,
            schema,
            dataset_id="dataset_legacy_candidate",
            root_run_id="root-legacy-candidate",
            seal=False,
        )
        await _install_receipt_candidate(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "publication_metadata_json = '{}'::jsonb "
            "WHERE dataset_id = 'dataset_candidate';"
        )

        fence = await _resolve_candidate()
        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(fence, database)

        assert fence.datasets[0].dataset_id == "dataset_candidate"


@pytest.mark.asyncio
async def test_sealed_candidate_without_receipt_does_not_suppress_legacy(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(
            database,
            schema,
            dataset_id="dataset_legacy_candidate",
            root_run_id="root-legacy-candidate",
            seal=False,
        )
        await _insert_validated_shared_dataset(database, schema)

        with pytest.raises(
            RuntimeError,
            match="provider_directory_artifact_validated_candidate_ambiguous",
        ):
            await _resolve_candidate()


@pytest.mark.asyncio
async def test_two_normalized_sealed_candidates_remain_ambiguous(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_receipt_candidate(database, schema)
        await _install_receipt_candidate(
            database,
            schema,
            dataset_id="dataset_candidate_peer",
            root_run_id="root-candidate-peer",
        )

        with pytest.raises(
            RuntimeError,
            match="provider_directory_artifact_validated_candidate_ambiguous",
        ):
            await _resolve_candidate()


@pytest.mark.asyncio
async def test_legacy_only_candidate_remains_selectable(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema, seal=False)

        fence = await _resolve_candidate()

        assert fence.datasets[0].dataset_id == "dataset_candidate"
