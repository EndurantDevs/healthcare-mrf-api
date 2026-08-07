# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _source_metadata,
    importer,
)


async def _orphan_source_alias(database, schema: str) -> None:
    source_metadata = json.loads(_source_metadata(["Location"]))
    source_metadata[
        importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY
    ] = "endpoint_shared"
    await database.status(
        f"UPDATE {schema}.provider_directory_source "
        "SET endpoint_id = 'endpoint_repoint', "
        "metadata_json = CAST(:metadata AS json) "
        "WHERE source_id = 'source_primary';",
        metadata=json.dumps(source_metadata),
    )


async def _bind_published_dataset_to_source(database, schema: str) -> None:
    publication_metadata_by_field = {
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        "source_ids": ["source_primary", "source_sibling"],
        "requires_twin_root_verification": False,
    }
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET publication_metadata_json = CAST(:metadata AS json) "
        "WHERE dataset_id = 'dataset_shared';",
        metadata=json.dumps(publication_metadata_by_field),
    )


async def _install_current_serving_dataset(database, schema: str) -> None:
    publication_metadata_by_field = {
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        "source_ids": ["source_primary"],
    }
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "status, is_current, published_at, publication_metadata_json"
        ") VALUES ("
        "'dataset_serving', 'endpoint_repoint', 'run-serving', "
        "'root-serving', :published_status, true, now(), "
        "CAST(:metadata AS json)"
        ");",
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        metadata=json.dumps(publication_metadata_by_field),
    )


def _assert_reconciliation_fence(fence) -> None:
    selected = fence.datasets[0]
    assert (
        selected.dataset_id,
        selected.endpoint_id,
        selected.serving_endpoint_id,
        selected.promote_on_cutover,
        selected.reconcile_source_alias_on_cutover,
    ) == (
        "dataset_shared",
        "endpoint_shared",
        "endpoint_repoint",
        False,
        True,
    )
    assert fence.promotion_datasets == []
    assert fence.published_source_endpoint_tuples == (
        ("source_primary", "endpoint_shared"),
    )


async def _reconcile_source_alias(database, fence) -> None:
    async with database.transaction():
        await importer._lock_and_verify_artifact_dataset_fence(
            fence,
            database,
        )
        await importer._cutover_provider_directory_artifact_sources(fence)


async def _assert_reconciled_state(database, schema: str) -> None:
    assert await database.scalar(
        f"SELECT endpoint_id FROM {schema}.provider_directory_source "
        "WHERE source_id = 'source_primary';"
    ) == "endpoint_shared"
    assert await database.scalar(
        f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared';"
    ) == importer.ENDPOINT_DATASET_PUBLISHED
    steady_fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"]
    )
    assert steady_fence.datasets[0].reconcile_source_alias_on_cutover is False


@pytest.mark.asyncio
async def test_real_postgres_reconciles_only_proven_published_alias(monkeypatch):
    """Repair an orphaned serving alias inside the existing cutover fence."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _orphan_source_alias(database, schema)
        with pytest.raises(
            RuntimeError,
            match="provider_directory_artifact_current_dataset_missing",
        ):
            await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"]
            )
        await _bind_published_dataset_to_source(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        _assert_reconciliation_fence(fence)
        await _reconcile_source_alias(database, fence)
        await _assert_reconciled_state(database, schema)


@pytest.mark.asyncio
async def test_real_postgres_keeps_alias_with_current_serving_dataset(monkeypatch):
    """Never replace an alias that still has a published serving dataset."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _orphan_source_alias(database, schema)
        await _bind_published_dataset_to_source(database, schema)
        await _install_current_serving_dataset(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        selected = fence.datasets[0]
        assert (
            selected.dataset_id,
            selected.endpoint_id,
            selected.reconcile_source_alias_on_cutover,
        ) == ("dataset_serving", "endpoint_repoint", False)
