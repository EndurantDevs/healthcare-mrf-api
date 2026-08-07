"""Source-local publication follow-up contract tests."""

import importlib
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")
entity_address_unified = importlib.import_module("process.entity_address_unified")


def _published_artifact_dataset(**overrides):
    dataset_by_name = {
        "source_id": "source-current",
        "endpoint_id": "endpoint-current",
        "dataset_id": "dataset-current",
        "evidence_run_id": "run-root",
        "dataset_hash": "a" * 64,
        "serving_endpoint_id": "endpoint-stale",
        "reconcile_source_alias_on_cutover": True,
    }
    dataset_by_name.update(overrides)
    return importer.ProviderDirectoryArtifactDataset(**dataset_by_name)


def test_dataset_fence_requires_exact_source_run_scope():
    entity_address_unified._validate_provider_directory_dataset_fence_scope(
        dataset_id="dataset-current",
        source_ids=["source-current"],
        run_id="run-overlay",
        partial_scope="latest-run",
    )

    invalid_scopes = (
        ([], "run-overlay", "latest-run"),
        (["source-a", "source-b"], "run-overlay", "latest-run"),
        (["source-current"], None, "latest-run"),
        (["source-current"], "run-overlay", "all"),
    )
    for source_ids, run_id, partial_scope in invalid_scopes:
        with pytest.raises(ValueError, match="requires one explicit source"):
            entity_address_unified._validate_provider_directory_dataset_fence_scope(
                dataset_id="dataset-current",
                source_ids=source_ids,
                run_id=run_id,
                partial_scope=partial_scope,
            )


@pytest.mark.asyncio
async def test_dataset_fence_matches_current_publication(monkeypatch):
    current_dataset = AsyncMock(return_value={"dataset_id": "dataset-current"})
    monkeypatch.setattr(entity_address_unified.db, "first", current_dataset)

    await entity_address_unified._assert_current_provider_directory_dataset(
        "mrf",
        source_id="source-current",
        expected_dataset_id="dataset-current",
        expected_root_run_id="run-root",
    )

    query = current_dataset.await_args.args[0]
    assert "dataset.endpoint_id = source.endpoint_id" not in query
    assert "dataset.dataset_id = :expected_dataset_id" in query
    assert "dataset.status = 'published'" in query
    assert "dataset.is_current IS TRUE" in query
    assert "dataset.acquisition_root_run_id = :expected_root_run_id" in query
    assert "dataset.published_at IS NOT NULL" in query
    assert "dataset.superseded_at IS NULL" in query
    assert "dataset.publication_metadata_json::jsonb -> 'source_ids'" in query
    assert "jsonb_build_array(source.source_id)" in query
    assert "competing.dataset_id <> dataset.dataset_id" in query
    assert "competing.status = 'published'" in query
    assert "competing.is_current IS TRUE" in query
    assert "competing.published_at IS NOT NULL" in query
    assert "competing.superseded_at IS NULL" in query
    assert current_dataset.await_args.kwargs == {
        "source_id": "source-current",
        "expected_dataset_id": "dataset-current",
        "expected_root_run_id": "run-root",
    }

    current_dataset.return_value = None
    with pytest.raises(RuntimeError, match="dataset fence changed"):
        await entity_address_unified._assert_current_provider_directory_dataset(
            "mrf",
            source_id="source-current",
            expected_dataset_id="dataset-current",
            expected_root_run_id="run-root",
        )


def test_dataset_followup_descriptor_binds_exact_publication():
    descriptor = importer._provider_directory_dataset_followup(
        source_id="source-current",
        endpoint_id="endpoint-current",
        dataset_id="dataset-current",
        dataset_hash="a" * 64,
        parent_run_id="run-root",
    )

    assert descriptor == {
        "version": 1,
        "status": "required",
        "kind": "provider_directory_dataset_publication",
        "intent": "ensure_address_overlay_then_unified_address",
        "importer": "provider-directory-fhir",
        "source_id": "source-current",
        "endpoint_id": "endpoint-current",
        "dataset_id": "dataset-current",
        "dataset_hash": "a" * 64,
        "parent_run_id": "run-root",
        "idempotency_key": (
            "provider-directory-dataset-followup:dataset-current"
        ),
        "triggered_by": "pd_dataset_followup",
    }


def test_followup_trigger_requires_atomic_publication_metrics():
    source_rows = [{"source_id": "source-current"}]
    assert not importer._has_dataset_publication_followup_trigger(
        {},
        ["source-current"],
        source_rows,
    )
    assert importer._has_dataset_publication_followup_trigger(
        {
            "artifact_dataset_ids": ["dataset-current"],
            "artifact_dataset_evidence_run_ids": ["run-root"],
        },
        ["source-current"],
        source_rows,
    )
    assert not importer._has_dataset_publication_followup_trigger(
        {
            "resource_fetch_stats": {
                "Practitioner": {
                    "official_provider_file_sources": True,
                    "collection_complete_sources": True,
                }
            }
        },
        ["source-current"],
        source_rows,
    )
    assert importer._has_dataset_publication_followup_trigger(
        {
            "resource_fetch_stats": {
                "Practitioner": {
                    "official_provider_file_sources": 1,
                    "collection_complete_sources": 1,
                }
            }
        },
        ["source-current"],
        source_rows,
    )
    assert not importer._has_dataset_publication_followup_trigger(
        {
            "artifact_dataset_ids": ["dataset-a", "dataset-b"],
            "artifact_dataset_evidence_run_ids": ["run-root"],
        },
        ["source-current"],
        source_rows,
    )


@pytest.mark.asyncio
async def test_dataset_followup_requires_one_exact_current_dataset(monkeypatch):
    current_dataset = AsyncMock(
        return_value=importer.ProviderDirectoryArtifactDatasetFence(
            (_published_artifact_dataset(),)
        )
    )
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        current_dataset,
    )

    assert await importer._source_local_dataset_followup_if_current(
        source_ids=[], expected_acquisition_root_run_id="run-root"
    ) is None
    assert await importer._source_local_dataset_followup_if_current(
        source_ids=["source-current", "source-other"],
        expected_acquisition_root_run_id="run-root",
    ) is None
    current_dataset.return_value = importer.ProviderDirectoryArtifactDatasetFence(())
    assert await importer._source_local_dataset_followup_if_current(
        source_ids=["source-current"],
        expected_acquisition_root_run_id="run-root",
    ) is None
    current_dataset.return_value = importer.ProviderDirectoryArtifactDatasetFence(
        (_published_artifact_dataset(endpoint_id=""),)
    )
    assert await importer._source_local_dataset_followup_if_current(
        source_ids=["source-current"],
        expected_acquisition_root_run_id="run-root",
    ) is None
    current_dataset.return_value = importer.ProviderDirectoryArtifactDatasetFence(
        (_published_artifact_dataset(),)
    )
    assert await importer._source_local_dataset_followup_if_current(
        source_ids=["source-current"],
        expected_acquisition_root_run_id="run-other",
    ) is None

    replay = await importer._source_local_dataset_followup_if_current(
        source_ids=["source-current"],
        expected_acquisition_root_run_id=None,
    )
    assert replay is not None
    assert replay["dataset_id"] == "dataset-current"
    assert replay["parent_run_id"] == "run-root"
    current_dataset.assert_awaited_with(
        ["source-current"],
        should_select_validated_candidates=False,
    )


@pytest.mark.asyncio
async def test_dataset_followup_replay_performs_no_acquisition(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    followup = importer._provider_directory_dataset_followup(
        source_id="source-current",
        endpoint_id="endpoint-current",
        dataset_id="dataset-current",
        dataset_hash="a" * 64,
        parent_run_id="run-root",
    )
    resolve_followup = AsyncMock(return_value=followup)
    monkeypatch.setattr(
        importer,
        "_source_local_dataset_followup_if_current",
        resolve_followup,
    )

    replay_metrics = await importer.process_data(
        {"context": {}},
        {
            "dataset_followup_only": True,
            "source_ids": ["source-current"],
            "probe": False,
            "import_resources": False,
            "publish_artifacts": False,
            "publish_after_acquisition": False,
        },
    )

    assert replay_metrics == {
        "dataset_followup_only": True,
        "source_ids": ["source-current"],
        "dataset_followup": followup,
    }
    resolve_followup.assert_awaited_once_with(
        source_ids=["source-current"],
        expected_acquisition_root_run_id=None,
    )


@pytest.mark.asyncio
async def test_dataset_followup_replay_requires_current_dataset(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_source_local_dataset_followup_if_current",
        AsyncMock(return_value=None),
    )

    with pytest.raises(
        RuntimeError,
        match="provider_directory_dataset_followup_current_dataset_missing",
    ):
        await importer.process_data(
            {"context": {}},
            {
                "dataset_followup_only": True,
                "source_ids": ["synthetic-source"],
                "probe": False,
                "import_resources": False,
                "publish_artifacts": False,
                "publish_after_acquisition": False,
            },
        )


@pytest.mark.asyncio
async def test_dataset_followup_replay_rejects_mutating_modes(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())

    with pytest.raises(
        ValueError,
        match="provider_directory_dataset_followup_replay_scope_invalid",
    ):
        await importer.process_data(
            {"context": {}},
            {
                "dataset_followup_only": True,
                "source_ids": ["source-current"],
                "import_resources": True,
                "publish_artifacts": False,
            },
        )

    for incompatible_mode in (
        "canonical_backfill_only",
        "contact_backfill_only",
    ):
        with pytest.raises(
            ValueError,
            match="provider_directory_dataset_followup_replay_scope_invalid",
        ):
            await importer.process_data(
                {"context": {}},
                {
                    "dataset_followup_only": True,
                    incompatible_mode: True,
                    "source_ids": ["source-current"],
                },
            )

    with pytest.raises(
        ValueError,
        match="provider_directory_dataset_followup_replay_scope_invalid",
    ):
        await importer.process_data(
            {"context": {}},
            {
                "dataset_followup_only": True,
                "dataset_rehydrate_only": True,
                "source_ids": ["source-current"],
            },
        )
