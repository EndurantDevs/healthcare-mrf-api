"""Artifact-only Provider Directory publication follow-up tests."""

import importlib
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_full_artifact_publication_emits_dataset_followup(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_dataset_artifacts",
        AsyncMock(
            return_value={
                "publish_artifacts": True,
                "publish_artifacts_only": True,
                "source_ids": ["source-current"],
                "artifact_dataset_ids": ["dataset-current"],
            }
        ),
    )
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

    metrics = await importer.process_data(
        {"context": {}},
        {
            "run_id": "run-publish",
            "publish_artifacts_only": True,
            "source_ids": ["source-current"],
            "probe": False,
            "import_resources": False,
            "publish_artifacts": False,
            "publish_corroboration": False,
        },
    )

    assert metrics["dataset_followup"] == followup
    resolve_followup.assert_awaited_once_with(
        source_ids=["source-current"],
        expected_acquisition_root_run_id=None,
    )


@pytest.mark.asyncio
async def test_overlay_only_followup_does_not_emit_another_descriptor(
    monkeypatch,
):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_dataset_artifacts",
        AsyncMock(return_value={"address_overlay": {"published": True}}),
    )
    resolve_followup = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_source_local_dataset_followup_if_current",
        resolve_followup,
    )

    metrics = await importer.process_data(
        {"context": {}},
        {
            "run_id": "run-overlay",
            "publish_artifacts_only": True,
            "publish_artifacts_targets": "address_overlay",
            "source_ids": ["source-current"],
            "probe": False,
            "import_resources": False,
            "publish_artifacts": False,
            "publish_corroboration": False,
        },
    )

    assert "dataset_followup" not in metrics
    resolve_followup.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_artifact_publication_requires_current_descriptor(monkeypatch):
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_publish_provider_directory_dataset_artifacts",
        AsyncMock(return_value={"publish_artifacts_only": True}),
    )
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
                "publish_artifacts_only": True,
                "source_ids": ["source-current"],
                "probe": False,
                "import_resources": False,
                "publish_artifacts": False,
                "publish_corroboration": False,
            },
        )
