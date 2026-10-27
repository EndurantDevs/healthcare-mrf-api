"""Artifact-only Provider Directory publication follow-up tests."""

import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_artifacts as source_artifacts

importer = importlib.import_module("process.provider_directory_fhir")


def _overlay_followup_params():
    """Match the existing source-scoped followup, including exact publication markers."""
    return {
        "publish_artifacts_only": True, "publish_artifacts_targets": "address_overlay",
        "source_ids": ["source-current"], "publish_corroboration": False,
        "probe": False, "import_resources": False,
        "provider_directory_dataset_followup_version": 1,
        "provider_directory_dataset_followup_stage": "address_overlay",
        "provider_directory_dataset_followup_source_id": "source-current",
        "provider_directory_dataset_followup_endpoint_id": "endpoint-current",
        "provider_directory_dataset_followup_dataset_id": "dataset-current",
        "provider_directory_dataset_followup_dataset_hash": "a" * 64,
        "provider_directory_dataset_followup_parent_run_id": "run-root",
        "provider_directory_dataset_followup_attempt": 1,
    }


@pytest.fixture
def overlay_entry(monkeypatch):
    """Keep real task parsing, preflight and fences; stop at the staging boundary."""
    dataset = importer.ProviderDirectoryArtifactDataset(
        "source-current", "endpoint-current", "dataset-current", "run-root", dataset_hash="a" * 64,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    resolve_fence = AsyncMock(return_value=fence)
    relation_builder = AsyncMock()
    bundle_publisher = AsyncMock(return_value={"address_overlay": {"published": True}})
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(importer, "_raise_if_resource_import_cancelled", AsyncMock())
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(importer.db, "status", AsyncMock(side_effect=AssertionError("unexpected write")))
    monkeypatch.setattr(importer, "_resolve_provider_directory_artifact_datasets", resolve_fence)
    monkeypatch.setattr(importer, "_publish_current_dataset_relation_artifacts", relation_builder)
    monkeypatch.setattr(importer, "_publish_artifact_bundle_from_fence", bundle_publisher)
    return SimpleNamespace(
        context={"control_run_id": "run-overlay", "context": {}}, dataset=dataset, fence=fence,
        resolve_fence=resolve_fence, relation_builder=relation_builder, bundle_publisher=bundle_publisher,
    )


@pytest.mark.asyncio
async def test_overlay_followup_uses_publication_fence_without_acquisition_preflight(overlay_entry):
    """The ordinary entry accepts lifecycle fields without granting acquisition readiness."""
    task_by_name = {**_overlay_followup_params(), "test_mode": False, "run_id": "run-overlay"}
    assert not source_artifacts.source.is_source_storage_required()
    metrics = await importer.process_data(overlay_entry.context, task_by_name)
    assert metrics["address_overlay"]["published"] is True
    assert "provider_directory_tables_ready" not in overlay_entry.context["context"]
    importer.db.all.assert_not_awaited()
    importer.db.status.assert_not_awaited()
    overlay_entry.bundle_publisher.assert_awaited_once()
    passed_fence, request = overlay_entry.bundle_publisher.await_args.args
    assert passed_fence is overlay_entry.fence
    assert request.metrics["provider_directory_dataset_followup_dataset_id"] == "dataset-current"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["source_id", "endpoint_id", "dataset_id", "dataset_hash", "parent_run_id"])
async def test_overlay_followup_rejects_stale_pin(overlay_entry, field):
    """All publication identities must match before any relation or artifact work."""
    task_by_name = _overlay_followup_params()
    task_by_name["provider_directory_dataset_followup_" + field] = "b" * 64 if field == "dataset_hash" else "changed"
    if field == "source_id":
        task_by_name["source_ids"] = ["changed"]
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="overlay_followup_selection_changed"):
        await importer.process_data(overlay_entry.context, task_by_name)
    overlay_entry.relation_builder.assert_not_awaited()
    overlay_entry.bundle_publisher.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_fields", [
    {"status": "validated"}, {"is_current": False}, {"promote_on_cutover": True}, {"serving_endpoint_id": "changed"},
])
async def test_overlay_followup_requires_published_current(overlay_entry, changed_fields):
    """An overlay can never promote a candidate or reuse a superseded selection."""
    changed_dataset = replace(overlay_entry.dataset, **changed_fields)
    overlay_entry.resolve_fence.return_value = importer.ProviderDirectoryArtifactDatasetFence((changed_dataset,))
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="overlay_followup_selection_changed"):
        await importer.process_data(overlay_entry.context, _overlay_followup_params())
    overlay_entry.relation_builder.assert_not_awaited()
    overlay_entry.bundle_publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_followup_missing_layout_refuses(overlay_entry):
    """Ordinary overlays never repair missing canonical layout at runtime."""
    overlay_entry.resolve_fence.side_effect = RuntimeError("missing column")
    with pytest.raises(RuntimeError, match="missing column"):
        await importer.process_data(overlay_entry.context, _overlay_followup_params())
    overlay_entry.resolve_fence.assert_awaited_once()
    overlay_entry.relation_builder.assert_not_awaited()
    overlay_entry.bundle_publisher.assert_not_awaited()
    importer.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [None, "validated_publication", " address_overlay"])
async def test_overlay_marker_fallback_requires_source(overlay_entry, stage):
    """Unrecognized stages cannot acquire the ordinary read-only preflight exemption."""
    task_by_name = _overlay_followup_params()
    task_by_name["provider_directory_dataset_followup_stage"] = stage
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await importer.process_data(overlay_entry.context, task_by_name)
    importer.db.all.assert_not_awaited()
    overlay_entry.resolve_fence.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_selection_refresh_remains_fenced(overlay_entry):
    """A valid initial marker does not permit a changed selection after relation checks."""
    replacement = replace(overlay_entry.dataset, dataset_id="replacement")
    overlay_entry.resolve_fence.side_effect = [
        overlay_entry.fence, importer.ProviderDirectoryArtifactDatasetFence((replacement,)),
    ]
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="artifact_dataset_selection_changed"):
        await importer.process_data(overlay_entry.context, _overlay_followup_params())
    overlay_entry.relation_builder.assert_awaited_once()
    overlay_entry.bundle_publisher.assert_not_awaited()


@pytest.mark.parametrize("changed_fields", [
    {"import_resources": True}, {"publish_artifacts_only": 1}, {"probe": 0}, {"test_mode": True},
    {"provider_directory_dataset_followup_version": True}, {"provider_directory_dataset_followup_attempt": True},
    {"provider_directory_dataset_followup_dataset_hash": "bad"}, {"provider_directory_dataset_followup_endpoint_id": " "},
    {"publish_targets": "profile"}, {"validated_publication_candidate": {}}, {"full_refresh": False},
    {"cms_npd_retained_release_id": "release"}, {"provider_directory_serving_generation": "generation"},
    {"source_ids": ["other"]}, {"run_id": " "},
])
def test_overlay_followup_refuses_parameter_mixing(changed_fields):
    """Only the exact overlay contract may use ordinary canonical preflight."""
    with pytest.raises(RuntimeError, match="ordinary_overlay_request_invalid"):
        source_artifacts.ordinary_overlay_followup(importer, {**_overlay_followup_params(), **changed_fields})


@pytest.mark.parametrize("source_id", ["cms-npd", importer.UHC_RETAINED_SOURCE_ID, "specialized-source"])
def test_overlay_followup_excludes_specialized_sources(monkeypatch, source_id):
    """Dedicated source lanes retain their existing admission paths."""
    monkeypatch.setattr(importer, "is_uhc_flex_profile_source", lambda identifier: identifier == "specialized-source")
    task_by_name = _overlay_followup_params()
    task_by_name.update(source_ids=[source_id], provider_directory_dataset_followup_source_id=source_id)
    with pytest.raises(RuntimeError, match="ordinary_overlay_request_invalid"):
        source_artifacts.ordinary_overlay_followup(importer, task_by_name)


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
