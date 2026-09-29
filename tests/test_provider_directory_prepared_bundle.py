# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep prepared serving stages uncommitted until their publication owner commits."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_prepared_bundle_never_publishes_before_owner_decides(monkeypatch, abort):
    events = []
    fence = SimpleNamespace(datasets=(SimpleNamespace(source_id="source_a"),))
    bundle = importer.ProviderDirectoryArtifactBundle()
    bundle.promote = AsyncMock()
    metrics_by_name = {"profile": {"prepared": True}}

    @asynccontextmanager
    async def dataset_scope(**options):
        assert options["fence"] is fence
        events.append("dataset-enter")
        try:
            yield
        finally:
            events.append("dataset-exit")

    @asynccontextmanager
    async def bundle_scope():
        events.append("bundle-enter")
        try:
            yield bundle
        finally:
            events.append("bundle-exit")

    monkeypatch.setattr(importer, "_provider_directory_artifact_dataset_scope", dataset_scope)
    monkeypatch.setattr(importer, "_provider_directory_artifact_bundle_scope", bundle_scope)
    monkeypatch.setattr(importer, "_attach_artifact_fence_metrics", lambda *args: None)
    complete = AsyncMock(return_value=metrics_by_name)
    monkeypatch.setattr(importer, "_publish_provider_directory_artifacts", complete)
    monkeypatch.setattr(importer, "_assert_candidate_artifact_bundle_complete", lambda *args, **kwargs: None)
    request = importer.ProviderDirectoryArtifactPublishRequest(
        run_id="run-a",
        metrics={},
        source_ids=["source_a"],
        publish_corroboration=False,
        publish_artifacts_targets={"profile"},
    )
    try:
        async with importer._prepare_artifact_bundle_from_fence(
            fence, request, artifact_resource_types=frozenset(), resource_fence=fence
        ) as (prepared, prepared_metrics_by_name):
            assert prepared is bundle
            assert prepared_metrics_by_name is metrics_by_name
            assert events == ["dataset-enter", "bundle-enter"]
            assert not bundle.promoted
            bundle.promote.assert_not_awaited()
            if abort:
                raise RuntimeError("owner aborted")
    except RuntimeError as error:
        assert abort and str(error) == "owner aborted"
    assert events == ["dataset-enter", "bundle-enter", "bundle-exit", "dataset-exit"]
    bundle.promote.assert_not_awaited()
    assert not bundle.promoted
    complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_prepared_bundle_requires_bound_transaction():
    with pytest.raises(RuntimeError, match="artifact_bundle_requires_transaction"):
        await importer._apply_prepared_artifact_bundle_in_transaction(())


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_prepared_bundle_obeys_outer_commit_and_rollback(monkeypatch, abort):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"], should_select_validated_candidates=True
        )
        await _create_artifact_bundle_relations(database, schema)
        bundle = importer.ProviderDirectoryArtifactBundle(
            stages=[
                await _prepared_bundle_stage(schema, "artifact_stage_a", "artifact_target_a", _keep_stage_indexes),
                await _prepared_bundle_stage(schema, "artifact_stage_b", "artifact_target_b", _keep_stage_indexes),
            ]
        )
        token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(fence)
        try:
            await _commit_or_abort_bundle(database, bundle, abort)
        finally:
            importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(token)
        await _assert_artifact_relation_values(
            database,
            schema,
            {
                "artifact_target_a": "old-a" if abort else "new-a",
                "artifact_target_b": "old-b" if abort else "new-b",
            },
        )
        current = await database.scalar(
            f"SELECT dataset_id FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE endpoint_id='endpoint_shared' AND is_current=true"
        )
        assert current == ("dataset_shared" if abort else "dataset_candidate")
        assert bundle.promoted is (not abort)


async def _commit_or_abort_bundle(database, bundle, abort):
    """Reject early consumption and preserve the owner's outer rollback."""
    try:
        async with database.transaction():
            await importer._apply_prepared_artifact_bundle_in_transaction(tuple(bundle.stages))
            assert not bundle.promoted
            with pytest.raises(RuntimeError, match="artifact_bundle_commit_pending"):
                await bundle.mark_promoted()
            if abort:
                raise RuntimeError("owner aborted after artifacts")
    except RuntimeError as error:
        assert abort and str(error) == "owner aborted after artifacts"
    if not abort:
        await bundle.mark_promoted()
