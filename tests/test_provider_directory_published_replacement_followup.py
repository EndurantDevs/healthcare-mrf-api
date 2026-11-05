# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The closed ordinary followup reaches staged replacement before the default fence."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_published_replacement_followup as followup


def test_serving_peer_inputs_cannot_borrow_an_unrelated_preparation_scope():
    from process import entity_address_candidate_preparation as preparation

    pin = preparation.ProviderDirectoryAddressDatasetPin("peer", "endpoint", "dataset", "a" * 64, "root")
    inputs = preparation.ProviderDirectoryAddressPreparationInput(
        (pin,), "overlay_next", 1, 0, ordinary_serving_peer_pins=(pin,)
    )
    with pytest.raises(ValueError, match="replacement_inputs_"):
        preparation.validate_preparation_input(inputs)


@pytest.fixture
def publication_case(monkeypatch):
    directory = SimpleNamespace(
        db=SimpleNamespace(scalar=AsyncMock(return_value=False)),
        UHC_RETAINED_SOURCE_ID="file_source",
        is_uhc_flex_profile_source=lambda _: False,
        _provider_directory_profile_capacity_admission=lambda: None,
        ProviderDirectoryArtifactBuildStale=RuntimeError,
        _unscoped_qt=lambda schema, table: f'"{schema}"."{table}"',
        _schema=lambda: "sample",
    )
    markers_by_field = dict(
        source_id="source_a",
        endpoint_id="endpoint_new",
        dataset_id="dataset_new",
        dataset_hash="a" * 64,
        parent_run_id="root_new",
        version=1,
        stage="address_overlay",
        attempt=1,
    )
    request = SimpleNamespace(
        metrics={followup.artifacts._FOLLOWUP_PREFIX + name: marker for name, marker in markers_by_field.items()},
        source_ids=["source_a"],
        publish_artifacts_targets={"address_overlay"},
        publish_corroboration=False,
        full_address_artifact_rebuild=False,
        run_id=None,
    )
    monkeypatch.setattr(followup.source, "is_source_storage_required", lambda: False)
    monkeypatch.setattr(followup.artifacts, "replay_published_replacement", AsyncMock(return_value=None))
    token = followup.source._FACTORY.set(None)
    try:
        yield SimpleNamespace(directory=directory, request=request)
    finally:
        followup.source._FACTORY.reset(token)


@pytest.mark.asyncio
async def test_ordinary_requests_do_not_enter_replacement(publication_case):
    publication_case.request.metrics.clear()
    assert await followup.publish_if_required(publication_case.directory, publication_case.request) is None
    followup.artifacts.replay_published_replacement.assert_not_awaited()
    publication_case.directory.db.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["stage", "digest", "source", "capacity", "full", "target"])
async def test_changed_request_refuses_before_replay_or_candidate_reads(publication_case, damage):
    match damage:
        case "stage":
            publication_case.request.metrics[followup.artifacts._FOLLOWUP_PREFIX + "stage"] = "profile"
        case "digest":
            publication_case.request.metrics[followup.artifacts._FOLLOWUP_PREFIX + "dataset_hash"] = "b" * 63
        case "source":
            publication_case.request.source_ids = ["other"]
        case "capacity":
            publication_case.directory._provider_directory_profile_capacity_admission = lambda: object()
        case "full":
            publication_case.request.full_address_artifact_rebuild = True
        case "target":
            publication_case.request.publish_artifacts_targets = {"profile"}
    with pytest.raises((ValueError, RuntimeError), match="replacement_"):
        await followup.publish_if_required(publication_case.directory, publication_case.request)
    followup.artifacts.replay_published_replacement.assert_not_awaited()
    publication_case.directory.db.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_committed_journal_replay_precedes_recapturing_incumbent(publication_case):
    proof_by_field = dict(
        serving_recovery=dict(generation="generation", phase="published"), result_generation={"local_generation": 2}
    )
    followup.artifacts.replay_published_replacement.return_value = proof_by_field
    assert (
        await followup.publish_if_required(publication_case.directory, publication_case.request)
        is publication_case.request.metrics
    )
    assert all(publication_case.request.metrics[name] == value for name, value in proof_by_field.items())
    publication_case.directory.db.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_aligned_and_orphan_sources_keep_the_existing_path(publication_case):
    assert await followup.publish_if_required(publication_case.directory, publication_case.request) is None
    assert followup.artifacts.replay_published_replacement.await_count == 2
    publication_case.directory.db.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_replacement_committed_during_incumbent_read_replays(publication_case):
    """The alias becoming aligned cannot hide its newly committed replacement journal."""
    proof_by_field = {"serving_recovery": {"generation": "generation", "phase": "published"}}
    followup.artifacts.replay_published_replacement.side_effect = [None, proof_by_field]
    result = await followup.publish_if_required(publication_case.directory, publication_case.request)
    assert result["serving_recovery"] == proof_by_field["serving_recovery"]
    assert followup.artifacts.replay_published_replacement.await_count == 2


@pytest.mark.asyncio
async def test_production_entry_dispatches_before_the_default_alias_fence(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")

    metrics_by_field = {"published": True}
    publish = AsyncMock(return_value=metrics_by_field)
    default_fence = AsyncMock(side_effect=AssertionError("default resolver reached"))
    monkeypatch.setattr(followup, "publish_if_required", publish)
    monkeypatch.setattr(directory, "_prepare_artifact_publication_fence", default_fence)
    assert (
        await directory._publish_provider_directory_dataset_artifacts(
            run_id="run_new",
            metrics=metrics_by_field,
            source_ids=["source_a"],
            publish_corroboration=False,
            publish_artifacts_targets={"address_overlay"},
        )
        is metrics_by_field
    )
    request = publish.await_args.args[1]
    assert request.metrics is metrics_by_field and request.source_ids == ["source_a"] and request.run_id == "run_new"
    assert request.publish_artifacts_targets == {"address_overlay"}
    default_fence.assert_not_awaited()
