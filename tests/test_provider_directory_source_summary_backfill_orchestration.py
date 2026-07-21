# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_source_summary_backfill import (
    _candidate,
    _content_proof,
    _dataset,
    _publication_metadata_with_summary,
    _relation_proofs,
    _source_summary,
    importer,
)


def _install_rebuild_mocks(
    monkeypatch,
    publication_metadata,
    *,
    content_proof=None,
):
    """Install deterministic collaborators around the transaction boundary."""
    transaction_events = []

    @asynccontextmanager
    async def transaction():
        transaction_events.append("begin")
        try:
            yield
        except BaseException:
            transaction_events.append("rollback")
            raise
        transaction_events.append("commit")

    candidate_loader = AsyncMock(
        return_value=(_candidate(), publication_metadata)
    )
    relation_builder = AsyncMock(return_value=_relation_proofs())
    content_proof_builder = AsyncMock(
        return_value=content_proof or _content_proof()
    )
    proof_recorder = AsyncMock()
    summary_builder = AsyncMock(return_value=_source_summary())
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    collaborator_by_name = {
        "_lock_and_verify_artifact_dataset_fence": AsyncMock(),
        "_lock_dataset_serving_relation_build": AsyncMock(),
        "_current_artifact_dataset_source_summary_candidate": candidate_loader,
        "_rebuild_one_current_dataset_relation_set": relation_builder,
        "_endpoint_dataset_content_proof": content_proof_builder,
        "_record_current_dataset_publication_proof": proof_recorder,
        "_endpoint_dataset_source_summary": summary_builder,
    }
    for collaborator_name, collaborator in collaborator_by_name.items():
        monkeypatch.setattr(importer, collaborator_name, collaborator)
    return (
        transaction_events,
        candidate_loader,
        content_proof_builder,
        proof_recorder,
        summary_builder,
    )


@pytest.mark.asyncio
async def test_relation_rebuild_can_skip_both_independent_artifacts():
    assert (
        await importer._rebuild_one_current_dataset_relation_set(
            _dataset(),
            build_run_id="summary-only",
            should_rebuild_network_plan=False,
            should_rebuild_affiliation_organization=False,
        )
        == {}
    )


@pytest.mark.asyncio
async def test_missing_summary_is_backfilled_atomically_from_retained_rows(
    monkeypatch,
):
    (
        transaction_events,
        candidate_loader,
        content_proof_builder,
        proof_recorder,
        summary_builder,
    ) = _install_rebuild_mocks(
        monkeypatch,
        {"source_ids": ["source-a", "source-b"]},
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((_dataset(),))

    aggregate_by_name = (
        await importer._rebuild_current_dataset_serving_relations(
            fence,
            build_run_id="artifact-1",
            should_rebuild_network_plan=True,
            should_rebuild_affiliation_organization=True,
            should_rebuild_source_summary=True,
        )
    )

    assert transaction_events == ["begin", "commit"]
    candidate_loader.assert_awaited_once()
    content_proof_builder.assert_awaited_once_with(
        importer.db,
        "dataset-1",
        _dataset().selected_resources,
        verify_payload_hashes=True,
    )
    assert [call.args[1] for call in proof_recorder.await_args_list] == [
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY,
        importer.SOURCE_SUMMARY_METADATA_KEY,
    ]
    summary_builder.assert_awaited_once()
    assert aggregate_by_name[importer.SOURCE_SUMMARY_METADATA_KEY][
        "dataset_ids"
    ] == ["dataset-1"]


@pytest.mark.asyncio
async def test_sealed_summary_reuse_skips_resource_rescan(monkeypatch):
    (
        transaction_events,
        _candidate_loader,
        content_proof_builder,
        proof_recorder,
        summary_builder,
    ) = _install_rebuild_mocks(
        monkeypatch,
        _publication_metadata_with_summary(),
    )

    aggregate_by_name = (
        await importer._rebuild_current_dataset_serving_relations(
            importer.ProviderDirectoryArtifactDatasetFence((_dataset(),)),
            build_run_id="artifact-2",
            should_rebuild_network_plan=True,
            should_rebuild_affiliation_organization=True,
            should_rebuild_source_summary=True,
        )
    )

    assert transaction_events == ["begin", "commit"]
    content_proof_builder.assert_not_awaited()
    proof_recorder.assert_not_awaited()
    summary_builder.assert_not_awaited()
    assert aggregate_by_name[importer.SOURCE_SUMMARY_METADATA_KEY][
        "summary_sha256_by_dataset"
    ] == {"dataset-1": _source_summary()["summary_sha256"]}


@pytest.mark.asyncio
async def test_content_mismatch_rolls_back_before_summary_write(monkeypatch):
    (
        transaction_events,
        _candidate_loader,
        _content_proof_builder,
        proof_recorder,
        summary_builder,
    ) = _install_rebuild_mocks(
        monkeypatch,
        {"source_ids": ["source-a", "source-b"]},
        content_proof=_content_proof(dataset_hash="f" * 64),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_content_changed",
    ):
        await importer._rebuild_current_dataset_serving_relations(
            importer.ProviderDirectoryArtifactDatasetFence((_dataset(),)),
            build_run_id="artifact-3",
            should_rebuild_network_plan=True,
            should_rebuild_affiliation_organization=True,
            should_rebuild_source_summary=True,
        )

    assert transaction_events == ["begin", "rollback"]
    proof_recorder.assert_not_awaited()
    summary_builder.assert_not_awaited()


class _ProofOrderHarness:
    def __init__(self):
        self.events = []

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        self.events.append("commit")

    def async_result(self, event_name, result=None):
        async def operation(*_args, **_kwargs):
            self.events.append(event_name)
            return result

        return AsyncMock(side_effect=operation)

    async def record_relation(self, _dataset_value, metadata_key, _proof):
        self.events.append(f"relation-proof:{metadata_key}")

    async def record_publication(self, _dataset_value, metadata_key, _proof):
        self.events.append(f"publication-proof:{metadata_key}")

    def install(self, monkeypatch):
        relation_proof_by_name = _relation_proofs()
        monkeypatch.setattr(importer.db, "transaction", self.transaction)
        monkeypatch.setattr(importer.db, "status", self.async_result("isolation"))
        collaborator_by_name = {
            "_lock_and_verify_artifact_dataset_fence": self.async_result("fence"),
            "_lock_dataset_serving_relation_build": self.async_result("relation-lock"),
            "_current_artifact_dataset_source_summary_candidate": self.async_result(
                "candidate", (_candidate(), {"source_ids": ["source-a", "source-b"]})
            ),
            "_build_provider_directory_dataset_network_plan": self.async_result(
                "network", relation_proof_by_name[
                    importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY
                ]
            ),
            "_build_provider_directory_dataset_affiliation_organization": (
                self.async_result(
                    "affiliation", relation_proof_by_name[
                        importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY
                    ]
                )
            ),
            "_record_current_dataset_serving_relation_proof": self.record_relation,
            "_endpoint_dataset_content_proof": self.async_result(
                "content", _content_proof()
            ),
            "_record_current_dataset_publication_proof": self.record_publication,
            "_endpoint_dataset_source_summary": self.async_result(
                "summary", _source_summary()
            ),
        }
        for collaborator_name, collaborator in collaborator_by_name.items():
            monkeypatch.setattr(importer, collaborator_name, collaborator)


@pytest.mark.asyncio
async def test_relation_proofs_precede_outcome_and_summary_proofs(monkeypatch):
    proof_order_harness = _ProofOrderHarness()
    proof_order_harness.install(monkeypatch)

    await importer._rebuild_one_current_dataset_artifacts(
        _dataset(),
        importer.ProviderDirectoryArtifactDatasetFence((_dataset(),)),
        build_run_id="ordered-build",
        should_rebuild_network_plan=True,
        should_rebuild_affiliation_organization=True,
        should_rebuild_source_summary=True,
    )

    assert proof_order_harness.events == [
        "begin",
        "isolation",
        "fence",
        "relation-lock",
        "candidate",
        "network",
        "affiliation",
        "relation-proof:dataset_network_plan",
        "relation-proof:dataset_affiliation_organization",
        "content",
        "publication-proof:outcome_resource_counts_v1",
        "summary",
        "publication-proof:source_summary_v1",
        "commit",
    ]
