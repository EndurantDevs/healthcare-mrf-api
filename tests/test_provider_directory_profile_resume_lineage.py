# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260720120000_provider_directory_profile_build_checkpoint.py"
)


def _source_context(
    *,
    canonical_api_base: str | None = "https://example.test/fhir",
    org_name: str | None = "Example Health",
    plan_name: str | None = "Example Plan",
) -> importer._ProviderDirectoryProfileSourceContext:
    return importer._ProviderDirectoryProfileSourceContext(
        source_id="source-a",
        canonical_api_base=canonical_api_base,
        org_name=org_name,
        plan_name=plan_name,
    )


def _dataset(
    **overrides: object,
) -> importer.ProviderDirectoryArtifactDataset:
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
        serving_endpoint_id="endpoint-a",
        selected_resources=("Organization", "Practitioner"),
        expected_resources=("Organization", "Practitioner"),
        recorded_expected_resources=("Organization", "Practitioner"),
        dataset_hash="dataset-hash-a",
        resource_count=12,
        validated_at="2026-07-20T10:00:00Z",
        publication_metadata_hash="publication-hash-a",
    )
    return replace(dataset, **overrides)


def _lineage_hash(
    dataset: importer.ProviderDirectoryArtifactDataset,
    source_context: importer._ProviderDirectoryProfileSourceContext,
) -> str:
    return importer._provider_directory_profile_resume_lineage_hash(
        importer.ProviderDirectoryArtifactDatasetFence((dataset,)),
        ["source-a"],
        ["source-a"],
        ["dataset-a"],
        (source_context,),
    )


def _build() -> importer._ProviderDirectoryProfileBuild:
    build_id = f"pdpb_{'a' * 32}"
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-20",
        evidence_stage=profile.profile_evidence_stage_table_name(build_id),
        profile_stage=profile.profile_stage_table_name(build_id),
        resume_lineage_hash="a" * 64,
        build_id=build_id,
        owner_run_id="run-a",
    )


def _checkpoint(
    build: importer._ProviderDirectoryProfileBuild,
) -> dict[str, object]:
    return {
        "build_id": build.build_id,
        "strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": profile.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "profile_as_of": build.profile_as_of,
        "source_ids": list(build.source_ids),
        "retained_source_ids": list(build.retained_source_ids),
        "dataset_ids": list(build.dataset_ids),
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": 31,
        "profile_stage_oid": 32,
        "evidence_target_oid": None,
        "profile_target_oid": None,
        "has_existing_artifacts": False,
        "evidence_next_batch": 52,
        "evidence_total_batches": 52,
        "profile_next_batch": 120,
        "profile_total_batches": 400,
        "state": "building_profile",
    }


def _patch_one_evidence_batch(monkeypatch) -> AsyncMock:
    """Install one conflict-safe evidence batch and mocked finalization."""
    batch = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="source-a",
        dataset_id="dataset-a",
        fact_type="name",
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_evidence_batches",
        lambda *_args, **_kwargs: (batch,),
    )
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    mark_state = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_state",
        mark_state,
    )
    return mark_state


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    [
        ("dataset_hash", "dataset-hash-b"),
        ("resource_count", 13),
        ("publication_metadata_hash", "publication-hash-b"),
        ("status", importer.ENDPOINT_DATASET_VALIDATED),
        ("is_current", False),
    ],
)
def test_resume_lineage_changes_for_same_id_dataset_contract_drift(
    field_name,
    changed_value,
):
    """Fence every publication field, not only source and dataset IDs."""
    dataset = _dataset()
    changed_dataset = replace(dataset, **{field_name: changed_value})

    assert _lineage_hash(dataset, _source_context()) != _lineage_hash(
        changed_dataset,
        _source_context(),
    )


@pytest.mark.parametrize(
    "changed_context",
    [
        _source_context(canonical_api_base="https://changed.test/fhir"),
        _source_context(org_name="Changed Health"),
        _source_context(plan_name="Changed Plan"),
    ],
)
def test_resume_lineage_changes_for_emitted_source_context_drift(
    changed_context,
):
    dataset = _dataset()

    assert _lineage_hash(dataset, _source_context()) != _lineage_hash(
        dataset,
        changed_context,
    )


def test_profile_checkpoint_migration_is_strict_and_chained_after_catalog():
    migration_source = MIGRATION_PATH.read_text(encoding="utf-8")

    assert (
        'down_revision = "20260720110000_uhc_provider_file_catalog"'
        in migration_source
    )
    assert "op.create_table(" in migration_source
    assert "create_table_or_validate" not in migration_source
    assert "create_index_if_missing" not in migration_source


@pytest.mark.asyncio
async def test_same_name_replacement_oid_is_never_reused_or_deleted(
    monkeypatch,
):
    build = _build()
    checkpoint = _checkpoint(build)
    stage_identity = AsyncMock(return_value=(999, "r", "p"))
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    monkeypatch.setattr(importer.db, "status", status)

    reusable = await importer._is_profile_build_checkpoint_reusable(
        build,
        checkpoint,
        has_existing_artifacts=False,
        evidence_build_fence=importer.ProviderDirectoryArtifactBuildFence(
            None
        ),
        profile_build_fence=importer.ProviderDirectoryArtifactBuildFence(None),
        evidence_total_batches=52,
        profile_total_batches=400,
    )
    assert reusable is False

    with pytest.raises(RuntimeError, match="reinitialize_stage_oid_mismatch"):
        await importer._drop_profile_stages_for_reinitialize(
            build,
            checkpoint,
        )
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_reaper_rejects_same_name_replacement_oid(monkeypatch):
    build = _build()
    checkpoint = _checkpoint(build)

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[{"build_id": build.build_id}]),
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=checkpoint),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=(999, "r", "p")),
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    with pytest.raises(RuntimeError, match="stale_stage_oid_mismatch"):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=f"pdpb_{'b' * 32}",
        )
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_batch_insert_commit_replays_idempotently(monkeypatch):
    """A lost insert response replays the same conflict-safe batch once."""
    build = _build()
    mark_state = _patch_one_evidence_batch(monkeypatch)
    advance = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_advance_provider_directory_profile_build_checkpoint",
        advance,
    )
    state = SimpleNamespace(
        committed_evidence_keys=set(),
        insert_attempts=0,
    )

    async def ambiguous_status(sql, **_params):
        if "ON CONFLICT (evidence_key) DO NOTHING" in str(sql):
            state.insert_attempts += 1
            state.committed_evidence_keys.add("source-a:dataset-a:name")
            if state.insert_attempts == 1:
                raise RuntimeError("insert response lost after commit")
        return 0

    monkeypatch.setattr(importer.db, "status", ambiguous_status)

    with pytest.raises(RuntimeError, match="response lost"):
        await importer._populate_provider_directory_profile_evidence_stage(
            build,
            has_evidence_target=False,
            bounded=True,
            checkpointed=True,
        )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=False,
        bounded=True,
        checkpointed=True,
    )

    assert state.committed_evidence_keys == {"source-a:dataset-a:name"}
    assert state.insert_attempts == 2
    advance.assert_awaited_once_with(
        build,
        phase="evidence",
        expected_batch=0,
        total_batches=1,
    )
    mark_state.assert_awaited_once_with(build, "evidence_complete")


@pytest.mark.asyncio
async def test_ambiguous_checkpoint_advance_skips_committed_batch_on_retry(
    monkeypatch,
):
    """A lost checkpoint response resumes from its committed next offset."""
    build = _build()
    _patch_one_evidence_batch(monkeypatch)
    state = SimpleNamespace(
        committed_next_batch=0,
        insert_count=0,
    )

    async def status(sql, **_params):
        if "ON CONFLICT (evidence_key) DO NOTHING" in str(sql):
            state.insert_count += 1
        return 1

    async def ambiguous_advance(*_args, **_kwargs):
        state.committed_next_batch = 1
        raise RuntimeError("checkpoint response lost after commit")

    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_advance_provider_directory_profile_build_checkpoint",
        ambiguous_advance,
    )

    with pytest.raises(RuntimeError, match="checkpoint response lost"):
        await importer._populate_provider_directory_profile_evidence_stage(
            build,
            has_evidence_target=False,
            bounded=True,
            checkpointed=True,
        )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=False,
        bounded=True,
        start_batch=state.committed_next_batch,
        checkpointed=False,
    )

    assert state.committed_next_batch == 1
    assert state.insert_count == 1
