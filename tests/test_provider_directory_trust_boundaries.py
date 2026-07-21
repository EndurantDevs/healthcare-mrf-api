# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from api import provider_directory_sources
from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _catalog_manifest(entries: list[object]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "importer": "provider-directory-fhir",
        "entries": entries,
    }


@pytest.mark.parametrize(
    ("payload", "profile_source_ids", "error"),
    [
        ([], (), "provider_directory_source_manifest_invalid"),
        (
            _catalog_manifest([None]),
            (),
            "provider_directory_source_manifest_invalid",
        ),
        (
            _catalog_manifest(
                [
                    {
                        "entry_id": "malformed",
                        "classification": "probe_only",
                        "source_ids": [],
                    }
                ]
            ),
            (),
            "provider_directory_source_manifest_invalid",
        ),
        (
            _catalog_manifest(
                [
                    {
                        "entry_id": "runnable",
                        "classification": "acquisition",
                        "source_ids": ["pdfhir_runnable"],
                    }
                ]
            ),
            ("pdfhir_other",),
            "provider_directory_profile_source_catalog_drift",
        ),
    ],
)
def test_source_catalog_rejects_invalid_or_profile_drifted_manifests(
    tmp_path,
    monkeypatch,
    payload,
    profile_source_ids,
    error,
):
    """Fail closed before exposing malformed or non-Profile runnable sources."""
    manifest_path = _write_json(tmp_path / "manifest.json", payload)
    monkeypatch.setattr(
        provider_directory_sources.profile_artifact,
        "configured_profile_source_ids",
        lambda: profile_source_ids,
    )

    with pytest.raises(RuntimeError, match=error):
        provider_directory_sources.provider_directory_source_catalog(
            manifest_path
        )


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {
            "schema_version": 1,
            "source_ids": [],
            "entry_ids": ["entry"],
        },
    ],
)
def test_profile_source_spec_rejects_invalid_contracts(tmp_path, payload):
    """Reject both an invalid envelope and invalid source membership."""
    spec_path = _write_json(tmp_path / "profile-sources.json", payload)

    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_source_spec_invalid",
    ):
        profile.load_profile_source_spec(spec_path)


def test_profile_sql_template_renderer_rejects_contract_drift(monkeypatch):
    """Surface both missing requested tokens and unresolved template tokens."""
    monkeypatch.setattr(profile, "_sql_template", lambda _filename: "SELECT 1")
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_sql_token_missing:EXPECTED",
    ):
        profile._render_sql_template("fixture.sql", {"EXPECTED": "value"})

    monkeypatch.setattr(
        profile,
        "_sql_template",
        lambda _filename: "{{KNOWN}} {{LEFTOVER}}",
    )
    with pytest.raises(
        RuntimeError,
        match=r"provider_directory_profile_sql_tokens_unresolved:\{\{LEFTOVER\}\}",
    ):
        profile._render_sql_template("fixture.sql", {"KNOWN": "value"})


def test_profile_helpers_cover_incremental_and_bounded_identifier_paths():
    """Keep incremental invalidation and PostgreSQL name bounds explicit."""
    assert profile.sibling_table_ref("unqualified", "sibling") == '"sibling"'

    index_name = profile.profile_index_name("p" * 64, "generation_idx")
    assert len(index_name) <= 63
    assert index_name.startswith("p" * 50 + "_")

    incremental_sql = profile.profile_insert_sql(
        evidence_ref='"fixture"."new_evidence"',
        target_ref='"fixture"."new_profile"',
        old_evidence_ref='"fixture"."old_evidence"',
        rebuild_all=False,
    )
    assert 'FROM "fixture"."old_evidence"' in incremental_sql
    assert "source_id <> ALL(CAST(:retained_source_ids AS varchar[]))" in (
        incremental_sql
    )
    assert "OR NOT" in incremental_sql


def test_fhir_reference_sql_rejects_non_resource_type_fragments():
    """Do not permit a resource type to become an injected SQL regex fragment."""
    with pytest.raises(ValueError, match="invalid FHIR resource type"):
        profile.fhir_reference_resource_id_sql("role.reference", "Organization|")


def test_artifact_stage_normalization_and_collection_reject_invalid_shapes():
    """Reject malformed deferred stages before an atomic artifact promotion."""
    bundle = importer.ProviderDirectoryArtifactBundle()

    assert importer._normalize_provider_directory_artifact_stages(None) is None
    with pytest.raises(
        RuntimeError,
        match="provider_directory_artifact_stage_result_invalid",
    ):
        importer._collect_provider_directory_artifact_stage("invalid", bundle)
    with pytest.raises(
        RuntimeError,
        match="provider_directory_artifact_stage_result_invalid",
    ):
        importer._collect_provider_directory_artifact_stage(
            ("invalid-metrics", ()),
            bundle,
        )


@contextlib.asynccontextmanager
async def _transaction():
    yield


@pytest.mark.asyncio
async def test_retry_state_cleanup_only_clears_all_finalized_datasets(
    monkeypatch,
):
    """Clear checkpoints only after every selected dataset is finalized."""
    all_rows = AsyncMock(
        return_value=[
            {"dataset_id": "dataset_b"},
            {"dataset_id": "dataset_a"},
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_rows)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "transaction", _transaction)

    cleared = await importer._clear_promoted_endpoint_dataset_retry_state(
        [" dataset_b ", "dataset_a", "", None]
    )

    assert cleared == ["dataset_a", "dataset_b"]
    assert all_rows.await_args.kwargs["dataset_ids"] == [
        "dataset_a",
        "dataset_b",
    ]
    delete_sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "provider_directory_dataset_resource" in delete_sql
    assert "provider_directory_pagination_checkpoint" in delete_sql


@pytest.mark.asyncio
async def test_retry_state_cleanup_keeps_partial_or_empty_selections(
    monkeypatch,
):
    """Preserve resumability when no IDs or only some IDs are finalized."""
    all_rows = AsyncMock(return_value=[{"dataset_id": "dataset_a"}])
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "all", all_rows)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "transaction", _transaction)

    assert await importer._clear_promoted_endpoint_dataset_retry_state([]) == []
    assert (
        await importer._clear_promoted_endpoint_dataset_retry_state(
            ["dataset_a", "dataset_b"]
        )
        == []
    )
    assert "DELETE FROM" not in "\n".join(
        call.args[0] for call in status.await_args_list
    )


@pytest.mark.asyncio
async def test_retry_state_cleanup_is_best_effort_on_lock_failure(monkeypatch):
    """Leave checkpoints intact when the cutover cleanup lock cannot be acquired."""

    @contextlib.asynccontextmanager
    async def failed_transaction():
        raise RuntimeError("lock unavailable")
        yield

    monkeypatch.setattr(importer.db, "transaction", failed_transaction)

    assert (
        await importer._clear_promoted_endpoint_dataset_retry_state(
            ["dataset_a"]
        )
        == []
    )


@pytest.mark.asyncio
async def test_deferred_corroboration_requires_unified_address_table(monkeypatch):
    """Do not claim a prepared corroboration artifact without its base table."""
    monkeypatch.setattr(
        importer,
        "_is_table_present",
        AsyncMock(return_value=False),
    )

    assert not await importer._is_provider_directory_corroboration_artifact_published(
        source_ids=["source_a"],
        network_catalog_metrics={},
        artifact_bundle=importer.ProviderDirectoryArtifactBundle(),
    )


@pytest.mark.asyncio
async def test_profile_index_renames_cover_compact_and_evidence_sets(monkeypatch):
    """Rename every prepared index to its stable post-cutover name."""
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)

    await importer._rename_provider_directory_profile_table_indexes(
        "mrf",
        "profile_stage",
    )
    await importer._rename_provider_directory_profile_evidence_indexes(
        "mrf",
        "evidence_stage",
    )

    statements = [call.args[0] for call in status.await_args_list]
    assert len(statements) == 1 + len(profile.PROFILE_EVIDENCE_INDEX_SUFFIXES)
    assert all(statement.startswith("ALTER INDEX") for statement in statements)
    assert all("RENAME TO" in statement for statement in statements)


def _profile_build() -> importer._ProviderDirectoryProfileBuild:
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation",
        source_ids=("source_a",),
        retained_source_ids=("source_a", "source_b"),
        dataset_ids=("dataset_a",),
        profile_as_of="2026-07-19",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )


async def _noop_stage_index_rename(_schema: str, _stage: str) -> None:
    return None


def _artifact_stage(
    *,
    schema: str = "mrf",
    stage_table: str = "profile_stage",
    target_relation: str = "provider_directory_profile",
    target_oid: int | None = None,
    fenced: bool = False,
) -> importer.ProviderDirectoryPreparedArtifactStage:
    build_fence = (
        importer.ProviderDirectoryArtifactBuildFence(target_oid=target_oid)
        if fenced
        else None
    )
    return importer.ProviderDirectoryPreparedArtifactStage(
        schema=schema,
        stage_table=stage_table,
        target_relation=target_relation,
        rename_stage_indexes=_noop_stage_index_rename,
        build_fence=build_fence,
    )


def _promotion_dataset(
    **overrides: object,
) -> importer.ProviderDirectoryArtifactDataset:
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-new",
        dataset_id="dataset-new",
        evidence_run_id="run-a",
        serving_endpoint_id="endpoint-old",
        expected_incumbent_dataset_id="dataset-old",
        promote_on_cutover=True,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        dataset_hash="hash-a",
    )
    return importer.replace(dataset, **overrides)


def _profile_checkpoint_by_name(
    build: importer._ProviderDirectoryProfileBuild,
    *,
    state: str = "building_profile",
    evidence_next_batch: int = 52,
    evidence_total_batches: int = 52,
    profile_next_batch: int = 1,
    profile_total_batches: int = 400,
) -> dict[str, object]:
    return {
        "build_id": importer._provider_directory_profile_build_id(build),
        "strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": profile.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "profile_as_of": build.profile_as_of,
        "source_ids": list(build.source_ids),
        "retained_source_ids": list(build.retained_source_ids),
        "dataset_ids": list(build.dataset_ids),
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": 11,
        "profile_stage_oid": 12,
        "evidence_target_oid": None,
        "profile_target_oid": None,
        "has_existing_artifacts": False,
        "evidence_next_batch": evidence_next_batch,
        "evidence_total_batches": evidence_total_batches,
        "profile_next_batch": profile_next_batch,
        "profile_total_batches": profile_total_batches,
        "state": state,
    }


def test_profile_batch_and_checkpoint_value_contracts():
    """Cover bounded batch construction and strict checkpoint decoding."""
    build = _profile_build()
    assert importer._provider_directory_profile_build_id(build) == (
        build.generation_id
    )
    explicit_build = importer.replace(build, build_id="profile-build")
    assert importer._provider_directory_profile_build_id(explicit_build) == (
        "profile-build"
    )
    evidence_batches = importer._provider_directory_profile_evidence_batches(
        build,
        has_existing_artifacts=True,
    )
    assert evidence_batches[0].kind == "copy"
    affiliation_batches = [
        batch
        for batch in evidence_batches
        if batch.fact_type == "affiliation"
    ]
    assert len(affiliation_batches) == profile.PROFILE_AFFILIATION_ROLE_BUCKETS
    assert affiliation_batches[-1].role_bucket == (
        profile.PROFILE_AFFILIATION_ROLE_BUCKETS - 1
    )
    compact_batches = importer._provider_directory_profile_compact_batches(
        has_existing_artifacts=True,
        npi_batch_size=500_000_000,
    )
    assert compact_batches[0].kind == "copy"
    assert compact_batches[-1].npi_end == profile.NPI_MAX + 1
    with pytest.raises(ValueError, match="batch size must be positive"):
        importer._provider_directory_profile_compact_batches(
            has_existing_artifacts=False,
            npi_batch_size=0,
        )
    assert importer._provider_directory_profile_checkpoint_array(
        '["source_a", "source_b"]'
    ) == ("source_a", "source_b")
    assert importer._provider_directory_profile_checkpoint_array(
        ("source_a",)
    ) == ()
    checkpoint_state = importer._provider_directory_profile_checkpoint_state(
        {
            "evidence_next_batch": 2,
            "evidence_total_batches": 3,
            "profile_next_batch": 4,
            "profile_total_batches": 5,
            "state": "failed",
        }
    )
    assert checkpoint_state == (
        importer._ProviderDirectoryProfileBuildCheckpointState(
            evidence_next_batch=2,
            evidence_total_batches=3,
            profile_next_batch=4,
            profile_total_batches=5,
            state="failed",
        )
    )


@pytest.mark.asyncio
async def test_profile_build_resolution_preserves_only_logged_retry_lineage(
    monkeypatch,
):
    """Reuse an as-of date only for the exact permanent stage pair."""
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(
            return_value=(
                ["source_a"],
                ["source_a", "source_b"],
                (
                    importer._ProviderDirectoryProfileSourceContext(
                        source_id="source_a",
                        endpoint_id="endpoint_a",
                        canonical_api_base="https://example.test/fhir",
                        org_name="Example",
                        plan_name=None,
                    ),
                ),
            )
        ),
    )
    first = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first)
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=7)
    dataset_fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.ProviderDirectoryArtifactDataset(
                source_id="source_a",
                endpoint_id="endpoint_a",
                dataset_id="dataset_a",
                evidence_run_id="run_a",
            ),
        )
    )
    initial_build = await importer._resolve_provider_directory_profile_build(
        "mrf",
        "owner-run",
        dataset_fence,
        fence,
        fence,
    )
    checkpoint_by_name = {
        **_profile_checkpoint_by_name(initial_build),
        "profile_as_of": "2026-07-01",
        "evidence_target_oid": 7,
        "profile_target_oid": 7,
    }
    first.return_value = checkpoint_by_name
    stage_identity = AsyncMock(
        side_effect=[(11, "r", "p"), (12, "r", "p")]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    resumed_build = await importer._resolve_provider_directory_profile_build(
        "mrf",
        "retry-run",
        dataset_fence,
        fence,
        fence,
    )
    assert resumed_build.profile_as_of == "2026-07-01"
    assert resumed_build.owner_run_id == "retry-run"
    assert stage_identity.await_count == 2

    checkpoint_by_name["profile_as_of"] = "not-a-date"
    stage_identity.reset_mock()
    invalid_date_build = (
        await importer._resolve_provider_directory_profile_build(
            "mrf",
            None,
            dataset_fence,
            fence,
            fence,
        )
    )
    assert invalid_date_build.profile_as_of != "not-a-date"
    stage_identity.assert_not_awaited()

    checkpoint_by_name["profile_as_of"] = "2026-07-01"
    stage_identity.side_effect = [(11, "r", "p"), (12, "r", "u")]
    nonlogged_build = await importer._resolve_provider_directory_profile_build(
        "mrf",
        None,
        dataset_fence,
        fence,
        fence,
    )
    assert nonlogged_build.profile_as_of != "2026-07-01"


@pytest.mark.asyncio
async def test_profile_checkpoint_rejects_compact_progress_before_evidence(
    monkeypatch,
):
    """Reinitialize rather than aggregate from an incomplete evidence stage."""
    build = _profile_build()
    stage_identity = AsyncMock(return_value=(11, "r", "p"))
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    checkpoint_by_name = {
        "build_id": build.generation_id,
        "strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": profile.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "profile_as_of": build.profile_as_of,
        "source_ids": list(build.source_ids),
        "retained_source_ids": list(build.retained_source_ids),
        "dataset_ids": list(build.dataset_ids),
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": 11,
        "profile_stage_oid": 12,
        "evidence_target_oid": None,
        "profile_target_oid": None,
        "has_existing_artifacts": False,
        "evidence_next_batch": 51,
        "evidence_total_batches": 52,
        "profile_next_batch": 1,
        "profile_total_batches": 400,
        "state": "building_profile",
    }
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    assert not await importer._is_profile_build_checkpoint_reusable(
        build,
        checkpoint_by_name,
        has_existing_artifacts=False,
        evidence_build_fence=fence,
        profile_build_fence=fence,
        evidence_total_batches=52,
        profile_total_batches=400,
    )
    stage_identity.assert_not_awaited()


@pytest.mark.asyncio
async def test_profile_checkpoint_claim_initializes_and_reclaims_logged_stages(
    monkeypatch,
):
    """Cover fresh initialization plus partial and ready reclaim states."""
    build = _profile_build()
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    first = AsyncMock(return_value=None)
    status = AsyncMock(return_value=1)
    stage_identity = AsyncMock(
        side_effect=[
            None,
            None,
            (21, "r", "p"),
            (22, "r", "p"),
        ]
    )
    monkeypatch.setattr(importer.db, "first", first)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    fresh_state = (
        await importer._claim_provider_directory_profile_build_checkpoint(
            build,
            has_existing_artifacts=False,
            evidence_build_fence=fence,
            profile_build_fence=fence,
        )
    )
    assert fresh_state.state == "building_evidence"
    assert fresh_state.evidence_next_batch == 0
    assert fresh_state.profile_next_batch == 0
    assert any(
        "INSERT INTO" in str(call.args[0])
        and "profile_build_checkpoint" in str(call.args[0])
        for call in status.await_args_list
    )

    checkpoint_by_name = _profile_checkpoint_by_name(build)
    first.return_value = checkpoint_by_name
    status.reset_mock()
    stage_identity.side_effect = [
        (11, "r", "p"),
        (12, "r", "p"),
        (11, "r", "p"),
        (12, "r", "p"),
        (11, "r", "p"),
        (12, "r", "p"),
    ]
    reclaimed_state = (
        await importer._claim_provider_directory_profile_build_checkpoint(
            build,
            has_existing_artifacts=False,
            evidence_build_fence=fence,
            profile_build_fence=fence,
        )
    )
    assert reclaimed_state.state == "building_profile"
    assert reclaimed_state.profile_next_batch == 1
    assert status.await_count == 1

    checkpoint_by_name.update(
        {
            "state": "failed",
            "profile_next_batch": 400,
        }
    )
    ready_state = (
        await importer._claim_provider_directory_profile_build_checkpoint(
            build,
            has_existing_artifacts=False,
            evidence_build_fence=fence,
            profile_build_fence=fence,
        )
    )
    assert ready_state.state == "ready"

    status.return_value = 0
    with pytest.raises(
        RuntimeError,
        match="checkpoint_claim_lost",
    ):
        await importer._claim_provider_directory_profile_build_checkpoint(
            build,
            has_existing_artifacts=False,
            evidence_build_fence=fence,
            profile_build_fence=fence,
        )


@pytest.mark.asyncio
async def test_profile_checkpoint_mutations_fail_closed(monkeypatch):
    """Require valid phases, ownership, and best-effort failure cleanup."""
    build = _profile_build()
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._advance_provider_directory_profile_build_checkpoint(
        build,
        phase="evidence",
        expected_batch=2,
        total_batches=52,
    )
    await importer._advance_provider_directory_profile_build_checkpoint(
        build,
        phase="profile",
        expected_batch=3,
        total_batches=400,
    )
    assert status.await_args_list[0].kwargs["next_batch"] == 3
    assert status.await_args_list[1].kwargs["state"] == "building_profile"
    with pytest.raises(ValueError, match="unsupported profile checkpoint"):
        await importer._advance_provider_directory_profile_build_checkpoint(
            build,
            phase="unknown",
            expected_batch=0,
            total_batches=1,
        )

    await importer._mark_profile_build_checkpoint_state(build, "ready")
    await importer._mark_profile_build_checkpoint_failed(
        build,
        RuntimeError("forced failure"),
    )
    await importer._delete_provider_directory_profile_build_checkpoint(
        build.schema,
        build.generation_id,
    )
    assert "RuntimeError: forced failure" in str(
        status.await_args_list[-2].kwargs["last_error"]
    )

    status.return_value = 0
    with pytest.raises(
        RuntimeError,
        match="checkpoint_ownership_lost",
    ):
        await importer._advance_provider_directory_profile_build_checkpoint(
            build,
            phase="evidence",
            expected_batch=0,
            total_batches=52,
        )
    with pytest.raises(
        RuntimeError,
        match="checkpoint_ownership_lost",
    ):
        await importer._mark_profile_build_checkpoint_state(
            build,
            "evidence_complete",
        )

    status.side_effect = RuntimeError("checkpoint storage unavailable")
    await importer._mark_profile_build_checkpoint_failed(
        build,
        RuntimeError("original failure"),
    )
    await importer._delete_provider_directory_profile_build_checkpoint(
        build.schema,
        build.generation_id,
    )


@pytest.mark.asyncio
async def test_existing_profile_stages_receive_retention_and_currentness_params(
    monkeypatch,
):
    """Pass the trust fence into both evidence copy and compact invalidation."""
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    build = _profile_build()

    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
    )
    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=True,
    )

    parameter_sets = [call.kwargs for call in status.await_args_list]
    retained_calls = [
        params
        for params in parameter_sets
        if params.get("retained_source_ids") == ["source_a", "source_b"]
    ]
    assert len(retained_calls) == 3
    assert all(params["profile_as_of"] == "2026-07-19" for params in retained_calls)


@pytest.mark.asyncio
async def test_profile_bounded_population_rejects_invalid_resume_batches(
    monkeypatch,
):
    """Fail closed on invalid offsets or malformed evidence/profile batches."""
    build = _profile_build()
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=1))
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    with pytest.raises(
        RuntimeError,
        match="profile_evidence_checkpoint_invalid",
    ):
        await importer._populate_provider_directory_profile_evidence_stage(
            build,
            has_evidence_target=False,
            bounded=True,
            start_batch=-1,
        )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_evidence_batches",
        lambda *_args, **_params: (
            importer._ProviderDirectoryProfileEvidenceBatch(kind="fact"),
        ),
    )
    with pytest.raises(
        RuntimeError,
        match="profile_evidence_batch_invalid",
    ):
        await importer._populate_provider_directory_profile_evidence_stage(
            build,
            has_evidence_target=False,
            bounded=True,
        )

    with pytest.raises(ValueError, match="batch size must be positive"):
        await importer._populate_provider_directory_profile_compact_stage(
            build,
            has_existing_artifacts=False,
            npi_batch_size=0,
        )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_compact_batches",
        lambda **_params: (
            importer._ProviderDirectoryProfileCompactBatch(kind="npi"),
        ),
    )
    with pytest.raises(
        RuntimeError,
        match="profile_compact_checkpoint_invalid",
    ):
        await importer._populate_provider_directory_profile_compact_stage(
            build,
            has_existing_artifacts=False,
            npi_batch_size=5_000_000,
            start_batch=2,
        )
    with pytest.raises(
        RuntimeError,
        match="profile_compact_batch_invalid",
    ):
        await importer._populate_provider_directory_profile_compact_stage(
            build,
            has_existing_artifacts=False,
            npi_batch_size=5_000_000,
        )


@pytest.mark.asyncio
async def test_profile_bounded_population_checkpoints_copy_and_range_batches(
    monkeypatch,
):
    """Advance durable progress only after each successful bounded write."""
    build = _profile_build()
    status = AsyncMock(return_value=3)
    advance = AsyncMock()
    mark_state = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_advance_provider_directory_profile_build_checkpoint",
        advance,
    )
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_state",
        mark_state,
    )
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_evidence_batches",
        lambda *_args, **_params: (
            importer._ProviderDirectoryProfileEvidenceBatch(kind="copy"),
            importer._ProviderDirectoryProfileEvidenceBatch(
                kind="fact",
                source_id="source_a",
                dataset_id="dataset_a",
                fact_type="affiliation",
                role_bucket_count=2,
                role_bucket=1,
            ),
        ),
    )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
        bounded=True,
        checkpointed=True,
    )
    assert [call.kwargs["expected_batch"] for call in advance.await_args_list] == [
        0,
        1,
    ]
    assert status.await_args_list[1].kwargs["profile_role_bucket"] == 1
    mark_state.assert_awaited_once_with(build, "evidence_complete")

    status.reset_mock()
    advance.reset_mock()
    mark_state.reset_mock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_compact_batches",
        lambda **_params: (
            importer._ProviderDirectoryProfileCompactBatch(kind="copy"),
            importer._ProviderDirectoryProfileCompactBatch(
                kind="npi",
                npi_start=1_000_000_000,
                npi_end=1_005_000_000,
            ),
        ),
    )
    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=True,
        npi_batch_size=5_000_000,
        checkpointed=True,
    )
    assert [call.kwargs["expected_batch"] for call in advance.await_args_list] == [
        0,
        1,
    ]
    assert status.await_args_list[1].kwargs["profile_npi_start"] == (
        1_000_000_000
    )
    mark_state.assert_awaited_once_with(build, "ready")


@pytest.mark.asyncio
async def test_profile_stage_finalization_supports_deferred_and_immediate_cutover(
    monkeypatch,
):
    """Return prepared stages when deferred and clean both after immediate cutover."""
    stages = (
        SimpleNamespace(stage_table="a", resume_checkpoint=None),
        SimpleNamespace(stage_table="b", resume_checkpoint=None),
    )
    metric_map = {"profile_rows": 2}
    promote = AsyncMock()
    remove = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promote,
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove,
    )

    assert await importer._finalize_provider_directory_profile_stages(
        metric_map,
        stages,
        defer_cutover=True,
    ) == (metric_map, stages)
    assert await importer._finalize_provider_directory_profile_stages(
        metric_map,
        stages,
        defer_cutover=False,
    ) == metric_map
    promote.assert_awaited_once_with(stages)
    assert [call.args[0] for call in remove.await_args_list] == list(
        reversed(stages)
    )


@pytest.mark.asyncio
async def test_artifact_bundle_retains_resumable_stages_until_cutover_succeeds(
    monkeypatch,
):
    """Keep checkpointed Profile stages after failure, then clean after cutover."""

    async def rename_indexes(_schema, _stage):
        return None

    checkpoint = ("mrf", "profile-build-1")
    retained_stages = [
        importer.ProviderDirectoryPreparedArtifactStage(
            schema="mrf",
            stage_table=stage_table,
            target_relation=target_relation,
            rename_stage_indexes=rename_indexes,
            retain_on_failed_bundle=True,
            resume_checkpoint=checkpoint,
        )
        for stage_table, target_relation in (
            ("profile_evidence_stage", profile.PROFILE_EVIDENCE_TABLE),
            ("profile_stage", profile.PROFILE_TABLE),
        )
    ]
    disposable_stage = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="other_stage",
        target_relation="other_target",
        rename_stage_indexes=rename_indexes,
    )
    bundle = importer.ProviderDirectoryArtifactBundle(
        stages=[*retained_stages, disposable_stage]
    )
    promotion = AsyncMock(side_effect=RuntimeError("cutover failed"))
    remove = AsyncMock()
    delete_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promotion,
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove,
    )
    monkeypatch.setattr(
        importer,
        "_delete_provider_directory_profile_build_checkpoint",
        delete_checkpoint,
    )

    with pytest.raises(RuntimeError, match="cutover failed"):
        await bundle.promote()
    await bundle.cleanup()

    assert bundle.promoted is False
    remove.assert_awaited_once_with(disposable_stage)
    delete_checkpoint.assert_not_awaited()

    promotion.side_effect = None
    promotion.reset_mock()
    remove.reset_mock()
    assert await bundle.promote() == 3
    await bundle.cleanup()

    assert bundle.promoted is True
    promotion.assert_awaited_once_with(tuple(bundle.stages))
    delete_checkpoint.assert_awaited_once_with(*checkpoint)
    assert [call.args[0] for call in remove.await_args_list] == list(
        reversed(bundle.stages)
    )


def test_stale_profile_reaper_rejects_non_deterministic_stage_names():
    """Never trust checkpoint-provided identifiers as cleanup targets."""
    build_id = f"pdpb_{'a' * 32}"
    with pytest.raises(
        RuntimeError,
        match="provider_directory_profile_stale_checkpoint_stage_invalid",
    ):
        importer._validated_profile_checkpoint_stage_names(
            {
                "build_id": build_id,
                "evidence_stage": profile.PROFILE_EVIDENCE_TABLE,
                "profile_stage": profile.PROFILE_TABLE,
            }
        )


@pytest.mark.asyncio
async def test_profile_stage_identity_and_stale_reaper_are_oid_fenced(
    monkeypatch,
):
    """Lock, recheck, and remove only exact permanent stale stage tables."""
    build_id = f"pdpb_{'a' * 32}"
    evidence_stage = profile.profile_evidence_stage_table_name(build_id)
    profile_stage = profile.profile_stage_table_name(build_id)
    first = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first)
    assert await importer._provider_directory_profile_stage_relation_identity(
        "mrf",
        evidence_stage,
    ) is None
    first.return_value = {
        "relation_oid": 11,
        "relation_kind": "r",
        "relation_persistence": "p",
    }
    assert await importer._provider_directory_profile_stage_relation_identity(
        "mrf",
        evidence_stage,
    ) == (11, "r", "p")
    assert importer._validated_profile_checkpoint_stage_names(
        {
            "build_id": build_id,
            "evidence_stage": evidence_stage,
            "profile_stage": profile_stage,
        }
    ) == (evidence_stage, profile_stage)
    with pytest.raises(
        RuntimeError,
        match="stale_checkpoint_identity_invalid",
    ):
        importer._validated_profile_checkpoint_stage_names(
            {
                "build_id": "not-a-build-id",
                "evidence_stage": evidence_stage,
                "profile_stage": profile_stage,
            }
        )

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    checkpoint_by_name = {
        "build_id": build_id,
        "evidence_stage": evidence_stage,
        "profile_stage": profile_stage,
        "evidence_stage_oid": 21,
        "profile_stage_oid": 22,
    }
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[checkpoint_by_name, {}]),
    )
    first.return_value = checkpoint_by_name
    relation_identity = AsyncMock(
        side_effect=[
            (22, "r", "p"),
            (22, "r", "p"),
            (21, "r", "p"),
            (21, "r", "p"),
        ]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        relation_identity,
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    assert await importer._reap_stale_provider_directory_profile_builds(
        "mrf",
        current_build_id=f"pdpb_{'b' * 32}",
    ) == 1
    statements = [str(call.args[0]) for call in status.await_args_list]
    assert sum(statement.startswith("LOCK TABLE") for statement in statements) == 2
    assert sum(statement.startswith("DROP TABLE") for statement in statements) == 2

    relation_identity.side_effect = [(21, "v", "p")]
    with pytest.raises(
        RuntimeError,
        match="stale_stage_oid_mismatch",
    ):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=f"pdpb_{'b' * 32}",
        )

    relation_identity.side_effect = [
        (22, "r", "p"),
        (23, "r", "p"),
    ]
    with pytest.raises(
        RuntimeError,
        match="stale_stage_identity_changed",
    ):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=f"pdpb_{'b' * 32}",
        )


@pytest.mark.asyncio
async def test_profile_stage_preparation_metrics_and_pair_contract(
    monkeypatch,
):
    """Prepare both logged stages and reject a partial serving-table pair."""
    build = _profile_build()
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=9)
    assert_logged = AsyncMock()
    assert_ready = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_logged_relation",
        assert_logged,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_checkpoint_ready",
        assert_ready,
    )
    stages = await importer._prepare_provider_directory_profile_stages(
        build,
        fence,
        fence,
    )
    assert [stage.stage_table for stage in stages] == [
        build.evidence_stage,
        build.profile_stage,
    ]
    assert all(stage.retain_on_failed_bundle for stage in stages)
    assert all(
        stage.resume_checkpoint == (build.schema, build.generation_id)
        for stage in stages
    )
    assert assert_logged.await_count == 2
    assert_ready.assert_awaited_once_with(build, fence, fence)

    stage_metrics = AsyncMock(return_value={"profile_rows": 4})
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_metrics",
        stage_metrics,
    )
    metrics = await importer._provider_directory_profile_metrics(
        build,
        should_rebuild_all_profiles=False,
    )
    assert metrics["generation_id"] == build.generation_id
    assert metrics["dataset_ids"] == ["dataset_a"]
    assert metrics["incremental"] is True

    table_exists = AsyncMock(side_effect=[False, False])
    monkeypatch.setattr(importer, "_is_table_present", table_exists)
    assert not await importer._has_provider_directory_profile_artifacts("mrf")
    table_exists.side_effect = [True, True]
    assert await importer._has_provider_directory_profile_artifacts("mrf")
    table_exists.side_effect = [True, False]
    with pytest.raises(
        RuntimeError,
        match="profile_artifact_pair_incomplete",
    ):
        await importer._has_provider_directory_profile_artifacts("mrf")


@pytest.mark.asyncio
async def test_artifact_promotion_identity_recovers_only_exact_cutover(
    monkeypatch,
):
    """Resolve ambiguous cutover results from exact stage and target OIDs."""

    async def rename_indexes(_schema, _stage):
        return None

    stage = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="profile_stage",
        target_relation="provider_directory_profile",
        rename_stage_indexes=rename_indexes,
    )
    relation_oid = AsyncMock(side_effect=[11])
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_oid",
        relation_oid,
    )
    identities = (
        await importer._capture_provider_directory_artifact_promotion_identities(
            (stage,)
        )
    )
    assert identities == (
        importer.ProviderDirectoryArtifactPromotionIdentity(
            stage=stage,
            stage_oid=11,
        ),
    )
    relation_oid.side_effect = [None]
    assert (
        await importer._capture_provider_directory_artifact_promotion_identities(
            (stage,)
        )
        == ()
    )
    assert not await importer._is_provider_directory_artifact_promotion_committed(
        ()
    )

    identity = identities[0]
    relation_attribute = AsyncMock(return_value="p")
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_attribute",
        relation_attribute,
    )
    for relation_values, persistence in (
        ((12, None, None), "p"),
        ((11, 12, None), "p"),
        ((11, None, 12), "p"),
        ((11, None, None), "u"),
    ):
        relation_oid.side_effect = relation_values
        relation_attribute.return_value = persistence
        assert not (
            await importer._is_provider_directory_artifact_promotion_committed(
                (identity,)
            )
        )

    relation_oid.side_effect = [11, None, None]
    relation_attribute.return_value = "p"
    assert await importer._is_provider_directory_artifact_promotion_committed(
        (identity,)
    )
    dataset_committed = AsyncMock(return_value=True)
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_dataset_cutover_committed",
        dataset_committed,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    relation_oid.side_effect = [11, None, None]
    assert await importer._is_provider_directory_artifact_promotion_committed(
        (identity,),
        fence,
    )
    dataset_committed.assert_awaited_once_with(fence)


@pytest.mark.asyncio
async def test_dataset_cutover_commit_requires_exact_rows_and_aliases(
    monkeypatch,
):
    """Reject missing, stale, duplicated, or unsuperseded dataset pointers."""
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="endpoint_a",
        dataset_id="dataset_new",
        evidence_run_id="run_a",
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        expected_incumbent_dataset_id="dataset_old",
        promote_on_cutover=True,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    dataset_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(
        importer,
        "_artifact_fence_dataset_rows",
        dataset_rows,
    )
    assert not await importer._is_provider_directory_dataset_cutover_committed(
        fence
    )

    selected_row_by_field = {
        "dataset_id": dataset.dataset_id,
        "endpoint_id": dataset.endpoint_id,
    }
    dataset_rows.return_value = [selected_row_by_field]
    row_exact = Mock(return_value=False)
    monkeypatch.setattr(
        importer,
        "_is_artifact_fence_dataset_row_exact",
        row_exact,
    )
    assert not await importer._is_provider_directory_dataset_cutover_committed(
        fence
    )
    expected_dataset = row_exact.call_args.args[0]
    assert expected_dataset.status == importer.ENDPOINT_DATASET_PUBLISHED
    assert expected_dataset.is_current is True

    row_exact.return_value = True
    current_ids = Mock(return_value=[])
    monkeypatch.setattr(
        importer,
        "_current_published_artifact_dataset_ids",
        current_ids,
    )
    assert not await importer._is_provider_directory_dataset_cutover_committed(
        fence
    )
    current_ids.return_value = [dataset.dataset_id]
    incumbent_superseded = Mock(return_value=False)
    monkeypatch.setattr(
        importer,
        "_is_artifact_incumbent_superseded",
        incumbent_superseded,
    )
    assert not await importer._is_provider_directory_dataset_cutover_committed(
        fence
    )
    incumbent_superseded.return_value = True
    source_endpoint_cutover = (
        importer._is_artifact_source_endpoint_cutover_committed
    )
    aliases_committed = AsyncMock(return_value=True)
    monkeypatch.setattr(
        importer,
        "_is_artifact_source_endpoint_cutover_committed",
        aliases_committed,
    )
    assert await importer._is_provider_directory_dataset_cutover_committed(
        fence
    )
    aliases_committed.assert_awaited_once_with(fence)
    monkeypatch.setattr(
        importer,
        "_is_artifact_source_endpoint_cutover_committed",
        source_endpoint_cutover,
    )

    source_rows = AsyncMock(
        return_value=[{"source_id": "source_a", "endpoint_id": "endpoint_a"}]
    )
    monkeypatch.setattr(importer.db, "all", source_rows)
    ordinary_fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.replace(
                dataset,
                status=importer.ENDPOINT_DATASET_PUBLISHED,
                is_current=True,
                expected_incumbent_dataset_id=None,
                promote_on_cutover=False,
            ),
        )
    )
    assert await importer._is_artifact_source_endpoint_cutover_committed(
        ordinary_fence
    )
    source_rows.return_value = []
    assert not await importer._is_artifact_source_endpoint_cutover_committed(
        ordinary_fence
    )


def test_artifact_incumbent_supersession_contract():
    """Accept only the expected noncurrent superseded incumbent row."""
    ordinary = importer.ProviderDirectoryArtifactDataset(
        source_id="source_a",
        endpoint_id="endpoint_a",
        dataset_id="dataset_a",
        evidence_run_id="run_a",
    )
    assert importer._is_artifact_incumbent_superseded(ordinary, {})
    candidate = importer.replace(
        ordinary,
        promote_on_cutover=True,
        expected_incumbent_dataset_id="dataset_old",
    )
    assert not importer._is_artifact_incumbent_superseded(candidate, {})
    assert not importer._is_artifact_incumbent_superseded(
        candidate,
        {
            "dataset_old": {
                "is_current": True,
                "status": importer.ENDPOINT_DATASET_SUPERSEDED,
                "superseded_at": "2026-07-20",
            }
        },
    )
    assert not importer._is_artifact_incumbent_superseded(
        candidate,
        {
            "dataset_old": {
                "is_current": False,
                "status": importer.ENDPOINT_DATASET_PUBLISHED,
                "superseded_at": "2026-07-20",
            }
        },
    )
    assert not importer._is_artifact_incumbent_superseded(
        candidate,
        {
            "dataset_old": {
                "is_current": False,
                "status": importer.ENDPOINT_DATASET_SUPERSEDED,
                "superseded_at": None,
            }
        },
    )
    assert importer._is_artifact_incumbent_superseded(
        candidate,
        {
            "dataset_old": {
                "is_current": False,
                "status": importer.ENDPOINT_DATASET_SUPERSEDED,
                "superseded_at": "2026-07-20",
            }
        },
    )


@pytest.mark.asyncio
async def test_resource_id_npi_backfill_honors_seen_and_run_scopes(
    monkeypatch,
):
    """Update only selected ten-digit resource IDs under the active fence."""
    status = AsyncMock(side_effect=["UPDATE 2", "UPDATE 3"])
    monkeypatch.setattr(importer.db, "status", status)
    counts = await importer.backfill_provider_directory_resource_id_npis(
        "mrf",
        run_id="run-a",
        source_ids=["source-a", "source-a", ""],
        seen_table="seen_stage",
    )
    assert counts == {"Practitioner": 2, "Organization": 3}
    for call in status.await_args_list:
        assert "seen.run_id" in call.args[0]
        assert call.kwargs["run_id"] == "run-a"
        assert call.kwargs["source_ids"] == ["source-a"]

    status.side_effect = [1, 1]
    status.reset_mock()
    await importer.backfill_provider_directory_resource_id_npis(
        "mrf",
        seen_table="seen_stage",
    )
    assert all("seen.run_id" not in call.args[0] for call in status.await_args_list)

    status.side_effect = [1, 1]
    status.reset_mock()
    await importer.backfill_provider_directory_resource_id_npis(
        "mrf",
        run_id="run-b",
    )
    assert all(
        "resource.last_seen_run_id" in call.args[0]
        for call in status.await_args_list
    )


@pytest.mark.asyncio
async def test_dataset_fence_helpers_lock_record_and_aggregate_proof(
    monkeypatch,
):
    """Keep dataset identity unambiguous and persist one exact proof row."""
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
        dataset_hash="hash-a",
    )
    duplicate_alias = importer.replace(dataset, source_id="source-b")
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (dataset, duplicate_alias),
        should_select_validated_candidates=True,
        promotion_aliases=(dataset,),
    )
    assert importer._unique_artifact_datasets(fence) == [dataset]
    scoped_fence = importer._artifact_fence_for_dataset(fence, "dataset-a")
    assert scoped_fence.datasets == fence.datasets
    assert scoped_fence.promotion_aliases == fence.promotion_aliases
    with pytest.raises(
        RuntimeError,
        match="dataset_endpoint_ambiguous",
    ):
        importer._unique_artifact_datasets(
            importer.ProviderDirectoryArtifactDatasetFence(
                (
                    dataset,
                    importer.replace(
                        dataset,
                        source_id="source-c",
                        endpoint_id="endpoint-b",
                    ),
                )
            )
        )

    executor = SimpleNamespace(scalar=AsyncMock())
    await importer._lock_dataset_serving_relation_build(
        executor,
        dataset.dataset_id,
    )
    assert executor.scalar.await_args.kwargs["lock_key"].endswith(
        dataset.dataset_id
    )

    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    await importer._record_current_dataset_serving_relation_proof(
        dataset,
        "network_plan_proof",
        {"complete": True},
    )
    assert json.loads(status.await_args.kwargs["proof_json"]) == {
        "complete": True
    }
    status.return_value = 0
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="endpoint_dataset_metadata_changed",
    ):
        await importer._record_current_dataset_serving_relation_proof(
            dataset,
            "network_plan_proof",
            {"complete": True},
        )

    count_fields = (
        "insurance_plan_resource_count",
        "insurance_plan_with_network_refs_count",
        "zero_network_reference_plan_count",
        "malformed_network_refs_payload_count",
        "network_reference_value_count",
        "valid_network_reference_count",
        "invalid_network_reference_count",
        "expected_edge_count",
        "edge_count",
        "duplicate_network_reference_count",
        "replaced_edge_count",
        "plan_projection_count",
        "replaced_plan_projection_count",
        "inserted_plan_projection_count",
    )
    proofs = [
        {"dataset_id": "dataset-a", **dict.fromkeys(count_fields, 1)},
        {"dataset_id": "dataset-b", **dict.fromkeys(count_fields, 2)},
    ]
    aggregate = importer._dataset_network_plan_aggregate_proof(
        proofs,
        build_run_id="build-a",
    )
    assert aggregate["complete"] is True
    assert aggregate["dataset_count"] == 2
    assert aggregate["edge_count"] == 3
    assert aggregate["dataset_ids"] == ["dataset-a", "dataset-b"]


@pytest.mark.asyncio
async def test_immediate_profile_cutover_failure_retains_checkpointed_stages(
    monkeypatch,
):
    """Do not delete retry state or stages when immediate promotion fails."""
    stages = (
        SimpleNamespace(
            stage_table="evidence_stage",
            resume_checkpoint=("mrf", "profile-build-1"),
        ),
        SimpleNamespace(
            stage_table="profile_stage",
            resume_checkpoint=("mrf", "profile-build-1"),
        ),
    )
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        AsyncMock(side_effect=RuntimeError("cutover failed")),
    )
    remove = AsyncMock()
    delete_checkpoint = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove,
    )
    monkeypatch.setattr(
        importer,
        "_delete_provider_directory_profile_build_checkpoint",
        delete_checkpoint,
    )

    with pytest.raises(RuntimeError, match="cutover failed"):
        await importer._finalize_provider_directory_profile_stages(
            {"profile_rows": 2},
            stages,
            defer_cutover=False,
        )

    remove.assert_not_awaited()
    delete_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_profile_stage_build_retains_checkpointed_stage_after_failure(
    monkeypatch,
):
    """Retain logged resumable stages and record compact-build failure."""
    build = _profile_build()
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
    mark_failed = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_populate_provider_directory_profile_evidence_stage",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_populate_provider_directory_profile_compact_stage",
        AsyncMock(side_effect=RuntimeError("compact failed")),
    )
    monkeypatch.setattr(
        importer,
        "_claim_provider_directory_profile_build_checkpoint",
        AsyncMock(
            return_value=importer._ProviderDirectoryProfileBuildCheckpointState(
                evidence_next_batch=52,
                evidence_total_batches=52,
                profile_next_batch=9,
                profile_total_batches=400,
                state="building_profile",
            )
        ),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_mark_profile_build_checkpoint_failed",
        mark_failed,
    )

    with pytest.raises(RuntimeError, match="compact failed"):
        await importer._build_provider_directory_profile_stages(
            build,
            fence,
            fence,
        )

    mark_failed.assert_awaited_once()
    assert isinstance(mark_failed.await_args.args[1], RuntimeError)


@pytest.mark.asyncio
async def test_artifact_relation_metadata_and_logged_contract(monkeypatch):
    """Reject unsupported metadata and require persistent serving relations."""
    scalar = AsyncMock(side_effect=[None, "p", "p", "u"])
    monkeypatch.setattr(importer.db, "scalar", scalar)

    with pytest.raises(ValueError, match="unsupported PostgreSQL"):
        await importer._provider_directory_relation_attribute(
            "mrf",
            "profile",
            "relowner",
        )
    assert (
        await importer._provider_directory_relation_attribute(
            "mrf",
            "profile",
            "relkind",
        )
        is None
    )
    assert await importer._provider_directory_relation_attribute(
        "mrf",
        "profile",
        "relpersistence",
    ) == "p"

    await importer._assert_provider_directory_logged_relation("mrf", "profile")
    with pytest.raises(RuntimeError, match="is not LOGGED"):
        await importer._assert_provider_directory_logged_relation(
            "mrf",
            "profile",
        )


def test_artifact_cutover_retryability_follows_postgres_lock_identity():
    """Retry only the explicit cutover conflict and PostgreSQL lock timeout."""
    assert importer._is_provider_directory_artifact_cutover_retryable(
        importer.ProviderDirectoryArtifactCutoverConflict("profile")
    )
    pgcode_error = RuntimeError("lock")
    pgcode_error.pgcode = "55P03"
    assert importer._is_provider_directory_artifact_cutover_retryable(
        pgcode_error
    )
    wrapped_error = RuntimeError("wrapped")
    wrapped_error.orig = SimpleNamespace(sqlstate="55P03")
    assert importer._is_provider_directory_artifact_cutover_retryable(
        wrapped_error
    )
    caused_error = RuntimeError("caused")
    caused_error.__cause__ = RuntimeError("cause")
    caused_error.__cause__.pgcode = "55P03"
    assert importer._is_provider_directory_artifact_cutover_retryable(
        caused_error
    )
    assert not importer._is_provider_directory_artifact_cutover_retryable(
        RuntimeError("ordinary")
    )


@pytest.mark.asyncio
async def test_artifact_lock_prepare_and_build_fence_contract(monkeypatch):
    """Lock tables only and reject missing locks or changed target OIDs."""
    relation_attribute = AsyncMock(side_effect=["r", "p", "v", None])
    status = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_attribute",
        relation_attribute,
    )
    monkeypatch.setattr(importer.db, "status", status)
    await importer._lock_provider_directory_artifact_tables(
        "mrf",
        ("stage", "target", "view", "missing"),
    )
    assert status.await_count == 2

    assert_logged = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_logged_relation",
        assert_logged,
    )
    await importer._prepare_provider_directory_artifact_stage("mrf", "stage")
    assert "SET LOGGED" in status.await_args_list[-1].args[0]
    assert_logged.assert_awaited_once_with("mrf", "stage")

    stage = _artifact_stage()
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=False))
    with pytest.raises(importer.ProviderDirectoryArtifactCutoverConflict):
        await importer._acquire_provider_directory_artifact_cutover_lock(stage)
    importer.db.scalar.return_value = True
    await importer._acquire_provider_directory_artifact_cutover_lock(stage)

    relation_oid = AsyncMock(return_value=11)
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_oid",
        relation_oid,
    )
    await importer._assert_provider_directory_artifact_build_fence(stage)
    relation_oid.assert_not_awaited()
    fenced_stage = _artifact_stage(target_oid=11, fenced=True)
    await importer._assert_provider_directory_artifact_build_fence(fenced_stage)
    relation_oid.return_value = 12
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        await importer._assert_provider_directory_artifact_build_fence(
            fenced_stage
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_kind", "statement_count"),
    [("r", 3), ("p", 3), ("v", 3), ("m", 3), (None, 2)],
)
async def test_artifact_stage_install_supports_expected_relation_kinds(
    monkeypatch,
    target_kind,
    statement_count,
):
    """Install over tables, views, materialized views, or an absent target."""
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_attribute",
        AsyncMock(return_value=target_kind),
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    await importer._install_provider_directory_prepared_stage(_artifact_stage())
    assert status.await_count == statement_count
    assert "RENAME TO" in status.await_args_list[-1].args[0]


@pytest.mark.asyncio
async def test_artifact_stage_install_and_finish_fail_closed(monkeypatch):
    """Reject unsupported target kinds and verify the installed table."""
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_attribute",
        AsyncMock(return_value="x"),
    )
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    with pytest.raises(RuntimeError, match="Unsupported Provider Directory"):
        await importer._install_provider_directory_prepared_stage(
            _artifact_stage()
        )

    rename_indexes = AsyncMock()
    assert_logged = AsyncMock()
    stage = importer.replace(
        _artifact_stage(),
        rename_stage_indexes=rename_indexes,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_logged_relation",
        assert_logged,
    )
    await importer._finish_provider_directory_prepared_stage(stage)
    rename_indexes.assert_awaited_once_with("mrf", "profile_stage")
    assert_logged.assert_awaited_once_with(
        "mrf",
        "provider_directory_profile",
    )


@pytest.mark.asyncio
async def test_artifact_dataset_promotion_orders_pointer_changes(monkeypatch):
    """Supersede, publish, and then cut over source aliases in that order."""
    dataset = _promotion_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    supersede = AsyncMock()
    publish = AsyncMock()
    cutover = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_supersede_artifact_dataset_incumbent",
        supersede,
    )
    monkeypatch.setattr(
        importer,
        "_publish_validated_artifact_dataset",
        publish,
    )
    monkeypatch.setattr(
        importer,
        "_cutover_provider_directory_artifact_sources",
        cutover,
    )
    await importer._promote_provider_directory_artifact_datasets(fence)
    supersede.assert_awaited_once_with(dataset)
    publish.assert_awaited_once_with(dataset)
    cutover.assert_awaited_once_with(fence)


@pytest.mark.asyncio
async def test_artifact_source_alias_cutover_is_exact(monkeypatch):
    """Move only changed promotion aliases and require one matching row."""
    ordinary = _promotion_dataset(
        source_id="source-ordinary",
        promote_on_cutover=False,
    )
    unchanged = _promotion_dataset(
        source_id="source-unchanged",
        serving_endpoint_id="endpoint-new",
    )
    changed = _promotion_dataset(source_id="source-changed")
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (ordinary, unchanged, changed)
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    await importer._cutover_provider_directory_artifact_sources(fence)
    status.assert_awaited_once()
    assert status.await_args.kwargs["source_id"] == "source-changed"

    status.return_value = 0
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_endpoint_dataset_changed",
    ):
        await importer._cutover_provider_directory_artifact_sources(
            importer.ProviderDirectoryArtifactDatasetFence((changed,))
        )


@pytest.mark.asyncio
async def test_artifact_dataset_row_transitions_require_exact_matches(
    monkeypatch,
):
    """Require one incumbent and one validated candidate row at cutover."""
    no_incumbent = _promotion_dataset(expected_incumbent_dataset_id=None)
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    await importer._supersede_artifact_dataset_incumbent(no_incumbent)
    status.assert_not_awaited()

    dataset = _promotion_dataset()
    await importer._supersede_artifact_dataset_incumbent(dataset)
    await importer._publish_validated_artifact_dataset(dataset)
    assert status.await_count == 2

    status.return_value = 0
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_changed",
    ):
        await importer._supersede_artifact_dataset_incumbent(dataset)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="candidate_changed",
    ):
        await importer._publish_validated_artifact_dataset(dataset)


def test_artifact_bundle_ordering_rejects_schema_and_target_ambiguity():
    """Sort one-schema bundles and reject ambiguous publication targets."""
    stage_b = _artifact_stage(
        stage_table="stage-b",
        target_relation="target-b",
    )
    stage_a = _artifact_stage(
        stage_table="stage-a",
        target_relation="target-a",
    )
    assert importer._ordered_provider_directory_artifact_bundle(
        (stage_b, stage_a)
    ) == (stage_a, stage_b)
    with pytest.raises(ValueError, match="schema_mismatch"):
        importer._ordered_provider_directory_artifact_bundle(
            (stage_a, importer.replace(stage_b, schema="other"))
        )
    with pytest.raises(ValueError, match="target_duplicate"):
        importer._ordered_provider_directory_artifact_bundle(
            (
                stage_a,
                importer.replace(stage_b, target_relation="target-a"),
            )
        )


@pytest.mark.asyncio
async def test_artifact_bundle_transaction_runs_all_fenced_steps(monkeypatch):
    """Execute ordered swaps and dataset publication in one transaction."""
    @contextlib.asynccontextmanager
    async def transaction():
        yield

    stage_b = _artifact_stage(
        stage_table="stage-b",
        target_relation="target-b",
    )
    stage_a = _artifact_stage(
        stage_table="stage-a",
        target_relation="target-a",
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_promotion_dataset(),)
    )
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    helper_names = (
        "_acquire_provider_directory_artifact_cutover_lock",
        "_lock_and_verify_artifact_dataset_fence",
        "_assert_provider_directory_artifact_build_fence",
        "_lock_provider_directory_artifact_tables",
        "_install_provider_directory_prepared_stage",
        "_finish_provider_directory_prepared_stage",
        "_promote_provider_directory_artifact_datasets",
    )
    helpers_by_name = {name: AsyncMock() for name in helper_names}
    for name, helper in helpers_by_name.items():
        monkeypatch.setattr(importer, name, helper)
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        fence
    )
    try:
        await importer._promote_provider_directory_artifact_bundle_transaction(
            (stage_b, stage_a)
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)

    assert helpers_by_name[
        "_acquire_provider_directory_artifact_cutover_lock"
    ].await_count == 2
    helpers_by_name[
        "_lock_and_verify_artifact_dataset_fence"
    ].assert_awaited_once_with(fence)
    helpers_by_name[
        "_promote_provider_directory_artifact_datasets"
    ].assert_awaited_once_with(fence)
    await importer._promote_provider_directory_artifact_bundle_transaction(())


@pytest.mark.asyncio
async def test_artifact_bundle_timeout_recovers_only_verified_commit(
    monkeypatch,
):
    """Treat timeout as success only when exact stage identities moved."""
    stage = _artifact_stage()
    identities = (
        importer.ProviderDirectoryArtifactPromotionIdentity(stage, 11),
    )
    monkeypatch.setattr(
        importer,
        "_capture_provider_directory_artifact_promotion_identities",
        AsyncMock(return_value=identities),
    )
    monkeypatch.setattr(
        importer,
        "_promote_provider_directory_artifact_bundle_transaction",
        AsyncMock(side_effect=TimeoutError),
    )
    committed = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_artifact_promotion_committed",
        committed,
    )
    await importer._promote_provider_directory_artifact_bundle((stage,))
    with pytest.raises(TimeoutError):
        await importer._promote_provider_directory_artifact_bundle((stage,))


@pytest.mark.asyncio
async def test_artifact_bundle_retry_stops_on_cause_or_exhaustion(monkeypatch):
    """Retry transient lock conflicts and propagate all terminal failures."""
    stage = _artifact_stage()
    promote = AsyncMock(
        side_effect=[
            importer.ProviderDirectoryArtifactCutoverConflict("profile"),
            None,
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(
        importer,
        "PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_ATTEMPTS",
        2,
    )
    monkeypatch.setattr(
        importer,
        "_promote_provider_directory_artifact_bundle",
        promote,
    )
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    await importer._retry_provider_directory_artifact_bundle_promotion(
        (stage,)
    )
    assert promote.await_count == 2
    sleep.assert_awaited_once()

    promote.reset_mock(side_effect=True)
    promote.side_effect = RuntimeError("terminal")
    with pytest.raises(RuntimeError, match="terminal"):
        await importer._retry_provider_directory_artifact_bundle_promotion(
            (stage,)
        )
    promote.reset_mock(side_effect=True)
    promote.side_effect = importer.ProviderDirectoryArtifactCutoverConflict(
        "profile"
    )
    with pytest.raises(importer.ProviderDirectoryArtifactCutoverConflict):
        await importer._retry_provider_directory_artifact_bundle_promotion(
            (stage,)
        )


@pytest.mark.asyncio
async def test_single_artifact_timeout_recovers_only_verified_commit(
    monkeypatch,
):
    """Resolve a one-stage timeout with the same exact-identity contract."""
    stage = _artifact_stage()
    identities = (
        importer.ProviderDirectoryArtifactPromotionIdentity(stage, 11),
    )
    monkeypatch.setattr(
        importer,
        "_capture_provider_directory_artifact_promotion_identities",
        AsyncMock(return_value=identities),
    )
    monkeypatch.setattr(
        importer,
        "_promote_provider_directory_artifact_stage_transaction",
        AsyncMock(side_effect=TimeoutError),
    )
    committed = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_artifact_promotion_committed",
        committed,
    )
    await importer._promote_provider_directory_artifact_stage(
        stage.schema,
        stage.stage_table,
        stage.target_relation,
        stage.rename_stage_indexes,
        stage.build_fence,
    )
    with pytest.raises(TimeoutError):
        await importer._promote_provider_directory_artifact_stage(
            stage.schema,
            stage.stage_table,
            stage.target_relation,
            stage.rename_stage_indexes,
            stage.build_fence,
        )


def test_compact_linked_references_drop_empty_ids_and_values():
    """Retain only usable resource IDs and linked reference values."""
    rows = [
        {"resource_id": None},
        {
            "resource_id": "role-a",
            "network_refs": [None, "Organization/a"],
            "location_refs": [None],
        },
        {
            "resource_id": "role-b",
            "practitioner_ref": "Practitioner/b",
        },
    ]
    assert importer._compact_linked_reference_rows(
        "PractitionerRole",
        rows,
    ) == [
        {
            "resource_id": "role-a",
            "network_refs": ["Organization/a"],
        },
        {
            "resource_id": "role-b",
            "practitioner_ref": "Practitioner/b",
        },
    ]


def test_credentials_config_ignores_invalid_shapes_and_merges_objects(
    tmp_path,
    monkeypatch,
):
    """Load only JSON objects from the private file and environment overlay."""
    file_path = _write_json(tmp_path / "credentials.json", ["not-an-object"])
    config_token = importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(
        str(file_path)
    )
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        "[]",
    )
    try:
        assert importer._load_credentials_config() == {}
        file_path.write_text("{broken", encoding="utf-8")
        monkeypatch.setenv(
            importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
            "{broken",
        )
        assert importer._load_credentials_config() == {}
        _write_json(file_path, {"defaults": {"headers": {"X-A": "a"}}})
        monkeypatch.setenv(
            importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
            '{"sources":{"aetna":{"api_key":"secret"}}}',
        )
        assert importer._load_credentials_config() == {
            "defaults": {"headers": {"X-A": "a"}},
            "sources": {"aetna": {"api_key": "secret"}},
        }
    finally:
        importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.reset(
            config_token
        )


def test_credential_rules_merge_all_scopes_and_allow_relative_urls(
    monkeypatch,
):
    """Apply defaults through source overrides and honor explicit disable."""
    config_by_name = {
        "defaults": {"headers": {"X-Default": "default"}},
        "hosts": {"example.test": {"query": {"host": "yes"}}},
        "api_bases": {
            "https://example.test/fhir": {"headers": {"X-Base": "base"}}
        },
        "org_names": {"Example Org": {"query_params": {"org": "yes"}}},
        "sources": {"source-a": {"api_key": "source-secret"}},
    }
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        Mock(return_value=config_by_name),
    )
    source_by_field = {
        "source_id": "source-a",
        "api_base": "https://example.test/fhir",
        "org_name": "EXAMPLE ORG",
    }
    spec = importer._credential_spec_for_source(source_by_field)
    assert spec["headers"] == {
        "X-Default": "default",
        "X-Base": "base",
    }
    assert spec["query_params"] == {"host": "yes", "org": "yes"}
    assert spec["api_key"] == "source-secret"
    assert importer._is_credential_allowed_for_url(
        source_by_field,
        "Practitioner",
    )

    config_by_name["sources"]["source-a"] = {"enabled": False}
    assert importer._credential_spec_for_source(source_by_field) == {}
    assert importer._source_hosts({"api_base": "Practitioner"}) == set()


def test_aetna_credential_base_candidates_keep_exact_partition_rules():
    """Do not bleed a custom Aetna key across separately configured bases."""
    exact_rules_by_base = {
        importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE: {},
        importer.AETNA_PROVIDER_DIRECTORY_BASE: {},
    }
    assert importer._credential_api_base_candidates(
        importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        {},
        exact_rules_by_base,
    ) == [importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE]
    assert importer._credential_api_base_candidates(
        importer.AETNA_PROVIDER_DIRECTORY_BASE,
        {},
        {},
    ) == [
        importer.AETNA_PROVIDER_DIRECTORY_BASE,
        importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
    ]


def test_oauth_credentials_fail_closed_and_cache_valid_token(monkeypatch):
    """Reject incomplete token responses and cache one valid body-auth token."""
    importer._OAUTH_TOKEN_CACHE.clear()
    assert (
        importer._fetch_oauth2_client_credentials_token_sync(
            {"token_url": "https://auth.test/token"}
        )
        is None
    )
    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = b"{}"
    monkeypatch.setattr(importer.urllib.request, "urlopen", Mock(return_value=response))
    decoded = Mock(
        side_effect=[
            {},
            {"expires_in": 60},
            {"access_token": "token-a", "expires_in": "invalid"},
        ]
    )
    monkeypatch.setattr(importer, "_decode_json_body", decoded)
    oauth_by_field = {
        "token_url": "https://auth.test/token",
        "client_id": "client-a",
        "client_secret": "secret-a",
        "auth": "body",
        "scope": "scope-a",
        "extra_params": {"audience": "aud-a", "empty": ""},
    }
    assert (
        importer._fetch_oauth2_client_credentials_token_sync(oauth_by_field)
        is None
    )
    assert (
        importer._fetch_oauth2_client_credentials_token_sync(oauth_by_field)
        is None
    )
    assert (
        importer._fetch_oauth2_client_credentials_token_sync(oauth_by_field)
        == "token-a"
    )
    assert (
        importer._fetch_oauth2_client_credentials_token_sync(oauth_by_field)
        == "token-a"
    )
    importer._OAUTH_TOKEN_CACHE.clear()


def test_credential_request_options_resolve_oauth_and_drop_empty_values(
    monkeypatch,
):
    """Expose credential names while excluding empty header/query secrets."""
    specs = [
        {
            "oauth2": {
                "token_url": "https://auth.test/token",
                "client_id": "client-a",
                "client_secret": "secret-a",
            },
            "headers": {"X-Empty": ""},
            "query_params": {"empty": ""},
            "_matched_by": ["sources:source-a"],
        },
        {"enabled": True},
    ]
    monkeypatch.setattr(
        importer,
        "_credential_spec_for_source",
        Mock(side_effect=specs),
    )
    monkeypatch.setattr(
        importer,
        "_is_credential_allowed_for_url",
        Mock(return_value=True),
    )
    monkeypatch.setattr(
        importer,
        "_fetch_oauth2_client_credentials_token_sync",
        Mock(return_value="token-a"),
    )
    source_by_field = {
        "source_id": "source-a",
        "api_base": "https://api.test/fhir",
    }
    options = importer._credential_request_options_for_source(
        source_by_field,
        "https://api.test/fhir/Practitioner",
    )
    assert options == {
        "headers": {"Authorization": "Bearer token-a"},
        "query_params": {},
        "descriptor": {
            "matched_by": ["sources:source-a"],
            "header_names": ["Authorization"],
            "query_param_names": [],
        },
    }
    assert importer._credential_request_options_for_source(
        source_by_field,
        "https://api.test/fhir/Practitioner",
    )["descriptor"] is None


def test_profile_identity_helpers_handle_malformed_fhir_values():
    """Extract stable IDs while ignoring malformed or empty FHIR fields."""
    assert importer._identifier_descriptor(
        {"type": {"coding": [None, {"code": "NPI"}]}}
    ).endswith("npi")
    resource_by_field = {
        "identifier": [
            None,
            {"value": ""},
            {"value": "systemless"},
        ]
    }
    assert importer._identifier_value(
        resource_by_field,
        "missing",
        allow_systemless=True,
    ) == "systemless"
    assert importer._tin({}) is None
    assert importer._string_list("one") == ["one"]
    assert importer._string_list({"one": 1}) == []
    assert importer._attach_location_contact_fields([]) == []
    assert importer._name({"name": {"family": "Smith"}}) == (
        "Smith",
        [],
        "Smith",
    )
    assert importer._name({"name": [None]}) == (None, [], None)


def test_profile_npi_and_qualification_precedence_ignores_bad_entries():
    """Prefer recognized NPIs and skip malformed qualification entries."""
    resource_by_field = {
        "identifier": [
            None,
            {"system": "other-npi", "value": "1234567890"},
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "0987654321",
            },
            {"system": "other-npi", "value": "1111111111"},
        ]
    }
    assert importer._npi(resource_by_field) == 987654321
    years = importer._derived_years_of_practice(
        [
            None,
            {"period": {"start": "2010-01-01"}},
        ],
        as_of=importer.datetime.date(2026, 7, 20),
    )
    assert years[0] == 16


@pytest.mark.asyncio
async def test_profile_checkpoint_reuse_requires_both_logged_stages(
    monkeypatch,
):
    """Reject an otherwise exact checkpoint when either stage is unlogged."""
    build = _profile_build()
    checkpoint = _profile_checkpoint_by_name(build)
    stage_identity = AsyncMock(
        side_effect=[(11, "r", "p"), (12, "r", "u")]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        stage_identity,
    )
    assert not await importer._is_profile_build_checkpoint_reusable(
        build,
        checkpoint,
        has_existing_artifacts=False,
        evidence_build_fence=importer.ProviderDirectoryArtifactBuildFence(None),
        profile_build_fence=importer.ProviderDirectoryArtifactBuildFence(None),
        evidence_total_batches=52,
        profile_total_batches=400,
    )


@pytest.mark.asyncio
async def test_stale_profile_reaper_handles_disappeared_rows_and_lost_delete(
    monkeypatch,
):
    """Skip concurrent disappearance and fail if final checkpoint delete loses."""
    @contextlib.asynccontextmanager
    async def transaction():
        yield

    build_id = f"pdpb_{'a' * 32}"
    checkpoint_by_name = {
        "build_id": build_id,
        "evidence_stage": profile.profile_evidence_stage_table_name(build_id),
        "profile_stage": profile.profile_stage_table_name(build_id),
        "evidence_stage_oid": 21,
        "profile_stage_oid": 22,
    }
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[checkpoint_by_name]),
    )
    first = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first)
    assert await importer._reap_stale_provider_directory_profile_builds(
        "mrf",
        current_build_id=f"pdpb_{'b' * 32}",
    ) == 0

    first.return_value = checkpoint_by_name
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(side_effect=[None, None]),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="stale_checkpoint_delete_lost"):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=f"pdpb_{'b' * 32}",
        )


@pytest.mark.asyncio
async def test_profile_unbounded_and_completed_batch_paths(monkeypatch):
    """Cover full-build copies and already-complete bounded resumes."""
    build = _profile_build()
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_profile_indexes",
        AsyncMock(),
    )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
    )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=False,
    )
    evidence_batches = importer._provider_directory_profile_evidence_batches(
        build,
        has_existing_artifacts=True,
    )
    await importer._populate_provider_directory_profile_evidence_stage(
        build,
        has_evidence_target=True,
        bounded=True,
        start_batch=len(evidence_batches),
    )
    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=False,
    )
    compact_batches = importer._provider_directory_profile_compact_batches(
        has_existing_artifacts=True,
        npi_batch_size=profile.PROFILE_NPI_BATCH_SIZE,
    )
    await importer._populate_provider_directory_profile_compact_stage(
        build,
        has_existing_artifacts=True,
        npi_batch_size=profile.PROFILE_NPI_BATCH_SIZE,
        start_batch=len(compact_batches),
    )


@pytest.mark.asyncio
async def test_single_stage_transaction_and_unfenced_bundle_paths(monkeypatch):
    """Exercise active dataset fencing and the ordinary bundle branch."""
    @contextlib.asynccontextmanager
    async def transaction():
        yield

    stage = _artifact_stage()
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (_promotion_dataset(),)
    )
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    helper_names = (
        "_acquire_provider_directory_artifact_cutover_lock",
        "_lock_and_verify_artifact_dataset_fence",
        "_assert_provider_directory_artifact_build_fence",
        "_lock_provider_directory_artifact_tables",
        "_install_provider_directory_prepared_stage",
        "_finish_provider_directory_prepared_stage",
        "_promote_provider_directory_artifact_datasets",
    )
    helpers_by_name = {name: AsyncMock() for name in helper_names}
    for name, helper in helpers_by_name.items():
        monkeypatch.setattr(importer, name, helper)
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        fence
    )
    try:
        await importer._promote_provider_directory_artifact_stage_transaction(
            stage.schema,
            stage.stage_table,
            stage.target_relation,
            stage.rename_stage_indexes,
            stage.build_fence,
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
    helpers_by_name[
        "_lock_and_verify_artifact_dataset_fence"
    ].assert_awaited_once()
    helpers_by_name[
        "_promote_provider_directory_artifact_datasets"
    ].assert_awaited_once()
    await importer._promote_provider_directory_artifact_bundle_transaction(
        (stage,)
    )


@pytest.mark.asyncio
async def test_artifact_cleanup_and_zero_attempt_configuration(monkeypatch):
    """Drop ordinary stages and make zero configured attempts a no-op."""
    stage = _artifact_stage()
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    await importer._remove_provider_directory_artifact_stage(stage)
    status.assert_awaited_once()
    monkeypatch.setattr(
        importer,
        "PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_ATTEMPTS",
        0,
    )
    await importer._retry_provider_directory_artifact_bundle_promotion(
        (stage,)
    )
    await importer._retry_provider_directory_artifact_promotion(
        stage.schema,
        stage.stage_table,
        stage.target_relation,
        stage.rename_stage_indexes,
        stage.build_fence,
    )


@pytest.mark.asyncio
async def test_artifact_bundle_and_index_noop_paths(monkeypatch):
    """Ignore absent stages and avoid renaming already-serving indexes."""
    stage = _artifact_stage()
    bundle = importer.ProviderDirectoryArtifactBundle()
    bundle.add(None)
    bundle.add(stage)
    assert bundle.stages == [stage]
    target_index = importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_INDEXES[0]
    assert importer._address_corroboration_index_name(
        importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
        target_index,
    ) == target_index
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    await importer._rename_address_corroboration_stage_indexes(
        "mrf",
        importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
    )
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_build_lock_loss_and_connection_failure(monkeypatch):
    """Close failed lock attempts and report a lost advisory unlock."""
    failed_connection = SimpleNamespace(
        execute=AsyncMock(side_effect=RuntimeError("lock query failed")),
        close=AsyncMock(),
    )
    engine = SimpleNamespace(connect=AsyncMock(return_value=failed_connection))
    with pytest.raises(RuntimeError, match="lock query failed"):
        await importer._try_provider_directory_artifact_build_lock(
            engine,
            "build-key",
        )
    failed_connection.close.assert_awaited_once()

    unlock_result = SimpleNamespace(scalar=Mock(return_value=False))
    connection = SimpleNamespace(
        execute=AsyncMock(return_value=unlock_result),
        commit=AsyncMock(),
        close=AsyncMock(),
    )
    await importer._release_provider_directory_artifact_build_lock(
        connection,
        "build-key",
    )
    connection.commit.assert_awaited_once()
    connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_artifact_build_guard_connects_and_releases(monkeypatch):
    """Initialize the engine once and release the dedicated build lock."""
    engine = SimpleNamespace()

    async def connect():
        importer.db.engine = engine

    connection = SimpleNamespace()
    monkeypatch.setattr(importer.db, "engine", None)
    monkeypatch.setattr(importer.db, "connect", connect)
    monkeypatch.setattr(
        importer,
        "_acquire_provider_directory_artifact_build_lock",
        AsyncMock(return_value=connection),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_relation_oid",
        AsyncMock(return_value=17),
    )
    release = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_release_provider_directory_artifact_build_lock",
        release,
    )
    async with importer._provider_directory_artifact_build_guard(
        "mrf",
        "profile",
    ) as fence:
        assert fence.target_oid == 17
    release.assert_awaited_once_with(
        connection,
        "provider-directory-artifact-build:mrf.profile",
    )


def test_source_selection_records_probe_and_open_policy_skips(monkeypatch):
    """Distinguish failed live probes from the credentialed open-only policy."""
    monkeypatch.setattr(
        importer,
        "_resource_acquisition_blocked_reason",
        Mock(return_value=None),
    )
    source_by_field = {
        "source_id": "source-a",
        "api_base": "https://api.test/fhir",
        "auth_type": "oauth2",
        "last_validated_status": "valid",
    }
    selected, metrics = importer._select_resource_import_sources(
        [source_by_field],
        valid_source_ids={"source-b"},
        open_only=False,
        include_auth_required=True,
    )
    assert selected == []
    assert metrics["source_import_skipped_probe_not_valid"] == 1
    selected, metrics = importer._select_resource_import_sources(
        [source_by_field],
        valid_source_ids=None,
        open_only=True,
        include_auth_required=True,
    )
    assert selected == []
    assert metrics["source_import_skipped_open_only"] == 1


@pytest.mark.asyncio
async def test_profile_finalization_without_checkpoint_still_cleans_stages(
    monkeypatch,
):
    """Promote disposable stages without attempting checkpoint deletion."""
    stages = (
        _artifact_stage(stage_table="evidence-stage", target_relation="evidence"),
        _artifact_stage(stage_table="profile-stage", target_relation="profile"),
    )
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        AsyncMock(),
    )
    delete_checkpoint = AsyncMock()
    remove = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_delete_provider_directory_profile_build_checkpoint",
        delete_checkpoint,
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove,
    )
    assert await importer._finalize_provider_directory_profile_stages(
        {"profile_rows": 1},
        stages,
        defer_cutover=False,
    ) == {"profile_rows": 1}
    delete_checkpoint.assert_not_awaited()
    assert remove.await_count == 2


def test_profile_tin_truncates_matching_identifier():
    """Bound tax identifiers before storing Profile evidence."""
    assert importer._tin(
        {
            "identifier": [
                {
                    "system": "https://example.test/tin",
                    "value": "1" * 80,
                }
            ]
        }
    ) == "1" * 64
