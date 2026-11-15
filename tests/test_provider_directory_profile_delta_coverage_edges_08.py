# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Narrow edge proofs for bounded Provider Directory Profile deltas."""

from __future__ import annotations

from tests.provider_directory_profile_delta_coverage_support import (
    AsyncMock,
    Mock,
    SimpleNamespace,
    _execution,
    _matching_delta_receipt,
    _matching_delta_serving_state,
    _prepared_delta,
    _ready_delta_checkpoint,
    _valid_serving_row,
    _wal_tracker_admission,
    contextlib,
    dataclasses,
    datetime,
    importer,
    json,
    profile,
    pytest,
)
from tests.test_provider_directory_profile_delta_coverage_edges_04 import _logical_capacity_layout


def _stage_storage_build():
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=(),
        retained_source_ids=(),
        dataset_ids=(),
        profile_as_of="2026-07-30",
        evidence_stage="evidence",
        profile_stage="profile",
        owner_run_id="run-a",
        resume_lineage_hash="a" * 64,
    )
    plan = importer._provider_directory_profile_build_plan(build, has_existing_artifacts=False)
    return dataclasses.replace(build, batch_plan=plan)


def _named_stage_checkpoint(build):
    """Use the real frozen plan and complete source/geometry/target checkpoint preimage."""
    return {
        **importer._profile_checkpoint_relation_params(build),
        "has_existing_artifacts": False,
        "materialization_mode": build.materialization_mode,
        "evidence_total_batches": len(build.batch_plan.evidence_batches),
        "profile_total_batches": len(build.batch_plan.compact_batches),
        "refresh_source_ids": [],
        "removed_source_ids": [],
    }


@pytest.mark.asyncio
async def test_capacity_consumption_insert_and_replay_edges(monkeypatch):
    values_by_name = {
        "attestation_id": "a",
        "reservation_id": "r",
        "lease_digest": "l",
        "capacity_geometry_hash": "g",
        "executable_plan_hash": "e",
        "selection_proof_id": "p",
        "source_vector_hash": "s",
        "source_context_vector_hash": "c",
        "run_id": "run",
        "build_id": "build",
        "profile_as_of": "2026-07-30",
        "contract_id": "contract",
        "key_id": "key",
        "environment_id": "dev",
        "attestor_id": "attestor",
        "attestor_release_digest": "release",
        "public_key_fingerprint": "fingerprint",
        "canonical_lease_json": "{}",
        "signature": "signature",
        "observed_at": "observed",
        "issued_at": "issued",
        "accepted_at": "accepted",
        "expires_at": "expires",
        "max_build_deadline": "deadline",
        "recorded_at": "recorded",
    }
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="INSERT 0 1"))
    await importer._consume_provider_directory_profile_capacity_lease(
        schema="mrf",
        values_by_name=values_by_name,
    )
    assert importer.db.status.await_args.kwargs == {"admission_purpose": "profile", **values_by_name}
    importer.db.status.return_value = "INSERT 0 0"
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[values_by_name]),
    )
    await importer._consume_provider_directory_profile_capacity_lease(
        schema="mrf",
        values_by_name=values_by_name,
    )
    importer.db.all.return_value = []
    with pytest.raises(RuntimeError, match="already_consumed"):
        await importer._consume_provider_directory_profile_capacity_lease(
            schema="mrf",
            values_by_name=values_by_name,
        )


@pytest.mark.asyncio
async def test_stage_storage_checkpoint_scope_and_missing_edges(monkeypatch):
    build = _stage_storage_build()
    query = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", query)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="storage_scope_invalid"):
        await importer._assert_profile_stage_storage(build, stage_table="other", stage_field="profile_stage")
    query.assert_not_awaited()
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="checkpoint_missing"):
        await importer._assert_profile_stage_storage(build, stage_table="profile", stage_field="profile_stage")
    query.return_value = {**_named_stage_checkpoint(build), "profile_stage": "different"}
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="checkpoint_invalid"):
        await importer._assert_profile_stage_storage(build, stage_table="profile", stage_field="profile_stage")


@pytest.mark.asyncio
async def test_stage_storage_complete_checkpoint_and_semantic_cas(monkeypatch):
    """A real valid checkpoint reaches the named layout; changed durable semantics refuse first."""
    build = _stage_storage_build()
    checkpoint_by_field = _named_stage_checkpoint(build)
    query = AsyncMock(return_value=checkpoint_by_field)
    capture = AsyncMock()
    monkeypatch.setattr(importer.db, "first", query)
    monkeypatch.setattr(importer, "_provider_directory_profile_stage_layout", capture)
    await importer._assert_profile_stage_storage(build, stage_table="profile", stage_field="profile_stage")
    capture.assert_awaited_once_with("mrf", "profile", lock_relation=True)
    assert query.await_args.kwargs["owner_run_id"] == build.owner_run_id
    assert "FOR SHARE" in query.await_args.args[0]
    capture.reset_mock()
    for field, changed in (
        ("resume_lineage_hash", "changed"),
        ("executable_plan_hash", "changed"),
        ("desired_source_vector_hash", "changed"),
        ("targets_json", json.dumps({"evidence": {"schema": "other", "name": "evidence"}})),
        ("has_existing_artifacts", None),
    ):
        query.return_value = {**checkpoint_by_field, field: changed}
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="checkpoint_invalid"):
            await importer._assert_profile_stage_storage(build, stage_table="profile", stage_field="profile_stage")
    query.return_value = {**checkpoint_by_field, "has_existing_artifacts": True}
    with pytest.raises(RuntimeError, match="artifact_presence_changed"):
        await importer._assert_profile_stage_storage(build, stage_table="profile", stage_field="profile_stage")
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_profile_named_stage_layout_and_lock_edges(monkeypatch):
    """Logged table shape is required; fresh catalog operands may change without changing the name."""
    identities = AsyncMock(return_value=None)
    monkeypatch.setattr(importer, "_provider_directory_profile_stage_relation_identity", identities)
    capture = AsyncMock(return_value=object())
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_relation_layout", capture)
    for invalid in (None, (12, "v", "p"), (12, "r", "u")):
        identities.return_value = invalid
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="storage_shape_invalid"):
            await importer._provider_directory_profile_stage_layout("mrf", "stage", lock_relation=False)
    capture.assert_not_awaited()
    identities.side_effect = [(12, "r", "p"), (13, "r", "p")]
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    expected = capture.return_value
    assert await importer._provider_directory_profile_stage_layout("mrf", "stage", lock_relation=False) is expected
    assert await importer._provider_directory_profile_stage_layout("mrf", "stage", lock_relation=True) is expected
    assert [call.args[0] for call in capture.await_args_list] == [12, 13]
    importer.db.status.assert_awaited_once_with('LOCK TABLE "mrf"."stage" IN ACCESS SHARE MODE;')


@pytest.mark.asyncio
async def test_delta_preparation_context_edges(monkeypatch):
    delta = _prepared_delta()
    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id=delta.generation_id,
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of=delta.profile_as_of,
        evidence_stage=delta.evidence_stage,
        profile_stage=delta.profile_stage,
        affected_npi_stage=None,
    )
    with pytest.raises(RuntimeError, match="affected_stage_missing"):
        await importer._profile_delta_preparation_context(build)

    build = dataclasses.replace(
        build,
        affected_npi_stage=delta.affected_npi_stage,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_logged_relation",
        AsyncMock(),
    )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="ready_checkpoint_missing"):
        await importer._profile_delta_preparation_context(build)

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value={}))
    with pytest.raises(RuntimeError, match="delta_identity_missing"):
        await importer._profile_delta_preparation_context(build)


@pytest.mark.asyncio
async def test_profile_delta_serving_lookup_edges(monkeypatch):
    delta = _prepared_delta()
    serving = _matching_delta_serving_state(delta)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_serving_state",
        AsyncMock(return_value=None),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_generation_missing",
    ):
        await importer._provider_directory_profile_delta_serving_state(
            "mrf",
            allow_adoption=False,
        )

    importer._provider_directory_profile_serving_state.return_value = serving
    assert (
        await importer._provider_directory_profile_delta_serving_state(
            "mrf",
            allow_adoption=False,
        )
        == serving
    )

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    importer._provider_directory_profile_serving_state.return_value = None
    monkeypatch.setattr(
        importer,
        "_adopt_provider_directory_profile_serving_generation",
        AsyncMock(return_value=serving),
    )
    assert await importer._provider_directory_profile_delta_serving_state("mrf") == serving


def test_profile_delta_source_edges():
    delta = _prepared_delta()
    serving = _matching_delta_serving_state(delta)
    incompatible = dataclasses.replace(
        serving,
        profile_strategy_version="unsupported",
    )
    with pytest.raises(RuntimeError, match="strategy_incompatible"):
        importer._provider_directory_profile_delta_sources(
            incompatible,
            serving.source_vector,
            serving.source_context_vector,
        )
    with pytest.raises(RuntimeError, match="context_vector_invalid"):
        importer._provider_directory_profile_delta_sources(
            serving,
            serving.source_vector,
            (),
        )
    refreshed_source_ids, removed_source_ids = importer._provider_directory_profile_delta_sources(
        serving,
        (("source-a", "changed"), ("source-b", "dataset-b")),
        (
            ("source-a", "a" * 64),
            ("source-b", "b" * 64),
        ),
    )
    assert refreshed_source_ids == ("source-a", "source-b")
    assert removed_source_ids == ()


@pytest.mark.asyncio
async def test_profile_checkpoint_named_layout_and_toast_bounds(monkeypatch):
    admission = _wal_tracker_admission()
    layout = _logical_capacity_layout(admission.geometry, "build_checkpoint")
    capture = AsyncMock(return_value=dataclasses.replace(layout, relation_name="other_checkpoint"))
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=12))
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_relation_layout", capture)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="layout_scope_changed"):
        await importer._validate_profile_checkpoint_layout(admission, '"mrf"."checkpoint"', "build")
    capture.return_value = layout
    maximum = admission.control_wal_projection.plan_input.build_checkpoint_update.deleted_toast_chunks
    chunks = AsyncMock(return_value=maximum + 1)
    monkeypatch.setattr(importer, "_provider_directory_profile_toast_chunk_count", chunks)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="checkpoint_toast_changed"):
        await importer._validate_profile_checkpoint_layout(admission, '"mrf"."checkpoint"', "build")
    chunks.return_value = maximum
    await importer._validate_profile_checkpoint_layout(admission, '"mrf"."checkpoint"', "build")
    assert chunks.await_args.kwargs["params"] == {"build_id": "build"}
    assert chunks.await_args.kwargs["relation_oid"] == layout.relation_oid
