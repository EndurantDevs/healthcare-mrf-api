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


def _stage_storage_build():
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=(),
        retained_source_ids=(),
        dataset_ids=(),
        profile_as_of="2026-07-30",
        evidence_stage="evidence",
        profile_stage="profile",
        owner_run_id="run-a",
    )


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
        "database_system_identifier": "system",
        "database_oid": 1,
        "database_name": "db",
        "tablespace_identity_hash": "tablespace",
        "volume_identity_hash": "volume",
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
async def test_stage_storage_checkpoint_identity_edges(monkeypatch):
    build = _stage_storage_build()
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="checkpoint_missing",
    ):
        await importer._assert_profile_stage_storage(
            build,
            stage_table="profile",
            oid_field="profile_stage_oid",
            fingerprint_field="profile_stage_storage_fingerprint",
        )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "profile_stage_oid": None,
                "profile_stage_storage_fingerprint": "bad",
            }
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="checkpoint_invalid",
    ):
        await importer._assert_profile_stage_storage(
            build,
            stage_table="profile",
            oid_field="profile_stage_oid",
            fingerprint_field="profile_stage_storage_fingerprint",
        )


@pytest.mark.asyncio
async def test_stage_storage_fingerprint_edges(monkeypatch):
    build = _stage_storage_build()
    fingerprint = "a" * 64
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "profile_stage_oid": 12,
                "profile_stage_storage_fingerprint": fingerprint,
            }
        ),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_storage_fingerprint",
        AsyncMock(return_value="b" * 64),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="layout_changed",
    ):
        await importer._assert_profile_stage_storage(
            build,
            stage_table="profile",
            oid_field="profile_stage_oid",
            fingerprint_field="profile_stage_storage_fingerprint",
        )

    importer._provider_directory_profile_stage_storage_fingerprint.return_value = (
        fingerprint
    )
    await importer._assert_profile_stage_storage(
        build,
        stage_table="profile",
        oid_field="profile_stage_oid",
        fingerprint_field="profile_stage_storage_fingerprint",
    )

@pytest.mark.asyncio
async def test_profile_stage_fingerprint_edges(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=(99, "r", "p")),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="storage_identity_changed",
    ):
        await importer._provider_directory_profile_stage_storage_fingerprint(
            "mrf",
            "stage",
            expected_oid=12,
            lock_relation=False,
        )

    identities = AsyncMock(
        side_effect=[(12, "r", "p"), (13, "r", "p")]
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        identities,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="storage_identity_changed",
    ):
        await importer._provider_directory_profile_stage_storage_fingerprint(
            "mrf",
            "stage",
            expected_oid=12,
            lock_relation=True,
        )

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
    assert await importer._provider_directory_profile_delta_serving_state(
        "mrf",
        allow_adoption=False,
    ) == serving

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
    assert await importer._provider_directory_profile_delta_serving_state(
        "mrf"
    ) == serving


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
    refreshed_source_ids, removed_source_ids = (
        importer._provider_directory_profile_delta_sources(
        serving,
        (("source-a", "changed"), ("source-b", "dataset-b")),
        (
            ("source-a", "a" * 64),
            ("source-b", "b" * 64),
        ),
        )
    )
    assert refreshed_source_ids == ("source-a", "source-b")
    assert removed_source_ids == ()

@pytest.mark.asyncio
async def test_profile_checkpoint_layout_edges(monkeypatch):
    admission = _wal_tracker_admission()
    layout = SimpleNamespace(
        exact_fingerprint="changed",
        relation_oid=12,
        toast_oid=None,
        toastable_columns=(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(return_value=layout),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="checkpoint_layout_changed",
    ):
        await importer._validate_profile_checkpoint_layout(
            admission,
            '"mrf"."checkpoint"',
            "build",
        )

    layout.exact_fingerprint = (
        admission.geometry.build_checkpoint_storage_fingerprint
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_toast_chunk_count",
        AsyncMock(
            return_value=(
                admission.control_wal_projection.plan_input
                .build_checkpoint_update.deleted_toast_chunks
                + 1
            )
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="checkpoint_toast_changed",
    ):
        await importer._validate_profile_checkpoint_layout(
            admission,
            '"mrf"."checkpoint"',
            "build",
        )
