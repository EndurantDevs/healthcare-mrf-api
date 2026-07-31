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


def _profile_build():
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-30",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
    )


@pytest.mark.asyncio
async def test_profile_preflight_empty_window_edges(monkeypatch):
    build = _profile_build()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    assert await importer._preflight_profile_evidence_window_capacity(
        build,
        [],
        {},
    ) == {}
    assert await importer._preflight_profile_compact_window_capacity(
        build,
        [],
        profile_count_sql=Mock(),
        profile_sql_args_by_name={},
        profile_params_by_name={},
    ) == {}


@pytest.mark.asyncio
async def test_profile_preflight_evidence_window_edges(monkeypatch):
    build = _profile_build()
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    with pytest.raises(RuntimeError, match="copy_batch_unsupported"):
        await importer._preflight_profile_evidence_window_capacity(
            build,
            [
                (
                    0,
                    importer._ProviderDirectoryProfileEvidenceBatch(
                        kind="copy"
                    ),
                )
            ],
            {},
        )
    monkeypatch.setattr(
        importer,
        "_evidence_window_projections",
        AsyncMock(
            return_value={
                0: importer._ProviderDirectoryProfileEvidenceProjection(
                    projected_rows=admission.geometry.max_evidence_rows + 1,
                    projected_logical_bytes=8,
                )
            }
        ),
    )
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="evidence_rows_projected"):
        await importer._preflight_profile_evidence_window_capacity(
            build,
            [
                (
                    0,
                    importer._ProviderDirectoryProfileEvidenceBatch(
                        kind="fact"
                    ),
                )
            ],
            {},
        )


@pytest.mark.asyncio
async def test_profile_preflight_compact_window_edges(monkeypatch):
    build = _profile_build()
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        importer,
        "_compact_window_projections",
        AsyncMock(
            return_value={
                0: importer._ProviderDirectoryProfileEvidenceProjection(
                    projected_rows=admission.geometry.max_profile_rows + 1,
                    projected_logical_bytes=8,
                )
            }
        ),
    )
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="profile_rows_projected"):
        await importer._preflight_profile_compact_window_capacity(
            build,
            [
                (
                    0,
                    importer._ProviderDirectoryProfileCompactBatch(
                        kind="npi",
                        npi_start=profile.NPI_MIN,
                        npi_end=profile.NPI_MIN + 1,
                    ),
                )
            ],
            profile_count_sql=Mock(),
            profile_sql_args_by_name={},
            profile_params_by_name={},
        )


def test_replay_timestamp_edges():
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="timestamp_invalid",
    ):
        importer._provider_directory_profile_replay_timestamp(
            datetime.datetime(2026, 7, 30),
            "created_at",
        )
    aware = datetime.datetime(
        2026,
        7,
        30,
        tzinfo=datetime.timezone.utc,
    )
    assert importer._provider_directory_profile_replay_timestamp(
        aware,
        "created_at",
    ) == aware


def test_replay_count_edges():
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="counts_invalid",
    ):
        importer._provider_directory_profile_replay_counts({})
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="counts_invalid",
    ):
        importer._provider_directory_profile_replay_counts(
            {
                "evidence_rows": -1,
                "profile_rows": 0,
                "evidence_inserted": 0,
                "evidence_deleted": 0,
                "profile_inserted": 0,
                "profile_deleted": 0,
            }
        )
    assert importer._provider_directory_profile_replay_counts(
        {
            "evidence_rows": 1,
            "profile_rows": 1,
            "evidence_inserted": 1,
            "evidence_deleted": 0,
            "profile_inserted": 1,
            "profile_deleted": 0,
        }
    )["profile_rows"] == 1


def test_replay_geometry_edges():
    receipt = _matching_delta_receipt(_prepared_delta())
    receipt.update(
        {
            "capacity_geometry_status": "legacy_unavailable",
            "capacity_geometry_hash": None,
            "capacity_geometry_json": None,
        }
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="geometry_unverified",
    ):
        importer._provider_directory_profile_replay_geometry(
            receipt,
            _execution(),
        )


@pytest.mark.asyncio
async def test_replay_control_run_lookup_edges(monkeypatch):
    run_id = "run_" + "1" * 32
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="control_run_changed",
    ):
        await importer._replay_control_run("mrf", run_id)

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "run_id": run_id,
                "importer": "provider-directory-fhir",
                "status": "running",
            }
        ),
    )
    await importer._replay_control_run("mrf", run_id)


@pytest.mark.asyncio
async def test_replay_consumption_lookup_edges(monkeypatch):
    run_id = "run_" + "1" * 32
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[{}, {}]))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="consumption_ambiguous",
    ):
        await importer._replay_current_consumption("consumption", run_id)
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[]))
    assert await importer._replay_current_consumption(
        "consumption",
        run_id,
    ) is None

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="consumption_missing",
    ):
        await importer._replay_bound_consumption(
            "consumption",
            run_id,
            "build",
        )
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[{}, {}]))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="consumption_ambiguous",
    ):
        await importer._replay_bound_consumption(
            "consumption",
            run_id,
            "build",
        )


@pytest.mark.asyncio
async def test_replay_owner_edges(monkeypatch):
    run_id = "run_" + "1" * 32
    other_run_id = "run_" + "2" * 32
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="competing_owner",
    ):
        await importer._assert_replay_owner(
            "mrf",
            other_run_id,
            run_id,
        )
    await importer._assert_replay_owner("mrf", run_id, run_id)

@pytest.mark.asyncio
async def test_replay_receipt_resolution_edges(monkeypatch):
    execution = _execution()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_committed_receipt",
        AsyncMock(return_value=None),
    )
    assert await importer._replay_exact_receipt(
        "mrf",
        execution,
        None,
    ) is None

    current_consumption_by_field = {"build_id": "build-a"}
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_missing",
    ):
        await importer._replay_exact_receipt(
            "mrf",
            execution,
            current_consumption_by_field,
        )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value={}))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="identity_conflict",
    ):
        await importer._replay_exact_receipt(
            "mrf",
            execution,
            current_consumption_by_field,
        )

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_missing",
    ):
        await importer._replay_serving_state(
            "mrf",
            execution,
            {},
        )

def test_profile_delta_serving_state_edges():
    delta = _prepared_delta()
    serving = _matching_delta_serving_state(delta)
    importer._validate_profile_delta_serving_state(serving, delta)
    for field_name, replacement in (
        ("status", "building"),
        ("generation_id", "changed"),
        ("source_vector_hash", "changed"),
        ("source_context_vector_hash", "changed"),
        ("profile_as_of", "2026-07-29"),
        ("capacity_geometry_status", "verified"),
        ("evidence_target_oid", 999),
        ("profile_target_oid", 999),
        ("evidence_rows", 999),
        ("profile_rows", 999),
    ):
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="serving_generation_changed",
        ):
            importer._validate_profile_delta_serving_state(
                dataclasses.replace(
                    serving,
                    **{field_name: replacement},
                ),
                delta,
            )

    receipt = _matching_delta_receipt(delta)
    assert not importer._is_profile_delta_serving_committed(
        None,
        delta,
        receipt,
        None,
    )

@pytest.mark.asyncio
async def test_profile_delta_locked_state_and_apply_edges(monkeypatch):
    delta = _prepared_delta()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    with pytest.raises(RuntimeError, match="admission_required"):
        await importer._apply_provider_directory_profile_delta(
            delta,
            pending_commit_items=1,
        )

    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        importer,
        "_profile_delta_locked_state",
        AsyncMock(return_value=None),
    )
    await importer._apply_provider_directory_profile_delta(
        delta,
        pending_commit_items=1,
    )

    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_delta_identity",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_is_profile_delta_receipt_replay",
        AsyncMock(return_value=True),
    )
    assert await importer._profile_delta_locked_state(delta) is None
