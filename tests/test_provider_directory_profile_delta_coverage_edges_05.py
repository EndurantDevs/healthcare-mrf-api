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


@pytest.mark.asyncio
async def test_capacity_settings_and_transaction_edges(monkeypatch):
    admission = _wal_tracker_admission()
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="settings_missing"):
        await importer._profile_capacity_observed_settings()

    monkeypatch.setattr(
        importer,
        "_profile_capacity_remaining_ms",
        AsyncMock(return_value=30_000),
    )
    monkeypatch.setattr(
        importer,
        "_set_profile_capacity_limits",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_observed_settings",
        AsyncMock(return_value={}),
    )
    with pytest.raises(RuntimeError, match="settings_changed"):
        await importer._apply_provider_directory_profile_capacity_settings(
            admission
        )

    applied = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_apply_provider_directory_profile_capacity_settings",
        applied,
    )

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        admission
    )
    transaction_events = []
    try:
        async with importer._provider_directory_profile_capacity_transaction():
            transaction_events.append("entered")
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
    assert transaction_events == ["entered"]
    applied.assert_awaited_once_with(admission)


@pytest.mark.asyncio
async def test_capacity_total_wal_and_scratch_projection_edges(monkeypatch):
    admission = _wal_tracker_admission()
    maximum = admission.geometry.reservation_bytes_by_storage_class["wal"]
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_current_wal_bytes",
        AsyncMock(return_value=maximum),
    )
    with pytest.raises(RuntimeError, match="total_wal_projected"):
        await importer._validate_profile_total_wal_budget(admission, 1, 0)

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    layout = SimpleNamespace(
        relation_oid=999,
        toastable_columns=(),
        main_index_pages=(1,),
        toast_index_pages=(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(return_value=layout),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="scratch_oid_changed",
    ):
        await importer._project_provider_directory_profile_scratch_window(
            "profile_stage",
            '"mrf"."stage"',
            12,
            inserted_rows=1,
            inserted_logical_bytes=8,
            expected_persistence="p",
        )

    profile_cap = importer._provider_directory_profile_capacity_relation_cap(
        admission,
        "profile_stage",
    )
    layout.relation_oid = 12
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(return_value=profile_cap.max_scratch_bytes),
    )
    with pytest.raises(RuntimeError, match="scratch_growth_projected"):
        await importer._project_provider_directory_profile_scratch_window(
            "profile_stage",
            '"mrf"."stage"',
            12,
            inserted_rows=1,
            inserted_logical_bytes=8,
            expected_persistence="p",
        )


@pytest.mark.asyncio
async def test_capacity_target_projection_edges(monkeypatch):
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    assert await importer._assert_provider_directory_profile_capacity_target(
        "profile_target",
        '"mrf"."profile"',
        bytes_before=10,
        deleted_logical_bytes=0,
        projected_growth_bytes=0,
    ) == 10

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    target_cap = importer._provider_directory_profile_capacity_relation_cap(
        admission,
        "profile_target",
    )
    with pytest.raises(RuntimeError, match="deleted_logical_bytes"):
        await importer._assert_provider_directory_profile_capacity_target(
            "profile_target",
            '"mrf"."profile"',
            bytes_before=10,
            deleted_logical_bytes=(
                target_cap.max_deleted_logical_bytes + 1
            ),
            projected_growth_bytes=0,
        )

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(return_value=20),
    )
    with pytest.raises(RuntimeError, match="target_growth_exceeded"):
        await importer._assert_provider_directory_profile_capacity_target(
            "profile_target",
            '"mrf"."profile"',
            bytes_before=10,
            deleted_logical_bytes=0,
            projected_growth_bytes=5,
        )


def test_affected_projection_value_edges():
    for projection_row in (
        None,
        {},
        {"projected_rows": -1, "projected_logical_bytes": -8},
        {"projected_rows": 1, "projected_logical_bytes": 7},
    ):
        with pytest.raises(RuntimeError, match="affected_projection"):
            importer._affected_npi_projection_values(projection_row)
    assert importer._affected_npi_projection_values(
        {"projected_rows": 2, "projected_logical_bytes": 16}
    ) == (2, 16)


def _affected_stage_build():
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-30",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
        affected_npi_stage=None,
    )


@pytest.mark.asyncio
async def test_affected_npi_insert_without_capacity_edges(monkeypatch):
    build = _affected_stage_build()
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    with pytest.raises(RuntimeError, match="affected_stage_missing"):
        await importer._execute_affected_npi_insert(
            build,
            projection_sql="SELECT projection",
            insert_sql="INSERT",
            params={},
        )

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="INSERT 0 2"))
    assert await importer._execute_affected_npi_insert(
        build,
        projection_sql="SELECT projection",
        insert_sql="INSERT",
        params={},
    ) == 2


@pytest.mark.asyncio
async def test_affected_npi_projection_limit_edges(monkeypatch):
    admission = _wal_tracker_admission()
    source_delta = dataclasses.replace(
        _affected_stage_build(),
        materialization_mode="source_delta",
        affected_npi_stage="affected_stage",
    )

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        transaction,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_stage_storage_identity",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "projected_rows": 1,
                "projected_logical_bytes": 8,
            }
        ),
    )
    monkeypatch.setattr(
        importer,
        "_admit_affected_npi_projection",
        AsyncMock(),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="INSERT 0 2"))
    with pytest.raises(RuntimeError, match="projection_exceeded"):
        await importer._execute_affected_npi_insert(
            source_delta,
            projection_sql="SELECT projection",
            insert_sql="INSERT",
            params={},
        )


@pytest.mark.asyncio
async def test_affected_npi_stage_analyze_edges(monkeypatch):
    admission = _wal_tracker_admission()
    source_delta = dataclasses.replace(
        _affected_stage_build(),
        materialization_mode="source_delta",
        affected_npi_stage="affected_stage",
    )
    reserve = AsyncMock()
    scratch = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        importer,
        "_reserve_provider_directory_profile_wal_budget",
        reserve,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_capacity_scratch",
        scratch,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_status",
        AsyncMock(),
    )
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=1))
    await importer._analyze_affected_npi_stage(
        source_delta,
        '"mrf"."affected"',
    )
    reserve.assert_awaited_once()
    scratch.assert_awaited_once()


def test_profile_window_and_progress_edges():
    copy_batch = importer._ProviderDirectoryProfileEvidenceBatch(kind="copy")
    fact_batch = importer._ProviderDirectoryProfileEvidenceBatch(kind="fact")
    batches = (copy_batch, fact_batch)
    assert importer._provider_directory_profile_window_end(
        batches,
        2,
        2,
    ) == 2
    assert importer._provider_directory_profile_window_end(
        batches,
        0,
        2,
    ) == 1
    assert importer._provider_directory_profile_window_end(
        batches,
        1,
        2,
    ) == 2
    assert importer._provider_directory_profile_frozen_wave_end(
        ((0, 1), (1, 2)),
        start_batch=0,
        total_batches=2,
    ) == 1
    assert importer._provider_directory_profile_frozen_wave_end(
        ((0, 1), (1, 2)),
        start_batch=2,
        total_batches=2,
    ) == 2
    with pytest.raises(RuntimeError, match="checkpoint_wave_invalid"):
        importer._provider_directory_profile_frozen_wave_end(
            ((0, 1),),
            start_batch=1,
            total_batches=2,
        )

    build = importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=(),
        retained_source_ids=(),
        dataset_ids=(),
        profile_as_of="2026-07-30",
        evidence_stage="e",
        profile_stage="p",
    )
    assert importer._provider_directory_profile_overall_pct(
        build,
        phase="profile",
        completed_batches=1,
        total_batches=2,
    ) > 50
    with pytest.raises(ValueError, match="unsupported profile progress"):
        importer._provider_directory_profile_overall_pct(
            build,
            phase="invalid",
            completed_batches=0,
            total_batches=1,
        )
