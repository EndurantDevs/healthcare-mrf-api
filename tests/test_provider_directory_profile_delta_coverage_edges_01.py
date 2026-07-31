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


def _cutover_actual_fields_by_name():
    """Return canonical signed cutover scalar fields."""

    return {
        "cutover_forecast_hash": "a" * 64,
        "cutover_actual_hash": "b" * 64,
        "cutover_wal_start_lsn": "0/1",
        "cutover_wal_observed_lsn": "0/2",
        "cutover_wal_bytes": 1,
        "evidence_target_bytes_before": 10,
        "evidence_target_bytes_after": 11,
        "evidence_target_growth_bytes": 1,
        "profile_target_bytes_before": 20,
        "profile_target_bytes_after": 21,
        "profile_target_growth_bytes": 1,
    }


@pytest.mark.asyncio
async def test_all_new_cleanup_edges_preserve_primary_failure(monkeypatch):
    """Every newly uncovered cleanup branch is explicitly best effort."""

    delta = _prepared_delta()
    failed_status = AsyncMock(side_effect=RuntimeError("drop unavailable"))
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_status",
        failed_status,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )

    await importer._remove_provider_directory_profile_delta_stages(delta)
    assert failed_status.await_count == 3

    stage = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="artifact_stage",
        target_relation="artifact_target",
        rename_stage_indexes=AsyncMock(),
    )
    await importer._remove_provider_directory_artifact_stage(stage)

    database_status = AsyncMock(side_effect=RuntimeError("cleanup failed"))
    monkeypatch.setattr(importer.db, "status", database_status)
    await importer._best_effort_drop_address_corroboration_stage(
        '"mrf"."address_stage"'
    )
    await importer._drop_artifact_scope_tables(
        "mrf",
        ["scope_a", "scope_b"],
    )
    await importer._remove_provider_directory_profile_stage_table(
        "mrf",
        "profile_stage",
    )

    primary = RuntimeError("prepare failed")
    monkeypatch.setattr(
        importer,
        "_prepare_provider_directory_artifact_stage",
        AsyncMock(side_effect=primary),
    )
    with pytest.raises(RuntimeError, match="prepare failed") as raised:
        await importer._cutover_provider_directory_artifact_stage(
            schema="mrf",
            stage_table="artifact_stage",
            target_relation="artifact_target",
            rename_stage_indexes=AsyncMock(),
        )
    assert raised.value is primary

@pytest.mark.asyncio
async def test_cleanup_is_fail_closed_after_capacity_admission(monkeypatch):
    """Admitted cleanup failures propagate instead of losing reserved WAL."""

    admission = SimpleNamespace()
    reserve = AsyncMock()
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
        "_provider_directory_profile_capacity_status",
        AsyncMock(side_effect=RuntimeError("admitted drop failed")),
    )

    with pytest.raises(RuntimeError, match="admitted drop failed"):
        await importer._remove_provider_directory_profile_delta_stages(
            _prepared_delta()
        )
    reserve.assert_awaited_once_with(
        admission,
        control_operation_counts={"profile_stage_drop": 1},
    )

    with pytest.raises(RuntimeError, match="admitted drop failed"):
        await importer._drop_artifact_scope_tables("mrf", ["scope_a"])
    assert reserve.await_count == 2

@pytest.mark.asyncio
async def test_delta_relation_and_checkpoint_identity_edges(monkeypatch):
    """Targets, stages, and a ready checkpoint are all immutable."""

    delta = _prepared_delta()
    valid_identities = [
        (delta.evidence_target_oid, "r", "p"),
        (delta.profile_target_oid, "r", "p"),
        (delta.evidence_stage_oid, "r", "p"),
        (delta.profile_stage_oid, "r", "p"),
        (delta.affected_npi_stage_oid, "r", "p"),
    ]
    identity = AsyncMock(side_effect=valid_identities)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        identity,
    )
    await importer._validate_profile_delta_relations(delta)

    identity.side_effect = [(999, "r", "p")]
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="delta_target_changed",
    ):
        await importer._validate_profile_delta_relations(delta)

    identity.side_effect = valid_identities[:2] + [(999, "r", "p")]
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="delta_stage_changed",
    ):
        await importer._validate_profile_delta_relations(delta)

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="delta_checkpoint_missing",
    ):
        await importer._lock_profile_delta_checkpoint(delta)

    checkpoint_by_field = _ready_delta_checkpoint(delta)
    importer._validate_profile_delta_checkpoint_fields(
        delta,
        checkpoint_by_field,
    )
    importer._validate_profile_delta_checkpoint_geometry(
        delta,
        checkpoint_by_field,
    )
    importer._validate_profile_delta_checkpoint_completion(
        delta,
        checkpoint_by_field,
    )

@pytest.mark.parametrize(
    "field_name",
    (
        "owner_run_id",
        "state",
        "resume_lineage_hash",
        "executable_plan_hash",
        "materialization_mode",
        "evidence_stage",
        "profile_stage",
        "affected_npi_stage",
        "current_source_vector_hash",
        "desired_source_vector_hash",
        "current_source_context_vector_hash",
        "desired_source_context_vector_hash",
        "profile_as_of",
        "capacity_geometry_status",
        "capacity_geometry_hash",
    ),
)
def test_delta_checkpoint_scalar_drift_is_rejected(field_name):
    delta = _prepared_delta()
    checkpoint_by_field = _ready_delta_checkpoint(delta)
    checkpoint_by_field[field_name] = "changed"
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="delta_checkpoint_changed",
    ):
        importer._validate_profile_delta_checkpoint_fields(
            delta,
            checkpoint_by_field,
        )

@pytest.mark.parametrize(
    "field_name",
    (
        "evidence_stage_oid",
        "profile_stage_oid",
        "affected_npi_stage_oid",
        "evidence_target_oid",
        "profile_target_oid",
    ),
)
def test_delta_checkpoint_oid_drift_is_rejected(field_name):
    delta = _prepared_delta()
    checkpoint_by_field = _ready_delta_checkpoint(delta)
    checkpoint_by_field[field_name] = 999
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="delta_checkpoint_changed",
    ):
        importer._validate_profile_delta_checkpoint_completion(
            delta,
            checkpoint_by_field,
        )

def test_delta_checkpoint_geometry_and_completion_edges():
    delta = _prepared_delta()
    checkpoint_by_field = _ready_delta_checkpoint(delta)

    malformed_checkpoint_by_field = dict(
        checkpoint_by_field,
        capacity_geometry_json="{",
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="capacity_geometry",
    ):
        importer._validate_profile_delta_checkpoint_geometry(
            delta,
            malformed_checkpoint_by_field,
        )

    changed_checkpoint_by_field = dict(
        checkpoint_by_field,
        capacity_geometry_status="legacy_unavailable",
        capacity_geometry_hash=None,
        capacity_geometry_json=None,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="capacity_geometry",
    ):
        importer._validate_profile_delta_checkpoint_geometry(
            delta,
            changed_checkpoint_by_field,
        )

    for field_name in ("refresh_source_ids", "removed_source_ids"):
        changed_checkpoint_by_field = dict(checkpoint_by_field)
        changed_checkpoint_by_field[field_name] = ["different-source"]
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="checkpoint_changed:sources",
        ):
            importer._validate_profile_delta_checkpoint_completion(
                delta,
                changed_checkpoint_by_field,
            )

    for field_name in ("evidence_next_batch", "profile_next_batch"):
        changed_checkpoint_by_field = dict(checkpoint_by_field)
        changed_checkpoint_by_field[field_name] = 0
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="checkpoint_incomplete",
        ):
            importer._validate_profile_delta_checkpoint_completion(
                delta,
                changed_checkpoint_by_field,
            )

@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    (
        ("evidence_rows", None),
        ("profile_rows", -1),
        ("evidence_inserted", "bad"),
        ("evidence_deleted", object()),
        ("profile_inserted", -2),
        ("profile_deleted", None),
    ),
)
def test_delta_receipt_counts_are_nonnegative(field_name, bad_value):
    receipt_by_field = _matching_delta_receipt(_prepared_delta())
    receipt_by_field[field_name] = bad_value
    assert not importer._has_nonnegative_profile_delta_counts(receipt_by_field)

def test_delta_receipt_identity_and_geometry_edges():
    delta = _prepared_delta()
    receipt_by_field = _matching_delta_receipt(delta)
    assert importer._is_provider_directory_profile_delta_receipt_matching(
        receipt_by_field,
        delta,
    )

    for field_name in (
        "build_id",
        "control_generation",
        "authority_revision",
        "evidence_target_oid",
        "profile_target_oid",
    ):
        changed_receipt_by_field = dict(receipt_by_field)
        changed_receipt_by_field[field_name] = (
            "different"
            if isinstance(receipt_by_field[field_name], str)
            else "bad"
        )
        assert not (
            importer._is_provider_directory_profile_delta_receipt_matching(
                changed_receipt_by_field,
                delta,
            )
        )

    malformed_receipt_by_field = dict(
        receipt_by_field,
        capacity_geometry_json="{",
    )
    assert not importer._is_profile_delta_geometry_matching(
        malformed_receipt_by_field,
        delta,
    )

    changed_receipt_by_field = dict(
        receipt_by_field,
        from_capacity_geometry_status="verified",
        from_capacity_geometry_hash=delta.capacity_geometry_hash,
        from_capacity_geometry_json=delta.capacity_geometry_json,
    )
    assert not importer._is_profile_delta_geometry_matching(
        changed_receipt_by_field,
        delta,
    )

def test_cutover_receipt_decoding_edges():
    empty_receipt_by_field = {}
    assert importer._profile_cutover_receipt_parts(
        empty_receipt_by_field
    ) is None

    partial_receipt_by_field = {"cutover_forecast_hash": "a" * 64}
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="cutover_receipt_incomplete",
    ):
        importer._profile_cutover_receipt_parts(partial_receipt_by_field)

    cutover_fields_by_name = {
        "cutover_forecast_hash": "a" * 64,
        "cutover_forecast_json": json.dumps({"forecast": 1}),
        "cutover_actual_hash": "b" * 64,
        "cutover_actual_json": json.dumps({"actual": 1}),
        "cutover_wal_start_lsn": "0/1",
        "cutover_wal_observed_lsn": "0/2",
        "cutover_wal_bytes": 1,
        "evidence_target_bytes_before": 10,
        "evidence_target_bytes_after": 11,
        "evidence_target_growth_bytes": 1,
        "profile_target_bytes_before": 20,
        "profile_target_bytes_after": 21,
        "profile_target_growth_bytes": 1,
    }
    parts = importer._profile_cutover_receipt_parts(cutover_fields_by_name)
    assert parts is not None
    assert parts[1] == {"forecast": 1}
    assert parts[2] == {"actual": 1}

    for field_name in ("cutover_forecast_json", "cutover_actual_json"):
        changed_fields_by_name = dict(cutover_fields_by_name)
        changed_fields_by_name[field_name] = []
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="cutover_receipt_invalid",
        ):
            importer._profile_cutover_receipt_parts(changed_fields_by_name)

def test_cutover_actual_accepts_signed_values():
    """Canonical scalar values match their signed cutover receipt."""

    cutover_fields_by_name = _cutover_actual_fields_by_name()
    cutover_scalars_by_name = importer._profile_cutover_actual_scalars(
        cutover_fields_by_name
    )
    actual_by_field = dict(cutover_scalars_by_name)
    importer._validate_profile_cutover_actual(
        cutover_fields_by_name,
        {},
        actual_by_field,
        "a" * 64,
        "b" * 64,
        cutover_scalars_by_name,
    )


@pytest.mark.parametrize(
    ("mismatch", "replacement"),
    (
        ("forecast_hash", "c" * 64),
        ("actual_hash", "c" * 64),
        ("actual_payload", {"cutover_wal_bytes": 999}),
        ("evidence_growth", {"evidence_target_growth_bytes": 0}),
        ("profile_growth", {"profile_target_growth_bytes": 0}),
    ),
)
def test_cutover_actual_rejects_signed_mismatch(mismatch, replacement):
    """Each signed scalar mismatch invalidates the cutover receipt."""

    cutover_fields_by_name = _cutover_actual_fields_by_name()
    cutover_scalars_by_name = importer._profile_cutover_actual_scalars(
        cutover_fields_by_name
    )
    changed_actual_by_field = dict(cutover_scalars_by_name)
    changed_scalars_by_name = dict(cutover_scalars_by_name)
    forecast_hash = "a" * 64
    actual_hash = "b" * 64
    if mismatch == "forecast_hash":
        forecast_hash = replacement
    elif mismatch == "actual_hash":
        actual_hash = replacement
    elif mismatch == "actual_payload":
        changed_actual_by_field.update(replacement)
    else:
        changed_scalars_by_name.update(replacement)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="cutover_receipt_changed",
    ):
        importer._validate_profile_cutover_actual(
            cutover_fields_by_name,
            {},
            changed_actual_by_field,
            forecast_hash,
            actual_hash,
            changed_scalars_by_name,
        )
