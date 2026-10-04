# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Narrow edge proofs for bounded Provider Directory Profile deltas."""

from __future__ import annotations

import hashlib

from process import provider_directory_profile_failed_cleanup as cleanup
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
from tests.test_provider_directory_profile_failed_cleanup import (
    NOW,
    authorization_fixture,
    sign,
)


def _compact_profile_build():
    """Return a minimal build identity for compact projection tests."""

    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-30",
        evidence_stage="evidence_stage",
        profile_stage="profile_stage",
        build_id="pdpb_" + "a" * 32,
    )


@contextlib.asynccontextmanager
async def _capacity_transaction():
    """Yield a no-op capacity transaction for focused edge tests."""

    yield


def _prepared_artifact_stages():
    """Return two ordered stages sharing one resume checkpoint."""

    stage_a = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="a_stage",
        target_relation="a_target",
        rename_stage_indexes=AsyncMock(),
        resume_checkpoint=("mrf", "build-a"),
    )
    stage_b = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="b_stage",
        target_relation="b_target",
        rename_stage_indexes=AsyncMock(),
        resume_checkpoint=("mrf", "build-a"),
    )
    return stage_a, stage_b


@pytest.mark.asyncio
async def test_compact_projection_rejection_edges(monkeypatch):
    """Compact projection rejects unsupported and invalid inputs."""

    build = _compact_profile_build()
    unsupported = importer._ProviderDirectoryProfileCompactBatch(kind="copy")
    with pytest.raises(RuntimeError, match="copy_batch_unsupported"):
        await importer._project_provider_directory_profile_compact_batch(
            build,
            unsupported,
            profile_count_sql=Mock(),
            profile_sql_args_by_name={},
            profile_params_by_name={},
        )

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        _capacity_transaction,
    )
    batch = importer._ProviderDirectoryProfileCompactBatch(
        kind="npi",
        npi_start=profile.NPI_MIN,
        npi_end=profile.NPI_MIN + 10,
    )
    count_sql = Mock(return_value="SELECT projection")
    for projection_row, expected_error in (
        (None, "projection_missing"),
        ({}, "projection_invalid"),
        (
            {"projected_rows": -1, "projected_logical_bytes": 0},
            "projection_invalid",
        ),
    ):
        monkeypatch.setattr(
            importer.db,
            "first",
            AsyncMock(return_value=projection_row),
        )
        with pytest.raises(RuntimeError, match=expected_error):
            await importer._project_provider_directory_profile_compact_batch(
                build,
                batch,
                profile_count_sql=count_sql,
                profile_sql_args_by_name={},
                profile_params_by_name={},
            )


@pytest.mark.asyncio
async def test_compact_insert_projection_limit_edge(monkeypatch):
    """Compact inserts cannot exceed their signed projection."""

    build = _compact_profile_build()
    batch = importer._ProviderDirectoryProfileCompactBatch(
        kind="npi",
        npi_start=profile.NPI_MIN,
        npi_end=profile.NPI_MIN + 10,
    )
    count_sql = Mock(return_value="SELECT projection")
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        _capacity_transaction,
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "projected_rows": 2,
                "projected_logical_bytes": 128,
            }
        ),
    )
    projection = (
        await importer._project_provider_directory_profile_compact_batch(
            build,
            batch,
            profile_count_sql=count_sql,
            profile_sql_args_by_name={},
            profile_params_by_name={},
        )
    )
    assert projection.projected_rows == 2

    monkeypatch.setattr(
        importer,
        "_execute_provider_directory_profile_compact_statement",
        AsyncMock(return_value=3),
    )
    with pytest.raises(RuntimeError, match="projection_exceeded"):
        await importer._execute_provider_directory_profile_compact_batch(
            build,
            batch,
            copy_profiles_sql="",
            profile_insert_sql=Mock(),
            profile_sql_args_by_name={},
            profile_params_by_name={},
            projection=projection,
        )


@pytest.mark.asyncio
async def test_capacity_relation_size_edges(monkeypatch):
    """Relation sizing handles empty, missing, and invalid observations."""

    assert (
        await importer._provider_directory_profile_capacity_relation_bytes(())
        == 0
    )
    for relation_size_by_field, expected_error in (
        (None, "relation_size_missing"),
        (
            {"all_present": False, "total_bytes": 0},
            "relation_missing",
        ),
        (
            {"all_present": True, "total_bytes": -1},
            "relation_size_invalid",
        ),
    ):
        monkeypatch.setattr(
            importer.db,
            "first",
            AsyncMock(return_value=relation_size_by_field),
        )
        with pytest.raises(
            (RuntimeError, importer.ProviderDirectoryArtifactBuildStale),
            match=expected_error,
        ):
            await importer._provider_directory_profile_capacity_relation_bytes(
                ('"mrf"."stage"',)
            )


@pytest.mark.asyncio
async def test_capacity_scratch_requires_admission(monkeypatch):
    """Scratch projection is unavailable without an admitted geometry."""

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    assert (
        await importer._assert_provider_directory_profile_capacity_scratch(
            "profile_stage",
            ('"mrf"."stage"',),
        )
        == 0
    )
    with pytest.raises(RuntimeError, match="admission_missing"):
        await importer._project_provider_directory_profile_scratch_window(
            "profile_stage",
            '"mrf"."stage"',
            12,
            inserted_rows=1,
            inserted_logical_bytes=1,
            expected_persistence="p",
        )


@pytest.mark.asyncio
async def test_capacity_scratch_limit_edges(monkeypatch):
    """Scratch rows, bytes, relations, and operations remain bounded."""

    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    with pytest.raises(RuntimeError, match="row_limit_exceeded"):
        await importer._assert_provider_directory_profile_capacity_scratch(
            "profile_stage",
            ('"mrf"."stage"',),
            observed_rows=2,
            maximum_rows=1,
        )

    cap = importer._provider_directory_profile_capacity_relation_cap(
        admission,
        "profile_stage",
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(return_value=cap.max_scratch_bytes + 1),
    )
    with pytest.raises(RuntimeError, match="scratch_bytes_exceeded"):
        await importer._assert_provider_directory_profile_capacity_scratch(
            "profile_stage",
            ('"mrf"."stage"',),
        )

    with pytest.raises(RuntimeError, match="relation_cap_missing"):
        importer._provider_directory_profile_capacity_relation_cap(
            admission,
            "unknown",
        )
    with pytest.raises(RuntimeError, match="control_operation_missing"):
        importer._provider_directory_profile_control_operation_count(
            admission,
            "unknown",
        )

@pytest.mark.parametrize(
    ("control", "relation", "metadata"),
    (
        ({}, {}, 0),
        ({"operation": -1}, {}, 0),
        ({}, {"profile_stage": True}, 0),
        ({}, {}, -1),
    ),
)
def test_wal_reservation_inputs_reject_empty_or_negative(
    control,
    relation,
    metadata,
):
    with pytest.raises(RuntimeError, match="wal_reservation_invalid"):
        importer._profile_wal_reservation_inputs(
            control,
            relation,
            metadata,
        )

def test_artifact_bundle_ordering_edges():
    """Artifact bundles require one schema and unique target relations."""

    stage_a, stage_b = _prepared_artifact_stages()
    assert importer._ordered_provider_directory_artifact_bundle(
        (stage_b, stage_a)
    ) == (stage_a, stage_b)
    with pytest.raises(ValueError, match="schema_mismatch"):
        importer._ordered_provider_directory_artifact_bundle(
            (stage_a, dataclasses.replace(stage_b, schema="other"))
        )
    with pytest.raises(ValueError, match="target_duplicate"):
        importer._ordered_provider_directory_artifact_bundle(
            (
                stage_a,
                dataclasses.replace(
                    stage_b,
                    target_relation=stage_a.target_relation,
                ),
            )
        )


@pytest.mark.asyncio
async def test_profile_stage_finalization_edges(monkeypatch):
    """Deferred and delta finalization retain their signed metrics."""

    stage_a, stage_b = _prepared_artifact_stages()
    metrics_by_name = {"profile_rows": 1}
    deferred = await importer._finalize_provider_directory_profile_stages(
        metrics_by_name,
        (stage_a, stage_b),
        defer_cutover=True,
    )
    assert deferred == (metrics_by_name, (stage_a, stage_b))

    promote = AsyncMock()
    refresh = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promote,
    )
    monkeypatch.setattr(
        importer,
        "_refresh_provider_directory_profile_delta_metrics",
        refresh,
    )
    delta = _prepared_delta()
    assert (
        await importer._finalize_provider_directory_profile_stages(
            metrics_by_name,
            delta,
            defer_cutover=False,
        )
        == metrics_by_name
    )
    promote.assert_awaited_once_with((), profile_delta=delta)
    refresh.assert_awaited_once_with(metrics_by_name, delta)

@pytest.mark.asyncio
async def test_profile_artifact_presence_edges(monkeypatch):
    table_present = AsyncMock(side_effect=[False, False])
    monkeypatch.setattr(importer, "_is_table_present", table_present)
    assert not await importer._has_provider_directory_profile_artifacts("mrf")

    table_present.side_effect = [True, False]
    with pytest.raises(RuntimeError, match="artifact_pair_incomplete"):
        await importer._has_provider_directory_profile_artifacts("mrf")

    table_present.side_effect = [True, True]
    monkeypatch.setattr(
        importer.db,
        "scalar",
        AsyncMock(side_effect=[True, False]),
    )
    with pytest.raises(RuntimeError, match="pair_empty_incomplete"):
        await importer._has_provider_directory_profile_artifacts("mrf")

@pytest.mark.asyncio
async def test_stale_build_cleanup_marker_edges(monkeypatch):
    @contextlib.asynccontextmanager
    async def transaction():
        yield

    current_build_id = "pdpb_" + "c" * 32
    stale_build_id = "pdpb_" + "d" * 32
    checkpoint_by_field = {
        "build_id": stale_build_id,
        "evidence_stage": (
            profile.profile_evidence_stage_table_name(stale_build_id)
        ),
        "profile_stage": profile.profile_stage_table_name(stale_build_id),
        "affected_npi_stage": importer._bounded_identifier(
            f"provider_directory_profile_affected_{stale_build_id}"
        ),
        "materialization_mode": "source_delta",
        "last_error": (
            "[provider_directory_failed_profile_cleanup_v1 cleaned]"
        ),
    }
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[checkpoint_by_field]),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(),
    )
    relation_identity = AsyncMock(return_value=None)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        relation_identity,
    )
    with pytest.raises(RuntimeError, match="failed_profile_cleanup_completion_missing"):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=current_build_id,
        )
    relation_identity.assert_not_awaited()


def _cleanup_completion_claim(checkpoint):
    """Bind the existing synthetic signing fixture to the exact failed checkpoint."""
    envelope, trust, key = authorization_fixture()
    body = envelope["authorization"]
    body["checkpoint"]["build_id"] = checkpoint["build_id"]
    body["checkpoint"]["preimage_sha256"] = cleanup.digest(
        cleanup.json_values(checkpoint), cleanup.CONTRACT + ".checkpoint"
    )
    body["checkpoint"]["original_error_sha256"] = hashlib.sha256(
        checkpoint["last_error"].encode()
    ).hexdigest()
    envelope = sign(body, key)
    claim_by_field = {
        "operation_id": body["operation_id"],
        "reservation_id": body["reservation_id"],
        "nonce": body["nonce"],
        "build_id": body["checkpoint"]["build_id"],
        "owner_run_id": body["checkpoint"]["owner_run_id"],
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "authorization_sha256": cleanup.digest(envelope),
        "authorization_json": cleanup.canonical(body),
        "signature": envelope["signature"],
        "claimed_at": NOW,
        "expires_at": cleanup.timestamp(body["expires_at"]),
        "max_operation_deadline": cleanup.timestamp(body["max_operation_deadline"]),
    }
    return claim_by_field, trust, body


def _completed_cleanup_checkpoint():
    """Use the existing signed authority and exact receipt shape for retained cleanup."""
    build_id = "pdpb_" + "d" * 32
    checkpoint_by_field = {
        "build_id": build_id,
        "evidence_stage": profile.profile_evidence_stage_table_name(build_id),
        "profile_stage": profile.profile_stage_table_name(build_id),
        "affected_npi_stage": importer._bounded_identifier(
            f"provider_directory_profile_affected_{build_id}"
        ),
        "materialization_mode": "source_delta",
        "last_error": "projection failed",
        "updated_at": NOW,
    }
    claim, trust, body = _cleanup_completion_claim(checkpoint_by_field)
    completed_at = NOW + datetime.timedelta(seconds=1)
    receipt_by_field = {
        "contract_id": cleanup.RECEIPT_CONTRACT,
        "operation_id": claim["operation_id"],
        "authorization_sha256": claim["authorization_sha256"],
        "checkpoint_preimage_sha256": claim["checkpoint_preimage_sha256"],
        "original_updated_at": NOW.isoformat(),
        "original_error_sha256": body["checkpoint"]["original_error_sha256"],
        "disposed_stages": body["stages"],
        "completed_at": completed_at.isoformat(),
        "wal_start_lsn": "0/100",
        "wal_precommit_lsn": "0/200",
        "wal_precommit_bytes": 256,
    }
    checkpoint_by_field.update(
        last_error=checkpoint_by_field["last_error"] + cleanup.completion_suffix(receipt_by_field),
        updated_at=completed_at,
    )
    return checkpoint_by_field, claim, trust, body["database"]


@pytest.mark.asyncio
async def test_stale_build_authenticated_cleanup_completion(monkeypatch):
    """Authenticate retained completion before accepting absent stages or refusing drift."""
    checkpoint, claim, trust, database = _completed_cleanup_checkpoint()
    database_binding_by_field = {
        field: database[field] for field in (
            "database_system_identifier", "database_oid", "database_name"
        )
    }
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[checkpoint]))
    monkeypatch.setattr(
        importer.db, "first", AsyncMock(side_effect=[
            SimpleNamespace(_mapping=claim), SimpleNamespace(_mapping=database_binding_by_field)
        ] * 3)
    )
    monkeypatch.setattr(cleanup, "configured_cleanup_trust", lambda: trust)
    monkeypatch.setattr(cleanup, "_now_utc", lambda: NOW + datetime.timedelta(hours=1))
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_admission", lambda: SimpleNamespace())
    relation_identity = AsyncMock(return_value=None)
    monkeypatch.setattr(importer, "_provider_directory_profile_stage_relation_identity", relation_identity)
    current_build_id = "pdpb_" + "c" * 32
    assert await importer._reap_stale_provider_directory_profile_builds(
        "mrf", current_build_id=current_build_id
    ) == 0
    assert {invocation.args for invocation in relation_identity.await_args_list} == {
        ("mrf", checkpoint[field])
        for field in ("evidence_stage", "profile_stage", "affected_npi_stage")
    }

    relation_identity.reset_mock()
    # Keep completed-history stages absent, then test the reaper's final presence check.
    relation_identity.side_effect = [None] * 4 + [(22, "r", "p"), None]
    with pytest.raises(RuntimeError, match="disposed_checkpoint_stage_present"):
        await importer._reap_stale_provider_directory_profile_builds("mrf", current_build_id=current_build_id)

    relation_identity.reset_mock()
    checkpoint["profile_stage"] += "_changed"
    with pytest.raises(RuntimeError, match="completion_preimage_changed"):
        await importer._reap_stale_provider_directory_profile_builds("mrf", current_build_id=current_build_id)
    relation_identity.assert_not_awaited()

@pytest.mark.asyncio
async def test_bundle_profile_delta_edges(monkeypatch):
    delta = _prepared_delta()
    bundle = importer.ProviderDirectoryArtifactBundle()
    assert bundle.target_relations == set()
    bundle.add_profile_delta(delta)
    assert bundle.target_relations == {
        profile.PROFILE_EVIDENCE_TABLE,
        profile.PROFILE_TABLE,
    }
    with pytest.raises(RuntimeError, match="delta_duplicate"):
        bundle.add_profile_delta(delta)

    promote = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promote,
    )
    assert await bundle.promote() == 1
    promote.assert_awaited_once_with((), profile_delta=delta)

    ordinary = importer.ProviderDirectoryArtifactBundle()
    stage = importer.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="stage",
        target_relation="target",
        rename_stage_indexes=AsyncMock(),
    )
    ordinary.add(stage)
    assert await ordinary.promote() == 1
    promote.assert_awaited_with((stage,))
