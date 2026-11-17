# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication failures preserve original errors, committed data and named targets."""

import asyncio
import copy
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_profile_delta_coverage_support import (
    _matching_delta_receipt,
    _matching_delta_serving_state,
    _prepared_delta,
    _ready_delta_checkpoint,
)
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_profile_capacity_observation import _sample
from tests.test_provider_directory_profile_selection_attestation import _execution

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("fault_phase", ["before", "after", "emit"])
@pytest.mark.parametrize("body_failure", [None, RuntimeError, asyncio.CancelledError])
async def test_observation_failure_never_changes_the_publication_body(monkeypatch, fault_phase, body_failure):
    observation_error = OSError("synthetic observation failure")
    samples = [observation_error] if fault_phase == "before" else [_sample("0/10"), _sample("0/20")]
    if fault_phase == "after":
        samples[1] = observation_error
    sampler = AsyncMock(side_effect=samples)
    emitter = Mock(side_effect=observation_error if fault_phase == "emit" else None)
    monkeypatch.setenv(fhir.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV, "1")
    monkeypatch.setattr(fhir, "_profile_capacity_observation_sample", sampler)
    monkeypatch.setattr(fhir, "_emit_profile_capacity_observation", emitter)
    original = body_failure("synthetic publication failure") if body_failure else None
    body_calls = []

    async def publication():
        async with fhir._observe_profile_capacity_wave(
            "target", {"profile": '"example"."profile_stage"'}, coordinate_by_field={"wave": 1}
        ):
            body_calls.append("applied")
            if original is not None:
                raise original
        return "published"

    if original is None:
        assert await publication() == "published"
    else:
        with pytest.raises(type(original)) as raised:
            await publication()
        assert raised.value is original
    assert body_calls == ["applied"] and emitter.call_count == 1
    assert emitter.call_args.args[5] is original
    assert sampler.await_count == (2 if fault_phase != "before" and original is None else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_network_candidate_failure_preserves_error_and_skips_swap(monkeypatch, cleanup_fails):
    original = RuntimeError("synthetic candidate population failure")

    @asynccontextmanager
    async def build_guard(*_args):
        yield object()

    monkeypatch.setattr(fhir, "_provider_directory_artifact_build_guard", build_guard)
    monkeypatch.setattr(fhir, "_network_catalog_missing_requirement", AsyncMock(return_value=None))
    monkeypatch.setattr(fhir, "_ensure_provider_directory_network_catalog_table", AsyncMock())
    monkeypatch.setattr(fhir, "_network_catalog_scope_sources", AsyncMock(return_value=["source_example"]))
    monkeypatch.setattr(fhir, "_network_catalog_stage_table_name", lambda _run: "network_candidate_example")
    monkeypatch.setattr(fhir, "_populate_network_catalog_stage", AsyncMock(side_effect=original))
    swap = AsyncMock()
    monkeypatch.setattr(fhir, "_swap_network_catalog_stage", swap)
    status = AsyncMock(side_effect=[None, OSError("synthetic cleanup failure") if cleanup_fails else None])
    monkeypatch.setattr(fhir.db, "status", status)
    with pytest.raises(RuntimeError) as raised:
        await fhir.publish_provider_directory_network_catalog("example", run_id="run_example")
    assert raised.value is original
    swap.assert_not_awaited()
    assert [call.args[0] for call in status.await_args_list] == [
        'DROP TABLE IF EXISTS "example"."network_candidate_example";'
    ] * 2


@pytest.mark.asyncio
async def test_bundle_metrics_use_exact_committed_counts(monkeypatch):
    delta = _prepared_delta()
    receipt = _matching_delta_receipt(delta)
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value=receipt))
    monkeypatch.setattr(fhir, "_is_provider_directory_profile_delta_committed", AsyncMock(return_value=True))
    bundle = fhir.ProviderDirectoryArtifactBundle(profile_delta=delta)
    metrics_by_name = {"profile": {"selected_evidence_rows": -1, "estimated": True}}
    await fhir._refresh_bundle_profile_delta_metrics(bundle, metrics_by_name)
    assert metrics_by_name["profile"]["selected_evidence_rows"] == receipt["evidence_rows"]
    assert metrics_by_name["profile"]["profile_rows"] == receipt["profile_rows"]
    assert metrics_by_name["profile"]["evidence_inserted"] == receipt["evidence_inserted"]
    assert metrics_by_name["profile"]["estimated"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["no_receipt", "lost_readback", "different_generation"])
async def test_bundle_keeps_staged_counts_when_commit_cannot_be_verified(monkeypatch, failure):
    delta = _prepared_delta()
    receipt = _matching_delta_receipt(delta)
    readback = {**receipt, "generation_id": delta.from_generation_id} if failure == "different_generation" else None
    reader = AsyncMock(side_effect=[None] if failure == "no_receipt" else [receipt, readback])
    serving_reader = AsyncMock()
    monkeypatch.setattr(fhir.db, "first", reader)
    monkeypatch.setattr(fhir, "_provider_directory_profile_serving_state", serving_reader)

    profile_metrics_by_name = {"selected_evidence_rows": 3, "estimated": True}
    bundle = fhir.ProviderDirectoryArtifactBundle(profile_delta=delta)
    reason = "receipt_missing" if failure == "no_receipt" else "receipt_unverified"
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match=reason):
        await fhir._refresh_bundle_profile_delta_metrics(bundle, {"profile": profile_metrics_by_name})

    assert profile_metrics_by_name == {"selected_evidence_rows": 3, "estimated": True}
    assert reader.await_count == (1 if failure == "no_receipt" else 2)
    serving_reader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("profile_metrics", [None, []])
async def test_delta_bundle_requires_profile_metrics_before_receipt_read(monkeypatch, profile_metrics):
    reader = AsyncMock()
    monkeypatch.setattr(fhir.db, "first", reader)
    with pytest.raises(RuntimeError, match="profile_metrics_missing"):
        await fhir._refresh_bundle_profile_delta_metrics(
            fhir.ProviderDirectoryArtifactBundle(profile_delta=_prepared_delta()), {"profile": profile_metrics}
        )
    reader.assert_not_awaited()
    await fhir._refresh_bundle_profile_delta_metrics(fhir.ProviderDirectoryArtifactBundle(), {})
    reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_committed_generation_is_a_conflict(monkeypatch):
    execution = _execution()
    rows = [{"control_generation": "not_an_integer", "selection_proof_id": execution.attestation.proof_id}]
    original = copy.deepcopy(rows)
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=rows))
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="receipt_identity_conflict"):
        await fhir._provider_directory_profile_committed_receipt(schema="example", execution=execution)
    assert rows == original


@pytest.mark.asyncio
async def test_persisted_selection_json_keeps_complete_logical_result(monkeypatch):
    selection = fhir.profile_selection_result(
        _execution(),
        profile_generation_id="pdprofile_" + "1" * 32,
        profile_as_of="2026-07-30",
        profile_rows=3,
        profile_source_evidence_rows=5,
    )
    row_by_field = {"selection_result": json.dumps(selection), "finished_at": "2026-07-30T12:00:00Z"}
    original = copy.deepcopy(row_by_field)
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value=row_by_field))
    persisted, decoded = await fhir._profile_adoption_result_row("example")
    assert decoded == selection and persisted == original and row_by_field == original
    assert decoded["row_counts"] == selection["row_counts"]


@pytest.mark.asyncio
async def test_committed_delta_replay_does_not_reopen_target_writers(monkeypatch):
    delta = _prepared_delta()
    identity = AsyncMock()
    target_reader = AsyncMock()
    writer = AsyncMock()
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_delta_identity", identity)
    monkeypatch.setattr(fhir, "_is_profile_delta_receipt_replay", AsyncMock(return_value=True))
    monkeypatch.setattr(fhir, "_provider_directory_profile_serving_state", target_reader)
    monkeypatch.setattr(fhir.db, "status", writer)
    assert await fhir._profile_delta_locked_state(delta) is None
    identity.assert_awaited_once_with(delta)
    target_reader.assert_not_awaited()
    writer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "error"),
    [
        ("missing_projection", "delta_evidence_projection_missing"),
        ("excess_delete", "delta_count_invalid"),
        ("serving_limit", "capacity_serving_rows_exceeded"),
    ],
)
async def test_cutover_requires_complete_row_accounting_before_target_writes(monkeypatch, failure, error):
    delta = _prepared_delta(expected_evidence_rows=2, expected_profile_rows=1, refresh_source_ids=("source-a",))
    checkpoint = _ready_delta_checkpoint(delta)
    serving = _matching_delta_serving_state(delta)
    admission = _wal_tracker_admission()
    inserted = admission.geometry.max_evidence_rows + 1 if failure == "serving_limit" else 0
    deleted = delta.expected_evidence_rows + 1 if failure == "excess_delete" else 0
    evidence = None if failure == "missing_projection" else {"row_count": deleted, "logical_bytes": deleted * 128}
    profile_accounting_by_field = {"row_count": 0, "logical_bytes": 0}

    monkeypatch.setattr(
        fhir.db, "first", AsyncMock(side_effect=[checkpoint, None, checkpoint, evidence, profile_accounting_by_field])
    )
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(side_effect=["p"] * 10 + [False] * 4 + [inserted, 0]))
    status, forecast, write = AsyncMock(), AsyncMock(), AsyncMock()
    serving_reader = AsyncMock(return_value=serving)
    monkeypatch.setattr(fhir.db, "status", status)
    monkeypatch.setattr(fhir, "_provider_directory_profile_serving_state", serving_reader)
    monkeypatch.setattr(fhir, "_provider_directory_profile_delta_capacity_projection", forecast)
    monkeypatch.setattr(fhir, "_apply_profile_delta_target_rows", write)

    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    try:
        with pytest.raises(RuntimeError, match=error):
            await fhir._apply_provider_directory_profile_delta(delta, pending_commit_items=1)
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    serving_reader.assert_awaited_once_with(delta.schema, for_update=True)
    assert status.await_count == 5
    assert all(operation.args[0].startswith("LOCK TABLE ") for operation in status.await_args_list)
    forecast.assert_not_awaited()
    write.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["evidence_inserted", "profile_deleted"])
async def test_actual_delta_rowcount_mismatch_refuses_completion(monkeypatch, field):
    monkeypatch.setattr(fhir, "_replace_profile_delta_evidence", AsyncMock(return_value=(2, 3)))
    monkeypatch.setattr(fhir, "_replace_profile_delta_profiles", AsyncMock(return_value=(4, 5)))
    expected_counts_by_name = {
        "evidence_deleted": 2,
        "evidence_inserted": 3,
        "profile_deleted": 4,
        "profile_inserted": 5,
    }
    expected_counts_by_name[field] += 1
    delta = _prepared_delta()
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="rowcount_changed"):
        await fhir._apply_profile_delta_target_rows(
            delta, fhir._profile_delta_relations(delta), expected_counts_by_name
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint_matches", [False, True])
async def test_delta_cleanup_preserves_checkpoint_when_it_cannot_finish(monkeypatch, checkpoint_matches):
    delta = _prepared_delta()
    checkpoint_id = delta.build_id if checkpoint_matches else "pdpb_" + "2" * 32
    delta = replace(delta, resume_checkpoint=(delta.schema, checkpoint_id))
    drop_error = PermissionError("could not drop evidence stage")
    drop_stage = AsyncMock(side_effect=[None, drop_error])
    delete_checkpoint = AsyncMock()
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_status", drop_stage)
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", AsyncMock())
    monkeypatch.setattr(fhir, "_delete_provider_directory_profile_build_checkpoint", delete_checkpoint)

    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(_wal_tracker_admission())
    try:
        if checkpoint_matches:
            with pytest.raises(PermissionError) as caught:
                await fhir._finalize_provider_directory_profile_delta_scratch(delta)
            assert caught.value is drop_error
            assert [operation.args[0] for operation in drop_stage.await_args_list] == [
                f'DROP TABLE IF EXISTS "{delta.schema}"."{stage}";'
                for stage in (delta.profile_stage, delta.evidence_stage)
            ]
        else:
            with pytest.raises(RuntimeError, match="delta_resume_checkpoint_invalid"):
                await fhir._finalize_provider_directory_profile_delta_scratch(delta)
            drop_stage.assert_not_awaited()
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    delete_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_unavailable_temp_statistics_remain_optional(monkeypatch):
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(side_effect=PermissionError("synthetic unavailable statistics")))
    assert await fhir._provider_directory_profile_temp_bytes() is None
