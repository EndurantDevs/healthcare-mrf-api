# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal caller boundaries and retry safety for relation cap release."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def phase_events(monkeypatch):
    events = []
    for name, event in (
        ("_populate_provider_directory_profile_evidence_stage", "evidence"),
        ("_populate_provider_directory_profile_affected_npi_stage", "affected"),
        ("_populate_provider_directory_profile_compact_stage", "profile"),
        ("_provider_directory_profile_metrics", "metrics"),
        ("_prepare_provider_directory_profile_stages", "prepare"),
    ):

        async def run(*args, _event=event, **kwargs):
            events.append(_event)
            return {"ready": True} if _event == "metrics" else ("prepared",)

        monkeypatch.setattr(fhir, name, run)

    async def complete(relation_name):
        events.append("close:" + relation_name)

    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", complete)
    return events


async def populate(mode="source_delta", *, cursor=0, state="building_evidence", affected="affected"):
    build = SimpleNamespace(materialization_mode=mode, affected_npi_stage=affected)
    checkpoint = SimpleNamespace(
        evidence_next_batch=cursor,
        evidence_total_batches=cursor,
        profile_next_batch=cursor,
        state=state,
    )
    return await fhir._populate_claimed_provider_directory_profile_stages(
        build,
        checkpoint,
        True,
        object(),
        object(),
        object(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor,state,evidence_runs", [(0, "building_evidence", True), (3, "ready", False)])
async def test_claimed_stages_close_in_terminal_order(phase_events, cursor, state, evidence_runs):
    await populate(cursor=cursor, state=state)
    assert phase_events == (["evidence"] if evidence_runs else []) + [
        "close:evidence_stage",
        "affected",
        "close:affected_npi_stage",
        "profile",
        "close:profile_stage",
        "metrics",
        "prepare",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,closed_names",
    [
        ("evidence", []),
        ("affected", ["evidence_stage"]),
        ("profile", ["evidence_stage", "affected_npi_stage"]),
        ("prepare", ["evidence_stage", "affected_npi_stage", "profile_stage"]),
    ],
)
async def test_failed_phase_keeps_its_future_exposure(monkeypatch, phase_events, phase, closed_names):
    functions_by_phase = {
        "evidence": "_populate_provider_directory_profile_evidence_stage",
        "affected": "_populate_provider_directory_profile_affected_npi_stage",
        "profile": "_populate_provider_directory_profile_compact_stage",
        "prepare": "_prepare_provider_directory_profile_stages",
    }
    monkeypatch.setattr(fhir, functions_by_phase[phase], AsyncMock(side_effect=RuntimeError("phase failed")))
    with pytest.raises(RuntimeError, match="phase failed"):
        await populate()
    assert [event[6:] for event in phase_events if event.startswith("close:")] == closed_names


@pytest.mark.asyncio
async def test_source_delta_missing_affected_stage_does_not_close_it(phase_events):
    with pytest.raises(RuntimeError, match="affected_stage_missing"):
        await populate(affected=None)
    assert phase_events == ["evidence", "close:evidence_stage"]


@pytest.mark.asyncio
async def test_full_swap_closes_unused_targets_only_after_preparation(phase_events):
    await populate("full_swap", affected=None)
    assert phase_events == [
        "evidence",
        "close:evidence_stage",
        "close:affected_npi_stage",
        "profile",
        "close:profile_stage",
        "metrics",
        "prepare",
        "close:evidence_target",
        "close:profile_target",
    ]


@pytest.mark.asyncio
async def test_full_swap_preparation_failure_keeps_targets_open(monkeypatch, phase_events):
    monkeypatch.setattr(
        fhir, "_prepare_provider_directory_profile_stages", AsyncMock(side_effect=RuntimeError("invalid stage"))
    )
    with pytest.raises(RuntimeError, match="invalid stage"):
        await populate("full_swap", affected=None)
    assert not any(event in phase_events for event in ("close:evidence_target", "close:profile_target"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("payload failed"), asyncio.CancelledError()])
async def test_artifact_completion_waits_for_payload_and_never_cleanup(monkeypatch, failure):
    events = []
    plan = SimpleNamespace(relation_by_table={"source": "scratch"}, created_tables=["scratch"])
    monkeypatch.setattr(fhir, "_artifact_scope_materialization_plan", lambda _: plan)

    async def recover(*args):
        events.append("recover")

    async def payload(*args):
        events.append("payload")
        if failure is not None:
            raise failure

    async def cleanup(*args):
        events.append("cleanup")

    async def complete(name):
        events.append("close:" + name)

    monkeypatch.setattr(fhir, "_recover_provider_directory_artifact_scope", recover)
    monkeypatch.setattr(fhir, "_materialize_artifact_scope_payload", payload)
    monkeypatch.setattr(fhir, "_cleanup_failed_artifact_scope", cleanup)
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", complete)
    if failure is None:
        assert await fhir._materialize_artifact_scope_tables("synthetic", None, object(), frozenset()) == (
            plan.relation_by_table,
            plan.created_tables,
        )
        assert events == ["recover", "payload", "close:artifact_scope"]
    else:
        with pytest.raises(type(failure)):
            await fhir._materialize_artifact_scope_tables("synthetic", None, object(), frozenset())
        assert events == ["recover", "payload", "cleanup"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("completion unresolved"), asyncio.CancelledError()])
async def test_artifact_completion_failure_cleans_exact_created_tables(monkeypatch, failure):
    plan = SimpleNamespace(relation_by_table={"source": "scratch"}, created_tables=["scratch"])
    monkeypatch.setattr(fhir, "_artifact_scope_materialization_plan", lambda _: plan)
    monkeypatch.setattr(fhir, "_recover_provider_directory_artifact_scope", AsyncMock())
    monkeypatch.setattr(fhir, "_materialize_artifact_scope_payload", AsyncMock())
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", AsyncMock(side_effect=failure))
    cleanup = AsyncMock()
    monkeypatch.setattr(fhir, "_cleanup_failed_artifact_scope", cleanup)
    with pytest.raises(type(failure)):
        await fhir._materialize_artifact_scope_tables("synthetic", None, object(), frozenset())
    cleanup.assert_awaited_once_with("synthetic", plan.created_tables, failure)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "counts", "growth"])
async def test_target_classes_close_after_count_and_physical_growth_guards(monkeypatch, failure):
    events = []
    admission = object()
    relations = SimpleNamespace(evidence_target="evidence", profile_target="profile")
    locked = SimpleNamespace(relations=relations, counts_by_name={}, serving_state=object())
    forecast = SimpleNamespace(wal_start_lsn="0/1")
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(fhir, "_profile_delta_locked_state", AsyncMock(return_value=locked))
    monkeypatch.setattr(fhir, "_prepare_profile_delta_capacity", AsyncMock(return_value=forecast))
    monkeypatch.setattr(fhir, "_profile_delta_target_wal_start_lsn", AsyncMock(return_value="0/2"))
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=0))

    @asynccontextmanager
    async def observe(*args, **kwargs):
        yield

    async def counts(*args):
        events.append("counts")
        if failure == "counts":
            raise RuntimeError("counts failed")

    async def growth(*args):
        events.append("growth")
        if failure == "growth":
            raise RuntimeError("growth failed")
        return object()

    async def complete(name):
        events.append("close:" + name)

    async def actual(*args):
        events.append("actual")
        return {}

    monkeypatch.setattr(fhir, "_observe_profile_capacity_wave", observe)
    monkeypatch.setattr(fhir, "_apply_profile_delta_target_rows", counts)
    monkeypatch.setattr(fhir, "_profile_delta_target_bytes_after", growth)
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", complete)
    monkeypatch.setattr(fhir, "_profile_delta_cutover_actual", actual)
    if failure:
        with pytest.raises(RuntimeError, match=failure + " failed"):
            await fhir._apply_provider_directory_profile_delta_rows(object(), pending_commit_items=0)
        assert not any(event.startswith("close:") for event in events)
    else:
        await fhir._apply_provider_directory_profile_delta_rows(object(), pending_commit_items=0)
        assert events == ["counts", "growth", "close:evidence_target", "close:profile_target", "actual"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bounded,closed,refuses",
    [
        (True, {"evidence_target"}, True),
        (True, {"profile_target"}, True),
        (True, {"artifact_scope", "evidence_stage"}, False),
        (False, {"evidence_target"}, False),
    ],
)
async def test_closed_target_refuses_retry_but_preserves_pre_target_and_legacy_retry(
    monkeypatch, bounded, closed, refuses
):
    failure = RuntimeError("retryable lock failure")
    promote = AsyncMock(side_effect=[failure, None])
    sleep = AsyncMock()
    admission = SimpleNamespace(
        geometry=SimpleNamespace(bounded_admission=bounded),
        wal_tracker=SimpleNamespace(completed_relation_classes=closed),
    )
    monkeypatch.setattr(fhir, "_promote_provider_directory_artifact_bundle", promote)
    monkeypatch.setattr(fhir, "_is_provider_directory_artifact_cutover_retryable", lambda _: True)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(fhir.asyncio, "sleep", sleep)
    if refuses:
        with pytest.raises(RuntimeError) as result:
            await fhir._retry_provider_directory_artifact_bundle_promotion((), profile_delta=object())
        assert result.value is failure
        assert promote.await_count == 1
        sleep.assert_not_awaited()
    else:
        await fhir._retry_provider_directory_artifact_bundle_promotion((), profile_delta=object())
        assert promote.await_count == 2
        sleep.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name",
    ["artifact_scope", "evidence_stage", "affected_npi_stage", "profile_stage", "evidence_target", "profile_target"],
)
async def test_closed_class_cannot_reopen_through_direct_growth_reservation(name):
    tracker = fhir._ProviderDirectoryProfileWalTracker()
    tracker.completed_relation_classes.add(name)
    admission = SimpleNamespace(wal_tracker=tracker)
    with pytest.raises(RuntimeError, match="relation_completed"):
        await fhir._reserve_profile_capacity_growth(admission, name, 1)
    assert tracker.pending_growth_bytes == {}
