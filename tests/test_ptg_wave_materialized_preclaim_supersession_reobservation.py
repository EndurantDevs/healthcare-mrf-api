"""Focused reobservation-boundary coverage for materialized-preclaim recovery."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock

import pytest

from api import ptg_wave_kubernetes_attestation as kubernetes_attestation
from process import ptg_wave_materialized_preclaim_supersession_runtime as runtime
from tests.test_ptg_wave_materialized_preclaim_supersession import _attest
from tests.test_ptg_wave_materialized_preclaim_supersession_runtime import (
    _ReadSession,
    _Result,
    _snapshot,
)


@pytest.mark.asyncio
async def test_locked_reobservation_returns_the_matching_materialized_witness(
    monkeypatch,
):
    """The locked V5 handoff returns only its freshly matched witness."""

    proof = _attest()
    snapshot = _snapshot()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    loader = AsyncMock(return_value=snapshot)
    observe = AsyncMock(return_value=proof)
    monkeypatch.setattr(runtime, "_load_snapshot", loader)
    monkeypatch.setattr(runtime, "_observe", observe)

    observed = await runtime.attest_locked_materialized_preclaim_supersession(
        object(),
        "materialized-wave",
        "successor-wave",
        proof,
        redis="redis-observer",
    )

    assert observed == proof
    loader.assert_awaited_once_with(
        ANY,
        "materialized-wave",
        lock_rows=True,
    )
    observe.assert_awaited_once_with(
        snapshot,
        "successor-wave",
        redis="redis-observer",
    )


@pytest.mark.asyncio
async def test_locked_snapshot_reads_every_materialized_boundary_in_lock_order(
    monkeypatch,
):
    """V5 reobservation locks wave, work, and prior-recovery evidence together."""

    wave = SimpleNamespace(wave_id="materialized-wave")
    intent = SimpleNamespace(run_id="materialized-run")
    run = SimpleNamespace(run_id="materialized-run")
    claim = SimpleNamespace(ordinal=0)
    outcome = SimpleNamespace(ordinal=0)
    logical = SimpleNamespace(successor_wave_id="materialized-wave")
    rollback = SimpleNamespace(successor_wave_id="materialized-wave")
    session = _ReadSession(
        _Result(wave),
        _Result(None),
        _Result(values=(intent,)),
        _Result(values=(run,)),
        _Result(values=(claim,)),
        _Result(values=(outcome,)),
        _Result(logical),
        _Result(rollback),
    )
    worker_events = AsyncMock(return_value=(0,))
    monkeypatch.setattr(runtime, "_worker_start_event_ordinals", worker_events)

    snapshot = await runtime._load_snapshot(
        session,
        "materialized-wave",
        lock_rows=True,
    )

    assert snapshot == runtime._MaterializedDatabaseSnapshot(
        wave=wave,
        intents=(intent,),
        runs=(run,),
        claims=(claim,),
        outcomes=(outcome,),
        worker_start_event_ordinals=(0,),
        logical_supersession=logical,
        admission_rollback=rollback,
    )
    worker_events.assert_awaited_once_with(session, (intent,))
    assert len(session.statements) == 8
    assert all("FOR UPDATE" in str(statement) for statement in session.statements)


@pytest.mark.parametrize(
    ("template", "desired_template"),
    (
        ({"spec": None}, {"spec": {}}),
        ({"spec": {"containers": {}}}, {"spec": {"containers": []}}),
        ({"spec": {"containers": ["not-a-container"]}}, {"spec": {"containers": []}}),
    ),
)
def test_materialized_job_template_normalization_preserves_malformed_inputs(
    template,
    desired_template,
):
    """V5 Job evidence must not normalize malformed Kubernetes templates."""

    assert (
        kubernetes_attestation._normalize_kubernetes_defaulted_template(
            template,
            desired_template,
        )
        is template
    )


@pytest.mark.parametrize(
    ("volumes", "desired_volumes"),
    (
        ({}, []),
        ([{"name": "worker"}], []),
        (["not-a-volume"], [{"name": "worker"}]),
    ),
)
def test_materialized_job_volume_normalization_preserves_noncanonical_shapes(
    volumes,
    desired_volumes,
):
    """A V5 Job receipt only permits the one bounded default-mode rewrite."""

    assert kubernetes_attestation._normalize_kubernetes_defaulted_volumes(
        volumes,
        desired_volumes,
    ) is volumes


@pytest.mark.parametrize("environment", ({}, ["not-an-entry"]))
def test_materialized_job_environment_normalization_preserves_noncanonical_shapes(
    environment,
):
    """Malformed environment entries remain visible to the V5 Job comparator."""

    normalized = kubernetes_attestation._normalize_kubernetes_defaulted_environment(
        environment
    )

    assert normalized == environment
    if not isinstance(environment, list):
        assert normalized is environment
