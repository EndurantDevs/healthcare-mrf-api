"""Facade dispatch, lifecycle, and delegation contracts for the wave controller."""

from __future__ import annotations

import asyncio
import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import ptg_wave_controller as controller


class _Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class _Session:
    def __init__(self, *results):
        self.results = list(results)

    async def execute(self, _statement):
        assert self.results
        return self.results.pop(0)


class _Context:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": "1" * 64,
        "state": "released",
        "intent_count": 2,
        "uncertainty_resume_state": None,
        "linkage_ack_digest": None,
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_manifest_identity": None,
        "kubernetes_config_identity": "2" * 64,
        "pinned_image_reference": "registry/unit@sha256:" + "3" * 64,
        "runtime_image_identity": "sha256:" + "4" * 64,
        "enqueue_time_ms": 1234,
        "jobs_digest": "5" * 64,
        "manifest_digest": "6" * 64,
        "protocol_identity": "protocol-v1",
        "serializer_identity": "serializer-v1",
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _bundle(**overrides):
    return controller.PTGWaveBundle(wave=_wave(**overrides), intents=())


def test_enablement_is_explicit_and_case_insensitive():
    for value in ("1", "true", "TRUE", " yes ", "on"):
        assert controller.is_controller_enabled({controller.CONTROLLER_ENABLED_ENV: value})
    for value in (None, "", "0", "false", "enabled"):
        assert not controller.is_controller_enabled(
            {} if value is None else {controller.CONTROLLER_ENABLED_ENV: value}
        )


def test_runtime_config_is_validated_by_the_canonical_builder(monkeypatch):
    monkeypatch.setenv(controller.WORKER_IMAGE_ENV, " registry/unit@sha256:" + "1" * 64 + " ")
    monkeypatch.setenv(controller.RUNTIME_IMAGE_ENV, " sha256:" + "2" * 64 + " ")
    builder = Mock(return_value={})
    monkeypatch.setattr(controller, "build_ptg_wave_job", builder)
    image, runtime = controller._controller_runtime_config()
    assert image.endswith("1" * 64)
    assert runtime == "sha256:" + "2" * 64
    assert builder.call_args.kwargs["barrier_factory"] == controller.BARRIER_FACTORY


@pytest.mark.asyncio
async def test_capacity_owner_load_handles_idle_ambiguous_incomplete_and_exact(monkeypatch):
    sessions = [
        _Session(_Result([])),
        _Session(_Result([_wave(), _wave(wave_id="other")])) ,
        _Session(_Result([_wave(intent_count=2)]), _Result([types.SimpleNamespace(ordinal=0)])),
        _Session(
            _Result([_wave(intent_count=2)]),
            _Result([types.SimpleNamespace(ordinal=0), types.SimpleNamespace(ordinal=1)]),
        ),
    ]

    monkeypatch.setattr(controller.db, "session", lambda: _Context(sessions.pop(0)))
    assert await controller.load_capacity_owning_wave() is None
    with pytest.raises(controller.PTGWaveStateConflict, match="ambiguous"):
        await controller.load_capacity_owning_wave()
    with pytest.raises(controller.PTGWaveStateConflict, match="incomplete"):
        await controller.load_capacity_owning_wave()
    bundle = await controller.load_capacity_owning_wave()
    assert [intent.ordinal for intent in bundle.intents] == [0, 1]


def test_restore_manifest_rebuilds_jobs_and_optionally_binds_runtime(monkeypatch):
    intents = (
        types.SimpleNamespace(
            ordinal=0,
            job_id="job-0",
            serialized_job=b"payload",
            serialized_job_digest="7" * 64,
        ),
    )
    wave = _wave(kubernetes_manifest_identity=None)
    bundle = controller.PTGWaveBundle(wave=wave, intents=intents)
    restored = object()
    restore = Mock(return_value=restored)
    bind = Mock(return_value="bound")
    monkeypatch.setattr(controller, "restore_ptg_small_wave_manifest", restore)
    monkeypatch.setattr(controller, "bind_ptg_small_wave_runtime_identity", bind)
    assert controller.restore_wave_manifest(bundle) is restored
    assert restore.call_args.args[0][0].serialized_job == b"payload"
    bind.assert_not_called()

    wave.kubernetes_manifest_identity = "8" * 64
    assert controller.restore_wave_manifest(bundle) == "bound"
    runtime = bind.call_args.args[1]
    assert runtime.kubernetes_manifest_identity == "8" * 64
    assert runtime.runtime_image_identity == wave.runtime_image_identity


def _install_dispatch(monkeypatch, bundle, *, recovery=None, preclaim=False, terminal=False):
    monkeypatch.setattr(controller, "load_capacity_owning_wave", AsyncMock(return_value=bundle))
    monkeypatch.setattr(controller, "read_only_recovery_plan", Mock(return_value=recovery))
    monkeypatch.setattr(controller, "restore_wave_manifest", Mock(return_value=object()))
    monkeypatch.setattr(controller, "_reconcile_read_only_recovery", AsyncMock(return_value="recovered"))
    monkeypatch.setattr(controller, "_reconcile_uncertain", AsyncMock(return_value="uncertain"))
    monkeypatch.setattr(controller, "_materialize", AsyncMock())
    monkeypatch.setattr(controller, "_post_job_once", AsyncMock())
    monkeypatch.setattr(controller, "_reconcile_slots", AsyncMock())
    monkeypatch.setattr(controller, "_reconcile_redis_release", AsyncMock())
    monkeypatch.setattr(controller, "_maybe_snapshot_preclaim_failure", AsyncMock(return_value=preclaim))
    monkeypatch.setattr(controller, "_all_wave_runs_terminal", AsyncMock(return_value=terminal))
    monkeypatch.setattr(controller, "snapshot_terminal_outcomes", AsyncMock())
    monkeypatch.setattr(controller, "begin_terminalizing", AsyncMock())
    monkeypatch.setattr(controller, "_requires_early_kubernetes_stop", Mock(return_value=False))
    monkeypatch.setattr(controller, "_reconcile_kubernetes_delete", AsyncMock())
    monkeypatch.setattr(controller, "_persist_terminal_proof", AsyncMock())
    monkeypatch.setattr(controller, "_reconcile_cleanup", AsyncMock())


@pytest.mark.asyncio
async def test_dispatch_handles_idle_recovery_and_uncertainty(monkeypatch):
    _install_dispatch(monkeypatch, None)
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "idle"
    bundle = _bundle(state="released")
    recovery = object()
    _install_dispatch(monkeypatch, bundle, recovery=recovery)
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "recovered"
    bundle.wave.state = "uncertain"
    controller.read_only_recovery_plan.return_value = None
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "uncertain"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state_name", "expected"),
    [
        ("admitted", "materialized"),
        ("materializing", "kubernetes-post-started"),
        ("slots_waiting", "slots-waiting"),
        ("redis_releasing", "redis-reconciling"),
        ("released", "released"),
        ("executing", "executing"),
        ("awaiting_linkage", "awaiting-linkage"),
        ("terminalizing", "terminal-proof-persisted"),
        ("cleaning", "cleaning"),
    ],
)
async def test_dispatch_covers_each_capacity_owning_state(monkeypatch, state_name, expected):
    bundle = _bundle(state=state_name)
    _install_dispatch(monkeypatch, bundle)
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == expected


@pytest.mark.asyncio
async def test_dispatch_handles_preclaim_terminal_linkage_and_early_delete(monkeypatch):
    bundle = _bundle(state="slots_waiting")
    _install_dispatch(monkeypatch, bundle, preclaim=True)
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "preclaim-failure-dead-lettered"

    bundle.wave.state = "executing"
    controller._maybe_snapshot_preclaim_failure.return_value = False
    controller._all_wave_runs_terminal.return_value = True
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "outcomes-snapshotted"

    bundle.wave.state = "awaiting_linkage"
    bundle.wave.linkage_ack_digest = "2" * 64
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "terminalizing"

    bundle.wave.state = "terminalizing"
    controller._requires_early_kubernetes_stop.return_value = True
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "failure-kubernetes-stopping"

    bundle.wave.kubernetes_delete_evidence_digest = "3" * 64
    assert await controller.reconcile_ptg_wave_once(
        object(), image="image", runtime_image="runtime",
    ) == "terminal-proof-persisted"

    bundle.wave.state = "unsupported"
    with pytest.raises(controller.PTGWaveStateConflict, match="unsupported"):
        await controller.reconcile_ptg_wave_once(
            object(), image="image", runtime_image="runtime",
        )


@pytest.mark.asyncio
async def test_facade_helpers_delegate_to_owner_modules(monkeypatch):
    bundle = _bundle()
    manifest = object()
    redis = object()
    recovery = object()
    operation = types.SimpleNamespace(
        materialize_wave=AsyncMock(),
        post_wave_job_once=AsyncMock(),
        reconcile_slots=AsyncMock(),
        reconcile_redis_release=AsyncMock(),
        reconcile_uncertain=AsyncMock(return_value="uncertain"),
        reconcile_read_only_recovery=AsyncMock(return_value="recovery"),
    )
    terminal_owner = types.SimpleNamespace(
        persist_terminal_proof=AsyncMock(),
        reconcile_cleanup=AsyncMock(),
        should_snapshot_preclaim_failure=AsyncMock(return_value=True),
        has_terminal_job_failure=Mock(return_value=True),
        reconcile_kubernetes_delete=AsyncMock(),
        observe_kubernetes_delete_absence=AsyncMock(return_value={"absent": True}),
        reconcile_redis_cleanup_get_only=AsyncMock(),
        failure_redis_attestation_digest=Mock(return_value="a" * 64),
    )
    isolation = types.SimpleNamespace(
        require_ptg_only_idle=AsyncMock(),
        has_only_terminal_wave_runs=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(controller, "_controller_operations", operation)
    monkeypatch.setattr(controller, "_controller_terminal", terminal_owner)
    monkeypatch.setattr(controller, "_controller_isolation", isolation)

    await controller._materialize(bundle, redis, image="image", runtime_image="runtime")
    await controller._post_job_once(bundle)
    await controller._reconcile_slots(bundle, manifest, redis)
    await controller._reconcile_redis_release(bundle, manifest, redis, mutate=True)
    assert await controller._reconcile_uncertain(bundle, redis) == "uncertain"
    assert await controller._reconcile_read_only_recovery(bundle, redis, recovery) == "recovery"
    await controller._persist_terminal_proof(bundle, manifest, redis)
    await controller._reconcile_cleanup(bundle, manifest, redis)
    assert await controller._should_snapshot_preclaim_failure(bundle, manifest, redis)
    assert controller._has_terminal_job_failure({})
    await controller._reconcile_kubernetes_delete(bundle, expected_state="cleaning")
    assert await controller._observe_kubernetes_delete_absence(bundle, "ticket") == {"absent": True}
    await controller._reconcile_redis_cleanup_get_only(bundle, manifest, redis, "ticket")
    assert controller._failure_redis_attestation_digest(bundle.wave) == "a" * 64
    await controller._require_ptg_only_idle(bundle, redis)
    assert await controller._has_only_terminal_wave_runs(bundle)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    [None, controller.PTGWaveControllerHold("wait"), RuntimeError("failure")],
)
async def test_controller_loop_continues_after_success_hold_and_error(monkeypatch, outcome):
    reconcile = AsyncMock()
    if outcome is not None:
        reconcile.side_effect = outcome
    monkeypatch.setattr(controller, "reconcile_ptg_wave_once", reconcile)
    monkeypatch.setattr(controller.asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError()))
    monkeypatch.setenv("HLTHPRT_PTG_WAVE_CONTROLLER_INTERVAL_SECONDS", "0")
    with pytest.raises(asyncio.CancelledError):
        await controller.run_ptg_wave_controller(
            object(), image="image", runtime_image="runtime",
        )


@pytest.mark.asyncio
async def test_controller_loop_propagates_reconciliation_cancellation(monkeypatch):
    monkeypatch.setattr(
        controller,
        "reconcile_ptg_wave_once",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    with pytest.raises(asyncio.CancelledError):
        await controller.run_ptg_wave_controller(
            object(), image="image", runtime_image="runtime",
        )


@pytest.mark.asyncio
async def test_start_controller_is_explicit_and_saves_task_and_pool(monkeypatch):
    app = types.SimpleNamespace(ctx=types.SimpleNamespace())
    monkeypatch.setattr(controller, "controller_enabled", Mock(return_value=False))
    await controller.start_ptg_wave_controller(app)
    assert not hasattr(app.ctx, "ptg_wave_controller_task")

    redis = object()
    task = object()
    monkeypatch.setattr(controller, "controller_enabled", Mock(return_value=True))
    monkeypatch.setattr(controller, "_controller_runtime_config", Mock(return_value=("image", "runtime")))
    monkeypatch.setattr(controller, "build_redis_settings", Mock(return_value={"redis": True}))
    monkeypatch.setattr(controller, "create_pool", AsyncMock(return_value=redis))
    monkeypatch.setattr(controller.asyncio, "create_task", Mock(return_value=task))
    await controller.start_ptg_wave_controller(app)
    assert app.ctx.ptg_wave_redis is redis
    assert app.ctx.ptg_wave_controller_task is task
    coroutine = controller.asyncio.create_task.call_args.args[0]
    coroutine.close()


class _Task:
    def __init__(self):
        self.cancelled = False

    def cancel(self):
        self.cancelled = True

    def __await__(self):
        async def cancelled():
            raise asyncio.CancelledError
        return cancelled().__await__()


@pytest.mark.asyncio
@pytest.mark.parametrize("async_close", [False, True])
async def test_stop_controller_cancels_task_and_closes_sync_or_async_pool(async_close):
    task = _Task()
    closed_values = []
    if async_close:
        async def close():
            closed_values.append(True)
    else:
        def close():
            closed_values.append(True)
    app = types.SimpleNamespace(
        ctx=types.SimpleNamespace(
            ptg_wave_controller_task=task,
            ptg_wave_redis=types.SimpleNamespace(close=close),
        ),
    )
    await controller.stop_ptg_wave_controller(app)
    assert task.cancelled
    assert closed_values == [True]

    await controller.stop_ptg_wave_controller(
        types.SimpleNamespace(ctx=types.SimpleNamespace()),
    )
    await controller.stop_ptg_wave_controller(
        types.SimpleNamespace(ctx=types.SimpleNamespace(ptg_wave_redis=object())),
    )
