"""Fail-closed exact-wave claims at the PTG function boundary."""

from __future__ import annotations

import types
from unittest.mock import AsyncMock

import pytest

from process import ptg_control
from process.ptg_wave_claims import _advance_released_wave_for_rejection


_WAVE = "1" * 64
_MANIFEST = "2" * 64
_JOBS = "3" * 64
_CONFIG = "4" * 64
_IMAGE_DIGEST = "5" * 64
_RUNTIME = "sha256:" + "6" * 64
_JOB_ID = "ptg_start_" + "7" * 64
_ATTEMPT_TOKEN = "8" * 32


def _wave_params() -> dict[str, str]:
    return {
        "_wave_id": "wave-unit",
        "_wave_digest": _WAVE,
        "_wave_job_id": _JOB_ID,
        "_expected_queue": f"arq:PTGSmall:wave:{_WAVE}",
        "_expected_worker_class": "process.PTGSmall",
    }


def _set_wave_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    environment_field_map = {
        "HLTHPRT_PTG_WAVE_DIGEST": _WAVE,
        "HLTHPRT_ACTIVE_WORKER_QUEUE": f"arq:PTGSmall:wave:{_WAVE}",
        "HLTHPRT_ACTIVE_WORKER_CLASS": "process.PTGSmall",
        "HLTHPRT_PTG_WAVE_SLOT_INDEX": "7",
        "HLTHPRT_PTG_WAVE_POD_UID": "pod-unit-7",
        "HLTHPRT_PTG_WAVE_REDIS_MANIFEST_DIGEST": _MANIFEST,
        "HLTHPRT_PTG_WAVE_JOBS_DIGEST": _JOBS,
        "HLTHPRT_PTG_WAVE_JOB_COUNT": "3586",
        "HLTHPRT_PTG_WAVE_CONFIG_IDENTITY": _CONFIG,
        "HLTHPRT_PTG_WAVE_MANIFEST_IDENTITY": _MANIFEST,
        "HLTHPRT_PTG_WAVE_IMAGE_IDENTITY": (
            f"registry.example.invalid/worker@sha256:{_IMAGE_DIGEST}"
        ),
        "HLTHPRT_PTG_WAVE_RUNTIME_IMAGE_IDENTITY": _RUNTIME,
    }
    for name, value in environment_field_map.items():
        monkeypatch.setenv(name, value)


@pytest.mark.asyncio
async def test_wave_claim_binds_arq_context_and_exact_runtime_identity(monkeypatch):
    _set_wave_environment(monkeypatch)
    calls = []

    async def fake_claim(**values):
        calls.append(values)

    monkeypatch.setattr(ptg_control, "claim_wave_job_start", fake_claim)
    await ptg_control._claim_exact_wave_worker_start(
        {"job_id": _JOB_ID},
        _wave_params(),
        run_id="run-unit",
        claim_attempt_token=_ATTEMPT_TOKEN,
    )

    assert calls == [{
        "wave_id": "wave-unit",
        "run_id": "run-unit",
        "job_id": _JOB_ID,
        "slot": 7,
        "pod_uid": "pod-unit-7",
        "pinned_image_reference": (
            f"registry.example.invalid/worker@sha256:{_IMAGE_DIGEST}"
        ),
        "pinned_image_digest": _IMAGE_DIGEST,
        "runtime_image_identity": _RUNTIME,
        "config_identity": _CONFIG,
        "manifest_identity": _MANIFEST,
        "claim_attempt_token": _ATTEMPT_TOKEN,
    }]


@pytest.mark.asyncio
async def test_wave_claim_refuses_wrong_arq_job_before_db_claim(monkeypatch):
    _set_wave_environment(monkeypatch)

    async def unexpected_claim(**values):  # pragma: no cover - safety assertion
        raise AssertionError(values)

    monkeypatch.setattr(ptg_control, "claim_wave_job_start", unexpected_claim)
    with pytest.raises(RuntimeError, match="ARQ job identity"):
        await ptg_control._claim_exact_wave_worker_start(
            {"job_id": "different-job"},
            _wave_params(),
            run_id="run-unit",
            claim_attempt_token=_ATTEMPT_TOKEN,
        )


@pytest.mark.asyncio
async def test_wave_pod_refuses_an_ordinary_task_without_wave_identity(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG_WAVE_DIGEST", _WAVE)
    with pytest.raises(RuntimeError, match="payload identity is incomplete"):
        await ptg_control._claim_exact_wave_worker_start(
            {"job_id": "ordinary-job"},
            {},
            run_id="run-unit",
            claim_attempt_token=_ATTEMPT_TOKEN,
        )


@pytest.mark.asyncio
async def test_ordinary_worker_task_does_not_create_a_wave_claim(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG_WAVE_DIGEST", raising=False)

    async def unexpected_claim(**values):  # pragma: no cover - safety assertion
        raise AssertionError(values)

    monkeypatch.setattr(ptg_control, "claim_wave_job_start", unexpected_claim)
    await ptg_control._claim_exact_wave_worker_start(
        {},
        {},
        run_id="run-unit",
        claim_attempt_token=_ATTEMPT_TOKEN,
    )


@pytest.mark.asyncio
async def test_ptg_control_claims_wave_before_source_attempt_admission(monkeypatch):
    calls = []

    async def fake_claim(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("claim", run_id))

    async def fake_admission(task, *, run_id, attempt_id):
        calls.append(("source-admission", run_id))
        return {"status": "skipped", "run_id": run_id, "reason": "unit-stop"}

    async def fake_terminal(run_id, *, reason, error=None):
        assert error is None
        calls.append(("terminal", run_id, reason))

    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", fake_claim)
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", fake_admission)
    monkeypatch.setattr(ptg_control, "_mark_exact_wave_preexecution_failure", fake_terminal)
    admission_result = await ptg_control.ptg_control_start(
        {"job_id": _JOB_ID},
        {"run_id": "run-unit", "params": _wave_params()},
    )

    assert admission_result["reason"] == "unit-stop"
    assert calls == [
        ("claim", "run-unit"),
        ("source-admission", "run-unit"),
        ("terminal", "run-unit", "unit-stop"),
    ]


@pytest.mark.asyncio
async def test_valid_claim_rejection_is_durable_but_does_not_start_work(monkeypatch):
    calls = []
    failure = RuntimeError("synthetic claim rejection")

    async def reject_claim(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("claim", run_id))
        raise failure

    async def rejected(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("rejected", run_id))
        return types.SimpleNamespace(status="rejected", same_attempt=True)

    async def unexpected_admission(*_args, **_kwargs):  # pragma: no cover
        raise AssertionError("source admission must not follow a rejected claim")

    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", reject_claim)
    monkeypatch.setattr(ptg_control, "_reconcile_exact_wave_claim_exception", rejected)
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", unexpected_admission)

    with pytest.raises(RuntimeError, match="synthetic claim rejection"):
        await ptg_control.ptg_control_start(
            {"job_id": _JOB_ID},
            {"run_id": "run-unit", "params": _wave_params()},
        )

    assert calls == [
        ("claim", "run-unit"),
        ("rejected", "run-unit"),
    ]


@pytest.mark.asyncio
async def test_same_attempt_commit_ack_recovery_continues_once_without_reclaiming(monkeypatch):
    calls = []

    async def ambiguous_claim(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("claim", run_id, claim_attempt_token))
        raise RuntimeError("commit acknowledgement lost")

    async def recovered(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("reconcile", run_id, claim_attempt_token))
        return types.SimpleNamespace(status="claimed", same_attempt=True)

    async def stop_before_ptg(task, *, run_id, attempt_id):
        calls.append(("source-admission", run_id))
        return {"status": "skipped", "run_id": run_id, "reason": "unit-stop"}

    async def terminal(run_id, *, reason, error=None):
        assert error is None
        calls.append(("terminal", run_id, reason))

    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", ambiguous_claim)
    monkeypatch.setattr(ptg_control, "_reconcile_exact_wave_claim_exception", recovered)
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", stop_before_ptg)
    monkeypatch.setattr(ptg_control, "_mark_exact_wave_preexecution_failure", terminal)

    claim_result = await ptg_control.ptg_control_start(
        {"job_id": _JOB_ID},
        {"run_id": "run-unit", "params": _wave_params()},
    )

    assert claim_result["reason"] == "unit-stop"
    assert [event_entry[0] for event_entry in calls] == [
        "claim", "reconcile", "source-admission", "terminal",
    ]
    assert calls[0][2] == calls[1][2]


@pytest.mark.asyncio
async def test_later_replay_cannot_use_a_different_attempt_token_to_run_ptg(monkeypatch):
    calls = []

    async def duplicate_claim(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("claim", claim_attempt_token))
        raise RuntimeError("duplicate worker delivery")

    async def prior_attempt(ctx, params, *, run_id, claim_attempt_token):
        calls.append(("reconcile", claim_attempt_token))
        return types.SimpleNamespace(status="claimed", same_attempt=False)

    async def unexpected_admission(*args, **kwargs):  # pragma: no cover - safety assertion
        raise AssertionError("replayed worker must not start PTG")

    monkeypatch.setattr(ptg_control, "_claim_exact_wave_worker_start", duplicate_claim)
    monkeypatch.setattr(ptg_control, "_reconcile_exact_wave_claim_exception", prior_attempt)
    monkeypatch.setattr(ptg_control, "guard_ptg_worker_start", unexpected_admission)

    with pytest.raises(RuntimeError, match="duplicate worker delivery"):
        await ptg_control.ptg_control_start(
            {"job_id": _JOB_ID},
            {"run_id": "run-unit", "params": _wave_params()},
        )

    assert [item[0] for item in calls] == ["claim", "reconcile"]
    assert calls[0][1] == calls[1][1]


@pytest.mark.asyncio
async def test_rejected_claim_does_not_preempt_redis_release_receipt_path():
    wave = types.SimpleNamespace(
        wave_id="wave-unit", state="redis_releasing", state_version=4,
    )
    session = types.SimpleNamespace(execute=AsyncMock())

    await _advance_released_wave_for_rejection(session, wave)

    assert wave.state == "redis_releasing"
    session.execute.assert_not_awaited()
