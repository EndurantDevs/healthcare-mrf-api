

"""Direct terminal, cleanup, and deletion controller branch contracts."""


from __future__ import annotations


import types


from unittest.mock import AsyncMock, Mock


import pytest


from process import ptg_wave_controller_terminal as terminal


class _Hold(RuntimeError):
    pass


class _StateConflict(RuntimeError):
    pass


class _ContractError(RuntimeError):
    pass


class _FailureConflict(RuntimeError):
    pass


def _wave(**overrides):
    fields_by_field = {
        "wave_id": "wave-unit",
        "wave_digest": "1" * 64,
        "state": "executing",
        "failure_receipt": None,
        "failure_receipt_digest": None,
        "kubernetes_delete_evidence": {"absent": True},
        "kubernetes_delete_evidence_digest": None,
        "kubernetes_delete_ticket": "delete-ticket",
        "redis_cleanup_evidence_digest": None,
        "kubernetes_ready_attestation_digest": "2" * 64,
        "kubernetes_manifest": {"kind": "Job"},
        "terminal_summary": None,
    }
    fields_by_field.update(overrides)
    return types.SimpleNamespace(**fields_by_field)


def _bundle(**overrides):
    return types.SimpleNamespace(wave=_wave(**overrides), intents=())


def _controller(**overrides):
    values_by_field = {
        "PTGWaveControllerHold": _Hold,
        "PTGWaveStateConflict": _StateConflict,
        "PTGWaveContractError": _ContractError,
        "PTGWaveFailureConflict": _FailureConflict,
        "_ticket": Mock(side_effect=lambda value: f"ticket-{value}"),
    }
    values_by_field.update(overrides)
    return types.SimpleNamespace(**values_by_field)


def _claimed_controller(*, job=object(), pods=None):
    if pods is None:
        pods = [object()] * 12
    return _controller(
        get_wave_job=Mock(return_value=job),
        list_wave_pods=Mock(return_value=pods),
        _initial_kubernetes_attestation=Mock(return_value=object()),
        attest_terminal_ptg_wave_kubernetes_objects=Mock(return_value=object()),
        attest_ptg_wave_pre_cleanup=AsyncMock(return_value=object()),
        _kubernetes_terminal_receipt=Mock(return_value={"kube": True}),
        _redis_terminal_receipt=Mock(return_value={"redis": True}),
        persist_terminal_evidence=AsyncMock(),
    )


def _redis_cleanup_controller(*, owner, failure):
    operation_by_field = {"owner": owner, "operation_ticket": "ticket"}
    return _controller(
        mark_redis_cleanup_started=AsyncMock(return_value=operation_by_field),
        _failure_redis_attestation_digest=Mock(return_value="a" * 64),
        cleanup_ptg_small_wave_unclaimed_failure_redis=AsyncMock(return_value=object()),
        attest_ptg_small_wave_unclaimed_failure_redis_post_cleanup=AsyncMock(
            return_value=object(),
        ),
        cleanup_ptg_small_wave_terminal_state=AsyncMock(return_value=object()),
        attest_ptg_wave_post_cleanup=AsyncMock(return_value=object()),
        _redis_cleanup_receipt=Mock(return_value={"cleanup": True}),
        record_redis_cleanup_absent=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )


def _preclaim_controller(*, job=object(), terminal_failure=True):
    return _controller(
        get_wave_job=Mock(return_value=job),
        _job_reports_terminal_failure=Mock(return_value=terminal_failure),
        list_wave_pods=Mock(return_value=[object()] * 12),
        _initial_kubernetes_attestation=Mock(return_value=object()),
        attest_preclaim_failure_ptg_wave_kubernetes_objects=Mock(
            return_value=types.SimpleNamespace(as_mapping=lambda: {"kube": True}),
        ),
    )


def _delete_controller(*, owner=True, permitted=True):
    return _controller(
        mark_kubernetes_delete_started=AsyncMock(
            return_value={
                "owner": owner,
                "delete_permitted": permitted,
                "job_uid": "job-uid",
                "operation_ticket": "ticket",
            },
        ),
        delete_wave_job=Mock(),
        _observe_kubernetes_delete_absence=AsyncMock(return_value={"absent": True}),
        record_kubernetes_delete_absent=AsyncMock(),
        mark_uncertain=AsyncMock(),
    )
