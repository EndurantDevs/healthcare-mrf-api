from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports


RUN_ID = "run_missing_worker"
ATTEMPT_ID = f"{RUN_ID}:attempt"
ATTEMPT_STARTED_AT = "2026-08-27T22:24:15.779700+00:00"
HEARTBEAT_AT = dt.datetime(2026, 8, 27, 22, 30, 38, 663966)
EXPECTED_BODY = {
    "expected_importer": "plan-pricing-projection",
    "expected_status": "running",
    "expected_heartbeat_at": "2026-08-27T22:30:38.663966+00:00",
    "expected_attempt_id": ATTEMPT_ID,
    "expected_attempt_started_at": ATTEMPT_STARTED_AT,
}


def _run(importer: str = "plan-pricing-projection") -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "engine": "healthcare-mrf-api",
        "importer": importer,
        "family": "mrf",
        "status": "running",
        "phase_detail": "build running",
        "params": {},
        "created_at": dt.datetime(2026, 8, 27, 22, 13),
        "started_at": dt.datetime(2026, 8, 27, 22, 24, 15, 779734),
        "heartbeat_at": HEARTBEAT_AT,
        "progress": {
            "attempt_id": ATTEMPT_ID,
            "attempt_started_at": ATTEMPT_STARTED_AT,
        },
        "metrics": {
            "queue": "arq:PTGCandidateAudit",
            "worker_class": "process.PTGCandidateAudit",
            "job_id": f"plan_pricing_{importer.removeprefix('plan-pricing-')}_{RUN_ID}",
        },
    }


class _Row:
    def __init__(self, values: dict[str, object]):
        self._mapping = values


class _Connection:
    def __init__(self, run: dict[str, object], *, rowcount: int = 1):
        self.run = run
        self.rowcount = rowcount
        self.updates = []

    async def all(self, _statement):
        return [_Row(self.run)]

    async def status(self, statement):
        self.updates.append(statement)
        return self.rowcount


def _install_reconciliation_dependencies(
    monkeypatch,
    run: dict[str, object],
    *,
    rowcount: int = 1,
    kubernetes: dict[str, object] | None = None,
    arq: dict[str, object] | None = None,
) -> _Connection:
    connection = _Connection(run, rowcount=rowcount)

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    monkeypatch.setattr(
        control_imports,
        "acquire_control_run_worker_action_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        control_imports,
        "exact_worker_presence",
        lambda _payload: kubernetes
        or {"enabled": True, "job_count": 0, "pod_count": 0},
    )
    monkeypatch.setattr(
        control_imports,
        "_arq_worker_presence",
        AsyncMock(
            return_value=arq
            or {
                "queue_member": False,
                "job": False,
                "retry": False,
                "in_progress": False,
                "result": False,
            }
        ),
    )
    monkeypatch.setattr(
        control_imports,
        "utc_now",
        lambda: dt.datetime(2026, 8, 28, 8, 0),
    )
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda *_args, **_kwargs: None)
    return connection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "importer",
    ("plan-pricing-projection", "plan-pricing-prewarm"),
)
async def test_reconcile_missing_worker_terminalizes_supported_exact_attempt(
    monkeypatch,
    importer,
):
    run_by_field = _run(importer)
    body_by_field = {**EXPECTED_BODY, "expected_importer": importer}
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    receipt_by_field = await control_imports.reconcile_stale_worker_failure(
        RUN_ID,
        body_by_field,
    )

    assert receipt_by_field == {
        "run_id": RUN_ID,
        "importer": importer,
        "status": "failed",
        "reconciled": True,
        "error_code": "worker_lifecycle_lost",
        "attempt_id": ATTEMPT_ID,
        "attempt_started_at": ATTEMPT_STARTED_AT,
    }
    assert len(connection.updates) == 1
    update_values_by_name = connection.updates[0].compile().params
    error_by_field = update_values_by_name["error"]
    assert error_by_field["code"] == "worker_lifecycle_lost"
    assert error_by_field["retryable"] is False
    assert (
        error_by_field["observed_heartbeat_at"]
        == EXPECTED_BODY["expected_heartbeat_at"]
    )
    assert update_values_by_name["progress"]["attempt_id"] == ATTEMPT_ID
    assert (
        update_values_by_name["progress"]["attempt_started_at"]
        == ATTEMPT_STARTED_AT
    )
    control_imports.acquire_control_run_worker_action_lock.assert_awaited_once_with(
        connection,
        RUN_ID,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "progress",
    ({}, {"attempt_id": ATTEMPT_ID}, {"attempt_started_at": ATTEMPT_STARTED_AT}),
)
async def test_reconcile_missing_worker_requires_complete_attempt_pair(
    monkeypatch,
    progress,
):
    run_by_field = {**_run(), "progress": progress}
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="complete worker attempt identity",
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert connection.updates == []
    control_imports._arq_worker_presence.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kubernetes", "arq", "message"),
    (
        ({"enabled": True, "job_count": 1, "pod_count": 0}, None, "Kubernetes"),
        ({"enabled": True, "job_count": 0, "pod_count": 1}, None, "Kubernetes"),
        (None, {"queue_member": True}, "ARQ"),
        (None, {"job": True}, "ARQ"),
        (None, {"retry": True}, "ARQ"),
        (None, {"in_progress": True}, "ARQ"),
        (None, {"result": True}, "ARQ"),
    ),
)
async def test_reconcile_missing_worker_rejects_live_worker_residue(
    monkeypatch,
    kubernetes,
    arq,
    message,
):
    connection = _install_reconciliation_dependencies(
        monkeypatch,
        _run(),
        kubernetes=kubernetes,
        arq=arq,
    )

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match=message,
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert connection.updates == []


@pytest.mark.asyncio
async def test_reconcile_missing_worker_cas_loss_preserves_newer_attempt(
    monkeypatch,
):
    connection = _install_reconciliation_dependencies(
        monkeypatch,
        _run(),
        rowcount=0,
    )

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="changed during reconciliation",
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert len(connection.updates) == 1


@pytest.mark.asyncio
async def test_reconcile_missing_worker_is_idempotent(monkeypatch):
    run_by_field = {
        **_run(),
        "status": "failed",
        "error": {
            "code": "worker_lifecycle_lost",
            "observed_heartbeat_at": "2026-08-27T22:30:38.663966Z",
            "attempt_id": ATTEMPT_ID,
            "attempt_started_at": ATTEMPT_STARTED_AT,
        },
    }
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    receipt_by_field = await control_imports.reconcile_stale_worker_failure(
        RUN_ID,
        EXPECTED_BODY,
    )

    assert receipt_by_field["reconciled"] is False
    assert receipt_by_field["status"] == "failed"
    assert connection.updates == []
    control_imports._arq_worker_presence.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    (
        {},
        {**EXPECTED_BODY, "extra": True},
        {**EXPECTED_BODY, "expected_status": "starting"},
        {**EXPECTED_BODY, "expected_attempt_id": ""},
        {**EXPECTED_BODY, "expected_heartbeat_at": "not-a-timestamp"},
        {**EXPECTED_BODY, "expected_importer": []},
        {**EXPECTED_BODY, "expected_importer": {}},
    ),
)
async def test_reconcile_missing_worker_rejects_inexact_body(payload):
    with pytest.raises(ValueError):
        await control_imports.reconcile_stale_worker_failure(RUN_ID, payload)


@pytest.mark.asyncio
async def test_reconcile_missing_worker_rejects_mismatched_idempotent_progress(
    monkeypatch,
):
    run_by_field = {
        **_run(),
        "status": "failed",
        "progress": {
            "attempt_id": f"{RUN_ID}:newer",
            "attempt_started_at": "2026-08-28T08:00:00+00:00",
        },
        "error": {
            "code": "worker_lifecycle_lost",
            "observed_heartbeat_at": EXPECTED_BODY["expected_heartbeat_at"],
            "attempt_id": ATTEMPT_ID,
            "attempt_started_at": ATTEMPT_STARTED_AT,
        },
    }
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="status changed",
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert connection.updates == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "field_value", "message"),
    (
        ("importer", "plan-pricing-prewarm", "importer changed"),
        ("status", "starting", "status changed"),
        (
            "progress",
            {
                "attempt_id": f"{RUN_ID}:newer",
                "attempt_started_at": ATTEMPT_STARTED_AT,
            },
            "worker attempt changed",
        ),
        (
            "heartbeat_at",
            HEARTBEAT_AT + dt.timedelta(seconds=1),
            "heartbeat changed",
        ),
        (
            "heartbeat_at",
            dt.datetime(2026, 8, 28, 7, 59, 30),
            "heartbeat is not stale",
        ),
    ),
)
async def test_reconcile_missing_worker_rejects_identity_drift_before_probes(
    monkeypatch,
    field_name,
    field_value,
    message,
):
    connection = _install_reconciliation_dependencies(
        monkeypatch,
        {**_run(), field_name: field_value},
    )
    kubernetes_probe = Mock(side_effect=AssertionError("worker probe ran"))
    monkeypatch.setattr(
        control_imports,
        "exact_worker_presence",
        kubernetes_probe,
    )
    request_by_field = EXPECTED_BODY
    if message == "heartbeat is not stale":
        request_by_field = {
            **EXPECTED_BODY,
            "expected_heartbeat_at": field_value.replace(tzinfo=dt.UTC).isoformat(),
        }

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match=message,
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            request_by_field,
        )

    assert connection.updates == []
    kubernetes_probe.assert_not_called()
    control_imports._arq_worker_presence.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_missing_worker_rejects_persisted_queue_mismatch(
    monkeypatch,
):
    run_by_field = _run()
    run_by_field["metrics"] = {
        **run_by_field["metrics"],
        "queue": "arq:PTGNormal",
    }
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="queue conflicts with importer adapter",
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert connection.updates == []


@pytest.mark.asyncio
async def test_reconcile_missing_worker_rejects_persisted_job_id_mismatch(
    monkeypatch,
):
    run_by_field = _run()
    run_by_field["metrics"] = {
        **run_by_field["metrics"],
        "job_id": "plan_pricing_projection_run_different",
    }
    connection = _install_reconciliation_dependencies(monkeypatch, run_by_field)

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="job ID conflicts with importer adapter",
    ):
        await control_imports.reconcile_stale_worker_failure(
            RUN_ID,
            EXPECTED_BODY,
        )

    assert connection.updates == []
