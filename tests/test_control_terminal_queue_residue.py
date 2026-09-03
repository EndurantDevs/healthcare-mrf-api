from __future__ import annotations

import json
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, call

import pytest
from sanic.exceptions import SanicException, Unauthorized

from api import control_imports, control_route_registration


RUN_ID = "run_terminal"
IMPORTER = "plan-pricing-projection"
QUEUE = "arq:PTGCandidateAudit"
JOB_ID = f"plan_pricing_projection_{RUN_ID}"
BODY = {"expected_importer": IMPORTER, "expected_status": "failed"}
KEYS = (
    f"arq:job:{JOB_ID}",
    f"arq:retry:{JOB_ID}",
    f"arq:in-progress:{JOB_ID}",
    f"arq:result:{JOB_ID}",
)


def _run(**overrides):
    run_by_field = {
        "run_id": RUN_ID,
        "importer": IMPORTER,
        "status": "failed",
        "metrics": {"queue": QUEUE, "job_id": JOB_ID},
    }
    run_by_field.update(overrides)
    return run_by_field


class _Row:
    def __init__(self, values):
        self._mapping = values


class _Connection:
    def __init__(self, run):
        self.run = run

    async def all(self, _statement):
        return [] if self.run is None else [_Row(self.run)]


def _install_dependencies(
    monkeypatch,
    run,
    *,
    queue_score=1.0,
    key_presence=(False, False, False, False),
    execute_result=(1,),
):
    connection = _Connection(run)

    @asynccontextmanager
    async def acquire():
        yield connection

    pipeline = Mock()
    pipeline.__aenter__ = AsyncMock(return_value=pipeline)
    pipeline.__aexit__ = AsyncMock(return_value=None)
    pipeline.watch = AsyncMock()
    pipeline.zscore = AsyncMock(return_value=queue_score)
    pipeline.exists = AsyncMock(side_effect=key_presence)
    pipeline.multi = Mock()
    pipeline.ping = Mock()
    pipeline.zrem = Mock()
    pipeline.execute = AsyncMock(return_value=list(execute_result))
    redis_pool = Mock()
    redis_pool.pipeline.return_value = pipeline
    redis_pool.aclose = AsyncMock()

    monkeypatch.setattr(control_imports.db, "acquire", acquire)
    monkeypatch.setattr(
        control_imports,
        "acquire_control_run_worker_action_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        control_imports,
        "create_pool",
        AsyncMock(return_value=redis_pool),
    )
    return connection, redis_pool, pipeline


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_removes_only_exact_member(monkeypatch):
    connection, redis_pool, pipeline = _install_dependencies(monkeypatch, _run())

    receipt = await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)

    assert receipt == {
        "run_id": RUN_ID,
        "importer": IMPORTER,
        "status": "failed",
        "queue": QUEUE,
        "job_id": JOB_ID,
        "residue_found": True,
        "removed": True,
        "already_absent": False,
        "evidence": {
            "queue_member": True,
            "job": False,
            "retry": False,
            "in_progress": False,
            "result": False,
        },
    }
    control_imports.acquire_control_run_worker_action_lock.assert_awaited_once_with(
        connection, RUN_ID
    )
    pipeline.watch.assert_awaited_once_with(QUEUE, *KEYS)
    assert pipeline.mock_calls == [
        call.__aenter__(),
        call.watch(QUEUE, *KEYS),
        call.zscore(QUEUE, JOB_ID),
        *(call.exists(key) for key in KEYS),
        call.multi(),
        call.zrem(QUEUE, JOB_ID),
        call.execute(),
        call.__aexit__(None, None, None),
    ]
    assert not any(mock_call[0] == "delete" for mock_call in pipeline.mock_calls)
    redis_pool.aclose.assert_awaited_once_with(close_connection_pool=True)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_is_idempotent_when_already_absent(
    monkeypatch,
):
    _, _, pipeline = _install_dependencies(
        monkeypatch,
        _run(),
        queue_score=None,
    )

    receipt = await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)

    assert receipt["residue_found"] is False
    assert receipt["removed"] is False
    assert receipt["already_absent"] is True
    assert receipt["evidence"] == {
        "queue_member": False,
        "job": False,
        "retry": False,
        "in_progress": False,
        "result": False,
    }
    pipeline.multi.assert_called_once_with()
    pipeline.zrem.assert_not_called()
    pipeline.ping.assert_called_once_with()
    pipeline.execute.assert_awaited_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run", "body", "message"),
    (
        (_run(status="running"), {**BODY, "expected_status": "running"}, "terminal"),
        (
            _run(importer="nucc", metrics={"queue": "arq:NUCC", "job_id": "legacy"}),
            {**BODY, "expected_importer": "nucc"},
            "does not support",
        ),
        (
            _run(
                importer="claims-pricing",
                metrics={
                    "queue": "arq:ClaimsPricing",
                    "job_id": f"claims_start_{RUN_ID}",
                },
            ),
            {**BODY, "expected_importer": "claims-pricing"},
            "does not support",
        ),
        (_run(), {**BODY, "expected_status": "succeeded"}, "status changed"),
        (_run(), {**BODY, "expected_importer": "plan-pricing-prewarm"}, "importer changed"),
        (
            _run(metrics={"queue": "arq:PTG", "job_id": JOB_ID}),
            BODY,
            "queue conflicts",
        ),
        (
            _run(metrics={"queue": QUEUE, "job_id": "different-job"}),
            BODY,
            "job ID conflicts",
        ),
    ),
)
async def test_reconcile_terminal_queue_residue_refuses_unsafe_identity(
    monkeypatch,
    run,
    body,
    message,
):
    _, _, pipeline = _install_dependencies(monkeypatch, run)

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match=message,
    ):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, body)

    control_imports.create_pool.assert_not_awaited()
    pipeline.zrem.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("present_index", range(4))
async def test_reconcile_terminal_queue_residue_refuses_any_arq_key(
    monkeypatch,
    present_index,
):
    arq_key_presence_flags = tuple(index == present_index for index in range(4))
    _, _, pipeline = _install_dependencies(
        monkeypatch,
        _run(),
        key_presence=arq_key_presence_flags,
    )

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="ARQ job state is present",
    ):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)

    pipeline.multi.assert_not_called()
    pipeline.zrem.assert_not_called()
    pipeline.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("queue_score", (1.0, None))
async def test_reconcile_terminal_queue_residue_refuses_concurrent_key_appearance(
    monkeypatch,
    queue_score,
):
    _, _, pipeline = _install_dependencies(
        monkeypatch,
        _run(),
        queue_score=queue_score,
    )
    pipeline.execute.side_effect = control_imports.WatchError("changed")

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="changed during reconciliation",
    ):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)

    if queue_score is None:
        pipeline.zrem.assert_not_called()
        pipeline.ping.assert_called_once_with()
    else:
        pipeline.zrem.assert_called_once_with(QUEUE, JOB_ID)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_refuses_zero_remove(monkeypatch):
    _, _, _ = _install_dependencies(monkeypatch, _run(), execute_result=(0,))

    with pytest.raises(
        control_imports.StaleWorkerReconciliationConflict,
        match="was not removed",
    ):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_maps_missing_and_unavailable(
    monkeypatch,
):
    _install_dependencies(monkeypatch, None)
    assert await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY) is None
    control_imports.create_pool.assert_not_awaited()

    _install_dependencies(monkeypatch, _run())
    control_imports.create_pool.side_effect = RuntimeError("redis unavailable")
    with pytest.raises(
        control_imports.StaleWorkerReconciliationUnavailable,
        match="queue residue proof is unavailable",
    ):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_keeps_receipt_on_close_failure(
    monkeypatch,
):
    _, redis_pool, _ = _install_dependencies(monkeypatch, _run())
    redis_pool.aclose.side_effect = RuntimeError("close failed")

    receipt = await control_imports.reconcile_terminal_queue_residue(RUN_ID, BODY)

    assert receipt["removed"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    (
        {},
        {**BODY, "extra": True},
        {**BODY, "expected_importer": ""},
        {**BODY, "expected_status": []},
    ),
)
async def test_reconcile_terminal_queue_residue_requires_exact_body(body):
    with pytest.raises(ValueError):
        await control_imports.reconcile_terminal_queue_residue(RUN_ID, body)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_requires_run_id():
    with pytest.raises(ValueError, match="run_id is required"):
        await control_imports.reconcile_terminal_queue_residue(" ", BODY)


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_route_auth_and_error_mapping(monkeypatch):
    service = AsyncMock()
    monkeypatch.setattr(
        control_route_registration,
        "require_control_auth",
        lambda _request: (_ for _ in ()).throw(Unauthorized("unauthorized")),
    )
    monkeypatch.setattr(
        control_route_registration,
        "reconcile_terminal_queue_residue",
        service,
        raising=False,
    )
    request = types.SimpleNamespace(json=BODY, headers={})

    with pytest.raises(Unauthorized):
        await control_route_registration.control_reconcile_terminal_queue_residue(
            request, RUN_ID
        )
    service.assert_not_awaited()

    monkeypatch.setattr(control_route_registration, "require_control_auth", lambda _: None)
    for error, status in (
        (control_imports.StaleWorkerReconciliationConflict("conflict"), 409),
        (control_imports.StaleWorkerReconciliationUnavailable("unavailable"), 503),
        (ValueError("malformed"), 400),
    ):
        service.side_effect = error
        with pytest.raises(SanicException) as exc_info:
            await control_route_registration.control_reconcile_terminal_queue_residue(
                request, RUN_ID
            )
        assert exc_info.value.status_code == status

    service.side_effect = None
    service.return_value = None
    with pytest.raises(SanicException) as exc_info:
        await control_route_registration.control_reconcile_terminal_queue_residue(
            request, RUN_ID
        )
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_reconcile_terminal_queue_residue_route_returns_stable_receipt(monkeypatch):
    receipt_by_field = {
        "run_id": RUN_ID,
        "importer": IMPORTER,
        "status": "failed",
        "queue": QUEUE,
        "job_id": JOB_ID,
        "residue_found": False,
        "removed": False,
        "already_absent": True,
        "evidence": {
            "queue_member": False,
            "job": False,
            "retry": False,
            "in_progress": False,
            "result": False,
        },
    }
    monkeypatch.setattr(control_route_registration, "require_control_auth", lambda _: None)
    monkeypatch.setattr(
        control_route_registration,
        "reconcile_terminal_queue_residue",
        AsyncMock(return_value=receipt_by_field),
        raising=False,
    )

    response = await control_route_registration.control_reconcile_terminal_queue_residue(
        types.SimpleNamespace(json=BODY, headers={}), RUN_ID
    )

    assert response.status == 200
    assert json.loads(response.body) == receipt_by_field
