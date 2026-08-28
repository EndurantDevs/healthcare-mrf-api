from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import SanicException, Unauthorized

from api import control_imports, control_route_registration


RUN_ID = "run_missing_worker"
EXPECTED_BODY = {
    "expected_importer": "plan-pricing-projection",
    "expected_status": "running",
    "expected_heartbeat_at": "2026-08-27T22:30:38.663966+00:00",
    "expected_attempt_id": f"{RUN_ID}:attempt",
    "expected_attempt_started_at": "2026-08-27T22:24:15.779700+00:00",
}


@pytest.mark.asyncio
async def test_reconcile_missing_worker_requires_auth_before_service(monkeypatch):
    service = AsyncMock()
    monkeypatch.setattr(
        control_route_registration,
        "require_control_auth",
        lambda _request: (_ for _ in ()).throw(Unauthorized("unauthorized")),
    )
    monkeypatch.setattr(
        control_route_registration,
        "reconcile_stale_worker_failure",
        service,
    )

    with pytest.raises(Unauthorized):
        await control_route_registration.control_reconcile_stale_worker(
            types.SimpleNamespace(json=EXPECTED_BODY, headers={}),
            RUN_ID,
        )

    service.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("service_result", "expected_status"),
    (
        (control_imports.StaleWorkerReconciliationConflict("active"), 409),
        (control_imports.StaleWorkerReconciliationUnavailable("unavailable"), 503),
        (ValueError("malformed"), 400),
        (None, 404),
    ),
)
async def test_reconcile_missing_worker_route_maps_fail_closed_states(
    monkeypatch,
    service_result,
    expected_status,
):
    monkeypatch.setattr(
        control_route_registration,
        "require_control_auth",
        lambda _request: None,
    )
    service = (
        AsyncMock(return_value=None)
        if service_result is None
        else AsyncMock(side_effect=service_result)
    )
    monkeypatch.setattr(
        control_route_registration,
        "reconcile_stale_worker_failure",
        service,
    )

    with pytest.raises(SanicException) as exc_info:
        await control_route_registration.control_reconcile_stale_worker(
            types.SimpleNamespace(json=EXPECTED_BODY, headers={}),
            RUN_ID,
        )

    assert exc_info.value.status_code == expected_status


@pytest.mark.asyncio
async def test_reconcile_missing_worker_route_returns_sanitized_receipt(
    monkeypatch,
):
    receipt_by_field = {
        "run_id": RUN_ID,
        "importer": "plan-pricing-projection",
        "status": "failed",
        "reconciled": True,
        "error_code": "worker_lifecycle_lost",
        "attempt_id": f"{RUN_ID}:attempt",
        "attempt_started_at": EXPECTED_BODY["expected_attempt_started_at"],
    }
    monkeypatch.setattr(
        control_route_registration,
        "require_control_auth",
        lambda _request: None,
    )
    monkeypatch.setattr(
        control_route_registration,
        "reconcile_stale_worker_failure",
        AsyncMock(return_value=receipt_by_field),
    )

    response = await control_route_registration.control_reconcile_stale_worker(
        types.SimpleNamespace(json=EXPECTED_BODY, headers={}),
        RUN_ID,
    )

    assert response.status == 200
    assert json.loads(response.body) == receipt_by_field
