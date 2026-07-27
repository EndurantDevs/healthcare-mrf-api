# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, Forbidden, SanicException

from api import control_snapshot_predecessor_retirement as control_route
from api import control_snapshot_rollback
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)


def _request(payload: dict, *, authenticated: bool = True):
    return types.SimpleNamespace(
        json=payload,
        headers={"Authorization": "Bearer secret"} if authenticated else {},
    )


def test_predecessor_retirement_route_is_registered_with_control_blueprint():
    routes_by_path = {}
    blueprint = types.SimpleNamespace(
        post=lambda path: lambda handler: routes_by_path.setdefault(
            path,
            handler,
        )
    )

    control_snapshot_rollback.register_source_snapshot_rollback_route(
        blueprint
    )

    assert routes_by_path[
        "/ptg/source-snapshots/retire-predecessor"
    ] is control_route.control_ptg_source_predecessor_retire


@pytest.mark.asyncio
async def test_predecessor_retirement_route_requires_control_auth(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    retire = AsyncMock()
    monkeypatch.setattr(control_route, "retire_ptg2_source_predecessor", retire)

    with pytest.raises(Forbidden):
        await control_route.control_ptg_source_predecessor_retire(
            _request({}, authenticated=False)
        )

    retire.assert_not_awaited()


@pytest.mark.asyncio
async def test_predecessor_retirement_route_passes_exact_audit_coordinates(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    retire = AsyncMock(return_value={"status": "retired", "idempotent": False})
    monkeypatch.setattr(control_route, "retire_ptg2_source_predecessor", retire)
    payload = {
        "source_key": "synthetic-source",
        "current_snapshot_id": "snapshot-current",
        "predecessor_snapshot_id": "snapshot-previous",
        "rollback_pin_mode": "owned",
        "rollback_owner_id": "rollback-owner",
        "actor": "operator@example.invalid",
        "reason": "retention window complete",
        "idempotency_key": "retire-synthetic-001",
    }

    response = await control_route.control_ptg_source_predecessor_retire(
        _request(payload)
    )

    assert response.status == 200
    assert json.loads(response.body) == {
        "status": "retired",
        "idempotent": False,
    }
    retire.assert_awaited_once_with(**payload)


@pytest.mark.asyncio
async def test_predecessor_retirement_route_preserves_absent_owner(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    retire = AsyncMock(return_value={"status": "retired"})
    monkeypatch.setattr(control_route, "retire_ptg2_source_predecessor", retire)
    payload = {
        "source_key": "synthetic-source",
        "current_snapshot_id": "snapshot-current",
        "predecessor_snapshot_id": "snapshot-previous",
        "rollback_pin_mode": "absent",
        "actor": "operator@example.invalid",
        "reason": "retention window complete",
        "idempotency_key": "retire-synthetic-absent-001",
    }

    response = await control_route.control_ptg_source_predecessor_retire(
        _request(payload)
    )

    assert response.status == 200
    retire.assert_awaited_once_with(
        **payload,
        rollback_owner_id=None,
    )


@pytest.mark.asyncio
async def test_predecessor_retirement_route_maps_validation_and_conflict(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    retire = AsyncMock(side_effect=ValueError("all coordinates are required"))
    monkeypatch.setattr(control_route, "retire_ptg2_source_predecessor", retire)

    with pytest.raises(BadRequest, match="all coordinates are required"):
        await control_route.control_ptg_source_predecessor_retire(_request({}))

    retire.side_effect = PTG2PredecessorRetirementConflict(
        "pointer state changed"
    )
    with pytest.raises(SanicException) as conflict:
        await control_route.control_ptg_source_predecessor_retire(_request({}))
    assert conflict.value.status_code == 409
