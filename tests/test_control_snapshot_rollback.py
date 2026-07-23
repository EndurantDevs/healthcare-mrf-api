# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, Forbidden, SanicException

from api import control_snapshot_rollback
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
)


SOURCE_KEY = "source_a"
TARGET_SNAPSHOT = "snapshot_a"
CURRENT_SNAPSHOT = "snapshot_b"
ROLLBACK_OWNER = "source-a-reference"


def _request(payload_by_field: dict, *, is_authenticated: bool = True):
    headers_by_name = (
        {"Authorization": "Bearer secret"} if is_authenticated else {}
    )
    return types.SimpleNamespace(
        json=payload_by_field,
        headers=headers_by_name,
    )


@pytest.mark.asyncio
async def test_control_rollback_route_requires_authentication(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    rollback_call = AsyncMock()
    monkeypatch.setattr(
        control_snapshot_rollback,
        "rollback_pinned_ptg2_source_snapshot",
        rollback_call,
    )

    with pytest.raises(Forbidden):
        await control_snapshot_rollback.control_ptg_source_snapshot_rollback(
            _request({}, is_authenticated=False)
        )

    rollback_call.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_rollback_route_passes_exact_coordinates(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    rollback_call = AsyncMock(
        return_value={
            "status": "rolled_back",
            "source_key": SOURCE_KEY,
            "snapshot_id": TARGET_SNAPSHOT,
        }
    )
    monkeypatch.setattr(
        control_snapshot_rollback,
        "rollback_pinned_ptg2_source_snapshot",
        rollback_call,
    )

    response = (
        await control_snapshot_rollback.control_ptg_source_snapshot_rollback(
            _request(
                {
                    "source_key": SOURCE_KEY,
                    "snapshot_id": TARGET_SNAPSHOT,
                    "expected_current_snapshot_id": CURRENT_SNAPSHOT,
                    "rollback_owner_id": ROLLBACK_OWNER,
                }
            )
        )
    )

    assert response.status == 200
    assert json.loads(response.body)["status"] == "rolled_back"
    rollback_call.assert_awaited_once_with(
        source_key=SOURCE_KEY,
        snapshot_id=TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT_SNAPSHOT,
        rollback_owner_id=ROLLBACK_OWNER,
    )


@pytest.mark.asyncio
async def test_control_rollback_route_maps_invalid_request_to_400(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control_snapshot_rollback,
        "rollback_pinned_ptg2_source_snapshot",
        AsyncMock(side_effect=ValueError("rollback coordinates are required")),
    )

    with pytest.raises(BadRequest, match="coordinates are required"):
        await control_snapshot_rollback.control_ptg_source_snapshot_rollback(
            _request({})
        )


@pytest.mark.asyncio
async def test_control_rollback_route_maps_stale_state_to_409(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control_snapshot_rollback,
        "rollback_pinned_ptg2_source_snapshot",
        AsyncMock(
            side_effect=PTG2SourceSnapshotRollbackConflict(
                "source pointer changed"
            )
        ),
    )

    with pytest.raises(SanicException) as conflict:
        await control_snapshot_rollback.control_ptg_source_snapshot_rollback(
            _request({})
        )
    assert conflict.value.status_code == 409
