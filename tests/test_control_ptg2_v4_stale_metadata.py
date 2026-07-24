# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, Forbidden, SanicException

from api import control_ptg2_v4_stale_metadata as control_module
from api.control_ptg_v4 import register_v4_control_routes
from process.ptg_parts.ptg2_v4_stale_metadata_reconcile import (
    PTG2V4StaleMetadataConflict,
)


SNAPSHOT_ID = "ptg2:202607:synthetic"
INTERNAL_RUN_ID = "ptg2:synthetic-run"
PLAN_DIGEST = "a" * 64


class _RouteRecorder:
    def __init__(self) -> None:
        self.route_uris: list[str] = []

    def post(self, route_uri: str):
        def record_route(handler):
            self.route_uris.append(route_uri)
            return handler

        return record_route

    def add_route(self, _handler, route_uri: str, **_kwargs) -> None:
        self.route_uris.append(route_uri)


def _request(payload_by_field: dict, *, authenticated: bool = True):
    return types.SimpleNamespace(
        json=payload_by_field,
        headers=(
            {"Authorization": "Bearer secret"} if authenticated else {}
        ),
    )


def test_v4_control_registration_preserves_both_recovery_families():
    route_recorder = _RouteRecorder()

    register_v4_control_routes(route_recorder)

    assert set(route_recorder.route_uris) == {
        "/ptg/v4/stale-metadata/reconcile-plan",
        "/ptg/v4/stale-metadata/reconcile",
        "/ptg/v4/failed-layouts/recovery-plan",
        "/ptg/v4/failed-layouts/recover",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "handler_name,dependency_name",
    (
        (
            "control_v4_stale_plan",
            "plan_v4_stale_metadata",
        ),
        (
            "control_v4_stale_execute",
            "reconcile_v4_stale_metadata",
        ),
    ),
)
async def test_stale_metadata_routes_require_control_auth(
    monkeypatch,
    handler_name,
    dependency_name,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    dependency = AsyncMock()
    monkeypatch.setattr(control_module, dependency_name, dependency)

    with pytest.raises(Forbidden):
        await getattr(control_module, handler_name)(
            _request({}, authenticated=False)
        )

    dependency.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_metadata_plan_passes_only_exact_coordinates(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    plan_call = AsyncMock(
        return_value={
            "status": "ready",
            "target_digest": "b" * 64,
            "plan_digest": PLAN_DIGEST,
        }
    )
    monkeypatch.setattr(
        control_module,
        "plan_v4_stale_metadata",
        plan_call,
    )

    route_response = await control_module.control_v4_stale_plan(
        _request(
            {
                "snapshot_id": SNAPSHOT_ID,
                "internal_run_id": INTERNAL_RUN_ID,
                "stale_after_seconds": 0,
            }
        )
    )

    assert route_response.status == 200
    assert json.loads(route_response.body)["status"] == "ready"
    plan_call.assert_awaited_once_with(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
    )


@pytest.mark.asyncio
async def test_stale_metadata_execute_requires_reviewed_plan_digest(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    execute_call = AsyncMock(
        return_value={
            "status": "reconciled",
            "target_digest": "b" * 64,
            "plan_digest": PLAN_DIGEST,
        }
    )
    monkeypatch.setattr(
        control_module,
        "reconcile_v4_stale_metadata",
        execute_call,
    )

    route_response = (
        await control_module.control_v4_stale_execute(
            _request(
                {
                    "snapshot_id": SNAPSHOT_ID,
                    "internal_run_id": INTERNAL_RUN_ID,
                    "expected_plan_digest": PLAN_DIGEST,
                }
            )
        )
    )

    assert route_response.status == 200
    assert json.loads(route_response.body)["status"] == "reconciled"
    execute_call.assert_awaited_once_with(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        expected_plan_digest=PLAN_DIGEST,
    )


@pytest.mark.asyncio
async def test_stale_metadata_execute_maps_state_conflict_to_409(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control_module,
        "reconcile_v4_stale_metadata",
        AsyncMock(
            side_effect=PTG2V4StaleMetadataConflict(
                "stale-build state changed"
            )
        ),
    )

    with pytest.raises(SanicException) as conflict:
        await control_module.control_v4_stale_execute(
            _request(
                {
                    "snapshot_id": SNAPSHOT_ID,
                    "internal_run_id": INTERNAL_RUN_ID,
                    "expected_plan_digest": PLAN_DIGEST,
                }
            )
        )

    assert conflict.value.status_code == 409


@pytest.mark.asyncio
async def test_stale_metadata_routes_map_invalid_coordinates_to_400(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(
        control_module,
        "plan_v4_stale_metadata",
        AsyncMock(side_effect=ValueError("snapshot_id is required")),
    )

    with pytest.raises(BadRequest, match="snapshot_id is required"):
        await control_module.control_v4_stale_plan(_request({}))
