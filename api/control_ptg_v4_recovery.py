# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated exact recovery routes for failed PTG V4 layouts."""

from __future__ import annotations

from typing import Any, Mapping

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_parts.ptg2_v4_failed_layout_recovery import (
    PTG2V4RecoveryConflict,
    plan_ptg2_v4_recovery,
    recover_ptg2_v4_layout,
)


def _recovery_request(
    payload_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "snapshot_id": str(payload_by_field.get("snapshot_id") or ""),
        "import_run_id": str(payload_by_field.get("import_run_id") or ""),
        "snapshot_key": payload_by_field.get("snapshot_key") or 0,
    }


async def control_v4_recovery_plan(request):
    """Preview exact recovery of one failed, unpublished V4 layout."""

    require_control_auth(request)
    payload_by_field = request.json if isinstance(request.json, dict) else {}
    try:
        plan_by_field = await plan_ptg2_v4_recovery(
            **_recovery_request(payload_by_field)
        )
    except PTG2V4RecoveryConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except (TypeError, ValueError) as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(plan_by_field, default=str)


async def control_v4_recover(request):
    """Execute a previously previewed exact V4 failed-layout recovery."""

    require_control_auth(request)
    payload_by_field = request.json if isinstance(request.json, dict) else {}
    try:
        recovery_by_field = await recover_ptg2_v4_layout(
            **_recovery_request(payload_by_field),
            expected_plan_digest=str(
                payload_by_field.get("expected_plan_digest") or ""
            ),
        )
    except PTG2V4RecoveryConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except (TypeError, ValueError) as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(recovery_by_field, default=str)


def register_v4_recovery_routes(control_blueprint) -> None:
    """Register preview and execution routes on the control blueprint."""

    control_blueprint.add_route(
        control_v4_recovery_plan,
        "/ptg/v4/failed-layouts/recovery-plan",
        methods=("POST",),
    )
    control_blueprint.add_route(
        control_v4_recover,
        "/ptg/v4/failed-layouts/recover",
        methods=("POST",),
    )


__all__ = [
    "control_v4_recover",
    "control_v4_recovery_plan",
    "register_v4_recovery_routes",
]
