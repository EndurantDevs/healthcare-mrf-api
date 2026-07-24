# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated control routes for exact PTG V4 stale metadata repair."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_parts.ptg2_v4_stale_metadata_reconcile import (
    PTG2V4StaleMetadataConflict,
    plan_v4_stale_metadata,
    reconcile_v4_stale_metadata,
)


def _request_payload(request) -> dict:
    return request.json if isinstance(request.json, dict) else {}


async def control_v4_stale_plan(request):
    """Return a no-write plan for one exact stale V4 snapshot/run pair."""

    require_control_auth(request)
    payload_by_field = _request_payload(request)
    try:
        plan_by_field = (
            await plan_v4_stale_metadata(
                snapshot_id=str(payload_by_field.get("snapshot_id") or ""),
                internal_run_id=str(
                    payload_by_field.get("internal_run_id") or ""
                ),
            )
        )
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(plan_by_field, default=str)


async def control_v4_stale_execute(request):
    """Apply an exact reviewed stale-build plan as metadata updates only."""

    require_control_auth(request)
    payload_by_field = _request_payload(request)
    try:
        report_by_field = await reconcile_v4_stale_metadata(
            snapshot_id=str(payload_by_field.get("snapshot_id") or ""),
            internal_run_id=str(
                payload_by_field.get("internal_run_id") or ""
            ),
            expected_plan_digest=str(
                payload_by_field.get("expected_plan_digest") or ""
            ),
        )
    except PTG2V4StaleMetadataConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(report_by_field, default=str)


def register_v4_stale_routes(blueprint) -> None:
    """Register separate plan and execute capabilities."""

    blueprint.post("/ptg/v4/stale-metadata/reconcile-plan")(
        control_v4_stale_plan
    )
    blueprint.post("/ptg/v4/stale-metadata/reconcile")(
        control_v4_stale_execute
    )


__all__ = [
    "control_v4_stale_execute",
    "control_v4_stale_plan",
    "register_v4_stale_routes",
]
