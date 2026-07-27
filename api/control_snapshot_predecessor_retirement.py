# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated control route for audited PTG predecessor retirement."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_parts.source_snapshot_predecessor_retirement import (
    retire_ptg2_source_predecessor,
)
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)


async def control_ptg_source_predecessor_retire(request):
    """Clear one exact predecessor retention set and persist its audit."""

    require_control_auth(request)
    payload = request.json if isinstance(request.json, dict) else {}
    try:
        report = await retire_ptg2_source_predecessor(
            source_key=str(payload.get("source_key") or ""),
            current_snapshot_id=str(payload.get("current_snapshot_id") or ""),
            predecessor_snapshot_id=str(
                payload.get("predecessor_snapshot_id") or ""
            ),
            rollback_pin_mode=str(payload.get("rollback_pin_mode") or ""),
            rollback_owner_id=payload.get("rollback_owner_id"),
            actor=str(payload.get("actor") or ""),
            reason=str(payload.get("reason") or ""),
            idempotency_key=str(payload.get("idempotency_key") or ""),
        )
    except PTG2PredecessorRetirementConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(report, default=str)


def register_predecessor_retirement_route(blueprint) -> None:
    """Register the distinct audited predecessor-retirement operation."""

    blueprint.post("/ptg/source-snapshots/retire-predecessor")(
        control_ptg_source_predecessor_retire
    )


__all__ = [
    "control_ptg_source_predecessor_retire",
    "register_predecessor_retirement_route",
]
