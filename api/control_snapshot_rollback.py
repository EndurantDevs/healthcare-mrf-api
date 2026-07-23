# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated control-plane route for exact PTG source rollback."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_parts.source_snapshot_rollback import (
    rollback_pinned_ptg2_source_snapshot,
)
from process.ptg_parts.source_snapshot_rollback_types import (
    PTG2SourceSnapshotRollbackConflict,
)


async def control_ptg_source_snapshot_rollback(request):
    """Roll one source back to its exact pinned published predecessor."""

    require_control_auth(request)
    payload_by_field = request.json if isinstance(request.json, dict) else {}
    try:
        rollback_report = await rollback_pinned_ptg2_source_snapshot(
            source_key=str(payload_by_field.get("source_key") or ""),
            snapshot_id=str(payload_by_field.get("snapshot_id") or ""),
            expected_current_snapshot_id=str(
                payload_by_field.get("expected_current_snapshot_id") or ""
            ),
            rollback_owner_id=str(
                payload_by_field.get("rollback_owner_id") or ""
            ),
        )
    except PTG2SourceSnapshotRollbackConflict as exc:
        raise SanicException(str(exc), status_code=409) from exc
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc
    return response.json(rollback_report, default=str)


def register_source_snapshot_rollback_route(blueprint) -> None:
    """Register the distinct published-snapshot rollback operation."""

    blueprint.post("/ptg/source-snapshots/rollback")(
        control_ptg_source_snapshot_rollback
    )
