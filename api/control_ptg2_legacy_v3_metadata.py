# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticated plan/execute routes for legacy PTG V3 metadata repair."""

from __future__ import annotations

from sanic import response
from sanic.exceptions import BadRequest, SanicException

from api.control_auth import require_control_auth
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred
from process.ptg_parts.ptg2_legacy_v3_metadata_reconcile import (
    LegacyV3MetadataConflict,
    plan_legacy_v3_metadata_reconcile,
    reconcile_legacy_v3_metadata,
)


def _payload(request) -> dict:
    return request.json if isinstance(request.json, dict) else {}


def _required_string(payload: dict, field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise BadRequest(f"{field_name} must be a non-empty string")
    return value.strip()


async def control_legacy_v3_metadata_plan(request):
    """Return one exact no-write legacy V3 repair plan."""

    require_control_auth(request)
    payload = _payload(request)
    try:
        plan = await plan_legacy_v3_metadata_reconcile(
            snapshot_id=_required_string(payload, "snapshot_id"),
            internal_run_id=_required_string(payload, "internal_run_id"),
            outer_run_id=_required_string(payload, "outer_run_id"),
        )
    except LegacyV3MetadataConflict as error:
        raise SanicException(str(error), status_code=409) from error
    except ValueError as error:
        raise BadRequest(str(error)) from error
    return response.json(plan, default=str)


async def control_legacy_v3_metadata_execute(request):
    """Apply one reviewed metadata-only transition without cleanup."""

    require_control_auth(request)
    payload = _payload(request)
    try:
        report = await reconcile_legacy_v3_metadata(
            snapshot_id=_required_string(payload, "snapshot_id"),
            internal_run_id=_required_string(payload, "internal_run_id"),
            outer_run_id=_required_string(payload, "outer_run_id"),
            expected_plan_digest=_required_string(
                payload,
                "expected_plan_digest",
            ),
        )
    except PTG2LifecycleLockDeferred as error:
        raise SanicException(
            str(error),
            status_code=503,
            headers={"Retry-After": "1"},
        ) from error
    except LegacyV3MetadataConflict as error:
        raise SanicException(str(error), status_code=409) from error
    except ValueError as error:
        raise BadRequest(str(error)) from error
    return response.json(report, default=str)


def register_legacy_v3_metadata_routes(blueprint) -> None:
    """Register separate plan and execute capabilities."""

    blueprint.post("/ptg/v3/stale-metadata/reconcile-plan")(
        control_legacy_v3_metadata_plan
    )
    blueprint.post("/ptg/v3/stale-metadata/reconcile")(
        control_legacy_v3_metadata_execute
    )


__all__ = [
    "control_legacy_v3_metadata_execute",
    "control_legacy_v3_metadata_plan",
    "register_legacy_v3_metadata_routes",
]
