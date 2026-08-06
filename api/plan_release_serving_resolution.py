# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed resolution of immutable plan releases for fail-closed serving."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from sqlalchemy import text

from api import plan_release_serving
from api.plan_release_serving import (
    PLAN_RELEASE_PIN_OWNER_TYPE,
    PTG2_SCHEMA,
    PlanReleaseServingSelection,
    _selection_from_rows,
    normalize_plan_release_id,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

PLAN_RELEASE_RESOLUTION_READY = "ready"
PLAN_RELEASE_RESOLUTION_NOT_FOUND = "not_found"
PLAN_RELEASE_RESOLUTION_UNAVAILABLE = "unavailable"
PLAN_RELEASE_RESOLUTION_STATES = frozenset(
    {
        PLAN_RELEASE_RESOLUTION_READY,
        PLAN_RELEASE_RESOLUTION_NOT_FOUND,
        PLAN_RELEASE_RESOLUTION_UNAVAILABLE,
    }
)

_PLAN_RELEASE_SERVING_SQL = f"""
SELECT revision.serving_revision_id,
       revision.plan_release_id,
       revision.healthporta_plan_id,
       revision.plan_version_id,
       revision.release_month,
       revision.release_status,
       revision.expected_binding_count,
       revision.binding_set_digest,
       binding.binding_ordinal,
       binding.snapshot_id,
       binding.source_key,
       binding.plan_id,
       binding.plan_market_type,
       binding.role,
       binding.required,
       snapshot.status AS snapshot_status,
       EXISTS (
           SELECT 1
             FROM {PTG2_SCHEMA}.ptg2_snapshot_pin pin
            WHERE pin.owner_type = :pin_owner_type
              AND pin.owner_id = revision.serving_revision_id
              AND pin.snapshot_id = binding.snapshot_id
       ) AS is_pinned
  FROM {PTG2_SCHEMA}.plan_release_serving_revision revision
  JOIN {PTG2_SCHEMA}.plan_release_snapshot_binding binding
    ON binding.serving_revision_id = revision.serving_revision_id
  LEFT JOIN {PTG2_SCHEMA}.ptg2_snapshot snapshot
    ON snapshot.snapshot_id = binding.snapshot_id
 WHERE revision.plan_release_id = :plan_release_id
   AND revision.serving_status = 'published'
   AND revision.release_status = 'published'
   AND revision.is_current
 ORDER BY CASE binding.role WHEN 'in_network' THEN 0 ELSE 1 END,
          binding.binding_ordinal
"""

_PLAN_RELEASE_EXISTS_SQL = f"""
SELECT EXISTS (
    SELECT 1
      FROM {PTG2_SCHEMA}.plan_release_serving_revision
     WHERE plan_release_id = :plan_release_id
) AS release_exists
"""


@dataclass(frozen=True, slots=True, repr=False)
class PlanReleaseServingResolution:
    """Typed distinction between an absent and an unusable release."""

    state: str
    selection: PlanReleaseServingSelection | None

    def __post_init__(self) -> None:
        is_ready = self.state == PLAN_RELEASE_RESOLUTION_READY
        has_selection = type(self.selection) is PlanReleaseServingSelection
        if self.state not in PLAN_RELEASE_RESOLUTION_STATES or (
            is_ready != has_selection
        ):
            raise ValueError("invalid plan release serving resolution")

    def __repr__(self) -> str:
        return f"<plan-release-serving-resolution state={self.state}>"


def _resolution(
    state: str,
    selection: PlanReleaseServingSelection | None = None,
) -> PlanReleaseServingResolution:
    return PlanReleaseServingResolution(state, selection)


async def _load_release_rows(
    session: Any,
    plan_release_id: str,
) -> list[Any]:
    result = await session.execute(
        text(_PLAN_RELEASE_SERVING_SQL),
        {
            "plan_release_id": plan_release_id,
            "pin_owner_type": PLAN_RELEASE_PIN_OWNER_TYPE,
        },
    )
    return list(result)


async def _has_release_revision(session: Any, plan_release_id: str) -> bool:
    result = await session.execute(
        text(_PLAN_RELEASE_EXISTS_SQL),
        {"plan_release_id": plan_release_id},
    )
    first_row = next(iter(result), None)
    if first_row is None:
        return False
    row_by_field = dict(getattr(first_row, "_mapping", first_row))
    return row_by_field.get("release_exists") is True


async def _validate_release_bindings(
    session: Any,
    selection: PlanReleaseServingSelection,
) -> PlanReleaseServingSelection | None:
    serving_tables_by_snapshot_id: dict[str, PTG2ServingTables] = {}
    try:
        for binding in selection.bindings:
            if not await plan_release_serving.is_release_binding_serving_ready(
                session,
                binding,
                validated_serving_tables_by_snapshot_id=(serving_tables_by_snapshot_id),
            ):
                return None
    except PTG2ManifestArtifactError:
        return None
    validated_selection = replace(
        selection,
        _validated_serving_tables=tuple(serving_tables_by_snapshot_id.items()),
    )
    if validated_selection.network_tables_by_snapshot() is None:
        return None
    return validated_selection


async def resolve_plan_release_serving_resolution(
    session: Any,
    plan_release_id: Any,
) -> PlanReleaseServingResolution:
    """Resolve one release while preserving absent versus unavailable state."""

    normalized_release_id = normalize_plan_release_id(plan_release_id)
    if normalized_release_id is None:
        return _resolution(PLAN_RELEASE_RESOLUTION_NOT_FOUND)
    release_rows = await _load_release_rows(session, normalized_release_id)
    if not release_rows:
        state = (
            PLAN_RELEASE_RESOLUTION_UNAVAILABLE
            if await _has_release_revision(session, normalized_release_id)
            else PLAN_RELEASE_RESOLUTION_NOT_FOUND
        )
        return _resolution(state)
    selection = _selection_from_rows(normalized_release_id, release_rows)
    if selection is None:
        return _resolution(PLAN_RELEASE_RESOLUTION_UNAVAILABLE)
    validated_selection = await _validate_release_bindings(session, selection)
    if validated_selection is None:
        return _resolution(PLAN_RELEASE_RESOLUTION_UNAVAILABLE)
    return _resolution(PLAN_RELEASE_RESOLUTION_READY, validated_selection)
