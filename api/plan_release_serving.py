# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed resolution of canonical plan releases to exact PTG2 snapshots."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from sqlalchemy import text


def _projection_schema() -> str:
    """Use the same explicit schema contract as the projection migration/ORM."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


PTG2_SCHEMA = _projection_schema()
PLAN_RELEASE_ID_PATTERN = re.compile(
    r"^hprelease_[0-9A-HJKMNP-TV-Z]{26}$"
)
PLAN_RELEASE_PIN_OWNER_TYPE = "plan_release_serving_revision"
PLAN_RELEASE_IN_NETWORK_ROLE = "in_network"
PLAN_RELEASE_ALLOWED_AMOUNTS_ROLE = "allowed_amounts"
PLAN_RELEASE_BINDING_ROLES = frozenset(
    {PLAN_RELEASE_IN_NETWORK_ROLE, PLAN_RELEASE_ALLOWED_AMOUNTS_ROLE}
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


def _row_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


def normalize_plan_release_id(value: Any) -> str | None:
    """Return a syntactically valid immutable release ID without case folding."""

    normalized = str(value or "").strip()
    if not normalized or not PLAN_RELEASE_ID_PATTERN.fullmatch(normalized):
        return None
    return normalized


def has_conflicting_release_selectors(args: Mapping[str, Any]) -> bool:
    """Reject raw routing selectors that could weaken a canonical release pin."""

    return any(
        str(args.get(selector) or "").strip()
        for selector in (
            "snapshot_id",
            "source_key",
            "plan_id",
            "plan_external_id",
            "plan_id_type",
        )
    )


@dataclass(frozen=True)
class PlanReleaseSnapshotBinding:
    binding_ordinal: int
    snapshot_id: str
    source_key: str
    plan_id: str
    plan_market_type: str
    role: str
    required: bool


@dataclass(frozen=True)
class PlanReleaseServingSelection:
    serving_revision_id: str
    plan_release_id: str
    healthporta_plan_id: str
    plan_version_id: str | None
    release_month: str
    release_status: str
    binding_set_digest: str
    bindings: tuple[PlanReleaseSnapshotBinding, ...]

    def bindings_for_role(
        self,
        role: str,
    ) -> tuple[PlanReleaseSnapshotBinding, ...]:
        """Return unique physical reads in their frozen binding order."""

        selected_bindings: list[PlanReleaseSnapshotBinding] = []
        seen_binding_identities: set[tuple[str, str, str, str]] = set()
        for binding in self.bindings:
            if binding.role != role:
                continue
            binding_identity = (
                binding.source_key,
                binding.snapshot_id,
                binding.plan_id,
                binding.plan_market_type,
            )
            if binding_identity in seen_binding_identities:
                continue
            seen_binding_identities.add(binding_identity)
            selected_bindings.append(binding)
        return tuple(selected_bindings)

    @property
    def in_network_bindings(self) -> tuple[PlanReleaseSnapshotBinding, ...]:
        """Return the release's deduplicated in-network bindings."""

        return self.bindings_for_role(PLAN_RELEASE_IN_NETWORK_ROLE)

    @property
    def allowed_amount_bindings(self) -> tuple[PlanReleaseSnapshotBinding, ...]:
        """Return the release's deduplicated allowed-amount bindings."""

        return self.bindings_for_role(PLAN_RELEASE_ALLOWED_AMOUNTS_ROLE)

    def response_metadata(self) -> dict[str, Any]:
        """Return canonical coordinates suitable for a pricing response."""

        return {
            "healthporta_plan_id": self.healthporta_plan_id,
            "plan_release_id": self.plan_release_id,
            "plan_version_id": self.plan_version_id,
            "serving_revision_id": self.serving_revision_id,
            "release_month": self.release_month,
            "release_status": self.release_status,
            "is_current": True,
            "binding_set_digest": self.binding_set_digest,
        }


def _single_text_value(rows: Iterable[Mapping[str, Any]], field: str) -> str | None:
    values = {str(row.get(field) or "").strip() for row in rows}
    if len(values) != 1:
        return None
    value = values.pop()
    return value or None


@dataclass(frozen=True)
class _PlanReleaseHeader:
    serving_revision_id: str
    plan_release_id: str
    healthporta_plan_id: str
    plan_version_id: str | None
    release_month: str
    release_status: str
    binding_set_digest: str


def _release_header_from_rows(
    requested_release_id: str,
    release_rows: list[dict[str, Any]],
) -> _PlanReleaseHeader | None:
    plan_release_id = _single_text_value(release_rows, "plan_release_id")
    serving_revision_id = _single_text_value(
        release_rows, "serving_revision_id"
    )
    healthporta_plan_id = _single_text_value(
        release_rows, "healthporta_plan_id"
    )
    release_month = _single_text_value(release_rows, "release_month")
    release_status = _single_text_value(release_rows, "release_status")
    binding_set_digest = _single_text_value(
        release_rows, "binding_set_digest"
    )
    plan_version_values = {
        str(release_row.get("plan_version_id") or "").strip()
        for release_row in release_rows
    }
    try:
        expected_counts = {
            int(release_row.get("expected_binding_count"))
            for release_row in release_rows
        }
    except (TypeError, ValueError):
        return None
    if (
        plan_release_id != requested_release_id
        or not serving_revision_id
        or not healthporta_plan_id
        or not release_month
        or release_status != "published"
        or not binding_set_digest
        or len(plan_version_values) != 1
        or expected_counts != {len(release_rows)}
    ):
        return None
    return _PlanReleaseHeader(
        serving_revision_id=serving_revision_id,
        plan_release_id=plan_release_id,
        healthporta_plan_id=healthporta_plan_id,
        plan_version_id=plan_version_values.pop() or None,
        release_month=release_month,
        release_status=release_status,
        binding_set_digest=binding_set_digest,
    )


def _plan_release_binding_from_row(
    release_row: Mapping[str, Any],
) -> PlanReleaseSnapshotBinding | None:
    try:
        ordinal = int(release_row.get("binding_ordinal"))
    except (TypeError, ValueError):
        return None
    role = str(release_row.get("role") or "").strip()
    snapshot_id = str(release_row.get("snapshot_id") or "").strip()
    source_key = str(release_row.get("source_key") or "").strip()
    plan_id = str(release_row.get("plan_id") or "").strip()
    if (
        ordinal < 0
        or role not in PLAN_RELEASE_BINDING_ROLES
        or not snapshot_id
        or not source_key
        or not plan_id
        or str(release_row.get("snapshot_status") or "").strip()
        != "published"
        or release_row.get("is_pinned") is not True
    ):
        return None
    return PlanReleaseSnapshotBinding(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=str(
            release_row.get("plan_market_type") or ""
        ).strip().lower(),
        role=role,
        required=bool(release_row.get("required")),
    )


def _collect_plan_release_bindings(
    release_rows: Iterable[Mapping[str, Any]],
) -> tuple[PlanReleaseSnapshotBinding, ...] | None:
    bindings: list[PlanReleaseSnapshotBinding] = []
    role_ordinals: set[tuple[str, int]] = set()
    routing_by_snapshot: dict[
        tuple[str, str, str], tuple[str, str]
    ] = {}
    for release_row in release_rows:
        binding = _plan_release_binding_from_row(release_row)
        if binding is None:
            return None
        role_ordinal = (binding.role, binding.binding_ordinal)
        snapshot_route = (
            binding.source_key,
            binding.snapshot_id,
            binding.role,
        )
        routing_identity = (binding.plan_id, binding.plan_market_type)
        prior_routing_identity = routing_by_snapshot.get(snapshot_route)
        if role_ordinal in role_ordinals or (
            prior_routing_identity is not None
            and prior_routing_identity != routing_identity
        ):
            return None
        role_ordinals.add(role_ordinal)
        routing_by_snapshot[snapshot_route] = routing_identity
        bindings.append(binding)
    bindings.sort(
        key=lambda binding: (
            0 if binding.role == PLAN_RELEASE_IN_NETWORK_ROLE else 1,
            binding.binding_ordinal,
        )
    )
    return tuple(bindings)


def _selection_from_rows(
    requested_release_id: str,
    raw_rows: Iterable[Any],
) -> PlanReleaseServingSelection | None:
    """Validate completeness and construct one immutable release selection."""

    release_rows = [_row_mapping(raw_row) for raw_row in raw_rows]
    if not release_rows:
        return None
    release_header = _release_header_from_rows(
        requested_release_id,
        release_rows,
    )
    bindings = _collect_plan_release_bindings(release_rows)
    if release_header is None or bindings is None:
        return None
    return PlanReleaseServingSelection(
        serving_revision_id=release_header.serving_revision_id,
        plan_release_id=release_header.plan_release_id,
        healthporta_plan_id=release_header.healthporta_plan_id,
        plan_version_id=release_header.plan_version_id,
        release_month=release_header.release_month,
        release_status=release_header.release_status,
        binding_set_digest=release_header.binding_set_digest,
        bindings=bindings,
    )


async def resolve_plan_release_serving(
    session: Any,
    plan_release_id: Any,
) -> PlanReleaseServingSelection | None:
    """Load one exact, complete, pinned release without any current fallback."""

    normalized_release_id = normalize_plan_release_id(plan_release_id)
    if normalized_release_id is None:
        return None
    result = await session.execute(
        text(_PLAN_RELEASE_SERVING_SQL),
        {
            "plan_release_id": normalized_release_id,
            "pin_owner_type": PLAN_RELEASE_PIN_OWNER_TYPE,
        },
    )
    return _selection_from_rows(normalized_release_id, result)


def binding_query_args(
    args: Mapping[str, Any],
    binding: PlanReleaseSnapshotBinding,
) -> dict[str, Any]:
    """Bind a physical snapshot read to the release's source-local plan key."""

    resolved_args_by_name = dict(args)
    resolved_args_by_name.update(
        plan_id=binding.plan_id,
        plan_external_id=None,
        plan_market_type=binding.plan_market_type or None,
        source_key=binding.source_key,
        snapshot_id=binding.snapshot_id,
    )
    return resolved_args_by_name


def annotate_plan_release_response(
    payload: dict[str, Any] | None,
    selection: PlanReleaseServingSelection,
) -> dict[str, Any] | None:
    """Attach immutable release coordinates to top-level and query metadata."""

    if payload is None:
        return None
    metadata = selection.response_metadata()
    payload.setdefault("resolved", True)
    payload.update(metadata)
    query_by_field = payload.get("query")
    if not isinstance(query_by_field, dict):
        query_by_field = {}
        payload["query"] = query_by_field
    query_by_field.update(metadata)
    return payload
