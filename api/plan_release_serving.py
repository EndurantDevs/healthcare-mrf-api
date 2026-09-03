# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed resolution of canonical plan releases to exact PTG2 snapshots."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.plan_release_pricing_projection import (
    has_pricing_projection_relation,
    plan_release_serving_queries,
)
from api.plan_release_readiness import is_release_binding_serving_ready
from api.plan_release_serving_metadata import (
    plan_release_header_from_rows as _release_header_from_rows,
    single_text_value as _single_text_value,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


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

(
    _PLAN_RELEASE_SERVING_SQL,
    _PLAN_RELEASE_SERVING_WITHOUT_PROJECTION_SQL,
) = plan_release_serving_queries(PTG2_SCHEMA)


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
    pricing_projection_id: str | None = None
    pricing_projection_contract: str | None = None
    serving_revision_published_at: str | None = None
    _validated_serving_tables: tuple[tuple[str, PTG2ServingTables], ...] = field(
        default=(), repr=False, compare=False
    )
    _includes_billing_tax_identity_source: bool = field(default=False, repr=False, compare=False)

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

    def serving_tables_for_snapshot(
        self,
        snapshot_id: str,
    ) -> PTG2ServingTables | None:
        """Reuse a descriptor validated while resolving this exact release."""

        return next(
            (
                serving_tables
                for validated_snapshot_id, serving_tables in (
                    self._validated_serving_tables
                )
                if validated_snapshot_id == snapshot_id
            ),
            None,
        )

    def network_tables_by_snapshot(self) -> dict[str, PTG2ServingTables] | None:
        """Return complete readiness descriptors or fail a partial release."""

        serving_tables_by_snapshot_id = dict(
            self._validated_serving_tables
        )
        validated_snapshot_ids = [
            snapshot_id
            for snapshot_id, _ in self._validated_serving_tables
        ]
        expected_snapshot_ids = {
            binding.snapshot_id for binding in self.in_network_bindings
        }
        if (
            len(validated_snapshot_ids) != len(serving_tables_by_snapshot_id)
            or set(validated_snapshot_ids) != expected_snapshot_ids
        ):
            return None
        return serving_tables_by_snapshot_id

    @property
    def includes_billing_tax_identity_source(self) -> bool:
        """Prove descriptors were loaded with source-publication metadata."""
        return self._includes_billing_tax_identity_source is True

    def response_metadata(self) -> dict[str, Any]:
        """Return canonical coordinates suitable for a pricing response."""

        return {
            "healthporta_plan_id": self.healthporta_plan_id,
            "plan_release_id": self.plan_release_id,
            "plan_version_id": self.plan_version_id,
            "serving_revision_id": self.serving_revision_id,
            "serving_revision_published_at": (
                self.serving_revision_published_at
            ),
            "release_month": self.release_month,
            "release_status": self.release_status,
            "is_current": True,
            "binding_set_digest": self.binding_set_digest,
            "in_network_snapshot_ids": sorted(
                {binding.snapshot_id for binding in self.in_network_bindings}
            ),
        }


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
        pricing_projection_id=release_header.pricing_projection_id,
        pricing_projection_contract=(
            release_header.pricing_projection_contract
        ),
        serving_revision_published_at=(
            release_header.serving_revision_published_at
        ),
    )


async def _is_release_binding_set_serving_ready(
    session: Any,
    selection: PlanReleaseServingSelection,
    validated_serving_tables_by_snapshot_id: dict[str, PTG2ServingTables],
    *,
    include_billing_tax_identity_source: bool = False,
) -> bool:
    """Require every frozen binding to pass the normal PTG2 serving guard."""

    try:
        for binding in selection.bindings:
            readiness_options_by_name: dict[str, Any] = {}
            if include_billing_tax_identity_source:
                readiness_options_by_name["include_billing_tax_identity_source"] = True
            if not await is_release_binding_serving_ready(
                session,
                binding,
                validated_serving_tables_by_snapshot_id=validated_serving_tables_by_snapshot_id,
                **readiness_options_by_name,
            ):
                return False
    except PTG2ManifestArtifactError:
        return False
    return True


async def resolve_plan_release_serving(
    session: Any,
    plan_release_id: Any,
    *,
    include_billing_tax_identity_source: bool = False,
    projection_only: bool = False,
) -> PlanReleaseServingSelection | None:
    """Load one exact, complete, optionally source-aware pinned release."""

    if (
        type(include_billing_tax_identity_source) is not bool
        or type(projection_only) is not bool
    ):
        return None
    normalized_release_id = normalize_plan_release_id(plan_release_id)
    if normalized_release_id is None:
        return None
    release_sql = (
        _PLAN_RELEASE_SERVING_SQL
        if await has_pricing_projection_relation(session, PTG2_SCHEMA)
        else _PLAN_RELEASE_SERVING_WITHOUT_PROJECTION_SQL
    )
    release_query_result = await session.execute(
        text(release_sql),
        {
            "plan_release_id": normalized_release_id,
            "pin_owner_type": PLAN_RELEASE_PIN_OWNER_TYPE,
        },
    )
    selection = _selection_from_rows(
        normalized_release_id,
        release_query_result,
    )
    if selection is None:
        return None
    if projection_only:
        return selection
    validated_serving_tables_by_snapshot_id: dict[
        str, PTG2ServingTables
    ] = {}
    if not await _is_release_binding_set_serving_ready(
        session,
        selection,
        validated_serving_tables_by_snapshot_id,
        include_billing_tax_identity_source=include_billing_tax_identity_source,
    ):
        return None
    resolved_selection = replace(
        selection,
        _validated_serving_tables=tuple(validated_serving_tables_by_snapshot_id.items()),
        _includes_billing_tax_identity_source=include_billing_tax_identity_source,
    )
    if resolved_selection.network_tables_by_snapshot() is None:
        return None
    return resolved_selection


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
    if payload.get("pricing_scope") == "plan_scoped_allowed_amounts":
        metadata.pop("in_network_snapshot_ids", None)
    else:
        # Preserve the established negotiated-response alias for API clients.
        metadata["resolved_snapshot_ids"] = metadata[
            "in_network_snapshot_ids"
        ]
    payload.setdefault("resolved", True)
    payload.update(metadata)
    query_by_field = payload.get("query")
    if not isinstance(query_by_field, dict):
        query_by_field = {}
        payload["query"] = query_by_field
    query_by_field.update(metadata)
    return payload
