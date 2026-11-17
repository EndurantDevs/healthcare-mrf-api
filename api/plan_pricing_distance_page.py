# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact merge and continuation for ranked plan pricing batches."""

from __future__ import annotations

import hashlib
import hmac
import math
import time
from dataclasses import dataclass
from itertools import takewhile
from typing import Any, Mapping, Sequence

import orjson

from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
    open_billing_search_cursor,
    seal_billing_search_cursor,
)
from api.billing_search_cursor_keys import BillingSearchCursorKeyringError
from api.network_address_scope import current_network_address_scope
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, canonical_json
from api.plan_pricing_state_scan import _cursor_keyring
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
    annotate_plan_release_response,
)
from api.provider_service_legacy_selection import service_legacy_selection_sha256
from api.provider_specialty_filters import _normalize_taxonomy_codes
from api.ptg2_code_details import _enrich_ptg2_code_details
from api.ptg2_response import _fragment_exact_numbers, _shape_ptg2_response
from api.ptg_membership_distance import distance_input_snapshot
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_POSITION_KIND = "distance-v1"
_PUBLIC_SCOPE = hashlib.sha256(b"HEALTHPORTA_PLAN_PRICING_DISTANCE_PUBLIC_V1").hexdigest()
DISTANCE_CONTINUATION_FIELD = "distance_continuation"
DISTANCE_QUERY_COORDINATES_FIELD = "distance_query_coordinates"
DISTANCE_QUERY_TAXONOMY_CODES_FIELD = "distance_query_taxonomy_codes"
_DISTANCE_UNRESOLVED_SPECIALTY_FIELD = "distance_unresolved_specialty"


def is_distance_order(args: Mapping[str, Any]) -> bool:
    """Identify the explicit ascending or descending distance ordering family."""
    return str(args.get("order_by") or "").strip().lower() in {"distance", "distance_miles"}


def is_distance_continuation_query(args, pagination, *, include_allowed_amounts, include_providers) -> bool:
    """Require the bounded full-rate query and its established display ordering."""
    from api.ptg2_serving import _include_unverified_addresses

    return bool(
        args["order"] == "asc"
        and args["plan_release_id"]
        and pagination.offset == 0
        and (args["lat"] is not None or (args["zip5"] and args["zip_radius_miles"] > 0))
        and args["code"]
        and not args["q"]
        and args["npi"] is None
        and not include_allowed_amounts
        and include_providers
        and _include_unverified_addresses(args)
        and not any(
            args[field]
            for field in ("pos", "service_code", "modifier", "billing_code_modifier", "rate", "negotiated_rate")
        )
    )


def bind_distance_continuation(args, request_args, cursor, coordinates, specialty_filter) -> None:
    """Attach server-owned cursor state only after route eligibility is checked."""
    taxonomy_codes = (
        _normalize_taxonomy_codes(request_args.get("taxonomy_codes"))
        or _normalize_taxonomy_codes(request_args.get("taxonomy_code"))
        or specialty_filter.taxonomy_codes
    )
    args.update(
        {
            "taxonomy_codes": taxonomy_codes or None,
            DISTANCE_CONTINUATION_FIELD: cursor,
            DISTANCE_QUERY_COORDINATES_FIELD: coordinates,
            DISTANCE_QUERY_TAXONOMY_CODES_FIELD: (
                request_args.get("taxonomy_codes") or request_args.get("taxonomy_code") or None
            ),
            _DISTANCE_UNRESOLVED_SPECIALTY_FIELD: specialty_filter.unresolved_specialty if not taxonomy_codes else None,
        }
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _distance_query(args: Mapping[str, Any]) -> dict[str, Any]:
    """Bind caller selectors independently from resolved geography and taxonomy."""
    query_by_field = {
        key: option_value
        for key, option_value in args.items()
        if key
        not in {
            DISTANCE_CONTINUATION_FIELD,
            DISTANCE_QUERY_COORDINATES_FIELD,
            DISTANCE_QUERY_TAXONOMY_CODES_FIELD,
            _DISTANCE_UNRESOLVED_SPECIALTY_FIELD,
        }
    }
    coordinates = args.get(DISTANCE_QUERY_COORDINATES_FIELD, (args.get("lat"), args.get("long")))
    query_by_field.update(lat=coordinates[0], long=coordinates[1])
    if DISTANCE_QUERY_TAXONOMY_CODES_FIELD in args:
        query_by_field["taxonomy_codes"] = args[DISTANCE_QUERY_TAXONOMY_CODES_FIELD]
    return query_by_field


def _distance_scope(
    selection: PlanReleaseServingSelection,
    bindings: Sequence[PlanReleaseSnapshotBinding],
    args: Mapping[str, Any],
    limit: int,
    provider_inputs: Mapping[str, Any],
) -> tuple[str, str, str, str]:
    snapshot_set = _digest(
        {
            "release_binding_digest": selection.binding_set_digest,
            "admitted_bindings": [
                (
                    binding.binding_ordinal,
                    binding.source_key,
                    binding.snapshot_id,
                    binding.plan_id,
                    binding.plan_market_type,
                )
                for binding in bindings
            ],
        }
    )
    network = current_network_address_scope()
    authorization = _PUBLIC_SCOPE
    generation_by_field = {
        "revision": selection.serving_revision_id,
        "snapshot_set": snapshot_set,
        "provider_inputs": provider_inputs,
        "resolved_center": (args.get("lat"), args.get("long")),
        "resolved_taxonomy_codes": args.get("taxonomy_codes"),
        "unresolved_specialty": args.get(_DISTANCE_UNRESOLVED_SPECIALTY_FIELD),
    }
    if network is not None:
        if network.access_scope_sha256 is None:
            raise PlanPricingProjectionUnavailable("distance continuation requires an authenticated network scope")
        authorization = network.access_scope_sha256
        generation_by_field["network"] = {
            "generation": network.manifest.generation_id,
            "approved_custom_revision": network.manifest.approved_custom_revision,
            "manifest_sha256": network.manifest.manifest_sha256,
        }
    return (
        _digest(
            {
                "kind": _POSITION_KIND,
                "plan_release_id": selection.plan_release_id,
                "limit": limit,
                "query": _distance_query(args),
            }
        ),
        authorization,
        _digest({**generation_by_field, "legacy": service_legacy_selection_sha256()}),
        snapshot_set,
    )


@dataclass(frozen=True)
class RankedSnapshotBatch:
    source_key: str
    snapshot_id: str
    items: list[dict[str, Any]]
    exhausted: bool


@dataclass(frozen=True)
class DistancePagePosition:
    distance: float | None
    npi: int
    group_offset: int
    emitted: int
    group_sha256: str

    @property
    def rank(self) -> tuple[float, int]:
        """Compare provider ranks with unknown distances last."""
        return (math.inf if self.distance is None else self.distance, self.npi)


@dataclass(frozen=True)
class RankedDistancePage:
    items: list[dict[str, Any]]
    offset: int
    total_lower_bound: int
    total_is_exact: bool
    next_position: DistancePagePosition | None


def _rank(item: dict[str, Any]) -> tuple[float, int]:
    distance, npi = item.get("distance_miles"), item.get("npi")
    if type(npi) not in (int, str) or not str(npi).isascii() or not str(npi).isdigit() or int(npi) < 1:
        raise PTG2ManifestArtifactError("PTG2 ranked result is missing its provider identity")
    if distance is None:
        return (math.inf, int(npi))
    if type(distance) not in (int, float) or not math.isfinite(distance) or distance < 0:
        raise PTG2ManifestArtifactError("PTG2 ranked result has an invalid distance")
    return (float(distance), int(npi))


def _group_digest(items: list[dict[str, Any]]) -> str:
    payload = orjson.dumps(_fragment_exact_numbers(items), option=orjson.OPT_SORT_KEYS, default=str)
    return hashlib.sha256(payload).hexdigest()


def merge_ranked_distance_page(
    batches: list[RankedSnapshotBatch], limit: int, after: DistancePagePosition | None = None
) -> RankedDistancePage:
    """Emit only a complete prefix of the union of admitted snapshot batches."""
    from api.ptg2_serving import _ptg2_distance_sort_key

    if limit < 1:
        raise ValueError("Ranked pricing page size must be positive")

    ordered_groups: list[dict[str, Any]] = []
    incomplete_frontiers: list[tuple[float, int]] = []
    for batch in batches:
        ranked_items = []
        for rate_group in batch.items:
            if (rate_group.get("source_key"), rate_group.get("snapshot_id")) != (batch.source_key, batch.snapshot_id):
                raise PTG2ManifestArtifactError("PTG2 ranked result escaped its snapshot binding")
            _rank(rate_group)
            group_by_field = dict(rate_group)
            if len(batches) > 1:
                group_by_field.setdefault("network", batch.source_key)
            ranked_items.append(group_by_field)
        if not batch.exhausted:
            if not ranked_items:
                raise PTG2ManifestArtifactError("PTG2 ranked snapshot has no proven distance frontier")
            incomplete_frontiers.append(max(map(_rank, ranked_items)))
        ordered_groups.extend(ranked_items)

    keys = [_ptg2_distance_sort_key(rate_group, is_descending=False) for rate_group in ordered_groups]
    if len(set(keys)) != len(keys):
        raise PTG2ManifestArtifactError("PTG2 ranked snapshot union repeated a rate group")
    ordered_groups = [
        rate_group for _, rate_group in sorted(zip(keys, ordered_groups, strict=True), key=lambda pair: pair[0])
    ]
    offset, skip = 0, 0
    if after is not None:
        tied_groups = list(takewhile(lambda rate_group: _rank(rate_group) == after.rank, ordered_groups))
        if not (
            1 <= after.group_offset <= len(tied_groups)
            and hmac.compare_digest(_group_digest(tied_groups), after.group_sha256)
        ):
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        offset, skip = after.emitted, after.group_offset

    remaining = ordered_groups[skip:]
    if incomplete_frontiers and (len(remaining) <= limit or min(incomplete_frontiers) < _rank(remaining[limit])):
        raise PTG2ManifestArtifactError("PTG2 ranked snapshots cannot prove the next complete page")

    page = remaining[:limit]
    position = None
    if len(remaining) > limit:
        rank = _rank(page[-1])
        tied_groups = [rate_group for rate_group in ordered_groups if _rank(rate_group) == rank]
        consumed = sum(_rank(rate_group) == rank for rate_group in ordered_groups[: skip + len(page)])
        position = DistancePagePosition(
            None if rank[0] == math.inf else rank[0], rank[1], consumed, offset + len(page), _group_digest(tied_groups)
        )
    return RankedDistancePage(page, offset, offset + len(remaining), not incomplete_frontiers, position)


def seal_distance_position(
    position: DistancePagePosition,
    *,
    keyring: BillingSearchCursorKeyring,
    trusted_now: int,
    scope: tuple[str, str, str, str],
) -> str:
    """Authenticate the complete tie-group position and release scope."""
    return seal_billing_search_cursor(
        BillingSearchCursorState(
            request_fingerprint_sha256=scope[0],
            authorization_context_sha256=scope[1],
            generation_bundle_sha256=scope[2],
            snapshot_set_sha256=scope[3],
            sort_key=(
                _POSITION_KIND,
                "null" if position.distance is None else position.distance.hex(),
                position.npi,
                position.group_offset,
                position.emitted,
                position.group_sha256,
            ),
            issued_at=trusted_now,
            expires_at=trusted_now + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
        ),
        keyring=keyring,
        trusted_now=trusted_now,
    )


def open_distance_position(
    cursor: object,
    *,
    keyring: BillingSearchCursorKeyring,
    trusted_now: int,
    scope: tuple[str, str, str, str],
) -> DistancePagePosition:
    """Reject malformed positions and cursors from a different scope."""
    state = open_billing_search_cursor(
        cursor,
        keyring=keyring,
        trusted_now=trusted_now,
        request_fingerprint_sha256=scope[0],
        authorization_context_sha256=scope[1],
        generation_bundle_sha256=scope[2],
        snapshot_set_sha256=scope[3],
    )
    if len(state.sort_key) != 6:
        raise BillingSearchCursorError("billing_search_cursor_invalid")
    kind, distance_key, npi, group_offset, emitted, group_sha256 = state.sort_key
    if (
        kind != _POSITION_KIND
        or type(distance_key) is not str
        or any(type(position_value) is not int for position_value in (npi, group_offset, emitted))
        or not (npi > 0 and 0 < group_offset <= emitted)
        or type(group_sha256) is not str
        or len(group_sha256) != 64
        or any(digit not in "0123456789abcdef" for digit in group_sha256)
    ):
        raise BillingSearchCursorError("billing_search_cursor_invalid")
    distance = None
    if distance_key != "null":
        try:
            distance = float.fromhex(distance_key)
        except ValueError, OverflowError:
            raise BillingSearchCursorError("billing_search_cursor_invalid") from None
        if not math.isfinite(distance) or distance < 0 or distance.hex() != distance_key:
            raise BillingSearchCursorError("billing_search_cursor_invalid")
    return DistancePagePosition(distance, npi, group_offset, emitted, group_sha256)


def _ranked_distance_document(
    page: RankedDistancePage,
    limit: int,
    next_cursor: str | None,
    batches: Sequence[RankedSnapshotBatch],
    metadata: dict[str, Any],
    args: dict[str, Any],
) -> dict[str, Any]:
    """Preserve full negotiated rows and expose the exactness of this traversal."""
    from api.ptg2_serving import _apply_address_display_policy, _hide_source_artifact_key_unless_requested

    for provider_item_by_field in page.items:
        _apply_address_display_policy(
            provider_item_by_field, {**args, "plan_id": provider_item_by_field.get("plan_id")}
        )
    _hide_source_artifact_key_unless_requested(page.items, args)
    pagination_by_field = {
        "limit": limit,
        "offset": page.offset,
        "page": page.offset // limit + 1,
        "total": page.total_lower_bound,
        "total_lower_bound": page.total_lower_bound,
        "total_is_exact": page.total_is_exact,
        "has_more": next_cursor is not None,
        "next_cursor": next_cursor,
    }
    if len(batches) > 1:
        metadata = {
            "query": {
                **metadata.get("query", {}),
                "source_key": None,
                "snapshot_id": None,
                "snapshots": [batch.snapshot_id for batch in batches],
                "networks": [
                    {"source_key": batch.source_key, "snapshot_id": batch.snapshot_id}
                    for batch in batches
                    if batch.items
                ],
                "combined": True,
            }
        }
    return {**metadata, "items": page.items, "pagination": pagination_by_field}


async def _read_ranked_distance_batches(
    session,
    bindings: Sequence[PlanReleaseSnapshotBinding],
    tables_by_snapshot: Mapping[str, Any],
    args: dict[str, Any],
    candidate_target: int,
    after: tuple[float | None, int] | None,
) -> tuple[list[RankedSnapshotBatch], dict[str, Any]]:
    """Keep every admitted source and the first matching response metadata."""
    from api.ptg2_serving import _read_ranked_distance_snapshot

    batches, metadata = [], {}
    for binding in bindings:
        batch, snapshot_metadata = await _read_ranked_distance_snapshot(
            session, binding, tables_by_snapshot[binding.snapshot_id], args, candidate_target, after
        )
        batches.append(batch)
        if not metadata:
            metadata = snapshot_metadata
    return batches, metadata


def _supports_ranked_distance_bindings(bindings, tables_by_snapshot, args, limit):
    """Require distinct snapshots that can prove complete bounded rate groups."""
    from api.ptg2_serving import _uses_local_distance_rate_scope

    if len({(binding.source_key, binding.snapshot_id) for binding in bindings}) != len(bindings):
        raise PlanPricingProjectionUnavailable("ranked distance continuation requires distinct source snapshots")
    return all(
        _uses_local_distance_rate_scope(tables_by_snapshot[binding.snapshot_id], args, {}, None, None, limit + 2)
        for binding in bindings
    )


async def search_ranked_distance_release(
    session,
    selection: PlanReleaseServingSelection,
    bindings: tuple[PlanReleaseSnapshotBinding, ...],
    tables_by_snapshot: Mapping[str, Any],
    args: dict[str, Any],
    pagination,
) -> dict[str, Any] | None:
    """Read every admitted snapshot under the same exact continuation boundary."""
    from api.ptg2_serving import (
        PTG2_SCHEMA,
        _plan_release_no_match_response,
    )

    cursor = args[DISTANCE_CONTINUATION_FIELD]
    if not _supports_ranked_distance_bindings(bindings, tables_by_snapshot, args, pagination.limit):
        if cursor is not None:
            raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
        return None
    try:
        keyring = _cursor_keyring()
    except BillingSearchCursorKeyringError:
        if cursor is not None:
            raise
        return None

    async with distance_input_snapshot(session, PTG2_SCHEMA, args) as provider_inputs:
        if provider_inputs is None:
            if cursor is not None:
                raise BillingSearchCursorGenerationExpired("billing_search_cursor_generation_expired")
            return None
        now = int(time.time())
        scope = _distance_scope(selection, bindings, args, pagination.limit, provider_inputs)
        position = (
            None if cursor is None else open_distance_position(cursor, keyring=keyring, trusted_now=now, scope=scope)
        )
        if args.get(_DISTANCE_UNRESOLVED_SPECIALTY_FIELD):
            raise BillingSearchCursorError("billing_search_cursor_invalid")
        after = None if position is None else (position.distance, position.npi)
        candidate_target = pagination.limit + 1 + (position is not None)
        batches, metadata = await _read_ranked_distance_batches(
            session, bindings, tables_by_snapshot, args, candidate_target, after
        )
        page = merge_ranked_distance_page(batches, pagination.limit, position)
        next_cursor = None
        if page.next_position is not None:
            next_cursor = seal_distance_position(page.next_position, keyring=keyring, trusted_now=now, scope=scope)
    response_by_field = _ranked_distance_document(page, pagination.limit, next_cursor, batches, metadata, args)
    if not page.items:
        empty_response_by_field = _plan_release_no_match_response(
            selection, args, pagination, contributing_bindings=bindings
        )
        empty_response_by_field["pagination"] = response_by_field["pagination"]
        return empty_response_by_field
    response_by_field = await _enrich_ptg2_code_details(session, response_by_field, args)
    response_by_field = (
        annotate_plan_release_response(response_by_field, selection, contributing_bindings=bindings)
        or response_by_field
    )
    return _shape_ptg2_response(response_by_field, args)
