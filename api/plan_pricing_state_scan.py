# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed-work, release-bound statewide plan-pricing traversal."""

from __future__ import annotations

import hashlib
import os
import time
from functools import lru_cache
from typing import Any, Mapping

from sqlalchemy import text

from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorError,
    BillingSearchCursorState,
    open_billing_search_cursor,
    seal_billing_search_cursor,
)
from api.billing_search_cursor_keys import (
    BILLING_SEARCH_CURSOR_KEYRING_ENV,
    load_billing_search_cursor_keyring,
)
from api.plan_pricing_projection_contract import (
    PROJECTION_CONTRACT,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    canonical_json,
    row_mapping,
)
from api.plan_pricing_state_scan_contract import (
    STATE_SCAN_MAX_LIMIT,
    STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT,
    STATE_SCAN_RATE_OCCURRENCE_LIMIT,
    STATE_SCAN_RESPONSE_BYTE_LIMIT,
    PlanPricingStateScanBudgetExceeded,
    is_plan_pricing_state_scan,
    pagination_metadata as _pagination_metadata,
    query_metadata as _query_metadata,
    response_document as _response_document,
    validate_plan_pricing_state_scan,
)
from api.plan_pricing_state_scan_hydration import (
    eligible_provider_npis as _eligible_provider_npis,
    hydrate_selected_groups as _hydrate_selected_groups,
)
from api.plan_pricing_state_scan_sql import (
    state_scan_page_sql as _page_sql,
    state_scan_provider_page_sql as _provider_page_sql,
)
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_response import _is_request_flag_enabled
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_AUTHORIZATION_SCOPE_SHA256 = hashlib.sha256(b"HEALTHPORTA_PLAN_PRICING_STATE_SCAN_PUBLIC_V1").hexdigest()


@lru_cache(maxsize=1)
def _cursor_keyring_for_document(document: str | None):
    environment = {} if document is None else {BILLING_SEARCH_CURSOR_KEYRING_ENV: document}
    return load_billing_search_cursor_keyring(environment)


def _cursor_keyring():
    return _cursor_keyring_for_document(os.environ.get(BILLING_SEARCH_CURSOR_KEYRING_ENV))


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _request_fingerprint(
    args: Mapping[str, Any],
    code_system: str,
    code: str,
    state: str,
    limit: int,
) -> str:
    return _digest(
        {
            "code": code,
            "code_system": code_system,
            "include_code_details": _is_request_flag_enabled(args.get("include_code_details"), default=False),
            "include_debug": _is_request_flag_enabled(args.get("include_debug"), default=False),
            "include_details": _is_request_flag_enabled(args.get("include_details"), default=False),
            "include_evidence": _is_request_flag_enabled(args.get("include_evidence"), default=False),
            "include_sources": _is_request_flag_enabled(args.get("include_sources"), default=False),
            "include_unverified_addresses": _is_request_flag_enabled(
                args.get("include_unverified_addresses"), default=True
            ),
            "limit": limit,
            "mode": str(args.get("mode") or "").strip(),
            "order": "asc",
            "order_by": "npi",
            "plan_release_id": str(args.get("plan_release_id")),
            "state": state,
            "view": "full",
        }
    )


def _cursor_scope(
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    code_system: str,
    code: str,
    state: str,
    limit: int,
) -> tuple[str, str, str, str]:
    # The reused AEAD format has a billing-domain AAD, but all four authenticated
    # scope digests below are scan-specific. A billing token therefore decrypts
    # under the shared keyring yet fails closed on the scope comparison. Each
    # successful page issues a fresh 15-minute cursor, so the TTL bounds idle
    # continuation time rather than total statewide traversal time.
    request_fingerprint = _request_fingerprint(args, code_system, code, state, limit)
    snapshot_set = _digest(
        {
            "binding_set_digest": selection.binding_set_digest,
            "bindings": [
                {
                    "binding_ordinal": binding.binding_ordinal,
                    "plan_id": binding.plan_id,
                    "plan_market_type": binding.plan_market_type,
                    "snapshot_id": binding.snapshot_id,
                    "source_key": binding.source_key,
                }
                for binding in selection.in_network_bindings
            ],
            "plan_release_id": selection.plan_release_id,
        }
    )
    generation_bundle = _digest(
        {
            "pricing_projection_contract": selection.pricing_projection_contract,
            "pricing_projection_id": selection.pricing_projection_id,
            "serving_revision_id": selection.serving_revision_id,
            "snapshot_set_sha256": snapshot_set,
        }
    )
    return (
        request_fingerprint,
        _AUTHORIZATION_SCOPE_SHA256,
        generation_bundle,
        snapshot_set,
    )


def _open_position(
    cursor: Any,
    *,
    keyring: Any,
    trusted_now: int,
    scope: tuple[str, str, str, str],
) -> tuple[int, int, int, int]:
    if cursor in (None, "", "null"):
        return 0, 0, 0, 0
    state = open_billing_search_cursor(
        cursor,
        keyring=keyring,
        trusted_now=trusted_now,
        request_fingerprint_sha256=scope[0],
        authorization_context_sha256=scope[1],
        generation_bundle_sha256=scope[2],
        snapshot_set_sha256=scope[3],
    )
    if len(state.sort_key) != 4 or any(type(value) is not int or value < 0 for value in state.sort_key):
        raise BillingSearchCursorError("billing_search_cursor_invalid")
    return (
        int(state.sort_key[0]),
        int(state.sort_key[1]),
        int(state.sort_key[2]),
        int(state.sort_key[3]),
    )


def _seal_position(
    position: tuple[int, int, int, int],
    *,
    keyring: Any,
    trusted_now: int,
    scope: tuple[str, str, str, str],
) -> str:
    return seal_billing_search_cursor(
        BillingSearchCursorState(
            request_fingerprint_sha256=scope[0],
            authorization_context_sha256=scope[1],
            generation_bundle_sha256=scope[2],
            snapshot_set_sha256=scope[3],
            sort_key=position,
            issued_at=trusted_now,
            expires_at=trusted_now + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
        ),
        keyring=keyring,
        trusted_now=trusted_now,
    )


async def _read_state_npis(
    session: Any,
    projection_id: str,
    state: str,
    after_npi: int,
    limit: int,
) -> tuple[tuple[int, ...], dict[int, Any], bool]:
    state_result = await session.execute(
        text(_provider_page_sql()),
        {
            "after_npi": after_npi,
            "npi_sentinel_limit": limit + 1,
            "projection_id": projection_id,
            "state": state,
        },
    )
    state_rows = [row_mapping(state_row) for state_row in state_result.mappings().all()]
    has_more = len(state_rows) > limit
    selected_rows = state_rows[:limit]
    selected_npis = tuple(int(state_row["npi"]) for state_row in selected_rows)
    return (
        selected_npis,
        {
            int(state_row["npi"]): state_row.get("provider_fragment")
            for state_row in selected_rows
        },
        has_more,
    )


async def _read_projected_occurrences(
    session: Any,
    projection_id: str,
    code_system: str,
    code: str,
    selected_npis: tuple[int, ...],
    limit: int,
) -> list[dict[str, Any]]:
    page_result = await session.execute(
        text(_page_sql()),
        {
            "code": code,
            "code_system": code_system,
            "membership_limit": STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT,
            "membership_probe_limit": STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT + 1,
            "membership_sentinel_limit": (STATE_SCAN_PROVIDER_MEMBERSHIP_LIMIT + 1),
            "occurrence_probe_limit": STATE_SCAN_RATE_OCCURRENCE_LIMIT + 1,
            "occurrence_sentinel_limit": STATE_SCAN_RATE_OCCURRENCE_LIMIT + 1,
            "page_row_limit": STATE_SCAN_RATE_OCCURRENCE_LIMIT + limit + 1,
            "projection_id": projection_id,
            "selected_npis": list(selected_npis),
        },
    )
    return [row_mapping(database_row) for database_row in page_result.mappings().all()]


def _validated_occurrences(
    database_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if any(database_row.get("membership_budget_exceeded") is True for database_row in database_rows):
        raise PlanPricingStateScanBudgetExceeded("state scan page exceeds its provider-membership budget")
    occurrence_rows = [
        database_row for database_row in database_rows if database_row.get("binding_ordinal") is not None
    ]
    logical_occurrences = sum(
        int(occurrence_by_field.get("occurrence_multiplicity") or 0) for occurrence_by_field in occurrence_rows
    )
    if (
        len(occurrence_rows) > STATE_SCAN_RATE_OCCURRENCE_LIMIT
        or logical_occurrences > STATE_SCAN_RATE_OCCURRENCE_LIMIT
    ):
        raise PlanPricingStateScanBudgetExceeded("state scan page exceeds its complete rate-group budget")
    occurrence_rows.sort(
        key=lambda occurrence_by_field: (
            int(occurrence_by_field["npi"]),
            int(occurrence_by_field["binding_ordinal"]),
            int(occurrence_by_field["occurrence_ordinal"]),
            int(occurrence_by_field["provider_set_key"]),
        )
    )
    return occurrence_rows


def _validate_search_request(
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> tuple[str, str, str, Any]:
    code_system, code, state = validate_plan_pricing_state_scan(args)
    if (
        selection is None
        or not selection.pricing_projection_id
        or selection.pricing_projection_contract != PROJECTION_CONTRACT
    ):
        raise PlanPricingProjectionUnavailable("the selected release has no ready state-scan projection")
    if not 1 <= int(pagination.limit) <= STATE_SCAN_MAX_LIMIT:
        raise PlanPricingProjectionUnsupported(f"state scan limit must be between 1 and {STATE_SCAN_MAX_LIMIT}")
    cursor = args.get("cursor")
    if int(pagination.offset) != 0:
        message = (
            "state scan cursor requires compatibility offset 0"
            if cursor not in (None, "", "null")
            else "state scan starts at offset 0; use its cursor for continuation"
        )
        raise PlanPricingProjectionUnsupported(message)
    return code_system, code, state, cursor


def _next_page_cursor(
    selected_npis: tuple[int, ...],
    scanned_after: int,
    emitted_after: int,
    page_number: int,
    *,
    has_more: bool,
    keyring: Any,
    trusted_now: int,
    scope: tuple[str, str, str, str],
) -> str | None:
    if not has_more:
        return None
    if not selected_npis:
        raise PTG2ManifestArtifactError("pricing state scan cursor made no progress")
    return _seal_position(
        (selected_npis[-1], scanned_after, emitted_after, page_number),
        keyring=keyring,
        trusted_now=trusted_now,
        scope=scope,
    )


def _search_cursor_context(
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    code_system: str,
    code: str,
    state: str,
    limit: int,
    cursor: Any,
) -> tuple[int, Any, tuple[str, str, str, str], int, int, int, int]:
    trusted_now = int(time.time())
    keyring = _cursor_keyring()
    scope = _cursor_scope(selection, args, code_system, code, state, limit)
    after_npi, scanned_before, emitted_before, completed_page_count = _open_position(
        cursor,
        keyring=keyring,
        trusted_now=trusted_now,
        scope=scope,
    )
    return (
        trusted_now, keyring, scope, after_npi,
        scanned_before, emitted_before, completed_page_count,
    )


async def _prefix_response(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    code_identity: tuple[str, str, str],
    provider_page: tuple[tuple[int, ...], dict[int, Any], bool, frozenset[int]],
    cursor_context: tuple[Any, ...],
    requested_limit: int,
    selected_npis: tuple[int, ...],
) -> dict[str, Any]:
    """Build one complete bounded prefix response."""

    candidate_npis, provider_fragments, state_has_more, eligible_candidates = provider_page
    eligible_npis = tuple(npi for npi in selected_npis if npi in eligible_candidates)
    occurrence_rows = []
    if eligible_npis:
        occurrence_rows = _validated_occurrences(
            await _read_projected_occurrences(
                session, selection.pricing_projection_id,
                code_identity[0], code_identity[1], eligible_npis, len(eligible_npis),
            )
        )
    response_items = (
        await _hydrate_selected_groups(
            session, selection, args, occurrence_rows,
            {npi: provider_fragments[npi] for npi in eligible_npis},
        )
        if occurrence_rows
        else []
    )
    scanned_after = cursor_context[4] + len(selected_npis)
    emitted_after = cursor_context[5] + len(response_items)
    page_number = cursor_context[6] + 1
    has_more = state_has_more or len(selected_npis) < len(candidate_npis)
    next_cursor = _next_page_cursor(
        selected_npis, scanned_after, emitted_after, page_number,
        has_more=has_more,
        keyring=cursor_context[1], trusted_now=cursor_context[0], scope=cursor_context[2],
    )
    return _response_document(
        selection,
        response_items,
        _pagination_metadata(
            requested_limit, cursor_context[5], scanned_after,
            emitted_after, page_number, has_more, next_cursor,
        ),
        _query_metadata(*code_identity),
        args,
        byte_limit=STATE_SCAN_RESPONSE_BYTE_LIMIT,
    )


async def _adaptive_page_response(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    code_identity: tuple[str, str, str],
    provider_page: tuple[tuple[int, ...], dict[int, Any], bool],
    cursor_context: tuple[Any, ...],
    requested_limit: int,
) -> dict[str, Any]:
    """Return the largest attempted complete NPI prefix or refuse one NPI."""

    candidate_npis, provider_fragments, state_has_more = provider_page
    attempt_page = (
        candidate_npis,
        provider_fragments,
        state_has_more,
        frozenset(_eligible_provider_npis(provider_fragments, args)),
    )
    selected_npis = candidate_npis
    while True:
        try:
            return await _prefix_response(
                session,
                selection,
                args,
                code_identity,
                attempt_page,
                cursor_context,
                requested_limit,
                selected_npis,
            )
        except PlanPricingStateScanBudgetExceeded:
            if len(selected_npis) <= 1:
                raise
            selected_npis = selected_npis[: len(selected_npis) // 2]


async def search_plan_pricing_state_scan(
    session: Any,
    selection: PlanReleaseServingSelection,
    args: Mapping[str, Any],
    pagination: Any,
) -> dict[str, Any]:
    """Return one fixed-work NPI keyset page from an exact v4 release."""

    code_system, code, state, cursor = _validate_search_request(selection, args, pagination)
    requested_limit = int(pagination.limit)
    cursor_context = _search_cursor_context(
        selection, args, code_system, code, state, requested_limit, cursor,
    )
    provider_page = await _read_state_npis(
        session, selection.pricing_projection_id, state,
        cursor_context[3], requested_limit,
    )
    return await _adaptive_page_response(
        session, selection, args, (code_system, code, state),
        provider_page, cursor_context, requested_limit,
    )


__all__ = [
    "PlanPricingStateScanBudgetExceeded",
    "is_plan_pricing_state_scan",
    "search_plan_pricing_state_scan",
    "validate_plan_pricing_state_scan",
]
