# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact billing-identity traversal for one generation-bound provider page."""

from __future__ import annotations

from dataclasses import dataclass

from api import (
    billing_search_pagination,
    plan_release_serving,
    ptg2_billing_code_reader,
    ptg2_billing_entity_source_resolution,
    ptg2_billing_exact_reader,
    ptg2_billing_geo_reader,
    ptg2_billing_search_page,
    ptg2_serving,
)
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
)
from api.billing_search_endpoint_access import (
    BillingSearchEndpointAccess,
    validate_billing_search_endpoint_access,
)
from api.plan_release_serving import PlanReleaseServingSelection
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_NOT_FOUND,
    PLAN_RELEASE_RESOLUTION_READY,
)
from api.ptg2_billing_entity_refs import PTG2BillingAssociationDataError
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BillingSearchProviderCandidate,
    BillingSearchServiceResult,
    BillingSearchServingUnavailableError,
    serving_unavailable,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


@dataclass(frozen=True, slots=True)
class _BillingSearchTraversal:
    candidates: tuple[BillingSearchProviderCandidate, ...]
    has_identity: bool
    has_provider_rates: bool
    is_identity_projection_available: bool


def _empty_result(
    state: str,
    selection: PlanReleaseServingSelection | None,
) -> BillingSearchServiceResult:
    return BillingSearchServiceResult(
        state=state,
        providers=(),
        next_cursor=None,
        has_more=False,
        selection=selection,
    )


async def _binding_source_scope(
    session,
    *,
    serving_tables,
    billing_entity_ref: str,
):
    snapshot_key = ptg2_serving._required_shared_snapshot_key(serving_tables)
    return await ptg2_billing_entity_source_resolution.resolve_billing_entity_ref_source_scope(
        session,
        schema_name=ptg2_serving.PTG2_SCHEMA,
        snapshot_key=snapshot_key,
        billing_entity_ref=billing_entity_ref,
    )


async def _binding_candidates(
    session,
    *,
    binding,
    serving_tables,
    source_scope,
    request,
) -> tuple[tuple[BillingSearchProviderCandidate, ...], bool]:
    code_witnesses = await ptg2_billing_code_reader.load_exact_billing_code_witnesses(
        session,
        serving_tables,
        binding,
        code_system=request.code_system,
        code=request.code,
    )
    if not code_witnesses:
        return (), False
    rate_witnesses = (
        await (
            ptg2_billing_exact_reader.load_exact_billing_rate_occurrence_witnesses(
                session,
                serving_tables,
                source_scope=source_scope,
                code_keys=tuple(witness.code_key for witness in code_witnesses),
            )
        )
    )
    provider_rate_witnesses = (
        await (
            ptg2_billing_geo_reader.expand_billing_rate_witnesses_to_npis(
                session,
                serving_tables,
                rate_witnesses=rate_witnesses,
                provider_npi=request.provider_npi,
            )
        )
    )
    if not provider_rate_witnesses:
        return (), False
    geo_selection = await ptg2_billing_geo_reader.load_exact_billing_geo_witnesses(
        session,
        serving_tables,
        provider_rate_witnesses=provider_rate_witnesses,
        geo_args=request.geo_args,
    )
    if not geo_selection.address_projection_available:
        raise serving_unavailable()
    return (
        ptg2_billing_search_page.group_billing_geo_candidates(
            binding=binding,
            serving_tables=serving_tables,
            code_witnesses=code_witnesses,
            geo_witnesses=geo_selection.witnesses,
        ),
        True,
    )


async def _traverse_release(
    session,
    *,
    selection: PlanReleaseServingSelection,
    request,
) -> _BillingSearchTraversal:
    candidates: list[BillingSearchProviderCandidate] = []
    has_identity = False
    has_provider_rates = False
    for binding in selection.in_network_bindings:
        serving_tables = selection.serving_tables_for_snapshot(binding.snapshot_id)
        if serving_tables is None:
            raise serving_unavailable()
        try:
            source_scope = await _binding_source_scope(
                session,
                serving_tables=serving_tables,
                billing_entity_ref=request.billing_entity_ref,
            )
        except PTG2BillingAssociationDataError:
            return _BillingSearchTraversal((), False, False, False)
        if source_scope is None:
            continue
        has_identity = True
        binding_candidates, binding_has_provider_rates = await _binding_candidates(
            session,
            binding=binding,
            serving_tables=serving_tables,
            source_scope=source_scope,
            request=request,
        )
        has_provider_rates = has_provider_rates or binding_has_provider_rates
        candidates.extend(binding_candidates)
    return _BillingSearchTraversal(
        candidates=tuple(sorted(candidates, key=lambda candidate: candidate.sort_key)),
        has_identity=has_identity,
        has_provider_rates=has_provider_rates,
        is_identity_projection_available=True,
    )


async def _ready_release_and_cursor(
    session,
    *,
    access: BillingSearchEndpointAccess,
    cursor_keyring: BillingSearchCursorKeyring,
):
    resolution = await plan_release_serving.resolve_plan_release_serving_resolution(
        session,
        access.request.plan_release_id,
    )
    if resolution.state == PLAN_RELEASE_RESOLUTION_NOT_FOUND:
        return resolution, None, None
    if (
        resolution.state != PLAN_RELEASE_RESOLUTION_READY
        or resolution.selection is None
    ):
        raise serving_unavailable()
    generation_pin = (
        await (
            billing_search_pagination.capture_billing_search_generation_pin(
                session,
                resolution.selection,
            )
        )
    )
    cursor_binding = billing_search_pagination.build_billing_search_cursor_binding(
        access.request,
        access.authorization_context,
        generation_pin,
        trusted_now=access.trusted_now,
    )
    after_sort_key = billing_search_pagination.open_billing_search_page_cursor(
        access.request,
        keyring=cursor_keyring,
        binding=cursor_binding,
    )
    return resolution, cursor_binding, after_sort_key


def _state_for_empty_traversal(
    traversal: _BillingSearchTraversal,
) -> str:
    if not traversal.is_identity_projection_available:
        return BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE
    if not traversal.has_identity:
        return BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY
    if traversal.has_provider_rates and not traversal.candidates:
        return BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS
    return BILLING_SEARCH_RESULT_NO_MATCHING_RATES


def _sealed_next_cursor(
    provider_page,
    *,
    cursor_keyring: BillingSearchCursorKeyring,
    cursor_binding,
) -> str | None:
    if not provider_page.has_more:
        return None
    try:
        return billing_search_pagination.seal_billing_search_page_cursor(
            provider_page.next_sort_key,
            keyring=cursor_keyring,
            binding=cursor_binding,
        )
    except BillingSearchCursorError:
        raise serving_unavailable() from None


async def _search_ready_release(
    session,
    *,
    access: BillingSearchEndpointAccess,
    cursor_keyring: BillingSearchCursorKeyring,
    selection: PlanReleaseServingSelection,
    cursor_binding,
    after_sort_key,
) -> BillingSearchServiceResult:
    traversal = await _traverse_release(
        session,
        selection=selection,
        request=access.request,
    )
    if not traversal.candidates:
        if after_sort_key is not None:
            raise serving_unavailable()
        return _empty_result(_state_for_empty_traversal(traversal), selection)
    provider_page = await ptg2_billing_search_page.hydrate_billing_search_page(
        session,
        candidates=traversal.candidates,
        after_sort_key=after_sort_key,
        limit=access.request.limit,
        price_filter_args=access.request.price_filter_args,
    )
    if not provider_page.providers:
        if after_sort_key is not None:
            raise serving_unavailable()
        return _empty_result(BILLING_SEARCH_RESULT_NO_MATCHING_RATES, selection)
    next_cursor = _sealed_next_cursor(
        provider_page,
        cursor_keyring=cursor_keyring,
        cursor_binding=cursor_binding,
    )
    return BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_MATCHED,
        providers=provider_page.providers,
        next_cursor=next_cursor,
        has_more=provider_page.has_more,
        selection=selection,
    )


async def search_exact_billing_provider_page(
    session,
    *,
    access: BillingSearchEndpointAccess,
    cursor_keyring: BillingSearchCursorKeyring,
) -> BillingSearchServiceResult:
    """Serve one exact TIN-group-rate-NPI-address page without fallbacks."""

    validated_access = validate_billing_search_endpoint_access(access)
    if type(cursor_keyring) is not BillingSearchCursorKeyring:
        raise serving_unavailable()
    try:
        resolution, cursor_binding, after_sort_key = await _ready_release_and_cursor(
            session,
            access=validated_access,
            cursor_keyring=cursor_keyring,
        )
        if resolution.state == PLAN_RELEASE_RESOLUTION_NOT_FOUND:
            if validated_access.request.cursor is not None:
                raise BillingSearchCursorGenerationExpired(
                    "billing_search_cursor_generation_expired"
                )
            return _empty_result(BILLING_SEARCH_RESULT_NO_SNAPSHOT, None)
        selection = resolution.selection
        if selection is None or cursor_binding is None:
            raise serving_unavailable()
        return await _search_ready_release(
            session,
            access=validated_access,
            cursor_keyring=cursor_keyring,
            selection=selection,
            cursor_binding=cursor_binding,
            after_sort_key=after_sort_key,
        )
    except BillingSearchServingUnavailableError:
        raise
    except PTG2ManifestArtifactError:
        raise serving_unavailable() from None


__all__ = ["search_exact_billing_provider_page"]
