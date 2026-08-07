# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed public response shaping for exact billing-identity searches."""

from __future__ import annotations

import hmac
from typing import Any

from api import ptg2_serving
from api.billing_search_cursor import BillingSearchCursorKeyring
from api.billing_search_endpoint_access import (
    BillingSearchEndpointAccess,
    validate_billing_search_endpoint_access_state,
)
from api.billing_search_request import BillingSearchRequest
from api.billing_search_response_fields import (
    SourceGroupOrdinals,
    public_provider_payload,
)
from api.billing_search_response_validation import (
    _PublicResponseBudget,
    _validate_public_page,
    _validate_total_text_budget,
)
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchSelectorBindingScope,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_billing_exact_contract import source_groups
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BillingSearchMatchedProvider,
    BillingSearchServiceResult,
    BillingSearchServingUnavailableError,
    serving_unavailable,
    validate_service_result,
)
from api.ptg2_response import _fragment_exact_numbers

BILLING_SEARCH_PRICING_SCOPE = "plan_scoped_ptg_tax_identity"
BILLING_SEARCH_ASSOCIATION_SCOPE = "tax_identity_match_only"
BILLING_SEARCH_GEO_SCOPE = "provider_address_evidence"

SourceGroupsByBinding = dict[tuple[int, str], SourceGroupOrdinals]


def _is_selector_state_coherent(
    service_result: BillingSearchServiceResult,
    selector_states: tuple[str, ...],
) -> bool:
    if any(
        state == BILLING_SELECTOR_PROJECTION_UNAVAILABLE for state in selector_states
    ):
        return service_result.state == BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE
    if all(state == BILLING_SELECTOR_NO_MATCH for state in selector_states):
        return service_result.state == BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY
    return service_result.state not in {
        BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
        BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    }


def _validated_selector_bindings(
    selection: PlanReleaseServingSelection,
    service_result: BillingSearchServiceResult,
) -> tuple[BillingSearchSelectorBindingScope, ...]:
    selector_resolution = service_result.selector_resolution
    if selector_resolution is None:
        raise serving_unavailable()
    try:
        selector_resolution.__post_init__()
        selector_resolution.selector_scope.__post_init__()
    except Exception:
        raise serving_unavailable() from None
    selector_bindings = selector_resolution.selector_scope.bindings
    expected_coordinates = tuple(
        (binding.binding_ordinal, binding.snapshot_id)
        for binding in selection.in_network_bindings
    )
    selector_coordinates = tuple(
        (binding.binding_ordinal, binding.snapshot_id) for binding in selector_bindings
    )
    if expected_coordinates != selector_coordinates:
        raise serving_unavailable()
    return selector_bindings


def _binding_source_groups(
    request: BillingSearchRequest,
    selection: PlanReleaseServingSelection,
    release_binding: PlanReleaseSnapshotBinding,
    selector_binding: BillingSearchSelectorBindingScope,
) -> SourceGroupOrdinals | None:
    try:
        selector_binding.__post_init__()
    except Exception:
        raise serving_unavailable() from None
    if selector_binding.state != BILLING_SELECTOR_MATCHED:
        return None
    if not hmac.compare_digest(
        selector_binding.billing_entity_ref,
        request.billing_entity_ref,
    ):
        raise serving_unavailable()
    serving_tables = selection.serving_tables_for_snapshot(release_binding.snapshot_id)
    if serving_tables is None or not is_release_binding_serving_scope_exact(
        serving_tables,
        release_binding,
    ):
        raise serving_unavailable()
    try:
        return source_groups(
            selector_binding.source_scope,
            snapshot_key=ptg2_serving._required_shared_snapshot_key(serving_tables),
            source_count=ptg2_serving._required_source_count(serving_tables),
            source_publication=(
                serving_tables.provider_tax_identity_source_publication
            ),
        )
    except Exception:
        raise serving_unavailable() from None


def _validated_selector_source_groups(
    request: BillingSearchRequest,
    service_result: BillingSearchServiceResult,
) -> SourceGroupsByBinding:
    """Reprove selector coordinates and exact source triples for one result."""

    selection = service_result.selection
    if selection is None:
        if service_result.state != BILLING_SEARCH_RESULT_NO_SNAPSHOT:
            raise serving_unavailable()
        return {}
    if (
        type(selection) is not PlanReleaseServingSelection
        or not selection.includes_billing_tax_identity_source
    ):
        raise serving_unavailable()
    selector_bindings = _validated_selector_bindings(selection, service_result)
    groups_by_binding: SourceGroupsByBinding = {}
    for release_binding, selector_binding in zip(
        selection.in_network_bindings,
        selector_bindings,
        strict=True,
    ):
        group_ordinals = _binding_source_groups(
            request,
            selection,
            release_binding,
            selector_binding,
        )
        if group_ordinals is not None:
            coordinate = (
                release_binding.binding_ordinal,
                release_binding.snapshot_id,
            )
            groups_by_binding[coordinate] = group_ordinals
    selector_states = tuple(binding.state for binding in selector_bindings)
    if not selector_states or not _is_selector_state_coherent(
        service_result,
        selector_states,
    ):
        raise serving_unavailable()
    return groups_by_binding


def _provider_source_group_ordinals(
    selection: PlanReleaseServingSelection,
    provider: BillingSearchMatchedProvider,
    groups_by_binding: SourceGroupsByBinding,
) -> SourceGroupOrdinals:
    provider_candidate = provider.candidate
    coordinate = (
        provider_candidate.binding_ordinal,
        provider_candidate.snapshot_id,
    )
    matching_bindings = tuple(
        binding
        for binding in selection.in_network_bindings
        if (binding.binding_ordinal, binding.snapshot_id) == coordinate
    )
    if len(matching_bindings) != 1 or coordinate not in groups_by_binding:
        raise serving_unavailable()
    matching_binding = matching_bindings[0]
    serving_tables = selection.serving_tables_for_snapshot(matching_binding.snapshot_id)
    if (
        serving_tables is None
        or serving_tables != provider_candidate.serving_tables
        or not is_release_binding_serving_scope_exact(
            serving_tables,
            matching_binding,
        )
    ):
        raise serving_unavailable()
    return groups_by_binding[coordinate]


def _public_provider(
    request: BillingSearchRequest,
    selection: PlanReleaseServingSelection,
    provider: BillingSearchMatchedProvider,
    groups_by_binding: SourceGroupsByBinding,
    budget: _PublicResponseBudget,
) -> dict[str, Any]:
    source_group_ordinals = _provider_source_group_ordinals(
        selection,
        provider,
        groups_by_binding,
    )
    return public_provider_payload(
        request,
        provider,
        source_group_ordinals,
        budget,
    )


def _validated_response_inputs(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    cursor_keyring: BillingSearchCursorKeyring | None,
    *,
    trusted_now: object,
) -> tuple[
    BillingSearchEndpointAccess,
    BillingSearchServiceResult,
    SourceGroupsByBinding,
    str | None,
]:
    validated_access, endpoint_access_state_sha256 = (
        validate_billing_search_endpoint_access_state(
            endpoint_access,
            trusted_now=trusted_now,
        )
    )
    validated_result = validate_service_result(service_result)
    if not hmac.compare_digest(
        validated_result.endpoint_access_state_sha256,
        endpoint_access_state_sha256,
    ):
        raise serving_unavailable()
    if validated_result.selection is not None and not hmac.compare_digest(
        validated_result.selection.plan_release_id,
        validated_access.request.plan_release_id,
    ):
        raise serving_unavailable()
    groups_by_binding = _validated_selector_source_groups(
        validated_access.request,
        validated_result,
    )
    public_next_cursor = _validate_public_page(
        validated_access,
        validated_result,
        cursor_keyring=cursor_keyring,
        trusted_now=trusted_now,
    )
    return (
        validated_access,
        validated_result,
        groups_by_binding,
        public_next_cursor,
    )


def _public_response_payload(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    groups_by_binding: SourceGroupsByBinding,
    public_next_cursor: str | None,
) -> dict[str, Any]:
    request = endpoint_access.request
    selection = service_result.selection
    if service_result.providers and type(selection) is not PlanReleaseServingSelection:
        raise serving_unavailable()
    budget = _PublicResponseBudget()
    return {
        "result_state": service_result.state,
        "pricing_scope": BILLING_SEARCH_PRICING_SCOPE,
        "billing_association_scope": BILLING_SEARCH_ASSOCIATION_SCOPE,
        "geo_match_scope": BILLING_SEARCH_GEO_SCOPE,
        "plan_release_id": request.plan_release_id,
        "billing_entity_ref": request.billing_entity_ref,
        "procedure": {
            "code_system": request.code_system,
            "code": request.code,
            "modifiers": list(request.modifiers),
            "place_of_service": list(request.place_of_service),
        },
        "items": [
            _public_provider(
                request,
                selection,
                provider,
                groups_by_binding,
                budget,
            )
            for provider in service_result.providers
        ],
        "pagination": {
            "limit": request.limit,
            "has_more": service_result.has_more,
            "next_cursor": public_next_cursor,
        },
    }


def shape_billing_search_response(
    endpoint_access: BillingSearchEndpointAccess,
    service_result: BillingSearchServiceResult,
    *,
    cursor_keyring: BillingSearchCursorKeyring | None = None,
    trusted_now: object,
) -> dict[str, Any]:
    """Return an allowlisted response after exact source and cursor proof."""

    try:
        validated_inputs = _validated_response_inputs(
            endpoint_access,
            service_result,
            cursor_keyring,
            trusted_now=trusted_now,
        )
        response_by_field = _public_response_payload(*validated_inputs)
        _validate_total_text_budget(response_by_field)
        return _fragment_exact_numbers(response_by_field)
    except BillingSearchServingUnavailableError:
        raise
    except Exception:
        raise serving_unavailable() from None


__all__ = [
    "BILLING_SEARCH_ASSOCIATION_SCOPE",
    "BILLING_SEARCH_GEO_SCOPE",
    "BILLING_SEARCH_PRICING_SCOPE",
    "shape_billing_search_response",
]
