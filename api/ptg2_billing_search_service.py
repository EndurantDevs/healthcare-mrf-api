# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact billing-identity traversal for one immutable provider page."""

from __future__ import annotations

from dataclasses import dataclass, replace

from api import (
    ptg2_billing_code_reader,
    ptg2_billing_exact_reader,
    ptg2_billing_geo_reader,
    ptg2_billing_price_reader,
    ptg2_billing_search_page,
    ptg2_tables,
)
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_billing_exact_contract import BillingRateOccurrenceWitness
from api.ptg2_billing_geo_contract import MAX_PROVIDER_RATE_WITNESSES
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchBindingPin,
    BillingSearchResolvedQuery,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
    serving_unavailable,
)
from api.ptg2_billing_search_result import (
    BillingSearchProviderCandidate,
    BillingSearchServiceResult,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


@dataclass(frozen=True, slots=True)
class _BillingSearchTraversal:
    candidates: tuple[BillingSearchProviderCandidate, ...]
    has_provider_rates: bool


def _without_source_publication(
    serving_tables: PTG2ServingTables,
) -> PTG2ServingTables:
    return replace(
        serving_tables,
        provider_tax_identity_source_publication=None,
    )


async def _source_pinned_selection(
    session,
    selection: PlanReleaseServingSelection,
) -> PlanReleaseServingSelection:
    if type(selection) is not PlanReleaseServingSelection:
        raise serving_unavailable()
    prior_tables_by_snapshot = selection.network_tables_by_snapshot()
    if prior_tables_by_snapshot is None:
        raise serving_unavailable()
    pinned_tables_by_snapshot: dict[str, PTG2ServingTables] = {}
    for binding in selection.in_network_bindings:
        prior_tables = prior_tables_by_snapshot.get(binding.snapshot_id)
        if prior_tables is None:
            raise serving_unavailable()
        pinned_tables = pinned_tables_by_snapshot.get(binding.snapshot_id)
        if pinned_tables is None:
            pinned_tables = await ptg2_tables.snapshot_serving_tables(
                session,
                binding.snapshot_id,
                include_billing_tax_identity_source=True,
            )
            if _without_source_publication(pinned_tables) != (
                _without_source_publication(prior_tables)
            ):
                raise serving_unavailable()
            pinned_tables_by_snapshot[binding.snapshot_id] = pinned_tables
        BillingSearchBindingPin(binding, pinned_tables)
    return replace(
        selection,
        _validated_serving_tables=tuple(pinned_tables_by_snapshot.items()),
    )


async def pin_billing_search_selection(
    session,
    selection: PlanReleaseServingSelection,
) -> PlanReleaseServingSelection:
    """Re-read and retain each exact source publication before resolution."""

    try:
        return await _source_pinned_selection(session, selection)
    except BillingSearchServingUnavailableError:
        raise
    except (PTG2ManifestArtifactError, PTG2SharedBlockError):
        raise serving_unavailable() from None


def _binding_pins(
    selection: PlanReleaseServingSelection,
) -> tuple[BillingSearchBindingPin, ...]:
    serving_tables_by_snapshot = selection.network_tables_by_snapshot()
    if serving_tables_by_snapshot is None:
        raise serving_unavailable()
    return tuple(
        BillingSearchBindingPin(
            binding,
            serving_tables_by_snapshot[binding.snapshot_id],
        )
        for binding in selection.in_network_bindings
    )


def _validate_selector_binding(
    query: BillingSearchResolvedQuery,
    selector_binding: BillingSearchSelectorBindingScope,
    binding_pin: BillingSearchBindingPin,
) -> None:
    source_publication = binding_pin.source_publication
    if selector_binding.state == BILLING_SELECTOR_PROJECTION_UNAVAILABLE:
        is_npi_projection_gap = (
            query.selector_kind == "tax_identity" and query.tax_identity_type == "npi"
        )
        if source_publication is not None and not is_npi_projection_gap:
            raise serving_unavailable()
        return
    if source_publication is None:
        raise serving_unavailable()
    if selector_binding.state == BILLING_SELECTOR_NO_MATCH:
        return
    if selector_binding.state != BILLING_SELECTOR_MATCHED:
        raise serving_unavailable()
    source_scope = selector_binding.source_scope
    if (
        source_scope is None
        or source_scope.snapshot_key != binding_pin.serving_tables.shared_snapshot_key
        or source_scope.publication != source_publication
    ):
        raise serving_unavailable()


def _validated_scope_bindings(
    query: BillingSearchResolvedQuery,
    selector_scope: BillingSearchSelectorScope,
    binding_pins: tuple[BillingSearchBindingPin, ...],
) -> tuple[BillingSearchSelectorBindingScope, ...]:
    if (
        type(query) is not BillingSearchResolvedQuery
        or type(selector_scope) is not BillingSearchSelectorScope
        or selector_scope.selector_kind != query.selector_kind
    ):
        raise serving_unavailable()
    expected_coordinates = tuple(
        (pin.binding.binding_ordinal, pin.binding.snapshot_id) for pin in binding_pins
    )
    actual_coordinates = tuple(
        (binding.binding_ordinal, binding.snapshot_id)
        for binding in selector_scope.bindings
    )
    if actual_coordinates != expected_coordinates:
        raise serving_unavailable()
    for selector_binding, binding_pin in zip(
        selector_scope.bindings,
        binding_pins,
        strict=True,
    ):
        _validate_selector_binding(query, selector_binding, binding_pin)
    return selector_scope.bindings


async def _load_price_filtered_rate_witnesses(
    session,
    *,
    query: BillingSearchResolvedQuery,
    binding_pin: BillingSearchBindingPin,
    source_scope: ResolvedBillingEntitySourceScope,
    code_keys: tuple[int, ...],
) -> tuple[BillingRateOccurrenceWitness, ...]:
    """Load exact occurrences and apply price filters before NPI expansion."""

    rate_witnesses = (
        await ptg2_billing_exact_reader.load_exact_billing_rate_occurrence_witnesses(
            session,
            binding_pin.serving_tables,
            source_scope=source_scope,
            code_keys=code_keys,
        )
    )
    return await ptg2_billing_price_reader.filter_exact_billing_rate_occurrences(
        session,
        binding_pin.serving_tables,
        rate_witnesses=rate_witnesses,
        price_filter_args=query.price_filter_args,
    )


async def _binding_candidates(
    session,
    *,
    query: BillingSearchResolvedQuery,
    selector_binding: BillingSearchSelectorBindingScope,
    binding_pin: BillingSearchBindingPin,
) -> tuple[tuple[BillingSearchProviderCandidate, ...], bool]:
    source_scope = selector_binding.source_scope
    billing_entity_ref = selector_binding.billing_entity_ref
    if source_scope is None or billing_entity_ref is None:
        raise serving_unavailable()
    code_witnesses = await ptg2_billing_code_reader.load_exact_billing_code_witnesses(
        session,
        binding_pin.serving_tables,
        binding_pin.binding,
        code_system=query.code_system,
        code=query.code,
    )
    if not code_witnesses:
        return (), False
    filtered_rate_witnesses = await _load_price_filtered_rate_witnesses(
        session,
        query=query,
        binding_pin=binding_pin,
        source_scope=source_scope,
        code_keys=tuple(witness.code_key for witness in code_witnesses),
    )
    if not filtered_rate_witnesses:
        return (), False
    provider_rates = (
        await ptg2_billing_geo_reader.expand_billing_rate_witnesses_to_npis(
            session,
            binding_pin.serving_tables,
            rate_witnesses=filtered_rate_witnesses,
            provider_npi=query.provider_npi,
        )
    )
    if not provider_rates:
        return (), False
    geo_selection = await ptg2_billing_geo_reader.load_exact_billing_geo_witnesses(
        session,
        binding_pin.serving_tables,
        provider_rate_witnesses=provider_rates,
        geo_args=query.geo_args,
    )
    if not geo_selection.address_projection_available:
        raise serving_unavailable()
    candidates = ptg2_billing_search_page.group_billing_geo_candidates(
        binding_pin=binding_pin,
        billing_entity_ref=billing_entity_ref,
        code_witnesses=code_witnesses,
        geo_witnesses=geo_selection.witnesses,
    )
    return candidates, True


async def _traverse_matched_bindings(
    session,
    *,
    query: BillingSearchResolvedQuery,
    selector_bindings: tuple[BillingSearchSelectorBindingScope, ...],
    binding_pins: tuple[BillingSearchBindingPin, ...],
) -> _BillingSearchTraversal:
    candidates: list[BillingSearchProviderCandidate] = []
    has_provider_rates = False
    for selector_binding, binding_pin in zip(
        selector_bindings,
        binding_pins,
        strict=True,
    ):
        if selector_binding.state != BILLING_SELECTOR_MATCHED:
            continue
        binding_candidates, binding_has_provider_rates = await _binding_candidates(
            session,
            query=query,
            selector_binding=selector_binding,
            binding_pin=binding_pin,
        )
        has_provider_rates = has_provider_rates or binding_has_provider_rates
        if len(candidates) + len(binding_candidates) > (MAX_PROVIDER_RATE_WITNESSES):
            raise serving_unavailable()
        candidates.extend(binding_candidates)
    sorted_candidates = tuple(
        sorted(candidates, key=lambda candidate: candidate.sort_key)
    )
    if len({candidate.sort_key for candidate in sorted_candidates}) != len(
        sorted_candidates
    ):
        raise serving_unavailable()
    return _BillingSearchTraversal(sorted_candidates, has_provider_rates)


def _empty_result(
    state: str,
    *,
    query: BillingSearchResolvedQuery,
    selection: PlanReleaseServingSelection,
    selector_scope: BillingSearchSelectorScope,
    binding_pins: tuple[BillingSearchBindingPin, ...],
) -> BillingSearchServiceResult:
    if query.after_sort_key is not None:
        raise serving_unavailable()
    return BillingSearchServiceResult(
        state=state,
        request=query,
        selection=selection,
        selector_scope=selector_scope,
        binding_pins=binding_pins,
        providers=(),
        has_more=False,
        next_sort_key=None,
    )


def _empty_traversal_state(traversal: _BillingSearchTraversal) -> str:
    if traversal.has_provider_rates:
        return BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS
    return BILLING_SEARCH_RESULT_NO_MATCHING_RATES


def _selector_terminal_state(
    binding_pins: tuple[BillingSearchBindingPin, ...],
    selector_bindings: tuple[BillingSearchSelectorBindingScope, ...],
) -> str | None:
    if not binding_pins:
        return BILLING_SEARCH_RESULT_NO_SNAPSHOT
    selector_states = {binding.state for binding in selector_bindings}
    if BILLING_SELECTOR_PROJECTION_UNAVAILABLE in selector_states:
        return BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE
    if BILLING_SELECTOR_MATCHED not in selector_states:
        return BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY
    return None


def _matched_service_result(
    provider_page,
    *,
    query: BillingSearchResolvedQuery,
    selection: PlanReleaseServingSelection,
    selector_scope: BillingSearchSelectorScope,
    binding_pins: tuple[BillingSearchBindingPin, ...],
) -> BillingSearchServiceResult:
    return BillingSearchServiceResult(
        state=BILLING_SEARCH_RESULT_MATCHED,
        request=query,
        selection=selection,
        selector_scope=selector_scope,
        binding_pins=binding_pins,
        providers=provider_page.providers,
        has_more=provider_page.has_more,
        next_sort_key=provider_page.next_sort_key,
    )


async def _search_source_pinned_selection(
    session,
    *,
    query: BillingSearchResolvedQuery,
    selection: PlanReleaseServingSelection,
    selector_scope: BillingSearchSelectorScope,
) -> BillingSearchServiceResult:
    if (
        type(selection) is not PlanReleaseServingSelection
        or selection.plan_release_id != query.plan_release_id
    ):
        raise serving_unavailable()
    binding_pins = _binding_pins(selection)
    selector_bindings = _validated_scope_bindings(
        query,
        selector_scope,
        binding_pins,
    )
    common_result_args_by_name = {
        "query": query,
        "selection": selection,
        "selector_scope": selector_scope,
        "binding_pins": binding_pins,
    }
    terminal_state = _selector_terminal_state(binding_pins, selector_bindings)
    if terminal_state is not None:
        return _empty_result(
            terminal_state,
            **common_result_args_by_name,
        )
    traversal = await _traverse_matched_bindings(
        session,
        query=query,
        selector_bindings=selector_bindings,
        binding_pins=binding_pins,
    )
    if not traversal.candidates:
        return _empty_result(
            _empty_traversal_state(traversal),
            **common_result_args_by_name,
        )
    provider_page = await ptg2_billing_search_page.hydrate_billing_search_page(
        session,
        candidates=traversal.candidates,
        after_sort_key=query.after_sort_key,
        limit=query.limit,
        price_filter_args=query.price_filter_args,
    )
    if not provider_page.providers:
        return _empty_result(
            BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
            **common_result_args_by_name,
        )
    return _matched_service_result(
        provider_page,
        **common_result_args_by_name,
    )


async def search_exact_billing_provider_page(
    session,
    *,
    query: BillingSearchResolvedQuery,
    selection: PlanReleaseServingSelection,
    selector_scope: BillingSearchSelectorScope,
) -> BillingSearchServiceResult:
    """Serve exact identity-group-set-rate-NPI-address witnesses only."""

    try:
        return await _search_source_pinned_selection(
            session,
            query=query,
            selection=selection,
            selector_scope=selector_scope,
        )
    except BillingSearchServingUnavailableError:
        raise
    except (PTG2ManifestArtifactError, PTG2SharedBlockError):
        raise serving_unavailable() from None


__all__ = [
    "pin_billing_search_selection",
    "search_exact_billing_provider_page",
]
