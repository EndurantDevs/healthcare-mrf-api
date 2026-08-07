# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed public response shaping for exact billing-identity searches."""

from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Any

from api import ptg2_serving
from api.billing_search_response_values import (
    MAX_PUBLIC_PRICE_ATOMS,
    PUBLIC_RELEASE_METADATA_FIELDS,
    PublicResponseBudget,
    public_text,
    public_text_array,
    public_timestamp,
    validate_public_rate_value,
    validate_total_text_budget,
    validated_response_page,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_geo_contract import (
    BILLING_ADDRESS_SELECTION_CONTRACT,
    GEO_EVIDENCE_SOURCE_ID_BY_LEVEL,
    MAX_ADDRESS_PROVENANCE_ENTRIES,
    BillingAddressProvenance,
    BillingProviderGeoPriceWitness,
    validated_provider_npi,
)
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SELECTOR_MATCHED,
    BillingSearchResolvedQuery,
    serving_unavailable,
)
from api.ptg2_billing_search_result import (
    BillingSearchMatchedProvider,
    BillingSearchServiceResult,
)
from api.ptg2_response import _canonical_price_row, _fragment_exact_numbers

BILLING_SEARCH_PRICING_SCOPE = "plan_scoped_ptg_tax_identity"
BILLING_SEARCH_ASSOCIATION_SCOPE = "tax_identity_match_only"
BILLING_SEARCH_GEO_SCOPE = "provider_address_evidence"
BILLING_SEARCH_EXACT_WITNESS_SCOPE = "exact_tax_identity_group_rate_npi"

_PUBLIC_ADDRESS_FIELDS = (
    "first_line",
    "second_line",
    "city",
    "state",
    "postal_code",
    "country_code",
)
_PUBLIC_PRICE_FIELDS = (
    "negotiated_rate",
    "negotiated_type",
    "expiration_date",
    "service_code",
    "billing_class",
    "setting",
    "billing_code_modifier",
    "additional_information",
)
_PRICE_ARRAY_FIELDS = ("service_code", "billing_code_modifier")
_PRICE_TEXT_FIELDS = (
    "negotiated_type",
    "expiration_date",
    "billing_class",
    "setting",
    "additional_information",
)


def _public_price(
    raw_price: Mapping[str, Any],
    budget: PublicResponseBudget,
    *,
    required_modifiers: frozenset[str],
    required_place_of_service: frozenset[str],
) -> dict[str, Any]:
    """Return one closed price atom that still satisfies exact filters."""

    if (
        type(raw_price) is not dict
        or "negotiated_rate" not in raw_price
        or not frozenset(raw_price).issubset(_PUBLIC_PRICE_FIELDS)
    ):
        raise serving_unavailable()
    validate_public_rate_value(raw_price.get("negotiated_rate"))
    validated_price_by_field: dict[str, Any] = {
        "negotiated_rate": raw_price["negotiated_rate"]
    }
    for field_name in _PRICE_TEXT_FIELDS:
        validated_price_by_field[field_name] = public_text(
            raw_price.get(field_name),
            budget,
        )
    for field_name in _PRICE_ARRAY_FIELDS:
        validated_price_by_field[field_name] = public_text_array(
            raw_price.get(field_name, ()),
            budget,
        )
    normalized_price = _canonical_price_row(validated_price_by_field)
    if type(normalized_price["negotiated_rate"]) is str:
        raise serving_unavailable()
    normalized_modifiers = frozenset(normalized_price["billing_code_modifier"])
    normalized_place_of_service = frozenset(normalized_price["service_code"])
    if (required_modifiers and normalized_modifiers != required_modifiers) or (
        required_place_of_service
        and required_place_of_service.isdisjoint(normalized_place_of_service)
    ):
        raise serving_unavailable()
    return {
        field_name: normalized_price[field_name] for field_name in _PUBLIC_PRICE_FIELDS
    }


def _public_address(
    provider: BillingSearchMatchedProvider,
    budget: PublicResponseBudget,
) -> dict[str, Any]:
    display_by_field = provider.candidate.address.display
    if not isinstance(display_by_field, Mapping):
        raise serving_unavailable()
    public_address_by_field: dict[str, Any] = {"address_kind": "physical"}
    for field_name in _PUBLIC_ADDRESS_FIELDS:
        public_address_by_field[field_name] = public_text(
            display_by_field.get(field_name),
            budget,
        )
    public_address_by_field["purpose"] = public_text(
        provider.candidate.address.address_purpose,
        budget,
    )
    return public_address_by_field


def _validated_address_provenance(
    provider: BillingSearchMatchedProvider,
) -> tuple[BillingAddressProvenance, ...]:
    """Revalidate closed address-source lineage before public emission."""

    address = provider.candidate.address
    provenance_entries = address.provenance
    expected_source_id = (
        GEO_EVIDENCE_SOURCE_ID_BY_LEVEL.get(address.geo_evidence_level)
        if type(address.geo_evidence_level) is str
        else None
    )
    if (
        type(address.geo_evidence_level) is not str
        or expected_source_id is None
        or type(address.geo_evidence_source_id) is not int
        or address.geo_evidence_source_id != expected_source_id
        or address.selection_contract != BILLING_ADDRESS_SELECTION_CONTRACT
        or type(provenance_entries) is not tuple
        or not 1 <= len(provenance_entries) <= MAX_ADDRESS_PROVENANCE_ENTRIES
        or any(
            type(provenance) is not BillingAddressProvenance
            for provenance in provenance_entries
        )
        or sum(
            provenance.source_id == expected_source_id
            for provenance in provenance_entries
        )
        != 1
        or any(
            type(provenance.source_id) is not int
            or provenance.dataset_id
            != ptg2_serving._ADDRESS_DATASET_ID_BY_SOURCE_ID.get(provenance.source_id)
            for provenance in provenance_entries
        )
    ):
        raise serving_unavailable()
    return provenance_entries


def _public_address_evidence(
    provider: BillingSearchMatchedProvider,
    budget: PublicResponseBudget,
) -> dict[str, Any]:
    address = provider.candidate.address
    public_evidence_sources = [
        {
            "dataset": public_text(
                provenance.dataset_id,
                budget,
                optional=False,
            ),
            "retrieved_at": public_timestamp(provenance.retrieved_at, budget),
        }
        for provenance in _validated_address_provenance(provider)
    ]
    return {
        "evidence_level": public_text(
            address.geo_evidence_level,
            budget,
            optional=False,
        ),
        "selection_contract": public_text(
            address.selection_contract,
            budget,
            optional=False,
        ),
        "sources": public_evidence_sources,
    }


def _public_distance(
    query: BillingSearchResolvedQuery,
    provider: BillingSearchMatchedProvider,
) -> float | None:
    distance_value = provider.candidate.address.distance_miles
    if distance_value is None:
        if query.radius_miles is not None:
            raise serving_unavailable()
        return None
    if type(distance_value) not in {float, int}:
        raise serving_unavailable()
    distance_miles = float(distance_value)
    if (
        not math.isfinite(distance_miles)
        or distance_miles < 0
        or (query.radius_miles is not None and distance_miles > query.radius_miles)
    ):
        raise serving_unavailable()
    return 0.0 if distance_miles == 0.0 else distance_miles


def _public_price_atoms(
    price_witness: BillingProviderGeoPriceWitness,
    budget: PublicResponseBudget,
    *,
    required_modifiers: frozenset[str],
    required_place_of_service: frozenset[str],
) -> list[dict[str, Any]]:
    price_atoms = price_witness.prices
    if (
        type(price_atoms) is not tuple
        or not 1 <= len(price_atoms) <= MAX_PUBLIC_PRICE_ATOMS
        or any(type(price_atom) is not dict for price_atom in price_atoms)
    ):
        raise serving_unavailable()
    public_price_atoms = []
    for price in price_atoms:
        budget.retain_price_atom()
        public_price_atoms.append(
            _public_price(
                price,
                budget,
                required_modifiers=required_modifiers,
                required_place_of_service=required_place_of_service,
            )
        )
    return public_price_atoms


def _public_rate_occurrences(
    query: BillingSearchResolvedQuery,
    provider: BillingSearchMatchedProvider,
    budget: PublicResponseBudget,
) -> list[dict[str, Any]]:
    code_witness_by_key = dict(provider.candidate.code_witnesses_by_key)
    required_modifiers = frozenset(query.modifiers)
    required_place_of_service = frozenset(query.place_of_service)
    public_occurrences: list[dict[str, Any]] = []
    billing_entity_ref = public_text(
        provider.candidate.billing_entity_ref,
        budget,
        optional=False,
    )
    for response_ordinal, price_witness in enumerate(
        provider.price_witnesses,
        start=1,
    ):
        if type(price_witness) is not BillingProviderGeoPriceWitness:
            raise serving_unavailable()
        provider_rate = price_witness.geo_witness.provider_rate
        code_witness = code_witness_by_key.get(provider_rate.code_key)
        if (
            code_witness is None
            or code_witness.code_system != query.code_system
            or code_witness.code != query.code
            or provider_rate.npi != provider.candidate.address.npi
        ):
            raise serving_unavailable()
        public_occurrences.append(
            {
                "occurrence_ordinal": response_ordinal,
                "billing_entity_ref": billing_entity_ref,
                "billing_witness_scope": BILLING_SEARCH_EXACT_WITNESS_SCOPE,
                "procedure": {
                    "code_system": code_witness.code_system,
                    "code": code_witness.code,
                    "negotiation_arrangement": public_text(
                        code_witness.negotiation_arrangement,
                        budget,
                    ),
                    "billing_code_type_version": public_text(
                        code_witness.billing_code_type_version,
                        budget,
                    ),
                },
                "prices": _public_price_atoms(
                    price_witness,
                    budget,
                    required_modifiers=required_modifiers,
                    required_place_of_service=required_place_of_service,
                ),
            }
        )
    if not public_occurrences:
        raise serving_unavailable()
    return public_occurrences


def _validate_provider_scope(
    billing_result: BillingSearchServiceResult,
    provider: BillingSearchMatchedProvider,
) -> None:
    candidate = provider.candidate
    binding_pin = candidate.binding_pin
    selection_tables = billing_result.selection.serving_tables_for_snapshot(
        binding_pin.binding.snapshot_id
    )
    if (
        binding_pin not in billing_result.binding_pins
        or selection_tables != binding_pin.serving_tables
        or binding_pin.source_publication is None
        or not is_release_binding_serving_scope_exact(
            binding_pin.serving_tables,
            binding_pin.binding,
        )
    ):
        raise serving_unavailable()


def _public_provider(
    billing_result: BillingSearchServiceResult,
    matched_provider: BillingSearchMatchedProvider,
    budget: PublicResponseBudget,
) -> dict[str, Any]:
    if type(matched_provider) is not BillingSearchMatchedProvider:
        raise serving_unavailable()
    matched_provider.__post_init__()
    matched_provider.candidate.__post_init__()
    _validate_provider_scope(billing_result, matched_provider)
    try:
        public_npi = validated_provider_npi(matched_provider.candidate.address.npi)
    except ValueError:
        raise serving_unavailable() from None
    query = billing_result.request
    if query.provider_npi is not None and public_npi != query.provider_npi:
        raise serving_unavailable()
    public_provider_by_field = {
        "npi": public_npi,
        "billing_entity_ref": public_text(
            matched_provider.candidate.billing_entity_ref,
            budget,
            optional=False,
        ),
        "billing_witness_scope": BILLING_SEARCH_EXACT_WITNESS_SCOPE,
        "address": _public_address(matched_provider, budget),
        "distance_miles": _public_distance(query, matched_provider),
        "billing_entity_site_match": {
            "classification": "not_comparable",
            "confidence": "unknown",
        },
        "rate_occurrences": _public_rate_occurrences(
            query,
            matched_provider,
            budget,
        ),
    }
    if query.include_evidence:
        public_provider_by_field["address_evidence"] = _public_address_evidence(
            matched_provider,
            budget,
        )
    return public_provider_by_field


def _public_release_metadata(
    selection: PlanReleaseServingSelection,
    budget: PublicResponseBudget,
) -> dict[str, Any]:
    metadata = selection.response_metadata()
    if frozenset(metadata) != frozenset(PUBLIC_RELEASE_METADATA_FIELDS):
        raise serving_unavailable()
    public_metadata_by_field = {
        field_name: (
            True
            if field_name == "is_current" and metadata[field_name] is True
            else public_text(metadata[field_name], budget)
        )
        for field_name in PUBLIC_RELEASE_METADATA_FIELDS
    }
    if metadata["is_current"] is not True:
        raise serving_unavailable()
    public_metadata_by_field["snapshot_refs"] = [
        public_text(snapshot_id, budget, optional=False)
        for snapshot_id in dict.fromkeys(
            binding.snapshot_id for binding in selection.in_network_bindings
        )
    ]
    return public_metadata_by_field


def _matched_billing_entity_refs(
    billing_result: BillingSearchServiceResult,
    budget: PublicResponseBudget,
) -> list[str]:
    if billing_result.state not in {
        BILLING_SEARCH_RESULT_MATCHED,
        BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
        BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    }:
        return []
    references = tuple(
        dict.fromkeys(
            binding.billing_entity_ref
            for binding in billing_result.selector_scope.bindings
            if binding.state == BILLING_SELECTOR_MATCHED
        )
    )
    if any(reference is None for reference in references):
        raise serving_unavailable()
    return [
        str(public_text(reference, budget, optional=False)) for reference in references
    ]


def shape_billing_search_response(
    service_result: BillingSearchServiceResult,
    *,
    next_cursor: str | None = None,
) -> dict[str, Any]:
    """Return an allowlisted response with no raw or internal witness keys."""

    billing_result, public_next_cursor = validated_response_page(
        service_result,
        next_cursor,
    )
    budget = PublicResponseBudget()
    query = billing_result.request
    response_by_field = {
        "result_state": billing_result.state,
        "pricing_scope": BILLING_SEARCH_PRICING_SCOPE,
        "billing_association_scope": BILLING_SEARCH_ASSOCIATION_SCOPE,
        "geo_match_scope": BILLING_SEARCH_GEO_SCOPE,
        "resolved_release": _public_release_metadata(
            billing_result.selection,
            budget,
        ),
        "billing_identity": {
            "selector_kind": query.selector_kind,
            "tax_identity_type": query.tax_identity_type,
            "matched_billing_entity_refs": _matched_billing_entity_refs(
                billing_result,
                budget,
            ),
        },
        "procedure": {
            "code_system": query.code_system,
            "code": query.code,
            "modifiers": list(query.modifiers),
            "place_of_service": list(query.place_of_service),
        },
        "items": [
            _public_provider(billing_result, matched_provider, budget)
            for matched_provider in billing_result.providers
        ],
        "pagination": {
            "limit": query.limit,
            "has_more": billing_result.has_more,
            "next_cursor": public_next_cursor,
        },
    }
    validate_total_text_budget(response_by_field)
    return _fragment_exact_numbers(response_by_field)


__all__ = [
    "BILLING_SEARCH_ASSOCIATION_SCOPE",
    "BILLING_SEARCH_EXACT_WITNESS_SCOPE",
    "BILLING_SEARCH_GEO_SCOPE",
    "BILLING_SEARCH_PRICING_SCOPE",
    "shape_billing_search_response",
]
