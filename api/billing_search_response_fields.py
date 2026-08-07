# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed field-level shaping for public billing-search results."""

from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Any

from api import ptg2_billing_address_lineage, ptg2_serving
from api.billing_search_request import BillingSearchRequest
from api.billing_search_response_validation import (
    _MAX_PUBLIC_PRICE_ATOMS,
    _PublicResponseBudget,
    _public_text,
    _public_text_array,
    _public_timestamp,
    _validate_public_rate_value,
)
from api.ptg2_address_policy import (
    PTG_ADDRESS_KIND_PHYSICAL,
    classify_ptg_address_kind,
)
from api.ptg2_billing_geo_contract import (
    BILLING_ADDRESS_SELECTION_CONTRACT,
    GEO_EVIDENCE_SOURCE_ID_BY_LEVEL,
    MAX_ADDRESS_PROVENANCE_ENTRIES,
    PHYSICAL_ADDRESS_PURPOSES,
    BillingAddressProvenance,
    BillingProviderGeoPriceWitness,
    require_valid_provider_address,
    require_valid_provider_rate_witness,
    validated_provider_npi,
)
from api.ptg2_billing_search_contract import (
    BillingSearchMatchedProvider,
    serving_unavailable,
)
from api.ptg2_response import _canonical_price_row

_PUBLIC_NPI_MIN = 1_000_000_000
_PUBLIC_NPI_MAX = 2_999_999_999
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
)

SourceGroupOrdinals = dict[int, dict[str, int]]


def _public_price(
    raw_price: Mapping[str, Any],
    budget: _PublicResponseBudget,
    *,
    required_modifiers: frozenset[str],
    required_place_of_service: frozenset[str],
) -> dict[str, Any]:
    """Return one closed price atom that still satisfies exact filters."""

    if type(raw_price) is not dict or frozenset(raw_price) != frozenset(
        _PUBLIC_PRICE_FIELDS
    ):
        raise serving_unavailable()
    _validate_public_rate_value(raw_price.get("negotiated_rate"))
    validated_price_by_field = dict(raw_price)
    for field_name in _PRICE_TEXT_FIELDS:
        validated_price_by_field[field_name] = _public_text(
            raw_price.get(field_name),
            budget,
        )
    for field_name in _PRICE_ARRAY_FIELDS:
        validated_price_by_field[field_name] = _public_text_array(
            raw_price.get(field_name),
            budget,
        )
    # TiC additional_information is unconstrained source prose. It can repeat
    # identifiers supplied elsewhere, so this boundary never forwards it.
    validated_price_by_field["additional_information"] = None
    normalized_price = _canonical_price_row(validated_price_by_field)
    if type(normalized_price["negotiated_rate"]) is str:
        raise serving_unavailable()
    normalized_modifier_values = tuple(normalized_price["billing_code_modifier"])
    normalized_service_code_values = tuple(normalized_price["service_code"])
    if (
        normalized_modifier_values != tuple(sorted(set(normalized_modifier_values)))
        or normalized_service_code_values
        != tuple(sorted(set(normalized_service_code_values)))
        or (
            required_modifiers
            and frozenset(normalized_modifier_values) != required_modifiers
        )
        or (
            required_place_of_service
            and required_place_of_service.isdisjoint(normalized_service_code_values)
        )
    ):
        raise serving_unavailable()
    return {
        field_name: normalized_price[field_name] for field_name in _PUBLIC_PRICE_FIELDS
    }


def _public_address(
    provider: BillingSearchMatchedProvider,
    budget: _PublicResponseBudget,
) -> dict[str, Any]:
    selected_address = provider.candidate.address
    display_by_field = selected_address.display
    if (
        not isinstance(display_by_field, Mapping)
        or selected_address.address_purpose not in PHYSICAL_ADDRESS_PURPOSES
        or classify_ptg_address_kind(display_by_field) != PTG_ADDRESS_KIND_PHYSICAL
        or selected_address.location_hash
        != f"entity_address_unified:{selected_address.location_key}"
    ):
        raise serving_unavailable()
    public_address_by_field: dict[str, Any] = {"address_kind": "physical"}
    for field_name in _PUBLIC_ADDRESS_FIELDS:
        public_address_by_field[field_name] = _public_text(
            display_by_field.get(field_name),
            budget,
        )
    return public_address_by_field


def _canonical_address_provenance(
    provenance: BillingAddressProvenance,
) -> BillingAddressProvenance:
    if type(provenance) is not BillingAddressProvenance:
        raise serving_unavailable()
    try:
        canonical_provenance = ptg2_billing_address_lineage._provenance_entry(
            {
                "dataset_id": provenance.dataset_id,
                "source_id": provenance.source_id,
                "source_record_id": provenance.source_record_id,
                "record_version_id": provenance.record_version_id,
                "record_version_ids": list(provenance.record_version_ids),
                "retrieved_at": provenance.retrieved_at,
                "issuer_names": list(provenance.issuer_names),
                "source_urls": list(provenance.source_urls),
            }
        )
    except Exception:
        raise serving_unavailable() from None
    if canonical_provenance != provenance:
        raise serving_unavailable()
    return canonical_provenance


def _validated_address_provenance(
    provider: BillingSearchMatchedProvider,
) -> tuple[BillingAddressProvenance, ...]:
    """Revalidate closed address-source lineage before public emission."""

    selected_address = provider.candidate.address
    provenance_entries = selected_address.provenance
    expected_source_id = (
        GEO_EVIDENCE_SOURCE_ID_BY_LEVEL.get(selected_address.geo_evidence_level)
        if type(selected_address.geo_evidence_level) is str
        else None
    )
    if (
        expected_source_id is None
        or type(selected_address.geo_evidence_source_id) is not int
        or selected_address.geo_evidence_source_id != expected_source_id
        or selected_address.selection_contract != BILLING_ADDRESS_SELECTION_CONTRACT
        or type(provenance_entries) is not tuple
        or not 1 <= len(provenance_entries) <= MAX_ADDRESS_PROVENANCE_ENTRIES
    ):
        raise serving_unavailable()
    canonical_provenance_entries = tuple(
        _canonical_address_provenance(provenance) for provenance in provenance_entries
    )
    expected_provenance_entries = tuple(
        sorted(
            canonical_provenance_entries,
            key=lambda entry: (
                entry.source_id,
                entry.source_record_id,
                entry.record_version_id,
            ),
        )
    )
    if (
        canonical_provenance_entries != expected_provenance_entries
        or len(set(canonical_provenance_entries)) != len(canonical_provenance_entries)
        or sum(
            provenance.source_id == expected_source_id
            for provenance in canonical_provenance_entries
        )
        != 1
    ):
        raise serving_unavailable()
    return canonical_provenance_entries


def _public_address_evidence(
    provider: BillingSearchMatchedProvider,
    budget: _PublicResponseBudget,
) -> dict[str, Any]:
    selected_address = provider.candidate.address
    provenance_entries = _validated_address_provenance(provider)
    return {
        "evidence_level": _public_text(
            selected_address.geo_evidence_level,
            budget,
            optional=False,
        ),
        "selection_contract": _public_text(
            selected_address.selection_contract,
            budget,
            optional=False,
        ),
        "sources": [
            {
                "dataset": _public_text(
                    provenance.dataset_id,
                    budget,
                    optional=False,
                ),
                "retrieved_at": _public_timestamp(
                    provenance.retrieved_at,
                    budget,
                ),
            }
            for provenance in provenance_entries
        ],
    }


def _public_distance(
    request: BillingSearchRequest,
    provider: BillingSearchMatchedProvider,
) -> float | None:
    distance_value = provider.candidate.address.distance_miles
    if distance_value is None:
        if request.radius_miles is not None:
            raise serving_unavailable()
        return None
    if type(distance_value) not in {float, int}:
        raise serving_unavailable()
    distance_miles = float(distance_value)
    if (
        not math.isfinite(distance_miles)
        or distance_miles < 0
        or (request.radius_miles is not None and distance_miles > request.radius_miles)
    ):
        raise serving_unavailable()
    return distance_miles


def _public_price_atoms(
    price_witness: BillingProviderGeoPriceWitness,
    budget: _PublicResponseBudget,
    *,
    required_modifiers: frozenset[str],
    required_place_of_service: frozenset[str],
) -> list[dict[str, Any]]:
    price_atoms = price_witness.prices
    if (
        type(price_atoms) is not tuple
        or not 1 <= len(price_atoms) <= _MAX_PUBLIC_PRICE_ATOMS
        or any(type(price_atom) is not dict for price_atom in price_atoms)
    ):
        raise serving_unavailable()
    public_price_atoms = []
    for price_atom in price_atoms:
        budget.retain_price_atom()
        public_price_atoms.append(
            _public_price(
                price_atom,
                budget,
                required_modifiers=required_modifiers,
                required_place_of_service=required_place_of_service,
            )
        )
    return public_price_atoms


def _require_source_witness(
    price_witness: BillingProviderGeoPriceWitness,
    source_group_ordinals: SourceGroupOrdinals,
    *,
    snapshot_key: int,
) -> None:
    provider_rate = price_witness.geo_witness.provider_rate
    try:
        require_valid_provider_rate_witness(provider_rate)
    except Exception:
        raise serving_unavailable() from None
    group_ordinals = source_group_ordinals.get(provider_rate.source_key)
    if (
        provider_rate.snapshot_key != snapshot_key
        or group_ordinals is None
        or group_ordinals.get(provider_rate.provider_group_ref)
        != provider_rate.source_record_ordinal
    ):
        raise serving_unavailable()


def _public_rate_occurrences(
    request: BillingSearchRequest,
    provider: BillingSearchMatchedProvider,
    source_group_ordinals: SourceGroupOrdinals,
    budget: _PublicResponseBudget,
) -> list[dict[str, Any]]:
    code_witness_by_key = dict(provider.candidate.code_witnesses_by_key)
    required_modifiers = frozenset(request.modifiers)
    required_place_of_service = frozenset(request.place_of_service)
    price_witnesses = provider.price_witnesses
    if not 1 <= len(price_witnesses) <= _MAX_PUBLIC_PRICE_ATOMS:
        raise serving_unavailable()
    snapshot_key = ptg2_serving._required_shared_snapshot_key(
        provider.candidate.serving_tables
    )
    public_occurrences: list[dict[str, Any]] = []
    for occurrence_ordinal, price_witness in enumerate(price_witnesses, start=1):
        if type(price_witness) is not BillingProviderGeoPriceWitness:
            raise serving_unavailable()
        _require_source_witness(
            price_witness,
            source_group_ordinals,
            snapshot_key=snapshot_key,
        )
        provider_rate = price_witness.geo_witness.provider_rate
        code_witness = code_witness_by_key.get(provider_rate.code_key)
        if (
            code_witness is None
            or code_witness.code_system != request.code_system
            or code_witness.code != request.code
            or provider_rate.npi != provider.candidate.address.npi
        ):
            raise serving_unavailable()
        public_occurrences.append(
            {
                "occurrence_ordinal": occurrence_ordinal,
                "billing_entity_ref": request.billing_entity_ref,
                "procedure": {
                    "code_system": code_witness.code_system,
                    "code": code_witness.code,
                    "negotiation_arrangement": _public_text(
                        code_witness.negotiation_arrangement,
                        budget,
                    ),
                    "billing_code_type_version": _public_text(
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
    return public_occurrences


def public_provider_payload(
    request: BillingSearchRequest,
    provider: BillingSearchMatchedProvider,
    source_group_ordinals: SourceGroupOrdinals,
    budget: _PublicResponseBudget,
) -> dict[str, Any]:
    """Return one fully revalidated, closed provider response object."""

    if type(provider) is not BillingSearchMatchedProvider:
        raise serving_unavailable()
    try:
        provider.__post_init__()
        provider.candidate.__post_init__()
        require_valid_provider_address(provider.candidate.address)
        _validated_address_provenance(provider)
        public_npi = validated_provider_npi(provider.candidate.address.npi)
    except Exception:
        raise serving_unavailable() from None
    if (
        public_npi is None
        or not _PUBLIC_NPI_MIN <= public_npi <= _PUBLIC_NPI_MAX
        or (request.provider_npi is not None and public_npi != request.provider_npi)
    ):
        raise serving_unavailable()
    public_provider_by_field = {
        "npi": public_npi,
        "billing_entity_ref": request.billing_entity_ref,
        "address": _public_address(provider, budget),
        "distance_miles": _public_distance(request, provider),
        "rate_occurrences": _public_rate_occurrences(
            request,
            provider,
            source_group_ordinals,
            budget,
        ),
    }
    if request.include_evidence:
        public_provider_by_field["address_evidence"] = _public_address_evidence(
            provider, budget
        )
    return public_provider_by_field


__all__ = ["SourceGroupOrdinals", "public_provider_payload"]
