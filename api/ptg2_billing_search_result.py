# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable provider-page results for exact billing-identity search."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_code_reader import BillingCodeWitness
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    decode_billing_entity_ref,
)
from api.ptg2_billing_geo_contract import (
    BillingProviderAddress,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
)
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_STATES,
    BillingSearchBindingPin,
    BillingSearchResolvedQuery,
    BillingSearchSelectorScope,
    serving_unavailable,
)


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchProviderCandidate:
    """All exact rate witnesses for one provider address in one binding."""

    binding_pin: BillingSearchBindingPin
    billing_entity_ref: str
    address: BillingProviderAddress
    geo_witnesses: tuple[BillingProviderGeoWitness, ...]
    code_witnesses_by_key: tuple[tuple[int, BillingCodeWitness], ...]

    def __post_init__(self) -> None:
        try:
            decode_billing_entity_ref(self.billing_entity_ref)
        except PTG2BillingAssociationDataError:
            raise serving_unavailable() from None
        if (
            type(self.binding_pin) is not BillingSearchBindingPin
            or self.binding_pin.source_publication is None
            or type(self.address) is not BillingProviderAddress
            or type(self.geo_witnesses) is not tuple
            or not self.geo_witnesses
            or type(self.code_witnesses_by_key) is not tuple
        ):
            raise serving_unavailable()
        snapshot_key = self.binding_pin.serving_tables.shared_snapshot_key
        if any(
            type(witness) is not BillingProviderGeoWitness
            or witness.address != self.address
            or witness.provider_rate.npi != self.address.npi
            or witness.provider_rate.snapshot_key != snapshot_key
            for witness in self.geo_witnesses
        ):
            raise serving_unavailable()
        geo_sort_keys = tuple(witness.stable_sort_key for witness in self.geo_witnesses)
        code_keys = tuple(key for key, _witness in self.code_witnesses_by_key)
        referenced_code_keys = {
            witness.provider_rate.code_key for witness in self.geo_witnesses
        }
        if (
            geo_sort_keys != tuple(sorted(geo_sort_keys))
            or len(geo_sort_keys) != len(set(geo_sort_keys))
            or code_keys != tuple(sorted(set(code_keys)))
            or any(
                type(key) is not int
                or type(witness) is not BillingCodeWitness
                or witness.code_key != key
                for key, witness in self.code_witnesses_by_key
            )
            or set(code_keys) != referenced_code_keys
        ):
            raise serving_unavailable()

    @property
    def sort_key(self) -> tuple[int | float | str, ...]:
        """Return the frozen provider-address page coordinate."""

        distance = self.address.distance_miles
        return (
            1 if distance is None else 0,
            0.0 if distance is None or distance == 0.0 else float(distance),
            self.binding_pin.binding.binding_ordinal,
            self.binding_pin.binding.snapshot_id,
            self.address.npi,
            self.address.address_key,
            self.address.location_key,
        )

    @property
    def price_keys(self) -> tuple[int, ...]:
        """Return the bounded distinct price dictionary coordinates."""

        return tuple(
            sorted({witness.provider_rate.price_key for witness in self.geo_witnesses})
        )

    def __repr__(self) -> str:
        return (
            "<billing-search-provider-candidate "
            f"npi={self.address.npi} witness_count={len(self.geo_witnesses)} "
            "scope=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchMatchedProvider:
    """One page candidate retaining every filtered exact price witness."""

    candidate: BillingSearchProviderCandidate
    price_witnesses: tuple[BillingProviderGeoPriceWitness, ...]

    def __post_init__(self) -> None:
        if (
            type(self.candidate) is not BillingSearchProviderCandidate
            or type(self.price_witnesses) is not tuple
            or not self.price_witnesses
        ):
            raise serving_unavailable()
        candidate_position_by_witness_id = {
            id(witness): position
            for position, witness in enumerate(self.candidate.geo_witnesses)
        }
        retained_positions = tuple(
            candidate_position_by_witness_id.get(id(witness.geo_witness))
            for witness in self.price_witnesses
        )
        if (
            any(
                type(witness) is not BillingProviderGeoPriceWitness
                or not witness.prices
                for witness in self.price_witnesses
            )
            or any(position is None for position in retained_positions)
            or retained_positions != tuple(sorted(set(retained_positions)))
        ):
            raise serving_unavailable()

    @property
    def price_atom_count(self) -> int:
        """Return all retained price atoms without merging occurrences."""

        return sum(len(witness.prices) for witness in self.price_witnesses)

    def __repr__(self) -> str:
        return (
            "<billing-search-matched-provider "
            f"npi={self.candidate.address.npi} "
            f"price_witness_count={len(self.price_witnesses)}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchProviderPage:
    """One bounded provider page before its position is sealed."""

    providers: tuple[BillingSearchMatchedProvider, ...]
    has_more: bool
    next_sort_key: tuple[int | float | str, ...] | None

    def __post_init__(self) -> None:
        if (
            type(self.providers) is not tuple
            or any(
                type(provider) is not BillingSearchMatchedProvider
                for provider in self.providers
            )
            or type(self.has_more) is not bool
        ):
            raise serving_unavailable()
        provider_keys = tuple(
            provider.candidate.sort_key for provider in self.providers
        )
        expected_next_key = (
            provider_keys[-1] if self.has_more and provider_keys else None
        )
        if (
            provider_keys != tuple(sorted(set(provider_keys)))
            or (self.has_more and not provider_keys)
            or self.next_sort_key != expected_next_key
        ):
            raise serving_unavailable()

    def __repr__(self) -> str:
        return (
            "<billing-search-provider-page "
            f"provider_count={len(self.providers)} has_more={self.has_more}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchServiceResult:
    """One explicit result bound to the request, release, and source pins."""

    state: str
    request: BillingSearchResolvedQuery
    selection: PlanReleaseServingSelection
    selector_scope: BillingSearchSelectorScope
    binding_pins: tuple[BillingSearchBindingPin, ...]
    providers: tuple[BillingSearchMatchedProvider, ...]
    has_more: bool
    next_sort_key: tuple[int | float | str, ...] | None

    def __post_init__(self) -> None:
        is_match = self.state == BILLING_SEARCH_RESULT_MATCHED
        is_no_snapshot = self.state == BILLING_SEARCH_RESULT_NO_SNAPSHOT
        if (
            self.state not in BILLING_SEARCH_RESULT_STATES
            or type(self.request) is not BillingSearchResolvedQuery
            or type(self.selection) is not PlanReleaseServingSelection
            or self.selection.plan_release_id != self.request.plan_release_id
            or type(self.selector_scope) is not BillingSearchSelectorScope
            or self.selector_scope.selector_kind != self.request.selector_kind
            or type(self.binding_pins) is not tuple
            or any(
                type(pin) is not BillingSearchBindingPin for pin in self.binding_pins
            )
            or type(self.providers) is not tuple
            or any(
                type(provider) is not BillingSearchMatchedProvider
                for provider in self.providers
            )
            or type(self.has_more) is not bool
            or self.has_more != (self.next_sort_key is not None)
            or (not is_match and (self.providers or self.has_more))
            or (is_match and not self.providers)
            or is_no_snapshot != (not self.selection.in_network_bindings)
        ):
            raise serving_unavailable()
        expected_binding_coordinates = tuple(
            (binding.binding_ordinal, binding.snapshot_id)
            for binding in self.selection.in_network_bindings
        )
        pin_coordinates = tuple(
            (pin.binding.binding_ordinal, pin.binding.snapshot_id)
            for pin in self.binding_pins
        )
        selector_coordinates = tuple(
            (binding.binding_ordinal, binding.snapshot_id)
            for binding in self.selector_scope.bindings
        )
        provider_keys = tuple(
            provider.candidate.sort_key for provider in self.providers
        )
        if (
            pin_coordinates != expected_binding_coordinates
            or selector_coordinates != expected_binding_coordinates
            or provider_keys != tuple(sorted(set(provider_keys)))
            or (
                self.has_more
                and self.next_sort_key != self.providers[-1].candidate.sort_key
            )
            or any(
                provider.candidate.binding_pin not in self.binding_pins
                for provider in self.providers
            )
        ):
            raise serving_unavailable()

    def __repr__(self) -> str:
        return (
            "<billing-search-service-result "
            f"state={self.state} provider_count={len(self.providers)}>"
        )


def validate_service_result(result: Any) -> BillingSearchServiceResult:
    """Recheck an internal service result before public response shaping."""

    if type(result) is not BillingSearchServiceResult:
        raise serving_unavailable()
    result.__post_init__()
    return result


__all__ = [
    "BillingSearchMatchedProvider",
    "BillingSearchProviderCandidate",
    "BillingSearchProviderPage",
    "BillingSearchServiceResult",
    "validate_service_result",
]
