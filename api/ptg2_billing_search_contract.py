# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Internal result contracts for exact billing-identity pricing search."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from api.billing_search_cursor import BillingSearchSealedPageCursor
from api.plan_release_serving import PlanReleaseServingSelection
from api.ptg2_billing_code_reader import BillingCodeWitness
from api.ptg2_billing_geo_contract import (
    BillingProviderAddress,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

BILLING_SEARCH_RESULT_MATCHED = "matched"
BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY = "no_matching_tax_identity"
BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE = "tax_identity_unavailable_for_snapshot"
BILLING_SEARCH_RESULT_NO_MATCHING_RATES = "no_matching_rates"
BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS = "no_match_in_radius"
BILLING_SEARCH_RESULT_NO_SNAPSHOT = "no_snapshot_for_plan"
BILLING_SEARCH_RESULT_STATES = frozenset(
    {
        BILLING_SEARCH_RESULT_MATCHED,
        BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
        BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
        BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
        BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
        BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    }
)
_LOWER_HEXADECIMAL = frozenset("0123456789abcdef")


class BillingSearchServingUnavailableError(PTG2ManifestArtifactError):
    """One value-free failure for an unavailable immutable serving bundle."""


class BillingSearchResourceNotFoundError(RuntimeError):
    """One value-free failure for an inaccessible opaque billing resource."""


def resource_not_found() -> BillingSearchResourceNotFoundError:
    """Return the generic API-safe opaque-resource failure."""

    return BillingSearchResourceNotFoundError("billing_search_resource_not_found")


def serving_unavailable() -> BillingSearchServingUnavailableError:
    """Return the generic API-safe serving failure."""

    return BillingSearchServingUnavailableError(
        "billing_search_serving_generation_unavailable"
    )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchProviderCandidate:
    """All exact rate witnesses for one provider address in one binding."""

    binding_ordinal: int
    snapshot_id: str
    serving_tables: PTG2ServingTables
    address: BillingProviderAddress
    geo_witnesses: tuple[BillingProviderGeoWitness, ...]
    code_witnesses_by_key: tuple[tuple[int, BillingCodeWitness], ...]

    def __post_init__(self) -> None:
        if (
            type(self.binding_ordinal) is not int
            or self.binding_ordinal < 0
            or type(self.snapshot_id) is not str
            or not self.snapshot_id
            or type(self.serving_tables) is not PTG2ServingTables
            or self.serving_tables.snapshot_id != self.snapshot_id
            or type(self.address) is not BillingProviderAddress
            or type(self.geo_witnesses) is not tuple
            or not self.geo_witnesses
            or type(self.code_witnesses_by_key) is not tuple
        ):
            raise serving_unavailable()
        if any(
            type(witness) is not BillingProviderGeoWitness
            or witness.address != self.address
            or witness.provider_rate.npi != self.address.npi
            or witness.provider_rate.snapshot_key
            != self.serving_tables.shared_snapshot_key
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
        """Return the stable provider-level cursor coordinate."""

        distance = self.address.distance_miles
        return (
            1 if distance is None else 0,
            0.0 if distance is None or distance == 0.0 else float(distance),
            self.binding_ordinal,
            self.snapshot_id,
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
    """One page candidate retaining all filtered exact price witnesses."""

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
    """One bounded provider page before its cursor coordinate is sealed."""

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
    """One explicit state and optional generation-bound provider page."""

    state: str
    providers: tuple[BillingSearchMatchedProvider, ...]
    next_cursor: BillingSearchSealedPageCursor | None
    has_more: bool
    selection: PlanReleaseServingSelection | None
    endpoint_access_state_sha256: str

    def __post_init__(self) -> None:
        is_match = self.state == BILLING_SEARCH_RESULT_MATCHED
        is_no_snapshot = self.state == BILLING_SEARCH_RESULT_NO_SNAPSHOT
        has_selection = type(self.selection) is PlanReleaseServingSelection
        if (
            self.state not in BILLING_SEARCH_RESULT_STATES
            or type(self.providers) is not tuple
            or any(
                type(provider) is not BillingSearchMatchedProvider
                for provider in self.providers
            )
            or type(self.has_more) is not bool
            or (
                self.next_cursor is not None
                and type(self.next_cursor) is not BillingSearchSealedPageCursor
            )
            or type(self.endpoint_access_state_sha256) is not str
            or len(self.endpoint_access_state_sha256) != 64
            or not set(self.endpoint_access_state_sha256).issubset(_LOWER_HEXADECIMAL)
            or (is_no_snapshot and self.selection is not None)
            or (not is_no_snapshot and not has_selection)
            or (not is_match and (self.providers or self.next_cursor or self.has_more))
            or (is_match and not self.providers)
            or self.has_more != (self.next_cursor is not None)
        ):
            raise serving_unavailable()
        provider_keys = tuple(
            provider.candidate.sort_key for provider in self.providers
        )
        if provider_keys != tuple(sorted(set(provider_keys))):
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
    "BILLING_SEARCH_RESULT_MATCHED",
    "BILLING_SEARCH_RESULT_NO_MATCHING_RATES",
    "BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY",
    "BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS",
    "BILLING_SEARCH_RESULT_NO_SNAPSHOT",
    "BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE",
    "BillingSearchMatchedProvider",
    "BillingSearchProviderPage",
    "BillingSearchProviderCandidate",
    "BillingSearchResourceNotFoundError",
    "BillingSearchServiceResult",
    "BillingSearchServingUnavailableError",
    "resource_not_found",
    "serving_unavailable",
    "validate_service_result",
]
