# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable query and scope contracts for exact billing-identity search."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import (
    PLAN_RELEASE_IN_NETWORK_ROLE,
    PlanReleaseSnapshotBinding,
    normalize_plan_release_id,
)
from api.ptg2_billing_code_reader import _exact_code
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    decode_billing_entity_ref,
)
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_billing_geo_contract import (
    validated_geo_args,
    validated_provider_npi,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

BILLING_SEARCH_DEFAULT_PAGE_SIZE = 25
BILLING_SEARCH_MAX_PAGE_SIZE = 200
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
BILLING_SELECTOR_MATCHED = "matched"
BILLING_SELECTOR_NO_MATCH = "no_match"
BILLING_SELECTOR_PROJECTION_UNAVAILABLE = "projection_unavailable"
BILLING_SELECTOR_STATES = frozenset(
    {
        BILLING_SELECTOR_MATCHED,
        BILLING_SELECTOR_NO_MATCH,
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    }
)
BILLING_SELECTOR_KINDS = frozenset({"tax_identity", "billing_entity_ref"})

_MODIFIER_PATTERN = re.compile(r"[A-Z0-9]{1,8}", flags=re.ASCII)
_PLACE_OF_SERVICE_PATTERN = re.compile(r"[0-9]{2}", flags=re.ASCII)
_MAX_MODIFIERS = 8
_MAX_PLACE_OF_SERVICE = 16


class BillingSearchServingUnavailableError(PTG2ManifestArtifactError):
    """Value-free failure for an unavailable immutable serving bundle."""


def serving_unavailable() -> BillingSearchServingUnavailableError:
    """Return the generic API-safe serving failure."""

    return BillingSearchServingUnavailableError(
        "billing_search_serving_generation_unavailable"
    )


def _canonical_filter_values(
    values: object,
    *,
    pattern: re.Pattern[str],
    maximum_count: int,
) -> tuple[str, ...]:
    if (
        type(values) is not tuple
        or len(values) > maximum_count
        or any(
            type(value) is not str or pattern.fullmatch(value) is None
            for value in values
        )
        or values != tuple(sorted(set(values)))
    ):
        raise serving_unavailable()
    return values


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchResolvedQuery:
    """Validated non-sensitive fields consumed by the exact-reader service."""

    plan_release_id: str
    selector_kind: str
    tax_identity_type: str | None
    code_system: str
    code: str
    zip5: str | None
    latitude: float | None
    longitude: float | None
    radius_miles: float | None
    provider_npi: int | None = None
    modifiers: tuple[str, ...] = ()
    place_of_service: tuple[str, ...] = ()
    include_evidence: bool = False
    limit: int = BILLING_SEARCH_DEFAULT_PAGE_SIZE
    after_sort_key: tuple[int | float | str, ...] | None = None

    def __post_init__(self) -> None:
        try:
            if (
                normalize_plan_release_id(self.plan_release_id) != self.plan_release_id
                or self.selector_kind not in BILLING_SELECTOR_KINDS
                or (
                    self.selector_kind == "tax_identity"
                    and self.tax_identity_type not in {"ein", "npi"}
                )
                or (
                    self.selector_kind == "billing_entity_ref"
                    and self.tax_identity_type is not None
                )
                or _exact_code(self.code_system, self.code)
                != (self.code_system, self.code)
                or type(self.include_evidence) is not bool
                or type(self.limit) is not int
                or not 1 <= self.limit <= BILLING_SEARCH_MAX_PAGE_SIZE
                or (
                    self.after_sort_key is not None
                    and type(self.after_sort_key) is not tuple
                )
            ):
                raise serving_unavailable()
            validated_provider_npi(self.provider_npi, optional=True)
            _canonical_filter_values(
                self.modifiers,
                pattern=_MODIFIER_PATTERN,
                maximum_count=_MAX_MODIFIERS,
            )
            _canonical_filter_values(
                self.place_of_service,
                pattern=_PLACE_OF_SERVICE_PATTERN,
                maximum_count=_MAX_PLACE_OF_SERVICE,
            )
            expected_geo_args = self.geo_args
            if validated_geo_args(expected_geo_args) != {
                "mode": "exact_source",
                "include_evidence": True,
                **expected_geo_args,
            }:
                raise serving_unavailable()
        except (PTG2ManifestArtifactError, ValueError):
            raise serving_unavailable() from None

    @property
    def geo_args(self) -> dict[str, Any]:
        """Return an exact ZIP or centrally resolved radius selector."""

        if self.zip5 is not None:
            if any(
                value is not None
                for value in (self.latitude, self.longitude, self.radius_miles)
            ):
                raise serving_unavailable()
            return {"zip5": self.zip5}
        if any(
            value is None
            for value in (self.latitude, self.longitude, self.radius_miles)
        ):
            raise serving_unavailable()
        return {
            "lat": self.latitude,
            "long": self.longitude,
            "radius_miles": self.radius_miles,
        }

    @property
    def price_filter_args(self) -> dict[str, Any]:
        """Return exact optional modifier and place-of-service filters."""

        return {
            "modifiers": self.modifiers,
            "place_of_service": self.place_of_service,
        }

    def __repr__(self) -> str:
        return "<billing-search-resolved-query selector=<redacted>>"


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchSelectorBindingScope:
    """One explicit selector outcome for one deduplicated network binding."""

    binding_ordinal: int
    snapshot_id: str
    state: str
    source_scope: ResolvedBillingEntitySourceScope | None = None
    billing_entity_ref: str | None = None

    def __post_init__(self) -> None:
        if (
            type(self.binding_ordinal) is not int
            or self.binding_ordinal < 0
            or type(self.snapshot_id) is not str
            or not self.snapshot_id
            or self.state not in BILLING_SELECTOR_STATES
        ):
            raise serving_unavailable()
        is_matched = self.state == BILLING_SELECTOR_MATCHED
        if is_matched:
            if (
                type(self.source_scope) is not ResolvedBillingEntitySourceScope
                or type(self.billing_entity_ref) is not str
            ):
                raise serving_unavailable()
            try:
                decode_billing_entity_ref(self.billing_entity_ref)
            except PTG2BillingAssociationDataError:
                raise serving_unavailable() from None
        elif self.source_scope is not None or self.billing_entity_ref is not None:
            raise serving_unavailable()

    def __repr__(self) -> str:
        return (
            "<billing-search-selector-binding-scope "
            f"state={self.state} binding_ordinal={self.binding_ordinal}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchSelectorScope:
    """Raw-value-free selector outcomes covering one release binding set."""

    selector_kind: str
    bindings: tuple[BillingSearchSelectorBindingScope, ...]

    def __post_init__(self) -> None:
        if (
            self.selector_kind not in BILLING_SELECTOR_KINDS
            or type(self.bindings) is not tuple
            or any(
                type(binding) is not BillingSearchSelectorBindingScope
                for binding in self.bindings
            )
        ):
            raise serving_unavailable()
        coordinates = tuple(
            (binding.binding_ordinal, binding.snapshot_id) for binding in self.bindings
        )
        if coordinates != tuple(sorted(set(coordinates))):
            raise serving_unavailable()

    def __repr__(self) -> str:
        return (
            "<billing-search-selector-scope "
            f"selector_kind={self.selector_kind} binding_count={len(self.bindings)}>"
        )


def _canonical_source_publication(
    publication: object,
) -> TaxIdentitySourcePublication | None:
    if publication is None:
        return None
    if type(publication) is not TaxIdentitySourcePublication:
        raise serving_unavailable()
    try:
        canonical = tax_identity_source_publication_from_metadata(publication.as_dict())
    except TaxIdentitySourceProjectionError:
        raise serving_unavailable() from None
    if canonical != publication:
        raise serving_unavailable()
    return publication


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchBindingPin:
    """One release binding and its source-publication-aware serving descriptor."""

    binding: PlanReleaseSnapshotBinding
    serving_tables: PTG2ServingTables

    def __post_init__(self) -> None:
        if (
            type(self.binding) is not PlanReleaseSnapshotBinding
            or self.binding.role != PLAN_RELEASE_IN_NETWORK_ROLE
            or type(self.serving_tables) is not PTG2ServingTables
            or not self.serving_tables.uses_v4_graph
            or self.serving_tables.snapshot_id != self.binding.snapshot_id
            or not is_release_binding_serving_scope_exact(
                self.serving_tables,
                self.binding,
            )
        ):
            raise serving_unavailable()
        publication = _canonical_source_publication(
            self.serving_tables.provider_tax_identity_source_publication
        )
        if publication is not None and publication.source_count != (
            self.serving_tables.source_count
        ):
            raise serving_unavailable()

    @property
    def source_publication(self) -> TaxIdentitySourcePublication | None:
        """Return the canonical snapshot-local source publication."""

        return self.serving_tables.provider_tax_identity_source_publication

    def __repr__(self) -> str:
        return (
            "<billing-search-binding-pin "
            f"binding_ordinal={self.binding.binding_ordinal} source=<redacted>>"
        )


__all__ = [
    "BILLING_SEARCH_DEFAULT_PAGE_SIZE",
    "BILLING_SEARCH_MAX_PAGE_SIZE",
    "BILLING_SEARCH_RESULT_MATCHED",
    "BILLING_SEARCH_RESULT_NO_MATCHING_RATES",
    "BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY",
    "BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS",
    "BILLING_SEARCH_RESULT_NO_SNAPSHOT",
    "BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE",
    "BILLING_SELECTOR_MATCHED",
    "BILLING_SELECTOR_NO_MATCH",
    "BILLING_SELECTOR_PROJECTION_UNAVAILABLE",
    "BillingSearchBindingPin",
    "BillingSearchResolvedQuery",
    "BillingSearchSelectorBindingScope",
    "BillingSearchSelectorScope",
    "BillingSearchServingUnavailableError",
    "serving_unavailable",
]
