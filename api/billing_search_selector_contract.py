# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable, redacted contracts for source-pinned billing selectors."""

from __future__ import annotations

from dataclasses import dataclass

from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import (
    PLAN_RELEASE_IN_NETWORK_ROLE,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    decode_billing_entity_ref,
)
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
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
_SHA256_HEX_CHARACTERS = frozenset("0123456789abcdef")


class BillingSearchSelectorNotFoundError(LookupError):
    """Generic inaccessible-or-unknown resource failure for HTTP 404."""


BillingSearchSelectorResourceNotFoundError = BillingSearchSelectorNotFoundError


class BillingSearchServingUnavailableError(PTG2ManifestArtifactError):
    """Value-free failure for an unavailable immutable serving bundle."""


def serving_unavailable() -> BillingSearchServingUnavailableError:
    """Return the generic API-safe serving failure."""

    return BillingSearchServingUnavailableError(
        "billing_search_serving_generation_unavailable"
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
    """One release binding and its validated source-aware descriptor."""

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
        """Return the snapshot-local sealed source publication, when present."""

        return self.serving_tables.provider_tax_identity_source_publication

    def __repr__(self) -> str:
        return (
            "<billing-search-binding-pin "
            f"binding_ordinal={self.binding.binding_ordinal} source=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchSelectorBindingScope:
    """One explicit selector outcome for one in-network binding."""

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
            or type(self.state) is not str
            or self.state not in BILLING_SELECTOR_STATES
        ):
            raise serving_unavailable()
        if self.state == BILLING_SELECTOR_MATCHED:
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
    """Raw-value-free outcomes covering one exact release binding set."""

    selector_kind: str
    bindings: tuple[BillingSearchSelectorBindingScope, ...]

    def __post_init__(self) -> None:
        if (
            type(self.selector_kind) is not str
            or self.selector_kind not in BILLING_SELECTOR_KINDS
            or type(self.bindings) is not tuple
            or not self.bindings
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
            f"selector_kind={self.selector_kind} "
            f"binding_count={len(self.bindings)}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchSelectorResolution:
    """Resolved scope plus a pseudonymous future-cursor binding."""

    selector_scope: BillingSearchSelectorScope
    selector_scope_sha256: str | None

    def __post_init__(self) -> None:
        digest = self.selector_scope_sha256
        if type(self.selector_scope) is not BillingSearchSelectorScope or (
            digest is not None
            and (
                type(digest) is not str
                or len(digest) != 64
                or any(character not in _SHA256_HEX_CHARACTERS for character in digest)
                or digest == "0" * 64
            )
        ):
            raise serving_unavailable()

    def __repr__(self) -> str:
        return (
            "<billing-search-selector-resolution "
            f"selector_kind={self.selector_scope.selector_kind}>"
        )


__all__ = [
    "BILLING_SELECTOR_MATCHED",
    "BILLING_SELECTOR_NO_MATCH",
    "BILLING_SELECTOR_PROJECTION_UNAVAILABLE",
    "BillingSearchBindingPin",
    "BillingSearchSelectorNotFoundError",
    "BillingSearchSelectorResourceNotFoundError",
    "BillingSearchSelectorBindingScope",
    "BillingSearchSelectorResolution",
    "BillingSearchSelectorScope",
    "BillingSearchServingUnavailableError",
    "serving_unavailable",
]
