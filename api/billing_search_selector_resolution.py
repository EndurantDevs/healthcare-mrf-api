# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve one authorized POST selector against source-pinned PTG snapshots."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
from typing import Mapping

from api.billing_search_post_endpoint_access import (
    BillingSearchPostEndpointAccess,
    validate_billing_search_post_endpoint_access,
)
from api.billing_search_post_request import (
    BillingSearchPostRequest,
    apply_entitled_billing_search_tax_identity,
)
from api.billing_search_tin_policy import load_billing_search_tin_policy
from api.plan_release_serving import PTG2_SCHEMA, PlanReleaseServingSelection
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    PTG2BillingAssociationProjectionUnavailable,
    decode_billing_entity_ref,
    encode_billing_entity_ref,
)
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
    resolve_billing_entity_ref_source_scope,
)
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
    serving_unavailable,
)
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    token_policy_descriptor_sha256,
)

_SELECTOR_SCOPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_SELECTOR_SCOPE_V1\x00"
_OPAQUE_REF_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_OPAQUE_REF_SCOPE_V1\x00"
_EIN_TOKEN_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_EIN_TOKEN_SCOPE_V1\x00"
_RESOURCE_NOT_FOUND = "billing_search_resource_not_found"
_SHA256_HEX_CHARACTERS = frozenset("0123456789abcdef")


class BillingSearchSelectorNotFoundError(LookupError):
    """Generic inaccessible-or-unknown resource failure for HTTP 404."""


# Compatibility name used by the central HTTP operation while this slice lands.
BillingSearchSelectorResourceNotFoundError = BillingSearchSelectorNotFoundError


def _resource_not_found() -> BillingSearchSelectorNotFoundError:
    return BillingSearchSelectorNotFoundError(_RESOURCE_NOT_FOUND)


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchSelectorResolution:
    """Resolved scope plus an optional non-raw selector cursor binding."""

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


@dataclass(frozen=True, slots=True, repr=False)
class _PreparedBindingSelector:
    pin: BillingSearchBindingPin
    billing_entity_ref: str | None
    selector_component_sha256: str | None


def _framed_sha256(domain: bytes, *values: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    for value in values:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def _opaque_ref_component(reference: str) -> str:
    return _framed_sha256(_OPAQUE_REF_DOMAIN, reference.encode("ascii"))


def _ein_token_component(
    token: TinTaxIdentityToken,
    *,
    snapshot_key: int,
) -> str:
    return _framed_sha256(
        _EIN_TOKEN_DOMAIN,
        token.token_policy_id.encode("ascii"),
        snapshot_key.to_bytes(8, "big"),
        token.tin_hmac_sha256,
    )


def _source_pinned_binding_pins(
    selection: PlanReleaseServingSelection,
) -> tuple[BillingSearchBindingPin, ...]:
    tables_by_snapshot = selection.network_tables_by_snapshot()
    if tables_by_snapshot is None:
        raise serving_unavailable()
    return tuple(
        BillingSearchBindingPin(
            binding,
            tables_by_snapshot[binding.snapshot_id],
        )
        for binding in selection.in_network_bindings
    )


def _validate_access_selection(
    access: BillingSearchPostEndpointAccess,
    selection: PlanReleaseServingSelection,
) -> tuple[BillingSearchPostRequest, tuple[BillingSearchBindingPin, ...]]:
    validated_access = validate_billing_search_post_endpoint_access(access)
    if type(selection) is not PlanReleaseServingSelection:
        raise serving_unavailable()
    request = validated_access.request
    if not (
        hmac.compare_digest(selection.plan_release_id, validated_access.plan_release_id)
        and hmac.compare_digest(
            selection.healthporta_plan_id,
            request.healthporta_plan_id,
        )
    ):
        raise _resource_not_found()
    return request, _source_pinned_binding_pins(selection)


def _projection_unavailable(
    pin: BillingSearchBindingPin,
) -> BillingSearchSelectorBindingScope:
    return BillingSearchSelectorBindingScope(
        binding_ordinal=pin.binding.binding_ordinal,
        snapshot_id=pin.binding.snapshot_id,
        state=BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    )


def _prepared_opaque_bindings(
    request: BillingSearchPostRequest,
    pins: tuple[BillingSearchBindingPin, ...],
) -> tuple[_PreparedBindingSelector, ...]:
    reference = request.billing_entity_ref
    if type(reference) is not str:
        raise _resource_not_found()
    try:
        decode_billing_entity_ref(reference)
        selector_component = _opaque_ref_component(reference)
    except (PTG2BillingAssociationDataError, UnicodeError):
        raise _resource_not_found() from None
    return tuple(
        _PreparedBindingSelector(
            pin=pin,
            billing_entity_ref=(
                reference if pin.source_publication is not None else None
            ),
            selector_component_sha256=selector_component,
        )
        for pin in pins
    )


def _verified_policy_token(
    publication,
    normalized_ein: str,
    *,
    environment_map: Mapping[str, str] | None,
) -> TinTaxIdentityToken:
    expected_descriptor = token_policy_descriptor_sha256(publication.token_policy_id)
    if not hmac.compare_digest(
        publication.token_policy_descriptor_sha256.hex(),
        expected_descriptor,
    ):
        raise serving_unavailable()
    projector = load_billing_search_tin_policy(
        publication.token_policy_id,
        environment_map,
    )
    if not hmac.compare_digest(
        projector.token_policy_id,
        publication.token_policy_id,
    ):
        raise serving_unavailable()
    token = projector.tokenize_ein(normalized_ein)
    if type(token) is not TinTaxIdentityToken or not hmac.compare_digest(
        token.token_policy_id, publication.token_policy_id
    ):
        raise serving_unavailable()
    return token


def _prepared_ein_bindings(
    request: BillingSearchPostRequest,
    pins: tuple[BillingSearchBindingPin, ...],
    *,
    environment_map: Mapping[str, str] | None,
) -> tuple[_PreparedBindingSelector, ...]:
    def prepare(
        tax_identity_type: str,
        normalized_ein: str,
    ) -> tuple[_PreparedBindingSelector, ...]:
        """Tokenize the transient EIN under each exact published policy."""

        if tax_identity_type != "ein":
            raise serving_unavailable()
        prepared_bindings: list[_PreparedBindingSelector] = []
        for pin in pins:
            publication = pin.source_publication
            if publication is None:
                prepared_bindings.append(_PreparedBindingSelector(pin, None, None))
                continue
            snapshot_key = pin.serving_tables.shared_snapshot_key
            if type(snapshot_key) is not int:
                raise serving_unavailable()
            token = _verified_policy_token(
                publication,
                normalized_ein,
                environment_map=environment_map,
            )
            reference = encode_billing_entity_ref(
                snapshot_key=snapshot_key,
                tin_id_128=token.tin_id_128,
                tin_hmac_sha256=token.tin_hmac_sha256,
            )
            prepared_bindings.append(
                _PreparedBindingSelector(
                    pin,
                    reference,
                    _ein_token_component(token, snapshot_key=snapshot_key),
                )
            )
        return tuple(prepared_bindings)

    return apply_entitled_billing_search_tax_identity(request, prepare)


def _prepared_bindings(
    request: BillingSearchPostRequest,
    pins: tuple[BillingSearchBindingPin, ...],
    *,
    environment_map: Mapping[str, str] | None,
) -> tuple[_PreparedBindingSelector, ...]:
    if request.selector_kind == "billing_entity_ref":
        return _prepared_opaque_bindings(request, pins)
    if request.tax_identity_type == "npi":
        return tuple(_PreparedBindingSelector(pin, None, None) for pin in pins)
    if request.tax_identity_type == "ein":
        return _prepared_ein_bindings(
            request,
            pins,
            environment_map=environment_map,
        )
    raise serving_unavailable()


async def _resolved_binding(
    session,
    prepared: _PreparedBindingSelector,
    *,
    schema_name: str,
) -> BillingSearchSelectorBindingScope:
    pin = prepared.pin
    reference = prepared.billing_entity_ref
    publication = pin.source_publication
    if reference is None or publication is None:
        return _projection_unavailable(pin)
    try:
        source_scope = await resolve_billing_entity_ref_source_scope(
            session,
            schema_name=schema_name,
            snapshot_key=pin.serving_tables.shared_snapshot_key,
            billing_entity_ref=reference,
            source_publication=publication,
        )
    except PTG2BillingAssociationProjectionUnavailable:
        return _projection_unavailable(pin)
    if source_scope is None:
        return BillingSearchSelectorBindingScope(
            binding_ordinal=pin.binding.binding_ordinal,
            snapshot_id=pin.binding.snapshot_id,
            state=BILLING_SELECTOR_NO_MATCH,
        )
    if (
        type(source_scope) is not ResolvedBillingEntitySourceScope
        or source_scope.snapshot_key != pin.serving_tables.shared_snapshot_key
        or source_scope.publication != publication
    ):
        raise serving_unavailable()
    return BillingSearchSelectorBindingScope(
        binding_ordinal=pin.binding.binding_ordinal,
        snapshot_id=pin.binding.snapshot_id,
        state=BILLING_SELECTOR_MATCHED,
        source_scope=source_scope,
        billing_entity_ref=reference,
    )


def _selector_scope_digest(
    *,
    selector_kind: str,
    prepared: tuple[_PreparedBindingSelector, ...],
    resolved: tuple[BillingSearchSelectorBindingScope, ...],
) -> str | None:
    components = tuple(
        candidate.selector_component_sha256
        for candidate in prepared
        if candidate.selector_component_sha256 is not None
    )
    if not components:
        return None
    scope_fields_by_name = {
        "bindings": [
            {
                "binding_ordinal": binding.binding_ordinal,
                "component_sha256": candidate.selector_component_sha256,
                "snapshot_id": binding.snapshot_id,
                "state": binding.state,
            }
            for candidate, binding in zip(prepared, resolved, strict=True)
        ],
        "selector_kind": selector_kind,
    }
    encoded = json.dumps(
        scope_fields_by_name,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return _framed_sha256(_SELECTOR_SCOPE_DOMAIN, encoded)


async def resolve_billing_search_selector(
    session,
    *,
    access: BillingSearchPostEndpointAccess,
    source_pinned_selection: PlanReleaseServingSelection,
    schema_name: str = PTG2_SCHEMA,
    environment_map: Mapping[str, str] | None = None,
) -> BillingSearchSelectorResolution:
    """Resolve one authorized selector without retaining or hashing raw TINs."""

    try:
        request, pins = _validate_access_selection(
            access,
            source_pinned_selection,
        )
        prepared_bindings = _prepared_bindings(
            request,
            pins,
            environment_map=environment_map,
        )
        resolved_bindings = tuple(
            [
                await _resolved_binding(
                    session,
                    candidate,
                    schema_name=schema_name,
                )
                for candidate in prepared_bindings
            ]
        )
        if (
            request.selector_kind == "billing_entity_ref"
            and resolved_bindings
            and all(
                binding.state == BILLING_SELECTOR_NO_MATCH
                for binding in resolved_bindings
            )
        ):
            raise _resource_not_found()
        selector_scope = BillingSearchSelectorScope(
            selector_kind=request.selector_kind,
            bindings=resolved_bindings,
        )
        return BillingSearchSelectorResolution(
            selector_scope=selector_scope,
            selector_scope_sha256=_selector_scope_digest(
                selector_kind=request.selector_kind,
                prepared=prepared_bindings,
                resolved=resolved_bindings,
            ),
        )
    except BillingSearchSelectorNotFoundError:
        raise
    except BillingSearchServingUnavailableError:
        raise
    except PTG2BillingAssociationProjectionUnavailable:
        raise serving_unavailable() from None
    except Exception:
        raise serving_unavailable() from None


__all__ = [
    "BillingSearchSelectorResolution",
    "BillingSearchSelectorNotFoundError",
    "BillingSearchSelectorResourceNotFoundError",
    "resolve_billing_search_selector",
]
