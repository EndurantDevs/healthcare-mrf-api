# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve one authorized billing selector against source-pinned snapshots."""

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
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorResolution,
    BillingSearchSelectorScope,
    serving_unavailable,
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
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    token_policy_descriptor_sha256,
)

_SELECTOR_SCOPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_SELECTOR_SCOPE_V1\x00"
_OPAQUE_REF_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_OPAQUE_REF_SCOPE_V1\x00"
_EIN_TOKEN_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_EIN_TOKEN_SCOPE_V1\x00"
_RESOURCE_NOT_FOUND = "billing_search_resource_not_found"
_PREPARATION_READY = "ready"
_PREPARATION_NOT_FOUND = "not_found"
_PREPARATION_UNAVAILABLE = "unavailable"


class BillingSearchSelectorNotFoundError(LookupError):
    """Generic inaccessible-or-unknown resource failure for HTTP 404."""


BillingSearchSelectorResourceNotFoundError = BillingSearchSelectorNotFoundError


def _resource_not_found() -> BillingSearchSelectorNotFoundError:
    return BillingSearchSelectorNotFoundError(_RESOURCE_NOT_FOUND)


@dataclass(frozen=True, slots=True, repr=False)
class _PreparedBindingSelector:
    pin: BillingSearchBindingPin
    billing_entity_ref: str | None
    selector_component_sha256: str | None


@dataclass(frozen=True, slots=True, repr=False)
class _PreparedSelector:
    selector_kind: str
    bindings: tuple[_PreparedBindingSelector, ...]


def _framed_sha256(domain: bytes, *values: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    for value in values:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def _opaque_ref_component(reference: str) -> str:
    return _framed_sha256(_OPAQUE_REF_DOMAIN, reference.encode("ascii"))


def _ein_token_component(token: TinTaxIdentityToken, *, snapshot_key: int) -> str:
    return _framed_sha256(
        _EIN_TOKEN_DOMAIN,
        token.token_policy_id.encode("ascii"),
        snapshot_key.to_bytes(8, "big"),
        token.tin_hmac_sha256,
    )


def _source_pinned_binding_pins(
    selection: PlanReleaseServingSelection,
) -> tuple[BillingSearchBindingPin, ...]:
    if not selection.includes_billing_tax_identity_source:
        raise serving_unavailable()
    tables_by_snapshot = selection.network_tables_by_snapshot()
    if tables_by_snapshot is None:
        raise serving_unavailable()
    return tuple(
        BillingSearchBindingPin(binding, tables_by_snapshot[binding.snapshot_id])
        for binding in selection.in_network_bindings
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
    expected_descriptor = token_policy_descriptor_sha256(
        publication.token_policy_id
    )
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
        token.token_policy_id,
        publication.token_policy_id,
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
        """Project one transient EIN under every exact snapshot policy."""

        if tax_identity_type != "ein":
            raise serving_unavailable()
        prepared_bindings: list[_PreparedBindingSelector] = []
        for pin in pins:
            publication = pin.source_publication
            if publication is None:
                prepared_bindings.append(_PreparedBindingSelector(pin, None, None))
                continue
            snapshot_key = pin.serving_tables.shared_snapshot_key
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


def _prepared_selector_or_state(
    access: object,
    source_pinned_selection: object,
    *,
    trusted_now: object,
    environment_map: Mapping[str, str] | None,
) -> tuple[str, _PreparedSelector | None]:
    validated_access = None
    request = None
    pins = None
    try:
        validated_access = validate_billing_search_post_endpoint_access(
            access,
            trusted_now=trusted_now,
        )
        if type(source_pinned_selection) is not PlanReleaseServingSelection:
            raise serving_unavailable()
        request = validated_access.request
        if not (
            hmac.compare_digest(
                source_pinned_selection.plan_release_id,
                validated_access.plan_release_id,
            )
            and hmac.compare_digest(
                source_pinned_selection.healthporta_plan_id,
                request.healthporta_plan_id,
            )
        ):
            return _PREPARATION_NOT_FOUND, None
        pins = _source_pinned_binding_pins(source_pinned_selection)
        return _PREPARATION_READY, _PreparedSelector(
            selector_kind=request.selector_kind,
            bindings=_prepared_bindings(
                request,
                pins,
                environment_map=environment_map,
            ),
        )
    except BillingSearchSelectorNotFoundError:
        return _PREPARATION_NOT_FOUND, None
    except Exception:
        return _PREPARATION_UNAVAILABLE, None
    finally:
        del (
            access,
            environment_map,
            pins,
            request,
            source_pinned_selection,
            trusted_now,
            validated_access,
        )


def _projection_unavailable(
    pin: BillingSearchBindingPin,
) -> BillingSearchSelectorBindingScope:
    return BillingSearchSelectorBindingScope(
        binding_ordinal=pin.binding.binding_ordinal,
        snapshot_id=pin.binding.snapshot_id,
        state=BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    )


async def _resolved_binding_or_none(
    session,
    prepared: _PreparedBindingSelector,
    *,
    schema_name: str,
) -> BillingSearchSelectorBindingScope | None:
    try:
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
            return None
        return BillingSearchSelectorBindingScope(
            binding_ordinal=pin.binding.binding_ordinal,
            snapshot_id=pin.binding.snapshot_id,
            state=BILLING_SELECTOR_MATCHED,
            source_scope=source_scope,
            billing_entity_ref=reference,
        )
    except Exception:
        return None


def _selector_scope_digest(
    prepared: _PreparedSelector,
    resolved: tuple[BillingSearchSelectorBindingScope, ...],
) -> str | None:
    components = tuple(
        candidate.selector_component_sha256
        for candidate in prepared.bindings
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
            for candidate, binding in zip(
                prepared.bindings,
                resolved,
                strict=True,
            )
        ],
        "selector_kind": prepared.selector_kind,
    }
    encoded = json.dumps(
        scope_fields_by_name,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return _framed_sha256(_SELECTOR_SCOPE_DOMAIN, encoded)


def _completed_resolution_or_none(
    prepared: _PreparedSelector,
    resolved: tuple[BillingSearchSelectorBindingScope, ...],
) -> BillingSearchSelectorResolution | None:
    try:
        scope = BillingSearchSelectorScope(
            selector_kind=prepared.selector_kind,
            bindings=resolved,
        )
        return BillingSearchSelectorResolution(
            selector_scope=scope,
            selector_scope_sha256=_selector_scope_digest(prepared, resolved),
        )
    except Exception:
        return None


async def resolve_billing_search_selector(
    session,
    *,
    access: BillingSearchPostEndpointAccess,
    source_pinned_selection: PlanReleaseServingSelection,
    trusted_now: object,
    schema_name: str = PTG2_SCHEMA,
    environment_map: Mapping[str, str] | None = None,
) -> BillingSearchSelectorResolution:
    """Resolve one authorized selector without retaining or hashing raw TINs.

    The supplied selection must come from ``resolve_plan_release_serving`` with
    ``include_billing_tax_identity_source=True``.
    """

    state, prepared = _prepared_selector_or_state(
        access,
        source_pinned_selection,
        trusted_now=trusted_now,
        environment_map=environment_map,
    )
    del access, environment_map, source_pinned_selection, trusted_now
    if state == _PREPARATION_NOT_FOUND:
        raise _resource_not_found()
    if state != _PREPARATION_READY or prepared is None:
        raise serving_unavailable()
    resolved_bindings: list[BillingSearchSelectorBindingScope] = []
    for candidate in prepared.bindings:
        resolved = await _resolved_binding_or_none(
            session,
            candidate,
            schema_name=schema_name,
        )
        if resolved is None:
            raise serving_unavailable()
        resolved_bindings.append(resolved)
    resolved_scopes = tuple(resolved_bindings)
    if (
        prepared.selector_kind == "billing_entity_ref"
        and resolved_scopes
        and all(
            binding.state == BILLING_SELECTOR_NO_MATCH
            for binding in resolved_scopes
        )
    ):
        raise _resource_not_found()
    resolution = _completed_resolution_or_none(prepared, resolved_scopes)
    if resolution is None:
        raise serving_unavailable()
    return resolution


__all__ = [
    "BillingSearchSelectorNotFoundError",
    "BillingSearchSelectorResolution",
    "BillingSearchSelectorResourceNotFoundError",
    "resolve_billing_search_selector",
]
