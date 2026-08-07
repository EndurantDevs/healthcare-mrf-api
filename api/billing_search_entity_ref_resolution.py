# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve an authorized opaque billing reference without transport coupling."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json

from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorNotFoundError,
    BillingSearchSelectorResolution,
    BillingSearchSelectorResourceNotFoundError,
    BillingSearchSelectorScope,
    serving_unavailable,
)
from api.plan_release_serving import PTG2_SCHEMA, PlanReleaseServingSelection
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationDataError,
    PTG2BillingAssociationProjectionUnavailable,
    decode_billing_entity_ref,
)
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
    resolve_billing_entity_ref_source_scope,
)

_SELECTOR_SCOPE_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_SELECTOR_SCOPE_V1\x00"
_OPAQUE_REF_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_OPAQUE_REF_SCOPE_V1\x00"
_RESOURCE_NOT_FOUND = "billing_search_resource_not_found"
_PREPARATION_READY = "ready"
_PREPARATION_NOT_FOUND = "not_found"
_PREPARATION_UNAVAILABLE = "unavailable"


def _resource_not_found() -> BillingSearchSelectorNotFoundError:
    return BillingSearchSelectorNotFoundError(_RESOURCE_NOT_FOUND)


@dataclass(frozen=True, slots=True, repr=False)
class _PreparedBindingSelector:
    pin: BillingSearchBindingPin
    billing_entity_ref: str | None
    selector_component_sha256: str


@dataclass(frozen=True, slots=True, repr=False)
class _PreparedSelector:
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
    billing_entity_ref: object,
    pins: tuple[BillingSearchBindingPin, ...],
) -> tuple[_PreparedBindingSelector, ...]:
    reference = billing_entity_ref
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


def _prepared_entity_ref_selector_or_state(
    billing_entity_ref: object,
    authorized_plan_release_id: object,
    source_pinned_selection: object,
) -> tuple[str, _PreparedSelector | None]:
    pins = None
    try:
        if type(source_pinned_selection) is not PlanReleaseServingSelection:
            raise serving_unavailable()
        if type(authorized_plan_release_id) is not str or not hmac.compare_digest(
            source_pinned_selection.plan_release_id,
            authorized_plan_release_id,
        ):
            return _PREPARATION_NOT_FOUND, None
        pins = _source_pinned_binding_pins(source_pinned_selection)
        return _PREPARATION_READY, _PreparedSelector(
            bindings=_prepared_opaque_bindings(billing_entity_ref, pins),
        )
    except BillingSearchSelectorNotFoundError:
        return _PREPARATION_NOT_FOUND, None
    except Exception:
        return _PREPARATION_UNAVAILABLE, None
    finally:
        del (
            authorized_plan_release_id,
            billing_entity_ref,
            pins,
            source_pinned_selection,
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
) -> str:
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
        "selector_kind": "billing_entity_ref",
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
            selector_kind="billing_entity_ref",
            bindings=resolved,
        )
        return BillingSearchSelectorResolution(
            selector_scope=scope,
            selector_scope_sha256=_selector_scope_digest(prepared, resolved),
        )
    except Exception:
        return None


async def _resolve_prepared_selector(
    session,
    *,
    prepared: _PreparedSelector,
    schema_name: str = PTG2_SCHEMA,
) -> BillingSearchSelectorResolution:
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
    if resolved_scopes and all(
        binding.state == BILLING_SELECTOR_NO_MATCH for binding in resolved_scopes
    ):
        raise _resource_not_found()
    resolution = _completed_resolution_or_none(prepared, resolved_scopes)
    if resolution is None:
        raise serving_unavailable()
    return resolution


async def resolve_billing_search_entity_ref_selector(
    session,
    *,
    billing_entity_ref: object,
    authorized_plan_release_id: object,
    source_pinned_selection: object,
    schema_name: str = PTG2_SCHEMA,
) -> BillingSearchSelectorResolution:
    """Resolve one authorized opaque reference against source-pinned snapshots."""

    state, prepared = _prepared_entity_ref_selector_or_state(
        billing_entity_ref,
        authorized_plan_release_id,
        source_pinned_selection,
    )
    del authorized_plan_release_id, billing_entity_ref, source_pinned_selection
    if state == _PREPARATION_NOT_FOUND:
        raise _resource_not_found()
    if state != _PREPARATION_READY or prepared is None:
        raise serving_unavailable()
    return await _resolve_prepared_selector(
        session,
        prepared=prepared,
        schema_name=schema_name,
    )


__all__ = [
    "BillingSearchSelectorNotFoundError",
    "BillingSearchSelectorResourceNotFoundError",
    "resolve_billing_search_entity_ref_selector",
]
