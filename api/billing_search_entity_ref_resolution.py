# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve an authorized opaque billing reference without transport coupling."""

from __future__ import annotations

import hmac

from api.billing_search_selector_contract import (
    BillingSearchSelectorResolution,
    serving_unavailable,
)
from api.billing_search_selector_resolution import (
    _PREPARATION_NOT_FOUND,
    _PREPARATION_READY,
    _PREPARATION_UNAVAILABLE,
    _PreparedSelector,
    _prepared_opaque_bindings,
    _resolve_prepared_selector,
    _resource_not_found,
    _source_pinned_binding_pins,
    BillingSearchSelectorNotFoundError,
)
from api.plan_release_serving import PTG2_SCHEMA, PlanReleaseServingSelection


def _prepared_entity_ref_selector_or_state(
    billing_entity_ref: object,
    authorized_plan_release_id: object,
    source_pinned_selection: object,
) -> tuple[str, _PreparedSelector | None]:
    pins = None
    try:
        if type(source_pinned_selection) is not PlanReleaseServingSelection:
            raise serving_unavailable()
        if (
            type(authorized_plan_release_id) is not str
            or not hmac.compare_digest(
                source_pinned_selection.plan_release_id,
                authorized_plan_release_id,
            )
        ):
            return _PREPARATION_NOT_FOUND, None
        pins = _source_pinned_binding_pins(source_pinned_selection)
        return _PREPARATION_READY, _PreparedSelector(
            selector_kind="billing_entity_ref",
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


__all__ = ["resolve_billing_search_entity_ref_selector"]
