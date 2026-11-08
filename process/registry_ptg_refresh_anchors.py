# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive exact PTG refresh anchors from protected native captures and holds."""

import asyncpg
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.registry_schema import registry_schema
from process.network_approved_membership_source import ApprovedMembershipSource
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget, RegistryPTGGraphReadError
from process.registry_ptg_office_membership import require_ptg_office_authority, require_ptg_office_recipe
from process.registry_ptg_office_selection import _read_references, _requested
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from process.registry_refresh_anchor_contract import (
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_store import canonical_registry_source_recipes

_UNAVAILABLE_ERRORS = {
    "registry_office_selection_store_unprotected",
    "registry_office_selection_transaction_required",
    "registry_office_selection_unavailable",
    "registry_ptg_scope_store_unprotected",
    "registry_ptg_scope_transaction_required",
    "registry_ptg_scope_unavailable",
    "registry_ptg_published_scope_unavailable",
    "registry_ptg_office_transaction_changed",
    "registry_ptg_scope_company_fence_unavailable",
    "registry_ptg_office_recipe_owner_unavailable",
}


async def resolve_ptg_refresh_anchors(session, bindings, *, store, read_budget, approved_source):
    """Return all exact native scopes, sharing physical verification by capture.

    The host owns the genuine pre-snapshot company fence and reader credentials.
    Incoming request scopes never enter source qualification.
    """
    _require_anchor_request(session, bindings, store, read_budget, approved_source)
    schema = store.control_schema or registry_schema()
    try:
        namespace = _namespace(schema)
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        authority = await require_ptg_office_authority(driver, schema, (session, store, read_budget))
    except (ValueError, TypeError, AttributeError, SQLAlchemyError, asyncpg.PostgresError) as error:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable") from error
    try:
        requested = _requested([binding.record_json for binding in bindings])
        if len(requested) != len(bindings):
            raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
        recipes = await _read_references(driver, namespace, requested, store)
        identity_by_capture = {}
        scopes_by_binding = {}
        for binding, recipe in zip(bindings, recipes, strict=True):
            pin = recipe.source_pin
            identity = canonical_registry_source_recipes((recipe,))
            if pin.capture_id in identity_by_capture and identity_by_capture[pin.capture_id] != identity:
                raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
            if pin.capture_id not in identity_by_capture:
                await require_ptg_office_recipe(driver, recipe, approved_source, schema, authority)
                identity_by_capture[pin.capture_id] = identity
            coordinates = pin.coordinates
            scopes_by_binding[binding.binding_id] = {
                "source_system": "ptg",
                "source_id": coordinates.source_id,
                "dataset_schema": coordinates.dataset_schema,
                "producer_id": coordinates.producer_id,
                "source_key": binding.record_json["source_key"],
                "approved_binding_sha256": binding.approved_binding_sha256,
                "capture_generation": pin.generation_id,
            }
        return scopes_by_binding
    except RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable:
        raise
    except (
        RegistryPTGGraphReadError,
        SQLAlchemyError,
        asyncpg.PostgresError,
        KeyError,
        TypeError,
        AttributeError,
    ) as error:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable") from error
    except ValueError as error:
        if str(error) in _UNAVAILABLE_ERRORS:
            raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable") from error
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch") from error


def _require_anchor_request(session, bindings, store, read_budget, approved_source):
    if (
        type(session) is not AsyncSession
        or type(store) is not RegistryPTGProducerScopeStore
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or type(approved_source) is not ApprovedMembershipSource
        or not session.in_transaction()
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    if (
        type(bindings) is not tuple
        or len(bindings) > 1000
        or any(
            type(binding) is not ApprovedRefreshBinding
            or binding.record_kind != "network_binding"
            or type(binding.record_json) is not dict
            or binding.record_json.get("source_system") != "ptg"
            or binding.record_json.get("archived") is not False
            for binding in bindings
        )
    ):
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
