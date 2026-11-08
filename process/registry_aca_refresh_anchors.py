# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive ACA continuity from the original accepted logical capture."""

from uuid import UUID

import asyncpg
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.registry_aca_source_notifications import RegistryACASourceNotificationStore
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget, RegistryPTGGraphReadError
from process.registry_refresh_anchor_contract import (
    ApprovedRefreshBinding,
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorUnavailable,
)


def _capture_groups(bindings):
    if type(bindings) is not tuple or len(bindings) > 1000:
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
    bindings_by_capture = {}
    binding_ids = set()
    for binding in bindings:
        if (
            type(binding) is not ApprovedRefreshBinding
            or binding.record_json.get("source_system") != "aca"
            or binding.record_json.get("archived") is not False
        ):
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        if binding.binding_id in binding_ids:
            raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
        binding_ids.add(binding.binding_id)
        coordinates = RegistryNetworkSourceCoordinates(
            **{name: binding.record_json[name] for name in RegistryNetworkSourceCoordinates.__dataclass_fields__}
        )
        bindings_by_capture.setdefault(coordinates, []).append(binding)
    if len(bindings_by_capture) > 100:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    return bindings_by_capture


async def _accepted_original(session, driver, store, coordinates, budget):
    capture_id = UUID(coordinates.dataset_id)
    await store.qualify(driver, writing=False)
    size = await driver.fetchval(
        f"SELECT octet_length(notification_json) FROM {_identifier(store.control_schema)}.registry_aca_source_notification WHERE capture_id=$1",
        capture_id,
    )
    if size is None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    budget.reserve(({"raw_byte_count": size},), 1)
    accepted = await store.accepted(session, capture_id)
    recipe = accepted.get("recipe") if type(accepted) is dict else None
    if recipe is None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    if recipe.binding_coordinates != coordinates:
        raise RegistryRefreshAnchorMismatch("registry_refresh_anchor_mismatch")
    pin = recipe.source_pin
    if pin.retained_successor is not None or pin.retained_epoch is not None:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    return pin


async def resolve_aca_refresh_anchors(session, bindings, *, store, read_budget):
    """Reprove each original accepted capture once, sharing its immutable native receipt."""
    if (
        type(session) is not AsyncSession
        or type(store) is not RegistryACASourceNotificationStore
        or type(read_budget) is not RegistryPTGGraphReadBudget
        or not session.in_transaction()
    ):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable")
    try:
        bindings_by_capture = _capture_groups(bindings)
        driver = await native_driver(session)
        scopes_by_binding = {}
        for coordinates, selected in bindings_by_capture.items():
            pin = await _accepted_original(session, driver, store, coordinates, read_budget)
            for binding in selected:
                scopes_by_binding[binding.binding_id] = {
                    "source_system": "aca",
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
        ValueError,
        TypeError,
        KeyError,
        RegistryPTGGraphReadError,
        SQLAlchemyError,
        asyncpg.PostgresError,
    ) as error:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_anchor_unavailable") from error
