# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scheduling metadata from immutable PTG approvals; publication still replays its witness."""

import json
from uuid import UUID

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.result_archive_published_authority import lock_ptg_published_result_for_clone
from process.registry_ptg_office_approval import _descriptor
from process.registry_ptg_office_membership import (
    _recipe_context,
    _retained_documents,
    require_ptg_office_authority,
)
from process.registry_ptg_office_retention import office_hold_document
from process.registry_ptg_office_selection import _read_references, _requested
from process.registry_ptg_office_witness import RegistryPTGOfficeWitnessEvidence
from process.registry_ptg_office_witness import _descriptor as validate_office_descriptor
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from process.registry_source_recipe_store import canonical_registry_source_recipes


async def _capture_metadata(session, driver, pin, readers, budget):
    document, hold = await _retained_documents(session, readers.ptg_store, pin)
    budget.reserve(({"raw_byte_count": len(json.dumps(document).encode()) + len(json.dumps(hold).encode())},))
    context, request = await _recipe_context(session, readers.ptg_store, document, pin, readers.control_schema)
    source = await lock_ptg_published_result_for_clone(
        session, schema_name=context.source_specification.ptg_schema_name, authority=document["source_pin"]
    )
    if (
        source.as_dict() != document["source_pin"]
        or document["source_pin"]["operation_id"] != "registry_ptg_office_review_" + UUID(pin.capture_id).hex
    ):
        raise RegistryRefreshAnchorMismatch("registry_refresh_ptg_capture_mismatch")
    size = await driver.fetchval(
        f"SELECT octet_length(manifest_json::text) FROM {_identifier('registry_ptg_office_' + UUID(pin.capture_id).hex)}.capture_manifest WHERE id=1"
    )
    budget.reserve(({"raw_byte_count": size},))
    descriptor = await _descriptor(session, context, request)
    validate_office_descriptor(descriptor, request, context)
    witness = RegistryPTGOfficeWitnessEvidence(
        json.dumps(document["witness"], sort_keys=True, separators=(",", ":")).encode()
    )
    if (
        descriptor.manifest_sha256 != pin.manifest_sha256
        or json.loads(descriptor.manifest_json)["command"] != document["command"]
        or office_hold_document(descriptor, witness, pin.approval_sha256) != hold
    ):
        raise RegistryRefreshAnchorMismatch("registry_refresh_ptg_capture_mismatch")


async def observe_accepted_ptg_recipes(session, bindings, approved, readers, budget):
    """Authenticate exact native metadata without reading graph or office assertion rows."""
    from process.network_cms_registry_address_capture import native_driver

    driver = await native_driver(session)
    await require_ptg_office_authority(driver, readers.control_schema, (session, readers.ptg_store, budget))
    original_path = await session.scalar(text("SELECT pg_catalog.current_setting('search_path')"))
    await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
    original_error = None
    try:
        return await _observed_captures(session, driver, bindings, readers, budget)
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            await session.execute(
                text("SELECT pg_catalog.set_config('search_path',:path,true)"), {"path": original_path}
            )
        except BaseException:
            if original_error is None:
                raise


async def _observed_captures(session, driver, bindings, readers, budget):
    recipes = await _read_references(
        driver,
        _identifier(readers.control_schema),
        _requested([binding.record_json for binding in bindings]),
        readers.ptg_store,
    )
    if len(recipes) != len(bindings):
        raise RegistryRefreshAnchorUnavailable("registry_refresh_ptg_incomplete")
    verified_by_capture, scopes_by_binding = {}, {}
    for binding, recipe in zip(bindings, recipes, strict=True):
        pin = recipe.source_pin
        identity = canonical_registry_source_recipes((recipe,))
        if pin.capture_id in verified_by_capture and verified_by_capture[pin.capture_id] != identity:
            raise RegistryRefreshAnchorMismatch("registry_refresh_ptg_capture_mismatch")
        if pin.capture_id not in verified_by_capture:
            await _capture_metadata(session, driver, pin, readers, budget)
            verified_by_capture[pin.capture_id] = identity
        scopes_by_binding[binding.binding_id] = {
            "source_system": "ptg",
            "source_id": pin.coordinates.source_id,
            "dataset_schema": pin.coordinates.dataset_schema,
            "producer_id": pin.coordinates.producer_id,
            "source_key": binding.record_json["source_key"],
            "approved_binding_sha256": binding.approved_binding_sha256,
            "capture_generation": pin.generation_id,
        }
    return scopes_by_binding, recipes
