# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep original approved generic scope while selecting a genuine accepted capture."""

from dataclasses import replace

from process.network_address_projection import _identifier
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, with_fhir_successor
from process.network_fhir_retained_capture import pin_retained_fhir_read
from process.network_fhir_retained_capture_contract import PRODUCER, SEMANTIC_PROFILE
from process.provider_directory_admission_seal import ADMISSION_METADATA_SUMMARY_MAX_BYTES
from process.registry_generic_accepted_capture import (
    is_original_generic_capture_current,
    read_accepted_generic_capture_metadata,
    read_current_generic_fhir_head,
)
from process.registry_generic_refresh_anchors import (
    _capture_groups,
    original_generic_capture_scopes,
    read_original_generic_capture,
    read_original_generic_capture_metadata,
)
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch, RegistryRefreshAnchorUnavailable
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    resolve_retained_registry_source_recipes,
)


async def generic_refresh_endpoints(driver, bindings, read_budget):
    """Discover immutable original endpoints before the final source snapshot; metadata grants no read brand."""
    endpoints = set()
    for coordinates in _capture_groups(bindings):
        capture = await read_original_generic_capture_metadata(driver, coordinates, read_budget)
        endpoints.add((capture.origin.schema_name, capture.origin.source_id, capture.origin.endpoint_id))
    return tuple(sorted(endpoints))


def _original_pin(capture, coordinates, retained_recipes):
    matches = set()
    for recipe in retained_recipes:
        pin = recipe.source_pin
        if recipe.binding_coordinates == coordinates and type(pin) is PinnedFHIRMembershipSource:
            original = replace(pin, retained_successor=None)
            if original.retained_capture == capture:
                matches.add(original)
    if len(matches) > 1:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_recipe_ambiguous")
    if matches:
        return next(iter(matches))
    if capture.origin.admission_profile != SEMANTIC_PROFILE:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_semantic_date_unavailable")
    return _capture_pin(capture, PRODUCER, capture.origin.semantic_projection_as_of)


def _capture_pin(capture, alias_scope, as_of):
    origin = capture.origin
    return PinnedFHIRMembershipSource(
        origin.schema_name,
        origin.source_id,
        origin.endpoint_id,
        origin.dataset_id,
        origin.dataset_sha256,
        capture.release_id,
        origin.resource_parent_oid,
        alias_scope,
        as_of,
        retained_capture=capture,
    )


async def _current_organizations(driver, capture, bindings, budget):
    identities = sorted({binding.record_json["source_key"] for binding in bindings})
    budget.reserve(({"raw_byte_count": len(identities) * 256},), len(identities))
    namespace = _identifier(capture.schema_name)
    rows = await driver.fetch(
        f"""SELECT wanted.resource_id FROM unnest($1::text[]) wanted(resource_id)
        JOIN {namespace}.provider_directory_dataset_resource resource
          ON resource.dataset_id=$2 AND resource.resource_type='Organization' AND resource.resource_id=wanted.resource_id
        JOIN {namespace}.provider_directory_entity_release_evidence evidence
          ON evidence.source_id=$3 AND evidence.release_id=$4 AND evidence.resource_type='Organization'
          AND evidence.resource_id=resource.resource_id AND evidence.payload_sha256=resource.payload_hash
        ORDER BY wanted.resource_id COLLATE "C" LIMIT 1001""",
        identities,
        capture.origin.dataset_id,
        capture.origin.source_id,
        capture.release_id,
    )
    if [row["resource_id"] for row in rows] != identities:
        raise RegistryRefreshAnchorMismatch("registry_refresh_generic_organization_unavailable")


async def _accepted_capture(driver, original, budget):
    if await is_original_generic_capture_current(driver, original):
        return original
    if original.origin.admission_profile != SEMANTIC_PROFILE:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_legacy_advance_unavailable")
    budget.reserve(({"raw_byte_count": ADMISSION_METADATA_SUMMARY_MAX_BYTES},))
    head = await read_current_generic_fhir_head(
        driver,
        schema_name=original.origin.schema_name,
        source_id=original.origin.source_id,
        endpoint_id=original.origin.endpoint_id,
    )
    return await read_accepted_generic_capture_metadata(driver, head, budget)


async def _generic_recipes(driver, bindings, budget, retained_recipes, original_reader):
    scopes, recipes = {}, []
    for coordinates, selected in _capture_groups(bindings).items():
        admitted = await original_reader(driver, coordinates, budget)
        original = admitted.capture if hasattr(admitted, "capture") else admitted
        derived = await original_generic_capture_scopes(driver, original, selected, budget)
        current = await _accepted_capture(driver, original, budget)
        if original_reader is read_original_generic_capture and current != original:
            await pin_retained_fhir_read(driver, current)
        await _current_organizations(driver, current, selected, budget)
        original_pin = _original_pin(original, coordinates, retained_recipes)
        current_pin = (
            original_pin
            if current == original
            else _capture_pin(
                current,
                original_pin.alias_scope,
                current.origin.semantic_projection_as_of,
            )
        )
        recipes.append(RegistrySourceMembershipRecipe(with_fhir_successor(original_pin, current_pin), coordinates))
        scopes.update(derived)
    return scopes, tuple(recipes)


async def _retained_recipes(driver, source_manifest, control_schema):
    if source_manifest is None:
        return ()
    return await resolve_retained_registry_source_recipes(driver, source_manifest, control_schema=control_schema)


async def resolve_accepted_generic_recipes(driver, bindings, *, read_budget, source_manifest=None, control_schema=None):
    """Fully authenticate original and accepted content for worker execution and readiness changes."""
    retained = await _retained_recipes(driver, source_manifest, control_schema)
    return await _generic_recipes(driver, bindings, read_budget, retained, read_original_generic_capture)


async def observe_accepted_generic_recipes(driver, bindings, *, read_budget, source_manifest=None, control_schema=None):
    """Observe authenticated metadata only; this scheduling result cannot authorize publication."""
    retained = await _retained_recipes(driver, source_manifest, control_schema)
    return await _generic_recipes(driver, bindings, read_budget, retained, read_original_generic_capture_metadata)
