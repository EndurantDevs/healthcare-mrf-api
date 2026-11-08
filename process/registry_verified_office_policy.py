# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize and reconstruct the exact registered legacy verified-office policy."""

import hashlib
import json

from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, _generation_id
from process.registry_initial_source_composition import (
    RegistryInitialSourceError,
    verified_retained_source_office_summary,
    verify_initial_source_office_receipt,
)
from process.registry_source_contribution_store import verify_registry_source_contributions
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from process.registry_source_recipe_composition import (
    SourceRecipeReadContext,
    _source_stage_generation,
    stage_registry_source_recipes,
)
from process.registry_source_recipe_store import verify_registry_source_recipes
from process.registry_source_retention import verify_registry_source_retention
from process.registry_source_selection_receipt import (
    _generations,
    build_registry_source_selection_receipt,
    verify_registry_source_selection_receipt,
)
from process.uhc_flex_practitioner_async_safety import drain_operation


async def cleanup_legacy_verified_stage(connection, stage, commands=None, *, primary=None):
    """Drain exact temporary cleanup without replacing an existing failure."""
    from process.network_source_contribution_custody import _drop_stage

    try:
        await drain_operation(_drop_stage(connection, stage, commands), preserve_cancellation=primary is None)
    except BaseException as cleanup_error:
        if primary is None:
            raise
        primary.add_note("Legacy verified office cleanup failed; caller rollback remains required.")
        primary._legacy_verified_cleanup_error = cleanup_error


def _verified_stage_commitments(recipes, approved, summary, selection):
    """Recompute the existing raw and mapping-bound generation encodings exactly."""
    raw_generations = tuple(
        _generation_id(recipe.source_pin, approved, recipe.binding_coordinates, True) for recipe in recipes
    )
    mapped_generations = tuple(
        hashlib.sha256(json.dumps([raw, summary["correspondence_sha256"]], separators=(",", ":")).encode()).hexdigest()
        for raw in raw_generations
    )
    stage_generation = _source_stage_generation(
        selection["recipe_sha256"],
        approved,
        mapped_generations,
        selection["mapped_rows"],
        selection["omitted_rows"],
    )
    return raw_generations, stage_generation


def _has_matching_retention(candidate, retention, generation, rows):
    """Pair current commitments with separately closed carry; native rederivation remains mandatory."""
    from process.registry_fhir_serving_preservation import (
        fhir_serving_preservation_sha256,
        verify_fhir_serving_preservation,
    )

    preservation = verify_fhir_serving_preservation(candidate)
    if preservation is not None:
        generation = hashlib.sha256(
            json.dumps([generation, fhir_serving_preservation_sha256(preservation)], separators=(",", ":")).encode()
        ).hexdigest()
        rows += preservation["carried_rows"]
    return generation == retention["source_generation_sha256"] and rows == retention["source_rows"]


async def select_legacy_verified_office_inputs(connection, candidate, *, control_schema=None, approved=None):
    """Select metadata by exact commitments; actual SOURCE/EAU authority is still required at use."""
    summary = verify_initial_source_office_receipt(candidate)
    if summary is None or summary["revision"] != 1:
        return None
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    generations = _generations(candidate)
    if COMPACT_FORMAT_KEY in generations:
        if type(generations[COMPACT_FORMAT_KEY]) is not str or generations[COMPACT_FORMAT_KEY] != COMPACT_FORMAT:
            raise RegistryInitialSourceError("registry_legacy_verified_office_format_unknown")
        return None
    if verify_source_recipe_metadata(candidate).inventory is not None:
        return None
    if verify_registry_source_contributions(candidate) is not None:
        return None
    recipes = verify_registry_source_recipes(candidate)
    if not recipes or not all(
        type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.retained_capture is not None
        for recipe in recipes
    ):
        return None
    selection = verify_registry_source_selection_receipt(candidate)
    retention = verify_registry_source_retention(candidate)
    if any(receipt is None or receipt["revision"] != 1 for receipt in (summary, selection, retention)):
        return None
    native_approved = await pin_retained_approved_membership_source(
        connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
    )
    if approved is not None and approved != native_approved:
        raise RegistryInitialSourceError("registry_legacy_verified_office_approved_differs")
    if selection["approved_generation_sha256"] != native_approved.generation_id:
        raise RegistryInitialSourceError("registry_legacy_verified_office_approved_differs")
    if native_approved.total_rows:
        return None
    raw_generations, stage_generation = _verified_stage_commitments(recipes, native_approved, summary, selection)
    if stage_generation != selection["stage_generation_sha256"] or not _has_matching_retention(
        candidate, retention, stage_generation, selection["mapped_rows"]
    ):
        return None
    if (
        retention["add_rows"]
        or retention["remove_rows"]
        or retention["suppressed_source_rows"]
        or any(generations.get(f"initial_source_recipe_{index}") != raw for index, raw in enumerate(raw_generations))
    ):
        raise RegistryInitialSourceError("registry_legacy_verified_office_commitment_differs")
    from process.registry_source_office_custody import _office_inputs

    inputs = _office_inputs(candidate)
    if inputs[0].generation_id != summary["address_generation_sha256"]:
        raise RegistryInitialSourceError("registry_legacy_verified_office_address_differs")
    return inputs


async def reconstruct_legacy_verified_office_stage(connection, candidate, approved, *, control_schema, request_id):
    """Freshly authenticate SOURCE/EAU and reproduce the complete registered filtered stage."""
    inputs = await select_legacy_verified_office_inputs(
        connection, candidate, control_schema=control_schema, approved=approved
    )
    if inputs is None:
        raise RegistryInitialSourceError("registry_legacy_verified_office_policy_differs")
    recipes = verify_registry_source_recipes(candidate)
    stage = await stage_registry_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        contribution_revision=1,
        read_context=SourceRecipeReadContext(retained_fhir_reads={}, verified_office_inputs=inputs),
    )
    try:
        actual = await verified_retained_source_office_summary(
            connection, recipes, approved, *inputs, control_schema, stage.office_matches
        )
        if actual != verify_initial_source_office_receipt(candidate) or build_registry_source_selection_receipt(
            recipes, approved, stage
        ) != verify_registry_source_selection_receipt(candidate):
            raise RegistryInitialSourceError("registry_legacy_verified_office_reconstruction_differs")
        retention = verify_registry_source_retention(candidate)
        if not _has_matching_retention(candidate, retention, stage.generation_sha256, stage.membership_rows):
            raise RegistryInitialSourceError("registry_legacy_verified_office_reconstruction_differs")
    except BaseException as primary:
        await cleanup_legacy_verified_stage(connection, stage, primary=primary)
        raise
    return stage
