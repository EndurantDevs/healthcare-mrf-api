# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independently derive new source facts before sealing their reusable storage."""

import hashlib
import json
from dataclasses import replace
from uuid import UUID, uuid5

from process.network_address_projection import _identifier
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_membership_composition_stage import (
    drop_registry_command_stage,
    stage_approved_membership_commands,
    verify_registry_composition_rows,
)
from process.registry_retained_generation import resolve_registry_retained_generation
from process.registry_source_contribution_store import (
    RegistrySourceContributionError,
    _inventory_source_metadata,
    registry_source_contribution_receipt,
    verify_registry_source_contributions,
    verify_registry_source_contributions_rows,
)
from process.registry_source_recipe_composition import (
    RegistrySourceRecipeStage,
    SourceRecipeReadContext,
    stage_registry_source_recipes,
)
from process.registry_source_recipe_store import verify_registry_source_recipes
from process.registry_source_retention import verify_registry_source_retention, verify_registry_source_retention_rows
from process.uhc_flex_practitioner_async_safety import drain_operation


async def _drop_stage(connection, stage, commands):
    if commands is not None:
        await drop_registry_command_stage(connection, commands)
    matches = getattr(stage, "office_matches", None)
    for name in (stage.table_name, stage.contribution_table_name, None if matches is None else matches.table_name):
        if name is not None:
            await connection.execute(f"DROP TABLE pg_temp.{_identifier(name)}")


async def _retained_source_stage(connection, copy_target, candidate, control_schema):
    from process.registry_candidate_composition import _retained_memberships

    retained = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
    receipt = verify_registry_source_retention(candidate)
    if receipt is None:
        raise RegistrySourceContributionError("registry_source_contribution_retention_required")
    generation = hashlib.sha256(b"[]").hexdigest() if retained is None else retained[0].manifest_sha256
    name = "registry_source_page_" + uuid5(UUID(copy_target.candidate_id), "independent-retained:" + generation).hex
    table = "pg_temp." + _identifier(name)
    async with connection.transaction():
        if retained is None:
            await connection.execute(f"CREATE TEMP TABLE {table}(ordinal bigint PRIMARY KEY,input_json text NOT NULL)")
        else:
            relation, predicate, original_retention = await _retained_memberships(
                connection, retained[0], control_schema
            )
            if original_retention is None and await connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {relation} WHERE evidence_id !~ '^[0-9a-f]{{64}}$' "
                "AND evidence_id !~ '^approved-custom:[0-9]+:[0-9a-f]{64}$')"
            ):
                raise RegistrySourceContributionError("registry_source_contribution_retained_evidence_unsupported")
            await connection.execute(f"""CREATE TEMP TABLE {table} AS
              SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
                location_id,evidence_id) AS ordinal,to_jsonb(member)::text AS input_json
              FROM {relation} member WHERE {predicate}""")
            await connection.execute(f"ALTER TABLE {table} ADD PRIMARY KEY(ordinal)")
        if await connection.fetchval(f"SELECT count(*) FROM {table}") != receipt["source_rows"]:
            raise RegistrySourceContributionError("registry_source_contribution_retained_rows_differ")
    return RegistrySourceRecipeStage(name, receipt["source_rows"], 0, generation)


def _source_stage_context(office_authority):
    """Preserve office-only private callers while accepting the independent Source context."""
    return (
        office_authority
        if type(office_authority) is SourceRecipeReadContext
        else SourceRecipeReadContext(office_authority)
    )


async def _independent_source_stage(
    connection, copy_target, candidate, approved, *, expected, control_schema, office_authority, inventory=None
):
    """Rebuild selected Source facts with independent runtime, office and predecessor context."""
    read_context = _source_stage_context(office_authority)
    recipes = verify_registry_source_recipes(candidate) if inventory is None else inventory[1]
    if not recipes:
        generations = candidate.get("source_generations", {}) if inventory is not None else {}
        generations = json.loads(generations) if type(generations) is str else generations
        if inventory is None or "registry_source_membership" not in generations:
            return await _retained_source_stage(connection, copy_target, candidate, control_schema)
    if inventory is None and expected is None:
        from process.registry_verified_office_policy import (
            reconstruct_legacy_verified_office_stage,
            select_legacy_verified_office_inputs,
        )

        if (
            await select_legacy_verified_office_inputs(
                connection, candidate, control_schema=control_schema, approved=approved
            )
            is not None
        ):
            return await reconstruct_legacy_verified_office_stage(
                connection,
                candidate,
                approved,
                control_schema=control_schema,
                request_id=uuid5(UUID(copy_target.candidate_id), "independent-legacy-verified-source-v1"),
            )
    previous = None
    verified_inputs = None
    generations = candidate.get("source_generations", {})
    generations = json.loads(generations) if type(generations) is str else generations
    if inventory is not None or "retained_network_source" in generations:
        retained = await resolve_registry_retained_generation(connection, candidate, control_schema=control_schema)
        previous = None if retained is None else retained[0]
    if expected is not None and expected["revision"] == 4:
        from process.registry_initial_source_composition import verify_initial_source_office_receipt
        from process.registry_source_office_custody import _office_inputs

        office_receipt = verify_initial_source_office_receipt(candidate)
        if inventory is None or office_receipt is None or office_receipt["revision"] != 5:
            raise RegistrySourceContributionError("registry_source_contribution_offices_differ")
        verified_inputs = _office_inputs(candidate)
    elif inventory is None and previous is None and candidate["expected_head"]:
        previous = await resolve_network_serving_manifest(
            connection, generation_id=candidate["expected_head"], control_schema=control_schema
        )
    return await stage_registry_source_recipes(
        connection,
        recipes,
        approved,
        request_id=uuid5(UUID(copy_target.candidate_id), "independent-source-contributions-v1"),
        control_schema=control_schema,
        contribution_revision=2 if inventory is not None or expected is None else expected["revision"],
        **{"inventory": True} if inventory is not None else {},
        read_context=replace(read_context, source_manifest=previous, verified_office_inputs=verified_inputs),
    )


async def _verify_inventory_stage(
    connection, copy_target, candidate, approved, stage, inventory, control_schema, office_authority
):
    """Join independently staged facts and native office rows before projection and temporary-stage DROP."""
    from process.network_provider_only_projection import _verify_staged_provider_only_projection
    from process.registry_initial_source_composition import verify_initial_source_office_receipt
    from process.registry_source_inventory_preparation import SourceInventoryPreparation
    from process.registry_source_inventory_summaries import office_summary_v4
    from process.registry_source_office_custody import _office_inputs

    root, recipes, _descriptors = inventory
    preparation = SourceInventoryPreparation(recipes)
    for descriptor in stage.contribution_receipts:
        preparation.add_contribution(descriptor)
    is_witness_stage = getattr(stage, "office_policy_context", None) is not None
    if is_witness_stage:
        from process.registry_source_office_custody import verify_verified_witness_bindings

        for derivation in stage.office_derivations:
            preparation.add_derivation(derivation)
        await verify_verified_witness_bindings(
            connection, copy_target, candidate, approved, stage, control_schema=control_schema
        )
    else:
        await _verify_stage_offices(
            connection,
            copy_target,
            candidate,
            recipes,
            approved,
            (preparation.add_derivation, office_authority, stage.preservation),
            control_schema,
        )
    reference, entries = preparation.finish()
    if reference.root != root:
        raise RegistrySourceContributionError("registry_source_contribution_inventory_differs")
    expected = verify_initial_source_office_receipt(candidate)
    if recipes:
        derivations = sorted(
            (json.loads(entry.office_derivation_json) for entry in entries),
            key=lambda document: document["recipe_sha256"],
        )
        if is_witness_stage:
            from process.registry_source_inventory_summaries import office_summary_v5

            actual = office_summary_v5(root, stage.office_policy_context, derivations)
        else:
            actual = office_summary_v4(
                root, approved, _office_inputs(candidate)[0], derivations, expected["origin_manifest_sha256"]
            )
        if actual != expected:
            raise RegistrySourceContributionError("registry_source_contribution_offices_differ")
    await _verify_staged_provider_only_projection(
        connection, copy_target, recipes, stage, approved, control_schema=control_schema
    )


async def _verify_stage_offices(connection, copy_target, candidate, recipes, approved, options, control_schema):
    from process.registry_source_office_custody import (
        InventoryOfficeCustodyContext,
        _verify_inventory_offices,
        _verify_inventory_source_offices,
    )

    derivation_sink, office_authority, preservation = options
    if preservation is None:
        await _verify_inventory_source_offices(
            connection,
            copy_target,
            candidate,
            recipes,
            approved,
            control_schema=control_schema,
            derivation_sink=derivation_sink,
            office_authority=office_authority,
        )
    else:
        context = InventoryOfficeCustodyContext(recipes, approved, derivation_sink, office_authority, preservation)
        await _verify_inventory_offices(connection, copy_target, candidate, context, control_schema=control_schema)


async def _verify_source_stage(
    connection, copy_target, candidate, stage, expected, inventory, control_schema, inventory_entries
):
    if inventory is None:
        if expected is None:
            return
        if registry_source_contribution_receipt(stage) != expected:
            raise RegistrySourceContributionError("registry_source_contribution_derivation_invalid")
    else:
        actual = registry_source_contribution_receipt(stage, inventory_root=inventory[0])
        if actual != expected and not (expected is None and inventory[0].contribution_count == 0):
            raise RegistrySourceContributionError("registry_source_contribution_derivation_invalid")
    await verify_registry_source_contributions_rows(
        connection,
        copy_target,
        candidate,
        source_stage=stage,
        control_schema=control_schema,
        **({"inventory_entries": inventory_entries} if inventory is not None else {}),
    )


async def _cleanup_independent_source_stage(connection, stage, commands, primary):
    """Drain phase-owned cleanup without replacing its original failure."""
    if getattr(stage, "office_matches", None) is not None:
        from process.registry_verified_office_policy import cleanup_legacy_verified_stage

        await cleanup_legacy_verified_stage(connection, stage, commands, primary=primary)
    else:
        try:
            await drain_operation(_drop_stage(connection, stage, commands), preserve_cancellation=primary is None)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("Independent Source stage cleanup failed; caller rollback remains required.")
            primary._registry_source_cleanup_error = cleanup_error


async def verify_independent_source_contributions(
    connection,
    copy_target,
    candidate,
    *,
    control_schema,
    office_authority=None,
    inventory_entries=None,
    preservation=None,
    source_witness_runtime=None,
):
    """Authenticate current Source facts and mappings before adding independently proved carry."""
    expected = verify_registry_source_contributions(candidate)
    inventory = _inventory_source_metadata(candidate, inventory_entries)
    if inventory is not None and verify_registry_source_retention(candidate) is None:
        raise RegistrySourceContributionError("registry_source_contribution_retention_required")
    if inventory is None and expected is None and verify_registry_source_retention(candidate) is None:
        if preservation is not None:
            raise RegistrySourceContributionError("registry_source_contribution_retention_required")
        return
    approved = await _custody_approved_source(connection, candidate, inventory, expected, control_schema)
    stage = await _independent_source_stage(
        connection,
        copy_target,
        candidate,
        approved,
        expected=expected,
        control_schema=control_schema,
        office_authority=SourceRecipeReadContext(office_authority, source_witness_runtime=source_witness_runtime),
        **({"inventory": inventory} if inventory is not None else {}),
    )
    commands = None
    primary = None
    try:
        if preservation is not None:
            stage = replace(stage, preservation=preservation)
        await _verify_source_stage(
            connection, copy_target, candidate, stage, expected, inventory, control_schema, inventory_entries
        )
        stage = await _append_verified_preservation(
            connection, copy_target, candidate, approved, stage, inventory, control_schema, office_authority
        )
        await verify_registry_source_retention_rows(connection, copy_target, candidate, source_stage=stage)
        commands = await stage_approved_membership_commands(
            connection,
            approved,
            uuid5(UUID(copy_target.candidate_id), "independent-approved-commands-v1"),
            control_schema=control_schema,
        )
        await verify_registry_composition_rows(connection, copy_target, candidate, commands)
        if inventory is not None and preservation is None:
            await _verify_inventory_stage(
                connection, copy_target, candidate, approved, stage, inventory, control_schema, office_authority
            )
        return preservation
    except BaseException as error:
        primary = error
        raise
    finally:
        await _cleanup_independent_source_stage(connection, stage, commands, primary)


async def _custody_approved_source(connection, candidate, inventory, expected, control_schema):
    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
    )
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    if (
        inventory is not None
        or (expected is not None and expected["revision"] == 2)
        or "custom_membership" in generations
    ) and generations.get("custom_membership") != approved.generation_id:
        raise RegistrySourceContributionError("registry_source_contribution_approved_scope_changed")
    return approved


async def _append_verified_preservation(
    connection, copy_target, candidate, approved, stage, inventory, control_schema, office_authority
):
    if stage.preservation is None:
        return stage
    from process.registry_fhir_serving_preservation import append_fhir_preservation_memberships

    if inventory is not None:
        await _verify_inventory_stage(
            connection, copy_target, candidate, approved, stage, inventory, control_schema, office_authority
        )
    combined = await append_fhir_preservation_memberships(connection, stage, stage.preservation)
    if inventory is None:
        from process.registry_source_office_custody import verify_independent_initial_source_offices

        await verify_independent_initial_source_offices(
            connection,
            copy_target,
            candidate,
            control_schema=control_schema,
            office_authority=office_authority,
            preservation=stage.preservation,
            source_stage=combined,
        )
    return combined
