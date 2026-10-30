# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recompose retained source memberships with the current approved custom map."""

import hashlib
import json
from dataclasses import dataclass, replace
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import (
    pin_approved_membership_source,
)
from process.network_custom_address_source import verify_custom_address_source as verify_custom_address_source
from process.network_em_provider_inputs import (
    EMProviderInputs,
    prepare_em_provider_inputs,
    record_em_provider_inputs,
    resolve_em_provider_inputs,
)
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_membership_candidate_lifecycle import (
    _locked_candidate,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import _retain_closure
from process.network_membership_writer_closure import (
    freeze_network_candidate_writers,
    verify_network_candidate_writer_closure,
)
from process.network_serving_read import (
    PinnedNetworkServingManifest,
    resolve_network_serving_manifest,
)
from process.registry_compact_composition import CompactCompositionAddressSource
from process.registry_company_approval_fence import (
    _company_approval_connection,
    _company_approval_snapshot,
    require_registry_company_approval_fence,
)
from process.registry_composition_addresses import (
    RegistryAddressArtifactCustody as RegistryAddressArtifactCustody,
)
from process.registry_composition_addresses import (
    RegistryCompositionAddressSources as RegistryCompositionAddressSources,
)
from process.registry_composition_addresses import (
    RegistryCompositionError as RegistryCompositionError,
)
from process.registry_composition_addresses import (
    RegistryRetainedAddressArtifact as RegistryRetainedAddressArtifact,
)
from process.registry_composition_addresses import (
    _address_arguments as _address_arguments,
)
from process.registry_composition_addresses import (
    _composition_artifact as _composition_artifact,
)
from process.registry_composition_addresses import (
    _composition_artifact_with_reads as _composition_artifact_with_reads,
)
from process.registry_composition_addresses import (
    _composition_office_summary as _composition_office_summary,
)
from process.registry_composition_addresses import (
    _inventory_office_summary as _inventory_office_summary,
)
from process.registry_composition_addresses import (
    _prepare_composition_artifact as _prepare_composition_artifact,
)
from process.registry_composition_addresses import (
    _prepare_inventory_artifact as _prepare_inventory_address_artifact,
)
from process.registry_composition_addresses import (
    _prepare_preservation as _prepare_preservation,
)
from process.registry_composition_addresses import (
    _prepare_registry_composition_address_artifact,
)
from process.registry_composition_addresses import (
    _prepared_address_artifact as _prepared_address_artifact,
)
from process.registry_composition_addresses import (
    _require_source_recipe_phase as _require_source_recipe_phase,
)
from process.registry_composition_addresses import (
    _retained_address_artifact as _retained_address_artifact,
)
from process.registry_composition_addresses import (
    _retained_fhir_only as _retained_fhir_only,
)
from process.registry_composition_addresses import (
    _verified_office_inputs as _verified_office_inputs,
)
from process.registry_composition_addresses import (
    _verify_compact_preparation_origin as _verify_compact_preparation_origin,
)
from process.registry_composition_addresses import (
    _verify_retained_address_sources as _verify_retained_address_sources,
)
from process.registry_composition_resume import (
    require_registered_composition_target,
    resolve_registered_compact_format,
    resolve_registered_contribution_resume,
    resolve_registered_legacy_mapping_marker,
    resolve_registered_legacy_office_inputs,
    resolve_registered_office_summary_revision,
    resolve_registered_source_inventory,
)
from process.registry_initial_source_composition import (
    SUMMARY_KEY,
    OfficeSummaryRequest,
    initial_source_office_receipt_sha256,
    record_initial_source_offices_with_reads,
    record_source_offices,
    verify_initial_source_office_receipt,
)
from process.registry_membership_composition_stage import (
    build_registry_source_retention,
    materialize_registry_composition,
    stage_approved_membership_commands,
)
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_composition import (
    OFFICE_MAPPING_KEY,
    RegistrySourceRecipeStage,
    SourceRecipeReadContext,
    stage_registry_source_recipes,
)
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)
from process.registry_source_retention import (
    SOURCE_TABLE,
    registry_source_retention_sha256,
    retain_registry_source_memberships,
    verify_registry_source_retention,
)
from process.registry_source_retention import (
    SUMMARY_KEY as RETENTION_KEY,
)


async def _retained_memberships(connection, source_manifest, control_schema):
    candidate = await connection.fetchrow(
        f"SELECT * FROM {_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(source_manifest.candidate_id),
    )
    retention = verify_registry_source_retention(candidate)
    namespace = _identifier(source_manifest.schema_name)
    if retention is not None:
        return f"{namespace}.{SOURCE_TABLE}", "TRUE", retention
    return f"{namespace}.network_membership", "evidence_id ~ '^[0-9a-f]{64}$'", None


async def _retained_source(connection, source_manifest, control_schema):
    if source_manifest is None:
        return {}, 0
    if type(source_manifest) is not PinnedNetworkServingManifest:
        raise RegistryCompositionError("registry_composition_source_invalid")
    actual = await resolve_network_serving_manifest(
        connection,
        generation_id=source_manifest.generation_id,
        control_schema=control_schema,
    )
    if actual != source_manifest:
        raise RegistryCompositionError("registry_composition_source_changed")
    relation, predicate, retention = await _retained_memberships(connection, source_manifest, control_schema)
    summary = await connection.fetchrow(
        f"SELECT count(*) FILTER(WHERE {predicate}) AS source_rows,"
        "count(*) FILTER(WHERE evidence_id !~ '^[0-9a-f]{64}$' "
        "AND evidence_id !~ '^approved-custom:[0-9]+:[0-9a-f]{64}$') AS unknown_rows "
        f"FROM {relation}"
    )
    if retention is None and summary["unknown_rows"]:
        raise RegistryCompositionError("registry_composition_source_evidence_unsupported")
    if retention is not None and summary["source_rows"] != retention["source_rows"]:
        raise RegistryCompositionError("registry_composition_source_accounting_invalid")
    generations_by_source = {
        key: generation
        for key, generation in source_manifest.source_generations.items()
        if key
        not in {
            "custom_membership",
            "unified_address",
            "retained_network_source",
            "em_provider_inputs",
            "registry_fhir_serving_preservation",
            "registry_source_rollback",
            OFFICE_MAPPING_KEY,
            SUMMARY_KEY,
            RETENTION_KEY,
            "registry_source_contributions",
            INVENTORY_DIGEST_KEY,
        }
        and not key.startswith("initial_source_recipe_")
    }
    generations_by_source["retained_network_source"] = source_manifest.manifest_sha256
    return generations_by_source, summary["source_rows"]


def _target(
    request_id,
    approved_revision,
    expected_head,
    generations_by_source,
    initial_summary=None,
    *,
    inventory_root=None,
):
    if not isinstance(request_id, UUID) or not request_id.int:
        raise RegistryCompositionError("registry_composition_request_invalid")
    if initial_summary is not None:
        generations_by_source[SUMMARY_KEY] = initial_source_office_receipt_sha256(
            initial_summary, inventory_root=inventory_root
        )
    recipe = json.dumps(
        [approved_revision, expected_head, generations_by_source],
        sort_keys=True,
        separators=(",", ":"),
    )
    candidate_id = uuid5(request_id, "registry-composition:" + recipe)
    return MembershipCopyTarget(
        str(uuid5(request_id, "dataset")),
        str(uuid5(request_id, "schema")),
        str(uuid5(request_id, "producer")),
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )


async def _bindings(
    connection,
    copy_target,
    artifact,
    source_manifest,
    control_schema,
    *,
    retain_source_bindings=True,
    source_stage=None,
):
    namespace = _identifier(copy_target.schema_name)
    custom_namespace = _identifier(artifact.address_source.schema_name)
    await connection.execute(f"""CREATE TABLE IF NOT EXISTS {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    binding_relation = f"{custom_namespace}.provider_location_binding"
    bindings = None
    has_verified_mapping = source_stage is not None and source_stage.office_matches is not None
    if (
        not has_verified_mapping
        and type(artifact) is not CompactCompositionAddressSource
        and await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", binding_relation)
    ):
        bindings = f"SELECT * FROM {binding_relation}"
    if source_manifest is not None and retain_source_bindings:
        retained = _identifier(source_manifest.schema_name)
        relation, predicate, _ = await _retained_memberships(connection, source_manifest, control_schema)
        retained_bindings = f"""SELECT binding.* FROM {retained}.provider_location_binding binding
          WHERE EXISTS(SELECT 1 FROM {relation} member
            WHERE {predicate}
              AND (member.provider_system,member.provider_id,member.location_id)=
                  (binding.provider_system,binding.provider_id,binding.location_id))"""
        bindings = retained_bindings if bindings is None else bindings + " UNION " + retained_bindings
    preservation = None if source_stage is None else source_stage.preservation
    if preservation is not None:
        bindings = preservation.binding_query if bindings is None else bindings + " UNION " + preservation.binding_query
    if bindings is None:
        return
    await connection.execute(f"INSERT INTO {namespace}.provider_location_binding {bindings} ON CONFLICT DO NOTHING")
    if await connection.fetchval(f"""WITH expected AS ({bindings})
      SELECT EXISTS(SELECT 1 FROM expected LEFT JOIN {namespace}.provider_location_binding actual
        USING(provider_system,provider_id,location_id)
        WHERE (expected.location_key,expected.entity_type,expected.entity_id)
          IS DISTINCT FROM (actual.location_key,actual.entity_type,actual.entity_id))"""):
        raise RegistryCompositionError("registry_composition_binding_conflict")


async def _stage_source(connection, source_manifest, request_id, expected_rows, control_schema):
    await _retained_source(connection, source_manifest, control_schema)
    generation = source_manifest.manifest_sha256 if source_manifest else hashlib.sha256(b"[]").hexdigest()
    stage_name = "registry_source_page_" + uuid5(request_id, "retained-source:" + generation).hex
    stage = _identifier(stage_name)
    if source_manifest is None:
        await connection.execute(f"CREATE TEMP TABLE {stage}(ordinal bigint PRIMARY KEY,input_json text NOT NULL)")
    else:
        relation, predicate, _ = await _retained_memberships(connection, source_manifest, control_schema)
        await connection.execute(f"""CREATE TEMP TABLE {stage} AS
          SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
            location_id,evidence_id) AS ordinal,to_jsonb(member)::text AS input_json
          FROM {relation} member WHERE {predicate}""")
        await connection.execute(f"ALTER TABLE pg_temp.{stage} ADD PRIMARY KEY(ordinal)")
    if await connection.fetchval(f"SELECT count(*) FROM pg_temp.{stage}") != expected_rows:
        raise RegistryCompositionError("registry_composition_source_accounting_invalid")
    return RegistrySourceRecipeStage(stage_name, expected_rows, 0, generation)


async def _copy_composition(connection, copy_target, control_schema, source_stage, command_stage, writer_roles):
    from process.network_membership_candidate_lifecycle import _locked_candidate
    from process.registry_source_contribution_store import (
        retain_registry_source_contributions,
    )
    from process.registry_source_inventory_store import _registered_inventory_entries

    async with connection.transaction(isolation="repeatable_read"):
        candidate = await _locked_candidate(connection, copy_target, _namespace(control_schema))
        entries = await _registered_inventory_entries(connection, copy_target, candidate, control_schema=control_schema)
        inventory_options = {} if entries is None else {"inventory_entries": entries}
        await retain_registry_source_contributions(
            connection,
            copy_target,
            source_stage,
            control_schema=control_schema,
            **inventory_options,
        )
        await retain_registry_source_memberships(connection, copy_target, source_stage, control_schema=control_schema)
        await materialize_registry_composition(connection, copy_target, command_stage, control_schema=control_schema)
        if entries is not None:
            await _prepare_provider_only_rows(
                connection,
                copy_target,
                candidate,
                entries,
                source_stage,
                control_schema,
            )
        await seal_network_candidate(
            connection,
            copy_target,
            control_schema=control_schema,
            source_stage=source_stage,
            command_stage=command_stage,
            **inventory_options,
        )
        closure = await freeze_network_candidate_writers(
            connection, copy_target, **writer_roles, control_schema=control_schema
        )
        await _retain_closure(connection, copy_target, _namespace(control_schema)[1:-1], closure)


async def _prepare_provider_only_rows(connection, target, candidate, entries, stage, control_schema):
    from process.network_approved_membership_source import (
        pin_retained_approved_membership_source,
    )
    from process.network_provider_only_projection import (
        prepare_provider_only_projection,
    )
    from process.registry_source_contribution_store import _inventory_source_metadata

    _root, recipes, _descriptors = _inventory_source_metadata(candidate, entries)
    approved = await pin_retained_approved_membership_source(
        connection,
        approved_revision=candidate["approved_custom_revision"],
        control_schema=control_schema,
    )
    await prepare_provider_only_projection(connection, target, recipes, stage, approved, control_schema=control_schema)


async def _require_introduced_offices(
    connection,
    recipe_sets,
    approved,
    control_schema,
    office_authority,
    *,
    inventory=False,
):
    from process.registry_ptg_office_selection import (
        require_selected_ptg_office_recipes,
    )

    retained, recipes = recipe_sets
    retained_identities = {canonical_registry_source_recipes((recipe,)) for recipe in retained}
    new_recipes = tuple(
        recipe for recipe in recipes if canonical_registry_source_recipes((recipe,)) not in retained_identities
    )
    await require_selected_ptg_office_recipes(
        connection,
        new_recipes,
        approved,
        control_schema=control_schema,
        office_authority=office_authority,
        candidate_recipes=recipes,
        **({"inventory": True} if inventory else {}),
    )


async def _selected_source_recipes(connection, retained, approved, source_manifest, options, *, inventory=False):
    """Historical source rollback retains its source; ordinary publication follows reviewed binding deltas."""
    control_schema, office_authority, expected_head, request_id = options
    historical = None
    if source_manifest is not None and request_id is not None:
        from process.registry_publication_queue import require_registry_source_rollback_composition

        historical = await require_registry_source_rollback_composition(
            connection,
            request_id,
            source_manifest,
            approved.approved_revision,
            expected_head,
            control_schema=control_schema,
        )
    if historical is not None:
        return retained
    if source_manifest is not None and expected_head is not None and source_manifest.generation_id != expected_head:
        raise RegistryCompositionError("registry_source_rollback_operation_required")
    from process.registry_ptg_office_selection import resolve_ptg_office_recipe_selection

    return await resolve_ptg_office_recipe_selection(
        connection,
        retained,
        approved.approved_revision,
        source_manifest,
        control_schema=control_schema,
        office_authority=office_authority,
        **({"inventory": True} if inventory else {}),
    )


async def _composition_sources(
    connection,
    source_manifest,
    approved,
    request_id,
    control_schema,
    initial_recipes=(),
    office_authority=None,
    expected_head=None,
):
    """Keep standalone source selection's original authority and head arguments."""
    return await _composition_sources_with_reads(
        connection,
        source_manifest,
        approved,
        request_id,
        control_schema,
        initial_recipes,
        (office_authority, None),
        expected_head,
    )


async def _selected_composition_sources(
    connection,
    source_manifest,
    approved,
    control_schema,
    office_authority,
    initial_recipes,
    request_scope,
    inventory,
):
    expected_head, request_id = request_scope
    from process.registry_ptg_office_selection import (
        require_ptg_startup_selection,
    )

    retained = (
        await resolve_retained_registry_source_recipes(connection, source_manifest, control_schema=control_schema)
        if source_manifest is not None and RECIPE_DIGEST_KEY in source_manifest.source_generations
        else ()
    )
    selected = await _selected_source_recipes(
        connection,
        retained,
        approved,
        source_manifest,
        (control_schema, office_authority, expected_head, request_id),
        inventory=inventory,
    )
    if initial_recipes:
        require_ptg_startup_selection(initial_recipes, selected, **({"inventory": True} if inventory else {}))
    recipes = initial_recipes or selected
    identity = (
        (lambda source_recipes: SourceInventoryPreparation(source_recipes).recipe_root.recipes_sha256)
        if inventory
        else canonical_registry_source_recipes
    )
    has_selection_changed = identity(recipes) != identity(retained)
    return retained, recipes, has_selection_changed


async def _composition_sources_for_format(
    inputs, request_id, control_schema, initial_recipes=(), expected_head=None, *, inventory=False
):
    """Keep standalone source-selection callers on the original four-value protocol."""
    metadata, _artifact = await _prepare_composition_sources_for_format(
        inputs, request_id, control_schema, initial_recipes, expected_head, inventory=inventory
    )
    return metadata


async def _prepare_composition_sources_for_format(
    inputs,
    request_id,
    control_schema,
    initial_recipes=(),
    expected_head=None,
    *,
    inventory=False,
    artifact_inputs=None,
):
    """Keep registered legacy identity while bounded inventories use constant commitments."""
    connection, source_manifest, approved, office_authority = (
        inputs.connection,
        inputs.source_manifest,
        inputs.approved,
        inputs.office_context,
    )
    generations_by_source, source_rows = await _retained_source(connection, source_manifest, control_schema)
    retained, recipes, has_selection_changed = await _selected_composition_sources(
        connection,
        source_manifest,
        approved,
        control_schema,
        office_authority,
        initial_recipes,
        (expected_head, request_id),
        inventory,
    )
    if (
        not recipes
        and not has_selection_changed
        and (source_manifest is None or RECIPE_DIGEST_KEY not in source_manifest.source_generations)
    ):
        source_stage = await _stage_source(connection, source_manifest, request_id, source_rows, control_schema)
        if inventory:
            generations_by_source.pop("registry_source_membership", None)
        return (
            generations_by_source,
            source_rows,
            (),
            replace(source_stage, inventory=inventory),
        ), None
    return await _prepare_selected_source_stage(
        inputs,
        (request_id, expected_head),
        control_schema,
        (retained, recipes, has_selection_changed),
        initial_recipes,
        inventory,
        artifact_inputs,
        generations_by_source,
    )


def _bind_composition_stage(generations, stage, introduced, changed, inventory):
    generations.pop(OFFICE_MAPPING_KEY, None)
    if introduced or changed:
        generations.pop(RECIPE_DIGEST_KEY, None)
    generations["registry_source_membership"] = stage.generation_sha256
    if (introduced or stage.office_matches is not None) and not inventory:
        generations.update(
            {
                f"initial_source_recipe_{index}": generation
                for index, generation in enumerate(stage.raw_source_generations or stage.source_generations)
            }
        )


async def _contribution_revision(connection, recipes, approved, request_id, expected_head, control_schema):
    if not any(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in recipes):
        return 2
    registered = await resolve_registered_contribution_resume(
        connection,
        request_id,
        approved.approved_revision,
        expected_head,
        control_schema=control_schema,
    )
    return 2 if registered is None else registered.contribution_revision


async def _record_source_receipts(
    connection,
    copy_target,
    recipes,
    approved,
    source_stage,
    artifact,
    office_phase,
    control_schema,
):
    """Bind source selection and office receipts to the same open candidate."""
    initial_summary, office_authority, retained_fhir_reads, source_manifest = office_phase
    verified_matches = source_stage.office_matches
    await _record_stage_preservation(connection, copy_target, source_stage, control_schema)
    if recipes:
        from process.registry_source_selection_receipt import (
            record_registry_source_selection,
        )

        await record_registry_source_selection(
            connection,
            copy_target,
            recipes,
            approved,
            source_stage,
            control_schema=control_schema,
        )
    if initial_summary is not None and initial_summary["revision"] == 5:
        await _record_witness_offices(
            connection, copy_target, recipes, approved, artifact, source_stage, control_schema
        )
    elif initial_summary is not None and initial_summary["revision"] == 1:
        await record_initial_source_offices_with_reads(
            connection,
            copy_target,
            recipes,
            approved,
            artifact.address_source,
            runtime_roles=artifact.runtime_roles,
            control_schema=control_schema,
            office_phase=(office_authority, retained_fhir_reads, verified_matches),
        )
    elif initial_summary is not None:
        await record_source_offices(
            connection,
            copy_target,
            recipes,
            artifact.address_source,
            OfficeSummaryRequest(
                artifact.runtime_roles,
                control_schema,
                office_authority,
                initial_summary.get("origin_manifest_sha256"),
                source_manifest,
                initial_summary["revision"],
            ),
        )


async def _record_witness_offices(connection, copy_target, recipes, approved, artifact, stage, control_schema):
    """The explicit v5 recorder owns copying the already authenticated match set."""
    from process.registry_initial_source_composition import record_verified_witness_offices

    await record_verified_witness_offices(
        connection,
        copy_target,
        recipes,
        approved,
        artifact.address_source,
        artifact.runtime_roles,
        stage.selected_stage if stage.selected_stage is not None else stage,
        control_schema=control_schema,
    )


async def _prepare_composition(
    connection,
    request_id,
    approved_revision,
    expected_head,
    address_sources,
    source_manifest,
    writer_roles,
    control_schema,
):
    """Keep existing raw callers' repeatable-read transaction boundary."""
    async with connection.transaction(isolation="repeatable_read"):
        return await _prepare_composition_snapshot(
            connection,
            request_id,
            approved_revision,
            expected_head,
            address_sources,
            source_manifest,
            writer_roles,
            control_schema,
        )


async def _preparation_driver(connection, address_sources, control_schema):
    if type(connection) is not AsyncSession:
        return connection, None
    await require_registry_company_approval_fence(connection, _namespace(control_schema)[1:-1])
    driver = (await (await connection.connection()).get_raw_connection()).driver_connection
    if not driver.is_in_transaction():
        raise RegistryCompositionError("registry_composition_requires_phase_owned_transactions")
    return driver, (
        connection,
        address_sources.office_scope_store,
        address_sources.office_read_budget,
    )


async def _composition_retention(connection, approved, request_id, source_stage, control_schema):
    command_stage = await stage_approved_membership_commands(
        connection, approved, request_id=request_id, control_schema=control_schema
    )
    return command_stage, await build_registry_source_retention(connection, source_stage, command_stage)


async def _composition_candidate(
    connection,
    request_id,
    copy_target,
    generations_by_source,
    approved_revision,
    expected_head,
    source_metadata,
    control_schema,
):
    retention, recipes, provider_receipt, initial_summary, contributions = source_metadata[:5]
    inventory = source_metadata[5] if len(source_metadata) == 6 else None
    await require_registered_composition_target(
        connection,
        request_id,
        approved_revision,
        expected_head,
        copy_target,
        control_schema=control_schema,
    )
    candidate = await create_network_candidate(
        connection,
        copy_target,
        source_generations=generations_by_source,
        approved_custom_revision=approved_revision,
        expected_head=expected_head,
        expected_rows=retention["effective_rows"],
        control_schema=control_schema,
        source_recipes=recipes if inventory is None else (),
        **({"source_inventory": inventory} if inventory is not None else {}),
        source_retention=retention,
        source_contributions=contributions,
    )
    _require_retained_em_provider_receipt(candidate, provider_receipt)
    if (
        candidate["state"] != "open"
        and initial_summary is not None
        and verify_initial_source_office_receipt(candidate) != initial_summary
    ):
        raise RegistryCompositionError("registry_composition_initial_sources_changed")
    if candidate["state"] == "open" and provider_receipt is not None:
        await record_em_provider_inputs(connection, copy_target, provider_receipt, control_schema)
    return candidate


def _bind_prepared_generations(
    generations_by_source,
    approved,
    artifact,
    provider_receipt,
    source_stage,
    inventory=None,
    *,
    retention=None,
):
    """Bind current custom and prepared dependency proofs before deriving candidate identity."""
    if source_stage is None and inventory is not None:
        raise RegistryCompositionError("registry_composition_source_stage_required")
    if retention is not None:
        generations_by_source[RETENTION_KEY] = registry_source_retention_sha256(retention)
    generations_by_source.update(
        custom_membership=approved.generation_id,
        unified_address=artifact.generation_sha256,
    )
    if type(artifact) is RegistryRetainedAddressArtifact and artifact.original_source != artifact.address_source:
        generations_by_source["retained_unified_address"] = artifact.original_source.generation_id
    if provider_receipt is not None:
        generations_by_source["em_provider_inputs"] = provider_receipt["receipt_sha256"]
    if type(artifact) is CompactCompositionAddressSource:
        from process.registry_compact_composition import (
            COMPACT_FORMAT,
            COMPACT_FORMAT_KEY,
        )

        generations_by_source.update(
            {
                COMPACT_FORMAT_KEY: COMPACT_FORMAT,
                "network_address_origin": artifact.address_artifact_sha256,
            }
        )
    if inventory is not None:
        generations_by_source[INVENTORY_DIGEST_KEY] = inventory.root.entries_sha256
    elif source_stage is not None and source_stage.contribution_revision == 2:
        generations_by_source.update(
            {
                f"initial_source_recipe_{index}": generation
                for index, generation in enumerate(
                    source_stage.raw_source_generations or source_stage.source_generations
                )
            }
        )
    return None if source_stage is None else _bind_source_contributions(generations_by_source, source_stage, inventory)


@dataclass(frozen=True)
class _CompositionInputs:
    connection: object
    office_context: object
    approved: object
    provider_receipt: dict | None
    source_manifest: PinnedNetworkServingManifest | None
    retained_fhir_reads: dict | None = None
    verified_office_inputs: tuple | None = None
    compact: bool = False
    source_operation: dict | None = None


async def _composition_inputs(connection, address_sources, source_manifest, writer_roles, revision, control_schema):
    connection, office_context = await _preparation_driver(connection, address_sources, control_schema)
    provider_receipt = await _em_provider_receipt(
        connection, address_sources, source_manifest, writer_roles, control_schema
    )
    approved = await pin_approved_membership_source(
        connection, approved_revision=revision, control_schema=control_schema
    )
    return _CompositionInputs(
        connection,
        office_context,
        approved,
        provider_receipt,
        source_manifest,
        {},
        _verified_office_inputs(address_sources, writer_roles),
        address_sources.compact,
    )


async def _prepare_composition_snapshot(
    connection,
    request_id,
    approved_revision,
    expected_head,
    address_sources,
    source_manifest,
    writer_roles,
    control_schema,
):
    """Prepare exact sources and register their candidate within the same snapshot."""
    inputs = await _composition_inputs(
        connection,
        address_sources,
        source_manifest,
        writer_roles,
        approved_revision,
        control_schema,
    )
    inputs = replace(
        inputs,
        source_operation=await _source_operation(
            inputs.connection,
            request_id,
            source_manifest,
            (approved_revision, expected_head, control_schema),
        ),
    )
    inputs, inventory = await _registered_composition_inputs(
        inputs, (request_id, approved_revision, expected_head), address_sources, control_schema
    )
    source_metadata, phase_artifact = await _prepare_composition_sources_for_format(
        inputs,
        request_id,
        control_schema,
        address_sources.introduced_source_recipes,
        expected_head,
        inventory=inventory,
        artifact_inputs=(address_sources, writer_roles),
    )
    generations_by_source, _, recipes, source_stage = source_metadata
    command_stage, retention = await _composition_retention(
        inputs.connection, inputs.approved, request_id, source_stage, control_schema
    )
    generations_by_source[RETENTION_KEY] = registry_source_retention_sha256(retention)
    return await _register_composition_snapshot(
        inputs,
        request_id,
        expected_head,
        address_sources,
        writer_roles,
        (generations_by_source, recipes, source_stage, command_stage, retention),
        control_schema,
        phase_artifact=phase_artifact,
    )


async def _prepare_inventory_artifact(
    inputs,
    request_id,
    expected_head,
    address_sources,
    writer_roles,
    recipes_and_stage,
    control_schema,
    *,
    phase_artifact=None,
):
    """Complete SOURCE and office metadata before its immutable candidate identity is registered."""
    recipes, source_stage = recipes_and_stage
    office_revision = await resolve_registered_office_summary_revision(
        inputs.connection,
        request_id,
        inputs.approved.approved_revision,
        expected_head,
        control_schema=control_schema,
    )
    return await _prepare_inventory_address_artifact(
        inputs,
        (request_id, expected_head),
        address_sources,
        writer_roles,
        (recipes, source_stage),
        control_schema,
        office_phase=(phase_artifact, office_revision),
        artifact_builder=_composition_artifact,
    )


async def _register_composition_snapshot(
    inputs,
    request_id,
    expected_head,
    address_sources,
    writer_roles,
    source_metadata,
    control_schema,
    *,
    phase_artifact=None,
):
    """Bind all prepared generations before registering or copying any candidate rows."""
    generations_by_source, recipes, source_stage, command_stage, retention = source_metadata
    artifact, initial_summary, inventory, entries = await _prepare_inventory_artifact(
        inputs,
        request_id,
        expected_head,
        address_sources,
        writer_roles,
        (recipes, source_stage),
        control_schema,
        phase_artifact=phase_artifact,
    )
    copy_target, candidate = await _register_prepared_candidate(
        inputs,
        (request_id, expected_head),
        generations_by_source,
        artifact,
        initial_summary,
        (recipes, source_stage, retention, inventory),
        control_schema,
    )
    if candidate["state"] == "open":
        await _record_source_operation(inputs.connection, copy_target, inputs.source_operation, control_schema)
        await _record_prepared_composition(
            inputs,
            copy_target,
            (recipes, source_stage, artifact, initial_summary, inventory, entries),
            control_schema,
        )
    return (
        copy_target,
        artifact.address_source,
        candidate["state"],
        source_stage,
        command_stage,
        writer_roles,
    )


async def _record_prepared_composition(inputs, copy_target, prepared_sources, control_schema):
    recipes, source_stage, artifact, initial_summary, inventory, entries = prepared_sources
    if initial_summary is not None and initial_summary["revision"] == 1:
        await _record_composition_address(inputs.connection, copy_target, artifact.address_source, control_schema)
    if inventory is not None:
        from process.registry_source_inventory_store import write_source_inventory

        await write_source_inventory(
            inputs.connection,
            copy_target,
            entries,
            expected_root=inventory.root,
            control_schema=control_schema,
        )
    if source_stage.office_matches is None:
        await _bindings(
            inputs.connection,
            copy_target,
            artifact,
            inputs.source_manifest,
            control_schema,
            retain_source_bindings=initial_summary is None and source_stage.membership_rows > 0,
            source_stage=source_stage,
        )
    await _record_source_receipts(
        inputs.connection,
        copy_target,
        recipes,
        inputs.approved,
        source_stage,
        artifact,
        (
            initial_summary,
            inputs.office_context,
            inputs.retained_fhir_reads,
            inputs.source_manifest,
        ),
        control_schema,
    )
    if source_stage.office_matches is not None and source_stage.preservation is not None:
        await _bindings(
            inputs.connection,
            copy_target,
            artifact,
            inputs.source_manifest,
            control_schema,
            retain_source_bindings=False,
            source_stage=source_stage,
        )


async def _record_composition_address(connection, copy_target, address_source, control_schema):
    """Retain authenticated preparation coordinates before a sealed retry can need them."""
    if type(address_source) is not PinnedAddressSource:
        raise RegistryCompositionError("registry_composition_address_scope_changed")
    address_by_field = {
        "schema_name": address_source.schema_name,
        "table_name": address_source.table_name,
        "generation_id": address_source.generation_id,
    }
    recorded = await connection.fetch(
        f"UPDATE {_namespace(control_schema)}.network_membership_candidate "
        "SET validation_json=jsonb_set(coalesce(validation_json,'{}'::jsonb),'{address_source}',$2::jsonb) "
        "WHERE candidate_id=$1 AND state='open' AND source_generations->>'unified_address'=$3 "
        "AND (NOT (coalesce(validation_json,'{}'::jsonb) ? 'address_source') "
        "OR validation_json->'address_source'=$2::jsonb) RETURNING true AS recorded",
        UUID(copy_target.candidate_id),
        json.dumps(address_by_field, sort_keys=True, separators=(",", ":")),
        address_source.generation_id,
    )
    if len(recorded) != 1 or recorded[0]["recorded"] is not True:
        raise RegistryCompositionError("registry_composition_address_scope_changed")


def _bind_source_contributions(generations_by_source, source_stage, inventory=None):
    from process.registry_source_contribution_store import (
        SUMMARY_KEY as CONTRIBUTION_KEY,
    )
    from process.registry_source_contribution_store import (
        registry_source_contribution_receipt,
        registry_source_contribution_sha256,
    )

    contributions = registry_source_contribution_receipt(
        source_stage,
        **({"inventory_root": inventory.root} if inventory is not None else {}),
    )
    if contributions is not None:
        generations_by_source[CONTRIBUTION_KEY] = registry_source_contribution_sha256(
            contributions, inventory_root=None if inventory is None else inventory.root
        )
    return contributions


async def _em_provider_receipt(connection, address_sources, source_manifest, writer_roles, control_schema):
    """Declare retained provider dependencies before deriving the candidate identity."""
    inputs = address_sources.em_provider_inputs
    if inputs is None and source_manifest is not None and "em_provider_inputs" in source_manifest.source_generations:
        inputs = await resolve_em_provider_inputs(connection, source_manifest, control_schema)
    if inputs is None:
        return None
    return await prepare_em_provider_inputs(
        connection,
        inputs,
        runtime_roles=tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"]))),
    )


def _require_retained_em_provider_receipt(candidate, receipt):
    """A closed replay must already retain the complete predeclared provider recipe."""
    if receipt is None or candidate["state"] == "open":
        return
    validation = candidate.get("validation_json")
    if type(validation) is str:
        validation = json.loads(validation)
    if type(validation) is not dict or validation.get("em_provider_inputs") != receipt:
        raise RegistryCompositionError("registry_composition_em_provider_inputs_changed")


async def compose_registry_membership_candidate(
    connection,
    *,
    request_id,
    approved_revision,
    expected_head,
    address_sources,
    writer_roles,
    source_manifest=None,
    control_schema=None,
):
    """Retain raw source and atomically seal the approved effective composition.

    A retained generation contributes verified raw source evidence. Its older
    custom intent is replaced by the latest approved map, including on rollback.
    A failed materialization rolls back the entire output phase. An exact
    async_sessionmaker uses its genuine fenced session for preparation and keeps
    that physical driver through COPY after releasing the preparation lock.
    Initial recipes require head zero. Explicit replacement recipes require an
    existing retained head and may introduce a new closed address edition. Both
    prove exact office correspondence before candidate construction; manual rows
    always come from the current approved map.
    """
    if isinstance(connection, (AsyncConnection, AsyncSession)):
        raise RegistryCompositionError("registry_composition_requires_phase_owned_transactions")
    if not isinstance(connection, async_sessionmaker) and connection.is_in_transaction():
        raise RegistryCompositionError("registry_composition_requires_phase_owned_transactions")
    if not isinstance(request_id, UUID) or not request_id.int:
        raise RegistryCompositionError("registry_composition_request_invalid")
    request_id = UUID(str(request_id))
    if type(address_sources) is not RegistryCompositionAddressSources:
        raise RegistryCompositionError("registry_composition_address_sources_invalid")
    _require_source_recipe_phase(address_sources, source_manifest, expected_head)
    _namespace(control_schema)
    if isinstance(connection, async_sessionmaker):
        return await _compose_fenced_candidate(
            connection,
            request_id,
            approved_revision,
            expected_head,
            address_sources,
            source_manifest,
            writer_roles,
            control_schema,
        )
    prepared = await _prepare_composition(
        connection,
        request_id,
        approved_revision,
        expected_head,
        address_sources,
        source_manifest,
        writer_roles,
        control_schema,
    )
    return await _complete_composition(connection, prepared, source_manifest, control_schema)


async def _compose_fenced_candidate(
    sessions,
    request_id,
    approved_revision,
    expected_head,
    address_sources,
    source_manifest,
    writer_roles,
    control_schema,
):
    """Retain the genuine connection; release the preparation fence before COPY."""
    schema = _namespace(control_schema)[1:-1]
    async with _company_approval_connection(sessions) as connection:
        async with _company_approval_snapshot(sessions, connection, control_schema=schema) as session:
            prepared = await _prepare_composition_snapshot(
                session,
                request_id,
                approved_revision,
                expected_head,
                address_sources,
                source_manifest,
                writer_roles,
                control_schema,
            )
            driver = (await (await session.connection()).get_raw_connection()).driver_connection
        try:
            if connection.in_transaction() or driver.is_in_transaction():
                raise RegistryCompositionError("registry_composition_requires_phase_owned_transactions")
            await connection.execution_options(isolation_level="AUTOCOMMIT")
            if (await connection.get_raw_connection()).driver_connection is not driver:
                raise RegistryCompositionError("registry_composition_connection_changed")
        except BaseException as error:
            await _drop_composition_stages(driver, prepared, error)
            raise
        return await _complete_composition(driver, prepared, source_manifest, control_schema)


async def _drop_composition_stages(connection, prepared, original_error):
    source_stage, command_stage = prepared[3:5]
    cleanup_error = None
    for name in (
        source_stage.table_name,
        source_stage.contribution_table_name,
        command_stage.add_table,
        command_stage.remove_table,
    ):
        if name is None:
            continue
        try:
            await connection.execute(f"DROP TABLE IF EXISTS pg_temp.{_identifier(name)}")
        except BaseException as error:
            cleanup_error = cleanup_error or error
    if original_error is None and cleanup_error is not None:
        raise cleanup_error


async def _complete_composition(connection, prepared, source_manifest, control_schema):
    (
        copy_target,
        address_source,
        _prepared_state,
        source_stage,
        command_stage,
        writer_roles,
    ) = prepared
    original_error = None
    try:
        if _prepared_state not in {"open", "sealed", "validated", "ready", "published"}:
            raise RegistryCompositionError("registry_composition_candidate_unavailable")
        async with connection.transaction(isolation="repeatable_read"):
            candidate = await _locked_candidate(connection, copy_target, _namespace(control_schema))
            if candidate["state"] not in {
                "open",
                "sealed",
                "validated",
                "ready",
                "published",
            }:
                raise RegistryCompositionError("registry_composition_candidate_unavailable")
            if candidate["state"] == "open":
                await _copy_composition(
                    connection,
                    copy_target,
                    control_schema,
                    source_stage,
                    command_stage,
                    writer_roles,
                )
            else:
                await _complete_closed_composition(connection, prepared, candidate["state"], control_schema)
    except BaseException as error:
        original_error = error
        raise
    finally:
        await _drop_composition_stages(connection, prepared, original_error)
    return copy_target, address_source


async def _complete_closed_composition(connection, prepared, state, control_schema):
    """Verify a newer completed phase without reopening or recopying its candidate."""
    copy_target, _address, _old_state, source_stage, command_stage, writer_roles = prepared
    custody = None
    if state in {"ready", "published"}:
        from process.network_candidate_custody import verify_network_candidate_custody

        custody = await verify_network_candidate_custody(
            connection, copy_target, control_schema=control_schema, required=False
        )
    if custody is None:
        await seal_network_candidate(
            connection,
            copy_target,
            control_schema=control_schema,
            source_stage=source_stage,
            command_stage=command_stage,
        )
        await verify_network_candidate_writer_closure(
            connection, copy_target, **writer_roles, control_schema=control_schema
        )


async def compose_registry_provider_refresh(
    connection,
    request_id,
    approved_revision,
    source_manifest,
    address_sources,
    provider_inputs,
    writer_roles,
    control_schema=None,
):
    """Compose a trusted explicit provider edition; no scheduler or source authority is inferred."""
    if (
        type(source_manifest) is not PinnedNetworkServingManifest
        or type(address_sources) is not RegistryCompositionAddressSources
        or type(provider_inputs) is not EMProviderInputs
    ):
        raise RegistryCompositionError("registry_composition_provider_refresh_invalid")
    return await compose_registry_membership_candidate(
        connection,
        request_id=request_id,
        approved_revision=approved_revision,
        expected_head=source_manifest.generation_id,
        address_sources=replace(address_sources, em_provider_inputs=provider_inputs),
        source_manifest=source_manifest,
        writer_roles=writer_roles,
        control_schema=control_schema,
    )


async def prepare_registry_composition_address_artifact(
    connection,
    *,
    request_id,
    approved_revision,
    address_sources,
    source_manifest,
    expected_head,
    custody,
    office_authority=None,
):
    """Prepare the exact address dependency using main's current Source-selection callback."""
    return await _prepare_registry_composition_address_artifact(
        connection,
        request_scope=(request_id, approved_revision),
        address_sources=address_sources,
        source_manifest=source_manifest,
        expected_head=expected_head,
        custody=custody,
        office_authority=office_authority,
        select_source_recipes=_composition_source_recipes,
    )


async def _composition_sources_with_reads(
    connection,
    source_manifest,
    approved,
    request_id,
    control_schema,
    initial_recipes,
    office_phase,
    expected_head,
):
    office_authority, retained_fhir_reads, *verified_inputs = office_phase
    inputs = _CompositionInputs(
        connection,
        office_authority,
        approved,
        None,
        source_manifest,
        retained_fhir_reads,
        verified_inputs[0] if verified_inputs else None,
        verified_inputs[1] if len(verified_inputs) > 1 else False,
    )
    return await _composition_sources_for_format(inputs, request_id, control_schema, initial_recipes, expected_head)


async def _composition_source_recipes(
    connection,
    source_manifest,
    approved,
    introduced,
    control_schema,
    office_authority,
    request_scope,
    *,
    inventory=False,
):
    """Use the same bounded selection in independent preparation and execution."""
    retained, recipes, _changed = await _selected_composition_sources(
        connection,
        source_manifest,
        approved,
        control_schema,
        office_authority,
        introduced,
        request_scope,
        inventory,
    )
    return retained, recipes


async def _source_preservation(connection, source_stage, preservation):
    """Preserve mapped current accounting while appending only proved prior raw facts."""
    from process.registry_fhir_serving_preservation import (
        append_fhir_preservation_memberships,
        fhir_serving_preservation_sha256,
    )

    if preservation is None:
        return source_stage, None
    combined = await append_fhir_preservation_memberships(connection, source_stage, preservation)
    return combined, fhir_serving_preservation_sha256(preservation.receipt)


async def _source_operation(connection, request_id, source_manifest, scope):
    """Authenticate historical selection using the existing exact durable request."""
    from process.registry_publication_queue import registry_source_rollback_composition_provenance

    if source_manifest is None:
        return None
    approved_revision, expected_head, control_schema = scope
    return await registry_source_rollback_composition_provenance(
        connection, request_id, source_manifest, approved_revision, expected_head, control_schema=control_schema
    )


async def _record_source_operation(connection, copy_target, source_operation, control_schema):
    """Retain historical-operation provenance before candidate write closure."""
    from process.registry_publication_queue import SOURCE_ROLLBACK_PROVENANCE

    if source_operation is not None:
        await connection.execute(
            f"UPDATE {_namespace(control_schema)}.network_membership_candidate SET validation_json="
            "jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            SOURCE_ROLLBACK_PROVENANCE,
            json.dumps(source_operation),
        )


def _bind_source_operation(generations_by_source, source_operation):
    from process.registry_publication_queue import (
        SOURCE_ROLLBACK_PROVENANCE,
        registry_source_rollback_provenance_sha256,
    )

    if source_operation is not None:
        generations_by_source[SOURCE_ROLLBACK_PROVENANCE] = registry_source_rollback_provenance_sha256(source_operation)


async def _prepare_preserved_address_inputs(inputs, recipes, preservation, request_id, control_schema, artifact_inputs):
    connection, approved, office_authority = inputs.connection, inputs.approved, inputs.office_context
    verified_inputs, phase_artifact = inputs.verified_office_inputs, None
    if not inputs.compact and preservation is not None and preservation.receipt["carried_rows"]:
        verified_inputs = None
        if artifact_inputs is not None:
            address_sources, writer_roles = artifact_inputs
            phase_artifact = await _prepare_composition_artifact(
                connection,
                address_sources,
                request_id,
                approved,
                writer_roles,
                control_schema,
                office_authority,
                (recipes, preservation),
            )
            verified_inputs = (phase_artifact.address_source, phase_artifact.runtime_roles)
    return verified_inputs, phase_artifact


async def _prepare_selected_source_stage(
    inputs, request_scope, control_schema, selection, initial_recipes, inventory, artifact_inputs, generations_by_source
):
    """Stage the chosen recipe set and bind preservation after validating introduced offices."""
    request_id, expected_head = request_scope
    retained, recipes, has_selection_changed = selection
    connection, approved, office_authority = inputs.connection, inputs.approved, inputs.office_context
    source_manifest = inputs.source_manifest
    if initial_recipes:
        await _require_introduced_offices(
            connection,
            (retained, recipes),
            approved,
            control_schema,
            office_authority,
            inventory=inventory,
        )
    preservation = await _prepare_preservation(
        connection,
        source_manifest,
        recipes,
        approved.approved_revision,
        (request_id, expected_head, control_schema),
        inventory=inventory,
    )
    verified_inputs, phase_artifact = await _prepare_preserved_address_inputs(
        inputs, recipes, preservation, request_id, control_schema, artifact_inputs
    )
    revision = await _contribution_revision(connection, recipes, approved, request_id, expected_head, control_schema)
    source_stage = await stage_registry_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        contribution_revision=revision,
        **{"inventory": True} if inventory else {},
        read_context=SourceRecipeReadContext(
            office_authority=office_authority,
            source_manifest=source_manifest,
            retained_fhir_reads=inputs.retained_fhir_reads,
            verified_office_inputs=verified_inputs
            if _retained_fhir_only(recipes) and (inventory or approved.total_rows == 0)
            else None,
        ),
    )
    _bind_composition_stage(
        generations_by_source,
        source_stage,
        bool(initial_recipes),
        has_selection_changed,
        inventory,
    )
    source_stage, preservation_sha256 = await _source_preservation(connection, source_stage, preservation)
    if preservation_sha256 is not None:
        generations_by_source["registry_fhir_serving_preservation"] = preservation_sha256
    return (generations_by_source, source_stage.membership_rows, recipes, source_stage), phase_artifact


async def _record_stage_preservation(connection, copy_target, source_stage, control_schema):
    if source_stage.preservation is not None:
        from process.registry_fhir_serving_preservation import SUMMARY_KEY as PRESERVATION_KEY

        await connection.execute(
            f"UPDATE {_namespace(control_schema)}.network_membership_candidate SET validation_json="
            "jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            PRESERVATION_KEY,
            json.dumps(source_stage.preservation.receipt, sort_keys=True, separators=(",", ":")),
        )


async def _registered_composition_inputs(inputs, request_scope, address_sources, control_schema):
    request_id, approved_revision, expected_head = request_scope
    registered_inventory = await resolve_registered_source_inventory(
        inputs.connection,
        request_id,
        approved_revision,
        expected_head,
        control_schema=control_schema,
    )
    inventory = address_sources.compact if registered_inventory is None else registered_inventory
    if inventory and not address_sources.compact:
        raise RegistryCompositionError("registry_composition_source_inventory_requires_compact")
    if registered_inventory is not None:
        registered_compact = await resolve_registered_compact_format(
            inputs.connection,
            request_id,
            approved_revision,
            expected_head,
            control_schema=control_schema,
        )
        if registered_compact is not address_sources.compact:
            raise RegistryCompositionError("registry_composition_prepared_format_changed")
        verified_inputs = (
            await resolve_registered_legacy_office_inputs(
                inputs.connection,
                request_id,
                approved_revision,
                expected_head,
                control_schema=control_schema,
            )
            if not registered_inventory
            else None
        )
        if registered_inventory:
            from process.registry_composition_resume import resolve_registered_verified_office_inputs

            verified_inputs = await resolve_registered_verified_office_inputs(
                inputs.connection, request_id, approved_revision, expected_head, control_schema=control_schema
            )
        inputs = replace(inputs, verified_office_inputs=verified_inputs)
    return inputs, inventory


async def _register_prepared_candidate(
    inputs, request_scope, generations_by_source, artifact, initial_summary, source_scope, control_schema
):
    request_id, expected_head = request_scope
    recipes, source_stage, retention, inventory = source_scope
    if (
        inventory is None
        and source_stage.office_matches is not None
        and initial_summary is not None
        and await resolve_registered_legacy_mapping_marker(
            inputs.connection,
            request_id,
            inputs.approved.approved_revision,
            expected_head,
            initial_summary["correspondence_sha256"],
            default=source_stage.preservation is not None,
            control_schema=control_schema,
        )
    ):
        generations_by_source[OFFICE_MAPPING_KEY] = initial_summary["correspondence_sha256"]
    contributions = _bind_prepared_generations(
        generations_by_source,
        inputs.approved,
        artifact,
        inputs.provider_receipt,
        source_stage,
        inventory,
    )
    _bind_source_operation(generations_by_source, inputs.source_operation)
    copy_target = _target(
        request_id,
        inputs.approved.approved_revision,
        expected_head,
        generations_by_source,
        initial_summary,
        inventory_root=None if inventory is None else inventory.root,
    )
    candidate = await _composition_candidate(
        inputs.connection,
        request_id,
        copy_target,
        generations_by_source,
        inputs.approved.approved_revision,
        expected_head,
        (
            retention,
            recipes,
            inputs.provider_receipt,
            initial_summary,
            contributions,
            inventory,
        ),
        control_schema,
    )
    return copy_target, candidate
