# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed membership stages with named workspaces and logical office context."""

import hashlib
import json
from dataclasses import dataclass, replace
from uuid import UUID, uuid4, uuid5

from process.network_address_projection import _identifier
from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_contract import require
from process.network_fhir_typed_mapping import (
    BINDING_COLUMNS,
    MEMBER_COLUMNS,
    TypedGraphApproval,
    map_typed_graph_batch,
)
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.registry_source_recipe_composition import (
    RegistrySourceRecipeStage,
    _source_stage_generation,
    _source_stage_recipes,
)
from process.registry_source_recipe_store import canonical_registry_source_recipes
from process.registry_typed_source_metadata import validate_typed_contribution, validate_typed_derivation


@dataclass
class TypedOfficeMatches:
    table_name: str
    recipe_sha256: str
    approved_generation_id: str
    address_source: object
    runtime_roles: tuple
    mappings: tuple
    legacy_stages: tuple = ()
    child_stages: tuple = ()

    def require_ready(self, address_source, approved):
        """Validate the mapped data workspace and its logical approval/address context."""
        _identifier(self.table_name)
        require(
            address_source == self.address_source and approved.generation_id == self.approved_generation_id,
            "office_context",
        )
        from process.network_address_projection import PinnedAddressSource

        require(type(self.address_source) is PinnedAddressSource, "office_context")
        self.address_source.__post_init__()
        for mapping in self.mappings:
            mapping.require_ready()


def is_typed_source_stage(stage):
    """Recognize the typed membership and office data representation."""
    return (
        type(stage) is RegistrySourceRecipeStage
        and stage.contribution_revision == 4
        and type(stage.office_matches) is TypedOfficeMatches
    )


def typed_stage_projection(stage):
    """Render the declared typed membership workspace."""
    require(is_typed_source_stage(stage), "stage_kind")
    return "SELECT " + ",".join(MEMBER_COLUMNS) + " FROM pg_temp." + _identifier(stage.table_name)


async def verify_typed_stage_offices(connection, stage, address_source, runtime_roles, approved, control_schema):
    """Validate logical approval and the named address snapshot."""
    require(is_typed_source_stage(stage), "stage_kind")
    stage.office_matches.require_ready(address_source, approved)


async def verify_independent_typed_source_stage(connection, stage):
    """Require complete or explicitly sampled Source semantics before publication."""
    from process.network_fhir_sampled_validation import SampledTypedGraphRead
    from process.network_fhir_typed_contract import TypedFHIRGraphStage

    require(is_typed_source_stage(stage), "stage_kind")
    for mapping in stage.office_matches.mappings:
        from process.registry_typed_retained_source import RetainedTypedGraphRead

        require(
            type(mapping.source_stage) is RetainedTypedGraphRead
            or type(mapping.source_stage) is SampledTypedGraphRead
            or (type(mapping.source_stage) is TypedFHIRGraphStage and mapping.source_stage.checked),
            "independent_source_required",
        )
        mapping.source_stage.require_ready()


async def _bindings(connection, names):
    from process.network_initial_source_office_bindings import copy_source_office_binding_rows

    name = "typed_office_bindings_" + uuid4().hex
    await copy_source_office_binding_rows(connection, "pg_temp", name, ())
    target = "pg_temp." + _identifier(name)
    for source in names:
        require(
            not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM "
                + source
                + " a JOIN "
                + target
                + " b USING(provider_system,provider_id,location_id) WHERE ROW(a.location_key,a.entity_type,a.entity_id) IS DISTINCT FROM ROW(b.location_key,b.entity_type,b.entity_id))"
            ),
            "office_collision",
        )
        await connection.execute(
            "INSERT INTO "
            + target
            + " SELECT "
            + ",".join(BINDING_COLUMNS)
            + " FROM "
            + source
            + " ON CONFLICT DO NOTHING"
        )
    return name


async def stage_typed_source_recipes(connection, recipes, approved, *, request_id, control_schema, read_context):
    """Build typed memberships from named producer captures."""
    from process.registry_source_recipe_composition import load_typed_source_read

    return await _stage_typed_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        read_context=read_context,
        source_reader=load_typed_source_read,
    )


async def stage_verified_typed_source_recipes(
    connection, recipes, approved, *, request_id, control_schema, read_context
):
    """Rebuild Source graphs through complete EOF or explicit semantic sampling."""
    from process.registry_source_recipe_composition import require_typed_source_read
    from process.registry_typed_retained_source import retained_typed_source_proofs

    sampling = getattr(read_context.source_witness_runtime, "semantic_validation", None)
    proofs = await retained_typed_source_proofs(
        connection,
        read_context.source_manifest,
        recipes,
        control_schema,
        **({"allow_sampled": True} if sampling is not None else {}),
    )
    return await _stage_typed_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        read_context=read_context,
        source_reader=require_typed_source_read,
        retained_proofs=proofs,
    )


def _typed_mapping_documents(recipe, mapped):
    recipe_sha256 = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    metadata = mapped.metadata
    contribution = validate_typed_contribution(
        {
            "component": "registry_fhir_typed_contribution",
            "descriptor_version": 4,
            "recipe_sha256": recipe_sha256,
            "capture_sha256": recipe.source_pin.generation_id,
            **{key: metadata[key] for key in ("catalog_generation", "mapping_sha256", "counters")},
        }
    )
    derivation = validate_typed_derivation(
        {
            "derivation_version": 3,
            "recipe_sha256": recipe_sha256,
            "capture_sha256": recipe.source_pin.generation_id,
            "contribution_sha256": digest(contribution),
            **{
                key: metadata[key]
                for key in (
                    "mapping_sha256",
                    "office_policy",
                    "canonicalizer_version",
                    "approved_revision",
                    "approved_generation_sha256",
                    "address_source",
                )
            },
            **{key: metadata["counters"][key] for key in ("matched_offices", "missing_offices")},
        }
    )
    return contribution, derivation


async def _map_typed_recipes(
    connection, recipes, approved, table, read_context, source_reader, retained_proofs, control_schema
):
    address_source, roles = read_context.verified_office_inputs
    inputs, contributions, derivations, generations = [], [], [], []
    for recipe in recipes:
        recipe_sha256 = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
        if retained_proofs and await _has_retained_sample_coverage(
            connection, recipe, approved, control_schema, retained_proofs.get(recipe_sha256)
        ):
            from process.registry_source_recipe_composition import load_typed_source_read
            from process.registry_typed_retained_source import RetainedTypedGraphRead

            loaded = await load_typed_source_read(
                connection, recipe, approved, control_schema, read_context.source_witness_runtime
            )
            read = RetainedTypedGraphRead(loaded, retained_proofs[recipe_sha256])
            read.require_ready()
            sampling = getattr(read_context.source_witness_runtime, "semantic_validation", None)
            if sampling is not None:
                await sampling.record_retained(read.proof, connection)
        else:
            read = await source_reader(
                connection, recipe, approved, control_schema, read_context.source_witness_runtime
            )
        inputs.append(
            (
                read,
                TypedGraphApproval(
                    approved_source=approved,
                    binding_coordinates=recipe.binding_coordinates,
                    catalog_descriptor=recipe.source_pin.catalog_descriptor,
                    catalog_generation=recipe.source_pin.catalog_generation,
                ),
                None,
            )
        )
    mappings = await map_typed_graph_batch(
        connection,
        inputs,
        address_source=address_source,
        runtime_roles=roles,
        control_schema=control_schema,
    )
    for recipe, mapped in zip(recipes, mappings, strict=True):
        await connection.execute(
            "INSERT INTO pg_temp."
            + _identifier(table)
            + " SELECT "
            + ",".join(MEMBER_COLUMNS)
            + " FROM "
            + mapped.relation("memberships")
        )
        contribution, derivation = _typed_mapping_documents(recipe, mapped)
        contributions.append(contribution)
        derivations.append(derivation)
        generations.append(digest([contribution, derivation]))
    return mappings, contributions, derivations, generations


async def _has_retained_sample_coverage(connection, recipe, approved, control_schema, proof):
    """A widened approval requires fresh samples for previously uncovered Source networks."""
    if proof is None:
        return False
    if proof.semantic_receipt is None:
        return True
    from process.network_approved_source_bindings import (
        APPROVED_NETWORK_BINDINGS_SQL,
        require_approved_network_source_bindings,
    )
    from process.network_fhir_typed_mapping import _approved_graph_keys
    from process.network_membership_candidate_lifecycle import _control_namespace

    await require_approved_network_source_bindings(
        connection, approved, recipe.binding_coordinates, control_schema=control_schema
    )
    bindings = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema))
    arguments = (
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        recipe.source_pin.catalog_generation,
    )
    current_keys = await _approved_graph_keys(connection, bindings, arguments)
    sampled_keys = {network["network_source_key"] for network in proof.semantic_receipt["networks"]}
    return set(current_keys) <= sampled_keys


async def _typed_office_matches(connection, mappings, recipe_hash, approved, address_source, roles, control_schema):
    name = await _bindings(connection, [mapping.relation("bindings") for mapping in mappings])
    return TypedOfficeMatches(name, recipe_hash, approved.generation_id, address_source, tuple(roles), tuple(mappings))


async def _typed_recipe_stage(connection, recipes, approved, table, recipe_hash, mapped_outputs, matches):
    mappings, contributions, derivations, generations = mapped_outputs
    mapped_rows = sum(contribution_document["counters"]["mapped_pair_rows"] for contribution_document in contributions)
    omitted = sum(
        contribution_document["counters"]["strict_pair_rows"] - contribution_document["counters"]["mapped_pair_rows"]
        for contribution_document in contributions
    )
    provider_rows = sum(
        [await connection.fetchval("SELECT count(*) FROM " + mapping.relation("provider_only")) for mapping in mappings]
    )
    context_by_name = {
        key: mappings[0].metadata[key]
        for key in (
            "office_policy",
            "canonicalizer_version",
            "approved_revision",
            "approved_generation_sha256",
            "address_source",
        )
    }
    return RegistrySourceRecipeStage(
        table,
        mapped_rows,
        omitted,
        _source_stage_generation(recipe_hash, approved, generations, mapped_rows, omitted),
        source_generations=tuple(generations),
        provider_only_rows=provider_rows,
        contribution_receipts=tuple(contributions),
        contribution_revision=4,
        inventory=True,
        office_matches=matches,
        raw_source_generations=tuple(recipe.source_pin.generation_id for recipe in recipes),
        office_derivations=tuple(derivations),
        office_policy_context=context_by_name,
    )


async def _stage_typed_source_recipes(
    connection, recipes, approved, *, request_id, control_schema, read_context, source_reader, retained_proofs=None
):
    """Stage complete typed recipes with membership, office and accounting data."""
    require(
        connection.is_in_transaction()
        and type(request_id) is UUID
        and recipes
        and all(type(recipe.source_pin) is PinnedTypedFHIRMembershipSource for recipe in recipes),
        "stage_scope",
    )
    require(
        type(read_context.verified_office_inputs) is tuple and len(read_context.verified_office_inputs) == 2,
        "office_inputs",
    )
    address_source, roles = read_context.verified_office_inputs
    recipes, recipe_hash = _source_stage_recipes(recipes, True)
    table = "registry_source_page_" + uuid5(request_id, recipe_hash + ":" + approved.generation_id).hex
    async with connection.transaction():
        await connection.execute(
            "CREATE TEMP TABLE "
            + _identifier(table)
            + "(network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,evidence_id text NOT NULL)"
        )
        from process.registry_typed_membership_parity import create_order_index

        await create_order_index(connection, "pg_temp." + _identifier(table), table + "_order_idx")
        mapped_outputs = await _map_typed_recipes(
            connection, recipes, approved, table, read_context, source_reader, retained_proofs, control_schema
        )
        matches = await _typed_office_matches(
            connection, mapped_outputs[0], recipe_hash, approved, address_source, roles, control_schema
        )
        return await _typed_recipe_stage(connection, recipes, approved, table, recipe_hash, mapped_outputs, matches)


async def _copy_combined_stage(connection, typed_stage, legacy_stage, request_id, recipe_hash, approved):
    from process.registry_source_office_custody import _copy_verified_binding_pages
    from process.registry_source_retention import registry_source_stage_projection

    matches = typed_stage.office_matches
    name = "registry_source_page_" + uuid5(request_id, "mixed:" + recipe_hash + approved.generation_id).hex
    from process.registry_typed_membership_parity import create_order_index

    await connection.execute(
        "CREATE TEMP TABLE "
        + _identifier(name)
        + "(LIKE pg_temp."
        + _identifier(typed_stage.table_name)
        + " INCLUDING CONSTRAINTS)"
    )
    await create_order_index(connection, "pg_temp." + _identifier(name), name + "_order_idx")
    await connection.execute(
        "INSERT INTO pg_temp."
        + _identifier(name)
        + " "
        + typed_stage_projection(typed_stage)
        + " UNION ALL "
        + registry_source_stage_projection("pg_temp." + _identifier(legacy_stage.table_name))
    )
    await _copy_verified_binding_pages(connection, legacy_stage.office_matches, matches.table_name)
    return name


async def combine_typed_source_stages(
    connection, typed_stage, legacy_stage, *, request_id, recipes, approved, control_schema
):
    """Union typed and legacy mapped data while preserving contribution accounting."""
    require(
        is_typed_source_stage(typed_stage)
        and type(legacy_stage) is RegistrySourceRecipeStage
        and legacy_stage.office_matches is not None,
        "mixed_stage",
    )
    recipes, recipe_hash = _source_stage_recipes(recipes, True)
    matches = typed_stage.office_matches
    matches.require_ready(matches.address_source, approved)
    legacy_recipes = tuple(
        recipe for recipe in recipes if type(recipe.source_pin) is not PinnedTypedFHIRMembershipSource
    )
    _, legacy_recipe_hash = _source_stage_recipes(legacy_recipes, True)
    require(
        legacy_stage.office_matches.address_source == matches.address_source
        and legacy_stage.office_matches.approved_generation_id == approved.generation_id
        and legacy_stage.office_matches.recipe_sha256 == legacy_recipe_hash,
        "office_context",
    )
    name = await _copy_combined_stage(connection, typed_stage, legacy_stage, request_id, recipe_hash, approved)
    combined = replace(
        matches, recipe_sha256=recipe_hash, legacy_stages=(legacy_stage,), child_stages=(typed_stage, legacy_stage)
    )
    mapped, omitted = (
        typed_stage.membership_rows + legacy_stage.membership_rows,
        typed_stage.omitted_rows + legacy_stage.omitted_rows,
    )
    generations = typed_stage.source_generations + legacy_stage.source_generations
    return replace(
        typed_stage,
        table_name=name,
        membership_rows=mapped,
        omitted_rows=omitted,
        generation_sha256=_source_stage_generation(recipe_hash, approved, generations, mapped, omitted),
        source_generations=generations,
        raw_source_rows=legacy_stage.raw_source_rows,
        accepted_raw_source_rows=legacy_stage.accepted_raw_source_rows,
        omitted_raw_source_rows=legacy_stage.omitted_raw_source_rows,
        provider_only_rows=typed_stage.provider_only_rows + legacy_stage.provider_only_rows,
        contribution_table_name=legacy_stage.contribution_table_name,
        contribution_receipts=typed_stage.contribution_receipts + legacy_stage.contribution_receipts,
        office_derivations=typed_stage.office_derivations + legacy_stage.office_derivations,
        office_matches=combined,
        raw_source_generations=typed_stage.raw_source_generations + legacy_stage.raw_source_generations,
    )


async def _copy_typed_offices(connection, copy_target, stage):
    from process.network_initial_source_office_bindings import _copy_bindings

    await _copy_bindings(connection, copy_target, ())
    target_relation = _identifier(copy_target.schema_name) + ".provider_location_binding"
    index = _identifier(copy_target.schema_name) + ".typed_office_location_idx"
    if await connection.fetchval("SELECT to_regclass($1)", index) is None:
        require(
            not await connection.fetchval("SELECT EXISTS(SELECT FROM " + target_relation + ")"),
            "office_index_requires_empty_heap",
        )
        await connection.execute(
            "CREATE INDEX typed_office_location_idx ON "
            + target_relation
            + "(location_key,provider_system,provider_id,entity_type,entity_id)"
        )
    source_relation = "pg_temp." + _identifier(stage.office_matches.table_name)
    require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM "
            + source_relation
            + " a JOIN "
            + target_relation
            + " b USING(provider_system,provider_id,location_id) WHERE ROW(a.location_key,a.entity_type,a.entity_id) IS DISTINCT FROM ROW(b.location_key,b.entity_type,b.entity_id))"
        ),
        "office_collision",
    )
    await connection.execute(
        "INSERT INTO "
        + target_relation
        + " SELECT "
        + ",".join(BINDING_COLUMNS)
        + " FROM "
        + source_relation
        + " ON CONFLICT DO NOTHING"
    )


async def record_typed_source_offices(
    connection, copy_target, recipes, approved, address_source, runtime_roles, stage, *, control_schema=None
):
    """Copy office bindings and record the complete logical inventory summary."""
    from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
    from process.registry_initial_source_composition import SUMMARY_KEY, _is_receipt_match, _report
    from process.registry_source_inventory_preparation import prepare_source_inventory
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_typed_source_metadata import typed_office_summary

    await verify_typed_stage_offices(connection, stage, address_source, runtime_roles, approved, control_schema)
    candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
    _require_open(candidate)
    reference, _ = prepare_source_inventory(recipes, stage.contribution_receipts, stage.office_derivations)
    require(reference == verify_source_recipe_metadata(candidate).inventory, "office_inventory")
    receipt = typed_office_summary(
        reference.root, sorted(stage.office_derivations, key=lambda item: item["recipe_sha256"])
    )
    require(_is_receipt_match(candidate, receipt, None), "office_summary")
    await _copy_typed_offices(connection, copy_target, stage)
    report = _report(candidate)
    require(SUMMARY_KEY not in report or report[SUMMARY_KEY] == receipt, "office_summary")
    if SUMMARY_KEY not in report:
        await connection.execute(
            "UPDATE "
            + _control_namespace(control_schema)
            + ".network_membership_candidate SET validation_json=jsonb_set(coalesce(validation_json,'{}'::jsonb),ARRAY[$2::text],$3::jsonb) WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            SUMMARY_KEY,
            json.dumps(receipt),
        )
    return receipt


async def rebind_typed_producer_stage(connection, stage):
    """Reload declared capture names and compare content in the producer copy transaction."""
    from process.network_fhir_typed_capture import TypedGraphRead, load_typed_graph_capture

    require(is_typed_source_stage(stage), "stage_kind")
    require(connection.is_in_transaction(), "transaction_required")
    for mapping in stage.office_matches.mappings:
        source = mapping.source_stage
        require(type(source) is TypedGraphRead, "producer_rebind")
        source.require_ready()
        mapping.source_stage = await load_typed_graph_capture(connection, source.capture, expected_anchor=source.anchor)
        mapping.require_ready()


async def drop_typed_source_stage(connection, stage):
    """Drop only temporary relations owned by this typed stage and its attached children."""
    require(is_typed_source_stage(stage), "stage_kind")
    matches, names = stage.office_matches, {stage.table_name, stage.office_matches.table_name}
    for mapped in matches.mappings:
        names.update(mapped.table_names.values())
    for child in matches.child_stages:
        names.add(child.table_name)
        if child.contribution_table_name:
            names.add(child.contribution_table_name)
        if child.office_matches is not None:
            names.add(child.office_matches.table_name)
    for name in sorted(names):
        await connection.execute("DROP TABLE IF EXISTS pg_temp." + _identifier(name))
