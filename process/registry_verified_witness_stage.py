# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage complete authenticated witness facts and one globally pruned office multiset."""

from dataclasses import replace
from uuid import uuid5

from db.registry_schema import registry_schema
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    PinnedFHIRMembershipSource,
    _require_source_recipe,
    _reviewed_descriptor,
    fhir_normalized_capture,
    fhir_physical_source,
    read_fhir_source_witness_page_v2,
)
from process.registry_source_contribution_artifacts import anchored_source_fact_descriptor
from process.registry_source_contribution_store import (
    _digest,
    copy_fhir_source_witness_page_v2,
    create_witness_mapping_workspace,
    map_fhir_source_witnesses_v2,
    retained_source_contributions,
    verified_office_fact_identity,
)
from process.registry_source_recipe_composition import (
    _STAGE_COUNTER_FIELDS,
    RegistrySourceCompositionError,
    SourceRecipeReadContext,
    _create_source_stage_tables,
    _finish_source_stage,
    _require_recipe_custody,
    _require_source_stage_scope,
    _source_stage_recipes,
    _source_stage_result,
    _source_stage_table_names,
)
from process.registry_verified_witness_offices import (
    WitnessOfficeSource,
    _prepare_source_witness_mapping,
    _WitnessOfficeContext,
    prepare_witness_office_mapping_v2,
)
from process.uhc_flex_practitioner_async_safety import drain_operation


def _require_witness_scope(recipes, approved, office_inputs):
    if (
        type(approved) is not ApprovedMembershipSource
        or not recipes
        or not all(
            type(recipe.source_pin) is PinnedFHIRMembershipSource
            and fhir_normalized_capture(recipe.source_pin) is not None
            for recipe in recipes
        )
        or type(office_inputs) is not tuple
        or len(office_inputs) != 2
        or type(office_inputs[0]) is not PinnedAddressSource
        or office_inputs[0].table_name != "entity_address_unified"
        or type(office_inputs[1]) is not tuple
        or not 1 <= len(office_inputs[1]) <= 32
        or any(type(role) is not str for role in office_inputs[1])
        or tuple(sorted(set(office_inputs[1]))) != office_inputs[1]
    ):
        raise RegistrySourceCompositionError("registry_source_composition_verified_inventory_unbound")
    for role in office_inputs[1]:
        _identifier(role)


async def _authorize_witness_recipe(
    connection, recipe, approved, control_schema, retained_reads, *, source_witness_runtime=None
):
    if recipe.source_pin.source_witness_capture is not None:
        read = await _require_recipe_custody(
            connection,
            recipe,
            approved,
            control_schema,
            retained_fhir_reads=None,
            source_witness_runtime=source_witness_runtime,
        )
    else:
        read = await _require_recipe_custody(
            connection, recipe, approved, control_schema, retained_fhir_reads=retained_reads
        )
    await _require_source_recipe(
        connection,
        recipe.source_pin,
        _reviewed_descriptor(recipe.source_pin, approved, recipe.binding_coordinates, True),
        approved,
        recipe.binding_coordinates,
        control_schema,
        read,
    )
    return read


async def _native_witness_headers(connection, recipe):
    """Compare the complete physical resource census with its independently pinned Source origin."""
    physical = fhir_physical_source(recipe.source_pin)
    resources = await connection.fetch(
        f"SELECT resource_type,count(*) AS rows FROM {_identifier(recipe.source_pin.read_schema_name)}."
        "provider_directory_dataset_resource WHERE dataset_id=$1 GROUP BY resource_type",
        physical.dataset_id,
    )
    counts_by_kind = {resource["resource_type"]: resource["rows"] for resource in resources}
    expected_by_kind = dict(fhir_normalized_capture(physical).origin.resource_counts)
    if any(counts_by_kind.get(kind, 0) != count for kind, count in expected_by_kind.items()) or any(
        count and kind not in expected_by_kind for kind, count in counts_by_kind.items()
    ):
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    return sum(counts_by_kind.get(kind, 0) for kind in ("PractitionerRole", "OrganizationAffiliation"))


async def _read_witness_page(connection, recipe, approved, control_schema, position, read):
    cursor, limit = position
    while True:
        try:
            page = await read_fhir_source_witness_page_v2(
                connection,
                recipe.source_pin,
                registry_schema=control_schema,
                after_resource=cursor,
                limit=limit,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                retained_read=read,
            )
            return page, limit
        except FHIRMembershipBatchBoundsError:
            if limit == 1:
                raise
            limit //= 2


async def _capture_witness_facts(connection, recipe, approved, control_schema, fact_table, read):
    """Capture every native header, including an authenticated empty terminal page, in bounded COPY chunks."""
    cursor, limit, ordinal, headers = None, 1000, 0, 0
    identity = verified_office_fact_identity(recipe)[0]
    while True:
        page, limit = await _read_witness_page(connection, recipe, approved, control_schema, (cursor, limit), read)
        if page.source != recipe.source_pin:
            raise RegistrySourceCompositionError("registry_source_composition_generation_changed")
        ordinal = await copy_fhir_source_witness_page_v2(connection, page, fact_table, identity, ordinal)
        headers += page.source_rows
        if not page.source_rows:
            if page.next_resource is not None:
                raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
            return ordinal, headers
        if page.next_resource is None or cursor is not None and page.next_resource <= cursor:
            raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
        cursor = page.next_resource


async def _stage_witness_source(
    connection, recipe, approved, control_schema, tables, retained_reads, artifact, *, source_witness_runtime=None
):
    fact_table, member_table = tables
    read = await _authorize_witness_recipe(
        connection, recipe, approved, control_schema, retained_reads, source_witness_runtime=source_witness_runtime
    )
    expected_headers = await _native_witness_headers(connection, recipe)
    await _create_source_stage_tables(connection, member_table, None)
    if artifact is None:
        copied, headers = await _capture_witness_facts(connection, recipe, approved, control_schema, fact_table, read)
        if headers != expected_headers:
            raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
        relation = "pg_temp." + _identifier(fact_table)
    else:
        relation, copied = artifact.relation, artifact.descriptor["fact_rows"]
    workspace = await create_witness_mapping_workspace(connection, member_table)
    try:
        descriptor = await map_fhir_source_witnesses_v2(
            connection, recipe, approved, relation, member_table, control_schema
        )
    finally:
        await drain_operation(
            connection.execute(f"DROP TABLE pg_temp.{_identifier(workspace)}"), preserve_cancellation=True
        )
    if descriptor["fact_rows"] != copied or descriptor["raw_source_rows"] != expected_headers:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    if artifact is not None:
        descriptor = anchored_source_fact_descriptor(descriptor, artifact)
    await _authorize_witness_recipe(
        connection, recipe, approved, control_schema, retained_reads, source_witness_runtime=source_witness_runtime
    )
    if await _native_witness_headers(connection, recipe) != expected_headers:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    return WitnessOfficeSource(recipe, descriptor, member_table)


def _witness_totals(descriptors, derivations):
    totals_by_field = {name: sum(descriptor[name] for descriptor in descriptors) for name in _STAGE_COUNTER_FIELDS}
    totals_by_field["membership_rows"] = sum(derivation["accepted_membership_rows"] for derivation in derivations)
    totals_by_field["omitted_rows"] += sum(descriptor["unresolved_rows"] for descriptor in descriptors)
    totals_by_field["omitted_rows"] += sum(derivation["omitted_membership_rows"] for derivation in derivations)
    return totals_by_field


def _witness_commitments(recipes, witness_sources, derivations, context):
    """Validate complete paired metadata and commit each full descriptor plus policy derivation."""
    from process.registry_source_inventory_preparation import prepare_source_inventory
    from process.registry_source_inventory_summaries import contribution_summary_v4, office_summary_v5

    descriptors = tuple(witness_source.descriptor for witness_source in witness_sources)
    reference, _ = prepare_source_inventory(recipes, descriptors, derivations)
    contribution_summary_v4(reference.root, sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"]))
    office_summary_v5(reference.root, context, sorted(derivations, key=lambda derivation: derivation["recipe_sha256"]))
    derivations_by_recipe = {derivation["recipe_sha256"]: derivation for derivation in derivations}
    generations = tuple(
        _digest([descriptor, derivations_by_recipe[descriptor["recipe_sha256"]]]) for descriptor in descriptors
    )
    return descriptors, generations


async def _finish_witness_inventory(
    connection, recipes, approved, tables, witness_sources, office_inputs, scope, *, source_witness_runtime=None
):
    """Bind full derivations and all counters before the final serving page can survive the savepoint."""
    control_schema, recipe_sha256 = scope
    raw_table, page_table, fact_table = tables
    await connection.execute(f"ALTER TABLE pg_temp.{_identifier(raw_table)} ADD COLUMN page_key text NOT NULL")
    if source_witness_runtime is None:
        matches, derivations, context = await prepare_witness_office_mapping_v2(
            connection,
            *office_inputs,
            witness_sources,
            raw_table,
            approved=approved,
            control_schema=control_schema,
            recipes_sha256=recipe_sha256,
        )
    else:
        matches, derivations, context = await _prepare_source_witness_mapping(
            connection,
            *office_inputs,
            witness_sources,
            raw_table,
            _WitnessOfficeContext(approved, control_schema, recipe_sha256, source_witness_runtime),
        )
    descriptors, generations = _witness_commitments(recipes, witness_sources, derivations, context)
    own = sum(descriptor["fact_rows"] for descriptor in descriptors if descriptor["fact_candidate_id"] is None)
    if await connection.fetchval(f"SELECT count(*) FROM pg_temp.{_identifier(fact_table)}") != own:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    for witness_source in witness_sources:
        await drain_operation(
            connection.execute(f"DROP TABLE pg_temp.{_identifier(witness_source.table_name)}"),
            preserve_cancellation=True,
        )
    await connection.execute(f"ALTER TABLE pg_temp.{_identifier(raw_table)} DROP COLUMN page_key")
    totals_by_field = _witness_totals(descriptors, derivations)
    await _finish_source_stage(connection, raw_table, page_table, totals_by_field["membership_rows"])
    stage = replace(
        _source_stage_result(
            page_table,
            recipe_sha256,
            approved,
            generations,
            totals_by_field,
            descriptors,
            (fact_table, 2, sum((descriptor["fact_rows"] for descriptor in descriptors)) - own),
            True,
        ),
        office_matches=matches,
        raw_source_generations=tuple((descriptor["source_generation"] for descriptor in descriptors)),
    )
    return replace(
        stage,
        office_derivations=tuple(derivations),
        office_policy_context=context,
        unresolved_rows=sum(descriptor["unresolved_rows"] for descriptor in descriptors),
        unresolved_raw_source_rows=sum(descriptor["unresolved_raw_source_rows"] for descriptor in descriptors),
    )


async def stage_verified_witness_inventory(
    connection,
    recipes,
    approved,
    *,
    request_id,
    control_schema,
    read_context=None,
    **legacy_options,
):
    """Use the exact supplied predecessor for reproducible fresh/reuse decisions; never select the current head."""
    read_context = _witness_read_context(read_context, legacy_options)
    recipes, recipe_sha256 = _source_stage_recipes(recipes, True)
    _require_witness_scope(recipes, approved, read_context.verified_office_inputs)
    await _require_source_stage_scope(connection, approved, request_id, 2)
    control_schema = registry_schema() if control_schema is None else control_schema
    _identifier(control_schema)
    retained_reads = {} if read_context.retained_fhir_reads is None else read_context.retained_fhir_reads
    stage_id = uuid5(request_id, recipe_sha256 + ":" + approved.generation_id).hex
    tables = _source_stage_table_names(stage_id, recipes)
    async with connection.transaction():
        retained = await retained_source_contributions(
            connection, read_context.source_manifest, control_schema, selected_recipes=recipes, witnesses=True
        )
        await _create_source_stage_tables(connection, tables[0], tables[2])
        witness_sources = []
        for recipe in recipes:
            identity, capture = verified_office_fact_identity(recipe)
            member_table = "registry_witness_members_" + uuid5(request_id, identity).hex
            witness_sources.append(
                await _stage_witness_source(
                    connection,
                    recipe,
                    approved,
                    control_schema,
                    (tables[2], member_table),
                    retained_reads,
                    retained.get(capture),
                    source_witness_runtime=read_context.source_witness_runtime,
                )
            )
        return await _finish_witness_inventory(
            connection,
            recipes,
            approved,
            tables,
            witness_sources,
            read_context.verified_office_inputs,
            (control_schema, recipe_sha256),
            source_witness_runtime=read_context.source_witness_runtime,
        )


def _witness_read_context(read_context, legacy_options):
    if read_context is not None:
        if type(read_context) is SourceRecipeReadContext and not legacy_options:
            return read_context
    elif set(legacy_options) <= {
        "retained_fhir_reads",
        "verified_office_inputs",
        "source_manifest",
        "source_witness_runtime",
    } and {"retained_fhir_reads", "verified_office_inputs"} <= set(legacy_options):
        return SourceRecipeReadContext(**legacy_options)
    raise RegistrySourceCompositionError("registry_source_composition_verified_inventory_unbound")
