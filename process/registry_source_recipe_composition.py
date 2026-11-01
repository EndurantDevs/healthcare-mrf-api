# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replay sealed source editions through the current approved network bindings."""

import asyncio
import hashlib
import io
import json
from dataclasses import asdict, dataclass, replace
from uuid import UUID, uuid5

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    PinnedFHIRMembershipSource,
    RetainedFHIRSuccessorReads,
    _reviewed_descriptor,
    fhir_normalized_capture,
    fhir_physical_source,
    read_fhir_membership_batch,
    read_fhir_membership_fact_batch,
    read_retained_fhir_membership_batch,
)
from process.network_fhir_membership_source import (
    _generation_id as fhir_membership_generation,
)
from process.network_fhir_source_custody import (
    FHIRSourceCustodyError,
    RetainedCMSFHIRSourceCustody,
    require_retained_cms_fhir_source_custody,
)
from process.network_fhir_source_epoch import FHIRSourceEpochError, require_retained_cms_fhir_source_epoch
from process.network_legacy_membership_source import (
    LegacyMembershipBatchBoundsError,
    PinnedACAMembershipSource,
    _read_aca_batch,
    read_aca_raw_fact_batch,
)
from process.network_membership_copy import COPY_COLUMNS, _encode
from process.registry_ptg_office_membership import (
    PTGOfficeBatchBoundsError,
    read_ptg_office_membership_batch,
    require_ptg_office_recipe,
)
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_contribution_artifacts import anchored_source_fact_descriptor
from process.registry_source_contribution_store import (
    copy_fhir_fact_page,
    create_contribution_stage,
    locally_owned_source_descriptor,
    map_fhir_facts,
    recipe_contribution_identity,
    retained_source_contributions,
    reuse_fhir_facts,
)
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from process.uhc_flex_practitioner_async_safety import drain_operation

OFFICE_MAPPING_KEY = "registry_retained_fhir_office_mapping"


class RegistrySourceCompositionError(ValueError):
    """A retained source page or approved mapping cannot be safely replayed."""


@dataclass(frozen=True)
class RegistrySourceRecipeStage:
    table_name: str
    membership_rows: int
    omitted_rows: int
    generation_sha256: str
    source_generations: tuple[str, ...] = ()
    raw_source_rows: int = 0
    accepted_raw_source_rows: int = 0
    omitted_raw_source_rows: int = 0
    provider_only_rows: int = 0
    contribution_table_name: str | None = None
    contribution_receipts: tuple[dict, ...] = ()
    contribution_revision: int = 2
    own_fact_rows: int = 0
    reused_fact_rows: int = 0
    copied_reused_fact_rows: int = 0
    inventory: bool = False
    office_matches: object = None
    raw_source_generations: tuple[str, ...] = ()
    office_derivations: tuple = ()
    office_policy_context: dict | None = None
    unresolved_rows: int = 0
    unresolved_raw_source_rows: int = 0
    selected_stage: object = None
    preservation: object = None


@dataclass(frozen=True)
class SourceRecipeReadContext:
    """Caller-owned source pins, verified office inputs and reusable custody reads."""

    office_authority: object = None
    aca_reads_by_oid: dict | None = None
    retained_fhir_reads: dict | None = None
    verified_office_inputs: object = None
    source_manifest: object = None
    office_matches: object = None
    source_witness_runtime: object = None


@dataclass(frozen=True, repr=False)
class SourceWitnessRuntime:
    """Unserialized installed Source capability and independently configured capture census."""

    source_export: object
    source_profiles: tuple
    capture_owner_role: str
    runtime_roles: tuple[str, ...]

    def __post_init__(self):
        if not callable(self.source_export):
            raise RegistrySourceCompositionError("registry_source_witness_runtime_invalid")
        validate_source_witness_configuration(self.source_profiles, self.capture_owner_role, self.runtime_roles)


def validate_source_witness_configuration(source_profiles, capture_owner_role, runtime_roles):
    """Keep installed capabilities and server configuration under the same closed profile contract."""
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

    if (
        type(source_profiles) is not tuple
        or not source_profiles
        or any(type(profile) is not FHIRCatalogSourceReadProfile for profile in source_profiles)
        or len({profile.schema_name for profile in source_profiles}) != len(source_profiles)
        or type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or any(type(role) is not str for role in runtime_roles)
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or capture_owner_role in runtime_roles
    ):
        raise RegistrySourceCompositionError("registry_source_witness_runtime_invalid")
    for profile in source_profiles:
        profile.__post_init__()
    for role in (capture_owner_role, *runtime_roles):
        _identifier(role)


async def _approved_source_witness_capture(connection, recipe, approved, control_schema):
    """Authenticate the whole capture against protected approval before using its chosen labels."""
    from process.network_approved_source_bindings import (
        APPROVED_NETWORK_BINDINGS_SQL,
        require_approved_network_source_bindings,
    )
    from process.network_fhir_source_witness_contract import validate_capture

    capture = validate_capture(recipe.source_pin.source_witness_capture)
    _reviewed_descriptor(recipe.source_pin, approved, recipe.binding_coordinates, True)
    if approved is None:
        raise RegistrySourceCompositionError("registry_source_witness_approval_unavailable")
    await require_approved_network_source_bindings(
        connection, approved, recipe.binding_coordinates, control_schema=control_schema
    )
    selected = await connection.fetchval(
        "WITH "
        + APPROVED_NETWORK_BINDINGS_SQL.format(
            namespace=_identifier(registry_schema() if control_schema is None else control_schema)
        )
        + " SELECT count(*)>0 AND coalesce(bool_and(record_json->'source_scope_json'="
        "jsonb_build_object('organization_id',record_json->>'source_key','capture_sha256',$8::text)),false)"
        " FROM approved_network_bindings",
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        capture.capture_sha256,
    )
    if selected is not True:
        raise RegistrySourceCompositionError("registry_source_witness_approval_unavailable")
    return capture


async def require_source_witness_read(connection, recipe, approved, control_schema, runtime):
    """Fresh Source replay, never a producer cache, admits one exact approved capture."""
    from process.network_fhir_source_witness_capture import (
        pin_source_fhir_witness_read,
        verify_source_fhir_witness_capture,
    )
    from process.network_fhir_source_witness_contract import SourceWitnessCaptureAnchor

    if type(runtime) is not SourceWitnessRuntime:
        raise RegistrySourceCompositionError("registry_source_composition_source_witness_custody_unavailable")
    runtime.__post_init__()
    capture = await _approved_source_witness_capture(connection, recipe, approved, control_schema)
    if (capture.owner_role, capture.runtime_roles) != (runtime.capture_owner_role, runtime.runtime_roles):
        raise RegistrySourceCompositionError("registry_source_witness_runtime_census_differs")
    profiles = [profile for profile in runtime.source_profiles if profile.schema_name == capture.origin.schema_name]
    if len(profiles) != 1:
        raise RegistrySourceCompositionError("registry_source_witness_profile_unavailable")
    try:
        from directory_membership_runtime import FHIRSourceMembershipInput
    except ImportError:
        raise RegistrySourceCompositionError("registry_source_witness_factory_unavailable") from None
    # Protected approval authenticates these expected labels; the original factory still proves every Source byte.
    expected_origin = FHIRSourceMembershipInput(**asdict(capture.origin))
    anchor = SourceWitnessCaptureAnchor(capture.capture_id, capture.release_id, capture.observed_at)
    await verify_source_fhir_witness_capture(
        runtime.source_export,
        connection,
        source_profile=profiles[0],
        reviewed_origin=expected_origin,
        capture=capture,
        expected_anchor=anchor,
    )
    return await pin_source_fhir_witness_read(connection, capture)


async def _read_recipe_page(
    connection, recipe, approved, cursor, control_schema, limit, office_source=None, *, capture_facts=False
):
    options_by_name = {
        "registry_schema": control_schema,
        "approved_source": approved,
        "binding_coordinates": recipe.binding_coordinates,
        "approved_only": True,
    }
    while True:
        try:
            if type(recipe.source_pin) is PinnedPTGOfficeMembershipSource:
                batch = await read_ptg_office_membership_batch(
                    connection, office_source, approved, cursor, limit, control_schema
                )
                return batch, batch.next_ordinal, limit
            if type(recipe.source_pin) is PinnedFHIRMembershipSource:
                batch = await _read_fhir_recipe_page(
                    connection, recipe, cursor, limit, options_by_name, office_source, capture_facts
                )
                return batch, batch.next_resource, limit
            if office_source is None:
                batch = await read_aca_raw_fact_batch(
                    connection, recipe.source_pin, after_source_position=cursor, limit=limit, **options_by_name
                )
            else:
                batch = await _read_aca_batch(
                    connection,
                    recipe.source_pin,
                    control_schema,
                    approved,
                    recipe.binding_coordinates,
                    True,
                    (None, limit, cursor, True),
                    office_source,
                )
            return batch, batch.next_source_position, limit
        except FHIRMembershipBatchBoundsError, LegacyMembershipBatchBoundsError, PTGOfficeBatchBoundsError:
            if limit == 1:
                raise
            limit //= 2


async def _read_fhir_recipe_page(connection, recipe, cursor, limit, options_by_name, office_source, capture_facts):
    if capture_facts:
        options_by_name.pop("approved_only", None)
        if fhir_normalized_capture(recipe.source_pin) is not None:
            options_by_name["retained_read"] = office_source
        reader = read_fhir_membership_fact_batch
    elif fhir_normalized_capture(recipe.source_pin) is not None:
        options_by_name.pop("approved_only", None)
        return await read_retained_fhir_membership_batch(
            connection, recipe.source_pin, office_source, after_resource=cursor, limit=limit, **options_by_name
        )
    else:
        reader = read_fhir_membership_batch
    return await reader(connection, recipe.source_pin, after_resource=cursor, limit=limit, **options_by_name)


async def _copy_recipe_page(connection, batch, raw_table):
    if (
        batch.unresolved_rows
        or getattr(batch, "unresolved_source_rows", 0)
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
    ):
        raise RegistrySourceCompositionError("registry_source_composition_unresolved")
    copy_bytes, row_count = await drain_operation(
        asyncio.to_thread(_encode, batch.input_bytes), preserve_cancellation=True
    )
    if row_count != batch.membership_rows:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    if row_count:
        with io.BytesIO(copy_bytes) as copy_source:
            status = await connection.copy_to_table(
                raw_table, schema_name="pg_temp", columns=COPY_COLUMNS, format="binary", source=copy_source
            )
            if status != f"COPY {row_count}" or copy_source.tell() != len(copy_bytes):
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")


async def _stage_recipe(
    connection,
    recipe,
    approved,
    raw_table,
    control_schema,
    read_context,
    *,
    fact_table=None,
):
    """Copy bounded pages from one custody-verified recipe with exact aggregate accounting."""
    office_source = await _require_recipe_custody(
        connection,
        recipe,
        approved,
        control_schema,
        read_context.office_authority,
        read_context.aca_reads_by_oid,
        retained_fhir_reads=read_context.retained_fhir_reads,
    )
    cursor, generation = None, None
    limit = 1000
    totals_by_field = dict.fromkeys(_STAGE_COUNTER_FIELDS[:-1], 0)
    fact_rows = 0
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection,
            recipe,
            approved,
            cursor,
            control_schema,
            limit,
            office_source,
            capture_facts=fact_table is not None,
        )
        if generation is not None and generation != batch.generation_id:
            raise RegistrySourceCompositionError("registry_source_composition_generation_changed")
        generation = batch.generation_id
        await _copy_recipe_page(connection, batch, raw_table)
        if fact_table is not None and type(recipe.source_pin) is PinnedFHIRMembershipSource:
            fact_rows = await copy_fhir_fact_page(
                connection, batch, fact_table, recipe_contribution_identity(recipe)[0], fact_rows
            )
        _accumulate_recipe_page(batch, totals_by_field)
        if not batch.source_rows:
            if batch.membership_rows or batch.omitted_rows:
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
            return (generation, *(totals_by_field[field] for field in _STAGE_COUNTER_FIELDS[:-1]))
        if next_cursor is None or next_cursor == cursor:
            raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
        cursor = next_cursor


async def require_registry_source_recipe_custody(
    connection,
    recipe,
    approved=None,
    control_schema=None,
    office_authority=None,
    aca_reads_by_oid=None,
    *,
    retained_fhir_reads=None,
    source_witness_runtime=None,
):
    """Recheck CMS retained custody once before paging its complete recipe."""
    source_pin = recipe.source_pin
    if type(source_pin) is PinnedPTGOfficeMembershipSource:
        return await require_ptg_office_recipe(connection, recipe, approved, control_schema, office_authority)
    if type(source_pin) is PinnedACAMembershipSource:
        from process.registry_aca_source_recipe import prepare_registry_aca_batch_read

        schema_oid = source_pin.prepared.ownership.schema_oid
        prior = (
            aca_reads_by_oid.get(schema_oid)
            if aca_reads_by_oid is not None
            and source_pin.retained_successor is None
            and source_pin.retained_epoch is None
            else None
        )
        verified = await prepare_registry_aca_batch_read(connection, source_pin, verified=prior)
        if aca_reads_by_oid is not None and verified is not None:
            aca_reads_by_oid[schema_oid] = verified
        return verified
    if type(source_pin) is not PinnedFHIRMembershipSource:
        return
    if source_pin.source_witness_capture is not None:
        return await require_source_witness_read(connection, recipe, approved, control_schema, source_witness_runtime)
    if source_pin.retained_capture is not None:
        original = await _retained_recipe_read(connection, source_pin.retained_capture, retained_fhir_reads)
        if source_pin.retained_successor is None:
            return original
        return RetainedFHIRSuccessorReads(
            original,
            await _retained_recipe_read(
                connection, source_pin.retained_successor.retained_capture, retained_fhir_reads
            ),
        )
    await _require_fhir_custody(connection, replace(source_pin, retained_successor=None))
    if source_pin.retained_successor is not None:
        await _require_fhir_custody(connection, source_pin.retained_successor)


async def _require_fhir_custody(connection, source):
    """Authenticate each flat original or successor capture with its own native receipt."""
    if source.source_witness_capture is not None:
        # Source replay and final independent custody are wired by the publication owner.
        raise RegistrySourceCompositionError("registry_source_composition_source_witness_custody_unavailable")
    if source.retained_capture is not None:
        return await _retained_recipe_read(connection, source.retained_capture, None)
    if source.retained_epoch is not None:
        try:
            await require_retained_cms_fhir_source_epoch(connection, source, source.retained_epoch)
            return
        except FHIRSourceEpochError as error:
            raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from error
    if source.custody_owner_role is None:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable")
    custody = RetainedCMSFHIRSourceCustody(
        source,
        source.custody_owner_role,
        source.custody_runtime_roles,
        source.custody_proof_sha256,
        source.custody_catalog_sha256,
    )
    try:
        await require_retained_cms_fhir_source_custody(connection, custody)
    except FHIRSourceCustodyError as error:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from error


_require_recipe_custody = require_registry_source_recipe_custody


async def _retained_recipe_read(connection, capture, retained_fhir_reads):
    """Share one full admission, retaining native custody checks at every use."""
    from process.network_fhir_retained_capture import RetainedFHIRRead, pin_retained_fhir_read

    if retained_fhir_reads is None:
        return await pin_retained_fhir_read(connection, capture)
    if type(retained_fhir_reads) is not dict:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable")
    if capture not in retained_fhir_reads:
        retained_fhir_reads[capture] = await pin_retained_fhir_read(connection, capture)
    read = retained_fhir_reads[capture]
    if type(read) is not RetainedFHIRRead:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable")
    await read.verify(connection, capture)
    return read


_STAGE_COUNTER_FIELDS = (
    "membership_rows",
    "omitted_rows",
    "raw_source_rows",
    "accepted_raw_source_rows",
    "omitted_raw_source_rows",
    "provider_only_rows",
)


async def _stage_source_contribution(
    connection,
    recipe,
    approved,
    raw_table,
    control_schema,
    retained_tables,
    read_context,
):
    """Reuse authenticated FHIR facts or extract one complete contribution before mapping."""
    fact_table, retained_by_capture, revision = retained_tables
    if type(recipe.source_pin) is not PinnedFHIRMembershipSource:
        generation, *counters = await _stage_recipe(
            connection,
            recipe,
            approved,
            raw_table,
            control_schema,
            read_context,
        )
        return generation, (*counters, 0), None, 0
    _, capture_sha256 = recipe_contribution_identity(recipe)
    artifact = retained_by_capture.get(capture_sha256)
    relation = f"pg_temp.{_identifier(fact_table)}"
    if artifact is not None:
        if revision == 1:
            await reuse_fhir_facts(
                connection, artifact.relation, artifact.descriptor, fact_table, artifact.descriptor["recipe_sha256"]
            )
        else:
            relation = artifact.relation
        descriptor = await map_fhir_facts(connection, recipe, approved, relation, raw_table, control_schema)
    else:
        _, *counters = await _stage_recipe(
            connection,
            recipe,
            approved,
            raw_table,
            control_schema,
            read_context,
            fact_table=fact_table,
        )
        descriptor = await map_fhir_facts(
            connection, recipe, approved, relation, raw_table, control_schema, insert=False
        )
        if tuple(descriptor[field] for field in _STAGE_COUNTER_FIELDS[:-1]) != tuple(counters):
            raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    return _contribution_stage_result(descriptor, artifact, recipe, approved, revision)


async def _create_source_stage_tables(connection, raw_table, fact_table):
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(raw_table)}(
      network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
      location_id uuid NOT NULL,evidence_id text NOT NULL)""")
    if fact_table is not None:
        await create_contribution_stage(connection, fact_table)


async def _require_source_stage_scope(connection, approved, request_id, contribution_revision):
    if (
        not connection.is_in_transaction()
        or type(approved) is not ApprovedMembershipSource
        or type(contribution_revision) is not int
        or contribution_revision not in (1, 2)
        or type(request_id) is not UUID
        or not request_id.int
        or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
    ):
        raise RegistrySourceCompositionError("registry_source_composition_scope_invalid")


def _source_stage_result(
    page_table,
    recipe_sha256,
    approved,
    generations,
    totals_by_field,
    receipts,
    storage,
    inventory=False,
):
    fact_table, revision, reused_fact_rows = storage
    generation_sha256 = _source_stage_generation(
        recipe_sha256, approved, generations, totals_by_field["membership_rows"], totals_by_field["omitted_rows"]
    )
    return RegistrySourceRecipeStage(
        page_table,
        generation_sha256=generation_sha256,
        source_generations=tuple(generations),
        contribution_table_name=fact_table,
        contribution_receipts=tuple(receipts),
        contribution_revision=revision,
        own_fact_rows=sum(
            descriptor["fact_rows"] for descriptor in receipts if descriptor.get("fact_candidate_id") is None
        ),
        reused_fact_rows=reused_fact_rows,
        copied_reused_fact_rows=reused_fact_rows if revision == 1 else 0,
        inventory=inventory,
        **totals_by_field,
    )


def _source_stage_table_names(stage_id, recipes):
    fact_table = (
        "registry_source_fact_" + stage_id
        if any(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in recipes)
        else None
    )
    return "registry_recipe_raw_" + stage_id, "registry_source_page_" + stage_id, fact_table


async def stage_registry_source_recipes(
    connection,
    recipes,
    approved,
    *,
    request_id,
    control_schema,
    read_context=None,
    **contribution_options,
):
    """Stage bounded source pages or authenticated retained facts; never publish.

    The caller owns repeatable isolation, admission and temporary-table cleanup.
    Invalid selected evidence rejects the complete stage, including prior pages.
    Legacy verified offices retain their mapping proof without fact receipts.
    """
    contribution_revision, inventory = _source_stage_options(contribution_options)
    read_context = SourceRecipeReadContext() if read_context is None else read_context
    if any(getattr(recipe.source_pin, "source_witness_capture", None) is not None for recipe in recipes) and not (
        inventory and read_context.verified_office_inputs is not None
    ):
        raise RegistrySourceCompositionError("registry_source_witness_verified_inventory_required")
    if inventory and read_context.verified_office_inputs is not None:
        from process.registry_verified_witness_stage import stage_verified_witness_inventory

        return await stage_verified_witness_inventory(
            connection,
            recipes,
            approved,
            request_id=request_id,
            control_schema=control_schema,
            read_context=read_context,
        )
    await _require_source_stage_scope(connection, approved, request_id, contribution_revision)
    recipes, recipe_sha256 = _source_stage_recipes(recipes, inventory)
    control_schema = registry_schema() if control_schema is None else control_schema
    _identifier(control_schema)
    stage_id = uuid5(request_id, recipe_sha256 + ":" + approved.generation_id).hex
    raw_table, page_table, fact_table = _source_stage_table_names(stage_id, recipes)
    read_context = replace(
        read_context,
        retained_fhir_reads={} if read_context.retained_fhir_reads is None else read_context.retained_fhir_reads,
    )
    retained_by_capture = (
        await retained_source_contributions(
            connection, read_context.source_manifest, control_schema, selected_recipes=recipes
        )
        if read_context.verified_office_inputs is None
        else {}
    )
    return await _stage_recipe_snapshot(
        connection,
        recipes,
        approved,
        (raw_table, page_table, fact_table),
        control_schema,
        read_context,
        (recipe_sha256, contribution_revision, inventory),
        retained_by_capture,
    )


def _source_stage_options(options):
    if set(options) - {"contribution_revision", "inventory"}:
        raise RegistrySourceCompositionError("registry_source_composition_scope_invalid")
    revision, inventory = options.get("contribution_revision", 2), options.get("inventory", False)
    if type(inventory) is not bool or inventory and (type(revision) is not int or revision != 2):
        raise RegistrySourceCompositionError("registry_source_composition_scope_invalid")
    return revision, inventory


def _source_stage_recipes(recipes, inventory):
    if inventory:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        preparation = SourceInventoryPreparation(recipes)
        return preparation.recipes, preparation.recipe_root.recipes_sha256
    return recipes, registry_source_recipes_sha256(canonical_registry_source_recipes(recipes))


async def _collect_source_contributions(
    connection, recipes, approved, raw_table, control_schema, storage, office_authority, retained_fhir_reads=None
):
    read_context = SourceRecipeReadContext(
        office_authority=office_authority, aca_reads_by_oid={}, retained_fhir_reads=retained_fhir_reads
    )
    totals_by_field, receipts, generations, reused_rows = dict.fromkeys(_STAGE_COUNTER_FIELDS, 0), [], [], 0
    for recipe in recipes:
        generation, counters, descriptor, reused = await _stage_source_contribution(
            connection,
            recipe,
            approved,
            raw_table,
            control_schema,
            storage,
            read_context,
        )
        generations.append(generation)
        reused_rows += reused
        for field, counter in zip(_STAGE_COUNTER_FIELDS, counters, strict=True):
            totals_by_field[field] += counter
        if descriptor is not None:
            receipts.append(descriptor)
    return totals_by_field, receipts, generations, reused_rows


async def _finish_source_stage(connection, raw_table, page_table, membership_rows):
    if await connection.fetchval(f"SELECT count(*) FROM pg_temp.{_identifier(raw_table)}") != membership_rows:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(page_table)} AS
      SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
        location_id,evidence_id) AS ordinal,to_jsonb(member)::text AS input_json
      FROM pg_temp.{_identifier(raw_table)} member""")
    await connection.execute(f"ALTER TABLE pg_temp.{_identifier(page_table)} ADD PRIMARY KEY(ordinal)")
    await connection.execute(f"DROP TABLE pg_temp.{_identifier(raw_table)}")


def _source_stage_generation(recipe_sha256, approved, generations, membership_rows, omitted_rows):
    return hashlib.sha256(
        json.dumps(
            [recipe_sha256, approved.generation_id, sorted(generations), membership_rows, omitted_rows],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


async def _verified_retained_pages(connection, recipes, approved, control_schema, retained_reads, totals_by_kind):
    """Keep authentic raw pages and three-way accounting while selecting office inputs."""
    for recipe in recipes:
        retained_read = await _require_recipe_custody(
            connection, recipe, approved, control_schema, retained_fhir_reads=retained_reads
        )
        cursor, limit, generation, source_count = None, 1000, None, 0
        while True:
            batch, next_cursor, limit = await _read_recipe_page(
                connection, recipe, approved, cursor, control_schema, limit, retained_read
            )
            if (
                generation is not None
                and generation != batch.generation_id
                or not batch.complete_raw_facts
                or batch.source_rows
                != batch.accepted_source_rows + batch.unresolved_source_rows + batch.omitted_source_rows
            ):
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
            generation = batch.generation_id
            totals_by_kind["membership"] += batch.membership_rows
            totals_by_kind["omitted"] += batch.omitted_rows + batch.unresolved_rows
            totals_by_kind["raw"] += batch.source_rows
            totals_by_kind["accepted"] += batch.accepted_source_rows
            totals_by_kind["omitted_raw"] += batch.omitted_source_rows
            source_count += batch.source_rows
            if not batch.source_rows:
                if batch.membership_rows or next_cursor is not None:
                    raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
                break
            yield batch, cursor, retained_read
            if next_cursor is None or cursor is not None and next_cursor <= cursor:
                raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
            cursor = next_cursor
        counts_by_resource = dict(fhir_physical_source(recipe.source_pin).retained_capture.origin.resource_counts)
        if source_count != counts_by_resource.get("PractitionerRole", 0) + counts_by_resource.get(
            "OrganizationAffiliation", 0
        ):
            raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
        totals_by_kind["generations"].append(generation)


async def _stage_verified_retained_recipes(
    connection, recipes, approved, raw_table, control_schema, retained_reads, office_inputs
):
    """Prune only the existing retained-FHIR/no-custom branch before retention identity."""
    from process.network_initial_cms_office_bindings import (
        prepare_verified_retained_office_mapping,
        verified_retained_offices,
    )

    if (
        approved.total_rows
        or not recipes
        or not all(
            type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.retained_capture is not None
            for recipe in recipes
        )
    ):
        raise RegistrySourceCompositionError("registry_source_composition_scope_invalid")
    address_source, runtime_roles = office_inputs
    totals_by_kind = {"membership": 0, "omitted": 0, "raw": 0, "accepted": 0, "omitted_raw": 0, "generations": []}
    await connection.execute(f"ALTER TABLE pg_temp.{_identifier(raw_table)} ADD COLUMN page_key text NOT NULL")
    pages = _verified_retained_pages(connection, recipes, approved, control_schema, retained_reads, totals_by_kind)
    matches = await prepare_verified_retained_office_mapping(
        connection,
        address_source,
        runtime_roles,
        pages,
        raw_table,
        control_schema=control_schema,
        mapping_scope=(
            registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)),
            approved.generation_id,
        ),
    )
    mapped_rows = await connection.fetchval(f"SELECT count(*) FROM pg_temp.{_identifier(raw_table)}")
    mapping_sha256 = await verified_retained_offices(connection, matches, address_source, runtime_roles, control_schema)
    generations = [
        hashlib.sha256(json.dumps([generation, mapping_sha256], separators=(",", ":")).encode()).hexdigest()
        for generation in totals_by_kind["generations"]
    ]
    await connection.execute(f"ALTER TABLE pg_temp.{_identifier(raw_table)} DROP COLUMN page_key")
    return (
        (
            generations,
            mapped_rows,
            totals_by_kind["omitted"] + totals_by_kind["membership"] - mapped_rows,
            totals_by_kind["raw"],
            totals_by_kind["accepted"],
            totals_by_kind["omitted_raw"],
        ),
        matches,
        tuple(totals_by_kind["generations"]),
    )


def _accumulate_recipe_page(batch, totals_by_field):
    totals_by_field["membership_rows"] += batch.membership_rows
    totals_by_field["omitted_rows"] += batch.omitted_rows
    if getattr(batch, "complete_raw_facts", False):
        totals_by_field["raw_source_rows"] += batch.source_rows
        totals_by_field["accepted_raw_source_rows"] += batch.accepted_source_rows
        totals_by_field["omitted_raw_source_rows"] += batch.omitted_source_rows
        if totals_by_field["raw_source_rows"] != (
            totals_by_field["accepted_raw_source_rows"] + totals_by_field["omitted_raw_source_rows"]
        ):
            raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")


def _contribution_stage_result(descriptor, artifact, recipe, approved, revision):
    if revision == 2:
        descriptor = (
            anchored_source_fact_descriptor(descriptor, artifact)
            if artifact is not None
            else (locally_owned_source_descriptor(descriptor))
        )
    reused = descriptor["fact_rows"] if artifact is not None else 0
    generation = (
        descriptor["source_generation"]
        if revision == 1
        else fhir_membership_generation(recipe.source_pin, approved, recipe.binding_coordinates, True)
    )
    return (
        generation,
        tuple(descriptor[field] for field in _STAGE_COUNTER_FIELDS),
        descriptor,
        reused,
    )


async def _stage_recipe_snapshot(
    connection, recipes, approved, tables, control_schema, read_context, stage_identity, retained_by_capture
):
    """Finish source aggregation and its temporary tables in the caller's nested transaction."""
    raw_table, page_table, fact_table = tables
    recipe_sha256, contribution_revision, inventory = stage_identity
    matches, raw_generations = None, ()
    if read_context.verified_office_inputs is not None:
        fact_table = None
    async with connection.transaction():
        await _create_source_stage_tables(connection, raw_table, fact_table)
        if read_context.verified_office_inputs is None:
            totals_by_field, contribution_receipts, generations, reused_fact_rows = await _collect_source_contributions(
                connection,
                recipes,
                approved,
                raw_table,
                control_schema,
                (fact_table, retained_by_capture, contribution_revision),
                read_context.office_authority,
                read_context.retained_fhir_reads,
            )
        else:
            totals, matches, raw_generations = await _stage_verified_retained_recipes(
                connection,
                recipes,
                approved,
                raw_table,
                control_schema,
                read_context.retained_fhir_reads,
                read_context.verified_office_inputs,
            )
            generations, *counters = totals
            totals_by_field = dict(zip(_STAGE_COUNTER_FIELDS, (*counters, 0), strict=True))
            contribution_receipts, reused_fact_rows = [], 0
        await _finish_source_stage(connection, raw_table, page_table, totals_by_field["membership_rows"])
    stage = _source_stage_result(
        page_table,
        recipe_sha256,
        approved,
        generations,
        totals_by_field,
        contribution_receipts,
        (fact_table, contribution_revision, reused_fact_rows),
        inventory,
    )
    return replace(stage, office_matches=matches, raw_source_generations=raw_generations)
