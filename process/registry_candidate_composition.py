# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recompose retained source memberships with the current approved custom map."""

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import (
    pin_approved_membership_source,
)
from process.network_custom_address_source import (
    CustomAddressSourceArguments,
    CustomAddressSourceReceipt,
    _artifact_dependency_identity,
    _require_transaction,
    _source_identity,
    prepare_recipe_address_source,
    verify_custom_address_source,
)
from process.network_em_provider_inputs import (
    EMProviderInputs,
    prepare_em_provider_inputs,
    record_em_provider_inputs,
    resolve_em_provider_inputs,
)
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
from process.registry_company_approval_fence import (
    _company_approval_connection,
    _company_approval_snapshot,
    require_registry_company_approval_fence,
)
from process.registry_initial_source_composition import (
    SUMMARY_KEY,
    initial_source_office_receipt_sha256,
    prepare_initial_source_offices,
    record_initial_source_offices_with_reads,
    validate_initial_source_recipes,
    verify_initial_source_office_receipt,
)
from process.registry_membership_composition_stage import (
    build_registry_source_retention,
    materialize_registry_composition,
    stage_approved_membership_commands,
)
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_composition import (
    RegistrySourceRecipeStage,
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


class RegistryCompositionError(ValueError):
    """A retained source, approved map or immutable composition recipe changed."""


@dataclass(frozen=True)
class RegistryRetainedAddressArtifact:
    """An independently closed base reused without creating a loading-owner copy."""

    address_source: PinnedAddressSource
    runtime_roles: tuple[str, ...]
    generation_sha256: str
    original_source: PinnedAddressSource


def _retained_address_artifact(source, runtime_roles):
    """Keep the source pin while giving non-digest labels a canonical receipt identity."""
    generation = source.generation_id
    if len(generation) != 64 or any(character not in "0123456789abcdef" for character in generation):
        generation = hashlib.sha256(
            json.dumps(asdict(source), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    return RegistryRetainedAddressArtifact(replace(source, generation_id=generation), runtime_roles, generation, source)


@dataclass(frozen=True)
class RegistryCompositionAddressSources:
    base_source: PinnedAddressSource
    npi_source: PinnedAddressSource | None = None
    initial_source_recipes: tuple = ()
    replacement_source_recipes: tuple = ()
    office_scope_store: object = None
    office_read_budget: object = None
    em_provider_inputs: EMProviderInputs | None = None
    prepared_address_artifact: CustomAddressSourceReceipt | None = None

    def __post_init__(self):
        if type(self.base_source) is not PinnedAddressSource or (
            self.npi_source is not None and type(self.npi_source) is not PinnedAddressSource
        ):
            raise RegistryCompositionError("registry_composition_address_sources_invalid")
        if type(self.initial_source_recipes) is not tuple or type(self.replacement_source_recipes) is not tuple:
            raise RegistryCompositionError("registry_composition_initial_sources_invalid")
        if self.em_provider_inputs is not None and type(self.em_provider_inputs) is not EMProviderInputs:
            raise RegistryCompositionError("registry_composition_em_provider_inputs_invalid")
        if (
            self.prepared_address_artifact is not None
            and type(self.prepared_address_artifact) is not CustomAddressSourceReceipt
        ):
            raise RegistryCompositionError("registry_composition_address_artifact_invalid")
        if self.initial_source_recipes and self.replacement_source_recipes:
            raise RegistryCompositionError("registry_composition_initial_sources_invalid")
        for name in ("initial_source_recipes", "replacement_source_recipes"):
            if getattr(self, name):
                object.__setattr__(self, name, validate_initial_source_recipes(getattr(self, name)))

    @property
    def introduced_source_recipes(self):
        """A complete explicitly admitted recipe set replaces the retained source set."""
        return self.initial_source_recipes or self.replacement_source_recipes


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
        connection, generation_id=source_manifest.generation_id, control_schema=control_schema
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
            SUMMARY_KEY,
            RETENTION_KEY,
        }
        and not key.startswith("initial_source_recipe_")
    }
    generations_by_source["retained_network_source"] = source_manifest.manifest_sha256
    return generations_by_source, summary["source_rows"]


def _target(request_id, approved_revision, expected_head, generations_by_source, initial_summary=None):
    if not isinstance(request_id, UUID) or not request_id.int:
        raise RegistryCompositionError("registry_composition_request_invalid")
    if initial_summary is not None:
        generations_by_source[SUMMARY_KEY] = initial_source_office_receipt_sha256(initial_summary)
    recipe = json.dumps(
        [approved_revision, expected_head, generations_by_source], sort_keys=True, separators=(",", ":")
    )
    candidate_id = uuid5(request_id, "registry-composition:" + recipe)
    return MembershipCopyTarget(
        str(uuid5(request_id, "dataset")),
        str(uuid5(request_id, "schema")),
        str(uuid5(request_id, "producer")),
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )


async def _bindings(connection, copy_target, artifact, source_manifest, control_schema):
    namespace = _identifier(copy_target.schema_name)
    custom_namespace = _identifier(artifact.address_source.schema_name)
    await connection.execute(f"""CREATE TABLE IF NOT EXISTS {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    binding_relation = f"{custom_namespace}.provider_location_binding"
    if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", binding_relation):
        bindings = f"SELECT * FROM {binding_relation}"
    else:
        bindings = (
            "SELECT NULL::text AS provider_system,NULL::text AS provider_id,NULL::uuid AS location_id,"
            "NULL::varchar(64) AS location_key,NULL::text AS entity_type,NULL::text AS entity_id WHERE false"
        )
    if source_manifest is not None:
        retained = _identifier(source_manifest.schema_name)
        relation, predicate, _ = await _retained_memberships(connection, source_manifest, control_schema)
        bindings += f""" UNION SELECT binding.* FROM {retained}.provider_location_binding binding
          WHERE EXISTS(SELECT 1 FROM {relation} member
            WHERE {predicate}
              AND (member.provider_system,member.provider_id,member.location_id)=
                  (binding.provider_system,binding.provider_id,binding.location_id))"""
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
    async with connection.transaction(isolation="repeatable_read"):
        await retain_registry_source_memberships(connection, copy_target, source_stage, control_schema=control_schema)
        await materialize_registry_composition(connection, copy_target, command_stage, control_schema=control_schema)
        await seal_network_candidate(
            connection,
            copy_target,
            control_schema=control_schema,
            source_stage=source_stage,
            command_stage=command_stage,
        )
        closure = await freeze_network_candidate_writers(
            connection, copy_target, **writer_roles, control_schema=control_schema
        )
        await _retain_closure(connection, copy_target, _namespace(control_schema)[1:-1], closure)


async def _require_introduced_offices(connection, recipe_sets, approved, control_schema, office_authority):
    from process.registry_ptg_office_selection import require_selected_ptg_office_recipes

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
    )


async def _selected_source_recipes(connection, retained, approved, source_manifest, options):
    """Historical source rollback retains its source; ordinary publication follows reviewed binding deltas."""
    control_schema, office_authority, expected_head = options
    if source_manifest is not None and expected_head is not None and source_manifest.generation_id != expected_head:
        return retained
    from process.registry_ptg_office_selection import resolve_ptg_office_recipe_selection

    return await resolve_ptg_office_recipe_selection(
        connection,
        retained,
        approved.approved_revision,
        source_manifest,
        control_schema=control_schema,
        office_authority=office_authority,
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
    office_authority, retained_fhir_reads = office_phase
    generations_by_source, source_rows = await _retained_source(connection, source_manifest, control_schema)
    retained, recipes = await _composition_source_recipes(
        connection, source_manifest, approved, initial_recipes, control_schema, office_authority, expected_head
    )
    has_selection_changed = canonical_registry_source_recipes(recipes) != canonical_registry_source_recipes(retained)
    if (
        not recipes
        and not has_selection_changed
        and (source_manifest is None or RECIPE_DIGEST_KEY not in source_manifest.source_generations)
    ):
        source_stage = await _stage_source(connection, source_manifest, request_id, source_rows, control_schema)
        return generations_by_source, source_rows, (), source_stage
    if initial_recipes:
        await _require_introduced_offices(connection, (retained, recipes), approved, control_schema, office_authority)
    source_stage = await stage_registry_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        office_authority=office_authority,
        retained_fhir_reads=retained_fhir_reads,
    )
    if initial_recipes or has_selection_changed:
        generations_by_source.pop(RECIPE_DIGEST_KEY, None)
    generations_by_source["registry_source_membership"] = source_stage.generation_sha256
    if initial_recipes:
        generations_by_source.update(
            {
                f"initial_source_recipe_{index}": generation
                for index, generation in enumerate(source_stage.source_generations)
            }
        )
    return generations_by_source, source_stage.membership_rows, recipes, source_stage


async def _composition_source_recipes(
    connection, source_manifest, approved, introduced, control_schema, office_authority, expected_head
):
    """Resolve only exact recipe metadata before any source rows or address artifact are staged."""
    from process.registry_ptg_office_selection import require_ptg_startup_selection

    retained = (
        await resolve_retained_registry_source_recipes(connection, source_manifest, control_schema=control_schema)
        if source_manifest is not None and RECIPE_DIGEST_KEY in source_manifest.source_generations
        else ()
    )
    selected = await _selected_source_recipes(
        connection, retained, approved, source_manifest, (control_schema, office_authority, expected_head)
    )
    if introduced:
        require_ptg_startup_selection(introduced, selected)
    return retained, introduced or selected


def _address_arguments(request_id, approved, owner_role, runtime_roles, address_sources, control_schema):
    """Keep the same deterministic address recipe across independent preparation and consumption."""
    return CustomAddressSourceArguments(
        composition_id=str(uuid5(request_id, "address:" + approved.generation_id)),
        approved_revision=approved.approved_revision,
        owner_role=owner_role,
        runtime_roles=runtime_roles,
        control_schema=control_schema,
        npi_source=address_sources.npi_source,
    )


@dataclass(frozen=True)
class RegistryAddressArtifactCustody:
    """Fixed owner and runtime census supplied by the qualified independent custodian."""

    owner_role: str
    runtime_roles: tuple[str, ...]
    control_schema: str

    def __post_init__(self):
        _identifier(self.owner_role)
        _namespace(self.control_schema)
        if (
            type(self.runtime_roles) is not tuple
            or not 1 <= len(self.runtime_roles) <= 32
            or any(type(role) is not str for role in self.runtime_roles)
            or tuple(sorted(set(self.runtime_roles))) != self.runtime_roles
            or self.owner_role in self.runtime_roles
        ):
            raise RegistryCompositionError("registry_composition_address_artifact_roles_differ")
        for role in self.runtime_roles:
            _identifier(role)


def _retained_fhir_only(recipes):
    """Recognize the existing no-custom path without treating an empty recipe set as FHIR."""
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource

    return bool(recipes) and all(
        type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.retained_capture is not None
        for recipe in recipes
    )


async def _verify_retained_address_sources(connection, sources, runtime_roles, control_schema):
    """Keep optional authenticated NPI custody even when no NPI-derived row is constructed."""
    await _artifact_dependency_identity(
        connection, sources.base_source, runtime_roles, address=True, control_schema=control_schema
    )
    if sources.npi_source is not None:
        await _artifact_dependency_identity(connection, sources.npi_source, runtime_roles)


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
    """Prepare one sealed artifact in the independent custodian's caller-owned transaction.

    The trusted caller supplies source selection and the runtime census. Execution
    replays the exact recipe after commit; publication authority is checked separately.
    """
    if (
        type(request_id) is not UUID
        or not request_id.int
        or type(address_sources) is not RegistryCompositionAddressSources
        or address_sources.prepared_address_artifact is not None
        or type(custody) is not RegistryAddressArtifactCustody
    ):
        raise RegistryCompositionError("registry_composition_address_artifact_invalid")
    await _require_transaction(connection)
    actor = await connection.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,rolcanlogin,rolsuper,rolcreatedb,"
        "rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=session_user"
    )
    if (
        not actor
        or actor["login"] != actor["actor"]
        or not actor["rolcanlogin"]
        or any(actor[name] for name in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"))
    ):
        raise RegistryCompositionError("registry_composition_artifact_custodian_invalid")
    _require_source_recipe_phase(address_sources, source_manifest, expected_head)
    approved = await pin_approved_membership_source(
        connection, approved_revision=approved_revision, control_schema=custody.control_schema
    )
    _, recipes = await _composition_source_recipes(
        connection,
        source_manifest,
        approved,
        address_sources.introduced_source_recipes,
        custody.control_schema,
        office_authority,
        expected_head,
    )
    if approved.total_rows == 0 and _retained_fhir_only(recipes):
        await _verify_retained_address_sources(
            connection, address_sources, custody.runtime_roles, custody.control_schema
        )
        return None
    arguments = _address_arguments(
        request_id, approved, custody.owner_role, custody.runtime_roles, address_sources, custody.control_schema
    )
    return await prepare_recipe_address_source(
        connection, address_sources.base_source, arguments, (recipes, office_authority)
    )


async def _prepared_address_artifact(
    connection, receipt, address_sources, request_id, approved, writer_roles, control_schema, office_inputs
):
    """Read only: require the exact independently sealed recipe and complete executing-role closure."""
    actor = await connection.fetchval("SELECT session_user::text")
    required_roles = {
        actor,
        writer_roles["owner_role"],
        *writer_roles["loader_roles"],
        *writer_roles["reader_roles"],
    }
    if not required_roles <= set(receipt.runtime_roles) or receipt.owner_role in required_roles:
        raise RegistryCompositionError("registry_composition_address_artifact_roles_differ")
    await verify_custom_address_source(connection, receipt)
    arguments = _address_arguments(
        request_id, approved, receipt.owner_role, receipt.runtime_roles, address_sources, control_schema
    )
    if (
        receipt.composition_id != arguments.composition_id
        or receipt.approved_revision != arguments.approved_revision
        or receipt.control_schema != arguments.control_schema
        or receipt.base_source != address_sources.base_source
        or arguments.npi_source is not None
        and receipt.npi_source != arguments.npi_source
    ):
        raise RegistryCompositionError("registry_composition_address_artifact_changed")
    artifact = await prepare_recipe_address_source(connection, address_sources.base_source, arguments, office_inputs)
    if artifact != receipt:
        raise RegistryCompositionError("registry_composition_address_artifact_changed")
    return artifact


async def _composition_artifact(
    connection,
    address_sources,
    request_id,
    approved,
    writer_roles,
    control_schema,
    office_authority=None,
    source_recipes=(),
):
    """Keep standalone address preparation's original recipe and authority arguments."""
    return await _composition_artifact_with_reads(
        connection,
        address_sources,
        request_id,
        approved,
        writer_roles,
        control_schema,
        office_authority,
        (source_recipes, None),
    )


async def _composition_artifact_with_reads(
    connection,
    address_sources,
    request_id,
    approved,
    writer_roles,
    control_schema,
    office_authority,
    source_phase,
):
    source_recipes, retained_fhir_reads = source_phase
    arguments = _address_arguments(
        request_id,
        approved,
        writer_roles["owner_role"],
        tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"]))),
        address_sources,
        control_schema,
    )
    if address_sources.prepared_address_artifact is not None:
        artifact = await _prepared_address_artifact(
            connection,
            address_sources.prepared_address_artifact,
            address_sources,
            request_id,
            approved,
            writer_roles,
            control_schema,
            (source_recipes, office_authority),
        )
    elif approved.total_rows == 0 and _retained_fhir_only(source_recipes):
        await _verify_retained_address_sources(connection, address_sources, arguments.runtime_roles, control_schema)
        artifact = _retained_address_artifact(address_sources.base_source, arguments.runtime_roles)
    else:
        artifact = await prepare_recipe_address_source(
            connection,
            address_sources.base_source,
            arguments,
            (source_recipes, office_authority),
        )
    summary = None
    if address_sources.introduced_source_recipes:
        summary = await prepare_initial_source_offices(
            connection,
            address_sources.introduced_source_recipes,
            approved,
            artifact.address_source,
            runtime_roles=artifact.runtime_roles,
            control_schema=control_schema,
            office_authority=office_authority,
            retained_fhir_reads=retained_fhir_reads,
        )
    return artifact, summary


async def _record_source_receipts(
    connection, copy_target, recipes, approved, source_stage, artifact, office_phase, control_schema
):
    initial_summary, office_authority, retained_fhir_reads = office_phase
    if recipes:
        from process.registry_source_selection_receipt import record_registry_source_selection

        await record_registry_source_selection(
            connection, copy_target, recipes, approved, source_stage, control_schema=control_schema
        )
    if initial_summary is not None:
        await record_initial_source_offices_with_reads(
            connection,
            copy_target,
            recipes,
            approved,
            artifact.address_source,
            runtime_roles=artifact.runtime_roles,
            control_schema=control_schema,
            office_phase=(office_authority, retained_fhir_reads),
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
    return driver, (connection, address_sources.office_scope_store, address_sources.office_read_budget)


async def _composition_retention(connection, approved, request_id, source_stage, control_schema):
    command_stage = await stage_approved_membership_commands(
        connection, approved, request_id=request_id, control_schema=control_schema
    )
    return command_stage, await build_registry_source_retention(connection, source_stage, command_stage)


async def _composition_candidate(
    connection,
    copy_target,
    generations_by_source,
    approved_revision,
    expected_head,
    source_metadata,
    control_schema,
):
    retention, recipes, provider_receipt, initial_summary = source_metadata
    candidate = await create_network_candidate(
        connection,
        copy_target,
        source_generations=generations_by_source,
        approved_custom_revision=approved_revision,
        expected_head=expected_head,
        expected_rows=retention["effective_rows"],
        control_schema=control_schema,
        source_recipes=recipes,
        source_retention=retention,
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


def _bind_prepared_generations(generations_by_source, approved, artifact, provider_receipt):
    """Bind current custom and prepared dependency proofs before deriving candidate identity."""
    generations_by_source.update(custom_membership=approved.generation_id, unified_address=artifact.generation_sha256)
    if type(artifact) is RegistryRetainedAddressArtifact and artifact.original_source != artifact.address_source:
        generations_by_source["retained_unified_address"] = artifact.original_source.generation_id
    if provider_receipt is not None:
        generations_by_source["em_provider_inputs"] = provider_receipt["receipt_sha256"]


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
    """Prepare exact raw source and commands under the caller-owned snapshot."""
    connection, office_context = await _preparation_driver(connection, address_sources, control_schema)
    provider_receipt = await _em_provider_receipt(
        connection, address_sources, source_manifest, writer_roles, control_schema
    )
    approved = await pin_approved_membership_source(
        connection, approved_revision=approved_revision, control_schema=control_schema
    )
    retained_reads_by_capture = {}
    recipe_inputs = address_sources.introduced_source_recipes
    office_phase = office_context, retained_reads_by_capture
    generations_by_source, source_rows, recipes, source_stage = await _composition_sources_with_reads(
        connection, source_manifest, approved, request_id, control_schema, recipe_inputs, office_phase, expected_head
    )
    command_stage, retention = await _composition_retention(
        connection, approved, request_id, source_stage, control_schema
    )
    generations_by_source[RETENTION_KEY] = registry_source_retention_sha256(retention)
    artifact, initial_summary = await _composition_artifact_with_reads(
        connection,
        address_sources,
        request_id,
        approved,
        writer_roles,
        control_schema,
        office_context,
        (recipes, retained_reads_by_capture),
    )
    _bind_prepared_generations(generations_by_source, approved, artifact, provider_receipt)
    copy_target = _target(request_id, approved_revision, expected_head, generations_by_source, initial_summary)
    candidate = await _composition_candidate(
        connection,
        copy_target,
        generations_by_source,
        approved_revision,
        expected_head,
        (retention, recipes, provider_receipt, initial_summary),
        control_schema,
    )
    if candidate["state"] == "open":
        await _bindings(connection, copy_target, artifact, source_manifest, control_schema)
        office_receipt_phase = initial_summary, office_context, retained_reads_by_capture
        await _record_source_receipts(
            connection, copy_target, recipes, approved, source_stage, artifact, office_receipt_phase, control_schema
        )
    return copy_target, artifact.address_source, candidate["state"], source_stage, command_stage, writer_roles


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
    for name in (source_stage.table_name, command_stage.add_table, command_stage.remove_table):
        try:
            await connection.execute(f"DROP TABLE IF EXISTS pg_temp.{_identifier(name)}")
        except BaseException as error:
            cleanup_error = cleanup_error or error
    if original_error is None and cleanup_error is not None:
        raise cleanup_error


async def _complete_composition(connection, prepared, source_manifest, control_schema):
    copy_target, address_source, _prepared_state, source_stage, command_stage, writer_roles = prepared
    original_error = None
    try:
        if _prepared_state not in {"open", "sealed", "validated", "ready", "published"}:
            raise RegistryCompositionError("registry_composition_candidate_unavailable")
        async with connection.transaction(isolation="repeatable_read"):
            candidate = await _locked_candidate(connection, copy_target, _namespace(control_schema))
            if candidate["state"] not in {"open", "sealed", "validated", "ready", "published"}:
                raise RegistryCompositionError("registry_composition_candidate_unavailable")
            if candidate["state"] == "open":
                await _copy_composition(
                    connection, copy_target, control_schema, source_stage, command_stage, writer_roles
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


def _require_source_recipe_phase(address_sources, source_manifest, expected_head):
    if source_manifest is not None and type(source_manifest) is not PinnedNetworkServingManifest:
        raise RegistryCompositionError("registry_composition_source_invalid")
    if address_sources.initial_source_recipes and (
        source_manifest is not None or type(expected_head) is not int or expected_head != 0
    ):
        raise RegistryCompositionError("registry_composition_initial_sources_invalid")
    if address_sources.replacement_source_recipes and (
        type(source_manifest) is not PinnedNetworkServingManifest
        or type(expected_head) is not int
        or not 0 < expected_head < 2**63
        or source_manifest.generation_id != expected_head
    ):
        raise RegistryCompositionError("registry_composition_replacement_sources_invalid")
    if source_manifest is not None and not address_sources.replacement_source_recipes:
        retained_address = PinnedAddressSource(
            source_manifest.schema_name, "entity_address_unified", source_manifest.manifest_sha256
        )
        if address_sources.base_source != retained_address:
            raise RegistryCompositionError("registry_composition_source_address_mismatch")


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
