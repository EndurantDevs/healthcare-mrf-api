# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare exact composition address dependencies without owning candidate lifecycle."""

import hashlib
import json
from dataclasses import asdict, dataclass, replace
from uuid import UUID, uuid5

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_custom_address_source import (
    CustomAddressSourceArguments,
    CustomAddressSourceReceipt,
    _artifact_dependency_identity,
    _dependency_actors_closed,
    _require_transaction,
    prepare_recipe_address_source,
    verify_custom_address_source,
)
from process.network_em_provider_inputs import EMProviderInputs
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_initial_source_composition import (
    OfficeSummaryRequest,
    prepare_initial_source_offices,
    prepare_source_offices,
    validate_initial_source_recipes,
)
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_composition import SourceRecipeReadContext


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
    compact: bool = False
    prepared_address_artifact: CustomAddressSourceReceipt | None = None

    def __post_init__(self):
        if type(self.compact) is not bool:
            raise RegistryCompositionError("registry_composition_address_sources_invalid")
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
        if self.compact and self.prepared_address_artifact is not None:
            raise RegistryCompositionError("registry_composition_address_artifact_invalid")
        if self.initial_source_recipes and self.replacement_source_recipes:
            raise RegistryCompositionError("registry_composition_initial_sources_invalid")
        for name in ("initial_source_recipes", "replacement_source_recipes"):
            if getattr(self, name):
                recipes = getattr(self, name)
                recipes = (
                    validate_initial_source_recipes(recipes, inventory=True)
                    if self.compact
                    else validate_initial_source_recipes(recipes)
                )
                object.__setattr__(self, name, recipes)

    @property
    def introduced_source_recipes(self):
        """A complete explicitly admitted recipe set replaces the retained source set."""
        return self.initial_source_recipes or self.replacement_source_recipes


async def _composition_artifact(
    connection,
    address_sources,
    request_id,
    approved,
    writer_roles,
    control_schema,
    read_context=None,
    source_context=((), None, 1),
):
    read_context = SourceRecipeReadContext() if read_context is None else read_context
    source_recipes, source_manifest, contribution_revision = source_context[:3]
    office_revision = source_context[3] if len(source_context) >= 4 else None
    inventory_preparation = source_context[4] if len(source_context) >= 5 else None
    verified_stage = source_context[5] if len(source_context) >= 6 else None
    preservation = source_context[6] if len(source_context) >= 7 else None
    summary_by_field = {"source_manifest": source_manifest, "revision": office_revision}
    if inventory_preparation is not None:
        summary_by_field["derivation_sink"] = inventory_preparation.add_derivation
    if address_sources.compact:
        from process.registry_compact_composition import (
            prepare_compact_composition_source,
        )

        options = {} if verified_stage is None else {"verified_stage": verified_stage}
        return await prepare_compact_composition_source(
            connection,
            address_sources,
            approved,
            writer_roles,
            source_recipes,
            OfficeSummaryRequest(
                tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"]))),
                control_schema,
                read_context.office_authority,
                **summary_by_field,
            ),
            **options,
        )
    return await _composition_artifact_with_reads(
        connection,
        address_sources,
        request_id,
        approved,
        writer_roles,
        control_schema,
        read_context.office_authority,
        (
            source_recipes,
            read_context.retained_fhir_reads,
            read_context.office_matches,
            source_manifest,
            contribution_revision,
            office_revision,
            preservation,
        ),
    )


async def _composition_office_summary(
    connection,
    address_sources,
    approved,
    artifact,
    runtime_roles,
    control_schema,
    office_authority,
    source_phase,
):
    """Build the receipt from the same verified context or unchanged strict initial source path."""
    source_recipes, retained_fhir_reads, *verified = source_phase
    verified_matches = verified[0] if verified else None
    source_manifest = verified[1] if len(verified) > 1 else None
    contribution_revision = verified[2] if len(verified) > 2 else 1
    office_revision = verified[3] if len(verified) > 3 else None
    summary = None
    if verified_matches is not None:
        from process.registry_initial_source_composition import (
            verified_retained_source_office_summary,
        )

        summary = await verified_retained_source_office_summary(
            connection,
            source_recipes,
            approved,
            artifact.address_source,
            runtime_roles,
            control_schema,
            verified_matches,
        )
    elif source_recipes and contribution_revision == 2:
        summary = await prepare_source_offices(
            connection,
            source_recipes,
            approved,
            artifact.address_source,
            OfficeSummaryRequest(
                runtime_roles,
                control_schema,
                office_authority,
                source_manifest=source_manifest,
                revision=office_revision,
            ),
        )
    elif address_sources.introduced_source_recipes:
        summary = await prepare_initial_source_offices(
            connection,
            address_sources.introduced_source_recipes,
            approved,
            artifact.address_source,
            retained_fhir_reads=retained_fhir_reads,
            request=OfficeSummaryRequest(
                runtime_roles=artifact.runtime_roles, control_schema=control_schema, office_authority=office_authority
            ),
        )
    return summary


def _verified_office_inputs(address_sources, writer_roles):
    """Use only an authenticated retained base when no independent artifact is supplied."""
    if address_sources.prepared_address_artifact is not None:
        return None
    return (
        address_sources.base_source
        if address_sources.compact
        else _retained_address_artifact(address_sources.base_source, ()).address_source,
        tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"]))),
    )


async def _prepare_inventory_artifact(
    inputs,
    request_scope,
    address_sources,
    writer_roles,
    recipes_and_stage,
    control_schema,
    *,
    office_phase,
    artifact_builder,
):
    """Complete SOURCE and office metadata before its immutable candidate identity is registered."""
    request_id, _expected_head = request_scope
    phase_artifact, office_revision = office_phase
    recipes, source_stage = recipes_and_stage
    preparation, is_witness_stage, office_revision = _prepare_source_inventory(recipes, source_stage, office_revision)
    if phase_artifact is not None:
        artifact, initial_summary = await _reuse_composition_artifact(
            inputs, address_sources, phase_artifact, recipes, source_stage, office_revision, control_schema
        )
    else:
        artifact, initial_summary = await artifact_builder(
            inputs.connection,
            address_sources,
            request_id,
            inputs.approved,
            writer_roles,
            control_schema,
            SourceRecipeReadContext(
                office_authority=inputs.office_context,
                retained_fhir_reads=inputs.retained_fhir_reads,
                office_matches=source_stage.office_matches,
            ),
            (
                recipes,
                inputs.source_manifest,
                source_stage.contribution_revision,
                office_revision,
                preparation,
                (source_stage.selected_stage or source_stage) if is_witness_stage else None,
                source_stage.preservation,
            ),
        )
    return _finish_inventory_artifact(preparation, recipes, inputs, artifact, initial_summary, source_stage)


def _inventory_office_summary(root, entries, approved, artifact, stage):
    """Fold the exact independently bound office version into the registered inventory."""
    from process.registry_source_inventory_summaries import office_summary_v4, office_summary_v5

    derivations = sorted(
        (json.loads(entry.office_derivation_json) for entry in entries), key=lambda row: row["recipe_sha256"]
    )
    if getattr(stage, "office_policy_context", None) is not None:
        return office_summary_v5(root, stage.office_policy_context, derivations)
    return office_summary_v4(root, approved, artifact.address_source, derivations, artifact.origin_manifest_sha256)


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
        retained_address = (
            PinnedAddressSource(
                source_manifest.compact_layout.base.schema_name,
                "entity_address_unified",
                source_manifest.source_generations["unified_address"],
            )
            if address_sources.compact and source_manifest.compact_layout is not None
            else PinnedAddressSource(
                source_manifest.schema_name,
                "entity_address_unified",
                source_manifest.manifest_sha256,
            )
        )
        if address_sources.base_source != retained_address:
            raise RegistryCompositionError("registry_composition_source_address_mismatch")


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
    address_reader_roles: tuple[str, ...] | None = None

    def __post_init__(self):
        _identifier(self.owner_role)
        _namespace(self.control_schema)
        if (
            type(self.runtime_roles) is not tuple
            or not 1 <= len(self.runtime_roles) <= 128
            or any(type(role) is not str for role in self.runtime_roles)
            or tuple(sorted(set(self.runtime_roles))) != self.runtime_roles
            or self.owner_role in self.runtime_roles
        ):
            raise RegistryCompositionError("registry_composition_address_artifact_roles_differ")
        for role in self.runtime_roles:
            _identifier(role)
        if self.address_reader_roles is not None:
            from process.network_address_artifact import _roles

            _roles(self.address_reader_roles)
            if not set(self.address_reader_roles) <= set(self.runtime_roles):
                raise RegistryCompositionError("registry_composition_address_artifact_roles_differ")


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
        connection,
        sources.base_source,
        runtime_roles,
        address=True,
        control_schema=control_schema,
    )
    if sources.npi_source is not None:
        await _artifact_dependency_identity(connection, sources.npi_source, runtime_roles)


async def _prepare_registry_composition_address_artifact(
    connection,
    *,
    request_scope,
    address_sources,
    source_manifest,
    expected_head,
    custody,
    office_authority=None,
    select_source_recipes,
):
    """Prepare one sealed artifact in the independent custodian's caller-owned transaction.

    The trusted caller supplies source selection and the runtime census. Execution
    replays the exact recipe after commit; publication authority is checked separately.
    """
    request_id, approved_revision = request_scope
    if (
        type(request_id) is not UUID
        or not request_id.int
        or type(address_sources) is not RegistryCompositionAddressSources
        or address_sources.prepared_address_artifact is not None
        or type(custody) is not RegistryAddressArtifactCustody
    ):
        raise RegistryCompositionError("registry_composition_address_artifact_invalid")
    await _require_transaction(connection)
    await _require_artifact_custodian(connection)
    _require_source_recipe_phase(address_sources, source_manifest, expected_head)
    approved = await pin_approved_membership_source(
        connection,
        approved_revision=approved_revision,
        control_schema=custody.control_schema,
    )
    _, recipes = await select_source_recipes(
        connection,
        source_manifest,
        approved,
        address_sources.introduced_source_recipes,
        custody.control_schema,
        office_authority,
        (expected_head, request_id),
        inventory=address_sources.compact,
    )
    if address_sources.compact:
        await _verify_compact_preparation_origin(connection, address_sources, source_manifest, custody)
        return None
    return await _prepare_selected_address_artifact(
        connection,
        (request_id, expected_head),
        source_manifest,
        recipes,
        approved,
        address_sources,
        custody,
        office_authority,
    )


async def _prepared_address_artifact(
    connection,
    receipt,
    address_sources,
    request_id,
    approved,
    writer_roles,
    control_schema,
    office_inputs,
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
        request_id,
        approved,
        receipt.owner_role,
        receipt.runtime_roles,
        address_sources,
        control_schema,
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
    """Keep retained office mapping in the same preparation snapshot and scope."""
    source_recipes = source_phase[0]
    preservation = source_phase[6] if len(source_phase) > 6 else None
    artifact = await _prepare_composition_artifact(
        connection,
        address_sources,
        request_id,
        approved,
        writer_roles,
        control_schema,
        office_authority,
        (source_recipes, preservation),
    )
    summary = await _composition_office_summary(
        connection,
        address_sources,
        approved,
        artifact,
        artifact.runtime_roles,
        control_schema,
        office_authority,
        source_phase,
    )
    return artifact, summary


async def _prepare_composition_artifact(
    connection, address_sources, request_id, approved, writer_roles, control_schema, office_authority, source_phase
):
    """Construct the exact address dependency before legacy retained-office pruning."""
    source_recipes = source_phase[0]
    preservation = source_phase[1]
    office_inputs = (source_recipes, office_authority)
    if preservation is not None:
        office_inputs += (preservation,)
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
            office_inputs,
        )
    elif (
        approved.total_rows == 0
        and _retained_fhir_only(source_recipes)
        and (preservation is None or not preservation.receipt["carried_rows"])
    ):
        await _verify_retained_address_sources(connection, address_sources, arguments.runtime_roles, control_schema)
        artifact = _retained_address_artifact(address_sources.base_source, arguments.runtime_roles)
    else:
        artifact = await prepare_recipe_address_source(
            connection,
            address_sources.base_source,
            arguments,
            office_inputs,
        )
    return artifact


async def _verify_compact_preparation_origin(connection, sources, source_manifest, custody):
    """Authenticate reader intent separately from the complete output-actor closure."""
    from process.network_compact_address_origin import resolve_compact_address_origin

    if custody.address_reader_roles is None:
        raise RegistryCompositionError("registry_composition_address_artifact_roles_differ")
    generations_by_source = {"unified_address": sources.base_source.generation_id}
    if source_manifest is not None:
        generations_by_source["retained_network_source"] = source_manifest.manifest_sha256
    _descriptor, physical = await resolve_compact_address_origin(
        connection,
        {"source_generations": generations_by_source},
        sources.base_source,
        runtime_roles=custody.address_reader_roles,
        control_schema=custody.control_schema,
    )
    dependency = replace(
        sources.base_source,
        schema_name=physical.schema_name,
        table_name=physical.table_name,
    )
    await _dependency_actors_closed(connection, dependency, custody.runtime_roles)
    if sources.npi_source is not None:
        await _artifact_dependency_identity(connection, sources.npi_source, custody.runtime_roles)


async def _prepare_preservation(connection, source_manifest, recipes, approved_revision, scope, *, inventory=False):
    """Keep both independent preparation paths on the same public native derivation."""
    if source_manifest is None:
        return None
    from process.registry_fhir_serving_preservation import prepare_fhir_serving_preservation

    request_id, expected_head, control_schema = scope
    return await prepare_fhir_serving_preservation(
        connection,
        source_manifest,
        recipes,
        request_id=request_id,
        expected_head=expected_head,
        approved_revision=approved_revision,
        control_schema=control_schema,
        inventory=inventory,
    )


def _prepare_source_inventory(recipes, source_stage, office_revision):
    preparation = None
    is_witness_stage = getattr(source_stage, "office_policy_context", None) is not None
    if source_stage.inventory:
        selected_revision = 5 if is_witness_stage else 4
        if office_revision not in (None, selected_revision if recipes else 1):
            raise RegistryCompositionError("registry_composition_initial_sources_changed")
        preparation = SourceInventoryPreparation(recipes)
        for descriptor in source_stage.contribution_receipts:
            preparation.add_contribution(descriptor)
        if is_witness_stage:
            for derivation in source_stage.office_derivations:
                preparation.add_derivation(derivation)
        office_revision = selected_revision
    return preparation, is_witness_stage, office_revision


async def _require_artifact_custodian(connection):
    actor = await connection.fetchrow(
        "SELECT session_user::text AS login,current_user::text AS actor,rolcanlogin,rolsuper,rolcreatedb,"
        "rolcreaterole,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=session_user"
    )
    if (
        not actor
        or actor["login"] != actor["actor"]
        or not actor["rolcanlogin"]
        or any(
            actor[name]
            for name in (
                "rolsuper",
                "rolcreatedb",
                "rolcreaterole",
                "rolreplication",
                "rolbypassrls",
            )
        )
    ):
        raise RegistryCompositionError("registry_composition_artifact_custodian_invalid")


def _finish_inventory_artifact(preparation, recipes, inputs, artifact, initial_summary, source_stage):
    inventory, entries = None, ()
    if preparation is not None:
        inventory, entries = preparation.finish()
        if recipes:
            initial_summary = _inventory_office_summary(
                inventory.root, entries, inputs.approved, artifact, source_stage
            )
    return artifact, initial_summary, inventory, entries


async def _prepare_selected_address_artifact(
    connection, request_scope, source_manifest, recipes, approved, address_sources, custody, office_authority
):
    request_id, expected_head = request_scope
    preservation = await _prepare_preservation(
        connection,
        source_manifest,
        recipes,
        approved.approved_revision,
        (request_id, expected_head, custody.control_schema),
    )
    if (
        approved.total_rows == 0
        and _retained_fhir_only(recipes)
        and (preservation is None or not preservation.receipt["carried_rows"])
    ):
        await _verify_retained_address_sources(
            connection, address_sources, custody.runtime_roles, custody.control_schema
        )
        return None
    arguments = _address_arguments(
        request_id,
        approved,
        custody.owner_role,
        custody.runtime_roles,
        address_sources,
        custody.control_schema,
    )
    office_inputs = (recipes, office_authority)
    if preservation is not None:
        office_inputs += (preservation,)
    return await prepare_recipe_address_source(connection, address_sources.base_source, arguments, office_inputs)


async def _reuse_composition_artifact(
    inputs, address_sources, phase_artifact, recipes, source_stage, office_revision, control_schema
):
    if source_stage.inventory or source_stage.office_policy_context is not None:
        raise RegistryCompositionError("registry_composition_address_artifact_changed")
    artifact = phase_artifact
    initial_summary = await _composition_office_summary(
        inputs.connection,
        address_sources,
        inputs.approved,
        artifact,
        artifact.runtime_roles,
        control_schema,
        inputs.office_context,
        (
            recipes,
            inputs.retained_fhir_reads,
            source_stage.office_matches,
            inputs.source_manifest,
            source_stage.contribution_revision,
            office_revision,
        ),
    )
    return artifact, initial_summary
