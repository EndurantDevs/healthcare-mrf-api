# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recompose retained source memberships with the current approved custom map."""

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import (
    copy_approved_membership_batch,
    pin_approved_membership_source,
)
from process.network_custom_address_source import prepare_custom_address_source
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest
from process.registry_company_approval_fence import (
    _company_approval_connection,
    _company_approval_snapshot,
    require_registry_company_approval_fence,
)
from process.registry_initial_source_composition import (
    SUMMARY_KEY,
    initial_source_office_receipt_sha256,
    prepare_initial_source_offices,
    record_initial_source_offices,
    validate_initial_source_recipes,
    verify_initial_source_office_receipt,
)
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_composition import stage_registry_source_recipes
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, resolve_retained_registry_source_recipes


class RegistryCompositionError(ValueError):
    """A retained source, approved map or immutable composition recipe changed."""


@dataclass(frozen=True)
class RegistryCompositionAddressSources:
    base_source: PinnedAddressSource
    npi_source: PinnedAddressSource | None = None
    initial_source_recipes: tuple = ()
    replacement_source_recipes: tuple = ()
    office_scope_store: object = None
    office_read_budget: object = None

    def __post_init__(self):
        if type(self.base_source) is not PinnedAddressSource or (
            self.npi_source is not None and type(self.npi_source) is not PinnedAddressSource
        ):
            raise RegistryCompositionError("registry_composition_address_sources_invalid")
        if type(self.initial_source_recipes) is not tuple or type(self.replacement_source_recipes) is not tuple:
            raise RegistryCompositionError("registry_composition_initial_sources_invalid")
        if self.initial_source_recipes and self.replacement_source_recipes:
            raise RegistryCompositionError("registry_composition_initial_sources_invalid")
        for name in ("initial_source_recipes", "replacement_source_recipes"):
            if getattr(self, name):
                object.__setattr__(self, name, validate_initial_source_recipes(getattr(self, name)))

    @property
    def introduced_source_recipes(self):
        """A complete explicitly admitted recipe set replaces the retained source set."""
        return self.initial_source_recipes or self.replacement_source_recipes


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
    namespace = _identifier(source_manifest.schema_name)
    # Imported FHIR/ACA evidence is a digest. Approved custom evidence always
    # uses the reserved prefix emitted by the approved-map reader.
    summary = await connection.fetchrow(
        f"SELECT count(*) FILTER(WHERE evidence_id ~ '^[0-9a-f]{{64}}$') AS source_rows,"
        "count(*) FILTER(WHERE evidence_id !~ '^[0-9a-f]{64}$' "
        "AND evidence_id !~ '^approved-custom:[0-9]+:[0-9a-f]{64}$') AS unknown_rows "
        f"FROM {namespace}.network_membership"
    )
    if summary["unknown_rows"]:
        raise RegistryCompositionError("registry_composition_source_evidence_unsupported")
    generations_by_source = {
        key: value
        for key, value in source_manifest.source_generations.items()
        if key not in {"custom_membership", "unified_address", "retained_network_source", SUMMARY_KEY}
        and not key.startswith("initial_source_recipe_")
    }
    generations_by_source["retained_network_source"] = source_manifest.manifest_sha256
    return generations_by_source, summary["source_rows"]


def _target(request_id, approved_revision, expected_head, generations_by_source):
    if not isinstance(request_id, UUID) or not request_id.int:
        raise RegistryCompositionError("registry_composition_request_invalid")
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


async def _bindings(connection, copy_target, artifact, source_manifest):
    namespace = _identifier(copy_target.schema_name)
    custom_namespace = _identifier(artifact.address_source.schema_name)
    await connection.execute(f"""CREATE TABLE IF NOT EXISTS {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    bindings = f"SELECT * FROM {custom_namespace}.provider_location_binding"
    if source_manifest is not None:
        retained = _identifier(source_manifest.schema_name)
        bindings += f""" UNION SELECT binding.* FROM {retained}.provider_location_binding binding
          WHERE EXISTS(SELECT 1 FROM {retained}.network_membership member
            WHERE member.evidence_id ~ '^[0-9a-f]{{64}}$'
              AND (member.provider_system,member.provider_id,member.location_id)=
                  (binding.provider_system,binding.provider_id,binding.location_id))"""
    await connection.execute(f"INSERT INTO {namespace}.provider_location_binding {bindings} ON CONFLICT DO NOTHING")
    if await connection.fetchval(f"""WITH expected AS ({bindings})
      SELECT EXISTS(SELECT 1 FROM expected LEFT JOIN {namespace}.provider_location_binding actual
        USING(provider_system,provider_id,location_id)
        WHERE (expected.location_key,expected.entity_type,expected.entity_id)
          IS DISTINCT FROM (actual.location_key,actual.entity_type,actual.entity_id))"""):
        raise RegistryCompositionError("registry_composition_binding_conflict")


async def _stage_source(connection, source_manifest, copy_target, expected_rows, control_schema):
    await _retained_source(connection, source_manifest, control_schema)
    namespace = _identifier(source_manifest.schema_name)
    stage_name = "registry_source_page_" + UUID(copy_target.candidate_id).hex
    stage = _identifier(stage_name)
    await connection.execute(f"""CREATE TEMP TABLE {stage} AS
      SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
        location_id,evidence_id) AS ordinal,to_jsonb(member)::text AS input_json
      FROM {namespace}.network_membership member WHERE evidence_id ~ '^[0-9a-f]{{64}}$'""")
    await connection.execute(f"ALTER TABLE pg_temp.{stage} ADD PRIMARY KEY(ordinal)")
    if await connection.fetchval(f"SELECT count(*) FROM pg_temp.{stage}") != expected_rows:
        raise RegistryCompositionError("registry_composition_source_accounting_invalid")
    return stage


async def _source_page(connection, stage, copy_target, offset, control_schema):
    page = await connection.fetchrow(
        f"""WITH page AS MATERIALIZED (
      SELECT ordinal,input_json FROM pg_temp.{stage} WHERE ordinal>$1 AND ordinal<=$1+$2
    ), bounds AS (
      SELECT count(*)::bigint AS rows,coalesce(sum(octet_length(input_json)+2),0)+2<=$3 AS bounded
      FROM page
    ) SELECT bounds.*,coalesce((SELECT jsonb_agg(input_json::jsonb ORDER BY ordinal)
      FROM page WHERE bounds.bounded),'[]'::jsonb)::text AS input FROM bounds""",
        offset,
        MAX_ROWS,
        MAX_INPUT_BYTES,
    )
    if not page["bounded"]:
        raise RegistryCompositionError("registry_composition_source_page_too_large")
    input_bytes = page["input"].encode()
    receipt = await admit_network_membership_batch(
        connection,
        copy_target,
        batch_id=uuid5(UUID(copy_target.candidate_id), "retained-source:" + str(offset)),
        input_bytes=input_bytes,
        expected_input_sha256=hashlib.sha256(input_bytes).hexdigest(),
        control_schema=control_schema,
    )
    if receipt.row_count != page["rows"]:
        raise RegistryCompositionError("registry_composition_source_accounting_invalid")


async def _copy_composition(
    connection, copy_target, approved, source_manifest, source_rows, control_schema, source_stage
):
    stage = _identifier(source_stage.table_name) if source_stage is not None else None
    try:
        if source_rows and stage is None:
            async with connection.transaction(isolation="repeatable_read"):
                stage = await _stage_source(connection, source_manifest, copy_target, source_rows, control_schema)
        for offset in range(0, source_rows, MAX_ROWS):
            async with connection.transaction(isolation="repeatable_read"):
                await _source_page(connection, stage, copy_target, offset, control_schema)
        for offset in range(0, approved.total_rows, MAX_ROWS):
            async with connection.transaction(isolation="repeatable_read"):
                await copy_approved_membership_batch(
                    connection, approved, copy_target, offset=offset, control_schema=control_schema
                )
        async with connection.transaction():
            await seal_network_candidate(connection, copy_target, control_schema=control_schema)
    finally:
        if stage is not None and source_stage is None:
            await connection.execute(f"DROP TABLE pg_temp.{stage}")


async def _composition_sources(
    connection, source_manifest, approved, request_id, control_schema, initial_recipes=(), office_authority=None
):
    generations_by_source, source_rows = await _retained_source(connection, source_manifest, control_schema)
    if not initial_recipes and (source_manifest is None or RECIPE_DIGEST_KEY not in source_manifest.source_generations):
        return generations_by_source, source_rows, (), None
    recipes = initial_recipes or await resolve_retained_registry_source_recipes(
        connection, source_manifest, control_schema=control_schema
    )
    source_stage = await stage_registry_source_recipes(
        connection,
        recipes,
        approved,
        request_id=request_id,
        control_schema=control_schema,
        office_authority=office_authority,
    )
    if initial_recipes:
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


async def _composition_artifact(
    connection, address_sources, request_id, approved, writer_roles, control_schema, office_authority=None
):
    artifact = await prepare_custom_address_source(
        connection,
        address_sources.base_source,
        composition_id=str(uuid5(request_id, "address:" + approved.generation_id)),
        approved_revision=approved.approved_revision,
        owner_role=writer_roles["owner_role"],
        runtime_roles=tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"]))),
        control_schema=control_schema,
        npi_source=address_sources.npi_source,
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
        )
    return artifact, summary


async def _record_source_receipts(
    connection, copy_target, recipes, approved, source_stage, artifact, office_phase, control_schema
):
    initial_summary, office_authority = office_phase
    if source_stage is not None:
        from process.registry_source_selection_receipt import record_registry_source_selection

        await record_registry_source_selection(
            connection, copy_target, recipes, approved, source_stage, control_schema=control_schema
        )
    if initial_summary is not None:
        await record_initial_source_offices(
            connection,
            copy_target,
            recipes,
            approved,
            artifact.address_source,
            runtime_roles=artifact.runtime_roles,
            control_schema=control_schema,
            office_authority=office_authority,
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
    """Run preparation in its original raw owner or the genuine fenced session."""
    connection, office_authority = await _preparation_driver(connection, address_sources, control_schema)
    approved = await pin_approved_membership_source(
        connection, approved_revision=approved_revision, control_schema=control_schema
    )
    generations_by_source, source_rows, recipes, source_stage = await _composition_sources(
        connection,
        source_manifest,
        approved,
        request_id,
        control_schema,
        address_sources.introduced_source_recipes,
        office_authority,
    )
    artifact, initial_summary = await _composition_artifact(
        connection, address_sources, request_id, approved, writer_roles, control_schema, office_authority
    )
    generations_by_source.update(custom_membership=approved.generation_id, unified_address=artifact.generation_sha256)
    if initial_summary is not None:
        generations_by_source[SUMMARY_KEY] = initial_source_office_receipt_sha256(initial_summary)
    copy_target = _target(request_id, approved_revision, expected_head, generations_by_source)
    candidate = await create_network_candidate(
        connection,
        copy_target,
        source_generations=generations_by_source,
        approved_custom_revision=approved_revision,
        expected_head=expected_head,
        expected_rows=source_rows + approved.total_rows,
        control_schema=control_schema,
        source_recipes=recipes,
    )
    if candidate["state"] == "open":
        await _bindings(connection, copy_target, artifact, source_manifest)
        await _record_source_receipts(
            connection,
            copy_target,
            recipes,
            approved,
            source_stage,
            artifact,
            (initial_summary, office_authority),
            control_schema,
        )
    elif initial_summary is not None and verify_initial_source_office_receipt(candidate) != initial_summary:
        raise RegistryCompositionError("registry_composition_initial_sources_changed")
    return copy_target, artifact.address_source, approved, source_rows, candidate["state"], source_stage


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
    """Commit bounded native batches and seal; never change the serving manifest.

    A retained generation contributes only verified imported evidence. Its older
    custom rows are replaced by the latest approved map, including on rollback.
    Exact batch receipts make interruption and retry idempotent. An exact
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
        if connection.in_transaction() or driver.is_in_transaction():
            raise RegistryCompositionError("registry_composition_requires_phase_owned_transactions")
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        if (await connection.get_raw_connection()).driver_connection is not driver:
            raise RegistryCompositionError("registry_composition_connection_changed")
        return await _complete_composition(driver, prepared, source_manifest, control_schema)


async def _complete_composition(connection, prepared, source_manifest, control_schema):
    copy_target, address_source, approved, source_rows, state, source_stage = prepared
    original_error = None
    try:
        if state not in {"open", "sealed", "validated", "ready", "published"}:
            raise RegistryCompositionError("registry_composition_candidate_unavailable")
        if state == "open":
            await _copy_composition(
                connection, copy_target, approved, source_manifest, source_rows, control_schema, source_stage
            )
    except BaseException as error:
        original_error = error
        raise
    finally:
        if source_stage is not None:
            try:
                await connection.execute(f"DROP TABLE pg_temp.{_identifier(source_stage.table_name)}")
            except BaseException:
                if original_error is None:
                    raise
    return copy_target, address_source


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
