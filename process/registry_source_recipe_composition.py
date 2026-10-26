# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replay sealed source editions through the current approved network bindings."""

import asyncio
import hashlib
import io
import json
from dataclasses import dataclass
from uuid import UUID, uuid5

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    PinnedFHIRMembershipSource,
    read_fhir_membership_batch,
    read_retained_fhir_membership_batch,
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
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from process.uhc_flex_practitioner_async_safety import drain_operation


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


async def _read_recipe_page(connection, recipe, approved, cursor, control_schema, limit, office_source=None):
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
                if recipe.source_pin.retained_capture is not None:
                    batch = await read_retained_fhir_membership_batch(
                        connection,
                        recipe.source_pin,
                        office_source,
                        registry_schema=control_schema,
                        after_resource=cursor,
                        limit=limit,
                        approved_source=approved,
                        binding_coordinates=recipe.binding_coordinates,
                    )
                else:
                    batch = await read_fhir_membership_batch(
                        connection, recipe.source_pin, after_resource=cursor, limit=limit, **options_by_name
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
    office_authority=None,
    aca_reads_by_oid=None,
    retained_fhir_reads=None,
):
    office_source = await _require_recipe_custody(
        connection,
        recipe,
        approved,
        control_schema,
        office_authority,
        aca_reads_by_oid,
        retained_fhir_reads=retained_fhir_reads,
    )
    cursor, generation = None, None
    limit = 1000
    membership_rows = omitted_rows = 0
    raw_source_rows = accepted_raw_source_rows = omitted_raw_source_rows = 0
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection, recipe, approved, cursor, control_schema, limit, office_source
        )
        if generation is not None and generation != batch.generation_id:
            raise RegistrySourceCompositionError("registry_source_composition_generation_changed")
        generation = batch.generation_id
        await _copy_recipe_page(connection, batch, raw_table)
        membership_rows += batch.membership_rows
        omitted_rows += batch.omitted_rows
        if getattr(batch, "complete_raw_facts", False):
            raw_source_rows += batch.source_rows
            accepted_raw_source_rows += batch.accepted_source_rows
            omitted_raw_source_rows += batch.omitted_source_rows
            if raw_source_rows != accepted_raw_source_rows + omitted_raw_source_rows:
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
        if not batch.source_rows:
            if batch.membership_rows or batch.omitted_rows:
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
            return (
                generation,
                membership_rows,
                omitted_rows,
                raw_source_rows,
                accepted_raw_source_rows,
                omitted_raw_source_rows,
            )
        if next_cursor is None or next_cursor == cursor:
            raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
        cursor = next_cursor


async def _require_recipe_custody(
    connection,
    recipe,
    approved=None,
    control_schema=None,
    office_authority=None,
    aca_reads_by_oid=None,
    *,
    retained_fhir_reads=None,
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
    if source_pin.retained_capture is not None:
        return await _retained_recipe_read(connection, source_pin.retained_capture, retained_fhir_reads)
    if source_pin.retained_epoch is not None:
        try:
            await require_retained_cms_fhir_source_epoch(connection, source_pin, source_pin.retained_epoch)
            return
        except FHIRSourceEpochError as error:
            raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from error
    if source_pin.custody_owner_role is None:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable")
    custody = RetainedCMSFHIRSourceCustody(
        source_pin,
        source_pin.custody_owner_role,
        source_pin.custody_runtime_roles,
        source_pin.custody_proof_sha256,
        source_pin.custody_catalog_sha256,
    )
    try:
        await require_retained_cms_fhir_source_custody(connection, custody)
    except FHIRSourceCustodyError as error:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from error


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


async def stage_registry_source_recipes(
    connection, recipes, approved, *, request_id, control_schema, office_authority=None, retained_fhir_reads=None
):
    """Stage bounded native pages in one repeatable transaction; never publish.

    Missing approved mappings are counted by the reviewed reader. Malformed
    selected evidence rejects the complete stage. The caller removes the returned
    temporary page table after candidate admission, including interruptions.
    """
    if (
        not connection.is_in_transaction()
        or type(approved) is not ApprovedMembershipSource
        or type(request_id) is not UUID
        or not request_id.int
        or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
    ):
        raise RegistrySourceCompositionError("registry_source_composition_scope_invalid")
    recipe_sha256 = registry_source_recipes_sha256(canonical_registry_source_recipes(recipes))
    control_schema = registry_schema() if control_schema is None else control_schema
    _identifier(control_schema)
    stage_id = uuid5(request_id, recipe_sha256 + ":" + approved.generation_id).hex
    raw_table = "registry_recipe_raw_" + stage_id
    page_table = "registry_source_page_" + stage_id
    retained_fhir_reads = {} if retained_fhir_reads is None else retained_fhir_reads
    async with connection.transaction():
        await connection.execute(f"""CREATE TEMP TABLE {_identifier(raw_table)}(
          network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
          location_id uuid NOT NULL,evidence_id text NOT NULL)""")
        totals = await _stage_recipe_totals(
            connection, recipes, approved, raw_table, control_schema, office_authority, retained_fhir_reads
        )
        (
            generations,
            membership_rows,
            omitted_rows,
            raw_source_rows,
            accepted_raw_source_rows,
            omitted_raw_source_rows,
        ) = totals
        await _finish_source_stage(connection, raw_table, page_table, membership_rows)
    generation_sha256 = _source_stage_generation(recipe_sha256, approved, generations, membership_rows, omitted_rows)
    return RegistrySourceRecipeStage(
        page_table,
        membership_rows,
        omitted_rows,
        generation_sha256,
        tuple(generations),
        raw_source_rows,
        accepted_raw_source_rows,
        omitted_raw_source_rows,
    )


async def _stage_recipe_totals(
    connection, recipes, approved, raw_table, control_schema, office_authority, retained_reads
):
    """Keep complete raw accounting unchanged across every admitted recipe."""
    generations = []
    membership_rows = omitted_rows = 0
    raw_source_rows = accepted_raw_source_rows = omitted_raw_source_rows = 0
    aca_reads_by_oid = {}
    for recipe in recipes:
        generation, row_count, omitted_count, source_count, accepted_count, omitted_source_count = await _stage_recipe(
            connection, recipe, approved, raw_table, control_schema, office_authority, aca_reads_by_oid, retained_reads
        )
        generations.append(generation)
        membership_rows += row_count
        omitted_rows += omitted_count
        raw_source_rows += source_count
        accepted_raw_source_rows += accepted_count
        omitted_raw_source_rows += omitted_source_count
    return (
        generations,
        membership_rows,
        omitted_rows,
        raw_source_rows,
        accepted_raw_source_rows,
        omitted_raw_source_rows,
    )


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
