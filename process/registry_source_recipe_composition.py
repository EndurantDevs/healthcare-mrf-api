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
)
from process.network_fhir_source_custody import (
    FHIRSourceCustodyError,
    RetainedCMSFHIRSourceCustody,
    require_retained_cms_fhir_source_custody,
)
from process.network_fhir_source_epoch import FHIRSourceEpochError, require_retained_cms_fhir_source_epoch
from process.network_legacy_membership_source import LegacyMembershipBatchBoundsError, read_aca_membership_batch
from process.network_membership_copy import COPY_COLUMNS, _encode
from process.registry_ptg_office_membership import (
    PTGOfficeBatchBoundsError,
    read_ptg_office_membership_batch,
    require_ptg_office_recipe,
)
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256


class RegistrySourceCompositionError(ValueError):
    """A retained source page or approved mapping cannot be safely replayed."""


@dataclass(frozen=True)
class RegistrySourceRecipeStage:
    table_name: str
    membership_rows: int
    omitted_rows: int
    generation_sha256: str
    source_generations: tuple[str, ...] = ()


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
                batch = await read_fhir_membership_batch(
                    connection, recipe.source_pin, after_resource=cursor, limit=limit, **options_by_name
                )
                return batch, batch.next_resource, limit
            batch = await read_aca_membership_batch(
                connection, recipe.source_pin, after_evidence_checksum=cursor, limit=limit, **options_by_name
            )
            return batch, batch.next_evidence_checksum, limit
        except FHIRMembershipBatchBoundsError, LegacyMembershipBatchBoundsError, PTGOfficeBatchBoundsError:
            if limit == 1:
                raise
            limit //= 2


async def _copy_recipe_page(connection, batch, raw_table):
    if batch.unresolved_rows or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256:
        raise RegistrySourceCompositionError("registry_source_composition_unresolved")
    copy_bytes, row_count = await asyncio.to_thread(_encode, batch.input_bytes)
    if row_count != batch.membership_rows:
        raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
    if row_count:
        with io.BytesIO(copy_bytes) as copy_source:
            status = await connection.copy_to_table(
                raw_table, schema_name="pg_temp", columns=COPY_COLUMNS, format="binary", source=copy_source
            )
            if status != f"COPY {row_count}" or copy_source.tell() != len(copy_bytes):
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")


async def _stage_recipe(connection, recipe, approved, raw_table, control_schema, office_authority=None):
    office_source = await _require_recipe_custody(connection, recipe, approved, control_schema, office_authority)
    cursor, generation = None, None
    limit = 1000
    membership_rows = omitted_rows = 0
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
        if not batch.source_rows:
            if batch.membership_rows or batch.omitted_rows:
                raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
            return generation, membership_rows, omitted_rows
        if next_cursor is None or next_cursor == cursor:
            raise RegistrySourceCompositionError("registry_source_composition_cursor_invalid")
        cursor = next_cursor


async def _require_recipe_custody(connection, recipe, approved=None, control_schema=None, office_authority=None):
    """Recheck CMS retained custody once before paging its complete recipe."""
    source = recipe.source_pin
    if type(source) is PinnedPTGOfficeMembershipSource:
        return await require_ptg_office_recipe(connection, recipe, approved, control_schema, office_authority)
    if type(source) is not PinnedFHIRMembershipSource:
        return
    if source.retained_epoch is not None:
        try:
            await require_retained_cms_fhir_source_epoch(connection, source, source.retained_epoch)
            return
        except FHIRSourceEpochError:
            raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from None
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
    except FHIRSourceCustodyError:
        raise RegistrySourceCompositionError("registry_source_composition_custody_unavailable") from None


async def stage_registry_source_recipes(
    connection, recipes, approved, *, request_id, control_schema, office_authority=None
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
    generations = []
    membership_rows = omitted_rows = 0
    async with connection.transaction():
        await connection.execute(f"""CREATE TEMP TABLE {_identifier(raw_table)}(
          network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
          location_id uuid NOT NULL,evidence_id text NOT NULL)""")
        for recipe in recipes:
            generation, row_count, omitted_count = await _stage_recipe(
                connection, recipe, approved, raw_table, control_schema, office_authority
            )
            generations.append(generation)
            membership_rows += row_count
            omitted_rows += omitted_count
        if await connection.fetchval(f"SELECT count(*) FROM pg_temp.{_identifier(raw_table)}") != membership_rows:
            raise RegistrySourceCompositionError("registry_source_composition_accounting_invalid")
        await connection.execute(f"""CREATE TEMP TABLE {_identifier(page_table)} AS
          SELECT row_number() OVER(ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",
            location_id,evidence_id) AS ordinal,to_jsonb(member)::text AS input_json
          FROM pg_temp.{_identifier(raw_table)} member""")
        await connection.execute(f"ALTER TABLE pg_temp.{_identifier(page_table)} ADD PRIMARY KEY(ordinal)")
        await connection.execute(f"DROP TABLE pg_temp.{_identifier(raw_table)}")
    generation_sha256 = hashlib.sha256(
        json.dumps(
            [recipe_sha256, approved.generation_id, sorted(generations), membership_rows, omitted_rows],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return RegistrySourceRecipeStage(page_table, membership_rows, omitted_rows, generation_sha256, tuple(generations))
