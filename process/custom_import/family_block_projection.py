# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Derive the existing numeric projections from a compact candidate's typed cells."""

import json

from sqlalchemy import and_, func, select, text

from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import (
    index_specifications,
    index_statements,
    projection_index_specifications,
)
from process.custom_import.family_block_scalar_layout import scalar_column
from process.custom_import.family_block_write import _lock_open_layout
from process.custom_import.query_projection_evidence import ProjectionDigest
from process.custom_import.query_projection_sources import (
    _source_fields,
    _value_relation,
    child_hash_collision_statement,
    field_slots,
)
from process.custom_import.query_projection_storage import query_projection_index_count
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields, reduction_statement

_GENERATION_COLUMNS = ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")


def _root_sources(binding, build):
    definition = binding.layout.definition
    selection = definition.query.entity_selection
    profile_slot = next(
        slot
        for slot, profile in enumerate(definition.selection_profiles, 1)
        if profile.profile_id == selection.by_value_profile
    )
    tables = binding.tables_by_role
    family, member, winner = tables["family"], tables["generation_family"], tables["winner"]
    selected = family.c[scalar_column(definition.fields_by_id[selection.field_id])]
    group = family.c[scalar_column(definition.fields_by_id[selection.group_field_id])]
    statement = (
        select(
            winner.c.entity_binding_id,
            family.c.family_revision_id,
            selected.label("selected_value"),
            group.label("group_value"),
        )
        .join(
            family,
            and_(
                family.c.family_revision_id == winner.c.family_revision_id,
                family.c.entity_binding_id == winner.c.entity_binding_id,
            ),
        )
        .join(
            member,
            and_(
                *(member.c[name] == winner.c[name] for name in _GENERATION_COLUMNS),
                member.c.root_record_id == family.c.root_record_id,
                member.c.family_revision_id == family.c.family_revision_id,
            ),
        )
        .where(
            *(winner.c[name] == getattr(build, name) for name in _GENERATION_COLUMNS),
            winner.c.profile_slot == profile_slot,
            winner.c.context_collection_slot == 0,
            winner.c.context_child_revision_id.is_(None),
            selected.is_not(None),
            group.in_(selection.group_values),
        )
    )
    # Native column checks require missing, null and wrong-kind cells to be SQL NULL.
    values_by_field = {
        field.field_id: family.c[scalar_column(field)] for field in _source_fields(definition, child=False)
    }
    return _value_relation(statement, values_by_field, "compact_projection_roots")


def _child_sources(binding, roots):
    definition = binding.layout.definition
    children = binding.tables_by_role["query_record"]
    keys = binding.tables_by_role["key_dictionary"]
    owned = roots.join(
        children,
        and_(
            children.c.family_revision_id == roots.c.family_revision_id,
            children.c.collection_slot == binding.layout.collection_slots[definition.query.child_collection],
        ),
    ).join(keys, and_(keys.c.key_rank == children.c.key_rank, keys.c.key_variant == children.c.key_variant))
    statement = select(
        roots.c.entity_binding_id,
        roots.c.selected_value,
        roots.c.group_value,
        keys.c.canonical_child_key,
        keys.c.child_key_sha256,
    ).select_from(owned)
    values_by_field = {
        field.field_id: children.c[scalar_column(field)] for field in _source_fields(definition, child=True)
    }
    return _value_relation(statement, values_by_field, "compact_projection_children")


def _child_output(binding, reduced):
    keys = binding.tables_by_role["key_dictionary"]
    joined = reduced.outerjoin(
        keys,
        and_(
            keys.c.child_key_sha256 == reduced.c.child_key_sha256,
            keys.c.canonical_child_key == reduced.c.canonical_child_key,
        ),
    )
    identity_columns = tuple(
        reduced.c[name] for name in ("entity_binding_id", "selected_value", "child_key_sha256", "canonical_child_key")
    )
    guard = (
        select(*identity_columns)
        .select_from(joined)
        .group_by(*identity_columns)
        .having(func.count(keys.c.key_rank) != 1)
    )
    rows = select(
        reduced.c.entity_binding_id,
        reduced.c.selected_value,
        keys.c.key_rank,
        keys.c.key_variant,
        *(column for column in reduced.c if column.name.startswith("field_")),
    ).select_from(joined)
    return guard, rows


async def _reject_matches(session, statement):
    await _prepare_statement(session)
    if (await session.execute(statement.limit(1))).first() is not None:
        raise FamilyBlockUnavailable("compact projection sources contain an ambiguous context or key")


async def _write_rows(session, binding, roots, values, fields, *, child):
    role = "query_child" if child else "query_root"
    target = binding.tables_by_role[role]
    if child:
        roots, values = _child_sources(binding, roots)
        await _reject_matches(session, ambiguity_statement(roots))
        await _reject_matches(session, child_hash_collision_statement(roots))
    rows = reduction_statement(
        roots, fields, values_by_field=values, field_slots=field_slots(binding.layout.definition)
    )
    if child:
        guard, rows = _child_output(binding, rows.cte("compact_projection_reduced"))
        await _reject_matches(session, guard)
    await _prepare_statement(session)
    written = await session.execute(target.insert().from_select(tuple(target.c.keys()), rows, include_defaults=False))
    return written.rowcount


def _ordered_contexts(binding, build, fields, *, child):
    tables = binding.tables_by_role
    target = tables["query_child" if child else "query_root"]
    entity = tables["entity_binding"]
    canonical = entity.c.canonical_value.collate("C")
    adapter = json.loads(binding.layout.definition.schema_canonical)["root"]["entity"]["adapter"]
    owned = target.join(
        entity,
        and_(
            entity.c.entity_binding_id == target.c.entity_binding_id,
            entity.c.dataset_id == build.dataset_id,
            entity.c.adapter_id == adapter,
        ),
    )
    identity = (canonical, target.c.selected_value)
    if child:
        keys = tables["key_dictionary"]
        owned = owned.join(keys, and_(keys.c.key_rank == target.c.key_rank, keys.c.key_variant == target.c.key_variant))
        identity += (keys.c.canonical_child_key,)
    slots = field_slots(binding.layout.definition)
    cells = (target.c[f"field_{slots[field.field_id]}"] for field in fields)
    return select(*identity, *cells).select_from(owned).order_by(*identity)


async def _add_contexts(session, binding, build, fields, digest, *, child):
    await _prepare_statement(session)
    rows = await session.stream(_ordered_contexts(binding, build, fields, child=child))
    field_ids = tuple(field.field_id for field in fields)
    add, size = (digest.add_child, 3) if child else (digest.add_root, 2)
    try:
        async for row in rows.yield_per(4096):
            add(*row[:size], dict(zip(field_ids, row[size:], strict=True)))
    finally:
        await rows.close()


def _complete_manifest(binding, build, digest, counts):
    state = digest.checkpoint()
    evidence = digest.finish()
    if counts != (evidence.root_count, evidence.child_count):
        raise FamilyBlockUnavailable("compact projection digest population differs from its inserted rows")
    definition = binding.layout.definition
    return CustomImportQueryProjection(
        **{
            name: getattr(build, name)
            for name in (*_GENERATION_COLUMNS, "execution_id", "producing_fence", "producing_token_sha256")
        },
        family_id=binding.family_id,
        phase="complete",
        complete_at=func.statement_timestamp(),
        definition_sha256=bytes.fromhex(definition.digest),
        capability_sha256=evidence.capability_sha256,
        index_count=query_projection_index_count(definition),
        root_count=evidence.root_count,
        child_count=evidence.child_count,
        verified_root_count=evidence.root_count,
        verified_child_count=evidence.child_count,
        digested_root_count=evidence.root_count,
        digested_child_count=evidence.child_count,
        digest_chain_sha256=state.chain_sha256,
        digest_after_entity=state.canonical_entity,
        digest_after_selected_value=state.selected_value,
        digest_after_child_key=state.canonical_child_key,
        projection_sha256=evidence.projection_sha256,
    )


async def _create_indexes(session, binding, specifications):
    for statement in index_statements(binding.family_id, binding.layout, specifications):
        await _prepare_statement(session)
        await session.execute(text(statement))


async def write_block_projections(session_factory, request, build_id, binding, generation_id):
    """Create the derived rows and bind the existing projection evidence in the output phase."""
    definition = binding.layout.definition
    fields = configured_fields(definition)
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        if build.phase != "output" or build.generation_id != generation_id:
            raise FamilyBlockUnavailable("compact projections require the current output generation")
        if not fields:
            await _create_indexes(session, binding, index_specifications(binding.family_id, binding.layout))
            return
        lookups = projection_index_specifications(binding.family_id, binding.layout)
        await _create_indexes(session, binding, lookups)
        roots, values_by_field = _root_sources(binding, build)
        await _reject_matches(session, ambiguity_statement(roots))
        scopes = tuple(
            tuple(field for field in fields if (field.collection is not None) == child) for child in (False, True)
        )
        counts = tuple(
            [
                await _write_rows(session, binding, roots, values_by_field, scope_fields, child=bool(index))
                if scope_fields
                else 0
                for index, scope_fields in enumerate(scopes)
            ]
        )
        indexes = index_specifications(binding.family_id, binding.layout)
        await _create_indexes(session, binding, (spec for spec in indexes if spec not in lookups))
        digest = ProjectionDigest(definition)
        for child, scope_fields in zip((False, True), scopes, strict=True):
            if scope_fields:
                await _add_contexts(session, binding, build, scope_fields, digest, child=child)
        session.add(_complete_manifest(binding, build, digest, counts))
