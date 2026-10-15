# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Derived contexts inside the legacy runner's real locked graph transaction."""

from sqlalchemy import func, select, text, tuple_

from db.models.custom_import import CustomImportWinner
from process.custom_import.legacy_family_store import _PAGE_BYTES
from process.custom_import.legacy_generation_store import _generation_arguments
from process.custom_import.materialization_store import _PAGE_ROWS, _authority, _call, verify_materialization_authority
from process.custom_import.query_projection_binding import validate_projection_indexes
from process.custom_import.query_projection_digest import _ordered_contexts
from process.custom_import.query_projection_evidence import ProjectionDigest, completed_projection_evidence
from process.custom_import.query_projection_initialize import initialize_projection_parent, projection_parent
from process.custom_import.query_projection_pages import cell_insert_statement, context_key_statement
from process.custom_import.query_projection_sources import (
    ProjectionSource,
    child_hash_collision_statement,
    child_source,
    field_slots,
    root_source,
)
from process.custom_import.query_projection_storage import query_projection_index_statements, query_projection_tables
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields, reduction_statement
from process.custom_import.query_projection_verify import _comparison_statement
from process.custom_import.runner_registry import prepare_materialization_statement
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_models


async def _prepare(session):
    await _authority(session)
    await prepare_materialization_statement(session)


async def _execute(session, statement):
    await _prepare(session)
    return await session.execute(statement)


async def _flush(session):
    await _prepare(session)
    await session.flush()


def _scope_fields(definition, child):
    return tuple(field for field in configured_fields(definition) if (field.collection is not None) == child)


async def _initialize(session, request, generation):
    window = await _authority(session)
    _generation_arguments(request, generation, window)
    await verify_materialization_authority(session)
    family_id = await _call(
        session, "resolve_custom_import_legacy_generation_snapshot", (("bigint", generation.generation_id),)
    )
    if type(family_id) is not int or not 0 < family_id < 2**63:
        raise CandidateRunnerError("legacy projection snapshot binding is malformed")
    expected_by_name = {
        name: getattr(generation, name)
        for name in (
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            "producing_fence",
        )
    }
    parent, root_relation = await projection_parent(
        session, request.definition, expected_by_name, bytes(generation.producing_token_sha256), prepare=_prepare
    )
    if parent.family_id != family_id:
        raise CandidateRunnerError("legacy projection snapshot resolver differs")
    manifest = await initialize_projection_parent(
        session, request.definition, parent, root_relation, prepare=_prepare, flush=_flush
    )
    if manifest.phase != "root" or manifest.root_count or manifest.child_count or manifest.after_entity_binding_id:
        raise CandidateRunnerError("legacy projection refuses retained partial contexts")
    return manifest


def _source_context(request, generation, family_id):
    selection = request.definition.query.entity_selection
    profile_slot = next(
        slot
        for slot, profile in enumerate(request.definition.selection_profiles, 1)
        if profile.profile_id == selection.by_value_profile
    )
    return ProjectionSource(
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        generation.generation_id,
        profile_slot,
        request.definition,
        snapshot_models(family_id),
    )


async def _entity_window(session, context, after):
    winner = context.model(CustomImportWinner)
    statement = (
        select(winner.entity_binding_id)
        .where(
            winner.generation_id == context.generation_id,
            winner.dataset_id == context.dataset_id,
            winner.definition_revision_id == context.definition_revision_id,
            winner.schema_revision_id == context.schema_revision_id,
            winner.profile_slot == context.profile_slot,
            winner.entity_binding_id > after,
        )
        .distinct()
        .order_by(winner.entity_binding_id)
        .limit(_PAGE_ROWS)
    )
    return (await _execute(session, statement)).scalars().all()


async def _validated_source(session, context, registry, entity_ids, *, child):
    roots, values_by_field = root_source(context, entity_ids)
    if (await _execute(session, ambiguity_statement(roots).limit(1))).first() is not None:
        raise CandidateRunnerError("legacy projection root context is ambiguous")
    if not child:
        return roots, values_by_field
    source_relation, values_by_field = child_source(
        context, roots, registry.child_collection_slots[context.definition.query.child_collection]
    )
    for statement in (ambiguity_statement(source_relation), child_hash_collision_statement(source_relation)):
        if (await _execute(session, statement.limit(1))).first() is not None:
            raise CandidateRunnerError("legacy projection complete child key is ambiguous")
    return source_relation, values_by_field


def _accepted_keys(metadata):
    accepted_keys, used = [], 0
    for context_row in metadata:
        if context_row[-1] > _PAGE_BYTES and not accepted_keys:
            raise CandidateRunnerError("one legacy projection context exceeds its byte page")
        if used + context_row[-1] > _PAGE_BYTES:
            break
        accepted_keys.append(tuple(context_row[:-1]))
        used += context_row[-1]
    return accepted_keys


async def _context_keys(session, source_relation, field_count, *, child, after):
    page = context_key_statement(source_relation, child=child, after=after, limit=_PAGE_ROWS).cte()
    identities = [page.c.entity_binding_id, page.c.selected_value]
    if child:
        identities.append(page.c.child_key_sha256)
    size = 64 + 40 * field_count
    if child:
        size += func.octet_length(page.c.canonical_child_key)
    metadata = (await _execute(session, select(*identities, size).order_by(*identities))).all()
    accepted_keys = _accepted_keys(metadata)
    if not child or not accepted_keys:
        return accepted_keys
    statement = select(page).where(tuple_(*identities).in_(accepted_keys)).order_by(*identities)
    return [tuple(context_row) for context_row in (await _execute(session, statement)).all()]


async def _write_window(session, context, manifest, entity_ids, registry, fields, *, child):
    source_relation, values_by_field = await _validated_source(session, context, registry, entity_ids, child=child)
    table = query_projection_tables(manifest.family_id, context.definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    after_keys = None
    while True:
        keys = await _context_keys(session, source_relation, len(fields), child=child, after=after_keys)
        if not keys:
            return
        insert_result = await _execute(
            session,
            cell_insert_statement(
                table,
                source_relation,
                values_by_field=values_by_field,
                fields=fields,
                field_slots=field_slots(context.definition),
                keys=keys,
            ),
        )
        if insert_result.rowcount != len(keys):
            raise CandidateRunnerError("legacy projection context coverage differs")
        count_name = "child_count" if child else "root_count"
        setattr(manifest, count_name, getattr(manifest, count_name) + len(keys))
        after_keys = tuple(keys[-1][: 3 if child else 2])


async def _write_scope(session, context, manifest, registry, *, child):
    fields = _scope_fields(context.definition, child)
    after_entity = 0
    if not fields:
        return
    while entity_ids := await _entity_window(session, context, after_entity):
        await _write_window(session, context, manifest, entity_ids, registry, fields, child=child)
        after_entity = entity_ids[-1]


async def _verify_scope(session, context, manifest, registry, *, child):
    fields = _scope_fields(context.definition, child)
    table = query_projection_tables(manifest.family_id, context.definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    expected_count, after_entity = 0, 0
    if fields:
        while entity_ids := await _entity_window(session, context, after_entity):
            source_relation, values_by_field = await _validated_source(
                session, context, registry, entity_ids, child=child
            )
            expected = reduction_statement(
                source_relation, fields, values_by_field=values_by_field, field_slots=field_slots(context.definition)
            )
            comparison = (await _execute(session, _comparison_statement(expected, table, entity_ids))).one()
            if comparison.differs or comparison.expected_count != comparison.actual_count:
                raise CandidateRunnerError("legacy projection persisted values differ from finalized reducers")
            expected_count += comparison.expected_count
            after_entity = entity_ids[-1]
    actual_count = (await _execute(session, select(func.count()).select_from(table))).scalar_one()
    count_name = "child_count" if child else "root_count"
    if actual_count != expected_count or actual_count != getattr(manifest, count_name):
        raise CandidateRunnerError("legacy projection verification accounting differs")
    setattr(manifest, f"verified_{count_name}", expected_count)


async def _digest_rows(session, context, manifest, *, child, after):
    table = query_projection_tables(manifest.family_id, context.definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    page = _ordered_contexts(context, table, child=child, after=after, limit=_PAGE_ROWS)
    identities = [page.c.entity_binding_id, page.c.selected_value]
    ordered_columns = [page.c.canonical_entity, page.c.selected_value]
    size = func.octet_length(page.c.canonical_entity) + 64 + 40 * len(configured_fields(context.definition))
    if child:
        identities.append(page.c.child_key_sha256)
        ordered_columns.append(page.c.canonical_child_key)
        size += func.octet_length(page.c.canonical_child_key)
    metadata = (await _execute(session, select(*identities, size).order_by(*ordered_columns))).all()
    accepted_keys = _accepted_keys(metadata)
    if not accepted_keys:
        return []
    return (
        (await _execute(session, select(page).where(tuple_(*identities).in_(accepted_keys)).order_by(*ordered_columns)))
        .mappings()
        .all()
    )


async def _digest_scope(session, context, manifest, digest, *, child):
    fields = _scope_fields(context.definition, child)
    if not fields:
        return
    slots = field_slots(context.definition)
    after = None
    while context_rows := await _digest_rows(session, context, manifest, child=child, after=after):
        for context_row in context_rows:
            values_by_field = {field.field_id: context_row[f"field_{slots[field.field_id]}"] for field in fields}
            if child:
                digest.add_child(
                    context_row["canonical_entity"],
                    context_row["selected_value"],
                    context_row["canonical_child_key"],
                    values_by_field,
                )
            else:
                digest.add_root(context_row["canonical_entity"], context_row["selected_value"], values_by_field)
        last = context_rows[-1]
        after = (last["canonical_entity"], last["selected_value"])
        if child:
            after += (last["canonical_child_key"],)


async def _complete(session, request, generation, context, manifest):
    digest = ProjectionDigest(request.definition)
    for child, phase in ((False, "digest_root"), (True, "digest_child")):
        manifest.phase = phase
        await _digest_scope(session, context, manifest, digest, child=child)
    evidence = digest.finish()
    state = digest.checkpoint()
    if (evidence.root_count, evidence.child_count) != (manifest.root_count, manifest.child_count):
        raise CandidateRunnerError("legacy projection semantic context coverage differs")
    manifest.digested_root_count, manifest.digested_child_count = evidence.root_count, evidence.child_count
    manifest.digest_chain_sha256 = state.chain_sha256
    manifest.digest_after_entity = state.canonical_entity
    manifest.digest_after_selected_value = state.selected_value
    manifest.digest_after_child_key = state.canonical_child_key
    manifest.projection_sha256, manifest.phase = evidence.projection_sha256, "complete"
    manifest.complete_at = func.clock_timestamp()
    await _flush(session)
    await _prepare(session)
    await session.refresh(manifest, attribute_names=["complete_at"])
    if completed_projection_evidence(manifest, generation=generation, definition=request.definition) != evidence:
        raise CandidateRunnerError("legacy projection completed evidence differs")
    await verify_materialization_authority(session)
    return evidence


async def persist_legacy_query_projection(session, request, registry, generation):
    """Append and qualify derived rows without committing or fabricating authority."""

    if not configured_fields(request.definition):
        return None
    manifest = await _initialize(session, request, generation)
    context = _source_context(request, generation, manifest.family_id)
    for child, phase in ((False, "root"), (True, "child")):
        manifest.phase = phase
        await _write_scope(session, context, manifest, registry, child=child)
    manifest.phase = "indexes"
    for statement in query_projection_index_statements(manifest.family_id, request.definition):
        await _execute(session, text(statement))
        manifest.index_count += 1
    await validate_projection_indexes(session, manifest, request.definition, prepare=_prepare)
    for child, phase in ((False, "verify_root"), (True, "verify_child")):
        manifest.phase = phase
        await _verify_scope(session, context, manifest, registry, child=child)
    return await _complete(session, request, generation, context, manifest)
