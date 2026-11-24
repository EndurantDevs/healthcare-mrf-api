# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Select compact winners from canonical context keys and typed query cells."""

from sqlalchemy import BigInteger, LargeBinary, and_, func, literal, select

from db.models.custom_import import CustomImportBuildCandidateContext
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_scalar_layout import scalar_column
from process.custom_import.family_block_write import _lock_open_layout
from process.custom_import.materialization import _profile_scopes
from process.custom_import.storage_layout import snapshot_tables


def _context_table(binding):
    return snapshot_tables(binding.family_id)[CustomImportBuildCandidateContext.__tablename__]


def _selection_order(definition, profile, family, child):
    for term in profile.selection_terms:
        field = definition.fields_by_id[term.field_id]
        table = family if field.collection is None else child
        value = table.c[scalar_column(field)]
        if field.value_type == "string":
            value = value.collate("C")
        ordered = value.desc() if term.direction == "desc" else value.asc()
        yield ordered.nulls_first() if term.nulls == "first" else ordered.nulls_last()


def _winner_sources(binding, build, contexts, scope):
    tables = binding.tables_by_role
    family, member = tables["family"], tables["generation_family"]
    owned = contexts.join(
        family,
        and_(
            family.c.family_revision_id == contexts.c.family_revision_id,
            family.c.entity_binding_id == contexts.c.entity_binding_id,
        ),
    ).join(
        member,
        and_(
            member.c.generation_id == build.generation_id,
            member.c.dataset_id == build.dataset_id,
            member.c.definition_revision_id == build.definition_revision_id,
            member.c.schema_revision_id == build.schema_revision_id,
            member.c.root_record_id == family.c.root_record_id,
            member.c.family_revision_id == family.c.family_revision_id,
        ),
    )
    if scope.collection_slot == 0:
        return owned, family, None, literal(None, LargeBinary)
    child, dictionary = tables["query_record"], tables["key_dictionary"]
    owned = owned.join(
        child,
        and_(
            child.c.family_revision_id == family.c.family_revision_id,
            child.c.child_revision_id == contexts.c.context_child_revision_id,
            child.c.collection_slot == contexts.c.context_collection_slot,
        ),
    ).join(dictionary, and_(dictionary.c.key_rank == child.c.key_rank, dictionary.c.key_variant == child.c.key_variant))
    return owned, family, child, dictionary.c.child_key_sha256


def _insert_profile_winners(binding, build, contexts, slot, profile, scope):
    definition = binding.layout.definition
    owned, family, child, child_hash = _winner_sources(binding, build, contexts, scope)
    partition = (contexts.c.entity_binding_id, contexts.c.context_key_sha256)
    ordering = (
        *_selection_order(definition, profile, family, child),
        family.c.family_sha256,
        *((child_hash,) if child is not None else ()),
    )
    canonical = contexts.c.canonical_context_key.collate("C")
    ranked = (
        select(
            contexts.c.entity_binding_id,
            contexts.c.family_revision_id,
            contexts.c.context_collection_slot,
            contexts.c.context_key_sha256,
            contexts.c.context_child_revision_id,
            child_hash.label("context_child_key_sha256"),
            func.count().over(partition_by=partition).label("candidate_count"),
            func.rank().over(partition_by=partition, order_by=ordering).label("preference_rank"),
            func.min(canonical).over(partition_by=partition).label("first_context"),
            func.max(canonical).over(partition_by=partition).label("last_context"),
        )
        .select_from(owned)
        .where(
            contexts.c.build_id == build.build_id,
            contexts.c.profile_slot == slot,
            contexts.c.context_collection_slot == scope.collection_slot,
        )
        .subquery("ranked_block_contexts")
    )
    winner = binding.tables_by_role["winner"]
    ids = (build.generation_id, build.dataset_id, build.definition_revision_id, build.schema_revision_id, slot)
    columns = tuple(winner.c.keys())
    selected_rows = select(
        *(literal(identity, BigInteger) for identity in ids),
        *(ranked.c[name] for name in columns[len(ids) :]),
    ).where(ranked.c.preference_rank == 1, ranked.c.first_context == ranked.c.last_context)
    # Returning every top tie intentionally preserves the existing winner uniqueness check.
    return winner.insert().from_select(columns, selected_rows, include_defaults=False)


async def _count(session, table, *predicates):
    await _prepare_statement(session)
    return await session.scalar(select(func.count()).select_from(table).where(*predicates))


async def _finish_counts(session, build, contexts, expected):
    count = await _count(session, contexts, contexts.c.build_id == build.build_id)
    if count != expected:
        raise FamilyBlockUnavailable("compact winner contexts differ from the exact profile populations")
    keys = (contexts.c.profile_slot, contexts.c.entity_binding_id, contexts.c.context_key_sha256)
    await _prepare_statement(session)
    last = (
        await session.execute(
            select(*keys).where(contexts.c.build_id == build.build_id).order_by(*(key.desc() for key in keys)).limit(1)
        )
    ).one_or_none()
    build.candidate_context_count = count
    if last is not None:
        build.output_after_profile_slot, build.output_after_entity_binding_id, build.output_after_context_key_sha256 = (
            last
        )


async def _materialize_winners(session, build, binding):
    definition = binding.layout.definition
    scopes = _profile_scopes(definition, binding.layout.collection_slots)
    contexts = _context_table(binding)
    winner = binding.tables_by_role["winner"]
    child_count = (
        await _count(session, binding.tables_by_role["query_record"])
        if any(scope.collection_slot for scope in scopes)
        else 0
    )
    expected_contexts = winner_count = 0
    for slot, (profile, scope) in enumerate(zip(definition.selection_profiles, scopes, strict=True), 1):
        expected = build.generation_family_count if scope.collection_slot == 0 else child_count
        expected_contexts += expected
        await _prepare_statement(session)
        await session.execute(_insert_profile_winners(binding, build, contexts, slot, profile, scope))
        await _prepare_statement(session)
        selected, population = (
            await session.execute(
                select(func.count(), func.coalesce(func.sum(winner.c.candidate_count), 0)).where(
                    winner.c.generation_id == build.generation_id, winner.c.profile_slot == slot
                )
            )
        ).one()
        if population != expected:
            raise FamilyBlockUnavailable("compact winner contexts are incomplete or have a digest collision")
        winner_count += selected
    await _finish_counts(session, build, contexts, expected_contexts)
    build.winner_count = winner_count


async def write_block_winners(session_factory, request, build_id, binding):
    """Select exact profile winners in one authorized OUTPUT transaction."""
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        if build.phase != "output" or build.winner_count or build.output_after_profile_slot is not None:
            raise FamilyBlockUnavailable("compact winners require a fresh output generation")
        if build.generation_family_count != build.selected_family_count:
            raise FamilyBlockUnavailable("compact winners require complete generation membership")
        await _materialize_winners(session, build, binding)
