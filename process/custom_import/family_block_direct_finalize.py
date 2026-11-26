# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded finalize, structure verification, and atomic seal for direct compact v2 builds."""

from __future__ import annotations

import hashlib
import json
from collections import Counter

from sqlalchemy import BigInteger, LargeBinary, Sequence, and_, func, literal, select, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.orm import aliased
from sqlalchemy.schema import DropTable

from db.models.custom_import import (
    CustomImportBuildCandidateContext,
    CustomImportBuildVerification,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPack,
    CustomImportRejection,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import import build_graph as graph
from process.custom_import import build_output, compact_materialization, publication, runner_codec
from process.custom_import import build_source as staging
from process.custom_import.bulk_page_codec import MAX_BATCH_ROWS
from process.custom_import.continuation_runtime import producer_token_sha256, verify_and_bind_continuation
from process.custom_import.family_block_binding import FamilyBlockBinding, FamilyBlockUnavailable, checked_binding
from process.custom_import.family_block_direct_buckets import default_direct_limits, process_direct_buckets
from process.custom_import.family_block_direct_retention import carry_retained_families
from process.custom_import.family_block_direct_stage import (
    DIRECT_COMMITMENTS_TABLE,
    DIRECT_PROGRESS_TABLE,
    DIRECT_REJECTED_TABLE,
    DIRECT_VOLATILE_GUARD_TABLE,
    require_direct_stage_intact,
)
from process.custom_import.family_block_finalize import _role_counts
from process.custom_import.family_block_layout import _NEW, index_specifications, projection_index_specifications
from process.custom_import.family_block_projection import (
    _add_contexts,
    _child_output,
    _child_sources,
    _create_indexes,
    _reject_matches,
)
from process.custom_import.family_block_query_input import (
    query_input_table,
    write_query_dictionary,
    write_query_records,
)
from process.custom_import.family_block_read import _directory
from process.custom_import.family_block_scalar_layout import scalar_column
from process.custom_import.family_block_v2_codec import (
    needs_inline_entity_join,
    verify_inline_root_family,
    verify_v2_fragments_and_records,
)
from process.custom_import.family_block_winners import _selection_order
from process.custom_import.family_block_write import _lock_open_layout
from process.custom_import.materialization import _profile_scopes
from process.custom_import.query_projection_evidence import ProjectionDigest
from process.custom_import.query_projection_finality import bind_generation_materialization
from process.custom_import.query_projection_sources import (
    _source_fields,
    _value_relation,
    child_hash_collision_statement,
    field_slots,
)
from process.custom_import.query_projection_storage import query_projection_index_count
from process.custom_import.query_projection_values import ambiguity_statement, configured_fields, reduction_statement
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_schema, snapshot_tables

_DIALECT = dialect()
_GEN_SEQ = Sequence("custom_import_generation_generation_id_seq", schema=CustomImportGeneration.__table__.schema)
_SPOT_CHECK_LIMIT = 16
_MATERIALIZATION_PAGE_LIMIT = 50_000


async def _load_progress(session, family_id: int) -> dict[str, object]:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await staging._prepare_statement(session)
    mapping = (
        (await session.execute(text(f"SELECT * FROM {schema_sql}.{DIRECT_PROGRESS_TABLE} WHERE id = 1 FOR UPDATE")))
        .mappings()
        .one()
    )
    return dict(mapping)


async def _drop_direct_stage_tables(session, family_id: int) -> None:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    for tbl_name in (
        DIRECT_PROGRESS_TABLE,
        DIRECT_VOLATILE_GUARD_TABLE,
        DIRECT_COMMITMENTS_TABLE,
        DIRECT_REJECTED_TABLE,
    ):
        await staging._prepare_statement(session)
        await session.execute(text(f"DROP TABLE IF EXISTS {schema_sql}.{tbl_name}"))


async def _control_profiles_relation(session) -> str:
    quote = _DIALECT.identifier_preparer.format_table
    profile_table = CustomImportSelectionProfile.__table__
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(profile_table.schema, profile_table.schema)
    profile_name = quote(profile_table, use_schema=False)
    return (
        f"{_DIALECT.identifier_preparer.quote_schema(control_schema)}.{profile_name}"
        if control_schema
        else profile_name
    )


async def _build_query_projection_records(session, binding: FamilyBlockBinding) -> None:
    query_source = query_input_table(binding.family_id, binding.layout)
    dictionary = binding.tables_by_role["key_dictionary"]
    query_records = binding.tables_by_role["query_record"]
    await staging._prepare_statement(session)
    await session.execute(write_query_dictionary(dictionary, query_source))
    await staging._prepare_statement(session)
    inserted = await session.execute(write_query_records(query_records, dictionary, query_source))
    await staging._prepare_statement(session)
    expected = await session.scalar(select(func.count()).select_from(query_source))
    if inserted.rowcount != expected:
        raise FamilyBlockUnavailable("compact query records differ from the admitted population")
    await staging._prepare_statement(session)
    await session.execute(DropTable(query_source))


async def _run_f1_dictionary_and_retained(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
    *,
    limits,
) -> None:
    """Carry forward retained base families, populate query_record, and reserve generation_id."""
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    ret_n, ret_child_n, ret_root_sc, ret_child_sc, ret_slots_map = await carry_retained_families(
        session, request, build, registry, binding, limits=limits
    )
    if binding.layout.definition.query.child_collection is not None:
        await _build_query_projection_records(session, binding)
    await staging._prepare_statement(session)
    reserved_gen_id = int(await session.scalar(select(_GEN_SEQ.next_value())))
    counts_by_slot = {
        int(slot_key): int(count_val)
        for slot_key, count_val in json.loads(progress_by_col["collection_counts_json"]).items()
    }
    for slot, count_val in ret_slots_map.items():
        counts_by_slot[slot] = counts_by_slot.get(slot, 0) + count_val
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"""
            UPDATE {schema_sql}.{DIRECT_PROGRESS_TABLE}
            SET finalize_stage = 'f2',
                reserved_generation_id = :gen_id,
                retained_family_count = retained_family_count + :ret_n,
                total_child_count = total_child_count + :ret_child_n,
                root_scalar_count = root_scalar_count + :ret_root_sc,
                child_scalar_count = child_scalar_count + :ret_child_sc,
                collection_counts_json = :counts_json
            WHERE id = 1
            """
        ),
        {
            "gen_id": reserved_gen_id,
            "ret_n": ret_n,
            "ret_child_n": ret_child_n,
            "ret_root_sc": ret_root_sc,
            "ret_child_sc": ret_child_sc,
            "counts_json": json.dumps(
                {str(slot_key): count_val for slot_key, count_val in sorted(counts_by_slot.items())}
            ),
        },
    )


def _direct_winner_sources(binding: FamilyBlockBinding, contexts, scope):
    tables = binding.tables_by_role
    family = tables["family"]
    owned = contexts.join(
        family,
        and_(
            family.c.family_revision_id == contexts.c.family_revision_id,
            family.c.entity_binding_id == contexts.c.entity_binding_id,
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


def _ranked_direct_contexts_subquery(binding: FamilyBlockBinding, build, contexts, slot: int, profile_and_scope: tuple):
    profile, scope = profile_and_scope
    definition = binding.layout.definition
    owned, family, child, child_hash = _direct_winner_sources(binding, contexts, scope)
    partition = (contexts.c.entity_binding_id, contexts.c.context_key_sha256)
    ordering = (
        *_selection_order(definition, profile, family, child),
        family.c.family_sha256,
        *((child_hash,) if child is not None else ()),
    )
    canonical = contexts.c.canonical_context_key.collate("C")
    return (
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
        .subquery("ranked_direct_contexts")
    )


async def _select_profile_winners(
    session,
    build,
    binding: FamilyBlockBinding,
    contexts,
    gen_id: int,
    slot: int,
    profile_and_scope: tuple,
    expected: int,
) -> int:
    """Insert ranked winners for one selection profile and verify candidate population."""
    winner = binding.tables_by_role["winner"]
    ranked = _ranked_direct_contexts_subquery(binding, build, contexts, slot, profile_and_scope)
    ids = (gen_id, build.dataset_id, build.definition_revision_id, build.schema_revision_id, slot)
    cols = tuple(winner.c.keys())
    selected_rows = select(
        *(literal(identity, BigInteger) for identity in ids), *(ranked.c[name] for name in cols[len(ids) :])
    ).where(ranked.c.preference_rank == 1, ranked.c.first_context == ranked.c.last_context)
    await staging._prepare_statement(session)
    await session.execute(winner.insert().from_select(cols, selected_rows, include_defaults=False))
    await staging._prepare_statement(session)
    selected_n, population_n = (
        await session.execute(
            select(func.count(), func.coalesce(func.sum(winner.c.candidate_count), 0)).where(
                winner.c.generation_id == gen_id, winner.c.profile_slot == slot
            )
        )
    ).one()
    if int(population_n) != expected:
        raise FamilyBlockUnavailable("direct winner contexts are incomplete or have a digest collision")
    return int(selected_n)


async def _record_winner_progress(
    session,
    binding: FamilyBlockBinding,
    contexts,
    build_id: int,
    winner_count: int,
    expected_contexts: int,
) -> None:
    await staging._prepare_statement(session)
    actual_contexts = int(
        await session.scalar(select(func.count()).select_from(contexts).where(contexts.c.build_id == build_id))
    )
    if actual_contexts != expected_contexts:
        raise FamilyBlockUnavailable("direct winner context population differs")
    key_cols = (contexts.c.profile_slot, contexts.c.entity_binding_id, contexts.c.context_key_sha256)
    await staging._prepare_statement(session)
    last_ctx = (
        await session.execute(
            select(*key_cols)
            .where(contexts.c.build_id == build_id)
            .order_by(*(col.desc() for col in key_cols))
            .limit(1)
        )
    ).one_or_none()
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"""
            UPDATE {schema_sql}.{DIRECT_PROGRESS_TABLE}
            SET finalize_stage = 'f3',
                winner_count = :winner_count,
                candidate_context_count = :context_count,
                last_profile_slot = :last_slot,
                last_entity_binding_id = :last_entity,
                last_context_key_sha256 = :last_hash
            WHERE id = 1
            """
        ),
        {
            "winner_count": winner_count,
            "context_count": actual_contexts,
            "last_slot": None if last_ctx is None else int(last_ctx[0]),
            "last_entity": None if last_ctx is None else int(last_ctx[1]),
            "last_hash": None if last_ctx is None else bytes(last_ctx[2]),
        },
    )


async def _run_f2_winners(
    session,
    build,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
) -> None:
    """Rank candidate contexts per selection profile and write compact winner rows."""
    definition = binding.layout.definition
    gen_id = int(progress_by_col["reserved_generation_id"])
    total_families = int(progress_by_col["accepted_family_count"]) + int(progress_by_col["retained_family_count"])
    scopes = _profile_scopes(definition, binding.layout.collection_slots)
    contexts = snapshot_tables(binding.family_id)[CustomImportBuildCandidateContext.__tablename__]
    child_count = 0
    if any(scope.collection_slot for scope in scopes):
        await staging._prepare_statement(session)
        child_count = int(
            await session.scalar(select(func.count()).select_from(binding.tables_by_role["query_record"]))
        )
    expected_contexts = winner_count = 0
    for slot, (profile, scope) in enumerate(zip(definition.selection_profiles, scopes, strict=True), 1):
        expected = total_families if scope.collection_slot == 0 else child_count
        expected_contexts += expected
        winner_count += await _select_profile_winners(
            session, build, binding, contexts, gen_id, slot, (profile, scope), expected
        )
    await _record_winner_progress(session, binding, contexts, build.build_id, winner_count, expected_contexts)


def _direct_root_sources(binding: FamilyBlockBinding, build, generation_id: int):
    definition = binding.layout.definition
    selection = definition.query.entity_selection
    profile_slot = next(
        slot
        for slot, profile in enumerate(definition.selection_profiles, 1)
        if profile.profile_id == selection.by_value_profile
    )
    tables = binding.tables_by_role
    family, winner = tables["family"], tables["winner"]
    selected_col = family.c[scalar_column(definition.fields_by_id[selection.field_id])]
    group_col = family.c[scalar_column(definition.fields_by_id[selection.group_field_id])]
    stmt = (
        select(
            winner.c.entity_binding_id,
            family.c.family_revision_id,
            selected_col.label("selected_value"),
            group_col.label("group_value"),
        )
        .join(
            family,
            and_(
                family.c.family_revision_id == winner.c.family_revision_id,
                family.c.entity_binding_id == winner.c.entity_binding_id,
            ),
        )
        .where(
            winner.c.generation_id == generation_id,
            winner.c.dataset_id == build.dataset_id,
            winner.c.definition_revision_id == build.definition_revision_id,
            winner.c.schema_revision_id == build.schema_revision_id,
            winner.c.profile_slot == profile_slot,
            winner.c.context_collection_slot == 0,
            winner.c.context_child_revision_id.is_(None),
            selected_col.is_not(None),
            group_col.in_(selection.group_values),
        )
    )
    values_by_field = {
        field.field_id: family.c[scalar_column(field)] for field in _source_fields(definition, child=False)
    }
    return _value_relation(stmt, values_by_field, "direct_projection_roots")


async def _deduplicate_entity_indexes(session, binding: FamilyBlockBinding) -> None:
    """Replace LIKE-created redundant entity_binding unique indexes with one covering index."""
    entity_table = binding.tables_by_role["entity_binding"]
    quoted_table = _DIALECT.identifier_preparer.format_table(entity_table)
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"ALTER TABLE {quoted_table} "
            "DROP CONSTRAINT IF EXISTS custom_import_entity_binding_value_key, "
            "DROP CONSTRAINT IF EXISTS custom_import_entity_binding_dataset_value_key, "
            "DROP CONSTRAINT IF EXISTS custom_import_entity_binding_owner_key"
        )
    )
    await staging._prepare_statement(session)
    await session.execute(text(f"DROP INDEX IF EXISTS {schema_sql}.custom_import_entity_binding_value_key"))
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"CREATE UNIQUE INDEX {entity_table.name}_canonical_order_idx "
            f'ON {quoted_table} (dataset_id, adapter_id, canonical_value COLLATE "C") '
            "INCLUDE (entity_binding_id)"
        )
    )


async def _populate_projection_tables(
    session,
    binding: FamilyBlockBinding,
    build,
    gen_id: int,
    scopes: tuple,
) -> tuple[int, int]:
    definition = binding.layout.definition
    roots, root_values_by_field = _direct_root_sources(binding, build, gen_id)
    await _reject_matches(session, ambiguity_statement(roots))
    written_counts: list[int] = []
    for is_child_idx, scope_fields in enumerate(scopes):
        is_child = bool(is_child_idx)
        if not scope_fields:
            written_counts.append(0)
            continue
        projection_table = binding.tables_by_role["query_child" if is_child else "query_root"]
        src_rel, src_values_by_field = (roots, root_values_by_field)
        if is_child:
            src_rel, src_values_by_field = _child_sources(binding, roots)
            await _reject_matches(session, ambiguity_statement(src_rel))
            await _reject_matches(session, child_hash_collision_statement(src_rel))
        reduced_stmt = reduction_statement(
            src_rel, scope_fields, values_by_field=src_values_by_field, field_slots=field_slots(definition)
        )
        if is_child:
            guard, reduced_stmt = _child_output(binding, reduced_stmt.cte("direct_projection_reduced"))
            await _reject_matches(session, guard)
        await staging._prepare_statement(session)
        inserted = await session.execute(
            projection_table.insert().from_select(
                tuple(projection_table.c.keys()), reduced_stmt, include_defaults=False
            )
        )
        written_counts.append(int(inserted.rowcount))
    return written_counts[0], written_counts[1]


async def _digest_projection_tables(
    session,
    binding: FamilyBlockBinding,
    build,
    scopes: tuple,
    written_counts: tuple[int, int],
) -> str:
    definition = binding.layout.definition
    digest = ProjectionDigest(definition)
    for is_child, scope_fields in zip((False, True), scopes, strict=True):
        if scope_fields:
            await _add_contexts(session, binding, build, scope_fields, digest, child=is_child)
    state = digest.checkpoint()
    evidence = digest.finish()
    if written_counts != (evidence.root_count, evidence.child_count):
        raise FamilyBlockUnavailable("direct projection digest population differs from inserted rows")
    return json.dumps(
        {
            "capability_sha256": evidence.capability_sha256.hex(),
            "projection_sha256": evidence.projection_sha256.hex(),
            "index_count": query_projection_index_count(definition),
            "root_count": evidence.root_count,
            "child_count": evidence.child_count,
            "digest_chain_sha256": state.chain_sha256.hex(),
            "digest_after_entity": state.canonical_entity,
            "digest_after_selected_value": state.selected_value,
            "digest_after_child_key": state.canonical_child_key,
        }
    )


async def _run_f3_projections_and_indexes(
    session,
    build,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
) -> None:
    """Populate derived query projections and build minimal compact v2 serving indexes."""
    definition = binding.layout.definition
    gen_id = int(progress_by_col["reserved_generation_id"])
    fields = configured_fields(definition)
    projection_evidence_json: str | None = None
    if fields:
        lookups = projection_index_specifications(binding.family_id, binding.layout)
        await _create_indexes(session, binding, lookups)
        scopes = tuple(
            tuple(field for field in fields if (field.collection is not None) == is_child) for is_child in (False, True)
        )
        written_counts = await _populate_projection_tables(session, binding, build, gen_id, scopes)
        remaining_specs = [
            spec
            for spec in index_specifications(binding.family_id, binding.layout)
            if spec not in lookups and spec[0] not in ("fragment", "entity_binding")
        ]
        await _create_indexes(session, binding, remaining_specs)
        await _deduplicate_entity_indexes(session, binding)
        projection_evidence_json = await _digest_projection_tables(session, binding, build, scopes, written_counts)
    else:
        minimal_specs = [
            spec
            for spec in index_specifications(binding.family_id, binding.layout)
            if spec[0] not in ("fragment", "entity_binding")
            and (spec[0] != "query_record" or definition.query.child_collection is not None)
        ]
        await _create_indexes(session, binding, minimal_specs)
        await _deduplicate_entity_indexes(session, binding)
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"""
            UPDATE {schema_sql}.{DIRECT_PROGRESS_TABLE}
            SET finalize_stage = 'f4a',
                projection_evidence_json = :evidence_json
            WHERE id = 1
            """
        ),
        {"evidence_json": projection_evidence_json},
    )


_V2_STRUCTURE_CHECKS_TEMPLATE = """
    SELECT 'family entity scope' AS defect FROM {family} f
    WHERE NOT EXISTS (SELECT 1 FROM {entity_binding} e
                      WHERE e.entity_binding_id = f.entity_binding_id AND e.dataset_id = :dataset_id)
    UNION ALL
    SELECT 'duplicate family root' FROM {family} GROUP BY root_record_id HAVING count(*) <> 1
    UNION ALL
    SELECT 'duplicate root revision' FROM {family} GROUP BY root_revision_id HAVING count(*) <> 1
    UNION ALL
    SELECT 'orphan fragment' FROM {fragment} fr
    WHERE NOT EXISTS (SELECT 1 FROM {family} f WHERE f.family_revision_id = fr.family_revision_id)
    UNION ALL
    SELECT 'inline family fragment' FROM {family} f
    WHERE f.fragment_count = 0 AND EXISTS (
        SELECT 1 FROM {fragment} fr WHERE fr.family_revision_id = f.family_revision_id)
    UNION ALL
    SELECT 'fragment census' FROM {family} f
    LEFT JOIN (
        SELECT family_revision_id, count(*) AS population,
               min(fragment_ordinal) AS first_frag, max(fragment_ordinal) AS last_frag,
               sum(raw_bytes) AS raw_sum, sum(8 + octet_length(encoded_fragment)) AS stored_sum,
               sum(starting_record_count) AS record_sum
        FROM {fragment} GROUP BY family_revision_id
    ) c USING (family_revision_id)
    WHERE f.fragment_count > 0 AND ROW(c.population, c.first_frag, c.last_frag, c.raw_sum, c.stored_sum, c.record_sum)
          IS DISTINCT FROM ROW(f.fragment_count, 0::bigint, f.fragment_count - 1,
                               f.raw_bytes::numeric, f.stored_bytes::bigint, (f.child_count + 1)::numeric)
    UNION ALL
    SELECT 'unexpected revision_run' FROM {revision_run}
    UNION ALL
    SELECT 'winner scope' FROM {winner} w WHERE NOT EXISTS (
        SELECT 1 FROM {family} f
        JOIN {profiles} p ON p.definition_revision_id = :definition_revision_id
          AND p.dataset_id = :dataset_id AND p.schema_revision_id = :schema_revision_id
          AND p.profile_slot = w.profile_slot
        WHERE f.family_revision_id = w.family_revision_id AND f.entity_binding_id = w.entity_binding_id
          AND w.generation_id = :generation_id AND w.dataset_id = :dataset_id
          AND w.schema_revision_id = :schema_revision_id AND w.definition_revision_id = :definition_revision_id
          AND w.context_collection_slot = coalesce(p.context_collection_slot, 0))
    UNION ALL
    SELECT 'winner child' FROM {winner} w WHERE w.context_child_revision_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM {query_record} q JOIN {key_dictionary} k USING (key_rank, key_variant)
        WHERE q.child_revision_id = w.context_child_revision_id AND q.family_revision_id = w.family_revision_id
          AND q.collection_slot = w.context_collection_slot AND k.child_key_sha256 = w.context_child_key_sha256)
    UNION ALL
    SELECT 'query record family or key' FROM {query_record} q
    WHERE NOT EXISTS (SELECT 1 FROM {family} f WHERE f.family_revision_id = q.family_revision_id)
       OR NOT EXISTS (SELECT 1 FROM {key_dictionary} k WHERE k.key_rank = q.key_rank AND k.key_variant = q.key_variant)
    UNION ALL
    SELECT 'root query entity' FROM {query_root} q WHERE NOT EXISTS (
        SELECT 1 FROM {entity_binding} e WHERE e.entity_binding_id = q.entity_binding_id AND e.dataset_id = :dataset_id)
    UNION ALL
    SELECT 'child query entity or key' FROM {query_child} q WHERE NOT EXISTS (
        SELECT 1 FROM {entity_binding} e WHERE e.entity_binding_id = q.entity_binding_id AND e.dataset_id = :dataset_id)
       OR NOT EXISTS (SELECT 1 FROM {key_dictionary} k WHERE k.key_rank = q.key_rank AND k.key_variant = q.key_variant)
    UNION ALL
    SELECT 'pack producer' FROM {pack} p
    WHERE {pack_scope} OR p.capture_bundle_id IS DISTINCT FROM :capture_bundle_id
    UNION ALL
    SELECT 'rejection producer or pack' FROM {rejection} r
    WHERE {rej_scope} OR (r.pack_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM {pack} p WHERE p.pack_id = r.pack_id))
"""


def _v2_structure_checks_sql(rel_by_role: dict[str, str], profiles_sql: str) -> str:
    producer_cols = (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "producing_fence",
        "producing_token_sha256",
    )
    return _V2_STRUCTURE_CHECKS_TEMPLATE.format(
        **rel_by_role,
        profiles=profiles_sql,
        pack_scope=" OR ".join(f"p.{col} IS DISTINCT FROM :{col}" for col in producer_cols),
        rej_scope=" OR ".join(f"r.{col} IS DISTINCT FROM :{col}" for col in producer_cols),
    )


async def check_block_structure_v2(
    session,
    binding: FamilyBlockBinding,
    build,
    generation_id: int,
    progress_by_col: dict[str, object],
) -> None:
    """Set-based anti-join structural verification for compact v2 snapshots."""
    tables = binding.tables_by_role
    family, winner = tables["family"], tables["winner"]
    expected_families = int(progress_by_col["accepted_family_count"]) + int(progress_by_col["retained_family_count"])
    expected_children = int(progress_by_col["total_child_count"])
    await staging._prepare_statement(session)
    actual_families, actual_children = (
        await session.execute(select(func.count(), func.coalesce(func.sum(family.c.child_count), 0)))
    ).one()
    if (int(actual_families), int(actual_children)) != (expected_families, expected_children):
        raise FamilyBlockUnavailable("compact v2 family and child counts differ")
    await staging._prepare_statement(session)
    actual_winners, actual_contexts = (
        await session.execute(select(func.count(), func.coalesce(func.sum(winner.c.candidate_count), 0)))
    ).one()
    if (int(actual_winners), int(actual_contexts)) != (
        int(progress_by_col["winner_count"]),
        int(progress_by_col["candidate_context_count"]),
    ):
        raise FamilyBlockUnavailable("compact v2 winner counts differ")
    quote = _DIALECT.identifier_preparer.format_table
    rel_by_role = {role: quote(tbl) for role, tbl in tables.items()}
    checks_sql = _v2_structure_checks_sql(rel_by_role, await _control_profiles_relation(session))
    params_by_name = {
        "generation_id": generation_id,
        **{
            col: getattr(build, col)
            for col in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "capture_bundle_id",
                "producing_fence",
                "producing_token_sha256",
            )
        },
    }
    await staging._prepare_statement(session)
    defect = await session.scalar(text(f"SELECT defect FROM ({checks_sql}) AS v2_checks LIMIT 1"), params_by_name)
    if defect is not None:
        raise FamilyBlockUnavailable(f"compact v2 structure differs: {defect}")


async def _verify_structure_and_log(
    session,
    build,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
) -> None:
    """Verify closed compact v2 snapshot invariants and switch tables to LOGGED."""
    gen_id = int(progress_by_col["reserved_generation_id"])
    await check_block_structure_v2(session, binding, build, gen_id, progress_by_col)
    quote = _DIALECT.identifier_preparer.format_table
    for role in _NEW:
        await staging._prepare_statement(session)
        await session.execute(text(f"ALTER TABLE {quote(binding.tables_by_role[role])} SET LOGGED"))
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    await staging._prepare_statement(session)
    await session.execute(text(f"ALTER TABLE {schema_sql}.{DIRECT_COMMITMENTS_TABLE} SET LOGGED"))
    await staging._prepare_statement(session)
    await session.execute(
        text(
            f"CREATE INDEX IF NOT EXISTS {DIRECT_COMMITMENTS_TABLE}_family_idx ON {schema_sql}.{DIRECT_COMMITMENTS_TABLE} (family_revision_id)"
        )
    )
    await staging._prepare_statement(session)
    await session.execute(text(f"UPDATE {schema_sql}.{DIRECT_PROGRESS_TABLE} SET finalize_stage = 'f4b' WHERE id = 1"))


async def _verify_spot_checked_family(
    session,
    binding: FamilyBlockBinding,
    family_entry: dict,
    sampled_by_collection: Counter,
    *,
    limits,
) -> None:
    definition = binding.layout.definition
    if int(family_entry["fragment_count"]) == 0:
        entity_val = family_entry["entity_canonical_value"] if needs_inline_entity_join(definition) else None
        verify_inline_root_family(definition, family_entry, entity_canonical_value=entity_val)
        sampled_by_collection[None] += 1
        return
    frag_tbl = binding.tables_by_role["fragment"]
    await staging._prepare_statement(session)
    frag_rows = (
        (
            await session.execute(
                select(frag_tbl)
                .where(frag_tbl.c.family_revision_id == family_entry["family_revision_id"])
                .order_by(frag_tbl.c.fragment_ordinal)
            )
        )
        .mappings()
        .all()
    )
    verified = verify_v2_fragments_and_records(
        definition,
        ((_directory(frag), frag["encoded_fragment"]) for frag in frag_rows),
        binding.manifest(family_entry),
        limits=limits,
        observe=lambda col_name, _rec, _parts: sampled_by_collection.__setitem__(
            col_name, sampled_by_collection[col_name] + 1
        ),
    )
    verified.close()


async def _spot_check_v2_families(
    session,
    binding: FamilyBlockBinding,
    seed: bytes,
    *,
    limits,
) -> tuple[str, Counter]:
    """Deterministic capped 16-family spot check producing real verification coverage."""
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    quote = _DIALECT.identifier_preparer.format_table
    family_tbl = binding.tables_by_role["family"]
    entity_tbl = binding.tables_by_role["entity_binding"]
    await staging._prepare_statement(session)
    sample_entries = (
        (
            await session.execute(
                text(
                    f"""
                    SELECT f.*, c.logical_key_sha256, e.canonical_value AS entity_canonical_value
                    FROM {schema_sql}.{DIRECT_COMMITMENTS_TABLE} c
                    JOIN {quote(family_tbl)} f ON f.family_revision_id = c.family_revision_id
                    JOIN {quote(entity_tbl)} e ON e.entity_binding_id = f.entity_binding_id
                    ORDER BY pg_catalog.sha256(CAST(:seed AS bytea) || c.logical_key_sha256), f.family_revision_id
                    LIMIT :spot_limit
                    """
                ),
                {"seed": seed, "spot_limit": _SPOT_CHECK_LIMIT},
            )
        )
        .mappings()
        .all()
    )
    selection = hashlib.sha256(seed)
    sampled_by_collection: Counter = Counter()
    for family_entry in sample_entries:
        selection.update(bytes(family_entry["logical_key_sha256"]))
        await _verify_spot_checked_family(session, binding, family_entry, sampled_by_collection, limits=limits)
    return selection.hexdigest(), sampled_by_collection


async def _build_spot_check_evidence(
    session,
    binding: FamilyBlockBinding,
    generation: CustomImportGeneration,
    proof: CustomImportBuildVerification,
    progress_by_col: dict[str, object],
    *,
    limits,
) -> dict[str, object]:
    seed = compact_materialization._seed(generation, proof)
    selection_hex, sampled_by_collection = await _spot_check_v2_families(session, binding, seed, limits=limits)
    children_by_slot = {
        int(slot_key): int(count_val)
        for slot_key, count_val in json.loads(progress_by_col["collection_counts_json"]).items()
    }
    winners_tbl = binding.tables_by_role["winner"]
    await staging._prepare_statement(session)
    winners_by_slot = dict(
        (await session.execute(select(winners_tbl.c.profile_slot, func.count()).group_by(winners_tbl.c.profile_slot)))
        .tuples()
        .all()
    )
    populations = [("root", 0, proof.root_count, sampled_by_collection[None])]
    populations.extend(
        ("child", slot, int(children_by_slot.pop(slot, 0)), sampled_by_collection[name])
        for name, slot in sorted(binding.layout.collection_slots.items(), key=lambda pair: pair[1])
    )
    populations.extend(
        ("winner", slot, int(winners_by_slot.pop(slot, 0)), 0) for slot in range(1, proof.profile_count + 1)
    )
    if children_by_slot or winners_by_slot:
        raise CandidateRunnerError("direct compact evidence population differs from definition")
    evidence_by_field = {
        "contract": compact_materialization.SAMPLE_CONTRACT,
        "seed_sha256": seed.hex(),
        "selection_sha256": selection_hex,
        "coverage": [
            {"kind": kind, "slot": slot, "population": pop, "sampled": samp, "capped": samp < pop}
            for kind, slot, pop, samp in populations
        ],
    }
    publication._validate_verification_evidence(
        compact_materialization.MATERIALIZATION_CONTRACT,
        evidence_by_field,
        counts=(proof.root_count, proof.family_child_count, proof.winner_count, proof.profile_count),
    )
    return evidence_by_field


async def _iter_cursor_batches(
    session, cursor_name: str, query_sql: str, params_by_name: dict[str, object] | None = None
):
    """Stream one ordered SQL query in bounded batches and explicitly close its PostgreSQL portal."""
    await staging._prepare_statement(session)
    await session.execute(text("SET LOCAL cursor_tuple_fraction = 1.0"))
    await staging._prepare_statement(session)
    await session.execute(text(f"DECLARE {cursor_name} NO SCROLL CURSOR FOR {query_sql}"), params_by_name or {})
    try:
        while True:
            await staging._prepare_statement(session)
            batch_rows = (await session.execute(text(f"FETCH {_MATERIALIZATION_PAGE_LIMIT} FROM {cursor_name}"))).all()
            if not batch_rows:
                break
            yield batch_rows
            if len(batch_rows) < _MATERIALIZATION_PAGE_LIMIT:
                break
    finally:
        await session.execute(text(f"CLOSE {cursor_name}"))


async def _digest_direct_families(session, binding: FamilyBlockBinding, generation, material, effective) -> int:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    entity_sql = _DIALECT.identifier_preparer.format_table(binding.tables_by_role["entity_binding"])
    family_sql = f"""
        SELECT c.root_record_id, c.logical_key_sha256, c.key_contract_sha256, c.canonical_logical_key,
               c.family_sha256, c.child_count, c.root_payload_sha256, c.root_source_ordinal,
               e.adapter_id, e.value_sha256, e.canonical_value,
               CASE WHEN c.retained THEN 'retained' ELSE 'source' END
        FROM {schema_sql}.{DIRECT_COMMITMENTS_TABLE} c
        JOIN {entity_sql} e ON e.entity_binding_id = c.entity_binding_id AND e.dataset_id = :dataset_id
        ORDER BY c.logical_key_sha256
    """
    family_count = 0
    async for family_batch in _iter_cursor_batches(
        session, "direct_family_digest_cur", family_sql, {"dataset_id": generation.dataset_id}
    ):
        for f_row in family_batch:
            for is_eff, digest in enumerate((material, effective)):
                publication._add_digest_record(
                    digest, "family_commitment", compact_materialization._family_document(f_row, effective=bool(is_eff))
                )
            family_count += 1
    return family_count


_DIRECT_WINNER_DIGEST_SQL = """
    SELECT w.profile_slot, w.entity_binding_id, w.context_key_sha256, w.family_revision_id,
           w.context_collection_slot, w.context_child_revision_id,
           e.adapter_id, e.value_sha256, e.canonical_value,
           c.key_contract_sha256, c.logical_key_sha256,
           coalesce(w.context_child_key_sha256, ''::bytea) AS child_key_bytes,
           c.family_sha256
    FROM {winner_sql} w
    JOIN {schema_sql}.{commitments_table} c
      ON c.family_revision_id = w.family_revision_id AND c.entity_binding_id = w.entity_binding_id
    JOIN {entity_sql} e
      ON e.entity_binding_id = w.entity_binding_id AND e.dataset_id = w.dataset_id
    JOIN {profiles_sql} p
      ON p.profile_slot = w.profile_slot AND p.dataset_id = w.dataset_id
     AND p.definition_revision_id = w.definition_revision_id AND p.schema_revision_id = w.schema_revision_id
    WHERE w.generation_id = :generation_id AND w.dataset_id = :dataset_id
      AND w.definition_revision_id = :definition_revision_id AND w.schema_revision_id = :schema_revision_id
      AND w.profile_slot = :profile_slot
    ORDER BY e.adapter_id, e.value_sha256, e.canonical_value,
             c.key_contract_sha256, c.logical_key_sha256,
             coalesce(w.context_child_key_sha256, ''::bytea), c.family_sha256,
             w.context_collection_slot, w.context_key_sha256
"""


async def _digest_direct_winners(
    session,
    binding: FamilyBlockBinding,
    generation,
    profiles_sql: str,
    material,
    effective,
) -> int:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(binding.family_id))
    quote = _DIALECT.identifier_preparer.format_table
    entity_sql = quote(binding.tables_by_role["entity_binding"])
    winner_sql = quote(binding.tables_by_role["winner"])
    winner_sql_text = _DIRECT_WINNER_DIGEST_SQL.format(
        winner_sql=winner_sql,
        schema_sql=schema_sql,
        commitments_table=DIRECT_COMMITMENTS_TABLE,
        entity_sql=entity_sql,
        profiles_sql=profiles_sql,
    )
    winner_count = 0
    profile_count = len(binding.layout.definition.selection_profiles)
    for profile_slot in range(1, profile_count + 1):
        winner_params_by_name = {
            "generation_id": generation.generation_id,
            "dataset_id": generation.dataset_id,
            "definition_revision_id": generation.definition_revision_id,
            "schema_revision_id": generation.schema_revision_id,
            "profile_slot": profile_slot,
        }
        async for winner_batch in _iter_cursor_batches(
            session, "direct_winner_digest_cur", winner_sql_text, winner_params_by_name
        ):
            for w_row in winner_batch:
                publication._add_digest_record_to_all(
                    (material, effective), "winner_commitment", compact_materialization._winner_document(w_row)
                )
                winner_count += 1
    return winner_count


async def _compute_direct_materialization(
    session,
    binding: FamilyBlockBinding,
    generation: CustomImportGeneration,
    proof: CustomImportBuildVerification,
    progress_by_col: dict[str, object],
    *,
    limits,
):
    """Compute exact materialization and effective-output digests with 16-family spot check."""
    evidence_by_field = await _build_spot_check_evidence(
        session, binding, generation, proof, progress_by_col, limits=limits
    )
    source_digest = await publication._validated_source_bundle_digest(session, generation)
    material = publication._new_digest("generation-materialization/v2")
    effective = publication._new_digest("generation-effective-output/v2")
    await publication._add_generation_identity_material(session, material, generation)
    await publication._add_capture_material(session, material, generation)
    for section, model, rev_id in (
        ("definition", CustomImportDefinitionRevision, generation.definition_revision_id),
        ("schema", CustomImportSchemaRevision, generation.schema_revision_id),
    ):
        revision = await session.get(model, rev_id)
        if revision is None or revision.dataset_id != generation.dataset_id:
            raise publication.PublicationConflict("direct materialization definition or schema differs")
        publication._add_digest_record(effective, section, publication._materialization_document(revision))
    for digest in (material, effective):
        if await publication._add_definition_shape_material(session, digest, generation) != proof.profile_count:
            raise publication.PublicationConflict("direct materialization profile count differs")
    tables = binding.tables_by_role
    model_map = {
        CustomImportPack: aliased(CustomImportPack, tables["pack"], adapt_on_names=True),
        CustomImportRejection: aliased(CustomImportRejection, tables["rejection"], adapt_on_names=True),
    }
    await publication._add_execution_material(session, material, generation, models=model_map)
    family_count = await _digest_direct_families(session, binding, generation, material, effective)
    winner_count = await _digest_direct_winners(
        session, binding, generation, await _control_profiles_relation(session), material, effective
    )
    if (family_count, winner_count) != (proof.family_count, proof.winner_count):
        raise publication.PublicationConflict("direct commitments differ from structural counts")
    publication._add_digest_record(material, "verification_evidence", evidence_by_field)
    counts = tuple(getattr(proof, name) for name in build_output._COUNT_NAMES)
    commitment = publication._Materialization(
        source_digest,
        material.digest(),
        effective.digest(),
        *counts,
        materialization_contract=compact_materialization.MATERIALIZATION_CONTRACT,
        verification_evidence=evidence_by_field,
    )
    return await bind_generation_materialization(
        session, generation, commitment, binding.layout.definition, family_block_binding=binding
    )


async def _insert_direct_projection_manifest(
    session,
    request: staging.SourceBuildRequest,
    build,
    family_id: int,
    gen_id: int,
    proj_json: str,
) -> None:
    proj_by_field = json.loads(proj_json)
    session.add(
        CustomImportQueryProjection(
            generation_id=gen_id,
            dataset_id=build.dataset_id,
            definition_revision_id=build.definition_revision_id,
            schema_revision_id=build.schema_revision_id,
            execution_id=build.execution_id,
            producing_fence=build.producing_fence,
            producing_token_sha256=bytes(producer_token_sha256(request)),
            family_id=family_id,
            phase="complete",
            complete_at=func.statement_timestamp(),
            definition_sha256=bytes.fromhex(request.definition.digest),
            capability_sha256=bytes.fromhex(proj_by_field["capability_sha256"]),
            index_count=int(proj_by_field["index_count"]),
            root_count=int(proj_by_field["root_count"]),
            child_count=int(proj_by_field["child_count"]),
            verified_root_count=int(proj_by_field["root_count"]),
            verified_child_count=int(proj_by_field["child_count"]),
            digested_root_count=int(proj_by_field["root_count"]),
            digested_child_count=int(proj_by_field["child_count"]),
            digest_chain_sha256=bytes.fromhex(proj_by_field["digest_chain_sha256"]),
            digest_after_entity=proj_by_field["digest_after_entity"],
            digest_after_selected_value=proj_by_field["digest_after_selected_value"],
            digest_after_child_key=proj_by_field["digest_after_child_key"],
            projection_sha256=bytes.fromhex(proj_by_field["projection_sha256"]),
        )
    )
    await staging._flush_page(session)


async def _direct_candidate_digests(
    session,
    request: staging.SourceBuildRequest,
    build,
    family_id: int,
) -> tuple[bytes, bytes]:
    schema_sql = _DIALECT.identifier_preparer.quote_schema(snapshot_schema(family_id))
    await staging._prepare_statement(session)
    source_digest = await publication._capture_source_bundle_digest(
        session,
        capture_bundle_id=build.capture_bundle_id,
        dataset_id=build.dataset_id,
        definition_revision_id=build.definition_revision_id,
        schema_revision_id=build.schema_revision_id,
    )
    digest_builder = runner_codec.IncrementalCandidateDigest(
        execution_id=request.execution_id,
        fence=request.fence,
        base_generation_id=build.base_generation_id,
    )
    hash_sql = f"SELECT logical_key_sha256 FROM {schema_sql}.{DIRECT_COMMITMENTS_TABLE} ORDER BY logical_key_sha256"
    async for hash_batch in _iter_cursor_batches(session, "direct_candidate_digest_cur", hash_sql):
        for hash_row in hash_batch:
            digest_builder.update(bytes(hash_row[0]))
    return source_digest, digest_builder.digest()


async def _insert_direct_generation(
    session,
    request: staging.SourceBuildRequest,
    build,
    gen_id: int,
    total_families: int,
    digests: tuple[bytes, bytes],
) -> CustomImportGeneration:
    source_digest, candidate_digest = digests
    await staging._prepare_statement(session)
    now_stamp = await session.scalar(select(func.clock_timestamp()))
    build.phase = "graph"
    build.plan_stage = "complete"
    build.plan_page_sequence = 1
    build.plan_complete_at = now_stamp
    build.selected_family_count = total_families
    build.completed_family_count = total_families
    await staging._flush_page(session)
    generation = CustomImportGeneration(
        generation_id=gen_id,
        dataset_id=build.dataset_id,
        definition_revision_id=build.definition_revision_id,
        schema_revision_id=build.schema_revision_id,
        execution_id=build.execution_id,
        capture_bundle_id=build.capture_bundle_id,
        base_generation_id=build.base_generation_id,
        base_dataset_id=None if build.base_generation_id is None else build.dataset_id,
        source_bundle_sha256=source_digest,
        candidate_sha256=candidate_digest,
        root_count=total_families,
        family_count=total_families,
        producing_fence=build.producing_fence,
        producing_token_sha256=bytes(producer_token_sha256(request)),
    )
    session.add(generation)
    await staging._flush_page(session)
    return generation


async def _open_and_freeze_direct_generation(
    session,
    request: staging.SourceBuildRequest,
    build,
    family_id: int,
    progress_by_col: dict[str, object],
) -> tuple[CustomImportGeneration, CustomImportBuildVerification]:
    """Transition build through graph/output/verifying and create the reserved generation."""
    gen_id = int(progress_by_col["reserved_generation_id"])
    total_families = int(progress_by_col["accepted_family_count"]) + int(progress_by_col["retained_family_count"])
    digests = await _direct_candidate_digests(session, request, build, family_id)
    generation = await _insert_direct_generation(session, request, build, gen_id, total_families, digests)
    await staging._call(session, "open_custom_import_build_output", (("bigint", build.build_id), ("bigint", gen_id)))
    await session.refresh(build)
    build.generation_family_count = total_families
    build.candidate_context_count = int(progress_by_col["candidate_context_count"])
    build.winner_count = int(progress_by_col["winner_count"])
    build.output_after_profile_slot = progress_by_col["last_profile_slot"]
    build.output_after_entity_binding_id = progress_by_col["last_entity_binding_id"]
    build.output_after_context_key_sha256 = (
        None
        if progress_by_col["last_context_key_sha256"] is None
        else bytes(progress_by_col["last_context_key_sha256"])
    )
    await staging._flush_page(session)
    if progress_by_col["projection_evidence_json"] is not None:
        await _insert_direct_projection_manifest(
            session, request, build, family_id, gen_id, progress_by_col["projection_evidence_json"]
        )
    await staging._call(session, "freeze_custom_import_build_output", (("bigint", build.build_id),))
    await session.refresh(build, ("phase", "source_frozen_at", "graph_frozen_at", "output_frozen_at"))
    if build.phase != "verifying" or build.output_frozen_at is None:
        raise FamilyBlockUnavailable("direct compact output did not freeze")
    proof = CustomImportBuildVerification(
        build_id=build.build_id,
        generation_id=gen_id,
        source_frozen_at=build.source_frozen_at,
        graph_frozen_at=build.graph_frozen_at,
        output_frozen_at=build.output_frozen_at,
        root_count=total_families,
        family_count=total_families,
        generation_family_count=total_families,
        family_child_count=int(progress_by_col["total_child_count"]),
        winner_count=int(progress_by_col["winner_count"]),
        profile_count=len(request.definition.selection_profiles),
        root_scalar_count=int(progress_by_col["root_scalar_count"]),
        child_scalar_count=int(progress_by_col["child_scalar_count"]),
    )
    return generation, proof


async def _cleanup_and_mark_verified(
    session,
    build,
    binding: FamilyBlockBinding,
    proof: CustomImportBuildVerification,
    progress_by_col: dict[str, object],
    materialization,
) -> int:
    await _drop_direct_stage_tables(session, binding.family_id)
    quote = _DIALECT.identifier_preparer.format_table
    used_names = {tbl.name for tbl in binding.tables_by_role.values()}
    unused_tables = [quote(tbl) for name, tbl in snapshot_tables(binding.family_id).items() if name not in used_names]
    if unused_tables:
        await staging._prepare_statement(session)
        await session.execute(text("TRUNCATE TABLE " + ", ".join(unused_tables)))
    role_counts = await _role_counts(session, binding)
    await staging._prepare_statement(session)
    verify_stamp = await publication._database_now(session)
    proof_fields = (
        "build_id",
        "generation_id",
        "source_frozen_at",
        "graph_frozen_at",
        "output_frozen_at",
        *build_output._COUNT_NAMES,
    )
    proof_by_field = {name: getattr(proof, name) for name in proof_fields}
    await staging._prepare_statement(session)
    await session.execute(
        CustomImportBuildVerification.__table__.insert().values(
            **proof_by_field, verification_state="complete", scan_stage="complete", verified_at=verify_stamp
        )
    )
    descriptor = await session.get(CustomImportFamilyBlockLayout, binding.family_id, with_for_update=True)
    descriptor.phase = "logged"
    descriptor.closed_at = build.output_frozen_at
    descriptor.source_accepted_family_count = int(progress_by_col["accepted_family_count"])
    descriptor.source_rejection_count = int(progress_by_col["source_rejection_count"])
    descriptor.validation_sha256 = materialization.materialization_sha256
    descriptor.source_record_count = build.source_occurrence_count
    descriptor.role_counts = role_counts
    build.phase = "verified"
    build.verified_at = verify_stamp
    build.admission_after_occurrence_id += 1
    await staging._flush_page(session)
    return int(build.admission_after_occurrence_id)


async def _seal_verified_generation(
    session,
    request: staging.SourceBuildRequest,
    build,
    generation: CustomImportGeneration,
    materialization,
) -> None:
    unchanged_base = await build_output._unchanged_base(
        session, request, build, materialization, compact_candidate=True
    )
    await staging._prepare_statement(session)
    execution = await session.get(CustomImportExecution, request.execution_id)
    await staging._prepare_statement(session)
    lease = await session.get(CustomImportLease, request.execution_id)
    seal_request = publication._GenerationSealRequest(
        request.dataset_id, generation.generation_id, request.fence, producer_token_sha256(request)
    )
    await staging._prepare_statement(session)
    seal_now = await publication._database_now(session)
    deadline = staging.processing_deadline(request)
    if deadline is not None and seal_now >= deadline:
        raise LeaseAuthorityLost("build deadline elapsed before seal commit")
    if request.authorization_expires_at is not None and seal_now >= request.authorization_expires_at:
        raise LeaseAuthorityLost("writer authorization expired before seal commit")
    continuation_by_opt: dict[str, object] = {}
    if request.continuation is not None:
        auth = await verify_and_bind_continuation(session, request, execution, lease, seal_now, build=build)
        continuation_by_opt = {"continuation_request": request, "continuation_authorization": auth}
    publication._validate_generation_sealing_authority(
        execution, lease, generation, materialization, seal_request, seal_now, **continuation_by_opt
    )
    candidate_seal = publication._new_generation_seal(generation, materialization, seal_request)
    session.add(candidate_seal)
    await staging._flush_page(session)
    await build_output._finalize_sealed_build(
        session, request, build, generation, candidate_seal, unchanged_base, materialization
    )


async def _run_f4b_seal_transaction(
    session_factory,
    request: staging.SourceBuildRequest,
    build_id: int,
    family_id: int,
    *,
    admission_permit=None,
    verify_permit=None,
) -> tuple[str, int, int, int]:
    """Perform atomic phase transitions, 16-family spot check, cleanup, and terminal seal."""
    limits = default_direct_limits()
    async with graph._session(session_factory, transaction=True) as session:
        await staging._set_timeout(session, request.statement_timeout_ms)
        await publication._begin_finality_operation(session)
        build = await staging._lock_page(session, request, build_id)
        if admission_permit is not None and verify_permit is not None:
            await verify_permit(session, request, admission_permit)
        registry = await load_registry(session, request)
        binding = checked_binding(family_id, request.definition, registry.child_collection_slots)
        await _lock_open_layout(session, build, family_id, binding.layout)
        progress_by_col = await _load_progress(session, family_id)
        if progress_by_col["finalize_stage"] != "f4b":
            raise FamilyBlockUnavailable("direct seal requires completed f4a structure check")
        generation, proof = await _open_and_freeze_direct_generation(
            session, request, build, family_id, progress_by_col
        )
        materialization = await _compute_direct_materialization(
            session, binding, generation, proof, progress_by_col, limits=limits
        )
        final_cursor = await _cleanup_and_mark_verified(
            session, build, binding, proof, progress_by_col, materialization
        )
        await _seal_verified_generation(session, request, build, generation, materialization)
        return "verified", final_cursor, 1, int(build.candidate_error_count)


async def _reject_direct_candidate(
    session, build, family_id: int, progress_by_col: dict[str, object], outcome, rows_reported: int
):
    from process.custom_import.admission_sql import AdmissionResult

    await staging._prepare_statement(session)
    closed_stamp = await session.scalar(select(func.clock_timestamp()))
    descriptor = await session.get(CustomImportFamilyBlockLayout, family_id, with_for_update=True)
    descriptor.phase = "closed"
    descriptor.closed_at = closed_stamp
    descriptor.source_accepted_family_count = int(progress_by_col["accepted_family_count"]) + outcome.accepted_families
    descriptor.source_rejection_count = int(progress_by_col["source_rejection_count"]) + outcome.source_rejections
    await _drop_direct_stage_tables(session, family_id)
    build.phase = "rejected"
    build.admission_after_occurrence_id += 1
    await staging._flush_page(session)
    return AdmissionResult(
        "rejected", int(build.admission_after_occurrence_id), rows_reported, int(build.candidate_error_count)
    )


async def _dispatch_finalize_stage(
    session,
    request: staging.SourceBuildRequest,
    build,
    registry,
    binding: FamilyBlockBinding,
    progress_by_col: dict[str, object],
    *,
    limits,
) -> None:
    stage_name = progress_by_col["finalize_stage"]
    if stage_name == "f1":
        await _run_f1_dictionary_and_retained(
            session, request, build, registry, binding, progress_by_col, limits=limits
        )
        return
    if stage_name == "f2":
        await _run_f2_winners(session, build, binding, progress_by_col)
        return
    if stage_name == "f3":
        await _run_f3_projections_and_indexes(session, build, binding, progress_by_col)
        return
    if stage_name == "f4a":
        await _verify_structure_and_log(session, build, binding, progress_by_col)
        return
    raise FamilyBlockUnavailable(f"unknown direct finalize stage: {stage_name}")


async def run_direct_admission_locked(
    session,
    request: staging.SourceBuildRequest,
    build,
    expected_after_id: int,
    family_id: int,
):
    """Run one bounded bucket or finalize step inside an active page session.

    Returns None when the candidate is ready for the dedicated f4b finality transaction.
    """
    from process.custom_import.admission_sql import AdmissionError, AdmissionResult

    limits = default_direct_limits()
    if build.phase != "admission" or build.source_frozen_at is None:
        raise AdmissionError("custom_import_build_phase_mismatch")
    if build.admission_after_occurrence_id != expected_after_id:
        raise AdmissionError("custom_import_build_progress_conflict")
    registry = await load_registry(session, request)
    binding = checked_binding(family_id, request.definition, registry.child_collection_slots)
    await _lock_open_layout(session, build, family_id, binding.layout)
    progress_by_col = await _load_progress(session, family_id)
    stage_name = progress_by_col["finalize_stage"]
    if stage_name == "f4b":
        return None
    await require_direct_stage_intact(session, family_id)
    rows_reported = 1
    if stage_name == "buckets":
        outcome = await process_direct_buckets(
            session, request, build, registry, binding, progress_by_col, limits=limits
        )
        build.candidate_error_count += outcome.candidate_errors
        rows_reported = max(1, min(outcome.staged_rows_processed, MAX_BATCH_ROWS))
        is_last_bucket = int(progress_by_col["next_bucket"]) + outcome.buckets_processed >= int(
            progress_by_col["bucket_count"]
        )
        if is_last_bucket and build.candidate_error_count > 0:
            return await _reject_direct_candidate(session, build, family_id, progress_by_col, outcome, rows_reported)
    else:
        await _dispatch_finalize_stage(session, request, build, registry, binding, progress_by_col, limits=limits)
    build.admission_after_occurrence_id += 1
    await staging._flush_page(session)
    return AdmissionResult(
        build.phase, int(build.admission_after_occurrence_id), rows_reported, int(build.candidate_error_count)
    )


async def run_direct_seal_step(
    session_factory,
    request: staging.SourceBuildRequest,
    build_id: int,
    family_id: int,
    *,
    admission_permit=None,
    verify_permit=None,
):
    """Run the dedicated f4b finality transaction after closing the admission page session."""
    from process.custom_import.admission_sql import AdmissionResult

    phase, after_id, rows_n, err_n = await _run_f4b_seal_transaction(
        session_factory,
        request,
        build_id,
        family_id,
        admission_permit=admission_permit,
        verify_permit=verify_permit,
    )
    return AdmissionResult(phase, after_id, rows_n, err_n)


async def run_direct_admission_step(
    session_factory,
    request: staging.SourceBuildRequest,
    build_id: int,
    expected_after_id: int,
    family_id: int,
    *,
    admission_permit=None,
    verify_permit=None,
):
    """Execute one bounded direct compact v2 bucket or finalize step."""
    async with staging._page_session(session_factory, request, build_id) as (session, build):
        if admission_permit is not None and verify_permit is not None:
            await verify_permit(session, request, admission_permit)
        step_result = await run_direct_admission_locked(session, request, build, expected_after_id, family_id)
    if step_result is not None:
        return step_result
    return await run_direct_seal_step(
        session_factory,
        request,
        build_id,
        family_id,
        admission_permit=admission_permit,
        verify_permit=verify_permit,
    )
