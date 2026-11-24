# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-based census and relationship checks for closed compact family storage."""

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import dialect

from db.models.custom_import import CustomImportSelectionProfile
from process.custom_import.build_source import _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable


def _fragment_checks(family, fragment):
    return f"""
        SELECT 'orphan fragment' AS defect FROM {fragment} AS fragments
        WHERE NOT EXISTS (SELECT 1 FROM {family} AS families
                          WHERE families.family_revision_id = fragments.family_revision_id)
        UNION ALL
        SELECT 'fragment census' FROM {family} AS families
        LEFT JOIN (
            SELECT family_revision_id, count(*) AS population,
                   min(fragment_ordinal) AS first_fragment, max(fragment_ordinal) AS last_fragment,
                   sum(raw_bytes) AS bytes_before_compression,
                   sum(8 + octet_length(encoded_fragment)) AS bytes_in_envelope,
                   sum(starting_record_count) AS records
            FROM {fragment} GROUP BY family_revision_id
        ) AS counts USING (family_revision_id)
        WHERE ROW(counts.population, counts.first_fragment, counts.last_fragment,
                  counts.bytes_before_compression, counts.bytes_in_envelope, counts.records)
              IS DISTINCT FROM ROW(families.fragment_count, 0::bigint, families.fragment_count - 1,
                                   families.raw_bytes::numeric, families.stored_bytes::bigint,
                                   (families.child_count + 1)::numeric)
    """


def _membership_checks(family, runs):
    return f"""
        SELECT 'orphan membership' AS defect FROM {runs} AS members
        WHERE NOT EXISTS (SELECT 1 FROM {family} AS families
                          WHERE families.family_revision_id = members.family_revision_id)
        UNION ALL
        SELECT 'membership census' FROM {family} AS families
        LEFT JOIN (
            SELECT family_revision_id, count(*) AS population, sum(record_count) AS records,
                   count(*) FILTER (WHERE collection_slot = 0) AS roots,
                   max(first_revision_id) FILTER (WHERE collection_slot = 0 AND record_count = 1
                       AND first_revision_id = last_revision_id AND first_record_ordinal = 0
                       AND encoded_membership IS NULL) AS root_id
            FROM {runs} GROUP BY family_revision_id
        ) AS counts USING (family_revision_id)
        WHERE ROW(counts.population, counts.records, counts.roots, counts.root_id)
              IS DISTINCT FROM ROW(families.membership_count, (families.child_count + 1)::numeric,
                                   1::bigint, families.root_revision_id)
        UNION ALL
        SELECT 'overlapping membership' FROM (
            SELECT first_revision_id, lag(last_revision_id) OVER (
                PARTITION BY family_revision_id, collection_slot ORDER BY first_revision_id
            ) AS previous_end FROM {runs}
        ) AS ordered WHERE first_revision_id <= previous_end
    """


def _owner_checks(family, root, binding):
    return f"""
        SELECT 'family root or binding scope' AS defect FROM {family} AS families
        WHERE NOT EXISTS (SELECT 1 FROM {root} AS roots WHERE roots.root_record_id = families.root_record_id
                          AND roots.dataset_id = :dataset_id)
           OR NOT EXISTS (SELECT 1 FROM {binding} AS bindings
                          WHERE bindings.entity_binding_id = families.entity_binding_id
                          AND bindings.dataset_id = :dataset_id)
        UNION ALL
        SELECT 'duplicate family root' FROM {family} GROUP BY root_record_id HAVING count(*) <> 1
        UNION ALL
        SELECT 'duplicate root revision' FROM {family} GROUP BY root_revision_id HAVING count(*) <> 1
    """


def _generation_checks(tables, profiles):
    family = tables["family"]
    members = tables["generation_family"]
    winner = tables["winner"]
    query, keys = tables["query_record"], tables["key_dictionary"]
    return f"""
        SELECT 'generation membership scope' AS defect FROM {members} m
        WHERE m.generation_id IS DISTINCT FROM :generation_id OR m.dataset_id IS DISTINCT FROM :dataset_id
           OR m.definition_revision_id IS DISTINCT FROM :definition_revision_id
           OR m.schema_revision_id IS DISTINCT FROM :schema_revision_id OR NOT EXISTS (
              SELECT 1 FROM {family} f WHERE f.family_revision_id=m.family_revision_id
              AND f.root_record_id=m.root_record_id)
        UNION ALL
        SELECT 'missing generation member' FROM {family} f WHERE NOT EXISTS (
            SELECT 1 FROM {members} m WHERE m.generation_id=:generation_id AND m.root_record_id=f.root_record_id
            AND m.family_revision_id=f.family_revision_id)
        UNION ALL
        SELECT 'winner scope' FROM {winner} w WHERE NOT EXISTS (
            SELECT 1 FROM {family} f JOIN {members} m
              ON m.generation_id=w.generation_id AND m.root_record_id=f.root_record_id
              AND m.family_revision_id=f.family_revision_id
            JOIN {profiles} p ON p.definition_revision_id=m.definition_revision_id AND p.dataset_id=m.dataset_id
              AND p.schema_revision_id=m.schema_revision_id AND p.profile_slot=w.profile_slot
            WHERE f.family_revision_id=w.family_revision_id AND f.entity_binding_id=w.entity_binding_id
              AND w.dataset_id=m.dataset_id AND w.schema_revision_id=m.schema_revision_id
              AND w.definition_revision_id=m.definition_revision_id
              AND w.context_collection_slot=coalesce(p.context_collection_slot,0))
        UNION ALL
        SELECT 'winner child' FROM {winner} w WHERE w.context_child_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM {query} q JOIN {keys} k USING (key_rank,key_variant)
            WHERE q.child_revision_id=w.context_child_revision_id AND q.family_revision_id=w.family_revision_id
              AND q.collection_slot=w.context_collection_slot AND k.child_key_sha256=w.context_child_key_sha256)
    """


def _query_checks(tables):
    families = tables["family"]
    records, keys = tables["query_record"], tables["key_dictionary"]
    entities = tables["entity_binding"]
    roots, children = tables["query_root"], tables["query_child"]
    return f"""
        SELECT 'query record family or key' AS defect FROM {records} q
        WHERE NOT EXISTS (SELECT 1 FROM {families} f WHERE f.family_revision_id=q.family_revision_id)
           OR NOT EXISTS (SELECT 1 FROM {keys} k WHERE k.key_rank=q.key_rank AND k.key_variant=q.key_variant)
        UNION ALL
        SELECT 'root query entity' FROM {roots} q WHERE NOT EXISTS (
            SELECT 1 FROM {entities} e WHERE e.entity_binding_id=q.entity_binding_id AND e.dataset_id=:dataset_id)
        UNION ALL
        SELECT 'child query entity or key' FROM {children} q WHERE NOT EXISTS (
            SELECT 1 FROM {entities} e WHERE e.entity_binding_id=q.entity_binding_id AND e.dataset_id=:dataset_id)
           OR NOT EXISTS (SELECT 1 FROM {keys} k WHERE k.key_rank=q.key_rank AND k.key_variant=q.key_variant)
    """


def _producer_checks(tables):
    packs, rejections = tables["pack"], tables["rejection"]
    producer_columns = (
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "producing_fence",
        "producing_token_sha256",
    )
    pack_scope = " OR ".join(f"p.{name} IS DISTINCT FROM :{name}" for name in producer_columns)
    rejection_scope = " OR ".join(f"r.{name} IS DISTINCT FROM :{name}" for name in producer_columns)
    return f"""
        SELECT 'pack producer' AS defect FROM {packs} p
        WHERE {pack_scope} OR p.capture_bundle_id IS DISTINCT FROM :capture_bundle_id
        UNION ALL
        SELECT 'rejection producer or pack' FROM {rejections} r
        WHERE {rejection_scope} OR (r.pack_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM {packs} p WHERE p.pack_id=r.pack_id))
    """


async def check_block_structure(session, binding, build, *, expected_children):
    """Compare closed candidate counts and references without replaying record payloads."""
    if not session.in_transaction() or build.phase != "verifying":
        raise FamilyBlockUnavailable("compact structure checks require a closed output")
    tables = binding.tables_by_role
    family, winner = tables["family"], tables["winner"]
    await _prepare_statement(session)
    family_count, child_count = (
        await session.execute(select(func.count(), func.coalesce(func.sum(family.c.child_count), 0)))
    ).one()
    if (family_count, child_count) != (build.selected_family_count, expected_children):
        raise FamilyBlockUnavailable("compact family and child counts differ from SOURCE")
    await _prepare_statement(session)
    winner_count, contexts = (
        await session.execute(select(func.count(), func.coalesce(func.sum(winner.c.candidate_count), 0)))
    ).one()
    if (winner_count, contexts) != (build.winner_count, build.candidate_context_count):
        raise FamilyBlockUnavailable("compact winner counts differ from OUTPUT")

    quote = dialect().identifier_preparer.format_table
    relations_by_role = {role: quote(table) for role, table in tables.items()}
    profile_table = CustomImportSelectionProfile.__table__
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(profile_table.schema, profile_table.schema)
    name = quote(profile_table, use_schema=False)
    profiles = f"{dialect().identifier_preparer.quote_schema(control_schema)}.{name}" if control_schema else name
    checks = " UNION ALL ".join(
        (
            _owner_checks(
                relations_by_role["family"], relations_by_role["root_record"], relations_by_role["entity_binding"]
            ),
            _fragment_checks(relations_by_role["family"], relations_by_role["fragment"]),
            _membership_checks(relations_by_role["family"], relations_by_role["revision_run"]),
            _generation_checks(relations_by_role, profiles),
            _query_checks(relations_by_role),
            _producer_checks(relations_by_role),
        )
    )
    parameters_by_name = {
        name: getattr(build, name)
        for name in (
            "generation_id",
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            "producing_fence",
            "producing_token_sha256",
        )
    }
    await _prepare_statement(session)
    defect = await session.scalar(text(f"SELECT defect FROM ({checks}) AS checks LIMIT 1"), parameters_by_name)
    if defect is not None:
        raise FamilyBlockUnavailable(f"compact block structure differs: {defect}")
