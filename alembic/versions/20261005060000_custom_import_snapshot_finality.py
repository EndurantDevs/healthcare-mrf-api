# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare isolated indexes and verify complete frozen snapshots with sets."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261005060000_custom_import_snapshot_finality"
down_revision = "20261005050000_custom_import_legacy_snapshot_writers"
branch_labels = None
depends_on = None

_RESOURCE_DIRECTORY = Path(__file__).resolve().parents[1] / "sql" / "custom_import_snapshot_finality"
_QUERY_NAMES = ("graph_relationships", "source_lineage", "retained_lineage", "output_relationships", "legacy_origins")


_INDEX_SPECIFICATIONS = r"""[{"phase":"admission","name":"custom_import_build_final_child_idx","table":"custom_import_build_occurrence","keys":["build_id","stream_slot","root_record_id","collection_slot","raw_parent_key_sha256","child_key_sha256","source_ordinal DESC"],"predicate":"origin='source'ANDchild_revision_idISNOTNULL","ddl":"CREATE INDEX custom_import_build_final_child_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, stream_slot, root_record_id, collection_slot, raw_parent_key_sha256, child_key_sha256, source_ordinal DESC) WHERE origin = 'source' AND child_revision_id IS NOT NULL"},{"phase":"admission","name":"custom_import_build_occurrence_page_idx","table":"custom_import_build_occurrence","keys":["build_id","origin","occurrence_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_occurrence_page_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, origin, occurrence_id)"},{"phase":"admission","name":"custom_import_build_raw_parent_idx","table":"custom_import_build_occurrence","keys":["build_id","record_kind","raw_parent_key_sha256","occurrence_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_raw_parent_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, record_kind, raw_parent_key_sha256, occurrence_id)"},{"phase":"admission","name":"custom_import_build_source_child_idx","table":"custom_import_build_occurrence","keys":["build_id","raw_parent_key_sha256","collection_slot","child_key_sha256","occurrence_id"],"predicate":"origin='source'","ddl":"CREATE INDEX custom_import_build_source_child_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, raw_parent_key_sha256, collection_slot, child_key_sha256, occurrence_id) WHERE origin = 'source'"},{"phase":"admission","name":"custom_import_build_typed_root_idx","table":"custom_import_build_occurrence","keys":["build_id","origin","record_kind","root_record_id","occurrence_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_typed_root_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, origin, record_kind, root_record_id, occurrence_id)"},{"phase":"graph","name":"custom_import_build_family_hash_idx","table":"custom_import_build_family","keys":["build_id","root_key_sha256","root_record_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_family_hash_idx ON __CANDIDATE__.custom_import_build_family (build_id, root_key_sha256, root_record_id)"},{"phase":"graph","name":"custom_import_build_family_pending_idx","table":"custom_import_build_family","keys":["build_id","root_record_id"],"predicate":"complete_atISNULL","ddl":"CREATE INDEX custom_import_build_family_pending_idx ON __CANDIDATE__.custom_import_build_family (build_id, root_record_id) WHERE complete_at IS NULL"},{"phase":"graph","name":"custom_import_build_graph_child_idx","table":"custom_import_build_occurrence","keys":["build_id","origin","root_record_id","collection_slot","child_key_sha256","child_revision_id"],"predicate":"child_revision_idISNOTNULL","ddl":"CREATE INDEX custom_import_build_graph_child_idx ON __CANDIDATE__.custom_import_build_occurrence (build_id, origin, root_record_id, collection_slot, child_key_sha256, child_revision_id) WHERE child_revision_id IS NOT NULL"},{"phase":"graph","name":"custom_import_build_occurrence_pack_idx","table":"custom_import_build_occurrence","keys":["pack_id","occurrence_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_occurrence_pack_idx ON __CANDIDATE__.custom_import_build_occurrence (pack_id, occurrence_id)"},{"phase":"output","name":"custom_import_build_context_order_idx","table":"custom_import_build_candidate_context","keys":["build_id","profile_slot","entity_binding_id","context_key_sha256","candidate_context_id"],"predicate":"","ddl":"CREATE INDEX custom_import_build_context_order_idx ON __CANDIDATE__.custom_import_build_candidate_context (build_id, profile_slot, entity_binding_id, context_key_sha256, candidate_context_id)"},{"phase":"serving","name":"custom_import_root_scalar_text_idx","table":"custom_import_root_scalar","keys":["schema_revision_id","field_slot","string_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_root_scalar_text_idx ON __CANDIDATE__.custom_import_root_scalar (schema_revision_id, field_slot, string_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_root_scalar_int_idx","table":"custom_import_root_scalar","keys":["schema_revision_id","field_slot","integer_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_root_scalar_int_idx ON __CANDIDATE__.custom_import_root_scalar (schema_revision_id, field_slot, integer_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_root_scalar_number_idx","table":"custom_import_root_scalar","keys":["schema_revision_id","field_slot","decimal_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_root_scalar_number_idx ON __CANDIDATE__.custom_import_root_scalar (schema_revision_id, field_slot, decimal_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_root_scalar_date_idx","table":"custom_import_root_scalar","keys":["schema_revision_id","field_slot","date_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_root_scalar_date_idx ON __CANDIDATE__.custom_import_root_scalar (schema_revision_id, field_slot, date_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_root_scalar_time_idx","table":"custom_import_root_scalar","keys":["schema_revision_id","field_slot","timestamp_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_root_scalar_time_idx ON __CANDIDATE__.custom_import_root_scalar (schema_revision_id, field_slot, timestamp_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_child_scalar_text_idx","table":"custom_import_child_scalar","keys":["schema_revision_id","collection_slot","field_slot","string_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_child_scalar_text_idx ON __CANDIDATE__.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, string_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_child_scalar_int_idx","table":"custom_import_child_scalar","keys":["schema_revision_id","collection_slot","field_slot","integer_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_child_scalar_int_idx ON __CANDIDATE__.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, integer_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_child_scalar_number_idx","table":"custom_import_child_scalar","keys":["schema_revision_id","collection_slot","field_slot","decimal_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_child_scalar_number_idx ON __CANDIDATE__.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, decimal_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_child_scalar_date_idx","table":"custom_import_child_scalar","keys":["schema_revision_id","collection_slot","field_slot","date_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_child_scalar_date_idx ON __CANDIDATE__.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, date_value) WHERE value_state = 'value'"},{"phase":"serving","name":"custom_import_child_scalar_time_idx","table":"custom_import_child_scalar","keys":["schema_revision_id","collection_slot","field_slot","timestamp_value"],"predicate":"value_state='value'","ddl":"CREATE INDEX custom_import_child_scalar_time_idx ON __CANDIDATE__.custom_import_child_scalar (schema_revision_id, collection_slot, field_slot, timestamp_value) WHERE value_state = 'value'"}]"""

_INDEX_BODY = r"""
DECLARE f __CONTROL__.custom_import_snapshot_family; b __CONTROL__.custom_import_build_attempt;
    item jsonb; specifications jsonb := __INDEX_SPECS__; namespace text; index_oid oid;
    expected_keys text[]; actual_keys text[]; predicate text; created boolean := false;
BEGIN
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    IF f.family_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_family_missing'; END IF;
    IF p_phase NOT IN ('admission','graph','output','serving') THEN
        RAISE EXCEPTION 'custom_import_snapshot_index_phase_invalid'; END IF;
    IF p_phase='serving' THEN
        PERFORM __CONTROL__.lock_custom_import_snapshot_finality(f.generation_id,f.dataset_id,
            f.definition_revision_id,f.schema_revision_id,f.execution_id,f.capture_bundle_id,
            f.producing_fence,f.producing_token_sha256);
    ELSE
        PERFORM __CONTROL__.lock_custom_import_writable_snapshot(f.execution_id,f.producing_fence,f.producing_token_sha256);
        SELECT * INTO b FROM __CONTROL__.custom_import_build_attempt
            WHERE execution_id=f.execution_id AND producing_fence=f.producing_fence;
        IF b.phase IS DISTINCT FROM p_phase THEN RAISE EXCEPTION 'custom_import_snapshot_index_phase_invalid'; END IF;
    END IF;
    namespace:='ci_snapshot_'||f.family_id::text;
    FOR item IN SELECT value FROM jsonb_array_elements(specifications) WHERE value->>'phase'=p_phase LOOP
        index_oid:=to_regclass(format('%I.%I',namespace,item->>'name'));
        IF index_oid IS NULL THEN
            -- One native index per transaction; callers renew authority between indexes.
            PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(f.execution_id,f.producing_fence,f.producing_token_sha256);
            IF created THEN RETURN false; END IF;
            EXECUTE replace(item->>'ddl','__CANDIDATE__',quote_ident(namespace));
            created:=true;
            index_oid:=to_regclass(format('%I.%I',namespace,item->>'name'));
        END IF;
        SELECT array_agg(regexp_replace(value,' DESC$','') ORDER BY ordinal) INTO expected_keys
            FROM jsonb_array_elements_text(item->'keys') WITH ORDINALITY AS keys(value,ordinal);
        SELECT array_agg(pg_get_indexdef(index_oid,ordinal,false) ORDER BY ordinal) INTO actual_keys
            FROM generate_series(1,cardinality(expected_keys)) ordinal;
        SELECT regexp_replace(coalesce(pg_get_expr(i.indpred,i.indrelid),''),'[()[:space:]]|::text','','g')
            INTO predicate FROM pg_index i WHERE i.indexrelid=index_oid;
        IF actual_keys IS DISTINCT FROM expected_keys OR predicate IS DISTINCT FROM item->>'predicate'
            OR NOT EXISTS (
                SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
                JOIN pg_class heap ON heap.oid=i.indrelid JOIN pg_am am ON am.oid=c.relam
                WHERE c.oid=index_oid AND i.indrelid=to_regclass(format('%I.%I',namespace,item->>'table'))
                  AND c.relowner=heap.relowner AND c.relpersistence='p' AND am.amname='btree'
                  AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
                  AND i.indnatts=cardinality(expected_keys) AND i.indexprs IS NULL
                  AND ARRAY(SELECT unnest(i.indoption::smallint[]))=ARRAY(SELECT CASE WHEN value LIKE '% DESC' THEN 3 ELSE 0 END::smallint
                    FROM jsonb_array_elements_text(item->'keys') WITH ORDINALITY AS keys(value,ordinal) ORDER BY ordinal)
            ) THEN RAISE EXCEPTION 'custom_import_snapshot_index_mismatch'; END IF;
    END LOOP;
    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);
    PERFORM __CONTROL__.lock_custom_import_snapshot_attempt(f.execution_id,f.producing_fence,f.producing_token_sha256);
    RETURN true;
END;
"""

_CANDIDATE_STATISTICS = r"""
    IF p_phase='serving' THEN
        -- Writes are closed; sample only this exact registry-bound candidate.
        EXECUTE 'ANALYZE ' || (
            SELECT string_agg(table_oid::oid::regclass::text,',' ORDER BY relation_slot)
            FROM __CONTROL__.custom_import_snapshot_relation WHERE family_id=f.family_id);
    END IF;
"""

_VALIDATE_BODY = r"""
DECLARE g __CONTROL__.custom_import_generation; f __CONTROL__.custom_import_snapshot_family;
    snapshot_id bigint; base_family_id bigint; namespace text; base_namespace text; query text;
    failure record; counts jsonb; b __CONTROL__.custom_import_build_attempt;
BEGIN
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation_id;
    IF g.generation_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_generation_missing'; END IF;
    snapshot_id:=__CONTROL__.lock_custom_import_snapshot_finality(g.generation_id,g.dataset_id,
        g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=snapshot_id;
    base_family_id:=__CONTROL__.lock_custom_import_sealed_snapshot_base(g.base_generation_id,g.dataset_id);
    namespace:='ci_snapshot_'||snapshot_id::text;
    base_namespace:=CASE WHEN base_family_id IS NULL THEN __CONTROL_LITERAL__ ELSE 'ci_snapshot_'||base_family_id::text END;
    SELECT * INTO b FROM __CONTROL__.custom_import_build_attempt
        WHERE execution_id=g.execution_id AND producing_fence=g.producing_fence;
    IF b.build_id IS NULL AND f.origin_table_oid IS NULL THEN
        RAISE EXCEPTION 'custom_import_snapshot_legacy_origins_missing'; END IF;
    PERFORM __CONTROL__.verify_custom_import_snapshot_indexes(snapshot_id,'serving');
    IF b.build_id IS NOT NULL THEN
        PERFORM __CONTROL__.verify_custom_import_snapshot_indexes(snapshot_id,'admission');
        PERFORM __CONTROL__.verify_custom_import_snapshot_indexes(snapshot_id,'graph');
        PERFORM __CONTROL__.verify_custom_import_snapshot_indexes(snapshot_id,'output');
    END IF;
    FOREACH query IN ARRAY __QUERIES__ LOOP
        IF position('__ORIGIN_QUERY__' IN query)>0 AND b.build_id IS NOT NULL THEN CONTINUE; END IF;
        query:=replace(replace(query,'__CANDIDATE__',quote_ident(namespace)),'__BASE__',quote_ident(base_namespace));
        query:=replace(query,'__ORIGIN_QUERY__','');
        EXECUTE query INTO failure USING g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
            g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id;
        IF failure.failure_code IS NOT NULL THEN
            RAISE EXCEPTION 'custom_import_candidate_relationship_invalid'
                USING ERRCODE='23514',DETAIL=format('%s:%s',failure.failure_code,failure.row_identity);
        END IF;
    END LOOP;
    query:=replace(__COUNTS__,'__CANDIDATE__',quote_ident(namespace));
    EXECUTE query INTO counts USING g.generation_id,g.definition_revision_id;
    PERFORM __CONTROL__.lock_custom_import_snapshot_finality(g.generation_id,g.dataset_id,
        g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256);
    RETURN counts;
END;
"""

_COUNTS_QUERY = r"""
WITH members AS (SELECT * FROM __CANDIDATE__.custom_import_generation_family WHERE generation_id=$1),
families AS (SELECT f.* FROM __CANDIDATE__.custom_import_family_revision f JOIN members m USING(family_revision_id)),
edges AS (SELECT e.* FROM __CANDIDATE__.custom_import_family_child e JOIN families f USING(family_revision_id))
SELECT jsonb_build_object(
    'root_count',(SELECT count(*) FROM members),'family_count',(SELECT count(*) FROM families),
    'generation_family_count',(SELECT count(*) FROM members),'family_child_count',(SELECT count(*) FROM edges),
    'winner_count',(SELECT count(*) FROM __CANDIDATE__.custom_import_winner WHERE generation_id=$1),
    'profile_count',(SELECT count(*) FROM __CONTROL__.custom_import_selection_profile WHERE definition_revision_id=$2),
    'root_scalar_count',(SELECT count(*) FROM __CANDIDATE__.custom_import_root_scalar s JOIN families f USING(root_revision_id)),
    'child_scalar_count',(SELECT count(*) FROM __CANDIDATE__.custom_import_child_scalar s JOIN edges e USING(child_revision_id)))
"""

_BUILD_BODY = r"""
DECLARE b __CONTROL__.custom_import_build_attempt; v __CONTROL__.custom_import_build_verification;
    counts jsonb; verified timestamptz;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF b.phase NOT IN ('verifying','verified') THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
    SELECT * INTO v FROM __CONTROL__.custom_import_build_verification WHERE build_id=b.build_id FOR UPDATE;
    IF v.verification_state='complete' THEN
        PERFORM __CONTROL__.lock_custom_import_snapshot_finality(b.generation_id,b.dataset_id,b.definition_revision_id,
            b.schema_revision_id,b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256);
        RETURN QUERY SELECT v.verification_state::text,v.scan_stage::text,v.page_sequence,0; RETURN;
    END IF;
    IF v.build_id IS NULL THEN
        INSERT INTO __CONTROL__.custom_import_build_verification
            (build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at)
            VALUES(b.build_id,b.generation_id,b.source_frozen_at,b.graph_frozen_at,b.output_frozen_at) RETURNING * INTO v;
    END IF;
    IF ROW(v.generation_id,v.source_frozen_at,v.graph_frozen_at,v.output_frozen_at) IS DISTINCT FROM
        ROW(b.generation_id,b.source_frozen_at,b.graph_frozen_at,b.output_frozen_at) THEN
        RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
    counts:=__CONTROL__.verify_custom_import_snapshot_structure(b.generation_id);
    IF (counts->>'family_count')::bigint<>b.selected_family_count
       OR (counts->>'generation_family_count')::bigint<>b.generation_family_count
       OR (counts->>'winner_count')::bigint<>b.winner_count THEN
        RAISE EXCEPTION 'custom_import_build_structure_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    verified:=clock_timestamp();
    UPDATE __CONTROL__.custom_import_build_verification SET verification_state='complete',scan_stage='complete',
        page_sequence=v.page_sequence+1,verified_at=verified,current_family_revision_id=NULL,
        current_family_expected_child_count=NULL,current_family_seen_child_count=NULL,
        root_count=(counts->>'root_count')::bigint,family_count=(counts->>'family_count')::bigint,
        generation_family_count=(counts->>'generation_family_count')::bigint,
        family_child_count=(counts->>'family_child_count')::bigint,winner_count=(counts->>'winner_count')::bigint,
        profile_count=(counts->>'profile_count')::bigint,root_scalar_count=(counts->>'root_scalar_count')::bigint,
        child_scalar_count=(counts->>'child_scalar_count')::bigint WHERE build_id=b.build_id;
    UPDATE __CONTROL__.custom_import_build_attempt SET phase='verified',verified_at=verified WHERE build_id=b.build_id;
    RETURN QUERY SELECT 'complete'::text,'complete'::text,v.page_sequence+1,0;
END;
"""


def _bulk():
    path = Path(__file__).with_name("20261005040000_custom_import_bulk_snapshot_writers.py")
    spec = importlib.util.spec_from_file_location("snapshot_finality_bulk", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema() -> str:
    return _bulk()._schema()


def _resource(name: str) -> str:
    if name not in _QUERY_NAMES:
        raise ValueError("unknown snapshot verification resource")
    return (_RESOURCE_DIRECTORY / (name + ".sql")).read_text()


def _function(bulk, schema, name, arguments, result, body, *, existing=False):
    storage = bulk._storage()
    creation = "CREATE OR REPLACE" if existing else "CREATE"
    identity = f"{storage._quote(schema)}.{name}({bulk._argument_types(arguments) if arguments else ''})"
    op.execute(
        f"{creation} FUNCTION {storage._quote(schema)}.{name}({arguments}) RETURNS {result}\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog\n"
        f"AS $snapshot_finality$ {bulk._control_sql(storage, schema, body)} $snapshot_finality$"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {identity} FROM PUBLIC")
    if not existing:
        storage._revoke_defaults("FUNCTION", identity)


def upgrade() -> None:
    """Install fixed definitions only; actual candidates prepare their own indexes."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    index_body = _INDEX_BODY.replace("__INDEX_SPECS__", storage._literal(_INDEX_SPECIFICATIONS) + "::jsonb")
    prepare_indexes = index_body.replace(
        "    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);",
        _CANDIDATE_STATISTICS + "    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);",
        1,
    )
    _function(
        bulk,
        schema,
        "prepare_custom_import_snapshot_indexes",
        "p_family_id bigint,p_phase text",
        "boolean",
        prepare_indexes,
    )
    verify_indexes = index_body.replace("IF p_phase='serving' THEN", "IF true THEN")
    create_from = verify_indexes.index("            -- One native index per transaction")
    create_to = verify_indexes.index("        END IF;", create_from)
    verify_indexes = (
        verify_indexes[:create_from]
        + "            RAISE EXCEPTION 'custom_import_snapshot_index_missing';\n"
        + verify_indexes[create_to:]
    )
    _function(
        bulk,
        schema,
        "verify_custom_import_snapshot_indexes",
        "p_family_id bigint,p_phase text",
        "boolean",
        verify_indexes,
    )
    queries = (
        "ARRAY["
        + ",".join(storage._literal(bulk._control_sql(storage, schema, _resource(name))) for name in _QUERY_NAMES)
        + "]::text[]"
    )
    body = _VALIDATE_BODY.replace("__QUERIES__", queries).replace("__CONTROL_LITERAL__", storage._literal(schema))
    body = body.replace("__COUNTS__", storage._literal(bulk._control_sql(storage, schema, _COUNTS_QUERY)))
    _function(bulk, schema, "verify_custom_import_snapshot_structure", "p_generation_id bigint", "jsonb", body)
    _function(
        bulk,
        schema,
        "verify_custom_import_build_structure",
        "p_build_id bigint",
        "TABLE(verification_state text,scan_stage text,page_sequence bigint,rows_processed integer)",
        _BUILD_BODY,
        existing=True,
    )
    _install_seal_guard(bulk, schema)


def _install_seal_guard(bulk, schema) -> None:
    """Preserve canonical finality and route registered snapshots to frozen sets."""
    previous = bulk._previous("20261002010000_custom_import_bounded_build")
    legacy = previous._finality_module()
    original = legacy._GENERATION_SEAL_FUNCTION_BODY.replace(
        "DECLARE",
        "DECLARE\n"
        "build __SCHEMA__.custom_import_build_attempt; proof __SCHEMA__.custom_import_build_verification;"
        "built_generation __SCHEMA__.custom_import_generation;",
        1,
    )
    original = legacy._with_read_committed_guard(
        original.replace("__READ_COMMITTED_GUARD__", "__READ_COMMITTED_GUARD__" + previous._BUILD_SEAL_BRANCH, 1)
    ).replace("__SCHEMA__", "__CONTROL__")
    frozen = (_RESOURCE_DIRECTORY / "generation_seal.sql").read_text()
    dispatch = (
        """BEGIN
        IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family WHERE generation_id=NEW.generation_id
            OR (execution_id=NEW.execution_id AND producing_fence=NEW.sealing_fence)) THEN
    """
        + frozen
        + "\nELSE\n"
        + original
        + "\nEND IF;\nEND;"
    )
    _function(bulk, schema, "guard_custom_import_generation_seal_insert", "", "trigger", dispatch, existing=True)


def downgrade() -> None:
    """Retained snapshots prevent reverting their complete validation contract."""
    bulk = _bulk()
    storage = bulk._storage()
    schema = _schema()
    op.execute(
        bulk._control_sql(
            storage,
            schema,
            """
        DO $snapshot_finality$ BEGIN
            LOCK TABLE __CONTROL__.custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE;
            IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family) THEN
                RAISE EXCEPTION 'custom_import_snapshot_finality_downgrade_blocked'; END IF;
        END $snapshot_finality$
    """,
        )
    )
    previous = bulk._previous("20261002010000_custom_import_bounded_build")
    previous._seal_function(schema)
    _function(
        bulk,
        schema,
        "verify_custom_import_build_structure",
        "p_build_id bigint",
        "TABLE(verification_state text,scan_stage text,page_sequence bigint,rows_processed integer)",
        previous._VERIFY_CUSTOM_IMPORT_BUILD_STRUCTURE_BODY.replace("__SCHEMA__", "__CONTROL__"),
        existing=True,
    )
    for signature in (
        "verify_custom_import_snapshot_structure(bigint)",
        "verify_custom_import_snapshot_indexes(bigint,text)",
        "prepare_custom_import_snapshot_indexes(bigint,text)",
    ):
        op.execute(f"DROP FUNCTION {storage._quote(schema)}.{signature}")
