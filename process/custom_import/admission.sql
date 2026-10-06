-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Fixed application SQL. All identifiers are package-owned; all values are bound.
-- Existing page locks and the registered-snapshot resolver precede these statements.
SELECT CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
        WHERE r.relation_slot=1 AND r.table_oid=CAST(:root_relation AS regclass)::oid
          AND f.landing_table_oid=CAST(:landing_relation AS regclass)::oid
          AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
          AND f.frozen_at IS NULL
          AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
              f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
            IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
    ) THEN 'custom_import_source_snapshot_binding_mismatch'
    WHEN b.phase<>'admission' OR b.source_frozen_at IS NULL THEN 'custom_import_build_phase_mismatch'
    WHEN CAST(:expected_after_id AS bigint) IS DISTINCT FROM b.admission_after_occurrence_id
      THEN 'custom_import_build_progress_conflict'
    WHEN EXISTS (
        SELECT 1 FROM pg_trigger t
        WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1
          AND ((t.tgrelid=CAST(:rejection_relation AS regclass) AND (t.tgtype & 4)=4)
            OR (t.tgrelid=CAST(:occurrence_relation AS regclass) AND (t.tgtype & 16)=16))
    ) THEN 'custom_import_bulk_shared_migration_required'
    WHEN EXISTS (
        SELECT 1 FROM pg_constraint c WHERE c.contype='f' AND c.conrelid=ANY(ARRAY[
            CAST(:rejection_relation AS regclass),CAST(:occurrence_relation AS regclass)])
    ) THEN 'custom_import_bulk_hot_relationship_guards_present'
    WHEN EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_generation_seal s
        WHERE s.execution_id=b.execution_id AND s.dataset_id=b.dataset_id
        UNION ALL
        SELECT 1 FROM __CONTROL__.custom_import_no_change_seal s
        WHERE s.execution_id=b.execution_id AND s.dataset_id=b.dataset_id
    ) THEN 'custom_import_sealed_append'
    WHEN EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_build_stream s
        WHERE s.build_id=b.build_id AND s.replay_verified_at IS NULL
    ) OR NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_capture_bundle c
        WHERE c.capture_bundle_id=b.capture_bundle_id AND c.capture_state='sealed'
          AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id)
            IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id)
    ) THEN 'custom_import_build_incomplete'
END problem,d.canonical_definition::jsonb->'streams' definition_streams,
    coalesce(d.canonical_definition::jsonb->'child_memberships','[]'::jsonb) memberships
FROM __CONTROL__.custom_import_build_attempt b
LEFT JOIN __CONTROL__.custom_import_definition_revision d ON d.definition_revision_id=b.definition_revision_id
WHERE b.build_id=CAST(:build_id AS bigint);

-- statement boundary --

WITH
candidates AS MATERIALIZED (
    SELECT x.occurrence_id,x.build_id,x.record_kind,x.stream_slot,x.pack_id,x.source_ordinal,
        x.collection_slot,x.raw_parent_key_sha256,x.root_record_id,x.root_revision_id,x.child_revision_id,
        x.child_key_sha256,x.rejection_id,x.resolved_rejection_id,
        coalesce(octet_length(x.raw_parent_key_canonical),0) raw_parent_bytes,j.code initial_code,
        coalesce(:definition_streams->(x.stream_slot-1)->>'duplicate_policy','reject')='collapse_identical' collapse_identical
    FROM __CANDIDATE__.custom_import_build_occurrence x
    LEFT JOIN __CANDIDATE__.custom_import_rejection j ON j.rejection_id=x.rejection_id
    WHERE x.build_id=:build_id AND x.origin='source'
      AND x.occurrence_id>:admission_after_occurrence_id
    ORDER BY x.occurrence_id LIMIT (:physical_row_cap/:page_row_limit)*:page_row_limit
),
raw_keys AS MATERIALIZED (
    SELECT DISTINCT c.record_kind,c.raw_parent_key_sha256 FROM candidates c
    WHERE c.raw_parent_key_sha256 IS NOT NULL
    UNION
    SELECT DISTINCT 'root',c.raw_parent_key_sha256 FROM candidates c
    WHERE c.record_kind='child' AND c.raw_parent_key_sha256 IS NOT NULL
),
first_raw AS MATERIALIZED (
    SELECT DISTINCT ON (x.record_kind,x.raw_parent_key_sha256)
        x.record_kind,x.raw_parent_key_sha256,x.occurrence_id first_id,
        coalesce(octet_length(x.raw_parent_key_canonical),0) raw_parent_bytes
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN raw_keys k ON k.record_kind=x.record_kind AND k.raw_parent_key_sha256=x.raw_parent_key_sha256
    WHERE x.build_id=:build_id
    ORDER BY x.record_kind,x.raw_parent_key_sha256,x.occurrence_id
),
source_roots AS MATERIALIZED (
    SELECT x.raw_parent_key_sha256,min(x.occurrence_id) parent_id,count(*) raw_root_count
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN raw_keys k ON k.record_kind='root' AND k.raw_parent_key_sha256=x.raw_parent_key_sha256
    WHERE x.build_id=:build_id AND x.origin='source' AND x.record_kind='root'
    GROUP BY x.raw_parent_key_sha256
),
typed_root_keys AS MATERIALIZED (
    SELECT DISTINCT c.root_record_id FROM candidates c
    WHERE c.record_kind='root' AND c.root_record_id IS NOT NULL
),
typed_roots AS MATERIALIZED (
    SELECT x.root_record_id,count(*) typed_root_count
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN typed_root_keys k ON k.root_record_id=x.root_record_id
    WHERE x.build_id=:build_id AND x.origin='source' AND x.record_kind='root'
    GROUP BY x.root_record_id
),
child_keys AS MATERIALIZED (
    SELECT DISTINCT c.root_record_id,c.raw_parent_key_sha256,c.collection_slot,c.child_key_sha256
    FROM candidates c WHERE c.record_kind='child' AND c.child_key_sha256 IS NOT NULL
),
child_duplicates AS MATERIALIZED (
    SELECT x.root_record_id,x.raw_parent_key_sha256,x.collection_slot,x.child_key_sha256,count(*) child_count
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN child_keys k ON k.root_record_id=x.root_record_id AND k.raw_parent_key_sha256=x.raw_parent_key_sha256
      AND k.collection_slot=x.collection_slot AND k.child_key_sha256=x.child_key_sha256
    WHERE x.build_id=:build_id AND x.origin='source' AND x.record_kind='child'
    GROUP BY x.root_record_id,x.raw_parent_key_sha256,x.collection_slot,x.child_key_sha256
),
typed_child_keys AS MATERIALIZED (
    SELECT DISTINCT c.root_record_id,c.collection_slot,c.child_key_sha256
    FROM candidates c WHERE c.child_revision_id IS NOT NULL
),
first_typed_child AS MATERIALIZED (
    SELECT DISTINCT ON (x.root_record_id,x.collection_slot,x.child_key_sha256)
        x.root_record_id,x.collection_slot,x.child_key_sha256,x.child_revision_id
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN typed_child_keys k ON k.root_record_id=x.root_record_id AND k.collection_slot=x.collection_slot
      AND k.child_key_sha256=x.child_key_sha256
    WHERE x.build_id=:build_id AND x.origin='source' AND x.child_revision_id IS NOT NULL
    ORDER BY x.root_record_id,x.collection_slot,x.child_key_sha256,x.child_revision_id
),
collapse_keys AS MATERIALIZED (
    SELECT DISTINCT c.stream_slot,c.root_record_id,c.collection_slot,c.raw_parent_key_sha256,c.child_key_sha256
    FROM candidates c WHERE c.collapse_identical AND c.child_key_sha256 IS NOT NULL
),
final_child AS MATERIALIZED (
    SELECT DISTINCT ON (x.stream_slot,x.root_record_id,x.collection_slot,x.raw_parent_key_sha256,x.child_key_sha256)
        x.stream_slot,x.root_record_id,x.collection_slot,x.raw_parent_key_sha256,x.child_key_sha256,x.child_revision_id
    FROM __CANDIDATE__.custom_import_build_occurrence x
    JOIN collapse_keys k ON k.stream_slot=x.stream_slot AND k.root_record_id=x.root_record_id
      AND k.collection_slot=x.collection_slot AND k.raw_parent_key_sha256=x.raw_parent_key_sha256
      AND k.child_key_sha256=x.child_key_sha256
    WHERE x.build_id=:build_id AND x.origin='source' AND x.child_revision_id IS NOT NULL
    ORDER BY x.stream_slot,x.root_record_id,x.collection_slot,x.raw_parent_key_sha256,x.child_key_sha256,x.source_ordinal DESC
),
metadata AS MATERIALIZED (
    SELECT c.*,
        CASE WHEN c.initial_code IS DISTINCT FROM 'child_not_object' THEN roots.parent_id END parent_id,
        (parent.raw_parent_key_canonical COLLATE "C" IS DISTINCT FROM current_raw.raw_parent_key_canonical COLLATE "C") parent_raw_differs,
        (first_raw_row.raw_parent_key_canonical COLLATE "C" IS DISTINCT FROM current_raw.raw_parent_key_canonical COLLATE "C") raw_collision,
        roots.raw_root_count,typed.typed_root_count,duplicates.child_count,
        final.child_revision_id final_child_revision_id,
        (child.payload_sha256 IS DISTINCT FROM final_payload.payload_sha256
          OR child.canonical_payload COLLATE "C" IS DISTINCT FROM final_payload.canonical_payload COLLATE "C") payload_differs,
        (first_child.canonical_child_key COLLATE "C" IS DISTINCT FROM child.canonical_child_key COLLATE "C") child_key_collision,
        c.raw_parent_bytes
          +coalesce(octet_length(k.canonical_logical_key),0)
          +coalesce(first.raw_parent_bytes,0)
          +CASE WHEN c.record_kind='child' THEN coalesce(octet_length(parent.raw_parent_key_canonical),0) ELSE 0 END
          +coalesce(octet_length(child.canonical_child_key),0)*2
          +coalesce(octet_length(final_payload.canonical_payload),0)
          +CASE WHEN c.collapse_identical THEN coalesce(octet_length(child.canonical_payload),0) ELSE 0 END
          +CASE WHEN :memberships<>'[]'::jsonb AND child.child_revision_id IS NOT NULL
            THEN octet_length(child.canonical_child_key)*3+octet_length(CAST(:memberships AS text))*2 ELSE 0 END raw_bytes,
        (pack.pack_id IS NULL OR ROW(pack.execution_id,pack.producing_fence,pack.producing_token_sha256,
            pack.capture_bundle_id,pack.stream_slot,pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id)
          IS DISTINCT FROM ROW(:execution_id,:producing_fence,:producing_token_sha256,:capture_bundle_id,
            c.stream_slot,:dataset_id,:definition_revision_id,:schema_revision_id)
          OR stream.build_id IS NULL OR stream.next_pack_ordinal<=pack.pack_ordinal) invalid_pack
    FROM candidates c
    JOIN __CANDIDATE__.custom_import_build_occurrence current_raw ON current_raw.occurrence_id=c.occurrence_id
    LEFT JOIN __CANDIDATE__.custom_import_root_record k ON k.root_record_id=c.root_record_id AND k.dataset_id=:dataset_id
    LEFT JOIN __CANDIDATE__.custom_import_child_revision child ON child.child_revision_id=c.child_revision_id
    LEFT JOIN first_raw first ON first.record_kind=c.record_kind AND first.raw_parent_key_sha256=c.raw_parent_key_sha256
    LEFT JOIN __CANDIDATE__.custom_import_build_occurrence first_raw_row ON first_raw_row.occurrence_id=first.first_id
    LEFT JOIN source_roots roots ON roots.raw_parent_key_sha256=c.raw_parent_key_sha256
    LEFT JOIN __CANDIDATE__.custom_import_build_occurrence parent ON parent.occurrence_id=roots.parent_id
    LEFT JOIN typed_roots typed ON typed.root_record_id=c.root_record_id
    LEFT JOIN child_duplicates duplicates ON duplicates.root_record_id=c.root_record_id
      AND duplicates.raw_parent_key_sha256=c.raw_parent_key_sha256
      AND duplicates.collection_slot=c.collection_slot AND duplicates.child_key_sha256=c.child_key_sha256
    LEFT JOIN first_typed_child first_id ON first_id.root_record_id=c.root_record_id
      AND first_id.collection_slot=c.collection_slot AND first_id.child_key_sha256=c.child_key_sha256
    LEFT JOIN __CANDIDATE__.custom_import_child_revision first_child ON first_child.child_revision_id=first_id.child_revision_id
    LEFT JOIN final_child final ON final.stream_slot=c.stream_slot AND final.root_record_id=c.root_record_id
      AND final.collection_slot=c.collection_slot AND final.raw_parent_key_sha256=c.raw_parent_key_sha256
      AND final.child_key_sha256=c.child_key_sha256
    LEFT JOIN __CANDIDATE__.custom_import_child_revision final_payload ON final_payload.child_revision_id=final.child_revision_id
    LEFT JOIN __CANDIDATE__.custom_import_pack pack ON pack.pack_id=c.pack_id
    LEFT JOIN __CONTROL__.custom_import_build_stream stream ON stream.build_id=:build_id AND stream.stream_slot=c.stream_slot
),
logical_rows AS MATERIALIZED (
    SELECT m.*,(row_number() OVER (ORDER BY m.occurrence_id)-1)/:page_row_limit logical_group
    FROM metadata m
),
sized AS MATERIALIZED (
    SELECT m.*,sum(m.raw_bytes::bigint) OVER (
        PARTITION BY m.logical_group ORDER BY m.occurrence_id ROWS UNBOUNDED PRECEDING) prefix_bytes
    FROM logical_rows m
),
logical_groups AS MATERIALIZED (
    SELECT s.logical_group,count(*) row_count,sum(s.raw_bytes::bigint) group_bytes,
        max(s.occurrence_id) last_id
    FROM sized s GROUP BY s.logical_group
),
group_bounds AS MATERIALIZED (
    SELECT g.*,sum(g.group_bytes) OVER (ORDER BY g.logical_group ROWS UNBOUNDED PRECEDING) physical_bytes,
        CASE WHEN g.row_count=:page_row_limit THEN true ELSE NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence x
            WHERE x.build_id=:build_id AND x.origin='source' AND x.occurrence_id>g.last_id
        ) END complete_group
    FROM logical_groups g
),
group_boundary AS MATERIALIZED (
    SELECT g.logical_group,g.group_bytes>:page_byte_limit logical_byte_overflow
    FROM group_bounds g
    WHERE NOT g.complete_group OR g.group_bytes>:page_byte_limit OR g.physical_bytes>268435456
    ORDER BY g.logical_group LIMIT 1
),
page AS MATERIALIZED (
    SELECT s.* FROM sized s
    WHERE s.logical_group<coalesce((SELECT g.logical_group FROM group_boundary g),100000)
      OR (s.logical_group=0 AND s.prefix_bytes<=:page_byte_limit AND EXISTS (
        SELECT 1 FROM group_boundary g WHERE g.logical_group=0 AND g.logical_byte_overflow
      ))
),
byte_boundary AS MATERIALIZED (
    SELECT s.occurrence_id,s.raw_bytes FROM sized s
    WHERE s.logical_group=0 AND s.prefix_bytes>:page_byte_limit AND EXISTS (
        SELECT 1 FROM group_boundary g WHERE g.logical_group=0 AND g.logical_byte_overflow
    ) ORDER BY s.occurrence_id LIMIT 1
),
primary_codes AS MATERIALIZED (
    SELECT p.*,child.canonical_child_key membership_key,
        CASE
          WHEN p.resolved_rejection_id IS NOT NULL THEN p.initial_code
          WHEN p.record_kind='root' THEN
            CASE WHEN p.initial_code IS NOT NULL THEN p.initial_code
              WHEN p.raw_root_count>1 OR p.typed_root_count>1 THEN 'duplicate_root_key' END
          WHEN p.initial_code='child_not_object' THEN p.initial_code
          WHEN p.parent_id IS NULL OR p.parent_raw_differs
            THEN 'orphan_child'
          WHEN p.initial_code IS NULL AND p.child_count>1
            AND (NOT p.collapse_identical OR p.final_child_revision_id IS NULL OR p.payload_differs)
            THEN 'duplicate_child_key'
          ELSE p.initial_code
        END primary_code
    FROM page p LEFT JOIN __CANDIDATE__.custom_import_child_revision child ON child.child_revision_id=p.child_revision_id
),
membership_requirements AS MATERIALIZED (
    SELECT p.occurrence_id,p.root_record_id,p.membership_key,m.value relation,m.ordinality relation_order,
        outer_collection.collection_slot outer_slot
    FROM primary_codes p
    JOIN __CONTROL__.custom_import_child_collection inner_collection
      ON inner_collection.schema_revision_id=:schema_revision_id AND inner_collection.collection_slot=p.collection_slot
    JOIN jsonb_array_elements(:memberships) WITH ORDINALITY m(value,ordinality)
      ON m.value->>'inner_collection'=inner_collection.collection_name
    LEFT JOIN __CONTROL__.custom_import_child_collection outer_collection
      ON outer_collection.schema_revision_id=:schema_revision_id AND outer_collection.collection_name=m.value->>'outer_collection'
    WHERE p.resolved_rejection_id IS NULL AND p.primary_code IS NULL AND p.child_revision_id IS NOT NULL
),
mapped_memberships AS MATERIALIZED (
    SELECT r.occurrence_id,r.root_record_id,r.relation_order,r.outer_slot,
        bool_or(field.value->'value' IS NULL OR field.value->'value'->>'state' IS DISTINCT FROM 'value'
            OR coalesce(field.value->'value'->>'type' NOT IN ('string','integer'),true)) malformed_key,
        '{"contract":"custom-import-key/v1","fields":['||string_agg(
          '{"field":'||to_json(mapping.value->>'outer_field')::text
          ||',"value":{"state":"value","type":'||to_json(field.value->'value'->>'type')::text
          ||',"value":'||(field.value->'value'->'value')::text||'}}',',' ORDER BY mapping.ordinality)||']}' expected_key
    FROM membership_requirements r
    JOIN jsonb_array_elements(r.relation->'key_mapping') WITH ORDINALITY mapping(value,ordinality) ON true
    LEFT JOIN jsonb_array_elements(r.membership_key::jsonb->'fields') field(value)
      ON field.value->>'field'=mapping.value->>'inner_field'
    GROUP BY r.occurrence_id,r.root_record_id,r.relation_order,r.outer_slot
),
membership_keys AS MATERIALIZED (
    SELECT m.*,sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31006368696c642d6b657900','hex')
        ||convert_to(m.expected_key,'UTF8')) expected_hash FROM mapped_memberships m
),
requested_memberships AS MATERIALIZED (
    SELECT DISTINCT m.root_record_id,m.outer_slot,m.expected_hash FROM membership_keys m WHERE NOT m.malformed_key
),
membership_peers AS MATERIALIZED (
    SELECT DISTINCT ON (x.root_record_id,x.collection_slot,x.child_key_sha256)
        x.root_record_id,x.collection_slot,x.child_key_sha256,c.canonical_child_key
    FROM requested_memberships m
    JOIN __CANDIDATE__.custom_import_build_occurrence x ON x.build_id=:build_id AND x.origin='source'
      AND x.root_record_id=m.root_record_id AND x.collection_slot=m.outer_slot
      AND x.child_key_sha256=m.expected_hash AND x.child_revision_id IS NOT NULL
    JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id
    ORDER BY x.root_record_id,x.collection_slot,x.child_key_sha256,x.child_revision_id
),
membership_checks AS MATERIALIZED (
    SELECT m.occurrence_id,m.relation_order,
        CASE WHEN m.malformed_key THEN 'custom_import_build_structure_mismatch: membership key differs'
          WHEN peer.canonical_child_key IS NULL THEN 'child_membership_missing'
          WHEN peer.canonical_child_key COLLATE "C" IS DISTINCT FROM m.expected_key COLLATE "C"
            THEN 'custom_import_build_structure_mismatch: membership key digest collision' END problem
    FROM membership_keys m
    LEFT JOIN membership_peers peer ON peer.root_record_id=m.root_record_id AND peer.collection_slot=m.outer_slot
      AND peer.child_key_sha256=m.expected_hash
),
first_membership_problem AS MATERIALIZED (
    SELECT DISTINCT ON (m.occurrence_id) m.occurrence_id,m.problem
    FROM membership_checks m WHERE m.problem IS NOT NULL ORDER BY m.occurrence_id,m.relation_order
),
local_codes AS MATERIALIZED (
    SELECT p.*,m.problem membership_problem,
        CASE WHEN p.primary_code IS NULL AND m.problem='child_membership_missing'
          THEN m.problem ELSE p.primary_code END code
    FROM primary_codes p LEFT JOIN first_membership_problem m ON m.occurrence_id=p.occurrence_id
),
first_child_rejection AS MATERIALIZED (
    SELECT c.parent_id,min(c.occurrence_id) child_event_id FROM local_codes c
    WHERE c.record_kind='child' AND c.resolved_rejection_id IS NULL AND c.code IS NOT NULL
      AND c.code<>'orphan_child' AND c.parent_id IS NOT NULL
    GROUP BY c.parent_id
),
ordered_decisions AS MATERIALIZED (
    SELECT c.*,
        (c.resolved_rejection_id IS NULL AND c.code IS NOT NULL
          AND NOT coalesce(c.record_kind='root' AND earlier.child_event_id<c.occurrence_id,false)) own_action,
        (c.resolved_rejection_id IS NULL
          AND NOT coalesce(c.record_kind='root' AND earlier.child_event_id<c.occurrence_id,false)) validate_local,
        CASE WHEN c.resolved_rejection_id IS NOT NULL OR (c.record_kind='root' AND earlier.child_event_id<c.occurrence_id)
          THEN c.initial_code ELSE c.code END error_code
    FROM local_codes c LEFT JOIN first_child_rejection earlier ON earlier.parent_id=c.occurrence_id
),
failures AS MATERIALIZED (
    SELECT boundary.occurrence_id,-1 stage,'custom_import_build_page_too_large' problem
    FROM byte_boundary boundary WHERE boundary.raw_bytes>:page_byte_limit
    UNION ALL
    SELECT d.occurrence_id,-1,'custom_import_build_identity_mismatch'
    FROM ordered_decisions d WHERE d.invalid_pack
    UNION ALL
    SELECT d.occurrence_id,0,'custom_import_build_structure_mismatch: raw key digest collision'
    FROM ordered_decisions d WHERE d.validate_local AND d.raw_parent_key_sha256 IS NOT NULL
      AND d.raw_collision
    UNION ALL
    SELECT d.occurrence_id,1,d.membership_problem FROM ordered_decisions d
    WHERE d.validate_local AND d.membership_problem IS NOT NULL AND d.membership_problem<>'child_membership_missing'
    UNION ALL
    SELECT d.occurrence_id,2,'custom_import_build_structure_mismatch: child key digest collision'
    FROM ordered_decisions d WHERE d.validate_local AND d.record_kind='child' AND d.child_revision_id IS NOT NULL
      AND d.child_key_collision
)
SELECT (SELECT f.problem FROM failures f ORDER BY f.occurrence_id,f.stage LIMIT 1) fatal_code,
    count(*)::integer rows_processed,coalesce(max(d.occurrence_id),:admission_after_occurrence_id) last_id,
    count(*) FILTER (WHERE d.error_code IN ('root_not_object','root_key_missing','child_not_object','orphan_child')) errors,
    coalesce(jsonb_agg(jsonb_build_object(
        'occurrence_id',d.occurrence_id,'parent_id',d.parent_id,'record_kind',d.record_kind,
        'pack_id',d.pack_id,'source_ordinal',d.source_ordinal,'collection_slot',d.collection_slot,
        'initial_code',d.initial_code,'code',d.code,'rejection_id',d.rejection_id,
        'own_action',coalesce(d.own_action,false),'root_record_id',d.root_record_id
    ) ORDER BY d.occurrence_id),'[]'::jsonb)
  decisions FROM ordered_decisions d;

-- statement boundary --

WITH
page_decisions AS MATERIALIZED (
    SELECT d.* FROM jsonb_to_recordset(:decisions) AS d(
        occurrence_id bigint,parent_id bigint,record_kind text,pack_id bigint,source_ordinal bigint,
        collection_slot smallint,initial_code text,code text,rejection_id bigint,own_action boolean,
        root_record_id bigint
    )
),
new_rejection_events AS MATERIALIZED (
    SELECT d.*,:next_rejection_ordinal+row_number() OVER (ORDER BY d.occurrence_id)-1 rejection_ordinal
    FROM page_decisions d WHERE d.own_action AND d.code IS DISTINCT FROM d.initial_code
),
new_rejections AS (
    INSERT INTO __CANDIDATE__.custom_import_rejection(
        rejection_id,execution_id,rejection_ordinal,dataset_id,definition_revision_id,schema_revision_id,pack_id,
        root_key_sha256,canonical_root_key,collection_slot,source_ordinal,code,canonical_evidence,
        producing_fence,producing_token_sha256
    )
    SELECT nextval(CAST(:rejection_sequence AS regclass)),
        :execution_id,d.rejection_ordinal,:dataset_id,:definition_revision_id,:schema_revision_id,d.pack_id,
        root.logical_key_sha256,root.canonical_logical_key,nullif(d.collection_slot,0),d.source_ordinal,d.code,'{}',
        :producing_fence,:producing_token_sha256 FROM new_rejection_events d
    LEFT JOIN __CANDIDATE__.custom_import_root_record root ON root.root_record_id=d.root_record_id AND root.dataset_id=:dataset_id
    ORDER BY d.occurrence_id
    RETURNING rejection_ordinal,rejection_id,execution_id,dataset_id,definition_revision_id,
        schema_revision_id,producing_fence,producing_token_sha256
),
events AS MATERIALIZED (
    SELECT d.*,coalesce(created.rejection_id,d.rejection_id) resolution
    FROM page_decisions d
    LEFT JOIN new_rejection_events numbered ON numbered.occurrence_id=d.occurrence_id
    LEFT JOIN new_rejections created ON created.rejection_ordinal=numbered.rejection_ordinal
    WHERE d.own_action
),
targets AS MATERIALIZED (
    SELECT e.occurrence_id target_id,e.occurrence_id event_id,e.resolution FROM events e
    UNION ALL
    SELECT e.parent_id,e.occurrence_id,e.resolution FROM events e
    WHERE e.record_kind='child' AND e.code<>'orphan_child' AND e.parent_id IS NOT NULL
),
chosen_targets AS MATERIALIZED (
    SELECT DISTINCT ON (t.target_id) t.target_id,t.resolution
    FROM targets t
    JOIN __CANDIDATE__.custom_import_build_occurrence x ON x.occurrence_id=t.target_id AND x.build_id=:build_id
      AND x.origin='source' AND x.resolved_rejection_id IS NULL
    ORDER BY t.target_id,t.event_id
),
updated AS (
    UPDATE __CANDIDATE__.custom_import_build_occurrence x SET resolved_rejection_id=t.resolution
    FROM chosen_targets t WHERE x.occurrence_id=t.target_id AND x.build_id=:build_id
      AND x.resolved_rejection_id IS NULL
    RETURNING x.occurrence_id,x.resolved_rejection_id
)
SELECT (SELECT count(*) FROM new_rejections) inserted_n,(SELECT count(*) FROM updated) updated_n,
    (SELECT count(*) FROM new_rejection_events) expected_inserted_n,(SELECT count(*) FROM chosen_targets) expected_updated_n,
    (SELECT count(*) FROM updated u
        LEFT JOIN new_rejections fresh ON fresh.rejection_id=u.resolved_rejection_id
        LEFT JOIN __CANDIDATE__.custom_import_rejection prior ON prior.rejection_id=u.resolved_rejection_id
        WHERE coalesce(fresh.rejection_id,prior.rejection_id) IS NULL
            OR ROW(coalesce(fresh.execution_id,prior.execution_id),coalesce(fresh.dataset_id,prior.dataset_id),
                coalesce(fresh.definition_revision_id,prior.definition_revision_id),coalesce(fresh.schema_revision_id,prior.schema_revision_id),
                coalesce(fresh.producing_fence,prior.producing_fence),coalesce(fresh.producing_token_sha256,prior.producing_token_sha256))
                IS DISTINCT FROM ROW(:execution_id,:dataset_id,:definition_revision_id,:schema_revision_id,
                    :producing_fence,:producing_token_sha256))
  invalid_resolution_n;

-- statement boundary --

WITH tail AS MATERIALIZED (
    SELECT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence x
        WHERE x.build_id=:build_id AND x.origin='source' AND x.occurrence_id>:last_id
    ) remaining
), delta AS MATERIALIZED (
    SELECT t.remaining,:errors+CASE
        WHEN NOT t.remaining AND b.refresh_mode='snapshot' AND NOT b.complete_scope THEN 1 ELSE 0 END errors
    FROM tail t JOIN __CONTROL__.custom_import_build_attempt b ON b.build_id=:build_id
)
UPDATE __CONTROL__.custom_import_build_attempt a
SET admission_after_occurrence_id=:last_id,
    candidate_error_count=a.candidate_error_count+d.errors,
    next_rejection_ordinal=a.next_rejection_ordinal+:inserted_n,
    phase=CASE WHEN d.remaining THEN 'admission'
        WHEN a.candidate_error_count+d.errors>0 THEN 'rejected' ELSE 'graph' END
FROM delta d WHERE a.build_id=:build_id
RETURNING a.phase::text phase,a.admission_after_occurrence_id after_occurrence_id,
    CAST(:rows_processed AS integer) rows_processed,a.candidate_error_count;
