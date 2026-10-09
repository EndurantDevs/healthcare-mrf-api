-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Fixed ordinary application SQL: identifiers are verified registry/model namespaces;
-- values come from the current transaction's persisted authorization and build.

-- query: scope
SELECT a.accepting AS a_accepting,
    a.batch_id AS a_batch_id,
    a.build_id AS a_build_id,
    a.capture_bundle_id AS a_capture_bundle_id,
    a.dataset_id AS a_dataset_id,
    a.definition_revision_id AS a_definition_revision_id,
    a.execution_id AS a_execution_id,
    a.expected_count AS a_expected_count,
    a.fence AS a_fence,
    a.first_pack AS a_first_pack,
    a.first_part AS a_first_part,
    a.first_row AS a_first_row,
    a.first_source AS a_first_source,
    a.opened_by AS a_opened_by,
    a.schema_revision_id AS a_schema_revision_id,
    a.source_byte_limit AS a_source_byte_limit,
    a.stream_slot AS a_stream_slot,
    a.token_hash AS a_token_hash,
    a.transaction_id::text AS a_transaction_id,
    b.build_deadline_at AS b_build_deadline_at,
    b.build_id AS b_build_id,
    b.capture_bundle_id AS b_capture_bundle_id,
    b.dataset_id AS b_dataset_id,
    b.definition_revision_id AS b_definition_revision_id,
    b.execution_id AS b_execution_id,
    b.next_rejection_ordinal AS b_next_rejection_ordinal,
    b.page_byte_limit AS b_page_byte_limit,
    b.page_row_limit AS b_page_row_limit,
    b.phase AS b_phase,
    b.producing_fence AS b_producing_fence,
    b.producing_token_sha256 AS b_producing_token_sha256,
    b.schema_revision_id AS b_schema_revision_id,
    b.source_occurrence_count AS b_source_occurrence_count,
    s.build_id AS s_build_id,
    s.next_pack_ordinal AS s_next_pack_ordinal,
    s.next_part_ordinal AS s_next_part_ordinal,
    s.next_part_row_ordinal AS s_next_part_row_ordinal,
    s.next_source_ordinal AS s_next_source_ordinal,
    s.replay_verified_at AS s_replay_verified_at,
    stream.collection_slot AS stream_collection_slot,
    stream.dataset_id AS stream_dataset_id,
    stream.record_kind AS stream_record_kind,
    stream.schema_revision_id AS stream_schema_revision_id,
    stream.stream_slot AS stream_stream_slot
FROM __CONTROL__.source_bulk_authorization a
JOIN __CONTROL__.custom_import_build_attempt b ON b.build_id=a.build_id
JOIN __CONTROL__.custom_import_build_stream s ON s.build_id=b.build_id AND s.stream_slot=a.stream_slot
LEFT JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=b.definition_revision_id
    AND stream.stream_slot=a.stream_slot
WHERE a.batch_id=:p_batch FOR UPDATE OF a,b,s;

-- query: row_guards
SELECT CASE WHEN EXISTS(SELECT 1 FROM pg_trigger t
        WHERE t.tgrelid=ANY(ARRAY[
            CAST(:custom_import_root_record_relation AS regclass),CAST(:custom_import_pack_relation AS regclass),
            CAST(:custom_import_root_revision_relation AS regclass),
            CAST(:custom_import_child_revision_relation AS regclass),CAST(:custom_import_rejection_relation AS regclass),
            CAST(:custom_import_build_occurrence_relation AS regclass)])
        AND NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4) THEN 'source_set_row_guards_require_all_writer_retirement' END problem;

-- query: relationship_guards
SELECT CASE WHEN EXISTS(SELECT 1 FROM pg_constraint c WHERE c.contype='f' AND c.conrelid=ANY(ARRAY[
        CAST(:source_bulk_landing_relation AS regclass),CAST(:custom_import_root_record_relation AS regclass),
        CAST(:custom_import_pack_relation AS regclass),CAST(:custom_import_root_revision_relation AS regclass),
        CAST(:custom_import_child_revision_relation AS regclass),CAST(:custom_import_rejection_relation AS regclass),
        CAST(:custom_import_build_occurrence_relation AS regclass)])) THEN 'source_set_hot_relationship_guards_present' END problem;

-- query: authority
SELECT CASE WHEN :a_batch_id IS NULL OR NOT :a_accepting OR :a_opened_by<>session_user
        OR CAST(:a_transaction_id AS xid8)<>pg_current_xact_id() THEN 'source_bulk_authority_mismatch' END problem;

-- query: binding
SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
        WHERE r.relation_slot=1 AND r.table_oid=CAST(:custom_import_root_record_relation AS regclass)::oid
            AND f.landing_table_oid=CAST(:source_bulk_landing_relation AS regclass)::oid
            AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
            AND f.frozen_at IS NULL
            AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,:b_execution_id,
                    :b_capture_bundle_id,:b_producing_fence,:b_producing_token_sha256)) THEN 'custom_import_source_snapshot_binding_mismatch' END problem;

-- query: locked_authority
SELECT CASE WHEN :a_batch_id IS NULL OR NOT :a_accepting OR :a_opened_by<>session_user
        OR CAST(:a_transaction_id AS xid8)<>pg_current_xact_id() OR :a_build_id<>:b_build_id THEN 'source_bulk_authority_mismatch' END problem;

-- query: phase
SELECT CASE WHEN :b_phase<>'source' OR ROW(:b_producing_fence,:b_producing_token_sha256)
        IS DISTINCT FROM ROW(:a_fence,:a_token_hash) THEN 'source_bulk_authority_mismatch' END problem;

-- query: identity
SELECT CASE WHEN ROW(:a_dataset_id,:a_definition_revision_id,:a_schema_revision_id,:a_execution_id,:a_capture_bundle_id)
        IS DISTINCT FROM ROW(:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,:b_execution_id,:b_capture_bundle_id) THEN 'source_bulk_authority_mismatch' END problem;

-- query: lock_landing
LOCK TABLE __CANDIDATE__.source_bulk_landing IN SHARE ROW EXCLUSIVE MODE;

-- query: landing_bound
SELECT CASE WHEN (SELECT count(*) FROM (SELECT 1 FROM __CANDIDATE__.source_bulk_landing LIMIT 100001) bounded_landing)>100000 THEN 'source_bulk_bounds' END problem;

-- query: landing_owner
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        LEFT JOIN __CONTROL__.source_bulk_authorization authorized
            ON authorized.batch_id=l.batch_id AND authorized.transaction_id=l.transaction_id
            AND authorized.accepting=l.accepting AND authorized.accepting
            AND authorized.opened_by=session_user AND authorized.transaction_id=pg_current_xact_id()
            AND ROW(authorized.build_id,authorized.dataset_id,authorized.definition_revision_id,
                authorized.schema_revision_id,authorized.execution_id,authorized.capture_bundle_id,
                authorized.fence,authorized.token_hash)
                IS NOT DISTINCT FROM ROW(:b_build_id,:b_dataset_id,:b_definition_revision_id,
                    :b_schema_revision_id,:b_execution_id,:b_capture_bundle_id,:b_producing_fence,:b_producing_token_sha256)
        LEFT JOIN __CONTROL__.custom_import_build_stream authorized_stream
            ON authorized_stream.build_id=authorized.build_id AND authorized_stream.stream_slot=authorized.stream_slot
            AND authorized_stream.replay_verified_at IS NULL
        LEFT JOIN __CONTROL__.custom_import_source_stream declared_stream
            ON declared_stream.definition_revision_id=:b_definition_revision_id
            AND declared_stream.stream_slot=authorized.stream_slot
            AND declared_stream.dataset_id=:b_dataset_id AND declared_stream.schema_revision_id=:b_schema_revision_id
        WHERE authorized.batch_id IS NULL OR authorized_stream.build_id IS NULL OR declared_stream.stream_slot IS NULL) THEN 'source_bulk_landing_owner_mismatch' END problem;

-- query: sealed
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=:b_execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=:b_execution_id) THEN 'source_bulk_sealed_attempt' END problem;

-- query: cursor
SELECT CASE WHEN :s_build_id IS NULL OR :s_replay_verified_at IS NOT NULL OR ROW(:s_next_pack_ordinal,:s_next_part_ordinal,:s_next_part_row_ordinal,:s_next_source_ordinal)
        IS DISTINCT FROM ROW(:a_first_pack,:a_first_part,:a_first_row,:a_first_source) THEN 'source_bulk_cursor_changed' END problem;

-- query: stream
SELECT CASE WHEN :stream_stream_slot IS NULL OR ROW(:stream_dataset_id,:stream_schema_revision_id)
        IS DISTINCT FROM ROW(:b_dataset_id,:b_schema_revision_id) OR :stream_record_kind NOT IN ('root','child') THEN 'source_bulk_stream_mismatch' END problem;

-- query: metadata
SELECT :stream_record_kind AS kind,
    CASE WHEN :stream_record_kind='root' THEN 'root' ELSE collection.value->>'name' END label,
    __CONTROL__.source_bulk_digest('root-key-contract',__CONTROL__.source_bulk_canonical(jsonb_build_object(
        'contract','custom-import-root-key/v1','fields',d.canonical_definition::jsonb#>'{schema,root,logical_key}'))) key_contract,
    (SELECT payload_part_count FROM __CONTROL__.custom_import_capture
        WHERE capture_bundle_id=:b_capture_bundle_id AND stream_slot=:a_stream_slot AND capture_state='sealed') part_count
FROM __CONTROL__.custom_import_definition_revision d
LEFT JOIN LATERAL (
    SELECT x value FROM jsonb_array_elements(d.canonical_definition::jsonb#>'{schema,children}') x
    JOIN __CONTROL__.custom_import_child_collection c ON c.collection_name=x->>'name'
    WHERE c.schema_revision_id=:b_schema_revision_id AND c.collection_slot=:stream_collection_slot
) collection ON true
WHERE d.definition_revision_id=:b_definition_revision_id;

-- query: landing_counts
SELECT count(*) AS n,
    sum(coalesce(octet_length(raw_key),0)+coalesce(octet_length(typed_key),0)
        +coalesce(octet_length(payload),0)+coalesce(octet_length(child_key),0)
        +CASE WHEN :kind='child' AND payload IS NOT NULL THEN coalesce(octet_length(typed_key),0) ELSE 0 END
        +coalesce(octet_length(rejection_key),0)+coalesce(octet_length(rejection_evidence),0)) AS bytes FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch;

-- query: batch_bounds
SELECT CASE WHEN :part_count IS NULL OR :n NOT BETWEEN 1 AND 100000 OR :n<>:a_expected_count
        OR :bytes IS NULL OR :bytes>:a_source_byte_limit OR :bytes>268435456 THEN 'source_bulk_bounds' END problem;

-- query: row_identity
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l WHERE l.batch_id=:p_batch AND (
        l.transaction_id<>CAST(:a_transaction_id AS xid8) OR l.landing_ordinal<>l.source_ordinal-:a_first_source
        OR l.pack_id IS NOT NULL OR l.root_id IS NOT NULL OR l.outcome_id IS NOT NULL
        OR num_nonnulls(l.raw_key,l.raw_hash) NOT IN (0,2) OR num_nonnulls(l.typed_key,l.typed_hash) NOT IN (0,2)
        OR (l.raw_key IS NOT NULL AND l.raw_hash IS DISTINCT FROM sha256(convert_to('custom-import/raw-family-key/v1:'||l.raw_key,'UTF8')))
        OR (l.typed_key IS NOT NULL AND l.typed_hash IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')
            ||convert_to('root-key','UTF8')||decode('00','hex')||convert_to(l.typed_key,'UTF8')))
        OR (l.rejection_code IS NULL AND (l.payload IS NULL OR l.typed_key IS NULL OR l.raw_key IS NULL
            OR l.payload_hash IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')
                ||convert_to(:kind||'-payload','UTF8')||decode('00','hex')||convert_to(l.payload,'UTF8'))
            OR num_nonnulls(l.rejection_key,l.rejection_hash,l.rejection_evidence)<>0
            OR (:kind='root' AND num_nonnulls(l.child_key,l.child_hash)<>0)
            OR (:kind='child' AND (l.child_key IS NULL OR l.child_hash IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')
                ||convert_to('child-key','UTF8')||decode('00','hex')||convert_to(l.child_key,'UTF8'))))))
        OR (l.raw_hash IS NOT NULL AND octet_length(l.raw_hash)<>32)
        OR (l.typed_hash IS NOT NULL AND octet_length(l.typed_hash)<>32)
        OR (l.rejection_code IS NOT NULL AND (num_nonnulls(l.payload,l.payload_hash,l.child_key,l.child_hash)<>0
            OR l.rejection_code !~ '^[a-z][a-z0-9_]{0,62}$' OR l.rejection_evidence IS NULL
            OR l.rejection_key IS DISTINCT FROM l.typed_key OR l.rejection_hash IS DISTINCT FROM l.typed_hash
            OR l.rejection_evidence IS DISTINCT FROM ('{"code":'||to_json(l.rejection_code)::text
                ||',"contract":"custom-import-rejection/v1","root_key_sha256":'
                ||coalesce(to_json(encode(l.rejection_hash,'hex'))::text,'null')||'}')))
    )) THEN 'source_bulk_row_mismatch' END problem;

-- query: analyze_landing
ANALYZE __CANDIDATE__.source_bulk_landing(batch_id,landing_ordinal,pack_ordinal,pack_id,root_id,
        outcome_id,part_ordinal,part_row_ordinal,source_ordinal,typed_hash,rejection_code);

-- query: ordinal_bounds
SELECT CASE WHEN (SELECT ROW(min(landing_ordinal),max(landing_ordinal)) FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch)
        IS DISTINCT FROM ROW(0,CAST(:n-1 AS integer)) THEN 'source_bulk_position_gap' END problem;

-- query: key_collisions
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND typed_hash IS NOT NULL
        GROUP BY typed_hash HAVING count(DISTINCT convert_to(typed_key,'UTF8'))<>1) THEN 'source_bulk_root_collision' END problem;

-- query: pack_counts
SELECT count(DISTINCT pack_ordinal) AS pack_n,
    max(part_ordinal) AS last_part,
    count(*) FILTER (WHERE rejection_code IS NOT NULL) AS rejected_n FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch;

-- query: last_row
SELECT max(part_row_ordinal)+1 AS last_row FROM __CANDIDATE__.source_bulk_landing
        WHERE batch_id=:p_batch AND part_ordinal=:last_part;

-- query: verified_parts
SELECT CASE WHEN :last_part>:part_count OR :p_verified_parts IS NULL
        OR cardinality(:p_verified_parts)<>(SELECT count(DISTINCT value) FROM unnest(:p_verified_parts) value)
        OR cardinality(:p_verified_parts)<>:last_part-:a_first_part
        OR EXISTS(SELECT 1 FROM unnest(:p_verified_parts) value
            WHERE value IS NULL OR value<:a_first_part OR value>=:last_part)
        OR (SELECT count(*) FROM __CONTROL__.custom_import_capture_parquet_part p
            WHERE p.capture_bundle_id=:b_capture_bundle_id AND p.stream_slot=:a_stream_slot
                AND p.part_ordinal BETWEEN :a_first_part AND :last_part-1)<>:last_part-:a_first_part
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_capture_parquet_part p
            LEFT JOIN (SELECT part_ordinal,max(part_row_ordinal)+1 row_end
                FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch GROUP BY part_ordinal) l
                ON l.part_ordinal=p.part_ordinal
            WHERE p.capture_bundle_id=:b_capture_bundle_id AND p.stream_slot=:a_stream_slot
                AND p.part_ordinal BETWEEN :a_first_part AND :last_part-1
                AND (NOT(p.part_ordinal=ANY(:p_verified_parts)) OR p.record_count<>coalesce(l.row_end,
                    CASE WHEN p.part_ordinal=:a_first_part THEN :a_first_row ELSE 0 END))) THEN 'source_set_earlier_part_not_verified' END problem;

-- query: positions
SELECT CASE WHEN EXISTS(WITH ordered AS (SELECT l.*,lag(part_ordinal) OVER w previous_part,
            lag(part_row_ordinal) OVER w previous_row,lag(pack_ordinal) OVER w previous_pack
            FROM __CANDIDATE__.source_bulk_landing l WHERE batch_id=:p_batch WINDOW w AS (ORDER BY landing_ordinal))
        SELECT 1 FROM ordered o LEFT JOIN __CONTROL__.custom_import_capture_parquet_part p
            ON p.capture_bundle_id=:b_capture_bundle_id AND p.stream_slot=:a_stream_slot AND p.part_ordinal=o.part_ordinal
        WHERE p.part_ordinal IS NULL OR o.part_ordinal<:a_first_part OR o.part_row_ordinal>=p.record_count
            OR (o.landing_ordinal=0 AND (ROW(o.pack_ordinal,o.source_ordinal) IS DISTINCT FROM ROW(:a_first_pack,:a_first_source)
                OR o.part_row_ordinal<>CASE WHEN o.part_ordinal=:a_first_part THEN :a_first_row ELSE 0 END))
            OR (o.landing_ordinal>0 AND NOT ((o.part_ordinal=o.previous_part AND o.part_row_ordinal=o.previous_row+1)
                OR (o.part_ordinal>o.previous_part AND o.part_row_ordinal=0)))
            OR (o.landing_ordinal>0 AND o.pack_ordinal NOT IN (o.previous_pack,o.previous_pack+1))) THEN 'source_bulk_position_gap' END problem;

-- query: pack_identity
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch GROUP BY pack_ordinal HAVING
        count(*)>:b_page_row_limit OR count(DISTINCT part_ordinal)<>1 OR count(DISTINCT pack_sha256)<>1
        OR sum(coalesce(octet_length(raw_key),0)+coalesce(octet_length(typed_key),0)+coalesce(octet_length(payload),0)
            +coalesce(octet_length(child_key),0)+CASE WHEN :kind='child' AND payload IS NOT NULL THEN coalesce(octet_length(typed_key),0) ELSE 0 END
            +coalesce(octet_length(rejection_key),0)+coalesce(octet_length(rejection_evidence),0))>:b_page_byte_limit
        OR (array_agg(pack_sha256 ORDER BY landing_ordinal))[1] IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
            ||convert_to(:label,'UTF8')||decode('00','hex')||coalesce(string_agg(payload_hash,''::bytea ORDER BY payload_hash),''::bytea))) THEN 'source_bulk_pack_mismatch' END problem;

-- query: dictionary_counts
SELECT count(*) AS dictionary_n,
    coalesce(sum(octet_length(typed_key)::bigint+88),0) AS dictionary_bytes,
    coalesce(sum(reference_count),0) AS dictionary_reference_n FROM (SELECT typed_key,typed_hash,count(*) reference_count FROM __CANDIDATE__.source_bulk_landing
            WHERE batch_id=:p_batch AND typed_key IS NOT NULL GROUP BY typed_key,typed_hash) keys;

-- query: dictionary_bound
SELECT CASE WHEN :dictionary_n>:n OR :dictionary_bytes>:a_source_byte_limit+88::bigint*:n THEN 'source_set_dictionary_bounds' END problem;

-- query: insert_global_keys
WITH written AS (INSERT INTO __CONTROL__.custom_import_root_record(dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256)
        SELECT :b_dataset_id,:key_contract,typed_key,typed_hash FROM __CANDIDATE__.source_bulk_landing
            WHERE batch_id=:p_batch AND typed_key IS NOT NULL GROUP BY typed_key,typed_hash
            ORDER BY min(source_ordinal)
        ON CONFLICT(dataset_id,key_contract_sha256,logical_key_sha256) DO NOTHING RETURNING 1)
SELECT count(*) AS dictionary_written_n FROM written;

-- query: global_key_count
SELECT CASE WHEN :dictionary_written_n>:dictionary_n THEN 'source_set_dictionary_count_mismatch' END problem;

-- query: global_key_reads
SELECT count(r.root_record_id) AS dictionary_read_n,
    count(*) FILTER (WHERE r.root_record_id IS NULL OR convert_to(r.canonical_logical_key,'UTF8')
        IS DISTINCT FROM convert_to(l.typed_key,'UTF8')) AS dictionary_collision_n,
    coalesce(sum(octet_length(r.canonical_logical_key)::bigint+88),0) AS dictionary_bytes FROM __CANDIDATE__.source_bulk_landing l
        LEFT JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=:b_dataset_id AND r.key_contract_sha256=:key_contract
            AND r.logical_key_sha256=l.typed_hash
        WHERE l.batch_id=:p_batch AND l.typed_key IS NOT NULL;

-- query: global_key_bound
SELECT CASE WHEN :dictionary_read_n<>:dictionary_reference_n OR :dictionary_bytes>:a_source_byte_limit+88::bigint*:n THEN 'source_set_dictionary_bounds' END problem;

-- query: global_key_collision
SELECT CASE WHEN :dictionary_collision_n>0 THEN 'source_bulk_root_collision' END problem;

-- query: insert_candidate_keys
WITH written AS (INSERT INTO __CANDIDATE__.custom_import_root_record(root_record_id,dataset_id,key_contract_sha256,
        canonical_logical_key,logical_key_sha256,created_at)
        SELECT DISTINCT r.root_record_id,r.dataset_id,r.key_contract_sha256,
            r.canonical_logical_key,r.logical_key_sha256,r.created_at
        FROM __CANDIDATE__.source_bulk_landing l JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=:b_dataset_id AND r.key_contract_sha256=:key_contract
            AND r.logical_key_sha256=l.typed_hash
        WHERE l.batch_id=:p_batch ORDER BY r.root_record_id
        ON CONFLICT(root_record_id) DO NOTHING RETURNING 1)
SELECT count(*) AS dictionary_written_n FROM written;

-- query: candidate_key_count
SELECT CASE WHEN :dictionary_written_n>:dictionary_n
        OR (SELECT count(DISTINCT r.root_record_id) FROM __CANDIDATE__.source_bulk_landing l
            JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=:b_dataset_id AND r.key_contract_sha256=:key_contract
            AND r.logical_key_sha256=l.typed_hash
            WHERE l.batch_id=:p_batch)<>:dictionary_n THEN 'source_set_dictionary_count_mismatch' END problem;

-- query: candidate_key_reads
SELECT count(*) AS dictionary_read_n,
    coalesce(sum(octet_length(r.canonical_logical_key)::bigint+88),0) AS dictionary_bytes FROM __CANDIDATE__.source_bulk_landing l
        JOIN __CONTROL__.custom_import_root_record global_key ON global_key.dataset_id=:b_dataset_id
            AND global_key.key_contract_sha256=:key_contract AND global_key.logical_key_sha256=l.typed_hash
        JOIN __CANDIDATE__.custom_import_root_record r ON r.root_record_id=global_key.root_record_id
        WHERE l.batch_id=:p_batch AND l.typed_key IS NOT NULL;

-- query: candidate_key_bound
SELECT CASE WHEN :dictionary_read_n<>:dictionary_reference_n OR :dictionary_bytes>:a_source_byte_limit+88::bigint*:n THEN 'source_set_dictionary_bounds' END problem;

-- query: candidate_key_identity
SELECT CASE WHEN EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        JOIN __CONTROL__.custom_import_root_record global_key ON global_key.dataset_id=:b_dataset_id
            AND global_key.key_contract_sha256=:key_contract AND global_key.logical_key_sha256=l.typed_hash
        LEFT JOIN __CANDIDATE__.custom_import_root_record local_key ON local_key.root_record_id=global_key.root_record_id
        WHERE l.batch_id=:p_batch AND (local_key.root_record_id IS NULL
            OR ROW(local_key.dataset_id,local_key.key_contract_sha256,local_key.logical_key_sha256,local_key.created_at)
                IS DISTINCT FROM ROW(global_key.dataset_id,global_key.key_contract_sha256,global_key.logical_key_sha256,global_key.created_at)
            OR convert_to(local_key.canonical_logical_key,'UTF8') IS DISTINCT FROM convert_to(global_key.canonical_logical_key,'UTF8'))) THEN 'source_set_dictionary_copy_mismatch' END problem;

-- query: assign
WITH inserted AS (INSERT INTO __CANDIDATE__.custom_import_pack(execution_id,dataset_id,definition_revision_id,schema_revision_id,
        stream_slot,pack_ordinal,capture_bundle_id,record_count,pack_sha256,producing_fence,producing_token_sha256)
        SELECT :b_execution_id,:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,:a_stream_slot,l.pack_ordinal,
            :b_capture_bundle_id,count(l.payload),(array_agg(l.pack_sha256 ORDER BY l.landing_ordinal))[1],:b_producing_fence,:b_producing_token_sha256
            FROM __CANDIDATE__.source_bulk_landing l WHERE batch_id=:p_batch GROUP BY l.pack_ordinal ORDER BY l.pack_ordinal
        RETURNING pack_id,pack_ordinal),
    assigned AS MATERIALIZED (SELECT l.landing_ordinal,r.root_record_id,p.pack_id,nextval(CASE
        WHEN l.rejection_code IS NOT NULL THEN CAST(:custom_import_rejection_rejection_id_seq_relation AS regclass)
        WHEN :kind='root' THEN CAST(:custom_import_root_revision_root_revision_id_seq_relation AS regclass)
        ELSE CAST(:custom_import_child_revision_child_revision_id_seq_relation AS regclass) END) AS id
        FROM __CANDIDATE__.source_bulk_landing l
        JOIN inserted p ON p.pack_ordinal=l.pack_ordinal
        LEFT JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=:b_dataset_id
            AND r.key_contract_sha256=:key_contract AND r.logical_key_sha256=l.typed_hash
        WHERE l.batch_id=:p_batch ORDER BY l.landing_ordinal)
    UPDATE __CANDIDATE__.source_bulk_landing l
        SET root_id=assigned.root_record_id,pack_id=assigned.pack_id,outcome_id=assigned.id
        FROM assigned WHERE l.batch_id=:p_batch AND l.landing_ordinal=assigned.landing_ordinal;

-- query: insert_roots
WITH written AS (INSERT INTO __CANDIDATE__.custom_import_root_revision(root_revision_id,dataset_id,definition_revision_id,schema_revision_id,root_record_id,
        pack_id,source_ordinal,canonical_payload,payload_sha256)
        SELECT outcome_id,:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,root_id,pack_id,source_ordinal,payload,payload_hash
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND rejection_code IS NULL AND :kind='root' ORDER BY landing_ordinal RETURNING 1)
SELECT count(*) AS root_written_n FROM written;

-- query: insert_children
WITH written AS (INSERT INTO __CANDIDATE__.custom_import_child_revision(child_revision_id,dataset_id,definition_revision_id,schema_revision_id,root_record_id,
        collection_slot,pack_id,source_ordinal,canonical_parent_key,parent_key_sha256,canonical_child_key,child_key_sha256,canonical_payload,payload_sha256)
        SELECT outcome_id,:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,root_id,:stream_collection_slot,pack_id,
            source_ordinal,typed_key,typed_hash,child_key,child_hash,payload,payload_hash
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND rejection_code IS NULL AND :kind='child' ORDER BY landing_ordinal RETURNING 1)
SELECT count(*) AS child_written_n FROM written;

-- query: insert_rejections
WITH written AS (INSERT INTO __CANDIDATE__.custom_import_rejection(rejection_id,execution_id,rejection_ordinal,dataset_id,
        definition_revision_id,schema_revision_id,pack_id,root_key_sha256,canonical_root_key,collection_slot,
        source_ordinal,code,canonical_evidence,producing_fence,producing_token_sha256)
        SELECT outcome_id,:b_execution_id,:b_next_rejection_ordinal+row_number() OVER (ORDER BY landing_ordinal)-1,
            :b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,pack_id,rejection_hash,rejection_key,
            :stream_collection_slot,source_ordinal,rejection_code,rejection_evidence,:b_producing_fence,:b_producing_token_sha256
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND rejection_code IS NOT NULL ORDER BY landing_ordinal RETURNING 1)
SELECT count(*) AS written_n FROM written;

-- query: outcome_count
SELECT CASE WHEN :written_n<>:rejected_n OR :outcomes_n+:written_n<>:n THEN 'source_set_outcome_count_mismatch' END problem;

-- query: insert_occurrences
WITH written AS (INSERT INTO __CANDIDATE__.custom_import_build_occurrence(build_id,stream_slot,pack_id,origin,source_part_ordinal,part_row_ordinal,
        source_ordinal,record_kind,collection_slot,raw_parent_key_canonical,raw_parent_key_sha256,root_record_id,child_key_sha256,
        root_revision_id,child_revision_id,rejection_id)
        SELECT :b_build_id,:a_stream_slot,pack_id,'source',part_ordinal,part_row_ordinal,source_ordinal,:kind,coalesce(:stream_collection_slot,0),
            raw_key,raw_hash,root_id,child_hash,CASE WHEN rejection_code IS NULL AND :kind='root' THEN outcome_id END,
            CASE WHEN rejection_code IS NULL AND :kind='child' THEN outcome_id END,CASE WHEN rejection_code IS NOT NULL THEN outcome_id END
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch ORDER BY landing_ordinal RETURNING 1)
SELECT count(*) AS written_n FROM written;

-- query: occurrence_count
SELECT CASE WHEN :written_n<>:n THEN 'source_set_occurrence_count_mismatch' END problem;

-- query: reanalyze_landing
ANALYZE __CANDIDATE__.source_bulk_landing(batch_id,landing_ordinal,pack_ordinal,pack_id,root_id,
        outcome_id,part_ordinal,part_row_ordinal,source_ordinal,typed_hash,rejection_code);

-- query: needs_occurrence_analyze
SELECT :a_first_source=0 OR :b_source_occurrence_count+:n>2*(SELECT reltuples FROM pg_class
        WHERE oid=CAST(:custom_import_build_occurrence_relation AS regclass)) AS needed;

-- query: analyze_occurrences
ANALYZE __CANDIDATE__.custom_import_build_occurrence(build_id,origin,stream_slot,source_ordinal,
            occurrence_id,source_part_ordinal,part_row_ordinal,record_kind,root_record_id,collection_slot,
            raw_parent_key_sha256,child_key_sha256,child_revision_id,root_revision_id,pack_id,rejection_id,resolved_rejection_id);

-- query: stored_identity
SELECT CASE WHEN EXISTS(WITH expected AS (SELECT pack_ordinal,count(payload) accepted_count
            FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch GROUP BY pack_ordinal),
        rejected AS (SELECT landing_ordinal,:b_next_rejection_ordinal+row_number() OVER (ORDER BY landing_ordinal)-1 expected_ordinal
            FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND rejection_code IS NOT NULL)
        SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        JOIN expected ep ON ep.pack_ordinal=l.pack_ordinal
        LEFT JOIN rejected ej ON ej.landing_ordinal=l.landing_ordinal
        LEFT JOIN __CANDIDATE__.custom_import_root_record k ON k.root_record_id=l.root_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=l.pack_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON :kind='root' AND l.rejection_code IS NULL AND r.root_revision_id=l.outcome_id
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON :kind='child' AND l.rejection_code IS NULL AND c.child_revision_id=l.outcome_id
        LEFT JOIN __CANDIDATE__.custom_import_rejection j ON l.rejection_code IS NOT NULL AND j.rejection_id=l.outcome_id
        WHERE l.batch_id=:p_batch AND (
            p.pack_id IS NULL OR p.record_count<>ep.accepted_count OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.stream_slot,p.pack_ordinal,p.pack_sha256,p.producing_fence,p.producing_token_sha256)
                IS DISTINCT FROM ROW(:b_execution_id,:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,
                    :b_capture_bundle_id,:a_stream_slot,l.pack_ordinal,l.pack_sha256,:b_producing_fence,:b_producing_token_sha256)
            OR (l.typed_key IS NOT NULL AND (k.root_record_id IS NULL OR k.root_record_id<=0 OR ROW(k.dataset_id,k.key_contract_sha256,k.logical_key_sha256)
                IS DISTINCT FROM ROW(:b_dataset_id,:key_contract,l.typed_hash)
                OR convert_to(k.canonical_logical_key,'UTF8') IS DISTINCT FROM convert_to(l.typed_key,'UTF8')))
            OR (l.typed_key IS NULL AND l.root_id IS NOT NULL)
            OR (l.rejection_code IS NULL AND :kind='root' AND (r.root_revision_id IS NULL OR
                ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,r.source_ordinal,r.payload_sha256)
                IS DISTINCT FROM ROW(:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,l.root_id,l.pack_id,l.source_ordinal,l.payload_hash)
                OR convert_to(r.canonical_payload,'UTF8') IS DISTINCT FROM convert_to(l.payload,'UTF8')))
            OR (l.rejection_code IS NULL AND :kind='child' AND (c.child_revision_id IS NULL OR
                ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,
                    c.pack_id,c.source_ordinal,c.parent_key_sha256,c.child_key_sha256,c.payload_sha256)
                IS DISTINCT FROM ROW(:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,l.root_id,:stream_collection_slot,
                    l.pack_id,l.source_ordinal,l.typed_hash,l.child_hash,l.payload_hash)
                OR convert_to(c.canonical_parent_key,'UTF8') IS DISTINCT FROM convert_to(l.typed_key,'UTF8')
                OR convert_to(c.canonical_child_key,'UTF8') IS DISTINCT FROM convert_to(l.child_key,'UTF8')
                OR convert_to(c.canonical_payload,'UTF8') IS DISTINCT FROM convert_to(l.payload,'UTF8')))
            OR (l.rejection_code IS NOT NULL AND (j.rejection_id IS NULL OR j.rejection_ordinal<>ej.expected_ordinal OR
                ROW(j.execution_id,j.dataset_id,j.definition_revision_id,j.schema_revision_id,j.pack_id,j.source_ordinal,
                    j.root_key_sha256,j.collection_slot,j.code,j.field_slot,j.producing_fence,j.producing_token_sha256)
                IS DISTINCT FROM ROW(:b_execution_id,:b_dataset_id,:b_definition_revision_id,:b_schema_revision_id,l.pack_id,
                    l.source_ordinal,l.rejection_hash,:stream_collection_slot,l.rejection_code,NULL::smallint,:b_producing_fence,:b_producing_token_sha256)
                OR convert_to(j.canonical_root_key,'UTF8') IS DISTINCT FROM convert_to(l.rejection_key,'UTF8')
                OR convert_to(j.canonical_evidence,'UTF8') IS DISTINCT FROM convert_to(l.rejection_evidence,'UTF8')))
            OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
                WHERE o.build_id=:b_build_id AND o.origin='source'
                    AND o.stream_slot=:a_stream_slot AND o.source_ordinal=l.source_ordinal
                AND ROW(o.pack_id,o.origin,o.source_part_ordinal,o.part_row_ordinal,o.record_kind,
                o.collection_slot,o.raw_parent_key_sha256,o.root_record_id,o.child_key_sha256,
                o.root_revision_id,o.child_revision_id,o.rejection_id,o.resolved_rejection_id,
                o.base_family_revision_id,o.base_root_revision_id,o.base_child_revision_id)
                IS NOT DISTINCT FROM ROW(l.pack_id,'source',l.part_ordinal,l.part_row_ordinal,:kind,coalesce(:stream_collection_slot,0),
                    l.raw_hash,l.root_id,l.child_hash,
                    CASE WHEN l.rejection_code IS NULL AND :kind='root' THEN l.outcome_id END,
                    CASE WHEN l.rejection_code IS NULL AND :kind='child' THEN l.outcome_id END,
                    CASE WHEN l.rejection_code IS NOT NULL THEN l.outcome_id END,NULL::bigint,NULL::bigint,NULL::bigint,NULL::bigint)
                AND convert_to(o.raw_parent_key_canonical,'UTF8') IS NOT DISTINCT FROM convert_to(l.raw_key,'UTF8'))
        )) THEN 'source_set_stored_identity_mismatch' END problem;

-- query: advance_stream
UPDATE __CONTROL__.custom_import_build_stream SET next_part_ordinal=:last_part,next_part_row_ordinal=:last_row,
        next_source_ordinal=:a_first_source+:n,next_pack_ordinal=:a_first_pack+:pack_n
        WHERE build_id=:b_build_id AND stream_slot=:a_stream_slot;

-- query: advance_build
UPDATE __CONTROL__.custom_import_build_attempt SET source_occurrence_count=source_occurrence_count+:n,
        next_rejection_ordinal=next_rejection_ordinal+:rejected_n WHERE build_id=:b_build_id;

-- query: counter_cursor
SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream current_stream
            WHERE current_stream.build_id=:b_build_id AND current_stream.stream_slot=:a_stream_slot
                AND ROW(current_stream.next_part_ordinal,current_stream.next_part_row_ordinal,
                    current_stream.next_source_ordinal,current_stream.next_pack_ordinal,current_stream.replay_verified_at)
                IS NOT DISTINCT FROM ROW(:last_part,:last_row,:a_first_source+:n,:a_first_pack+:pack_n,:s_replay_verified_at))
        OR NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_attempt current_build
            WHERE current_build.build_id=:b_build_id AND current_build.phase='source'
                AND current_build.source_occurrence_count=:b_source_occurrence_count+:n
                AND current_build.next_rejection_ordinal=:b_next_rejection_ordinal+:rejected_n) THEN 'source_set_counter_cursor_mismatch' END problem;

-- query: completion
INSERT INTO __CONTROL__.source_bulk_completion(batch_id,transaction_id,attempted_count,
        build_id,stream_slot,first_source,after_source,input_sha256)
        SELECT :p_batch,CAST(:a_transaction_id AS xid8),:n,:b_build_id,:a_stream_slot,:a_first_source,:a_first_source+:n,
            sha256(convert_to(string_agg(array_to_json(ARRAY[
                to_json(pack_ordinal),to_json(encode(pack_sha256,'hex')),to_json(part_ordinal),
                to_json(part_row_ordinal),to_json(source_ordinal),to_json(raw_key),to_json(encode(raw_hash,'hex')),
                to_json(typed_key),to_json(encode(typed_hash,'hex')),to_json(payload),to_json(encode(payload_hash,'hex')),
                to_json(child_key),to_json(encode(child_hash,'hex')),to_json(rejection_code),to_json(rejection_key),
                to_json(encode(rejection_hash,'hex')),to_json(rejection_evidence)])::text,E'\n' ORDER BY landing_ordinal),'UTF8'))
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch;

-- query: homes
SELECT __CONTROL__.append_custom_import_revision_home(:family_id,
    coalesce(array_agg(outcome_id) FILTER(WHERE :kind='root'),'{}'::bigint[]),
    coalesce(array_agg(outcome_id) FILTER(WHERE :kind='child'),'{}'::bigint[])) AS appended
FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch AND rejection_code IS NULL;

-- query: delete_landing
DELETE FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=:p_batch;

-- query: landing_empty
SELECT NOT EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing) AS empty;

-- query: truncate_landing
TRUNCATE __CANDIDATE__.source_bulk_landing;

-- query: close_authorization
UPDATE __CONTROL__.source_bulk_authorization SET accepting=false WHERE batch_id=:p_batch;

-- query: fresh_lease
SELECT CASE WHEN e.state IS DISTINCT FROM 'running' OR l.fence IS DISTINCT FROM :b_producing_fence
    OR l.token_sha256 IS DISTINCT FROM :b_producing_token_sha256 OR l.expires_at IS NULL
    OR least(l.expires_at,:b_build_deadline_at)<=clock_timestamp() THEN 'source_set_lease_lost' END problem
FROM __CONTROL__.custom_import_execution e LEFT JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id
WHERE e.execution_id=:b_execution_id;

-- query: copy_closure
SELECT session_user::text AS role_name,NOT EXISTS(
    SELECT 1 FROM __CONTROL__.source_bulk_authorization a
    WHERE a.build_id=:b_build_id AND a.opened_by=session_user
        AND a.transaction_id=pg_current_xact_id() AND a.accepting
) AS close_copy;
