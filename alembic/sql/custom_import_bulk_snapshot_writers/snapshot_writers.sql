-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE TABLE __CANDIDATE__.source_bulk_landing (
    batch_id uuid NOT NULL,
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    accepting boolean NOT NULL DEFAULT true CHECK (accepting),
    landing_ordinal integer NOT NULL CHECK (landing_ordinal BETWEEN 0 AND 99999),
    pack_ordinal integer NOT NULL CHECK (pack_ordinal>=0),
    pack_sha256 bytea NOT NULL CHECK (octet_length(pack_sha256)=32),
    part_ordinal integer NOT NULL CHECK (part_ordinal>0),
    part_row_ordinal bigint NOT NULL CHECK (part_row_ordinal>=0),
    source_ordinal bigint NOT NULL CHECK (source_ordinal>=0),
    raw_key text, raw_hash bytea, typed_key text, typed_hash bytea,
    payload text, payload_hash bytea, child_key text, child_hash bytea,
    rejection_code text, rejection_key text, rejection_hash bytea, rejection_evidence text,
    pack_id bigint, root_id bigint, outcome_id bigint,
    PRIMARY KEY(batch_id,landing_ordinal),
    UNIQUE(batch_id,source_ordinal),
    UNIQUE(batch_id,part_ordinal,part_row_ordinal)
);

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.source_bulk_authorize(p_build bigint,p_stream smallint,p_fence bigint,
    p_token bytea,p_count integer,p_bytes bigint) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; s __CONTROL__.custom_import_build_stream; result uuid;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build);
    IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
        WHERE r.relation_slot=1 AND r.table_oid='__CANDIDATE__.custom_import_root_record'::regclass::oid
            AND f.landing_table_oid='__CANDIDATE__.source_bulk_landing'::regclass::oid
            AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
            AND f.frozen_at IS NULL
            AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                    b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
    IF b.phase<>'source' OR b.producing_fence IS DISTINCT FROM p_fence
        OR b.producing_token_sha256 IS DISTINCT FROM p_token THEN
        RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    SELECT * INTO s FROM __CONTROL__.custom_import_build_stream
        WHERE build_id=p_build AND stream_slot=p_stream FOR UPDATE;
    IF s.build_id IS NULL OR s.replay_verified_at IS NOT NULL THEN RAISE EXCEPTION 'source_bulk_stream_mismatch'; END IF;
    INSERT INTO __CONTROL__.source_bulk_authorization(opened_by,build_id,dataset_id,definition_revision_id,
        schema_revision_id,execution_id,capture_bundle_id,stream_slot,fence,token_hash,
        expected_count,source_byte_limit,first_pack,first_part,first_row,first_source)
        VALUES(session_user,p_build,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
            b.execution_id,b.capture_bundle_id,p_stream,p_fence,p_token,p_count,p_bytes,
            s.next_pack_ordinal,s.next_part_ordinal,s.next_part_row_ordinal,s.next_source_ordinal)
        RETURNING batch_id INTO result;
    RETURN result;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.source_set_finalize(p_batch uuid,p_verified_parts integer[]) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE a __CONTROL__.source_bulk_authorization; b __CONTROL__.custom_import_build_attempt;
    s __CONTROL__.custom_import_build_stream; stream __CONTROL__.custom_import_source_stream;
    definition jsonb; collection jsonb; key_contract bytea;
    label text; kind text; part_count integer; n integer; bytes bigint;
    rejected_n bigint; pack_n integer; last_part integer; last_row bigint; written_n integer; outcomes_n integer:=0;
    dictionary_n bigint; dictionary_bytes bigint; dictionary_written_n bigint;
    dictionary_read_n bigint; dictionary_reference_n bigint;
    fresh_root_ids bigint[]; fresh_child_ids bigint[];
    lease_end timestamptz; producer_state text; producer_fence bigint; producer_token bytea; now_at timestamptz;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t
        WHERE t.tgrelid=ANY(ARRAY[
            '__CANDIDATE__.custom_import_root_record'::regclass,'__CANDIDATE__.custom_import_pack'::regclass,
            '__CANDIDATE__.custom_import_root_revision'::regclass,
            '__CANDIDATE__.custom_import_child_revision'::regclass,'__CANDIDATE__.custom_import_rejection'::regclass,
            '__CANDIDATE__.custom_import_build_occurrence'::regclass])
        AND NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4) THEN
        RAISE EXCEPTION 'source_set_row_guards_require_all_writer_retirement'; END IF;
    IF EXISTS(SELECT 1 FROM pg_constraint c WHERE c.contype='f' AND c.conrelid=ANY(ARRAY[
        '__CANDIDATE__.source_bulk_landing'::regclass,'__CANDIDATE__.custom_import_root_record'::regclass,
        '__CANDIDATE__.custom_import_pack'::regclass,'__CANDIDATE__.custom_import_root_revision'::regclass,
        '__CANDIDATE__.custom_import_child_revision'::regclass,'__CANDIDATE__.custom_import_rejection'::regclass,
        '__CANDIDATE__.custom_import_build_occurrence'::regclass])) THEN
        RAISE EXCEPTION 'source_set_hot_relationship_guards_present'; END IF;
    SELECT * INTO a FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch;
    IF a.batch_id IS NULL OR NOT a.accepting OR a.opened_by<>session_user
        OR a.transaction_id<>pg_current_xact_id() THEN RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(a.build_id);
    IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
        WHERE r.relation_slot=1 AND r.table_oid='__CANDIDATE__.custom_import_root_record'::regclass::oid
            AND f.landing_table_oid='__CANDIDATE__.source_bulk_landing'::regclass::oid
            AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
            AND f.frozen_at IS NULL
            AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                    b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
    SELECT * INTO a FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch FOR UPDATE;
    IF a.batch_id IS NULL OR NOT a.accepting OR a.opened_by<>session_user
        OR a.transaction_id<>pg_current_xact_id() OR a.build_id<>b.build_id THEN
        RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    IF b.phase<>'source' OR ROW(b.producing_fence,b.producing_token_sha256)
        IS DISTINCT FROM ROW(a.fence,a.token_hash) THEN RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    IF ROW(a.dataset_id,a.definition_revision_id,a.schema_revision_id,a.execution_id,a.capture_bundle_id)
        IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,b.capture_bundle_id) THEN
        RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    LOCK TABLE __CANDIDATE__.source_bulk_landing IN SHARE ROW EXCLUSIVE MODE;
    IF (SELECT count(*) FROM (SELECT 1 FROM __CANDIDATE__.source_bulk_landing LIMIT 100001) bounded_landing)>100000 THEN
        RAISE EXCEPTION 'source_bulk_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        LEFT JOIN __CONTROL__.source_bulk_authorization authorized
            ON authorized.batch_id=l.batch_id AND authorized.transaction_id=l.transaction_id
            AND authorized.accepting=l.accepting AND authorized.accepting
            AND authorized.opened_by=session_user AND authorized.transaction_id=pg_current_xact_id()
            AND ROW(authorized.build_id,authorized.dataset_id,authorized.definition_revision_id,
                authorized.schema_revision_id,authorized.execution_id,authorized.capture_bundle_id,
                authorized.fence,authorized.token_hash)
                IS NOT DISTINCT FROM ROW(b.build_id,b.dataset_id,b.definition_revision_id,
                    b.schema_revision_id,b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
        LEFT JOIN __CONTROL__.custom_import_build_stream authorized_stream
            ON authorized_stream.build_id=authorized.build_id AND authorized_stream.stream_slot=authorized.stream_slot
            AND authorized_stream.replay_verified_at IS NULL
        LEFT JOIN __CONTROL__.custom_import_source_stream declared_stream
            ON declared_stream.definition_revision_id=b.definition_revision_id
            AND declared_stream.stream_slot=authorized.stream_slot
            AND declared_stream.dataset_id=b.dataset_id AND declared_stream.schema_revision_id=b.schema_revision_id
        WHERE authorized.batch_id IS NULL OR authorized_stream.build_id IS NULL OR declared_stream.stream_slot IS NULL) THEN
        RAISE EXCEPTION 'source_bulk_landing_owner_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=b.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'source_bulk_sealed_attempt'; END IF;
    SELECT * INTO s FROM __CONTROL__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=a.stream_slot FOR UPDATE;
    IF s.build_id IS NULL OR s.replay_verified_at IS NOT NULL OR ROW(s.next_pack_ordinal,s.next_part_ordinal,s.next_part_row_ordinal,s.next_source_ordinal)
        IS DISTINCT FROM ROW(a.first_pack,a.first_part,a.first_row,a.first_source) THEN RAISE EXCEPTION 'source_bulk_cursor_changed'; END IF;
    SELECT * INTO stream FROM __CONTROL__.custom_import_source_stream WHERE definition_revision_id=b.definition_revision_id AND stream_slot=a.stream_slot;
    IF stream.stream_slot IS NULL OR ROW(stream.dataset_id,stream.schema_revision_id)
        IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id) OR stream.record_kind NOT IN ('root','child') THEN RAISE EXCEPTION 'source_bulk_stream_mismatch'; END IF;
    SELECT canonical_definition::jsonb INTO definition FROM __CONTROL__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
    kind:=stream.record_kind;
    SELECT x INTO collection FROM jsonb_array_elements(definition#>'{schema,children}') x
        JOIN __CONTROL__.custom_import_child_collection c ON c.collection_name=x->>'name'
        WHERE c.schema_revision_id=b.schema_revision_id AND c.collection_slot=stream.collection_slot;
    label:=CASE WHEN kind='root' THEN 'root' ELSE collection->>'name' END;
    key_contract:=__CONTROL__.source_bulk_digest('root-key-contract',__CONTROL__.source_bulk_canonical(jsonb_build_object(
        'contract','custom-import-root-key/v1','fields',definition#>'{schema,root,logical_key}')));
    SELECT payload_part_count INTO part_count FROM __CONTROL__.custom_import_capture
        WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=a.stream_slot AND capture_state='sealed';
    SELECT count(*),sum(coalesce(octet_length(raw_key),0)+coalesce(octet_length(typed_key),0)
        +coalesce(octet_length(payload),0)+coalesce(octet_length(child_key),0)
        +CASE WHEN kind='child' AND payload IS NOT NULL THEN coalesce(octet_length(typed_key),0) ELSE 0 END
        +coalesce(octet_length(rejection_key),0)+coalesce(octet_length(rejection_evidence),0)) INTO n,bytes
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch;
    IF part_count IS NULL OR n NOT BETWEEN 1 AND 100000 OR n<>a.expected_count
        OR bytes IS NULL OR bytes>a.source_byte_limit OR bytes>268435456 THEN RAISE EXCEPTION 'source_bulk_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l WHERE l.batch_id=p_batch AND (
        l.transaction_id<>a.transaction_id OR l.landing_ordinal<>l.source_ordinal-a.first_source
        OR l.pack_id IS NOT NULL OR l.root_id IS NOT NULL OR l.outcome_id IS NOT NULL
        OR num_nonnulls(l.raw_key,l.raw_hash) NOT IN (0,2) OR num_nonnulls(l.typed_key,l.typed_hash) NOT IN (0,2)
        OR (l.raw_key IS NOT NULL AND l.raw_hash IS DISTINCT FROM sha256(convert_to('custom-import/raw-family-key/v1:'||l.raw_key,'UTF8')))
        OR (l.typed_key IS NOT NULL AND l.typed_hash IS DISTINCT FROM __CONTROL__.source_bulk_digest('root-key',l.typed_key))
        OR (l.rejection_code IS NULL AND (l.payload IS NULL OR l.typed_key IS NULL OR l.raw_key IS NULL
            OR l.payload_hash IS DISTINCT FROM __CONTROL__.source_bulk_digest(kind||'-payload',l.payload)
            OR num_nonnulls(l.rejection_key,l.rejection_hash,l.rejection_evidence)<>0
            OR (kind='root' AND num_nonnulls(l.child_key,l.child_hash)<>0)
            OR (kind='child' AND (l.child_key IS NULL OR l.child_hash IS DISTINCT FROM __CONTROL__.source_bulk_digest('child-key',l.child_key)))))
        OR (l.raw_hash IS NOT NULL AND octet_length(l.raw_hash)<>32)
        OR (l.typed_hash IS NOT NULL AND octet_length(l.typed_hash)<>32)
        OR (l.rejection_code IS NOT NULL AND (num_nonnulls(l.payload,l.payload_hash,l.child_key,l.child_hash)<>0
            OR l.rejection_code !~ '^[a-z][a-z0-9_]{0,62}$' OR l.rejection_evidence IS NULL
            OR l.rejection_key IS DISTINCT FROM l.typed_key OR l.rejection_hash IS DISTINCT FROM l.typed_hash
            OR l.rejection_evidence IS DISTINCT FROM __CONTROL__.source_bulk_canonical(jsonb_build_object(
                'code',l.rejection_code,'contract','custom-import-rejection/v1',
                'root_key_sha256',CASE WHEN l.rejection_hash IS NULL THEN NULL ELSE encode(l.rejection_hash,'hex') END))))
    )) THEN RAISE EXCEPTION 'source_bulk_row_mismatch'; END IF;
    ANALYZE __CANDIDATE__.source_bulk_landing(batch_id,landing_ordinal,pack_ordinal,pack_id,root_id,
        outcome_id,part_ordinal,part_row_ordinal,source_ordinal,typed_hash,rejection_code);
    IF (SELECT ROW(min(landing_ordinal),max(landing_ordinal)) FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch)
        IS DISTINCT FROM ROW(0,n-1) THEN RAISE EXCEPTION 'source_bulk_position_gap'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch AND typed_hash IS NOT NULL
        GROUP BY typed_hash HAVING count(DISTINCT convert_to(typed_key,'UTF8'))<>1) THEN
        RAISE EXCEPTION 'source_bulk_root_collision'; END IF;
    SELECT count(DISTINCT pack_ordinal),max(part_ordinal),
        count(*) FILTER (WHERE rejection_code IS NOT NULL) INTO pack_n,last_part,rejected_n
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch;
    SELECT max(part_row_ordinal)+1 INTO last_row FROM __CANDIDATE__.source_bulk_landing
        WHERE batch_id=p_batch AND part_ordinal=last_part;
    IF last_part>part_count OR p_verified_parts IS NULL
        OR cardinality(p_verified_parts)<>(SELECT count(DISTINCT value) FROM unnest(p_verified_parts) value)
        OR cardinality(p_verified_parts)<>last_part-a.first_part
        OR EXISTS(SELECT 1 FROM unnest(p_verified_parts) value
            WHERE value IS NULL OR value<a.first_part OR value>=last_part)
        OR (SELECT count(*) FROM __CONTROL__.custom_import_capture_parquet_part p
            WHERE p.capture_bundle_id=b.capture_bundle_id AND p.stream_slot=a.stream_slot
                AND p.part_ordinal BETWEEN a.first_part AND last_part-1)<>last_part-a.first_part
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_capture_parquet_part p
            LEFT JOIN (SELECT part_ordinal,max(part_row_ordinal)+1 row_end
                FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch GROUP BY part_ordinal) l
                ON l.part_ordinal=p.part_ordinal
            WHERE p.capture_bundle_id=b.capture_bundle_id AND p.stream_slot=a.stream_slot
                AND p.part_ordinal BETWEEN a.first_part AND last_part-1
                AND (NOT(p.part_ordinal=ANY(p_verified_parts)) OR p.record_count<>coalesce(l.row_end,
                    CASE WHEN p.part_ordinal=a.first_part THEN a.first_row ELSE 0 END))) THEN
        RAISE EXCEPTION 'source_set_earlier_part_not_verified'; END IF;
    IF EXISTS(WITH ordered AS (SELECT l.*,lag(part_ordinal) OVER w previous_part,
            lag(part_row_ordinal) OVER w previous_row,lag(pack_ordinal) OVER w previous_pack
            FROM __CANDIDATE__.source_bulk_landing l WHERE batch_id=p_batch WINDOW w AS (ORDER BY landing_ordinal))
        SELECT 1 FROM ordered o LEFT JOIN __CONTROL__.custom_import_capture_parquet_part p
            ON p.capture_bundle_id=b.capture_bundle_id AND p.stream_slot=a.stream_slot AND p.part_ordinal=o.part_ordinal
        WHERE p.part_ordinal IS NULL OR o.part_ordinal<a.first_part OR o.part_row_ordinal>=p.record_count
            OR (o.landing_ordinal=0 AND (ROW(o.pack_ordinal,o.source_ordinal) IS DISTINCT FROM ROW(a.first_pack,a.first_source)
                OR o.part_row_ordinal<>CASE WHEN o.part_ordinal=a.first_part THEN a.first_row ELSE 0 END))
            OR (o.landing_ordinal>0 AND NOT ((o.part_ordinal=o.previous_part AND o.part_row_ordinal=o.previous_row+1)
                OR (o.part_ordinal>o.previous_part AND o.part_row_ordinal=0)))
            OR (o.landing_ordinal>0 AND o.pack_ordinal NOT IN (o.previous_pack,o.previous_pack+1))) THEN
        RAISE EXCEPTION 'source_bulk_position_gap'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch GROUP BY pack_ordinal HAVING
        count(*)>b.page_row_limit OR count(DISTINCT part_ordinal)<>1 OR count(DISTINCT pack_sha256)<>1
        OR sum(coalesce(octet_length(raw_key),0)+coalesce(octet_length(typed_key),0)+coalesce(octet_length(payload),0)
            +coalesce(octet_length(child_key),0)+CASE WHEN kind='child' AND payload IS NOT NULL THEN coalesce(octet_length(typed_key),0) ELSE 0 END
            +coalesce(octet_length(rejection_key),0)+coalesce(octet_length(rejection_evidence),0))>b.page_byte_limit
        OR (array_agg(pack_sha256 ORDER BY landing_ordinal))[1] IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
            ||convert_to(label,'UTF8')||decode('00','hex')||coalesce(string_agg(payload_hash,''::bytea ORDER BY payload_hash),''::bytea))) THEN
        RAISE EXCEPTION 'source_bulk_pack_mismatch'; END IF;

    SELECT count(*),coalesce(sum(octet_length(typed_key)::bigint+88),0),coalesce(sum(reference_count),0)
        INTO dictionary_n,dictionary_bytes,dictionary_reference_n
        FROM (SELECT typed_key,typed_hash,count(*) reference_count FROM __CANDIDATE__.source_bulk_landing
            WHERE batch_id=p_batch AND typed_key IS NOT NULL GROUP BY typed_key,typed_hash) keys;
    IF dictionary_n>n OR dictionary_bytes>a.source_byte_limit+88::bigint*n THEN
        RAISE EXCEPTION 'source_set_dictionary_bounds'; END IF;
    INSERT INTO __CONTROL__.custom_import_root_record(dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256)
        SELECT b.dataset_id,key_contract,typed_key,typed_hash FROM __CANDIDATE__.source_bulk_landing
            WHERE batch_id=p_batch AND typed_key IS NOT NULL GROUP BY typed_key,typed_hash
            ORDER BY min(source_ordinal)
        ON CONFLICT(dataset_id,key_contract_sha256,logical_key_sha256) DO NOTHING;
    GET DIAGNOSTICS dictionary_written_n=ROW_COUNT;
    IF dictionary_written_n>dictionary_n THEN RAISE EXCEPTION 'source_set_dictionary_count_mismatch'; END IF;
    UPDATE __CANDIDATE__.source_bulk_landing l SET root_id=r.root_record_id FROM __CONTROL__.custom_import_root_record r
        WHERE l.batch_id=p_batch AND r.dataset_id=b.dataset_id AND r.key_contract_sha256=key_contract AND r.logical_key_sha256=l.typed_hash;
    SELECT count(*),coalesce(sum(octet_length(r.canonical_logical_key)::bigint+88),0)
        INTO dictionary_read_n,dictionary_bytes
        FROM __CANDIDATE__.source_bulk_landing l
        JOIN __CONTROL__.custom_import_root_record r ON r.root_record_id=l.root_id
        WHERE l.batch_id=p_batch AND l.typed_key IS NOT NULL;
    IF dictionary_read_n<>dictionary_reference_n OR dictionary_bytes>a.source_byte_limit+88::bigint*n THEN
        RAISE EXCEPTION 'source_set_dictionary_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        LEFT JOIN __CONTROL__.custom_import_root_record r ON r.root_record_id=l.root_id
        WHERE l.batch_id=p_batch AND l.typed_key IS NOT NULL AND (r.root_record_id IS NULL
            OR ROW(r.dataset_id,r.key_contract_sha256,r.logical_key_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,key_contract,l.typed_hash)
            OR convert_to(r.canonical_logical_key,'UTF8') IS DISTINCT FROM convert_to(l.typed_key,'UTF8'))) THEN
        RAISE EXCEPTION 'source_bulk_root_collision'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_root_record(root_record_id,dataset_id,key_contract_sha256,
        canonical_logical_key,logical_key_sha256,created_at)
        SELECT DISTINCT r.root_record_id,r.dataset_id,r.key_contract_sha256,
            r.canonical_logical_key,r.logical_key_sha256,r.created_at
        FROM __CANDIDATE__.source_bulk_landing l JOIN __CONTROL__.custom_import_root_record r ON r.root_record_id=l.root_id
        WHERE l.batch_id=p_batch ORDER BY r.root_record_id
        ON CONFLICT(root_record_id) DO NOTHING;
    GET DIAGNOSTICS dictionary_written_n=ROW_COUNT;
    IF dictionary_written_n>dictionary_n
        OR (SELECT count(DISTINCT root_id) FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch)<>dictionary_n THEN
        RAISE EXCEPTION 'source_set_dictionary_count_mismatch'; END IF;
    SELECT count(*),coalesce(sum(octet_length(r.canonical_logical_key)::bigint+88),0)
        INTO dictionary_read_n,dictionary_bytes
        FROM __CANDIDATE__.source_bulk_landing l
        JOIN __CANDIDATE__.custom_import_root_record r ON r.root_record_id=l.root_id
        WHERE l.batch_id=p_batch AND l.typed_key IS NOT NULL;
    IF dictionary_read_n<>dictionary_reference_n OR dictionary_bytes>a.source_byte_limit+88::bigint*n THEN
        RAISE EXCEPTION 'source_set_dictionary_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        JOIN __CONTROL__.custom_import_root_record global_key ON global_key.root_record_id=l.root_id
        LEFT JOIN __CANDIDATE__.custom_import_root_record local_key ON local_key.root_record_id=global_key.root_record_id
        WHERE l.batch_id=p_batch AND (local_key.root_record_id IS NULL
            OR ROW(local_key.dataset_id,local_key.key_contract_sha256,local_key.logical_key_sha256,local_key.created_at)
                IS DISTINCT FROM ROW(global_key.dataset_id,global_key.key_contract_sha256,global_key.logical_key_sha256,global_key.created_at)
            OR convert_to(local_key.canonical_logical_key,'UTF8') IS DISTINCT FROM convert_to(global_key.canonical_logical_key,'UTF8'))) THEN
        RAISE EXCEPTION 'source_set_dictionary_copy_mismatch'; END IF;
    WITH inserted AS (INSERT INTO __CANDIDATE__.custom_import_pack(execution_id,dataset_id,definition_revision_id,schema_revision_id,
        stream_slot,pack_ordinal,capture_bundle_id,record_count,pack_sha256,producing_fence,producing_token_sha256)
        SELECT b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,a.stream_slot,l.pack_ordinal,
            b.capture_bundle_id,count(l.payload),(array_agg(l.pack_sha256 ORDER BY l.landing_ordinal))[1],b.producing_fence,b.producing_token_sha256
            FROM __CANDIDATE__.source_bulk_landing l WHERE batch_id=p_batch GROUP BY l.pack_ordinal ORDER BY l.pack_ordinal
        RETURNING pack_id,pack_ordinal)
        UPDATE __CANDIDATE__.source_bulk_landing l SET pack_id=p.pack_id FROM inserted p
            WHERE l.batch_id=p_batch AND l.pack_ordinal=p.pack_ordinal;
    WITH assigned AS MATERIALIZED (SELECT landing_ordinal,nextval(CASE
        WHEN rejection_code IS NOT NULL THEN '__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass
        WHEN kind='root' THEN '__CONTROL__.custom_import_root_revision_root_revision_id_seq'::regclass
        ELSE '__CONTROL__.custom_import_child_revision_child_revision_id_seq'::regclass END) AS id
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch ORDER BY landing_ordinal)
    UPDATE __CANDIDATE__.source_bulk_landing l SET outcome_id=assigned.id FROM assigned
        WHERE l.batch_id=p_batch AND l.landing_ordinal=assigned.landing_ordinal;
    INSERT INTO __CANDIDATE__.custom_import_root_revision(root_revision_id,dataset_id,definition_revision_id,schema_revision_id,root_record_id,
        pack_id,source_ordinal,canonical_payload,payload_sha256)
        SELECT outcome_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,root_id,pack_id,source_ordinal,payload,payload_hash
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch AND rejection_code IS NULL AND kind='root' ORDER BY landing_ordinal;
    GET DIAGNOSTICS written_n=ROW_COUNT;
    outcomes_n:=outcomes_n+written_n;
    INSERT INTO __CANDIDATE__.custom_import_child_revision(child_revision_id,dataset_id,definition_revision_id,schema_revision_id,root_record_id,
        collection_slot,pack_id,source_ordinal,canonical_parent_key,parent_key_sha256,canonical_child_key,child_key_sha256,canonical_payload,payload_sha256)
        SELECT outcome_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,root_id,stream.collection_slot,pack_id,
            source_ordinal,typed_key,typed_hash,child_key,child_hash,payload,payload_hash
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch AND rejection_code IS NULL AND kind='child' ORDER BY landing_ordinal;
    GET DIAGNOSTICS written_n=ROW_COUNT;
    outcomes_n:=outcomes_n+written_n;
    INSERT INTO __CANDIDATE__.custom_import_rejection(rejection_id,execution_id,rejection_ordinal,dataset_id,
        definition_revision_id,schema_revision_id,pack_id,root_key_sha256,canonical_root_key,collection_slot,
        source_ordinal,code,canonical_evidence,producing_fence,producing_token_sha256)
        SELECT outcome_id,b.execution_id,b.next_rejection_ordinal+row_number() OVER (ORDER BY landing_ordinal)-1,
            b.dataset_id,b.definition_revision_id,b.schema_revision_id,pack_id,rejection_hash,rejection_key,
            stream.collection_slot,source_ordinal,rejection_code,rejection_evidence,b.producing_fence,b.producing_token_sha256
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch AND rejection_code IS NOT NULL ORDER BY landing_ordinal;
    GET DIAGNOSTICS written_n=ROW_COUNT;
    IF written_n<>rejected_n OR outcomes_n+written_n<>n THEN RAISE EXCEPTION 'source_set_outcome_count_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_build_occurrence(build_id,stream_slot,pack_id,origin,source_part_ordinal,part_row_ordinal,
        source_ordinal,record_kind,collection_slot,raw_parent_key_canonical,raw_parent_key_sha256,root_record_id,child_key_sha256,
        root_revision_id,child_revision_id,rejection_id)
        SELECT b.build_id,a.stream_slot,pack_id,'source',part_ordinal,part_row_ordinal,source_ordinal,kind,coalesce(stream.collection_slot,0),
            raw_key,raw_hash,root_id,child_hash,CASE WHEN rejection_code IS NULL AND kind='root' THEN outcome_id END,
            CASE WHEN rejection_code IS NULL AND kind='child' THEN outcome_id END,CASE WHEN rejection_code IS NOT NULL THEN outcome_id END
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch ORDER BY landing_ordinal;
    GET DIAGNOSTICS written_n=ROW_COUNT;
    IF written_n<>n THEN RAISE EXCEPTION 'source_set_occurrence_count_mismatch'; END IF;
    -- COPY and set promotion precede planning; analyze only this isolated candidate.
    ANALYZE __CANDIDATE__.source_bulk_landing(batch_id,landing_ordinal,pack_ordinal,pack_id,root_id,
        outcome_id,part_ordinal,part_row_ordinal,source_ordinal,typed_hash,rejection_code);
    IF a.first_source=0 OR b.source_occurrence_count+n>2*(SELECT reltuples FROM pg_class
        WHERE oid='__CANDIDATE__.custom_import_build_occurrence'::regclass) THEN
        ANALYZE __CANDIDATE__.custom_import_build_occurrence(build_id,origin,stream_slot,source_ordinal,
            occurrence_id,source_part_ordinal,part_row_ordinal,record_kind,root_record_id,collection_slot,
            raw_parent_key_sha256,child_key_sha256,child_revision_id,root_revision_id,pack_id,rejection_id,resolved_rejection_id);
    END IF;
    IF EXISTS(WITH expected AS (SELECT pack_ordinal,count(payload) accepted_count
            FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch GROUP BY pack_ordinal),
        rejected AS (SELECT landing_ordinal,b.next_rejection_ordinal+row_number() OVER (ORDER BY landing_ordinal)-1 expected_ordinal
            FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch AND rejection_code IS NOT NULL)
        SELECT 1 FROM __CANDIDATE__.source_bulk_landing l
        JOIN expected ep ON ep.pack_ordinal=l.pack_ordinal
        LEFT JOIN rejected ej ON ej.landing_ordinal=l.landing_ordinal
        LEFT JOIN __CANDIDATE__.custom_import_root_record k ON k.root_record_id=l.root_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=l.pack_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON kind='root' AND l.rejection_code IS NULL AND r.root_revision_id=l.outcome_id
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON kind='child' AND l.rejection_code IS NULL AND c.child_revision_id=l.outcome_id
        LEFT JOIN __CANDIDATE__.custom_import_rejection j ON l.rejection_code IS NOT NULL AND j.rejection_id=l.outcome_id
        WHERE l.batch_id=p_batch AND (
            p.pack_id IS NULL OR p.record_count<>ep.accepted_count OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.stream_slot,p.pack_ordinal,p.pack_sha256,p.producing_fence,p.producing_token_sha256)
                IS DISTINCT FROM ROW(b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
                    b.capture_bundle_id,a.stream_slot,l.pack_ordinal,l.pack_sha256,b.producing_fence,b.producing_token_sha256)
            OR (l.typed_key IS NOT NULL AND (k.root_record_id IS NULL OR k.root_record_id<=0 OR ROW(k.dataset_id,k.key_contract_sha256,k.logical_key_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,key_contract,l.typed_hash)
                OR convert_to(k.canonical_logical_key,'UTF8') IS DISTINCT FROM convert_to(l.typed_key,'UTF8')))
            OR (l.typed_key IS NULL AND l.root_id IS NOT NULL)
            OR (l.rejection_code IS NULL AND kind='root' AND (r.root_revision_id IS NULL OR
                ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,r.source_ordinal,r.payload_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,l.root_id,l.pack_id,l.source_ordinal,l.payload_hash)
                OR convert_to(r.canonical_payload,'UTF8') IS DISTINCT FROM convert_to(l.payload,'UTF8')))
            OR (l.rejection_code IS NULL AND kind='child' AND (c.child_revision_id IS NULL OR
                ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,
                    c.pack_id,c.source_ordinal,c.parent_key_sha256,c.child_key_sha256,c.payload_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,l.root_id,stream.collection_slot,
                    l.pack_id,l.source_ordinal,l.typed_hash,l.child_hash,l.payload_hash)
                OR convert_to(c.canonical_parent_key,'UTF8') IS DISTINCT FROM convert_to(l.typed_key,'UTF8')
                OR convert_to(c.canonical_child_key,'UTF8') IS DISTINCT FROM convert_to(l.child_key,'UTF8')
                OR convert_to(c.canonical_payload,'UTF8') IS DISTINCT FROM convert_to(l.payload,'UTF8')))
            OR (l.rejection_code IS NOT NULL AND (j.rejection_id IS NULL OR j.rejection_ordinal<>ej.expected_ordinal OR
                ROW(j.execution_id,j.dataset_id,j.definition_revision_id,j.schema_revision_id,j.pack_id,j.source_ordinal,
                    j.root_key_sha256,j.collection_slot,j.code,j.field_slot,j.producing_fence,j.producing_token_sha256)
                IS DISTINCT FROM ROW(b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,l.pack_id,
                    l.source_ordinal,l.rejection_hash,stream.collection_slot,l.rejection_code,NULL::smallint,b.producing_fence,b.producing_token_sha256)
                OR convert_to(j.canonical_root_key,'UTF8') IS DISTINCT FROM convert_to(l.rejection_key,'UTF8')
                OR convert_to(j.canonical_evidence,'UTF8') IS DISTINCT FROM convert_to(l.rejection_evidence,'UTF8')))
            OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
                WHERE o.build_id=b.build_id AND o.origin='source'
                    AND o.stream_slot=a.stream_slot AND o.source_ordinal=l.source_ordinal
                AND ROW(o.pack_id,o.origin,o.source_part_ordinal,o.part_row_ordinal,o.record_kind,
                o.collection_slot,o.raw_parent_key_sha256,o.root_record_id,o.child_key_sha256,
                o.root_revision_id,o.child_revision_id,o.rejection_id,o.resolved_rejection_id,
                o.base_family_revision_id,o.base_root_revision_id,o.base_child_revision_id)
                IS NOT DISTINCT FROM ROW(l.pack_id,'source',l.part_ordinal,l.part_row_ordinal,kind,coalesce(stream.collection_slot,0),
                    l.raw_hash,l.root_id,l.child_hash,
                    CASE WHEN l.rejection_code IS NULL AND kind='root' THEN l.outcome_id END,
                    CASE WHEN l.rejection_code IS NULL AND kind='child' THEN l.outcome_id END,
                    CASE WHEN l.rejection_code IS NOT NULL THEN l.outcome_id END,NULL::bigint,NULL::bigint,NULL::bigint,NULL::bigint)
                AND convert_to(o.raw_parent_key_canonical,'UTF8') IS NOT DISTINCT FROM convert_to(l.raw_key,'UTF8'))
        )) THEN RAISE EXCEPTION 'source_set_stored_identity_mismatch'; END IF;
    UPDATE __CONTROL__.custom_import_build_stream SET next_part_ordinal=last_part,next_part_row_ordinal=last_row,
        next_source_ordinal=a.first_source+n,next_pack_ordinal=a.first_pack+pack_n
        WHERE build_id=b.build_id AND stream_slot=a.stream_slot;
    UPDATE __CONTROL__.custom_import_build_attempt SET source_occurrence_count=source_occurrence_count+n,
        next_rejection_ordinal=next_rejection_ordinal+rejected_n WHERE build_id=b.build_id;
    IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream current_stream
            WHERE current_stream.build_id=b.build_id AND current_stream.stream_slot=a.stream_slot
                AND ROW(current_stream.next_part_ordinal,current_stream.next_part_row_ordinal,
                    current_stream.next_source_ordinal,current_stream.next_pack_ordinal,current_stream.replay_verified_at)
                IS NOT DISTINCT FROM ROW(last_part,last_row,a.first_source+n,a.first_pack+pack_n,s.replay_verified_at))
        OR NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_attempt current_build
            WHERE current_build.build_id=b.build_id AND current_build.phase='source'
                AND current_build.source_occurrence_count=b.source_occurrence_count+n
                AND current_build.next_rejection_ordinal=b.next_rejection_ordinal+rejected_n) THEN
        RAISE EXCEPTION 'source_set_counter_cursor_mismatch'; END IF;
    INSERT INTO __CONTROL__.source_bulk_completion(batch_id,transaction_id,attempted_count,
        build_id,stream_slot,first_source,after_source,input_sha256)
        SELECT p_batch,a.transaction_id,n,b.build_id,a.stream_slot,a.first_source,a.first_source+n,
            sha256(convert_to(string_agg(__CONTROL__.source_bulk_canonical(jsonb_build_array(
                pack_ordinal,encode(pack_sha256,'hex'),part_ordinal,part_row_ordinal,source_ordinal,
                raw_key,encode(raw_hash,'hex'),typed_key,encode(typed_hash,'hex'),payload,encode(payload_hash,'hex'),
                child_key,encode(child_hash,'hex'),rejection_code,rejection_key,encode(rejection_hash,'hex'),
                rejection_evidence)),E'\n' ORDER BY landing_ordinal),'UTF8'))
        FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch;
    SELECT coalesce(array_agg(outcome_id) FILTER(WHERE kind='root'),'{}'::bigint[]),
        coalesce(array_agg(outcome_id) FILTER(WHERE kind='child'),'{}'::bigint[])
        INTO fresh_root_ids,fresh_child_ids FROM __CANDIDATE__.source_bulk_landing
        WHERE batch_id=p_batch AND rejection_code IS NULL;
    PERFORM __CONTROL__.append_custom_import_revision_home(__FAMILY_ID__,fresh_root_ids,fresh_child_ids);
    DELETE FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch;
    IF NOT EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing) THEN
        TRUNCATE __CANDIDATE__.source_bulk_landing;
    END IF;
    UPDATE __CONTROL__.source_bulk_authorization SET accepting=false WHERE batch_id=p_batch;
    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer_state,producer_fence,producer_token,lease_end
        FROM __CONTROL__.custom_import_execution e JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id
        WHERE e.execution_id=b.execution_id;
    now_at:=clock_timestamp();
    IF producer_state IS DISTINCT FROM 'running' OR producer_fence IS DISTINCT FROM b.producing_fence
        OR producer_token IS DISTINCT FROM b.producing_token_sha256 OR lease_end IS NULL
        OR least(lease_end,b.build_deadline_at)<=now_at THEN RAISE EXCEPTION 'source_set_lease_lost'; END IF;
    RETURN n;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.finish_custom_import_build_source_part(p_build_id bigint,p_stream_slot smallint,p_part_ordinal integer) RETURNS integer
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    DECLARE b __CONTROL__.custom_import_build_attempt; s __CONTROL__.custom_import_build_stream; n bigint; parts integer;
    BEGIN
        b:=__CONTROL__.lock_custom_import_build(p_build_id);
        IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
            JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
            WHERE r.relation_slot=1 AND r.table_oid='__CANDIDATE__.custom_import_root_record'::regclass::oid
                AND f.landing_table_oid='__CANDIDATE__.source_bulk_landing'::regclass::oid
                AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
                AND f.frozen_at IS NULL
                AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                    f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                    IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                        b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
            RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
        LOCK TABLE __CANDIDATE__.source_bulk_landing IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing)
            OR EXISTS(SELECT 1 FROM __CONTROL__.source_bulk_authorization WHERE build_id=b.build_id AND accepting) THEN
            RAISE EXCEPTION 'source_bulk_incomplete_batch'; END IF;
        IF b.phase<>'source' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        SELECT * INTO s FROM __CONTROL__.custom_import_build_stream WHERE build_id=b.build_id AND stream_slot=p_stream_slot FOR UPDATE;
        IF s.build_id IS NULL THEN RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        IF p_part_ordinal<s.next_part_ordinal THEN RETURN s.next_part_ordinal; END IF;
        SELECT record_count INTO n FROM __CONTROL__.custom_import_capture_parquet_part
            WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=p_stream_slot AND part_ordinal=p_part_ordinal;
        SELECT payload_part_count INTO parts FROM __CONTROL__.custom_import_capture
            WHERE capture_bundle_id=b.capture_bundle_id AND stream_slot=p_stream_slot AND capture_state='sealed';
        IF n IS NULL OR parts IS NULL OR p_part_ordinal<>s.next_part_ordinal OR n<>s.next_part_row_ordinal THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __CONTROL__.custom_import_build_stream SET next_part_ordinal=p_part_ordinal+1,next_part_row_ordinal=0,
            replay_verified_at=CASE WHEN p_part_ordinal=parts THEN clock_timestamp() ELSE NULL END
            WHERE build_id=b.build_id AND stream_slot=p_stream_slot;
        RETURN p_part_ordinal+1;
    END;

$fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.freeze_custom_import_build_source(p_build_id bigint) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    DECLARE b __CONTROL__.custom_import_build_attempt;
    BEGIN
        b:=__CONTROL__.lock_custom_import_build(p_build_id);
        IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
            JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
            WHERE r.relation_slot=1 AND r.table_oid='__CANDIDATE__.custom_import_root_record'::regclass::oid
                AND f.landing_table_oid='__CANDIDATE__.source_bulk_landing'::regclass::oid
                AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
                AND f.frozen_at IS NULL
                AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                    f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                    IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                        b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
            RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
        LOCK TABLE __CANDIDATE__.source_bulk_landing IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS(SELECT 1 FROM __CANDIDATE__.source_bulk_landing)
            OR EXISTS(SELECT 1 FROM __CONTROL__.source_bulk_authorization WHERE build_id=b.build_id AND accepting) THEN
            RAISE EXCEPTION 'source_bulk_incomplete_batch'; END IF;
        IF b.phase='admission' THEN RETURN b.phase; END IF;
        IF b.phase<>'source' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream WHERE build_id=b.build_id AND replay_verified_at IS NULL)
           OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_source_stream x WHERE x.definition_revision_id=b.definition_revision_id
                AND NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream s WHERE s.build_id=b.build_id AND s.stream_slot=x.stream_slot)) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __CONTROL__.custom_import_build_attempt SET phase='admission',source_frozen_at=clock_timestamp() WHERE build_id=b.build_id;
        RETURN 'admission';
    END;

$fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.admit_custom_import_build_page(
    p_build_id bigint, p_expected_after_id bigint
) RETURNS TABLE(
    phase text, after_occurrence_id bigint, rows_processed integer,
    candidate_error_count bigint
) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $function$
DECLARE
    b __CONTROL__.custom_import_build_attempt;
    definition_streams jsonb;
    memberships jsonb;
    decisions jsonb;
    fatal_code text;
    n integer;
    last_id bigint;
    errors bigint;
    inserted_n bigint;
    updated_n bigint;
    expected_inserted_n bigint;
    expected_updated_n bigint;
    invalid_resolution_n bigint;
    remaining boolean;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_snapshot_family f
        JOIN __CONTROL__.custom_import_snapshot_relation r ON r.family_id=f.family_id
        WHERE r.relation_slot=1 AND r.table_oid='__CANDIDATE__.custom_import_root_record'::regclass::oid
            AND f.landing_table_oid='__CANDIDATE__.source_bulk_landing'::regclass::oid
            AND f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL
            AND f.frozen_at IS NULL
            AND ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
                f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                    b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
    IF b.phase<>'admission' OR b.source_frozen_at IS NULL THEN
        RAISE EXCEPTION 'custom_import_build_phase_mismatch';
    END IF;
    IF p_expected_after_id IS DISTINCT FROM b.admission_after_occurrence_id THEN
        RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_trigger t
        WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1
          AND ((t.tgrelid='__CANDIDATE__.custom_import_rejection'::regclass AND (t.tgtype & 4)=4)
            OR (t.tgrelid='__CANDIDATE__.custom_import_build_occurrence'::regclass AND (t.tgtype & 16)=16))
    ) THEN
        RAISE EXCEPTION 'custom_import_bulk_shared_migration_required';
    END IF;
    IF EXISTS(SELECT 1 FROM pg_constraint c WHERE c.contype='f' AND c.conrelid=ANY(ARRAY[
        '__CANDIDATE__.custom_import_rejection'::regclass,'__CANDIDATE__.custom_import_build_occurrence'::regclass])) THEN
        RAISE EXCEPTION 'custom_import_bulk_hot_relationship_guards_present'; END IF;
    IF EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_generation_seal s
        WHERE s.execution_id=b.execution_id AND s.dataset_id=b.dataset_id
        UNION ALL
        SELECT 1 FROM __CONTROL__.custom_import_no_change_seal s
        WHERE s.execution_id=b.execution_id AND s.dataset_id=b.dataset_id
    ) THEN
        RAISE EXCEPTION 'custom_import_sealed_append';
    END IF;
    IF EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_build_stream s
        WHERE s.build_id=b.build_id AND s.replay_verified_at IS NULL
    ) OR NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_capture_bundle c
        WHERE c.capture_bundle_id=b.capture_bundle_id AND c.capture_state='sealed'
          AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id)
            IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id)
    ) THEN
        RAISE EXCEPTION 'custom_import_build_incomplete';
    END IF;
    SELECT d.canonical_definition::jsonb->'streams',
           coalesce(d.canonical_definition::jsonb->'child_memberships','[]'::jsonb)
      INTO definition_streams,memberships
    FROM __CONTROL__.custom_import_definition_revision d
    WHERE d.definition_revision_id=b.definition_revision_id;

    WITH
    candidates AS MATERIALIZED (
        SELECT x.occurrence_id,x.build_id,x.record_kind,x.stream_slot,x.pack_id,x.source_ordinal,
            x.collection_slot,x.raw_parent_key_sha256,x.root_record_id,x.root_revision_id,x.child_revision_id,
            x.child_key_sha256,x.rejection_id,x.resolved_rejection_id,
            coalesce(octet_length(x.raw_parent_key_canonical),0) raw_parent_bytes,j.code initial_code,
            coalesce(definition_streams->(x.stream_slot-1)->>'duplicate_policy','reject')='collapse_identical' collapse_identical
        FROM __CANDIDATE__.custom_import_build_occurrence x
        LEFT JOIN __CANDIDATE__.custom_import_rejection j ON j.rejection_id=x.rejection_id
        WHERE x.build_id=b.build_id AND x.origin='source'
          AND x.occurrence_id>b.admission_after_occurrence_id
        ORDER BY x.occurrence_id LIMIT b.page_row_limit
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
        WHERE x.build_id=b.build_id
        ORDER BY x.record_kind,x.raw_parent_key_sha256,x.occurrence_id
    ),
    source_roots AS MATERIALIZED (
        SELECT x.raw_parent_key_sha256,min(x.occurrence_id) parent_id,count(*) raw_root_count
        FROM __CANDIDATE__.custom_import_build_occurrence x
        JOIN raw_keys k ON k.record_kind='root' AND k.raw_parent_key_sha256=x.raw_parent_key_sha256
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
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
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
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
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='child'
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
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.child_revision_id IS NOT NULL
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
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.child_revision_id IS NOT NULL
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
              +CASE WHEN memberships<>'[]'::jsonb AND child.child_revision_id IS NOT NULL
                THEN octet_length(child.canonical_child_key)*3+octet_length(memberships::text)*2 ELSE 0 END raw_bytes,
            (pack.pack_id IS NULL OR ROW(pack.execution_id,pack.producing_fence,pack.producing_token_sha256,
                pack.capture_bundle_id,pack.stream_slot,pack.dataset_id,pack.definition_revision_id,pack.schema_revision_id)
              IS DISTINCT FROM ROW(b.execution_id,b.producing_fence,b.producing_token_sha256,b.capture_bundle_id,
                c.stream_slot,b.dataset_id,b.definition_revision_id,b.schema_revision_id)
              OR stream.build_id IS NULL OR stream.next_pack_ordinal<=pack.pack_ordinal) invalid_pack
        FROM candidates c
        JOIN __CANDIDATE__.custom_import_build_occurrence current_raw ON current_raw.occurrence_id=c.occurrence_id
        LEFT JOIN __CANDIDATE__.custom_import_root_record k ON k.root_record_id=c.root_record_id AND k.dataset_id=b.dataset_id
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
        LEFT JOIN __CONTROL__.custom_import_build_stream stream ON stream.build_id=b.build_id AND stream.stream_slot=c.stream_slot
    ),
    sized AS MATERIALIZED (
        SELECT m.*,sum(m.raw_bytes::bigint) OVER (ORDER BY m.occurrence_id ROWS UNBOUNDED PRECEDING) prefix_bytes
        FROM metadata m
    ),
    page AS MATERIALIZED (
        SELECT s.* FROM sized s WHERE s.prefix_bytes<=b.page_byte_limit
    ),
    byte_boundary AS MATERIALIZED (
        SELECT s.occurrence_id,s.raw_bytes FROM sized s
        WHERE s.prefix_bytes>b.page_byte_limit ORDER BY s.occurrence_id LIMIT 1
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
          ON inner_collection.schema_revision_id=b.schema_revision_id AND inner_collection.collection_slot=p.collection_slot
        JOIN jsonb_array_elements(memberships) WITH ORDINALITY m(value,ordinality)
          ON m.value->>'inner_collection'=inner_collection.collection_name
        LEFT JOIN __CONTROL__.custom_import_child_collection outer_collection
          ON outer_collection.schema_revision_id=b.schema_revision_id AND outer_collection.collection_name=m.value->>'outer_collection'
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
        JOIN __CANDIDATE__.custom_import_build_occurrence x ON x.build_id=b.build_id AND x.origin='source'
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
        FROM byte_boundary boundary WHERE boundary.raw_bytes>b.page_byte_limit
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
    SELECT (SELECT f.problem FROM failures f ORDER BY f.occurrence_id,f.stage LIMIT 1),
        count(*)::integer,coalesce(max(d.occurrence_id),b.admission_after_occurrence_id),
        count(*) FILTER (WHERE d.error_code IN ('root_not_object','root_key_missing','child_not_object','orphan_child')),
        coalesce(jsonb_agg(jsonb_build_object(
            'occurrence_id',d.occurrence_id,'parent_id',d.parent_id,'record_kind',d.record_kind,
            'pack_id',d.pack_id,'source_ordinal',d.source_ordinal,'collection_slot',d.collection_slot,
            'initial_code',d.initial_code,'code',d.code,'rejection_id',d.rejection_id,
            'own_action',coalesce(d.own_action,false),'root_record_id',d.root_record_id
        ) ORDER BY d.occurrence_id),'[]'::jsonb)
      INTO fatal_code,n,last_id,errors,decisions FROM ordered_decisions d;
    IF fatal_code IS NOT NULL THEN RAISE EXCEPTION '%',fatal_code; END IF;

    WITH
    page_decisions AS MATERIALIZED (
        SELECT d.* FROM jsonb_to_recordset(decisions) AS d(
            occurrence_id bigint,parent_id bigint,record_kind text,pack_id bigint,source_ordinal bigint,
            collection_slot smallint,initial_code text,code text,rejection_id bigint,own_action boolean,
            root_record_id bigint
        )
    ),
    new_rejection_events AS MATERIALIZED (
        SELECT d.*,b.next_rejection_ordinal+row_number() OVER (ORDER BY d.occurrence_id)-1 rejection_ordinal
        FROM page_decisions d WHERE d.own_action AND d.code IS DISTINCT FROM d.initial_code
    ),
    new_rejections AS (
        INSERT INTO __CANDIDATE__.custom_import_rejection(
            rejection_id,execution_id,rejection_ordinal,dataset_id,definition_revision_id,schema_revision_id,pack_id,
            root_key_sha256,canonical_root_key,collection_slot,source_ordinal,code,canonical_evidence,
            producing_fence,producing_token_sha256
        )
        SELECT nextval('__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass),
            b.execution_id,d.rejection_ordinal,b.dataset_id,b.definition_revision_id,b.schema_revision_id,d.pack_id,
            root.logical_key_sha256,root.canonical_logical_key,nullif(d.collection_slot,0),d.source_ordinal,d.code,'{}',
            b.producing_fence,b.producing_token_sha256 FROM new_rejection_events d
        LEFT JOIN __CANDIDATE__.custom_import_root_record root ON root.root_record_id=d.root_record_id AND root.dataset_id=b.dataset_id
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
        JOIN __CANDIDATE__.custom_import_build_occurrence x ON x.occurrence_id=t.target_id AND x.build_id=b.build_id
          AND x.origin='source' AND x.resolved_rejection_id IS NULL
        ORDER BY t.target_id,t.event_id
    ),
    updated AS (
        UPDATE __CANDIDATE__.custom_import_build_occurrence x SET resolved_rejection_id=t.resolution
        FROM chosen_targets t WHERE x.occurrence_id=t.target_id AND x.build_id=b.build_id
          AND x.resolved_rejection_id IS NULL
        RETURNING x.occurrence_id,x.resolved_rejection_id
    )
    SELECT (SELECT count(*) FROM new_rejections),(SELECT count(*) FROM updated),
        (SELECT count(*) FROM new_rejection_events),(SELECT count(*) FROM chosen_targets),
        (SELECT count(*) FROM updated u
            LEFT JOIN new_rejections fresh ON fresh.rejection_id=u.resolved_rejection_id
            LEFT JOIN __CANDIDATE__.custom_import_rejection prior ON prior.rejection_id=u.resolved_rejection_id
            WHERE coalesce(fresh.rejection_id,prior.rejection_id) IS NULL
                OR ROW(coalesce(fresh.execution_id,prior.execution_id),coalesce(fresh.dataset_id,prior.dataset_id),
                    coalesce(fresh.definition_revision_id,prior.definition_revision_id),coalesce(fresh.schema_revision_id,prior.schema_revision_id),
                    coalesce(fresh.producing_fence,prior.producing_fence),coalesce(fresh.producing_token_sha256,prior.producing_token_sha256))
                    IS DISTINCT FROM ROW(b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
                        b.producing_fence,b.producing_token_sha256))
      INTO inserted_n,updated_n,expected_inserted_n,expected_updated_n,invalid_resolution_n;
    IF inserted_n<>expected_inserted_n OR updated_n<>expected_updated_n OR invalid_resolution_n<>0 THEN
        RAISE EXCEPTION 'custom_import_build_structure_mismatch: admission aggregate differs';
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence x
        WHERE x.build_id=b.build_id AND x.origin='source' AND x.occurrence_id>last_id
    ) INTO remaining;
    IF NOT remaining AND b.refresh_mode='snapshot' AND NOT b.complete_scope THEN errors:=errors+1; END IF;
    UPDATE __CONTROL__.custom_import_build_attempt a
       SET admission_after_occurrence_id=last_id,
           candidate_error_count=a.candidate_error_count+errors,
           next_rejection_ordinal=a.next_rejection_ordinal+inserted_n,
           phase=CASE WHEN remaining THEN 'admission'
             WHEN a.candidate_error_count+errors>0 THEN 'rejected' ELSE 'graph' END
    WHERE a.build_id=b.build_id RETURNING a.* INTO b;
    RETURN QUERY SELECT b.phase::text,b.admission_after_occurrence_id,n,b.candidate_error_count;
END;
$function$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.start_custom_import_build_source_roots_page(
    p_build_id bigint,p_execution_id bigint,p_fence bigint,p_token_sha256 bytea,
    p_root_ids bigint[],p_occurrence_ids bigint[],p_revision_ids bigint[],p_payload_hashes bytea[],
    p_family_hashes bytea[],p_child_counts bigint[],p_entity_values text[],p_entity_hashes bytea[],
    p_scalar_root_ids bigint[],p_field_slots smallint[],p_field_types text[],p_value_states text[],
    p_string_values text[],p_integer_values bigint[],p_decimal_values numeric[],p_boolean_values boolean[],
    p_date_values date[],p_timestamp_values timestamptz[],
    p_context_root_ids bigint[],p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]
) RETURNS TABLE(root_record_id bigint,family_revision_id bigint,root_revision_id bigint,entity_binding_id bigint,
    attached_child_count bigint,last_child_collection_slot smallint,last_child_key_sha256 bytea,
    last_input_child_revision_id bigint,complete boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; owner_name name;
    n integer; scalar_n integer; context_n integer; entity_n bigint:=0;
    fresh_ids bigint[]; family_ids bigint[]; entity_ids bigint[]; root_pages integer[]; entity_pages integer[];
    entity_costs bigint[]; work_bytes bigint; model_bytes bigint; fresh_context_n bigint; invalid_page boolean;
    definition_streams jsonb;
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'graph_roots_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF ROW(b.execution_id,b.producing_fence,b.producing_token_sha256)
        IS DISTINCT FROM ROW(p_execution_id,p_fence,p_token_sha256)
        OR b.phase<>'graph' OR b.plan_complete_at IS NULL OR b.generation_id IS NOT NULL
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=b.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'graph_roots_authority_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype::integer & 4)<>0
        AND t.tgrelid IN ('__CONTROL__.custom_import_entity_binding'::regclass,
            '__CANDIDATE__.custom_import_root_record'::regclass,'__CANDIDATE__.custom_import_entity_binding'::regclass,
            '__CANDIDATE__.custom_import_family_revision'::regclass,'__CANDIDATE__.custom_import_root_scalar'::regclass,
            '__CANDIDATE__.custom_import_build_candidate_context'::regclass)) THEN
        RAISE EXCEPTION 'graph_roots_requires_shared_set_boundary'; END IF;
    n:=cardinality(p_root_ids); scalar_n:=cardinality(p_field_slots); context_n:=cardinality(p_profile_slots);
    IF num_nonnulls(p_root_ids,p_occurrence_ids,p_revision_ids,p_payload_hashes,p_family_hashes,p_child_counts,
        p_entity_values,p_entity_hashes,p_scalar_root_ids,p_field_slots,p_field_types,p_value_states,p_string_values,
        p_integer_values,p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values,
        p_context_root_ids,p_profile_slots,p_context_keys,p_page_sizes)<>22
        OR n NOT BETWEEN 1 AND 100000 OR scalar_n NOT BETWEEN 0 AND 100000 OR context_n NOT BETWEEN 0 AND 100000
        OR cardinality(p_page_sizes) NOT BETWEEN 1 AND 100000 OR array_ndims(p_page_sizes) IS DISTINCT FROM 1
        OR array_lower(p_page_sizes,1) IS DISTINCT FROM 1
        OR EXISTS(SELECT 1 FROM unnest(p_page_sizes) size WHERE size IS NULL OR size NOT BETWEEN 1 AND 256)
        OR (SELECT sum(size::bigint) FROM unnest(p_page_sizes) size) IS DISTINCT FROM n::bigint
        OR NOT n=ALL(ARRAY[cardinality(p_occurrence_ids),cardinality(p_revision_ids),cardinality(p_payload_hashes),
            cardinality(p_family_hashes),cardinality(p_child_counts),cardinality(p_entity_values),cardinality(p_entity_hashes)])
        OR NOT scalar_n=ALL(ARRAY[cardinality(p_scalar_root_ids),cardinality(p_field_types),cardinality(p_value_states),
            cardinality(p_string_values),cardinality(p_integer_values),cardinality(p_decimal_values),
            cardinality(p_boolean_values),cardinality(p_date_values),cardinality(p_timestamp_values)])
        OR NOT context_n=ALL(ARRAY[cardinality(p_context_root_ids),cardinality(p_context_keys)])
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_ndims(p_root_ids),array_ndims(p_occurrence_ids),
            array_ndims(p_revision_ids),array_ndims(p_payload_hashes),array_ndims(p_family_hashes),array_ndims(p_child_counts),
            array_ndims(p_entity_values),array_ndims(p_entity_hashes),array_ndims(p_scalar_root_ids),array_ndims(p_field_slots),
            array_ndims(p_field_types),array_ndims(p_value_states),array_ndims(p_string_values),array_ndims(p_integer_values),
            array_ndims(p_decimal_values),array_ndims(p_boolean_values),array_ndims(p_date_values),array_ndims(p_timestamp_values),
            array_ndims(p_context_root_ids),array_ndims(p_profile_slots),array_ndims(p_context_keys)]) d WHERE d>1)
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_lower(p_root_ids,1),array_lower(p_occurrence_ids,1),
            array_lower(p_revision_ids,1),array_lower(p_payload_hashes,1),array_lower(p_family_hashes,1),array_lower(p_child_counts,1),
            array_lower(p_entity_values,1),array_lower(p_entity_hashes,1),array_lower(p_scalar_root_ids,1),array_lower(p_field_slots,1),
            array_lower(p_field_types,1),array_lower(p_value_states,1),array_lower(p_string_values,1),array_lower(p_integer_values,1),
            array_lower(p_decimal_values,1),array_lower(p_boolean_values,1),array_lower(p_date_values,1),array_lower(p_timestamp_values,1),
            array_lower(p_context_root_ids,1),array_lower(p_profile_slots,1),array_lower(p_context_keys,1)]) d WHERE d<>1)
        OR p_root_ids IS DISTINCT FROM (SELECT array_agg(DISTINCT id ORDER BY id) FROM unnest(p_root_ids) i(id))
        OR (SELECT count(DISTINCT (root_id,slot)) FROM unnest(p_scalar_root_ids,p_field_slots) s(root_id,slot))<>scalar_n
        OR (SELECT count(DISTINCT (root_id,slot)) FROM unnest(p_context_root_ids,p_profile_slots) c(root_id,slot))<>context_n
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_occurrence_ids,p_revision_ids,p_payload_hashes,p_family_hashes,
            p_child_counts,p_entity_values,p_entity_hashes) r(root_id,occurrence_id,revision_id,payload_hash,family_hash,
                child_count,entity_value,entity_hash)
            WHERE root_id IS NULL OR root_id<=0 OR occurrence_id IS NULL OR occurrence_id<=0
                OR revision_id IS NULL OR revision_id<=0 OR payload_hash IS NULL OR octet_length(payload_hash)<>32
                OR family_hash IS NULL OR octet_length(family_hash)<>32 OR child_count IS NULL OR child_count<0
                OR entity_value IS NULL OR octet_length(entity_value) NOT BETWEEN 1 AND 512
                OR entity_hash IS DISTINCT FROM sha256(
                    decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100656e746974793a6e706900','hex')
                    ||convert_to(entity_value,'UTF8'))) THEN RAISE EXCEPTION 'graph_roots_bounds'; END IF;
    SELECT array_agg(page::integer ORDER BY page,position) INTO root_pages
        FROM unnest(p_page_sizes) WITH ORDINALITY i(size,page) CROSS JOIN LATERAL generate_series(1,i.size) position;
    IF 5::bigint*n+scalar_n+context_n>100000 THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    PERFORM 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_occurrence_ids,p_revision_ids,p_payload_hashes)
        i(root_id,occurrence_id,revision_id,payload_hash)
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.occurrence_id=i.occurrence_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=i.revision_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        LEFT JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
            AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id AND ss.stream_slot=o.stream_slot
            AND ss.record_kind='root' AND ss.collection_slot IS NULL
        LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=ss.stream_slot
        WHERE ROW(bf.selection_kind,bf.source_root_occurrence_id,bf.root_key_sha256)
                IS DISTINCT FROM ROW('source'::text,i.occurrence_id,k.logical_key_sha256)
            OR ROW(o.origin,o.record_kind,o.collection_slot,o.root_record_id,o.root_revision_id,o.resolved_rejection_id)
                IS DISTINCT FROM ROW('source'::text,'root'::text,0::smallint,i.root_id,i.revision_id,NULL::bigint)
            OR ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,r.source_ordinal,r.payload_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.root_id,o.pack_id,o.source_ordinal,i.payload_hash)
            OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
                p.producing_fence,p.producing_token_sha256,p.stream_slot) IS DISTINCT FROM ROW(b.execution_id,b.dataset_id,
                    b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256,ss.stream_slot)
            OR bs.replay_verified_at IS NULL OR p.pack_ordinal>=bs.next_pack_ordinal) THEN
        RAISE EXCEPTION 'graph_roots_identity_mismatch'; END IF;
    SELECT canonical_definition::jsonb->'streams' INTO definition_streams
        FROM __CONTROL__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id
            AND dataset_id=b.dataset_id AND schema_revision_id=b.schema_revision_id;
    IF definition_streams IS NULL THEN RAISE EXCEPTION 'graph_roots_definition_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_child_counts) i(root_id,child_count)
        WHERE (i.child_count=0) IS DISTINCT FROM (NOT EXISTS(
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence c WHERE c.build_id=b.build_id
                AND c.origin='source' AND c.root_record_id=i.root_id AND c.child_revision_id IS NOT NULL
                AND c.resolved_rejection_id IS NULL AND
                (coalesce(definition_streams->(c.stream_slot-1)->>'duplicate_policy','reject')<>'collapse_identical'
                    OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence later
                        WHERE later.build_id=c.build_id AND later.origin='source' AND later.stream_slot=c.stream_slot
                            AND later.root_record_id=c.root_record_id AND later.collection_slot=c.collection_slot
                            AND later.raw_parent_key_sha256=c.raw_parent_key_sha256 AND later.child_key_sha256=c.child_key_sha256
                            AND later.child_revision_id IS NOT NULL AND later.source_ordinal>c.source_ordinal))))) THEN
        RAISE EXCEPTION 'graph_roots_child_count_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_scalar_root_ids,p_field_slots,p_field_types,p_value_states,p_string_values,
        p_integer_values,p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values)
        s(root_id,field_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        LEFT JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=0 AND f.field_slot=s.field_slot
        WHERE s.root_id IS NULL OR NOT s.root_id=ANY(p_root_ids) OR f.field_slot IS NULL OR f.projection_slot<=0
            OR s.field_type IS DISTINCT FROM f.field_type OR s.value_state IS NULL OR s.value_state NOT IN ('value','null')
            OR (s.value_state='null' AND NOT f.is_nullable) OR octet_length(s.string_value)>4096
            OR s.decimal_value::text IN ('NaN','Infinity','-Infinity')
            OR s.decimal_value IS DISTINCT FROM s.decimal_value::numeric(30,12)) THEN
        RAISE EXCEPTION 'graph_roots_scalar_mismatch'; END IF;
    IF EXISTS(WITH expected AS (
        SELECT i.root_id,f.field_slot,e.value->'value'->>'state' value_state
        FROM unnest(p_root_ids,p_revision_ids) i(root_id,revision_id)
        JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=i.revision_id
        JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=0 AND f.projection_slot>0
        JOIN LATERAL json_array_elements(replace(r.canonical_payload,chr(92)||'u0000',chr(92)||'u0001')::json->'fields') e(value)
            ON e.value->>'field'=f.field_name
        WHERE e.value->'value'->>'state' IS DISTINCT FROM 'missing'
    ), supplied AS (SELECT * FROM unnest(p_scalar_root_ids,p_field_slots,p_value_states) s(root_id,field_slot,value_state))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
        (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_scalar_presence'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
        WHERE c.root_id IS NULL OR NOT c.root_id=ANY(p_root_ids) OR c.slot IS NULL OR c.canonical IS NULL
            OR octet_length(c.canonical) NOT BETWEEN 1 AND 8192)
        OR EXISTS(WITH expected AS (SELECT i.root_id,p.profile_slot FROM unnest(p_root_ids) i(root_id)
            CROSS JOIN __CONTROL__.custom_import_selection_profile p WHERE p.definition_revision_id=b.definition_revision_id
                AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id
                AND coalesce(p.context_collection_slot,0)=0), supplied AS (
            SELECT * FROM unnest(p_context_root_ids,p_profile_slots) c(root_id,profile_slot))
            (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
            (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_context_mismatch'; END IF;
    SELECT coalesce(array_agg(bf.root_record_id ORDER BY bf.root_record_id),'{}'::bigint[]) INTO fresh_ids
        FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids)
            AND bf.family_revision_id IS NULL;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(fresh_ids) AND (bf.attached_child_count<>0 OR bf.complete_at IS NOT NULL
            OR num_nonnulls(bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id)<>0))
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_revision_ids) i(root_id,revision_id)
            JOIN __CANDIDATE__.custom_import_root_scalar s ON s.root_revision_id=i.revision_id WHERE i.root_id=ANY(fresh_ids)) THEN
        RAISE EXCEPTION 'graph_roots_uncommitted_work'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_revision_ids) i(root_id,revision_id)
        JOIN __CANDIDATE__.custom_import_root_scalar s ON s.root_revision_id=i.revision_id
        LEFT JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.field_slot=s.field_slot AND f.collection_slot=0 AND f.projection_slot>0
        WHERE ROW(s.dataset_id,s.schema_revision_id,s.root_record_id,s.field_collection_slot,s.projection_slot,s.field_type)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,0::smallint,f.projection_slot,f.field_type)
            OR f.field_slot IS NULL) THEN RAISE EXCEPTION 'graph_roots_retry_scalar_owner_mismatch'; END IF;
    WITH inserted AS (INSERT INTO __CONTROL__.custom_import_entity_binding(dataset_id,adapter_id,canonical_value,value_sha256)
        SELECT DISTINCT b.dataset_id,'npi',i.entity_value,i.entity_hash
        FROM unnest(p_root_ids,p_entity_values,p_entity_hashes) i(root_id,entity_value,entity_hash) WHERE i.root_id=ANY(fresh_ids)
        ON CONFLICT(dataset_id,adapter_id,canonical_value) DO NOTHING RETURNING canonical_value), charged AS (
        SELECT inserted.canonical_value,min(i.page)::integer page,octet_length(inserted.canonical_value)+35::bigint bytes
        FROM inserted JOIN unnest(p_root_ids,p_entity_values,root_pages) i(root_id,entity_value,page)
            ON i.entity_value=inserted.canonical_value WHERE i.root_id=ANY(fresh_ids)
        GROUP BY inserted.canonical_value)
    SELECT count(*),coalesce(array_agg(page ORDER BY canonical_value),'{}'::integer[]),
        coalesce(array_agg(bytes ORDER BY canonical_value),'{}'::bigint[])
        INTO entity_n,entity_pages,entity_costs FROM charged;
    SELECT array_agg(e.entity_binding_id ORDER BY i.ordinality) INTO entity_ids
        FROM unnest(p_entity_values,p_entity_hashes) WITH ORDINALITY i(entity_value,entity_hash,ordinality)
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=b.dataset_id
            AND e.adapter_id='npi' AND e.canonical_value=i.entity_value AND e.value_sha256=i.entity_hash;
    IF EXISTS(SELECT 1 FROM unnest(entity_ids) e WHERE e IS NULL) THEN RAISE EXCEPTION 'graph_roots_entity_mismatch'; END IF;
    IF 5::bigint*n+scalar_n+context_n+entity_n>100000 OR EXISTS(
        WITH costs AS (
            SELECT page,5::bigint cost,0 scalar_count,0 context_count FROM unnest(root_pages) page
            UNION ALL SELECT i.page,1,1,0 FROM unnest(p_scalar_root_ids) s(root_id)
                JOIN unnest(p_root_ids,root_pages) i(root_id,page) USING(root_id)
            UNION ALL SELECT i.page,1,0,1 FROM unnest(p_context_root_ids) c(root_id)
                JOIN unnest(p_root_ids,root_pages) i(root_id,page) USING(root_id)
            UNION ALL SELECT page,1,0,0 FROM unnest(entity_pages) page)
        SELECT 1 FROM costs GROUP BY page HAVING sum(cost)>b.page_row_limit
            OR sum(scalar_count)>256 OR sum(context_count)>256) THEN
        RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_revision_ids,p_family_hashes,p_child_counts,entity_ids)
        i(root_id,revision_id,family_hash,child_count,entity_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=bf.family_revision_id
        WHERE NOT i.root_id=ANY(fresh_ids) AND (ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.root_revision_id,
            f.entity_binding_id,f.family_sha256,f.child_count,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,i.revision_id,i.entity_id,i.family_hash,
                i.child_count,b.execution_id,b.producing_fence,b.producing_token_sha256)
            OR bf.attached_child_count>i.child_count OR (bf.complete_at IS NOT NULL AND bf.attached_child_count<>i.child_count)
            OR (i.child_count=0 AND bf.complete_at IS NULL))) THEN RAISE EXCEPTION 'graph_roots_retry_mismatch'; END IF;
    IF EXISTS(WITH expected AS (SELECT * FROM unnest(p_scalar_root_ids,p_field_slots,p_field_types,p_value_states,
            p_string_values,p_integer_values,p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values)
            s(root_id,field_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
            WHERE NOT root_id=ANY(fresh_ids)), actual AS (
            SELECT i.root_id,s.field_slot,s.field_type::text,s.value_state::text,s.string_value::text,s.integer_value,
                s.decimal_value,s.boolean_value,s.date_value,s.timestamp_value FROM unnest(p_root_ids,p_revision_ids) i(root_id,revision_id)
            JOIN __CANDIDATE__.custom_import_root_scalar s ON s.root_revision_id=i.revision_id WHERE NOT i.root_id=ANY(fresh_ids))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_retry_scalar_mismatch'; END IF;
    SELECT array_agg(coalesce(bf.family_revision_id,
        nextval('__CONTROL__.custom_import_family_revision_family_revision_id_seq'::regclass)) ORDER BY i.ordinality)
        INTO family_ids FROM unnest(p_root_ids) WITH ORDINALITY i(root_id,ordinality)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id;
    IF EXISTS(WITH expected AS (
        SELECT i.family_id,c.slot,i.entity_id,0::smallint collection_slot,convert_to(c.canonical,'UTF8') canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8')) digest
        FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
        JOIN unnest(p_root_ids,family_ids,entity_ids) i(root_id,family_id,entity_id) USING(root_id)
        WHERE NOT i.root_id=ANY(fresh_ids)), actual AS (
        SELECT c.family_revision_id,c.profile_slot,c.entity_binding_id,c.context_collection_slot,
            convert_to(c.canonical_context_key,'UTF8'),c.context_key_sha256
        FROM unnest(p_root_ids,family_ids) i(root_id,family_id)
        JOIN __CANDIDATE__.custom_import_build_candidate_context c ON c.build_id=b.build_id
            AND c.family_revision_id=i.family_id AND c.context_child_revision_id IS NULL
        WHERE NOT i.root_id=ANY(fresh_ids))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_retry_context_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_root_record(root_record_id,dataset_id,key_contract_sha256,
        canonical_logical_key,logical_key_sha256,created_at)
        SELECT k.root_record_id,k.dataset_id,k.key_contract_sha256,k.canonical_logical_key,k.logical_key_sha256,k.created_at
        FROM __CONTROL__.custom_import_root_record k WHERE k.dataset_id=b.dataset_id AND k.root_record_id=ANY(fresh_ids)
        ON CONFLICT ON CONSTRAINT custom_import_root_record_pkey DO NOTHING;
    INSERT INTO __CANDIDATE__.custom_import_entity_binding(entity_binding_id,dataset_id,adapter_id,
        canonical_value,value_sha256,created_at)
        SELECT e.entity_binding_id,e.dataset_id,e.adapter_id,e.canonical_value,e.value_sha256,e.created_at
        FROM __CONTROL__.custom_import_entity_binding e WHERE e.dataset_id=b.dataset_id
            AND e.entity_binding_id IN (SELECT i.entity_id FROM unnest(p_root_ids,entity_ids) i(root_id,entity_id)
                WHERE i.root_id=ANY(fresh_ids))
        ON CONFLICT ON CONSTRAINT custom_import_entity_binding_pkey DO NOTHING;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,entity_ids) i(root_id,entity_id)
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_root_record copied_k ON copied_k.root_record_id=i.root_id
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=i.entity_id AND e.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_entity_binding copied_e ON copied_e.entity_binding_id=i.entity_id
        WHERE k.root_record_id IS NULL OR e.entity_binding_id IS NULL
            OR ROW(copied_k.root_record_id,copied_k.dataset_id,copied_k.key_contract_sha256,
                convert_to(copied_k.canonical_logical_key,'UTF8'),copied_k.logical_key_sha256,copied_k.created_at)
                IS DISTINCT FROM ROW(k.root_record_id,k.dataset_id,k.key_contract_sha256,
                    convert_to(k.canonical_logical_key,'UTF8'),k.logical_key_sha256,k.created_at)
            OR ROW(copied_e.entity_binding_id,copied_e.dataset_id,convert_to(copied_e.adapter_id,'UTF8'),
                convert_to(copied_e.canonical_value,'UTF8'),copied_e.value_sha256,copied_e.created_at)
                IS DISTINCT FROM ROW(e.entity_binding_id,e.dataset_id,convert_to(e.adapter_id,'UTF8'),
                    convert_to(e.canonical_value,'UTF8'),e.value_sha256,e.created_at)) THEN
        RAISE EXCEPTION 'graph_roots_identity_copy_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_family_revision(family_revision_id,dataset_id,schema_revision_id,root_record_id,
        root_revision_id,entity_binding_id,family_sha256,child_count,producing_execution_id,producing_fence,producing_token_sha256)
        SELECT i.family_id,b.dataset_id,b.schema_revision_id,i.root_id,i.revision_id,i.entity_id,i.family_hash,i.child_count,
            b.execution_id,b.producing_fence,b.producing_token_sha256
        FROM unnest(p_root_ids,p_revision_ids,family_ids,entity_ids,p_family_hashes,p_child_counts)
            i(root_id,revision_id,family_id,entity_id,family_hash,child_count) WHERE i.root_id=ANY(fresh_ids);
    INSERT INTO __CANDIDATE__.custom_import_root_scalar(root_revision_id,dataset_id,schema_revision_id,root_record_id,
        field_slot,field_collection_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,
        boolean_value,date_value,timestamp_value)
        SELECT i.revision_id,b.dataset_id,b.schema_revision_id,i.root_id,s.field_slot,0,f.projection_slot,
            s.field_type,s.value_state,s.string_value,s.integer_value,s.decimal_value,s.boolean_value,s.date_value,s.timestamp_value
        FROM unnest(p_scalar_root_ids,p_field_slots,p_field_types,p_value_states,p_string_values,p_integer_values,
            p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values)
            s(root_id,field_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        JOIN unnest(p_root_ids,p_revision_ids) i(root_id,revision_id) USING(root_id)
        JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=0 AND f.field_slot=s.field_slot
        WHERE i.root_id=ANY(fresh_ids);
    INSERT INTO __CANDIDATE__.custom_import_build_candidate_context(build_id,profile_slot,entity_binding_id,family_revision_id,
        context_collection_slot,context_child_revision_id,canonical_context_key,context_key_sha256)
        SELECT b.build_id,c.slot,i.entity_id,i.family_id,0,NULL,c.canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8'))
        FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
        JOIN unnest(p_root_ids,family_ids,entity_ids) i(root_id,family_id,entity_id) USING(root_id) WHERE i.root_id=ANY(fresh_ids);
    GET DIAGNOSTICS fresh_context_n=ROW_COUNT;
    WITH costs AS (
        SELECT i.page,octet_length(r.canonical_payload)+octet_length(k.canonical_logical_key)
            +coalesce(octet_length(o.raw_parent_key_canonical),0) work,64::bigint model
        FROM unnest(p_root_ids,p_revision_ids,p_occurrence_ids,root_pages) i(root_id,revision_id,occurrence_id,page)
        JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=i.revision_id
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id
        JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.occurrence_id=i.occurrence_id
        UNION ALL SELECT i.page,octet_length(k.canonical_logical_key)+64+octet_length(e.adapter_id)+octet_length(e.canonical_value)+32,
            octet_length(k.canonical_logical_key)+64+octet_length(e.adapter_id)+octet_length(e.canonical_value)+32
        FROM unnest(p_root_ids,entity_ids,root_pages) i(root_id,entity_id,page)
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=i.entity_id AND e.dataset_id=b.dataset_id
        UNION ALL SELECT i.page,octet_length(to_jsonb(s)::text),
            octet_length(s.field_type)+octet_length(s.value_state)+coalesce(octet_length(s.string_value),0)
        FROM unnest(p_revision_ids,root_pages) i(revision_id,page)
        JOIN __CANDIDATE__.custom_import_root_scalar s ON s.root_revision_id=i.revision_id
        UNION ALL SELECT i.page,octet_length(c.canonical),octet_length(c.canonical)+32
        FROM unnest(p_context_root_ids,p_context_keys) c(root_id,canonical)
        JOIN unnest(p_root_ids,root_pages) i(root_id,page) USING(root_id)
        UNION ALL SELECT i.page,0,i.bytes FROM unnest(entity_pages,entity_costs) i(page,bytes)
    ), pages AS (SELECT page,sum(work) work,sum(model) model FROM costs GROUP BY page)
    SELECT coalesce(sum(work),0),coalesce(sum(model),0),
        coalesce(bool_or(greatest(work,model)>b.page_byte_limit),false)
        INTO work_bytes,model_bytes,invalid_page FROM pages;
    IF invalid_page OR greatest(work_bytes,model_bytes)>268435456 THEN
        RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    UPDATE __CANDIDATE__.custom_import_build_family bf SET family_revision_id=i.family_id,
        complete_at=CASE WHEN i.child_count=0 THEN clock_timestamp() END
        FROM unnest(p_root_ids,family_ids,p_child_counts) i(root_id,family_id,child_count)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=i.root_id AND i.root_id=ANY(fresh_ids);
    UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=candidate_context_count+fresh_context_n,
        completed_family_count=completed_family_count+(SELECT count(*) FROM unnest(p_root_ids,p_child_counts) i(root_id,child_count)
            WHERE i.root_id=ANY(fresh_ids) AND i.child_count=0)
        WHERE build_id=b.build_id AND cardinality(fresh_ids)>0;
    PERFORM __CONTROL__.lock_custom_import_build(b.build_id);
    RETURN QUERY SELECT bf.root_record_id,bf.family_revision_id,f.root_revision_id,f.entity_binding_id,bf.attached_child_count,
        bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id,bf.complete_at IS NOT NULL
        FROM __CANDIDATE__.custom_import_build_family bf JOIN __CANDIDATE__.custom_import_family_revision f USING(family_revision_id)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.copy_custom_import_build_retained_roots_page(
    p_build_id bigint,p_execution_id bigint,p_fence bigint,p_token_sha256 bytea,
    p_root_ids bigint[],p_base_family_ids bigint[],p_base_root_ids bigint[],p_entity_ids bigint[],
    p_family_hashes bytea[],p_child_counts bigint[],
    p_context_root_ids bigint[],p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]
) RETURNS TABLE(root_record_id bigint,family_revision_id bigint,root_revision_id bigint,entity_binding_id bigint,
    attached_child_count bigint,last_child_collection_slot smallint,last_child_key_sha256 bytea,
    last_input_child_revision_id bigint,complete boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; build_stream __CONTROL__.custom_import_build_stream;
    owner_name name; n integer; context_n integer; work_rows bigint; work_bytes bigint; model_bytes bigint; invalid_page boolean;
    fresh_ids bigint[]; family_ids bigint[]; revision_ids bigint[]; pack_ids bigint[]; inserted_revision_ids bigint[];
    root_stream smallint; stream_n bigint; fresh_context_n bigint; producer record; root_pages integer[];
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'graph_roots_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF ROW(b.execution_id,b.producing_fence,b.producing_token_sha256)
        IS DISTINCT FROM ROW(p_execution_id,p_fence,p_token_sha256)
        OR b.phase<>'graph' OR b.plan_complete_at IS NULL OR b.generation_id IS NOT NULL
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=b.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'graph_roots_authority_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype::integer & 4)<>0
        AND t.tgrelid IN ('__CANDIDATE__.custom_import_root_record'::regclass,'__CANDIDATE__.custom_import_entity_binding'::regclass,
            '__CANDIDATE__.custom_import_pack'::regclass,'__CANDIDATE__.custom_import_root_revision'::regclass,
            '__CANDIDATE__.custom_import_family_revision'::regclass,'__CANDIDATE__.custom_import_build_occurrence'::regclass,
            '__CANDIDATE__.custom_import_root_scalar'::regclass,'__CANDIDATE__.custom_import_build_candidate_context'::regclass)) THEN
        RAISE EXCEPTION 'graph_roots_requires_shared_set_boundary'; END IF;
    n:=cardinality(p_root_ids); context_n:=cardinality(p_profile_slots);
    IF num_nonnulls(p_root_ids,p_base_family_ids,p_base_root_ids,p_entity_ids,p_family_hashes,p_child_counts,
        p_context_root_ids,p_profile_slots,p_context_keys,p_page_sizes)<>10
        OR n NOT BETWEEN 1 AND 100000 OR context_n NOT BETWEEN 0 AND 100000
        OR cardinality(p_page_sizes) NOT BETWEEN 1 AND 100000 OR array_ndims(p_page_sizes) IS DISTINCT FROM 1
        OR array_lower(p_page_sizes,1) IS DISTINCT FROM 1
        OR EXISTS(SELECT 1 FROM unnest(p_page_sizes) size WHERE size IS NULL OR size NOT BETWEEN 1 AND 256)
        OR (SELECT sum(size::bigint) FROM unnest(p_page_sizes) size) IS DISTINCT FROM n::bigint
        OR NOT n=ALL(ARRAY[cardinality(p_base_family_ids),cardinality(p_base_root_ids),cardinality(p_entity_ids),
            cardinality(p_family_hashes),cardinality(p_child_counts)])
        OR NOT context_n=ALL(ARRAY[cardinality(p_context_root_ids),cardinality(p_context_keys)])
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_ndims(p_root_ids),array_ndims(p_base_family_ids),array_ndims(p_base_root_ids),
            array_ndims(p_entity_ids),array_ndims(p_family_hashes),array_ndims(p_child_counts),array_ndims(p_context_root_ids),
            array_ndims(p_profile_slots),array_ndims(p_context_keys)]) d WHERE d>1)
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_lower(p_root_ids,1),array_lower(p_base_family_ids,1),array_lower(p_base_root_ids,1),
            array_lower(p_entity_ids,1),array_lower(p_family_hashes,1),array_lower(p_child_counts,1),array_lower(p_context_root_ids,1),
            array_lower(p_profile_slots,1),array_lower(p_context_keys,1)]) d WHERE d<>1)
        OR p_root_ids IS DISTINCT FROM (SELECT array_agg(DISTINCT id ORDER BY id) FROM unnest(p_root_ids) i(id))
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_base_family_ids,p_base_root_ids,p_entity_ids,p_family_hashes,p_child_counts)
            i(root_id,base_family_id,base_root_id,entity_id,family_hash,child_count)
            WHERE root_id IS NULL OR root_id<=0 OR base_family_id IS NULL OR base_family_id<=0
                OR base_root_id IS NULL OR base_root_id<=0 OR entity_id IS NULL OR entity_id<=0
                OR family_hash IS NULL OR octet_length(family_hash)<>32 OR child_count IS NULL OR child_count<0)
        OR EXISTS(SELECT 1 FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
            WHERE root_id IS NULL OR NOT root_id=ANY(p_root_ids) OR slot IS NULL OR slot<=0
                OR canonical IS NULL OR octet_length(canonical) NOT BETWEEN 1 AND 8192)
        OR (SELECT count(DISTINCT (root_id,slot)) FROM unnest(p_context_root_ids,p_profile_slots) c(root_id,slot))<>context_n THEN
        RAISE EXCEPTION 'graph_roots_bounds'; END IF;
    SELECT array_agg(page::integer ORDER BY page,position) INTO root_pages
        FROM unnest(p_page_sizes) WITH ORDINALITY i(size,page) CROSS JOIN LATERAL generate_series(1,i.size) position;
    IF 4::bigint*n+context_n>100000 THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    PERFORM 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_base_family_ids,p_base_root_ids,p_entity_ids,p_family_hashes,p_child_counts)
        i(root_id,base_family_id,base_root_id,entity_id,family_hash,child_count)
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __BASE__.custom_import_generation_family gf ON gf.generation_id=b.base_generation_id
            AND gf.dataset_id=b.dataset_id AND gf.schema_revision_id=b.schema_revision_id
            AND gf.root_record_id=i.root_id AND gf.family_revision_id=i.base_family_id
        LEFT JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=gf.generation_id
            AND seal.dataset_id=gf.dataset_id AND seal.definition_revision_id=gf.definition_revision_id
            AND seal.schema_revision_id=gf.schema_revision_id
        LEFT JOIN __BASE__.custom_import_family_revision old ON old.family_revision_id=gf.family_revision_id
        LEFT JOIN __BASE__.custom_import_root_revision r ON r.root_revision_id=old.root_revision_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=old.entity_binding_id AND e.dataset_id=b.dataset_id
        WHERE ROW(bf.selection_kind,bf.base_family_revision_id,bf.root_key_sha256)
                IS DISTINCT FROM ROW('retained'::text,i.base_family_id,k.logical_key_sha256)
            OR seal.generation_id IS NULL OR e.entity_binding_id IS NULL
            OR ROW(old.dataset_id,old.schema_revision_id,old.root_record_id,old.root_revision_id,
                old.entity_binding_id,old.family_sha256,old.child_count) IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,
                    i.root_id,i.base_root_id,i.entity_id,i.family_hash,i.child_count)
            OR ROW(r.dataset_id,r.schema_revision_id,r.root_record_id) IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id)) THEN
        RAISE EXCEPTION 'graph_roots_base_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_base_root_ids) i(root_id,base_root_id)
        JOIN __BASE__.custom_import_root_scalar s ON s.root_revision_id=i.base_root_id
        LEFT JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.field_slot=s.field_slot AND f.collection_slot=0 AND f.projection_slot>0
        WHERE ROW(s.dataset_id,s.schema_revision_id,s.root_record_id,s.field_collection_slot,s.projection_slot,s.field_type)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,0::smallint,f.projection_slot,f.field_type)
            OR f.field_slot IS NULL) THEN RAISE EXCEPTION 'graph_roots_base_scalar_mismatch'; END IF;
    IF EXISTS(WITH expected AS (SELECT i.root_id,p.profile_slot FROM unnest(p_root_ids) i(root_id)
        CROSS JOIN __CONTROL__.custom_import_selection_profile p WHERE p.definition_revision_id=b.definition_revision_id
            AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id
            AND coalesce(p.context_collection_slot,0)=0), supplied AS (
        SELECT * FROM unnest(p_context_root_ids,p_profile_slots) c(root_id,profile_slot))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
        (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_context_mismatch'; END IF;
    SELECT min(ss.stream_slot),count(*) INTO root_stream,stream_n FROM __CONTROL__.custom_import_source_stream ss
        WHERE ss.definition_revision_id=b.definition_revision_id AND ss.dataset_id=b.dataset_id
            AND ss.schema_revision_id=b.schema_revision_id AND ss.record_kind='root' AND ss.collection_slot IS NULL;
    IF stream_n<>1 THEN RAISE EXCEPTION 'graph_roots_stream_mismatch'; END IF;
    SELECT * INTO build_stream FROM __CONTROL__.custom_import_build_stream bs
        WHERE bs.build_id=b.build_id AND bs.stream_slot=root_stream FOR UPDATE;
    IF build_stream.replay_verified_at IS NULL THEN RAISE EXCEPTION 'graph_roots_stream_mismatch'; END IF;
    SELECT coalesce(array_agg(bf.root_record_id ORDER BY bf.root_record_id),'{}'::bigint[]) INTO fresh_ids
        FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids)
            AND bf.family_revision_id IS NULL;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(fresh_ids) AND (bf.attached_child_count<>0 OR bf.complete_at IS NOT NULL
            OR num_nonnulls(bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id)<>0))
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_base_family_ids,p_base_root_ids) i(root_id,base_family_id,base_root_id)
            JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id
                AND o.base_family_revision_id=i.base_family_id AND o.base_root_revision_id=i.base_root_id
            WHERE i.root_id=ANY(fresh_ids)) THEN RAISE EXCEPTION 'graph_roots_uncommitted_work'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_entity_ids,p_family_hashes,p_child_counts) i(root_id,entity_id,family_hash,child_count)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=bf.family_revision_id
        WHERE NOT i.root_id=ANY(fresh_ids) AND (ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.entity_binding_id,
            f.family_sha256,f.child_count,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,i.entity_id,i.family_hash,i.child_count,
                b.execution_id,b.producing_fence,b.producing_token_sha256)
            OR bf.attached_child_count>i.child_count OR (bf.complete_at IS NOT NULL AND bf.attached_child_count<>i.child_count)
            OR (i.child_count=0 AND bf.complete_at IS NULL))) THEN RAISE EXCEPTION 'graph_roots_retry_mismatch'; END IF;
    SELECT array_agg(coalesce(bf.family_revision_id,
            nextval('__CONTROL__.custom_import_family_revision_family_revision_id_seq'::regclass)) ORDER BY i.ordinality),
        array_agg(CASE WHEN i.root_id=ANY(fresh_ids) THEN nextval('__CONTROL__.custom_import_root_revision_root_revision_id_seq'::regclass)
            ELSE f.root_revision_id END ORDER BY i.ordinality),
        array_agg(CASE WHEN i.root_id=ANY(fresh_ids) THEN nextval('__CONTROL__.custom_import_pack_pack_id_seq'::regclass)
            ELSE r.pack_id END ORDER BY i.ordinality) INTO family_ids,revision_ids,pack_ids
        FROM unnest(p_root_ids) WITH ORDINALITY i(root_id,ordinality)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=bf.family_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id;
    WITH costs AS (
        SELECT i.page,6::bigint cost,0 context_count,octet_length(r.canonical_payload)+octet_length(k.canonical_logical_key) work,
            172::bigint+octet_length(r.canonical_payload) model
        FROM unnest(p_base_root_ids,p_root_ids,root_pages) i(base_root_id,root_id,page)
        JOIN __BASE__.custom_import_root_revision r ON r.root_revision_id=i.base_root_id
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id
        UNION ALL SELECT i.page,0,0,
            octet_length(k.canonical_logical_key)+64+octet_length(e.adapter_id)+octet_length(e.canonical_value)+32,
            octet_length(k.canonical_logical_key)+64+octet_length(e.adapter_id)+octet_length(e.canonical_value)+32
        FROM unnest(p_root_ids,p_entity_ids,root_pages) i(root_id,entity_id,page)
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=i.entity_id AND e.dataset_id=b.dataset_id
        UNION ALL SELECT i.page,1,0,octet_length((to_jsonb(rs)||jsonb_build_object('root_revision_id',i.revision_id))::text),
            octet_length(rs.field_type)+octet_length(rs.value_state)+coalesce(octet_length(rs.string_value),0)
        FROM unnest(p_base_root_ids,revision_ids,root_pages) i(base_root_id,revision_id,page)
        JOIN __BASE__.custom_import_root_scalar rs ON rs.root_revision_id=i.base_root_id
        UNION ALL SELECT i.page,1,1,octet_length(c.canonical),octet_length(c.canonical)+32
        FROM unnest(p_context_root_ids,p_context_keys) c(root_id,canonical)
        JOIN unnest(p_root_ids,root_pages) i(root_id,page) USING(root_id)
    ), pages AS (SELECT page,sum(cost) cost,sum(context_count) context_count,sum(work) work,sum(model) model
        FROM costs GROUP BY page)
    SELECT coalesce(sum(cost),0),coalesce(sum(work),0),coalesce(sum(model),0),
        coalesce(bool_or(cost>b.page_row_limit OR context_count>256 OR greatest(work,model)>b.page_byte_limit),false)
        INTO work_rows,work_bytes,model_bytes,invalid_page FROM pages;
    IF invalid_page OR work_rows>100000 OR greatest(work_bytes,model_bytes)>268435456 THEN
        RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_base_family_ids,p_base_root_ids,revision_ids,pack_ids)
        i(root_id,base_family_id,base_root_id,revision_id,pack_id)
        JOIN __BASE__.custom_import_root_revision old ON old.root_revision_id=i.base_root_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=i.revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=i.pack_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.root_revision_id=i.revision_id
        WHERE NOT i.root_id=ANY(fresh_ids) AND (
            ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,r.source_ordinal,
                r.payload_sha256,convert_to(r.canonical_payload,'UTF8')) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,
                    b.schema_revision_id,i.root_id,i.pack_id,0::bigint,old.payload_sha256,convert_to(old.canonical_payload,'UTF8'))
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,p.stream_slot,p.capture_bundle_id,
                p.record_count,p.pack_sha256,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM ROW(b.dataset_id,
                    b.definition_revision_id,b.schema_revision_id,b.execution_id,root_stream,b.capture_bundle_id,1::bigint,
                    sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00726f6f7400','hex')
                        ||old.payload_sha256),b.producing_fence,b.producing_token_sha256)
            OR p.pack_ordinal>=build_stream.next_pack_ordinal
            OR ROW(o.origin,o.base_family_revision_id,o.base_root_revision_id,o.stream_slot,o.pack_id,o.record_kind,o.collection_slot,o.root_record_id,
                o.resolved_rejection_id,o.base_child_revision_id,o.child_revision_id,o.child_key_sha256,o.source_ordinal,
                o.raw_parent_key_sha256,o.raw_parent_key_canonical)
                IS DISTINCT FROM ROW('retained'::text,i.base_family_id,i.base_root_id,root_stream,i.pack_id,'root'::text,0::smallint,i.root_id,
                    NULL::bigint,NULL::bigint,NULL::bigint,NULL::bytea,NULL::bigint,NULL::bytea,NULL::text)
            OR (SELECT count(*) FROM (SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence x WHERE x.pack_id=i.pack_id LIMIT 2) exact_pack)<>1)) THEN
        RAISE EXCEPTION 'graph_roots_retry_copy_mismatch'; END IF;
    IF EXISTS(WITH expected AS (SELECT to_jsonb(rs)||jsonb_build_object('root_revision_id',i.revision_id) document
        FROM unnest(p_root_ids,p_base_root_ids,revision_ids) i(root_id,base_root_id,revision_id)
        JOIN __BASE__.custom_import_root_scalar rs ON rs.root_revision_id=i.base_root_id WHERE NOT i.root_id=ANY(fresh_ids)), actual AS (
        SELECT to_jsonb(rs) document FROM unnest(p_root_ids,revision_ids) i(root_id,revision_id)
        JOIN __CANDIDATE__.custom_import_root_scalar rs ON rs.root_revision_id=i.revision_id WHERE NOT i.root_id=ANY(fresh_ids))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_retry_scalar_mismatch'; END IF;
    IF EXISTS(WITH expected AS (
        SELECT i.family_id,c.slot,i.entity_id,0::smallint collection_slot,convert_to(c.canonical,'UTF8') canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8')) digest
        FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
        JOIN unnest(p_root_ids,family_ids,p_entity_ids) i(root_id,family_id,entity_id) USING(root_id)
        WHERE NOT i.root_id=ANY(fresh_ids)), actual AS (
        SELECT c.family_revision_id,c.profile_slot,c.entity_binding_id,c.context_collection_slot,
            convert_to(c.canonical_context_key,'UTF8'),c.context_key_sha256 FROM unnest(p_root_ids,family_ids) i(root_id,family_id)
        JOIN __CANDIDATE__.custom_import_build_candidate_context c ON c.build_id=b.build_id
            AND c.family_revision_id=i.family_id AND c.context_child_revision_id IS NULL WHERE NOT i.root_id=ANY(fresh_ids))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_roots_retry_context_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_root_record(root_record_id,dataset_id,key_contract_sha256,
        canonical_logical_key,logical_key_sha256,created_at)
        SELECT k.root_record_id,k.dataset_id,k.key_contract_sha256,k.canonical_logical_key,k.logical_key_sha256,k.created_at
        FROM __CONTROL__.custom_import_root_record k WHERE k.dataset_id=b.dataset_id AND k.root_record_id=ANY(fresh_ids)
        ON CONFLICT ON CONSTRAINT custom_import_root_record_pkey DO NOTHING;
    INSERT INTO __CANDIDATE__.custom_import_entity_binding(entity_binding_id,dataset_id,adapter_id,
        canonical_value,value_sha256,created_at)
        SELECT e.entity_binding_id,e.dataset_id,e.adapter_id,e.canonical_value,e.value_sha256,e.created_at
        FROM __CONTROL__.custom_import_entity_binding e WHERE e.dataset_id=b.dataset_id
            AND e.entity_binding_id IN (SELECT i.entity_id FROM unnest(p_root_ids,p_entity_ids) i(root_id,entity_id)
                WHERE i.root_id=ANY(fresh_ids))
        ON CONFLICT ON CONSTRAINT custom_import_entity_binding_pkey DO NOTHING;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_entity_ids) i(root_id,entity_id)
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_root_record copied_k ON copied_k.root_record_id=i.root_id
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=i.entity_id AND e.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_entity_binding copied_e ON copied_e.entity_binding_id=i.entity_id
        WHERE k.root_record_id IS NULL OR e.entity_binding_id IS NULL
            OR ROW(copied_k.root_record_id,copied_k.dataset_id,copied_k.key_contract_sha256,
                convert_to(copied_k.canonical_logical_key,'UTF8'),copied_k.logical_key_sha256,copied_k.created_at)
                IS DISTINCT FROM ROW(k.root_record_id,k.dataset_id,k.key_contract_sha256,
                    convert_to(k.canonical_logical_key,'UTF8'),k.logical_key_sha256,k.created_at)
            OR ROW(copied_e.entity_binding_id,copied_e.dataset_id,convert_to(copied_e.adapter_id,'UTF8'),
                convert_to(copied_e.canonical_value,'UTF8'),copied_e.value_sha256,copied_e.created_at)
                IS DISTINCT FROM ROW(e.entity_binding_id,e.dataset_id,convert_to(e.adapter_id,'UTF8'),
                    convert_to(e.canonical_value,'UTF8'),e.value_sha256,e.created_at)) THEN
        RAISE EXCEPTION 'graph_roots_identity_copy_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_pack(pack_id,execution_id,dataset_id,definition_revision_id,schema_revision_id,
        stream_slot,pack_ordinal,capture_bundle_id,record_count,pack_sha256,producing_fence,producing_token_sha256)
        SELECT i.pack_id,b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,root_stream,
            build_stream.next_pack_ordinal+(row_number() OVER (ORDER BY i.root_id)-1)::integer,b.capture_bundle_id,1,
            sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00726f6f7400','hex')||old.payload_sha256),
            b.producing_fence,b.producing_token_sha256 FROM unnest(p_root_ids,p_base_root_ids,pack_ids) i(root_id,base_root_id,pack_id)
        JOIN __BASE__.custom_import_root_revision old ON old.root_revision_id=i.base_root_id WHERE i.root_id=ANY(fresh_ids);
    IF EXISTS(SELECT 1 FROM __CONTROL__.lookup_custom_import_revision_home(
        ARRAY(SELECT i.revision_id FROM unnest(p_root_ids,revision_ids) i(root_id,revision_id)
            WHERE NOT i.root_id=ANY(fresh_ids)),'{}') h WHERE h.family_id IS DISTINCT FROM __FAMILY_ID__) THEN
        RAISE EXCEPTION 'graph_roots_retry_home_mismatch'; END IF;
    WITH inserted AS (INSERT INTO __CANDIDATE__.custom_import_root_revision(root_revision_id,dataset_id,definition_revision_id,schema_revision_id,
        root_record_id,pack_id,source_ordinal,canonical_payload,payload_sha256)
        SELECT i.revision_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.root_id,i.pack_id,0,
            old.canonical_payload,old.payload_sha256 FROM unnest(p_root_ids,p_base_root_ids,revision_ids,pack_ids)
            i(root_id,base_root_id,revision_id,pack_id)
        JOIN __BASE__.custom_import_root_revision old ON old.root_revision_id=i.base_root_id WHERE i.root_id=ANY(fresh_ids)
        RETURNING custom_import_root_revision.root_revision_id AS inserted_id)
    SELECT coalesce(array_agg(inserted_id),'{}'::bigint[]) INTO inserted_revision_ids FROM inserted;
    IF cardinality(inserted_revision_ids)>0 THEN
        PERFORM __CONTROL__.append_custom_import_revision_home(__FAMILY_ID__,inserted_revision_ids,'{}'); END IF;
    INSERT INTO __CANDIDATE__.custom_import_build_occurrence(build_id,stream_slot,pack_id,origin,base_family_revision_id,
        base_root_revision_id,record_kind,collection_slot,root_record_id,root_revision_id)
        SELECT b.build_id,root_stream,i.pack_id,'retained',i.base_family_id,i.base_root_id,'root',0,i.root_id,i.revision_id
        FROM unnest(p_root_ids,p_base_family_ids,p_base_root_ids,revision_ids,pack_ids)
            i(root_id,base_family_id,base_root_id,revision_id,pack_id) WHERE i.root_id=ANY(fresh_ids);
    INSERT INTO __CANDIDATE__.custom_import_family_revision(family_revision_id,dataset_id,schema_revision_id,root_record_id,
        root_revision_id,entity_binding_id,family_sha256,child_count,producing_execution_id,producing_fence,producing_token_sha256)
        SELECT i.family_id,b.dataset_id,b.schema_revision_id,i.root_id,i.revision_id,i.entity_id,i.family_hash,i.child_count,
            b.execution_id,b.producing_fence,b.producing_token_sha256
        FROM unnest(p_root_ids,revision_ids,family_ids,p_entity_ids,p_family_hashes,p_child_counts)
            i(root_id,revision_id,family_id,entity_id,family_hash,child_count) WHERE i.root_id=ANY(fresh_ids);
    INSERT INTO __CANDIDATE__.custom_import_root_scalar(root_revision_id,dataset_id,schema_revision_id,root_record_id,field_slot,
        field_collection_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        SELECT i.revision_id,rs.dataset_id,rs.schema_revision_id,rs.root_record_id,rs.field_slot,rs.field_collection_slot,
            rs.projection_slot,rs.field_type,rs.value_state,rs.string_value,rs.integer_value,rs.decimal_value,
            rs.boolean_value,rs.date_value,rs.timestamp_value FROM unnest(p_root_ids,p_base_root_ids,revision_ids)
            i(root_id,base_root_id,revision_id)
        JOIN __BASE__.custom_import_root_scalar rs ON rs.root_revision_id=i.base_root_id WHERE i.root_id=ANY(fresh_ids);
    INSERT INTO __CANDIDATE__.custom_import_build_candidate_context(build_id,profile_slot,entity_binding_id,family_revision_id,
        context_collection_slot,context_child_revision_id,canonical_context_key,context_key_sha256)
        SELECT b.build_id,c.slot,i.entity_id,i.family_id,0,NULL,c.canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8'))
        FROM unnest(p_context_root_ids,p_profile_slots,p_context_keys) c(root_id,slot,canonical)
        JOIN unnest(p_root_ids,family_ids,p_entity_ids) i(root_id,family_id,entity_id) USING(root_id) WHERE i.root_id=ANY(fresh_ids);
    GET DIAGNOSTICS fresh_context_n=ROW_COUNT;
    UPDATE __CONTROL__.custom_import_build_stream bs SET next_pack_ordinal=bs.next_pack_ordinal+cardinality(fresh_ids)
        WHERE bs.build_id=b.build_id AND bs.stream_slot=root_stream AND cardinality(fresh_ids)>0;
    UPDATE __CANDIDATE__.custom_import_build_family bf SET family_revision_id=i.family_id,
        complete_at=CASE WHEN i.child_count=0 THEN clock_timestamp() END
        FROM unnest(p_root_ids,family_ids,p_child_counts) i(root_id,family_id,child_count)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=i.root_id AND i.root_id=ANY(fresh_ids);
    UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=candidate_context_count+fresh_context_n,
        completed_family_count=completed_family_count+(SELECT count(*) FROM unnest(p_root_ids,p_child_counts) i(root_id,child_count)
            WHERE i.root_id=ANY(fresh_ids) AND i.child_count=0)
        WHERE build_id=b.build_id AND cardinality(fresh_ids)>0;
    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer FROM __CONTROL__.custom_import_execution e
        JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id WHERE e.execution_id=b.execution_id;
    IF producer.state IS DISTINCT FROM 'running' OR producer.fence IS DISTINCT FROM b.producing_fence
        OR producer.token_sha256 IS DISTINCT FROM b.producing_token_sha256 OR producer.expires_at IS NULL
        OR least(producer.expires_at,b.build_deadline_at)<=clock_timestamp() THEN RAISE EXCEPTION 'graph_roots_lease_lost'; END IF;
    RETURN QUERY SELECT bf.root_record_id,bf.family_revision_id,f.root_revision_id,f.entity_binding_id,bf.attached_child_count,
        bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id,bf.complete_at IS NOT NULL
        FROM __CANDIDATE__.custom_import_build_family bf JOIN __CANDIDATE__.custom_import_family_revision f USING(family_revision_id)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.append_custom_import_build_source_families_page(
    p_build_id bigint,p_execution_id bigint,p_fence bigint,p_token_sha256 bytea,
    p_root_ids bigint[],p_family_ids bigint[],p_expected_counts bigint[],p_after_slots smallint[],
    p_after_hashes bytea[],p_after_ids bigint[],p_child_root_ids bigint[],p_child_ids bigint[],
    p_scalar_child_ids bigint[],p_field_slots smallint[],p_field_types text[],p_value_states text[],
    p_string_values text[],p_integer_values bigint[],p_decimal_values numeric[],p_boolean_values boolean[],
    p_date_values date[],p_timestamp_values timestamptz[],
    p_context_child_ids bigint[],p_profile_slots smallint[],p_context_keys text[],p_page_sizes integer[]
) RETURNS TABLE(root_record_id bigint,attached_child_count bigint,complete boolean,
    last_child_collection_slot smallint,last_child_key_sha256 bytea,last_input_child_revision_id bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; owner_name name;
    root_n integer; n integer; scalar_n integer; context_n integer; work_bytes bigint; model_bytes bigint;
    group_counts bigint[]; final_slots smallint[]; final_hashes bytea[]; final_ids bigint[]; exhausted boolean[];
    invalid_prefix boolean; completed_n bigint; definition_streams jsonb; child_pages integer[];
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'graph_children_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF ROW(b.execution_id,b.producing_fence,b.producing_token_sha256)
        IS DISTINCT FROM ROW(p_execution_id,p_fence,p_token_sha256)
        OR b.phase<>'graph' OR b.plan_complete_at IS NULL OR b.generation_id IS NOT NULL
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=b.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'graph_children_authority_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype::integer & 4)<>0
        AND t.tgrelid IN ('__CANDIDATE__.custom_import_family_child'::regclass,'__CANDIDATE__.custom_import_child_scalar'::regclass,
            '__CANDIDATE__.custom_import_build_candidate_context'::regclass)) THEN
        RAISE EXCEPTION 'graph_children_requires_shared_set_boundary'; END IF;
    root_n:=cardinality(p_root_ids); n:=cardinality(p_child_ids);
    scalar_n:=cardinality(p_field_slots); context_n:=cardinality(p_profile_slots);
    IF num_nonnulls(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_hashes,p_after_ids,
        p_child_root_ids,p_child_ids,p_scalar_child_ids,p_field_slots,p_field_types,p_value_states,p_string_values,
        p_integer_values,p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values,
        p_context_child_ids,p_profile_slots,p_context_keys,p_page_sizes)<>22
        OR root_n NOT BETWEEN 1 AND 100000 OR (n=0 AND (root_n>256 OR root_n>b.page_row_limit))
        OR n NOT BETWEEN 0 AND 100000
        OR scalar_n NOT BETWEEN 0 AND 100000 OR context_n NOT BETWEEN 0 AND 100000
        OR cardinality(p_page_sizes) NOT BETWEEN 1 AND 100000 OR array_ndims(p_page_sizes) IS DISTINCT FROM 1
        OR array_lower(p_page_sizes,1) IS DISTINCT FROM 1
        OR EXISTS(SELECT 1 FROM unnest(p_page_sizes) size WHERE size IS NULL OR size NOT BETWEEN 0 AND 256
            OR size>b.page_row_limit OR (size=0 AND n<>0))
        OR (SELECT sum(size::bigint) FROM unnest(p_page_sizes) size) IS DISTINCT FROM n::bigint
        OR NOT root_n=ALL(ARRAY[cardinality(p_family_ids),cardinality(p_expected_counts),cardinality(p_after_slots),
            cardinality(p_after_hashes),cardinality(p_after_ids)]) OR cardinality(p_child_root_ids)<>n
        OR NOT scalar_n=ALL(ARRAY[cardinality(p_scalar_child_ids),cardinality(p_field_types),cardinality(p_value_states),
            cardinality(p_string_values),cardinality(p_integer_values),cardinality(p_decimal_values),
            cardinality(p_boolean_values),cardinality(p_date_values),cardinality(p_timestamp_values)])
        OR NOT context_n=ALL(ARRAY[cardinality(p_context_child_ids),cardinality(p_context_keys)])
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_ndims(p_root_ids),array_ndims(p_family_ids),array_ndims(p_expected_counts),
            array_ndims(p_after_slots),array_ndims(p_after_hashes),array_ndims(p_after_ids),array_ndims(p_child_root_ids),
            array_ndims(p_child_ids),array_ndims(p_scalar_child_ids),array_ndims(p_field_slots),array_ndims(p_field_types),
            array_ndims(p_value_states),array_ndims(p_string_values),array_ndims(p_integer_values),array_ndims(p_decimal_values),
            array_ndims(p_boolean_values),array_ndims(p_date_values),array_ndims(p_timestamp_values),array_ndims(p_context_child_ids),
            array_ndims(p_profile_slots),array_ndims(p_context_keys)]) d WHERE d>1)
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_lower(p_root_ids,1),array_lower(p_family_ids,1),array_lower(p_expected_counts,1),
            array_lower(p_after_slots,1),array_lower(p_after_hashes,1),array_lower(p_after_ids,1),array_lower(p_child_root_ids,1),
            array_lower(p_child_ids,1),array_lower(p_scalar_child_ids,1),array_lower(p_field_slots,1),array_lower(p_field_types,1),
            array_lower(p_value_states,1),array_lower(p_string_values,1),array_lower(p_integer_values,1),array_lower(p_decimal_values,1),
            array_lower(p_boolean_values,1),array_lower(p_date_values,1),array_lower(p_timestamp_values,1),array_lower(p_context_child_ids,1),
            array_lower(p_profile_slots,1),array_lower(p_context_keys,1)]) d WHERE d<>1)
        OR p_root_ids IS DISTINCT FROM (SELECT array_agg(DISTINCT id ORDER BY id) FROM unnest(p_root_ids) i(id))
        OR (SELECT count(DISTINCT id) FROM unnest(p_child_ids) i(id))<>n
        OR (SELECT count(DISTINCT (id,slot)) FROM unnest(p_scalar_child_ids,p_field_slots) s(id,slot))<>scalar_n
        OR (SELECT count(DISTINCT (id,slot)) FROM unnest(p_context_child_ids,p_profile_slots) c(id,slot))<>context_n
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_hashes,p_after_ids)
            i(root_id,family_id,expected_count,after_slot,after_hash,after_id)
            WHERE root_id IS NULL OR root_id<=0 OR family_id IS NULL OR family_id<=0 OR expected_count IS NULL OR expected_count<0
                OR num_nonnulls(after_slot,after_hash,after_id) NOT IN (0,3)
                OR (after_slot IS NOT NULL AND (after_slot<=0 OR after_id<=0 OR octet_length(after_hash)<>32)))
        OR EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
            WHERE root_id IS NULL OR NOT root_id=ANY(p_root_ids) OR child_id IS NULL OR child_id<=0)
        OR (n>0 AND EXISTS(SELECT 1 FROM unnest(p_root_ids) i(root_id)
            WHERE NOT root_id=ANY(p_child_root_ids))) THEN
        RAISE EXCEPTION 'graph_children_bounds'; END IF;
    SELECT coalesce(array_agg(page::integer ORDER BY page,position),'{}'::integer[]) INTO child_pages
        FROM unnest(p_page_sizes) WITH ORDINALITY i(size,page) CROSS JOIN LATERAL generate_series(1,i.size) position;
    IF 3::bigint*n+scalar_n+context_n+root_n>100000 OR EXISTS(
        WITH costs AS (
            SELECT page,3::bigint cost FROM unnest(child_pages) page
            UNION ALL SELECT page,1 FROM unnest(p_child_root_ids,child_pages) i(root_id,page) GROUP BY page,root_id
            UNION ALL SELECT c.page,1 FROM unnest(p_scalar_child_ids) s(child_id)
                JOIN unnest(p_child_ids,child_pages) c(child_id,page) USING(child_id)
            UNION ALL SELECT c.page,1 FROM unnest(p_context_child_ids) s(child_id)
                JOIN unnest(p_child_ids,child_pages) c(child_id,page) USING(child_id))
        SELECT 1 FROM costs GROUP BY page HAVING sum(cost)>b.page_row_limit)
        OR EXISTS(SELECT 1 FROM (SELECT root_id,lag(root_id) OVER(PARTITION BY page ORDER BY position) prior
            FROM unnest(p_child_root_ids,child_pages) WITH ORDINALITY c(root_id,page,position)) ordered
            WHERE prior>root_id) THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    PERFORM 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_family_ids) i(root_id,family_id)
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.occurrence_id=bf.source_root_occurrence_id
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=f.entity_binding_id AND e.dataset_id=b.dataset_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        LEFT JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
            AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id AND ss.stream_slot=o.stream_slot
            AND ss.record_kind='root' AND ss.collection_slot IS NULL
        LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=ss.stream_slot
        WHERE ROW(bf.selection_kind,bf.family_revision_id) IS DISTINCT FROM ROW('source'::text,i.family_id)
            OR e.entity_binding_id IS NULL
            OR bf.root_key_sha256 IS DISTINCT FROM k.logical_key_sha256
            OR ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.root_revision_id)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.root_id,o.root_revision_id)
            OR ROW(r.pack_id,r.source_ordinal) IS DISTINCT FROM ROW(o.pack_id,o.source_ordinal)
            OR ROW(o.origin,o.record_kind,o.root_record_id,o.collection_slot,o.resolved_rejection_id)
                IS DISTINCT FROM ROW('source'::text,'root'::text,i.root_id,0::smallint,NULL::bigint)
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,p.capture_bundle_id,
                p.producing_fence,p.producing_token_sha256,p.stream_slot) IS DISTINCT FROM ROW(b.dataset_id,
                    b.definition_revision_id,b.schema_revision_id,b.execution_id,b.capture_bundle_id,
                    b.producing_fence,b.producing_token_sha256,ss.stream_slot)
            OR bs.replay_verified_at IS NULL OR p.pack_ordinal>=bs.next_pack_ordinal
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,b.execution_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'graph_children_family_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_expected_counts,p_after_slots,p_after_hashes,p_after_ids)
        i(root_id,expected_count,after_slot,after_hash,after_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CONTROL__.custom_import_child_collection prior ON prior.dataset_id=b.dataset_id AND prior.schema_revision_id=b.schema_revision_id
            AND prior.collection_slot=i.after_slot
        WHERE ROW(bf.attached_child_count,bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id)
                IS DISTINCT FROM ROW(i.expected_count,i.after_slot,i.after_hash,i.after_id)
            OR (i.after_slot IS NOT NULL AND prior.collection_slot IS NULL)) THEN
        RAISE EXCEPTION 'graph_children_progress_conflict' USING ERRCODE='40001'; END IF;
    SELECT canonical_definition::jsonb->'streams' INTO definition_streams
        FROM __CONTROL__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id
            AND dataset_id=b.dataset_id AND schema_revision_id=b.schema_revision_id;
    IF definition_streams IS NULL THEN RAISE EXCEPTION 'graph_children_definition_mismatch'; END IF;
    WITH requested AS (
        SELECT i.*,f.child_count,prior.collection_name prior_name,coalesce(sup.ids,'{}'::bigint[]) supplied_ids,
            coalesce(cardinality(sup.ids),0) take
        FROM unnest(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_hashes,p_after_ids)
            i(root_id,family_id,expected_count,after_slot,after_hash,after_id)
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id
        LEFT JOIN __CONTROL__.custom_import_child_collection prior ON prior.dataset_id=b.dataset_id AND prior.schema_revision_id=b.schema_revision_id
            AND prior.collection_slot=i.after_slot
        LEFT JOIN (SELECT root_id,array_agg(child_id ORDER BY ordinality) ids
            FROM unnest(p_child_root_ids,p_child_ids) WITH ORDINALITY c(root_id,child_id,ordinality) GROUP BY root_id) sup USING(root_id)
    ), checked AS (
        SELECT r.*,coalesce(e.ids,'{}'::bigint[]) expected_ids,e.slots,e.hashes
        FROM requested r CROSS JOIN LATERAL (
            SELECT array_agg(x.child_id ORDER BY x.collection_name COLLATE "C",x.child_hash,x.child_id) ids,
                array_agg(x.collection_slot ORDER BY x.collection_name COLLATE "C",x.child_hash,x.child_id) slots,
                array_agg(x.child_hash ORDER BY x.collection_name COLLATE "C",x.child_hash,x.child_id) hashes
            FROM (
                SELECT cc.collection_name,cc.collection_slot,o.child_id,o.child_hash
                FROM __CONTROL__.custom_import_child_collection cc
                JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
                    AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id
                    AND ss.record_kind='child' AND ss.collection_slot=cc.collection_slot
                JOIN LATERAL (SELECT c.child_revision_id child_id,c.child_key_sha256 child_hash
                    FROM __CANDIDATE__.custom_import_build_occurrence c WHERE c.build_id=b.build_id AND c.origin='source'
                        AND c.root_record_id=r.root_id AND c.collection_slot=cc.collection_slot AND c.stream_slot=ss.stream_slot
                        AND c.child_revision_id IS NOT NULL AND c.resolved_rejection_id IS NULL
                        AND (r.after_slot IS NULL OR (cc.collection_name COLLATE "C",c.child_key_sha256,c.child_revision_id)>
                            (r.prior_name COLLATE "C",r.after_hash,r.after_id))
                        AND (coalesce(definition_streams->(c.stream_slot-1)->>'duplicate_policy','reject')<>'collapse_identical'
                            OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence later
                                WHERE later.build_id=c.build_id AND later.origin='source' AND later.stream_slot=c.stream_slot
                                    AND later.root_record_id=c.root_record_id AND later.collection_slot=c.collection_slot
                                    AND later.raw_parent_key_sha256=c.raw_parent_key_sha256 AND later.child_key_sha256=c.child_key_sha256
                                    AND later.child_revision_id IS NOT NULL AND later.source_ordinal>c.source_ordinal))
                    ORDER BY c.child_key_sha256,c.child_revision_id LIMIT r.take+1) o ON true
                WHERE cc.dataset_id=b.dataset_id AND cc.schema_revision_id=b.schema_revision_id
                    AND (r.after_slot IS NULL OR cc.collection_name COLLATE "C">=r.prior_name COLLATE "C")
                ORDER BY cc.collection_name COLLATE "C",o.child_hash,o.child_id LIMIT r.take+1
            ) x
        ) e
    ) SELECT bool_or(supplied_ids IS DISTINCT FROM coalesce(expected_ids[1:take],'{}'::bigint[])
            OR expected_count+take>child_count OR (cardinality(expected_ids)=take AND expected_count+take<>child_count)
            OR (take=0 AND cardinality(expected_ids)>0)),
        array_agg(take::bigint ORDER BY root_id),array_agg(coalesce(slots[take],after_slot) ORDER BY root_id),
        array_agg(coalesce(hashes[take],after_hash) ORDER BY root_id),array_agg(coalesce(expected_ids[take],after_id) ORDER BY root_id),
        array_agg(cardinality(expected_ids)=take ORDER BY root_id)
        INTO invalid_prefix,group_counts,final_slots,final_hashes,final_ids,exhausted FROM checked;
    IF invalid_prefix OR cardinality(group_counts)<>root_n THEN RAISE EXCEPTION 'graph_children_prefix_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=c.pack_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.child_revision_id=i.child_id
        LEFT JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
            AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id AND ss.stream_slot=p.stream_slot
            AND ss.record_kind='child' AND ss.collection_slot=c.collection_slot
        LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=ss.stream_slot
        WHERE ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.parent_key_sha256,convert_to(c.canonical_parent_key,'UTF8'))
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.root_id,k.logical_key_sha256,convert_to(k.canonical_logical_key,'UTF8'))
            OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
                IS DISTINCT FROM ROW(b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
            OR ROW(o.origin,o.record_kind,o.root_record_id,o.pack_id,o.stream_slot,o.collection_slot,o.child_key_sha256,o.source_ordinal,o.resolved_rejection_id)
                IS DISTINCT FROM ROW('source'::text,'child'::text,i.root_id,c.pack_id,ss.stream_slot,c.collection_slot,c.child_key_sha256,c.source_ordinal,NULL::bigint)
            OR bs.replay_verified_at IS NULL OR p.pack_ordinal>=bs.next_pack_ordinal) THEN
        RAISE EXCEPTION 'graph_children_identity_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_scalar_child_ids,p_field_slots,p_field_types,p_value_states,p_string_values,
        p_integer_values,p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values)
        s(child_id,field_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=s.child_id
        LEFT JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=c.collection_slot AND f.field_slot=s.field_slot
        WHERE s.child_id IS NULL OR NOT s.child_id=ANY(p_child_ids) OR f.field_slot IS NULL OR f.projection_slot<=0
            OR s.field_type IS DISTINCT FROM f.field_type OR s.value_state IS NULL OR s.value_state NOT IN ('value','null')
            OR (s.value_state='null' AND NOT f.is_nullable) OR octet_length(s.string_value)>4096
            OR s.decimal_value::text IN ('NaN','Infinity','-Infinity')
            OR s.decimal_value IS DISTINCT FROM s.decimal_value::numeric(30,12)) THEN RAISE EXCEPTION 'graph_children_scalar_mismatch'; END IF;
    IF EXISTS(WITH expected AS (
        SELECT c.child_revision_id child_id,f.field_slot,e.value->'value'->>'state' value_state
        FROM unnest(p_child_ids) i(child_id) JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=c.collection_slot AND f.projection_slot>0
        JOIN LATERAL json_array_elements(replace(c.canonical_payload,chr(92)||'u0000',chr(92)||'u0001')::json->'fields') e(value)
            ON e.value->>'field'=f.field_name WHERE e.value->'value'->>'state' IS DISTINCT FROM 'missing'
    ), supplied AS (SELECT * FROM unnest(p_scalar_child_ids,p_field_slots,p_value_states) s(child_id,field_slot,value_state))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
        (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_children_scalar_presence'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_context_child_ids,p_profile_slots,p_context_keys) c(child_id,slot,canonical)
        WHERE c.child_id IS NULL OR NOT c.child_id=ANY(p_child_ids) OR c.slot IS NULL OR c.canonical IS NULL
            OR octet_length(c.canonical) NOT BETWEEN 1 AND 8192)
        OR EXISTS(WITH expected AS (SELECT c.child_revision_id child_id,p.profile_slot
            FROM unnest(p_child_ids) i(child_id) JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
            JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=b.definition_revision_id
                AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id
                AND p.context_collection_slot=c.collection_slot), supplied AS (
            SELECT * FROM unnest(p_context_child_ids,p_profile_slots) c(child_id,profile_slot))
            (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
            (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_children_context_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_child_scalar s WHERE s.child_revision_id=ANY(p_child_ids))
        OR EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
            JOIN unnest(p_root_ids,p_family_ids) f(root_id,family_id) USING(root_id)
            JOIN __CANDIDATE__.custom_import_family_child fc ON fc.family_revision_id=f.family_id AND fc.child_revision_id=i.child_id)
        OR EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
            AND c.family_revision_id=ANY(p_family_ids) AND c.context_child_revision_id=ANY(p_child_ids)) THEN
        RAISE EXCEPTION 'graph_children_uncommitted_work'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_family_child(family_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,child_revision_id)
        SELECT f.family_id,b.dataset_id,b.schema_revision_id,i.root_id,c.collection_slot,i.child_id
        FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
        JOIN unnest(p_root_ids,p_family_ids) f(root_id,family_id) USING(root_id)
        JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id;
    INSERT INTO __CANDIDATE__.custom_import_child_scalar(child_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,
        field_slot,field_collection_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,
        boolean_value,date_value,timestamp_value)
        SELECT s.child_id,b.dataset_id,b.schema_revision_id,c.root_record_id,c.collection_slot,s.field_slot,c.collection_slot,f.projection_slot,
            s.field_type,s.value_state,s.string_value,s.integer_value,s.decimal_value,s.boolean_value,s.date_value,s.timestamp_value
        FROM unnest(p_scalar_child_ids,p_field_slots,p_field_types,p_value_states,p_string_values,p_integer_values,
            p_decimal_values,p_boolean_values,p_date_values,p_timestamp_values)
            s(child_id,field_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=s.child_id
        JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.collection_slot=c.collection_slot AND f.field_slot=s.field_slot;
    INSERT INTO __CANDIDATE__.custom_import_build_candidate_context(build_id,profile_slot,entity_binding_id,family_revision_id,
        context_collection_slot,context_child_revision_id,canonical_context_key,context_key_sha256)
        SELECT b.build_id,c.slot,f.entity_binding_id,f.family_revision_id,child.collection_slot,c.child_id,c.canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8'))
        FROM unnest(p_context_child_ids,p_profile_slots,p_context_keys) c(child_id,slot,canonical)
        JOIN __CANDIDATE__.custom_import_child_revision child ON child.child_revision_id=c.child_id
        JOIN unnest(p_root_ids,p_family_ids) i(root_id,family_id) ON i.root_id=child.root_record_id
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id;
    WITH costs AS (
        SELECT i.page,octet_length(c.canonical_payload)+octet_length(c.canonical_parent_key)+octet_length(c.canonical_child_key)
            +octet_length(k.canonical_logical_key)+coalesce(octet_length(o.raw_parent_key_canonical),0) work,0::bigint model
        FROM unnest(p_child_ids,child_pages) i(child_id,page)
        JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=c.root_record_id
        JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.child_revision_id=i.child_id
        UNION ALL SELECT i.page,octet_length(to_jsonb(s)::text),
            octet_length(s.field_type)+octet_length(s.value_state)+coalesce(octet_length(s.string_value),0)
        FROM unnest(p_child_ids,child_pages) i(child_id,page)
        JOIN __CANDIDATE__.custom_import_child_scalar s ON s.child_revision_id=i.child_id
        UNION ALL SELECT i.page,octet_length(c.canonical),octet_length(c.canonical)+32
        FROM unnest(p_context_child_ids,p_context_keys) c(child_id,canonical)
        JOIN unnest(p_child_ids,child_pages) i(child_id,page) USING(child_id)
    ), pages AS (SELECT page,sum(work) work,sum(model) model FROM costs GROUP BY page)
    SELECT coalesce(sum(work),0),coalesce(sum(model),0),
        coalesce(bool_or(greatest(work,model)>b.page_byte_limit),false)
        INTO work_bytes,model_bytes,invalid_prefix FROM pages;
    IF invalid_prefix OR greatest(work_bytes,model_bytes)>268435456 THEN
        RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    SELECT count(*) INTO completed_n FROM unnest(p_root_ids,exhausted) i(root_id,done)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        WHERE i.done AND bf.complete_at IS NULL;
    UPDATE __CANDIDATE__.custom_import_build_family bf SET attached_child_count=i.expected_count+i.taken,
        last_child_collection_slot=i.final_slot,last_child_key_sha256=i.final_hash,last_input_child_revision_id=i.final_id,
        complete_at=CASE WHEN i.done THEN clock_timestamp() END
        FROM unnest(p_root_ids,p_expected_counts,group_counts,final_slots,final_hashes,final_ids,exhausted)
            i(root_id,expected_count,taken,final_slot,final_hash,final_id,done)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=i.root_id AND bf.complete_at IS NULL;
    UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=candidate_context_count+context_n,
        completed_family_count=completed_family_count+completed_n
        WHERE build_id=b.build_id AND (n>0 OR completed_n>0);
    PERFORM __CONTROL__.lock_custom_import_build(b.build_id);
    RETURN QUERY SELECT bf.root_record_id,bf.attached_child_count,bf.complete_at IS NOT NULL,
        bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id
        FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids)
        ORDER BY bf.root_record_id;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.copy_custom_import_build_retained_families_page(
    p_build_id bigint,p_execution_id bigint,p_fence bigint,p_token_sha256 bytea,
    p_root_ids bigint[],p_family_ids bigint[],p_expected_counts bigint[],p_after_slots smallint[],p_after_ids bigint[],
    p_child_root_ids bigint[],p_child_ids bigint[],p_context_child_ids bigint[],p_profile_slots smallint[],p_context_keys text[],
    p_page_sizes integer[]
) RETURNS TABLE(root_record_id bigint,attached_child_count bigint,complete boolean,
    last_child_collection_slot smallint,last_child_key_sha256 bytea,last_input_child_revision_id bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; owner_name name; producer record;
    root_n integer; n integer; context_n integer; scalar_n bigint; pack_n bigint; fresh_context_n bigint; completed_n bigint;
    group_counts bigint[]; final_slots smallint[]; final_ids bigint[]; finished boolean[]; replays boolean[];
    member_slots smallint[]; member_streams smallint[]; local_ordinals bigint[]; new_ids bigint[]; inserted_revision_ids bigint[];
    pack_ids bigint[]; child_pages integer[]; invalid_prefix boolean; work_bytes bigint; model_bytes bigint;
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'graph_children_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF ROW(b.execution_id,b.producing_fence,b.producing_token_sha256)
        IS DISTINCT FROM ROW(p_execution_id,p_fence,p_token_sha256)
        OR b.phase<>'graph' OR b.plan_complete_at IS NULL OR b.generation_id IS NOT NULL
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE execution_id=b.execution_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'graph_children_authority_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype::integer & 4)<>0
        AND t.tgrelid IN ('__CANDIDATE__.custom_import_pack'::regclass,'__CANDIDATE__.custom_import_child_revision'::regclass,
            '__CANDIDATE__.custom_import_build_occurrence'::regclass,'__CANDIDATE__.custom_import_family_child'::regclass,
            '__CANDIDATE__.custom_import_child_scalar'::regclass,'__CANDIDATE__.custom_import_build_candidate_context'::regclass)) THEN
        RAISE EXCEPTION 'graph_children_requires_shared_set_boundary'; END IF;
    root_n:=cardinality(p_root_ids); n:=cardinality(p_child_ids); context_n:=cardinality(p_context_child_ids);
    IF num_nonnulls(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_ids,p_child_root_ids,p_child_ids,
        p_context_child_ids,p_profile_slots,p_context_keys,p_page_sizes)<>11 OR root_n NOT BETWEEN 1 AND 100000
        OR (n=0 AND (root_n>256 OR root_n>b.page_row_limit))
        OR n NOT BETWEEN 0 AND 100000 OR context_n NOT BETWEEN 0 AND 100000
        OR cardinality(p_page_sizes) NOT BETWEEN 1 AND 100000 OR array_ndims(p_page_sizes) IS DISTINCT FROM 1
        OR array_lower(p_page_sizes,1) IS DISTINCT FROM 1
        OR EXISTS(SELECT 1 FROM unnest(p_page_sizes) size WHERE size IS NULL OR size NOT BETWEEN 0 AND 256
            OR size>b.page_row_limit OR (size=0 AND n<>0))
        OR (SELECT sum(size::bigint) FROM unnest(p_page_sizes) size) IS DISTINCT FROM n::bigint
        OR NOT root_n=ALL(ARRAY[cardinality(p_family_ids),cardinality(p_expected_counts),cardinality(p_after_slots),cardinality(p_after_ids)])
        OR cardinality(p_child_root_ids)<>n OR NOT context_n=ALL(ARRAY[cardinality(p_profile_slots),cardinality(p_context_keys)])
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_ndims(p_root_ids),array_ndims(p_family_ids),array_ndims(p_expected_counts),
            array_ndims(p_after_slots),array_ndims(p_after_ids),array_ndims(p_child_root_ids),array_ndims(p_child_ids),
            array_ndims(p_context_child_ids),array_ndims(p_profile_slots),array_ndims(p_context_keys)]) d WHERE d>1)
        OR EXISTS(SELECT 1 FROM unnest(ARRAY[array_lower(p_root_ids,1),array_lower(p_family_ids,1),array_lower(p_expected_counts,1),
            array_lower(p_after_slots,1),array_lower(p_after_ids,1),array_lower(p_child_root_ids,1),array_lower(p_child_ids,1),
            array_lower(p_context_child_ids,1),array_lower(p_profile_slots,1),array_lower(p_context_keys,1)]) d WHERE d<>1)
        OR p_root_ids IS DISTINCT FROM (SELECT array_agg(DISTINCT id ORDER BY id) FROM unnest(p_root_ids) i(id))
        OR (SELECT count(DISTINCT id) FROM unnest(p_child_ids) i(id))<>n
        OR (SELECT count(DISTINCT (id,slot)) FROM unnest(p_context_child_ids,p_profile_slots) c(id,slot))<>context_n
        OR EXISTS(SELECT 1 FROM unnest(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_ids)
            i(root_id,family_id,expected_count,after_slot,after_id)
            WHERE root_id IS NULL OR root_id<=0 OR family_id IS NULL OR family_id<=0 OR expected_count IS NULL OR expected_count<0
                OR num_nonnulls(after_slot,after_id) NOT IN (0,2) OR (after_slot IS NOT NULL AND (after_slot<=0 OR after_id<=0)))
        OR EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
            WHERE root_id IS NULL OR NOT root_id=ANY(p_root_ids) OR child_id IS NULL OR child_id<=0)
        OR (n>0 AND EXISTS(SELECT 1 FROM unnest(p_root_ids) i(root_id)
            WHERE NOT root_id=ANY(p_child_root_ids)))
        OR EXISTS(SELECT 1 FROM unnest(p_context_child_ids,p_profile_slots,p_context_keys) c(child_id,slot,canonical)
            WHERE child_id IS NULL OR NOT child_id=ANY(p_child_ids) OR slot IS NULL OR slot<=0
                OR canonical IS NULL OR octet_length(canonical) NOT BETWEEN 1 AND 8192) THEN
        RAISE EXCEPTION 'graph_children_bounds'; END IF;
    SELECT coalesce(array_agg(page::integer ORDER BY page,position),'{}'::integer[]) INTO child_pages
        FROM unnest(p_page_sizes) WITH ORDINALITY i(size,page) CROSS JOIN LATERAL generate_series(1,i.size) position;
    IF 3::bigint*n+context_n+root_n>100000 OR EXISTS(SELECT 1 FROM (
        SELECT root_id,lag(root_id) OVER(PARTITION BY page ORDER BY position) prior
        FROM unnest(p_child_root_ids,child_pages) WITH ORDINALITY c(root_id,page,position)) ordered
        WHERE prior>root_id) THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    PERFORM 1 FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id
        AND bf.root_record_id=ANY(p_root_ids) ORDER BY bf.root_record_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_family_ids) i(root_id,family_id)
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id
        LEFT JOIN __BASE__.custom_import_generation_family gf ON gf.generation_id=b.base_generation_id
            AND gf.dataset_id=b.dataset_id AND gf.schema_revision_id=b.schema_revision_id
            AND gf.root_record_id=i.root_id AND gf.family_revision_id=bf.base_family_revision_id
        LEFT JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=gf.generation_id
            AND seal.dataset_id=gf.dataset_id AND seal.definition_revision_id=gf.definition_revision_id AND seal.schema_revision_id=gf.schema_revision_id
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=gf.family_revision_id
        LEFT JOIN __BASE__.custom_import_root_revision old ON old.root_revision_id=base.root_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision current_root ON current_root.root_revision_id=f.root_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=current_root.pack_id
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.entity_binding_id=f.entity_binding_id AND e.dataset_id=b.dataset_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
            AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id AND ss.stream_slot=p.stream_slot
            AND ss.record_kind='root' AND ss.collection_slot IS NULL
        LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=ss.stream_slot
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence ro ON ro.build_id=b.build_id AND ro.root_revision_id=current_root.root_revision_id
        WHERE ROW(bf.selection_kind,bf.family_revision_id,bf.last_child_key_sha256)
                IS DISTINCT FROM ROW('retained'::text,i.family_id,NULL::bytea) OR seal.generation_id IS NULL OR e.entity_binding_id IS NULL
            OR bf.root_key_sha256 IS DISTINCT FROM k.logical_key_sha256
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,b.execution_id,b.producing_fence,b.producing_token_sha256)
            OR ROW(base.dataset_id,base.schema_revision_id,base.root_record_id,base.entity_binding_id,base.family_sha256,base.child_count)
                IS DISTINCT FROM ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.entity_binding_id,f.family_sha256,f.child_count)
            OR ROW(old.dataset_id,old.schema_revision_id,old.root_record_id) IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id)
            OR ROW(current_root.dataset_id,current_root.definition_revision_id,current_root.schema_revision_id,current_root.root_record_id,
                current_root.payload_sha256,convert_to(current_root.canonical_payload,'UTF8')) IS DISTINCT FROM ROW(b.dataset_id,
                    b.definition_revision_id,b.schema_revision_id,i.root_id,old.payload_sha256,convert_to(old.canonical_payload,'UTF8'))
            OR current_root.source_ordinal IS DISTINCT FROM 0::bigint OR ss.stream_slot IS NULL
            OR bs.replay_verified_at IS NULL OR p.pack_ordinal>=bs.next_pack_ordinal
            OR ROW(ro.origin,ro.record_kind,ro.collection_slot,ro.root_record_id,ro.base_family_revision_id,ro.base_root_revision_id,
                ro.pack_id,ro.stream_slot,ro.resolved_rejection_id)
                IS DISTINCT FROM ROW('retained'::text,'root'::text,0::smallint,i.root_id,base.family_revision_id,old.root_revision_id,
                    p.pack_id,ss.stream_slot,NULL::bigint)
            OR ROW(p.record_count,p.pack_sha256) IS DISTINCT FROM ROW(1::bigint,
                sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00726f6f7400','hex')||old.payload_sha256))
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'graph_children_base_mismatch'; END IF;
    WITH requested AS (
        SELECT i.*,bf.base_family_revision_id,f.child_count,coalesce(sup.ids,'{}'::bigint[]) supplied_ids,
            coalesce(cardinality(sup.ids),0) take
        FROM unnest(p_root_ids,p_family_ids,p_expected_counts,p_after_slots,p_after_ids)
            i(root_id,family_id,expected_count,after_slot,after_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id
        LEFT JOIN (SELECT root_id,array_agg(child_id ORDER BY ordinality) ids
            FROM unnest(p_child_root_ids,p_child_ids) WITH ORDINALITY c(root_id,child_id,ordinality) GROUP BY root_id) sup USING(root_id)
    ), checked AS (
        SELECT r.*,coalesce(e.ids,'{}'::bigint[]) expected_ids,e.slots
        FROM requested r CROSS JOIN LATERAL (
            SELECT array_agg(x.child_revision_id ORDER BY x.collection_slot,x.child_revision_id) ids,
                array_agg(x.collection_slot ORDER BY x.collection_slot,x.child_revision_id) slots
            FROM (SELECT fc.collection_slot,fc.child_revision_id FROM __BASE__.custom_import_family_child fc
                WHERE fc.family_revision_id=r.base_family_revision_id AND (r.after_slot IS NULL
                    OR (fc.collection_slot,fc.child_revision_id)>(r.after_slot,r.after_id))
                ORDER BY fc.collection_slot,fc.child_revision_id LIMIT r.take+1) x
        ) e
    ) SELECT bool_or(supplied_ids IS DISTINCT FROM coalesce(expected_ids[1:take],'{}'::bigint[])
            OR expected_count+take>child_count OR (cardinality(expected_ids)=take AND expected_count+take<>child_count)
            OR (take=0 AND cardinality(expected_ids)>0)),
        array_agg(take::bigint ORDER BY root_id),array_agg(coalesce(slots[take],after_slot) ORDER BY root_id),
        array_agg(coalesce(expected_ids[take],after_id) ORDER BY root_id),array_agg(cardinality(expected_ids)=take ORDER BY root_id)
        INTO invalid_prefix,group_counts,final_slots,final_ids,finished FROM checked;
    IF invalid_prefix OR cardinality(group_counts)<>root_n THEN RAISE EXCEPTION 'graph_children_prefix_mismatch'; END IF;
    SELECT coalesce(array_agg(c.collection_slot ORDER BY i.ordinality),'{}'::smallint[]),
        coalesce(array_agg(ss.stream_slot ORDER BY i.ordinality),'{}'::smallint[]) INTO member_slots,member_streams
        FROM unnest(p_child_root_ids,p_child_ids) WITH ORDINALITY i(root_id,child_id,ordinality)
        LEFT JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=i.child_id AND c.dataset_id=b.dataset_id
            AND c.schema_revision_id=b.schema_revision_id AND c.root_record_id=i.root_id
        LEFT JOIN __CONTROL__.custom_import_source_stream ss ON ss.definition_revision_id=b.definition_revision_id
            AND ss.dataset_id=b.dataset_id AND ss.schema_revision_id=b.schema_revision_id AND ss.record_kind='child' AND ss.collection_slot=c.collection_slot;
    IF cardinality(member_slots)<>n OR EXISTS(SELECT 1 FROM unnest(member_slots,member_streams) m(slot,stream) WHERE slot IS NULL OR stream IS NULL)
        OR EXISTS(SELECT 1 FROM unnest(p_child_root_ids,member_slots,member_streams,child_pages) m(root_id,slot,stream,page)
            GROUP BY root_id,page HAVING count(DISTINCT slot)<>1 OR count(DISTINCT stream)<>1) THEN
        RAISE EXCEPTION 'graph_children_mixed_stream_subpage'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids,member_slots) i(root_id,child_id,slot)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __BASE__.custom_import_family_child fc ON fc.family_revision_id=bf.base_family_revision_id AND fc.child_revision_id=i.child_id
        LEFT JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        LEFT JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=i.root_id AND k.dataset_id=b.dataset_id
        LEFT JOIN __CONTROL__.custom_import_child_collection cc ON cc.dataset_id=b.dataset_id AND cc.schema_revision_id=b.schema_revision_id
            AND cc.collection_slot=i.slot
        WHERE ROW(fc.dataset_id,fc.schema_revision_id,fc.root_record_id,fc.collection_slot)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,i.slot)
            OR ROW(c.parent_key_sha256,convert_to(c.canonical_parent_key,'UTF8'))
                IS DISTINCT FROM ROW(k.logical_key_sha256,convert_to(k.canonical_logical_key,'UTF8'))
            OR cc.collection_slot IS NULL) THEN RAISE EXCEPTION 'graph_children_base_membership_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids,member_slots) i(root_id,child_id,slot)
        JOIN __BASE__.custom_import_child_scalar s ON s.child_revision_id=i.child_id
        LEFT JOIN __CONTROL__.custom_import_field f ON f.dataset_id=b.dataset_id AND f.schema_revision_id=b.schema_revision_id
            AND f.field_slot=s.field_slot AND f.collection_slot=i.slot AND f.projection_slot>0
        WHERE ROW(s.dataset_id,s.schema_revision_id,s.root_record_id,s.collection_slot,s.field_collection_slot,s.projection_slot,s.field_type)
            IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,i.slot,i.slot,f.projection_slot,f.field_type)
            OR f.field_slot IS NULL) THEN RAISE EXCEPTION 'graph_children_base_scalar_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_root_ids,p_after_slots,p_after_ids) i(root_id,after_slot,after_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        JOIN LATERAL (SELECT m.slot,m.child_id FROM unnest(p_child_root_ids,member_slots,p_child_ids)
            WITH ORDINALITY m(root_id,slot,child_id,ordinality) WHERE m.root_id=i.root_id ORDER BY m.ordinality LIMIT 1) first_member ON true
        LEFT JOIN LATERAL (SELECT fc.collection_slot,fc.child_revision_id FROM __BASE__.custom_import_family_child fc
            WHERE fc.family_revision_id=bf.base_family_revision_id
                AND (fc.collection_slot,fc.child_revision_id)<(first_member.slot,first_member.child_id)
            ORDER BY fc.collection_slot DESC,fc.child_revision_id DESC LIMIT 1) predecessor ON true
        WHERE ROW(predecessor.collection_slot,predecessor.child_revision_id) IS DISTINCT FROM ROW(i.after_slot,i.after_id)) THEN
        RAISE EXCEPTION 'graph_children_progress_conflict' USING ERRCODE='40001'; END IF;
    IF EXISTS(WITH expected AS (SELECT m.child_id,p.profile_slot FROM unnest(p_child_ids,member_slots) m(child_id,slot)
        JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=b.definition_revision_id
            AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id AND p.context_collection_slot=m.slot),
        supplied AS (SELECT * FROM unnest(p_context_child_ids,p_profile_slots) c(child_id,profile_slot))
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM supplied) UNION ALL
        (SELECT * FROM supplied EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_children_context_mismatch'; END IF;
    SELECT array_agg(CASE
        WHEN bf.complete_at IS NULL AND ROW(bf.attached_child_count,bf.last_child_collection_slot,bf.last_input_child_revision_id)
            IS NOT DISTINCT FROM ROW(i.expected_count,i.after_slot,i.after_id) THEN false
        WHEN ROW(bf.attached_child_count,bf.last_child_collection_slot,bf.last_input_child_revision_id,bf.complete_at IS NOT NULL)
            IS NOT DISTINCT FROM ROW(i.expected_count+i.taken,i.final_slot,i.final_id,i.done) THEN true
        ELSE NULL END ORDER BY i.root_id) INTO replays
        FROM unnest(p_root_ids,p_expected_counts,p_after_slots,p_after_ids,group_counts,final_slots,final_ids,finished)
            i(root_id,expected_count,after_slot,after_id,taken,final_slot,final_id,done)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id;
    IF EXISTS(SELECT 1 FROM unnest(replays) r WHERE r IS NULL) THEN
        RAISE EXCEPTION 'graph_children_progress_conflict' USING ERRCODE='40001'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids) i(root_id,child_id)
        JOIN unnest(p_root_ids,replays) r(root_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id
            AND o.base_family_revision_id=bf.base_family_revision_id AND o.base_child_revision_id=i.child_id WHERE NOT r.replay) THEN
        RAISE EXCEPTION 'graph_children_uncommitted_work'; END IF;
    SELECT coalesce(array_agg(CASE WHEN r.replay THEN o.child_revision_id
            ELSE nextval('__CONTROL__.custom_import_child_revision_child_revision_id_seq'::regclass) END ORDER BY i.ordinality),'{}'::bigint[])
        INTO new_ids FROM unnest(p_child_root_ids,p_child_ids) WITH ORDINALITY i(root_id,child_id,ordinality)
        JOIN unnest(p_root_ids,replays) r(root_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id
            AND o.base_family_revision_id=bf.base_family_revision_id AND o.base_child_revision_id=i.child_id;
    IF cardinality(new_ids)<>n OR EXISTS(SELECT 1 FROM unnest(new_ids) i WHERE i IS NULL) THEN
        RAISE EXCEPTION 'graph_children_stored_identity_mismatch'; END IF;
    SELECT coalesce(array_agg(position ORDER BY ordinality),'{}'::bigint[]) INTO local_ordinals FROM (
        SELECT ordinality,row_number() OVER (PARTITION BY root_id,page ORDER BY ordinality)-1 position
        FROM unnest(p_child_root_ids,child_pages) WITH ORDINALITY i(root_id,page,ordinality)) numbered;
    WITH packs AS MATERIALIZED (
        SELECT m.root_id,m.page,CASE WHEN i.replay THEN min(c.pack_id)
            ELSE nextval('__CONTROL__.custom_import_pack_pack_id_seq'::regclass) END pack_id
        FROM unnest(p_child_root_ids,child_pages,new_ids) m(root_id,page,new_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=m.new_id
        GROUP BY m.root_id,m.page,i.replay)
    SELECT coalesce(array_agg(p.pack_id ORDER BY m.position),'{}'::bigint[]) INTO pack_ids
        FROM unnest(p_child_root_ids,child_pages) WITH ORDINALITY m(root_id,page,position)
        JOIN packs p USING(root_id,page);
    SELECT count(DISTINCT pack_id) INTO pack_n FROM unnest(pack_ids) pack_id;
    SELECT count(*) INTO scalar_n FROM unnest(p_child_ids) m(base_id)
        JOIN __BASE__.custom_import_child_scalar s ON s.child_revision_id=m.base_id;
    WITH costs AS (
        SELECT i.page,3::bigint work_rows,
            octet_length(c.canonical_payload)+octet_length(c.canonical_parent_key)+octet_length(c.canonical_child_key)
                +octet_length(k.canonical_logical_key) work,
            141::bigint+octet_length(c.canonical_payload)+octet_length(c.canonical_parent_key)
                +octet_length(c.canonical_child_key) model
        FROM unnest(p_child_ids,child_pages) i(child_id,page)
        JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=i.child_id
        JOIN __CONTROL__.custom_import_root_record k ON k.root_record_id=c.root_record_id AND k.dataset_id=b.dataset_id
        UNION ALL SELECT i.page,1,octet_length((to_jsonb(s)||jsonb_build_object('child_revision_id',i.new_id))::text),
            octet_length(s.field_type)+octet_length(s.value_state)+coalesce(octet_length(s.string_value),0)
        FROM unnest(p_child_ids,new_ids,child_pages) i(child_id,new_id,page)
        JOIN __BASE__.custom_import_child_scalar s ON s.child_revision_id=i.child_id
        UNION ALL SELECT i.page,1,octet_length(c.canonical),octet_length(c.canonical)+32
        FROM unnest(p_context_child_ids,p_context_keys) c(child_id,canonical)
        JOIN unnest(p_child_ids,child_pages) i(child_id,page) USING(child_id)
        UNION ALL SELECT page,1,0,64 FROM unnest(pack_ids,child_pages) p(pack_id,page) GROUP BY pack_id,page
        UNION ALL SELECT page,1,0,0 FROM unnest(p_child_root_ids,child_pages) i(root_id,page) GROUP BY page,root_id
    ), pages AS (SELECT page,sum(work_rows) work_rows,sum(work) work,sum(model) model FROM costs GROUP BY page)
    SELECT coalesce(sum(work),0),coalesce(sum(model),0),
        coalesce(bool_or(work_rows>b.page_row_limit OR greatest(work,model)>b.page_byte_limit),false)
        INTO work_bytes,model_bytes,invalid_prefix FROM pages;
    IF invalid_prefix OR 3::bigint*n+pack_n+scalar_n+context_n+root_n>100000
        OR greatest(work_bytes,model_bytes)>268435456 THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    PERFORM 1 FROM __CONTROL__.custom_import_build_stream bs WHERE bs.build_id=b.build_id
        AND bs.stream_slot=ANY(member_streams) ORDER BY bs.stream_slot FOR UPDATE;
    IF EXISTS(SELECT 1 FROM unnest(member_streams) m(stream_slot) LEFT JOIN __CONTROL__.custom_import_build_stream bs
        ON bs.build_id=b.build_id AND bs.stream_slot=m.stream_slot WHERE bs.replay_verified_at IS NULL) THEN
        RAISE EXCEPTION 'graph_children_stream_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_child_root_ids,p_child_ids,new_ids,member_slots,member_streams,local_ordinals,pack_ids)
        m(root_id,base_id,new_id,slot,stream_slot,position,pack_id)
        JOIN unnest(p_root_ids,p_family_ids,replays) i(root_id,family_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        JOIN __BASE__.custom_import_child_revision old ON old.child_revision_id=m.base_id
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=m.new_id
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id
            AND o.base_family_revision_id=bf.base_family_revision_id AND o.base_child_revision_id=m.base_id
        LEFT JOIN __CANDIDATE__.custom_import_family_child fc ON fc.family_revision_id=i.family_id AND fc.child_revision_id=m.new_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=m.pack_id
        LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=m.stream_slot
        WHERE i.replay AND (ROW(fc.dataset_id,fc.schema_revision_id,fc.root_record_id,fc.collection_slot,fc.child_revision_id)
                IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,i.root_id,m.slot,m.new_id)
            OR ROW(o.origin,o.child_revision_id,o.stream_slot,o.pack_id,o.collection_slot,o.root_record_id,o.record_kind,
                o.child_key_sha256,o.resolved_rejection_id,o.base_root_revision_id,o.root_revision_id,o.source_ordinal,
                o.raw_parent_key_sha256,o.raw_parent_key_canonical)
                IS DISTINCT FROM ROW('retained'::text,m.new_id,m.stream_slot,m.pack_id,m.slot,i.root_id,'child'::text,
                    old.child_key_sha256,NULL::bigint,NULL::bigint,NULL::bigint,NULL::bigint,NULL::bytea,NULL::text)
            OR ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,c.pack_id,c.source_ordinal,
                c.parent_key_sha256,c.child_key_sha256,c.payload_sha256,convert_to(c.canonical_parent_key,'UTF8'),
                convert_to(c.canonical_child_key,'UTF8'),convert_to(c.canonical_payload,'UTF8'))
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.root_id,m.slot,m.pack_id,m.position,
                    old.parent_key_sha256,old.child_key_sha256,old.payload_sha256,convert_to(old.canonical_parent_key,'UTF8'),
                    convert_to(old.canonical_child_key,'UTF8'),convert_to(old.canonical_payload,'UTF8'))
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.execution_id,p.capture_bundle_id,
                p.stream_slot,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,
                    b.schema_revision_id,b.execution_id,b.capture_bundle_id,m.stream_slot,b.producing_fence,b.producing_token_sha256)
            OR bs.replay_verified_at IS NULL OR p.pack_ordinal>=bs.next_pack_ordinal)) THEN
        RAISE EXCEPTION 'graph_children_stored_identity_mismatch'; END IF;
    IF EXISTS(WITH requested AS (
        SELECT m.root_id,m.pack_id,count(*) taken,i.replay FROM unnest(p_child_root_ids,pack_ids) m(root_id,pack_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id) GROUP BY m.root_id,m.pack_id,i.replay)
        SELECT 1 FROM requested i
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=i.pack_id
        LEFT JOIN LATERAL (SELECT count(*) n FROM (SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.pack_id=i.pack_id LIMIT i.taken+1) bounded) actual ON true
        LEFT JOIN LATERAL (SELECT sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
            ||convert_to(cc.collection_name,'UTF8')||decode('00','hex')||string_agg(c.payload_sha256,''::bytea ORDER BY c.payload_sha256)) digest
            FROM unnest(p_child_root_ids,p_child_ids,pack_ids) m(root_id,base_id,pack_id)
            JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=m.base_id
            JOIN __CONTROL__.custom_import_child_collection cc ON cc.dataset_id=b.dataset_id AND cc.schema_revision_id=b.schema_revision_id AND cc.collection_slot=c.collection_slot
            WHERE m.pack_id=i.pack_id GROUP BY cc.collection_name) expected ON true
        WHERE i.replay AND i.taken>0 AND ROW(p.record_count,p.pack_sha256,actual.n)
            IS DISTINCT FROM ROW(i.taken,expected.digest,i.taken)) THEN RAISE EXCEPTION 'graph_children_stored_pack_mismatch'; END IF;
    IF EXISTS(WITH expected AS (SELECT to_jsonb(s)||jsonb_build_object('child_revision_id',m.new_id) document
        FROM unnest(p_child_root_ids,p_child_ids,new_ids) m(root_id,base_id,new_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __BASE__.custom_import_child_scalar s ON s.child_revision_id=m.base_id WHERE i.replay), actual AS (
        SELECT to_jsonb(s) document FROM unnest(p_child_root_ids,new_ids) m(root_id,new_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_child_scalar s ON s.child_revision_id=m.new_id WHERE i.replay)
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_children_stored_scalar_mismatch'; END IF;
    IF EXISTS(WITH expected AS (
        SELECT f.family_revision_id,m.new_id,c.slot,f.entity_binding_id,m.collection_slot,convert_to(c.canonical,'UTF8') canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8')) digest
        FROM unnest(p_context_child_ids,p_profile_slots,p_context_keys) c(base_id,slot,canonical)
        JOIN unnest(p_child_root_ids,p_child_ids,new_ids,member_slots) m(root_id,base_id,new_id,collection_slot) USING(base_id)
        JOIN unnest(p_root_ids,p_family_ids,replays) i(root_id,family_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id WHERE i.replay), actual AS (
        SELECT c.family_revision_id,c.context_child_revision_id,c.profile_slot,c.entity_binding_id,c.context_collection_slot,
            convert_to(c.canonical_context_key,'UTF8'),c.context_key_sha256 FROM unnest(p_child_root_ids,new_ids) m(root_id,new_id)
        JOIN unnest(p_root_ids,p_family_ids,replays) i(root_id,family_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_build_candidate_context c ON c.build_id=b.build_id
            AND c.family_revision_id=i.family_id AND c.context_child_revision_id=m.new_id WHERE i.replay)
        (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL
        (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)) THEN RAISE EXCEPTION 'graph_children_stored_context_mismatch'; END IF;
    WITH packs AS (
        SELECT m.root_id,m.page,m.stream_slot,m.pack_id,count(*) taken,bs.next_pack_ordinal,
            sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31007061636b00','hex')
                ||convert_to(cc.collection_name,'UTF8')||decode('00','hex')
                ||string_agg(c.payload_sha256,''::bytea ORDER BY c.payload_sha256)) digest
        FROM unnest(p_child_root_ids,p_child_ids,member_slots,member_streams,pack_ids,child_pages)
            m(root_id,base_id,collection_slot,stream_slot,pack_id,page)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=m.base_id
        JOIN __CONTROL__.custom_import_child_collection cc ON cc.dataset_id=b.dataset_id
            AND cc.schema_revision_id=b.schema_revision_id AND cc.collection_slot=m.collection_slot
        JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=m.stream_slot
        WHERE NOT i.replay GROUP BY m.root_id,m.page,m.stream_slot,m.pack_id,bs.next_pack_ordinal,cc.collection_name)
    INSERT INTO __CANDIDATE__.custom_import_pack(pack_id,execution_id,dataset_id,definition_revision_id,schema_revision_id,
        stream_slot,pack_ordinal,capture_bundle_id,record_count,pack_sha256,producing_fence,producing_token_sha256)
        SELECT i.pack_id,b.execution_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,i.stream_slot,
            i.next_pack_ordinal+(row_number() OVER (PARTITION BY i.stream_slot ORDER BY i.page,i.root_id)-1)::integer,
            b.capture_bundle_id,i.taken,i.digest,
            b.producing_fence,b.producing_token_sha256
        FROM packs i;
    IF EXISTS(SELECT 1 FROM __CONTROL__.lookup_custom_import_revision_home('{}',
        ARRAY(SELECT m.new_id FROM unnest(p_child_root_ids,new_ids) m(root_id,new_id)
            JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id) WHERE i.replay)) h
        WHERE h.family_id IS DISTINCT FROM __FAMILY_ID__) THEN
        RAISE EXCEPTION 'graph_children_retry_home_mismatch'; END IF;
    WITH inserted AS (INSERT INTO __CANDIDATE__.custom_import_child_revision(child_revision_id,dataset_id,definition_revision_id,schema_revision_id,
        root_record_id,collection_slot,pack_id,source_ordinal,canonical_parent_key,parent_key_sha256,
        canonical_child_key,child_key_sha256,canonical_payload,payload_sha256)
        SELECT m.new_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,m.root_id,m.collection_slot,m.pack_id,m.position,
            c.canonical_parent_key,c.parent_key_sha256,c.canonical_child_key,c.child_key_sha256,c.canonical_payload,c.payload_sha256
        FROM unnest(p_child_root_ids,p_child_ids,new_ids,member_slots,local_ordinals,pack_ids)
            m(root_id,base_id,new_id,collection_slot,position,pack_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=m.base_id WHERE NOT i.replay
        RETURNING custom_import_child_revision.child_revision_id AS inserted_id)
    SELECT coalesce(array_agg(inserted_id),'{}'::bigint[]) INTO inserted_revision_ids FROM inserted;
    IF cardinality(inserted_revision_ids)>0 THEN
        PERFORM __CONTROL__.append_custom_import_revision_home(__FAMILY_ID__,'{}',inserted_revision_ids); END IF;
    INSERT INTO __CANDIDATE__.custom_import_build_occurrence(build_id,stream_slot,pack_id,origin,base_family_revision_id,
        base_child_revision_id,record_kind,collection_slot,root_record_id,child_key_sha256,child_revision_id)
        SELECT b.build_id,m.stream_slot,m.pack_id,'retained',bf.base_family_revision_id,m.base_id,'child',m.collection_slot,
            m.root_id,c.child_key_sha256,m.new_id FROM unnest(p_child_root_ids,p_child_ids,new_ids,member_slots,member_streams,pack_ids)
            m(root_id,base_id,new_id,collection_slot,stream_slot,pack_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=i.root_id
        JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=m.base_id WHERE NOT i.replay;
    INSERT INTO __CANDIDATE__.custom_import_family_child(family_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,child_revision_id)
        SELECT i.family_id,b.dataset_id,b.schema_revision_id,m.root_id,m.collection_slot,m.new_id
        FROM unnest(p_child_root_ids,new_ids,member_slots) m(root_id,new_id,collection_slot)
        JOIN unnest(p_root_ids,p_family_ids,replays) i(root_id,family_id,replay) USING(root_id) WHERE NOT i.replay;
    INSERT INTO __CANDIDATE__.custom_import_child_scalar(child_revision_id,dataset_id,schema_revision_id,root_record_id,collection_slot,
        field_slot,field_collection_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,
        boolean_value,date_value,timestamp_value)
        SELECT m.new_id,s.dataset_id,s.schema_revision_id,s.root_record_id,s.collection_slot,s.field_slot,s.field_collection_slot,
            s.projection_slot,s.field_type,s.value_state,s.string_value,s.integer_value,s.decimal_value,s.boolean_value,s.date_value,s.timestamp_value
        FROM unnest(p_child_root_ids,p_child_ids,new_ids) m(root_id,base_id,new_id)
        JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
        JOIN __BASE__.custom_import_child_scalar s ON s.child_revision_id=m.base_id WHERE NOT i.replay;
    INSERT INTO __CANDIDATE__.custom_import_build_candidate_context(build_id,profile_slot,entity_binding_id,family_revision_id,
        context_collection_slot,context_child_revision_id,canonical_context_key,context_key_sha256)
        SELECT b.build_id,c.slot,f.entity_binding_id,i.family_id,m.collection_slot,m.new_id,c.canonical,
            sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(c.canonical,'UTF8'))
        FROM unnest(p_context_child_ids,p_profile_slots,p_context_keys) c(base_id,slot,canonical)
        JOIN unnest(p_child_root_ids,p_child_ids,new_ids,member_slots) m(root_id,base_id,new_id,collection_slot) USING(base_id)
        JOIN unnest(p_root_ids,p_family_ids,replays) i(root_id,family_id,replay) USING(root_id)
        JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=i.family_id WHERE NOT i.replay;
    GET DIAGNOSTICS fresh_context_n=ROW_COUNT;
    UPDATE __CONTROL__.custom_import_build_stream bs SET next_pack_ordinal=bs.next_pack_ordinal+delta.packs
        FROM (SELECT m.stream_slot,count(DISTINCT m.pack_id)::integer packs
            FROM unnest(p_child_root_ids,member_streams,pack_ids) m(root_id,stream_slot,pack_id)
            JOIN unnest(p_root_ids,replays) i(root_id,replay) USING(root_id)
            WHERE NOT i.replay GROUP BY m.stream_slot) delta
        WHERE bs.build_id=b.build_id AND bs.stream_slot=delta.stream_slot;
    SELECT count(*) INTO completed_n FROM unnest(finished,replays) i(done,replay) WHERE done AND NOT replay;
    UPDATE __CANDIDATE__.custom_import_build_family bf SET attached_child_count=i.expected_count+i.taken,
        last_child_collection_slot=i.final_slot,last_child_key_sha256=NULL,last_input_child_revision_id=i.final_id,
        complete_at=CASE WHEN i.done THEN clock_timestamp() END
        FROM unnest(p_root_ids,p_expected_counts,group_counts,final_slots,final_ids,finished,replays)
            i(root_id,expected_count,taken,final_slot,final_id,done,replay)
        WHERE bf.build_id=b.build_id AND bf.root_record_id=i.root_id AND NOT i.replay;
    UPDATE __CONTROL__.custom_import_build_attempt SET candidate_context_count=candidate_context_count+fresh_context_n,
        completed_family_count=completed_family_count+completed_n
        WHERE build_id=b.build_id AND (fresh_context_n>0 OR completed_n>0);
    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer FROM __CONTROL__.custom_import_execution e
        JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id WHERE e.execution_id=b.execution_id;
    IF producer.state IS DISTINCT FROM 'running' OR producer.fence IS DISTINCT FROM b.producing_fence
        OR producer.token_sha256 IS DISTINCT FROM b.producing_token_sha256 OR producer.expires_at IS NULL
        OR least(producer.expires_at,b.build_deadline_at)<=clock_timestamp() THEN RAISE EXCEPTION 'graph_children_lease_lost'; END IF;
    RETURN QUERY SELECT bf.root_record_id,bf.attached_child_count,bf.complete_at IS NOT NULL,
        bf.last_child_collection_slot,bf.last_child_key_sha256,bf.last_input_child_revision_id
        FROM __CANDIDATE__.custom_import_build_family bf WHERE bf.build_id=b.build_id AND bf.root_record_id=ANY(p_root_ids)
        ORDER BY bf.root_record_id;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.plan_custom_import_build_family_page(p_build_id bigint,p_expected_page_sequence bigint)
RETURNS TABLE(phase text,plan_stage text,page_sequence bigint,rows_processed integer,plan_complete boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE
    b __CONTROL__.custom_import_build_attempt;
    memberships jsonb;
    n integer; available_n integer; ordinary_n integer; selected_n bigint;
    last_root bigint; used_bytes bigint; invalid_retained boolean;
    pending_root bigint; pending_family bigint; pending_schema bigint; pending_hash bytea;
    root_ids bigint[]; root_hashes bytea[]; kinds text[]; source_ids bigint[]; base_ids bigint[];
    member_n integer; member_available integer; member_bytes bigint;
    member_slots smallint[]; member_ids bigint[]; member_keys text[];
    missing_member boolean; collision boolean;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF b.phase<>'graph' OR b.plan_complete_at IS NOT NULL THEN
        RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
    IF p_expected_page_sequence IS DISTINCT FROM b.plan_page_sequence THEN
        RAISE EXCEPTION 'custom_import_build_progress_conflict' USING ERRCODE='40001'; END IF;
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype::integer & 4)<>0
        AND t.tgrelid='__CANDIDATE__.custom_import_build_family'::regclass) THEN
        RAISE EXCEPTION 'custom_import_build_insert_guards_not_retired'; END IF;
    SELECT coalesce(canonical_definition::jsonb->'child_memberships','[]'::jsonb) INTO memberships
        FROM __CONTROL__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
    IF memberships IS NULL OR jsonb_typeof(memberships)<>'array' OR jsonb_array_length(memberships)>8
        OR (b.plan_membership_after_collection_slot=0)<>(b.plan_membership_after_child_revision_id=0)
        OR ((b.plan_stage<>'base' OR memberships='[]'::jsonb)
            AND b.plan_membership_after_child_revision_id<>0) THEN
        RAISE EXCEPTION 'custom_import_build_structure_mismatch: membership cursor differs'; END IF;

    WITH RECURSIVE source_page(root_record_id,ordinal) AS (
        (SELECT o.root_record_id,1 FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE b.plan_stage='source' AND o.build_id=b.build_id AND o.origin='source' AND o.record_kind='root'
                AND o.root_record_id>b.plan_after_source_root_record_id
            ORDER BY o.root_record_id,o.occurrence_id LIMIT 1)
        UNION ALL
        SELECT next_root.root_record_id,p.ordinal+1 FROM source_page p CROSS JOIN LATERAL (
            SELECT o.root_record_id FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.build_id=b.build_id AND o.origin='source' AND o.record_kind='root'
                AND o.root_record_id>p.root_record_id
            ORDER BY o.root_record_id,o.occurrence_id LIMIT 1
        ) next_root WHERE p.ordinal<b.page_row_limit
    ), requested AS MATERIALIZED (
        SELECT root_record_id FROM (
            SELECT gf.root_record_id FROM __BASE__.custom_import_generation_family gf
            WHERE b.plan_stage='base' AND gf.generation_id=b.base_generation_id
                AND gf.root_record_id>b.plan_after_base_root_record_id
            ORDER BY gf.root_record_id LIMIT b.page_row_limit
        ) base_page UNION ALL SELECT root_record_id FROM source_page
    ), choices AS MATERIALIZED (
        SELECT p.root_record_id,k.logical_key_sha256,o.occurrence_id,gf.family_revision_id,base.schema_revision_id,
            CASE WHEN b.plan_stage='source' AND gf.family_revision_id IS NOT NULL THEN NULL
                WHEN o.occurrence_id IS NOT NULL AND o.resolved_rejection_id IS NULL
                    AND o.root_revision_id IS NOT NULL THEN 'source'
                WHEN gf.family_revision_id IS NOT NULL
                    AND (b.refresh_mode='upsert' OR o.resolved_rejection_id IS NOT NULL) THEN 'retained'
            END selection_kind
        FROM requested p JOIN __CONTROL__.custom_import_root_record k USING(root_record_id)
        LEFT JOIN LATERAL (
            SELECT x.occurrence_id,x.root_revision_id,x.resolved_rejection_id
            FROM __CANDIDATE__.custom_import_build_occurrence x
            WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='root'
                AND x.root_record_id=p.root_record_id ORDER BY x.occurrence_id LIMIT 1
        ) o ON true
        LEFT JOIN __BASE__.custom_import_generation_family gf
            ON gf.generation_id=b.base_generation_id AND gf.root_record_id=p.root_record_id
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=gf.family_revision_id
    ), pending AS MATERIALIZED (
        SELECT * FROM choices WHERE b.plan_stage='base' AND memberships<>'[]'::jsonb AND selection_kind='retained'
            ORDER BY root_record_id LIMIT 1
    ), ordinary AS MATERIALIZED (
        SELECT * FROM choices WHERE NOT EXISTS(SELECT 1 FROM pending)
            OR root_record_id<(SELECT root_record_id FROM pending)
    ), costed AS MATERIALIZED (
        SELECT *,sum(CASE WHEN selection_kind IS NULL THEN 0 ELSE 32+octet_length(selection_kind) END)
            OVER (ORDER BY root_record_id ROWS UNBOUNDED PRECEDING) admitted_bytes FROM ordinary
    ), page AS MATERIALIZED (
        SELECT * FROM costed WHERE admitted_bytes<=b.page_byte_limit
    )
    SELECT count(*)::integer,(SELECT count(*)::integer FROM requested),(SELECT count(*)::integer FROM ordinary),
        max(root_record_id),coalesce(max(admitted_bytes),0),
        coalesce(bool_or(selection_kind='retained' AND schema_revision_id<>b.schema_revision_id),false),
        coalesce(array_agg(root_record_id ORDER BY root_record_id) FILTER(WHERE selection_kind IS NOT NULL),'{}'::bigint[]),
        coalesce(array_agg(logical_key_sha256 ORDER BY root_record_id) FILTER(WHERE selection_kind IS NOT NULL),'{}'::bytea[]),
        coalesce(array_agg(selection_kind ORDER BY root_record_id) FILTER(WHERE selection_kind IS NOT NULL),'{}'::text[]),
        coalesce(array_agg(CASE WHEN selection_kind='source' THEN occurrence_id END ORDER BY root_record_id)
            FILTER(WHERE selection_kind IS NOT NULL),'{}'::bigint[]),
        coalesce(array_agg(CASE WHEN selection_kind='retained' THEN family_revision_id END ORDER BY root_record_id)
            FILTER(WHERE selection_kind IS NOT NULL),'{}'::bigint[]),
        (SELECT root_record_id FROM pending),(SELECT family_revision_id FROM pending),
        (SELECT schema_revision_id FROM pending),(SELECT logical_key_sha256 FROM pending)
    INTO n,available_n,ordinary_n,last_root,used_bytes,invalid_retained,root_ids,root_hashes,kinds,source_ids,base_ids,
        pending_root,pending_family,pending_schema,pending_hash FROM page;
    IF invalid_retained THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch: retained schema differs'; END IF;
    IF b.plan_membership_after_child_revision_id<>0 AND (pending_root IS NULL OR ordinary_n<>0) THEN
        RAISE EXCEPTION 'custom_import_build_structure_mismatch: membership cursor differs'; END IF;
    last_root:=coalesce(last_root,CASE WHEN b.plan_stage='base' THEN b.plan_after_base_root_record_id
        ELSE b.plan_after_source_root_record_id END);

    IF pending_root IS NOT NULL AND n=ordinary_n AND n<b.page_row_limit THEN
        IF pending_schema IS DISTINCT FROM b.schema_revision_id THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch: retained schema differs'; END IF;
        WITH candidates AS MATERIALIZED (
            SELECT fc.collection_slot,fc.child_revision_id,c.canonical_child_key,
                octet_length(c.canonical_child_key)*3+octet_length(memberships::text)*2 raw_bytes
            FROM __BASE__.custom_import_family_child fc
            JOIN __BASE__.custom_import_child_revision c ON c.child_revision_id=fc.child_revision_id
            WHERE fc.family_revision_id=pending_family AND (fc.collection_slot,fc.child_revision_id)>
                (b.plan_membership_after_collection_slot,b.plan_membership_after_child_revision_id)
            ORDER BY fc.collection_slot,fc.child_revision_id LIMIT b.page_row_limit-n+1
        ), costed AS MATERIALIZED (
            SELECT *,row_number() OVER(ORDER BY collection_slot,child_revision_id) ordinal,
                sum(raw_bytes) OVER(ORDER BY collection_slot,child_revision_id ROWS UNBOUNDED PRECEDING) admitted_bytes
            FROM candidates
        ), page AS MATERIALIZED (
            SELECT * FROM costed WHERE ordinal<=b.page_row_limit-n AND admitted_bytes+used_bytes<=b.page_byte_limit
        )
        SELECT count(*)::integer,(SELECT count(*)::integer FROM candidates),coalesce(sum(raw_bytes),0),
            coalesce(array_agg(collection_slot ORDER BY collection_slot,child_revision_id),'{}'::smallint[]),
            coalesce(array_agg(child_revision_id ORDER BY collection_slot,child_revision_id),'{}'::bigint[]),
            coalesce(array_agg(canonical_child_key ORDER BY collection_slot,child_revision_id),'{}'::text[])
        INTO member_n,member_available,member_bytes,member_slots,member_ids,member_keys FROM page;

        WITH expected AS MATERIALIZED (
            SELECT member.child_revision_id,outer_collection.collection_slot,
                __CONTROL__.custom_import_membership_key(policy.value,member.child_key) expected_key
            FROM unnest(member_slots,member_ids,member_keys) member(collection_slot,child_revision_id,child_key)
            JOIN __CONTROL__.custom_import_child_collection inner_collection
                ON inner_collection.schema_revision_id=b.schema_revision_id
                AND inner_collection.collection_slot=member.collection_slot
            CROSS JOIN jsonb_array_elements(memberships) policy(value)
            LEFT JOIN __CONTROL__.custom_import_child_collection outer_collection
                ON outer_collection.schema_revision_id=b.schema_revision_id
                AND outer_collection.collection_name=policy.value->>'outer_collection'
            WHERE policy.value->>'inner_collection'=inner_collection.collection_name
        ), hashed AS MATERIALIZED (
            SELECT *,sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31006368696c642d6b657900','hex')
                ||convert_to(expected_key,'UTF8')) expected_hash FROM expected
        )
        SELECT coalesce(bool_or(actual.canonical_child_key IS NULL),false),
            coalesce(bool_or(actual.canonical_child_key IS NOT NULL
                AND actual.canonical_child_key COLLATE "C" IS DISTINCT FROM expected.expected_key COLLATE "C"),false)
        INTO missing_member,collision FROM hashed expected LEFT JOIN LATERAL (
            SELECT c.canonical_child_key FROM __BASE__.custom_import_child_revision c
            JOIN __BASE__.custom_import_family_child fc ON fc.family_revision_id=pending_family
                AND fc.collection_slot=expected.collection_slot AND fc.child_revision_id=c.child_revision_id
            WHERE c.schema_revision_id=b.schema_revision_id AND c.root_record_id=pending_root
                AND c.collection_slot=expected.collection_slot AND c.child_key_sha256=expected.expected_hash
            ORDER BY c.child_revision_id LIMIT 1
        ) actual ON true;
        IF missing_member THEN
            RAISE EXCEPTION 'custom_import_build_structure_mismatch: retained family violates child membership'; END IF;
        IF collision THEN RAISE EXCEPTION 'custom_import_build_structure_mismatch: membership key digest collision'; END IF;
        n:=n+member_n; used_bytes:=used_bytes+member_bytes;
        IF member_n>0 THEN
            b.plan_membership_after_collection_slot:=member_slots[member_n];
            b.plan_membership_after_child_revision_id:=member_ids[member_n];
        END IF;
        IF member_n=member_available AND used_bytes+40<=b.page_byte_limit THEN
            root_ids:=array_append(root_ids,pending_root); root_hashes:=array_append(root_hashes,pending_hash);
            kinds:=array_append(kinds,'retained'); source_ids:=array_append(source_ids,NULL::bigint);
            base_ids:=array_append(base_ids,pending_family); last_root:=pending_root;
            IF member_n=0 THEN n:=n+1; END IF;
            b.plan_membership_after_collection_slot:=0; b.plan_membership_after_child_revision_id:=0;
        END IF;
    END IF;
    IF n=0 AND available_n>0 THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
    selected_n:=cardinality(root_ids);
    INSERT INTO __CANDIDATE__.custom_import_build_family(build_id,root_record_id,root_key_sha256,
        selection_kind,source_root_occurrence_id,base_family_revision_id)
    SELECT b.build_id,p.root_id,p.root_hash,p.kind,p.source_id,p.base_id
        FROM unnest(root_ids,root_hashes,kinds,source_ids,base_ids) p(root_id,root_hash,kind,source_id,base_id);
    IF n>0 THEN
        IF b.plan_stage='base' THEN b.plan_after_base_root_record_id:=last_root;
        ELSE b.plan_after_source_root_record_id:=last_root; END IF;
    ELSE
        IF b.plan_stage='base' THEN b.plan_stage:='source';
        ELSE b.plan_stage:='complete'; b.plan_complete_at:=clock_timestamp(); END IF;
    END IF;
    UPDATE __CONTROL__.custom_import_build_attempt SET plan_stage=b.plan_stage,plan_page_sequence=b.plan_page_sequence+1,
        plan_after_base_root_record_id=b.plan_after_base_root_record_id,
        plan_after_source_root_record_id=b.plan_after_source_root_record_id,
        plan_membership_after_collection_slot=b.plan_membership_after_collection_slot,
        plan_membership_after_child_revision_id=b.plan_membership_after_child_revision_id,
        plan_complete_at=b.plan_complete_at,selected_family_count=custom_import_build_attempt.selected_family_count+selected_n
    WHERE build_id=b.build_id RETURNING * INTO b;
    PERFORM __CONTROL__.lock_custom_import_build(p_build_id);
    RETURN QUERY SELECT b.phase::text,b.plan_stage::text,b.plan_page_sequence,n,b.plan_complete_at IS NOT NULL;
END;
$fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.membership_batch_finalize(
    p_build_id bigint, p_after_root_record_id bigint
) RETURNS TABLE(after_root_record_id bigint, rows_processed integer,
    generation_family_count bigint, inserted_count integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; g __CONTROL__.custom_import_generation;
    selected __CANDIDATE__.custom_import_build_family[];
    owner_name name; n integer; inserted_n integer; last_root bigint;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t
        WHERE t.tgrelid='__CANDIDATE__.custom_import_generation_family'::regclass
        AND NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4) THEN
        RAISE EXCEPTION 'membership_set_row_guards_require_all_writer_retirement'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN
        RAISE EXCEPTION 'membership_set_protected_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF b.phase<>'output' OR b.generation_id IS NULL OR b.graph_frozen_at IS NULL
        OR b.completed_family_count<>b.selected_family_count
        OR b.generation_family_count>b.selected_family_count THEN
        RAISE EXCEPTION 'membership_set_phase_mismatch'; END IF;
    IF p_after_root_record_id IS NULL OR p_after_root_record_id<0 THEN
        RAISE EXCEPTION 'membership_set_bounds'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=b.generation_id;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,
        g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id,
        g.root_count,g.family_count) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,
        b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256,b.base_generation_id,
        b.selected_family_count,b.selected_family_count)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE generation_id=b.generation_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'membership_set_generation_mismatch'; END IF;
    SELECT array_agg(page.bf ORDER BY (page.bf).root_record_id),count(*)::integer,
        coalesce(max((page.bf).root_record_id),p_after_root_record_id)
    INTO selected,n,last_root FROM (
        SELECT bf FROM __CANDIDATE__.custom_import_build_family bf
        WHERE bf.build_id=b.build_id AND bf.root_record_id>p_after_root_record_id
        ORDER BY bf.root_record_id LIMIT 100000
    ) page;
    IF EXISTS(SELECT 1 FROM unnest(selected) bf
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=bf.family_revision_id
        WHERE bf.complete_at IS NULL OR f.family_revision_id IS NULL
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,
                f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,bf.root_record_id,
                    b.execution_id,b.producing_fence,b.producing_token_sha256)) THEN
        RAISE EXCEPTION 'membership_set_selected_owner_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) bf JOIN __CANDIDATE__.custom_import_generation_family gf
        ON gf.generation_id=b.generation_id AND gf.root_record_id=bf.root_record_id
        WHERE ROW(gf.dataset_id,gf.definition_revision_id,gf.schema_revision_id,gf.family_revision_id)
            IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,bf.family_revision_id)) THEN
        RAISE EXCEPTION 'membership_set_stored_identity_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_generation_family(generation_id,dataset_id,
        definition_revision_id,schema_revision_id,root_record_id,family_revision_id)
        SELECT b.generation_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
            bf.root_record_id,bf.family_revision_id FROM unnest(selected) bf
        LEFT JOIN __CANDIDATE__.custom_import_generation_family gf
            ON gf.generation_id=b.generation_id AND gf.root_record_id=bf.root_record_id
        WHERE gf.generation_id IS NULL;
    GET DIAGNOSTICS inserted_n=ROW_COUNT;
    IF inserted_n>n OR b.generation_family_count+inserted_n>b.selected_family_count THEN
        RAISE EXCEPTION 'membership_set_insert_count_mismatch'; END IF;
    IF inserted_n>0 THEN
        UPDATE __CONTROL__.custom_import_build_attempt
            SET generation_family_count=custom_import_build_attempt.generation_family_count+inserted_n
            WHERE build_id=b.build_id RETURNING * INTO b;
    END IF;
    PERFORM __CONTROL__.lock_custom_import_build(p_build_id);
    RETURN QUERY SELECT last_root,n,b.generation_family_count,inserted_n;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.winner_batch_finalize(
    p_build_id bigint, p_candidate_context_ids bigint[],
    p_after_profile_slot smallint, p_after_entity_binding_id bigint,
    p_after_context_key_sha256 bytea,p_page_sizes integer[]
) RETURNS TABLE(output_after_profile_slot smallint, output_after_entity_binding_id bigint,
    output_after_context_key_sha256 bytea, winner_count bigint, inserted_count integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; g __CONTROL__.custom_import_generation;
    selected __CANDIDATE__.custom_import_build_candidate_context[];
    last_c __CANDIDATE__.custom_import_build_candidate_context;
    owner_name name; n integer; inserted_n integer:=0; replay boolean;
    producer_state text; producer_fence bigint; producer_token bytea;
    lease_end timestamptz; now_at timestamptz;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t
        WHERE t.tgrelid='__CANDIDATE__.custom_import_winner'::regclass
        AND NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4) THEN
        RAISE EXCEPTION 'winner_set_row_guards_require_all_writer_retirement'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN
        RAISE EXCEPTION 'winner_set_protected_owner_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF b.phase<>'output' OR b.generation_id IS NULL OR b.graph_frozen_at IS NULL
        OR b.completed_family_count<>b.selected_family_count
        OR b.generation_family_count<>b.selected_family_count THEN
        RAISE EXCEPTION 'winner_set_phase_or_membership_mismatch'; END IF;
    n:=cardinality(p_candidate_context_ids);
    IF n IS NULL OR n NOT BETWEEN 1 AND 100000
        OR 32::bigint*n>268435456 OR array_ndims(p_candidate_context_ids)<>1
        OR array_lower(p_candidate_context_ids,1)<>1
        OR EXISTS(SELECT 1 FROM unnest(p_candidate_context_ids) i WHERE i IS NULL OR i<=0)
        OR num_nonnulls(p_after_profile_slot,p_after_entity_binding_id,p_after_context_key_sha256) NOT IN (0,3)
        OR (p_after_profile_slot IS NOT NULL AND (p_after_profile_slot<=0
            OR p_after_entity_binding_id<=0 OR octet_length(p_after_context_key_sha256)<>32)) THEN
        RAISE EXCEPTION 'winner_set_bounds'; END IF;
    IF p_page_sizes IS NULL OR cardinality(p_page_sizes) NOT BETWEEN 1 AND 100000
        OR array_ndims(p_page_sizes) IS DISTINCT FROM 1 OR array_lower(p_page_sizes,1) IS DISTINCT FROM 1
        OR EXISTS(SELECT 1 FROM unnest(p_page_sizes) size WHERE size IS NULL OR size NOT BETWEEN 1 AND 256
            OR size>b.page_row_limit OR 32::bigint*size>b.page_byte_limit)
        OR (SELECT sum(size::bigint) FROM unnest(p_page_sizes) size) IS DISTINCT FROM n::bigint THEN
        RAISE EXCEPTION 'winner_batch_logical_page_bounds'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=b.generation_id;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,
        g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id,
        g.root_count,g.family_count) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,
        b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256,b.base_generation_id,
        b.selected_family_count,b.selected_family_count)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal WHERE generation_id=b.generation_id)
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal WHERE execution_id=b.execution_id) THEN
        RAISE EXCEPTION 'winner_set_generation_mismatch'; END IF;
    IF (SELECT coalesce(sum(32::bigint+octet_length(c.canonical_context_key)),0)
        FROM unnest(p_candidate_context_ids) i(id)
        JOIN __CANDIDATE__.custom_import_build_candidate_context c
            ON c.candidate_context_id=i.id AND c.build_id=b.build_id)>268435456 THEN
        RAISE EXCEPTION 'winner_batch_context_bytes'; END IF;
    SELECT array_agg(c ORDER BY i.ordinality) INTO selected
        FROM unnest(p_candidate_context_ids) WITH ORDINALITY i(id,ordinality)
        JOIN __CANDIDATE__.custom_import_build_candidate_context c
            ON c.candidate_context_id=i.id AND c.build_id=b.build_id;
    IF cardinality(selected) IS DISTINCT FROM n
        OR (SELECT count(DISTINCT (c.profile_slot,c.entity_binding_id,c.context_key_sha256)) FROM unnest(selected) c)<>n
        OR (SELECT array_agg(c.candidate_context_id ORDER BY c.profile_slot,c.entity_binding_id,c.context_key_sha256)
            FROM unnest(selected) c) IS DISTINCT FROM p_candidate_context_ids THEN
        RAISE EXCEPTION 'winner_set_context_identity_or_order_mismatch'; END IF;
    last_c:=selected[n];
    IF EXISTS(WITH prefix AS (
        SELECT DISTINCT ON (c.profile_slot,c.entity_binding_id,c.context_key_sha256)
            c.profile_slot,c.entity_binding_id,c.context_key_sha256,c.canonical_context_key
        FROM __CANDIDATE__.custom_import_build_candidate_context c WHERE c.build_id=b.build_id
            AND (p_after_profile_slot IS NULL OR (c.profile_slot,c.entity_binding_id,c.context_key_sha256)>
                (p_after_profile_slot,p_after_entity_binding_id,p_after_context_key_sha256))
        ORDER BY c.profile_slot,c.entity_binding_id,c.context_key_sha256,c.candidate_context_id LIMIT n
    ), numbered AS (
        SELECT p.*,row_number() OVER (ORDER BY p.profile_slot,p.entity_binding_id,p.context_key_sha256) page_ordinal FROM prefix p
    ) SELECT 1 FROM numbered p FULL JOIN unnest(selected) WITH ORDINALITY c ON c.ordinality=p.page_ordinal
        WHERE ROW(p.profile_slot,p.entity_binding_id,p.context_key_sha256,p.canonical_context_key)
            IS DISTINCT FROM ROW(c.profile_slot,c.entity_binding_id,c.context_key_sha256,c.canonical_context_key)) THEN
        RAISE EXCEPTION 'winner_set_progress_conflict' USING ERRCODE='40001'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) c WHERE
        octet_length(c.canonical_context_key) NOT BETWEEN 1 AND 8192 OR octet_length(c.context_key_sha256)<>32
        OR c.context_key_sha256 IS DISTINCT FROM sha256(
            decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')
            || convert_to(c.canonical_context_key,'UTF8')))
        OR EXISTS(SELECT 1 FROM unnest(selected) c JOIN __CANDIDATE__.custom_import_build_candidate_context x
            ON x.build_id=b.build_id AND (x.profile_slot,x.entity_binding_id,x.context_key_sha256)=
                (c.profile_slot,c.entity_binding_id,c.context_key_sha256)
            WHERE x.canonical_context_key IS DISTINCT FROM c.canonical_context_key) THEN
        RAISE EXCEPTION 'winner_set_context_collision'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) c
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=c.family_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id
            AND bf.root_record_id=f.root_record_id AND bf.family_revision_id=f.family_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_generation_family gf ON gf.generation_id=b.generation_id
            AND gf.root_record_id=f.root_record_id AND gf.family_revision_id=f.family_revision_id
        LEFT JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=b.definition_revision_id
            AND p.profile_slot=c.profile_slot AND p.dataset_id=b.dataset_id AND p.schema_revision_id=b.schema_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_child fc ON fc.family_revision_id=f.family_revision_id
            AND fc.collection_slot=c.context_collection_slot AND fc.child_revision_id=c.context_child_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_child_revision ch ON ch.child_revision_id=c.context_child_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack pk ON pk.pack_id=ch.pack_id
        WHERE f.family_revision_id IS NULL OR bf.complete_at IS NULL OR gf.generation_id IS NULL OR p.profile_slot IS NULL
            OR ROW(f.dataset_id,f.schema_revision_id,f.entity_binding_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,c.entity_binding_id,b.execution_id,b.producing_fence,b.producing_token_sha256)
            OR ROW(gf.dataset_id,gf.definition_revision_id,gf.schema_revision_id)
                IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id)
            OR c.context_collection_slot IS DISTINCT FROM coalesce(p.context_collection_slot,0)
            OR (c.context_collection_slot=0 AND c.context_child_revision_id IS NOT NULL)
            OR (c.context_collection_slot>0 AND (fc.child_revision_id IS NULL OR ch.child_revision_id IS NULL OR pk.pack_id IS NULL
                OR ROW(fc.dataset_id,fc.schema_revision_id,fc.root_record_id,ch.dataset_id,ch.schema_revision_id,
                    ch.root_record_id,ch.collection_slot,pk.dataset_id,pk.definition_revision_id,pk.schema_revision_id,
                    pk.execution_id,pk.producing_fence,pk.producing_token_sha256)
                    IS DISTINCT FROM ROW(b.dataset_id,b.schema_revision_id,f.root_record_id,b.dataset_id,b.schema_revision_id,
                        f.root_record_id,c.context_collection_slot,ch.dataset_id,ch.definition_revision_id,ch.schema_revision_id,
                        b.execution_id,b.producing_fence,b.producing_token_sha256)))) THEN
        RAISE EXCEPTION 'winner_set_selected_owner_mismatch'; END IF;
    replay:=b.output_after_profile_slot IS NOT NULL AND
        (last_c.profile_slot,last_c.entity_binding_id,last_c.context_key_sha256)<=
        (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256);
    IF replay THEN
        inserted_n:=0;
    ELSE
        IF ROW(b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256)
            IS DISTINCT FROM ROW(p_after_profile_slot,p_after_entity_binding_id,p_after_context_key_sha256) THEN
            RAISE EXCEPTION 'winner_set_progress_conflict' USING ERRCODE='40001'; END IF;
        IF EXISTS(SELECT 1 FROM unnest(selected) c JOIN __CANDIDATE__.custom_import_winner w
            ON w.generation_id=b.generation_id AND (w.profile_slot,w.entity_binding_id,w.context_key_sha256)=
                (c.profile_slot,c.entity_binding_id,c.context_key_sha256)) THEN
            RAISE EXCEPTION 'winner_set_uncommitted_winner'; END IF;
        INSERT INTO __CANDIDATE__.custom_import_winner(generation_id,dataset_id,definition_revision_id,schema_revision_id,
            profile_slot,entity_binding_id,family_revision_id,context_collection_slot,context_key_sha256,context_child_revision_id)
            SELECT b.generation_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,c.profile_slot,c.entity_binding_id,
                c.family_revision_id,c.context_collection_slot,c.context_key_sha256,c.context_child_revision_id FROM unnest(selected) c;
        GET DIAGNOSTICS inserted_n=ROW_COUNT;
        IF inserted_n<>n THEN RAISE EXCEPTION 'winner_set_insert_count_mismatch'; END IF;
        UPDATE __CONTROL__.custom_import_build_attempt SET output_after_profile_slot=last_c.profile_slot,
            output_after_entity_binding_id=last_c.entity_binding_id,output_after_context_key_sha256=last_c.context_key_sha256,
            winner_count=custom_import_build_attempt.winner_count+inserted_n WHERE build_id=b.build_id RETURNING * INTO b;
    END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) c LEFT JOIN __CANDIDATE__.custom_import_winner w
        ON w.generation_id=b.generation_id AND (w.profile_slot,w.entity_binding_id,w.context_key_sha256)=
            (c.profile_slot,c.entity_binding_id,c.context_key_sha256)
        WHERE ROW(w.dataset_id,w.definition_revision_id,w.schema_revision_id,w.family_revision_id,
            w.context_collection_slot,w.context_child_revision_id)
            IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,c.family_revision_id,
                c.context_collection_slot,c.context_child_revision_id)) THEN
        RAISE EXCEPTION 'winner_set_stored_identity_mismatch'; END IF;
    SELECT e.state,l.fence,l.token_sha256,l.expires_at INTO producer_state,producer_fence,producer_token,lease_end
        FROM __CONTROL__.custom_import_execution e JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id
        WHERE e.execution_id=b.execution_id;
    now_at:=clock_timestamp();
    IF producer_state IS DISTINCT FROM 'running' OR producer_fence IS DISTINCT FROM b.producing_fence
        OR producer_token IS DISTINCT FROM b.producing_token_sha256 OR lease_end IS NULL
        OR least(lease_end,b.build_deadline_at)<=now_at THEN RAISE EXCEPTION 'winner_set_lease_lost'; END IF;
    RETURN QUERY SELECT b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256,
        b.winner_count,inserted_n;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.open_custom_import_build_output(p_build_id bigint,p_generation_id bigint) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; g __CONTROL__.custom_import_generation;
    BEGIN
        b:=__CONTROL__.lock_custom_import_build(p_build_id);
        IF b.phase='output' AND b.generation_id=p_generation_id THEN RETURN b.phase; END IF;
        IF b.phase<>'graph' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF b.plan_complete_at IS NULL OR b.completed_family_count<>b.selected_family_count THEN RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
        IF ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,
            g.base_generation_id,g.root_count,g.family_count) IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
            b.capture_bundle_id,b.producing_fence,b.producing_token_sha256,b.base_generation_id,b.selected_family_count,b.selected_family_count) THEN
            RAISE EXCEPTION 'custom_import_build_identity_mismatch'; END IF;
        UPDATE __CONTROL__.custom_import_build_attempt SET generation_id=g.generation_id,graph_frozen_at=clock_timestamp(),phase='output'
            WHERE build_id=b.build_id;
        RETURN 'output';
    END;
$fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.freeze_custom_import_build_output(p_build_id bigint) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt;
    BEGIN
        b:=__CONTROL__.lock_custom_import_build(p_build_id);
        IF b.phase='verifying' THEN RETURN b.phase; END IF;
        IF b.phase<>'output' THEN RAISE EXCEPTION 'custom_import_build_phase_mismatch'; END IF;
        IF b.selected_family_count<>b.generation_family_count OR EXISTS(
            SELECT 1 FROM __CANDIDATE__.custom_import_build_candidate_context x WHERE x.build_id=b.build_id
            AND (b.output_after_profile_slot IS NULL OR (x.profile_slot,x.entity_binding_id,x.context_key_sha256)>
                (b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256))) THEN
            RAISE EXCEPTION 'custom_import_build_incomplete'; END IF;
        UPDATE __CONTROL__.custom_import_build_attempt SET phase='verifying',output_frozen_at=clock_timestamp() WHERE build_id=b.build_id;
        RETURN 'verifying';
    END;
$fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.check_custom_import_source_replay_homes(
    p_build_id bigint,p_stream_slot smallint,p_part_ordinal integer,p_first_row bigint,p_count integer
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE b __CONTROL__.custom_import_build_attempt; root_ids bigint[]; child_ids bigint[]; n integer;
BEGIN
    b:=__CONTROL__.lock_custom_import_build(p_build_id);
    IF num_nonnulls(p_stream_slot,p_part_ordinal,p_first_row,p_count)<>4 OR p_stream_slot<=0 OR p_part_ordinal<=0
        OR p_first_row<0 OR p_count NOT BETWEEN 1 AND 100000 OR p_count>b.page_row_limit
        OR p_first_row>9223372036854775807-p_count OR b.phase<>'source' THEN
        RAISE EXCEPTION 'source_replay_home_bounds'; END IF;
    SELECT count(*),coalesce(array_agg(o.root_revision_id) FILTER(WHERE o.root_revision_id IS NOT NULL),'{}'),
        coalesce(array_agg(o.child_revision_id) FILTER(WHERE o.child_revision_id IS NOT NULL),'{}')
        INTO n,root_ids,child_ids FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.build_id=b.build_id AND o.origin='source' AND o.stream_slot=p_stream_slot
            AND o.source_part_ordinal=p_part_ordinal AND o.part_row_ordinal>=p_first_row
            AND o.part_row_ordinal<p_first_row+p_count;
    IF n<>p_count OR EXISTS(SELECT 1 FROM __CONTROL__.lookup_custom_import_revision_home(root_ids,child_ids) h
        WHERE h.family_id IS DISTINCT FROM __FAMILY_ID__) THEN
        RAISE EXCEPTION 'source_replay_home_mismatch'; END IF;
    RETURN n;
END $fn$;
