-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_identity_set(
    p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,
    p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz,
    p_contracts bytea[],p_keys text[],p_hashes bytea[],p_entities text[],p_entity_hashes bytea[],
    p_expected_roots bigint[],p_expected_entities bigint[],p_single_key text
) RETURNS bigint[] LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; owner_name name; work_bytes bigint; result_ids bigint[];
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid IN ('__CONTROL__.custom_import_root_record'::regclass,'__CONTROL__.custom_import_entity_binding'::regclass)) THEN
        RAISE EXCEPTION 'persist_custom_import_legacy_identity_set_requires_shared_set_boundary'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'legacy_graph_owner_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    IF __FAMILY_ID__ IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(
        (SELECT generation_id FROM __CONTROL__.custom_import_generation
         WHERE execution_id=p_execution_id AND producing_fence=p_fence)) THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;
    n:=cardinality(p_contracts);
    IF n IS NULL OR n NOT BETWEEN 1 AND 64 OR EXISTS(SELECT 1 FROM (VALUES
        (cardinality(p_contracts),array_ndims(p_contracts),array_lower(p_contracts,1)),
        (cardinality(p_keys),array_ndims(p_keys),array_lower(p_keys,1)),
        (cardinality(p_hashes),array_ndims(p_hashes),array_lower(p_hashes,1)),
        (cardinality(p_entities),array_ndims(p_entities),array_lower(p_entities,1)),
        (cardinality(p_entity_hashes),array_ndims(p_entity_hashes),array_lower(p_entity_hashes,1)),
        (cardinality(p_expected_roots),array_ndims(p_expected_roots),array_lower(p_expected_roots,1)),
        (cardinality(p_expected_entities),array_ndims(p_expected_entities),array_lower(p_expected_entities,1))
    ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1)) THEN
        RAISE EXCEPTION 'legacy_graph_array_bounds'; END IF;
    IF p_single_key IS NOT NULL AND (n<>1 OR p_keys[1] IS NOT NULL) THEN
        RAISE EXCEPTION 'legacy_identity_singleton_payload'; END IF;
    SELECT 1024+1024::bigint*n+2*coalesce(octet_length(p_single_key),0)
        +2*coalesce(sum(coalesce(octet_length(key),0)+coalesce(octet_length(entity),0)),0)
        INTO work_bytes FROM unnest(p_keys,p_entities) x(key,entity);
    IF (work_bytes>8388608 AND n<>1) OR EXISTS(SELECT 1 FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity)
        WHERE num_nonnulls(x.contract,coalesce(p_single_key,x.key),x.hash,x.entity,x.entity_hash)<>5
            OR octet_length(x.contract)<>32 OR octet_length(x.hash)<>32 OR octet_length(x.entity_hash)<>32
            OR octet_length(x.entity) NOT BETWEEN 1 AND 512
            OR x.expected_root<=0 OR x.expected_entity<=0)
        OR (SELECT count(DISTINCT (x.contract,x.hash)) FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity))<>n
        OR EXISTS(SELECT 1 FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity) GROUP BY x.entity HAVING count(DISTINCT x.entity_hash)<>1) THEN
        RAISE EXCEPTION 'legacy_identity_value_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity)
        LEFT JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=p_dataset_id
            AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=p_dataset_id
            AND e.adapter_id='npi' AND e.canonical_value=x.entity
        WHERE (r.root_record_id IS NOT NULL AND r.canonical_logical_key IS DISTINCT FROM coalesce(p_single_key,x.key))
            OR (e.entity_binding_id IS NOT NULL AND e.value_sha256 IS DISTINCT FROM x.entity_hash)
            OR (x.expected_root IS NOT NULL AND x.expected_root IS DISTINCT FROM r.root_record_id)
            OR (x.expected_entity IS NOT NULL AND x.expected_entity IS DISTINCT FROM e.entity_binding_id)) THEN
        RAISE EXCEPTION 'legacy_identity_collision_or_retained_mismatch' USING ERRCODE='40001'; END IF;
    INSERT INTO __CONTROL__.custom_import_root_record(dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256)
        SELECT p_dataset_id,x.contract,coalesce(p_single_key,x.key),x.hash FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity)
        WHERE NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_root_record r
            WHERE r.dataset_id=p_dataset_id AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash);
    INSERT INTO __CONTROL__.custom_import_entity_binding(dataset_id,adapter_id,canonical_value,value_sha256)
        SELECT DISTINCT p_dataset_id,'npi',x.entity,x.entity_hash FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity)
        WHERE NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_entity_binding e
            WHERE e.dataset_id=p_dataset_id AND e.adapter_id='npi' AND e.canonical_value=x.entity);
    IF EXISTS(SELECT 1 FROM unnest(p_contracts,p_keys,p_hashes,p_entities,p_entity_hashes,p_expected_roots,p_expected_entities) x(contract,key,hash,entity,entity_hash,expected_root,expected_entity)
        LEFT JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=p_dataset_id
            AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash
        LEFT JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=p_dataset_id
            AND e.adapter_id='npi' AND e.canonical_value=x.entity
        WHERE r.root_record_id IS NULL OR e.entity_binding_id IS NULL
            OR r.canonical_logical_key IS DISTINCT FROM coalesce(p_single_key,x.key)
            OR e.value_sha256 IS DISTINCT FROM x.entity_hash) THEN
        RAISE EXCEPTION 'legacy_identity_stored_mismatch'; END IF;

    INSERT INTO __CANDIDATE__.custom_import_root_record
        (root_record_id,dataset_id,key_contract_sha256,canonical_logical_key,logical_key_sha256,created_at)
    SELECT r.root_record_id,r.dataset_id,r.key_contract_sha256,r.canonical_logical_key,r.logical_key_sha256,r.created_at
    FROM unnest(p_contracts,p_hashes) x(contract,hash)
    JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=p_dataset_id
        AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash
    ON CONFLICT (root_record_id) DO NOTHING;
    INSERT INTO __CANDIDATE__.custom_import_entity_binding
        (entity_binding_id,dataset_id,adapter_id,canonical_value,value_sha256,created_at)
    SELECT DISTINCT e.entity_binding_id,e.dataset_id,e.adapter_id,e.canonical_value,e.value_sha256,e.created_at
    FROM unnest(p_entities) x(entity)
    JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=p_dataset_id
        AND e.adapter_id='npi' AND e.canonical_value=x.entity
    ON CONFLICT (entity_binding_id) DO NOTHING;
    IF EXISTS (
        SELECT 1 FROM unnest(p_contracts,p_hashes) x(contract,hash)
        JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=p_dataset_id
            AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash
        LEFT JOIN __CANDIDATE__.custom_import_root_record copied ON copied.root_record_id=r.root_record_id
        WHERE copied.root_record_id IS NULL OR ROW(copied.dataset_id,copied.key_contract_sha256,copied.canonical_logical_key,
            copied.logical_key_sha256,copied.created_at) IS DISTINCT FROM
            ROW(r.dataset_id,r.key_contract_sha256,r.canonical_logical_key,r.logical_key_sha256,r.created_at)
    ) OR EXISTS (
        SELECT 1 FROM unnest(p_entities) x(entity)
        JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=p_dataset_id
            AND e.adapter_id='npi' AND e.canonical_value=x.entity
        LEFT JOIN __CANDIDATE__.custom_import_entity_binding copied ON copied.entity_binding_id=e.entity_binding_id
        WHERE copied.entity_binding_id IS NULL OR ROW(copied.dataset_id,copied.adapter_id,copied.canonical_value,copied.value_sha256,copied.created_at)
            IS DISTINCT FROM ROW(e.dataset_id,e.adapter_id,e.canonical_value,e.value_sha256,e.created_at)
    ) THEN RAISE EXCEPTION 'legacy_identity_snapshot_collision' USING ERRCODE='40001'; END IF;
    SELECT array_agg(ids.value ORDER BY x.ordinality,ids.part) INTO result_ids
        FROM unnest(p_contracts,p_hashes,p_entities) WITH ORDINALITY x(contract,hash,entity,ordinality)
        JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=p_dataset_id
            AND r.key_contract_sha256=x.contract AND r.logical_key_sha256=x.hash
        JOIN __CONTROL__.custom_import_entity_binding e ON e.dataset_id=p_dataset_id
            AND e.adapter_id='npi' AND e.canonical_value=x.entity
        CROSS JOIN LATERAL (VALUES (1,r.root_record_id),(2,e.entity_binding_id)) ids(part,value);
    IF cardinality(result_ids) IS DISTINCT FROM 2*n THEN RAISE EXCEPTION 'legacy_identity_result_count'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    RETURN result_ids;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_family_root_set(
    p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,
    p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz,
    p_revision_ids bigint[],p_family_ids bigint[],p_root_ids bigint[],p_entity_ids bigint[],p_pack_ids bigint[],
    p_ordinals bigint[],p_payloads text[],p_payload_hashes bytea[],p_family_hashes bytea[],
    p_child_counts bigint[],p_base_families bigint[],p_single_payload text
) RETURNS bigint[] LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; owner_name name; work_bytes bigint; result_ids bigint[]; g __CONTROL__.custom_import_generation; revision_ids bigint[]; fresh_revision_ids bigint[]; family_ids bigint[];
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid IN ('__CONTROL__.custom_import_root_revision'::regclass,'__CONTROL__.custom_import_family_revision'::regclass)) THEN
        RAISE EXCEPTION 'persist_custom_import_legacy_family_root_set_requires_shared_set_boundary'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'legacy_graph_owner_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    IF __FAMILY_ID__ IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(
        (SELECT generation_id FROM __CONTROL__.custom_import_generation
         WHERE execution_id=p_execution_id AND producing_fence=p_fence)) THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;
    n:=cardinality(p_revision_ids);
    IF n IS NULL OR n NOT BETWEEN 1 AND 64 OR EXISTS(SELECT 1 FROM (VALUES
        (cardinality(p_revision_ids),array_ndims(p_revision_ids),array_lower(p_revision_ids,1)),
        (cardinality(p_family_ids),array_ndims(p_family_ids),array_lower(p_family_ids,1)),
        (cardinality(p_root_ids),array_ndims(p_root_ids),array_lower(p_root_ids,1)),
        (cardinality(p_entity_ids),array_ndims(p_entity_ids),array_lower(p_entity_ids,1)),
        (cardinality(p_pack_ids),array_ndims(p_pack_ids),array_lower(p_pack_ids,1)),
        (cardinality(p_ordinals),array_ndims(p_ordinals),array_lower(p_ordinals,1)),
        (cardinality(p_payloads),array_ndims(p_payloads),array_lower(p_payloads,1)),
        (cardinality(p_payload_hashes),array_ndims(p_payload_hashes),array_lower(p_payload_hashes,1)),
        (cardinality(p_family_hashes),array_ndims(p_family_hashes),array_lower(p_family_hashes,1)),
        (cardinality(p_child_counts),array_ndims(p_child_counts),array_lower(p_child_counts,1)),
        (cardinality(p_base_families),array_ndims(p_base_families),array_lower(p_base_families,1))
    ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1)) THEN
        RAISE EXCEPTION 'legacy_graph_array_bounds'; END IF;
    IF p_single_payload IS NOT NULL AND (n<>1 OR p_payloads[1] IS NOT NULL) THEN
        RAISE EXCEPTION 'legacy_root_singleton_payload'; END IF;
    SELECT 1024+640::bigint*n+coalesce(octet_length(p_single_payload),0)
        +coalesce(sum(coalesce(octet_length(value),0)),0) INTO work_bytes FROM unnest(p_payloads) value;
    IF (work_bytes>8388608 AND n<>1) OR EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        WHERE num_nonnulls(x.root_id,x.entity_id,x.pack_id,x.ordinal,coalesce(p_single_payload,x.payload),
                x.payload_hash,x.family_hash,x.child_count)<>8
            OR num_nonnulls(x.revision_id,x.family_id) NOT IN (0,2)
            OR x.revision_id<=0 OR x.family_id<=0 OR x.root_id<=0 OR x.entity_id<=0 OR x.pack_id<=0
            OR x.ordinal<0 OR x.child_count<0 OR x.base_family<=0
            OR octet_length(x.payload_hash)<>32 OR octet_length(x.family_hash)<>32)
        OR (SELECT count(DISTINCT x) FROM unnest(p_root_ids) x)<>n
        OR (SELECT count(DISTINCT (x.pack_id,x.ordinal)) FROM unnest(p_pack_ids,p_ordinals) x(pack_id,ordinal))<>n
        OR (SELECT count(DISTINCT x) FROM unnest(p_revision_ids) x)<>(SELECT count(x) FROM unnest(p_revision_ids) x)
        OR (SELECT count(DISTINCT x) FROM unnest(p_family_ids) x)<>(SELECT count(x) FROM unnest(p_family_ids) x) THEN
        RAISE EXCEPTION 'legacy_root_value_bounds'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation
        WHERE execution_id=p_execution_id AND producing_fence=p_fence;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.capture_bundle_id,
        g.producing_token_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,
            p_capture_bundle_id,p_token_sha256) THEN RAISE EXCEPTION 'legacy_root_generation_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=x.pack_id
        LEFT JOIN __CONTROL__.custom_import_source_stream s ON s.definition_revision_id=p_definition_revision_id
            AND s.dataset_id=p_dataset_id AND s.schema_revision_id=p_schema_revision_id
            AND s.stream_slot=p.stream_slot AND s.record_kind='root'
        LEFT JOIN __CANDIDATE__.custom_import_root_record r ON r.root_record_id=x.root_id AND r.dataset_id=p_dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_entity_binding e ON e.entity_binding_id=x.entity_id
            AND e.dataset_id=p_dataset_id AND e.adapter_id='npi'
        WHERE r.root_record_id IS NULL OR e.entity_binding_id IS NULL OR s.stream_slot IS NULL
            OR x.ordinal>=p.record_count OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
                IS DISTINCT FROM ROW(p_execution_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
                    p_capture_bundle_id,p_fence,p_token_sha256)) THEN
        RAISE EXCEPTION 'legacy_root_reference_mismatch'; END IF;
    -- Retained input authority comes from the exact sealed base membership, not
    -- the new producer tuple. Only the newly copied rows use the fresh tuple.
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        LEFT JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=g.base_generation_id
            AND seal.dataset_id=p_dataset_id AND seal.schema_revision_id=p_schema_revision_id
        LEFT JOIN __BASE__.custom_import_generation_family membership ON membership.generation_id=seal.generation_id
            AND membership.dataset_id=p_dataset_id AND membership.schema_revision_id=p_schema_revision_id
            AND membership.definition_revision_id=seal.definition_revision_id
            AND membership.root_record_id=x.root_id AND membership.family_revision_id=x.base_family
        LEFT JOIN __BASE__.custom_import_family_revision f ON f.family_revision_id=membership.family_revision_id
        LEFT JOIN __BASE__.custom_import_root_revision r ON r.root_revision_id=f.root_revision_id
        WHERE x.base_family IS NOT NULL AND (f.family_revision_id IS NULL
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.entity_binding_id,f.family_sha256,f.child_count)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,x.entity_id,x.family_hash,x.child_count)
            OR ROW(r.dataset_id,r.schema_revision_id,r.root_record_id,r.definition_revision_id)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,membership.definition_revision_id)
            OR r.payload_sha256 IS DISTINCT FROM x.payload_hash
            OR r.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload))) THEN
        RAISE EXCEPTION 'legacy_root_retained_source_mismatch'; END IF;
    -- IDs are native sequence values retained by the caller for exact PK replay.
    -- No nonexistent pack/source-ordinal unique constraint is assumed.
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids) x(revision_id,family_id)
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=x.revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_id
        WHERE x.revision_id IS NOT NULL AND (r.root_revision_id IS NULL OR f.family_revision_id IS NULL)) THEN
        RAISE EXCEPTION 'legacy_root_replay_identity_missing'; END IF;
    SELECT array_agg(coalesce(value,nextval(pg_get_serial_sequence(
        '__CONTROL__.custom_import_root_revision','root_revision_id'))) ORDER BY ordinal)
        INTO revision_ids FROM unnest(p_revision_ids) WITH ORDINALITY x(value,ordinal);
    SELECT array_agg(coalesce(value,nextval(pg_get_serial_sequence(
        '__CONTROL__.custom_import_family_revision','family_revision_id'))) ORDER BY ordinal)
        INTO family_ids FROM unnest(p_family_ids) WITH ORDINALITY x(value,ordinal);
    IF EXISTS(SELECT 1 FROM unnest(revision_ids,family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=x.revision_id
        WHERE ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,
            r.source_ordinal,r.payload_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,
                p_schema_revision_id,x.root_id,x.pack_id,x.ordinal,x.payload_hash)
            OR r.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload))
        OR EXISTS(SELECT 1 FROM unnest(revision_ids,family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
            JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_id
            WHERE ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.root_revision_id,f.entity_binding_id,
                f.family_sha256,f.child_count,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,x.revision_id,x.entity_id,
                    x.family_hash,x.child_count,p_execution_id,p_fence,p_token_sha256)) THEN
        RAISE EXCEPTION 'legacy_root_replay_mismatch' USING ERRCODE='40001'; END IF;
    WITH inserted AS (
    INSERT INTO __CANDIDATE__.custom_import_root_revision(root_revision_id,dataset_id,definition_revision_id,
        schema_revision_id,root_record_id,pack_id,source_ordinal,canonical_payload,payload_sha256)
        SELECT x.revision_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,x.root_id,x.pack_id,x.ordinal,
            coalesce(p_single_payload,x.payload),x.payload_hash FROM unnest(revision_ids,family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_root_revision r WHERE r.root_revision_id=x.revision_id)
        RETURNING root_revision_id
    ) SELECT coalesce(array_agg(root_revision_id),'{}'::bigint[]) INTO fresh_revision_ids FROM inserted;
    INSERT INTO __CANDIDATE__.custom_import_family_revision(family_revision_id,dataset_id,schema_revision_id,root_record_id,
        root_revision_id,entity_binding_id,family_sha256,child_count,producing_execution_id,producing_fence,producing_token_sha256)
        SELECT x.family_id,p_dataset_id,p_schema_revision_id,x.root_id,x.revision_id,x.entity_id,x.family_hash,x.child_count,
            p_execution_id,p_fence,p_token_sha256 FROM unnest(revision_ids,family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f WHERE f.family_revision_id=x.family_id);
    IF EXISTS(SELECT 1 FROM unnest(revision_ids,family_ids,p_root_ids,p_entity_ids,p_pack_ids,p_ordinals,p_payloads,p_payload_hashes,p_family_hashes,p_child_counts,p_base_families) x(revision_id,family_id,root_id,entity_id,pack_id,ordinal,payload,payload_hash,family_hash,child_count,base_family)
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=x.revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_id
        WHERE r.root_revision_id IS NULL OR f.family_revision_id IS NULL
            OR ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id,
                r.source_ordinal,r.payload_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,
                    p_schema_revision_id,x.root_id,x.pack_id,x.ordinal,x.payload_hash)
            OR r.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload)
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.root_revision_id,f.entity_binding_id,
                f.family_sha256,f.child_count,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,x.revision_id,x.entity_id,
                    x.family_hash,x.child_count,p_execution_id,p_fence,p_token_sha256)) THEN
        RAISE EXCEPTION 'legacy_root_stored_mismatch'; END IF;
    SELECT array_agg(ids.value ORDER BY x.ordinal,ids.part) INTO result_ids
        FROM unnest(revision_ids,family_ids) WITH ORDINALITY x(revision_id,family_id,ordinal)
        CROSS JOIN LATERAL (VALUES (1,x.revision_id),(2,x.family_id)) ids(part,value);

    -- Persist only the exact BASE origin already checked above; replay cannot change it.
    IF EXISTS (
        SELECT 1 FROM unnest(revision_ids,family_ids,p_root_ids,p_base_families,p_revision_ids) x(revision_id,family_id,root_id,base_family,prior_revision)
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
        LEFT JOIN __CANDIDATE__.legacy_copy_origin origin ON origin.kind='root' AND origin.revision_id=x.revision_id
        WHERE ((x.prior_revision IS NOT NULL OR origin.revision_id IS NOT NULL) AND
            ((x.base_family IS NULL AND origin.revision_id IS NOT NULL)
             OR (x.base_family IS NOT NULL AND (ROW(origin.family_revision_id,origin.root_record_id,origin.root_revision_id,origin.collection_slot,origin.base_generation_id,origin.base_family_revision_id,origin.base_root_revision_id,origin.base_child_revision_id) IS DISTINCT FROM ROW(x.family_id,x.root_id,x.revision_id,NULL::smallint,g.base_generation_id,x.base_family,base.root_revision_id,NULL::bigint)))))
    ) THEN RAISE EXCEPTION 'legacy_root_origin_replay_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.legacy_copy_origin
        (kind,revision_id,family_revision_id,root_record_id,root_revision_id,collection_slot,
         base_generation_id,base_family_revision_id,base_root_revision_id,base_child_revision_id)
    SELECT 'root',x.revision_id,x.family_id,x.root_id,x.revision_id,NULL::smallint,g.base_generation_id,x.base_family,base.root_revision_id,NULL::bigint
    FROM unnest(revision_ids,family_ids,p_root_ids,p_base_families,p_revision_ids) x(revision_id,family_id,root_id,base_family,prior_revision)
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
    WHERE x.base_family IS NOT NULL AND x.prior_revision IS NULL;
    IF EXISTS (
        SELECT 1 FROM unnest(revision_ids,family_ids,p_root_ids,p_base_families,p_revision_ids) x(revision_id,family_id,root_id,base_family,prior_revision)
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
        LEFT JOIN __CANDIDATE__.legacy_copy_origin origin ON origin.kind='root' AND origin.revision_id=x.revision_id
        WHERE (x.base_family IS NULL AND origin.revision_id IS NOT NULL)
            OR (x.base_family IS NOT NULL AND (ROW(origin.family_revision_id,origin.root_record_id,origin.root_revision_id,origin.collection_slot,origin.base_generation_id,origin.base_family_revision_id,origin.base_root_revision_id,origin.base_child_revision_id) IS DISTINCT FROM ROW(x.family_id,x.root_id,x.revision_id,NULL::smallint,g.base_generation_id,x.base_family,base.root_revision_id,NULL::bigint)))
    ) THEN RAISE EXCEPTION 'legacy_root_origin_stored_mismatch'; END IF;
    IF cardinality(fresh_revision_ids)>0 THEN
        PERFORM __CONTROL__.append_custom_import_revision_home(__FAMILY_ID__,
        fresh_revision_ids,'{}'::bigint[]);
    END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    RETURN result_ids;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_child_set(
    p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,
    p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea,p_deadline_at timestamptz,
    p_revision_ids bigint[],p_family_ids bigint[],p_root_ids bigint[],p_collections smallint[],p_pack_ids bigint[],
    p_ordinals bigint[],p_parent_keys text[],p_parent_hashes bytea[],p_child_keys text[],p_child_hashes bytea[],
    p_payloads text[],p_payload_hashes bytea[],p_base_families bigint[],p_base_children bigint[],
    p_single_parent_key text,p_single_child_key text,p_single_payload text
) RETURNS bigint[] LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; owner_name name; work_bytes bigint; result_ids bigint[]; g __CONTROL__.custom_import_generation; revision_ids bigint[]; fresh_revision_ids bigint[];
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid IN ('__CONTROL__.custom_import_child_revision'::regclass,'__CONTROL__.custom_import_family_child'::regclass)) THEN
        RAISE EXCEPTION 'persist_custom_import_legacy_child_set_requires_shared_set_boundary'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN RAISE EXCEPTION 'legacy_graph_owner_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    IF __FAMILY_ID__ IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(
        (SELECT generation_id FROM __CONTROL__.custom_import_generation
         WHERE execution_id=p_execution_id AND producing_fence=p_fence)) THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;
    n:=cardinality(p_revision_ids);
    IF n IS NULL OR n NOT BETWEEN 1 AND 85 OR EXISTS(SELECT 1 FROM (VALUES
        (cardinality(p_revision_ids),array_ndims(p_revision_ids),array_lower(p_revision_ids,1)),
        (cardinality(p_family_ids),array_ndims(p_family_ids),array_lower(p_family_ids,1)),
        (cardinality(p_root_ids),array_ndims(p_root_ids),array_lower(p_root_ids,1)),
        (cardinality(p_collections),array_ndims(p_collections),array_lower(p_collections,1)),
        (cardinality(p_pack_ids),array_ndims(p_pack_ids),array_lower(p_pack_ids,1)),
        (cardinality(p_ordinals),array_ndims(p_ordinals),array_lower(p_ordinals,1)),
        (cardinality(p_parent_keys),array_ndims(p_parent_keys),array_lower(p_parent_keys,1)),
        (cardinality(p_parent_hashes),array_ndims(p_parent_hashes),array_lower(p_parent_hashes,1)),
        (cardinality(p_child_keys),array_ndims(p_child_keys),array_lower(p_child_keys,1)),
        (cardinality(p_child_hashes),array_ndims(p_child_hashes),array_lower(p_child_hashes,1)),
        (cardinality(p_payloads),array_ndims(p_payloads),array_lower(p_payloads,1)),
        (cardinality(p_payload_hashes),array_ndims(p_payload_hashes),array_lower(p_payload_hashes,1)),
        (cardinality(p_base_families),array_ndims(p_base_families),array_lower(p_base_families,1)),
        (cardinality(p_base_children),array_ndims(p_base_children),array_lower(p_base_children,1))
    ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1)) THEN
        RAISE EXCEPTION 'legacy_graph_array_bounds'; END IF;
    IF (p_single_parent_key IS NOT NULL AND (n<>1 OR p_parent_keys[1] IS NOT NULL))
        OR (p_single_child_key IS NOT NULL AND (n<>1 OR p_child_keys[1] IS NOT NULL))
        OR (p_single_payload IS NOT NULL AND (n<>1 OR p_payloads[1] IS NOT NULL)) THEN
        RAISE EXCEPTION 'legacy_child_singleton_payload'; END IF;
    SELECT 1024+640::bigint*n+coalesce(octet_length(p_single_parent_key),0)+coalesce(octet_length(p_single_child_key),0)
        +coalesce(octet_length(p_single_payload),0)+coalesce(sum(coalesce(octet_length(parent_key),0)
            +coalesce(octet_length(child_key),0)+coalesce(octet_length(payload),0)),0)
        INTO work_bytes FROM unnest(p_parent_keys,p_child_keys,p_payloads) x(parent_key,child_key,payload);
    IF (work_bytes>8388608 AND n<>1) OR EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        WHERE num_nonnulls(x.family_id,x.root_id,x.collection,x.pack_id,x.ordinal,coalesce(p_single_parent_key,x.parent_key),
                x.parent_hash,coalesce(p_single_child_key,x.child_key),x.child_hash,coalesce(p_single_payload,x.payload),
                x.payload_hash)<>11 OR num_nonnulls(x.base_family,x.base_child) NOT IN (0,2)
            OR x.revision_id<=0 OR x.family_id<=0 OR x.root_id<=0 OR x.collection<=0 OR x.pack_id<=0 OR x.ordinal<0
            OR x.base_family<=0 OR x.base_child<=0
            OR octet_length(x.parent_hash)<>32 OR octet_length(x.child_hash)<>32 OR octet_length(x.payload_hash)<>32)
        OR (SELECT count(DISTINCT x) FROM unnest(p_revision_ids) x)<>(SELECT count(x) FROM unnest(p_revision_ids) x)
        OR (SELECT count(DISTINCT (x.pack_id,x.ordinal)) FROM unnest(p_pack_ids,p_ordinals) x(pack_id,ordinal))<>n THEN
        RAISE EXCEPTION 'legacy_child_value_bounds'; END IF;
    SELECT * INTO g FROM __CONTROL__.custom_import_generation
        WHERE execution_id=p_execution_id AND producing_fence=p_fence;
    IF g.generation_id IS NULL OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.capture_bundle_id,
        g.producing_token_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,
            p_capture_bundle_id,p_token_sha256) THEN RAISE EXCEPTION 'legacy_child_generation_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_id
        LEFT JOIN __CANDIDATE__.custom_import_root_revision rr ON rr.root_revision_id=f.root_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack root_pack ON root_pack.pack_id=rr.pack_id
        LEFT JOIN __CANDIDATE__.custom_import_root_record r ON r.root_record_id=x.root_id AND r.dataset_id=p_dataset_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=x.pack_id
        LEFT JOIN __CONTROL__.custom_import_source_stream s ON s.definition_revision_id=p_definition_revision_id
            AND s.dataset_id=p_dataset_id AND s.schema_revision_id=p_schema_revision_id
            AND s.stream_slot=p.stream_slot AND s.record_kind='child' AND s.collection_slot=x.collection
        WHERE f.family_revision_id IS NULL OR r.root_record_id IS NULL OR s.stream_slot IS NULL OR x.ordinal>=p.record_count
            OR ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,f.producing_fence,
                f.producing_token_sha256) IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,
                    p_execution_id,p_fence,p_token_sha256)
            OR ROW(rr.dataset_id,rr.definition_revision_id,rr.schema_revision_id,rr.root_record_id)
                IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,x.root_id)
            OR ROW(root_pack.execution_id,root_pack.dataset_id,root_pack.definition_revision_id,root_pack.schema_revision_id,
                root_pack.capture_bundle_id,root_pack.producing_fence,root_pack.producing_token_sha256)
                IS DISTINCT FROM ROW(p_execution_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
                    p_capture_bundle_id,p_fence,p_token_sha256)
            OR ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
                p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM ROW(p_execution_id,p_dataset_id,
                    p_definition_revision_id,p_schema_revision_id,p_capture_bundle_id,p_fence,p_token_sha256)
            OR r.logical_key_sha256 IS DISTINCT FROM x.parent_hash
            OR r.canonical_logical_key IS DISTINCT FROM coalesce(p_single_parent_key,x.parent_key)) THEN
        RAISE EXCEPTION 'legacy_child_reference_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        LEFT JOIN __CONTROL__.custom_import_generation_seal seal ON seal.generation_id=g.base_generation_id
            AND seal.dataset_id=p_dataset_id AND seal.schema_revision_id=p_schema_revision_id
        LEFT JOIN __BASE__.custom_import_generation_family membership ON membership.generation_id=seal.generation_id
            AND membership.dataset_id=p_dataset_id AND membership.schema_revision_id=p_schema_revision_id
            AND membership.definition_revision_id=seal.definition_revision_id
            AND membership.root_record_id=x.root_id AND membership.family_revision_id=x.base_family
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=membership.family_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_revision fresh ON fresh.family_revision_id=x.family_id
        LEFT JOIN __BASE__.custom_import_family_child edge ON edge.family_revision_id=base.family_revision_id
            AND edge.collection_slot=x.collection AND edge.child_revision_id=x.base_child
        LEFT JOIN __BASE__.custom_import_child_revision child ON child.child_revision_id=edge.child_revision_id
        WHERE x.base_child IS NOT NULL AND (child.child_revision_id IS NULL
            OR ROW(base.dataset_id,base.schema_revision_id,base.root_record_id,base.entity_binding_id,
                base.family_sha256,base.child_count) IS DISTINCT FROM ROW(fresh.dataset_id,fresh.schema_revision_id,
                    fresh.root_record_id,fresh.entity_binding_id,fresh.family_sha256,fresh.child_count)
            OR ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id)
            OR ROW(child.dataset_id,child.schema_revision_id,child.root_record_id,child.collection_slot,
                child.parent_key_sha256,child.child_key_sha256,child.payload_sha256)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id,x.collection,
                    x.parent_hash,x.child_hash,x.payload_hash)
            OR child.canonical_parent_key IS DISTINCT FROM coalesce(p_single_parent_key,x.parent_key)
            OR child.canonical_child_key IS DISTINCT FROM coalesce(p_single_child_key,x.child_key)
            OR child.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload))) THEN
        RAISE EXCEPTION 'legacy_child_retained_source_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_family_ids,p_collections) x(revision_id,family_id,collection)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_child edge ON edge.family_revision_id=x.family_id
            AND edge.collection_slot=x.collection AND edge.child_revision_id=x.revision_id
        WHERE x.revision_id IS NOT NULL AND (c.child_revision_id IS NULL OR edge.child_revision_id IS NULL)) THEN
        RAISE EXCEPTION 'legacy_child_replay_identity_missing'; END IF;
    SELECT array_agg(coalesce(value,nextval(pg_get_serial_sequence(
        '__CONTROL__.custom_import_child_revision','child_revision_id'))) ORDER BY ordinal)
        INTO revision_ids FROM unnest(p_revision_ids) WITH ORDINALITY x(value,ordinal);
    IF EXISTS(SELECT 1 FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.revision_id
        WHERE ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,
            c.pack_id,c.source_ordinal,c.parent_key_sha256,c.child_key_sha256,c.payload_sha256)
            IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,x.root_id,x.collection,
                x.pack_id,x.ordinal,x.parent_hash,x.child_hash,x.payload_hash)
            OR c.canonical_parent_key IS DISTINCT FROM coalesce(p_single_parent_key,x.parent_key)
            OR c.canonical_child_key IS DISTINCT FROM coalesce(p_single_child_key,x.child_key)
            OR c.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload))
        OR EXISTS(SELECT 1 FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
            JOIN __CANDIDATE__.custom_import_family_child edge ON edge.family_revision_id=x.family_id
                AND edge.collection_slot=x.collection AND edge.child_revision_id=x.revision_id
            WHERE ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id)) THEN
        RAISE EXCEPTION 'legacy_child_replay_mismatch' USING ERRCODE='40001'; END IF;
    WITH inserted AS (
    INSERT INTO __CANDIDATE__.custom_import_child_revision(child_revision_id,dataset_id,definition_revision_id,schema_revision_id,
        root_record_id,collection_slot,pack_id,source_ordinal,canonical_parent_key,parent_key_sha256,
        canonical_child_key,child_key_sha256,canonical_payload,payload_sha256)
        SELECT x.revision_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,x.root_id,x.collection,x.pack_id,x.ordinal,
            coalesce(p_single_parent_key,x.parent_key),x.parent_hash,coalesce(p_single_child_key,x.child_key),
            x.child_hash,coalesce(p_single_payload,x.payload),x.payload_hash FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c WHERE c.child_revision_id=x.revision_id)
        RETURNING child_revision_id
    ) SELECT coalesce(array_agg(child_revision_id),'{}'::bigint[]) INTO fresh_revision_ids FROM inserted;
    INSERT INTO __CANDIDATE__.custom_import_family_child(family_revision_id,dataset_id,schema_revision_id,root_record_id,
        collection_slot,child_revision_id)
        SELECT x.family_id,p_dataset_id,p_schema_revision_id,x.root_id,x.collection,x.revision_id FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_family_child edge
            WHERE edge.family_revision_id=x.family_id AND edge.collection_slot=x.collection AND edge.child_revision_id=x.revision_id);
    IF EXISTS(SELECT 1 FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_pack_ids,p_ordinals,p_parent_keys,p_parent_hashes,p_child_keys,p_child_hashes,p_payloads,p_payload_hashes,p_base_families,p_base_children) x(revision_id,family_id,root_id,collection,pack_id,ordinal,parent_key,parent_hash,child_key,child_hash,payload,payload_hash,base_family,base_child)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.revision_id
        LEFT JOIN __CANDIDATE__.custom_import_family_child edge ON edge.family_revision_id=x.family_id
            AND edge.collection_slot=x.collection AND edge.child_revision_id=x.revision_id
        WHERE c.child_revision_id IS NULL OR edge.child_revision_id IS NULL
            OR ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,
                c.pack_id,c.source_ordinal,c.parent_key_sha256,c.child_key_sha256,c.payload_sha256)
                IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,x.root_id,x.collection,
                    x.pack_id,x.ordinal,x.parent_hash,x.child_hash,x.payload_hash)
            OR c.canonical_parent_key IS DISTINCT FROM coalesce(p_single_parent_key,x.parent_key)
            OR c.canonical_child_key IS DISTINCT FROM coalesce(p_single_child_key,x.child_key)
            OR c.canonical_payload IS DISTINCT FROM coalesce(p_single_payload,x.payload)
            OR ROW(edge.dataset_id,edge.schema_revision_id,edge.root_record_id)
                IS DISTINCT FROM ROW(p_dataset_id,p_schema_revision_id,x.root_id)) THEN
        RAISE EXCEPTION 'legacy_child_stored_mismatch'; END IF;
    result_ids:=revision_ids;

    -- Persist only the exact BASE origin already checked above; replay cannot change it.
    IF EXISTS (
        SELECT 1 FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_base_families,p_base_children,p_revision_ids) x(revision_id,family_id,root_id,collection,base_family,base_child,prior_revision)
        JOIN __CANDIDATE__.custom_import_family_revision fresh ON fresh.family_revision_id=x.family_id
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
        LEFT JOIN __CANDIDATE__.legacy_copy_origin parent ON parent.kind='root'
            AND parent.family_revision_id=x.family_id
        LEFT JOIN __CANDIDATE__.legacy_copy_origin origin ON origin.kind='child' AND origin.revision_id=x.revision_id
        WHERE ((x.prior_revision IS NOT NULL OR origin.revision_id IS NOT NULL) AND
            ((x.base_family IS NULL AND origin.revision_id IS NOT NULL)
             OR (x.base_family IS NOT NULL AND (ROW(origin.family_revision_id,origin.root_record_id,origin.root_revision_id,origin.collection_slot,origin.base_generation_id,origin.base_family_revision_id,origin.base_root_revision_id,origin.base_child_revision_id) IS DISTINCT FROM ROW(x.family_id,x.root_id,fresh.root_revision_id,x.collection,g.base_generation_id,x.base_family,base.root_revision_id,x.base_child))))) OR (x.base_family IS NOT NULL AND
            ROW(parent.base_generation_id,parent.base_family_revision_id,parent.base_root_revision_id)
            IS DISTINCT FROM ROW(g.base_generation_id,x.base_family,base.root_revision_id))
            OR (x.base_family IS NULL AND parent.revision_id IS NOT NULL)
    ) THEN RAISE EXCEPTION 'legacy_child_origin_replay_mismatch'; END IF;
    INSERT INTO __CANDIDATE__.legacy_copy_origin
        (kind,revision_id,family_revision_id,root_record_id,root_revision_id,collection_slot,
         base_generation_id,base_family_revision_id,base_root_revision_id,base_child_revision_id)
    SELECT 'child',x.revision_id,x.family_id,x.root_id,fresh.root_revision_id,x.collection,g.base_generation_id,x.base_family,base.root_revision_id,x.base_child
    FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_base_families,p_base_children,p_revision_ids) x(revision_id,family_id,root_id,collection,base_family,base_child,prior_revision)
        JOIN __CANDIDATE__.custom_import_family_revision fresh ON fresh.family_revision_id=x.family_id
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
        LEFT JOIN __CANDIDATE__.legacy_copy_origin parent ON parent.kind='root'
            AND parent.family_revision_id=x.family_id
    WHERE x.base_family IS NOT NULL AND x.prior_revision IS NULL;
    IF EXISTS (
        SELECT 1 FROM unnest(revision_ids,p_family_ids,p_root_ids,p_collections,p_base_families,p_base_children,p_revision_ids) x(revision_id,family_id,root_id,collection,base_family,base_child,prior_revision)
        JOIN __CANDIDATE__.custom_import_family_revision fresh ON fresh.family_revision_id=x.family_id
        LEFT JOIN __BASE__.custom_import_family_revision base ON base.family_revision_id=x.base_family
        LEFT JOIN __CANDIDATE__.legacy_copy_origin parent ON parent.kind='root'
            AND parent.family_revision_id=x.family_id
        LEFT JOIN __CANDIDATE__.legacy_copy_origin origin ON origin.kind='child' AND origin.revision_id=x.revision_id
        WHERE (x.base_family IS NULL AND origin.revision_id IS NOT NULL)
            OR (x.base_family IS NOT NULL AND (ROW(origin.family_revision_id,origin.root_record_id,origin.root_revision_id,origin.collection_slot,origin.base_generation_id,origin.base_family_revision_id,origin.base_root_revision_id,origin.base_child_revision_id) IS DISTINCT FROM ROW(x.family_id,x.root_id,fresh.root_revision_id,x.collection,g.base_generation_id,x.base_family,base.root_revision_id,x.base_child)))
    ) THEN RAISE EXCEPTION 'legacy_child_origin_stored_mismatch'; END IF;
    IF cardinality(fresh_revision_ids)>0 THEN
        PERFORM __CONTROL__.append_custom_import_revision_home(__FAMILY_ID__,
        '{}'::bigint[],fresh_revision_ids);
    END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    RETURN result_ids;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_pack_set(
    p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint,
    p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint,
    p_token_sha256 bytea, p_deadline_at timestamptz,
    p_stream_slots smallint[], p_pack_ordinals integer[], p_record_counts bigint[], p_pack_hashes bytea[]
) RETURNS bigint[] LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; owner_name name; pack_ids bigint[];
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid='__CONTROL__.custom_import_pack'::regclass) THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_requires_all_writer_guard_retirement'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_owner_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    -- One root and at most eight declared child streams; all variable values
    -- are fixed-width digests. Empty streams still have an ordinal-zero pack.
    IF __FAMILY_ID__ IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(
        (SELECT generation_id FROM __CONTROL__.custom_import_generation
         WHERE execution_id=p_execution_id AND producing_fence=p_fence)) THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;
    n:=cardinality(p_stream_slots);
    IF n IS NULL OR n NOT BETWEEN 1 AND 9 OR EXISTS(SELECT 1 FROM (VALUES
        (cardinality(p_stream_slots),array_ndims(p_stream_slots),array_lower(p_stream_slots,1)),
        (cardinality(p_pack_ordinals),array_ndims(p_pack_ordinals),array_lower(p_pack_ordinals,1)),
        (cardinality(p_record_counts),array_ndims(p_record_counts),array_lower(p_record_counts,1)),
        (cardinality(p_pack_hashes),array_ndims(p_pack_hashes),array_lower(p_pack_hashes,1))
    ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1))
        OR pg_column_size(ROW(p_stream_slots,p_pack_ordinals,p_record_counts,p_pack_hashes))>8192
        OR EXISTS(SELECT 1 FROM unnest(p_stream_slots,p_pack_ordinals,p_record_counts,p_pack_hashes)
            x(stream_slot,pack_ordinal,record_count,pack_hash)
            WHERE num_nonnulls(x.stream_slot,x.pack_ordinal,x.record_count,x.pack_hash)<>4
                OR x.stream_slot<=0 OR x.pack_ordinal<0 OR x.record_count<0 OR octet_length(x.pack_hash)<>32)
        OR (SELECT count(DISTINCT (x.stream_slot,x.pack_ordinal))
            FROM unnest(p_stream_slots,p_pack_ordinals) x(stream_slot,pack_ordinal))<>n THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_bounds'; END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_stream_slots) x(stream_slot)
        LEFT JOIN __CONTROL__.custom_import_source_stream s ON s.stream_slot=x.stream_slot
            AND s.dataset_id=p_dataset_id AND s.definition_revision_id=p_definition_revision_id
            AND s.schema_revision_id=p_schema_revision_id
        WHERE s.stream_slot IS NULL
    ) THEN RAISE EXCEPTION 'custom_import_legacy_pack_stream_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_stream_slots,p_pack_ordinals,p_record_counts,p_pack_hashes)
        x(stream_slot,pack_ordinal,record_count,pack_hash)
        JOIN __CANDIDATE__.custom_import_pack p ON p.execution_id=p_execution_id AND p.producing_fence=p_fence
            AND (p.stream_slot,p.pack_ordinal)=(x.stream_slot,x.pack_ordinal)
        WHERE ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
            p.record_count,p.pack_sha256,p.producing_token_sha256)
            IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_capture_bundle_id,
                x.record_count,x.pack_hash,p_token_sha256)) THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_replay_mismatch' USING ERRCODE='40001'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_pack(execution_id,dataset_id,definition_revision_id,schema_revision_id,
        stream_slot,pack_ordinal,capture_bundle_id,record_count,pack_sha256,producing_fence,producing_token_sha256)
        SELECT p_execution_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
            x.stream_slot,x.pack_ordinal,p_capture_bundle_id,x.record_count,x.pack_hash,p_fence,p_token_sha256
        FROM unnest(p_stream_slots,p_pack_ordinals,p_record_counts,p_pack_hashes)
            x(stream_slot,pack_ordinal,record_count,pack_hash)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_pack p
            WHERE p.execution_id=p_execution_id AND p.producing_fence=p_fence
                AND (p.stream_slot,p.pack_ordinal)=(x.stream_slot,x.pack_ordinal));
    IF EXISTS(SELECT 1 FROM unnest(p_stream_slots,p_pack_ordinals,p_record_counts,p_pack_hashes)
        x(stream_slot,pack_ordinal,record_count,pack_hash)
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.execution_id=p_execution_id AND p.producing_fence=p_fence
            AND (p.stream_slot,p.pack_ordinal)=(x.stream_slot,x.pack_ordinal)
        WHERE p.pack_id IS NULL OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.capture_bundle_id,
            p.record_count,p.pack_sha256,p.producing_token_sha256)
            IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,p_capture_bundle_id,
                x.record_count,x.pack_hash,p_token_sha256)) THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_stored_mismatch'; END IF;
    SELECT array_agg(p.pack_id ORDER BY x.ordinality) INTO pack_ids
        FROM unnest(p_stream_slots,p_pack_ordinals) WITH ORDINALITY x(stream_slot,pack_ordinal,ordinality)
        JOIN __CANDIDATE__.custom_import_pack p ON p.execution_id=p_execution_id AND p.producing_fence=p_fence
            AND (p.stream_slot,p.pack_ordinal)=(x.stream_slot,x.pack_ordinal);
    IF cardinality(pack_ids) IS DISTINCT FROM n THEN
        RAISE EXCEPTION 'custom_import_legacy_pack_set_returned_count'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    RETURN pack_ids;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_rejection_set(
    p_dataset_id bigint, p_definition_revision_id bigint, p_schema_revision_id bigint,
    p_execution_id bigint, p_capture_bundle_id bigint, p_fence bigint,
    p_token_sha256 bytea, p_deadline_at timestamptz,
    p_ordinals bigint[], p_root_keys text[], p_root_hashes bytea[], p_codes text[], p_evidence text[],
    p_single_root_key text
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; owner_name name; work_bytes bigint;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid='__CONTROL__.custom_import_rejection'::regclass) THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_requires_all_writer_guard_retirement'; END IF;
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_name IS NULL OR current_user<>owner_name THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_owner_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    IF __FAMILY_ID__ IS DISTINCT FROM __CONTROL__.lock_custom_import_legacy_generation_snapshot(
        (SELECT generation_id FROM __CONTROL__.custom_import_generation
         WHERE execution_id=p_execution_id AND producing_fence=p_fence)) THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;
    n:=cardinality(p_ordinals);
    IF n IS NULL OR n NOT BETWEEN 1 AND 256 OR EXISTS(SELECT 1 FROM (VALUES
        (cardinality(p_ordinals),array_ndims(p_ordinals),array_lower(p_ordinals,1)),
        (cardinality(p_root_keys),array_ndims(p_root_keys),array_lower(p_root_keys,1)),
        (cardinality(p_root_hashes),array_ndims(p_root_hashes),array_lower(p_root_hashes,1)),
        (cardinality(p_codes),array_ndims(p_codes),array_lower(p_codes,1)),
        (cardinality(p_evidence),array_ndims(p_evidence),array_lower(p_evidence,1))
    ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1))
        OR (SELECT count(DISTINCT x) FROM unnest(p_ordinals) x)<>n
        OR (p_single_root_key IS NOT NULL AND (n<>1 OR p_root_keys[1] IS NOT NULL)) THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_bounds'; END IF;
    -- The codec's code/contract/digest-only evidence is <=256 UTF-8 bytes.
    -- The 8 MiB multirow target is not a source admission limit. One larger
    -- key uses this same entrypoint as scalar TEXT, retaining the native type
    -- ceiling without an additional array header or a direct-write fallback.
    SELECT 1024+128::bigint*n+coalesce(octet_length(p_single_root_key),0)
        +coalesce(sum(coalesce(octet_length(x.root_key),0)
        +coalesce(octet_length(x.root_hash),0)+coalesce(octet_length(x.code),0)+coalesce(octet_length(x.evidence),0)),0)
        INTO work_bytes FROM unnest(p_root_keys,p_root_hashes,p_codes,p_evidence) x(root_key,root_hash,code,evidence);
    IF (work_bytes>8388608 AND n<>1) OR EXISTS(SELECT 1 FROM unnest(p_ordinals,p_root_keys,p_root_hashes,p_codes,p_evidence)
        x(ordinal,root_key,root_hash,code,evidence)
        WHERE num_nonnulls(x.ordinal,x.code,x.evidence)<>3 OR x.ordinal<0
            OR x.code !~ '^[a-z][a-z0-9_]{0,62}$' OR octet_length(x.evidence)>256
            OR num_nonnulls(coalesce(p_single_root_key,x.root_key),x.root_hash) NOT IN (0,2)
            OR (x.root_hash IS NOT NULL AND octet_length(x.root_hash)<>32)
            OR x.evidence IS DISTINCT FROM ('{"code":"'||x.code||'","contract":"custom-import-rejection/v1",'
                ||'"root_key_sha256":'||coalesce('"'||encode(x.root_hash,'hex')||'"','null')||'}')) THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_evidence_or_bytes_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_ordinals,p_root_keys,p_root_hashes,p_codes,p_evidence)
        x(ordinal,root_key,root_hash,code,evidence)
        JOIN __CANDIDATE__.custom_import_rejection r ON r.execution_id=p_execution_id AND r.producing_fence=p_fence
            AND r.rejection_ordinal=x.ordinal
        WHERE ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.pack_id,r.root_key_sha256,
            r.collection_slot,r.source_ordinal,r.code,r.field_slot,r.canonical_evidence,r.producing_token_sha256)
            IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,NULL::bigint,x.root_hash,
                NULL::smallint,NULL::bigint,x.code,NULL::smallint,x.evidence,p_token_sha256)
            OR r.canonical_root_key IS DISTINCT FROM coalesce(p_single_root_key,x.root_key)) THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_replay_mismatch' USING ERRCODE='40001'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_rejection(rejection_id,execution_id,rejection_ordinal,dataset_id,definition_revision_id,
        schema_revision_id,pack_id,root_key_sha256,canonical_root_key,collection_slot,source_ordinal,code,field_slot,
        canonical_evidence,producing_fence,producing_token_sha256)
        SELECT nextval('__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass),
            p_execution_id,x.ordinal,p_dataset_id,p_definition_revision_id,p_schema_revision_id,NULL,x.root_hash,
            coalesce(p_single_root_key,x.root_key),NULL,NULL,x.code,NULL,x.evidence,p_fence,p_token_sha256
        FROM unnest(p_ordinals,p_root_keys,p_root_hashes,p_codes,p_evidence) x(ordinal,root_key,root_hash,code,evidence)
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_rejection r
            WHERE r.execution_id=p_execution_id AND r.producing_fence=p_fence AND r.rejection_ordinal=x.ordinal)
        ORDER BY x.ordinal;
    IF EXISTS(SELECT 1 FROM unnest(p_ordinals,p_root_keys,p_root_hashes,p_codes,p_evidence)
        x(ordinal,root_key,root_hash,code,evidence)
        LEFT JOIN __CANDIDATE__.custom_import_rejection r ON r.execution_id=p_execution_id AND r.producing_fence=p_fence
            AND r.rejection_ordinal=x.ordinal
        WHERE r.rejection_id IS NULL OR ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.pack_id,r.root_key_sha256,
            r.collection_slot,r.source_ordinal,r.code,r.field_slot,r.canonical_evidence,r.producing_token_sha256)
            IS DISTINCT FROM ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id,NULL::bigint,x.root_hash,
                NULL::smallint,NULL::bigint,x.code,NULL::smallint,x.evidence,p_token_sha256)
            OR r.canonical_root_key IS DISTINCT FROM coalesce(p_single_root_key,x.root_key)) THEN
        RAISE EXCEPTION 'custom_import_legacy_rejection_set_stored_mismatch'; END IF;
    PERFORM __CONTROL__.check_custom_import_materialization_authority(p_dataset_id,p_definition_revision_id,
        p_schema_revision_id,p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256,p_deadline_at);
    RETURN n;
END $fn$;

-- statement boundary --

CREATE FUNCTION __CANDIDATE__.persist_custom_import_legacy_generation_family_set(
    p_generation_id bigint, p_dataset_id bigint, p_definition_revision_id bigint,
    p_schema_revision_id bigint, p_execution_id bigint, p_capture_bundle_id bigint,
    p_fence bigint, p_token bytea, p_root_record_ids bigint[], p_family_revision_ids bigint[],
    p_expected_authority bigint[], p_expected_token bytea, p_expected_expires timestamptz
) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $function$
DECLARE
    page_count integer;
    inserted_count bigint;
BEGIN
    page_count := cardinality(p_root_record_ids);
    IF page_count IS NULL OR page_count NOT BETWEEN 1 AND 16384
        OR cardinality(p_family_revision_ids) IS DISTINCT FROM page_count
        OR array_ndims(p_root_record_ids) IS DISTINCT FROM 1
        OR array_ndims(p_family_revision_ids) IS DISTINCT FROM 1
        OR array_lower(p_root_record_ids, 1) IS DISTINCT FROM 1
        OR array_lower(p_family_revision_ids, 1) IS DISTINCT FROM 1
        OR 1024 + octet_length(array_send(p_root_record_ids))
            + octet_length(array_send(p_family_revision_ids)) > 524288 THEN
        RAISE EXCEPTION 'custom_import_generation_membership_page_malformed' USING ERRCODE = 'P0001';
    END IF;
    PERFORM __CONTROL__.check_custom_import_generation_materialization_authority(
        p_generation_id, p_dataset_id, p_definition_revision_id, p_schema_revision_id,
        p_execution_id, p_capture_bundle_id, p_fence, p_token,
        p_expected_authority, p_expected_token, p_expected_expires
    );

    IF __CONTROL__.lock_custom_import_legacy_generation_snapshot(p_generation_id) IS DISTINCT FROM __FAMILY_ID__ THEN
        RAISE EXCEPTION 'custom_import_legacy_leaf_binding_mismatch'; END IF;

    -- A family has no definition column: its root revision and producing pack
    -- complete the exact definition/capture relationship in one set anti-join.
    IF EXISTS (
            SELECT 1 FROM unnest(p_root_record_ids::bigint[], p_family_revision_ids::bigint[])
                AS candidate(root_record_id, family_revision_id)
            LEFT JOIN __CANDIDATE__.custom_import_family_revision AS family
                ON family.family_revision_id = candidate.family_revision_id
                AND family.root_record_id = candidate.root_record_id
                AND family.dataset_id = p_dataset_id AND family.schema_revision_id = p_schema_revision_id
                AND family.producing_execution_id = p_execution_id AND family.producing_fence = p_fence
                AND family.producing_token_sha256 = p_token
            LEFT JOIN __CANDIDATE__.custom_import_root_revision AS root
                ON root.root_revision_id = family.root_revision_id AND root.root_record_id = candidate.root_record_id
                AND root.dataset_id = p_dataset_id AND root.definition_revision_id = p_definition_revision_id AND root.schema_revision_id = p_schema_revision_id
            LEFT JOIN __CANDIDATE__.custom_import_root_record AS identity
                ON identity.root_record_id = root.root_record_id AND identity.dataset_id = p_dataset_id
            LEFT JOIN __CANDIDATE__.custom_import_pack AS pack
                ON pack.pack_id = root.pack_id AND pack.dataset_id = p_dataset_id
                AND pack.definition_revision_id = p_definition_revision_id AND pack.schema_revision_id = p_schema_revision_id
                AND pack.execution_id = p_execution_id AND pack.capture_bundle_id = p_capture_bundle_id
                AND pack.producing_fence = p_fence AND pack.producing_token_sha256 = p_token
            WHERE candidate.root_record_id IS NULL OR candidate.root_record_id <= 0
                OR candidate.family_revision_id IS NULL OR candidate.family_revision_id <= 0
                OR family.family_revision_id IS NULL OR root.root_revision_id IS NULL
                OR identity.root_record_id IS NULL OR pack.pack_id IS NULL
        ) THEN
        RAISE EXCEPTION 'custom_import_generation_family_authority_mismatch' USING ERRCODE = 'P0001';
    END IF;

    -- Preserve duplicate-root and duplicate-family failures through native
    -- primary/unique constraints; no deduplication, upsert or retry suppression.
    INSERT INTO __CANDIDATE__.custom_import_generation_family (
        generation_id,dataset_id,definition_revision_id,schema_revision_id,root_record_id,family_revision_id
    ) SELECT p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
        candidate.root_record_id,candidate.family_revision_id
    FROM unnest(p_root_record_ids,p_family_revision_ids) AS candidate(root_record_id,family_revision_id);
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    IF inserted_count <> page_count THEN
        RAISE EXCEPTION 'custom_import_generation_membership_count_mismatch' USING ERRCODE = 'P0001';
    END IF;
    PERFORM __CONTROL__.check_custom_import_generation_materialization_authority(
        p_generation_id, p_dataset_id, p_definition_revision_id, p_schema_revision_id,
        p_execution_id, p_capture_bundle_id, p_fence, p_token,
        p_expected_authority, p_expected_token, p_expected_expires
    );
    RETURN inserted_count;
END;
$function$;
