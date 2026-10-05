CREATE FUNCTION __CANDIDATE__.persist_custom_import_scalar_models(
    p_dataset bigint,p_schema bigint,roots __CONTROL__.custom_import_root_scalar[],
    children __CONTROL__.custom_import_child_scalar[],p_expected bigint[],p_expected_token bytea,p_expected_deadline timestamptz
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE root_ids bigint[]; child_ids bigint[]; n integer;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid=ANY(ARRAY['__CANDIDATE__.custom_import_root_scalar'::regclass,'__CANDIDATE__.custom_import_child_scalar'::regclass])) THEN
        RAISE EXCEPTION 'custom_import_scalar_set_requires_all_writer_guard_retirement'; END IF;
    n:=cardinality(roots)+cardinality(children);
    IF n IS NULL OR n NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'custom_import_scalar_set_bounds'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(__FAMILY_ID__,true);
    SELECT coalesce(array_agg(DISTINCT x.root_revision_id ORDER BY x.root_revision_id),'{}') INTO root_ids FROM unnest(roots) x;
    SELECT coalesce(array_agg(DISTINCT x.child_revision_id ORDER BY x.child_revision_id),'{}') INTO child_ids FROM unnest(children) x;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(p_dataset,root_ids,child_ids,NULL,p_expected,p_expected_token,p_expected_deadline);
    IF EXISTS(
        SELECT 1 FROM unnest(roots) x
        LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=x.root_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        LEFT JOIN __CONTROL__.custom_import_field fld ON
            (fld.schema_revision_id,fld.dataset_id,fld.field_slot,fld.field_type,fld.collection_slot,fld.projection_slot)=
            (x.schema_revision_id,x.dataset_id,x.field_slot,x.field_type,x.field_collection_slot,x.projection_slot)
        LEFT JOIN __CONTROL__.custom_import_snapshot_family storage ON storage.family_id=__FAMILY_ID__
        WHERE fld.field_slot IS NULL OR (x.dataset_id,x.schema_revision_id) IS DISTINCT FROM (p_dataset,p_schema)
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id) IS DISTINCT FROM
                ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id)
            OR (__FAMILY_ID__ IS NOT NULL AND ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.execution_id,p.capture_bundle_id,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM
                ROW(storage.dataset_id,storage.definition_revision_id,storage.schema_revision_id,
                    storage.execution_id,storage.capture_bundle_id,storage.producing_fence,storage.producing_token_sha256))
        UNION ALL
        SELECT 1 FROM unnest(children) x
        LEFT JOIN __CANDIDATE__.custom_import_child_revision r ON r.child_revision_id=x.child_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        LEFT JOIN __CONTROL__.custom_import_field fld ON
            (fld.schema_revision_id,fld.dataset_id,fld.field_slot,fld.field_type,fld.collection_slot,fld.projection_slot)=
            (x.schema_revision_id,x.dataset_id,x.field_slot,x.field_type,x.field_collection_slot,x.projection_slot)
        LEFT JOIN __CONTROL__.custom_import_snapshot_family storage ON storage.family_id=__FAMILY_ID__
        WHERE fld.field_slot IS NULL OR (x.dataset_id,x.schema_revision_id) IS DISTINCT FROM (p_dataset,p_schema)
            OR ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id) IS DISTINCT FROM
                ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id)
            OR (__FAMILY_ID__ IS NOT NULL AND ROW(p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.execution_id,p.capture_bundle_id,p.producing_fence,p.producing_token_sha256) IS DISTINCT FROM
                ROW(storage.dataset_id,storage.definition_revision_id,storage.schema_revision_id,
                    storage.execution_id,storage.capture_bundle_id,storage.producing_fence,storage.producing_token_sha256))
    ) THEN RAISE EXCEPTION 'custom_import_scalar_set_field_or_pack'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(roots) x LEFT JOIN __CANDIDATE__.custom_import_root_revision r ON r.root_revision_id=x.root_revision_id
        WHERE ROW(r.dataset_id,r.schema_revision_id,r.root_record_id) IS DISTINCT FROM ROW(p_dataset,p_schema,x.root_record_id))
        OR EXISTS(SELECT 1 FROM unnest(children) x LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id
            WHERE ROW(c.dataset_id,c.schema_revision_id,c.root_record_id,c.collection_slot) IS DISTINCT FROM ROW(p_dataset,p_schema,x.root_record_id,x.collection_slot))
        OR EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f JOIN __CANDIDATE__.custom_import_generation_family gf ON gf.family_revision_id=f.family_revision_id
            JOIN __CONTROL__.custom_import_generation_seal s ON s.generation_id=gf.generation_id WHERE f.root_revision_id=ANY(root_ids))
        OR EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_family_child fc JOIN __CANDIDATE__.custom_import_generation_family gf ON gf.family_revision_id=fc.family_revision_id
            JOIN __CONTROL__.custom_import_generation_seal s ON s.generation_id=gf.generation_id WHERE fc.child_revision_id=ANY(child_ids)) THEN
        RAISE EXCEPTION 'custom_import_scalar_set_scope_or_finality'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(roots) x JOIN __CANDIDATE__.custom_import_root_scalar s USING(root_revision_id,field_slot) WHERE s IS DISTINCT FROM x)
        OR EXISTS(SELECT 1 FROM unnest(children) x JOIN __CANDIDATE__.custom_import_child_scalar s USING(child_revision_id,field_slot) WHERE s IS DISTINCT FROM x) THEN
        RAISE EXCEPTION 'custom_import_scalar_set_replay_mismatch' USING ERRCODE='40001'; END IF;
    -- Only new scalar identities may extend an open GRAPH page. Exact replay
    -- never mutates an already committed page or recharges its accounting.
    IF EXISTS(SELECT 1 FROM unnest(roots) x JOIN __CANDIDATE__.custom_import_root_revision r USING(root_revision_id)
        JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=p.execution_id AND b.producing_fence=p.producing_fence
        WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_root_scalar s WHERE (s.root_revision_id,s.field_slot)=(x.root_revision_id,x.field_slot))
            AND (b.phase<>'graph' OR EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_family bf JOIN __CANDIDATE__.custom_import_family_revision f
                ON f.family_revision_id=bf.family_revision_id WHERE bf.build_id=b.build_id AND f.root_revision_id=x.root_revision_id)))
        OR EXISTS(SELECT 1 FROM unnest(children) x JOIN __CANDIDATE__.custom_import_child_revision c USING(child_revision_id)
            JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=c.pack_id
            JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=p.execution_id AND b.producing_fence=p.producing_fence
            LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.child_revision_id=c.child_revision_id
            LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=o.root_record_id
            LEFT JOIN __CONTROL__.custom_import_child_collection cc ON cc.schema_revision_id=b.schema_revision_id AND cc.collection_slot=o.collection_slot
            LEFT JOIN __CONTROL__.custom_import_child_collection last_cc ON last_cc.schema_revision_id=b.schema_revision_id AND last_cc.collection_slot=bf.last_child_collection_slot
            WHERE NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_child_scalar s WHERE (s.child_revision_id,s.field_slot)=(x.child_revision_id,x.field_slot))
                AND (b.phase<>'graph' OR o.occurrence_id IS NULL OR bf.build_id IS NULL OR bf.complete_at IS NOT NULL
                    OR (bf.last_child_collection_slot IS NOT NULL AND ((o.origin='retained' AND (o.collection_slot,o.base_child_revision_id)<=
                        (bf.last_child_collection_slot,bf.last_input_child_revision_id)) OR (o.origin='source' AND
                        (cc.collection_name COLLATE "C",o.child_key_sha256,o.child_revision_id)<=
                        (last_cc.collection_name COLLATE "C",bf.last_child_key_sha256,bf.last_input_child_revision_id)))))) THEN
        RAISE EXCEPTION 'custom_import_materialization_scalar_page_closed'; END IF;
    INSERT INTO __CANDIDATE__.custom_import_root_scalar SELECT x.* FROM unnest(roots) x WHERE NOT EXISTS(
        SELECT 1 FROM __CANDIDATE__.custom_import_root_scalar s WHERE (s.root_revision_id,s.field_slot)=(x.root_revision_id,x.field_slot));
    INSERT INTO __CANDIDATE__.custom_import_child_scalar SELECT x.* FROM unnest(children) x WHERE NOT EXISTS(
        SELECT 1 FROM __CANDIDATE__.custom_import_child_scalar s WHERE (s.child_revision_id,s.field_slot)=(x.child_revision_id,x.field_slot));
    IF EXISTS(SELECT 1 FROM unnest(roots) x LEFT JOIN __CANDIDATE__.custom_import_root_scalar s USING(root_revision_id,field_slot) WHERE s IS DISTINCT FROM x)
        OR EXISTS(SELECT 1 FROM unnest(children) x LEFT JOIN __CANDIDATE__.custom_import_child_scalar s USING(child_revision_id,field_slot) WHERE s IS DISTINCT FROM x) THEN
        RAISE EXCEPTION 'custom_import_scalar_set_stored_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(p_dataset,root_ids,child_ids,NULL,p_expected,p_expected_token,p_expected_deadline);
    RETURN n;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CANDIDATE__.persist_custom_import_winner_models(
    p_generation bigint,p_dataset bigint,p_definition bigint,p_schema bigint,selected __CONTROL__.custom_import_winner[],
    p_expected bigint[],p_expected_token bytea,p_expected_deadline timestamptz
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; g __CONTROL__.custom_import_generation;
BEGIN
    IF EXISTS(SELECT 1 FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype & 1)=1 AND (t.tgtype & 4)=4
        AND t.tgrelid='__CANDIDATE__.custom_import_winner'::regclass) THEN
        RAISE EXCEPTION 'custom_import_winner_set_requires_all_writer_guard_retirement'; END IF;
    n:=cardinality(selected);
    IF n IS NULL OR n NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'custom_import_winner_set_bounds'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(__FAMILY_ID__,true);
    PERFORM __CONTROL__.lock_custom_import_materialization_set(
        p_dataset,'{}','{}',p_generation,p_expected,p_expected_token,p_expected_deadline);
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation;
    IF ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id) IS DISTINCT FROM ROW(p_dataset,p_definition,p_schema) THEN
        RAISE EXCEPTION 'custom_import_winner_set_generation'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) x
        LEFT JOIN __CANDIDATE__.custom_import_entity_binding e ON
            (e.entity_binding_id,e.dataset_id)=(x.entity_binding_id,x.dataset_id)
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_generation_family m ON
            (m.generation_id,m.dataset_id,m.definition_revision_id,m.schema_revision_id,m.family_revision_id,m.root_record_id)=
            (x.generation_id,x.dataset_id,x.definition_revision_id,x.schema_revision_id,x.family_revision_id,f.root_record_id)
        LEFT JOIN __CANDIDATE__.custom_import_family_child fc ON
            (fc.family_revision_id,fc.collection_slot,fc.child_revision_id,fc.dataset_id,fc.schema_revision_id,fc.root_record_id)=
            (x.family_revision_id,x.context_collection_slot,x.context_child_revision_id,x.dataset_id,x.schema_revision_id,f.root_record_id)
        WHERE ROW(x.generation_id,x.dataset_id,x.definition_revision_id,x.schema_revision_id) IS DISTINCT FROM
            ROW(p_generation,p_dataset,p_definition,p_schema)
            OR e.entity_binding_id IS NULL OR m.generation_id IS NULL
            OR (x.context_child_revision_id IS NOT NULL AND fc.child_revision_id IS NULL)
    ) THEN RAISE EXCEPTION 'custom_import_winner_set_membership'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) x
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f ON f.family_revision_id=x.family_revision_id
        LEFT JOIN __CONTROL__.custom_import_selection_profile p ON p.definition_revision_id=p_definition AND p.profile_slot=x.profile_slot
            AND p.dataset_id=p_dataset AND p.schema_revision_id=p_schema
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c ON c.child_revision_id=x.context_child_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_pack pk ON pk.pack_id=c.pack_id
        WHERE p.profile_slot IS NULL OR x.context_collection_slot IS DISTINCT FROM coalesce(p.context_collection_slot,0)
            OR ROW(f.dataset_id,f.schema_revision_id,f.entity_binding_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS DISTINCT FROM ROW(p_dataset,p_schema,x.entity_binding_id,g.execution_id,g.producing_fence,g.producing_token_sha256)
            OR (x.context_child_revision_id IS NOT NULL AND ROW(c.dataset_id,c.schema_revision_id,c.definition_revision_id,c.root_record_id,c.collection_slot,
                pk.dataset_id,pk.schema_revision_id,pk.definition_revision_id,pk.capture_bundle_id,pk.execution_id,pk.producing_fence,pk.producing_token_sha256)
                IS DISTINCT FROM ROW(p_dataset,p_schema,p_definition,f.root_record_id,x.context_collection_slot,p_dataset,p_schema,p_definition,
                    g.capture_bundle_id,g.execution_id,g.producing_fence,g.producing_token_sha256))) THEN
        RAISE EXCEPTION 'custom_import_winner_set_lineage_or_profile'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(selected) x JOIN __CANDIDATE__.custom_import_winner w
        USING(generation_id,profile_slot,entity_binding_id,context_key_sha256) WHERE w IS DISTINCT FROM x) THEN
        RAISE EXCEPTION 'custom_import_winner_set_replay_mismatch' USING ERRCODE='40001'; END IF;
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_attempt b CROSS JOIN unnest(selected) x
        WHERE b.execution_id=g.execution_id AND b.producing_fence=g.producing_fence
            AND (b.generation_id IS DISTINCT FROM p_generation OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_build_family bf
                WHERE bf.build_id=b.build_id AND bf.family_revision_id=x.family_revision_id AND bf.complete_at IS NOT NULL)
                OR (b.phase<>'output' AND NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_winner w
                    WHERE (w.generation_id,w.profile_slot,w.entity_binding_id,w.context_key_sha256)=
                        (x.generation_id,x.profile_slot,x.entity_binding_id,x.context_key_sha256))))) THEN
        RAISE EXCEPTION 'custom_import_materialization_winner_page_closed'; END IF;
    WITH inserted AS (
        INSERT INTO __CANDIDATE__.custom_import_winner SELECT x.* FROM unnest(selected) x WHERE NOT EXISTS(
            SELECT 1 FROM __CANDIDATE__.custom_import_winner w WHERE (w.generation_id,w.profile_slot,w.entity_binding_id,w.context_key_sha256)=
                (x.generation_id,x.profile_slot,x.entity_binding_id,x.context_key_sha256)) RETURNING generation_id
    ), charges AS (SELECT generation_id,count(*) n FROM inserted GROUP BY generation_id)
    UPDATE __CONTROL__.custom_import_build_attempt b SET winner_count=b.winner_count+charges.n FROM charges
        WHERE b.execution_id=g.execution_id AND b.producing_fence=g.producing_fence AND b.generation_id=charges.generation_id;
    IF EXISTS(SELECT 1 FROM unnest(selected) x LEFT JOIN __CANDIDATE__.custom_import_winner w
        USING(generation_id,profile_slot,entity_binding_id,context_key_sha256) WHERE w IS DISTINCT FROM x) THEN
        RAISE EXCEPTION 'custom_import_winner_set_stored_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(p_dataset,'{}','{}',p_generation,p_expected,p_expected_token,p_expected_deadline);
    RETURN n;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CANDIDATE__.check_custom_import_materialization_completion(
    p_root_revision_ids bigint[],p_child_revision_ids bigint[],p_generation_id bigint,
    p_winner_profile_slots smallint[],p_winner_entity_ids bigint[],p_winner_context_hashes bytea[]
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
BEGIN
    -- Match the old deferred build-link guard, not a new commit-time lease
    -- precondition. Authority is checked at protected page entry and exit.
    -- A caller may legitimately seal or outlive the lease before committing.
    IF EXISTS(WITH packs AS (
        SELECT r.pack_id FROM __CANDIDATE__.custom_import_root_revision r WHERE r.root_revision_id=ANY(p_root_revision_ids)
        UNION SELECT c.pack_id FROM __CANDIDATE__.custom_import_child_revision c WHERE c.child_revision_id=ANY(p_child_revision_ids)
    ) SELECT 1 FROM packs x JOIN __CANDIDATE__.custom_import_pack p USING(pack_id)
        JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=p.execution_id AND b.producing_fence=p.producing_fence
        WHERE NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream s
            WHERE s.build_id=b.build_id AND s.stream_slot=p.stream_slot AND s.next_pack_ordinal>p.pack_ordinal)) THEN
        RAISE EXCEPTION 'custom_import_materialization_pack_page_incomplete'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_root_revision r
        JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=r.pack_id
        JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=p.execution_id AND b.producing_fence=p.producing_fence
        WHERE r.root_revision_id=ANY(p_root_revision_ids) AND NOT EXISTS(
            SELECT 1 FROM __CANDIDATE__.custom_import_build_family bf JOIN __CANDIDATE__.custom_import_family_revision f
                ON f.family_revision_id=bf.family_revision_id
            WHERE bf.build_id=b.build_id AND f.root_revision_id=r.root_revision_id)) THEN
        RAISE EXCEPTION 'custom_import_materialization_root_page_incomplete'; END IF;
    IF EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c
        JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=c.pack_id
        JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=p.execution_id AND b.producing_fence=p.producing_fence
        LEFT JOIN __CANDIDATE__.custom_import_build_occurrence o ON o.build_id=b.build_id AND o.child_revision_id=c.child_revision_id
        LEFT JOIN __CANDIDATE__.custom_import_build_family bf ON bf.build_id=b.build_id AND bf.root_record_id=o.root_record_id
        LEFT JOIN __CONTROL__.custom_import_child_collection cc ON cc.schema_revision_id=b.schema_revision_id AND cc.collection_slot=o.collection_slot
        LEFT JOIN __CONTROL__.custom_import_child_collection last_cc ON last_cc.schema_revision_id=b.schema_revision_id AND last_cc.collection_slot=bf.last_child_collection_slot
        WHERE c.child_revision_id=ANY(p_child_revision_ids) AND (o.occurrence_id IS NULL OR bf.build_id IS NULL
            OR o.origin IS DISTINCT FROM bf.selection_kind OR bf.last_child_collection_slot IS NULL
            OR NOT EXISTS(SELECT 1 FROM __CANDIDATE__.custom_import_family_child fc WHERE fc.family_revision_id=bf.family_revision_id
                AND fc.collection_slot=o.collection_slot AND fc.child_revision_id=c.child_revision_id)
            OR (o.origin='retained' AND (o.collection_slot,o.base_child_revision_id)>(bf.last_child_collection_slot,bf.last_input_child_revision_id))
            OR (o.origin='source' AND (cc.collection_name COLLATE "C",o.child_key_sha256,o.child_revision_id)>
                (last_cc.collection_name COLLATE "C",bf.last_child_key_sha256,bf.last_input_child_revision_id)))) THEN
        RAISE EXCEPTION 'custom_import_materialization_child_page_incomplete'; END IF;
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation g
        JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=g.execution_id AND b.producing_fence=g.producing_fence
        CROSS JOIN unnest(p_winner_profile_slots,p_winner_entity_ids,p_winner_context_hashes) x(profile_slot,entity_id,context_hash)
        WHERE g.generation_id=p_generation_id AND (b.generation_id IS DISTINCT FROM g.generation_id OR b.output_after_profile_slot IS NULL
            OR (x.profile_slot,x.entity_id,x.context_hash)>(b.output_after_profile_slot,b.output_after_entity_binding_id,b.output_after_context_key_sha256))) THEN
        RAISE EXCEPTION 'custom_import_materialization_winner_page_incomplete'; END IF;
END $fn$;
