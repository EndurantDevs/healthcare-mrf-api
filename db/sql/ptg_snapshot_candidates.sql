-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __S__.ptg_snapshot_writer_authorized(p_parent text,p_actor name)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN
    RETURN EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_partition_boundary boundary
      JOIN pg_roles role ON role.rolname=ANY(boundary.writer_roles)
      WHERE boundary.table_name=p_parent AND pg_has_role(p_actor,role.rolname,'USAGE'));
END $body$;

CREATE FUNCTION __S__.check_ptg_snapshot_write(p_relation oid,p_operation text,p_actor name)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE trusted boolean; candidate __S__.ptg2_snapshot_candidate;
BEGIN
    IF pg_trigger_depth()=0 THEN RAISE EXCEPTION 'ptg_snapshot_direct_write' USING ERRCODE='42501'; END IF;
    SELECT p_actor=pg_get_userbyid(t.relowner) OR p_actor=pg_get_userbyid(p.proowner)
      INTO trusted FROM pg_class t,pg_proc p
      WHERE t.oid=p_relation AND p.oid='__Q__.begin_ptg_snapshot_candidate(text,bigint,text)'::regprocedure;
    IF trusted THEN RETURN; END IF;
    SELECT * INTO candidate FROM __S__.ptg2_snapshot_candidate WHERE table_oid=p_relation;
    IF p_operation<>'INSERT' OR candidate.table_oid IS NULL OR candidate.prepared OR candidate.published
       OR candidate.writer_name<>p_actor
       OR NOT __S__.ptg_snapshot_writer_authorized(candidate.parent_name,p_actor) THEN
        RAISE EXCEPTION 'ptg_snapshot_direct_write' USING ERRCODE='42501';
    END IF;
    PERFORM 1 FROM __S__.ptg2_v3_snapshot_layout layout JOIN __S__.ptg2_v4_snapshot_map_root root USING(snapshot_key)
      WHERE layout.snapshot_key=candidate.snapshot_key AND layout.build_token=candidate.build_token
        AND layout.state='building' AND root.state='building' FOR UPDATE OF layout,root;
    IF NOT FOUND THEN RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501'; END IF;
    RETURN;
END $body$;

CREATE FUNCTION __S__.guard_ptg_snapshot_write()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
BEGIN
    PERFORM __S__.check_ptg_snapshot_write(TG_RELID,TG_OP,current_user);
    RETURN NULL;
END $body$;

CREATE FUNCTION __S__.begin_ptg_snapshot_candidate(p_table text, p_snapshot bigint, p_token text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog SET lock_timeout='1s' AS $body$
DECLARE candidate text; caller name; grantee record;
BEGIN
    caller:=COALESCE(NULLIF(current_setting('role'),'none'),session_user);
    PERFORM 1 FROM __S__.ptg2_snapshot_partition_boundary WHERE table_name=p_table;
    IF NOT FOUND OR p_snapshot<0 THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_table' USING ERRCODE='55000';
    END IF;
    IF NOT __S__.ptg_snapshot_writer_authorized(p_table,caller) THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501';
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtext('ptg2_source_pointer_gc_v1'));
    PERFORM 1 FROM __S__.ptg2_v3_snapshot_layout l JOIN __S__.ptg2_v4_snapshot_map_root r USING(snapshot_key)
      WHERE l.snapshot_key=p_snapshot AND l.build_token=p_token AND l.state='building'
        AND l.generation='shared_blocks_v4' AND r.state='building' FOR UPDATE OF l,r;
    IF NOT FOUND THEN RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501'; END IF;
    IF EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_legacy_build WHERE snapshot_key=p_snapshot) THEN
        RAISE EXCEPTION 'ptg_snapshot_rerun_required' USING ERRCODE='55000';
    END IF;
    candidate:='ptg_candidate_'||md5(p_table||p_snapshot::text||clock_timestamp()::text||random()::text);
    EXECUTE format('CREATE TABLE __Q__.%I (LIKE __Q__.%I INCLUDING DEFAULTS)',candidate,p_table);
    EXECUTE format('REVOKE ALL ON __Q__.%I FROM PUBLIC',candidate);
    FOR grantee IN SELECT DISTINCT pg_get_userbyid(a.grantee) AS role_name
      FROM pg_class t CROSS JOIN LATERAL aclexplode(t.relacl) a
      WHERE t.oid=to_regclass(format('__Q__.%I',candidate)) AND a.grantee<>0 AND a.grantee<>t.relowner LOOP
        EXECUTE format('REVOKE ALL ON __Q__.%I FROM %I',candidate,grantee.role_name);
    END LOOP;
    EXECUTE format('GRANT INSERT,SELECT ON __Q__.%I TO %I',candidate,caller);
    INSERT INTO __S__.ptg2_snapshot_candidate(table_oid,table_name,parent_name,snapshot_key,build_token,writer_name)
      VALUES(to_regclass(format('__Q__.%I',candidate)),candidate,p_table,p_snapshot,p_token,caller);
    EXECUTE format('CREATE TRIGGER ptg_snapshot_write_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON __Q__.%I FOR EACH STATEMENT EXECUTE FUNCTION __Q__.guard_ptg_snapshot_write()',candidate);
    RETURN candidate;
END $body$;

CREATE FUNCTION __S__.ptg_snapshot_relation(p_parent text, p_snapshot bigint, p_token text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE candidate __S__.ptg2_snapshot_candidate;
BEGIN
    SELECT * INTO candidate FROM __S__.ptg2_snapshot_candidate
      WHERE parent_name=p_parent AND snapshot_key=p_snapshot AND build_token=p_token
        AND prepared AND NOT published ORDER BY table_oid LIMIT 1;
    IF candidate.table_oid IS NOT NULL THEN
        IF to_regclass(format('__Q__.%I',candidate.table_name)) IS DISTINCT FROM candidate.table_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
        RETURN format('__Q__.%I',candidate.table_name);
    END IF;
    RETURN format('__Q__.%I',p_parent);
END $body$;

CREATE FUNCTION __S__.validate_ptg_snapshot_relationships(p_candidate text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE candidate __S__.ptg2_snapshot_candidate; relationship jsonb; target text;
        predicate text; required text; missing boolean;
BEGIN
    SELECT * INTO STRICT candidate FROM __S__.ptg2_snapshot_candidate WHERE table_name=p_candidate;
    FOR relationship IN SELECT jsonb_array_elements(relationships)
      FROM __S__.ptg2_snapshot_partition_boundary WHERE table_name=candidate.parent_name LOOP
        target:=format('%I.%I',relationship->>'schema',relationship->>'table');
        IF relationship->>'schema'=__SCHEMA_LITERAL__ THEN
            target:=__S__.ptg_snapshot_relation(relationship->>'table',candidate.snapshot_key,candidate.build_token);
        END IF;
        SELECT string_agg(format('r.%I=c.%I',remote.column_name,local.column_name),' AND ' ORDER BY local.ordinality),
               string_agg(format('c.%I IS NOT NULL',local.column_name),' AND ' ORDER BY local.ordinality)
          INTO predicate,required
          FROM jsonb_array_elements_text(relationship->'columns') WITH ORDINALITY local(column_name,ordinality)
          JOIN jsonb_array_elements_text(relationship->'target_columns') WITH ORDINALITY remote(column_name,ordinality)
            USING(ordinality);
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM __Q__.%I c WHERE %s AND NOT EXISTS(SELECT 1 FROM %s r WHERE %s))',
          p_candidate,required,target,predicate) INTO missing;
        IF missing THEN RAISE EXCEPTION 'ptg_snapshot_candidate_reference' USING ERRCODE='23514'; END IF;
    END LOOP;
END $body$;

CREATE FUNCTION __S__.read_ptg_snapshot_candidates(p_snapshot bigint, p_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $body$
DECLARE caller name; result jsonb;
BEGIN
    caller:=COALESCE(NULLIF(current_setting('role'),'none'),session_user);
    PERFORM 1 FROM __S__.ptg2_v3_snapshot_layout l JOIN __S__.ptg2_v4_snapshot_map_root r USING(snapshot_key)
      WHERE l.snapshot_key=p_snapshot AND l.build_token=p_token AND l.state='building'
        AND l.generation='shared_blocks_v4' AND r.state='building' FOR UPDATE OF l,r;
    IF NOT FOUND THEN RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501'; END IF;
    IF EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_candidate WHERE snapshot_key=p_snapshot
      AND (build_token<>p_token OR writer_name<>caller OR NOT prepared
           OR NOT __S__.ptg_snapshot_writer_authorized(parent_name,caller))) THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501';
    END IF;
    SELECT COALESCE(jsonb_object_agg(parent_name,table_name),'{}'::jsonb) INTO result
      FROM __S__.ptg2_snapshot_candidate WHERE snapshot_key=p_snapshot AND prepared AND NOT published;
    RETURN result;
END $body$;

CREATE FUNCTION __S__.finish_ptg_snapshot_candidate(p_candidate text, p_count bigint)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog SET lock_timeout='1s' AS $body$
DECLARE c __S__.ptg2_snapshot_candidate; caller name; observed bigint; invalid boolean;
        idx record; sequence integer:=0; columns text; index_tail text; dense_key text; index_name text; reference_table text;
BEGIN
    caller:=COALESCE(NULLIF(current_setting('role'),'none'),session_user);
    SELECT * INTO c FROM __S__.ptg2_snapshot_candidate WHERE table_name=p_candidate FOR UPDATE;
    IF c.table_oid IS NULL OR c.writer_name<>caller OR c.prepared OR c.published
       OR NOT __S__.ptg_snapshot_writer_authorized(c.parent_name,caller) THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501';
    END IF;
    PERFORM 1 FROM __S__.ptg2_v3_snapshot_layout l JOIN __S__.ptg2_v4_snapshot_map_root r USING(snapshot_key)
      WHERE l.snapshot_key=c.snapshot_key AND l.build_token=c.build_token AND l.state='building'
        AND l.generation='shared_blocks_v4' AND r.state='building' FOR UPDATE OF l,r;
    IF NOT FOUND THEN RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501'; END IF;
    EXECUTE format('LOCK TABLE __Q__.%I IN ACCESS EXCLUSIVE MODE',p_candidate);
    IF to_regclass(format('__Q__.%I',p_candidate))<>c.table_oid THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
    END IF;
    EXECUTE format('REVOKE INSERT ON __Q__.%I FROM %I',p_candidate,caller);
    PERFORM pg_advisory_xact_lock_shared(hashtext('ptg2_source_pointer_gc_v1'));
    -- Finish isolated indexes before indexed relationship and semantic checks.
    FOR idx IN SELECT i.indexrelid,i.indisunique,k.contype FROM pg_index i
      LEFT JOIN pg_constraint k ON k.conindid=i.indexrelid AND k.conrelid=i.indrelid AND k.contype IN ('p','u')
      WHERE i.indrelid=to_regclass(format('__Q__.%I',c.parent_name)) ORDER BY i.indexrelid LOOP
        sequence:=sequence+1;
        index_tail:=substring(pg_get_indexdef(idx.indexrelid) from ' USING .+$');
        index_name:='ptg_candidate_idx_'||c.table_oid::text||'_'||sequence;
        EXECUTE format('CREATE %s INDEX %I ON __Q__.%I%s',CASE WHEN idx.indisunique THEN 'UNIQUE' ELSE '' END,
          index_name,p_candidate,index_tail);
        IF idx.contype IN ('p','u') THEN
            EXECUTE format('ALTER TABLE __Q__.%I ADD CONSTRAINT %I %s USING INDEX %I',
              p_candidate,index_name,CASE WHEN idx.contype='p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,index_name);
        END IF;
    END LOOP;
    EXECUTE format('ALTER TABLE __Q__.%I ADD CONSTRAINT snapshot_identity CHECK(snapshot_key=%s)',p_candidate,c.snapshot_key);
    FOR idx IN SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint
      WHERE conrelid=to_regclass(format('__Q__.%I',c.parent_name)) AND contype='c' LOOP
        EXECUTE format('ALTER TABLE __Q__.%I ADD CONSTRAINT %I %s',p_candidate,idx.conname,idx.definition);
    END LOOP;
    EXECUTE format('SELECT count(*) FROM __Q__.%I',p_candidate) INTO observed;
    IF p_count IS NULL OR p_count<0 OR observed<>p_count THEN RAISE EXCEPTION 'ptg_snapshot_candidate_count' USING ERRCODE='23514'; END IF;
    IF c.parent_name='ptg2_v4_snapshot_map_pack' THEN
        EXECUTE format($sql$
            SELECT EXISTS(SELECT 1 FROM (
              SELECT *, row_number() OVER (PARTITION BY object_kind ORDER BY pack_no)-1 AS expected_no,
                lag(last_block_key) OVER w AS previous_key, lag(last_fragment_no) OVER w AS previous_fragment
                FROM __S__.%I WINDOW w AS(PARTITION BY object_kind ORDER BY pack_no)
            ) p WHERE pack_no<>expected_no OR
                ROW(previous_key,previous_fragment)>=ROW(first_block_key,first_fragment_no))
        $sql$,p_candidate) INTO invalid;
        IF invalid THEN RAISE EXCEPTION 'ptg_snapshot_candidate_overlap' USING ERRCODE='23514'; END IF;
        EXECUTE format($sql$
            SELECT EXISTS(SELECT 1 FROM __S__.%I p LEFT JOIN __S__.ptg2_v3_block b ON b.block_hash=p.map_block_hash
              WHERE b.block_hash IS NULL OR b.object_kind<>'snapshot_coordinate_map_v1'
                OR b.codec<>'none' OR b.format_version<>2 OR b.entry_count<>p.coordinate_count)
        $sql$,p_candidate) INTO invalid;
    ELSIF c.parent_name='ptg2_provider_tax_identity' THEN
        EXECUTE format($sql$SELECT count(*)<>COALESCE(max(tin_key)+1,0) OR COALESCE(min(tin_key),0)<>0
            OR COALESCE(bool_or(previous_hmac>=tin_hmac_sha256),false)
            FROM (SELECT tin_key,tin_hmac_sha256,lag(tin_hmac_sha256) OVER(ORDER BY tin_key) AS previous_hmac
                  FROM __S__.%I) ordered$sql$,p_candidate) INTO invalid;
    ELSIF c.parent_name='ptg2_provider_group_tax_identity_source' THEN
        reference_table:=__S__.ptg_snapshot_relation('ptg2_provider_group_tax_identity',c.snapshot_key,c.build_token);
        EXECUTE format($sql$SELECT EXISTS(SELECT 1 FROM __Q__.%I source
          WHERE source.tax_identity_state='matched_ein' AND NOT EXISTS(SELECT 1 FROM %s merged
            WHERE merged.snapshot_key=source.snapshot_key
              AND merged.provider_group_global_id_128=source.provider_group_global_id_128
              AND merged.tax_identity_state='matched_ein' AND merged.tin_key=source.tin_key))$sql$,
          p_candidate,reference_table) INTO invalid;
    ELSIF c.parent_name='ptg2_provider_group_tax_identity' THEN
        reference_table:=__S__.ptg_snapshot_relation('ptg2_provider_tax_identity',c.snapshot_key,c.build_token);
        EXECUTE format($sql$
            SELECT EXISTS(SELECT 1 FROM __S__.%I g
                LEFT JOIN __S__.ptg2_provider_tax_identity_manifest m USING(snapshot_key)
                LEFT JOIN %s t ON t.snapshot_key=g.snapshot_key AND t.tin_key=g.tin_key
              WHERE m.snapshot_key IS NULL OR (g.tin_key IS NOT NULL AND t.tin_key IS NULL)
                OR g.source_bitmap=decode(repeat('00',octet_length(g.source_bitmap)),'hex')
                OR octet_length(g.source_bitmap)<>(m.source_shard_count+7)/8
                OR (m.source_shard_count%%8<>0 AND get_byte(g.source_bitmap,octet_length(g.source_bitmap)-1)>=(1<<(m.source_shard_count%%8))))
        $sql$,p_candidate,reference_table) INTO invalid;
    ELSE
        dense_key:=CASE c.parent_name WHEN 'ptg2_v4_npi_scope' THEN 'npi_key'
          WHEN 'ptg2_v4_provider_component' THEN 'component_key'
          WHEN 'ptg2_v4_pattern' THEN 'pattern_key' END;
        invalid:=false;
        IF dense_key IS NOT NULL THEN
            EXECUTE format('SELECT count(*)<>COALESCE(max(%I)::bigint+1,0) OR COALESCE(min(%I),0)<>0 FROM __Q__.%I',
              dense_key,dense_key,p_candidate) INTO invalid;
        END IF;
        IF c.parent_name='ptg2_v4_heavy_owner' THEN
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM __Q__.%I WHERE octet_length(relation) NOT BETWEEN 1 AND 32 OR octet_length(object_kind) NOT BETWEEN 1 AND 64)',p_candidate) INTO invalid;
        END IF;
        -- Sparse prefix and heavy-owner sets retain their native checks and FKs.
    END IF;
    IF invalid THEN RAISE EXCEPTION 'ptg_snapshot_candidate_reference' USING ERRCODE='23514'; END IF;
    PERFORM __S__.validate_ptg_snapshot_relationships(p_candidate);
    reference_table:=__S__.ptg_snapshot_relation(c.parent_name,c.snapshot_key,c.build_token);
    -- Exact replays compare complete sets before disposing of the redundant heap.
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s WHERE snapshot_key=$1)',reference_table) INTO invalid USING c.snapshot_key;
    IF invalid OR EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_candidate WHERE parent_name=c.parent_name AND snapshot_key=c.snapshot_key AND prepared AND table_oid<>c.table_oid) THEN
        SELECT string_agg(quote_ident(attname),',' ORDER BY attnum) INTO columns FROM pg_attribute
          WHERE attrelid=c.table_oid AND attnum>0 AND NOT attisdropped AND attname<>'created_at';
        EXECUTE format('SELECT EXISTS((SELECT %s FROM __Q__.%I EXCEPT ALL SELECT %s FROM %s WHERE snapshot_key=$1) '
          'UNION ALL (SELECT %s FROM %s WHERE snapshot_key=$1 EXCEPT ALL SELECT %s FROM __Q__.%I))',
          columns,p_candidate,columns,reference_table,columns,reference_table,columns,p_candidate) INTO invalid USING c.snapshot_key;
        IF NOT invalid THEN
            UPDATE __S__.ptg2_snapshot_candidate SET prepared_xid=pg_current_xact_id()
              WHERE parent_name=c.parent_name AND snapshot_key=c.snapshot_key AND prepared AND NOT published;
            EXECUTE format('DROP TABLE __Q__.%I',p_candidate);
            DELETE FROM __S__.ptg2_snapshot_candidate WHERE table_oid=c.table_oid;
            RETURN observed;
        END IF;
        RAISE EXCEPTION 'ptg_snapshot_candidate_replay' USING ERRCODE='23514';
    END IF;
    EXECUTE format('ANALYZE __Q__.%I',p_candidate);
    UPDATE __S__.ptg2_snapshot_candidate SET prepared=true,prepared_xid=pg_current_xact_id() WHERE table_oid=c.table_oid;
    RETURN observed;
END $body$;

CREATE FUNCTION __S__.attach_ptg_snapshot_candidates(p_snapshot bigint, p_token text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog SET lock_timeout='1s' AS $body$
DECLARE candidate __S__.ptg2_snapshot_candidate; caller name; attached bigint:=0;
BEGIN
    caller:=COALESCE(NULLIF(current_setting('role'),'none'),session_user);
    PERFORM pg_advisory_xact_lock_shared(hashtext('ptg2_source_pointer_gc_v1'));
    PERFORM 1 FROM __S__.ptg2_v3_snapshot_layout l
      WHERE snapshot_key=p_snapshot AND build_token=p_token AND generation='shared_blocks_v4' FOR UPDATE;
    IF NOT FOUND OR EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_candidate WHERE snapshot_key=p_snapshot
      AND (build_token<>p_token OR writer_name<>caller OR NOT prepared
           OR NOT __S__.ptg_snapshot_writer_authorized(parent_name,caller))) THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_authority' USING ERRCODE='42501';
    END IF;
    IF EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_candidate WHERE snapshot_key=p_snapshot
        AND NOT published AND prepared_xid IS DISTINCT FROM pg_current_xact_id()) THEN
        RAISE EXCEPTION 'ptg_snapshot_candidate_validation_expired' USING ERRCODE='55000';
    END IF;
    -- The complete frozen family has already passed its indexed set checks.
    -- Acquire every parent before the first attach, with no queued lock waiter.
    FOR candidate IN SELECT * FROM __S__.ptg2_snapshot_candidate
      WHERE snapshot_key=p_snapshot AND NOT published ORDER BY parent_name FOR UPDATE LOOP
        IF to_regclass(format('__Q__.%I',candidate.table_name)) IS DISTINCT FROM candidate.table_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
        EXECUTE format('LOCK TABLE ONLY __Q__.%I IN SHARE UPDATE EXCLUSIVE MODE NOWAIT',candidate.parent_name);
    END LOOP;
    FOR candidate IN SELECT * FROM __S__.ptg2_snapshot_candidate
      WHERE snapshot_key=p_snapshot AND NOT published ORDER BY parent_name LOOP
        EXECUTE format('ALTER TABLE __Q__.%I ATTACH PARTITION __Q__.%I FOR VALUES IN (%s)',
          candidate.parent_name,candidate.table_name,candidate.snapshot_key);
        UPDATE __S__.ptg2_snapshot_candidate SET published=true WHERE table_oid=candidate.table_oid;
        attached:=attached+1;
    END LOOP;
    RETURN attached;
END $body$;

CREATE FUNCTION __S__.cleanup_ptg_snapshot_candidates()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog SET lock_timeout='1s' AS $body$
DECLARE candidate record; parent text; history record; has_rows boolean;
BEGIN
    FOR history IN SELECT table_name,plan,parent_oid FROM __S__.ptg2_snapshot_partition_preparation LOOP
        IF to_regclass(format('__Q__.%I',history.table_name||'_history'))::oid IS DISTINCT FROM (history.plan->>'oid')::oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM ONLY __Q__.%I WHERE snapshot_key=$1)',history.table_name||'_history')
          INTO has_rows USING OLD.snapshot_key;
        IF has_rows THEN RAISE EXCEPTION 'ptg_snapshot_history_cleanup_pending' USING ERRCODE='55000'; END IF;
    END LOOP;
    -- Never queue an exclusive parent lock behind incumbent readers. A busy
    -- parent aborts this GC transaction; the unchanged layout is retried later.
    FOR parent IN SELECT DISTINCT parent_name FROM __S__.ptg2_snapshot_candidate
      WHERE snapshot_key=OLD.snapshot_key AND published ORDER BY parent_name LOOP
        EXECUTE format('LOCK TABLE ONLY __Q__.%I IN ACCESS EXCLUSIVE MODE NOWAIT',parent);
    END LOOP;
    FOR candidate IN SELECT c.* FROM __S__.ptg2_snapshot_candidate c
      WHERE c.snapshot_key=OLD.snapshot_key ORDER BY c.table_oid FOR UPDATE OF c LOOP
        IF to_regclass(format('__Q__.%I',candidate.table_name)) IS DISTINCT FROM candidate.table_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_candidate_identity' USING ERRCODE='55000';
        END IF;
        IF candidate.published THEN
            -- Eligibility is established by layout GC; this exact private leaf may be dropped whole.
            EXECUTE format('ALTER TABLE __Q__.%I DETACH PARTITION __Q__.%I',candidate.parent_name,candidate.table_name);
        END IF;
        EXECUTE format('DROP TABLE __Q__.%I',candidate.table_name);
        DELETE FROM __S__.ptg2_snapshot_candidate WHERE table_oid=candidate.table_oid;
    END LOOP;
    DELETE FROM __S__.ptg2_snapshot_legacy_build WHERE snapshot_key=OLD.snapshot_key;
    DELETE FROM __S__.ptg2_snapshot_completion_receipt WHERE snapshot_key=OLD.snapshot_key;
    RETURN NULL;
END $body$;
