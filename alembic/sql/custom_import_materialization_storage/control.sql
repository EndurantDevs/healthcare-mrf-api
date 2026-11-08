CREATE TABLE __CONTROL__.custom_import_materialization_page (
    page_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    dataset_id bigint NOT NULL,
    root_revision_ids bigint[] NOT NULL,
    child_revision_ids bigint[] NOT NULL,
    generation_id bigint,
    winner_profile_slots smallint[] NOT NULL,
    winner_entity_ids bigint[] NOT NULL,
    winner_context_hashes bytea[] NOT NULL,
    CONSTRAINT custom_import_materialization_page_bounds CHECK (
        cardinality(root_revision_ids)+cardinality(child_revision_ids)+cardinality(winner_profile_slots) BETWEEN 1 AND 256
        AND cardinality(winner_entity_ids)=cardinality(winner_profile_slots)
        AND cardinality(winner_context_hashes)=cardinality(winner_profile_slots))
);
-- statement boundary --
CREATE FUNCTION __CONTROL__.custom_import_materialization_lineage(
    p_roots bigint[],p_children bigint[],p_generation bigint
) RETURNS TABLE(dataset_id bigint,definition_revision_id bigint,schema_revision_id bigint,
    execution_id bigint,capture_bundle_id bigint,fence bigint,token_sha256 bytea)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    SELECT DISTINCT a.dataset_id,a.definition_revision_id,a.schema_revision_id,a.execution_id,
        a.capture_bundle_id,a.fence,a.token_sha256
    FROM __CONTROL__.custom_import_materialization_origins(p_roots,p_children) a
    UNION
    SELECT g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,
        g.capture_bundle_id,g.producing_fence,g.producing_token_sha256
    FROM __CONTROL__.custom_import_generation g WHERE g.generation_id=p_generation
$fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.custom_import_materialization_budget(
    p_dataset bigint, p_roots bigint[], p_children bigint[], p_generation bigint,
    p_expected bigint[], p_expected_token bytea, p_expected_deadline timestamptz
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE budget numeric; existing_timeout numeric;
BEGIN
    IF p_dataset IS NULL OR p_dataset<=0 OR p_roots IS NULL OR p_children IS NULL
        OR cardinality(p_roots)+cardinality(p_children)>256 THEN
        RAISE EXCEPTION 'custom_import_materialization_scope_bounds'; END IF;
    -- This read only sets a statement budget. It does not grant or cache authority.
    SELECT min(least(extract(epoch FROM (least(l.expires_at,b.build_deadline_at,p_expected_deadline)
        -clock_timestamp()))*500,b.statement_timeout_ms,2147483647)) INTO budget
    FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation) a
    JOIN __CONTROL__.custom_import_lease l ON l.execution_id=a.execution_id
    LEFT JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=a.execution_id AND b.producing_fence=a.fence
    WHERE a.dataset_id=p_dataset;
    IF budget IS NULL OR budget<1 THEN RAISE EXCEPTION 'custom_import_materialization_deadline'; END IF;
    existing_timeout:=extract(epoch FROM current_setting('statement_timeout')::interval)*1000;
    IF existing_timeout>0 THEN budget:=least(budget,existing_timeout); END IF;
    RETURN floor(budget)::integer;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.lock_custom_import_materialization_set(
    p_dataset bigint, p_roots bigint[], p_children bigint[], p_generation bigint,
    p_expected bigint[], p_expected_token bytea, p_expected_deadline timestamptz
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE owner_name name; now_at timestamptz; timeout_ms numeric;
BEGIN
    SELECT pg_get_userbyid(proowner) INTO owner_name FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF current_user IS DISTINCT FROM owner_name THEN
        RAISE EXCEPTION 'custom_import_materialization_owner_mismatch'; END IF;
    IF num_nonnulls(p_expected,p_expected_token,p_expected_deadline) NOT IN (0,3)
        OR (p_expected IS NOT NULL AND (cardinality(p_expected)<>6 OR array_ndims(p_expected)<>1
            OR array_lower(p_expected,1)<>1 OR array_position(p_expected,NULL) IS NOT NULL
            OR 0>=ANY(p_expected) OR octet_length(p_expected_token)<>32 OR NOT isfinite(p_expected_deadline))) THEN
        RAISE EXCEPTION 'custom_import_materialization_expected_shape'; END IF;
    IF p_dataset IS NULL OR p_dataset<=0 OR p_roots IS NULL OR p_children IS NULL
        OR cardinality(p_roots)+cardinality(p_children)>256 THEN
        RAISE EXCEPTION 'custom_import_materialization_scope_bounds'; END IF;
    PERFORM 1 FROM __CONTROL__.custom_import_dataset d WHERE d.dataset_id=p_dataset FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'custom_import_materialization_dataset_missing'; END IF;
    -- No record loop: lock each distinct authority parent in deterministic order.
    PERFORM e.execution_id FROM __CONTROL__.custom_import_execution e WHERE e.execution_id IN (
        SELECT a.execution_id FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation) a
    ) ORDER BY e.execution_id FOR UPDATE OF e;
    PERFORM l.execution_id FROM __CONTROL__.custom_import_lease l WHERE l.execution_id IN (
        SELECT a.execution_id FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation) a
    ) ORDER BY l.execution_id FOR UPDATE OF l;
    PERFORM b.build_id FROM __CONTROL__.custom_import_build_attempt b WHERE (b.execution_id,b.producing_fence) IN (
        SELECT a.execution_id,a.fence FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation) a
    ) ORDER BY b.execution_id,b.producing_fence FOR UPDATE OF b;
    now_at:=clock_timestamp();
    timeout_ms:=extract(epoch FROM current_setting('statement_timeout')::interval)*1000;
    IF NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation))
        OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_materialization_lineage(p_roots,p_children,p_generation) a
        LEFT JOIN __CONTROL__.custom_import_execution e ON e.execution_id=a.execution_id
        LEFT JOIN __CONTROL__.custom_import_lease l ON l.execution_id=a.execution_id
        LEFT JOIN __CONTROL__.custom_import_build_attempt b ON b.execution_id=a.execution_id AND b.producing_fence=a.fence
        WHERE a.dataset_id IS DISTINCT FROM p_dataset
            OR ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id,e.state,l.fence,l.token_sha256)
                IS DISTINCT FROM ROW(a.dataset_id,a.definition_revision_id,a.schema_revision_id,a.capture_bundle_id,'running'::varchar,a.fence,a.token_sha256)
            OR a.fence IS NULL OR a.fence<=0 OR a.token_sha256 IS NULL OR octet_length(a.token_sha256)<>32
            OR l.expires_at IS NULL OR least(l.expires_at,b.build_deadline_at,p_expected_deadline)<=now_at
            OR (b.build_id IS NOT NULL AND ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.capture_bundle_id,
                b.producing_token_sha256,b.request_identity_sha256) IS DISTINCT FROM
                ROW(a.dataset_id,a.definition_revision_id,a.schema_revision_id,a.capture_bundle_id,a.token_sha256,e.request_identity_sha256))
            OR (p_expected IS NOT NULL AND (ARRAY[a.dataset_id,a.definition_revision_id,a.schema_revision_id,a.execution_id,
                a.capture_bundle_id,a.fence] IS DISTINCT FROM p_expected OR a.token_sha256 IS DISTINCT FROM p_expected_token
                OR p_expected_deadline>l.expires_at))
            OR timeout_ms<=0 OR timeout_ms>=extract(epoch FROM (least(l.expires_at,b.build_deadline_at,p_expected_deadline)-now_at))*1000
            OR (b.build_id IS NOT NULL AND timeout_ms>b.statement_timeout_ms)
            OR (b.build_id IS NOT NULL AND current_setting('transaction_isolation')<>'read committed')
            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal s WHERE s.execution_id=a.execution_id)
            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal s WHERE s.execution_id=a.execution_id)) THEN
        RAISE EXCEPTION 'custom_import_materialization_authority_lost'; END IF;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.finish_custom_import_materialization_page() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE group_row record; namespace text; g __CONTROL__.custom_import_generation;
    f __CONTROL__.custom_import_snapshot_family;
BEGIN
    IF TG_RELID<>'__CONTROL__.custom_import_materialization_page'::regclass OR TG_OP<>'INSERT'
        OR TG_WHEN<>'AFTER' OR TG_LEVEL<>'ROW' OR NEW.transaction_id<>pg_current_xact_id() THEN
        RAISE EXCEPTION 'custom_import_materialization_header_mismatch'; END IF;
    -- Completion follows immutable homes but deliberately does not renew or
    -- recheck a lease. Same-transaction sealing and expiry after page success
    -- retain the original exported transaction contract.
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_materialization_origins(
        NEW.root_revision_ids,NEW.child_revision_ids) a WHERE a.execution_id IS NULL) THEN
        RAISE EXCEPTION 'custom_import_materialization_header_origin_missing'; END IF;
    FOR group_row IN
        SELECT a.family_id,coalesce(array_agg(a.revision_id) FILTER(WHERE a.revision_kind=1),'{}') root_ids,
            coalesce(array_agg(a.revision_id) FILTER(WHERE a.revision_kind=2),'{}') child_ids
        FROM __CONTROL__.custom_import_materialization_origins(NEW.root_revision_ids,NEW.child_revision_ids) a
        GROUP BY a.family_id ORDER BY a.family_id NULLS FIRST
    LOOP
        PERFORM __CONTROL__.lock_custom_import_materialization_storage(group_row.family_id,false);
        IF group_row.family_id IS NOT NULL THEN
            PERFORM __CONTROL__.verify_custom_import_materialization_writers(group_row.family_id);
        END IF;
        namespace:=CASE WHEN group_row.family_id IS NULL THEN __CONTROL_LITERAL__
            ELSE 'ci_snapshot_'||group_row.family_id::text END;
        EXECUTE format('SELECT %I.check_custom_import_materialization_completion($1,$2,$3,$4,$5,$6)',namespace)
            USING group_row.root_ids,group_row.child_ids,NULL::bigint,'{}'::smallint[],'{}'::bigint[],'{}'::bytea[];
    END LOOP;
    IF NEW.generation_id IS NOT NULL THEN
        SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=NEW.generation_id;
        SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family
            WHERE execution_id=g.execution_id AND producing_fence=g.producing_fence;
        IF g.generation_id IS NULL OR g.dataset_id IS DISTINCT FROM NEW.dataset_id OR
            (f.family_id IS NOT NULL AND ROW(f.generation_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
                f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) IS DISTINCT FROM
                ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
                    g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256)) THEN
            RAISE EXCEPTION 'custom_import_materialization_header_generation_mismatch'; END IF;
        PERFORM __CONTROL__.lock_custom_import_materialization_storage(f.family_id,false);
        IF f.family_id IS NOT NULL THEN PERFORM __CONTROL__.verify_custom_import_materialization_writers(f.family_id); END IF;
        namespace:=CASE WHEN f.family_id IS NULL THEN __CONTROL_LITERAL__ ELSE 'ci_snapshot_'||f.family_id::text END;
        EXECUTE format('SELECT %I.check_custom_import_materialization_completion($1,$2,$3,$4,$5,$6)',namespace)
            USING '{}'::bigint[],'{}'::bigint[],NEW.generation_id,
                NEW.winner_profile_slots,NEW.winner_entity_ids,NEW.winner_context_hashes;
    END IF;
    DELETE FROM __CONTROL__.custom_import_materialization_page
        WHERE page_id=NEW.page_id AND transaction_id=pg_current_xact_id();
    RETURN NULL;
END $fn$;
-- statement boundary --
CREATE CONSTRAINT TRIGGER custom_import_materialization_page_completion
    AFTER INSERT ON __CONTROL__.custom_import_materialization_page DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION __CONTROL__.finish_custom_import_materialization_page();
-- statement boundary --
ALTER TABLE __CONTROL__.custom_import_materialization_page ENABLE ALWAYS TRIGGER custom_import_materialization_page_completion;
-- statement boundary --
CREATE FUNCTION __CONTROL__.persist_custom_import_scalar_set(
    p_dataset bigint, p_schema bigint, p_revision_ids bigint[], p_root_ids bigint[], p_collection_slots smallint[],
    p_field_slots smallint[], p_projection_slots smallint[], p_field_types text[], p_value_states text[],
    p_strings text[], p_integers bigint[], p_decimals numeric[], p_booleans boolean[], p_dates date[], p_timestamps timestamptz[],
    p_expected bigint[], p_expected_token bytea, p_expected_deadline timestamptz
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; roots __CONTROL__.custom_import_root_scalar[]; children __CONTROL__.custom_import_child_scalar[];
    root_ids bigint[]; child_ids bigint[]; group_row record; namespace text; answer integer;
    part_roots __CONTROL__.custom_import_root_scalar[]; part_children __CONTROL__.custom_import_child_scalar[];
BEGIN
    n:=cardinality(p_revision_ids);
    IF n IS NULL OR n NOT BETWEEN 1 AND 256 OR p_dataset IS NULL OR p_schema IS NULL OR least(p_dataset,p_schema)<=0
        OR EXISTS(SELECT 1 FROM (VALUES
            (cardinality(p_revision_ids),array_ndims(p_revision_ids),array_lower(p_revision_ids,1)),
            (cardinality(p_root_ids),array_ndims(p_root_ids),array_lower(p_root_ids,1)),
            (cardinality(p_collection_slots),array_ndims(p_collection_slots),array_lower(p_collection_slots,1)),
            (cardinality(p_field_slots),array_ndims(p_field_slots),array_lower(p_field_slots,1)),
            (cardinality(p_projection_slots),array_ndims(p_projection_slots),array_lower(p_projection_slots,1)),
            (cardinality(p_field_types),array_ndims(p_field_types),array_lower(p_field_types,1)),
            (cardinality(p_value_states),array_ndims(p_value_states),array_lower(p_value_states,1)),
            (cardinality(p_strings),array_ndims(p_strings),array_lower(p_strings,1)),
            (cardinality(p_integers),array_ndims(p_integers),array_lower(p_integers,1)),
            (cardinality(p_decimals),array_ndims(p_decimals),array_lower(p_decimals,1)),
            (cardinality(p_booleans),array_ndims(p_booleans),array_lower(p_booleans,1)),
            (cardinality(p_dates),array_ndims(p_dates),array_lower(p_dates,1)),
            (cardinality(p_timestamps),array_ndims(p_timestamps),array_lower(p_timestamps,1))
        ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1))
        OR pg_column_size(ROW(p_revision_ids,p_root_ids,p_collection_slots,p_field_slots,p_projection_slots,
            p_field_types,p_value_states,p_strings,p_integers,p_decimals,p_booleans,p_dates,p_timestamps))>8388608 THEN
        RAISE EXCEPTION 'custom_import_scalar_set_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_revision_ids,p_root_ids,p_collection_slots,p_field_slots,p_projection_slots,p_field_types,p_value_states,p_strings,p_decimals,p_dates,p_timestamps)
        x(revision_id,root_id,collection_slot,field_slot,projection_slot,field_type,value_state,string_value,decimal_value,date_value,timestamp_value)
        WHERE num_nonnulls(x.revision_id,x.root_id,x.collection_slot,x.field_slot,x.projection_slot,x.field_type,x.value_state)<>7
            OR least(x.revision_id,x.root_id,x.field_slot,x.projection_slot)<=0 OR x.collection_slot<0
            OR x.field_type NOT IN ('string','integer','decimal','boolean','date','timestamp') OR x.value_state NOT IN ('value','null')
            OR octet_length(x.string_value)>2048
            OR (x.decimal_value IS NOT NULL AND (x.decimal_value::text IN ('NaN','Infinity','-Infinity')
                OR abs(x.decimal_value)>=1000000000000000000 OR round(x.decimal_value,12)<>x.decimal_value))
            OR (x.date_value IS NOT NULL AND NOT isfinite(x.date_value)) OR (x.timestamp_value IS NOT NULL AND NOT isfinite(x.timestamp_value)))
        OR (SELECT count(DISTINCT (x.revision_id,x.collection_slot=0,x.field_slot))
            FROM unnest(p_revision_ids,p_collection_slots,p_field_slots) x(revision_id,collection_slot,field_slot))<>n THEN
        RAISE EXCEPTION 'custom_import_scalar_set_value_or_identity'; END IF;
    SELECT coalesce(array_agg(ROW(x.revision_id,p_dataset,p_schema,x.root_id,x.field_slot,0,x.projection_slot,x.field_type,
        x.value_state,x.string_value,x.integer_value,x.decimal_value,x.boolean_value,x.date_value,x.timestamp_value)::__CONTROL__.custom_import_root_scalar),'{}') INTO roots
        FROM unnest(p_revision_ids,p_root_ids,p_collection_slots,p_field_slots,p_projection_slots,p_field_types,p_value_states,p_strings,p_integers,p_decimals,p_booleans,p_dates,p_timestamps)
        x(revision_id,root_id,collection_slot,field_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        WHERE x.collection_slot=0;
    SELECT coalesce(array_agg(ROW(x.revision_id,p_dataset,p_schema,x.root_id,x.collection_slot,x.field_slot,x.collection_slot,x.projection_slot,x.field_type,
        x.value_state,x.string_value,x.integer_value,x.decimal_value,x.boolean_value,x.date_value,x.timestamp_value)::__CONTROL__.custom_import_child_scalar),'{}') INTO children
        FROM unnest(p_revision_ids,p_root_ids,p_collection_slots,p_field_slots,p_projection_slots,p_field_types,p_value_states,p_strings,p_integers,p_decimals,p_booleans,p_dates,p_timestamps)
        x(revision_id,root_id,collection_slot,field_slot,projection_slot,field_type,value_state,string_value,integer_value,decimal_value,boolean_value,date_value,timestamp_value)
        WHERE x.collection_slot>0;
    SELECT coalesce(array_agg(DISTINCT x.root_revision_id ORDER BY x.root_revision_id),'{}') INTO root_ids FROM unnest(roots) x;
    SELECT coalesce(array_agg(DISTINCT x.child_revision_id ORDER BY x.child_revision_id),'{}') INTO child_ids FROM unnest(children) x;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(p_dataset,root_ids,child_ids,NULL,p_expected,p_expected_token,p_expected_deadline);
    FOR group_row IN
        SELECT a.family_id,coalesce(array_agg(a.revision_id) FILTER(WHERE a.revision_kind=1),'{}') root_ids,
            coalesce(array_agg(a.revision_id) FILTER(WHERE a.revision_kind=2),'{}') child_ids
        FROM __CONTROL__.custom_import_materialization_origins(root_ids,child_ids) a
        GROUP BY a.family_id ORDER BY min(a.execution_id),a.family_id NULLS FIRST
    LOOP
        PERFORM __CONTROL__.lock_custom_import_materialization_storage(group_row.family_id,true);
        IF group_row.family_id IS NOT NULL THEN
            PERFORM __CONTROL__.install_custom_import_materialization_writers(group_row.family_id);
        END IF;
        namespace:=CASE WHEN group_row.family_id IS NULL THEN __CONTROL_LITERAL__
            ELSE 'ci_snapshot_'||group_row.family_id::text END;
        SELECT coalesce(array_agg(x),'{}') INTO part_roots
            FROM unnest(roots) x WHERE x.root_revision_id=ANY(group_row.root_ids);
        SELECT coalesce(array_agg(x),'{}') INTO part_children
            FROM unnest(children) x WHERE x.child_revision_id=ANY(group_row.child_ids);
        EXECUTE format('SELECT %I.persist_custom_import_scalar_models($1,$2,$3,$4,$5,$6,$7)',namespace)
            INTO answer USING p_dataset,p_schema,part_roots,part_children,p_expected,p_expected_token,p_expected_deadline;
        IF answer IS DISTINCT FROM cardinality(part_roots)+cardinality(part_children) THEN
            RAISE EXCEPTION 'custom_import_scalar_set_count_mismatch'; END IF;
    END LOOP;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(
        p_dataset,root_ids,child_ids,NULL,p_expected,p_expected_token,p_expected_deadline);
    INSERT INTO __CONTROL__.custom_import_materialization_page(dataset_id,root_revision_ids,child_revision_ids,
        winner_profile_slots,winner_entity_ids,winner_context_hashes)
        VALUES(p_dataset,root_ids,child_ids,'{}','{}','{}');
    RETURN n;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.persist_custom_import_winner_set(
    p_generation bigint, p_dataset bigint, p_definition bigint, p_schema bigint,
    p_profile_slots smallint[], p_entity_ids bigint[], p_family_ids bigint[], p_context_slots smallint[],
    p_context_hashes bytea[], p_child_ids bigint[], p_context_keys text[],
    p_expected bigint[], p_expected_token bytea, p_expected_deadline timestamptz
) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE n integer; selected __CONTROL__.custom_import_winner[]; g __CONTROL__.custom_import_generation;
    f __CONTROL__.custom_import_snapshot_family; namespace text; answer integer;
BEGIN
    n:=cardinality(p_profile_slots);
    IF num_nonnulls(p_generation,p_dataset,p_definition,p_schema)<>4 OR least(p_generation,p_dataset,p_definition,p_schema)<=0
        OR n IS NULL OR n NOT BETWEEN 1 AND 256 OR EXISTS(SELECT 1 FROM (VALUES
            (cardinality(p_profile_slots),array_ndims(p_profile_slots),array_lower(p_profile_slots,1)),
            (cardinality(p_entity_ids),array_ndims(p_entity_ids),array_lower(p_entity_ids,1)),
            (cardinality(p_family_ids),array_ndims(p_family_ids),array_lower(p_family_ids,1)),
            (cardinality(p_context_slots),array_ndims(p_context_slots),array_lower(p_context_slots,1)),
            (cardinality(p_context_hashes),array_ndims(p_context_hashes),array_lower(p_context_hashes,1)),
            (cardinality(p_child_ids),array_ndims(p_child_ids),array_lower(p_child_ids,1)),
            (cardinality(p_context_keys),array_ndims(p_context_keys),array_lower(p_context_keys,1))
        ) a(size,dimensions,first_index) WHERE ROW(a.size,a.dimensions,a.first_index) IS DISTINCT FROM ROW(n,1,1))
        OR pg_column_size(ROW(p_profile_slots,p_entity_ids,p_family_ids,p_context_slots,p_context_hashes,p_child_ids,p_context_keys))>8388608 THEN
        RAISE EXCEPTION 'custom_import_winner_set_bounds'; END IF;
    IF EXISTS(SELECT 1 FROM unnest(p_profile_slots,p_entity_ids,p_family_ids,p_context_slots,p_context_hashes,p_child_ids,p_context_keys)
        x(profile_slot,entity_id,family_id,context_slot,context_hash,child_id,canonical)
        WHERE num_nonnulls(x.profile_slot,x.entity_id,x.family_id,x.context_slot,x.context_hash,x.canonical)<>6
            OR x.profile_slot NOT BETWEEN 1 AND 4 OR least(x.entity_id,x.family_id)<=0 OR x.context_slot<0
            OR (x.context_slot=0 AND x.child_id IS NOT NULL) OR (x.context_slot>0 AND (x.child_id IS NULL OR x.child_id<=0))
            OR octet_length(x.canonical) NOT BETWEEN 1 AND 8192 OR octet_length(x.context_hash)<>32
            OR x.context_hash IS DISTINCT FROM sha256(decode('637573746f6d2d696d706f72742f76310077696e6e65722d636f6e7465787400','hex')||convert_to(x.canonical,'UTF8')))
        OR (SELECT count(DISTINCT (x.profile_slot,x.entity_id,x.context_hash))
            FROM unnest(p_profile_slots,p_entity_ids,p_context_hashes) x(profile_slot,entity_id,context_hash))<>n THEN
        RAISE EXCEPTION 'custom_import_winner_set_context'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(p_dataset,'{}','{}',p_generation,p_expected,p_expected_token,p_expected_deadline);
    SELECT * INTO g FROM __CONTROL__.custom_import_generation WHERE generation_id=p_generation;
    IF ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id) IS DISTINCT FROM ROW(p_dataset,p_definition,p_schema) THEN
        RAISE EXCEPTION 'custom_import_winner_set_generation'; END IF;
    SELECT array_agg(ROW(p_generation,p_dataset,p_definition,p_schema,x.profile_slot,x.entity_id,x.family_id,x.context_slot,x.context_hash,x.child_id)::__CONTROL__.custom_import_winner)
        INTO selected FROM unnest(p_profile_slots,p_entity_ids,p_family_ids,p_context_slots,p_context_hashes,p_child_ids)
        x(profile_slot,entity_id,family_id,context_slot,context_hash,child_id);
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family
        WHERE execution_id=g.execution_id AND producing_fence=g.producing_fence;
    IF f.family_id IS NOT NULL AND ROW(f.generation_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
        f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) IS DISTINCT FROM
        ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
            g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256) THEN
        RAISE EXCEPTION 'custom_import_winner_set_storage_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(f.family_id,true);
    IF f.family_id IS NOT NULL THEN
        PERFORM __CONTROL__.install_custom_import_materialization_writers(f.family_id);
    END IF;
    namespace:=CASE WHEN f.family_id IS NULL THEN __CONTROL_LITERAL__ ELSE 'ci_snapshot_'||f.family_id::text END;
    EXECUTE format('SELECT %I.persist_custom_import_winner_models($1,$2,$3,$4,$5,$6,$7,$8)',namespace)
        INTO answer USING p_generation,p_dataset,p_definition,p_schema,selected,p_expected,p_expected_token,p_expected_deadline;
    IF answer IS DISTINCT FROM n THEN RAISE EXCEPTION 'custom_import_winner_set_count_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_materialization_set(
        p_dataset,'{}','{}',p_generation,p_expected,p_expected_token,p_expected_deadline);
    INSERT INTO __CONTROL__.custom_import_materialization_page(dataset_id,root_revision_ids,child_revision_ids,generation_id,
        winner_profile_slots,winner_entity_ids,winner_context_hashes)
        VALUES(p_dataset,'{}','{}',p_generation,p_profile_slots,p_entity_ids,p_context_hashes);
    RETURN n;
END $fn$;
