-- No canonical hot row is backfilled, copied, activated or discarded here.
DO $cutover$
DECLARE table_name text; owner_oid oid; relation_oid oid;
BEGIN
    SELECT proowner INTO owner_oid FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    IF owner_oid IS DISTINCT FROM (SELECT oid FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'custom_import_cutover_owner_mismatch'; END IF;
    -- Existing writers first take these authority locks. Wait for them, then
    -- prevent a new producer or finality transition throughout this transaction.
    LOCK TABLE __CONTROL__.custom_import_dataset,__CONTROL__.custom_import_execution,
        __CONTROL__.custom_import_lease,__CONTROL__.custom_import_build_attempt,
        __CONTROL__.custom_import_build_stream,__CONTROL__.custom_import_generation,
        __CONTROL__.custom_import_generation_seal,__CONTROL__.custom_import_no_change_seal
        IN SHARE ROW EXCLUSIVE MODE;
    FOR table_name IN SELECT name FROM unnest(__CLOSED_TABLES__) names(name) ORDER BY name LOOP
        relation_oid:=to_regclass(format('%I.%I',__CONTROL_LITERAL__,table_name));
        IF relation_oid IS NULL OR NOT EXISTS(SELECT 1 FROM pg_class c
            WHERE c.oid=relation_oid AND c.relowner=owner_oid AND c.relkind='r' AND c.relpersistence='p'
                AND NOT c.relispartition AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)) THEN
            RAISE EXCEPTION 'custom_import_cutover_relation_mismatch: %',table_name; END IF;
        EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',__CONTROL_LITERAL__,table_name);
    END LOOP;
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_attempt b
        JOIN __CONTROL__.custom_import_execution e ON e.execution_id=b.execution_id
        LEFT JOIN __CONTROL__.custom_import_snapshot_family f
            ON (f.execution_id,f.producing_fence)=(b.execution_id,b.producing_fence)
        WHERE f.family_id IS NULL AND e.state='running'
            AND NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal s
                JOIN __CONTROL__.custom_import_generation g ON g.generation_id=s.generation_id
                WHERE (g.execution_id,g.producing_fence)=(b.execution_id,b.producing_fence))
            AND (b.phase<>'source'
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_stream x WHERE x.build_id=b.build_id
                    AND (x.next_part_ordinal>1 OR x.next_part_row_ordinal>0 OR x.next_source_ordinal>0
                        OR x.next_pack_ordinal>0 OR x.replay_verified_at IS NOT NULL))
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_occurrence x WHERE x.build_id=b.build_id)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_family x WHERE x.build_id=b.build_id)
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_build_candidate_context x WHERE x.build_id=b.build_id))) THEN
        RAISE EXCEPTION 'custom_import_cutover_active_canonical_build'; END IF;
    -- A registered producer must not be split either. Sealed canonical BASE
    -- inputs remain immutable historical inputs and are intentionally allowed.
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_execution e
        JOIN __CONTROL__.custom_import_lease l ON l.execution_id=e.execution_id
        WHERE e.state='running' AND l.fence>0
            AND NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_seal s
                JOIN __CONTROL__.custom_import_generation g ON g.generation_id=s.generation_id
                WHERE (g.execution_id,g.producing_fence)=(e.execution_id,l.fence))
            AND NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_no_change_seal s
                WHERE (s.execution_id,s.sealing_fence)=(e.execution_id,l.fence))
            AND (EXISTS(SELECT 1 FROM __CONTROL__.custom_import_pack x
                    WHERE (x.execution_id,x.producing_fence)=(e.execution_id,l.fence))
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_rejection x
                    WHERE (x.execution_id,x.producing_fence)=(e.execution_id,l.fence))
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_family_revision x
                    WHERE (x.producing_execution_id,x.producing_fence)=(e.execution_id,l.fence))
                OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation g
                    WHERE (g.execution_id,g.producing_fence)=(e.execution_id,l.fence)
                        AND (EXISTS(SELECT 1 FROM __CONTROL__.custom_import_generation_family x WHERE x.generation_id=g.generation_id)
                            OR EXISTS(SELECT 1 FROM __CONTROL__.custom_import_winner x WHERE x.generation_id=g.generation_id))))) THEN
        RAISE EXCEPTION 'custom_import_cutover_active_canonical_producer'; END IF;
    IF EXISTS(SELECT 1 FROM __CONTROL__.custom_import_materialization_page) THEN
        RAISE EXCEPTION 'custom_import_cutover_pending_materialization'; END IF;
END $cutover$;
