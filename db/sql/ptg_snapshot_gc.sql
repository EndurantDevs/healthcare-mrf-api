-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __S__.delete_ptg_snapshot_history(
    p_snapshot bigint, p_batch_rows integer, p_building_max_age_seconds integer,
    p_abandonment_token text DEFAULT NULL
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog SET lock_timeout='500ms' AS $body$
DECLARE caller name; eligible boolean; history record; history_name text; remaining boolean;
        all_empty boolean:=true;
BEGIN
    caller:=COALESCE(NULLIF(current_setting('role'),'none'),session_user);
    IF NOT EXISTS(SELECT 1 FROM __S__.ptg2_snapshot_lifecycle_writer authority
      WHERE pg_has_role(caller,authority.role_name,'USAGE')) THEN
        RAISE EXCEPTION 'ptg_snapshot_gc_authority' USING ERRCODE='42501';
    END IF;
    IF p_snapshot IS NULL OR p_snapshot<1 OR p_batch_rows IS NULL
       OR p_batch_rows NOT BETWEEN 1 AND 65536
       OR p_building_max_age_seconds IS NULL OR p_building_max_age_seconds<0 THEN
        RAISE EXCEPTION 'ptg_snapshot_gc_bounds' USING ERRCODE='22023';
    END IF;
    PERFORM pg_advisory_xact_lock_shared(hashtext('ptg2_source_pointer_gc_v1'));
    SELECT NOT EXISTS(SELECT 1 FROM __S__.ptg2_v3_snapshot_binding binding
                      WHERE binding.snapshot_key=layout.snapshot_key)
       AND NOT EXISTS(SELECT 1 FROM __S__.ptg2_block_build_pin pin
                      WHERE pin.snapshot_key=layout.snapshot_key
                        AND pin.lease_until>transaction_timestamp())
       AND CASE WHEN p_abandonment_token IS NOT NULL THEN
           layout.generation='shared_blocks_v4' AND layout.state='building'
           AND layout.build_token=p_abandonment_token
           AND p_abandonment_token ~ '^abandon-[0-9a-f]{64}$'
       ELSE
           (layout.state='sealed' AND COALESCE(layout.lease_until,'-infinity'::timestamptz)<=transaction_timestamp())
           OR (layout.state='building' AND (
               (EXISTS(SELECT 1 FROM __S__.ptg2_layout_build_candidate candidate
                        WHERE candidate.snapshot_key=layout.snapshot_key AND candidate.cleanup_pending_at IS NOT NULL)
                AND NOT EXISTS(SELECT 1 FROM __S__.ptg2_v3_layout_fingerprint fingerprint
                                WHERE fingerprint.snapshot_key=layout.snapshot_key))
               OR (layout.heartbeat_at<transaction_timestamp()-p_building_max_age_seconds*INTERVAL '1 second'
                   AND COALESCE(layout.lease_until,'-infinity'::timestamptz)<=transaction_timestamp())
           ))
       END INTO eligible
      FROM __S__.ptg2_v3_snapshot_layout layout
     WHERE layout.snapshot_key=p_snapshot
       AND layout.generation IN ('shared_blocks_v1','shared_blocks_v2','shared_blocks_v3','shared_blocks_v4')
     FOR UPDATE OF layout;
    IF eligible IS DISTINCT FROM true THEN RETURN false; END IF;
    FOR history IN SELECT table_name, (plan->>'oid')::oid AS table_oid, parent_oid
      FROM __S__.ptg2_snapshot_partition_preparation
      ORDER BY (table_name='ptg2_provider_group_tax_identity_source') DESC, ordinal LOOP
        history_name:=history.table_name||'_history';
        EXECUTE format('LOCK TABLE __Q__.%I IN ROW EXCLUSIVE MODE',history_name);
        IF to_regclass(format('__Q__.%I',history_name)) IS DISTINCT FROM history.table_oid
           OR to_regclass(format('__Q__.%I',history.table_name)) IS DISTINCT FROM history.parent_oid THEN
            RAISE EXCEPTION 'ptg_snapshot_gc_identity' USING ERRCODE='55000';
        END IF;
        EXECUTE format($sql$
            WITH selected AS MATERIALIZED (
                SELECT ctid FROM ONLY __Q__.%I WHERE snapshot_key=$1 LIMIT $2
            ) DELETE FROM ONLY __Q__.%I payload USING selected
                WHERE payload.ctid=selected.ctid AND payload.snapshot_key=$1
        $sql$,history_name,history_name) USING p_snapshot,p_batch_rows;
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM ONLY __Q__.%I WHERE snapshot_key=$1)',history_name)
            INTO remaining USING p_snapshot;
        all_empty:=all_empty AND NOT remaining;
    END LOOP;
    RETURN all_empty;
END $body$;
