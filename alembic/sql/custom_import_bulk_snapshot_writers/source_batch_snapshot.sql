-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __CONTROL__.resolve_custom_import_source_batch_snapshot(p_batch uuid) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE a __CONTROL__.source_bulk_authorization; b __CONTROL__.custom_import_build_attempt;
    s __CONTROL__.custom_import_build_stream; f __CONTROL__.custom_import_snapshot_family;
    result bigint; namespace text;
BEGIN
    SELECT * INTO a FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch;
    IF a.batch_id IS NULL OR NOT a.accepting OR a.opened_by<>session_user
        OR a.transaction_id<>pg_current_xact_id() THEN
        RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    b:=__CONTROL__.lock_custom_import_build(a.build_id);
    result:=__CONTROL__.lock_custom_import_writable_snapshot(b.execution_id,a.fence,a.token_hash);
    SELECT * INTO a FROM __CONTROL__.source_bulk_authorization WHERE batch_id=p_batch FOR UPDATE;
    IF a.batch_id IS NULL OR NOT a.accepting OR a.opened_by<>session_user
        OR a.transaction_id<>pg_current_xact_id() OR b.phase<>'source'
        OR ROW(a.build_id,a.dataset_id,a.definition_revision_id,a.schema_revision_id,
            a.execution_id,a.capture_bundle_id,a.fence,a.token_hash)
            IS DISTINCT FROM ROW(b.build_id,b.dataset_id,b.definition_revision_id,b.schema_revision_id,
                b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256) THEN
        RAISE EXCEPTION 'source_bulk_authority_mismatch'; END IF;
    SELECT * INTO s FROM __CONTROL__.custom_import_build_stream
        WHERE build_id=b.build_id AND stream_slot=a.stream_slot FOR UPDATE;
    IF s.build_id IS NULL OR s.replay_verified_at IS NOT NULL
        OR ROW(s.next_pack_ordinal,s.next_part_ordinal,s.next_part_row_ordinal,s.next_source_ordinal)
            IS DISTINCT FROM ROW(a.first_pack,a.first_part,a.first_row,a.first_source)
        OR NOT EXISTS(SELECT 1 FROM __CONTROL__.custom_import_source_stream declared_stream
            WHERE declared_stream.definition_revision_id=b.definition_revision_id
                AND declared_stream.stream_slot=a.stream_slot AND declared_stream.dataset_id=b.dataset_id
                AND declared_stream.schema_revision_id=b.schema_revision_id) THEN
        RAISE EXCEPTION 'source_bulk_cursor_changed'; END IF;
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=result;
    IF f.family_id IS NULL OR f.frozen_at IS NOT NULL
        OR f.landing_table_oid IS NULL OR f.landing_table_owner IS NULL OR f.landing_columns_sha256 IS NULL
        OR ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,
            f.capture_bundle_id,f.producing_fence,f.producing_token_sha256)
            IS DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,b.execution_id,
                b.capture_bundle_id,b.producing_fence,b.producing_token_sha256) THEN
        RAISE EXCEPTION 'custom_import_source_snapshot_binding_mismatch'; END IF;
    namespace:='ci_snapshot_'||f.family_id::text;
    EXECUTE format('LOCK TABLE %I.source_bulk_landing IN ROW EXCLUSIVE MODE',namespace);
    IF NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=f.landing_table_oid::oid AND n.nspname=namespace AND c.relname='source_bulk_landing'
            AND c.relkind='r' AND c.relpersistence='p' AND c.relowner::bigint=f.landing_table_owner
            AND __CONTROL__.custom_import_snapshot_columns_sha256(f.landing_table_oid)=f.landing_columns_sha256
            AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
            AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
            AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)) THEN
        RAISE EXCEPTION 'custom_import_source_snapshot_landing_mismatch'; END IF;
    PERFORM __CONTROL__.lock_custom_import_build(b.build_id);
    RETURN result;
END $fn$;
