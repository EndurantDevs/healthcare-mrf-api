CREATE FUNCTION __CONTROL__.verify_custom_import_materialization_writers(p_family_id bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE f __CONTROL__.custom_import_snapshot_family; namespace text; owner_oid oid;
BEGIN
    SELECT * INTO f FROM __CONTROL__.custom_import_snapshot_family WHERE family_id=p_family_id;
    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;
    namespace:='ci_snapshot_'||f.family_id::text;
    IF f.family_id IS NULL OR NOT EXISTS(
        SELECT 1 FROM pg_namespace n WHERE n.nspname=namespace AND n.nspowner=owner_oid)
        OR EXISTS(SELECT 1 FROM pg_namespace n,
            LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
            WHERE n.nspname=namespace AND a.grantee<>owner_oid AND a.privilege_type='CREATE') THEN
        RAISE EXCEPTION 'custom_import_materialization_writer_owner'; END IF;
    PERFORM __CONTROL__.resolve_custom_import_snapshot_relations(f.family_id);
    IF EXISTS(SELECT 1 FROM unnest(__SIGNATURES__,__RESULTS__) expected(signature,result)
        LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',namespace,expected.signature))
        WHERE p.oid IS NULL OR p.proowner<>owner_oid OR p.prokind<>'f' OR NOT p.prosecdef
            OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
            OR pg_get_function_result(p.oid) IS DISTINCT FROM expected.result
            OR EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
                WHERE a.grantee<>p.proowner)) THEN
        RAISE EXCEPTION 'custom_import_materialization_writer_signature'; END IF;
END $fn$;
-- statement boundary --
CREATE FUNCTION __CONTROL__.install_custom_import_materialization_writers(p_family_id bigint)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $materialization_install$
DECLARE namespace text; statement text; signature text; identity text; grantee record;
BEGIN
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(p_family_id,true);
    IF p_family_id IS NULL THEN RAISE EXCEPTION 'custom_import_materialization_storage_missing'; END IF;
    namespace:='ci_snapshot_'||p_family_id::text;
    -- A partial or mismatched prior installation is not silently replaced.
    IF EXISTS(SELECT 1 FROM unnest(__SIGNATURES__) s(signature)
        WHERE to_regprocedure(format('%I.%s',namespace,s.signature)) IS NOT NULL) THEN
        PERFORM __CONTROL__.verify_custom_import_materialization_writers(p_family_id);
        RETURN p_family_id;
    END IF;
    FOREACH statement IN ARRAY __LEAF_DDL__ LOOP
        EXECUTE replace(replace(statement,'__CANDIDATE__',quote_ident(namespace)),
            '__FAMILY_ID__',p_family_id::text||'::bigint');
    END LOOP;
    FOREACH signature IN ARRAY __SIGNATURES__ LOOP
        identity:=format('%I.%s',namespace,signature);
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',identity);
        FOR grantee IN SELECT DISTINCT a.grantee FROM pg_proc p,
            LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE p.oid=to_regprocedure(identity) AND a.grantee<>p.proowner AND a.grantee<>0
        LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I',identity,pg_get_userbyid(grantee.grantee));
        END LOOP;
    END LOOP;
    PERFORM __CONTROL__.verify_custom_import_materialization_writers(p_family_id);
    PERFORM __CONTROL__.lock_custom_import_materialization_storage(p_family_id,true);
    RETURN p_family_id;
END $materialization_install$;
