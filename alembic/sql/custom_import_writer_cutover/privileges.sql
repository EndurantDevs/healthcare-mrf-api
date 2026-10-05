-- Revoke at ACL grant roots: membership cannot recover a removed grant.
-- SELECT remains available. Role ownership, superusers and SET ROLE to the
-- migration owner are deliberately the trusted administrative boundary.
DO $cutover$
DECLARE target record; column_name text; grantee record; role_sql text;
BEGIN
    FOR target IN SELECT c.oid,c.relowner,n.nspname,c.relname,c.relacl
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__CLOSED_TABLES__)
        ORDER BY c.relname
    LOOP
        FOR grantee IN SELECT DISTINCT acl.grantee,r.rolname
            FROM aclexplode(coalesce(target.relacl,acldefault('r',target.relowner))) acl
            LEFT JOIN pg_roles r ON r.oid=acl.grantee
            WHERE acl.grantee<>target.relowner
            UNION SELECT 0::oid,NULL::name
        LOOP
            role_sql:=CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(grantee.rolname) END;
            EXECUTE format('REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN ON TABLE %I.%I FROM %s CASCADE',
                target.nspname,target.relname,role_sql);
        END LOOP;
        FOR column_name IN SELECT attname FROM pg_attribute
            WHERE attrelid=target.oid AND attnum>0 AND NOT attisdropped ORDER BY attnum
        LOOP
            FOR grantee IN SELECT DISTINCT acl.grantee,r.rolname
                FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
                LEFT JOIN pg_roles r ON r.oid=acl.grantee
                WHERE a.attrelid=target.oid AND a.attname=column_name AND acl.grantee<>target.relowner
                UNION SELECT 0::oid,NULL::name
            LOOP
                role_sql:=CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(grantee.rolname) END;
                EXECUTE format('REVOKE INSERT(%I),UPDATE(%I),REFERENCES(%I) ON TABLE %I.%I FROM %s CASCADE',
                    column_name,column_name,column_name,target.nspname,target.relname,role_sql);
            END LOOP;
        END LOOP;
    END LOOP;
    -- These are only sequences natively owned by a closed table column.
    FOR target IN SELECT DISTINCT s.oid,s.relowner,n.nspname,s.relname,s.relacl
        FROM pg_class s JOIN pg_namespace n ON n.oid=s.relnamespace
        JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=s.oid AND d.deptype IN ('a','i')
        JOIN pg_class t ON d.refclassid='pg_class'::regclass AND d.refobjid=t.oid
        JOIN pg_namespace tn ON tn.oid=t.relnamespace
        WHERE s.relkind='S' AND tn.nspname=__CONTROL_LITERAL__ AND t.relname=ANY(__CLOSED_TABLES__)
        ORDER BY n.nspname,s.relname
    LOOP
        FOR grantee IN SELECT DISTINCT acl.grantee,r.rolname
            FROM aclexplode(coalesce(target.relacl,acldefault('S',target.relowner))) acl
            LEFT JOIN pg_roles r ON r.oid=acl.grantee
            WHERE acl.grantee<>target.relowner
            UNION SELECT 0::oid,NULL::name
        LOOP
            role_sql:=CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(grantee.rolname) END;
            EXECUTE format('REVOKE USAGE,UPDATE ON SEQUENCE %I.%I FROM %s CASCADE',target.nspname,target.relname,role_sql);
        END LOOP;
    END LOOP;
    IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,
        LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
        WHERE n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__CLOSED_TABLES__)
            AND acl.grantee<>c.relowner AND acl.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'))
        OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_attribute a ON a.attrelid=c.oid,LATERAL aclexplode(a.attacl) acl
            WHERE n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__CLOSED_TABLES__)
                AND acl.grantee<>c.relowner AND acl.privilege_type IN ('INSERT','UPDATE','REFERENCES')) THEN
        RAISE EXCEPTION 'custom_import_cutover_write_acl_survived'; END IF;
END $cutover$;
