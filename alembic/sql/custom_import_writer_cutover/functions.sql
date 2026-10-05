-- PostgreSQL does not record PL/pgSQL dynamic-body call dependencies. Pin the
-- reviewed definitions and supplement native dependencies with exact surface
-- identifiers. Opaque in-scope definers are reported, never approved by name.
DO $cutover$
DECLARE receipt record; function_row record; grantee record; identity text;
    reviewed_oids oid[]:='{}'; runtime_oids oid[]:='{}'; protected_oids oid[]:='{}';
    independent_oids oid[]:='{}'; literal_oids oid[]:='{}';
    obsolete_oids oid[]:='{}'; blocked_oids oid[]:='{}'; unresolved text; role_sql text; owner_oid oid;
BEGIN
    SELECT proowner INTO owner_oid FROM pg_proc WHERE oid=
        '__CONTROL__.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)'::regprocedure;
    FOR receipt IN SELECT * FROM jsonb_to_recordset(__REVIEWED__)
        AS receipts(identity text,body_sha256 text,runtime boolean)
    LOOP
        SELECT * INTO function_row FROM pg_proc WHERE oid=to_regprocedure(receipt.identity);
        IF NOT FOUND OR NOT function_row.prosecdef OR function_row.proowner<>owner_oid
            OR function_row.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[]
            OR encode(sha256(convert_to(function_row.prosrc,'UTF8')),'hex')<>receipt.body_sha256 THEN
            RAISE EXCEPTION 'custom_import_cutover_function_mismatch: %',receipt.identity; END IF;
        reviewed_oids:=array_append(reviewed_oids,function_row.oid);
        IF receipt.runtime THEN runtime_oids:=array_append(runtime_oids,function_row.oid);
        ELSE protected_oids:=array_append(protected_oids,function_row.oid); END IF;
    END LOOP;
    -- A second installation's exact known body is not an ambiguous name edge
    -- into this one. Native dependencies and explicit qualified references
    -- remain edges; a missing or changed independent definition is not trusted.
    -- Its exact authority anchor binds its own owner, not this installation's.
    FOR receipt IN
        WITH receipts AS MATERIALIZED (
            SELECT * FROM jsonb_to_recordset(__INDEPENDENT_REVIEWED__)
                AS expected(identity text,body_sha256 text,language text,runtime boolean)
        ), installation_owners AS (
            SELECT anchor.pronamespace,anchor.proowner FROM receipts expected
            JOIN pg_proc anchor ON anchor.oid=to_regprocedure(expected.identity)
            JOIN pg_namespace namespace ON namespace.oid=anchor.pronamespace
            WHERE anchor.oid=to_regprocedure(format('%I.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)',namespace.nspname))
                AND anchor.prokind='f' AND anchor.prosecdef
                AND anchor.proconfig=ARRAY['search_path=pg_catalog']::text[]
                AND anchor.prolang=(SELECT oid FROM pg_language WHERE lanname=expected.language)
                AND encode(sha256(convert_to(anchor.prosrc,'UTF8')),'hex')=expected.body_sha256
        )
        SELECT receipts.*,installation_owners.proowner AS owner_oid FROM receipts
        JOIN pg_proc candidate ON candidate.oid=to_regprocedure(receipts.identity)
        JOIN installation_owners ON installation_owners.pronamespace=candidate.pronamespace
    LOOP
        SELECT * INTO function_row FROM pg_proc WHERE oid=to_regprocedure(receipt.identity);
        IF FOUND AND function_row.prokind='f' AND function_row.prosecdef AND function_row.proowner=receipt.owner_oid
            AND function_row.proconfig=ARRAY['search_path=pg_catalog']::text[]
            AND function_row.prolang=(SELECT oid FROM pg_language WHERE lanname=receipt.language)
            AND encode(sha256(convert_to(function_row.prosrc,'UTF8')),'hex')=receipt.body_sha256 THEN
            independent_oids:=array_append(independent_oids,function_row.oid);
        END IF;
    END LOOP;
    -- Only simple static SQL permits excluding a different literal qualifier.
    -- Strings, quoted names, comments, dynamic SQL and other languages retain
    -- the conservative text fallback instead of depending on a partial parser.
    SELECT coalesce(array_agg(p.oid),'{}'::oid[]) INTO literal_oids
        FROM pg_proc p JOIN pg_language language ON language.oid=p.prolang
        WHERE p.prokind='f' AND language.lanname IN ('sql','plpgsql')
            AND p.prosrc COLLATE "C"~'^[\t\n\r -~]*$'
            AND p.prosrc!~'["''$\\]|--|/\*|\*/' AND p.prosrc!~*'\mEXECUTE\M';
    FOREACH identity IN ARRAY __OBSOLETE__ LOOP
        IF to_regprocedure(identity) IS NULL THEN
            RAISE EXCEPTION 'custom_import_cutover_obsolete_function_missing: %',identity; END IF;
        obsolete_oids:=array_append(obsolete_oids,to_regprocedure(identity)::oid);
    END LOOP;
    -- Close old roots and their actual callers. Reviewed dispatchers never call
    -- an obsolete function, and their existing OID ACLs are not reconstructed.
    -- Reject absent literal identifiers before compiling a dynamic regex. CASE
    -- guarantees this fast path; quoted/metacharacter names remain conservative.
    WITH RECURSIVE procedure_sources AS MATERIALIZED (
        SELECT p.oid,p.prosrc,lower(p.prosrc COLLATE "C") AS folded_source
        FROM pg_proc p WHERE p.prokind='f' AND p.prorettype<>'trigger'::regtype
    ), callers(oid) AS (
        SELECT unnest(obsolete_oids)
        UNION
        SELECT p.oid FROM callers called JOIN pg_proc target ON target.oid=called.oid
        JOIN pg_namespace target_namespace ON target_namespace.oid=target.pronamespace
        JOIN procedure_sources p ON (EXISTS(SELECT 1 FROM pg_depend d
                WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid
                    AND d.refclassid='pg_proc'::regclass AND d.refobjid=target.oid)
                OR CASE WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND strpos(p.folded_source,lower(target.proname::text COLLATE "C"))=0 THEN false
                    WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND target_namespace.nspname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND p.oid=ANY(independent_oids)
                        AND p.prosrc!~*('\m'||target_namespace.nspname||'"?\s*\.\s*"?'||target.proname||'\M') THEN false
                    WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$' AND p.oid=ANY(literal_oids) THEN EXISTS(
                        SELECT 1 FROM regexp_matches(p.prosrc,
                            '(\m[A-Za-z_][A-Za-z_0-9]*\s*\.\s*)?\m'||target.proname||'\M','gi') refs(parts)
                        WHERE refs.parts[1] IS NULL
                            OR lower(regexp_replace(refs.parts[1],'\s*\.\s*$','') COLLATE "C")=target_namespace.nspname)
                    ELSE p.prosrc~*('\m'||target.proname||'\M') END)
        WHERE NOT p.oid=ANY(reviewed_oids)
    ) SELECT coalesce(array_agg(DISTINCT oid),'{}'::oid[]) INTO blocked_oids FROM callers;
    blocked_oids:=blocked_oids||protected_oids;
    FOR function_row IN SELECT p.* FROM pg_proc p WHERE p.oid=ANY(blocked_oids) ORDER BY p.oid LOOP
        identity:=format('%I.%I(%s)',(SELECT nspname FROM pg_namespace WHERE oid=function_row.pronamespace),
            function_row.proname,pg_get_function_identity_arguments(function_row.oid));
        FOR grantee IN SELECT DISTINCT acl.grantee,r.rolname
            FROM aclexplode(coalesce(function_row.proacl,acldefault('f',function_row.proowner))) acl
            LEFT JOIN pg_roles r ON r.oid=acl.grantee
            WHERE acl.grantee<>function_row.proowner
            UNION SELECT 0::oid,NULL::name
        LOOP
            role_sql:=CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(grantee.rolname) END;
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s CASCADE',identity,role_sql);
        END LOOP;
    END LOOP;
    -- Candidate functions are owner-only, including snapshots registered before
    -- this schema revision. No role can invoke a leaf without its dispatcher.
    FOR function_row IN SELECT p.* FROM __CONTROL__.custom_import_snapshot_family f
        JOIN pg_namespace n ON n.nspname='ci_snapshot_'||f.family_id::text
        JOIN pg_proc p ON p.pronamespace=n.oid ORDER BY p.oid
    LOOP
        IF function_row.proowner<>owner_oid OR NOT function_row.prosecdef
            OR function_row.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[] THEN
            RAISE EXCEPTION 'custom_import_cutover_candidate_function_mismatch: %',function_row.oid::regprocedure; END IF;
        identity:=format('%I.%I(%s)',(SELECT nspname FROM pg_namespace WHERE oid=function_row.pronamespace),
            function_row.proname,pg_get_function_identity_arguments(function_row.oid));
        FOR grantee IN SELECT DISTINCT acl.grantee,r.rolname
            FROM aclexplode(coalesce(function_row.proacl,acldefault('f',function_row.proowner))) acl
            LEFT JOIN pg_roles r ON r.oid=acl.grantee
            WHERE acl.grantee<>function_row.proowner
            UNION SELECT 0::oid,NULL::name
        LOOP
            role_sql:=CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(grantee.rolname) END;
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s CASCADE',identity,role_sql);
        END LOOP;
    END LOOP;
    -- Seed only the exact custom-import write surface, not all SECURITY DEFINER
    -- functions in the control schema. Follow native and exact-name call edges.
    WITH RECURSIVE procedure_sources AS MATERIALIZED (
        SELECT p.oid,p.proname,p.prosrc,p.prosqlbody,lower(p.prosrc COLLATE "C") AS folded_source
        FROM pg_proc p WHERE p.prokind='f' AND p.prorettype<>'trigger'::regtype
    ), surface(oid) AS (
        SELECT p.oid FROM procedure_sources p
            WHERE (p.oid=ANY(obsolete_oids) OR (p.oid=ANY(reviewed_oids) AND p.proname=ANY(__WRITE_ROOT_NAMES__))
                OR EXISTS(SELECT 1 FROM pg_depend d JOIN pg_class c ON c.oid=d.refobjid
                    JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.refclassid='pg_class'::regclass
                        AND n.nspname=__CONTROL_LITERAL__ AND c.relname=ANY(__CLOSED_TABLES__)
                        AND (CASE WHEN p.prosqlbody IS NOT NULL THEN pg_get_functiondef(p.oid)
                            ELSE p.prosrc END)~*'\m(INSERT|UPDATE|DELETE|MERGE|COPY|TRUNCATE|EXECUTE)\M')
                OR EXISTS(SELECT 1 FROM unnest(__CLOSED_TABLES__) tables(name)
                    WHERE CASE WHEN strpos(p.folded_source,tables.name)=0 THEN false
                        WHEN __CONTROL_LITERAL__ COLLATE "C"~'^[A-Za-z0-9_]+$' AND p.oid=ANY(independent_oids)
                            AND p.prosrc!~*('\m'||__CONTROL_LITERAL__||'"?\s*\.\s*"?'||tables.name||'\M') THEN false
                        WHEN p.oid=ANY(literal_oids) THEN EXISTS(
                            SELECT 1 FROM regexp_matches(p.prosrc,
                                '(\m[A-Za-z_][A-Za-z_0-9]*\s*\.\s*)?\m'||tables.name||'\M','gi') refs(parts)
                            WHERE refs.parts[1] IS NULL
                                OR lower(regexp_replace(refs.parts[1],'\s*\.\s*$','') COLLATE "C")=__CONTROL_LITERAL__)
                        ELSE p.prosrc~*('\m'||tables.name||'\M') END
                        AND p.prosrc~*'\m(INSERT|UPDATE|DELETE|MERGE|COPY|TRUNCATE|EXECUTE)\M'))
        UNION
        SELECT matched.oid FROM surface called
        CROSS JOIN LATERAL (
            SELECT p.oid FROM pg_proc target
            JOIN pg_namespace target_namespace ON target_namespace.oid=target.pronamespace
            JOIN procedure_sources p ON (EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid
                    AND d.refclassid='pg_proc'::regclass AND d.refobjid=target.oid)
                OR CASE WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND strpos(p.folded_source,lower(target.proname::text COLLATE "C"))=0 THEN false
                    WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND target_namespace.nspname::text COLLATE "C"~'^[A-Za-z0-9_]+$'
                        AND p.oid=ANY(independent_oids)
                        AND p.prosrc!~*('\m'||target_namespace.nspname||'"?\s*\.\s*"?'||target.proname||'\M') THEN false
                    WHEN target.proname::text COLLATE "C"~'^[A-Za-z0-9_]+$' AND p.oid=ANY(literal_oids) THEN EXISTS(
                        SELECT 1 FROM regexp_matches(p.prosrc,
                            '(\m[A-Za-z_][A-Za-z_0-9]*\s*\.\s*)?\m'||target.proname||'\M','gi') refs(parts)
                        WHERE refs.parts[1] IS NULL
                            OR lower(regexp_replace(refs.parts[1],'\s*\.\s*$','') COLLATE "C")=target_namespace.nspname)
                    ELSE p.prosrc~*('\m'||target.proname||'\M') END)
            WHERE target.oid=called.oid
            -- Preserve every match while preventing a whole-catalog edge join.
            OFFSET 0
        ) matched
    )
    SELECT string_agg(p.oid::regprocedure::text,', ' ORDER BY p.oid::regprocedure::text) INTO unresolved
        FROM surface s JOIN pg_proc p ON p.oid=s.oid
        WHERE p.prosecdef AND NOT p.oid=ANY(reviewed_oids) AND NOT p.oid=ANY(blocked_oids)
            AND EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
                WHERE acl.grantee<>p.proowner AND acl.privilege_type='EXECUTE');
    IF unresolved IS NOT NULL THEN
        RAISE EXCEPTION 'custom_import_cutover_unreviewed_callable_definer: %',unresolved; END IF;
    IF EXISTS(SELECT 1 FROM pg_proc p,LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
        WHERE p.oid=ANY(blocked_oids) AND acl.grantee<>p.proowner AND acl.privilege_type='EXECUTE') THEN
        RAISE EXCEPTION 'custom_import_cutover_obsolete_execute_survived'; END IF;
END $cutover$;
