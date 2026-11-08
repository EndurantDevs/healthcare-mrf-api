-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

LOCK TABLE __SCHEMA__.provider_directory_rooted_graph_acquisition,
    __SCHEMA__.provider_directory_rooted_graph_work,
    __SCHEMA__.provider_directory_rooted_graph_resource,
    __SCHEMA__.provider_directory_rooted_graph_edge IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $fence$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_class WHERE oid IN (
        '__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)
        AND (relkind<>'r' OR relrowsecurity OR relforcerowsecurity)) THEN
        RAISE EXCEPTION 'rooted_graph_storage_shape_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid IN (
        '__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)
        AND NOT convalidated) THEN
        RAISE EXCEPTION 'rooted_graph_storage_constraint_unvalidated' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_index WHERE indrelid IN (
        '__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)
        AND (NOT indisvalid OR NOT indisready)) THEN
        RAISE EXCEPTION 'rooted_graph_storage_index_invalid' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid IN (
        '__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
        '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)
        AND conparentid=0 AND contype NOT IN ('c','n') AND conname NOT IN (
            'provider_directory_rooted_graph_work_pkey','provider_directory_rooted_graph_work_scope_key',
            'provider_directory_rooted_graph_work_acquisition_fkey','provider_directory_rooted_graph_resource_pkey',
            'provider_directory_rooted_graph_resource_work_fkey','provider_directory_rooted_graph_edge_pkey',
            'provider_directory_rooted_graph_edge_resource_fkey')) THEN
        RAISE EXCEPTION 'rooted_graph_storage_constraint_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (
        WITH expected(source,target,source_columns,target_columns) AS (VALUES
            ('__SCHEMA__.provider_directory_rooted_graph_work'::regclass,'__SCHEMA__.provider_directory_rooted_graph_acquisition'::regclass,
             ARRAY['acquisition_id','scope_id'],ARRAY['acquisition_id','scope_id']),
            ('__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,'__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
             ARRAY['acquisition_id','scope_id','query_id'],ARRAY['acquisition_id','scope_id','query_id']),
            ('__SCHEMA__.provider_directory_rooted_graph_edge'::regclass,'__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
             ARRAY['acquisition_id','query_id','attempt','source_resource_type','source_resource_id'],
             ARRAY['acquisition_id','query_id','attempt','resource_type','resource_id']))
        SELECT FROM pg_constraint constraint_record
        WHERE contype='f' AND conparentid=0 AND conrelid IN (SELECT source FROM expected)
          AND NOT EXISTS(SELECT FROM expected WHERE source=conrelid AND target=confrelid
              AND source_columns=ARRAY(SELECT attname::text FROM unnest(conkey) WITH ORDINALITY key(attnum,position)
                  JOIN pg_attribute ON attrelid=conrelid AND pg_attribute.attnum=key.attnum ORDER BY position)
              AND target_columns=ARRAY(SELECT attname::text FROM unnest(confkey) WITH ORDINALITY key(attnum,position)
                  JOIN pg_attribute ON attrelid=confrelid AND pg_attribute.attnum=key.attnum ORDER BY position)
              AND confmatchtype='s' AND confdeltype='a' AND confupdtype='a' AND NOT condeferrable)
    ) THEN
        RAISE EXCEPTION 'rooted_graph_bulk_relationship_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM pg_constraint WHERE contype='f' AND conparentid=0
        AND confrelid IN ('__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
                         '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
                         '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)
        AND conrelid NOT IN ('__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
                            '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass)) THEN
        RAISE EXCEPTION 'rooted_graph_witness_dependency_changed' USING ERRCODE='55000';
    END IF;
END; $fence$;
CREATE TEMP TABLE pdrg_work_writer_acl ON COMMIT DROP AS
    SELECT DISTINCT c.relname, acl.grantee, acl.privilege_type, NULL::text AS column_name
    FROM pg_class c, LATERAL aclexplode(coalesce(c.relacl, acldefault('r',c.relowner))) acl
    WHERE c.oid='__SCHEMA__.provider_directory_rooted_graph_work'::regclass;
INSERT INTO pdrg_work_writer_acl
    SELECT DISTINCT c.relname, acl.grantee, acl.privilege_type, col.attname
    FROM pg_class c JOIN pg_attribute col ON col.attrelid=c.oid
    CROSS JOIN LATERAL aclexplode(col.attacl) acl
    WHERE c.oid='__SCHEMA__.provider_directory_rooted_graph_work'::regclass;
ALTER INDEX __SCHEMA__.provider_directory_rooted_graph_plan_census_key RENAME TO pdrg_work_legacy_plan_census_key;
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work RENAME TO provider_directory_rooted_graph_work_legacy;
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work_legacy
    RENAME CONSTRAINT provider_directory_rooted_graph_work_pkey TO pdrg_work_legacy_pkey;
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work_legacy
    RENAME CONSTRAINT provider_directory_rooted_graph_work_scope_key TO pdrg_work_legacy_scope_key;
ALTER TABLE __SCHEMA__.pdrg_work_parent RENAME TO provider_directory_rooted_graph_work;
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work
    RENAME CONSTRAINT pdrg_work_parent_pkey TO provider_directory_rooted_graph_work_pkey;
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work
    RENAME CONSTRAINT pdrg_work_parent_scope_key TO provider_directory_rooted_graph_work_scope_key;
ALTER INDEX __SCHEMA__.pdrg_work_parent_plan_census_key RENAME TO provider_directory_rooted_graph_plan_census_key;
DROP TRIGGER provider_directory_rooted_graph_work_row_guard ON __SCHEMA__.provider_directory_rooted_graph_work_legacy;
DROP TRIGGER provider_directory_rooted_graph_work_truncate_guard ON __SCHEMA__.provider_directory_rooted_graph_work_legacy;
DROP TRIGGER provider_directory_rooted_graph_work_budget_guard ON __SCHEMA__.provider_directory_rooted_graph_work_legacy;
CREATE TRIGGER provider_directory_rooted_graph_work_row_guard BEFORE UPDATE OR DELETE
    ON __SCHEMA__.provider_directory_rooted_graph_work FOR EACH ROW
    EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_work();
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work ENABLE ALWAYS TRIGGER provider_directory_rooted_graph_work_row_guard;
CREATE TRIGGER rooted_graph_work_truncate BEFORE TRUNCATE
    ON __SCHEMA__.provider_directory_rooted_graph_work FOR EACH STATEMENT
    EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_work();
ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_work ENABLE ALWAYS TRIGGER rooted_graph_work_truncate;

LOCK TABLE __SCHEMA__."provider_directory_rooted_graph_acquisition", __SCHEMA__."provider_directory_rooted_graph_work", __SCHEMA__."provider_directory_rooted_graph_resource", __SCHEMA__."provider_directory_rooted_graph_edge" IN ACCESS EXCLUSIVE MODE NOWAIT;
    DO $fence$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_constraint WHERE contype='f' AND conparentid=0
                    AND confrelid IN ('__SCHEMA__."provider_directory_rooted_graph_resource"'::regclass, '__SCHEMA__."provider_directory_rooted_graph_edge"'::regclass)
                    AND conrelid <> '__SCHEMA__."provider_directory_rooted_graph_edge"'::regclass
                    AND conrelid IS DISTINCT FROM to_regclass('__SCHEMA__.pdrg_edge_parent')) THEN
            RAISE EXCEPTION 'rooted_graph_witness_dependency_changed' USING ERRCODE='55000';
        END IF;
    END; $fence$;
    CREATE TEMP TABLE pdrg_writer_acl ON COMMIT DROP AS
        SELECT DISTINCT c.relname, a.grantee, a.privilege_type, NULL::text AS column_name FROM pg_class c,
          LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) a
         WHERE c.oid IN ('__SCHEMA__."provider_directory_rooted_graph_resource"'::regclass, '__SCHEMA__."provider_directory_rooted_graph_edge"'::regclass);
    INSERT INTO pdrg_writer_acl
        SELECT DISTINCT c.relname, a.grantee, a.privilege_type, col.attname FROM pg_class c
          JOIN pg_attribute col ON col.attrelid=c.oid
          CROSS JOIN LATERAL aclexplode(col.attacl) a
         WHERE c.oid IN ('__SCHEMA__."provider_directory_rooted_graph_resource"'::regclass, '__SCHEMA__."provider_directory_rooted_graph_edge"'::regclass);
    INSERT INTO pdrg_writer_acl SELECT * FROM pdrg_work_writer_acl;
    DROP TABLE pdrg_work_writer_acl;
    ALTER TABLE __SCHEMA__."provider_directory_rooted_graph_resource" RENAME TO "provider_directory_rooted_graph_resource_legacy";
    ALTER TABLE __SCHEMA__."provider_directory_rooted_graph_edge" RENAME TO "provider_directory_rooted_graph_edge_legacy";
    ALTER TABLE __SCHEMA__."provider_directory_rooted_graph_resource_legacy" RENAME CONSTRAINT "provider_directory_rooted_graph_resource_pkey" TO pdrg_resource_legacy_pkey;
    ALTER TABLE __SCHEMA__."provider_directory_rooted_graph_edge_legacy" RENAME CONSTRAINT "provider_directory_rooted_graph_edge_pkey" TO pdrg_edge_legacy_pkey;
    ALTER TABLE __SCHEMA__.pdrg_resource_parent RENAME TO provider_directory_rooted_graph_resource;
    ALTER TABLE __SCHEMA__.pdrg_edge_parent RENAME TO provider_directory_rooted_graph_edge;
    ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_resource
        RENAME CONSTRAINT pdrg_resource_parent_pkey TO provider_directory_rooted_graph_resource_pkey;
    ALTER TABLE __SCHEMA__.provider_directory_rooted_graph_edge
        RENAME CONSTRAINT pdrg_edge_parent_pkey TO provider_directory_rooted_graph_edge_pkey;
    -- The replacement is complete before retiring these guards. No trigger is reenabled.
    DROP TRIGGER "provider_directory_rooted_graph_resource_row_guard" ON __SCHEMA__."provider_directory_rooted_graph_resource_legacy";
    DROP TRIGGER "provider_directory_rooted_graph_resource_truncate_guard" ON __SCHEMA__."provider_directory_rooted_graph_resource_legacy";
    DROP TRIGGER provider_directory_rooted_graph_resource_budget_guard ON __SCHEMA__."provider_directory_rooted_graph_resource_legacy";
    DROP TRIGGER "provider_directory_rooted_graph_edge_row_guard" ON __SCHEMA__."provider_directory_rooted_graph_edge_legacy";
    DROP TRIGGER "provider_directory_rooted_graph_edge_truncate_guard" ON __SCHEMA__."provider_directory_rooted_graph_edge_legacy";
    DROP TRIGGER provider_directory_rooted_graph_edge_budget_guard ON __SCHEMA__."provider_directory_rooted_graph_edge_legacy";
    DO $owner$
    DECLARE original_owner name; parent_name text;
    BEGIN
        FOR parent_name IN VALUES ('provider_directory_rooted_graph_work'), ('provider_directory_rooted_graph_resource'), ('provider_directory_rooted_graph_edge') LOOP
            SELECT pg_get_userbyid(relowner) INTO original_owner FROM pg_class
             WHERE oid=to_regclass(format('%I.%I','__SCHEMA_NAME__',parent_name || '_legacy'));
            EXECUTE format('ALTER TABLE %I.%I OWNER TO %I','__SCHEMA_NAME__',parent_name,original_owner);
        END LOOP;
    END; $owner$;

CREATE OR REPLACE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_copy_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
BEGIN
    IF NOT EXISTS(SELECT FROM pg_class relation JOIN pg_roles caller ON caller.rolname=current_user
                  WHERE relation.oid=TG_RELID AND (relation.relowner=caller.oid OR caller.rolsuper)) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_copy_unauthorized' USING ERRCODE='42501';
    END IF;
    RETURN NULL;
END; $function$;

-- Set-based equivalent of the accepted work identity, census and discovery contract.
CREATE FUNCTION __SCHEMA__.validate_provider_directory_rooted_graph_stage_checks(stage regclass, history regclass)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
DECLARE invalid_predicate text; invalid boolean;
BEGIN
    SELECT string_agg('NOT (' || pg_get_expr(conbin,conrelid) || ')',' OR ') INTO invalid_predicate
      FROM pg_constraint WHERE conrelid=history AND contype='c' AND conname NOT LIKE 'pdrg_%_legacy_scope';
    IF invalid_predicate IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS(SELECT FROM %s WHERE %s)',stage,invalid_predicate) INTO invalid;
        IF invalid THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_stage_invalid' USING ERRCODE='23514';
        END IF;
    END IF;
END; $function$;

CREATE FUNCTION __SCHEMA__.validate_provider_directory_rooted_graph_work_stage(p_acquisition text,p_action text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
DECLARE expected_header __SCHEMA__.provider_directory_rooted_graph_acquisition;
BEGIN
    PERFORM __SCHEMA__.validate_provider_directory_rooted_graph_stage_checks(
        'pg_temp.pdrg_work_stage','__SCHEMA__.provider_directory_rooted_graph_work_legacy');
    SELECT * INTO expected_header FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id=p_acquisition AND status='building';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_work_invalid' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming
        WHERE incoming.acquisition_id IS DISTINCT FROM p_acquisition
           OR incoming.scope_id IS DISTINCT FROM expected_header.scope_id
           OR incoming.status<>'pending' OR incoming.attempt_count<>0 OR incoming.pagination_terminal
           OR incoming.created_at IS DISTINCT FROM transaction_timestamp()
           OR incoming.updated_at IS DISTINCT FROM transaction_timestamp()
           OR incoming.query_identity_json_text::jsonb IS DISTINCT FROM jsonb_build_object(
               'kind',incoming.kind,'page_size',CASE WHEN incoming.kind='direct_read' THEN NULL::integer ELSE 100 END,
               'pagination',CASE WHEN incoming.kind='direct_read' THEN 'forbidden' ELSE 'same-origin-source-issued-until-terminal' END,
               'reference',CASE WHEN incoming.reference_type IS NULL THEN NULL ELSE incoming.reference_type||'/'||incoming.reference_id END,
               'resource_type',incoming.resource_type,'search_parameter',incoming.search_parameter)
           OR incoming.query_identity_sha256 IS DISTINCT FROM encode(sha256(convert_to(incoming.query_identity_json_text,'UTF8')),'hex')
           OR incoming.query_id IS DISTINCT FROM 'pdrgq_' || substr(encode(sha256(convert_to(
               'healthporta.provider-directory.rooted-graph-identity.v1' || chr(31) || incoming.scope_id || chr(31)
               || incoming.query_identity_json_text,'UTF8')),'hex'),1,48)
    ) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_work_invalid' USING ERRCODE='23514';
    END IF;
    IF p_action='initialize' AND EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming
        LEFT JOIN __SCHEMA__.provider_directory_dataset_resource AS member
          ON member.dataset_id=expected_header.root_dataset_id AND member.resource_type='Practitioner'
         AND member.resource_id=incoming.reference_id
        WHERE incoming.resource_type<>'PractitionerRole' OR member.resource_id IS NULL
    ) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_root_work_invalid' USING ERRCODE='23514';
    END IF;
    IF p_action='derive' AND EXISTS (
        SELECT 1 FROM pg_temp.pdrg_work_stage WHERE resource_type='PractitionerRole' OR kind='full_insurance_plan_census'
    ) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_root_work_invalid' USING ERRCODE='23514';
    END IF;
    IF p_action='derive' AND EXISTS (SELECT 1 FROM pg_temp.pdrg_work_stage WHERE closure_scope='root')
       AND EXISTS (SELECT 1 FROM __SCHEMA__.provider_directory_rooted_graph_work
                   WHERE acquisition_id=p_acquisition AND kind='full_insurance_plan_census') THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_root_closure_frozen' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_temp.pdrg_work_stage AS incoming WHERE (p_action = 'census' AND (
                incoming.kind <> 'full_insurance_plan_census'
                OR NOT EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.closure_scope = 'root'
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND (root_query.closure_scope <> 'root'
                            OR NOT (root_query.status = 'completed' OR (root_query.status = 'error' AND root_query.error_code = 'transport_timeout')))
                )
                OR EXISTS (
                    SELECT member.resource_id
                      FROM __SCHEMA__."provider_directory_rooted_graph_acquisition" AS header
                      JOIN __SCHEMA__."provider_directory_dataset_resource" AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = incoming.acquisition_id
                       AND member.resource_type = 'Practitioner'
                    EXCEPT
                    SELECT root_query.reference_id FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                       AND (root_query.status = 'completed' OR (root_query.status = 'error' AND root_query.error_code = 'transport_timeout'))
                )
                OR EXISTS (
                    SELECT root_query.reference_id FROM __SCHEMA__."provider_directory_rooted_graph_work" AS root_query
                     WHERE root_query.acquisition_id = incoming.acquisition_id
                       AND root_query.kind = 'exact_reference_search'
                       AND root_query.resource_type = 'PractitionerRole'
                       AND root_query.closure_scope = 'root'
                    EXCEPT
                    SELECT member.resource_id
                      FROM __SCHEMA__."provider_directory_rooted_graph_acquisition" AS header
                      JOIN __SCHEMA__."provider_directory_dataset_resource" AS member
                        ON member.dataset_id = header.root_dataset_id
                     WHERE header.acquisition_id = incoming.acquisition_id
                       AND member.resource_type = 'Practitioner'
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_edge" AS reference_edge
                      JOIN __SCHEMA__."provider_directory_rooted_graph_work" AS source_query
                        ON source_query.acquisition_id =
                           reference_edge.acquisition_id
                       AND source_query.query_id = reference_edge.query_id
                       AND source_query.attempt_count = reference_edge.attempt
                     WHERE reference_edge.acquisition_id = incoming.acquisition_id
                       AND reference_edge.closure_scope = 'root'
                       AND source_query.status = 'completed'
                       AND reference_edge.target_resource_type IN (
                           'Organization', 'Location', 'HealthcareService', 'Endpoint'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS target_query
                            WHERE target_query.acquisition_id = incoming.acquisition_id
                              AND target_query.kind = 'direct_read'
                              AND target_query.reference_type =
                                  reference_edge.target_resource_type
                              AND target_query.reference_id =
                                  reference_edge.target_resource_id
                              AND target_query.closure_scope = 'root'
                              AND (target_query.status = 'completed' OR (target_query.status = 'error' AND target_query.error_code = 'transport_timeout'))
                       )
                )
                OR EXISTS (
                    SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_resource" AS organization
                      JOIN __SCHEMA__."provider_directory_rooted_graph_work" AS organization_query
                        ON organization_query.acquisition_id =
                           organization.acquisition_id
                       AND organization_query.query_id = organization.query_id
                       AND organization_query.attempt_count = organization.attempt
                     WHERE organization.acquisition_id = incoming.acquisition_id
                       AND organization.resource_type = 'Organization'
                       AND organization.closure_scope = 'root'
                       AND organization_query.status = 'completed'
                       AND NOT EXISTS (
                           SELECT 1 FROM __SCHEMA__."provider_directory_rooted_graph_work" AS affiliation_query
                            WHERE affiliation_query.acquisition_id =
                                  incoming.acquisition_id
                              AND affiliation_query.kind = 'exact_reference_search'
                              AND affiliation_query.resource_type =
                                  'OrganizationAffiliation'
                              AND affiliation_query.reference_id =
                                  organization.resource_id
                              AND affiliation_query.closure_scope = 'root'
                              AND (affiliation_query.status = 'completed' OR (affiliation_query.status = 'error' AND affiliation_query.error_code = 'transport_timeout'))
                       )
                )
            ) )) THEN
                RAISE EXCEPTION
                    'provider_directory_rooted_graph_root_fixed_point_incomplete'
                    USING ERRCODE = '23514';
            END IF;
    IF p_action='derive' AND EXISTS (
        SELECT query_id FROM pg_temp.pdrg_work_stage WHERE kind='direct_read'
        EXCEPT
        SELECT incoming.query_id FROM pg_temp.pdrg_work_stage AS incoming
        JOIN __SCHEMA__.provider_directory_rooted_graph_edge AS proof
          ON proof.acquisition_id=p_acquisition AND proof.query_id=incoming.discovered_by_query_id
         AND proof.source_resource_type=incoming.discovered_source_type
         AND proof.source_resource_id=incoming.discovered_source_id
         AND proof.edge_sha256=incoming.discovered_edge_sha256
         AND proof.target_resource_type=incoming.reference_type AND proof.target_resource_id=incoming.reference_id
         AND proof.closure_scope=incoming.closure_scope
        JOIN __SCHEMA__.provider_directory_rooted_graph_work AS parent
          ON parent.acquisition_id=proof.acquisition_id AND parent.query_id=proof.query_id
         AND parent.attempt_count=proof.attempt AND parent.status='completed'
         AND parent.terminal_at=transaction_timestamp()
        WHERE incoming.kind='direct_read'
    ) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_discovery_invalid' USING ERRCODE='23514';
    END IF;
    IF p_action='derive' AND EXISTS (
        SELECT query_id FROM pg_temp.pdrg_work_stage WHERE resource_type='OrganizationAffiliation'
        EXCEPT
        SELECT incoming.query_id FROM pg_temp.pdrg_work_stage AS incoming
        JOIN __SCHEMA__.provider_directory_rooted_graph_resource AS proof
          ON proof.acquisition_id=p_acquisition AND proof.query_id=incoming.discovered_by_query_id
         AND proof.resource_type='Organization' AND proof.resource_id=incoming.reference_id
         AND proof.closure_scope=incoming.closure_scope
        JOIN __SCHEMA__.provider_directory_rooted_graph_work AS parent
          ON parent.acquisition_id=proof.acquisition_id AND parent.query_id=proof.query_id
         AND parent.attempt_count=proof.attempt AND parent.status='completed'
         AND parent.terminal_at=transaction_timestamp()
        WHERE incoming.resource_type='OrganizationAffiliation'
    ) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_discovery_invalid' USING ERRCODE='23514';
    END IF;
END;
$function$;

CREATE FUNCTION __SCHEMA__.assert_provider_directory_rooted_graph_storage(p_acquisition text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
BEGIN
    IF (SELECT count(*) FROM (VALUES
        ('provider_directory_rooted_graph_work','pdrgw_'),
        ('provider_directory_rooted_graph_resource','pdrgr_'),
        ('provider_directory_rooted_graph_edge','pdrge_')
    ) expected(parent_name,prefix)
    JOIN pg_inherits child ON child.inhparent=to_regclass(format('__SCHEMA__.%I',expected.parent_name))
       AND child.inhrelid=to_regclass(format('__SCHEMA__.%I',expected.prefix || substring(p_acquisition FROM 7)))
    JOIN pg_class candidate ON candidate.oid=child.inhrelid
       AND candidate.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
       AND candidate.relkind='r' AND candidate.relpersistence='p') <> 3 THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_rerun_required' USING ERRCODE='55000';
    END IF;
END;
$function$;

CREATE FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_work_stage()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE caller name; grant_row record;
BEGIN
    caller := CASE WHEN current_setting('role')='none' THEN session_user ELSE current_setting('role') END;
    CREATE TEMP TABLE pdrg_work_stage (LIKE __SCHEMA__.provider_directory_rooted_graph_work
        INCLUDING DEFAULTS) ON COMMIT DROP;
    FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_class c,LATERAL aclexplode(c.relacl) acl
        WHERE c.oid='pg_temp.pdrg_work_stage'::regclass AND acl.grantee<>c.relowner
    LOOP
        EXECUTE format('REVOKE ALL ON pg_temp.pdrg_work_stage FROM %s',
            CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
    END LOOP;
    EXECUTE format('GRANT INSERT ON pg_temp.pdrg_work_stage TO %I',caller);
END;
$function$;

CREATE FUNCTION __SCHEMA__.admit_provider_directory_rooted_graph_work(p_acquisition text,p_action text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE header __SCHEMA__.provider_directory_rooted_graph_acquisition; inserted_count bigint; work_name text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=to_regclass('pg_temp.pdrg_work_stage')
                   AND relnamespace=pg_my_temp_schema() AND relkind='r' AND relpersistence='t'
                   AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_stage_invalid' USING ERRCODE='23514';
    END IF;
    LOCK TABLE pg_temp.pdrg_work_stage IN ACCESS EXCLUSIVE MODE NOWAIT;
    SELECT * INTO header FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id=p_acquisition AND status='building' FOR UPDATE;
    IF NOT FOUND OR p_action IS NULL OR p_action NOT IN ('initialize','derive','census') THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_work_invalid' USING ERRCODE='23514';
    END IF;
    work_name := 'pdrgw_' || substring(p_acquisition FROM 7);
    IF p_action<>'initialize' THEN
        PERFORM __SCHEMA__.assert_provider_directory_rooted_graph_storage(p_acquisition);
    ELSIF NOT EXISTS(SELECT FROM pg_class WHERE oid=to_regclass(format('__SCHEMA__.%I',work_name))
        AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND relkind='r')
        OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=to_regclass(format('__SCHEMA__.%I',work_name))) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_stage_invalid' USING ERRCODE='23514';
    END IF;
    ALTER TABLE pg_temp.pdrg_work_stage ADD PRIMARY KEY(acquisition_id,query_id);
    IF (p_action='census' AND (SELECT count(*) FROM pg_temp.pdrg_work_stage)<>1)
       OR (p_action='initialize' AND (SELECT count(*) FROM pg_temp.pdrg_work_stage)<>header.root_resource_count) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_work_invalid' USING ERRCODE='23514';
    END IF;
    PERFORM __SCHEMA__.validate_provider_directory_rooted_graph_work_stage(p_acquisition,p_action);
    EXECUTE format('INSERT INTO __SCHEMA__.%I SELECT * FROM pg_temp.pdrg_work_stage %s',work_name,
        CASE WHEN p_action='initialize' THEN '' ELSE 'ON CONFLICT(acquisition_id,query_id) DO NOTHING' END);
    GET DIAGNOSTICS inserted_count=ROW_COUNT;
    IF p_action='initialize' THEN
        PERFORM __SCHEMA__.finish_provider_directory_rooted_graph_initial_storage(p_acquisition);
    END IF;
    DROP TABLE pg_temp.pdrg_work_stage;
    RETURN inserted_count;
END;
$function$;

CREATE FUNCTION __SCHEMA__.initialize_provider_directory_rooted_graph_work(p_acquisition text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE header __SCHEMA__.provider_directory_rooted_graph_acquisition;
BEGIN
    SELECT * INTO header FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id=p_acquisition AND status='building' FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_invalid' USING ERRCODE='55000';
    END IF;
    IF EXISTS(SELECT FROM __SCHEMA__.provider_directory_rooted_graph_work WHERE acquisition_id=p_acquisition) THEN
        PERFORM __SCHEMA__.assert_provider_directory_rooted_graph_storage(p_acquisition);
        RETURN 0;
    END IF;
    PERFORM __SCHEMA__.prepare_provider_directory_rooted_graph_work_stage();
    RETURN 1;
END;
$function$;

CREATE FUNCTION __SCHEMA__.read_provider_directory_rooted_graph_initial_work(p_acquisition text,p_after text,p_limit integer)
RETURNS TABLE(acquisition_id text,scope_id text,query_id text,query_identity_sha256 text,query_identity_json_text text,
              kind text,resource_type text,search_parameter text,reference_type text,reference_id text,closure_scope text,
              discovered_by_query_id text,discovered_source_type text,discovered_source_id text,discovered_edge_sha256 text,
              status text,attempt_count integer,pagination_terminal boolean)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $function$
    WITH header AS (SELECT * FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
                    WHERE acquisition_id=p_acquisition AND status='building'), canonical_root_query AS (
            SELECT member.resource_id,
                   '{"kind":"exact_reference_search","page_size":' ||
                   CAST(100 AS text) ||
                   ',"pagination":"same-origin-source-issued-until-terminal","reference":' ||
                   pg_catalog.to_json(
                       ('Practitioner/' || member.resource_id)::text
                   )::text ||
                   ',"resource_type":"PractitionerRole",'
                   '"search_parameter":"practitioner"}'
                       AS query_identity_json_text
              FROM __SCHEMA__.provider_directory_dataset_resource AS member CROSS JOIN header
             WHERE member.dataset_id = header.root_dataset_id
               AND member.resource_type = 'Practitioner' AND member.resource_id>p_after
             ORDER BY member.resource_id LIMIT LEAST(GREATEST(p_limit,0),4096)
        ), canonical_root_identity AS (
            SELECT root.resource_id, root.query_identity_json_text,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       root.query_identity_json_text, 'UTF8'
                   )), 'hex') AS query_identity_sha256,
                   pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                       'healthporta.provider-directory.rooted-graph-identity.v1' || pg_catalog.chr(31) || header.scope_id ||
                       pg_catalog.chr(31) || root.query_identity_json_text,
                       'UTF8'
                   )), 'hex') AS query_id_sha256
              FROM canonical_root_query AS root CROSS JOIN header
        )
        SELECT
            header.acquisition_id, header.scope_id,
            'pdrgq_' || pg_catalog.substr(root.query_id_sha256, 1, 48),
            root.query_identity_sha256, root.query_identity_json_text,
            'exact_reference_search', 'PractitionerRole', 'practitioner',
            'Practitioner', root.resource_id, 'root',
            NULL, NULL, NULL, NULL, 'pending', 0, false
          FROM canonical_root_identity AS root CROSS JOIN header
         ORDER BY root.resource_id
        ;

$function$;

-- All semantic checks cover the isolated result set; no row trigger executes them.
CREATE FUNCTION __SCHEMA__.validate_provider_directory_rooted_graph_witnesses(
    resource_stage regclass, edge_stage regclass,
    expected __SCHEMA__.provider_directory_rooted_graph_work
) RETURNS bigint[] LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
DECLARE invalid boolean; resource_count bigint; edge_count bigint; payload_bytes bigint;
BEGIN
    PERFORM __SCHEMA__.validate_provider_directory_rooted_graph_stage_checks(
        resource_stage,'__SCHEMA__.provider_directory_rooted_graph_resource_legacy');
    PERFORM __SCHEMA__.validate_provider_directory_rooted_graph_stage_checks(
        edge_stage,'__SCHEMA__.provider_directory_rooted_graph_edge_legacy');
    EXECUTE format($query$
        WITH RECURSIVE payloads AS MATERIALIZED (
            SELECT incoming.*, payload_json_text::jsonb AS payload FROM %s AS incoming
        ), extensions AS (
            SELECT resource_type, resource_id, item.value AS node, 1 AS depth
              FROM payloads CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(payload -> 'extension') = 'array'
                     THEN payload -> 'extension' ELSE '[]'::jsonb END
              ) AS item(value)
            UNION ALL
            SELECT parent.resource_type, parent.resource_id, child.value, parent.depth + 1
              FROM extensions AS parent CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(parent.node -> 'extension') = 'array'
                     THEN parent.node -> 'extension' ELSE '[]'::jsonb END
              ) AS child(value) WHERE parent.depth <= 6
        ), extension_checks AS (
            SELECT resource_type, resource_id, count(*) AS nodes,
                   bool_or(
                     CASE WHEN jsonb_typeof(node) <> 'object' THEN true
                       WHEN node ->> 'url' IS NULL OR node ->> 'url' !~ '^[^|]+(\|[A-Za-z0-9.-]{1,64})?$' THEN true
                       WHEN split_part(node ->> 'url', '|', 1) IN (__NETWORK_URLS__) THEN
                         resource_type <> 'PractitionerRole' OR node ? 'extension'
                         OR NOT node ? 'valueReference'
                         OR EXISTS (SELECT 1 FROM jsonb_object_keys(node) AS key(name)
                                     WHERE key.name NOT IN ('url', 'valueReference'))
                         OR jsonb_typeof(node -> 'valueReference') <> 'object'
                         OR EXISTS (SELECT 1 FROM jsonb_object_keys(node -> 'valueReference') AS key(name)
                                     WHERE key.name <> 'reference')
                         OR node #>> '{valueReference,reference}' !~ '^Organization/[A-Za-z0-9.-]{1,64}$'
                       ELSE (node ? 'extension' AND EXISTS (
                               SELECT 1 FROM jsonb_object_keys(node) AS key(name) WHERE key.name LIKE 'value%%'))
                         OR EXISTS (SELECT 1 FROM jsonb_each(node) AS field(name, value)
                                     WHERE field.name = 'valueReference'
                                        OR (jsonb_typeof(field.value) = 'object' AND field.value ? 'reference'))
                     END OR depth > 6 OR (node ? 'extension' AND jsonb_typeof(node -> 'extension') <> 'array')
                   ) AS invalid
              FROM extensions GROUP BY resource_type, resource_id
        )
        SELECT count(*)::bigint, coalesce(sum(octet_length(payload_json_text)), 0)::bigint,
               coalesce(bool_or(
                   incoming.acquisition_id IS DISTINCT FROM ($1).acquisition_id
                   OR incoming.scope_id IS DISTINCT FROM ($1).scope_id
                   OR incoming.query_id IS DISTINCT FROM ($1).query_id
                   OR incoming.attempt IS DISTINCT FROM ($1).attempt_count
                   OR incoming.created_at IS DISTINCT FROM transaction_timestamp()
                   OR incoming.payload_sha256 IS DISTINCT FROM encode(sha256(convert_to(payload_json_text, 'UTF8')), 'hex')
                   OR jsonb_typeof(payload) IS DISTINCT FROM 'object'
                   OR payload ->> 'resourceType' IS DISTINCT FROM incoming.resource_type
                   OR payload ->> 'id' IS DISTINCT FROM incoming.resource_id
                   OR incoming.resource_type IS DISTINCT FROM ($1).resource_type
                   OR (payload ? 'extension' AND jsonb_typeof(payload -> 'extension') <> 'array')
                   OR extension_checks.nodes > 4096 OR extension_checks.invalid IS TRUE
                   OR (($1).kind = 'exact_reference_search' AND CASE ($1).resource_type
                         WHEN 'PractitionerRole' THEN payload #>> '{practitioner,reference}'
                         WHEN 'OrganizationAffiliation' THEN payload #>> '{participatingOrganization,reference}'
                         ELSE NULL END IS DISTINCT FROM ($1).reference_type || '/' || ($1).reference_id)
                   OR (($1).kind = 'direct_read' AND incoming.resource_id IS DISTINCT FROM ($1).reference_id)
                   OR (($1).kind = 'full_insurance_plan_census' AND incoming.closure_scope NOT IN ('census', 'plan'))
                   OR (($1).kind <> 'full_insurance_plan_census' AND incoming.closure_scope IS DISTINCT FROM ($1).closure_scope)
               ), false)
          FROM payloads AS incoming LEFT JOIN extension_checks USING (resource_type, resource_id)
    $query$, resource_stage) INTO resource_count, payload_bytes, invalid USING expected;
    IF invalid THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_resource_invalid' USING ERRCODE = '23514';
    END IF;

    EXECUTE format($query$
        WITH field_contract(source_type, field_name, repeated, target_type) AS (VALUES __FIELD_CONTRACT__),
        joined AS MATERIALIZED (
            SELECT edge.*, resource.payload_json_text::jsonb AS payload,
                   resource.scope_id AS parent_scope_id, resource.closure_scope AS parent_scope,
                   string_to_array(replace(replace(edge.field_path, '[', '.'), ']', ''), '.') AS path,
                   edge.field_path ~ '^extension\[' AS extension,
                   fields.target_type AS field_target, fields.repeated
              FROM %s AS edge LEFT JOIN %s AS resource
                ON resource.acquisition_id = edge.acquisition_id
               AND resource.query_id = edge.query_id AND resource.attempt = edge.attempt
               AND resource.resource_type = edge.source_resource_type
               AND resource.resource_id = edge.source_resource_id
              LEFT JOIN field_contract AS fields
                ON fields.source_type = edge.source_resource_type
               AND fields.field_name = regexp_replace(edge.field_path, '\[[0-9]+\]$', '')
        )
        SELECT count(*)::bigint, coalesce(bool_or(
            acquisition_id IS DISTINCT FROM ($1).acquisition_id
            OR scope_id IS DISTINCT FROM ($1).scope_id
            OR query_id IS DISTINCT FROM ($1).query_id OR attempt IS DISTINCT FROM ($1).attempt_count
            OR created_at IS DISTINCT FROM transaction_timestamp()
            OR payload IS NULL OR parent_scope IS DISTINCT FROM closure_scope
            OR parent_scope_id IS DISTINCT FROM scope_id
            OR (extension AND (
                source_resource_type <> 'PractitionerRole' OR target_resource_type <> 'Organization'
                OR field_path !~ '^extension\[[0-9]+\](\.extension\[[0-9]+\]){0,5}\.valueReference$'
                OR payload #>> (path[1:cardinality(path)-1] || ARRAY['url']) IS NULL
                OR split_part(payload #>> (path[1:cardinality(path)-1] || ARRAY['url']), '|', 1) NOT IN (__NETWORK_URLS__)
                OR payload #>> (path[1:cardinality(path)-1] || ARRAY['url']) !~ '^[^|]+(\|[A-Za-z0-9.-]{1,64})?$'
            ))
            OR (NOT extension AND (field_target IS NULL OR target_resource_type IS DISTINCT FROM field_target
                OR repeated IS DISTINCT FROM (field_path ~ '\[[0-9]+\]$')))
            OR payload #>> (path || ARRAY['reference']) IS DISTINCT FROM target_resource_type || '/' || target_resource_id
            OR edge_sha256 IS DISTINCT FROM encode(sha256(convert_to(
                source_resource_type || chr(31) || source_resource_id || chr(31) || field_path
                || chr(31) || target_resource_type || chr(31) || target_resource_id, 'UTF8')), 'hex')
        ), false) FROM joined
    $query$, edge_stage, resource_stage) INTO edge_count, invalid USING expected;
    IF invalid THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_edge_invalid' USING ERRCODE = '23514';
    END IF;
    RETURN ARRAY[resource_count, edge_count, payload_bytes];
EXCEPTION WHEN invalid_text_representation OR invalid_parameter_value THEN
    RAISE EXCEPTION 'provider_directory_rooted_graph_resource_invalid' USING ERRCODE = '23514';
END;
$function$;

CREATE FUNCTION __SCHEMA__.reject_provider_directory_rooted_graph_witness_mutation()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
BEGIN
    RAISE EXCEPTION 'provider_directory_rooted_graph_witness_immutable' USING ERRCODE = '55000';
END;
$function$;

CREATE FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_storage(p_acquisition text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE header __SCHEMA__.provider_directory_rooted_graph_acquisition; relation_name text; parent_name text; kind text; grant_row record; legacy_check text; legacy_id boolean; caller name := COALESCE(NULLIF(current_setting('role'),'none'),session_user);
BEGIN
    SELECT * INTO header FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id = p_acquisition FOR UPDATE;
    IF header.status IS DISTINCT FROM 'building' THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_invalid' USING ERRCODE = '55000';
    END IF;
    SELECT pg_get_expr(conbin, conrelid) INTO legacy_check FROM pg_constraint
     WHERE conrelid='__SCHEMA__.provider_directory_rooted_graph_resource_legacy'::regclass
       AND conname='pdrg_resource_legacy_scope';
    IF legacy_check IS NULL THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_storage_conflict' USING ERRCODE='55000';
    END IF;
    EXECUTE 'SELECT ' || legacy_check || ' FROM (SELECT $1::varchar AS acquisition_id) candidate'
        INTO legacy_id USING p_acquisition;
    IF legacy_id THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_rerun_required' USING ERRCODE='55000';
    END IF;
    FOR parent_name, relation_name IN VALUES
        ('provider_directory_rooted_graph_work', 'pdrgw_' || substring(p_acquisition FROM 7)),
        ('provider_directory_rooted_graph_resource', 'pdrgr_' || substring(p_acquisition FROM 7)),
        ('provider_directory_rooted_graph_edge', 'pdrge_' || substring(p_acquisition FROM 7))
    LOOP
        IF to_regclass(format('__SCHEMA__.%I', relation_name)) IS NOT NULL THEN
            IF NOT EXISTS (SELECT 1 FROM pg_inherits
                           WHERE inhrelid = to_regclass(format('__SCHEMA__.%I', relation_name))
                             AND inhparent = to_regclass(format('__SCHEMA__.%I', parent_name))) THEN
                RAISE EXCEPTION 'provider_directory_rooted_graph_storage_conflict' USING ERRCODE = '55000';
            END IF;
            CONTINUE;
        END IF;
        EXECUTE format('CREATE TABLE __SCHEMA__.%I (LIKE __SCHEMA__.%I INCLUDING DEFAULTS %s)',
                       relation_name, parent_name,
                       CASE WHEN parent_name='provider_directory_rooted_graph_work' THEN '' ELSE 'INCLUDING INDEXES' END);
        EXECUTE format('REVOKE ALL ON __SCHEMA__.%I FROM PUBLIC', relation_name);
        FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_class c,
            LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
            WHERE c.oid = to_regclass(format('__SCHEMA__.%I', relation_name)) AND acl.grantee <> c.relowner
        LOOP
            EXECUTE format('REVOKE ALL ON __SCHEMA__.%I FROM %s', relation_name,
                CASE WHEN grant_row.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
        END LOOP;
        EXECUTE format('CREATE TRIGGER rooted_graph_copy_owner BEFORE INSERT ON __SCHEMA__.%I '
                       'FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_copy_owner()',relation_name);
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_copy_owner',relation_name);
        kind := CASE parent_name WHEN 'provider_directory_rooted_graph_resource' THEN 'resource'
                                WHEN 'provider_directory_rooted_graph_edge' THEN 'edge' ELSE 'work' END;
        IF kind='work' AND has_table_privilege(caller,'__SCHEMA__.provider_directory_rooted_graph_work','SELECT') THEN
            EXECUTE format('GRANT SELECT ON __SCHEMA__.%I TO %I',relation_name,caller);
        END IF;
        EXECUTE format('CREATE TRIGGER rooted_graph_witness_budget AFTER INSERT ON __SCHEMA__.%I '
                       'REFERENCING NEW TABLE AS inserted_%s FOR EACH STATEMENT '
                       'EXECUTE FUNCTION __SCHEMA__.account_provider_directory_rooted_graph_%s_budget()', relation_name, kind, kind);
        IF kind='work' THEN
            EXECUTE format('CREATE TRIGGER rooted_graph_work_truncate BEFORE TRUNCATE ON __SCHEMA__.%I '
                           'FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_work()',relation_name);
            EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_work_truncate',relation_name);
        ELSE
            EXECUTE format('CREATE TRIGGER rooted_graph_witness_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON __SCHEMA__.%I '
                           'FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.reject_provider_directory_rooted_graph_witness_mutation()', relation_name);
            EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_witness_immutable', relation_name);
        END IF;
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_witness_budget', relation_name);
    END LOOP;
END;
$function$;

CREATE FUNCTION __SCHEMA__.finish_provider_directory_rooted_graph_initial_storage(p_acquisition text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
DECLARE work_name text := 'pdrgw_' || substring(p_acquisition FROM 7); index_row record; index_name text;
BEGIN
    FOR index_row IN SELECT indexrelid,indisunique,pg_get_indexdef(indexrelid) AS definition,constraint_record.contype
        FROM pg_index LEFT JOIN pg_constraint constraint_record ON constraint_record.conindid=indexrelid
         AND constraint_record.conrelid=indrelid AND constraint_record.contype IN ('p','u') AND constraint_record.conparentid=0
        WHERE indrelid='__SCHEMA__.provider_directory_rooted_graph_work'::regclass ORDER BY indexrelid
    LOOP
        index_name := 'pdrgw_idx_' || md5(work_name || index_row.indexrelid);
        EXECUTE format('CREATE %s INDEX %I ON __SCHEMA__.%I %s',
            CASE WHEN index_row.indisunique THEN 'UNIQUE' ELSE '' END,index_name,work_name,
            substring(index_row.definition FROM 'USING .*$'));
        IF index_row.contype IN ('p','u') THEN
            EXECUTE format('ALTER TABLE __SCHEMA__.%I ADD CONSTRAINT %I %s USING INDEX %I',
                work_name,index_name,CASE index_row.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,index_name);
        END IF;
    END LOOP;
    EXECUTE format('CREATE INDEX %I ON __SCHEMA__.%I (acquisition_id,status,lease_expires_at,query_id)',work_name || '_claim',work_name);
    -- History keeps its native checks; new leaves acquire them after loading
    -- so subsequent lease updates preserve the same state and shape invariants.
    FOR index_row IN SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint
        WHERE conrelid='__SCHEMA__.provider_directory_rooted_graph_work_legacy'::regclass
          AND contype='c' AND conname <> 'pdrg_work_legacy_scope' ORDER BY conname
    LOOP
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ADD CONSTRAINT %I %s',work_name,index_row.conname,index_row.definition);
    END LOOP;
    EXECUTE format('ANALYZE __SCHEMA__.%I',work_name);
END; $function$;

CREATE FUNCTION __SCHEMA__.attach_provider_directory_rooted_graph_storage(p_acquisition text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE parent_name text; relation_name text; previous_lock_timeout text;
BEGIN
    PERFORM 1 FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id=p_acquisition AND status='building' AND used_work_items>=root_resource_count FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_invalid' USING ERRCODE='55000';
    END IF;
    previous_lock_timeout := current_setting('lock_timeout');
    PERFORM set_config('lock_timeout','1s',true);
    FOR parent_name, relation_name IN VALUES
        ('provider_directory_rooted_graph_work','pdrgw_' || substring(p_acquisition FROM 7)),
        ('provider_directory_rooted_graph_resource','pdrgr_' || substring(p_acquisition FROM 7)),
        ('provider_directory_rooted_graph_edge','pdrge_' || substring(p_acquisition FROM 7))
    LOOP
        IF EXISTS(SELECT FROM pg_inherits WHERE inhrelid=to_regclass(format('__SCHEMA__.%I',relation_name))
                  AND inhparent=to_regclass(format('__SCHEMA__.%I',parent_name))) THEN CONTINUE; END IF;
        IF NOT EXISTS(SELECT FROM pg_class relation JOIN pg_index index_record ON index_record.indrelid=relation.oid
            WHERE relation.oid=to_regclass(format('__SCHEMA__.%I',relation_name)) AND relation.relkind='r'
              AND relation.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
              AND index_record.indisprimary AND index_record.indisvalid AND index_record.indisready) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_indexes_incomplete' USING ERRCODE='55000';
        END IF;
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ADD CONSTRAINT pdrg_candidate_scope CHECK(acquisition_id=%L)',relation_name,p_acquisition);
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ATTACH PARTITION __SCHEMA__.%I FOR VALUES IN (%L)',parent_name,relation_name,p_acquisition);
    END LOOP;
    PERFORM set_config('lock_timeout',previous_lock_timeout,true);
END; $function$;

CREATE FUNCTION __SCHEMA__.finish_provider_directory_rooted_graph_storage(p_acquisition text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE header __SCHEMA__.provider_directory_rooted_graph_acquisition; resource_name text; edge_name text;
BEGIN
    SELECT * INTO header FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id = p_acquisition FOR UPDATE;
    IF header.status IS DISTINCT FROM 'building'
       OR EXISTS (SELECT 1 FROM __SCHEMA__.provider_directory_rooted_graph_work
                   WHERE acquisition_id = p_acquisition AND status IN ('pending', 'leased')) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_acquisition_incomplete' USING ERRCODE = '55000';
    END IF;
    PERFORM __SCHEMA__.assert_provider_directory_rooted_graph_storage(p_acquisition);
    resource_name := 'pdrgr_' || substring(p_acquisition FROM 7);
    edge_name := 'pdrge_' || substring(p_acquisition FROM 7);
    EXECUTE format('LOCK TABLE __SCHEMA__.%I, __SCHEMA__.%I IN SHARE MODE', resource_name, edge_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON __SCHEMA__.%I (acquisition_id, closure_scope, resource_type, resource_id)',
                   resource_name || '_closure', resource_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON __SCHEMA__.%I (acquisition_id, closure_scope, target_resource_type, target_resource_id)',
                   edge_name || '_target', edge_name);
    EXECUTE format('ANALYZE __SCHEMA__.%I', resource_name);
    EXECUTE format('ANALYZE __SCHEMA__.%I', edge_name);
END;
$function$;

CREATE FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_witness_stage()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE caller name; grant_row record;
BEGIN
    caller := CASE WHEN current_setting('role') = 'none' THEN session_user ELSE current_setting('role') END;
    CREATE TEMP TABLE pdrg_resource_stage (LIKE __SCHEMA__.provider_directory_rooted_graph_resource
        INCLUDING DEFAULTS) ON COMMIT DROP;
    CREATE TEMP TABLE pdrg_edge_stage (LIKE __SCHEMA__.provider_directory_rooted_graph_edge
        INCLUDING DEFAULTS) ON COMMIT DROP;
    FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_class c, LATERAL aclexplode(c.relacl) acl
        WHERE c.oid IN ('pg_temp.pdrg_resource_stage'::regclass, 'pg_temp.pdrg_edge_stage'::regclass)
          AND acl.grantee <> c.relowner
    LOOP
        EXECUTE format('REVOKE ALL ON pg_temp.pdrg_resource_stage, pg_temp.pdrg_edge_stage FROM %s',
            CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
    END LOOP;
    EXECUTE format('GRANT INSERT ON pg_temp.pdrg_resource_stage, pg_temp.pdrg_edge_stage TO %I', caller);
END;
$function$;

CREATE FUNCTION __SCHEMA__.admit_provider_directory_rooted_graph_witnesses(
    p_acquisition text, p_query text, p_attempt integer, p_lease text,
    resource_stage regclass, edge_stage regclass
) RETURNS bigint[] LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $function$
DECLARE expected __SCHEMA__.provider_directory_rooted_graph_work; counts bigint[]; stage regclass;
BEGIN
    FOREACH stage IN ARRAY ARRAY[resource_stage, edge_stage] LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid = stage
                        AND relnamespace = pg_my_temp_schema() AND relkind = 'r' AND relpersistence = 't'
                        AND relowner = (SELECT oid FROM pg_roles WHERE rolname = current_user)) THEN
            RAISE EXCEPTION 'provider_directory_rooted_graph_stage_invalid' USING ERRCODE = '23514';
        END IF;
        EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', stage);
    END LOOP;
    IF resource_stage IS DISTINCT FROM to_regclass('pg_temp.pdrg_resource_stage')
       OR edge_stage IS DISTINCT FROM to_regclass('pg_temp.pdrg_edge_stage') THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_stage_invalid' USING ERRCODE = '23514';
    END IF;
    ALTER TABLE pg_temp.pdrg_resource_stage ADD PRIMARY KEY (acquisition_id, query_id, attempt, resource_type, resource_id);
    ALTER TABLE pg_temp.pdrg_edge_stage ADD PRIMARY KEY (acquisition_id, query_id, attempt, edge_sha256);
    SELECT * INTO expected FROM __SCHEMA__.provider_directory_rooted_graph_work
     WHERE acquisition_id = p_acquisition AND query_id = p_query FOR UPDATE;
    IF expected.status IS DISTINCT FROM 'leased' OR expected.attempt_count IS DISTINCT FROM p_attempt
       OR expected.lease_token IS DISTINCT FROM p_lease OR expected.lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_lease_lost' USING ERRCODE = '55000';
    END IF;
    PERFORM __SCHEMA__.assert_provider_directory_rooted_graph_storage(p_acquisition);
    counts := __SCHEMA__.validate_provider_directory_rooted_graph_witnesses(resource_stage, edge_stage, expected);
    PERFORM set_config('healthporta.rooted_graph_action', 'witness', true),
            set_config('healthporta.rooted_graph_acquisition', p_acquisition, true),
            set_config('healthporta.rooted_graph_lease', p_lease, true);
    PERFORM 1 FROM __SCHEMA__.provider_directory_rooted_graph_acquisition
     WHERE acquisition_id = p_acquisition AND status = 'building'
       AND used_resource_rows + counts[1] <= max_resource_rows
       AND used_edge_rows + counts[2] <= max_edge_rows
       AND used_payload_bytes + counts[3] <= max_payload_bytes FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_witness_budget_exceeded' USING ERRCODE = '54000';
    END IF;
    EXECUTE format('INSERT INTO __SCHEMA__.%I SELECT * FROM %s',
                   'pdrgr_' || substring(p_acquisition FROM 7), resource_stage);
    EXECUTE format('INSERT INTO __SCHEMA__.%I SELECT * FROM %s',
                   'pdrge_' || substring(p_acquisition FROM 7), edge_stage);
    IF expected.lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_lease_lost' USING ERRCODE = '55000';
    END IF;
    DROP TABLE pg_temp.pdrg_resource_stage, pg_temp.pdrg_edge_stage;
    RETURN counts[1:2];
END;
$function$;

CREATE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_storage_seal()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
DECLARE resource_name text; edge_name text;
BEGIN
    IF NEW.status <> 'sealed' OR OLD.status <> 'building' THEN RETURN NEW; END IF;
    resource_name := 'pdrgr_' || substring(NEW.acquisition_id FROM 7);
    edge_name := 'pdrge_' || substring(NEW.acquisition_id FROM 7);
    IF NOT EXISTS (SELECT 1 FROM pg_index
        WHERE indexrelid=to_regclass(format('__SCHEMA__.%I', resource_name || '_closure'))
          AND indrelid=to_regclass(format('__SCHEMA__.%I', resource_name)) AND indisvalid AND indisready)
       OR NOT EXISTS (SELECT 1 FROM pg_index
        WHERE indexrelid=to_regclass(format('__SCHEMA__.%I', edge_name || '_target'))
          AND indrelid=to_regclass(format('__SCHEMA__.%I', edge_name)) AND indisvalid AND indisready) THEN
        RAISE EXCEPTION 'provider_directory_rooted_graph_indexes_incomplete' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END;
$function$;
REVOKE ALL ON FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_storage_seal() FROM PUBLIC;
CREATE TRIGGER rooted_graph_storage_seal BEFORE UPDATE OF status
    ON __SCHEMA__.provider_directory_rooted_graph_acquisition FOR EACH ROW
    EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_storage_seal();

REVOKE ALL ON FUNCTION
    __SCHEMA__.assert_provider_directory_rooted_graph_storage(text),
    __SCHEMA__.prepare_provider_directory_rooted_graph_storage(text),
    __SCHEMA__.finish_provider_directory_rooted_graph_storage(text),
    __SCHEMA__.prepare_provider_directory_rooted_graph_witness_stage(),
    __SCHEMA__.admit_provider_directory_rooted_graph_witnesses(text,text,integer,text,regclass,regclass),
    __SCHEMA__.validate_provider_directory_rooted_graph_witnesses(regclass,regclass,__SCHEMA__.provider_directory_rooted_graph_work),
    __SCHEMA__.reject_provider_directory_rooted_graph_witness_mutation(),
    __SCHEMA__.prepare_provider_directory_rooted_graph_work_stage(),
    __SCHEMA__.admit_provider_directory_rooted_graph_work(text,text),
    __SCHEMA__.initialize_provider_directory_rooted_graph_work(text),
    __SCHEMA__.read_provider_directory_rooted_graph_initial_work(text,text,integer),
    __SCHEMA__.attach_provider_directory_rooted_graph_storage(text),
    __SCHEMA__.finish_provider_directory_rooted_graph_initial_storage(text),
    __SCHEMA__.validate_provider_directory_rooted_graph_stage_checks(regclass,regclass),
    __SCHEMA__.guard_provider_directory_rooted_graph_copy_owner(),
    __SCHEMA__.validate_provider_directory_rooted_graph_work_stage(text,text)
FROM PUBLIC;
DO $acl$
DECLARE relation record; grant_row record; columns_sql text; role_sql text;
BEGIN
    FOR grant_row IN SELECT procedure.oid::regprocedure AS signature, acl.grantee
        FROM pg_proc procedure CROSS JOIN LATERAL
            aclexplode(COALESCE(proacl,acldefault('f',proowner))) acl
        WHERE pronamespace='__SCHEMA__'::regnamespace AND acl.grantee<>proowner
          AND proname IN ('assert_provider_directory_rooted_graph_storage',
              'prepare_provider_directory_rooted_graph_storage','finish_provider_directory_rooted_graph_storage',
              'prepare_provider_directory_rooted_graph_witness_stage','admit_provider_directory_rooted_graph_witnesses',
              'validate_provider_directory_rooted_graph_witnesses','reject_provider_directory_rooted_graph_witness_mutation',
              'prepare_provider_directory_rooted_graph_work_stage','admit_provider_directory_rooted_graph_work',
              'initialize_provider_directory_rooted_graph_work','validate_provider_directory_rooted_graph_work_stage',
              'read_provider_directory_rooted_graph_initial_work',
              'attach_provider_directory_rooted_graph_storage','finish_provider_directory_rooted_graph_initial_storage',
              'validate_provider_directory_rooted_graph_stage_checks','guard_provider_directory_rooted_graph_copy_owner',
              'guard_provider_directory_rooted_graph_storage_seal')
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s',grant_row.signature,
            CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
    END LOOP;
    FOR relation IN SELECT c.oid,c.relname,c.relowner FROM pg_class c
        WHERE c.oid IN ('__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_work'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_work_legacy'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_resource_legacy'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_edge_legacy'::regclass)
           OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent IN (
                       '__SCHEMA__.provider_directory_rooted_graph_resource'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_edge'::regclass,
                       '__SCHEMA__.provider_directory_rooted_graph_work'::regclass)) LOOP
        SELECT string_agg(quote_ident(attname), ',') INTO columns_sql FROM pg_attribute
         WHERE attrelid=relation.oid AND attnum>0 AND NOT attisdropped;
        FOR grant_row IN
            SELECT DISTINCT grantee FROM pdrg_writer_acl WHERE grantee<>relation.relowner
            UNION SELECT acl.grantee FROM pg_class c,LATERAL aclexplode(c.relacl) acl
                   WHERE c.oid=relation.oid AND acl.grantee<>relation.relowner
            UNION SELECT 0 LOOP
            role_sql := CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END;
            EXECUTE format('REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON __SCHEMA__.%I FROM %s',relation.relname,role_sql);
            EXECUTE format('REVOKE INSERT(%s),UPDATE(%s) ON __SCHEMA__.%I FROM %s',columns_sql,columns_sql,relation.relname,role_sql);
        END LOOP;
        EXECUTE format('CREATE TRIGGER rooted_graph_copy_owner BEFORE INSERT ON __SCHEMA__.%I '
                       'FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_provider_directory_rooted_graph_copy_owner()',relation.relname);
        EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_copy_owner',relation.relname);
        IF relation.relname<>'provider_directory_rooted_graph_work' THEN
            EXECUTE format('CREATE TRIGGER rooted_graph_witness_immutable BEFORE UPDATE OR DELETE OR TRUNCATE '
                'ON __SCHEMA__.%I FOR EACH STATEMENT '
                'EXECUTE FUNCTION __SCHEMA__.reject_provider_directory_rooted_graph_witness_mutation()',relation.relname);
            EXECUTE format('ALTER TABLE __SCHEMA__.%I ENABLE ALWAYS TRIGGER rooted_graph_witness_immutable',relation.relname);
        END IF;
    END LOOP;
    FOR grant_row IN SELECT DISTINCT * FROM pdrg_writer_acl LOOP
        role_sql := CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END;
        IF grant_row.privilege_type='INSERT' AND grant_row.column_name IS NULL THEN
            EXECUTE 'GRANT EXECUTE ON FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_storage(text), '
                '__SCHEMA__.finish_provider_directory_rooted_graph_storage(text) TO ' || role_sql;
            IF grant_row.relname='provider_directory_rooted_graph_work' THEN
                EXECUTE 'GRANT EXECUTE ON FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_work_stage(), '
                    '__SCHEMA__.admit_provider_directory_rooted_graph_work(text,text), '
                    '__SCHEMA__.attach_provider_directory_rooted_graph_storage(text), '
                    '__SCHEMA__.read_provider_directory_rooted_graph_initial_work(text,text,integer), '
                    '__SCHEMA__.initialize_provider_directory_rooted_graph_work(text) TO ' || role_sql;
            ELSE
                EXECUTE 'GRANT EXECUTE ON FUNCTION __SCHEMA__.prepare_provider_directory_rooted_graph_witness_stage(), '
                    '__SCHEMA__.admit_provider_directory_rooted_graph_witnesses(text,text,integer,text,regclass,regclass) TO ' || role_sql;
            END IF;
        ELSIF grant_row.privilege_type='SELECT'
              OR (grant_row.privilege_type='UPDATE' AND grant_row.relname='provider_directory_rooted_graph_work') THEN
            IF grant_row.column_name IS NULL THEN
                EXECUTE format('GRANT %s ON __SCHEMA__.%I TO %s',grant_row.privilege_type,grant_row.relname,role_sql);
            ELSE
                EXECUTE format('GRANT %s(%I) ON __SCHEMA__.%I TO %s',grant_row.privilege_type,grant_row.column_name,grant_row.relname,role_sql);
            END IF;
        END IF;
    END LOOP;
END; $acl$;
DROP TABLE pdrg_writer_acl;
