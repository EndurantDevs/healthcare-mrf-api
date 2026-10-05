-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

CREATE FUNCTION __SCHEMA__.guard_pd_dataset_migration_owner() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $fn$
BEGIN
    IF NOT EXISTS(SELECT FROM pg_class relation JOIN pg_roles caller ON caller.rolname=current_user
                  WHERE relation.oid=TG_RELID AND (relation.relowner=caller.oid OR caller.rolsuper)) THEN
        RAISE EXCEPTION 'provider_directory_dataset_migration_unauthorized' USING ERRCODE='42501';
    END IF;
    RETURN NULL;
END; $fn$;

CREATE FUNCTION __SCHEMA__.deny_pd_dataset_candidate_write() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $fn$
BEGIN
    RAISE EXCEPTION 'provider_directory_dataset_candidate_immutable' USING ERRCODE='55000';
END; $fn$;

CREATE FUNCTION __SCHEMA__.prepare_pd_generic_dataset_storage() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE
    parent_name text; candidate_name text; grant_row record; event_spec text[];
BEGIN
    IF NEW.publication_metadata_json->>'publication_contract_id' IN (
        'healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1',
        'healthporta.provider-directory.rooted-graph-publication.v1',
        'healthporta.provider-directory.rooted-graph-publication.v2'
    ) THEN
        RETURN NEW;
    END IF;
    FOREACH parent_name IN ARRAY ARRAY[
        'provider_directory_dataset_resource', 'provider_directory_dataset_insurance_plan',
        'provider_directory_dataset_network_plan', 'provider_directory_dataset_affiliation_organization'
    ] LOOP
        candidate_name := 'pd_generic_' || md5(parent_name || ':' || NEW.dataset_id);
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING ALL)',
                       __SCHEMA_LITERAL__, candidate_name, __SCHEMA_LITERAL__, parent_name);
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT pd_dataset_bound CHECK (dataset_id=%L)',
                       __SCHEMA_LITERAL__, candidate_name, NEW.dataset_id);
        FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_class relation,
            LATERAL aclexplode(relacl) acl
            WHERE relation.oid=to_regclass(format('%I.%I',__SCHEMA_LITERAL__,candidate_name))
              AND acl.grantee<>relowner LOOP
            EXECUTE format('REVOKE ALL ON %I.%I FROM %s', __SCHEMA_LITERAL__, candidate_name,
                CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
        END LOOP;
        FOREACH event_spec SLICE 1 IN ARRAY ARRAY[['INSERT','NEW'],['UPDATE','NEW'],['UPDATE','OLD'],['DELETE','OLD']] LOOP
            EXECUTE format('CREATE TRIGGER %I AFTER %s ON %I.%I REFERENCING %s TABLE AS affected_rows FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.guard_pd_dataset_candidate_parent(%L,%L)',
                'a_pd_candidate_' || lower(event_spec[1]) || '_' || lower(event_spec[2]),
                event_spec[1], __SCHEMA_LITERAL__, candidate_name, event_spec[2], parent_name, event_spec[2]);
        END LOOP;
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%L)',
                       __SCHEMA_LITERAL__, parent_name, __SCHEMA_LITERAL__, candidate_name, NEW.dataset_id);
    END LOOP;
    RETURN NEW;
END; $fn$;

CREATE FUNCTION __SCHEMA__.pd_dataset_candidate_tables(kind text) RETURNS text[]
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $fn$
    SELECT CASE kind
        WHEN 'generic' THEN ARRAY['provider_directory_dataset_resource','provider_directory_dataset_insurance_plan','provider_directory_dataset_network_plan','provider_directory_dataset_affiliation_organization']
        WHEN 'practitioner' THEN ARRAY['provider_directory_dataset_resource','provider_directory_uhc_flex_practitioner_dataset_resource']
        WHEN 'rooted' THEN ARRAY['provider_directory_dataset_resource','provider_directory_dataset_insurance_plan','provider_directory_dataset_network_plan','provider_directory_dataset_affiliation_organization','provider_directory_rooted_graph_dataset_resource']
    END;
$fn$;

CREATE FUNCTION __SCHEMA__.guard_pd_dataset_candidate_parent() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE parent_name text := TG_ARGV[0]; image_kind text := TG_ARGV[1]; check_predicate text; invalid boolean;
BEGIN
    PERFORM dataset.dataset_id FROM __SCHEMA__.provider_directory_endpoint_dataset dataset
        JOIN (SELECT DISTINCT dataset_id FROM affected_rows) changed USING(dataset_id)
        ORDER BY dataset.dataset_id FOR SHARE OF dataset;
    IF TG_OP<>'DELETE' AND EXISTS (
        SELECT FROM affected_rows changed LEFT JOIN __SCHEMA__.provider_directory_endpoint_dataset dataset USING(dataset_id)
        WHERE dataset.dataset_id IS NULL
    ) THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_relationship' USING ERRCODE='23503';
    END IF;
    IF parent_name='provider_directory_dataset_resource' AND image_kind='NEW'
       AND EXISTS(SELECT FROM affected_rows changed JOIN __SCHEMA__.provider_directory_endpoint_dataset dataset USING(dataset_id)
           WHERE dataset.status IN ('validated','published','superseded','verification_baseline',
                                    'verification_mismatch','acquisition_abandoned','acquisition_retired')) THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_immutable' USING ERRCODE='55000';
    END IF;
    IF parent_name<>'provider_directory_dataset_resource' AND image_kind='NEW' THEN
        SELECT string_agg('NOT (' || pg_get_expr(conbin,conrelid) || ')',' OR ') INTO check_predicate
          FROM pg_constraint,__SCHEMA__.pd_dataset_migration_plan
         WHERE singleton AND conrelid=(plan#>>ARRAY['tables',parent_name,'original_oid'])::oid
           AND contype='c' AND conname<>'pd_dataset_history_bound';
        IF check_predicate IS NOT NULL THEN
            EXECUTE 'SELECT EXISTS(SELECT FROM affected_rows WHERE ' || check_predicate || ')' INTO invalid;
            IF invalid THEN RAISE EXCEPTION 'provider_directory_dataset_candidate_content' USING ERRCODE='23514'; END IF;
        END IF;
    END IF;
    IF parent_name='provider_directory_dataset_resource' AND image_kind='OLD' THEN
      IF EXISTS(
        SELECT FROM affected_rows changed JOIN (
            SELECT dataset_id,resource_type,resource_id FROM __SCHEMA__.provider_directory_rooted_graph_dataset_resource
            UNION ALL SELECT dataset_id,resource_type,resource_id FROM __SCHEMA__.provider_directory_uhc_flex_practitioner_dataset_resource
        ) evidence USING(dataset_id,resource_type,resource_id)
        WHERE NOT EXISTS(SELECT FROM __SCHEMA__.provider_directory_dataset_resource retained
                         WHERE (retained.dataset_id,retained.resource_type,retained.resource_id)=
                               (changed.dataset_id,changed.resource_type,changed.resource_id))
    ) THEN
        RAISE EXCEPTION 'provider_directory_dataset_reference_exists' USING ERRCODE='23503';
      END IF;
      IF EXISTS(
        SELECT FROM affected_rows changed JOIN __SCHEMA__.provider_directory_endpoint_dataset dataset USING(dataset_id)
        WHERE dataset.status IN ('validated','published','acquisition_abandoned','acquisition_retired')
           OR (dataset.status='superseded' AND TG_OP<>'DELETE')
           OR (dataset.completion_proof_required_version=3
               AND dataset.status IN ('superseded','verification_baseline','verification_mismatch'))
      ) THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_immutable' USING ERRCODE='55000';
      END IF;
    END IF;
    IF EXISTS (SELECT 1 FROM affected_rows changed
               JOIN __SCHEMA__.provider_directory_endpoint_dataset dataset USING (dataset_id)
               WHERE dataset.status='acquisition_retired') THEN
        RAISE EXCEPTION 'provider_directory_terminal_root_retirement_child_immutable' USING ERRCODE='55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM (SELECT DISTINCT dataset_id FROM affected_rows) changed
        JOIN pg_class candidate
          ON candidate.relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=TG_TABLE_SCHEMA)
         AND candidate.relname='pd_ds_' || md5(TG_TABLE_NAME || ':' || changed.dataset_id)
        JOIN pg_inherits inheritance
          ON inheritance.inhrelid=candidate.oid AND inheritance.inhparent=TG_RELID
    ) THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_immutable' USING ERRCODE='55000';
    END IF;
    RETURN NULL;
END; $fn$;

CREATE FUNCTION __SCHEMA__.prepare_pd_dataset_candidate(requested_dataset text, kind text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE
    names text[] := __SCHEMA__.pd_dataset_candidate_tables(kind);
    parent_name text; candidate_name text; header_name text; header_valid boolean; grant_row record;
    caller name := COALESCE(NULLIF(current_setting('role'), 'none'), session_user);
    result jsonb := '{}';
BEGIN
    IF NOT has_table_privilege(caller, '__SCHEMA__.provider_directory_dataset_resource', 'INSERT') THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_unauthorized' USING ERRCODE='42501';
    END IF;
    IF names IS NULL OR requested_dataset IS NULL THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_invalid' USING ERRCODE='23514';
    END IF;
    header_name := CASE kind WHEN 'rooted' THEN 'provider_directory_rooted_graph_dataset'
        ELSE 'provider_directory_uhc_flex_practitioner_dataset' END;
    EXECUTE format('SELECT status=''building'' AND is_current IS FALSE FROM %I.%I WHERE dataset_id=$1 FOR UPDATE',
                   __SCHEMA_LITERAL__, header_name) INTO header_valid USING requested_dataset;
    IF header_valid IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_state' USING ERRCODE='55000';
    END IF;
    FOREACH parent_name IN ARRAY names LOOP
        candidate_name := 'pd_ds_' || md5(parent_name || ':' || requested_dataset);
        EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING STORAGE)',
                       __SCHEMA_LITERAL__, candidate_name, __SCHEMA_LITERAL__, parent_name);
        FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_class relation,
            LATERAL aclexplode(relacl) acl
            WHERE relation.oid=to_regclass(format('%I.%I',__SCHEMA_LITERAL__,candidate_name))
              AND acl.grantee<>relowner LOOP
            EXECUTE format('REVOKE ALL ON %I.%I FROM %s', __SCHEMA_LITERAL__, candidate_name,
                CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
        END LOOP;
        EXECUTE format('GRANT SELECT, INSERT, DELETE ON %I.%I TO %I', __SCHEMA_LITERAL__, candidate_name, caller);
        result := result || jsonb_build_object(parent_name, candidate_name);
    END LOOP;
    RETURN result;
END; $fn$;

CREATE FUNCTION __SCHEMA__.validate_pd_dataset_bulk_relationships(requested_dataset text, kind text)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $fn$
DECLARE migration_plan jsonb; parent_name text; candidate_name text; target_name text;
    relationship jsonb; predicate text; nonnull_predicate text; invalid boolean; check_predicate text;
BEGIN
    SELECT plan INTO STRICT migration_plan FROM __SCHEMA__.pd_dataset_migration_plan WHERE singleton;
    FOREACH parent_name IN ARRAY __SCHEMA__.pd_dataset_candidate_tables(kind) LOOP
        candidate_name := CASE WHEN kind='generic' THEN parent_name ELSE 'pd_ds_' || md5(parent_name || ':' || requested_dataset) END;
        SELECT string_agg('NOT (' || pg_get_expr(conbin,conrelid) || ')',' OR ') INTO check_predicate
          FROM pg_constraint WHERE conrelid=(migration_plan#>>ARRAY['tables',parent_name,'original_oid'])::oid
            AND contype='c' AND conname<>'pd_dataset_history_bound';
        IF check_predicate IS NOT NULL THEN
            EXECUTE format('SELECT EXISTS(SELECT FROM __SCHEMA__.%I WHERE dataset_id=$1 AND (%s))',candidate_name,check_predicate) INTO invalid USING requested_dataset;
            IF invalid THEN RAISE EXCEPTION 'provider_directory_dataset_candidate_content' USING ERRCODE='23514'; END IF;
        END IF;
        FOR relationship IN SELECT value FROM jsonb_array_elements(migration_plan->'relationships')
            WHERE value->>'source_table'=parent_name
        LOOP
            SELECT string_agg(format('source.%I=target.%I',source_column,relationship->'target_columns'->>(position-1)::int),' AND '),
                   string_agg(format('source.%I IS NOT NULL',source_column),' AND ')
              INTO predicate,nonnull_predicate
              FROM jsonb_array_elements_text(relationship->'source_columns') WITH ORDINALITY pair(source_column,position);
            target_name := relationship->>'target_table';
            IF kind<>'generic' AND target_name=ANY(__SCHEMA__.pd_dataset_candidate_tables(kind)) THEN
                target_name := 'pd_ds_' || md5(target_name || ':' || requested_dataset);
            END IF;
            EXECUTE format('SELECT EXISTS(SELECT FROM __SCHEMA__.%I source WHERE source.dataset_id=$1 AND %s '
                'AND NOT EXISTS(SELECT FROM %I.%I target WHERE %s))',candidate_name,nonnull_predicate,
                relationship->>'target_schema',target_name,predicate) INTO invalid USING requested_dataset;
            IF invalid THEN RAISE EXCEPTION 'provider_directory_dataset_candidate_relationship' USING ERRCODE='23503'; END IF;
        END LOOP;
    END LOOP;
END; $fn$;

CREATE FUNCTION __SCHEMA__.guard_pd_generic_dataset_finalization() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
BEGIN
    IF NEW.status IN ('validated','published') AND OLD.status IS DISTINCT FROM NEW.status
       AND COALESCE(NEW.publication_metadata_json->>'publication_contract_id','') NOT IN (
           'healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1',
           'healthporta.provider-directory.rooted-graph-publication.v1',
           'healthporta.provider-directory.rooted-graph-publication.v2') THEN
        PERFORM __SCHEMA__.validate_pd_dataset_bulk_relationships(NEW.dataset_id,'generic');
    END IF;
    RETURN NEW;
END; $fn$;

CREATE FUNCTION __SCHEMA__.guard_pd_dataset_parent_key_update() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE relationship jsonb; referenced boolean;
BEGIN
    IF to_jsonb(NEW)->TG_ARGV[0] IS NOT DISTINCT FROM to_jsonb(OLD)->TG_ARGV[0] THEN RETURN NEW; END IF;
    FOR relationship IN SELECT value FROM __SCHEMA__.pd_dataset_migration_plan,
        LATERAL jsonb_array_elements(plan->'relationships')
        WHERE singleton AND value->>'target_table'=TG_TABLE_NAME AND value->>'target_schema'=TG_TABLE_SCHEMA
    LOOP
        EXECUTE format('SELECT EXISTS(SELECT FROM __SCHEMA__.%I WHERE %I=$1)',
                       relationship->>'source_table',relationship->'source_columns'->>0)
            INTO referenced USING to_jsonb(OLD)->>TG_ARGV[0];
        IF referenced THEN RAISE EXCEPTION 'provider_directory_dataset_reference_exists' USING ERRCODE='23503'; END IF;
    END LOOP;
    RETURN NEW;
END; $fn$;

CREATE FUNCTION __SCHEMA__.guard_pd_dataset_bulk_parent_delete() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE relationship jsonb; predicate text; invalid boolean;
BEGIN
    FOR relationship IN SELECT value FROM __SCHEMA__.pd_dataset_migration_plan,
        LATERAL jsonb_array_elements(plan->'relationships')
        WHERE singleton AND value->>'target_table'=TG_TABLE_NAME AND value->>'target_schema'=TG_TABLE_SCHEMA
    LOOP
        SELECT string_agg(format('source.%I=target.%I',source_column,relationship->'target_columns'->>(position-1)::int),' AND ')
          INTO predicate FROM jsonb_array_elements_text(relationship->'source_columns') WITH ORDINALITY pair(source_column,position);
        IF relationship->>'delete_action'='c' THEN
            EXECUTE format('DELETE FROM __SCHEMA__.%I source USING deleted_rows target WHERE %s',relationship->>'source_table',predicate);
        ELSE
            EXECUTE format('SELECT EXISTS(SELECT FROM __SCHEMA__.%I source JOIN deleted_rows target ON %s)',
                relationship->>'source_table',predicate) INTO invalid;
            IF invalid THEN RAISE EXCEPTION 'provider_directory_dataset_reference_exists' USING ERRCODE='23503'; END IF;
        END IF;
    END LOOP;
    RETURN NULL;
END; $fn$;

CREATE FUNCTION __SCHEMA__.validate_pd_dataset_candidate(requested_dataset text, kind text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE
    resource_name text := 'pd_ds_' || md5('provider_directory_dataset_resource:' || requested_dataset);
    evidence_name text; invalid boolean;
BEGIN
    EXECUTE format($query$
        SELECT EXISTS (SELECT 1 FROM %I.%I resource
            WHERE resource.dataset_id IS DISTINCT FROM $1
               OR resource.resource_type NOT IN ('Practitioner','PractitionerRole',
                   'OrganizationAffiliation','Organization','Location','HealthcareService','InsurancePlan','Endpoint')
               OR resource.resource_id !~ '^[A-Za-z0-9.-]{1,64}$'
               OR resource.payload_hash !~ '^[0-9a-f]{64}$'
               OR jsonb_typeof(resource.payload_json::jsonb) IS DISTINCT FROM 'object'
               OR resource.payload_json::jsonb->>'resource_id' IS DISTINCT FROM resource.resource_id
               OR resource.acquired_resource_sha256 IS NOT NULL)
    $query$, __SCHEMA_LITERAL__, resource_name) INTO invalid USING requested_dataset;
    IF invalid THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_content' USING ERRCODE='23514';
    END IF;
    IF kind='rooted' THEN
        evidence_name := 'pd_ds_' || md5('provider_directory_rooted_graph_dataset_resource:' || requested_dataset);
        EXECUTE format($query$
            SELECT EXISTS (SELECT 1 FROM %I.%I resource FULL JOIN %I.%I evidence
                USING(dataset_id,resource_type,resource_id)
                WHERE resource.dataset_id IS NULL OR evidence.dataset_id IS NULL
                   OR resource.payload_hash IS DISTINCT FROM evidence.published_payload_hash
                   OR evidence.root_dataset_id IS DISTINCT FROM header.root_dataset_id
                   OR evidence.publication_acquisition_id IS DISTINCT FROM header.publication_acquisition_id)
            FROM __SCHEMA__.provider_directory_rooted_graph_dataset header WHERE header.dataset_id=$1
        $query$, __SCHEMA_LITERAL__,resource_name,__SCHEMA_LITERAL__,evidence_name)
        INTO invalid USING requested_dataset;
    ELSE
        evidence_name := 'pd_ds_' || md5('provider_directory_uhc_flex_practitioner_dataset_resource:' || requested_dataset);
        EXECUTE format($query$
            SELECT EXISTS (SELECT 1 FROM %I.%I resource FULL JOIN %I.%I evidence
                USING(dataset_id,resource_type,resource_id)
                WHERE resource.dataset_id IS NULL OR evidence.dataset_id IS NULL
                   OR resource.resource_type IS DISTINCT FROM 'Practitioner'
                   OR resource.payload_hash IS DISTINCT FROM evidence.payload_hash
                   OR resource.payload_json::jsonb->>'npi' IS DISTINCT FROM evidence.requested_npi::text
                   OR resource.payload_json::jsonb ?| ARRAY['_acquired_resource_sha256','last_seen_run_id','observed_at','source_id','updated_at']
                   OR evidence.candidate_acquisition_id IS DISTINCT FROM header.candidate_acquisition_id)
            FROM __SCHEMA__.provider_directory_uhc_flex_practitioner_dataset header WHERE header.dataset_id=$1
        $query$, __SCHEMA_LITERAL__,resource_name,__SCHEMA_LITERAL__,evidence_name)
        INTO invalid USING requested_dataset;
    END IF;
    IF invalid IS DISTINCT FROM FALSE THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_content' USING ERRCODE='23514';
    END IF;
    PERFORM __SCHEMA__.validate_pd_dataset_bulk_relationships(requested_dataset,kind);
END; $fn$;

CREATE FUNCTION __SCHEMA__.finish_pd_dataset_candidate(requested_dataset text, kind text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
DECLARE
    names text[] := __SCHEMA__.pd_dataset_candidate_tables(kind);
    parent_name text; candidate_name text; relation_oid oid; index_row record; grant_row record;
    header_name text; header_valid boolean; invalid boolean;
    caller name := COALESCE(NULLIF(current_setting('role'), 'none'), session_user);
    previous_lock_timeout text;
BEGIN
    IF NOT has_table_privilege(caller, '__SCHEMA__.provider_directory_dataset_resource', 'INSERT') THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_unauthorized' USING ERRCODE='42501';
    END IF;
    IF names IS NULL OR requested_dataset IS NULL THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_invalid' USING ERRCODE='23514';
    END IF;
    header_name := CASE kind WHEN 'rooted' THEN 'provider_directory_rooted_graph_dataset'
        ELSE 'provider_directory_uhc_flex_practitioner_dataset' END;
    EXECUTE format('SELECT status=''building'' AND is_current IS FALSE FROM %I.%I WHERE dataset_id=$1 FOR UPDATE',
                   __SCHEMA_LITERAL__, header_name) INTO header_valid USING requested_dataset;
    IF header_valid IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'provider_directory_dataset_candidate_state' USING ERRCODE='55000';
    END IF;
    FOREACH parent_name IN ARRAY names LOOP
        candidate_name := 'pd_ds_' || md5(parent_name || ':' || requested_dataset);
        relation_oid := to_regclass(format('%I.%I', __SCHEMA_LITERAL__, candidate_name));
        IF NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=relation_oid AND relkind='r'
                       AND relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))
           OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=relation_oid) THEN
            RAISE EXCEPTION 'provider_directory_dataset_candidate_invalid' USING ERRCODE='23514';
        END IF;
        EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE', __SCHEMA_LITERAL__, candidate_name);
    END LOOP;
    FOREACH parent_name IN ARRAY names LOOP
        candidate_name := 'pd_ds_' || md5(parent_name || ':' || requested_dataset);
        relation_oid := to_regclass(format('%I.%I', __SCHEMA_LITERAL__, candidate_name));
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I WHERE dataset_id IS DISTINCT FROM $1)',
                       __SCHEMA_LITERAL__, candidate_name) INTO invalid USING requested_dataset;
        IF invalid THEN
            RAISE EXCEPTION 'provider_directory_dataset_candidate_foreign_row' USING ERRCODE='23514';
        END IF;
        EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT pd_dataset_candidate_bound CHECK (dataset_id=%L)',
                       __SCHEMA_LITERAL__, candidate_name, requested_dataset);
        FOR index_row IN SELECT indexrelid, indisunique, pg_get_indexdef(indexrelid) AS definition,
                constraint_record.contype, constraint_record.condeferrable, constraint_record.condeferred
            FROM pg_index LEFT JOIN pg_constraint constraint_record
              ON constraint_record.conindid=indexrelid AND constraint_record.conrelid=indrelid
             AND constraint_record.contype IN ('p','u') AND constraint_record.conparentid=0
            WHERE indrelid=to_regclass(format('%I.%I', __SCHEMA_LITERAL__, parent_name))
            ORDER BY indexrelid LOOP
            EXECUTE format('CREATE %s INDEX %I ON %I.%I %s',
                CASE WHEN index_row.indisunique THEN 'UNIQUE' ELSE '' END,
                candidate_name || '_' || index_row.indexrelid,
                __SCHEMA_LITERAL__, candidate_name, substring(index_row.definition FROM 'USING .*$'));
            IF index_row.contype IN ('p','u') THEN
                EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s USING INDEX %I %s',
                    __SCHEMA_LITERAL__,candidate_name,candidate_name || '_' || index_row.indexrelid,
                    CASE index_row.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                    candidate_name || '_' || index_row.indexrelid,
                    CASE WHEN index_row.condeferrable THEN 'DEFERRABLE INITIALLY ' ||
                        CASE WHEN index_row.condeferred THEN 'DEFERRED' ELSE 'IMMEDIATE' END
                        ELSE 'NOT DEFERRABLE' END);
            END IF;
        END LOOP;
        EXECUTE format('ANALYZE %I.%I', __SCHEMA_LITERAL__, candidate_name);
        FOR grant_row IN SELECT DISTINCT a.grantee FROM pg_class c,
            LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
            WHERE c.oid=relation_oid AND a.grantee<>c.relowner LOOP
            EXECUTE format('REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON %I.%I FROM %s',
                __SCHEMA_LITERAL__, candidate_name,
                CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grant_row.grantee)) END);
        END LOOP;
        EXECUTE format('CREATE TRIGGER pd_dataset_sealed BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION __SCHEMA__.deny_pd_dataset_candidate_write()',
                       __SCHEMA_LITERAL__, candidate_name);
        EXECUTE format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_dataset_sealed', __SCHEMA_LITERAL__, candidate_name);
    END LOOP;
    PERFORM __SCHEMA__.validate_pd_dataset_candidate(requested_dataset,kind);
    previous_lock_timeout := current_setting('lock_timeout');
    PERFORM set_config('lock_timeout', '1s', true);
    FOREACH parent_name IN ARRAY names LOOP
        candidate_name := 'pd_ds_' || md5(parent_name || ':' || requested_dataset);
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%L)',
                       __SCHEMA_LITERAL__, parent_name, __SCHEMA_LITERAL__, candidate_name, requested_dataset);
    END LOOP;
    PERFORM set_config('lock_timeout', previous_lock_timeout, true);
END; $fn$;

CREATE FUNCTION __SCHEMA__.prepare_pd_rooted_dataset_candidate(dataset text) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    SELECT __SCHEMA__.prepare_pd_dataset_candidate(dataset,'rooted');
$fn$;
CREATE FUNCTION __SCHEMA__.finish_pd_rooted_dataset_candidate(dataset text) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    SELECT __SCHEMA__.finish_pd_dataset_candidate(dataset,'rooted');
$fn$;
CREATE FUNCTION __SCHEMA__.prepare_pd_practitioner_dataset_candidate(dataset text) RETURNS jsonb
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    SELECT __SCHEMA__.prepare_pd_dataset_candidate(dataset,'practitioner');
$fn$;
CREATE FUNCTION __SCHEMA__.finish_pd_practitioner_dataset_candidate(dataset text) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$
    SELECT __SCHEMA__.finish_pd_dataset_candidate(dataset,'practitioner');
$fn$;
