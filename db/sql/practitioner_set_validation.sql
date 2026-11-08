-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).

LOCK TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition", "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work", "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $dependencies$ BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_class WHERE oid IN (
        '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition"'::regclass,
        '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
        '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
        AND (relkind<>'r' OR relrowsecurity OR relforcerowsecurity
            OR EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=pg_class.oid))) THEN
        RAISE EXCEPTION 'practitioner_storage_shape_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_trigger WHERE NOT tgisinternal AND tgtype & 1=1
        AND tgrelid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
        AND tgname NOT IN ('pd_uhc_flex_practitioner_work_guard','pd_uhc_flex_practitioner_resource_guard')) THEN
        RAISE EXCEPTION 'practitioner_storage_guard_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_constraint WHERE NOT convalidated
        AND conrelid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)) THEN
        RAISE EXCEPTION 'practitioner_storage_constraint_unvalidated' USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_index WHERE (NOT indisvalid OR NOT indisready)
        AND indrelid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)) THEN
        RAISE EXCEPTION 'practitioner_storage_index_invalid' USING ERRCODE='55000';
    END IF;
    IF EXISTS(SELECT FROM pg_catalog.pg_constraint WHERE contype='f'
        AND conrelid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
        AND conname NOT IN ('pd_uhc_flex_practitioner_work_acquisition_fkey',
            'pd_uhc_flex_practitioner_work_member_fkey','pd_uhc_flex_practitioner_resource_work_fkey')) THEN
        RAISE EXCEPTION 'practitioner_resource_dependencies_changed' USING ERRCODE='55000';
    END IF;
    IF EXISTS(SELECT FROM pg_catalog.pg_constraint WHERE contype='f'
        AND confrelid='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
        OR EXISTS(SELECT FROM pg_catalog.pg_constraint WHERE contype='f'
            AND confrelid='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass
            AND conrelid<>'"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
        OR EXISTS(SELECT FROM pg_catalog.pg_depend d
            JOIN pg_catalog.pg_rewrite r ON r.oid=d.objid
            JOIN pg_catalog.pg_class c ON c.oid=r.ev_class
            WHERE d.classid='pg_catalog.pg_rewrite'::regclass
              AND d.refclassid='pg_catalog.pg_class'::regclass AND c.relkind IN ('v','m')
              AND d.refobjid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass,
                  '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass)) THEN
        RAISE EXCEPTION 'practitioner_resource_dependencies_changed' USING ERRCODE='55000';
    END IF;
END; $dependencies$;
CREATE TEMP TABLE pd_practitioner_writer_acl ON COMMIT DROP AS
    SELECT DISTINCT c.relname, a.grantee, a.privilege_type, NULL::name AS column_name
    FROM pg_catalog.pg_class c
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
        c.relacl, pg_catalog.acldefault('r', c.relowner))) a
    WHERE c.oid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass, '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass);
INSERT INTO pd_practitioner_writer_acl
    SELECT DISTINCT c.relname, a.grantee, a.privilege_type, col.attname
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute col ON col.attrelid=c.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(col.attacl) a
    WHERE c.oid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass, '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass);
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" DROP CONSTRAINT pd_uhc_flex_practitioner_resource_work_fkey;
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" DROP CONSTRAINT pd_uhc_flex_practitioner_work_acquisition_fkey,
    DROP CONSTRAINT pd_uhc_flex_practitioner_work_member_fkey;
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" RENAME TO "provider_directory_uhc_flex_practitioner_work_legacy";
DROP TRIGGER pd_uhc_flex_practitioner_work_guard ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work_legacy";
CREATE TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" (LIKE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work_legacy" INCLUDING DEFAULTS)
    PARTITION BY LIST(acquisition_id);
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" ADD CONSTRAINT pd_uhc_flex_practitioner_set_work_pkey
    PRIMARY KEY(acquisition_id,npi);
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" ADD CONSTRAINT pd_uhc_flex_practitioner_set_work_cohort_key
    UNIQUE(acquisition_id,cohort_id,npi);
CREATE INDEX pd_uhc_flex_practitioner_set_work_claim_idx ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"(acquisition_id,status,lease_expires_at,npi);
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" RENAME TO "provider_directory_uhc_flex_practitioner_resource_legacy";
DROP TRIGGER pd_uhc_flex_practitioner_resource_guard ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy";
DROP TRIGGER pd_uhc_flex_practitioner_resource_truncate ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy";
CREATE TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" (LIKE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy" INCLUDING DEFAULTS)
    PARTITION BY LIST(acquisition_id);
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" ADD CONSTRAINT pd_uhc_flex_practitioner_set_pkey
    PRIMARY KEY(acquisition_id,npi,attempt,resource_id);
DO $migration$
DECLARE legacy_ids text; original_owner name; suffix text;
BEGIN
    FOREACH suffix IN ARRAY ARRAY['work','resource'] LOOP
    SELECT pg_catalog.pg_get_userbyid(relowner) INTO original_owner
      FROM pg_catalog.pg_class WHERE oid=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix||'_legacy'));
    EXECUTE pg_catalog.format('ALTER TABLE %I.%I OWNER TO %I','__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix,original_owner);
    SELECT pg_catalog.string_agg(pg_catalog.quote_literal(acquisition_id),',')
      INTO legacy_ids FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition";
    IF legacy_ids IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%s)',
            '__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix,
            '__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix||'_legacy',legacy_ids);
    END IF;
    END LOOP;
END; $migration$;
CREATE TRIGGER pd_uhc_flex_practitioner_work_guard BEFORE UPDATE OR DELETE ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"
    FOR EACH ROW EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_practitioner_work"();
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" ENABLE ALWAYS TRIGGER pd_uhc_flex_practitioner_work_guard;
CREATE TRIGGER pd_uhc_flex_practitioner_work_truncate BEFORE TRUNCATE ON "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"
    FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_practitioner_work"();
ALTER TABLE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" ENABLE ALWAYS TRIGGER pd_uhc_flex_practitioner_work_truncate;

CREATE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_candidate_insert"()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $function$
BEGIN
    IF TG_ARGV[0]='frozen' OR CURRENT_USER IS DISTINCT FROM
        (SELECT pg_catalog.pg_get_userbyid(relowner) FROM pg_catalog.pg_class WHERE oid=TG_RELID) THEN
        RAISE EXCEPTION 'practitioner_direct_write_forbidden' USING ERRCODE='42501';
    END IF;
    RETURN NULL;
END; $function$;
DO $insert_guards$
DECLARE suffix text;
BEGIN
    FOREACH suffix IN ARRAY ARRAY['work','work_legacy','resource','resource_legacy'] LOOP
        EXECUTE pg_catalog.format('CREATE TRIGGER pd_uhc_flex_candidate_insert BEFORE INSERT ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_candidate_insert"(''frozen'')',
            '__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix);
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_uhc_flex_candidate_insert',
            '__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix);
    END LOOP;
END; $insert_guards$;

CREATE FUNCTION "__SCHEMA__"."prepare_pd_uhc_flex_practitioner_work"(aid text,cid text)
RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE header record; candidate_table text; candidate_oid oid; caller name; legacy_scope text; legacy_candidate boolean; grantee record;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(aid,0));
    SELECT * INTO header FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition" WHERE acquisition_id=aid AND cohort_id=cid FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'practitioner_acquisition_invalid' USING ERRCODE='23514'; END IF;
    IF header.status='sealed' THEN RETURN NULL; END IF;
    SELECT pg_catalog.pg_get_expr(conbin,conrelid) INTO legacy_scope
        FROM pg_catalog.pg_constraint
        WHERE conrelid='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work_legacy"'::regclass
          AND conname='pd_uhc_flex_legacy_scope';
    EXECUTE 'SELECT '||legacy_scope||' FROM (SELECT $1::varchar AS acquisition_id) candidate'
        INTO legacy_candidate USING aid;
    IF legacy_candidate THEN
        RAISE EXCEPTION 'practitioner_acquisition_rerun_required' USING ERRCODE='55000';
    END IF;
    IF header.status <> 'building' THEN
        RAISE EXCEPTION 'practitioner_workset_invalid' USING ERRCODE='23514';
    END IF;
    candidate_table:='pd_uhc_flex_pw_'||pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(aid,'UTF8')),'hex'),1,40);
    candidate_oid:=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__',candidate_table));
    IF candidate_oid IS NOT NULL THEN
        IF EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass AND inhrelid=candidate_oid) THEN RETURN NULL; END IF;
        RAISE EXCEPTION 'practitioner_candidate_identity_invalid' USING ERRCODE='23514';
    END IF;
    EXECUTE pg_catalog.format('CREATE TABLE %I.%I (LIKE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" INCLUDING DEFAULTS)','__SCHEMA__',candidate_table);
    FOR grantee IN SELECT DISTINCT a.grantee FROM pg_catalog.pg_class c,
        LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE c.oid=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__',candidate_table)) AND a.grantee<>c.relowner
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON %I.%I FROM %s','__SCHEMA__',candidate_table,
            CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grantee.grantee)) END);
    END LOOP;
    caller:=CASE WHEN pg_catalog.current_setting('role')='none' THEN SESSION_USER ELSE pg_catalog.current_setting('role') END;
    EXECUTE pg_catalog.format('GRANT INSERT ON %I.%I TO %I','__SCHEMA__',candidate_table,caller);
    RETURN candidate_table;
END; $function$;

CREATE FUNCTION "__SCHEMA__"."initialize_pd_uhc_flex_practitioner_work"(aid text,cid text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE header record; work_table text; resource_table text; candidate_oid oid; grantee record; invalid boolean; total bigint; suffix text; candidate_table text; previous_lock_timeout text; predicate record;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(aid,0));
    SELECT * INTO header FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition" WHERE acquisition_id=aid AND cohort_id=cid FOR SHARE;
    IF NOT FOUND OR header.status<>'building' THEN RAISE EXCEPTION 'practitioner_acquisition_invalid' USING ERRCODE='23514'; END IF;
    work_table:='pd_uhc_flex_pw_'||pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(aid,'UTF8')),'hex'),1,40);
    resource_table:='pd_uhc_flex_pr_'||pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(aid,'UTF8')),'hex'),1,40);
    candidate_oid:=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__',work_table));
    IF NOT EXISTS(SELECT FROM pg_catalog.pg_class WHERE oid=candidate_oid AND relkind='r'
        AND relowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER))
        OR EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=candidate_oid) THEN
        RAISE EXCEPTION 'practitioner_candidate_identity_invalid' USING ERRCODE='23514';
    END IF;
    EXECUTE pg_catalog.format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE','__SCHEMA__',work_table);
    FOR grantee IN SELECT DISTINCT a.grantee FROM pg_catalog.pg_class c,
        LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE c.oid=candidate_oid AND a.grantee<>c.relowner
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON %I.%I FROM %s','__SCHEMA__',work_table,
            CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grantee.grantee)) END);
    END LOOP;
    EXECUTE pg_catalog.format('CREATE TRIGGER pd_uhc_flex_candidate_insert BEFORE INSERT ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_candidate_insert"(''frozen'')','__SCHEMA__',work_table);
    EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_uhc_flex_candidate_insert','__SCHEMA__',work_table);
    EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD PRIMARY KEY(acquisition_id,npi), ADD UNIQUE(acquisition_id,cohort_id,npi)','__SCHEMA__',work_table);
    EXECUTE pg_catalog.format('CREATE INDEX ON %I.%I(acquisition_id,status,lease_expires_at,npi)','__SCHEMA__',work_table);
    EXECUTE pg_catalog.format('ANALYZE %I.%I','__SCHEMA__',work_table);
    FOR predicate IN SELECT conname,pg_catalog.pg_get_constraintdef(oid) AS definition FROM pg_catalog.pg_constraint
        WHERE conrelid='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work_legacy"'::regclass
        AND contype='c' AND conname<>'pd_uhc_flex_legacy_scope'
    LOOP
        BEGIN
            EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                '__SCHEMA__',work_table,predicate.conname,predicate.definition);
        EXCEPTION WHEN check_violation THEN
            RAISE EXCEPTION 'practitioner_workset_invalid' USING ERRCODE='23514';
        END;
    END LOOP;
    EXECUTE pg_catalog.format('SELECT count(*),COALESCE(bool_or(acquisition_id IS DISTINCT FROM $1 OR cohort_id IS DISTINCT FROM $2
        OR status IS DISTINCT FROM ''pending'' OR attempt_count IS DISTINCT FROM 0 OR npi NOT BETWEEN 1000000000 AND 2999999999
        OR lease_token IS NOT NULL OR lease_expires_at IS NOT NULL OR lease_heartbeat_at IS NOT NULL
        OR result_sha256 IS NOT NULL OR resource_count IS NOT NULL OR error_code IS NOT NULL
        OR terminal_record_sha256 IS NOT NULL OR terminal_at IS NOT NULL
        OR created_at IS DISTINCT FROM transaction_timestamp() OR updated_at IS DISTINCT FROM transaction_timestamp()),false)
        FROM %I.%I','__SCHEMA__',work_table) INTO total,invalid USING aid,cid;
    IF invalid OR total IS DISTINCT FROM header.expected_npi_count THEN RAISE EXCEPTION 'practitioner_workset_invalid' USING ERRCODE='23514'; END IF;
    EXECUTE pg_catalog.format('SELECT EXISTS(SELECT FROM %I.%I w WHERE NOT EXISTS(SELECT FROM "__SCHEMA__"."provider_directory_uhc_flex_npi_member" m WHERE m.cohort_id=$1 AND m.npi=w.npi))
        OR EXISTS(SELECT FROM "__SCHEMA__"."provider_directory_uhc_flex_npi_member" m WHERE m.cohort_id=$1 AND NOT EXISTS(SELECT FROM %I.%I w WHERE w.acquisition_id=$2 AND w.npi=m.npi))',
        '__SCHEMA__',work_table,'__SCHEMA__',work_table) INTO invalid USING cid,aid;
    IF invalid THEN RAISE EXCEPTION 'practitioner_workset_invalid' USING ERRCODE='23514'; END IF;
    EXECUTE pg_catalog.format('CREATE TABLE %I.%I (LIKE "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" INCLUDING DEFAULTS INCLUDING INDEXES)','__SCHEMA__',resource_table);
    EXECUTE pg_catalog.format('CREATE TRIGGER pd_uhc_flex_resource_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_practitioner_resource"()','__SCHEMA__',resource_table);
    EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_uhc_flex_resource_immutable','__SCHEMA__',resource_table);
    EXECUTE pg_catalog.format('CREATE TRIGGER pd_uhc_flex_candidate_insert BEFORE INSERT ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_candidate_insert"(''admission'')','__SCHEMA__',resource_table);
    EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_uhc_flex_candidate_insert','__SCHEMA__',resource_table);
    FOR grantee IN SELECT DISTINCT a.grantee FROM pg_catalog.pg_class c,
        LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
        WHERE c.oid=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__',resource_table)) AND a.grantee<>c.relowner
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON %I.%I FROM %s','__SCHEMA__',resource_table,
            CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grantee.grantee)) END);
    END LOOP;
    previous_lock_timeout:=pg_catalog.current_setting('lock_timeout');
    PERFORM pg_catalog.set_config('lock_timeout','1s',true);
    FOREACH suffix IN ARRAY ARRAY['work','resource'] LOOP
        candidate_table:=CASE suffix WHEN 'work' THEN work_table ELSE resource_table END;
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK(acquisition_id=%L)',
            '__SCHEMA__',candidate_table,candidate_table||'_scope',aid);
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%L)',
            '__SCHEMA__','provider_directory_uhc_flex_practitioner_'||suffix,'__SCHEMA__',candidate_table,aid);
    END LOOP;
    PERFORM pg_catalog.set_config('lock_timeout',previous_lock_timeout,true);
END; $function$;

CREATE FUNCTION "__SCHEMA__"."prepare_pd_uhc_flex_practitioner_stage"()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE caller name;
BEGIN
    caller:=CASE WHEN pg_catalog.current_setting('role')='none' THEN SESSION_USER
        ELSE pg_catalog.current_setting('role') END;
    CREATE TEMP TABLE pd_uhc_flex_practitioner_stage(resource_id text,payload_sha256 text,payload_json_text text) ON COMMIT DROP;
    EXECUTE pg_catalog.format('GRANT INSERT ON pg_temp.pd_uhc_flex_practitioner_stage TO %I',caller);
END; $function$;

CREATE FUNCTION "__SCHEMA__"."admit_pd_uhc_flex_practitioner_stage"(
    aid text,cid text,requested_npi bigint,claim_attempt integer,claim_token text)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
DECLARE total bigint; invalid boolean; candidate_table text; candidate_oid oid; predicate record;
BEGIN
    IF NOT EXISTS(SELECT FROM pg_catalog.pg_class WHERE oid=pg_catalog.to_regclass('pg_temp.pd_uhc_flex_practitioner_stage')
        AND relnamespace=pg_catalog.pg_my_temp_schema() AND relkind='r'
        AND relowner=(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER)) THEN
        RAISE EXCEPTION 'practitioner_stage_invalid' USING ERRCODE='23514';
    END IF;
    LOCK TABLE pg_temp.pd_uhc_flex_practitioner_stage IN ACCESS EXCLUSIVE MODE;
    FOR predicate IN SELECT pg_catalog.pg_get_expr(conbin,conrelid) AS expression FROM pg_catalog.pg_constraint
        WHERE conrelid='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy"'::regclass
        AND contype='c' AND conname<>'pd_uhc_flex_legacy_scope'
    LOOP
        EXECUTE 'SELECT EXISTS(SELECT FROM (SELECT $1::varchar AS acquisition_id,$2::varchar AS cohort_id,
            $3::bigint AS npi,$4::integer AS attempt,stage.*,transaction_timestamp() AS created_at
            FROM pg_temp.pd_uhc_flex_practitioner_stage stage) candidate WHERE ('||predicate.expression||') IS FALSE)'
            INTO invalid USING aid,cid,requested_npi,claim_attempt;
        IF invalid THEN RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid' USING ERRCODE='23514'; END IF;
    END LOOP;
    SELECT count(*),count(*)<>count(DISTINCT resource_id) OR
        COALESCE(bool_or(resource_id IS NULL OR resource_id !~ '^[A-Za-z0-9.-]{1,64}$'
            OR payload_sha256 IS NULL OR payload_sha256 !~ '^[0-9a-f]{64}$'
            OR payload_json_text IS NULL OR octet_length(payload_json_text) NOT BETWEEN 2 AND 1048576),false)
        INTO total,invalid FROM pg_temp.pd_uhc_flex_practitioner_stage;
    IF total NOT BETWEEN 1 AND 16 OR invalid THEN
        RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid' USING ERRCODE='23514';
    END IF;
    BEGIN
        WITH parsed AS MATERIALIZED (SELECT *,payload_json_text::jsonb AS payload FROM pg_temp.pd_uhc_flex_practitioner_stage)
        SELECT EXISTS(SELECT FROM parsed p WHERE payload_sha256 IS DISTINCT FROM
            encode(sha256(convert_to(payload_json_text,'UTF8')),'hex')
            OR payload->>'resourceType' IS DISTINCT FROM 'Practitioner'
            OR payload->>'id' IS DISTINCT FROM resource_id
            OR jsonb_typeof(payload->'identifier') IS DISTINCT FROM 'array'
            OR NOT EXISTS(SELECT FROM jsonb_array_elements(CASE WHEN jsonb_typeof(payload->'identifier')='array'
                THEN payload->'identifier' ELSE '[]'::jsonb END) i
                WHERE i->>'system'='http://hl7.org/fhir/sid/us-npi'
                AND jsonb_typeof(i->'value')='string' AND i->>'value'=requested_npi::text)
            OR EXISTS(SELECT FROM jsonb_array_elements(CASE WHEN jsonb_typeof(payload->'identifier')='array'
                THEN payload->'identifier' ELSE '[]'::jsonb END) i
                WHERE i->>'system'='http://hl7.org/fhir/sid/us-npi'
                AND (jsonb_typeof(i->'value') IS DISTINCT FROM 'string'
                    OR i->>'value' IS DISTINCT FROM requested_npi::text))) INTO invalid;
    EXCEPTION WHEN invalid_text_representation THEN
        RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid' USING ERRCODE='23514';
    END;
    IF invalid THEN RAISE EXCEPTION 'provider_directory_uhc_flex_resource_invalid' USING ERRCODE='23514'; END IF;
    PERFORM FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_acquisition" WHERE acquisition_id=aid AND cohort_id=cid AND status='building' FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'practitioner_acquisition_invalid' USING ERRCODE='23514'; END IF;
    PERFORM FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_work" WHERE acquisition_id=aid AND cohort_id=cid AND npi=requested_npi
        AND status='leased' AND attempt_count=claim_attempt AND lease_token=claim_token
        AND lease_expires_at>clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'provider_directory_uhc_flex_lease_lost' USING ERRCODE='55000'; END IF;
    candidate_table:='pd_uhc_flex_pr_' || pg_catalog.substr(pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(aid, 'UTF8')), 'hex'), 1, 40);
    candidate_oid:=pg_catalog.to_regclass(pg_catalog.format('%I.%I','__SCHEMA__',candidate_table));
    IF NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass AND inhrelid=candidate_oid)
        OR EXISTS(SELECT FROM "__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource" WHERE acquisition_id=aid AND npi=requested_npi AND attempt=claim_attempt) THEN
        RAISE EXCEPTION 'practitioner_candidate_invalid' USING ERRCODE='23514';
    END IF;
    EXECUTE pg_catalog.format('INSERT INTO %I.%I(acquisition_id,cohort_id,npi,attempt,resource_id,payload_sha256,payload_json_text)
        SELECT $1,$2,$3,$4,resource_id,payload_sha256,payload_json_text FROM pg_temp.pd_uhc_flex_practitioner_stage',
        '__SCHEMA__',candidate_table) USING aid,cid,requested_npi,claim_attempt;
    DROP TABLE pg_temp.pd_uhc_flex_practitioner_stage;
    RETURN total;
END; $function$;

REVOKE ALL ON FUNCTION "__SCHEMA__"."prepare_pd_uhc_flex_practitioner_stage"(),"__SCHEMA__"."admit_pd_uhc_flex_practitioner_stage"(text,text,bigint,integer,text),"__SCHEMA__"."initialize_pd_uhc_flex_practitioner_work"(text,text),"__SCHEMA__"."prepare_pd_uhc_flex_practitioner_work"(text,text) FROM PUBLIC;
DO $function_acl$
DECLARE grant_row record;
BEGIN
    FOR grant_row IN SELECT procedure.oid::regprocedure AS signature, acl.grantee
        FROM pg_catalog.pg_proc procedure CROSS JOIN LATERAL
            pg_catalog.aclexplode(COALESCE(proacl,pg_catalog.acldefault('f',proowner))) acl
        WHERE pronamespace='"__SCHEMA__"'::regnamespace AND acl.grantee<>proowner
          AND proname IN ('prepare_pd_uhc_flex_practitioner_stage',
              'admit_pd_uhc_flex_practitioner_stage','initialize_pd_uhc_flex_practitioner_work','prepare_pd_uhc_flex_practitioner_work',
              'guard_pd_uhc_flex_candidate_insert')
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s',grant_row.signature,
            CASE WHEN grant_row.grantee=0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grant_row.grantee)) END);
    END LOOP;
END; $function_acl$;
CREATE OR REPLACE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_practitioner_resource"()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
BEGIN RAISE EXCEPTION 'provider_directory_uhc_flex_resource_immutable' USING ERRCODE='55000'; END;
$function$;
DO $migration$
DECLARE grant_row record; relation record; role_sql text; columns_sql text;
BEGIN
    FOR relation IN SELECT oid,relname,relowner FROM pg_catalog.pg_class
        WHERE oid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
    LOOP
        FOR grant_row IN SELECT DISTINCT acl.grantee FROM pg_catalog.pg_class,
            LATERAL pg_catalog.aclexplode(COALESCE(relacl,pg_catalog.acldefault('r',relowner))) acl
            WHERE oid=relation.oid AND acl.grantee<>relation.relowner
        LOOP
            EXECUTE pg_catalog.format('REVOKE ALL ON %I.%I FROM %s','__SCHEMA__',relation.relname,
                CASE WHEN grant_row.grantee=0 THEN 'PUBLIC' ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grant_row.grantee)) END);
        END LOOP;
    END LOOP;
    FOR grant_row IN SELECT DISTINCT * FROM pd_practitioner_writer_acl LOOP
        role_sql:=CASE WHEN grant_row.grantee=0 THEN 'PUBLIC'
            ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grant_row.grantee)) END;
        IF grant_row.privilege_type='INSERT' AND grant_row.grantee<>0 AND grant_row.column_name IS NULL THEN
            IF grant_row.relname='provider_directory_uhc_flex_practitioner_work' THEN
                EXECUTE 'GRANT EXECUTE ON FUNCTION "__SCHEMA__"."initialize_pd_uhc_flex_practitioner_work"(text,text),"__SCHEMA__"."prepare_pd_uhc_flex_practitioner_work"(text,text) TO '||role_sql;
            ELSE
                EXECUTE 'GRANT EXECUTE ON FUNCTION "__SCHEMA__"."prepare_pd_uhc_flex_practitioner_stage"(),"__SCHEMA__"."admit_pd_uhc_flex_practitioner_stage"(text,text,bigint,integer,text) TO '||role_sql;
            END IF;
        END IF;
        IF grant_row.privilege_type='SELECT' OR (grant_row.relname='provider_directory_uhc_flex_practitioner_work' AND grant_row.privilege_type='UPDATE') THEN
            IF grant_row.column_name IS NULL THEN
                EXECUTE pg_catalog.format('GRANT %s ON TABLE %I.%I TO %s',grant_row.privilege_type,'__SCHEMA__',grant_row.relname,role_sql);
            ELSE
                EXECUTE pg_catalog.format('GRANT %s(%I) ON TABLE %I.%I TO %s',grant_row.privilege_type,grant_row.column_name,'__SCHEMA__',grant_row.relname,role_sql);
            END IF;
        END IF;
    END LOOP;
    FOR relation IN SELECT c.oid,c.relname FROM pg_catalog.pg_class c WHERE c.oid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass,'"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy"'::regclass)
        OR c.oid IN (SELECT inhrelid FROM pg_catalog.pg_inherits WHERE inhparent='"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass)
    LOOP
        EXECUTE pg_catalog.format('CREATE TRIGGER pd_uhc_flex_resource_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION "__SCHEMA__"."guard_pd_uhc_flex_practitioner_resource"()',
            '__SCHEMA__',relation.relname);
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I ENABLE ALWAYS TRIGGER pd_uhc_flex_resource_immutable','__SCHEMA__',relation.relname);
    END LOOP;
    FOR relation IN SELECT c.oid,c.relname,c.relowner FROM pg_catalog.pg_class c WHERE c.oid IN ('"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource"'::regclass,'"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work"'::regclass,
            '"__SCHEMA__"."provider_directory_uhc_flex_practitioner_resource_legacy"'::regclass,'"__SCHEMA__"."provider_directory_uhc_flex_practitioner_work_legacy"'::regclass)
    LOOP
        SELECT string_agg(quote_ident(attname),',') INTO columns_sql FROM pg_catalog.pg_attribute
            WHERE attrelid=relation.oid AND attnum>0 AND NOT attisdropped;
        FOR grant_row IN SELECT DISTINCT grantee FROM pd_practitioner_writer_acl
            UNION SELECT a.grantee FROM pg_catalog.pg_class c,
                LATERAL pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a
                WHERE c.oid=relation.oid
            UNION SELECT a.grantee FROM pg_catalog.pg_attribute col,
                LATERAL pg_catalog.aclexplode(col.attacl) a WHERE col.attrelid=relation.oid
            UNION SELECT 0 LOOP
            IF grant_row.grantee=relation.relowner THEN CONTINUE; END IF;
            role_sql:=CASE WHEN grant_row.grantee=0 THEN 'PUBLIC'
                ELSE pg_catalog.quote_ident(pg_catalog.pg_get_userbyid(grant_row.grantee)) END;
            EXECUTE pg_catalog.format('REVOKE INSERT ON %I.%I FROM %s','__SCHEMA__',relation.relname,role_sql);
            EXECUTE pg_catalog.format('REVOKE INSERT(%s) ON %I.%I FROM %s',columns_sql,'__SCHEMA__',relation.relname,role_sql);
            IF relation.relname NOT IN ('provider_directory_uhc_flex_practitioner_work','provider_directory_uhc_flex_practitioner_work_legacy') THEN
                EXECUTE pg_catalog.format('REVOKE UPDATE,DELETE,TRUNCATE ON %I.%I FROM %s','__SCHEMA__',relation.relname,role_sql);
                EXECUTE pg_catalog.format('REVOKE UPDATE(%s) ON %I.%I FROM %s',columns_sql,'__SCHEMA__',relation.relname,role_sql);
            END IF;
        END LOOP;
    END LOOP;
END; $migration$;
DROP TABLE pd_practitioner_writer_acl;
