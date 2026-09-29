# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Require one native-authority-bound receipt for CMS serving transitions."""

import os

from alembic import op
from process.provider_directory_cms_receipt_guard import profile_transition_body

revision = "20260930100000_cms_npd_serving_receipt"
down_revision = "20260930090000_cms_npd_candidate_coverage"
branch_labels = None
depends_on = None
_TABLE = "provider_directory_cms_serving_receipt"
_PROFILE_FIELDS = (
    "status",
    "operation",
    "control_generation",
    "generation_id",
    "selection_proof_id",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_vector_hash",
    "source_context_vector_hash",
    "executable_plan_hash",
    "profile_as_of",
    "evidence_target_oid",
    "profile_target_oid",
    "evidence_rows",
    "profile_rows",
)
_ADDRESS_RELATIONS = (
    "entity_address_unified",
    "entity_address_evidence",
    "entity_address_plan_bridge",
    "entity_address_network_bridge",
    "entity_address_procedure_bridge",
    "entity_address_medication_bridge",
    "facility_anchor_npi_candidate",
)
_DOCTOR_RELATIONS = ("doctor_clinician_address", "cms_doctor_education", "cms_doctor_group_site")


def _json_fields(alias, fields):
    return "jsonb_build_object(" + ",".join(f"'{field}',{alias}.{field}" for field in fields) + ")"


def _authority(alias, *, doctors=False):
    fields = ("local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "relation_oids")
    value = _json_fields(alias, (("importer_id",) if doctors else ()) + fields)
    return (
        value
        + f" || jsonb_build_object('published_at',to_char({alias}.published_at AT TIME ZONE 'UTC',"
        + '\'YYYY-MM-DD"T"HH24:MI:SS.US"Z"\'))'
    )


def _create_native_readers(schema):
    """Expose fixed native scalar fields and canonical UTC timestamps without new authority."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_oids(names text[]) RETURNS jsonb
        LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
        SELECT jsonb_agg(to_regclass(format('%I.%I','{schema.strip(chr(34))}',name))::oid::bigint ORDER BY position)
        FROM unnest(names) WITH ORDINALITY AS relation(name,position) $$""")
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_native_snapshot() RETURNS jsonb
        LANGUAGE sql STABLE SET search_path=pg_catalog AS $$ SELECT jsonb_build_object(
        'profile',(SELECT {_json_fields("p", _PROFILE_FIELDS)}
            FROM {schema}.provider_directory_profile_serving_generation p WHERE singleton_key='global'),
        'address',(SELECT {_authority("a")} FROM {schema}.entity_address_result_generation a WHERE singleton),
        'doctors',(SELECT {_authority("d", doctors=True)} FROM {schema}.reference_family_result_generation d
            WHERE importer_id='cms-doctors'),
        'alias_generation',(SELECT generation FROM {schema}.address_alias_state_v1 WHERE singleton),
        'overlay_oid',to_regclass('{schema}.provider_directory_address_overlay')::oid::bigint) $$""")
    address_names = "ARRAY[" + ",".join(f"'{name}'" for name in _ADDRESS_RELATIONS) + "]"
    doctor_names = "ARRAY[" + ",".join(f"'{name}'" for name in _DOCTOR_RELATIONS) + "]"
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_native_matches(p jsonb) RETURNS boolean
        LANGUAGE sql STABLE SET search_path=pg_catalog AS $$ SELECT COALESCE(
        p={schema}.cms_serving_native_snapshot()
        AND (p->'doctors'->>'local_generation')::bigint>0
        AND (p->'doctors'->>'origin_generation')::bigint>0
        AND p->'doctors'->>'published_at' IS NOT NULL
        AND p->'doctors'->'relation_oids'={schema}.cms_serving_oids({doctor_names})
        AND (p->'address'->>'origin_generation' IS NULL OR
             p->'address'->'relation_oids'={schema}.cms_serving_oids({address_names}))
        AND (p->'profile'='null'::jsonb OR (
            (p->'profile'->>'profile_target_oid')::bigint=to_regclass('{schema}.provider_directory_profile')::oid::bigint
            AND (p->'profile'->>'evidence_target_oid')::bigint=to_regclass('{schema}.provider_directory_profile_evidence')::oid::bigint)),false) $$""")


def _create_table(schema):
    """Keep immutable results in one predecessor chain indexed by the existing native pair."""
    table = f"{schema}.{_TABLE}"
    op.execute(f"""CREATE TABLE {table} (
        receipt_id varchar(64) PRIMARY KEY,
        payload jsonb NOT NULL,
        publication_xid xid8 NOT NULL DEFAULT pg_current_xact_id() UNIQUE,
        predecessor_receipt_id varchar(64) GENERATED ALWAYS AS (payload->>'predecessor_receipt_id') STORED
            REFERENCES {table}(receipt_id),
        address_lineage_id uuid GENERATED ALWAYS AS ((payload->'address'->>'local_lineage_id')::uuid) STORED NOT NULL,
        address_generation bigint GENERATED ALWAYS AS ((payload->'address'->>'local_generation')::bigint) STORED NOT NULL,
        profile_generation_id text GENERATED ALWAYS AS (payload->'profile'->>'generation_id') STORED NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        CHECK (receipt_id=encode(sha256(convert_to(payload::text,'UTF8')),'hex')),
        UNIQUE(address_lineage_id,address_generation,profile_generation_id),
        UNIQUE(predecessor_receipt_id)
    )""")
    op.execute(
        f"CREATE UNIQUE INDEX cms_serving_receipt_one_root ON {table} ((true)) WHERE predecessor_receipt_id IS NULL"
    )
    op.execute(
        f"CREATE INDEX cms_serving_receipt_profile_history ON {table} (profile_generation_id,created_at,receipt_id)"
    )
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_receipt_immutable() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        RAISE EXCEPTION 'cms_serving_receipt_immutable'; END $$""")
    op.execute(f"""CREATE TRIGGER cms_serving_receipt_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
        ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {schema}.cms_serving_receipt_immutable()""")


def _coverage_match_sql(schema):
    """Match the immutable coverage and relationship receipts to the exact current CMS pin."""
    return f"""EXISTS (SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
                JOIN {schema}.provider_directory_source s ON s.source_id='cms-npd' AND s.endpoint_id=d.endpoint_id
                JOIN {schema}.provider_directory_cms_candidate_coverage c ON c.dataset_id=d.dataset_id
                JOIN {schema}.provider_directory_cms_serving_coverage served ON served.dataset_id=d.dataset_id
                JOIN {schema}.provider_directory_cms_npd_relationship_receipt r ON r.dataset_id=d.dataset_id
                WHERE d.dataset_id=p->'cms'->>'dataset_id' AND d.endpoint_id=p->'cms'->>'endpoint_id'
                  AND d.dataset_hash=p->'cms'->>'dataset_hash'
                  AND d.acquisition_root_run_id=p->'cms'->>'acquisition_root_run_id'
                  AND d.is_current AND d.status='published' AND c.proof_version=2 AND served.proof_version=2
                  AND c.release_id=p->'cms'->>'release_id' AND served.release_id=c.release_id
                  AND c.dataset_hash=d.dataset_hash AND served.dataset_hash=d.dataset_hash
                  AND served.published_at=d.published_at
                  AND c.admission_sha256=d.content_proof_admission_sha256 AND c.metadata_sha256=d.publication_metadata_sha256
                  AND r.release_id=c.release_id AND r.relationship_count=c.relationship_count
                  AND r.projection_contract='cms-npd-reference-ledger-v1')"""


def _create_result_check(schema):
    """Check final native pointers, exact CMS proof, and the bounded desired source vector."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_receipt_matches(p jsonb) RETURNS boolean
        LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
        DECLARE native jsonb; actual_vector jsonb;
        BEGIN
            native := p - ARRAY['contract_version','predecessor_receipt_id','expected_incumbent','cms',
                                'desired_datasets','selection','archive'];
            IF p->>'contract_version' IS DISTINCT FROM '1'
              OR NOT {schema}.cms_serving_native_matches(native)
              OR NOT {schema}.cms_serving_archive_result_matches(p)
              OR (p->'address'->>'local_generation')::bigint<=0
              OR (p->'address'->>'origin_generation')::bigint IS NULL
              OR p->'address'->>'published_at' IS NULL
              OR (p->>'overlay_oid')::bigint IS NULL
              OR NOT ((p->'profile'->>'status'='published' AND p->'profile'->>'operation'='publish')
                OR (p->'profile'->>'status'='purged' AND p->'profile'->>'operation'='purge'))
              OR p->'profile'->>'selection_proof_id' IS DISTINCT FROM p->'selection'->>'proof_id'
              OR NOT (p->'selection' ?& ARRAY['proof_id','fingerprint','catalog_digest'])
              OR (SELECT count(*) FROM jsonb_object_keys(p->'selection'))<>3
              OR (SELECT count(*) FROM jsonb_object_keys(p->'cms'))<>7
              OR EXISTS (SELECT 1 FROM jsonb_each_text(p->'selection') kv WHERE kv.value !~ '^[0-9a-f]{{64}}$')
              OR jsonb_array_length(p->'desired_datasets') NOT BETWEEN 0 AND 256
              OR p->'cms'->>'source_id' IS DISTINCT FROM 'cms-npd'
              OR p->'cms'->>'proof_version' IS DISTINCT FROM '2'
            THEN RETURN false; END IF;
            SELECT source_vector_json INTO actual_vector FROM {schema}.provider_directory_profile_serving_generation
              WHERE singleton_key='global';
            IF actual_vector IS DISTINCT FROM (SELECT COALESCE(jsonb_agg(jsonb_build_object('source_id',v->>'source_id',
                'dataset_id',v->>'dataset_id') ORDER BY v->>'source_id'),'[]'::jsonb) FROM jsonb_array_elements(p->'desired_datasets') v)
              OR EXISTS (SELECT 1 FROM jsonb_array_elements(p->'desired_datasets') v GROUP BY v->>'source_id' HAVING count(*)<>1)
              OR EXISTS (SELECT 1 FROM jsonb_array_elements(p->'desired_datasets') v WHERE
                NOT v ?& ARRAY['source_id','endpoint_id','dataset_id','dataset_hash','acquisition_root_run_id']
                OR (SELECT count(*) FROM jsonb_object_keys(v))<>5)
              OR EXISTS (SELECT 1 FROM jsonb_array_elements(p->'desired_datasets') v
                LEFT JOIN {schema}.provider_directory_endpoint_dataset d ON d.dataset_id=v->>'dataset_id'
                LEFT JOIN {schema}.provider_directory_source s ON s.source_id=v->>'source_id'
                WHERE d.dataset_id IS NULL OR d.endpoint_id IS DISTINCT FROM v->>'endpoint_id'
                  OR d.dataset_hash IS DISTINCT FROM v->>'dataset_hash'
                  OR d.acquisition_root_run_id IS DISTINCT FROM v->>'acquisition_root_run_id'
                  OR d.status<>'published' OR NOT d.is_current OR s.endpoint_id IS DISTINCT FROM d.endpoint_id)
            THEN RETURN false; END IF;
            RETURN {_coverage_match_sql(schema)}
              AND EXISTS (SELECT 1 FROM {schema}.address_alias_artifact_state_v1
                WHERE artifact_name='provider_directory_address_overlay' AND generation=(p->>'alias_generation')::bigint);
        END $$""")


def _create_archive_check(schema):
    """Check new archive results in their owner transaction, retaining immutable predecessor history."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_archive_result_matches(p jsonb) RETURNS boolean
        LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
        DECLARE a jsonb; prior jsonb; current_revision bigint; mutation_xid bigint;
        BEGIN
            SELECT payload INTO prior FROM {schema}.{_TABLE} WHERE receipt_id=p->>'predecessor_receipt_id';
            IF NOT p ? 'archive' THEN RETURN prior IS NULL OR NOT prior ? 'archive'; END IF;
            a := p->'archive';
            IF jsonb_typeof(a) IS DISTINCT FROM 'object'
              OR NOT a ?& ARRAY['target_oid','from_revision','to_revision','native_input_hash','delta_rows','delta_sha256']
              OR (SELECT count(*) FROM jsonb_object_keys(a))<>6
              OR EXISTS (SELECT 1 FROM unnest(ARRAY['target_oid','from_revision','to_revision','delta_rows']) k
                  WHERE jsonb_typeof(a->k) IS DISTINCT FROM 'number' OR a->>k !~ '^[0-9]{{1,19}}$')
              OR EXISTS (SELECT 1 FROM unnest(ARRAY['native_input_hash','delta_sha256']) k
                  WHERE jsonb_typeof(a->k) IS DISTINCT FROM 'string' OR a->>k !~ '^[0-9a-f]{{64}}$')
            THEN RETURN false; END IF;
            IF (a->>'target_oid')::numeric NOT BETWEEN 1 AND 4294967295
              OR (a->>'from_revision')::numeric>9223372036854775805
              OR (a->>'to_revision')::numeric<>(a->>'from_revision')::numeric+2
              OR (a->>'delta_rows')::numeric>9223372036854775807
            THEN RETURN false; END IF;
            IF prior->'archive'=a THEN RETURN COALESCE(prior->'cms'=p->'cms',false); END IF;
            IF to_regclass('{schema}.cms_native_input_revision') IS NULL
              OR (a->>'target_oid')::bigint IS DISTINCT FROM to_regclass('{schema}.address_archive_v2')::oid::bigint
            THEN RETURN false; END IF;
            EXECUTE 'SELECT revision,xmin::text::bigint FROM {schema}.cms_native_input_revision
                WHERE relation_oid=$1 AND schema_name=$2 AND table_name=''address_archive_v2'''
                INTO current_revision,mutation_xid USING (a->>'target_oid')::bigint,'{schema.strip(chr(34))}';
            RETURN COALESCE(current_revision=(a->>'to_revision')::bigint AND
                mutation_xid=mod(pg_current_xact_id()::text::numeric,4294967296)::bigint,false);
        END $$""")


def _create_snapshot_check(schema):
    """Verify accepted physical serving state independently of mutable build inputs."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_snapshot_receipt_matches(p jsonb) RETURNS boolean
        LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
        SELECT COALESCE({schema}.cms_serving_native_matches(current_native)
          AND current_native->'profile'=p->'profile' AND current_native->'address'=p->'address'
          AND current_native->'doctors'=p->'doctors' AND current_native->'overlay_oid'=p->'overlay_oid',false)
        FROM (SELECT {schema}.cms_serving_native_snapshot() current_native) snapshot $$""")


def _create_current_check(schema):
    """Keep predecessor identity discoverable when independent dependencies need rebuilding."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_current_receipt_matches(p jsonb) RETURNS boolean
        LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
        SELECT COALESCE({schema}.cms_serving_native_matches(current_native)
          AND current_native->'profile'=p->'profile' AND current_native->'address'=p->'address'
          AND current_native->'doctors'=p->'doctors' AND EXISTS (
            SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
            JOIN {schema}.provider_directory_source s ON s.source_id='cms-npd' AND s.endpoint_id=d.endpoint_id
            JOIN {schema}.provider_directory_cms_serving_coverage c ON c.dataset_id=d.dataset_id
            WHERE d.dataset_id=p->'cms'->>'dataset_id' AND d.endpoint_id=p->'cms'->>'endpoint_id'
              AND d.dataset_hash=p->'cms'->>'dataset_hash'
              AND d.acquisition_root_run_id=p->'cms'->>'acquisition_root_run_id'
              AND d.is_current AND d.status='published' AND c.dataset_hash=d.dataset_hash
              AND c.proof_version=2 AND c.release_id=p->'cms'->>'release_id'
              AND c.published_at=d.published_at),false)
        FROM (SELECT {schema}.cms_serving_native_snapshot() current_native) snapshot $$""")


def _create_insert_guard(schema):
    """Require exact predecessor provenance and a current-transaction result, never fabricated adoption."""
    table = f"{schema}.{_TABLE}"
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_receipt_insert() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE prior jsonb;
        BEGIN
            IF NEW.publication_xid<>pg_current_xact_id() OR NOT {schema}.cms_serving_receipt_matches(NEW.payload) THEN
                RAISE EXCEPTION 'cms_serving_receipt_result_mismatch'; END IF;
            IF NEW.predecessor_receipt_id IS NULL THEN
                IF NEW.payload->'expected_incumbent' IS DISTINCT FROM 'null'::jsonb THEN
                    RAISE EXCEPTION 'cms_serving_receipt_predecessor_mismatch'; END IF;
            ELSE
                SELECT payload INTO prior FROM {table} WHERE receipt_id=NEW.predecessor_receipt_id;
                IF prior IS NULL OR NEW.payload->'expected_incumbent' IS DISTINCT FROM
                     ((prior->'cms') - ARRAY['release_id','proof_version']) THEN
                    RAISE EXCEPTION 'cms_serving_receipt_predecessor_mismatch'; END IF;
            END IF;
            RETURN NULL;
        END $$""")
    op.execute(f"""CREATE CONSTRAINT TRIGGER cms_serving_receipt_insert AFTER INSERT ON {table}
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {schema}.cms_serving_receipt_insert()""")


def _create_fresh_guard(schema):
    """Require the fresh composite result for changes to any participating native authority."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_require_fresh(kind text, previous jsonb) RETURNS void
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ DECLARE result jsonb; prior jsonb;
        BEGIN
            SELECT payload INTO result FROM {schema}.{_TABLE} WHERE publication_xid=pg_current_xact_id();
            IF result IS NULL OR NOT {schema}.cms_serving_receipt_matches(result) THEN
                RAISE EXCEPTION 'cms_serving_fresh_receipt_required'; END IF;
            SELECT payload INTO prior FROM {schema}.{_TABLE} WHERE receipt_id=result->>'predecessor_receipt_id';
            IF prior IS NOT NULL AND kind IN ('profile','address','doctors')
                AND previous IS DISTINCT FROM prior->kind THEN
                RAISE EXCEPTION 'cms_serving_predecessor_authority_changed'; END IF;
        END $$""")


def _create_native_transition_guards(schema):
    """Protect Profile removal as well as addition, and native address/Doctors replacement."""
    specs = (
        (
            "provider_directory_profile_serving_generation",
            "profile",
            _json_fields("OLD", _PROFILE_FIELDS),
            _json_fields("NEW", _PROFILE_FIELDS),
            "(TG_OP<>'INSERT' AND OLD.source_vector_json @> '[{\"source_id\":\"cms-npd\"}]'::jsonb) OR "
            "(TG_OP<>'DELETE' AND NEW.source_vector_json @> '[{\"source_id\":\"cms-npd\"}]'::jsonb)",
        ),
        ("entity_address_result_generation", "address", _authority("OLD"), _authority("NEW"), "false"),
        (
            "reference_family_result_generation",
            "doctors",
            _authority("OLD", doctors=True),
            _authority("NEW", doctors=True),
            "false",
        ),
    )
    for table, kind, old_value, new_value, participates in specs:
        doctor_filter = (
            "IF COALESCE(NEW.importer_id,OLD.importer_id)<>'cms-doctors' "
            "AND NOT (TG_OP='UPDATE' AND OLD.importer_id='cms-doctors') THEN RETURN NULL; END IF;"
            if kind == "doctors"
            else ""
        )
        vector_unchanged = (
            "AND OLD.source_vector_json IS NOT DISTINCT FROM NEW.source_vector_json" if kind == "profile" else ""
        )
        body = f"""BEGIN
            {doctor_filter}
            IF TG_OP='UPDATE' AND ({old_value}) IS NOT DISTINCT FROM ({new_value})
                {vector_unchanged} THEN RETURN NULL; END IF;
            IF ({participates}) OR EXISTS (SELECT 1 FROM {schema}.{_TABLE}) THEN
                PERFORM {schema}.cms_serving_require_fresh('{kind}',CASE WHEN TG_OP='INSERT' THEN NULL ELSE {old_value} END);
            END IF;
            RETURN NULL; END"""
        if kind == "profile":
            body = profile_transition_body(schema)
        op.execute(f"""CREATE FUNCTION {schema}.cms_serving_{kind}_transition() RETURNS trigger
            LANGUAGE plpgsql SET search_path=pg_catalog AS $$ {body} $$""")
        op.execute(f"""CREATE CONSTRAINT TRIGGER cms_serving_{kind}_transition AFTER INSERT OR UPDATE OR DELETE
            ON {schema}.{table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION {schema}.cms_serving_{kind}_transition()""")


def _create_source_transition_guards(schema):
    """Stop source-only publication and alias bypasses without guarding acquiring rows."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_dataset_transition() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF TG_OP='UPDATE' AND ROW(OLD.status,OLD.is_current,OLD.dataset_hash,OLD.endpoint_id,OLD.acquisition_root_run_id,
            OLD.published_at,OLD.content_proof_admission_sha256,OLD.publication_metadata_sha256,OLD.publication_metadata_summary_json)
            IS NOT DISTINCT FROM ROW(NEW.status,NEW.is_current,NEW.dataset_hash,NEW.endpoint_id,NEW.acquisition_root_run_id,
            NEW.published_at,NEW.content_proof_admission_sha256,NEW.publication_metadata_sha256,NEW.publication_metadata_summary_json) THEN RETURN NULL; END IF;
        IF (TG_OP<>'INSERT' AND OLD.is_current AND
             (OLD.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
              OR EXISTS (SELECT 1 FROM {schema}.provider_directory_source WHERE source_id='cms-npd' AND endpoint_id=OLD.endpoint_id)))
          OR (TG_OP<>'DELETE' AND NEW.is_current AND
             (NEW.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
              OR EXISTS (SELECT 1 FROM {schema}.provider_directory_source WHERE source_id='cms-npd' AND endpoint_id=NEW.endpoint_id))) THEN
            PERFORM {schema}.cms_serving_require_fresh('source',NULL);
        END IF; RETURN NULL; END $$""")
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_source_transition() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF TG_OP='UPDATE' AND ROW(OLD.source_id,OLD.endpoint_id) IS NOT DISTINCT FROM
             ROW(NEW.source_id,NEW.endpoint_id) THEN RETURN NULL; END IF;
        IF (COALESCE(NEW.source_id,OLD.source_id)='cms-npd' OR (TG_OP='UPDATE' AND OLD.source_id='cms-npd'))
          AND (EXISTS (SELECT 1 FROM {schema}.{_TABLE})
            OR EXISTS (SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
                WHERE d.is_current AND d.endpoint_id IN (OLD.endpoint_id,NEW.endpoint_id))) THEN
            PERFORM {schema}.cms_serving_require_fresh('source',NULL);
        END IF; RETURN NULL; END $$""")
    for kind, table in (("dataset", "provider_directory_endpoint_dataset"), ("source", "provider_directory_source")):
        op.execute(f"""CREATE CONSTRAINT TRIGGER cms_serving_{kind}_transition AFTER INSERT OR UPDATE OR DELETE
            ON {schema}.{table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION {schema}.cms_serving_{kind}_transition()""")


def _create_truncate_guards(schema):
    """Do not permit bulk deletion to bypass a native transition check."""
    op.execute(f"""CREATE FUNCTION {schema}.cms_serving_no_truncate() RETURNS trigger
        LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN
        IF EXISTS (SELECT 1 FROM {schema}.{_TABLE}) THEN
            RAISE EXCEPTION 'cms_serving_native_truncate_forbidden'; END IF; RETURN NULL; END $$""")
    for table in (
        "provider_directory_source",
        "provider_directory_endpoint_dataset",
        "provider_directory_profile_serving_generation",
        "entity_address_result_generation",
        "reference_family_result_generation",
    ):
        op.execute(f"""CREATE TRIGGER cms_serving_no_truncate BEFORE TRUNCATE ON {schema}.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION {schema}.cms_serving_no_truncate()""")


def upgrade():
    """Refuse legacy current CMS rather than inventing a composite serving history."""
    schema = '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(
        f"LOCK TABLE {schema}.provider_directory_endpoint_dataset, {schema}.provider_directory_source IN SHARE MODE"
    )
    op.execute(f"""DO $$ BEGIN IF EXISTS (SELECT 1 FROM {schema}.provider_directory_endpoint_dataset d
        WHERE d.is_current AND (d.publication_metadata_summary_json->'source_ids'='["cms-npd"]'::jsonb
          OR EXISTS (SELECT 1 FROM {schema}.provider_directory_source s WHERE s.source_id='cms-npd'
                     AND s.endpoint_id=d.endpoint_id))) THEN
        RAISE EXCEPTION 'cms_serving_legacy_current_requires_staged_upgrade'; END IF; END $$""")
    _create_native_readers(schema)
    _create_table(schema)
    op.execute(
        f"CREATE INDEX cms_serving_profile_delta_proof ON {schema}.provider_directory_profile_delta_receipt (selection_proof_id)"
    )
    _create_archive_check(schema)
    _create_result_check(schema)
    _create_current_check(schema)
    _create_snapshot_check(schema)
    _create_insert_guard(schema)
    _create_fresh_guard(schema)
    _create_native_transition_guards(schema)
    _create_source_transition_guards(schema)
    _create_truncate_guards(schema)


def downgrade():
    """Keep common serving history until an explicit withdrawal and retention plan exists."""
    raise RuntimeError("cms_serving_receipt_downgrade_requires_explicit_plan")
