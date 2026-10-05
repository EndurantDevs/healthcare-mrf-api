# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register fixed snapshot families; do not activate or retire legacy storage.

Creation and binding are protected low-volume operations. This schema-only
revision creates no snapshot, execution, generation, or publication rows.
New leaves have no row triggers or foreign keys and no direct write grants.
"""

from __future__ import annotations

import os

from alembic import op

revision = "20261005030000_custom_import_snapshot_storage"
down_revision = "20261005130000_provider_dataset_candidates"
branch_labels = None
depends_on = None

_TABLE_DDL = (
    "\nCREATE TABLE __SCHEMA__.custom_import_snapshot_family (\n\tfamily_id BIGSERIAL NOT NULL, \n\tdataset_id BIGINT NOT NULL, \n\tdefinition_revision_id BIGINT NOT NULL, \n\tschema_revision_id BIGINT NOT NULL, \n\texecution_id BIGINT NOT NULL, \n\tcapture_bundle_id BIGINT NOT NULL, \n\tproducing_fence BIGINT NOT NULL, \n\tproducing_token_sha256 BYTEA NOT NULL, \n\tgeneration_id BIGINT, \n\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT transaction_timestamp() NOT NULL, \n\tfrozen_at TIMESTAMP WITH TIME ZONE, \n\tlanding_table_oid BIGINT, \n\tlanding_table_owner BIGINT, \n\tlanding_columns_sha256 BYTEA, \n\torigin_table_oid BIGINT, \n\torigin_table_owner BIGINT, \n\torigin_columns_sha256 BYTEA, \n\tPRIMARY KEY (family_id), \n\tCONSTRAINT custom_import_snapshot_attempt_key UNIQUE (execution_id, producing_fence), \n\tCONSTRAINT custom_import_snapshot_generation_key UNIQUE (generation_id), \n\tCONSTRAINT custom_import_snapshot_execution_fkey FOREIGN KEY(execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) REFERENCES __SCHEMA__.custom_import_execution (execution_id, dataset_id, definition_revision_id, schema_revision_id, capture_bundle_id) ON DELETE RESTRICT, \n\tCONSTRAINT custom_import_snapshot_generation_fkey FOREIGN KEY(generation_id, dataset_id, definition_revision_id, schema_revision_id, execution_id, capture_bundle_id) REFERENCES __SCHEMA__.custom_import_generation (generation_id, dataset_id, definition_revision_id, schema_revision_id, execution_id, capture_bundle_id) ON DELETE RESTRICT, \n\tCONSTRAINT custom_import_snapshot_producer_shape CHECK (producing_fence > 0 AND octet_length(producing_token_sha256) = 32), \n\tCONSTRAINT custom_import_snapshot_landing_shape CHECK ((landing_table_oid IS NULL AND landing_table_owner IS NULL AND landing_columns_sha256 IS NULL) OR (landing_table_oid IS NOT NULL AND landing_table_owner IS NOT NULL AND landing_columns_sha256 IS NOT NULL AND landing_table_oid > 0 AND landing_table_owner > 0 AND octet_length(landing_columns_sha256) = 32)), \n\tCONSTRAINT custom_import_snapshot_origin_shape CHECK ((origin_table_oid IS NULL AND origin_table_owner IS NULL AND origin_columns_sha256 IS NULL) OR (origin_table_oid IS NOT NULL AND origin_table_owner IS NOT NULL AND origin_columns_sha256 IS NOT NULL AND origin_table_oid > 0 AND origin_table_owner > 0 AND octet_length(origin_columns_sha256) = 32))\n)\n\n",
    "\nCREATE TABLE __SCHEMA__.custom_import_snapshot_relation (\n\tfamily_id BIGINT NOT NULL, \n\trelation_slot SMALLINT NOT NULL, \n\ttable_oid BIGINT NOT NULL, \n\ttable_owner BIGINT NOT NULL, \n\tcolumns_sha256 BYTEA NOT NULL, \n\tPRIMARY KEY (family_id, relation_slot), \n\tCONSTRAINT custom_import_snapshot_relation_oid_key UNIQUE (table_oid), \n\tCONSTRAINT custom_import_snapshot_relation_shape CHECK (relation_slot BETWEEN 1 AND 15 AND table_oid > 0), \n\tCONSTRAINT custom_import_snapshot_layout_shape CHECK (table_owner > 0 AND octet_length(columns_sha256) = 32), \n\tFOREIGN KEY(family_id) REFERENCES __SCHEMA__.custom_import_snapshot_family (family_id) ON DELETE RESTRICT\n)\n\n",
)
_RELATION_NAMES = (
    "custom_import_root_record",
    "custom_import_entity_binding",
    "custom_import_pack",
    "custom_import_rejection",
    "custom_import_root_revision",
    "custom_import_child_revision",
    "custom_import_family_revision",
    "custom_import_family_child",
    "custom_import_generation_family",
    "custom_import_root_scalar",
    "custom_import_child_scalar",
    "custom_import_winner",
    "custom_import_build_occurrence",
    "custom_import_build_family",
    "custom_import_build_candidate_context",
)
_LOAD_DDL = (
    "CREATE TABLE __LEAF__.custom_import_root_record (LIKE __SCHEMA__.custom_import_root_record INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_root_record ADD CONSTRAINT custom_import_root_record_pkey PRIMARY KEY (root_record_id)",
    "ALTER TABLE __LEAF__.custom_import_root_record ADD CONSTRAINT custom_import_root_record_key UNIQUE (dataset_id, key_contract_sha256, logical_key_sha256)",
    "CREATE TABLE __LEAF__.custom_import_entity_binding (LIKE __SCHEMA__.custom_import_entity_binding INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_entity_binding ADD CONSTRAINT custom_import_entity_binding_pkey PRIMARY KEY (entity_binding_id)",
    "ALTER TABLE __LEAF__.custom_import_entity_binding ADD CONSTRAINT custom_import_entity_binding_value_key UNIQUE (dataset_id, adapter_id, canonical_value)",
    "CREATE TABLE __LEAF__.custom_import_pack (LIKE __SCHEMA__.custom_import_pack INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_pack ADD CONSTRAINT custom_import_pack_pkey PRIMARY KEY (pack_id)",
    "ALTER TABLE __LEAF__.custom_import_pack ADD CONSTRAINT custom_import_pack_attempt_key UNIQUE (execution_id, producing_fence, stream_slot, pack_ordinal)",
    "CREATE TABLE __LEAF__.custom_import_rejection (LIKE __SCHEMA__.custom_import_rejection INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_rejection ADD CONSTRAINT custom_import_rejection_pkey PRIMARY KEY (rejection_id)",
    "ALTER TABLE __LEAF__.custom_import_rejection ADD CONSTRAINT custom_import_rejection_attempt_key UNIQUE (execution_id, producing_fence, rejection_ordinal)",
    "CREATE TABLE __LEAF__.custom_import_root_revision (LIKE __SCHEMA__.custom_import_root_revision INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_root_revision ADD CONSTRAINT custom_import_root_revision_pkey PRIMARY KEY (root_revision_id)",
    "CREATE TABLE __LEAF__.custom_import_child_revision (LIKE __SCHEMA__.custom_import_child_revision INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_child_revision ADD CONSTRAINT custom_import_child_revision_pkey PRIMARY KEY (child_revision_id)",
    "CREATE TABLE __LEAF__.custom_import_family_revision (LIKE __SCHEMA__.custom_import_family_revision INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_family_revision ADD CONSTRAINT custom_import_family_revision_pkey PRIMARY KEY (family_revision_id)",
    "ALTER TABLE __LEAF__.custom_import_family_revision ADD CONSTRAINT custom_import_family_revision_attempt_content_key UNIQUE (dataset_id, schema_revision_id, root_record_id, family_sha256, producing_execution_id, producing_fence)",
    "CREATE TABLE __LEAF__.custom_import_family_child (LIKE __SCHEMA__.custom_import_family_child INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_family_child ADD CONSTRAINT custom_import_family_child_pkey PRIMARY KEY (family_revision_id, collection_slot, child_revision_id)",
    "CREATE TABLE __LEAF__.custom_import_generation_family (LIKE __SCHEMA__.custom_import_generation_family INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_generation_family ADD CONSTRAINT custom_import_generation_family_pkey PRIMARY KEY (generation_id, root_record_id)",
    "ALTER TABLE __LEAF__.custom_import_generation_family ADD CONSTRAINT custom_import_generation_family_member_key UNIQUE (generation_id, dataset_id, family_revision_id)",
    "CREATE TABLE __LEAF__.custom_import_root_scalar (LIKE __SCHEMA__.custom_import_root_scalar INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_root_scalar ADD CONSTRAINT custom_import_root_scalar_pkey PRIMARY KEY (root_revision_id, field_slot)",
    "CREATE TABLE __LEAF__.custom_import_child_scalar (LIKE __SCHEMA__.custom_import_child_scalar INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_child_scalar ADD CONSTRAINT custom_import_child_scalar_pkey PRIMARY KEY (child_revision_id, field_slot)",
    "CREATE TABLE __LEAF__.custom_import_winner (LIKE __SCHEMA__.custom_import_winner INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_winner ADD CONSTRAINT custom_import_winner_pkey PRIMARY KEY (generation_id, profile_slot, entity_binding_id, context_key_sha256)",
    "CREATE TABLE __LEAF__.custom_import_build_occurrence (LIKE __SCHEMA__.custom_import_build_occurrence INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_build_occurrence ADD PRIMARY KEY (occurrence_id)",
    "ALTER TABLE __LEAF__.custom_import_build_occurrence ADD UNIQUE (child_revision_id)",
    "ALTER TABLE __LEAF__.custom_import_build_occurrence ADD UNIQUE (root_revision_id)",
    "CREATE UNIQUE INDEX custom_import_build_copy_child_key ON __LEAF__.custom_import_build_occurrence (build_id, base_family_revision_id, base_child_revision_id) WHERE base_child_revision_id IS NOT NULL",
    "CREATE UNIQUE INDEX custom_import_build_copy_root_key ON __LEAF__.custom_import_build_occurrence (build_id, base_family_revision_id) WHERE base_root_revision_id IS NOT NULL",
    "CREATE UNIQUE INDEX custom_import_build_source_ordinal_key ON __LEAF__.custom_import_build_occurrence (build_id, stream_slot, source_ordinal) WHERE origin = 'source'",
    "CREATE UNIQUE INDEX custom_import_build_source_position_key ON __LEAF__.custom_import_build_occurrence (build_id, stream_slot, source_part_ordinal, part_row_ordinal) WHERE origin = 'source'",
    "CREATE TABLE __LEAF__.custom_import_build_family (LIKE __SCHEMA__.custom_import_build_family INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_build_family ADD PRIMARY KEY (build_id, root_record_id)",
    "ALTER TABLE __LEAF__.custom_import_build_family ADD CONSTRAINT custom_import_build_family_output_key UNIQUE (build_id, family_revision_id)",
    "CREATE TABLE __LEAF__.custom_import_build_candidate_context (LIKE __SCHEMA__.custom_import_build_candidate_context INCLUDING DEFAULTS INCLUDING CONSTRAINTS)",
    "ALTER TABLE __LEAF__.custom_import_build_candidate_context ADD PRIMARY KEY (candidate_context_id)",
    "ALTER TABLE __LEAF__.custom_import_build_candidate_context ADD CONSTRAINT custom_import_build_context_candidate_key UNIQUE NULLS NOT DISTINCT (build_id, profile_slot, family_revision_id, context_child_revision_id)",
)

_AUTHORITY_BODY = r"""
DECLARE e __SCHEMA__.custom_import_execution; l __SCHEMA__.custom_import_lease;
    b __SCHEMA__.custom_import_build_attempt; c __SCHEMA__.custom_import_capture_bundle;
    now_at timestamptz; timeout_ms numeric;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'custom_import_snapshot_requires_read_committed';
    END IF;
    SELECT * INTO e FROM __SCHEMA__.custom_import_execution WHERE execution_id=p_execution_id;
    IF e.execution_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_attempt_missing'; END IF;
    PERFORM 1 FROM __SCHEMA__.custom_import_dataset WHERE dataset_id=e.dataset_id FOR UPDATE;
    SELECT * INTO e FROM __SCHEMA__.custom_import_execution WHERE execution_id=p_execution_id FOR UPDATE;
    SELECT * INTO l FROM __SCHEMA__.custom_import_lease WHERE execution_id=p_execution_id FOR UPDATE;
    now_at := clock_timestamp();
    IF e.state IS DISTINCT FROM 'running' OR p_fence IS NULL OR p_fence <= 0
       OR p_token_sha256 IS NULL OR octet_length(p_token_sha256) <> 32
       OR l.fence IS DISTINCT FROM p_fence OR l.token_sha256 IS DISTINCT FROM p_token_sha256
       OR l.expires_at IS NULL OR l.expires_at <= now_at THEN
        RAISE EXCEPTION 'custom_import_snapshot_attempt_lost';
    END IF;
    timeout_ms := extract(epoch FROM current_setting('statement_timeout')::interval)*1000;
    IF timeout_ms <= 0 OR timeout_ms >= extract(epoch FROM l.expires_at-now_at)*1000 THEN
        RAISE EXCEPTION 'custom_import_snapshot_invalid_bounds';
    END IF;
    SELECT * INTO c FROM __SCHEMA__.custom_import_capture_bundle WHERE capture_bundle_id=e.capture_bundle_id;
    IF c.capture_state IS DISTINCT FROM 'sealed' OR
        ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM
        ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id) THEN
        RAISE EXCEPTION 'custom_import_snapshot_capture_mismatch';
    END IF;
    SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt
        WHERE execution_id=e.execution_id AND producing_fence=p_fence;
    IF b.build_id IS NOT NULL THEN PERFORM __SCHEMA__.lock_custom_import_build(b.build_id); END IF;
    RETURN e;
END;
"""

_COLUMNS_BODY = r"""
    SELECT sha256(convert_to(string_agg(
        ROW(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attcollation,
            a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid))::text,
        E'\\n' ORDER BY a.attnum),'UTF8'))
    FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE a.attrelid=p_table_oid::oid AND a.attnum>0 AND NOT a.attisdropped
"""

_RELATIONS_BODY = r"""
DECLARE f __SCHEMA__.custom_import_snapshot_family; names text[] := __NAMES__; namespace text;
BEGIN
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family WHERE family_id=p_family_id;
    IF f.family_id IS NULL THEN RAISE EXCEPTION 'custom_import_snapshot_family_missing'; END IF;
    namespace := 'ci_snapshot_' || f.family_id::text;
    IF (SELECT count(*) FROM __SCHEMA__.custom_import_snapshot_relation WHERE family_id=f.family_id) <> 15
       OR EXISTS (
        SELECT 1 FROM __SCHEMA__.custom_import_snapshot_relation r
        LEFT JOIN pg_class c ON c.oid=r.table_oid::oid
        LEFT JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE r.family_id=f.family_id AND
            (c.oid IS NULL OR c.relkind <> 'r' OR c.relpersistence <> 'p'
             OR c.relowner::bigint IS DISTINCT FROM r.table_owner OR n.nspname IS DISTINCT FROM namespace
             OR c.relname IS DISTINCT FROM names[r.relation_slot]
             OR __SCHEMA__.custom_import_snapshot_columns_sha256(r.table_oid) IS DISTINCT FROM r.columns_sha256
             OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
             OR EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
             OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal))
       ) THEN RAISE EXCEPTION 'custom_import_snapshot_relation_mismatch'; END IF;
    IF f.landing_table_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=f.landing_table_oid::oid AND c.relowner::bigint=f.landing_table_owner
          AND c.relkind='r' AND c.relpersistence='p' AND n.nspname=namespace
          AND c.relname='source_bulk_landing'
          AND __SCHEMA__.custom_import_snapshot_columns_sha256(c.oid::bigint)=f.landing_columns_sha256
          AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
          AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
          AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
    ) THEN RAISE EXCEPTION 'custom_import_snapshot_landing_mismatch'; END IF;
    IF f.origin_table_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.oid=f.origin_table_oid::oid AND c.relowner::bigint=f.origin_table_owner
          AND c.relowner=n.nspowner AND c.relkind='r' AND c.relpersistence='p' AND n.nspname=namespace
          AND c.relname='legacy_copy_origin'
          AND __SCHEMA__.custom_import_snapshot_columns_sha256(c.oid::bigint)=f.origin_columns_sha256
          AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
          AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid=c.oid AND contype='f')
          AND NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal)
          AND NOT EXISTS (SELECT 1 FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
                          WHERE acl.grantee<>c.relowner)
          AND NOT EXISTS (SELECT 1 FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
                          WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND acl.grantee<>c.relowner)
    ) THEN RAISE EXCEPTION 'custom_import_snapshot_origin_mismatch'; END IF;
    RETURN QUERY SELECT r.relation_slot,r.table_oid FROM __SCHEMA__.custom_import_snapshot_relation r
        WHERE r.family_id=f.family_id ORDER BY r.relation_slot;
END;
"""

_READ_BINDING_BODY = r"""
DECLARE g __SCHEMA__.custom_import_generation; f __SCHEMA__.custom_import_snapshot_family;
    names text[] := __NAMES__; lock_targets text;
BEGIN
    SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id;
    IF g.generation_id IS NULL OR
       ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id) IS DISTINCT FROM
       ROW(p_dataset_id,p_definition_revision_id,p_schema_revision_id)
       OR g.producing_fence IS NULL OR g.producing_fence <= 0
       OR g.producing_token_sha256 IS NULL OR octet_length(g.producing_token_sha256) <> 32
       OR NOT EXISTS (
        SELECT 1 FROM __SCHEMA__.custom_import_generation_seal s
        WHERE ROW(s.generation_id,s.dataset_id,s.definition_revision_id,s.schema_revision_id) =
              ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id)
          AND s.seal_contract='custom-import-generation-seal/v1'
       ) THEN RAISE EXCEPTION 'custom_import_snapshot_read_generation_mismatch'; END IF;
    IF (SELECT count(*) FROM __SCHEMA__.custom_import_snapshot_family
        WHERE generation_id=g.generation_id OR
              (execution_id=g.execution_id AND producing_fence=g.producing_fence)) > 1 THEN
        RAISE EXCEPTION 'custom_import_snapshot_read_binding_mismatch';
    END IF;
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE generation_id=g.generation_id OR
              (execution_id=g.execution_id AND producing_fence=g.producing_fence);
    -- Only an unregistered legacy producer may use canonical hot tables.
    IF f.family_id IS NULL THEN RETURN NULL; END IF;
    IF ROW(f.generation_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
           f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) IS DISTINCT FROM
       ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
           g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256)
       OR f.frozen_at IS NULL THEN
        RAISE EXCEPTION 'custom_import_snapshot_read_binding_mismatch';
    END IF;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
        INTO lock_targets FROM unnest(names) WITH ORDINALITY AS relations(name,ordinal);
    -- Frozen bindings are immutable; READ ONLY requests pin leaves, not row locks.
    IF f.origin_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.legacy_copy_origin','ci_snapshot_' || f.family_id::text);
    END IF;
    EXECUTE 'LOCK TABLE ' || lock_targets || ' IN ACCESS SHARE MODE';
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    RETURN f.family_id;
END;
"""

_WRITE_BINDING_BODY = r"""
DECLARE e __SCHEMA__.custom_import_execution; f __SCHEMA__.custom_import_snapshot_family;
    names text[] := __NAMES__; lock_targets text;
BEGIN
    e := __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE execution_id=p_execution_id AND producing_fence=p_fence FOR UPDATE;
    IF f.family_id IS NULL OR
       ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.capture_bundle_id,f.producing_token_sha256)
       IS DISTINCT FROM ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id,p_token_sha256)
       THEN RAISE EXCEPTION 'custom_import_snapshot_write_binding_mismatch'; END IF;
    IF f.frozen_at IS NOT NULL OR EXISTS (
        SELECT 1 FROM __SCHEMA__.custom_import_generation_seal WHERE generation_id=f.generation_id
    ) OR EXISTS (
        SELECT 1 FROM __SCHEMA__.custom_import_no_change_seal
        WHERE execution_id=f.execution_id AND dataset_id=f.dataset_id
    ) THEN RAISE EXCEPTION 'custom_import_snapshot_writes_closed'; END IF;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
        INTO lock_targets FROM unnest(names) WITH ORDINALITY AS relations(name,ordinal);
    IF f.landing_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.source_bulk_landing','ci_snapshot_' || f.family_id::text);
    END IF;
    IF f.origin_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.legacy_copy_origin','ci_snapshot_' || f.family_id::text);
    END IF;
    EXECUTE 'LOCK TABLE ' || lock_targets || ' IN ROW EXCLUSIVE MODE';
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    RETURN f.family_id;
END;
"""

_FINALITY_BINDING_BODY = r"""
DECLARE g __SCHEMA__.custom_import_generation; f __SCHEMA__.custom_import_snapshot_family;
    names text[] := __NAMES__; lock_targets text;
BEGIN
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE execution_id=p_execution_id AND producing_fence=p_fence FOR SHARE;
    SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
    IF f.family_id IS NULL OR g.generation_id IS NULL OR f.frozen_at IS NULL
       OR ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
              g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256) IS DISTINCT FROM
          ROW(p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
              p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256)
       OR ROW(f.generation_id,f.dataset_id,f.definition_revision_id,f.schema_revision_id,
              f.execution_id,f.capture_bundle_id,f.producing_fence,f.producing_token_sha256) IS DISTINCT FROM
          ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
              g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256) THEN
        RAISE EXCEPTION 'custom_import_snapshot_finality_binding_mismatch';
    END IF;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
        INTO lock_targets FROM unnest(names) WITH ORDINALITY AS relations(name,ordinal);
    IF f.landing_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.source_bulk_landing','ci_snapshot_' || f.family_id::text);
    END IF;
    IF f.origin_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.legacy_copy_origin','ci_snapshot_' || f.family_id::text);
    END IF;
    EXECUTE 'LOCK TABLE ' || lock_targets || ' IN ACCESS SHARE MODE';
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    RETURN f.family_id;
END;
"""

_CREATE_BODY = r"""
DECLARE e __SCHEMA__.custom_import_execution; f __SCHEMA__.custom_import_snapshot_family;
    namespace text; statement text; names text[] := __NAMES__; slot integer; relation oid;
    acl_entry record; read_entry record; grantee text; owner_oid oid;
BEGIN
    e := __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE execution_id=e.execution_id AND producing_fence=p_fence FOR UPDATE;
    IF f.family_id IS NOT NULL THEN
        IF ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.capture_bundle_id,f.producing_token_sha256)
           IS DISTINCT FROM ROW(e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.capture_bundle_id,p_token_sha256)
            THEN RAISE EXCEPTION 'custom_import_snapshot_attempt_mismatch'; END IF;
        PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
        RETURN f.family_id;
    END IF;
    SELECT oid INTO owner_oid FROM pg_roles WHERE rolname=current_user;
    IF EXISTS (
        SELECT 1 FROM unnest(names) name
        LEFT JOIN pg_class c ON c.oid=to_regclass(format('%I.%I',__SCHEMA_LITERAL__,name))
        WHERE c.relowner IS DISTINCT FROM owner_oid OR c.relkind IS DISTINCT FROM 'r'
    ) THEN RAISE EXCEPTION 'custom_import_snapshot_owner_mismatch'; END IF;
    INSERT INTO __SCHEMA__.custom_import_snapshot_family
        (dataset_id,definition_revision_id,schema_revision_id,execution_id,capture_bundle_id,
         producing_fence,producing_token_sha256)
    VALUES (e.dataset_id,e.definition_revision_id,e.schema_revision_id,e.execution_id,e.capture_bundle_id,
            p_fence,p_token_sha256) RETURNING * INTO f;
    namespace := 'ci_snapshot_' || f.family_id::text;
    EXECUTE format('CREATE SCHEMA %I',namespace);
    EXECUTE format('REVOKE ALL ON SCHEMA %I FROM PUBLIC',namespace);
    FOR acl_entry IN
        SELECT DISTINCT acl.grantee FROM pg_namespace n,
            LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) acl
        WHERE n.nspname=namespace AND acl.grantee NOT IN (0,owner_oid)
    LOOP
        SELECT rolname INTO grantee FROM pg_roles WHERE oid=acl_entry.grantee;
        EXECUTE format('REVOKE ALL ON SCHEMA %I FROM %I',namespace,grantee);
    END LOOP;
    FOREACH statement IN ARRAY __DDL__ LOOP
        EXECUTE replace(statement,'__LEAF__',quote_ident(namespace));
    END LOOP;
    FOR slot IN 1..15 LOOP
        relation := to_regclass(format('%I.%I',namespace,names[slot]));
        EXECUTE format('REVOKE ALL ON TABLE %I.%I FROM PUBLIC',namespace,names[slot]);
        FOR acl_entry IN
            SELECT DISTINCT acl.grantee FROM pg_class c,
                LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
            WHERE c.oid=relation AND acl.grantee NOT IN (0,owner_oid)
        LOOP
            SELECT rolname INTO grantee FROM pg_roles WHERE oid=acl_entry.grantee;
            EXECUTE format('REVOKE ALL ON TABLE %I.%I FROM %I',namespace,names[slot],grantee);
        END LOOP;
        FOR read_entry IN
            SELECT DISTINCT acl.grantee FROM pg_class c,
                LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
            WHERE c.oid=to_regclass(format('%I.%I',__SCHEMA_LITERAL__,names[slot]))
              AND acl.privilege_type='SELECT' AND acl.grantee NOT IN (0,owner_oid)
        LOOP
            SELECT rolname INTO grantee FROM pg_roles WHERE oid=read_entry.grantee;
            EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',namespace,grantee);
            EXECUTE format('GRANT SELECT ON TABLE %I.%I TO %I',namespace,names[slot],grantee);
        END LOOP;
        INSERT INTO __SCHEMA__.custom_import_snapshot_relation
            (family_id,relation_slot,table_oid,table_owner,columns_sha256)
        SELECT f.family_id,slot,relation::bigint,c.relowner::bigint,
            __SCHEMA__.custom_import_snapshot_columns_sha256(relation::bigint)
        FROM pg_class c WHERE c.oid=relation;
    END LOOP;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    RETURN f.family_id;
END;
"""

_BIND_BODY = r"""
DECLARE f __SCHEMA__.custom_import_snapshot_family; g __SCHEMA__.custom_import_generation;
BEGIN
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE execution_id=p_execution_id AND producing_fence=p_fence FOR UPDATE;
    SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
    IF f.family_id IS NULL OR g.generation_id IS NULL
       OR ROW(g.dataset_id,g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,
              g.producing_fence,g.producing_token_sha256) IS DISTINCT FROM
          ROW(f.dataset_id,f.definition_revision_id,f.schema_revision_id,f.execution_id,f.capture_bundle_id,
              f.producing_fence,f.producing_token_sha256)
       OR (f.generation_id IS NOT NULL AND f.generation_id IS DISTINCT FROM g.generation_id)
       OR (f.frozen_at IS NOT NULL AND f.generation_id IS DISTINCT FROM g.generation_id) THEN
        RAISE EXCEPTION 'custom_import_snapshot_generation_mismatch';
    END IF;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    IF f.generation_id IS NULL THEN
        UPDATE __SCHEMA__.custom_import_snapshot_family SET generation_id=g.generation_id WHERE family_id=f.family_id;
    END IF;
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    RETURN f.family_id;
END;
"""

_FREEZE_BODY = r"""
DECLARE f __SCHEMA__.custom_import_snapshot_family; b __SCHEMA__.custom_import_build_attempt;
    names text[] := __NAMES__; lock_targets text; columns_list text; grantee record; has_landing_rows boolean;
BEGIN
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO f FROM __SCHEMA__.custom_import_snapshot_family
        WHERE execution_id=p_execution_id AND producing_fence=p_fence FOR UPDATE;
    IF f.family_id IS NULL OR f.generation_id IS NULL THEN
        RAISE EXCEPTION 'custom_import_snapshot_generation_required';
    END IF;
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt
        WHERE execution_id=p_execution_id AND producing_fence=p_fence;
    IF b.build_id IS NOT NULL AND (b.generation_id IS DISTINCT FROM f.generation_id
        OR b.phase NOT IN ('verifying','verified') OR b.output_frozen_at IS NULL) THEN
        RAISE EXCEPTION 'custom_import_snapshot_output_incomplete';
    END IF;
    SELECT string_agg(format('%I.%I','ci_snapshot_' || f.family_id::text,name),',' ORDER BY ordinal)
        INTO lock_targets FROM unnest(names) WITH ORDINALITY AS relations(name,ordinal);
    IF f.landing_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.source_bulk_landing','ci_snapshot_' || f.family_id::text);
    END IF;
    IF f.origin_table_oid IS NOT NULL THEN
        lock_targets := lock_targets || ',' || format('%I.legacy_copy_origin','ci_snapshot_' || f.family_id::text);
    END IF;
    EXECUTE 'LOCK TABLE ' || lock_targets || ' IN SHARE MODE';
    PERFORM __SCHEMA__.resolve_custom_import_snapshot_relations(f.family_id);
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    IF f.landing_table_oid IS NOT NULL THEN
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)',f.landing_table_oid::oid::regclass) INTO has_landing_rows;
        IF has_landing_rows THEN RAISE EXCEPTION 'custom_import_snapshot_unfinished_landing'; END IF;
        SELECT string_agg(quote_ident(attname),',' ORDER BY attnum) INTO columns_list
        FROM pg_attribute WHERE attrelid=f.landing_table_oid::oid AND attnum>0 AND NOT attisdropped;
        FOR grantee IN
            SELECT DISTINCT rights.grantee FROM (
                SELECT acl.grantee FROM pg_class c,
                    LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
                WHERE c.oid=f.landing_table_oid::oid
                UNION ALL
                SELECT acl.grantee FROM pg_attribute a,LATERAL aclexplode(a.attacl) acl
                WHERE a.attrelid=f.landing_table_oid::oid AND a.attnum>0 AND NOT a.attisdropped
            ) rights WHERE rights.grantee<>f.landing_table_owner::oid
        LOOP
            EXECUTE format('REVOKE ALL ON TABLE %s FROM %s',f.landing_table_oid::oid::regclass,
                CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grantee.grantee)) END);
            EXECUTE format('REVOKE SELECT (%s), INSERT (%s), UPDATE (%s), REFERENCES (%s) ON TABLE %s FROM %s',
                columns_list,columns_list,columns_list,columns_list,f.landing_table_oid::oid::regclass,
                CASE WHEN grantee.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(grantee.grantee)) END);
        END LOOP;
    END IF;
    IF f.frozen_at IS NULL THEN
        UPDATE __SCHEMA__.custom_import_snapshot_family SET frozen_at=clock_timestamp() WHERE family_id=f.family_id;
    END IF;
    RETURN f.family_id;
END;
"""


def _schema() -> str:
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "E'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _sql(schema: str, statement: str) -> str:
    return statement.replace("__SCHEMA__", _quote(schema))


_FINALITY_RESOLVER = r"""
DECLARE g __SCHEMA__.custom_import_generation; snapshot_id bigint; targets text;
BEGIN
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    SELECT * INTO g FROM __SCHEMA__.custom_import_generation WHERE generation_id=p_generation_id FOR UPDATE;
    IF g.generation_id IS NULL OR ROW(g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
        g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256) IS DISTINCT FROM
        ROW(p_generation_id,p_dataset_id,p_definition_revision_id,p_schema_revision_id,
            p_execution_id,p_capture_bundle_id,p_fence,p_token_sha256) THEN
        RAISE EXCEPTION 'custom_import_snapshot_finality_binding_mismatch'; END IF;
    IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_snapshot_family WHERE generation_id=g.generation_id
        OR (execution_id=g.execution_id AND producing_fence=g.producing_fence)) THEN
        RETURN __SCHEMA__.lock_custom_import_snapshot_finality(g.generation_id,g.dataset_id,
            g.definition_revision_id,g.schema_revision_id,g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256);
    END IF;
    IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_attempt WHERE generation_id=g.generation_id
        OR (execution_id=g.execution_id AND producing_fence=g.producing_fence)) THEN
        RAISE EXCEPTION 'custom_import_snapshot_existing_build_requires_migration'; END IF;
    SELECT string_agg(format('%I.%I',__SCHEMA_LITERAL__,name),',' ORDER BY ordinal) INTO targets
        FROM unnest(__NAMES__::text[]) WITH ORDINALITY AS relations(name,ordinal);
    EXECUTE 'LOCK TABLE '||targets||' IN ACCESS SHARE MODE';
    PERFORM __SCHEMA__.lock_custom_import_snapshot_attempt(p_execution_id,p_fence,p_token_sha256);
    RETURN NULL;
END;
"""


def _body(schema: str, source: str) -> str:
    """Embed only frozen model DDL and fixed names, not caller SQL."""
    names = "ARRAY[" + ",".join(_literal(name) for name in _RELATION_NAMES) + "]"
    ddl = "ARRAY[" + ",".join(_literal(_sql(schema, statement)) for statement in _LOAD_DDL) + "]"
    return source.replace("__NAMES__", names).replace("__DDL__", ddl).replace("__SCHEMA_LITERAL__", _literal(schema))


def _function(schema: str, name: str, arguments: str, result: str, body: str, *, sql: bool = False) -> None:
    """Install protected native functions without default PUBLIC execution."""
    op.execute(
        _sql(
            schema,
            f"""
        CREATE FUNCTION __SCHEMA__.{name}({arguments}) RETURNS {result}
        LANGUAGE {"sql" if sql else "plpgsql"} SECURITY DEFINER SET search_path=pg_catalog
        AS $snapshot$ {_body(schema, body)} $snapshot$
    """,
        )
    )
    types = ",".join(argument.strip().split(" ", 1)[1] for argument in arguments.split(","))
    op.execute(_sql(schema, f"REVOKE ALL ON FUNCTION __SCHEMA__.{name}({types}) FROM PUBLIC"))
    _revoke_defaults("FUNCTION", f"{_quote(schema)}.{name}({types})")


def _revoke_defaults(kind: str, identity: str) -> None:
    """Deny inherited default ACLs; provisioning grants explicit entry points."""

    catalog, acl, owner, resolver = (
        ("pg_proc", "proacl", "proowner", "to_regprocedure")
        if kind == "FUNCTION"
        else ("pg_class", "relacl", "relowner", "to_regclass")
    )
    default_kind = "f" if kind == "FUNCTION" else "S" if kind == "SEQUENCE" else "r"
    op.execute(f"""
        DO $snapshot$ DECLARE grantee name; BEGIN
            FOR grantee IN SELECT DISTINCT roles.rolname FROM pg_catalog.{catalog} c,
                LATERAL pg_catalog.aclexplode(coalesce(c.{acl},pg_catalog.acldefault('{default_kind}',c.{owner}))) rights
                JOIN pg_catalog.pg_roles roles ON roles.oid=rights.grantee
                WHERE c.oid=pg_catalog.{resolver}({_literal(identity)}) AND rights.grantee<>c.{owner}
            LOOP
                EXECUTE format('REVOKE ALL ON %s %s FROM %I',{_literal(kind)},{_literal(identity)},grantee);
            END LOOP;
        END $snapshot$
    """)


def upgrade() -> None:
    """Create control schema/functions only; no data or legacy guard changes."""
    schema = _schema()
    for statement in _TABLE_DDL:
        op.execute(_sql(schema, statement))
    for name in ("custom_import_snapshot_family", "custom_import_snapshot_relation"):
        op.execute(_sql(schema, f"REVOKE ALL ON TABLE __SCHEMA__.{name} FROM PUBLIC"))
        _revoke_defaults("TABLE", f"{_quote(schema)}.{name}")
    op.execute(
        _sql(schema, "REVOKE ALL ON SEQUENCE __SCHEMA__.custom_import_snapshot_family_family_id_seq FROM PUBLIC")
    )
    _revoke_defaults("SEQUENCE", f"{_quote(schema)}.custom_import_snapshot_family_family_id_seq")
    authority = "p_execution_id bigint,p_fence bigint,p_token_sha256 bytea"
    _function(
        schema, "lock_custom_import_snapshot_attempt", authority, "__SCHEMA__.custom_import_execution", _AUTHORITY_BODY
    )
    _function(schema, "custom_import_snapshot_columns_sha256", "p_table_oid bigint", "bytea", _COLUMNS_BODY, sql=True)
    _function(
        schema,
        "resolve_custom_import_snapshot_relations",
        "p_family_id bigint",
        "TABLE(relation_slot smallint,table_oid bigint)",
        _RELATIONS_BODY,
    )
    _function(
        schema,
        "resolve_custom_import_generation_snapshot",
        "p_generation_id bigint,p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint",
        "bigint",
        _READ_BINDING_BODY,
    )
    _function(schema, "lock_custom_import_writable_snapshot", authority, "bigint", _WRITE_BINDING_BODY)
    _function(
        schema,
        "lock_custom_import_snapshot_finality",
        "p_generation_id bigint,p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,"
        "p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea",
        "bigint",
        _FINALITY_BINDING_BODY,
    )
    _function(schema, "create_custom_import_snapshot_family", authority, "bigint", _CREATE_BODY)
    _function(
        schema,
        "resolve_custom_import_generation_finality_snapshot",
        "p_generation_id bigint,p_dataset_id bigint,p_definition_revision_id bigint,p_schema_revision_id bigint,"
        "p_execution_id bigint,p_capture_bundle_id bigint,p_fence bigint,p_token_sha256 bytea",
        "bigint",
        _FINALITY_RESOLVER,
    )
    _function(
        schema, "bind_custom_import_snapshot_generation", authority + ",p_generation_id bigint", "bigint", _BIND_BODY
    )
    _function(schema, "freeze_custom_import_snapshot_family", authority, "bigint", _FREEZE_BODY)


def downgrade() -> None:
    """Refuse to delete retained registry rows or their physical snapshots."""
    schema = _schema()
    op.execute(
        _sql(
            schema,
            """
        DO $snapshot$ BEGIN
            LOCK TABLE __SCHEMA__.custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE;
            IF EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_snapshot_family) THEN
                RAISE EXCEPTION 'custom_import_snapshot_storage_downgrade_blocked';
            END IF;
        END $snapshot$
    """,
        )
    )
    authority = "bigint,bigint,bytea"
    for name, arguments in (
        (
            "resolve_custom_import_generation_finality_snapshot",
            "bigint,bigint,bigint,bigint,bigint,bigint,bigint,bytea",
        ),
        ("lock_custom_import_snapshot_finality", "bigint,bigint,bigint,bigint,bigint,bigint,bigint,bytea"),
        ("lock_custom_import_writable_snapshot", authority),
        ("resolve_custom_import_generation_snapshot", "bigint,bigint,bigint,bigint"),
        ("freeze_custom_import_snapshot_family", authority),
        ("bind_custom_import_snapshot_generation", authority + ",bigint"),
        ("create_custom_import_snapshot_family", authority),
        ("resolve_custom_import_snapshot_relations", "bigint"),
        ("custom_import_snapshot_columns_sha256", "bigint"),
        ("lock_custom_import_snapshot_attempt", authority),
    ):
        op.execute(_sql(schema, f"DROP FUNCTION __SCHEMA__.{name}({arguments})"))
    op.execute(_sql(schema, "DROP TABLE __SCHEMA__.custom_import_snapshot_relation"))
    op.execute(_sql(schema, "DROP TABLE __SCHEMA__.custom_import_snapshot_family"))
