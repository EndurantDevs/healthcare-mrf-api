# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit explicit first Profile publication and retain its immutable commit proof."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261001110000_profile_initial_publication"
down_revision = "20261001100000_profile_failed_cleanup_claim"
branch_labels = None
depends_on = None

_TABLE = "provider_directory_profile_initial_receipt"
_SERVING = "provider_directory_profile_serving_generation"
_CHECKPOINT = "provider_directory_profile_build_checkpoint"
_CHECKPOINT_CHECK = "pd_profile_build_checkpoint_delta_identity_check"
_GEOMETRY = "healthporta.provider-directory-profile-initial-capacity-geometry.v1"
_TARGET = "healthporta.provider-directory-profile-initial-target-state.v1"
_RECEIPT = "healthporta.provider-directory-profile-initial-receipt.v1"
# Revision-owned SQL cannot change when runtime guard recognition evolves.
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
IMMUTABLE_BODY = "BEGIN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_immutable'; END;"
# Frozen predicate from the original delta migration; its two branches are retained verbatim.
_CHECKPOINT_PREDECESSOR = (
    "(materialization_mode = 'full_swap' "
    "AND current_source_vector_hash IS NULL "
    "AND desired_source_vector_hash IS NULL "
    "AND current_source_context_vector_hash IS NULL "
    "AND desired_source_context_vector_hash IS NULL "
    "AND affected_npi_stage IS NULL "
    "AND affected_npi_stage_oid IS NULL "
    "AND capacity_geometry_status = 'legacy_unavailable' "
    "AND capacity_geometry_hash IS NULL "
    "AND capacity_geometry_json IS NULL) "
    "OR (materialization_mode = 'source_delta' "
    "AND current_source_vector_hash IS NOT NULL "
    "AND current_source_vector_hash ~ '^[0-9a-f]{64}$' "
    "AND desired_source_vector_hash IS NOT NULL "
    "AND desired_source_vector_hash ~ '^[0-9a-f]{64}$' "
    "AND current_source_context_vector_hash IS NOT NULL "
    "AND current_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
    "AND desired_source_context_vector_hash IS NOT NULL "
    "AND desired_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
    "AND affected_npi_stage IS NOT NULL "
    "AND affected_npi_stage_oid IS NOT NULL "
    "AND affected_npi_stage_oid > 0 "
    "AND capacity_geometry_status = 'verified' "
    "AND capacity_geometry_hash IS NOT NULL "
    "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
    "AND capacity_geometry_json IS NOT NULL "
    "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object')"
)


def _predecessor():
    path = Path(__file__).with_name("20260930140000_cms_capacity_preflight_receipt.py")
    spec = importlib.util.spec_from_file_location("_initial_profile_preflight_predecessor", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("profile_initial_predecessor_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_PREVIOUS = _predecessor()
_ORIGINAL = _PREVIOUS._ORIGINAL
_q = _ORIGINAL._q
_qt = _ORIGINAL._qt
_literal = _ORIGINAL._literal


def _serving_snapshot(alias):
    return "jsonb_build_object(" + ",".join(f"'{name}',{alias}.{name}" for name in _PROFILE_FIELDS) + ")"


def _receipt_matches_sql(schema):
    """Bind the sole initial receipt to the real installed result and previously consumed signed authority."""
    return f"""SELECT COALESCE(EXISTS (
        SELECT 1 FROM {_qt(schema, _SERVING)} serving
        JOIN {_qt(schema, "provider_directory_profile_capacity_lease_consumption")} consumed
          ON consumed.attestation_id=receipt.attestation_id AND consumed.build_id=receipt.build_id
          AND consumed.run_id=receipt.run_id AND consumed.admission_purpose='profile'
        JOIN {_qt(schema, "provider_directory_profile_capacity_preflight_receipt")} preflight
          ON preflight.receipt_sha256=receipt.payload->>'preflight_receipt_sha256'
          AND preflight.consumed_attestation_id=receipt.attestation_id AND preflight.consumed_run_id=receipt.run_id
        WHERE serving.singleton_key='global' AND serving.status='published' AND serving.operation='publish'
          AND serving.generation_id=receipt.generation_id AND receipt.publication_xid=pg_current_xact_id()
          AND serving.capacity_geometry_status='verified'
          AND serving.capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)}
          AND receipt.payload->'serving'={_serving_snapshot("serving")}
          AND serving.evidence_target_oid=to_regclass({_literal(_qt(schema, "provider_directory_profile_evidence"))})::oid::bigint
          AND serving.profile_target_oid=to_regclass({_literal(_qt(schema, "provider_directory_profile"))})::oid::bigint
          AND receipt.payload->>'contract_id'=receipt.contract_id
          AND receipt.payload->>'build_id'=receipt.build_id AND receipt.payload->>'run_id'=receipt.run_id
          AND receipt.payload->>'attestation_id'=receipt.attestation_id
          AND receipt.payload->>'capacity_geometry_hash'=serving.capacity_geometry_hash
          AND receipt.payload->'capacity_geometry'=serving.capacity_geometry_json
          AND receipt.payload->>'initial_target_state_sha256'=serving.capacity_geometry_json->>'initial_target_state_sha256'
          AND receipt.payload->'source_vector'=serving.source_vector_json
          AND receipt.payload->'source_context_vector'=serving.source_context_vector_json
          AND receipt.payload->>'executable_plan_hash'=serving.executable_plan_hash
          AND consumed.contract_id='provider-directory-database-capacity-lease-v3'
          AND consumed.lease_digest=receipt.payload->>'lease_digest'
          AND consumed.capacity_geometry_hash=serving.capacity_geometry_hash
          AND consumed.executable_plan_hash=serving.executable_plan_hash
          AND consumed.selection_proof_id=serving.selection_proof_id
          AND consumed.source_vector_hash=serving.source_vector_hash
          AND consumed.source_context_vector_hash=serving.source_context_vector_hash
          AND consumed.profile_as_of=serving.profile_as_of
          AND consumed.accepted_at<=receipt.committed_at AND receipt.committed_at<consumed.max_build_deadline
          AND consumed.max_build_deadline<=consumed.expires_at AND clock_timestamp()<consumed.max_build_deadline
          AND consumed.canonical_lease_json::jsonb->>'nonce'=preflight.receipt_sha256
          AND preflight.consumed_at<=receipt.committed_at AND receipt.committed_at<preflight.expires_at
          AND preflight.contract_id='healthporta.provider-directory-profile-capacity-preflight.v5'
          AND preflight.capacity_geometry_hash=serving.capacity_geometry_hash
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_request'->>'contract_id'
              ='healthporta.provider-directory-profile-capacity-preflight-request.v5'
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_request'->>'profile_materialization'
              ='initial_full_swap'
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_receipt'->'capacity_geometry'
              =serving.capacity_geometry_json
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_receipt'->>'serving_generation_preflight_sha256'
              =receipt.payload->>'initial_target_state_sha256'
    ),false)"""


def _receipt_insert_body(schema):
    return f"""BEGIN
        IF NOT {_qt(schema, "pd_profile_initial_receipt_matches")}(NEW) THEN
            RAISE EXCEPTION 'provider_directory_profile_initial_receipt_binding_invalid';
        END IF;
        RETURN NULL;
    END;"""


def _serving_insert_body(schema):
    return f"""BEGIN
        IF NEW.capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)} AND NOT EXISTS (
            SELECT 1 FROM {_qt(schema, _TABLE)} receipt
            WHERE receipt.generation_id=NEW.generation_id AND receipt.publication_xid=pg_current_xact_id()
              AND {_qt(schema, "pd_profile_initial_receipt_matches")}(receipt)
        ) THEN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_required'; END IF;
        RETURN NULL;
    END;"""


def _initial_checkpoint_check():
    """Require the verified sibling geometry without pretending it has current vectors."""
    return f"""COALESCE((materialization_mode='full_swap'
        AND current_source_vector_hash IS NULL AND current_source_context_vector_hash IS NULL
        AND desired_source_vector_hash IS NOT NULL AND desired_source_vector_hash ~ '^[0-9a-f]{{64}}$'
        AND desired_source_context_vector_hash IS NOT NULL AND desired_source_context_vector_hash ~ '^[0-9a-f]{{64}}$'
        AND affected_npi_stage IS NULL AND affected_npi_stage_oid IS NULL
        AND affected_npi_stage_storage_fingerprint IS NULL
        AND capacity_geometry_status='verified' AND capacity_geometry_hash IS NOT NULL
        AND capacity_geometry_hash ~ '^[0-9a-f]{{64}}$' AND capacity_geometry_json IS NOT NULL
        AND jsonb_typeof(capacity_geometry_json)='object'
        AND capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)}
        AND capacity_geometry_json->>'materialization_mode'='full_swap'
        AND capacity_geometry_json->'current_source_vector_hash'='null'::jsonb
        AND capacity_geometry_json->'current_context_vector_hash'='null'::jsonb
        AND capacity_geometry_json->>'desired_source_vector_hash'=desired_source_vector_hash
        AND capacity_geometry_json->>'desired_context_vector_hash'=desired_source_context_vector_hash
        AND capacity_geometry_json->>'profile_as_of'=profile_as_of
        AND capacity_geometry_json->>'executable_plan_hash'=executable_plan_hash
        AND capacity_geometry_json->>'initial_target_state_sha256' ~ '^[0-9a-f]{{64}}$'
        AND capacity_geometry_json->>'initial_receipt_oid' ~ '^[1-9][0-9]{{0,9}}$'
        AND (capacity_geometry_json->>'initial_receipt_oid')::numeric<=4294967295
        AND capacity_geometry_json->>'initial_receipt_storage_fingerprint' ~ '^[0-9a-f]{{64}}$'),false)"""


def _initial_preflight_check():
    """Add closed initial Profile and explicitly paired CMS cases to the unchanged old predicate."""
    common = _ORIGINAL._values_check().split("AND limits_contract_id = ", 1)[1]
    old_mode = "AND materialization_mode = 'source_delta' "
    if common.count(old_mode) != 1:
        raise RuntimeError("profile_initial_preflight_predecessor_changed")
    common = "limits_contract_id = " + common.replace(old_mode, "AND materialization_mode = 'full_swap' ")
    snapshot = f"""receipt_json::jsonb->'serving_generation_preflight'->>'contract_id'={_literal(_TARGET)}
        AND receipt_json::jsonb->'profile_execution_identity'->>'materialization_mode'='full_swap'"""
    profile = f"""contract_id='healthporta.provider-directory-profile-capacity-preflight.v5'
        AND request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v5'
        AND receipt_json::jsonb->>'profile_materialization'='initial_full_swap'
        AND receipt_json::jsonb->'capacity_geometry'->>'contract_id'={_literal(_GEOMETRY)}
        AND receipt_json::jsonb->'capacity_geometry'->>'materialization_mode'='full_swap'"""
    cms = """contract_id='healthporta.provider-directory-profile-capacity-preflight.v4'
        AND request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v4'
        AND receipt_json::jsonb->'capacity_geometry'->>'contract_id'='provider-directory-cms-nonprofile-capacity.v1'
        AND receipt_json::jsonb->'capacity_geometry'->>'admission_purpose'='cms_nonprofile'"""
    return f"({_PREVIOUS._cms_values_check()}) OR COALESCE((({profile}) OR ({cms})) AND {snapshot} AND {common},false)"


def _replace_checkpoint_check(schema):
    """Reject predecessor drift before adding a narrow verified full-swap alternative."""
    table = _qt(schema, _CHECKPOINT)
    probe = "pd_profile_initial_checkpoint_probe"
    replacement = "pd_profile_initial_checkpoint_next"
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {_q(probe)} CHECK ({_CHECKPOINT_PREDECESSOR}) NOT VALID")
    op.execute(f"""DO $$ DECLARE live_row pg_constraint%ROWTYPE; probe_row pg_constraint%ROWTYPE;
        BEGIN
            SELECT * INTO STRICT live_row FROM pg_constraint
                WHERE conrelid={_literal(table)}::regclass AND conname={_literal(_CHECKPOINT_CHECK)};
            SELECT * INTO STRICT probe_row FROM pg_constraint
                WHERE conrelid=live_row.conrelid AND conname={_literal(probe)};
            IF live_row.contype<>'c' OR NOT live_row.convalidated OR live_row.condeferrable
              OR live_row.condeferred OR live_row.connoinherit
              OR pg_get_expr(live_row.conbin, live_row.conrelid)
                 IS DISTINCT FROM pg_get_expr(probe_row.conbin, probe_row.conrelid)
            THEN RAISE EXCEPTION 'profile_initial_checkpoint_constraint_drift'; END IF;
        END $$""")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_q(probe)}")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT {_q(replacement)} "
        f"CHECK (({_CHECKPOINT_PREDECESSOR}) OR {_initial_checkpoint_check()}) NOT VALID"
    )
    op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {_q(replacement)}")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_q(_CHECKPOINT_CHECK)}")
    op.execute(f"ALTER TABLE {table} RENAME CONSTRAINT {_q(replacement)} TO {_q(_CHECKPOINT_CHECK)}")


def _empty_target_ddl(schema):
    """Freeze the existing canonical logged heap/index layouts for a genuinely empty schema."""
    evidence = _qt(schema, "provider_directory_profile_evidence")
    profile = _qt(schema, "provider_directory_profile")
    return f"""CREATE TABLE {evidence} (
        evidence_key char(32) PRIMARY KEY, npi bigint NOT NULL, fact_type varchar(64) NOT NULL,
        fact_key char(32) NOT NULL, value_json jsonb NOT NULL, source_id varchar(64) NOT NULL,
        endpoint_id varchar(64) NOT NULL, dataset_id varchar(96) NOT NULL, canonical_api_base text,
        source_org_name varchar(256), source_plan_name varchar(512), resource_type varchar(64) NOT NULL,
        resource_id varchar(256) NOT NULL, role_resource_id varchar(256), active boolean,
        effective_start varchar(64), effective_end varchar(64), observed_at timestamp without time zone);
        CREATE INDEX provider_directory_profile_evidence_npi_idx ON {evidence} (npi);
        CREATE INDEX provider_directory_profile_evidence_npi_fact_idx ON {evidence} (npi,fact_type,fact_key);
        CREATE INDEX provider_directory_profile_evidence_source_idx ON {evidence} (source_id,npi);
        CREATE INDEX provider_directory_profile_evidence_endpoint_idx ON {evidence} (endpoint_id,npi);
        CREATE TABLE {profile} (
        npi bigint PRIMARY KEY, profile_json jsonb NOT NULL, evidence_json jsonb NOT NULL,
        source_ids varchar[] NOT NULL, endpoint_ids varchar[] NOT NULL, dataset_ids varchar[] NOT NULL,
        source_count integer NOT NULL, independent_source_count integer NOT NULL, fact_count integer NOT NULL,
        generation_id varchar(64) NOT NULL, published_at timestamp without time zone NOT NULL);
        CREATE INDEX provider_directory_profile_generation_idx ON {profile} (generation_id);"""


def _create_empty_targets(schema):
    profile = _literal(_qt(schema, "provider_directory_profile"))
    evidence = _literal(_qt(schema, "provider_directory_profile_evidence"))
    op.execute(f"""DO $$ BEGIN
        IF (to_regclass({profile}) IS NULL) IS DISTINCT FROM (to_regclass({evidence}) IS NULL) THEN
            RAISE EXCEPTION 'provider_directory_profile_initial_partial_targets';
        END IF;
        IF to_regclass({profile}) IS NULL THEN
            IF EXISTS (SELECT 1 FROM {_qt(schema, _SERVING)}) THEN
                RAISE EXCEPTION 'provider_directory_profile_initial_missing_serving_targets';
            END IF;
            {_empty_target_ddl(schema)}
        END IF;
    END $$""")


def _create_receipt(schema):
    table = _qt(schema, _TABLE)
    op.execute(f"""CREATE TABLE {table} (
        build_id varchar(64) PRIMARY KEY CHECK (build_id ~ '^pdpb_[0-9a-f]{{32}}$'),
        attestation_id varchar(64) UNIQUE NOT NULL CHECK (attestation_id ~ '^[0-9a-f]{{64}}$'),
        generation_id varchar(64) UNIQUE NOT NULL CHECK (generation_id ~ '^pdprofile_[0-9a-f]{{32}}$'),
        singleton_key varchar(16) UNIQUE NOT NULL DEFAULT 'global' CHECK (singleton_key='global'),
        run_id varchar(64) NOT NULL CHECK (run_id ~ '^run_[0-9a-f]{{32}}$'),
        contract_id text NOT NULL CHECK (contract_id={_literal(_RECEIPT)}),
        payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object'),
        payload_sha256 varchar(64) NOT NULL
            CHECK (payload_sha256=encode(sha256(convert_to(payload::text,'UTF8')),'hex')),
        publication_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
        committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )""")
    for name, event in (
        ("pd_profile_initial_receipt_write_guard", "UPDATE OR DELETE"),
        ("pd_profile_initial_receipt_truncate_guard", "TRUNCATE"),
    ):
        function = _qt(schema, name)
        op.execute(
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql "
            f"SET search_path=pg_catalog AS $$ {IMMUTABLE_BODY} $$"
        )
        op.execute(
            f"CREATE TRIGGER {_q(name)} BEFORE {event} ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_q(name)}")
        op.execute(f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC")


def _create_deferred_guards(schema):
    matches = _qt(schema, "pd_profile_initial_receipt_matches")
    op.execute(
        f"CREATE FUNCTION {matches}(receipt {_qt(schema, _TABLE)}) RETURNS boolean "
        f"LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$ {_receipt_matches_sql(schema)} $$"
    )
    for table_name, name, events, body in (
        (_TABLE, "pd_profile_initial_receipt_insert", "INSERT", _receipt_insert_body(schema)),
        (_SERVING, "pd_profile_initial_serving_insert", "INSERT OR UPDATE", _serving_insert_body(schema)),
    ):
        function = _qt(schema, name)
        op.execute(
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ {body} $$"
        )
        op.execute(
            f"CREATE CONSTRAINT TRIGGER {_q(name)} AFTER {events} ON {_qt(schema, table_name)} "
            f"DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {function}()"
        )
        op.execute(f"ALTER TABLE {_qt(schema, table_name)} ENABLE ALWAYS TRIGGER {_q(name)}")
        op.execute(f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC")


def upgrade():
    """Hold the existing admission fence while installing schema without a serving-history row."""
    schema = _ORIGINAL._schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SELECT pg_advisory_xact_lock(hashtextextended('provider-directory-profile-capacity-preflight:v3',0))")
    tables = (
        "import_run",
        _CHECKPOINT,
        "provider_directory_profile_capacity_lease_consumption",
        _SERVING,
        _ORIGINAL._TABLE,
    )
    op.execute("LOCK TABLE " + ",".join(_qt(schema, name) for name in tables) + " IN ACCESS EXCLUSIVE MODE")
    _PREVIOUS._assert_exact_check(schema, _PREVIOUS._cms_values_check())
    _PREVIOUS._replace_check(schema, _initial_preflight_check())
    _replace_checkpoint_check(schema)
    _create_empty_targets(schema)
    _create_receipt(schema)
    _create_deferred_guards(schema)


def downgrade():
    """Retain initial history and serving dependencies until an explicit withdrawal plan exists."""
    raise RuntimeError("profile_initial_publication_downgrade_requires_explicit_plan")
