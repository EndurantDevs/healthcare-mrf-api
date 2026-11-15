# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replace duplicated physical Profile coordinates with schema-qualified names."""

from __future__ import annotations

import json
import os

from alembic import op

revision = "20261116090000_profile_capacity_logical_names"
down_revision = "20261115120000_named_output_recovery_document"
branch_labels = None
depends_on = None

_CHECKPOINT = "provider_directory_profile_build_checkpoint"
_SERVING = "provider_directory_profile_serving_generation"
_DELTA = "provider_directory_profile_delta_receipt"
_CONSUMPTION = "provider_directory_profile_capacity_lease_consumption"
_PREFLIGHT = "provider_directory_profile_capacity_preflight_receipt"
_INITIAL = "provider_directory_profile_initial_receipt"

# Revision-owned predicates retain semantic checks and original signed history.
_CHECKS = (
    (
        "provider_directory_profile_build_checkpoint",
        "pd_profile_build_checkpoint_delta_identity_check",
        (
            "((materialization_mode = 'full_swap' AND current_source_vector_hash IS NULL AND "
            "desired_source_vector_hash IS NULL AND current_source_context_vector_hash IS NULL AND "
            "desired_source_context_vector_hash IS NULL AND affected_npi_stage IS NULL AND capacity_geometry_status = "
            "'legacy_unavailable' AND capacity_geometry_hash IS NULL AND capacity_geometry_json IS NULL) OR "
            "(materialization_mode = 'source_delta' AND current_source_vector_hash IS NOT NULL AND "
            "current_source_vector_hash ~ '^[0-9a-f]{64}$' AND desired_source_vector_hash IS NOT NULL AND "
            "desired_source_vector_hash ~ '^[0-9a-f]{64}$' AND current_source_context_vector_hash IS NOT NULL AND "
            "current_source_context_vector_hash ~ '^[0-9a-f]{64}$' AND desired_source_context_vector_hash IS NOT NULL "
            "AND desired_source_context_vector_hash ~ '^[0-9a-f]{64}$' AND affected_npi_stage IS NOT NULL AND "
            "capacity_geometry_status = 'verified' AND capacity_geometry_hash IS NOT NULL AND capacity_geometry_hash ~"
            " '^[0-9a-f]{64}$' AND capacity_geometry_json IS NOT NULL AND jsonb_typeof(capacity_geometry_json::jsonb) "
            "= 'object')) OR (materialization_mode='full_swap' AND current_source_vector_hash IS NULL AND "
            "current_source_context_vector_hash IS NULL AND desired_source_vector_hash ~ '^[0-9a-f]{64}$' AND "
            "desired_source_context_vector_hash ~ '^[0-9a-f]{64}$' AND affected_npi_stage IS NULL AND "
            "capacity_geometry_status='verified' AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' AND "
            "jsonb_typeof(capacity_geometry_json::jsonb)='object' AND capacity_geometry_json::jsonb->>'contract_id' IN"
            " "
            "('healthporta.provider-directory-profile-initial-capacity-geometry.v1','healthporta.provider-directory-profile-initial-capacity-geometry.v2')"
            " AND capacity_geometry_json::jsonb->>'desired_source_vector_hash'=desired_source_vector_hash AND "
            "capacity_geometry_json::jsonb->>'desired_context_vector_hash'=desired_source_context_vector_hash AND "
            "capacity_geometry_json::jsonb->>'profile_as_of'=profile_as_of AND "
            "capacity_geometry_json::jsonb->>'executable_plan_hash'=executable_plan_hash AND "
            "capacity_geometry_json::jsonb->>'initial_target_state_sha256' ~ '^[0-9a-f]{64}$')"
        ),
    ),
    (
        "provider_directory_profile_build_checkpoint",
        "pd_profile_build_checkpoint_targets_check",
        (
            "targets_json IS NULL OR (jsonb_typeof(targets_json::jsonb)='object' AND targets_json::jsonb - "
            "ARRAY['evidence','profile'] = '{}'::jsonb AND targets_json::jsonb ?& ARRAY['evidence','profile'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence')='object' AND (targets_json::jsonb->'evidence') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'evidence') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'schema')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'name')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'name') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile')='object' AND (targets_json::jsonb->'profile') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'profile') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'schema')='string' AND "
            "length(targets_json::jsonb->'profile'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'name')='string' AND "
            "length(targets_json::jsonb->'profile'->>'name') BETWEEN 1 AND 63 AND targets_json::jsonb->'evidence' <> "
            "targets_json::jsonb->'profile')"
        ),
    ),
    (
        "provider_directory_profile_serving_generation",
        "pd_profile_serving_generation_targets_check",
        (
            "jsonb_typeof(targets_json::jsonb)='object' AND targets_json::jsonb - ARRAY['evidence','profile'] = "
            "'{}'::jsonb AND targets_json::jsonb ?& ARRAY['evidence','profile'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence')='object' AND (targets_json::jsonb->'evidence') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'evidence') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'schema')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'name')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'name') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile')='object' AND (targets_json::jsonb->'profile') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'profile') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'schema')='string' AND "
            "length(targets_json::jsonb->'profile'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'name')='string' AND "
            "length(targets_json::jsonb->'profile'->>'name') BETWEEN 1 AND 63 AND targets_json::jsonb->'evidence' <> "
            "targets_json::jsonb->'profile'"
        ),
    ),
    (
        "provider_directory_profile_serving_generation",
        "pd_profile_serving_generation_values_check",
        (
            "singleton_key = 'global' AND ((status = 'published' AND operation = 'publish') OR (status = 'purged' AND "
            "operation = 'purge')) AND generation_id ~ '^pdprofile_[0-9a-f]{32}$' AND selection_proof_id ~ "
            "'^[0-9a-f]{64}$' AND source_vector_hash ~ '^[0-9a-f]{64}$' AND source_context_vector_hash ~ "
            "'^[0-9a-f]{64}$' AND executable_plan_hash ~ '^[0-9a-f]{64}$' AND ((capacity_geometry_status = "
            "'legacy_unavailable' AND capacity_geometry_hash IS NULL AND capacity_geometry_json IS NULL) OR "
            "(capacity_geometry_status = 'verified' AND capacity_geometry_hash IS NOT NULL AND capacity_geometry_hash "
            "~ '^[0-9a-f]{64}$' AND capacity_geometry_json IS NOT NULL AND jsonb_typeof(capacity_geometry_json::jsonb)"
            " = 'object')) AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND control_generation > 0 AND "
            "authority_revision > 0 AND profile_schema_version > 0 AND evidence_rows >= 0 AND profile_rows >= 0 AND "
            "(cutover_forecast_hash IS NULL OR cutover_forecast_hash ~ '^[0-9a-f]{64}$')"
        ),
    ),
    (
        "provider_directory_profile_delta_receipt",
        "pd_profile_delta_receipt_targets_check",
        (
            "jsonb_typeof(targets_json::jsonb)='object' AND targets_json::jsonb - ARRAY['evidence','profile'] = "
            "'{}'::jsonb AND targets_json::jsonb ?& ARRAY['evidence','profile'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence')='object' AND (targets_json::jsonb->'evidence') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'evidence') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'schema')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'evidence'->'name')='string' AND "
            "length(targets_json::jsonb->'evidence'->>'name') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile')='object' AND (targets_json::jsonb->'profile') - "
            "ARRAY['schema','name'] = '{}'::jsonb AND (targets_json::jsonb->'profile') ?& ARRAY['schema','name'] AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'schema')='string' AND "
            "length(targets_json::jsonb->'profile'->>'schema') BETWEEN 1 AND 63 AND "
            "jsonb_typeof(targets_json::jsonb->'profile'->'name')='string' AND "
            "length(targets_json::jsonb->'profile'->>'name') BETWEEN 1 AND 63 AND targets_json::jsonb->'evidence' <> "
            "targets_json::jsonb->'profile'"
        ),
    ),
    (
        "provider_directory_profile_delta_receipt",
        "pd_profile_delta_receipt_values_check",
        (
            "build_id ~ '^pdpb_[0-9a-f]{32}$' AND executable_plan_hash ~ '^[0-9a-f]{64}$' AND "
            "((from_capacity_geometry_status = 'legacy_unavailable' AND from_capacity_geometry_hash IS NULL AND "
            "from_capacity_geometry_json IS NULL) OR (from_capacity_geometry_status = 'verified' AND "
            "from_capacity_geometry_hash IS NOT NULL AND from_capacity_geometry_hash ~ '^[0-9a-f]{64}$' AND "
            "from_capacity_geometry_json IS NOT NULL AND jsonb_typeof(from_capacity_geometry_json::jsonb) = 'object'))"
            " AND capacity_geometry_status = 'verified' AND capacity_geometry_hash IS NOT NULL AND "
            "capacity_geometry_hash ~ '^[0-9a-f]{64}$' AND capacity_geometry_json IS NOT NULL AND "
            "jsonb_typeof(capacity_geometry_json::jsonb) = 'object' AND from_source_vector_hash ~ '^[0-9a-f]{64}$' AND"
            " to_source_vector_hash ~ '^[0-9a-f]{64}$' AND from_source_context_vector_hash ~ '^[0-9a-f]{64}$' AND "
            "to_source_context_vector_hash ~ '^[0-9a-f]{64}$' AND from_generation_id ~ '^pdprofile_[0-9a-f]{32}$' AND "
            "generation_id ~ '^pdprofile_[0-9a-f]{32}$' AND operation IN ('publish', 'purge') AND profile_as_of ~ "
            "'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND selection_proof_id ~ '^[0-9a-f]{64}$' AND control_generation > 0 AND "
            "authority_revision > 0 AND evidence_rows >= 0 AND profile_rows >= 0 AND evidence_inserted >= 0 AND "
            "evidence_deleted >= 0 AND profile_inserted >= 0 AND profile_deleted >= 0 AND cutover_forecast_hash ~ "
            "'^[0-9a-f]{64}$' AND jsonb_typeof(cutover_forecast_json::jsonb) = 'object' AND cutover_actual_hash ~ "
            "'^[0-9a-f]{64}$' AND jsonb_typeof(cutover_actual_json::jsonb) = 'object' AND cutover_wal_start_lsn IS NOT"
            " NULL AND cutover_wal_observed_lsn IS NOT NULL AND cutover_wal_bytes >= 0 AND "
            "evidence_target_bytes_before >= 0 AND evidence_target_bytes_after >= 0 AND evidence_target_growth_bytes "
            ">= 0 AND profile_target_bytes_before >= 0 AND profile_target_bytes_after >= 0 AND "
            "profile_target_growth_bytes >= 0"
        ),
    ),
    (
        "provider_directory_profile_capacity_lease_consumption",
        "pd_profile_capacity_consumption_values_check",
        (
            "attestation_id ~ '^[0-9a-f]{64}$' AND reservation_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' AND "
            "lease_digest ~ '^[0-9a-f]{64}$' AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' AND executable_plan_hash ~ "
            "'^[0-9a-f]{64}$' AND selection_proof_id ~ '^[0-9a-f]{64}$' AND source_vector_hash ~ '^[0-9a-f]{64}$' AND "
            "source_context_vector_hash ~ '^[0-9a-f]{64}$' AND run_id ~ '^run_[0-9a-f]{32}$' AND build_id ~ "
            "'^pdpb_[0-9a-f]{32}$' AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND contract_id IN "
            "('provider-directory-database-capacity-lease-v1', 'provider-directory-database-capacity-lease-v2', "
            "'provider-directory-database-capacity-lease-v3', 'provider-directory-database-capacity-lease-v4') AND "
            "key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' AND environment_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' AND"
            " attestor_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' AND attestor_release_digest ~ '^[0-9a-f]{64}$' AND "
            "public_key_fingerprint ~ '^[0-9a-f]{64}$' AND signature ~ '^[A-Za-z0-9_-]{86}$' AND observed_at <= "
            "issued_at AND issued_at - observed_at <= interval '300 seconds' AND accepted_at + interval '5 seconds' >="
            " issued_at AND accepted_at - observed_at <= interval '305 seconds' AND accepted_at < expires_at AND "
            "accepted_at < max_build_deadline AND recorded_at = accepted_at AND recorded_at < expires_at AND "
            "recorded_at < max_build_deadline AND issued_at < max_build_deadline AND max_build_deadline <= expires_at "
            "AND expires_at - issued_at <= interval '86400 seconds'"
        ),
    ),
    (
        "provider_directory_profile_capacity_preflight_receipt",
        "pd_profile_capacity_preflight_values_check",
        (
            "((contract_id='healthporta.provider-directory-profile-capacity-preflight.v3' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v3') OR "
            "(contract_id='healthporta.provider-directory-profile-capacity-preflight.v4' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v4') OR "
            "(contract_id='healthporta.provider-directory-profile-capacity-preflight.v5' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v5') OR "
            "(contract_id='healthporta.provider-directory-profile-capacity-preflight.v6' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v6') OR "
            "(contract_id='healthporta.provider-directory-profile-capacity-preflight.v7' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v7') OR "
            "(contract_id='healthporta.provider-directory-profile-capacity-preflight.v8' AND "
            "request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v8')) AND "
            "limits_contract_id = 'healthporta.provider-directory-profile-capacity-limits.v2' AND ((contract_id IN "
            "('healthporta.provider-directory-profile-capacity-preflight.v3','healthporta.provider-directory-profile-capacity-preflight.v6')"
            " AND materialization_mode='source_delta') OR (contract_id IN "
            "('healthporta.provider-directory-profile-capacity-preflight.v5','healthporta.provider-directory-profile-capacity-preflight.v7')"
            " AND materialization_mode='full_swap' AND "
            "receipt_json::jsonb->>'profile_materialization'='initial_full_swap') OR (contract_id IN "
            "('healthporta.provider-directory-profile-capacity-preflight.v4','healthporta.provider-directory-profile-capacity-preflight.v8')"
            " AND materialization_mode IN ('source_delta','full_swap') AND "
            "receipt_json::jsonb->'capacity_geometry'->>'contract_id'='provider-directory-cms-nonprofile-capacity.v1' "
            "AND receipt_json::jsonb->'capacity_geometry'->>'admission_purpose'='cms_nonprofile')) AND "
            "profile_strategy_version = 'source-fact-role32-org32-member32-dataset-graph8-auth-npi5m-v6' AND "
            "receipt_sha256 ~ '^[0-9a-f]{64}$' AND request_nonce ~ '^[0-9a-f]{64}$' AND request_sha256 ~ "
            "'^[0-9a-f]{64}$' AND control_plane_receipt_sha256 ~ '^[0-9a-f]{64}$' AND selection_proof_id ~ "
            "'^[0-9a-f]{64}$' AND profile_input_digest ~ '^[0-9a-f]{64}$' AND limits_sha256 ~ '^[0-9a-f]{64}$' AND "
            "capacity_geometry_hash ~ '^[0-9a-f]{64}$' AND serving_preflight_sha256 ~ '^[0-9a-f]{64}$' AND "
            "quiescence_sha256 ~ '^[0-9a-f]{64}$' AND control_generation > 0 AND profile_schema_version > 0 AND "
            "issued_at < expires_at AND expires_at - issued_at <= interval '86400 seconds' AND "
            "jsonb_typeof(receipt_json::jsonb) = 'object' AND ((consumed_at IS NULL AND consumed_run_id IS NULL AND "
            "consumed_attestation_id IS NULL) OR (consumed_at IS NOT NULL AND consumed_at >= issued_at AND consumed_at"
            " < expires_at AND consumed_run_id ~ '^run_[0-9a-f]{32}$' AND consumed_attestation_id ~ '^[0-9a-f]{64}$'))"
        ),
    ),
)
_DROP_COLUMNS = {
    _CHECKPOINT: (
        "evidence_stage_oid",
        "profile_stage_oid",
        "affected_npi_stage_oid",
        "evidence_target_oid",
        "profile_target_oid",
        "evidence_stage_storage_fingerprint",
        "profile_stage_storage_fingerprint",
        "affected_npi_stage_storage_fingerprint",
    ),
    _SERVING: ("evidence_target_oid", "profile_target_oid"),
    _DELTA: ("evidence_target_oid", "profile_target_oid"),
    _CONSUMPTION: (
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespace_identity_hash",
        "volume_identity_hash",
    ),
}


def _schema():
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema, table):
    return f"{_q(schema)}.{_q(table)}"


def _literal(text):
    return "'" + text.replace("'", "''") + "'"


def _named_targets(schema):
    return {
        "evidence": {"schema": schema, "name": "provider_directory_profile_evidence"},
        "profile": {"schema": schema, "name": "provider_directory_profile"},
    }


def _add_named_targets(schema):
    """Backfill old writer names without updating immutable receipt rows."""
    default = _literal(json.dumps(_named_targets(schema), sort_keys=True, separators=(",", ":")))
    for table in (_CHECKPOINT, _SERVING, _DELTA):
        nullable = "" if table == _CHECKPOINT else " NOT NULL"
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD COLUMN targets_json jsonb{nullable} DEFAULT {default}::jsonb")
        op.execute(f"ALTER TABLE {_qt(schema, table)} ALTER COLUMN targets_json DROP DEFAULT")


def _replace_semantic_checks(schema):
    for table, name, condition in _CHECKS:
        op.execute(f"ALTER TABLE {_qt(schema, table)} DROP CONSTRAINT IF EXISTS {_q(name)}")
        op.execute(f"ALTER TABLE {_qt(schema, table)} ADD CONSTRAINT {_q(name)} CHECK ({condition})")
    table = _qt(schema, _INITIAL)
    name = "provider_directory_profile_initial_receipt_contract_id_check"
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_q(name)}")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT {_q(name)} CHECK (contract_id IN ("
        "'healthporta.provider-directory-profile-initial-receipt.v1',"
        "'healthporta.provider-directory-profile-initial-receipt.v2'))"
    )


def _drop_physical_coordinates(schema):
    for name in ("pd_profile_build_checkpoint_oid_check", "pd_profile_build_checkpoint_stage_storage_check"):
        op.execute(f"ALTER TABLE {_qt(schema, _CHECKPOINT)} DROP CONSTRAINT IF EXISTS {_q(name)}")
    for table, columns in _DROP_COLUMNS.items():
        for column in columns:
            op.execute(f"ALTER TABLE {_qt(schema, table)} DROP COLUMN {_q(column)}")


def downgrade():
    """Physical coordinates cannot be recovered from portable named snapshots."""
    raise RuntimeError("profile_capacity_logical_names_requires_forward_migration")


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
    "evidence_rows",
    "profile_rows",
)
_GEOMETRY = "healthporta.provider-directory-profile-initial-capacity-geometry.v2"
_TABLE = _INITIAL


def _serving_snapshot(alias):
    fields = ",".join(f"'{name}',{alias}.{name}" for name in _PROFILE_FIELDS)
    return f"jsonb_build_object({fields},'targets',{alias}.targets_json)"


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
          AND ((receipt.contract_id='healthporta.provider-directory-profile-initial-receipt.v1'
            AND serving.capacity_geometry_json->>'contract_id'='healthporta.provider-directory-profile-initial-capacity-geometry.v1'
            AND consumed.contract_id='provider-directory-database-capacity-lease-v3'
            AND preflight.contract_id='healthporta.provider-directory-profile-capacity-preflight.v5'
            AND preflight.request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v5'
            AND (receipt.payload->'serving') - ARRAY['evidence_target_oid','profile_target_oid']
                =({_serving_snapshot("serving")}) - 'targets')
          OR (receipt.contract_id='healthporta.provider-directory-profile-initial-receipt.v2'
            AND serving.capacity_geometry_json->>'contract_id'={_literal(_GEOMETRY)}
            AND consumed.contract_id='provider-directory-database-capacity-lease-v4'
            AND preflight.contract_id='healthporta.provider-directory-profile-capacity-preflight.v7'
            AND preflight.request_contract_id='healthporta.provider-directory-profile-capacity-preflight-request.v7'
            AND receipt.payload->'serving'={_serving_snapshot("serving")}))
          AND receipt.payload->>'contract_id'=receipt.contract_id
          AND receipt.payload->>'build_id'=receipt.build_id AND receipt.payload->>'run_id'=receipt.run_id
          AND receipt.payload->>'attestation_id'=receipt.attestation_id
          AND receipt.payload->>'capacity_geometry_hash'=serving.capacity_geometry_hash
          AND receipt.payload->'capacity_geometry'=serving.capacity_geometry_json
          AND receipt.payload->>'initial_target_state_sha256'=serving.capacity_geometry_json->>'initial_target_state_sha256'
          AND receipt.payload->'source_vector'=serving.source_vector_json
          AND receipt.payload->'source_context_vector'=serving.source_context_vector_json
          AND receipt.payload->>'executable_plan_hash'=serving.executable_plan_hash
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
          AND preflight.capacity_geometry_hash=serving.capacity_geometry_hash
          AND consumed.canonical_lease_json::jsonb->'signing_preflight_guard'->'healthcare_request'->>'contract_id'
              =preflight.request_contract_id
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
        IF NEW.capacity_geometry_json->>'contract_id' IN (
            'healthporta.provider-directory-profile-initial-capacity-geometry.v1',{_literal(_GEOMETRY)}) AND NOT EXISTS (
            SELECT 1 FROM {_qt(schema, _TABLE)} receipt
            WHERE receipt.generation_id=NEW.generation_id AND receipt.publication_xid=pg_current_xact_id()
              AND {_qt(schema, "pd_profile_initial_receipt_matches")}(receipt)
        ) THEN RAISE EXCEPTION 'provider_directory_profile_initial_receipt_required'; END IF;
        RETURN NULL;
    END;"""


def _replace_initial_guards(schema):
    matches = _qt(schema, "pd_profile_initial_receipt_matches")
    op.execute(
        f"CREATE OR REPLACE FUNCTION {matches}(receipt {_qt(schema, _INITIAL)}) RETURNS boolean "
        f"LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$ {_receipt_matches_sql(schema)} $$"
    )
    for name, body in (
        ("pd_profile_initial_receipt_insert", _receipt_insert_body(schema)),
        ("pd_profile_initial_serving_insert", _serving_insert_body(schema)),
    ):
        op.execute(
            f"CREATE OR REPLACE FUNCTION {_qt(schema, name)}() RETURNS trigger LANGUAGE plpgsql "
            f"SET search_path=pg_catalog AS $$ {body} $$"
        )


def _qualified_target(schema, name):
    return f"jsonb_build_object('schema_name',{_literal(schema)},'relation_name',{_literal(name)})"


def _stored_target(alias, role):
    return (
        f"jsonb_build_object('schema_name',{alias}.targets_json->'{role}'->>'schema',"
        f"'relation_name',{alias}.targets_json->'{role}'->>'name')"
    )


def _legacy_profile_snapshot(document, schema):
    old = "ARRAY['evidence_target_oid','profile_target_oid']"
    addition = (
        f"jsonb_build_object('evidence_target',{_qualified_target(schema, 'provider_directory_profile_evidence')},"
        f"'profile_target',{_qualified_target(schema, 'provider_directory_profile')})"
    )
    projection = (
        f"CASE WHEN {document} ?& {old} AND NOT {document} ?| ARRAY['evidence_target','profile_target'] "
        f"THEN ({document}-{old})||{addition} ELSE {document} END"
    )
    return f"(CASE WHEN {document} IS NULL OR {document}='null'::jsonb THEN {document} ELSE {projection} END)"


def _native_snapshot_replacements(schema):
    pairs = []
    for role in ("evidence", "profile"):
        table = "provider_directory_profile_evidence" if role == "evidence" else "provider_directory_profile"
        named = _qualified_target(schema, table)
        replacement = f"'{role}_target',{_stored_target('p', role)}"
        pairs.extend(
            (
                (f"'{role}_target_oid',p.{role}_target_oid", replacement),
                (
                    f"'{role}_target',CASE WHEN to_regclass('{_q(schema)}.{table}') IS NOT NULL THEN {named} END",
                    replacement,
                ),
            )
        )
    return pairs


def _native_matches_replacements(schema):
    quoted = _q(schema)
    projected = _legacy_profile_snapshot("(p->'profile')", schema)
    previous = f"""AND (p->'profile'='null'::jsonb OR (
            (p->'profile'->>'profile_target_oid')::bigint=to_regclass('{quoted}.provider_directory_profile')::oid::bigint
            AND (p->'profile'->>'evidence_target_oid')::bigint=to_regclass('{quoted}.provider_directory_profile_evidence')::oid::bigint))"""
    pairs = [
        (
            f"p={quoted}.cms_serving_native_snapshot()",
            f"jsonb_set(p,'{{profile}}',{projected})={quoted}.cms_serving_native_snapshot()",
        ),
        (previous, ""),
    ]
    for role in ("evidence", "profile"):
        table = "provider_directory_profile_evidence" if role == "evidence" else "provider_directory_profile"
        pairs.append(
            (
                f"{projected}->'{role}_target'={_qualified_target(schema, table)}",
                f"{projected}->'{role}_target'=({quoted}.cms_serving_native_snapshot())->'profile'->'{role}_target'",
            )
        )
    return pairs


def _cms_profile_replacements(schema):
    projected = _legacy_profile_snapshot("(p->'profile')", schema)
    comparisons = [("current_native->'profile'=p->'profile'", f"current_native->'profile'={projected}")]
    previous = _legacy_profile_snapshot("previous", schema)
    prior = _legacy_profile_snapshot("(prior->kind)", schema)
    return {
        "cms_serving_native_snapshot": _native_snapshot_replacements(schema),
        "cms_serving_native_matches": _native_matches_replacements(schema),
        "cms_serving_snapshot_receipt_matches": comparisons,
        "cms_serving_current_receipt_matches": comparisons,
        "cms_serving_require_fresh": [
            (
                "previous IS DISTINCT FROM prior->kind",
                f"(CASE WHEN kind='profile' THEN {previous} ELSE previous END) IS DISTINCT FROM "
                f"(CASE WHEN kind='profile' THEN {prior} ELSE prior->kind END)",
            ),
        ],
    }


def _replace_existing_function(schema, name, arguments, replacements):
    """Change only retired Profile expressions in the existing named routine."""
    signature = _literal(f"{_qt(schema, name)}({arguments})")
    statements = [f"definition := pg_get_functiondef({signature}::regprocedure);"]
    statements.extend(
        f"definition := replace(definition,{_literal(old)},{_literal(new)});" for old, new in replacements
    )
    op.execute(
        "DO $migration$ DECLARE definition text; BEGIN " + " ".join(statements) + " EXECUTE definition; END $migration$"
    )


def _replace_cms_profile_guards(schema):
    for name, replacements in _cms_profile_replacements(schema).items():
        arguments = (
            "" if name.endswith("native_snapshot") else "text,jsonb" if name.endswith("require_fresh") else "jsonb"
        )
        _replace_existing_function(schema, name, arguments, replacements)


PROFILE_FIELDS = (
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


def profile_transition_body(schema):
    """Return the migration's unchanged Profile transition body for installation and recognition."""
    old_sql = "jsonb_build_object(" + ",".join(f"'{field}',OLD.{field}" for field in PROFILE_FIELDS) + ")"
    new_sql = "jsonb_build_object(" + ",".join(f"'{field}',NEW.{field}" for field in PROFILE_FIELDS) + ")"
    return f"""BEGIN
        IF TG_OP='UPDATE' AND ({old_sql}) IS NOT DISTINCT FROM ({new_sql})
            AND OLD.source_vector_json IS NOT DISTINCT FROM NEW.source_vector_json THEN RETURN NULL; END IF;
        IF ((TG_OP<>'INSERT' AND OLD.source_vector_json @> '[{{"source_id":"cms-npd"}}]'::jsonb) OR
            (TG_OP<>'DELETE' AND NEW.source_vector_json @> '[{{"source_id":"cms-npd"}}]'::jsonb))
            OR EXISTS (SELECT 1 FROM {schema}.provider_directory_cms_serving_receipt) THEN
            PERFORM {schema}.cms_serving_require_fresh('profile',CASE WHEN TG_OP='INSERT' THEN NULL ELSE {old_sql} END);
        END IF;
        RETURN NULL; END"""


def _named_profile_snapshot(alias):
    semantic_fields = [name for name in PROFILE_FIELDS if not name.endswith("_target_oid")]
    fields = ",".join(f"'{field}',{alias}.{field}" for field in semantic_fields)
    for role in ("evidence", "profile"):
        fields += (
            f",'{role}_target',jsonb_build_object('schema_name',"
            f"{alias}.targets_json->'{role}'->>'schema','relation_name',"
            f"{alias}.targets_json->'{role}'->>'name')"
        )
    return f"jsonb_build_object({fields})"


def named_profile_transition_body(schema):
    """Retain data transition checks while using the stored schema-qualified targets."""
    body = profile_transition_body(schema)
    for alias in ("OLD", "NEW"):
        previous = "jsonb_build_object(" + ",".join(f"'{name}',{alias}.{name}" for name in PROFILE_FIELDS) + ")"
        body = body.replace(previous, _named_profile_snapshot(alias))
    return body


def upgrade():
    """Preserve signed history and semantic guards while retiring duplicated coordinates."""
    schema = _schema()
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute("SET LOCAL search_path=pg_catalog")
    tables = sorted(
        (_CHECKPOINT, _SERVING, _DELTA, _CONSUMPTION, _PREFLIGHT, _INITIAL, "provider_directory_cms_serving_receipt")
    )
    op.execute("LOCK TABLE " + ",".join(_qt(schema, table) for table in tables) + " IN ACCESS EXCLUSIVE MODE NOWAIT")
    _add_named_targets(schema)
    _replace_semantic_checks(schema)
    _replace_cms_profile_guards(schema)
    body = named_profile_transition_body(_q(schema))
    op.execute(
        f"CREATE OR REPLACE FUNCTION {_qt(schema, 'cms_serving_profile_transition')}() RETURNS trigger "
        f"LANGUAGE plpgsql SET search_path=pg_catalog AS $$ {body} $$"
    )
    _replace_initial_guards(schema)
    _drop_physical_coordinates(schema)
