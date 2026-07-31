"""Add bounded Provider Directory Profile delta publication state.

Revision ID: 20260730110000_provider_directory_profile_delta
Revises: 20260729120000_uhc_organization_evidence
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260730110000_provider_directory_profile_delta"
down_revision = "20260729120000_uhc_organization_evidence"
branch_labels = None
depends_on = None

_DELTA_RECEIPT_GUARD_FUNCTION = (
    "provider_directory_profile_delta_receipt_immutable_v1"
)
_DELTA_RECEIPT_WRITE_GUARD = (
    "provider_directory_profile_delta_receipt_write_guard"
)
_DELTA_RECEIPT_TRUNCATE_GUARD = (
    "provider_directory_profile_delta_receipt_truncate_guard"
)
_CAPACITY_CONSUMPTION_GUARD_FUNCTION = (
    "pd_profile_capacity_consumption_immutable_v1"
)
_CAPACITY_CONSUMPTION_WRITE_GUARD = (
    "pd_profile_capacity_consumption_write_guard"
)
_CAPACITY_CONSUMPTION_TRUNCATE_GUARD = (
    "pd_profile_capacity_consumption_truncate_guard"
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _add_checkpoint_columns(schema: str) -> None:
    table = "provider_directory_profile_build_checkpoint"
    for column in (
        sa.Column("executable_plan_hash", sa.String(64)),
        sa.Column(
            "materialization_mode",
            sa.String(16),
            server_default="full_swap",
            nullable=False,
        ),
        sa.Column("current_source_vector_hash", sa.String(64)),
        sa.Column("desired_source_vector_hash", sa.String(64)),
        sa.Column("current_source_context_vector_hash", sa.String(64)),
        sa.Column("desired_source_context_vector_hash", sa.String(64)),
        sa.Column("refresh_source_ids", postgresql.JSONB()),
        sa.Column("removed_source_ids", postgresql.JSONB()),
        sa.Column("affected_npi_stage", sa.String(63)),
        sa.Column("affected_npi_stage_oid", sa.BigInteger()),
        sa.Column("evidence_stage_storage_fingerprint", sa.String(64)),
        sa.Column("profile_stage_storage_fingerprint", sa.String(64)),
        sa.Column(
            "affected_npi_stage_storage_fingerprint",
            sa.String(64),
        ),
        sa.Column(
            "capacity_geometry_status",
            sa.String(32),
            server_default="legacy_unavailable",
            nullable=False,
        ),
        sa.Column("capacity_geometry_hash", sa.String(64)),
        sa.Column("capacity_geometry_json", postgresql.JSONB()),
        sa.Column(
            "cutover_forecast_status",
            sa.String(32),
            server_default="not_started",
            nullable=False,
        ),
        sa.Column("cutover_forecast_hash", sa.String(64)),
        sa.Column("cutover_forecast_json", postgresql.JSONB()),
    ):
        op.add_column(table, column, schema=schema)
    op.create_check_constraint(
        "pd_profile_build_checkpoint_plan_hash_check",
        table,
        "executable_plan_hash IS NULL "
        "OR executable_plan_hash ~ '^[0-9a-f]{64}$'",
        schema=schema,
    )
    op.create_check_constraint(
        "pd_profile_build_checkpoint_mode_check",
        table,
        "materialization_mode IN ('full_swap', 'source_delta')",
        schema=schema,
    )
    op.create_check_constraint(
        "pd_profile_build_checkpoint_stage_storage_check",
        table,
        "(evidence_stage_storage_fingerprint IS NULL "
        "AND profile_stage_storage_fingerprint IS NULL "
        "AND affected_npi_stage_storage_fingerprint IS NULL) "
        "OR (evidence_stage_storage_fingerprint "
        "~ '^[0-9a-f]{64}$' "
        "AND profile_stage_storage_fingerprint "
        "~ '^[0-9a-f]{64}$' "
        "AND ((materialization_mode = 'source_delta' "
        "AND affected_npi_stage_storage_fingerprint "
        "~ '^[0-9a-f]{64}$') "
        "OR (materialization_mode = 'full_swap' "
        "AND affected_npi_stage_storage_fingerprint IS NULL)))",
        schema=schema,
    )
    op.create_check_constraint(
        "pd_profile_build_checkpoint_delta_identity_check",
        table,
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
        "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object')",
        schema=schema,
    )
    op.create_check_constraint(
        "pd_profile_build_checkpoint_forecast_check",
        table,
        "(cutover_forecast_status = 'not_started' "
        "AND cutover_forecast_hash IS NULL "
        "AND cutover_forecast_json IS NULL) "
        "OR (cutover_forecast_status = 'verified' "
        "AND cutover_forecast_hash IS NOT NULL "
        "AND cutover_forecast_hash ~ '^[0-9a-f]{64}$' "
        "AND cutover_forecast_json IS NOT NULL "
        "AND jsonb_typeof(cutover_forecast_json::jsonb) = 'object')",
        schema=schema,
    )


def _create_serving_generation_table(schema: str) -> None:
    op.create_table(
        "provider_directory_profile_serving_generation",
        sa.Column("singleton_key", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("control_generation", sa.BigInteger(), nullable=False),
        sa.Column("generation_id", sa.String(64), nullable=False),
        sa.Column("selection_proof_id", sa.String(64), nullable=False),
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("profile_schema_version", sa.Integer(), nullable=False),
        sa.Column("profile_strategy_version", sa.String(128), nullable=False),
        sa.Column("source_vector_hash", sa.String(64), nullable=False),
        sa.Column("source_vector_json", postgresql.JSONB(), nullable=False),
        sa.Column(
            "source_context_vector_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "source_context_vector_json",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("executable_plan_hash", sa.String(64), nullable=False),
        sa.Column(
            "capacity_geometry_status",
            sa.String(32),
            nullable=False,
        ),
        sa.Column("capacity_geometry_hash", sa.String(64)),
        sa.Column("capacity_geometry_json", postgresql.JSONB()),
        sa.Column("cutover_forecast_hash", sa.String(64)),
        sa.Column("evidence_target_oid", sa.BigInteger(), nullable=False),
        sa.Column("profile_target_oid", sa.BigInteger(), nullable=False),
        sa.Column("evidence_rows", sa.BigInteger(), nullable=False),
        sa.Column("profile_rows", sa.BigInteger(), nullable=False),
        sa.Column("profile_as_of", sa.String(10), nullable=False),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("singleton_key"),
        sa.CheckConstraint(
            "singleton_key = 'global' "
            "AND ((status = 'published' AND operation = 'publish') "
            "OR (status = 'purged' AND operation = 'purge')) "
            "AND generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND ((capacity_geometry_status = 'legacy_unavailable' "
            "AND capacity_geometry_hash IS NULL "
            "AND capacity_geometry_json IS NULL) "
            "OR (capacity_geometry_status = 'verified' "
            "AND capacity_geometry_hash IS NOT NULL "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object')) "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND control_generation > 0 "
            "AND authority_revision > 0 "
            "AND profile_schema_version > 0 "
            "AND evidence_target_oid > 0 "
            "AND profile_target_oid > 0 "
            "AND evidence_rows >= 0 "
            "AND profile_rows >= 0 "
            "AND (cutover_forecast_hash IS NULL "
            "OR cutover_forecast_hash ~ '^[0-9a-f]{64}$')",
            name="pd_profile_serving_generation_values_check",
        ),
        schema=schema,
    )


def _create_delta_receipt_table(schema: str) -> None:
    table = "provider_directory_profile_delta_receipt"
    op.create_table(
        table,
        sa.Column("build_id", sa.String(64), nullable=False),
        sa.Column("executable_plan_hash", sa.String(64), nullable=False),
        sa.Column(
            "from_capacity_geometry_status",
            sa.String(32),
            nullable=False,
        ),
        sa.Column("from_capacity_geometry_hash", sa.String(64)),
        sa.Column("from_capacity_geometry_json", postgresql.JSONB()),
        sa.Column(
            "capacity_geometry_status",
            sa.String(32),
            nullable=False,
        ),
        sa.Column(
            "capacity_geometry_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "capacity_geometry_json",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("from_source_vector_hash", sa.String(64), nullable=False),
        sa.Column("to_source_vector_hash", sa.String(64), nullable=False),
        sa.Column(
            "from_source_context_vector_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "to_source_context_vector_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column("from_generation_id", sa.String(64), nullable=False),
        sa.Column("generation_id", sa.String(64), nullable=False),
        sa.Column("operation", sa.String(16), nullable=False),
        sa.Column("profile_as_of", sa.String(10), nullable=False),
        sa.Column("selection_proof_id", sa.String(64), nullable=False),
        sa.Column("control_generation", sa.BigInteger(), nullable=False),
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("evidence_target_oid", sa.BigInteger(), nullable=False),
        sa.Column("profile_target_oid", sa.BigInteger(), nullable=False),
        sa.Column("evidence_rows", sa.BigInteger(), nullable=False),
        sa.Column("profile_rows", sa.BigInteger(), nullable=False),
        sa.Column("evidence_inserted", sa.BigInteger(), nullable=False),
        sa.Column("evidence_deleted", sa.BigInteger(), nullable=False),
        sa.Column("profile_inserted", sa.BigInteger(), nullable=False),
        sa.Column("profile_deleted", sa.BigInteger(), nullable=False),
        sa.Column("cutover_forecast_hash", sa.String(64), nullable=False),
        sa.Column(
            "cutover_forecast_json",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column("cutover_actual_hash", sa.String(64), nullable=False),
        sa.Column(
            "cutover_actual_json",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "cutover_wal_start_lsn",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "cutover_wal_observed_lsn",
            sa.String(64),
            nullable=False,
        ),
        sa.Column("cutover_wal_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "evidence_target_bytes_before",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "evidence_target_bytes_after",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "evidence_target_growth_bytes",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "profile_target_bytes_before",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "profile_target_bytes_after",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "profile_target_growth_bytes",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "committed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("build_id"),
        sa.UniqueConstraint(
            "control_generation",
            "selection_proof_id",
            name="pd_profile_delta_receipt_control_proof_key",
        ),
        sa.CheckConstraint(
            "build_id ~ '^pdpb_[0-9a-f]{32}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND ((from_capacity_geometry_status = 'legacy_unavailable' "
            "AND from_capacity_geometry_hash IS NULL "
            "AND from_capacity_geometry_json IS NULL) "
            "OR (from_capacity_geometry_status = 'verified' "
            "AND from_capacity_geometry_hash IS NOT NULL "
            "AND from_capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND from_capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof("
            "from_capacity_geometry_json::jsonb) = 'object')) "
            "AND capacity_geometry_status = 'verified' "
            "AND capacity_geometry_hash IS NOT NULL "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_json IS NOT NULL "
            "AND jsonb_typeof(capacity_geometry_json::jsonb) = 'object' "
            "AND from_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND to_source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND from_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND to_source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND from_generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND generation_id ~ '^pdprofile_[0-9a-f]{32}$' "
            "AND operation IN ('publish', 'purge') "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND control_generation > 0 "
            "AND authority_revision > 0 "
            "AND evidence_target_oid > 0 "
            "AND profile_target_oid > 0 "
            "AND evidence_rows >= 0 "
            "AND profile_rows >= 0 "
            "AND evidence_inserted >= 0 "
            "AND evidence_deleted >= 0 "
            "AND profile_inserted >= 0 "
            "AND profile_deleted >= 0 "
            "AND cutover_forecast_hash ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(cutover_forecast_json::jsonb) = 'object' "
            "AND cutover_actual_hash ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(cutover_actual_json::jsonb) = 'object' "
            "AND cutover_wal_start_lsn IS NOT NULL "
            "AND cutover_wal_observed_lsn IS NOT NULL "
            "AND cutover_wal_bytes >= 0 "
            "AND evidence_target_bytes_before >= 0 "
            "AND evidence_target_bytes_after >= 0 "
            "AND evidence_target_growth_bytes >= 0 "
            "AND profile_target_bytes_before >= 0 "
            "AND profile_target_bytes_after >= 0 "
            "AND profile_target_growth_bytes >= 0",
            name="pd_profile_delta_receipt_values_check",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_profile_delta_receipt_vector_idx",
        table,
        ["to_source_vector_hash"],
        schema=schema,
    )
    receipt_ref = _qt(schema, table)
    function_ref = _qt(schema, _DELTA_RECEIPT_GUARD_FUNCTION)
    op.execute(
        f"""
        CREATE FUNCTION {function_ref}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'provider_directory_profile_delta_receipt_immutable';
        END;
        $$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_DELTA_RECEIPT_WRITE_GUARD)}
        BEFORE UPDATE OR DELETE ON {receipt_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {function_ref}();
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_DELTA_RECEIPT_TRUNCATE_GUARD)}
        BEFORE TRUNCATE ON {receipt_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {function_ref}();
        """
    )
    for trigger_name in (
        _DELTA_RECEIPT_WRITE_GUARD,
        _DELTA_RECEIPT_TRUNCATE_GUARD,
    ):
        op.execute(
            f"ALTER TABLE {receipt_ref} "
            f"ENABLE ALWAYS TRIGGER {_q(trigger_name)};"
        )


def _create_capacity_lease_consumption_table(schema: str) -> None:
    table = "provider_directory_profile_capacity_lease_consumption"
    op.create_table(
        table,
        sa.Column("attestation_id", sa.String(64), nullable=False),
        sa.Column("reservation_id", sa.String(128), nullable=False),
        sa.Column("lease_digest", sa.String(64), nullable=False),
        sa.Column("capacity_geometry_hash", sa.String(64), nullable=False),
        sa.Column("executable_plan_hash", sa.String(64), nullable=False),
        sa.Column("selection_proof_id", sa.String(64), nullable=False),
        sa.Column("source_vector_hash", sa.String(64), nullable=False),
        sa.Column(
            "source_context_vector_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("build_id", sa.String(64), nullable=False),
        sa.Column("profile_as_of", sa.String(10), nullable=False),
        sa.Column("contract_id", sa.String(64), nullable=False),
        sa.Column("key_id", sa.String(64), nullable=False),
        sa.Column("environment_id", sa.String(64), nullable=False),
        sa.Column("attestor_id", sa.String(64), nullable=False),
        sa.Column(
            "attestor_release_digest",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "public_key_fingerprint",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "database_system_identifier",
            sa.String(20),
            nullable=False,
        ),
        sa.Column("database_oid", sa.BigInteger(), nullable=False),
        sa.Column("database_name", sa.String(63), nullable=False),
        sa.Column(
            "tablespace_identity_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column(
            "volume_identity_hash",
            sa.String(64),
            nullable=False,
        ),
        sa.Column("canonical_lease_json", sa.TEXT(), nullable=False),
        sa.Column("signature", sa.String(86), nullable=False),
        sa.Column(
            "observed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "issued_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "accepted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "expires_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "max_build_deadline",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "recorded_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("attestation_id"),
        sa.UniqueConstraint(
            "reservation_id",
            name="pd_profile_capacity_consumption_reservation_key",
        ),
        sa.UniqueConstraint(
            "run_id",
            name="pd_profile_capacity_consumption_run_key",
        ),
        sa.CheckConstraint(
            "attestation_id ~ '^[0-9a-f]{64}$' "
            "AND reservation_id ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' "
            "AND lease_digest ~ '^[0-9a-f]{64}$' "
            "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
            "AND executable_plan_hash ~ '^[0-9a-f]{64}$' "
            "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
            "AND source_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND source_context_vector_hash ~ '^[0-9a-f]{64}$' "
            "AND run_id ~ '^run_[0-9a-f]{32}$' "
            "AND build_id ~ '^pdpb_[0-9a-f]{32}$' "
            "AND profile_as_of ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' "
            "AND contract_id = "
            "'provider-directory-database-capacity-lease-v1' "
            "AND key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND environment_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND attestor_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' "
            "AND attestor_release_digest ~ '^[0-9a-f]{64}$' "
            "AND public_key_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND database_system_identifier ~ '^[1-9][0-9]{0,19}$' "
            "AND database_system_identifier::numeric "
            "<= 18446744073709551615 "
            "AND database_oid BETWEEN 1 AND 4294967295 "
            "AND database_name ~ "
            "'^[A-Za-z0-9_$][A-Za-z0-9_$.-]{0,62}$' "
            "AND tablespace_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND volume_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND signature ~ '^[A-Za-z0-9_-]{86}$' "
            "AND observed_at <= issued_at "
            "AND issued_at - observed_at <= interval '300 seconds' "
            "AND accepted_at + interval '5 seconds' >= issued_at "
            "AND accepted_at - observed_at <= interval '305 seconds' "
            "AND accepted_at < expires_at "
            "AND accepted_at < max_build_deadline "
            "AND recorded_at = accepted_at "
            "AND recorded_at < expires_at "
            "AND recorded_at < max_build_deadline "
            "AND issued_at < max_build_deadline "
            "AND max_build_deadline <= expires_at "
            "AND expires_at - issued_at <= interval '86400 seconds'",
            name="pd_profile_capacity_consumption_values_check",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_profile_capacity_consumption_build_idx",
        table,
        ["build_id"],
        schema=schema,
    )
    table_ref = _qt(schema, table)
    function_ref = _qt(schema, _CAPACITY_CONSUMPTION_GUARD_FUNCTION)
    op.execute(
        f"""
        CREATE FUNCTION {function_ref}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'provider_directory_profile_capacity_consumption_immutable';
        END;
        $$;
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_CAPACITY_CONSUMPTION_WRITE_GUARD)}
        BEFORE UPDATE OR DELETE ON {table_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {function_ref}();
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_CAPACITY_CONSUMPTION_TRUNCATE_GUARD)}
        BEFORE TRUNCATE ON {table_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {function_ref}();
        """
    )
    for trigger_name in (
        _CAPACITY_CONSUMPTION_WRITE_GUARD,
        _CAPACITY_CONSUMPTION_TRUNCATE_GUARD,
    ):
        op.execute(
            f"ALTER TABLE {table_ref} "
            f"ENABLE ALWAYS TRIGGER {_q(trigger_name)};"
        )


def upgrade() -> None:
    schema = _schema()
    _add_checkpoint_columns(schema)
    _create_serving_generation_table(schema)
    _create_delta_receipt_table(schema)
    _create_capacity_lease_consumption_table(schema)


def _assert_downgrade_preconditions(schema: str) -> None:
    checkpoint_ref = _qt(
        schema,
        "provider_directory_profile_build_checkpoint",
    )
    serving_ref = _qt(
        schema,
        "provider_directory_profile_serving_generation",
    )
    receipt_ref = _qt(
        schema,
        "provider_directory_profile_delta_receipt",
    )
    consumption_ref = _qt(
        schema,
        "provider_directory_profile_capacity_lease_consumption",
    )
    op.execute(
        "LOCK TABLE "
        f"{consumption_ref}, {checkpoint_ref}, {receipt_ref}, {serving_ref} "
        "IN ACCESS EXCLUSIVE MODE NOWAIT;"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {serving_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_profile_delta_downgrade_serving_generation_not_empty'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (SELECT 1 FROM {receipt_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_profile_delta_downgrade_receipt_not_empty'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (SELECT 1 FROM {consumption_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_profile_delta_downgrade_capacity_consumption_not_empty'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM {checkpoint_ref}
                 WHERE executable_plan_hash IS NOT NULL
                    OR materialization_mode IS DISTINCT FROM 'full_swap'
                    OR current_source_vector_hash IS NOT NULL
                    OR desired_source_vector_hash IS NOT NULL
                    OR current_source_context_vector_hash IS NOT NULL
                    OR desired_source_context_vector_hash IS NOT NULL
                    OR refresh_source_ids IS NOT NULL
                    OR removed_source_ids IS NOT NULL
                    OR affected_npi_stage IS NOT NULL
                    OR affected_npi_stage_oid IS NOT NULL
                    OR evidence_stage_storage_fingerprint IS NOT NULL
                    OR profile_stage_storage_fingerprint IS NOT NULL
                    OR affected_npi_stage_storage_fingerprint IS NOT NULL
                    OR capacity_geometry_status
                       IS DISTINCT FROM 'legacy_unavailable'
                    OR capacity_geometry_hash IS NOT NULL
                    OR capacity_geometry_json IS NOT NULL
                    OR cutover_forecast_status
                       IS DISTINCT FROM 'not_started'
                    OR cutover_forecast_hash IS NOT NULL
                    OR cutover_forecast_json IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_profile_delta_downgrade_checkpoint_uses_delta_state'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$;
        """
    )


def downgrade() -> None:
    schema = _schema()
    _assert_downgrade_preconditions(schema)
    consumption_ref = _qt(
        schema,
        "provider_directory_profile_capacity_lease_consumption",
    )
    for trigger_name in (
        _CAPACITY_CONSUMPTION_TRUNCATE_GUARD,
        _CAPACITY_CONSUMPTION_WRITE_GUARD,
    ):
        op.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} "
            f"ON {consumption_ref};"
        )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_qt(schema, _CAPACITY_CONSUMPTION_GUARD_FUNCTION)}();"
    )
    op.drop_table(
        "provider_directory_profile_capacity_lease_consumption",
        schema=schema,
    )
    receipt_ref = _qt(
        schema,
        "provider_directory_profile_delta_receipt",
    )
    for trigger_name in (
        _DELTA_RECEIPT_TRUNCATE_GUARD,
        _DELTA_RECEIPT_WRITE_GUARD,
    ):
        op.execute(
            f"DROP TRIGGER IF EXISTS {_q(trigger_name)} "
            f"ON {receipt_ref};"
        )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_qt(schema, _DELTA_RECEIPT_GUARD_FUNCTION)}();"
    )
    op.drop_table(
        "provider_directory_profile_delta_receipt",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_profile_serving_generation",
        schema=schema,
    )
    table = "provider_directory_profile_build_checkpoint"
    op.drop_constraint(
        "pd_profile_build_checkpoint_forecast_check",
        table,
        type_="check",
        schema=schema,
    )
    op.drop_constraint(
        "pd_profile_build_checkpoint_delta_identity_check",
        table,
        type_="check",
        schema=schema,
    )
    op.drop_constraint(
        "pd_profile_build_checkpoint_mode_check",
        table,
        type_="check",
        schema=schema,
    )
    op.drop_constraint(
        "pd_profile_build_checkpoint_stage_storage_check",
        table,
        type_="check",
        schema=schema,
    )
    op.drop_constraint(
        "pd_profile_build_checkpoint_plan_hash_check",
        table,
        type_="check",
        schema=schema,
    )
    for column_name in (
        "cutover_forecast_json",
        "cutover_forecast_hash",
        "cutover_forecast_status",
        "capacity_geometry_json",
        "capacity_geometry_hash",
        "capacity_geometry_status",
        "affected_npi_stage_oid",
        "affected_npi_stage",
        "affected_npi_stage_storage_fingerprint",
        "profile_stage_storage_fingerprint",
        "evidence_stage_storage_fingerprint",
        "removed_source_ids",
        "refresh_source_ids",
        "desired_source_context_vector_hash",
        "current_source_context_vector_hash",
        "desired_source_vector_hash",
        "current_source_vector_hash",
        "materialization_mode",
        "executable_plan_hash",
    ):
        op.drop_column(table, column_name, schema=schema)
