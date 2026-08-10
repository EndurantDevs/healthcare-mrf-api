# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist replay-fenced Provider Profile capacity preflight receipts.

Revision ID: 20260811010000_provider_directory_profile_capacity_preflight_receipt
Revises: 20260810130000_provider_directory_reviewed_subset_terminal_window
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260811010000_provider_directory_profile_capacity_preflight_receipt"
down_revision = "20260810130000_provider_directory_reviewed_subset_terminal_window"
branch_labels = None
depends_on = None

_TABLE = "provider_directory_profile_capacity_preflight_receipt"
_INDEX = "pd_profile_capacity_preflight_open_idx"
_VALUES_CONSTRAINT = "pd_profile_capacity_preflight_values_check"
_UPDATE_FUNCTION = "provider_directory_profile_capacity_preflight_update_guard"
_UPDATE_TRIGGER = "pd_profile_capacity_preflight_update_guard"
_DELETE_FUNCTION = "provider_directory_profile_capacity_preflight_delete_guard"
_DELETE_TRIGGER = "pd_profile_capacity_preflight_delete_guard"
_TRUNCATE_TRIGGER = "pd_profile_capacity_preflight_truncate_guard"
_CAPACITY_TABLE = "provider_directory_profile_capacity_lease_consumption"
_CAPACITY_CONSTRAINT = "pd_profile_capacity_consumption_values_check"
_CAPACITY_PROBE_CONSTRAINT = "pd_profile_capacity_consumption_values_probe"
_CAPACITY_NEXT_CONSTRAINT = "pd_profile_capacity_consumption_values_next"
_V1_CONTRACT = "provider-directory-database-capacity-lease-v1"
_V2_CONTRACT = "provider-directory-database-capacity-lease-v2"
_V3_CONTRACT = "provider-directory-database-capacity-lease-v3"
_CONTROL_PLANE_RECEIPT_COLUMN = "control_plane_receipt_sha256"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _values_check() -> str:
    return (
        "contract_id = "
        "'healthporta.provider-directory-profile-capacity-preflight.v3' "
        "AND request_contract_id = "
        "'healthporta.provider-directory-profile-capacity-preflight-request.v3' "
        "AND limits_contract_id = "
        "'healthporta.provider-directory-profile-capacity-limits.v2' "
        "AND materialization_mode = 'source_delta' "
        "AND profile_strategy_version = "
        "'source-fact-role32-org32-member32-dataset-graph8-auth-npi5m-v6' "
        "AND receipt_sha256 ~ '^[0-9a-f]{64}$' "
        "AND request_nonce ~ '^[0-9a-f]{64}$' "
        "AND request_sha256 ~ '^[0-9a-f]{64}$' "
        f"AND {_CONTROL_PLANE_RECEIPT_COLUMN} ~ '^[0-9a-f]{{64}}$' "
        "AND selection_proof_id ~ '^[0-9a-f]{64}$' "
        "AND profile_input_digest ~ '^[0-9a-f]{64}$' "
        "AND limits_sha256 ~ '^[0-9a-f]{64}$' "
        "AND capacity_geometry_hash ~ '^[0-9a-f]{64}$' "
        "AND serving_preflight_sha256 ~ '^[0-9a-f]{64}$' "
        "AND quiescence_sha256 ~ '^[0-9a-f]{64}$' "
        "AND control_generation > 0 "
        "AND profile_schema_version > 0 "
        "AND issued_at < expires_at "
        "AND expires_at - issued_at <= interval '86400 seconds' "
        "AND jsonb_typeof(receipt_json::jsonb) = 'object' "
        "AND ((consumed_at IS NULL AND consumed_run_id IS NULL "
        "AND consumed_attestation_id IS NULL) "
        "OR (consumed_at IS NOT NULL "
        "AND consumed_at >= issued_at AND consumed_at < expires_at "
        "AND consumed_run_id ~ '^run_[0-9a-f]{32}$' "
        "AND consumed_attestation_id ~ '^[0-9a-f]{64}$'))"
    )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _capacity_contract_predicate(*contract_ids: str) -> str:
    quoted = ", ".join(_literal(value) for value in contract_ids)
    return f"contract_id IN ({quoted})"


def _capacity_consumption_check(contract_predicate: str) -> str:
    return (
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
        f"AND {contract_predicate} "
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
        "AND expires_at - issued_at <= interval '86400 seconds'"
    )


def _add_capacity_constraint(
    schema: str,
    constraint_name: str,
    condition: str,
) -> None:
    op.execute(
        f"ALTER TABLE {_qt(schema, _CAPACITY_TABLE)} "
        f"ADD CONSTRAINT {_q(constraint_name)} "
        f"CHECK ({condition}) NOT VALID;"
    )


def _assert_exact_capacity_constraint(schema: str, condition: str) -> None:
    _add_capacity_constraint(
        schema,
        _CAPACITY_PROBE_CONSTRAINT,
        condition,
    )
    op.execute(
        f"""
        DO $$
        DECLARE
            live_row pg_constraint%ROWTYPE;
            probe_row pg_constraint%ROWTYPE;
        BEGIN
            SELECT constraint_row.* INTO STRICT live_row
              FROM pg_constraint AS constraint_row
              JOIN pg_class AS relation
                ON relation.oid = constraint_row.conrelid
              JOIN pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = {_literal(schema)}
               AND relation.relname = {_literal(_CAPACITY_TABLE)}
               AND constraint_row.conname = {_literal(_CAPACITY_CONSTRAINT)};
            SELECT constraint_row.* INTO STRICT probe_row
              FROM pg_constraint AS constraint_row
             WHERE constraint_row.conrelid = live_row.conrelid
               AND constraint_row.conname =
                   {_literal(_CAPACITY_PROBE_CONSTRAINT)};
            IF live_row.contype <> 'c'
               OR NOT live_row.convalidated
               OR live_row.condeferrable
               OR live_row.condeferred
               OR live_row.connoinherit
               OR live_row.conbin IS DISTINCT FROM probe_row.conbin
            THEN
                RAISE EXCEPTION
                    'provider_directory_capacity_lease_constraint_drift';
            END IF;
        END;
        $$;
        """
    )
    op.execute(
        f"ALTER TABLE {_qt(schema, _CAPACITY_TABLE)} "
        f"DROP CONSTRAINT {_q(_CAPACITY_PROBE_CONSTRAINT)};"
    )


def _replace_capacity_constraint(
    schema: str,
    *,
    expected_contracts: tuple[str, ...],
    replacement_contracts: tuple[str, ...],
) -> None:
    expected = _capacity_consumption_check(
        _capacity_contract_predicate(*expected_contracts)
    )
    replacement = _capacity_consumption_check(
        _capacity_contract_predicate(*replacement_contracts)
    )
    _assert_exact_capacity_constraint(schema, expected)
    _add_capacity_constraint(schema, _CAPACITY_NEXT_CONSTRAINT, replacement)
    table_ref = _qt(schema, _CAPACITY_TABLE)
    op.execute(
        f"ALTER TABLE {table_ref} "
        f"VALIDATE CONSTRAINT {_q(_CAPACITY_NEXT_CONSTRAINT)};"
    )
    op.execute(
        f"ALTER TABLE {table_ref} " f"DROP CONSTRAINT {_q(_CAPACITY_CONSTRAINT)};"
    )
    op.execute(
        f"ALTER TABLE {table_ref} "
        f"RENAME CONSTRAINT {_q(_CAPACITY_NEXT_CONSTRAINT)} "
        f"TO {_q(_CAPACITY_CONSTRAINT)};"
    )
    _assert_exact_capacity_constraint(schema, replacement)


def _lock_capacity_ledger(schema: str) -> None:
    op.execute(
        f"LOCK TABLE {_qt(schema, _CAPACITY_TABLE)} " "IN ACCESS EXCLUSIVE MODE NOWAIT;"
    )


def _assert_no_v3_consumption(schema: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {_qt(schema, _CAPACITY_TABLE)}
                 WHERE contract_id = {_literal(_V3_CONTRACT)}
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_capacity_lease_v3_history_exists';
            END IF;
        END;
        $$;
        """
    )


def _create_table(schema: str) -> None:
    op.create_table(
        _TABLE,
        sa.Column("receipt_sha256", sa.String(length=64), nullable=False),
        sa.Column("request_nonce", sa.String(length=64), nullable=False),
        sa.Column("request_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            _CONTROL_PLANE_RECEIPT_COLUMN,
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("contract_id", sa.String(length=96), nullable=False),
        sa.Column("request_contract_id", sa.String(length=96), nullable=False),
        sa.Column("limits_contract_id", sa.String(length=96), nullable=False),
        sa.Column("selection_proof_id", sa.String(length=64), nullable=False),
        sa.Column("profile_input_digest", sa.String(length=64), nullable=False),
        sa.Column("control_generation", sa.BigInteger(), nullable=False),
        sa.Column("profile_schema_version", sa.Integer(), nullable=False),
        sa.Column(
            "profile_strategy_version",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column("materialization_mode", sa.String(length=16), nullable=False),
        sa.Column("limits_sha256", sa.String(length=64), nullable=False),
        sa.Column("capacity_geometry_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "serving_preflight_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("quiescence_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "receipt_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("issued_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("consumed_run_id", sa.String(length=64)),
        sa.Column("consumed_attestation_id", sa.String(length=64)),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(_values_check(), name=_VALUES_CONSTRAINT),
        sa.PrimaryKeyConstraint("receipt_sha256"),
        sa.UniqueConstraint(
            "request_nonce",
            name="pd_profile_capacity_preflight_request_nonce_key",
        ),
        sa.UniqueConstraint(
            "request_sha256",
            name="pd_profile_capacity_preflight_request_sha_key",
        ),
        schema=schema,
    )
    op.create_index(
        _INDEX,
        _TABLE,
        ["consumed_at", "expires_at"],
        unique=False,
        schema=schema,
    )


def _create_guards(schema: str) -> None:
    table_ref = _qt(schema, _TABLE)
    op.execute(
        f"""
        CREATE FUNCTION {_qt(schema, _UPDATE_FUNCTION)}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.consumed_at IS NOT NULL
               OR NEW.consumed_at IS NULL
               OR NEW.consumed_run_id IS NULL
               OR NEW.consumed_attestation_id IS NULL
               OR to_jsonb(NEW) - ARRAY[
                    'consumed_at', 'consumed_run_id',
                    'consumed_attestation_id'
                  ]::text[]
                  IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
                    'consumed_at', 'consumed_run_id',
                    'consumed_attestation_id'
                  ]::text[]
            THEN
                RAISE EXCEPTION
                    'provider_directory_profile_capacity_preflight_update_invalid';
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {_qt(schema, _DELETE_FUNCTION)}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION
                'provider_directory_profile_capacity_preflight_history_immutable';
        END;
        $$;
        """
    )
    trigger_statements = (
        f"CREATE TRIGGER {_q(_UPDATE_TRIGGER)} "
        f"BEFORE UPDATE ON {table_ref} FOR EACH ROW "
        f"EXECUTE FUNCTION {_qt(schema, _UPDATE_FUNCTION)}();",
        f"CREATE TRIGGER {_q(_DELETE_TRIGGER)} "
        f"BEFORE DELETE ON {table_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {_qt(schema, _DELETE_FUNCTION)}();",
        f"CREATE TRIGGER {_q(_TRUNCATE_TRIGGER)} "
        f"BEFORE TRUNCATE ON {table_ref} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {_qt(schema, _DELETE_FUNCTION)}();",
    )
    for statement in trigger_statements:
        op.execute(statement)
    for trigger_name in (
        _UPDATE_TRIGGER,
        _DELETE_TRIGGER,
        _TRUNCATE_TRIGGER,
    ):
        op.execute(
            f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER " f"{_q(trigger_name)};"
        )


def upgrade() -> None:
    schema = _schema()
    _lock_capacity_ledger(schema)
    _replace_capacity_constraint(
        schema,
        expected_contracts=(_V1_CONTRACT, _V2_CONTRACT),
        replacement_contracts=(
            _V1_CONTRACT,
            _V2_CONTRACT,
            _V3_CONTRACT,
        ),
    )
    _create_table(schema)
    _create_guards(schema)


def downgrade() -> None:
    schema = _schema()
    _lock_capacity_ledger(schema)
    table_ref = _qt(schema, _TABLE)
    op.execute(f"LOCK TABLE {table_ref} IN ACCESS EXCLUSIVE MODE NOWAIT;")
    _assert_no_v3_consumption(schema)
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {table_ref}) THEN
                RAISE EXCEPTION
                    'provider_directory_profile_capacity_preflight_history_exists';
            END IF;
        END;
        $$;
        """
    )
    op.execute(f"DROP TRIGGER {_q(_TRUNCATE_TRIGGER)} ON {table_ref};")
    op.execute(f"DROP TRIGGER {_q(_DELETE_TRIGGER)} ON {table_ref};")
    op.execute(f"DROP TRIGGER {_q(_UPDATE_TRIGGER)} ON {table_ref};")
    op.execute(f"DROP FUNCTION {_qt(schema, _DELETE_FUNCTION)}();")
    op.execute(f"DROP FUNCTION {_qt(schema, _UPDATE_FUNCTION)}();")
    op.drop_index(_INDEX, table_name=_TABLE, schema=schema)
    op.drop_table(_TABLE, schema=schema)
    _replace_capacity_constraint(
        schema,
        expected_contracts=(
            _V1_CONTRACT,
            _V2_CONTRACT,
            _V3_CONTRACT,
        ),
        replacement_contracts=(_V1_CONTRACT, _V2_CONTRACT),
    )
