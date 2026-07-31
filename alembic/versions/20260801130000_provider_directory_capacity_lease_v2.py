"""Admit runtime-bound Provider Directory capacity leases.

Revision ID: 20260801130000_provider_directory_capacity_lease_v2
Revises: 20260801010000_uhc_semantic_layout_identity
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260801130000_provider_directory_capacity_lease_v2"
down_revision = "20260801010000_uhc_semantic_layout_identity"
branch_labels = None
depends_on = None

_TABLE = "provider_directory_profile_capacity_lease_consumption"
_CONSTRAINT = "pd_profile_capacity_consumption_values_check"
_PROBE_CONSTRAINT = "pd_profile_capacity_consumption_values_probe"
_NEXT_CONSTRAINT = "pd_profile_capacity_consumption_values_next"
_V1_CONTRACT = "provider-directory-database-capacity-lease-v1"
_V2_CONTRACT = "provider-directory-database-capacity-lease-v2"


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


def _contract_predicate(*contract_ids: str) -> str:
    if len(contract_ids) == 1:
        return f"contract_id = '{contract_ids[0]}'"
    quoted_contracts = ", ".join(f"'{value}'" for value in contract_ids)
    return f"contract_id IN ({quoted_contracts})"


def _consumption_check(contract_predicate: str) -> str:
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


def _add_not_valid_constraint(
    schema: str,
    constraint_name: str,
    condition: str,
) -> None:
    op.execute(
        f"ALTER TABLE {_qt(schema, _TABLE)} "
        f"ADD CONSTRAINT {_q(constraint_name)} "
        f"CHECK ({condition}) NOT VALID;"
    )


def _assert_exact_live_constraint(schema: str, condition: str) -> None:
    _add_not_valid_constraint(schema, _PROBE_CONSTRAINT, condition)
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
               AND relation.relname = {_literal(_TABLE)}
               AND constraint_row.conname = {_literal(_CONSTRAINT)};
            SELECT constraint_row.* INTO STRICT probe_row
              FROM pg_constraint AS constraint_row
             WHERE constraint_row.conrelid = live_row.conrelid
               AND constraint_row.conname = {_literal(_PROBE_CONSTRAINT)};
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
        f"ALTER TABLE {_qt(schema, _TABLE)} "
        f"DROP CONSTRAINT {_q(_PROBE_CONSTRAINT)};"
    )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replace_constraint(
    schema: str,
    *,
    expected_condition: str,
    replacement_condition: str,
) -> None:
    _assert_exact_live_constraint(schema, expected_condition)
    _add_not_valid_constraint(
        schema,
        _NEXT_CONSTRAINT,
        replacement_condition,
    )
    table_ref = _qt(schema, _TABLE)
    op.execute(
        f"ALTER TABLE {table_ref} "
        f"VALIDATE CONSTRAINT {_q(_NEXT_CONSTRAINT)};"
    )
    op.execute(
        f"ALTER TABLE {table_ref} DROP CONSTRAINT {_q(_CONSTRAINT)};"
    )
    op.execute(
        f"ALTER TABLE {table_ref} "
        f"RENAME CONSTRAINT {_q(_NEXT_CONSTRAINT)} TO {_q(_CONSTRAINT)};"
    )
    _assert_exact_live_constraint(schema, replacement_condition)


def _assert_no_v2_consumption(schema: str) -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {_qt(schema, _TABLE)}
                 WHERE contract_id = {_literal(_V2_CONTRACT)}
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_capacity_lease_v2_history_exists';
            END IF;
        END;
        $$;
        """
    )


def _lock_capacity_ledger(schema: str) -> None:
    op.execute(
        f"LOCK TABLE {_qt(schema, _TABLE)} "
        "IN ACCESS EXCLUSIVE MODE NOWAIT;"
    )


def upgrade() -> None:
    """Allow retained v1 history while current application admits only v2."""

    schema = _schema()
    _lock_capacity_ledger(schema)
    _replace_constraint(
        schema,
        expected_condition=_consumption_check(
            _contract_predicate(_V1_CONTRACT)
        ),
        replacement_condition=_consumption_check(
            _contract_predicate(_V1_CONTRACT, _V2_CONTRACT)
        ),
    )


def downgrade() -> None:
    """Restore v1-only storage when no consumed v2 history exists."""

    schema = _schema()
    _lock_capacity_ledger(schema)
    _assert_no_v2_consumption(schema)
    _replace_constraint(
        schema,
        expected_condition=_consumption_check(
            _contract_predicate(_V1_CONTRACT, _V2_CONTRACT)
        ),
        replacement_condition=_consumption_check(
            _contract_predicate(_V1_CONTRACT)
        ),
    )
