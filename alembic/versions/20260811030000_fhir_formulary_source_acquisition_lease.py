# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Add a reusable fenced lease for formulary source acquisition.

Revision ID: 20260811030000_fhir_formulary_source_acquisition_lease
Revises: 20260811020000_provider_directory_rooted_graph_acquisition
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa


revision = "20260811030000_fhir_formulary_source_acquisition_lease"
down_revision = "20260811020000_provider_directory_rooted_graph_acquisition"
branch_labels = None
depends_on = None


_TABLE = "fhir_formulary_source_acquisition_lease"
_GUARD = "guard_fhir_formulary_source_acquisition_lease"
_TRIGGER = "fhir_formulary_source_acquisition_lease_guard"
_ACTION_SETTING = "healthporta.formulary_source_acquisition_action"
_SOURCE_SETTING = "healthporta.formulary_source_acquisition_source"
_GENERATION_SETTING = "healthporta.formulary_source_acquisition_generation"
_TOKEN_SETTING = "healthporta.formulary_source_acquisition_token"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime_schema or legacy_schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("FHIR formulary database schema is invalid")
    return schema


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, identifier: str) -> str:
    return f"{_quoted(schema)}.{_quoted(identifier)}"


def _guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _GUARD)
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    DECLARE
        action_name text := pg_catalog.current_setting(
            '{_ACTION_SETTING}', true
        );
        action_source text := pg_catalog.current_setting(
            '{_SOURCE_SETTING}', true
        );
        action_generation bigint := NULLIF(
            pg_catalog.current_setting('{_GENERATION_SETTING}', true), ''
        )::bigint;
        action_token text := pg_catalog.current_setting(
            '{_TOKEN_SETTING}', true
        );
    BEGIN
        IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
            RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
                USING ERRCODE = '55000';
        END IF;

        IF TG_OP = 'INSERT' THEN
            IF action_name IS DISTINCT FROM 'claim'
               OR action_source IS DISTINCT FROM NEW.source_id
               OR action_token IS NULL
               OR action_token !~ '^[0-9a-f]{{64}}$'
               OR NEW.lease_generation <> 0
               OR NEW.lease_token IS NOT NULL
               OR NEW.lease_expires_at IS NOT NULL
               OR NEW.lease_heartbeat_at IS NOT NULL
               OR NEW.claimed_at IS NOT NULL
               OR NEW.created_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF action_name = 'claim' THEN
            IF action_source IS DISTINCT FROM OLD.source_id
               OR action_token IS NULL
               OR action_token !~ '^[0-9a-f]{{64}}$'
               OR NOT (
                    OLD.lease_token IS NULL
                    OR OLD.lease_expires_at <= clock_timestamp()
               )
               OR NEW.source_id IS DISTINCT FROM OLD.source_id
               OR NEW.lease_generation <> OLD.lease_generation + 1
               OR NEW.lease_token IS DISTINCT FROM action_token
               OR NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token
               OR NEW.lease_expires_at <= clock_timestamp()
               OR NEW.lease_expires_at >
                    NEW.lease_heartbeat_at + INTERVAL '1 hour'
               OR NEW.lease_heartbeat_at IS DISTINCT FROM
                    transaction_timestamp()
               OR NEW.claimed_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF action_name = 'heartbeat' THEN
            IF action_source IS DISTINCT FROM OLD.source_id
               OR action_generation IS DISTINCT FROM OLD.lease_generation
               OR action_token IS DISTINCT FROM OLD.lease_token
               OR OLD.lease_token IS NULL
               OR OLD.lease_expires_at <= clock_timestamp()
               OR NEW.source_id IS DISTINCT FROM OLD.source_id
               OR NEW.lease_generation IS DISTINCT FROM OLD.lease_generation
               OR NEW.lease_token IS DISTINCT FROM OLD.lease_token
               OR NEW.lease_expires_at <= OLD.lease_expires_at
               OR NEW.lease_expires_at >
                    NEW.lease_heartbeat_at + INTERVAL '1 hour'
               OR NEW.lease_heartbeat_at IS DISTINCT FROM
                    transaction_timestamp()
               OR NEW.claimed_at IS DISTINCT FROM OLD.claimed_at
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF action_name = 'release' THEN
            IF action_source IS DISTINCT FROM OLD.source_id
               OR action_generation IS DISTINCT FROM OLD.lease_generation
               OR action_token IS DISTINCT FROM OLD.lease_token
               OR OLD.lease_token IS NULL
               OR OLD.lease_expires_at <= clock_timestamp()
               OR NEW.source_id IS DISTINCT FROM OLD.source_id
               OR NEW.lease_generation IS DISTINCT FROM OLD.lease_generation
               OR NEW.lease_token IS NOT NULL
               OR NEW.lease_expires_at IS NOT NULL
               OR NEW.lease_heartbeat_at IS NOT NULL
               OR NEW.claimed_at IS NOT NULL
               OR NEW.created_at IS DISTINCT FROM OLD.created_at
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        RAISE EXCEPTION 'fhir_formulary_source_acquisition_lease_guard'
            USING ERRCODE = '55000';
    END;
    $guard$;
    """


def _guard_install_statements(schema: str) -> tuple[str, ...]:
    table_ref = _qualified(schema, _TABLE)
    function_ref = _qualified(schema, _GUARD)
    return (
        f"REVOKE ALL ON FUNCTION {function_ref}() FROM PUBLIC;",
        f"REVOKE ALL ON TABLE {table_ref} FROM PUBLIC;",
        f"CREATE TRIGGER {_quoted(_TRIGGER)} BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {table_ref} FOR EACH ROW EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER {_quoted(_TRIGGER)};",
        f"CREATE TRIGGER {_quoted(_TRIGGER + '_truncate')} BEFORE TRUNCATE "
        f"ON {table_ref} FOR EACH STATEMENT EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(_TRIGGER + '_truncate')};",
    )


def _downgrade_fence_sql(schema: str) -> str:
    table_ref = _qualified(schema, _TABLE)
    return f"""
    DO $downgrade$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {table_ref}
             WHERE lease_token IS NOT NULL
               AND lease_expires_at > clock_timestamp()
             LIMIT 1
        ) THEN
            RAISE EXCEPTION
                'fhir_formulary_source_acquisition_lease_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $downgrade$;
    """


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        _TABLE,
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column(
            "lease_generation",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("lease_token", sa.String(length=64)),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("claimed_at", sa.TIMESTAMP(timezone=True)),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            [f"{schema}.fhir_formulary_source.source_id"],
            name="fhir_formulary_source_acquisition_lease_source_fkey",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "source_id",
            name="fhir_formulary_source_acquisition_lease_pkey",
        ),
        sa.CheckConstraint(
            "lease_generation >= 0 AND ((lease_token IS NULL AND "
            "lease_expires_at IS NULL AND lease_heartbeat_at IS NULL AND "
            "claimed_at IS NULL) OR (lease_generation > 0 AND "
            "lease_token ~ '^[0-9a-f]{64}$' AND lease_expires_at IS NOT NULL "
            "AND lease_heartbeat_at IS NOT NULL AND claimed_at IS NOT NULL "
            "AND lease_expires_at > lease_heartbeat_at AND "
            "lease_expires_at <= lease_heartbeat_at + INTERVAL '1 hour' AND "
            "lease_heartbeat_at >= claimed_at))",
            name="fhir_formulary_source_acquisition_lease_state_check",
        ),
        schema=schema,
    )
    op.execute(_guard_function_sql(schema))
    for statement in _guard_install_statements(schema):
        op.execute(statement)


def downgrade() -> None:
    schema = _schema()
    table_ref = _qualified(schema, _TABLE)
    op.execute(f"LOCK TABLE {table_ref} IN ACCESS EXCLUSIVE MODE;")
    op.execute(_downgrade_fence_sql(schema))
    op.drop_table(_TABLE, schema=schema)
    op.execute(f"DROP FUNCTION {_qualified(schema, _GUARD)}();")
