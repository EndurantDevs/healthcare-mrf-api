"""Persist restart-safe UHC formulary admission evidence.

Revision ID: 20260810040000_fhir_formulary_uhc_admission_receipt
Revises: 20260810030000_fhir_formulary_source_artifact
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa


revision = "20260810040000_fhir_formulary_uhc_admission_receipt"
down_revision = "20260810030000_fhir_formulary_source_artifact"
branch_labels = None
depends_on = None


_TABLE = "fhir_formulary_uhc_admission_receipt"
_GUARD = "guard_fhir_formulary_uhc_admission_receipt"
_TRIGGER = "fhir_formulary_uhc_admission_receipt_guard"


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
    observation_ref = _qualified(
        schema,
        "fhir_formulary_source_artifact_observation",
    )
    set_ref = _qualified(schema, "fhir_formulary_source_artifact_set")
    artifact_ref = _qualified(schema, "fhir_formulary_source_artifact")
    admission_ref = _qualified(schema, "fhir_formulary_twin_admission")
    artifact_set_hash_ref = _qualified(
        schema,
        "fhir_formulary_source_artifact_set_sha256",
    )
    return f"""
    CREATE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    DECLARE
        observed_set_sha256 text;
        expected_file_count integer;
        artifact_count bigint;
        verified_artifact_count bigint;
        cs_artifact_count bigint;
        ifp_artifact_count bigint;
        max_artifact_verified_at timestamptz;
        observed_artifact_set_sha256 text;
        expected_receipt_id text;
        admission_list_count bigint;
        admission_alias_count bigint;
        admission_medication_count bigint;
        admission_cutoff_at timestamptz;
        admission_admitted_at timestamptz;
    BEGIN
        IF TG_OP <> 'INSERT' THEN
            RAISE EXCEPTION 'fhir_formulary_uhc_admission_receipt_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF NEW.recorded_at IS DISTINCT FROM transaction_timestamp() THEN
            RAISE EXCEPTION 'fhir_formulary_uhc_admission_receipt_invalid'
                USING ERRCODE = '23514';
        END IF;
        SELECT observation.source_file_set_sha256,
               artifact_set.expected_file_count
          INTO observed_set_sha256, expected_file_count
          FROM {observation_ref} AS observation
          JOIN {set_ref} AS artifact_set
            ON artifact_set.source_id = observation.source_id
           AND artifact_set.source_file_set_sha256 =
               observation.source_file_set_sha256
         WHERE observation.source_id = NEW.source_id
           AND observation.source_observation_sha256 =
               NEW.source_observation_sha256
         FOR SHARE OF observation, artifact_set;
        SELECT admission.list_count,
               admission.alias_count,
               admission.medication_count,
               admission.cutoff_at,
               admission.admitted_at
          INTO admission_list_count,
               admission_alias_count,
               admission_medication_count,
               admission_cutoff_at,
               admission_admitted_at
          FROM {admission_ref} AS admission
         WHERE admission.source_id = NEW.source_id
           AND admission.candidate_dataset_id = NEW.candidate_dataset_id
         FOR SHARE OF admission;
        PERFORM 1
          FROM {artifact_ref} AS artifact
         WHERE artifact.source_id = NEW.source_id
           AND artifact.source_file_set_sha256 = NEW.source_file_set_sha256
         FOR SHARE OF artifact;
        SELECT count(*),
               count(*) FILTER (WHERE artifact.status = 'verified'),
               count(*) FILTER (WHERE artifact.family = 'cs'),
               count(*) FILTER (WHERE artifact.family = 'ifp'),
               max(artifact.verified_at)
          INTO artifact_count,
               verified_artifact_count,
               cs_artifact_count,
               ifp_artifact_count,
               max_artifact_verified_at
          FROM {artifact_ref} AS artifact
         WHERE artifact.source_id = NEW.source_id
           AND artifact.source_file_set_sha256 = NEW.source_file_set_sha256;
        IF observed_set_sha256 IS DISTINCT FROM NEW.source_file_set_sha256
           OR expected_file_count IS DISTINCT FROM 48
           OR artifact_count IS DISTINCT FROM 48
           OR verified_artifact_count IS DISTINCT FROM 48
           OR cs_artifact_count IS DISTINCT FROM 24
           OR ifp_artifact_count IS DISTINCT FROM 24
           OR admission_list_count IS DISTINCT FROM NEW.plan_count
           OR admission_alias_count IS DISTINCT FROM NEW.plan_count
           OR admission_medication_count IS DISTINCT FROM
              NEW.medication_membership_count
           OR max_artifact_verified_at > admission_cutoff_at
           OR NEW.max_last_updated_at > admission_cutoff_at
           OR NEW.recorded_at < admission_admitted_at THEN
            RAISE EXCEPTION 'fhir_formulary_uhc_admission_receipt_invalid'
                USING ERRCODE = '23514';
        END IF;
        observed_artifact_set_sha256 :=
            {artifact_set_hash_ref}(
                NEW.source_id,
                NEW.source_file_set_sha256
            );
        expected_receipt_id :=
            'ffur_' || pg_catalog.substr(
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(
                            NEW.source_id
                            || pg_catalog.chr(31)
                            || NEW.candidate_dataset_id
                            || pg_catalog.chr(31)
                            || NEW.source_observation_sha256
                            || pg_catalog.chr(31)
                            || NEW.source_file_set_sha256
                            || pg_catalog.chr(31)
                            || NEW.artifact_set_sha256
                            || pg_catalog.chr(31)
                            || NEW.spool_content_sha256,
                            'UTF8'
                        )
                    ),
                    'hex'
                ),
                1,
                48
            );
        IF observed_artifact_set_sha256 IS DISTINCT FROM
               NEW.artifact_set_sha256
           OR expected_receipt_id IS DISTINCT FROM NEW.receipt_id THEN
            RAISE EXCEPTION 'fhir_formulary_uhc_admission_receipt_invalid'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _guard_install_statements(schema: str) -> tuple[str, ...]:
    function_ref = _qualified(schema, _GUARD)
    table_ref = _qualified(schema, _TABLE)
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
        IF EXISTS (SELECT 1 FROM {table_ref} LIMIT 1) THEN
            RAISE EXCEPTION
                'fhir_formulary_uhc_admission_receipt_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $downgrade$;
    """


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        _TABLE,
        sa.Column("receipt_id", sa.String(length=53), nullable=False),
        sa.Column("source_id", sa.String(length=64), nullable=False),
        sa.Column(
            "source_observation_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "source_file_set_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "artifact_set_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "candidate_dataset_id",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "spool_content_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("raw_record_count", sa.BigInteger(), nullable=False),
        sa.Column("raw_plan_entry_count", sa.BigInteger(), nullable=False),
        sa.Column("plan_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "medication_membership_count",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column("duplicate_count", sa.BigInteger(), nullable=False),
        sa.Column("superseded_count", sa.BigInteger(), nullable=False),
        sa.Column(
            "max_last_updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
        sa.Column(
            "recorded_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("transaction_timestamp()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "receipt_id",
            name="fhir_formulary_uhc_admission_receipt_pkey",
        ),
        sa.UniqueConstraint(
            "candidate_dataset_id",
            name="fhir_formulary_uhc_admission_receipt_candidate_key",
        ),
        sa.ForeignKeyConstraint(
            ["source_id", "candidate_dataset_id"],
            [
                f"{schema}.fhir_formulary_twin_admission.source_id",
                f"{schema}.fhir_formulary_twin_admission.candidate_dataset_id",
            ],
            name="fhir_formulary_uhc_admission_receipt_admission_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id", "source_file_set_sha256"],
            [
                f"{schema}.fhir_formulary_source_artifact_set.source_id",
                f"{schema}.fhir_formulary_source_artifact_set."
                "source_file_set_sha256",
            ],
            name="fhir_formulary_uhc_admission_receipt_set_fkey",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_id", "source_observation_sha256"],
            [
                f"{schema}.fhir_formulary_source_artifact_observation."
                "source_id",
                f"{schema}.fhir_formulary_source_artifact_observation."
                "source_observation_sha256",
            ],
            name="fhir_formulary_uhc_admission_receipt_observation_fkey",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "receipt_id ~ '^ffur_[0-9a-f]{48}$' AND "
            "source_id = 'uhc-official-formulary-mrf' AND "
            "source_observation_sha256 ~ '^[0-9a-f]{64}$' AND "
            "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "artifact_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "spool_content_sha256 ~ '^[0-9a-f]{64}$' AND "
            "file_count = 48 AND raw_record_count > 0 AND "
            "raw_plan_entry_count > 0 AND plan_count > 0 AND "
            "medication_membership_count > 0 AND duplicate_count >= 0 AND "
            "superseded_count >= 0 AND isfinite(max_last_updated_at) AND "
            "max_last_updated_at >= TIMESTAMPTZ '2000-01-01 00:00:00+00' "
            "AND max_last_updated_at < "
            "TIMESTAMPTZ '2101-01-01 00:00:00+00'",
            name="fhir_formulary_uhc_admission_receipt_values_check",
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
