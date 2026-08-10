# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add immutable sealed-pair comparison and matched publication authority.

Revision ID: 20260810070000_provider_directory_uhc_flex_practitioner_twin_admission
Revises: 20260810060000_provider_directory_uhc_flex_practitioner_acquisition
"""

from __future__ import annotations

import os
import re

from alembic import op
import sqlalchemy as sa


revision = "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission"
down_revision = "20260810060000_provider_directory_uhc_flex_practitioner_acquisition"
branch_labels = None
depends_on = None


_ACQUISITION = "provider_directory_uhc_flex_practitioner_acquisition"
_ATTEMPT = "provider_directory_uhc_flex_practitioner_twin_attempt"
_ADMISSION = "provider_directory_uhc_flex_practitioner_twin_admission"
_ATTEMPT_INSERT_GUARD = "guard_pd_uhc_flex_practitioner_twin_attempt_insert"
_ATTEMPT_IMMUTABLE_GUARD = "guard_pd_uhc_flex_practitioner_twin_attempt_immutable"
_ADMISSION_INSERT_GUARD = "guard_pd_uhc_flex_practitioner_admission_insert"
_ADMISSION_IMMUTABLE_GUARD = "guard_pd_uhc_flex_practitioner_admission_immutable"
_ATTEMPT_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-twin-attempt.v1"
)
_ADMISSION_CONTRACT = (
    "healthporta.provider-directory.uhc-flex-practitioner-matched-admission.v1"
)
_INTENT_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-intent.v1"
)
_RUN_DOMAIN = (
    "healthporta.provider-directory.uhc-flex-practitioner-acquisition-run.v1"
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    schema = runtime_schema or legacy_schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("Provider Directory database schema is invalid")
    return schema


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified(schema: str, identifier: str) -> str:
    return f"{_quoted(schema)}.{_quoted(identifier)}"


def _create_attempt_table(schema: str) -> None:
    op.create_table(
        _ATTEMPT,
        sa.Column("attempt_id", sa.String(56), nullable=False),
        sa.Column("attempt_contract_id", sa.String(96), nullable=False),
        sa.Column("semantic_projection_as_of", sa.Date(), nullable=False),
        sa.Column("operation_key", sa.String(64), nullable=False),
        sa.Column("baseline_acquisition_id", sa.String(55), nullable=False),
        sa.Column("candidate_acquisition_id", sa.String(55), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(55), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("query_contract_id", sa.String(96), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("baseline_run_id", sa.String(55), nullable=False),
        sa.Column("candidate_run_id", sa.String(55), nullable=False),
        sa.Column("expected_npi_count", sa.BigInteger(), nullable=False),
        sa.Column("baseline_terminal_set_sha256", sa.String(64), nullable=False),
        sa.Column("candidate_terminal_set_sha256", sa.String(64), nullable=False),
        sa.Column("baseline_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("candidate_resource_count", sa.BigInteger(), nullable=False),
        sa.Column("matched", sa.Boolean(), nullable=False),
        sa.Column(
            "attempted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "attempt_id",
            name="pd_uhc_flex_practitioner_twin_attempt_pkey",
        ),
        sa.UniqueConstraint(
            "baseline_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_baseline_key",
        ),
        sa.UniqueConstraint(
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_candidate_key",
        ),
        sa.UniqueConstraint(
            "dataset_intent_id",
            name="pd_uhc_flex_practitioner_twin_intent_key",
        ),
        sa.UniqueConstraint(
            "baseline_acquisition_id",
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_twin_pair_key",
        ),
        sa.ForeignKeyConstraint(
            ["baseline_acquisition_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id"],
            name="pd_uhc_flex_practitioner_twin_baseline_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id"],
            name="pd_uhc_flex_practitioner_twin_candidate_fkey",
        ),
        sa.CheckConstraint(
            f"attempt_contract_id = '{_ATTEMPT_CONTRACT}' AND "
            "attempt_id ~ '^pdufpta_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "expected_npi_count > 0 AND baseline_resource_count >= 0 AND "
            "candidate_resource_count >= 0 AND "
            "baseline_terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "candidate_terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "matched = (baseline_terminal_set_sha256 = "
            "candidate_terminal_set_sha256 AND baseline_resource_count = "
            "candidate_resource_count)",
            name="pd_uhc_flex_practitioner_twin_attempt_check",
        ),
        schema=schema,
    )


def _create_admission_table(schema: str) -> None:
    op.create_table(
        _ADMISSION,
        sa.Column("admission_id", sa.String(56), nullable=False),
        sa.Column("admission_contract_id", sa.String(96), nullable=False),
        sa.Column("semantic_projection_as_of", sa.Date(), nullable=False),
        sa.Column("operation_key", sa.String(64), nullable=False),
        sa.Column("attempt_id", sa.String(56), nullable=False),
        sa.Column("baseline_acquisition_id", sa.String(55), nullable=False),
        sa.Column("candidate_acquisition_id", sa.String(55), nullable=False),
        sa.Column("cohort_id", sa.String(54), nullable=False),
        sa.Column("dataset_intent_id", sa.String(55), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("connector_id", sa.String(64), nullable=False),
        sa.Column("query_contract_id", sa.String(96), nullable=False),
        sa.Column("storage_contract_id", sa.String(96), nullable=False),
        sa.Column("baseline_run_id", sa.String(55), nullable=False),
        sa.Column("candidate_run_id", sa.String(55), nullable=False),
        sa.Column("expected_npi_count", sa.BigInteger(), nullable=False),
        sa.Column("terminal_set_sha256", sa.String(64), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("publication_authority", sa.Boolean(), nullable=False),
        sa.Column(
            "admitted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "admission_id",
            name="pd_uhc_flex_practitioner_twin_admission_pkey",
        ),
        sa.UniqueConstraint(
            "attempt_id",
            name="pd_uhc_flex_practitioner_admission_attempt_key",
        ),
        sa.UniqueConstraint(
            "candidate_acquisition_id",
            name="pd_uhc_flex_practitioner_admission_candidate_key",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            [f"{schema}.{_ATTEMPT}.attempt_id"],
            name="pd_uhc_flex_practitioner_admission_attempt_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["baseline_acquisition_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id"],
            name="pd_uhc_flex_practitioner_admission_baseline_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_acquisition_id"],
            [f"{schema}.{_ACQUISITION}.acquisition_id"],
            name="pd_uhc_flex_practitioner_admission_candidate_fkey",
        ),
        sa.CheckConstraint(
            f"admission_contract_id = '{_ADMISSION_CONTRACT}' AND "
            "admission_id ~ '^pdufpad_[0-9a-f]{48}$' AND "
            "operation_key ~ '^[0-9a-f]{64}$' AND "
            "semantic_projection_as_of BETWEEN DATE '0001-01-01' "
            "AND DATE '9999-12-31' AND "
            "baseline_acquisition_id <> candidate_acquisition_id AND "
            "baseline_run_id <> candidate_run_id AND "
            "expected_npi_count > 0 AND resource_count >= 0 AND "
            "terminal_set_sha256 ~ '^[0-9a-f]{64}$' AND "
            "publication_authority IS TRUE",
            name="pd_uhc_flex_practitioner_twin_admission_check",
        ),
        schema=schema,
    )


def _attempt_insert_guard_sql(schema: str) -> str:
    acquisition = _qualified(schema, _ACQUISITION)
    attempt = _qualified(schema, _ATTEMPT)
    guard = _qualified(schema, _ATTEMPT_INSERT_GUARD)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        baseline_root record; candidate_root record; existing_attempt record;
        conflict_count bigint; expected_intent_id text;
        expected_baseline_run_id text; expected_candidate_run_id text;
        expected_attempt_id text; expected_matched boolean;
    BEGIN
        IF NEW.baseline_acquisition_id = NEW.candidate_acquisition_id THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_roots_invalid'
                USING ERRCODE = '55000';
        END IF;
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            LEAST(NEW.baseline_acquisition_id, NEW.candidate_acquisition_id), 2701));
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            GREATEST(NEW.baseline_acquisition_id, NEW.candidate_acquisition_id), 2701));
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            NEW.dataset_intent_id, 2702));
        PERFORM 1 FROM {acquisition} AS root
         WHERE root.acquisition_id IN (
             NEW.baseline_acquisition_id, NEW.candidate_acquisition_id
         ) ORDER BY root.acquisition_id FOR SHARE;
        SELECT * INTO baseline_root FROM {acquisition}
         WHERE acquisition_id = NEW.baseline_acquisition_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_baseline_invalid'
                USING ERRCODE = '55000';
        END IF;
        SELECT * INTO candidate_root FROM {acquisition}
         WHERE acquisition_id = NEW.candidate_acquisition_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_candidate_invalid'
                USING ERRCODE = '55000';
        END IF;
        expected_intent_id := 'pdufdi_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_INTENT_DOMAIN}' || pg_catalog.chr(31)
                || baseline_root.cohort_id || pg_catalog.chr(31)
                || NEW.semantic_projection_as_of::text || pg_catalog.chr(31)
                || NEW.operation_key, 'UTF8'
            )), 'hex'), 1, 48);
        expected_baseline_run_id := 'pdufpr_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_RUN_DOMAIN}' || pg_catalog.chr(31)
                || expected_intent_id || pg_catalog.chr(31) || 'baseline',
                'UTF8'
            )), 'hex'), 1, 48);
        expected_candidate_run_id := 'pdufpr_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_RUN_DOMAIN}' || pg_catalog.chr(31)
                || expected_intent_id || pg_catalog.chr(31) || 'candidate',
                'UTF8'
            )), 'hex'), 1, 48);
        IF baseline_root.acquisition_role IS DISTINCT FROM 'baseline'
           OR candidate_root.acquisition_role IS DISTINCT FROM 'candidate'
           OR baseline_root.status IS DISTINCT FROM 'sealed'
           OR candidate_root.status IS DISTINCT FROM 'sealed'
           OR baseline_root.cohort_complete IS DISTINCT FROM TRUE
           OR candidate_root.cohort_complete IS DISTINCT FROM TRUE
           OR baseline_root.pending_count IS DISTINCT FROM 0
           OR candidate_root.pending_count IS DISTINCT FROM 0
           OR baseline_root.leased_count IS DISTINCT FROM 0
           OR candidate_root.leased_count IS DISTINCT FROM 0
           OR baseline_root.error_count IS DISTINCT FROM 0
           OR candidate_root.error_count IS DISTINCT FROM 0
           OR baseline_root.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR candidate_root.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR baseline_root.endpoint_complete IS DISTINCT FROM FALSE
           OR candidate_root.endpoint_complete IS DISTINCT FROM FALSE
           OR baseline_root.sealed_at IS NULL OR candidate_root.sealed_at IS NULL
           OR baseline_root.terminal_set_sha256 IS NULL
           OR candidate_root.terminal_set_sha256 IS NULL
           OR baseline_root.resource_count IS NULL
           OR candidate_root.resource_count IS NULL THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_roots_unsealed'
                USING ERRCODE = '55000';
        END IF;
        IF baseline_root.cohort_id IS DISTINCT FROM candidate_root.cohort_id
           OR baseline_root.dataset_intent_id IS DISTINCT FROM
              candidate_root.dataset_intent_id
           OR baseline_root.source_id IS DISTINCT FROM candidate_root.source_id
           OR baseline_root.connector_id IS DISTINCT FROM candidate_root.connector_id
           OR baseline_root.query_contract_id IS DISTINCT FROM
              candidate_root.query_contract_id
           OR baseline_root.storage_contract_id IS DISTINCT FROM
              candidate_root.storage_contract_id
           OR baseline_root.expected_npi_count IS DISTINCT FROM
              candidate_root.expected_npi_count
           OR baseline_root.acquisition_id = candidate_root.acquisition_id
           OR baseline_root.run_id = candidate_root.run_id
           OR baseline_root.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR baseline_root.run_id IS DISTINCT FROM expected_baseline_run_id
           OR candidate_root.run_id IS DISTINCT FROM expected_candidate_run_id THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_identity_invalid'
                USING ERRCODE = '55000';
        END IF;
        expected_matched := (
            baseline_root.terminal_set_sha256 = candidate_root.terminal_set_sha256
            AND baseline_root.resource_count = candidate_root.resource_count
        );
        expected_attempt_id := 'pdufpta_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_ATTEMPT_CONTRACT}' || pg_catalog.chr(31)
                || NEW.semantic_projection_as_of::text || pg_catalog.chr(31)
                || NEW.operation_key || pg_catalog.chr(31)
                || baseline_root.acquisition_id || pg_catalog.chr(31)
                || candidate_root.acquisition_id || pg_catalog.chr(31)
                || baseline_root.cohort_id || pg_catalog.chr(31)
                || baseline_root.dataset_intent_id || pg_catalog.chr(31)
                || baseline_root.source_id || pg_catalog.chr(31)
                || baseline_root.connector_id || pg_catalog.chr(31)
                || baseline_root.query_contract_id || pg_catalog.chr(31)
                || baseline_root.storage_contract_id || pg_catalog.chr(31)
                || baseline_root.run_id || pg_catalog.chr(31)
                || candidate_root.run_id || pg_catalog.chr(31)
                || baseline_root.expected_npi_count::text || pg_catalog.chr(31)
                || baseline_root.terminal_set_sha256 || pg_catalog.chr(31)
                || candidate_root.terminal_set_sha256 || pg_catalog.chr(31)
                || baseline_root.resource_count::text || pg_catalog.chr(31)
                || candidate_root.resource_count::text || pg_catalog.chr(31)
                || expected_matched::text, 'UTF8'
            )), 'hex'), 1, 48);
        IF NEW.attempt_id IS DISTINCT FROM expected_attempt_id
           OR NEW.attempt_contract_id IS DISTINCT FROM '{_ATTEMPT_CONTRACT}'
           OR NEW.cohort_id IS DISTINCT FROM baseline_root.cohort_id
           OR NEW.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR NEW.source_id IS DISTINCT FROM baseline_root.source_id
           OR NEW.connector_id IS DISTINCT FROM baseline_root.connector_id
           OR NEW.query_contract_id IS DISTINCT FROM baseline_root.query_contract_id
           OR NEW.storage_contract_id IS DISTINCT FROM
              baseline_root.storage_contract_id
           OR NEW.baseline_run_id IS DISTINCT FROM expected_baseline_run_id
           OR NEW.candidate_run_id IS DISTINCT FROM expected_candidate_run_id
           OR NEW.expected_npi_count IS DISTINCT FROM
              baseline_root.expected_npi_count
           OR NEW.baseline_terminal_set_sha256 IS DISTINCT FROM
              baseline_root.terminal_set_sha256
           OR NEW.candidate_terminal_set_sha256 IS DISTINCT FROM
              candidate_root.terminal_set_sha256
           OR NEW.baseline_resource_count IS DISTINCT FROM
              baseline_root.resource_count
           OR NEW.candidate_resource_count IS DISTINCT FROM
              candidate_root.resource_count
           OR NEW.matched IS DISTINCT FROM expected_matched THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_attempt_invalid'
                USING ERRCODE = '55000';
        END IF;
        SELECT count(*)::bigint INTO conflict_count FROM {attempt}
         WHERE attempt_id = NEW.attempt_id
            OR baseline_acquisition_id IN (
                NEW.baseline_acquisition_id, NEW.candidate_acquisition_id
            ) OR candidate_acquisition_id IN (
                NEW.baseline_acquisition_id, NEW.candidate_acquisition_id
            ) OR dataset_intent_id = NEW.dataset_intent_id;
        IF conflict_count > 0 THEN
            SELECT * INTO existing_attempt FROM {attempt}
             WHERE attempt_id = NEW.attempt_id
                OR baseline_acquisition_id IN (
                    NEW.baseline_acquisition_id, NEW.candidate_acquisition_id
                ) OR candidate_acquisition_id IN (
                    NEW.baseline_acquisition_id, NEW.candidate_acquisition_id
                ) OR dataset_intent_id = NEW.dataset_intent_id
             ORDER BY attempt_id LIMIT 1 FOR SHARE;
            IF conflict_count = 1
               AND ROW(existing_attempt.attempt_id,
                       existing_attempt.attempt_contract_id,
                       existing_attempt.semantic_projection_as_of,
                       existing_attempt.operation_key,
                       existing_attempt.baseline_acquisition_id,
                       existing_attempt.candidate_acquisition_id,
                       existing_attempt.cohort_id,
                       existing_attempt.dataset_intent_id,
                       existing_attempt.source_id,
                       existing_attempt.connector_id,
                       existing_attempt.query_contract_id,
                       existing_attempt.storage_contract_id,
                       existing_attempt.baseline_run_id,
                       existing_attempt.candidate_run_id,
                       existing_attempt.expected_npi_count,
                       existing_attempt.baseline_terminal_set_sha256,
                       existing_attempt.candidate_terminal_set_sha256,
                       existing_attempt.baseline_resource_count,
                       existing_attempt.candidate_resource_count,
                       existing_attempt.matched)
                   IS NOT DISTINCT FROM
                   ROW(NEW.attempt_id, NEW.attempt_contract_id,
                       NEW.semantic_projection_as_of, NEW.operation_key,
                       NEW.baseline_acquisition_id, NEW.candidate_acquisition_id,
                       NEW.cohort_id, NEW.dataset_intent_id, NEW.source_id,
                       NEW.connector_id, NEW.query_contract_id,
                       NEW.storage_contract_id, NEW.baseline_run_id,
                       NEW.candidate_run_id, NEW.expected_npi_count,
                       NEW.baseline_terminal_set_sha256,
                       NEW.candidate_terminal_set_sha256,
                       NEW.baseline_resource_count,
                       NEW.candidate_resource_count, NEW.matched) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'provider_directory_flex_twin_pair_consumed'
                USING ERRCODE = '55000';
        END IF;
        IF NEW.attempted_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.attempted_at < baseline_root.sealed_at
           OR NEW.attempted_at < candidate_root.sealed_at THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_attempt_time_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _admission_insert_guard_sql(schema: str) -> str:
    acquisition = _qualified(schema, _ACQUISITION)
    attempt = _qualified(schema, _ATTEMPT)
    admission = _qualified(schema, _ADMISSION)
    guard = _qualified(schema, _ADMISSION_INSERT_GUARD)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    DECLARE
        attempt_root record; baseline_root record; candidate_root record;
        existing_admission record; conflict_count bigint;
        expected_intent_id text; expected_baseline_run_id text;
        expected_candidate_run_id text; expected_admission_id text;
    BEGIN
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            NEW.attempt_id, 2703));
        SELECT * INTO attempt_root FROM {attempt}
         WHERE attempt_id = NEW.attempt_id FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_attempt_not_matched'
                USING ERRCODE = '55000';
        END IF;
        IF attempt_root.matched IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_attempt_not_matched'
                USING ERRCODE = '55000';
        END IF;
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            LEAST(attempt_root.baseline_acquisition_id,
                  attempt_root.candidate_acquisition_id), 2701));
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            GREATEST(attempt_root.baseline_acquisition_id,
                     attempt_root.candidate_acquisition_id), 2701));
        PERFORM 1 FROM {acquisition} AS root
         WHERE root.acquisition_id IN (
             attempt_root.baseline_acquisition_id,
             attempt_root.candidate_acquisition_id
         ) ORDER BY root.acquisition_id FOR SHARE;
        SELECT * INTO baseline_root FROM {acquisition}
         WHERE acquisition_id = attempt_root.baseline_acquisition_id;
        SELECT * INTO candidate_root FROM {acquisition}
         WHERE acquisition_id = attempt_root.candidate_acquisition_id;
        expected_intent_id := 'pdufdi_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_INTENT_DOMAIN}' || pg_catalog.chr(31)
                || attempt_root.cohort_id || pg_catalog.chr(31)
                || attempt_root.semantic_projection_as_of::text
                || pg_catalog.chr(31) || attempt_root.operation_key, 'UTF8'
            )), 'hex'), 1, 48);
        expected_baseline_run_id := 'pdufpr_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_RUN_DOMAIN}' || pg_catalog.chr(31)
                || expected_intent_id || pg_catalog.chr(31) || 'baseline',
                'UTF8'
            )), 'hex'), 1, 48);
        expected_candidate_run_id := 'pdufpr_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_RUN_DOMAIN}' || pg_catalog.chr(31)
                || expected_intent_id || pg_catalog.chr(31) || 'candidate',
                'UTF8'
            )), 'hex'), 1, 48);
        IF baseline_root.acquisition_role IS DISTINCT FROM 'baseline'
           OR candidate_root.acquisition_role IS DISTINCT FROM 'candidate'
           OR baseline_root.status IS DISTINCT FROM 'sealed'
           OR candidate_root.status IS DISTINCT FROM 'sealed'
           OR baseline_root.cohort_complete IS DISTINCT FROM TRUE
           OR candidate_root.cohort_complete IS DISTINCT FROM TRUE
           OR baseline_root.pending_count IS DISTINCT FROM 0
           OR candidate_root.pending_count IS DISTINCT FROM 0
           OR baseline_root.leased_count IS DISTINCT FROM 0
           OR candidate_root.leased_count IS DISTINCT FROM 0
           OR baseline_root.error_count IS DISTINCT FROM 0
           OR candidate_root.error_count IS DISTINCT FROM 0
           OR baseline_root.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR candidate_root.endpoint_collection_complete IS DISTINCT FROM FALSE
           OR baseline_root.endpoint_complete IS DISTINCT FROM FALSE
           OR candidate_root.endpoint_complete IS DISTINCT FROM FALSE
           OR baseline_root.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR candidate_root.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR baseline_root.run_id IS DISTINCT FROM expected_baseline_run_id
           OR candidate_root.run_id IS DISTINCT FROM expected_candidate_run_id
           OR baseline_root.terminal_set_sha256 IS DISTINCT FROM
              candidate_root.terminal_set_sha256
           OR baseline_root.resource_count IS DISTINCT FROM
              candidate_root.resource_count THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_authority_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF ROW(attempt_root.baseline_acquisition_id,
               attempt_root.candidate_acquisition_id, attempt_root.cohort_id,
               attempt_root.dataset_intent_id, attempt_root.source_id,
               attempt_root.connector_id, attempt_root.query_contract_id,
               attempt_root.storage_contract_id, attempt_root.baseline_run_id,
               attempt_root.candidate_run_id, attempt_root.expected_npi_count,
               attempt_root.baseline_terminal_set_sha256,
               attempt_root.candidate_terminal_set_sha256,
               attempt_root.baseline_resource_count,
               attempt_root.candidate_resource_count)
           IS DISTINCT FROM
           ROW(baseline_root.acquisition_id, candidate_root.acquisition_id,
               baseline_root.cohort_id, baseline_root.dataset_intent_id,
               baseline_root.source_id, baseline_root.connector_id,
               baseline_root.query_contract_id, baseline_root.storage_contract_id,
               baseline_root.run_id, candidate_root.run_id,
               baseline_root.expected_npi_count,
               baseline_root.terminal_set_sha256,
               candidate_root.terminal_set_sha256,
               baseline_root.resource_count, candidate_root.resource_count) THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_attempt_tampered'
                USING ERRCODE = '55000';
        END IF;
        expected_admission_id := 'pdufpad_' || pg_catalog.substr(
            pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                '{_ADMISSION_CONTRACT}' || pg_catalog.chr(31)
                || attempt_root.semantic_projection_as_of::text
                || pg_catalog.chr(31) || attempt_root.operation_key
                || pg_catalog.chr(31) || attempt_root.attempt_id
                || pg_catalog.chr(31) || attempt_root.baseline_acquisition_id
                || pg_catalog.chr(31) || attempt_root.candidate_acquisition_id
                || pg_catalog.chr(31) || attempt_root.cohort_id
                || pg_catalog.chr(31) || attempt_root.dataset_intent_id
                || pg_catalog.chr(31) || attempt_root.source_id
                || pg_catalog.chr(31) || attempt_root.connector_id
                || pg_catalog.chr(31) || attempt_root.query_contract_id
                || pg_catalog.chr(31) || attempt_root.storage_contract_id
                || pg_catalog.chr(31) || attempt_root.baseline_run_id
                || pg_catalog.chr(31) || attempt_root.candidate_run_id
                || pg_catalog.chr(31) || attempt_root.expected_npi_count::text
                || pg_catalog.chr(31)
                || attempt_root.candidate_terminal_set_sha256
                || pg_catalog.chr(31)
                || attempt_root.candidate_resource_count::text
                || pg_catalog.chr(31) || 'true', 'UTF8'
            )), 'hex'), 1, 48);
        IF NEW.admission_id IS DISTINCT FROM expected_admission_id
           OR NEW.admission_contract_id IS DISTINCT FROM '{_ADMISSION_CONTRACT}'
           OR NEW.semantic_projection_as_of IS DISTINCT FROM
              attempt_root.semantic_projection_as_of
           OR NEW.operation_key IS DISTINCT FROM attempt_root.operation_key
           OR NEW.baseline_acquisition_id IS DISTINCT FROM
              attempt_root.baseline_acquisition_id
           OR NEW.candidate_acquisition_id IS DISTINCT FROM
              attempt_root.candidate_acquisition_id
           OR NEW.cohort_id IS DISTINCT FROM attempt_root.cohort_id
           OR NEW.dataset_intent_id IS DISTINCT FROM expected_intent_id
           OR NEW.source_id IS DISTINCT FROM attempt_root.source_id
           OR NEW.connector_id IS DISTINCT FROM attempt_root.connector_id
           OR NEW.query_contract_id IS DISTINCT FROM
              attempt_root.query_contract_id
           OR NEW.storage_contract_id IS DISTINCT FROM
              attempt_root.storage_contract_id
           OR NEW.baseline_run_id IS DISTINCT FROM expected_baseline_run_id
           OR NEW.candidate_run_id IS DISTINCT FROM expected_candidate_run_id
           OR NEW.expected_npi_count IS DISTINCT FROM
              attempt_root.expected_npi_count
           OR NEW.terminal_set_sha256 IS DISTINCT FROM
              attempt_root.candidate_terminal_set_sha256
           OR NEW.resource_count IS DISTINCT FROM
              attempt_root.candidate_resource_count
           OR NEW.publication_authority IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_admission_invalid'
                USING ERRCODE = '55000';
        END IF;
        SELECT count(*)::bigint INTO conflict_count FROM {admission}
         WHERE admission_id = NEW.admission_id OR attempt_id = NEW.attempt_id
            OR candidate_acquisition_id = NEW.candidate_acquisition_id;
        IF conflict_count > 0 THEN
            SELECT * INTO existing_admission FROM {admission}
             WHERE admission_id = NEW.admission_id OR attempt_id = NEW.attempt_id
                OR candidate_acquisition_id = NEW.candidate_acquisition_id
             ORDER BY admission_id LIMIT 1 FOR SHARE;
            IF conflict_count = 1
               AND ROW(existing_admission.admission_id,
                       existing_admission.admission_contract_id,
                       existing_admission.semantic_projection_as_of,
                       existing_admission.operation_key,
                       existing_admission.attempt_id,
                       existing_admission.baseline_acquisition_id,
                       existing_admission.candidate_acquisition_id,
                       existing_admission.cohort_id,
                       existing_admission.dataset_intent_id,
                       existing_admission.source_id,
                       existing_admission.connector_id,
                       existing_admission.query_contract_id,
                       existing_admission.storage_contract_id,
                       existing_admission.baseline_run_id,
                       existing_admission.candidate_run_id,
                       existing_admission.expected_npi_count,
                       existing_admission.terminal_set_sha256,
                       existing_admission.resource_count,
                       existing_admission.publication_authority)
                   IS NOT DISTINCT FROM
                   ROW(NEW.admission_id, NEW.admission_contract_id,
                       NEW.semantic_projection_as_of, NEW.operation_key,
                       NEW.attempt_id, NEW.baseline_acquisition_id,
                       NEW.candidate_acquisition_id, NEW.cohort_id,
                       NEW.dataset_intent_id, NEW.source_id, NEW.connector_id,
                       NEW.query_contract_id, NEW.storage_contract_id,
                       NEW.baseline_run_id, NEW.candidate_run_id,
                       NEW.expected_npi_count, NEW.terminal_set_sha256,
                       NEW.resource_count, NEW.publication_authority) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'provider_directory_flex_twin_authority_consumed'
                USING ERRCODE = '55000';
        END IF;
        IF NEW.admitted_at IS DISTINCT FROM transaction_timestamp()
           OR NEW.admitted_at < attempt_root.attempted_at THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_admission_time_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $guard$;
    """


def _immutable_guard_sql(schema: str, function_name: str, message: str) -> str:
    guard = _qualified(schema, function_name)
    return f"""
    CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog AS $guard$
    BEGIN
        RAISE EXCEPTION '{message}' USING ERRCODE = '55000';
    END;
    $guard$;
    """


def _install_guards(schema: str) -> None:
    attempt = _qualified(schema, _ATTEMPT)
    admission = _qualified(schema, _ADMISSION)
    attempt_insert = _qualified(schema, _ATTEMPT_INSERT_GUARD)
    attempt_immutable = _qualified(schema, _ATTEMPT_IMMUTABLE_GUARD)
    admission_insert = _qualified(schema, _ADMISSION_INSERT_GUARD)
    admission_immutable = _qualified(schema, _ADMISSION_IMMUTABLE_GUARD)
    statements = (
        *(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;" for guard in (
            attempt_insert,
            attempt_immutable,
            admission_insert,
            admission_immutable,
        )),
        f"REVOKE ALL ON TABLE {attempt} FROM PUBLIC;",
        f"REVOKE ALL ON TABLE {admission} FROM PUBLIC;",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_twin_attempt_insert "
        f"BEFORE INSERT ON {attempt} FOR EACH ROW "
        f"EXECUTE FUNCTION {attempt_insert}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_twin_attempt_immutable "
        f"BEFORE UPDATE OR DELETE ON {attempt} FOR EACH ROW "
        f"EXECUTE FUNCTION {attempt_immutable}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_twin_attempt_truncate "
        f"BEFORE TRUNCATE ON {attempt} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {attempt_immutable}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_admission_insert "
        f"BEFORE INSERT ON {admission} FOR EACH ROW "
        f"EXECUTE FUNCTION {admission_insert}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_admission_immutable "
        f"BEFORE UPDATE OR DELETE ON {admission} FOR EACH ROW "
        f"EXECUTE FUNCTION {admission_immutable}();",
        f"CREATE TRIGGER pd_uhc_flex_practitioner_admission_truncate "
        f"BEFORE TRUNCATE ON {admission} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION {admission_immutable}();",
        *(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_quoted(trigger)};"
          for table, trigger in (
              (attempt, "pd_uhc_flex_practitioner_twin_attempt_insert"),
              (attempt, "pd_uhc_flex_practitioner_twin_attempt_immutable"),
              (attempt, "pd_uhc_flex_practitioner_twin_attempt_truncate"),
              (admission, "pd_uhc_flex_practitioner_admission_insert"),
              (admission, "pd_uhc_flex_practitioner_admission_immutable"),
              (admission, "pd_uhc_flex_practitioner_admission_truncate"),
          )),
    )
    for statement in statements:
        op.execute(statement)


def _downgrade_lock_sql(schema: str) -> str:
    return "LOCK TABLE " + ", ".join(
        _qualified(schema, table_name) for table_name in (_ADMISSION, _ATTEMPT)
    ) + " IN ACCESS EXCLUSIVE MODE;"


def _downgrade_fence_sql(schema: str) -> str:
    return f"""
    DO $downgrade$ BEGIN
        IF EXISTS (SELECT 1 FROM {_qualified(schema, _ATTEMPT)} LIMIT 1)
           OR EXISTS (SELECT 1 FROM {_qualified(schema, _ADMISSION)} LIMIT 1)
        THEN
            RAISE EXCEPTION 'provider_directory_flex_twin_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END; $downgrade$;
    """


def upgrade() -> None:
    schema = _schema()
    _create_attempt_table(schema)
    _create_admission_table(schema)
    op.execute(_attempt_insert_guard_sql(schema))
    op.execute(
        _immutable_guard_sql(
            schema,
            _ATTEMPT_IMMUTABLE_GUARD,
            "provider_directory_flex_twin_attempt_immutable",
        )
    )
    op.execute(_admission_insert_guard_sql(schema))
    op.execute(
        _immutable_guard_sql(
            schema,
            _ADMISSION_IMMUTABLE_GUARD,
            "provider_directory_flex_twin_admission_immutable",
        )
    )
    _install_guards(schema)


def downgrade() -> None:
    schema = _schema()
    op.execute(_downgrade_lock_sql(schema))
    op.execute(_downgrade_fence_sql(schema))
    op.drop_table(_ADMISSION, schema=schema)
    op.drop_table(_ATTEMPT, schema=schema)
    for function_name in (
        _ADMISSION_IMMUTABLE_GUARD,
        _ADMISSION_INSERT_GUARD,
        _ATTEMPT_IMMUTABLE_GUARD,
        _ATTEMPT_INSERT_GUARD,
    ):
        op.execute(f"DROP FUNCTION {_qualified(schema, function_name)}();")
