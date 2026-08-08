"""Add admitted FHIR formulary twin evidence.

Revision ID: 20260808120000_fhir_formulary_twin_admission
Revises: 20260808110000_fhir_formulary_twin_attempt
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808120000_fhir_formulary_twin_admission"
down_revision = "20260808110000_fhir_formulary_twin_attempt"
branch_labels = None
depends_on = None

_ADMISSION = "fhir_formulary_twin_admission"
_ATTEMPT = "fhir_formulary_twin_attempt"
_ADMISSION_INSERT_GUARD = "guard_fhir_formulary_twin_admission_insert"
_ADMISSION_IMMUTABLE_GUARD = "guard_fhir_formulary_twin_admission_immutable"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table(schema: str, table_name: str) -> str:
    return f"{_quote(schema)}.{_quote(table_name)}"


def upgrade() -> None:
    """Install admitted-pair evidence and its exact insert guards."""

    schema = _schema()
    admission = _table(schema, _ADMISSION)
    attempt = _table(schema, _ATTEMPT)
    dataset = _table(schema, "fhir_formulary_dataset")
    op.execute(
        f"""
        CREATE TABLE {admission} (
            source_id varchar(64) NOT NULL,
            baseline_dataset_id varchar(64) NOT NULL,
            baseline_run_id varchar(64) NOT NULL,
            candidate_dataset_id varchar(64) NOT NULL,
            candidate_run_id varchar(64) NOT NULL,
            predecessor_dataset_id varchar(64),
            cutoff_at timestamptz NOT NULL,
            source_configuration_hash varchar(64) NOT NULL,
            acquisition_contract_hash varchar(64) NOT NULL,
            list_count integer NOT NULL,
            alias_count integer NOT NULL,
            medication_count bigint NOT NULL,
            coverage_hash varchar(64) NOT NULL,
            membership_hash varchar(64) NOT NULL,
            alternative_count bigint NOT NULL,
            alternative_hash varchar(64) NOT NULL,
            baseline_verified_at timestamptz NOT NULL,
            candidate_verified_at timestamptz NOT NULL,
            admitted_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_twin_admission_pkey
                PRIMARY KEY (source_id, candidate_dataset_id),
            CONSTRAINT fhir_formulary_twin_admission_candidate_key UNIQUE (candidate_dataset_id),
            CONSTRAINT fhir_formulary_twin_admission_baseline_key UNIQUE (baseline_dataset_id),
            CONSTRAINT fhir_formulary_twin_admission_attempt_fkey FOREIGN KEY (
                source_id, baseline_dataset_id, baseline_run_id,
                candidate_dataset_id, candidate_run_id
            ) REFERENCES {attempt}(
                source_id, baseline_dataset_id, baseline_run_id,
                candidate_dataset_id, candidate_run_id
            ),
            CONSTRAINT fhir_formulary_twin_admission_baseline_fkey
                FOREIGN KEY (source_id, baseline_dataset_id, baseline_run_id)
                REFERENCES {dataset}(source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_twin_admission_candidate_fkey
                FOREIGN KEY (source_id, candidate_dataset_id, candidate_run_id)
                REFERENCES {dataset}(source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_twin_admission_predecessor_fkey
                FOREIGN KEY (source_id, predecessor_dataset_id)
                REFERENCES {dataset}(source_id, dataset_id),
            CONSTRAINT fhir_formulary_twin_admission_identity_check CHECK (
                baseline_dataset_id <> candidate_dataset_id AND baseline_run_id <> candidate_run_id
            ),
            CONSTRAINT fhir_formulary_twin_admission_proof_check CHECK (
                list_count > 0 AND alias_count > 0 AND medication_count > 0
                AND alternative_count >= 0
                AND source_configuration_hash ~ '^[0-9a-f]{{64}}$'
                AND acquisition_contract_hash ~ '^[0-9a-f]{{64}}$'
                AND coverage_hash ~ '^[0-9a-f]{{64}}$'
                AND membership_hash ~ '^[0-9a-f]{{64}}$'
                AND alternative_hash ~ '^[0-9a-f]{{64}}$'
            ),
            CONSTRAINT fhir_formulary_twin_admission_time_check CHECK (
                baseline_verified_at <= candidate_verified_at
                AND candidate_verified_at <= admitted_at
            )
        );
        """
    )
    _install_admission_guards(schema)


def _install_admission_guards(schema: str) -> None:
    admission = _table(schema, _ADMISSION)
    attempt = _table(schema, _ATTEMPT)
    dataset = _table(schema, "fhir_formulary_dataset")
    current = _table(schema, "fhir_formulary_current")
    insert_guard = _table(schema, _ADMISSION_INSERT_GUARD)
    immutable_guard = _table(schema, _ADMISSION_IMMUTABLE_GUARD)
    op.execute(
        f"""
        CREATE FUNCTION {insert_guard}()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
            attempt_row record;
            baseline_row record;
            candidate_row record;
            current_dataset_id varchar(64);
            current_exists boolean;
        BEGIN
            SELECT * INTO attempt_row FROM {attempt}
             WHERE source_id = NEW.source_id AND baseline_dataset_id = NEW.baseline_dataset_id
               AND baseline_run_id = NEW.baseline_run_id
               AND candidate_dataset_id = NEW.candidate_dataset_id
               AND candidate_run_id = NEW.candidate_run_id FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'fhir_formulary_twin_attempt_not_matched' USING ERRCODE = '55000';
            END IF;
            IF attempt_row.matched IS DISTINCT FROM true
               OR attempt_row.cutoff_at IS DISTINCT FROM NEW.cutoff_at
               OR attempt_row.source_configuration_hash IS DISTINCT FROM NEW.source_configuration_hash
               OR attempt_row.acquisition_contract_hash IS DISTINCT FROM NEW.acquisition_contract_hash
               OR attempt_row.attempted_at > NEW.admitted_at THEN
                RAISE EXCEPTION 'fhir_formulary_twin_attempt_not_matched' USING ERRCODE = '55000';
            END IF;
            SELECT * INTO baseline_row FROM {dataset}
             WHERE source_id = NEW.source_id AND dataset_id = NEW.baseline_dataset_id
               AND run_id = NEW.baseline_run_id FOR SHARE;
            SELECT * INTO candidate_row FROM {dataset}
             WHERE source_id = NEW.source_id AND dataset_id = NEW.candidate_dataset_id
               AND run_id = NEW.candidate_run_id FOR SHARE;
            IF baseline_row.status IS DISTINCT FROM 'verified'
               OR baseline_row.publish_requested IS DISTINCT FROM false
               OR baseline_row.seed_eligible IS DISTINCT FROM false
               OR candidate_row.status IS DISTINCT FROM 'verified'
               OR candidate_row.publish_requested IS DISTINCT FROM true
               OR candidate_row.seed_eligible IS DISTINCT FROM false
               OR baseline_row.failed_at IS NOT NULL OR candidate_row.failed_at IS NOT NULL
               OR baseline_row.error_json IS NOT NULL OR candidate_row.error_json IS NOT NULL
               OR baseline_row.published_at IS NOT NULL OR candidate_row.published_at IS NOT NULL THEN
                RAISE EXCEPTION 'fhir_formulary_twin_roles_invalid' USING ERRCODE = '55000';
            END IF;
            IF baseline_row.previous_dataset_id IS DISTINCT FROM NEW.predecessor_dataset_id
               OR candidate_row.previous_dataset_id IS DISTINCT FROM NEW.predecessor_dataset_id
               OR baseline_row.cutoff_at IS DISTINCT FROM NEW.cutoff_at
               OR candidate_row.cutoff_at IS DISTINCT FROM NEW.cutoff_at
               OR baseline_row.verified_at IS DISTINCT FROM NEW.baseline_verified_at
               OR candidate_row.verified_at IS DISTINCT FROM NEW.candidate_verified_at
               OR baseline_row.summary_json ->> 'acquisition_contract_hash'
                    IS DISTINCT FROM NEW.acquisition_contract_hash
               OR candidate_row.summary_json ->> 'acquisition_contract_hash'
                    IS DISTINCT FROM NEW.acquisition_contract_hash
               OR baseline_row.list_count IS DISTINCT FROM NEW.list_count
               OR candidate_row.list_count IS DISTINCT FROM NEW.list_count
               OR baseline_row.alias_count IS DISTINCT FROM NEW.alias_count
               OR candidate_row.alias_count IS DISTINCT FROM NEW.alias_count
               OR baseline_row.medication_count IS DISTINCT FROM NEW.medication_count
               OR candidate_row.medication_count IS DISTINCT FROM NEW.medication_count
               OR baseline_row.coverage_hash IS DISTINCT FROM NEW.coverage_hash
               OR candidate_row.coverage_hash IS DISTINCT FROM NEW.coverage_hash
               OR baseline_row.membership_hash IS DISTINCT FROM NEW.membership_hash
               OR candidate_row.membership_hash IS DISTINCT FROM NEW.membership_hash THEN
                RAISE EXCEPTION 'fhir_formulary_twin_proof_mismatch' USING ERRCODE = '55000';
            END IF;
            SELECT dataset_id INTO current_dataset_id FROM {current}
             WHERE source_id = NEW.source_id FOR SHARE;
            current_exists := FOUND;
            IF (NEW.predecessor_dataset_id IS NULL AND current_exists)
               OR (NEW.predecessor_dataset_id IS NOT NULL AND (
                    NOT current_exists OR current_dataset_id <> NEW.predecessor_dataset_id
               )) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_predecessor_stale' USING ERRCODE = '40001';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(
        f"""CREATE FUNCTION {immutable_guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ BEGIN
            RAISE EXCEPTION 'fhir_formulary_twin_admission_immutable' USING ERRCODE = '55000';
        END; $function$;"""
    )
    for guard in (insert_guard, immutable_guard):
        op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_admission_insert_guard BEFORE INSERT ON {admission} "
        f"FOR EACH ROW EXECUTE FUNCTION {insert_guard}();"
    )
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_admission_immutable_guard "
        f"BEFORE UPDATE OR DELETE ON {admission} FOR EACH ROW EXECUTE FUNCTION {immutable_guard}();"
    )
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_admission_truncate_guard BEFORE TRUNCATE ON {admission} "
        f"FOR EACH STATEMENT EXECUTE FUNCTION {immutable_guard}();"
    )


def downgrade() -> None:
    """Remove empty admission evidence while retaining attempts."""
    schema = _schema()
    admission = _table(schema, _ADMISSION)
    op.execute(
        f"""DO $block$ BEGIN
            IF EXISTS (SELECT 1 FROM {admission}) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_admission_downgrade_forbidden'
                    USING ERRCODE = '55000';
            END IF;
        END; $block$;"""
    )
    op.execute(f"DROP TABLE IF EXISTS {admission};")
    for function_name in (_ADMISSION_INSERT_GUARD, _ADMISSION_IMMUTABLE_GUARD):
        op.execute(f"DROP FUNCTION IF EXISTS {_table(schema, function_name)}();")
