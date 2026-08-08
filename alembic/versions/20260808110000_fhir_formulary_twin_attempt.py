"""Add one-use FHIR formulary twin-attempt evidence.

Revision ID: 20260808110000_fhir_formulary_twin_attempt
Revises: 20260808100000_public_evidence_reference_roots
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808110000_fhir_formulary_twin_attempt"
down_revision = "20260808100000_public_evidence_reference_roots"
branch_labels = None
depends_on = None

_ATTEMPT = "fhir_formulary_twin_attempt"
_ATTEMPT_INSERT_GUARD = "guard_fhir_formulary_twin_attempt_insert"
_IMMUTABLE_GUARD = "guard_fhir_formulary_twin_attempt_immutable"
_DATASET_GUARD = "guard_fhir_formulary_twin_dataset"


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
    """Create immutable attempts and burn each evaluated root once."""

    schema = _schema()
    attempt = _table(schema, _ATTEMPT)
    dataset = _table(schema, "fhir_formulary_dataset")
    op.execute(
        f"""
        CREATE TABLE {attempt} (
            source_id varchar(64) NOT NULL,
            baseline_dataset_id varchar(64) NOT NULL,
            baseline_run_id varchar(64) NOT NULL,
            candidate_dataset_id varchar(64) NOT NULL,
            candidate_run_id varchar(64) NOT NULL,
            cutoff_at timestamptz NOT NULL,
            source_configuration_hash varchar(64) NOT NULL,
            acquisition_contract_hash varchar(64) NOT NULL,
            baseline_evidence_hash varchar(64) NOT NULL,
            candidate_evidence_hash varchar(64) NOT NULL,
            matched boolean NOT NULL,
            attempted_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_twin_attempt_pkey
                PRIMARY KEY (source_id, baseline_dataset_id, candidate_dataset_id),
            CONSTRAINT fhir_formulary_twin_attempt_baseline_key
                UNIQUE (baseline_dataset_id),
            CONSTRAINT fhir_formulary_twin_attempt_candidate_key
                UNIQUE (candidate_dataset_id),
            CONSTRAINT fhir_formulary_twin_attempt_binding_key UNIQUE (
                source_id, baseline_dataset_id, baseline_run_id,
                candidate_dataset_id, candidate_run_id
            ),
            CONSTRAINT fhir_formulary_twin_attempt_baseline_fkey
                FOREIGN KEY (source_id, baseline_dataset_id, baseline_run_id)
                REFERENCES {dataset}(source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_twin_attempt_candidate_fkey
                FOREIGN KEY (source_id, candidate_dataset_id, candidate_run_id)
                REFERENCES {dataset}(source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_twin_attempt_identity_check CHECK (
                baseline_dataset_id <> candidate_dataset_id
                AND baseline_run_id <> candidate_run_id
            ),
            CONSTRAINT fhir_formulary_twin_attempt_proof_check CHECK (
                source_configuration_hash ~ '^[0-9a-f]{{64}}$'
                AND acquisition_contract_hash ~ '^[0-9a-f]{{64}}$'
                AND baseline_evidence_hash ~ '^[0-9a-f]{{64}}$'
                AND candidate_evidence_hash ~ '^[0-9a-f]{{64}}$'
                AND matched = (baseline_evidence_hash = candidate_evidence_hash)
            )
        );
        """
    )
    _install_attempt_insert_guard(schema)
    _install_immutable_guard(schema)
    _install_strict_dataset_guard(schema)


def _install_attempt_insert_guard(schema: str) -> None:
    attempt = _table(schema, _ATTEMPT)
    source = _table(schema, "fhir_formulary_source")
    dataset = _table(schema, "fhir_formulary_dataset")
    current = _table(schema, "fhir_formulary_current")
    guard = _table(schema, _ATTEMPT_INSERT_GUARD)
    op.execute(
        f"""
        CREATE FUNCTION {guard}()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            existing_row record;
            baseline_row record;
            candidate_row record;
            current_dataset_id varchar(64);
            current_exists boolean;
        BEGIN
            PERFORM 1 FROM {source} WHERE source_id = NEW.source_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'fhir_formulary_twin_source_invalid' USING ERRCODE = '55000';
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
                LEAST(NEW.baseline_dataset_id, NEW.candidate_dataset_id), 1701));
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
                GREATEST(NEW.baseline_dataset_id, NEW.candidate_dataset_id), 1701));
            SELECT * INTO existing_row FROM {attempt}
             WHERE baseline_dataset_id IN (NEW.baseline_dataset_id, NEW.candidate_dataset_id)
                OR candidate_dataset_id IN (NEW.baseline_dataset_id, NEW.candidate_dataset_id)
             FOR SHARE;
            IF FOUND AND existing_row.source_id = NEW.source_id
               AND existing_row.baseline_dataset_id = NEW.baseline_dataset_id
               AND existing_row.baseline_run_id = NEW.baseline_run_id
               AND existing_row.candidate_dataset_id = NEW.candidate_dataset_id
               AND existing_row.candidate_run_id = NEW.candidate_run_id
               AND existing_row.cutoff_at = NEW.cutoff_at
               AND existing_row.source_configuration_hash = NEW.source_configuration_hash
               AND existing_row.acquisition_contract_hash = NEW.acquisition_contract_hash
               AND existing_row.baseline_evidence_hash = NEW.baseline_evidence_hash
               AND existing_row.candidate_evidence_hash = NEW.candidate_evidence_hash
               AND existing_row.matched = NEW.matched THEN
                RETURN NEW;
            END IF;
            IF FOUND THEN
                RAISE EXCEPTION 'fhir_formulary_twin_root_already_evaluated'
                    USING ERRCODE = '55000';
            END IF;
            SELECT * INTO baseline_row FROM {dataset}
             WHERE source_id = NEW.source_id AND dataset_id = NEW.baseline_dataset_id
               AND run_id = NEW.baseline_run_id FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'fhir_formulary_twin_baseline_invalid' USING ERRCODE = '55000';
            END IF;
            SELECT * INTO candidate_row FROM {dataset}
             WHERE source_id = NEW.source_id AND dataset_id = NEW.candidate_dataset_id
               AND run_id = NEW.candidate_run_id FOR SHARE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'fhir_formulary_twin_candidate_invalid' USING ERRCODE = '55000';
            END IF;
            IF baseline_row.status IS DISTINCT FROM 'verified'
               OR baseline_row.publish_requested IS DISTINCT FROM false
               OR baseline_row.seed_eligible IS DISTINCT FROM false
               OR candidate_row.status IS DISTINCT FROM 'verified'
               OR candidate_row.publish_requested IS DISTINCT FROM true
               OR candidate_row.seed_eligible IS DISTINCT FROM false
               OR baseline_row.verified_at IS NULL OR candidate_row.verified_at IS NULL
               OR baseline_row.failed_at IS NOT NULL OR candidate_row.failed_at IS NOT NULL
               OR baseline_row.error_json IS NOT NULL OR candidate_row.error_json IS NOT NULL
               OR baseline_row.published_at IS NOT NULL OR candidate_row.published_at IS NOT NULL THEN
                RAISE EXCEPTION 'fhir_formulary_twin_roles_invalid' USING ERRCODE = '55000';
            END IF;
            IF baseline_row.previous_dataset_id IS DISTINCT FROM candidate_row.previous_dataset_id
               OR baseline_row.cutoff_at IS DISTINCT FROM NEW.cutoff_at
               OR candidate_row.cutoff_at IS DISTINCT FROM NEW.cutoff_at
               OR baseline_row.summary_json ->> 'acquisition_contract_hash'
                    IS DISTINCT FROM NEW.acquisition_contract_hash
               OR candidate_row.summary_json ->> 'acquisition_contract_hash'
                    IS DISTINCT FROM NEW.acquisition_contract_hash
               OR NEW.attempted_at < baseline_row.verified_at
               OR NEW.attempted_at < candidate_row.verified_at THEN
                RAISE EXCEPTION 'fhir_formulary_twin_attempt_invalid' USING ERRCODE = '55000';
            END IF;
            SELECT dataset_id INTO current_dataset_id FROM {current}
             WHERE source_id = NEW.source_id FOR SHARE;
            current_exists := FOUND;
            IF (baseline_row.previous_dataset_id IS NULL AND current_exists)
               OR (baseline_row.previous_dataset_id IS NOT NULL AND (
                    NOT current_exists OR current_dataset_id <> baseline_row.previous_dataset_id
               )) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_predecessor_stale' USING ERRCODE = '40001';
            END IF;
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_attempt_insert_guard BEFORE INSERT ON {attempt} "
        f"FOR EACH ROW EXECUTE FUNCTION {guard}();"
    )


def _install_immutable_guard(schema: str) -> None:
    attempt = _table(schema, _ATTEMPT)
    guard = _table(schema, _IMMUTABLE_GUARD)
    op.execute(
        f"""CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$
        BEGIN
            RAISE EXCEPTION 'fhir_formulary_twin_attempt_immutable' USING ERRCODE = '55000';
        END; $function$;"""
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_attempt_immutable_guard "
        f"BEFORE UPDATE OR DELETE ON {attempt} FOR EACH ROW EXECUTE FUNCTION {guard}();"
    )
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_attempt_truncate_guard "
        f"BEFORE TRUNCATE ON {attempt} FOR EACH STATEMENT EXECUTE FUNCTION {guard}();"
    )


def _strict_dataset_guard_sql(schema: str) -> str:
    attempt = _table(schema, _ATTEMPT)
    guard = _table(schema, _DATASET_GUARD)
    return f"""CREATE OR REPLACE FUNCTION {guard}()
    RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {attempt} WHERE source_id = OLD.source_id
             AND (baseline_dataset_id = OLD.dataset_id OR candidate_dataset_id = OLD.dataset_id)
        ) THEN
            RAISE EXCEPTION 'fhir_formulary_twin_dataset_immutable' USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END;
    $function$;"""


def _install_strict_dataset_guard(schema: str) -> None:
    dataset = _table(schema, "fhir_formulary_dataset")
    guard = _table(schema, _DATASET_GUARD)
    op.execute(_strict_dataset_guard_sql(schema))
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_twin_dataset_guard BEFORE UPDATE OR DELETE ON {dataset} "
        f"FOR EACH ROW EXECUTE FUNCTION {guard}();"
    )


def downgrade() -> None:
    """Remove attempt evidence and its dataset freeze."""

    schema = _schema()
    attempt = _table(schema, _ATTEMPT)
    dataset = _table(schema, "fhir_formulary_dataset")
    op.execute(
        f"""DO $block$ BEGIN
            IF EXISTS (SELECT 1 FROM {attempt}) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_attempt_downgrade_forbidden'
                    USING ERRCODE = '55000';
            END IF;
        END; $block$;"""
    )
    op.execute(f"DROP TRIGGER IF EXISTS fhir_formulary_twin_dataset_guard ON {dataset};")
    op.execute(f"DROP TABLE IF EXISTS {attempt};")
    for function_name in (_DATASET_GUARD, _ATTEMPT_INSERT_GUARD, _IMMUTABLE_GUARD):
        op.execute(f"DROP FUNCTION IF EXISTS {_table(schema, function_name)}();")
