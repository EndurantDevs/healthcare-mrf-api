"""Add immutable FHIR formulary generations and atomic publication pointer.

Revision ID: 20260806100000_fhir_formulary_generations
Revises: 20260806100000_ptg2_tax_identity_source
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260806100000_fhir_formulary_generations"
down_revision = "20260806100000_ptg2_tax_identity_source"
branch_labels = None
depends_on = None


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


def upgrade() -> None:
    schema = _schema()
    source = _qt(schema, "fhir_formulary_source")
    dataset = _qt(schema, "fhir_formulary_dataset")
    current = _qt(schema, "fhir_formulary_current")
    plan = _qt(schema, "fhir_formulary_coverage_plan")
    plan_version = _qt(schema, "fhir_formulary_coverage_plan_version")
    dataset_plan = _qt(schema, "fhir_formulary_dataset_coverage_plan")
    alias = _qt(schema, "fhir_formulary_drug_plan_alias")
    alias_version = _qt(schema, "fhir_formulary_drug_plan_alias_version")
    dataset_alias = _qt(schema, "fhir_formulary_dataset_alias")
    medication = _qt(schema, "fhir_formulary_medication")
    membership = _qt(schema, "fhir_formulary_alias_membership")
    alternative = _qt(schema, "fhir_formulary_alternative")
    checkpoint = _qt(schema, "fhir_formulary_checkpoint")

    op.execute(
        f"""
        CREATE TABLE {source} (
            source_id varchar(64) PRIMARY KEY,
            canonical_base text NOT NULL UNIQUE,
            display_name varchar(256) NOT NULL,
            enabled boolean NOT NULL DEFAULT false,
            metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        );

        CREATE TABLE {dataset} (
            dataset_id varchar(64) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            run_id varchar(64) NOT NULL UNIQUE,
            previous_dataset_id varchar(64) REFERENCES {dataset}(dataset_id),
            cutoff_at timestamptz NOT NULL,
            status varchar(16) NOT NULL,
            publish_requested boolean NOT NULL DEFAULT false,
            list_count integer NOT NULL DEFAULT 0,
            alias_count integer NOT NULL DEFAULT 0,
            medication_count bigint NOT NULL DEFAULT 0,
            coverage_hash varchar(64),
            membership_hash varchar(64),
            summary_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            verified_at timestamptz,
            published_at timestamptz,
            failed_at timestamptz,
            error_json jsonb,
            CONSTRAINT fhir_formulary_dataset_status_check CHECK (
                status IN ('building', 'verified', 'published', 'failed')
            )
        );
        CREATE INDEX fhir_formulary_dataset_source_created_idx
            ON {dataset}(source_id, created_at DESC);
        CREATE INDEX fhir_formulary_dataset_status_created_idx
            ON {dataset}(status, created_at DESC);

        CREATE TABLE {current} (
            source_id varchar(64) PRIMARY KEY REFERENCES {source}(source_id),
            dataset_id varchar(64) NOT NULL UNIQUE REFERENCES {dataset}(dataset_id),
            generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
            published_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        );

        CREATE TABLE {plan} (
            public_id varchar(31) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            upstream_list_id varchar(256) NOT NULL,
            canonical_identity text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_coverage_plan_public_id_check
                CHECK (public_id ~ '^fhir_[a-z2-7]{{26}}$'),
            CONSTRAINT fhir_formulary_coverage_plan_identity_key
                UNIQUE (source_id, canonical_identity)
        );

        CREATE TABLE {plan_version} (
            coverage_version_id varchar(64) PRIMARY KEY,
            public_id varchar(31) NOT NULL REFERENCES {plan}(public_id),
            upstream_version_id varchar(256),
            upstream_last_updated timestamptz,
            status varchar(32),
            title text,
            name text,
            period_start timestamptz,
            period_end timestamptz,
            upstream_date timestamptz,
            content_hash varchar(64) NOT NULL,
            metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_coverage_plan_version_content_key
                UNIQUE (public_id, content_hash)
        );

        CREATE TABLE {dataset_plan} (
            dataset_id varchar(64) NOT NULL REFERENCES {dataset}(dataset_id),
            public_id varchar(31) NOT NULL REFERENCES {plan}(public_id),
            coverage_version_id varchar(64) NOT NULL
                REFERENCES {plan_version}(coverage_version_id),
            PRIMARY KEY (dataset_id, public_id),
            CONSTRAINT fhir_formulary_dataset_coverage_version_key
                UNIQUE (dataset_id, coverage_version_id)
        );

        CREATE TABLE {alias} (
            alias_id varchar(64) PRIMARY KEY,
            public_id varchar(31) NOT NULL REFERENCES {plan}(public_id),
            source_plan_identifier varchar(512) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_alias_plan_key
                UNIQUE (public_id, source_plan_identifier)
        );
        CREATE INDEX fhir_formulary_alias_source_plan_idx
            ON {alias}(source_plan_identifier);

        CREATE TABLE {alias_version} (
            alias_version_id varchar(64) PRIMARY KEY,
            alias_id varchar(64) NOT NULL REFERENCES {alias}(alias_id),
            expected_count bigint NOT NULL CHECK (expected_count >= 0),
            membership_count bigint NOT NULL CHECK (membership_count >= 0),
            membership_hash varchar(64) NOT NULL,
            cutoff_at timestamptz NOT NULL,
            acquisition_mode varchar(32) NOT NULL CHECK (
                acquisition_mode IN ('full', 'delta', 'reuse')
            ),
            reused_from_alias_version_id varchar(64)
                REFERENCES {alias_version}(alias_version_id),
            summary_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_alias_version_membership_key
                UNIQUE (alias_id, membership_hash),
            CONSTRAINT fhir_formulary_alias_version_exact_count_check
                CHECK (expected_count = membership_count)
        );
        CREATE INDEX fhir_formulary_alias_version_created_idx
            ON {alias_version}(alias_id, created_at DESC);

        CREATE TABLE {dataset_alias} (
            dataset_id varchar(64) NOT NULL REFERENCES {dataset}(dataset_id),
            alias_id varchar(64) NOT NULL REFERENCES {alias}(alias_id),
            alias_version_id varchar(64) NOT NULL
                REFERENCES {alias_version}(alias_version_id),
            PRIMARY KEY (dataset_id, alias_id)
        );
        CREATE INDEX fhir_formulary_dataset_alias_version_idx
            ON {dataset_alias}(dataset_id, alias_version_id);

        CREATE TABLE {medication} (
            medication_version_id varchar(64) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            upstream_medication_id varchar(256) NOT NULL,
            upstream_version_id varchar(256),
            upstream_last_updated timestamptz,
            status varchar(32),
            drug_name text,
            rxnorm_id varchar(64),
            ndc11 varchar(11),
            codings_json jsonb NOT NULL,
            content_hash varchar(64) NOT NULL,
            metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_medication_content_key
                UNIQUE (source_id, upstream_medication_id, content_hash),
            CONSTRAINT fhir_formulary_medication_ndc11_check
                CHECK (ndc11 IS NULL OR ndc11 ~ '^[0-9]{{11}}$')
        );
        CREATE INDEX fhir_formulary_medication_rxnorm_idx
            ON {medication}(rxnorm_id) WHERE rxnorm_id IS NOT NULL;
        CREATE INDEX fhir_formulary_medication_ndc11_idx
            ON {medication}(ndc11) WHERE ndc11 IS NOT NULL;

        CREATE TABLE {membership} (
            alias_version_id varchar(64) NOT NULL
                REFERENCES {alias_version}(alias_version_id),
            upstream_medication_id varchar(256) NOT NULL,
            medication_version_id varchar(64) NOT NULL
                REFERENCES {medication}(medication_version_id),
            rxnorm_id varchar(64),
            drug_tier text,
            prior_authorization boolean,
            step_therapy boolean,
            quantity_limit boolean,
            variant_hash varchar(64) NOT NULL,
            PRIMARY KEY (alias_version_id, upstream_medication_id)
        );
        CREATE INDEX fhir_formulary_membership_rxnorm_idx
            ON {membership}(alias_version_id, rxnorm_id)
            WHERE rxnorm_id IS NOT NULL;

        CREATE TABLE {alternative} (
            alias_version_id varchar(64) NOT NULL,
            upstream_medication_id varchar(256) NOT NULL,
            raw_reference text NOT NULL,
            corrected_reference text,
            resolved_medication_id varchar(256),
            resolved boolean NOT NULL DEFAULT false,
            rule_version varchar(64),
            evidence_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (
                alias_version_id,
                upstream_medication_id,
                raw_reference
            ),
            FOREIGN KEY (alias_version_id, upstream_medication_id)
                REFERENCES {membership}(alias_version_id, upstream_medication_id),
            CONSTRAINT fhir_formulary_alternative_resolution_check CHECK (
                (resolved AND resolved_medication_id IS NOT NULL)
                OR (NOT resolved AND resolved_medication_id IS NULL)
            )
        );

        CREATE TABLE {checkpoint} (
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            alias_id varchar(64) NOT NULL REFERENCES {alias}(alias_id),
            source_plan_identifier varchar(512) NOT NULL,
            run_id varchar(64) NOT NULL,
            dataset_id varchar(64) NOT NULL REFERENCES {dataset}(dataset_id),
            fence_token bigint NOT NULL CHECK (fence_token > 0),
            cutoff_at timestamptz NOT NULL,
            acquisition_mode varchar(32) NOT NULL,
            next_url text,
            expected_count bigint,
            processed_count bigint NOT NULL DEFAULT 0,
            membership_hash varchar(64),
            completed boolean NOT NULL DEFAULT false,
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            PRIMARY KEY (source_id, alias_id, run_id),
            CONSTRAINT fhir_formulary_checkpoint_count_check CHECK (
                expected_count IS NULL OR (
                    expected_count >= 0
                    AND processed_count >= 0
                    AND processed_count <= expected_count
                )
            )
        );
        CREATE INDEX fhir_formulary_checkpoint_run_fence_idx
            ON {checkpoint}(run_id, fence_token);
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {_qt(schema, 'guard_fhir_formulary_checkpoint_fence')}()
        RETURNS trigger LANGUAGE plpgsql AS $function$
        BEGIN
            IF NEW.fence_token <= OLD.fence_token THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_stale_fence'
                    USING ERRCODE = '40001';
            END IF;
            IF NEW.dataset_id <> OLD.dataset_id
               OR NEW.run_id <> OLD.run_id
               OR NEW.source_id <> OLD.source_id
               OR NEW.alias_id <> OLD.alias_id
               OR NEW.source_plan_identifier <> OLD.source_plan_identifier
               OR NEW.cutoff_at <> OLD.cutoff_at THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_owner_immutable'
                    USING ERRCODE = '55000';
            END IF;
            NEW.updated_at := transaction_timestamp();
            RETURN NEW;
        END;
        $function$;
        CREATE TRIGGER fhir_formulary_checkpoint_fence_guard
            BEFORE UPDATE ON {checkpoint}
            FOR EACH ROW EXECUTE FUNCTION
                {_qt(schema, 'guard_fhir_formulary_checkpoint_fence')}();
        """
    )

    op.execute(
        f"""
        INSERT INTO {source} (
            source_id,
            canonical_base,
            display_name,
            enabled,
            metadata_json
        ) VALUES (
            'fhir-formulary-primary',
            'https://kpx-service-bus.kp.org/service/hp/mhpo/healthplanproviderv1rc',
            'FHIR formulary source',
            false,
            '{{"automation_enabled": false, "manual_seed_only": true}}'::jsonb
        );
        """
    )


def downgrade() -> None:
    schema = _schema()
    for table in (
        "fhir_formulary_checkpoint",
        "fhir_formulary_alternative",
        "fhir_formulary_alias_membership",
        "fhir_formulary_medication",
        "fhir_formulary_dataset_alias",
        "fhir_formulary_drug_plan_alias_version",
        "fhir_formulary_drug_plan_alias",
        "fhir_formulary_dataset_coverage_plan",
        "fhir_formulary_coverage_plan_version",
        "fhir_formulary_coverage_plan",
        "fhir_formulary_current",
        "fhir_formulary_dataset",
        "fhir_formulary_source",
    ):
        op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)} CASCADE;")
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_qt(schema, 'guard_fhir_formulary_checkpoint_fence')}();"
    )
