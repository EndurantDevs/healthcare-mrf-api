"""Add dormant, source-neutral FHIR formulary storage.

Revision ID: 20260807110000_fhir_formulary_storage_foundation
Revises: 20260807100000_provider_directory_endpoint_dataset_guard

The migration only creates storage and integrity primitives.  It registers no
source and enables no acquisition, publication, or serving path.  A publisher
must lock the source row before creating or replacing the current pointer.
"""

from __future__ import annotations

import os

from alembic import op

revision = "20260807110000_fhir_formulary_storage_foundation"
down_revision = "20260807100000_provider_directory_endpoint_dataset_guard"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _execute_plain_statements(sql: str) -> None:
    """Execute a semicolon-delimited DDL batch one statement at a time."""

    for statement in sql.split(";"):
        normalized_statement = statement.strip()
        if normalized_statement:
            op.execute(normalized_statement)


def upgrade() -> None:
    """Create inactive copy-on-write storage with ownership-safe references."""

    schema = _schema()
    source = _qualified_table(schema, "fhir_formulary_source")
    dataset = _qualified_table(schema, "fhir_formulary_dataset")
    current = _qualified_table(schema, "fhir_formulary_current")
    coverage_plan = _qualified_table(schema, "fhir_formulary_coverage_plan")
    coverage_plan_version = _qualified_table(schema, "fhir_formulary_coverage_plan_version")
    dataset_coverage_plan = _qualified_table(schema, "fhir_formulary_dataset_coverage_plan")
    alias = _qualified_table(schema, "fhir_formulary_drug_plan_alias")
    alias_version = _qualified_table(schema, "fhir_formulary_drug_plan_alias_version")
    dataset_alias = _qualified_table(schema, "fhir_formulary_dataset_alias")
    medication = _qualified_table(schema, "fhir_formulary_medication")
    membership = _qualified_table(schema, "fhir_formulary_alias_membership")
    alternative = _qualified_table(schema, "fhir_formulary_alternative")
    checkpoint = _qualified_table(schema, "fhir_formulary_checkpoint")

    _execute_plain_statements(
        f"""
        CREATE TABLE {source} (
            source_id varchar(64) PRIMARY KEY,
            canonical_base text NOT NULL,
            display_name varchar(256) NOT NULL,
            enabled boolean NOT NULL DEFAULT false,
            runtime_config_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_source_base_key UNIQUE (canonical_base),
            CONSTRAINT fhir_formulary_source_runtime_config_check CHECK (
                jsonb_typeof(runtime_config_json) = 'object'
            ),
            CONSTRAINT fhir_formulary_source_metadata_check CHECK (
                jsonb_typeof(metadata_json) = 'object'
            )
        );

        CREATE TABLE {dataset} (
            dataset_id varchar(64) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            run_id varchar(64) NOT NULL,
            previous_dataset_id varchar(64),
            cutoff_at timestamptz NOT NULL,
            status varchar(16) NOT NULL,
            publish_requested boolean NOT NULL DEFAULT false,
            seed_eligible boolean NOT NULL DEFAULT false,
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
            CONSTRAINT fhir_formulary_dataset_run_key UNIQUE (run_id),
            CONSTRAINT fhir_formulary_dataset_source_dataset_key
                UNIQUE (source_id, dataset_id),
            CONSTRAINT fhir_formulary_dataset_checkpoint_owner_key
                UNIQUE (source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_dataset_previous_owner_fkey
                FOREIGN KEY (source_id, previous_dataset_id)
                REFERENCES {dataset}(source_id, dataset_id),
            CONSTRAINT fhir_formulary_dataset_status_check CHECK (
                status IN ('building', 'verified', 'published', 'failed')
            ),
            CONSTRAINT fhir_formulary_dataset_count_check CHECK (
                list_count >= 0
                AND alias_count >= 0
                AND medication_count >= 0
            ),
            CONSTRAINT fhir_formulary_dataset_summary_check CHECK (
                jsonb_typeof(summary_json) = 'object'
            )
        );
        CREATE INDEX fhir_formulary_dataset_source_created_idx
            ON {dataset}(source_id, created_at DESC);
        CREATE INDEX fhir_formulary_dataset_status_created_idx
            ON {dataset}(status, created_at DESC);

        CREATE TABLE {current} (
            source_id varchar(64) PRIMARY KEY REFERENCES {source}(source_id),
            dataset_id varchar(64) NOT NULL,
            generation bigint NOT NULL DEFAULT 1,
            published_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_current_dataset_key UNIQUE (dataset_id),
            CONSTRAINT fhir_formulary_current_source_dataset_fkey
                FOREIGN KEY (source_id, dataset_id)
                REFERENCES {dataset}(source_id, dataset_id),
            CONSTRAINT fhir_formulary_current_generation_check CHECK (
                generation > 0
            )
        );

        CREATE TABLE {coverage_plan} (
            public_id varchar(31) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            upstream_list_id varchar(256) NOT NULL,
            canonical_identity text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_coverage_plan_public_id_check CHECK (
                public_id ~ '^fhir_[a-z2-7]{{26}}$'
            ),
            CONSTRAINT fhir_formulary_coverage_plan_identity_key
                UNIQUE (source_id, canonical_identity),
            CONSTRAINT fhir_formulary_coverage_plan_source_public_key
                UNIQUE (source_id, public_id)
        );

        CREATE TABLE {coverage_plan_version} (
            coverage_version_id varchar(64) PRIMARY KEY,
            public_id varchar(31) NOT NULL REFERENCES {coverage_plan}(public_id),
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
            CONSTRAINT fhir_formulary_coverage_plan_version_owner_key
                UNIQUE (public_id, coverage_version_id),
            CONSTRAINT fhir_formulary_coverage_plan_version_content_key
                UNIQUE (public_id, content_hash),
            CONSTRAINT fhir_formulary_coverage_plan_version_metadata_check CHECK (
                jsonb_typeof(metadata_json) = 'object'
            )
        );

        CREATE TABLE {dataset_coverage_plan} (
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            dataset_id varchar(64) NOT NULL,
            public_id varchar(31) NOT NULL,
            coverage_version_id varchar(64) NOT NULL,
            PRIMARY KEY (dataset_id, public_id),
            CONSTRAINT fhir_formulary_dataset_coverage_version_key
                UNIQUE (dataset_id, coverage_version_id),
            CONSTRAINT fhir_formulary_dataset_coverage_dataset_owner_fkey
                FOREIGN KEY (source_id, dataset_id)
                REFERENCES {dataset}(source_id, dataset_id),
            CONSTRAINT fhir_formulary_dataset_coverage_plan_owner_fkey
                FOREIGN KEY (source_id, public_id)
                REFERENCES {coverage_plan}(source_id, public_id),
            CONSTRAINT fhir_formulary_dataset_coverage_version_owner_fkey
                FOREIGN KEY (public_id, coverage_version_id)
                REFERENCES {coverage_plan_version}(
                    public_id,
                    coverage_version_id
                )
        );

        CREATE TABLE {alias} (
            alias_id varchar(64) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            public_id varchar(31) NOT NULL,
            source_plan_identifier varchar(512) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_alias_plan_key
                UNIQUE (public_id, source_plan_identifier),
            CONSTRAINT fhir_formulary_alias_source_alias_key
                UNIQUE (source_id, alias_id),
            CONSTRAINT fhir_formulary_alias_checkpoint_owner_key
                UNIQUE (source_id, alias_id, source_plan_identifier),
            CONSTRAINT fhir_formulary_alias_coverage_owner_fkey
                FOREIGN KEY (source_id, public_id)
                REFERENCES {coverage_plan}(source_id, public_id)
        );
        CREATE INDEX fhir_formulary_alias_source_plan_idx
            ON {alias}(source_plan_identifier);

        CREATE TABLE {alias_version} (
            alias_version_id varchar(64) PRIMARY KEY,
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            alias_id varchar(64) NOT NULL,
            expected_count bigint NOT NULL,
            membership_count bigint NOT NULL,
            membership_hash varchar(64) NOT NULL,
            cutoff_at timestamptz NOT NULL,
            acquisition_mode varchar(32) NOT NULL,
            summary_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT fhir_formulary_alias_version_owner_key
                UNIQUE (alias_id, alias_version_id),
            CONSTRAINT fhir_formulary_alias_version_source_version_key
                UNIQUE (source_id, alias_version_id),
            CONSTRAINT fhir_formulary_alias_version_membership_key
                UNIQUE (alias_id, membership_hash),
            CONSTRAINT fhir_formulary_alias_version_alias_owner_fkey
                FOREIGN KEY (source_id, alias_id)
                REFERENCES {alias}(source_id, alias_id),
            CONSTRAINT fhir_formulary_alias_version_count_check CHECK (
                expected_count >= 0 AND membership_count >= 0
            ),
            CONSTRAINT fhir_formulary_alias_version_exact_count_check CHECK (
                expected_count = membership_count
            ),
            CONSTRAINT fhir_formulary_alias_version_mode_check CHECK (
                acquisition_mode IN ('full', 'delta')
            ),
            CONSTRAINT fhir_formulary_alias_version_summary_check CHECK (
                jsonb_typeof(summary_json) = 'object'
            )
        );
        CREATE INDEX fhir_formulary_alias_version_created_idx
            ON {alias_version}(alias_id, created_at DESC);

        CREATE TABLE {dataset_alias} (
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            dataset_id varchar(64) NOT NULL,
            alias_id varchar(64) NOT NULL,
            alias_version_id varchar(64) NOT NULL,
            PRIMARY KEY (dataset_id, alias_id),
            CONSTRAINT fhir_formulary_dataset_alias_dataset_owner_fkey
                FOREIGN KEY (source_id, dataset_id)
                REFERENCES {dataset}(source_id, dataset_id),
            CONSTRAINT fhir_formulary_dataset_alias_alias_owner_fkey
                FOREIGN KEY (source_id, alias_id)
                REFERENCES {alias}(source_id, alias_id),
            CONSTRAINT fhir_formulary_dataset_alias_version_owner_fkey
                FOREIGN KEY (alias_id, alias_version_id)
                REFERENCES {alias_version}(alias_id, alias_version_id)
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
            CONSTRAINT fhir_formulary_medication_source_version_key
                UNIQUE (source_id, medication_version_id),
            CONSTRAINT fhir_formulary_medication_membership_owner_key
                UNIQUE (
                    source_id,
                    upstream_medication_id,
                    medication_version_id
                ),
            CONSTRAINT fhir_formulary_medication_ndc11_check CHECK (
                ndc11 IS NULL OR ndc11 ~ '^[0-9]{{11}}$'
            ),
            CONSTRAINT fhir_formulary_medication_codings_check CHECK (
                jsonb_typeof(codings_json) = 'array'
            ),
            CONSTRAINT fhir_formulary_medication_metadata_check CHECK (
                jsonb_typeof(metadata_json) = 'object'
            )
        );
        CREATE INDEX fhir_formulary_medication_rxnorm_idx
            ON {medication}(rxnorm_id) WHERE rxnorm_id IS NOT NULL;
        CREATE INDEX fhir_formulary_medication_ndc11_idx
            ON {medication}(ndc11) WHERE ndc11 IS NOT NULL;

        CREATE TABLE {membership} (
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            alias_version_id varchar(64) NOT NULL,
            upstream_medication_id varchar(256) NOT NULL,
            medication_version_id varchar(64) NOT NULL,
            rxnorm_id varchar(64),
            drug_tier text,
            prior_authorization boolean,
            step_therapy boolean,
            quantity_limit boolean,
            variant_hash varchar(64) NOT NULL,
            PRIMARY KEY (alias_version_id, upstream_medication_id),
            CONSTRAINT fhir_formulary_membership_alias_owner_fkey
                FOREIGN KEY (source_id, alias_version_id)
                REFERENCES {alias_version}(source_id, alias_version_id),
            CONSTRAINT fhir_formulary_membership_medication_owner_fkey
                FOREIGN KEY (
                    source_id,
                    upstream_medication_id,
                    medication_version_id
                ) REFERENCES {medication}(
                    source_id,
                    upstream_medication_id,
                    medication_version_id
                )
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
            CONSTRAINT fhir_formulary_alternative_membership_fkey
                FOREIGN KEY (alias_version_id, upstream_medication_id)
                REFERENCES {membership}(alias_version_id, upstream_medication_id),
            CONSTRAINT fhir_formulary_alternative_target_owner_fkey
                FOREIGN KEY (alias_version_id, resolved_medication_id)
                REFERENCES {membership}(alias_version_id, upstream_medication_id),
            CONSTRAINT fhir_formulary_alternative_resolution_check CHECK (
                (resolved AND resolved_medication_id IS NOT NULL)
                OR (NOT resolved AND resolved_medication_id IS NULL)
            ),
            CONSTRAINT fhir_formulary_alternative_evidence_check CHECK (
                jsonb_typeof(evidence_json) = 'object'
            )
        );

        CREATE TABLE {checkpoint} (
            source_id varchar(64) NOT NULL REFERENCES {source}(source_id),
            alias_id varchar(64) NOT NULL,
            source_plan_identifier varchar(512) NOT NULL,
            run_id varchar(64) NOT NULL,
            dataset_id varchar(64) NOT NULL,
            fence_token bigint NOT NULL,
            cutoff_at timestamptz NOT NULL,
            acquisition_mode varchar(32) NOT NULL,
            expected_count bigint,
            processed_count bigint NOT NULL DEFAULT 0,
            membership_hash varchar(64),
            completed boolean NOT NULL DEFAULT false,
            updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            PRIMARY KEY (source_id, alias_id, run_id),
            CONSTRAINT fhir_formulary_checkpoint_dataset_owner_fkey
                FOREIGN KEY (source_id, dataset_id, run_id)
                REFERENCES {dataset}(source_id, dataset_id, run_id),
            CONSTRAINT fhir_formulary_checkpoint_alias_owner_fkey
                FOREIGN KEY (source_id, alias_id, source_plan_identifier)
                REFERENCES {alias}(source_id, alias_id, source_plan_identifier),
            CONSTRAINT fhir_formulary_checkpoint_fence_check CHECK (
                fence_token > 0
            ),
            CONSTRAINT fhir_formulary_checkpoint_mode_check CHECK (
                acquisition_mode IN ('full', 'delta', 'reuse')
            ),
            CONSTRAINT fhir_formulary_checkpoint_count_check CHECK (
                processed_count >= 0
                AND (
                    expected_count IS NULL
                    OR (
                        expected_count >= 0
                        AND processed_count <= expected_count
                    )
                )
            ),
            CONSTRAINT fhir_formulary_checkpoint_completion_check CHECK (
                NOT completed
                OR (
                    expected_count IS NOT NULL
                    AND processed_count = expected_count
                    AND membership_hash IS NOT NULL
                    AND membership_hash ~ '^[0-9a-f]{{64}}$'
                )
            )
        );
        CREATE INDEX fhir_formulary_checkpoint_run_fence_idx
            ON {checkpoint}(run_id, fence_token);
        """
    )

    guard_function = _qualified_table(schema, "guard_fhir_formulary_checkpoint_fence")
    op.execute(
        f"""
        CREATE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.completed THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_complete_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF NEW.fence_token <= OLD.fence_token THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_stale_fence'
                    USING ERRCODE = '40001';
            END IF;
            IF NEW.dataset_id <> OLD.dataset_id
               OR NEW.run_id <> OLD.run_id
               OR NEW.source_id <> OLD.source_id
               OR NEW.alias_id <> OLD.alias_id
               OR NEW.source_plan_identifier <> OLD.source_plan_identifier
               OR NEW.cutoff_at <> OLD.cutoff_at
               OR NEW.acquisition_mode <> OLD.acquisition_mode THEN
                RAISE EXCEPTION 'fhir_formulary_checkpoint_owner_immutable'
                    USING ERRCODE = '55000';
            END IF;
            NEW.updated_at := transaction_timestamp();
            RETURN NEW;
        END;
        $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard_function}() FROM PUBLIC;")
    op.execute(
        f"""
        CREATE TRIGGER fhir_formulary_checkpoint_fence_guard
            BEFORE UPDATE OR DELETE ON {checkpoint}
            FOR EACH ROW EXECUTE FUNCTION {guard_function}();
        """
    )


def downgrade() -> None:
    """Remove only the dormant formulary storage introduced here."""

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
        op.execute(f"DROP TABLE IF EXISTS {_qualified_table(schema, table)};")
    op.execute(
        "DROP FUNCTION IF EXISTS "
        f"{_qualified_table(schema, 'guard_fhir_formulary_checkpoint_fence')}();"
    )
