"""Keep endpoint-dataset lifecycle checks independent of large JSON proofs."""

from __future__ import annotations

import os

from alembic import op


revision = "20260807100000_provider_directory_endpoint_dataset_guard"
down_revision = "20260806110000_ptg_import_wave_contract"
branch_labels = None
depends_on = None


ENDPOINT_DATASET_MUTABLE_COLUMNS = (
    "status",
    "is_current",
    "published_at",
    "superseded_at",
    "publication_metadata_json",
)
ENDPOINT_DATASET_IMMUTABLE_COLUMNS = (
    "dataset_id",
    "endpoint_id",
    "import_run_id",
    "acquisition_root_run_id",
    "previous_dataset_id",
    "dataset_hash",
    "resource_count",
    "created_at",
    "validated_at",
)
ENDPOINT_DATASET_FORWARD_COMPATIBLE_COLUMNS = (
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
    "publication_metadata_summary_json",
    "publication_metadata_sha256",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
)


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime or legacy or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qf(schema: str, relation: str) -> str:
    return f"{_q(schema)}.{_q(relation)}"


def _endpoint_dataset_schema_fence_sql(schema: str) -> str:
    expected_legacy_columns = sorted(
        ENDPOINT_DATASET_MUTABLE_COLUMNS + ENDPOINT_DATASET_IMMUTABLE_COLUMNS
    )
    expected_current_columns = sorted(
        ENDPOINT_DATASET_MUTABLE_COLUMNS
        + ENDPOINT_DATASET_IMMUTABLE_COLUMNS
        + ENDPOINT_DATASET_FORWARD_COMPATIBLE_COLUMNS
    )
    legacy_array = ", ".join(
        f"'{column}'" for column in expected_legacy_columns
    )
    current_array = ", ".join(
        f"'{column}'" for column in expected_current_columns
    )
    dataset_ref = _qf(schema, "provider_directory_endpoint_dataset")
    return f"""
    DO $migration$
    DECLARE
        observed_columns text[];
    BEGIN
        SELECT array_agg(attribute.attname ORDER BY attribute.attname)
          INTO observed_columns
         FROM pg_catalog.pg_attribute AS attribute
         WHERE attribute.attrelid = {_ql(dataset_ref)}::regclass
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;
        IF observed_columns IS DISTINCT FROM ARRAY[{legacy_array}]::text[]
           AND observed_columns IS DISTINCT FROM
                ARRAY[{current_array}]::text[] THEN
            RAISE EXCEPTION
                'provider_directory_endpoint_dataset_guard_schema_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


ROW_IMMUTABLE_COMPARISON_SQL = """
                ROW(
                    NEW.dataset_id,
                    NEW.endpoint_id,
                    NEW.import_run_id,
                    NEW.acquisition_root_run_id,
                    NEW.previous_dataset_id,
                    NEW.dataset_hash,
                    NEW.resource_count,
                    NEW.created_at,
                    NEW.validated_at
                ) IS DISTINCT FROM ROW(
                    OLD.dataset_id,
                    OLD.endpoint_id,
                    OLD.import_run_id,
                    OLD.acquisition_root_run_id,
                    OLD.previous_dataset_id,
                    OLD.dataset_hash,
                    OLD.resource_count,
                    OLD.created_at,
                    OLD.validated_at
                )
"""


LEGACY_IMMUTABLE_COMPARISON_SQL = """
                to_jsonb(NEW)
                    - ARRAY[
                        'status',
                        'is_current',
                        'published_at',
                        'superseded_at',
                        'publication_metadata_json'
                    ]
                <>
                to_jsonb(OLD)
                    - ARRAY[
                        'status',
                        'is_current',
                        'published_at',
                        'superseded_at',
                        'publication_metadata_json'
                    ]
"""


def _endpoint_dataset_guard_sql(
    schema: str,
    immutable_comparison_sql: str,
) -> str:
    guard_ref = _qf(schema, "guard_tin_npi_connector_endpoint_dataset")
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.status IN (
                'validated',
                'published',
                'superseded'
            ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF OLD.status NOT IN ('validated', 'published', 'superseded') THEN
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.status IN ('published', 'superseded') THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_delete_forbidden'
                USING ERRCODE = '55000';
        END IF;
        IF (
            {immutable_comparison_sql}
        ) OR (
            OLD.status = 'validated'
            AND NOT (
                (
                    NEW.status = 'validated'
                    AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at
                )
                OR (
                    NEW.status = 'published'
                    AND OLD.is_current IS FALSE
                    AND OLD.validated_at IS NOT NULL
                    AND OLD.published_at IS NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.is_current IS TRUE
                    AND NEW.published_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                    AND NEW.superseded_at IS NULL
                )
            )
        ) OR (
            OLD.status = 'published'
            AND NOT (
                (
                    NEW.status = 'published'
                    AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at
                )
                OR (
                    NEW.status = 'superseded'
                    AND OLD.is_current IS TRUE
                    AND OLD.published_at IS NOT NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.is_current IS FALSE
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT NULL
                    AND NEW.superseded_at >= NEW.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                )
            )
        ) OR (
            OLD.status = 'superseded'
            AND (
                NEW.status <> 'superseded'
                OR NEW.is_current IS NOT DISTINCT FROM TRUE
                OR NEW.is_current IS DISTINCT FROM OLD.is_current
                OR NEW.published_at IS DISTINCT FROM OLD.published_at
                OR NEW.superseded_at IS DISTINCT FROM OLD.superseded_at
            )
        ) THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _revoke_public_execute(schema: str) -> None:
    guard_ref = _qf(schema, "guard_tin_npi_connector_endpoint_dataset")
    op.execute(f"REVOKE ALL ON FUNCTION {guard_ref}() FROM PUBLIC;")


def upgrade() -> None:
    schema = _schema()
    op.execute(_endpoint_dataset_schema_fence_sql(schema))
    op.execute(
        _endpoint_dataset_guard_sql(schema, ROW_IMMUTABLE_COMPARISON_SQL)
    )
    _revoke_public_execute(schema)


def downgrade() -> None:
    schema = _schema()
    op.execute(
        _endpoint_dataset_guard_sql(schema, LEGACY_IMMUTABLE_COMPARISON_SQL)
    )
    _revoke_public_execute(schema)
