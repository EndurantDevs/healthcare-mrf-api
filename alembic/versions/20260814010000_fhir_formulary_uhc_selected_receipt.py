# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind UHC receipts to an immutable selected artifact subset.

Revision ID: 20260814010000_fhir_formulary_uhc_selected_receipt
Revises: 20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
import os
from pathlib import Path
import re
from types import ModuleType

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260814010000_fhir_formulary_uhc_selected_receipt"
down_revision = (
    "20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance"
)
branch_labels = None
depends_on = None


_TABLE = "fhir_formulary_uhc_admission_receipt"
_GUARD = "guard_fhir_formulary_uhc_admission_receipt"
_TRIGGER = "fhir_formulary_uhc_admission_receipt_guard"
_SELECTION_HASH = "fhir_formulary_source_artifact_selection_sha256"
_LEGACY_MIGRATION = "20260810040000_fhir_formulary_uhc_admission_receipt.py"


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


@lru_cache(maxsize=1)
def _legacy() -> ModuleType:
    path = Path(__file__).with_name(_LEGACY_MIGRATION)
    module_spec = importlib.util.spec_from_file_location(
        "_fhir_formulary_uhc_legacy_receipt",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("UHC receipt legacy migration is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _selection_hash_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _SELECTION_HASH)
    artifact_ref = _qualified(schema, "fhir_formulary_source_artifact")
    return f"""
    CREATE FUNCTION {function_ref}(
        candidate_source_id text,
        candidate_source_file_set_sha256 text,
        candidate_selected_source_file_ids character varying[]
    )
    RETURNS text
    LANGUAGE sql
    STABLE
    STRICT
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT CASE
            WHEN pg_catalog.count(*) = 0
              OR pg_catalog.count(*) <>
                 pg_catalog.cardinality(candidate_selected_source_file_ids)
              OR pg_catalog.count(*) FILTER (
                     WHERE artifact.status = 'verified'
                 ) <> pg_catalog.count(*)
            THEN NULL
            ELSE pg_catalog.encode(
                pg_catalog.sha256(
                    pg_catalog.convert_to(
                        'fhir-formulary-source-artifact-set-v1'
                        || pg_catalog.chr(10)
                        || '['
                        || pg_catalog.string_agg(
                            '{{"artifact_byte_count":'
                            || artifact.artifact_byte_count::text
                            || ',"artifact_sha256":'
                            || pg_catalog.to_json(
                                artifact.artifact_sha256
                            )::text
                            || ',"catalog_entry_sha256":'
                            || pg_catalog.to_json(
                                artifact.catalog_entry_sha256
                            )::text
                            || ',"catalog_modified_at":'
                            || pg_catalog.to_json(
                                artifact.catalog_modified_at
                            )::text
                            || ',"expected_byte_count":'
                            || CASE
                                WHEN artifact.expected_byte_count IS NULL
                                THEN 'null'
                                ELSE artifact.expected_byte_count::text
                               END
                            || ',"family":'
                            || pg_catalog.to_json(artifact.family)::text
                            || ',"file_name":'
                            || pg_catalog.to_json(artifact.file_name)::text
                            || ',"raw_listing_projection_sha256":'
                            || pg_catalog.to_json(
                                artifact.raw_listing_projection_sha256
                            )::text
                            || ',"source_file_id":'
                            || pg_catalog.to_json(
                                artifact.source_file_id
                            )::text
                            || ',"source_file_set_sha256":'
                            || pg_catalog.to_json(
                                artifact.source_file_set_sha256
                            )::text
                            || ',"source_id":'
                            || pg_catalog.to_json(artifact.source_id)::text
                            || ',"source_url":'
                            || pg_catalog.to_json(artifact.source_url)::text
                            || '}}',
                            ',' ORDER BY
                                pg_catalog.convert_to(
                                    artifact.family,
                                    'UTF8'
                                ),
                                pg_catalog.convert_to(
                                    artifact.file_name,
                                    'UTF8'
                                ),
                                pg_catalog.convert_to(
                                    artifact.source_file_id,
                                    'UTF8'
                                )
                        )
                        || ']',
                        'UTF8'
                    )
                ),
                'hex'
            )
        END
        FROM {artifact_ref} AS artifact
        WHERE artifact.source_id = candidate_source_id
          AND artifact.source_file_set_sha256 =
              candidate_source_file_set_sha256
          AND artifact.source_file_id = ANY (
              candidate_selected_source_file_ids
          );
    $function$;
    """


def _guard_function_sql(schema: str) -> str:
    function_ref = _qualified(schema, _GUARD)
    observation_ref = _qualified(
        schema,
        "fhir_formulary_source_artifact_observation",
    )
    set_ref = _qualified(schema, "fhir_formulary_source_artifact_set")
    artifact_ref = _qualified(schema, "fhir_formulary_source_artifact")
    admission_ref = _qualified(schema, "fhir_formulary_twin_admission")
    full_hash_ref = _qualified(
        schema,
        "fhir_formulary_source_artifact_set_sha256",
    )
    selection_hash_ref = _qualified(schema, _SELECTION_HASH)
    return f"""
    CREATE OR REPLACE FUNCTION {function_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $guard$
    DECLARE
        observed_set_sha256 text;
        expected_file_count integer;
        artifact_count bigint;
        cs_artifact_count bigint;
        ifp_artifact_count bigint;
        canonical_source_file_ids character varying[];
        selected_artifact_count bigint;
        selected_verified_artifact_count bigint;
        canonical_selected_source_file_ids character varying[];
        max_selected_artifact_verified_at timestamptz;
        observed_artifact_set_sha256 text;
        expected_receipt_id text;
        receipt_identity text;
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
        SELECT pg_catalog.count(*),
               pg_catalog.count(*) FILTER (WHERE artifact.family = 'cs'),
               pg_catalog.count(*) FILTER (WHERE artifact.family = 'ifp'),
               pg_catalog.array_agg(
                   artifact.source_file_id ORDER BY
                       pg_catalog.convert_to(artifact.family, 'UTF8'),
                       pg_catalog.convert_to(artifact.file_name, 'UTF8'),
                       pg_catalog.convert_to(artifact.source_file_id, 'UTF8')
               )
          INTO artifact_count,
               cs_artifact_count,
               ifp_artifact_count,
               canonical_source_file_ids
          FROM {artifact_ref} AS artifact
         WHERE artifact.source_id = NEW.source_id
           AND artifact.source_file_set_sha256 = NEW.source_file_set_sha256;
        IF NEW.selected_source_file_ids IS NULL
           AND NEW.file_count = 48
           AND NEW.expected_file_count = 48
           AND NEW.excluded_file_count = 0
           AND NEW.exclusion_code IS NULL THEN
            NEW.selected_source_file_ids := canonical_source_file_ids;
        END IF;
        SELECT pg_catalog.count(*),
               pg_catalog.count(*) FILTER (
                   WHERE artifact.status = 'verified'
               ),
               pg_catalog.array_agg(
                   artifact.source_file_id ORDER BY
                       pg_catalog.convert_to(artifact.family, 'UTF8'),
                       pg_catalog.convert_to(artifact.file_name, 'UTF8'),
                       pg_catalog.convert_to(artifact.source_file_id, 'UTF8')
               ),
               pg_catalog.max(artifact.verified_at)
          INTO selected_artifact_count,
               selected_verified_artifact_count,
               canonical_selected_source_file_ids,
               max_selected_artifact_verified_at
          FROM {artifact_ref} AS artifact
         WHERE artifact.source_id = NEW.source_id
           AND artifact.source_file_set_sha256 = NEW.source_file_set_sha256
           AND artifact.source_file_id = ANY (NEW.selected_source_file_ids);
        IF observed_set_sha256 IS DISTINCT FROM NEW.source_file_set_sha256
           OR expected_file_count IS DISTINCT FROM 48
           OR NEW.expected_file_count IS DISTINCT FROM 48
           OR artifact_count IS DISTINCT FROM 48
           OR cs_artifact_count IS DISTINCT FROM 24
           OR ifp_artifact_count IS DISTINCT FROM 24
           OR selected_artifact_count IS DISTINCT FROM NEW.file_count::bigint
           OR selected_verified_artifact_count IS DISTINCT FROM
              NEW.file_count::bigint
           OR canonical_selected_source_file_ids IS DISTINCT FROM
              NEW.selected_source_file_ids
           OR admission_list_count IS DISTINCT FROM NEW.plan_count
           OR admission_alias_count IS DISTINCT FROM NEW.plan_count
           OR admission_medication_count IS DISTINCT FROM
              NEW.medication_membership_count
           OR max_selected_artifact_verified_at > admission_cutoff_at
           OR NEW.max_last_updated_at > admission_cutoff_at
           OR NEW.recorded_at < admission_admitted_at THEN
            RAISE EXCEPTION 'fhir_formulary_uhc_admission_receipt_invalid'
                USING ERRCODE = '23514';
        END IF;
        IF NEW.file_count = 48 THEN
            observed_artifact_set_sha256 :=
                {full_hash_ref}(
                    NEW.source_id,
                    NEW.source_file_set_sha256
                );
        ELSE
            observed_artifact_set_sha256 :=
                {selection_hash_ref}(
                    NEW.source_id,
                    NEW.source_file_set_sha256,
                    NEW.selected_source_file_ids
                );
        END IF;
        receipt_identity :=
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
            || NEW.spool_content_sha256;
        IF NEW.file_count < 48 THEN
            receipt_identity :=
                receipt_identity
                || pg_catalog.chr(31)
                || pg_catalog.array_to_string(
                    NEW.selected_source_file_ids,
                    ','
                )
                || pg_catalog.chr(31)
                || NEW.exclusion_code;
        END IF;
        expected_receipt_id :=
            'ffur_' || pg_catalog.substr(
                pg_catalog.encode(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(receipt_identity, 'UTF8')
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


def _values_check() -> str:
    return (
        "receipt_id ~ '^ffur_[0-9a-f]{48}$' AND "
        "source_id = 'uhc-official-formulary-mrf' AND "
        "source_observation_sha256 ~ '^[0-9a-f]{64}$' AND "
        "source_file_set_sha256 ~ '^[0-9a-f]{64}$' AND "
        "artifact_set_sha256 ~ '^[0-9a-f]{64}$' AND "
        "spool_content_sha256 ~ '^[0-9a-f]{64}$' AND "
        "expected_file_count = 48 AND file_count BETWEEN 1 AND 48 AND "
        "excluded_file_count = expected_file_count - file_count AND "
        "cardinality(selected_source_file_ids) = file_count AND "
        "((excluded_file_count = 0 AND exclusion_code IS NULL) OR "
        "(excluded_file_count > 0 AND "
        "exclusion_code = 'not_selected')) AND "
        "raw_record_count > 0 AND raw_plan_entry_count > 0 AND "
        "plan_count > 0 AND medication_membership_count > 0 AND "
        "duplicate_count >= 0 AND superseded_count >= 0 AND "
        "isfinite(max_last_updated_at) AND "
        "max_last_updated_at >= TIMESTAMPTZ '2000-01-01 00:00:00+00' "
        "AND max_last_updated_at < "
        "TIMESTAMPTZ '2101-01-01 00:00:00+00'"
    )


def _legacy_values_check() -> str:
    return (
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
        "TIMESTAMPTZ '2101-01-01 00:00:00+00'"
    )


def _drop_guard_statements(schema: str) -> tuple[str, ...]:
    table_ref = _qualified(schema, _TABLE)
    return (
        f"DROP TRIGGER IF EXISTS {_quoted(_TRIGGER)} ON {table_ref};",
        f"DROP TRIGGER IF EXISTS {_quoted(_TRIGGER + '_truncate')} "
        f"ON {table_ref};",
    )


def _install_guard_statements(schema: str) -> tuple[str, ...]:
    function_ref = _qualified(schema, _GUARD)
    selection_hash_ref = _qualified(schema, _SELECTION_HASH)
    table_ref = _qualified(schema, _TABLE)
    return (
        f"REVOKE ALL ON FUNCTION {function_ref}() FROM PUBLIC;",
        "REVOKE ALL ON FUNCTION "
        f"{selection_hash_ref}(text, text, character varying[]) FROM PUBLIC;",
        f"REVOKE ALL ON TABLE {table_ref} FROM PUBLIC;",
        f"CREATE TRIGGER {_quoted(_TRIGGER)} BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {table_ref} FOR EACH ROW EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER {_quoted(_TRIGGER)};",
        f"CREATE TRIGGER {_quoted(_TRIGGER + '_truncate')} BEFORE TRUNCATE "
        f"ON {table_ref} FOR EACH STATEMENT EXECUTE FUNCTION {function_ref}();",
        f"ALTER TABLE {table_ref} ENABLE ALWAYS TRIGGER "
        f"{_quoted(_TRIGGER + '_truncate')};",
    )


def _backfill_sql(schema: str) -> str:
    receipt_ref = _qualified(schema, _TABLE)
    artifact_ref = _qualified(schema, "fhir_formulary_source_artifact")
    return f"""
    UPDATE {receipt_ref} AS receipt
       SET selected_source_file_ids = (
           SELECT pg_catalog.array_agg(
               artifact.source_file_id ORDER BY
                   pg_catalog.convert_to(artifact.family, 'UTF8'),
                   pg_catalog.convert_to(artifact.file_name, 'UTF8'),
                   pg_catalog.convert_to(artifact.source_file_id, 'UTF8')
           )
             FROM {artifact_ref} AS artifact
            WHERE artifact.source_id = receipt.source_id
              AND artifact.source_file_set_sha256 =
                  receipt.source_file_set_sha256
       )
     WHERE receipt.selected_source_file_ids IS NULL;
    """


def _backfill_validation_sql(schema: str) -> str:
    receipt_ref = _qualified(schema, _TABLE)
    return f"""
    DO $backfill$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {receipt_ref} AS receipt
             WHERE receipt.file_count <> 48
                OR receipt.expected_file_count <> 48
                OR receipt.excluded_file_count <> 0
                OR receipt.exclusion_code IS NOT NULL
                OR pg_catalog.cardinality(
                       receipt.selected_source_file_ids
                   ) <> 48
        ) THEN
            RAISE EXCEPTION
                'fhir_formulary_uhc_selected_receipt_backfill_invalid'
                USING ERRCODE = '23514';
        END IF;
    END;
    $backfill$;
    """


def _partial_downgrade_fence_sql(schema: str) -> str:
    table_ref = _qualified(schema, _TABLE)
    return f"""
    DO $downgrade$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {table_ref}
             WHERE excluded_file_count > 0
        ) THEN
            RAISE EXCEPTION
                'fhir_formulary_uhc_selected_receipt_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $downgrade$;
    """


def upgrade() -> None:
    schema = _schema()
    table_ref = _qualified(schema, _TABLE)
    op.execute(f"LOCK TABLE {table_ref} IN ACCESS EXCLUSIVE MODE;")
    op.add_column(
        _TABLE,
        sa.Column(
            "expected_file_count",
            sa.Integer(),
            server_default=sa.text("48"),
            nullable=False,
        ),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column(
            "excluded_file_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column(
            "selected_source_file_ids",
            postgresql.ARRAY(sa.String(length=64)),
            nullable=True,
        ),
        schema=schema,
    )
    op.add_column(
        _TABLE,
        sa.Column("exclusion_code", sa.String(length=32), nullable=True),
        schema=schema,
    )
    for statement in _drop_guard_statements(schema):
        op.execute(statement)
    op.execute(_backfill_sql(schema))
    op.execute(_backfill_validation_sql(schema))
    op.alter_column(
        _TABLE,
        "selected_source_file_ids",
        existing_type=postgresql.ARRAY(sa.String(length=64)),
        nullable=False,
        schema=schema,
    )
    op.drop_constraint(
        "fhir_formulary_uhc_admission_receipt_values_check",
        _TABLE,
        schema=schema,
        type_="check",
    )
    op.create_check_constraint(
        "fhir_formulary_uhc_admission_receipt_values_check",
        _TABLE,
        _values_check(),
        schema=schema,
    )
    op.execute(_selection_hash_function_sql(schema))
    op.execute(_guard_function_sql(schema))
    for statement in _install_guard_statements(schema):
        op.execute(statement)


def downgrade() -> None:
    schema = _schema()
    table_ref = _qualified(schema, _TABLE)
    op.execute(f"LOCK TABLE {table_ref} IN ACCESS EXCLUSIVE MODE;")
    op.execute(_partial_downgrade_fence_sql(schema))
    for statement in _drop_guard_statements(schema):
        op.execute(statement)
    legacy_guard_sql = _legacy()._guard_function_sql(schema).replace(
        "CREATE FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        1,
    )
    op.execute(legacy_guard_sql)
    op.drop_constraint(
        "fhir_formulary_uhc_admission_receipt_values_check",
        _TABLE,
        schema=schema,
        type_="check",
    )
    op.create_check_constraint(
        "fhir_formulary_uhc_admission_receipt_values_check",
        _TABLE,
        _legacy_values_check(),
        schema=schema,
    )
    for column_name in (
        "exclusion_code",
        "selected_source_file_ids",
        "excluded_file_count",
        "expected_file_count",
    ):
        op.drop_column(_TABLE, column_name, schema=schema)
    op.execute(
        f"DROP FUNCTION {_qualified(schema, _SELECTION_HASH)}"
        "(text, text, character varying[]);"
    )
    for statement in _legacy()._guard_install_statements(schema):
        op.execute(statement)
