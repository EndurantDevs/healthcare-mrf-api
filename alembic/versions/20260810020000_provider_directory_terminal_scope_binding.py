# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair reviewed-subset terminal scope and serial diagnostic binding.

Revision ID: 20260810020000_provider_directory_terminal_scope_binding
Revises: 20260810010000_provider_directory_reviewed_subset_terminal_disposition
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260810020000_provider_directory_terminal_scope_binding"
down_revision = (
    "20260810010000_provider_directory_reviewed_subset_terminal_disposition"
)
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260810010000_provider_directory_reviewed_subset_terminal_disposition.py"
)
_SERIAL_CONCURRENCY_FIELDS = (
    "resource_scan_concurrency_requested",
    "resource_scan_concurrency_effective",
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_terminal_scope_binding_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(
            "provider directory terminal disposition revision is unavailable"
        )
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _replace_exact(
    source: str,
    needle: str,
    replacement: str,
    *,
    expected_count: int = 1,
) -> str:
    if source.count(needle) != expected_count:
        raise RuntimeError(
            "provider directory terminal disposition renderer changed"
        )
    return source.replace(needle, replacement)


def _create_or_replace_valid_sql(sql: str, schema: str) -> str:
    predecessor = _predecessor()
    subset = predecessor._subset()
    valid_ref = subset._qf(schema, predecessor._VALID)
    return _replace_exact(
        sql,
        f"CREATE FUNCTION {valid_ref}(candidate_dataset_id text)",
        f"CREATE OR REPLACE FUNCTION {valid_ref}(candidate_dataset_id text)",
    )


def _diagnostic_fields() -> tuple[str, ...]:
    predecessor = _predecessor()
    return tuple(
        sorted(
            (
                *predecessor._DIAGNOSTIC_FIELDS,
                *_SERIAL_CONCURRENCY_FIELDS,
            )
        )
    )


def _serial_concurrency_invalid_sql(diagnostic_sql: str) -> str:
    predecessor = _predecessor()
    subset = predecessor._subset()
    return " OR ".join(
        "NOT ("
        + predecessor._number_sql(
            diagnostic_sql + " -> " + subset._ql(field_name)
        )
        + ") OR "
        + diagnostic_sql
        + " -> "
        + subset._ql(field_name)
        + " #>> '{}' IS DISTINCT FROM '1'"
        for field_name in _SERIAL_CONCURRENCY_FIELDS
    )


def _valid_function_sql(schema: str) -> str:
    """Replace the stable validator while preserving its database identity."""

    predecessor = _predecessor()
    old_fields = predecessor._json_fields_sql(
        predecessor._DIAGNOSTIC_FIELDS
    )
    new_fields = predecessor._json_fields_sql(_diagnostic_fields())
    fetch_mode_check = (
        "OR diagnostic ->> 'fetch_mode' <> "
        + predecessor._subset()._ql(predecessor._FETCH_MODE)
    )
    serial_checks = _serial_concurrency_invalid_sql("diagnostic")
    sql = predecessor._valid_function_sql(schema)
    sql = _replace_exact(
        sql,
        old_fields,
        new_fields,
        expected_count=2,
    )
    sql = _replace_exact(
        sql,
        fetch_mode_check,
        fetch_mode_check + "\n               OR " + serial_checks,
    )
    return _create_or_replace_valid_sql(sql, schema)


def _transition_diagnostic_sql() -> str:
    predecessor = _predecessor()
    subset = predecessor._subset()
    fields = predecessor._json_fields_sql(_diagnostic_fields())
    resources = predecessor._resource_array_sql()
    diagnostics = (
        "NEW.publication_metadata_json::jsonb -> 'resource_diagnostics'"
    )
    serial_checks = _serial_concurrency_invalid_sql(
        "observed_diagnostic.diagnostic"
    )
    return f"""
                       AND pg_catalog.jsonb_typeof({diagnostics}) = 'object'
                       AND ({diagnostics}) ?& {resources}
                       AND ({diagnostics}) - {resources} = '{{}}'::jsonb
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_catalog.jsonb_each(
                                   CASE
                                       WHEN pg_catalog.jsonb_typeof(
                                            {diagnostics}
                                       ) = 'object'
                                       THEN {diagnostics}
                                       ELSE '{{}}'::jsonb
                                   END
                              ) AS observed_diagnostic(
                                   resource_type, diagnostic
                              )
                             WHERE pg_catalog.jsonb_typeof(
                                      observed_diagnostic.diagnostic
                                   ) IS DISTINCT FROM 'object'
                                OR NOT (
                                     observed_diagnostic.diagnostic
                                         ?& ARRAY[{fields}]::text[]
                                   )
                                OR observed_diagnostic.diagnostic
                                     - ARRAY[{fields}]::text[]
                                   <> '{{}}'::jsonb
                                OR {serial_checks}
                       )
    """


def _dataset_guard_sql(schema: str) -> str:
    """Bind the transition to verification scope and exact serial evidence."""

    predecessor = _predecessor()
    marker_scope_target = f"""NEW.publication_metadata_json::jsonb
                              #>> '{{{predecessor._MARKER},source_scope_sha256}}'"""
    verification_scope_target = """NEW.publication_metadata_json::jsonb
                              ->> 'verification_source_scope_hash'"""
    source_copy_tail = """                       AND source.metadata_json::jsonb
                              #> '{last_resource_import,resources}'
                           = NEW.publication_metadata_json::jsonb
                              #> '{completion_proof_v1,resource_diagnostics}'
                       AND NULLIF(source.canonical_api_base, '') IS NOT NULL"""
    source_copy_with_diagnostics = (
        source_copy_tail.rsplit(
            "                       AND NULLIF(source.canonical_api_base, '') IS NOT NULL",
            1,
        )[0]
        + _transition_diagnostic_sql()
        + "                       AND NULLIF(source.canonical_api_base, '') IS NOT NULL"
    )
    sql = predecessor._dataset_guard_sql(schema)
    sql = _replace_exact(
        sql,
        marker_scope_target,
        verification_scope_target,
    )
    return _replace_exact(
        sql,
        source_copy_tail,
        source_copy_with_diagnostics,
    )


def _evidence_fence_sql(schema: str) -> str:
    predecessor = _predecessor()
    subset = predecessor._subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             WHERE dataset.publication_metadata_json::jsonb
                       ? '{predecessor._MARKER}'
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_scope_binding_evidence_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _transition_lock_sql(schema: str) -> str:
    predecessor = _predecessor()
    subset = predecessor._subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    return f"""
    DO $migration$
    DECLARE
        attempt integer;
    BEGIN
        FOR attempt IN 1..150 LOOP
            BEGIN
                LOCK TABLE {dataset_ref}
                    IN SHARE ROW EXCLUSIVE MODE NOWAIT;
                RETURN;
            EXCEPTION WHEN lock_not_available THEN
                IF attempt = 150 THEN
                    RAISE EXCEPTION
                        'provider_directory_terminal_scope_binding_lock_unavailable'
                        USING ERRCODE = '55P03';
                END IF;
                PERFORM pg_catalog.pg_sleep(0.2);
            END;
        END LOOP;
    END;
    $migration$;
    """


def _shape_fences(schema: str) -> None:
    predecessor = _predecessor()
    original_op = predecessor.op
    try:
        predecessor.op = op
        predecessor._base_shape_fences(schema)
        op.execute(
            predecessor._new_object_shape_fence_sql(
                schema,
                expect_installed=True,
            )
        )
    finally:
        predecessor.op = original_op


def upgrade() -> None:
    predecessor = _predecessor()
    schema = predecessor._subset()._schema()
    op.execute(_transition_lock_sql(schema))
    _shape_fences(schema)
    op.execute(_evidence_fence_sql(schema))
    op.execute(_valid_function_sql(schema))
    op.execute(_dataset_guard_sql(schema))
    _shape_fences(schema)


def downgrade() -> None:
    predecessor = _predecessor()
    schema = predecessor._subset()._schema()
    op.execute(_transition_lock_sql(schema))
    _shape_fences(schema)
    op.execute(_evidence_fence_sql(schema))
    op.execute(
        _create_or_replace_valid_sql(
            predecessor._valid_function_sql(schema),
            schema,
        )
    )
    op.execute(predecessor._dataset_guard_sql(schema))
    _shape_fences(schema)
