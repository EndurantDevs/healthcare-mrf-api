"""Permit one advertised-count tail page for reviewed subset proofs.

Revision ID: 20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance
Revises: 20260813010000_provider_directory_observed_npi_index
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance"
)
down_revision = "20260813010000_provider_directory_observed_npi_index"
branch_labels = None
depends_on = None


_TERMINAL_WINDOW_FILE = (
    "20260810130000_provider_directory_reviewed_subset_terminal_window.py"
)
_STRICT_TERMINAL_BOUND = """OR advertised_pre >
                            (resource_value ->>
                                'logical_window_end_offset')::numeric"""
_TAIL_TOLERANT_BOUND = """OR advertised_pre >=
                            (resource_value ->
                                'logical_window_end_offset')::numeric
                            + page_count"""


@lru_cache(maxsize=1)
def _terminal() -> ModuleType:
    path = Path(__file__).with_name(_TERMINAL_WINDOW_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_reviewed_subset_terminal_tail_tolerance",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory terminal window revision is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _proof_shape_valid_function_sql(schema: str, *, tail_tolerant: bool) -> str:
    """Render the installed v5 proof function with its one-page tail rule."""

    subset = _terminal()._subset()
    sql = subset._proof_shape_valid_function_sql(
        schema,
        replace_existing=True,
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
    )
    if not tail_tolerant:
        return sql
    if sql.count(_STRICT_TERMINAL_BOUND) != 1:
        raise RuntimeError("provider_directory_terminal_window_renderer_changed")
    return sql.replace(_STRICT_TERMINAL_BOUND, _TAIL_TOLERANT_BOUND)


def _function_shape_fence_sql(schema: str, *, tail_tolerant: bool) -> str:
    subset = _terminal()._subset()
    function_signature = (
        subset._qf(schema, subset._PROOF_SHAPE_VALID_FUNCTION)
        + "(jsonb,text,bigint)"
    )
    expected = _TAIL_TOLERANT_BOUND if tail_tolerant else _STRICT_TERMINAL_BOUND
    forbidden = _STRICT_TERMINAL_BOUND if tail_tolerant else _TAIL_TOLERANT_BOUND
    return f"""
    DO $migration$
    DECLARE
        definition text;
    BEGIN
        SELECT pg_catalog.pg_get_functiondef(
            {subset._ql(function_signature)}::regprocedure
        ) INTO definition;
        IF definition IS NULL
           OR pg_catalog.strpos(definition, {subset._ql(expected)}) = 0
           OR pg_catalog.strpos(definition, {subset._ql(forbidden)}) <> 0 THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_terminal_tail_shape_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _downgrade_evidence_fence_sql(schema: str) -> str:
    terminal = _terminal()
    subset = terminal._subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    strategy_version = subset._ql(terminal._TERMINAL_WINDOW_STRATEGY_VERSION)
    completion_scopes = subset._ql(
        terminal._TERMINAL_WINDOW_COMPLETION_SCOPES_JSON
    )
    numeric = "'^[0-9]+$'"
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             CROSS JOIN LATERAL pg_catalog.jsonb_each(
                 CASE
                     WHEN pg_catalog.jsonb_typeof(
                         dataset.completion_proof_json -> 'resources'
                     ) = 'object' THEN dataset.completion_proof_json -> 'resources'
                     ELSE '{{}}'::jsonb
                 END
             ) AS resource_entry(resource_type, resource_value)
             WHERE dataset.completion_proof_json ->> 'strategy_version' =
                    {strategy_version}
               AND dataset.completion_proof_json -> 'completion_scopes' =
                    {completion_scopes}::jsonb
               AND pg_catalog.jsonb_typeof(resource_entry.resource_value) =
                    'object'
               AND resource_entry.resource_value ->> 'advertised_pre' ~ {numeric}
               AND resource_entry.resource_value ->>
                    'logical_window_end_offset' ~ {numeric}
               AND resource_entry.resource_value ->> 'page_count' ~ {numeric}
               AND (resource_entry.resource_value ->>
                    'advertised_pre')::numeric >
                    (resource_entry.resource_value ->>
                    'logical_window_end_offset')::numeric
               AND (resource_entry.resource_value ->>
                    'advertised_pre')::numeric <
                    (resource_entry.resource_value ->>
                    'logical_window_end_offset')::numeric +
                    (resource_entry.resource_value ->> 'page_count')::numeric
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_terminal_tail_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def upgrade() -> None:
    terminal = _terminal()
    schema = terminal._subset()._schema()
    terminal._lock_relations(schema)
    op.execute(_function_shape_fence_sql(schema, tail_tolerant=False))
    op.execute(_proof_shape_valid_function_sql(schema, tail_tolerant=True))
    terminal._revoke_execute(schema)
    op.execute(_function_shape_fence_sql(schema, tail_tolerant=True))


def downgrade() -> None:
    terminal = _terminal()
    schema = terminal._subset()._schema()
    terminal._lock_relations(schema)
    op.execute(_function_shape_fence_sql(schema, tail_tolerant=True))
    op.execute(_downgrade_evidence_fence_sql(schema))
    op.execute(_proof_shape_valid_function_sql(schema, tail_tolerant=False))
    terminal._revoke_execute(schema)
    op.execute(_function_shape_fence_sql(schema, tail_tolerant=False))
