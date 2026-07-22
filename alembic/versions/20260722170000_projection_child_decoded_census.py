"""Bind projection child verification to decoded-resource census.

Revision ID: 20260722170000_projection_child_decoded_census
Revises: 20260722160000_plan_release_serving_projection
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260722170000_projection_child_decoded_census"
down_revision = "20260722160000_plan_release_serving_projection"
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


def _quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _rewrite_guard_sql(schema: str, *, upgrade: bool) -> str:
    quoted_schema = _quoted_identifier(schema)
    old_decoded_check = "AND block.record_count = NEW.expected_record_count"
    new_decoded_check = """AND NEW.expected_record_count = COALESCE(
                           (block.summary_json ->> 'resource_count')::bigint,
                           block.record_count
                       )"""
    old_physical_check = """AND NEW.expected_record_count = CASE
                           WHEN binding.retained_range_ordinal IS NULL
                           THEN layout.artifact_record_count
                           ELSE retained_range.record_count
                       END"""
    new_physical_check = """AND block.record_count = CASE
                           WHEN binding.retained_range_ordinal IS NULL
                           THEN layout.artifact_record_count
                           ELSE retained_range.record_count
                       END"""
    if not upgrade:
        old_decoded_check, new_decoded_check = new_decoded_check, old_decoded_check
        old_physical_check, new_physical_check = new_physical_check, old_physical_check
    child_table = f"{quoted_schema}.provider_directory_projection_child_read_lease"
    block_table = f"{quoted_schema}.provider_directory_projection_input_block"
    function_name = (
        f"{quoted_schema}.guard_provider_directory_projection_child_read_lease()"
    )
    function_literal = sa.literal(function_name).compile(
        compile_kwargs={"literal_binds": True}
    )
    expected_count_sql = (
        "COALESCE((block.summary_json ->> 'resource_count')::bigint, "
        "block.record_count)"
        if upgrade
        else "block.record_count"
    )
    return f"""
    DO $migration$
    DECLARE
        current_definition text;
        rewritten_definition text;
    BEGIN
        LOCK TABLE {child_table} IN SHARE ROW EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1
              FROM {child_table} AS child
              JOIN {block_table} AS block
                ON block.recipe_id = child.recipe_id
               AND block.block_id = child.block_id
             WHERE child.expected_record_count <> {expected_count_sql}
        ) THEN
            RAISE EXCEPTION
                'provider_directory_projection_child_census_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;

        SELECT pg_get_functiondef(CAST({function_literal} AS regprocedure))
          INTO current_definition;
        IF strpos(current_definition, {sa.literal(old_decoded_check).compile(compile_kwargs={'literal_binds': True})}) = 0
           OR strpos(current_definition, {sa.literal(old_physical_check).compile(compile_kwargs={'literal_binds': True})}) = 0 THEN
            RAISE EXCEPTION
                'provider_directory_projection_child_guard_contract_unexpected'
                USING ERRCODE = '55000';
        END IF;
        rewritten_definition := replace(
            current_definition,
            {sa.literal(old_decoded_check).compile(compile_kwargs={'literal_binds': True})},
            {sa.literal(new_decoded_check).compile(compile_kwargs={'literal_binds': True})}
        );
        rewritten_definition := replace(
            rewritten_definition,
            {sa.literal(old_physical_check).compile(compile_kwargs={'literal_binds': True})},
            {sa.literal(new_physical_check).compile(compile_kwargs={'literal_binds': True})}
        );
        EXECUTE rewritten_definition;
    END;
    $migration$;
    """


def upgrade() -> None:
    """Adopt decoded resource counts only when existing rows already agree."""

    op.execute(sa.text(_rewrite_guard_sql(_schema(), upgrade=True)))


def downgrade() -> None:
    """Restore physical document counts only when existing rows already agree."""

    op.execute(sa.text(_rewrite_guard_sql(_schema(), upgrade=False)))
