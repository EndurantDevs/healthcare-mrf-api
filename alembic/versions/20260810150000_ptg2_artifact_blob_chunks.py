"""Make PTG2 artifact chunks migration-owned.

Revision ID: 20260810150000_ptg2_artifact_blob_chunks
Revises: 20260810140000_ptg2_plan_catalog_outbox
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260810150000_ptg2_artifact_blob_chunks"
down_revision = "20260810140000_ptg2_plan_catalog_outbox"
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


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def upgrade() -> None:
    schema = _schema()
    table = "ptg2_artifact_blob_chunk"
    qualified_table = _qt(schema, table)
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            artifact_id varchar(96) NOT NULL,
            chunk_no integer NOT NULL,
            compression varchar(32),
            payload bytea NOT NULL,
            raw_byte_count integer NOT NULL,
            byte_count integer NOT NULL,
            created_at timestamp,
            CONSTRAINT {_q(f"{table}_pkey")}
                PRIMARY KEY (artifact_id, chunk_no)
        )
        """
    )
    op.execute(
        f"""
        DO $$
        DECLARE
            actual_shape text[];
        BEGIN
            SELECT array_agg(
                       column_name || ':' || udt_name || ':' || is_nullable
                       ORDER BY ordinal_position
                   )
              INTO actual_shape
              FROM information_schema.columns
             WHERE table_schema = {_lit(schema)}
               AND table_name = {_lit(table)};
            IF actual_shape IS DISTINCT FROM ARRAY[
                'artifact_id:varchar:NO',
                'chunk_no:int4:NO',
                'compression:varchar:YES',
                'payload:bytea:NO',
                'raw_byte_count:int4:NO',
                'byte_count:int4:NO',
                'created_at:timestamp:YES'
            ]::text[] THEN
                RAISE EXCEPTION
                    'ptg2_artifact_blob_chunk has incompatible shape: %',
                    actual_shape;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_constraint AS constraint_record
                  JOIN pg_class AS table_record
                    ON table_record.oid = constraint_record.conrelid
                  JOIN pg_namespace AS namespace_record
                    ON namespace_record.oid = table_record.relnamespace
                 WHERE namespace_record.nspname = {_lit(schema)}
                   AND table_record.relname = {_lit(table)}
                   AND constraint_record.contype = 'p'
                   AND pg_get_constraintdef(constraint_record.oid) =
                       'PRIMARY KEY (artifact_id, chunk_no)'
            ) THEN
                RAISE EXCEPTION
                    'ptg2_artifact_blob_chunk requires its exact primary key';
            END IF;
        END
        $$
        """
    )
    op.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {_q('ptg2_artifact_blob_artifact_idx')}
            ON {qualified_table} (artifact_id)
        """
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM pg_index AS index_record
                  JOIN pg_class AS index_relation
                    ON index_relation.oid = index_record.indexrelid
                  JOIN pg_class AS table_relation
                    ON table_relation.oid = index_record.indrelid
                  JOIN pg_namespace AS namespace_record
                    ON namespace_record.oid = table_relation.relnamespace
                 WHERE namespace_record.nspname = {_lit(schema)}
                   AND table_relation.relname = {_lit(table)}
                   AND index_relation.relname =
                       'ptg2_artifact_blob_artifact_idx'
                   AND index_record.indisvalid
                   AND index_record.indpred IS NULL
                   AND index_record.indexprs IS NULL
                   AND ARRAY(
                       SELECT attribute_record.attname
                         FROM unnest(index_record.indkey::smallint[])
                              WITH ORDINALITY AS key_record(attnum, ordinal)
                         JOIN pg_attribute AS attribute_record
                           ON attribute_record.attrelid = table_relation.oid
                          AND attribute_record.attnum = key_record.attnum
                        WHERE key_record.attnum > 0
                        ORDER BY key_record.ordinal
                   ) = ARRAY['artifact_id']::name[]
            ) THEN
                RAISE EXCEPTION
                    'ptg2_artifact_blob_artifact_idx has incompatible shape';
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    schema = _schema()
    table = "ptg2_artifact_blob_chunk"
    op.execute(
        f"""
        DO $$
        BEGIN
            IF to_regclass('{_qt(schema, table)}') IS NOT NULL
               AND EXISTS (SELECT 1 FROM {_qt(schema, table)} LIMIT 1)
            THEN
                RAISE EXCEPTION
                    'refusing to downgrade nonempty PTG artifact chunks';
            END IF;
        END
        $$
        """
    )
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)}")
