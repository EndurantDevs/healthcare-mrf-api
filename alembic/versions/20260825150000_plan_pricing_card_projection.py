"""Add immutable plan-pricing card and aggregate projections.

Revision ID: 20260825150000_plan_pricing_card_projection
Revises: 20260825120000_ptg_v4_finalizer_map_pack
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text


revision = "20260825150000_plan_pricing_card_projection"
down_revision = "20260825120000_ptg_v4_finalizer_map_pack"
branch_labels = None
depends_on = None


ZIP_INDEX_NAME = "plan_pricing_geo_zip_coordinates_idx"
ZIP_TABLE_NAME = "geo_zip_lookup"
IDEMPOTENCY_INDEX_NAME = "import_run_plan_pricing_idempotency_idx"


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _zip_table_has_index_columns(schema: str) -> bool:
    return bool(
        op.get_bind().execute(
            text(
                """
                SELECT COUNT(*) = 3
                  FROM pg_catalog.pg_class AS table_record
                  JOIN pg_catalog.pg_namespace AS table_namespace
                    ON table_namespace.oid = table_record.relnamespace
                  JOIN pg_catalog.pg_attribute AS column_record
                    ON column_record.attrelid = table_record.oid
                 WHERE table_namespace.nspname = :schema
                   AND table_record.relname = :table_name
                   AND table_record.relkind IN ('r', 'p')
                   AND column_record.attnum > 0
                   AND NOT column_record.attisdropped
                   AND column_record.attname IN (
                           'latitude', 'longitude', 'zip_code'
                       )
                """
            ),
            {"schema": schema, "table_name": ZIP_TABLE_NAME},
        ).scalar()
    )


def _zip_index_record(schema: str):
    return op.get_bind().execute(
        text(
            """
            SELECT table_namespace.nspname AS table_schema,
                   table_record.relname AS table_name,
                   index_record.indisvalid AS is_valid
              FROM pg_catalog.pg_index AS index_record
              JOIN pg_catalog.pg_class AS index_class
                ON index_class.oid = index_record.indexrelid
              JOIN pg_catalog.pg_namespace AS index_namespace
                ON index_namespace.oid = index_class.relnamespace
              JOIN pg_catalog.pg_class AS table_record
                ON table_record.oid = index_record.indrelid
              JOIN pg_catalog.pg_namespace AS table_namespace
                ON table_namespace.oid = table_record.relnamespace
             WHERE index_namespace.nspname = :schema
               AND index_class.relname = :index_name
            """
        ),
        {"schema": schema, "index_name": ZIP_INDEX_NAME},
    ).mappings().one_or_none()


def _create_zip_index(schema: str) -> None:
    context = op.get_context()
    if context.as_sql or not _zip_table_has_index_columns(schema):
        return
    index_record = _zip_index_record(schema)
    drop_invalid_index = False
    if index_record is not None:
        if (
            str(index_record["table_schema"]),
            str(index_record["table_name"]),
        ) != (schema, ZIP_TABLE_NAME):
            raise RuntimeError(
                f"existing_schema_index_mismatch:{schema}.{ZIP_INDEX_NAME}"
            )
        drop_invalid_index = not bool(index_record["is_valid"])
    with context.autocommit_block():
        if drop_invalid_index:
            op.drop_index(
                ZIP_INDEX_NAME,
                table_name=ZIP_TABLE_NAME,
                schema=schema,
                if_exists=True,
                postgresql_concurrently=True,
            )
        op.create_index(
            ZIP_INDEX_NAME,
            ZIP_TABLE_NAME,
            ["latitude", "longitude", "zip_code"],
            schema=schema,
            if_not_exists=True,
            postgresql_concurrently=True,
        )


def upgrade() -> None:
    schema = _schema()
    import_run = _qt(schema, "import_run")
    candidate = _qt(schema, "plan_pricing_projection_candidate")
    card = _qt(schema, "plan_pricing_card")
    aggregate = _qt(schema, "plan_pricing_cell_aggregate")
    candidate_guard = _qt(schema, "plan_pricing_projection_candidate_guard")
    child_guard = _qt(schema, "plan_pricing_projection_child_guard")
    truncate_guard = _qt(schema, "plan_pricing_projection_truncate_guard")

    _create_zip_index(schema)
    statements = (
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {IDEMPOTENCY_INDEX_NAME}
            ON {import_run} (importer, idempotency_key)
         WHERE importer IN (
                   'plan-pricing-projection', 'plan-pricing-prewarm'
               )
           AND idempotency_key IS NOT NULL
        """,
        f"""
        CREATE TABLE {candidate} (
            projection_id varchar(64) PRIMARY KEY,
            contract_version varchar(64) NOT NULL,
            binding_manifest_digest varchar(64) NOT NULL,
            binding_manifest jsonb NOT NULL,
            provider_signature varchar(64) NOT NULL,
            state varchar(16) NOT NULL,
            content_digest varchar(64),
            card_row_count bigint,
            aggregate_row_count bigint,
            fragment_byte_count bigint,
            build_seconds numeric,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            completed_at timestamptz,
            CONSTRAINT plan_pricing_projection_id_ck
                CHECK (projection_id ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_projection_contract_ck
                CHECK (contract_version = 'plan_pricing_card_v2'),
            CONSTRAINT plan_pricing_projection_binding_digest_ck
                CHECK (binding_manifest_digest ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_projection_binding_manifest_ck
                CHECK (jsonb_typeof(binding_manifest) = 'array'),
            CONSTRAINT plan_pricing_projection_provider_signature_ck
                CHECK (provider_signature ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT plan_pricing_projection_state_ck
                CHECK (state IN ('building', 'ready')),
            CONSTRAINT plan_pricing_projection_ready_ck CHECK (
                state <> 'ready' OR (
                    content_digest ~ '^[0-9a-f]{{64}}$'
                    AND card_row_count >= 0
                    AND aggregate_row_count >= 0
                    AND fragment_byte_count >= 0
                    AND build_seconds >= 0
                    AND completed_at IS NOT NULL
                )
            ),
            UNIQUE (
                contract_version,
                binding_manifest_digest,
                provider_signature
            )
        )
        """,
        f"""
        CREATE TABLE {card} (
            projection_id varchar(64) NOT NULL REFERENCES {candidate}
                (projection_id) ON DELETE CASCADE,
            code_system varchar(64) NOT NULL,
            code varchar(128) NOT NULL,
            geo_cell varchar(16) NOT NULL,
            npi bigint NOT NULL,
            minimum_negotiated_rate numeric NOT NULL,
            maximum_negotiated_rate numeric NOT NULL,
            rate_count bigint NOT NULL,
            fragment bytea NOT NULL,
            PRIMARY KEY (
                projection_id, code_system, code, geo_cell, npi
            ),
            CONSTRAINT plan_pricing_card_cell_ck
                CHECK (geo_cell ~ '^[0-9]{{5}}$'),
            CONSTRAINT plan_pricing_card_npi_ck CHECK (npi > 0),
            CONSTRAINT plan_pricing_card_rates_ck CHECK (
                minimum_negotiated_rate >= 0
                AND maximum_negotiated_rate >= minimum_negotiated_rate
                AND rate_count > 0
            ),
            CONSTRAINT plan_pricing_card_fragment_ck
                CHECK (octet_length(fragment) BETWEEN 2 AND 4096)
        )
        """,
        f"""
        CREATE INDEX plan_pricing_card_lookup_idx ON {card} (
            projection_id, code_system, code, geo_cell,
            minimum_negotiated_rate, npi
        )
        """,
        f"""
        CREATE TABLE {aggregate} (
            projection_id varchar(64) NOT NULL REFERENCES {candidate}
                (projection_id) ON DELETE CASCADE,
            code_system varchar(64) NOT NULL,
            code varchar(128) NOT NULL,
            geo_cell varchar(16) NOT NULL,
            provider_count bigint NOT NULL,
            rate_count bigint NOT NULL,
            minimum_negotiated_rate numeric NOT NULL,
            median_negotiated_rate numeric NOT NULL,
            maximum_negotiated_rate numeric NOT NULL,
            fragment bytea NOT NULL,
            PRIMARY KEY (projection_id, code_system, code, geo_cell),
            CONSTRAINT plan_pricing_cell_aggregate_cell_ck
                CHECK (geo_cell ~ '^[0-9]{{5}}$'),
            CONSTRAINT plan_pricing_cell_aggregate_counts_ck CHECK (
                provider_count > 0 AND rate_count > 0
            ),
            CONSTRAINT plan_pricing_cell_aggregate_rates_ck CHECK (
                minimum_negotiated_rate >= 0
                AND median_negotiated_rate >= minimum_negotiated_rate
                AND maximum_negotiated_rate >= median_negotiated_rate
            ),
            CONSTRAINT plan_pricing_cell_aggregate_fragment_ck
                CHECK (octet_length(fragment) BETWEEN 2 AND 2048)
        )
        """,
        f"""
        CREATE FUNCTION {candidate_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            actual_card_rows bigint;
            actual_aggregate_rows bigint;
            actual_fragment_bytes bigint;
        BEGIN
            IF TG_OP <> 'INSERT' AND OLD.state = 'ready' THEN
                RAISE EXCEPTION 'ready plan-pricing projections are immutable';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.state = 'ready' THEN
                SELECT COUNT(*), COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_card_rows, actual_fragment_bytes
                  FROM {card}
                 WHERE projection_id = NEW.projection_id;
                SELECT COUNT(*),
                       actual_fragment_bytes
                           + COALESCE(SUM(octet_length(fragment)), 0)
                  INTO actual_aggregate_rows, actual_fragment_bytes
                  FROM {aggregate}
                 WHERE projection_id = NEW.projection_id;
                IF NEW.card_row_count IS DISTINCT FROM actual_card_rows
                   OR NEW.aggregate_row_count IS DISTINCT FROM actual_aggregate_rows
                   OR NEW.fragment_byte_count IS DISTINCT FROM actual_fragment_bytes
                THEN
                    RAISE EXCEPTION 'plan-pricing projection receipt counts do not match rows';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$
        """,
        f"""
        CREATE FUNCTION {child_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            parent_state text;
        BEGIN
            IF TG_OP = 'INSERT' THEN
                SELECT state INTO parent_state
                  FROM {candidate}
                 WHERE projection_id = NEW.projection_id
                   FOR UPDATE;
                IF parent_state = 'ready' THEN
                    RAISE EXCEPTION 'ready plan-pricing projection rows are immutable';
                END IF;
            ELSIF TG_OP = 'DELETE' THEN
                SELECT state INTO parent_state
                  FROM {candidate}
                 WHERE projection_id = OLD.projection_id
                   FOR UPDATE;
                IF parent_state = 'ready' THEN
                    RAISE EXCEPTION 'ready plan-pricing projection rows are immutable';
                END IF;
            ELSE
                PERFORM 1
                  FROM {candidate}
                 WHERE projection_id IN (
                           OLD.projection_id,
                           NEW.projection_id
                       )
                 ORDER BY projection_id
                   FOR UPDATE;
                IF EXISTS (
                    SELECT 1 FROM {candidate}
                     WHERE state = 'ready'
                       AND projection_id IN (
                               OLD.projection_id,
                               NEW.projection_id
                           )
                ) THEN
                    RAISE EXCEPTION 'ready plan-pricing projection rows are immutable';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END
        $$
        """,
        f"""
        CREATE FUNCTION {truncate_guard}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {candidate} WHERE state = 'ready') THEN
                RAISE EXCEPTION 'ready plan-pricing projections cannot be truncated';
            END IF;
            RETURN NULL;
        END
        $$
        """,
        f"""
        CREATE TRIGGER plan_pricing_projection_candidate_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {candidate}
        FOR EACH ROW EXECUTE FUNCTION {candidate_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_card_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {card}
        FOR EACH ROW EXECUTE FUNCTION {child_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_cell_aggregate_guard_trg
        BEFORE INSERT OR UPDATE OR DELETE ON {aggregate}
        FOR EACH ROW EXECUTE FUNCTION {child_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_projection_candidate_truncate_guard_trg
        BEFORE TRUNCATE ON {candidate}
        FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_card_truncate_guard_trg
        BEFORE TRUNCATE ON {card}
        FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()
        """,
        f"""
        CREATE TRIGGER plan_pricing_cell_aggregate_truncate_guard_trg
        BEFORE TRUNCATE ON {aggregate}
        FOR EACH STATEMENT EXECUTE FUNCTION {truncate_guard}()
        """,
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    schema = _schema()
    for table in (
        "plan_pricing_cell_aggregate",
        "plan_pricing_card",
        "plan_pricing_projection_candidate",
    ):
        op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table)}")
    for function_name in (
        "plan_pricing_projection_candidate_guard",
        "plan_pricing_projection_child_guard",
        "plan_pricing_projection_truncate_guard",
    ):
        op.execute(
            f"DROP FUNCTION IF EXISTS {_qt(schema, function_name)}()"
        )
    op.drop_index(
        IDEMPOTENCY_INDEX_NAME,
        table_name="import_run",
        schema=schema,
        if_exists=True,
    )
    with op.get_context().autocommit_block():
        op.drop_index(
            ZIP_INDEX_NAME,
            table_name=ZIP_TABLE_NAME,
            schema=schema,
            if_exists=True,
            postgresql_concurrently=True,
        )
