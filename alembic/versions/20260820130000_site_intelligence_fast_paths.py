"""Precompute procedure search and taxonomy resolver signals.

Revision ID: 20260820130000_site_intelligence_fast_paths
Revises: 20260820030000_ptg_ordinary_terminal_json_canonical_digest
"""

from __future__ import annotations

import os

from alembic import op
from sqlalchemy import text

from db.migration_index_adoption import (
    _create_temporary_index_table,
    _shape_from_catalog,
    _temporary_table_schema,
)
from db.migration_index_catalog import _index_catalog_record
from db.procedure_taxonomy_signal_sql import procedure_taxonomy_signal_insert_sql


revision = "20260820130000_site_intelligence_fast_paths"
down_revision = "20260820030000_ptg_ordinary_terminal_json_canonical_digest"
branch_labels = None
depends_on = None


PROCEDURE_TABLE = "pricing_procedure"
PROVIDER_TABLE = "pricing_provider"
PROVIDER_PROCEDURE_TABLE = "pricing_provider_procedure"
QUALITY_FEATURE_TABLE = "pricing_provider_quality_feature"
NPI_TAXONOMY_TABLE = "npi_taxonomy"
NUCC_TAXONOMY_TABLE = "nucc_taxonomy"
SIGNAL_TABLE = "procedure_taxonomy_signal"
PAGE_INDEX_NAME = "pricing_provider_proc_amount_page_idx"
SIGNAL_INDEX_NAME = "procedure_taxonomy_signal_lookup_idx"
PAGE_INDEX_EXPRESSIONS = (
    "year",
    "procedure_code",
    "total_allowed_amount DESC",
    "npi",
)
SIGNAL_INDEX_EXPRESSIONS = (
    "year",
    "procedure_code",
    "setting_key",
    "evidence_source",
    "distinct_npis DESC",
    "total_services DESC",
    "taxonomy_code",
)
SIGNAL_COLUMNS = {
    "procedure_code",
    "year",
    "setting_key",
    "evidence_source",
    "taxonomy_code",
    "classification",
    "specialization",
    "display_name",
    "distinct_npis",
    "total_services",
    "total_beneficiaries",
    "provider_types",
    "source_relation_fingerprint",
    "updated_at",
}


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _table_columns(schema: str, table_name: str) -> set[str]:
    return {
        str(row[0])
        for row in op.get_bind().execute(
            text(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table_name
                """
            ),
            {"schema": schema, "table_name": table_name},
        )
    }


def _has_columns(schema: str, table_name: str, required: set[str]) -> bool:
    return required.issubset(_table_columns(schema, table_name))


def _create_signal_table_sql(schema: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, SIGNAL_TABLE)} (
            procedure_code bigint NOT NULL,
            year integer NOT NULL,
            setting_key varchar(64) NOT NULL,
            evidence_source varchar(32) NOT NULL,
            taxonomy_code varchar(32) NOT NULL,
            classification varchar,
            specialization varchar,
            display_name varchar,
            distinct_npis integer NOT NULL,
            total_services double precision NOT NULL,
            total_beneficiaries double precision NOT NULL,
            provider_types varchar[] NOT NULL,
            source_relation_fingerprint varchar(128) NOT NULL,
            updated_at timestamp without time zone,
            PRIMARY KEY (
                procedure_code,
                year,
                setting_key,
                evidence_source,
                taxonomy_code
            )
        )
    """


def _create_page_index_sql(schema: str, *, concurrently: bool = True) -> str:
    concurrent_clause = " CONCURRENTLY" if concurrently else ""
    return (
        f"CREATE INDEX{concurrent_clause} IF NOT EXISTS {_q(PAGE_INDEX_NAME)} "
        f"ON {_qt(schema, PROVIDER_PROCEDURE_TABLE)} "
        f"({', '.join(PAGE_INDEX_EXPRESSIONS)})"
    )


def _drop_page_index_sql(schema: str) -> str:
    return f"DROP INDEX CONCURRENTLY IF EXISTS {_qt(schema, PAGE_INDEX_NAME)}"


def _create_signal_index_sql(schema: str) -> str:
    return (
        f"CREATE INDEX IF NOT EXISTS {_q(SIGNAL_INDEX_NAME)} "
        f"ON {_qt(schema, SIGNAL_TABLE)} "
        f"({', '.join(SIGNAL_INDEX_EXPRESSIONS)})"
    )


def _expected_page_index_shape(schema: str):
    bind = op.get_bind()
    temporary = _create_temporary_index_table(
        bind,
        schema,
        PROVIDER_PROCEDURE_TABLE,
    )
    bind.exec_driver_sql(
        f"CREATE INDEX {temporary.quoted_index} "
        f"ON {temporary.quoted_table} ({', '.join(PAGE_INDEX_EXPRESSIONS)})"
    )
    record = _index_catalog_record(
        op,
        temporary.index_name,
        temporary.table_name,
        _temporary_table_schema(bind, temporary.table_name),
    )
    if record is None:
        raise RuntimeError("temporary_expected_index_missing")
    return _shape_from_catalog(record)


def _matching_page_index_record(schema: str, expected_shape):
    record = _index_catalog_record(
        op,
        PAGE_INDEX_NAME,
        PROVIDER_PROCEDURE_TABLE,
        schema,
    )
    if record is None:
        return None
    if not record["indisvalid"] or not record["indisready"]:
        return False
    if _shape_from_catalog(record) != expected_shape:
        raise RuntimeError(
            f"existing_schema_index_mismatch:{schema}.{PAGE_INDEX_NAME}"
        )
    return record


def _optional_table(
    schema: str,
    table_name: str,
    required_columns: set[str],
) -> str | None:
    return table_name if _has_columns(schema, table_name, required_columns) else None


def _backfill_provider_count(schema: str) -> None:
    op.execute(
        f"""
        UPDATE {_qt(schema, PROCEDURE_TABLE)} procedure
           SET provider_count = 0
         WHERE provider_count <> 0
           AND NOT EXISTS (
                SELECT 1
                  FROM {_qt(schema, PROVIDER_PROCEDURE_TABLE)} pp
                  JOIN {_qt(schema, PROVIDER_TABLE)} provider
                    ON provider.npi = pp.npi
                   AND provider.year = pp.year
                 WHERE pp.procedure_code = procedure.procedure_code
                   AND pp.year = procedure.source_year
           )
        """
    )
    op.execute(
        f"""
        WITH provider_counts AS (
            SELECT
                pp.year,
                pp.procedure_code,
                COUNT(DISTINCT pp.npi)::integer AS provider_count
            FROM {_qt(schema, PROVIDER_PROCEDURE_TABLE)} pp
            JOIN {_qt(schema, PROVIDER_TABLE)} provider
              ON provider.npi = pp.npi
             AND provider.year = pp.year
            GROUP BY pp.year, pp.procedure_code
        )
        UPDATE {_qt(schema, PROCEDURE_TABLE)} procedure
           SET provider_count = counts.provider_count
          FROM provider_counts counts
         WHERE counts.procedure_code = procedure.procedure_code
           AND counts.year = procedure.source_year
        """
    )


def _backfill_signal(schema: str) -> None:
    op.execute(f"TRUNCATE TABLE {_qt(schema, SIGNAL_TABLE)}")
    op.execute(
        procedure_taxonomy_signal_insert_sql(
            schema=schema,
            signal_table=SIGNAL_TABLE,
            provider_table=PROVIDER_TABLE,
            provider_procedure_table=PROVIDER_PROCEDURE_TABLE,
            quality_feature_table=_optional_table(
                schema,
                QUALITY_FEATURE_TABLE,
                {"npi", "year", "taxonomy_code", "taxonomy_classification"},
            ),
            npi_taxonomy_table=_optional_table(
                schema,
                NPI_TAXONOMY_TABLE,
                {
                    "npi",
                    "healthcare_provider_taxonomy_code",
                    "healthcare_provider_primary_taxonomy_switch",
                    "checksum",
                },
            ),
            nucc_taxonomy_table=_optional_table(
                schema,
                NUCC_TAXONOMY_TABLE,
                {"code", "classification", "specialization", "display_name"},
            ),
        )
    )


def upgrade() -> None:
    schema = _schema()
    context = op.get_context()
    op.execute(_create_signal_table_sql(schema))
    if context.as_sql:
        op.execute(_create_signal_index_sql(schema))
        op.execute(
            f"ALTER TABLE IF EXISTS {_qt(schema, PROCEDURE_TABLE)} "
            "ADD COLUMN IF NOT EXISTS provider_count integer NOT NULL DEFAULT 0"
        )
        with context.autocommit_block():
            op.execute(_create_page_index_sql(schema))
        return
    if _table_columns(schema, SIGNAL_TABLE) != SIGNAL_COLUMNS:
        raise RuntimeError(f"existing_schema_table_mismatch:{schema}.{SIGNAL_TABLE}")
    op.execute(_create_signal_index_sql(schema))

    procedure_ready = _has_columns(
        schema,
        PROCEDURE_TABLE,
        {"procedure_code", "source_year"},
    )
    provider_count_source_ready = _has_columns(
        schema,
        PROVIDER_TABLE,
        {"npi", "year"},
    ) and _has_columns(
        schema,
        PROVIDER_PROCEDURE_TABLE,
        {"npi", "year", "procedure_code"},
    )
    signal_source_ready = _has_columns(
        schema,
        PROVIDER_TABLE,
        {"npi", "year", "provider_type"},
    ) and _has_columns(
        schema,
        PROVIDER_PROCEDURE_TABLE,
        {
            "npi",
            "year",
            "procedure_code",
            "total_services",
            "total_beneficiaries",
        },
    )
    page_source_ready = _has_columns(
        schema,
        PROVIDER_PROCEDURE_TABLE,
        {"npi", "year", "procedure_code", "total_allowed_amount"},
    )
    if procedure_ready:
        op.execute(
            f"ALTER TABLE {_qt(schema, PROCEDURE_TABLE)} "
            "ADD COLUMN IF NOT EXISTS provider_count integer NOT NULL DEFAULT 0"
        )
    if procedure_ready and provider_count_source_ready:
        _backfill_provider_count(schema)
    if signal_source_ready:
        _backfill_signal(schema)
    if not page_source_ready:
        return

    expected_shape = _expected_page_index_shape(schema)
    existing_record = _matching_page_index_record(schema, expected_shape)
    if existing_record is not None and existing_record is not False:
        return
    with context.autocommit_block():
        if existing_record is False:
            op.get_bind().exec_driver_sql(_drop_page_index_sql(schema))
        op.get_bind().exec_driver_sql(_create_page_index_sql(schema))
    if not _matching_page_index_record(schema, expected_shape):
        raise RuntimeError(f"required_index_missing:{schema}.{PAGE_INDEX_NAME}")


def downgrade() -> None:
    schema = _schema()
    with op.get_context().autocommit_block():
        op.execute(_drop_page_index_sql(schema))
    op.execute(f"DROP TABLE IF EXISTS {_qt(schema, SIGNAL_TABLE)}")
    op.execute(
        f"ALTER TABLE IF EXISTS {_qt(schema, PROCEDURE_TABLE)} "
        "DROP COLUMN IF EXISTS provider_count"
    )
