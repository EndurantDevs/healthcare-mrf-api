# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Track the exact serving revision of the six-table NPI result family.

Revision ID: 20260914120000_npi_result_generation
Revises: 20260914120000_custom_import_v1_schema
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260914120000_npi_result_generation"
down_revision = "20260914120000_custom_import_v1_schema"
branch_labels = None
depends_on = None

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STATE_TABLE = "npi_result_generation"
_REVISION_FUNCTION = "advance_npi_result_generation"
_REVISION_TRIGGER = "npi_result_generation_revision_guard"
_NPI_TABLES = (
    "npi",
    "npi_address",
    "npi_taxonomy",
    "npi_taxonomy_group",
    "npi_other_identifier",
    "npi_phone_staffing",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    schema = runtime_schema or legacy_schema or "mrf"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode("utf-8")) > 63:
        raise RuntimeError("NPI result generation schema is invalid")
    return schema


def _quoted(value: str) -> str:
    if _IDENTIFIER.fullmatch(value) is None:
        raise RuntimeError("NPI result generation identifier is invalid")
    return f'"{value}"'


def _shape_check() -> str:
    return (
        "singleton IS TRUE AND local_generation BETWEEN 0 AND 9223372036854775807 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL "
        "AND published_at IS NULL AND relation_oids IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation BETWEEN 1 AND 9223372036854775807 "
        "AND published_at IS NOT NULL AND relation_oids IS NOT NULL "
        "AND array_ndims(relation_oids) = 1 AND array_lower(relation_oids, 1) = 1 "
        "AND cardinality(relation_oids) = 6 AND array_position(relation_oids, NULL) IS NULL "
        "AND 0 < ALL(relation_oids) AND 4294967295 >= ALL(relation_oids) "
        "AND relation_oids[1] <> ALL(relation_oids[2:6]) "
        "AND relation_oids[2] <> ALL(relation_oids[3:6]) "
        "AND relation_oids[3] <> ALL(relation_oids[4:6]) "
        "AND relation_oids[4] <> ALL(relation_oids[5:6]) "
        "AND relation_oids[5] <> relation_oids[6])) AND ("
        "(canonical_publication_ref IS NULL AND canonical_publication_generation IS NULL "
        "AND canonical_chain_ref IS NULL AND canonical_import_date IS NULL) OR ("
        "canonical_publication_ref IS NOT NULL AND canonical_publication_generation IS NOT NULL "
        "AND canonical_publication_generation BETWEEN 1 AND 9007199254740991 "
        "AND canonical_chain_ref IS NOT NULL AND canonical_import_date IS NOT NULL))"
    )


def _create_revision_function(schema: str) -> None:
    state = f"{_quoted(schema)}.{_quoted(_STATE_TABLE)}"
    function = f"{_quoted(schema)}.{_quoted(_REVISION_FUNCTION)}"
    op.execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql
        SECURITY DEFINER SET search_path=pg_catalog AS $function$
        BEGIN
            UPDATE {state}
               SET local_generation = local_generation + 1,
                   origin_lineage_id = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE local_lineage_id
                   END,
                   origin_generation = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE local_generation + 1
                   END,
                   published_at = CASE
                       WHEN origin_lineage_id IS NULL THEN NULL
                       ELSE transaction_timestamp()
                   END
             WHERE singleton IS TRUE
               AND relation_oids IS NOT NULL
               AND TG_RELID::bigint = ANY(relation_oids);
            RETURN NULL;
        END; $function$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {function}() FROM PUBLIC;")


def _create_revision_triggers(schema: str) -> None:
    function = f"{_quoted(schema)}.{_quoted(_REVISION_FUNCTION)}"
    for table_name in _NPI_TABLES:
        table = f"{_quoted(schema)}.{_quoted(table_name)}"
        op.execute(
            f"CREATE TRIGGER {_quoted(_REVISION_TRIGGER)} "
            f"AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {function}();"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {_quoted(_REVISION_TRIGGER)};")


def upgrade() -> None:
    """Install generationless state and transaction-bound revision tracking."""

    schema = _schema()
    op.create_table(
        _STATE_TABLE,
        sa.Column("singleton", sa.Boolean(), nullable=False),
        sa.Column("local_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column("origin_lineage_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("origin_generation", sa.BigInteger(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("relation_oids", postgresql.ARRAY(sa.BigInteger()), nullable=True),
        sa.Column("canonical_publication_ref", sa.String(length=50), nullable=True),
        sa.Column("canonical_publication_generation", sa.BigInteger(), nullable=True),
        sa.Column("canonical_chain_ref", sa.String(length=50), nullable=True),
        sa.Column("canonical_import_date", sa.Date(), nullable=True),
        sa.Column(
            "tracking_started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("transaction_timestamp()"),
        ),
        sa.CheckConstraint(_shape_check(), name="npi_result_generation_shape_check"),
        sa.PrimaryKeyConstraint("singleton"),
        schema=schema,
    )
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    op.execute(
        sa.text(
            f'INSERT INTO {quoted_schema}."{_STATE_TABLE}" '
            "(singleton, local_lineage_id, local_generation) "
            "VALUES (TRUE, :lineage_id, 0)"
        ).bindparams(
            sa.bindparam(
                "lineage_id",
                value=uuid4(),
                type_=postgresql.UUID(as_uuid=True),
            )
        )
    )
    _create_revision_function(schema)
    _create_revision_triggers(schema)
    op.execute(f"REVOKE ALL ON TABLE {_quoted(schema)}.{_quoted(_STATE_TABLE)} FROM PUBLIC;")


def downgrade() -> None:
    """Refuse to erase any recorded NPI result revision or provenance."""

    schema = _schema()
    state = f"{_quoted(schema)}.{_quoted(_STATE_TABLE)}"
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {state} WHERE local_generation <> 0 "
                "OR origin_generation IS NOT NULL OR canonical_publication_ref IS NOT NULL)"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("NPI result generation evidence prevents downgrade")
    for table_name in _NPI_TABLES:
        table = f"{_quoted(schema)}.{_quoted(table_name)}"
        op.execute(f"DROP TRIGGER {_quoted(_REVISION_TRIGGER)} ON {table};")
    op.execute(f"DROP FUNCTION {_quoted(schema)}.{_quoted(_REVISION_FUNCTION)}();")
    op.drop_table(_STATE_TABLE, schema=schema)
