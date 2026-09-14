# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add durable generation authority for closed reference families.

Revision ID: 20260914110000_reference_family_result_generation
Revises: 20260914100000_entity_address_result_generation
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260914110000_reference_family_result_generation"
down_revision = "20260914100000_entity_address_result_generation"
branch_labels = None
depends_on = None
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_IMPORTER_CARDINALITY = {
    "plan-attributes": 4,
    "places-zcta": 1,
    "lodes": 1,
    "medicare-enrollment": 2,
}


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    schema = runtime_schema or legacy_schema or "mrf"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode("utf-8")) > 63:
        raise RuntimeError("reference family result generation schema is invalid")
    return schema


def _shape_check() -> str:
    admitted = ", ".join(f"'{name}'" for name in _IMPORTER_CARDINALITY)
    cardinality = " OR ".join(
        f"(importer_id = '{name}' AND cardinality(relation_oids) = {count})"
        for name, count in _IMPORTER_CARDINALITY.items()
    )
    return (
        f"importer_id IN ({admitted}) AND local_generation >= 0 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL "
        "AND published_at IS NULL AND relation_oids IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 AND published_at IS NOT NULL "
        "AND relation_oids IS NOT NULL AND array_ndims(relation_oids) = 1 "
        "AND array_lower(relation_oids, 1) = 1 "
        f"AND ({cardinality}) "
        "AND array_position(relation_oids, NULL) IS NULL "
        "AND 0 < ALL(relation_oids) AND 4294967295 >= ALL(relation_oids) "
        "AND (cardinality(relation_oids) < 2 OR relation_oids[1] <> ALL(relation_oids[2:])) "
        "AND (cardinality(relation_oids) < 3 OR relation_oids[2] <> ALL(relation_oids[3:])) "
        "AND (cardinality(relation_oids) < 4 OR relation_oids[3] <> relation_oids[4])))"
    )


def upgrade() -> None:
    """Install generation-less rows without inventing legacy publication history."""

    schema = _schema()
    op.create_table(
        "reference_family_result_generation",
        sa.Column("importer_id", sa.Text(), nullable=False),
        sa.Column("local_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column("origin_lineage_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("origin_generation", sa.BigInteger(), nullable=True),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("relation_oids", postgresql.ARRAY(sa.BigInteger()), nullable=True),
        sa.CheckConstraint(
            _shape_check(),
            name="reference_family_result_generation_shape_check",
        ),
        sa.PrimaryKeyConstraint("importer_id"),
        schema=schema,
    )
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    for importer_id in _IMPORTER_CARDINALITY:
        op.execute(
            sa.text(
                f'INSERT INTO {quoted_schema}."reference_family_result_generation" '
                "(importer_id, local_lineage_id, local_generation) "
                "VALUES (:importer_id, :lineage_id, 0)"
            ).bindparams(
                sa.bindparam("importer_id", value=importer_id),
                sa.bindparam(
                    "lineage_id",
                    value=uuid4(),
                    type_=postgresql.UUID(as_uuid=True),
                ),
            )
        )


def downgrade() -> None:
    """Refuse to erase any local or adopted publication evidence."""

    schema = _schema()
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                f'SELECT EXISTS (SELECT 1 FROM {quoted_schema}."reference_family_result_generation" '
                "WHERE local_generation <> 0 OR origin_generation IS NOT NULL)"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError("reference family generation evidence prevents downgrade")
    op.drop_table("reference_family_result_generation", schema=schema)
