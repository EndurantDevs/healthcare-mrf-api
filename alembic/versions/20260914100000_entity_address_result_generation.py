# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add durable unified-address result generation authority.

Revision ID: 20260914100000_entity_address_result_generation
Revises: 20260911100000_hospital_price_tall_notes
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = "20260914100000_entity_address_result_generation"
down_revision = "20260911100000_hospital_price_tall_notes"
branch_labels = None
depends_on = None
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    schema = runtime_schema or legacy_schema or "mrf"
    if not _IDENTIFIER.fullmatch(schema) or len(schema.encode("utf-8")) > 63:
        raise RuntimeError("entity-address result generation schema is invalid")
    return schema


def _shape_check() -> str:
    """Require either explicit legacy absence or one complete serving tuple."""

    return (
        "singleton IS TRUE AND local_generation >= 0 AND ("
        "(origin_lineage_id IS NULL AND origin_generation IS NULL "
        "AND published_at IS NULL AND relation_oids IS NULL) OR ("
        "origin_lineage_id IS NOT NULL AND origin_generation IS NOT NULL "
        "AND origin_generation > 0 "
        "AND published_at IS NOT NULL AND relation_oids IS NOT NULL "
        "AND array_ndims(relation_oids) = 1 "
        "AND array_lower(relation_oids, 1) = 1 "
        "AND cardinality(relation_oids) = 7 "
        "AND array_position(relation_oids, NULL) IS NULL "
        "AND 0 < ALL(relation_oids) "
        "AND 4294967295 >= ALL(relation_oids) "
        "AND relation_oids[1] <> ALL(relation_oids[2:7]) "
        "AND relation_oids[2] <> ALL(relation_oids[3:7]) "
        "AND relation_oids[3] <> ALL(relation_oids[4:7]) "
        "AND relation_oids[4] <> ALL(relation_oids[5:7]) "
        "AND relation_oids[5] <> ALL(relation_oids[6:7]) "
        "AND relation_oids[6] <> relation_oids[7]))"
    )


def upgrade() -> None:
    """Install one generation-less singleton without inferring history."""

    schema = _schema()
    op.create_table(
        "entity_address_result_generation",
        sa.Column("singleton", sa.Boolean(), nullable=False),
        sa.Column(
            "local_lineage_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "origin_lineage_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("origin_generation", sa.BigInteger(), nullable=True),
        sa.Column(
            "published_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "relation_oids",
            postgresql.ARRAY(sa.BigInteger()),
            nullable=True,
        ),
        sa.CheckConstraint(
            _shape_check(),
            name="entity_address_result_generation_shape_check",
        ),
        sa.PrimaryKeyConstraint("singleton"),
        schema=schema,
    )
    lineage_id = uuid4()
    quoted_schema = op.get_bind().dialect.identifier_preparer.quote_schema(schema)
    op.execute(
        sa.text(
            f'INSERT INTO {quoted_schema}."entity_address_result_generation" '
            "(singleton, local_lineage_id, local_generation) "
            "VALUES (TRUE, :lineage_id, 0)"
        ).bindparams(
            sa.bindparam(
                "lineage_id",
                value=lineage_id,
                type_=postgresql.UUID(as_uuid=True),
            )
        )
    )


def downgrade() -> None:
    """Remove only the generation authority table."""

    op.drop_table("entity_address_result_generation", schema=_schema())
