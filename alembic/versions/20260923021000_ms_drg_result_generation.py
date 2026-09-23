# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed generationless MS-DRG source-slice authority.

Revision ID: 20260923021000_ms_drg_result_generation
Revises: 20260923010000_code_sets_result_generation
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260923021000_ms_drg_result_generation"
down_revision = "20260923010000_code_sets_result_generation"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema.encode()) > 63:
        raise RuntimeError("MS-DRG generation schema is invalid")
    return schema


def upgrade() -> None:
    """Create the singleton authority before any MS-DRG result is published."""
    schema = _schema()
    op.create_table(
        "ms_drg_result_generation",
        sa.Column("id", sa.SmallInteger(), primary_key=True),
        sa.Column("local_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column("origin_lineage_id", postgresql.UUID(as_uuid=True)),
        sa.Column("origin_generation", sa.BigInteger()),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("include_relationships", sa.Boolean()),
        sa.Column("receipt", postgresql.JSONB()),
        sa.CheckConstraint("id=1 AND local_generation>=0", name="ms_drg_generation_singleton_check"),
        sa.CheckConstraint(
            "(origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL "
            "AND include_relationships IS NULL AND receipt IS NULL) OR "
            "(origin_lineage_id IS NOT NULL AND origin_generation>0 AND published_at IS NOT NULL "
            "AND include_relationships IS NOT NULL AND receipt IS NOT NULL)",
            name="ms_drg_generation_shape_check",
        ),
        schema=schema,
    )
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".ms_drg_result_generation '
            "(id,local_lineage_id,local_generation) VALUES (1,:lineage,0)"
        ).bindparams(sa.bindparam("lineage", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade() -> None:
    """Remove only an unused authority; published evidence blocks downgrade."""
    schema = _schema()
    active = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation<>0 OR origin_generation IS NOT NULL "
                f'FROM "{schema}".ms_drg_result_generation WHERE id=1'
            )
        )
        .scalar_one()
    )
    if active:
        raise RuntimeError("MS-DRG generation evidence prevents downgrade")
    op.drop_table("ms_drg_result_generation", schema=schema)
