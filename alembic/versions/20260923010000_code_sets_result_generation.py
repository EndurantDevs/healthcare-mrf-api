# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed generationless authority for the source-scoped code-set catalog.

Revision ID: 20260923010000_code_sets_result_generation
Revises: 20260923000000_facility_address_contribution
"""

from __future__ import annotations

import os
import re
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "20260923010000_code_sets_result_generation"
down_revision = "20260923000000_facility_address_contribution"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schema configuration differs")
    schema = runtime or legacy or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) or len(schema.encode()) > 63:
        raise RuntimeError("code-set generation schema is invalid")
    return schema


def upgrade() -> None:
    """Create an unclaimed singleton for scoped code-set publication."""
    schema = _schema()
    op.create_table(
        "code_sets_result_generation",
        sa.Column("id", sa.SmallInteger(), primary_key=True),
        sa.Column("local_lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("local_generation", sa.BigInteger(), nullable=False),
        sa.Column("origin_lineage_id", postgresql.UUID(as_uuid=True)),
        sa.Column("origin_generation", sa.BigInteger()),
        sa.Column("published_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("code_catalog_oid", sa.BigInteger()),
        sa.Column("row_count", sa.BigInteger()),
        sa.Column("row_sha256", sa.Text()),
        sa.CheckConstraint("id = 1 AND local_generation >= 0", name="code_sets_generation_singleton_check"),
        sa.CheckConstraint(
            "(origin_lineage_id IS NULL AND origin_generation IS NULL AND published_at IS NULL "
            "AND code_catalog_oid IS NULL AND row_count IS NULL AND row_sha256 IS NULL) OR "
            "(origin_lineage_id IS NOT NULL AND origin_generation > 0 AND published_at IS NOT NULL "
            "AND code_catalog_oid > 0 AND row_count >= 0 AND row_sha256 ~ '^[0-9a-f]{64}$')",
            name="code_sets_generation_shape_check",
        ),
        schema=schema,
    )
    op.execute(
        sa.text(
            f'INSERT INTO "{schema}".code_sets_result_generation (id,local_lineage_id,local_generation) '
            "VALUES (1,:lineage,0)"
        ).bindparams(sa.bindparam("lineage", value=uuid4(), type_=postgresql.UUID(as_uuid=True)))
    )


def downgrade() -> None:
    """Remove only a generationless, never-published singleton."""
    schema = _schema()
    active = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT local_generation <> 0 OR origin_generation IS NOT NULL "
                f'FROM "{schema}".code_sets_result_generation WHERE id=1'
            )
        )
        .scalar_one()
    )
    if active:
        raise RuntimeError("code-set generation evidence prevents downgrade")
    op.drop_table("code_sets_result_generation", schema=schema)
