"""Add normalized code catalog and crosswalk tables"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260215103000_add_code_catalog_and_crosswalk"
down_revision = "20260214170000_add_cms_claims_pricing_tables"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "code_catalog",
        sa.Column("code_system", sa.String(length=32), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("short_description", sa.String(), nullable=True),
        sa.Column("long_description", sa.TEXT(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column("effective_year", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("code_system", "code"),
        schema=schema,
    )
    op.create_index(
        "code_catalog_system_display_idx",
        "code_catalog",
        ["code_system", "display_name"],
        schema=schema,
    )
    op.create_index("code_catalog_source_idx", "code_catalog", ["source"], schema=schema)

    op.create_table(
        "code_crosswalk",
        sa.Column("from_system", sa.String(length=32), nullable=False),
        sa.Column("from_code", sa.String(length=64), nullable=False),
        sa.Column("to_system", sa.String(length=32), nullable=False),
        sa.Column("to_code", sa.String(length=64), nullable=False),
        sa.Column("match_type", sa.String(length=32), nullable=True),
        sa.Column("confidence", sa.Numeric(precision=6, scale=4, asdecimal=False), nullable=True),
        sa.Column("source", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("from_system", "from_code", "to_system", "to_code"),
        schema=schema,
    )
    op.create_index(
        "code_crosswalk_from_idx",
        "code_crosswalk",
        ["from_system", "from_code"],
        schema=schema,
    )
    op.create_index(
        "code_crosswalk_to_idx",
        "code_crosswalk",
        ["to_system", "to_code"],
        schema=schema,
    )


def downgrade():
    schema = _schema()

    op.drop_index("code_crosswalk_to_idx", table_name="code_crosswalk", schema=schema)
    op.drop_index("code_crosswalk_from_idx", table_name="code_crosswalk", schema=schema)
    op.drop_table("code_crosswalk", schema=schema)

    op.drop_index("code_catalog_source_idx", table_name="code_catalog", schema=schema)
    op.drop_index("code_catalog_system_display_idx", table_name="code_catalog", schema=schema)
    op.drop_table("code_catalog", schema=schema)
