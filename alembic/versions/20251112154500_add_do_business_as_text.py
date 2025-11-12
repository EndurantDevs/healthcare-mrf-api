"""Add do_business_as_text column to NPI"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20251112154500_add_do_business_as_text"
down_revision = "20251112120000_drop_do_business_as_search"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    op.add_column(
        "npi",
        sa.Column(
            "do_business_as_text",
            sa.Text(),
            server_default=sa.text("''"),
            nullable=False,
        ),
        schema=schema,
    )
    op.execute(
        f"UPDATE {schema}.npi SET do_business_as_text = '' WHERE do_business_as_text IS NULL;"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS npi_idx_do_business_as_trgm ON {schema}.npi USING gin (do_business_as_text gin_trgm_ops);"
    )


def downgrade():
    schema = _schema()
    op.execute(
        f"DROP INDEX IF EXISTS {schema}.npi_idx_do_business_as_trgm;"
    )
    op.drop_column("npi", "do_business_as_text", schema=schema)
