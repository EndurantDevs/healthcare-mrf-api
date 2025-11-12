
"""Add do_business_as columns to NPI"""

from alembic import op
import sqlalchemy as sa
import os


revision = "20251110160323_add_do_business_as_to_npi"
down_revision = None
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    op.add_column(
        "npi",
        sa.Column(
            "do_business_as",
            sa.ARRAY(sa.String()),
            server_default=sa.text("ARRAY[]::varchar[]"),
            nullable=False,
        ),
        schema=schema,
    )
    op.execute(
        f"UPDATE {schema}.npi SET do_business_as = ARRAY[]::varchar[] WHERE do_business_as IS NULL;"
    )


def downgrade():
    schema = _schema()
    op.drop_column("npi", "do_business_as", schema=schema)
