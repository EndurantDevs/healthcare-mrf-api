"""Drop do_business_as_search column from NPI"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20251112120000_drop_do_business_as_search"
down_revision = "20251110160323_add_do_business_as_to_npi"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()
    op.execute(f"ALTER TABLE {schema}.npi DROP COLUMN IF EXISTS do_business_as_search;")

def downgrade():
    schema = _schema()
    op.add_column(
        "npi",
        sa.Column(
            "do_business_as_search",
            sa.Text(),
            server_default=sa.text("''"),
            nullable=False,
        ),
        schema=schema,
    )
    op.execute(
        f"UPDATE {schema}.npi SET do_business_as_search = '' WHERE do_business_as_search IS NULL;"
    )
