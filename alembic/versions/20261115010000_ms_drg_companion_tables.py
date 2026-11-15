# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install shared catalog companions before restricted managed imports run."""

import sqlalchemy as sa

from alembic import op
from db.migration_adoption import create_table_or_validate
from db.plan_release_storage import plan_release_schema

revision = "20261115010000_ms_drg_companion_tables"
down_revision = "20261113010000_address_canonical_space_normalization"
branch_labels = None
depends_on = None


def upgrade():
    """Create missing companions while preserving ordinary-import tables and grants."""
    schema = plan_release_schema()
    create_table_or_validate(
        op,
        "code_synonym",
        sa.Column("code_system", sa.String(32), nullable=False),
        sa.Column("code", sa.String(128), nullable=False),
        sa.Column("synonym", sa.String(), nullable=False),
        sa.Column("term_type", sa.String(64), nullable=False),
        sa.Column("language", sa.String(16)),
        sa.Column("source", sa.String(128), nullable=False),
        sa.Column("source_attribution", sa.Text()),
        sa.Column("updated_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("code_system", "code", "synonym", "term_type"),
        schema=schema,
    )
    create_table_or_validate(
        op,
        "code_relationship",
        sa.Column("from_system", sa.String(32), nullable=False),
        sa.Column("from_code", sa.String(128), nullable=False),
        sa.Column("relationship", sa.String(64), nullable=False),
        sa.Column("to_system", sa.String(32), nullable=False),
        sa.Column("to_code", sa.String(128), nullable=False),
        sa.Column("source", sa.String(128), nullable=False),
        sa.Column("source_attribution", sa.Text()),
        sa.Column("updated_at", sa.DateTime()),
        sa.PrimaryKeyConstraint("from_system", "from_code", "relationship", "to_system", "to_code"),
        schema=schema,
    )


def downgrade():
    """Keep companions that may contain retained catalog contributions."""
    raise RuntimeError("Catalog companions require an explicit retained-data migration")
