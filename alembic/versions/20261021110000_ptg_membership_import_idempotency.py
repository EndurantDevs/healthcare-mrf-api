# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the exact all-status owner of a physical membership build request."""

import os

from sqlalchemy import text

from alembic import op
from db.migration_index_adoption import has_matching_index

revision = "20261021110000_ptg_membership_import_idempotency"
down_revision = "20261020200000_custom_import_manual_acceptance"
branch_labels = None
depends_on = None


def _schema():
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    return runtime or legacy or "mrf"


def upgrade():
    """Keep one all-status run owner for each membership idempotency key."""
    index_name = "import_run_ptg_membership_idempotency_idx"
    columns = ["importer", "idempotency_key"]
    index_options_by_name = {
        "unique": True,
        "postgresql_where": text("importer = 'ptg-provider-membership-projection' AND idempotency_key IS NOT NULL"),
    }
    schema = _schema()
    if not has_matching_index(op, index_name, "import_run", columns, schema=schema, **index_options_by_name):
        op.create_index(index_name, "import_run", columns, schema=schema, **index_options_by_name)


def downgrade():
    """Remove only the membership importer idempotency index."""
    op.drop_index("import_run_ptg_membership_idempotency_idx", table_name="import_run", schema=_schema())
