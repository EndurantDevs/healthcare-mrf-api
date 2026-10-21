# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the exact all-status owner of a physical membership build request."""

import os

from alembic import op

revision = "20261021110000_ptg_membership_import_idempotency"
down_revision = "20261020200000_custom_import_manual_acceptance"
branch_labels = None
depends_on = None


def _schema():
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    return '"' + (runtime or legacy or "mrf").replace('"', '""') + '"'


def upgrade():
    op.execute(f"""CREATE UNIQUE INDEX import_run_ptg_membership_idempotency_idx
        ON {_schema()}.import_run (importer,idempotency_key)
        WHERE importer = 'ptg-provider-membership-projection' AND idempotency_key IS NOT NULL""")


def downgrade():
    op.execute(f"DROP INDEX {_schema()}.import_run_ptg_membership_idempotency_idx")
