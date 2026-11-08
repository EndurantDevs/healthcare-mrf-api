# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allow independently reserved artifact purposes within one control run."""

import os

from alembic import op

revision = "20260930110000_cms_npd_nonprofile_capacity"
down_revision = "20260930100000_cms_npd_serving_receipt"
branch_labels = None
depends_on = None
_TABLE = "provider_directory_profile_capacity_lease_consumption"


def _locked_table():
    """Fence both directions before inspecting immutable consumption history."""
    schema = '"' + (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").replace('"', '""') + '"'
    table = f'{schema}."{_TABLE}"'
    op.execute("SET LOCAL lock_timeout='5s'")
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    return table


def upgrade():
    """Keep globally unique signatures and reservations with purpose-scoped run use."""
    table = _locked_table()
    op.execute(f"ALTER TABLE {table} ADD COLUMN admission_purpose varchar(32) NOT NULL DEFAULT 'profile'")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT pd_profile_capacity_consumption_purpose_check "
        "CHECK (admission_purpose IN ('profile','cms_nonprofile'))"
    )
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT pd_profile_capacity_consumption_run_key")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT pd_profile_capacity_consumption_run_purpose_key "
        "UNIQUE (run_id,admission_purpose)"
    )


def downgrade():
    """Refuse to erase immutable nonprofile admission or its distinction."""
    table = _locked_table()
    op.execute(
        f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM {table} WHERE admission_purpose<>'profile') THEN "
        "RAISE EXCEPTION 'capacity_nonprofile_history_requires_retention'; END IF; END $$"
    )
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT pd_profile_capacity_consumption_run_purpose_key")
    op.execute(f"ALTER TABLE {table} ADD CONSTRAINT pd_profile_capacity_consumption_run_key UNIQUE (run_id)")
    op.execute(f"ALTER TABLE {table} DROP COLUMN admission_purpose")
