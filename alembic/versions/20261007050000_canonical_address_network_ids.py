# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add canonical integer IDs without rebuilding indexes under serving readers."""

import os
import re

from sqlalchemy import text

from alembic import op

revision = "20261007050000_canonical_address_network_ids"
down_revision = "20261007040000_registry_source_evidence"
branch_labels = None
depends_on = None


def upgrade():
    """Add empty canonical arrays with a bounded metadata-lock wait."""
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Address schema identifier is invalid")
    connection = op.get_bind()
    existing_type = connection.scalar(
        text(
            "SELECT a.atttypid::regtype::text FROM pg_attribute a "
            "WHERE a.attrelid=to_regclass(:relation) AND a.attname='canonical_network_ids' "
            "AND a.attnum>0 AND NOT a.attisdropped"
        ),
        {"relation": f'"{schema}".entity_address_unified'},
    )
    if existing_type is not None and existing_type != "integer[]":
        raise ValueError("Canonical network IDs require an integer array")
    connection.execute(text("SET LOCAL lock_timeout='1s'"))
    op.execute(
        f'ALTER TABLE IF EXISTS "{schema}".entity_address_unified '
        "ADD COLUMN IF NOT EXISTS canonical_network_ids INTEGER[] NOT NULL DEFAULT '{}'::integer[]"
    )


def downgrade():
    """Require explicit handling of retained canonical memberships."""
    raise RuntimeError("Canonical network membership requires an explicit retention migration")
