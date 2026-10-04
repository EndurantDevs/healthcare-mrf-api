# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve exact held dependency identities through geo-assurance publication."""

import os
import re

from alembic import op

revision = "20261004000000_geo_assurance_dependency_bindings"
down_revision = "20261003000000_address_alias_generation_guard"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    schema = runtime or legacy or "mrf"
    if (runtime and legacy and runtime != legacy) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise RuntimeError("database schema must be one matching simple identifier")
    return schema


def upgrade():
    """NULL preserves canonical dependency resolution for existing projections."""
    op.execute(
        f'ALTER TABLE "{_schema()}".entity_address_geo_assurance_state '
        "ADD COLUMN active_dependency_bindings jsonb, ADD COLUMN candidate_dependency_bindings jsonb"
    )


def downgrade():
    """Drop unused nullable bindings without erasing any held projection identity."""
    table = f'"{_schema()}".entity_address_geo_assurance_state'
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {table} "
            "WHERE active_dependency_bindings IS NOT NULL OR candidate_dependency_bindings IS NOT NULL)"
        )
        .scalar()
    ):
        raise RuntimeError("held geo-assurance bindings require explicit retirement before downgrade")
    op.execute(f"ALTER TABLE {table} DROP COLUMN active_dependency_bindings, DROP COLUMN candidate_dependency_bindings")
