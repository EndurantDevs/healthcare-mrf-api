# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain bounded raw-source recipes independently of resolved memberships."""

import os
import re

from alembic import op

revision = "20261007140000_registry_source_recipes"
down_revision = "20261007130000_registry_network_bindings"
branch_labels = None
depends_on = None


def _ddl(schema):
    namespace = '"' + schema.replace('"', '""') + '"'
    return [
        f"ALTER TABLE {namespace}.network_membership_candidate "
        "ADD COLUMN source_recipes_json JSONB NOT NULL DEFAULT '[]'::jsonb, "
        "ADD CONSTRAINT network_candidate_source_recipes CHECK "
        "(jsonb_typeof(source_recipes_json)='array' AND jsonb_array_length(source_recipes_json)<=100 "
        "AND octet_length(source_recipes_json::text)<=1048576)"
    ]


def upgrade():
    """Add immutable replay inputs without changing retained source snapshots."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require explicit retention handling before removing replay evidence."""
    raise RuntimeError("Source recipes require an explicit retained-data migration")
