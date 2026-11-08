# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit bounded references to candidate-local source inventories."""

import os
import re

from alembic import op
from db.registry_source_inventory_schema import source_recipe_storage_check

revision = "20261029010000_registry_source_inventory"
down_revision = "20261028020000_registry_membership_refresh"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    return (
        f'ALTER TABLE "{schema}".network_membership_candidate '
        "DROP CONSTRAINT network_candidate_source_recipes, "
        "ADD CONSTRAINT network_candidate_source_recipes CHECK (" + source_recipe_storage_check() + ")"
    )


def upgrade():
    """Keep historical arrays valid while registering the new reference syntax."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    op.execute(_ddl(schema))


def downgrade():
    """Retained inventory references require an explicit data migration."""
    raise RuntimeError("Source inventory references require an explicit retained-data migration")
