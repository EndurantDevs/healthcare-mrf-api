# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep editable company name provenance explicit without rewriting retained history."""

import os
import re

from alembic import op

revision = "20261012010000_company_registry_name_origin"
down_revision = "20261011010000_company_registry_group_assertions"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    return [
        f"ALTER TABLE {namespace}.company_registry "
        "ADD COLUMN display_name_origin VARCHAR(32) DEFAULT 'unknown' NOT NULL",
        f"ALTER TABLE {namespace}.company_registry ADD CONSTRAINT registry_company_name_origin "
        "CHECK (display_name_origin IN ('manual','source_company','source_issuer','identifier','unknown'))",
    ]


def upgrade():
    """Add only provenance; existing names and immutable record JSON remain untouched."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard provenance without an explicit retained-data migration."""
    raise RuntimeError("Company name origin requires an explicit retained-data migration")
