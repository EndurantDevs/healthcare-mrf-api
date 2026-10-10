# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain optional reviewed network references without rewriting history."""

import os
import re

from alembic import op

revision = "20261009030000_network_catalog_evidence"
down_revision = "20261009020000_company_registry_assertions"
branch_labels = None
depends_on = None


def _ddl(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    return [
        f"""ALTER TABLE "{schema}".network_registry_record
ADD COLUMN catalog_evidence_json JSONB,
ADD CONSTRAINT registry_network_catalog_evidence CHECK (
  catalog_evidence_json IS NULL OR (
    jsonb_typeof(catalog_evidence_json)='object'
    AND octet_length(catalog_evidence_json::text)<=32768
  )
)"""
    ]


def upgrade():
    """Keep parser input at 16 KiB; permit JSONB serializer whitespace."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard retained reviewed evidence through a schema rollback."""
    raise RuntimeError("Network catalog evidence requires an explicit retained-data migration")
