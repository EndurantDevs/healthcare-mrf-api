# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal completed native source admissions independently of refresh checkpoints."""

import os
import re

from alembic import op

revision = "20261010030000_registry_source_admission_receipt"
down_revision = "20261010020000_registry_ptg_office_retention"
branch_labels = None
depends_on = None


def _ddl(schema):
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    return [
        f'''CREATE TABLE "{schema}".registry_source_admission_receipt (
        snapshot_id UUID PRIMARY KEY,
        manifest_sha256 VARCHAR(64) NOT NULL,
        manifest_json JSONB NOT NULL,
        sealed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
        CONSTRAINT registry_admission_receipt_identity CHECK (snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid),
        CONSTRAINT registry_admission_receipt_digest CHECK (manifest_sha256 ~ '^[0-9a-f]{{64}}$'),
        CONSTRAINT registry_admission_receipt_document CHECK (jsonb_typeof(manifest_json)='object' AND octet_length(manifest_json::text)<=16384)
    )'''
    ]


def upgrade():
    """Install the bounded immutable completion-receipt table in the configured schema."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard retained completion authority without an explicit data migration."""
    raise RuntimeError("Completed source receipts require an explicit retained-data migration")
