# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain custom office lists and their independently approved versions."""

import os
import re

from alembic import op

revision = "20261007090000_network_membership_drafts"
down_revision = "20261007080000_manual_directory_registry"
branch_labels = None
depends_on = None


def _ddl(schema):
    namespace = '"' + schema.replace('"', '""') + '"'
    statements = [
        f"""CREATE TABLE {namespace}.network_membership_draft (
        network_id INTEGER PRIMARY KEY CHECK(network_id>0),
        memberships_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK(jsonb_typeof(memberships_json)='array'),
        archived BOOLEAN NOT NULL DEFAULT false,
        revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now())"""
    ]
    for table, constraint in (
        ("registry_record_history", "registry_history_kind"),
        ("registry_approved_record", "registry_approved_kind"),
    ):
        statements.extend(
            (
                f"ALTER TABLE {namespace}.{table} DROP CONSTRAINT {constraint}",
                f"ALTER TABLE {namespace}.{table} ADD CONSTRAINT {constraint} CHECK "
                "(record_kind IN ('group','company','network','company_links','provider','location','membership'))",
            )
        )
    return statements


def upgrade():
    """Create durable exact-office membership drafts beside source snapshots."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Reject destructive removal of retained custom membership drafts."""
    raise RuntimeError("Custom memberships require an explicit retained-data migration")
