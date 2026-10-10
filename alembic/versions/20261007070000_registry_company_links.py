# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain company links without extending group network membership."""

import os
import re

from alembic import op

revision = "20261007070000_registry_company_links"
down_revision = "20261007060000_registry_approved_selection"
branch_labels = None
depends_on = None


def _ddl(schema):
    """Add native relationship storage and existing history kind support."""
    namespace = '"' + schema.replace('"', '""') + '"'
    statements = [
        f"""CREATE TABLE {namespace}.company_registry_links (
        company_id UUID PRIMARY KEY, network_ids INTEGER[] NOT NULL DEFAULT '{{}}', group_id UUID,
        archived BOOLEAN NOT NULL DEFAULT false, revision BIGINT NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT registry_company_links_revision CHECK (revision>0),
        CONSTRAINT registry_company_links_networks CHECK (
            cardinality(network_ids)<=5000 AND array_position(network_ids,NULL) IS NULL AND 0<ALL(network_ids)),
        CONSTRAINT registry_company_links_group CHECK (
            group_id IS NULL OR group_id<>'00000000-0000-0000-0000-000000000000'::uuid))"""
    ]
    for table, constraint in (
        ("registry_record_history", "registry_history_kind"),
        ("registry_approved_record", "registry_approved_kind"),
    ):
        statements.extend(
            (
                f"ALTER TABLE {namespace}.{table} DROP CONSTRAINT {constraint}",
                f"ALTER TABLE {namespace}.{table} ADD CONSTRAINT {constraint} CHECK "
                "(record_kind IN ('group','company','network','company_links'))",
            )
        )
    return statements


def upgrade():
    """Preserve every prior draft and approval while enabling explicit links."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse to discard retained relationship drafts or approval history."""
    raise RuntimeError("Company relationships require an explicit retained-data migration")
