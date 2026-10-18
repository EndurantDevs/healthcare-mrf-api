# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain immutable CMS notification evidence without publication authority."""

import os
import re

from sqlalchemy import text

from alembic import op

revision = "20261017010000_registry_cms_source_notifications"
down_revision = "20261016070000_registry_allowed_plan_scope"
branch_labels = None
depends_on = None


def _namespace(schema):
    if type(schema) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    return '"' + schema + '"'


def _schema():
    return os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _ddl(schema):
    namespace = _namespace(schema)
    return (
        f"""CREATE TABLE {namespace}.registry_cms_source_notification (
 source_schema text NOT NULL, receipt_id text NOT NULL CHECK(receipt_id ~ '^[0-9a-f]{{64}}$'),
 capture_id uuid NOT NULL, publication_xid xid8 NOT NULL, profile_generation_id text NOT NULL,
 notification_sha256 text NOT NULL CHECK(notification_sha256 ~ '^[0-9a-f]{{64}}$'),
 notification_json text NOT NULL CHECK(octet_length(notification_json)<=71680),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_schema,receipt_id),
 CHECK(notification_sha256=encode(sha256(convert_to(notification_json,'UTF8')),'hex'))
)""",
        f"REVOKE ALL ON {namespace}.registry_cms_source_notification FROM PUBLIC",
    )


def upgrade():
    """Create frozen native storage; explicit operator configuration owns runtime grants."""
    for statement in _ddl(_schema()):
        op.execute(statement)


def downgrade():
    """Refuse to discard populated immutable history before dropping empty storage."""
    table = _namespace(_schema()) + ".registry_cms_source_notification"
    op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(text(f"SELECT EXISTS(SELECT FROM {table})")):
        raise RuntimeError("CMS source notifications require an explicit retained-data migration")
    op.execute(f"DROP TABLE {table}")
