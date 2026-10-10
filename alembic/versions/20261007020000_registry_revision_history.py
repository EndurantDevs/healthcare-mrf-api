# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain management drafts separately from approved custom revisions."""

import os
import re

from alembic import op

revision = "20261007020000_registry_revision_history"
down_revision = "20261007010000_managed_network_registry"
branch_labels = None
depends_on = None


def _ddl(schema):
    """Render control and append history tables without row hooks."""
    qualified = '"' + schema.replace('"', '""') + '".'
    return [
        f"""CREATE TABLE {qualified}registry_revision_control (
            id integer PRIMARY KEY CHECK (id=1), draft_revision bigint NOT NULL DEFAULT 0,
            approved_revision bigint NOT NULL DEFAULT 0,
            CHECK (draft_revision >= approved_revision AND approved_revision >= 0))""",
        f"""CREATE TABLE {qualified}registry_record_history (
            record_kind varchar(16) NOT NULL, record_key varchar(36) NOT NULL,
            revision bigint NOT NULL, custom_revision bigint NOT NULL,
            record_json jsonb NOT NULL, actor_json jsonb NOT NULL,
            reason varchar(1000) NOT NULL, idempotency_key varchar(128) NOT NULL,
            request_sha256 varchar(64) NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(record_kind,record_key,revision),
            CONSTRAINT registry_history_kind CHECK(record_kind IN ('group','company','network')),
            CONSTRAINT registry_history_revision CHECK(revision>0 AND custom_revision>0),
            CONSTRAINT registry_history_json CHECK(jsonb_typeof(record_json)='object' AND jsonb_typeof(actor_json)='object'),
            CONSTRAINT registry_history_reason CHECK(btrim(reason)<>'' AND btrim(idempotency_key)<>''),
            CONSTRAINT registry_history_digest CHECK(request_sha256 ~ '^[0-9a-f]{{64}}$'),
            CONSTRAINT registry_history_custom_revision UNIQUE(custom_revision),
            CONSTRAINT registry_history_replay UNIQUE(record_kind,record_key,idempotency_key))""",
        f"INSERT INTO {qualified}registry_revision_control(id) VALUES(1)",
    ]


def upgrade():
    """Install the durable revision ledger beside source snapshots."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require reviewed data retention before removing history."""
    raise RuntimeError("Registry history requires an explicit retained-data migration")
