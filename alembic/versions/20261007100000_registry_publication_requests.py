# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Queue bounded publication commands for the separate protected controller."""

import os
import re

from alembic import op

revision = "20261007100000_registry_publication_requests"
down_revision = "20261007090000_network_membership_drafts"
branch_labels = None
depends_on = None


def upgrade():
    """Create idempotent request control and native lease constraints."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    namespace = '"' + schema + '"'
    op.execute(f"""CREATE TABLE {namespace}.registry_publication_request (
      request_id UUID PRIMARY KEY CHECK(request_id<>'00000000-0000-0000-0000-000000000000'::uuid),
      actor_key VARCHAR(64) NOT NULL, session_token_sha256 VARCHAR(64) NOT NULL,
      actor_json JSONB NOT NULL, command_json JSONB NOT NULL,
      idempotency_key VARCHAR(128) NOT NULL, request_sha256 VARCHAR(64) NOT NULL,
      state VARCHAR(16) NOT NULL DEFAULT 'queued', lease_id UUID, lease_expires_at TIMESTAMPTZ,
      result_json JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT registry_request_state CHECK(state IN ('queued','running','completed','rejected')),
      CONSTRAINT registry_request_digests CHECK(actor_key ~ '^[0-9a-f]{{64}}$'
        AND session_token_sha256 ~ '^[0-9a-f]{{64}}$' AND request_sha256 ~ '^[0-9a-f]{{64}}$'),
      CONSTRAINT registry_request_json CHECK(jsonb_typeof(actor_json)='object' AND jsonb_typeof(command_json)='object'
        AND (result_json IS NULL OR jsonb_typeof(result_json)='object')),
      CONSTRAINT registry_request_replay_key CHECK(btrim(idempotency_key)<>''),
      CONSTRAINT registry_request_lease CHECK((state='running' AND lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (state<>'running' AND lease_id IS NULL AND lease_expires_at IS NULL)),
      CONSTRAINT registry_request_replay UNIQUE(actor_key,idempotency_key))""")
    op.execute(
        f"CREATE INDEX registry_publication_request_pending ON {namespace}.registry_publication_request(created_at,request_id) WHERE state IN ('queued','running')"
    )


def downgrade():
    """Reject destructive removal of retained publication requests."""
    raise RuntimeError("Publication requests require explicit retained-data migration")
