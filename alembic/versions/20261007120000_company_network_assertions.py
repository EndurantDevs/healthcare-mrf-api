# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain explicit network roles, benefits, periods and atomic batch receipts."""

import os
import re

from alembic import op

revision = "20261007120000_company_network_assertions"
down_revision = "20261007110000_registry_site_bindings"
branch_labels = None
depends_on = None


def _ddl(schema):
    namespace = '"' + schema + '"'
    return [
        f"ALTER TABLE {namespace}.registry_record_history DROP CONSTRAINT registry_history_custom_revision",
        f"CREATE INDEX registry_history_custom_revision_idx ON {namespace}.registry_record_history(custom_revision)",
        f"ALTER TABLE {namespace}.company_registry_links ADD COLUMN network_assertions JSONB NOT NULL DEFAULT '[]'::jsonb",
        f"""ALTER TABLE {namespace}.company_registry_links ADD CONSTRAINT registry_company_links_assertions CHECK(
          jsonb_typeof(network_assertions)='array' AND jsonb_array_length(network_assertions)<=5000
          AND octet_length(network_assertions::text)<=16777216)""",
        f"""CREATE TABLE {namespace}.registry_company_link_batch (
          batch_id UUID PRIMARY KEY,actor_sha256 VARCHAR(64) NOT NULL,idempotency_key VARCHAR(128) NOT NULL,
          request_sha256 VARCHAR(64) NOT NULL,custom_revision BIGINT NOT NULL,result_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT registry_company_link_batch_identity CHECK(batch_id<>'00000000-0000-0000-0000-000000000000'::uuid),
          CONSTRAINT registry_company_link_batch_actor CHECK(actor_sha256 ~ '^[0-9a-f]{{64}}$'),
          CONSTRAINT registry_company_link_batch_request CHECK(request_sha256 ~ '^[0-9a-f]{{64}}$'),
          CONSTRAINT registry_company_link_batch_key CHECK(idempotency_key<>'' AND idempotency_key=btrim(idempotency_key) AND idempotency_key !~ '[[:cntrl:]]'),
          CONSTRAINT registry_company_link_batch_revision CHECK(custom_revision>0),
          CONSTRAINT registry_company_link_batch_result CHECK(jsonb_typeof(result_json)='object' AND octet_length(result_json::text)<=1048576),
          CONSTRAINT registry_company_link_batch_replay UNIQUE(actor_sha256,idempotency_key))""",
    ]


def upgrade():
    """Add empty assertions without rewriting immutable historical records."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Refuse destructive removal of relationship evidence and retry receipts."""
    raise RuntimeError("Company network assertions require an explicit retained-data migration")
