# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add reviewed source binding heads and immutable batch history."""

import os
import re

from alembic import op

revision = "20261007130000_registry_network_bindings"
down_revision = "20261007120000_company_network_assertions"
branch_labels = None
depends_on = None


def _ddl(schema):
    """Add native objects without changing imported snapshots or existing aliases."""
    namespace = '"' + schema.replace('"', '""') + '"'
    statements = [
        f"""CREATE TABLE {namespace}.registry_network_binding (
          binding_id UUID PRIMARY KEY,source_system VARCHAR(16) NOT NULL,
          source_id VARCHAR(128) NOT NULL,dataset_schema VARCHAR(63) NOT NULL,
          dataset_id VARCHAR(128) NOT NULL,producer_id VARCHAR(128) NOT NULL,
          edition_id VARCHAR(128) NOT NULL,source_key VARCHAR(512) NOT NULL,
          source_scope_json JSONB NOT NULL,binding_key VARCHAR(64) NOT NULL,
          network_id INTEGER NOT NULL,evidence_id VARCHAR(512) NOT NULL,evidence_sha256 VARCHAR(64) NOT NULL,
          archived BOOLEAN NOT NULL DEFAULT false,revision BIGINT NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT registry_network_binding_identity CHECK(binding_id<>'00000000-0000-0000-0000-000000000000'::uuid),
          CONSTRAINT registry_network_binding_system CHECK(source_system IN ('aca','ptg','fhir')),
          CONSTRAINT registry_network_binding_scope CHECK(btrim(source_id)<>'' AND btrim(dataset_schema)<>''
            AND btrim(dataset_id)<>'' AND btrim(producer_id)<>'' AND btrim(edition_id)<>'' AND btrim(source_key)<>''),
          CONSTRAINT registry_network_binding_scope_json CHECK(jsonb_typeof(source_scope_json)='object'
            AND octet_length(source_scope_json::text)<=16384),
          CONSTRAINT registry_network_binding_evidence CHECK(binding_key ~ '^[0-9a-f]{{64}}$'
            AND evidence_sha256 ~ '^[0-9a-f]{{64}}$' AND btrim(evidence_id)<>''),
          CONSTRAINT registry_network_binding_revision CHECK(network_id>0 AND revision>0),
          CONSTRAINT registry_network_binding_key UNIQUE(binding_key))""",
        f"CREATE INDEX registry_network_binding_network_idx ON {namespace}.registry_network_binding(network_id,source_system,archived)",
        f"""CREATE TABLE {namespace}.registry_network_binding_batch (
          actor_key VARCHAR(128) NOT NULL,idempotency_key VARCHAR(128) NOT NULL,
          request_sha256 VARCHAR(64) NOT NULL,receipt_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(actor_key,idempotency_key),
          CONSTRAINT registry_network_binding_batch_key CHECK(btrim(actor_key)<>'' AND btrim(idempotency_key)<>''),
          CONSTRAINT registry_network_binding_batch_digest CHECK(request_sha256 ~ '^[0-9a-f]{{64}}$'),
          CONSTRAINT registry_network_binding_batch_receipt CHECK(jsonb_typeof(receipt_json)='object'
            AND octet_length(receipt_json::text)<=1048576))""",
    ]
    for table, constraint in (
        ("registry_record_history", "registry_history_kind"),
        ("registry_approved_record", "registry_approved_kind"),
    ):
        statements.append(f"ALTER TABLE {namespace}.{table} DROP CONSTRAINT {constraint}")
        statements.append(
            f"ALTER TABLE {namespace}.{table} ADD CONSTRAINT {constraint} CHECK "
            "(record_kind IN ('group','company','network','company_links','provider','location','membership','site_binding','network_binding'))"
        )
    return statements


def upgrade():
    """Retain durable review records outside source-table swaps."""
    schema = os.getenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None:
        raise ValueError("Registry schema identifier is invalid")
    for statement in _ddl(schema):
        op.execute(statement)


def downgrade():
    """Require an explicit retained-data migration before dropping review history."""
    raise RuntimeError("Network source bindings require an explicit retained-data migration")
