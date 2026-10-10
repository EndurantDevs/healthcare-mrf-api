# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inactive immutable whole-office human approvals; provisioning stays explicit."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261010010000_registry_ptg_office_approval"
down_revision = "20261009040000_registry_ptg_published_plan_scope"
branch_labels = None
depends_on = None


def _ddl(schema):
    table = _identifier(schema) + '."registry_ptg_office_approval"'
    return [
        f"""CREATE TABLE {table} (
      capture_id uuid PRIMARY KEY CHECK(capture_id<>'00000000-0000-0000-0000-000000000000'::uuid),
      actor_key varchar(64) NOT NULL CHECK(actor_key ~ '^[0-9a-f]{{64}}$'),
      idempotency_key varchar(128) NOT NULL CHECK(btrim(idempotency_key)<>''),
      approval_sha256 varchar(64) NOT NULL CHECK(approval_sha256 ~ '^[0-9a-f]{{64}}$'),
      approval_json jsonb NOT NULL CHECK(jsonb_typeof(approval_json)='object' AND octet_length(approval_json::text)<=131072),
      created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(actor_key,idempotency_key),
      CHECK(COALESCE(approval_json->>'contract'='registry_ptg_office_approval.v1'
        AND approval_json->>'capture_id'=capture_id::text
        AND approval_json->'command'->>'capture_id'=capture_id::text
        AND approval_json->'command'->>'idempotency_key'=idempotency_key
        AND approval_json->'command'->>'operation'='review_registry_ptg_office_capture'
        AND approval_json->'actor'->>'kind'='platform_admin'
        AND approval_json->'actor'->>'client_id'='system'
        AND approval_json->'witness'->>'contract'='registry_ptg_office_witness.v1',false)))""",
        f"REVOKE ALL ON {table} FROM PUBLIC",
    ]


def upgrade():
    """Create inactive storage; protected owner/reader/approval grants are explicit."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Refuse to discard authenticated retained review history."""
    raise RuntimeError("Office approvals require an explicit retained-data migration")
