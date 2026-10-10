# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inactive protected receipts for distinct published complete-snapshot plan review."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261009040000_registry_ptg_published_plan_scope"
down_revision = "20261009030000_network_catalog_evidence"
branch_labels = None
depends_on = None


def _ddl(schema):
    table = _identifier(schema) + '."registry_ptg_published_plan_scope"'
    return [
        f"""CREATE TABLE {table} (
          scope_id UUID PRIMARY KEY CHECK(scope_id<>'00000000-0000-0000-0000-000000000000'::uuid),
          scope_key VARCHAR(64) NOT NULL UNIQUE CHECK(scope_key ~ '^[0-9a-f]{{64}}$'),
          actor_key VARCHAR(64) NOT NULL CHECK(actor_key ~ '^[0-9a-f]{{64}}$'),
          idempotency_key VARCHAR(128) NOT NULL CHECK(btrim(idempotency_key)<>''),
          approval_sha256 VARCHAR(64) NOT NULL CHECK(approval_sha256 ~ '^[0-9a-f]{{64}}$'),
          approval_json JSONB NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
          UNIQUE(actor_key,idempotency_key),
          CHECK(jsonb_typeof(approval_json)='object' AND octet_length(approval_json::text)<=131072),
          CONSTRAINT registry_ptg_published_plan_receipt CHECK(COALESCE(
            approval_json->>'contract'='registry_ptg_published_plan_scope.v1'
            AND approval_json->>'scope_id'=scope_id::text
            AND approval_json->>'idempotency_key'=idempotency_key
            AND jsonb_typeof(approval_json->'actor')='object'
            AND approval_json->'actor'->>'kind'='platform_admin'
            AND approval_json->'actor'->>'client_id'='system'
            AND jsonb_typeof(approval_json->'command')='object'
            AND approval_json->'command' ?& ARRAY['scope_id','statement_id','client_id','legal_company_id',
                'network_id','approved_revision','source_file_import_id','plan_id','plan_market_type',
                'file_versions','reason','idempotency_key','ownership','source','coordinates','published_identity']
            AND approval_json->'command'->'published_identity'->>'source_count'
                =jsonb_array_length(approval_json->'command'->'file_versions')::text
            AND approval_json->'evidence'->'source_authority'->'identity'=approval_json->'command'->'published_identity'
            AND jsonb_typeof(approval_json->'command'->'network_id')='number'
            AND (approval_json->'command'->>'network_id')::numeric BETWEEN 1 AND 2147483647
            AND approval_json->'command'->>'operation'='approve_ptg_published_plan_scope'
            AND approval_json->'command'->>'review_type'='published_complete_snapshot_plan'
            AND approval_json->'command'->>'selection_mode'='complete_snapshot_source_set'
            AND approval_json->'command'->>'scope_id'=scope_id::text
            AND approval_json->'command'->>'idempotency_key'=idempotency_key
            AND jsonb_typeof(approval_json->'command'->'file_versions')='array'
            AND jsonb_array_length(approval_json->'command'->'file_versions') BETWEEN 1 AND 128
            AND jsonb_typeof(approval_json->'evidence'->'selected_source_keys')='array'
            AND jsonb_array_length(approval_json->'evidence'->'selected_source_keys')
                =jsonb_array_length(approval_json->'command'->'file_versions')
            AND approval_json->'evidence'->'source_authority'->>'contract'='ptg_published_result_source_authority.v1'
            AND approval_json->'evidence'->>'selection_mode'='complete_snapshot_source_set',false))
        )""",
        f"REVOKE ALL ON {table} FROM PUBLIC",
    ]


def upgrade():
    """Owner/reader/approval grants require explicit protected-store provisioning."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Retained approvals cannot be discarded by an automatic downgrade."""
    raise RuntimeError("Published plan reviews require explicit retained-data migration")
