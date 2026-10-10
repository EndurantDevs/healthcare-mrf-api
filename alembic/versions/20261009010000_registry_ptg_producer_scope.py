# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable explicit producer approvals, independent of manual network mappings."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261009010000_registry_ptg_producer_scope"
down_revision = "20261007150000_provider_directory_content_cursor_index"
branch_labels = None
depends_on = None


def _ddl(schema):
    table = _identifier(schema) + '."registry_ptg_producer_scope"'
    return [
        f"""CREATE TABLE {table} (
          scope_id UUID PRIMARY KEY CHECK (scope_id<>'00000000-0000-0000-0000-000000000000'::uuid),
          scope_key VARCHAR(64) NOT NULL UNIQUE CHECK (scope_key ~ '^[0-9a-f]{{64}}$'),
          actor_key VARCHAR(64) NOT NULL CHECK (actor_key ~ '^[0-9a-f]{{64}}$'),
          idempotency_key VARCHAR(128) NOT NULL CHECK (btrim(idempotency_key)<>''),
          approval_sha256 VARCHAR(64) NOT NULL CHECK (approval_sha256 ~ '^[0-9a-f]{{64}}$'),
          approval_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (actor_key,idempotency_key),
          CHECK (jsonb_typeof(approval_json)='object' AND octet_length(approval_json::text)<=131072),
          CHECK (COALESCE(approval_json ?& ARRAY['contract','scope_id','coordinates','client_id','legal_company_id',
            'approved_revision','company_key','cohort_id','binding_source_key','snapshot_id','producer_statement_id',
            'producer_statement_sha256','source_file_import_id','file_versions','actor','reason','idempotency_key','evidence']
            AND approval_json->>'contract'='registry_ptg_producer_scope.v1'
            AND approval_json->>'scope_id'=scope_id::text
            AND approval_json->>'idempotency_key'=idempotency_key
            AND jsonb_typeof(approval_json->'actor')='object'
            AND jsonb_typeof(approval_json->'coordinates')='object'
            AND approval_json->'coordinates'->>'source_system'='ptg'
            AND jsonb_typeof(approval_json->'evidence')='object'
            AND jsonb_typeof(approval_json->'file_versions')='array'
            AND jsonb_array_length(approval_json->'file_versions') BETWEEN 1 AND 128
            AND jsonb_typeof(approval_json->'evidence'->'selected_dense_source_keys')='array'
            AND jsonb_array_length(approval_json->'evidence'->'selected_dense_source_keys')
              =jsonb_array_length(approval_json->'file_versions'),false))
        )""",
        f"REVOKE ALL ON {table} FROM PUBLIC",
    ]


def upgrade():
    """Create inactive storage; an explicit protected owner and ACL policy is required."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Require explicit retention handling before removing authenticated approvals."""
    raise RuntimeError("Producer approvals require an explicit retained-data migration")
