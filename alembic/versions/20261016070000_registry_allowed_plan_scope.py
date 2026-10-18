# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Extend the protected review receipt CHECK with an explicit allowed role."""

from sqlalchemy import text

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261016070000_registry_allowed_plan_scope"
down_revision = "20261015020000_custom_import_query_projections"
branch_labels = None
depends_on = None

_BASE_CHECK = """COALESCE(
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
 AND approval_json->'command'->>'scope_id'=scope_id::text
 AND approval_json->'command'->>'idempotency_key'=idempotency_key
 AND jsonb_typeof(approval_json->'command'->'file_versions')='array'
 AND jsonb_array_length(approval_json->'command'->'file_versions') BETWEEN 1 AND 128
 AND jsonb_typeof(approval_json->'evidence'->'selected_source_keys')='array'
 AND jsonb_array_length(approval_json->'evidence'->'selected_source_keys')
   =jsonb_array_length(approval_json->'command'->'file_versions')
 AND {role_predicate},false)"""


def _receipt_check(include_allowed):
    role_pairs = (
        "('published_complete_snapshot_plan','complete_snapshot_source_set',"
        "'ptg_published_result_source_authority.v1','complete_snapshot_source_set')"
    )
    if include_allowed:
        role_pairs += (
            ",('published_allowed_amount_plan','complete_allowed_snapshot_source_set',"
            "'ptg_published_allowed_source_authority.v1','complete_allowed_snapshot_source_set')"
        )
    role_predicate = (
        "(approval_json->'command'->>'review_type',"
        "approval_json->'command'->>'selection_mode',"
        "approval_json->'evidence'->'source_authority'->>'contract',"
        "approval_json->'evidence'->>'selection_mode') IN (" + role_pairs + ")"
    )
    return _BASE_CHECK.format(role_predicate=role_predicate)


def _ddl(schema, *, include_allowed):
    table = _identifier(schema) + '."registry_ptg_published_plan_scope"'
    return (
        f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE",
        f"ALTER TABLE {table} DROP CONSTRAINT registry_ptg_published_plan_receipt",
        f"ALTER TABLE {table} ADD CONSTRAINT registry_ptg_published_plan_receipt CHECK({_receipt_check(include_allowed)})",
    )


def upgrade():
    """Keep original receipt invariants and permit only the new closed role tuple."""
    for statement in _ddl(registry_schema(), include_allowed=True):
        op.execute(statement)


def downgrade():
    """Refuse retained allowed reviews before restoring the negotiated-only CHECK."""
    schema = registry_schema()
    statements = _ddl(schema, include_allowed=False)
    op.execute(statements[0])
    if op.get_bind().scalar(
        text(
            f"SELECT EXISTS(SELECT FROM {_identifier(schema)}.registry_ptg_published_plan_scope "
            "WHERE approval_json->'command'->>'review_type'='published_allowed_amount_plan')"
        )
    ):
        raise RuntimeError("Retained allowed reviews require explicit migration")
    for statement in statements[1:]:
        op.execute(statement)
