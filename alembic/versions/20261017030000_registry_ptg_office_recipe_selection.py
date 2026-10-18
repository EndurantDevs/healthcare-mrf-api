"""Retain a reviewed exact office recipe beside the unchanged plan source proof."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261017030000_registry_ptg_office_recipe_selection"
down_revision = "20261017020000_source_registry_snapshot_join"
branch_labels = None
depends_on = None


def _ddl(schema):
    table = _identifier(schema) + ".registry_network_binding"
    constraint = "office_recipe_json IS NULL OR COALESCE((source_system='ptg'\n AND source_scope_json->>'review_type'='published_complete_snapshot_plan'\n AND jsonb_typeof(office_recipe_json)='object' AND octet_length(office_recipe_json::text)<=256\n AND office_recipe_json ?& ARRAY['capture_id','recipe_sha256']\n AND office_recipe_json - ARRAY['capture_id','recipe_sha256']='{}'::jsonb\n AND jsonb_typeof(office_recipe_json->'capture_id')='string'\n AND jsonb_typeof(office_recipe_json->'recipe_sha256')='string'\n AND office_recipe_json->>'capture_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'\n AND office_recipe_json->>'capture_id'<>'00000000-0000-0000-0000-000000000000'\n AND office_recipe_json->>'recipe_sha256' ~ '^[0-9a-f]{64}$'),false)"
    return [
        f"ALTER TABLE {table} ADD COLUMN office_recipe_json jsonb",
        f"ALTER TABLE {table} ADD CONSTRAINT registry_network_binding_office_recipe CHECK ({constraint})",
    ]


def upgrade():
    """Add nullable selection storage without changing existing records or plan evidence."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Preserve the immutable reviewed selection history until an explicit migration."""
    raise RuntimeError("Office recipe selections require an explicit retained-data migration")
