# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inactive immutable office retention; explicit protected provisioning required."""

from alembic import op
from db.registry_schema import registry_schema
from process.network_address_projection import _identifier

revision = "20261010020000_registry_ptg_office_retention"
down_revision = "20261010010000_registry_ptg_office_approval"
branch_labels = None
depends_on = None


def _ddl(schema):
    namespace = _identifier(schema)
    return [
        f"ALTER TABLE {namespace}.registry_ptg_office_approval ADD CONSTRAINT registry_ptg_office_approval_identity UNIQUE(capture_id,approval_sha256)",
        f"""CREATE TABLE {namespace}.registry_ptg_office_hold (
      capture_id uuid PRIMARY KEY, approval_sha256 varchar(64) NOT NULL,
      hold_sha256 varchar(64) NOT NULL CHECK(hold_sha256 ~ '^[0-9a-f]{{64}}$'),
      hold_json jsonb NOT NULL CHECK(jsonb_typeof(hold_json)='object' AND octet_length(hold_json::text)<=32768),
      created_at timestamptz NOT NULL DEFAULT now(),
      FOREIGN KEY(capture_id,approval_sha256) REFERENCES {namespace}.registry_ptg_office_approval(capture_id,approval_sha256),
      CHECK(COALESCE(hold_json->>'contract'='registry_ptg_office_hold.v1'
        AND hold_json->>'capture_id'=capture_id::text AND hold_json->>'approval_sha256'=approval_sha256,false)))""",
        f"REVOKE ALL ON {namespace}.registry_ptg_office_hold FROM PUBLIC",
    ]


def upgrade():
    """Create inactive native control constraints; do not grant runtime writers."""
    for statement in _ddl(registry_schema()):
        op.execute(statement)


def downgrade():
    """Hold history cannot be removed by an implicit rollback migration."""
    raise RuntimeError("Office retention requires an explicit retained-data migration")
