# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep accepted ACA notifications under the existing protected serving owner."""

import re

from sqlalchemy import text

from alembic import op
from db.registry_aca_source_notification import CREATE_TABLE_SQL, INSERT_COLUMNS, TABLE
from db.registry_schema import registry_schema

revision = "20261018040000_registry_aca_source_notifications"
down_revision = "20261018011000_network_candidate_custody"
branch_labels = None
depends_on = None


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", value) is None:
        raise ValueError("Registry identifier is invalid")
    return '"' + value + '"'


def upgrade():
    """Create closed storage; runtime grants require the separate operator plan."""
    bind = op.get_bind()
    schema = registry_schema()
    namespace = _identifier(schema)
    owner = (
        bind.execute(
            text(
                "SELECT r.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname=:schema "
                "AND c.relname='network_serving_control' AND c.relkind='r' AND c.relpersistence='p'"
            ),
            {"schema": schema},
        )
        .mappings()
        .one_or_none()
    )
    if owner is None:
        raise RuntimeError("ACA notifications require the existing serving owner")
    relation = namespace + "." + TABLE
    bind.execute(text(CREATE_TABLE_SQL.format(namespace=namespace)))
    bind.execute(text(f"ALTER TABLE {relation} OWNER TO {_identifier(owner['rolname'])}"))
    bind.execute(text(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC"))
    grantees = (
        bind.execute(
            text(
                "SELECT DISTINCT r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL (SELECT a.grantee FROM aclexplode(c.relacl) a UNION "
                "SELECT a.grantee FROM pg_attribute att CROSS JOIN LATERAL aclexplode(att.attacl) a "
                "WHERE att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped) acl "
                "JOIN pg_roles r ON r.oid=acl.grantee WHERE n.nspname=:schema AND c.relname=:table "
                "AND r.oid<>c.relowner ORDER BY r.rolname"
            ),
            {"schema": schema, "table": TABLE},
        )
        .scalars()
        .all()
    )
    if len(grantees) > 128:
        raise RuntimeError("ACA notification default ACL inventory exceeds its bound")
    columns = ",".join(_identifier(name) for name in (*INSERT_COLUMNS, "created_at"))
    bind.execute(text(f"REVOKE ALL({columns}) ON {relation} FROM PUBLIC"))
    for role in grantees:
        bind.execute(text(f"REVOKE ALL ON TABLE {relation} FROM {_identifier(role)}"))
        bind.execute(text(f"REVOKE ALL({columns}) ON {relation} FROM {_identifier(role)}"))


def downgrade():
    """Accepted rows are indefinite retention holds; no implicit retirement exists."""
    raise RuntimeError("ACA notifications require an explicit retained-data migration")
