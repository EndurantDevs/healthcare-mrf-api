# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One native legacy enrollment; subsequent cohorts have no DEFAULT scan."""

import sqlalchemy as sa

from db.migration_plan_release_storage_catalog import (
    has_legacy_binding_foreign_key,
    legacy_index_names,
    relation_oid,
    validate_legacy_binding,
)
from db.plan_release_storage import (
    BINDING_TABLE,
    LEGACY_BINDING_TABLE,
    PIN_TABLE,
    REVISION_TABLE,
    storage_identifier,
    storage_relation,
    storage_snapshot_index_sql,
    storage_table_sql,
)

MAX_LEGACY_REVISIONS = 1_000_000
LEGACY_BOUND_CHECK = "plan_release_legacy_cohort_bound"


def _literal(text_value):
    """Quote stored values; identifiers and SQL never come from a package."""
    return "'" + text_value.replace("'", "''") + "'"


def _close_root_acl(connection, schema_name, table_name):
    """Discard inherited creator defaults; explicit admin provisioning follows."""
    relation = storage_relation(schema_name, table_name)
    grants = connection.execute(
        sa.text("""SELECT DISTINCT a.grantee,r.rolname FROM pg_class c
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=to_regclass(:relation)
        AND a.grantee<>c.relowner"""),
        {"relation": relation},
    )
    for grantee, role_name in grants:
        principal = "PUBLIC" if grantee == 0 else storage_identifier(role_name)
        connection.exec_driver_sql(f"REVOKE ALL PRIVILEGES ON TABLE {relation} FROM {principal}")


def _upgrade_plan_release_storage(connection, schema_name):
    """Enroll existing rows/indexes atomically without copying their payload."""
    binding = storage_relation(schema_name, BINDING_TABLE)
    revision = storage_relation(schema_name, REVISION_TABLE)
    legacy = storage_relation(schema_name, LEGACY_BINDING_TABLE)
    connection.exec_driver_sql("SET LOCAL lock_timeout='5s'")
    connection.exec_driver_sql(f"LOCK TABLE {revision} IN SHARE ROW EXCLUSIVE MODE")
    connection.exec_driver_sql(f"LOCK TABLE {binding} IN ACCESS EXCLUSIVE MODE")
    legacy_oid = validate_legacy_binding(connection, schema_name)
    revision_ids = list(
        connection.execute(
            sa.text(f"SELECT serving_revision_id FROM {revision} ORDER BY serving_revision_id LIMIT :limit"),
            {"limit": MAX_LEGACY_REVISIONS + 1},
        ).scalars()
    )
    if len(revision_ids) > MAX_LEGACY_REVISIONS:
        raise RuntimeError("plan_release_storage_legacy_revision_bound_exceeded")
    for index_name in legacy_index_names(connection, legacy_oid):
        if len(index_name.encode()) > 55:
            raise RuntimeError("plan_release_storage_legacy_index_name_bound_exceeded")
        connection.exec_driver_sql(
            f"ALTER INDEX {storage_relation(schema_name, index_name)} RENAME TO {storage_identifier(index_name + '_legacy')}"
        )
    connection.exec_driver_sql(f"ALTER TABLE {binding} RENAME TO {storage_identifier(LEGACY_BINDING_TABLE)}")
    for table_name, is_pin in ((BINDING_TABLE, False), (PIN_TABLE, True)):
        connection.exec_driver_sql(storage_table_sql(schema_name, table_name, pin=is_pin, partitioned=True))
        connection.exec_driver_sql(storage_snapshot_index_sql(schema_name, table_name, table_name + "_snapshot_idx"))
        _close_root_acl(connection, schema_name, table_name)
    if revision_ids:
        bound_values = ",".join(_literal(revision_id) for revision_id in revision_ids)
        # Validate once over the retained heap; later attaches never scan it.
        connection.exec_driver_sql(
            f"ALTER TABLE {legacy} ADD CONSTRAINT {storage_identifier(LEGACY_BOUND_CHECK)} "
            f"CHECK (serving_revision_id IN ({bound_values}))"
        )
        connection.exec_driver_sql(f"ALTER TABLE {binding} ATTACH PARTITION {legacy} FOR VALUES IN ({bound_values})")
    if relation_oid(connection, schema_name, LEGACY_BINDING_TABLE) != legacy_oid:
        raise RuntimeError("plan_release_storage_legacy_identity_changed")


def _downgrade_plan_release_storage(connection, schema_name):
    """Restore the legacy-only schema; reject a destructive post-publication down."""
    binding = storage_relation(schema_name, BINDING_TABLE)
    legacy = storage_relation(schema_name, LEGACY_BINDING_TABLE)
    pin = storage_relation(schema_name, PIN_TABLE)
    connection.exec_driver_sql(f"LOCK TABLE {binding}, {pin}, {legacy} IN ACCESS EXCLUSIVE MODE")
    legacy_oid = relation_oid(connection, schema_name, LEGACY_BINDING_TABLE)
    child_oids = list(
        connection.execute(
            sa.text("SELECT inhrelid FROM pg_inherits WHERE inhparent=to_regclass(:parent)"), {"parent": binding}
        ).scalars()
    )
    if (
        not has_legacy_binding_foreign_key(
            connection, legacy_oid, relation_oid(connection, schema_name, REVISION_TABLE)
        )
        or any(oid != legacy_oid for oid in child_oids)
        or connection.exec_driver_sql(f"SELECT EXISTS(SELECT FROM {pin})").scalar_one()
    ):
        raise RuntimeError("plan_release_storage_downgrade_requires_serving_rollback")
    if child_oids:
        connection.exec_driver_sql(f"ALTER TABLE {binding} DETACH PARTITION {legacy}")
    connection.exec_driver_sql(f"DROP TABLE {pin}")
    connection.exec_driver_sql(f"DROP TABLE {binding}")
    connection.exec_driver_sql(
        f"ALTER TABLE {legacy} DROP CONSTRAINT IF EXISTS {storage_identifier(LEGACY_BOUND_CHECK)}"
    )
    for index_name in legacy_index_names(connection, legacy_oid):
        if not index_name.endswith("_legacy"):
            raise RuntimeError("plan_release_storage_legacy_index_changed")
        connection.exec_driver_sql(
            f"ALTER INDEX {storage_relation(schema_name, index_name)} RENAME TO {storage_identifier(index_name[:-7])}"
        )
    connection.exec_driver_sql(f"ALTER TABLE {legacy} RENAME TO {storage_identifier(BINDING_TABLE)}")


def upgrade_plan_release_storage(connection, schema_name):
    """Keep validation exceptions from leaving partially renamed native objects."""
    with connection.begin_nested():
        _upgrade_plan_release_storage(connection, schema_name)


def downgrade_plan_release_storage(connection, schema_name):
    """Make a rejected schema rollback leave every retained cohort untouched."""
    with connection.begin_nested():
        _downgrade_plan_release_storage(connection, schema_name)
