# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate the legacy heap before enrolling it without rewriting rows."""

import uuid

import sqlalchemy as sa

from db.plan_release_storage import BINDING_TABLE, REVISION_TABLE, storage_relation, storage_table_sql

_COLUMNS_SQL = """SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
 pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a LEFT JOIN pg_attrdef d
 ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"""
_CONSTRAINTS_SQL = """SELECT contype::text,pg_get_constraintdef(oid),convalidated
 FROM pg_constraint WHERE conrelid=:oid AND contype IN ('p','c') ORDER BY contype,pg_get_constraintdef(oid)"""


def relation_oid(connection, schema_name, table_name):
    """Require an existing server-selected relation, never substitute a name."""
    qualified_name = storage_relation(schema_name, table_name)
    oid = connection.execute(sa.text("SELECT to_regclass(:name)::oid"), {"name": qualified_name}).scalar_one()
    if oid is None:
        raise RuntimeError("plan_release_storage_relation_missing")
    return oid


def _native_shape(connection, oid):
    """Capture native column/default and canonical essential constraint shape."""
    return (
        [tuple(column) for column in connection.execute(sa.text(_COLUMNS_SQL), {"oid": oid})],
        [tuple(constraint) for constraint in connection.execute(sa.text(_CONSTRAINTS_SQL), {"oid": oid})],
    )


def validate_legacy_binding(connection, schema_name):
    """Prove the old FK and keys before deriving bounds from the header PK."""
    oid = relation_oid(connection, schema_name, BINDING_TABLE)
    header_oid = relation_oid(connection, schema_name, REVISION_TABLE)
    template_name = "plan_release_shape_" + uuid.uuid4().hex
    connection.exec_driver_sql(storage_table_sql("pg_temp", template_name, temporary=True))
    try:
        expected = _native_shape(connection, relation_oid(connection, "pg_temp", template_name))
        actual = _native_shape(connection, oid)
        if actual != expected:
            raise RuntimeError("plan_release_storage_legacy_shape_changed")
    finally:
        connection.exec_driver_sql(f"DROP TABLE {storage_relation('pg_temp', template_name)}")
    protected = connection.execute(
        sa.text("""SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
        AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
        AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid)
        AND NOT EXISTS(SELECT FROM pg_constraint WHERE confrelid=c.oid)
        AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=c.oid AND (NOT indisvalid OR NOT indisready))
        FROM pg_class c WHERE c.oid=:oid"""),
        {"oid": oid, "header": header_oid},
    ).scalar_one()
    if protected is not True or not has_legacy_binding_foreign_key(connection, oid, header_oid):
        raise RuntimeError("plan_release_storage_legacy_authority_changed")
    return oid


def legacy_index_names(connection, oid):
    """Keep the original physical index OIDs while freeing canonical root names."""
    return list(
        connection.execute(
            sa.text("""SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            WHERE i.indrelid=:oid ORDER BY c.relname"""),
            {"oid": oid},
        ).scalars()
    )


def has_legacy_binding_foreign_key(connection, oid, header_oid):
    """Require the complete original revision FK before a reversible legacy rollback."""
    return (
        connection.execute(
            sa.text("""SELECT count(*)=1 AND bool_and(convalidated AND NOT condeferrable AND confrelid=:header
          AND confdeltype='c' AND confupdtype='a'
          AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=:oid
            AND attname='serving_revision_id')]::smallint[]
          AND confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=:header
            AND attname='serving_revision_id')]::smallint[])
          FROM pg_constraint WHERE conrelid=:oid AND contype='f'"""),
            {"oid": oid, "header": header_oid},
        ).scalar_one()
        is True
    )
