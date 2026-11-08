# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add cursor access online; native catalogs identify heaps and partition leaves."""

from __future__ import annotations

import hashlib
import os

from sqlalchemy import text

from alembic import op
from db.migration_index_adoption import (
    _create_temporary_index_table,
    _shape_from_catalog,
    _temporary_table_schema,
)
from db.migration_index_catalog import _index_catalog_record

revision = "20261007150000_provider_directory_content_cursor_index"
down_revision = "20261007140000_registry_source_recipes"
branch_labels = None
depends_on = None

INDEX_NAME = "provider_directory_dataset_resource_content_cursor_c_idx"
TABLE_NAME = "provider_directory_dataset_resource"
INDEX_KEYS = 'dataset_id, resource_type COLLATE "C", resource_id COLLATE "C"'


def _schema():
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _create_index_sql(schema, table_name=TABLE_NAME, index_name=INDEX_NAME, *, metadata_only=False):
    method = "" if metadata_only else "CONCURRENTLY "
    target = "ONLY " if metadata_only else ""
    return (
        f"CREATE INDEX {method}IF NOT EXISTS {_q(index_name)} "
        f"ON {target}{_q(schema)}.{_q(table_name)} USING btree ({INDEX_KEYS})"
    )


def _drop_index_sql(schema, index_name=INDEX_NAME, *, partitioned=False):
    method = "" if partitioned else "CONCURRENTLY "
    return f"DROP INDEX {method}IF EXISTS {_q(schema)}.{_q(index_name)}"


def _table_exists(schema):
    return bool(
        op.get_bind().execute(text("SELECT to_regclass(:name)"), {"name": f"{_q(schema)}.{_q(TABLE_NAME)}"}).scalar()
    )


def _expected_index_shape(schema):
    """Use native metadata on an empty temporary table to bind the exact shape."""
    bind = op.get_bind()
    temporary = _create_temporary_index_table(bind, schema, TABLE_NAME)
    try:
        bind.exec_driver_sql(
            f"CREATE INDEX {temporary.quoted_index} ON {temporary.quoted_table} USING btree ({INDEX_KEYS})"
        )
        record = _index_catalog_record(
            op,
            temporary.index_name,
            temporary.table_name,
            _temporary_table_schema(bind, temporary.table_name),
        )
        if record is None:
            raise RuntimeError("temporary_expected_index_missing")
        return _shape_from_catalog(record)
    finally:
        bind.exec_driver_sql(f"DROP TABLE IF EXISTS {temporary.quoted_table}")


def _matching_index_record(schema, expected_shape, table_name=TABLE_NAME, index_name=INDEX_NAME, *, partitioned=False):
    """Reject collisions and rebuild only an exact invalid concurrent residue."""
    index_target = (
        op.get_bind()
        .execute(
            text("""SELECT tables.nspname,table_record.relname
          FROM pg_class index_record JOIN pg_namespace indexes ON indexes.oid=index_record.relnamespace
          LEFT JOIN pg_index metadata ON metadata.indexrelid=index_record.oid
          LEFT JOIN pg_class table_record ON table_record.oid=metadata.indrelid
          LEFT JOIN pg_namespace tables ON tables.oid=table_record.relnamespace
          WHERE indexes.nspname=:schema AND index_record.relname=:index_name"""),
            {"schema": schema, "index_name": index_name},
        )
        .first()
    )
    if index_target is None:
        return None
    if tuple(index_target) != (schema, table_name):
        raise RuntimeError("provider_directory_content_cursor_index_mismatch")
    catalog_record = _index_catalog_record(op, index_name, table_name, schema)
    if catalog_record is None or _shape_from_catalog(catalog_record) != expected_shape:
        raise RuntimeError("provider_directory_content_cursor_index_mismatch")
    if not catalog_record["indislive"] or (catalog_record["indisvalid"] and not catalog_record["indisready"]):
        raise RuntimeError("provider_directory_content_cursor_index_mismatch")
    if partitioned:
        identity = _index_identity(schema, index_name)
        if identity is None or identity["kind"] != "I" or not catalog_record["indisready"]:
            raise RuntimeError("provider_directory_content_cursor_index_mismatch")
        return catalog_record
    if not catalog_record["indisvalid"]:
        return False
    return catalog_record


def _table_kind(schema):
    return (
        op.get_bind()
        .execute(
            text("SELECT relkind::text FROM pg_class WHERE oid=to_regclass(:name)"),
            {"name": f"{_q(schema)}.{_q(TABLE_NAME)}"},
        )
        .scalar()
    )


def _partition_inventory(schema):
    rows = (
        op.get_bind()
        .execute(
            text("""SELECT tree.relid::oid AS oid,tree.parentrelid::oid AS parent_oid,
          tree.level,tree.isleaf,n.nspname AS schema,c.relname AS name,c.relkind::text AS kind,
          c.relpersistence::text,am.amname,pg_get_expr(c.relpartbound,c.oid) AS bound
          FROM pg_partition_tree(to_regclass(:name)) tree
          JOIN pg_class c ON c.oid=tree.relid JOIN pg_namespace n ON n.oid=c.relnamespace
          LEFT JOIN pg_am am ON am.oid=c.relam ORDER BY tree.level,c.oid"""),
            {"name": f"{_q(schema)}.{_q(TABLE_NAME)}"},
        )
        .mappings()
        .all()
    )
    inventory_rows = tuple(dict(row) for row in rows)
    if not inventory_rows or inventory_rows[0]["kind"] != "p":
        raise RuntimeError("provider_directory_content_cursor_partition_inventory_mismatch")
    for row in inventory_rows:
        if row["relpersistence"] != "p" or row["level"] > 1:
            raise RuntimeError("provider_directory_content_cursor_partition_shape_unsupported")
        if row["level"] == 1 and (not row["isleaf"] or row["kind"] != "r" or row["amname"] != "heap"):
            raise RuntimeError("provider_directory_content_cursor_partition_shape_unsupported")
    return inventory_rows


def _leaf_index_name(schema, table_name):
    identity = hashlib.sha256(f"{schema}\0{table_name}".encode()).hexdigest()[:24]
    return f"pd_content_cursor_c_{identity}"


def _index_identity(schema, index_name):
    row = (
        op.get_bind()
        .execute(
            text("""SELECT c.oid,c.relkind::text AS kind,i.indrelid AS table_oid,
          parent.inhparent AS parent_oid FROM pg_class c
          JOIN pg_namespace n ON n.oid=c.relnamespace
          JOIN pg_index i ON i.indexrelid=c.oid
          LEFT JOIN pg_inherits parent ON parent.inhrelid=c.oid
          WHERE n.nspname=:schema AND c.relname=:name"""),
            {"schema": schema, "name": index_name},
        )
        .mappings()
        .first()
    )
    return dict(row) if row is not None else None


def _attached_indexes(parent_oid):
    rows = (
        op.get_bind()
        .execute(
            text("""SELECT c.oid,c.relkind::text AS kind,i.indrelid AS table_oid,
          n.nspname AS schema,c.relname AS name FROM pg_inherits attachment
          JOIN pg_class c ON c.oid=attachment.inhrelid
          JOIN pg_namespace n ON n.oid=c.relnamespace
          JOIN pg_index i ON i.indexrelid=c.oid WHERE attachment.inhparent=:parent_oid"""),
            {"parent_oid": parent_oid},
        )
        .mappings()
        .all()
    )
    index_by_table_oid = {row["table_oid"]: dict(row) for row in rows}
    if len(index_by_table_oid) != len(rows):
        raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
    return index_by_table_oid


def _validate_leaf(index, table, expected, *, parent_oid=None):
    record = _matching_index_record(table["schema"], expected, table["name"], index["name"])
    identity = _index_identity(index["schema"], index["name"])
    if not record or identity is None or identity["kind"] != "i" or identity["table_oid"] != table["oid"]:
        raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
    if identity["parent_oid"] != parent_oid:
        raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
    return identity


def _partition_closure(schema, inventory, expected, *, complete):
    root = _matching_index_record(schema, expected, partitioned=True)
    identity = _index_identity(schema, INDEX_NAME)
    if (
        root is None
        or identity is None
        or identity["table_oid"] != inventory[0]["oid"]
        or identity["parent_oid"] is not None
    ):
        raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
    attached = _attached_indexes(identity["oid"])
    leaf_by_oid = {row["oid"]: row for row in inventory[1:]}
    if set(attached) - set(leaf_by_oid):
        raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
    for table_oid, index in attached.items():
        _validate_leaf(index, leaf_by_oid[table_oid], expected, parent_oid=identity["oid"])
    if complete and (set(attached) != set(leaf_by_oid) or not root["indisvalid"]):
        raise RuntimeError("provider_directory_content_cursor_partition_index_missing")
    return identity, attached


def _metadata_ddl(statement):
    """Bound brief parent DDL waits and restore the caller's timeout exactly."""
    bind = op.get_bind()
    previous = bind.execute(text("SELECT current_setting('lock_timeout')")).scalar()
    bind.execute(text("SELECT set_config('lock_timeout','5s',false)"))
    try:
        bind.exec_driver_sql(statement)
    finally:
        bind.execute(text("SELECT set_config('lock_timeout',:previous,false)"), {"previous": previous})


def _upgrade_partitioned(schema, expected):
    inventory = _partition_inventory(schema)
    context = op.get_context()
    # Each statement commits separately: no parent lock survives a leaf heap scan.
    with context.autocommit_block():
        existing = _matching_index_record(schema, expected, partitioned=True)
        if existing is None:
            _metadata_ddl(_create_index_sql(schema, metadata_only=True))
        parent, attached = _partition_closure(schema, inventory, expected, complete=False)
        expected_index_oid_by_table = {table_oid: index["oid"] for table_oid, index in attached.items()}
        for table in inventory[1:]:
            if table["oid"] in attached:
                continue
            name = _leaf_index_name(table["schema"], table["name"])
            existing = _matching_index_record(table["schema"], expected, table["name"], name)
            identity = _index_identity(table["schema"], name)
            if identity is not None and identity["parent_oid"] is not None:
                raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")
            if existing is False:
                op.get_bind().exec_driver_sql(_drop_index_sql(table["schema"], name))
            if not existing:
                op.get_bind().exec_driver_sql(_create_index_sql(table["schema"], table["name"], name))
            identity = _validate_leaf({"schema": table["schema"], "name": name}, table, expected)
            expected_index_oid_by_table[table["oid"]] = identity["oid"]
        if _partition_inventory(schema) != inventory:
            raise RuntimeError("provider_directory_content_cursor_partition_inventory_mismatch")
        for table in inventory[1:]:
            if table["oid"] not in attached:
                name = _leaf_index_name(table["schema"], table["name"])
                _metadata_ddl(
                    f"ALTER INDEX {_q(schema)}.{_q(INDEX_NAME)} ATTACH PARTITION {_q(table['schema'])}.{_q(name)}"
                )
        if _partition_inventory(schema) != inventory:
            raise RuntimeError("provider_directory_content_cursor_partition_inventory_mismatch")
        completed_parent, completed_indexes = _partition_closure(schema, inventory, expected, complete=True)
        actual_index_oid_by_table = {table_oid: index["oid"] for table_oid, index in completed_indexes.items()}
        if completed_parent["oid"] != parent["oid"] or actual_index_oid_by_table != expected_index_oid_by_table:
            raise RuntimeError("provider_directory_content_cursor_partition_index_mismatch")


def upgrade():
    """Build heap indexes concurrently and attach exact partition coverage."""
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        raise RuntimeError("provider_directory_content_cursor_requires_online_catalog")
    if not _table_exists(schema):
        return
    expected = _expected_index_shape(schema)
    if _table_kind(schema) == "p":
        _upgrade_partitioned(schema, expected)
        return
    if _table_kind(schema) != "r":
        raise RuntimeError("provider_directory_content_cursor_partition_shape_unsupported")
    existing = _matching_index_record(schema, expected)
    if existing is not None and existing is not False:
        return
    with context.autocommit_block():
        if existing is False:
            op.get_bind().exec_driver_sql(_drop_index_sql(schema))
        op.get_bind().exec_driver_sql(_create_index_sql(schema))
    if not _matching_index_record(schema, expected):
        raise RuntimeError("provider_directory_content_cursor_index_missing")


def downgrade():
    """Drop the exact cursor index; partitioned trees need brief native DDL."""
    schema = _schema()
    context = op.get_context()
    if context.as_sql:
        raise RuntimeError("provider_directory_content_cursor_requires_online_catalog")
    if not _table_exists(schema):
        return
    expected = _expected_index_shape(schema)
    is_partitioned = _table_kind(schema) == "p"
    if _matching_index_record(schema, expected, partitioned=is_partitioned) is None:
        return
    if is_partitioned:
        _partition_closure(schema, _partition_inventory(schema), expected, complete=False)
    with context.autocommit_block():
        if is_partitioned:
            _metadata_ddl(_drop_index_sql(schema, partitioned=True))
        else:
            op.execute(_drop_index_sql(schema))
