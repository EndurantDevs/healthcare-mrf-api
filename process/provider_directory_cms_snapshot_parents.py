# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render and read future payload parents, without changing existing CMS storage.

Only fixed columns, native CHECK/key constraints and equivalent indexes are
created. Existing FKs, guards, data, owners and callables stay untouched. The
caller authenticates creation provenance and owns execution and rollback;
these statements neither issue authority nor perform the eventual conversion.
"""

import hashlib
from uuid import UUID

from db.migration_cms_snapshot_allocator import _ADMIN_SQL
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_canonical_layout as layout
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_prepared_workspace as workspace
from process.network_cms_registry_address_capture import NativeReceiptSession
from process.provider_directory_cms_allocation_semantics import _EXTRA_SQL
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

_ENVIRONMENT_SQL = """SELECT current_setting('server_version_num')::integer AS version,
 current_setting('search_path') AS search_path,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_oid,
 n.oid::bigint AS schema_oid,n.nspowner::bigint AS namespace_owner_oid,quote_ident(n.nspname) AS schema_sql
 FROM pg_catalog.pg_namespace n WHERE n.nspname=$1"""
_FOREIGN_SQL = """SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_constraint
 WHERE contype='f' AND (conrelid=ANY($1::oid[]) OR confrelid=ANY($1::oid[])))"""
_PARTITIONS_SQL = """SELECT count(*)=2 AND bool_and(partstrat='l' AND partattrs='1'::pg_catalog.int2vector
 AND partexprs IS NULL AND partdefid=0) FROM pg_catalog.pg_partitioned_table WHERE partrelid=ANY($1::oid[])"""


def cms_snapshot_parent_name(preparation_id, root):
    """Bind a temporary parent name to one fixed preparation and payload family."""
    if (
        type(preparation_id) is not str
        or str(UUID(preparation_id)) != preparation_id
        or root not in workspace.storage.RELATIONS[-2:]
    ):
        raise ValueError("cms_snapshot_parents_preparation_required")
    return "pd_cms_future_" + hashlib.sha256((preparation_id + ":" + root).encode()).hexdigest()[:24]


def cms_snapshot_parent_statements(schema, root, name):
    """Render an empty LIST parent from the same fixed layout as private payloads."""
    if root not in workspace.storage.RELATIONS:
        raise ValueError("cms_snapshot_parents_root_required")
    relation = storage_relation(schema, name)
    spec = workspace._LAYOUTS[root]
    constraints = [
        "CONSTRAINT " + storage_identifier(name + "_pkey" if kind == "p" else key) + " " + definition
        for key, kind, definition, _columns in workspace._payload_constraints(root)
        if kind in {"c", "p", "u"}
    ]
    statements = [
        f"CREATE TABLE {relation}({','.join(workspace._layouts()[root][0] + constraints)}) "
        "PARTITION BY LIST(dataset_id)"
    ]
    for index, (_key, definition, *_semantics, primary) in enumerate(spec["indexes"]):
        if not primary:
            method = definition.split(" USING ", 1)[1]
            statements.append(
                f"CREATE INDEX {storage_identifier(name + '_i' + str(index))} ON {relation} USING {method}"
            )
    return tuple(statements)


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_parents_" + code)


async def _environment(connection, source_binding):
    _require(type(source_binding) is FHIRSourceStorageBinding, "source_binding_required")
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    observed = await connection.fetchrow(_ENVIRONMENT_SQL, source_binding.catalog_schema)
    _require(
        observed is not None
        and 180000 <= observed["version"] < 190000
        and observed["search_path"] in {"pg_catalog", "pg_catalog, pg_temp"}
        and observed["database_oid"] == source_binding.database_oid
        and observed["schema_oid"] == source_binding.catalog_schema_oid
        and observed["namespace_owner_oid"] == source_binding.namespace_owner_oid,
        "catalog_changed",
    )
    return dict(observed)


def _require_parent(entry, source_binding, name, extra, root):
    native = entry["native"]
    _require(
        entry["nspname"] == source_binding.catalog_schema
        and entry["relname"] == name
        and entry["relowner"] == source_binding.namespace_owner_oid
        and entry["relkind"] == "p"
        and entry["relpersistence"] == "p"
        and native["namespace"][:3]
        == [source_binding.catalog_schema_oid, source_binding.catalog_schema, source_binding.namespace_owner_oid]
        and native["relation"][5:10] == [False, False, False, 0, None]
        and native["relation"][11:] == [None, "LIST (dataset_id)"]
        and not any(native[key] for key in ("parents", "triggers", "rules")),
        "parent_changed",
    )
    _require(
        all(extra[field] is True for field in ("owner_protected", "acl_closed", "no_policies", "default_storage"))
        and all(not topology[1] and topology[3:] == ["p", 0] for topology in custody._decoded(extra["index_topology"])),
        "parent_custody_changed",
    )
    _require(
        all(index["class"][1:] == [source_binding.namespace_owner_oid, None, None] for index in native["indexes"]),
        "index_custody_changed",
    )
    _require(
        root["relname"] == name
        and root["relkind"] == "p"
        and root["amname"] is None
        and root["relreplident"] == "d"
        and not any(
            root[field]
            for field in (
                "relispartition",
                "relrowsecurity",
                "relforcerowsecurity",
                "inherited",
                "dropped",
                "rules",
                "policies",
            )
        ),
        "parent_native_changed",
    )


async def _require_layout(connection, entry, root, schema_sql):
    oid, name = entry["oid"], entry["relname"]
    spec = layout._LAYOUTS[root]
    columns = await connection.fetch(layout._COLUMNS_SQL, [oid], sorted(layout._TYPES))
    constraints = await connection.fetch(layout._CONSTRAINTS_SQL, [oid])
    indexes = await connection.fetch(layout._INDEXES_SQL, [oid])
    expected_constraints = [
        (name + "_pkey" if kind == "p" else key, kind, definition, keys)
        for key, kind, definition, keys in spec["constraints"]
        if kind != "f"
    ]
    expected_indexes = [
        (
            name + "_pkey" if index[-1] else name + "_i" + str(position),
            "CREATE "
            + ("UNIQUE " if index[-2] else "")
            + "INDEX "
            + (name + "_pkey" if index[-1] else name + "_i" + str(position))
            + " ON ONLY __schema__."
            + name
            + " USING "
            + index[1].split(" USING ", 1)[1],
            *index[2:],
        )
        for position, index in enumerate(spec["indexes"])
    ]
    layout._require_columns(columns, spec["columns"], root, schema_sql)
    layout._require_constraints(constraints, expected_constraints, spec["columns"], root, schema_sql, frozenset())
    layout._require_indexes(indexes, expected_indexes, root, schema_sql)


async def read_cms_snapshot_parents(connection, source_binding, preparation_id):
    """Read two exact empty-parent inventories, without issuance or writer closure.

    Source/profile authority and original creation provenance are external.
    This point-in-time read checks protected native ownership and fixed layout;
    the caller still protects role/callee/DDL stability through its operation.
    """
    names_by_root = {root: cms_snapshot_parent_name(preparation_id, root) for root in workspace.storage.RELATIONS[-2:]}
    environment = await _environment(connection, source_binding)
    schema = source_binding.catalog_schema
    inventory = await custody._inventory(connection, schema, tuple(names_by_root.values()))
    _require(len(inventory) == 2, "descendants_changed")
    by_name = {entry["relname"]: entry for entry in inventory}
    _require(set(by_name) == set(names_by_root.values()), "parents_changed")
    oids = [entry["oid"] for entry in inventory]
    _require(len(set(oids)) == 2, "parents_changed")
    extras_by_oid = {entry["oid"]: entry for entry in await connection.fetch(_EXTRA_SQL, oids)}
    roots_by_oid = {
        entry["oid"]: entry
        for entry in await connection.fetch(layout._ROOTS_SQL, [storage_relation(schema, name) for name in by_name])
    }
    _require(set(extras_by_oid) == set(roots_by_oid) == set(oids), "parents_changed")
    _require(await connection.fetchval(_PARTITIONS_SQL, oids) is True, "partition_shape_changed")
    _require(await connection.fetchval(_FOREIGN_SQL, oids) is True, "foreign_edge_changed")
    _require(
        await connection.fetchval(
            layout._DEPENDENCIES_SQL, oids, schema, storage_relation(schema, workspace.storage.HEADER), []
        )
        is True,
        "dependencies_changed",
    )
    for root, name in names_by_root.items():
        entry = by_name[name]
        _require_parent(entry, source_binding, name, extras_by_oid[entry["oid"]], roots_by_oid[entry["oid"]])
        await _require_layout(connection, entry, root, environment["schema_sql"])
    _require(
        await custody._inventory(connection, schema, tuple(names_by_root.values())) == inventory
        and await _environment(connection, source_binding) == environment,
        "parents_changed",
    )
    return {root: by_name[name] for root, name in names_by_root.items()}


async def create_cms_snapshot_parents(connection, source_binding, preparation_id):
    """Create exactly two empty future parents without issuing CMS authority.

    The original Admin caller authenticates its source/profile/claim, protects
    callable/role/DDL stability and records provenance in the same transaction.
    Existing objects refuse; recovery reads its protected allocation instead.
    No existing heap, grant or serving pointer changes. The caller commits.
    """
    names_by_root = {root: cms_snapshot_parent_name(preparation_id, root) for root in workspace.storage.RELATIONS[-2:]}
    await _environment(connection, source_binding)
    _require(await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True, "original_admin_required")
    actor_by_field = dict(await connection.fetchrow(custody._ACTOR_SQL))
    owner = await connection.fetchval(
        "SELECT rolname::text FROM pg_catalog.pg_roles WHERE oid=$1::oid", source_binding.namespace_owner_oid
    )
    _require(type(owner) is str, "owner_required")
    owner_sql = storage_identifier(owner)
    async with workspace._savepoint(connection):
        _require(
            await connection.fetchval(
                "SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_class WHERE relnamespace=$1::oid "
                "AND relname=ANY($2::text[]))",
                source_binding.catalog_schema_oid,
                list(names_by_root.values()),
            )
            is True,
            "parent_exists",
        )
        for root, name in names_by_root.items():
            for statement in cms_snapshot_parent_statements(source_binding.catalog_schema, root, name):
                await connection.execute(statement)
            await connection.execute(
                f"ALTER TABLE {storage_relation(source_binding.catalog_schema, name)} OWNER TO {owner_sql}"
            )
        parents = await read_cms_snapshot_parents(connection, source_binding, preparation_id)
        _require(
            await NativeReceiptSession(connection).scalar(_ADMIN_SQL) is True
            and dict(await connection.fetchrow(custody._ACTOR_SQL)) == actor_by_field,
            "original_admin_changed",
        )
    return parents
