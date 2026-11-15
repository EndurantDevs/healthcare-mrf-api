# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attach the two prevalidated closed CMS witness/relationship payloads on its CMS admin transaction.

The operator supplies protected profile/claim provenance, canonical parent and
callable/writer custody, deadlines and atomic terminal accounting. This data
primitive does not issue that authority, commit or move a serving pointer.
"""

from collections import Counter

from db.migration_expression_adoption import _normalized_expression
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_canonical_layout as layout
from process import provider_directory_cms_preparation_custody as inventory
from process import provider_directory_cms_prepared_workspace as workspace
from process import provider_directory_cms_snapshot_custody as custody
from process import provider_directory_cms_snapshot_seal as seal
from process.provider_directory_cms_allocation_semantics import _clone_columns, _index_signature

CONTRACT = "cms-attached-witness-data-v1"
ROOTS = (workspace.storage.WITNESS, workspace.storage.RELATIONSHIP)
_RECURSION = (
    " UNION SELECT child.inhrelid FROM pg_catalog.pg_inherits child JOIN family parent ON parent.oid=child.inhparent\n"
)


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_attach_" + code)


def _parent_references(recorded):
    """Project current or retained catalog names; numeric identities never resolve a parent."""
    _require(type(recorded) is dict and set(recorded) == set(ROOTS), "parents_required")
    parent_by_root = {}
    for root, value in recorded.items():
        _require(type(value) is dict, "parents_required")
        pairs = [
            (value[schema], value[name])
            for schema, name in (("schema", "name"), ("nspname", "relname"), ("schema_name", "relation_name"))
            if schema in value and name in value
        ]
        _require(bool(pairs) and all(pair == pairs[0] for pair in pairs), "parents_required")
        schema, name = pairs[0]
        storage_relation(schema, name)
        parent_by_root[root] = {"schema": schema, "name": name}
    return parent_by_root


def _root_query():
    """Scope the existing native catalog query to roots without walking siblings."""
    _require(inventory._CATALOG_SQL.count(_RECURSION) == 1, "catalog_template_changed")
    return inventory._CATALOG_SQL.replace(_RECURSION, "", 1)


async def _tables(connection, schema, names):
    relations = [dict(item) for item in await connection.fetch(_root_query(), schema, names)]
    _require(
        len(relations) == len(names) and {item["relname"] for item in relations} == set(names), "inventory_changed"
    )
    return {item["relname"]: {**item, "native": inventory._decoded(item["native"])} for item in relations}


def _checks(parent, child):
    actual_by_name = {native["conname"]: (native, definition) for native, definition in child["native"]["constraints"]}
    for native, definition in parent["native"]["constraints"]:
        _require(
            native["contype"] in {"c", "n", "p", "u"}
            and native["convalidated"]
            and native.get("conenforced", True)
            and not native["condeferrable"]
            and not native["condeferred"],
            "parent_constraints_changed",
        )
        if native["contype"] == "c":
            selected = actual_by_name.get(native["conname"])
            _require(
                selected is not None
                and selected[0]["convalidated"]
                and selected[0].get("conenforced", True)
                and not selected[0]["connoinherit"]
                and _normalized_expression(selected[1]) == _normalized_expression(definition),
                "check_changed",
            )


def _indexes(parent, child):
    import json

    def semantics(index):
        """Compare native index definitions without owners or instance identities."""
        selected = _index_signature(index)
        selected["class"] = selected["class"][1:]
        return json.dumps(selected, sort_keys=True)

    required = Counter(semantics(index) for index in parent["native"]["indexes"])
    available = Counter(semantics(index) for index in child["native"]["indexes"])
    _require(required and not required - available, "index_changed")


async def _compatible(connection, source_binding, retained, parent_oids, owner_role):
    roots = ROOTS
    parents = await _tables(connection, source_binding.catalog_schema, list(roots))
    children = await _tables(
        connection, source_binding.storage_schema, [retained["tables"][root]["name"] for root in roots]
    )
    _require(
        await connection.fetchval(
            "SELECT count(*)=2 AND bool_and(partstrat='l' AND partattrs='1'::pg_catalog.int2vector "
            "AND partexprs IS NULL AND partdefid=0) FROM pg_catalog.pg_partitioned_table "
            "WHERE partrelid=ANY($1::oid[])",
            [entry["oid"] for entry in parents.values()],
        )
        is True,
        "partition_shape_changed",
    )
    for root, parent in parents.items():
        native = parent["native"]
        _require(
            parent_oids[root] == {"schema": source_binding.catalog_schema, "name": root}
            and parent["relkind"] == "p"
            and parent["relpersistence"] == "p"
            and not native["parents"]
            and native["relation"][5:9] == [False, False, False, 0]
            and not any(trigger["tgtype"] & 1 for trigger in native["triggers"]),
            "parents_changed",
        )
        child = children[retained["tables"][root]["name"]]
        columns = await connection.fetch(layout._COLUMNS_SQL, [parent["oid"]], sorted(layout._TYPES))
        layout._require_columns(
            columns, layout._LAYOUTS[root]["columns"], root, workspace.storage_identifier(source_binding.catalog_schema)
        )
        _require(_clone_columns(parent) == _clone_columns(child), "columns_changed")
        _checks(parent, child)
        _indexes(parent, child)
        _require(
            await connection.fetchval(
                "SELECT convalidated AND coalesce((to_jsonb(k)->>'conenforced')::boolean,true) "
                "AND pg_catalog.pg_get_expr(conbin,conrelid,true)=pg_catalog.format('dataset_id::text = %L::text',$2::text) "
                "FROM pg_catalog.pg_constraint k WHERE conrelid=$1 AND conname='pd_dataset_bound' AND contype='c'",
                child["oid"],
                retained["identity"]["dataset_id"],
            )
            is True,
            "dataset_bound_changed",
        )
    return parents


async def _capture_attached(connection, source_binding, retained, parents):
    """Read logical attachment edges by the exact qualified parent/child names."""
    tables_by_root = {root: retained["tables"][root] for root in ROOTS}
    for root, receipt in tables_by_root.items():
        parent = parents[root]
        _require(
            await connection.fetchval(
                "SELECT count(*)=1 AND bool_and(inhparent=to_regclass($2)) FROM pg_catalog.pg_inherits "
                "WHERE inhrelid=to_regclass($1)",
                storage_relation(receipt["schema"], receipt["name"]),
                storage_relation(parent["schema"], parent["name"]),
            )
            is True,
            "attached_inventory_changed",
        )
        await connection.execute("SELECT 1 FROM " + storage_relation(receipt["schema"], receipt["name"]) + " LIMIT 0")
    return workspace._copy(tables_by_root)


def _request(request_by_name):
    fields = {
        "source_binding",
        "prepared_inputs",
        "handoff",
        "expected_workspace",
        "owner_role",
        "migration_role",
    }
    _require(type(request_by_name) is dict and set(request_by_name) == fields, "request_required")
    request_copy_by_name = {
        **request_by_name,
        "prepared_inputs": workspace._copy(request_by_name["prepared_inputs"]),
        "expected_workspace": workspace._copy(request_by_name["expected_workspace"]),
    }
    for field in ("owner_role", "migration_role"):
        workspace.storage_identifier(request_copy_by_name[field])
    retained, _binding, _receipts = workspace._retained_workspace(
        request_copy_by_name["source_binding"],
        request_copy_by_name["prepared_inputs"],
        request_copy_by_name["handoff"],
        request_copy_by_name["expected_workspace"],
    )
    return request_copy_by_name, retained


async def _actor(connection, request_by_name, retained):
    return await seal._admin(
        connection,
        request_by_name["source_binding"],
        request_by_name["prepared_inputs"],
        retained,
        request_by_name["owner_role"],
        request_by_name["migration_role"],
    )


async def _preflight(connection, request_by_name, retained, parent_oids, expected):
    source_binding = request_by_name["source_binding"]
    children = sorted(
        storage_relation(source_binding.storage_schema, retained["tables"][root]["name"])
        for root in workspace.storage.RELATIONS
    )
    roots = sorted(storage_relation(source_binding.catalog_schema, root) for root in ROOTS)
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in children) + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in roots) + " IN ACCESS SHARE MODE NOWAIT"
    )
    observed = await custody._observe(
        connection,
        source_binding,
        retained,
        request_by_name["owner_role"],
        request_by_name["migration_role"],
    )
    _require(observed == expected, "seal_changed")
    _require(
        await connection.fetchval(
            seal._DEPENDENCIES_SQL.replace("$1::oid[]", "ARRAY(SELECT to_regclass(name) FROM unnest($1::text[]) name)"),
            children,
        )
        is True,
        "unsupported_dependency",
    )
    await _compatible(connection, source_binding, retained, parent_oids, request_by_name["owner_role"])


async def _attach_group(connection, source, retained, parent_oids, owner_role):
    literal = await connection.fetchval("SELECT pg_catalog.quote_literal($1::text)", retained["identity"]["dataset_id"])
    _require(type(literal) is str, "dataset_literal_required")
    roots = sorted(storage_relation(source.catalog_schema, root) for root in ROOTS)
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in roots) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    )
    # A peer ATTACH can change the parent after the earlier ACCESS SHARE observation.
    await _compatible(connection, source, retained, parent_oids, owner_role)
    for root in sorted(ROOTS):
        child = storage_relation(source.storage_schema, retained["tables"][root]["name"])
        await connection.execute(
            f"ALTER TABLE {storage_relation(source.catalog_schema, root)} ATTACH PARTITION {child} FOR VALUES IN ({literal})"
        )
    return await _capture_attached(connection, source, retained, parent_oids)


async def attach_sealed_cms_payload_snapshots(connection, request_by_name, *, expected_seal, expected_parent_oids):
    """Attach both CMS witness/relationship snapshots or roll back; the CMS operator owns commit/accounting.

    The request contains the same seven source/preparation/owner arguments as
    private sealing, never a transferable authority. Close parent DML/callable/
    DDL bypasses separately: leaf ACLs do not protect a writable partition parent.
    Default partitions, row hooks/FKs and missing equivalent indexes refuse.
    The historical expected_parent_oids keyword accepts qualified-name records;
    retained catalog name pairs are projected without consulting stored IDs.
    """
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    parent_oids = _parent_references(expected_parent_oids)
    request, retained = _request(request_by_name)
    expected = workspace._copy(expected_seal)
    actor = await _actor(connection, request, retained)
    async with workspace._savepoint(connection):
        await _preflight(connection, request, retained, parent_oids, expected)
        tables = await _attach_group(
            connection,
            request["source_binding"],
            retained,
            parent_oids,
            request["owner_role"],
        )
        _require(await _actor(connection, request, retained) == actor, "transaction_changed")
        current, current_retained = _request(request_by_name)
        _require(
            current == request
            and current_retained == retained
            and _parent_references(expected_parent_oids) == parent_oids
            and expected_seal == expected,
            "inputs_changed",
        )
    return {"contract": CONTRACT, "parents": parent_oids, "tables": tables}
