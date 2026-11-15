# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allocate and adopt CMS candidate data by qualified name under caller transactions."""

import hashlib
import json

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import _savepoint

CONTRACT = "cms-prepared-canonical-allocation-data-v1"
PRIVATE_ALLOCATION_CONTRACT = "cms-prepared-private-allocation-data-v2"
_FIELDS = {"contract", "workspace", "inventory"}
_TARGET_SQL = """SELECT wanted.name,c.relkind::text AS kind,n.nspname AS schema,
 c.relispartition,c.relpersistence::text AS relpersistence,c.relrowsecurity,c.relforcerowsecurity,
 ARRAY(SELECT pn.nspname||'.'||p.relname FROM pg_inherits h
 JOIN pg_class p ON p.oid=h.inhparent JOIN pg_namespace pn ON pn.oid=p.relnamespace
 WHERE h.inhrelid=c.oid ORDER BY pn.nspname,p.relname) AS parents,
 NOT EXISTS(SELECT FROM pg_inherits WHERE inhparent=c.oid) AS leaf,
 pg_get_expr(c.relpartbound,c.oid)=format('FOR VALUES IN (%L)',$3::text) AS bound_matches
 FROM unnest($1::text[]) wanted(name)
 LEFT JOIN pg_class c ON c.oid=to_regclass(format('%I.%I',$2::text,wanted.name))
 LEFT JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY wanted.name"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_allocation_" + code)


def generic_leaf_name(root, dataset_id):
    """Use the existing allocator's deterministic logical dataset name."""
    return "pd_generic_" + hashlib.md5((root + ":" + dataset_id).encode()).hexdigest()


async def _inventory(workspace):
    """Validate only this dataset's named parents/leaves, leaving other generations alone."""
    roots, schema = TABLES[:4], workspace.catalog_schema
    children_by_name = {generic_leaf_name(root, workspace.identity["dataset_id"]): root for root in roots}
    target_rows = await workspace.connection.fetch(
        _TARGET_SQL, [*roots, *children_by_name], schema, workspace.identity["dataset_id"]
    )
    by_name = {entry["name"]: entry for entry in target_rows}
    _require(len(target_rows) == len(by_name) == 8 and set(by_name) == {*roots, *children_by_name}, "inventory_changed")
    selected_by_name = {}
    for root in roots:
        parent = by_name[root]
        _require(
            parent["schema"] == schema
            and parent["kind"] in {"r", "p"}
            and parent["relpersistence"] == "p"
            and not parent["parents"]
            and not parent["relispartition"]
            and not parent["relrowsecurity"]
            and not parent["relforcerowsecurity"],
            "root_changed",
        )
        selected_by_name[root] = {"schema": schema, "name": root, "kind": parent["kind"], "parent": None}
        if parent["kind"] == "r":
            _require(parent["leaf"], "root_changed")
            continue
        name = generic_leaf_name(root, workspace.identity["dataset_id"])
        child = by_name[name]
        _require(
            child["schema"] == schema
            and child["kind"] == "r"
            and child["relpersistence"] == "p"
            and child["relispartition"]
            and child["parents"] == [schema + "." + root]
            and child["bound_matches"] is True
            and child["leaf"]
            and not child["relrowsecurity"]
            and not child["relforcerowsecurity"],
            "partition_changed",
        )
        selected_by_name[name] = {
            "schema": schema,
            "name": name,
            "kind": "r",
            "parent": {"schema": schema, "name": root},
        }
    return [selected_by_name[name] for name in sorted(selected_by_name)]


async def allocate_prepared_cms_candidate(workspace, handoff):
    """Insert one fresh acquiring header; rollback includes all allocated candidate leaves."""
    from process.provider_directory_cms_prepared_candidate import _allocate_header, _inputs

    frozen, header = await _inputs(workspace, handoff)
    async with _savepoint(workspace.connection):
        await _allocate_header(workspace.connection, workspace.catalog_schema, frozen, header)
        allocation_by_field = {
            "contract": CONTRACT,
            "workspace": workspace.binding,
            "inventory": await _inventory(workspace),
        }
        _require(await _inputs(workspace, handoff) == (frozen, header), "frozen_header_changed")
    return json.loads(json.dumps(allocation_by_field, allow_nan=False))


async def retain_prepared_cms_workspace(workspace, handoff):
    """Describe indexed private outputs without allocating or publishing canonical data."""
    from process.provider_directory_cms_prepared_candidate import _inputs

    _require(workspace.connection.is_in_transaction() and not workspace.connection.is_closed(), "transaction_required")
    binding = json.loads(json.dumps(workspace.binding, allow_nan=False))
    original = await _inputs(workspace, handoff)
    _require(await _inputs(workspace, handoff) == original, "frozen_header_changed")
    _require(workspace.binding == binding, "workspace_changed")
    return {"contract": PRIVATE_ALLOCATION_CONTRACT, "workspace": binding}


async def adopt_prepared_cms_allocation(workspace, handoff, allocation):
    """Lock the visible logical candidate and its exact named targets before writing.

    PostgreSQL visibility excludes another transaction's uncommitted allocation.
    The row lock serializes writers; failed work rolls back without consuming or
    replacing an existing candidate. No database-local tuple identity is retained.
    """
    from process.provider_directory_cms_prepared_candidate import _header, _inputs
    from process.provider_directory_generic_source_storage import HEADER

    _require(type(allocation) is dict and set(allocation) == _FIELDS, "record_required")
    allocation = json.loads(json.dumps(allocation, allow_nan=False))
    _require(
        allocation["contract"] == CONTRACT
        and allocation["workspace"] == workspace.binding
        and type(allocation["inventory"]) is list,
        "record_changed",
    )
    _, expected = await _inputs(workspace, handoff)
    canonical = storage_relation(workspace.catalog_schema, HEADER)
    observed = await workspace.connection.fetchrow(
        f"SELECT * FROM {canonical} WHERE dataset_id=$1 FOR UPDATE NOWAIT", expected["dataset_id"]
    )
    _require(
        observed is not None
        and _header(observed)
        == {**expected, "status": "acquiring", "validated_at": None, "candidate_storage_json": None},
        "header_changed",
    )
    inventory = await _inventory(workspace)
    _require(inventory == allocation["inventory"], "inventory_changed")
    from process.provider_directory_cms_allocation_pin import pin_cms_allocation_targets

    await pin_cms_allocation_targets(workspace, inventory)
    _require(await _inventory(workspace) == inventory, "inventory_changed")
    await workspace.verify_current()
    return canonical


async def read_cms_generic_copy_tables(workspace):
    """Return the four validated qualified target names, without physical receipts."""
    inventory = await _inventory(workspace)
    leaves_by_parent = {
        entry["parent"]["name"]: {"schema": entry["schema"], "name": entry["name"]}
        for entry in inventory
        if entry["parent"] is not None
    }
    _require(set(leaves_by_parent) == set(TABLES[:4]), "native_table_changed")
    return leaves_by_parent
