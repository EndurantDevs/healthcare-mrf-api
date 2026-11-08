# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scan fixed history data after the original-Admin producer authenticates its common fence.

This module supplies mechanics, never an authority handle. Its only issuer caller
must retain authenticated native inventories and continuous common custody through
the eventual outer commit. No DDL, record write or commit occurs here.
"""

from copy import deepcopy

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_history_bounds as bounds
from process import provider_directory_cms_history_conversion as history
from process import provider_directory_cms_history_record as records
from process import provider_directory_cms_legacy_history as scans


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_capture_" + code)


def _oid(value):
    if type(value) is str and value.isascii() and value.isdecimal() and not value.startswith("0"):
        value = int(value)
    _require(type(value) is int and 0 < value < 2**32, "native_oid_required")
    return value


def _old_roots(family):
    roots_by_name = {entry["relname"]: entry for entry in family if entry["relname"] in scans.ROOTS}
    _require(set(roots_by_name) == set(scans.ROOTS), "roots_required")
    _require(all(not entry["native"]["parents"] for entry in roots_by_name.values()), "roots_changed")
    return roots_by_name


def _identity(entry, index_names, schema, root):
    native = entry["native"]
    dependencies = history.cms_history_dependencies(schema, root)
    hooks_by_name = {}
    for hook in native["triggers"]:
        if not hook["tgisinternal"]:
            name = hook["tgname"]
            _require(name in dependencies["hooks"], "hook_changed")
            hooks_by_name[name] = dict(
                oid=_oid(hook["oid"]),
                function_oid=_oid(hook["tgfoid"]),
                function=dependencies["hooks"][name]["function"],
                tgtype=hook["tgtype"],
                enabled=hook["tgenabled"],
                old_table=hook["tgoldtable"],
                new_table=hook["tgnewtable"],
            )
    return dict(
        oid=entry["oid"],
        name=entry["relname"],
        schema_oid=native["namespace"][0],
        owner_oid=entry["relowner"],
        kind=entry["relkind"],
        indexes={index_names[index["class"][0]]: index["class"][0] for index in native["indexes"]},
        foreign_keys={
            constraint["conname"]: {"oid": _oid(constraint["oid"]), "definition": definition}
            for constraint, definition in native["constraints"]
            if constraint["contype"] == "f" and str(constraint["conparentid"]) == "0"
        },
        hooks=hooks_by_name,
    )


async def _index_names(connection, entries):
    oids = sorted({index["class"][0] for entry in entries for index in entry["native"]["indexes"]})
    rows = await connection.fetch(
        "SELECT oid::bigint,relname::text AS name FROM pg_catalog.pg_class WHERE oid=ANY($1::oid[])", oids
    )
    names_by_oid = {row["oid"]: row["name"] for row in rows}
    _require(set(names_by_oid) == set(oids), "indexes_changed")
    return names_by_oid


async def scan_cms_history_prework_data(connection, session, *, storage, family, expected_history_bounds=None):
    """Read complete fixed history without inventing an allocation or conversion authority.

    The caller authenticates the family and holds the original-Admin history
    fence throughout the scan. Finalization must capture its own fresh history.
    """
    roots_by_name = _old_roots(family)
    catalog_by_field = dict(
        database_oid=storage["database_oid"],
        schema_oid=storage["catalog_schema_oid"],
        roots={root: {"oid": entry["oid"], "owner": entry["relowner"]} for root, entry in roots_by_name.items()},
    )
    schema = storage["catalog_schema"]
    checked = (
        None if expected_history_bounds is None else bounds.require_cms_history_expectations(expected_history_bounds)
    )
    if checked is None:
        bounds_sql = " UNION ".join(
            "SELECT DISTINCT dataset_id FROM ONLY " + storage_relation(schema, root) for root in history.ROOTS
        )
        datasets = sorted({bound_row["dataset_id"] for bound_row in await connection.fetch(bounds_sql)})
    else:
        datasets = sorted({key for entry in checked.values() for key in entry["dataset_ids"]})
    expected, datasets = scans._request(schema, catalog_by_field, datasets)
    checked = scans._history_bounds(checked, expected, datasets)
    options = {} if checked is None else {"expected_history_bounds": checked}
    scanned = await scans._scan_history(
        connection, session, schema=schema, expected=expected, datasets=datasets, **options
    )
    _require(scans._history_bounds(expected_history_bounds, expected, datasets) == checked, "inputs_changed")
    return {
        **scanned,
        "history_dataset_ids": {
            root: list(datasets if checked is None else checked[root]["dataset_ids"]) for root in history.ROOTS
        },
    }


async def capture_cms_history_data(connection, session, *, storage, family, parents, expected_history_bounds=None):
    """Capture fresh complete history and its genuine allocated conversion plan."""
    roots_by_name = _old_roots(family)
    scanned = await scan_cms_history_prework_data(
        connection,
        session,
        storage=storage,
        family=family,
        expected_history_bounds=expected_history_bounds,
    )
    schema = storage["catalog_schema"]
    index_names = await _index_names(connection, [*(roots_by_name[root] for root in history.ROOTS), *parents.values()])
    request_by_field = dict(
        database_oid=storage["database_oid"],
        catalog_schema=schema,
        catalog_schema_oid=storage["catalog_schema_oid"],
        future_schema=schema,
        future_schema_oid=storage["catalog_schema_oid"],
        owner_oid=storage["namespace_owner_oid"],
        heaps={
            root: dict(
                old=_identity(roots_by_name[root], index_names, schema, root),
                parent=_identity(parents[root], index_names, schema, root),
                dataset_ids=scanned["history_dataset_ids"][root],
            )
            for root in history.ROOTS
        },
    )
    history_by_field = dict(
        transaction=await connection.fetchval("SELECT pg_catalog.pg_current_xact_id()::text"),
        request=request_by_field,
        dataset_rows=scanned["dataset_rows"],
        content=scanned["content"],
    )
    records.require_cms_history_record(history_by_field, storage=storage, parents=parents)
    return deepcopy(history_by_field)


async def read_converted_cms_history_data(connection, session, *, record, plan):
    """Compare both complete retained heaps to the already authenticated history commitment.

    The caller authenticates protected record provenance, exact converted storage
    and bounds, and the original-Admin snapshot before and after these scans.
    Current rows never supply expected bounds, identities or content hashes.
    """
    _require(type(record) is dict and plan == history.plan_cms_history_conversion(record["request"]), "plan_changed")
    schema = plan["request"]["future_schema"]
    receipts_by_root = {
        root: {key: plan["expected"][root]["old"][key] for key in ("name", "oid")} for root in history.ROOTS
    }
    relations_by_root = {
        scans.RESOURCE: storage_relation(schema, scans.RESOURCE),
        **{root: storage_relation(schema, proof["name"]) for root, proof in receipts_by_root.items()},
    }
    scanned = await scans._scan_relations(
        connection,
        session,
        schema=schema,
        receipts=receipts_by_root,
        datasets_by_root={root: plan["request"]["heaps"][root]["dataset_ids"] for root in history.ROOTS},
        relations_by_root=relations_by_root,
    )
    _require(scanned == {key: record[key] for key in ("dataset_rows", "content")}, "retained_content_changed")
    return deepcopy(scanned)
