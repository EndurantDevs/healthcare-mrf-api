# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scan complete fixed history by qualified name; execute no DDL or commit.

The caller owns authorization and the atomic publication transaction.
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


def _schema(storage):
    return storage["catalog_schema"] if type(storage) is dict else storage.catalog_schema


def _old_roots(family):
    """Accept declared names, including stored relname mappings; ignore physical metadata."""
    names = [entry.get("name", entry.get("relname")) for entry in family]
    _require(all(names.count(root) == 1 for root in scans.ROOTS), "roots_required")
    return {root: root for root in scans.ROOTS}


_IDENTITY_SQL = """SELECT n.nspname AS schema,c.relname AS name,c.relkind::text AS kind,
 ARRAY(SELECT i.relname FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid
  WHERE x.indrelid=c.oid ORDER BY i.relname) AS indexes,
 COALESCE((SELECT jsonb_object_agg(k.conname,pg_get_constraintdef(k.oid,true)) FROM pg_constraint k
  WHERE k.conrelid=c.oid AND k.contype='f' AND k.conparentid=0),'{}'::jsonb) AS foreign_keys,
 COALESCE((SELECT jsonb_object_agg(t.tgname,jsonb_build_object('function',p.proname,'tgtype',t.tgtype,
  'enabled',t.tgenabled::text,'old_table',t.tgoldtable,'new_table',t.tgnewtable))
  FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid WHERE t.tgrelid=c.oid AND NOT t.tgisinternal),'{}'::jsonb) AS hooks
 FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 AND c.relname=$2
 AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid)"""


async def _identity(connection, relation):
    row = await connection.fetchrow(_IDENTITY_SQL, relation["schema"], relation["name"])
    _require(row is not None, "relation_changed")
    import json

    identity_by_field = dict(row)
    for key in ("foreign_keys", "hooks"):
        if type(identity_by_field[key]) is str:
            identity_by_field[key] = json.loads(identity_by_field[key])
    return identity_by_field


async def scan_cms_history_prework_data(connection, session, *, storage, family, expected_history_bounds=None):
    """Read complete fixed history without inventing an allocation or conversion authority.

    The caller declares the family and owns its repeatable-read transaction.
    Finalization must capture fresh history within its publication boundary.
    """
    roots_by_name = _old_roots(family)
    schema = _schema(storage)
    catalog_by_field = {"schema": schema, "roots": {root: {"schema": schema, "name": root} for root in roots_by_name}}
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


async def capture_cms_history_data(
    connection, session, *, storage, family, parents, history_names, expected_history_bounds=None
):
    """Capture complete history and a plan using explicit retained relation names.

    No ID-derived history name is invented. The caller declares collision-free
    names and applies the returned plan within its atomic publication boundary.
    """
    _old_roots(family)
    scanned = await scan_cms_history_prework_data(
        connection, session, storage=storage, family=family, expected_history_bounds=expected_history_bounds
    )
    schema = _schema(storage)
    _require(type(history_names) is dict and set(history_names) == set(history.ROOTS), "history_names_required")
    heaps_by_root = {}
    for root in history.ROOTS:
        heaps_by_root[root] = {
            "old": await _identity(connection, {"schema": schema, "name": root}),
            "parent": await _identity(connection, parents[root]),
            "history_name": history_names[root],
            "dataset_ids": scanned["history_dataset_ids"][root],
        }
    record_by_field = {
        "request": {"catalog_schema": schema, "future_schema": schema, "heaps": heaps_by_root},
        "dataset_rows": scanned["dataset_rows"],
        "content": scanned["content"],
    }
    records.require_cms_history_record(record_by_field, storage=storage, parents=parents)
    return deepcopy(record_by_field)


async def read_converted_cms_history_data(connection, session, *, record, plan):
    """Compare complete retained heaps to the accepted content and dataset bounds.

    The plan supplies declared names and expected bounds. Native COPY scans all
    stored values in primary-key order within the caller's read snapshot.
    """
    _require(type(record) is dict and plan == history.plan_cms_history_conversion(record["request"]), "plan_changed")
    schema = plan["request"]["catalog_schema"]
    receipts_by_root = {
        root: {key: plan["expected"][root]["old"][key] for key in ("schema", "name")} for root in history.ROOTS
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
