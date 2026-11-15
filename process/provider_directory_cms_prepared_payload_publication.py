# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Copy one prepared CMS payload family without publishing a header or pointer.

The trusted finisher must separately authenticate its protected claim/profile,
canonical writer custody and mutation fence through commit. This component
uses existing rights and guards; its data report grants no authority.
"""

import json

from sqlalchemy import JSON

from db import models
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_allocation as allocations
from process import provider_directory_cms_prepared_context as context
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_identity_candidate import copy_identity_rows_native
from process.provider_directory_cms_prepared_workspace import _GRAPH_COLUMNS, CMSPreparedWorkspace, _policy
from process.provider_directory_generic_candidate import COPY_FRAME_BYTES, COPY_MAX_BYTES, COPY_MAX_ROWS
from process.provider_directory_generic_source_policy import _savepoint
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff

ROOTS = storage.RELATIONS
_MODELS = (
    models.ProviderDirectoryDatasetResource,
    models.ProviderDirectoryDatasetInsurancePlan,
    models.ProviderDirectoryDatasetNetworkPlan,
    models.ProviderDirectoryDatasetAffiliationOrganization,
    models.ProviderDirectoryCMSNPDResourceWitness,
)
_INTEGER_COLUMNS = {"parent_ordinal", "reference_ordinal"}
_TARGET_SQL = """SELECT wanted.root,n.nspname AS schema,
 c.relkind::text AS relkind,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,
 ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a
       WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum) AS columns,
 (SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid::bigint,a.atttypmod,
  a.attnotnull,a.attgenerated,pg_catalog.pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
  FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d
  ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS layout
 FROM unnest($1::text[]) wanted(root)
 LEFT JOIN pg_catalog.pg_class c ON c.oid=pg_catalog.to_regclass(pg_catalog.format('%I.%I',$2::text,wanted.root))
 LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace ORDER BY wanted.root"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_payload_publication_" + code)


def _columns(root, *, generated=False):
    if root == storage.RELATIONSHIP:
        return tuple(name for name, _ in _GRAPH_COLUMNS), frozenset()
    table = _MODELS[ROOTS.index(root)].__table__
    _require(table.name == root, "layout_changed")
    columns = tuple(str(c.name) for c in table.columns if generated or c.computed is None)
    return columns, frozenset(str(c.name) for c in table.columns if isinstance(c.type, JSON))


async def _coordinates(workspace, handoff):
    _require(type(workspace) is CMSPreparedWorkspace and type(handoff) is FHIRSourceCandidateHandoff, "scope_required")
    receipts = await workspace.verify_current()
    document = handoff.document
    _require(
        workspace.binding["indexed"]
        and document["contract"] == storage.CMS_ISOLATED_CONTRACT
        and document["identity"] == workspace.identity
        and handoff.document_sha256 == workspace.binding["handoff_sha256"],
        "prepared_handoff_changed",
    )
    observed = await context._observe(
        workspace.connection,
        workspace.source_binding,
        _policy(workspace._inputs["source_policy"]),
        workspace.policy,
        workspace._inputs["frozen_inventory"],
    )
    _require(observed == workspace._inputs["native_coordinates"], "frozen_coordinates_changed")
    sources_by_root = {
        root: document["tables"][root] if root in {storage.RESOURCE, storage.WITNESS} else receipts[root]
        for root in ROOTS
    }
    _require(len({(item["schema"], item["name"]) for item in sources_by_root.values()}) == len(ROOTS), "source_alias")
    return sources_by_root


async def _targets(workspace):
    rows = await workspace.connection.fetch(_TARGET_SQL, list(ROOTS), workspace.catalog_schema)
    excluded_names = {
        (row["schema_name"], row["relation_name"]) for row in workspace._inputs["frozen_inventory"]["relations"]
    }
    excluded_names.update((receipt["schema"], receipt["name"]) for receipt in workspace.receipts.values())
    _require(len(rows) == len(ROOTS) and {row["root"] for row in rows} == set(ROOTS), "targets_changed")
    targets_by_root = {}
    for row in rows:
        reference_by_field = {"schema": workspace.catalog_schema, "name": row["root"]}
        _require(
            row["schema"] == workspace.catalog_schema
            and (reference_by_field["schema"], reference_by_field["name"]) not in excluded_names
            and row["relkind"] in {"r", "p"}
            and not row["relispartition"]
            and not row["relrowsecurity"]
            and not row["relforcerowsecurity"]
            and tuple(row["columns"]) == _columns(row["root"], generated=True)[0],
            "target_unavailable",
        )
        await workspace.connection.execute(
            "SELECT 1 FROM " + storage_relation(reference_by_field["schema"], reference_by_field["name"]) + " LIMIT 0"
        )
        targets_by_root[row["root"]] = reference_by_field
    return targets_by_root


def _page(root, source_relation, *, first=False):
    columns, json_columns = _columns(root)
    keys = completion._KEYS[root]
    order = ",".join(storage_identifier(key) for key in keys)
    after = ",".join(
        f"${index}::integer" if key in _INTEGER_COLUMNS else f"${index}::text" for index, key in enumerate(keys, 2)
    )
    predicate = "" if first else f" AND ({order})>({after})"
    row_limit = 2 if first else len(keys) + 2
    byte_limit = row_limit + 1
    fields = ",".join(
        storage_identifier(column_name)
        + ("::text AS " + storage_identifier(column_name) if column_name in json_columns else "")
        for column_name in columns
    )
    width = "2+" + "+".join(
        "12"
        if column_name in _INTEGER_COLUMNS
        else "4+coalesce(octet_length(" + storage_identifier(column_name) + "::text)+1,0)"
        for column_name in columns
    )
    selected = ",".join(
        f"CASE WHEN copy_row_bytes<=${byte_limit} THEN {storage_identifier(column_name)} END AS {storage_identifier(column_name)}"
        for column_name in columns
    )
    return f"""WITH page AS MATERIALIZED (
 SELECT {fields},{width} AS copy_row_bytes FROM {source_relation}
 WHERE dataset_id=$1{predicate}
 ORDER BY {order} LIMIT ${row_limit}
), bounded AS (
 SELECT *,row_number() OVER(ORDER BY {order}) AS ordinal,
 sum(copy_row_bytes) OVER(ORDER BY {order}) AS page_bytes FROM page
)
SELECT {selected},copy_row_bytes FROM bounded
WHERE ordinal=1 OR page_bytes<=${byte_limit} ORDER BY {order}"""


async def _source_scope(workspace, sources):
    for receipt in sources.values():
        relation = storage_relation(workspace.policy.schema, receipt["name"])
        changed = await workspace.connection.fetchval(
            f"SELECT EXISTS(SELECT FROM {relation} WHERE dataset_id IS DISTINCT FROM $1)",
            workspace.identity["dataset_id"],
        )
        _require(changed is False, "source_dataset_changed")


async def _copy_table(workspace, root, receipt):
    columns, _ = _columns(root)
    keys = completion._KEYS[root]
    source_relation = storage_relation(workspace.policy.schema, receipt["name"])
    after, count = None, 0
    while True:
        query = _page(root, source_relation, first=after is None)
        page_rows = await workspace.connection.fetch(
            query, workspace.identity["dataset_id"], *(after or ()), COPY_MAX_ROWS, COPY_MAX_BYTES - COPY_FRAME_BYTES
        )
        if not page_rows:
            return count
        _require(
            len(page_rows) <= COPY_MAX_ROWS
            and all(0 < row["copy_row_bytes"] <= COPY_MAX_BYTES - COPY_FRAME_BYTES for row in page_rows)
            and sum(row["copy_row_bytes"] for row in page_rows) <= COPY_MAX_BYTES - COPY_FRAME_BYTES,
            "copy_page_unbounded",
        )
        records = [tuple(row[column] for column in columns) for row in page_rows]
        await workspace.verify_current()
        copied = await copy_identity_rows_native(
            workspace.connection, workspace.session, workspace.catalog_schema, root, columns, records
        )
        _require(copied == len(records), "copy_incomplete")
        next_keys = tuple(page_rows[-1][key] for key in keys)
        _require(next_keys != after, "copy_key_not_advanced")
        after, count = next_keys, count + copied


async def _parity(workspace, root, receipt):
    columns, json_columns = _columns(root, generated=True)
    fields = ",".join(storage_identifier(c) + ("::jsonb" if c in json_columns else "") for c in columns)
    source = storage_relation(workspace.policy.schema, receipt["name"])
    target = storage_relation(workspace.catalog_schema, root)
    expected = f"SELECT {fields} FROM {source}"
    actual = f"SELECT {fields} FROM {target} WHERE dataset_id=$1"
    result = await workspace.connection.fetchrow(
        f"SELECT EXISTS(({expected} EXCEPT ALL {actual}) UNION ALL "
        f"({actual} EXCEPT ALL {expected})) AS changed,(SELECT count(*) FROM {target} WHERE dataset_id=$1) AS count",
        workspace.identity["dataset_id"],
    )
    _require(result is not None and result["changed"] is False, "payload_changed")
    return result["count"]


def _report(workspace, handoff, counts):
    return {
        "contract": "cms-prepared-canonical-payload-copy-v1",
        "state": "verified_data",
        "dataset_id": workspace.identity["dataset_id"],
        "preparation_id": workspace.binding["preparation_id"],
        "admission_sha256": workspace.binding["admission_sha256"],
        "handoff_sha256": handoff.document_sha256,
        "row_counts": counts,
    }


async def verify_prepared_cms_payloads(workspace, handoff):
    """Read full six-table parity; this does not attest writer custody or a fence."""
    sources = await _coordinates(workspace, handoff)
    targets = await _targets(workspace)
    await _source_scope(workspace, sources)
    counts_by_root = {root: await _parity(workspace, root, receipt) for root, receipt in sources.items()}
    _require(
        await _coordinates(workspace, handoff) == sources and await _targets(workspace) == targets,
        "coordinates_changed",
    )
    return _report(workspace, handoff, counts_by_root)


async def copy_prepared_cms_payloads(workspace, handoff):
    """Copy six fixed targets atomically using the original prepared Publisher.

    The trusted caller owns the acquiring noncurrent header, protected authority,
    profile/claim and writer fence. Existing guards remain enabled. A rejected
    copy rolls back all six targets; it never resets a partial accepted target.
    Header, identity, coverage, receipt and current pointers are outside this body.
    """
    sources = await _coordinates(workspace, handoff)
    targets = await _targets(workspace)
    async with _savepoint(workspace.connection):
        await _source_scope(workspace, sources)
        for root in ROOTS:
            target = storage_relation(workspace.catalog_schema, root)
            occupied = await workspace.connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {target} WHERE dataset_id=$1)", workspace.identity["dataset_id"]
            )
            _require(occupied is False, "target_not_empty")
        copied_by_root = {}
        for root, receipt in sources.items():
            copied_by_root[root] = await _copy_table(workspace, root, receipt)
            _require(copied_by_root[root] == await _parity(workspace, root, receipt), "copy_count_changed")
        proof = await verify_prepared_cms_payloads(workspace, handoff)
        _require(proof["row_counts"] == copied_by_root and await _targets(workspace) == targets, "coordinates_changed")
    return proof


def _generic_leaves(workspace, inventory):
    """Select the complete four named leaves of this logical dataset allocation."""
    _require(type(inventory) is list and len(inventory) == 8, "generic_inventory_required")
    coordinates = {(entry["schema"], entry["name"]) for entry in inventory}
    excluded_names = {
        (entry["schema_name"], entry["relation_name"]) for entry in workspace._inputs["frozen_inventory"]["relations"]
    }
    excluded_names.update((receipt["schema"], receipt["name"]) for receipt in workspace.receipts.values())
    parents_by_root = {entry["name"]: entry for entry in inventory if entry["name"] in ROOTS[:4]}
    _require(
        len(coordinates) == 8
        and set(parents_by_root) == set(ROOTS[:4])
        and all(entry["schema"] == workspace.catalog_schema for entry in inventory)
        and not coordinates & excluded_names,
        "generic_inventory_required",
    )
    leaves_by_root = {}
    for root, parent in parents_by_root.items():
        child_entries = [
            entry for entry in inventory if entry["parent"] == {"schema": workspace.catalog_schema, "name": root}
        ]
        _require(parent["kind"] == "p" and parent["parent"] is None, "generic_partition_required")
        _require(
            len(child_entries) == 1
            and child_entries[0]["kind"] == "r"
            and child_entries[0]["name"] == allocations.generic_leaf_name(root, workspace.identity["dataset_id"])
            and not any(
                entry["parent"] == {"schema": workspace.catalog_schema, "name": child_entries[0]["name"]}
                for entry in inventory
            ),
            "generic_partition_required",
        )
        leaves_by_root[root] = {"schema": child_entries[0]["schema"], "name": child_entries[0]["name"]}
    return leaves_by_root


async def verify_allocated_cms_generic_payloads(workspace, handoff, *, allocation):
    """Read exact four-target parity before witness attachment; issue no six-target receipt."""
    _require(type(allocation) is dict, "allocation_required")
    allocation = json.loads(storage._encoded(allocation))
    sources_by_root = await _coordinates(workspace, handoff)
    leaves_by_root = _generic_leaves(workspace, allocation["inventory"])
    _require(await allocations._inventory(workspace) == allocation["inventory"], "inventory_changed")
    selected_by_root = {root: sources_by_root[root] for root in ROOTS[:4]}
    await _source_scope(workspace, selected_by_root)
    counts_by_root = {root: await _parity(workspace, root, receipt) for root, receipt in selected_by_root.items()}
    _require(
        await _coordinates(workspace, handoff) == sources_by_root
        and await allocations._inventory(workspace) == allocation["inventory"],
        "coordinates_changed",
    )
    return {
        **_report(workspace, handoff, counts_by_root),
        "contract": "cms-prepared-generic-payload-copy-v1",
        "tables": leaves_by_root,
    }


async def copy_allocated_cms_generic_payloads(workspace, handoff, *, allocation):
    """Copy four committed generic leaves; return data evidence without publishing.

    The caller authenticates the protected allocation record, profile/claim and
    guard/owner/all-writer custody through commit. A readonly private workspace
    stays readonly: only its four allocated canonical targets receive COPY.
    Header/object pins and ONLY-leaf SUE locks precede full content checks; the
    existing savepoint and interrupted-COPY cleanup preserve failure rollback.
    """
    _require(type(allocation) is dict, "allocation_required")
    allocation = json.loads(storage._encoded(allocation))
    sources_by_root = await _coordinates(workspace, handoff)
    async with _savepoint(workspace.connection):
        await allocations.adopt_prepared_cms_allocation(workspace, handoff, allocation)
        leaves_by_root = _generic_leaves(workspace, allocation["inventory"])
        names = sorted(storage_relation(leaf["schema"], leaf["name"]) for leaf in leaves_by_root.values())
        await workspace.connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
        )
        _require(await allocations._inventory(workspace) == allocation["inventory"], "inventory_changed")
        generic_sources_by_root = {root: sources_by_root[root] for root in ROOTS[:4]}
        await _source_scope(workspace, generic_sources_by_root)
        for root in generic_sources_by_root:
            occupied = await workspace.connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {storage_relation(workspace.catalog_schema, root)} WHERE dataset_id=$1)",
                workspace.identity["dataset_id"],
            )
            _require(occupied is False, "target_not_empty")
        copied_by_root = {
            root: await _copy_table(workspace, root, receipt) for root, receipt in generic_sources_by_root.items()
        }
        counts_by_root = {
            root: await _parity(workspace, root, receipt) for root, receipt in generic_sources_by_root.items()
        }
        _require(copied_by_root == counts_by_root, "copy_count_changed")
        _require(
            await _coordinates(workspace, handoff) == sources_by_root
            and await allocations._inventory(workspace) == allocation["inventory"],
            "coordinates_changed",
        )
    return {
        **_report(workspace, handoff, counts_by_root),
        "contract": "cms-prepared-generic-payload-copy-v1",
        "tables": leaves_by_root,
    }
