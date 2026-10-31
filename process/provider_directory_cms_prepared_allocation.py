# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Record and adopt exact committed CMS allocation data; issue no authority.

The protected caller must authenticate the allocation record and its original
claim, reviewed allocator and target semantics and complete writer custody. Captured
native metadata is comparison evidence, never a substitute for those checks.
"""

import hashlib
import json

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as custody
from process.provider_directory_generic_source_policy import _savepoint

CONTRACT = "cms-prepared-canonical-allocation-data-v1"
PRIVATE_ALLOCATION_CONTRACT = "cms-prepared-private-allocation-data-v2"
_FIELDS = {"contract", "workspace", "transaction", "header_xmin", "inventory"}
_NATIVE_COPY_TABLE_SQL = (
    "SELECT c.oid::bigint,c.relowner::bigint,pg_get_expr(c.relpartbound,c.oid) AS bound, "
    "(SELECT jsonb_agg(jsonb_build_array(a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attgenerated, "
    "pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a "
    "LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
    "WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns, "
    "(SELECT jsonb_agg(jsonb_build_array(i.indisunique,i.indisprimary,i.indisvalid,i.indisready, "
    "i.indkey::text,pg_get_indexdef(i.indexrelid)) ORDER BY i.indexrelid) "
    "FROM pg_index i WHERE i.indrelid=c.oid) AS indexes, "
    "(SELECT jsonb_agg(jsonb_build_array(k.contype,k.convalidated,pg_get_constraintdef(k.oid)) "
    "ORDER BY k.oid) FROM pg_constraint k WHERE k.conrelid=c.oid) AS constraints "
    "FROM pg_class c JOIN pg_inherits h ON h.inhrelid=c.oid JOIN pg_class p ON p.oid=h.inhparent "
    "WHERE c.oid=to_regclass($1) AND p.oid=to_regclass($2) AND p.relkind='p' "
    "AND c.relkind='r' AND c.relispartition AND c.relowner=p.relowner "
    "AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity "
    "AND pg_get_expr(c.relpartbound,c.oid)=format('FOR VALUES IN (%L)',$3::text) "
    "AND NOT EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND (NOT i.indisvalid OR NOT i.indisready))"
)


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_allocation_" + code)


async def _inventory(workspace):
    """Capture this dataset's targets, retaining root metadata without other data partitions.

    This snapshot does not authenticate storage semantics. The protected finisher
    must independently require its reviewed production storage shape.
    """
    roots = TABLES[:4]
    family = await custody._inventory(workspace.connection, workspace.catalog_schema, roots)
    by_name = {relation_by_field["relname"]: relation_by_field for relation_by_field in family}
    selected_by_name = {root: by_name[root] for root in roots}
    for root in roots:
        parent = by_name[root]
        _require(not parent["native"]["parents"], "root_changed")
        if parent["relkind"] == "r":
            _require(
                not any(parent["oid"] in relation_by_field["native"]["parents"] for relation_by_field in family),
                "root_changed",
            )
            continue
        name = "pd_generic_" + hashlib.md5((root + ":" + workspace.identity["dataset_id"]).encode()).hexdigest()
        child = by_name.get(name)
        _require(
            child is not None
            and child["relkind"] == "r"
            and child["relowner"] == parent["relowner"]
            and child["native"]["parents"] == [parent["oid"]]
            and not any(child["oid"] in relation_by_field["native"]["parents"] for relation_by_field in family),
            "partition_changed",
        )
        bound_matches = await workspace.connection.fetchval(
            "SELECT pg_catalog.pg_get_expr(c.relpartbound,c.oid)=pg_catalog.format('FOR VALUES IN (%L)',$2::text) "
            "FROM pg_catalog.pg_class c WHERE c.oid=$1 AND c.relispartition",
            child["oid"],
            workspace.identity["dataset_id"],
        )
        _require(bound_matches is True, "partition_changed")
        selected_by_name[name] = child
    return sorted(selected_by_name.values(), key=lambda relation_by_field: relation_by_field["oid"])


async def allocate_prepared_cms_candidate(workspace, handoff):
    """Insert a fresh acquiring header and return detached native allocation data.

    Call only in the protected caller's short allocation transaction, then commit
    before composition. Refuse any existing header; authenticated recovery reuses
    its retained allocation record instead of reallocating or repairing targets.
    """
    from process.provider_directory_cms_prepared_candidate import _allocate_header, _inputs

    frozen, header = await _inputs(workspace, handoff)
    async with _savepoint(workspace.connection):
        canonical = await _allocate_header(workspace.connection, workspace.catalog_schema, frozen, header)
        allocation_by_field = dict(
            await workspace.connection.fetchrow(
                f"SELECT pg_catalog.pg_current_xact_id()::text AS transaction,xmin::text AS header_xmin "
                f"FROM {canonical} WHERE dataset_id=$1",
                header["dataset_id"],
            )
        )
        allocation_by_field.update(
            contract=CONTRACT, workspace=workspace.binding, inventory=await _inventory(workspace)
        )
        _require(await _inputs(workspace, handoff) == (frozen, header), "frozen_header_changed")
    return json.loads(json.dumps(allocation_by_field, allow_nan=False))


async def retain_prepared_cms_workspace(workspace, handoff):
    """Describe indexed private outputs without allocating canonical storage.

    The caller authenticates the claim/content and persists this data with its
    protected record in the same outer transaction. This neither commits nor
    supplies CMS authority, sealing, attachment or a canonical header.
    """
    from process.provider_directory_cms_prepared_candidate import _inputs

    connection = workspace.connection
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    binding = json.loads(json.dumps(workspace.binding, allow_nan=False))
    original = await _inputs(workspace, handoff)
    transaction = await connection.fetchval("SELECT pg_catalog.pg_current_xact_id()::text")
    _require(type(transaction) is str and transaction.isdigit(), "transaction_changed")
    _require(await _inputs(workspace, handoff) == original, "frozen_header_changed")
    _require(
        await connection.fetchval("SELECT pg_catalog.pg_current_xact_id()::text") == transaction
        and connection.is_in_transaction() is True
        and not connection.is_closed(),
        "transaction_changed",
    )
    _require(workspace.connection is connection and workspace.binding == binding, "workspace_changed")
    return {"contract": PRIVATE_ALLOCATION_CONTRACT, "workspace": binding, "transaction": transaction}


async def adopt_prepared_cms_allocation(workspace, handoff, allocation):
    """Lock and compare authenticated allocation evidence before any canonical write.

    The caller loads this evidence from its protected claim record. This helper
    establishes only exact data/transaction agreement, not record provenance,
    complete child semantics, a role/DDL fence or all-writer immutability.
    For an authenticated record, an earlier top transaction and its exact visible
    tuple establish commit without
    depending on recent-only native commit-status history.
    """
    from process.provider_directory_cms_prepared_candidate import _header, _inputs
    from process.provider_directory_generic_source_storage import HEADER

    _require(type(allocation) is dict and set(allocation) == _FIELDS, "record_required")
    allocation = json.loads(json.dumps(allocation, allow_nan=False))
    _require(
        allocation["contract"] == CONTRACT
        and allocation["workspace"] == workspace.binding
        and all(type(allocation[key]) is str and allocation[key].isdigit() for key in ("transaction", "header_xmin"))
        and type(allocation["inventory"]) is list,
        "record_changed",
    )
    _, expected = await _inputs(workspace, handoff)
    _require(
        await workspace.connection.fetchval(
            "SELECT $1::text::pg_catalog.xid8 < pg_catalog.pg_current_xact_id()", allocation["transaction"]
        )
        is True,
        "allocation_not_committed",
    )
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
    _require(
        await workspace.connection.fetchval(
            f"SELECT xmin::text FROM {canonical} WHERE dataset_id=$1", expected["dataset_id"]
        )
        == allocation["header_xmin"],
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
    """Read the existing native-four table proof shape; this grants no authority."""
    proofs_by_root = {}
    for root in TABLES[:4]:
        name = "pd_generic_" + hashlib.md5((root + ":" + workspace.identity["dataset_id"]).encode()).hexdigest()
        row = await workspace.connection.fetchrow(
            _NATIVE_COPY_TABLE_SQL,
            storage_relation(workspace.catalog_schema, name),
            storage_relation(workspace.catalog_schema, root),
            workspace.identity["dataset_id"],
        )
        _require(row is not None, "native_table_changed")
        layout_by_field = {
            key: custody._decoded(value) if key in {"columns", "indexes", "constraints"} else value
            for key, value in row.items()
        }
        proofs_by_root[root] = {
            "name": name,
            "oid": row["oid"],
            "owner": row["relowner"],
            "layout_sha256": custody._digest(layout_by_field),
        }
    return proofs_by_root
