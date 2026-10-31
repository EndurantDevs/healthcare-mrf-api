# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read populated legacy CMS history; never convert, close writers or issue authority.

Expected native identities and dataset bounds come from the protected caller.
The returned repeatable-read observation cannot attest subsequent immutability,
callable custody or permission to retire existing foreign keys and guards.
"""

import asyncio

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_canonical_layout as layout
from process import provider_directory_cms_prepared_context as context
from process.provider_directory_cms_prepared_outputs import _copy_native_content
from process.provider_directory_generic_source_policy import _savepoint

CONTRACT = "cms-legacy-history-observation-v1"
RESOURCE = "provider_directory_dataset_resource"
WITNESS = "provider_directory_cms_npd_resource_witness"
RELATIONSHIP = "provider_directory_cms_npd_relationship"
ROOTS = (RESOURCE, WITNESS, RELATIONSHIP)
_EDGES = ((WITNESS, RESOURCE), (RELATIONSHIP, RESOURCE), (RELATIONSHIP, WITNESS))
_ROOTS_SQL = """SELECT c.oid::bigint,c.relowner::bigint AS owner,c.relname AS name,
 c.relkind::text,n.nspname,n.oid::bigint AS schema_oid,
 NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=c.oid) AS leaf
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) ORDER BY c.relname"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_legacy_history_" + code)


def _request(schema, expected_catalog, dataset_ids):
    storage_identifier(schema)
    _require(
        type(expected_catalog) is dict
        and set(expected_catalog) == {"database_oid", "schema_oid", "roots"}
        and all(
            type(expected_catalog[key]) is int and 0 < expected_catalog[key] < 2**32
            for key in ("database_oid", "schema_oid")
        ),
        "catalog_required",
    )
    expected_roots = expected_catalog["roots"]
    _require(type(expected_roots) is dict and set(expected_roots) == set(ROOTS), "roots_required")
    for proof in expected_roots.values():
        _require(
            type(proof) is dict
            and set(proof) == {"oid", "owner"}
            and all(type(oid) is int and 0 < oid < 2**32 for oid in proof.values()),
            "roots_required",
        )
    _require(len({proof["oid"] for proof in expected_roots.values()}) == len(ROOTS), "roots_required")
    _require(
        type(dataset_ids) is list
        and all(type(key) is str and 0 < len(key) <= 96 and "\x00" not in key for key in dataset_ids)
        and dataset_ids == sorted(set(dataset_ids)),
        "datasets_required",
    )
    return {**expected_catalog, "roots": {root: dict(proof) for root, proof in expected_roots.items()}}, list(
        dataset_ids
    )


async def _state(database, session, connection):
    _require(isinstance(database, context.Database) and isinstance(session, context.AsyncSession), "session_required")
    await context._physical(database, session, connection, asyncio.current_task(), session.get_transaction())
    actor_by_field = dict(await connection.fetchrow(context._ORIGIN_SQL))
    _require(
        actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["readonly"] == "on"
        and actor_by_field["isolation"] == "repeatable read"
        and actor_by_field["replication_role"] == "origin"
        and await connection.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user") is True,
        "original_admin_snapshot_required",
    )
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    _require(
        await connection.fetchval(
            "SELECT count(*)=2 AND bool_and(setting::bigint>0) FROM pg_catalog.pg_settings "
            "WHERE name IN ('statement_timeout','lock_timeout')"
        )
        is True,
        "bounded_settings_required",
    )
    return actor_by_field


async def _roots(connection, schema, expected):
    await layout.require_cms_canonical_layout(connection, catalog_schema=schema, storage_class=layout.CONTRACT)
    rows = await connection.fetch(_ROOTS_SQL, schema, list(ROOTS))
    _require(len(rows) == len(ROOTS) and {row["name"] for row in rows} == set(ROOTS), "roots_changed")
    for row in rows:
        root = row["name"]
        _require(
            row["nspname"] == schema
            and row["schema_oid"] == expected["schema_oid"]
            and row["relkind"] == ("p" if root == RESOURCE else "r")
            and (root == RESOURCE or row["leaf"])
            and {"oid": row["oid"], "owner": row["owner"]} == expected["roots"][root],
            "roots_changed",
        )


async def _counts(connection, relation, dataset_ids):
    _require(
        await connection.fetchval(
            f"SELECT EXISTS(SELECT FROM ONLY {relation} WHERE NOT(dataset_id=ANY($1::text[])))", dataset_ids
        )
        is False,
        "dataset_outside_bounds",
    )
    rows = await connection.fetch(
        f"SELECT dataset_id,count(*) AS rows FROM ONLY {relation} WHERE dataset_id=ANY($1::text[]) GROUP BY dataset_id",
        dataset_ids,
    )
    counts_by_dataset = {row["dataset_id"]: row["rows"] for row in rows}
    return {key: counts_by_dataset.get(key, 0) for key in dataset_ids}


async def _relationships(connection, relations_by_root):
    """Check the complete three composite-key edges without a per-row application query."""
    for child, parent in _EDGES:
        accepted = await connection.fetchval(
            f"SELECT NOT EXISTS(SELECT FROM ONLY {relations_by_root[child]} child WHERE NOT EXISTS("
            f"SELECT FROM {relations_by_root[parent]} parent WHERE parent.dataset_id=child.dataset_id "
            "AND parent.resource_type=child.resource_type AND parent.resource_id=child.resource_id))"
        )
        _require(accepted is True, "relationship_changed:" + child + ":" + parent)


async def _scan_relations(connection, session, *, schema, receipts, datasets_by_root, relations_by_root):
    """Scan authenticated heap identities; the caller supplies custody and snapshot."""
    counts, content = {}, {}
    for root in (WITNESS, RELATIONSHIP):
        counts[root] = await _counts(connection, relations_by_root[root], datasets_by_root[root])
        content[root] = await _copy_native_content(
            connection,
            session,
            schema=schema,
            receipt=receipts[root],
        )
        _require(content[root]["rows"] == sum(counts[root].values()), "row_count_changed")
    await _relationships(connection, relations_by_root)
    return {"dataset_rows": counts, "content": content}


async def _scan_history(connection, session, *, schema, expected, datasets):
    """Share complete scans; the enclosing entry point authenticates custody and transaction mode."""
    await _roots(connection, schema, expected)
    scanned = await _scan_relations(
        connection,
        session,
        schema=schema,
        receipts={root: {"name": root, "oid": expected["roots"][root]["oid"]} for root in (WITNESS, RELATIONSHIP)},
        datasets_by_root=dict.fromkeys((WITNESS, RELATIONSHIP), datasets),
        relations_by_root={root: storage_relation(schema, root) for root in ROOTS},
    )
    await _roots(connection, schema, expected)
    return {"catalog": expected, **scanned}


async def read_cms_legacy_history(database, session, connection, *, catalog_schema, expected_catalog, dataset_ids):
    """Read both complete heaps on one bounded original-admin snapshot and physical session.

    Native COPY streams every stored value in primary-key order. Three indexed
    anti-joins check the existing relationships. No setting, row, ACL or guard is
    changed; successful read pins last until the caller ends its transaction.
    This is preparatory data evidence, not a closed-writer or conversion receipt.
    """
    expected, datasets = _request(catalog_schema, expected_catalog, dataset_ids)
    actor_by_field = await _state(database, session, connection)
    _require(actor_by_field["database_oid"] == expected["database_oid"], "database_changed")
    async with _savepoint(connection):
        scanned = await _scan_history(connection, session, schema=catalog_schema, expected=expected, datasets=datasets)
        _require(await _state(database, session, connection) == actor_by_field, "snapshot_changed")
        _require(_request(catalog_schema, expected_catalog, dataset_ids) == (expected, datasets), "inputs_changed")
    return {
        "contract": CONTRACT,
        "evidence": "read_only_snapshot",
        **scanned,
    }
