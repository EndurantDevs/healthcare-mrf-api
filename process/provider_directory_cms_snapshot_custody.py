# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check closed private payloads against authenticated preparation coordinates.

The caller supplies protected record/profile provenance and complete content,
callable and role/DDL custody. This observes native DML/owner closure only; it
does not transfer ownership, issue CMS authority or attach/publish any table.
"""

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_workspace as workspace
from process.plan_release_storage_catalog import CATALOG_SQL

CONTRACT = "cms-private-payload-custody-evidence-v1"
_PRINCIPALS_SQL = """SELECT o.oid::bigint AS owner_oid,n.oid::bigint AS schema_oid,
 n.nspowner::bigint AS namespace_owner_oid,
 NOT (o.rolcanlogin OR o.rolsuper OR o.rolcreatedb OR o.rolcreaterole OR o.rolreplication OR o.rolbypassrls)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles e WHERE pg_catalog.pg_has_role(o.oid,e.oid,'MEMBER')
   AND (e.rolsuper OR e.rolcreatedb OR e.rolcreaterole OR e.rolreplication OR e.rolbypassrls))
 AND p.rolcanlogin AND NOT (p.rolsuper OR p.rolcreatedb OR p.rolcreaterole OR p.rolreplication OR p.rolbypassrls)
 AND NOT (ns.rolcanlogin OR ns.rolsuper OR ns.rolcreatedb OR ns.rolcreaterole
   OR ns.rolreplication OR ns.rolbypassrls)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles e WHERE pg_catalog.pg_has_role(ns.oid,e.oid,'MEMBER')
   AND (e.rolsuper OR e.rolcreatedb OR e.rolcreaterole OR e.rolreplication OR e.rolbypassrls)) AS protected,
 p.oid::bigint AS publisher_oid,m.oid::bigint AS migration_oid
 FROM pg_catalog.pg_roles o,pg_catalog.pg_roles p,pg_catalog.pg_roles m,
 pg_catalog.pg_namespace n JOIN pg_catalog.pg_roles ns ON ns.oid=n.nspowner
 WHERE o.rolname=$1 AND p.rolname=$2 AND m.rolname=$3 AND n.nspname=$4"""
_WRITERS_SQL = """WITH actors AS (
 SELECT p.oid FROM pg_catalog.pg_roles a JOIN pg_catalog.pg_roles p
 ON pg_catalog.pg_has_role(a.oid,p.oid,'MEMBER')
 WHERE a.rolcanlogin AND NOT a.rolsuper AND a.oid<>$4::oid
)
SELECT NOT EXISTS(SELECT FROM actors a WHERE
 pg_catalog.pg_has_role(a.oid,$2::oid,'MEMBER')
 OR pg_catalog.pg_has_role(a.oid,$3::oid,'MEMBER'))
 AND NOT EXISTS(SELECT FROM actors a,unnest($1::oid[]) r(oid) WHERE
 pg_catalog.has_table_privilege(a.oid,r.oid,
   'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN,SELECT WITH GRANT OPTION')
 OR pg_catalog.has_any_column_privilege(a.oid,r.oid,'INSERT,UPDATE,REFERENCES,SELECT WITH GRANT OPTION'))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_snapshot_custody_" + code)


async def _principals(connection, source_binding, retained, owner_role, owner_oid, migration_role):
    policy = retained["prepared_policy"]
    principals = await connection.fetchrow(
        _PRINCIPALS_SQL, owner_role, policy["publisher_role"], migration_role, policy["schema"]
    )
    _require(
        principals is not None
        and principals["protected"] is True
        and principals["owner_oid"] == owner_oid
        and principals["schema_oid"] == source_binding.storage_schema_oid
        and principals["namespace_owner_oid"] == source_binding.namespace_owner_oid
        and principals["publisher_oid"] != principals["migration_oid"]
        and owner_oid not in {source_binding.owner_oid, retained["tables"][workspace.storage.RESOURCE]["owner"]},
        "principals_changed",
    )
    return principals


async def _observe(connection, source_binding, retained, owner_role, owner_oid, migration_role):
    policy = retained["prepared_policy"]
    principals = await _principals(connection, source_binding, retained, owner_role, owner_oid, migration_role)
    names = [retained["tables"][root]["name"] for root in workspace.storage.RELATIONS]
    relations = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(policy["schema"], name) for name in names],
        owner_oid,
        [principals["publisher_oid"]],
    )
    _require(
        len(relations) == len(names)
        and {relation_by_field["relname"] for relation_by_field in relations} == set(names),
        "inventory_changed",
    )
    proofs_by_name = {}
    for relation_by_field in relations:
        _require(
            relation_by_field["nspname"] == policy["schema"]
            and relation_by_field["relowner"] == owner_oid
            and relation_by_field["readers_closed"] is True,
            "rights_changed",
        )
        proofs_by_name[relation_by_field["relname"]] = workspace.storage._table_proof(relation_by_field, private=True)
    for root in workspace.storage.RELATIONS:
        original = retained["tables"][root]
        _require(proofs_by_name[original["name"]] == {**original, "owner": owner_oid}, "layout_changed")
    oids = sorted(proof["oid"] for proof in proofs_by_name.values())
    _require(
        await connection.fetchval(
            _WRITERS_SQL, oids, owner_oid, principals["namespace_owner_oid"], principals["migration_oid"]
        )
        is True,
        "writer_reachable",
    )
    return {
        "contract": CONTRACT,
        "owner_oid": owner_oid,
        "schema_oid": principals["schema_oid"],
        "namespace_owner_oid": principals["namespace_owner_oid"],
        "tables": {root: proofs_by_name[retained["tables"][root]["name"]] for root in workspace.storage.RELATIONS},
    }


async def require_cms_payload_snapshot_custody(
    connection, *, source_binding, prepared_inputs, handoff, expected_workspace, owner_role, owner_oid, migration_role
):
    """Pin only six closed private payloads and compare the retained native layouts.

    Expected owner/maintenance identities and the workspace must come from the
    protected caller, never observed privileges or an untrusted input document.
    Frozen Source inputs and the five mutable identity outputs are not adopted.
    Recheck this evidence under the caller's role/DDL fence before publication.
    """
    storage_identifier(owner_role)
    storage_identifier(migration_role)
    _require(type(owner_oid) is int and 0 < owner_oid < 2**32, "owner_required")
    retained, _binding, _receipts = workspace._retained_workspace(
        source_binding, prepared_inputs, handoff, expected_workspace
    )
    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    actor_by_field = dict(await connection.fetchrow(workspace.context._ORIGIN_SQL))
    _require(
        actor_by_field["session"] == actor_by_field["current"] == retained["prepared_policy"]["publisher_role"]
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["database_oid"] == source_binding.database_oid
        and actor_by_field["replication_role"] == "origin"
        and bool(actor_by_field["transaction"]),
        "original_publisher_required",
    )
    names = sorted(
        storage_relation(retained["prepared_policy"]["schema"], retained["tables"][root]["name"])
        for root in workspace.storage.RELATIONS
    )
    async with workspace._savepoint(connection):
        await connection.execute(
            "LOCK TABLE " + ",".join("ONLY " + name for name in names) + " IN ACCESS SHARE MODE NOWAIT"
        )
        before = await _observe(connection, source_binding, retained, owner_role, owner_oid, migration_role)
        after = await _observe(connection, source_binding, retained, owner_role, owner_oid, migration_role)
        _require(
            dict(await connection.fetchrow(workspace.context._ORIGIN_SQL)) == actor_by_field, "transaction_changed"
        )
        current, _binding, _receipts = workspace._retained_workspace(
            source_binding, prepared_inputs, handoff, expected_workspace
        )
        _require(before == after and current == retained, "inventory_changed")
    return before
