# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed Source reads of the two protected inherited geographic parents."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from copy import deepcopy
from uuid import UUID

from sqlalchemy import text

ROOTS = ("zcta5", "zip_state")
_FIELDS = {
    "version",
    "database_oid",
    "node_id",
    "generation_id",
    "installation_id",
    "package_id",
    "fence_token",
    "registration_sha256",
    "receipt_sha256",
    "parent_inventory_sha256",
    "parent_oids",
    "inventory",
}
_INVENTORY_FIELDS = {
    "schema_name",
    "relation_name",
    "relation_oid",
    "schema_oid",
    "schema_owner_oid",
    "owner_oid",
    "relfilenode",
}
CATALOG_SQL = """SELECT requested.name AS parent_name,parent.oid::bigint AS parent_oid,
    parent.relkind::text AS parent_kind,parent.relpersistence::text AS parent_persistence,
    pg_relation_filenode(parent.oid)::bigint AS parent_filenode,parent.relowner::bigint AS parent_owner,
    parent.relispartition AS parent_partition,parent.relrowsecurity AS parent_rls,
    parent.relforcerowsecurity AS parent_force_rls,
    (SELECT count(*) FROM pg_inherits WHERE inhparent=parent.oid) AS child_count,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=parent.oid) AS parent_is_child,
    namespace.nspname AS schema_name,child.relname AS relation_name,child.oid::bigint AS relation_oid,
    namespace.oid::bigint AS schema_oid,namespace.nspowner::bigint AS schema_owner_oid,
    child.relowner::bigint AS owner_oid,pg_relation_filenode(child.oid)::bigint AS relfilenode,
    child.relkind::text AS child_kind,child.relpersistence::text AS child_persistence,
    child.relispartition AS child_partition,child.relrowsecurity AS child_rls,
    child.relforcerowsecurity AS child_force_rls,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=child.oid) AS child_has_children,
    (SELECT count(*) FROM pg_inherits WHERE inhrelid=child.oid) AS child_parent_count,
    (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
    EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation'
      AND relation=parent.oid AND mode='AccessShareLock' AND granted) AS parent_read_lock,
    EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation'
      AND relation=child.oid AND mode='AccessShareLock' AND granted) AS child_read_lock
    FROM (VALUES ('zcta5'),('zip_state')) requested(name)
    LEFT JOIN pg_class parent ON parent.oid=to_regclass('tiger.'||requested.name)
    LEFT JOIN pg_inherits edge ON edge.inhparent=parent.oid
    LEFT JOIN pg_class child ON child.oid=edge.inhrelid
    LEFT JOIN pg_namespace namespace ON namespace.oid=child.relnamespace ORDER BY requested.name"""


def _require(condition):
    if not condition:
        raise RuntimeError("cms_address_native_tiger_proof_changed")


def _is_oid(number):
    return type(number) is int and 0 < number < 2**32


def _identifier(identifier, maximum):
    return (
        type(identifier) is str
        and len(identifier) <= maximum
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]*", identifier)
    )


def require_protected_tiger_proof(proof):
    """Validate serialized linkage; only an admitted native callback grants authority."""
    if type(proof) is dict and type(proof.get("version")) is int and proof["version"] == 2:
        _require_captured_tiger_proof(proof)
        return
    _require(type(proof) is dict and set(proof) == _FIELDS)
    _require(type(proof["version"]) is int and proof["version"] == 1 and _is_oid(proof["database_oid"]))
    _require(_identifier(proof["node_id"], 128) and _identifier(proof["package_id"], 256))
    for field in ("generation_id", "installation_id", "fence_token"):
        try:
            _require(type(proof[field]) is str and str(UUID(proof[field])) == proof[field])
        except (ValueError, AttributeError, TypeError) as exc:
            raise RuntimeError("cms_address_native_tiger_proof_changed") from exc
    for field in ("registration_sha256", "receipt_sha256", "parent_inventory_sha256"):
        _require(type(proof[field]) is str and re.fullmatch(r"[0-9a-f]{64}", proof[field]))
    parents = proof["parent_oids"]
    _require(type(parents) is list and len(parents) == 2)
    _require(
        all(
            type(pair) is list and len(pair) == 2 and pair[0] == name and _is_oid(pair[1])
            for pair, name in zip(parents, ROOTS)
        )
    )
    _require(parents[0][1] != parents[1][1])
    _require_inventory(proof)


def _require_inventory(proof):
    inventory = proof["inventory"]
    _require(type(inventory) is dict and set(inventory) == {"database_oid", "relations"})
    _require(inventory["database_oid"] == proof["database_oid"] and _is_oid(inventory["database_oid"]))
    children = inventory["relations"]
    _require(type(children) is list and len(children) == 2)
    for child in children:
        _require(type(child) is dict and set(child) == _INVENTORY_FIELDS)
        _require(all(_is_oid(child[field]) for field in _INVENTORY_FIELDS - {"schema_name", "relation_name"}))
        _require(
            type(child["schema_name"]) is str
            and re.fullmatch(r"reference_family_archive_[0-9a-f]{32}", child["schema_name"])
        )
        _require(type(child["relation_name"]) is str and child["relation_name"] in ROOTS)
        _require(child["schema_owner_oid"] == child["owner_oid"])
    _require({child["relation_name"] for child in children} == set(ROOTS))
    _require(len({(child["schema_name"], child["schema_oid"], child["owner_oid"]) for child in children}) == 1)
    _require(children[0]["relation_oid"] < children[1]["relation_oid"])
    _require(len({child["relfilenode"] for child in children}) == 2)
    if proof["version"] == 1:
        _require(not {child["relation_oid"] for child in children} & {pair[1] for pair in proof["parent_oids"]})


def protected_tiger_relations(proof, relations):
    """Exclude only both fixed persistent inherited parents matching typed evidence."""
    if proof is None:
        _require(not any(relations.get(f'"tiger"."{name}"', {}).get("inherited") for name in ROOTS))
        return set()
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 1)
    names = set()
    for name, oid in proof["parent_oids"]:
        quoted = f'"tiger"."{name}"'
        physical = relations.get(quoted, {})
        _require(physical.get("relation_oid") == oid and physical.get("relkind") == "r")
        _require(physical.get("relpersistence") == "p" and physical.get("inherited") is True)
        _require(_is_oid(physical.get("relfilenode")))
        names.add(quoted)
    return names


def require_tiger_catalog(proof, catalog_rows):
    """Reobserve exact native edges, ownership, files and held Source read locks."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 1)
    _require(len(catalog_rows) == 2)
    children_by_name = {child["relation_name"]: child for child in proof["inventory"]["relations"]}
    for catalog, (name, oid) in zip(catalog_rows, proof["parent_oids"]):
        _require(catalog["parent_name"] == name and catalog["parent_oid"] == oid)
        _require(catalog["database_oid"] == proof["database_oid"] and _is_oid(catalog["database_oid"]))
        _require(catalog["parent_kind"] == catalog["child_kind"] == "r")
        _require(catalog["parent_persistence"] == catalog["child_persistence"] == "p")
        _require(_is_oid(catalog["parent_filenode"]) and catalog["parent_owner"] == catalog["owner_oid"])
        _require(catalog["child_count"] == catalog["child_parent_count"] == 1)
        _require(
            all(
                catalog[field] is False
                for field in (
                    "parent_partition",
                    "parent_rls",
                    "parent_force_rls",
                    "parent_is_child",
                    "child_partition",
                    "child_rls",
                    "child_force_rls",
                    "child_has_children",
                )
            )
        )
        _require(catalog["parent_read_lock"] is True and catalog["child_read_lock"] is True)
        _require({field: catalog[field] for field in _INVENTORY_FIELDS} == children_by_name[name])


async def capture_protected_tiger_input(session, relations):
    """Use only the bound issued Factory and keep the caller's Source transaction."""
    from process import provider_directory_generic_source_policy as source_policy

    factory = source_policy._FACTORY.get()
    producer = source_policy.source_producer_identity()
    callback = getattr(factory, "native_tiger_input_fence", None)
    _require(callable(callback) and session.in_transaction())
    task = asyncio.current_task()
    proof = deepcopy(await callback(session))
    _require(asyncio.current_task() is task and source_policy._FACTORY.get() is factory and session.in_transaction())
    _require(source_policy.source_producer_identity() == producer)
    protected_tiger_relations(proof, relations)
    catalog = (await session.execute(text(CATALOG_SQL))).mappings().all()
    require_tiger_catalog(proof, catalog)
    _require(source_policy._FACTORY.get() is factory and session.in_transaction())
    _require(source_policy.source_producer_identity() == producer)
    _require(
        all(relations[f'"tiger"."{row["parent_name"]}"']["relfilenode"] == row["parent_filenode"] for row in catalog)
    )
    return proof


async def verify_raw_protected_tiger_input(connection, proof):
    """Reobserve admin-provided linkage on its original native transaction, without a Factory."""
    _require(connection.is_in_transaction())
    task = asyncio.current_task()
    frozen_proof = deepcopy(proof)
    require_protected_tiger_proof(frozen_proof)
    if frozen_proof["version"] == 2:
        from process.provider_directory_cms_native_registration import _NativeNuccSession

        query, parameters = _NativeNuccSession(connection)._compiled(
            text(CAPTURED_CATALOG_SQL), {"names": captured_tiger_names(frozen_proof)}
        )
        require_captured_tiger_catalog(frozen_proof, await connection.fetch(query, *parameters))
    else:
        require_tiger_catalog(frozen_proof, await connection.fetch(CATALOG_SQL))
    _require(asyncio.current_task() is task and connection.is_in_transaction())
    return frozen_proof


def _require_captured_tiger_proof(proof):
    _require(
        set(proof)
        == {
            "version",
            "database_oid",
            "node_id",
            "selection_kind",
            "source_id",
            "package_id",
            "registration_sha256",
            "selection_sha256",
            "inventory",
        }
    )
    _require(_is_oid(proof["database_oid"]) and _identifier(proof["node_id"], 128))
    _require(proof["selection_kind"] in {"captured-source", "captured-epoch"})
    try:
        _require(type(proof["source_id"]) is str and str(UUID(proof["source_id"])) == proof["source_id"])
    except (ValueError, TypeError, AttributeError) as exc:
        raise RuntimeError("cms_address_native_tiger_proof_changed") from exc
    for field in ("package_id", "registration_sha256", "selection_sha256"):
        _require(type(proof[field]) is str and re.fullmatch(r"[0-9a-f]{64}", proof[field]))
    _require_inventory(proof)
    payload = {field: attribute for field, attribute in proof.items() if field != "selection_sha256"}
    _require(
        hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        == proof["selection_sha256"]
    )


def captured_tiger_bindings(proof):
    """Decode physical linkage only; actual Source authority is observed separately."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 2)
    return {
        "tiger." + child["relation_name"]: dict(
            schema_name=child["schema_name"],
            table_name=child["relation_name"],
            relation_oid=child["relation_oid"],
            relfilenode=child["relfilenode"],
        )
        for child in proof["inventory"]["relations"]
    }


async def capture_selected_captured_tiger_input(session):
    """Ask only the issued Factory before resolving any canonical geographic reads."""
    from process import provider_directory_generic_source_policy as policy

    factory = policy._FACTORY.get()
    if factory is None:
        return None
    producer = policy.source_producer_identity()
    callback = getattr(factory, "native_tiger_input_fence", None)
    _require(callable(callback) and session.in_transaction())
    task = asyncio.current_task()
    proof = deepcopy(await callback(session))
    _require(asyncio.current_task() is task and policy._FACTORY.get() is factory and session.in_transaction())
    _require(policy.source_producer_identity() == producer)
    if proof is None:
        return None
    require_protected_tiger_proof(proof)
    if proof["version"] == 1:
        return None
    catalog = (
        (await session.execute(text(CAPTURED_CATALOG_SQL), {"names": captured_tiger_names(proof)})).mappings().all()
    )
    require_captured_tiger_catalog(proof, catalog)
    _require(
        policy._FACTORY.get() is factory and session.in_transaction() and policy.source_producer_identity() == producer
    )
    return proof


CAPTURED_CATALOG_SQL = """SELECT c.oid::bigint AS relation_oid,c.relowner::bigint AS owner_oid,
    n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,
    pg_relation_filenode(c.oid)::bigint AS relfilenode,n.nspname AS schema_name,c.relname AS relation_name,
    (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
    EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation'
      AND relation=c.oid AND mode='AccessShareLock' AND granted) AS read_lock,
    c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
      AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND c.relowner=n.nspowner
      AND NOT r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreaterole AND NOT r.rolcreatedb
      AND NOT r.rolreplication AND NOT r.rolbypassrls
      AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
        WHERE a.grantee<>n.nspowner AND a.privilege_type='CREATE')
      AND NOT EXISTS(SELECT 1 FROM (
        SELECT a.grantee,a.privilege_type FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
        UNION ALL SELECT a.grantee,a.privilege_type FROM pg_attribute col,
          LATERAL aclexplode(col.attacl) a WHERE col.attrelid=c.oid) grants
        WHERE grantee<>c.relowner AND privilege_type<>'SELECT')
      AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) AS protected
    FROM unnest(CAST(:names AS text[])) requested(name)
    JOIN pg_class c ON c.oid=to_regclass(requested.name)
    JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner ORDER BY c.oid"""


def captured_tiger_names(proof):
    """Quote only the fixed two names from already typed physical linkage."""
    return [
        '"' + child["schema_name"] + '"."' + child["relation_name"] + '"' for child in proof["inventory"]["relations"]
    ]


def require_captured_tiger_catalog(proof, catalog_rows):
    """Verify exact closed native data heaps, independent of the observing actor's privileges."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 2 and len(catalog_rows) == 2)
    for expected, actual in zip(proof["inventory"]["relations"], catalog_rows):
        _require({field: actual[field] for field in _INVENTORY_FIELDS} == expected)
        _require(actual["database_oid"] == proof["database_oid"])
        _require(actual["protected"] is True and actual["inherited"] is False and actual["read_lock"] is True)


def captured_tiger_relations(proof, relations):
    """Match the captured two heaps before excluding them from mutable input handling."""
    if proof is None:
        return set()
    bindings = captured_tiger_bindings(proof)
    for canonical, binding in bindings.items():
        observed = relations.get('"' + canonical.replace(".", '"."') + '"', {})
        _require(
            observed.get("relation_oid") == binding["relation_oid"]
            and observed.get("relfilenode") == binding["relfilenode"]
        )
        _require(
            observed.get("relkind") == "r"
            and observed.get("relpersistence") == "p"
            and observed.get("inherited") is False
        )
    return {'"' + name.replace(".", '"."') + '"' for name in bindings}
