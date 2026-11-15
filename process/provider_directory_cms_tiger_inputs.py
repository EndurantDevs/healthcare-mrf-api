# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed Source reads of the two protected inherited geographic parents."""

from __future__ import annotations

import asyncio
import re
from copy import deepcopy
from uuid import UUID

from sqlalchemy import text

ROOTS = ("zcta5", "zip_state")
_INVENTORY_FIELDS = {"schema_name", "relation_name"}
CATALOG_SQL = """SELECT requested.name AS parent_name,
    parent.relkind::text AS parent_kind,parent.relpersistence::text AS parent_persistence,
    parent.relispartition AS parent_partition,parent.relrowsecurity AS parent_rls,
    parent.relforcerowsecurity AS parent_force_rls,
    (SELECT count(*) FROM pg_inherits WHERE inhparent=parent.oid) AS child_count,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=parent.oid) AS parent_is_child,
    namespace.nspname AS schema_name,child.relname AS relation_name,
    child.relkind::text AS child_kind,child.relpersistence::text AS child_persistence,
    child.relispartition AS child_partition,child.relrowsecurity AS child_rls,
    child.relforcerowsecurity AS child_force_rls,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=child.oid) AS child_has_children,
    (SELECT count(*) FROM pg_inherits WHERE inhrelid=child.oid) AS child_parent_count
    FROM (VALUES ('zcta5'),('zip_state')) requested(name)
    LEFT JOIN pg_class parent ON parent.oid=to_regclass('tiger.'||requested.name)
    LEFT JOIN pg_inherits edge ON edge.inhparent=parent.oid
    LEFT JOIN pg_class child ON child.oid=edge.inhrelid
    LEFT JOIN pg_namespace namespace ON namespace.oid=child.relnamespace ORDER BY requested.name"""


def _require(condition):
    if not condition:
        raise RuntimeError("cms_address_native_tiger_proof_changed")


def _identifier(identifier, maximum):
    return (
        type(identifier) is str
        and len(identifier) <= maximum
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]*", identifier)
    )


def _uuid(value):
    try:
        _require(type(value) is str and str(UUID(value)) == value)
    except (ValueError, AttributeError, TypeError) as error:
        raise RuntimeError("cms_address_native_tiger_proof_changed") from error


def _parents(proof):
    if "parents" in proof:
        _require(proof["parents"] == [["tiger", name] for name in ROOTS])
    else:
        pairs = proof.get("parent_oids")
        _require(
            type(pairs) is list
            and len(pairs) == 2
            and all(type(pair) is list and len(pair) == 2 and pair[0] == name for pair, name in zip(pairs, ROOTS))
        )
    return [("tiger", name) for name in ROOTS]


def require_protected_tiger_proof(proof):
    """Validate logical selection fields; saved physical tokens are never authority."""
    _require(type(proof) is dict and type(proof.get("version")) is int and proof["version"] in (1, 2))
    if proof["version"] == 2:
        _require_captured_tiger_proof(proof)
        return
    required_fields = {
        "version",
        "node_id",
        "generation_id",
        "installation_id",
        "package_id",
        "fence_token",
        "registration_sha256",
        "inventory",
    }
    legacy_fields = {"database_oid", "receipt_sha256", "parent_inventory_sha256", "parent_oids"}
    _require(required_fields <= proof.keys() and proof.keys() <= required_fields | legacy_fields | {"parents"})
    _require(_identifier(proof["node_id"], 128) and _identifier(proof["package_id"], 256))
    for field in ("generation_id", "installation_id", "fence_token"):
        _uuid(proof[field])
    for field in ("registration_sha256", "receipt_sha256", "parent_inventory_sha256"):
        if field in proof:
            _require(type(proof[field]) is str and re.fullmatch(r"[0-9a-f]{64}", proof[field]))
    _parents(proof)
    _require_inventory(proof)


def _require_inventory(proof):
    inventory = proof["inventory"]
    _require(type(inventory) is dict and set(inventory) <= {"database_oid", "relations"})
    children = inventory["relations"]
    _require(type(children) is list and len(children) == 2)
    for child in children:
        _require(
            type(child) is dict
            and set(child)
            in (
                _INVENTORY_FIELDS,
                _INVENTORY_FIELDS | {"relation_oid", "schema_oid", "schema_owner_oid", "owner_oid", "relfilenode"},
            )
        )
        _require(
            type(child["schema_name"]) is str
            and re.fullmatch(r"reference_family_archive_[0-9a-f]{32}", child["schema_name"])
        )
        _require(type(child["relation_name"]) is str and child["relation_name"] in ROOTS)
    _require(tuple(child["relation_name"] for child in sorted(children, key=lambda row: row["relation_name"])) == ROOTS)
    _require(len({child["schema_name"] for child in children}) == 1)
    if all(set(child) == _INVENTORY_FIELDS for child in children):
        _require(tuple(child["relation_name"] for child in children) == ROOTS)


def protected_tiger_relations(proof, relations):
    """Exclude the fixed named inherited parents from mutable input handling."""
    if proof is None:
        _require(not any(relations.get(f'"tiger"."{name}"', {}).get("inherited") for name in ROOTS))
        return set()
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 1)
    names = set()
    for schema, name in _parents(proof):
        quoted = f'"{schema}"."{name}"'
        observed = relations.get(quoted, {})
        _require(
            observed.get("relkind") == "r"
            and observed.get("relpersistence") == "p"
            and observed.get("inherited") is True
        )
        names.add(quoted)
    return names


def require_tiger_catalog(proof, catalog_rows):
    """Require exact named edges and native heap shape."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 1 and len(catalog_rows) == 2)
    children_by_table = {row["relation_name"]: row for row in proof["inventory"]["relations"]}
    for catalog, (_schema, name) in zip(catalog_rows, _parents(proof)):
        _require(catalog["parent_name"] == name)
        _require(catalog["parent_kind"] == catalog["child_kind"] == "r")
        _require(catalog["parent_persistence"] == catalog["child_persistence"] == "p")
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
        _require(
            {field: catalog[field] for field in _INVENTORY_FIELDS}
            == {field: children_by_table[name][field] for field in _INVENTORY_FIELDS}
        )


async def _lock_names(session, proof):
    names = captured_tiger_names(proof)
    if proof["version"] == 1:
        names += [f'"tiger"."{name}"' for name in ROOTS]
    await session.execute(text("LOCK TABLE ONLY " + ", ONLY ".join(sorted(names)) + " IN ACCESS SHARE MODE NOWAIT"))


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
    await _lock_names(session, proof)
    catalog = (await session.execute(text(CATALOG_SQL))).mappings().all()
    require_tiger_catalog(proof, catalog)
    _require(source_policy._FACTORY.get() is factory and session.in_transaction())
    _require(source_policy.source_producer_identity() == producer)
    return proof


async def verify_raw_protected_tiger_input(connection, proof):
    """Reobserve admin-provided linkage on its original native transaction, without a Factory."""
    _require(connection.is_in_transaction())
    task = asyncio.current_task()
    frozen_proof = deepcopy(proof)
    require_protected_tiger_proof(frozen_proof)
    names = captured_tiger_names(frozen_proof)
    if frozen_proof["version"] == 1:
        names += [f'"tiger"."{name}"' for name in ROOTS]
    await connection.execute("LOCK TABLE ONLY " + ", ONLY ".join(sorted(names)) + " IN ACCESS SHARE MODE NOWAIT")
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
    required_fields = {
        "version",
        "node_id",
        "selection_kind",
        "source_id",
        "package_id",
        "registration_sha256",
        "selection_sha256",
        "inventory",
    }
    _require(required_fields <= proof.keys() and proof.keys() <= required_fields | {"database_oid"})
    _require(_identifier(proof["node_id"], 128) and proof["selection_kind"] in {"captured-source", "captured-epoch"})
    _uuid(proof["source_id"])
    for field in ("package_id", "registration_sha256", "selection_sha256"):
        _require(type(proof[field]) is str and re.fullmatch(r"[0-9a-f]{64}", proof[field]))
    _require_inventory(proof)


def captured_tiger_bindings(proof):
    """Decode physical linkage only; actual Source authority is observed separately."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 2)
    return {
        "tiger." + child["relation_name"]: dict(
            schema_name=child["schema_name"],
            table_name=child["relation_name"],
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
    await _lock_names(session, proof)
    catalog = (
        (await session.execute(text(CAPTURED_CATALOG_SQL), {"names": captured_tiger_names(proof)})).mappings().all()
    )
    require_captured_tiger_catalog(proof, catalog)
    _require(
        policy._FACTORY.get() is factory and session.in_transaction() and policy.source_producer_identity() == producer
    )
    return proof


CAPTURED_CATALOG_SQL = """SELECT n.nspname AS schema_name,c.relname AS relation_name,
    EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
    c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
      AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
      AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid)
      AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid) AS valid
    FROM unnest(CAST(:names AS text[])) requested(name)
    JOIN pg_class c ON c.oid=to_regclass(requested.name)
    JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY n.nspname,c.relname"""


def captured_tiger_names(proof):
    """Quote only the fixed two names from already typed physical linkage."""
    return [
        '"' + child["schema_name"] + '"."' + child["relation_name"] + '"' for child in proof["inventory"]["relations"]
    ]


def require_captured_tiger_catalog(proof, catalog_rows):
    """Verify exact closed native data heaps, independent of the observing actor's privileges."""
    require_protected_tiger_proof(proof)
    _require(proof["version"] == 2 and len(catalog_rows) == 2)
    expected = sorted(
        ({field: row[field] for field in _INVENTORY_FIELDS} for row in proof["inventory"]["relations"]),
        key=lambda row: row["relation_name"],
    )
    actual = sorted(catalog_rows, key=lambda row: row["relation_name"])
    for saved, row in zip(expected, actual):
        _require({field: row[field] for field in _INVENTORY_FIELDS} == saved)
        _require(row["valid"] is True and row["inherited"] is False)


def captured_tiger_relations(proof, relations):
    """Match the captured two heaps before excluding them from mutable input handling."""
    if proof is None:
        return set()
    bindings = captured_tiger_bindings(proof)
    for canonical, binding in bindings.items():
        observed = relations.get('"' + canonical.replace(".", '"."') + '"', {})
        _require(
            observed.get("schema_name") == binding["schema_name"]
            and observed.get("table_name", observed.get("relation_name")) == binding["table_name"]
        )
        _require(
            observed.get("relkind") == "r"
            and observed.get("relpersistence") == "p"
            and observed.get("inherited") is False
        )
    return {'"' + name.replace(".", '"."') + '"' for name in bindings}
