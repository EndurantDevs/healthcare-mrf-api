# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact protected TIGER custody for ordinary native Address publication."""

import hashlib
import json
import os
import re

from sqlalchemy import text

from process import reference_family_archive as archive
from process.tiger_captured_epoch import validate_captured_origin

_SCHEMA = re.compile(r"reference_family_archive_[0-9a-f]{32}\Z")
_RELATION = re.compile(r"[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}\Z")
_CURRENT = "hp_snapshot_retention.current_generation"
_GENERATION = "hp_snapshot_retention.generation"
_CAPTURE = "hp_snapshot_retention.tiger_captured_epoch"
_CUSTODY = "hp_snapshot_retention.tiger_snapshot_inheritance"


def _require(condition):
    if not condition:
        raise RuntimeError("entity-address protected TIGER input changed")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _source_selection_relations():
    """Read an optional trusted catalog pair; configuration never provides custody."""
    relations = tuple(
        os.getenv(name) for name in ("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", "HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION")
    )
    if relations == (None, None):
        return None
    if any(value is None or _RELATION.fullmatch(value) is None for value in relations) or len(set(relations)) != 2:
        raise ValueError("TIGER source selection requires two distinct schema.table relation settings")
    return tuple('"' + value.replace(".", '"."') + '"' for value in relations)


async def _scope(session, value):
    _require(
        await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:scope,0))"),
            {"scope": "snapshot-retention:" + value},
        )
    )


async def _protected_relation(session, qualified, *, expected_owner=None, catalog=False):
    """Require immutable ordinary-reader access, not a writable look-alike catalog."""
    await session.execute(text(f"LOCK TABLE ONLY {qualified} IN ACCESS SHARE MODE NOWAIT"))
    relation = (
        (
            await session.execute(
                text("""
        SELECT c.oid::bigint AS relation_oid,c.relowner::bigint AS owner_oid,
          c.relnamespace::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,
          pg_relation_filenode(c.oid)::bigint AS relfilenode,
          n.nspname AS schema_name,c.relname AS relation_name,
          c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
            AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
            AND NOT r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolcreaterole
            AND NOT r.rolcreatedb AND NOT r.rolreplication AND NOT r.rolbypassrls
            AND c.relowner=n.nspowner
            AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
              WHERE a.grantee<>n.nspowner AND a.privilege_type='CREATE')
            AND NOT EXISTS(SELECT 1 FROM (
              SELECT a.grantee,a.privilege_type FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
              UNION ALL SELECT a.grantee,a.privilege_type FROM pg_attribute col,
                LATERAL aclexplode(col.attacl) a WHERE col.attrelid=c.oid
            ) grants WHERE grantee<>c.relowner AND privilege_type<>'SELECT')
            AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid) AS protected
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=to_regclass(:name)
    """),
                {"name": qualified},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(relation is not None and relation["protected"])
    _require(expected_owner is None or relation["owner_oid"] == expected_owner)
    if catalog:
        _require(
            not await session.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=:oid OR inhparent=:oid)"),
                {"oid": relation["relation_oid"]},
            )
        )
    return {key: attribute for key, attribute in relation.items() if key != "protected"}


async def selected_tiger_inventory(session):
    """Select one actual local current installation or source-bound captured epoch.

    Ordinary package rows select a capture; only its protected catalog provides
    authority. No latest capture, root OID, or synthetic serving generation is used.
    """
    archive._require_transaction(session)
    if await session.scalar(text("SELECT to_regclass(:name)"), {"name": _CURRENT}) is not None:
        await _protected_relation(session, _CURRENT, catalog=True)
        owner = await _protected_relation(session, _GENERATION, catalog=True)
        generation_rows = (
            (
                await session.execute(
                    text(f"""
            SELECT g.* FROM {_CURRENT} c JOIN {_GENERATION} g USING(generation_id)
            WHERE c.importer_id='tiger' AND c.dataset_key='tiger'
        """)
                )
            )
            .mappings()
            .all()
        )
        _require(len(generation_rows) <= 1)
        if generation_rows:
            generation = generation_rows[0]
            if all(relation["schema_name"] == "tiger" for relation in generation["inventory"]["relations"]):
                return None
            await _require_installed_selection(session, generation, owner["owner_oid"])
            return await _require_inventory(session, generation["inventory"], owner["owner_oid"])
    relations = _source_selection_relations()
    if relations is None:
        return None
    binding_relation, package_relation = relations
    for relation in relations:
        _require(await session.scalar(text("SELECT to_regclass(:name)"), {"name": relation}) is not None)
    selected = (
        (
            await session.execute(
                text(f"""
        SELECT b.node_id,b.inventory,p.manifest FROM {binding_relation} b
        JOIN {package_relation} p USING(package_id)
        WHERE b.importer_id='tiger' AND b.dataset_key='tiger' AND b.is_current
    """)
            )
        )
        .mappings()
        .all()
    )
    _require(len(selected) <= 1)
    if not selected:
        return None
    family = selected[0]["manifest"].get("adapter_metadata", {}).get("family", {})
    if family.get("publication_authority") != "captured-epoch":
        _require(all(relation["schema_name"] == "tiger" for relation in selected[0]["inventory"]["relations"]))
        return None
    return await _require_captured_selection(session, selected[0], family)


async def _require_captured_selection(session, selected, family):
    origin = validate_captured_origin(family["source_metadata"])
    await _scope(session, "tiger-captured-epoch:" + origin["epoch_id"])
    owner = await _protected_relation(session, _CAPTURE, catalog=True)
    epoch = (
        (
            await session.execute(
                text(f"SELECT * FROM {_CAPTURE} WHERE epoch_id=:id AND node_id=:node"),
                {"id": origin["epoch_id"], "node": selected["node_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(epoch is not None and epoch["state"] == "retained" and epoch["origin_kind"] == "captured")
    payload = {key: epoch[key] for key in ("node_id", "authority", "source_graph", "inventory")}
    payload["epoch_id"] = str(epoch["epoch_id"])
    _require(_digest(payload) == epoch["registration_sha256"])
    _require(_digest(epoch["source_graph"]) == origin["source_graph_sha256"])
    _require(epoch["authority"]["manifest"] == family and epoch["inventory"] == selected["inventory"])
    archive.validate_reference_family_manifest(family)
    return await _require_inventory(session, epoch["inventory"], owner["owner_oid"])


async def _require_inventory(session, inventory, owner_oid):
    _require(isinstance(inventory, dict) and set(inventory) == {"database_oid", "relations"})
    _require(
        inventory["database_oid"]
        == await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"))
    )
    relations = inventory["relations"]
    _require(len(relations) == 2 and {relation["relation_name"] for relation in relations} == {"zip_state", "zcta5"})
    _require(len({relation["schema_name"] for relation in relations}) == 1)
    for relation in relations:
        _require(_SCHEMA.fullmatch(relation["schema_name"]) is not None)
        actual = await _protected_relation(
            session, f'"{relation["schema_name"]}"."{relation["relation_name"]}"', expected_owner=owner_oid
        )
        _require(actual == relation)
    return inventory


async def _require_installed_selection(session, generation, owner_oid):
    from process import tiger_snapshot_inheritance as inherited

    _require(generation["state"] == "retained" and generation["origin_kind"] == "installed")
    scope_by_field = {"node_id": generation["node_id"], "importer_id": "tiger", "dataset_key": "tiger"}
    await _scope(session, "current:" + _digest(scope_by_field))
    _require(
        await session.scalar(
            text(
                f"SELECT generation_id FROM {_CURRENT} WHERE node_id=:node "
                "AND importer_id='tiger' AND dataset_key='tiger'"
            ),
            {"node": generation["node_id"]},
        )
        == generation["generation_id"]
    )
    await _protected_relation(session, _CUSTODY, expected_owner=owner_oid, catalog=True)
    custody = (
        (
            await session.execute(
                text(f"SELECT * FROM {_CUSTODY} WHERE generation_id=:id"), {"id": generation["generation_id"]}
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        custody is not None
        and custody["installation_id"] == generation["installation_id"]
        and custody["package_id"] == generation["package_id"]
    )
    receipt = custody["receipt"]
    _require(receipt["contract"] == inherited.CONTRACT and _digest(receipt) == custody["receipt_sha256"])
    pairs = sorted(
        [relation["relation_name"], relation["relation_oid"]] for relation in generation["inventory"]["relations"]
    )
    _require(receipt["relation_oids"] == pairs)
    await session.execute(text("LOCK TABLE ONLY tiger.zip_state, ONLY tiger.zcta5 IN ACCESS SHARE MODE NOWAIT"))
    _require(await inherited.current_tiger_snapshot_children(session) == tuple(map(tuple, pairs)))
    await _require_received_parents(session, receipt["parent_inventory"], owner_oid)


async def _require_received_parents(session, parents, owner_oid):
    from process import tiger_snapshot_inheritance as inherited

    for name, oid in parents["parent_oids"]:
        _require(name in inherited.TABLES)
        actual = await session.scalar(text("SELECT to_regclass(:name)::oid"), {"name": "tiger." + name})
        _require(actual == oid)
        _require(not await session.scalar(text(f'SELECT EXISTS(SELECT 1 FROM ONLY tiger."{name}" LIMIT 1)')))
    objects = await inherited._tiger_parent_object_inventory(session, parents["parents"], owner_oid)
    _require([dict(row) for row in objects] == parents["objects"])
    extension = await inherited._tiger_extension_inventory(session, parents["parents"])
    _require(_digest(extension) == parents["extension_sha256"])
    catalog = await session.scalar(
        text(inherited.PARENT_CATALOG_SQL), {"oids": [oid for _name, oid in parents["parent_oids"]]}
    )
    _require(_digest(catalog) == parents["catalog_sha256"])
