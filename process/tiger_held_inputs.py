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
_CAPTURE_FAMILIES = ("npi", "mrf-address", "cms-doctors", "geo", "tiger")


async def selected_captured_inventories(session, *, node_id=None):
    """Hold complete sealed families using the existing exact source selection."""
    _catalog, selected = await _selected_captured_generations(session, node_id=node_id, families=_CAPTURE_FAMILIES)
    return {
        generation_row["importer_id"]: await _require_model_capture(session, generation_row)
        for generation_row in selected
    }


async def _selected_captured_generations(session, *, node_id, families):
    """Hold complete sealed model families selected by the existing source catalog pair."""
    relations = _source_selection_relations()
    if relations is None:
        return None, []
    archive._require_transaction(session)
    catalog = await _protected_relation(session, _GENERATION, catalog=True)
    binding, package = relations
    selected = (
        (
            await session.execute(
                text(f"""
        SELECT g.*,p.manifest AS selected_manifest,p.package_id AS selected_package_id
        FROM {binding} b JOIN {package} p USING(package_id)
        JOIN {_GENERATION} g ON g.generation_id::text=p.manifest#>>'{{adapter_metadata,dataset_id}}'
          AND g.node_id=b.node_id AND g.importer_id=b.importer_id AND g.dataset_key=b.dataset_key
        WHERE b.is_current AND b.importer_id=ANY(CAST(:families AS text[]))
          AND (CAST(:node_id AS text) IS NULL OR b.node_id=:node_id)
          AND g.origin_kind='captured'
          AND NOT EXISTS(SELECT 1 FROM {_CURRENT} c JOIN {_GENERATION} installed USING(generation_id)
            WHERE c.node_id=g.node_id AND c.importer_id=g.importer_id AND c.dataset_key=g.dataset_key
              AND installed.origin_kind='installed')
        ORDER BY g.generation_id
    """),
                {"families": list(families), "node_id": node_id},
            )
        )
        .mappings()
        .all()
    )
    _require(len({generation_row["importer_id"] for generation_row in selected}) == len(selected))
    return catalog, selected


async def _require_model_capture(session, generation):
    """Keep exact package/scope selection while admitting saved inventories by name."""
    capture = generation["source_capture"]
    _require(isinstance(capture, dict) and capture.get("contract") == "snapshot-captured-source.v1")
    authority, manifest = capture["source_authority"], generation["selected_manifest"]
    _require(
        _digest(manifest) == generation["selected_package_id"]
        and manifest["producer_node_id"] == generation["node_id"]
        and manifest["importer_id"] == manifest["dataset_key"] == generation["importer_id"] == generation["dataset_key"]
        and manifest["contract_version"] == authority["contract"]
        and manifest["adapter_metadata"]["family"] == authority["manifest"]
        and authority["ownership"]["dataset_id"]
        == str(generation["source_operation_id"])
        == str(generation["generation_id"])
        and generation["current_location_fence"] is None
        and generation["state"] == "retained"
    )
    inventory = _named_inventory(generation["inventory"])
    ownership = authority["ownership"]
    recorded = (
        sorted(
            relation_record.get("table_name", relation_record.get("relation_name"))
            for relation_record in ownership["relations"]
        )
        if "relations" in ownership
        else sorted(pair[0] for pair in ownership["relation_oids"])
    )
    _require(
        recorded == sorted(relation_record["relation_name"] for relation_record in inventory["relations"])
        and all(
            relation_record["schema_name"] == ownership["schema_name"] for relation_record in inventory["relations"]
        )
    )
    for relation_record in inventory["relations"]:
        await _protected_relation(
            session,
            archive._quoted(relation_record["schema_name"]) + "." + archive._quoted(relation_record["relation_name"]),
        )
    if generation["importer_id"] != "tiger":
        await _require_model_schema(session, generation["importer_id"], inventory, authority["manifest"])
    if generation["importer_id"] == "tiger":
        await _require_captured_selection(
            session, {"node_id": generation["node_id"], "inventory": inventory}, authority["manifest"]
        )
    return inventory


async def _captured_schema_identity(session, importer_id, relation):
    """Reuse normalized column/key/sequence semantics, resolving only the recorded name."""
    schema, name = relation["schema_name"], relation["relation_name"]
    address = await archive._relation_oid(session, schema, name)
    _require(address is not None)
    if importer_id == "npi":
        from process import npi_result_archive

        return await npi_result_archive._npi_schema_identity(session, address, schema, name)
    return await archive._family_schema_identity(session, importer_id, address, schema, name)


async def _require_model_schema(session, importer_id, inventory, manifest):
    tables_by_name = {table["table_name"]: table["schema_sha256"] for table in manifest["tables"]}
    _require(set(tables_by_name) == {row["relation_name"] for row in inventory["relations"]})
    for row in inventory["relations"]:
        _require(await _captured_schema_identity(session, importer_id, row) == tables_by_name[row["relation_name"]])


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


async def _protected_relation(session, qualified, *, catalog=False):
    """Read and lock the configured relation; PostgreSQL enforces access.

    Retention catalog tables carry the internal triggers of their existing foreign keys; user triggers still refuse.
    """
    await session.execute(text(f"LOCK TABLE ONLY {qualified} IN ACCESS SHARE MODE NOWAIT"))
    relation = (
        (
            await session.execute(
                text("""SELECT n.nspname AS schema_name,c.relname AS relation_name,
        c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
          AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
          AND NOT EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class=c.oid)
          AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=c.oid AND NOT (:catalog AND tgisinternal))
          AND (NOT :catalog OR NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)) AS valid
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass(:name)"""),
                {"name": qualified, "catalog": catalog},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(relation is not None and relation["valid"])
    await session.execute(text(f"SELECT * FROM ONLY {qualified} LIMIT 0"))
    return {key: relation[key] for key in ("schema_name", "relation_name")}


def _named_inventory(inventory):
    _require(
        isinstance(inventory, dict) and isinstance(inventory.get("relations"), list) and bool(inventory["relations"])
    )
    names = [(row["schema_name"], row["relation_name"]) for row in inventory["relations"]]
    _require(len(set(names)) == len(names))
    for schema, name in names:
        archive._quoted(schema)
        archive._quoted(name)
    return {"relations": [{"schema_name": schema, "relation_name": name} for schema, name in sorted(names)]}


async def _selected_tiger_observation(session, *, node_id=None, observe=False):
    """Select one actual local current installation or source-bound captured epoch.

    Ordinary package rows select a capture; only its protected catalog provides
    authority. No latest capture, root OID, or synthetic serving generation is used.
    """
    archive._require_transaction(session)
    installed, inventory = await _installed_tiger_inventory(session, node_id)
    if installed:
        return None if inventory is None else {"selection_kind": "installed", "inventory": inventory}
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
        SELECT b.node_id,b.inventory,p.manifest,p.package_id AS selected_package_id FROM {binding_relation} b
        JOIN {package_relation} p USING(package_id)
        WHERE b.importer_id='tiger' AND b.dataset_key='tiger' AND b.is_current
          AND (CAST(:node_id AS text) IS NULL OR b.node_id=:node_id)
    """),
                {"node_id": node_id},
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
    if observe:
        return await _require_captured_selection(session, selected[0], family, observe=True)
    return {"inventory": await _require_captured_selection(session, selected[0], family)}


async def _require_captured_selection(session, selected, family, *, observe=False):
    origin = validate_captured_origin(family["source_metadata"])
    await _scope(session, "tiger-captured-epoch:" + origin["epoch_id"])
    await _protected_relation(session, _CAPTURE, catalog=True)
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
    _require(
        epoch["authority"]["manifest"] == family
        and _named_inventory(epoch["inventory"]) == _named_inventory(selected["inventory"])
    )
    archive.validate_reference_family_manifest(family)
    inventory = await _require_inventory(session, epoch["inventory"])
    await _require_model_schema(session, "tiger", inventory, family)
    if observe:
        _require(_digest(selected["manifest"]) == selected["selected_package_id"])
        return _captured_tiger_proof(
            "captured-epoch",
            selected["node_id"],
            origin["epoch_id"],
            selected["selected_package_id"],
            epoch["registration_sha256"],
            inventory,
        )
    return inventory


async def _require_inventory(session, inventory):
    inventory = _named_inventory(inventory)
    relations = inventory["relations"]
    _require(
        len(relations) == 2
        and {row["relation_name"] for row in relations} == {"zip_state", "zcta5"}
        and len({row["schema_name"] for row in relations}) == 1
    )
    for row in relations:
        _require(_SCHEMA.fullmatch(row["schema_name"]) is not None)
        _require(
            await _protected_relation(
                session, archive._quoted(row["schema_name"]) + "." + archive._quoted(row["relation_name"])
            )
            == row
        )
    return inventory


async def _require_installed_selection(session, generation):
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
    await _protected_relation(session, _CUSTODY, catalog=True)
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
    _require(receipt["contract"] == inherited.CONTRACT)
    names = inherited.receipt_relations(receipt, generation["inventory"])
    await _require_received_parents(session, receipt["parent_inventory"])
    _require(await inherited.current_tiger_snapshot_children(session) == names)


async def _require_received_parents(session, parents):
    from process import tiger_snapshot_inheritance as inherited

    inherited.parent_names(parents)
    await inherited.require_tiger_snapshot_parents(session)


async def selected_tiger_inventory(session, *, node_id=None):
    """Preserve ordinary installed/captured inventory selection and precedence."""
    observed = await _selected_tiger_observation(session, node_id=node_id)
    return None if observed is None else observed["inventory"]


def _captured_tiger_proof(kind, node_id, source_id, package_id, registration_sha256, inventory):
    inventory = _named_inventory(inventory)
    proof_by_field = dict(
        version=2,
        node_id=node_id,
        selection_kind=kind,
        source_id=str(source_id),
        package_id=package_id,
        registration_sha256=registration_sha256,
        inventory=inventory,
    )
    proof_by_field["selection_sha256"] = _digest(proof_by_field)
    return proof_by_field


async def selected_tiger_source_observation(session, *, node_id, owner_oid=None):
    """Expose only the authenticated Tiger selection, never a latest-epoch guess."""
    archive._require_transaction(session)
    _require(type(node_id) is str and node_id)
    _catalog, selected = await _selected_captured_generations(session, node_id=node_id, families=("tiger",))
    if selected:
        _require(len(selected) == 1 and selected[0]["node_id"] == node_id)
        generation = selected[0]
        inventory = await _require_model_capture(session, generation)
        return _captured_tiger_proof(
            "captured-source",
            node_id,
            generation["generation_id"],
            generation["selected_package_id"],
            generation["registration_sha256"],
            inventory,
        )
    observed = await _selected_tiger_observation(session, node_id=node_id, observe=True)
    if observed is None or observed.get("selection_kind") == "installed":
        return None
    _require(observed["node_id"] == node_id)
    await _protected_relation(session, _CAPTURE, catalog=True)
    return observed


async def _installed_tiger_inventory(session, node_id):
    if await session.scalar(text("SELECT to_regclass(:name)"), {"name": _CURRENT}) is not None:
        await _protected_relation(session, _CURRENT, catalog=True)
        await _protected_relation(session, _GENERATION, catalog=True)
        generation_rows = (
            (
                await session.execute(
                    text(f"""
            SELECT g.* FROM {_CURRENT} c JOIN {_GENERATION} g USING(generation_id)
            WHERE c.importer_id='tiger' AND c.dataset_key='tiger'
              AND (CAST(:node_id AS text) IS NULL OR c.node_id=:node_id)
        """),
                    {"node_id": node_id},
                )
            )
            .mappings()
            .all()
        )
        _require(len(generation_rows) <= 1)
        if generation_rows:
            generation = generation_rows[0]
            if all(relation["schema_name"] == "tiger" for relation in generation["inventory"]["relations"]):
                return True, None
            await _require_installed_selection(session, generation)
            return True, await _require_inventory(session, generation["inventory"])
    return False, None
