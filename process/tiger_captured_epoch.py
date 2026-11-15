# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Flatten a completely writer-fenced TIGER graph without changing its parents."""

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import replace
from uuid import UUID

from sqlalchemy import text

from process import reference_family_archive as archive

ORIGIN_CONTRACT = "tiger.captured-origin.v1"
GRAPH_CONTRACT = "tiger.closed-inheritance-graph.v1"
SNAPSHOT_ORIGIN_CONTRACT = "tiger.captured-origin.v2"
SNAPSHOT_GRAPH_CONTRACT = "tiger.readonly-finite-graph.v2"


def validate_captured_origin(value):
    """A capture UUID identifies an epoch, never a historical serving generation."""
    if not isinstance(value, Mapping) or set(value) != {"contract", "epoch_id", "source_graph_sha256"}:
        raise archive.ReferenceFamilyArchiveError("captured TIGER origin is invalid")
    try:
        epoch_id = str(UUID(value["epoch_id"]))
    except ValueError, TypeError, AttributeError:
        raise archive.ReferenceFamilyArchiveError("captured TIGER epoch is invalid") from None
    if (
        value["contract"] not in {ORIGIN_CONTRACT, SNAPSHOT_ORIGIN_CONTRACT}
        or not isinstance(value["source_graph_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["source_graph_sha256"])
    ):
        raise archive.ReferenceFamilyArchiveError("captured TIGER origin is invalid")
    return {"contract": value["contract"], "epoch_id": epoch_id, "source_graph_sha256": value["source_graph_sha256"]}


_GRAPH_SQL = """
        WITH RECURSIVE graph(oid) AS (
          SELECT unnest(ARRAY['tiger.zip_state'::regclass::oid,'tiger.zcta5'::regclass::oid])
          UNION SELECT inheritance.inhrelid FROM pg_catalog.pg_inherits inheritance JOIN graph ON graph.oid=inheritance.inhparent
        ) SELECT n.nspname AS schema_name,c.relname AS table_name,
          c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,
          (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
            cn.nspname,co.collname,a.attisdropped,a.attgenerated,a.attidentity) ORDER BY a.attnum)
            FROM pg_catalog.pg_attribute a LEFT JOIN pg_collation co ON co.oid=a.attcollation
            LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace
            WHERE a.attrelid=c.oid AND a.attnum>0) AS attributes,
          COALESCE((SELECT jsonb_agg(jsonb_build_array(pn.nspname,p.relname) ORDER BY pn.nspname,p.relname)
            FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_namespace pn ON pn.oid=p.relnamespace
            WHERE i.inhrelid=c.oid),'[]'::jsonb) AS parents,
          ARRAY(SELECT e.extname FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e
            ON d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid
            WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0 AND d.deptype='e'
            ORDER BY e.extname) AS extensions
        FROM graph JOIN pg_catalog.pg_class c ON c.oid=graph.oid
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace ORDER BY n.nspname,c.relname
    """


async def read_locked_tiger_graph(session):
    """Read the complete named graph after the caller's recursive SHARE locks."""
    rows = (await session.execute(text(_GRAPH_SQL))).mappings().all()
    _snapshot_graph(rows, pinned=True)
    return {"contract": GRAPH_CONTRACT, "relations": [dict(row) for row in rows]}


async def has_attested_inherited_tiger_source_capture(session, *, owner_oid=None):
    """Use actual native locks and reads; no role or catalog identity is attested."""
    archive._require_transaction(session)
    async with archive._bounded_capture(session):
        await archive._lock_family(session, "tiger", archive.reference_family_spec("tiger").table_names, "SHARE")
        graph = await read_locked_tiger_graph(session)
        return len(graph["relations"]) > 2


async def prepare_captured_tiger_epoch(
    source_sessions,
    publisher_sessions,
    *,
    epoch_id,
    on_prepared,
    source_copy=None,
    on_precreated=None,
):
    """Copy under source fences; the caller protects and records the clone before commit.

    Source fencing and publisher custody use separate credentials. The callback
    must transfer ownership and record the exact epoch in the clone transaction;
    it must not commit. No canonical relation, extension or inheritance is edited.
    """
    epoch_id = UUID(str(epoch_id))
    archive._require_native_source_copy(source_copy, on_precreated)
    async with (
        asyncio.timeout(source_copy.timeout) as deadline,
        source_sessions() as source_session,
        source_session.begin(),
    ):
        await source_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        async with archive._bounded_capture(source_session):
            await archive._lock_family(
                source_session, "tiger", archive.reference_family_spec("tiger").table_names, "SHARE"
            )
            graph = await read_locked_tiger_graph(source_session)
        metadata_by_field = {
            "contract": ORIGIN_CONTRACT,
            "epoch_id": str(epoch_id),
            "source_graph_sha256": hashlib.sha256(
                json.dumps(graph, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        capture = await archive._capture_reference_family_source(
            source_session,
            importer_id="tiger",
            schema_name="tiger",
            source_metadata=metadata_by_field,
            source_capture_contract=archive.CAPTURED_TIGER_CONTRACT,
        )
        return await _clone_captured_model(
            publisher_sessions,
            capture,
            epoch_id,
            graph,
            on_prepared,
            source_url=source_session.get_bind().url,
            source_copy=source_copy,
            on_precreated=on_precreated,
            deadline=deadline.when(),
        )


async def _clone_captured_model(
    publisher_sessions,
    capture,
    epoch_id,
    graph,
    on_prepared,
    *,
    source_url,
    capture_contract=archive.CAPTURED_TIGER_CONTRACT,
    **source_options,
):
    """Seal the completed native model while the source transaction keeps its writer fence."""
    if set(source_options) - {"source_copy", "on_precreated", "deadline", "authorize_creation"}:
        raise archive.ReferenceFamilyArchiveError("TIGER source options are invalid")
    if capture_contract not in {archive.CAPTURED_TIGER_CONTRACT, archive.CAPTURED_TIGER_SNAPSHOT_CONTRACT}:
        raise archive.ReferenceFamilyArchiveError("TIGER capture contract is unsupported")
    source_copy = source_options.get("source_copy")
    stage_schema = archive.reference_family_stage_schema(epoch_id)
    async with publisher_sessions() as session, session.begin():
        await archive._clone_source(
            session,
            capture,
            stage_schema,
            source_copy=source_copy,
            on_precreated=source_options.get("on_precreated"),
            deadline=source_options.get("deadline"),
            **(
                {"authorize_creation": source_options["authorize_creation"]}
                if "authorize_creation" in source_options
                else {}
            ),
        )
        ownership = await archive.capture_reference_family_stage_ownership(
            session,
            importer_id="tiger",
            dataset_id=epoch_id,
        )
        manifest = await archive._family_manifest(
            session,
            spec=archive.reference_family_spec("tiger"),
            schema_name=stage_schema,
            source_metadata=capture.manifest.source_metadata,
        )
        manifest = replace(
            manifest,
            publication_authority="captured-epoch",
            source_serving_generation=None,
            source_capture_contract=capture_contract,
        )
        prepared = archive.ReferenceFamilyPreparedSource(
            archive.validate_reference_family_manifest(manifest), ownership
        )
        await on_prepared(session, prepared, graph)
        return prepared


_SNAPSHOT_GRAPH_SQL = _GRAPH_SQL


def _snapshot_graph(catalog_rows, *, pinned):
    """A finite named graph must be closed, reachable and have unambiguous model membership."""
    if not 2 <= len(catalog_rows) <= 4096:
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph is unbounded")
    by_name = {
        (relation_record["schema_name"], relation_record["table_name"]): dict(relation_record)
        for relation_record in catalog_rows
    }
    if len(by_name) != len(catalog_rows):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph repeats a relation")
    for relation_record in catalog_rows:
        parents = {tuple(pair) for pair in relation_record["parents"]}
        if (
            relation_record["relkind"] != "r"
            or relation_record["relpersistence"] != "p"
            or relation_record["relispartition"]
            or relation_record["relrowsecurity"]
            or relation_record["relforcerowsecurity"]
            or not parents <= by_name.keys()
        ):
            raise archive.ReferenceFamilyArchiveError("TIGER source graph is not a closed persistent heap family")
        archive._quoted(relation_record["schema_name"])
        archive._quoted(relation_record["table_name"])
    roots_by_table = {
        relation_record["table_name"]: relation_record
        for relation_record in catalog_rows
        if not relation_record["parents"]
    }
    if (
        len([relation_record for relation_record in catalog_rows if not relation_record["parents"]]) != 2
        or set(roots_by_table) != {"zip_state", "zcta5"}
        or any(
            relation_record["schema_name"] != "tiger" or "postgis_tiger_geocoder" not in relation_record["extensions"]
            for relation_record in roots_by_table.values()
        )
    ):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot requires genuine geocoder roots")
    return by_name, _snapshot_projection(by_name, roots_by_table)


def _snapshot_projection(by_name, roots):
    projection, all_names = [], set()
    for table in ("zip_state", "zcta5"):
        root = roots[table]
        names = {("tiger", table)}
        while True:
            expanded = names | {
                name for name, row in by_name.items() if {tuple(pair) for pair in row["parents"]} & names
            }
            if expanded == names:
                break
            names = expanded
        if names & all_names or any(by_name[name]["attributes"] != root["attributes"] for name in names):
            raise archive.ReferenceFamilyArchiveError("TIGER snapshot has ambiguous model membership")
        all_names.update(names)
        projection.append((table, tuple(sorted(names))))
    if all_names != by_name.keys():
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph contains unreachable relations")
    return tuple(projection)


async def _discover_snapshot_graph(source_sessions):
    async with source_sessions() as session, session.begin():
        async with archive._bounded_capture(session):
            catalog_rows = (await session.execute(text(_SNAPSHOT_GRAPH_SQL))).mappings().all()
        _snapshot_graph(catalog_rows, pinned=False)
        return [dict(row) for row in catalog_rows]


async def _pin_snapshot_graph(session, discovered):
    """Take no snapshot-bearing SELECT until every discovered object is AS-pinned."""
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
    await session.execute(text(f"SET LOCAL lock_timeout='{archive._LOCK_TIMEOUT}'"))
    await session.execute(text(f"SET LOCAL statement_timeout='{archive._CAPTURE_TIMEOUT}'"))
    physical = ", ".join(
        "ONLY " + archive._quoted(row["schema_name"]) + "." + archive._quoted(row["table_name"]) for row in discovered
    )
    await session.execute(text(f"LOCK TABLE {physical} IN ACCESS SHARE MODE NOWAIT"))
    catalog_rows = (await session.execute(text(_SNAPSHOT_GRAPH_SQL))).mappings().all()
    _by_name, projection = _snapshot_graph(catalog_rows, pinned=True)
    comparable = lambda rows: [{key: item for key, item in row.items() if key != "read_pinned"} for row in rows]
    if comparable(discovered) != comparable(catalog_rows):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph changed before read locks")
    return {
        "contract": SNAPSHOT_GRAPH_CONTRACT,
        "relations": [dict(row) for row in catalog_rows],
        "projection": {name: [list(pair) for pair in relations] for name, relations in projection},
    }, projection


async def prepare_snapshot_tiger_epoch(
    source_sessions,
    publisher_sessions,
    *,
    epoch_id,
    on_prepared,
    source_copy,
    on_precreated,
    authorize_creation=None,
):
    """Capture one finite READ ONLY snapshot without changing the canonical graph."""
    epoch_id = UUID(str(epoch_id))
    archive._require_native_source_copy(source_copy, on_precreated)
    async with asyncio.timeout(source_copy.timeout) as deadline:
        discovered = await _discover_snapshot_graph(source_sessions)
        async with source_sessions() as session, session.begin():
            graph, projection = await _pin_snapshot_graph(session, discovered)
            metadata = dict(
                contract=SNAPSHOT_ORIGIN_CONTRACT,
                epoch_id=str(epoch_id),
                source_graph_sha256=hashlib.sha256(
                    json.dumps(graph, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            )
            manifest = await archive._frozen_source_manifest(
                session, source_metadata=metadata, frozen_source_projection=projection
            )
            manifest = replace(
                manifest,
                publication_authority="captured-epoch",
                source_capture_contract=archive.CAPTURED_TIGER_SNAPSHOT_CONTRACT,
            )
            snapshot = (await session.execute(text("SELECT pg_export_snapshot()"))).scalar_one()
            if not isinstance(snapshot, str) or archive._SNAPSHOT.fullmatch(snapshot) is None:
                raise archive.ReferenceFamilyArchiveError("reference family source snapshot is invalid")
            capture = archive.ReferenceFamilySourceCapture(
                manifest, "tiger", snapshot, frozen_source_projection=projection
            )
            return await _clone_captured_model(
                publisher_sessions,
                capture,
                epoch_id,
                graph,
                on_prepared,
                source_url=session.get_bind().url,
                capture_contract=archive.CAPTURED_TIGER_SNAPSHOT_CONTRACT,
                source_copy=source_copy,
                on_precreated=on_precreated,
                authorize_creation=authorize_creation,
                deadline=deadline.when(),
            )


async def can_prepare_snapshot_tiger_epoch(session, *, owner_oid=None):
    """Validate bounded model shape; native SELECT and COPY enforce source access."""
    archive._require_transaction(session)
    async with archive._bounded_capture(session):
        rows = (await session.execute(text(_SNAPSHOT_GRAPH_SQL))).mappings().all()
        _snapshot_graph(rows, pinned=False)
        return len(rows) > 2
