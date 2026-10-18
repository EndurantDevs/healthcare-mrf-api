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
        ) SELECT c.oid::bigint AS relation_oid,n.nspname AS schema_name,c.relname AS table_name,
          c.relowner::bigint AS owner_oid,pg_relation_filenode(c.oid)::bigint AS relfilenode,
          c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,
          EXISTS(SELECT 1 FROM pg_catalog.pg_locks l WHERE l.pid=pg_backend_pid()
            AND l.locktype='relation' AND l.relation=c.oid AND l.granted
            AND l.mode IN ('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock')) AS writer_fenced,
          (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,
            a.attcollation,a.attisdropped,a.attgenerated,a.attidentity) ORDER BY a.attnum)
            FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0) AS attributes,
          ARRAY(SELECT i.inhparent::bigint FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid ORDER BY i.inhparent) AS parents,
          ARRAY(SELECT e.extname||':'||e.extversion FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e
            ON d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid
            WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0 AND d.deptype='e'
            ORDER BY e.extname) AS extensions
        FROM graph JOIN pg_catalog.pg_class c ON c.oid=graph.oid
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid
    """


async def read_locked_tiger_graph(session):
    """Read complete parent/child identity only after the recursive SHARE fence."""
    catalog_rows = (await session.execute(text(_GRAPH_SQL))).mappings().all()
    oids = {catalog_row["relation_oid"] for catalog_row in catalog_rows}
    roots = {
        catalog_row["table_name"]
        for catalog_row in catalog_rows
        if catalog_row["schema_name"] == "tiger" and not catalog_row["parents"]
    }
    if (
        not 2 <= len(catalog_rows) <= 4096
        or roots != {"zip_state", "zcta5"}
        or any(
            catalog_row["relkind"] != "r"
            or catalog_row["relpersistence"] != "p"
            or catalog_row["relispartition"]
            or catalog_row["relrowsecurity"]
            or catalog_row["relforcerowsecurity"]
            or not catalog_row["writer_fenced"]
            or not set(catalog_row["parents"]) <= oids
            for catalog_row in catalog_rows
        )
    ):
        raise archive.ReferenceFamilyArchiveError("TIGER source graph is not a closed persistent heap family")
    for root in (catalog_row for catalog_row in catalog_rows if not catalog_row["parents"]):
        if not any(extension.startswith("postgis_tiger_geocoder:") for extension in root["extensions"]):
            raise archive.ReferenceFamilyArchiveError("TIGER capture requires genuine geocoder roots")
    return {
        "contract": GRAPH_CONTRACT,
        "database_oid": await session.scalar(
            text("SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()")
        ),
        "relations": [dict(catalog_row) for catalog_row in catalog_rows],
    }


async def has_attested_inherited_tiger_source_capture(session, *, owner_oid):
    """Attest the existing source fence; protected or empty targets use target admission."""
    archive._require_transaction(session)
    async with archive._bounded_capture(session):
        await archive._lock_family(session, "tiger", archive.reference_family_spec("tiger").table_names, "SHARE")
        graph = await read_locked_tiger_graph(session)
        relations = graph["relations"]
        if len(relations) <= 2 or any(relation["owner_oid"] == owner_oid for relation in relations):
            return False
        roots = [relation["relation_oid"] for relation in relations if not relation["parents"]]
        capable = await session.scalar(
            text("""SELECT bool_and(has_table_privilege(current_user,oid,'SELECT')
                AND has_table_privilege(current_user,oid,'MAINTAIN')
                AND NOT has_table_privilege(current_user,oid,'INSERT,UPDATE,DELETE,TRUNCATE')
                AND NOT has_any_column_privilege(current_user,oid,'INSERT,UPDATE'))
                FROM unnest(CAST(:oids AS oid[])) AS roots(oid)"""),
            {"oids": roots},
        )
        if capable is not True:
            raise archive.ReferenceFamilyArchiveError("TIGER source capture fence privilege is unavailable")
        return True


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
        publisher_url = session.get_bind().url
        if any(getattr(source_url, field) != getattr(publisher_url, field) for field in ("host", "port", "database")):
            raise archive.ReferenceFamilyArchiveError("TIGER capture source and publisher databases differ")
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


_SNAPSHOT_GRAPH_SQL = _GRAPH_SQL.replace(
    "('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock')", "('AccessShareLock')"
).replace("AS writer_fenced", "AS read_pinned")


def _snapshot_graph(catalog_rows, *, pinned):
    """A finite native graph is distinct from the legacy writer-fenced graph."""
    if not 2 <= len(catalog_rows) <= 4096:
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph is unbounded")
    relations_by_oid = {catalog_row["relation_oid"]: dict(catalog_row) for catalog_row in catalog_rows}
    if len(relations_by_oid) != len(catalog_rows):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph repeats a relation")
    for catalog_row in catalog_rows:
        if (
            catalog_row["relkind"] != "r"
            or catalog_row["relpersistence"] != "p"
            or catalog_row["relispartition"]
            or catalog_row["relrowsecurity"]
            or catalog_row["relforcerowsecurity"]
            or not set(catalog_row["parents"]) <= relations_by_oid.keys()
            or any(
                type(catalog_row[key]) is not int or not 0 < catalog_row[key] < 2**32
                for key in ("relation_oid", "owner_oid", "relfilenode")
            )
            or (pinned and catalog_row["read_pinned"] is not True)
        ):
            raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph is not a closed read-pinned heap family")
        archive._quoted(catalog_row["schema_name"])
        archive._quoted(catalog_row["table_name"])
    roots_by_name = {
        catalog_row["table_name"]: catalog_row for catalog_row in catalog_rows if not catalog_row["parents"]
    }
    if set(roots_by_name) != {"zip_state", "zcta5"} or any(
        catalog_row["schema_name"] != "tiger"
        or not any(extension.startswith("postgis_tiger_geocoder:") for extension in catalog_row["extensions"])
        for catalog_row in roots_by_name.values()
    ):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot requires genuine geocoder roots_by_name")
    projection = _snapshot_projection(relations_by_oid, roots_by_name)
    return relations_by_oid, projection


def _snapshot_projection(relations_by_oid, roots_by_name):
    projection, all_oids = [], set()
    for table in ("zip_state", "zcta5"):
        root = roots_by_name[table]
        oids = {root["relation_oid"]}
        while True:
            expanded = oids | {
                oid for oid, catalog_row in relations_by_oid.items() if set(catalog_row["parents"]) & oids
            }
            if expanded == oids:
                break
            oids = expanded
        if oids & all_oids or any(relations_by_oid[oid]["attributes"] != root["attributes"] for oid in oids):
            raise archive.ReferenceFamilyArchiveError("TIGER snapshot has ambiguous model membership")
        all_oids.update(oids)
        projection.append(
            (
                table,
                tuple(
                    (
                        relations_by_oid[oid]["schema_name"],
                        relations_by_oid[oid]["table_name"],
                        oid,
                        relations_by_oid[oid]["relfilenode"],
                    )
                    for oid in sorted(oids)
                ),
            )
        )
    if all_oids != relations_by_oid.keys():
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
    relation_by_oid, projection = _snapshot_graph(catalog_rows, pinned=True)
    comparable = lambda rows: [{key: item for key, item in row.items() if key != "read_pinned"} for row in rows]
    if comparable(discovered) != comparable(catalog_rows):
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot graph changed before read locks")
    oids = sorted(relation_by_oid)
    capable = await _snapshot_read_capability(session, oids)
    if capable is not True:
        raise archive.ReferenceFamilyArchiveError("TIGER snapshot requires a read-only source principal")
    return dict(
        contract=SNAPSHOT_GRAPH_CONTRACT,
        database_oid=await session.scalar(text("SELECT oid::bigint FROM pg_database WHERE datname=current_database()")),
        relations=[dict(row) for row in catalog_rows],
        projection_oids={name: [physical[2] for physical in relations] for name, relations in projection},
    ), projection


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


async def _snapshot_read_capability(session, oids):
    return await session.scalar(
        text("""SELECT bool_and(
        has_schema_privilege(current_user,n.oid,'USAGE') AND has_table_privilege(current_user,c.oid,'SELECT')
        AND NOT pg_has_role(current_user,c.relowner,'USAGE')
        AND NOT has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE')
        AND NOT has_any_column_privilege(current_user,c.oid,'INSERT,UPDATE'))
        FROM unnest(CAST(:oids AS oid[])) roots(oid) JOIN pg_class c ON c.oid=roots.oid
        JOIN pg_namespace n ON n.oid=c.relnamespace"""),
        {"oids": oids},
    )


async def can_prepare_snapshot_tiger_epoch(session, *, owner_oid):
    """Capability preflight only; no held snapshot or protected epoch is admitted."""
    archive._require_transaction(session)
    async with archive._bounded_capture(session):
        rows = (await session.execute(text(_SNAPSHOT_GRAPH_SQL))).mappings().all()
        by_oid, _ = _snapshot_graph(rows, pinned=False)
        if len(rows) <= 2 or any(row["owner_oid"] == owner_oid for row in rows):
            return False
        if await _snapshot_read_capability(session, sorted(by_oid)) is not True:
            raise archive.ReferenceFamilyArchiveError("TIGER snapshot read capability is unavailable")
        return True
