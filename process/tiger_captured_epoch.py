# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Flatten a completely writer-fenced TIGER graph without changing its parents."""

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


def validate_captured_origin(value):
    """A capture UUID identifies an epoch, never a historical serving generation."""
    if not isinstance(value, Mapping) or set(value) != {"contract", "epoch_id", "source_graph_sha256"}:
        raise archive.ReferenceFamilyArchiveError("captured TIGER origin is invalid")
    try:
        epoch_id = str(UUID(value["epoch_id"]))
    except ValueError, TypeError, AttributeError:
        raise archive.ReferenceFamilyArchiveError("captured TIGER epoch is invalid") from None
    if (
        value["contract"] != ORIGIN_CONTRACT
        or not isinstance(value["source_graph_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["source_graph_sha256"])
    ):
        raise archive.ReferenceFamilyArchiveError("captured TIGER origin is invalid")
    return {"contract": ORIGIN_CONTRACT, "epoch_id": epoch_id, "source_graph_sha256": value["source_graph_sha256"]}


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


async def prepare_captured_tiger_epoch(source_sessions, publisher_sessions, *, epoch_id, on_prepared):
    """Copy under source fences; the caller protects and records the clone before commit.

    Source fencing and publisher custody use separate credentials. The callback
    must transfer ownership and record the exact epoch in the clone transaction;
    it must not commit. No canonical relation, extension or inheritance is edited.
    """
    epoch_id = UUID(str(epoch_id))
    async with source_sessions() as source_session, source_session.begin():
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
            configure_isolation=False,
            source_capture_contract=archive.CAPTURED_TIGER_CONTRACT,
        )
        return await _clone_captured_model(
            publisher_sessions,
            capture,
            epoch_id,
            graph,
            on_prepared,
            source_url=source_session.get_bind().url,
        )


async def _clone_captured_model(publisher_sessions, capture, epoch_id, graph, on_prepared, *, source_url):
    """Seal the completed native model while the source transaction keeps its writer fence."""
    stage_schema = archive.reference_family_stage_schema(epoch_id)
    async with publisher_sessions() as session, session.begin():
        publisher_url = session.get_bind().url
        if any(getattr(source_url, field) != getattr(publisher_url, field) for field in ("host", "port", "database")):
            raise archive.ReferenceFamilyArchiveError("TIGER capture source and publisher databases differ")
        await archive._clone_source(session, capture, stage_schema)
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
            source_capture_contract=archive.CAPTURED_TIGER_CONTRACT,
        )
        prepared = archive.ReferenceFamilyPreparedSource(
            archive.validate_reference_family_manifest(manifest), ownership
        )
        await on_prepared(session, prepared, graph)
        return prepared
