# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Attempt-fenced completed PLACES stages awaiting protected publication."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re

from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import suppress_control_run_heartbeat_persistence
from process.reference_family_result_generation import _schema_name

HANDOFF_FORMAT = "places-stage-handoff-v1"
HANDOFF_PHASE = "places stages awaiting publication"
MAX_HANDOFF_BYTES = 16_384
logger = logging.getLogger(__name__)


def is_protected_places_publication_enabled() -> bool:
    """Enable the coordinated publisher only through explicit deployment configuration."""
    return os.getenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", "false").strip().lower() == "true"


def _handoff_json(evidence) -> str:
    """Encode bounded evidence with the same deterministic digest on either side."""
    encoded = json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    if len(encoded.encode()) > MAX_HANDOFF_BYTES:
        raise RuntimeError("PLACES stage handoff exceeds its envelope")
    return encoded


def _attempt_parameters(ctx, schema, table_name, row_count):
    """Require the wrapper's attempt identity and its one fixed staging relation."""
    context = ctx["context"]
    run_id = ctx.get("control_run_id")
    attempt_id = context.get("_control_attempt_id")
    started_at = context.get("_control_attempt_started_at")
    if (
        not isinstance(run_id, str)
        or not 0 < len(run_id) <= 64
        or not isinstance(attempt_id, str)
        or not re.fullmatch(re.escape(run_id) + r":[0-9a-f]{32}", attempt_id)
        or not isinstance(started_at, str)
        or not 0 < len(started_at) <= 64
        or context.get("test_mode")
        or type(row_count) is not int
        or not 0 < row_count <= 50_000_000
    ):
        raise RuntimeError("PLACES stage handoff requires a complete controlled attempt")
    suffix = hashlib.sha256(attempt_id.encode()).hexdigest()[:20]
    if table_name != f"pricing_places_zcta_{suffix}":
        raise RuntimeError("PLACES stage does not belong to this attempt")
    return {
        "run_id": run_id,
        "attempt_id": attempt_id,
        "attempt_started_at": started_at,
        "schema": _schema_name(schema),
        "table": table_name,
        "row_count": row_count,
    }


async def _read_handoff(database, parameters):
    """Read back only this exact durable attempt after an ambiguous commit."""
    return await database.scalar(
        f"SELECT metrics->'places_handoff' FROM \"{parameters['schema']}\".import_run "
        "WHERE run_id=:run_id AND importer='places-zcta' "
        "AND status IN ('finalizing','succeeded') AND error IS NULL "
        "AND metrics->'places_handoff'->>'attempt_id'=:attempt_id "
        "AND metrics->'places_handoff'->>'attempt_started_at'=:attempt_started_at",
        **parameters,
    )


async def _capture_handoff(database, ctx, parameters):
    """Bind catalog identities and census while the run and stage are locked."""
    schema, table = parameters["schema"], parameters["table"]
    native_run = await database.first(
        f'SELECT run_id FROM "{schema}".import_run WHERE run_id=:run_id '
        "AND importer='places-zcta' AND status='running' AND finished_at IS NULL "
        "AND (error IS NULL OR error='null'::jsonb) "
        "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
        "AND metrics->'places_handoff' IS NULL "
        "AND (metrics IS NULL OR metrics='null'::jsonb OR jsonb_typeof(metrics)='object') "
        "AND COALESCE(params->>'test_mode','false')='false' AND COALESCE(params->>'test','false')='false' "
        "FOR UPDATE",
        **parameters,
    )
    if native_run is None:
        raise RuntimeError("PLACES attempt changed before stage handoff")
    await raise_if_cancelled(ctx, {"run_id": parameters["run_id"]})
    await database.status(f'LOCK TABLE "{schema}"."{table}" IN ACCESS EXCLUSIVE MODE')
    stage_rows = await database.scalar(f'SELECT count(*) FROM "{schema}"."{table}"')
    if stage_rows != parameters["row_count"]:
        raise RuntimeError("PLACES stage census changed before handoff")
    identities = await database.first(
        "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()), "
        "to_regclass(:history)::oid::bigint, to_regclass(:relation)::oid::bigint",
        history=f"{schema}.import_run",
        relation=f"{schema}.{table}",
    )
    if identities[2] != ctx["context"].get("_places_stage_oid"):
        raise RuntimeError("PLACES stage identity changed before handoff")
    indexes = await database.scalar(
        "SELECT jsonb_agg(jsonb_build_object('oid',indexrelid::bigint,'definition',pg_get_indexdef(indexrelid)) "
        "ORDER BY indexrelid) FROM pg_index WHERE indrelid=:oid AND indisvalid AND indisready",
        oid=identities[2],
    )
    if not indexes:
        raise RuntimeError("PLACES stage indexes are unavailable")
    handoff_by_field = {
        "format": HANDOFF_FORMAT,
        **parameters,
        "database_oid": identities[0],
        "import_run_oid": identities[1],
        "stage_oid": identities[2],
        "incumbent_oid": ctx["context"]["_places_incumbent_oid"],
        "indexes": indexes,
        "complete": True,
        "published": False,
        "audit": {key: ctx["context"]["audit"][key] for key in ("latest_year", "processed_rows", "accepted_rows")},
    }
    handoff_by_field["handoff_sha256"] = hashlib.sha256(_handoff_json(handoff_by_field).encode()).hexdigest()
    return handoff_by_field


async def _write_handoff(database, parameters, handoff):
    """Commit the stage marker and finalizing transition as one durable handoff."""
    schema, table = parameters["schema"], parameters["table"]
    marker_by_field = {key: handoff[key] for key in ("format", "run_id", "attempt_id", "handoff_sha256")}
    quoted_marker = _handoff_json(marker_by_field).replace("'", "''")
    await database.status(f'COMMENT ON TABLE "{schema}"."{table}" IS \'{quoted_marker}\'')
    changed = await database.scalar(
        f"UPDATE \"{schema}\".import_run SET status='finalizing',phase_detail=:phase, "
        "metrics=COALESCE(NULLIF(metrics,'null'::jsonb),'{}'::jsonb) || jsonb_build_object('places_handoff',CAST(:handoff AS jsonb)), "
        "progress=COALESCE(progress,'{}'::jsonb) || jsonb_build_object('phase',CAST(:phase AS text)), "
        "heartbeat_at=clock_timestamp(),finished_at=NULL,error=NULL "
        "WHERE run_id=:run_id AND importer='places-zcta' AND status='running' "
        "AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at "
        "AND metrics->'places_handoff' IS NULL AND (error IS NULL OR error='null'::jsonb) "
        "AND finished_at IS NULL RETURNING run_id",
        **parameters,
        phase=HANDOFF_PHASE,
        handoff=_handoff_json(handoff),
    )
    if changed != parameters["run_id"]:
        raise RuntimeError("PLACES attempt changed during stage handoff")


async def handoff_places_stage(database, ctx, *, schema, table_name, row_count):
    """Transfer publication responsibility without reporting import success."""
    parameters = _attempt_parameters(ctx, schema, table_name, row_count)
    handoff = None
    async with suppress_control_run_heartbeat_persistence(parameters["run_id"]):
        try:
            async with database.transaction():
                handoff = await _capture_handoff(database, ctx, parameters)
                await _write_handoff(database, parameters, handoff)
        except Exception, asyncio.CancelledError:
            if handoff is None or await _read_handoff(database, parameters) != handoff:
                raise
        ctx["context"]["control_run_handoff_committed"] = True
        ctx["context"]["_control_committed_result"] = {"places_handoff": handoff}
        ctx["context"]["run"] = 0
    return ctx["context"]["_control_committed_result"]


async def _has_dropped_unpublished_stage(database, ctx, schema, table, stage_oid):
    """Drop only the same unmarked OID after serializing with a possible handoff."""
    async with database.transaction():
        await database.status("SET LOCAL lock_timeout = '1s'")
        native_run = await database.first(
            f"SELECT metrics->'places_handoff' FROM \"{schema}\".import_run "
            "WHERE run_id=:run_id AND importer='places-zcta' FOR UPDATE",
            run_id=ctx["control_run_id"],
        )
        if native_run is None or native_run[0] is not None:
            return False
        relation = f"{schema}.{table}"
        if await database.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=relation) != stage_oid:
            return False
        await database.status(f'LOCK TABLE "{schema}"."{table}" IN ACCESS EXCLUSIVE MODE')
        is_unpublished = await database.scalar(
            "SELECT to_regclass(:relation)::oid::bigint=:stage_oid "
            "AND to_regclass(:canonical)::oid::bigint IS DISTINCT FROM :stage_oid "
            "AND obj_description(CAST(:stage_oid AS oid),'pg_class') IS NULL",
            relation=relation,
            canonical=f"{schema}.pricing_places_zcta",
            stage_oid=stage_oid,
        )
        if not is_unpublished:
            return False
        if await database.scalar(
            f'SELECT EXISTS (SELECT 1 FROM "{schema}".reference_family_result_generation '
            "WHERE :stage_oid = ANY(relation_oids))",
            stage_oid=stage_oid,
        ):
            return False
        await database.status(f'DROP TABLE "{schema}"."{table}"')
    return True


async def cleanup_places_attempt(database, ctx):
    """Best-effort cleanup cannot remove a handed-off, rebound, or unknown stage."""
    context = ctx.get("context") or {}
    stage_oid = context.get("_places_stage_oid")
    if context.get("control_run_handoff_committed") or type(stage_oid) is not int:
        return False
    # ponytail: crashes before OID capture need an exact orphan janitor, never a broad name scan.
    try:
        attempt_id = context["_control_attempt_id"]
        table = "pricing_places_zcta_" + hashlib.sha256(attempt_id.encode()).hexdigest()[:20]
        if table != context.get("_places_stage_table"):
            return False
        return await _has_dropped_unpublished_stage(
            database, ctx, _schema_name(context["_places_stage_schema"]), table, stage_oid
        )
    except Exception, asyncio.CancelledError:
        logger.warning("PLACES attempt stage cleanup deferred", exc_info=True)
        return False
