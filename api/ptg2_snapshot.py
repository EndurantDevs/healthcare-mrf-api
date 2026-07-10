# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Current PTG2 snapshot resolution and index loading."""

from __future__ import annotations

import logging
import os
import time

from sqlalchemy import text

from api.ptg2_index_cache import (_PTG2_INDEX_CACHE,
                                  PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX,
                                  PTG2_INDEX_CACHE_TTL_SECONDS, _artifact_root,
                                  _path_from_uri, load_ptg2_index_from_path)
from api.ptg2_serving_utils import ein_plan_id_variants
from api.ptg2_types import PTG2ServingIndex

PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
logger = logging.getLogger(__name__)
_PTG2_SNAPSHOT_RESOLVE_CACHE: dict[tuple[object, ...], tuple[float, str | None]] = {}


def _snapshot_cache_enabled(session) -> bool:
    return hasattr(session, "sync_session")


def _snapshot_cache_get(key: tuple[object, ...]) -> str | None | object:
    cached = _PTG2_SNAPSHOT_RESOLVE_CACHE.get(key)
    if cached is None:
        return None
    cached_at, value = cached
    if PTG2_INDEX_CACHE_TTL_SECONDS == 0 or (time.monotonic() - cached_at) <= PTG2_INDEX_CACHE_TTL_SECONDS:
        return value
    _PTG2_SNAPSHOT_RESOLVE_CACHE.pop(key, None)
    return None


def _snapshot_cache_set(key: tuple[object, ...], value: str | None) -> str | None:
    _PTG2_SNAPSHOT_RESOLVE_CACHE[key] = (time.monotonic(), value)
    return value


async def current_snapshot_id(session, requested_snapshot_id: str | None = None) -> str | None:
    """Return the requested published snapshot or the current published snapshot."""
    if requested_snapshot_id:
        snapshot_result = await session.execute(
            text(
                f"""
                SELECT snapshot_id
                  FROM {PTG2_SCHEMA}.ptg2_snapshot
                 WHERE snapshot_id = :snapshot_id
                   AND status = 'published'
                 LIMIT 1
                """
            ),
            {"snapshot_id": str(requested_snapshot_id)},
        )
        snapshot_value = snapshot_result.scalar()
        return str(snapshot_value) if snapshot_value else None
    cache_key = ("current",)
    if _snapshot_cache_enabled(session):
        cached = _snapshot_cache_get(cache_key)
        if cached is not None:
            return cached
    snapshot_result = await session.execute(
        text(
            f"""
            SELECT pointer.snapshot_id
              FROM {PTG2_SCHEMA}.ptg2_current_snapshot pointer
              JOIN {PTG2_SCHEMA}.ptg2_snapshot published_snapshot
                ON published_snapshot.snapshot_id = pointer.snapshot_id
             WHERE pointer.slot = 'current'
               AND published_snapshot.status = 'published'
            """
        )
    )
    snapshot_value = snapshot_result.scalar()
    snapshot_value = str(snapshot_value) if snapshot_value else None
    return _snapshot_cache_set(cache_key, snapshot_value) if _snapshot_cache_enabled(session) else snapshot_value


async def current_source_snapshot_id(session, source_key: str) -> str | None:
    """Return the published serving snapshot currently selected for one source."""
    normalized_source_key = str(source_key or "").strip().lower()
    if not normalized_source_key:
        return None
    result = await session.execute(
        text(
            f"""
            SELECT pointer.snapshot_id
              FROM {PTG2_SCHEMA}.ptg2_current_source_snapshot pointer
              JOIN {PTG2_SCHEMA}.ptg2_snapshot published_snapshot
                ON published_snapshot.snapshot_id = pointer.snapshot_id
             WHERE pointer.source_key = :source_key
               AND published_snapshot.status = 'published'
               AND published_snapshot.manifest->'serving_index'->>'table' IS NOT NULL
             LIMIT 1
            """
        ),
        {"source_key": normalized_source_key},
    )
    value = result.scalar()
    return str(value) if value else None


async def current_source_snapshot_id_for_plan(session, args: dict[str, object]) -> str | None:
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return None
    plan_variants = ein_plan_id_variants(requested_plan)
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    # Plan-source pointers are updated by importer worker processes while API
    # servers keep their own memory. Do not cache this lookup in-process, or a
    # just-published network can stay invisible until the API pod's TTL expires.
    params: dict[str, object] = {"plan_ids": plan_variants}
    market_sql = ""
    if market_type:
        params["plan_market_type"] = market_type
        market_sql = "AND cps.plan_market_type = :plan_market_type"
    source_sql = ""
    if source_key:
        params["source_key"] = source_key
        source_sql = "AND cps.source_key = :source_key"
    try:
        result = await session.execute(
            text(
                f"""
                 SELECT cps.snapshot_id
                  FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
                  JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
                 WHERE cps.plan_id = ANY(CAST(:plan_ids AS text[]))
                   {market_sql}
                   {source_sql}
                   AND s.status = 'published'
                   AND s.manifest->'serving_index'->>'table' IS NOT NULL
                 -- Prefer a snapshot whose serving table is actually materialized
                 -- (to_regclass), not merely claimed by the manifest, so a plan with
                 -- multiple networks resolves to the loaded one instead of an
                 -- unloaded newer network (which yields snapshot_not_loaded / 0
                 -- results). Falls back to recency when none/all are loaded.
                 ORDER BY (to_regclass(s.manifest->'serving_index'->>'table') IS NOT NULL) DESC,
                          cps.import_month DESC NULLS LAST, cps.updated_at DESC NULLS LAST
                 LIMIT 1
                """
            ),
            params,
        )
    except Exception:
        rollback = getattr(session, "rollback", None)
        if callable(rollback):
            try:
                await rollback()
            except Exception as rollback_exc:
                logger.debug("failed to rollback source snapshot lookup: %s", rollback_exc)
        return None
    value = result.scalar()
    value = str(value) if value else None
    return value


async def current_source_snapshot_ids_for_plan(
    session, args: dict[str, object]
) -> list[tuple[str, str]]:
    """Resolve the newest *materialized* serving snapshot for EACH network of a plan.

    A group plan can be served by several networks/sources at once (e.g. a dental
    C2 network plus a PPO carrier feed). ``current_source_snapshot_id_for_plan``
    collapses those to a single snapshot via ``LIMIT 1``, which silently drops
    every other network's pricing -- so a procedure only priced in one network
    returns 0 results whenever a different network happens to own the newest
    snapshot. This plural variant returns one ``(source_key, snapshot_id)`` pair
    per network -- the newest snapshot whose serving table is actually
    materialized (mirroring the single-plan resolver's ``to_regclass`` tiebreaker
    so an unloaded newer network does not win and contribute 0 rows) -- letting
    callers fan out across all of a plan's networks and combine the results.
    """
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return []
    plan_variants = ein_plan_id_variants(requested_plan)
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    # This list changes whenever a source/network publish completes. API pods
    # cannot see cache invalidation from importer workers, so resolve it live.
    params: dict[str, object] = {"plan_ids": plan_variants}
    market_sql = ""
    if market_type:
        params["plan_market_type"] = market_type
        market_sql = "AND cps.plan_market_type = :plan_market_type"
    source_sql = ""
    if source_key:
        params["source_key"] = source_key
        source_sql = "AND cps.source_key = :source_key"
    try:
        result = await session.execute(
            text(
                f"""
                 SELECT DISTINCT ON (cps.source_key) cps.source_key, cps.snapshot_id
                  FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
                  JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
                 WHERE cps.plan_id = ANY(CAST(:plan_ids AS text[]))
                   {market_sql}
                   {source_sql}
                   AND s.status = 'published'
                   AND s.manifest->'serving_index'->>'table' IS NOT NULL
                 -- One snapshot per network (source_key): the newest whose serving
                 -- table is actually materialized. DISTINCT ON requires source_key
                 -- to lead the ORDER BY; the to_regclass check keeps an unloaded
                 -- newer network from winning and returning snapshot_not_loaded.
                 ORDER BY cps.source_key,
                          (to_regclass(s.manifest->'serving_index'->>'table') IS NOT NULL) DESC,
                          cps.import_month DESC NULLS LAST, cps.updated_at DESC NULLS LAST
                """
            ),
            params,
        )
    except Exception:
        rollback = getattr(session, "rollback", None)
        if callable(rollback):
            try:
                await rollback()
            except Exception as rollback_exc:
                logger.debug("failed to rollback source snapshot lookup: %s", rollback_exc)
        return []
    pairs = [
        (str(row[0] or ""), str(row[1]))
        for row in result
        if row[1]
    ]
    return pairs


async def resolve_current_ptg2_snapshot_id(session, args: dict[str, object]) -> str | None:
    if args.get("snapshot_id"):
        # Serving callers immediately load snapshot_serving_tables(), whose
        # query is published-only. Avoid issuing the same status check twice.
        return str(args["snapshot_id"])
    if args.get("plan_id") or args.get("plan_external_id"):
        return await current_source_snapshot_id_for_plan(session, args)
    if args.get("source_key"):
        return await current_source_snapshot_id(session, str(args["source_key"]))
    return await current_snapshot_id(session)


async def snapshot_artifact_uri(session, snapshot_id: str) -> str | None:
    result = await session.execute(
        text(
            f"""
            SELECT storage_uri
              FROM {PTG2_SCHEMA}.ptg2_artifact_manifest
             WHERE snapshot_id = :snapshot_id
               AND artifact_kind = :artifact_kind
             ORDER BY created_at DESC NULLS LAST
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id, "artifact_kind": PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX},
    )
    value = result.scalar()
    return str(value) if value else None


async def load_current_ptg2_index(session, requested_snapshot_id: str | None = None) -> PTG2ServingIndex | None:
    snapshot_id = await current_snapshot_id(session, requested_snapshot_id=requested_snapshot_id)
    if not snapshot_id:
        return None
    cached = _PTG2_INDEX_CACHE.get(snapshot_id)
    if cached is not None:
        cached_at, cached_index = cached
        if PTG2_INDEX_CACHE_TTL_SECONDS == 0 or (time.monotonic() - cached_at) <= PTG2_INDEX_CACHE_TTL_SECONDS:
            return cached_index
        _PTG2_INDEX_CACHE.pop(snapshot_id, None)

    storage_uri = await snapshot_artifact_uri(session, snapshot_id)
    candidate_paths = []
    if storage_uri:
        candidate_paths.append(_path_from_uri(storage_uri))
    candidate_paths.append(_artifact_root() / PTG2_ARTIFACT_KIND_SNAPSHOT_INDEX / f"{snapshot_id}.json")

    for path in candidate_paths:
        if path.exists():
            index = load_ptg2_index_from_path(path)
            _PTG2_INDEX_CACHE[snapshot_id] = (time.monotonic(), index)
            return index
    return None
