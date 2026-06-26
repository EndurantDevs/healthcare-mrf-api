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
    if requested_snapshot_id:
        return str(requested_snapshot_id)
    cache_key = ("current",)
    if _snapshot_cache_enabled(session):
        cached = _snapshot_cache_get(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]
    result = await session.execute(
        text(f"SELECT snapshot_id FROM {PTG2_SCHEMA}.ptg2_current_snapshot WHERE slot = 'current'")
    )
    value = result.scalar()
    value = str(value) if value else None
    return _snapshot_cache_set(cache_key, value) if _snapshot_cache_enabled(session) else value


async def current_source_snapshot_id_for_plan(session, args: dict[str, object]) -> str | None:
    requested_plan = str(args.get("plan_id") or args.get("plan_external_id") or "").strip()
    if not requested_plan:
        return None
    market_type = str(args.get("plan_market_type") or "").strip().lower()
    source_key = str(args.get("source_key") or "").strip().lower()
    cache_key = ("source_plan", requested_plan, market_type, source_key)
    if _snapshot_cache_enabled(session):
        cached = _snapshot_cache_get(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]
    params: dict[str, object] = {"plan_id": requested_plan}
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
                 WHERE cps.plan_id = :plan_id
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
        return _snapshot_cache_set(cache_key, None) if _snapshot_cache_enabled(session) else None
    value = result.scalar()
    value = str(value) if value else None
    return _snapshot_cache_set(cache_key, value) if _snapshot_cache_enabled(session) else value


async def resolve_current_ptg2_snapshot_id(session, args: dict[str, object]) -> str | None:
    if args.get("snapshot_id"):
        return str(args["snapshot_id"])
    source_snapshot_id = await current_source_snapshot_id_for_plan(session, args)
    if source_snapshot_id:
        return source_snapshot_id
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
