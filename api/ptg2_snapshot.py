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


def _serving_relation_name_sql(snapshot_alias: str) -> str:
    """Prefer the retained binary relation when testing snapshot availability."""
    serving_index = f"{snapshot_alias}.manifest->'serving_index'"
    return (
        f"COALESCE(NULLIF({serving_index}->>'serving_binary_table', ''), "
        f"NULLIF({serving_index}->>'table', ''))"
    )


def _serving_relation_available_sql(snapshot_alias: str) -> str:
    """Return SQL that rejects manifests whose retained serving relation is gone."""
    return f"to_regclass({_serving_relation_name_sql(snapshot_alias)}) IS NOT NULL"


def _logical_network_key_sql(pointer_alias: str, snapshot_alias: str) -> str:
    """Group dated source files that publish the same logical plan network."""
    network_names = f"{snapshot_alias}.manifest->'serving_index'->>'network_names'"
    return (
        f"COALESCE(NULLIF(NULLIF({network_names}, ''), '[]'), "
        f"{pointer_alias}.source_key)"
    )


def _source_effective_month_sql(pointer_alias: str, snapshot_alias: str) -> str:
    """Prefer the month encoded in a source URL over the orchestration month."""
    source_url = f"{snapshot_alias}.manifest->'successful_files'->0->>'url'"
    source_month = f"substring({source_url} FROM '(20[0-9]{{2}}-(?:0[1-9]|1[0-2]))')"
    return f"COALESCE(({source_month} || '-01')::date, {pointer_alias}.import_month)"


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
               AND {_serving_relation_available_sql('published_snapshot')}
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
    effective_month_sql = _source_effective_month_sql("cps", "s")
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
                   AND {_serving_relation_available_sql('s')}
                 ORDER BY {effective_month_sql} DESC NULLS LAST,
                          cps.import_month DESC NULLS LAST,
                          cps.updated_at DESC NULLS LAST
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
    """Resolve the newest materialized snapshot for each logical plan network.

    Dated files receive different source keys, so source key alone cannot identify
    a network across months. Manifest network names define that stable grouping;
    the source URL month selects its newest retained snapshot. Sources without
    either metadata retain the source-key and orchestration-month fallback.
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
    logical_network_sql = _logical_network_key_sql("cps", "s")
    effective_month_sql = _source_effective_month_sql("cps", "s")
    try:
        result = await session.execute(
            text(
                f"""
                 WITH candidate_snapshots AS (
                     SELECT cps.source_key,
                            cps.snapshot_id,
                            {logical_network_sql} AS logical_network_key,
                            {effective_month_sql} AS source_effective_month,
                            cps.import_month,
                            cps.updated_at
                       FROM {PTG2_SCHEMA}.ptg2_current_plan_source cps
                       JOIN {PTG2_SCHEMA}.ptg2_snapshot s ON s.snapshot_id = cps.snapshot_id
                      WHERE cps.plan_id = ANY(CAST(:plan_ids AS text[]))
                        {market_sql}
                        {source_sql}
                        AND s.status = 'published'
                        AND {_serving_relation_available_sql('s')}
                 )
                 SELECT DISTINCT ON (logical_network_key) source_key, snapshot_id
                   FROM candidate_snapshots
                  ORDER BY logical_network_key,
                           source_effective_month DESC NULLS LAST,
                           import_month DESC NULLS LAST,
                           updated_at DESC NULLS LAST
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
    """Resolve the published snapshot selected by explicit or scoped arguments."""
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
    """Return the newest snapshot-index artifact URI for a snapshot."""
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
    """Load and briefly cache the current legacy snapshot index when available."""
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
