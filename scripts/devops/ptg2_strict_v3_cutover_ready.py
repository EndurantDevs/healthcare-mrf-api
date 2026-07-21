#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail closed unless every current PTG pointer is ready for strict V3 serving."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


_STALE_ACTIVITY_SECONDS_ENV = "HLTHPRT_PTG2_STALE_BUILD_SECONDS"
_STALE_ACTIVITY_SECONDS_DEFAULT = 21_600
_STALE_ACTIVITY_SECONDS_MINIMUM = 300


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parents[2]
    for path in (root, Path("/opt")):
        if path.exists():
            sys.path.insert(0, str(path))


_bootstrap_import_path()


def _row_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    mapping = getattr(row, "_mapping", None)
    if isinstance(mapping, Mapping):
        return mapping
    raise TypeError("cutover readiness query returned an unsupported row")


def _quote_ident(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("invalid database schema")
    return '"' + value.replace('"', '""') + '"'


def _stale_activity_seconds(value: int | None) -> int:
    raw_value: Any = (
        value
        if value is not None
        else os.getenv(
            _STALE_ACTIVITY_SECONDS_ENV,
            _STALE_ACTIVITY_SECONDS_DEFAULT,
        )
    )
    try:
        return max(int(raw_value), _STALE_ACTIVITY_SECONDS_MINIMUM)
    except (TypeError, ValueError):
        return _STALE_ACTIVITY_SECONDS_DEFAULT


async def collect_cutover_readiness(
    executor: Any,
    *,
    schema_name: str,
    stale_activity_seconds: int | None = None,
) -> dict[str, Any]:
    """Collect fail-closed cutover counts for current pointers and active builds."""

    schema = _quote_ident(schema_name)
    stale_seconds = _stale_activity_seconds(stale_activity_seconds)
    pointer_rows = await executor.all(
        f"""
        WITH pointers AS (
            SELECT snapshot_id FROM {schema}.ptg2_current_snapshot
            UNION ALL
            SELECT snapshot_id FROM {schema}.ptg2_current_source_snapshot
            UNION ALL
            SELECT snapshot_id FROM {schema}.ptg2_current_plan_source
        ), checked AS (
            SELECT pointer.snapshot_id,
                   snapshot.snapshot_id IS NOT NULL AS snapshot_exists,
                   snapshot.status = 'published' AS snapshot_published,
                   COALESCE(snapshot.manifest_jsonb->'serving_index'->>'arch_version', '')
                       = 'postgres_binary_v3' AS manifest_arch_valid,
                   COALESCE(snapshot.manifest_jsonb->'serving_index'->>'storage_generation', '')
                       = 'shared_blocks_v3' AS manifest_generation_valid,
                   jsonb_typeof(snapshot.manifest_jsonb->'serving_index'->'source_set')
                       = 'object' AS source_set_sealed,
                   jsonb_typeof(snapshot.manifest_jsonb->'serving_index'->'audit_sample')
                       = 'object' AS audit_sample_sealed,
                   binding.snapshot_id IS NOT NULL AS binding_exists,
                   layout.state = 'sealed' AS layout_sealed,
                   layout.generation = 'shared_blocks_v3' AS layout_generation_valid,
                   CASE
                       WHEN snapshot.manifest_jsonb->'serving_index'->>'shared_snapshot_key'
                            ~ '^[1-9][0-9]*$'
                       THEN (snapshot.manifest_jsonb->'serving_index'->>'shared_snapshot_key')::bigint
                            = binding.snapshot_key
                       ELSE FALSE
                   END AS manifest_binding_matches
              FROM pointers AS pointer
              LEFT JOIN (
                    SELECT snapshot_id, status, manifest::jsonb AS manifest_jsonb
                      FROM {schema}.ptg2_snapshot
              ) AS snapshot
                ON snapshot.snapshot_id = pointer.snapshot_id
              LEFT JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = pointer.snapshot_id
              LEFT JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
        )
        SELECT COUNT(*)::bigint AS pointer_count,
               COUNT(*) FILTER (WHERE NOT snapshot_exists)::bigint
                   AS missing_snapshot_count,
               COUNT(*) FILTER (WHERE NOT snapshot_published)::bigint
                   AS unpublished_snapshot_count,
               COUNT(*) FILTER (WHERE NOT manifest_arch_valid)::bigint
                   AS invalid_arch_count,
               COUNT(*) FILTER (WHERE NOT manifest_generation_valid)::bigint
                   AS invalid_manifest_generation_count,
               COUNT(*) FILTER (WHERE NOT source_set_sealed)::bigint
                   AS unsealed_source_set_count,
               COUNT(*) FILTER (WHERE NOT audit_sample_sealed)::bigint
                   AS unsealed_audit_sample_count,
               COUNT(*) FILTER (WHERE NOT binding_exists)::bigint
                   AS missing_binding_count,
               COUNT(*) FILTER (WHERE NOT layout_sealed)::bigint
                   AS unsealed_layout_count,
               COUNT(*) FILTER (WHERE NOT layout_generation_valid)::bigint
                   AS invalid_layout_generation_count,
               COUNT(*) FILTER (WHERE NOT manifest_binding_matches)::bigint
                   AS mismatched_binding_count
          FROM checked
        """
    )
    activity_rows = await executor.all(
        f"""
        WITH import_activity AS (
            SELECT import_run_id,
                   status,
                   COALESCE(
                       heartbeat_at,
                       started_at,
                       '-infinity'::timestamp
                   ) >= timezone('UTC', transaction_timestamp())
                       - (:stale_activity_seconds * INTERVAL '1 second')
                       AS is_fresh
              FROM {schema}.ptg2_import_run
        ), snapshot_activity AS (
            SELECT snapshot.snapshot_id,
                   CASE
                       WHEN import_activity.import_run_id IS NOT NULL
                       THEN import_activity.status IN ('pending', 'running', 'building')
                            AND import_activity.is_fresh
                       ELSE COALESCE(
                           snapshot.created_at,
                           '-infinity'::timestamp
                       ) >= timezone('UTC', transaction_timestamp())
                           - (:stale_activity_seconds * INTERVAL '1 second')
                   END AS is_fresh
              FROM {schema}.ptg2_snapshot AS snapshot
              LEFT JOIN import_activity
                ON import_activity.import_run_id = snapshot.import_run_id
             WHERE snapshot.status = 'building'
        )
        SELECT
            COUNT(*) FILTER (
                WHERE status IN ('pending', 'running', 'building') AND is_fresh
            )::bigint AS active_import_run_count,
            COUNT(*) FILTER (
                WHERE status IN ('pending', 'running', 'building') AND NOT is_fresh
            )::bigint AS stale_import_run_count,
            (
                SELECT COUNT(*)::bigint
                  FROM snapshot_activity
                 WHERE is_fresh
            ) AS building_snapshot_count,
            (
                SELECT COUNT(*)::bigint
                  FROM snapshot_activity
                 WHERE NOT is_fresh
            ) AS stale_building_snapshot_count
          FROM import_activity
        """,
        stale_activity_seconds=stale_seconds,
    )
    if len(pointer_rows) != 1 or len(activity_rows) != 1:
        raise RuntimeError("cutover readiness queries returned invalid cardinality")

    pointer = _row_mapping(pointer_rows[0])
    activity = _row_mapping(activity_rows[0])
    failure_fields = (
        "missing_snapshot_count",
        "unpublished_snapshot_count",
        "invalid_arch_count",
        "invalid_manifest_generation_count",
        "unsealed_source_set_count",
        "unsealed_audit_sample_count",
        "missing_binding_count",
        "unsealed_layout_count",
        "invalid_layout_generation_count",
        "mismatched_binding_count",
    )
    failure_count_by_field = {
        field: int(pointer.get(field) or 0) for field in failure_fields
    }
    pointer_count = int(pointer.get("pointer_count") or 0)
    active_import_run_count = int(activity.get("active_import_run_count") or 0)
    building_snapshot_count = int(activity.get("building_snapshot_count") or 0)
    stale_import_run_count = int(activity.get("stale_import_run_count") or 0)
    stale_building_snapshot_count = int(
        activity.get("stale_building_snapshot_count") or 0
    )
    is_ready = (
        pointer_count > 0
        and not any(failure_count_by_field.values())
        and active_import_run_count == 0
        and building_snapshot_count == 0
    )
    return {
        "contract": "ptg_strict_v3_cutover_ready_v1",
        "ready": is_ready,
        "pointer_count": pointer_count,
        "active_import_run_count": active_import_run_count,
        "building_snapshot_count": building_snapshot_count,
        "stale_import_run_count": stale_import_run_count,
        "stale_building_snapshot_count": stale_building_snapshot_count,
        "stale_activity_seconds": stale_seconds,
        "failure_counts": failure_count_by_field,
    }


async def _run(
    schema_name: str,
    stale_activity_seconds: int | None,
) -> dict[str, Any]:
    from db.connection import db

    try:
        return await collect_cutover_readiness(
            db,
            schema_name=schema_name,
            stale_activity_seconds=stale_activity_seconds,
        )
    finally:
        await db.disconnect()


def _parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """Parse the bounded cutover-readiness CLI surface."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schema",
        default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
    )
    parser.add_argument(
        "--stale-activity-seconds",
        type=int,
        help=(
            "Heartbeat age after which abandoned import/build rows are reported as stale "
            f"instead of active (default: {_STALE_ACTIVITY_SECONDS_ENV} or "
            f"{_STALE_ACTIVITY_SECONDS_DEFAULT})"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Print the cutover report and return its readiness exit code."""

    args = _parse_arguments(argv)
    try:
        readiness = asyncio.run(
            _run(args.schema, args.stale_activity_seconds)
        )
    except Exception:
        print(
            json.dumps(
                {
                    "contract": "ptg_strict_v3_cutover_ready_v1",
                    "ready": False,
                    "error": "cutover_readiness_unavailable",
                },
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(readiness, indent=2, sort_keys=True))
    return 0 if readiness["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
