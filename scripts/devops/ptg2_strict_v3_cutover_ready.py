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


async def collect_cutover_readiness(
    executor: Any,
    *,
    schema_name: str,
) -> dict[str, Any]:
    """Collect fail-closed cutover counts for current pointers and active builds."""

    schema = _quote_ident(schema_name)
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
                   COALESCE(snapshot.manifest->'serving_index'->>'arch_version', '')
                       = 'postgres_binary_v3' AS manifest_arch_valid,
                   COALESCE(snapshot.manifest->'serving_index'->>'storage_generation', '')
                       = 'shared_blocks_v3' AS manifest_generation_valid,
                   jsonb_typeof(snapshot.manifest->'serving_index'->'source_set')
                       = 'object' AS source_set_sealed,
                   jsonb_typeof(snapshot.manifest->'serving_index'->'audit_sample')
                       = 'object' AS audit_sample_sealed,
                   binding.snapshot_id IS NOT NULL AS binding_exists,
                   layout.state = 'sealed' AS layout_sealed,
                   layout.generation = 'shared_blocks_v3' AS layout_generation_valid,
                   CASE
                       WHEN snapshot.manifest->'serving_index'->>'shared_snapshot_key'
                            ~ '^[1-9][0-9]*$'
                       THEN (snapshot.manifest->'serving_index'->>'shared_snapshot_key')::bigint
                            = binding.snapshot_key
                       ELSE FALSE
                   END AS manifest_binding_matches
              FROM pointers AS pointer
              LEFT JOIN {schema}.ptg2_snapshot AS snapshot
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
        SELECT
            COUNT(*) FILTER (
                WHERE status IN ('pending', 'running', 'building')
            )::bigint AS active_import_run_count,
            (
                SELECT COUNT(*)::bigint
                  FROM {schema}.ptg2_snapshot
                 WHERE status = 'building'
            ) AS building_snapshot_count
          FROM {schema}.ptg2_import_run
        """
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
    failure_counts = {field: int(pointer.get(field) or 0) for field in failure_fields}
    pointer_count = int(pointer.get("pointer_count") or 0)
    active_import_run_count = int(activity.get("active_import_run_count") or 0)
    building_snapshot_count = int(activity.get("building_snapshot_count") or 0)
    ready = (
        pointer_count > 0
        and not any(failure_counts.values())
        and active_import_run_count == 0
        and building_snapshot_count == 0
    )
    return {
        "contract": "ptg_strict_v3_cutover_ready_v1",
        "ready": ready,
        "pointer_count": pointer_count,
        "active_import_run_count": active_import_run_count,
        "building_snapshot_count": building_snapshot_count,
        "failure_counts": failure_counts,
    }


async def _run(schema_name: str) -> dict[str, Any]:
    from db.connection import db

    try:
        return await collect_cutover_readiness(db, schema_name=schema_name)
    finally:
        await db.disconnect()


def main(argv: list[str] | None = None) -> int:
    """Print the cutover report and return its readiness exit code."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--schema",
        default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
    )
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(_run(args.schema))
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
