#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare an old PTG2 snapshot with a rebuilt snapshot before cleanup."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parents[2]
    script_dir = Path(__file__).resolve().parent
    for path in (script_dir, root, Path("/opt")):
        if path.exists():
            sys.path.insert(0, str(path))


_bootstrap_import_path()


def _db_connection():
    from db.connection import db

    return db


def _sql_text(statement: str):
    from sqlalchemy import text as sqlalchemy_text

    return sqlalchemy_text(statement)


def _as_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _sidecar_summary(serving_index: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = serving_index.get("artifacts") if isinstance(serving_index.get("artifacts"), dict) else {}
    sidecars = artifacts.get("sidecars") if isinstance(artifacts, dict) else []
    sidecar_rows = []
    for sidecar_record in sidecars if isinstance(sidecars, list) else []:
        if not isinstance(sidecar_record, dict):
            continue
        sidecar_rows.append(
            {
                "name": sidecar_record.get("name") or sidecar_record.get("kind"),
                "byte_count": sidecar_record.get("byte_count") or sidecar_record.get("bytes"),
                "row_count": sidecar_record.get("row_count") or sidecar_record.get("rows"),
                "sha256": sidecar_record.get("sha256") or sidecar_record.get("content_sha256"),
            }
        )
    return sorted(sidecar_rows, key=lambda sidecar_row: str(sidecar_row.get("name")))


def _serving_index_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else manifest
    return serving_index if isinstance(serving_index, dict) else {}


def _snapshot_table_names(serving_index: dict[str, Any]) -> dict[str, Any]:
    return {
        "serving": serving_index.get("table"),
        "price_atom": serving_index.get("price_atom_table"),
        "price_atom_dict": serving_index.get("price_atom_dictionary_table"),
        "provider_member": serving_index.get("provider_group_member_table"),
        "provider_location": serving_index.get("provider_group_location_table"),
        "provider_component": serving_index.get("provider_set_component_table"),
        "provider_rate_scope": serving_index.get("provider_group_rate_scope_table"),
        "provider_set_dict": serving_index.get("provider_set_dictionary_table"),
        "code_count": serving_index.get("code_count_table"),
    }


async def _table_sizes_by_role(session, table_names_by_role: dict[str, Any]) -> dict[str, Any]:
    table_size_by_role = {}
    for role, table_name in table_names_by_role.items():
        if not table_name:
            continue
        size_result = await session.execute(
            _sql_text(
                """
                SELECT pg_total_relation_size(to_regclass(:table_name)) AS total_bytes,
                       pg_relation_size(to_regclass(:table_name)) AS heap_bytes,
                       pg_size_pretty(pg_total_relation_size(to_regclass(:table_name))) AS total_size
                """
            ),
            {"table_name": table_name},
        )
        size_record = size_result.mappings().first()
        table_size_by_role[role] = dict(size_record) if size_record else {}
    return table_size_by_role


async def _snapshot_info(session, snapshot_id: str) -> dict[str, Any]:
    """Load table, count, and sidecar metadata for one PTG2 snapshot."""
    snapshot_result = await session.execute(
        _sql_text(
            """
            SELECT s.snapshot_id,
                   s.import_run_id,
                   s.import_month::text AS import_month,
                   s.status,
                   s.previous_snapshot_id,
                   s.manifest,
                   r.options,
                   r.report
              FROM mrf.ptg2_snapshot s
              LEFT JOIN mrf.ptg2_import_run r ON r.import_run_id = s.import_run_id
             WHERE s.snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    snapshot_record = snapshot_result.mappings().first()
    if snapshot_record is None:
        raise RuntimeError(f"snapshot not found: {snapshot_id}")
    manifest = _as_json(snapshot_record["manifest"])
    options = _as_json(snapshot_record["options"])
    report = _as_json(snapshot_record["report"])
    serving_index = _serving_index_from_manifest(manifest)
    table_names_by_role = _snapshot_table_names(serving_index)
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": snapshot_record["import_run_id"],
        "import_month": snapshot_record["import_month"],
        "status": snapshot_record["status"],
        "previous_snapshot_id": snapshot_record["previous_snapshot_id"],
        "source_key": options.get("source_key") or serving_index.get("source_key"),
        "arch_version": serving_index.get("arch_version") or "legacy_implicit",
        "provider_scope_strategy": serving_index.get("provider_scope_strategy") or "legacy_implicit",
        "materialized_tables": serving_index.get("materialized_tables") or {},
        "serving_rates": manifest.get("serving_rates") or serving_index.get("serving_rates") or report.get("serving_rates"),
        "files_processed": manifest.get("files_processed") or report.get("files_processed"),
        "serving_layout": serving_index.get("serving_table_layout") or "legacy_or_default",
        "price_atom_layout": serving_index.get("price_atom_table_layout") or "wide_or_default",
        "tables": table_names_by_role,
        "sizes": await _table_sizes_by_role(session, table_names_by_role),
        "sidecars": _sidecar_summary(serving_index),
    }


from ptg2_compare_snapshot_checks import (
    _latency_benchmark,
    _sample_price_atoms,
    _sample_provider_members,
    _sample_serving,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-snapshot-id", required=True, help="Existing snapshot being replaced.")
    parser.add_argument("--new-snapshot-id", required=True, help="Rebuilt snapshot candidate.")
    parser.add_argument("--sample-limit", type=int, default=500, help="Rows to sample from each checked table.")
    parser.add_argument("--sample-pct", type=float, default=0.1, help="Postgres TABLESAMPLE SYSTEM percentage.")
    parser.add_argument("--skip-sidecars", action="store_true", help="Do not fail on sidecar hash/size mismatch.")
    parser.add_argument("--benchmark-cases", type=int, default=0, help="Plan/code cases to time against both snapshots.")
    parser.add_argument("--benchmark-iterations", type=int, default=3, help="Measured iterations per benchmark case.")
    parser.add_argument("--benchmark-limit", type=int, default=5, help="Serving result limit for latency benchmark cases.")
    return parser


async def _compare_snapshots(cli_args: argparse.Namespace) -> dict[str, Any]:
    limit = max(int(cli_args.sample_limit), 1)
    sample_pct = max(float(cli_args.sample_pct), 0.0001)
    database = _db_connection()
    async with database.session() as session:
        old_info = await _snapshot_info(session, cli_args.old_snapshot_id.strip())
        new_info = await _snapshot_info(session, cli_args.new_snapshot_id.strip())
        is_sidecar_match = old_info["sidecars"] == new_info["sidecars"]
        check_result_by_name = {
            "old_status_published": str(old_info["status"]).lower() == "published",
            "new_status_published": str(new_info["status"]).lower() == "published",
            "source_key_equal": str(old_info["source_key"]) == str(new_info["source_key"]),
            "serving_rates_equal": str(old_info["serving_rates"]) == str(new_info["serving_rates"]),
            "files_processed_equal": str(old_info["files_processed"]) == str(new_info["files_processed"]),
            "sidecars_equal": is_sidecar_match,
            "sidecars_checked": not cli_args.skip_sidecars,
            "serving_sample": await _sample_serving(session, old_info, new_info, limit, sample_pct),
            "price_atom_sample": await _sample_price_atoms(session, old_info, new_info, limit, sample_pct),
            "provider_member_sample": await _sample_provider_members(session, old_info, new_info, limit, sample_pct),
            "latency_benchmark": await _latency_benchmark(
                session,
                old_info,
                new_info,
                case_count=max(int(cli_args.benchmark_cases), 0),
                iterations=max(int(cli_args.benchmark_iterations), 1),
                limit=max(int(cli_args.benchmark_limit), 1),
                sample_pct=sample_pct,
            ),
        }
    missing_counts = [
        int(check_result_by_name["serving_sample"]["missing"] or 0),
        int(check_result_by_name["price_atom_sample"]["missing"] or 0),
        int(check_result_by_name["provider_member_sample"]["missing"] or 0),
    ]
    required_conditions = [
        check_result_by_name["old_status_published"],
        check_result_by_name["new_status_published"],
        check_result_by_name["source_key_equal"],
        check_result_by_name["serving_rates_equal"],
        check_result_by_name["files_processed_equal"],
        not any(missing_counts),
        bool(cli_args.skip_sidecars or is_sidecar_match),
    ]
    check_result_by_name["passed"] = all(required_conditions)
    return {"old": old_info, "new": new_info, "checks": check_result_by_name}


async def _run_cli(args: argparse.Namespace) -> dict[str, Any]:
    try:
        return await _compare_snapshots(args)
    finally:
        await _db_connection().disconnect()


def main(argv: list[str] | None = None) -> int:
    """Run the snapshot comparison CLI and print a JSON result."""
    args = _build_parser().parse_args(argv)
    output = asyncio.run(_run_cli(args))
    try:
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0 if output["checks"]["passed"] else 2
    except BrokenPipeError:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
