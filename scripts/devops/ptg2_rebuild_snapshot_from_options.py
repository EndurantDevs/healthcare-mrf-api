#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rebuild a PTG2 source snapshot from the import options stored in Postgres.

Run this from an environment that already has the HealthPorta database
configuration, for example inside a deployed API image.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parents[2]
    for path in (root, Path("/opt")):
        if path.exists():
            sys.path.insert(0, str(path))


_bootstrap_import_path()


def _db_connection():
    from db.connection import db

    return db


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


def _as_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item or "").strip()]
    text_value = str(value).strip()
    return [text_value] if text_value else []


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


async def _load_snapshot_options(snapshot_id: str) -> dict[str, Any]:
    database = _db_connection()
    async with database.session() as session:
        snapshot_result = await session.execute(
            _sql_text(
                """
                SELECT s.snapshot_id,
                       s.import_run_id,
                       s.import_month::text AS import_month,
                       s.manifest,
                       r.options
                  FROM mrf.ptg2_snapshot s
                  JOIN mrf.ptg2_import_run r ON r.import_run_id = s.import_run_id
                 WHERE s.snapshot_id = :snapshot_id
                 LIMIT 1
                """
            ),
            {"snapshot_id": snapshot_id},
        )
        snapshot_row = snapshot_result.mappings().first()
    if snapshot_row is None:
        raise RuntimeError(f"snapshot not found: {snapshot_id}")
    options = _as_json(snapshot_row["options"])
    manifest = _as_json(snapshot_row["manifest"])
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else {}
    source_key = str(options.get("source_key") or serving_index.get("source_key") or "").strip()
    if not source_key:
        raise RuntimeError(f"snapshot has no source_key: {snapshot_id}")
    return {
        "snapshot_id": snapshot_row["snapshot_id"],
        "import_run_id": snapshot_row["import_run_id"],
        "import_month": snapshot_row["import_month"],
        "options": options,
        "source_key": source_key,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True, help="Existing snapshot id to rebuild from stored import options.")
    parser.add_argument("--import-id", help="Import id suffix to use for the rebuild run.")
    parser.add_argument(
        "--reuse-raw-artifacts",
        dest="reuse_raw_artifacts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse retained raw artifacts when the stored options allow it.",
    )
    parser.add_argument(
        "--keep-partial-artifacts",
        dest="keep_partial_artifacts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep partial artifacts for post-run debugging.",
    )
    return parser


async def _execute_rebuild(cli_args: argparse.Namespace) -> dict[str, Any]:
    from process.ptg import main as run_ptg

    target_snapshot_id = cli_args.snapshot_id.strip()
    snapshot_options = await _load_snapshot_options(target_snapshot_id)
    options = snapshot_options["options"]
    source_key = snapshot_options["source_key"]
    import_id = cli_args.import_id
    if not import_id:
        stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        import_id = f"space_rebuild_{source_key[-10:]}_{stamp}"
    ptg_run_result = await run_ptg(
        test_mode=_is_truthy(options.get("test_mode")),
        toc_urls=_as_list(options.get("toc_urls")),
        toc_list=str(options.get("toc_list") or "").strip() or None,
        in_network_url=str(options.get("in_network_url") or "").strip() or None,
        allowed_url=str(options.get("allowed_url") or "").strip() or None,
        provider_ref_url=str(options.get("provider_ref_url") or "").strip() or None,
        import_id=import_id,
        source_key=source_key,
        import_month=str(options.get("import_month") or snapshot_options["import_month"]),
        max_files=options.get("max_files"),
        max_items=options.get("max_items"),
        plan_ids=_as_list(options.get("plan_ids")),
        plan_name_contains=_as_list(options.get("plan_name_contains")),
        plan_market_types=_as_list(options.get("plan_market_types")),
        file_url_contains=_as_list(options.get("file_url_contains")),
        source_network_names=_as_list(options.get("source_network_names")),
        reuse_raw_artifacts=bool(cli_args.reuse_raw_artifacts and _is_truthy(options.get("reuse_raw_artifacts", True))),
        keep_partial_artifacts=bool(cli_args.keep_partial_artifacts),
    )
    return {
        "status": ptg_run_result.get("status"),
        "old_snapshot_id": target_snapshot_id,
        "new_snapshot_id": ptg_run_result.get("snapshot_id"),
        "source_key": ptg_run_result.get("source_key"),
        "import_month": ptg_run_result.get("import_month"),
        "import_run_id": ptg_run_result.get("import_run_id"),
        "files_attempted": ptg_run_result.get("files_attempted"),
        "files_processed": ptg_run_result.get("files_processed"),
        "files_failed": ptg_run_result.get("files_failed"),
        "serving_rates": ptg_run_result.get("serving_rates"),
    }


def main(argv: list[str] | None = None) -> int:
    """Run the rebuild CLI and print a JSON result."""
    args = _build_parser().parse_args(argv)
    try:
        output = asyncio.run(_execute_rebuild(args))
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0 if output.get("status") == "succeeded" else 2
    finally:
        asyncio.run(_db_connection().disconnect())


if __name__ == "__main__":
    raise SystemExit(main())
