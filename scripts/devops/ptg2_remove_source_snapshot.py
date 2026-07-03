#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dry-run or remove one old source-scoped PTG2 snapshot."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", required=True, help="Snapshot id to inspect or remove.")
    parser.add_argument("--source-key", help="Expected source key for a safety cross-check.")
    parser.add_argument("--execute", action="store_true", help="Drop tables and delete metadata. Default is dry-run.")
    return parser


async def _execute_removal(cli_args: argparse.Namespace) -> dict[str, Any]:
    from process.ptg_parts.source_snapshot_control import (
        build_ptg2_source_snapshot_remove_plan,
        remove_ptg2_source_snapshot,
    )

    remove_request_by_name = {
        "snapshot_id": cli_args.snapshot_id.strip(),
        "source_key": str(cli_args.source_key or "").strip() or None,
    }
    if cli_args.execute:
        return await remove_ptg2_source_snapshot(**remove_request_by_name)
    plan = await build_ptg2_source_snapshot_remove_plan(**remove_request_by_name)
    return {**plan, "executed": False}


def _exit_code(output: dict[str, Any]) -> int:
    if output.get("removable") is False:
        return 2
    if output.get("executed") and not output.get("deleted_snapshots") and output.get("exists"):
        return 3
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the remove CLI and print a JSON result."""
    args = _build_parser().parse_args(argv)
    try:
        output = asyncio.run(_execute_removal(args))
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return _exit_code(output)
    finally:
        asyncio.run(_db_connection().disconnect())


if __name__ == "__main__":
    raise SystemExit(main())
