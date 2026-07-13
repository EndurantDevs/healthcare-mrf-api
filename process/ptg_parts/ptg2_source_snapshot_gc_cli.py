# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Command-line wrapper for bounded PTG source-snapshot garbage collection."""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import Iterable

from process.ptg_parts.ptg2_source_snapshot_gc import (
    PTG2SourceSnapshotGCPlan,
    build_ptg2_source_snapshot_gc_plan,
    execute_ptg2_source_snapshot_gc_plan,
    validate_ptg2_source_snapshot_gc_plan,
)


def _print_plan(plan: PTG2SourceSnapshotGCPlan) -> None:
    print(f"current_snapshot_refs={len(plan.current_snapshot_ids)}")
    print(f"candidate_snapshots={len(plan.candidate_snapshot_ids)}")
    reason_counts: dict[str, int] = {}
    for _snapshot_id, reason in plan.candidate_reasons:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    for reason, count in sorted(reason_counts.items()):
        print(f"candidate_reason={reason} snapshots={count}")
    print(f"candidate_tables={plan.table_count}")
    print(f"candidate_bytes={plan.total_bytes}")
    print(f"candidate_gib={plan.total_bytes / (1024 ** 3):.2f}")
    print(f"shared_snapshots={len(plan.shared_snapshot_ids)}")
    print(f"shared_layouts={plan.shared_layout_count}")
    print(f"shared_candidate_hashes={plan.shared_candidate_hash_count}")
    print(f"shared_stored_bytes={plan.shared_stored_bytes}")
    family_stats_by_name: dict[str, tuple[int, int]] = {}
    for table in plan.tables:
        family = table.table_name.rsplit("_", 1)[0] + "_*"
        count, byte_count = family_stats_by_name.get(family, (0, 0))
        family_stats_by_name[family] = (count + 1, byte_count + table.bytes)
    for family, (count, byte_count) in sorted(
        family_stats_by_name.items(), key=lambda entry: entry[1][1], reverse=True
    ):
        print(
            f"family={family} tables={count} bytes={byte_count} "
            f"gib={byte_count / (1024 ** 3):.2f}"
        )


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


async def _amain(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Clean non-current PTG2 manifest source snapshots."
    )
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--execute", action="store_true", help="Apply cleanup. Default is dry-run.")
    parser.add_argument("--max-snapshots", type=_non_negative_int, default=400)
    parser.add_argument("--max-tables", type=_non_negative_int, default=2000)
    parser.add_argument("--max-bytes-gb", type=_non_negative_int, default=80)
    parser.add_argument("--lock-timeout", default="5s")
    parser.add_argument("--retain-current-lineage", type=_non_negative_int, default=4)
    args = parser.parse_args(list(argv) if argv is not None else None)
    max_bytes = args.max_bytes_gb * 1024 * 1024 * 1024
    if args.execute:
        plan = await execute_ptg2_source_snapshot_gc_plan(
            schema_name=args.schema,
            max_snapshots=args.max_snapshots,
            max_tables=args.max_tables,
            max_bytes=max_bytes,
            lock_timeout=args.lock_timeout,
            retain_current_lineage=args.retain_current_lineage,
        )
        _print_plan(plan)
        print("cleanup_executed=true")
        return
    plan = await build_ptg2_source_snapshot_gc_plan(
        schema_name=args.schema,
        retain_current_lineage=args.retain_current_lineage,
        max_snapshots=args.max_snapshots,
        max_tables=args.max_tables,
        max_bytes=max_bytes,
    )
    validate_ptg2_source_snapshot_gc_plan(
        plan,
        max_snapshots=args.max_snapshots,
        max_tables=args.max_tables,
        max_bytes=max_bytes,
    )
    _print_plan(plan)
    print("cleanup_executed=false")


def main() -> None:
    """Run the bounded source-snapshot cleanup command."""
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
