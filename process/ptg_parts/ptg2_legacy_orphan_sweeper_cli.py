# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CLI for the bounded legacy PTG orphan sweeper."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Iterable

from process.ptg_parts.ptg2_legacy_orphan_contract import LegacySweepLimits
from process.ptg_parts.ptg2_legacy_orphan_sweeper import (
    build_legacy_orphan_sweep_plan,
    execute_legacy_orphan_sweep,
)


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _blocked_reason_counts(plan: object) -> dict[str, int]:
    counts_by_reason: dict[str, int] = {}
    for blocked_suffix in getattr(plan, "blocked"):
        for reason in blocked_suffix.reasons:
            counts_by_reason[reason] = counts_by_reason.get(reason, 0) + 1
    return dict(sorted(counts_by_reason.items()))


def _limit_summary(limits: LegacySweepLimits) -> dict[str, int]:
    return {
        "max_suffixes": limits.max_suffixes,
        "max_tables": limits.max_tables,
        "max_relations": limits.max_relations,
        "max_bytes": limits.max_bytes,
    }


def _plan_summary(
    plan: object,
    *,
    state: str,
    limits: LegacySweepLimits,
) -> dict[str, object]:
    return {
        "contract": "ptg2_legacy_orphan_sweep_v1",
        "state": state,
        "plan_digest": getattr(plan, "plan_digest"),
        "authority_digest": getattr(plan, "authority_digest"),
        "catalog_digest": getattr(plan, "catalog_digest"),
        "selected_suffixes": len(getattr(plan, "candidates")),
        "selected_root_tables": getattr(plan, "table_count"),
        "selected_relations": getattr(plan, "relation_count"),
        "selected_bytes": getattr(plan, "total_bytes"),
        "selected_snapshots": len(getattr(plan, "snapshot_ids")),
        "eligible_suffixes": getattr(plan, "eligible_suffix_count"),
        "remaining_eligible_suffixes": getattr(
            plan,
            "remaining_eligible_suffix_count",
        ),
        "catalog_suffixes": getattr(plan, "catalog_suffix_count"),
        "scanned_suffixes": getattr(plan, "scanned_suffix_count"),
        "unscanned_suffixes": getattr(plan, "unscanned_suffix_count"),
        "blocked_suffixes": len(getattr(plan, "blocked")),
        "blocked_reason_counts": _blocked_reason_counts(plan),
        "limits": _limit_summary(limits),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect or remove proven non-serving legacy PTG relations. "
            "Default is dry-run."
        )
    )
    parser.add_argument(
        "--schema",
        default=None,
        help=(
            "Explicit PTG schema. When omitted, the shared resolver validates "
            "HLTHPRT_DB_SCHEMA and DB_SCHEMA."
        ),
    )
    parser.add_argument(
        "--control-schema",
        default=os.getenv("HLTHPRT_PTG_CONTROL_SCHEMA"),
        help=(
            "Required lifecycle-control schema. May also be supplied through "
            "HLTHPRT_PTG_CONTROL_SCHEMA."
        ),
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-plan-digest")
    parser.add_argument("--actor")
    parser.add_argument("--max-suffixes", type=_non_negative_int, default=25)
    parser.add_argument("--max-tables", type=_non_negative_int, default=300)
    parser.add_argument("--max-relations", type=_non_negative_int, default=1200)
    parser.add_argument(
        "--max-bytes",
        type=_non_negative_int,
        default=2 * 1024 * 1024 * 1024,
    )
    parser.add_argument("--lock-timeout", default="5s")
    return parser


async def _amain(argv: Iterable[str] | None = None) -> None:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    limits = LegacySweepLimits(
        max_suffixes=args.max_suffixes,
        max_tables=args.max_tables,
        max_relations=args.max_relations,
        max_bytes=args.max_bytes,
    )
    if not args.apply:
        plan = await build_legacy_orphan_sweep_plan(
            schema_name=args.schema,
            control_schema_name=args.control_schema,
            limits=limits,
        )
        print(
            json.dumps(
                _plan_summary(plan, state="dry_run", limits=limits),
                sort_keys=True,
            )
        )
        return
    if not args.expected_plan_digest or not args.actor:
        raise SystemExit(
            "--apply requires --expected-plan-digest and --actor"
        )
    execution = await execute_legacy_orphan_sweep(
        expected_plan_digest=args.expected_plan_digest,
        actor=args.actor,
        schema_name=args.schema,
        control_schema_name=args.control_schema,
        limits=limits,
        lock_timeout=args.lock_timeout,
    )
    summary_by_field = {
        "contract": "ptg2_legacy_orphan_sweep_v1",
        "state": execution.state,
        "plan_digest": args.expected_plan_digest,
        "selected_suffixes": execution.selected_suffixes,
        "selected_root_tables": execution.selected_root_tables,
        "selected_relations": execution.selected_relations,
        "selected_bytes": execution.selected_bytes,
        "selected_snapshots": execution.selected_snapshots,
        "audit_id": execution.audit_id,
        "limits": _limit_summary(limits),
    }
    print(json.dumps(summary_by_field, sort_keys=True))


def main() -> None:
    """Run the maintenance command."""

    asyncio.run(_amain())


if __name__ == "__main__":
    main()
