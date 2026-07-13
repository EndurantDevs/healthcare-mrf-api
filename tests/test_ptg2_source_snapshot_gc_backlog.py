# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc


class _BacklogExecutor:
    async def all(self, statement, **_params):
        if "SELECT DISTINCT snapshot_id" in statement:
            return []
        if 'FROM "mrf".ptg2_snapshot' in statement:
            return [
                {
                    "snapshot_id": f"snap-{index:03d}",
                    "status": "failed",
                    "source_key": "source_a",
                    "serving_index": {
                        "arch_version": "postgres_binary_v3",
                        "storage_generation": "shared_blocks_v3",
                    },
                }
                for index in range(1, 5)
            ]
        raise AssertionError(statement)


def test_bounded_gc_selects_only_strict_v3_snapshot_metadata():
    plan = asyncio.run(
        snapshot_gc.build_ptg2_source_snapshot_gc_plan(
            executor=_BacklogExecutor(),
            max_snapshots=2,
            max_tables=0,
            max_bytes=0,
        )
    )

    assert plan.candidate_snapshot_ids == ("snap-001", "snap-002")
    assert plan.shared_snapshot_ids == ("snap-001", "snap-002")
    assert plan.tables == ()
