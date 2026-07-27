# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-time canonical address archive migration helpers."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any

from arq import create_pool

from process.control_cancel import raise_if_cancelled
from process.ext.address_canon import migrate_legacy_archive_to_v2
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

ADDRESS_ARCHIVE_QUEUE_NAME = "arq:AddressArchive"


async def process_address_archive_migration_data(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Process one address-archive migration task."""
    task_by_field = task if isinstance(task, dict) else {}
    run_id = str(
        task_by_field.get("run_id") or ctx.get("control_run_id") or ""
    ).strip()

    async def _cancel_check() -> None:
        await raise_if_cancelled(ctx, task_by_field)

    enqueue_live_progress(
        run_id=run_id or None,
        importer="address-archive-v2-migrate",
        status="running",
        phase="address archive v2 migration",
        unit="phase",
        done=0,
        total=4,
        pct=0,
        message="starting legacy address archive migration",
    )
    stats = await migrate_legacy_archive_to_v2(
        schema=task_by_field.get("schema") or None,
        legacy_table=str(
            task_by_field.get("legacy_table") or "address_archive"
        ),
        archive_table=str(
            task_by_field.get("archive_table") or "address_archive_v2"
        ),
        work_mem=str(task_by_field.get("work_mem") or "512MB"),
        timeout=str(task_by_field.get("timeout") or "30min"),
        dry_run=bool(task_by_field.get("dry_run", False)),
        sample_limit=int(task_by_field.get("sample_limit") or 20),
        cancel_check=_cancel_check,
    )
    migration_result_by_field = asdict(stats)
    enqueue_live_progress(
        run_id=run_id or None,
        importer="address-archive-v2-migrate",
        status="succeeded",
        phase="address archive v2 migration",
        unit="phase",
        done=4,
        total=4,
        pct=100,
        message="legacy address archive migration verified",
        **{
            metric_name: metric_value
            for metric_name, metric_value in migration_result_by_field.items()
            if metric_name != "sample_rows"
        },
    )
    return migration_result_by_field


process_data = process_address_archive_migration_data
process_data.__name__ = "process_data"


async def run_address_archive_migration_command(
    *,
    dry_run: bool = False,
    legacy_table: str = "address_archive",
    archive_table: str = "address_archive_v2",
    work_mem: str = "512MB",
    timeout: str = "30min",
    sample_limit: int = 20,
    enqueue: bool = False,
    test_mode: bool = False,
) -> dict[str, Any] | None:
    """Run or enqueue the verified legacy address-archive migration."""

    task_by_field = {
        "dry_run": dry_run,
        "legacy_table": legacy_table,
        "archive_table": archive_table,
        "work_mem": work_mem,
        "timeout": timeout,
        "sample_limit": sample_limit,
        "test_mode": test_mode,
    }
    if enqueue:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_data",
            task_by_field,
            _queue_name=ADDRESS_ARCHIVE_QUEUE_NAME,
        )
        return None
    return await process_data({}, task_by_field)


main = run_address_archive_migration_command
main.__name__ = "main"


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
