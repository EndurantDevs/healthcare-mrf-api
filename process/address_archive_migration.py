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


async def process_data(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    """Process one address-archive migration task."""
    payload = task if isinstance(task, dict) else {}
    run_id = str(payload.get("run_id") or ctx.get("control_run_id") or "").strip()

    async def _cancel_check() -> None:
        await raise_if_cancelled(ctx, payload)

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
        schema=payload.get("schema") or None,
        legacy_table=str(payload.get("legacy_table") or "address_archive"),
        archive_table=str(payload.get("archive_table") or "address_archive_v2"),
        work_mem=str(payload.get("work_mem") or "512MB"),
        timeout=str(payload.get("timeout") or "30min"),
        dry_run=bool(payload.get("dry_run", False)),
        sample_limit=int(payload.get("sample_limit") or 20),
        cancel_check=_cancel_check,
    )
    result = asdict(stats)
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
        **{key: value for key, value in result.items() if key != "sample_rows"},
    )
    return result


async def main(
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
    """Run or enqueue the legacy-to-canonical address archive migration."""

    payload = {
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
        await redis.enqueue_job("process_data", payload, _queue_name=ADDRESS_ARCHIVE_QUEUE_NAME)
        return None
    return await process_data({}, payload)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
