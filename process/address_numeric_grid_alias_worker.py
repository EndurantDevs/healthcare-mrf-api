# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Queue and command adapters for the reviewed numeric-grid alias workflow."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from typing import Any

from arq import create_pool

from process.address_numeric_grid_alias import run_numeric_grid_alias
from process.address_numeric_grid_alias_revoke import revoke_numeric_grid_alias
from process.address_strict_source_backfill import (
    _target_limit,
    run_strict_source_backfill,
)
from process.control_cancel import raise_if_cancelled
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


ADDRESS_ALIAS_QUEUE_NAME = "arq:AddressArchive"


async def process_address_numeric_grid_alias(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process one controlled address alias operation."""
    options = task if isinstance(task, dict) else {}

    async def _cancel_check() -> None:
        await raise_if_cancelled(ctx, options)

    result = await run_numeric_grid_alias(
        mode=str(options.get("mode") or "off"),
        state_code=options.get("state_code") or None,
        zip_prefix=options.get("zip_prefix") or None,
        alias_run_id=options.get("alias_run_id") or None,
        expected_candidate_sha256=options.get("expected_candidate_sha256") or None,
        reviewed_by=options.get("reviewed_by") or None,
        sample_limit=int(options.get("sample_limit") or 20),
        timeout=str(options.get("timeout") or "10min"),
        cancel_check=_cancel_check,
    )
    payload = asdict(result)
    enqueue_live_progress(
        run_id=result.run_id,
        importer="address-numeric-grid-alias",
        status="succeeded",
        phase="address numeric-grid alias",
        unit="candidate",
        total=result.candidate_rows,
        done=result.candidate_rows,
        pct=100,
        message=f"numeric-grid alias {result.status}",
        **{
            key: value
            for key, value in payload.items()
            if key not in {"run_id", "status", "sample_rows"}
        },
    )
    return payload


async def run_address_numeric_grid_alias_command(
    *,
    mode: str = "off",
    state_code: str | None = None,
    zip_prefix: str | None = None,
    alias_run_id: str | None = None,
    expected_candidate_sha256: str | None = None,
    reviewed_by: str | None = None,
    sample_limit: int = 20,
    timeout: str = "10min",
    enqueue: bool = False,
) -> dict[str, Any] | None:
    """Run inline or enqueue one reviewed alias operation."""
    task = {
        "mode": mode,
        "state_code": state_code,
        "zip_prefix": zip_prefix,
        "alias_run_id": alias_run_id,
        "expected_candidate_sha256": expected_candidate_sha256,
        "reviewed_by": reviewed_by,
        "sample_limit": sample_limit,
        "timeout": timeout,
    }
    if enqueue:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_address_numeric_grid_alias",
            task,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_numeric_grid_alias({}, task)


process_data = process_address_numeric_grid_alias
main = run_address_numeric_grid_alias_command


async def process_address_strict_source_backfill(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process one controlled numeric-grid target evidence backfill."""
    options = task if isinstance(task, dict) else {}

    async def _cancel_check() -> None:
        await raise_if_cancelled(ctx, options)

    configured_max_targets = (
        256 if options.get("max_targets") is None else int(options["max_targets"])
    )
    result = await run_strict_source_backfill(
        alias_run_id=str(options.get("alias_run_id") or ""),
        expected_candidate_sha256=str(
            options.get("expected_candidate_sha256") or ""
        ),
        reviewed_by=str(options.get("reviewed_by") or ""),
        max_targets=_target_limit(configured_max_targets),
        timeout=str(options.get("timeout") or "10min"),
        cancel_check=_cancel_check,
    )
    payload = asdict(result)
    enqueue_live_progress(
        run_id=result.run_id,
        importer="address-strict-source-backfill",
        status="succeeded",
        phase="address strict source backfill",
        unit="target",
        total=result.target_count,
        done=result.target_count,
        pct=100,
        message="strict address source evidence backfilled",
        **{
            key: value
            for key, value in payload.items()
            if key not in {"run_id", "status"}
        },
    )
    return payload


async def run_address_strict_source_backfill_command(
    *,
    alias_run_id: str,
    expected_candidate_sha256: str,
    reviewed_by: str,
    max_targets: int = 256,
    timeout: str = "10min",
    enqueue: bool = False,
) -> dict[str, Any] | None:
    """Run inline or enqueue one target-scoped evidence backfill."""
    validated_max_targets = _target_limit(max_targets)
    task = {
        "alias_run_id": alias_run_id,
        "expected_candidate_sha256": expected_candidate_sha256,
        "reviewed_by": reviewed_by,
        "max_targets": validated_max_targets,
        "timeout": timeout,
    }
    if enqueue:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_address_strict_source_backfill",
            task,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_strict_source_backfill({}, task)


async def process_address_numeric_grid_alias_revoke(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process one controlled, one-way alias revocation."""
    options = task if isinstance(task, dict) else {}
    await raise_if_cancelled(ctx, options)
    result = await revoke_numeric_grid_alias(
        source_address_key=str(options.get("source_address_key") or ""),
        expected_target_address_key=str(
            options.get("expected_target_address_key") or ""
        ),
        reason=str(options.get("reason") or ""),
        reviewed_by=str(options.get("reviewed_by") or ""),
        timeout=str(options.get("timeout") or "30s"),
    )
    payload = asdict(result)
    enqueue_live_progress(
        run_id=result.run_id,
        importer="address-numeric-grid-alias-revoke",
        status="succeeded",
        phase="address numeric-grid alias revoke",
        unit="alias",
        total=1,
        done=1,
        pct=100,
        message="numeric-grid alias revoked; full artifact rebuild required",
        **{key: value for key, value in payload.items() if key not in {"run_id", "status"}},
    )
    return payload


async def run_address_numeric_grid_alias_revoke_command(
    *,
    source_address_key: str,
    expected_target_address_key: str,
    reason: str,
    reviewed_by: str,
    timeout: str = "30s",
    enqueue: bool = False,
) -> dict[str, Any] | None:
    """Run inline or enqueue one exact alias revocation."""
    task = {
        "source_address_key": source_address_key,
        "expected_target_address_key": expected_target_address_key,
        "reason": reason,
        "reviewed_by": reviewed_by,
        "timeout": timeout,
    }
    if enqueue:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_address_numeric_grid_alias_revoke",
            task,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_numeric_grid_alias_revoke({}, task)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
