# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Queue and command adapters for the reviewed numeric-grid alias workflow."""

from __future__ import annotations

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

    alias_result = await run_numeric_grid_alias(
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
    result_payload = asdict(alias_result)
    enqueue_live_progress(
        run_id=alias_result.run_id,
        importer="address-numeric-grid-alias",
        status="succeeded",
        phase="address numeric-grid alias",
        unit="candidate",
        total=alias_result.candidate_rows,
        done=alias_result.candidate_rows,
        pct=100,
        message=f"numeric-grid alias {alias_result.status}",
        **{
            key: field_value
            for key, field_value in result_payload.items()
            if key not in {"run_id", "status", "sample_rows"}
        },
    )
    return result_payload


async def run_address_numeric_grid_alias_command(
    **option_values_by_name: Any,
) -> dict[str, Any] | None:
    """Run inline or enqueue one reviewed alias operation."""
    task_options_by_name = {
        "mode": option_values_by_name.get("mode", "off"),
        "state_code": option_values_by_name.get("state_code"),
        "zip_prefix": option_values_by_name.get("zip_prefix"),
        "alias_run_id": option_values_by_name.get("alias_run_id"),
        "expected_candidate_sha256": option_values_by_name.get(
            "expected_candidate_sha256"
        ),
        "reviewed_by": option_values_by_name.get("reviewed_by"),
        "sample_limit": option_values_by_name.get("sample_limit", 20),
        "timeout": option_values_by_name.get("timeout", "10min"),
    }
    if bool(option_values_by_name.get("enqueue", False)):
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_address_numeric_grid_alias",
            task_options_by_name,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_numeric_grid_alias({}, task_options_by_name)


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
    backfill_result = await run_strict_source_backfill(
        alias_run_id=str(options.get("alias_run_id") or ""),
        expected_candidate_sha256=str(
            options.get("expected_candidate_sha256") or ""
        ),
        reviewed_by=str(options.get("reviewed_by") or ""),
        max_targets=_target_limit(configured_max_targets),
        timeout=str(options.get("timeout") or "10min"),
        cancel_check=_cancel_check,
    )
    result_payload = asdict(backfill_result)
    enqueue_live_progress(
        run_id=backfill_result.run_id,
        importer="address-strict-source-backfill",
        status="succeeded",
        phase="address strict source backfill",
        unit="target",
        total=backfill_result.target_count,
        done=backfill_result.target_count,
        pct=100,
        message="strict address source evidence backfilled",
        **{
            key: field_value
            for key, field_value in result_payload.items()
            if key not in {"run_id", "status"}
        },
    )
    return result_payload


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
    task_options_by_name = {
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
            task_options_by_name,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_strict_source_backfill({}, task_options_by_name)


async def process_address_numeric_grid_alias_revoke(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Process one controlled, one-way alias revocation."""
    options = task if isinstance(task, dict) else {}
    await raise_if_cancelled(ctx, options)
    revoke_result = await revoke_numeric_grid_alias(
        source_address_key=str(options.get("source_address_key") or ""),
        expected_target_address_key=str(
            options.get("expected_target_address_key") or ""
        ),
        reason=str(options.get("reason") or ""),
        reviewed_by=str(options.get("reviewed_by") or ""),
        timeout=str(options.get("timeout") or "30s"),
    )
    result_payload = asdict(revoke_result)
    enqueue_live_progress(
        run_id=revoke_result.run_id,
        importer="address-numeric-grid-alias-revoke",
        status="succeeded",
        phase="address numeric-grid alias revoke",
        unit="alias",
        total=1,
        done=1,
        pct=100,
        message="numeric-grid alias revoked; full artifact rebuild required",
        **{
            key: field_value
            for key, field_value in result_payload.items()
            if key not in {"run_id", "status"}
        },
    )
    return result_payload


async def run_address_alias_revoke_command(
    *,
    source_address_key: str,
    expected_target_address_key: str,
    reason: str,
    reviewed_by: str,
    timeout: str = "30s",
    enqueue: bool = False,
) -> dict[str, Any] | None:
    """Run inline or enqueue one exact alias revocation."""
    task_options_by_name = {
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
            task_options_by_name,
            _queue_name=ADDRESS_ALIAS_QUEUE_NAME,
        )
        return None
    return await process_address_numeric_grid_alias_revoke(
        {},
        task_options_by_name,
    )
