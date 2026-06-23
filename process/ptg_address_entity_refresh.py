# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Chained PTG address and entity-address refresh workflow."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from arq import create_pool

from process.control_lifecycle import mark_control_run
from process.entity_address_unified import (
    ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL,
    ENTITY_ADDRESS_REFRESH_MODES,
)
from process.ptg_address import (
    PTG_ADDRESS_REFRESH_MODE_PARTIAL,
    PTG_ADDRESS_REFRESH_MODES,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

PTG_ADDRESS_ENTITY_REFRESH_QUEUE_NAME = "arq:EntityAddressUnified"
DEFAULT_PTG_REFRESH_MODE = PTG_ADDRESS_REFRESH_MODE_PARTIAL
DEFAULT_ENTITY_REFRESH_MODE = ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL
ptg_address = import_module("process.ptg_address")
entity_address_unified = import_module("process.entity_address_unified")


def _clean_optional(value: Any) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _control_run_id(ctx: dict[str, Any], task: dict[str, Any]) -> str:
    context = ctx.get("context") if isinstance(ctx.get("context"), dict) else {}
    return str(task.get("run_id") or ctx.get("control_run_id") or context.get("control_run_id") or "").strip()


def _workflow_source_key(task: dict[str, Any]) -> str | None:
    return _clean_optional(task.get("source_key") or task.get("ptg_source_key"))


def _refresh_mode(task: dict[str, Any], *, key: str, default: str, allowed: set[str]) -> str:
    raw = task.get(key)
    value = str(raw or default).strip().lower().replace("_", "-")
    if value in allowed:
        return value
    raise ValueError(f"Unsupported {key} {raw!r}; expected one of {sorted(allowed)}")


def _ptg_task(task: dict[str, Any], *, source_key: str | None, snapshot_id: str | None, test_mode: bool) -> dict[str, Any]:
    refresh_mode = _refresh_mode(
        task,
        key="ptg_refresh_mode",
        default=DEFAULT_PTG_REFRESH_MODE,
        allowed=PTG_ADDRESS_REFRESH_MODES,
    )
    payload: dict[str, Any] = {
        "test_mode": test_mode,
        "refresh_mode": refresh_mode,
    }
    if source_key:
        payload["source_key"] = source_key
    if snapshot_id:
        payload["snapshot_id"] = snapshot_id
    return payload


def _entity_task(task: dict[str, Any], *, source_key: str | None, test_mode: bool) -> dict[str, Any]:
    refresh_mode = _refresh_mode(
        task,
        key="entity_refresh_mode",
        default=DEFAULT_ENTITY_REFRESH_MODE,
        allowed=ENTITY_ADDRESS_REFRESH_MODES,
    )
    payload: dict[str, Any] = {
        "test_mode": test_mode,
        "refresh_mode": refresh_mode,
    }
    if refresh_mode == ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL and source_key:
        payload["ptg_source_key"] = source_key
    if task.get("limit_per_source") not in (None, ""):
        payload["limit_per_source"] = max(int(task["limit_per_source"]), 0)
    if task.get("publish") not in (None, ""):
        payload["publish"] = _coerce_bool(task["publish"])
    if task.get("skip_publish") not in (None, ""):
        payload["skip_publish"] = _coerce_bool(task["skip_publish"])
    return payload


def _validate_child_tasks(ptg_task: dict[str, Any], entity_task: dict[str, Any], *, source_key: str | None) -> None:
    if ptg_task["refresh_mode"] == PTG_ADDRESS_REFRESH_MODE_PARTIAL and not source_key:
        raise RuntimeError("ptg-address-entity-refresh requires source_key for PTG partial refresh.")
    if entity_task["refresh_mode"] == ENTITY_ADDRESS_REFRESH_MODE_PTG_PARTIAL and not source_key:
        raise RuntimeError("ptg-address-entity-refresh requires source_key for entity PTG partial refresh.")


async def _run_child(module: Any, task: dict[str, Any]) -> dict[str, Any]:
    child_ctx: dict[str, Any] = {}
    await module.startup(child_ctx)
    await module.process_data(child_ctx, task)
    await module.shutdown(child_ctx)
    return child_ctx.get("context") if isinstance(child_ctx.get("context"), dict) else {}


async def process_data(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    task = task or {}
    run_id = _control_run_id(ctx, task)
    test_mode = _coerce_bool(task.get("test_mode", task.get("test", False)))
    source_key = _workflow_source_key(task)
    snapshot_id = _clean_optional(task.get("snapshot_id"))
    ptg_payload = _ptg_task(task, source_key=source_key, snapshot_id=snapshot_id, test_mode=test_mode)
    entity_payload = _entity_task(task, source_key=source_key, test_mode=test_mode)
    _validate_child_tasks(ptg_payload, entity_payload, source_key=source_key)

    await mark_control_run(
        run_id,
        status="running",
        phase_detail="ptg-address refresh running",
        progress_message="ptg-address refresh running",
        progress={
            "unit": "step",
            "done": 0,
            "total": 2,
            "pct": 0,
            "message": "ptg-address refresh running",
            "phase": "ptg-address refresh running",
        },
    )
    ptg_context = await _run_child(ptg_address, ptg_payload)

    await mark_control_run(
        run_id,
        status="running",
        phase_detail="entity-address-unified refresh running",
        progress_message="entity-address-unified refresh running",
        progress={
            "unit": "step",
            "done": 1,
            "total": 2,
            "pct": 50,
            "message": "entity-address-unified refresh running",
            "phase": "entity-address-unified refresh running",
        },
    )
    entity_context = await _run_child(entity_address_unified, entity_payload)

    entity_rows = int(entity_context.get("staged_rows") or 0)
    return {
        "rows": entity_rows,
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "ptg_address": {
            "refresh_mode": ptg_context.get("refresh_mode") or ptg_payload["refresh_mode"],
            "source_keys": ptg_context.get("source_keys") or ([source_key] if source_key else []),
            "snapshot_ids": ptg_context.get("snapshot_ids") or ([snapshot_id] if snapshot_id else []),
            "staged_rows": int(ptg_context.get("staged_rows") or 0),
            "partial_refresh_reused_rows": int(ptg_context.get("partial_refresh_reused_rows") or 0),
            "partial_refresh_patched_rows": int(ptg_context.get("partial_refresh_patched_rows") or 0),
            "partial_refresh_patch_publish": bool(ptg_context.get("partial_refresh_patch_publish")),
        },
        "entity_address_unified": {
            "refresh_mode": entity_context.get("refresh_mode") or entity_payload["refresh_mode"],
            "partial_ptg_source_keys": entity_context.get("partial_ptg_source_keys") or ([source_key] if source_key else []),
            "staged_rows": entity_rows,
            "partial_ptg_reused_rows": int(entity_context.get("partial_ptg_reused_rows") or 0),
            "partial_ptg_patched_rows": int(entity_context.get("partial_ptg_patched_rows") or 0),
            "support_counts": entity_context.get("support_counts") or {},
        },
    }


async def main(
    test_mode: bool = False,
    source_key: str | None = None,
    snapshot_id: str | None = None,
    ptg_refresh_mode: str | None = None,
    entity_refresh_mode: str | None = None,
    limit_per_source: int | None = None,
    publish: bool | None = None,
):
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload: dict[str, Any] = {"test_mode": bool(test_mode)}
    if source_key:
        payload["source_key"] = source_key
    if snapshot_id:
        payload["snapshot_id"] = snapshot_id
    if ptg_refresh_mode:
        payload["ptg_refresh_mode"] = ptg_refresh_mode
    if entity_refresh_mode:
        payload["entity_refresh_mode"] = entity_refresh_mode
    if limit_per_source is not None:
        payload["limit_per_source"] = max(int(limit_per_source), 0)
    if publish is not None:
        payload["publish"] = bool(publish)
    await redis.enqueue_job(
        "control_single_job_start",
        {
            "importer": "ptg-address-entity-refresh",
            "family": "provider",
            "target_module": "process.ptg_address_entity_refresh",
            "target_function": "process_data",
            "call_style": "ctx_task",
            "run_shutdown": False,
            "task": payload,
        },
        _queue_name=PTG_ADDRESS_ENTITY_REFRESH_QUEUE_NAME,
        _max_tries=1,
    )
