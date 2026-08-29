"""Scoped environment projection for PTG control worker lanes."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

from process.ptg_parts.config import (
    PTG2_FILE_PROCESS_CONCURRENCY_ENV,
    PTG2_MANIFEST_MERGE_CHUNK_BYTES_ENV,
    PTG2_MANIFEST_MERGE_SORT_WORKERS_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_PARSE_IN_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS_ENV,
    PTG2_RUST_PROVIDER_REF_QUEUE_ENV,
    PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES_ENV,
    PTG2_RUST_PROVIDER_REF_WORKERS_ENV,
    PTG2_RUST_PROVIDER_REFS_IN_WORKERS_ENV,
    PTG2_RUST_RAPIDGZIP_THREADS_ENV,
    PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
    PTG2_RUST_TOP_LEVEL_BYTE_SCAN_ENV,
    PTG2_RUST_RAW_CHUNK_BYTES_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
)


@contextmanager
def _ptg_lane_environment(params: dict[str, Any]):
    lane_environment_by_name = {
        PTG2_RUST_WORKERS_ENV: _optional_env_value(params.get("_scanner_rust_workers")),
        PTG2_RUST_RAPIDGZIP_THREADS_ENV: _optional_env_value(
            params.get("_scanner_rapidgzip_threads")
        ),
        PTG2_RUST_PARSE_IN_WORKERS_ENV: _bool_env_value(params.get("_scanner_parse_in_workers")),
        PTG2_RUST_TOP_LEVEL_BYTE_SCAN_ENV: _bool_env_value(params.get("_scanner_top_level_byte_scan")),
        PTG2_RUST_WORK_QUEUE_ENV: _optional_env_value(params.get("_scanner_work_queue")),
        PTG2_RUST_EVENT_QUEUE_ENV: _optional_env_value(params.get("_scanner_event_queue")),
        PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV: _optional_env_value(
            params.get("_scanner_split_negotiated_rates")
        ),
        PTG2_RUST_RAW_CHUNK_BYTES_ENV: _optional_env_value(params.get("_scanner_raw_chunk_bytes")),
        PTG2_RUST_PROVIDER_REFS_IN_WORKERS_ENV: _bool_env_value(
            params.get("_scanner_provider_refs_in_workers")
        ),
        PTG2_RUST_PROVIDER_REF_WORKERS_ENV: _optional_env_value(params.get("_scanner_provider_ref_workers")),
        PTG2_RUST_PROVIDER_REF_QUEUE_ENV: _optional_env_value(params.get("_scanner_provider_ref_queue")),
        PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS_ENV: _optional_env_value(
            params.get("_scanner_provider_ref_chunk_items")
        ),
        PTG2_RUST_PROVIDER_REF_RAW_CHUNK_BYTES_ENV: _optional_env_value(
            params.get("_scanner_provider_ref_raw_chunk_bytes")
        ),
        PTG2_MANIFEST_MERGE_CHUNK_BYTES_ENV: _optional_env_value(
            params.get("_manifest_merge_chunk_bytes")
        ),
        PTG2_MANIFEST_MERGE_SORT_WORKERS_ENV: _optional_env_value(
            params.get("_manifest_merge_sort_workers")
        ),
        PTG2_FILE_PROCESS_CONCURRENCY_ENV: _optional_env_value(
            params.get("_file_process_concurrency")
        ),
    }
    previous_environment_by_name: dict[str, str | None] = {}
    try:
        for name, environment_value in lane_environment_by_name.items():
            if environment_value is None:
                continue
            previous_environment_by_name[name] = os.environ.get(name)
            os.environ[name] = environment_value
        yield
    finally:
        for name, environment_value in previous_environment_by_name.items():
            if environment_value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = environment_value


def _optional_env_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _bool_env_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return (
        "true"
        if str(value).strip().lower() in {"1", "true", "yes", "on"}
        else "false"
    )


def _string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else None
    if isinstance(value, (list, tuple)):
        normalized_values = [
            str(item).strip() for item in value if str(item).strip()
        ]
        return normalized_values or None
    return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def assert_expected_ptg_lane(params: dict[str, Any]) -> None:
    """Reject a task routed to a different configured worker lane."""

    expected_queue = str(params.get("_expected_queue") or "").strip()
    active_queue = os.getenv("HLTHPRT_ACTIVE_WORKER_QUEUE", "").strip()
    if expected_queue and active_queue and expected_queue != active_queue:
        raise RuntimeError(
            f"PTG payload expected {expected_queue}, but active worker queue is {active_queue}"
        )
    expected_class = str(params.get("_expected_worker_class") or "").strip()
    active_class = os.getenv("HLTHPRT_ACTIVE_WORKER_CLASS", "").strip()
    if expected_class and active_class and expected_class != active_class:
        raise RuntimeError(
            f"PTG payload expected {expected_class}, but active worker class is {active_class}"
        )
