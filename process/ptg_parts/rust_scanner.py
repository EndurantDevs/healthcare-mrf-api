# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Python subprocess bridge for the PTG2 Rust scanner."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import subprocess
import threading
from pathlib import Path
from typing import Any

try:
    import orjson
except ImportError:  # pragma: no cover - optional acceleration
    orjson = None

from process.ptg_parts.config import (
    PTG2_DEFAULT_RUST_EVENT_QUEUE,
    PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES,
    PTG2_DEFAULT_RUST_WORK_QUEUE,
    PTG2_DEFAULT_RUST_WORKERS,
    PTG2_FAST_JSON_LOADS_ENV,
    PTG2_RUST_EVENT_QUEUE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
    _env_bool,
    _env_int,
)
from process.ptg_parts.domain import PTG2_CONFIDENCE_TIC_RATE_NPI_TIN
from process.ptg_parts.screen import _emit_screen_line

logger = logging.getLogger(__name__)


def _json_loads(value: str | bytes | bytearray) -> Any:
    if orjson is not None and _env_bool(PTG2_FAST_JSON_LOADS_ENV, True):
        return orjson.loads(value)
    return json.loads(value)


def _ptg2_rust_scanner_binary() -> Path | None:
    configured = os.getenv(PTG2_RUST_SCANNER_BIN_ENV)
    candidates = []
    if configured:
        candidates.append(Path(configured))
    root = Path(__file__).resolve().parents[2]
    candidates.extend(
        [
            root / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner",
            root / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner",
        ]
    )
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _scanner_error_message(prefix: str, return_code: int, stderr_tail: list[str]) -> str:
    return f"{prefix} failed with exit code {return_code}: {chr(10).join(stderr_tail)[-1000:]}"


def _iter_top_level_object_bytes_rust(
    path: str | Path,
    array_names: set[str],
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust scanner is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    process = subprocess.Popen(
        [str(binary), str(path), *sorted(array_names)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    stderr_tail: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:

        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_tail.append(line)
                if len(stderr_tail) > 20:
                    del stderr_tail[:-20]
                if line.startswith("PTG2_SCANNER_PROGRESS\t"):
                    _emit_screen_line(line)
                    logger.info(line)
                else:
                    logger.warning("PTG2 Rust scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-scanner-stderr",
            daemon=True,
        )
        stderr_thread.start()
    terminated_by_consumer = False
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust scanner frame header: {header!r}") from exc
            payload = process.stdout.read(payload_len)
            if len(payload) != payload_len:
                raise RuntimeError("PTG2 Rust scanner ended mid-frame")
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust scanner frame trailer")
            yield name_bytes.decode("utf-8"), payload
    finally:
        if process.poll() is None:
            terminated_by_consumer = True
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join(timeout=2)
        if return_code != 0 and not terminated_by_consumer:
            raise RuntimeError(_scanner_error_message("PTG2 Rust scanner", return_code, stderr_tail))


def _iter_compact_serving_records_rust(
    path: str | Path,
    *,
    snapshot_id: str,
    plan_id: str,
    plan_month_id: str,
    source_trace_set_hash: str,
    confidence_code: str = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    compact_copy_path: str | Path | None = None,
    procedure_copy_path: str | Path | None = None,
    price_code_set_copy_path: str | Path | None = None,
    price_atom_copy_path: str | Path | None = None,
    price_set_entry_copy_path: str | Path | None = None,
    provider_set_copy_path: str | Path | None = None,
    provider_group_member_copy_path: str | Path | None = None,
    provider_set_component_copy_path: str | Path | None = None,
    provider_set_entry_copy_path: str | Path | None = None,
    provider_entry_component_copy_path: str | Path | None = None,
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust compact serving is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": snapshot_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": plan_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": plan_month_id,
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": source_trace_set_hash,
        "HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE": confidence_code,
    }
    if compact_copy_path is not None:
        env["HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH"] = str(compact_copy_path)
    if procedure_copy_path is not None:
        env["HLTHPRT_PTG2_PROCEDURE_COPY_PATH"] = str(procedure_copy_path)
    if price_code_set_copy_path is not None:
        env["HLTHPRT_PTG2_PRICE_CODE_SET_COPY_PATH"] = str(price_code_set_copy_path)
    if price_atom_copy_path is not None:
        env["HLTHPRT_PTG2_PRICE_ATOM_COPY_PATH"] = str(price_atom_copy_path)
    if price_set_entry_copy_path is not None:
        env["HLTHPRT_PTG2_PRICE_SET_ENTRY_COPY_PATH"] = str(price_set_entry_copy_path)
    if provider_set_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH"] = str(provider_set_copy_path)
    if provider_group_member_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH"] = str(provider_group_member_copy_path)
    if provider_set_component_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_SET_COMPONENT_COPY_PATH"] = str(provider_set_component_copy_path)
    if provider_set_entry_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_SET_ENTRY_COPY_PATH"] = str(provider_set_entry_copy_path)
    if provider_entry_component_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_ENTRY_COMPONENT_COPY_PATH"] = str(provider_entry_component_copy_path)
    env.setdefault(PTG2_RUST_WORKERS_ENV, str(PTG2_DEFAULT_RUST_WORKERS))
    env.setdefault(PTG2_RUST_WORK_QUEUE_ENV, str(PTG2_DEFAULT_RUST_WORK_QUEUE))
    env.setdefault(PTG2_RUST_EVENT_QUEUE_ENV, str(PTG2_DEFAULT_RUST_EVENT_QUEUE))
    env.setdefault(PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV, str(PTG2_DEFAULT_RUST_SPLIT_NEGOTIATED_RATES))
    process = subprocess.Popen(
        [str(binary), "--compact-serving", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert process.stdout is not None
    stderr_tail: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:

        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_tail.append(line)
                if len(stderr_tail) > 20:
                    del stderr_tail[:-20]
                if (
                    line.startswith("PTG2_SCANNER_PROGRESS\t")
                    or line.startswith("PTG2_DEDUPE_SUMMARY\t")
                    or line.startswith("PTG2_SCANNER_WORKER_FAILED\t")
                ):
                    _emit_screen_line(line)
                    logger.info(line)
                else:
                    logger.warning("PTG2 Rust compact scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-compact-stderr",
            daemon=True,
        )
        stderr_thread.start()
    terminated_by_consumer = False
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust compact frame header: {header!r}") from exc
            payload = process.stdout.read(payload_len)
            if len(payload) != payload_len:
                raise RuntimeError("PTG2 Rust compact scanner ended mid-frame")
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust compact scanner frame trailer")
            yield name_bytes.decode("utf-8"), _json_loads(payload)
    finally:
        if process.poll() is None:
            terminated_by_consumer = True
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join(timeout=2)
        if return_code != 0 and not terminated_by_consumer:
            raise RuntimeError(_scanner_error_message("PTG2 Rust compact scanner", return_code, stderr_tail))


async def _aiter_compact_serving_records_rust(
    path: str | Path,
    *,
    snapshot_id: str,
    plan_id: str,
    plan_month_id: str,
    source_trace_set_hash: str,
    confidence_code: str = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    compact_copy_path: str | Path | None = None,
    procedure_copy_path: str | Path | None = None,
    price_code_set_copy_path: str | Path | None = None,
    price_atom_copy_path: str | Path | None = None,
    price_set_entry_copy_path: str | Path | None = None,
    provider_set_copy_path: str | Path | None = None,
    provider_group_member_copy_path: str | Path | None = None,
    provider_set_component_copy_path: str | Path | None = None,
    provider_set_entry_copy_path: str | Path | None = None,
    provider_entry_component_copy_path: str | Path | None = None,
):
    iterator = _iter_compact_serving_records_rust(
        path,
        snapshot_id=snapshot_id,
        plan_id=plan_id,
        plan_month_id=plan_month_id,
        source_trace_set_hash=source_trace_set_hash,
        confidence_code=confidence_code,
        compact_copy_path=compact_copy_path,
        procedure_copy_path=procedure_copy_path,
        price_code_set_copy_path=price_code_set_copy_path,
        price_atom_copy_path=price_atom_copy_path,
        price_set_entry_copy_path=price_set_entry_copy_path,
        provider_set_copy_path=provider_set_copy_path,
        provider_group_member_copy_path=provider_group_member_copy_path,
        provider_set_component_copy_path=provider_set_component_copy_path,
        provider_set_entry_copy_path=provider_set_entry_copy_path,
        provider_entry_component_copy_path=provider_entry_component_copy_path,
    )
    event_queue: queue.Queue[Any] = queue.Queue(
        maxsize=max(_env_int(PTG2_RUST_EVENT_QUEUE_ENV, PTG2_DEFAULT_RUST_EVENT_QUEUE), 1)
    )
    sentinel = object()

    def read_records() -> None:
        try:
            for item in iterator:
                event_queue.put(item)
        except BaseException as exc:
            event_queue.put(exc)
        finally:
            event_queue.put(sentinel)

    reader_thread = threading.Thread(
        target=read_records,
        name="ptg2-rust-compact-stdout",
        daemon=True,
    )
    reader_thread.start()
    try:
        while True:
            item = await asyncio.to_thread(event_queue.get)
            if item is sentinel:
                break
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        if reader_thread.is_alive():
            close = getattr(iterator, "close", None)
            if close is not None:
                await asyncio.to_thread(close)
            reader_thread.join(timeout=5)
