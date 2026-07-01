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
    PTG2_RUST_REQUIRE_RELEASE_ENV,
    PTG2_RUST_SCANNER_BIN_ENV,
    PTG2_RUST_SPLIT_NEGOTIATED_RATES_ENV,
    PTG2_RUST_WORK_QUEUE_ENV,
    PTG2_RUST_WORKERS_ENV,
    _env_bool,
    _env_int,
)
from process.ptg_parts.domain import PTG2_CONFIDENCE_TIC_RATE_NPI_TIN
from process.ptg_parts.live_progress import current_live_progress_context, write_live_progress
from process.ptg_parts.screen import _emit_screen_line

logger = logging.getLogger(__name__)

_SCANNER_PROGRESS_PREFIX = "PTG2_SCANNER_PROGRESS\t"
_SCANNER_PROGRESS_BASE_KEYS = {
    "path",
    "compressed_bytes",
    "total_bytes",
    "percent",
    "compressed_mib_s",
    "elapsed_seconds",
    "eta_seconds",
    "objects",
    "done",
}


def _ptg2_scanner_binary_profile(path: Path) -> str:
    parts = set(path.parts)
    if "release" in parts:
        return "release"
    if "debug" in parts:
        return "debug"
    return "custom"


def _json_loads(value: str | bytes | bytearray) -> Any:
    if orjson is not None and _env_bool(PTG2_FAST_JSON_LOADS_ENV, True):
        return orjson.loads(value)
    return json.loads(value)


def _ptg2_rust_scanner_binary() -> Path | None:
    configured = os.getenv(PTG2_RUST_SCANNER_BIN_ENV)
    require_release = _env_bool(PTG2_RUST_REQUIRE_RELEASE_ENV, False)
    if configured:
        candidate = Path(configured)
        if candidate.exists() and os.access(candidate, os.X_OK):
            profile = _ptg2_scanner_binary_profile(candidate)
            if require_release and profile == "debug":
                logger.warning(
                    "Ignoring PTG2 Rust scanner debug binary because %s is enabled: %s",
                    PTG2_RUST_REQUIRE_RELEASE_ENV,
                    candidate,
                )
                return None
            return candidate
        return None
    root = Path(__file__).resolve().parents[2]
    candidates = [
        root / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner",
        root / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner",
    ]
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            profile = _ptg2_scanner_binary_profile(candidate)
            if require_release and profile == "debug":
                logger.warning(
                    "Ignoring PTG2 Rust scanner debug binary because %s is enabled: %s",
                    PTG2_RUST_REQUIRE_RELEASE_ENV,
                    candidate,
                )
                continue
            return candidate
    return None


def _log_ptg2_rust_scanner_binary(binary: Path) -> None:
    logger.info(
        "PTG2 Rust scanner binary selected path=%s profile=%s require_release=%s",
        binary,
        _ptg2_scanner_binary_profile(binary),
        _env_bool(PTG2_RUST_REQUIRE_RELEASE_ENV, False),
    )


def _scanner_error_message(prefix: str, return_code: int, stderr_tail: list[str]) -> str:
    return f"{prefix} failed with {_scanner_return_code_label(return_code)}: {chr(10).join(stderr_tail)[-1000:]}"


def _scanner_progress_fields(line: str) -> dict[str, str] | None:
    if not line.startswith(_SCANNER_PROGRESS_PREFIX):
        return None
    fields: dict[str, str] = {}
    for part in line[len(_SCANNER_PROGRESS_PREFIX):].split("\t"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            fields[key] = value.strip()
    return fields


def _coerce_progress_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_progress_float(value: Any) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _format_progress_mib(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value / (1024 ** 2):.1f} MiB"


def _emit_scanner_live_progress(
    line: str,
    *,
    phase: str,
    live_progress_context: dict[str, Any] | None = None,
) -> None:
    fields = _scanner_progress_fields(line)
    if not fields:
        return
    compressed_bytes = _coerce_progress_int(fields.get("compressed_bytes"))
    total_bytes = _coerce_progress_int(fields.get("total_bytes"))
    percent = _coerce_progress_float(fields.get("percent"))
    eta_seconds = _coerce_progress_float(fields.get("eta_seconds"))
    elapsed_seconds = _coerce_progress_float(fields.get("elapsed_seconds"))
    compressed_mib_s = _coerce_progress_float(fields.get("compressed_mib_s"))
    object_count = _coerce_progress_int(fields.get("objects"))
    scanner_objects = {
        key: count
        for key, value in fields.items()
        if key not in _SCANNER_PROGRESS_BASE_KEYS and (count := _coerce_progress_int(value)) is not None
    }
    context = {
        key: value
        for key, value in (live_progress_context or {}).items()
        if value not in (None, "")
    }
    message_parts = [
        f"{phase} {percent:.2f}%" if percent is not None else phase,
        f"read {_format_progress_mib(compressed_bytes)} of {_format_progress_mib(total_bytes)}",
    ]
    if object_count is not None:
        message_parts.append(f"objects={object_count}")
    if compressed_mib_s is not None:
        message_parts.append(f"{compressed_mib_s:.2f} MiB/s")
    payload: dict[str, Any] = {
        **context,
        "phase": phase,
        "unit": "compressed_bytes",
        "done": compressed_bytes,
        "total": total_bytes,
        "pct": percent,
        "eta_seconds": eta_seconds,
        "elapsed_seconds": elapsed_seconds,
        "message": ", ".join(message_parts),
        "source": "ptg2-scanner-progress",
        "confidence": "live",
        "scanner_path": fields.get("path"),
        "scanner_done": str(fields.get("done") or "").lower() == "true",
        "scanner_objects": scanner_objects or None,
    }
    try:
        write_live_progress(**payload)
    except Exception:
        logger.debug("Failed to write PTG2 scanner live progress", exc_info=True)


def _iter_top_level_object_bytes_rust(
    path: str | Path,
    array_names: set[str],
    *,
    live_progress_context: dict[str, Any] | None = None,
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust scanner is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    _log_ptg2_rust_scanner_binary(binary)
    if live_progress_context is None:
        live_progress_context = current_live_progress_context()
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
                if line.startswith(_SCANNER_PROGRESS_PREFIX):
                    _emit_screen_line(line)
                    logger.info(line)
                    _emit_scanner_live_progress(
                        line,
                        phase="PTG scanner",
                        live_progress_context=live_progress_context,
                    )
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
    manifest_serving_copy_path: str | Path | None = None,
    manifest_provider_forward_sidecar_path: str | Path | None = None,
    manifest_provider_inverted_sidecar_path: str | Path | None = None,
    manifest_provider_npi_sidecar_path: str | Path | None = None,
    manifest_price_forward_sidecar_path: str | Path | None = None,
    manifest_price_atom_copy_path: str | Path | None = None,
    manifest_provider_group_member_copy_path: str | Path | None = None,
    source_network_names: list[str] | tuple[str, ...] | set[str] | None = None,
    manifest_only: bool | None = None,
    live_progress_context: dict[str, Any] | None = None,
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust compact serving is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    _log_ptg2_rust_scanner_binary(binary)
    if live_progress_context is None:
        live_progress_context = current_live_progress_context()
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
    if manifest_serving_copy_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_SERVING_COPY_PATH"] = str(manifest_serving_copy_path)
    if manifest_provider_forward_sidecar_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PROVIDER_FORWARD_SIDECAR_PATH"] = str(manifest_provider_forward_sidecar_path)
    if manifest_provider_inverted_sidecar_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PROVIDER_INVERTED_SIDECAR_PATH"] = str(manifest_provider_inverted_sidecar_path)
    if manifest_provider_npi_sidecar_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PROVIDER_NPI_SIDECAR_PATH"] = str(manifest_provider_npi_sidecar_path)
    if manifest_price_forward_sidecar_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PRICE_FORWARD_SIDECAR_PATH"] = str(manifest_price_forward_sidecar_path)
    if manifest_price_atom_copy_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH"] = str(manifest_price_atom_copy_path)
    if manifest_provider_group_member_copy_path is not None:
        env["HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH"] = str(manifest_provider_group_member_copy_path)
    normalized_source_network_names = sorted(
        {str(value).strip() for value in (source_network_names or []) if str(value or "").strip()}
    )
    if normalized_source_network_names:
        env["HLTHPRT_PTG2_SOURCE_NETWORK_NAMES_JSON"] = json.dumps(normalized_source_network_names)
    if manifest_only is not None:
        env["HLTHPRT_PTG2_MANIFEST_ONLY"] = "true" if manifest_only else "false"
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
                    line.startswith(_SCANNER_PROGRESS_PREFIX)
                    or line.startswith("PTG2_DEDUPE_SUMMARY\t")
                    or line.startswith("PTG2_SCANNER_WORKER_FAILED\t")
                ):
                    _emit_screen_line(line)
                    logger.info(line)
                    if line.startswith(_SCANNER_PROGRESS_PREFIX):
                        _emit_scanner_live_progress(
                            line,
                            phase="compact-serving scanner",
                            live_progress_context=live_progress_context,
                        )
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
    manifest_serving_copy_path: str | Path | None = None,
    manifest_provider_forward_sidecar_path: str | Path | None = None,
    manifest_provider_inverted_sidecar_path: str | Path | None = None,
    manifest_provider_npi_sidecar_path: str | Path | None = None,
    manifest_price_forward_sidecar_path: str | Path | None = None,
    manifest_price_atom_copy_path: str | Path | None = None,
    manifest_provider_group_member_copy_path: str | Path | None = None,
    source_network_names: list[str] | tuple[str, ...] | set[str] | None = None,
    manifest_only: bool | None = None,
):
    live_progress_context = current_live_progress_context()
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
        manifest_serving_copy_path=manifest_serving_copy_path,
        manifest_provider_forward_sidecar_path=manifest_provider_forward_sidecar_path,
        manifest_provider_inverted_sidecar_path=manifest_provider_inverted_sidecar_path,
        manifest_provider_npi_sidecar_path=manifest_provider_npi_sidecar_path,
        manifest_price_forward_sidecar_path=manifest_price_forward_sidecar_path,
        manifest_price_atom_copy_path=manifest_price_atom_copy_path,
        manifest_provider_group_member_copy_path=manifest_provider_group_member_copy_path,
        source_network_names=source_network_names,
        manifest_only=manifest_only,
        live_progress_context=live_progress_context,
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
                try:
                    await asyncio.to_thread(close)
                except ValueError as exc:
                    if "generator already executing" not in str(exc):
                        raise
            reader_thread.join(timeout=5)


def _scanner_return_code_label(return_code: int) -> str:
    if return_code >= 0:
        return f"exit code {return_code}"
    signal_number = -return_code
    try:
        import signal

        signal_name = signal.Signals(signal_number).name
    except ValueError:
        return f"signal {signal_number}"
    return f"signal {signal_number} ({signal_name})"
