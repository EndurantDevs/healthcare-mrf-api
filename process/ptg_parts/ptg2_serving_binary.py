# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL-native PTG2 serving block writer.

The existing serving sidecars are efficient binary files.  This module stores
the same hot serving relationships as block-keyed PostgreSQL rows so API pods
can serve directly from PostgreSQL without a filesystem artifact cache.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import mmap
import os
import re
import subprocess
import struct
import tempfile
import time
import uuid
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterable, Callable, Iterable, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_VERSION,
    PTG2ManifestArtifactError,
)
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary

PTG2_SERVING_BINARY_TABLE_FORMAT = "ptg2_serving_binary_blocks_v1"
PTG2_SERVING_BINARY_BY_CODE_KIND = "by_code"
PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND = "by_code_grouped"
PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND = "by_code_price_dictionary"
PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND = "provider_set_count_dictionary"
PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND = "by_provider_set"
PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND = "by_provider_set_price_dictionary"
PTG2_SERVING_BINARY_PRICE_SET_ATOMS_KIND = "price_set_atoms"
PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_KIND = "price_set_atoms_by_id"
PTG2_SERVING_BINARY_BLOCK_BYTES_ENV = "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES"
PTG2_SERVING_BINARY_COPY_RECORDS_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COPY_RECORDS"
PTG2_SERVING_BINARY_RUST_ENV = "HLTHPRT_PTG2_SERVING_BINARY_RUST"
PTG2_SERVING_BINARY_STREAM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_STREAM"
PTG2_SERVING_BINARY_STREAM_TASKS_ENV = "HLTHPRT_PTG2_SERVING_BINARY_STREAM_TASKS"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES"
PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COPY_WORK_MEM"
PTG2_SERVING_BINARY_COLUMNS = [
    "artifact_kind",
    "block_key",
    "block_no",
    "entry_count",
    "payload",
    "payload_compression",
    "raw_payload_bytes",
]
_DEFAULT_BLOCK_BYTES = 2 * 1024 * 1024
_DEFAULT_COPY_RECORDS = 2048
_DEFAULT_STREAM_TASKS = 2
_DEFAULT_COMPRESSION_MIN_BYTES = 128
_DEFAULT_COPY_WORK_MEM = "128MB"
_PRICE_SET_ATOM_BLOCK_SIZE = 1024
_PRICE_SET_ATOM_ID_BUCKETS = 256
_MEMBERSHIP_HEADER = struct.Struct("<8sIQ")
_DENSE_MEMBERSHIP_HEADER = struct.Struct("<8sIQQ")
_MEMBERSHIP_INDEX_RECORD = struct.Struct("<16sQI")
_DENSE_MEMBER_RECORD = struct.Struct("<I")
_STANDARD_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC}
_DENSE_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC}
_WORK_MEM_RE = re.compile(r"^[1-9][0-9]*(?:kB|MB|GB|TB)?$")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _StreamStageConfig:
    kind: str
    progress_step: str
    progress_done: int
    progress_total: int
    progress_message: str
    timing_key: str


@dataclass(frozen=True)
class _AtomMapStageProgress:
    progress_step: str
    done: int
    total: int
    message: str


def _elapsed_seconds(started_at: float) -> float:
    return round(time.monotonic() - started_at, 3)


def _serving_binary_block_bytes() -> int:
    try:
        return max(int(os.getenv(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, str(_DEFAULT_BLOCK_BYTES))), 64 * 1024)
    except ValueError:
        return _DEFAULT_BLOCK_BYTES


def _serving_binary_copy_records() -> int:
    try:
        return max(int(os.getenv(PTG2_SERVING_BINARY_COPY_RECORDS_ENV, str(_DEFAULT_COPY_RECORDS))), 1)
    except ValueError:
        return _DEFAULT_COPY_RECORDS


def _serving_binary_rust_enabled() -> bool:
    value = os.getenv(PTG2_SERVING_BINARY_RUST_ENV)
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _use_serving_binary_stream() -> bool:
    value = os.getenv(PTG2_SERVING_BINARY_STREAM_ENV)
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _serving_binary_stream_tasks() -> int:
    try:
        return max(int(os.getenv(PTG2_SERVING_BINARY_STREAM_TASKS_ENV, str(_DEFAULT_STREAM_TASKS))), 1)
    except ValueError:
        return _DEFAULT_STREAM_TASKS


def _serving_binary_payload_compression() -> str:
    value = os.getenv(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib").strip().lower()
    return value if value in {"none", "zlib"} else "zlib"


def _serving_binary_payload_compression_level() -> int:
    try:
        return min(max(int(os.getenv(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV, "6")), 0), 9)
    except ValueError:
        return 6


def _serving_binary_payload_compression_min_bytes() -> int:
    try:
        return max(int(os.getenv(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV, str(_DEFAULT_COMPRESSION_MIN_BYTES))), 0)
    except ValueError:
        return _DEFAULT_COMPRESSION_MIN_BYTES


def _serving_binary_copy_work_mem() -> str | None:
    value = os.getenv(PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, _DEFAULT_COPY_WORK_MEM).strip()
    if value.lower() in {"", "0", "none", "off", "false"}:
        return None
    if _WORK_MEM_RE.fullmatch(value):
        return value
    logger.warning("Ignoring unsafe %s=%r", PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV, value)
    return _DEFAULT_COPY_WORK_MEM


def _emit_progress(
    progress_callback: Callable[..., None] | None,
    publish_step: str,
    *,
    done: int,
    total: int,
    message: str,
    **details: Any,
) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(publish_step, done=done, total=total, message=message, **details)
    except Exception:
        logger.debug("Failed to emit PTG2 serving binary progress", exc_info=True)


def _short_index_name(seed: str, suffix: str) -> str:
    normalized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in seed.lower()).strip("_")
    digest = hashlib.sha1(f"{normalized}:{suffix}".encode("utf-8")).hexdigest()[:10]  # nosec B324
    suffix = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in suffix.lower()).strip("_")
    limit = 63 - len(digest) - len(suffix) - 2
    return "_".join(part for part in (normalized[: max(limit, 1)].strip("_"), suffix, digest) if part)


def _append_uvarint(buffer: bytearray, value: int) -> None:
    value = int(value)
    if value < 0:
        raise ValueError("uvarint cannot encode negative values")
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value)


def _normalize_128_id(value: Any) -> bytes:
    if isinstance(value, uuid.UUID):
        raw = value.bytes
    elif isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        text_value = str(value or "").strip().lower().replace("-", "")
        try:
            raw = bytes.fromhex(text_value)
        except ValueError as exc:
            raise ValueError("128-bit ids must be UUID values or 32-character hex strings") from exc
    if len(raw) != 16:
        raise ValueError(f"128-bit ids must be 16 bytes; got {len(raw)}")
    return raw


def _row_value(row: Any, key: str, position: int) -> Any:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping[key]
    if isinstance(row, dict):
        return row[key]
    return row[position]


def _price_dictionary_payload(price_set_values: list[bytes]) -> bytes:
    return b"".join(price_set_values)


def _price_set_atom_payload(price_key: int, atom_ids: tuple[bytes, ...]) -> bytes:
    payload = bytearray()
    _append_uvarint(payload, int(price_key))
    _append_uvarint(payload, len(atom_ids))
    for atom_id in atom_ids:
        payload.extend(_normalize_128_id(atom_id))
    return bytes(payload)


def _price_set_atom_id_bucket(price_set_id: bytes) -> int:
    return int.from_bytes(_normalize_128_id(price_set_id)[:4], "big") % _PRICE_SET_ATOM_ID_BUCKETS


def _price_set_atom_by_id_payload(price_set_id: bytes, atom_ids: tuple[bytes, ...]) -> bytes:
    payload = bytearray(_normalize_128_id(price_set_id))
    _append_uvarint(payload, len(atom_ids))
    for atom_id in atom_ids:
        payload.extend(_normalize_128_id(atom_id))
    return bytes(payload)


def _provider_count_dictionary_payload(provider_count_by_key: dict[int, int]) -> bytes:
    payload = bytearray()
    previous_provider_set_key = 0
    for provider_set_key in sorted(provider_count_by_key):
        _append_uvarint(payload, int(provider_set_key) - previous_provider_set_key)
        _append_uvarint(payload, int(provider_count_by_key[provider_set_key]))
        previous_provider_set_key = int(provider_set_key)
    return bytes(payload)


def _serving_binary_payload_for_storage(payload: bytes | bytearray | memoryview) -> tuple[bytes, str, int]:
    raw_payload = bytes(payload)
    if _serving_binary_payload_compression() != "zlib" or len(raw_payload) < _serving_binary_payload_compression_min_bytes():
        return raw_payload, "none", 0
    compressed_payload = zlib.compress(raw_payload, _serving_binary_payload_compression_level())
    if len(compressed_payload) >= len(raw_payload):
        return raw_payload, "none", 0
    return compressed_payload, "zlib", len(raw_payload)


def _serving_binary_record(
    kind: str,
    block_key: int,
    block_no: int,
    entry_count: int,
    payload: bytes | bytearray | memoryview,
) -> tuple[str, int, int, int, bytes, str, int]:
    stored_payload, compression, raw_payload_bytes = _serving_binary_payload_for_storage(payload)
    return (
        kind,
        int(block_key),
        int(block_no),
        int(entry_count),
        stored_payload,
        compression,
        raw_payload_bytes,
    )


class _PriceDictionary:
    def __init__(self) -> None:
        self._by_id: dict[bytes, int] = {}
        self.values: list[bytes] = []

    def key_for(self, value: Any) -> int:
        price_set_id = _normalize_128_id(value)
        price_key = self._by_id.get(price_set_id)
        if price_key is None:
            price_key = len(self.values)
            self._by_id[price_set_id] = price_key
            self.values.append(price_set_id)
        return price_key


async def _copy_serving_binary_records(
    *,
    schema_name: str,
    table_name: str,
    records: list[tuple[Any, ...]],
) -> None:
    if not records:
        return
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_records_to_table = getattr(driver_conn, "copy_records_to_table", None)
        if copy_records_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_records_to_table")
        await copy_records_to_table(
            table_name,
            schema_name=schema_name,
            columns=PTG2_SERVING_BINARY_COLUMNS,
            records=records,
        )


async def create_ptg2_serving_binary_table(*, schema_name: str, target_table: str) -> None:
    qualified_target = f"{_quote_ident(schema_name)}.{_quote_ident(target_table)}"
    await db.status(f"DROP TABLE IF EXISTS {qualified_target} CASCADE;")
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {qualified_target} (
            artifact_kind varchar(64) NOT NULL,
            block_key integer NOT NULL,
            block_no integer NOT NULL,
            entry_count integer NOT NULL,
            payload bytea NOT NULL,
            payload_compression varchar(16) NOT NULL DEFAULT 'none',
            raw_payload_bytes integer NOT NULL DEFAULT 0
        );
        """
    )


async def copy_ptg2_serving_binary_file_to_table(
    copy_path: Path,
    *,
    schema_name: str,
    target_table: str,
) -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        with copy_path.open("rb") as source:
            await copy_to_table(
                target_table,
                source=source,
                schema_name=schema_name,
                columns=PTG2_SERVING_BINARY_COLUMNS,
                format="text",
                delimiter="\t",
                null="\\N",
            )


async def finalize_ptg2_serving_binary_table(*, schema_name: str, target_table: str) -> dict[str, Any]:
    qualified_target = f"{_quote_ident(schema_name)}.{_quote_ident(target_table)}"
    index_name = _short_index_name(target_table, "kind_key_block_uidx")
    await db.status(
        f"""
        CREATE UNIQUE INDEX {_quote_ident(index_name)}
        ON {qualified_target} (artifact_kind, block_key, block_no);
        """
    )
    await db.status(f"ANALYZE {qualified_target};")
    storage = await db.first(
        f"""
        SELECT
            pg_total_relation_size(CAST(:qualified_target AS regclass)) AS total_bytes,
            pg_relation_size(CAST(:qualified_target AS regclass)) AS heap_bytes,
            pg_indexes_size(CAST(:qualified_target AS regclass)) AS index_bytes,
            COUNT(*) AS record_count,
            COALESCE(SUM(octet_length(payload)), 0) AS payload_bytes,
            COALESCE(SUM(NULLIF(raw_payload_bytes, 0)), 0) AS raw_payload_bytes,
            COALESCE(SUM(CASE WHEN payload_compression <> 'none' THEN 1 ELSE 0 END), 0) AS compressed_records,
            COALESCE(
                SUM(CASE WHEN payload_compression <> 'none' THEN raw_payload_bytes - octet_length(payload) ELSE 0 END),
                0
            ) AS compressed_saved_bytes
        FROM {qualified_target}
        """,
        qualified_target=f"{schema_name}.{target_table}",
    )
    storage_mapping = getattr(storage, "_mapping", None)
    return dict(storage_mapping) if storage_mapping is not None else dict(storage or {})


async def _copy_serving_binary_query_to_file(sql: str, output_path: Path) -> None:
    if output_path.parent:
        output_path.parent.mkdir(parents=True, exist_ok=True)
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_from_query = getattr(driver_conn, "copy_from_query", None)
        if copy_from_query is None:
            raise NotImplementedError("Active database driver does not expose copy_from_query")
        with output_path.open("wb") as output:
            work_mem = _serving_binary_copy_work_mem()
            if work_mem and hasattr(driver_conn, "transaction"):
                async with driver_conn.transaction():
                    await driver_conn.execute(f"SET LOCAL work_mem TO '{work_mem}'")
                    await copy_from_query(
                        sql,
                        output=output,
                        format="text",
                        delimiter="\t",
                        null="\\N",
                    )
                    return
            await copy_from_query(
                sql,
                output=output,
                format="text",
                delimiter="\t",
                null="\\N",
            )


def _parse_scanner_sized_frames(stdout: bytes) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    offset = 0
    length = len(stdout)
    while offset < length:
        line_end = stdout.find(b"\n", offset)
        if line_end < 0:
            break
        header = stdout[offset:line_end]
        offset = line_end + 1
        if b"\t" not in header:
            continue
        name_raw, size_raw = header.split(b"\t", 1)
        try:
            payload_size = int(size_raw)
        except ValueError:
            continue
        payload = stdout[offset : offset + payload_size]
        offset += payload_size
        if offset < length and stdout[offset : offset + 1] == b"\n":
            offset += 1
        try:
            parsed_payload = json.loads(payload.decode("utf-8"))
        except Exception:
            parsed_payload = {"raw": payload.decode("utf-8", errors="replace")}
        frames.append({"name": name_raw.decode("utf-8", errors="replace"), "payload": parsed_payload})
    return frames


def _run_ptg2_serving_binary_copy_from_key_copy(kind: str, input_path: Path, output_path: Path) -> dict[str, Any]:
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("PTG2 Rust serving binary COPY encoder is enabled but no scanner binary was found")
    completed = subprocess.run(
        [
            str(binary),
            "--serving-binary-copy-from-key-copy",
            kind,
            str(input_path),
            str(output_path),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        stderr_text = completed.stderr.decode("utf-8", errors="replace")
        stdout_text = completed.stdout.decode("utf-8", errors="replace")
        raise RuntimeError(
            "PTG2 Rust serving binary COPY encoder failed "
            f"for {kind}: stdout={stdout_text[-500:]} stderr={stderr_text[-1000:]}"
        )
    for frame in _parse_scanner_sized_frames(completed.stdout):
        if frame.get("name") == "serving_binary_copy_file" and isinstance(frame.get("payload"), dict):
            return dict(frame["payload"])
    raise RuntimeError(f"PTG2 Rust serving binary COPY encoder did not emit a summary for {kind}")


def _parse_serving_binary_stream_summary(stderr: bytes) -> dict[str, Any]:
    stderr_text = stderr.decode("utf-8", errors="replace")
    for line in reversed(stderr_text.splitlines()):
        if not line.startswith("PTG2_SERVING_BINARY_COPY\t"):
            continue
        try:
            payload = json.loads(line.split("\t", 1)[1])
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"PTG2 Rust streaming COPY emitted invalid summary JSON: {line}") from exc
        if isinstance(payload, dict):
            return payload
    raise RuntimeError(f"PTG2 Rust streaming COPY did not emit a summary: {stderr_text[-1000:]}")


async def _feed_rust_stdin(process: Any, data: bytes) -> None:
    """Forward one PostgreSQL COPY chunk into the Rust encoder stdin."""
    if process.stdin is None:
        raise RuntimeError("PTG2 Rust streaming COPY process stdin is closed")
    process.stdin.write(data)
    await process.stdin.drain()


async def _close_rust_stdin(process: Any) -> None:
    """Close Rust encoder stdin after PostgreSQL COPY OUT reaches EOF."""
    if process.stdin is None:
        return
    process.stdin.close()
    try:
        await process.stdin.wait_closed()
    except (BrokenPipeError, ConnectionResetError):
        return


async def _rust_stdout_chunks(process: Any):
    """Yield PostgreSQL COPY rows produced by the Rust encoder stdout."""
    if process.stdout is None:
        raise RuntimeError("PTG2 Rust streaming COPY process stdout is closed")
    while True:
        chunk = await process.stdout.read(1024 * 1024)
        if not chunk:
            break
        yield chunk


async def _copy_pg_query_to_rust(source_driver: Any, sql: str, process: Any) -> Any:
    """Stream a PostgreSQL COPY query into the Rust encoder process."""
    try:
        work_mem = _serving_binary_copy_work_mem()
        if work_mem and hasattr(source_driver, "transaction"):
            async with source_driver.transaction():
                await source_driver.execute(f"SET LOCAL work_mem TO '{work_mem}'")
                return await source_driver.copy_from_query(
                    sql,
                    output=lambda data: _feed_rust_stdin(process, data),
                    format="text",
                    delimiter="\t",
                    null="\\N",
                )
        return await source_driver.copy_from_query(
            sql,
            output=lambda data: _feed_rust_stdin(process, data),
            format="text",
            delimiter="\t",
            null="\\N",
        )
    finally:
        await _close_rust_stdin(process)


async def _stream_serving_binary_copy(
    *,
    kind: str,
    sql: str,
    schema_name: str,
    target_table: str,
) -> dict[str, Any]:
    """Pipe PostgreSQL COPY rows through Rust and into the binary block table."""
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("PTG2 Rust serving binary streaming encoder is enabled but no scanner binary was found")
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--serving-binary-copy-from-key-copy-stdio",
        kind,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        raise RuntimeError("PTG2 Rust streaming COPY process did not expose stdio pipes")
    stderr_task = asyncio.create_task(process.stderr.read())

    try:
        async with db.acquire() as source_conn, db.acquire() as target_conn:
            source_raw = source_conn.raw_connection
            target_raw = target_conn.raw_connection
            source_driver = getattr(source_raw, "driver_connection", source_raw)
            target_driver = getattr(target_raw, "driver_connection", target_raw)

            await asyncio.gather(
                _copy_pg_query_to_rust(source_driver, sql, process),
                target_driver.copy_to_table(
                    target_table,
                    source=_rust_stdout_chunks(process),
                    schema_name=schema_name,
                    columns=PTG2_SERVING_BINARY_COLUMNS,
                    format="text",
                    delimiter="\t",
                    null="\\N",
                ),
            )
    except Exception:
        process.kill()
        await _close_rust_stdin(process)
        await process.wait()
        await stderr_task
        raise
    await _close_rust_stdin(process)
    returncode = await process.wait()
    stderr = await stderr_task
    if returncode != 0:
        raise RuntimeError(
            "PTG2 Rust streaming serving binary COPY encoder failed "
            f"for {kind}: stderr={stderr.decode('utf-8', errors='replace')[-2000:]}"
        )
    return _parse_serving_binary_stream_summary(stderr)


async def _copy_serving_binary_record_batches(
    *,
    schema_name: str,
    table_name: str,
    records: Iterable[tuple[Any, ...]],
) -> int:
    copied = 0
    batch: list[tuple[Any, ...]] = []
    batch_size = _serving_binary_copy_records()
    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            await _copy_serving_binary_records(schema_name=schema_name, table_name=table_name, records=batch)
            copied += len(batch)
            batch = []
    if batch:
        await _copy_serving_binary_records(schema_name=schema_name, table_name=table_name, records=batch)
        copied += len(batch)
    return copied


def _decode_stored_payload(record: Mapping[str, Any]) -> bytes:
    payload = bytes(record.get("payload") or b"")
    compression = str(record.get("payload_compression") or "none").strip().lower()
    if compression in {"", "none"}:
        return payload
    if compression != "zlib":
        raise PTG2ManifestArtifactError(f"unsupported PTG2 serving binary payload compression: {compression}")
    raw_payload = zlib.decompress(payload)
    expected_raw_bytes = record.get("raw_payload_bytes")
    if expected_raw_bytes is not None and int(expected_raw_bytes or 0) > 0 and len(raw_payload) != int(expected_raw_bytes):
        raise PTG2ManifestArtifactError("PTG2 serving binary raw payload byte count mismatch")
    return raw_payload


async def _load_price_key_map(*, schema_name: str, table_name: str) -> dict[bytes, int]:
    """Load ``price_set_id -> price_key`` from the by-code binary dictionary."""

    qualified_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    dictionary_row = await db.first(
        f"""
        SELECT
            payload,
            COALESCE(payload_compression, 'none') AS payload_compression,
            raw_payload_bytes
        FROM {qualified_target} binary_block
        WHERE artifact_kind = :artifact_kind
          AND block_key = 0
          AND block_no = 0
        LIMIT 1
        """,
        artifact_kind=PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
    )
    if dictionary_row is None:
        return {}
    row_mapping = getattr(dictionary_row, "_mapping", None)
    dictionary_payload = _decode_stored_payload(dict(row_mapping) if row_mapping is not None else dict(dictionary_row))
    if len(dictionary_payload) % 16:
        raise PTG2ManifestArtifactError("PTG2 serving binary price dictionary payload has invalid length")
    return {
        dictionary_payload[offset : offset + 16]: index
        for index, offset in enumerate(range(0, len(dictionary_payload), 16))
    }


def _iter_artifact_entries(artifacts: Mapping[str, Any] | None) -> Iterable[dict[str, Any]]:
    """Yield normalized artifact entries from a named or list-based manifest payload."""

    if not isinstance(artifacts, Mapping):
        return
    for name, value in artifacts.items():
        if name == "sidecars" and isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    yield dict(item)
            continue
        if isinstance(value, Mapping):
            artifact_entry_map = dict(value)
            artifact_entry_map.setdefault("name", name)
            yield artifact_entry_map


def _price_forward_artifact_entry(*artifact_payloads: Mapping[str, Any] | None) -> dict[str, Any] | None:
    for artifacts in artifact_payloads:
        for entry in _iter_artifact_entries(artifacts):
            entry_name = str(entry.get("name") or entry.get("kind") or "").strip()
            if entry_name == "price_forward":
                return entry
    return None


def _price_forward_artifact_path(entry: Mapping[str, Any] | None) -> Path | None:
    if not isinstance(entry, Mapping):
        return None
    raw_path = str(entry.get("path") or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.exists() and path.stat().st_size > 0:
        return path
    return None


def _validate_price_forward_sidecar_file(path: Path, metadata: Mapping[str, Any] | None) -> None:
    if metadata is None:
        return
    expected_byte_count = metadata.get("byte_count")
    if expected_byte_count is not None and int(expected_byte_count) != path.stat().st_size:
        raise PTG2ManifestArtifactError(
            f"price_forward sidecar byte_count mismatch: expected {expected_byte_count}, got {path.stat().st_size}"
        )


def _iter_standard_price_members(payload: mmap.mmap) -> Iterable[tuple[bytes, tuple[bytes, ...]]]:
    if len(payload) < _MEMBERSHIP_HEADER.size:
        raise PTG2ManifestArtifactError("price_forward sidecar is missing its membership header")
    _magic, version, entry_count = _MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported price_forward sidecar version: {version!r}")
    index_start = _MEMBERSHIP_HEADER.size
    index_end = index_start + entry_count * _MEMBERSHIP_INDEX_RECORD.size
    if len(payload) < index_end:
        raise PTG2ManifestArtifactError("price_forward sidecar ended inside the owner index")
    member_start = index_end
    for index in range(entry_count):
        record_offset = index_start + index * _MEMBERSHIP_INDEX_RECORD.size
        owner_id, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(payload, record_offset)
        start = member_start + member_offset * 16
        end = start + member_count * 16
        if end > len(payload):
            raise PTG2ManifestArtifactError("price_forward sidecar member block is truncated")
        yield owner_id, tuple(bytes(payload[position : position + 16]) for position in range(start, end, 16))


def _iter_dense_price_members(payload: mmap.mmap) -> Iterable[tuple[bytes, tuple[bytes, ...]]]:
    if len(payload) < _DENSE_MEMBERSHIP_HEADER.size:
        raise PTG2ManifestArtifactError("dense price_forward sidecar is missing its membership header")
    _magic, version, entry_count, member_global_count = _DENSE_MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported dense price_forward sidecar version: {version!r}")
    index_start = _DENSE_MEMBERSHIP_HEADER.size
    index_end = index_start + entry_count * _MEMBERSHIP_INDEX_RECORD.size
    member_global_start = index_end
    member_index_start = member_global_start + member_global_count * 16
    if len(payload) < member_index_start:
        raise PTG2ManifestArtifactError("dense price_forward sidecar ended inside the dictionary")
    for index in range(entry_count):
        record_offset = index_start + index * _MEMBERSHIP_INDEX_RECORD.size
        owner_id, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(payload, record_offset)
        start = member_index_start + member_offset * _DENSE_MEMBER_RECORD.size
        end = start + member_count * _DENSE_MEMBER_RECORD.size
        if end > len(payload):
            raise PTG2ManifestArtifactError("dense price_forward sidecar member block is truncated")
        members: list[bytes] = []
        for position in range(start, end, _DENSE_MEMBER_RECORD.size):
            member_local_id = _DENSE_MEMBER_RECORD.unpack_from(payload, position)[0]
            if member_local_id >= member_global_count:
                raise PTG2ManifestArtifactError("dense price_forward sidecar member id is out of range")
            member_position = member_global_start + member_local_id * 16
            members.append(bytes(payload[member_position : member_position + 16]))
        yield owner_id, tuple(members)


def _iter_price_forward_members(
    path: Path,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> Iterable[tuple[bytes, tuple[bytes, ...]]]:
    _validate_price_forward_sidecar_file(path, metadata)
    with path.open("rb") as fp:
        with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as payload:
            if len(payload) < 8:
                raise PTG2ManifestArtifactError("price_forward sidecar is missing its magic header")
            magic = bytes(payload[:8])
            if magic in _STANDARD_MEMBERSHIP_MAGICS:
                yield from _iter_standard_price_members(payload)
                return
            if magic in _DENSE_MEMBERSHIP_MAGICS:
                yield from _iter_dense_price_members(payload)
                return
            raise PTG2ManifestArtifactError("price_forward sidecar has an invalid magic header")


@dataclass
class _PriceSetAtomBlockWriter:
    """Buffers price-set atom mappings into block-keyed serving-binary rows."""

    schema_name: str
    table_name: str
    max_payload_bytes: int
    batch_size: int
    batch_records: list[tuple[Any, ...]] = field(default_factory=list)
    copied_records: int = 0
    block_count: int = 0
    id_block_count: int = 0
    price_set_count: int = 0
    atom_ref_count: int = 0
    current_block_key: int | None = None
    current_block_no: int = 0
    current_payload: bytearray = field(default_factory=bytearray)
    current_entry_count: int = 0
    block_no_by_key: dict[int, int] = field(default_factory=dict)
    id_payload_by_bucket: dict[int, bytearray] = field(default_factory=dict)
    id_entry_count_by_bucket: dict[int, int] = field(default_factory=dict)
    id_block_no_by_bucket: dict[int, int] = field(default_factory=dict)

    async def flush_batch(self) -> None:
        """COPY any buffered serving-binary rows into PostgreSQL."""

        if not self.batch_records:
            return
        await _copy_serving_binary_records(
            schema_name=self.schema_name,
            table_name=self.table_name,
            records=self.batch_records,
        )
        self.copied_records += len(self.batch_records)
        self.batch_records = []

    async def append_record(self, record: tuple[Any, ...]) -> None:
        """Append one table row and flush when the COPY batch is full."""

        self.batch_records.append(record)
        if len(self.batch_records) >= self.batch_size:
            await self.flush_batch()

    async def flush_block(self) -> None:
        """Flush the current price-key block into the pending COPY batch."""

        if self.current_block_key is None or self.current_entry_count <= 0:
            return
        await self.append_record(
            _serving_binary_record(
                PTG2_SERVING_BINARY_PRICE_SET_ATOMS_KIND,
                self.current_block_key,
                self.current_block_no,
                self.current_entry_count,
                self.current_payload,
            )
        )
        self.block_count += 1
        self.block_no_by_key[self.current_block_key] = self.current_block_no + 1
        self.current_block_no += 1
        self.current_entry_count = 0
        self.current_payload = bytearray()

    async def flush_id_bucket(self, bucket_key: int) -> None:
        """Flush one price-set-id hash bucket into the pending COPY batch."""

        entry_count = int(self.id_entry_count_by_bucket.get(bucket_key) or 0)
        if entry_count <= 0:
            return
        await self.append_record(
            _serving_binary_record(
                PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_KIND,
                bucket_key,
                int(self.id_block_no_by_bucket.get(bucket_key) or 0),
                entry_count,
                self.id_payload_by_bucket.get(bucket_key, bytearray()),
            )
        )
        self.id_block_count += 1
        self.id_block_no_by_bucket[bucket_key] = int(self.id_block_no_by_bucket.get(bucket_key) or 0) + 1
        self.id_entry_count_by_bucket[bucket_key] = 0
        self.id_payload_by_bucket[bucket_key] = bytearray()

    async def flush_id_buckets(self) -> None:
        """Flush all pending price-set-id hash buckets."""

        for bucket_key in sorted(self.id_entry_count_by_bucket):
            await self.flush_id_bucket(bucket_key)

    async def add_price_set(self, price_key: int, price_set_id: bytes, atom_ids: tuple[bytes, ...]) -> None:
        """Append one price-set membership entry to the active block."""

        entry_payload = _price_set_atom_payload(price_key, atom_ids)
        block_key = int(price_key) // _PRICE_SET_ATOM_BLOCK_SIZE
        if self.current_block_key != block_key:
            await self.flush_block()
            self.current_block_key = block_key
            self.current_block_no = int(self.block_no_by_key.get(block_key) or 0)
        elif self.current_entry_count > 0 and len(self.current_payload) + len(entry_payload) > self.max_payload_bytes:
            await self.flush_block()
        self.current_payload.extend(entry_payload)
        self.current_entry_count += 1
        id_bucket_key = _price_set_atom_id_bucket(price_set_id)
        id_entry_payload = _price_set_atom_by_id_payload(price_set_id, atom_ids)
        id_payload = self.id_payload_by_bucket.setdefault(id_bucket_key, bytearray())
        if (
            int(self.id_entry_count_by_bucket.get(id_bucket_key) or 0) > 0
            and len(id_payload) + len(id_entry_payload) > self.max_payload_bytes
        ):
            await self.flush_id_bucket(id_bucket_key)
            id_payload = self.id_payload_by_bucket.setdefault(id_bucket_key, bytearray())
        id_payload.extend(id_entry_payload)
        self.id_entry_count_by_bucket[id_bucket_key] = int(self.id_entry_count_by_bucket.get(id_bucket_key) or 0) + 1
        self.price_set_count += 1
        self.atom_ref_count += len(atom_ids)

    async def finish(self) -> None:
        """Flush the active block and pending COPY batch."""

        await self.flush_block()
        await self.flush_id_buckets()
        await self.flush_batch()

    def summary(self) -> dict[str, Any]:
        """Return import metrics for the generated price-set atom blocks."""

        return {
            "block_size": _PRICE_SET_ATOM_BLOCK_SIZE,
            "block_count": self.block_count,
            "id_bucket_count": _PRICE_SET_ATOM_ID_BUCKETS,
            "id_block_count": self.id_block_count,
            "price_set_count": self.price_set_count,
            "atom_ref_count": self.atom_ref_count,
            "copied_records": self.copied_records,
        }


async def _write_price_set_atom_blocks(
    *,
    schema_name: str,
    table_name: str,
    price_set_atom_table: str | None,
    price_forward_artifact: Mapping[str, Any] | None,
    max_payload_bytes: int,
) -> dict[str, Any]:
    """Encode the price_forward sidecar as PostgreSQL serving-binary blocks."""

    price_key_by_id = await _load_price_key_map(schema_name=schema_name, table_name=table_name)
    if not price_key_by_id:
        return {"skipped": "missing_price_dictionary"}
    if price_set_atom_table:
        table_summary = await _write_atom_map_blocks_from_table(
            schema_name=schema_name,
            table_name=table_name,
            price_set_atom_table=price_set_atom_table,
            price_key_by_id=price_key_by_id,
            max_payload_bytes=max_payload_bytes,
        )
        if int(table_summary.get("price_set_count") or 0) > 0 or not _price_forward_artifact_path(price_forward_artifact):
            return table_summary

    sidecar_path = _price_forward_artifact_path(price_forward_artifact)
    if sidecar_path is None:
        return {"skipped": "missing_price_forward_sidecar"}
    block_writer = _PriceSetAtomBlockWriter(
        schema_name=schema_name,
        table_name=table_name,
        max_payload_bytes=max_payload_bytes,
        batch_size=_serving_binary_copy_records(),
    )
    for owner_id, atom_ids in _iter_price_forward_members(sidecar_path, metadata=price_forward_artifact):
        price_key = price_key_by_id.get(owner_id)
        if price_key is None:
            continue
        await block_writer.add_price_set(price_key, owner_id, atom_ids)
    await block_writer.finish()
    summary = block_writer.summary()
    summary["source"] = "price_forward_sidecar"
    return summary


async def _write_atom_map_blocks_from_table(
    *,
    schema_name: str,
    table_name: str,
    price_set_atom_table: str,
    price_key_by_id: Mapping[bytes, int],
    max_payload_bytes: int,
) -> dict[str, Any]:
    """Encode staged price-set atom rows as PostgreSQL serving-binary blocks."""

    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(price_set_atom_table)}"
    block_writer = _PriceSetAtomBlockWriter(
        schema_name=schema_name,
        table_name=table_name,
        max_payload_bytes=max_payload_bytes,
        batch_size=_serving_binary_copy_records(),
    )
    current_price_set_id: bytes | None = None
    current_atom_ids: list[bytes] = []

    async def flush_current_price_set() -> None:
        """Write the pending price-set group after the ordered stream advances."""
        if current_price_set_id is None:
            return
        price_key = price_key_by_id.get(current_price_set_id)
        if price_key is not None:
            await block_writer.add_price_set(price_key, current_price_set_id, tuple(current_atom_ids))

    async with db.session() as session:
        atom_pair_result = await session.stream(
            db.text(
                f"""
                SELECT price_set_global_id_128, price_atom_global_id_128
                FROM {qualified_source}
                ORDER BY price_set_global_id_128, price_atom_global_id_128
                """
            )
        )
        async for atom_pair_row in atom_pair_result:
            price_set_id = _normalize_128_id(_row_value(atom_pair_row, "price_set_global_id_128", 0))
            price_atom_id = _normalize_128_id(_row_value(atom_pair_row, "price_atom_global_id_128", 1))
            if current_price_set_id != price_set_id:
                await flush_current_price_set()
                current_price_set_id = price_set_id
                current_atom_ids = []
            current_atom_ids.append(price_atom_id)
    await flush_current_price_set()
    await block_writer.finish()
    summary = block_writer.summary()
    summary["source"] = "price_set_atom_table"
    summary["stage_table"] = f"{schema_name}.{price_set_atom_table}"
    return summary


async def _write_atom_map_stage(
    *,
    schema_name: str,
    target_table: str,
    price_forward_artifact: Mapping[str, Any] | None,
    price_set_atom_table: str | None,
    max_payload_bytes: int,
    progress_callback: Callable[..., None] | None,
    stage_progress: _AtomMapStageProgress,
    timing_by_stage: dict[str, float],
) -> dict[str, Any]:
    """Run the publish stage that stores price-set atom mappings in PostgreSQL."""

    _emit_progress(
        progress_callback,
        stage_progress.progress_step,
        done=stage_progress.done,
        total=stage_progress.total,
        message=stage_progress.message,
    )
    stage_started_at = time.monotonic()
    price_set_atoms = await _write_price_set_atom_blocks(
        schema_name=schema_name,
        table_name=target_table,
        price_set_atom_table=price_set_atom_table,
        price_forward_artifact=price_forward_artifact,
        max_payload_bytes=max_payload_bytes,
    )
    timing_by_stage["price_set_atoms_stream_seconds"] = _elapsed_seconds(stage_started_at)
    return price_set_atoms


def _by_code_group_payload(provider_set_key: int, previous_key: int, price_keys: list[int]) -> bytearray:
    """Encode one provider-set group for forward serving-binary blocks."""
    binary_payload = bytearray()
    _append_uvarint(binary_payload, int(provider_set_key) - int(previous_key))
    _append_uvarint(binary_payload, len(price_keys))
    for price_key in price_keys:
        _append_uvarint(binary_payload, price_key)
    return binary_payload


async def _write_by_code_blocks(
    *,
    schema_name: str,
    table_name: str,
    rows: AsyncIterable[Any],
    max_payload_bytes: int,
) -> dict[str, Any]:
    price_dictionary = _PriceDictionary()
    provider_count_by_key: dict[int, int] = {}
    block_records: list[tuple[Any, ...]] = []
    current_code: int | None = None
    current_payload = bytearray()
    current_group_count = 0
    current_provider_set_key: int | None = None
    current_price_keys: list[int] = []
    block_no = 0
    previous_provider_set_key = 0
    row_count = 0
    code_count = 0
    block_count = 0

    def append_by_code_block(
        code_key: int | None,
        block_index: int,
        entry_count: int,
        binary_payload: bytearray,
    ) -> tuple[bytearray, int, int]:
        """Append one by-code block and return reset payload state."""
        if code_key is None or entry_count <= 0:
            return binary_payload, entry_count, 0
        block_records.append(
            _serving_binary_record(
                PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
                int(code_key),
                int(block_index),
                int(entry_count),
                binary_payload,
            )
        )
        return bytearray(), 0, 1

    def append_by_code_group(
        code_key: int | None,
        block_index: int,
        binary_payload: bytearray,
        entry_count: int,
        previous_key: int,
        provider_set_key: int | None,
        price_keys: list[int],
    ) -> tuple[bytearray, int, int, int, int]:
        """Append one provider-set price group, flushing the current block if needed."""
        if provider_set_key is None or not price_keys:
            return binary_payload, entry_count, block_index, previous_key, 0
        group_payload = _by_code_group_payload(provider_set_key, previous_key, price_keys)
        added_blocks = 0
        if entry_count > 0 and len(binary_payload) + len(group_payload) > max_payload_bytes:
            binary_payload, entry_count, added_blocks = append_by_code_block(
                code_key,
                block_index,
                entry_count,
                binary_payload,
            )
            block_index += added_blocks
            previous_key = 0
            group_payload = _by_code_group_payload(provider_set_key, previous_key, price_keys)
        binary_payload.extend(group_payload)
        entry_count += 1
        previous_key = int(provider_set_key)
        return binary_payload, entry_count, block_index, previous_key, added_blocks

    async for raw_row in rows:
        code_key = int(_row_value(raw_row, "code_key", 0))
        provider_set_key = int(_row_value(raw_row, "provider_set_key", 1))
        provider_count = int(_row_value(raw_row, "provider_count", 2))
        price_key = price_dictionary.key_for(_row_value(raw_row, "price_set_global_id_128", 3))
        existing_provider_count = provider_count_by_key.setdefault(provider_set_key, provider_count)
        if existing_provider_count != provider_count:
            raise RuntimeError(
                "PTG2 serving binary provider count changed for provider_set_key "
                f"{provider_set_key}: {existing_provider_count} != {provider_count}"
            )
        if current_code != code_key:
            current_payload, current_group_count, block_no, previous_provider_set_key, added_blocks = append_by_code_group(
                current_code,
                block_no,
                current_payload,
                current_group_count,
                previous_provider_set_key,
                current_provider_set_key,
                current_price_keys,
            )
            block_count += added_blocks
            current_payload, current_group_count, added_blocks = append_by_code_block(
                current_code,
                block_no,
                current_group_count,
                current_payload,
            )
            block_count += added_blocks
            current_code = code_key
            previous_provider_set_key = 0
            block_no = 0
            current_provider_set_key = None
            current_price_keys = []
            code_count += 1
        elif current_provider_set_key != provider_set_key:
            current_payload, current_group_count, block_no, previous_provider_set_key, added_blocks = append_by_code_group(
                current_code,
                block_no,
                current_payload,
                current_group_count,
                previous_provider_set_key,
                current_provider_set_key,
                current_price_keys,
            )
            block_count += added_blocks
            current_provider_set_key = None
            current_price_keys = []
        current_provider_set_key = provider_set_key
        current_price_keys.append(price_key)
        row_count += 1
    current_payload, current_group_count, block_no, previous_provider_set_key, added_blocks = append_by_code_group(
        current_code,
        block_no,
        current_payload,
        current_group_count,
        previous_provider_set_key,
        current_provider_set_key,
        current_price_keys,
    )
    block_count += added_blocks
    current_payload, current_group_count, added_blocks = append_by_code_block(
        current_code,
        block_no,
        current_group_count,
        current_payload,
    )
    block_count += added_blocks
    block_records.append(
        _serving_binary_record(
            PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
            0,
            0,
            len(price_dictionary.values),
            _price_dictionary_payload(price_dictionary.values),
        )
    )
    block_records.append(
        _serving_binary_record(
            PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
            0,
            0,
            len(provider_count_by_key),
            _provider_count_dictionary_payload(provider_count_by_key),
        )
    )
    copied = await _copy_serving_binary_record_batches(
        schema_name=schema_name,
        table_name=table_name,
        records=block_records,
    )
    return {
        "row_count": row_count,
        "code_count": code_count,
        "block_count": block_count,
        "price_set_count": len(price_dictionary.values),
        "provider_set_count": len(provider_count_by_key),
        "copied_records": copied,
    }


def _provider_set_pattern_payload(
    pattern_items: list[tuple[tuple[tuple[int, int], ...], list[int]]],
) -> tuple[bytes, int, int]:
    payload = bytearray()
    pattern_count = 0
    expanded_row_count = 0
    for entries, code_key_list in pattern_items:
        code_keys = sorted(code_key_list)
        _append_uvarint(payload, len(code_keys))
        previous_code_key = 0
        for index, code_key in enumerate(code_keys):
            _append_uvarint(payload, int(code_key) if index == 0 else int(code_key) - previous_code_key)
            previous_code_key = int(code_key)
        _append_uvarint(payload, len(entries))
        for provider_count, price_key in entries:
            _append_uvarint(payload, provider_count)
            _append_uvarint(payload, price_key)
        pattern_count += 1
        expanded_row_count += len(code_keys) * len(entries)
    return bytes(payload), pattern_count, expanded_row_count


async def _write_by_provider_set_blocks(
    *,
    schema_name: str,
    table_name: str,
    rows: AsyncIterable[Any],
    max_payload_bytes: int,
) -> dict[str, Any]:
    price_dictionary = _PriceDictionary()
    block_records: list[tuple[Any, ...]] = []
    current_provider_set: int | None = None
    current_code: int | None = None
    current_code_entries: list[tuple[int, int]] = []
    current_patterns: dict[tuple[tuple[int, int], ...], list[int]] = {}
    row_count = 0
    provider_set_count = 0
    code_keys_seen: set[int] = set()
    block_count = 0
    pattern_count = 0

    def collect_code_pattern(
        code_key: int | None,
        code_entries: list[tuple[int, int]],
        pattern_by_entries: dict[tuple[tuple[int, int], ...], list[int]],
    ) -> list[tuple[int, int]]:
        """Move the current code's entries into the provider pattern map."""
        if code_key is not None:
            pattern_by_entries.setdefault(tuple(code_entries), []).append(int(code_key))
        return []

    def append_provider_binary_blocks(
        provider_set_key: int | None,
        code_key: int | None,
        code_entries: list[tuple[int, int]],
        pattern_by_entries: dict[tuple[tuple[int, int], ...], list[int]],
    ) -> tuple[list[tuple[int, int]], dict[tuple[tuple[int, int], ...], list[int]], int, int]:
        """Append reverse blocks and return reset provider state plus counts."""
        if provider_set_key is None:
            return code_entries, pattern_by_entries, 0, 0
        code_entries = collect_code_pattern(code_key, code_entries, pattern_by_entries)
        ordered_patterns = sorted(pattern_by_entries.items(), key=lambda item: (item[1][0] if item[1] else -1, item[0]))
        pending_block_patterns: list[tuple[tuple[tuple[int, int], ...], list[int]]] = []
        chunk_payload_bytes = 0
        block_no = 0
        added_blocks = 0
        added_patterns = 0
        for entries, code_key_list in ordered_patterns:
            binary_payload, _patterns, _expanded = _provider_set_pattern_payload([(entries, code_key_list)])
            if pending_block_patterns and chunk_payload_bytes + len(binary_payload) > max_payload_bytes:
                block_payload, block_patterns, _block_expanded = _provider_set_pattern_payload(pending_block_patterns)
                block_records.append(
                    _serving_binary_record(
                        PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
                        int(provider_set_key),
                        block_no,
                        block_patterns,
                        block_payload,
                    )
                )
                added_blocks += 1
                added_patterns += block_patterns
                block_no += 1
                pending_block_patterns = []
                chunk_payload_bytes = 0
            pending_block_patterns.append((entries, code_key_list))
            chunk_payload_bytes += len(binary_payload)
        if pending_block_patterns:
            block_payload, block_patterns, _block_expanded = _provider_set_pattern_payload(pending_block_patterns)
            block_records.append(
                _serving_binary_record(
                    PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND,
                    int(provider_set_key),
                    block_no,
                    block_patterns,
                    block_payload,
                )
            )
            added_blocks += 1
            added_patterns += block_patterns
        return code_entries, {}, added_blocks, added_patterns

    async for raw_row in rows:
        provider_set_key = int(_row_value(raw_row, "provider_set_key", 0))
        code_key = int(_row_value(raw_row, "code_key", 1))
        provider_count = int(_row_value(raw_row, "provider_count", 2))
        price_key = price_dictionary.key_for(_row_value(raw_row, "price_set_global_id_128", 3))
        if current_provider_set != provider_set_key:
            current_code_entries, current_patterns, added_blocks, added_patterns = append_provider_binary_blocks(
                current_provider_set,
                current_code,
                current_code_entries,
                current_patterns,
            )
            block_count += added_blocks
            pattern_count += added_patterns
            current_provider_set = provider_set_key
            current_code = None
            provider_set_count += 1
        if current_code != code_key:
            current_code_entries = collect_code_pattern(current_code, current_code_entries, current_patterns)
            current_code = code_key
        current_code_entries.append((provider_count, price_key))
        code_keys_seen.add(code_key)
        row_count += 1
    current_code_entries, current_patterns, added_blocks, added_patterns = append_provider_binary_blocks(
        current_provider_set,
        current_code,
        current_code_entries,
        current_patterns,
    )
    block_count += added_blocks
    pattern_count += added_patterns
    block_records.append(
        _serving_binary_record(
            PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND,
            0,
            0,
            len(price_dictionary.values),
            _price_dictionary_payload(price_dictionary.values),
        )
    )
    copied = await _copy_serving_binary_record_batches(
        schema_name=schema_name,
        table_name=table_name,
        records=block_records,
    )
    return {
        "row_count": row_count,
        "provider_set_count": provider_set_count,
        "code_count": len(code_keys_seen),
        "block_count": block_count,
        "pattern_count": pattern_count,
        "price_set_count": len(price_dictionary.values),
        "copied_records": copied,
    }


async def _write_ptg2_serving_binary_table_python(
    *,
    schema_name: str,
    source_table: str,
    target_table: str,
    expected_row_count: int | None = None,
    price_set_atom_table: str | None = None,
    price_forward_artifact: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    _emit_progress(
        progress_callback,
        "serving binary create table",
        done=1,
        total=8,
        message="creating PostgreSQL binary serving table",
    )
    await create_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)

    async def by_code_rows():
        async with db.session() as session:
            result = await session.stream(
                db.text(
                    f"""
                    SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
                    FROM {qualified_source}
                    ORDER BY code_key, provider_set_key, price_set_global_id_128
                    """
                )
            )
            async for row in result:
                yield row

    async def by_provider_set_rows():
        async with db.session() as session:
            result = await session.stream(
                db.text(
                    f"""
                    SELECT provider_set_key, code_key, provider_count, price_set_global_id_128
                    FROM {qualified_source}
                    ORDER BY provider_set_key, code_key, price_set_global_id_128
                    """
                )
            )
            async for row in result:
                yield row

    _emit_progress(
        progress_callback,
        "serving binary python by code",
        done=2,
        total=8,
        message="streaming PostgreSQL binary by-code blocks in Python",
        expected_row_count=expected_row_count,
    )
    stage_started_at = time.monotonic()
    by_code = await _write_by_code_blocks(
        schema_name=schema_name,
        table_name=target_table,
        rows=by_code_rows(),
        max_payload_bytes=max_payload_bytes,
    )
    timing_by_stage["by_code_stream_seconds"] = _elapsed_seconds(stage_started_at)
    price_set_atoms = await _write_atom_map_stage(
        schema_name=schema_name,
        target_table=target_table,
        price_forward_artifact=price_forward_artifact,
        price_set_atom_table=price_set_atom_table,
        max_payload_bytes=max_payload_bytes,
        progress_callback=progress_callback,
        stage_progress=_AtomMapStageProgress(
            progress_step="serving binary python price atoms",
            done=3,
            total=8,
            message="streaming PostgreSQL binary price-set atom blocks in Python",
        ),
        timing_by_stage=timing_by_stage,
    )
    _emit_progress(
        progress_callback,
        "serving binary python reverse",
        done=4,
        total=8,
        message="streaming PostgreSQL binary reverse blocks in Python",
        expected_row_count=expected_row_count,
    )
    stage_started_at = time.monotonic()
    by_provider_set = await _write_by_provider_set_blocks(
        schema_name=schema_name,
        table_name=target_table,
        rows=by_provider_set_rows(),
        max_payload_bytes=max_payload_bytes,
    )
    timing_by_stage["reverse_stream_seconds"] = _elapsed_seconds(stage_started_at)
    if expected_row_count is not None and int(by_code.get("row_count") or 0) != int(expected_row_count):
        raise RuntimeError(
            "PTG2 serving binary by-code row count mismatch: "
            f"expected {expected_row_count}, got {by_code.get('row_count')}"
        )
    if expected_row_count is not None and int(by_provider_set.get("row_count") or 0) != int(expected_row_count):
        raise RuntimeError(
            "PTG2 serving binary reverse row count mismatch: "
            f"expected {expected_row_count}, got {by_provider_set.get('row_count')}"
        )
    _emit_progress(
        progress_callback,
        "serving binary index",
        done=7,
        total=8,
        message="indexing PostgreSQL binary serving table",
    )
    stage_started_at = time.monotonic()
    storage_data = await finalize_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    timing_by_stage["index_seconds"] = _elapsed_seconds(stage_started_at)
    timing_by_stage["total_seconds"] = _elapsed_seconds(total_started_at)
    return {
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "table": f"{schema_name}.{target_table}",
        "writer": "python_stream",
        "block_bytes": max_payload_bytes,
        "row_count": int(by_code.get("row_count") or 0),
        "by_code": by_code,
        "by_provider_set": by_provider_set,
        "price_set_atoms": price_set_atoms,
        "timing": timing_by_stage,
        "build_elapsed_seconds": timing_by_stage["total_seconds"],
        "storage": storage_data,
    }


def _serving_binary_stream_sql(*, qualified_source: str, kind: str) -> str:
    """Return the ordered serving-row query required by one stream encoder."""
    if kind == "by_code":
        return f"""
        SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
        FROM {qualified_source}
        ORDER BY code_key, provider_set_key, price_set_global_id_128
        """
    if kind == "by_provider_set":
        return f"""
        SELECT provider_set_key, code_key, provider_count, price_set_global_id_128
        FROM {qualified_source}
        ORDER BY provider_set_key, code_key, price_set_global_id_128
        """
    raise ValueError(f"unsupported serving binary stream kind: {kind}")


async def _stream_serving_binary_stage(
    *,
    config: _StreamStageConfig,
    progress_callback: Callable[..., None] | None,
    expected_row_count: int | None,
    qualified_source: str,
    schema_name: str,
    target_table: str,
    timing_by_stage: dict[str, float],
) -> dict[str, Any]:
    """Run one ordered PostgreSQL-to-Rust-to-PostgreSQL streaming stage."""
    _emit_progress(
        progress_callback,
        config.progress_step,
        done=config.progress_done,
        total=config.progress_total,
        message=config.progress_message,
        expected_row_count=expected_row_count,
    )
    stage_started_at = time.monotonic()
    stage_result = await _stream_serving_binary_copy(
        kind=config.kind,
        sql=_serving_binary_stream_sql(qualified_source=qualified_source, kind=config.kind),
        schema_name=schema_name,
        target_table=target_table,
    )
    timing_by_stage[config.timing_key] = _elapsed_seconds(stage_started_at)
    return stage_result


def _serving_binary_stream_stage_configs() -> tuple[_StreamStageConfig, ...]:
    """Return the two Rust streaming stages needed for bidirectional serving."""
    return (
        _StreamStageConfig(
            kind="by_code",
            progress_step="serving binary stream by code",
            progress_done=1,
            progress_total=5,
            progress_message="streaming PostgreSQL by-code COPY through Rust",
            timing_key="by_code_stream_seconds",
        ),
        _StreamStageConfig(
            kind="by_provider_set",
            progress_step="serving binary stream reverse",
            progress_done=3,
            progress_total=5,
            progress_message="streaming PostgreSQL reverse COPY through Rust",
            timing_key="reverse_stream_seconds",
        ),
    )


def _validate_stream_row_count(
    *,
    label: str,
    expected_row_count: int | None,
    stream_result: dict[str, Any],
) -> None:
    """Ensure a streaming encoder preserved the source serving row count."""
    if expected_row_count is None:
        return
    actual_row_count = int(stream_result.get("row_count") or 0)
    if actual_row_count != int(expected_row_count):
        raise RuntimeError(
            f"PTG2 serving binary streaming {label} row count mismatch: "
            f"expected {expected_row_count}, got {stream_result.get('row_count')}"
        )


async def _finalize_stream_binary_table(
    *,
    schema_name: str,
    target_table: str,
    progress_callback: Callable[..., None] | None,
    timing_by_stage: dict[str, float],
) -> dict[str, Any]:
    """Index and measure a streamed PostgreSQL binary serving table."""
    _emit_progress(
        progress_callback,
        "serving binary index",
        done=5,
        total=6,
        message="indexing PostgreSQL binary serving table",
    )
    stage_started_at = time.monotonic()
    storage_data = await finalize_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    timing_by_stage["index_seconds"] = _elapsed_seconds(stage_started_at)
    return storage_data


async def _run_serving_binary_stream_stages(
    *,
    stage_configs: tuple[_StreamStageConfig, ...],
    stream_tasks: int,
    progress_callback: Callable[..., None] | None,
    expected_row_count: int | None,
    qualified_source: str,
    schema_name: str,
    target_table: str,
    timing_by_stage: dict[str, float],
) -> dict[str, dict[str, Any]]:
    """Run the forward and reverse binary encoders, parallelizing when allowed."""
    if stream_tasks <= 1:
        stage_result_by_kind: dict[str, dict[str, Any]] = {}
        for config in stage_configs:
            stage_result_by_kind[config.kind] = await _stream_serving_binary_stage(
                config=config,
                progress_callback=progress_callback,
                expected_row_count=expected_row_count,
                qualified_source=qualified_source,
                schema_name=schema_name,
                target_table=target_table,
                timing_by_stage=timing_by_stage,
            )
        return stage_result_by_kind

    stage_task_by_kind = {
        config.kind: asyncio.create_task(
            _stream_serving_binary_stage(
                config=config,
                progress_callback=progress_callback,
                expected_row_count=expected_row_count,
                qualified_source=qualified_source,
                schema_name=schema_name,
                target_table=target_table,
                timing_by_stage=timing_by_stage,
            )
        )
        for config in stage_configs
    }
    try:
        stage_results = await asyncio.gather(*stage_task_by_kind.values())
    except Exception:
        for task in stage_task_by_kind.values():
            task.cancel()
        await asyncio.gather(*stage_task_by_kind.values(), return_exceptions=True)
        raise
    return dict(zip(stage_task_by_kind.keys(), stage_results, strict=True))


async def _write_serving_binary_rust_stream(
    *, schema_name: str, source_table: str, target_table: str,
    expected_row_count: int | None = None, price_set_atom_table: str | None = None,
    price_forward_artifact: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Create the binary table with Rust stdin/stdout COPY streaming."""
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    await create_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    stage_configs = _serving_binary_stream_stage_configs()
    stream_tasks = _serving_binary_stream_tasks()
    stage_result_by_kind = await _run_serving_binary_stream_stages(
        stage_configs=stage_configs,
        stream_tasks=stream_tasks,
        progress_callback=progress_callback,
        expected_row_count=expected_row_count,
        qualified_source=qualified_source,
        schema_name=schema_name,
        target_table=target_table,
        timing_by_stage=timing_by_stage,
    )

    by_code = stage_result_by_kind["by_code"]
    by_provider_set = stage_result_by_kind["by_provider_set"]
    _validate_stream_row_count(label="by-code", expected_row_count=expected_row_count, stream_result=by_code)
    _validate_stream_row_count(label="reverse", expected_row_count=expected_row_count, stream_result=by_provider_set)
    price_set_atoms = await _write_atom_map_stage(
        schema_name=schema_name,
        target_table=target_table,
        price_forward_artifact=price_forward_artifact,
        price_set_atom_table=price_set_atom_table,
        max_payload_bytes=max_payload_bytes,
        progress_callback=progress_callback,
        stage_progress=_AtomMapStageProgress(
            progress_step="serving binary stream price atoms",
            done=4,
            total=6,
            message="streaming PostgreSQL binary price-set atom blocks",
        ),
        timing_by_stage=timing_by_stage,
    )
    storage_data = await _finalize_stream_binary_table(
        schema_name=schema_name,
        target_table=target_table,
        progress_callback=progress_callback,
        timing_by_stage=timing_by_stage,
    )
    timing_by_stage["total_seconds"] = _elapsed_seconds(total_started_at)
    return dict(
        format=PTG2_SERVING_BINARY_TABLE_FORMAT, table=f"{schema_name}.{target_table}", writer="rust_stream",
        stream_tasks=stream_tasks, block_bytes=max_payload_bytes, row_count=int(by_code.get("row_count") or 0),
        by_code=by_code, by_provider_set=by_provider_set, price_set_atoms=price_set_atoms,
        timing=timing_by_stage, build_elapsed_seconds=timing_by_stage["total_seconds"], storage=storage_data,
    )


async def _write_ptg2_serving_binary_table_rust(
    *,
    schema_name: str,
    source_table: str,
    target_table: str,
    expected_row_count: int | None = None,
    price_set_atom_table: str | None = None,
    price_forward_artifact: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    await create_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    with tempfile.TemporaryDirectory(prefix="ptg2_serving_binary_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        by_code_source_copy = temp_dir / "serving_by_code_source.copy"
        by_code_binary_copy = temp_dir / "serving_by_code_binary.copy"
        by_provider_source_copy = temp_dir / "serving_by_provider_set_source.copy"
        by_provider_binary_copy = temp_dir / "serving_by_provider_set_binary.copy"

        _emit_progress(
            progress_callback,
            "serving binary export by code",
            done=1,
            total=10,
            message="exporting serving rows ordered by code",
            expected_row_count=expected_row_count,
        )
        stage_started_at = time.monotonic()
        await _copy_serving_binary_query_to_file(
            f"""
            SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
            FROM {qualified_source}
            ORDER BY code_key, provider_set_key, price_set_global_id_128
            """,
            by_code_source_copy,
        )
        timing_by_stage["by_code_export_seconds"] = _elapsed_seconds(stage_started_at)
        _emit_progress(
            progress_callback,
            "serving binary encode by code",
            done=2,
            total=10,
            message="encoding PostgreSQL binary by-code COPY in Rust",
            copy_bytes=by_code_source_copy.stat().st_size if by_code_source_copy.exists() else None,
        )
        stage_started_at = time.monotonic()
        by_code = await asyncio.to_thread(
            _run_ptg2_serving_binary_copy_from_key_copy,
            "by_code",
            by_code_source_copy,
            by_code_binary_copy,
        )
        timing_by_stage["by_code_encode_seconds"] = _elapsed_seconds(stage_started_at)
        by_code.pop("path", None)
        _emit_progress(
            progress_callback,
            "serving binary load by code",
            done=3,
            total=10,
            message="loading PostgreSQL binary by-code blocks",
            copy_bytes=by_code.get("byte_count"),
            copy_records=by_code.get("copy_record_count"),
        )
        stage_started_at = time.monotonic()
        await copy_ptg2_serving_binary_file_to_table(
            by_code_binary_copy,
            schema_name=schema_name,
            target_table=target_table,
        )
        timing_by_stage["by_code_load_seconds"] = _elapsed_seconds(stage_started_at)
        by_code_source_copy.unlink(missing_ok=True)
        by_code_binary_copy.unlink(missing_ok=True)

        _emit_progress(
            progress_callback,
            "serving binary export reverse",
            done=5,
            total=10,
            message="exporting serving rows ordered by provider set",
            expected_row_count=expected_row_count,
        )
        stage_started_at = time.monotonic()
        await _copy_serving_binary_query_to_file(
            f"""
            SELECT provider_set_key, code_key, provider_count, price_set_global_id_128
            FROM {qualified_source}
            ORDER BY provider_set_key, code_key, price_set_global_id_128
            """,
            by_provider_source_copy,
        )
        timing_by_stage["reverse_export_seconds"] = _elapsed_seconds(stage_started_at)
        _emit_progress(
            progress_callback,
            "serving binary encode reverse",
            done=6,
            total=10,
            message="encoding PostgreSQL binary reverse COPY in Rust",
            copy_bytes=by_provider_source_copy.stat().st_size if by_provider_source_copy.exists() else None,
        )
        stage_started_at = time.monotonic()
        by_provider_set = await asyncio.to_thread(
            _run_ptg2_serving_binary_copy_from_key_copy,
            "by_provider_set",
            by_provider_source_copy,
            by_provider_binary_copy,
        )
        timing_by_stage["reverse_encode_seconds"] = _elapsed_seconds(stage_started_at)
        by_provider_set.pop("path", None)
        _emit_progress(
            progress_callback,
            "serving binary load reverse",
            done=7,
            total=10,
            message="loading PostgreSQL binary reverse blocks",
            copy_bytes=by_provider_set.get("byte_count"),
            copy_records=by_provider_set.get("copy_record_count"),
        )
        stage_started_at = time.monotonic()
        await copy_ptg2_serving_binary_file_to_table(
            by_provider_binary_copy,
            schema_name=schema_name,
            target_table=target_table,
        )
        timing_by_stage["reverse_load_seconds"] = _elapsed_seconds(stage_started_at)

    if expected_row_count is not None and int(by_code.get("row_count") or 0) != int(expected_row_count):
        raise RuntimeError(
            "PTG2 serving binary Rust by-code row count mismatch: "
            f"expected {expected_row_count}, got {by_code.get('row_count')}"
        )
    if expected_row_count is not None and int(by_provider_set.get("row_count") or 0) != int(expected_row_count):
        raise RuntimeError(
            "PTG2 serving binary Rust reverse row count mismatch: "
            f"expected {expected_row_count}, got {by_provider_set.get('row_count')}"
        )
    price_set_atoms = await _write_atom_map_stage(
        schema_name=schema_name,
        target_table=target_table,
        price_forward_artifact=price_forward_artifact,
        price_set_atom_table=price_set_atom_table,
        max_payload_bytes=max_payload_bytes,
        progress_callback=progress_callback,
        stage_progress=_AtomMapStageProgress(
            progress_step="serving binary price atoms",
            done=8,
            total=10,
            message="encoding PostgreSQL binary price-set atom blocks",
        ),
        timing_by_stage=timing_by_stage,
    )
    _emit_progress(
        progress_callback,
        "serving binary index",
        done=9,
        total=10,
        message="indexing PostgreSQL binary serving table",
    )
    stage_started_at = time.monotonic()
    storage_data = await finalize_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    timing_by_stage["index_seconds"] = _elapsed_seconds(stage_started_at)
    timing_by_stage["total_seconds"] = _elapsed_seconds(total_started_at)
    return {
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "table": f"{schema_name}.{target_table}",
        "writer": "rust_copy",
        "block_bytes": max_payload_bytes,
        "row_count": int(by_code.get("row_count") or 0),
        "by_code": by_code,
        "by_provider_set": by_provider_set,
        "price_set_atoms": price_set_atoms,
        "timing": timing_by_stage,
        "build_elapsed_seconds": timing_by_stage["total_seconds"],
        "storage": storage_data,
    }


async def write_ptg2_serving_binary_table(
    *,
    schema_name: str,
    source_table: str,
    target_table: str,
    expected_row_count: int | None = None,
    price_set_atom_table: str | None = None,
    artifacts: Mapping[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Create a PostgreSQL-native forward/reverse serving block table."""

    price_forward_artifact = _price_forward_artifact_entry(sidecar_artifacts, artifacts)
    if _serving_binary_rust_enabled() and _ptg2_rust_scanner_binary() is not None:
        if _use_serving_binary_stream():
            try:
                return await _write_serving_binary_rust_stream(
                    schema_name=schema_name,
                    source_table=source_table,
                    target_table=target_table,
                    expected_row_count=expected_row_count,
                    price_set_atom_table=price_set_atom_table,
                    price_forward_artifact=price_forward_artifact,
                    progress_callback=progress_callback,
                )
            except Exception:
                logger.warning("PTG2 Rust streaming serving binary generation failed; falling back to Rust files", exc_info=True)
        try:
            return await _write_ptg2_serving_binary_table_rust(
                schema_name=schema_name,
                source_table=source_table,
                target_table=target_table,
                expected_row_count=expected_row_count,
                price_set_atom_table=price_set_atom_table,
                price_forward_artifact=price_forward_artifact,
                progress_callback=progress_callback,
            )
        except Exception:
            logger.warning("PTG2 Rust serving binary generation failed; falling back to Python", exc_info=True)
    return await _write_ptg2_serving_binary_table_python(
        schema_name=schema_name,
        source_table=source_table,
        target_table=target_table,
        expected_row_count=expected_row_count,
        price_set_atom_table=price_set_atom_table,
        price_forward_artifact=price_forward_artifact,
        progress_callback=progress_callback,
    )
