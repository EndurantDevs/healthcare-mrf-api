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
from process.ptg_parts.config import (
    PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
    _is_postgres_binary_v3_arch,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_VERSION,
    PTG2ManifestArtifactError,
)
from process.ptg_parts.ptg2_serving_binary_v3 import select_atom_key_bits
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary

PTG2_SERVING_BINARY_TABLE_FORMAT = "ptg2_serving_binary_blocks_v1"
PTG2_SERVING_BINARY_BY_CODE_KIND = "by_code"
PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND = "by_code_grouped"
PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND = "by_code_price_dictionary"
PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND = "provider_set_count_dictionary"
PTG2_SERVING_BINARY_BY_PROVIDER_SET_KIND = "by_provider_set"
PTG2_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND = "by_provider_set_price_dictionary"
PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND = "price_set_atoms_by_id_v2"
PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND = "provider_set_codes_v3"
PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND = "price_set_atom_memberships_v3"
PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND = "price_atoms_v3"
PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND = "by_code_assigned_v3"
PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND = "by_code_price_dictionary_v3"
PTG2_SERVING_BINARY_V3_ARTIFACT_VERSION = 3
PTG2_SERVING_BINARY_BLOCK_BYTES_ENV = "HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES"
PTG2_SERVING_BINARY_DICTIONARY_BLOCK_BYTES_ENV = (
    "HLTHPRT_PTG2_SERVING_BINARY_DICTIONARY_BLOCK_BYTES"
)
PTG2_SERVING_BINARY_COPY_RECORDS_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COPY_RECORDS"
PTG2_SERVING_BINARY_RUST_ENV = "HLTHPRT_PTG2_SERVING_BINARY_RUST"
PTG2_SERVING_BINARY_STREAM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_STREAM"
PTG2_SERVING_BINARY_COMBINED_STREAM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COMBINED_STREAM"
PTG2_SERVING_BINARY_STREAM_TASKS_ENV = "HLTHPRT_PTG2_SERVING_BINARY_STREAM_TASKS"
PTG2_SERVING_BINARY_SOURCE_COPY_FORMAT_ENV = "HLTHPRT_PTG2_SERVING_BINARY_SOURCE_COPY_FORMAT"
PTG2_SERVING_BINARY_TARGET_COPY_FORMAT_ENV = "HLTHPRT_PTG2_SERVING_BINARY_TARGET_COPY_FORMAT"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV = "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES"
PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV = (
    "HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT"
)
PTG2_SERVING_BINARY_COPY_WORK_MEM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COPY_WORK_MEM"
DB_POOL_MAX_SIZE_ENV = "HLTHPRT_DB_POOL_MAX_SIZE"
PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED = "keyed"
PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN = "natural_lean"
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
_DEFAULT_DICTIONARY_BLOCK_BYTES = 64 * 1024
_DEFAULT_COPY_RECORDS = 2048
_DEFAULT_STREAM_TASKS = 3
_DEFAULT_COMPRESSION_MIN_BYTES = 128
_DEFAULT_COMPRESSION_MIN_SAVINGS_PCT = 2.0
_DEFAULT_COPY_WORK_MEM = "128MB"
_DEFAULT_DB_POOL_MAX_SIZE = 5
_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES = 2
_PRICE_SET_ATOM_ID_V2_BUCKETS = 1 << (_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES * 8)
_MEMBERSHIP_HEADER = struct.Struct("<8sIQ")
_DENSE_MEMBERSHIP_HEADER = struct.Struct("<8sIQQ")
_MEMBERSHIP_INDEX_RECORD = struct.Struct("<16sQI")
_DENSE_MEMBER_RECORD = struct.Struct("<I")
_STANDARD_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC}
_DENSE_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC}
_WORK_MEM_RE = re.compile(r"^[1-9][0-9]*(?:kB|MB|GB|TB)?$")
_V3_MAX_DENSE_KEY_COUNT = 1 << 32
_V3_ATTRIBUTE_KEY_COLUMNS = (
    "negotiated_type_key",
    "expiration_date_key",
    "service_code_key",
    "billing_class_key",
    "setting_key",
    "billing_code_modifier_key",
    "additional_information_key",
)
_V3_REQUIRED_ARTIFACT_KINDS = frozenset(
    {
        PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
    }
)
_V3_STREAM_ENCODER_KINDS = frozenset(
    {
        PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND,
        PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND,
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
    }
)

logger = logging.getLogger(__name__)

_V3_PUBLISH_PROGRESS_TOTAL = 6


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


@dataclass(frozen=True)
class _V3StageTables:
    price_key_map: str
    atom_key_map: str


@dataclass(frozen=True)
class _V3PublishOptions:
    schema_name: str
    source_table: str
    target_table: str
    expected_row_count: int | None
    price_set_atom_table: str
    price_atom_table: str
    source_layout: str
    code_count_table: str | None
    provider_set_dictionary_table: str | None
    price_atom_table_layout: str | None
    price_atom_constant_keys: Mapping[str, Any]
    progress_callback: Callable[..., None] | None


@dataclass(frozen=True)
class _V3BuildState:
    by_code_summary: Mapping[str, Any]
    stream_summary_by_kind: Mapping[str, Mapping[str, Any]]
    provider_stats: Mapping[str, Any]
    membership_stats: Mapping[str, Any]
    price_map_stats: Mapping[str, Any]
    atom_map_stats: Mapping[str, Any]
    atom_key_bits: int
    timing_by_stage: dict[str, Any]
    started_at: float


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


def _use_serving_binary_combined_stream() -> bool:
    value = os.getenv(PTG2_SERVING_BINARY_COMBINED_STREAM_ENV)
    if value is None:
        return False
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _configured_serving_binary_stream_tasks() -> int:
    try:
        return max(int(os.getenv(PTG2_SERVING_BINARY_STREAM_TASKS_ENV, str(_DEFAULT_STREAM_TASKS))), 1)
    except ValueError:
        return _DEFAULT_STREAM_TASKS


def _serving_binary_stream_tasks() -> int:
    """Limit two-connection COPY pipelines to the configured DB pool."""
    requested_tasks = _configured_serving_binary_stream_tasks()
    try:
        pool_max_size = max(int(os.getenv(DB_POOL_MAX_SIZE_ENV, str(_DEFAULT_DB_POOL_MAX_SIZE))), 1)
    except ValueError:
        pool_max_size = _DEFAULT_DB_POOL_MAX_SIZE
    if pool_max_size < 2:
        raise RuntimeError("PTG2 Rust serving streams require at least two database connections")
    concurrent_pair_capacity = max((pool_max_size - 1) // 2, 1)
    return min(requested_tasks, concurrent_pair_capacity)


def _serving_binary_source_copy_format() -> str:
    value = os.getenv(PTG2_SERVING_BINARY_SOURCE_COPY_FORMAT_ENV, "binary").strip().lower()
    return value if value in {"text", "binary"} else "binary"


def _serving_binary_target_copy_format() -> str:
    value = os.getenv(PTG2_SERVING_BINARY_TARGET_COPY_FORMAT_ENV, "binary").strip().lower()
    return value if value in {"text", "binary"} else "binary"


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


def _compression_min_savings_pct() -> float:
    try:
        return min(
            max(
                float(
                    os.getenv(
                        PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV,
                        str(_DEFAULT_COMPRESSION_MIN_SAVINGS_PCT),
                    )
                ),
                0.0,
            ),
            100.0,
        )
    except ValueError:
        return _DEFAULT_COMPRESSION_MIN_SAVINGS_PCT


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


def _price_set_atom_prefix_bucket(price_set_id: bytes) -> int:
    return int.from_bytes(
        _normalize_128_id(price_set_id)[:_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES],
        "big",
    )


def _price_set_atom_by_id_payload(price_set_id: bytes, atom_ids: tuple[bytes, ...]) -> bytes:
    payload = bytearray(_normalize_128_id(price_set_id))
    _append_uvarint(payload, len(atom_ids))
    for atom_id in atom_ids:
        payload.extend(_normalize_128_id(atom_id))
    return bytes(payload)


def _price_set_atom_membership_payload_bytes(atom_count: int) -> int:
    """Return the exact uncompressed bytes required by one v2 membership."""
    encoded_count_bytes = 1
    remaining_count = int(atom_count)
    while remaining_count >= 0x80:
        remaining_count >>= 7
        encoded_count_bytes += 1
    return 16 + encoded_count_bytes + int(atom_count) * 16


def _assert_price_set_atom_membership_fits(atom_count: int, max_payload_bytes: int) -> None:
    """Reject one membership before it can exceed the bounded block buffer."""
    membership_bytes = _price_set_atom_membership_payload_bytes(atom_count)
    if membership_bytes > max_payload_bytes:
        raise RuntimeError(
            "price-set atom membership requires "
            f"{membership_bytes} bytes, exceeding the maximum single-membership size of "
            f"{max_payload_bytes} bytes"
        )


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
    saved_bytes = len(raw_payload) - len(compressed_payload)
    minimum_savings_pct = _compression_min_savings_pct()
    if saved_bytes <= 0 or (saved_bytes * 100.0) < (len(raw_payload) * minimum_savings_pct):
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
        CREATE TABLE {qualified_target} (
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


async def _drop_ptg2_serving_binary_table(*, schema_name: str, target_table: str) -> None:
    qualified_target = f"{_quote_ident(schema_name)}.{_quote_ident(target_table)}"
    await db.status(f"DROP TABLE IF EXISTS {qualified_target} CASCADE;")


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
            ) AS compressed_saved_bytes,
            CASE (
                SELECT relation.relpersistence
                FROM pg_class relation
                WHERE relation.oid = CAST(:qualified_target AS regclass)
            )
                WHEN 'p' THEN 'logged'
                WHEN 'u' THEN 'unlogged'
                WHEN 't' THEN 'temporary'
                ELSE 'unknown'
            END AS relation_persistence
        FROM {qualified_target}
        """,
        qualified_target=f"{schema_name}.{target_table}",
    )
    storage_mapping = getattr(storage, "_mapping", None)
    storage_data = dict(storage_mapping) if storage_mapping is not None else dict(storage or {})
    if storage_data.get("relation_persistence") != "logged":
        raise RuntimeError(
            f"PTG2 serving binary table is not logged: {schema_name}.{target_table} "
            f"(persistence={storage_data.get('relation_persistence')!r})"
        )
    return storage_data


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


async def _feed_rust_stdin(
    process: Any,
    data: bytes,
    *,
    metrics: dict[str, Any] | None = None,
    started_at: float | None = None,
) -> None:
    """Forward one PostgreSQL COPY chunk into the Rust encoder stdin."""
    if process.stdin is None:
        raise RuntimeError("PTG2 Rust streaming COPY process stdin is closed")
    if metrics is not None:
        metrics["source_copy_bytes"] = int(metrics.get("source_copy_bytes") or 0) + len(data)
        if "source_first_byte_seconds" not in metrics and started_at is not None:
            metrics["source_first_byte_seconds"] = time.monotonic() - started_at
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


async def _rust_stdout_chunks(
    process: Any,
    *,
    metrics: dict[str, Any] | None = None,
    started_at: float | None = None,
):
    """Yield PostgreSQL COPY rows produced by the Rust encoder stdout."""
    if process.stdout is None:
        raise RuntimeError("PTG2 Rust streaming COPY process stdout is closed")
    while True:
        chunk = await process.stdout.read(1024 * 1024)
        if not chunk:
            break
        if metrics is not None:
            metrics["target_copy_bytes"] = int(metrics.get("target_copy_bytes") or 0) + len(chunk)
            if "target_first_byte_seconds" not in metrics and started_at is not None:
                metrics["target_first_byte_seconds"] = time.monotonic() - started_at
        yield chunk
    if metrics is not None and started_at is not None:
        metrics["target_copy_complete_seconds"] = time.monotonic() - started_at


async def _copy_pg_query_to_rust(
    source_driver: Any,
    sql: str,
    process: Any,
    *,
    source_copy_format: str = "text",
    metrics: dict[str, Any] | None = None,
    started_at: float | None = None,
) -> Any:
    """Stream a PostgreSQL COPY query into the Rust encoder process."""
    copy_kwargs: dict[str, Any]
    if source_copy_format == "binary":
        copy_kwargs = {"format": "binary"}
    else:
        copy_kwargs = {"format": "text", "delimiter": "\t", "null": "\\N"}

    async def feed(data: bytes) -> None:
        """Count and forward one PostgreSQL COPY chunk."""
        await _feed_rust_stdin(process, data, metrics=metrics, started_at=started_at)

    try:
        work_mem = _serving_binary_copy_work_mem()
        if work_mem and hasattr(source_driver, "transaction"):
            async with source_driver.transaction():
                await source_driver.execute(f"SET LOCAL work_mem TO '{work_mem}'")
                return await source_driver.copy_from_query(
                    sql,
                    output=feed,
                    **copy_kwargs,
                )
        return await source_driver.copy_from_query(
            sql,
            output=feed,
            **copy_kwargs,
        )
    finally:
        if metrics is not None and started_at is not None:
            metrics["source_copy_complete_seconds"] = time.monotonic() - started_at
        await _close_rust_stdin(process)


async def _cancel_rust_copy_tasks(copy_tasks: list[asyncio.Task[Any]]) -> None:
    """Cancel and drain both sides of a Rust COPY pipeline."""
    for task in copy_tasks:
        task.cancel()
    await asyncio.gather(*copy_tasks, return_exceptions=True)


async def _stop_rust_stream_process(process: Any, stderr_task: asyncio.Task[bytes]) -> bytes:
    """Close stdin, kill a live encoder, and drain its process tasks."""
    if getattr(process, "returncode", None) is None:
        try:
            process.kill()
        except ProcessLookupError:
            logger.debug("PTG2 Rust stream process exited before cancellation")
    await _close_rust_stdin(process)
    await process.wait()
    return await stderr_task


async def _copy_through_rust_process(
    *,
    process: Any,
    sql: str,
    schema_name: str,
    target_table: str,
    source_copy_format: str,
    target_copy_format: str,
    metrics: dict[str, Any],
    started_at: float,
) -> None:
    """Run the database source and target COPY tasks for one encoder."""
    async with db.acquire() as source_conn, db.acquire() as target_conn:
        source_raw = source_conn.raw_connection
        target_raw = target_conn.raw_connection
        source_driver = getattr(source_raw, "driver_connection", source_raw)
        target_driver = getattr(target_raw, "driver_connection", target_raw)
        target_copy_kwargs = (
            {"format": "binary"}
            if target_copy_format == "binary"
            else {"format": "text", "delimiter": "\t", "null": "\\N"}
        )
        copy_tasks: list[asyncio.Task[Any]] = []
        try:
            copy_tasks.append(
                asyncio.create_task(
                    _copy_pg_query_to_rust(
                        source_driver,
                        sql,
                        process,
                        source_copy_format=source_copy_format,
                        metrics=metrics,
                        started_at=started_at,
                    )
                )
            )
            copy_tasks.append(
                asyncio.create_task(
                    target_driver.copy_to_table(
                        target_table,
                        source=_rust_stdout_chunks(process, metrics=metrics, started_at=started_at),
                        schema_name=schema_name,
                        columns=PTG2_SERVING_BINARY_COLUMNS,
                        **target_copy_kwargs,
                    )
                )
            )
            await asyncio.gather(*copy_tasks)
        except (Exception, asyncio.CancelledError):
            await _cancel_rust_copy_tasks(copy_tasks)
            raise


def _rust_stream_process_kind(encoder_kind: str, source_copy_format: str) -> str:
    if encoder_kind in _V3_STREAM_ENCODER_KINDS or source_copy_format != "binary":
        return encoder_kind
    return f"{encoder_kind}_pg_binary"


async def _run_rust_stream_copy(
    *,
    encoder_kind: str,
    failure_label: str,
    sql: str,
    schema_name: str,
    target_table: str,
    source_copy_format: str,
    target_copy_format: str,
    encoder_options: Iterable[str] = (),
) -> dict[str, Any]:
    """Launch one Rust encoder and return its summary with pipeline metrics."""
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(f"PTG2 Rust {failure_label} encoder is enabled but no scanner binary was found")
    process_kind = _rust_stream_process_kind(encoder_kind, source_copy_format)
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--serving-binary-copy-from-key-copy-stdio",
        process_kind,
        *(str(option) for option in encoder_options),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        try:
            process.kill()
        except ProcessLookupError:
            logger.debug("PTG2 Rust stream process exited without complete stdio pipes")
        await process.wait()
        raise RuntimeError(f"PTG2 Rust {failure_label} process did not expose stdio pipes")
    stderr_task = asyncio.create_task(process.stderr.read())
    stream_started_at = time.monotonic()
    stream_metrics_by_name: dict[str, Any] = {}
    try:
        await _copy_through_rust_process(
            process=process, sql=sql, schema_name=schema_name, target_table=target_table,
            source_copy_format=source_copy_format, target_copy_format=target_copy_format,
            metrics=stream_metrics_by_name, started_at=stream_started_at,
        )
        await _close_rust_stdin(process)
        returncode = await process.wait()
        stderr = await stderr_task
    except (Exception, asyncio.CancelledError):
        await _stop_rust_stream_process(process, stderr_task)
        raise
    if returncode != 0:
        raise RuntimeError(
            f"PTG2 Rust {failure_label} encoder failed: "
            f"stderr={stderr.decode('utf-8', errors='replace')[-2000:]}"
        )
    summary = _parse_serving_binary_stream_summary(stderr)
    summary.update(stream_metrics_by_name)
    summary["pipeline_seconds"] = time.monotonic() - stream_started_at
    return summary


async def _stream_serving_binary_copy(
    *, kind: str, sql: str, schema_name: str, target_table: str,
    source_copy_format: str = "text", target_copy_format: str = "text",
    encoder_options: Iterable[str] = (),
) -> dict[str, Any]:
    """Pipe one ordered PostgreSQL stream through a Rust block encoder."""
    return await _run_rust_stream_copy(
        encoder_kind=kind, failure_label=f"streaming serving binary COPY for {kind}",
        sql=sql, schema_name=schema_name, target_table=target_table,
        source_copy_format=source_copy_format, target_copy_format=target_copy_format,
        encoder_options=encoder_options,
    )


async def _stream_serving_binary_combined_copy(
    *, sql: str, schema_name: str, target_table: str,
    source_copy_format: str = "text", target_copy_format: str = "text",
) -> dict[str, Any]:
    """Pipe one by-code stream through the combined forward/reverse encoder."""
    return await _run_rust_stream_copy(
        encoder_kind="combined", failure_label="combined serving binary COPY",
        sql=sql, schema_name=schema_name, target_table=target_table,
        source_copy_format=source_copy_format, target_copy_format=target_copy_format,
    )


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
class _PriceSetAtomPrefixBlockWriter:
    """Write prefix-bucketed price-set memberships in source-id order."""

    schema_name: str
    table_name: str
    max_payload_bytes: int
    batch_size: int
    batch_records: list[tuple[Any, ...]] = field(default_factory=list)
    copied_records: int = 0
    block_count: int = 0
    price_set_count: int = 0
    atom_ref_count: int = 0
    current_bucket_key: int | None = None
    current_block_no: int = 0
    current_payload: bytearray = field(default_factory=bytearray)
    current_entry_count: int = 0

    async def flush_batch(self) -> None:
        """COPY buffered v2 membership rows into PostgreSQL."""

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
        """Append one encoded block and flush a complete COPY batch."""

        self.batch_records.append(record)
        if len(self.batch_records) >= self.batch_size:
            await self.flush_batch()

    async def flush_block(self) -> None:
        """Flush the current prefix bucket block."""

        if self.current_bucket_key is None or self.current_entry_count <= 0:
            return
        await self.append_record(
            _serving_binary_record(
                PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
                self.current_bucket_key,
                self.current_block_no,
                self.current_entry_count,
                self.current_payload,
            )
        )
        self.block_count += 1
        self.current_block_no += 1
        self.current_entry_count = 0
        self.current_payload = bytearray()

    async def add_price_set(self, price_set_id: bytes, atom_ids: tuple[bytes, ...]) -> None:
        """Append one membership entry to its monotonic ID-prefix bucket."""

        bucket_key = _price_set_atom_prefix_bucket(price_set_id)
        if self.current_bucket_key is not None and bucket_key < self.current_bucket_key:
            raise RuntimeError("price-set atom rows must be ordered by price_set_global_id_128")
        _assert_price_set_atom_membership_fits(len(atom_ids), self.max_payload_bytes)
        entry_payload = _price_set_atom_by_id_payload(price_set_id, atom_ids)
        if self.current_bucket_key != bucket_key:
            await self.flush_block()
            self.current_bucket_key = bucket_key
            self.current_block_no = 0
        elif self.current_entry_count > 0 and len(self.current_payload) + len(entry_payload) > self.max_payload_bytes:
            await self.flush_block()
        self.current_payload.extend(entry_payload)
        self.current_entry_count += 1
        self.price_set_count += 1
        self.atom_ref_count += len(atom_ids)

    async def finish(self) -> None:
        """Flush the final prefix block and COPY batch."""

        await self.flush_block()
        await self.flush_batch()

    def summary(self) -> dict[str, Any]:
        """Return counts and format metadata for the generated blocks."""

        return {
            "format": "price_set_atoms_by_id_v2",
            "artifact_kind": PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
            "id_prefix_bytes": _PRICE_SET_ATOM_ID_V2_PREFIX_BYTES,
            "id_bucket_count": _PRICE_SET_ATOM_ID_V2_BUCKETS,
            "id_block_count": self.block_count,
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

    if price_set_atom_table:
        return await _write_atom_map_blocks_from_table(
            schema_name=schema_name,
            table_name=table_name,
            price_set_atom_table=price_set_atom_table,
            max_payload_bytes=max_payload_bytes,
        )

    sidecar_path = _price_forward_artifact_path(price_forward_artifact)
    if sidecar_path is None:
        return {"skipped": "missing_price_forward_sidecar"}
    block_writer = _PriceSetAtomPrefixBlockWriter(
        schema_name=schema_name,
        table_name=table_name,
        max_payload_bytes=max_payload_bytes,
        batch_size=_serving_binary_copy_records(),
    )
    for owner_id, atom_ids in _iter_price_forward_members(sidecar_path, metadata=price_forward_artifact):
        await block_writer.add_price_set(owner_id, atom_ids)
    await block_writer.finish()
    summary = block_writer.summary()
    summary["source"] = "price_forward_sidecar"
    return summary


async def _write_atom_map_blocks_from_table(
    *,
    schema_name: str,
    table_name: str,
    price_set_atom_table: str,
    max_payload_bytes: int,
) -> dict[str, Any]:
    """Encode staged price-set atom rows as PostgreSQL serving-binary blocks."""

    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(price_set_atom_table)}"
    block_writer = _PriceSetAtomPrefixBlockWriter(
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
        await block_writer.add_price_set(current_price_set_id, tuple(current_atom_ids))

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
            _assert_price_set_atom_membership_fits(len(current_atom_ids) + 1, max_payload_bytes)
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
    source_layout: str = PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
    code_count_table: str | None = None,
    provider_set_dictionary_table: str | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    source_copy_format = _serving_binary_source_copy_format()
    target_copy_format = _serving_binary_target_copy_format()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    qualified_code_count_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(code_count_table)}" if code_count_table else None
    )
    qualified_provider_set_dictionary_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}"
        if provider_set_dictionary_table
        else None
    )
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
                    _serving_binary_stream_sql(
                        qualified_source=qualified_source,
                        kind="by_code",
                        source_layout=source_layout,
                        qualified_code_count_table=qualified_code_count_table,
                        qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
                    )
                )
            )
            async for row in result:
                yield row

    async def by_provider_set_rows():
        async with db.session() as session:
            result = await session.stream(
                db.text(
                    _serving_binary_stream_sql(
                        qualified_source=qualified_source,
                        kind="by_provider_set",
                        source_layout=source_layout,
                        qualified_code_count_table=qualified_code_count_table,
                        qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
                    )
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


def _serving_binary_stream_sql(
    *,
    qualified_source: str,
    kind: str,
    source_layout: str = PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
    qualified_code_count_table: str | None = None,
    qualified_provider_set_dictionary_table: str | None = None,
) -> str:
    """Return the ordered serving-row query required by one stream encoder."""
    if source_layout == PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN:
        if not qualified_code_count_table or not qualified_provider_set_dictionary_table:
            raise ValueError("natural lean serving binary streams require dictionary tables")
        if kind == "by_code":
            return f"""
            SELECT
                code_count.code_key,
                provider_set_dictionary.provider_set_key,
                serving.provider_count,
                serving.price_set_global_id_128
            FROM {qualified_source} serving
            JOIN {qualified_code_count_table} code_count
              ON code_count.plan_id = serving.plan_id
             AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
             AND code_count.reported_code = serving.reported_code
            JOIN {qualified_provider_set_dictionary_table} provider_set_dictionary
              ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128
            ORDER BY code_count.code_key, provider_set_dictionary.provider_set_key, serving.price_set_global_id_128
            """
        if kind == "by_provider_set":
            return f"""
            SELECT
                provider_set_dictionary.provider_set_key,
                code_count.code_key,
                serving.provider_count,
                serving.price_set_global_id_128
            FROM {qualified_source} serving
            JOIN {qualified_code_count_table} code_count
              ON code_count.plan_id = serving.plan_id
             AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
             AND code_count.reported_code = serving.reported_code
            JOIN {qualified_provider_set_dictionary_table} provider_set_dictionary
              ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128
            ORDER BY provider_set_dictionary.provider_set_key, code_count.code_key, serving.price_set_global_id_128
            """
        raise ValueError(f"unsupported serving binary stream kind: {kind}")

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


def _price_set_atom_stream_sql(*, qualified_source: str) -> str:
    """Return the authoritative ordered price-set atom pair query."""
    return f"""
    SELECT price_set_global_id_128, price_atom_global_id_128
    FROM {qualified_source}
    ORDER BY price_set_global_id_128, price_atom_global_id_128
    """


def _stream_stage_sql(
    *,
    config: _StreamStageConfig,
    qualified_source: str,
    source_layout: str,
    qualified_code_count_table: str | None,
    qualified_provider_set_dictionary_table: str | None,
    qualified_price_set_atom_table: str | None,
) -> str:
    """Resolve the ordered PostgreSQL source query for one Rust stream."""
    if config.kind == PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND:
        if qualified_price_set_atom_table is None:
            raise ValueError("price-set atom stream requires an authoritative PostgreSQL table")
        return _price_set_atom_stream_sql(qualified_source=qualified_price_set_atom_table)
    return _serving_binary_stream_sql(
        qualified_source=qualified_source,
        kind=config.kind,
        source_layout=source_layout,
        qualified_code_count_table=qualified_code_count_table,
        qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
    )


async def _stream_serving_binary_stage(
    *,
    config: _StreamStageConfig,
    progress_callback: Callable[..., None] | None,
    expected_row_count: int | None,
    qualified_source: str,
    source_layout: str,
    qualified_code_count_table: str | None,
    qualified_provider_set_dictionary_table: str | None,
    qualified_price_set_atom_table: str | None,
    schema_name: str,
    target_table: str,
    source_copy_format: str,
    target_copy_format: str,
    timing_by_stage: dict[str, float],
) -> dict[str, Any]:
    """Run one ordered PostgreSQL-to-Rust-to-PostgreSQL streaming stage."""
    stage_expected_row_count = (
        None if config.kind == PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND else expected_row_count
    )
    _emit_progress(
        progress_callback,
        config.progress_step,
        done=config.progress_done,
        total=config.progress_total,
        message=config.progress_message,
        expected_row_count=stage_expected_row_count,
    )
    stage_started_at = time.monotonic()
    stage_result = await _stream_serving_binary_copy(
        kind=config.kind,
        sql=_stream_stage_sql(
            config=config,
            qualified_source=qualified_source,
            source_layout=source_layout,
            qualified_code_count_table=qualified_code_count_table,
            qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
            qualified_price_set_atom_table=qualified_price_set_atom_table,
        ),
        schema_name=schema_name,
        target_table=target_table,
        source_copy_format=source_copy_format,
        target_copy_format=target_copy_format,
    )
    if config.kind == PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND:
        stage_result["source"] = "price_set_atom_table"
    timing_by_stage[config.timing_key] = _elapsed_seconds(stage_started_at)
    return stage_result


def _price_set_atom_stream_stage_config() -> _StreamStageConfig:
    """Return the Rust stream definition for the v2 atom lookup blocks."""
    return _StreamStageConfig(
        kind=PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
        progress_step="serving binary stream price atoms",
        progress_done=2,
        progress_total=6,
        progress_message="streaming PostgreSQL price-set atom COPY through Rust",
        timing_key="price_set_atoms_stream_seconds",
    )


def _serving_binary_stream_stage_configs(*, include_price_set_atoms: bool) -> tuple[_StreamStageConfig, ...]:
    """Return the Rust stages for bidirectional serving and optional atom lookup."""
    stage_configs = [
        _StreamStageConfig(
            kind="by_code",
            progress_step="serving binary stream by code",
            progress_done=1,
            progress_total=6,
            progress_message="streaming PostgreSQL by-code COPY through Rust",
            timing_key="by_code_stream_seconds",
        ),
        _StreamStageConfig(
            kind="by_provider_set",
            progress_step="serving binary stream reverse",
            progress_done=3,
            progress_total=6,
            progress_message="streaming PostgreSQL reverse COPY through Rust",
            timing_key="reverse_stream_seconds",
        ),
    ]
    if include_price_set_atoms:
        stage_configs.insert(1, _price_set_atom_stream_stage_config())
    return tuple(stage_configs)


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
    source_layout: str,
    qualified_code_count_table: str | None,
    qualified_provider_set_dictionary_table: str | None,
    qualified_price_set_atom_table: str | None,
    schema_name: str,
    target_table: str,
    source_copy_format: str,
    target_copy_format: str,
    timing_by_stage: dict[str, float],
) -> dict[str, dict[str, Any]]:
    """Run serving-binary encoders with the configured concurrency limit."""
    if stream_tasks <= 1:
        stage_result_by_kind: dict[str, dict[str, Any]] = {}
        for config in stage_configs:
            stage_result_by_kind[config.kind] = await _stream_serving_binary_stage(
                config=config,
                progress_callback=progress_callback,
                expected_row_count=expected_row_count,
                qualified_source=qualified_source,
                source_layout=source_layout,
                qualified_code_count_table=qualified_code_count_table,
                qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
                qualified_price_set_atom_table=qualified_price_set_atom_table,
                schema_name=schema_name,
                target_table=target_table,
                source_copy_format=source_copy_format,
                target_copy_format=target_copy_format,
                timing_by_stage=timing_by_stage,
            )
        return stage_result_by_kind

    stream_semaphore = asyncio.Semaphore(stream_tasks)

    async def run_stage(config: _StreamStageConfig) -> dict[str, Any]:
        """Run one stage after acquiring a stream concurrency slot."""
        async with stream_semaphore:
            return await _stream_serving_binary_stage(
                config=config,
                progress_callback=progress_callback,
                expected_row_count=expected_row_count,
                qualified_source=qualified_source,
                source_layout=source_layout,
                qualified_code_count_table=qualified_code_count_table,
                qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
                qualified_price_set_atom_table=qualified_price_set_atom_table,
                schema_name=schema_name,
                target_table=target_table,
                source_copy_format=source_copy_format,
                target_copy_format=target_copy_format,
                timing_by_stage=timing_by_stage,
            )

    stage_task_by_kind = {config.kind: asyncio.create_task(run_stage(config)) for config in stage_configs}
    try:
        stage_results = await asyncio.gather(*stage_task_by_kind.values())
    except (Exception, asyncio.CancelledError):
        for task in stage_task_by_kind.values():
            task.cancel()
        await asyncio.gather(*stage_task_by_kind.values(), return_exceptions=True)
        raise
    return dict(zip(stage_task_by_kind.keys(), stage_results, strict=True))


def _v3_stage_tables(target_table: str) -> _V3StageTables:
    return _V3StageTables(
        price_key_map=_short_index_name(target_table, "v3_price_key_map"),
        atom_key_map=_short_index_name(target_table, "v3_atom_key_map"),
    )


async def _drop_v3_stage_tables(*, schema_name: str, stage_tables: _V3StageTables) -> None:
    qualified_tables = ", ".join(
        f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        for table_name in (stage_tables.price_key_map, stage_tables.atom_key_map)
    )
    await db.status(f"DROP TABLE IF EXISTS {qualified_tables} CASCADE;")


async def _validate_v3_dense_map(
    *, schema_name: str, table_name: str, id_column: str, key_column: str
) -> dict[str, int | None]:
    dense_row = await db.first(
        f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            COUNT(DISTINCT {_quote_ident(id_column)})::bigint AS distinct_id_count,
            COUNT(DISTINCT {_quote_ident(key_column)})::bigint AS distinct_key_count,
            MIN({_quote_ident(key_column)})::bigint AS minimum_key,
            MAX({_quote_ident(key_column)})::bigint AS maximum_key
        FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}
        """
    )
    dense_stat_by_name = {
        "row_count": int(_row_value(dense_row, "row_count", 0) or 0),
        "distinct_id_count": int(_row_value(dense_row, "distinct_id_count", 1) or 0),
        "distinct_key_count": int(_row_value(dense_row, "distinct_key_count", 2) or 0),
        "minimum_key": _row_value(dense_row, "minimum_key", 3),
        "maximum_key": _row_value(dense_row, "maximum_key", 4),
    }
    expected_count = int(dense_stat_by_name["row_count"] or 0)
    expected_minimum = 0 if expected_count else None
    expected_maximum = expected_count - 1 if expected_count else None
    observed_counts = {
        dense_stat_by_name[name]
        for name in ("row_count", "distinct_id_count", "distinct_key_count")
    }
    if observed_counts != {expected_count}:
        raise RuntimeError(
            f"PTG2 v3 dense map count mismatch for {schema_name}.{table_name}: "
            f"{dense_stat_by_name}"
        )
    if (
        dense_stat_by_name["minimum_key"] != expected_minimum
        or dense_stat_by_name["maximum_key"] != expected_maximum
    ):
        raise RuntimeError(
            f"PTG2 v3 dense map bounds mismatch for {schema_name}.{table_name}: "
            f"{dense_stat_by_name}"
        )
    return dense_stat_by_name


async def _create_v3_price_key_stage(
    *, schema_name: str, price_set_atom_table: str, stage_table: str
) -> dict[str, int | None]:
    # V3 plans a large DISTINCT over this transient table. Without fresh
    # cardinality statistics PostgreSQL can underestimate dense sources by an
    # order of magnitude and choose a heavily spilling aggregate.
    await db.status(
        f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(price_set_atom_table)};"
    )
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} AS
        SELECT
            price_set_global_id_128,
            (ROW_NUMBER() OVER (ORDER BY price_set_global_id_128) - 1)::bigint AS price_key
        FROM (
            SELECT DISTINCT price_set_global_id_128
            FROM {_quote_ident(schema_name)}.{_quote_ident(price_set_atom_table)}
        ) distinct_price_set
        ORDER BY price_set_global_id_128;
        """
    )
    await db.status(
        f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
        "ALTER COLUMN price_set_global_id_128 SET NOT NULL, ALTER COLUMN price_key SET NOT NULL;"
    )
    await db.status(
        f"CREATE UNIQUE INDEX {_quote_ident(_short_index_name(stage_table, 'id_uidx'))} "
        f"ON {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (price_set_global_id_128);"
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    dense_stat_by_name = await _validate_v3_dense_map(
        schema_name=schema_name,
        table_name=stage_table,
        id_column="price_set_global_id_128",
        key_column="price_key",
    )
    if int(dense_stat_by_name["row_count"] or 0) > _V3_MAX_DENSE_KEY_COUNT:
        raise RuntimeError("PTG2 v3 supports at most 2^32 canonical price keys")
    return dense_stat_by_name


async def _create_v3_atom_key_stage(
    *, schema_name: str, price_atom_table: str, stage_table: str
) -> dict[str, int | None]:
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} AS
        SELECT
            price_atom_global_id_128,
            (ROW_NUMBER() OVER (ORDER BY price_atom_global_id_128) - 1)::bigint AS atom_key
        FROM {_quote_ident(schema_name)}.{_quote_ident(price_atom_table)}
        ORDER BY price_atom_global_id_128;
        """
    )
    await db.status(
        f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
        "ALTER COLUMN price_atom_global_id_128 SET NOT NULL, ALTER COLUMN atom_key SET NOT NULL;"
    )
    await db.status(
        f"CREATE UNIQUE INDEX {_quote_ident(_short_index_name(stage_table, 'id_uidx'))} "
        f"ON {_quote_ident(schema_name)}.{_quote_ident(stage_table)} (price_atom_global_id_128);"
    )
    await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
    dense_stat_by_name = await _validate_v3_dense_map(
        schema_name=schema_name,
        table_name=stage_table,
        id_column="price_atom_global_id_128",
        key_column="atom_key",
    )
    select_atom_key_bits(int(dense_stat_by_name["row_count"] or 0))
    return dense_stat_by_name


def _v3_assigned_by_code_sql(
    *,
    qualified_source: str,
    qualified_price_key_map: str,
    source_layout: str,
    qualified_code_count_table: str | None,
    qualified_provider_set_dictionary_table: str | None,
) -> str:
    """Return forward rows joined to the one canonical dense price map."""
    if source_layout == PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN:
        if not qualified_code_count_table or not qualified_provider_set_dictionary_table:
            raise ValueError("natural lean v3 streams require dictionary tables")
        return f"""
        SELECT
            code_count.code_key,
            provider_set_dictionary.provider_set_key,
            serving.provider_count,
            price_key_map.price_key
        FROM {qualified_source} serving
        JOIN {qualified_code_count_table} code_count
          ON code_count.plan_id = serving.plan_id
         AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
         AND code_count.reported_code = serving.reported_code
        JOIN {qualified_provider_set_dictionary_table} provider_set_dictionary
          ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128
        JOIN {qualified_price_key_map} price_key_map
          ON price_key_map.price_set_global_id_128 = serving.price_set_global_id_128
        ORDER BY code_count.code_key, provider_set_dictionary.provider_set_key, price_key_map.price_key
        """
    if source_layout != PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED:
        raise ValueError(f"unsupported v3 serving source layout: {source_layout}")
    return f"""
    SELECT serving.code_key, serving.provider_set_key, serving.provider_count, price_key_map.price_key
    FROM {qualified_source} serving
    JOIN {qualified_price_key_map} price_key_map
      ON price_key_map.price_set_global_id_128 = serving.price_set_global_id_128
    ORDER BY serving.code_key, serving.provider_set_key, price_key_map.price_key
    """


def _v3_price_dictionary_sql(*, qualified_price_key_map: str) -> str:
    """Return the canonical price dictionary in dense-key order."""
    return f"""
    SELECT price_key, price_set_global_id_128
    FROM {qualified_price_key_map}
    ORDER BY price_set_global_id_128
    """


def _v3_provider_set_codes_sql(
    *,
    qualified_source: str,
    source_layout: str,
    qualified_code_count_table: str | None,
    qualified_provider_set_dictionary_table: str | None,
) -> str:
    """Return distinct provider/code edges in the v3 Rust encoder order."""
    if source_layout == PTG2_SERVING_BINARY_SOURCE_LAYOUT_NATURAL_LEAN:
        if not qualified_code_count_table or not qualified_provider_set_dictionary_table:
            raise ValueError("natural lean v3 streams require dictionary tables")
        return f"""
        SELECT DISTINCT
            provider_set_dictionary.provider_set_key,
            code_count.code_key
        FROM {qualified_source} serving
        JOIN {qualified_code_count_table} code_count
          ON code_count.plan_id = serving.plan_id
         AND code_count.reported_code_system IS NOT DISTINCT FROM serving.reported_code_system
         AND code_count.reported_code = serving.reported_code
        JOIN {qualified_provider_set_dictionary_table} provider_set_dictionary
          ON provider_set_dictionary.provider_set_global_id_128 = serving.provider_set_global_id_128
        ORDER BY provider_set_dictionary.provider_set_key, code_count.code_key
        """
    if source_layout != PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED:
        raise ValueError(f"unsupported v3 serving source layout: {source_layout}")
    return f"""
    SELECT DISTINCT provider_set_key, code_key
    FROM {qualified_source}
    ORDER BY provider_set_key, code_key
    """


def _v3_price_membership_sql(
    *,
    qualified_price_set_atom_table: str,
    qualified_price_key_map: str,
    qualified_atom_key_map: str,
) -> str:
    """Return distinct canonical price/atom edges in encoder order."""
    return f"""
    SELECT DISTINCT price_key_map.price_key, atom_key_map.atom_key
    FROM {qualified_price_set_atom_table} price_set_atom
    LEFT JOIN {qualified_price_key_map} price_key_map
      ON price_key_map.price_set_global_id_128 = price_set_atom.price_set_global_id_128
    LEFT JOIN {qualified_atom_key_map} atom_key_map
      ON atom_key_map.price_atom_global_id_128 = price_set_atom.price_atom_global_id_128
    ORDER BY price_key_map.price_key, atom_key_map.atom_key
    """


def _v3_price_atom_sql(
    *,
    qualified_price_atom_table: str,
    qualified_atom_key_map: str,
    constant_key_by_column: Mapping[str, Any] | None,
) -> str:
    """Return dense atom payload rows with all seven dictionary keys."""
    constant_key_by_name = dict(constant_key_by_column or {})
    attribute_expressions = []
    for column_name in _V3_ATTRIBUTE_KEY_COLUMNS:
        if column_name in constant_key_by_name:
            attribute_expressions.append(
                f"{int(constant_key_by_name[column_name])}::bigint AS {column_name}"
            )
        else:
            attribute_expressions.append(f"price_atom.{column_name}::bigint AS {column_name}")
    return f"""
    SELECT
        atom_key_map.atom_key,
        price_atom.negotiated_rate,
        {", ".join(attribute_expressions)}
    FROM {qualified_atom_key_map} atom_key_map
    JOIN {qualified_price_atom_table} price_atom
      ON price_atom.price_atom_global_id_128 = atom_key_map.price_atom_global_id_128
    ORDER BY atom_key_map.price_atom_global_id_128
    """


def _report_v3_stream_progress(
    progress_callback: Callable[..., None] | None,
    completed_stream_count: int,
    stream_summary: Mapping[str, Any],
) -> None:
    artifact_kind = str(stream_summary.get("artifact_kind") or stream_summary.get("kind") or "")
    _emit_progress(
        progress_callback,
        "serving binary artifact stream",
        done=1 + completed_stream_count,
        total=_V3_PUBLISH_PROGRESS_TOTAL,
        message=f"completed PostgreSQL binary artifact stream {artifact_kind}",
        artifact_kind=artifact_kind,
    )


async def _run_v3_streams(
    *,
    sql_by_kind: Mapping[str, str],
    schema_name: str,
    target_table: str,
    target_copy_format: str,
    atom_count: int,
    atom_key_bits: int,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, dict[str, Any]]:
    """Run v3 artifact encoders concurrently and report each completed stream."""
    stream_limit = min(_serving_binary_stream_tasks(), len(sql_by_kind))
    stream_semaphore = asyncio.Semaphore(stream_limit)
    if select_atom_key_bits(atom_count) != atom_key_bits:
        raise RuntimeError("PTG2 v3 atom-key width changed after map validation")

    async def _stream_kind(kind: str, sql: str) -> dict[str, Any]:
        encoder_options = (
            (str(atom_key_bits),)
            if kind
            in {
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
                PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
            }
            else ()
        )
        async with stream_semaphore:
            return await _stream_serving_binary_copy(
                kind=kind,
                sql=sql,
                schema_name=schema_name,
                target_table=target_table,
                source_copy_format="binary",
                target_copy_format=target_copy_format,
                encoder_options=encoder_options,
            )

    task_by_kind = {
        kind: asyncio.create_task(_stream_kind(kind, sql)) for kind, sql in sql_by_kind.items()
    }
    try:
        completed_stream_count = 0
        for completed_stream in asyncio.as_completed(task_by_kind.values()):
            stream_summary = await completed_stream
            completed_stream_count += 1
            _report_v3_stream_progress(progress_callback, completed_stream_count, stream_summary)
        stream_summaries = [stream_task.result() for stream_task in task_by_kind.values()]
    except (Exception, asyncio.CancelledError):
        for stream_task in task_by_kind.values():
            stream_task.cancel()
        await asyncio.gather(*task_by_kind.values(), return_exceptions=True)
        raise
    return dict(zip(task_by_kind.keys(), stream_summaries, strict=True))


def _v3_summary_integer(summary: Mapping[str, Any], label: str, *field_names: str) -> int:
    for field_name in field_names:
        if summary.get(field_name) is not None:
            try:
                return int(summary[field_name])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"PTG2 v3 {label} summary has invalid {field_name}") from exc
    raise RuntimeError(f"PTG2 v3 {label} summary is missing {' or '.join(field_names)}")


def _validate_v3_summary_kind(summary: Mapping[str, Any], expected_kind: str) -> None:
    artifact_kind = str(summary.get("artifact_kind") or summary.get("kind") or "")
    if artifact_kind != expected_kind:
        raise RuntimeError(
            f"PTG2 v3 stream summary kind mismatch: expected {expected_kind!r}, got {artifact_kind!r}"
        )


def _validate_v3_atom_width(summary: Mapping[str, Any], label: str, expected_bits: int) -> None:
    summary_bits = summary.get("atom_key_bits")
    summary_bytes = summary.get("atom_key_bytes")
    if summary_bits is None and summary_bytes is None:
        raise RuntimeError(f"PTG2 v3 {label} summary is missing atom-key width")
    if summary_bits is not None and int(summary_bits) != expected_bits:
        raise RuntimeError(f"PTG2 v3 {label} atom-key width mismatch")
    if summary_bytes is not None and int(summary_bytes) != expected_bits // 8:
        raise RuntimeError(f"PTG2 v3 {label} atom-key byte width mismatch")


def _v3_summary_optional_integer(
    summary: Mapping[str, Any], label: str, field_name: str
) -> int | None:
    if field_name not in summary:
        raise RuntimeError(f"PTG2 v3 {label} summary is missing {field_name}")
    value = summary[field_name]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"PTG2 v3 {label} summary has invalid {field_name}") from exc


def _validate_v3_assigned_summary(
    summary: Mapping[str, Any], *, expected_row_count: int | None, price_set_count: int
) -> None:
    if expected_row_count is None:
        raise RuntimeError("PTG2 v3 assigned by-code validation requires an exact row count")
    _validate_stream_row_count(
        label="v3 assigned by-code",
        expected_row_count=expected_row_count,
        stream_result=summary,
    )
    _validate_v3_summary_kind(summary, PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND)
    price_key_upper_bound = _v3_summary_integer(
        summary, "assigned by-code", "price_key_upper_bound"
    )
    if _v3_summary_integer(summary, "assigned by-code", "price_set_count") != price_key_upper_bound:
        raise RuntimeError("PTG2 v3 assigned by-code price-key summary mismatch")
    if price_key_upper_bound > price_set_count:
        raise RuntimeError("PTG2 v3 assigned by-code price-key exceeds the canonical map")
    maximum_price_key = _v3_summary_optional_integer(
        summary, "assigned by-code", "maximum_price_key"
    )
    expected_maximum = price_key_upper_bound - 1 if price_key_upper_bound else None
    if maximum_price_key != expected_maximum:
        raise RuntimeError("PTG2 v3 assigned by-code price-key bounds mismatch")


def _validate_v3_price_dictionary_summary(
    summary: Mapping[str, Any], *, price_set_count: int
) -> None:
    _validate_v3_summary_kind(summary, PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND)
    if _v3_summary_integer(summary, "price dictionary", "row_count") != price_set_count:
        raise RuntimeError("PTG2 v3 price dictionary row count mismatch")
    if _v3_summary_integer(summary, "price dictionary", "price_set_count") != price_set_count:
        raise RuntimeError("PTG2 v3 price dictionary price-set count mismatch")
    if _v3_summary_integer(summary, "price dictionary", "id_bytes") != 16:
        raise RuntimeError("PTG2 v3 price dictionary ID width mismatch")


def _v3_provider_stats_from_summary(summary: Mapping[str, Any]) -> dict[str, int]:
    _validate_v3_summary_kind(summary, PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND)
    row_count = _v3_summary_integer(summary, "provider codes", "row_count")
    pair_count = _v3_summary_integer(summary, "provider codes", "pair_count")
    code_count = _v3_summary_integer(summary, "provider codes", "code_count")
    duplicate_pair_count = _v3_summary_integer(
        summary, "provider codes", "duplicate_pair_count"
    )
    provider_set_count = _v3_summary_integer(
        summary, "provider codes", "provider_set_count"
    )
    if row_count != pair_count or pair_count != code_count:
        raise RuntimeError("PTG2 v3 provider-code stream pair count mismatch")
    if duplicate_pair_count != 0:
        raise RuntimeError("PTG2 v3 provider-code stream contains duplicate pairs")
    if provider_set_count > pair_count:
        raise RuntimeError("PTG2 v3 provider-code stream provider count exceeds pair count")
    return {
        "row_count": row_count,
        "pair_count": pair_count,
        "provider_set_count": provider_set_count,
        "duplicate_pair_count": duplicate_pair_count,
    }


def _v3_membership_stats_from_summary(
    summary: Mapping[str, Any],
    *,
    price_set_count: int,
    atom_count: int,
    atom_key_bits: int,
) -> dict[str, int | None]:
    _validate_v3_summary_kind(summary, PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND)
    row_count = _v3_summary_integer(summary, "price memberships", "row_count")
    atom_reference_count = _v3_summary_integer(
        summary, "price memberships", "atom_reference_count"
    )
    streamed_price_set_count = _v3_summary_integer(
        summary, "price memberships", "price_set_count"
    )
    maximum_price_key = _v3_summary_optional_integer(
        summary, "price memberships", "maximum_price_key"
    )
    expected_maximum_price_key = price_set_count - 1 if price_set_count else None
    if row_count != atom_reference_count:
        raise RuntimeError("PTG2 v3 price-membership reference count mismatch")
    if streamed_price_set_count != price_set_count:
        raise RuntimeError("PTG2 v3 price-membership stream price-set count mismatch")
    if maximum_price_key != expected_maximum_price_key:
        raise RuntimeError("PTG2 v3 price-membership stream dense price-key bounds mismatch")
    if row_count < streamed_price_set_count:
        raise RuntimeError("PTG2 v3 price-membership stream omitted a price-set membership")
    if atom_count == 0 and row_count != 0:
        raise RuntimeError("PTG2 v3 price-membership stream references an empty atom map")
    _validate_v3_atom_width(summary, "price memberships", atom_key_bits)
    return {
        "row_count": row_count,
        "atom_reference_count": atom_reference_count,
        "price_set_count": streamed_price_set_count,
        "maximum_price_key": maximum_price_key,
    }


def _validate_v3_atom_summary(
    summary: Mapping[str, Any], *, atom_count: int, atom_key_bits: int
) -> None:
    _validate_v3_summary_kind(summary, PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND)
    if _v3_summary_integer(summary, "price atoms", "atom_count", "row_count") != atom_count:
        raise RuntimeError("PTG2 v3 price-atom stream row count mismatch")
    if _v3_summary_integer(summary, "price atoms", "attribute_count") != len(_V3_ATTRIBUTE_KEY_COLUMNS):
        raise RuntimeError("PTG2 v3 price-atom stream attribute width mismatch")
    _validate_v3_atom_width(summary, "price atoms", atom_key_bits)


async def _v3_physical_artifact_summaries(
    *, schema_name: str, target_table: str
) -> dict[str, dict[str, int]]:
    artifact_rows = await db.all(
        f"""
        SELECT
            artifact_kind,
            COUNT(*)::bigint AS record_count,
            COALESCE(SUM(entry_count), 0)::bigint AS entry_count,
            COALESCE(SUM(octet_length(payload)), 0)::bigint AS stored_payload_bytes,
            COALESCE(SUM(
                CASE
                    WHEN raw_payload_bytes > 0 THEN raw_payload_bytes
                    ELSE octet_length(payload)
                END
            ), 0)::bigint AS raw_payload_bytes,
            COALESCE(SUM(NULLIF(raw_payload_bytes, 0)), 0)::bigint AS compressed_raw_payload_bytes
        FROM {_quote_ident(schema_name)}.{_quote_ident(target_table)}
        GROUP BY artifact_kind
        ORDER BY artifact_kind
        """
    )
    artifact_summary_by_kind = {}
    for artifact_row in artifact_rows:
        artifact_kind = str(_row_value(artifact_row, "artifact_kind", 0) or "")
        artifact_summary_by_kind[artifact_kind] = {
            "record_count": int(_row_value(artifact_row, "record_count", 1) or 0),
            "entry_count": int(_row_value(artifact_row, "entry_count", 2) or 0),
            "stored_payload_bytes": int(_row_value(artifact_row, "stored_payload_bytes", 3) or 0),
            "raw_payload_bytes": int(_row_value(artifact_row, "raw_payload_bytes", 4) or 0),
            "compressed_raw_payload_bytes": int(
                _row_value(artifact_row, "compressed_raw_payload_bytes", 5) or 0
            ),
        }
    observed_kinds = frozenset(artifact_summary_by_kind)
    if observed_kinds != _V3_REQUIRED_ARTIFACT_KINDS:
        raise RuntimeError(
            "PTG2 v3 persisted artifact kinds mismatch: "
            f"expected {sorted(_V3_REQUIRED_ARTIFACT_KINDS)}, got {sorted(observed_kinds)}"
        )
    return artifact_summary_by_kind


def _validate_v3_copy_records(
    summary: Mapping[str, Any], physical_summary: Mapping[str, Any], label: str
) -> None:
    summary_records = _v3_summary_integer(summary, label, "copy_record_count", "record_count")
    if summary_records != int(physical_summary.get("record_count") or 0):
        raise RuntimeError(f"PTG2 v3 {label} persisted record count mismatch")


def _validate_v3_physical_entries(
    artifact_summary_by_kind: Mapping[str, Mapping[str, Any]],
    *,
    by_code_summary: Mapping[str, Any],
    price_set_count: int,
    provider_stats: Mapping[str, Any],
    membership_stats: Mapping[str, Any],
    atom_count: int,
) -> None:
    expected_entry_count_by_kind = {
        PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND: _v3_summary_integer(
            by_code_summary, "assigned by-code", "group_count"
        ),
        PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND: price_set_count,
        PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND: int(provider_stats["provider_set_count"]),
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND: int(provider_stats["provider_set_count"]),
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: int(
            membership_stats["price_set_count"]
        ),
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: atom_count,
    }
    for artifact_kind, expected_count in expected_entry_count_by_kind.items():
        actual_count = int(artifact_summary_by_kind[artifact_kind].get("entry_count") or 0)
        if actual_count != expected_count:
            raise RuntimeError(
                f"PTG2 v3 persisted {artifact_kind} entry count mismatch: "
                f"expected {expected_count}, got {actual_count}"
            )


def _validate_v3_stream_storage(
    summary: Mapping[str, Any], physical_summary: Mapping[str, Any], label: str
) -> None:
    storage_summary = summary.get("storage")
    if not isinstance(storage_summary, Mapping):
        raise RuntimeError(f"PTG2 v3 {label} summary is missing storage metrics")
    for field_name in (
        "record_count",
        "entry_count",
        "stored_payload_bytes",
        "raw_payload_bytes",
    ):
        summary_value = _v3_summary_integer(storage_summary, f"{label} storage", field_name)
        physical_value = int(physical_summary.get(field_name) or 0)
        if summary_value != physical_value:
            raise RuntimeError(f"PTG2 v3 {label} persisted {field_name} mismatch")


def _validate_v3_stream_artifacts(
    stream_summary_by_kind: Mapping[str, Mapping[str, Any]],
    artifact_summary_by_kind: Mapping[str, Mapping[str, Any]],
) -> None:
    artifact_kind_by_stream_kind = {
        PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND: (
            PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND
        ),
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND: (
            PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
        ),
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: (
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND
        ),
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
    }
    for stream_kind, artifact_kind in artifact_kind_by_stream_kind.items():
        stream_summary = stream_summary_by_kind[stream_kind]
        physical_summary = artifact_summary_by_kind[artifact_kind]
        _validate_v3_copy_records(
            stream_summary,
            physical_summary,
            artifact_kind,
        )
        _validate_v3_stream_storage(stream_summary, physical_summary, artifact_kind)


def _validate_v3_total_storage(
    storage_summary: Mapping[str, Any],
    artifact_summary_by_kind: Mapping[str, Mapping[str, Any]],
) -> None:
    expected_by_field = {
        "record_count": sum(summary["record_count"] for summary in artifact_summary_by_kind.values()),
        "payload_bytes": sum(
            summary["stored_payload_bytes"] for summary in artifact_summary_by_kind.values()
        ),
        "raw_payload_bytes": sum(
            summary["compressed_raw_payload_bytes"]
            for summary in artifact_summary_by_kind.values()
        ),
    }
    for storage_field, expected_value in expected_by_field.items():
        if int(storage_summary.get(storage_field) or 0) != expected_value:
            raise RuntimeError(f"PTG2 v3 persisted total {storage_field} mismatch")


def _qualified_v3_table(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


def _v3_stream_sql_by_kind(
    publish_options: _V3PublishOptions, stage_tables: _V3StageTables
) -> dict[str, str]:
    qualified_source = _qualified_v3_table(publish_options.schema_name, publish_options.source_table)
    qualified_code_count = (
        _qualified_v3_table(publish_options.schema_name, publish_options.code_count_table)
        if publish_options.code_count_table
        else None
    )
    qualified_provider_dictionary = (
        _qualified_v3_table(publish_options.schema_name, publish_options.provider_set_dictionary_table)
        if publish_options.provider_set_dictionary_table
        else None
    )
    qualified_price_key_map = _qualified_v3_table(
        publish_options.schema_name, stage_tables.price_key_map
    )
    assigned_by_code_sql = _v3_assigned_by_code_sql(
        qualified_source=qualified_source,
        qualified_price_key_map=qualified_price_key_map,
        source_layout=publish_options.source_layout,
        qualified_code_count_table=qualified_code_count,
        qualified_provider_set_dictionary_table=qualified_provider_dictionary,
    )
    membership_sql = _v3_price_membership_sql(
        qualified_price_set_atom_table=_qualified_v3_table(
            publish_options.schema_name, publish_options.price_set_atom_table
        ),
        qualified_price_key_map=qualified_price_key_map,
        qualified_atom_key_map=_qualified_v3_table(
            publish_options.schema_name, stage_tables.atom_key_map
        ),
    )
    atom_sql = _v3_price_atom_sql(
        qualified_price_atom_table=_qualified_v3_table(
            publish_options.schema_name, publish_options.price_atom_table
        ),
        qualified_atom_key_map=_qualified_v3_table(
            publish_options.schema_name, stage_tables.atom_key_map
        ),
        constant_key_by_column=publish_options.price_atom_constant_keys,
    )
    return {
        PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND: assigned_by_code_sql,
        PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND: _v3_price_dictionary_sql(
            qualified_price_key_map=qualified_price_key_map
        ),
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: membership_sql,
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: atom_sql,
    }


async def _create_v3_stage_maps(
    publish_options: _V3PublishOptions,
    stage_tables: _V3StageTables,
) -> tuple[dict[str, int | None], dict[str, int | None]]:
    return await asyncio.gather(
        _create_v3_price_key_stage(
            schema_name=publish_options.schema_name,
            price_set_atom_table=publish_options.price_set_atom_table,
            stage_table=stage_tables.price_key_map,
        ),
        _create_v3_atom_key_stage(
            schema_name=publish_options.schema_name,
            price_atom_table=publish_options.price_atom_table,
            stage_table=stage_tables.atom_key_map,
        ),
    )


def _v3_build_stats_from_summaries(
    summary_by_kind: Mapping[str, Mapping[str, Any]],
    *,
    expected_row_count: int | None,
    price_set_count: int,
    atom_count: int,
    atom_key_bits: int,
) -> tuple[dict[str, int], dict[str, int | None]]:
    by_code_summary = summary_by_kind[PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND]
    _validate_v3_assigned_summary(
        by_code_summary,
        expected_row_count=expected_row_count,
        price_set_count=price_set_count,
    )
    _validate_v3_price_dictionary_summary(
        summary_by_kind[PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND],
        price_set_count=price_set_count,
    )
    provider_stats = _v3_provider_stats_from_summary(
        summary_by_kind[PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND]
    )
    membership_stats = _v3_membership_stats_from_summary(
        summary_by_kind[PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND],
        price_set_count=price_set_count,
        atom_count=atom_count,
        atom_key_bits=atom_key_bits,
    )
    _validate_v3_atom_summary(
        summary_by_kind[PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND],
        atom_count=atom_count,
        atom_key_bits=atom_key_bits,
    )
    if (
        _v3_summary_integer(by_code_summary, "assigned by-code", "provider_set_count")
        != provider_stats["provider_set_count"]
    ):
        raise RuntimeError("PTG2 v3 provider-set count differs between required streams")
    return provider_stats, membership_stats


async def _cleanup_v3_stages(
    *, schema_name: str, stage_tables: _V3StageTables
) -> None:
    cleanup_task = asyncio.create_task(
        _drop_v3_stage_tables(schema_name=schema_name, stage_tables=stage_tables)
    )
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        await cleanup_task
        raise


async def _cleanup_v3_target(*, schema_name: str, target_table: str) -> None:
    cleanup_task = asyncio.create_task(
        _drop_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    )
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        await cleanup_task
        raise


async def _write_v3_serving_binary(publish_options: _V3PublishOptions) -> dict[str, Any]:
    """Publish the explicit v3 PostgreSQL artifacts without a v2 fallback."""
    started_at = time.monotonic()
    stage_tables = _v3_stage_tables(publish_options.target_table)
    timing_by_stage: dict[str, Any] = {}
    try:
        await _drop_v3_stage_tables(
            schema_name=publish_options.schema_name,
            stage_tables=stage_tables,
        )
        _emit_progress(
            publish_options.progress_callback,
            "serving binary dense maps",
            done=0,
            total=_V3_PUBLISH_PROGRESS_TOTAL,
            message="building canonical price and atom key maps",
        )
        map_started_at = time.monotonic()
        price_map_stats, atom_map_stats = await _create_v3_stage_maps(
            publish_options,
            stage_tables,
        )
        timing_by_stage["dense_map_seconds"] = _elapsed_seconds(map_started_at)
        _emit_progress(
            publish_options.progress_callback,
            "serving binary dense maps complete",
            done=1,
            total=_V3_PUBLISH_PROGRESS_TOTAL,
            message="canonical price and atom key maps built",
            price_set_count=int(price_map_stats["row_count"] or 0),
            atom_count=int(atom_map_stats["row_count"] or 0),
        )
        await create_ptg2_serving_binary_table(
            schema_name=publish_options.schema_name,
            target_table=publish_options.target_table,
        )
        return await _finish_v3_serving_binary(
            publish_options=publish_options,
            stage_tables=stage_tables,
            price_map_stats=price_map_stats,
            atom_map_stats=atom_map_stats,
            timing_by_stage=timing_by_stage,
            started_at=started_at,
        )
    finally:
        await _cleanup_v3_stages(
            schema_name=publish_options.schema_name,
            stage_tables=stage_tables,
        )


async def _finish_v3_serving_binary(
    *,
    publish_options: _V3PublishOptions,
    stage_tables: _V3StageTables,
    price_map_stats: Mapping[str, Any],
    atom_map_stats: Mapping[str, Any],
    timing_by_stage: dict[str, Any],
    started_at: float,
) -> dict[str, Any]:
    price_set_count = int(price_map_stats["row_count"] or 0)
    atom_count = int(atom_map_stats["row_count"] or 0)
    atom_key_bits = select_atom_key_bits(atom_count)
    sql_by_kind = _v3_stream_sql_by_kind(publish_options, stage_tables)
    stream_started_at = time.monotonic()
    stream_summary_by_kind = await _run_v3_streams(
        sql_by_kind=sql_by_kind,
        schema_name=publish_options.schema_name,
        target_table=publish_options.target_table,
        target_copy_format=_serving_binary_target_copy_format(),
        atom_count=atom_count,
        atom_key_bits=atom_key_bits,
        progress_callback=publish_options.progress_callback,
    )
    timing_by_stage["v3_stream_seconds"] = _elapsed_seconds(stream_started_at)
    by_code_summary = stream_summary_by_kind[PTG2_SERVING_BINARY_BY_CODE_ASSIGNED_V3_ENCODER_KIND]
    provider_code_summary = by_code_summary.get("provider_set_codes")
    if not isinstance(provider_code_summary, Mapping):
        raise RuntimeError("PTG2 v3 fused by-code stream omitted provider-set codes")
    stream_summary_by_kind[PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND] = dict(
        provider_code_summary
    )
    provider_stats, membership_stats = _v3_build_stats_from_summaries(
        stream_summary_by_kind,
        expected_row_count=publish_options.expected_row_count,
        price_set_count=price_set_count,
        atom_count=atom_count,
        atom_key_bits=atom_key_bits,
    )
    return await _finalize_v3_serving_binary(
        publish_options,
        _V3BuildState(
            by_code_summary=by_code_summary,
            stream_summary_by_kind=stream_summary_by_kind,
            provider_stats=provider_stats,
            membership_stats=membership_stats,
            price_map_stats=price_map_stats,
            atom_map_stats=atom_map_stats,
            atom_key_bits=atom_key_bits,
            timing_by_stage=timing_by_stage,
            started_at=started_at,
        ),
    )


async def _finalize_v3_serving_binary(
    publish_options: _V3PublishOptions, build_state: _V3BuildState
) -> dict[str, Any]:
    atom_count = int(build_state.atom_map_stats["row_count"] or 0)
    price_set_count = int(build_state.price_map_stats["row_count"] or 0)
    _emit_progress(
        publish_options.progress_callback,
        "serving binary finalizing",
        done=5,
        total=_V3_PUBLISH_PROGRESS_TOTAL,
        message="building indexes and making PostgreSQL binary artifacts durable",
    )
    index_started_at = time.monotonic()
    storage_summary = await finalize_ptg2_serving_binary_table(
        schema_name=publish_options.schema_name,
        target_table=publish_options.target_table,
    )
    build_state.timing_by_stage["index_seconds"] = _elapsed_seconds(index_started_at)
    artifact_summary_by_kind = await _v3_physical_artifact_summaries(
        schema_name=publish_options.schema_name,
        target_table=publish_options.target_table,
    )
    _validate_v3_physical_entries(
        artifact_summary_by_kind,
        by_code_summary=build_state.by_code_summary,
        price_set_count=price_set_count,
        provider_stats=build_state.provider_stats,
        membership_stats=build_state.membership_stats,
        atom_count=atom_count,
    )
    _validate_v3_stream_artifacts(build_state.stream_summary_by_kind, artifact_summary_by_kind)
    _validate_v3_total_storage(storage_summary, artifact_summary_by_kind)
    by_code_record_count = sum(
        artifact_summary_by_kind[artifact_kind]["record_count"]
        for artifact_kind in (
            PTG2_SERVING_BINARY_BY_CODE_GROUPED_KIND,
            PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
            PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        )
    )
    _validate_v3_copy_records(
        build_state.by_code_summary,
        {"record_count": by_code_record_count},
        "by-code",
    )
    build_state.timing_by_stage["total_seconds"] = _elapsed_seconds(build_state.started_at)
    _emit_progress(
        publish_options.progress_callback,
        "serving binary finalized",
        done=_V3_PUBLISH_PROGRESS_TOTAL,
        total=_V3_PUBLISH_PROGRESS_TOTAL,
        message="PostgreSQL binary artifacts indexed and durable",
    )
    return _v3_serving_manifest(
        publish_options,
        build_state,
        artifact_summary_by_kind=artifact_summary_by_kind,
        storage_summary=storage_summary,
    )


def _v3_source_count_manifest(
    publish_options: _V3PublishOptions,
    build_state: _V3BuildState,
) -> dict[str, Any]:
    return {
        "serving_price_key_join": {
            "expected_rows": publish_options.expected_row_count,
            "joined_rows": int(build_state.by_code_summary.get("row_count") or 0),
            "all_serving_price_ids_mapped": True,
        },
        "provider_set_codes": dict(build_state.provider_stats),
        "price_set_atom_memberships": dict(build_state.membership_stats),
    }


def _v3_dense_key_manifest(build_state: _V3BuildState) -> dict[str, Any]:
    atom_count = int(build_state.atom_map_stats["row_count"] or 0)
    price_set_count = int(build_state.price_map_stats["row_count"] or 0)
    return {
        "canonical_price_keys": {
            "artifact_kind": PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
            "price_set_count": price_set_count,
            "dense_minimum": build_state.price_map_stats.get("minimum_key"),
            "dense_maximum": build_state.price_map_stats.get("maximum_key"),
        },
        "dense_atom_keys": {
            "atom_count": atom_count,
            "atom_key_bits": build_state.atom_key_bits,
            "atom_key_bytes": build_state.atom_key_bits // 8,
            "dense_minimum": build_state.atom_map_stats.get("minimum_key"),
            "dense_maximum": build_state.atom_map_stats.get("maximum_key"),
        },
    }


def _v3_serving_manifest(
    publish_options: _V3PublishOptions,
    build_state: _V3BuildState,
    *,
    artifact_summary_by_kind: Mapping[str, Mapping[str, Any]],
    storage_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "format": PTG2_SERVING_BINARY_TABLE_FORMAT,
        "table": f"{publish_options.schema_name}.{publish_options.target_table}",
        "writer": "rust_stream_v3",
        "arch_version": PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
        "artifact_version": PTG2_SERVING_BINARY_V3_ARTIFACT_VERSION,
        "artifact_kinds": sorted(artifact_summary_by_kind),
        "block_bytes": _serving_binary_block_bytes(),
        "row_count": int(build_state.by_code_summary.get("row_count") or 0),
        "source_copy_format": "binary",
        "target_copy_format": _serving_binary_target_copy_format(),
        "stream_tasks": min(_serving_binary_stream_tasks(), len(build_state.stream_summary_by_kind)),
        "by_code": dict(build_state.by_code_summary),
        "price_dictionary": dict(
            build_state.stream_summary_by_kind[
                PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND
            ]
        ),
        PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND: dict(
            build_state.stream_summary_by_kind[PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND]
        ),
        PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND: dict(
            build_state.stream_summary_by_kind[
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND
            ]
        ),
        PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND: dict(
            build_state.stream_summary_by_kind[PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND]
        ),
        **_v3_dense_key_manifest(build_state),
        "source_counts": _v3_source_count_manifest(publish_options, build_state),
        "artifact_summaries": {
            artifact_kind: dict(summary)
            for artifact_kind, summary in artifact_summary_by_kind.items()
        },
        "timing": dict(build_state.timing_by_stage),
        "build_elapsed_seconds": build_state.timing_by_stage["total_seconds"],
        "storage": dict(storage_summary),
    }


async def _write_serving_binary_rust_stream(
    *, schema_name: str, source_table: str, target_table: str,
    expected_row_count: int | None = None, price_set_atom_table: str | None = None,
    price_forward_artifact: Mapping[str, Any] | None = None,
    source_layout: str = PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
    code_count_table: str | None = None,
    provider_set_dictionary_table: str | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Create the binary table with Rust stdin/stdout COPY streaming."""
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    source_copy_format = _serving_binary_source_copy_format()
    target_copy_format = _serving_binary_target_copy_format()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    qualified_code_count_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(code_count_table)}" if code_count_table else None
    )
    qualified_provider_set_dictionary_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}"
        if provider_set_dictionary_table
        else None
    )
    qualified_price_set_atom_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(price_set_atom_table)}" if price_set_atom_table else None
    )
    await create_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
    if _use_serving_binary_combined_stream():
        _emit_progress(
            progress_callback,
            "serving binary stream combined",
            done=1,
            total=6,
            message="streaming PostgreSQL by-code COPY through combined Rust encoder",
            expected_row_count=expected_row_count,
        )
        stage_started_at = time.monotonic()
        combined_stream_limit = _serving_binary_stream_tasks()
        combined_task = asyncio.create_task(
            _stream_serving_binary_combined_copy(
                sql=_serving_binary_stream_sql(
                    qualified_source=qualified_source,
                    kind="by_code",
                    source_layout=source_layout,
                    qualified_code_count_table=qualified_code_count_table,
                    qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
                ),
                schema_name=schema_name,
                target_table=target_table,
                source_copy_format=source_copy_format,
                target_copy_format=target_copy_format,
            )
        )
        combined_tasks = [combined_task]
        atom_stream_options_by_name = {
            "config": _price_set_atom_stream_stage_config(),
            "progress_callback": progress_callback,
            "expected_row_count": None,
            "qualified_source": qualified_source,
            "source_layout": source_layout,
            "qualified_code_count_table": qualified_code_count_table,
            "qualified_provider_set_dictionary_table": qualified_provider_set_dictionary_table,
            "qualified_price_set_atom_table": qualified_price_set_atom_table,
            "schema_name": schema_name,
            "target_table": target_table,
            "source_copy_format": source_copy_format,
            "target_copy_format": target_copy_format,
            "timing_by_stage": timing_by_stage,
        }
        if qualified_price_set_atom_table is not None and combined_stream_limit >= 2:
            combined_tasks.append(
                asyncio.create_task(_stream_serving_binary_stage(**atom_stream_options_by_name))
            )
        try:
            combined_results = await asyncio.gather(*combined_tasks)
        except (Exception, asyncio.CancelledError):
            for task in combined_tasks:
                task.cancel()
            await asyncio.gather(*combined_tasks, return_exceptions=True)
            raise
        if qualified_price_set_atom_table is not None and combined_stream_limit < 2:
            combined_results.append(await _stream_serving_binary_stage(**atom_stream_options_by_name))
        combined = combined_results[0]
        timing_by_stage["combined_stream_seconds"] = round(
            float(combined.get("pipeline_seconds") or _elapsed_seconds(stage_started_at)), 3
        )
        by_code = dict(combined.get("by_code") or {})
        by_provider_set = dict(combined.get("by_provider_set") or {})
        _validate_stream_row_count(label="combined by-code", expected_row_count=expected_row_count, stream_result=by_code)
        _validate_stream_row_count(
            label="combined reverse",
            expected_row_count=expected_row_count,
            stream_result=by_provider_set,
        )
        if price_set_atom_table is not None:
            price_set_atoms = combined_results[1]
            price_set_atoms["stage_table"] = f"{schema_name}.{price_set_atom_table}"
        else:
            price_set_atoms = await _write_atom_map_stage(
                schema_name=schema_name,
                target_table=target_table,
                price_forward_artifact=price_forward_artifact,
                price_set_atom_table=None,
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
        timing_by_stage["source_copy_format"] = source_copy_format
        timing_by_stage["target_copy_format"] = target_copy_format
        return dict(
            format=PTG2_SERVING_BINARY_TABLE_FORMAT, table=f"{schema_name}.{target_table}", writer="rust_stream",
            stream_mode="combined", stream_tasks=len(combined_tasks), source_copy_format=source_copy_format,
            target_copy_format=target_copy_format, block_bytes=max_payload_bytes,
            row_count=int(by_code.get("row_count") or 0), by_code=by_code,
            by_provider_set=by_provider_set, price_set_atoms=price_set_atoms,
            timing=timing_by_stage, build_elapsed_seconds=timing_by_stage["total_seconds"],
            storage=storage_data,
        )

    stage_configs = _serving_binary_stream_stage_configs(include_price_set_atoms=price_set_atom_table is not None)
    stream_tasks = min(_serving_binary_stream_tasks(), len(stage_configs))
    stage_result_by_kind = await _run_serving_binary_stream_stages(
        stage_configs=stage_configs,
        stream_tasks=stream_tasks,
        progress_callback=progress_callback,
        expected_row_count=expected_row_count,
        qualified_source=qualified_source,
        source_layout=source_layout,
        qualified_code_count_table=qualified_code_count_table,
        qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
        qualified_price_set_atom_table=qualified_price_set_atom_table,
        schema_name=schema_name,
        target_table=target_table,
        source_copy_format=source_copy_format,
        target_copy_format=target_copy_format,
        timing_by_stage=timing_by_stage,
    )

    by_code = stage_result_by_kind["by_code"]
    by_provider_set = stage_result_by_kind["by_provider_set"]
    _validate_stream_row_count(label="by-code", expected_row_count=expected_row_count, stream_result=by_code)
    _validate_stream_row_count(label="reverse", expected_row_count=expected_row_count, stream_result=by_provider_set)
    if price_set_atom_table is not None:
        price_set_atoms = stage_result_by_kind[PTG2_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND]
        price_set_atoms["stage_table"] = f"{schema_name}.{price_set_atom_table}"
    else:
        price_set_atoms = await _write_atom_map_stage(
            schema_name=schema_name,
            target_table=target_table,
            price_forward_artifact=price_forward_artifact,
            price_set_atom_table=None,
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
    timing_by_stage["source_copy_format"] = source_copy_format
    timing_by_stage["target_copy_format"] = target_copy_format
    return dict(
        format=PTG2_SERVING_BINARY_TABLE_FORMAT, table=f"{schema_name}.{target_table}", writer="rust_stream",
        stream_tasks=stream_tasks, source_copy_format=source_copy_format, target_copy_format=target_copy_format,
        block_bytes=max_payload_bytes, row_count=int(by_code.get("row_count") or 0),
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
    source_layout: str = PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
    code_count_table: str | None = None,
    provider_set_dictionary_table: str | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    total_started_at = time.monotonic()
    timing_by_stage: dict[str, float] = {}
    max_payload_bytes = _serving_binary_block_bytes()
    qualified_source = f"{_quote_ident(schema_name)}.{_quote_ident(source_table)}"
    qualified_code_count_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(code_count_table)}" if code_count_table else None
    )
    qualified_provider_set_dictionary_table = (
        f"{_quote_ident(schema_name)}.{_quote_ident(provider_set_dictionary_table)}"
        if provider_set_dictionary_table
        else None
    )
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
            _serving_binary_stream_sql(
                qualified_source=qualified_source,
                kind="by_code",
                source_layout=source_layout,
                qualified_code_count_table=qualified_code_count_table,
                qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
            ),
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
            _serving_binary_stream_sql(
                qualified_source=qualified_source,
                kind="by_provider_set",
                source_layout=source_layout,
                qualified_code_count_table=qualified_code_count_table,
                qualified_provider_set_dictionary_table=qualified_provider_set_dictionary_table,
            ),
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
    price_atom_table: str | None = None,
    artifacts: Mapping[str, Any] | None = None,
    sidecar_artifacts: Mapping[str, Any] | None = None,
    source_layout: str = PTG2_SERVING_BINARY_SOURCE_LAYOUT_KEYED,
    code_count_table: str | None = None,
    provider_set_dictionary_table: str | None = None,
    arch_version: str | None = None,
    price_atom_table_layout: str | None = None,
    price_atom_constant_keys: Mapping[str, Any] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    """Create a PostgreSQL-native forward/reverse serving block table."""

    if _is_postgres_binary_v3_arch(arch_version):
        if not _serving_binary_rust_enabled() or not _use_serving_binary_stream():
            raise RuntimeError("postgres_binary_v3 requires the Rust streaming serving writer")
        if _ptg2_rust_scanner_binary() is None:
            raise RuntimeError("postgres_binary_v3 requires a PTG2 Rust scanner binary")
        if not price_set_atom_table or not price_atom_table:
            raise RuntimeError("postgres_binary_v3 requires relational price-atom stages")
        if expected_row_count is None:
            raise RuntimeError("postgres_binary_v3 requires an exact serving row count")
        if price_atom_table_layout not in {"lean_dict_v1", "lean_dict_v2"}:
            raise RuntimeError("postgres_binary_v3 requires a lean price-atom dictionary layout")
        publish_options = _V3PublishOptions(
            schema_name=schema_name,
            source_table=source_table,
            target_table=target_table,
            expected_row_count=expected_row_count,
            price_set_atom_table=price_set_atom_table,
            price_atom_table=price_atom_table,
            source_layout=source_layout,
            code_count_table=code_count_table,
            provider_set_dictionary_table=provider_set_dictionary_table,
            price_atom_table_layout=price_atom_table_layout,
            price_atom_constant_keys=dict(price_atom_constant_keys or {}),
            progress_callback=progress_callback,
        )
        try:
            return await _write_v3_serving_binary(publish_options)
        except (Exception, asyncio.CancelledError):
            await _cleanup_v3_target(schema_name=schema_name, target_table=target_table)
            raise

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
                    source_layout=source_layout,
                    code_count_table=code_count_table,
                    provider_set_dictionary_table=provider_set_dictionary_table,
                    progress_callback=progress_callback,
                )
            except asyncio.CancelledError:
                await asyncio.shield(
                    _drop_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
                )
                raise
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
                source_layout=source_layout,
                code_count_table=code_count_table,
                provider_set_dictionary_table=provider_set_dictionary_table,
                progress_callback=progress_callback,
            )
        except asyncio.CancelledError:
            await asyncio.shield(
                _drop_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table)
            )
            raise
        except Exception:
            logger.warning("PTG2 Rust serving binary generation failed; falling back to Python", exc_info=True)
    try:
        return await _write_ptg2_serving_binary_table_python(
            schema_name=schema_name,
            source_table=source_table,
            target_table=target_table,
            expected_row_count=expected_row_count,
            price_set_atom_table=price_set_atom_table,
            price_forward_artifact=price_forward_artifact,
            source_layout=source_layout,
            code_count_table=code_count_table,
            provider_set_dictionary_table=provider_set_dictionary_table,
            progress_callback=progress_callback,
        )
    except (Exception, asyncio.CancelledError):
        await asyncio.shield(_drop_ptg2_serving_binary_table(schema_name=schema_name, target_table=target_table))
        raise
