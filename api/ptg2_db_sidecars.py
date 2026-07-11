# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL-backed PTG2 sidecar readers.

These helpers read the same binary sidecars as the mmap-backed file readers,
but fetch only the required byte ranges from ``ptg2_artifact_blob_chunk``.
They are intentionally serving-only: import-time writers still produce the
same sidecar files and upload them to PostgreSQL as immutable artifact bytes.
"""

from __future__ import annotations

import json
import os
import re
import struct
import zlib
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_artifact_blobs import (
    ptg2_artifact_id_from_db_uri,
)
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_VERSION,
    PTG2_SERVING_BY_CODE_FORMAT,
    PTG2_SERVING_BY_CODE_MAGIC,
    PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
    PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
    PTG2ManifestArtifactError,
    PTG2ServingProviderSetPattern,
    PTG2ServingSidecarRow,
)


@dataclass(frozen=True)
class PTG2ServingBinaryRow(PTG2ServingSidecarRow):
    """Serving row decoded from PostgreSQL binary blocks with its local price key."""

    price_key: int | None = None


@dataclass(frozen=True)
class _DictionaryBlockMetadata:
    block_no: int
    entry_count: int
    payload_compression: str
    raw_payload_bytes: int
    payload_bytes: int


@dataclass(frozen=True)
class _DictionaryMetadata:
    blocks: tuple[_DictionaryBlockMetadata, ...] | None
    block_count: int
    entries_per_block: int
    item_count: int


def _dictionary_metadata_block(
    metadata: _DictionaryMetadata,
    block_no: int,
) -> _DictionaryBlockMetadata:
    if block_no < 0 or block_no >= metadata.block_count:
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block is out of range")
    if metadata.blocks is not None:
        return metadata.blocks[block_no]
    block_start = block_no * metadata.entries_per_block
    entry_count = min(metadata.entries_per_block, metadata.item_count - block_start)
    if entry_count <= 0:
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block is empty")
    return _DictionaryBlockMetadata(
        block_no=block_no,
        entry_count=entry_count,
        payload_compression="none",
        raw_payload_bytes=0,
        payload_bytes=entry_count * 16,
    )


_DEFAULT_CHUNK_BYTES = 8 * 1024 * 1024
_MEMBERSHIP_HEADER = struct.Struct("<8sIQ")
_DENSE_MEMBERSHIP_HEADER = struct.Struct("<8sIQQ")
_MEMBERSHIP_INDEX_RECORD = struct.Struct("<16sQI")
_DENSE_MEMBER_RECORD = struct.Struct("<I")
_SERVING_BLOCK_INDEX_RECORD = struct.Struct("<iQI")
_STANDARD_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC}
_DENSE_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC}
_MEMBERSHIP_FORMATS = {PTG2_MANIFEST_MEMBERSHIP_FORMAT, PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT}

_CHUNK_CACHE_MAX_BYTES = max(int(os.getenv("HLTHPRT_PTG2_DB_SIDECAR_CHUNK_CACHE_BYTES", str(64 * 1024 * 1024))), 0)
_INDEX_READ_MAX_BYTES = max(int(os.getenv("HLTHPRT_PTG2_DB_SIDECAR_INDEX_MAX_BYTES", str(128 * 1024 * 1024))), 0)
_CHUNK_CACHE: OrderedDict[tuple[str, str, int], bytes] = OrderedDict()
_CHUNK_CACHE_STATE = {"byte_count": 0}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_SERVING_BINARY_BY_CODE_KIND = "by_code"
_SERVING_BINARY_BY_CODE_GROUPED_KIND = "by_code_grouped"
_SERVING_BINARY_BY_CODE_DICTIONARY_KIND = "by_code_price_dictionary"
_SERVING_BINARY_BY_PROVIDER_SET_KIND = "by_provider_set"
_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND = "by_provider_set_price_dictionary"
_SERVING_BINARY_PRICE_SET_ATOMS_KIND = "price_set_atoms"
_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_KIND = "price_set_atoms_by_id"
_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND = "price_set_atoms_by_id_v2"
_DICTIONARY_LOOKUP_BLOCK_BATCH_SIZE = 64
_PRICE_SET_ATOM_BLOCK_SIZE = 1024
_PRICE_SET_ATOM_ID_BUCKETS = 256
_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES = 2


def _cache_key(artifact_id: str, metadata: Mapping[str, Any], chunk_no: int) -> tuple[str, str, int]:
    return artifact_id, str(metadata.get("sha256") or ""), int(chunk_no)


def _cache_get(key: tuple[str, str, int]) -> bytes | None:
    chunk = _CHUNK_CACHE.get(key)
    if chunk is not None:
        _CHUNK_CACHE.move_to_end(key)
    return chunk


def _cache_set(key: tuple[str, str, int], chunk: bytes) -> None:
    if _CHUNK_CACHE_MAX_BYTES <= 0 or len(chunk) > _CHUNK_CACHE_MAX_BYTES:
        return
    existing = _CHUNK_CACHE.pop(key, None)
    if existing is not None:
        _CHUNK_CACHE_STATE["byte_count"] -= len(existing)
    _CHUNK_CACHE[key] = chunk
    _CHUNK_CACHE_STATE["byte_count"] += len(chunk)
    while _CHUNK_CACHE_STATE["byte_count"] > _CHUNK_CACHE_MAX_BYTES and _CHUNK_CACHE:
        _old_key, old_chunk = _CHUNK_CACHE.popitem(last=False)
        _CHUNK_CACHE_STATE["byte_count"] -= len(old_chunk)


_BINARY_DICTIONARY_CACHE_MAX_BYTES = max(
    int(os.getenv("HLTHPRT_PTG2_DB_BINARY_DICTIONARY_CACHE_BYTES", str(64 * 1024 * 1024))),
    0,
)
_BINARY_DICTIONARY_CACHE: OrderedDict[tuple[str, str], tuple[int, Any]] = OrderedDict()
_BINARY_DICTIONARY_CACHE_STATE = {"byte_count": 0}
_BINARY_BLOCK_CACHE_MAX_BYTES = max(
    int(os.getenv("HLTHPRT_PTG2_DB_BINARY_BLOCK_CACHE_BYTES", str(64 * 1024 * 1024))),
    0,
)
_BINARY_BLOCK_CACHE: OrderedDict[tuple[str, str, int], tuple[int, tuple[dict[str, Any], ...]]] = OrderedDict()
_BINARY_BLOCK_CACHE_STATE = {"byte_count": 0}
_CACHE_MISS = object()


def _binary_dictionary_cache_get(key: tuple[str, str]) -> Any:
    cached = _BINARY_DICTIONARY_CACHE.get(key, _CACHE_MISS)
    if cached is _CACHE_MISS:
        return _CACHE_MISS
    _BINARY_DICTIONARY_CACHE.move_to_end(key)
    return cached[1]


def _binary_dictionary_cache_set(key: tuple[str, str], value: Any, byte_count: int) -> None:
    cache_bytes = max(int(byte_count), 1)
    if _BINARY_DICTIONARY_CACHE_MAX_BYTES <= 0 or cache_bytes > _BINARY_DICTIONARY_CACHE_MAX_BYTES:
        return
    existing = _BINARY_DICTIONARY_CACHE.pop(key, None)
    if existing is not None:
        _BINARY_DICTIONARY_CACHE_STATE["byte_count"] -= existing[0]
    _BINARY_DICTIONARY_CACHE[key] = (cache_bytes, value)
    _BINARY_DICTIONARY_CACHE_STATE["byte_count"] += cache_bytes
    while (
        _BINARY_DICTIONARY_CACHE_STATE["byte_count"] > _BINARY_DICTIONARY_CACHE_MAX_BYTES
        and _BINARY_DICTIONARY_CACHE
    ):
        _old_key, old_value = _BINARY_DICTIONARY_CACHE.popitem(last=False)
        _BINARY_DICTIONARY_CACHE_STATE["byte_count"] -= old_value[0]


def _binary_block_rows_bytes(rows: Iterable[Mapping[str, Any]]) -> int:
    total_bytes = 0
    for row in rows:
        total_bytes += len(bytes(row.get("payload") or b"")) + 128
        decoded_payload = row.get("_decoded_payload")
        if decoded_payload is not None:
            total_bytes += len(bytes(decoded_payload))
    return max(total_bytes, 1)


def _binary_block_cache_row(row: Mapping[str, Any]) -> dict[str, Any]:
    cached_row_by_column = dict(row)
    cached_row_by_column["_decoded_payload"] = _decode_serving_binary_payload(cached_row_by_column)
    return cached_row_by_column


def _binary_block_cache_get(key: tuple[str, str, int]) -> list[dict[str, Any]] | object:
    cached = _BINARY_BLOCK_CACHE.get(key, _CACHE_MISS)
    if cached is _CACHE_MISS:
        return _CACHE_MISS
    _BINARY_BLOCK_CACHE.move_to_end(key)
    return [dict(row) for row in cached[1]]


def _binary_block_cache_set(key: tuple[str, str, int], rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    cached_rows = tuple(_binary_block_cache_row(row) for row in rows)
    cache_bytes = _binary_block_rows_bytes(cached_rows)
    if _BINARY_BLOCK_CACHE_MAX_BYTES <= 0 or cache_bytes > _BINARY_BLOCK_CACHE_MAX_BYTES:
        return [dict(row) for row in cached_rows]
    existing = _BINARY_BLOCK_CACHE.pop(key, None)
    if existing is not None:
        _BINARY_BLOCK_CACHE_STATE["byte_count"] -= existing[0]
    _BINARY_BLOCK_CACHE[key] = (cache_bytes, cached_rows)
    _BINARY_BLOCK_CACHE_STATE["byte_count"] += cache_bytes
    while _BINARY_BLOCK_CACHE_STATE["byte_count"] > _BINARY_BLOCK_CACHE_MAX_BYTES and _BINARY_BLOCK_CACHE:
        _old_key, old_rows = _BINARY_BLOCK_CACHE.popitem(last=False)
        _BINARY_BLOCK_CACHE_STATE["byte_count"] -= old_rows[0]
    return [dict(row) for row in cached_rows]


def _metadata_int(metadata: Mapping[str, Any], key: str, default: int = 0) -> int:
    try:
        return int(metadata.get(key) or default)
    except (TypeError, ValueError):
        return default


def _normalize_global_id(value: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(value, str):
        text_value = value.strip().lower().replace("-", "")
        try:
            raw = bytes.fromhex(text_value)
        except ValueError as exc:
            raise PTG2ManifestArtifactError("global id strings must be 32-character hex values") from exc
    else:
        raw = bytes(value)
    if len(raw) != 16:
        raise PTG2ManifestArtifactError(f"global ids must be 16 bytes; got {len(raw)}")
    return raw


def _id_text(raw: bytes | bytearray | memoryview) -> str:
    value = bytes(raw)
    if len(value) != 16:
        raise PTG2ManifestArtifactError(f"128-bit ids must be 16 bytes; got {len(value)}")
    return value.hex()


def _price_set_atom_id_bucket(price_set_id: bytes | bytearray | memoryview | str) -> int:
    return int.from_bytes(_normalize_global_id(price_set_id)[:4], "big") % _PRICE_SET_ATOM_ID_BUCKETS


def _price_set_atom_prefix_bucket(price_set_id: bytes | bytearray | memoryview | str) -> int:
    return int.from_bytes(
        _normalize_global_id(price_set_id)[:_PRICE_SET_ATOM_ID_V2_PREFIX_BYTES],
        "big",
    )


def _read_uvarint(payload: bytes | bytearray | memoryview, offset: int) -> tuple[int, int]:
    cursor = int(offset)
    payload_length = len(payload)
    if cursor >= payload_length:
        raise PTG2ManifestArtifactError("serving sidecar ended inside a uvarint")
    first = int(payload[cursor])
    cursor += 1
    if first < 0x80:
        return first, cursor
    if cursor >= payload_length:
        raise PTG2ManifestArtifactError("serving sidecar ended inside a uvarint")
    second = int(payload[cursor])
    cursor += 1
    result = (first & 0x7F) | ((second & 0x7F) << 7)
    if second < 0x80:
        return result, cursor
    shift = 14
    while True:
        if cursor >= payload_length:
            raise PTG2ManifestArtifactError("serving sidecar ended inside a uvarint")
        byte = int(payload[cursor])
        cursor += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, cursor
        shift += 7
        if shift > 63:
            raise PTG2ManifestArtifactError("serving sidecar uvarint is too large")


def _decode_serving_binary_payload(record: Mapping[str, Any]) -> bytes:
    decoded_payload = record.get("_decoded_payload")
    if decoded_payload is not None:
        return bytes(decoded_payload)
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


def _read_serving_binary_price_keys(
    body: bytes,
    cursor: int,
    *,
    grouped_payload: bool,
) -> tuple[list[int], int]:
    """Decode one legacy price key or one grouped list of price keys."""
    if grouped_payload:
        price_key_count, cursor = _read_uvarint(body, cursor)
        price_keys = []
        for _price_index in range(price_key_count):
            price_key, cursor = _read_uvarint(body, cursor)
            price_keys.append(price_key)
        return price_keys, cursor
    price_key, cursor = _read_uvarint(body, cursor)
    return [price_key], cursor


def _decode_serving_binary_by_code_record(
    binary_record: Mapping[str, Any],
    *,
    provider_count_map: dict[int, int] | None,
    grouped_payload: bool,
    provider_filter: set[int] | None,
    provider_filter_max: int | None,
) -> list[tuple[int, int | None, int]]:
    """Decode provider-set and price keys without expanding the price dictionary."""
    payload_bytes = _decode_serving_binary_payload(binary_record)
    cursor = 0
    provider_set_key = 0
    decoded_keys: list[tuple[int, int | None, int]] = []
    for _ in range(int(binary_record.get("entry_count") or 0)):
        provider_delta, cursor = _read_uvarint(payload_bytes, cursor)
        provider_count: int | None = None
        if provider_count_map is None and not grouped_payload:
            provider_count, cursor = _read_uvarint(payload_bytes, cursor)
        provider_set_key += provider_delta
        if provider_count_map is not None:
            provider_count = provider_count_map.get(provider_set_key)
            if provider_count is None:
                raise PTG2ManifestArtifactError("PTG2 serving binary provider-count key is missing")
        price_keys, cursor = _read_serving_binary_price_keys(
            payload_bytes,
            cursor,
            grouped_payload=grouped_payload,
        )
        if provider_filter is not None and provider_set_key not in provider_filter:
            if provider_filter_max is not None and provider_set_key > provider_filter_max:
                break
            continue
        decoded_keys.extend(
            (provider_set_key, provider_count, price_key)
            for price_key in price_keys
        )
    return decoded_keys


def _validate_membership_record_format(metadata: Mapping[str, Any]) -> None:
    record_format = metadata.get("record_format")
    if record_format is not None and record_format not in _MEMBERSHIP_FORMATS:
        raise PTG2ManifestArtifactError("global membership sidecar has an unexpected record format")


class PTG2DbArtifactReader:
    def __init__(self, session: Any, entry: Mapping[str, Any], *, schema_name: str) -> None:
        storage_uri = str(entry.get("storage_uri") or "").strip()
        artifact_id = ptg2_artifact_id_from_db_uri(storage_uri)
        if not artifact_id:
            raise PTG2ManifestArtifactError(f"unsupported PTG2 artifact storage uri: {storage_uri!r}")
        self.session = session
        self.entry = dict(entry)
        self.schema_name = schema_name
        self.artifact_id = artifact_id
        self.chunk_bytes = max(_metadata_int(self.entry, "chunk_bytes", _DEFAULT_CHUNK_BYTES), 1)
        self._byte_count = _metadata_int(self.entry, "byte_count", -1)

    async def byte_count(self) -> int:
        if self._byte_count >= 0:
            return self._byte_count
        qualified_chunks = f"{_quote_ident(self.schema_name)}.ptg2_artifact_blob_chunk"
        result = await self.session.execute(
            text(f"SELECT COALESCE(SUM(raw_byte_count), 0) FROM {qualified_chunks} WHERE artifact_id = :artifact_id"),
            {"artifact_id": self.artifact_id},
        )
        self._byte_count = int(result.scalar() or 0)
        return self._byte_count

    async def _chunk(self, chunk_no: int) -> bytes:
        key = _cache_key(self.artifact_id, self.entry, chunk_no)
        cached = _cache_get(key)
        if cached is not None:
            return cached
        qualified_chunks = f"{_quote_ident(self.schema_name)}.ptg2_artifact_blob_chunk"
        result = await self.session.execute(
            text(
                f"""
                SELECT compression, payload, raw_byte_count
                  FROM {qualified_chunks}
                 WHERE artifact_id = :artifact_id
                   AND chunk_no = :chunk_no
                 LIMIT 1
                """
            ),
            {"artifact_id": self.artifact_id, "chunk_no": int(chunk_no)},
        )
        row = result.first() if hasattr(result, "first") else None
        if row is None:
            rows = list(result)
            row = rows[0] if rows else None
        if row is None:
            raise FileNotFoundError(f"PTG2 artifact chunk is missing: {self.artifact_id}:{chunk_no}")
        mapping = getattr(row, "_mapping", None)
        data = dict(mapping) if mapping is not None else dict(row)
        payload = bytes(data.get("payload") or b"")
        compression = str(data.get("compression") or "none")
        raw_chunk = zlib.decompress(payload) if compression == "zlib" else payload
        expected_raw = data.get("raw_byte_count")
        if expected_raw is not None and len(raw_chunk) != int(expected_raw):
            raise PTG2ManifestArtifactError(
                f"artifact chunk raw byte_count mismatch for {self.artifact_id}:{chunk_no}"
            )
        _cache_set(key, raw_chunk)
        return raw_chunk

    async def _read_artifact_chunks(self, chunk_numbers: Iterable[int]) -> dict[int, bytes]:
        unique_chunk_numbers = tuple(dict.fromkeys(int(chunk_number) for chunk_number in chunk_numbers))
        if not unique_chunk_numbers:
            return {}
        if len(unique_chunk_numbers) == 1:
            chunk_number = unique_chunk_numbers[0]
            return {chunk_number: await self._chunk(chunk_number)}

        chunks_by_number: dict[int, bytes] = {}
        missing_chunk_numbers: list[int] = []
        for chunk_number in unique_chunk_numbers:
            cache_lookup_key = _cache_key(self.artifact_id, self.entry, chunk_number)
            cached_chunk = _cache_get(cache_lookup_key)
            if cached_chunk is not None:
                chunks_by_number[chunk_number] = cached_chunk
            else:
                missing_chunk_numbers.append(chunk_number)

        if not missing_chunk_numbers:
            return chunks_by_number

        qualified_chunks = f"{_quote_ident(self.schema_name)}.ptg2_artifact_blob_chunk"
        chunk_query_result = await self.session.execute(
            text(
                f"""
                SELECT chunk_no, compression, payload, raw_byte_count
                  FROM {qualified_chunks}
                 WHERE artifact_id = :artifact_id
                   AND chunk_no = ANY(CAST(:chunk_nos AS integer[]))
                 ORDER BY chunk_no
                """
            ),
            {"artifact_id": self.artifact_id, "chunk_nos": missing_chunk_numbers},
        )
        for chunk_query_row in chunk_query_result:
            row_mapping = getattr(chunk_query_row, "_mapping", None)
            chunk_fields = dict(row_mapping) if row_mapping is not None else dict(chunk_query_row)
            chunk_number = int(chunk_fields.get("chunk_no"))
            stored_payload = bytes(chunk_fields.get("payload") or b"")
            compression = str(chunk_fields.get("compression") or "none")
            raw_chunk = zlib.decompress(stored_payload) if compression == "zlib" else stored_payload
            expected_raw = chunk_fields.get("raw_byte_count")
            if expected_raw is not None and len(raw_chunk) != int(expected_raw):
                raise PTG2ManifestArtifactError(
                    f"artifact chunk raw byte_count mismatch for {self.artifact_id}:{chunk_number}"
                )
            chunks_by_number[chunk_number] = raw_chunk
            _cache_set(_cache_key(self.artifact_id, self.entry, chunk_number), raw_chunk)

        missing_chunk_numbers_after_fetch_list = [
            chunk_number for chunk_number in missing_chunk_numbers if chunk_number not in chunks_by_number
        ]
        if missing_chunk_numbers_after_fetch_list:
            raise FileNotFoundError(
                f"PTG2 artifact chunk is missing: {self.artifact_id}:{missing_chunk_numbers_after_fetch_list[0]}"
            )
        return chunks_by_number

    async def read_at(self, offset: int, length: int) -> bytes:
        offset = int(offset)
        length = int(length)
        if offset < 0 or length < 0:
            raise PTG2ManifestArtifactError("artifact byte ranges must be non-negative")
        if length == 0:
            return b""
        total_size = await self.byte_count()
        if offset + length > total_size:
            raise PTG2ManifestArtifactError(
                f"artifact byte range exceeds byte_count: offset={offset} length={length} size={total_size}"
            )
        start_chunk = offset // self.chunk_bytes
        end_chunk = (offset + length - 1) // self.chunk_bytes
        chunks = await self._read_artifact_chunks(range(start_chunk, end_chunk + 1))
        pieces: list[bytes] = []
        remaining = length
        cursor = offset
        for chunk_no in range(start_chunk, end_chunk + 1):
            chunk = chunks[chunk_no]
            start = cursor % self.chunk_bytes
            take = min(remaining, len(chunk) - start)
            if take < 0:
                raise PTG2ManifestArtifactError("artifact chunk range is invalid")
            pieces.append(chunk[start : start + take])
            cursor += take
            remaining -= take
        if remaining != 0:
            raise PTG2ManifestArtifactError("artifact chunk range ended before requested bytes")
        return b"".join(pieces)

    async def read_ranges(
        self,
        ranges_by_key: Mapping[Any, tuple[int, int]],
    ) -> dict[Any, bytes]:
        """Read many byte ranges while fetching every required chunk once."""

        if not ranges_by_key:
            return {}
        total_size = await self.byte_count()
        payload_parts_by_key: dict[Any, list[bytes]] = {}
        slices_by_chunk: dict[int, list[tuple[Any, int, int]]] = {}
        for range_key, (raw_offset, raw_length) in ranges_by_key.items():
            offset = int(raw_offset)
            length = int(raw_length)
            if offset < 0 or length < 0:
                raise PTG2ManifestArtifactError("artifact byte ranges must be non-negative")
            if offset + length > total_size:
                raise PTG2ManifestArtifactError(
                    f"artifact byte range exceeds byte_count: offset={offset} length={length} size={total_size}"
                )
            payload_parts_by_key[range_key] = []
            if length == 0:
                continue
            cursor = offset
            remaining = length
            while remaining > 0:
                chunk_no = cursor // self.chunk_bytes
                start = cursor % self.chunk_bytes
                take = min(remaining, self.chunk_bytes - start)
                slices_by_chunk.setdefault(chunk_no, []).append((range_key, start, take))
                cursor += take
                remaining -= take

        ordered_chunk_numbers = sorted(slices_by_chunk)
        batch_byte_limit = max(_CHUNK_CACHE_MAX_BYTES, self.chunk_bytes)
        batch_chunk_count = max(batch_byte_limit // self.chunk_bytes, 1)
        for batch_start in range(0, len(ordered_chunk_numbers), batch_chunk_count):
            chunk_batch = ordered_chunk_numbers[batch_start : batch_start + batch_chunk_count]
            chunks_by_number = await self._read_artifact_chunks(chunk_batch)
            for chunk_no in chunk_batch:
                chunk = chunks_by_number[chunk_no]
                for range_key, start, take in slices_by_chunk[chunk_no]:
                    part = chunk[start : start + take]
                    if len(part) != take:
                        raise PTG2ManifestArtifactError("artifact chunk range is invalid")
                    payload_parts_by_key[range_key].append(part)
        return {range_key: b"".join(parts) for range_key, parts in payload_parts_by_key.items()}

    async def read_fixed_records(
        self,
        *,
        base_offset: int,
        record_size: int,
        record_numbers: Iterable[int],
    ) -> dict[int, bytes]:
        """Read fixed-size records by ordinal while batching artifact chunk fetches."""
        unique_record_numbers = tuple(dict.fromkeys(int(record_number) for record_number in record_numbers))
        if not unique_record_numbers:
            return {}
        if int(record_size) <= 0:
            raise PTG2ManifestArtifactError("artifact fixed record size must be positive")
        total_size = await self.byte_count()
        chunk_range_by_record_number: dict[int, tuple[int, int]] = {}
        chunk_numbers_to_fetch_set: set[int] = set()
        for record_number in unique_record_numbers:
            if record_number < 0:
                raise PTG2ManifestArtifactError("artifact fixed record numbers must be non-negative")
            offset = int(base_offset) + record_number * int(record_size)
            length = int(record_size)
            if offset < 0 or offset + length > total_size:
                raise PTG2ManifestArtifactError(
                    f"artifact fixed record exceeds byte_count: offset={offset} length={length} size={total_size}"
            )
            start_chunk = offset // self.chunk_bytes
            end_chunk = (offset + length - 1) // self.chunk_bytes
            chunk_range_by_record_number[record_number] = (offset, end_chunk)
            chunk_numbers_to_fetch_set.update(range(start_chunk, end_chunk + 1))

        raw_chunks_by_number = await self._read_artifact_chunks(sorted(chunk_numbers_to_fetch_set))
        fixed_records_by_number: dict[int, bytes] = {}
        for record_number in unique_record_numbers:
            offset, end_chunk = chunk_range_by_record_number[record_number]
            remaining = int(record_size)
            cursor = offset
            pieces: list[bytes] = []
            while remaining > 0:
                chunk_no = cursor // self.chunk_bytes
                if chunk_no > end_chunk:
                    raise PTG2ManifestArtifactError("artifact fixed record ended before requested bytes")
                chunk = raw_chunks_by_number[chunk_no]
                start = cursor % self.chunk_bytes
                take = min(remaining, len(chunk) - start)
                if take <= 0:
                    raise PTG2ManifestArtifactError("artifact fixed record chunk range is invalid")
                pieces.append(chunk[start : start + take])
                cursor += take
                remaining -= take
            fixed_records_by_number[record_number] = b"".join(pieces)
        return fixed_records_by_number


def db_artifact_entry_available(entry: Mapping[str, Any] | None) -> bool:
    return bool(entry and ptg2_artifact_id_from_db_uri(str(entry.get("storage_uri") or "")))


def _safe_qualified_table_name(value: str) -> str:
    parts = str(value or "").split(".", 1)
    if len(parts) != 2 or not _IDENTIFIER_RE.fullmatch(parts[0]) or not _IDENTIFIER_RE.fullmatch(parts[1]):
        raise PTG2ManifestArtifactError(f"unsafe PTG2 serving binary table name: {value!r}")
    return f"{_quote_ident(parts[0])}.{_quote_ident(parts[1])}"


def _result_rows(result: Any) -> list[Any]:
    return list(result)


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


async def _serving_binary_cache_relation_key(session: Any, qualified_table: str) -> str:
    """Bind process-local binary caches to the live PostgreSQL relation generation."""
    if not hasattr(session, "sync_session"):
        return qualified_table
    identity_by_table = getattr(session, "_ptg2_serving_binary_relation_identity", None)
    if identity_by_table is None:
        identity_by_table = {}
        setattr(session, "_ptg2_serving_binary_relation_identity", identity_by_table)
    if qualified_table in identity_by_table:
        return identity_by_table[qualified_table]
    identity_result = await session.execute(
        text(
            """
            SELECT relation.oid::bigint AS relation_oid,
                   relation.relfilenode::bigint AS relation_file_node
              FROM pg_class relation
             WHERE relation.oid = to_regclass(:qualified_table)
            """
        ),
        {"qualified_table": qualified_table},
    )
    raw_identity_row = identity_result.first()
    if raw_identity_row is None:
        raise PTG2ManifestArtifactError(f"PTG2 serving binary relation is missing: {qualified_table}")
    identity_row = _row_mapping(raw_identity_row)
    relation_oid = identity_row.get("relation_oid")
    relation_file_node = identity_row.get("relation_file_node")
    if relation_oid is None or relation_file_node is None:
        raise PTG2ManifestArtifactError(f"PTG2 serving binary relation is missing: {qualified_table}")
    relation_key = f"{qualified_table}:{int(relation_oid)}:{int(relation_file_node)}"
    identity_by_table[qualified_table] = relation_key
    return relation_key


async def _serving_binary_payload_rows(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
    block_key: int,
) -> list[dict[str, Any]]:
    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cache_key = (relation_key, artifact_kind, int(block_key))
    cached_rows = _binary_block_cache_get(cache_key)
    if cached_rows is not _CACHE_MISS:
        return cached_rows
    binary_block_result = await session.execute(
        text(
            f"""
            SELECT block_key, block_no, entry_count, payload,
                COALESCE(binary_block.payload_compression, 'none') AS payload_compression,
                binary_block.raw_payload_bytes
              FROM {qualified_table} binary_block
             WHERE artifact_kind = :artifact_kind
               AND block_key = :block_key
             ORDER BY block_no
            """
        ),
        {"artifact_kind": artifact_kind, "block_key": int(block_key)},
    )
    binary_block_rows = [_row_mapping(binary_block_row) for binary_block_row in _result_rows(binary_block_result)]
    return _binary_block_cache_set(cache_key, binary_block_rows)


async def _serving_binary_payload_rows_for_keys(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
    block_keys: Iterable[int],
) -> list[dict[str, Any]]:
    """Read selected binary blocks while sharing relation-generation-safe cache rows."""
    block_key_values = sorted({int(block_key) for block_key in block_keys})
    if not block_key_values:
        return []
    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cached_rows_by_key: dict[int, list[dict[str, Any]]] = {}
    missing_block_keys: list[int] = []
    for block_key in block_key_values:
        cache_key = (relation_key, artifact_kind, block_key)
        cached_rows = _binary_block_cache_get(cache_key)
        if cached_rows is _CACHE_MISS:
            missing_block_keys.append(block_key)
        else:
            cached_rows_by_key[block_key] = cached_rows
    if not missing_block_keys:
        return [
            binary_block_row
            for block_key in block_key_values
            for binary_block_row in cached_rows_by_key.get(block_key, [])
        ]

    fetched_rows_by_key: dict[int, list[dict[str, Any]]] = {block_key: [] for block_key in missing_block_keys}
    binary_block_result = await session.execute(
        text(
            f"""
            SELECT block_key, block_no, entry_count, payload,
                COALESCE(binary_block.payload_compression, 'none') AS payload_compression,
                binary_block.raw_payload_bytes
              FROM {qualified_table} binary_block
             WHERE artifact_kind = :artifact_kind
               AND block_key = ANY(CAST(:block_keys AS integer[]))
             ORDER BY block_key, block_no
            """
        ),
        {"artifact_kind": artifact_kind, "block_keys": missing_block_keys},
    )
    for binary_block_row in _result_rows(binary_block_result):
        binary_block_row_map = _row_mapping(binary_block_row)
        fetched_rows_by_key.setdefault(int(binary_block_row_map.get("block_key") or 0), []).append(binary_block_row_map)
    for block_key in missing_block_keys:
        fetched_rows_by_key[block_key] = _binary_block_cache_set(
            (relation_key, artifact_kind, block_key),
            fetched_rows_by_key.get(block_key, []),
        )
    return [
        binary_block_row
        for block_key in block_key_values
        for binary_block_row in cached_rows_by_key.get(block_key, fetched_rows_by_key.get(block_key, []))
    ]


async def _serving_binary_dictionary(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
) -> tuple[str, ...]:
    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cache_key = (relation_key, artifact_kind)
    cached_dictionary = _binary_dictionary_cache_get(cache_key)
    if cached_dictionary is not _CACHE_MISS:
        return cached_dictionary
    dictionary_payload = await _serving_binary_dictionary_payload(
        session,
        table_name,
        artifact_kind=artifact_kind,
    )
    price_set_ids = tuple(
        _id_text(dictionary_payload[payload_offset : payload_offset + 16])
        for payload_offset in range(0, len(dictionary_payload), 16)
    )
    _binary_dictionary_cache_set(cache_key, price_set_ids, len(dictionary_payload))
    return price_set_ids


async def _serving_binary_dictionary_payload(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
) -> bytes:
    """Return a raw binary dictionary payload without expanding every id."""

    qualified_table = _safe_qualified_table_name(table_name)
    metadata = await _serving_binary_dictionary_metadata(
        session,
        table_name,
        artifact_kind=artifact_kind,
    )
    payload_by_block_no = await _serving_binary_dictionary_blocks(
        session,
        qualified_table,
        artifact_kind=artifact_kind,
        metadata=metadata,
        block_nos=range(metadata.block_count),
    )
    return b"".join(payload_by_block_no[block_no] for block_no in range(metadata.block_count))


async def _serving_binary_dictionary_values_for_keys(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
    item_keys: Iterable[int],
    item_count_hint: int | None = None,
    block_bytes_hint: int | None = None,
    compressed_records_hint: int | None = None,
) -> dict[int, str]:
    """Return selected 128-bit ids from a binary dictionary payload."""

    requested_keys = tuple(sorted({int(item_key) for item_key in item_keys if item_key is not None}))
    if not requested_keys:
        return {}
    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    values_by_key, missing_keys = _cached_dictionary_values_for_keys(
        relation_key,
        artifact_kind,
        requested_keys,
    )
    if not missing_keys:
        return values_by_key

    metadata = _uncompressed_dictionary_metadata_hint(
        item_count=item_count_hint,
        block_bytes=block_bytes_hint,
        compressed_records=compressed_records_hint,
    )
    if metadata is None:
        metadata = await _serving_binary_dictionary_metadata(
            session,
            table_name,
            artifact_kind=artifact_kind,
        )
    _validate_dictionary_keys(missing_keys, metadata.item_count)
    missing_values_by_key = await _uncached_dictionary_values_for_keys(
        session,
        qualified_table,
        artifact_kind=artifact_kind,
        metadata=metadata,
        item_keys=missing_keys,
    )
    for item_key, item_value in missing_values_by_key.items():
        values_by_key[item_key] = item_value
        _binary_dictionary_cache_set(
            (relation_key, f"{artifact_kind}:item:{item_key}"),
            item_value,
            64,
        )
    return values_by_key


def _cached_dictionary_values_for_keys(
    relation_key: str,
    artifact_kind: str,
    item_keys: Iterable[int],
) -> tuple[dict[int, str], tuple[int, ...]]:
    """Split requested dictionary keys into cached values and cache misses."""

    values_by_key: dict[int, str] = {}
    missing_keys: list[int] = []
    for item_key in item_keys:
        cached_value = _binary_dictionary_cache_get(
            (relation_key, f"{artifact_kind}:item:{item_key}")
        )
        if cached_value is _CACHE_MISS:
            missing_keys.append(item_key)
        else:
            values_by_key[item_key] = cached_value
    return values_by_key, tuple(missing_keys)


async def _uncached_dictionary_values_for_keys(
    session: Any,
    qualified_table: str,
    *,
    artifact_kind: str,
    metadata: _DictionaryMetadata,
    item_keys: Iterable[int],
) -> dict[int, str]:
    """Fetch every referenced block once, then slice selected fixed-width ids."""

    requested_keys = tuple(item_keys)
    _validate_dictionary_keys(requested_keys, metadata.item_count)
    item_keys_by_block_no: dict[int, list[int]] = {}
    for item_key in requested_keys:
        block_no = item_key // metadata.entries_per_block
        item_keys_by_block_no.setdefault(block_no, []).append(item_key)
    block_nos = sorted(item_keys_by_block_no)
    values_by_key: dict[int, str] = {}
    for batch_start in range(0, len(block_nos), _DICTIONARY_LOOKUP_BLOCK_BATCH_SIZE):
        block_batch = block_nos[
            batch_start : batch_start + _DICTIONARY_LOOKUP_BLOCK_BATCH_SIZE
        ]
        payload_by_block_no = await _serving_binary_dictionary_blocks(
            session,
            qualified_table,
            artifact_kind=artifact_kind,
            metadata=metadata,
            block_nos=block_batch,
        )
        for block_no in block_batch:
            block_payload = payload_by_block_no[block_no]
            for item_key in item_keys_by_block_no[block_no]:
                block_item_key = item_key % metadata.entries_per_block
                value_offset = block_item_key * 16
                values_by_key[item_key] = _id_text(
                    block_payload[value_offset : value_offset + 16]
                )
    return values_by_key


async def _serving_binary_dictionary_metadata(
    session: Any,
    table_name: str,
    *,
    artifact_kind: str,
) -> _DictionaryMetadata:
    """Return small dictionary metadata without reading its potentially huge payload."""

    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cache_key = (relation_key, f"{artifact_kind}:metadata")
    cached_metadata = _binary_dictionary_cache_get(cache_key)
    if cached_metadata is not _CACHE_MISS:
        return cached_metadata
    metadata_result = await session.execute(
        text(
            f"""
            SELECT
                binary_block.block_no,
                binary_block.entry_count,
                COALESCE(binary_block.payload_compression, 'none') AS payload_compression,
                binary_block.raw_payload_bytes,
                octet_length(binary_block.payload) AS payload_bytes
              FROM {qualified_table} binary_block
             WHERE artifact_kind = :artifact_kind
               AND block_key = 0
             ORDER BY binary_block.block_no
            """
        ),
        {"artifact_kind": artifact_kind},
    )
    metadata_rows = _result_rows(metadata_result)
    if not metadata_rows:
        raise PTG2ManifestArtifactError(f"PTG2 serving binary dictionary is missing: {artifact_kind}")
    metadata = _validated_dictionary_metadata(
        _row_mapping(metadata_row) for metadata_row in metadata_rows
    )
    _binary_dictionary_cache_set(cache_key, metadata, max(metadata.block_count * 64, 1))
    return metadata


def _uncompressed_dictionary_metadata_hint(
    *,
    item_count: int | None,
    block_bytes: int | None,
    compressed_records: int | None,
) -> _DictionaryMetadata | None:
    if item_count is None or block_bytes is None or compressed_records != 0:
        return None
    normalized_item_count = int(item_count)
    normalized_block_bytes = int(block_bytes)
    if normalized_item_count <= 0 or normalized_block_bytes < 16 or normalized_block_bytes % 16:
        return None
    entries_per_block = normalized_block_bytes // 16
    block_count = (normalized_item_count + entries_per_block - 1) // entries_per_block
    return _DictionaryMetadata(
        blocks=None,
        block_count=block_count,
        entries_per_block=entries_per_block,
        item_count=normalized_item_count,
    )


def _validated_dictionary_metadata(
    metadata_records: Iterable[Mapping[str, Any]],
) -> _DictionaryMetadata:
    """Validate ordered fixed-width dictionary block metadata."""

    blocks = [
        _dictionary_block_metadata(metadata_record, expected_block_no)
        for expected_block_no, metadata_record in enumerate(metadata_records)
    ]

    entries_per_block = blocks[0].entry_count
    if len(blocks) > 1:
        if entries_per_block <= 0:
            raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block 0 is empty")
        if any(block.entry_count != entries_per_block for block in blocks[:-1]):
            raise PTG2ManifestArtifactError(
                "PTG2 serving binary dictionary has a short non-final block"
            )
        if blocks[-1].entry_count <= 0 or blocks[-1].entry_count > entries_per_block:
            raise PTG2ManifestArtifactError("PTG2 serving binary dictionary final block size is invalid")
    return _DictionaryMetadata(
        blocks=tuple(blocks),
        block_count=len(blocks),
        entries_per_block=entries_per_block,
        item_count=sum(block.entry_count for block in blocks),
    )


def _dictionary_metadata_integer(raw_value: Any, field_name: str) -> int:
    """Parse one non-negative dictionary metadata integer."""

    try:
        parsed_value = int(raw_value or 0)
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            f"PTG2 serving binary dictionary {field_name} is invalid"
        ) from exc
    if parsed_value < 0:
        raise PTG2ManifestArtifactError(
            f"PTG2 serving binary dictionary {field_name} is invalid"
        )
    return parsed_value


def _dictionary_payload_compression(raw_compression: Any) -> str:
    """Normalize and validate a dictionary block's compression label."""

    payload_compression = str(raw_compression or "none").strip().lower()
    if payload_compression not in {"", "none", "zlib"}:
        raise PTG2ManifestArtifactError(
            f"unsupported PTG2 serving binary dictionary compression: {payload_compression}"
        )
    return payload_compression or "none"


def _dictionary_block_metadata(
    metadata_record: Mapping[str, Any],
    expected_block_no: int,
) -> _DictionaryBlockMetadata:
    """Validate metadata for one block in dictionary order."""

    payload_compression = _dictionary_payload_compression(metadata_record.get("payload_compression"))
    payload_bytes = _dictionary_metadata_integer(metadata_record.get("payload_bytes"), "payload_bytes")
    raw_payload_bytes = _dictionary_metadata_integer(
        metadata_record.get("raw_payload_bytes"),
        "raw_payload_bytes",
    )
    raw_block_no = metadata_record.get("block_no")
    block_no = (
        expected_block_no
        if raw_block_no is None
        else _dictionary_metadata_integer(raw_block_no, "block_no")
    )
    raw_entry_count = metadata_record.get("entry_count")
    if raw_entry_count is None:
        decoded_payload_bytes = raw_payload_bytes if payload_compression == "zlib" else payload_bytes
        if decoded_payload_bytes % 16:
            raise PTG2ManifestArtifactError("PTG2 serving binary dictionary payload has invalid length")
        entry_count = decoded_payload_bytes // 16
    else:
        entry_count = _dictionary_metadata_integer(raw_entry_count, "entry_count")
    if block_no != expected_block_no:
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block numbers are non-contiguous")

    expected_raw_bytes = entry_count * 16
    if payload_compression == "none":
        if payload_bytes != expected_raw_bytes or raw_payload_bytes not in {0, expected_raw_bytes}:
            raise PTG2ManifestArtifactError("PTG2 serving binary dictionary payload byte count mismatch")
    elif raw_payload_bytes != expected_raw_bytes or entry_count == 0 or payload_bytes == 0:
        raise PTG2ManifestArtifactError(
            "PTG2 serving binary dictionary raw payload byte count mismatch"
        )
    return _DictionaryBlockMetadata(
        block_no=block_no,
        entry_count=entry_count,
        payload_compression=payload_compression,
        raw_payload_bytes=raw_payload_bytes,
        payload_bytes=payload_bytes,
    )


async def _serving_binary_dictionary_blocks(
    session: Any,
    qualified_table: str,
    *,
    artifact_kind: str,
    metadata: _DictionaryMetadata,
    block_nos: Iterable[int],
) -> dict[int, bytes]:
    """Fetch and decode only the selected dictionary blocks in one query."""

    requested_block_nos = tuple(sorted({int(block_no) for block_no in block_nos}))
    if not requested_block_nos:
        return {}
    if requested_block_nos[0] < 0 or requested_block_nos[-1] >= metadata.block_count:
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block is out of range")
    block_result = await session.execute(
        _dictionary_blocks_statement(qualified_table),
        {"artifact_kind": artifact_kind, "block_nos": list(requested_block_nos)},
    )
    block_records = [_row_mapping(block_record) for block_record in _result_rows(block_result)]
    payload_by_block_no: dict[int, bytes] = {}
    for block_record in block_records:
        block_no = _dictionary_record_block_no(
            block_record,
            requested_block_nos,
            len(block_records),
        )
        if block_no not in requested_block_nos or block_no in payload_by_block_no:
            raise PTG2ManifestArtifactError("PTG2 serving binary dictionary returned unexpected blocks")
        payload_by_block_no[block_no] = _validated_dictionary_block_payload(
            block_record,
            _dictionary_metadata_block(metadata, block_no),
        )
    if set(payload_by_block_no) != set(requested_block_nos):
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary blocks are missing")
    return payload_by_block_no


def _dictionary_blocks_statement(qualified_table: str) -> Any:
    """Build the selected-block payload query for a validated table name."""

    return text(
        f"""
        SELECT
            binary_block.block_no,
            binary_block.entry_count,
            binary_block.payload,
            COALESCE(binary_block.payload_compression, 'none') AS payload_compression,
            binary_block.raw_payload_bytes
          FROM {qualified_table} binary_block
         WHERE artifact_kind = :artifact_kind
           AND block_key = 0
           AND block_no = ANY(CAST(:block_nos AS integer[]))
         ORDER BY binary_block.block_no
        """
    )


def _dictionary_record_block_no(
    block_record: Mapping[str, Any],
    requested_block_nos: tuple[int, ...],
    returned_record_count: int,
) -> int:
    """Read a selected row's block number with single-row legacy fallback."""

    raw_block_no = block_record.get("block_no")
    if raw_block_no is not None:
        return _dictionary_metadata_integer(raw_block_no, "block_no")
    if len(requested_block_nos) == 1 and returned_record_count == 1:
        return requested_block_nos[0]
    raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block number is missing")


def _validated_dictionary_block_payload(
    block_record: Mapping[str, Any],
    expected_block: _DictionaryBlockMetadata,
) -> bytes:
    """Validate and decode one fetched dictionary block."""

    entry_count = _dictionary_metadata_integer(
        block_record.get("entry_count", expected_block.entry_count),
        "entry_count",
    )
    payload_compression = _dictionary_payload_compression(
        block_record.get("payload_compression", expected_block.payload_compression)
    )
    raw_payload_bytes = _dictionary_metadata_integer(
        block_record.get("raw_payload_bytes", expected_block.raw_payload_bytes),
        "raw_payload_bytes",
    )
    stored_payload = bytes(block_record.get("payload") or b"")
    has_matching_raw_payload_bytes = raw_payload_bytes == expected_block.raw_payload_bytes
    if payload_compression == "none":
        equivalent_raw_byte_counts = {0, entry_count * 16}
        has_matching_raw_payload_bytes = {
            raw_payload_bytes,
            expected_block.raw_payload_bytes,
        }.issubset(equivalent_raw_byte_counts)
    if (
        entry_count != expected_block.entry_count
        or payload_compression != expected_block.payload_compression
        or not has_matching_raw_payload_bytes
        or len(stored_payload) != expected_block.payload_bytes
    ):
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary block metadata changed")
    try:
        decoding_fields_by_name = dict(block_record)
        decoding_fields_by_name["payload_compression"] = payload_compression
        decoding_fields_by_name["raw_payload_bytes"] = raw_payload_bytes
        decoded_payload = _decode_serving_binary_payload(decoding_fields_by_name)
    except zlib.error as exc:
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary compressed payload is corrupt") from exc
    if len(decoded_payload) != expected_block.entry_count * 16:
        raise PTG2ManifestArtifactError(
            "PTG2 serving binary dictionary decoded payload byte count mismatch"
        )
    return decoded_payload


def _dictionary_value_lookup_hints(
    item_count: int | None,
    block_bytes: int | None,
    compressed_records: int | None,
) -> dict[str, int | None]:
    """Build optional metadata hints for fixed-width dictionary lookups."""

    if item_count is None:
        return {}
    return {
        "item_count_hint": item_count,
        "block_bytes_hint": block_bytes,
        "compressed_records_hint": compressed_records,
    }


def _validate_dictionary_keys(item_keys: Iterable[int], item_count: int) -> None:
    """Reject dictionary keys outside the immutable payload's item range."""

    if any(item_key < 0 or item_key >= item_count for item_key in item_keys):
        raise PTG2ManifestArtifactError("PTG2 serving binary dictionary key is out of range")


async def _binary_price_key_map_for_sets(
    session: Any,
    table_name: str,
    price_set_ids: Iterable[str],
) -> dict[str, int]:
    """Return price keys only for the requested price-set ids."""

    requested_ids = {
        _normalize_global_id(price_set_id)
        for price_set_id in price_set_ids
        if price_set_id
    }
    if not requested_ids:
        return {}
    dictionary_payload = await _serving_binary_dictionary_payload(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
    )
    price_key_by_set_id: dict[str, int] = {}
    for payload_offset in range(0, len(dictionary_payload), 16):
        candidate_id = bytes(dictionary_payload[payload_offset : payload_offset + 16])
        if candidate_id not in requested_ids:
            continue
        price_key_by_set_id[_id_text(candidate_id)] = payload_offset // 16
        if len(price_key_by_set_id) >= len(requested_ids):
            break
    return price_key_by_set_id


async def _binary_price_key_map(
    session: Any,
    table_name: str,
) -> dict[str, int]:
    """Return cached ``price_set_id -> price_key`` mappings for a binary table."""

    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cache_key = (relation_key, f"{_SERVING_BINARY_BY_CODE_DICTIONARY_KIND}:key_by_id")
    cached_key_by_id = _binary_dictionary_cache_get(cache_key)
    if cached_key_by_id is not _CACHE_MISS:
        return cached_key_by_id
    price_set_id_values = await _serving_binary_dictionary(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
    )
    price_key_by_set_id = {
        price_set_id: price_key
        for price_key, price_set_id in enumerate(price_set_id_values)
    }
    _binary_dictionary_cache_set(cache_key, price_key_by_set_id, max(len(price_set_id_values) * 48, 1))
    return price_key_by_set_id


async def _serving_binary_provider_counts(
    session: Any,
    table_name: str,
) -> dict[int, int] | None:
    qualified_table = _safe_qualified_table_name(table_name)
    relation_key = await _serving_binary_cache_relation_key(session, qualified_table)
    cache_key = (relation_key, "provider_set_count_dictionary")
    cached_provider_counts = _binary_dictionary_cache_get(cache_key)
    if cached_provider_counts is not _CACHE_MISS:
        return cached_provider_counts
    provider_count_result = await session.execute(
        text(
            f"""
            SELECT
                entry_count,
                payload,
                COALESCE(binary_block.payload_compression, 'none') AS payload_compression,
                binary_block.raw_payload_bytes
              FROM {qualified_table} binary_block
             WHERE artifact_kind = :artifact_kind
               AND block_key = 0
               AND block_no = 0
             LIMIT 1
            """
        ),
        {"artifact_kind": "provider_set_count_dictionary"},
    )
    provider_count_rows = _result_rows(provider_count_result)
    if not provider_count_rows:
        _binary_dictionary_cache_set(cache_key, None, 1)
        return None
    provider_count_record = _row_mapping(provider_count_rows[0])
    provider_count_payload = _decode_serving_binary_payload(provider_count_record)
    provider_count_map: dict[int, int] = {}
    payload_cursor = 0
    provider_set_key = 0
    for _ in range(int(provider_count_record.get("entry_count") or 0)):
        provider_delta, payload_cursor = _read_uvarint(provider_count_payload, payload_cursor)
        provider_count, payload_cursor = _read_uvarint(provider_count_payload, payload_cursor)
        provider_set_key += provider_delta
        provider_count_map[provider_set_key] = provider_count
    _binary_dictionary_cache_set(cache_key, provider_count_map, len(provider_count_payload))
    return provider_count_map


async def lookup_binary_price_atoms_from_db(
    session: Any,
    table_name: str,
    price_set_ids: Iterable[str],
) -> dict[str, tuple[str, ...]]:
    """Read ``price_set_id -> price_atom_ids`` mappings from PostgreSQL binary blocks."""

    normalized_set_ids = tuple(
        dict.fromkeys(_id_text(_normalize_global_id(source_price_set_id)) for source_price_set_id in price_set_ids)
    )
    if not normalized_set_ids:
        return {}
    price_key_by_set_id = await _binary_price_key_map_for_sets(session, table_name, normalized_set_ids)
    set_id_by_price_key = {
        price_key_by_set_id[price_set_id]: price_set_id
        for price_set_id in normalized_set_ids
        if price_set_id in price_key_by_set_id
    }
    if not set_id_by_price_key:
        return {}
    binary_block_records = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_PRICE_SET_ATOMS_KIND,
        block_keys=(price_key // _PRICE_SET_ATOM_BLOCK_SIZE for price_key in set_id_by_price_key),
    )
    if not binary_block_records:
        return {}
    atom_ids_by_set_id: dict[str, tuple[str, ...]] = {}
    requested_price_keys = set(set_id_by_price_key)
    for binary_block_record in binary_block_records:
        block_payload = _decode_serving_binary_payload(binary_block_record)
        cursor = 0
        for _ in range(int(binary_block_record.get("entry_count") or 0)):
            price_key, cursor = _read_uvarint(block_payload, cursor)
            atom_count, cursor = _read_uvarint(block_payload, cursor)
            price_atom_ids: list[str] = []
            for _atom_index in range(atom_count):
                atom_end = cursor + 16
                if atom_end > len(block_payload):
                    raise PTG2ManifestArtifactError("PTG2 serving binary price-set atom payload is truncated")
                price_atom_ids.append(_id_text(block_payload[cursor:atom_end]))
                cursor = atom_end
            if price_key in requested_price_keys:
                atom_ids_by_set_id[set_id_by_price_key[price_key]] = tuple(price_atom_ids)
    return atom_ids_by_set_id


async def lookup_atoms_by_price_key(
    session: Any,
    table_name: str,
    price_key_by_set_id: Mapping[str, int],
) -> dict[str, tuple[str, ...]]:
    """Read ``price_set_id -> price_atom_ids`` mappings when price keys are already known."""

    set_id_by_price_key = {
        int(price_key): _id_text(_normalize_global_id(price_set_id))
        for price_set_id, price_key in price_key_by_set_id.items()
        if price_set_id and price_key is not None
    }
    if not set_id_by_price_key:
        return {}
    binary_block_records = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_PRICE_SET_ATOMS_KIND,
        block_keys=(price_key // _PRICE_SET_ATOM_BLOCK_SIZE for price_key in set_id_by_price_key),
    )
    if not binary_block_records:
        return {}
    atom_ids_by_set_id: dict[str, tuple[str, ...]] = {}
    requested_price_keys = set(set_id_by_price_key)
    for binary_block_record in binary_block_records:
        block_payload = _decode_serving_binary_payload(binary_block_record)
        cursor = 0
        for _ in range(int(binary_block_record.get("entry_count") or 0)):
            price_key, cursor = _read_uvarint(block_payload, cursor)
            atom_count, cursor = _read_uvarint(block_payload, cursor)
            price_atom_ids: list[str] = []
            for _atom_index in range(atom_count):
                atom_end = cursor + 16
                if atom_end > len(block_payload):
                    raise PTG2ManifestArtifactError("PTG2 serving binary price-set atom payload is truncated")
                price_atom_ids.append(_id_text(block_payload[cursor:atom_end]))
                cursor = atom_end
            if price_key in requested_price_keys:
                atom_ids_by_set_id[set_id_by_price_key[price_key]] = tuple(price_atom_ids)
    return atom_ids_by_set_id


async def lookup_atoms_by_price_id(
    session: Any,
    table_name: str,
    price_set_ids: Iterable[str],
) -> dict[str, tuple[str, ...]]:
    """Read ``price_set_id -> price_atom_ids`` mappings directly by price-set id."""

    normalized_set_ids = tuple(
        dict.fromkeys(_id_text(_normalize_global_id(source_price_set_id)) for source_price_set_id in price_set_ids)
    )
    if not normalized_set_ids:
        return {}
    requested_ids = {bytes.fromhex(price_set_id) for price_set_id in normalized_set_ids}
    v2_block_records = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_V2_KIND,
        block_keys=(_price_set_atom_prefix_bucket(price_set_id) for price_set_id in normalized_set_ids),
    )
    atom_ids_by_set_id = _decode_price_set_atom_id_records(v2_block_records, requested_ids)
    missing_ids = requested_ids.difference(bytes.fromhex(price_set_id) for price_set_id in atom_ids_by_set_id)
    if not missing_ids:
        return atom_ids_by_set_id
    legacy_block_records = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_PRICE_SET_ATOMS_BY_ID_KIND,
        block_keys=(_price_set_atom_id_bucket(price_set_id) for price_set_id in missing_ids),
    )
    atom_ids_by_set_id.update(_decode_price_set_atom_id_records(legacy_block_records, missing_ids))
    return atom_ids_by_set_id


def _decode_price_set_atom_id_records(
    binary_block_records: Iterable[Mapping[str, Any]],
    requested_ids: set[bytes],
) -> dict[str, tuple[str, ...]]:
    """Decode selected price-set membership entries from ID-bucketed blocks."""

    atom_ids_by_set_id: dict[str, tuple[str, ...]] = {}
    for binary_block_record in binary_block_records:
        block_payload = _decode_serving_binary_payload(binary_block_record)
        cursor = 0
        for _ in range(int(binary_block_record.get("entry_count") or 0)):
            price_set_end = cursor + 16
            if price_set_end > len(block_payload):
                raise PTG2ManifestArtifactError("PTG2 serving binary price-set-id atom payload is truncated")
            price_set_id = bytes(block_payload[cursor:price_set_end])
            cursor = price_set_end
            atom_count, cursor = _read_uvarint(block_payload, cursor)
            atom_bytes = atom_count * 16
            atom_end = cursor + atom_bytes
            if atom_end > len(block_payload):
                raise PTG2ManifestArtifactError("PTG2 serving binary price-set-id atom payload is truncated")
            if price_set_id in requested_ids:
                price_atom_ids = tuple(
                    _id_text(block_payload[atom_cursor : atom_cursor + 16])
                    for atom_cursor in range(cursor, atom_end, 16)
                )
                atom_ids_by_set_id[_id_text(price_set_id)] = price_atom_ids
                if len(atom_ids_by_set_id) >= len(requested_ids):
                    return atom_ids_by_set_id
            cursor = atom_end
    return atom_ids_by_set_id


async def lookup_serving_binary_by_code_from_db(
    session: Any,
    table_name: str,
    code_key: int,
    *,
    provider_set_keys: Iterable[int] | None = None,
    price_dictionary_item_count: int | None = None,
    price_dictionary_block_bytes: int | None = None,
    price_dictionary_compressed_records: int | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    """Return decoded serving rows for one code block from PostgreSQL artifacts."""

    records = await _serving_binary_payload_rows(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        block_key=int(code_key),
    )
    grouped_payload = bool(records)
    if not records:
        records = await _serving_binary_payload_rows(
            session,
            table_name,
            artifact_kind=_SERVING_BINARY_BY_CODE_KIND,
            block_key=int(code_key),
        )
    if not records:
        return ()
    provider_count_map = await _serving_binary_provider_counts(session, table_name)
    decoded_keys = _decode_serving_binary_code_records(
        records,
        provider_count_map=provider_count_map,
        grouped_payload=grouped_payload,
        provider_set_keys=provider_set_keys,
    )
    needed_price_keys = {price_key for _provider_set_key, _provider_count, price_key in decoded_keys}
    price_ids_by_key = await _serving_binary_dictionary_values_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        item_keys=needed_price_keys,
        **_dictionary_value_lookup_hints(
            price_dictionary_item_count,
            price_dictionary_block_bytes,
            price_dictionary_compressed_records,
        ),
    )
    if set(price_ids_by_key) != needed_price_keys:
        raise PTG2ManifestArtifactError("PTG2 serving binary price key is out of range")
    return tuple(
        PTG2ServingBinaryRow(
            code_key=int(code_key),
            provider_set_key=provider_set_key,
            provider_count=provider_count,
            price_set_global_id_128=price_ids_by_key[price_key],
            price_key=price_key,
        )
        for provider_set_key, provider_count, price_key in decoded_keys
    )


async def lookup_price_ids_from_db(
    session: Any,
    table_name: str,
    price_keys: Iterable[int],
    *,
    price_dictionary_item_count: int | None = None,
    price_dictionary_block_bytes: int | None = None,
    price_dictionary_compressed_records: int | None = None,
) -> dict[int, str]:
    """Resolve selected dense price keys without decoding serving rows."""

    normalized_price_keys = {int(price_key) for price_key in price_keys}
    if not normalized_price_keys:
        return {}
    return await _serving_binary_dictionary_values_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        item_keys=normalized_price_keys,
        **_dictionary_value_lookup_hints(
            price_dictionary_item_count,
            price_dictionary_block_bytes,
            price_dictionary_compressed_records,
        ),
    )


def _decode_serving_binary_code_records(
    records: Iterable[Mapping[str, Any]],
    *,
    provider_count_map: dict[int, int] | None,
    grouped_payload: bool,
    provider_set_keys: Iterable[int] | None,
) -> list[tuple[int, int | None, int]]:
    """Decode code records and apply an optional provider-set filter."""

    provider_filter = {int(key) for key in provider_set_keys} if provider_set_keys is not None else None
    provider_filter_max = max(provider_filter) if provider_filter else None
    decoded_keys: list[tuple[int, int | None, int]] = []
    for binary_record in records:
        decoded_keys.extend(
            _decode_serving_binary_by_code_record(
                binary_record,
                provider_count_map=provider_count_map,
                grouped_payload=grouped_payload,
                provider_filter=provider_filter,
                provider_filter_max=provider_filter_max,
            )
        )
    return decoded_keys


async def has_serving_binary_code_block(
    session: Any,
    table_name: str,
    code_key: int,
) -> bool:
    """Return whether either supported forward artifact contains this code block."""

    grouped_records = await _serving_binary_payload_rows(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        block_key=int(code_key),
    )
    if grouped_records:
        return True
    legacy_records = await _serving_binary_payload_rows(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_KIND,
        block_key=int(code_key),
    )
    return bool(legacy_records)


serving_binary_code_block_exists = has_serving_binary_code_block


async def _serving_binary_forward_blocks_by_code(
    session: Any,
    table_name: str,
    code_keys: Iterable[int],
) -> tuple[dict[int, list[dict[str, Any]]], set[int]]:
    """Load grouped forward blocks, falling back to legacy blocks by code."""

    normalized_code_keys = tuple(sorted({int(code_key) for code_key in code_keys}))
    if not normalized_code_keys:
        return {}, set()
    grouped_block_rows = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_GROUPED_KIND,
        block_keys=normalized_code_keys,
    )
    grouped_blocks_by_code: dict[int, list[dict[str, Any]]] = {
        code_key: [] for code_key in normalized_code_keys
    }
    for block_row in grouped_block_rows:
        code_key = int(block_row.get("block_key") or 0)
        if code_key in grouped_blocks_by_code:
            grouped_blocks_by_code[code_key].append(block_row)
    missing_grouped_code_keys = [
        code_key for code_key, block_rows in grouped_blocks_by_code.items() if not block_rows
    ]
    legacy_blocks_by_code: dict[int, list[dict[str, Any]]] = {
        code_key: [] for code_key in missing_grouped_code_keys
    }
    if missing_grouped_code_keys:
        legacy_block_rows = await _serving_binary_payload_rows_for_keys(
            session,
            table_name,
            artifact_kind=_SERVING_BINARY_BY_CODE_KIND,
            block_keys=missing_grouped_code_keys,
        )
        for block_row in legacy_block_rows:
            code_key = int(block_row.get("block_key") or 0)
            if code_key in legacy_blocks_by_code:
                legacy_blocks_by_code[code_key].append(block_row)
    forward_blocks_by_code = {
        code_key: grouped_blocks_by_code[code_key] or legacy_blocks_by_code.get(code_key, [])
        for code_key in normalized_code_keys
    }
    grouped_code_keys = {
        code_key for code_key, block_rows in grouped_blocks_by_code.items() if block_rows
    }
    return forward_blocks_by_code, grouped_code_keys


def _decode_binary_forward_entries_by_code(
    forward_blocks_by_code: Mapping[int, Iterable[Mapping[str, Any]]],
    grouped_code_keys: set[int],
    provider_count_map: dict[int, int] | None,
    provider_set_keys: Iterable[int] | None,
) -> tuple[dict[int, list[tuple[int, int | None, int]]], set[int]]:
    """Decode requested forward block rows without resolving price ids."""

    provider_filter = {int(key) for key in provider_set_keys} if provider_set_keys is not None else None
    provider_filter_max = max(provider_filter) if provider_filter else None
    decoded_entries_by_code: dict[int, list[tuple[int, int | None, int]]] = {}
    needed_price_keys: set[int] = set()
    for code_key, block_rows in forward_blocks_by_code.items():
        decoded_entries = []
        for block_row in block_rows:
            decoded_entries.extend(
                _decode_serving_binary_by_code_record(
                    block_row,
                    provider_count_map=provider_count_map,
                    grouped_payload=code_key in grouped_code_keys,
                    provider_filter=provider_filter,
                    provider_filter_max=provider_filter_max,
                )
            )
        decoded_entries_by_code[code_key] = decoded_entries
        needed_price_keys.update(
            price_key for _provider_set_key, _provider_count, price_key in decoded_entries
        )
    return decoded_entries_by_code, needed_price_keys


async def lookup_binary_code_batch_from_db(
    session: Any,
    table_name: str,
    code_keys: Iterable[int],
    *,
    provider_set_keys: Iterable[int] | None = None,
    price_dictionary_item_count: int | None = None,
    price_dictionary_block_bytes: int | None = None,
    price_dictionary_compressed_records: int | None = None,
) -> dict[int, tuple[PTG2ServingBinaryRow, ...]]:
    """Read v2 forward code blocks in one batch for v3 reverse serving."""

    normalized_code_keys = tuple(sorted({int(code_key) for code_key in code_keys}))
    forward_blocks_by_code, grouped_code_keys = await _serving_binary_forward_blocks_by_code(
        session,
        table_name,
        normalized_code_keys,
    )
    missing_code_keys = [
        code_key for code_key, block_rows in forward_blocks_by_code.items() if not block_rows
    ]
    if missing_code_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward artifact is missing referenced code blocks"
        )
    provider_count_map = await _serving_binary_provider_counts(session, table_name)
    decoded_entries_by_code, needed_price_keys = _decode_binary_forward_entries_by_code(
        forward_blocks_by_code,
        grouped_code_keys,
        provider_count_map,
        provider_set_keys,
    )
    price_ids_by_key = await _serving_binary_dictionary_values_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        item_keys=needed_price_keys,
        **_dictionary_value_lookup_hints(
            price_dictionary_item_count,
            price_dictionary_block_bytes,
            price_dictionary_compressed_records,
        ),
    )
    if set(price_ids_by_key) != needed_price_keys:
        raise PTG2ManifestArtifactError("PTG2 serving binary price key is out of range")
    return {
        code_key: tuple(
            PTG2ServingBinaryRow(
                code_key=code_key,
                provider_set_key=provider_set_key,
                provider_count=provider_count,
                price_set_global_id_128=price_ids_by_key[price_key],
                price_key=price_key,
            )
            for provider_set_key, provider_count, price_key in decoded_entries_by_code[code_key]
        )
        for code_key in normalized_code_keys
    }


def _decode_provider_binary_record(
    binary_record: Mapping[str, Any],
    code_filter: set[int] | None,
) -> tuple[list[tuple[tuple[int, ...], tuple[tuple[int, int], ...]]], set[int]]:
    """Decode one provider-set binary block without resolving price ids."""

    raw_patterns: list[tuple[tuple[int, ...], tuple[tuple[int, int], ...]]] = []
    needed_price_keys: set[int] = set()
    provider_block_payload = _decode_serving_binary_payload(binary_record)
    cursor = 0
    for _ in range(int(binary_record.get("entry_count") or 0)):
        code_count, cursor = _read_uvarint(provider_block_payload, cursor)
        decoded_code_keys: list[int] = []
        previous_code_key = 0
        for index in range(code_count):
            encoded_code_key, cursor = _read_uvarint(provider_block_payload, cursor)
            code_key = encoded_code_key if index == 0 else previous_code_key + encoded_code_key
            if code_filter is None or code_key in code_filter:
                decoded_code_keys.append(code_key)
            previous_code_key = code_key
        entry_count, cursor = _read_uvarint(provider_block_payload, cursor)
        entries: list[tuple[int, int]] = []
        for _entry_index in range(entry_count):
            provider_count, cursor = _read_uvarint(provider_block_payload, cursor)
            price_key, cursor = _read_uvarint(provider_block_payload, cursor)
            if decoded_code_keys:
                entries.append((provider_count, price_key))
                needed_price_keys.add(price_key)
        if decoded_code_keys and entries:
            raw_patterns.append((tuple(decoded_code_keys), tuple(entries)))
    return raw_patterns, needed_price_keys


async def lookup_serving_binary_by_provider_set_patterns_from_db(
    session: Any,
    table_name: str,
    provider_set_key: int,
    *,
    code_keys: Iterable[int] | None = None,
) -> tuple[PTG2ServingProviderSetPattern, ...]:
    patterns_by_provider_set = await lookup_serving_binary_by_provider_sets_patterns_from_db(
        session,
        table_name,
        [int(provider_set_key)],
        code_keys=code_keys,
    )
    return patterns_by_provider_set.get(int(provider_set_key), ())


async def lookup_serving_binary_by_provider_sets_patterns_from_db(
    session: Any,
    table_name: str,
    provider_set_keys: Iterable[int],
    *,
    code_keys: Iterable[int] | None = None,
) -> dict[int, tuple[PTG2ServingProviderSetPattern, ...]]:
    normalized_provider_set_keys = tuple(
        sorted({int(provider_set_key) for provider_set_key in provider_set_keys})
    )
    if not normalized_provider_set_keys:
        return {}
    records = await _serving_binary_payload_rows_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_PROVIDER_SET_KIND,
        block_keys=normalized_provider_set_keys,
    )
    if not records:
        return {provider_set_key: () for provider_set_key in normalized_provider_set_keys}
    code_filter = {int(value) for value in code_keys} if code_keys is not None else None
    raw_patterns_by_provider_set: dict[
        int,
        list[tuple[tuple[int, ...], tuple[tuple[int, int], ...]]],
    ] = {provider_set_key: [] for provider_set_key in normalized_provider_set_keys}
    needed_price_keys: set[int] = set()
    for record in records:
        provider_set_key = int(record.get("block_key") or 0)
        if provider_set_key not in raw_patterns_by_provider_set:
            continue
        record_patterns, record_price_keys = _decode_provider_binary_record(record, code_filter)
        raw_patterns_by_provider_set[provider_set_key].extend(record_patterns)
        needed_price_keys.update(record_price_keys)
    price_ids_by_key = await _serving_binary_dictionary_values_for_keys(
        session,
        table_name,
        artifact_kind=_SERVING_BINARY_BY_PROVIDER_SET_DICTIONARY_KIND,
        item_keys=needed_price_keys,
    )
    patterns_by_provider_set: dict[int, tuple[PTG2ServingProviderSetPattern, ...]] = {}
    for provider_set_key in normalized_provider_set_keys:
        patterns: list[PTG2ServingProviderSetPattern] = []
        for decoded_code_keys, entries in raw_patterns_by_provider_set[provider_set_key]:
            resolved_entries = tuple(
                (provider_count, price_ids_by_key[price_key])
                for provider_count, price_key in entries
                if price_key in price_ids_by_key
            )
            if resolved_entries:
                patterns.append(
                    PTG2ServingProviderSetPattern(
                        code_keys=decoded_code_keys,
                        entries=resolved_entries,
                    )
                )
        patterns_by_provider_set[provider_set_key] = tuple(patterns)
    return patterns_by_provider_set


async def lookup_serving_binary_by_provider_set_from_db(
    session: Any,
    table_name: str,
    provider_set_key: int,
    *,
    code_keys: Iterable[int] | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    patterns = await lookup_serving_binary_by_provider_set_patterns_from_db(
        session,
        table_name,
        int(provider_set_key),
        code_keys=code_keys,
    )
    rows: list[PTG2ServingSidecarRow] = []
    for pattern in patterns:
        for code_key in pattern.code_keys:
            for provider_count, price_set_id in pattern.entries:
                rows.append(
                    PTG2ServingSidecarRow(
                        code_key=code_key,
                        provider_set_key=int(provider_set_key),
                        provider_count=provider_count,
                        price_set_global_id_128=price_set_id,
                    )
                )
    return tuple(rows)


def _index_lookup(index_bytes: bytes, key: bytes, entry_count: int) -> tuple[int, int] | None:
    low = 0
    high = int(entry_count) - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = mid * _MEMBERSHIP_INDEX_RECORD.size
        candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(index_bytes, record_offset)
        if candidate_owner < key:
            low = mid + 1
            continue
        if candidate_owner > key:
            high = mid - 1
            continue
        return int(member_offset), int(member_count)
    return None


async def _index_lookup_from_db(
    reader: PTG2DbArtifactReader,
    *,
    index_start: int,
    entry_count: int,
    key: bytes,
) -> tuple[int, int] | None:
    low = 0
    high = int(entry_count) - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = int(index_start) + mid * _MEMBERSHIP_INDEX_RECORD.size
        candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack(
            await reader.read_at(record_offset, _MEMBERSHIP_INDEX_RECORD.size)
        )
        if candidate_owner < key:
            low = mid + 1
            continue
        if candidate_owner > key:
            high = mid - 1
            continue
        return int(member_offset), int(member_count)
    return None


async def _index_lookup_many_from_db(
    reader: PTG2DbArtifactReader,
    *,
    index_start: int,
    entry_count: int,
    keys: Iterable[bytes],
) -> dict[bytes, tuple[int, int] | None]:
    search_range_by_key: dict[bytes, tuple[int, int]] = {bytes(key): (0, int(entry_count) - 1) for key in keys}
    index_match_by_key: dict[bytes, tuple[int, int] | None] = {}
    while search_range_by_key:
        record_numbers_by_key: dict[bytes, int] = {}
        next_search_range_by_key: dict[bytes, tuple[int, int]] = {}
        for key, (low, high) in search_range_by_key.items():
            if low > high:
                index_match_by_key[key] = None
                continue
            record_numbers_by_key[key] = (low + high) // 2
        if not record_numbers_by_key:
            break
        fixed_records_by_number = await reader.read_fixed_records(
            base_offset=index_start,
            record_size=_MEMBERSHIP_INDEX_RECORD.size,
            record_numbers=record_numbers_by_key.values(),
        )
        for key, mid in record_numbers_by_key.items():
            candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack(fixed_records_by_number[mid])
            low, high = search_range_by_key[key]
            if candidate_owner < key:
                next_search_range_by_key[key] = (mid + 1, high)
                continue
            if candidate_owner > key:
                next_search_range_by_key[key] = (low, mid - 1)
                continue
            index_match_by_key[key] = int(member_offset), int(member_count)
        search_range_by_key = next_search_range_by_key
    return index_match_by_key


async def _maybe_read_membership_index(
    reader: PTG2DbArtifactReader,
    *,
    index_start: int,
    index_bytes_len: int,
) -> bytes | None:
    if _INDEX_READ_MAX_BYTES and int(index_bytes_len) > _INDEX_READ_MAX_BYTES:
        return None
    return await reader.read_at(index_start, int(index_bytes_len))


async def lookup_global_sidecar_members_many_from_db(
    session: Any,
    entry: Mapping[str, Any],
    owners: Iterable[bytes | bytearray | memoryview | str],
    *,
    schema_name: str,
    max_members: int | None = None,
) -> dict[bytes, tuple[bytes, ...]]:
    owner_ids = tuple(dict.fromkeys(_normalize_global_id(owner) for owner in owners))
    if not owner_ids:
        return {}
    _validate_membership_record_format(entry)
    reader = PTG2DbArtifactReader(session, entry, schema_name=schema_name)
    first = await reader.read_at(0, _DENSE_MEMBERSHIP_HEADER.size)
    magic = first[:8]
    if magic in _STANDARD_MEMBERSHIP_MAGICS:
        magic, version, entry_count = _MEMBERSHIP_HEADER.unpack_from(first[: _MEMBERSHIP_HEADER.size], 0)
        if version != PTG2_MANIFEST_VERSION:
            raise PTG2ManifestArtifactError(f"unsupported global membership sidecar version: {version!r}")
        expected_entries = entry.get("entry_count", entry.get("owner_count"))
        if expected_entries is not None and int(expected_entries) != int(entry_count):
            raise PTG2ManifestArtifactError("global membership sidecar entry count mismatch")
        index_start = _MEMBERSHIP_HEADER.size
        index_bytes_len = int(entry_count) * _MEMBERSHIP_INDEX_RECORD.size
        member_start = index_start + index_bytes_len
        index_bytes = await _maybe_read_membership_index(
            reader,
            index_start=index_start,
            index_bytes_len=index_bytes_len,
        )
        db_matches = None
        if index_bytes is None:
            db_matches = await _index_lookup_many_from_db(
                reader,
                index_start=index_start,
                entry_count=int(entry_count),
                keys=owner_ids,
            )
        result: dict[bytes, tuple[bytes, ...]] = {}
        member_ranges_by_owner: dict[bytes, tuple[int, int]] = {}
        for owner_id in owner_ids:
            if index_bytes is not None:
                match = _index_lookup(index_bytes, owner_id, int(entry_count))
            else:
                match = db_matches.get(owner_id) if db_matches is not None else None
            if match is None:
                result[owner_id] = ()
                continue
            member_offset, member_count = match
            if max_members is not None:
                member_count = min(member_count, max(max_members, 0))
            member_ranges_by_owner[owner_id] = (member_start + member_offset * 16, member_count * 16)
        member_payloads_by_owner = await reader.read_ranges(member_ranges_by_owner)
        for owner_id, member_bytes in member_payloads_by_owner.items():
            result[owner_id] = tuple(member_bytes[pos : pos + 16] for pos in range(0, len(member_bytes), 16))
        return result
    if magic not in _DENSE_MEMBERSHIP_MAGICS:
        raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")

    magic, version, entry_count, member_global_count = _DENSE_MEMBERSHIP_HEADER.unpack_from(first, 0)
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported dense global membership sidecar version: {version!r}")
    expected_entries = entry.get("entry_count", entry.get("owner_count"))
    if expected_entries is not None and int(expected_entries) != int(entry_count):
        raise PTG2ManifestArtifactError("dense global membership sidecar entry count mismatch")
    expected_member_globals = entry.get("member_global_count")
    if expected_member_globals is not None and int(expected_member_globals) != int(member_global_count):
        raise PTG2ManifestArtifactError("dense global membership sidecar member dictionary count mismatch")

    index_start = _DENSE_MEMBERSHIP_HEADER.size
    index_bytes_len = int(entry_count) * _MEMBERSHIP_INDEX_RECORD.size
    globals_start = index_start + index_bytes_len
    globals_bytes_len = int(member_global_count) * 16
    members_start = globals_start + globals_bytes_len
    index_bytes = await _maybe_read_membership_index(
        reader,
        index_start=index_start,
        index_bytes_len=index_bytes_len,
    )
    db_matches = None
    if index_bytes is None:
        db_matches = await _index_lookup_many_from_db(
            reader,
            index_start=index_start,
            entry_count=int(entry_count),
            keys=owner_ids,
        )
    result: dict[bytes, tuple[bytes, ...]] = {}
    local_ranges_by_owner: dict[bytes, tuple[int, int]] = {}
    for owner_id in owner_ids:
        if index_bytes is not None:
            match = _index_lookup(index_bytes, owner_id, int(entry_count))
        else:
            match = db_matches.get(owner_id) if db_matches is not None else None
        if match is None:
            result[owner_id] = ()
            continue
        member_offset, member_count = match
        if max_members is not None:
            member_count = min(member_count, max(max_members, 0))
        local_ranges_by_owner[owner_id] = (
            members_start + member_offset * _DENSE_MEMBER_RECORD.size,
            member_count * _DENSE_MEMBER_RECORD.size,
        )
    local_payloads_by_owner = await reader.read_ranges(local_ranges_by_owner)
    local_ids_by_owner: dict[bytes, list[int]] = {}
    requested_local_ids: set[int] = set()
    for owner_id, local_bytes in local_payloads_by_owner.items():
        local_ids: list[int] = []
        for pos in range(0, len(local_bytes), _DENSE_MEMBER_RECORD.size):
            local_id = _DENSE_MEMBER_RECORD.unpack_from(local_bytes, pos)[0]
            if local_id >= int(member_global_count):
                raise PTG2ManifestArtifactError("dense global membership sidecar member id is out of range")
            local_ids.append(int(local_id))
            requested_local_ids.add(int(local_id))
        local_ids_by_owner[owner_id] = local_ids
    members_by_local_id = await reader.read_fixed_records(
        base_offset=globals_start,
        record_size=16,
        record_numbers=sorted(requested_local_ids),
    )
    for owner_id, local_ids in local_ids_by_owner.items():
        members = [members_by_local_id[local_id] for local_id in local_ids]
        result[owner_id] = tuple(members)
    return result


async def lookup_global_sidecar_members_from_db(
    session: Any,
    entry: Mapping[str, Any],
    owner: bytes | bytearray | memoryview | str,
    *,
    schema_name: str,
    max_members: int | None = None,
) -> tuple[bytes, ...]:
    owner_id = _normalize_global_id(owner)
    result = await lookup_global_sidecar_members_many_from_db(
        session,
        entry,
        (owner_id,),
        schema_name=schema_name,
        max_members=max_members,
    )
    return result.get(owner_id, ())


async def _serving_header(
    reader: PTG2DbArtifactReader,
    *,
    magic: bytes,
    expected_format: str,
    metadata: Mapping[str, Any],
) -> tuple[dict[str, Any], int]:
    prefix = await reader.read_at(0, 12)
    if prefix[:8] != magic:
        raise PTG2ManifestArtifactError("serving sidecar has an invalid magic header")
    header_len = struct.unpack("<I", prefix[8:12])[0]
    header_end = 12 + int(header_len)
    header = json.loads((await reader.read_at(12, int(header_len))).decode("utf-8"))
    if not isinstance(header, dict):
        raise PTG2ManifestArtifactError("serving sidecar header must be a JSON object")
    header_format = str(header.get("format") or "").strip().lower()
    if header_format != expected_format:
        raise PTG2ManifestArtifactError(f"unsupported serving sidecar format: {header_format!r}")
    expected_row_count = metadata.get("row_count")
    if expected_row_count is not None and int(header.get("row_count") or 0) != int(expected_row_count):
        raise PTG2ManifestArtifactError("serving sidecar row count mismatch")
    return header, header_end


async def _lookup_serving_block(
    reader: PTG2DbArtifactReader,
    *,
    key: int,
    magic: bytes,
    expected_format: str,
    block_count_field: str,
    metadata: Mapping[str, Any],
) -> tuple[dict[str, Any], int, int, int, int, int, int]:
    header, header_end = await _serving_header(reader, magic=magic, expected_format=expected_format, metadata=metadata)
    price_count = int(header.get("price_set_count") or 0)
    block_count = int(header.get(block_count_field) or 0)
    price_start = header_end
    index_start = price_start + price_count * 16
    index_bytes_len = block_count * _SERVING_BLOCK_INDEX_RECORD.size
    body_start = index_start + index_bytes_len
    total_size = await reader.byte_count()
    if body_start > total_size:
        raise PTG2ManifestArtifactError("serving sidecar index is truncated")
    if _INDEX_READ_MAX_BYTES and index_bytes_len > _INDEX_READ_MAX_BYTES:
        raise PTG2ManifestArtifactError("serving sidecar block index is too large for DB lookup")
    index_bytes = await reader.read_at(index_start, index_bytes_len)
    low = 0
    high = block_count - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = mid * _SERVING_BLOCK_INDEX_RECORD.size
        candidate_key, body_offset, row_count = _SERVING_BLOCK_INDEX_RECORD.unpack_from(index_bytes, record_offset)
        if candidate_key < key:
            low = mid + 1
            continue
        if candidate_key > key:
            high = mid - 1
            continue
        if mid + 1 < block_count:
            next_offset = _SERVING_BLOCK_INDEX_RECORD.unpack_from(
                index_bytes,
                (mid + 1) * _SERVING_BLOCK_INDEX_RECORD.size,
            )[1]
        else:
            next_offset = int(header.get("body_bytes") or 0)
        body_len = int(next_offset) - int(body_offset)
        if body_len < 0:
            raise PTG2ManifestArtifactError("serving sidecar block offsets are invalid")
        return header, price_start, body_start, int(body_offset), body_len, int(row_count), price_count
    return header, price_start, body_start, 0, 0, 0, price_count


def _contiguous_int_ranges(values: Iterable[int]) -> list[tuple[int, int]]:
    ordered = sorted({int(value) for value in values})
    if not ordered:
        return []
    ranges: list[tuple[int, int]] = []
    start = prev = ordered[0]
    for value in ordered[1:]:
        if value == prev + 1:
            prev = value
            continue
        ranges.append((start, prev))
        start = prev = value
    ranges.append((start, prev))
    return ranges


async def _price_ids_for_keys(
    reader: PTG2DbArtifactReader,
    *,
    price_start: int,
    price_count: int,
    price_keys: Iterable[int],
) -> dict[int, str]:
    result: dict[int, str] = {}
    for start, end in _contiguous_int_ranges(int(key) for key in price_keys if 0 <= int(key) < price_count):
        raw = await reader.read_at(price_start + start * 16, (end - start + 1) * 16)
        for index, pos in enumerate(range(0, len(raw), 16)):
            result[start + index] = _id_text(raw[pos : pos + 16])
    return result


async def lookup_serving_by_code_sidecar_from_db(
    session: Any,
    entry: Mapping[str, Any],
    code_key: int,
    *,
    schema_name: str,
    provider_set_keys: Iterable[int] | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    reader = PTG2DbArtifactReader(session, entry, schema_name=schema_name)
    _header, price_start, body_start, body_offset, body_len, row_count, price_count = await _lookup_serving_block(
        reader,
        key=int(code_key),
        magic=PTG2_SERVING_BY_CODE_MAGIC,
        expected_format=PTG2_SERVING_BY_CODE_FORMAT,
        block_count_field="code_count",
        metadata=entry,
    )
    if row_count <= 0:
        return ()
    provider_filter = {int(value) for value in provider_set_keys} if provider_set_keys is not None else None
    provider_filter_max = max(provider_filter) if provider_filter else None
    body = await reader.read_at(body_start + body_offset, body_len)
    cursor = 0
    provider_set_key = 0
    decoded: list[tuple[int, int, int]] = []
    for _ in range(row_count):
        provider_delta, cursor = _read_uvarint(body, cursor)
        provider_count, cursor = _read_uvarint(body, cursor)
        price_key, cursor = _read_uvarint(body, cursor)
        if price_key >= price_count:
            raise PTG2ManifestArtifactError("serving-by-code sidecar price key is out of range")
        provider_set_key += provider_delta
        if provider_filter is not None and provider_set_key not in provider_filter:
            if provider_filter_max is not None and provider_set_key > provider_filter_max:
                break
            continue
        decoded.append((provider_set_key, provider_count, price_key))
    price_ids = await _price_ids_for_keys(reader, price_start=price_start, price_count=price_count, price_keys=(item[2] for item in decoded))
    return tuple(
        PTG2ServingSidecarRow(
            code_key=int(code_key),
            provider_set_key=provider_set_key,
            provider_count=provider_count,
            price_set_global_id_128=price_ids[price_key],
        )
        for provider_set_key, provider_count, price_key in decoded
        if price_key in price_ids
    )


async def lookup_serving_by_provider_set_patterns_from_db(
    session: Any,
    entry: Mapping[str, Any],
    provider_set_key: int,
    *,
    schema_name: str,
    code_keys: Iterable[int] | None = None,
) -> tuple[PTG2ServingProviderSetPattern, ...]:
    reader = PTG2DbArtifactReader(session, entry, schema_name=schema_name)
    _header, price_start, body_start, body_offset, body_len, pattern_count, price_count = await _lookup_serving_block(
        reader,
        key=int(provider_set_key),
        magic=PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        expected_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        block_count_field="provider_set_count",
        metadata=entry,
    )
    if pattern_count <= 0:
        return ()
    code_filter = {int(value) for value in code_keys} if code_keys is not None else None
    body = await reader.read_at(body_start + body_offset, body_len)
    cursor = 0
    raw_patterns: list[tuple[tuple[int, ...], tuple[tuple[int, int], ...]]] = []
    price_keys_needed: set[int] = set()
    for _ in range(pattern_count):
        code_count, cursor = _read_uvarint(body, cursor)
        decoded_code_keys: list[int] = []
        previous_code_key = 0
        for index in range(code_count):
            encoded_code_key, cursor = _read_uvarint(body, cursor)
            code_key = encoded_code_key if index == 0 else previous_code_key + encoded_code_key
            if code_filter is None or code_key in code_filter:
                decoded_code_keys.append(code_key)
            previous_code_key = code_key
        entry_count, cursor = _read_uvarint(body, cursor)
        entries: list[tuple[int, int]] = []
        for _entry_index in range(entry_count):
            provider_count, cursor = _read_uvarint(body, cursor)
            price_key, cursor = _read_uvarint(body, cursor)
            if price_key >= price_count:
                raise PTG2ManifestArtifactError("serving-by-provider-set sidecar price key is out of range")
            if decoded_code_keys:
                entries.append((provider_count, price_key))
                price_keys_needed.add(price_key)
        if decoded_code_keys and entries:
            raw_patterns.append((tuple(decoded_code_keys), tuple(entries)))
    price_ids = await _price_ids_for_keys(
        reader,
        price_start=price_start,
        price_count=price_count,
        price_keys=price_keys_needed,
    )
    patterns: list[PTG2ServingProviderSetPattern] = []
    for decoded_code_keys, entries in raw_patterns:
        resolved_entries = tuple(
            (provider_count, price_ids[price_key])
            for provider_count, price_key in entries
            if price_key in price_ids
        )
        if resolved_entries:
            patterns.append(
                PTG2ServingProviderSetPattern(
                    code_keys=decoded_code_keys,
                    entries=resolved_entries,
                )
            )
    return tuple(patterns)


async def lookup_serving_by_provider_set_sidecar_from_db(
    session: Any,
    entry: Mapping[str, Any],
    provider_set_key: int,
    *,
    schema_name: str,
    code_keys: Iterable[int] | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    patterns = await lookup_serving_by_provider_set_patterns_from_db(
        session,
        entry,
        provider_set_key,
        schema_name=schema_name,
        code_keys=code_keys,
    )
    rows: list[PTG2ServingSidecarRow] = []
    for pattern in patterns:
        for code_key in pattern.code_keys:
            for provider_count, price_set_id in pattern.entries:
                rows.append(
                    PTG2ServingSidecarRow(
                        code_key=code_key,
                        provider_set_key=int(provider_set_key),
                        provider_count=provider_count,
                        price_set_global_id_128=price_set_id,
                    )
                )
    return tuple(rows)
