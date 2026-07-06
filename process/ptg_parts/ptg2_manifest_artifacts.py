# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small PTG2 artifact primitives.

PTG2 keeps globally stable 128-bit content ids separate from dense ids that
are only meaningful inside a snapshot.  This module intentionally handles only
the reusable file-level primitive for that relationship:

* a deterministic JSON manifest
* fixed-width binary sidecars
* read-time byte-count and SHA-256 validation

The import and publish flows are deliberately out of scope here.
"""

from __future__ import annotations

import hashlib
import json
import mmap
import os
import struct
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterable, Iterable, Mapping


PTG2_MANIFEST_VERSION = 1
PTG2_MANIFEST_MAPPING_ARTIFACT_TYPE = "ptg2_manifest_global_local_id_mapping"
PTG2_MANIFEST_MAPPING_RECORD_FORMAT = "global_id_be16:uint32_be"
PTG2_MANIFEST_MAPPING_RECORD_SIZE = 20
PTG2_MANIFEST_MEMBERSHIP_ARTIFACT_TYPE = "ptg2_manifest_global_membership_sidecar"
PTG2_MANIFEST_MEMBERSHIP_MAGIC = b"PTG2MNSC"
PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC = bytes.fromhex("5054473256335343")
PTG2_MANIFEST_MEMBERSHIP_FORMAT = "magic8:uint32_le_version:uint64_le_entry_count:index(owner16:uint64_le_offset:uint32_le_count):members16"
PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC = b"PTG2MNDS"
PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC = bytes.fromhex("5054473256334453")
PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:uint64_le_member_global_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):member_globals16:members_uint32_le"
)
PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE = 28
PTG2_MANIFEST_MEMBERSHIP_HEADER_SIZE = 20
PTG2_MANIFEST_DENSE_MEMBERSHIP_HEADER_SIZE = 28
PTG2_SERVING_BY_CODE_MAGIC = b"PTG2SBC1"
PTG2_SERVING_BY_PROVIDER_SET_MAGIC = b"PTG2SBP1"
PTG2_SERVING_BY_CODE_FORMAT = "ptg2_serving_by_code_v1"
PTG2_SERVING_BY_PROVIDER_SET_FORMAT = "ptg2_serving_by_provider_set_v1"
PTG2_SERVING_BY_CODE_ARTIFACT_KIND = "ptg2_serving_by_code"
PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND = "ptg2_serving_by_provider_set"
PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE = 16
_MAPPING_RECORD = struct.Struct(">16sI")
_MEMBERSHIP_HEADER = struct.Struct("<8sIQ")
_DENSE_MEMBERSHIP_HEADER = struct.Struct("<8sIQQ")
_MEMBERSHIP_INDEX_RECORD = struct.Struct("<16sQI")
_DENSE_MEMBER_RECORD = struct.Struct("<I")
_SERVING_BLOCK_INDEX_RECORD = struct.Struct("<iQI")
_UINT32_MAX = 2**32 - 1
_PTG2_MANIFEST_MEMBERSHIP_FORMATS = {PTG2_MANIFEST_MEMBERSHIP_FORMAT, PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT}
_SIDE_CAR_MMAP_LOCK = threading.Lock()
_SIDE_CAR_MMAP_CACHE: dict[str, tuple[Any, mmap.mmap, int, int]] = {}
_STANDARD_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_MEMBERSHIP_MAGIC}
_DENSE_MEMBERSHIP_MAGICS = {PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, PTG2_MANIFEST_OLD_DENSE_MEMBERSHIP_MAGIC}


class PTG2ManifestArtifactError(ValueError):
    """Raised when a PTG2 artifact is malformed or fails validation."""


@dataclass(frozen=True)
class PTG2ManifestSidecarEntry:
    """One owner entry from a Rust PTG2 global sidecar."""

    owner: bytes
    members: tuple[bytes, ...]


@dataclass(frozen=True)
class PTG2ServingSidecarRow:
    """One compact PTG2 serving row decoded from a serving sidecar."""

    code_key: int
    provider_set_key: int
    provider_count: int
    price_set_global_id_128: str


@dataclass(frozen=True)
class PTG2ServingProviderSetPattern:
    """One reverse-sidecar pattern shared by one or more code keys."""

    code_keys: tuple[int, ...]
    entries: tuple[tuple[int, str], ...]


def _validate_membership_record_format(metadata: Mapping[str, Any]) -> None:
    record_format = metadata.get("record_format")
    if record_format is not None and record_format not in _PTG2_MANIFEST_MEMBERSHIP_FORMATS:
        raise PTG2ManifestArtifactError("global membership sidecar has an unexpected record format")


def _is_standard_membership_magic(magic: bytes) -> bool:
    return magic in _STANDARD_MEMBERSHIP_MAGICS


def _is_dense_membership_magic(magic: bytes) -> bool:
    return magic in _DENSE_MEMBERSHIP_MAGICS


def _sidecar_mmap_cache_enabled() -> bool:
    raw = os.getenv("HLTHPRT_PTG2_MANIFEST_SIDECAR_MMAP_CACHE", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _cached_sidecar_mmap(path: Path, *, metadata: Mapping[str, Any] | None = None) -> mmap.mmap:
    sidecar_path = path.resolve()
    stat_result = sidecar_path.stat()
    if metadata is not None:
        expected_byte_count = metadata.get("byte_count")
        if not isinstance(expected_byte_count, int) or expected_byte_count < 0:
            raise PTG2ManifestArtifactError("PTG2 sidecar is missing a non-negative byte count")
        if stat_result.st_size != expected_byte_count:
            raise PTG2ManifestArtifactError(
                f"PTG2 sidecar byte_count mismatch for {sidecar_path.name}: "
                f"expected {expected_byte_count}, got {stat_result.st_size}"
            )
    cache_key = str(sidecar_path)
    with _SIDE_CAR_MMAP_LOCK:
        cached = _SIDE_CAR_MMAP_CACHE.get(cache_key)
        if cached is not None:
            _fp, payload, cached_size, cached_mtime_ns = cached
            if cached_size == stat_result.st_size and cached_mtime_ns == stat_result.st_mtime_ns:
                return payload
            payload.close()
            _fp.close()
            _SIDE_CAR_MMAP_CACHE.pop(cache_key, None)
        fp = open(sidecar_path, "rb")
        try:
            payload = mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ)
        except Exception:
            fp.close()
            raise
        _SIDE_CAR_MMAP_CACHE[cache_key] = (fp, payload, stat_result.st_size, stat_result.st_mtime_ns)
        return payload


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            digest.update(chunk)
            byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def _normalize_global_id(value: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(value, str):
        if len(value) != 32:
            raise PTG2ManifestArtifactError("global id strings must be 32-character hex values")
        try:
            raw = bytes.fromhex(value)
        except ValueError as exc:
            raise PTG2ManifestArtifactError("global id strings must be 32-character hex values") from exc
    else:
        raw = bytes(value)
    if len(raw) != 16:
        raise PTG2ManifestArtifactError(f"global ids must be 16 bytes; got {len(raw)}")
    return raw


def _normalize_local_ids(values: Iterable[int]) -> tuple[int, ...]:
    normalized: set[int] = set()
    for value in values:
        local_id = int(value)
        if local_id < 0 or local_id > _UINT32_MAX:
            raise PTG2ManifestArtifactError(f"local ids must fit uint32; got {value!r}")
        normalized.add(local_id)
    return tuple(sorted(normalized))


def _canonical_mapping(
    mapping: Mapping[bytes | bytearray | memoryview | str, Iterable[int]],
) -> list[tuple[bytes, tuple[int, ...]]]:
    merged: dict[bytes, set[int]] = {}
    for global_id, local_ids in mapping.items():
        normalized_global_id = _normalize_global_id(global_id)
        merged.setdefault(normalized_global_id, set()).update(_normalize_local_ids(local_ids))
    return sorted((global_id, tuple(sorted(local_ids))) for global_id, local_ids in merged.items())


def build_dense_id_mapping(
    global_ids: Iterable[bytes | bytearray | memoryview | str],
) -> dict[bytes, int]:
    """Assign deterministic uint32 dense ids by sorted 16-byte global id."""

    normalized = sorted({_normalize_global_id(global_id) for global_id in global_ids})
    if len(normalized) > _UINT32_MAX + 1:
        raise PTG2ManifestArtifactError("dense id mapping exceeds uint32 capacity")
    return {global_id: index for index, global_id in enumerate(normalized)}


def _canonical_membership(
    mapping: Mapping[bytes | bytearray | memoryview | str, Iterable[bytes | bytearray | memoryview | str]],
) -> list[tuple[bytes, tuple[bytes, ...]]]:
    merged: dict[bytes, set[bytes]] = {}
    for owner_id, member_ids in mapping.items():
        normalized_owner_id = _normalize_global_id(owner_id)
        members = {_normalize_global_id(member_id) for member_id in member_ids}
        merged.setdefault(normalized_owner_id, set()).update(members)
    return sorted((owner_id, tuple(sorted(member_ids))) for owner_id, member_ids in merged.items())


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_bytes(payload)
    os.replace(tmp_path, path)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    _atomic_write_bytes(path, encoded.encode("utf-8"))


def _write_uvarint(fp: Any, value: int) -> None:
    encoded = int(value)
    if encoded < 0:
        raise PTG2ManifestArtifactError(f"uvarint values must be non-negative; got {value!r}")
    while True:
        byte = encoded & 0x7F
        encoded >>= 7
        if encoded:
            fp.write(bytes([byte | 0x80]))
            continue
        fp.write(bytes([byte]))
        return


def _read_uvarint(payload: bytes | bytearray | memoryview | mmap.mmap, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    cursor = int(offset)
    while True:
        if cursor >= len(payload):
            raise PTG2ManifestArtifactError("serving sidecar ended inside a uvarint")
        byte = int(payload[cursor])
        cursor += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, cursor
        shift += 7
        if shift > 63:
            raise PTG2ManifestArtifactError("serving sidecar uvarint is too large")


def _normalize_128_id(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        text_value = str(value or "").strip().lower().replace("-", "")
        try:
            raw = bytes.fromhex(text_value)
        except ValueError as exc:
            raise PTG2ManifestArtifactError(f"128-bit ids must be hex or uuid text; got {value!r}") from exc
    if len(raw) != 16:
        raise PTG2ManifestArtifactError(f"128-bit ids must be 16 bytes; got {len(raw)}")
    return raw


def _id_text(raw: bytes | bytearray | memoryview) -> str:
    value = bytes(raw)
    if len(value) != 16:
        raise PTG2ManifestArtifactError(f"128-bit ids must be 16 bytes; got {len(value)}")
    return value.hex()


def _serving_sidecar_path_entry(
    *,
    name: str,
    path: Path,
    kind: str,
    record_format: str,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    sha256, byte_count = _sha256_file(path)
    return {
        "name": name,
        "kind": kind,
        "path": str(path),
        "format": record_format,
        "record_format": record_format,
        "sha256": sha256,
        "byte_count": byte_count,
        **dict(metadata),
    }


def _serving_sidecar_header(
    payload: bytes | bytearray | memoryview | mmap.mmap,
    magic: bytes,
    expected_format: str,
    metadata: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], int]:
    if len(payload) < 12 or bytes(payload[:8]) != magic:
        raise PTG2ManifestArtifactError("serving sidecar has an invalid magic header")
    header_len = struct.unpack("<I", bytes(payload[8:12]))[0]
    header_start = 12
    header_end = header_start + header_len
    if header_end > len(payload):
        raise PTG2ManifestArtifactError("serving sidecar header is truncated")
    header = json.loads(bytes(payload[header_start:header_end]).decode("utf-8"))
    if not isinstance(header, dict):
        raise PTG2ManifestArtifactError("serving sidecar header must be a JSON object")
    header_format = str(header.get("format") or "").strip().lower()
    if header_format != expected_format:
        raise PTG2ManifestArtifactError(f"unsupported serving sidecar format: {header_format!r}")
    if metadata is not None:
        expected_byte_count = metadata.get("byte_count")
        if isinstance(expected_byte_count, int) and expected_byte_count >= 0 and len(payload) != expected_byte_count:
            raise PTG2ManifestArtifactError(
                f"serving sidecar byte_count mismatch: expected {expected_byte_count}, got {len(payload)}"
            )
        expected_row_count = metadata.get("row_count")
        if expected_row_count is not None and int(header.get("row_count") or 0) != int(expected_row_count):
            raise PTG2ManifestArtifactError("serving sidecar row count mismatch")
    return header, header_end


def write_serving_by_code_sidecar(
    path: str | Path,
    rows: Iterable[Iterable[Any]],
    *,
    name: str = "serving_by_code",
) -> dict[str, Any]:
    """Write compact serving rows grouped by code key.

    Input rows must be ordered by ``code_key, provider_set_key, price_set_global_id_128``
    and shaped as ``code_key, provider_set_key, provider_count, price_set_global_id_128``.
    """

    sidecar_path = Path(path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    body_path = sidecar_path.with_name(f"{sidecar_path.name}.body.tmp")
    tmp_path = sidecar_path.with_name(f"{sidecar_path.name}.tmp")
    price_set_to_key: dict[bytes, int] = {}
    price_set_values: list[bytes] = []
    blocks: list[dict[str, int]] = []
    row_count = 0
    body_offset = 0
    current_code: int | None = None
    current_block_count = 0
    previous_provider_set_key = 0

    with body_path.open("wb") as body:
        for raw_row in rows:
            values = list(raw_row)
            if len(values) < 4:
                raise PTG2ManifestArtifactError("serving-by-code rows must have at least four columns")
            code_key = int(values[0])
            provider_set_key = int(values[1])
            provider_count = int(values[2])
            price_set_id = _normalize_128_id(values[3])
            if current_code != code_key:
                if current_code is not None:
                    blocks[-1]["count"] = current_block_count
                blocks.append({"key": code_key, "offset": body_offset, "count": 0})
                current_code = code_key
                current_block_count = 0
                previous_provider_set_key = 0
            price_set_key = price_set_to_key.get(price_set_id)
            if price_set_key is None:
                price_set_key = len(price_set_values)
                price_set_to_key[price_set_id] = price_set_key
                price_set_values.append(price_set_id)
            if provider_set_key < previous_provider_set_key:
                raise PTG2ManifestArtifactError("serving-by-code rows must be ordered by provider_set_key within code_key")
            before = body.tell()
            _write_uvarint(body, provider_set_key - previous_provider_set_key)
            _write_uvarint(body, provider_count)
            _write_uvarint(body, price_set_key)
            body_offset += body.tell() - before
            previous_provider_set_key = provider_set_key
            current_block_count += 1
            row_count += 1
    if blocks:
        blocks[-1]["count"] = current_block_count

    metadata = {
        "format": PTG2_SERVING_BY_CODE_FORMAT,
        "row_count": row_count,
        "code_count": len(blocks),
        "price_set_count": len(price_set_values),
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE,
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with tmp_path.open("wb") as out:
        out.write(PTG2_SERVING_BY_CODE_MAGIC)
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(price_set_id)
        for block in blocks:
            out.write(_SERVING_BLOCK_INDEX_RECORD.pack(block["key"], block["offset"], block["count"]))
        with body_path.open("rb") as body:
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                out.write(chunk)
    os.replace(tmp_path, sidecar_path)
    body_path.unlink(missing_ok=True)
    return _serving_sidecar_path_entry(
        name=name,
        path=sidecar_path,
        kind=PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
        record_format=PTG2_SERVING_BY_CODE_FORMAT,
        metadata=metadata,
    )


def write_serving_by_provider_set_sidecar(
    path: str | Path,
    rows: Iterable[Iterable[Any]],
    *,
    name: str = "serving_by_provider_set",
) -> dict[str, Any]:
    """Write compact serving rows grouped by provider-set key.

    Input rows must be ordered by ``provider_set_key, code_key, price_set_global_id_128``
    and shaped as ``provider_set_key, code_key, provider_count, price_set_global_id_128``.
    """

    sidecar_path = Path(path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    body_path = sidecar_path.with_name(f"{sidecar_path.name}.body.tmp")
    tmp_path = sidecar_path.with_name(f"{sidecar_path.name}.tmp")
    price_set_to_key: dict[bytes, int] = {}
    price_set_values: list[bytes] = []
    code_keys_seen: set[int] = set()
    blocks: list[dict[str, int]] = []
    row_count = 0
    body_offset = 0
    pattern_count = 0
    current_provider_set: int | None = None
    current_code: int | None = None
    current_code_entries: list[tuple[int, int]] = []
    current_patterns: dict[tuple[tuple[int, int], ...], list[int]] = {}

    def price_key_for(value: Any) -> int:
        price_set_id = _normalize_128_id(value)
        price_set_key = price_set_to_key.get(price_set_id)
        if price_set_key is None:
            price_set_key = len(price_set_values)
            price_set_to_key[price_set_id] = price_set_key
            price_set_values.append(price_set_id)
        return price_set_key

    def flush_code() -> None:
        nonlocal current_code_entries
        if current_code is None:
            return
        vector = tuple(current_code_entries)
        current_patterns.setdefault(vector, []).append(current_code)
        current_code_entries = []

    def write_provider_block(body: Any, provider_set_key: int) -> None:
        nonlocal body_offset, pattern_count
        flush_code()
        ordered_patterns = sorted(
            current_patterns.items(),
            key=lambda item: (item[1][0] if item[1] else -1, item[0]),
        )
        block_count = 0
        blocks.append({"key": provider_set_key, "offset": body_offset, "count": 0})
        for entries, code_key_list in ordered_patterns:
            code_keys = tuple(sorted(code_key_list))
            before = body.tell()
            _write_uvarint(body, len(code_keys))
            previous_code_key = 0
            for index, code_key in enumerate(code_keys):
                _write_uvarint(body, code_key if index == 0 else code_key - previous_code_key)
                previous_code_key = code_key
            _write_uvarint(body, len(entries))
            for provider_count, price_set_key in entries:
                _write_uvarint(body, provider_count)
                _write_uvarint(body, price_set_key)
            body_offset += body.tell() - before
            block_count += 1
            pattern_count += 1
        blocks[-1]["count"] = block_count
        current_patterns.clear()

    with body_path.open("wb") as body:
        for raw_row in rows:
            values = list(raw_row)
            if len(values) < 4:
                raise PTG2ManifestArtifactError("serving-by-provider-set rows must have at least four columns")
            provider_set_key = int(values[0])
            code_key = int(values[1])
            provider_count = int(values[2])
            price_set_key = price_key_for(values[3])
            if current_provider_set != provider_set_key:
                if current_provider_set is not None:
                    write_provider_block(body, current_provider_set)
                current_provider_set = provider_set_key
                current_code = None
            if current_code != code_key:
                flush_code()
                current_code = code_key
            current_code_entries.append((provider_count, price_set_key))
            code_keys_seen.add(code_key)
            row_count += 1
        if current_provider_set is not None:
            write_provider_block(body, current_provider_set)

    metadata = {
        "format": PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        "row_count": row_count,
        "provider_set_count": len(blocks),
        "code_count": len(code_keys_seen),
        "price_set_count": len(price_set_values),
        "pattern_count": pattern_count,
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE,
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with tmp_path.open("wb") as out:
        out.write(PTG2_SERVING_BY_PROVIDER_SET_MAGIC)
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(price_set_id)
        for block in blocks:
            out.write(_SERVING_BLOCK_INDEX_RECORD.pack(block["key"], block["offset"], block["count"]))
        with body_path.open("rb") as body:
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                out.write(chunk)
    os.replace(tmp_path, sidecar_path)
    body_path.unlink(missing_ok=True)
    return _serving_sidecar_path_entry(
        name=name,
        path=sidecar_path,
        kind=PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND,
        record_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        metadata=metadata,
    )


async def write_serving_by_code_sidecar_async(
    path: str | Path,
    rows: AsyncIterable[Iterable[Any]],
    *,
    name: str = "serving_by_code",
) -> dict[str, Any]:
    sidecar_path = Path(path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    body_path = sidecar_path.with_name(f"{sidecar_path.name}.body.tmp")
    tmp_path = sidecar_path.with_name(f"{sidecar_path.name}.tmp")
    price_set_to_key: dict[bytes, int] = {}
    price_set_values: list[bytes] = []
    blocks: list[dict[str, int]] = []
    row_count = 0
    body_offset = 0
    current_code: int | None = None
    current_block_count = 0
    previous_provider_set_key = 0

    with body_path.open("wb") as body:
        async for raw_row in rows:
            values = list(raw_row)
            if len(values) < 4:
                raise PTG2ManifestArtifactError("serving-by-code rows must have at least four columns")
            code_key = int(values[0])
            provider_set_key = int(values[1])
            provider_count = int(values[2])
            price_set_id = _normalize_128_id(values[3])
            if current_code != code_key:
                if current_code is not None:
                    blocks[-1]["count"] = current_block_count
                blocks.append({"key": code_key, "offset": body_offset, "count": 0})
                current_code = code_key
                current_block_count = 0
                previous_provider_set_key = 0
            price_set_key = price_set_to_key.get(price_set_id)
            if price_set_key is None:
                price_set_key = len(price_set_values)
                price_set_to_key[price_set_id] = price_set_key
                price_set_values.append(price_set_id)
            if provider_set_key < previous_provider_set_key:
                raise PTG2ManifestArtifactError("serving-by-code rows must be ordered by provider_set_key within code_key")
            before = body.tell()
            _write_uvarint(body, provider_set_key - previous_provider_set_key)
            _write_uvarint(body, provider_count)
            _write_uvarint(body, price_set_key)
            body_offset += body.tell() - before
            previous_provider_set_key = provider_set_key
            current_block_count += 1
            row_count += 1
    if blocks:
        blocks[-1]["count"] = current_block_count

    metadata = {
        "format": PTG2_SERVING_BY_CODE_FORMAT,
        "row_count": row_count,
        "code_count": len(blocks),
        "price_set_count": len(price_set_values),
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE,
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with tmp_path.open("wb") as out:
        out.write(PTG2_SERVING_BY_CODE_MAGIC)
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(price_set_id)
        for block in blocks:
            out.write(_SERVING_BLOCK_INDEX_RECORD.pack(block["key"], block["offset"], block["count"]))
        with body_path.open("rb") as body:
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                out.write(chunk)
    os.replace(tmp_path, sidecar_path)
    body_path.unlink(missing_ok=True)
    return _serving_sidecar_path_entry(
        name=name,
        path=sidecar_path,
        kind=PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
        record_format=PTG2_SERVING_BY_CODE_FORMAT,
        metadata=metadata,
    )


async def write_serving_by_provider_set_sidecar_async(
    path: str | Path,
    rows: AsyncIterable[Iterable[Any]],
    *,
    name: str = "serving_by_provider_set",
) -> dict[str, Any]:
    sidecar_path = Path(path)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    body_path = sidecar_path.with_name(f"{sidecar_path.name}.body.tmp")
    tmp_path = sidecar_path.with_name(f"{sidecar_path.name}.tmp")
    price_set_to_key: dict[bytes, int] = {}
    price_set_values: list[bytes] = []
    code_keys_seen: set[int] = set()
    blocks: list[dict[str, int]] = []
    row_count = 0
    body_offset = 0
    pattern_count = 0
    current_provider_set: int | None = None
    current_code: int | None = None
    current_code_entries: list[tuple[int, int]] = []
    current_patterns: dict[tuple[tuple[int, int], ...], list[int]] = {}

    def price_key_for(value: Any) -> int:
        price_set_id = _normalize_128_id(value)
        price_set_key = price_set_to_key.get(price_set_id)
        if price_set_key is None:
            price_set_key = len(price_set_values)
            price_set_to_key[price_set_id] = price_set_key
            price_set_values.append(price_set_id)
        return price_set_key

    def flush_code() -> None:
        nonlocal current_code_entries
        if current_code is None:
            return
        current_patterns.setdefault(tuple(current_code_entries), []).append(current_code)
        current_code_entries = []

    def write_provider_block(body: Any, provider_set_key: int) -> None:
        nonlocal body_offset, pattern_count
        flush_code()
        blocks.append({"key": provider_set_key, "offset": body_offset, "count": 0})
        block_count = 0
        for entries, code_key_list in sorted(
            current_patterns.items(),
            key=lambda item: (item[1][0] if item[1] else -1, item[0]),
        ):
            code_keys = tuple(sorted(code_key_list))
            before = body.tell()
            _write_uvarint(body, len(code_keys))
            previous_code_key = 0
            for index, code_key in enumerate(code_keys):
                _write_uvarint(body, code_key if index == 0 else code_key - previous_code_key)
                previous_code_key = code_key
            _write_uvarint(body, len(entries))
            for provider_count, price_set_key in entries:
                _write_uvarint(body, provider_count)
                _write_uvarint(body, price_set_key)
            body_offset += body.tell() - before
            block_count += 1
            pattern_count += 1
        blocks[-1]["count"] = block_count
        current_patterns.clear()

    with body_path.open("wb") as body:
        async for raw_row in rows:
            values = list(raw_row)
            if len(values) < 4:
                raise PTG2ManifestArtifactError("serving-by-provider-set rows must have at least four columns")
            provider_set_key = int(values[0])
            code_key = int(values[1])
            provider_count = int(values[2])
            price_set_key = price_key_for(values[3])
            if current_provider_set != provider_set_key:
                if current_provider_set is not None:
                    write_provider_block(body, current_provider_set)
                current_provider_set = provider_set_key
                current_code = None
            if current_code != code_key:
                flush_code()
                current_code = code_key
            current_code_entries.append((provider_count, price_set_key))
            code_keys_seen.add(code_key)
            row_count += 1
        if current_provider_set is not None:
            write_provider_block(body, current_provider_set)

    metadata = {
        "format": PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        "row_count": row_count,
        "provider_set_count": len(blocks),
        "code_count": len(code_keys_seen),
        "price_set_count": len(price_set_values),
        "pattern_count": pattern_count,
        "body_bytes": body_offset,
        "price_dictionary_bytes": len(price_set_values) * 16,
        "block_index_bytes": len(blocks) * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE,
    }
    header = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    with tmp_path.open("wb") as out:
        out.write(PTG2_SERVING_BY_PROVIDER_SET_MAGIC)
        out.write(struct.pack("<I", len(header)))
        out.write(header)
        for price_set_id in price_set_values:
            out.write(price_set_id)
        for block in blocks:
            out.write(_SERVING_BLOCK_INDEX_RECORD.pack(block["key"], block["offset"], block["count"]))
        with body_path.open("rb") as body:
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                out.write(chunk)
    os.replace(tmp_path, sidecar_path)
    body_path.unlink(missing_ok=True)
    return _serving_sidecar_path_entry(
        name=name,
        path=sidecar_path,
        kind=PTG2_SERVING_BY_PROVIDER_SET_ARTIFACT_KIND,
        record_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        metadata=metadata,
    )


def write_manifest(path: str | Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Write a deterministic PTG2 manifest JSON file and return it as a dict."""

    manifest_path = Path(path)
    payload = dict(manifest)
    _atomic_write_json(manifest_path, payload)
    return payload


def read_manifest(path: str | Path, *, validate_sidecars: bool = True) -> dict[str, Any]:
    """Read a PTG2 manifest JSON file.

    When ``validate_sidecars`` is true, every sidecar listed in the manifest is
    checked against its recorded ``sha256`` and ``byte_count`` before the
    manifest is returned.
    """

    manifest_path = Path(path)
    with open(manifest_path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if not isinstance(payload, dict):
        raise PTG2ManifestArtifactError("PTG2 manifest must be a JSON object")
    if validate_sidecars:
        _validate_sidecars(manifest_path.parent, payload)
    return payload


def write_global_local_id_mapping(
    directory: str | Path,
    name: str,
    mapping: Mapping[bytes | bytearray | memoryview | str, Iterable[int]],
) -> dict[str, Any]:
    """Write a global-id to local-id mapping artifact.

    The binary sidecar is a deterministic sequence of fixed-width 20-byte
    records.  Each record repeats the 16-byte global content id followed by one
    big-endian uint32 snapshot-local id.  Global ids and local ids are sorted;
    duplicate local ids for the same global id are collapsed.
    """

    artifact_dir = Path(directory)
    sidecar_name = f"{name}.global_local_ids.bin"
    manifest_name = f"{name}.manifest.json"
    sidecar_path = artifact_dir / sidecar_name
    manifest_path = artifact_dir / manifest_name
    canonical = _canonical_mapping(mapping)

    payload = bytearray()
    record_count = 0
    global_id_count = 0
    for global_id, local_ids in canonical:
        if not local_ids:
            continue
        global_id_count += 1
        for local_id in local_ids:
            payload.extend(_MAPPING_RECORD.pack(global_id, local_id))
            record_count += 1
    _atomic_write_bytes(sidecar_path, bytes(payload))
    sidecar_sha, sidecar_byte_count = _sha256_file(sidecar_path)

    manifest = {
        "version": PTG2_MANIFEST_VERSION,
        "artifact_type": PTG2_MANIFEST_MAPPING_ARTIFACT_TYPE,
        "name": name,
        "global_id_count": global_id_count,
        "record_count": record_count,
        "sidecars": [
            {
                "kind": "global_local_id_pairs",
                "path": sidecar_name,
                "record_format": PTG2_MANIFEST_MAPPING_RECORD_FORMAT,
                "record_size": PTG2_MANIFEST_MAPPING_RECORD_SIZE,
                "record_count": record_count,
                "sha256": sidecar_sha,
                "byte_count": sidecar_byte_count,
            }
        ],
    }
    write_manifest(manifest_path, manifest)
    return manifest


def read_global_local_id_mapping(manifest_path: str | Path) -> dict[bytes, tuple[int, ...]]:
    """Read and validate a global-id to local-id mapping artifact."""

    manifest = read_manifest(manifest_path, validate_sidecars=True)
    if manifest.get("version") != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 manifest version: {manifest.get('version')!r}")
    if manifest.get("artifact_type") != PTG2_MANIFEST_MAPPING_ARTIFACT_TYPE:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 artifact type: {manifest.get('artifact_type')!r}")

    sidecar = _mapping_sidecar(manifest)
    if int(sidecar.get("record_size") or 0) != PTG2_MANIFEST_MAPPING_RECORD_SIZE:
        raise PTG2ManifestArtifactError("global/local id sidecar has an unexpected record size")
    if sidecar.get("record_format") != PTG2_MANIFEST_MAPPING_RECORD_FORMAT:
        raise PTG2ManifestArtifactError("global/local id sidecar has an unexpected record format")

    sidecar_path = _sidecar_path(Path(manifest_path).parent, sidecar)
    byte_count = int(sidecar["byte_count"])
    if byte_count % PTG2_MANIFEST_MAPPING_RECORD_SIZE != 0:
        raise PTG2ManifestArtifactError("global/local id sidecar byte count is not record aligned")
    expected_records = int(sidecar.get("record_count") or 0)
    actual_records = byte_count // PTG2_MANIFEST_MAPPING_RECORD_SIZE
    if actual_records != expected_records:
        raise PTG2ManifestArtifactError(
            f"global/local id sidecar record count mismatch: expected {expected_records}, got {actual_records}"
        )

    result: dict[bytes, list[int]] = {}
    with open(sidecar_path, "rb") as fp:
        while chunk := fp.read(PTG2_MANIFEST_MAPPING_RECORD_SIZE):
            if len(chunk) != PTG2_MANIFEST_MAPPING_RECORD_SIZE:
                raise PTG2ManifestArtifactError("global/local id sidecar ended mid-record")
            global_id, local_id = _MAPPING_RECORD.unpack(chunk)
            result.setdefault(global_id, []).append(local_id)

    return {global_id: tuple(local_ids) for global_id, local_ids in sorted(result.items())}


def write_global_membership_sidecar(
    directory: str | Path,
    name: str,
    mapping: Mapping[bytes | bytearray | memoryview | str, Iterable[bytes | bytearray | memoryview | str]],
) -> dict[str, Any]:
    """Write a global-id membership sidecar.

    This is the Python counterpart to Rust ``write_global_sidecar``: a small
    fixed header, a sorted owner offset/count index, then a contiguous block of
    sorted 16-byte member ids.  It is intended for global provider-set
    membership, inverted provider membership, and price-set membership.
    """

    artifact_dir = Path(directory)
    sidecar_name = f"{name}.global_membership.bin"
    manifest_name = f"{name}.manifest.json"
    sidecar_path = artifact_dir / sidecar_name
    manifest_path = artifact_dir / manifest_name
    canonical = _canonical_membership(mapping)

    payload = bytearray()
    payload.extend(_MEMBERSHIP_HEADER.pack(PTG2_MANIFEST_MEMBERSHIP_MAGIC, PTG2_MANIFEST_VERSION, len(canonical)))
    member_offset = 0
    member_count = 0
    for owner_id, member_ids in canonical:
        payload.extend(_MEMBERSHIP_INDEX_RECORD.pack(owner_id, member_offset, len(member_ids)))
        member_offset += len(member_ids)
        member_count += len(member_ids)
    for _owner_id, member_ids in canonical:
        for member_id in member_ids:
            payload.extend(member_id)
    _atomic_write_bytes(sidecar_path, bytes(payload))
    sidecar_sha, sidecar_byte_count = _sha256_file(sidecar_path)

    manifest = {
        "version": PTG2_MANIFEST_VERSION,
        "artifact_type": PTG2_MANIFEST_MEMBERSHIP_ARTIFACT_TYPE,
        "name": name,
        "owner_count": len(canonical),
        "member_count": member_count,
        "sidecars": [
            {
                "kind": "global_membership",
                "path": sidecar_name,
                "record_format": PTG2_MANIFEST_MEMBERSHIP_FORMAT,
                "index_record_size": PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE,
                "owner_count": len(canonical),
                "member_count": member_count,
                "sha256": sidecar_sha,
                "byte_count": sidecar_byte_count,
            }
        ],
    }
    write_manifest(manifest_path, manifest)
    return manifest


def read_global_membership_sidecar(manifest_path: str | Path) -> dict[bytes, tuple[bytes, ...]]:
    """Read and validate a global-id membership sidecar."""

    manifest = read_manifest(manifest_path, validate_sidecars=True)
    if manifest.get("version") != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 manifest version: {manifest.get('version')!r}")
    if manifest.get("artifact_type") != PTG2_MANIFEST_MEMBERSHIP_ARTIFACT_TYPE:
        raise PTG2ManifestArtifactError(f"unsupported PTG2 artifact type: {manifest.get('artifact_type')!r}")
    sidecar = _membership_sidecar(manifest)
    if sidecar.get("record_format") != PTG2_MANIFEST_MEMBERSHIP_FORMAT:
        raise PTG2ManifestArtifactError("global membership sidecar has an unexpected record format")

    entries = read_global_sidecar_entries(_sidecar_path(Path(manifest_path).parent, sidecar), metadata=sidecar)
    return {entry.owner: entry.members for entry in entries}


def read_global_sidecar_entries(
    path: str | Path,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[PTG2ManifestSidecarEntry, ...]:
    """Read a Rust PTG2 ``write_global_sidecar`` binary file.

    ``metadata`` may be a manifest sidecar entry; when supplied, checksum,
    byte-count, entry-count, member-count, and record-format values are
    validated before entries are returned.
    """

    sidecar_path = Path(path)
    if metadata is not None:
        _validate_sidecar_metadata(sidecar_path, metadata)
        _validate_membership_record_format(metadata)
    payload = sidecar_path.read_bytes()
    if len(payload) < 8:
        raise PTG2ManifestArtifactError("global membership sidecar is missing its header")
    magic = bytes(payload[:8])
    if _is_dense_membership_magic(magic):
        return _read_dense_sidecar_entries(payload, metadata=metadata)
    if not _is_standard_membership_magic(magic):
        raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
    header_size = _MEMBERSHIP_HEADER.size
    magic, version, entry_count = _MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported global membership sidecar version: {version!r}")
    if metadata is not None:
        expected_entries = metadata.get("entry_count", metadata.get("owner_count"))
        if expected_entries is not None and entry_count != int(expected_entries):
            raise PTG2ManifestArtifactError("global membership sidecar entry count mismatch")

    index_start = header_size
    index_end = index_start + entry_count * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
    if len(payload) < index_end:
        raise PTG2ManifestArtifactError("global membership sidecar ended inside the owner index")
    member_start = index_end
    result: list[PTG2ManifestSidecarEntry] = []
    total_members = 0
    previous_owner: bytes | None = None
    for index in range(entry_count):
        record_offset = index_start + index * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
        owner_id, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(payload, record_offset)
        if previous_owner is not None and owner_id <= previous_owner:
            raise PTG2ManifestArtifactError("global membership sidecar owners must be sorted and unique")
        previous_owner = owner_id
        total_members += member_count
        members: list[bytes] = []
        start = member_start + member_offset * 16
        end = start + member_count * 16
        if end > len(payload):
            raise PTG2ManifestArtifactError("global membership sidecar member block is truncated")
        for member_pos in range(start, end, 16):
            members.append(payload[member_pos : member_pos + 16])
        if tuple(members) != tuple(sorted(set(members))):
            raise PTG2ManifestArtifactError("global membership sidecar members must be sorted and unique")
        result.append(PTG2ManifestSidecarEntry(owner=owner_id, members=tuple(members)))
    if metadata is not None:
        expected_members = metadata.get("member_count")
        if expected_members is not None and total_members != int(expected_members):
            raise PTG2ManifestArtifactError("global membership sidecar member count mismatch")
    expected_size = member_start + total_members * 16
    if len(payload) != expected_size:
        raise PTG2ManifestArtifactError("global membership sidecar has trailing bytes")
    return tuple(result)


def lookup_global_sidecar_members(
    path: str | Path,
    owner: bytes | bytearray | memoryview | str,
    *,
    metadata: Mapping[str, Any] | None = None,
    max_members: int | None = None,
) -> tuple[bytes, ...]:
    """Return one owner's members from a Rust PTG2 global sidecar.

    This is the hot API lookup primitive.  It validates cheap structural
    metadata and binary-searches the owner index instead of materializing the
    whole sidecar.
    """

    sidecar_path = Path(path)
    owner_id = _normalize_global_id(owner)
    if metadata is not None:
        _validate_membership_record_format(metadata)
        expected_byte_count = metadata.get("byte_count")
        if not isinstance(expected_byte_count, int) or expected_byte_count < 0:
            raise PTG2ManifestArtifactError("PTG2 sidecar is missing a non-negative byte count")
        actual_byte_count = sidecar_path.stat().st_size
        if actual_byte_count != expected_byte_count:
            raise PTG2ManifestArtifactError(
                f"PTG2 sidecar byte_count mismatch for {sidecar_path.name}: "
                f"expected {expected_byte_count}, got {actual_byte_count}"
            )
    with open(sidecar_path, "rb") as fp:
        with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as payload:
            header_size = _MEMBERSHIP_HEADER.size
            if len(payload) < header_size:
                raise PTG2ManifestArtifactError("global membership sidecar is missing its header")
            magic = bytes(payload[:8])
            if _is_dense_membership_magic(magic):
                return _lookup_dense_sidecar_members(payload, owner_id, metadata=metadata, max_members=max_members)
            if not _is_standard_membership_magic(magic):
                raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
            magic, version, entry_count = _MEMBERSHIP_HEADER.unpack_from(payload, 0)
            if version != PTG2_MANIFEST_VERSION:
                raise PTG2ManifestArtifactError(f"unsupported global membership sidecar version: {version!r}")
            expected_entries = metadata.get("entry_count", metadata.get("owner_count")) if metadata is not None else None
            if expected_entries is not None and entry_count != int(expected_entries):
                raise PTG2ManifestArtifactError("global membership sidecar entry count mismatch")
            index_start = header_size
            index_end = index_start + entry_count * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
            if len(payload) < index_end:
                raise PTG2ManifestArtifactError("global membership sidecar ended inside the owner index")
            member_start = index_end
            low = 0
            high = int(entry_count) - 1
            while low <= high:
                mid = (low + high) // 2
                record_offset = index_start + mid * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
                candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(
                    payload,
                    record_offset,
                )
                if candidate_owner < owner_id:
                    low = mid + 1
                    continue
                if candidate_owner > owner_id:
                    high = mid - 1
                    continue
                start = member_start + member_offset * 16
                end = start + member_count * 16
                if end > len(payload):
                    raise PTG2ManifestArtifactError("global membership sidecar member block is truncated")
                if max_members is not None:
                    end = min(end, start + max(max_members, 0) * 16)
                return tuple(bytes(payload[pos : pos + 16]) for pos in range(start, end, 16))
            return ()


def _lookup_standard_sidecar_members(
    payload: mmap.mmap,
    owner_id: bytes,
    *,
    metadata: Mapping[str, Any] | None = None,
    max_members: int | None = None,
) -> tuple[bytes, ...]:
    header_size = _MEMBERSHIP_HEADER.size
    magic, version, entry_count = _MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if not _is_standard_membership_magic(magic):
        raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported global membership sidecar version: {version!r}")
    expected_entries = metadata.get("entry_count", metadata.get("owner_count")) if metadata is not None else None
    if expected_entries is not None and entry_count != int(expected_entries):
        raise PTG2ManifestArtifactError("global membership sidecar entry count mismatch")
    index_start = header_size
    index_end = index_start + entry_count * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
    if len(payload) < index_end:
        raise PTG2ManifestArtifactError("global membership sidecar ended inside the owner index")
    member_start = index_end
    low = 0
    high = int(entry_count) - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = index_start + mid * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
        candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(
            payload,
            record_offset,
        )
        if candidate_owner < owner_id:
            low = mid + 1
            continue
        if candidate_owner > owner_id:
            high = mid - 1
            continue
        start = member_start + member_offset * 16
        end = start + member_count * 16
        if end > len(payload):
            raise PTG2ManifestArtifactError("global membership sidecar member block is truncated")
        if max_members is not None:
            end = min(end, start + max(max_members, 0) * 16)
        return tuple(bytes(payload[pos : pos + 16]) for pos in range(start, end, 16))
    return ()


def lookup_global_sidecar_members_many(
    path: str | Path,
    owners: Iterable[bytes | bytearray | memoryview | str],
    *,
    metadata: Mapping[str, Any] | None = None,
    max_members: int | None = None,
) -> dict[bytes, tuple[bytes, ...]]:
    """Return multiple owner memberships while opening and mapping the sidecar once."""

    owner_ids = tuple(dict.fromkeys(_normalize_global_id(owner) for owner in owners))
    if not owner_ids:
        return {}
    sidecar_path = Path(path)
    if metadata is not None:
        _validate_membership_record_format(metadata)
    if _sidecar_mmap_cache_enabled():
        payload = _cached_sidecar_mmap(sidecar_path, metadata=metadata)
        if len(payload) < _MEMBERSHIP_HEADER.size:
            raise PTG2ManifestArtifactError("global membership sidecar is missing its header")
        magic = bytes(payload[:8])
        if _is_dense_membership_magic(magic):
            return {
                owner_id: _lookup_dense_sidecar_members(payload, owner_id, metadata=metadata, max_members=max_members)
                for owner_id in owner_ids
            }
        if not _is_standard_membership_magic(magic):
            raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
        return {
            owner_id: _lookup_standard_sidecar_members(payload, owner_id, metadata=metadata, max_members=max_members)
            for owner_id in owner_ids
        }
    with open(sidecar_path, "rb") as fp:
        with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as payload:
            if len(payload) < _MEMBERSHIP_HEADER.size:
                raise PTG2ManifestArtifactError("global membership sidecar is missing its header")
            magic = bytes(payload[:8])
            if _is_dense_membership_magic(magic):
                return {
                    owner_id: _lookup_dense_sidecar_members(payload, owner_id, metadata=metadata, max_members=max_members)
                    for owner_id in owner_ids
                }
            if not _is_standard_membership_magic(magic):
                raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
            return {
                owner_id: _lookup_standard_sidecar_members(payload, owner_id, metadata=metadata, max_members=max_members)
                for owner_id in owner_ids
            }


def _lookup_serving_block(
    payload: bytes | bytearray | memoryview | mmap.mmap,
    *,
    key: int,
    magic: bytes,
    expected_format: str,
    block_count_field: str,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], int, int, int, int, int]:
    header, header_end = _serving_sidecar_header(payload, magic, expected_format, metadata)
    price_count = int(header.get("price_set_count") or 0)
    block_count = int(header.get(block_count_field) or 0)
    price_start = header_end
    index_start = price_start + price_count * 16
    body_start = index_start + block_count * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE
    if body_start > len(payload):
        raise PTG2ManifestArtifactError("serving sidecar index is truncated")
    low = 0
    high = block_count - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = index_start + mid * PTG2_SERVING_BLOCK_INDEX_RECORD_SIZE
        candidate_key, body_offset, row_count = _SERVING_BLOCK_INDEX_RECORD.unpack_from(payload, record_offset)
        if candidate_key < key:
            low = mid + 1
            continue
        if candidate_key > key:
            high = mid - 1
            continue
        return header, price_start, body_start, int(body_offset), int(row_count), price_count
    return header, price_start, body_start, 0, 0, price_count


def _serving_sidecar_payload(path: str | Path, metadata: Mapping[str, Any] | None) -> mmap.mmap:
    sidecar_path = Path(path)
    if metadata is not None:
        expected_byte_count = metadata.get("byte_count")
        if not isinstance(expected_byte_count, int) or expected_byte_count < 0:
            raise PTG2ManifestArtifactError("PTG2 serving sidecar is missing a non-negative byte count")
    return _cached_sidecar_mmap(sidecar_path, metadata=metadata)


def lookup_serving_by_code_sidecar(
    path: str | Path,
    code_key: int,
    *,
    provider_set_keys: Iterable[int] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    """Return compact serving rows for one code key from a serving-by-code sidecar."""

    payload = _serving_sidecar_payload(path, metadata)
    _header, price_start, body_start, body_offset, row_count, price_count = _lookup_serving_block(
        payload,
        key=int(code_key),
        magic=PTG2_SERVING_BY_CODE_MAGIC,
        expected_format=PTG2_SERVING_BY_CODE_FORMAT,
        block_count_field="code_count",
        metadata=metadata,
    )
    if row_count <= 0:
        return ()
    provider_filter = {int(value) for value in provider_set_keys} if provider_set_keys is not None else None
    provider_filter_max = max(provider_filter) if provider_filter else None
    cursor = body_start + body_offset
    provider_set_key = 0
    rows: list[PTG2ServingSidecarRow] = []
    for _ in range(row_count):
        provider_delta, cursor = _read_uvarint(payload, cursor)
        provider_count, cursor = _read_uvarint(payload, cursor)
        price_key, cursor = _read_uvarint(payload, cursor)
        if price_key >= price_count:
            raise PTG2ManifestArtifactError("serving-by-code sidecar price key is out of range")
        provider_set_key += provider_delta
        if provider_filter is not None and provider_set_key not in provider_filter:
            if provider_filter_max is not None and provider_set_key > provider_filter_max:
                break
            continue
        price_offset = price_start + price_key * 16
        rows.append(
            PTG2ServingSidecarRow(
                code_key=int(code_key),
                provider_set_key=provider_set_key,
                provider_count=provider_count,
                price_set_global_id_128=_id_text(payload[price_offset : price_offset + 16]),
            )
        )
    return tuple(rows)


def lookup_serving_by_provider_set_sidecar(
    path: str | Path,
    provider_set_key: int,
    *,
    code_keys: Iterable[int] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[PTG2ServingSidecarRow, ...]:
    """Return compact serving rows for one provider-set key from a reverse sidecar."""

    payload = _serving_sidecar_payload(path, metadata)
    _header, price_start, body_start, body_offset, pattern_count, price_count = _lookup_serving_block(
        payload,
        key=int(provider_set_key),
        magic=PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        expected_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        block_count_field="provider_set_count",
        metadata=metadata,
    )
    if pattern_count <= 0:
        return ()
    code_filter = {int(value) for value in code_keys} if code_keys is not None else None
    cursor = body_start + body_offset
    rows: list[PTG2ServingSidecarRow] = []
    for _ in range(pattern_count):
        code_count, cursor = _read_uvarint(payload, cursor)
        decoded_code_keys: list[int] = []
        previous_code_key = 0
        for index in range(code_count):
            encoded_code_key, cursor = _read_uvarint(payload, cursor)
            code_key = encoded_code_key if index == 0 else previous_code_key + encoded_code_key
            decoded_code_keys.append(code_key)
            previous_code_key = code_key
        entry_count, cursor = _read_uvarint(payload, cursor)
        entries: list[tuple[int, int]] = []
        for _entry_index in range(entry_count):
            provider_count, cursor = _read_uvarint(payload, cursor)
            price_key, cursor = _read_uvarint(payload, cursor)
            if price_key >= price_count:
                raise PTG2ManifestArtifactError("serving-by-provider-set sidecar price key is out of range")
            entries.append((provider_count, price_key))
        for code_key in decoded_code_keys:
            if code_filter is not None and code_key not in code_filter:
                continue
            for provider_count, price_key in entries:
                price_offset = price_start + price_key * 16
                rows.append(
                    PTG2ServingSidecarRow(
                        code_key=code_key,
                        provider_set_key=int(provider_set_key),
                        provider_count=provider_count,
                        price_set_global_id_128=_id_text(payload[price_offset : price_offset + 16]),
                    )
                )
    return tuple(rows)


def lookup_serving_by_provider_set_patterns(
    path: str | Path,
    provider_set_key: int,
    *,
    code_keys: Iterable[int] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[PTG2ServingProviderSetPattern, ...]:
    """Return compact code-vector patterns for one provider-set key."""

    payload = _serving_sidecar_payload(path, metadata)
    _header, price_start, body_start, body_offset, pattern_count, price_count = _lookup_serving_block(
        payload,
        key=int(provider_set_key),
        magic=PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        expected_format=PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        block_count_field="provider_set_count",
        metadata=metadata,
    )
    if pattern_count <= 0:
        return ()
    code_filter = {int(value) for value in code_keys} if code_keys is not None else None
    cursor = body_start + body_offset
    patterns: list[PTG2ServingProviderSetPattern] = []
    for _ in range(pattern_count):
        code_count, cursor = _read_uvarint(payload, cursor)
        decoded_code_keys: list[int] = []
        previous_code_key = 0
        for index in range(code_count):
            encoded_code_key, cursor = _read_uvarint(payload, cursor)
            code_key = encoded_code_key if index == 0 else previous_code_key + encoded_code_key
            if code_filter is None or code_key in code_filter:
                decoded_code_keys.append(code_key)
            previous_code_key = code_key
        entry_count, cursor = _read_uvarint(payload, cursor)
        entries: list[tuple[int, str]] = []
        for _entry_index in range(entry_count):
            provider_count, cursor = _read_uvarint(payload, cursor)
            price_key, cursor = _read_uvarint(payload, cursor)
            if price_key >= price_count:
                raise PTG2ManifestArtifactError("serving-by-provider-set sidecar price key is out of range")
            if decoded_code_keys:
                price_offset = price_start + price_key * 16
                entries.append((provider_count, _id_text(payload[price_offset : price_offset + 16])))
        if decoded_code_keys and entries:
            patterns.append(
                PTG2ServingProviderSetPattern(
                    code_keys=tuple(decoded_code_keys),
                    entries=tuple(entries),
                )
            )
    return tuple(patterns)


def _read_dense_sidecar_entries(
    payload: bytes | bytearray | mmap.mmap,
    *,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[PTG2ManifestSidecarEntry, ...]:
    if len(payload) < _DENSE_MEMBERSHIP_HEADER.size:
        raise PTG2ManifestArtifactError("dense global membership sidecar is missing its header")
    magic, version, entry_count, member_global_count = _DENSE_MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if not _is_dense_membership_magic(magic):
        raise PTG2ManifestArtifactError("dense global membership sidecar has an invalid magic header")
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported dense global membership sidecar version: {version!r}")
    if metadata is not None:
        expected_entries = metadata.get("entry_count", metadata.get("owner_count"))
        if expected_entries is not None and entry_count != int(expected_entries):
            raise PTG2ManifestArtifactError("dense global membership sidecar entry count mismatch")
        expected_member_globals = metadata.get("member_global_count")
        if expected_member_globals is not None and member_global_count != int(expected_member_globals):
            raise PTG2ManifestArtifactError("dense global membership sidecar member dictionary count mismatch")

    index_start = _DENSE_MEMBERSHIP_HEADER.size
    index_end = index_start + entry_count * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
    globals_start = index_end
    globals_end = globals_start + member_global_count * 16
    if len(payload) < globals_end:
        raise PTG2ManifestArtifactError("dense global membership sidecar ended inside the dictionary")
    members_start = globals_end

    member_globals = [bytes(payload[pos : pos + 16]) for pos in range(globals_start, globals_end, 16)]
    result: list[PTG2ManifestSidecarEntry] = []
    total_members = 0
    previous_owner: bytes | None = None
    for index in range(entry_count):
        record_offset = index_start + index * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
        owner_id, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(payload, record_offset)
        if previous_owner is not None and owner_id <= previous_owner:
            raise PTG2ManifestArtifactError("dense global membership sidecar owners must be sorted and unique")
        previous_owner = owner_id
        total_members += member_count
        start = members_start + member_offset * _DENSE_MEMBER_RECORD.size
        end = start + member_count * _DENSE_MEMBER_RECORD.size
        if end > len(payload):
            raise PTG2ManifestArtifactError("dense global membership sidecar member block is truncated")
        members: list[bytes] = []
        for pos in range(start, end, _DENSE_MEMBER_RECORD.size):
            local_id = _DENSE_MEMBER_RECORD.unpack_from(payload, pos)[0]
            if local_id >= member_global_count:
                raise PTG2ManifestArtifactError("dense global membership sidecar member id is out of range")
            members.append(member_globals[local_id])
        if tuple(members) != tuple(sorted(set(members))):
            raise PTG2ManifestArtifactError("dense global membership sidecar members must be sorted and unique")
        result.append(PTG2ManifestSidecarEntry(owner=owner_id, members=tuple(members)))
    if metadata is not None:
        expected_members = metadata.get("member_count")
        if expected_members is not None and total_members != int(expected_members):
            raise PTG2ManifestArtifactError("dense global membership sidecar member count mismatch")
    expected_size = members_start + total_members * _DENSE_MEMBER_RECORD.size
    if len(payload) != expected_size:
        raise PTG2ManifestArtifactError("dense global membership sidecar has trailing bytes")
    return tuple(result)


def _lookup_dense_sidecar_members(
    payload: mmap.mmap,
    owner_id: bytes,
    *,
    metadata: Mapping[str, Any] | None = None,
    max_members: int | None = None,
) -> tuple[bytes, ...]:
    if len(payload) < _DENSE_MEMBERSHIP_HEADER.size:
        raise PTG2ManifestArtifactError("dense global membership sidecar is missing its header")
    magic, version, entry_count, member_global_count = _DENSE_MEMBERSHIP_HEADER.unpack_from(payload, 0)
    if not _is_dense_membership_magic(magic):
        raise PTG2ManifestArtifactError("dense global membership sidecar has an invalid magic header")
    if version != PTG2_MANIFEST_VERSION:
        raise PTG2ManifestArtifactError(f"unsupported dense global membership sidecar version: {version!r}")
    expected_entries = metadata.get("entry_count", metadata.get("owner_count")) if metadata is not None else None
    if expected_entries is not None and entry_count != int(expected_entries):
        raise PTG2ManifestArtifactError("dense global membership sidecar entry count mismatch")
    expected_member_globals = metadata.get("member_global_count") if metadata is not None else None
    if expected_member_globals is not None and member_global_count != int(expected_member_globals):
        raise PTG2ManifestArtifactError("dense global membership sidecar member dictionary count mismatch")

    index_start = _DENSE_MEMBERSHIP_HEADER.size
    index_end = index_start + entry_count * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
    globals_start = index_end
    globals_end = globals_start + member_global_count * 16
    if len(payload) < globals_end:
        raise PTG2ManifestArtifactError("dense global membership sidecar ended inside the dictionary")
    members_start = globals_end
    low = 0
    high = int(entry_count) - 1
    while low <= high:
        mid = (low + high) // 2
        record_offset = index_start + mid * PTG2_MANIFEST_MEMBERSHIP_INDEX_RECORD_SIZE
        candidate_owner, member_offset, member_count = _MEMBERSHIP_INDEX_RECORD.unpack_from(payload, record_offset)
        if candidate_owner < owner_id:
            low = mid + 1
            continue
        if candidate_owner > owner_id:
            high = mid - 1
            continue
        start = members_start + member_offset * _DENSE_MEMBER_RECORD.size
        end = start + member_count * _DENSE_MEMBER_RECORD.size
        if end > len(payload):
            raise PTG2ManifestArtifactError("dense global membership sidecar member block is truncated")
        if max_members is not None:
            end = min(end, start + max(max_members, 0) * _DENSE_MEMBER_RECORD.size)
        members: list[bytes] = []
        for pos in range(start, end, _DENSE_MEMBER_RECORD.size):
            local_id = _DENSE_MEMBER_RECORD.unpack_from(payload, pos)[0]
            if local_id >= member_global_count:
                raise PTG2ManifestArtifactError("dense global membership sidecar member id is out of range")
            global_pos = globals_start + local_id * 16
            members.append(bytes(payload[global_pos : global_pos + 16]))
        return tuple(members)
    return ()


def _mapping_sidecar(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    for sidecar in manifest.get("sidecars") or []:
        if isinstance(sidecar, dict) and sidecar.get("kind") == "global_local_id_pairs":
            return sidecar
    raise PTG2ManifestArtifactError("PTG2 manifest does not include a global/local id sidecar")


def _membership_sidecar(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    for sidecar in manifest.get("sidecars") or []:
        if isinstance(sidecar, dict) and sidecar.get("kind") == "global_membership":
            return sidecar
    raise PTG2ManifestArtifactError("PTG2 manifest does not include a global membership sidecar")


def _sidecar_path(manifest_dir: Path, sidecar: Mapping[str, Any]) -> Path:
    raw_path = sidecar.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        raise PTG2ManifestArtifactError("PTG2 sidecar is missing a relative path")
    path = Path(raw_path)
    if path.is_absolute() or ".." in path.parts:
        raise PTG2ManifestArtifactError("PTG2 sidecar paths must stay under the manifest directory")
    return manifest_dir / path


def _validate_sidecars(manifest_dir: Path, manifest: Mapping[str, Any]) -> None:
    for sidecar in manifest.get("sidecars") or []:
        if not isinstance(sidecar, dict):
            raise PTG2ManifestArtifactError("PTG2 sidecar entries must be JSON objects")
        _validate_sidecar_metadata(_sidecar_path(manifest_dir, sidecar), sidecar)


def _validate_sidecar_metadata(sidecar_path: Path, sidecar: Mapping[str, Any]) -> None:
    expected_sha = sidecar.get("sha256")
    expected_byte_count = sidecar.get("byte_count")
    if not isinstance(expected_sha, str) or len(expected_sha) != 64:
        raise PTG2ManifestArtifactError("PTG2 sidecar is missing a SHA-256 checksum")
    if not isinstance(expected_byte_count, int) or expected_byte_count < 0:
        raise PTG2ManifestArtifactError("PTG2 sidecar is missing a non-negative byte count")
    actual_sha, actual_byte_count = _sha256_file(sidecar_path)
    if actual_byte_count != expected_byte_count:
        raise PTG2ManifestArtifactError(
            f"PTG2 sidecar byte_count mismatch for {sidecar_path.name}: "
            f"expected {expected_byte_count}, got {actual_byte_count}"
        )
    if actual_sha != expected_sha:
        raise PTG2ManifestArtifactError(f"PTG2 sidecar checksum mismatch for {sidecar_path.name}")
