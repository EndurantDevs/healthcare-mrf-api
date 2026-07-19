# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache-free reads from the strict PTG V3 shared PostgreSQL layout."""

from __future__ import annotations

import asyncio
import json
import re
import zlib
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable, Iterator, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V3_SHARED_FORMAT_VERSION,
    shared_block_hash,
)
from process.ptg_parts.ptg2_shared_reuse import shared_source_set_metadata
from process.ptg_parts.ptg2_shared_source_set import (
    ordered_source_ordinal_digest,
)


PTG2_V3_GRAPH_CHUNK_BYTES = 64 * 1024
PTG2_V3_GRAPH_NPI_TO_GROUP = 1
PTG2_V3_GRAPH_GROUP_TO_NPI = 2
PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET = 3
PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP = 4
PTG2_V3_MAX_AUDIT_SOURCE_FILES = 100_000

_GRAPH_KIND_AND_WIDTH = {
    PTG2_V3_GRAPH_NPI_TO_GROUP: ("graph_npi_groups_v1", 4),
    PTG2_V3_GRAPH_GROUP_TO_NPI: ("graph_group_npis_v1", 8),
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET: ("graph_group_provider_sets_v1", 4),
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP: ("graph_provider_set_groups_v1", 4),
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PTG2SharedBlockError(RuntimeError):
    pass


def read_strict_uvarint(
    payload: bytes | bytearray | memoryview,
    offset: int,
) -> tuple[int, int]:
    """Read one canonical unsigned 64-bit varint."""

    cursor = int(offset)
    start = cursor
    value = 0
    shift = 0
    while True:
        if cursor >= len(payload):
            raise PTG2SharedBlockError("shared PTG payload ended inside a uvarint")
        byte = int(payload[cursor])
        cursor += 1
        if shift == 63 and byte > 1:
            raise PTG2SharedBlockError("shared PTG payload uvarint exceeds uint64")
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            if cursor - start > 1 and byte == 0:
                raise PTG2SharedBlockError(
                    "shared PTG payload contains a non-canonical uvarint"
                )
            return value, cursor
        shift += 7
        if shift > 63:
            raise PTG2SharedBlockError("shared PTG payload uvarint exceeds uint64")


def dense_source_key_bits(source_count: int) -> int:
    """Validate a source count and return its minimum dense-key bit width."""

    normalized_count = int(source_count)
    if normalized_count < 1 or normalized_count > 2**31:
        raise PTG2SharedBlockError("shared PTG source_count must be in 1..=2^31")
    return 0 if normalized_count == 1 else (normalized_count - 1).bit_length()


def decode_dense_source_header(
    payload: bytes | bytearray | memoryview,
    offset: int,
    *,
    format_version: int,
    expected_source_count: int | None = None,
) -> tuple[int, int, int]:
    """Validate a dense-source header and return its count, width, and next offset."""

    cursor = int(offset)
    if cursor >= len(payload) or int(payload[cursor]) != int(format_version):
        raise PTG2SharedBlockError(
            "shared PTG payload has an unsupported format version"
        )
    source_count, cursor = read_strict_uvarint(payload, cursor + 1)
    if cursor >= len(payload):
        raise PTG2SharedBlockError("shared PTG payload is missing source_bits")
    source_bits = int(payload[cursor])
    cursor += 1
    if source_bits != dense_source_key_bits(source_count):
        raise PTG2SharedBlockError(
            "shared PTG payload source_bits does not match source_count"
        )
    if expected_source_count is not None and source_count != int(expected_source_count):
        raise PTG2SharedBlockError(
            "shared PTG payload source_count does not match the snapshot manifest"
        )
    return source_count, source_bits, cursor


def decode_dense_source_vector(
    encoded_payload: bytes | bytearray | memoryview,
    offset: int,
    *,
    entry_count: int,
    source_count: int,
    source_bits: int,
) -> tuple[tuple[int, ...], int]:
    """Decode validated dense source keys and return them with the next offset."""

    normalized_entry_count = int(entry_count)
    if normalized_entry_count < 0:
        raise PTG2SharedBlockError("shared PTG source vector count is negative")
    if int(source_bits) != dense_source_key_bits(source_count):
        raise PTG2SharedBlockError("shared PTG source vector width is invalid")
    total_bits = normalized_entry_count * int(source_bits)
    byte_count = (total_bits + 7) // 8
    cursor = int(offset)
    end = cursor + byte_count
    if end > len(encoded_payload):
        raise PTG2SharedBlockError("shared PTG source vector is truncated")
    encoded = encoded_payload[cursor:end]
    if total_bits % 8 and encoded and int(encoded[-1]) >> (total_bits % 8):
        raise PTG2SharedBlockError(
            "shared PTG source vector has non-zero padding bits"
        )
    source_keys: list[int] = []
    bit_offset = 0
    for _ in range(normalized_entry_count):
        source_key = 0
        for source_bit in range(int(source_bits)):
            if int(encoded[bit_offset // 8]) & (1 << (bit_offset % 8)):
                source_key |= 1 << source_bit
            bit_offset += 1
        if source_key >= int(source_count):
            raise PTG2SharedBlockError(
                "shared PTG source vector contains an out-of-range key"
            )
        source_keys.append(source_key)
    return tuple(source_keys), end


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def decode_shared_block_payload(
    *,
    codec: str,
    encoded_payload: bytes,
    raw_byte_count: int,
    maximum_raw_bytes: int | None = None,
) -> bytes:
    """Decode one independently compressed block without retaining it globally."""

    expected_raw_bytes = int(raw_byte_count)
    if expected_raw_bytes < 0 or (
        maximum_raw_bytes is not None
        and expected_raw_bytes > int(maximum_raw_bytes)
    ):
        raise PTG2SharedBlockError("shared PTG raw block exceeds its byte limit")
    normalized_codec = str(codec or "").strip().lower()
    if normalized_codec == "none":
        raw_payload = bytes(encoded_payload)
    elif normalized_codec == "zlib":
        try:
            decompressor = zlib.decompressobj()
            raw_payload = decompressor.decompress(
                bytes(encoded_payload),
                expected_raw_bytes + 1,
            )
        except zlib.error as exc:
            raise PTG2SharedBlockError(f"invalid shared PTG zlib block: {exc}") from exc
        if (
            not decompressor.eof
            or decompressor.unused_data
            or decompressor.unconsumed_tail
            or len(raw_payload) > expected_raw_bytes
        ):
            raise PTG2SharedBlockError(
                "invalid shared PTG zlib block framing or trailing bytes"
            )
    else:
        raise PTG2SharedBlockError(f"unsupported shared PTG block codec: {codec!r}")
    if len(raw_payload) != expected_raw_bytes:
        raise PTG2SharedBlockError(
            f"shared PTG raw block length mismatch: expected {expected_raw_bytes}, got {len(raw_payload)}"
        )
    return raw_payload


@dataclass(frozen=True)
class SharedBlockPayload:
    block_key: int
    fragment_no: int
    entry_count: int
    payload: bytes
    block_hash: bytes = b""


@dataclass(frozen=True)
class _SharedPhysicalBlock:
    block_hash: bytes
    object_kind: str
    entry_count: int | None
    payload: bytes


def _validated_physical_block(
    block_row: Mapping[str, Any],
    *,
    expected_kind: str,
    maximum_raw_bytes: int | None = None,
) -> _SharedPhysicalBlock:
    object_kind = str(block_row.get("object_kind") or "")
    codec = str(block_row.get("codec") or "")
    format_version = int(block_row.get("format_version") or 0)
    stored_payload = bytes(block_row.get("payload") or b"")
    stored_byte_value = block_row.get("stored_byte_count")
    block_hash = bytes(block_row.get("block_hash") or b"")
    if format_version != PTG2_V3_SHARED_FORMAT_VERSION:
        raise PTG2SharedBlockError(
            "shared PTG block has an unsupported format version"
        )
    try:
        expected_hash = shared_block_hash(
            format_version=format_version,
            object_kind=object_kind,
            codec=codec,
            payload=stored_payload,
        )
    except ValueError as exc:
        raise PTG2SharedBlockError(str(exc)) from exc
    if (
        object_kind != expected_kind
        or block_hash != expected_hash
        or (
            stored_byte_value is not None
            and int(stored_byte_value) != len(stored_payload)
        )
    ):
        raise PTG2SharedBlockError("shared PTG block identity validation failed")
    block_entry_value = block_row.get("block_entry_count")
    block_entry_count = (
        None if block_entry_value is None else int(block_entry_value)
    )
    if block_entry_count is not None and block_entry_count < 0:
        raise PTG2SharedBlockError("shared PTG block entry count validation failed")
    raw_payload = decode_shared_block_payload(
        codec=codec,
        encoded_payload=stored_payload,
        raw_byte_count=int(block_row.get("raw_byte_count") or 0),
        maximum_raw_bytes=maximum_raw_bytes,
    )
    return _SharedPhysicalBlock(
        block_hash=block_hash,
        object_kind=object_kind,
        entry_count=block_entry_count,
        payload=raw_payload,
    )


def _validated_payload(
    block_row: Mapping[str, Any],
    *,
    expected_kind: str,
) -> SharedBlockPayload:
    physical_block = _validated_physical_block(
        block_row,
        expected_kind=expected_kind,
    )
    mapping_entry_count = int(block_row.get("mapping_entry_count") or 0)
    if (
        mapping_entry_count < 0
        or (
            physical_block.entry_count is not None
            and mapping_entry_count != physical_block.entry_count
        )
    ):
        raise PTG2SharedBlockError("shared PTG block entry count validation failed")
    return SharedBlockPayload(
        block_key=int(block_row.get("block_key") or 0),
        fragment_no=int(block_row.get("fragment_no") or 0),
        entry_count=mapping_entry_count,
        payload=physical_block.payload,
        block_hash=physical_block.block_hash,
    )


@dataclass(frozen=True)
class _SharedBlockReadRequest:
    schema_name: str
    snapshot_key: int
    object_kind: str
    block_keys: tuple[int, ...]
    block_key_set: frozenset[int]
    fragment_nos: tuple[int, ...]
    fragment_no_set: frozenset[int]
    has_fragment_filter: bool
    requires_all: bool


@dataclass(frozen=True)
class _SharedMappingSelection:
    mapping_records: tuple[dict[str, Any], ...]
    observed_block_keys: frozenset[int]
    fragment_nos_by_block_key: dict[int, frozenset[int]]
    physical_hashes: frozenset[bytes]


def _shared_block_read_request(
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: Iterable[int],
    fragment_nos: Iterable[int] | None,
    require_all: bool,
) -> _SharedBlockReadRequest:
    requested_fragments = (
        tuple(sorted({int(fragment_no) for fragment_no in fragment_nos}))
        if fragment_nos is not None
        else ()
    )
    if any(fragment_no < 0 for fragment_no in requested_fragments):
        raise ValueError("shared PTG fragment numbers must be non-negative")
    requested_block_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    return _SharedBlockReadRequest(
        schema_name=str(schema_name),
        snapshot_key=int(snapshot_key),
        object_kind=str(object_kind),
        block_keys=requested_block_keys,
        block_key_set=frozenset(requested_block_keys),
        fragment_nos=requested_fragments,
        fragment_no_set=frozenset(requested_fragments),
        has_fragment_filter=fragment_nos is not None,
        requires_all=bool(require_all),
    )


async def _shared_mapping_records(
    session: Any,
    request: _SharedBlockReadRequest,
) -> tuple[dict[str, Any], ...]:
    schema = _quote_ident(request.schema_name)
    fragment_filter = (
        "AND mapping.fragment_no = ANY(CAST(:fragment_nos AS integer[]))"
        if request.has_fragment_filter
        else ""
    )
    mapping_query = await session.execute(
        text(
            f"""
            SELECT mapping.object_kind, mapping.block_key, mapping.fragment_no,
                   mapping.entry_count AS mapping_entry_count, mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_layout layout
              JOIN {schema}.ptg2_v3_snapshot_block mapping
                ON mapping.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND mapping.object_kind = :object_kind
               AND mapping.block_key = ANY(CAST(:block_keys AS bigint[]))
               {fragment_filter}
             ORDER BY mapping.block_key, mapping.fragment_no
            """
        ),
        {
            "snapshot_key": request.snapshot_key,
            "generation": PTG2_V3_SHARED_GENERATION,
            "object_kind": request.object_kind,
            "block_keys": request.block_keys,
            "fragment_nos": request.fragment_nos,
        },
    )
    return tuple(_row_mapping(mapping_record) for mapping_record in mapping_query)


def _validated_mapping_coordinate(
    request: _SharedBlockReadRequest,
    mapping_record: Mapping[str, Any],
    *,
    previous_coordinate: tuple[int, int] | None,
) -> tuple[tuple[int, int], bytes]:
    mapping_kind = str(mapping_record.get("object_kind") or "")
    coordinate = (
        int(mapping_record.get("block_key") or 0),
        int(mapping_record.get("fragment_no") or 0),
    )
    physical_hash = bytes(mapping_record.get("block_hash") or b"")
    if (
        mapping_kind != request.object_kind
        or coordinate[0] not in request.block_key_set
        or coordinate[1] < 0
        or (
            request.has_fragment_filter
            and coordinate[1] not in request.fragment_no_set
        )
        or (previous_coordinate is not None and coordinate <= previous_coordinate)
        or len(physical_hash) != 32
    ):
        raise PTG2SharedBlockError(
            "shared PTG query returned an unexpected or unordered fragment"
        )
    return coordinate, physical_hash


def _shared_delivery_key(
    request: _SharedBlockReadRequest,
    coordinate: tuple[int, int],
) -> tuple[str, int, str, int, int]:
    return (
        request.schema_name,
        request.snapshot_key,
        request.object_kind,
        coordinate[0],
        coordinate[1],
    )


def _require_complete_shared_mapping(
    request: _SharedBlockReadRequest,
    selection: _SharedMappingSelection,
) -> None:
    if not request.requires_all:
        return
    missing_block_keys = sorted(
        request.block_key_set - selection.observed_block_keys
    )
    if missing_block_keys:
        raise PTG2SharedBlockError(
            f"shared PTG layout is missing block keys: {missing_block_keys[:8]}"
        )
    if not request.has_fragment_filter:
        return
    for block_key in request.block_keys:
        missing_fragment_nos = sorted(
            request.fragment_no_set
            - selection.fragment_nos_by_block_key.get(block_key, frozenset())
        )
        if missing_fragment_nos:
            raise PTG2SharedBlockError(
                "shared PTG layout is missing fragments: "
                f"{missing_fragment_nos[:8]}"
            )


async def _stream_shared_physical_records(
    session: Any,
    schema_name: str,
    physical_hashes: Iterable[bytes],
) -> AsyncIterator[dict[str, Any]]:
    requested_hashes = tuple(sorted(set(physical_hashes)))
    if not requested_hashes:
        return
    schema = _quote_ident(schema_name)
    statement = text(
        f"""
            SELECT block_hash, object_kind, format_version, codec,
                   entry_count AS block_entry_count,
                   raw_byte_count, stored_byte_count, payload
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
             ORDER BY block_hash
        """
    )
    params_by_name = {"block_hashes": requested_hashes}
    stream = getattr(session, "stream", None)
    physical_query = (
        await stream(statement, params_by_name)
        if callable(stream)
        else await session.execute(statement, params_by_name)
    )
    async for physical_record in _iterate_shared_query_rows(physical_query):
        yield _row_mapping(physical_record)


class SharedBlockReadOnceScope:
    """Read each immutable payload at most once within one bounded request."""

    def __init__(self, *, max_retained_raw_bytes: int) -> None:
        retained_limit = int(max_retained_raw_bytes)
        if retained_limit < 1:
            raise ValueError("shared PTG retained byte limit must be positive")
        self._max_retained_raw_bytes = retained_limit
        self._seen_physical_identities: set[tuple[str, bytes]] = set()
        self._delivered_coordinates: set[tuple[str, int, str, int, int]] = set()
        self._physical_rows_read = 0
        self._payload_decode_count = 0
        self._prepared_physical_identities: set[tuple[str, bytes]] = set()
        self._registered_logical_identities: set[
            tuple[str, tuple[bytes, ...]]
        ] = set()
        self._processed_logical_identities: set[
            tuple[str, tuple[bytes, ...]]
        ] = set()
        self._peak_raw_bytes = 0
        self._poisoned_reason: str | None = None
        self._lock = asyncio.Lock()

    @property
    def ledger(self) -> dict[str, int]:
        """Return counters for physical preparation and logical parsing."""

        logical_deliveries = len(self._delivered_coordinates)
        unique_physical_blocks = len(self._seen_physical_identities)
        logical_fragment_references = sum(
            len(physical_hashes)
            for _schema_name, physical_hashes in self._processed_logical_identities
        )
        logically_referenced_physical_blocks = {
            (schema_name, block_hash)
            for schema_name, physical_hashes in self._processed_logical_identities
            for block_hash in physical_hashes
        }
        return {
            "logical_block_deliveries": logical_deliveries,
            "physical_mapping_references": logical_deliveries,
            "physical_mapping_aliases": (
                logical_deliveries - unique_physical_blocks
            ),
            "unique_physical_blocks": unique_physical_blocks,
            "physical_block_reads": self._physical_rows_read,
            "physical_block_decodes": self._payload_decode_count,
            "physical_payload_preparations": len(
                self._prepared_physical_identities
            ),
            "expected_logical_payload_processes": len(
                self._registered_logical_identities
            ),
            "logical_payload_processes": len(
                self._processed_logical_identities
            ),
            "logical_payload_fragment_references": logical_fragment_references,
            "logical_payload_fragment_aliases": (
                logical_fragment_references
                - len(logically_referenced_physical_blocks)
            ),
            "repeated_physical_reads": (
                self._physical_rows_read - unique_physical_blocks
            ),
            "repeated_physical_decodes": (
                self._payload_decode_count - unique_physical_blocks
            ),
            "repeated_physical_preparations": 0,
            "repeated_logical_payload_processes": 0,
            "peak_raw_bytes": self._peak_raw_bytes,
        }

    @property
    def maximum_raw_bytes(self) -> int:
        """Return the maximum raw bytes allowed in one unique fetch."""

        return self._max_retained_raw_bytes

    def assert_read_once(self) -> None:
        """Fail unless every unique physical hash had one row and one decode."""

        if self._poisoned_reason is not None:
            raise PTG2SharedBlockError(
                f"shared PTG read-once scope is poisoned: {self._poisoned_reason}"
            )
        unique_count = len(self._seen_physical_identities)
        if unique_count != self._physical_rows_read or unique_count != self._payload_decode_count:
            raise PTG2SharedBlockError(
                "shared PTG read-once ledger does not prove one read and decode per block"
            )

    def prepare_payload(self, schema_name: str, block_hash: bytes) -> None:
        """Claim one physical payload after its sole decode and validation."""

        identity = (str(schema_name), bytes(block_hash))
        if identity not in self._seen_physical_identities:
            raise PTG2SharedBlockError(
                "shared PTG payload preparation has no physical read"
            )
        if identity in self._prepared_physical_identities:
            raise PTG2SharedBlockError(
                "shared PTG physical payload would be prepared more than once"
            )
        self._prepared_physical_identities.add(identity)

    def register_logical_payload(
        self,
        schema_name: str,
        physical_hashes: Iterable[bytes],
    ) -> None:
        """Register one expected ordered logical identity before parsing starts."""

        normalized_hashes = tuple(bytes(block_hash) for block_hash in physical_hashes)
        if not normalized_hashes:
            raise PTG2SharedBlockError("shared PTG logical payload is empty")
        physical_identities = {
            (str(schema_name), block_hash) for block_hash in normalized_hashes
        }
        if not physical_identities.issubset(self._seen_physical_identities):
            raise PTG2SharedBlockError(
                "shared PTG logical payload registration has an unread fragment"
            )
        self._registered_logical_identities.add(
            (str(schema_name), normalized_hashes)
        )

    def claim_logical_payload_processing(
        self,
        schema_name: str,
        physical_hashes: Iterable[bytes],
    ) -> None:
        """Claim one distinct ordered logical payload before semantic parsing."""

        normalized_hashes = tuple(bytes(block_hash) for block_hash in physical_hashes)
        if not normalized_hashes:
            raise PTG2SharedBlockError("shared PTG logical payload is empty")
        physical_identities = {
            (str(schema_name), block_hash) for block_hash in normalized_hashes
        }
        if not physical_identities.issubset(self._prepared_physical_identities):
            raise PTG2SharedBlockError(
                "shared PTG logical payload processing has an unprepared fragment"
            )
        logical_identity = (str(schema_name), normalized_hashes)
        if logical_identity not in self._registered_logical_identities:
            raise PTG2SharedBlockError(
                "shared PTG logical payload processing was not registered"
            )
        if logical_identity in self._processed_logical_identities:
            raise PTG2SharedBlockError(
                "shared PTG logical payload would be processed more than once"
            )
        self._processed_logical_identities.add(logical_identity)

    def claim_payload_processing(self, schema_name: str, block_hash: bytes) -> None:
        """Prepare and claim one self-contained physical payload parse."""

        self.prepare_payload(schema_name, block_hash)
        self.register_logical_payload(schema_name, (block_hash,))
        self.claim_logical_payload_processing(schema_name, (block_hash,))

    def assert_processed_once(self) -> None:
        """Fail unless all reads were prepared and covered by logical parses."""

        self.assert_read_once()
        logically_processed_physical_identities = {
            (schema_name, block_hash)
            for schema_name, physical_hashes in self._processed_logical_identities
            for block_hash in physical_hashes
        }
        if (
            self._prepared_physical_identities != self._seen_physical_identities
            or self._processed_logical_identities
            != self._registered_logical_identities
            or logically_processed_physical_identities
            != self._seen_physical_identities
        ):
            raise PTG2SharedBlockError(
                "shared PTG processing ledger is incomplete"
            )

    def _raise_if_poisoned(self) -> None:
        if self._poisoned_reason is not None:
            raise PTG2SharedBlockError(
                f"shared PTG read-once scope is poisoned: {self._poisoned_reason}"
            )

    def _poison(self, exc: BaseException) -> None:
        if self._poisoned_reason is None:
            self._poisoned_reason = (
                "request cancelled"
                if isinstance(exc, asyncio.CancelledError)
                else str(exc) or type(exc).__name__
            )

    async def fetch(
        self,
        session: Any,
        *,
        schema_name: str,
        snapshot_key: int,
        object_kind: str,
        block_keys: Iterable[int],
        fragment_nos: Iterable[int] | None,
        require_all: bool,
    ) -> tuple[SharedBlockPayload, ...]:
        """Fetch a unique coordinate set and poison this scope on any failure."""

        request = _shared_block_read_request(
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            object_kind=object_kind,
            block_keys=block_keys,
            fragment_nos=fragment_nos,
            require_all=require_all,
        )
        async with self._lock:
            self._raise_if_poisoned()
            if not request.block_keys:
                return ()
            try:
                return await self._fetch_locked(session, request)
            except BaseException as exc:
                self._poison(exc)
                raise

    async def _fetch_locked(
        self,
        session: Any,
        request: _SharedBlockReadRequest,
    ) -> tuple[SharedBlockPayload, ...]:
        """Execute one serialized mapping, physical fetch, and delivery cycle."""

        mapping_records = await _shared_mapping_records(session, request)
        selection = self._validated_mapping_selection(request, mapping_records)
        _require_complete_shared_mapping(request, selection)
        physical_blocks_by_hash = await self._physical_blocks_by_hash(
            session,
            request,
            selection.physical_hashes,
        )
        deliveries = self._mapping_deliveries(
            request,
            selection.mapping_records,
            physical_blocks_by_hash,
        )
        self.assert_read_once()
        return deliveries

    def _validated_mapping_selection(
        self,
        request: _SharedBlockReadRequest,
        mapping_records: tuple[dict[str, Any], ...],
    ) -> _SharedMappingSelection:
        observed_block_keys: set[int] = set()
        fragment_nos_by_block_key: dict[int, set[int]] = {}
        physical_hashes: set[bytes] = set()
        previous_coordinate: tuple[int, int] | None = None
        for mapping_record in mapping_records:
            coordinate, physical_hash = _validated_mapping_coordinate(
                request,
                mapping_record,
                previous_coordinate=previous_coordinate,
            )
            delivery_key = _shared_delivery_key(request, coordinate)
            if delivery_key in self._delivered_coordinates:
                raise PTG2SharedBlockError(
                    "shared PTG logical block was requested more than once"
                )
            previous_coordinate = coordinate
            observed_block_keys.add(coordinate[0])
            fragment_nos_by_block_key.setdefault(coordinate[0], set()).add(coordinate[1])
            physical_hashes.add(physical_hash)
        return _SharedMappingSelection(
            mapping_records=mapping_records,
            observed_block_keys=frozenset(observed_block_keys),
            fragment_nos_by_block_key={
                block_key: frozenset(fragment_nos)
                for block_key, fragment_nos in fragment_nos_by_block_key.items()
            },
            physical_hashes=frozenset(physical_hashes),
        )

    async def _physical_blocks_by_hash(
        self,
        session: Any,
        request: _SharedBlockReadRequest,
        physical_hashes: frozenset[bytes],
    ) -> dict[bytes, _SharedPhysicalBlock]:
        repeated_hashes = tuple(
            physical_hash
            for physical_hash in physical_hashes
            if (request.schema_name, physical_hash) in self._seen_physical_identities
        )
        if repeated_hashes:
            raise PTG2SharedBlockError(
                "shared PTG physical block was requested more than once"
            )
        physical_blocks_by_hash: dict[bytes, _SharedPhysicalBlock] = {}
        raw_bytes_in_fetch = 0
        async for physical_record in _stream_shared_physical_records(
            session,
            request.schema_name,
            physical_hashes,
        ):
            returned_hash = bytes(physical_record.get("block_hash") or b"")
            if returned_hash not in physical_hashes or returned_hash in physical_blocks_by_hash:
                raise PTG2SharedBlockError(
                    "shared PTG physical block query returned an unexpected row"
                )
            self._physical_rows_read += 1
            remaining_raw_bytes = self._max_retained_raw_bytes - raw_bytes_in_fetch
            physical_block = _validated_physical_block(
                physical_record,
                expected_kind=request.object_kind,
                maximum_raw_bytes=remaining_raw_bytes,
            )
            self._payload_decode_count += 1
            raw_bytes_in_fetch += len(physical_block.payload)
            physical_blocks_by_hash[physical_block.block_hash] = physical_block
            self._seen_physical_identities.add(
                (request.schema_name, physical_block.block_hash)
            )
        if set(physical_hashes) != set(physical_blocks_by_hash):
            raise PTG2SharedBlockError(
                "shared PTG layout references a missing physical block"
            )
        self._peak_raw_bytes = max(self._peak_raw_bytes, raw_bytes_in_fetch)
        return physical_blocks_by_hash

    def _mapping_deliveries(
        self,
        request: _SharedBlockReadRequest,
        mapping_records: tuple[dict[str, Any], ...],
        physical_blocks_by_hash: Mapping[bytes, _SharedPhysicalBlock],
    ) -> tuple[SharedBlockPayload, ...]:
        deliveries: list[SharedBlockPayload] = []
        for mapping_record in mapping_records:
            physical_hash = bytes(mapping_record["block_hash"])
            physical_block = physical_blocks_by_hash[physical_hash]
            mapping_entry_count = int(mapping_record.get("mapping_entry_count") or 0)
            if mapping_entry_count < 0 or (
                physical_block.entry_count is not None
                and mapping_entry_count != physical_block.entry_count
            ):
                raise PTG2SharedBlockError(
                    "shared PTG block entry count validation failed"
                )
            coordinate = (
                int(mapping_record.get("block_key") or 0),
                int(mapping_record.get("fragment_no") or 0),
            )
            self._delivered_coordinates.add(_shared_delivery_key(request, coordinate))
            deliveries.append(
                SharedBlockPayload(
                    block_key=coordinate[0],
                    fragment_no=coordinate[1],
                    entry_count=mapping_entry_count,
                    payload=physical_block.payload,
                    block_hash=physical_hash,
                )
            )
        return tuple(deliveries)


_ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE: ContextVar[
    SharedBlockReadOnceScope | None
] = ContextVar("active_shared_block_read_once_scope", default=None)


@contextmanager
def shared_block_read_once_scope(
    *,
    max_retained_raw_bytes: int,
) -> Iterator[SharedBlockReadOnceScope]:
    """Create one non-nested request-local read-once scope."""

    if _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get() is not None:
        raise PTG2SharedBlockError("shared PTG read-once scopes cannot be nested")
    reader = SharedBlockReadOnceScope(
        max_retained_raw_bytes=max_retained_raw_bytes,
    )
    token = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.set(reader)
    try:
        yield reader
    finally:
        _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.reset(token)


def claim_shared_block_processing(*, schema_name: str, block_hash: bytes) -> None:
    """Prepare and claim one self-contained payload when a scope is active."""

    read_once_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if read_once_scope is not None:
        read_once_scope.claim_payload_processing(schema_name, block_hash)


def prepare_shared_block_payload(*, schema_name: str, block_hash: bytes) -> None:
    """Prepare one unique physical payload when a request-local scope is active."""

    read_once_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if read_once_scope is not None:
        read_once_scope.prepare_payload(schema_name, block_hash)


def register_shared_logical_payload(
    *,
    schema_name: str,
    physical_hashes: Iterable[bytes],
) -> None:
    """Register one expected logical payload when a request scope is active."""

    read_once_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if read_once_scope is not None:
        read_once_scope.register_logical_payload(schema_name, physical_hashes)


def claim_shared_logical_payload_processing(
    *,
    schema_name: str,
    physical_hashes: Iterable[bytes],
) -> None:
    """Claim one ordered logical payload when a request-local scope is active."""

    read_once_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if read_once_scope is not None:
        read_once_scope.claim_logical_payload_processing(
            schema_name,
            physical_hashes,
        )


async def fetch_shared_blocks(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: Iterable[int],
    fragment_nos: Iterable[int] | None = None,
    require_all: bool = False,
) -> dict[int, tuple[SharedBlockPayload, ...]]:
    """Fetch selected immutable blocks with one stable PostgreSQL statement."""

    payloads_by_key: dict[int, list[SharedBlockPayload]] = {}
    async for payload in stream_shared_blocks(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        object_kind=object_kind,
        block_keys=block_keys,
        fragment_nos=fragment_nos,
        require_all=require_all,
    ):
        payloads_by_key.setdefault(payload.block_key, []).append(payload)
    return {
        block_key: tuple(payloads)
        for block_key, payloads in payloads_by_key.items()
    }


async def stream_shared_blocks(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: Iterable[int],
    fragment_nos: Iterable[int] | None = None,
    require_all: bool = False,
) -> AsyncIterator[SharedBlockPayload]:
    """Stream validated immutable fragments in mapping order."""

    request = _shared_block_read_request(
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        object_kind=object_kind,
        block_keys=block_keys,
        fragment_nos=fragment_nos,
        require_all=require_all,
    )
    active_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if active_scope is not None:
        scoped_fragments = await active_scope.fetch(
            session,
            schema_name=request.schema_name,
            snapshot_key=request.snapshot_key,
            object_kind=request.object_kind,
            block_keys=request.block_keys,
            fragment_nos=(request.fragment_nos if request.has_fragment_filter else None),
            require_all=request.requires_all,
        )
        for scoped_fragment in scoped_fragments:
            yield scoped_fragment
        return
    if not request.block_keys:
        return
    async for direct_fragment in _stream_shared_blocks_direct(session, request):
        yield direct_fragment


async def _stream_shared_blocks_direct(
    session: Any,
    request: _SharedBlockReadRequest,
) -> AsyncIterator[SharedBlockPayload]:
    query_result = await _shared_direct_query_result(session, request)
    observed_keys: set[int] = set()
    observed_fragments_by_key: dict[int, set[int]] = {}
    previous_mapping_key: tuple[int, int] | None = None
    async for raw_row in _iterate_shared_query_rows(query_result):
        block_payload = _validated_payload(
            _row_mapping(raw_row),
            expected_kind=request.object_kind,
        )
        mapping_key = (block_payload.block_key, block_payload.fragment_no)
        if (
            block_payload.block_key not in request.block_key_set
            or block_payload.fragment_no < 0
            or (
                request.has_fragment_filter
                and block_payload.fragment_no not in request.fragment_no_set
            )
            or (
                previous_mapping_key is not None
                and mapping_key <= previous_mapping_key
            )
        ):
            raise PTG2SharedBlockError(
                "shared PTG query returned an unexpected or unordered fragment"
            )
        previous_mapping_key = mapping_key
        observed_keys.add(block_payload.block_key)
        observed_fragments_by_key.setdefault(block_payload.block_key, set()).add(
            block_payload.fragment_no
        )
        yield block_payload
    _require_complete_direct_stream(
        request,
        observed_keys,
        observed_fragments_by_key,
    )


async def _shared_direct_query_result(
    session: Any,
    request: _SharedBlockReadRequest,
) -> Any:
    fragment_filter = (
        "AND mapping.fragment_no = ANY(CAST(:fragment_nos AS integer[]))"
        if request.has_fragment_filter
        else ""
    )
    schema = _quote_ident(request.schema_name)
    statement = text(
        f"""
            SELECT mapping.object_kind, mapping.block_key, mapping.fragment_no,
                   mapping.entry_count AS mapping_entry_count, mapping.block_hash,
                   block.format_version, block.codec,
                   block.entry_count AS block_entry_count,
                   block.raw_byte_count, block.stored_byte_count, block.payload
              FROM {schema}.ptg2_v3_snapshot_layout layout
              JOIN {schema}.ptg2_v3_snapshot_block mapping
                ON mapping.snapshot_key = layout.snapshot_key
              JOIN {schema}.ptg2_v3_block block
                ON block.block_hash = mapping.block_hash
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND mapping.object_kind = :object_kind
               AND mapping.block_key = ANY(CAST(:block_keys AS bigint[]))
               {fragment_filter}
             ORDER BY mapping.block_key, mapping.fragment_no
            """
    )
    query_params_by_name = {
        "snapshot_key": request.snapshot_key,
        "generation": PTG2_V3_SHARED_GENERATION,
        "object_kind": request.object_kind,
        "block_keys": request.block_keys,
        "fragment_nos": request.fragment_nos,
    }
    stream = getattr(session, "stream", None)
    return (
        await stream(statement, query_params_by_name)
        if callable(stream)
        else await session.execute(statement, query_params_by_name)
    )


async def _iterate_shared_query_rows(query_result: Any) -> AsyncIterator[Any]:
    if hasattr(query_result, "__aiter__"):
        async for raw_row in query_result:
            yield raw_row
        return
    for raw_row in query_result:
        yield raw_row


def _require_complete_direct_stream(
    request: _SharedBlockReadRequest,
    observed_block_keys: set[int],
    fragment_nos_by_block_key: Mapping[int, set[int]],
) -> None:
    if not request.requires_all:
        return
    missing_block_keys = sorted(request.block_key_set - observed_block_keys)
    if missing_block_keys:
        raise PTG2SharedBlockError(
            f"shared PTG layout is missing block keys: {missing_block_keys[:8]}"
        )
    if not request.has_fragment_filter:
        return
    for block_key in request.block_keys:
        missing_fragment_nos = sorted(
            request.fragment_no_set
            - fragment_nos_by_block_key.get(block_key, set())
        )
        if missing_fragment_nos:
            raise PTG2SharedBlockError(
                "shared PTG layout is missing fragments: "
                f"{missing_fragment_nos[:8]}"
            )


async def fetch_snapshot_source_set_metadata(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    expected_source_count: int,
) -> dict[str, Any]:
    """Recompute one bounded logical snapshot source-set seal from PostgreSQL."""

    source_set, _ordered_digest, _raw_hashes = await fetch_snapshot_source_set_identity(
        session,
        schema_name=schema_name,
        logical_snapshot_id=logical_snapshot_id,
        expected_source_count=expected_source_count,
    )
    return source_set


async def fetch_snapshot_source_set_identity(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    expected_source_count: int,
) -> tuple[dict[str, Any], str, tuple[str, ...]]:
    """Return set, ordered identity, and raw hashes from one source-row read."""

    source_count = int(expected_source_count)
    dense_source_key_bits(source_count)
    if source_count > PTG2_V3_MAX_AUDIT_SOURCE_FILES:
        raise PTG2SharedBlockError(
            "shared PTG audit source set exceeds the bounded verification limit"
        )
    snapshot_id = str(logical_snapshot_id or "").strip()
    if not snapshot_id:
        raise PTG2SharedBlockError("shared PTG logical snapshot id is missing")
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT source_key, raw_container_sha256
              FROM {schema}.ptg2_v3_snapshot_source
             WHERE snapshot_id = :snapshot_id
             ORDER BY source_key
             LIMIT :row_limit
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "row_limit": source_count + 1,
        },
    )
    metadata_rows = [
        _row_mapping(metadata_row) for metadata_row in query_result
    ]
    if (
        len(metadata_rows) != source_count
        or [metadata_row.get("source_key") for metadata_row in metadata_rows]
        != list(range(source_count))
    ):
        raise PTG2SharedBlockError(
            "shared PTG source metadata is not complete and dense"
        )
    try:
        raw_hashes = tuple(
            str(metadata_row.get("raw_container_sha256") or "").strip().lower()
            for metadata_row in metadata_rows
        )
        return (
            shared_source_set_metadata(raw_hashes),
            ordered_source_ordinal_digest(raw_hashes),
            raw_hashes,
        )
    except ValueError as exc:
        raise PTG2SharedBlockError(
            "shared PTG source-set identity metadata is invalid"
        ) from exc


async def fetch_snapshot_source_provenance(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    source_keys: Iterable[int],
    expected_source_count: int,
) -> dict[int, dict[str, Any]]:
    """Load exact source identities and traces for selected dense source keys."""

    source_count = int(expected_source_count)
    dense_source_key_bits(source_count)
    requested_keys = tuple(sorted({int(source_key) for source_key in source_keys}))
    if not requested_keys:
        return {}
    if requested_keys[0] < 0 or requested_keys[-1] >= source_count:
        raise PTG2SharedBlockError(
            "shared PTG response contains a source key outside the manifest dictionary"
        )
    snapshot_id = str(logical_snapshot_id or "").strip()
    if not snapshot_id:
        raise PTG2SharedBlockError("shared PTG logical snapshot id is missing")
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            WITH source_summary AS MATERIALIZED (
                SELECT COUNT(*)::bigint AS source_count,
                       COUNT(DISTINCT source_key)::bigint AS distinct_source_count,
                       MIN(source_key) AS minimum_source_key,
                       MAX(source_key) AS maximum_source_key
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
            ), selected_source AS MATERIALIZED (
                SELECT source_key, source_type, identity_kind, identity_sha256,
                       raw_container_sha256, logical_json_sha256,
                       logical_hash_deferred, source_trace_set_hash
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
                   AND source_key = ANY(CAST(:source_keys AS integer[]))
            )
            SELECT source.source_key, source.source_type, source.identity_kind,
                   source.identity_sha256, source.raw_container_sha256,
                   source.logical_json_sha256, source.logical_hash_deferred,
                   source.source_trace_set_hash,
                   summary.source_count, summary.distinct_source_count,
                   summary.minimum_source_key, summary.maximum_source_key,
                   CARDINALITY(trace_set.source_trace_hashes) AS trace_hash_count,
                   COUNT(trace.source_trace_hash) AS resolved_trace_count,
                   COALESCE(
                       jsonb_agg(
                           jsonb_strip_nulls(
                               jsonb_build_object(
                                   'source_file_version_id', trace.source_file_version_id,
                                   'original_url', trace.original_url,
                                   'canonical_url', trace.canonical_url,
                                   'json_pointer', trace.json_pointer,
                                   'line_number', trace.line_number
                               )
                           )
                           ORDER BY trace_ref.ordinality
                       ) FILTER (WHERE trace.source_trace_hash IS NOT NULL),
                       '[]'::jsonb
                   ) AS source_trace
              FROM selected_source source
             CROSS JOIN source_summary summary
              JOIN {schema}.ptg2_source_trace_set trace_set
                ON trace_set.source_trace_set_hash = source.source_trace_set_hash
              LEFT JOIN LATERAL unnest(
                   COALESCE(trace_set.source_trace_hashes, ARRAY[]::varchar[])
              ) WITH ORDINALITY trace_ref(source_trace_hash, ordinality) ON TRUE
              LEFT JOIN {schema}.ptg2_source_trace trace
                ON trace.source_trace_hash = trace_ref.source_trace_hash
             GROUP BY source.source_key, source.source_type, source.identity_kind,
                      source.identity_sha256, source.raw_container_sha256,
                      source.logical_json_sha256, source.logical_hash_deferred,
                      source.source_trace_set_hash, summary.source_count,
                      summary.distinct_source_count, summary.minimum_source_key,
                      summary.maximum_source_key, trace_set.source_trace_hashes
             ORDER BY source.source_key
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "source_keys": requested_keys,
        },
    )
    provenance_by_key: dict[int, dict[str, Any]] = {}
    for raw_row in query_result:
        provenance_row = _row_mapping(raw_row)
        minimum_source_key = provenance_row.get("minimum_source_key")
        maximum_source_key = provenance_row.get("maximum_source_key")
        if (
            int(provenance_row.get("source_count") or 0) != source_count
            or int(provenance_row.get("distinct_source_count") or 0) != source_count
            or minimum_source_key is None
            or int(minimum_source_key) != 0
            or maximum_source_key is None
            or int(maximum_source_key) != source_count - 1
        ):
            raise PTG2SharedBlockError(
                "shared PTG source metadata is not complete and dense"
            )
        source_key = int(provenance_row.get("source_key"))
        identity_sha256 = str(provenance_row.get("identity_sha256") or "")
        raw_sha256 = str(provenance_row.get("raw_container_sha256") or "")
        logical_sha256 = provenance_row.get("logical_json_sha256")
        logical_sha256 = str(logical_sha256) if logical_sha256 is not None else None
        trace_set_hash = str(provenance_row.get("source_trace_set_hash") or "")
        deferred = bool(provenance_row.get("logical_hash_deferred"))
        if (
            not str(provenance_row.get("source_type") or "").strip()
            or not str(provenance_row.get("identity_kind") or "").strip()
            or not _SHA256_RE.fullmatch(identity_sha256)
            or not _SHA256_RE.fullmatch(raw_sha256)
            or not _SHA256_RE.fullmatch(trace_set_hash)
            or (deferred and logical_sha256 is not None)
            or (not deferred and not _SHA256_RE.fullmatch(logical_sha256 or ""))
            or int(provenance_row.get("resolved_trace_count") or 0)
            != int(provenance_row.get("trace_hash_count") or 0)
        ):
            raise PTG2SharedBlockError(
                "shared PTG source identity or trace mapping is invalid"
            )
        source_trace = provenance_row.get("source_trace")
        if isinstance(source_trace, str):
            try:
                source_trace = json.loads(source_trace)
            except json.JSONDecodeError as exc:
                raise PTG2SharedBlockError(
                    "shared PTG source trace payload is malformed"
                ) from exc
        if not isinstance(source_trace, list) or not all(
            isinstance(trace, Mapping) for trace in source_trace
        ):
            raise PTG2SharedBlockError(
                "shared PTG source trace payload is malformed"
            )
        if source_key in provenance_by_key:
            raise PTG2SharedBlockError(
                "shared PTG source metadata contains a duplicate source key"
            )
        provenance_by_key[source_key] = {
            "source_key": source_key,
            "source_type": str(provenance_row["source_type"]),
            "identity_kind": str(provenance_row["identity_kind"]),
            "identity_sha256": identity_sha256,
            "raw_container_sha256": raw_sha256,
            "logical_json_sha256": logical_sha256,
            "logical_hash_deferred": deferred,
            "source_trace_set_hash": trace_set_hash,
            "source_trace": [dict(trace) for trace in source_trace],
        }
    if set(provenance_by_key) != set(requested_keys):
        raise PTG2SharedBlockError(
            "shared PTG source mapping is missing a selected source key"
        )
    return provenance_by_key


def _selected_graph_bytes(
    raw_chunks_by_key: Mapping[int, bytes],
    *,
    first_chunk: int,
    member_offset: int,
    byte_count: int,
) -> bytes:
    remaining = int(byte_count)
    if remaining == 0:
        return b""
    chunk_key = int(first_chunk)
    chunk_offset = int(member_offset)
    selected_parts: list[memoryview] = []
    while remaining:
        chunk = raw_chunks_by_key.get(chunk_key)
        if chunk is None or chunk_offset < 0 or chunk_offset >= len(chunk):
            raise PTG2SharedBlockError("shared PTG graph member stream is truncated")
        selected_count = min(remaining, len(chunk) - chunk_offset)
        selected_parts.append(memoryview(chunk)[chunk_offset : chunk_offset + selected_count])
        remaining -= selected_count
        chunk_key += 1
        chunk_offset = 0
    return b"".join(selected_parts)


async def _fetch_shared_graph_members_read_once(
    session: Any,
    request: _SharedGraphReadRequest,
) -> dict[int, tuple[int, ...]]:
    """Resolve all owners through unique request-local graph chunks."""

    read_once_scope = _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get()
    if read_once_scope is None:
        raise PTG2SharedBlockError("shared PTG graph read-once scope is missing")
    owner_records = await _shared_graph_owner_records(session, request)
    owner_selection = _validated_graph_owner_selection(
        request,
        owner_records,
        maximum_raw_bytes=read_once_scope.maximum_raw_bytes,
    )
    chunks_by_key = await fetch_shared_blocks(
        session,
        schema_name=request.schema_name,
        snapshot_key=request.snapshot_key,
        object_kind=request.object_kind,
        block_keys=owner_selection.required_chunk_keys,
        require_all=True,
    )
    raw_chunks_by_key = _validated_graph_chunks(request, chunks_by_key)
    return _decoded_scoped_graph_members(
        request,
        owner_selection.locator_by_owner,
        raw_chunks_by_key,
    )


@dataclass(frozen=True)
class _SharedGraphReadRequest:
    schema_name: str
    snapshot_key: int
    direction: int
    owner_keys: tuple[int, ...]
    owner_key_set: frozenset[int]
    object_kind: str
    member_width: int
    member_count_sql: str
    params_by_name: dict[str, Any]


@dataclass(frozen=True)
class _GraphOwnerSelection:
    locator_by_owner: dict[int, tuple[int, int, int, int]]
    required_chunk_keys: frozenset[int]


def _shared_graph_read_request(
    *,
    schema_name: str,
    snapshot_key: int,
    direction: int,
    owner_keys: tuple[int, ...],
    max_members: int | None,
) -> _SharedGraphReadRequest:
    try:
        object_kind, member_width = _GRAPH_KIND_AND_WIDTH[int(direction)]
    except (KeyError, ValueError) as exc:
        raise ValueError(f"unsupported shared PTG graph direction: {direction!r}") from exc
    normalized_max_members: int | None = None
    member_count_sql = "owner.member_count"
    if max_members is not None:
        if isinstance(max_members, bool) or int(max_members) < 0:
            raise ValueError("shared PTG graph max_members must be non-negative")
        normalized_max_members = int(max_members)
        member_count_sql = "LEAST(owner.member_count, :max_members)"
    params_by_name = {
        "snapshot_key": int(snapshot_key),
        "generation": PTG2_V3_SHARED_GENERATION,
        "direction": int(direction),
        "owner_keys": owner_keys,
        "object_kind": object_kind,
        "member_width": member_width,
        "chunk_bytes": PTG2_V3_GRAPH_CHUNK_BYTES,
    }
    if normalized_max_members is not None:
        params_by_name["max_members"] = normalized_max_members
    return _SharedGraphReadRequest(
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        direction=int(direction),
        owner_keys=owner_keys,
        owner_key_set=frozenset(owner_keys),
        object_kind=object_kind,
        member_width=member_width,
        member_count_sql=member_count_sql,
        params_by_name=params_by_name,
    )


async def _shared_graph_owner_records(
    session: Any,
    request: _SharedGraphReadRequest,
) -> tuple[dict[str, Any], ...]:
    schema = _quote_ident(request.schema_name)
    owner_query = await session.execute(
        text(
            f"""
            SELECT owner.owner_key, owner.first_chunk, owner.member_offset,
                   owner.member_count,
                   {request.member_count_sql} AS selected_member_count
              FROM {schema}.ptg2_v3_snapshot_layout layout
              JOIN {schema}.ptg2_v3_graph_owner owner
                ON owner.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND owner.direction = :direction
               AND owner.owner_key = ANY(CAST(:owner_keys AS bigint[]))
             ORDER BY owner.owner_key
            """
        ),
        request.params_by_name,
    )
    return tuple(_row_mapping(owner_record) for owner_record in owner_query)


def _validated_graph_owner_selection(
    request: _SharedGraphReadRequest,
    owner_records: tuple[dict[str, Any], ...],
    *,
    maximum_raw_bytes: int,
) -> _GraphOwnerSelection:
    """Validate owner locators and collect each required physical chunk once."""

    locator_by_owner: dict[int, tuple[int, int, int, int]] = {}
    required_chunk_keys: set[int] = set()
    selected_byte_ranges: list[tuple[int, int]] = []
    previous_owner_key: int | None = None
    maximum_chunk_count = max(
        1,
        (int(maximum_raw_bytes) + PTG2_V3_GRAPH_CHUNK_BYTES - 1)
        // PTG2_V3_GRAPH_CHUNK_BYTES,
    )
    for owner_record in owner_records:
        owner_key, locator = _validated_graph_owner_locator(
            request,
            owner_record,
            previous_owner_key,
        )
        first_chunk, member_offset, member_count, selected_member_count = locator
        previous_owner_key = owner_key
        locator_by_owner[owner_key] = locator
        selected_byte_count = selected_member_count * request.member_width
        if selected_byte_count:
            range_start = first_chunk * PTG2_V3_GRAPH_CHUNK_BYTES + member_offset
            selected_byte_ranges.append(
                (range_start, range_start + selected_byte_count)
            )
            last_chunk = first_chunk + (
                (member_offset + selected_byte_count - 1)
                // PTG2_V3_GRAPH_CHUNK_BYTES
            )
            if last_chunk - first_chunk + 1 > maximum_chunk_count:
                raise PTG2SharedBlockError(
                    "shared PTG graph owner exceeds the read-once byte limit"
                )
            required_chunk_keys.update(range(first_chunk, last_chunk + 1))
            if len(required_chunk_keys) > maximum_chunk_count:
                raise PTG2SharedBlockError(
                    "shared PTG graph chunks exceed the read-once byte limit"
                )
    ordered_ranges = sorted(selected_byte_ranges)
    if any(
        right_start < left_end
        for (_left_start, left_end), (right_start, _right_end) in zip(
            ordered_ranges,
            ordered_ranges[1:],
        )
    ):
        raise PTG2SharedBlockError(
            "shared PTG graph owner ranges overlap"
        )
    return _GraphOwnerSelection(
        locator_by_owner=locator_by_owner,
        required_chunk_keys=frozenset(required_chunk_keys),
    )


def _validated_graph_owner_locator(
    request: _SharedGraphReadRequest,
    owner_record: Mapping[str, Any],
    previous_owner_key: int | None,
) -> tuple[int, tuple[int, int, int, int]]:
    owner_key = int(owner_record["owner_key"])
    locator = (
        int(owner_record["first_chunk"]),
        int(owner_record["member_offset"]),
        int(owner_record["member_count"]),
        int(owner_record.get("selected_member_count", owner_record["member_count"])),
    )
    first_chunk, member_offset, member_count, selected_member_count = locator
    if (
        owner_key not in request.owner_key_set
        or (previous_owner_key is not None and owner_key <= previous_owner_key)
        or first_chunk < 0
        or member_offset < 0
        or member_offset >= PTG2_V3_GRAPH_CHUNK_BYTES
        or member_count < 0
        or selected_member_count < 0
        or selected_member_count > member_count
    ):
        raise PTG2SharedBlockError(
            "shared PTG graph owner locator is invalid or unordered"
        )
    return owner_key, locator


def _validated_graph_chunks(
    request: _SharedGraphReadRequest,
    chunks_by_key: Mapping[int, tuple[SharedBlockPayload, ...]],
) -> dict[int, bytes]:
    if not chunks_by_key:
        return {}
    last_chunk_key = max(chunks_by_key)
    raw_chunks_by_key: dict[int, bytes] = {}
    validated_by_hash: dict[bytes, bytes] = {}
    for block_key, fragments in chunks_by_key.items():
        if len(fragments) != 1 or fragments[0].fragment_no != 0:
            raise PTG2SharedBlockError(
                "shared PTG graph chunk has an invalid fragment layout"
            )
        fragment = fragments[0]
        raw_chunk = validated_by_hash.get(fragment.block_hash)
        if raw_chunk is None:
            claim_shared_block_processing(
                schema_name=request.schema_name,
                block_hash=fragment.block_hash,
            )
            raw_chunk = fragment.payload
            if (
                not raw_chunk
                or len(raw_chunk) > PTG2_V3_GRAPH_CHUNK_BYTES
                or len(raw_chunk) % request.member_width
                or fragment.entry_count * request.member_width != len(raw_chunk)
            ):
                raise PTG2SharedBlockError(
                    "shared PTG graph chunk has invalid member framing"
                )
            validated_by_hash[fragment.block_hash] = raw_chunk
        if (
            fragment.entry_count * request.member_width != len(raw_chunk)
            or (
                block_key != last_chunk_key
                and len(raw_chunk) != PTG2_V3_GRAPH_CHUNK_BYTES
            )
        ):
            raise PTG2SharedBlockError(
                "shared PTG graph chunk has invalid member framing"
            )
        raw_chunks_by_key[block_key] = raw_chunk
    return raw_chunks_by_key


def _decoded_scoped_graph_members(
    request: _SharedGraphReadRequest,
    locator_by_owner: Mapping[int, tuple[int, int, int, int]],
    raw_chunks_by_key: Mapping[int, bytes],
) -> dict[int, tuple[int, ...]]:
    members_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key in request.owner_keys:
        locator = locator_by_owner.get(owner_key)
        if locator is None:
            members_by_owner[owner_key] = ()
            continue
        first_chunk, member_offset, _member_count, selected_member_count = locator
        selected = _selected_graph_bytes(
            raw_chunks_by_key,
            first_chunk=first_chunk,
            member_offset=member_offset,
            byte_count=selected_member_count * request.member_width,
        )
        members_by_owner[owner_key] = tuple(
            int.from_bytes(
                selected[offset : offset + request.member_width],
                "little",
                signed=False,
            )
            for offset in range(0, len(selected), request.member_width)
        )
    return members_by_owner


async def fetch_shared_graph_members(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    direction: int,
    owner_keys: Iterable[int],
    max_members: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Resolve one dense graph direction in one PostgreSQL round trip."""

    requested_owner_keys = tuple(sorted({int(owner_key) for owner_key in owner_keys}))
    if not requested_owner_keys:
        return {}
    request = _shared_graph_read_request(
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        direction=direction,
        owner_keys=requested_owner_keys,
        max_members=max_members,
    )
    if _ACTIVE_SHARED_BLOCK_READ_ONCE_SCOPE.get() is not None:
        return await _fetch_shared_graph_members_read_once(session, request)
    return await _fetch_shared_graph_members_direct(session, request)


async def _fetch_shared_graph_members_direct(
    session: Any,
    request: _SharedGraphReadRequest,
) -> dict[int, tuple[int, ...]]:
    schema = _quote_ident(request.schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT owner.owner_key, owner.first_chunk, owner.member_offset,
                   owner.member_count,
                   {request.member_count_sql} AS selected_member_count,
                   mapping.object_kind, mapping.block_key,
                   mapping.fragment_no, mapping.entry_count AS mapping_entry_count,
                   mapping.block_hash, block.format_version, block.codec,
                   block.raw_byte_count, block.stored_byte_count, block.payload
              FROM {schema}.ptg2_v3_snapshot_layout layout
              JOIN {schema}.ptg2_v3_graph_owner owner
                ON owner.snapshot_key = layout.snapshot_key
              JOIN LATERAL generate_series(
                   owner.first_chunk,
                   owner.first_chunk + CASE
                       WHEN {request.member_count_sql} = 0 THEN -1
                       ELSE ((owner.member_offset + {request.member_count_sql} * :member_width - 1)
                             / :chunk_bytes)::integer
                   END
              ) required_chunk(block_key) ON TRUE
              JOIN {schema}.ptg2_v3_snapshot_block mapping
                ON mapping.snapshot_key = owner.snapshot_key
               AND mapping.object_kind = :object_kind
               AND mapping.block_key = required_chunk.block_key
              JOIN {schema}.ptg2_v3_block block
                ON block.block_hash = mapping.block_hash
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
               AND owner.direction = :direction
               AND owner.owner_key = ANY(CAST(:owner_keys AS bigint[]))
             ORDER BY owner.owner_key, mapping.block_key, mapping.fragment_no
            """
        ),
        request.params_by_name,
    )
    graph_records = tuple(_row_mapping(graph_record) for graph_record in query_result)
    locator_by_owner, chunks_by_owner = _indexed_direct_graph_records(
        request,
        graph_records,
    )
    return _decoded_direct_graph_members(request, locator_by_owner, chunks_by_owner)


def _indexed_direct_graph_records(
    request: _SharedGraphReadRequest,
    graph_records: tuple[dict[str, Any], ...],
) -> tuple[
    dict[int, tuple[int, int, int]],
    dict[int, list[SharedBlockPayload]],
]:
    locator_by_owner: dict[int, tuple[int, int, int]] = {}
    chunks_by_owner: dict[int, list[SharedBlockPayload]] = {}
    for graph_record in graph_records:
        owner_key = int(graph_record["owner_key"])
        locator = (
            int(graph_record["member_offset"]),
            int(graph_record["member_count"]),
            int(
                graph_record.get(
                    "selected_member_count",
                    graph_record["member_count"],
                )
            ),
        )
        if locator[1] < 0 or locator[2] < 0 or locator[2] > locator[1]:
            raise PTG2SharedBlockError(
                "shared PTG graph owner member count is invalid"
            )
        previous_locator = locator_by_owner.setdefault(owner_key, locator)
        if previous_locator != locator:
            raise PTG2SharedBlockError("shared PTG graph owner locator changed within one query")
        chunks_by_owner.setdefault(owner_key, []).append(
            _validated_payload(graph_record, expected_kind=request.object_kind)
        )
    return locator_by_owner, chunks_by_owner


def _decoded_direct_graph_members(
    request: _SharedGraphReadRequest,
    locator_by_owner: Mapping[int, tuple[int, int, int]],
    chunks_by_owner: Mapping[int, list[SharedBlockPayload]],
) -> dict[int, tuple[int, ...]]:
    members_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key in request.owner_keys:
        locator = locator_by_owner.get(owner_key)
        if locator is None:
            members_by_owner[owner_key] = ()
            continue
        member_offset, _member_count, selected_member_count = locator
        raw_members = b"".join(chunk.payload for chunk in chunks_by_owner.get(owner_key, []))
        member_bytes = selected_member_count * request.member_width
        selected = raw_members[member_offset : member_offset + member_bytes]
        if len(selected) != member_bytes:
            raise PTG2SharedBlockError(f"shared PTG graph member stream is truncated for owner {owner_key}")
        members_by_owner[owner_key] = tuple(
            int.from_bytes(
                selected[offset : offset + request.member_width],
                "little",
                signed=False,
            )
            for offset in range(0, len(selected), request.member_width)
        )
    return members_by_owner
