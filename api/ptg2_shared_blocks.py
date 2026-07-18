# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cache-free reads from the strict PTG V3 shared PostgreSQL layout."""

from __future__ import annotations

import json
import re
import zlib
from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V3_SHARED_FORMAT_VERSION,
    shared_block_hash,
)
from process.ptg_parts.ptg2_shared_reuse import shared_source_set_metadata


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
    payload: bytes | bytearray | memoryview,
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
    if end > len(payload):
        raise PTG2SharedBlockError("shared PTG source vector is truncated")
    encoded = payload[cursor:end]
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
    payload: bytes,
    raw_byte_count: int,
) -> bytes:
    """Decode one independently compressed block without retaining it globally."""

    normalized_codec = str(codec or "").strip().lower()
    if normalized_codec == "none":
        raw_payload = bytes(payload)
    elif normalized_codec == "zlib":
        try:
            decompressor = zlib.decompressobj()
            raw_payload = decompressor.decompress(bytes(payload))
            raw_payload += decompressor.flush()
        except zlib.error as exc:
            raise PTG2SharedBlockError(f"invalid shared PTG zlib block: {exc}") from exc
        if (
            not decompressor.eof
            or decompressor.unused_data
            or decompressor.unconsumed_tail
        ):
            raise PTG2SharedBlockError(
                "invalid shared PTG zlib block framing or trailing bytes"
            )
    else:
        raise PTG2SharedBlockError(f"unsupported shared PTG block codec: {codec!r}")
    if len(raw_payload) != int(raw_byte_count):
        raise PTG2SharedBlockError(
            f"shared PTG raw block length mismatch: expected {raw_byte_count}, got {len(raw_payload)}"
        )
    return raw_payload


@dataclass(frozen=True)
class SharedBlockPayload:
    block_key: int
    fragment_no: int
    entry_count: int
    payload: bytes


def validate_shared_block_row(
    block_row: Mapping[str, Any],
    *,
    expected_kind: str,
) -> SharedBlockPayload:
    object_kind = str(block_row.get("object_kind") or "")
    codec = str(block_row.get("codec") or "")
    format_version = int(block_row.get("format_version") or 0)
    stored_payload = bytes(block_row.get("payload") or b"")
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
        or bytes(block_row.get("block_hash") or b"") != expected_hash
    ):
        raise PTG2SharedBlockError("shared PTG block identity validation failed")
    mapping_entry_count = int(block_row.get("mapping_entry_count") or 0)
    block_entry_value = block_row.get("block_entry_count")
    if (
        mapping_entry_count < 0
        or (
            block_entry_value is not None
            and mapping_entry_count != int(block_entry_value)
        )
    ):
        raise PTG2SharedBlockError("shared PTG block entry count validation failed")
    raw_payload = decode_shared_block_payload(
        codec=codec,
        payload=stored_payload,
        raw_byte_count=int(block_row.get("raw_byte_count") or 0),
    )
    return SharedBlockPayload(
        block_key=int(block_row.get("block_key") or 0),
        fragment_no=int(block_row.get("fragment_no") or 0),
        entry_count=mapping_entry_count,
        payload=raw_payload,
    )


_validated_payload = validate_shared_block_row


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

    requested_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    if not requested_keys:
        return
    requested_fragments = (
        tuple(sorted({int(fragment_no) for fragment_no in fragment_nos}))
        if fragment_nos is not None
        else ()
    )
    if any(fragment_no < 0 for fragment_no in requested_fragments):
        raise ValueError("shared PTG fragment numbers must be non-negative")
    fragment_filter = (
        "AND mapping.fragment_no = ANY(CAST(:fragment_nos AS integer[]))"
        if fragment_nos is not None
        else ""
    )
    schema = _quote_ident(schema_name)
    statement = text(
        f"""
            SELECT mapping.object_kind, mapping.block_key, mapping.fragment_no,
                   mapping.entry_count AS mapping_entry_count, mapping.block_hash,
                   block.format_version, block.codec,
                   block.entry_count AS block_entry_count,
                   block.raw_byte_count, block.payload
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
        "snapshot_key": int(snapshot_key),
        "generation": PTG2_V3_SHARED_GENERATION,
        "object_kind": str(object_kind),
        "block_keys": requested_keys,
        "fragment_nos": requested_fragments,
    }
    stream = getattr(session, "stream", None)
    query_result = (
        await stream(statement, query_params_by_name)
        if callable(stream)
        else await session.execute(statement, query_params_by_name)
    )
    observed_keys: set[int] = set()
    observed_fragments_by_key: dict[int, set[int]] = {}
    previous_mapping_key: tuple[int, int] | None = None

    async def _validated_rows():
        if hasattr(query_result, "__aiter__"):
            async for raw_row in query_result:
                yield raw_row
            return
        for raw_row in query_result:
            yield raw_row

    async for raw_row in _validated_rows():
        block_payload = validate_shared_block_row(
            _row_mapping(raw_row),
            expected_kind=str(object_kind),
        )
        mapping_key = (block_payload.block_key, block_payload.fragment_no)
        if (
            block_payload.block_key not in requested_keys
            or block_payload.fragment_no < 0
            or (
                fragment_nos is not None
                and block_payload.fragment_no not in requested_fragments
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

    if require_all:
        missing_keys = sorted(set(requested_keys) - observed_keys)
        if missing_keys:
            raise PTG2SharedBlockError(f"shared PTG layout is missing block keys: {missing_keys[:8]}")
        if fragment_nos is not None:
            for block_key in requested_keys:
                missing_fragments = sorted(
                    set(requested_fragments)
                    - observed_fragments_by_key.get(block_key, set())
                )
                if missing_fragments:
                    raise PTG2SharedBlockError(
                        "shared PTG layout is missing fragments: "
                        f"{missing_fragments[:8]}"
                    )


async def fetch_snapshot_source_set_metadata(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: str,
    expected_source_count: int,
) -> dict[str, Any]:
    """Recompute one bounded logical snapshot source-set seal from PostgreSQL."""

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
        return shared_source_set_metadata(
            metadata_row.get("raw_container_sha256")
            for metadata_row in metadata_rows
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
    try:
        object_kind, member_width = _GRAPH_KIND_AND_WIDTH[int(direction)]
    except (KeyError, ValueError) as exc:
        raise ValueError(f"unsupported shared PTG graph direction: {direction!r}") from exc
    if max_members is None:
        member_count_sql = "owner.member_count"
        normalized_max_members = None
    else:
        if isinstance(max_members, bool):
            raise ValueError("shared PTG graph max_members must be non-negative")
        normalized_max_members = int(max_members)
        if normalized_max_members < 0:
            raise ValueError("shared PTG graph max_members must be non-negative")
        member_count_sql = "LEAST(owner.member_count, :max_members)"
    schema = _quote_ident(schema_name)
    graph_params_by_name = {
        "snapshot_key": int(snapshot_key),
        "generation": PTG2_V3_SHARED_GENERATION,
        "direction": int(direction),
        "owner_keys": requested_owner_keys,
        "object_kind": object_kind,
        "member_width": member_width,
        "chunk_bytes": PTG2_V3_GRAPH_CHUNK_BYTES,
    }
    if normalized_max_members is not None:
        graph_params_by_name["max_members"] = normalized_max_members
    query_result = await session.execute(
        text(
            f"""
            SELECT owner.owner_key, owner.first_chunk, owner.member_offset,
                   owner.member_count,
                   {member_count_sql} AS selected_member_count,
                   mapping.object_kind, mapping.block_key,
                   mapping.fragment_no, mapping.entry_count AS mapping_entry_count,
                   mapping.block_hash, block.format_version, block.codec,
                   block.raw_byte_count, block.payload
              FROM {schema}.ptg2_v3_snapshot_layout layout
              JOIN {schema}.ptg2_v3_graph_owner owner
                ON owner.snapshot_key = layout.snapshot_key
              JOIN LATERAL generate_series(
                   owner.first_chunk,
                   owner.first_chunk + CASE
                       WHEN {member_count_sql} = 0 THEN -1
                       ELSE ((owner.member_offset + {member_count_sql} * :member_width - 1)
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
        graph_params_by_name,
    )
    locator_by_owner: dict[int, tuple[int, int, int]] = {}
    chunks_by_owner: dict[int, list[SharedBlockPayload]] = {}
    for raw_row in query_result:
        graph_row = _row_mapping(raw_row)
        owner_key = int(graph_row["owner_key"])
        locator = (
            int(graph_row["member_offset"]),
            int(graph_row["member_count"]),
            int(
                graph_row.get(
                    "selected_member_count",
                    graph_row["member_count"],
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
            _validated_payload(graph_row, expected_kind=object_kind)
        )

    members_by_owner: dict[int, tuple[int, ...]] = {}
    for owner_key in requested_owner_keys:
        locator = locator_by_owner.get(owner_key)
        if locator is None:
            members_by_owner[owner_key] = ()
            continue
        member_offset, _member_count, selected_member_count = locator
        raw_members = b"".join(chunk.payload for chunk in chunks_by_owner.get(owner_key, []))
        member_bytes = selected_member_count * member_width
        selected = raw_members[member_offset : member_offset + member_bytes]
        if len(selected) != member_bytes:
            raise PTG2SharedBlockError(f"shared PTG graph member stream is truncated for owner {owner_key}")
        members_by_owner[owner_key] = tuple(
            int.from_bytes(selected[offset : offset + member_width], "little", signed=False)
            for offset in range(0, len(selected), member_width)
        )
    return members_by_owner
