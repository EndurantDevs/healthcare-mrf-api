# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict cache-free PostgreSQL readers for shared-block PTG V3 serving."""

from __future__ import annotations

import heapq
import re
import zlib
from array import array
from dataclasses import dataclass
from typing import AbstractSet, Any, Callable, Iterable, Mapping

from sqlalchemy import text

from api.ptg2_shared_blocks import (
    PTG2SharedBlockError,
    claim_shared_block_processing,
    claim_shared_logical_payload_processing,
    decode_dense_source_header,
    dense_source_key_bits,
    fetch_shared_blocks,
    fetch_shared_graph_members,
    prepare_shared_block_payload,
    read_strict_uvarint,
    register_shared_logical_payload,
    stream_shared_blocks,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from process.ptg_parts.ptg2_shared_blocks import PTG2_V3_SHARED_GENERATION


@dataclass(frozen=True)
class PTG2ServingBinaryRow:
    """One strict V3 forward row with its dense price key."""

    code_key: int
    provider_set_key: int
    provider_count: int | None
    price_set_global_id_128: str
    source_key: int
    price_key: int | None = None


@dataclass(frozen=True)
class _ForwardLookupOptions:
    """Options for one strict V3 forward-block read."""

    shared_snapshot_key: int
    price_dictionary_item_count: int
    price_dictionary_block_bytes: int
    provider_shard_span: int | None = None
    provider_set_keys: Iterable[int] | None = None
    provider_counts_by_key: Mapping[int, int] | None = None
    source_count: int | None = None
    schema_name: str = "mrf"


@dataclass(frozen=True)
class _ForwardBatchOptions:
    """Options for a strict V3 forward-block batch read."""

    shared_snapshot_key: int
    source_count: int
    price_dictionary_item_count: int
    price_dictionary_block_bytes: int
    provider_shard_span: int | None = None
    provider_set_keys: Iterable[int] | None = None
    provider_set_keys_by_code: Mapping[int, Iterable[int]] | None = None
    source_keys_by_code: Mapping[int, Iterable[int]] | None = None
    occurrence_keys: Iterable[tuple[int, int, int]] | None = None
    provider_counts_by_key: Mapping[int, int] | None = None
    schema_name: str = "mrf"


@dataclass(frozen=True)
class _ForwardFragmentCursor:
    """Ordering state carried across adjacent physical fragments."""

    provider_set_key: int | None = None
    occurrence: tuple[int, int] | None = None


@dataclass(frozen=True)
class _ForwardFragmentValidation:
    """Bounds required to validate one authoritative forward fragment."""

    expected_source_count: int | None
    price_item_count: int | None = None
    provider_key_min: int | None = None
    provider_key_max: int | None = None
    source_filter: frozenset[int] | None = None


@dataclass(frozen=True)
class _ForwardShardVisitOptions:
    code_key: int
    expected_block_keys: Iterable[int]
    provider_set_keys: Iterable[int] | None
    expected_source_count: int | None
    price_item_count: int
    provider_shard_span: int | None = None
    source_keys: Iterable[int] | None = None


@dataclass(frozen=True)
class _ForwardBatchFragmentView:
    """One logical consumer of a request-local physical forward fragment."""

    code_key: int
    block_key: int
    fragment_no: int
    provider_key_min: int
    provider_key_max: int
    provider_filter: frozenset[int] | None
    source_filter: frozenset[int] | None
    occurrence_filter: frozenset[tuple[int, int]] | None
    fragment_row: Mapping[str, Any]


@dataclass(frozen=True)
class _ParsedForwardFragment:
    """Ordering metadata produced by one physical forward payload parse."""

    first_provider_set_key: int
    first_occurrence: tuple[int, int]
    last_cursor: _ForwardFragmentCursor
    source_count: int


@dataclass
class _ForwardFanoutCapture:
    """Retain one physical fragment's first row and logical deliveries."""

    exact_views_by_occurrence: Mapping[
        tuple[int, int],
        tuple[_ForwardBatchFragmentView, ...],
    ]
    fallback_views: tuple[_ForwardBatchFragmentView, ...]
    provider_filter_set: frozenset[int] | None
    retained_by_coordinate: dict[
        tuple[int, int, int],
        list[tuple[int, int, int]],
    ]
    first_provider_set_key: int | None = None
    first_occurrence: tuple[int, int] | None = None

    def __call__(
        self,
        provider_set_key: int,
        price_key: int,
        source_key: int,
    ) -> None:
        if self.first_provider_set_key is None:
            self.capture_first(provider_set_key, price_key, source_key)
        exact_views = self.exact_views_by_occurrence.get(
            (provider_set_key, source_key),
            (),
        )
        for view in exact_views:
            self._retain(view, provider_set_key, price_key, source_key)
        for view in self.fallback_views:
            is_provider_match = (
                view.provider_filter is None
                or provider_set_key in view.provider_filter
            )
            is_source_match = (
                view.source_filter is None or source_key in view.source_filter
            )
            if is_provider_match and is_source_match:
                self._retain(view, provider_set_key, price_key, source_key)

    def capture_first(
        self,
        provider_set_key: int,
        price_key: int,
        source_key: int,
    ) -> None:
        """Record physical ordering state without retaining an unmatched row."""

        if self.first_provider_set_key is None:
            self.first_provider_set_key = provider_set_key
            self.first_occurrence = (price_key, source_key)

    def _retain(
        self,
        view: _ForwardBatchFragmentView,
        provider_set_key: int,
        price_key: int,
        source_key: int,
    ) -> None:
        coordinate = (view.code_key, view.block_key, view.fragment_no)
        self.retained_by_coordinate[coordinate].append(
            (provider_set_key, price_key, source_key)
        )


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND = "by_code_provider_shard_v1"
_SERVING_BINARY_BY_CODE_DICTIONARY_KIND = "by_code_price_dictionary"
_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN_DEFAULT = 1024
_SERVING_BINARY_BY_CODE_BLOCK_SPAN = 1 << 31
_SERVING_BINARY_MAX_DENSE_KEY = 2**31 - 1


def _required_shared_snapshot_key(shared_snapshot_key: int | None) -> int:
    if isinstance(shared_snapshot_key, bool):
        raise PTG2ManifestArtifactError("PTG2 shared snapshot key is missing")
    try:
        normalized_key = int(shared_snapshot_key)
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError("PTG2 shared snapshot key is missing") from exc
    if normalized_key <= 0:
        raise PTG2ManifestArtifactError("PTG2 shared snapshot key must be positive")
    return normalized_key


def _safe_qualified_table_name(value: str) -> str:
    """Validate names used by out-of-slice V3 decoder modules."""

    parts = str(value or "").split(".", 1)
    if (
        len(parts) != 2
        or not _IDENTIFIER_RE.fullmatch(parts[0])
        or not _IDENTIFIER_RE.fullmatch(parts[1])
    ):
        raise PTG2ManifestArtifactError(
            f"unsafe PTG2 serving binary table name: {value!r}"
        )
    return f"{_quote_ident(parts[0])}.{_quote_ident(parts[1])}"


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _normalized_code_key(code_key: int) -> int:
    if isinstance(code_key, bool):
        raise PTG2ManifestArtifactError("PTG2 v3 code key is out of range")
    normalized = int(code_key)
    if normalized < 0 or normalized > _SERVING_BINARY_MAX_DENSE_KEY:
        raise PTG2ManifestArtifactError("PTG2 v3 code key is out of range")
    return normalized


def _normalized_price_item_count(item_count: int) -> int:
    if isinstance(item_count, bool) or int(item_count) < 0:
        raise PTG2ManifestArtifactError(
            "PTG2 shared price dictionary item count is invalid"
        )
    return int(item_count)


def _normalized_provider_set_filter(
    provider_set_keys: Iterable[int] | None,
) -> tuple[int, ...] | None:
    if provider_set_keys is None:
        return None
    normalized_keys: set[int] = set()
    for provider_set_key in provider_set_keys:
        if isinstance(provider_set_key, bool):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 provider-set key is out of range"
            )
        normalized_key = int(provider_set_key)
        if normalized_key < 0 or normalized_key > _SERVING_BINARY_MAX_DENSE_KEY:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 provider-set key is out of range"
            )
        normalized_keys.add(normalized_key)
    return tuple(sorted(normalized_keys))


def _normalized_provider_shard_span(provider_shard_span: int | None) -> int:
    normalized_span = (
        _SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN_DEFAULT
        if provider_shard_span is None
        else int(provider_shard_span)
    )
    if (
        isinstance(provider_shard_span, bool)
        or normalized_span <= 0
        or normalized_span > 1 << 24
        or normalized_span & (normalized_span - 1)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider shard span must be a bounded power of two"
        )
    return normalized_span


def _forward_code_block_bounds(code_key: int) -> tuple[int, int]:
    normalized_code_key = _normalized_code_key(code_key)
    lower_bound = normalized_code_key * _SERVING_BINARY_BY_CODE_BLOCK_SPAN
    return lower_bound, lower_bound + _SERVING_BINARY_BY_CODE_BLOCK_SPAN


def _forward_provider_shard_block_key(
    code_key: int,
    provider_set_key: int,
    provider_shard_span: int | None = None,
) -> int:
    normalized_provider_keys = _normalized_provider_set_filter(
        (provider_set_key,)
    )
    assert normalized_provider_keys is not None
    normalized_span = _normalized_provider_shard_span(provider_shard_span)
    lower_bound, _upper_bound = _forward_code_block_bounds(code_key)
    return lower_bound + (normalized_provider_keys[0] // normalized_span)


def _forward_provider_range_for_block(
    code_key: int,
    block_key: int,
    provider_shard_span: int | None = None,
) -> tuple[int, int]:
    lower_bound, upper_bound = _forward_code_block_bounds(code_key)
    normalized_block_key = int(block_key)
    if normalized_block_key < lower_bound or normalized_block_key >= upper_bound:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-shard block key is outside its code range"
        )
    shard_no = normalized_block_key - lower_bound
    normalized_span = _normalized_provider_shard_span(provider_shard_span)
    if shard_no > _SERVING_BINARY_MAX_DENSE_KEY // normalized_span:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 provider-shard block key has an invalid shard number"
        )
    provider_key_min = shard_no * normalized_span
    return (
        provider_key_min,
        provider_key_min + normalized_span,
    )


def _computed_forward_shard_keys(
    code_keys: Iterable[int],
    provider_set_keys: tuple[int, ...],
    provider_shard_span: int | None = None,
) -> dict[int, tuple[int, ...]]:
    normalized_span = _normalized_provider_shard_span(provider_shard_span)
    shard_numbers = tuple(
        sorted(
            {
                provider_set_key // normalized_span
                for provider_set_key in provider_set_keys
            }
        )
    )
    shard_keys_by_code: dict[int, tuple[int, ...]] = {}
    for code_key in code_keys:
        normalized_code_key = _normalized_code_key(code_key)
        shard_keys_by_code[normalized_code_key] = tuple(
            _forward_provider_shard_block_key(
                normalized_code_key,
                shard_no * normalized_span,
                normalized_span,
            )
            for shard_no in shard_numbers
        )
    return shard_keys_by_code


async def _discover_forward_shard_keys(
    session: Any,
    *,
    shared_snapshot_key: int,
    schema_name: str,
    code_keys: Iterable[int],
    provider_shard_span: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Discover every immutable provider shard in exact code-key ranges."""

    normalized_code_keys = tuple(
        sorted({_normalized_code_key(code_key) for code_key in code_keys})
    )
    if not normalized_code_keys:
        return {}
    schema = _quote_ident(schema_name)
    shard_query_result = await session.execute(
        text(
            f"""
            WITH requested_code(code_key) AS (
                SELECT unnest(CAST(:code_keys AS bigint[]))
            )
            SELECT DISTINCT requested_code.code_key, mapping.block_key
              FROM requested_code
              JOIN {schema}.ptg2_v3_snapshot_layout layout
                ON layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
              JOIN {schema}.ptg2_v3_snapshot_block mapping
                ON mapping.snapshot_key = layout.snapshot_key
               AND mapping.object_kind = :object_kind
               AND mapping.block_key >=
                   requested_code.code_key * :code_block_span
               AND mapping.block_key <
                   (requested_code.code_key + 1) * :code_block_span
             ORDER BY requested_code.code_key, mapping.block_key
            """
        ),
        {
            "snapshot_key": _required_shared_snapshot_key(
                shared_snapshot_key
            ),
            "generation": PTG2_V3_SHARED_GENERATION,
            "object_kind": _SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            "code_keys": normalized_code_keys,
            "code_block_span": _SERVING_BINARY_BY_CODE_BLOCK_SPAN,
        },
    )
    requested_code_set = set(normalized_code_keys)
    shard_keys_by_code: dict[int, list[int]] = {
        code_key: [] for code_key in normalized_code_keys
    }
    observed_pairs: set[tuple[int, int]] = set()
    for raw_row in shard_query_result:
        shard_row = _row_mapping(raw_row)
        code_key = int(shard_row.get("code_key"))
        block_key = int(shard_row.get("block_key"))
        if code_key not in requested_code_set:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 shard discovery returned an unexpected code key"
            )
        _forward_provider_range_for_block(
            code_key,
            block_key,
            provider_shard_span,
        )
        pair = (code_key, block_key)
        if pair in observed_pairs:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 shard discovery returned a duplicate block key"
            )
        observed_pairs.add(pair)
        shard_keys_by_code[code_key].append(block_key)
    return {
        code_key: tuple(block_keys)
        for code_key, block_keys in shard_keys_by_code.items()
    }


async def _forward_shard_keys_for_read(
    session: Any,
    options: _ForwardLookupOptions | _ForwardBatchOptions,
    code_keys: Iterable[int],
) -> tuple[dict[int, tuple[int, ...]], tuple[int, ...] | None, bool]:
    normalized_code_keys = tuple(
        sorted({_normalized_code_key(code_key) for code_key in code_keys})
    )
    provider_filter = _normalized_provider_set_filter(
        options.provider_set_keys
    )
    provider_shard_span = _normalized_provider_shard_span(
        options.provider_shard_span
    )
    if provider_filter is not None:
        return (
            _computed_forward_shard_keys(
                normalized_code_keys,
                provider_filter,
                provider_shard_span,
            ),
            provider_filter,
            False,
        )
    return (
        await _discover_forward_shard_keys(
            session,
            shared_snapshot_key=options.shared_snapshot_key,
            schema_name=options.schema_name,
            code_keys=normalized_code_keys,
            provider_shard_span=provider_shard_span,
        ),
        None,
        True,
    )


def _id_text(raw: bytes | bytearray | memoryview) -> str:
    value = bytes(raw)
    if len(value) != 16:
        raise PTG2ManifestArtifactError(
            f"128-bit ids must be 16 bytes; got {len(value)}"
        )
    return value.hex()


def _read_uvarint(
    payload: bytes | bytearray | memoryview, offset: int
) -> tuple[int, int]:
    cursor = int(offset)
    result = 0
    shift = 0
    while True:
        if cursor >= len(payload):
            raise PTG2ManifestArtifactError("serving block ended inside a uvarint")
        byte = int(payload[cursor])
        cursor += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, cursor
        shift += 7
        if shift > 63:
            raise PTG2ManifestArtifactError("serving block uvarint is too large")


def _decode_serving_binary_payload(record: Mapping[str, Any]) -> bytes:
    decoded_payload = record.get("_decoded_payload")
    if decoded_payload is not None:
        return bytes(decoded_payload)
    payload = bytes(record.get("payload") or b"")
    compression = str(record.get("payload_compression") or "none").strip().lower()
    if compression in {"", "none"}:
        raw_payload = payload
    elif compression == "zlib":
        try:
            raw_payload = zlib.decompress(payload)
        except zlib.error as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 serving block payload is corrupt"
            ) from exc
    else:
        raise PTG2ManifestArtifactError(
            f"unsupported PTG2 serving block payload compression: {compression}"
        )
    expected_raw_bytes = record.get("raw_payload_bytes")
    if (
        expected_raw_bytes is not None
        and int(expected_raw_bytes) >= 0
        and len(raw_payload) != int(expected_raw_bytes)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 serving block raw payload byte count mismatch"
        )
    return raw_payload


async def _fetch_shared_block_fragments(
    session: Any,
    *,
    shared_snapshot_key: int,
    schema_name: str,
    artifact_kind: str,
    block_keys: Iterable[int],
    fragment_nos: Iterable[int] | None = None,
    require_all: bool = False,
) -> list[dict[str, Any]]:
    """Read selected immutable fragments without retaining them in process memory."""

    try:
        payloads_by_key = await fetch_shared_blocks(
            session,
            schema_name=schema_name,
            snapshot_key=_required_shared_snapshot_key(shared_snapshot_key),
            object_kind=artifact_kind,
            block_keys=block_keys,
            fragment_nos=fragment_nos,
            require_all=require_all or fragment_nos is not None,
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc
    fragment_rows: list[dict[str, Any]] = []
    for block_key in sorted(payloads_by_key):
        for fragment in payloads_by_key[block_key]:
            fragment_rows.append(
                {
                    "block_key": block_key,
                    "block_no": fragment.fragment_no,
                    "entry_count": fragment.entry_count,
                    "payload": fragment.payload,
                    "payload_compression": "none",
                    "raw_payload_bytes": len(fragment.payload),
                    "_decoded_payload": fragment.payload,
                    "_block_hash": fragment.block_hash,
                }
            )
    return fragment_rows


_shared_serving_binary_payload_rows_for_keys = _fetch_shared_block_fragments


async def _serving_binary_payload_rows(
    session: Any,
    *,
    shared_snapshot_key: int,
    artifact_kind: str,
    block_key: int,
    schema_name: str = "mrf",
) -> list[dict[str, Any]]:
    return await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=artifact_kind,
        block_keys=(int(block_key),),
    )


async def _serving_binary_payload_rows_for_keys(
    session: Any,
    _unsupported_table_name: str | None = None,
    *,
    artifact_kind: str,
    block_keys: Iterable[int],
    shared_snapshot_key: int | None = None,
    schema_name: str = "mrf",
) -> list[dict[str, Any]]:
    """Shared-only adapter retained for the decoder modules outside this slice."""

    if _unsupported_table_name:
        raise PTG2ManifestArtifactError(
            "per-snapshot serving binary tables are unsupported; reimport the snapshot"
        )
    return await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=_required_shared_snapshot_key(shared_snapshot_key),
        schema_name=schema_name,
        artifact_kind=artifact_kind,
        block_keys=block_keys,
    )


def _validate_dictionary_keys(item_keys: Iterable[int], item_count: int) -> None:
    if any(item_key < 0 or item_key >= item_count for item_key in item_keys):
        raise PTG2ManifestArtifactError("PTG2 serving dictionary key is out of range")


def _shared_dictionary_values_for_keys(
    fragment_rows: Iterable[Mapping[str, Any]],
    requested_keys: tuple[int, ...],
    *,
    item_count: int,
    entries_per_fragment: int,
    schema_name: str,
) -> dict[int, str]:
    """Decode each selected physical dictionary fragment once and fan aliases out."""

    fragment_rows = tuple(fragment_rows)
    ordered_fragments = sorted(
        fragment_rows,
        key=lambda fragment_row: int(fragment_row.get("block_no") or 0),
    )
    if not ordered_fragments:
        raise PTG2ManifestArtifactError("PTG2 shared serving dictionary is missing")
    _validate_dictionary_keys(requested_keys, item_count)
    required_fragments = {
        item_key // entries_per_fragment for item_key in requested_keys
    }
    values_by_key: dict[int, str] = {}
    observed_fragments: set[int] = set()
    for block_hash, physical_aliases in _dictionary_fragments_by_hash(
        ordered_fragments
    ).items():
        alias_values, alias_fragment_nos = _dictionary_alias_values(
            block_hash,
            physical_aliases,
            requested_keys,
            required_fragments,
            item_count=item_count,
            entries_per_fragment=entries_per_fragment,
            schema_name=schema_name,
        )
        if observed_fragments.intersection(alias_fragment_nos):
            raise PTG2ManifestArtifactError(
                "PTG2 shared serving dictionary returned an unexpected fragment"
            )
        observed_fragments.update(alias_fragment_nos)
        values_by_key.update(alias_values)
    if observed_fragments != required_fragments:
        raise PTG2ManifestArtifactError(
            "PTG2 shared serving dictionary is missing a requested fragment"
        )
    if set(values_by_key) != set(requested_keys):
        raise PTG2ManifestArtifactError("PTG2 serving dictionary key is out of range")
    return values_by_key


def _dictionary_fragments_by_hash(
    ordered_fragments: Iterable[Mapping[str, Any]],
) -> dict[bytes, list[Mapping[str, Any]]]:
    """Require and group physical identities before dictionary interpretation."""

    fragments_by_hash: dict[bytes, list[Mapping[str, Any]]] = {}
    for fragment_row in ordered_fragments:
        raw_block_hash = fragment_row.get("_block_hash")
        if not isinstance(raw_block_hash, (bytes, bytearray, memoryview)):
            raise PTG2ManifestArtifactError(
                "PTG2 shared serving dictionary fragment is missing its physical block identity"
            )
        block_hash = bytes(raw_block_hash)
        if not block_hash:
            raise PTG2ManifestArtifactError(
                "PTG2 shared serving dictionary fragment is missing its physical block identity"
            )
        fragments_by_hash.setdefault(block_hash, []).append(fragment_row)
    return fragments_by_hash


def _dictionary_alias_values(
    block_hash: bytes,
    physical_aliases: list[Mapping[str, Any]],
    requested_keys: tuple[int, ...],
    required_fragments: set[int],
    *,
    item_count: int,
    entries_per_fragment: int,
    schema_name: str,
) -> tuple[dict[int, str], set[int]]:
    """Decode one physical dictionary payload and rebase all logical aliases."""

    claim_shared_block_processing(schema_name=schema_name, block_hash=block_hash)
    representative = physical_aliases[0]
    fragment_bytes = _decode_serving_binary_payload(representative)
    entry_count = int(representative.get("entry_count") or 0)
    if any(
        int(alias.get("entry_count") or 0) != entry_count
        for alias in physical_aliases[1:]
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 shared serving dictionary fragment is malformed"
        )
    requested_offsets = {
        item_key % entries_per_fragment
        for fragment_row in physical_aliases
        for item_key in requested_keys
        if int(fragment_row.get("block_no") or 0)
        == item_key // entries_per_fragment
    }
    value_by_offset = {
        item_offset: _id_text(
            fragment_bytes[item_offset * 16 : (item_offset + 1) * 16]
        )
        for item_offset in requested_offsets
    }
    values_by_key: dict[int, str] = {}
    fragment_nos: set[int] = set()
    for fragment_row in physical_aliases:
        fragment_no = int(fragment_row.get("block_no") or 0)
        if fragment_no in fragment_nos or fragment_no not in required_fragments:
            raise PTG2ManifestArtifactError(
                "PTG2 shared serving dictionary returned an unexpected fragment"
            )
        fragment_nos.add(fragment_no)
        item_offset = fragment_no * entries_per_fragment
        expected_entry_count = min(entries_per_fragment, item_count - item_offset)
        if (
            expected_entry_count <= 0
            or entry_count != expected_entry_count
            or len(fragment_bytes) != entry_count * 16
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 shared serving dictionary fragment is malformed"
            )
        for item_key in requested_keys:
            if item_offset <= item_key < item_offset + entry_count:
                values_by_key[item_key] = value_by_offset[item_key - item_offset]
    return values_by_key, fragment_nos


async def _serving_binary_dictionary_values_for_keys(
    session: Any,
    *,
    shared_snapshot_key: int,
    artifact_kind: str,
    item_keys: Iterable[int],
    item_count: int,
    block_bytes: int,
    schema_name: str = "mrf",
) -> dict[int, str]:
    requested_keys = tuple(sorted({int(item_key) for item_key in item_keys}))
    if not requested_keys:
        return {}
    if isinstance(item_count, bool) or int(item_count) < 0:
        raise PTG2ManifestArtifactError(
            "PTG2 shared price dictionary item count is invalid"
        )
    if (
        isinstance(block_bytes, bool)
        or int(block_bytes) < 16
        or int(block_bytes) % 16 != 0
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 shared price dictionary block size is invalid"
        )
    normalized_item_count = int(item_count)
    entries_per_fragment = int(block_bytes) // 16
    _validate_dictionary_keys(requested_keys, normalized_item_count)
    requested_fragments = tuple(
        sorted({item_key // entries_per_fragment for item_key in requested_keys})
    )
    fragment_rows = await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=artifact_kind,
        block_keys=(0,),
        fragment_nos=requested_fragments,
    )
    return _shared_dictionary_values_for_keys(
        fragment_rows,
        requested_keys,
        item_count=normalized_item_count,
        entries_per_fragment=entries_per_fragment,
        schema_name=schema_name,
    )


def _decode_forward_occurrences(
    fragment_bytes: bytes,
    cursor: int,
    *,
    source_count: int,
    source_bits: int,
) -> tuple[list[tuple[int, int]], int]:
    """Decode and validate one provider set's ordered price/source pairs."""

    occurrences: list[tuple[int, int]] = []
    cursor, _last_occurrence = _visit_forward_occurrences(
        fragment_bytes,
        cursor,
        source_count=source_count,
        source_bits=source_bits,
        occurrence_consumer=lambda price_key, source_key: occurrences.append(
            (price_key, source_key)
        ),
    )
    return occurrences, cursor


def _dense_source_vector_view(
    payload: bytes,
    offset: int,
    *,
    entry_count: int,
    source_count: int,
    source_bits: int,
) -> tuple[memoryview, int]:
    """Validate one packed source vector without expanding it."""

    if int(source_bits) != dense_source_key_bits(source_count):
        raise PTG2SharedBlockError(
            "PTG2 v3 grouped by-code source vector width is invalid"
        )
    total_bits = int(entry_count) * int(source_bits)
    byte_count = (total_bits + 7) // 8
    end = int(offset) + byte_count
    if end > len(payload):
        raise PTG2SharedBlockError(
            "PTG2 v3 grouped by-code source vector is truncated"
        )
    encoded = memoryview(payload)[int(offset) : end]
    if total_bits % 8 and encoded and int(encoded[-1]) >> (total_bits % 8):
        raise PTG2SharedBlockError(
            "PTG2 v3 grouped by-code source vector has non-zero padding bits"
        )
    return encoded, end


def _dense_source_key_at(
    encoded: memoryview,
    entry_index: int,
    *,
    source_count: int,
    source_bits: int,
) -> int:
    source_key = 0
    bit_offset = int(entry_index) * int(source_bits)
    for source_bit in range(int(source_bits)):
        if int(encoded[bit_offset // 8]) & (1 << (bit_offset % 8)):
            source_key |= 1 << source_bit
        bit_offset += 1
    if source_key >= int(source_count):
        raise PTG2SharedBlockError(
            "PTG2 v3 grouped by-code source key is out of range"
        )
    return source_key


def _visit_single_source_forward_occurrences(
    fragment_bytes: bytes,
    cursor: int,
    *,
    price_key_count: int,
    occurrence_consumer: Callable[[int, int], None] | None,
    first_occurrence_consumer: Callable[[int, int], None] | None,
    price_item_count: int | None,
    previous_occurrence: tuple[int, int] | None,
) -> tuple[int, tuple[int, int]]:
    """Validate and deliver zero-bit source occurrences in one pass."""

    for price_index in range(price_key_count):
        price_key, cursor = read_strict_uvarint(fragment_bytes, cursor)
        if price_key > 2**32 - 1 or (
            price_item_count is not None
            and price_key >= int(price_item_count)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code price key is out of range"
            )
        occurrence = (price_key, 0)
        if previous_occurrence is not None and occurrence < previous_occurrence:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code occurrences are not ordered"
            )
        previous_occurrence = occurrence
        if price_index == 0 and first_occurrence_consumer is not None:
            first_occurrence_consumer(price_key, 0)
        if occurrence_consumer is not None:
            occurrence_consumer(price_key, 0)
    _encoded_sources, source_vector_end = _dense_source_vector_view(
        fragment_bytes,
        cursor,
        entry_count=price_key_count,
        source_count=1,
        source_bits=0,
    )
    return source_vector_end, previous_occurrence


def _validated_forward_price_key_array(
    fragment_bytes: bytes,
    cursor: int,
    *,
    price_key_count: int,
    price_item_count: int | None,
) -> tuple[array, int]:
    """Decode bounded price keys needed by a packed multi-source vector."""

    price_key_array = array("I")
    for _price_index in range(price_key_count):
        price_key, cursor = read_strict_uvarint(fragment_bytes, cursor)
        if price_key > 2**32 - 1 or (
            price_item_count is not None
            and price_key >= int(price_item_count)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code price key is out of range"
            )
        price_key_array.append(price_key)
    return price_key_array, cursor


def _visit_forward_occurrences(
    fragment_bytes: bytes,
    cursor: int,
    *,
    source_count: int,
    source_bits: int,
    occurrence_consumer: Callable[[int, int], None] | None,
    first_occurrence_consumer: Callable[[int, int], None] | None = None,
    price_item_count: int | None = None,
    previous_occurrence: tuple[int, int] | None = None,
) -> tuple[int, tuple[int, int]]:
    """Validate occurrences with constant temporary memory."""

    price_key_count, cursor = read_strict_uvarint(fragment_bytes, cursor)
    if price_key_count <= 0:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code occurrence count is invalid"
        )
    if source_count == 1 and source_bits == 0:
        return _visit_single_source_forward_occurrences(
            fragment_bytes,
            cursor,
            price_key_count=price_key_count,
            occurrence_consumer=occurrence_consumer,
            first_occurrence_consumer=first_occurrence_consumer,
            price_item_count=price_item_count,
            previous_occurrence=previous_occurrence,
        )
    price_key_array, cursor = _validated_forward_price_key_array(
        fragment_bytes,
        cursor,
        price_key_count=price_key_count,
        price_item_count=price_item_count,
    )
    encoded_sources, source_vector_end = _dense_source_vector_view(
        fragment_bytes,
        cursor,
        entry_count=price_key_count,
        source_count=source_count,
        source_bits=source_bits,
    )
    for occurrence_index, price_key in enumerate(price_key_array):
        source_key = _dense_source_key_at(
            encoded_sources,
            occurrence_index,
            source_count=source_count,
            source_bits=source_bits,
        )
        occurrence = (price_key, source_key)
        if previous_occurrence is not None and occurrence < previous_occurrence:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code occurrences are not ordered"
            )
        previous_occurrence = occurrence
        if occurrence_index == 0 and first_occurrence_consumer is not None:
            first_occurrence_consumer(price_key, source_key)
        if occurrence_consumer is not None:
            occurrence_consumer(price_key, source_key)
    return source_vector_end, previous_occurrence


def _visit_forward_fragment_unchecked(
    fragment_row: Mapping[str, Any],
    *,
    provider_filter: AbstractSet[int] | None,
    fragment_cursor: _ForwardFragmentCursor,
    validation: _ForwardFragmentValidation,
    occurrence_consumer: Callable[[int, int, int], None],
    first_occurrence_consumer: Callable[[int, int, int], None] | None = None,
) -> tuple[_ForwardFragmentCursor, int]:
    """Validate one grouped fragment and visit selected occurrences."""

    fragment_bytes = _decode_serving_binary_payload(fragment_row)
    source_count, source_bits, cursor = decode_dense_source_header(
        fragment_bytes,
        0,
        format_version=2,
        expected_source_count=validation.expected_source_count,
    )
    previous_provider_set_key = fragment_cursor.provider_set_key
    previous_occurrence = fragment_cursor.occurrence
    provider_set_key = 0
    entry_count = int(fragment_row.get("entry_count") or 0)
    if entry_count <= 0:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment has an invalid entry count"
        )
    for provider_index in range(entry_count):
        provider_delta, cursor = read_strict_uvarint(fragment_bytes, cursor)
        provider_set_key += provider_delta
        if (
            previous_provider_set_key is not None
            and provider_set_key < previous_provider_set_key
        ) or provider_set_key > 2**31 - 1:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code provider sets are not ordered"
            )
        if (
            validation.provider_key_min is not None
            and provider_set_key < int(validation.provider_key_min)
        ) or (
            validation.provider_key_max is not None
            and provider_set_key >= int(validation.provider_key_max)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 provider set is outside its forward shard"
            )

        is_provider_match = (
            provider_filter is None or provider_set_key in provider_filter
        )

        def _consume(price_key: int, source_key: int) -> None:
            is_source_match = (
                validation.source_filter is None
                or source_key in validation.source_filter
            )
            if is_source_match:
                occurrence_consumer(provider_set_key, price_key, source_key)

        def _capture_first(price_key: int, source_key: int) -> None:
            if first_occurrence_consumer is not None:
                first_occurrence_consumer(
                    provider_set_key,
                    price_key,
                    source_key,
                )

        is_provider_continuation = (
            previous_provider_set_key == provider_set_key
        )
        cursor, previous_occurrence = _visit_forward_occurrences(
            fragment_bytes,
            cursor,
            source_count=source_count,
            source_bits=source_bits,
            occurrence_consumer=(_consume if is_provider_match else None),
            first_occurrence_consumer=(
                _capture_first
                if first_occurrence_consumer is not None
                and provider_index == 0
                else None
            ),
            price_item_count=validation.price_item_count,
            previous_occurrence=(
                previous_occurrence if is_provider_continuation else None
            ),
        )
        previous_provider_set_key = provider_set_key
    if cursor != len(fragment_bytes):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment has trailing bytes"
        )
    if previous_occurrence is None:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment has no occurrences"
        )
    return (
        _ForwardFragmentCursor(
            provider_set_key=provider_set_key,
            occurrence=previous_occurrence,
        ),
        source_count,
    )


def _decode_forward_fragment_unchecked(
    fragment_row: Mapping[str, Any],
    *,
    provider_filter: AbstractSet[int] | None,
    expected_source_count: int | None,
    previous_provider_set_key: int | None,
    previous_occurrence: tuple[int, int] | None,
    provider_key_min: int | None = None,
    provider_key_max: int | None = None,
    price_item_count: int | None = None,
) -> tuple[list[tuple[int, int, int]], int, tuple[int, int], int]:
    """Decode one grouped strict V3 forward fragment."""

    decoded_keys: list[tuple[int, int, int]] = []
    fragment_cursor, source_count = _visit_forward_fragment_unchecked(
        fragment_row,
        provider_filter=provider_filter,
        fragment_cursor=_ForwardFragmentCursor(
            provider_set_key=previous_provider_set_key,
            occurrence=previous_occurrence,
        ),
        validation=_ForwardFragmentValidation(
            expected_source_count=expected_source_count,
            price_item_count=price_item_count,
            provider_key_min=provider_key_min,
            provider_key_max=provider_key_max,
        ),
        occurrence_consumer=lambda provider_key, price_key, source_key: (
            decoded_keys.append((provider_key, price_key, source_key))
        ),
    )
    if fragment_cursor.provider_set_key is None or fragment_cursor.occurrence is None:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment has no ordering state"
        )
    return (
        decoded_keys,
        fragment_cursor.provider_set_key,
        fragment_cursor.occurrence,
        source_count,
    )


_decode_serving_binary_by_code_record_unchecked = _decode_forward_fragment_unchecked


def _decode_serving_binary_by_code_record(
    fragment_row: Mapping[str, Any],
    *,
    provider_filter: AbstractSet[int] | None,
    expected_source_count: int | None,
    previous_provider_set_key: int | None,
    previous_occurrence: tuple[int, int] | None,
    provider_key_min: int | None = None,
    provider_key_max: int | None = None,
    price_item_count: int | None = None,
) -> tuple[list[tuple[int, int, int]], int, tuple[int, int], int]:
    try:
        return _decode_serving_binary_by_code_record_unchecked(
            fragment_row,
            provider_filter=provider_filter,
            expected_source_count=expected_source_count,
            previous_provider_set_key=previous_provider_set_key,
            previous_occurrence=previous_occurrence,
            provider_key_min=provider_key_min,
            provider_key_max=provider_key_max,
            price_item_count=price_item_count,
        )
    except PTG2ManifestArtifactError:
        raise
    except Exception as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment is corrupt"
        ) from exc


def _visit_serving_binary_by_code_record(
    fragment_row: Mapping[str, Any],
    *,
    provider_filter: AbstractSet[int] | None,
    fragment_cursor: _ForwardFragmentCursor,
    validation: _ForwardFragmentValidation,
    occurrence_consumer: Callable[[int, int, int], None],
    first_occurrence_consumer: Callable[[int, int, int], None] | None = None,
) -> tuple[_ForwardFragmentCursor, int]:
    try:
        return _visit_forward_fragment_unchecked(
            fragment_row,
            provider_filter=provider_filter,
            fragment_cursor=fragment_cursor,
            validation=validation,
            occurrence_consumer=occurrence_consumer,
            first_occurrence_consumer=first_occurrence_consumer,
        )
    except PTG2ManifestArtifactError:
        raise
    except Exception as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment is corrupt"
        ) from exc


def _decode_serving_binary_code_records(
    fragment_rows: Iterable[Mapping[str, Any]],
    *,
    provider_set_keys: Iterable[int] | None,
    expected_source_count: int | None = None,
    provider_key_min: int | None = None,
    provider_key_max: int | None = None,
    price_item_count: int | None = None,
    source_keys: Iterable[int] | None = None,
) -> list[tuple[int, int, int]]:
    decoded_keys, _source_count = (
        _decode_code_records_with_source_count(
            fragment_rows,
            provider_set_keys=provider_set_keys,
            expected_source_count=expected_source_count,
            provider_key_min=provider_key_min,
            provider_key_max=provider_key_max,
            price_item_count=price_item_count,
            source_keys=source_keys,
        )
    )
    return decoded_keys


def _decode_code_records_with_source_count(
    fragment_rows: Iterable[Mapping[str, Any]],
    *,
    provider_set_keys: Iterable[int] | None,
    expected_source_count: int | None = None,
    provider_key_min: int | None = None,
    provider_key_max: int | None = None,
    price_item_count: int | None = None,
    source_keys: Iterable[int] | None = None,
) -> tuple[list[tuple[int, int, int]], int | None]:
    decoded_keys: list[tuple[int, int, int]] = []
    observed_source_count = _visit_code_records_with_source_count(
        fragment_rows,
        provider_set_keys=provider_set_keys,
        expected_source_count=expected_source_count,
        provider_key_min=provider_key_min,
        provider_key_max=provider_key_max,
        price_item_count=price_item_count,
        source_keys=source_keys,
        occurrence_consumer=lambda provider_key, price_key, source_key: (
            decoded_keys.append((provider_key, price_key, source_key))
        ),
    )
    return decoded_keys, observed_source_count


def _normalized_source_filter(
    source_keys: Iterable[int] | None,
    expected_source_count: int | None,
) -> frozenset[int] | None:
    if source_keys is None:
        return None
    normalized_source_keys: set[int] = set()
    for raw_source_key in source_keys:
        if isinstance(raw_source_key, bool):
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filter contains an invalid source key"
            )
        try:
            source_key = int(raw_source_key)
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filter contains an invalid source key"
            ) from exc
        if source_key < 0 or (
            expected_source_count is not None
            and source_key >= int(expected_source_count)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filter contains an invalid source key"
            )
        normalized_source_keys.add(source_key)
    return frozenset(normalized_source_keys)


def _ordered_forward_fragments(
    fragment_rows: Iterable[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    ordered_fragments = tuple(
        sorted(
            fragment_rows,
            key=lambda fragment_row: int(fragment_row.get("block_no") or 0),
        )
    )
    fragment_numbers = tuple(
        int(fragment_row.get("block_no") or 0)
        for fragment_row in ordered_fragments
    )
    if fragment_numbers != tuple(range(len(ordered_fragments))):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragments are not contiguous"
        )
    return ordered_fragments


def _visit_code_records_with_source_count(
    fragment_rows: Iterable[Mapping[str, Any]],
    *,
    provider_set_keys: Iterable[int] | None,
    occurrence_consumer: Callable[[int, int, int], None],
    expected_source_count: int | None = None,
    provider_key_min: int | None = None,
    provider_key_max: int | None = None,
    price_item_count: int | None = None,
    source_keys: Iterable[int] | None = None,
) -> int | None:
    """Validate ordered fragments while sending selected rows to one sink."""

    normalized_provider_filter = _normalized_provider_set_filter(
        provider_set_keys
    )
    provider_filter = (
        set(normalized_provider_filter)
        if normalized_provider_filter is not None
        else None
    )
    source_filter = _normalized_source_filter(
        source_keys,
        expected_source_count,
    )
    largest_source_key = max(source_filter) if source_filter else None
    ordered_fragments = _ordered_forward_fragments(fragment_rows)
    fragment_validation = _ForwardFragmentValidation(
        expected_source_count=expected_source_count,
        price_item_count=price_item_count,
        provider_key_min=provider_key_min,
        provider_key_max=provider_key_max,
        source_filter=source_filter,
    )
    fragment_cursor = _ForwardFragmentCursor()
    observed_source_count: int | None = None
    for fragment_row in ordered_fragments:
        fragment_cursor, fragment_source_count = (
            _visit_serving_binary_by_code_record(
                fragment_row,
                provider_filter=provider_filter,
                fragment_cursor=fragment_cursor,
                validation=fragment_validation,
                occurrence_consumer=occurrence_consumer,
            )
        )
        if (
            largest_source_key is not None
            and largest_source_key >= fragment_source_count
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filter contains an invalid source key"
            )
        if observed_source_count not in (None, fragment_source_count):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code fragments disagree on source_count"
            )
        observed_source_count = fragment_source_count
    return observed_source_count


def _decode_forward_shards_for_code(
    fragment_rows: Iterable[Mapping[str, Any]],
    *,
    code_key: int,
    expected_block_keys: Iterable[int],
    provider_set_keys: Iterable[int] | None,
    expected_source_count: int | None,
    price_item_count: int,
    provider_shard_span: int | None = None,
    source_keys: Iterable[int] | None = None,
) -> list[tuple[int, int, int]]:
    decoded_keys: list[tuple[int, int, int]] = []
    _visit_forward_shards_for_code(
        fragment_rows,
        options=_ForwardShardVisitOptions(
            code_key=code_key,
            expected_block_keys=expected_block_keys,
            provider_set_keys=provider_set_keys,
            expected_source_count=expected_source_count,
            price_item_count=price_item_count,
            provider_shard_span=provider_shard_span,
            source_keys=source_keys,
        ),
        occurrence_consumer=lambda provider_key, price_key, source_key: (
            decoded_keys.append((provider_key, price_key, source_key))
        ),
    )
    return decoded_keys


def _visit_forward_shards_for_code(
    fragment_rows: Iterable[Mapping[str, Any]],
    *,
    options: _ForwardShardVisitOptions,
    occurrence_consumer: Callable[[int, int, int], None],
) -> None:
    """Validate one code's selected shards and stream retained occurrences."""

    normalized_code_key = _normalized_code_key(options.code_key)
    expected_keys = tuple(
        sorted({int(key) for key in options.expected_block_keys})
    )
    expected_key_set = set(expected_keys)
    fragments_by_block: dict[int, list[Mapping[str, Any]]] = {}
    for fragment_row in fragment_rows:
        block_key = int(fragment_row.get("block_key") or 0)
        if block_key not in expected_key_set:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 forward read returned an unexpected shard block"
            )
        _forward_provider_range_for_block(
            normalized_code_key,
            block_key,
            options.provider_shard_span,
        )
        fragments_by_block.setdefault(block_key, []).append(fragment_row)

    observed_source_count: int | None = None
    for block_key in sorted(fragments_by_block):
        provider_key_min, provider_key_max = (
            _forward_provider_range_for_block(
                normalized_code_key,
                block_key,
                options.provider_shard_span,
            )
        )
        shard_source_count = _visit_code_records_with_source_count(
            fragments_by_block[block_key],
            provider_set_keys=options.provider_set_keys,
            occurrence_consumer=occurrence_consumer,
            expected_source_count=options.expected_source_count,
            provider_key_min=provider_key_min,
            provider_key_max=provider_key_max,
            price_item_count=options.price_item_count,
            source_keys=options.source_keys,
        )
        if observed_source_count not in (None, shard_source_count):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 forward shards disagree on source_count"
            )
        observed_source_count = shard_source_count


async def _shared_provider_counts_for_keys(
    session: Any,
    *,
    shared_snapshot_key: int,
    schema_name: str,
    provider_set_keys: Iterable[int],
) -> dict[int, int]:
    requested_keys = tuple(
        sorted({int(provider_set_key) for provider_set_key in provider_set_keys})
    )
    if not requested_keys:
        return {}
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT provider_set_key, provider_count
              FROM {schema}.ptg2_v3_provider_set
             WHERE snapshot_key = :snapshot_key
               AND provider_set_key = ANY(CAST(:provider_set_keys AS integer[]))
            """
        ),
        {
            "snapshot_key": _required_shared_snapshot_key(shared_snapshot_key),
            "provider_set_keys": requested_keys,
        },
    )
    provider_counts_by_key = {
        int(count_row["provider_set_key"]): int(count_row["provider_count"])
        for count_row in (_row_mapping(raw_row) for raw_row in query_result)
        if count_row.get("provider_set_key") is not None
        and count_row.get("provider_count") is not None
    }
    if set(provider_counts_by_key) != set(requested_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 shared provider-set dictionary key is missing"
        )
    return provider_counts_by_key


async def _provider_counts_for_decoded_keys(
    session: Any,
    *,
    shared_snapshot_key: int,
    schema_name: str,
    decoded_keys: Iterable[tuple[int, int, int]],
    provider_counts_by_key: Mapping[int, int] | None,
) -> dict[int, int]:
    needed_keys = {
        provider_set_key for provider_set_key, _price_key, _source_key in decoded_keys
    }
    if provider_counts_by_key is None:
        return await _shared_provider_counts_for_keys(
            session,
            shared_snapshot_key=shared_snapshot_key,
            schema_name=schema_name,
            provider_set_keys=needed_keys,
        )
    filtered_counts_by_key = {
        int(provider_set_key): int(provider_count)
        for provider_set_key, provider_count in provider_counts_by_key.items()
        if int(provider_set_key) in needed_keys
    }
    if set(filtered_counts_by_key) != needed_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 shared provider-set dictionary key is missing"
        )
    return filtered_counts_by_key


async def _lookup_forward_references(
    session: Any,
    options: _ForwardLookupOptions | _ForwardBatchOptions,
    decoded_keys: list[tuple[int, int, int]],
) -> tuple[dict[int, int], dict[int, str]]:
    """Read provider counts and only the referenced price dictionary entries."""

    provider_counts_by_key = await _provider_counts_for_decoded_keys(
        session,
        shared_snapshot_key=options.shared_snapshot_key,
        schema_name=options.schema_name,
        decoded_keys=decoded_keys,
        provider_counts_by_key=options.provider_counts_by_key,
    )
    needed_price_keys = {
        price_key for _provider_set_key, price_key, _source_key in decoded_keys
    }
    price_ids_by_key = await _serving_binary_dictionary_values_for_keys(
        session,
        shared_snapshot_key=options.shared_snapshot_key,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        item_keys=needed_price_keys,
        item_count=options.price_dictionary_item_count,
        block_bytes=options.price_dictionary_block_bytes,
        schema_name=options.schema_name,
    )
    return provider_counts_by_key, price_ids_by_key


def _materialize_forward_rows(
    code_key: int,
    decoded_keys: Iterable[tuple[int, int, int]],
    provider_counts_by_key: Mapping[int, int],
    price_ids_by_key: Mapping[int, str],
) -> tuple[PTG2ServingBinaryRow, ...]:
    """Build strict V3 response rows without altering occurrence order."""

    return tuple(
        PTG2ServingBinaryRow(
            code_key=code_key,
            provider_set_key=provider_set_key,
            provider_count=provider_counts_by_key[provider_set_key],
            price_set_global_id_128=price_ids_by_key[price_key],
            source_key=source_key,
            price_key=price_key,
        )
        for provider_set_key, price_key, source_key in decoded_keys
    )


def _flatten_forward_shard_keys(
    shard_keys_by_code: Mapping[int, Iterable[int]],
) -> tuple[int, ...]:
    return tuple(
        sorted(
            {
                int(block_key)
                for block_keys in shard_keys_by_code.values()
                for block_key in block_keys
            }
        )
    )


def _group_forward_fragments_by_code(
    fragment_rows: Iterable[Mapping[str, Any]],
    shard_keys_by_code: Mapping[int, Iterable[int]],
    provider_shard_span: int | None = None,
) -> dict[int, list[Mapping[str, Any]]]:
    code_by_block: dict[int, int] = {}
    fragments_by_code: dict[int, list[Mapping[str, Any]]] = {
        int(code_key): [] for code_key in shard_keys_by_code
    }
    for code_key, block_keys in shard_keys_by_code.items():
        normalized_code_key = _normalized_code_key(code_key)
        for block_key in block_keys:
            normalized_block_key = int(block_key)
            _forward_provider_range_for_block(
                normalized_code_key,
                normalized_block_key,
                provider_shard_span,
            )
            previous_code_key = code_by_block.setdefault(
                normalized_block_key,
                normalized_code_key,
            )
            if previous_code_key != normalized_code_key:
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 forward shard is assigned to multiple codes"
                )
    for fragment_row in fragment_rows:
        block_key = int(fragment_row.get("block_key") or 0)
        code_key = code_by_block.get(block_key)
        if code_key is None:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 forward read returned an unexpected shard block"
            )
        fragments_by_code[code_key].append(fragment_row)
    return fragments_by_code


async def lookup_code_rows_from_db(
    session: Any,
    code_key: int,
    **read_options: Any,
) -> tuple[PTG2ServingBinaryRow, ...]:
    """Read all strict provider shards needed for one code lookup."""

    options = _ForwardLookupOptions(**read_options)
    price_item_count = _normalized_price_item_count(
        options.price_dictionary_item_count
    )
    normalized_code_key = _normalized_code_key(code_key)
    shard_keys_by_code, provider_filter, require_all = (
        await _forward_shard_keys_for_read(
            session,
            options,
            (normalized_code_key,),
        )
    )
    block_keys = shard_keys_by_code[normalized_code_key]
    if not block_keys:
        return ()
    fragment_rows = await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=options.shared_snapshot_key,
        schema_name=options.schema_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
        block_keys=block_keys,
        require_all=require_all,
    )
    if not fragment_rows:
        return ()
    decoded_keys = _decode_forward_shards_for_code(
        fragment_rows,
        code_key=normalized_code_key,
        expected_block_keys=block_keys,
        provider_set_keys=provider_filter,
        expected_source_count=options.source_count,
        price_item_count=price_item_count,
        provider_shard_span=options.provider_shard_span,
    )
    provider_counts_by_key, price_ids_by_key = await _lookup_forward_references(
        session, options, decoded_keys
    )
    return _materialize_forward_rows(
        normalized_code_key,
        decoded_keys,
        provider_counts_by_key,
        price_ids_by_key,
    )


lookup_serving_binary_by_code_from_db = lookup_code_rows_from_db


def _forward_prefix_rank(
    provider_set_key: int,
    price_key: int,
    source_key: int,
    *,
    descending: bool,
) -> tuple[int, int, int]:
    return (
        -int(price_key) if descending else int(price_key),
        int(provider_set_key),
        int(source_key),
    )


async def lookup_code_prefix_rows_from_db(
    session: Any,
    code_key: int,
    *,
    limit: int,
    descending: bool = False,
    **read_options: Any,
) -> tuple[PTG2ServingBinaryRow, ...]:
    """Return the exact best bounded prefix while validating the full code block."""

    if isinstance(limit, bool) or int(limit) <= 0:
        raise ValueError("PTG2 serving binary prefix limit must be positive")
    normalized_limit = int(limit)
    options = _ForwardLookupOptions(**read_options)
    price_item_count = _normalized_price_item_count(
        options.price_dictionary_item_count
    )
    normalized_code_key = _normalized_code_key(code_key)
    shard_keys_by_code, normalized_provider_filter, require_all = (
        await _forward_shard_keys_for_read(
            session,
            options,
            (normalized_code_key,),
        )
    )
    block_keys = shard_keys_by_code[normalized_code_key]
    if not block_keys:
        return ()
    expected_block_keys = set(block_keys)
    provider_filter = (
        set(normalized_provider_filter)
        if normalized_provider_filter is not None
        else None
    )
    retained_occurrences: list[tuple[tuple[int, int, int, int], int, int, int, int]] = []
    occurrence_ordinals = [0]

    def _retain(provider_set_key: int, price_key: int, source_key: int) -> None:
        ordinal = occurrence_ordinals[0]
        occurrence_ordinals[0] += 1
        rank = _forward_prefix_rank(
            provider_set_key,
            price_key,
            source_key,
            descending=bool(descending),
        )
        # Negating the rank makes heap[0] the worst retained occurrence.
        heap_rank = (-rank[0], -rank[1], -rank[2], -ordinal)
        candidate = (
            heap_rank,
            int(provider_set_key),
            int(price_key),
            int(source_key),
            ordinal,
        )
        if len(retained_occurrences) < normalized_limit:
            heapq.heappush(retained_occurrences, candidate)
        elif candidate > retained_occurrences[0]:
            heapq.heapreplace(retained_occurrences, candidate)

    current_block_key: int | None = None
    previous_provider_set_key: int | None = None
    previous_occurrence: tuple[int, int] | None = None
    provider_key_min = 0
    provider_key_max = 0
    observed_source_count: int | None = None
    expected_fragment_no = 0
    try:
        async for fragment in stream_shared_blocks(
            session,
            schema_name=options.schema_name,
            snapshot_key=_required_shared_snapshot_key(
                options.shared_snapshot_key
            ),
            object_kind=_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            block_keys=block_keys,
            require_all=require_all,
        ):
            if fragment.block_key not in expected_block_keys:
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 forward stream returned an unexpected shard block"
                )
            if fragment.block_key != current_block_key:
                if (
                    current_block_key is not None
                    and fragment.block_key <= current_block_key
                ):
                    raise PTG2ManifestArtifactError(
                        "PTG2 v3 forward shard blocks are not ordered"
                    )
                current_block_key = fragment.block_key
                expected_fragment_no = 0
                previous_provider_set_key = None
                previous_occurrence = None
                provider_key_min, provider_key_max = (
                    _forward_provider_range_for_block(
                        normalized_code_key,
                        fragment.block_key,
                        options.provider_shard_span,
                    )
                )
            if fragment.fragment_no != expected_fragment_no:
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 provider-shard fragments are not contiguous"
                )
            expected_fragment_no += 1
            fragment_cursor, fragment_source_count = (
                _visit_serving_binary_by_code_record(
                    {
                        "block_no": fragment.fragment_no,
                        "entry_count": fragment.entry_count,
                        "_decoded_payload": fragment.payload,
                    },
                    provider_filter=provider_filter,
                    fragment_cursor=_ForwardFragmentCursor(
                        provider_set_key=previous_provider_set_key,
                        occurrence=previous_occurrence,
                    ),
                    validation=_ForwardFragmentValidation(
                        expected_source_count=options.source_count,
                        price_item_count=price_item_count,
                        provider_key_min=provider_key_min,
                        provider_key_max=provider_key_max,
                    ),
                    occurrence_consumer=_retain,
                )
            )
            previous_provider_set_key = fragment_cursor.provider_set_key
            previous_occurrence = fragment_cursor.occurrence
            if observed_source_count not in (None, fragment_source_count):
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 grouped by-code fragments disagree on source_count"
                )
            observed_source_count = fragment_source_count
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc

    if not retained_occurrences:
        return ()
    selected = sorted(
        (
            (provider_set_key, price_key, source_key, ordinal)
            for _heap_rank, provider_set_key, price_key, source_key, ordinal in retained_occurrences
        ),
        key=lambda item: (
            _forward_prefix_rank(
                item[0], item[1], item[2], descending=bool(descending)
            ),
            item[3],
        ),
    )
    decoded_keys = [
        (provider_set_key, price_key, source_key)
        for provider_set_key, price_key, source_key, _ordinal in selected
    ]
    provider_counts_by_key, price_ids_by_key = await _lookup_forward_references(
        session,
        options,
        decoded_keys,
    )
    response_rows = _materialize_forward_rows(
        normalized_code_key,
        decoded_keys,
        provider_counts_by_key,
        price_ids_by_key,
    )

    def _row_rank(row: PTG2ServingBinaryRow) -> tuple[int, int, int, int]:
        if row.price_key is None or row.provider_count is None:
            raise PTG2ManifestArtifactError(
                "PTG2 bounded forward row is missing a dense rank field"
            )
        return (
            -row.price_key if descending else row.price_key,
            row.provider_set_key,
            row.source_key,
            row.provider_count,
        )

    # provider_count is functionally dependent on provider_set_key, so adding it
    # here cannot change which occurrences crossed the bounded heap boundary.
    return tuple(sorted(response_rows, key=_row_rank))


async def lookup_price_ids_from_db(
    session: Any,
    price_keys: Iterable[int],
    *,
    shared_snapshot_key: int,
    price_dictionary_item_count: int,
    price_dictionary_block_bytes: int,
    source_count: int | None = None,
    schema_name: str = "mrf",
) -> dict[int, str]:
    """Read selected dense price IDs from fresh shared dictionary fragments."""

    normalized_price_keys = {int(price_key) for price_key in price_keys}
    if not normalized_price_keys:
        return {}
    return await _serving_binary_dictionary_values_for_keys(
        session,
        shared_snapshot_key=shared_snapshot_key,
        artifact_kind=_SERVING_BINARY_BY_CODE_DICTIONARY_KIND,
        item_keys=normalized_price_keys,
        item_count=price_dictionary_item_count,
        block_bytes=price_dictionary_block_bytes,
        schema_name=schema_name,
    )


async def has_serving_binary_code_block(
    session: Any,
    code_key: int,
    *,
    shared_snapshot_key: int,
    schema_name: str = "mrf",
) -> bool:
    """Return whether a sealed layout has a shard in the code-key range."""

    normalized_code_key = _normalized_code_key(code_key)
    lower_bound, upper_bound = _forward_code_block_bounds(
        normalized_code_key
    )
    schema = _quote_ident(schema_name)
    block_exists_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_layout layout
                  JOIN {schema}.ptg2_v3_snapshot_block mapping
                    ON mapping.snapshot_key = layout.snapshot_key
                 WHERE layout.snapshot_key = :snapshot_key
                   AND layout.state = 'sealed'
                   AND layout.generation = :generation
                   AND mapping.object_kind = :object_kind
                   AND mapping.block_key >= :lower_bound
                   AND mapping.block_key < :upper_bound
                 LIMIT 1
            )
            """
        ),
        {
            "snapshot_key": _required_shared_snapshot_key(
                shared_snapshot_key
            ),
            "generation": PTG2_V3_SHARED_GENERATION,
            "object_kind": _SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        },
    )
    return bool(block_exists_result.scalar())


serving_binary_code_block_exists = has_serving_binary_code_block


def _normalized_batch_provider_filters(
    options: _ForwardBatchOptions,
    code_keys: tuple[int, ...],
) -> dict[int, tuple[int, ...]] | None:
    filters_by_code = options.provider_set_keys_by_code
    if filters_by_code is None:
        provider_filter = _normalized_provider_set_filter(options.provider_set_keys)
        if provider_filter is None:
            return None
        return {code_key: provider_filter for code_key in code_keys}
    if options.provider_set_keys is not None:
        raise PTG2ManifestArtifactError(
            "PTG2 batch provider filters must use one filter mode"
        )
    normalized_filters_by_code: dict[int, tuple[int, ...]] = {}
    for raw_code_key, provider_set_keys in filters_by_code.items():
        code_key = _normalized_code_key(raw_code_key)
        if code_key in normalized_filters_by_code:
            raise PTG2ManifestArtifactError(
                "PTG2 batch provider filters contain a duplicate code key"
            )
        normalized_filters_by_code[code_key] = (
            _normalized_provider_set_filter(provider_set_keys) or ()
        )
    if set(normalized_filters_by_code) != set(code_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 batch provider filters must cover exactly the requested codes"
        )
    return normalized_filters_by_code


def _normalized_batch_source_filters(
    options: _ForwardBatchOptions,
    code_keys: tuple[int, ...],
) -> dict[int, frozenset[int]] | None:
    source_filters_by_code = options.source_keys_by_code
    if source_filters_by_code is None:
        return None
    if isinstance(options.source_count, bool):
        raise PTG2ManifestArtifactError(
            "PTG2 batch source filter requires a valid source count"
        )
    try:
        source_count = int(options.source_count)
        dense_source_key_bits(source_count)
    except (TypeError, ValueError, PTG2SharedBlockError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 batch source filter requires a valid source count"
        ) from exc
    normalized_filters_by_code: dict[int, frozenset[int]] = {}
    for raw_code_key, source_keys in source_filters_by_code.items():
        code_key = _normalized_code_key(raw_code_key)
        if code_key in normalized_filters_by_code:
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filters contain a duplicate code key"
            )
        source_filter = _normalized_source_filter(source_keys, source_count)
        if not source_filter:
            raise PTG2ManifestArtifactError(
                "PTG2 batch source filter must not be empty"
            )
        normalized_filters_by_code[code_key] = source_filter
    if set(normalized_filters_by_code) != set(code_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 batch source filters must cover exactly the requested codes"
        )
    return normalized_filters_by_code


def _normalized_batch_occurrence_filters(
    options: _ForwardBatchOptions,
    code_keys: tuple[int, ...],
) -> dict[int, frozenset[tuple[int, int]]] | None:
    """Normalize exact code/provider/source triples for one batch visit."""

    if options.occurrence_keys is None:
        return None
    if isinstance(options.source_count, bool):
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filter requires a valid source count"
        )
    try:
        source_count = int(options.source_count)
        dense_source_key_bits(source_count)
    except (TypeError, ValueError, PTG2SharedBlockError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filter requires a valid source count"
        ) from exc
    normalized_filters_by_code: dict[int, set[tuple[int, int]]] = {
        code_key: set() for code_key in code_keys
    }
    for raw_occurrence_key in options.occurrence_keys:
        try:
            raw_code_key, raw_provider_set_key, raw_source_key = raw_occurrence_key
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 batch occurrence filter has an invalid coordinate"
            ) from exc
        code_key = _normalized_code_key(raw_code_key)
        if code_key not in normalized_filters_by_code:
            raise PTG2ManifestArtifactError(
                "PTG2 batch occurrence filter contains an unexpected code key"
            )
        provider_filter = _normalized_provider_set_filter((raw_provider_set_key,))
        source_filter = _normalized_source_filter((raw_source_key,), source_count)
        if not provider_filter or not source_filter:
            raise PTG2ManifestArtifactError(
                "PTG2 batch occurrence filter must not be empty"
            )
        normalized_filters_by_code[code_key].add(
            (provider_filter[0], next(iter(source_filter)))
        )
    if any(not occurrence_filter for occurrence_filter in normalized_filters_by_code.values()):
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filters must cover exactly the requested codes"
        )
    return {
        code_key: frozenset(occurrence_filter)
        for code_key, occurrence_filter in normalized_filters_by_code.items()
    }


async def _forward_batch_shards_and_filters(
    session: Any,
    options: _ForwardBatchOptions,
    code_keys: tuple[int, ...],
    filters_by_code: dict[int, tuple[int, ...]] | None,
) -> tuple[
    dict[int, tuple[int, ...]],
    dict[int, tuple[int, ...] | None],
    bool,
]:
    if filters_by_code is None:
        shard_keys_by_code = await _discover_forward_shard_keys(
            session,
            shared_snapshot_key=options.shared_snapshot_key,
            schema_name=options.schema_name,
            code_keys=code_keys,
            provider_shard_span=_normalized_provider_shard_span(
                options.provider_shard_span
            ),
        )
        return shard_keys_by_code, {code_key: None for code_key in code_keys}, True
    provider_shard_span = _normalized_provider_shard_span(
        options.provider_shard_span
    )
    shard_keys_by_code = {
        code_key: _computed_forward_shard_keys(
            (code_key,),
            filters_by_code[code_key],
            provider_shard_span,
        )[code_key]
        for code_key in code_keys
    }
    return shard_keys_by_code, filters_by_code, False


async def _decoded_forward_batch_keys(
    session: Any,
    code_keys: tuple[int, ...],
    options: _ForwardBatchOptions,
) -> dict[int, list[tuple[int, int, int]]]:
    decoded_by_code = {code_key: [] for code_key in code_keys}

    def _retain(
        code_key: int,
        provider_set_key: int,
        price_key: int,
        source_key: int,
    ) -> None:
        decoded_by_code[code_key].append(
            (provider_set_key, price_key, source_key)
        )

    await _visit_forward_batch_keys(
        session,
        code_keys,
        options,
        _retain,
    )
    return decoded_by_code


def _forward_batch_fragment_views(
    code_keys: tuple[int, ...],
    fragments_by_code: Mapping[int, Iterable[Mapping[str, Any]]],
    shard_keys_by_code: Mapping[int, Iterable[int]],
    filters_by_code: Mapping[int, Iterable[int] | None],
    source_filters_by_code: Mapping[int, frozenset[int]] | None,
    occurrence_filters_by_code: Mapping[
        int,
        frozenset[tuple[int, int]],
    ]
    | None,
    provider_shard_span: int | None,
) -> tuple[_ForwardBatchFragmentView, ...]:
    """Validate logical shard coordinates before any physical payload parse."""

    views: list[_ForwardBatchFragmentView] = []
    for code_key in code_keys:
        provider_filter = (
            None
            if filters_by_code[code_key] is None
            else frozenset(int(key) for key in filters_by_code[code_key] or ())
        )
        source_filter = (
            None
            if source_filters_by_code is None
            else source_filters_by_code[code_key]
        )
        occurrence_filter = (
            None
            if occurrence_filters_by_code is None
            else occurrence_filters_by_code[code_key]
        )
        views.extend(
            _forward_code_fragment_views(
                code_key,
                fragments_by_code[code_key],
                shard_keys_by_code[code_key],
                provider_filter,
                source_filter,
                occurrence_filter,
                provider_shard_span,
            )
        )
    return tuple(views)


def _forward_code_fragment_views(
    code_key: int,
    fragment_rows: Iterable[Mapping[str, Any]],
    shard_keys: Iterable[int],
    provider_filter: frozenset[int] | None,
    source_filter: frozenset[int] | None,
    occurrence_filter: frozenset[tuple[int, int]] | None,
    provider_shard_span: int | None,
) -> tuple[_ForwardBatchFragmentView, ...]:
    """Build the logical views for one code after validating exact scope."""

    rows_by_block = _validated_forward_rows_by_block(
        code_key,
        fragment_rows,
        shard_keys,
        provider_shard_span,
    )
    views: list[_ForwardBatchFragmentView] = []
    for block_key in sorted(rows_by_block):
        provider_key_min, provider_key_max = _forward_provider_range_for_block(
            code_key,
            block_key,
            provider_shard_span,
        )
        block_occurrence_filter = _forward_block_occurrence_filter(
            occurrence_filter,
            provider_key_min,
            provider_key_max,
        )
        for fragment_row in _ordered_forward_fragments(rows_by_block[block_key]):
            views.append(
                _ForwardBatchFragmentView(
                    code_key=code_key,
                    block_key=block_key,
                    fragment_no=int(fragment_row.get("block_no") or 0),
                    provider_key_min=provider_key_min,
                    provider_key_max=provider_key_max,
                    provider_filter=provider_filter,
                    source_filter=source_filter,
                    occurrence_filter=block_occurrence_filter,
                    fragment_row=fragment_row,
                )
            )
    return tuple(views)


def _validate_forward_occurrence_scope(
    provider_filter: frozenset[int] | None,
    source_filter: frozenset[int] | None,
    occurrence_filter: frozenset[tuple[int, int]] | None,
) -> None:
    """Require exact occurrence coordinates to agree with broad read scope."""

    if occurrence_filter is None:
        return
    exact_provider_filter = frozenset(
        provider_set_key for provider_set_key, _source_key in occurrence_filter
    )
    if provider_filter is None or exact_provider_filter != provider_filter:
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filter must equal its provider scope"
        )
    if source_filter is not None and any(
        source_key not in source_filter
        for _provider_set_key, source_key in occurrence_filter
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filter exceeds its source scope"
        )


def _validate_forward_batch_occurrence_scopes(
    code_keys: tuple[int, ...],
    filters_by_code: Mapping[int, Iterable[int]] | None,
    source_filters_by_code: Mapping[int, frozenset[int]] | None,
    occurrence_filters_by_code: Mapping[
        int,
        frozenset[tuple[int, int]],
    ]
    | None,
) -> None:
    """Reject inconsistent exact scope before shard discovery or payload I/O."""

    if occurrence_filters_by_code is None:
        return
    for code_key in code_keys:
        provider_filter = (
            None
            if filters_by_code is None
            else frozenset(filters_by_code[code_key])
        )
        source_filter = (
            None
            if source_filters_by_code is None
            else source_filters_by_code[code_key]
        )
        _validate_forward_occurrence_scope(
            provider_filter,
            source_filter,
            occurrence_filters_by_code[code_key],
        )


def _forward_block_occurrence_filter(
    occurrence_filter: frozenset[tuple[int, int]] | None,
    provider_key_min: int,
    provider_key_max: int,
) -> frozenset[tuple[int, int]] | None:
    """Restrict exact coordinates to one already-selected provider shard."""

    if occurrence_filter is None:
        return None
    block_filter = frozenset(
        (provider_set_key, source_key)
        for provider_set_key, source_key in occurrence_filter
        if provider_key_min <= provider_set_key < provider_key_max
    )
    if not block_filter:
        raise PTG2ManifestArtifactError(
            "PTG2 batch occurrence filter is empty for a requested shard"
        )
    return block_filter


def _validated_forward_rows_by_block(
    code_key: int,
    fragment_rows: Iterable[Mapping[str, Any]],
    expected_block_keys: Iterable[int],
    provider_shard_span: int | None,
) -> dict[int, list[Mapping[str, Any]]]:
    expected_key_set = {int(block_key) for block_key in expected_block_keys}
    rows_by_block: dict[int, list[Mapping[str, Any]]] = {}
    for fragment_row in fragment_rows:
        block_key = int(fragment_row.get("block_key") or 0)
        if block_key not in expected_key_set:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 forward read returned an unexpected shard block"
            )
        _forward_provider_range_for_block(
            code_key,
            block_key,
            provider_shard_span,
        )
        rows_by_block.setdefault(block_key, []).append(fragment_row)
    return rows_by_block


def _forward_fragment_physical_identity(
    view: _ForwardBatchFragmentView,
) -> tuple[Any, ...]:
    """Require the sealed physical identity used by the read-once ledger."""

    raw_block_hash = view.fragment_row.get("_block_hash")
    if isinstance(raw_block_hash, (bytes, bytearray, memoryview)) and raw_block_hash:
        return ("physical", bytes(raw_block_hash))
    raise PTG2ManifestArtifactError(
        "PTG2 v3 forward fragment is missing its physical block identity"
    )


def _parse_forward_batch_physical_fragments_once(
    views: tuple[_ForwardBatchFragmentView, ...],
    *,
    options: _ForwardBatchOptions,
    price_item_count: int,
) -> tuple[
    dict[tuple[Any, ...], _ParsedForwardFragment],
    dict[tuple[int, int, int], list[tuple[int, int, int]]],
]:
    """Parse each physical payload once and fan retained rows to logical views."""

    views_by_identity = _forward_views_by_physical_identity(views)
    parsed_by_identity: dict[tuple[Any, ...], _ParsedForwardFragment] = {}
    retained_by_coordinate: dict[
        tuple[int, int, int], list[tuple[int, int, int]]
    ] = {
        (view.code_key, view.block_key, view.fragment_no): [] for view in views
    }
    for identity, physical_views in views_by_identity.items():
        parsed_by_identity[identity] = _parse_physical_forward_fragment_once(
            tuple(physical_views),
            options,
            price_item_count,
            retained_by_coordinate,
        )
    return parsed_by_identity, retained_by_coordinate


def _forward_fanout_capture(
    physical_views: tuple[_ForwardBatchFragmentView, ...],
    retained_by_coordinate: dict[
        tuple[int, int, int],
        list[tuple[int, int, int]],
    ],
) -> _ForwardFanoutCapture:
    """Index exact logical views once before visiting physical occurrences."""

    exact_views_by_occurrence: dict[
        tuple[int, int],
        list[_ForwardBatchFragmentView],
    ] = {}
    fallback_views: list[_ForwardBatchFragmentView] = []
    provider_filter_set: set[int] = set()
    is_provider_scope_unrestricted = False
    for view in physical_views:
        if view.occurrence_filter is None:
            fallback_views.append(view)
            if view.provider_filter is None:
                is_provider_scope_unrestricted = True
            else:
                provider_filter_set.update(view.provider_filter)
            continue
        for occurrence_key in view.occurrence_filter:
            provider_filter_set.add(occurrence_key[0])
            exact_views_by_occurrence.setdefault(occurrence_key, []).append(view)
    return _ForwardFanoutCapture(
        exact_views_by_occurrence={
            occurrence_key: tuple(views)
            for occurrence_key, views in exact_views_by_occurrence.items()
        },
        fallback_views=tuple(fallback_views),
        provider_filter_set=(
            None
            if is_provider_scope_unrestricted
            else frozenset(provider_filter_set)
        ),
        retained_by_coordinate=retained_by_coordinate,
    )


def _forward_views_by_physical_identity(
    views: tuple[_ForwardBatchFragmentView, ...],
) -> dict[tuple[Any, ...], list[_ForwardBatchFragmentView]]:
    views_by_identity: dict[tuple[Any, ...], list[_ForwardBatchFragmentView]] = {}
    for view in views:
        identity = _forward_fragment_physical_identity(view)
        views_by_identity.setdefault(identity, []).append(view)
    return views_by_identity


def _parse_physical_forward_fragment_once(
    physical_views: tuple[_ForwardBatchFragmentView, ...],
    options: _ForwardBatchOptions,
    price_item_count: int,
    retained_by_coordinate: dict[
        tuple[int, int, int],
        list[tuple[int, int, int]],
    ],
) -> _ParsedForwardFragment:
    representative = physical_views[0]
    raw_block_hash = representative.fragment_row.get("_block_hash")
    if not isinstance(raw_block_hash, (bytes, bytearray, memoryview)) or not raw_block_hash:
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward fragment is missing its physical block identity"
        )
    claim_shared_block_processing(
        schema_name=options.schema_name,
        block_hash=bytes(raw_block_hash),
    )
    fanout_capture = _forward_fanout_capture(
        physical_views,
        retained_by_coordinate,
    )
    last_cursor, source_count = _visit_serving_binary_by_code_record(
        representative.fragment_row,
        provider_filter=fanout_capture.provider_filter_set,
        fragment_cursor=_ForwardFragmentCursor(),
        validation=_ForwardFragmentValidation(
            expected_source_count=options.source_count,
            price_item_count=price_item_count,
        ),
        occurrence_consumer=fanout_capture,
        first_occurrence_consumer=fanout_capture.capture_first,
    )
    if (
        fanout_capture.first_provider_set_key is None
        or fanout_capture.first_occurrence is None
        or last_cursor.provider_set_key is None
        or last_cursor.occurrence is None
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 grouped by-code fragment has no occurrences"
        )
    return _ParsedForwardFragment(
        first_provider_set_key=fanout_capture.first_provider_set_key,
        first_occurrence=fanout_capture.first_occurrence,
        last_cursor=last_cursor,
        source_count=source_count,
    )


def _emit_forward_batch_logical_views(
    views: tuple[_ForwardBatchFragmentView, ...],
    parsed_by_identity: Mapping[tuple[Any, ...], _ParsedForwardFragment],
    retained_by_coordinate: Mapping[
        tuple[int, int, int], Iterable[tuple[int, int, int]]
    ],
    occurrence_consumer: Callable[[int, int, int, int], None],
) -> None:
    """Validate each logical ordering context and emit its physical fan-out."""

    previous_cursor_by_block: dict[tuple[int, int], _ForwardFragmentCursor] = {}
    observed_source_count_by_code: dict[int, int] = {}
    for view in views:
        parsed = parsed_by_identity[_forward_fragment_physical_identity(view)]
        if (
            parsed.first_provider_set_key < view.provider_key_min
            or int(parsed.last_cursor.provider_set_key) >= view.provider_key_max
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 v3 provider set is outside its forward shard"
            )
        block_identity = (view.code_key, view.block_key)
        previous_cursor = previous_cursor_by_block.get(block_identity)
        if previous_cursor is not None:
            previous_provider_set_key = int(previous_cursor.provider_set_key)
            if parsed.first_provider_set_key < previous_provider_set_key or (
                parsed.first_provider_set_key == previous_provider_set_key
                and parsed.first_occurrence < previous_cursor.occurrence
            ):
                raise PTG2ManifestArtifactError(
                    "PTG2 v3 grouped by-code occurrences are not ordered"
                )
        previous_cursor_by_block[block_identity] = parsed.last_cursor
        observed_source_count = observed_source_count_by_code.setdefault(
            view.code_key,
            parsed.source_count,
        )
        if observed_source_count != parsed.source_count:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 grouped by-code fragments disagree on source_count"
            )
        for provider_set_key, price_key, source_key in retained_by_coordinate[
            (view.code_key, view.block_key, view.fragment_no)
        ]:
            occurrence_consumer(
                view.code_key,
                provider_set_key,
                price_key,
                source_key,
            )


async def _visit_forward_batch_keys(
    session: Any,
    code_keys: tuple[int, ...],
    options: _ForwardBatchOptions,
    occurrence_consumer: Callable[[int, int, int, int], None],
) -> None:
    """Fetch the union of all code shards once and visit their occurrences."""

    price_item_count = _normalized_price_item_count(
        options.price_dictionary_item_count
    )
    source_filters_by_code = _normalized_batch_source_filters(
        options,
        code_keys,
    )
    occurrence_filters_by_code = _normalized_batch_occurrence_filters(
        options,
        code_keys,
    )
    views = await _fetch_forward_batch_fragment_views(
        session,
        code_keys,
        options,
        source_filters_by_code,
        occurrence_filters_by_code,
    )
    parsed_by_identity, retained_by_coordinate = (
        _parse_forward_batch_physical_fragments_once(
            views,
            options=options,
            price_item_count=price_item_count,
        )
    )
    _emit_forward_batch_logical_views(
        views,
        parsed_by_identity,
        retained_by_coordinate,
        occurrence_consumer,
    )


async def _fetch_forward_batch_fragment_views(
    session: Any,
    code_keys: tuple[int, ...],
    options: _ForwardBatchOptions,
    source_filters_by_code: Mapping[int, frozenset[int]] | None,
    occurrence_filters_by_code: Mapping[
        int,
        frozenset[tuple[int, int]],
    ]
    | None,
) -> tuple[_ForwardBatchFragmentView, ...]:
    """Fetch selected shards and bind them to validated logical views."""

    filters_by_code = _normalized_batch_provider_filters(options, code_keys)
    _validate_forward_batch_occurrence_scopes(
        code_keys,
        filters_by_code,
        source_filters_by_code,
        occurrence_filters_by_code,
    )
    shard_keys_by_code, filters_by_code, requires_all = (
        await _forward_batch_shards_and_filters(
            session,
            options,
            code_keys,
            filters_by_code,
        )
    )
    if requires_all and any(not shard_keys_by_code[code_key] for code_key in code_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 forward artifact is missing referenced code shards"
        )
    block_keys = _flatten_forward_shard_keys(shard_keys_by_code)
    if not block_keys:
        return ()
    fragment_rows = await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=options.shared_snapshot_key,
        schema_name=options.schema_name,
        artifact_kind=_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
        block_keys=block_keys,
        require_all=requires_all,
    )
    fragments_by_code = _group_forward_fragments_by_code(
        fragment_rows,
        shard_keys_by_code,
        options.provider_shard_span,
    )
    return _forward_batch_fragment_views(
        code_keys,
        fragments_by_code,
        shard_keys_by_code,
        filters_by_code,
        source_filters_by_code,
        occurrence_filters_by_code,
        options.provider_shard_span,
    )


async def lookup_binary_code_batch_from_db(
    session: Any,
    code_keys: Iterable[int],
    **read_options: Any,
) -> dict[int, tuple[PTG2ServingBinaryRow, ...]]:
    """Read and decode a fresh batch of strict V3 forward code blocks."""

    options = _ForwardBatchOptions(**read_options)
    normalized_code_keys = tuple(
        sorted({_normalized_code_key(code_key) for code_key in code_keys})
    )
    if not normalized_code_keys:
        return {}
    decoded_by_code = await _decoded_forward_batch_keys(
        session,
        normalized_code_keys,
        options,
    )
    all_decoded_keys = [
        entry for entries in decoded_by_code.values() for entry in entries
    ]
    provider_counts_by_key, price_ids_by_key = await _lookup_forward_references(
        session, options, all_decoded_keys
    )
    return {
        code_key: _materialize_forward_rows(
            code_key,
            decoded_by_code[code_key],
            provider_counts_by_key,
            price_ids_by_key,
        )
        for code_key in normalized_code_keys
    }


async def lookup_forward_occurrences_batch_from_db(
    session: Any,
    code_keys: Iterable[int],
    **read_options: Any,
) -> dict[int, tuple[tuple[int, int, int], ...]]:
    """Read exact forward occurrence keys without hydrating response labels."""

    options = _ForwardBatchOptions(**read_options)
    normalized_code_keys = tuple(
        sorted({_normalized_code_key(code_key) for code_key in code_keys})
    )
    if not normalized_code_keys:
        return {}
    decoded_by_code = await _decoded_forward_batch_keys(
        session,
        normalized_code_keys,
        options,
    )
    return {
        code_key: tuple(decoded_by_code[code_key])
        for code_key in normalized_code_keys
    }


async def lookup_forward_price_index_from_db(
    session: Any,
    code_keys: Iterable[int],
    **read_options: Any,
) -> dict[tuple[int, int, int], tuple[int, ...]]:
    """Index selected forward prices without retaining intermediate rows."""

    options = _ForwardBatchOptions(**read_options)
    normalized_code_keys = tuple(
        sorted({_normalized_code_key(code_key) for code_key in code_keys})
    )
    if not normalized_code_keys:
        return {}
    price_keys_by_occurrence: dict[tuple[int, int, int], list[int]] = {}

    def _retain(
        code_key: int,
        provider_set_key: int,
        price_key: int,
        source_key: int,
    ) -> None:
        occurrence_key = (code_key, provider_set_key, source_key)
        retained_price_keys = price_keys_by_occurrence.setdefault(occurrence_key, [])
        if not retained_price_keys or retained_price_keys[-1] != price_key:
            retained_price_keys.append(price_key)

    await _visit_forward_batch_keys(
        session,
        normalized_code_keys,
        options,
        _retain,
    )
    return {
        occurrence_key: tuple(price_keys)
        for occurrence_key, price_keys in price_keys_by_occurrence.items()
    }


@dataclass(frozen=True)
class _SharedLogicalBlock:
    """One assembled logical block bound to its ordered physical identity."""

    payload: bytes
    entry_count: int
    physical_hashes: tuple[bytes, ...]


def _logical_block_physical_hashes(
    fragments_by_number: Mapping[int, Mapping[str, Any]],
    *,
    artifact_kind: str,
    block_key: int,
) -> tuple[bytes, ...]:
    """Return the required ordered physical identities for one logical block."""

    block_numbers = tuple(sorted(fragments_by_number))
    if block_numbers != tuple(range(len(block_numbers))):
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 {artifact_kind} block {block_key} has non-contiguous block fragments"
        )
    physical_hashes: list[bytes] = []
    for block_number in block_numbers:
        raw_block_hash = fragments_by_number[block_number].get("_block_hash")
        if not isinstance(raw_block_hash, (bytes, bytearray, memoryview)):
            raise PTG2ManifestArtifactError(
                f"PTG2 v3 {artifact_kind} block {block_key} is missing its physical block identity"
            )
        block_hash = bytes(raw_block_hash)
        if not block_hash:
            raise PTG2ManifestArtifactError(
                f"PTG2 v3 {artifact_kind} block {block_key} is missing its physical block identity"
            )
        physical_hashes.append(block_hash)
    return tuple(physical_hashes)


def _claim_logical_block_processing(
    logical_block: _SharedLogicalBlock,
    *,
    schema_name: str,
) -> None:
    """Claim one distinct ordered logical payload before semantic parsing."""

    claim_shared_logical_payload_processing(
        schema_name=schema_name,
        physical_hashes=logical_block.physical_hashes,
    )


def _prepare_logical_block_fragments(
    logical_blocks: Mapping[int, _SharedLogicalBlock],
    *,
    schema_name: str,
) -> None:
    """Prepare each unique physical fragment once across all logical blocks."""

    for logical_block in logical_blocks.values():
        register_shared_logical_payload(
            schema_name=schema_name,
            physical_hashes=logical_block.physical_hashes,
        )
    prepared_hashes: set[bytes] = set()
    for logical_block in logical_blocks.values():
        for block_hash in logical_block.physical_hashes:
            if block_hash in prepared_hashes:
                continue
            prepare_shared_block_payload(
                schema_name=schema_name,
                block_hash=block_hash,
            )
            prepared_hashes.add(block_hash)


def _logical_blocks_by_physical_identity(
    logical_blocks: Mapping[int, _SharedLogicalBlock],
) -> dict[tuple[bytes, ...], list[tuple[int, _SharedLogicalBlock]]]:
    """Group complete physical aliases for one parse followed by logical fan-out."""

    blocks_by_identity: dict[
        tuple[bytes, ...],
        list[tuple[int, _SharedLogicalBlock]],
    ] = {}
    for block_key, logical_block in logical_blocks.items():
        blocks_by_identity.setdefault(logical_block.physical_hashes, []).append(
            (block_key, logical_block)
        )
    return blocks_by_identity


async def _shared_logical_blocks_by_key(
    session: Any,
    *,
    shared_snapshot_key: int,
    schema_name: str,
    artifact_kind: str,
    block_keys: Iterable[int],
) -> dict[int, _SharedLogicalBlock]:
    """Assemble requested shared fragments with the strict V3 block validator."""

    requested_keys = tuple(sorted({int(block_key) for block_key in block_keys}))
    fragment_rows = await _shared_serving_binary_payload_rows_for_keys(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=artifact_kind,
        block_keys=requested_keys,
    )
    fragments_by_key = _fragments_by_logical_key(fragment_rows, requested_keys)
    logical_block_by_key = _assembled_logical_blocks(
        fragments_by_key,
        artifact_kind=artifact_kind,
    )
    _prepare_logical_block_fragments(
        logical_block_by_key,
        schema_name=schema_name,
    )
    return logical_block_by_key


def _fragments_by_logical_key(
    fragment_rows: Iterable[Mapping[str, Any]],
    requested_keys: tuple[int, ...],
) -> dict[int, dict[int, Mapping[str, Any]]]:
    """Index unique returned fragments under the exact requested logical keys."""

    fragments_by_key: dict[int, dict[int, Mapping[str, Any]]] = {
        block_key: {} for block_key in requested_keys
    }
    for fragment_row in fragment_rows:
        block_key = int(fragment_row.get("block_key") or 0)
        fragment_no = int(fragment_row.get("block_no") or 0)
        fragments = fragments_by_key.get(block_key)
        if fragments is None or fragment_no < 0 or fragment_no in fragments:
            raise PTG2ManifestArtifactError(
                "PTG2 shared V3 query returned an invalid block fragment"
            )
        fragments[fragment_no] = fragment_row
    return fragments_by_key


def _assembled_logical_blocks(
    fragments_by_key: Mapping[int, Mapping[int, Mapping[str, Any]]],
    *,
    artifact_kind: str,
) -> dict[int, _SharedLogicalBlock]:
    """Assemble each complete physical identity once and reuse full aliases."""

    from api.ptg2_db_serving_v3 import _logical_block_bytes

    logical_block_by_key: dict[int, _SharedLogicalBlock] = {}
    assembled_by_identity: dict[tuple[bytes, ...], _SharedLogicalBlock] = {}
    for block_key, fragments in fragments_by_key.items():
        if not fragments:
            continue
        physical_hashes = _logical_block_physical_hashes(
            fragments,
            artifact_kind=artifact_kind,
            block_key=block_key,
        )
        logical_block = assembled_by_identity.get(physical_hashes)
        if logical_block is None:
            block_bytes, entry_count = _logical_block_bytes(
                fragments,
                artifact_kind=artifact_kind,
                block_key=block_key,
            )
            logical_block = _SharedLogicalBlock(
                payload=block_bytes,
                entry_count=entry_count,
                physical_hashes=physical_hashes,
            )
            assembled_by_identity[physical_hashes] = logical_block
        else:
            alias_entry_count = int(fragments[0].get("entry_count") or 0)
            if alias_entry_count != logical_block.entry_count or any(
                int(fragments[block_number].get("entry_count") or 0) != 0
                for block_number in tuple(sorted(fragments))[1:]
            ):
                raise PTG2ManifestArtifactError(
                    f"PTG2 v3 {artifact_kind} block {block_key} has invalid fragment entry counts"
                )
        logical_block_by_key[block_key] = logical_block
    return logical_block_by_key


async def lookup_provider_code_keys_from_db(
    session: Any,
    shared_snapshot_key: int,
    provider_set_keys: Iterable[int],
    *,
    schema_name: str = "mrf",
) -> dict[int, tuple[int, ...]]:
    """Read requested provider-set code memberships from fresh shared blocks."""

    from api.ptg2_db_serving_v3 import (
        PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND,
        PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
        _block_keys_for,
        _requested_keys,
    )

    requested_keys = _requested_keys(provider_set_keys)
    logical_blocks = await _shared_logical_blocks_by_key(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PROVIDER_SET_CODES_KIND,
        block_keys=_block_keys_for(
            requested_keys, PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
        ),
    )
    return _provider_code_keys_from_aliases(
        logical_blocks,
        requested_key_set=set(requested_keys),
        schema_name=schema_name,
    )


def _provider_code_keys_from_aliases(
    logical_blocks: Mapping[int, _SharedLogicalBlock],
    *,
    requested_key_set: set[int],
    schema_name: str,
) -> dict[int, tuple[int, ...]]:
    """Parse each distinct logical provider-code payload once and rebase it."""

    from api.ptg2_db_serving_v3 import (
        PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN,
        _decode_provider_code_block,
    )

    code_keys_by_provider: dict[int, tuple[int, ...]] = {}
    for physical_aliases in _logical_blocks_by_physical_identity(
        logical_blocks
    ).values():
        representative_key, logical_block = physical_aliases[0]
        representative_start = (
            representative_key
            * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
        )
        requested_offsets = {
            provider_set_key
            - block_key * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
            for block_key, _alias_block in physical_aliases
            for provider_set_key in requested_key_set
            if (
                block_key * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
                <= provider_set_key
                < (block_key + 1)
                * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
            )
        }
        _claim_logical_block_processing(
            logical_block,
            schema_name=schema_name,
        )
        decoded_by_representative_key = _decode_provider_code_block(
            logical_block.payload,
            block_key=representative_key,
            entry_count=logical_block.entry_count,
            requested_provider_set_keys={
                representative_start + requested_offset
                for requested_offset in requested_offsets
            },
        )
        code_keys_by_offset = {
            provider_set_key - representative_start: code_keys
            for provider_set_key, code_keys in decoded_by_representative_key.items()
        }
        for block_key, _alias_block in physical_aliases:
            block_start = (
                block_key * PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
            )
            for requested_offset, code_keys in code_keys_by_offset.items():
                provider_set_key = block_start + requested_offset
                if provider_set_key in requested_key_set:
                    code_keys_by_provider[provider_set_key] = code_keys
    return code_keys_by_provider


lookup_shared_provider_code_keys_from_db = lookup_provider_code_keys_from_db


async def lookup_price_atom_memberships_from_db(
    session: Any,
    shared_snapshot_key: int,
    price_keys: Iterable[int],
    *,
    atom_key_bits: int | None = None,
    block_span: int | None = None,
    schema_name: str = "mrf",
) -> dict[int, tuple[int, ...]]:
    """Read requested price-to-atom memberships from fresh shared blocks."""

    from api.ptg2_db_serving_v3 import (
        PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN,
        PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
        _block_keys_for,
        _decode_price_membership_block,
        _effective_block_span,
        _expected_atom_key_bits,
        _requested_keys,
    )

    requested_keys = _requested_keys(price_keys)
    effective_span = _effective_block_span(
        block_span, PTG2_SERVING_BINARY_V3_PRICE_KEY_BLOCK_SPAN
    )
    expected_bits = _expected_atom_key_bits(atom_key_bits)
    logical_blocks = await _shared_logical_blocks_by_key(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PRICE_MEMBERSHIPS_KIND,
        block_keys=_block_keys_for(requested_keys, effective_span),
    )
    requested_key_set = set(requested_keys)
    memberships_by_price_key: dict[int, tuple[int, ...]] = {}
    for physical_aliases in _logical_blocks_by_physical_identity(
        logical_blocks
    ).values():
        representative_key, logical_block = physical_aliases[0]
        group_requested_keys = {
            price_key
            for block_key, _alias_block in physical_aliases
            for price_key in requested_key_set
            if block_key * effective_span <= price_key < (block_key + 1) * effective_span
        }
        _claim_logical_block_processing(
            logical_block,
            schema_name=schema_name,
        )
        decoded_memberships = _decode_price_membership_block(
            logical_block.payload,
            block_key=representative_key,
            entry_count=logical_block.entry_count,
            atom_key_bits=expected_bits,
            block_span=effective_span,
            requested_price_keys=group_requested_keys,
        )
        if len(physical_aliases) > 1 and logical_block.entry_count:
            raise PTG2ManifestArtifactError(
                "PTG2 v3 price-membership block has an incompatible physical alias"
            )
        memberships_by_price_key.update(decoded_memberships)
    return memberships_by_price_key


lookup_shared_price_atom_memberships_from_db = lookup_price_atom_memberships_from_db


def _decode_price_atom_block(
    atom_block_bytes: bytes,
    *,
    block_key: int,
    entry_count: int,
    block_span: int,
    requested_keys: set[int],
) -> dict[int, Any]:
    """Decode selected atom offsets and restore their dense global keys."""

    from api.ptg2_db_serving_v3 import _is_key_in_block
    from process.ptg_parts.ptg2_serving_binary_v3 import (
        decode_price_atoms_for_offsets,
    )

    try:
        first_atom_key = block_key * block_span
        requested_offsets = {
            atom_key - first_atom_key
            for atom_key in requested_keys
            if _is_key_in_block(atom_key, block_key, block_span)
        }
        atoms_by_offset = decode_price_atoms_for_offsets(
            atom_block_bytes,
            requested_offsets,
            expected_entry_count=entry_count,
            maximum_entry_count=block_span,
        )
    except Exception as exc:
        raise PTG2ManifestArtifactError(
            f"PTG2 v3 price-atom block {block_key} is corrupt"
        ) from exc
    return {
        first_atom_key + atom_offset: price_atom
        for atom_offset, price_atom in atoms_by_offset.items()
    }


async def lookup_shared_price_atoms_from_db(
    session: Any,
    shared_snapshot_key: int,
    atom_keys: Iterable[int],
    *,
    atom_key_bits: int | None = None,
    block_span: int | None = None,
    schema_name: str = "mrf",
) -> dict[int, Any]:
    """Decode requested price atoms from fresh shared payload blocks."""

    from api.ptg2_db_serving_v3 import (
        PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN,
        PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND,
        _block_keys_for,
        _effective_block_span,
        _expected_atom_key_bits,
        _requested_keys,
    )

    requested_keys = _requested_keys(atom_keys)
    expected_bits = _expected_atom_key_bits(atom_key_bits)
    if (
        expected_bits is not None
        and requested_keys
        and requested_keys[-1] >= 1 << expected_bits
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 v3 atom key exceeds the manifest atom-key width"
        )
    effective_span = _effective_block_span(
        block_span, PTG2_SERVING_BINARY_V3_ATOM_KEY_BLOCK_SPAN
    )
    logical_blocks = await _shared_logical_blocks_by_key(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_ATOM_PAYLOAD_KIND,
        block_keys=_block_keys_for(requested_keys, effective_span),
    )
    return _price_atoms_from_aliases(
        logical_blocks,
        requested_key_set=set(requested_keys),
        block_span=effective_span,
        schema_name=schema_name,
    )


def _price_atoms_from_aliases(
    logical_blocks: Mapping[int, _SharedLogicalBlock],
    *,
    requested_key_set: set[int],
    block_span: int,
    schema_name: str,
) -> dict[int, Any]:
    """Parse each distinct logical atom payload once and rebase its offsets."""

    atoms_by_key: dict[int, Any] = {}
    for physical_aliases in _logical_blocks_by_physical_identity(
        logical_blocks
    ).values():
        representative_key, logical_block = physical_aliases[0]
        requested_offsets = {
            atom_key - block_key * block_span
            for block_key, _alias_block in physical_aliases
            for atom_key in requested_key_set
            if block_key * block_span <= atom_key < (block_key + 1) * block_span
        }
        representative_start = representative_key * block_span
        _claim_logical_block_processing(
            logical_block,
            schema_name=schema_name,
        )
        decoded_by_representative_key = _decode_price_atom_block(
            logical_block.payload,
            block_key=representative_key,
            entry_count=logical_block.entry_count,
            block_span=block_span,
            requested_keys={
                representative_start + requested_offset
                for requested_offset in requested_offsets
            },
        )
        atoms_by_offset = {
            atom_key - representative_start: price_atom
            for atom_key, price_atom in decoded_by_representative_key.items()
        }
        for block_key, _alias_block in physical_aliases:
            block_start = block_key * block_span
            for atom_offset, price_atom in atoms_by_offset.items():
                atom_key = block_start + atom_offset
                if atom_key not in requested_key_set:
                    continue
                if atom_key in atoms_by_key:
                    raise PTG2ManifestArtifactError(
                        "PTG2 v3 price-atom artifact contains a duplicate key"
                    )
                atoms_by_key[atom_key] = price_atom
    return atoms_by_key


async def lookup_shared_code_page_from_db(
    session: Any,
    shared_snapshot_key: int,
    code_key: int,
    *,
    source_count: int | None = None,
    schema_name: str = "mrf",
) -> Any:
    """Read and decode one shared forward page for a code key."""

    from api.ptg2_db_serving_v3_pages import (
        PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND,
        _decode_code_page_block,
    )

    normalized_key = int(code_key)
    logical_blocks = await _shared_logical_blocks_by_key(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND,
        block_keys=(normalized_key,),
    )
    block = logical_blocks.get(normalized_key)
    if block is None:
        return None
    _claim_logical_block_processing(block, schema_name=schema_name)
    return _decode_code_page_block(
        block.payload,
        code_key=normalized_key,
        entry_count=block.entry_count,
        expected_source_count=source_count,
    )


async def lookup_shared_provider_pages_from_db(
    session: Any,
    shared_snapshot_key: int,
    provider_set_keys: Iterable[int],
    *,
    source_count: int | None = None,
    schema_name: str = "mrf",
) -> dict[int, Any] | None:
    """Read and decode provider pages for the requested dense keys."""

    from api.ptg2_db_serving_v3 import _block_keys_for, _requested_keys
    from api.ptg2_db_serving_v3_pages import (
        PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN,
        PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
        _decode_provider_page_block,
    )

    requested_keys = _requested_keys(provider_set_keys)
    logical_blocks = await _shared_logical_blocks_by_key(
        session,
        shared_snapshot_key=shared_snapshot_key,
        schema_name=schema_name,
        artifact_kind=PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
        block_keys=_block_keys_for(
            requested_keys, PTG2_SERVING_BINARY_V3_PROVIDER_PAGE_BLOCK_SPAN
        ),
    )
    if not logical_blocks:
        return None
    requested_key_set = set(requested_keys)
    pages_by_provider_key: dict[int, Any] = {}
    for physical_aliases in _logical_blocks_by_physical_identity(
        logical_blocks
    ).values():
        representative_key, logical_block = physical_aliases[0]
        _claim_logical_block_processing(
            logical_block,
            schema_name=schema_name,
        )
        representative_pages = _decode_provider_page_block(
            logical_block.payload,
            block_key=representative_key,
            entry_count=logical_block.entry_count,
            requested_provider_set_keys={representative_key},
            expected_source_count=source_count,
        )
        representative_page = representative_pages.get(representative_key)
        if representative_page is None:
            continue
        for block_key, _alias_block in physical_aliases:
            if block_key not in requested_key_set:
                continue
            pages_by_provider_key[block_key] = type(representative_page)(
                entries=tuple(
                    type(page_entry)(
                        code_key=page_entry.code_key,
                        provider_set_key=block_key,
                        provider_count=page_entry.provider_count,
                        price_key=page_entry.price_key,
                        source_key=page_entry.source_key,
                    )
                    for page_entry in representative_page.entries
                ),
                total_row_count=representative_page.total_row_count,
            )
    return pages_by_provider_key


async def has_shared_provider_pages_in_db(
    session: Any,
    shared_snapshot_key: int,
    *,
    schema_name: str = "mrf",
) -> bool:
    """Return whether the sealed shared layout contains provider-page blocks."""

    from api.ptg2_db_serving_v3_pages import (
        PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
    )

    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        text(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_layout layout
                  JOIN {schema}.ptg2_v3_snapshot_block mapping
                    ON mapping.snapshot_key = layout.snapshot_key
                 WHERE layout.snapshot_key = :snapshot_key
                   AND layout.state = 'sealed'
                   AND layout.generation = 'shared_blocks_v3'
                   AND mapping.object_kind = :object_kind
                 LIMIT 1
            )
            """
        ),
        {
            "snapshot_key": _required_shared_snapshot_key(shared_snapshot_key),
            "object_kind": PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND,
        },
    )
    return bool(query_result.scalar())


async def lookup_shared_graph_members_from_db(
    session: Any,
    shared_snapshot_key: int,
    direction: int,
    owner_keys: Iterable[int],
    *,
    schema_name: str = "mrf",
    max_members: int | None = None,
) -> dict[int, tuple[int, ...]]:
    """Read graph members directly and optionally bound each owner result."""

    fetch_options_by_name: dict[str, Any] = {}
    if max_members is not None:
        fetch_options_by_name["max_members"] = max_members
    try:
        return await fetch_shared_graph_members(
            session,
            schema_name=schema_name,
            snapshot_key=_required_shared_snapshot_key(shared_snapshot_key),
            direction=int(direction),
            owner_keys=owner_keys,
            **fetch_options_by_name,
        )
    except PTG2SharedBlockError as exc:
        raise PTG2ManifestArtifactError(str(exc)) from exc


# Compatibility for callers that imported the pre-readability-ratchet name.
lookup_serving_binary_by_code_prefix_from_db = lookup_code_prefix_rows_from_db
