# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Packed coordinate-to-hash roots for strict PTG V4 snapshot layouts."""

from __future__ import annotations

import contextvars
import hashlib
import json
import os
import struct
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_BUILD_LEASE_SECONDS_DEFAULT,
    PTG2_V3_BUILD_LEASE_SECONDS_ENV,
    PTG2_V3_SEALED_LEASE_SECONDS_DEFAULT,
    PTG2_V3_SEALED_LEASE_SECONDS_ENV,
    PTG2_V3_SHARED_FORMAT_VERSION,
    SealedSharedLayout,
    SharedBlock,
    SharedBlockReference,
    SharedLayoutReservation,
    delete_shared_layout_dense_rows,
    shared_block_hash,
)


PTG2_V4_SHARED_GENERATION = "shared_blocks_v4"
PTG2_V4_MAP_FORMAT_VERSION = 1
PTG2_V4_MAP_FORMAT = "packed_coordinate_hash_v1"
PTG2_V4_MAP_BLOCK_KIND = "snapshot_coordinate_map_v1"
PTG2_V4_PROJECTION_ID_SCOPE = "snapshot_local_v1"
PTG2_V4_MAP_MANIFEST_CONTRACT = "ptg_v4_packed_snapshot_map_v1"
PTG2_V4_PROVIDER_GRAPH_CONTRACT = "ptg2_provider_graph_v4"
PTG2_V4_OWNER_LOCATOR_PAGE_CONTRACT = "packed_owner_locator_page_v1"
PTG2_V4_MEMBER_PAGE_CONTRACT = "packed_member_page_v1"
PTG2_V4_NPI_TABLE = "ptg2_v4_npi_scope"
PTG2_V4_COMPONENT_TABLE = "ptg2_v4_provider_component"
PTG2_V4_PATTERN_TABLE = "ptg2_v4_pattern"
PTG2_V4_RELATION_MANIFEST_TABLE = "ptg2_v4_relation_manifest"
PTG2_V4_HEAVY_OWNER_TABLE = "ptg2_v4_heavy_owner"
PTG2_V4_NPI_PREFIX_TABLE = "ptg2_v4_provider_set_npi_prefix"
PTG2_V4_GRAPH_DIAGNOSTIC_TABLE = "ptg2_v4_provider_graph_diagnostic"
PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS = (
    "npi_prefix_target",
    "max_set_patterns_per_set",
    "max_set_components_per_fallback_set",
    "max_online_group_keys_per_set",
    "max_online_source_owners_per_set",
    "max_online_source_members_per_set",
    "max_online_source_pages_per_set",
    "max_online_source_bytes_per_set",
    "online_group_npi_batch_size",
    "max_online_group_npi_members_per_set",
    "max_online_group_npi_locator_pages_per_set",
    "max_online_group_npi_member_pages_per_set",
    "max_online_group_npi_bytes_per_set",
    "max_online_group_npi_batches_per_set",
    "provider_expansion_rate_page_rows",
    "max_online_provider_expansion_rate_rows",
    "max_online_provider_expansion_provider_sets",
    "max_online_provider_expansion_graph_batches",
    "maximum_group_npi_member_work",
    "maximum_group_npi_locator_page_work",
    "maximum_group_npi_member_page_work",
    "maximum_group_npi_byte_work",
    "maximum_group_npi_batch_work",
    "group_unsafe_set_count",
    "physical_unsafe_set_count",
    "simulated_set_count",
    "override_owner_count",
    "override_member_count",
    "override_raw_bytes",
    "worst_provider_set_key",
    "worst_groups_to_target",
    "worst_uses_override",
    "worst_uses_component_fallback",
    "worst_member_count",
    "worst_member_digest",
    "worst_source_owner_work",
    "worst_source_member_work",
    "worst_source_page_work",
    "worst_source_byte_work",
    "worst_group_npi_member_work",
    "worst_group_npi_locator_page_work",
    "worst_group_npi_member_page_work",
    "worst_group_npi_byte_work",
    "worst_group_npi_batch_work",
    "worst_online_provider_set_key",
    "worst_online_groups_to_target",
    "worst_online_groups_to_target_exact",
    "worst_online_uses_component_fallback",
    "worst_online_group_work_bound",
    "worst_online_member_count",
    "worst_online_member_digest",
    "worst_online_source_owner_work",
    "worst_online_source_member_work",
    "worst_online_source_page_work",
    "worst_online_source_byte_work",
    "worst_online_group_npi_member_work",
    "worst_online_group_npi_locator_page_work",
    "worst_online_group_npi_member_page_work",
    "worst_online_group_npi_byte_work",
    "worst_online_group_npi_batch_work",
)
PTG2_V4_GRAPH_RESOURCE_FIELDS = (
    "compressed_acquisition_bytes",
    "input_factor_bytes",
    "factor_edge_count",
    "empty_npi_tin_only_normalization_count",
)
PTG2_V4_REPRESENTATIONS = frozenset({"direct_v1", "pattern_v1"})
# The compiler observes source-component geometry, but this release does not
# publish or serve it as an authoritative root representation.
PTG2_V4_FUTURE_REPRESENTATIONS = frozenset({"source_component_v1"})
PTG2_V4_DEFAULT_COORDINATES_PER_PACK = 256
PTG2_V4_MAX_COORDINATES_PER_PACK = 65_536
PTG2_V4_METADATA_RANGE_ROWS = 10_000


_MAP_MAGIC = b"PTG4MAP1"
_MAP_HEADER = struct.Struct(">8sHHI64s")
_MAP_RECORD = struct.Struct(">QIQ32s")
_MAP_DIGEST_DOMAIN = b"PTG2V4COORDINATEMAP\x01"
_LAYOUT_FINGERPRINT_DOMAIN = b"PTG2V4PHYSICALLAYOUT\x01"
_V4_SEAL_PROGRESS_CALLBACK: contextvars.ContextVar[
    Callable[[str, int], None] | None
] = contextvars.ContextVar("ptg2_v4_seal_progress_callback", default=None)


@contextmanager
def observe_v4_seal_progress(
    progress_callback: Callable[[str, int], None] | None,
):
    """Bind measured seal progress to the current async task."""

    token = _V4_SEAL_PROGRESS_CALLBACK.set(progress_callback)
    try:
        yield
    finally:
        _V4_SEAL_PROGRESS_CALLBACK.reset(token)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _env_non_negative_seconds(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(int(str(raw_value).strip()), 0)
    except ValueError:
        return default


def _lease_deadline(*, sealed: bool = False) -> datetime:
    name = PTG2_V3_SEALED_LEASE_SECONDS_ENV if sealed else PTG2_V3_BUILD_LEASE_SECONDS_ENV
    default = (
        PTG2_V3_SEALED_LEASE_SECONDS_DEFAULT
        if sealed
        else PTG2_V3_BUILD_LEASE_SECONDS_DEFAULT
    )
    return _utcnow() + timedelta(seconds=_env_non_negative_seconds(name, default))


def v4_layout_advisory_lock_key(digest: bytes) -> int:
    """Return the transaction-lock key for one persisted V4 fingerprint."""

    if len(digest) != 32:
        raise ValueError("PTG V4 layout fingerprint must contain 32 bytes")
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _length_prefixed(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def v4_layout_fingerprint(semantic_fingerprint: bytes) -> bytes:
    """Namespace logical content for V4 physical-layout reuse.

    Dense pattern/component identifiers are intentionally absent.  They are
    snapshot-local projection coordinates and must not become logical identity.
    """

    normalized = bytes(semantic_fingerprint)
    if len(normalized) != 32:
        raise ValueError("PTG V4 semantic fingerprint must contain 32 bytes")
    return hashlib.sha256(
        _LAYOUT_FINGERPRINT_DOMAIN
        + _length_prefixed(PTG2_V4_SHARED_GENERATION.encode("ascii"))
        + _length_prefixed(PTG2_V4_MAP_FORMAT.encode("ascii"))
        + normalized
    ).digest()


@dataclass(frozen=True)
class V4SnapshotMapCoordinate:
    object_kind: str
    block_key: int
    fragment_no: int
    entry_count: int
    block_hash: bytes

    def __post_init__(self) -> None:
        kind_bytes = str(self.object_kind or "").encode("utf-8")
        if not kind_bytes or len(kind_bytes) > 64:
            raise ValueError("PTG V4 map object_kind must contain 1 to 64 UTF-8 bytes")
        if not 0 <= int(self.block_key) <= (2**63 - 1):
            raise ValueError("PTG V4 map block_key is outside PostgreSQL bigint range")
        if not 0 <= int(self.fragment_no) <= (2**31 - 1):
            raise ValueError("PTG V4 map fragment_no is outside PostgreSQL integer range")
        if not 0 <= int(self.entry_count) <= (2**63 - 1):
            raise ValueError("PTG V4 map entry_count is outside PostgreSQL bigint range")
        if len(bytes(self.block_hash)) != 32:
            raise ValueError("PTG V4 map block_hash must contain 32 bytes")

    @property
    def coordinate(self) -> tuple[str, int, int]:
        """Return the stable object-kind and block coordinate."""

        return (self.object_kind, int(self.block_key), int(self.fragment_no))


@dataclass(frozen=True)
class V4SnapshotMapPack:
    object_kind: str
    pack_no: int
    references: tuple[SharedBlockReference, ...]
    map_block: SharedBlock
    logical_byte_count: int

    @property
    def first_coordinate(self) -> tuple[int, int]:
        """Return the first block coordinate covered by this pack."""

        first = self.references[0]
        return (int(first.block_key), int(first.fragment_no))

    @property
    def last_coordinate(self) -> tuple[int, int]:
        """Return the last block coordinate covered by this pack."""

        last = self.references[-1]
        return (int(last.block_key), int(last.fragment_no))

    @property
    def coordinate_count(self) -> int:
        """Return the number of coordinate records in this pack."""

        return len(self.references)

    @property
    def entry_count(self) -> int:
        """Return the logical entries addressed by this pack."""

        return sum(int(reference.entry_count) for reference in self.references)


@dataclass(frozen=True)
class V4SnapshotMapSummary:
    map_digest: bytes
    object_kinds: tuple[str, ...]
    map_pack_count: int
    coordinate_count: int
    entry_count: int
    logical_byte_count: int
    stored_map_byte_count: int

    @property
    def object_kind_count(self) -> int:
        """Return the number of object kinds covered by the map."""

        return len(self.object_kinds)


@dataclass(frozen=True)
class V4NPIDictionaryPublication:
    row_count: int


@dataclass(frozen=True)
class V4SnapshotMetadataSummary:
    npi_count: int
    component_count: int
    pattern_count: int
    relation_count: int
    heavy_owner_count: int
    provider_graph_diagnostics: Mapping[str, Any] = field(default_factory=dict)
    provider_graph_resources: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class V4MetadataTablePublication:
    row_count: int


_SUMMARY_FIELDS = (
    "map_digest",
    "object_kinds",
    "map_pack_count",
    "coordinate_count",
    "entry_count",
    "logical_byte_count",
    "stored_map_byte_count",
)


class _V4SnapshotMapSummaryAccumulator:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(_MAP_DIGEST_DOMAIN)
        self._previous_coordinate: tuple[str, int, int] | None = None
        self._object_kinds: set[str] = set()
        self._map_pack_count = 0
        self._coordinate_count = 0
        self._entry_count = 0
        self._logical_byte_count = 0
        self._stored_map_byte_count = 0

    def add_pack(self, pack: V4SnapshotMapPack) -> None:
        """Include one ordered map pack in the canonical summary."""

        self._map_pack_count += 1
        self._logical_byte_count += int(pack.logical_byte_count)
        self._stored_map_byte_count += pack.map_block.stored_byte_count
        self._object_kinds.add(pack.object_kind)
        for reference in pack.references:
            coordinate = (
                reference.object_kind,
                int(reference.block_key),
                int(reference.fragment_no),
            )
            if self._previous_coordinate is not None and coordinate <= self._previous_coordinate:
                raise ValueError("PTG V4 snapshot mappings must be strictly ordered")
            self._previous_coordinate = coordinate
            kind_bytes = reference.object_kind.encode("utf-8")
            self._digest.update(_length_prefixed(kind_bytes))
            self._digest.update(struct.pack(">Q", int(reference.block_key)))
            self._digest.update(struct.pack(">I", int(reference.fragment_no)))
            self._digest.update(struct.pack(">Q", int(reference.entry_count)))
            self._digest.update(bytes(reference.block_hash))
            self._coordinate_count += 1
            self._entry_count += int(reference.entry_count)

    def finish(self) -> V4SnapshotMapSummary:
        """Finish a nonempty canonical map summary."""

        if self._coordinate_count <= 0:
            raise ValueError("PTG V4 snapshot map cannot be empty")
        return V4SnapshotMapSummary(
            map_digest=self._digest.digest(),
            object_kinds=tuple(sorted(self._object_kinds)),
            map_pack_count=self._map_pack_count,
            coordinate_count=self._coordinate_count,
            entry_count=self._entry_count,
            logical_byte_count=self._logical_byte_count,
            stored_map_byte_count=self._stored_map_byte_count,
        )


def _coordinate_from_reference(
    reference: SharedBlockReference,
) -> V4SnapshotMapCoordinate:
    return V4SnapshotMapCoordinate(
        object_kind=str(reference.object_kind),
        block_key=int(reference.block_key),
        fragment_no=int(reference.fragment_no),
        entry_count=int(reference.entry_count),
        block_hash=bytes(reference.block_hash),
    )


def encode_v4_snapshot_map_pack(
    object_kind: str,
    references: Sequence[SharedBlockReference],
) -> bytes:
    """Encode one deterministic fixed-width coordinate-to-hash pack."""

    normalized_kind = str(object_kind or "")
    kind_bytes = normalized_kind.encode("utf-8")
    if not kind_bytes or len(kind_bytes) > 64:
        raise ValueError("PTG V4 map object_kind must contain 1 to 64 UTF-8 bytes")
    if not references or len(references) > PTG2_V4_MAX_COORDINATES_PER_PACK:
        raise ValueError("PTG V4 map pack coordinate count is outside the supported range")
    encoded_pack = bytearray(
        _MAP_HEADER.pack(
            _MAP_MAGIC,
            PTG2_V4_MAP_FORMAT_VERSION,
            len(kind_bytes),
            len(references),
            kind_bytes.ljust(64, b"\0"),
        )
    )
    previous: tuple[int, int] | None = None
    for reference in references:
        coordinate = _coordinate_from_reference(reference)
        if coordinate.object_kind != normalized_kind:
            raise ValueError("PTG V4 map pack mixes object kinds")
        local_coordinate = (coordinate.block_key, coordinate.fragment_no)
        if previous is not None and local_coordinate <= previous:
            raise ValueError("PTG V4 map pack coordinates must be strictly ordered")
        previous = local_coordinate
        encoded_pack.extend(
            _MAP_RECORD.pack(
                coordinate.block_key,
                coordinate.fragment_no,
                coordinate.entry_count,
                coordinate.block_hash,
            )
        )
    return bytes(encoded_pack)


def decode_v4_snapshot_map_pack(
    encoded_pack: bytes,
    *,
    expected_object_kind: str | None = None,
) -> tuple[V4SnapshotMapCoordinate, ...]:
    """Decode and fully validate one fixed-width map pack."""

    raw_payload = bytes(encoded_pack)
    if len(raw_payload) < _MAP_HEADER.size:
        raise ValueError("PTG V4 map pack is truncated")
    magic, version, kind_length, coordinate_count, padded_kind = _MAP_HEADER.unpack_from(
        raw_payload
    )
    if magic != _MAP_MAGIC or version != PTG2_V4_MAP_FORMAT_VERSION:
        raise ValueError("PTG V4 map pack has an incompatible format")
    if not 1 <= int(kind_length) <= 64 or any(padded_kind[kind_length:]):
        raise ValueError("PTG V4 map pack has an invalid object_kind field")
    try:
        object_kind = padded_kind[:kind_length].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("PTG V4 map pack object_kind is not valid UTF-8") from exc
    if expected_object_kind is not None and object_kind != expected_object_kind:
        raise ValueError("PTG V4 map pack object_kind does not match its root")
    if not 1 <= int(coordinate_count) <= PTG2_V4_MAX_COORDINATES_PER_PACK:
        raise ValueError("PTG V4 map pack has an invalid coordinate count")
    expected_size = _MAP_HEADER.size + int(coordinate_count) * _MAP_RECORD.size
    if len(raw_payload) != expected_size:
        raise ValueError("PTG V4 map pack byte count does not match its header")
    coordinates: list[V4SnapshotMapCoordinate] = []
    previous: tuple[int, int] | None = None
    offset = _MAP_HEADER.size
    for _index in range(int(coordinate_count)):
        block_key, fragment_no, entry_count, block_hash = _MAP_RECORD.unpack_from(
            raw_payload,
            offset,
        )
        offset += _MAP_RECORD.size
        coordinate = V4SnapshotMapCoordinate(
            object_kind=object_kind,
            block_key=block_key,
            fragment_no=fragment_no,
            entry_count=entry_count,
            block_hash=block_hash,
        )
        local_coordinate = (coordinate.block_key, coordinate.fragment_no)
        if previous is not None and local_coordinate <= previous:
            raise ValueError("PTG V4 map pack coordinates are not strictly ordered")
        previous = local_coordinate
        coordinates.append(coordinate)
    return tuple(coordinates)


def v4_map_pack_target_hashes(encoded_pack: bytes) -> tuple[bytes, ...]:
    """Expose exact CAS reachability for lifecycle/GC integration."""

    return tuple(
        coordinate.block_hash
        for coordinate in decode_v4_snapshot_map_pack(encoded_pack)
    )


def _make_map_pack(
    *,
    object_kind: str,
    pack_no: int,
    references: Sequence[SharedBlockReference],
) -> V4SnapshotMapPack:
    normalized_references = tuple(references)
    encoded_pack = encode_v4_snapshot_map_pack(object_kind, normalized_references)
    return V4SnapshotMapPack(
        object_kind=object_kind,
        pack_no=int(pack_no),
        references=normalized_references,
        map_block=SharedBlock(
            object_kind=PTG2_V4_MAP_BLOCK_KIND,
            block_key=int(pack_no),
            fragment_no=0,
            entry_count=len(normalized_references),
            codec="none",
            raw_byte_count=len(encoded_pack),
            payload=encoded_pack,
        ),
        logical_byte_count=sum(
            int(reference.raw_byte_count) for reference in normalized_references
        ),
    )


def iter_v4_snapshot_map_packs(
    references: Iterable[SharedBlockReference],
    *,
    max_coordinates_per_pack: int = PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
) -> Iterator[V4SnapshotMapPack]:
    """Pack a strictly ordered reference stream with bounded retained state."""

    pack_limit = int(max_coordinates_per_pack)
    if not 1 <= pack_limit <= PTG2_V4_MAX_COORDINATES_PER_PACK:
        raise ValueError("PTG V4 coordinate pack limit is outside the supported range")
    active_kind: str | None = None
    active_pack_no = 0
    active_references: list[SharedBlockReference] = []
    previous_coordinate: tuple[str, int, int] | None = None

    for reference in references:
        _coordinate_from_reference(reference)
        coordinate = (
            str(reference.object_kind),
            int(reference.block_key),
            int(reference.fragment_no),
        )
        if previous_coordinate is not None and coordinate <= previous_coordinate:
            raise ValueError("PTG V4 snapshot mappings must be strictly ordered")
        previous_coordinate = coordinate
        if active_kind is None:
            active_kind = coordinate[0]
        if coordinate[0] != active_kind or len(active_references) >= pack_limit:
            yield _make_map_pack(
                object_kind=active_kind,
                pack_no=active_pack_no,
                references=active_references,
            )
            if coordinate[0] != active_kind:
                active_kind = coordinate[0]
                active_pack_no = 0
            else:
                active_pack_no += 1
            active_references = []
        active_references.append(reference)

    if active_kind is not None:
        yield _make_map_pack(
            object_kind=active_kind,
            pack_no=active_pack_no,
            references=active_references,
        )


def summarize_v4_snapshot_map_packs(
    packs: Iterable[V4SnapshotMapPack],
) -> V4SnapshotMapSummary:
    """Return the canonical mapping digest independently of pack boundaries."""

    accumulator = _V4SnapshotMapSummaryAccumulator()
    for pack in packs:
        accumulator.add_pack(pack)
    return accumulator.finish()


def _summary_manifest(
    summary: V4SnapshotMapSummary,
    *,
    representation: str,
    metadata: V4SnapshotMetadataSummary,
) -> dict[str, Any]:
    return {
        "contract": PTG2_V4_MAP_MANIFEST_CONTRACT,
        "format_version": PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": PTG2_V4_MAP_FORMAT,
        "representation": str(representation),
        "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
        "root_table": "ptg2_v4_snapshot_map_root",
        "pack_table": "ptg2_v4_snapshot_map_pack",
        "npi_table": PTG2_V4_NPI_TABLE,
        "npi_key_column": "npi_key",
        "component_table": PTG2_V4_COMPONENT_TABLE,
        "pattern_table": PTG2_V4_PATTERN_TABLE,
        "relation_manifest_table": PTG2_V4_RELATION_MANIFEST_TABLE,
        "heavy_owner_table": PTG2_V4_HEAVY_OWNER_TABLE,
        "map_block_kind": PTG2_V4_MAP_BLOCK_KIND,
        "map_digest": summary.map_digest.hex(),
        "object_kinds": list(summary.object_kinds),
        "object_kind_count": summary.object_kind_count,
        "map_pack_count": summary.map_pack_count,
        "coordinate_count": summary.coordinate_count,
        "entry_count": summary.entry_count,
        "logical_byte_count": summary.logical_byte_count,
        "stored_map_byte_count": summary.stored_map_byte_count,
        "npi_count": metadata.npi_count,
        "component_count": metadata.component_count,
        "pattern_count": metadata.pattern_count,
        "relation_count": metadata.relation_count,
        "heavy_owner_count": metadata.heavy_owner_count,
    }


def _manifest_copy_with_index(
    layout_manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized = json.loads(
        json.dumps(
            dict(layout_manifest),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    raw_serving_index = normalized.get("serving_index")
    if raw_serving_index is None:
        serving_index: dict[str, Any] = {}
    elif isinstance(raw_serving_index, dict):
        serving_index = dict(raw_serving_index)
    else:
        raise ValueError("PTG V4 layout manifest serving_index must be an object")
    existing_generation = serving_index.get("storage_generation")
    if existing_generation not in (
        None,
        "shared_blocks_v3",
        PTG2_V4_SHARED_GENERATION,
    ):
        raise ValueError("PTG V4 layout manifest has another storage generation")
    return normalized, serving_index


def _apply_v4_index_markers(serving_index: dict[str, Any]) -> None:
    marker_by_field = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": PTG2_V4_SHARED_GENERATION,
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
    }
    compatible_existing_by_field = {
        "arch_version": {None, "postgres_binary_v3"},
        "type": {None, "ptg2_shared_blocks_v3", "ptg2_shared_blocks_v4"},
        "storage_generation": {
            None,
            "shared_blocks_v3",
            PTG2_V4_SHARED_GENERATION,
        },
        "provider_scope_strategy": {
            None,
            "postgres_shared_graph",
            "postgres_packed_graph_v4",
        },
        "shared_block_layout": {
            None,
            "dense_shared_blocks_v3",
            "packed_snapshot_maps_v4",
        },
    }
    for field_name, expected_value in marker_by_field.items():
        if serving_index.get(field_name) not in compatible_existing_by_field[field_name]:
            raise ValueError(f"PTG V4 layout manifest has incompatible {field_name}")
        serving_index[field_name] = expected_value


def _provider_graph_v4_map(
    representation: str,
    summary: V4SnapshotMapSummary,
    metadata: V4SnapshotMetadataSummary,
) -> dict[str, Any]:
    if set(metadata.provider_graph_resources) != set(
        PTG2_V4_GRAPH_RESOURCE_FIELDS
    ):
        raise ValueError("PTG V4 graph resource admission is invalid")
    resources_by_field = {
        field_name: int(metadata.provider_graph_resources[field_name])
        for field_name in PTG2_V4_GRAPH_RESOURCE_FIELDS
    }
    if (
        resources_by_field["compressed_acquisition_bytes"] <= 0
        or resources_by_field["input_factor_bytes"] < 0
        or resources_by_field["factor_edge_count"] < 0
        or resources_by_field["empty_npi_tin_only_normalization_count"] < 0
    ):
        raise ValueError("PTG V4 graph resource admission is invalid")
    return {
        "contract": PTG2_V4_PROVIDER_GRAPH_CONTRACT,
        "representation": str(representation),
        "map_format": PTG2_V4_MAP_FORMAT,
        "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": summary.map_digest.hex(),
        "locator_page_contract": PTG2_V4_OWNER_LOCATOR_PAGE_CONTRACT,
        "member_page_contract": PTG2_V4_MEMBER_PAGE_CONTRACT,
        "npi_table": PTG2_V4_NPI_TABLE,
        "component_table": PTG2_V4_COMPONENT_TABLE,
        "pattern_table": PTG2_V4_PATTERN_TABLE,
        "relation_manifest_table": PTG2_V4_RELATION_MANIFEST_TABLE,
        "heavy_owner_table": PTG2_V4_HEAVY_OWNER_TABLE,
        "npi_prefix_table": PTG2_V4_NPI_PREFIX_TABLE,
        "diagnostic_table": PTG2_V4_GRAPH_DIAGNOSTIC_TABLE,
        "hot_prefix": dict(metadata.provider_graph_diagnostics),
        "resource_admission": resources_by_field,
    }


def _serving_binary_v4_map(
    serving_index: Mapping[str, Any],
    *,
    representation: str,
    summary: V4SnapshotMapSummary,
    metadata: V4SnapshotMetadataSummary,
) -> dict[str, Any]:
    raw_serving_binary = serving_index.get("serving_binary")
    if raw_serving_binary is None:
        serving_binary_map: dict[str, Any] = {}
    elif isinstance(raw_serving_binary, dict):
        serving_binary_map = dict(raw_serving_binary)
    else:
        raise ValueError("PTG V4 layout manifest serving_binary must be an object")
    if serving_binary_map.get("format") not in (None, "postgres_binary_v3"):
        raise ValueError("PTG V4 price serving binary must remain postgres_binary_v3")
    serving_binary_map["format"] = "postgres_binary_v3"
    provider_graph_v4_map = _provider_graph_v4_map(
        representation, summary, metadata
    )
    existing_provider_graph = serving_binary_map.get("provider_graph_v4")
    if existing_provider_graph not in (None, provider_graph_v4_map):
        raise ValueError("PTG V4 layout manifest has conflicting provider_graph_v4")
    serving_binary_map["provider_graph_v4"] = provider_graph_v4_map
    return serving_binary_map


def _manifest_with_v4_root(
    layout_manifest: Mapping[str, Any],
    *,
    representation: str,
    summary: V4SnapshotMapSummary,
    metadata: V4SnapshotMetadataSummary,
) -> dict[str, Any]:
    """Return a canonical manifest containing the validated V4 root."""

    normalized, serving_index = _manifest_copy_with_index(layout_manifest)
    expected_root = _summary_manifest(
        summary,
        representation=representation,
        metadata=metadata,
    )
    existing_root = serving_index.get("snapshot_map")
    if existing_root not in (None, expected_root):
        raise ValueError("PTG V4 layout manifest has conflicting snapshot-map metadata")
    _apply_v4_index_markers(serving_index)
    serving_index["serving_binary"] = _serving_binary_v4_map(
        serving_index,
        representation=representation,
        summary=summary,
        metadata=metadata,
    )
    serving_index["snapshot_map"] = expected_root
    normalized["serving_index"] = serving_index
    return normalized


def _validate_v4_manifest_root(
    layout_manifest: Mapping[str, Any],
    *,
    representation: str,
    summary: V4SnapshotMapSummary,
    metadata: V4SnapshotMetadataSummary,
) -> None:
    serving_index = layout_manifest.get("serving_index")
    if not isinstance(serving_index, Mapping):
        raise RuntimeError("sealed PTG V4 layout manifest has no serving_index")
    expected_marker_by_field = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": PTG2_V4_SHARED_GENERATION,
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
    }
    if any(
        serving_index.get(field_name) != expected_value
        for field_name, expected_value in expected_marker_by_field.items()
    ):
        raise RuntimeError("sealed PTG V4 layout manifest markers are inconsistent")
    expected_root = _summary_manifest(
        summary,
        representation=representation,
        metadata=metadata,
    )
    if serving_index.get("snapshot_map") != expected_root:
        raise RuntimeError("sealed PTG V4 layout manifest root is inconsistent")
    serving_binary_map = serving_index.get("serving_binary")
    if not isinstance(serving_binary_map, Mapping) or serving_binary_map.get("format") != (
        "postgres_binary_v3"
    ):
        raise RuntimeError("sealed PTG V4 price serving binary is inconsistent")
    expected_provider_graph_map = _provider_graph_v4_map(
        representation, summary, metadata
    )
    if serving_binary_map.get("provider_graph_v4") != expected_provider_graph_map:
        raise RuntimeError("sealed PTG V4 provider graph manifest is inconsistent")


def _summary_from_root_row(root: Mapping[str, Any]) -> V4SnapshotMapSummary:
    raw_object_kinds = root.get("object_kinds")
    object_kinds = tuple(
        str(object_kind) for object_kind in (raw_object_kinds or ())
    )
    return V4SnapshotMapSummary(
        map_digest=bytes(root.get("map_digest") or b""),
        object_kinds=object_kinds,
        map_pack_count=int(root.get("map_pack_count") or 0),
        coordinate_count=int(root.get("coordinate_count") or 0),
        entry_count=int(root.get("entry_count") or 0),
        logical_byte_count=int(root.get("logical_byte_count") or 0),
        stored_map_byte_count=int(root.get("stored_map_byte_count") or 0),
    )


def _require_matching_summaries(
    expected: V4SnapshotMapSummary,
    observed: V4SnapshotMapSummary,
    *,
    context: str,
) -> None:
    for field_name in _SUMMARY_FIELDS:
        expected_value = getattr(expected, field_name)
        observed_value = getattr(observed, field_name)
        if observed_value != expected_value:
            raise RuntimeError(
                f"PTG V4 {context} differs for {field_name}: "
                f"expected {expected_value!r}, observed {observed_value!r}"
            )


@dataclass
class _MapSummaryCursor:
    after_kind: str = ""
    after_pack_no: int = -1
    previous_kind: str | None = None
    previous_pack_no: int = -1
    previous_last_coordinate: tuple[int, int] | None = None


async def _load_persisted_map_rows(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    cursor: _MapSummaryCursor,
    batch_size: int,
) -> list[dict[str, Any]]:
    pack_result = await session.execute(
        text(
            f"""
            SELECT mapping.object_kind, mapping.pack_no,
                   mapping.first_block_key, mapping.first_fragment_no,
                   mapping.last_block_key, mapping.last_fragment_no,
                   mapping.coordinate_count, mapping.entry_count,
                   mapping.logical_byte_count, mapping.map_block_hash,
                   block.format_version AS map_format_version,
                   block.object_kind AS map_object_kind,
                   block.codec AS map_codec,
                   block.entry_count AS map_entry_count,
                   block.raw_byte_count AS map_raw_byte_count,
                   block.stored_byte_count AS map_stored_byte_count,
                   block.payload AS map_payload
              FROM {schema}.ptg2_v4_snapshot_map_pack AS mapping
              JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = mapping.map_block_hash
             WHERE mapping.snapshot_key = :snapshot_key
               AND (
                    mapping.object_kind > :after_kind
                    OR (
                        mapping.object_kind = :after_kind
                        AND mapping.pack_no > :after_pack_no
                    )
               )
             ORDER BY mapping.object_kind, mapping.pack_no
             LIMIT :batch_rows
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "after_kind": cursor.after_kind,
            "after_pack_no": int(cursor.after_pack_no),
            "batch_rows": int(batch_size),
        },
    )
    return [_row_mapping(pack_record) for pack_record in pack_result]


def _advance_map_pack_sequence(
    pack_row: Mapping[str, Any],
    cursor: _MapSummaryCursor,
) -> str:
    object_kind = str(pack_row.get("object_kind") or "")
    pack_no = int(pack_row.get("pack_no") or 0)
    if object_kind == cursor.previous_kind:
        if pack_no != cursor.previous_pack_no + 1:
            raise RuntimeError("PTG V4 map packs are not contiguously numbered")
    else:
        if cursor.previous_kind is not None and object_kind <= cursor.previous_kind:
            raise RuntimeError("PTG V4 map object kinds are not strictly ordered")
        if pack_no != 0:
            raise RuntimeError("PTG V4 map object-kind packs must begin at zero")
        cursor.previous_last_coordinate = None
    cursor.previous_kind = object_kind
    cursor.previous_pack_no = pack_no
    return object_kind


def _decode_persisted_map_payload(
    pack_row: Mapping[str, Any],
    *,
    object_kind: str,
) -> tuple[V4SnapshotMapCoordinate, ...]:
    map_payload = bytes(pack_row.get("map_payload") or b"")
    if (
        int(pack_row.get("map_format_version") or -1)
        != PTG2_V3_SHARED_FORMAT_VERSION
        or pack_row.get("map_object_kind") != PTG2_V4_MAP_BLOCK_KIND
        or pack_row.get("map_codec") != "none"
        or int(pack_row.get("map_raw_byte_count") or -1) != len(map_payload)
        or int(pack_row.get("map_stored_byte_count") or -1) != len(map_payload)
    ):
        raise RuntimeError("PTG V4 map pack has incompatible CAS metadata")
    expected_map_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        codec="none",
        payload=map_payload,
    )
    if bytes(pack_row.get("map_block_hash") or b"") != expected_map_hash:
        raise RuntimeError("PTG V4 map pack CAS hash does not match its payload")
    coordinates = decode_v4_snapshot_map_pack(
        map_payload,
        expected_object_kind=object_kind,
    )
    if int(pack_row.get("map_entry_count") or -1) != len(coordinates):
        raise RuntimeError("PTG V4 map block entry count is inconsistent")
    return coordinates


def _validate_map_pack_coordinates(
    pack_row: Mapping[str, Any],
    coordinates: Sequence[V4SnapshotMapCoordinate],
    cursor: _MapSummaryCursor,
) -> None:
    first_coordinate = (coordinates[0].block_key, coordinates[0].fragment_no)
    last_coordinate = (coordinates[-1].block_key, coordinates[-1].fragment_no)
    stored_first = (
        int(pack_row.get("first_block_key") or 0),
        int(pack_row.get("first_fragment_no") or 0),
    )
    stored_last = (
        int(pack_row.get("last_block_key") or 0),
        int(pack_row.get("last_fragment_no") or 0),
    )
    if first_coordinate != stored_first or last_coordinate != stored_last:
        raise RuntimeError("PTG V4 map pack coordinate bounds are inconsistent")
    if (
        cursor.previous_last_coordinate is not None
        and first_coordinate <= cursor.previous_last_coordinate
    ):
        raise RuntimeError("PTG V4 map pack coordinate ranges overlap")
    cursor.previous_last_coordinate = last_coordinate
    if int(pack_row.get("coordinate_count") or 0) != len(coordinates):
        raise RuntimeError("PTG V4 map pack coordinate count is inconsistent")
    if int(pack_row.get("entry_count") or 0) != sum(
        coordinate.entry_count for coordinate in coordinates
    ):
        raise RuntimeError("PTG V4 map pack logical entry count is inconsistent")


def _decode_persisted_map_rows(
    pack_rows: Sequence[Mapping[str, Any]],
    cursor: _MapSummaryCursor,
) -> tuple[
    list[tuple[Mapping[str, Any], tuple[V4SnapshotMapCoordinate, ...]]],
    set[bytes],
]:
    decoded_rows: list[
        tuple[Mapping[str, Any], tuple[V4SnapshotMapCoordinate, ...]]
    ] = []
    target_hashes: set[bytes] = set()
    for pack_row in pack_rows:
        object_kind = _advance_map_pack_sequence(pack_row, cursor)
        coordinates = _decode_persisted_map_payload(
            pack_row,
            object_kind=object_kind,
        )
        _validate_map_pack_coordinates(pack_row, coordinates, cursor)
        target_hashes.update(coordinate.block_hash for coordinate in coordinates)
        decoded_rows.append((pack_row, coordinates))
    return decoded_rows, target_hashes


async def _load_target_metadata(
    session: Any,
    *,
    schema: str,
    target_hashes: set[bytes],
) -> dict[bytes, tuple[str, int, int]]:
    target_result = await session.execute(
        text(
            f"""
            SELECT block_hash, format_version, object_kind, codec,
                   entry_count, raw_byte_count, stored_byte_count
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
             FOR KEY SHARE
            """
        ),
        {"block_hashes": list(target_hashes)},
    )
    metadata_by_hash: dict[bytes, tuple[str, int, int]] = {}
    for target_row in target_result:
        target_fields = _row_mapping(target_row)
        if (
            int(target_fields.get("format_version") or -1)
            != PTG2_V3_SHARED_FORMAT_VERSION
        ):
            raise RuntimeError("PTG V4 target CAS block has an incompatible format")
        target_hash = bytes(target_fields.get("block_hash") or b"")
        object_kind = str(target_fields.get("object_kind") or "")
        codec = str(target_fields.get("codec") or "")
        entry_count = int(target_fields.get("entry_count") or 0)
        raw_byte_count = int(target_fields.get("raw_byte_count") or -1)
        stored_byte_count = int(target_fields.get("stored_byte_count") or -1)
        if (
            len(target_hash) != 32
            or not object_kind
            or codec != "none"
            or entry_count < 0
            or raw_byte_count < 0
            or stored_byte_count != raw_byte_count
        ):
            raise RuntimeError("PTG V4 target CAS metadata is invalid")
        metadata_by_hash[target_hash] = (object_kind, entry_count, raw_byte_count)
    if set(metadata_by_hash) != target_hashes:
        raise RuntimeError("PTG V4 map packs could not resolve every target CAS block")
    return metadata_by_hash


def _persisted_map_pack(
    pack_row: Mapping[str, Any],
    coordinates: Sequence[V4SnapshotMapCoordinate],
    metadata_by_hash: Mapping[bytes, tuple[str, int, int]],
) -> V4SnapshotMapPack:
    if any(
        metadata_by_hash[coordinate.block_hash][0] != coordinate.object_kind
        or metadata_by_hash[coordinate.block_hash][1] != int(coordinate.entry_count)
        for coordinate in coordinates
    ):
        raise RuntimeError("PTG V4 target CAS identity differs from its coordinate")
    references = tuple(
        SharedBlockReference(
            object_kind=coordinate.object_kind,
            block_key=coordinate.block_key,
            fragment_no=coordinate.fragment_no,
            entry_count=coordinate.entry_count,
            block_hash=coordinate.block_hash,
            raw_byte_count=metadata_by_hash[coordinate.block_hash][2],
        )
        for coordinate in coordinates
    )
    logical_byte_count = sum(reference.raw_byte_count for reference in references)
    if int(pack_row.get("logical_byte_count") or 0) != logical_byte_count:
        raise RuntimeError("PTG V4 map pack logical byte count is inconsistent")
    map_payload = bytes(pack_row["map_payload"])
    return V4SnapshotMapPack(
        object_kind=str(pack_row["object_kind"]),
        pack_no=int(pack_row["pack_no"]),
        references=references,
        map_block=SharedBlock(
            object_kind=PTG2_V4_MAP_BLOCK_KIND,
            block_key=int(pack_row.get("pack_no") or 0),
            fragment_no=0,
            entry_count=len(references),
            codec="none",
            raw_byte_count=len(map_payload),
            payload=map_payload,
        ),
        logical_byte_count=logical_byte_count,
    )


async def summarize_persisted_v4_snapshot_maps(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    batch_rows: int = 32,
    cancel_gc_candidates: bool = False,
    progress_callback: Callable[[str, int], None] | None = None,
) -> V4SnapshotMapSummary:
    """Recompute exact mapping and CAS reachability with bounded payload memory."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 persisted-map summary batch size must be positive")
    schema = _quote_ident(schema_name)
    cursor = _MapSummaryCursor()
    accumulator = _V4SnapshotMapSummaryAccumulator()

    while True:
        pack_rows = await _load_persisted_map_rows(
            session,
            schema=schema,
            snapshot_key=int(snapshot_key),
            cursor=cursor,
            batch_size=normalized_batch_rows,
        )
        if not pack_rows:
            break
        decoded_rows, target_hashes = _decode_persisted_map_rows(pack_rows, cursor)
        metadata_by_hash = await _load_target_metadata(
            session,
            schema=schema,
            target_hashes=target_hashes,
        )
        if cancel_gc_candidates:
            map_hashes = {
                bytes(pack_row["map_block_hash"]) for pack_row in pack_rows
            }
            await _cancel_map_gc_candidates(
                session,
                schema=schema,
                block_hashes=target_hashes | map_hashes,
            )
        for pack_row, coordinates in decoded_rows:
            accumulator.add_pack(
                _persisted_map_pack(pack_row, coordinates, metadata_by_hash)
            )
        if progress_callback is not None:
            progress_callback("seal_map_packs", len(pack_rows))
            progress_callback(
                "seal_map_coordinates",
                sum(len(coordinates) for _pack_row, coordinates in decoded_rows),
            )
        last_pack_row = pack_rows[-1]
        cursor.after_kind = str(last_pack_row["object_kind"])
        cursor.after_pack_no = int(last_pack_row["pack_no"])

    return accumulator.finish()


def _heavy_locator_requests(
    relations: Mapping[str, Mapping[str, Any]],
    heavy_owners: Mapping[tuple[str, int], Mapping[str, Any]],
) -> tuple[
    dict[str, set[int]],
    dict[tuple[str, int], tuple[str, int, int]],
]:
    requested_by_kind: dict[str, set[int]] = {}
    page_by_owner: dict[tuple[str, int], tuple[str, int, int]] = {}
    for relation_owner in heavy_owners:
        relation_name, owner_key = relation_owner
        relation = relations.get(relation_name)
        if relation is None:
            continue
        owner_base = int(relation["owner_base"])
        owner_span = int(relation["locator_owner_span"])
        page_key = owner_base + ((owner_key - owner_base) // owner_span) * owner_span
        object_kind = str(relation["locator_object_kind"])
        requested_by_kind.setdefault(object_kind, set()).add(page_key)
        page_by_owner[relation_owner] = (
            object_kind,
            page_key,
            owner_key - page_key,
        )
    return requested_by_kind, page_by_owner


async def _load_locator_coordinates(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    requested_by_kind: Mapping[str, set[int]],
) -> dict[tuple[str, int], V4SnapshotMapCoordinate]:
    pack_result = await session.execute(
        text(
            f"""
            SELECT mapping.object_kind, block.payload
              FROM {schema}.ptg2_v4_snapshot_map_pack AS mapping
              JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = mapping.map_block_hash
             WHERE mapping.snapshot_key = :snapshot_key
               AND mapping.object_kind = ANY(CAST(:object_kinds AS text[]))
             ORDER BY mapping.object_kind, mapping.pack_no
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "object_kinds": sorted(requested_by_kind),
        },
    )
    coordinate_by_page: dict[tuple[str, int], V4SnapshotMapCoordinate] = {}
    for raw_pack in pack_result:
        pack = _row_mapping(raw_pack)
        object_kind = str(pack.get("object_kind") or "")
        requested_pages = requested_by_kind.get(object_kind, set())
        coordinates = decode_v4_snapshot_map_pack(
            bytes(pack.get("payload") or b""),
            expected_object_kind=object_kind,
        )
        for coordinate in coordinates:
            page_key = (object_kind, int(coordinate.block_key))
            if coordinate.fragment_no == 0 and coordinate.block_key in requested_pages:
                if page_key in coordinate_by_page:
                    raise RuntimeError("PTG V4 heavy-owner locator page is ambiguous")
                coordinate_by_page[page_key] = coordinate
    expected_pages = {
        (object_kind, page_key)
        for object_kind, page_keys in requested_by_kind.items()
        for page_key in page_keys
    }
    if set(coordinate_by_page) != expected_pages:
        raise RuntimeError("PTG V4 heavy-owner locator page is missing")
    return coordinate_by_page


def _locator_identity_by_hash(
    coordinate_by_page: Mapping[tuple[str, int], V4SnapshotMapCoordinate],
) -> dict[bytes, tuple[str, int]]:
    identity_by_hash: dict[bytes, tuple[str, int]] = {}
    for coordinate in coordinate_by_page.values():
        identity_fields = (coordinate.object_kind, int(coordinate.entry_count))
        previous = identity_by_hash.setdefault(coordinate.block_hash, identity_fields)
        if previous != identity_fields:
            raise RuntimeError("PTG V4 heavy-owner locator CAS identity is ambiguous")
    return identity_by_hash


async def _load_locator_payloads(
    session: Any,
    *,
    schema: str,
    identity_by_hash: Mapping[bytes, tuple[str, int]],
) -> dict[bytes, bytes]:
    block_result = await session.execute(
        text(
            f"""
            SELECT block_hash, format_version, object_kind, codec,
                   entry_count, raw_byte_count, stored_byte_count, payload
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
            """
        ),
        {"block_hashes": list(identity_by_hash)},
    )
    payload_by_hash: dict[bytes, bytes] = {}
    for raw_block in block_result:
        block = _row_mapping(raw_block)
        block_hash = bytes(block.get("block_hash") or b"")
        locator_payload = bytes(block.get("payload") or b"")
        identity_fields = identity_by_hash.get(block_hash)
        if (
            identity_fields is None
            or int(block.get("format_version") or -1)
            != PTG2_V3_SHARED_FORMAT_VERSION
            or str(block.get("object_kind") or "") != identity_fields[0]
            or block.get("codec") != "none"
            or int(block.get("entry_count") or -1) != identity_fields[1]
            or int(block.get("raw_byte_count") or -1) != len(locator_payload)
            or int(block.get("stored_byte_count") or -1) != len(locator_payload)
            or shared_block_hash(
                format_version=PTG2_V3_SHARED_FORMAT_VERSION,
                object_kind=str(block.get("object_kind") or ""),
                codec="none",
                payload=locator_payload,
            )
            != block_hash
        ):
            raise RuntimeError("PTG V4 heavy-owner locator CAS block is invalid")
        payload_by_hash[block_hash] = locator_payload
    return payload_by_hash


def _verify_zero_locator_members(
    *,
    page_by_owner: Mapping[tuple[str, int], tuple[str, int, int]],
    coordinate_by_page: Mapping[tuple[str, int], V4SnapshotMapCoordinate],
    payload_by_hash: Mapping[bytes, bytes],
) -> None:
    locator = struct.Struct("<QI")
    for object_kind, page_key, local_index in page_by_owner.values():
        coordinate = coordinate_by_page[(object_kind, page_key)]
        locator_payload = payload_by_hash.get(coordinate.block_hash)
        if (
            locator_payload is None
            or len(locator_payload) != int(coordinate.entry_count) * locator.size
            or local_index < 0
            or local_index >= int(coordinate.entry_count)
        ):
            raise RuntimeError("PTG V4 heavy-owner locator page is malformed")
        _member_offset, member_count = locator.unpack_from(
            locator_payload,
            local_index * locator.size,
        )
        if member_count != 0:
            raise RuntimeError("PTG V4 heavy owner retains duplicate vector members")


async def _validate_zero_heavy_locators(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    relations: Mapping[str, Mapping[str, Any]],
    heavy_owners: Mapping[tuple[str, int], Mapping[str, Any]],
) -> None:
    """Prove heavy bitmap owners have no duplicate vector members."""

    requested_by_kind, page_by_owner = _heavy_locator_requests(
        relations,
        heavy_owners,
    )
    if not requested_by_kind:
        return
    coordinate_by_page = await _load_locator_coordinates(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        requested_by_kind=requested_by_kind,
    )
    identity_by_hash = _locator_identity_by_hash(coordinate_by_page)
    payload_by_hash = await _load_locator_payloads(
        session,
        schema=schema,
        identity_by_hash=identity_by_hash,
    )
    _verify_zero_locator_members(
        page_by_owner=page_by_owner,
        coordinate_by_page=coordinate_by_page,
        payload_by_hash=payload_by_hash,
    )


@dataclass(frozen=True)
class _MetadataCounts:
    npi_count: int
    component_count: int
    pattern_count: int
    relation_count: int
    heavy_owner_count: int


async def _load_dense_metadata_count(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    table_name: str,
    key_name: str,
    progress_callback: Callable[[str, int], None] | None,
) -> int:
    """Count one dense dictionary through bounded indexed key ranges."""

    table = _quote_ident(table_name)
    key = _quote_ident(key_name)
    maximum_key = await session.scalar(
        text(
            f"SELECT MAX({key}) FROM {schema}.{table} "
            "WHERE snapshot_key = :snapshot_key"
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    expected_count = int(maximum_key) + 1 if maximum_key is not None else 0
    for range_start in range(0, expected_count, PTG2_V4_METADATA_RANGE_ROWS):
        range_end = min(
            range_start + PTG2_V4_METADATA_RANGE_ROWS,
            expected_count,
        )
        observed_count = await session.scalar(
            text(
                f"SELECT COUNT(*)::bigint FROM {schema}.{table} "
                "WHERE snapshot_key = :snapshot_key "
                f"AND {key} >= :range_start AND {key} < :range_end"
            ),
            {
                "snapshot_key": int(snapshot_key),
                "range_start": range_start,
                "range_end": range_end,
            },
        )
        range_count = range_end - range_start
        if int(observed_count or 0) != range_count:
            raise RuntimeError("PTG V4 metadata dictionaries are not dense")
        if progress_callback is not None:
            progress_callback("seal_metadata_rows", range_count)
            progress_callback("seal_metadata_batches", 1)
    return expected_count


async def _load_metadata_aggregate(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    progress_callback: Callable[[str, int], None] | None = None,
) -> dict[str, Any]:
    count_by_prefix: dict[str, int] = {}
    for prefix, table_name, key_name in (
        ("npi", PTG2_V4_NPI_TABLE, "npi_key"),
        ("component", PTG2_V4_COMPONENT_TABLE, "component_key"),
        ("pattern", PTG2_V4_PATTERN_TABLE, "pattern_key"),
    ):
        count_by_prefix[prefix] = await _load_dense_metadata_count(
            session,
            schema=schema,
            snapshot_key=int(snapshot_key),
            table_name=table_name,
            key_name=key_name,
            progress_callback=progress_callback,
        )
    aggregate_by_field: dict[str, Any] = {
        "relation_count": 0,
        "heavy_owner_count": 0,
    }
    for prefix, row_count in count_by_prefix.items():
        aggregate_by_field[f"{prefix}_count"] = row_count
        aggregate_by_field[f"{prefix}_min"] = 0 if row_count else None
        aggregate_by_field[f"{prefix}_max"] = row_count - 1 if row_count else None
    return aggregate_by_field


def _has_dense_span(aggregate: Mapping[str, Any], prefix: str, count: int) -> bool:
    if count <= 0:
        return True
    minimum = aggregate.get(f"{prefix}_min")
    maximum = aggregate.get(f"{prefix}_max")
    return (
        minimum is not None
        and maximum is not None
        and int(minimum) == 0
        and int(maximum) == count - 1
    )


def _validated_metadata_counts(aggregate: Mapping[str, Any]) -> _MetadataCounts:
    counts = _MetadataCounts(
        npi_count=int(aggregate.get("npi_count") or 0),
        component_count=int(aggregate.get("component_count") or 0),
        pattern_count=int(aggregate.get("pattern_count") or 0),
        relation_count=int(aggregate.get("relation_count") or 0),
        heavy_owner_count=int(aggregate.get("heavy_owner_count") or 0),
    )
    if not all(
        (
            _has_dense_span(aggregate, "npi", counts.npi_count),
            _has_dense_span(aggregate, "component", counts.component_count),
            _has_dense_span(aggregate, "pattern", counts.pattern_count),
        )
    ):
        raise RuntimeError("PTG V4 metadata dictionaries are not dense")
    return counts


async def _load_relation_metadata(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    map_object_kinds: set[str],
) -> dict[str, dict[str, Any]]:
    relation_result = await session.execute(
        text(
            f"""
            SELECT relation, member_object_kind, locator_object_kind,
                   owner_base, owner_count, logical_member_count,
                   vector_member_count, member_width,
                   member_page_bytes, locator_page_bytes, locator_owner_span
              FROM {schema}.{PTG2_V4_RELATION_MANIFEST_TABLE}
             WHERE snapshot_key = :snapshot_key
             ORDER BY relation
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    relation_by_name: dict[str, dict[str, Any]] = {}
    for raw_relation in relation_result:
        relation = _row_mapping(raw_relation)
        relation_name = str(relation.get("relation") or "")
        owner_base = int(relation.get("owner_base") or 0)
        owner_count = int(relation.get("owner_count") or 0)
        logical_member_count = int(relation.get("logical_member_count") or 0)
        vector_member_count = int(relation.get("vector_member_count") or 0)
        if owner_base + owner_count > 2**63:
            raise RuntimeError("PTG V4 relation owner range exceeds bigint")
        if (
            owner_count > 0
            and relation.get("locator_object_kind") not in map_object_kinds
        ) or (
            vector_member_count > 0
            and relation.get("member_object_kind") not in map_object_kinds
        ):
            raise RuntimeError("PTG V4 relation manifest references a missing map kind")
        if vector_member_count > logical_member_count:
            raise RuntimeError("PTG V4 relation vector count exceeds its logical count")
        relation_by_name[relation_name] = relation
    return relation_by_name


async def _load_entry_counts_by_kind(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, int]:
    kind_entry_result = await session.execute(
        text(
            f"""
            SELECT object_kind, COALESCE(SUM(entry_count), 0)::bigint AS entry_count
              FROM {schema}.ptg2_v4_snapshot_map_pack
             WHERE snapshot_key = :snapshot_key
             GROUP BY object_kind
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    entry_count_by_kind: dict[str, int] = {}
    for raw_kind in kind_entry_result:
        kind_fields = _row_mapping(raw_kind)
        entry_count_by_kind[str(kind_fields.get("object_kind") or "")] = int(
            kind_fields.get("entry_count") or 0
        )
    return entry_count_by_kind


def _validate_relation_entry_counts(
    relation_by_name: Mapping[str, Mapping[str, Any]],
    entry_count_by_kind: Mapping[str, int],
) -> None:
    for relation in relation_by_name.values():
        if entry_count_by_kind.get(str(relation["locator_object_kind"]), 0) != int(
            relation["owner_count"]
        ):
            raise RuntimeError("PTG V4 relation locator count is inconsistent")
        if entry_count_by_kind.get(str(relation["member_object_kind"]), 0) != int(
            relation["vector_member_count"]
        ):
            raise RuntimeError("PTG V4 relation vector member count is inconsistent")


async def _load_heavy_owners(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[tuple[str, int], dict[str, Any]]:
    heavy_result = await session.execute(
        text(
            f"""
            SELECT owner.relation, owner.owner_key, owner.object_kind,
                   owner.member_count, owner.member_base, owner.member_span,
                   owner.fragment_count, mapping.map_block_hash, block.payload
              FROM {schema}.{PTG2_V4_HEAVY_OWNER_TABLE} AS owner
              LEFT JOIN {schema}.ptg2_v4_snapshot_map_pack AS mapping
                ON mapping.snapshot_key = owner.snapshot_key
               AND mapping.object_kind = owner.object_kind
               AND mapping.first_block_key <= owner.owner_key
               AND mapping.last_block_key >= owner.owner_key
              LEFT JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = mapping.map_block_hash
             WHERE owner.snapshot_key = :snapshot_key
             ORDER BY owner.relation, owner.owner_key, mapping.pack_no
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    observed_by_owner: dict[tuple[str, int], dict[str, Any]] = {}
    for raw_owner in heavy_result:
        owner = _row_mapping(raw_owner)
        owner_key = (str(owner.get("relation") or ""), int(owner["owner_key"]))
        aggregate_owner = observed_by_owner.setdefault(
            owner_key,
            {
                "object_kind": str(owner.get("object_kind") or ""),
                "member_count": int(owner.get("member_count") or 0),
                "member_base": int(owner.get("member_base") or 0),
                "member_span": int(owner.get("member_span") or 0),
                "fragment_count": int(owner.get("fragment_count") or 0),
                "fragments": set(),
                "entry_count": 0,
            },
        )
        if owner.get("map_block_hash") is None or owner.get("payload") is None:
            continue
        coordinates = decode_v4_snapshot_map_pack(
            bytes(owner["payload"]),
            expected_object_kind=aggregate_owner["object_kind"],
        )
        for coordinate in coordinates:
            if coordinate.block_key != owner_key[1]:
                continue
            fragments = aggregate_owner["fragments"]
            if coordinate.fragment_no in fragments:
                raise RuntimeError("PTG V4 heavy-owner bitmap fragment is duplicated")
            fragments.add(coordinate.fragment_no)
            aggregate_owner["entry_count"] += int(coordinate.entry_count)
    return observed_by_owner


def _validate_heavy_owners(
    observed_by_owner: Mapping[tuple[str, int], Mapping[str, Any]],
    relation_by_name: Mapping[str, Mapping[str, Any]],
    map_object_kinds: set[str],
) -> dict[str, int]:
    member_count_by_relation: dict[str, int] = {}
    for (relation_name, owner_key), owner in observed_by_owner.items():
        relation = relation_by_name.get(relation_name)
        if relation is None:
            raise RuntimeError("PTG V4 heavy owner has no relation manifest")
        owner_base = int(relation.get("owner_base") or 0)
        owner_count = int(relation.get("owner_count") or 0)
        if not owner_base <= owner_key < owner_base + owner_count:
            raise RuntimeError("PTG V4 heavy owner lies outside its relation range")
        if owner["object_kind"] not in map_object_kinds:
            raise RuntimeError("PTG V4 heavy owner references a missing map kind")
        expected_fragments = set(range(int(owner["fragment_count"])))
        if owner["fragments"] != expected_fragments:
            raise RuntimeError("PTG V4 heavy-owner fragment list is incomplete")
        if int(owner["entry_count"]) != int(owner["member_count"]):
            raise RuntimeError("PTG V4 heavy-owner member count is inconsistent")
        if int(owner["member_base"]) + int(owner["member_span"]) > 2**63:
            raise RuntimeError("PTG V4 heavy-owner member range exceeds bigint")
        member_count_by_relation[relation_name] = (
            member_count_by_relation.get(relation_name, 0)
            + int(owner["member_count"])
        )
    return member_count_by_relation


def _validate_replaced_member_counts(
    relation_by_name: Mapping[str, Mapping[str, Any]],
    member_count_by_relation: Mapping[str, int],
) -> None:
    for relation_name, relation in relation_by_name.items():
        replaced_member_count = int(relation["logical_member_count"]) - int(
            relation["vector_member_count"]
        )
        if member_count_by_relation.get(relation_name, 0) != replaced_member_count:
            raise RuntimeError(
                "PTG V4 relation logical/vector counts disagree with heavy owners"
            )


async def _load_graph_diagnostic_fields(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any]:
    selected_columns = ", ".join(PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS)
    diagnostic_result = await session.execute(
        text(
            f"""
            SELECT {selected_columns}
              FROM {schema}.{PTG2_V4_GRAPH_DIAGNOSTIC_TABLE}
             WHERE snapshot_key = :snapshot_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    diagnostic_rows = diagnostic_result.all()
    if len(diagnostic_rows) != 1:
        raise RuntimeError("PTG V4 graph diagnostics are missing or duplicated")
    diagnostic = _row_mapping(diagnostic_rows[0])
    if set(diagnostic) != set(PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS):
        raise RuntimeError("PTG V4 graph diagnostic columns changed")
    return diagnostic


async def _validate_prefix_aggregate(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    diagnostic: Mapping[str, Any],
) -> None:
    prefix_result = await session.execute(
        text(
            f"""
            SELECT COUNT(*) AS owner_count,
                   COALESCE(SUM(member_count), 0) AS member_count,
                   COALESCE(BOOL_AND(
                       member_count BETWEEN 0 AND :prefix_target
                       AND octet_length(member_digest) = 32
                   ), TRUE) AS valid
              FROM {schema}.{PTG2_V4_NPI_PREFIX_TABLE}
             WHERE snapshot_key = :snapshot_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "prefix_target": int(diagnostic["npi_prefix_target"]),
        },
    )
    prefix_aggregate = _row_mapping(prefix_result.one())
    if (
        int(prefix_aggregate.get("owner_count") or 0)
        != int(diagnostic["override_owner_count"])
        or int(prefix_aggregate.get("member_count") or 0)
        != int(diagnostic["override_member_count"])
        or not bool(prefix_aggregate.get("valid"))
    ):
        raise RuntimeError("PTG V4 graph prefix diagnostics changed")


async def _load_canary_prefixes(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    diagnostic: Mapping[str, Any],
) -> dict[int, tuple[int, bytes]]:
    owner_keys = {
        int(owner_key)
        for owner_key in (
            diagnostic.get("worst_provider_set_key"),
            diagnostic.get("worst_online_provider_set_key"),
        )
        if owner_key is not None
    }
    if not owner_keys:
        return {}
    owner_result = await session.execute(
        text(
            f"""
            SELECT provider_set_key, member_count, member_digest
              FROM {schema}.{PTG2_V4_NPI_PREFIX_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND (
                   provider_set_key = :worst_provider_set_key
                   OR provider_set_key = :worst_online_provider_set_key
               )
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "worst_provider_set_key": diagnostic.get(
                "worst_provider_set_key"
            ),
            "worst_online_provider_set_key": diagnostic.get(
                "worst_online_provider_set_key"
            ),
        },
    )
    return {
        int(owner_record.provider_set_key): (
            int(owner_record.member_count),
            bytes(owner_record.member_digest),
        )
        for owner_record in owner_result
    }


def _validate_canary_prefixes(
    diagnostic: Mapping[str, Any],
    prefix_by_owner: Mapping[int, tuple[int, bytes]],
) -> None:
    worst_key = diagnostic.get("worst_provider_set_key")
    worst_prefix = (
        prefix_by_owner.get(int(worst_key)) if worst_key is not None else None
    )
    expected_worst_prefix = (
        int(diagnostic["worst_member_count"]),
        bytes(diagnostic["worst_member_digest"]),
    ) if diagnostic.get("worst_member_digest") is not None else None
    if (
        bool(diagnostic["worst_uses_override"])
        != (worst_prefix is not None)
        or (
            bool(diagnostic["worst_uses_override"])
            and worst_prefix != expected_worst_prefix
        )
        or (
            diagnostic.get("worst_online_provider_set_key") is not None
            and int(diagnostic["worst_online_provider_set_key"])
            in prefix_by_owner
        )
    ):
        raise RuntimeError("PTG V4 graph canary-owner diagnostics changed")


def _normalized_graph_diagnostics(
    diagnostic: Mapping[str, Any],
) -> dict[str, Any]:
    diagnostics_by_field = dict(diagnostic)
    for digest_field in (
        "worst_member_digest",
        "worst_online_member_digest",
    ):
        digest = diagnostics_by_field.get(digest_field)
        diagnostics_by_field[digest_field] = (
            bytes(digest).hex() if digest is not None else None
        )
    return diagnostics_by_field


async def _load_provider_graph_diagnostics(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any]:
    """Load and cross-check sealed hot-prefix diagnostics."""

    diagnostic = await _load_graph_diagnostic_fields(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
    )
    await _validate_prefix_aggregate(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        diagnostic=diagnostic,
    )
    prefix_by_owner = await _load_canary_prefixes(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        diagnostic=diagnostic,
    )
    _validate_canary_prefixes(diagnostic, prefix_by_owner)
    return _normalized_graph_diagnostics(diagnostic)


async def _load_provider_graph_resources(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, int]:
    """Load the sealed inputs used by the size-relative import ceiling."""

    selected_columns = ", ".join(PTG2_V4_GRAPH_RESOURCE_FIELDS)
    resource_result = await session.execute(
        text(
            f"""
            SELECT {selected_columns}
              FROM {schema}.{PTG2_V4_GRAPH_DIAGNOSTIC_TABLE}
             WHERE snapshot_key = :snapshot_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    resource_rows = resource_result.all()
    if len(resource_rows) != 1:
        raise RuntimeError("PTG V4 graph resources are missing or duplicated")
    resource = _row_mapping(resource_rows[0])
    if set(resource) != set(PTG2_V4_GRAPH_RESOURCE_FIELDS):
        raise RuntimeError("PTG V4 graph resource columns changed")
    resources_by_field = {
        field_name: int(resource[field_name])
        for field_name in PTG2_V4_GRAPH_RESOURCE_FIELDS
    }
    if (
        resources_by_field["compressed_acquisition_bytes"] <= 0
        or resources_by_field["input_factor_bytes"] < 0
        or resources_by_field["factor_edge_count"] < 0
        or resources_by_field["empty_npi_tin_only_normalization_count"] < 0
    ):
        raise RuntimeError("PTG V4 graph resources are invalid")
    return resources_by_field


async def _validate_persisted_heavy_owners(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    map_object_kinds: set[str],
    relation_by_name: Mapping[str, Mapping[str, Any]],
) -> int:
    observed_by_owner = await _load_heavy_owners(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
    )
    member_count_by_relation = _validate_heavy_owners(
        observed_by_owner,
        relation_by_name,
        map_object_kinds,
    )
    await _validate_zero_heavy_locators(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        relations=relation_by_name,
        heavy_owners=observed_by_owner,
    )
    _validate_replaced_member_counts(
        relation_by_name,
        member_count_by_relation,
    )
    return len(observed_by_owner)


async def summarize_persisted_v4_snapshot_metadata(
    session: Any, *,
    schema_name: str,
    snapshot_key: int,
    map_summary: V4SnapshotMapSummary,
    progress_callback: Callable[[str, int], None] | None = None,
) -> V4SnapshotMetadataSummary:
    """Validate dense dictionaries and the exact manifest-listed owner contracts."""

    schema = _quote_ident(schema_name)
    aggregate = await _load_metadata_aggregate(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        **(
            {"progress_callback": progress_callback}
            if progress_callback is not None
            else {}
        ),
    )
    counts = _validated_metadata_counts(aggregate)
    map_object_kinds = set(map_summary.object_kinds)
    relation_by_name = await _load_relation_metadata(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        map_object_kinds=map_object_kinds,
    )
    entry_count_by_kind = await _load_entry_counts_by_kind(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
    )
    _validate_relation_entry_counts(relation_by_name, entry_count_by_kind)
    heavy_owner_count = await _validate_persisted_heavy_owners(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        map_object_kinds=map_object_kinds,
        relation_by_name=relation_by_name,
    )
    provider_graph_diagnostics = await _load_provider_graph_diagnostics(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
    )
    provider_graph_resources = await _load_provider_graph_resources(
        session,
        schema=schema,
        snapshot_key=int(snapshot_key),
    )
    return V4SnapshotMetadataSummary(
        npi_count=counts.npi_count,
        component_count=counts.component_count,
        pattern_count=counts.pattern_count,
        relation_count=len(relation_by_name),
        heavy_owner_count=heavy_owner_count,
        provider_graph_diagnostics=provider_graph_diagnostics,
        provider_graph_resources=provider_graph_resources,
    )


async def _load_v4_layout_reservation(
    session: Any,
    *,
    schema: str,
    fingerprint: bytes,
) -> dict[str, Any] | None:
    existing_result = await session.execute(
        text(
            f"""
            SELECT layout.snapshot_key, layout.state, layout.generation,
                   layout.build_token, layout.layout_manifest,
                   layout.mapping_digest AS layout_mapping_digest,
                   root.state AS root_state,
                   root.format_version AS root_format_version,
                   root.map_format, root.representation,
                   root.projection_id_scope, root.map_digest,
                   root.object_kind_count, root.map_pack_count,
                   root.coordinate_count, root.entry_count,
                   root.logical_byte_count, root.stored_map_byte_count,
                   root.npi_count, root.component_count, root.pattern_count,
                   root.relation_count, root.heavy_owner_count
              FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = fingerprint.snapshot_key
              LEFT JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = layout.snapshot_key
             WHERE fingerprint.semantic_fingerprint = :semantic_fingerprint
             LIMIT 1
             FOR UPDATE OF layout
            """
        ),
        {"semantic_fingerprint": fingerprint},
    )
    existing_row = existing_result.first()
    return _row_mapping(existing_row) if existing_row is not None else None


def _sealed_root_summaries(
    existing: Mapping[str, Any],
) -> tuple[Mapping[str, Any], V4SnapshotMapSummary, V4SnapshotMetadataSummary]:
    manifest = existing.get("layout_manifest") or {}
    serving_index = (
        manifest.get("serving_index") if isinstance(manifest, Mapping) else None
    )
    snapshot_map = (
        serving_index.get("snapshot_map")
        if isinstance(serving_index, Mapping)
        else None
    )
    serving_binary = (
        serving_index.get("serving_binary")
        if isinstance(serving_index, Mapping)
        else None
    )
    provider_graph = (
        serving_binary.get("provider_graph_v4")
        if isinstance(serving_binary, Mapping)
        else None
    )
    object_kinds = (
        tuple(
            str(object_kind)
            for object_kind in snapshot_map.get("object_kinds") or ()
        )
        if isinstance(snapshot_map, Mapping)
        else ()
    )
    sealed_summary = V4SnapshotMapSummary(
        map_digest=bytes(existing.get("map_digest") or b""),
        object_kinds=object_kinds,
        map_pack_count=int(existing.get("map_pack_count") or 0),
        coordinate_count=int(existing.get("coordinate_count") or 0),
        entry_count=int(existing.get("entry_count") or 0),
        logical_byte_count=int(existing.get("logical_byte_count") or 0),
        stored_map_byte_count=int(existing.get("stored_map_byte_count") or 0),
    )
    sealed_metadata = V4SnapshotMetadataSummary(
        npi_count=int(existing.get("npi_count") or 0),
        component_count=int(existing.get("component_count") or 0),
        pattern_count=int(existing.get("pattern_count") or 0),
        relation_count=int(existing.get("relation_count") or 0),
        heavy_owner_count=int(existing.get("heavy_owner_count") or 0),
        provider_graph_diagnostics=dict(
            provider_graph.get("hot_prefix") or {}
        )
        if isinstance(provider_graph, Mapping)
        else {},
        provider_graph_resources=dict(
            provider_graph.get("resource_admission") or {}
        )
        if isinstance(provider_graph, Mapping)
        else {},
    )
    return manifest, sealed_summary, sealed_metadata


def _validate_sealed_reservation(existing: Mapping[str, Any]) -> Mapping[str, Any]:
    manifest, sealed_summary, sealed_metadata = _sealed_root_summaries(existing)
    if (
        existing.get("root_state") != "complete"
        or int(existing.get("root_format_version") or -1)
        != PTG2_V4_MAP_FORMAT_VERSION
        or existing.get("map_format") != PTG2_V4_MAP_FORMAT
        or existing.get("projection_id_scope") != PTG2_V4_PROJECTION_ID_SCOPE
        or bytes(existing.get("layout_mapping_digest") or b"")
        != sealed_summary.map_digest
        or int(existing.get("object_kind_count") or 0)
        != sealed_summary.object_kind_count
    ):
        raise RuntimeError("sealed PTG V4 reuse root is inconsistent")
    _validate_v4_manifest_root(
        manifest,
        representation=str(existing.get("representation") or ""),
        summary=sealed_summary,
        metadata=sealed_metadata,
    )
    return manifest


async def _touch_sealed_reservation(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> None:
    await session.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET heartbeat_at = :heartbeat_at,
                   lease_until = :lease_until
             WHERE snapshot_key = :snapshot_key
               AND state = 'sealed'
               AND generation = :generation
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(sealed=True),
        },
    )


async def _existing_v4_reservation(
    session: Any,
    *,
    schema: str,
    existing: Mapping[str, Any],
    build_token: str,
) -> SharedLayoutReservation:
    if (
        existing.get("state") == "sealed"
        and existing.get("generation") == PTG2_V4_SHARED_GENERATION
    ):
        manifest = _validate_sealed_reservation(existing)
        snapshot_key = int(existing["snapshot_key"])
        await _touch_sealed_reservation(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
        )
        return SharedLayoutReservation(snapshot_key, True, dict(manifest))
    if (
        existing.get("state") == "building"
        and existing.get("generation") == PTG2_V4_SHARED_GENERATION
        and existing.get("build_token") == str(build_token)
    ):
        return SharedLayoutReservation(int(existing["snapshot_key"]), False, None)
    raise RuntimeError("matching PTG V4 layout is already owned by another build")


async def _create_v4_reservation(
    session: Any,
    *,
    schema: str,
    fingerprint: bytes,
    build_token: str,
    storage_shard_id: int,
) -> SharedLayoutReservation:
    reservation_result = await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (storage_shard_id, build_token, generation, state,
                 logical_byte_count, created_at, heartbeat_at, lease_until)
            VALUES
                (:storage_shard_id, :build_token, :generation, 'building',
                 0, :created_at, :heartbeat_at, :lease_until)
            RETURNING snapshot_key
            """
        ),
        {
            "storage_shard_id": int(storage_shard_id),
            "build_token": str(build_token),
            "generation": PTG2_V4_SHARED_GENERATION,
            "created_at": _utcnow(),
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(),
        },
    )
    snapshot_key = reservation_result.scalar()
    if snapshot_key is None:
        raise RuntimeError("PTG V4 layout reservation did not return a key")
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_layout_fingerprint
                (semantic_fingerprint, snapshot_key, created_at)
            VALUES
                (:semantic_fingerprint, :snapshot_key, :created_at)
            """
        ),
        {
            "semantic_fingerprint": fingerprint,
            "snapshot_key": int(snapshot_key),
            "created_at": _utcnow(),
        },
    )
    return SharedLayoutReservation(int(snapshot_key), False, None)


async def reserve_v4_shared_layout(
    session: Any,
    *,
    schema_name: str,
    semantic_fingerprint: bytes,
    build_token: str,
    storage_shard_id: int = 0,
) -> SharedLayoutReservation:
    """Reserve a V4 physical layout while leaving logical identity unchanged."""

    schema = _quote_ident(schema_name)
    fingerprint = v4_layout_fingerprint(semantic_fingerprint)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": v4_layout_advisory_lock_key(fingerprint)},
    )
    existing = await _load_v4_layout_reservation(
        session,
        schema=schema,
        fingerprint=fingerprint,
    )
    if existing is not None:
        return await _existing_v4_reservation(
            session,
            schema=schema,
            existing=existing,
            build_token=build_token,
        )
    return await _create_v4_reservation(
        session,
        schema=schema,
        fingerprint=fingerprint,
        build_token=build_token,
        storage_shard_id=int(storage_shard_id),
    )


async def lock_v4_layout_map_write(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    """Fence V4 map writes to the caller's building generation."""

    schema = _quote_ident(schema_name)
    ownership_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
               AND generation = :generation
               AND state = 'building'
               AND build_token = :build_token
             FOR UPDATE
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    if ownership_result.scalar() is None:
        raise RuntimeError("PTG V4 packed-map write lost build ownership")


lock_v4_shared_layout_for_map_write = lock_v4_layout_map_write


async def touch_v4_shared_layout_build(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    """Extend the V4 build lease at bounded publication checkpoints."""

    schema = _quote_ident(schema_name)
    touch_result = await session.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET heartbeat_at = :heartbeat_at,
                   lease_until = :lease_until
             WHERE snapshot_key = :snapshot_key
               AND generation = :generation
               AND state = 'building'
               AND build_token = :build_token
            RETURNING snapshot_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "build_token": str(build_token),
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(),
        },
    )
    if touch_result.scalar() is None:
        raise RuntimeError("PTG V4 build heartbeat lost ownership")


async def _verify_dense_table_keys(
    session: Any,
    *,
    schema_name: str,
    table_name: str,
    key_column: str,
    snapshot_key: int,
    expected_count: int,
    context: str,
) -> int:
    schema = _quote_ident(schema_name)
    table = _quote_ident(table_name)
    key = _quote_ident(key_column)
    aggregate_result = await session.execute(
        text(
            f"""
            SELECT COUNT(*)::bigint, MIN({key}), MAX({key})
              FROM {schema}.{table}
             WHERE snapshot_key = :snapshot_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    aggregate_row = aggregate_result.one()
    observed_count = int(aggregate_row[0])
    expected_first = 0 if expected_count else None
    expected_last = expected_count - 1 if expected_count else None
    if (
        observed_count != expected_count
        or aggregate_row[1] != expected_first
        or aggregate_row[2] != expected_last
    ):
        raise RuntimeError(f"PTG V4 {context} is not a complete dense keyspace")
    return observed_count


def _npi_pairs(npi_rows: Iterable[Mapping[str, Any]]) -> list[tuple[int, int]]:
    return [
        (int(npi_row["npi_key"]), int(npi_row["npi"]))
        for npi_row in npi_rows
    ]


def _normalized_npi_row(
    raw_entry: tuple[int, int] | Mapping[str, int],
    expected_key: int,
) -> dict[str, int]:
    if isinstance(raw_entry, Mapping):
        npi_key = int(raw_entry["npi_key"])
        npi = int(raw_entry["npi"])
    else:
        npi_key = int(raw_entry[0])
        npi = int(raw_entry[1])
    if npi_key != expected_key:
        raise ValueError("PTG V4 NPI keys must be contiguous from zero")
    if not 1_000_000_000 <= npi <= 9_999_999_999:
        raise ValueError("PTG V4 NPI dictionary contains an invalid NPI")
    return {"npi_key": npi_key, "npi": npi}


async def _publish_v4_npi_batch(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    npi_rows: Sequence[Mapping[str, int]],
) -> None:
    """Insert and verify one bounded NPI dictionary batch."""

    if not npi_rows:
        return
    schema = _quote_ident(schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{PTG2_V4_NPI_TABLE}
                (snapshot_key, npi_key, npi)
            VALUES
                (:snapshot_key, :npi_key, :npi)
            ON CONFLICT DO NOTHING
            """
        ),
        [
            {
                "snapshot_key": int(snapshot_key),
                "npi_key": int(npi_row["npi_key"]),
                "npi": int(npi_row["npi"]),
            }
            for npi_row in npi_rows
        ],
    )
    first_key = int(npi_rows[0]["npi_key"])
    last_key = int(npi_rows[-1]["npi_key"])
    stored_result = await session.execute(
        text(
            f"""
            SELECT npi_key, npi
              FROM {schema}.{PTG2_V4_NPI_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND npi_key BETWEEN :first_key AND :last_key
             ORDER BY npi_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "first_key": first_key,
            "last_key": last_key,
        },
    )
    observed_pairs = _npi_pairs(_row_mapping(stored_row) for stored_row in stored_result)
    expected_pairs = _npi_pairs(npi_rows)
    if observed_pairs != expected_pairs:
        raise RuntimeError("PTG V4 NPI dictionary conflicts with stored dense keys")


async def publish_v4_npi_dictionary(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    entries: Iterable[tuple[int, int] | Mapping[str, int]],
    batch_rows: int = 10_000,
) -> V4NPIDictionaryPublication:
    """Publish a contiguous snapshot-local NPI keyspace with bounded memory."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 NPI dictionary batch size must be positive")
    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    pending_rows: list[dict[str, int]] = []
    expected_key = 0
    for raw_entry in entries:
        pending_rows.append(_normalized_npi_row(raw_entry, expected_key))
        expected_key += 1
        if len(pending_rows) >= normalized_batch_rows:
            await _publish_v4_npi_batch(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                npi_rows=pending_rows,
            )
            pending_rows = []
    await _publish_v4_npi_batch(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        npi_rows=pending_rows,
    )

    observed_count = await _verify_dense_table_keys(
        session,
        schema_name=schema_name,
        table_name=PTG2_V4_NPI_TABLE,
        key_column="npi_key",
        snapshot_key=int(snapshot_key),
        expected_count=expected_key,
        context="NPI dictionary",
    )
    return V4NPIDictionaryPublication(row_count=observed_count)


def _validated_v4_name(value: Any, *, field_name: str, max_bytes: int) -> str:
    normalized = str(value or "")
    encoded = normalized.encode("utf-8")
    if not encoded or len(encoded) > max_bytes:
        raise ValueError(
            f"PTG V4 {field_name} must contain 1 to {max_bytes} UTF-8 bytes"
        )
    return normalized


def _component_pairs(
    component_rows: Iterable[Mapping[str, Any]],
) -> list[tuple[int, bytes]]:
    return [
        (
            int(component_row["component_key"]),
            bytes(component_row["component_global_id_128"]),
        )
        for component_row in component_rows
    ]


def _normalized_component_row(
    raw_entry: tuple[int, bytes] | Mapping[str, Any],
    expected_key: int,
) -> dict[str, Any]:
    if isinstance(raw_entry, Mapping):
        component_key = int(raw_entry["component_key"])
        component_id = bytes(raw_entry["component_global_id_128"])
    else:
        component_key = int(raw_entry[0])
        component_id = bytes(raw_entry[1])
    if component_key != expected_key:
        raise ValueError("PTG V4 component keys must be contiguous from zero")
    if len(component_id) != 16:
        raise ValueError("PTG V4 component ID must contain 16 bytes")
    return {
        "component_key": component_key,
        "component_global_id_128": component_id,
    }


async def _publish_v4_component_batch(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    component_rows: Sequence[Mapping[str, Any]],
) -> None:
    """Insert and verify one bounded provider-component batch."""

    if not component_rows:
        return
    schema = _quote_ident(schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{PTG2_V4_COMPONENT_TABLE}
                (snapshot_key, component_key, component_global_id_128)
            VALUES
                (:snapshot_key, :component_key, :component_global_id_128)
            ON CONFLICT (snapshot_key, component_key) DO NOTHING
            """
        ),
        [
            {
                "snapshot_key": int(snapshot_key),
                "component_key": int(component_row["component_key"]),
                "component_global_id_128": bytes(
                    component_row["component_global_id_128"]
                ),
            }
            for component_row in component_rows
        ],
    )
    stored_result = await session.execute(
        text(
            f"""
            SELECT component_key, component_global_id_128
              FROM {schema}.{PTG2_V4_COMPONENT_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND component_key BETWEEN :first_key AND :last_key
             ORDER BY component_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "first_key": int(component_rows[0]["component_key"]),
            "last_key": int(component_rows[-1]["component_key"]),
        },
    )
    observed_pairs = _component_pairs(
        _row_mapping(stored_row) for stored_row in stored_result
    )
    expected_pairs = _component_pairs(component_rows)
    if observed_pairs != expected_pairs:
        raise RuntimeError("PTG V4 component dictionary conflicts with stored keys")


async def publish_v4_provider_components(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    entries: Iterable[tuple[int, bytes] | Mapping[str, Any]],
    batch_rows: int = 10_000,
) -> V4MetadataTablePublication:
    """Publish source-stable provider components with dense snapshot keys."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 component batch size must be positive")
    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    expected_key = 0
    pending_rows: list[dict[str, Any]] = []
    for raw_entry in entries:
        pending_rows.append(_normalized_component_row(raw_entry, expected_key))
        expected_key += 1
        if len(pending_rows) >= normalized_batch_rows:
            await _publish_v4_component_batch(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                component_rows=pending_rows,
            )
            pending_rows = []
    await _publish_v4_component_batch(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        component_rows=pending_rows,
    )
    await _verify_dense_table_keys(
        session,
        schema_name=schema_name,
        table_name=PTG2_V4_COMPONENT_TABLE,
        key_column="component_key",
        snapshot_key=int(snapshot_key),
        expected_count=expected_key,
        context="component dictionary",
    )
    return V4MetadataTablePublication(row_count=expected_key)


def _pattern_tuples(
    pattern_rows: Iterable[Mapping[str, Any]],
) -> list[tuple[int, bytes, int]]:
    return [
        (
            int(pattern_row["pattern_key"]),
            bytes(pattern_row["pattern_digest"]),
            int(pattern_row["set_count"]),
        )
        for pattern_row in pattern_rows
    ]


def _normalized_pattern_row(
    raw_entry: tuple[int, bytes, int] | Mapping[str, Any],
    expected_key: int,
) -> dict[str, Any]:
    if isinstance(raw_entry, Mapping):
        pattern_key = int(raw_entry["pattern_key"])
        pattern_digest = bytes(raw_entry["pattern_digest"])
        set_count = int(raw_entry["set_count"])
    else:
        pattern_key = int(raw_entry[0])
        pattern_digest = bytes(raw_entry[1])
        set_count = int(raw_entry[2])
    if pattern_key != expected_key:
        raise ValueError("PTG V4 pattern keys must be contiguous from zero")
    if len(pattern_digest) != 32:
        raise ValueError("PTG V4 pattern digest must contain 32 bytes")
    if set_count < 0:
        raise ValueError("PTG V4 pattern set count must be non-negative")
    return {
        "pattern_key": pattern_key,
        "pattern_digest": pattern_digest,
        "set_count": set_count,
    }


async def _publish_v4_pattern_batch(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    pattern_rows: Sequence[Mapping[str, Any]],
) -> None:
    """Insert and verify one bounded pattern-metadata batch."""

    if not pattern_rows:
        return
    schema = _quote_ident(schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.{PTG2_V4_PATTERN_TABLE}
                (snapshot_key, pattern_key, pattern_digest, set_count)
            VALUES
                (:snapshot_key, :pattern_key, :pattern_digest, :set_count)
            ON CONFLICT (snapshot_key, pattern_key) DO NOTHING
            """
        ),
        [
            {
                "snapshot_key": int(snapshot_key),
                "pattern_key": int(pattern_row["pattern_key"]),
                "pattern_digest": bytes(pattern_row["pattern_digest"]),
                "set_count": int(pattern_row["set_count"]),
            }
            for pattern_row in pattern_rows
        ],
    )
    stored_result = await session.execute(
        text(
            f"""
            SELECT pattern_key, pattern_digest, set_count
              FROM {schema}.{PTG2_V4_PATTERN_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND pattern_key BETWEEN :first_key AND :last_key
             ORDER BY pattern_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "first_key": int(pattern_rows[0]["pattern_key"]),
            "last_key": int(pattern_rows[-1]["pattern_key"]),
        },
    )
    observed_tuples = _pattern_tuples(
        _row_mapping(stored_row) for stored_row in stored_result
    )
    expected_tuples = _pattern_tuples(pattern_rows)
    if observed_tuples != expected_tuples:
        raise RuntimeError("PTG V4 pattern metadata conflicts with stored keys")


async def publish_v4_patterns(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    entries: Iterable[tuple[int, bytes, int] | Mapping[str, Any]],
    batch_rows: int = 10_000,
) -> V4MetadataTablePublication:
    """Publish snapshot-local pattern audit metadata with dense keys."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 pattern batch size must be positive")
    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    expected_key = 0
    pending_rows: list[dict[str, Any]] = []
    for raw_entry in entries:
        pending_rows.append(_normalized_pattern_row(raw_entry, expected_key))
        expected_key += 1
        if len(pending_rows) >= normalized_batch_rows:
            await _publish_v4_pattern_batch(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                pattern_rows=pending_rows,
            )
            pending_rows = []
    await _publish_v4_pattern_batch(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        pattern_rows=pending_rows,
    )
    await _verify_dense_table_keys(
        session,
        schema_name=schema_name,
        table_name=PTG2_V4_PATTERN_TABLE,
        key_column="pattern_key",
        snapshot_key=int(snapshot_key),
        expected_count=expected_key,
        context="pattern metadata",
    )
    return V4MetadataTablePublication(row_count=expected_key)


def _relation_manifest_row(raw_entry: Mapping[str, Any]) -> dict[str, Any]:
    relation_row_by_field = {
        "relation": _validated_v4_name(
            raw_entry["relation"], field_name="relation", max_bytes=32
        ),
        "member_object_kind": _validated_v4_name(
            raw_entry["member_object_kind"],
            field_name="member object kind",
            max_bytes=64,
        ),
        "locator_object_kind": _validated_v4_name(
            raw_entry["locator_object_kind"],
            field_name="locator object kind",
            max_bytes=64,
        ),
        "owner_base": int(raw_entry["owner_base"]),
        "owner_count": int(raw_entry["owner_count"]),
        "logical_member_count": int(raw_entry["logical_member_count"]),
        "vector_member_count": int(raw_entry["vector_member_count"]),
        "member_width": int(raw_entry["member_width"]),
        "member_page_bytes": int(raw_entry["member_page_bytes"]),
        "locator_page_bytes": int(raw_entry["locator_page_bytes"]),
        "locator_owner_span": int(raw_entry["locator_owner_span"]),
    }
    if (
        relation_row_by_field["member_object_kind"]
        == relation_row_by_field["locator_object_kind"]
        or min(
            relation_row_by_field["owner_base"],
            relation_row_by_field["owner_count"],
            relation_row_by_field["logical_member_count"],
            relation_row_by_field["vector_member_count"],
        )
        < 0
        or relation_row_by_field["member_width"] not in {1, 2, 4, 8}
        or min(
            relation_row_by_field["member_page_bytes"],
            relation_row_by_field["locator_page_bytes"],
            relation_row_by_field["locator_owner_span"],
        )
        <= 0
        or relation_row_by_field["vector_member_count"]
        > relation_row_by_field["logical_member_count"]
    ):
        raise ValueError("PTG V4 relation manifest has invalid page metadata")
    return relation_row_by_field


def _normalized_relation_rows(
    entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    relation_rows: list[dict[str, Any]] = []
    previous_relation: str | None = None
    for raw_entry in entries:
        relation_row = _relation_manifest_row(raw_entry)
        relation_name = str(relation_row["relation"])
        if previous_relation is not None and relation_name <= previous_relation:
            raise ValueError("PTG V4 relation manifests must be strictly ordered")
        previous_relation = relation_name
        relation_rows.append(relation_row)
    return relation_rows


async def _insert_relation_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    relation_rows: Sequence[Mapping[str, Any]],
    batch_size: int,
) -> None:
    schema = _quote_ident(schema_name)
    for offset in range(0, len(relation_rows), batch_size):
        pending_rows = relation_rows[offset : offset + batch_size]
        await session.execute(
            text(
                f"""
                INSERT INTO {schema}.{PTG2_V4_RELATION_MANIFEST_TABLE}
                    (snapshot_key, relation,
                     member_object_kind, locator_object_kind,
                     owner_base, owner_count, logical_member_count,
                     vector_member_count, member_width,
                     member_page_bytes, locator_page_bytes, locator_owner_span)
                VALUES
                    (:snapshot_key, :relation,
                     :member_object_kind, :locator_object_kind,
                     :owner_base, :owner_count, :logical_member_count,
                     :vector_member_count, :member_width,
                     :member_page_bytes, :locator_page_bytes,
                     :locator_owner_span)
                ON CONFLICT (snapshot_key, relation) DO NOTHING
                """
            ),
            [
                {"snapshot_key": int(snapshot_key), **relation_row}
                for relation_row in pending_rows
            ],
        )


async def _load_relation_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> list[dict[str, Any]]:
    schema = _quote_ident(schema_name)
    stored_result = await session.execute(
        text(
            f"""
            SELECT relation, member_object_kind, locator_object_kind,
                   owner_base, owner_count, logical_member_count,
                   vector_member_count, member_width,
                   member_page_bytes, locator_page_bytes, locator_owner_span
              FROM {schema}.{PTG2_V4_RELATION_MANIFEST_TABLE}
             WHERE snapshot_key = :snapshot_key
             ORDER BY relation
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    field_names = (
        "relation",
        "member_object_kind",
        "locator_object_kind",
        "owner_base",
        "owner_count",
        "logical_member_count",
        "vector_member_count",
        "member_width",
        "member_page_bytes",
        "locator_page_bytes",
        "locator_owner_span",
    )
    observed_rows = [
        {
            field_name: _row_mapping(stored_row)[field_name]
            for field_name in field_names
        }
        for stored_row in stored_result
    ]
    return [
        {
            key: (
                str(field_value)
                if key
                in {"relation", "member_object_kind", "locator_object_kind"}
                else int(field_value)
            )
            for key, field_value in observed_row.items()
        }
        for observed_row in observed_rows
    ]


async def publish_v4_relation_manifests(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    entries: Iterable[Mapping[str, Any]],
    batch_rows: int = 1_000,
) -> V4MetadataTablePublication:
    """Publish the tiny authoritative relation/page-math manifest."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 relation-manifest batch size must be positive")
    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    expected_rows = _normalized_relation_rows(entries)
    await _insert_relation_rows(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        relation_rows=expected_rows,
        batch_size=normalized_batch_rows,
    )
    observed_rows = await _load_relation_rows(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
    )
    if observed_rows != expected_rows:
        raise RuntimeError("PTG V4 relation manifest conflicts with stored rows")
    return V4MetadataTablePublication(row_count=len(expected_rows))


def _heavy_owner_row(raw_entry: Mapping[str, Any]) -> dict[str, Any]:
    owner_row_by_field = {
        "relation": _validated_v4_name(
            raw_entry["relation"], field_name="relation", max_bytes=32
        ),
        "owner_key": int(raw_entry["owner_key"]),
        "object_kind": _validated_v4_name(
            raw_entry["object_kind"],
            field_name="heavy-owner object kind",
            max_bytes=64,
        ),
        "member_count": int(raw_entry["member_count"]),
        "member_base": int(raw_entry["member_base"]),
        "member_span": int(raw_entry["member_span"]),
        "fragment_count": int(raw_entry["fragment_count"]),
    }
    if (
        min(
            owner_row_by_field["owner_key"],
            owner_row_by_field["member_count"],
            owner_row_by_field["member_base"],
        )
        < 0
        or owner_row_by_field["member_span"] <= 0
        or owner_row_by_field["fragment_count"] <= 0
    ):
        raise ValueError("PTG V4 heavy owner has invalid bitmap metadata")
    return owner_row_by_field


def _normalized_heavy_owner_rows(
    entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    owner_rows: list[dict[str, Any]] = []
    previous_key: tuple[str, int] | None = None
    for raw_entry in entries:
        owner_row = _heavy_owner_row(raw_entry)
        row_key = (str(owner_row["relation"]), int(owner_row["owner_key"]))
        if previous_key is not None and row_key <= previous_key:
            raise ValueError("PTG V4 heavy owners must be strictly ordered")
        previous_key = row_key
        owner_rows.append(owner_row)
    return owner_rows


async def _insert_heavy_owner_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    owner_rows: Sequence[Mapping[str, Any]],
    batch_size: int,
) -> None:
    schema = _quote_ident(schema_name)
    for offset in range(0, len(owner_rows), batch_size):
        pending_rows = owner_rows[offset : offset + batch_size]
        await session.execute(
            text(
                f"""
                INSERT INTO {schema}.{PTG2_V4_HEAVY_OWNER_TABLE}
                    (snapshot_key, relation, owner_key, object_kind,
                     member_count, member_base, member_span, fragment_count)
                VALUES
                    (:snapshot_key, :relation, :owner_key, :object_kind,
                     :member_count, :member_base, :member_span, :fragment_count)
                ON CONFLICT (snapshot_key, relation, owner_key) DO NOTHING
                """
            ),
            [
                {"snapshot_key": int(snapshot_key), **owner_row}
                for owner_row in pending_rows
            ],
        )


async def _load_heavy_owner_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> list[dict[str, Any]]:
    schema = _quote_ident(schema_name)
    stored_result = await session.execute(
        text(
            f"""
            SELECT relation, owner_key, object_kind, member_count,
                   member_base, member_span, fragment_count
              FROM {schema}.{PTG2_V4_HEAVY_OWNER_TABLE}
             WHERE snapshot_key = :snapshot_key
             ORDER BY relation, owner_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    owner_rows: list[dict[str, Any]] = []
    for stored_row in stored_result:
        owner_row = _row_mapping(stored_row)
        owner_rows.append(
            {
                "relation": str(owner_row["relation"]),
                "owner_key": int(owner_row["owner_key"]),
                "object_kind": str(owner_row["object_kind"]),
                "member_count": int(owner_row["member_count"]),
                "member_base": int(owner_row["member_base"]),
                "member_span": int(owner_row["member_span"]),
                "fragment_count": int(owner_row["fragment_count"]),
            }
        )
    return owner_rows


async def publish_v4_heavy_owners(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    entries: Iterable[Mapping[str, Any]],
    batch_rows: int = 1_000,
) -> V4MetadataTablePublication:
    """Publish only manifest-qualified bitmap owners for direct lookup."""

    normalized_batch_rows = int(batch_rows)
    if normalized_batch_rows <= 0:
        raise ValueError("PTG V4 heavy-owner batch size must be positive")
    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    expected_rows = _normalized_heavy_owner_rows(entries)
    await _insert_heavy_owner_rows(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        owner_rows=expected_rows,
        batch_size=normalized_batch_rows,
    )
    observed_rows = await _load_heavy_owner_rows(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
    )
    if observed_rows != expected_rows:
        raise RuntimeError("PTG V4 heavy-owner manifest conflicts with stored rows")
    return V4MetadataTablePublication(row_count=len(expected_rows))


async def _initialize_v4_snapshot_map_root(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    representation: str,
) -> None:
    """Create or validate the building root for one V4 representation."""

    schema = _quote_ident(schema_name)
    normalized_representation = str(representation or "").strip()
    if normalized_representation not in PTG2_V4_REPRESENTATIONS:
        raise ValueError("PTG V4 snapshot map representation is unsupported")
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v4_snapshot_map_root
                (snapshot_key, state, format_version, map_format,
                 representation, projection_id_scope)
            VALUES
                (:snapshot_key, 'building', :format_version, :map_format,
                 :representation, :projection_id_scope)
            ON CONFLICT (snapshot_key) DO NOTHING
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "format_version": PTG2_V4_MAP_FORMAT_VERSION,
            "map_format": PTG2_V4_MAP_FORMAT,
            "representation": normalized_representation,
            "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
        },
    )
    root_result = await session.execute(
        text(
            f"""
            SELECT state, format_version, map_format, representation,
                   projection_id_scope
              FROM {schema}.ptg2_v4_snapshot_map_root
             WHERE snapshot_key = :snapshot_key
             FOR UPDATE
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    root_row = root_result.first()
    root = _row_mapping(root_row) if root_row is not None else {}
    expected_by_field = {
        "state": "building",
        "format_version": PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": PTG2_V4_MAP_FORMAT,
        "representation": normalized_representation,
        "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
    }
    if any(
        root.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        raise RuntimeError("PTG V4 snapshot map root conflicts with the requested layout")


def _target_metadata_by_hash(
    pack: V4SnapshotMapPack,
) -> dict[bytes, tuple[int, str, str, int, int, int]]:
    metadata_by_hash: dict[bytes, tuple[int, str, str, int, int, int]] = {}
    for reference in pack.references:
        target_hash = bytes(reference.block_hash)
        metadata_fields = (
            PTG2_V3_SHARED_FORMAT_VERSION,
            reference.object_kind,
            "none",
            int(reference.entry_count),
            int(reference.raw_byte_count),
            int(reference.raw_byte_count),
        )
        previous_metadata = metadata_by_hash.setdefault(target_hash, metadata_fields)
        if previous_metadata != metadata_fields:
            raise ValueError("PTG V4 target hash has inconsistent logical metadata")
    return metadata_by_hash


async def _verify_target_blocks(
    session: Any,
    *,
    schema: str,
    metadata_by_hash: Mapping[bytes, tuple[int, str, str, int, int, int]],
) -> None:
    target_result = await session.execute(
        text(
            f"""
            SELECT block_hash, format_version, object_kind, codec,
                   entry_count, raw_byte_count, stored_byte_count
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
             FOR KEY SHARE
            """
        ),
        {"block_hashes": list(metadata_by_hash)},
    )
    observed_by_hash: dict[bytes, tuple[int, str, str, int, int, int]] = {}
    for target_row in target_result:
        target_fields = _row_mapping(target_row)
        observed_by_hash[bytes(target_fields["block_hash"])] = (
            int(target_fields["format_version"]),
            str(target_fields["object_kind"]),
            str(target_fields["codec"]),
            int(target_fields["entry_count"]),
            int(target_fields["raw_byte_count"]),
            int(target_fields["stored_byte_count"]),
        )
    if observed_by_hash != metadata_by_hash:
        raise RuntimeError("PTG V4 map pack could not resolve every target CAS block")


async def _publish_map_block(
    session: Any,
    *,
    schema: str,
    map_block: SharedBlock,
) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_block
                (block_hash, format_version, object_kind, codec, entry_count,
                 raw_byte_count, stored_byte_count, payload, created_at)
            VALUES
                (:block_hash, :format_version, :object_kind, :codec, :entry_count,
                 :raw_byte_count, :stored_byte_count, :payload, :created_at)
            ON CONFLICT (block_hash) DO NOTHING
            """
        ),
        {
            "block_hash": map_block.block_hash,
            "format_version": int(map_block.format_version),
            "object_kind": map_block.object_kind,
            "codec": map_block.codec,
            "entry_count": int(map_block.entry_count),
            "raw_byte_count": int(map_block.raw_byte_count),
            "stored_byte_count": int(map_block.stored_byte_count),
            "payload": map_block.payload,
            "created_at": _utcnow(),
        },
    )


async def _verify_map_block(
    session: Any,
    *,
    schema: str,
    map_block: SharedBlock,
) -> None:
    map_block_result = await session.execute(
        text(
            f"""
            SELECT format_version, object_kind, codec, entry_count,
                   raw_byte_count, stored_byte_count
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = :block_hash
            """
        ),
        {"block_hash": map_block.block_hash},
    )
    map_block_row = map_block_result.first()
    observed_by_field = _row_mapping(map_block_row) if map_block_row is not None else {}
    expected_by_field = {
        "format_version": int(map_block.format_version),
        "object_kind": map_block.object_kind,
        "codec": map_block.codec,
        "entry_count": int(map_block.entry_count),
        "raw_byte_count": int(map_block.raw_byte_count),
        "stored_byte_count": int(map_block.stored_byte_count),
    }
    if any(
        observed_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        raise RuntimeError("PTG V4 map block conflicts with stored CAS metadata")


def _map_pack_row(
    pack: V4SnapshotMapPack,
    snapshot_key: int,
) -> dict[str, Any]:
    first_block_key, first_fragment_no = pack.first_coordinate
    last_block_key, last_fragment_no = pack.last_coordinate
    return {
        "snapshot_key": int(snapshot_key),
        "object_kind": pack.object_kind,
        "pack_no": int(pack.pack_no),
        "first_block_key": first_block_key,
        "first_fragment_no": first_fragment_no,
        "last_block_key": last_block_key,
        "last_fragment_no": last_fragment_no,
        "coordinate_count": int(pack.coordinate_count),
        "entry_count": int(pack.entry_count),
        "logical_byte_count": int(pack.logical_byte_count),
        "map_block_hash": pack.map_block.block_hash,
    }


async def _insert_map_pack_row(
    session: Any,
    *,
    schema: str,
    row_by_field: Mapping[str, Any],
) -> None:
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v4_snapshot_map_pack
                (snapshot_key, object_kind, pack_no,
                 first_block_key, first_fragment_no,
                 last_block_key, last_fragment_no,
                 coordinate_count, entry_count, logical_byte_count,
                 map_block_hash)
            VALUES
                (:snapshot_key, :object_kind, :pack_no,
                 :first_block_key, :first_fragment_no,
                 :last_block_key, :last_fragment_no,
                 :coordinate_count, :entry_count, :logical_byte_count,
                 :map_block_hash)
            ON CONFLICT (snapshot_key, object_kind, pack_no) DO NOTHING
            """
        ),
        dict(row_by_field),
    )


async def _verify_map_pack_row(
    session: Any,
    *,
    schema: str,
    row_by_field: Mapping[str, Any],
) -> None:
    stored_pack_result = await session.execute(
        text(
            f"""
            SELECT object_kind, pack_no,
                   first_block_key, first_fragment_no,
                   last_block_key, last_fragment_no,
                   coordinate_count, entry_count, logical_byte_count,
                   map_block_hash
              FROM {schema}.ptg2_v4_snapshot_map_pack
             WHERE snapshot_key = :snapshot_key
               AND object_kind = :object_kind
               AND pack_no = :pack_no
            """
        ),
        {
            "snapshot_key": int(row_by_field["snapshot_key"]),
            "object_kind": row_by_field["object_kind"],
            "pack_no": int(row_by_field["pack_no"]),
        },
    )
    stored_pack_row = stored_pack_result.first()
    stored_by_field = (
        _row_mapping(stored_pack_row) if stored_pack_row is not None else {}
    )
    comparable_fields = tuple(
        field_name for field_name in row_by_field if field_name != "snapshot_key"
    )
    if any(
        stored_by_field.get(field_name) != row_by_field[field_name]
        for field_name in comparable_fields
    ):
        raise RuntimeError("PTG V4 snapshot map pack conflicts with stored coordinates")


async def _cancel_map_gc_candidates(
    session: Any,
    *,
    schema: str,
    block_hashes: Iterable[bytes],
) -> None:
    await session.execute(
        text(
            f"""
            DELETE FROM {schema}.ptg2_v3_gc_candidate
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
            """
        ),
        {"block_hashes": list(block_hashes)},
    )


async def _publish_v4_map_pack(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    pack: V4SnapshotMapPack,
) -> None:
    """Publish and verify one map pack plus its exact CAS references."""

    schema = _quote_ident(schema_name)
    metadata_by_hash = _target_metadata_by_hash(pack)
    await _verify_target_blocks(
        session,
        schema=schema,
        metadata_by_hash=metadata_by_hash,
    )
    map_block = pack.map_block
    await _publish_map_block(session, schema=schema, map_block=map_block)
    await _verify_map_block(session, schema=schema, map_block=map_block)
    row_by_field = _map_pack_row(pack, int(snapshot_key))
    await _insert_map_pack_row(session, schema=schema, row_by_field=row_by_field)
    await _verify_map_pack_row(session, schema=schema, row_by_field=row_by_field)
    # A V4 sweep must also consult decoded map-pack target hashes.  Removing an
    # already queued candidate here closes the publication race until that
    # generation-dispatched sweep performs its exact reachability check.
    await _cancel_map_gc_candidates(
        session,
        schema=schema,
        block_hashes=[*metadata_by_hash, map_block.block_hash],
    )


async def publish_v4_snapshot_maps(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    representation: str,
    references: Iterable[SharedBlockReference],
    max_coordinates_per_pack: int = PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
    progress_callback: Callable[[str, int], None] | None = None,
) -> V4SnapshotMapSummary:
    """Publish one exact packed map root from a bounded ordered stream."""

    await lock_v4_shared_layout_for_map_write(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )
    await _initialize_v4_snapshot_map_root(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        representation=representation,
    )
    accumulator = _V4SnapshotMapSummaryAccumulator()
    for pack in iter_v4_snapshot_map_packs(
        references,
        max_coordinates_per_pack=max_coordinates_per_pack,
    ):
        await _publish_v4_map_pack(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            pack=pack,
        )
        accumulator.add_pack(pack)
        if progress_callback is not None:
            progress_callback("map_packs", 1)
            progress_callback("map_coordinates", len(pack.references))
    # The root remains building so component/pattern/relation metadata can be
    # inserted under the same root fence. Seal performs the authoritative
    # reread and the only transition to complete.
    return accumulator.finish()


async def seal_v4_shared_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    expected_summary: V4SnapshotMapSummary,
    support_digest: bytes,
    layout_manifest: Mapping[str, Any],
    summary_batch_rows: int = 32,
    progress_callback: Callable[[str, int], None] | None = None,
) -> SealedSharedLayout:
    """Authoritatively re-read, complete, and atomically seal one V4 root."""

    normalized_support_digest = bytes(support_digest)
    if len(normalized_support_digest) != 32:
        raise ValueError("PTG V4 support digest must contain 32 bytes")
    schema = _quote_ident(schema_name)
    owner_result = await session.execute(
        text(
            f"""
            SELECT layout.snapshot_key,
                   root.state AS root_state,
                   root.format_version AS root_format_version,
                   root.map_format, root.representation,
                   root.projection_id_scope, root.map_digest,
                   root.object_kind_count, root.map_pack_count,
                   root.coordinate_count, root.entry_count,
                   root.logical_byte_count, root.stored_map_byte_count,
                   root.npi_count, root.component_count, root.pattern_count,
                   root.relation_count, root.heavy_owner_count
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.generation = :generation
               AND layout.state = 'building'
               AND layout.build_token = :build_token
             FOR UPDATE OF layout, root
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    owner_row = owner_result.first()
    owner = _row_mapping(owner_row) if owner_row is not None else {}
    representation = str(owner.get("representation") or "")
    if (
        owner.get("root_state") != "building"
        or int(owner.get("root_format_version") or -1)
        != PTG2_V4_MAP_FORMAT_VERSION
        or owner.get("map_format") != PTG2_V4_MAP_FORMAT
        or representation not in PTG2_V4_REPRESENTATIONS
        or owner.get("projection_id_scope") != PTG2_V4_PROJECTION_ID_SCOPE
    ):
        raise RuntimeError("PTG V4 seal requires one building compatible map root")

    observed_summary = await summarize_persisted_v4_snapshot_maps(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        batch_rows=summary_batch_rows,
        cancel_gc_candidates=True,
        **(
            {"progress_callback": progress_callback}
            if progress_callback is not None
            else {}
        ),
    )
    _require_matching_summaries(
        expected_summary,
        observed_summary,
        context="authoritative persisted summary",
    )
    observed_metadata = await summarize_persisted_v4_snapshot_metadata(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        map_summary=observed_summary,
        **(
            {"progress_callback": progress_callback}
            if progress_callback is not None
            else {}
        ),
    )
    if representation == "pattern_v1" and observed_metadata.pattern_count <= 0:
        raise RuntimeError("PTG V4 pattern representation has no pattern metadata")
    completion_result = await session.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v4_snapshot_map_root
               SET state = 'complete',
                   map_digest = :map_digest,
                   object_kind_count = :object_kind_count,
                   map_pack_count = :map_pack_count,
                   coordinate_count = :coordinate_count,
                   entry_count = :entry_count,
                   logical_byte_count = :logical_byte_count,
                   stored_map_byte_count = :stored_map_byte_count,
                   npi_count = :npi_count,
                   component_count = :component_count,
                   pattern_count = :pattern_count,
                   relation_count = :relation_count,
                   heavy_owner_count = :heavy_owner_count,
                   completed_at = :completed_at
             WHERE snapshot_key = :snapshot_key
               AND state = 'building'
               AND format_version = :format_version
               AND map_format = :map_format
               AND representation = :representation
               AND projection_id_scope = :projection_id_scope
            RETURNING snapshot_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "map_digest": observed_summary.map_digest,
            "object_kind_count": observed_summary.object_kind_count,
            "map_pack_count": observed_summary.map_pack_count,
            "coordinate_count": observed_summary.coordinate_count,
            "entry_count": observed_summary.entry_count,
            "logical_byte_count": observed_summary.logical_byte_count,
            "stored_map_byte_count": observed_summary.stored_map_byte_count,
            "npi_count": observed_metadata.npi_count,
            "component_count": observed_metadata.component_count,
            "pattern_count": observed_metadata.pattern_count,
            "relation_count": observed_metadata.relation_count,
            "heavy_owner_count": observed_metadata.heavy_owner_count,
            "completed_at": _utcnow(),
            "format_version": PTG2_V4_MAP_FORMAT_VERSION,
            "map_format": PTG2_V4_MAP_FORMAT,
            "representation": representation,
            "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
        },
    )
    if completion_result.scalar() is None:
        raise RuntimeError("PTG V4 map root could not be completed during seal")

    sealed_manifest = _manifest_with_v4_root(
        layout_manifest,
        representation=representation,
        summary=observed_summary,
        metadata=observed_metadata,
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": v4_layout_advisory_lock_key(observed_summary.map_digest)},
    )
    reusable_result = await session.execute(
        text(
            f"""
            SELECT layout.snapshot_key, layout.layout_manifest,
                   root.state AS root_state,
                   root.format_version AS root_format_version,
                   root.map_format, root.representation,
                   root.projection_id_scope, root.map_digest,
                   root.object_kind_count, root.map_pack_count,
                   root.coordinate_count, root.entry_count,
                   root.logical_byte_count, root.stored_map_byte_count,
                   root.npi_count, root.component_count, root.pattern_count,
                   root.relation_count, root.heavy_owner_count
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = layout.snapshot_key
             WHERE layout.generation = :generation
               AND layout.state = 'sealed'
               AND layout.mapping_digest = :mapping_digest
               AND layout.support_digest = :support_digest
               AND layout.snapshot_key <> :snapshot_key
             LIMIT 1
             FOR UPDATE OF layout, root
            """
        ),
        {
            "generation": PTG2_V4_SHARED_GENERATION,
            "mapping_digest": observed_summary.map_digest,
            "support_digest": normalized_support_digest,
            "snapshot_key": int(snapshot_key),
        },
    )
    reusable_row = reusable_result.first()
    if reusable_row is not None:
        reusable = _row_mapping(reusable_row)
        _reusable_manifest, reusable_summary, reusable_metadata = (
            _sealed_root_summaries(reusable)
        )
        if (
            reusable.get("root_state") != "complete"
            or int(reusable.get("root_format_version") or -1)
            != PTG2_V4_MAP_FORMAT_VERSION
            or reusable.get("map_format") != PTG2_V4_MAP_FORMAT
            or reusable.get("representation") != representation
            or reusable.get("projection_id_scope")
            != PTG2_V4_PROJECTION_ID_SCOPE
            or int(reusable.get("object_kind_count") or 0)
            != observed_summary.object_kind_count
            or reusable_metadata != observed_metadata
        ):
            raise RuntimeError("reusable PTG V4 layout root is incompatible")
        _require_matching_summaries(
            observed_summary,
            reusable_summary,
            context="reusable root",
        )
        _validate_v4_manifest_root(
            reusable.get("layout_manifest") or {},
            representation=representation,
            summary=reusable_summary,
            metadata=reusable_metadata,
        )
        reusable_snapshot_key = int(reusable["snapshot_key"])
        await session.execute(
            text(
                f"""
                UPDATE {schema}.ptg2_v3_snapshot_layout
                   SET heartbeat_at = :heartbeat_at,
                       lease_until = :lease_until
                 WHERE snapshot_key = :snapshot_key
                   AND state = 'sealed'
                   AND generation = :generation
                """
            ),
            {
                "snapshot_key": reusable_snapshot_key,
                "generation": PTG2_V4_SHARED_GENERATION,
                "heartbeat_at": _utcnow(),
                "lease_until": _lease_deadline(sealed=True),
            },
        )
        await session.execute(
            text(
                f"""
                UPDATE {schema}.ptg2_v3_layout_fingerprint
                   SET snapshot_key = :reusable_snapshot_key
                 WHERE snapshot_key = :snapshot_key
                """
            ),
            {
                "reusable_snapshot_key": reusable_snapshot_key,
                "snapshot_key": int(snapshot_key),
            },
        )
        await delete_shared_layout_dense_rows(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
        )
        delete_result = await session.execute(
            text(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_layout
                 WHERE snapshot_key = :snapshot_key
                   AND generation = :generation
                   AND state = 'building'
                   AND build_token = :build_token
                RETURNING snapshot_key
                """
            ),
            {
                "snapshot_key": int(snapshot_key),
                "generation": PTG2_V4_SHARED_GENERATION,
                "build_token": str(build_token),
            },
        )
        if delete_result.scalar() is None:
            raise RuntimeError("PTG V4 duplicate build changed before reuse")
        return SealedSharedLayout(
            reusable_snapshot_key,
            observed_summary.map_digest,
            True,
        )

    update_result = await session.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET state = 'sealed',
                   mapping_digest = :mapping_digest,
                   support_digest = :support_digest,
                   layout_manifest = CAST(:layout_manifest AS jsonb),
                   logical_byte_count = :logical_byte_count,
                   heartbeat_at = :heartbeat_at,
                   lease_until = :lease_until,
                   published_at = :published_at
             WHERE snapshot_key = :snapshot_key
               AND generation = :generation
               AND state = 'building'
               AND build_token = :build_token
            RETURNING snapshot_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "build_token": str(build_token),
            "mapping_digest": observed_summary.map_digest,
            "support_digest": normalized_support_digest,
            "layout_manifest": json.dumps(
                sealed_manifest,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "logical_byte_count": observed_summary.logical_byte_count,
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(sealed=True),
            "published_at": _utcnow(),
        },
    )
    if update_result.scalar() is None:
        raise RuntimeError("PTG V4 layout was not in the expected building generation")
    return SealedSharedLayout(
        int(snapshot_key),
        observed_summary.map_digest,
        False,
    )


async def bind_snapshot_to_v4_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_key: int,
) -> None:
    """Bind one logical snapshot only to a sealed, complete V4 map root."""

    schema = _quote_ident(schema_name)
    binding_result = await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key, created_at)
            SELECT :snapshot_id, layout.snapshot_key, :created_at
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = layout.snapshot_key
               AND root.state = 'complete'
               AND root.map_digest = layout.mapping_digest
             WHERE layout.snapshot_key = :snapshot_key
               AND layout.state = 'sealed'
               AND layout.generation = :generation
            ON CONFLICT (snapshot_id) DO NOTHING
            RETURNING snapshot_id
            """
        ),
        {
            "snapshot_id": str(snapshot_id),
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V4_SHARED_GENERATION,
            "created_at": _utcnow(),
        },
    )
    if binding_result.scalar() is not None:
        return
    existing_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key
              FROM {schema}.ptg2_v3_snapshot_binding
             WHERE snapshot_id = :snapshot_id
            """
        ),
        {"snapshot_id": str(snapshot_id)},
    )
    existing_snapshot_key = existing_result.scalar()
    if existing_snapshot_key is not None and int(existing_snapshot_key) == int(snapshot_key):
        return
    raise RuntimeError("logical PTG snapshot is not bindable to the sealed V4 layout")


__all__ = [
    "PTG2_V4_DEFAULT_COORDINATES_PER_PACK",
    "PTG2_V4_COMPONENT_TABLE",
    "PTG2_V4_HEAVY_OWNER_TABLE",
    "PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS",
    "PTG2_V4_GRAPH_DIAGNOSTIC_TABLE",
    "PTG2_V4_GRAPH_RESOURCE_FIELDS",
    "PTG2_V4_MAP_BLOCK_KIND",
    "PTG2_V4_MAP_FORMAT",
    "PTG2_V4_MAP_FORMAT_VERSION",
    "PTG2_V4_MAP_MANIFEST_CONTRACT",
    "PTG2_V4_MEMBER_PAGE_CONTRACT",
    "PTG2_V4_NPI_TABLE",
    "PTG2_V4_NPI_PREFIX_TABLE",
    "PTG2_V4_MAX_COORDINATES_PER_PACK",
    "PTG2_V4_PROJECTION_ID_SCOPE",
    "PTG2_V4_OWNER_LOCATOR_PAGE_CONTRACT",
    "PTG2_V4_PATTERN_TABLE",
    "PTG2_V4_PROVIDER_GRAPH_CONTRACT",
    "PTG2_V4_FUTURE_REPRESENTATIONS",
    "PTG2_V4_REPRESENTATIONS",
    "PTG2_V4_RELATION_MANIFEST_TABLE",
    "PTG2_V4_SHARED_GENERATION",
    "V4SnapshotMapCoordinate",
    "V4SnapshotMapPack",
    "V4SnapshotMapSummary",
    "V4NPIDictionaryPublication",
    "V4MetadataTablePublication",
    "V4SnapshotMetadataSummary",
    "decode_v4_snapshot_map_pack",
    "encode_v4_snapshot_map_pack",
    "bind_snapshot_to_v4_layout",
    "iter_v4_snapshot_map_packs",
    "lock_v4_shared_layout_for_map_write",
    "publish_v4_npi_dictionary",
    "publish_v4_heavy_owners",
    "publish_v4_patterns",
    "publish_v4_provider_components",
    "publish_v4_relation_manifests",
    "publish_v4_snapshot_maps",
    "reserve_v4_shared_layout",
    "seal_v4_shared_layout",
    "summarize_persisted_v4_snapshot_maps",
    "summarize_persisted_v4_snapshot_metadata",
    "summarize_v4_snapshot_map_packs",
    "v4_layout_fingerprint",
    "v4_layout_advisory_lock_key",
    "v4_map_pack_target_hashes",
    "touch_v4_shared_layout_build",
]
