# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish-time audit verification for packed PTG V4 provider graphs.

The compiler emits one deterministic provider-set witness row.  This module
does not trust that row by itself: before a V4 layout is sealed it proves the
row through the persisted packed relations and then reuses the V3 price and
occurrence contracts.  Reads are bounded, content-addressed blocks are
authenticated, and only pages needed by the selected audit candidates are
loaded.
"""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_audit import (
    PTG2_V3_AUDIT_MAX_BLOCK_BYTES,
    AuditCandidate,
    SharedAuditPublication,
    _BuildingBlockReader,
    _CORE_LAYOUT_DOMAIN,
    _ReadBudget,
    _insert_occurrences,
    _integer,
    _price_memberships,
    _publication_metadata,
    _validate_candidate_provider_counts,
    _validate_snapshot_source_dictionary,
    build_audit_occurrences,
    load_audit_candidates,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedLayoutBuildOwnership,
    shared_block_hash,
    shared_support_digest,
)
from process.ptg_parts.ptg2_v4_graph_compiler import V4GraphCompilationResult
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_MAP_FORMAT_VERSION,
    PTG2_V4_NPI_TABLE,
    PTG2_V4_PROJECTION_ID_SCOPE,
    PTG2_V4_SHARED_GENERATION,
    V4SnapshotMapCoordinate,
    decode_v4_snapshot_map_pack,
)


PTG2_V4_AUDIT_PROVIDER_SELECTION = "compiler_witness_graph_verified_v1"
PTG2_V4_AUDIT_MAX_MAP_PACK_BYTES = 4 * 1024 * 1024
PTG2_V4_AUDIT_MAX_GRAPH_PAGE_BYTES = 4 * 1024 * 1024
PTG2_V4_AUDIT_MAX_WITNESS_BYTES = PTG2_V3_AUDIT_MAX_BLOCK_BYTES
PTG2_V4_AUDIT_MAX_BINARY_SEARCH_ROUNDS = 32
PTG2_V4_HEAVY_BITMAP_MAGIC = b"PTG2V4BM"
PTG2_V4_HEAVY_BITMAP_HEADER_BYTES = 24
PTG2_V4_HEAVY_BITMAP_FRAGMENT_MAGIC = b"PTG2V4BF"
PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES = 32

_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PG_COPY_TRAILER = -1
_LOCATOR = struct.Struct("<QI")
_HEAVY_HEADER = struct.Struct("<8sIIII")
_HEAVY_FRAGMENT_HEADER = struct.Struct("<8sIIIIII")

_COMMON_RELATIONS = frozenset(
    {
        "set_components",
        "component_groups",
        "npi_groups_exact",
        "group_npis_exact",
    }
)
_DIRECT_RELATIONS = frozenset({"group_sets_direct", "set_groups_direct"})
_PATTERN_RELATIONS = frozenset(
    {
        "group_patterns",
        "pattern_groups",
        "pattern_sets",
        "set_patterns",
        "npi_patterns",
    }
)
_REPRESENTATION_BY_COMPILER_LAYOUT = {
    "direct": "direct_v1",
    "pattern": "pattern_v1",
}


@dataclass(frozen=True)
class V4ProviderSetAuditWitness:
    provider_set_key: int
    provider_group_key: int
    npi: int


@dataclass(frozen=True)
class _V4RelationManifest:
    relation: str
    member_object_kind: str
    locator_object_kind: str
    owner_base: int
    owner_count: int
    logical_member_count: int
    vector_member_count: int
    member_width: int
    member_page_bytes: int
    locator_page_bytes: int
    locator_owner_span: int

    @property
    def members_per_page(self) -> int:
        """Return the authenticated member capacity of one graph page."""

        return self.member_page_bytes // self.member_width


@dataclass(frozen=True)
class _V4HeavyOwner:
    relation: str
    owner_key: int
    object_kind: str
    member_count: int
    member_base: int
    member_span: int
    fragment_count: int


@dataclass(frozen=True)
class _V4PhysicalBlock:
    block_hash: bytes
    object_kind: str
    entry_count: int
    payload: bytes


def _unframe_heavy_bitmap_fragment(
    fragment_payload: bytes,
    *,
    owner: _V4HeavyOwner,
    fragment_no: int,
    entry_count: int,
    logical_offset: int,
) -> bytes:
    """Reject legacy/unframed bitmap blocks and return logical bytes."""

    if len(fragment_payload) <= PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES:
        raise RuntimeError("PTG V4 audit heavy bitmap fragment is truncated")
    (
        magic,
        owner_key,
        member_base,
        member_span,
        member_count,
        stored_fragment_no,
        stored_entry_count,
    ) = _HEAVY_FRAGMENT_HEADER.unpack_from(fragment_payload)
    if (
        magic != PTG2_V4_HEAVY_BITMAP_FRAGMENT_MAGIC
        or owner_key != owner.owner_key
        or member_base != owner.member_base
        or member_span != owner.member_span
        or member_count != owner.member_count
        or stored_fragment_no != int(fragment_no)
        or stored_entry_count != int(entry_count)
    ):
        raise RuntimeError(
            "PTG V4 audit heavy bitmap fragment conflicts with its coordinate"
        )
    logical_payload = fragment_payload[
        PTG2_V4_HEAVY_BITMAP_FRAGMENT_HEADER_BYTES:
    ]
    bitmap_offset = max(
        PTG2_V4_HEAVY_BITMAP_HEADER_BYTES - int(logical_offset),
        0,
    )
    if (
        sum(byte.bit_count() for byte in logical_payload[bitmap_offset:])
        != int(entry_count)
    ):
        raise RuntimeError("PTG V4 audit heavy bitmap fragment count changed")
    return logical_payload


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _nonnegative_int(
    value: Any,
    *,
    label: str,
    maximum: int = 2**63 - 1,
) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"PTG V4 audit has invalid {label}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"PTG V4 audit has invalid {label}") from exc
    if normalized < 0 or normalized > maximum:
        raise RuntimeError(f"PTG V4 audit has invalid {label}")
    return normalized


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _provider_witness_artifact(
    graph_compilation: V4GraphCompilationResult,
) -> tuple[Path, int, str]:
    artifacts = tuple(
        artifact
        for artifact in graph_compilation.output_artifacts
        if artifact.name == "provider_set_audit_npi"
    )
    if len(artifacts) != 1:
        raise RuntimeError("PTG V4 audit witness artifact is missing or duplicated")
    artifact = artifacts[0]
    path = Path(artifact.path).resolve()
    expected_path = Path(
        graph_compilation.provider_set_audit_npi_copy_path
    ).resolve()
    if path != expected_path or path.is_symlink() or not path.is_file():
        raise RuntimeError("PTG V4 audit witness artifact path changed")
    if (
        artifact.byte_count < len(_PG_COPY_HEADER) + 2
        or artifact.byte_count > PTG2_V4_AUDIT_MAX_WITNESS_BYTES
        or path.stat().st_size != artifact.byte_count
    ):
        raise RuntimeError("PTG V4 audit witness artifact authentication failed")
    return path, int(artifact.row_count), str(artifact.sha256)


def _read_exact(source: Any, byte_count: int, *, label: str) -> bytes:
    payload = source.read(int(byte_count))
    if len(payload) != int(byte_count):
        raise RuntimeError(f"PTG V4 audit witness truncates {label}")
    return payload


def load_v4_audit_witnesses(
    graph_compilation: V4GraphCompilationResult,
    *,
    provider_set_keys: Iterable[int],
) -> dict[int, V4ProviderSetAuditWitness]:
    """Authenticate and stream the compiler witness, retaining requested sets only."""

    path, expected_row_count, expected_sha256 = _provider_witness_artifact(
        graph_compilation
    )
    requested_keys = {
        int(provider_set_key) for provider_set_key in provider_set_keys
    }
    if any(
        provider_set_key < 0 or provider_set_key > 0x7FFFFFFF
        for provider_set_key in requested_keys
    ):
        raise RuntimeError("PTG V4 audit provider-set key is outside int4 range")
    witnesses_by_set: dict[int, V4ProviderSetAuditWitness] = {}
    previous_provider_set_key: int | None = None
    observed_row_count = 0
    with path.open("rb") as copy_file:
        if (
            _read_exact(copy_file, len(_PG_COPY_HEADER), label="COPY header")
            != _PG_COPY_HEADER
        ):
            raise RuntimeError("PTG V4 audit witness has an invalid COPY header")
        while True:
            field_count = struct.unpack(
                ">h", _read_exact(copy_file, 2, label="COPY row header")
            )[0]
            if field_count == _PG_COPY_TRAILER:
                if copy_file.read(1):
                    raise RuntimeError("PTG V4 audit witness has trailing COPY bytes")
                break
            if field_count != 3:
                raise RuntimeError("PTG V4 audit witness has an invalid COPY row width")
            fields: list[bytes] = []
            for field_index, expected_width in enumerate((4, 4, 8)):
                width = struct.unpack(
                    ">i", _read_exact(copy_file, 4, label="COPY field width")
                )[0]
                if width != expected_width:
                    raise RuntimeError(
                        "PTG V4 audit witness has an invalid COPY field width "
                        f"at column {field_index}"
                    )
                fields.append(_read_exact(copy_file, width, label="COPY field"))
            provider_set_key = struct.unpack(">i", fields[0])[0]
            provider_group_key = struct.unpack(">i", fields[1])[0]
            npi = struct.unpack(">q", fields[2])[0]
            if (
                provider_set_key < 0
                or provider_group_key < 0
                or not 1_000_000_000 <= npi <= 9_999_999_999
                or (
                    previous_provider_set_key is not None
                    and provider_set_key <= previous_provider_set_key
                )
            ):
                raise RuntimeError("PTG V4 audit witness rows are invalid or unordered")
            previous_provider_set_key = provider_set_key
            observed_row_count += 1
            if provider_set_key in requested_keys:
                witnesses_by_set[provider_set_key] = V4ProviderSetAuditWitness(
                    provider_set_key=provider_set_key,
                    provider_group_key=provider_group_key,
                    npi=npi,
                )
    if observed_row_count != expected_row_count:
        raise RuntimeError("PTG V4 audit witness row count changed after compilation")
    if _sha256_file(path) != expected_sha256:
        raise RuntimeError("PTG V4 audit witness authentication changed while reading")
    return witnesses_by_set


def _relation_kinds(relation: str) -> tuple[str, str]:
    normalized = str(relation or "").strip().lower()
    if normalized not in _COMMON_RELATIONS | _DIRECT_RELATIONS | _PATTERN_RELATIONS:
        raise RuntimeError(f"PTG V4 audit relation is unsupported: {relation!r}")
    return (
        f"v4_{normalized}_locators_v1",
        f"v4_{normalized}_members_v1",
    )


def _validate_relation_for_representation(representation: str, relation: str) -> None:
    if relation in _COMMON_RELATIONS:
        return
    if representation == "direct_v1" and relation in _DIRECT_RELATIONS:
        return
    if representation == "pattern_v1" and relation in _PATTERN_RELATIONS:
        return
    raise RuntimeError(
        f"PTG V4 audit {representation} layout does not publish {relation}"
    )


def _validated_physical_payload(
    block_row: Mapping[str, Any],
    *,
    expected_kind: str,
    maximum_raw_bytes: int,
    hash_field: str = "block_hash",
    version_field: str = "format_version",
    kind_field: str = "object_kind",
    codec_field: str = "codec",
    entry_count_field: str = "block_entry_count",
    raw_byte_count_field: str = "raw_byte_count",
    stored_byte_count_field: str = "stored_byte_count",
    payload_field: str = "payload",
) -> _V4PhysicalBlock:
    block_hash = bytes(block_row.get(hash_field) or b"")
    block_payload = bytes(block_row.get(payload_field) or b"")
    format_version = _nonnegative_int(
        block_row.get(version_field), label="CAS format version"
    )
    entry_count = _nonnegative_int(
        block_row.get(entry_count_field), label="CAS entry count"
    )
    raw_byte_count = _nonnegative_int(
        block_row.get(raw_byte_count_field), label="CAS raw bytes"
    )
    stored_byte_count = _nonnegative_int(
        block_row.get(stored_byte_count_field), label="CAS stored bytes"
    )
    codec = str(block_row.get(codec_field) or "")
    object_kind = str(block_row.get(kind_field) or "")
    if (
        len(block_hash) != 32
        or format_version != PTG2_V3_SHARED_FORMAT_VERSION
        or object_kind != expected_kind
        or codec != "none"
        or raw_byte_count > int(maximum_raw_bytes)
        or raw_byte_count != stored_byte_count
        or stored_byte_count != len(block_payload)
        or shared_block_hash(
            format_version=format_version,
            object_kind=object_kind,
            codec=codec,
            payload=block_payload,
        )
        != block_hash
    ):
        raise RuntimeError("PTG V4 audit CAS block authentication failed")
    return _V4PhysicalBlock(
        block_hash=block_hash,
        object_kind=object_kind,
        entry_count=entry_count,
        payload=block_payload,
    )


class _V4PersistedGraphReader:
    """Read only authenticated pages reachable from one building V4 root."""

    def __init__(
        self,
        session: Any,
        *,
        schema_name: str,
        snapshot_key: int,
        representation: str,
        budget: _ReadBudget,
    ) -> None:
        self.session = session
        self.schema_name = str(schema_name)
        self.snapshot_key = int(snapshot_key)
        self.representation = str(representation)
        self.budget = budget
        self._coordinate_cache: dict[
            tuple[str, int, int], V4SnapshotMapCoordinate
        ] = {}
        self._physical_cache: dict[bytes, _V4PhysicalBlock] = {}
        self._manifest_cache: dict[str, _V4RelationManifest] = {}
        self._member_page_cache: dict[tuple[str, int], tuple[int, ...]] = {}
        self._charged_hashes: set[bytes] = set()

    def _charge_block(self, block: _V4PhysicalBlock) -> None:
        if block.block_hash in self._charged_hashes:
            return
        self._charged_hashes.add(block.block_hash)
        self.budget.add_block(
            f"{block.object_kind}:{block.block_hash.hex()}",
            0,
            0,
            len(block.payload),
        )

    async def _manifest(self, relation: str) -> _V4RelationManifest:
        """Load and validate one immutable relation manifest exactly once."""

        normalized = str(relation or "").strip().lower()
        _validate_relation_for_representation(self.representation, normalized)
        cached = self._manifest_cache.get(normalized)
        if cached is not None:
            return cached
        expected_locator_kind, expected_member_kind = _relation_kinds(normalized)
        schema = _quote_ident(self.schema_name)
        query_result = await self.session.execute(
            db.text(
                f"""
                SELECT relation, member_object_kind, locator_object_kind,
                       owner_base, owner_count, logical_member_count,
                       vector_member_count, member_width,
                       member_page_bytes, locator_page_bytes, locator_owner_span
                  FROM {schema}.ptg2_v4_relation_manifest
                 WHERE snapshot_key = :snapshot_key
                   AND relation = :relation
                """
            ),
            {"snapshot_key": self.snapshot_key, "relation": normalized},
        )
        manifest_rows = [
            _row_mapping(manifest_row) for manifest_row in query_result
        ]
        if len(manifest_rows) != 1:
            raise RuntimeError("PTG V4 audit relation manifest is missing or duplicated")
        manifest_row = manifest_rows[0]
        manifest = _V4RelationManifest(
            relation=str(manifest_row.get("relation") or ""),
            member_object_kind=str(
                manifest_row.get("member_object_kind") or ""
            ),
            locator_object_kind=str(
                manifest_row.get("locator_object_kind") or ""
            ),
            owner_base=_nonnegative_int(
                manifest_row.get("owner_base"),
                label="relation owner_base",
                maximum=0xFFFFFFFF,
            ),
            owner_count=_nonnegative_int(
                manifest_row.get("owner_count"),
                label="relation owner_count",
                maximum=0x100000000,
            ),
            logical_member_count=_nonnegative_int(
                manifest_row.get("logical_member_count"),
                label="relation logical_member_count",
            ),
            vector_member_count=_nonnegative_int(
                manifest_row.get("vector_member_count"),
                label="relation vector_member_count",
            ),
            member_width=_nonnegative_int(
                manifest_row.get("member_width"),
                label="relation member_width",
                maximum=64,
            ),
            member_page_bytes=_nonnegative_int(
                manifest_row.get("member_page_bytes"),
                label="relation member_page_bytes",
                maximum=PTG2_V4_AUDIT_MAX_GRAPH_PAGE_BYTES,
            ),
            locator_page_bytes=_nonnegative_int(
                manifest_row.get("locator_page_bytes"),
                label="relation locator_page_bytes",
                maximum=PTG2_V4_AUDIT_MAX_GRAPH_PAGE_BYTES,
            ),
            locator_owner_span=_nonnegative_int(
                manifest_row.get("locator_owner_span"),
                label="relation locator_owner_span",
                maximum=0xFFFFFFFF,
            ),
        )
        if (
            manifest.relation != normalized
            or manifest.member_object_kind != expected_member_kind
            or manifest.locator_object_kind != expected_locator_kind
            or manifest.member_width != 4
            or manifest.member_page_bytes < 4
            or manifest.member_page_bytes % manifest.member_width
            or manifest.locator_page_bytes < _LOCATOR.size
            or manifest.locator_page_bytes % _LOCATOR.size
            or manifest.locator_owner_span <= 0
            or manifest.locator_page_bytes
            != manifest.locator_owner_span * _LOCATOR.size
            or manifest.owner_base + manifest.owner_count > 0x100000000
            or manifest.vector_member_count > manifest.logical_member_count
        ):
            raise RuntimeError("PTG V4 audit relation manifest is inconsistent")
        self._manifest_cache[normalized] = manifest
        return manifest

    async def _map_coordinates(
        self,
        *,
        object_kind: str,
        coordinate_pairs: Iterable[tuple[int, int]],
    ) -> dict[tuple[int, int], V4SnapshotMapCoordinate]:
        """Load authenticated packed-map coordinates under the audit budget."""

        requested_pairs = tuple(
            sorted(
                {
                    (int(block_key), int(fragment_no))
                    for block_key, fragment_no in coordinate_pairs
                }
            )
        )
        if any(
            block_key < 0 or fragment_no < 0
            for block_key, fragment_no in requested_pairs
        ):
            raise RuntimeError("PTG V4 audit map coordinate is negative")
        result_by_pair: dict[tuple[int, int], V4SnapshotMapCoordinate] = {}
        missing_pairs: list[tuple[int, int]] = []
        for pair in requested_pairs:
            cached = self._coordinate_cache.get((object_kind, pair[0], pair[1]))
            if cached is None:
                missing_pairs.append(pair)
            else:
                result_by_pair[pair] = cached
        if missing_pairs:
            schema = _quote_ident(self.schema_name)
            query_result = await self.session.execute(
                db.text(
                    f"""
                    SELECT pack.pack_no, pack.first_block_key, pack.first_fragment_no,
                           pack.last_block_key, pack.last_fragment_no,
                           pack.coordinate_count, pack.entry_count AS pack_entry_count,
                           pack.map_block_hash,
                           block.format_version AS map_format_version,
                           block.object_kind AS map_object_kind,
                           block.codec AS map_codec,
                           block.entry_count AS map_block_entry_count,
                           block.raw_byte_count AS map_raw_byte_count,
                           block.stored_byte_count AS map_stored_byte_count,
                           block.payload AS map_payload
                      FROM {schema}.ptg2_v4_snapshot_map_pack AS pack
                      JOIN {schema}.ptg2_v3_block AS block
                        ON block.block_hash = pack.map_block_hash
                     WHERE pack.snapshot_key = :snapshot_key
                       AND pack.object_kind = :object_kind
                       AND EXISTS (
                           SELECT 1
                             FROM unnest(
                                      CAST(:block_keys AS bigint[]),
                                      CAST(:fragment_nos AS integer[])
                                  ) AS wanted(block_key, fragment_no)
                            WHERE ROW(wanted.block_key, wanted.fragment_no)
                                  BETWEEN ROW(pack.first_block_key, pack.first_fragment_no)
                                      AND ROW(pack.last_block_key, pack.last_fragment_no)
                       )
                     ORDER BY pack.pack_no
                    """
                ),
                {
                    "snapshot_key": self.snapshot_key,
                    "object_kind": object_kind,
                    "block_keys": tuple(pair[0] for pair in missing_pairs),
                    "fragment_nos": tuple(pair[1] for pair in missing_pairs),
                },
            )
            missing_pair_set = set(missing_pairs)
            observed_pack_nos: set[int] = set()
            for raw_row in query_result:
                map_pack_row = _row_mapping(raw_row)
                pack_no = _nonnegative_int(
                    map_pack_row.get("pack_no"),
                    label="map pack number",
                    maximum=0x7FFFFFFF,
                )
                if pack_no in observed_pack_nos:
                    raise RuntimeError("PTG V4 audit map query duplicated a pack")
                observed_pack_nos.add(pack_no)
                block = _validated_physical_payload(
                    map_pack_row,
                    expected_kind=PTG2_V4_MAP_BLOCK_KIND,
                    maximum_raw_bytes=PTG2_V4_AUDIT_MAX_MAP_PACK_BYTES,
                    hash_field="map_block_hash",
                    version_field="map_format_version",
                    kind_field="map_object_kind",
                    codec_field="map_codec",
                    entry_count_field="map_block_entry_count",
                    raw_byte_count_field="map_raw_byte_count",
                    stored_byte_count_field="map_stored_byte_count",
                    payload_field="map_payload",
                )
                self._charge_block(block)
                try:
                    coordinates = decode_v4_snapshot_map_pack(
                        block.payload,
                        expected_object_kind=object_kind,
                    )
                except ValueError as exc:
                    raise RuntimeError("PTG V4 audit map pack is invalid") from exc
                first = (coordinates[0].block_key, coordinates[0].fragment_no)
                last = (coordinates[-1].block_key, coordinates[-1].fragment_no)
                expected_first = (
                    _nonnegative_int(
                        map_pack_row.get("first_block_key"),
                        label="map first key",
                    ),
                    _nonnegative_int(
                        map_pack_row.get("first_fragment_no"),
                        label="map first fragment",
                    ),
                )
                expected_last = (
                    _nonnegative_int(
                        map_pack_row.get("last_block_key"),
                        label="map last key",
                    ),
                    _nonnegative_int(
                        map_pack_row.get("last_fragment_no"),
                        label="map last fragment",
                    ),
                )
                if (
                    first != expected_first
                    or last != expected_last
                    or len(coordinates)
                    != _nonnegative_int(
                        map_pack_row.get("coordinate_count"),
                        label="map coordinate count",
                    )
                    or block.entry_count != len(coordinates)
                    or sum(coordinate.entry_count for coordinate in coordinates)
                    != _nonnegative_int(
                        map_pack_row.get("pack_entry_count"),
                        label="map entry count",
                    )
                ):
                    raise RuntimeError("PTG V4 audit map pack metadata is inconsistent")
                for coordinate in coordinates:
                    cache_key = (
                        coordinate.object_kind,
                        int(coordinate.block_key),
                        int(coordinate.fragment_no),
                    )
                    previous = self._coordinate_cache.setdefault(cache_key, coordinate)
                    if previous != coordinate:
                        raise RuntimeError("PTG V4 audit map coordinate conflicts")
                    pair = (int(coordinate.block_key), int(coordinate.fragment_no))
                    if pair in missing_pair_set:
                        if pair in result_by_pair:
                            raise RuntimeError("PTG V4 audit map coordinate is ambiguous")
                        result_by_pair[pair] = coordinate
        if set(result_by_pair) != set(requested_pairs):
            raise RuntimeError("PTG V4 audit map is missing a required coordinate")
        return result_by_pair

    async def _physical_blocks(
        self,
        *,
        object_kind: str,
        coordinates: Iterable[V4SnapshotMapCoordinate],
        maximum_raw_bytes: int,
    ) -> dict[bytes, _V4PhysicalBlock]:
        coordinate_by_hash: dict[bytes, V4SnapshotMapCoordinate] = {}
        for coordinate in coordinates:
            block_hash = bytes(coordinate.block_hash)
            previous = coordinate_by_hash.setdefault(block_hash, coordinate)
            if int(previous.entry_count) != int(coordinate.entry_count):
                raise RuntimeError("PTG V4 audit aliased CAS count is inconsistent")
        result_by_hash: dict[bytes, _V4PhysicalBlock] = {}
        missing_hashes: list[bytes] = []
        for block_hash, coordinate in coordinate_by_hash.items():
            cached = self._physical_cache.get(block_hash)
            if cached is None:
                missing_hashes.append(block_hash)
            elif cached.object_kind != object_kind or cached.entry_count != coordinate.entry_count:
                raise RuntimeError("PTG V4 audit cached CAS identity is inconsistent")
            else:
                result_by_hash[block_hash] = cached
        if missing_hashes:
            schema = _quote_ident(self.schema_name)
            query_result = await self.session.execute(
                db.text(
                    f"""
                    SELECT block_hash, format_version, object_kind, codec,
                           entry_count AS block_entry_count,
                           raw_byte_count, stored_byte_count, payload
                      FROM {schema}.ptg2_v3_block
                     WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
                    """
                ),
                {"block_hashes": missing_hashes},
            )
            for raw_row in query_result:
                block = _validated_physical_payload(
                    _row_mapping(raw_row),
                    expected_kind=object_kind,
                    maximum_raw_bytes=maximum_raw_bytes,
                )
                coordinate = coordinate_by_hash.get(block.block_hash)
                if coordinate is None or block.block_hash in result_by_hash:
                    raise RuntimeError("PTG V4 audit CAS query returned an unexpected block")
                if block.entry_count != int(coordinate.entry_count):
                    raise RuntimeError("PTG V4 audit CAS entry count changed")
                self._physical_cache[block.block_hash] = block
                self._charge_block(block)
                result_by_hash[block.block_hash] = block
        if set(result_by_hash) != set(coordinate_by_hash):
            raise RuntimeError("PTG V4 audit graph references a missing CAS block")
        return result_by_hash

    async def _heavy_owners(
        self,
        manifest: _V4RelationManifest,
        owner_keys: Iterable[int],
    ) -> dict[int, _V4HeavyOwner]:
        """Load exact heavy-owner metadata for only the requested keys."""

        requested_keys = tuple(
            sorted({int(owner_key) for owner_key in owner_keys})
        )
        if not requested_keys:
            return {}
        schema = _quote_ident(self.schema_name)
        query_result = await self.session.execute(
            db.text(
                f"""
                SELECT relation, owner_key, object_kind, member_count,
                       member_base, member_span, fragment_count
                  FROM {schema}.ptg2_v4_heavy_owner
                 WHERE snapshot_key = :snapshot_key
                   AND relation = :relation
                   AND owner_key = ANY(CAST(:owner_keys AS bigint[]))
                 ORDER BY owner_key
                """
            ),
            {
                "snapshot_key": self.snapshot_key,
                "relation": manifest.relation,
                "owner_keys": requested_keys,
            },
        )
        expected_kind = f"v4_{manifest.relation}_heavy_bitmap_v1"
        owners_by_key: dict[int, _V4HeavyOwner] = {}
        owner_limit = manifest.owner_base + manifest.owner_count
        for raw_row in query_result:
            heavy_row = _row_mapping(raw_row)
            owner = _V4HeavyOwner(
                relation=str(heavy_row.get("relation") or ""),
                owner_key=_nonnegative_int(
                    heavy_row.get("owner_key"),
                    label="heavy owner key",
                    maximum=0xFFFFFFFF,
                ),
                object_kind=str(heavy_row.get("object_kind") or ""),
                member_count=_nonnegative_int(
                    heavy_row.get("member_count"),
                    label="heavy member count",
                    maximum=0xFFFFFFFF,
                ),
                member_base=_nonnegative_int(
                    heavy_row.get("member_base"),
                    label="heavy member base",
                    maximum=0xFFFFFFFF,
                ),
                member_span=_nonnegative_int(
                    heavy_row.get("member_span"),
                    label="heavy member span",
                    maximum=0xFFFFFFFF,
                ),
                fragment_count=_nonnegative_int(
                    heavy_row.get("fragment_count"),
                    label="heavy fragment count",
                    maximum=0x7FFFFFFF,
                ),
            )
            if (
                owner.relation != manifest.relation
                or owner.object_kind != expected_kind
                or not manifest.owner_base <= owner.owner_key < owner_limit
                or owner.member_span <= 0
                or owner.fragment_count <= 0
                or owner.member_count > owner.member_span
                or owner.member_base + owner.member_span > 0x100000000
                or owner.owner_key in owners_by_key
            ):
                raise RuntimeError("PTG V4 audit heavy-owner manifest is inconsistent")
            owners_by_key[owner.owner_key] = owner
        return owners_by_key

    async def _locators(
        self,
        manifest: _V4RelationManifest,
        owner_keys: Iterable[int],
    ) -> dict[int, tuple[int, int]]:
        requested_keys = tuple(
            sorted({int(owner_key) for owner_key in owner_keys})
        )
        owner_limit = manifest.owner_base + manifest.owner_count
        if any(
            owner_key < manifest.owner_base or owner_key >= owner_limit
            for owner_key in requested_keys
        ):
            raise RuntimeError("PTG V4 audit relation owner is outside its manifest")
        page_keys = {
            manifest.owner_base
            + ((owner_key - manifest.owner_base) // manifest.locator_owner_span)
            * manifest.locator_owner_span
            for owner_key in requested_keys
        }
        coordinates = await self._map_coordinates(
            object_kind=manifest.locator_object_kind,
            coordinate_pairs=((page_key, 0) for page_key in page_keys),
        )
        blocks = await self._physical_blocks(
            object_kind=manifest.locator_object_kind,
            coordinates=coordinates.values(),
            maximum_raw_bytes=manifest.locator_page_bytes,
        )
        locators_by_owner: dict[int, tuple[int, int]] = {}
        for owner_key in requested_keys:
            page_key = manifest.owner_base + (
                (owner_key - manifest.owner_base) // manifest.locator_owner_span
            ) * manifest.locator_owner_span
            coordinate = coordinates[(page_key, 0)]
            block = blocks[bytes(coordinate.block_hash)]
            expected_bytes = block.entry_count * _LOCATOR.size
            local_index = owner_key - page_key
            if (
                block.entry_count < 0
                or block.entry_count > manifest.locator_owner_span
                or len(block.payload) != expected_bytes
                or local_index < 0
                or local_index >= block.entry_count
            ):
                raise RuntimeError("PTG V4 audit locator page is malformed")
            member_offset, member_count = _LOCATOR.unpack_from(
                block.payload, local_index * _LOCATOR.size
            )
            if member_offset + member_count > manifest.vector_member_count:
                raise RuntimeError("PTG V4 audit locator exceeds its relation")
            locators_by_owner[owner_key] = (member_offset, member_count)
        return locators_by_owner

    async def _member_pages(
        self,
        manifest: _V4RelationManifest,
        page_keys: Iterable[int],
    ) -> dict[int, tuple[int, ...]]:
        requested_keys = tuple(sorted({int(page_key) for page_key in page_keys}))
        members_by_page: dict[int, tuple[int, ...]] = {}
        missing_keys: list[int] = []
        for page_key in requested_keys:
            cached = self._member_page_cache.get((manifest.member_object_kind, page_key))
            if cached is None:
                missing_keys.append(page_key)
            else:
                members_by_page[page_key] = cached
        if missing_keys:
            coordinates = await self._map_coordinates(
                object_kind=manifest.member_object_kind,
                coordinate_pairs=((page_key, 0) for page_key in missing_keys),
            )
            blocks = await self._physical_blocks(
                object_kind=manifest.member_object_kind,
                coordinates=coordinates.values(),
                maximum_raw_bytes=manifest.member_page_bytes,
            )
            for page_key in missing_keys:
                if page_key % manifest.members_per_page:
                    raise RuntimeError("PTG V4 audit member page key is unaligned")
                coordinate = coordinates[(page_key, 0)]
                block = blocks[bytes(coordinate.block_hash)]
                if (
                    block.entry_count < 0
                    or block.entry_count > manifest.members_per_page
                    or len(block.payload) != block.entry_count * manifest.member_width
                ):
                    raise RuntimeError("PTG V4 audit member page is malformed")
                members = tuple(
                    member_field[0]
                    for member_field in struct.iter_unpack("<I", block.payload)
                )
                # One physical page may straddle several owners.  Each owner's
                # locator span is ordered, but the next owner may restart at a
                # lower member key.
                self._member_page_cache[(manifest.member_object_kind, page_key)] = members
                members_by_page[page_key] = members
        return members_by_page

    async def _heavy_payloads(
        self,
        manifest: _V4RelationManifest,
        owners_by_key: Mapping[int, _V4HeavyOwner],
    ) -> dict[int, bytes]:
        """Load and authenticate exact heavy bitmaps for selected owners."""

        if not owners_by_key:
            return {}
        for owner in owners_by_key.values():
            encoded_bytes = PTG2_V4_HEAVY_BITMAP_HEADER_BYTES + (owner.member_span + 7) // 8
            if encoded_bytes > PTG2_V3_AUDIT_MAX_BLOCK_BYTES:
                raise RuntimeError("PTG V4 audit heavy bitmap exceeds the byte cap")
        coordinates = await self._map_coordinates(
            object_kind=next(iter(owners_by_key.values())).object_kind,
            coordinate_pairs=(
                (owner.owner_key, fragment_no)
                for owner in owners_by_key.values()
                for fragment_no in range(owner.fragment_count)
            ),
        )
        object_kind = next(iter(owners_by_key.values())).object_kind
        if any(
            owner.object_kind != object_kind for owner in owners_by_key.values()
        ):
            raise RuntimeError("PTG V4 audit heavy owners mix object kinds")
        blocks = await self._physical_blocks(
            object_kind=object_kind,
            coordinates=coordinates.values(),
            maximum_raw_bytes=manifest.member_page_bytes,
        )
        payloads_by_owner: dict[int, bytes] = {}
        for owner_key, owner in owners_by_key.items():
            fragments: list[bytes] = []
            logical_offset = 0
            observed_entry_count = 0
            for fragment_no in range(owner.fragment_count):
                coordinate = coordinates[(owner_key, fragment_no)]
                fragment_payload = blocks[bytes(coordinate.block_hash)].payload
                logical_fragment = _unframe_heavy_bitmap_fragment(
                    fragment_payload,
                    owner=owner,
                    fragment_no=fragment_no,
                    entry_count=int(coordinate.entry_count),
                    logical_offset=logical_offset,
                )
                fragments.append(logical_fragment)
                logical_offset += len(logical_fragment)
                observed_entry_count += int(coordinate.entry_count)
            if observed_entry_count != owner.member_count:
                raise RuntimeError("PTG V4 audit heavy member count changed")
            heavy_payload = b"".join(fragments)
            expected_size = PTG2_V4_HEAVY_BITMAP_HEADER_BYTES + (
                owner.member_span + 7
            ) // 8
            if len(heavy_payload) != expected_size:
                raise RuntimeError("PTG V4 audit heavy bitmap size changed")
            magic, stored_owner, member_base, member_span, member_count = _HEAVY_HEADER.unpack_from(
                heavy_payload
            )
            bitmap = heavy_payload[PTG2_V4_HEAVY_BITMAP_HEADER_BYTES :]
            if (
                magic != PTG2_V4_HEAVY_BITMAP_MAGIC
                or stored_owner != owner.owner_key
                or member_base != owner.member_base
                or member_span != owner.member_span
                or member_count != owner.member_count
                or (
                    owner.member_span % 8
                    and bitmap[-1] & (0xFF << (owner.member_span % 8))
                )
                or sum(byte.bit_count() for byte in bitmap) != owner.member_count
            ):
                raise RuntimeError("PTG V4 audit heavy bitmap is inconsistent")
            payloads_by_owner[owner_key] = heavy_payload
        return payloads_by_owner

    async def single_members(
        self,
        relation: str,
        owner_keys: Iterable[int],
    ) -> dict[int, int]:
        """Return the sole member for scalar relations, failing on cardinality drift."""

        requested_keys = tuple(
            sorted({int(owner_key) for owner_key in owner_keys})
        )
        manifest = await self._manifest(relation)
        heavy = await self._heavy_owners(manifest, requested_keys)
        regular_keys = tuple(
            owner_key for owner_key in requested_keys if owner_key not in heavy
        )
        locators = await self._locators(manifest, regular_keys)
        if any(member_count != 1 for _offset, member_count in locators.values()) or any(
            owner.member_count != 1 for owner in heavy.values()
        ):
            raise RuntimeError("PTG V4 audit scalar relation changed cardinality")
        page_keys = {
            (member_offset // manifest.members_per_page) * manifest.members_per_page
            for member_offset, _member_count in locators.values()
        }
        pages = await self._member_pages(manifest, page_keys)
        members_by_owner: dict[int, int] = {}
        for owner_key, (member_offset, _member_count) in locators.items():
            page_key = (
                member_offset // manifest.members_per_page
            ) * manifest.members_per_page
            local_offset = member_offset - page_key
            page = pages.get(page_key, ())
            if local_offset >= len(page):
                raise RuntimeError("PTG V4 audit scalar locator is truncated")
            self.budget.add_members(1)
            members_by_owner[owner_key] = page[local_offset]
        heavy_payloads = await self._heavy_payloads(manifest, heavy)
        for owner_key, owner in heavy.items():
            bitmap = heavy_payloads[owner_key][PTG2_V4_HEAVY_BITMAP_HEADER_BYTES :]
            set_offsets = [
                byte_index * 8 + bit
                for byte_index, byte in enumerate(bitmap)
                for bit in range(8)
                if byte & (1 << bit)
            ]
            if len(set_offsets) != 1:
                raise RuntimeError("PTG V4 audit scalar bitmap changed cardinality")
            self.budget.add_members(1)
            members_by_owner[owner_key] = owner.member_base + set_offsets[0]
        return members_by_owner

    async def contains_edges(
        self,
        relation: str,
        edges: Iterable[tuple[int, int]],
    ) -> dict[tuple[int, int], bool]:
        """Prove exact edges using bitmap tests or page-bounded binary searches."""

        requested_edges = tuple(
            sorted({(int(owner_key), int(member_key)) for owner_key, member_key in edges})
        )
        if any(
            owner_key < 0
            or owner_key > 0xFFFFFFFF
            or member_key < 0
            or member_key > 0xFFFFFFFF
            for owner_key, member_key in requested_edges
        ):
            raise RuntimeError("PTG V4 audit graph edge is outside uint32 range")
        manifest = await self._manifest(relation)
        owner_keys = {owner_key for owner_key, _member_key in requested_edges}
        heavy = await self._heavy_owners(manifest, owner_keys)
        heavy_payloads = await self._heavy_payloads(manifest, heavy)
        membership_by_edge: dict[tuple[int, int], bool] = {}
        for owner_key, member_key in requested_edges:
            owner = heavy.get(owner_key)
            if owner is None:
                continue
            self.budget.add_members(1)
            offset = member_key - owner.member_base
            if offset < 0 or offset >= owner.member_span:
                membership_by_edge[(owner_key, member_key)] = False
                continue
            bitmap = heavy_payloads[owner_key][PTG2_V4_HEAVY_BITMAP_HEADER_BYTES :]
            membership_by_edge[(owner_key, member_key)] = bool(
                bitmap[offset // 8] & (1 << (offset % 8))
            )

        regular_edges = tuple(
            edge for edge in requested_edges if edge[0] not in heavy
        )
        locators = await self._locators(
            manifest, (owner_key for owner_key, _member_key in regular_edges)
        )
        search_bounds_by_edge: dict[tuple[int, int], list[int]] = {}
        for edge in regular_edges:
            member_offset, member_count = locators[edge[0]]
            if member_count == 0:
                membership_by_edge[edge] = False
            else:
                search_bounds_by_edge[edge] = [
                    member_offset,
                    member_offset + member_count - 1,
                ]
        rounds = 0
        while search_bounds_by_edge:
            rounds += 1
            if rounds > PTG2_V4_AUDIT_MAX_BINARY_SEARCH_ROUNDS:
                raise RuntimeError("PTG V4 audit member search exceeded its round cap")
            middle_by_edge = {
                edge: (bounds[0] + bounds[1]) // 2
                for edge, bounds in search_bounds_by_edge.items()
            }
            page_keys = {
                (middle // manifest.members_per_page) * manifest.members_per_page
                for middle in middle_by_edge.values()
            }
            pages = await self._member_pages(manifest, page_keys)
            completed_edges: list[tuple[int, int]] = []
            for edge, middle in middle_by_edge.items():
                page_key = (
                    middle // manifest.members_per_page
                ) * manifest.members_per_page
                page = pages.get(page_key, ())
                local_offset = middle - page_key
                if local_offset >= len(page):
                    raise RuntimeError("PTG V4 audit member locator is truncated")
                self.budget.add_members(1)
                observed_member = page[local_offset]
                target_member = edge[1]
                if observed_member == target_member:
                    membership_by_edge[edge] = True
                    completed_edges.append(edge)
                elif observed_member < target_member:
                    search_bounds_by_edge[edge][0] = middle + 1
                else:
                    search_bounds_by_edge[edge][1] = middle - 1
                if (
                    search_bounds_by_edge[edge][0]
                    > search_bounds_by_edge[edge][1]
                    and edge not in completed_edges
                ):
                    membership_by_edge[edge] = False
                    completed_edges.append(edge)
            for edge in completed_edges:
                search_bounds_by_edge.pop(edge, None)
        return membership_by_edge


async def _validate_building_v4_context(
    session: Any,
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    representation: str,
) -> None:
    """Lock and verify the exact unsealed V4 root owned by this build."""

    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        db.text(
            f"""
            SELECT layout.snapshot_key, layout.generation, layout.state AS layout_state,
                   layout.build_token, root.state AS root_state,
                   root.format_version, root.map_format, root.representation,
                   root.projection_id_scope, root.map_digest,
                   root.object_kind_count, root.map_pack_count,
                   root.coordinate_count, root.entry_count,
                   root.logical_byte_count, root.stored_map_byte_count,
                   root.npi_count, root.component_count, root.pattern_count,
                   root.relation_count, root.heavy_owner_count,
                   root.completed_at
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = layout.snapshot_key
             WHERE layout.snapshot_key = :snapshot_key
             FOR UPDATE OF layout, root
            """
        ),
        {"snapshot_key": int(build_ownership.snapshot_key)},
    )
    root_rows = [_row_mapping(root_row) for root_row in query_result]
    if len(root_rows) != 1:
        raise RuntimeError("PTG V4 audit building root is missing or duplicated")
    root_row = root_rows[0]
    completion_count_fields = (
        "object_kind_count",
        "map_pack_count",
        "coordinate_count",
        "entry_count",
        "logical_byte_count",
        "stored_map_byte_count",
        "npi_count",
        "component_count",
        "pattern_count",
        "relation_count",
        "heavy_owner_count",
    )
    if (
        int(root_row.get("snapshot_key") or 0)
        != int(build_ownership.snapshot_key)
        or root_row.get("generation") != PTG2_V4_SHARED_GENERATION
        or root_row.get("layout_state") != "building"
        or root_row.get("build_token") != build_ownership.build_token
        or root_row.get("root_state") != "building"
        or int(root_row.get("format_version") or 0)
        != PTG2_V4_MAP_FORMAT_VERSION
        or root_row.get("map_format") != PTG2_V4_MAP_FORMAT
        or root_row.get("representation") != representation
        or root_row.get("projection_id_scope") != PTG2_V4_PROJECTION_ID_SCOPE
        or root_row.get("map_digest") is not None
        or root_row.get("completed_at") is not None
        or any(
            _nonnegative_int(
                root_row.get(field_name), label=f"root {field_name}"
            )
            != 0
            for field_name in completion_count_fields
        )
    ):
        raise RuntimeError("PTG V4 audit lost its exact building-root ownership")


async def _npi_keys_for_values(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    npis: Iterable[int],
) -> dict[int, int]:
    requested_npis = tuple(sorted({int(npi) for npi in npis}))
    if not requested_npis:
        return {}
    schema = _quote_ident(schema_name)
    query_result = await session.execute(
        db.text(
            f"""
            SELECT npi, npi_key
              FROM {schema}.{PTG2_V4_NPI_TABLE}
             WHERE snapshot_key = :snapshot_key
               AND npi = ANY(CAST(:npis AS bigint[]))
             ORDER BY npi
            """
        ),
        {"snapshot_key": int(snapshot_key), "npis": requested_npis},
    )
    by_npi: dict[int, int] = {}
    observed_keys: set[int] = set()
    for raw_row in query_result:
        npi_row = _row_mapping(raw_row)
        npi = _nonnegative_int(
            npi_row.get("npi"), label="NPI", maximum=9_999_999_999
        )
        npi_key = _nonnegative_int(
            npi_row.get("npi_key"),
            label="dense NPI key",
            maximum=0x7FFFFFFF,
        )
        if npi in by_npi or npi_key in observed_keys:
            raise RuntimeError("PTG V4 audit NPI dictionary is ambiguous")
        by_npi[npi] = npi_key
        observed_keys.add(npi_key)
    if set(by_npi) != set(requested_npis):
        raise RuntimeError("PTG V4 audit witness NPI is absent from its dense dictionary")
    return by_npi


async def _verified_provider_npis_by_candidate(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    candidates: Sequence[AuditCandidate],
    witnesses: Mapping[int, V4ProviderSetAuditWitness],
    reader: _V4PersistedGraphReader,
) -> dict[int, tuple[int, ...]]:
    """Prove compiler witnesses through the persisted graph and NPI dictionary."""

    expected_positive_sets = {
        candidate.provider_set_key
        for candidate in candidates
        if candidate.provider_count > 0
    }
    expected_empty_sets = {
        candidate.provider_set_key
        for candidate in candidates
        if candidate.provider_count == 0
    }
    if expected_positive_sets - set(witnesses) or expected_empty_sets & set(witnesses):
        raise RuntimeError("PTG V4 audit witness disagrees with candidate provider counts")
    selected_witnesses_by_set = {
        provider_set_key: witnesses[provider_set_key]
        for provider_set_key in expected_positive_sets
    }
    if reader.representation == "pattern_v1":
        pattern_by_group = await reader.single_members(
            "group_patterns",
            (
                witness.provider_group_key
                for witness in selected_witnesses_by_set.values()
            ),
        )
        set_pattern_edges = {
            (provider_set_key, pattern_by_group[witness.provider_group_key])
            for provider_set_key, witness in selected_witnesses_by_set.items()
        }
        pattern_group_edges = {
            (pattern_by_group[witness.provider_group_key], witness.provider_group_key)
            for witness in selected_witnesses_by_set.values()
        }
        set_pattern_membership = await reader.contains_edges(
            "set_patterns", set_pattern_edges
        )
        pattern_group_membership = await reader.contains_edges(
            "pattern_groups", pattern_group_edges
        )
        if not all(set_pattern_membership.values()) or not all(
            pattern_group_membership.values()
        ):
            raise RuntimeError("PTG V4 audit witness is absent from the pattern graph")
    elif reader.representation == "direct_v1":
        direct_edges = {
            (provider_set_key, witness.provider_group_key)
            for provider_set_key, witness in selected_witnesses_by_set.items()
        }
        direct_membership = await reader.contains_edges(
            "set_groups_direct", direct_edges
        )
        if not all(direct_membership.values()):
            raise RuntimeError("PTG V4 audit witness is absent from the direct graph")
    else:
        raise RuntimeError("PTG V4 audit representation is unsupported")

    npi_key_by_value = await _npi_keys_for_values(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        npis=(witness.npi for witness in selected_witnesses_by_set.values()),
    )
    group_npi_edges = {
        (witness.provider_group_key, npi_key_by_value[witness.npi])
        for witness in selected_witnesses_by_set.values()
    }
    group_npi_membership = await reader.contains_edges(
        "group_npis_exact", group_npi_edges
    )
    if not all(group_npi_membership.values()):
        raise RuntimeError("PTG V4 audit witness NPI is absent from its exact group")
    return {
        candidate.candidate_ordinal: (
            (witnesses[candidate.provider_set_key].npi,)
            if candidate.provider_count > 0
            else ()
        )
        for candidate in candidates
    }


async def publish_v4_audit_sample(
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    logical_snapshot_id: str,
    finalizer_summary: Mapping[str, Any],
    mapping_digest: bytes,
    core_support_digest: bytes,
    atom_key_bits: int,
    price_membership_block_span: int,
    graph_compilation: V4GraphCompilationResult,
) -> SharedAuditPublication:
    """Persist a V3-compatible occurrence sample proved through packed V4 CAS."""

    normalized_mapping_digest = bytes(mapping_digest)
    normalized_core_support = bytes(core_support_digest)
    if len(normalized_mapping_digest) != 32:
        raise ValueError("PTG V4 audit mapping digest must contain 32 bytes")
    if len(normalized_core_support) != 32:
        raise ValueError("PTG V4 audit core support digest must contain 32 bytes")
    try:
        representation = _REPRESENTATION_BY_COMPILER_LAYOUT[
            str(graph_compilation.selected_layout)
        ]
    except KeyError as exc:
        raise RuntimeError("PTG V4 audit compiler layout is unsupported") from exc
    candidates, candidate_summary = load_audit_candidates(finalizer_summary)
    witnesses = load_v4_audit_witnesses(
        graph_compilation,
        provider_set_keys=(candidate.provider_set_key for candidate in candidates),
    )
    source_count = _integer(finalizer_summary.get("source_count"), "source_count")
    core_layout_id = hashlib.sha256(
        _CORE_LAYOUT_DOMAIN + normalized_mapping_digest + normalized_core_support
    ).digest()
    budget = _ReadBudget()
    snapshot_key = int(build_ownership.snapshot_key)
    async with db.transaction() as session:
        await _validate_building_v4_context(
            session,
            schema_name=schema_name,
            build_ownership=build_ownership,
            representation=representation,
        )
        await _validate_candidate_provider_counts(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            candidates=candidates,
        )
        await _validate_snapshot_source_dictionary(
            session,
            schema_name=schema_name,
            logical_snapshot_id=str(logical_snapshot_id),
            source_count=source_count,
            required_source_keys=(candidate.source_key for candidate in candidates),
        )
        price_reader = _BuildingBlockReader(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            budget=budget,
        )
        price_memberships = await _price_memberships(
            price_reader,
            price_keys=(candidate.price_key for candidate in candidates),
            atom_key_bits=int(atom_key_bits),
            block_span=int(price_membership_block_span),
        )
        graph_reader = _V4PersistedGraphReader(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            representation=representation,
            budget=budget,
        )
        provider_npis = await _verified_provider_npis_by_candidate(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            candidates=candidates,
            witnesses=witnesses,
            reader=graph_reader,
        )
        audit_occurrences = build_audit_occurrences(
            candidates=candidates,
            provider_npis=provider_npis,
            price_memberships=price_memberships,
            core_layout_id=core_layout_id,
            budget=budget,
        )
        await _insert_occurrences(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            audit_occurrences=audit_occurrences,
        )
    metadata = _publication_metadata(
        audit_occurrences=audit_occurrences,
        candidates=candidates,
        candidate_summary=candidate_summary,
        source_count=source_count,
        core_layout_id=core_layout_id,
        price_membership_block_span=int(price_membership_block_span),
        budget=budget,
    )
    metadata["provider_selection"] = PTG2_V4_AUDIT_PROVIDER_SELECTION
    metadata["provider_graph_representation"] = representation
    metadata["provider_graph_verification"] = (
        "set_patterns_pattern_groups_group_npis_exact_v1"
        if representation == "pattern_v1"
        else "set_groups_direct_group_npis_exact_v1"
    )
    return SharedAuditPublication(
        metadata=metadata,
        support_digest=shared_support_digest({"audit_sample": metadata}),
        row_count=len(audit_occurrences),
        core_layout_id=core_layout_id,
    )


__all__ = [
    "PTG2_V4_AUDIT_PROVIDER_SELECTION",
    "V4ProviderSetAuditWitness",
    "load_v4_audit_witnesses",
    "publish_v4_audit_sample",
]
