# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed relation selection for a sealed PTG result archive.

This module deliberately only *selects* the rows that a source-clone worker may
copy into an archive schema before invoking ``pg_dump``.  It neither creates a
snapshot nor infers ownership from PostgreSQL OIDs.  In particular, the shared
block table is selected by the content-addressed hashes reached from one sealed
layout, never as a whole relation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_audit import PTG2_V3_AUDIT_MAX_BLOCK_BYTES
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    shared_block_hash,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_MAX_COORDINATES_PER_PACK,
    PTG2_V4_SHARED_GENERATION,
    decode_v4_snapshot_map_pack,
)

RESULT_ARCHIVE_CLOSURE_CONTRACT = "ptg_result_archive_closure_v1"
_MAP_PACK_PAGE_ROWS = 16
_MAP_PACK_BYTES_PER_COORDINATE_CEILING = 64
_MAX_MAP_PACK_PAYLOAD_BYTES = 128 + PTG2_V4_MAX_COORDINATES_PER_PACK * _MAP_PACK_BYTES_PER_COORDINATE_CEILING
_BLOCK_METADATA_PAGE_ROWS = 512
_PAYLOAD_BATCH_BYTES = 64 * 1024 * 1024
_MAX_TARGET_BLOCK_PAYLOAD_BYTES = PTG2_V3_AUDIT_MAX_BLOCK_BYTES
_DEFAULT_MAX_BLOCK_HASHES = 500_000
_SNAPSHOT_STATUS = frozenset(("validated", "published"))
_RELATIONAL_PRICE_OBJECT_KINDS = (
    "price_atoms_v3",
    "price_set_atom_memberships_v3",
)
DecodedMapSelection = tuple[
    tuple[dict[str, Any], ...],
    Mapping[bytes, tuple[str, int]],
]


class ResultArchiveClosureError(RuntimeError):
    """The requested layout cannot be copied as a complete archive closure."""


@dataclass(frozen=True)
class ArchiveRelation:
    """One model relation and a parameterized, exact source-clone predicate."""

    table_name: str
    predicate_sql: str
    purpose: str


@dataclass(frozen=True)
class ResultArchiveClosure:
    """Reviewed model-level archive input; the caller owns pin and dump lifecycle."""

    contract: str
    schema_name: str
    snapshot_id: str
    snapshot_key: int
    layout_generation: str
    layout_mapping_digest: bytes
    map_digest: bytes
    finalizer_map_digest: bytes
    block_hashes: tuple[bytes, ...]
    relations: tuple[ArchiveRelation, ...]
    source_clone_parameters: Mapping[str, Any]
    semantic_metadata: Mapping[str, Any]


def _mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    return dict(getattr(row, "_mapping", row))


def _single(rows: Iterable[Any], label: str) -> dict[str, Any]:
    values = [_mapping(row) for row in rows]
    if len(values) != 1:
        raise ResultArchiveClosureError(f"archive closure {label} is missing or ambiguous")
    return values[0]


def _required_bytes(value: Any, label: str) -> bytes:
    result = bytes(value or b"")
    if len(result) != 32:
        raise ResultArchiveClosureError(f"archive closure {label} is invalid")
    return result


def _required_nonnegative_int(value: Any, label: str) -> int:
    """Return a persisted non-negative integer without treating zero as absent."""

    if value is None or isinstance(value, bool):
        raise ResultArchiveClosureError(f"archive closure {label} is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ResultArchiveClosureError(f"archive closure {label} is invalid") from exc
    if result < 0:
        raise ResultArchiveClosureError(f"archive closure {label} is invalid")
    return result


def _required_pin(retention_pin: Mapping[str, Any] | None) -> str:
    if not isinstance(retention_pin, Mapping):
        raise ResultArchiveClosureError("archive closure requires a caller-held retention pin")
    pin_id = str(retention_pin.get("pin_id") or "").strip()
    read_token = str(retention_pin.get("repeatable_read_token") or "").strip()
    if not pin_id or not read_token:
        raise ResultArchiveClosureError("archive closure requires retention-pin and repeatable-read evidence")
    return pin_id


async def _locked_layout(session: Any, *, schema: str, snapshot_id: str) -> dict[str, Any]:
    layout_query = await session.execute(
        text(
            f"""
            SELECT snapshot.snapshot_id, snapshot.status, snapshot.manifest,
                   binding.snapshot_key, layout.generation, layout.state,
                   layout.mapping_digest, layout.layout_manifest,
                   map_root.state AS map_root_state,
                   map_root.map_format, map_root.map_digest,
                   map_root.object_kind_count AS map_object_kind_count,
                   map_root.map_pack_count AS map_pack_count,
                   map_root.coordinate_count AS map_coordinate_count,
                   map_root.entry_count AS map_entry_count,
                   map_root.logical_byte_count AS map_logical_byte_count,
                   map_root.stored_map_byte_count AS map_stored_map_byte_count,
                   finalizer_root.state AS finalizer_root_state,
                   finalizer_root.contract AS finalizer_contract,
                   finalizer_root.map_format AS finalizer_map_format,
                   finalizer_root.map_digest AS finalizer_map_digest,
                   finalizer_root.object_kind_count AS finalizer_object_kind_count,
                   finalizer_root.map_pack_count AS finalizer_map_pack_count,
                   finalizer_root.coordinate_count AS finalizer_coordinate_count,
                   finalizer_root.entry_count AS finalizer_entry_count,
                   finalizer_root.logical_byte_count AS finalizer_logical_byte_count,
                   finalizer_root.stored_map_byte_count AS finalizer_stored_map_byte_count,
                   finalizer_root.target_block_count AS finalizer_target_block_count
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
              LEFT JOIN {schema}.ptg2_v4_snapshot_map_root AS map_root
                ON map_root.snapshot_key = layout.snapshot_key
              LEFT JOIN {schema}.ptg2_v4_finalizer_map_root AS finalizer_root
                ON finalizer_root.snapshot_key = layout.snapshot_key
             WHERE snapshot.snapshot_id = :snapshot_id
             FOR KEY SHARE OF snapshot, binding, layout
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    return _single(layout_query, "snapshot layout")


def _validate_layout(layout_by_field: Mapping[str, Any]) -> tuple[int, bytes, bytes, bytes]:
    if str(layout_by_field.get("status") or "").strip().lower() not in _SNAPSHOT_STATUS:
        raise ResultArchiveClosureError("archive closure requires a completed PTG snapshot")
    if str(layout_by_field.get("state") or "").strip().lower() != "sealed":
        raise ResultArchiveClosureError("archive closure requires a sealed layout")
    if str(layout_by_field.get("generation") or "").strip().lower() != PTG2_V4_SHARED_GENERATION:
        raise ResultArchiveClosureError("archive closure does not support this layout generation")
    if str(layout_by_field.get("map_root_state") or "").strip().lower() != "complete":
        raise ResultArchiveClosureError("archive closure is missing its completed map root")
    if layout_by_field.get("map_format") != PTG2_V4_MAP_FORMAT:
        raise ResultArchiveClosureError("archive closure map root has an unknown format")
    if str(layout_by_field.get("finalizer_root_state") or "").strip().lower() != "complete":
        raise ResultArchiveClosureError("archive closure is missing its completed finalizer root")
    if (
        layout_by_field.get("finalizer_contract") != PTG2_V4_FINALIZER_MAP_CONTRACT
        or layout_by_field.get("finalizer_map_format") != PTG2_V4_MAP_FORMAT
    ):
        raise ResultArchiveClosureError("archive closure finalizer root has an unknown contract")
    manifest = layout_by_field.get("manifest")
    layout_manifest = layout_by_field.get("layout_manifest")
    if not isinstance(manifest, Mapping) or not isinstance(layout_manifest, Mapping):
        raise ResultArchiveClosureError("archive closure is missing semantic metadata")
    serving = manifest.get("serving_index")
    layout_serving = layout_manifest.get("serving_index")
    if not isinstance(serving, Mapping) or not isinstance(layout_serving, Mapping):
        raise ResultArchiveClosureError("archive closure has no serving-index metadata")
    snapshot_key = int(layout_by_field.get("snapshot_key"))
    if (
        serving.get("storage_generation") != PTG2_V4_SHARED_GENERATION
        or layout_serving.get("storage_generation") != PTG2_V4_SHARED_GENERATION
        or int(serving.get("shared_snapshot_key", -1)) != snapshot_key
        or int(layout_serving.get("shared_snapshot_key", -1)) != snapshot_key
    ):
        raise ResultArchiveClosureError("archive closure serving metadata does not bind this layout")
    return (
        snapshot_key,
        _required_bytes(layout_by_field.get("mapping_digest"), "layout mapping digest"),
        _required_bytes(layout_by_field.get("map_digest"), "map digest"),
        _required_bytes(layout_by_field.get("finalizer_map_digest"), "finalizer map digest"),
    )


async def _load_map_blocks(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    table_name: str,
    max_block_hashes: int,
    closure_block_hashes: set[bytes],
) -> tuple[tuple[dict[str, Any], ...], dict[bytes, tuple[str, int]]]:
    """Page one map table and decode every CAS-validated coordinate payload."""

    map_packs: list[dict[str, Any]] = []
    target_identity_by_hash: dict[bytes, tuple[str, int]] = {}
    after_kind = ""
    after_pack = -1
    while True:
        map_pack_records = await _map_pack_page(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            table_name=table_name,
            after_kind=after_kind,
            after_pack=after_pack,
        )
        if not map_pack_records:
            break
        for map_pack_by_field in map_pack_records:
            new_target_hashes = _append_map_pack_targets(
                map_pack_by_field,
                target_identity_by_hash=target_identity_by_hash,
            )
            closure_block_hashes.add(bytes(map_pack_by_field.get("map_block_hash") or b""))
            closure_block_hashes.update(new_target_hashes)
            if len(closure_block_hashes) > max_block_hashes:
                raise ResultArchiveClosureError("archive closure block reachability exceeds its declared bound")
            map_packs.append(_map_pack_metadata(map_pack_by_field))
        after_kind, after_pack = (
            str(map_pack_records[-1]["object_kind"]),
            int(map_pack_records[-1]["pack_no"]),
        )
        if len(map_pack_records) < _MAP_PACK_PAGE_ROWS:
            break
    if not map_packs:
        raise ResultArchiveClosureError("archive closure has no map packs")
    return tuple(map_packs), target_identity_by_hash


async def _map_pack_page(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    table_name: str,
    after_kind: str,
    after_pack: int,
) -> tuple[dict[str, Any], ...]:
    """Load one bounded page of persisted map-pack blocks."""

    query = await session.execute(
        text(
            f"""
            SELECT pack.object_kind, pack.pack_no, pack.coordinate_count,
                   pack.entry_count, pack.logical_byte_count, pack.map_block_hash,
                   block.format_version, block.object_kind AS map_object_kind,
                   block.codec, block.entry_count AS map_entry_count,
                   block.raw_byte_count, block.stored_byte_count, block.payload
              FROM {schema}.{_quote_ident(table_name)} AS pack
              JOIN {schema}.ptg2_v3_block AS block ON block.block_hash = pack.map_block_hash
             WHERE pack.snapshot_key = :snapshot_key
               AND block.raw_byte_count <= :max_map_payload_bytes
               AND block.stored_byte_count <= :max_map_payload_bytes
               AND octet_length(block.payload) = block.stored_byte_count
               AND (pack.object_kind, pack.pack_no) > (:after_kind, :after_pack)
             ORDER BY pack.object_kind, pack.pack_no LIMIT :page_rows
            """
        ),
        {
            "snapshot_key": snapshot_key,
            "after_kind": after_kind,
            "after_pack": after_pack,
            "max_map_payload_bytes": _MAX_MAP_PACK_PAYLOAD_BYTES,
            "page_rows": _MAP_PACK_PAGE_ROWS,
        },
    )
    map_pack_records = tuple(_mapping(query_row) for query_row in query)
    if len(map_pack_records) > _MAP_PACK_PAGE_ROWS:
        raise ResultArchiveClosureError("archive closure map page exceeded its bound")
    return map_pack_records


def _append_map_pack_targets(
    map_pack_by_field: Mapping[str, Any],
    *,
    target_identity_by_hash: dict[bytes, tuple[str, int]],
) -> set[bytes]:
    """Decode one stored map block and extend its exact target identity set."""

    if (
        int(map_pack_by_field.get("format_version") or -1) != PTG2_V3_SHARED_FORMAT_VERSION
        or map_pack_by_field.get("map_object_kind") != PTG2_V4_MAP_BLOCK_KIND
        or map_pack_by_field.get("codec") != "none"
        or int(map_pack_by_field.get("raw_byte_count") or -1) != int(map_pack_by_field.get("stored_byte_count") or -2)
    ):
        raise ResultArchiveClosureError("archive closure map pack block is invalid")
    map_payload = bytes(map_pack_by_field.get("payload") or b"")
    if bytes(map_pack_by_field.get("map_block_hash") or b"") != shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        codec="none",
        payload=map_payload,
    ):
        raise ResultArchiveClosureError("archive closure map pack hash is inconsistent")
    try:
        coordinates = decode_v4_snapshot_map_pack(
            map_payload,
            expected_object_kind=str(map_pack_by_field.get("object_kind") or ""),
        )
    except ValueError as exc:
        raise ResultArchiveClosureError("archive closure map pack cannot be decoded") from exc
    if len(coordinates) != int(map_pack_by_field.get("coordinate_count") or -1) or sum(
        coordinate.entry_count for coordinate in coordinates
    ) != int(map_pack_by_field.get("entry_count") or -1):
        raise ResultArchiveClosureError("archive closure map pack geometry is inconsistent")
    new_target_hashes: set[bytes] = set()
    for coordinate in coordinates:
        block_hash = bytes(coordinate.block_hash)
        target_identity = (coordinate.object_kind, int(coordinate.entry_count))
        is_new_identity = block_hash not in target_identity_by_hash
        _record_target_identity(
            target_identity_by_hash,
            block_hash=block_hash,
            target_identity=target_identity,
        )
        if is_new_identity:
            new_target_hashes.add(block_hash)
    return new_target_hashes


def _record_target_identity(
    target_identity_by_hash: dict[bytes, tuple[str, int]],
    *,
    block_hash: bytes,
    target_identity: tuple[str, int],
) -> None:
    """Record one exact block identity and reject conflicting reachability."""

    previous_identity = target_identity_by_hash.get(block_hash)
    if previous_identity is not None and previous_identity != target_identity:
        raise ResultArchiveClosureError("archive closure maps one block hash to conflicting identities")
    if previous_identity is None:
        target_identity_by_hash[block_hash] = target_identity


def _map_pack_metadata(map_pack_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Discard decoded payload bytes after preserving root-receipt fields."""

    return {field_name: value for field_name, value in map_pack_by_field.items() if field_name != "payload"}


async def _validate_target_blocks(
    session: Any,
    *,
    schema: str,
    target_hashes: set[bytes],
    coordinate_identity: Mapping[bytes, tuple[str, int]],
    max_block_hashes: int,
) -> None:
    """Validate bounded target blocks through metadata then byte-limited reads.

    Metadata pages prove the complete requested hash set and let the selector
    reject impossible payload sizes before ``bytea`` values are exposed to
    Python.  Payload batches are a fixed upper bound, except for one legal
    native block whose published maximum is larger than the normal batch.
    """

    if not target_hashes or len(target_hashes) > max_block_hashes:
        raise ResultArchiveClosureError("archive closure block reachability exceeds its declared bound")
    metadata_by_hash = await _load_target_block_metadata(
        session,
        schema=schema,
        target_hashes=target_hashes,
        coordinate_identity=coordinate_identity,
    )
    if set(metadata_by_hash) != target_hashes:
        raise ResultArchiveClosureError("archive closure has dangling map targets")
    for payload_hashes in _payload_hash_batches(metadata_by_hash):
        await _validate_payload_batch(
            session,
            schema=schema,
            payload_hashes=payload_hashes,
            metadata_by_hash=metadata_by_hash,
        )


async def _load_relational_mapping_blocks(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    max_block_hashes: int,
    closure_block_hashes: set[bytes],
) -> dict[bytes, tuple[str, int]]:
    """Page exact snapshot mappings and retain their CAS identity constraints."""

    identity_by_hash: dict[bytes, tuple[str, int]] = {}
    after_object_kind = ""
    after_block_key = -1
    after_fragment_no = -1
    while True:
        mapping_records = await _relational_mapping_page(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            after_object_kind=after_object_kind,
            after_block_key=after_block_key,
            after_fragment_no=after_fragment_no,
        )
        if not mapping_records:
            return identity_by_hash
        for mapping_by_field in mapping_records:
            block_hash = _required_bytes(
                mapping_by_field.get("block_hash"),
                "relational mapping block hash",
            )
            object_kind = str(mapping_by_field.get("object_kind") or "")
            entry_count = _required_nonnegative_int(
                mapping_by_field.get("entry_count"),
                "relational mapping entry count",
            )
            if not object_kind:
                raise ResultArchiveClosureError("archive closure relational mapping is invalid")
            _record_target_identity(
                identity_by_hash,
                block_hash=block_hash,
                target_identity=(object_kind, entry_count),
            )
            closure_block_hashes.add(block_hash)
            if len(closure_block_hashes) > max_block_hashes:
                raise ResultArchiveClosureError("archive closure block reachability exceeds its declared bound")
        after_object_kind = str(mapping_records[-1]["object_kind"])
        after_block_key = _required_nonnegative_int(
            mapping_records[-1].get("block_key"),
            "relational mapping block key",
        )
        after_fragment_no = _required_nonnegative_int(
            mapping_records[-1].get("fragment_no"),
            "relational mapping fragment number",
        )
        if len(mapping_records) < _BLOCK_METADATA_PAGE_ROWS:
            return identity_by_hash


async def _relational_mapping_page(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    after_object_kind: str,
    after_block_key: int,
    after_fragment_no: int,
) -> tuple[dict[str, Any], ...]:
    """Read one fixed-size page of the two V4 relational price map kinds."""

    query = await session.execute(
        text(
            f"""
            SELECT object_kind, block_key, fragment_no, entry_count, block_hash
              FROM {schema}.ptg2_v3_snapshot_block
             WHERE snapshot_key = :snapshot_key
               AND object_kind = ANY(CAST(:object_kinds AS text[]))
               AND (object_kind, block_key, fragment_no)
                   > (:after_object_kind, :after_block_key, :after_fragment_no)
             ORDER BY object_kind, block_key, fragment_no
             LIMIT :page_rows
            """
        ),
        {
            "snapshot_key": snapshot_key,
            "object_kinds": _RELATIONAL_PRICE_OBJECT_KINDS,
            "after_object_kind": after_object_kind,
            "after_block_key": after_block_key,
            "after_fragment_no": after_fragment_no,
            "page_rows": _BLOCK_METADATA_PAGE_ROWS,
        },
    )
    return tuple(_mapping(mapping_row) for mapping_row in query)


async def _load_target_block_metadata(
    session: Any,
    *,
    schema: str,
    target_hashes: set[bytes],
    coordinate_identity: Mapping[bytes, tuple[str, int]],
) -> dict[bytes, dict[str, Any]]:
    """Read and validate fixed-size pages of target block metadata only."""

    metadata_by_hash: dict[bytes, dict[str, Any]] = {}
    ordered_hashes = tuple(sorted(target_hashes))
    for start in range(0, len(ordered_hashes), _BLOCK_METADATA_PAGE_ROWS):
        page_hashes = ordered_hashes[start : start + _BLOCK_METADATA_PAGE_ROWS]
        query = await session.execute(
            text(
                f"""
                SELECT block_hash, format_version, object_kind, codec, entry_count,
                       raw_byte_count, stored_byte_count
                  FROM {schema}.ptg2_v3_block
                 WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
                """
            ),
            {"block_hashes": page_hashes},
        )
        for raw in query:
            block = _mapping(raw)
            block_hash = bytes(block.get("block_hash") or b"")
            metadata_by_hash[block_hash] = _validated_target_metadata(
                block,
                coordinate_identity=coordinate_identity,
            )
    return metadata_by_hash


def _validated_target_metadata(
    block_by_field: Mapping[str, Any],
    *,
    coordinate_identity: Mapping[bytes, tuple[str, int]],
) -> dict[str, Any]:
    """Check one persisted target metadata row before requesting its payload."""

    block_hash = bytes(block_by_field.get("block_hash") or b"")
    entry_count = _required_nonnegative_int(block_by_field.get("entry_count"), "target entry count")
    raw_byte_count = _required_nonnegative_int(block_by_field.get("raw_byte_count"), "target raw byte count")
    stored_byte_count = _required_nonnegative_int(block_by_field.get("stored_byte_count"), "target stored byte count")
    if (
        len(block_hash) != 32
        or _required_nonnegative_int(block_by_field.get("format_version"), "target format")
        != PTG2_V3_SHARED_FORMAT_VERSION
        or str(block_by_field.get("object_kind") or "") == ""
        or block_by_field.get("codec") not in ("none", "zlib")
        or stored_byte_count > _MAX_TARGET_BLOCK_PAYLOAD_BYTES
        or (block_by_field.get("codec") == "none" and raw_byte_count != stored_byte_count)
    ):
        raise ResultArchiveClosureError("archive closure target block is invalid")
    expected_identity = coordinate_identity.get(block_hash)
    if expected_identity is not None and (
        block_by_field.get("object_kind") != expected_identity[0] or entry_count != expected_identity[1]
    ):
        raise ResultArchiveClosureError("archive closure decoded target identity is inconsistent")
    return dict(block_by_field)


def _payload_hash_batches(
    metadata_by_hash: Mapping[bytes, Mapping[str, Any]],
) -> tuple[tuple[bytes, ...], ...]:
    """Partition metadata-validated hashes into bounded native payload reads."""

    batches: list[tuple[bytes, ...]] = []
    batch_hashes: list[bytes] = []
    batch_bytes = 0
    for block_hash in sorted(metadata_by_hash):
        stored_bytes = _required_nonnegative_int(
            metadata_by_hash[block_hash].get("stored_byte_count"),
            "target stored byte count",
        )
        if stored_bytes > _MAX_TARGET_BLOCK_PAYLOAD_BYTES:
            raise ResultArchiveClosureError("archive closure target block exceeds native byte limit")
        if stored_bytes > _PAYLOAD_BATCH_BYTES:
            if batch_hashes:
                batches.append(tuple(batch_hashes))
                batch_hashes = []
                batch_bytes = 0
            batches.append((block_hash,))
            continue
        if batch_hashes and (
            len(batch_hashes) == _BLOCK_METADATA_PAGE_ROWS or batch_bytes + stored_bytes > _PAYLOAD_BATCH_BYTES
        ):
            batches.append(tuple(batch_hashes))
            batch_hashes = []
            batch_bytes = 0
        batch_hashes.append(block_hash)
        batch_bytes += stored_bytes
    if batch_hashes:
        batches.append(tuple(batch_hashes))
    return tuple(batches)


async def _validate_payload_batch(
    session: Any,
    *,
    schema: str,
    payload_hashes: tuple[bytes, ...],
    metadata_by_hash: Mapping[bytes, Mapping[str, Any]],
) -> None:
    """Read one byte-limited batch and authenticate every exact CAS payload."""

    expected_sizes = tuple(
        _required_nonnegative_int(
            metadata_by_hash[block_hash].get("stored_byte_count"),
            "target stored byte count",
        )
        for block_hash in payload_hashes
    )
    batch_bytes = sum(expected_sizes)
    if (
        not payload_hashes
        or len(payload_hashes) > _BLOCK_METADATA_PAGE_ROWS
        or batch_bytes > _MAX_TARGET_BLOCK_PAYLOAD_BYTES
        or (len(payload_hashes) > 1 and batch_bytes > _PAYLOAD_BATCH_BYTES)
    ):
        raise ResultArchiveClosureError("archive closure target payload batch exceeds its bound")
    payload_by_hash = await _load_payload_batch(
        session,
        schema=schema,
        payload_hashes=payload_hashes,
        expected_sizes=expected_sizes,
    )
    if set(payload_by_hash) != set(payload_hashes):
        raise ResultArchiveClosureError("archive closure target payload length is inconsistent")
    _authenticate_payload_batch(
        payload_hashes,
        payload_by_hash=payload_by_hash,
        metadata_by_hash=metadata_by_hash,
    )


async def _load_payload_batch(
    session: Any,
    *,
    schema: str,
    payload_hashes: tuple[bytes, ...],
    expected_sizes: tuple[int, ...],
) -> dict[bytes, dict[str, Any]]:
    """Fetch one size-checked native payload batch with no table-wide scan."""

    query = await session.execute(
        text(
            f"""
            WITH requested AS (
                SELECT *
                  FROM unnest(
                      CAST(:block_hashes AS bytea[]),
                      CAST(:stored_byte_counts AS bigint[])
                  ) AS requested(block_hash, stored_byte_count)
            )
            SELECT block.block_hash, block.payload,
                   octet_length(block.payload) AS payload_byte_count
              FROM {schema}.ptg2_v3_block AS block
              JOIN requested
                ON requested.block_hash = block.block_hash
               AND requested.stored_byte_count = block.stored_byte_count
             WHERE octet_length(block.payload) = block.stored_byte_count
            """
        ),
        {
            "block_hashes": payload_hashes,
            "stored_byte_counts": expected_sizes,
        },
    )
    payload_by_hash = {
        bytes(block_by_field.get("block_hash") or b""): block_by_field
        for raw in query
        if (block_by_field := _mapping(raw))
    }
    return payload_by_hash


def _authenticate_payload_batch(
    payload_hashes: tuple[bytes, ...],
    *,
    payload_by_hash: Mapping[bytes, Mapping[str, Any]],
    metadata_by_hash: Mapping[bytes, Mapping[str, Any]],
) -> None:
    """Verify exact byte lengths and CAS identity after the bounded native read."""

    for block_hash in payload_hashes:
        payload_by_field = payload_by_hash[block_hash]
        block_payload = bytes(payload_by_field.get("payload") or b"")
        if (
            len(block_payload)
            != _required_nonnegative_int(payload_by_field.get("payload_byte_count"), "target payload length")
            or len(block_payload) != metadata_by_hash[block_hash]["stored_byte_count"]
        ):
            raise ResultArchiveClosureError("archive closure target payload length is inconsistent")
        metadata = metadata_by_hash[block_hash]
        if block_hash != shared_block_hash(
            format_version=PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind=str(metadata.get("object_kind") or ""),
            codec=str(metadata.get("codec") or ""),
            payload=block_payload,
        ):
            raise ResultArchiveClosureError("archive closure target block hash is inconsistent")


async def _validate_finalizer_targets(
    session: Any, *, schema: str, snapshot_key: int, decoded_targets: set[bytes]
) -> None:
    anchor_limit = len(decoded_targets) + 1
    query = await session.execute(
        text(
            f"""
            SELECT block_hash FROM {schema}.ptg2_v4_finalizer_map_target
             WHERE snapshot_key = :snapshot_key
             ORDER BY block_hash
             LIMIT :anchor_limit
            """
        ),
        {"snapshot_key": snapshot_key, "anchor_limit": anchor_limit},
    )
    anchored_targets = {bytes(_mapping(row).get("block_hash") or b"") for row in query}
    if not anchored_targets or anchored_targets != decoded_targets:
        raise ResultArchiveClosureError("archive closure finalizer targets are incomplete")


def _validate_root_geometry(
    root_by_field: Mapping[str, Any],
    *,
    prefix: str,
    packs: tuple[dict[str, Any], ...],
    target_count: int | None = None,
) -> None:
    """Match every bounded decoded pack aggregate to its immutable root receipt."""

    expected_by_field = {
        "object_kind_count": len({str(pack["object_kind"]) for pack in packs}),
        "map_pack_count": len(packs),
        "coordinate_count": sum(int(pack["coordinate_count"]) for pack in packs),
        "entry_count": sum(int(pack["entry_count"]) for pack in packs),
        "logical_byte_count": sum(int(pack["logical_byte_count"]) for pack in packs),
        "stored_map_byte_count": sum(int(pack["stored_byte_count"]) for pack in packs),
    }
    for field_name, observed_count in expected_by_field.items():
        root_field = field_name if prefix == "map" and field_name.startswith("map_") else f"{prefix}_{field_name}"
        if int(root_by_field.get(root_field) or -1) != observed_count:
            raise ResultArchiveClosureError(f"archive closure {prefix} root disagrees with decoded map packs")
    if target_count is not None and int(root_by_field.get(f"{prefix}_target_block_count") or -1) != target_count:
        raise ResultArchiveClosureError("archive closure finalizer root disagrees with decoded targets")


def _relations(schema_name: str) -> tuple[ArchiveRelation, ...]:
    """Return model selections with dependent subqueries bound to one schema."""

    schema = _quote_ident(schema_name)
    snapshot = "snapshot_id = :snapshot_id"
    key = "snapshot_key = :snapshot_key"
    snapshot_table = f"{schema}.ptg2_snapshot"
    source_table = f"{schema}.ptg2_v3_snapshot_source"
    trace_set_table = f"{schema}.ptg2_source_trace_set"
    trace_table = f"{schema}.ptg2_source_trace"
    file_version_table = f"{schema}.ptg2_source_file_version"
    artifact_table = f"{schema}.ptg2_artifact_manifest"
    import_run = f"import_run_id IN (SELECT import_run_id FROM {snapshot_table} WHERE snapshot_id = :snapshot_id)"
    return (
        _snapshot_relations(snapshot, key, import_run, artifact_table)
        + _source_evidence_relations(
            source_table,
            trace_set_table,
            trace_table,
            file_version_table,
        )
        + _layout_relations(key)
    )


def _snapshot_relations(
    snapshot: str,
    key: str,
    import_run: str,
    artifact_table: str,
) -> tuple[ArchiveRelation, ...]:
    """Return snapshot, import, and artifact rows selected by native identity."""

    return (
        ArchiveRelation("ptg2_snapshot", snapshot, "logical snapshot identity"),
        ArchiveRelation("ptg2_import_run", import_run, "existing dependent import run"),
        ArchiveRelation("ptg2_import_job", import_run, "existing dependent import jobs"),
        ArchiveRelation("ptg2_source_catalog", import_run, "existing dependent source catalog"),
        ArchiveRelation("ptg2_v3_snapshot_binding", snapshot, "snapshot-to-layout binding"),
        ArchiveRelation("ptg2_v3_snapshot_scope", snapshot, "coverage scope"),
        ArchiveRelation("ptg2_v3_snapshot_plan_scope", snapshot, "plan scope"),
        ArchiveRelation("ptg2_v3_snapshot_source", snapshot, "sealed source assignments"),
        ArchiveRelation("ptg2_v3_candidate_audit_attestation", snapshot, "candidate audit"),
        ArchiveRelation("ptg2_v3_audit_occurrence", key, "audit occurrence evidence"),
        ArchiveRelation("ptg2_artifact_manifest", snapshot, "snapshot artifacts"),
        ArchiveRelation(
            "ptg2_artifact_blob_chunk",
            f"artifact_id IN (SELECT artifact_id FROM {artifact_table} WHERE snapshot_id = :snapshot_id)",
            "artifact payloads",
        ),
    )


def _source_evidence_relations(
    source_table: str,
    trace_set_table: str,
    trace_table: str,
    file_version_table: str,
) -> tuple[ArchiveRelation, ...]:
    """Return source trace, file, and identity dependencies of one snapshot."""

    return (
        ArchiveRelation(
            "ptg2_source_trace_set",
            f"source_trace_set_hash IN (SELECT source_trace_set_hash FROM {source_table} WHERE snapshot_id = :snapshot_id)",
            "source trace sets",
        ),
        ArchiveRelation(
            "ptg2_source_trace",
            f"source_trace_hash IN (SELECT unnest(source_trace_hashes) FROM {trace_set_table} WHERE source_trace_set_hash IN (SELECT source_trace_set_hash FROM {source_table} WHERE snapshot_id = :snapshot_id))",
            "source traces",
        ),
        ArchiveRelation(
            "ptg2_source_file_version",
            f"source_file_version_id IN (SELECT source_file_version_id FROM {trace_table} WHERE source_trace_hash IN (SELECT unnest(source_trace_hashes) FROM {trace_set_table} WHERE source_trace_set_hash IN (SELECT source_trace_set_hash FROM {source_table} WHERE snapshot_id = :snapshot_id)))",
            "source file evidence",
        ),
        ArchiveRelation(
            "ptg2_source_identity",
            f"source_identity_hash IN (SELECT source_identity_hash FROM {file_version_table} WHERE source_file_version_id IN (SELECT source_file_version_id FROM {trace_table} WHERE source_trace_hash IN (SELECT unnest(source_trace_hashes) FROM {trace_set_table} WHERE source_trace_set_hash IN (SELECT source_trace_set_hash FROM {source_table} WHERE snapshot_id = :snapshot_id))))",
            "source identity evidence",
        ),
        ArchiveRelation(
            "ptg2_content_identity",
            f"content_hash IN (SELECT content_hash FROM {file_version_table} WHERE source_file_version_id IN (SELECT source_file_version_id FROM {trace_table} WHERE source_trace_hash IN (SELECT unnest(source_trace_hashes) FROM {trace_set_table} WHERE source_trace_set_hash IN (SELECT source_trace_set_hash FROM {source_table} WHERE snapshot_id = :snapshot_id))))",
            "source content identity evidence",
        ),
    )


def _layout_relations(key: str) -> tuple[ArchiveRelation, ...]:
    """Return sealed layout, map, dictionary, audit, and exact block selections."""

    return (
        ArchiveRelation("ptg2_v3_snapshot_layout", key, "sealed layout"),
        ArchiveRelation("ptg2_v3_layout_fingerprint", key, "sealed layout fingerprint"),
        ArchiveRelation(
            "ptg2_v3_snapshot_block",
            f"{key} AND object_kind IN ('price_atoms_v3', 'price_set_atom_memberships_v3')",
            "relational price mapping anchors",
        ),
        ArchiveRelation("ptg2_v4_snapshot_map_root", key, "map root"),
        ArchiveRelation("ptg2_v4_snapshot_map_pack", key, "map packs"),
        ArchiveRelation("ptg2_v4_finalizer_map_root", key, "finalizer root"),
        ArchiveRelation("ptg2_v4_finalizer_map_pack", key, "finalizer packs"),
        ArchiveRelation("ptg2_v4_finalizer_map_target", key, "finalizer targets"),
        *(
            ArchiveRelation(table, key, "sealed V4 dictionary or metadata")
            for table in (
                "ptg2_v4_npi_scope",
                "ptg2_v3_provider_group",
                "ptg2_v4_provider_component",
                "ptg2_v4_pattern",
                "ptg2_v4_relation_manifest",
                "ptg2_v4_heavy_owner",
                "ptg2_v4_provider_set_npi_prefix",
                "ptg2_v4_provider_graph_diagnostic",
                "ptg2_v4_inferred_taxonomy_candidate",
                "ptg2_v3_source_audit_witness",
                "ptg2_v3_source_audit_witness_part",
                "ptg2_provider_tax_identity_manifest",
                "ptg2_provider_tax_identity",
                "ptg2_provider_group_tax_identity",
                "ptg2_provider_tax_identity_source_manifest",
                "ptg2_provider_tax_identity_source_binding",
                "ptg2_provider_group_tax_identity_source",
            )
        ),
        ArchiveRelation(
            "ptg2_v3_block",
            "block_hash = ANY(CAST(:block_hashes AS bytea[]))",
            "CAS-validated map and finalizer blocks",
        ),
    )


async def select_result_archive_closure(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    retention_pin: Mapping[str, Any] | None,
    max_block_hashes: int = _DEFAULT_MAX_BLOCK_HASHES,
) -> ResultArchiveClosure:
    """Return one bounded, native-PG-copy selection for a sealed V4 PTG layout.

    ``retention_pin`` is caller evidence, not a database mutation: it must carry
    non-empty ``pin_id`` and ``repeatable_read_token`` while the caller keeps the
    associated retention pin alive through source clone and dump completion.
    The caller must invoke this inside the same repeatable-read source snapshot
    used for source clone, and must re-check its native current evidence before
    restoring or promoting the resulting dump.  Pin IDs are deliberately not
    treated as database identity and are never queried here.
    """

    pin_id = _required_pin(retention_pin)
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id:
        raise ValueError("snapshot_id is required")
    if isinstance(max_block_hashes, bool) or int(max_block_hashes) <= 0:
        raise ValueError("max_block_hashes must be positive")
    schema = _quote_ident(schema_name)
    layout_by_field = await _locked_layout(session, schema=schema, snapshot_id=normalized_snapshot_id)
    snapshot_key, layout_digest, map_digest, finalizer_digest = _validate_layout(layout_by_field)
    archive_blocks = await _archive_block_selection(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        layout_by_field=layout_by_field,
        max_block_hashes=int(max_block_hashes),
    )
    block_hashes, map_pack_count, finalizer_map_pack_count = archive_blocks
    return _closure_result(
        schema_name=schema_name,
        snapshot_id=normalized_snapshot_id,
        pin_id=pin_id,
        layout_values=(snapshot_key, layout_digest, map_digest, finalizer_digest),
        archive_blocks=(block_hashes, map_pack_count, finalizer_map_pack_count),
    )


def _closure_result(
    *,
    schema_name: str,
    snapshot_id: str,
    pin_id: str,
    layout_values: tuple[int, bytes, bytes, bytes],
    archive_blocks: tuple[tuple[bytes, ...], int, int],
) -> ResultArchiveClosure:
    """Build immutable source-clone metadata from fully validated layout evidence."""

    snapshot_key, layout_digest, map_digest, finalizer_digest = layout_values
    block_hashes, map_pack_count, finalizer_map_pack_count = archive_blocks
    return ResultArchiveClosure(
        contract=RESULT_ARCHIVE_CLOSURE_CONTRACT,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        snapshot_key=snapshot_key,
        layout_generation=PTG2_V4_SHARED_GENERATION,
        layout_mapping_digest=layout_digest,
        map_digest=map_digest,
        finalizer_map_digest=finalizer_digest,
        block_hashes=block_hashes,
        relations=_relations(schema_name),
        source_clone_parameters={
            "snapshot_id": snapshot_id,
            "snapshot_key": snapshot_key,
            "block_hashes": block_hashes,
        },
        semantic_metadata={
            "retention_pin_id": pin_id,
            "source_snapshot_id": snapshot_id,
            "shared_snapshot_key": snapshot_key,
            "storage_generation": PTG2_V4_SHARED_GENERATION,
            "map_pack_count": map_pack_count,
            "finalizer_map_pack_count": finalizer_map_pack_count,
            "block_count": len(block_hashes),
            "source_clone": "copy selected model rows into an isolated archive schema before pg_dump",
        },
    )


async def _archive_block_selection(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    layout_by_field: Mapping[str, Any],
    max_block_hashes: int,
) -> tuple[tuple[bytes, ...], int, int]:
    """Decode and validate all bounded map reachability for one sealed layout."""

    closure_block_hashes: set[bytes] = set()
    map_packs, map_target_identity_by_hash = await _load_map_blocks(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        table_name="ptg2_v4_snapshot_map_pack",
        max_block_hashes=max_block_hashes,
        closure_block_hashes=closure_block_hashes,
    )
    finalizer_packs, finalizer_target_identity_by_hash = await _load_map_blocks(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        table_name="ptg2_v4_finalizer_map_pack",
        max_block_hashes=max_block_hashes,
        closure_block_hashes=closure_block_hashes,
    )
    relational_target_identity_by_hash = await _load_relational_mapping_blocks(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        max_block_hashes=max_block_hashes,
        closure_block_hashes=closure_block_hashes,
    )
    target_identity_by_hash = await _validate_decoded_map_selection(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        layout_by_field=layout_by_field,
        map_selection=(map_packs, map_target_identity_by_hash),
        finalizer_selection=(finalizer_packs, finalizer_target_identity_by_hash),
        relational_target_identity_by_hash=relational_target_identity_by_hash,
    )
    block_hashes = closure_block_hashes
    expected_hashes = {bytes(map_pack["map_block_hash"]) for map_pack in map_packs + finalizer_packs}
    expected_hashes.update(target_identity_by_hash)
    if set(block_hashes) != expected_hashes:
        raise ResultArchiveClosureError("archive closure decoded block reachability is inconsistent")
    await _validate_target_blocks(
        session,
        schema=schema,
        target_hashes=block_hashes,
        coordinate_identity=target_identity_by_hash,
        max_block_hashes=max_block_hashes,
    )
    return tuple(sorted(block_hashes)), len(map_packs), len(finalizer_packs)


async def _validate_decoded_map_selection(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    layout_by_field: Mapping[str, Any],
    map_selection: DecodedMapSelection,
    finalizer_selection: DecodedMapSelection,
    relational_target_identity_by_hash: Mapping[bytes, tuple[str, int]],
) -> dict[bytes, tuple[str, int]]:
    """Validate map receipts and return one conflict-free target identity map."""

    map_packs, map_target_identity_by_hash = map_selection
    finalizer_packs, finalizer_target_identity_by_hash = finalizer_selection
    _validate_root_geometry(layout_by_field, prefix="map", packs=map_packs)
    _validate_root_geometry(
        layout_by_field, prefix="finalizer", packs=finalizer_packs, target_count=len(finalizer_target_identity_by_hash)
    )
    if {str(map_pack["object_kind"]) for map_pack in finalizer_packs} != set(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS):
        raise ResultArchiveClosureError("archive closure finalizer object kinds are incomplete")
    await _validate_finalizer_targets(
        session, schema=schema, snapshot_key=snapshot_key, decoded_targets=set(finalizer_target_identity_by_hash)
    )
    target_identity_by_hash = _merged_target_identities(
        map_target_identity_by_hash,
        finalizer_target_identity_by_hash,
    )
    target_identity_by_hash = _merged_target_identities(
        target_identity_by_hash,
        relational_target_identity_by_hash,
    )
    return target_identity_by_hash


def _merged_target_identities(
    map_target_identity_by_hash: Mapping[bytes, tuple[str, int]],
    finalizer_target_identity_by_hash: Mapping[bytes, tuple[str, int]],
) -> dict[bytes, tuple[str, int]]:
    """Merge map families while rejecting one hash with conflicting semantics."""

    target_identity_by_hash = dict(map_target_identity_by_hash)
    for block_hash, finalizer_identity in finalizer_target_identity_by_hash.items():
        _record_target_identity(
            target_identity_by_hash,
            block_hash=block_hash,
            target_identity=finalizer_identity,
        )
    return target_identity_by_hash


__all__ = (
    "ArchiveRelation",
    "RESULT_ARCHIVE_CLOSURE_CONTRACT",
    "ResultArchiveClosure",
    "ResultArchiveClosureError",
    "select_result_archive_closure",
)
