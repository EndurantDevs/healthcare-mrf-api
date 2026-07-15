# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable shared PostgreSQL block publication for strict PTG V3 snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident


PTG2_V3_SHARED_GENERATION = "shared_blocks_v3"
PTG2_V3_CLEANUP_GENERATIONS = (
    "shared_blocks_v1",
    "shared_blocks_v2",
    PTG2_V3_SHARED_GENERATION,
)
PTG2_V3_COLD_LOOKUP_CONTRACT = "ptg_v3_cold_v2"
PTG2_V3_SHARED_BLOCK_LAYOUT = "dense_shared_blocks_v3"
PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS = "multiset_v1"
PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS = "source_multiset_v1"
PTG2_V3_SHARED_FORMAT_VERSION = 2
PTG2_V3_BUILD_LEASE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BUILD_LEASE_SECONDS"
PTG2_V3_SEALED_LEASE_SECONDS_ENV = "HLTHPRT_PTG2_V3_SEALED_LEASE_SECONDS"
PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS"
PTG2_V3_BUILD_LEASE_SECONDS_DEFAULT = 21_600
PTG2_V3_SEALED_LEASE_SECONDS_DEFAULT = 3_600
PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT = 21_600
PTG2_V3_DENSE_LAYOUT_TABLES = (
    "ptg2_v3_graph_owner",
    "ptg2_v3_code",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_price_attr",
    "ptg2_v3_npi_scope",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_source_audit_witness",
)
_BLOCK_HASH_DOMAIN = b"PTG2V3BLOCK\x01"
_MAPPING_HASH_DOMAIN = b"PTG2V3MAP\x02"
_SEMANTIC_HASH_DOMAIN = b"PTG2V3SEMANTIC\x02"
_SUPPORT_HASH_DOMAIN = b"PTG2V3SUPPORT\x02"


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


def _length_prefixed(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def shared_semantic_fingerprint(payload: Mapping[str, Any]) -> bytes:
    """Fingerprint every input and semantic option that can change output."""

    canonical = json.dumps(
        dict(payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(_SEMANTIC_HASH_DOMAIN + canonical).digest()


def shared_support_digest(payload: Mapping[str, Any]) -> bytes:
    """Hash canonical relational dictionaries and physical layout metadata."""

    canonical = json.dumps(
        dict(payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(_SUPPORT_HASH_DOMAIN + canonical).digest()


async def delete_shared_layout_dense_rows(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> None:
    """Delete dense immutable children before removing their layout owner."""

    schema = _quote_ident(schema_name)
    for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
        await session.execute(
            text(
                f"DELETE FROM {schema}.{_quote_ident(table_name)} "
                "WHERE snapshot_key = :snapshot_key"
            ),
            {"snapshot_key": int(snapshot_key)},
        )


def shared_block_hash(
    *,
    format_version: int,
    object_kind: str,
    codec: str,
    payload: bytes,
) -> bytes:
    """Hash canonical stored bytes independently of snapshot ownership."""

    normalized_kind = str(object_kind or "").strip()
    normalized_codec = str(codec or "").strip().lower()
    if not normalized_kind or not normalized_codec:
        raise ValueError("shared PTG block kind and codec are required")
    if int(format_version) != PTG2_V3_SHARED_FORMAT_VERSION:
        raise ValueError(
            "strict shared PTG blocks require durable format_version "
            f"{PTG2_V3_SHARED_FORMAT_VERSION}"
        )
    digest = hashlib.sha256()
    digest.update(_BLOCK_HASH_DOMAIN)
    digest.update(struct.pack(">H", int(format_version)))
    digest.update(_length_prefixed(normalized_kind.encode("utf-8")))
    digest.update(_length_prefixed(normalized_codec.encode("ascii")))
    digest.update(_length_prefixed(bytes(payload)))
    return digest.digest()


@dataclass(frozen=True)
class SharedBlock:
    object_kind: str
    block_key: int
    fragment_no: int
    entry_count: int
    codec: str
    raw_byte_count: int
    payload: bytes
    format_version: int = PTG2_V3_SHARED_FORMAT_VERSION

    def __post_init__(self) -> None:
        if not str(self.object_kind or "").strip():
            raise ValueError("shared PTG block object_kind is required")
        if int(self.fragment_no) < 0 or int(self.entry_count) < 0:
            raise ValueError("shared PTG block counts must be non-negative")
        if int(self.raw_byte_count) < 0:
            raise ValueError("shared PTG block raw_byte_count must be non-negative")
        if int(self.format_version) != PTG2_V3_SHARED_FORMAT_VERSION:
            raise ValueError(
                "strict shared PTG blocks require durable format_version "
                f"{PTG2_V3_SHARED_FORMAT_VERSION}"
            )
        normalized_codec = str(self.codec or "").strip().lower()
        if normalized_codec not in {"none", "zlib"} or self.codec != normalized_codec:
            raise ValueError("shared PTG block codec must be none or zlib")
        if self.codec == "none" and int(self.raw_byte_count) != len(self.payload):
            raise ValueError("uncompressed shared PTG block size does not match payload")

    @property
    def block_hash(self) -> bytes:
        """Return the domain-separated content hash for this stored block."""

        return shared_block_hash(
            format_version=self.format_version,
            object_kind=self.object_kind,
            codec=self.codec,
            payload=self.payload,
        )

    @property
    def stored_byte_count(self) -> int:
        """Return the encoded payload size in bytes."""

        return len(self.payload)

    def reference(self) -> "SharedBlockReference":
        """Return the immutable mapping reference for this block."""

        return SharedBlockReference(
            object_kind=self.object_kind,
            block_key=int(self.block_key),
            fragment_no=int(self.fragment_no),
            entry_count=int(self.entry_count),
            block_hash=self.block_hash,
            raw_byte_count=int(self.raw_byte_count),
        )


@dataclass(frozen=True)
class SharedBlockReference:
    object_kind: str
    block_key: int
    fragment_no: int
    entry_count: int
    block_hash: bytes
    raw_byte_count: int


@dataclass(frozen=True)
class SharedBlockBatchResult:
    references: tuple[SharedBlockReference, ...]
    unique_block_count: int
    mapping_count: int
    logical_byte_count: int


@dataclass(frozen=True)
class SharedLayoutReservation:
    snapshot_key: int
    reused: bool
    layout_manifest: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class SharedLayoutBuildOwnership:
    snapshot_key: int
    build_token: str


@dataclass(frozen=True)
class SealedSharedLayout:
    snapshot_key: int
    mapping_digest: bytes
    reused: bool


def _block_reference(block: SharedBlock | SharedBlockReference) -> SharedBlockReference:
    return block.reference() if isinstance(block, SharedBlock) else block


def shared_mapping_digest(blocks: Iterable[SharedBlock | SharedBlockReference]) -> bytes:
    """Return the deterministic digest sealed into one snapshot layout."""

    ordered = sorted(
        (_block_reference(block) for block in blocks),
        key=lambda block: (
            block.object_kind,
            int(block.block_key),
            int(block.fragment_no),
        ),
    )
    digest = hashlib.sha256()
    digest.update(_MAPPING_HASH_DOMAIN)
    previous_key: tuple[str, int, int] | None = None
    for block in ordered:
        mapping_key = (block.object_kind, int(block.block_key), int(block.fragment_no))
        if mapping_key == previous_key:
            raise ValueError(f"duplicate shared PTG mapping key: {mapping_key!r}")
        previous_key = mapping_key
        digest.update(_length_prefixed(block.object_kind.encode("utf-8")))
        digest.update(struct.pack(">q", int(block.block_key)))
        digest.update(struct.pack(">I", int(block.fragment_no)))
        digest.update(struct.pack(">Q", int(block.entry_count)))
        digest.update(block.block_hash)
    return digest.digest()


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _advisory_lock_key(digest: bytes) -> int:
    if len(digest) != 32:
        raise ValueError("shared PTG digest must contain 32 bytes")
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


async def _queue_snapshot_blocks_for_gc(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> None:
    schema = _quote_ident(schema_name)
    grace_seconds = _env_non_negative_seconds(
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV,
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT,
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
                (block_hash, eligible_at, queued_at)
            SELECT DISTINCT mapping.block_hash,
                   transaction_timestamp()
                       + (:grace_seconds * INTERVAL '1 second'),
                   transaction_timestamp()
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
             WHERE mapping.snapshot_key = :snapshot_key
            ON CONFLICT (block_hash) DO UPDATE
                SET eligible_at = GREATEST(candidate.eligible_at, EXCLUDED.eligible_at)
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "grace_seconds": grace_seconds,
        },
    )


async def _is_shared_layout_bound(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> bool:
    schema = _quote_ident(schema_name)
    binding_result = await session.execute(
        text(
            f"""
            SELECT 1
              FROM {schema}.ptg2_v3_snapshot_binding
             WHERE snapshot_key = :snapshot_key
             LIMIT 1
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    return binding_result.scalar() is not None


async def _is_shared_layout_build_owned(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> bool:
    schema = _quote_ident(schema_name)
    owner_result = await session.execute(
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
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    return owner_result.scalar() is not None


async def is_shared_layout_build_abandoned(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> bool:
    """Remove only the caller's unpublished building layout after failure."""

    schema = _quote_ident(schema_name)
    if not await _is_shared_layout_build_owned(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
    ):
        return False
    if await _is_shared_layout_bound(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
    ):
        raise RuntimeError("refusing to abandon a bound shared PTG layout")
    await _queue_snapshot_blocks_for_gc(
        session,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
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
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    if delete_result.scalar() is None:
        raise RuntimeError("shared PTG layout ownership changed during abandonment")
    return True


async def lock_shared_layout_for_dense_write(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    """Fence GC while one transaction writes dense rows without foreign keys."""

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
             FOR KEY SHARE
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    if ownership_result.scalar() is None:
        raise RuntimeError("shared PTG dense write lost its build ownership")


async def touch_shared_layout_build(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    """Extend ownership of a live build at bounded publication checkpoints."""

    schema = _quote_ident(schema_name)
    heartbeat_result = await session.execute(
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
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(),
        },
    )
    if heartbeat_result.scalar() is None:
        raise RuntimeError("shared PTG build heartbeat lost ownership of the reserved layout")


async def reserve_shared_layout(
    session: Any,
    *,
    schema_name: str,
    semantic_fingerprint: bytes,
    build_token: str,
    storage_shard_id: int = 0,
) -> SharedLayoutReservation:
    """Reuse a sealed semantic match or reserve one immutable build layout."""

    schema = _quote_ident(schema_name)
    fingerprint = bytes(semantic_fingerprint)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": _advisory_lock_key(fingerprint)},
    )
    existing_result = await session.execute(
        text(
            f"""
            SELECT layout.snapshot_key, layout.state, layout.generation,
                   layout.build_token, layout.layout_manifest
              FROM {schema}.ptg2_v3_layout_fingerprint fingerprint
              JOIN {schema}.ptg2_v3_snapshot_layout layout
                ON layout.snapshot_key = fingerprint.snapshot_key
             WHERE fingerprint.semantic_fingerprint = :semantic_fingerprint
             LIMIT 1
             FOR UPDATE OF layout
            """
        ),
        {"semantic_fingerprint": fingerprint},
    )
    existing_row = existing_result.first()
    if existing_row is not None:
        existing = _row_mapping(existing_row)
        if existing.get("state") == "sealed" and existing.get("generation") == PTG2_V3_SHARED_GENERATION:
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
                    "snapshot_key": int(existing["snapshot_key"]),
                    "generation": PTG2_V3_SHARED_GENERATION,
                    "heartbeat_at": _utcnow(),
                    "lease_until": _lease_deadline(sealed=True),
                },
            )
            return SharedLayoutReservation(
                int(existing["snapshot_key"]),
                True,
                dict(existing.get("layout_manifest") or {}),
            )
        if (
            existing.get("state") == "building"
            and existing.get("generation") == PTG2_V3_SHARED_GENERATION
            and existing.get("build_token") == str(build_token)
        ):
            return SharedLayoutReservation(int(existing["snapshot_key"]), False, None)
        raise RuntimeError("matching shared PTG layout is already building or uses another generation")
    reservation_result = await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (storage_shard_id, build_token, generation, state, logical_byte_count,
                 created_at, heartbeat_at, lease_until)
            VALUES
                (:storage_shard_id, :build_token, :generation, 'building', 0,
                 :created_at, :heartbeat_at, :lease_until)
            RETURNING snapshot_key
            """
        ),
        {
            "storage_shard_id": int(storage_shard_id),
            "build_token": str(build_token),
            "generation": PTG2_V3_SHARED_GENERATION,
            "created_at": _utcnow(),
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(),
        },
    )
    snapshot_key = reservation_result.scalar()
    if snapshot_key is None:
        raise RuntimeError("shared PTG layout reservation did not return a key")
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


def _block_insert_rows(blocks: Sequence[SharedBlock]) -> list[dict[str, Any]]:
    created_at = _utcnow()
    return [
        {
            "block_hash": block.block_hash,
            "format_version": int(block.format_version),
            "object_kind": block.object_kind,
            "codec": block.codec,
            "entry_count": int(block.entry_count),
            "raw_byte_count": int(block.raw_byte_count),
            "stored_byte_count": block.stored_byte_count,
            "payload": block.payload,
            "created_at": created_at,
        }
        for block in blocks
    ]


async def insert_shared_blocks(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    blocks: Sequence[SharedBlock],
) -> SharedBlockBatchResult:
    """Insert immutable payloads and snapshot mappings in one transaction."""

    if not blocks:
        return SharedBlockBatchResult((), 0, 0, 0)
    shared_mapping_digest(blocks)
    schema = _quote_ident(schema_name)
    block_rows_by_hash: dict[bytes, dict[str, Any]] = {}
    for block_row in _block_insert_rows(blocks):
        existing = block_rows_by_hash.get(block_row["block_hash"])
        if existing is not None and any(
            existing[field_name] != block_row[field_name]
            for field_name in (
                "format_version",
                "object_kind",
                "codec",
                "entry_count",
                "raw_byte_count",
                "stored_byte_count",
                "payload",
            )
        ):
            raise ValueError("shared PTG content hash has inconsistent block metadata")
        block_rows_by_hash[block_row["block_hash"]] = block_row
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
        list(block_rows_by_hash.values()),
    )
    stored_result = await session.execute(
        text(
            f"""
            SELECT block_hash, format_version, object_kind, codec, entry_count,
                   raw_byte_count, stored_byte_count
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
            """
        ),
        {"block_hashes": list(block_rows_by_hash)},
    )
    observed_hashes: set[bytes] = set()
    for stored_row in stored_result:
        stored = _row_mapping(stored_row)
        block_hash = bytes(stored["block_hash"])
        expected = block_rows_by_hash.get(block_hash)
        if expected is None:
            raise RuntimeError("shared PTG block query returned an unexpected hash")
        observed_hashes.add(block_hash)
        for field_name in (
            "format_version",
            "object_kind",
            "codec",
            "entry_count",
            "raw_byte_count",
            "stored_byte_count",
        ):
            if stored[field_name] != expected[field_name]:
                raise RuntimeError(f"shared PTG block hash metadata mismatch: {field_name}")
    if observed_hashes != set(block_rows_by_hash):
        raise RuntimeError("shared PTG block insert did not retain every requested hash")
    mapping_rows = [
        {
            "snapshot_key": int(snapshot_key),
            "object_kind": block.object_kind,
            "block_key": int(block.block_key),
            "fragment_no": int(block.fragment_no),
            "entry_count": int(block.entry_count),
            "block_hash": block.block_hash,
        }
        for block in blocks
    ]
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_block
                (snapshot_key, object_kind, block_key, fragment_no, entry_count, block_hash)
            VALUES
                (:snapshot_key, :object_kind, :block_key, :fragment_no, :entry_count, :block_hash)
            ON CONFLICT (snapshot_key, object_kind, block_key, fragment_no) DO NOTHING
            """
        ),
        mapping_rows,
    )
    expected_mapping_count = len(mapping_rows)
    references = tuple(block.reference() for block in blocks)
    return SharedBlockBatchResult(
        references=references,
        unique_block_count=len(block_rows_by_hash),
        mapping_count=expected_mapping_count,
        logical_byte_count=sum(reference.raw_byte_count for reference in references),
    )


async def seal_shared_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    expected_blocks: Sequence[SharedBlockReference],
    support_digest: bytes,
    layout_manifest: Mapping[str, Any],
) -> SealedSharedLayout:
    """Validate every mapping and atomically seal the immutable layout."""

    expected_digest = shared_mapping_digest(expected_blocks)
    normalized_support_digest = bytes(support_digest)
    if len(normalized_support_digest) != 32:
        raise ValueError("shared PTG support digest must contain 32 bytes")
    normalized_layout_manifest = json.loads(
        json.dumps(
            dict(layout_manifest),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    schema = _quote_ident(schema_name)
    owner_result = await session.execute(
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
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    if owner_result.scalar() is None:
        raise RuntimeError("shared PTG seal lost ownership of its building layout")
    mapping_result = await session.execute(
        text(
            f"""
            SELECT mapping.object_kind, mapping.block_key, mapping.fragment_no,
                   mapping.entry_count, mapping.block_hash,
                   block.format_version, block.raw_byte_count
              FROM {schema}.ptg2_v3_snapshot_block mapping
              JOIN {schema}.ptg2_v3_block block
                ON block.block_hash = mapping.block_hash
             WHERE mapping.snapshot_key = :snapshot_key
             ORDER BY mapping.object_kind, mapping.block_key, mapping.fragment_no
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    observed_rows = [_row_mapping(mapping_row) for mapping_row in mapping_result]
    expected_by_key = {
        (block.object_kind, int(block.block_key), int(block.fragment_no)): block
        for block in expected_blocks
    }
    if len(observed_rows) != len(expected_by_key):
        raise RuntimeError(
            f"shared PTG mapping count mismatch: expected {len(expected_by_key)}, "
            f"observed {len(observed_rows)}"
        )
    logical_byte_count = 0
    for observed_row in observed_rows:
        key = (
            str(observed_row["object_kind"]),
            int(observed_row["block_key"]),
            int(observed_row["fragment_no"]),
        )
        expected = expected_by_key.get(key)
        if expected is None:
            raise RuntimeError(f"unexpected shared PTG mapping: {key!r}")
        if bytes(observed_row["block_hash"]) != expected.block_hash or int(
            observed_row["entry_count"]
        ) != int(expected.entry_count):
            raise RuntimeError(f"shared PTG mapping mismatch: {key!r}")
        if int(observed_row["format_version"]) != PTG2_V3_SHARED_FORMAT_VERSION:
            raise RuntimeError(f"shared PTG mapping uses an incompatible block version: {key!r}")
        logical_byte_count += int(observed_row["raw_byte_count"])
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": _advisory_lock_key(expected_digest)},
    )
    reusable_result = await session.execute(
        text(
            f"""
            SELECT snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout
             WHERE generation = :generation
               AND state = 'sealed'
               AND mapping_digest = :mapping_digest
               AND support_digest = :support_digest
               AND snapshot_key <> :snapshot_key
             LIMIT 1
            """
        ),
        {
            "generation": PTG2_V3_SHARED_GENERATION,
            "mapping_digest": expected_digest,
            "support_digest": normalized_support_digest,
            "snapshot_key": int(snapshot_key),
        },
    )
    reusable_snapshot_key = reusable_result.scalar()
    if reusable_snapshot_key is not None:
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
                "snapshot_key": int(reusable_snapshot_key),
                "generation": PTG2_V3_SHARED_GENERATION,
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
                "reusable_snapshot_key": int(reusable_snapshot_key),
                "snapshot_key": int(snapshot_key),
            },
        )
        await delete_shared_layout_dense_rows(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
        )
        await _queue_snapshot_blocks_for_gc(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
        )
        await session.execute(
            text(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_layout
                 WHERE snapshot_key = :snapshot_key
                   AND state = 'building'
                   AND build_token = :build_token
                """
            ),
            {
                "snapshot_key": int(snapshot_key),
                "build_token": str(build_token),
            },
        )
        return SealedSharedLayout(int(reusable_snapshot_key), expected_digest, True)
    update_result = await session.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET state = 'sealed', mapping_digest = :mapping_digest,
                   support_digest = :support_digest,
                   layout_manifest = CAST(:layout_manifest AS jsonb),
                   logical_byte_count = :logical_byte_count,
                   heartbeat_at = :heartbeat_at,
                   lease_until = :lease_until,
                   published_at = :published_at
             WHERE snapshot_key = :snapshot_key
               AND state = 'building'
               AND generation = :generation
               AND build_token = :build_token
            RETURNING snapshot_key
            """
        ),
        {
            "snapshot_key": int(snapshot_key),
            "mapping_digest": expected_digest,
            "support_digest": normalized_support_digest,
            "layout_manifest": json.dumps(
                normalized_layout_manifest,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "logical_byte_count": logical_byte_count,
            "heartbeat_at": _utcnow(),
            "lease_until": _lease_deadline(sealed=True),
            "published_at": _utcnow(),
            "generation": PTG2_V3_SHARED_GENERATION,
            "build_token": str(build_token),
        },
    )
    if update_result.scalar() is None:
        raise RuntimeError("shared PTG layout was not in the expected building generation")
    return SealedSharedLayout(int(snapshot_key), expected_digest, False)


async def bind_snapshot_to_shared_layout(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    snapshot_key: int,
) -> None:
    """Bind one logical published snapshot to one sealed physical layout."""

    schema = _quote_ident(schema_name)
    binding_result = await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key, created_at)
            SELECT :snapshot_id, layout.snapshot_key, :created_at
              FROM {schema}.ptg2_v3_snapshot_layout layout
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
            "generation": PTG2_V3_SHARED_GENERATION,
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
    raise RuntimeError("logical PTG snapshot is bound to another layout or physical layout is not sealed")
