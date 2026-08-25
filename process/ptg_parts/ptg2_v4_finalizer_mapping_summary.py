# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical V3-compatible summary for hybrid V4 finalizer mappings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    SharedMappingDigestSummary,
    _SharedMappingBinaryCopyDigest,
    _non_negative_mapping_aggregate,
    _row_mapping,
)
from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    new_v4_finalizer_kind_digest,
    update_v4_finalizer_kind_digest,
    v4_finalizer_map_root_digest,
)
from process.ptg_parts.ptg2_v4_finalizer_mapping_receipt import (
    V4FinalizerMappingReceiptSummary,
    _ALL_OBJECT_KINDS,
    _HybridRequest,
    _PRICE_OBJECT_KINDS,
    _RELATIONAL_COPY_QUERY,
    _RootCounts,
    _validated_root_counts,
    summarize_native_finalizer_mapping_receipts,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_PACK_TABLE,
    PTG2_V4_FINALIZER_MAP_TARGET_TABLE,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET,
    _load_target_metadata,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    _MapSummaryCursor,
    _decode_persisted_map_rows,
    _persisted_map_pack,
)


_PACK_BATCH_ROWS = 256


@dataclass
class _PackedTotals:
    map_pack_count: int = 0
    coordinate_count: int = 0
    entry_count: int = 0
    logical_byte_count: int = 0
    stored_map_byte_count: int = 0
    kind_digest_by_object_kind: dict[str, bytes] = field(default_factory=dict)


async def load_packed_finalizer_root(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
) -> dict[str, Any] | None:
    """Return the explicit packed root, or ``None`` for a legacy layout."""

    query_result = await session.execute(
        text(
            f"""
            SELECT root.snapshot_key AS root_snapshot_key,
                   root.state AS root_state, root.contract AS root_contract,
                   root.map_format AS root_map_format,
                   root.map_digest AS root_map_digest,
                   root.canonical_mapping_digest AS root_canonical_mapping_digest,
                   root.canonical_byte_count AS root_canonical_byte_count,
                   root.target_identity_digest AS root_target_identity_digest,
                   root.object_kind_count, root.map_pack_count,
                   root.coordinate_count, root.entry_count,
                   root.logical_byte_count, root.stored_map_byte_count,
                   root.target_block_count, root.completed_at,
                   layout.state AS layout_state,
                   layout.generation AS layout_generation
              FROM {schema}.ptg2_v4_finalizer_map_root AS root
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = root.snapshot_key
             WHERE root.snapshot_key = :snapshot_key
            """
        ),
        {"snapshot_key": int(snapshot_key)},
    )
    query_row = next(iter(query_result), None)
    root_by_name = _row_mapping(query_row) if query_row is not None else {}
    return root_by_name if root_by_name.get("root_snapshot_key") is not None else None


async def _load_pack_batch(
    request: _HybridRequest,
    *,
    object_kind: str,
    after_pack_no: int,
) -> list[dict[str, Any]]:
    query_result = await request.session.execute(
        text(
            f"""
            SELECT pack.object_kind, pack.pack_no,
                   pack.first_block_key, pack.first_fragment_no,
                   pack.last_block_key, pack.last_fragment_no,
                   pack.coordinate_count, pack.entry_count,
                   pack.logical_byte_count, pack.map_block_hash,
                   block.format_version AS map_format_version,
                   block.object_kind AS map_object_kind,
                   block.codec AS map_codec,
                   block.entry_count AS map_entry_count,
                   block.raw_byte_count AS map_raw_byte_count,
                   block.stored_byte_count AS map_stored_byte_count,
                   block.payload AS map_payload
              FROM {request.schema}.{_quote_ident(PTG2_V4_FINALIZER_MAP_PACK_TABLE)} AS pack
              JOIN {request.schema}.ptg2_v3_block AS block
                ON block.block_hash = pack.map_block_hash
             WHERE pack.snapshot_key = :snapshot_key
               AND pack.object_kind = :object_kind
               AND pack.pack_no > :after_pack_no
             ORDER BY pack.pack_no
             LIMIT :batch_rows
            """
        ),
        {
            "snapshot_key": request.snapshot_key,
            "object_kind": object_kind,
            "after_pack_no": int(after_pack_no),
            "batch_rows": _PACK_BATCH_ROWS,
        },
    )
    return [_row_mapping(query_row) for query_row in query_result]


def _append_persisted_pack(
    totals: _PackedTotals,
    persisted_pack: Any,
    kind_digest: Any,
    canonical_digest: _SharedMappingBinaryCopyDigest,
    packed_digest: _SharedMappingBinaryCopyDigest,
) -> None:
    """Add one fully authenticated pack to storage and canonical summaries."""

    update_v4_finalizer_kind_digest(kind_digest, persisted_pack)
    for reference in persisted_pack.references:
        canonical_digest.add_mapping(reference)
        packed_digest.add_mapping(reference)
    totals.map_pack_count += 1
    totals.coordinate_count += persisted_pack.coordinate_count
    totals.entry_count += persisted_pack.entry_count
    totals.logical_byte_count += persisted_pack.logical_byte_count
    totals.stored_map_byte_count += persisted_pack.map_block.stored_byte_count


async def _summarize_packed_kind(
    request: _HybridRequest,
    *,
    object_kind: str,
    totals: _PackedTotals,
    canonical_digest: _SharedMappingBinaryCopyDigest,
    packed_digest: _SharedMappingBinaryCopyDigest,
) -> None:
    """Read and validate one object kind through bounded pack batches."""

    cursor = _MapSummaryCursor()
    kind_digest = new_v4_finalizer_kind_digest(object_kind)
    initial_pack_count = totals.map_pack_count
    while True:
        pack_rows = await _load_pack_batch(
            request,
            object_kind=object_kind,
            after_pack_no=cursor.previous_pack_no,
        )
        if not pack_rows:
            break
        decoded_rows, target_hashes = _decode_persisted_map_rows(pack_rows, cursor)
        metadata_by_hash = await _load_target_metadata(
            request.session,
            schema=request.schema,
            snapshot_key=request.snapshot_key,
            target_hashes=target_hashes,
        )
        for pack_by_name, coordinates in decoded_rows:
            persisted_pack = _persisted_map_pack(
                pack_by_name, coordinates, metadata_by_hash
            )
            _append_persisted_pack(
                totals,
                persisted_pack,
                kind_digest,
                canonical_digest,
                packed_digest,
            )
        if len(pack_rows) < _PACK_BATCH_ROWS:
            break
    if totals.map_pack_count == initial_pack_count:
        raise RuntimeError(f"packed finalizer map is missing {object_kind!r}")
    totals.kind_digest_by_object_kind[object_kind] = kind_digest.digest()


async def _copy_price_kind(
    request: _HybridRequest,
    *,
    object_kind: str,
    canonical_digest: _SharedMappingBinaryCopyDigest,
    price_digest: _SharedMappingBinaryCopyDigest,
) -> None:
    """Copy one relational price kind into both canonical accumulators."""

    canonical_digest.begin_copy(object_kind)
    price_digest.begin_copy(object_kind)

    async def feed_copy(chunk_bytes: bytes) -> None:
        """Feed identical COPY bytes to both canonical digest domains."""

        canonical_digest.feed(chunk_bytes)
        price_digest.feed(chunk_bytes)

    await request.copy_from_query(
        _RELATIONAL_COPY_QUERY.format(schema=request.schema),
        request.snapshot_key,
        object_kind,
        output=feed_copy,
        format="binary",
    )
    observed_count = canonical_digest.finish_copy()
    observed_price_count = price_digest.finish_copy()
    expected_count = request.aggregate_by_object_kind[object_kind][0]
    if observed_count != expected_count or observed_price_count != expected_count:
        raise RuntimeError(
            f"shared PTG price mapping count changed for {object_kind!r}"
        )


async def _validated_target_count(request: _HybridRequest) -> int:
    """Validate every persisted anchor's complete CAS metadata setwise."""

    query_result = await request.session.execute(
        text(
            f"""
            SELECT COUNT(*) AS target_count,
                   COUNT(block.block_hash) FILTER (
                       WHERE block.format_version = :format_version
                         AND block.object_kind = ANY(CAST(:object_kinds AS text[]))
                         AND block.codec IN ('none', 'zlib')
                         AND block.entry_count >= 0
                         AND block.raw_byte_count >= 0
                         AND block.stored_byte_count >= 0
                         AND (block.codec <> 'none'
                              OR block.raw_byte_count = block.stored_byte_count)
                   ) AS valid_target_count
              FROM {request.schema}.{_quote_ident(PTG2_V4_FINALIZER_MAP_TARGET_TABLE)} AS target
              LEFT JOIN {request.schema}.ptg2_v3_block AS block
                ON block.block_hash = target.block_hash
             WHERE target.snapshot_key = :snapshot_key
            """
        ),
        {
            "snapshot_key": request.snapshot_key,
            "format_version": PTG2_V3_SHARED_FORMAT_VERSION,
            "object_kinds": PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
        },
    )
    counts_by_name = _row_mapping(next(iter(query_result), None))
    target_count = _non_negative_mapping_aggregate(
        counts_by_name.get("target_count"), name="packed target_count"
    )
    valid_count = _non_negative_mapping_aggregate(
        counts_by_name.get("valid_target_count"), name="packed valid_target_count"
    )
    if target_count != valid_count:
        raise RuntimeError("packed finalizer target anchors are invalid")
    return target_count


def _validate_packed_totals(
    totals: _PackedTotals,
    root_counts: _RootCounts,
    root_digest: bytes,
) -> None:
    """Match recomputed pack descriptors and aggregates to the sealed root."""

    for field_name in (
        "map_pack_count",
        "coordinate_count",
        "entry_count",
        "logical_byte_count",
        "stored_map_byte_count",
    ):
        if getattr(totals, field_name) != getattr(root_counts, field_name):
            raise RuntimeError(
                f"packed finalizer root disagrees for {field_name}"
            )
    observed_digest = v4_finalizer_map_root_digest(
        totals.kind_digest_by_object_kind,
        required_object_kinds=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    )
    if observed_digest != bytes(root_digest):
        raise RuntimeError("packed finalizer root digest changed")


def _finish_hybrid_summary(
    request: _HybridRequest,
    *,
    root_counts: _RootCounts,
    target_block_count: int,
    canonical_digest: _SharedMappingBinaryCopyDigest,
    packed_digest: _SharedMappingBinaryCopyDigest,
    price_digest: _SharedMappingBinaryCopyDigest,
) -> SharedMappingDigestSummary:
    """Finish exact component and global digests after all bounded reads."""

    canonical_summary = canonical_digest.finish()
    packed_summary = packed_digest.finish()
    price_summary = price_digest.finish()
    price_mapping_count = sum(
        aggregate_counts[0]
        for aggregate_counts in request.aggregate_by_object_kind.values()
    )
    price_unique_count = sum(
        aggregate_counts[1]
        for aggregate_counts in request.aggregate_by_object_kind.values()
    )
    price_entry_count = sum(
        aggregate_counts[2]
        for aggregate_counts in request.aggregate_by_object_kind.values()
    )
    price_logical_bytes = sum(
        aggregate_counts[3]
        for aggregate_counts in request.aggregate_by_object_kind.values()
    )
    expected_count = root_counts.coordinate_count + price_mapping_count
    expected_entries = root_counts.entry_count + price_entry_count
    if (
        packed_summary.mapping_count != root_counts.coordinate_count
        or price_summary.mapping_count != price_mapping_count
        or canonical_summary.mapping_count != expected_count
        or canonical_summary.entry_count != expected_entries
    ):
        raise RuntimeError("hybrid finalizer canonical mapping aggregates changed")
    return SharedMappingDigestSummary(
        mapping_digest=canonical_summary.mapping_digest,
        mapping_count=canonical_summary.mapping_count,
        unique_block_count=target_block_count + price_unique_count,
        entry_count=canonical_summary.entry_count,
        logical_byte_count=root_counts.logical_byte_count + price_logical_bytes,
        canonical_byte_count=canonical_summary.canonical_byte_count,
        object_kinds=_ALL_OBJECT_KINDS,
        packed_mapping_digest=packed_summary.mapping_digest,
        packed_mapping_count=packed_summary.mapping_count,
        relational_mapping_digest=price_summary.mapping_digest,
        relational_mapping_count=price_summary.mapping_count,
    )


async def summarize_hybrid_finalizer_mappings(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    root_by_name: Mapping[str, Any],
    aggregate_by_object_kind: Mapping[str, tuple[int, int, int, int]],
    copy_from_query: Any,
) -> SharedMappingDigestSummary:
    """Recompute canonical mappings over six packed and two relational kinds."""

    if set(aggregate_by_object_kind) != set(_PRICE_OBJECT_KINDS):
        raise RuntimeError(
            "packed finalizer layout requires exactly two relational price mappings"
        )
    request = _HybridRequest(
        session=session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        aggregate_by_object_kind=aggregate_by_object_kind,
        copy_from_query=copy_from_query,
    )
    root_counts = _validated_root_counts(root_by_name)
    totals = _PackedTotals()
    canonical_digest = _SharedMappingBinaryCopyDigest()
    packed_digest = _SharedMappingBinaryCopyDigest()
    price_digest = _SharedMappingBinaryCopyDigest()
    for object_kind in _ALL_OBJECT_KINDS:
        if object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET:
            await _summarize_packed_kind(
                request,
                object_kind=object_kind,
                totals=totals,
                canonical_digest=canonical_digest,
                packed_digest=packed_digest,
            )
        else:
            await _copy_price_kind(
                request,
                object_kind=object_kind,
                canonical_digest=canonical_digest,
                price_digest=price_digest,
            )
    _validate_packed_totals(totals, root_counts, bytes(root_by_name["root_map_digest"]))
    target_block_count = await _validated_target_count(request)
    if target_block_count != root_counts.target_block_count:
        raise RuntimeError("packed finalizer target anchor count changed")
    return _finish_hybrid_summary(
        request,
        root_counts=root_counts,
        target_block_count=target_block_count,
        canonical_digest=canonical_digest,
        packed_digest=packed_digest,
        price_digest=price_digest,
    )


__all__ = (
    "V4FinalizerMappingReceiptSummary",
    "load_packed_finalizer_root",
    "summarize_native_finalizer_mapping_receipts",
    "summarize_hybrid_finalizer_mappings",
)
