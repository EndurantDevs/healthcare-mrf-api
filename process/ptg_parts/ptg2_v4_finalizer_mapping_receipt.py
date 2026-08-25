# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native receipt summary for hybrid V4 finalizer mappings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_blocks import (
    SharedMappingDigestSummary,
    _SharedMappingBinaryCopyDigest,
    _non_negative_mapping_aggregate,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_SHARED_GENERATION,
)


_PRICE_OBJECT_KINDS = ("price_atoms_v3", "price_set_atom_memberships_v3")
_ALL_OBJECT_KINDS = tuple(
    sorted((*PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS, *_PRICE_OBJECT_KINDS))
)
_RELATIONAL_COPY_QUERY = """
    SELECT pg_catalog.int4send(
               pg_catalog.octet_length(
                   pg_catalog.convert_to(mapping.object_kind, 'UTF8')
               )
           )
           || pg_catalog.convert_to(mapping.object_kind, 'UTF8')
           || pg_catalog.int8send(mapping.block_key)
           || pg_catalog.int4send(mapping.fragment_no)
           || pg_catalog.int8send(mapping.entry_count)
           || mapping.block_hash AS mapping_record
      FROM {schema}.ptg2_v3_snapshot_block AS mapping
     WHERE mapping.snapshot_key = $1::bigint
       AND mapping.object_kind = $2::text
     ORDER BY mapping.block_key, mapping.fragment_no
"""


@dataclass(frozen=True)
class _RootCounts:
    object_kind_count: int
    map_pack_count: int
    coordinate_count: int
    entry_count: int
    logical_byte_count: int
    stored_map_byte_count: int
    target_block_count: int


@dataclass(frozen=True)
class _HybridRequest:
    session: Any
    schema: str
    snapshot_key: int
    aggregate_by_object_kind: Mapping[str, tuple[int, int, int, int]]
    copy_from_query: Any


@dataclass(frozen=True)
class V4FinalizerMappingReceiptSummary:
    """Exact V4 component evidence without rebuilding packed coordinates."""

    mapping_count: int
    unique_block_count: int
    entry_count: int
    logical_byte_count: int
    object_kinds: tuple[str, ...]
    packed_mapping_digest: bytes
    packed_mapping_count: int
    packed_canonical_byte_count: int
    relational_mapping_digest: bytes
    relational_mapping_count: int


def _validated_root_counts(root_by_name: Mapping[str, Any]) -> _RootCounts:
    """Validate the immutable root contract before reading any map payload."""

    if (
        root_by_name.get("root_state") != "complete"
        or root_by_name.get("completed_at") is None
        or root_by_name.get("root_contract") != PTG2_V4_FINALIZER_MAP_CONTRACT
        or root_by_name.get("root_map_format") != PTG2_V4_MAP_FORMAT
        or root_by_name.get("layout_generation") != PTG2_V4_SHARED_GENERATION
        or root_by_name.get("layout_state") not in {"building", "sealed"}
        or len(bytes(root_by_name.get("root_map_digest") or b"")) != 32
    ):
        raise RuntimeError("packed finalizer map root is incomplete or incompatible")
    counts = _RootCounts(
        **{
            field_name: _non_negative_mapping_aggregate(
                root_by_name.get(field_name), name=f"packed {field_name}"
            )
            for field_name in _RootCounts.__dataclass_fields__
        }
    )
    if (
        counts.object_kind_count != len(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
        or counts.map_pack_count < counts.object_kind_count
        or counts.coordinate_count < counts.map_pack_count
        or not 0 < counts.target_block_count <= counts.coordinate_count
        or counts.stored_map_byte_count <= 0
    ):
        raise RuntimeError("packed finalizer map root geometry is invalid")
    return counts


async def _summarize_price_mappings(
    request: _HybridRequest,
) -> SharedMappingDigestSummary:
    """Hash each small relational-price mapping exactly once."""

    price_digest = _SharedMappingBinaryCopyDigest()
    for object_kind in _PRICE_OBJECT_KINDS:
        price_digest.begin_copy(object_kind)

        async def feed_copy(chunk_bytes: bytes) -> None:
            """Feed one relational price COPY chunk to its digest."""

            price_digest.feed(chunk_bytes)

        await request.copy_from_query(
            _RELATIONAL_COPY_QUERY.format(schema=request.schema),
            request.snapshot_key,
            object_kind,
            output=feed_copy,
            format="binary",
        )
        observed_count = price_digest.finish_copy()
        if observed_count != request.aggregate_by_object_kind[object_kind][0]:
            raise RuntimeError(
                f"shared PTG price mapping count changed for {object_kind!r}"
            )
    parsed = price_digest.finish()
    expected_mapping_count = sum(
        aggregate_values[0]
        for aggregate_values in request.aggregate_by_object_kind.values()
    )
    expected_entry_count = sum(
        aggregate_values[2]
        for aggregate_values in request.aggregate_by_object_kind.values()
    )
    if (
        parsed.mapping_count != expected_mapping_count
        or parsed.entry_count != expected_entry_count
    ):
        raise RuntimeError("shared PTG relational price receipt changed")
    return SharedMappingDigestSummary(
        mapping_digest=parsed.mapping_digest,
        mapping_count=parsed.mapping_count,
        unique_block_count=sum(
            aggregate_values[1]
            for aggregate_values in request.aggregate_by_object_kind.values()
        ),
        entry_count=parsed.entry_count,
        logical_byte_count=sum(
            aggregate_values[3]
            for aggregate_values in request.aggregate_by_object_kind.values()
        ),
        canonical_byte_count=parsed.canonical_byte_count,
        object_kinds=_PRICE_OBJECT_KINDS,
    )


async def summarize_native_finalizer_mapping_receipts(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    root_by_name: Mapping[str, Any],
    aggregate_by_object_kind: Mapping[str, tuple[int, int, int, int]],
    copy_from_query: Any,
) -> V4FinalizerMappingReceiptSummary:
    """Use one immutable native receipt plus the two relational price streams."""

    if root_by_name.get("root_contract") != PTG2_V4_FINALIZER_MAP_CONTRACT:
        raise RuntimeError("packed finalizer native receipt contract is unavailable")
    if set(aggregate_by_object_kind) != set(_PRICE_OBJECT_KINDS):
        raise RuntimeError(
            "packed finalizer layout requires exactly two relational price mappings"
        )
    root_counts = _validated_root_counts(root_by_name)
    packed_digest = bytes(root_by_name.get("root_canonical_mapping_digest") or b"")
    target_digest = bytes(root_by_name.get("root_target_identity_digest") or b"")
    packed_canonical_bytes = _non_negative_mapping_aggregate(
        root_by_name.get("root_canonical_byte_count"),
        name="packed canonical_byte_count",
    )
    if (
        len(packed_digest) != 32
        or len(target_digest) != 32
        or packed_canonical_bytes <= 0
    ):
        raise RuntimeError("packed finalizer native receipt is incomplete")
    request = _HybridRequest(
        session=session,
        schema=schema,
        snapshot_key=int(snapshot_key),
        aggregate_by_object_kind=aggregate_by_object_kind,
        copy_from_query=copy_from_query,
    )
    price = await _summarize_price_mappings(request)
    return V4FinalizerMappingReceiptSummary(
        mapping_count=root_counts.coordinate_count + price.mapping_count,
        unique_block_count=root_counts.target_block_count + price.unique_block_count,
        entry_count=root_counts.entry_count + price.entry_count,
        logical_byte_count=root_counts.logical_byte_count + price.logical_byte_count,
        object_kinds=_ALL_OBJECT_KINDS,
        packed_mapping_digest=packed_digest,
        packed_mapping_count=root_counts.coordinate_count,
        packed_canonical_byte_count=packed_canonical_bytes,
        relational_mapping_digest=price.mapping_digest,
        relational_mapping_count=price.mapping_count,
    )


__all__ = (
    "V4FinalizerMappingReceiptSummary",
    "summarize_native_finalizer_mapping_receipts",
)
