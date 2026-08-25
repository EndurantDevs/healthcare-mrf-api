# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded logical-row selection from authenticated finalizer map packs."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from process.ptg_parts.ptg2_v4_finalizer_maps import (
    FinalizerMapError,
    FinalizerMapReadLimitError,
    _MAP_PACK_PAGE_ROWS,
    _load_map_packs,
    _load_target_metadata,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import V4SnapshotMapCoordinate


def _assert_pack_page_order(
    decoded_packs: Sequence[
        tuple[Mapping[str, Any], Sequence[V4SnapshotMapCoordinate]]
    ],
    previous_last: tuple[int, int] | None,
) -> None:
    first_coordinate = decoded_packs[0][1][0]
    first = (first_coordinate.block_key, first_coordinate.fragment_no)
    if previous_last is not None and first <= previous_last:
        raise FinalizerMapError("packed finalizer map pack ranges overlap")


def _pack_target_hashes(
    decoded_packs: Iterable[
        tuple[Mapping[str, Any], Sequence[V4SnapshotMapCoordinate]]
    ],
) -> set[bytes]:
    return {
        bytes(coordinate.block_hash)
        for _pack_fields, coordinates in decoded_packs
        for coordinate in coordinates
    }


def _last_pack_coordinate(
    decoded_packs: Sequence[
        tuple[Mapping[str, Any], Sequence[V4SnapshotMapCoordinate]]
    ],
) -> tuple[int, int]:
    last_coordinate = decoded_packs[-1][1][-1]
    return last_coordinate.block_key, last_coordinate.fragment_no


def _selected_mapping_rows(
    decoded_packs: Iterable[
        tuple[Mapping[str, Any], Sequence[V4SnapshotMapCoordinate]]
    ],
    *,
    metadata_by_hash: Mapping[bytes, tuple[str, int, int]],
    block_keys: frozenset[int],
    fragment_nos: frozenset[int],
    has_fragment_filter: bool,
    row_limit: int,
) -> tuple[dict[str, Any], ...]:
    mapping_rows: list[dict[str, Any]] = []
    for pack_fields, coordinates in decoded_packs:
        logical_byte_count = 0
        for coordinate in coordinates:
            target_kind, target_entry_count, raw_byte_count = metadata_by_hash[
                bytes(coordinate.block_hash)
            ]
            if (
                target_kind != coordinate.object_kind
                or target_entry_count != coordinate.entry_count
            ):
                raise FinalizerMapError(
                    "packed finalizer map target identity is inconsistent"
                )
            logical_byte_count += raw_byte_count
            if coordinate.block_key not in block_keys or (
                has_fragment_filter
                and coordinate.fragment_no not in fragment_nos
            ):
                continue
            if len(mapping_rows) >= row_limit:
                raise FinalizerMapReadLimitError(
                    "packed finalizer map exceeds its bounded row limit"
                )
            mapping_rows.append(
                {
                    "object_kind": coordinate.object_kind,
                    "block_key": coordinate.block_key,
                    "fragment_no": coordinate.fragment_no,
                    "mapping_entry_count": coordinate.entry_count,
                    "block_hash": coordinate.block_hash,
                }
            )
        if logical_byte_count != int(pack_fields.get("logical_byte_count") or 0):
            raise FinalizerMapError(
                "packed finalizer map logical byte count is inconsistent"
            )
    return tuple(mapping_rows)


async def load_selected_mapping_rows(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    object_kind: str,
    block_keys: tuple[int, ...],
    fragment_nos: tuple[int, ...],
    has_fragment_filter: bool,
    row_limit: int,
) -> tuple[dict[str, Any], ...]:
    """Load selected logical rows through bounded authenticated pack pages."""

    mapping_rows: list[dict[str, Any]] = []
    block_key_set = frozenset(block_keys)
    fragment_no_set = frozenset(fragment_nos)
    previous_last: tuple[int, int] | None = None
    after_pack_no = -1
    while True:
        decoded_packs = await _load_map_packs(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            object_kind=object_kind,
            block_keys=block_keys,
            fragment_nos=fragment_nos,
            has_fragment_filter=has_fragment_filter,
            after_pack_no=after_pack_no,
        )
        if not decoded_packs:
            break
        _assert_pack_page_order(decoded_packs, previous_last)
        metadata_by_hash = await _load_target_metadata(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            target_hashes=_pack_target_hashes(decoded_packs),
        )
        mapping_rows.extend(
            _selected_mapping_rows(
                decoded_packs,
                metadata_by_hash=metadata_by_hash,
                block_keys=block_key_set,
                fragment_nos=fragment_no_set,
                has_fragment_filter=has_fragment_filter,
                row_limit=row_limit - len(mapping_rows),
            )
        )
        after_pack_no = int(decoded_packs[-1][0].get("pack_no") or 0)
        previous_last = _last_pack_coordinate(decoded_packs)
        if len(decoded_packs) < _MAP_PACK_PAGE_ROWS:
            break
    return tuple(mapping_rows)


__all__ = ("load_selected_mapping_rows",)
