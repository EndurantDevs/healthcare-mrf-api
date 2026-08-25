# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded range reads over authenticated packed-finalizer mappings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_PACK_TABLE,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET,
    FinalizerMapError,
    _decode_map_packs,
    _load_target_metadata,
    _row_mapping,
    has_complete_v4_finalizer_map,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
    V4SnapshotMapCoordinate,
)


PTG2_V4_FINALIZER_RANGE_PACK_BATCH_ROWS = 128

_MAP_PACK_RANGE_BATCH_SQL = """
    WITH requested_range(range_key, lower_bound, upper_bound) AS (
        SELECT *
          FROM unnest(
              CAST(:range_keys AS bigint[]),
              CAST(:lower_bounds AS bigint[]),
              CAST(:upper_bounds AS bigint[])
          )
    )
    SELECT requested_range.range_key,
           pack.object_kind, pack.pack_no, pack.first_block_key,
           pack.first_fragment_no, pack.last_block_key, pack.last_fragment_no,
           pack.coordinate_count, pack.entry_count, pack.logical_byte_count,
           pack.map_block_hash, block.format_version AS map_format_version,
           block.object_kind AS map_object_kind, block.codec AS map_codec,
           block.entry_count AS map_entry_count,
           block.raw_byte_count AS map_raw_byte_count,
           block.stored_byte_count AS map_stored_byte_count,
           block.payload AS map_payload
      FROM requested_range
      JOIN {schema}.{pack_table} AS pack
        ON pack.snapshot_key = :snapshot_key
       AND pack.object_kind = :object_kind
       AND pack.last_block_key >= requested_range.lower_bound
       AND pack.first_block_key <= requested_range.upper_bound
      JOIN {schema}.ptg2_v3_block AS block
        ON block.block_hash = pack.map_block_hash
     WHERE NOT :has_cursor
        OR ROW(requested_range.range_key, pack.pack_no)
           > ROW(:last_range_key, :last_pack_no)
     ORDER BY requested_range.range_key, pack.pack_no
     LIMIT :batch_rows
"""

_DecodedRangePack = tuple[
    int,
    dict[str, Any],
    tuple[V4SnapshotMapCoordinate, ...],
]


@dataclass
class _RangeReadState:
    ranges: tuple[tuple[int, int, int], ...]
    pack_limit: int | None
    bounds_by_range: dict[int, tuple[int, int]]
    keys_by_range: dict[int, set[int]]
    previous_pack_by_range: dict[int, int] = field(default_factory=dict)
    previous_last_by_range: dict[int, tuple[int, int]] = field(default_factory=dict)
    last_coordinate: tuple[int, int] | None = None
    processed_pack_rows: int = 0

    @classmethod
    def create(
        cls,
        ranges: tuple[tuple[int, int, int], ...],
        pack_limit: int | None,
    ) -> _RangeReadState:
        """Build lookup indexes for one bounded range read."""

        return cls(
            ranges=ranges,
            pack_limit=pack_limit,
            bounds_by_range={
                range_key: (lower_bound, upper_bound)
                for range_key, lower_bound, upper_bound in ranges
            },
            keys_by_range={range_key: set() for range_key, _lower, _upper in ranges},
        )

    def has_more(self) -> bool:
        """Return whether the caller may load another pack page."""

        return self.pack_limit is None or self.processed_pack_rows < self.pack_limit

    def next_batch_rows(self) -> int:
        """Return the next page size without exceeding the caller's limit."""

        if self.pack_limit is None:
            return PTG2_V4_FINALIZER_RANGE_PACK_BATCH_ROWS
        return min(
            PTG2_V4_FINALIZER_RANGE_PACK_BATCH_ROWS,
            self.pack_limit - self.processed_pack_rows,
        )

    def result(self) -> dict[int, tuple[int, ...]]:
        """Return deterministic block-key tuples grouped by requested range."""

        return {
            range_key: tuple(sorted(block_keys))
            for range_key, block_keys in self.keys_by_range.items()
        }


def _normalized_map_ranges(
    ranges: Iterable[tuple[int, int, int]],
) -> tuple[tuple[int, int, int], ...]:
    normalized_ranges: list[tuple[int, int, int]] = []
    observed_range_keys: set[int] = set()
    for raw_range in ranges:
        try:
            raw_range_key, raw_lower_bound, raw_upper_bound = raw_range
        except (TypeError, ValueError) as exc:
            raise ValueError("packed finalizer map range is invalid") from exc
        raw_values = (raw_range_key, raw_lower_bound, raw_upper_bound)
        if any(isinstance(value, bool) for value in raw_values):
            raise ValueError("packed finalizer map range is invalid")
        try:
            range_key, lower_bound, upper_bound = map(int, raw_values)
        except (TypeError, ValueError) as exc:
            raise ValueError("packed finalizer map range is invalid") from exc
        if (
            min(range_key, lower_bound, upper_bound) < 0
            or lower_bound > upper_bound
            or range_key in observed_range_keys
        ):
            raise ValueError("packed finalizer map range is invalid")
        observed_range_keys.add(range_key)
        normalized_ranges.append((range_key, lower_bound, upper_bound))
    return tuple(normalized_ranges)


def _normalized_range_pack_limit(maximum_pack_rows: int | None) -> int | None:
    if maximum_pack_rows is None:
        return None
    if isinstance(maximum_pack_rows, bool) or int(maximum_pack_rows) <= 0:
        raise ValueError("packed finalizer map pack limit must be positive")
    return int(maximum_pack_rows)


async def _load_range_pack_batch(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    object_kind: str,
    state: _RangeReadState,
    batch_rows: int,
) -> tuple[dict[str, Any], ...]:
    query_result = await session.execute(
        text(
            _MAP_PACK_RANGE_BATCH_SQL.format(
                schema=schema,
                pack_table=_quote_ident(PTG2_V4_FINALIZER_MAP_PACK_TABLE),
            )
        ),
        {
            "snapshot_key": snapshot_key,
            "object_kind": object_kind,
            "range_keys": tuple(range_spec[0] for range_spec in state.ranges),
            "lower_bounds": tuple(range_spec[1] for range_spec in state.ranges),
            "upper_bounds": tuple(range_spec[2] for range_spec in state.ranges),
            "has_cursor": state.last_coordinate is not None,
            "last_range_key": state.last_coordinate[0] if state.last_coordinate else 0,
            "last_pack_no": state.last_coordinate[1] if state.last_coordinate else 0,
            "batch_rows": batch_rows,
        },
    )
    pack_records = tuple(_row_mapping(pack_row) for pack_row in query_result)
    if len(pack_records) > batch_rows:
        raise FinalizerMapError("packed finalizer map range batch exceeded its limit")
    return pack_records


def _decode_range_pack_batch(
    pack_records: Sequence[Mapping[str, Any]],
    *,
    object_kind: str,
    state: _RangeReadState,
) -> tuple[tuple[_DecodedRangePack, ...], tuple[int, int]]:
    decoded_packs: list[_DecodedRangePack] = []
    current_coordinate = state.last_coordinate
    for raw_record in pack_records:
        pack_field_map = dict(raw_record)
        try:
            range_key = int(pack_field_map.get("range_key"))
            pack_no = int(pack_field_map.get("pack_no"))
        except (TypeError, ValueError) as exc:
            raise FinalizerMapError(
                "packed finalizer map range query returned an invalid coordinate"
            ) from exc
        query_coordinate = (range_key, pack_no)
        if range_key not in state.bounds_by_range or (
            current_coordinate is not None and query_coordinate <= current_coordinate
        ):
            raise FinalizerMapError(
                "packed finalizer map range query returned an unexpected pack"
            )
        coordinates = _decode_map_packs((pack_field_map,), object_kind=object_kind)[0][1]
        if len(coordinates) > PTG2_V4_DEFAULT_COORDINATES_PER_PACK:
            raise FinalizerMapError("packed finalizer map range pack is oversized")
        first_coordinate = (coordinates[0].block_key, coordinates[0].fragment_no)
        previous_pack_no = state.previous_pack_by_range.get(range_key, -1)
        previous_last = state.previous_last_by_range.get(range_key)
        if pack_no <= previous_pack_no or (
            previous_last is not None and first_coordinate <= previous_last
        ):
            raise FinalizerMapError("packed finalizer map pack ranges overlap")
        state.previous_pack_by_range[range_key] = pack_no
        state.previous_last_by_range[range_key] = (
            coordinates[-1].block_key,
            coordinates[-1].fragment_no,
        )
        current_coordinate = query_coordinate
        decoded_packs.append((range_key, pack_field_map, coordinates))
    if current_coordinate is None:
        raise FinalizerMapError("packed finalizer map range batch is empty")
    return tuple(decoded_packs), current_coordinate


def _append_validated_range_keys(
    decoded_packs: Sequence[_DecodedRangePack],
    *,
    metadata_by_hash: Mapping[bytes, tuple[str, int, int]],
    state: _RangeReadState,
    claim_block_key: Callable[[int, int], None] | None,
) -> None:
    for range_key, pack_fields, coordinates in decoded_packs:
        logical_byte_count = 0
        lower_bound, upper_bound = state.bounds_by_range[range_key]
        retained_keys = state.keys_by_range[range_key]
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
            block_key = int(coordinate.block_key)
            if lower_bound <= block_key <= upper_bound and block_key not in retained_keys:
                if claim_block_key is not None:
                    claim_block_key(range_key, block_key)
                retained_keys.add(block_key)
        if logical_byte_count != int(pack_fields.get("logical_byte_count") or 0):
            raise FinalizerMapError(
                "packed finalizer map logical byte count is inconsistent"
            )


async def _consume_range_pack_batch(
    session: Any,
    *,
    schema: str,
    snapshot_key: int,
    decoded_packs: Sequence[_DecodedRangePack],
    state: _RangeReadState,
    claim_block_key: Callable[[int, int], None] | None,
) -> None:
    target_hashes = {
        bytes(coordinate.block_hash)
        for _range_key, _pack_fields, coordinates in decoded_packs
        for coordinate in coordinates
    }
    metadata_by_hash = await _load_target_metadata(
        session,
        schema=schema,
        snapshot_key=snapshot_key,
        target_hashes=target_hashes,
    )
    _append_validated_range_keys(
        decoded_packs,
        metadata_by_hash=metadata_by_hash,
        state=state,
        claim_block_key=claim_block_key,
    )


async def _load_range_pack_pages(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    state: _RangeReadState,
    claim_block_key: Callable[[int, int], None] | None,
) -> None:
    schema = _quote_ident(schema_name)
    while state.has_more():
        batch_rows = state.next_batch_rows()
        pack_records = await _load_range_pack_batch(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            object_kind=object_kind,
            state=state,
            batch_rows=batch_rows,
        )
        if not pack_records:
            return
        decoded_packs, next_coordinate = _decode_range_pack_batch(
            pack_records,
            object_kind=object_kind,
            state=state,
        )
        await _consume_range_pack_batch(
            session,
            schema=schema,
            snapshot_key=snapshot_key,
            decoded_packs=decoded_packs,
            state=state,
            claim_block_key=claim_block_key,
        )
        state.last_coordinate = next_coordinate
        state.processed_pack_rows += len(pack_records)
        if len(pack_records) < batch_rows:
            return


async def load_v4_finalizer_range_keys(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    object_kind: str,
    ranges: Iterable[tuple[int, int, int]],
    maximum_pack_rows: int | None = None,
    claim_block_key: Callable[[int, int], None] | None = None,
) -> dict[int, tuple[int, ...]] | None:
    """Return authenticated block keys through bounded map and target batches."""

    normalized_kind = str(object_kind)
    if normalized_kind not in PTG2_V4_FINALIZER_PACKED_OBJECT_KIND_SET:
        return None
    normalized_snapshot_key = int(snapshot_key)
    if not await has_complete_v4_finalizer_map(
        session,
        schema_name=schema_name,
        snapshot_key=normalized_snapshot_key,
    ):
        return None
    normalized_ranges = _normalized_map_ranges(ranges)
    pack_limit = _normalized_range_pack_limit(maximum_pack_rows)
    if claim_block_key is not None and not callable(claim_block_key):
        raise ValueError("packed finalizer map retention callback is invalid")
    state = _RangeReadState.create(normalized_ranges, pack_limit)
    if not normalized_ranges:
        return state.result()
    await _load_range_pack_pages(
        session,
        schema_name=schema_name,
        snapshot_key=normalized_snapshot_key,
        object_kind=normalized_kind,
        state=state,
        claim_block_key=claim_block_key,
    )
    return state.result()
