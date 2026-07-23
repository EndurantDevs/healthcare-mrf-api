"""Authenticated CAS reachability and attribution for PTG V3/V4 coexistence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from fractions import Fraction
from typing import Any, Mapping, Sequence

import asyncpg

from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_FORMAT_VERSION,
    shared_block_hash,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    decode_v4_snapshot_map_pack,
)
from scripts.ptg_v4_dev_canary_storage_sql import quote_identifier


_HASH_BATCH_SIZE = 2_000
REFERENCE_POPULATION = "published_sealed_layout_keys"


@dataclass
class _ReferenceIndex:
    """Exact snapshots and authenticated metadata expected for each CAS hash."""

    snapshot_keys_by_hash: dict[bytes, set[int]] = field(default_factory=dict)
    metadata_by_hash: dict[bytes, tuple[str, int]] = field(default_factory=dict)

    def add(
        self,
        *,
        snapshot_key: int,
        block_hash: bytes,
        object_kind: str,
        entry_count: int,
    ) -> None:
        """Add one reference while rejecting conflicting hash metadata."""

        normalized_hash = bytes(block_hash)
        normalized_metadata = (str(object_kind), int(entry_count))
        if len(normalized_hash) != 32 or not normalized_metadata[0]:
            raise RuntimeError("PTG CAS reference identity is malformed")
        previous_metadata = self.metadata_by_hash.setdefault(
            normalized_hash,
            normalized_metadata,
        )
        if previous_metadata != normalized_metadata:
            raise RuntimeError("PTG CAS hash has conflicting reference metadata")
        self.snapshot_keys_by_hash.setdefault(normalized_hash, set()).add(
            int(snapshot_key)
        )


@dataclass
class _MapPackSequence:
    """Cross-pack ordering state for the globally ordered map-pack query."""

    scope: tuple[int, str] | None = None
    pack_no: int = -1
    last_coordinate: tuple[int, int] | None = None

    def validate(
        self,
        *,
        snapshot_key: int,
        object_kind: str,
        pack_no: int,
        first_coordinate: tuple[int, int],
        last_coordinate: tuple[int, int],
    ) -> None:
        """Require contiguous packs and strictly increasing coordinate ranges."""

        current_scope = (int(snapshot_key), str(object_kind))
        if current_scope != self.scope:
            if pack_no != 0:
                raise RuntimeError("PTG V4 map object-kind packs must begin at zero")
            self.scope = current_scope
            self.pack_no = 0
            self.last_coordinate = last_coordinate
            return
        if pack_no != self.pack_no + 1:
            raise RuntimeError("PTG V4 map packs are not contiguously numbered")
        if self.last_coordinate is not None and first_coordinate <= self.last_coordinate:
            raise RuntimeError("PTG V4 map pack coordinate ranges overlap")
        self.pack_no = pack_no
        self.last_coordinate = last_coordinate


async def collect_cas_evidence(
    connection: asyncpg.Connection,
    *,
    schema_name: str,
    snapshot_key: int,
    import_started_at: datetime,
    import_finished_at: datetime,
) -> dict[str, Any]:
    """Decode all map packs and attribute every direct or mapped CAS block."""

    references = _ReferenceIndex()
    await _load_direct_references(connection, schema_name, references)
    await _load_map_references(connection, schema_name, references)
    return await _summarize_reference_blocks(
        connection,
        schema_name=schema_name,
        references=references,
        selected_snapshot_key=snapshot_key,
        import_started_at=import_started_at,
        import_finished_at=import_finished_at,
    )


async def _load_direct_references(
    connection: asyncpg.Connection,
    schema_name: str,
    references: _ReferenceIndex,
) -> None:
    """Stream direct V3 references without materializing graph-edge tables."""

    schema = quote_identifier(schema_name)
    direct_cursor = connection.cursor(
        f"""
        WITH live_snapshot_keys AS (
            SELECT DISTINCT binding.snapshot_key
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
             WHERE snapshot.status = 'published'
               AND layout.state = 'sealed'
               AND layout.generation IN ('shared_blocks_v3', 'shared_blocks_v4')
        )
        SELECT reference.snapshot_key, reference.block_hash,
               reference.object_kind, reference.entry_count
          FROM {schema}.ptg2_v3_snapshot_block AS reference
          JOIN live_snapshot_keys AS live
            ON live.snapshot_key = reference.snapshot_key
         ORDER BY reference.snapshot_key, reference.object_kind,
                  reference.block_key, reference.fragment_no
        """
    )
    async for direct_record in direct_cursor:
        references.add(
            snapshot_key=int(direct_record["snapshot_key"]),
            block_hash=bytes(direct_record["block_hash"]),
            object_kind=str(direct_record["object_kind"]),
            entry_count=int(direct_record["entry_count"]),
        )


async def _load_map_references(
    connection: asyncpg.Connection,
    schema_name: str,
    references: _ReferenceIndex,
) -> None:
    """Stream, authenticate, and decode every V4 coordinate-map pack."""

    schema = quote_identifier(schema_name)
    map_cursor = connection.cursor(_map_pack_query(schema))
    sequence = _MapPackSequence()
    async for raw_record in map_cursor:
        map_record_by_field = dict(raw_record)
        coordinates = _validated_map_coordinates(map_record_by_field, sequence)
        snapshot_key = int(map_record_by_field["snapshot_key"])
        references.add(
            snapshot_key=snapshot_key,
            block_hash=bytes(map_record_by_field["map_block_hash"]),
            object_kind=PTG2_V4_MAP_BLOCK_KIND,
            entry_count=len(coordinates),
        )
        for coordinate in coordinates:
            references.add(
                snapshot_key=snapshot_key,
                block_hash=coordinate.block_hash,
                object_kind=coordinate.object_kind,
                entry_count=coordinate.entry_count,
            )


def _map_pack_query(schema: str) -> str:
    """Return the ordered query containing map metadata and CAS payloads."""

    return f"""
        WITH live_snapshot_keys AS (
            SELECT DISTINCT binding.snapshot_key
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
             WHERE snapshot.status = 'published'
               AND layout.state = 'sealed'
               AND layout.generation IN ('shared_blocks_v3', 'shared_blocks_v4')
        )
        SELECT mapping.snapshot_key, mapping.object_kind, mapping.pack_no,
               mapping.first_block_key, mapping.first_fragment_no,
               mapping.last_block_key, mapping.last_fragment_no,
               mapping.coordinate_count, mapping.entry_count,
               mapping.map_block_hash,
               block.format_version AS map_format_version,
               block.object_kind AS map_object_kind,
               block.codec AS map_codec,
               block.entry_count AS map_entry_count,
               block.raw_byte_count AS map_raw_byte_count,
               block.stored_byte_count AS map_stored_byte_count,
               block.payload AS map_payload
          FROM {schema}.ptg2_v4_snapshot_map_pack AS mapping
          JOIN live_snapshot_keys AS live
            ON live.snapshot_key = mapping.snapshot_key
          JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = mapping.map_block_hash
         ORDER BY mapping.snapshot_key, mapping.object_kind, mapping.pack_no
    """


def _validated_map_coordinates(
    map_record: Mapping[str, Any],
    sequence: _MapPackSequence,
) -> tuple[Any, ...]:
    """Authenticate one map CAS block and validate its persisted geometry."""

    map_payload = bytes(map_record.get("map_payload") or b"")
    if (
        int(map_record.get("map_format_version") or -1)
        != PTG2_V3_SHARED_FORMAT_VERSION
        or map_record.get("map_object_kind") != PTG2_V4_MAP_BLOCK_KIND
        or map_record.get("map_codec") != "none"
        or int(map_record.get("map_raw_byte_count") or -1) != len(map_payload)
        or int(map_record.get("map_stored_byte_count") or -1) != len(map_payload)
    ):
        raise RuntimeError("PTG V4 map pack has incompatible CAS metadata")
    expected_hash = shared_block_hash(
        format_version=PTG2_V3_SHARED_FORMAT_VERSION,
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        codec="none",
        payload=map_payload,
    )
    if bytes(map_record.get("map_block_hash") or b"") != expected_hash:
        raise RuntimeError("PTG V4 map pack CAS hash does not match its payload")
    coordinates = decode_v4_snapshot_map_pack(
        map_payload,
        expected_object_kind=str(map_record.get("object_kind") or ""),
    )
    _validate_map_geometry(map_record, coordinates, sequence)
    return coordinates


def _validate_map_geometry(
    map_record: Mapping[str, Any],
    coordinates: Sequence[Any],
    sequence: _MapPackSequence,
) -> None:
    """Reconcile decoded coordinate counts, bounds, entries, and pack order."""

    if not coordinates:
        raise RuntimeError("PTG V4 map pack contains no coordinates")
    first_coordinate = (
        int(coordinates[0].block_key),
        int(coordinates[0].fragment_no),
    )
    last_coordinate = (
        int(coordinates[-1].block_key),
        int(coordinates[-1].fragment_no),
    )
    if first_coordinate != (
        int(map_record.get("first_block_key") or 0),
        int(map_record.get("first_fragment_no") or 0),
    ) or last_coordinate != (
        int(map_record.get("last_block_key") or 0),
        int(map_record.get("last_fragment_no") or 0),
    ):
        raise RuntimeError("PTG V4 map pack coordinate bounds are inconsistent")
    if (
        int(map_record.get("coordinate_count") or 0) != len(coordinates)
        or int(map_record.get("map_entry_count") or 0) != len(coordinates)
        or int(map_record.get("entry_count") or 0)
        != sum(int(coordinate.entry_count) for coordinate in coordinates)
    ):
        raise RuntimeError("PTG V4 map pack counts are inconsistent")
    sequence.validate(
        snapshot_key=int(map_record["snapshot_key"]),
        object_kind=str(map_record["object_kind"]),
        pack_no=int(map_record["pack_no"]),
        first_coordinate=first_coordinate,
        last_coordinate=last_coordinate,
    )


async def _summarize_reference_blocks(
    connection: asyncpg.Connection,
    *,
    schema_name: str,
    references: _ReferenceIndex,
    selected_snapshot_key: int,
    import_started_at: datetime,
    import_finished_at: datetime,
) -> dict[str, Any]:
    """Join exact reachability to CAS rows and calculate weighted attribution."""

    schema = quote_identifier(schema_name)
    global_row_bytes = int(
        await connection.fetchval(
            f"SELECT COALESCE(SUM(pg_column_size(block)), 0)::bigint "
            f"FROM {schema}.ptg2_v3_block AS block"
        )
        or 0
    )
    accumulator_by_field: dict[str, Any] = {
        "allocated_row_bytes": 0,
        "weighted_row_bytes": Fraction(0, 1),
        "selected_block_count": 0,
        "new_block_count": 0,
        "reused_block_count": 0,
        "shared_block_count": 0,
        "object_kind_counts": {},
        "map_block_count": 0,
    }
    observed_hashes: set[bytes] = set()
    block_hashes = tuple(references.snapshot_keys_by_hash)
    for start_index in range(0, len(block_hashes), _HASH_BATCH_SIZE):
        batch_hashes = block_hashes[start_index : start_index + _HASH_BATCH_SIZE]
        block_records = await connection.fetch(
            _reference_block_query(schema),
            list(batch_hashes),
        )
        for block_record in block_records:
            _accumulate_block_record(
                dict(block_record),
                references=references,
                selected_snapshot_key=selected_snapshot_key,
                import_started_at=import_started_at,
                import_finished_at=import_finished_at,
                accumulator_by_field=accumulator_by_field,
                observed_hashes=observed_hashes,
            )
    if observed_hashes != set(block_hashes):
        raise RuntimeError("PTG snapshot references a missing CAS block")
    return _cas_evidence_report(global_row_bytes, accumulator_by_field)


def _reference_block_query(schema: str) -> str:
    """Return the bounded metadata query used for referenced CAS blocks."""

    return f"""
        SELECT block_hash, object_kind, entry_count, created_at,
               pg_column_size(block)::bigint AS row_bytes
          FROM {schema}.ptg2_v3_block AS block
         WHERE block_hash = ANY($1::bytea[])
    """


def _accumulate_block_record(
    block_record: Mapping[str, Any],
    *,
    references: _ReferenceIndex,
    selected_snapshot_key: int,
    import_started_at: datetime,
    import_finished_at: datetime,
    accumulator_by_field: dict[str, Any],
    observed_hashes: set[bytes],
) -> None:
    """Validate one joined block and add its exact attribution contribution."""

    block_hash = bytes(block_record["block_hash"])
    if block_hash in observed_hashes:
        raise RuntimeError("PTG CAS metadata query returned a duplicate block")
    observed_hashes.add(block_hash)
    expected_metadata = references.metadata_by_hash[block_hash]
    actual_metadata = (
        str(block_record["object_kind"]),
        int(block_record["entry_count"]),
    )
    if actual_metadata != expected_metadata:
        raise RuntimeError("PTG CAS block metadata differs from its references")
    row_bytes = int(block_record["row_bytes"])
    snapshot_keys = references.snapshot_keys_by_hash[block_hash]
    accumulator_by_field["allocated_row_bytes"] += row_bytes
    if selected_snapshot_key not in snapshot_keys:
        return
    accumulator_by_field["weighted_row_bytes"] += Fraction(
        row_bytes,
        len(snapshot_keys),
    )
    accumulator_by_field["selected_block_count"] += 1
    if len(snapshot_keys) > 1:
        accumulator_by_field["shared_block_count"] += 1
    _accumulate_block_age(
        block_record["created_at"],
        import_started_at=import_started_at,
        import_finished_at=import_finished_at,
        accumulator_by_field=accumulator_by_field,
    )
    count_by_kind = accumulator_by_field["object_kind_counts"]
    count_by_kind[actual_metadata[0]] = count_by_kind.get(actual_metadata[0], 0) + 1
    if actual_metadata[0] == PTG2_V4_MAP_BLOCK_KIND:
        accumulator_by_field["map_block_count"] += 1


def _accumulate_block_age(
    created_at: datetime,
    *,
    import_started_at: datetime,
    import_finished_at: datetime,
    accumulator_by_field: dict[str, Any],
) -> None:
    """Classify one selected block as reused or created during the import."""

    if created_at < import_started_at:
        accumulator_by_field["reused_block_count"] += 1
    elif created_at <= import_finished_at:
        accumulator_by_field["new_block_count"] += 1
    else:
        raise RuntimeError("PTG snapshot references a CAS block created after import")


def _cas_evidence_report(
    global_row_bytes: int,
    accumulator_by_field: Mapping[str, Any],
) -> dict[str, Any]:
    """Shape reconciled CAS tuple-byte and object-kind evidence."""

    allocated_row_bytes = int(accumulator_by_field["allocated_row_bytes"])
    unallocated_row_bytes = global_row_bytes - allocated_row_bytes
    if unallocated_row_bytes < 0:
        raise RuntimeError("PTG CAS attribution exceeds global tuple bytes")
    weighted = accumulator_by_field["weighted_row_bytes"]
    weighted_row_bytes = (weighted.numerator * 2 + weighted.denominator) // (
        weighted.denominator * 2
    )
    return {
        "bytes": {
            "global_row_bytes": global_row_bytes,
            "snapshot_weighted_row_bytes": weighted_row_bytes,
            "snapshot_row_count": int(
                accumulator_by_field["selected_block_count"]
            ),
            "distinct_referenced_block_count": int(
                accumulator_by_field["selected_block_count"]
            ),
            "new_during_import_block_count": int(
                accumulator_by_field["new_block_count"]
            ),
            "preexisting_reused_block_count": int(
                accumulator_by_field["reused_block_count"]
            ),
            "shared_block_count": int(accumulator_by_field["shared_block_count"]),
            "allocated_all_snapshot_row_bytes": allocated_row_bytes,
            "unallocated_row_bytes": unallocated_row_bytes,
            "row_reconciliation_delta_bytes": 0,
        },
        "object_kind_counts": dict(accumulator_by_field["object_kind_counts"]),
        "map_cas_block_count": int(accumulator_by_field["map_block_count"]),
        "reference_source": "direct_rows_plus_authenticated_v4_map_payloads",
        "reference_population": REFERENCE_POPULATION,
    }
