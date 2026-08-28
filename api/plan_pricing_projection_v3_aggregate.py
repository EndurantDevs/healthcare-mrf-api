# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact aggregate SQL, packing, and prewarm selection for projection v3."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePack,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    encode_aggregate_pack,
)
from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_materialize import digest_row
from api.plan_pricing_projection_v3_types import (
    _BuildState,
    _insert_batches,
    _ordered_prewarm_shapes,
    _retain_prewarm_shape,
)


MAX_CODE_AGGREGATE_WORK_ROWS = 200_000_000
MAX_PROJECTION_AGGREGATE_WORK_ROWS = 50_000_000_000


_AGGREGATE_STATS_SQL = """
        WITH provider_stats AS MATERIALIZED (
            SELECT geo_cell, COUNT(DISTINCT npi)::bigint AS provider_count
              FROM plan_pricing_eligible_member_cell_stage
             GROUP BY geo_cell
        ), rate_frequency AS MATERIALIZED (
            SELECT cell.geo_cell, price.negotiated_rate,
                   SUM(occurrence.occurrence_count
                       * price.rate_multiplicity)::bigint AS frequency
              FROM plan_pricing_set_cell_stage cell
              JOIN plan_pricing_code_occurrence_stage occurrence
                USING (binding_ordinal, provider_set_key)
              JOIN plan_pricing_price_rate_stage price
                ON price.binding_ordinal = occurrence.binding_ordinal
               AND price.price_set_id = occurrence.price_set_id
             GROUP BY cell.geo_cell, price.negotiated_rate
        ), ranked AS MATERIALIZED (
            SELECT geo_cell, negotiated_rate, frequency,
                   SUM(frequency) OVER (PARTITION BY geo_cell)::bigint AS total,
                   SUM(frequency) OVER (
                       PARTITION BY geo_cell ORDER BY negotiated_rate
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   )::bigint AS cumulative
              FROM rate_frequency
        )
        SELECT ranked.geo_cell, provider.provider_count,
               MAX(ranked.total)::bigint AS rate_count,
               MIN(ranked.negotiated_rate) AS minimum_negotiated_rate,
               MIN(ranked.negotiated_rate) FILTER (
                   WHERE ranked.cumulative >= (ranked.total + 1) / 2
               ) AS median_lower,
               MIN(ranked.negotiated_rate) FILTER (
                   WHERE ranked.cumulative >= (ranked.total + 2) / 2
               ) AS median_upper,
               MAX(ranked.negotiated_rate) AS maximum_negotiated_rate
          FROM ranked
          JOIN provider_stats provider USING (geo_cell)
         GROUP BY ranked.geo_cell, provider.provider_count
         ORDER BY ranked.geo_cell
    """


async def _aggregate_records(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState | None = None,
    *,
    aggregate_stats_sql: str = _AGGREGATE_STATS_SQL,
) -> tuple[AggregateZipRecord, ...]:
    """Return exact ZIP aggregates from preflighted per-code stages."""

    aggregate_result = await session.execute(text(aggregate_stats_sql))
    return _aggregate_zip_records(aggregate_result.mappings())


def _aggregate_zip_records(
    aggregate_rows: Iterable[Mapping[str, Any]],
) -> tuple[AggregateZipRecord, ...]:
    """Convert exact weighted-median query rows into immutable pack records."""

    aggregate_records: list[AggregateZipRecord] = []
    for aggregate_row in aggregate_rows:
        median = (
            Decimal(aggregate_row["median_lower"])
            + Decimal(aggregate_row["median_upper"])
        ) / 2
        aggregate_records.append(
            AggregateZipRecord(
                zip5=str(aggregate_row["geo_cell"]),
                provider_count=int(aggregate_row["provider_count"]),
                rate_count=int(aggregate_row["rate_count"]),
                minimum_negotiated_rate=Decimal(
                    aggregate_row["minimum_negotiated_rate"]
                ),
                median_negotiated_rate=median,
                maximum_negotiated_rate=Decimal(
                    aggregate_row["maximum_negotiated_rate"]
                ),
            )
        )
    return tuple(aggregate_records)


def _aggregate_pack_row(
    projection_id: str,
    code: AggregateCodeIdentity,
    zip_prefix_2: str,
    prefix_records: list[AggregateZipRecord],
) -> dict[str, Any]:
    aggregate_pack = AggregatePack(
        AggregatePackKey(projection_id, code, zip_prefix_2),
        tuple(prefix_records),
    )
    encoded_pack = encode_aggregate_pack(aggregate_pack)
    return {
        "projection_id": projection_id,
        "code_system": code.code_system,
        "code": code.code,
        "zip_prefix_2": zip_prefix_2,
        "entry_count": len(aggregate_pack.records),
        "raw_byte_count": aggregate_pack_raw_byte_count(encoded_pack),
        "stored_byte_count": len(encoded_pack),
        "logical_digest": aggregate_logical_digest(
            code, aggregate_pack.records
        ),
        "payload": encoded_pack,
    }


async def _store_aggregate_packs(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    aggregate_records: tuple[AggregateZipRecord, ...],
    state: _BuildState,
    *,
    aggregate_pack_row: Any = _aggregate_pack_row,
    retain_prewarm_shape: Any = _retain_prewarm_shape,
    insert_batches: Any = _insert_batches,
) -> None:
    if not aggregate_records:
        return
    code = AggregateCodeIdentity(*code_identity)
    code_digest = aggregate_logical_digest(code, aggregate_records)
    digest_row(
        state.content_digest,
        "aggregate-code",
        code_identity,
        code_digest.encode("ascii"),
    )
    records_by_prefix: dict[str, list[AggregateZipRecord]] = defaultdict(list)
    for aggregate_record in aggregate_records:
        records_by_prefix[aggregate_record.zip5[:2]].append(aggregate_record)
        retain_prewarm_shape(
            state.prewarm_heap, code_identity, aggregate_record
        )
    pack_rows: list[dict[str, Any]] = []
    for prefix, prefix_records in sorted(records_by_prefix.items()):
        pack_row_by_field = aggregate_pack_row(
            projection_id, code, prefix, prefix_records
        )
        pack_rows.append(pack_row_by_field)
        state.aggregate_pack_count += 1
        state.aggregate_raw_byte_count += int(
            pack_row_by_field["raw_byte_count"]
        )
        state.aggregate_stored_byte_count += int(
            pack_row_by_field["stored_byte_count"]
        )
    state.aggregate_entry_count += len(aggregate_records)
    await insert_batches(
        session,
        f"""
        INSERT INTO {table('plan_pricing_aggregate_pack')} (
            projection_id, code_system, code, zip_prefix_2, entry_count,
            raw_byte_count, stored_byte_count, logical_digest,
            payload_sha256, payload
        ) VALUES (
            :projection_id, :code_system, :code, :zip_prefix_2, :entry_count,
            :raw_byte_count, :stored_byte_count, :logical_digest,
            pg_catalog.sha256(:payload), :payload
        )
        """,
        pack_rows,
    )


async def _store_prewarm_shapes(
    session: Any,
    projection_id: str,
    state: _BuildState,
    *,
    ordered_prewarm_shapes: Any = _ordered_prewarm_shapes,
    insert_batches: Any = _insert_batches,
) -> int:
    shapes = ordered_prewarm_shapes(state.prewarm_heap)
    shape_rows = [
        {
            "projection_id": projection_id,
            "shape_rank": rank,
            "code_system": shape.code_system,
            "code": shape.code,
            "geo_cell": shape.geo_cell,
            "provider_count": shape.provider_count,
        }
        for rank, shape in enumerate(shapes, start=1)
    ]
    for shape_row_by_field in shape_rows:
        digest_row(
            state.content_digest,
            "prewarm-shape",
            (
                shape_row_by_field["shape_rank"],
                shape_row_by_field["code_system"],
                shape_row_by_field["code"],
                shape_row_by_field["geo_cell"],
                shape_row_by_field["provider_count"],
            ),
            b"",
        )
    await insert_batches(
        session,
        f"""
        INSERT INTO {table('plan_pricing_prewarm_shape')} (
            projection_id, shape_rank, code_system, code, geo_cell,
            provider_count
        ) VALUES (
            :projection_id, :shape_rank, :code_system, :code, :geo_cell,
            :provider_count
        )
        """,
        shape_rows,
    )
    return len(shapes)
