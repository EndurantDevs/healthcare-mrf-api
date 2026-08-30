# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read bounded immutable ZIP2 aggregate packs."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any, Mapping

import orjson
from sqlalchemy import text

from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePackError,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    decode_aggregate_pack,
)
from api.plan_pricing_projection_contract import (
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
    table,
)
from api.plan_pricing_projection_materialize import rate_fragment


MAX_AGGREGATE_REQUEST_PREFIXES = 16
MAX_AGGREGATE_REQUEST_STORED_BYTES = 9 * 1024 * 1024


def _aggregate_fragment(record: AggregateZipRecord) -> orjson.Fragment:
    return orjson.Fragment(
        orjson.dumps(
            {
                "geo_cell": record.zip5,
                "provider_count": record.provider_count,
                "rate_count": record.rate_count,
                "minimum_negotiated_rate": rate_fragment(
                    record.minimum_negotiated_rate
                ),
                "median_negotiated_rate": rate_fragment(
                    record.median_negotiated_rate
                ),
                "maximum_negotiated_rate": rate_fragment(
                    record.maximum_negotiated_rate
                ),
            }
        )
    )


def _verified_payload(aggregate_row_by_field: Mapping[str, Any]) -> bytes:
    encoded_payload = bytes(aggregate_row_by_field["payload"])
    if (
        int(aggregate_row_by_field["stored_byte_count"])
        != len(encoded_payload)
        or not hmac.compare_digest(
            bytes(aggregate_row_by_field["payload_sha256"]),
            hashlib.sha256(encoded_payload).digest(),
        )
    ):
        raise PlanPricingProjectionUnavailable(
            "the selected aggregate projection payload is invalid"
        )
    return encoded_payload


def _decoded_records(
    request: Any,
    code_identity: AggregateCodeIdentity,
    aggregate_row_by_field: Mapping[str, Any],
) -> tuple[AggregateZipRecord, ...]:
    encoded_payload = _verified_payload(aggregate_row_by_field)
    aggregate_pack_key = AggregatePackKey(
        request.projection_id,
        code_identity,
        str(aggregate_row_by_field["zip_prefix_2"]),
    )
    try:
        aggregate_pack_raw_byte_count(
            encoded_payload,
            expected_raw_byte_count=int(
                aggregate_row_by_field["raw_byte_count"]
            ),
        )
        decoded_pack = decode_aggregate_pack(
            encoded_payload,
            expected_key=aggregate_pack_key,
        )
    except AggregatePackError as exc:
        raise PlanPricingProjectionUnavailable(
            "the selected aggregate projection is invalid"
        ) from exc
    if (
        len(decoded_pack.records) != int(aggregate_row_by_field["entry_count"])
        or aggregate_logical_digest(code_identity, decoded_pack.records)
        != str(aggregate_row_by_field["logical_digest"])
    ):
        raise PlanPricingProjectionUnavailable(
            "the selected aggregate projection receipt is invalid"
        )
    return decoded_pack.records


def _selected_records(
    aggregate_rows: Any,
    request: Any,
    selected_geo_cells: frozenset[str],
) -> list[AggregateZipRecord]:
    code_identity = AggregateCodeIdentity(request.code_system, request.code)
    selected_records: list[AggregateZipRecord] = []
    selected_payload_bytes = 0
    for raw_aggregate_row in aggregate_rows:
        aggregate_row_by_field = dict(raw_aggregate_row)
        selected_payload_bytes += int(
            aggregate_row_by_field["stored_byte_count"]
        )
        if selected_payload_bytes > MAX_AGGREGATE_REQUEST_STORED_BYTES:
            raise PlanPricingProjectionUnsupported(
                "aggregate projection exceeds its request byte bound"
            )
        selected_records.extend(
            record
            for record in _decoded_records(
                request, code_identity, aggregate_row_by_field
            )
            if record.zip5 in selected_geo_cells
        )
    return selected_records


async def read_aggregate_pack_page(
    session: Any,
    request: Any,
    geo_cells: list[str],
    args: Mapping[str, Any],
    pagination: Any,
) -> tuple[list[orjson.Fragment], int]:
    """Read one exact page while retaining only requested decoded cells."""

    zip_prefixes = sorted({geo_cell[:2] for geo_cell in geo_cells})
    if len(zip_prefixes) > MAX_AGGREGATE_REQUEST_PREFIXES:
        raise PlanPricingProjectionUnsupported(
            "aggregate projection spans too many ZIP prefixes"
        )
    aggregate_result = await session.execute(
        text(
            f"""
            SELECT zip_prefix_2, entry_count, raw_byte_count,
                   stored_byte_count, logical_digest, payload_sha256, payload
              FROM {table('plan_pricing_aggregate_pack')}
             WHERE projection_id = :projection_id
               AND code_system = :code_system AND code = :code
               AND zip_prefix_2 = ANY(CAST(:zip_prefixes AS varchar[]))
             ORDER BY zip_prefix_2
            """
        ),
        {
            "projection_id": request.projection_id,
            "code_system": request.code_system,
            "code": request.code,
            "zip_prefixes": zip_prefixes,
        },
    )
    page_records = _selected_records(
        aggregate_result.mappings(), request, frozenset(geo_cells)
    )
    is_descending_order = (
        str(args.get("order") or "asc").strip().lower() == "desc"
    )
    page_records.sort(key=lambda aggregate_record: aggregate_record.zip5)
    page_records.sort(
        key=lambda aggregate_record: aggregate_record.minimum_negotiated_rate,
        reverse=is_descending_order,
    )
    result_count = len(page_records)
    page_start = int(pagination.offset)
    page_records = page_records[
        page_start : page_start + int(pagination.limit)
    ]
    return [
        _aggregate_fragment(aggregate_record)
        for aggregate_record in page_records
    ], result_count
