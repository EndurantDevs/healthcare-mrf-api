# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stored aggregate-pack authentication for pricing projection v3."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any, Mapping

from sqlalchemy import text

from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePackKey,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    decode_aggregate_pack,
)
from api.plan_pricing_projection_contract import table
from api.plan_pricing_projection_v3_types import ProjectionV3Counts


def _stored_pack_bytes(pack_row_by_field: Mapping[str, Any]) -> bytes:
    encoded_pack = pack_row_by_field["payload"]
    if not isinstance(encoded_pack, (bytes, bytearray, memoryview)):
        raise ValueError("stored aggregate pack payload is invalid")
    encoded_pack = bytes(encoded_pack)
    expected_sha256 = hashlib.sha256(encoded_pack).digest()
    stored_digests = (
        pack_row_by_field["payload_sha256"],
        pack_row_by_field["computed_sha256"],
    )
    if not all(
        isinstance(stored_digest, (bytes, bytearray, memoryview))
        and hmac.compare_digest(bytes(stored_digest), expected_sha256)
        for stored_digest in stored_digests
    ):
        raise ValueError("stored aggregate pack SHA does not match payload")
    return encoded_pack


def _validated_stored_pack(
    projection_id: str,
    raw_pack_row: Mapping[str, Any],
) -> tuple[tuple[str, str, str], int, int, int]:
    pack_row_by_field = dict(raw_pack_row)
    code_identity = AggregateCodeIdentity(
        str(pack_row_by_field["code_system"]),
        str(pack_row_by_field["code"]),
    )
    pack_key = AggregatePackKey(
        projection_id,
        code_identity,
        str(pack_row_by_field["zip_prefix_2"]),
    )
    entry_count = pack_row_by_field["entry_count"]
    raw_byte_count = pack_row_by_field["raw_byte_count"]
    stored_byte_count = pack_row_by_field["stored_byte_count"]
    encoded_pack = _stored_pack_bytes(pack_row_by_field)
    if (
        type(entry_count) is not int
        or type(raw_byte_count) is not int
        or type(stored_byte_count) is not int
        or stored_byte_count != len(encoded_pack)
    ):
        raise ValueError("stored aggregate pack counts are invalid")
    aggregate_pack_raw_byte_count(
        encoded_pack, expected_raw_byte_count=raw_byte_count
    )
    decoded_pack = decode_aggregate_pack(encoded_pack, expected_key=pack_key)
    if (
        len(decoded_pack.records) != entry_count
        or aggregate_logical_digest(code_identity, decoded_pack.records)
        != pack_row_by_field["logical_digest"]
    ):
        raise ValueError("stored aggregate pack logical receipt is invalid")
    return (
        (code_identity.code_system, code_identity.code, pack_key.zip2),
        entry_count,
        raw_byte_count,
        stored_byte_count,
    )


async def validate_stored_aggregate_packs(
    session: Any,
    projection_id: str,
    expected_counts: ProjectionV3Counts,
) -> None:
    """Stream and authenticate every stored pack before candidate sealing."""

    statement = text(
        f"""
        SELECT code_system, code, zip_prefix_2, entry_count,
               raw_byte_count, stored_byte_count, logical_digest,
               payload_sha256, pg_catalog.sha256(payload) AS computed_sha256,
               payload
          FROM {table('plan_pricing_aggregate_pack')}
         WHERE projection_id = :projection_id
         ORDER BY code_system, code, zip_prefix_2
        """
    ).execution_options(yield_per=1)
    aggregate_stream = await session.stream(
        statement, {"projection_id": projection_id}
    )
    actual_counts = [0, 0, 0, 0]
    previous_coordinate: tuple[str, str, str] | None = None
    async for raw_pack_row in aggregate_stream.mappings():
        coordinate, entry_count, raw_byte_count, stored_byte_count = (
            _validated_stored_pack(projection_id, raw_pack_row)
        )
        if previous_coordinate is not None and coordinate <= previous_coordinate:
            raise ValueError("stored aggregate packs are not strictly ordered")
        previous_coordinate = coordinate
        actual_counts[0] += entry_count
        actual_counts[1] += 1
        actual_counts[2] += raw_byte_count
        actual_counts[3] += stored_byte_count
    expected_pack_counts = (
        expected_counts.aggregate_entry_count,
        expected_counts.aggregate_pack_count,
        expected_counts.aggregate_raw_byte_count,
        expected_counts.aggregate_stored_byte_count,
    )
    if tuple(actual_counts) != expected_pack_counts:
        raise ValueError("stored aggregate pack totals do not match builder output")
