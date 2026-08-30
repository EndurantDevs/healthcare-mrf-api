# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
import random
import zlib
from dataclasses import replace
from decimal import Decimal
from itertools import chain

import pytest

from api import plan_pricing_aggregate_pack as packs


PROJECTION_ID = "a" * 64
CODE_IDENTITY = packs.AggregateCodeIdentity("CPT", "27447")


def _record(
    zip5: str,
    *,
    provider_count: int = 2,
    rate_count: int = 3,
    minimum: object = "1.00",
    median: object = "2.500",
    maximum: object = "10.0",
) -> packs.AggregateZipRecord:
    return packs.AggregateZipRecord(
        zip5=zip5,
        provider_count=provider_count,
        rate_count=rate_count,
        minimum_negotiated_rate=minimum,
        median_negotiated_rate=median,
        maximum_negotiated_rate=maximum,
    )


def _pack(*records: packs.AggregateZipRecord) -> packs.AggregatePack:
    return packs.AggregatePack(
        packs.AggregatePackKey(PROJECTION_ID, CODE_IDENTITY, "10"),
        tuple(records),
    )


def _frame(raw_payload: bytes, *, raw_byte_count: int | None = None) -> bytes:
    return packs._HEADER.pack(
        packs._MAGIC,
        len(raw_payload) if raw_byte_count is None else raw_byte_count,
        hashlib.sha256(raw_payload).digest(),
    ) + zlib.compress(raw_payload)


def test_pack_round_trip_is_deterministic_and_canonical() -> None:
    aggregate_pack = _pack(_record("10001"), _record("10002", median="3"))

    encoded = packs.encode_aggregate_pack(aggregate_pack)
    decoded = packs.decode_aggregate_pack(encoded, expected_key=aggregate_pack.key)

    assert decoded == aggregate_pack
    assert packs.encode_aggregate_pack(decoded) == encoded
    assert decoded.records[0].minimum_negotiated_rate == Decimal("1")
    assert decoded.records[0].median_negotiated_rate == Decimal("2.5")


def test_pack_raw_byte_count_verifies_sql_row_metadata() -> None:
    encoded = packs.encode_aggregate_pack(_pack(_record("10001")))
    raw_byte_count = packs.aggregate_pack_raw_byte_count(encoded)

    assert packs.aggregate_pack_raw_byte_count(
        encoded,
        expected_raw_byte_count=raw_byte_count,
    ) == raw_byte_count
    with pytest.raises(packs.AggregatePackError, match="does not match its SQL row"):
        packs.aggregate_pack_raw_byte_count(
            encoded,
            expected_raw_byte_count=raw_byte_count + 1,
        )


def test_logical_digest_ignores_pack_and_compression_boundaries() -> None:
    records = (_record("10001"), _record("10002"), _record("10003"))
    first_partition = (_pack(*records[:1]), _pack(*records[1:]))
    second_partition = (_pack(*records[:2]), _pack(*records[2:]))

    first_digest = packs.aggregate_logical_digest(
        CODE_IDENTITY,
        chain.from_iterable(pack.records for pack in first_partition),
    )
    second_digest = packs.aggregate_logical_digest(
        CODE_IDENTITY,
        chain.from_iterable(pack.records for pack in second_partition),
    )

    assert first_digest == second_digest
    assert first_digest != packs.aggregate_logical_digest(
        packs.AggregateCodeIdentity("CPT", "27446"), records
    )


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ((_record("10001"), _record("10001")), "strictly ordered"),
        ((_record("10002"), _record("10001")), "strictly ordered"),
        ((_record("11001"),), "outside its ZIP2"),
        ((), "record count"),
    ],
)
def test_pack_rejects_invalid_record_layout(records, message: str) -> None:
    with pytest.raises(packs.AggregatePackError, match=message):
        _pack(*records)


def test_pack_rejects_more_than_one_thousand_records_before_zip_validation() -> None:
    records = tuple(_record(f"{ordinal:05d}") for ordinal in range(1_000)) + (
        _record("00999"),
    )

    with pytest.raises(packs.AggregatePackError, match="record count"):
        _pack(*records)


def test_pack_accepts_the_exact_one_thousand_record_bound() -> None:
    records = tuple(_record(f"10{ordinal:03d}") for ordinal in range(1_000))
    aggregate_pack = _pack(*records)

    assert packs.decode_aggregate_pack(
        packs.encode_aggregate_pack(aggregate_pack)
    ) == aggregate_pack


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"provider_count": 0}, "provider_count"),
        ({"provider_count": True}, "provider_count"),
        ({"rate_count": 2**63}, "rate_count"),
        ({"minimum": "NaN"}, "finite and non-negative"),
        ({"minimum": "-1"}, "finite and non-negative"),
        ({"minimum": 1.5}, "exact decimal"),
        ({"minimum": "1" * 65}, "canonical decimal bound"),
        ({"minimum": "1e999999999"}, "canonical decimal bound"),
        ({"minimum": "3", "median": "2"}, "not ordered"),
    ],
)
def test_record_rejects_invalid_counts_and_decimals(changes, message: str) -> None:
    with pytest.raises(packs.AggregatePackError, match=message):
        _record("10001", **changes)


@pytest.mark.parametrize(
    "key",
    [
        lambda: packs.AggregatePackKey("bad", CODE_IDENTITY, "10"),
        lambda: packs.AggregatePackKey(PROJECTION_ID, CODE_IDENTITY, "1A"),
        lambda: packs.AggregatePackKey(
            PROJECTION_ID,
            packs.AggregateCodeIdentity("HCPCS", "27447"),
            "10",
        ),
        lambda: packs.AggregatePackKey(
            PROJECTION_ID,
            packs.AggregateCodeIdentity("cpt", "27447"),
            "10",
        ),
    ],
)
def test_pack_key_rejects_invalid_or_noncanonical_identity(key) -> None:
    with pytest.raises(packs.AggregatePackError):
        key()


def test_decoder_rejects_corruption_trailing_data_and_wrong_key() -> None:
    aggregate_pack = _pack(_record("10001"))
    encoded = bytearray(packs.encode_aggregate_pack(aggregate_pack))
    encoded[-1] ^= 1

    with pytest.raises(packs.AggregatePackError, match="compression|checksum"):
        packs.decode_aggregate_pack(bytes(encoded))
    checksum_drift = bytearray(packs.encode_aggregate_pack(aggregate_pack))
    checksum_drift[12] ^= 1
    with pytest.raises(packs.AggregatePackError, match="checksum"):
        packs.decode_aggregate_pack(bytes(checksum_drift))
    with pytest.raises(packs.AggregatePackError, match="compression"):
        packs.decode_aggregate_pack(
            packs.encode_aggregate_pack(aggregate_pack) + b"trailing"
        )
    with pytest.raises(packs.AggregatePackError, match="does not match"):
        packs.decode_aggregate_pack(
            packs.encode_aggregate_pack(aggregate_pack),
            expected_key=replace(
                aggregate_pack.key,
                projection_id="b" * 64,
            ),
        )


def test_decoder_rejects_decompression_bombs_and_oversized_headers() -> None:
    bomb = b"x" * (packs.MAX_AGGREGATE_PACK_DECODED_BYTES + 1)
    oversized_header = packs._HEADER.pack(
        packs._MAGIC,
        packs.MAX_AGGREGATE_PACK_DECODED_BYTES + 1,
        hashlib.sha256(bomb).digest(),
    ) + zlib.compress(bomb)

    with pytest.raises(packs.AggregatePackError, match="header"):
        packs.decode_aggregate_pack(oversized_header)
    with pytest.raises(packs.AggregatePackError, match="compression"):
        packs.decode_aggregate_pack(
            _frame(bomb, raw_byte_count=packs.MAX_AGGREGATE_PACK_DECODED_BYTES)
        )


def test_incompressible_max_frame_fits_the_sql_bound() -> None:
    raw_payload = random.Random(27447).randbytes(
        packs.MAX_AGGREGATE_PACK_DECODED_BYTES
    )
    encoded_pack = _frame(raw_payload)

    assert packs._MAX_ENCODED_BYTES == 558_124
    assert len(encoded_pack) <= packs._MAX_ENCODED_BYTES
    assert packs.aggregate_pack_raw_byte_count(encoded_pack) == len(raw_payload)
    with pytest.raises(packs.AggregatePackError, match="invalid JSON"):
        packs.decode_aggregate_pack(encoded_pack)
    with pytest.raises(packs.AggregatePackError, match="byte count"):
        packs.aggregate_pack_raw_byte_count(
            encoded_pack + bytes(packs._MAX_ENCODED_BYTES - len(encoded_pack) + 1)
        )


def test_decoder_rejects_noncanonical_and_invalid_record_payloads() -> None:
    raw_pack_by_field = {
        "code": "27447",
        "code_system": "CPT",
        "projection_id": PROJECTION_ID,
        "records": [["10001", 1, 1, "1.00", "1", "1"]],
        "version": packs.AGGREGATE_PACK_FORMAT_VERSION,
        "zip2": "10",
    }
    noncanonical = json.dumps(
        raw_pack_by_field, separators=(",", ":")
    ).encode("ascii")

    with pytest.raises(packs.AggregatePackError, match="not canonical"):
        packs.decode_aggregate_pack(_frame(noncanonical))

    raw_pack_by_field["records"] = [["10001", 1, 1, "1", "1"]]
    invalid_record = json.dumps(
        raw_pack_by_field, sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    with pytest.raises(packs.AggregatePackError, match="record"):
        packs.decode_aggregate_pack(_frame(invalid_record))


def test_logical_digest_rejects_empty_duplicate_and_out_of_order_streams() -> None:
    with pytest.raises(packs.AggregatePackError, match="requires records"):
        packs.aggregate_logical_digest(CODE_IDENTITY, ())
    for records in (
        (_record("10001"), _record("10001")),
        (_record("10002"), _record("10001")),
    ):
        with pytest.raises(packs.AggregatePackError, match="strictly ordered"):
            packs.aggregate_logical_digest(CODE_IDENTITY, records)


def test_aggregate_objects_reject_wrong_runtime_types() -> None:
    """Reject invalid objects at each aggregate-pack trust boundary."""

    zero = _record("10001", minimum="-0", median="0.000", maximum=0)
    assert zero.minimum_negotiated_rate == Decimal("0")
    invalid_calls = (
        lambda: packs.AggregateCodeIdentity(1, "27447"),
        lambda: packs.AggregatePackKey(PROJECTION_ID, object(), "10"),
        lambda: packs.AggregateZipRecord("bad", 1, 1, 1, 1, 1),
        lambda: packs.AggregatePack(object(), (_record("10001"),)),
        lambda: _pack(object()),
        lambda: packs.aggregate_logical_digest(object(), (_record("10001"),)),
        lambda: packs.aggregate_logical_digest(CODE_IDENTITY, (object(),)),
    )
    for invalid_call in invalid_calls:
        with pytest.raises(packs.AggregatePackError):
            invalid_call()


def test_aggregate_serialized_bounds_remain_executable(monkeypatch) -> None:
    """Keep every independent pack byte bound fail closed."""

    with monkeypatch.context() as patch:
        patch.setattr(packs, "_MAX_DECIMAL_BYTES", 0)
        with pytest.raises(packs.AggregatePackError, match="decimal bound"):
            packs._canonical_decimal(0)
    with monkeypatch.context() as patch:
        patch.setattr(packs, "MAX_AGGREGATE_RECORD_BYTES", 0)
        with pytest.raises(packs.AggregatePackError, match="record exceeds"):
            _pack(_record("10001"))
    with monkeypatch.context() as patch:
        patch.setattr(packs, "MAX_AGGREGATE_PACK_DECODED_BYTES", 0)
        with pytest.raises(packs.AggregatePackError, match="decoded byte bound"):
            packs.encode_aggregate_pack(_pack(_record("10001")))
    with monkeypatch.context() as patch:
        patch.setattr(packs, "_MAX_ENCODED_BYTES", 0)
        with pytest.raises(packs.AggregatePackError, match="stored byte bound"):
            packs.encode_aggregate_pack(_pack(_record("10001")))


@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"zip2": None}, "fields"),
        ({"version": 2}, "format version"),
        ({"records": {}}, "records"),
    ],
)
def test_aggregate_decoder_rejects_structural_drift(update, message: str) -> None:
    """Reject unknown fields, versions, and record containers."""

    document_by_field = {
        "code": "27447",
        "code_system": "CPT",
        "projection_id": PROJECTION_ID,
        "records": [["10001", 1, 1, "1", "1", "1"]],
        "version": packs.AGGREGATE_PACK_FORMAT_VERSION,
        "zip2": "10",
    }
    if update == {"zip2": None}:
        document_by_field.pop("zip2")
    else:
        document_by_field.update(update)
    with pytest.raises(packs.AggregatePackError, match=message):
        packs.decode_aggregate_pack(_frame(packs._canonical_json(document_by_field)))
