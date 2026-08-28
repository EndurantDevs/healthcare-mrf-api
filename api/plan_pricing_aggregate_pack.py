# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded immutable packs for exact ZIP pricing aggregates."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import struct
import zlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from api.plan_pricing_projection_contract import projection_code_identity
from api.ptg2_shared_blocks import PTG2SharedBlockError, decode_shared_block_payload


AGGREGATE_PACK_FORMAT_VERSION = 1
AGGREGATE_PACK_CODEC = "zlib"
MAX_AGGREGATE_PACK_RECORDS = 1_000
MAX_AGGREGATE_RECORD_BYTES = 512
MAX_AGGREGATE_PACK_DECODED_BYTES = 544 * 1024

_MAGIC = b"HPAGG01\0"
_HEADER = struct.Struct(">8sI32s")
_MAX_ENCODED_BYTES = _HEADER.size + MAX_AGGREGATE_PACK_DECODED_BYTES + 1_024
_PROJECTION_ID = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_ZIP2 = re.compile(r"[0-9]{2}", flags=re.ASCII)
_ZIP5 = re.compile(r"[0-9]{5}", flags=re.ASCII)
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", flags=re.ASCII)
_MAX_DECIMAL_BYTES = 64
_MAX_COUNT = 2**63 - 1
_DIGEST_DOMAIN = b"HEALTHPORTA_PLAN_PRICING_AGGREGATES_V1\0"
_PACK_FIELDS = frozenset(
    {"code", "code_system", "projection_id", "records", "version", "zip2"}
)


class AggregatePackError(ValueError):
    """Raised when an aggregate pack violates its immutable contract."""


def _canonical_decimal(raw_decimal: Any) -> tuple[Decimal, str]:
    if isinstance(raw_decimal, bool) or isinstance(raw_decimal, float):
        raise AggregatePackError("aggregate rate must be an exact decimal")
    try:
        decimal_value = (
            raw_decimal
            if isinstance(raw_decimal, Decimal)
            else Decimal(raw_decimal)
        )
    except (InvalidOperation, TypeError, ValueError):
        raise AggregatePackError("aggregate rate must be an exact decimal") from None
    if not decimal_value.is_finite() or decimal_value < 0:
        raise AggregatePackError("aggregate rate must be finite and non-negative")
    if decimal_value == 0:
        decimal_text = "0"
    else:
        decimal_tuple = decimal_value.as_tuple()
        last_digit = len(decimal_tuple.digits)
        while last_digit > 1 and decimal_tuple.digits[last_digit - 1] == 0:
            last_digit -= 1
        exponent = int(decimal_tuple.exponent) + len(decimal_tuple.digits) - last_digit
        projected_length = (
            last_digit + exponent
            if exponent >= 0
            else last_digit + 1
            if last_digit + exponent > 0
            else 2 - exponent
        )
        if projected_length > _MAX_DECIMAL_BYTES:
            raise AggregatePackError(
                "aggregate rate exceeds its canonical decimal bound"
            )
        decimal_text = format(decimal_value, "f")
        if "." in decimal_text:
            decimal_text = decimal_text.rstrip("0").rstrip(".")
    if (
        len(decimal_text.encode("ascii", errors="ignore")) != len(decimal_text)
        or len(decimal_text) > _MAX_DECIMAL_BYTES
        or _DECIMAL.fullmatch(decimal_text) is None
    ):
        raise AggregatePackError("aggregate rate exceeds its canonical decimal bound")
    return Decimal(decimal_text), decimal_text


def _positive_count(value: Any, *, field_name: str) -> int:
    if type(value) is not int or not 1 <= value <= _MAX_COUNT:
        raise AggregatePackError(f"aggregate {field_name} is invalid")
    return value


@dataclass(frozen=True)
class AggregateCodeIdentity:
    """One exact canonical external procedure-code identity."""

    code_system: str
    code: str

    def __post_init__(self) -> None:
        if type(self.code_system) is not str or type(self.code) is not str:
            raise AggregatePackError("aggregate code identity is invalid")
        try:
            system_bytes = self.code_system.encode("ascii")
            code_bytes = self.code.encode("ascii")
        except UnicodeEncodeError:
            raise AggregatePackError("aggregate code identity is invalid") from None
        if (
            not 1 <= len(system_bytes) <= 64
            or not 1 <= len(code_bytes) <= 128
            or projection_code_identity(self.code_system, self.code)
            != (self.code_system, self.code)
        ):
            raise AggregatePackError("aggregate code identity is not canonical")


@dataclass(frozen=True)
class AggregatePackKey:
    """The immutable SQL coordinate for one compact ZIP2 pack."""

    projection_id: str
    code_identity: AggregateCodeIdentity
    zip2: str

    def __post_init__(self) -> None:
        if (
            type(self.projection_id) is not str
            or _PROJECTION_ID.fullmatch(self.projection_id) is None
        ):
            raise AggregatePackError("aggregate projection identity is invalid")
        if not isinstance(self.code_identity, AggregateCodeIdentity):
            raise AggregatePackError("aggregate code identity is invalid")
        if type(self.zip2) is not str or _ZIP2.fullmatch(self.zip2) is None:
            raise AggregatePackError("aggregate ZIP2 is invalid")


@dataclass(frozen=True)
class AggregateZipRecord:
    """One exact aggregate for a five-digit ZIP cell."""

    zip5: str
    provider_count: int
    rate_count: int
    minimum_negotiated_rate: Decimal
    median_negotiated_rate: Decimal
    maximum_negotiated_rate: Decimal

    def __post_init__(self) -> None:
        if type(self.zip5) is not str or _ZIP5.fullmatch(self.zip5) is None:
            raise AggregatePackError("aggregate ZIP5 is invalid")
        object.__setattr__(
            self,
            "provider_count",
            _positive_count(self.provider_count, field_name="provider_count"),
        )
        object.__setattr__(
            self,
            "rate_count",
            _positive_count(self.rate_count, field_name="rate_count"),
        )
        minimum, _ = _canonical_decimal(self.minimum_negotiated_rate)
        median, _ = _canonical_decimal(self.median_negotiated_rate)
        maximum, _ = _canonical_decimal(self.maximum_negotiated_rate)
        if not minimum <= median <= maximum:
            raise AggregatePackError("aggregate rates are not ordered")
        object.__setattr__(self, "minimum_negotiated_rate", minimum)
        object.__setattr__(self, "median_negotiated_rate", median)
        object.__setattr__(self, "maximum_negotiated_rate", maximum)


@dataclass(frozen=True)
class AggregatePack:
    """A nonempty bounded sequence of strictly ordered ZIP records."""

    key: AggregatePackKey
    records: tuple[AggregateZipRecord, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.key, AggregatePackKey):
            raise AggregatePackError("aggregate pack key is invalid")
        normalized_records = tuple(self.records)
        if not 1 <= len(normalized_records) <= MAX_AGGREGATE_PACK_RECORDS:
            raise AggregatePackError("aggregate pack record count is invalid")
        previous_zip: str | None = None
        for record in normalized_records:
            if not isinstance(record, AggregateZipRecord):
                raise AggregatePackError("aggregate pack record is invalid")
            if not record.zip5.startswith(self.key.zip2):
                raise AggregatePackError("aggregate record is outside its ZIP2 pack")
            if previous_zip is not None and record.zip5 <= previous_zip:
                raise AggregatePackError(
                    "aggregate pack ZIP records must be strictly ordered"
                )
            if len(_record_json(record)) > MAX_AGGREGATE_RECORD_BYTES:
                raise AggregatePackError("aggregate record exceeds its byte bound")
            previous_zip = record.zip5
        object.__setattr__(self, "records", normalized_records)


def _record_values(record: AggregateZipRecord) -> list[Any]:
    return [
        record.zip5,
        record.provider_count,
        record.rate_count,
        _canonical_decimal(record.minimum_negotiated_rate)[1],
        _canonical_decimal(record.median_negotiated_rate)[1],
        _canonical_decimal(record.maximum_negotiated_rate)[1],
    ]


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def _record_json(record: AggregateZipRecord) -> bytes:
    return _canonical_json(_record_values(record))


def _raw_pack(pack: AggregatePack) -> bytes:
    identity = pack.key.code_identity
    raw_payload = _canonical_json(
        {
            "code": identity.code,
            "code_system": identity.code_system,
            "projection_id": pack.key.projection_id,
            "records": [_record_values(record) for record in pack.records],
            "version": AGGREGATE_PACK_FORMAT_VERSION,
            "zip2": pack.key.zip2,
        }
    )
    if len(raw_payload) > MAX_AGGREGATE_PACK_DECODED_BYTES:
        raise AggregatePackError("aggregate pack exceeds its decoded byte bound")
    return raw_payload


def encode_aggregate_pack(pack: AggregatePack) -> bytes:
    """Encode one deterministic, checksummed zlib pack."""

    raw_payload = _raw_pack(pack)
    encoded_pack = _HEADER.pack(
        _MAGIC,
        len(raw_payload),
        hashlib.sha256(raw_payload).digest(),
    ) + zlib.compress(raw_payload, level=9)
    if len(encoded_pack) > _MAX_ENCODED_BYTES:
        raise AggregatePackError("aggregate pack exceeds its stored byte bound")
    return encoded_pack


def _decoded_record(raw_record: Any) -> AggregateZipRecord:
    if (
        not isinstance(raw_record, list)
        or len(raw_record) != 6
        or len(_canonical_json(raw_record)) > MAX_AGGREGATE_RECORD_BYTES
    ):
        raise AggregatePackError("aggregate pack record is invalid")
    return AggregateZipRecord(
        zip5=raw_record[0],
        provider_count=raw_record[1],
        rate_count=raw_record[2],
        minimum_negotiated_rate=raw_record[3],
        median_negotiated_rate=raw_record[4],
        maximum_negotiated_rate=raw_record[5],
    )


def _decoded_pack(raw_payload: bytes) -> AggregatePack:
    try:
        raw_pack = json.loads(raw_payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise AggregatePackError("aggregate pack payload is invalid JSON") from None
    if not isinstance(raw_pack, Mapping) or frozenset(raw_pack) != _PACK_FIELDS:
        raise AggregatePackError("aggregate pack payload fields are invalid")
    if raw_pack.get("version") != AGGREGATE_PACK_FORMAT_VERSION:
        raise AggregatePackError("aggregate pack format version is invalid")
    raw_records = raw_pack.get("records")
    if not isinstance(raw_records, list):
        raise AggregatePackError("aggregate pack records are invalid")
    identity = AggregateCodeIdentity(
        code_system=raw_pack.get("code_system"),
        code=raw_pack.get("code"),
    )
    return AggregatePack(
        key=AggregatePackKey(
            projection_id=raw_pack.get("projection_id"),
            code_identity=identity,
            zip2=raw_pack.get("zip2"),
        ),
        records=tuple(_decoded_record(record) for record in raw_records),
    )


def _aggregate_pack_header(encoded_pack: bytes) -> tuple[int, bytes]:
    if (
        not isinstance(encoded_pack, bytes)
        or not _HEADER.size < len(encoded_pack) <= _MAX_ENCODED_BYTES
    ):
        raise AggregatePackError("aggregate pack byte count is invalid")
    magic, raw_byte_count, expected_sha256 = _HEADER.unpack_from(encoded_pack)
    if magic != _MAGIC or not 1 <= raw_byte_count <= MAX_AGGREGATE_PACK_DECODED_BYTES:
        raise AggregatePackError("aggregate pack header is invalid")
    return raw_byte_count, expected_sha256


def aggregate_pack_raw_byte_count(
    encoded_pack: bytes,
    *,
    expected_raw_byte_count: int | None = None,
) -> int:
    """Return bounded frame metadata and optionally verify its SQL row count."""

    raw_byte_count, _expected_sha256 = _aggregate_pack_header(encoded_pack)
    if expected_raw_byte_count is not None and (
        type(expected_raw_byte_count) is not int
        or raw_byte_count != expected_raw_byte_count
    ):
        raise AggregatePackError(
            "aggregate pack decoded byte count does not match its SQL row"
        )
    return raw_byte_count


def decode_aggregate_pack(
    encoded_pack: bytes,
    *,
    expected_key: AggregatePackKey | None = None,
) -> AggregatePack:
    """Decode and fully validate one bounded aggregate pack."""

    raw_byte_count, expected_sha256 = _aggregate_pack_header(encoded_pack)
    try:
        raw_payload = decode_shared_block_payload(
            codec=AGGREGATE_PACK_CODEC,
            encoded_payload=encoded_pack[_HEADER.size :],
            raw_byte_count=raw_byte_count,
            maximum_raw_bytes=MAX_AGGREGATE_PACK_DECODED_BYTES,
        )
    except PTG2SharedBlockError as exc:
        raise AggregatePackError("aggregate pack compression is invalid") from exc
    if not hmac.compare_digest(hashlib.sha256(raw_payload).digest(), expected_sha256):
        raise AggregatePackError("aggregate pack checksum is invalid")
    pack = _decoded_pack(raw_payload)
    if _raw_pack(pack) != raw_payload:
        raise AggregatePackError("aggregate pack payload is not canonical")
    if expected_key is not None and pack.key != expected_key:
        raise AggregatePackError("aggregate pack key does not match its SQL row")
    return pack


def aggregate_logical_digest(
    code_identity: AggregateCodeIdentity,
    aggregate_records: Iterable[AggregateZipRecord],
) -> str:
    """Digest ordered logical rows without depending on packs or compression."""

    if not isinstance(code_identity, AggregateCodeIdentity):
        raise AggregatePackError("aggregate code identity is invalid")
    digest = hashlib.sha256()
    digest.update(_DIGEST_DOMAIN)
    identity_bytes = _canonical_json(
        [code_identity.code_system, code_identity.code]
    )
    digest.update(len(identity_bytes).to_bytes(4, "big"))
    digest.update(identity_bytes)
    previous_zip: str | None = None
    record_count = 0
    for aggregate_record in aggregate_records:
        if not isinstance(aggregate_record, AggregateZipRecord):
            raise AggregatePackError("aggregate digest record is invalid")
        if previous_zip is not None and aggregate_record.zip5 <= previous_zip:
            raise AggregatePackError(
                "aggregate digest ZIP records must be strictly ordered"
            )
        record_bytes = _record_json(aggregate_record)
        digest.update(len(record_bytes).to_bytes(4, "big"))
        digest.update(record_bytes)
        previous_zip = aggregate_record.zip5
        record_count += 1
    if not record_count:
        raise AggregatePackError("aggregate digest requires records")
    return digest.hexdigest()
