# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare mixed native/reference retained batches before any database write."""

import json
import struct
from dataclasses import dataclass
from datetime import UTC, datetime

MAX_INPUT_BYTES = 64 * 1024**2
MAX_OUTPUT_BYTES = 64 * 1024**2
MAX_COPY_ROWS = 5000
_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + bytes(8)
_TABLES = (
    "provider_directory_entity_source_binding",
    "provider_directory_entity_release_evidence",
    "provider_directory_insurance_network_plan_evidence",
)
_JSON_COLUMNS = ((), (5,), (5, 9))
_TIME_COLUMNS = (5, 6, 10)
_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


class RetainedFhirNativeError(ValueError):
    """A bounded batch cannot be prepared without changing reference semantics."""


@dataclass(frozen=True)
class PreparedRetainedFhirBatch:
    """All fallback validation and output accounting completed without writes."""

    copy_batches: tuple
    root_bytes: bytes
    families: dict
    row_count: int
    fallback_rows: tuple
    owned_output_bytes: int


def postgres_micros(value):
    """Encode aware timestamps using integer arithmetic, including before the epoch."""
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RetainedFhirNativeError("invalid_timestamp")
    elapsed = value.astimezone(UTC) - _EPOCH
    return (elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds


def _wire_rows(rows, context, location_id):
    """Keep each payload as text so supported resources are decoded only natively."""
    wire_rows = []
    for row in rows:
        payload = row["payload_json"]
        if type(payload) is not str:
            payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        site = None
        if context["derive"] and row["resource_type"] == "Location":
            site = str(location_id(context["source_id"], row["resource_id"]))
        wire_rows.append(
            {
                "resource_type": row["resource_type"],
                "resource_id": row["resource_id"],
                "payload_hash": row["payload_hash"],
                "payload_json": payload,
                "acquired_resource_sha256": row["acquired_resource_sha256"],
                "site_id": site,
            }
        )
    return wire_rows


def _field(table, column, value):
    if value is None:
        return None
    if column == _TIME_COLUMNS[table]:
        return struct.pack("!q", postgres_micros(value))
    if table == 0 and column in (3, 4):
        return value.bytes
    encoded = value.encode("utf-8")
    return b"\x01" + encoded if column in _JSON_COLUMNS[table] else encoded


def _fallback_batches(derived_records, initial_bytes):
    """Enforce one combined native/reference budget before exposing any COPY data."""
    buffers = [[], [], []]
    counts = [[], [], []]
    owned = initial_bytes
    for table, derived_record in derived_records:
        try:
            index = _TABLES.index(table)
        except ValueError:
            raise RetainedFhirNativeError("invalid_derived_table") from None
        fields = [_field(index, column, field_value) for column, field_value in enumerate(derived_record)]
        if len(fields) != (6, 7, 11)[index]:
            raise RetainedFhirNativeError("invalid_derived_record")
        width = 2 + sum(4 + (0 if field is None else len(field)) for field in fields)
        needs_batch = not buffers[index] or counts[index][-1] == MAX_COPY_ROWS
        owned += width + (len(_COPY_HEADER) + 2 if needs_batch else 0)
        if owned > MAX_OUTPUT_BYTES:
            raise RetainedFhirNativeError("output_limit")
        if needs_batch:
            if buffers[index]:
                buffers[index][-1].extend(struct.pack("!h", -1))
            buffers[index].append(bytearray(_COPY_HEADER))
            counts[index].append(0)
        output = buffers[index][-1]
        output.extend(struct.pack("!h", len(fields)))
        for field in fields:
            output.extend(struct.pack("!i", -1 if field is None else len(field)))
            if field is not None:
                output.extend(field)
        counts[index][-1] += 1
    for batches in buffers:
        if batches:
            batches[-1].extend(struct.pack("!h", -1))
    return tuple(tuple(zip(batches, table_counts)) for batches, table_counts in zip(buffers, counts)), owned


def _validate_native(streams, root, families, count, unsupported, owned, expected_count):
    """Reject stale bindings and inconsistent byte/count claims before reference work."""
    if (
        type(count) is not int
        or count != expected_count
        or type(owned) is not int
        or not 0 <= owned <= MAX_OUTPUT_BYTES
    ):
        raise RetainedFhirNativeError("native_accounting_invalid")
    if any(type(index) is not int or not 0 <= index < count for index in unsupported):
        raise RetainedFhirNativeError("native_fallback_invalid")
    if list(unsupported) != sorted(set(unsupported)):
        raise RetainedFhirNativeError("native_fallback_invalid")
    measured = len(root) + len(unsupported) * struct.calcsize("P")
    family_count = 0
    for encoded, row_count in families.values():
        measured += len(encoded)
        family_count += row_count
    for batches in streams:
        for encoded, row_count in batches:
            if type(encoded) is not bytes or type(row_count) is not int or not 0 < row_count <= MAX_COPY_ROWS:
                raise RetainedFhirNativeError("native_copy_invalid")
            if not encoded.startswith(_COPY_HEADER) or not encoded.endswith(b"\xff\xff"):
                raise RetainedFhirNativeError("native_copy_invalid")
            measured += len(encoded)
    if measured != owned or family_count != count:
        raise RetainedFhirNativeError("native_accounting_invalid")


def prepare_retained_fhir_batch(rows, context, *, native_encoder, reference_validator, reference_records, location_id):
    """Validate every fallback row before root use and prepare all COPY bytes before writes.

    Context contains source/release IDs, persisted hash contract, resource types,
    PostgreSQL timestamp microseconds and the derive flag. The reference callbacks
    retain ownership of payload validation, UUID authority and evidence semantics.
    """
    if len(rows) > 1000:
        raise RetainedFhirNativeError("row_limit")
    document_by_field = {**context, "rows": _wire_rows(rows, context, location_id)}
    encoded = json.dumps(document_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if len(encoded) > MAX_INPUT_BYTES:
        raise RetainedFhirNativeError("input_limit")
    bindings, evidence, plans, root, families, count, unsupported, owned = native_encoder(encoded)
    _validate_native((bindings, evidence, plans), root, families, count, unsupported, owned, len(rows))
    fallback_rows = [rows[index] for index in unsupported]
    if fallback_rows:
        reference_validator(fallback_rows)
    derived = reference_records(fallback_rows) if fallback_rows and context["derive"] else ()
    extra, owned = _fallback_batches(derived, owned)
    combined_batches = tuple(
        tuple(native) + additional for native, additional in zip((bindings, evidence, plans), extra)
    )
    return PreparedRetainedFhirBatch(combined_batches, root, families, count, tuple(unsupported), owned)
