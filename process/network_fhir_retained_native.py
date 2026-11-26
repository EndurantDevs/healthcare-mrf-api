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


def _encode_retained_batch(resource_rows, context, native_encoder, location_id, rows_encoder, native_location_id):
    if len(resource_rows) > 1000:
        raise RetainedFhirNativeError("row_limit")
    if rows_encoder is not None and all(type(resource_row["payload_json"]) is str for resource_row in resource_rows):
        derive = context["derive"]
        source_id = context["source_id"]
        native_rows = [
            (
                resource_row["resource_type"],
                resource_row["resource_id"],
                resource_row["payload_hash"],
                resource_row["payload_json"],
                resource_row["acquired_resource_sha256"],
                None
                if (native_location_id or not derive or resource_row["resource_type"] != "Location")
                else str(location_id(source_id, resource_row["resource_id"])),
            )
            for resource_row in resource_rows
        ]
        return rows_encoder(context, native_rows)
    document_by_field = {**context, "rows": _wire_rows(resource_rows, context, location_id)}
    encoded = json.dumps(document_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if len(encoded) > MAX_INPUT_BYTES:
        raise RetainedFhirNativeError("input_limit")
    return native_encoder(encoded)


def _finish_native_batch(rows, context, encoded, *, reference_validator, reference_records):
    bindings, evidence, plans, root, families, count, unsupported, owned = encoded
    _validate_native((bindings, evidence, plans), root, families, count, unsupported, owned, len(rows))
    fallback_rows = [rows[index] for index in unsupported]
    if fallback_rows:
        reference_validator(fallback_rows)
    derived = reference_records(fallback_rows) if fallback_rows and context["derive"] else ()
    extra, owned = _fallback_batches(derived, owned)
    batches = tuple(tuple(native) + additional for native, additional in zip((bindings, evidence, plans), extra))
    return PreparedRetainedFhirBatch(batches, root, families, count, tuple(unsupported), owned)


def prepare_retained_fhir_batch(
    rows,
    context,
    *,
    native_encoder,
    reference_validator,
    reference_records,
    location_id,
    rows_encoder=None,
    native_location_id=False,
):
    """Validate fallback rows before root use and prepare all retained COPY bytes before writes."""
    encoded = _encode_retained_batch(rows, context, native_encoder, location_id, rows_encoder, native_location_id)
    return _finish_native_batch(
        rows,
        context,
        encoded,
        reference_validator=reference_validator,
        reference_records=reference_records,
    )


def prepare_retained_fhir_witness_page(
    rows,
    context,
    witness_bounds,
    *,
    fused_encoder,
    reference_validator,
    reference_records,
):
    """Validate witness scope/order/budget and encode retained rows in one native call."""
    if len(rows) > 1000:
        raise RetainedFhirNativeError("row_limit")
    dataset_id, after, max_bytes = witness_bounds
    prepared = fused_encoder(rows, context, dataset_id, after, max_bytes)
    if prepared is None:
        return None
    last_type, last_id, encoded = prepared
    batch = _finish_native_batch(
        rows,
        context,
        encoded,
        reference_validator=reference_validator,
        reference_records=reference_records,
    )
    return (last_type, last_id), batch
