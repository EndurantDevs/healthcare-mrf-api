# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mixed native/reference preparation validates completely before exposing output."""

import hashlib
import json
import struct
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import UUID

import pytest

from process import network_fhir_retained_native as native
from process.provider_directory_resource_hash import resource_payload_sha256_for_contract

_TABLE = "provider_directory_entity_release_evidence"
_BINDING = "provider_directory_entity_source_binding"
_SITE = UUID("01234567-89ab-cdef-8123-456789abcdef")
_AT = datetime(2026, 1, 1, 2, 3, 4, 5, tzinfo=UTC)


def _context(derive=True):
    return {
        "source_id": "source-a",
        "release_id": "release-1",
        "observed_at_pg_micros": native.postgres_micros(_AT),
        "hash_contract": "transport_bound_v1",
        "resource_types": ["Location", "Organization"],
        "derive": derive,
    }


def _row(kind, identity, **extra):
    payload = {"source_id": "source-a", "resource_id": identity, **extra}
    return {
        "resource_type": kind,
        "resource_id": identity,
        "payload_json": json.dumps(payload),
        "payload_hash": resource_payload_sha256_for_contract(payload, "transport_bound_v1", resource_type=kind),
        "acquired_resource_sha256": None,
    }


def _validate(rows):
    for row in rows:
        payload = json.loads(row["payload_json"])
        assert payload["source_id"] == "source-a" and payload["resource_id"] == row["resource_id"]
        assert (
            resource_payload_sha256_for_contract(payload, "transport_bound_v1", resource_type=row["resource_type"])
            == row["payload_hash"]
        )


def _records(rows):
    for row in rows:
        if row["resource_type"] == "Location":
            yield _BINDING, ("source-a", "Location", row["resource_id"], None, _SITE, _AT)
        payload = json.dumps(
            json.loads(row["payload_json"]), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        yield (
            _TABLE,
            ("source-a", row["resource_type"], row["resource_id"], "release-1", row["payload_hash"], payload, _AT),
        )


def _root(rows):
    return b"\n".join(
        json.dumps([row["resource_type"], row["resource_id"], row["payload_hash"]], separators=(",", ":")).encode()
        for row in rows
    )


def _stub(rows, unsupported, streams=((), (), ())):
    root = _root(rows)
    family_by_kind = {
        kind: (
            _root([row for row in rows if row["resource_type"] == kind]),
            sum(row["resource_type"] == kind for row in rows),
        )
        for kind in ("Location", "Organization")
    }
    owned = (
        len(root) + sum(len(value[0]) for value in family_by_kind.values()) + len(unsupported) * struct.calcsize("P")
    )
    owned += sum(len(encoded) for batches in streams for encoded, _ in batches)
    return lambda _: (*streams, root, family_by_kind, len(rows), unsupported, owned)


def _decode(batches):
    decoded_rows = []
    for encoded, expected_count in batches:
        assert encoded[:19] == b"PGCOPY\n\xff\r\n\0" + bytes(8)
        offset, count = 19, 0
        while struct.unpack_from("!h", encoded, offset)[0] != -1:
            columns = struct.unpack_from("!h", encoded, offset)[0]
            offset += 2
            fields = []
            for _ in range(columns):
                length = struct.unpack_from("!i", encoded, offset)[0]
                offset += 4
                fields.append(None if length == -1 else bytes(encoded[offset : offset + length]))
                offset += max(length, 0)
            decoded_rows.append(fields)
            count += 1
        assert count == expected_count and offset + 2 == len(encoded)
    return decoded_rows


def _prepare(rows, encoder, **kwargs):
    return native.prepare_retained_fhir_batch(
        rows,
        _context(),
        native_encoder=encoder,
        reference_validator=kwargs.get("validator", _validate),
        reference_records=kwargs.get("records", _records),
        location_id=lambda *_: _SITE,
    )


def test_mixed_rows_preserve_root_order_and_reference_payloads():
    rows = [
        _row("Organization", "org-1", name="Example"),
        _row("Location", "site-1", latitude=1.5),
        _row("Organization", "org-2", name="Café"),
    ]
    first = _prepare(rows[:1], _stub(rows[:1], [0]))
    streams = ((), tuple((bytes(encoded), count) for encoded, count in first.copy_batches[1]), ())
    result = _prepare(rows, _stub(rows, [1, 2], streams))
    assert result.fallback_rows == (1, 2) and result.row_count == 3
    assert result.root_bytes == _root(rows)
    decoded = _decode(result.copy_batches[1])
    assert [fields[2].decode() for fields in decoded] == ["org-1", "site-1", "org-2"]
    bindings = _decode(result.copy_batches[0])
    assert bindings[0][2] == b"site-1" and bindings[0][3] is None and bindings[0][4] == _SITE.bytes
    assert json.loads(decoded[1][5][1:])["latitude"] == 1.5
    assert json.loads(decoded[2][5][1:])["name"] == "Café"
    expected = hashlib.sha256(_root(rows)).hexdigest()
    assert hashlib.sha256(result.root_bytes).hexdigest() == expected


def test_all_fallback_validation_finishes_before_derived_preparation():
    rows = [_row("Location", "site-1", latitude=1.5), _row("Location", "site-2", latitude=2.5)]
    rows[1]["payload_hash"] = "0" * 64
    prepared_rows = []

    def records(fallback):
        prepared_rows.append(fallback)
        return _records(fallback)

    with pytest.raises(AssertionError):
        _prepare(rows, _stub(rows, [0, 1]), records=records)
    assert prepared_rows == []


def test_native_and_reference_share_one_output_bound(monkeypatch):
    rows = [_row("Location", "site-1", latitude=1.5)]
    encoder = _stub(rows, [0])
    initial = encoder(b"")[-1]
    monkeypatch.setattr(native, "MAX_OUTPUT_BYTES", initial + 1)
    with pytest.raises(native.RetainedFhirNativeError, match="output_limit"):
        _prepare(rows, encoder)


def test_validation_only_does_not_prepare_reference_copy():
    rows = [_row("Location", "site-1", latitude=1.5)]

    def reject(_):
        raise AssertionError("derived callback must remain unused")

    result = native.prepare_retained_fhir_batch(
        rows,
        _context(False),
        native_encoder=_stub(rows, [0]),
        reference_validator=_validate,
        reference_records=reject,
        location_id=reject,
    )
    assert result.copy_batches == ((), (), ()) and result.root_bytes == _root(rows)


def test_postgres_timestamp_encoding_keeps_microseconds_and_timezones():
    assert native.postgres_micros(datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)) == -1
    assert native.postgres_micros(datetime(2000, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))) == 0
    with pytest.raises(native.RetainedFhirNativeError, match="invalid_timestamp"):
        native.postgres_micros(datetime(2000, 1, 1))


def test_real_native_mixed_batch_matches_reference_when_extension_is_available():
    extension = pytest.importorskip("ptg2_address_canon")
    if not hasattr(extension, "encode_retained_fhir_capture_batch"):
        pytest.skip("retained encoder is not registered in this extension")
    rows = [
        _row("Organization", "org-1", name="Example"),
        _row("Location", "site-1", latitude=1.5),
        _row("Organization", "org-2", name="Café"),
    ]
    result = _prepare(rows, extension.encode_retained_fhir_capture_batch)
    assert result.fallback_rows == (1,) and result.root_bytes == _root(rows)
    decoded = _decode(result.copy_batches[1])
    by_identity = {fields[2].decode(): json.loads(fields[5][1:]) for fields in decoded}
    assert by_identity == {row["resource_id"]: json.loads(row["payload_json"]) for row in rows}


def test_native_errors_expose_only_structured_retry_coordinates():
    extension = pytest.importorskip("ptg2_address_canon")
    if not hasattr(extension, "encode_retained_fhir_capture_batch"):
        pytest.skip("retained encoder is not registered in this extension")
    with pytest.raises(ValueError) as failure:
        extension.encode_retained_fhir_capture_batch(b'{"private_payload":"not an envelope"}')
    error = json.loads(str(failure.value))
    assert set(error) == {"code", "row_index"}
    assert type(error["code"]) is str and error["row_index"] is None
    assert "private_payload" not in str(failure.value)


@pytest.mark.parametrize(
    "damage",
    ["count", "owned", "fallback_index", "fallback_order", "family_count", "copy_type", "copy_count", "copy_frame"],
)
def test_native_accounting_mismatch_refuses_before_reference_callbacks(damage):
    rows = [_row("Organization", "one", name="Example")]
    native_values = list(_stub(rows, [0])(b""))
    if damage == "family_count":
        native_values[4]["Organization"] = (native_values[4]["Organization"][0], 0)
    elif damage.startswith("copy_"):
        encoded = native._COPY_HEADER + b"\xff\xff"
        native_values[0] = [
            (bytearray(encoded) if damage == "copy_type" else encoded, True if damage == "copy_count" else 1)
        ]
        if damage == "copy_frame":
            native_values[0] = [(b"invalid", 1)]
    else:
        position, replacement = {
            "count": (5, True),
            "owned": (7, native_values[7] + 1),
            "fallback_index": (6, [1]),
            "fallback_order": (6, [0, 0]),
        }[damage]
        native_values[position] = replacement
    validator, records = Mock(), Mock()
    with pytest.raises(native.RetainedFhirNativeError, match="native_"):
        _prepare(rows, lambda _: native_values, validator=validator, records=records)
    validator.assert_not_called()
    records.assert_not_called()


@pytest.mark.parametrize("table,record", [("unknown", ()), (_BINDING, ())])
def test_reference_derivation_refuses_unknown_table_and_incomplete_record(table, record):
    rows = [_row("Organization", "one", name="Example")]
    with pytest.raises(native.RetainedFhirNativeError, match="invalid_derived_table|invalid_derived_record"):
        _prepare(rows, _stub(rows, [0]), records=lambda _: [(table, record)])


def test_reference_copy_chunks_preserve_complete_rows_and_binary_framing(monkeypatch):
    rows = [_row("Organization", "one", name="First"), _row("Organization", "two", name="Second")]
    monkeypatch.setattr(native, "MAX_COPY_ROWS", 1)
    result = _prepare(rows, _stub(rows, [0, 1]))
    assert [count for _encoded, count in result.copy_batches[1]] == [1, 1]
    assert [fields[2].decode() for fields in _decode(result.copy_batches[1])] == ["one", "two"]
    assert result.root_bytes == _root(rows) and result.row_count == 2
    assert result.fallback_rows == (0, 1)


@pytest.mark.parametrize("bound", ["rows", "bytes"])
def test_input_bounds_refuse_before_native_or_reference_preparation(monkeypatch, bound):
    rows = [_row("Organization", "one", name="Example")]
    if bound == "rows":
        rows *= 1001
    else:
        monkeypatch.setattr(native, "MAX_INPUT_BYTES", 1)
    encoder, validator, records = Mock(), Mock(), Mock()
    with pytest.raises(native.RetainedFhirNativeError, match="row_limit|input_limit"):
        _prepare(rows, encoder, validator=validator, records=records)
    encoder.assert_not_called()
    validator.assert_not_called()
    records.assert_not_called()
