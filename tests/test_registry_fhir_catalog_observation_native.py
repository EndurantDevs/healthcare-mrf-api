# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native raw pages retain the reference observation's complete byte contract."""

import json
import sys
from copy import deepcopy

import pytest

from process import registry_fhir_catalog_observation as observation
from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, RESOURCE_TYPES, _decode_fhir_catalog_page
from process.registry_fhir_catalog_observed_source import _validate_metadata_page
from process.registry_fhir_catalog_reference_rollup import MAX_PAGE_REFERENCES


@pytest.fixture
def native(monkeypatch):
    extension = pytest.importorskip("ptg2_address_canon")
    encoder = extension.encode_catalog_observation_page
    monkeypatch.setattr(observation, "_native_page", encoder)
    return encoder


def _record(resource_type="PractitionerRole", resource_id="example", payload=None, **changes):
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": "a" * 64,
        "payload_json": json.dumps(
            payload if payload is not None else {"network_refs": ["Organization/net"]}, ensure_ascii=False
        ),
        "row_bytes": 1,
        **changes,
    }


def _validate(page):
    _validate_metadata_page(page, "https://example.test/fhir")


def _reference(records, page_bytes=DEFAULT_BYTES):
    digests = observation.FHIRCatalogObservationDigests()
    page = _decode_fhir_catalog_page(records, page_bytes)
    _validate(page)
    for row in page:
        digests.add(row)
    return digests.document()


def _actual(records, page_bytes=DEFAULT_BYTES):
    digests = observation.FHIRCatalogObservationDigests()
    digests.add_page(records, _validate, page_bytes=page_bytes)
    return digests.document()


def _native(native, records, page_bytes=DEFAULT_BYTES):
    return native(
        tuple(
            tuple(record[key] for key in ("resource_type", "resource_id", "payload_hash", "payload_json"))
            for record in records
        ),
        page_bytes,
        MAX_PAGE_REFERENCES,
        sys.get_int_max_str_digits(),
    )


def test_native_complete_payload_and_four_family_receipts_match_reference(native):
    records = [
        _record(
            family,
            resource_id,
            {
                "name": "Example",
                "network_refs": [None, "", "Organization/net"],
                "nested": {"é😀": [True, None, 2**100, "\n\u007f"]},
            },
        )
        for family in RESOURCE_TYPES
        for resource_id in ("A", "a")
    ]
    assert _native(native, records) is not None
    assert _actual(records) == _reference(records)
    digests = observation.FHIRCatalogObservationDigests()
    for record in records:
        digests.add_page([record], _validate)
    assert digests.document() == _reference(records)
    for index, record in enumerate(records):
        changed = deepcopy(records)
        payload = json.loads(record["payload_json"])
        payload["unconsumed"] = "changed"
        changed[index]["payload_json"] = json.dumps(payload)
        assert _actual(changed) == _reference(changed) != _reference(records)


@pytest.mark.parametrize(
    "raw",
    [
        '{"value":1.25}',
        '{"value":1e-9}',
        '{"value":-0.0}',
        '{"value":-0}',
        '{"value":1,"value":2}',
        '{"$serde_json::private::Number":"42"}',
        '{"nested":{"$serde_json::private::Number":"42"}}',
        '{"nested":{"$serde_json::private::Number":"42","sibling":true}}',
        '{"nested":{"sibling":true,"$serde_json::private::Number":"42"}}',
        r'{"nested":{"\u0024serde_json::private::Number":"42"}}',
        r'{"nested":{"$serde_json::private::Numb\u0065r":"42"}}',
        '{"value":' + "[" * 140 + "0" + "]" * 140 + "}",
    ],
)
def test_number_duplicate_key_and_deep_json_semantics(native, raw):
    records = [_record(resource_id="a"), _record(resource_id="b", payload_json=raw)]
    assert _actual(records) == _reference(records)
    if any(value in raw for value in ("1.25", "1e-9", "-0.0", "[[[[")):
        assert _native(native, records) is None


def test_float_preflight_preserves_numeric_text_and_escaped_quotes(native):
    records = [_record(payload={"values": ["1.25", "1e-9", 'quote"1.0', "backslash\\1e2", True, False, -123]})]
    assert _native(native, records) is not None
    assert _actual(records) == _reference(records)


def test_native_row_limit_precedes_input_extraction(native):
    assert native((None,) * 1001, DEFAULT_BYTES, MAX_PAGE_REFERENCES, sys.get_int_max_str_digits()) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"resource_type": "Practitioner"},
        {"resource_type": None},
        {"resource_id": "bad/id"},
        {"resource_id": ""},
        {"resource_id": "x" * 65},
        {"resource_id": 1},
        {"payload_hash": "A" * 64},
        {"payload_hash": "a" * 63},
        {"payload_json": "[]"},
        {"payload_json": "null"},
        {"payload_json": "malformed"},
        {"payload_json": '{"value":NaN}'},
        {"payload_json": '{"value":Infinity}'},
        {"payload_json": '{"value":"\\ud800"}'},
        {"payload_json": '{"value":"\ud800"}'},
        {"payload_json": '{"value":' + "1" * 4301 + "}"},
        {"payload_json": '{"network_refs":{}}'},
        {"payload_json": '{"network_refs":[true]}'},
        {"payload_json": '{"network_refs":["bad\\nref"]}'},
        {"payload_json": json.dumps({"network_refs": ["é" * 1025]})},
        {"payload_json": json.dumps({"network_refs": [None] * (MAX_PAGE_REFERENCES + 1)})},
        {"payload_json": None},
        {"row_bytes": DEFAULT_BYTES + 1},
    ],
)
def test_invalid_input_retains_reference_exception_type_and_message(native, changes):
    records = [_record(**changes)]
    with pytest.raises(Exception) as expected:
        _reference(records)
    with pytest.raises(type(expected.value)) as actual:
        _actual(records)
    assert str(actual.value) == str(expected.value)


@pytest.mark.parametrize("page_bytes", [1, 200, 1024])
def test_underreported_sql_bytes_still_enforce_actual_canonical_page_bound(native, page_bytes):
    records = [_record(payload={"unconsumed": "x" * 2048})]
    with pytest.raises(ValueError, match="page_bounds"):
        _actual(records, page_bytes)
    assert _native(native, records, page_bytes) is None


def test_exact_page_byte_boundary(native):
    records = [_record(resource_id="a"), _record(resource_id="b")]
    frames = _native(native, records)
    boundary = 2 + sum(map(len, frames)) + len(frames) - 1
    assert _actual(records, boundary) == _reference(records, boundary)
    with pytest.raises(ValueError, match="page_bounds"):
        _actual(records, boundary - 1)


@pytest.mark.parametrize("records", [[_record(), _record()], [_record(resource_id="b"), _record(resource_id="a")]])
def test_native_order_failure_does_not_advance_hash_state(native, records):
    digests = observation.FHIRCatalogObservationDigests()
    before = digests.document()
    with pytest.raises(ValueError, match="order_invalid"):
        digests.add_page(records, _validate)
    assert digests.document() == before


def test_dense_payload_and_escaped_unicode_use_bounded_reference_fallback(native):
    for raw in (json.dumps({"dense": [0] * 100_001}), r'{"label":"\u00e9"}'):
        records = [_record(payload_json=raw)]
        assert _native(native, records) is None
        assert _actual(records) == _reference(records)


def test_native_metadata_validation_and_counts_precede_hash_advance(native):
    digests = observation.FHIRCatalogObservationDigests()
    records = [_record("Organization", payload={"name": "x" * 513})]
    before = digests.document()
    with pytest.raises(ValueError):
        digests.add_page(records, _validate)
    assert digests.document() == before
    digests._counts_by_type["PractitionerRole"] = 2**63 - 1
    before = digests.document()
    with pytest.raises(ValueError):
        digests.add_page([_record()], _validate)
    assert digests.document() == before


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("failure", ["duplicate", "order", "overflow"])
def test_native_and_fallback_pages_preserve_seeded_state_on_late_failure(native, fallback, failure):
    digests = observation.FHIRCatalogObservationDigests()
    seed = _record(resource_id="a")
    digests.add_page([seed], _validate)
    if failure == "overflow":
        digests._counts_by_type["PractitionerRole"] = 2**63 - 2
    before = digests.document()
    previous_key = digests._last_key
    first, second = ("b", "b") if failure == "duplicate" else ("c", "b") if failure == "order" else ("b", "c")
    records = [_record(resource_id=first), _record(resource_id=second, payload={"value": 1.25} if fallback else {})]
    assert (_native(native, records) is None) is fallback
    with pytest.raises(ValueError):
        digests.add_page(records, _validate)
    assert digests.document() == before
    assert digests._last_key == previous_key
    digests.add_page([_record(resource_id="b")], _validate)
    assert digests.document()["PractitionerRole"]["rows"] == before["PractitionerRole"]["rows"] + 1


def test_unavailable_native_uses_reference_and_runtime_errors_propagate(monkeypatch):
    monkeypatch.setattr(observation, "_native_page", None)
    assert _actual([_record()]) == _reference([_record()])

    def broken(*_args):
        raise RuntimeError("native runtime failed")

    monkeypatch.setattr(observation, "_native_page", broken)
    with pytest.raises(RuntimeError, match="native runtime failed"):
        _actual([_record()])
