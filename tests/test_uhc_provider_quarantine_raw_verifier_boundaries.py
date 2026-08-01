# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from process import uhc_provider_quarantine_raw_verifier as raw_verifier
from process.uhc_provider_quarantine_contract import UhcProviderQuarantine
from process.uhc_provider_quarantine_raw_verifier import (
    UhcProviderQuarantineRawError,
    verify_provider_quarantine_source_records,
)
from process.uhc_provider_quarantine_record import (
    validate_checksum_invalid_provider_record,
)
from process.uhc_retained_range_manifest import RANGE_CONTRACT_VERSION
from process.uhc_retained_types import RawRangeProof
from tests.test_uhc_provider_quarantine_raw_verifier import (
    _fixture,
    _provider_record,
)


def _raw_request(arguments, **overrides):
    request_by_field = {
        "source": arguments["source"],
        "quarantines": arguments["quarantines"],
        "max_record_bytes": arguments["max_record_bytes"],
    }
    request_by_field.update(overrides)
    return raw_verifier._RawVerificationRequest(**request_by_field)


def test_checksum_invalid_record_accepts_absent_optional_text() -> None:
    record_by_field = _provider_record("1003821381")
    for field_name in ("facility_name", "gender", "last_updated_on"):
        record_by_field.pop(field_name)
    record_by_field["accepting"] = None

    census = validate_checksum_invalid_provider_record(record_by_field)

    assert census.individual_records == 1


def test_raw_json_guards_reject_duplicate_keys_and_missing_files(tmp_path):
    with pytest.raises(ValueError, match="duplicate key"):
        raw_verifier._reject_duplicate_keys([("key", 1), ("key", 2)])
    with pytest.raises(UhcProviderQuarantineRawError, match="unavailable"):
        raw_verifier._open_readonly(tmp_path / "missing.json")


def test_target_record_rejects_invalid_json_after_exact_hash_match():
    encoded_record = b'{"npi":'
    quarantine = UhcProviderQuarantine(
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        range_ordinal=0,
        occurrence_ordinal=0,
        record_sha256=hashlib.sha256(encoded_record).hexdigest(),
    )

    with pytest.raises(UhcProviderQuarantineRawError, match="JSON is invalid"):
        raw_verifier._validate_target_record(encoded_record, quarantine)


def test_manifest_reader_rejects_unsafe_short_drifted_and_duplicate_json(
    monkeypatch,
    tmp_path,
):
    empty_path = tmp_path / "empty.json"
    empty_path.write_bytes(b"")
    empty_path.chmod(0o600)
    with pytest.raises(UhcProviderQuarantineRawError, match="unsafe"):
        raw_verifier._read_manifest_identity(
            empty_path,
            hashlib.sha256(b"").hexdigest(),
            "test-build",
        )

    valid_path = tmp_path / "valid.json"
    valid_bytes = b'{}'
    valid_path.write_bytes(valid_bytes)
    valid_path.chmod(0o600)
    with monkeypatch.context() as patch:
        patch.setattr(raw_verifier.os, "read", lambda *_args: b"")
        with pytest.raises(UhcProviderQuarantineRawError, match="ended early"):
            raw_verifier._read_manifest_identity(
                valid_path,
                hashlib.sha256(valid_bytes).hexdigest(),
                "test-build",
            )
    with pytest.raises(UhcProviderQuarantineRawError, match="identity changed"):
        raw_verifier._read_manifest_identity(
            valid_path,
            "0" * 64,
            "test-build",
        )

    duplicate_path = tmp_path / "duplicate.json"
    duplicate_bytes = b'{"key":1,"key":2}'
    duplicate_path.write_bytes(duplicate_bytes)
    duplicate_path.chmod(0o600)
    with pytest.raises(UhcProviderQuarantineRawError, match="JSON is invalid"):
        raw_verifier._read_manifest_identity(
            duplicate_path,
            hashlib.sha256(duplicate_bytes).hexdigest(),
            "test-build",
        )


def test_json_object_framer_covers_whitespace_escaping_and_failures():
    observed_records = []
    framer = raw_verifier._JsonObjectFramer(observed_records.append, 128)
    framer.feed(b" \t\r\n,[]")
    framer.feed(br'{"value":"a\"b"}')
    framer.finish()
    assert observed_records == [br'{"value":"a\"b"}']

    invalid = raw_verifier._JsonObjectFramer(lambda _record: None, 128)
    with pytest.raises(UhcProviderQuarantineRawError, match="framing is invalid"):
        invalid.feed(b"x")

    incomplete = raw_verifier._JsonObjectFramer(lambda _record: None, 128)
    incomplete.feed(b'{"value":"')
    with pytest.raises(UhcProviderQuarantineRawError, match="incomplete"):
        incomplete.finish()


def _single_record_range(encoded_record: bytes = b"{}") -> RawRangeProof:
    canonical_record = encoded_record + b"\n"
    return RawRangeProof(
        artifact_sha256=hashlib.sha256(encoded_record).hexdigest(),
        contract_version=RANGE_CONTRACT_VERSION,
        range_count=1,
        range_ordinal=0,
        raw_byte_start=0,
        raw_byte_end=len(encoded_record),
        raw_sha256=hashlib.sha256(encoded_record).hexdigest(),
        raw_byte_count=len(encoded_record),
        record_start=0,
        record_end=1,
        record_count=1,
        canonical_sha256=hashlib.sha256(canonical_record).hexdigest(),
        canonical_byte_count=len(canonical_record),
        path="unused",
    )


def test_range_verifier_covers_nontarget_and_changed_proof():
    verifier = raw_verifier._RangeRecordVerifier(
        _single_record_range(),
        {},
        {},
    )
    verifier.observe(b"{}")

    with pytest.raises(UhcProviderQuarantineRawError, match="proof changed"):
        verifier.assert_complete(hashlib.sha256(b"different"))


def test_range_reader_rejects_read_error_and_early_end(monkeypatch):
    raw_range = _single_record_range()

    def fail_read(*_args):
        raise OSError("read failed")

    with monkeypatch.context() as patch:
        patch.setattr(raw_verifier.os, "pread", fail_read)
        with pytest.raises(UhcProviderQuarantineRawError, match="read failed"):
            raw_verifier._verify_range(-1, raw_range, {}, {}, 128)
    with monkeypatch.context() as patch:
        patch.setattr(raw_verifier.os, "pread", lambda *_args: b"")
        with pytest.raises(UhcProviderQuarantineRawError, match="ended early"):
            raw_verifier._verify_range(-1, raw_range, {}, {}, 128)


def test_quarantine_request_rejects_each_sparse_bound(tmp_path):
    arguments, _records = _fixture(tmp_path)
    quarantine = arguments["quarantines"][0]
    invalid_requests = (
        _raw_request(arguments, max_record_bytes=True),
        _raw_request(arguments, quarantines=(quarantine,) * 33),
        _raw_request(
            arguments,
            quarantines=(replace(quarantine, source_file_id="f" * 64),),
        ),
        _raw_request(arguments, quarantines=(quarantine, quarantine)),
    )

    for request in invalid_requests:
        with pytest.raises(UhcProviderQuarantineRawError):
            raw_verifier._quarantine_by_occurrence(request)


def test_manifest_and_range_lineage_fail_closed(monkeypatch, tmp_path):
    arguments, _records = _fixture(tmp_path)
    request = _raw_request(arguments)
    with monkeypatch.context() as patch:
        patch.setattr(raw_verifier, "_read_manifest_identity", lambda *_args: 1)

        def reject_manifest(**_kwargs):
            raise raw_verifier.UHCRetainedAdmissionError("invalid manifest")

        patch.setattr(
            raw_verifier,
            "load_verified_range_manifest",
            reject_manifest,
        )
        with pytest.raises(UhcProviderQuarantineRawError, match="proof is invalid"):
            raw_verifier._load_range_by_ordinal(request)

    with pytest.raises(UhcProviderQuarantineRawError, match="lineage changed"):
        raw_verifier._quarantine_by_range(request, {})


def test_raw_range_verification_rejects_unobserved_target(
    monkeypatch,
    tmp_path,
):
    arguments, _records = _fixture(tmp_path)
    request = _raw_request(arguments)
    range_by_ordinal = raw_verifier._load_range_by_ordinal(request)
    quarantine_by_occurrence = raw_verifier._quarantine_by_occurrence(request)
    quarantine_by_range = raw_verifier._quarantine_by_range(
        request,
        range_by_ordinal,
    )
    monkeypatch.setattr(raw_verifier, "_verify_range", lambda *_args: None)

    with pytest.raises(UhcProviderQuarantineRawError, match="proof is incomplete"):
        raw_verifier._verify_raw_ranges(
            request,
            range_by_ordinal,
            quarantine_by_range,
            quarantine_by_occurrence,
        )


def test_raw_verifier_empty_quarantine_is_an_exact_zero_census(tmp_path):
    arguments, _records = _fixture(tmp_path)

    census = verify_provider_quarantine_source_records(
        arguments["source"],
        (),
        arguments["max_record_bytes"],
    )

    assert census.counter_map == {
        "invalid_npi_individual_records": 0,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 0,
        "invalid_npi_provider_plan_rows": 0,
        "invalid_npi_structure_count": 0,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 0,
        "invalid_npi_structure_address_rows": 0,
        "invalid_npi_structure_provider_plan_rows": 0,
    }
