# See LICENSE.

from __future__ import annotations

import hashlib
import zlib

import pytest

from process.ptg_parts import ptg2_source_witness as source_witness
from process.ptg_parts import ptg2_source_witness_persisted_encode as encode
from process.ptg_parts import ptg2_source_witness_primitives as primitives
from process.ptg_parts import ptg2_source_witness_selection as selection
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    WitnessPayloadLimitError,
)


SOURCE_A = "11" * 32
SOURCE_B = "22" * 32


def test_witness_primitives_reject_invalid_values():
    with pytest.raises(RuntimeError, match="invalid digest"):
        primitives.sha256_hex("not-a-digest", field_name="digest")
    with pytest.raises(RuntimeError, match="invalid count"):
        primitives.nonnegative_int(
            {"count": True}, "count", error_field_name="count"
        )
    with pytest.raises(RuntimeError, match="truncated"):
        primitives.read_u32(b"", 0, field_name="length")


def test_witness_selection_rejects_invalid_populations(monkeypatch):
    with pytest.raises(RuntimeError, match="unique nonempty"):
        selection.source_set_digest([])
    with pytest.raises(RuntimeError, match="cohort metrics"):
        selection.source_population([{"rate_occurrence": None}])
    invalid_population_rows = [{
        "rate_occurrence": {
            "population_count": 1,
            "emitted_rate_row_count": 0,
            "unqueryable_rate_row_count": 1,
        },
        "provider_reference": {"population_count": 0},
    }]
    with pytest.raises(RuntimeError, match="unqueryable rate population"):
        selection.source_population(invalid_population_rows)

    occurrence = CompressedSourceWitnessRecord(
        "rate_occurrence", 1, "tie", SOURCE_A, b"record"
    )
    with pytest.raises(RuntimeError, match="global coverage"):
        selection.select_source_witness_records(
            [occurrence], occurrence_population=1, provider_population=1
        )
    monkeypatch.setattr(selection, "source_witness_targets", lambda **_kwargs: (0, 0, 1))
    with pytest.raises(RuntimeError, match="exact total budget"):
        selection.select_source_witness_records(
            [], occurrence_population=0, provider_population=0
        )


def test_persisted_encoder_rejects_bad_evidence(monkeypatch):
    with pytest.raises(RuntimeError, match="evidence digest"):
        encode._insert_evidence({}, SOURCE_A, b"wrong")

    existing = encode._EvidenceEntry(SOURCE_A, 5, zlib.compress(b"other"))

    class FixedDigest:
        def hexdigest(self):
            return SOURCE_A

    monkeypatch.setattr(encode.hashlib, "sha256", lambda _raw=b"": FixedDigest())
    with pytest.raises(RuntimeError, match="evidence digest"):
        encode._insert_evidence({SOURCE_A: existing}, SOURCE_A, b"value")


def test_persisted_encoder_enforces_record_and_payload_bounds(monkeypatch):
    monkeypatch.setattr(encode, "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES", 1)
    with pytest.raises(RuntimeError, match="entry exceeds"):
        encode._insert_evidence({}, hashlib.sha256(b"value").hexdigest(), b"value")
    monkeypatch.setattr(encode, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 1)
    with pytest.raises(WitnessPayloadLimitError, match="512 MiB"):
        encode._append_payload_part(bytearray(), b"too large")


def test_source_bundle_validation_rejects_incomplete_sets(monkeypatch):
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        source_witness._read_source_bundles([], [])
    monkeypatch.setattr(
        source_witness,
        "read_scanner_bundle",
        lambda _entry: ({"raw_source_sha256": SOURCE_A}, []),
    )
    with pytest.raises(RuntimeError, match="does not match"):
        source_witness._read_source_bundles([{}], [SOURCE_B])


def test_source_witness_rejects_zero_occurrence_selection(monkeypatch):
    population = selection.SourceWitnessPopulation(0, 0, 0, 0)
    monkeypatch.setattr(source_witness, "_read_source_bundles", lambda *_args: ([{}], []))
    monkeypatch.setattr(source_witness, "source_population", lambda _headers: population)
    monkeypatch.setattr(
        source_witness,
        "select_source_witness_records",
        lambda *_args, **_kwargs: ([], 0, 0, 0),
    )
    with pytest.raises(RuntimeError, match="no queryable occurrence"):
        source_witness.build_persisted_source_witness(
            [{}], expected_raw_source_sha256=[SOURCE_A]
        )
