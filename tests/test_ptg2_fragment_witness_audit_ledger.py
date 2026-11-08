# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Version-bound audit responses and durable reports for fragment witnesses."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from process.ptg_parts import ptg2_fragment_witness_audit_ledger as ledger
from process.ptg_parts.ptg2_batch_candidate_audit_report import validate_batch_candidate_release_audit_report
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchWitnessBinding,
    build_audit_batch_request,
    matched_audit_batch_digest,
    parse_audit_batch_response,
)
from process.ptg_parts.ptg2_source_witness import decode_persisted_source_witness
from process.ptg_parts.ptg2_source_witness_audit import map_source_witness_records
from process.ptg_parts.ptg2_source_witness_contract import CompressedSourceWitnessRecord
from process.ptg_parts.ptg2_source_witness_fragment_encode import encode_fragment_source_witness_candidates
from process.ptg_parts.ptg2_source_witness_persisted_encode import SourceWitnessPayloadCounts
from process.ptg_parts.ptg2_source_witness_selection import source_set_digest
from tests import test_ptg2_batch_candidate_audit as batch
from tests import test_ptg2_candidate_audit_batch_contract as contract
from tests.test_ptg2_fast_candidate_audit import _occurrence_record, _provider_record
from tests.test_ptg2_source_witness import SOURCE_A
from tests.test_ptg2_source_witness_fragment_decode import _semantic_record_bytes


@pytest.fixture
def fragment_audit():
    originals = (_provider_record(), _occurrence_record(), _occurrence_record(1))
    candidates = [
        CompressedSourceWitnessRecord(
            record.kind, record.priority, record.tie_breaker, record.raw_source_sha256, _semantic_record_bytes(record)
        )
        for record in originals
    ]
    counts = SourceWitnessPayloadCounts(1, source_set_digest([SOURCE_A]), 2, 1, 2, 0, 2, 1, 3)
    payload, metadata = encode_fragment_source_witness_candidates(candidates, counts)
    view = decode_persisted_source_witness(payload, expected_raw_source_sha256=[SOURCE_A], expected_metadata=metadata)
    _, witness_io = map_source_witness_records(view, lambda record, _evidence: record.kind)
    return metadata, witness_io


def _parse_fragment_response(metadata, witness_io, *, request_metadata=None):
    sealed_metadata = metadata if request_metadata is None else request_metadata
    request = build_audit_batch_request(
        snapshot_id="candidate-snapshot",
        source_key="logical-source",
        plan_id="12-3456789",
        plan_market_type="group",
        witness_binding=AuditBatchWitnessBinding(
            audit_sample_digest=contract._AUDIT_SAMPLE_DIGEST,
            source_witness_sample_digest=sealed_metadata["sample_digest"],
            source_witness_payload_sha256=sealed_metadata["payload_sha256"],
            raw_container_sha256=(SOURCE_A,),
            source_witness_occurrence_count=2,
        ),
    )
    response = contract._response_payload()
    response["witness_io"] = witness_io
    response["request_digest"] = request.request_digest
    response["matched_challenge_digest"] = matched_audit_batch_digest(request.request_digest, 2)
    return parse_audit_batch_response(
        response,
        request=request,
        expected_source_witness=metadata,
        expected_audit_sample={"sample_count": 2},
    )


def test_fragment_response_binds_truthful_traversal_to_sealed_manifest(fragment_audit):
    metadata, witness_io = fragment_audit
    response = _parse_fragment_response(metadata, witness_io)
    assert response.witness_io == witness_io
    assert witness_io["repeated_evidence_decompressions"] > 0
    assert witness_io["repeated_evidence_json_parses"] > 0
    assert witness_io["reconstruction_bytes"] >= 2 * metadata["evidence_reconstructed_bytes"]


@pytest.mark.parametrize("field", ("sample_digest", "payload_sha256"))
def test_fragment_response_rejects_another_sealed_witness(fragment_audit, field):
    metadata, witness_io = fragment_audit
    another_manifest_by_field = {**metadata, field: "ff" * 32}
    with pytest.raises(ValueError, match="candidate_mismatch"):
        _parse_fragment_response(another_manifest_by_field, witness_io, request_metadata=metadata)


@pytest.mark.parametrize(
    "field",
    (
        "payload_reads",
        "payload_decodes",
        "record_decodes",
        "unique_evidence_entries",
        "evidence_decompressions",
        "fragment_decompressions",
        "fragment_sha256_hashes",
        "token_reconstructions",
        "evidence_sha256_hashes",
        "evidence_json_parses",
        "decoded_evidence_bytes",
        "repeated_evidence_decompressions",
        "repeated_evidence_sha256_hashes",
        "repeated_evidence_json_parses",
        "reconstruction_bytes",
    ),
)
def test_fragment_response_rejects_false_work_counters(fragment_audit, field):
    metadata, witness_io = fragment_audit
    altered_io_by_name = {**witness_io, field: witness_io[field] + 1}
    with pytest.raises(ValueError, match="fragment_witness"):
        _parse_fragment_response(metadata, altered_io_by_name)


@pytest.mark.parametrize("mutation", ("unknown", "missing", "boolean", "negative", "legacy"))
def test_fragment_response_rejects_wrong_ledger_shape(fragment_audit, mutation):
    metadata, witness_io = fragment_audit
    altered_io_by_name = dict(witness_io)
    if mutation == "unknown":
        altered_io_by_name["unexpected"] = 0
    elif mutation == "missing":
        altered_io_by_name.pop("fragment_decompressions")
    elif mutation == "legacy":
        altered_io_by_name = contract._witness_io()
    else:
        altered_io_by_name["fragment_decompressions"] = True if mutation == "boolean" else -1
    with pytest.raises(ValueError, match="witness_io"):
        _parse_fragment_response(metadata, altered_io_by_name)


@pytest.mark.parametrize("field", ("contract", "format_version", "compression", "recipe_reference_count"))
def test_fragment_response_rejects_changed_manifest_binding(fragment_audit, field):
    metadata, witness_io = fragment_audit
    altered_manifest_by_field = dict(metadata)
    altered_manifest_by_field[field] = metadata[field] + 1 if type(metadata[field]) is int else "other"
    with pytest.raises(ValueError):
        _parse_fragment_response(altered_manifest_by_field, witness_io)


def test_fragment_response_rejects_excess_reconstruction_work(fragment_audit, monkeypatch):
    metadata, witness_io = fragment_audit
    monkeypatch.setattr(
        ledger, "PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES", witness_io["reconstruction_bytes"] - 1
    )
    with pytest.raises(ValueError, match="work_invalid"):
        _parse_fragment_response(metadata, witness_io)


def test_fragment_ledger_cannot_relax_legacy_once_only_validation(fragment_audit):
    _, witness_io = fragment_audit
    with pytest.raises(ValueError, match="fields_invalid"):
        contract._parse_response({**contract._response_payload(), "witness_io": witness_io})
    legacy = contract._response_payload()
    legacy["witness_io"]["repeated_evidence_json_parses"] = 1
    with pytest.raises(ValueError, match="witness_io_repeated"):
        contract._parse_response(legacy)


def test_fragment_report_round_trips_sealed_metrics_and_ledgers(fragment_audit, monkeypatch):
    metadata, witness_io = fragment_audit
    monkeypatch.setattr(batch, "_RAW_DIGEST", SOURCE_A)
    monkeypatch.setattr(batch, "_WITNESS_SAMPLE_DIGEST", metadata["sample_digest"])
    monkeypatch.setattr(batch, "_WITNESS_PAYLOAD_DIGEST", metadata["payload_sha256"])
    original_response = batch._response_payload

    def response_with_fragments(request_digest):
        return {**original_response(request_digest), "witness_io": witness_io}

    monkeypatch.setattr(batch, "_response_payload", response_with_fragments)
    audit_target = replace(batch._target(), raw_container_sha256=(SOURCE_A,), source_witness=metadata)
    report = batch._v4_report(audit_target)
    evidence = validate_batch_candidate_release_audit_report(
        report,
        snapshot_id=audit_target.snapshot_id,
        source_key=audit_target.source_key,
        plan_id=audit_target.plan_id,
        plan_market_type=audit_target.plan_market_type,
    )
    assert evidence["source_witness_manifest"] == dict(metadata)
    assert report["io"]["witness_io"] == witness_io
    for section, field in (("source", "witness"), ("io", "witness_io")):
        altered_report_by_field = deepcopy(report)
        altered_report_by_field[section][field]["unexpected"] = 0
        with pytest.raises(ValueError, match="invalid"):
            validate_batch_candidate_release_audit_report(
                altered_report_by_field,
                snapshot_id=audit_target.snapshot_id,
                source_key=audit_target.source_key,
                plan_id=audit_target.plan_id,
                plan_market_type=audit_target.plan_market_type,
            )
