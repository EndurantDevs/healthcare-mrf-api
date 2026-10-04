# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact fragment persistence guards, immutable inputs, and failure cleanup."""

from __future__ import annotations

import asyncio
import hashlib
import os
from dataclasses import replace
from io import BytesIO
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_source_witness as witness
from process.ptg_parts import ptg2_source_witness_fragment_encode as encode
from process.ptg_parts import ptg2_source_witness_streaming_encode as streaming
from process.ptg_parts.ptg2_source_witness_codec import externalize_source_evidence_record
from process.ptg_parts.ptg2_source_witness_contract import WitnessPayloadLimitError
from process.ptg_parts.ptg2_source_witness_locator_reader import read_scanner_bundle_locators
from process.ptg_parts.ptg2_source_witness_persisted_encode import encode_persisted_source_witness
from process.ptg_parts.ptg2_source_witness_primitives import U32
from tests.ptg2_candidate_audit_batch_postgres_fixture import SOURCE_DIGEST
from tests.test_ptg2_source_witness_dictionary_bundle import _write_dictionary_bundle
from tests.test_ptg2_source_witness_fragment_pipeline import _candidates, _counts, _fragment_bundle


@pytest.fixture
def temporary_buffers(monkeypatch, request):
    buffers = []
    request.addfinalizer(lambda: [buffer.close() for buffer in buffers])

    def temporary_file(**_kwargs):
        buffer = BytesIO()
        buffers.append(buffer)
        return buffer

    monkeypatch.setattr(encode.tempfile, "TemporaryFile", temporary_file)
    return buffers


def _fragment_locators(tmp_path):
    return read_scanner_bundle_locators(_fragment_bundle(tmp_path, _candidates()))[1]


@pytest.mark.parametrize("legacy_kind", ["inline", "dictionary_locator"])
def test_mixed_formats_preserve_exact_payload_and_original_records(tmp_path, legacy_kind):
    candidates = _candidates()
    if legacy_kind == "inline":
        first = candidates[0]
    else:
        entry = _write_dictionary_bundle(tmp_path, [candidate.compressed for candidate in candidates])
        first = read_scanner_bundle_locators(entry)[1][0]
    selected_candidates = [first, _fragment_locators(tmp_path)[1]]
    current = encode.encode_fragment_source_witness_candidates(selected_candidates, _counts(2))
    assert current == encode.encode_fragment_source_witness_candidates(candidates, _counts(2))
    legacy_payload, legacy_metadata = encode_persisted_source_witness(candidates, _counts(2))
    legacy = witness.decode_persisted_source_witness(legacy_payload, expected_raw_source_sha256=[SOURCE_DIGEST])
    loaded = witness.decode_persisted_source_witness(
        current[0], expected_raw_source_sha256=[SOURCE_DIGEST], expected_metadata=current[1]
    )
    assert tuple(loaded.records) == legacy.records
    assert loaded.metadata["sample_digest"] == legacy_metadata["sample_digest"]


@pytest.mark.parametrize("evidence_index", [0, 1])
def test_encoder_rejects_raw_or_linked_token_digest_corruption(evidence_index, temporary_buffers):
    candidate = _candidates()[0]
    compressed, evidence_by_sha256 = externalize_source_evidence_record(candidate.compressed, SOURCE_DIGEST)
    digest = tuple(evidence_by_sha256)[evidence_index]
    evidence_by_sha256[digest] = b"corrupted exact evidence"
    corrupted = replace(candidate, compressed=compressed, evidence_by_sha256=evidence_by_sha256)
    with pytest.raises(RuntimeError, match="complete token digest"):
        encode.encode_fragment_source_witness_candidates([corrupted], _counts(1))
    assert len(temporary_buffers) == 1 and temporary_buffers[0].closed


def test_encoder_rejects_selected_record_digest_corruption(tmp_path, temporary_buffers):
    locator = replace(_fragment_locators(tmp_path)[0], compressed_sha256="00" * 32)
    with pytest.raises(RuntimeError, match="bundle changed before materialization"):
        encode.encode_fragment_source_witness_candidates([locator], _counts(1))
    assert len(temporary_buffers) == 1 and temporary_buffers[0].closed


@pytest.mark.parametrize("mutation", ["size", "replaced_inode", "fragment_content"])
def test_encoder_rejects_immutable_source_mutation(tmp_path, mutation, temporary_buffers):
    locator = _fragment_locators(tmp_path)[0]
    path = Path(locator.bundle.path)
    if mutation == "size":
        with path.open("ab") as file:
            file.write(b"changed")
    elif mutation == "replaced_inode":
        replacement = tmp_path / "replacement.bin"
        replacement.write_bytes(path.read_bytes())
        replacement.replace(path)
    else:
        evidence = next(iter(locator.evidence_by_sha256.values()))
        fragment = next(iter(evidence.fragments_by_sha256.values()))
        original_stat = path.stat()
        with path.open("r+b") as file:
            file.seek(fragment.offset)
            first = file.read(1)
            file.seek(fragment.offset)
            file.write(bytes([first[0] ^ 255]))
        os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    with pytest.raises(RuntimeError, match="changed before materialization|invalid zlib framing"):
        encode.encode_fragment_source_witness_candidates([locator], _counts(1))
    assert len(temporary_buffers) == 1 and temporary_buffers[0].closed


def test_encoder_rejects_source_mutation_during_materialization(monkeypatch, tmp_path, temporary_buffers):
    locators = _fragment_locators(tmp_path)
    original = encode.read_fragment_locator_token

    def read_then_mutate(*args):
        raw = original(*args)
        with Path(locators[0].bundle.path).open("ab") as file:
            file.write(b"changed after evidence read")
        return raw

    monkeypatch.setattr(encode, "read_fragment_locator_token", read_then_mutate)
    with pytest.raises(RuntimeError, match="changed during materialization"):
        encode.encode_fragment_source_witness_candidates(locators, _counts(2))
    assert len(temporary_buffers) == 1 and temporary_buffers[0].closed


def test_fragment_payload_bound_includes_every_exact_frame(monkeypatch, temporary_buffers):
    candidates = _candidates()
    payload, metadata = encode.encode_fragment_source_witness_candidates(candidates, _counts(2))
    header_bytes = U32.unpack_from(payload, len(encode.FRAGMENT_PERSISTED_PAYLOAD_MAGIC))[0]
    expected_size = (
        len(encode.FRAGMENT_PERSISTED_PAYLOAD_MAGIC)
        + U32.size
        + header_bytes
        + U32.size * 3
        + metadata["fragment_count"] * 40
        + metadata["evidence_dictionary_stored_bytes"]
        + metadata["evidence_dictionary_count"] * 40
        + metadata["recipe_reference_count"] * 32
        + len(candidates) * 36
        + sum(
            len(externalize_source_evidence_record(candidate.compressed, SOURCE_DIGEST)[0]) for candidate in candidates
        )
    )
    assert metadata["payload_bytes"] == len(payload) == expected_size
    for module in (encode, streaming):
        monkeypatch.setattr(module, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", expected_size)
    assert encode.encode_fragment_source_witness_candidates(candidates, _counts(2)) == (payload, metadata)
    monkeypatch.setattr(encode, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", expected_size - 1)
    previous_count = len(temporary_buffers)
    with pytest.raises(WitnessPayloadLimitError, match="logical byte bound"):
        encode.encode_fragment_source_witness_candidates(candidates, _counts(2))
    assert len(temporary_buffers) == previous_count + 1
    assert all(buffer.closed for buffer in temporary_buffers)


@pytest.mark.parametrize(
    ("constant", "metric"),
    [
        ("PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENTS", "fragment_count"),
        ("PTG2_V3_SOURCE_WITNESS_MAX_RECIPES", "evidence_dictionary_count"),
        ("PTG2_V3_SOURCE_WITNESS_MAX_FRAGMENT_REFERENCES", "recipe_reference_count"),
        ("PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES", "evidence_reconstructed_bytes"),
    ],
)
def test_fragment_counts_and_work_enforce_exact_bounds(monkeypatch, constant, metric):
    expected = encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    maximum = expected[1][metric]
    monkeypatch.setattr(encode, constant, maximum)
    assert encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2)) == expected
    monkeypatch.setattr(encode, constant, maximum - 1)
    with pytest.raises(WitnessPayloadLimitError, match="count|work"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))


def test_unique_fragment_decoded_bytes_bound_counts_shared_evidence_once(monkeypatch):
    expected = encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    maximum = expected[1]["evidence_dictionary_raw_bytes"]
    monkeypatch.setattr(streaming, "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES", maximum)
    assert encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2)) == expected
    monkeypatch.setattr(streaming, "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES", maximum - 1)
    with pytest.raises(WitnessPayloadLimitError, match="aggregate decoded"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))


def test_persisted_record_entry_bound_includes_digest_and_length_framing(monkeypatch):
    candidates = _candidates()
    largest = max(
        len(externalize_source_evidence_record(candidate.compressed, SOURCE_DIGEST)[0]) for candidate in candidates
    )
    with BytesIO() as file:
        stage = encode._FragmentStage(file)
        staged_records = encode._stage_records(stage, candidates)
        for module in (encode, streaming):
            monkeypatch.setattr(module, "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES", largest + 36, raising=False)
        assert encode._write_payload(stage, staged_records, {"record_count": 2})
        monkeypatch.setattr(encode, "PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES", largest + 35)
        with pytest.raises(WitnessPayloadLimitError, match="stored|frame|entry"):
            encode._write_payload(stage, staged_records, {"record_count": 2})


@pytest.mark.parametrize("failure", [OSError, asyncio.CancelledError])
def test_staging_failure_closes_its_buffer_without_output(monkeypatch, temporary_buffers, failure):
    original = encode._append_stage

    def stage_then_fail(*args):
        original(*args)
        raise failure("staging stopped")

    monkeypatch.setattr(encode, "_append_stage", stage_then_fail)
    with pytest.raises(failure, match="staging stopped"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    assert len(temporary_buffers) == 1 and temporary_buffers[0].closed


@pytest.mark.parametrize("failure", [OSError, asyncio.CancelledError])
def test_output_failure_closes_staging_and_partial_output(monkeypatch, temporary_buffers, failure):
    def copy_then_fail(_stage, output, _payload, _written):
        output.write(b"partial output")
        raise failure("output stopped")

    monkeypatch.setattr(encode, "_copy_staged_payload", copy_then_fail)
    with pytest.raises(failure, match="output stopped"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    assert len(temporary_buffers) == 2
    assert all(buffer.closed for buffer in temporary_buffers)


def test_output_accounting_mismatch_cannot_return_a_payload(monkeypatch, temporary_buffers):
    monkeypatch.setattr(encode, "_copy_staged_payload", lambda _stage, _output, _payload, written: written)
    with pytest.raises(RuntimeError, match="staging byte count changed"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    assert len(temporary_buffers) == 2
    assert all(buffer.closed for buffer in temporary_buffers)


def test_truncated_output_cannot_return_a_payload(monkeypatch, request):
    class TruncatedOutput(BytesIO):
        def read(self, size=-1):
            return super().read(size)[:-1]

    buffers = [BytesIO(), TruncatedOutput()]
    request.addfinalizer(lambda: [buffer.close() for buffer in buffers])
    pending = iter(buffers)
    monkeypatch.setattr(encode.tempfile, "TemporaryFile", lambda **_kwargs: next(pending))
    with pytest.raises(RuntimeError, match="staging file is truncated"):
        encode.encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    assert all(buffer.closed for buffer in buffers)


def test_encoder_rejects_invalid_candidates_and_incomplete_locator_staging(monkeypatch, tmp_path):
    with pytest.raises(RuntimeError, match="candidate is invalid"):
        encode.encode_fragment_source_witness_candidates([object()], _counts(1))
    locators = _fragment_locators(tmp_path)
    monkeypatch.setattr(encode, "_stage_bundle_candidates", lambda *_args: None)
    with pytest.raises(RuntimeError, match="staging is incomplete"):
        encode.encode_fragment_source_witness_candidates(locators, _counts(2))


def test_fragment_stage_rejects_corrupt_existing_recipe_or_fragment_size():
    raw = b'{"small":true}'
    digest = hashlib.sha256(raw).hexdigest()
    with BytesIO() as file:
        stage = encode._FragmentStage(file)
        stage.token(digest, raw)
        stage.recipes[digest] = replace(stage.recipes[digest], raw_length=len(raw) + 1)
        with pytest.raises(RuntimeError, match="recipe digest is inconsistent"):
            stage.token(digest, raw)
        stage.fragments[digest] = replace(stage.fragments[digest], raw_byte_count=len(raw) + 1)
        with pytest.raises(RuntimeError, match="fragment digest is inconsistent"):
            stage.fragment(digest, raw)
