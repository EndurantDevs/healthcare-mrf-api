# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact v4 scanner-to-v6 persistence parity, including native PostgreSQL."""

from __future__ import annotations

import hashlib
import json
import os
import zlib
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_source_witness as witness
from process.ptg_parts import ptg2_source_witness_fragment_encode as fragment_encode
from process.ptg_parts import ptg2_source_witness_streaming_encode as streaming
from process.ptg_parts.ptg2_candidate_audit_evidence import source_audit_condition
from process.ptg_parts.ptg2_source_witness_codec import externalize_source_evidence_record
from process.ptg_parts.ptg2_source_witness_contract import WitnessPayloadLimitError
from process.ptg_parts.ptg2_source_witness_fragment_encode import encode_fragment_source_witness_candidates
from process.ptg_parts.ptg2_source_witness_fragments import encode_fragment_recipe
from process.ptg_parts.ptg2_source_witness_locator_reader import read_scanner_bundle_locators
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
    encode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_primitives import U32
from process.ptg_parts.ptg2_source_witness_selection import source_set_digest
from tests.ptg2_candidate_audit_batch_postgres_fixture import SOURCE_DIGEST, compressed_occurrence
from tests.test_ptg2_source_witness import SOURCE_A, _record
from tests.test_ptg2_source_witness_dictionary_bundle import _scanner_bundle_header


def _candidates():
    return (
        compressed_occurrence(0, code_system="CPT", code="99213"),
        compressed_occurrence(1, code_system="REVENUE_CODE", code="450"),
    )


def _counts(count):
    return SourceWitnessPayloadCounts(1, source_set_digest([SOURCE_DIGEST]), count, 0, count, 0, count, 0, count)


def _fragment_bundle(tmp_path, candidates):
    fragment_by_sha256 = {}
    recipe_by_sha256 = {}
    compressed_records = []
    for candidate in candidates:
        compressed, evidence = externalize_source_evidence_record(candidate.compressed, candidate.raw_source_sha256)
        compressed_records.append(compressed)
        for digest, raw in evidence.items():
            recipe_by_sha256[digest] = encode_fragment_recipe(raw, fragment_by_sha256.__setitem__)
    header = json.loads(_scanner_bundle_header(len(compressed_records)))
    header.update(
        format_version=4,
        evidence_encoding="fixed_byte_fragments_v1",
        fragment_byte_count=4096,
        fragment_count=len(fragment_by_sha256),
        recipe_reference_count=sum(len(recipe["fragment_sha256"]) for recipe in recipe_by_sha256.values()),
        evidence_reconstructed_bytes=sum(recipe["raw_byte_count"] for recipe in recipe_by_sha256.values()),
    )
    header_bytes = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
    bundle_payload = bytearray(
        b"PTG2SW04" + U32.pack(len(header_bytes)) + header_bytes + U32.pack(len(fragment_by_sha256))
    )
    for digest, raw in sorted(fragment_by_sha256.items()):
        compressed = zlib.compress(raw)
        bundle_payload.extend(bytes.fromhex(digest) + U32.pack(len(raw)) + U32.pack(len(compressed)) + compressed)
    bundle_payload.extend(U32.pack(len(recipe_by_sha256)))
    for digest, recipe in sorted(recipe_by_sha256.items()):
        bundle_payload.extend(
            bytes.fromhex(digest) + U32.pack(recipe["raw_byte_count"]) + U32.pack(len(recipe["fragment_sha256"]))
        )
        bundle_payload.extend(b"".join(bytes.fromhex(fragment) for fragment in recipe["fragment_sha256"]))
    bundle_payload.extend(U32.pack(len(compressed_records)))
    for compressed in compressed_records:
        bundle_payload.extend(U32.pack(len(compressed)) + compressed)
    path = tmp_path / "synthetic-fragment-bundle.bin"
    path.write_bytes(bundle_payload)
    return {
        "path": str(path),
        "sha256": hashlib.sha256(bundle_payload).hexdigest(),
        "byte_count": len(bundle_payload),
        "row_count": len(compressed_records),
        "raw_source_sha256": candidates[0].raw_source_sha256,
    }


def test_fragment_scanner_merge_retains_original_records_and_audit_conditions(tmp_path):
    candidates = _candidates()
    legacy_payload, legacy_metadata = encode_persisted_source_witness(candidates, _counts(2))
    legacy = witness.decode_persisted_source_witness(legacy_payload, expected_raw_source_sha256=[SOURCE_DIGEST])
    entry = _fragment_bundle(tmp_path, candidates)
    payload, metadata = witness.build_persisted_source_witness([entry], expected_raw_source_sha256=[SOURCE_DIGEST])
    current = witness.decode_persisted_source_witness(
        payload, expected_raw_source_sha256=[SOURCE_DIGEST], expected_metadata=metadata
    )
    assert payload.startswith(b"PTG2SWP6")
    assert current.metadata["format_version"] == 6
    assert current.metadata["sample_digest"] == legacy_metadata["sample_digest"]
    assert tuple(current.records) == legacy.records
    assert tuple(source_audit_condition(record) for record in current.records) == tuple(
        source_audit_condition(record, parsed_evidence_by_sha256=legacy.evidence_by_sha256) for record in legacy.records
    )


def test_fragment_sharing_fits_scaled_bound_without_thinning_sample(monkeypatch, tmp_path):
    candidates = []
    for index in range(3):
        token = b" " * 4096 + json.dumps({"negotiated_prices": [], "example": index}).encode()
        candidates.append(
            streaming.CompressedSourceWitnessRecord(
                "rate_occurrence",
                index,
                "22" * 32,
                SOURCE_A,
                _record(kind="rate_occurrence", priority=index, item_ordinal=index, raw_json=token),
            )
        )
    monkeypatch.setattr(streaming, "PTG2_V3_SOURCE_WITNESS_MAX_DECODED_TOTAL_BYTES", 5000)
    with pytest.raises(WitnessPayloadLimitError):
        streaming.encode_persisted_source_witness_candidates(candidates, _counts(3))
    entry = _fragment_bundle(tmp_path, candidates)
    payload, metadata = witness.build_persisted_source_witness([entry], expected_raw_source_sha256=[SOURCE_A])
    loaded = witness.decode_persisted_source_witness(payload, expected_raw_source_sha256=[SOURCE_A])
    assert metadata["record_count"] == 3
    assert metadata["evidence_dictionary_raw_bytes"] < 5000 < metadata["evidence_reconstructed_bytes"]
    assert [record.raw_json for record in loaded.records] == [
        b" " * 4096 + json.dumps({"negotiated_prices": [], "example": index}).encode() for index in range(3)
    ]


@pytest.mark.parametrize("failure", ["corrupt_fragment", "reconstruction_budget"])
def test_fragment_pipeline_failure_closes_partial_scratch_and_preserves_bundle(monkeypatch, tmp_path, failure):
    entry = _fragment_bundle(tmp_path, _candidates())
    locators = read_scanner_bundle_locators(entry)[1]
    path = Path(entry["path"])
    if failure == "corrupt_fragment":
        fragment = next(iter(locators[-1].evidence_by_sha256.values())).fragments_by_sha256
        frame = next(iter(fragment.values()))
        raw = bytearray(path.read_bytes())
        raw[frame.offset] ^= 255
        path.write_bytes(raw)
        # Authenticate the actual corrupt file at the outer boundary, so the
        # inner evidence validator must still reject it rather than a stale hash.
        entry["sha256"] = hashlib.sha256(raw).hexdigest()
    else:
        first_recipe = next(iter(locators[0].evidence_by_sha256.values()))
        monkeypatch.setattr(
            fragment_encode, "PTG2_V3_SOURCE_WITNESS_MAX_RECONSTRUCTED_BYTES", first_recipe.raw_byte_count
        )
    original_bytes = path.read_bytes()
    temporary_file = fragment_encode.tempfile.TemporaryFile
    scratch_files = []
    staged_sizes = []

    def tracked_file(**kwargs):
        file = temporary_file(dir=tmp_path, **kwargs)
        scratch_files.append(file)
        return file

    original_token = fragment_encode._FragmentStage.token

    def stage_token(stage, digest, raw):
        original_token(stage, digest, raw)
        staged_sizes.append(stage.file.tell())

    monkeypatch.setattr(fragment_encode.tempfile, "TemporaryFile", tracked_file)
    monkeypatch.setattr(fragment_encode._FragmentStage, "token", stage_token)
    with pytest.raises(RuntimeError, match="zlib|reconstruction work"):
        witness.build_persisted_source_witness([entry], expected_raw_source_sha256=[SOURCE_DIGEST])
    assert staged_sizes and staged_sizes[0] > 0
    assert scratch_files and all(file.closed for file in scratch_files)
    assert path.read_bytes() == original_bytes
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.asyncio
async def test_fragment_payload_native_postgres_candidate_audit(monkeypatch):
    if os.getenv("HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST=1")
    from tests import test_ptg2_candidate_audit_batch_postgres as postgres

    def source_witness_v6():
        return encode_fragment_source_witness_candidates(_candidates(), _counts(2))

    monkeypatch.setattr(postgres, "source_witness", source_witness_v6)
    case = postgres._candidate_batch_case()
    postgres._patch_candidate_modules(monkeypatch, case)
    result, block_io = await postgres._run_candidate_batch(case)
    assert case.witness_metadata["format_version"] == 6
    assert result.matched_challenge_count == result.unique_challenge_count == 2
    assert result.validated_persisted_audit_occurrence_count == 2
    assert result.witness_io["unique_evidence_entries"] == 2
    assert result.witness_io["repeated_evidence_decompressions"] > 0
    assert result.witness_io["repeated_evidence_json_parses"] == 2
    assert result.witness_io["token_reconstructions"] == 4
    postgres._assert_block_ledger(block_io)


@pytest.mark.asyncio
async def test_fragment_payload_native_postgres_partitioned_audit(monkeypatch):
    if os.getenv("HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_AUDIT_BATCH_POSTGRES_TEST=1")
    from tests import test_ptg2_candidate_audit_batch_postgres as postgres

    monkeypatch.setattr(
        postgres, "source_witness", lambda: encode_fragment_source_witness_candidates(_candidates(), _counts(2))
    )
    await postgres.test_real_postgres_partition_plan_dispatches_every_item_once(monkeypatch)
