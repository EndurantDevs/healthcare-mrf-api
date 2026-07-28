# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
import zlib

import pytest

from process import uhc_semantic_stage_verifier as verifier_module
from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UhcSemanticBuildClaim,
    UhcSemanticBuildError,
    UhcSemanticBuildIdentity,
)
from process.uhc_semantic_stage_verifier import (
    _EvidenceRangeState,
    _EvidenceRunAccumulator,
    _FactBlockVerifier,
    _evidence_identity,
    _fact_identity,
)


def test_fact_verifier_is_chunk_boundary_independent() -> None:
    source_file_id = hashlib.sha256(b"source").hexdigest()
    global_digest = hashlib.sha256()
    verifier = _FactBlockVerifier(
        source_file_id=source_file_id,
        fact_type="ProviderMembershipRecord",
        record_start=7,
        expected_record_count=2,
        max_record_bytes=1024,
        global_identity_digest=global_digest,
        global_identity_count=0,
    )
    first = b'{"npi":"1003821380"}'
    second = b'{"npi":"1003821398"}'
    framed_facts = first + b"\n" + second + b"\n"
    for chunk in (
        framed_facts[:3],
        framed_facts[3:19],
        framed_facts[19:],
    ):
        verifier.consume(chunk)

    block_hash = verifier.finish(7)
    expected_identities = [
        _fact_identity(
            source_file_id,
            "ProviderMembershipRecord",
            ordinal,
            hashlib.sha256(fact_bytes).hexdigest(),
        )
        for ordinal, fact_bytes in ((7, first), (8, second))
    ]
    expected = hashlib.sha256(b"\n".join(expected_identities)).hexdigest()
    assert block_hash == expected
    assert global_digest.hexdigest() == expected


def test_fact_verifier_rejects_unbounded_or_unframed_payload() -> None:
    verifier = _FactBlockVerifier(
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        fact_type="ProviderMembershipRecord",
        record_start=0,
        expected_record_count=1,
        max_record_bytes=8,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    with pytest.raises(Exception, match="memory bound"):
        verifier.consume(b'{"too":"long"}')


def test_evidence_identity_matches_rust_packed_signature_encoding() -> None:
    signature_pack = b"".join(
        hashlib.sha256(signature_value.encode()).digest()
        for signature_value in (
            '"ACCEPTING"',
            "[]",
            '"2026-07-01"',
            "null",
            "null",
            '"F"',
            '"Zoë"',
            "INDIVIDUAL",
            '["Family Medicine"]',
        )
    )
    evidence_by_field = {
        "occurrence_ordinal": 3,
        "npi": "1003821380",
        "conflict_signature_pack": signature_pack,
    }
    expected = json.dumps(
        [
            3,
            "1003821380",
            signature_pack.hex(),
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()

    assert _evidence_identity(evidence_by_field) == expected
    assert UHC_SEMANTIC_CONTRACT_ID in _fact_identity(
        hashlib.sha256(b"source").hexdigest(),
        UHC_SEMANTIC_CONTRACT_ID,
        0,
        hashlib.sha256(b"payload").hexdigest(),
    ).decode()


def _identity(collection_kind: str = "provider_membership"):
    return UhcSemanticBuildIdentity(
        catalog_set_sha256=hashlib.sha256(b"catalog").hexdigest(),
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        artifact_sha256=hashlib.sha256(b"artifact").hexdigest(),
        raw_contract_version=2,
        raw_range_count=4,
        collection_kind=collection_kind,
        encoder_sha256=hashlib.sha256(b"encoder").hexdigest(),
    )


def _claim(*, sealed_reuse=False):
    identity = _identity()
    return UhcSemanticBuildClaim(
        semantic_build_id=identity.semantic_build_id,
        lease_token="lease",
        attempt_count=1,
        stage_schema="mrf_test",
        stage_relation="semantic_stage",
        sealed_reuse=sealed_reuse,
    )


def _fact_block_fixture():
    identity = _identity()
    fact_type = "ProviderMembershipRecord"
    fact = b'{"npi":"1003821380"}'
    compressed = zlib.compress(fact + b"\n")
    fact_identity = _fact_identity(
        identity.source_file_id,
        fact_type,
        0,
        hashlib.sha256(fact).hexdigest(),
    )
    return (
        identity,
        fact_type,
        compressed,
        {
            "range_ordinal": 0,
            "record_start": 0,
            "record_count": 1,
            "compressed_bytes": len(compressed),
            "compressed_payload_sha256": hashlib.sha256(compressed).hexdigest(),
            "semantic_block_sha256": hashlib.sha256(fact_identity).hexdigest(),
        },
    )


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def __aiter__(self):
        async def iterate():
            for row in self.rows:
                yield row

        return iterate()


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _BlobConnection:
    def __init__(self, blob=b"", rows=()):
        self.blob = blob
        self.rows = rows

    async def fetchval(self, _query, _ordinal, offset, requested):
        return self.blob[offset - 1 : offset - 1 + requested]

    def cursor(self, _query, *, prefetch):
        assert prefetch == 128
        return _Rows(self.rows)

    def transaction(self):
        return _Transaction()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("occurrence_ordinal", True),
        ("occurrence_ordinal", "1"),
        ("occurrence_ordinal", -1),
        ("npi", None),
        ("conflict_signature_pack", "not-bytes"),
        ("conflict_signature_pack", b"short"),
    ],
)
def test_evidence_identity_rejects_each_invalid_dimension(field, value):
    evidence_by_field = {
        "occurrence_ordinal": 0,
        "npi": "1003821380",
        "conflict_signature_pack": b"x" * (9 * 32),
    }
    evidence_by_field[field] = value
    with pytest.raises(UhcSemanticBuildError, match="identity is invalid"):
        _evidence_identity(evidence_by_field)


def test_line_digest_frames_multiple_payloads():
    digest = hashlib.sha256()
    count = verifier_module._update_line_digest(digest, 0, b"a")
    count = verifier_module._update_line_digest(digest, count, b"b")
    assert count == 2
    assert digest.hexdigest() == hashlib.sha256(b"a\nb").hexdigest()


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (b"\n", "framing"),
        (b"{}\r\n", "framing"),
        (b"not-json\n", "invalid JSON"),
        (b"[]\n", "not an object"),
    ],
)
def test_fact_verifier_rejects_each_malformed_frame(payload, match):
    verifier = _FactBlockVerifier(
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        fact_type="ProviderMembershipRecord",
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    with pytest.raises(UhcSemanticBuildError, match=match):
        verifier.consume(payload)


def test_fact_verifier_finish_rejects_unframed_and_count_mismatch():
    unframed = _FactBlockVerifier(
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        fact_type="ProviderMembershipRecord",
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    unframed.consume(b"{}")
    with pytest.raises(UhcSemanticBuildError, match="final newline"):
        unframed.finish(0)

    empty = _FactBlockVerifier(
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        fact_type="ProviderMembershipRecord",
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    with pytest.raises(UhcSemanticBuildError, match="count mismatch"):
        empty.finish(0)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("range_ordinal", -1),
        ("record_start", -1),
        ("record_count", 0),
        ("compressed_bytes", 0),
        ("compressed_payload_sha256", "bad"),
        ("semantic_block_sha256", "bad"),
    ],
)
def test_fact_block_metadata_rejects_each_invalid_dimension(field, value):
    _identity_value, _fact_type, _compressed, metadata = _fact_block_fixture()
    metadata[field] = value
    with pytest.raises(UhcSemanticBuildError, match="metadata is invalid"):
        verifier_module._fact_block_metadata(metadata)


class _NoProgressDecompressor:
    unconsumed_tail = b""

    def decompress(self, pending, *, max_length):
        del max_length
        self.unconsumed_tail = pending
        return b""


def test_compressed_chunk_requires_forward_progress():
    _identity_value, fact_type, _compressed, _metadata = _fact_block_fixture()
    verifier = _FactBlockVerifier(
        source_file_id=_identity().source_file_id,
        fact_type=fact_type,
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    with pytest.raises(UhcSemanticBuildError, match="made no progress"):
        verifier_module._consume_compressed_chunk(
            _NoProgressDecompressor(),
            verifier,
            b"compressed",
        )


@pytest.mark.asyncio
async def test_compressed_fact_block_readback_and_bounded_failure():
    identity, fact_type, compressed, metadata = _fact_block_fixture()
    block = verifier_module._fact_block_metadata(metadata)
    verifier = _FactBlockVerifier(
        source_file_id=identity.source_file_id,
        fact_type=fact_type,
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    assert await verifier_module._read_compressed_fact_block(
        _BlobConnection(compressed),
        '"mrf"."stage"',
        block,
        verifier,
    ) == hashlib.sha256(compressed).hexdigest()
    assert verifier.finish(0) == metadata["semantic_block_sha256"]

    with pytest.raises(UhcSemanticBuildError, match="ended during"):
        await verifier_module._read_compressed_fact_block(
            _BlobConnection(compressed[:-1]),
            '"mrf"."stage"',
            block,
            _FactBlockVerifier(
                source_file_id=identity.source_file_id,
                fact_type=fact_type,
                record_start=0,
                expected_record_count=1,
                max_record_bytes=1024,
                global_identity_digest=hashlib.sha256(),
                global_identity_count=0,
            ),
        )


@pytest.mark.asyncio
async def test_compressed_fact_block_rejects_concatenated_stream():
    identity, fact_type, compressed, metadata = _fact_block_fixture()
    concatenated = compressed + zlib.compress(b"{}\n")
    metadata["compressed_bytes"] = len(concatenated)
    metadata["compressed_payload_sha256"] = hashlib.sha256(concatenated).hexdigest()
    with pytest.raises(UhcSemanticBuildError, match="concatenated"):
        await verifier_module._read_compressed_fact_block(
            _BlobConnection(concatenated),
            '"mrf"."stage"',
            verifier_module._fact_block_metadata(metadata),
            _FactBlockVerifier(
                source_file_id=identity.source_file_id,
                fact_type=fact_type,
                record_start=0,
                expected_record_count=1,
                max_record_bytes=1024,
                global_identity_digest=hashlib.sha256(),
                global_identity_count=0,
            ),
        )


@pytest.mark.asyncio
async def test_verify_fact_block_rejects_payload_and_semantic_hash_drift():
    identity, fact_type, compressed, metadata = _fact_block_fixture()
    for field in ("compressed_payload_sha256", "semantic_block_sha256"):
        drifted_metadata_by_field = dict(metadata)
        drifted_metadata_by_field[field] = "0" * 64
        with pytest.raises(UhcSemanticBuildError, match="hash mismatch"):
            await verifier_module._verify_fact_block(
                _BlobConnection(compressed),
                '"mrf"."stage"',
                drifted_metadata_by_field,
                source_file_id=identity.source_file_id,
                fact_type=fact_type,
                max_record_bytes=1024,
                global_identity_digest=hashlib.sha256(),
                global_identity_count=0,
            )


@pytest.mark.asyncio
async def test_verify_fact_block_and_ordered_block_set_succeed():
    identity, fact_type, compressed, metadata = _fact_block_fixture()
    verified, identity_count = await verifier_module._verify_fact_block(
        _BlobConnection(compressed),
        '"mrf"."stage"',
        metadata,
        source_file_id=identity.source_file_id,
        fact_type=fact_type,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    assert verified["fact_count"] == 1
    assert identity_count == 1
    blocks, digest = await verifier_module._verify_all_fact_blocks(
        _BlobConnection(compressed),
        _claim(),
        identity,
        [metadata],
        fact_type,
        1024,
    )
    assert blocks == [verified]
    assert len(digest.hexdigest()) == 64


@pytest.mark.asyncio
async def test_readback_consumes_delayed_decompressor_output(monkeypatch):
    identity, fact_type, compressed, metadata = _fact_block_fixture()

    class _DelayedDecompressor:
        unconsumed_tail = b""
        eof = True
        unused_data = b""

        def __init__(self):
            self.empty_calls = 0

        def decompress(self, pending, *, max_length):
            del max_length
            if pending:
                return b""
            self.empty_calls += 1
            return b'{"npi":"1003821380"}\n' if self.empty_calls == 1 else b""

        def flush(self):
            return b""

    monkeypatch.setattr(
        verifier_module,
        "_consume_compressed_chunk",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        verifier_module.zlib,
        "decompressobj",
        _DelayedDecompressor,
    )
    block = verifier_module._fact_block_metadata(metadata)
    fact_verifier = _FactBlockVerifier(
        source_file_id=identity.source_file_id,
        fact_type=fact_type,
        record_start=0,
        expected_record_count=1,
        max_record_bytes=1024,
        global_identity_digest=hashlib.sha256(),
        global_identity_count=0,
    )
    assert await verifier_module._read_compressed_fact_block(
        _BlobConnection(compressed),
        '"mrf"."stage"',
        block,
        fact_verifier,
    ) == hashlib.sha256(compressed).hexdigest()
    assert fact_verifier.finish(0) == metadata["semantic_block_sha256"]


def test_fact_and_evidence_set_hashes_frame_multiple_entries():
    _identity_value, _fact_type, _compressed, metadata = _fact_block_fixture()
    block_by_field = {
        "range_ordinal": 0,
        "record_start": 0,
        "record_count": 1,
        "fact_count": 1,
        "compressed_payload_sha256": metadata["compressed_payload_sha256"],
        "semantic_block_sha256": metadata["semantic_block_sha256"],
    }
    assert len(
        verifier_module._fact_set_sha256(
            [block_by_field, block_by_field]
        )
    ) == 64
    range_proof_by_field = {
        "range_ordinal": 0,
        "evidence_count": 1,
        "run_count": 1,
        "layout_sha256": hashlib.sha256(b"layout").hexdigest(),
    }
    assert len(
        verifier_module._evidence_layout_sha256(
            [range_proof_by_field, range_proof_by_field]
        )
    ) == 64


def test_evidence_run_accumulator_accepts_empty_and_contiguous_runs():
    states = [_EvidenceRangeState()]
    accumulator = _EvidenceRunAccumulator(1, states)
    accumulator.finish()
    accumulator.switch_to((0, 0))
    accumulator.switch_to((0, 0))
    accumulator.add_identity(b"a")
    accumulator.switch_to((0, 1))
    accumulator.add_identity(b"b")
    accumulator.finish()
    assert states[0].run_count == 2
    assert states[0].evidence_count == 2


@pytest.mark.parametrize("key", [(-1, 0), (1, 0), (0, 1)])
def test_evidence_run_accumulator_rejects_range_or_run_gaps(key):
    accumulator = _EvidenceRunAccumulator(1, [_EvidenceRangeState()])
    accumulator.switch_to(key)
    if key == (0, 1):
        accumulator.add_identity(b"identity")
    with pytest.raises(UhcSemanticBuildError):
        accumulator.finish()


@pytest.mark.asyncio
async def test_evidence_identity_and_range_readback():
    signature = b"x" * (9 * 32)
    evidence_rows = [
        {
            "range_ordinal": 0,
            "run_ordinal": 0,
            "occurrence_ordinal": 0,
            "npi": "1003821380",
            "conflict_signature_pack": signature,
        },
        {
            "range_ordinal": 0,
            "run_ordinal": 1,
            "occurrence_ordinal": 1,
            "npi": "1003821398",
            "conflict_signature_pack": signature,
        },
    ]
    count, identity_hash = await verifier_module._verify_evidence_identities(
        _BlobConnection(rows=evidence_rows),
        '"mrf"."stage"',
    )
    ranges = await verifier_module._verify_evidence_ranges(
        _BlobConnection(rows=evidence_rows),
        '"mrf"."stage"',
        range_count=2,
    )
    assert count == 2
    assert len(identity_hash) == 64
    assert ranges[0]["run_count"] == 2
    assert ranges[1]["run_count"] == 0
    verified = await verifier_module._verify_evidence(
        _BlobConnection(rows=evidence_rows),
        '"mrf"."stage"',
        range_count=2,
    )
    assert verified[0] == 2


@pytest.mark.asyncio
async def test_evidence_identity_readback_rejects_occurrence_gap():
    with pytest.raises(UhcSemanticBuildError, match="not contiguous"):
        await verifier_module._verify_evidence_identities(
            _BlobConnection(
                rows=[
                    {
                        "occurrence_ordinal": 1,
                        "npi": "1003821380",
                        "conflict_signature_pack": b"x" * (9 * 32),
                    }
                ]
            ),
            '"mrf"."stage"',
        )


@pytest.mark.parametrize("max_record_bytes", [True, 0, 64 * 1024 * 1024 + 1])
def test_verifier_inputs_rejects_invalid_record_bound(max_record_bytes):
    with pytest.raises(UhcSemanticBuildError, match="record bound"):
        verifier_module._verifier_inputs(
            _identity(),
            {"max_record_bytes": max_record_bytes, "fact_blocks": [{}] * 4},
        )


@pytest.mark.parametrize(
    "fact_blocks",
    [None, [], [{}] * 3, ["bad"] * 4],
)
def test_verifier_inputs_rejects_invalid_fact_block_set(fact_blocks):
    with pytest.raises(UhcSemanticBuildError, match="fact blocks"):
        verifier_module._verifier_inputs(
            _identity(),
            {"max_record_bytes": 1024, "fact_blocks": fact_blocks},
        )


def test_verifier_inputs_selects_both_fact_types():
    report_by_field = {
        "max_record_bytes": 1024,
        "fact_blocks": [{}] * 4,
    }
    assert verifier_module._verifier_inputs(
        _identity(),
        report_by_field,
    )[2] == (
        "ProviderMembershipRecord"
    )
    assert verifier_module._verifier_inputs(
        _identity("plan_reference"),
        report_by_field,
    )[2] == "PlanReferenceRecord"


@pytest.mark.asyncio
async def test_all_fact_blocks_rejects_unordered_metadata():
    with pytest.raises(UhcSemanticBuildError, match="unordered"):
        await verifier_module._verify_all_fact_blocks(
            object(),
            _claim(),
            _identity(),
            [{"range_ordinal": 1}],
            "ProviderMembershipRecord",
            1024,
        )


def _agreement_fixture():
    block_by_field = {"range_ordinal": 0}
    evidence_range_by_field = {"range_ordinal": 0}
    report_by_field = {
        "fact_count": 1,
        "evidence_count": 1,
        "verifier_sha256": hashlib.sha256(b"verifier").hexdigest(),
    }
    native_by_field = {
        "fact_count": 1,
        "evidence_count": 1,
        "fact_blocks": [block_by_field],
        "evidence_ranges": [evidence_range_by_field],
    }
    return (
        report_by_field,
        native_by_field,
        [block_by_field],
        [evidence_range_by_field],
    )


def test_verifier_agreement_rejects_field_block_and_range_drift():
    report, native, blocks, ranges = _agreement_fixture()
    verifier_module._assert_verifier_agreement(report, native, blocks, ranges)
    for mutation in ("field", "blocks", "ranges"):
        changed_report = copy.deepcopy(report)
        changed_native = copy.deepcopy(native)
        changed_blocks = copy.deepcopy(blocks)
        changed_ranges = copy.deepcopy(ranges)
        if mutation == "field":
            changed_native["fact_count"] = 2
        elif mutation == "blocks":
            changed_blocks = []
        else:
            changed_ranges = []
        with pytest.raises(UhcSemanticBuildError, match="disagree"):
            verifier_module._assert_verifier_agreement(
                changed_report,
                changed_native,
                changed_blocks,
                changed_ranges,
            )


@pytest.mark.asyncio
async def test_verified_evidence_fields_selects_rows_or_copy_proof(monkeypatch):
    expected = (1, "a" * 64, "b" * 64, [{"range_ordinal": 0}])
    monkeypatch.setattr(
        verifier_module,
        "_verify_evidence",
        AsyncMock(return_value=expected),
    )
    assert await verifier_module._verified_evidence_fields(
        object(),
        _claim(),
        _identity(),
        {},
        None,
    ) == expected

    native_by_field = {
        "evidence_count": 0,
        "evidence_identity_set_sha256": "a" * 64,
        "evidence_layout_set_sha256": "b" * 64,
        "evidence_ranges": [],
        "output_bytes": 1,
        "copy_row_count": 4,
        "output_sha256": "c" * 64,
    }
    copy_observation_by_field = {
        "output_bytes": 1,
        "copy_row_count": 4,
        "output_sha256": "c" * 64,
    }
    assert await verifier_module._verified_evidence_fields(
        object(),
        _claim(),
        _identity(),
        native_by_field,
        copy_observation_by_field,
    ) == (0, "a" * 64, "b" * 64, [])


@pytest.mark.asyncio
async def test_verify_stage_rejects_reuse_and_builds_report(monkeypatch):
    with pytest.raises(UhcSemanticBuildError, match="needs no verifier"):
        await verifier_module.verify_uhc_semantic_stage(
            object(),
            _claim(sealed_reuse=True),
            _identity(),
            {},
        )

    block_by_field = {
        "range_ordinal": 0,
        "record_start": 0,
        "record_count": 1,
        "fact_count": 1,
        "compressed_payload_sha256": "a" * 64,
        "semantic_block_sha256": "b" * 64,
    }
    monkeypatch.setattr(
        verifier_module,
        "_verifier_inputs",
        lambda *_args: (1024, [{}], "ProviderMembershipRecord"),
    )
    monkeypatch.setattr(
        verifier_module,
        "_verify_all_fact_blocks",
        AsyncMock(return_value=([block_by_field], hashlib.sha256())),
    )
    monkeypatch.setattr(
        verifier_module,
        "_verified_evidence_fields",
        AsyncMock(return_value=(0, "c" * 64, "d" * 64, [])),
    )
    monkeypatch.setattr(
        verifier_module,
        "_assert_verifier_agreement",
        lambda *_args: None,
    )
    verification_result = await verifier_module.verify_uhc_semantic_stage(
        _BlobConnection(),
        _claim(),
        _identity(),
        {},
        copy_observation={
            "output_bytes": 1,
            "copy_row_count": 4,
            "output_sha256": "e" * 64,
        },
    )
    assert verification_result["fact_count"] == 1
    assert verification_result["output_bytes"] == 1
    assert len(verification_result["verifier_sha256"]) == 64


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("output_bytes", True),
        ("output_bytes", "1"),
        ("output_bytes", 0),
        ("output_bytes", 2),
        ("copy_row_count", True),
        ("copy_row_count", "4"),
        ("copy_row_count", 0),
        ("copy_row_count", 5),
        ("output_sha256", None),
        ("output_sha256", "a" * 63),
        ("output_sha256", "g" * 64),
        ("output_sha256", "b" * 64),
        ("copy_row_count", 3),
    ],
)
def test_copy_observation_rejects_every_invalid_dimension(field, value):
    native_by_field = {
        "evidence_count": 0,
        "output_bytes": 1,
        "copy_row_count": 4,
        "output_sha256": "a" * 64,
    }
    observed_by_field = {
        "output_bytes": 1,
        "copy_row_count": 4,
        "output_sha256": "a" * 64,
    }
    observed_by_field[field] = value
    with pytest.raises(UhcSemanticBuildError, match="COPY"):
        verifier_module._assert_copy_observation(
            native_by_field,
            observed_by_field,
            range_count=4,
        )


def _sealed_row():
    identity = _identity()
    copy_proof_by_field = {
        "output_bytes": 1,
        "copy_row_count": 4,
        "output_sha256": "a" * 64,
    }
    return identity, {
        "status": "sealed",
        "semantic_build_id": identity.semantic_build_id,
        "semantic_contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "semantic_contract_version": 2,
        "encoder_sha256": identity.encoder_sha256,
        "counters_json": {"copy_proof": copy_proof_by_field},
        "fact_blocks_json": [],
        "evidence_ranges_json": [],
        "copy_format_id": "copy-v1",
        "fact_count": 0,
        "evidence_count": 0,
        "fact_set_sha256": "b" * 64,
        "record_identity_set_sha256": "c" * 64,
        "evidence_identity_set_sha256": "d" * 64,
        "evidence_layout_set_sha256": "e" * 64,
        "attempt_count": 2,
        "stage_schema": "mrf_test",
        "stage_relation": "stage",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "building"),
        ("semantic_build_id", "wrong"),
        ("semantic_contract_id", "wrong"),
        ("semantic_contract_version", 1),
        ("encoder_sha256", "wrong"),
    ],
)
def test_sealed_identity_rejects_each_row_mutation(field, value):
    identity, row = _sealed_row()
    row[field] = value
    with pytest.raises(UhcSemanticBuildError, match="identity is invalid"):
        verifier_module._assert_sealed_identity(identity, row, 1024)


@pytest.mark.parametrize("record_bound", [True, 0, 64 * 1024 * 1024 + 1])
def test_sealed_identity_rejects_invalid_record_bound(record_bound):
    identity, row = _sealed_row()
    with pytest.raises(UhcSemanticBuildError, match="identity is invalid"):
        verifier_module._assert_sealed_identity(identity, row, record_bound)


def test_sealed_field_decode_and_native_report_contract():
    identity, row = _sealed_row()
    row["fact_blocks_json"] = "[]"
    assert verifier_module._decoded_sealed_field(row, "fact_blocks") == []
    row["fact_blocks_json"] = "{"
    with pytest.raises(UhcSemanticBuildError, match="fact_blocks is invalid"):
        verifier_module._decoded_sealed_field(row, "fact_blocks")

    _identity_value, missing_copy = _sealed_row()
    missing_copy["counters_json"] = {}
    with pytest.raises(UhcSemanticBuildError, match="COPY proof is missing"):
        verifier_module._sealed_native_report(
            identity,
            missing_copy,
            1024,
        )
    _identity_value, valid_row = _sealed_row()
    report = verifier_module._sealed_native_report(identity, valid_row, 1024)
    assert report["output_bytes"] == 1
    claim = verifier_module._sealed_readback_claim(identity, valid_row)
    assert claim.attempt_count == 2
    assert claim.stage_schema == "mrf_test"


@pytest.mark.asyncio
async def test_verify_sealed_build_delegates_exact_copy_proof(monkeypatch):
    identity, row = _sealed_row()
    expected_report_by_field = {"verified": True}
    delegated = AsyncMock(return_value=expected_report_by_field)
    monkeypatch.setattr(
        verifier_module,
        "verify_uhc_semantic_stage",
        delegated,
    )
    assert await verifier_module.verify_sealed_uhc_semantic_build(
        object(),
        identity,
        row,
        max_record_bytes=1024,
    ) == expected_report_by_field
    assert delegated.await_args.kwargs["copy_observation"] == {
        "output_bytes": 1,
        "output_sha256": "a" * 64,
        "copy_row_count": 4,
    }
