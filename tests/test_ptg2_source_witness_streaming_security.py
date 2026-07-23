from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from process.ptg_parts import ptg2_source_witness_locator_materialize as materialize
from process.ptg_parts import ptg2_source_witness_store as witness_store
from process.ptg_parts import ptg2_source_witness_streaming_encode as streaming
from process.ptg_parts.ptg2_shared_blocks import SharedLayoutBuildOwnership
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    WitnessPayloadLimitError,
)
from tests.test_ptg2_source_witness import SOURCE_A, _record
from tests.test_ptg2_source_witness_security_paydown import (
    _dummy_locator,
    _one_locator,
    _payload_counts,
)


def test_materialization_passthrough_and_incomplete_guard(monkeypatch) -> None:
    compressed = CompressedSourceWitnessRecord(
        kind="rate_occurrence",
        priority=1,
        tie_breaker="a" * 64,
        raw_source_sha256=SOURCE_A,
        compressed=b"x",
    )
    assert materialize.materialize_source_witness_locators([compressed]) == [compressed]

    monkeypatch.setattr(materialize, "_materialize_bundle_locators", lambda *_args: None)
    with pytest.raises(RuntimeError, match="materialization is incomplete"):
        materialize.materialize_source_witness_locators([_dummy_locator()])


def test_locator_decoder_rejects_embedded_legacy_evidence() -> None:
    compressed = _record(
        kind="rate_occurrence",
        priority=1,
        item_ordinal=1,
        raw_json=b'{"rate":1}',
    )
    with pytest.raises(RuntimeError, match="unexpectedly embeds"):
        witness_codec.decode_record_locator_fields(compressed)


def test_streaming_budget_and_stage_guards(monkeypatch) -> None:
    budget = streaming._StreamingBudget()
    budget.register_decoded_evidence("a" * 64, 1)
    with pytest.raises(RuntimeError, match="locator is inconsistent"):
        budget.register_decoded_evidence("a" * 64, 2)

    with pytest.raises(WitnessPayloadLimitError, match="8 MiB stored"):
        streaming._append_stage(BytesIO(), b"")
    with pytest.raises(RuntimeError, match="staging file is truncated"):
        streaming._read_stage(BytesIO(b"x"), streaming._StagedPayload(0, 2))

    with pytest.raises(RuntimeError, match="evidence digest"):
        streaming._stage_evidence(
            BytesIO(),
            {},
            streaming._StreamingBudget(),
            evidence_sha256="0" * 64,
            raw_evidence=b"x",
        )

    raw = b"x"
    digest = hashlib.sha256(raw).hexdigest()
    existing = streaming._StagedEvidence(
        sha256=digest,
        raw_byte_count=1,
        compressed_sha256="0" * 64,
        payload=streaming._StagedPayload(0, 1),
    )
    with pytest.raises(RuntimeError, match="digest is inconsistent"):
        streaming._stage_evidence(
            BytesIO(),
            {digest: existing},
            streaming._StreamingBudget(),
            evidence_sha256=digest,
            raw_evidence=raw,
        )

    monkeypatch.setattr(streaming, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 1)
    with pytest.raises(WitnessPayloadLimitError, match="logical payload"):
        streaming._write_bounded(BytesIO(), b"xx", 0)


def test_streaming_locator_record_and_copy_guards(monkeypatch) -> None:
    locator = _dummy_locator()
    with pytest.raises(RuntimeError, match="bundle changed"):
        streaming._stage_locator_record(
            BytesIO(),
            replace(locator, compressed_sha256="0" * 64),
            BytesIO(b"x"),
            streaming._StreamingBudget(),
        )

    monkeypatch.setattr(
        streaming,
        "decode_record_locator_fields",
        lambda _record: SimpleNamespace(
            kind="provider_reference",
            priority=locator.priority,
            tie_breaker=locator.tie_breaker,
            raw_sha256=locator.raw_sha256,
            linked_provider_sha256=None,
        ),
    )
    with pytest.raises(RuntimeError, match="record locator is inconsistent"):
        streaming._stage_locator_record(
            BytesIO(),
            locator,
            BytesIO(b"x"),
            streaming._StreamingBudget(),
        )

    with pytest.raises(RuntimeError, match="staging file is truncated"):
        streaming._copy_staged_payload(
            BytesIO(b"x"),
            BytesIO(),
            streaming._StagedPayload(0, 2),
            0,
        )


def test_streaming_encoding_guards_projected_written_and_read_sizes(
    monkeypatch,
) -> None:
    stage = BytesIO(b"x")
    staged_record = streaming._StagedRecord(
        SOURCE_A,
        streaming._StagedPayload(0, 1),
    )
    header_by_field = {"record_count": 1}

    monkeypatch.setattr(streaming, "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES", 1)
    with pytest.raises(WitnessPayloadLimitError, match="logical payload"):
        streaming._encode_staged_payload(header_by_field, [staged_record], [], stage)

    monkeypatch.setattr(
        streaming,
        "PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES",
        1024 * 1024,
    )
    monkeypatch.setattr(
        streaming,
        "_copy_staged_payload",
        lambda _stage, _output, _payload, written: written,
    )
    with pytest.raises(RuntimeError, match="byte count changed"):
        streaming._encode_staged_payload(header_by_field, [staged_record], [], stage)

    class TruncatedOutput(BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

        def read(self, size: int = -1) -> bytes:
            if size > 1:
                size -= 2
            return super().read(size)

    def copy_exact(stage_file, output_file, payload, written):
        copied = stage_file.getvalue()[payload.offset : payload.offset + payload.length]
        output_file.write(copied)
        return written + len(copied)

    monkeypatch.setattr(streaming, "_copy_staged_payload", copy_exact)
    monkeypatch.setattr(
        streaming.tempfile,
        "TemporaryFile",
        lambda **_kwargs: TruncatedOutput(),
    )
    with pytest.raises(RuntimeError, match="staging file is truncated"):
        streaming._encode_staged_payload(header_by_field, [staged_record], [], stage)


def test_streaming_incomplete_staging_guard(tmp_path, monkeypatch) -> None:
    locator = _one_locator(tmp_path)
    monkeypatch.setattr(streaming, "_stage_locator_bundles", lambda *_args: None)
    with pytest.raises(RuntimeError, match="staging is incomplete"):
        streaming.encode_persisted_source_witness_candidates(
            [locator],
            _payload_counts(),
        )


@pytest.mark.asyncio
async def test_witness_store_publish_threads_generation_and_transaction(
    monkeypatch,
) -> None:
    metadata = {
        "record_count": 2,
        "sample_digest": "a" * 64,
        "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
    }
    session = object()
    observations_by_name: dict[str, object] = {}

    monkeypatch.setattr(
        witness_store,
        "build_persisted_source_witness",
        lambda *_args, **_kwargs: (b"payload", metadata),
    )

    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(witness_store.db, "transaction", transaction)

    async def lock(observed_session, **kwargs):
        observations_by_name["lock"] = (observed_session, kwargs)

    async def replace_row(**kwargs):
        observations_by_name["replace"] = kwargs

    monkeypatch.setattr(witness_store, "lock_shared_layout_for_dense_write", lock)
    monkeypatch.setattr(witness_store, "_replace_source_witness_row", replace_row)
    monkeypatch.setattr(witness_store, "shared_support_digest", lambda _payload: b"d" * 32)

    publication = await witness_store.publish_shared_source_witness(
        schema_name="mrf",
        build_ownership=SharedLayoutBuildOwnership(17, "token"),
        entries=(),
        expected_raw_source_sha256=(),
        expected_generation="shared_blocks_v4",
    )

    assert observations_by_name["lock"][0] is session
    assert (
        observations_by_name["lock"][1]["expected_generation"]
        == "shared_blocks_v4"
    )
    assert observations_by_name["replace"]["witness_payload"] == b"payload"
    assert publication.row_count == 2
    assert publication.stored_byte_count == len(b"payload")
