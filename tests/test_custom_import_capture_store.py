# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contract checks for generic custom-import capture registration."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

import process.custom_import.capture_store as capture_store
from process.custom_import.capture_store import (
    CaptureReceipt,
    CaptureStoreError,
    CaptureStoreTransactionRequired,
    ReplayableParquetCapture,
    register_capture_bundle,
    register_replayable_parquet_bundle,
)
from process.custom_import.snowflake import SnowflakeAcquisitionManifest, SnowflakeResultPartitionManifest


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _build_receipt(**changes: object) -> CaptureReceipt:
    receipt_values_by_field = {
        "stream_id": "records",
        "source_snapshot_token": "synthetic-snapshot-1",
        "byte_count": 17,
        "content_sha256": _digest("content"),
        "canonical_manifest": '{"connector":"synthetic","ordinal":1}',
        "manifest_sha256": _digest("manifest"),
    }
    receipt_values_by_field.update(changes)
    return CaptureReceipt(**receipt_values_by_field)


def _build_replayable_capture(
    parts: tuple[bytes, ...] = (b"parquet-part-one", b"parquet-part-two"),
) -> ReplayableParquetCapture:
    return ReplayableParquetCapture(
        receipt=_build_receipt(byte_count=sum(len(part) for part in parts)),
        parts=parts,
    )


def test_capture_receipt_requires_canonical_sealed_manifest_facts():
    receipt = _build_receipt()

    assert receipt.stream_id == "records"
    assert receipt.canonical_manifest == '{"connector":"synthetic","ordinal":1}'
    with pytest.raises(CaptureStoreError, match="canonical JSON"):
        _build_receipt(canonical_manifest='{"ordinal":1,"connector":"synthetic"}')
    with pytest.raises(CaptureStoreError, match="duplicate"):
        _build_receipt(canonical_manifest='{"connector":"synthetic","connector":"other"}')
    with pytest.raises(CaptureStoreError, match="content digest"):
        _build_receipt(content_sha256="not-a-digest")


def test_capture_receipt_preserves_a_verified_connector_domain_seal():
    partition = SnowflakeResultPartitionManifest(1, 17, _digest("partition"))
    manifest = SnowflakeAcquisitionManifest(
        request_sha256=_digest("request"),
        statement_sha256=_digest("statement"),
        source_snapshot_token="synthetic-snapshot-1",
        schema_fingerprint=_digest("schema"),
        result_partitions=(partition,),
        content_sha256=_digest("content"),
    )

    receipt = _build_receipt(
        byte_count=partition.content_bytes,
        content_sha256=manifest.content_sha256,
        canonical_manifest=manifest.canonical_manifest,
        manifest_sha256=manifest.manifest_sha256,
    )

    assert receipt.canonical_manifest == manifest.canonical_manifest
    assert receipt.manifest_sha256 == manifest.manifest_sha256
    assert receipt.manifest_sha256 != hashlib.sha256(receipt.canonical_manifest.encode("utf-8")).hexdigest()


async def test_registration_rejects_receipts_beyond_the_v1_stream_limit_first():
    receipt = _build_receipt()

    with pytest.raises(CaptureStoreError, match="v1 stream limit"):
        await register_capture_bundle(
            _ActiveTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(receipt,) * 10,
        )


async def test_registration_rejects_oversized_aggregate_manifests_before_database_work():
    large_manifest = '{"value":"' + ("x" * 1_100_000) + '"}'

    with pytest.raises(CaptureStoreError, match="aggregate byte limit"):
        await register_capture_bundle(
            _ActiveTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(
                _build_receipt(canonical_manifest=large_manifest),
                _build_receipt(stream_id="details", canonical_manifest=large_manifest),
            ),
        )


class _NoTransaction:
    def in_transaction(self) -> bool:
        return False


class _ActiveTransaction:
    new = dirty = deleted = ()

    def in_transaction(self) -> bool:
        return True


async def test_registration_requires_a_caller_owned_transaction_before_any_database_work():
    with pytest.raises(CaptureStoreTransactionRequired):
        await register_capture_bundle(
            _NoTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            receipts=(_build_receipt(),),
        )


async def test_durable_registration_requires_a_caller_owned_transaction_before_any_database_work():
    with pytest.raises(CaptureStoreTransactionRequired):
        await register_replayable_parquet_bundle(
            _NoTransaction(),
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
            captures=(_build_replayable_capture(),),
        )


def test_replayable_parquet_capture_binds_ordered_part_identity_without_exposing_parts():
    capture = _build_replayable_capture()
    expected = hashlib.sha256(
        b"custom-import/parquet-parts/v1\x00"
        + (1).to_bytes(4, byteorder="big")
        + len(capture.parts[0]).to_bytes(8, byteorder="big")
        + hashlib.sha256(capture.parts[0]).digest()
        + (2).to_bytes(4, byteorder="big")
        + len(capture.parts[1]).to_bytes(8, byteorder="big")
        + hashlib.sha256(capture.parts[1]).digest()
    ).digest()

    assert capture_store._payload_set_sha256(capture.parts) == expected
    assert capture_store._payload_set_sha256(capture.parts[::-1]) != expected
    assert "parts=" not in repr(capture)


def test_replayable_parquet_capture_rejects_count_size_and_byte_identity_drift(monkeypatch):
    with pytest.raises(CaptureStoreError, match="tuple"):
        ReplayableParquetCapture(receipt=_build_receipt(byte_count=1), parts=[b"x"])
    with pytest.raises(CaptureStoreError, match="non-empty"):
        ReplayableParquetCapture(receipt=_build_receipt(byte_count=0), parts=(b"",))
    with pytest.raises(CaptureStoreError, match="byte count"):
        ReplayableParquetCapture(receipt=_build_receipt(byte_count=2), parts=(b"x",))
    monkeypatch.setattr(capture_store, "_MAX_PARQUET_PART_BYTES", 1)
    with pytest.raises(CaptureStoreError, match="durable payload limit"):
        ReplayableParquetCapture(receipt=_build_receipt(byte_count=2), parts=(b"x", b"y"))
    monkeypatch.setattr(capture_store, "_MAX_PARQUET_PARTS_PER_CAPTURE", 1)
    with pytest.raises(CaptureStoreError, match="1 through 4096"):
        ReplayableParquetCapture(receipt=_build_receipt(byte_count=2), parts=(b"x", b"y"))


def test_capture_store_rejects_malformed_receipt_boundaries(monkeypatch):
    with pytest.raises(CaptureStoreError, match="stream_id"):
        _build_receipt(stream_id="UPPER")
    with pytest.raises(CaptureStoreError, match="byte_count"):
        _build_receipt(byte_count=True)
    with pytest.raises(CaptureStoreError, match="snapshot token"):
        _build_receipt(source_snapshot_token="\x00")
    with pytest.raises(CaptureStoreError, match="must be text"):
        capture_store._canonical_manifest(1)
    with pytest.raises(CaptureStoreError, match="valid UTF-8"):
        capture_store._canonical_manifest("\ud800")
    with pytest.raises(CaptureStoreError, match="byte limit"):
        capture_store._canonical_manifest("")
    with pytest.raises(CaptureStoreError, match="cannot contain NaN"):
        capture_store._canonical_manifest('{"value":NaN}')
    with pytest.raises(CaptureStoreError, match="not canonical JSON"):
        capture_store._canonical_manifest("{")
    with pytest.raises(CaptureStoreError, match="clean session"):
        capture_store._require_clean_session(SimpleNamespace(new=(object(),), dirty=(), deleted=()))
    with pytest.raises(CaptureStoreError, match="positive bigint"):
        capture_store._identity(dataset_id=0, definition_revision_id=1, schema_revision_id=1)

    receipt = _build_receipt()
    with pytest.raises(CaptureStoreError, match="non-empty tuple"):
        capture_store._validate_receipts([receipt])
    with pytest.raises(CaptureStoreError, match="declared receipt type"):
        capture_store._validate_receipts((object(),))
    with pytest.raises(CaptureStoreError, match="stream ids must be unique"):
        capture_store._validate_receipts((receipt, receipt))
    with pytest.raises(CaptureStoreError, match="one exact snapshot token"):
        capture_store._validate_receipts((receipt, _build_receipt(stream_id="details", source_snapshot_token="other")))
    with pytest.raises(CaptureStoreError, match="exactly cover"):
        capture_store._index_receipts_by_stream((("records", 1),), (_build_receipt(stream_id="details"),))

    prepared_receipt = _build_receipt()
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    monkeypatch.setattr(capture_store, "_MAX_MANIFEST_BYTES", 1)
    with pytest.raises(CaptureStoreError, match="bundle manifest exceeds"):
        capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": prepared_receipt})


class _MissingDatasetSession:
    async def execute(self, _statement):
        return SimpleNamespace(scalar_one_or_none=lambda: None)


@pytest.mark.asyncio
async def test_capture_store_rejects_missing_dataset_before_registration():
    with pytest.raises(CaptureStoreError, match="dataset does not exist"):
        await capture_store._lock_dataset(
            _MissingDatasetSession(),
            capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3),
        )


class _MissingDefinitionSession:
    async def get(self, *_args):
        return None


class _NoIdentifierInsertSession:
    def add(self, _model):
        return None

    def add_all(self, _models):
        return None

    async def flush(self):
        return None


class _ReplayableInsertSession:
    def __init__(self) -> None:
        self.flush_count = 0
        self.batches: list[tuple[object, ...]] = []

    def add(self, model) -> None:
        model.capture_bundle_id = 1

    def add_all(self, models) -> None:
        self.batches.append(tuple(models))

    async def flush(self) -> None:
        self.flush_count += 1


@pytest.mark.asyncio
async def test_capture_store_rejects_missing_definition_and_insert_identity():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)

    with pytest.raises(CaptureStoreError, match="identity does not match"):
        await capture_store._validated_streams(_MissingDefinitionSession(), identity)

    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": _build_receipt()})
    with pytest.raises(CaptureStoreError, match="did not return an identifier"):
        await capture_store._insert_capture_bundle(_NoIdentifierInsertSession(), prepared)


@pytest.mark.asyncio
async def test_replayable_parquet_insert_batches_all_part_rows_in_one_flush():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    capture = _build_replayable_capture()
    prepared = capture_store._prepare_capture_bundle(
        identity,
        (("records", 1),),
        {"records": capture.receipt},
    )
    session = _ReplayableInsertSession()

    registered = await capture_store._insert_replayable_parquet_bundle(
        session,
        prepared,
        {"records": capture},
    )

    assert registered.capture_bundle_id == 1
    assert session.flush_count == 3
    assert tuple(len(batch) for batch in session.batches) == (len(capture.parts),)


class _SnapshotBundleLookupSession:
    statement = None

    async def execute(self, statement):
        self.statement = statement
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: []))


@pytest.mark.asyncio
async def test_snapshot_lookup_uses_digest_then_retains_exact_token_comparison():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": _build_receipt()})
    session = _SnapshotBundleLookupSession()

    assert await capture_store._snapshot_bundles(session, prepared) == ()

    assert session.statement is not None
    where_clause = str(session.statement.whereclause)
    assert "snapshot_token_sha256" in where_clause
    assert "snapshot_token =" in where_clause
