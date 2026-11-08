# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contract checks for generic custom-import capture registration."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

import process.custom_import.capture_store as capture_store
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CapturePayloadUnavailable,
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
    invalid_receipt = _build_receipt()
    object.__setattr__(invalid_receipt, "stream_id", "UPPER")
    with pytest.raises(CaptureStoreError, match="stream_id"):
        capture_store._validate_receipts((invalid_receipt,))
    incomplete_receipt = object.__new__(CaptureReceipt)
    object.__setattr__(incomplete_receipt, "canonical_manifest", receipt.canonical_manifest)
    with pytest.raises(CaptureStoreError, match="declared receipt type"):
        capture_store._validate_receipts((incomplete_receipt,))
    with pytest.raises(CaptureStoreError, match="exactly cover"):
        capture_store._index_receipts_by_stream((("records", 1),), (_build_receipt(stream_id="details"),))

    prepared_receipt = _build_receipt()
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    monkeypatch.setattr(capture_store, "_MAX_MANIFEST_BYTES", 1)
    with pytest.raises(CaptureStoreError, match="bundle manifest exceeds"):
        capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": prepared_receipt})


def test_durable_replay_helpers_reject_corrupt_payload_shapes(monkeypatch):
    receipt = _build_receipt(byte_count=1, content_sha256=_digest("payload"))
    capture = ReplayableParquetCapture(receipt=receipt, parts=(b"x",))
    with pytest.raises(CaptureStoreError, match="requires a capture receipt"):
        ReplayableParquetCapture(receipt=object(), parts=(b"x",))
    with pytest.raises(CaptureStoreError, match="non-empty tuple"):
        capture_store._validate_replayable_parquet_captures(())
    with pytest.raises(CaptureStoreError, match="declared capture type"):
        capture_store._validate_replayable_parquet_captures((object(),))
    monkeypatch.setattr(capture_store, "_MAX_PARQUET_PARTS_PER_BUNDLE", 0)
    with pytest.raises(CaptureStoreError, match="aggregate durable payload"):
        capture_store._validate_replayable_parquet_captures((capture,))
    with pytest.raises(CaptureStoreError, match="exactly cover"):
        capture_store._index_replayable_captures_by_stream(
            (("records", 1),),
            (
                replace(
                    capture, receipt=_build_receipt(stream_id="other", byte_count=1, content_sha256=_digest("payload"))
                ),
            ),
        )
    assert capture_store._stored_bytes(object()) is None


def test_durable_replay_rejects_incomplete_or_drifted_payload_rows():
    receipt = _build_receipt(byte_count=1, content_sha256=_digest("payload"))
    invalid_metadata = SimpleNamespace(
        payload_contract=capture_store._PARQUET_PART_PAYLOAD_CONTRACT,
        payload_part_count=1,
        payload_set_sha256=b"short",
        byte_count=1,
    )
    with pytest.raises(CaptureBundleConflict, match="payload metadata"):
        capture_store._durable_payload_metadata(invalid_metadata)

    unavailable = SimpleNamespace(payload_contract=None, payload_part_count=None, payload_set_sha256=None, byte_count=1)
    valid_metadata = SimpleNamespace(
        payload_contract=capture_store._PARQUET_PART_PAYLOAD_CONTRACT,
        payload_part_count=1,
        payload_set_sha256=b"x" * 32,
        byte_count=1,
    )
    with pytest.raises(CapturePayloadUnavailable, match="no durable payload"):
        capture_store._complete_payload_metadata_by_slot({1: unavailable})
    with pytest.raises(CaptureBundleConflict, match="mixed payload"):
        capture_store._complete_payload_metadata_by_slot({1: unavailable, 2: valid_metadata})


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


class _RowsSession:
    def __init__(self, *, rows: tuple[object, ...] = (), row: object | None = None) -> None:
        self.rows = rows
        self.row = row

    async def execute(self, _statement):
        return SimpleNamespace(
            scalar_one_or_none=lambda: self.row,
            scalars=lambda: SimpleNamespace(all=lambda: self.rows),
        )


def _stored_replayable_capture(
    capture: ReplayableParquetCapture,
    *,
    stream_slot: int = 1,
    payload_metadata: bool = True,
) -> SimpleNamespace:
    receipt = capture.receipt
    return SimpleNamespace(
        stream_slot=stream_slot,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        byte_count=receipt.byte_count,
        content_sha256=bytes.fromhex(receipt.content_sha256),
        canonical_manifest=receipt.canonical_manifest,
        manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
        payload_contract=capture_store._PARQUET_PART_PAYLOAD_CONTRACT if payload_metadata else None,
        payload_part_count=len(capture.parts) if payload_metadata else None,
        payload_set_sha256=capture_store._payload_set_sha256(capture.parts) if payload_metadata else None,
    )


@pytest.mark.asyncio
async def test_capture_store_rejects_missing_definition_and_insert_identity():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)

    with pytest.raises(CaptureStoreError, match="identity does not match"):
        await capture_store._validated_streams(_MissingDefinitionSession(), identity)

    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": _build_receipt()})
    with pytest.raises(CaptureStoreError, match="did not return an identifier"):
        await capture_store._insert_capture_bundle(_NoIdentifierInsertSession(), prepared)


@pytest.mark.asyncio
async def test_durable_store_fails_closed_on_persisted_stream_and_capture_drift():
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    replayable = _build_replayable_capture()
    streams = (("records", 1),)
    prepared = capture_store._prepare_capture_bundle(identity, streams, {"records": replayable.receipt})

    with pytest.raises(CaptureStoreError, match="source streams do not exactly match"):
        await capture_store._validated_replayable_parquet_streams(_RowsSession(), identity, streams)
    with pytest.raises(CaptureStoreError, match="requires persisted Parquet streams"):
        await capture_store._validated_replayable_parquet_streams(
            _RowsSession(
                rows=(SimpleNamespace(stream_slot=1, stream_id="different", decoder="parquet", compression="none"),)
            ),
            identity,
            streams,
        )

    bundle = SimpleNamespace(capture_bundle_id=7)
    assert await capture_store._is_matching_capture_rows(_RowsSession(), bundle, prepared) is False
    drifted_capture = _stored_replayable_capture(replayable)
    drifted_capture.dataset_id = 99
    assert (
        await capture_store._is_matching_capture_rows(_RowsSession(rows=(drifted_capture,)), bundle, prepared) is False
    )
    assert (
        await capture_store._is_matching_replayable_parquet_rows(
            _RowsSession(), bundle, prepared, {"records": replayable}
        )
        is False
    )


@pytest.mark.asyncio
async def test_durable_store_requires_complete_exact_payload_parts(monkeypatch):
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    replayable = _build_replayable_capture(parts=(b"payload",))
    prepared = capture_store._prepare_capture_bundle(
        identity,
        (("records", 1),),
        {"records": replayable.receipt},
    )
    bundle = SimpleNamespace(capture_bundle_id=7)
    captures_by_stream = {"records": replayable}

    unavailable = _stored_replayable_capture(replayable, payload_metadata=False)
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: unavailable}
        )
        is None
    )

    stored = _stored_replayable_capture(replayable)
    part_rows_by_slot: dict[int, list[object]] = {}

    async def locked_parts(_session, _bundle):
        return part_rows_by_slot

    monkeypatch.setattr(capture_store, "_locked_parquet_parts_by_slot", locked_parts)
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {}
        )
        is False
    )
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: stored}
        )
        is False
    )

    valid_part = SimpleNamespace(
        part_ordinal=1,
        byte_count=len(replayable.parts[0]),
        payload=replayable.parts[0],
        payload_sha256=hashlib.sha256(replayable.parts[0]).digest(),
    )
    part_rows_by_slot[1] = [valid_part]
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: stored}
        )
        is True
    )


@pytest.mark.asyncio
async def test_durable_store_rejects_drifted_payload_parts(monkeypatch):
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    replayable = _build_replayable_capture(parts=(b"payload",))
    prepared = capture_store._prepare_capture_bundle(identity, (("records", 1),), {"records": replayable.receipt})
    bundle = SimpleNamespace(capture_bundle_id=7)
    captures_by_stream = {"records": replayable}
    stored = _stored_replayable_capture(replayable)
    valid_part = SimpleNamespace(
        part_ordinal=1,
        byte_count=7,
        payload=b"payload",
        payload_sha256=hashlib.sha256(b"payload").digest(),
    )
    part_rows_by_slot = {1: [valid_part]}

    async def locked_parts(_session, _bundle):
        return part_rows_by_slot

    monkeypatch.setattr(capture_store, "_locked_parquet_parts_by_slot", locked_parts)
    stored.payload_part_count = 2
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: stored}
        )
        is False
    )

    stored.payload_part_count = 1
    part_rows_by_slot[1] = []
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: stored}
        )
        is False
    )
    part_rows_by_slot[1] = [SimpleNamespace(**(vars(valid_part) | {"part_ordinal": 2}))]
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(), bundle, prepared, captures_by_stream, {1: stored}
        )
        is False
    )


@pytest.mark.asyncio
async def test_durable_store_rejects_mixed_payload_metadata_before_loading_parts(monkeypatch):
    replayable = _build_replayable_capture(parts=(b"payload",))
    details = ReplayableParquetCapture(
        receipt=_build_receipt(stream_id="details", byte_count=1, content_sha256=_digest("details")),
        parts=(b"y",),
    )
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    prepared = capture_store._prepare_capture_bundle(
        identity, (("records", 1), ("details", 2)), {"records": replayable.receipt, "details": details.receipt}
    )

    async def unexpected_part_load(_session, _bundle):
        raise AssertionError("mixed metadata must be rejected before part loading")

    monkeypatch.setattr(capture_store, "_locked_parquet_parts_by_slot", unexpected_part_load)
    assert (
        await capture_store._is_matching_stored_replayable_payload(
            _RowsSession(),
            SimpleNamespace(capture_bundle_id=7),
            prepared,
            {"records": replayable, "details": details},
            {
                1: _stored_replayable_capture(replayable),
                2: _stored_replayable_capture(details, stream_slot=2, payload_metadata=False),
            },
        )
        is False
    )


@pytest.mark.asyncio
async def test_durable_loader_and_idempotency_reject_partial_persisted_state(monkeypatch):
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    replayable = _build_replayable_capture()
    streams = (("records", 1),)
    prepared = capture_store._prepare_capture_bundle(identity, streams, {"records": replayable.receipt})
    bundle = capture_store._new_capture_bundle(prepared)
    bundle.capture_bundle_id = 9

    with pytest.raises(CaptureBundleConflict, match="does not match"):
        await capture_store._load_replayable_bundle_model(_RowsSession(), 9, identity)
    with pytest.raises(CaptureBundleConflict, match="missing or extra stream captures"):
        await capture_store._load_replayable_captures_by_slot(_RowsSession(), 9, identity, streams)
    drifted_capture = _stored_replayable_capture(replayable)
    drifted_capture.schema_revision_id = 99
    with pytest.raises(CaptureBundleConflict, match="stream identity has drifted"):
        await capture_store._load_replayable_captures_by_slot(
            _RowsSession(rows=(drifted_capture,)), 9, identity, streams
        )

    snapshots: tuple[object, ...] = ()
    is_matching_payload: bool | None = True

    async def snapshot_bundles(_session, _prepared):
        return snapshots

    async def matching_rows(_session, _bundle, _prepared, _captures_by_stream):
        return is_matching_payload

    monkeypatch.setattr(capture_store, "_snapshot_bundles", snapshot_bundles)
    monkeypatch.setattr(capture_store, "_is_matching_replayable_parquet_rows", matching_rows)
    assert (
        await capture_store._find_existing_replayable_parquet_registration(
            _RowsSession(), prepared, {"records": replayable}
        )
        is None
    )
    snapshots = (bundle, bundle)
    with pytest.raises(CaptureBundleConflict, match="different or partial capture bundle"):
        await capture_store._find_existing_replayable_parquet_registration(
            _RowsSession(), prepared, {"records": replayable}
        )
    snapshots = (bundle,)
    is_matching_payload = None
    with pytest.raises(CapturePayloadUnavailable, match="no durable payload"):
        await capture_store._find_existing_replayable_parquet_registration(
            _RowsSession(), prepared, {"records": replayable}
        )
    is_matching_payload = False
    with pytest.raises(CaptureBundleConflict, match="partial durable capture bundle"):
        await capture_store._find_existing_replayable_parquet_registration(
            _RowsSession(), prepared, {"records": replayable}
        )
    is_matching_payload = True
    assert await capture_store._find_existing_replayable_parquet_registration(
        _RowsSession(), prepared, {"records": replayable}
    ) == capture_store.CaptureBundleRegistration(capture_bundle_id=9, stream_slots=(1,), created=False)


@pytest.mark.asyncio
async def test_durable_store_rejects_missing_insert_identity_and_final_bundle_drift(monkeypatch):
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    replayable = _build_replayable_capture(parts=(b"payload",))
    streams = (("records", 1),)
    prepared = capture_store._prepare_capture_bundle(identity, streams, {"records": replayable.receipt})
    with pytest.raises(CaptureStoreError, match="did not return an identifier"):
        await capture_store._insert_replayable_parquet_bundle(
            _NoIdentifierInsertSession(), prepared, {"records": replayable}
        )

    case = _part_read_case(monkeypatch)
    case.bundle.manifest_sha256 = b"x" * 32
    with pytest.raises(CaptureBundleConflict, match="identity has drifted"):
        await capture_store.load_replayable_parquet_bundle(case.session, **_PART_IDS)
    assert case.session.rows is None


@pytest.mark.asyncio
async def test_durable_store_wraps_nonconflict_payload_decoding_errors(monkeypatch):
    case = _part_read_case(monkeypatch)
    case.captures_by_slot[1].content_sha256 = object()
    with pytest.raises(CaptureBundleConflict, match="payload is invalid"):
        await capture_store.load_replayable_parquet_bundle(case.session, **_PART_IDS)
    assert case.session.rows is None


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


_PART_IDS = {"capture_bundle_id": 9, "dataset_id": 1, "definition_revision_id": 2, "schema_revision_id": 3}


class _PartRows:
    def __init__(self, source):
        self.source = iter(source)
        self.read_count = self.close_count = 0
        self.pause = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.read_count += 1
        if self.pause is not None:
            self.pause.set()
            await asyncio.Event().wait()
        row = next(self.source, None)
        if row is None:
            raise StopAsyncIteration
        return row

    async def close(self):
        self.close_count += 1


class _PartReadSession:
    def __init__(self, source):
        self.source = source
        self.rows = self.statement = None
        self.autoflush_suppressed = False
        self.new = [object()]

    @property
    @contextmanager
    def no_autoflush(self):
        self.autoflush_suppressed = True
        try:
            yield
        finally:
            self.autoflush_suppressed = False

    async def stream(self, statement):
        assert self.autoflush_suppressed
        self.statement = statement
        self.rows = _PartRows(self.source)
        return self.rows

    async def close(self):
        raise AssertionError("borrowed session must stay open")

    async def commit(self):
        raise AssertionError("borrowed transaction must not be committed")

    async def rollback(self):
        raise AssertionError("borrowed transaction recovery belongs to the caller")


def _part_read_case(monkeypatch):
    captures = (
        _build_replayable_capture(parts=(b"first", b"second")),
        ReplayableParquetCapture(
            receipt=_build_receipt(stream_id="details", byte_count=5),
            parts=(b"third",),
        ),
    )
    streams = (("records", 1), ("details", 2))
    identity = capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3)
    prepared = capture_store._prepare_capture_bundle(
        identity, streams, {capture.receipt.stream_id: capture.receipt for capture in captures}
    )
    bundle = capture_store._new_capture_bundle(prepared)
    bundle.capture_bundle_id = 9
    captures_by_slot = {
        slot: _stored_replayable_capture(capture, stream_slot=slot) for slot, capture in enumerate(captures, start=1)
    }
    source_parts = [
        (9, slot, ordinal, len(part_payload), part_payload, hashlib.sha256(part_payload).digest())
        for slot, capture in enumerate(captures, start=1)
        for ordinal, part_payload in enumerate(capture.parts, start=1)
    ]
    session = _PartReadSession(source_parts)

    async def loaded_streams(borrowed, _identity):
        assert borrowed.autoflush_suppressed
        return streams

    async def validated_streams(borrowed, _identity, _streams):
        assert borrowed.autoflush_suppressed

    async def loaded_bundle(borrowed, _bundle_id, _identity):
        assert borrowed.autoflush_suppressed
        return bundle

    async def loaded_captures(borrowed, _bundle_id, _identity, _streams):
        assert borrowed.autoflush_suppressed
        return captures_by_slot

    monkeypatch.setattr(capture_store, "_validated_streams", loaded_streams)
    monkeypatch.setattr(capture_store, "_validated_replayable_parquet_streams", validated_streams)
    monkeypatch.setattr(capture_store, "_load_replayable_bundle_model", loaded_bundle)
    monkeypatch.setattr(capture_store, "_load_replayable_captures_by_slot", loaded_captures)
    return SimpleNamespace(
        session=session, source=source_parts, captures=captures, captures_by_slot=captures_by_slot, bundle=bundle
    )


@pytest.mark.asyncio
async def test_incremental_pull_preserves_eager_round_trip(monkeypatch):
    case = _part_read_case(monkeypatch)
    async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS) as parts:
        assert case.session.rows.read_count == 0
        first = await anext(parts)
        assert first == (case.captures[0].receipt, 1, b"first")
        assert case.session.rows.read_count == 1
        remaining_parts = [part async for part in parts]
        assert tuple(part[2] for part in remaining_parts) == (b"second", b"third")
        assert case.session.rows.read_count == 4
    assert case.session.rows.close_count == 1
    assert not case.session.autoflush_suppressed
    assert len(case.session.new) == 1
    query = case.session.statement
    assert len(query.selected_columns) == 6
    assert query.get_execution_options()["yield_per"] == 1
    assert str(query.whereclause).count("=") == 1
    assert "capture_bundle_id" in str(query.whereclause)
    assert tuple(str(column) for column in query._order_by_clauses) == (
        "custom_import_capture_parquet_part.stream_slot",
        "custom_import_capture_parquet_part.part_ordinal",
    )
    loaded = await capture_store.load_replayable_parquet_bundle(case.session, **_PART_IDS)
    assert loaded == case.captures
    assert case.session.rows.read_count == 4
    assert case.session.rows.close_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    (
        "missing",
        "extra",
        "reordered",
        "foreign",
        "unknown_stream",
        "empty",
        "length",
        "sha",
        "set_digest",
        "byte_total",
        "bool_ordinal",
        "bool_slot",
        "bool_length",
    ),
)
async def test_incremental_and_eager_parts_reject_complete_payload_drift(monkeypatch, damage):
    case = _part_read_case(monkeypatch)
    match damage:
        case "missing":
            case.source.pop()
        case "extra":
            case.source.insert(2, (9, 1, 3, 5, b"extra", hashlib.sha256(b"extra").digest()))
        case "reordered":
            case.source[:2] = case.source[1::-1]
        case "set_digest":
            case.captures_by_slot[2].payload_set_sha256 = b"x" * 32
        case "byte_total":
            case.captures_by_slot[2].byte_count += 1
            receipt = replace(case.captures[1].receipt, byte_count=6)
            prepared = capture_store._prepare_capture_bundle(
                capture_store._identity(dataset_id=1, definition_revision_id=2, schema_revision_id=3),
                (("records", 1), ("details", 2)),
                {"records": case.captures[0].receipt, "details": receipt},
            )
            rebuilt = capture_store._new_capture_bundle(prepared)
            case.bundle.canonical_manifest = rebuilt.canonical_manifest
            case.bundle.manifest_sha256 = rebuilt.manifest_sha256
        case _:
            row_fields = list(case.source[1])
            changes_by_damage = {
                "foreign": (0, 99),
                "unknown_stream": (1, 99),
                "empty": (4, b""),
                "length": (3, 1),
                "sha": (5, b"x" * 32),
                "bool_ordinal": (2, True),
                "bool_slot": (1, True),
                "bool_length": (3, True),
            }
            index, corrupted_value = changes_by_damage[damage]
            row_fields[index] = corrupted_value
            case.source[1] = tuple(row_fields)
    with pytest.raises(CaptureBundleConflict):
        async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS) as parts:
            received_parts = [part async for part in parts]
            pytest.fail(f"corrupt capture returned {len(received_parts)} parts")
    assert case.session.rows.close_count == 1
    with pytest.raises(CaptureBundleConflict):
        await capture_store.load_replayable_parquet_bundle(case.session, **_PART_IDS)
    assert case.session.rows.close_count == 1


@pytest.mark.asyncio
async def test_declared_last_part_does_not_establish_integrity_without_actual_eof(monkeypatch):
    case = _part_read_case(monkeypatch)
    case.source.append((9, 99, 1, 5, b"extra", hashlib.sha256(b"extra").digest()))
    async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS) as parts:
        for _ in range(3):
            await anext(parts)
        with pytest.raises(CaptureBundleConflict, match="missing or extra"):
            await anext(parts)
    assert case.session.rows.close_count == 1


@pytest.mark.asyncio
async def test_receipts_and_payload_metadata_are_frozen_before_first_yield(monkeypatch):
    case = _part_read_case(monkeypatch)
    async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS) as parts:
        await anext(parts)
        case.captures_by_slot[2].byte_count = 0
        case.captures_by_slot[2].payload_part_count = 99
        case.captures_by_slot[2].payload_set_sha256 = b"x" * 32
        remaining_parts = [part async for part in parts]
    assert remaining_parts[-1] == (case.captures[1].receipt, 1, b"third")
    assert case.session.rows.close_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_kind", ("break", "error", "cancel"))
async def test_incremental_context_closes_owned_resources_and_preserves_borrowed_session(monkeypatch, exit_kind):
    case = _part_read_case(monkeypatch)
    observed_iterators = []

    async def consume():
        async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS) as parts:
            observed_iterators.append(parts)
            if exit_kind == "cancel":
                case.session.rows.pause = asyncio.Event()
            await anext(parts)
            if exit_kind == "error":
                raise RuntimeError("synthetic consumer failure")

    if exit_kind == "cancel":
        task = asyncio.create_task(consume())
        while case.session.rows is None or case.session.rows.pause is None:
            await asyncio.sleep(0)
        await case.session.rows.pause.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif exit_kind == "error":
        with pytest.raises(RuntimeError, match="consumer failure"):
            await consume()
    else:
        await consume()
    assert case.session.rows.close_count == 1
    with pytest.raises(StopAsyncIteration):
        await anext(observed_iterators[0])
    assert len(case.session.new) == 1
    assert not case.session.autoflush_suppressed


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("unavailable", "mixed", "parts_cap", "bytes_cap"))
async def test_incremental_metadata_rejects_incomplete_or_over_budget_bundles_before_opening_result(
    monkeypatch, damage
):
    case = _part_read_case(monkeypatch)
    if damage in {"unavailable", "mixed"}:
        for slot in (1, 2) if damage == "unavailable" else (2,):
            capture = case.captures_by_slot[slot]
            capture.payload_contract = capture.payload_part_count = capture.payload_set_sha256 = None
    elif damage == "parts_cap":
        monkeypatch.setattr(capture_store, "_MAX_PARQUET_PARTS_PER_BUNDLE", 2)
    else:
        monkeypatch.setattr(capture_store, "_MAX_PARQUET_BYTES_PER_BUNDLE", 1)
    error_type = CapturePayloadUnavailable if damage == "unavailable" else CaptureStoreError
    with pytest.raises(error_type):
        async with capture_store.open_replayable_parquet_parts(case.session, **_PART_IDS):
            raise AssertionError("invalid metadata must not yield an iterator")
    assert case.session.rows is None
