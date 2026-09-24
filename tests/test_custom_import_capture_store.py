# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contract checks for generic custom-import capture registration."""

from __future__ import annotations

import hashlib
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

    bundle = SimpleNamespace(snapshot_token=receipt.source_snapshot_token)
    stored_capture = SimpleNamespace(
        byte_count=1,
        content_sha256=bytes.fromhex(receipt.content_sha256),
        canonical_manifest=receipt.canonical_manifest,
        manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
    )
    valid_part = SimpleNamespace(
        part_ordinal=1,
        payload=b"x",
        byte_count=1,
        payload_sha256=hashlib.sha256(b"x").digest(),
    )
    arguments = (bundle, (("records", 1),), {1: stored_capture})
    with pytest.raises(CaptureBundleConflict, match="incomplete part count"):
        capture_store._replayable_captures_from_rows(
            *arguments,
            {1: (2, capture_store._payload_set_sha256((b"x", b"y")))},
            {1: [valid_part]},
        )
    with pytest.raises(CaptureBundleConflict, match="payload part has drifted"):
        capture_store._replayable_captures_from_rows(
            *arguments,
            {1: (1, capture_store._payload_set_sha256((b"x",)))},
            {1: [SimpleNamespace(**(vars(valid_part) | {"part_ordinal": 2}))]},
        )
    with pytest.raises(CaptureBundleConflict, match="set digest has drifted"):
        capture_store._replayable_captures_from_rows(
            *arguments,
            {1: (1, b"z" * 32)},
            {1: [valid_part]},
        )


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
    with pytest.raises(CaptureBundleConflict, match="missing or extra payload parts"):
        await capture_store._load_replayable_parts_by_slot(_RowsSession(), 9, streams)

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

    async def loaded_streams(_session, _identity):
        return streams

    async def validated_streams(_session, _identity, _streams):
        return None

    async def loaded_bundle(_session, _capture_bundle_id, _identity):
        bundle = capture_store._new_capture_bundle(prepared)
        bundle.capture_bundle_id = 9
        bundle.manifest_sha256 = b"x" * 32
        return bundle

    async def loaded_captures(_session, _capture_bundle_id, _identity, _streams):
        return {1: object()}

    async def loaded_parts(_session, _capture_bundle_id, _streams):
        return {1: [object()]}

    monkeypatch.setattr(capture_store, "_validated_streams", loaded_streams)
    monkeypatch.setattr(capture_store, "_validated_replayable_parquet_streams", validated_streams)
    monkeypatch.setattr(capture_store, "_load_replayable_bundle_model", loaded_bundle)
    monkeypatch.setattr(capture_store, "_load_replayable_captures_by_slot", loaded_captures)
    monkeypatch.setattr(capture_store, "_complete_payload_metadata_by_slot", lambda _captures: {1: (1, b"x" * 32)})
    monkeypatch.setattr(capture_store, "_load_replayable_parts_by_slot", loaded_parts)
    monkeypatch.setattr(
        capture_store,
        "_replayable_captures_from_rows",
        lambda _bundle, _streams, _captures, _metadata, _parts: ((replayable,), {"records": replayable.receipt}),
    )
    with pytest.raises(CaptureBundleConflict, match="identity has drifted"):
        await capture_store.load_replayable_parquet_bundle(
            _RowsSession(),
            capture_bundle_id=9,
            dataset_id=1,
            definition_revision_id=2,
            schema_revision_id=3,
        )


def test_durable_store_wraps_nonconflict_payload_decoding_errors():
    receipt = _build_receipt(byte_count=1, content_sha256=_digest("payload"))
    capture = SimpleNamespace(
        byte_count=1,
        content_sha256=object(),
        canonical_manifest=receipt.canonical_manifest,
        manifest_sha256=bytes.fromhex(receipt.manifest_sha256),
    )
    with pytest.raises(CaptureBundleConflict, match="payload is invalid"):
        capture_store._replayable_captures_from_rows(
            SimpleNamespace(snapshot_token=receipt.source_snapshot_token),
            (("records", 1),),
            {1: capture},
            {1: (1, capture_store._payload_set_sha256((b"x",)))},
            {
                1: [
                    SimpleNamespace(
                        part_ordinal=1,
                        payload=b"x",
                        byte_count=1,
                        payload_sha256=hashlib.sha256(b"x").digest(),
                    )
                ]
            },
        )


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
