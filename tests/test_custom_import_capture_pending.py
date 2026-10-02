# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact retry, receipt and authority checks for incremental capture retention."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.custom_import.capture_pending as pending
import process.custom_import.capture_store as capture_store
from db.models.custom_import import CustomImportCaptureParquetPart
from process.custom_import.capture import capture_stream
from process.custom_import.capture_store import CaptureBundleConflict, CaptureReceipt, CaptureStoreError
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests.test_custom_import_capture import _stream
from tests.test_custom_import_capture_store import _PART_IDS, _part_read_case
from tests.test_custom_import_segmented_capture_policy import _document


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode()).digest()


def _request(**changes):
    values_by_field = dict(
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        fence=1,
        token=b"synthetic-owner",
        request_identity_sha256=_digest("request"),
        source_request_sha256=_digest("source"),
        statement_sha256=_digest("statement"),
        source_snapshot_token="synthetic-snapshot",
        policy=SegmentedCapturePolicy.from_mapping(_document()),
    )
    return pending.PendingCaptureRequest(**(values_by_field | changes))


def _bundle(request):
    return SimpleNamespace(
        capture_bundle_id=5,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        payload_contract=pending.SEGMENTED_PAYLOAD_CONTRACT,
        capture_state="pending",
        producing_execution_id=4,
        producing_fence=1,
        producing_token_sha256=pending.lifecycle.lease_token_sha256(request.token),
        request_identity_sha256=request.request_identity_sha256,
        source_binding_revision_id=None,
        source_binding_sha256=None,
        source_request_sha256=request.source_request_sha256,
        statement_sha256=request.statement_sha256,
        snapshot_token=request.source_snapshot_token,
        snapshot_token_sha256=_digest(request.source_snapshot_token),
        canonical_policy=request.policy.canonical,
        policy_sha256=bytes.fromhex(request.policy.digest),
        acquisition_deadline_at=dt.datetime(2026, 10, 2, 12, 1, tzinfo=dt.UTC),
        **dict.fromkeys(("committed_" + name for name in pending._ACCOUNTING_FIELDS), 0),
    )


def _stream_header(**changes):
    return SimpleNamespace(
        **(
            dict(capture_bundle_id=5, stream_slot=1, capture_state="pending", eof_at=None)
            | dict.fromkeys(("committed_" + name for name in pending._ACCOUNTING_FIELDS), 0)
            | changes
        )
    )


class _Session:
    new = dirty = deleted = ()

    def __init__(self, existing=None):
        self.existing = existing
        self.statements = []

    def in_transaction(self):
        return True

    async def get(self, model, _key, **_kwargs):
        assert model is CustomImportCaptureParquetPart
        return self.existing

    async def execute(self, statement):
        self.statements.append(statement)
        return SimpleNamespace(scalar_one=lambda: 1)


def _capture(request):
    stream = _stream(format_name="parquet")
    return capture_stream(
        BytesIO(b"synthetic-retained-bytes"),
        stream,
        source_snapshot_token=request.source_snapshot_token,
        limits=request.policy.part_limits,
    )


def test_request_copies_mutable_identity_and_hides_lease_token():
    raw = bytearray(b"synthetic-owner")
    digest = bytearray(_digest("request"))
    request = _request(token=raw, request_identity_sha256=digest)
    raw[0] = 0
    digest[0] ^= 1
    assert request.token == b"synthetic-owner" and request.request_identity_sha256 == _digest("request")
    assert "synthetic-owner" not in repr(request)
    with pytest.raises(CaptureStoreError, match="paired"):
        replace(request, source_binding_revision_id=7)
    with pytest.raises(CaptureStoreError, match="positive"):
        replace(request, fence=True)


@pytest.mark.parametrize(
    "field", ["policy_sha256", "statement_sha256", "producing_token_sha256", "source_request_sha256"]
)
def test_attempt_identity_never_accepts_a_different_digest(field):
    request = _request()
    bundle = _bundle(request)
    setattr(bundle, field, _digest("different"))
    with pytest.raises(CaptureBundleConflict, match="identity"):
        pending._validate_bundle(bundle, request)


async def _append_setup(monkeypatch, request, header, bundle):
    monkeypatch.setattr(pending, "_source_stream", AsyncMock(return_value=_stream(format_name="parquet")))
    monkeypatch.setattr(pending, "_lock_bundle", AsyncMock(return_value=(bundle, SimpleNamespace())))
    monkeypatch.setattr(pending, "_pending_stream", AsyncMock(return_value=header))
    monkeypatch.setattr(pending, "_fresh_remaining_seconds", AsyncMock(return_value=30))


async def test_exact_part_retry_has_no_insert_or_counter_charge(monkeypatch):
    request = _request()
    capture = _capture(request)
    values_by_field = pending._part_values(capture, 1, 0, 0, request.policy)
    session = _Session(SimpleNamespace(**values_by_field))
    await _append_setup(monkeypatch, request, _stream_header(committed_part_count=1), _bundle(request))
    assert (
        await pending.append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=5,
            capture=capture,
            ordinal=1,
            record_count=0,
            arrow_byte_count=0,
        )
        == 0
    )
    assert session.statements == []
    session.existing.record_count = 1
    with pytest.raises(CaptureBundleConflict, match="retry"):
        await pending.append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=5,
            capture=capture,
            ordinal=1,
            record_count=0,
            arrow_byte_count=0,
        )


@pytest.mark.parametrize("ordinal,eof,expected", [(2, None, "contiguous"), (1, dt.datetime.now(dt.UTC), "EOF")])
async def test_append_rejects_gaps_and_closed_streams(monkeypatch, ordinal, eof, expected):
    request = _request()
    session = _Session()
    await _append_setup(monkeypatch, request, _stream_header(eof_at=eof), _bundle(request))
    with pytest.raises(CaptureBundleConflict, match=expected):
        await pending.append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=5,
            capture=_capture(request),
            ordinal=ordinal,
            record_count=0,
            arrow_byte_count=0,
        )
    assert session.statements == []


async def test_append_rejects_payload_corruption_before_any_lock(monkeypatch):
    request = _request()
    capture = replace(_capture(request), payload=b"corrupted")
    lock = AsyncMock()
    monkeypatch.setattr(pending, "_source_stream", AsyncMock(return_value=_stream(format_name="parquet")))
    monkeypatch.setattr(pending, "_lock_bundle", lock)
    with pytest.raises(CaptureStoreError, match="invalid"):
        await pending.append_pending_parquet_part(
            _Session(),
            request=request,
            capture_bundle_id=5,
            capture=capture,
            ordinal=1,
            record_count=0,
            arrow_byte_count=0,
        )
    lock.assert_not_awaited()


async def test_append_rejects_aggregate_quota_without_insertion(monkeypatch):
    request = _request()
    bundle = _bundle(request)
    bundle.committed_byte_count = request.policy.bundle_budget["maximum_compressed_bytes"]
    await _append_setup(monkeypatch, request, _stream_header(), bundle)
    session = _Session()
    with pytest.raises(CaptureStoreError, match="budget"):
        await pending.append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=5,
            capture=_capture(request),
            ordinal=1,
            record_count=0,
            arrow_byte_count=0,
        )
    assert session.statements == []


async def test_eof_is_monotone_and_exact_retries_are_harmless(monkeypatch):
    request = _request()
    header = _stream_header(committed_part_count=1, committed_record_count=0)
    await _append_setup(monkeypatch, request, header, _bundle(request))
    session = _Session()
    assert (
        await pending.mark_pending_parquet_eof(
            session,
            request=request,
            capture_bundle_id=5,
            stream_id="records",
            part_count=1,
            record_count=0,
        )
        == 1
    )
    assert len(session.statements) == 1 and session.statements[0].is_update
    header.eof_at = dt.datetime.now(dt.UTC)
    assert (
        await pending.mark_pending_parquet_eof(
            session,
            request=request,
            capture_bundle_id=5,
            stream_id="records",
            part_count=1,
            record_count=0,
        )
        == 0
    )
    with pytest.raises(CaptureBundleConflict, match="accounting"):
        await pending.mark_pending_parquet_eof(
            session,
            request=request,
            capture_bundle_id=5,
            stream_id="records",
            part_count=1,
            record_count=1,
        )
    assert len(session.statements) == 1


def _receipt(request, **changes):
    document_by_field = (
        dict(
            contract_version=pending.SEGMENTED_PAYLOAD_CONTRACT,
            policy_sha256=request.policy.digest,
            part_count=1,
            byte_count=23,
            decoded_byte_count=23,
            arrow_byte_count=0,
            record_count=0,
            manifest_byte_count=800,
            payload_set_sha256=_digest("payload-set").hex(),
            manifest_set_sha256=_digest("manifest-set").hex(),
            source_request_sha256=request.source_request_sha256.hex(),
            statement_sha256=request.statement_sha256.hex(),
        )
        | changes
    )
    canonical = pending.canonical_json(document_by_field)
    return CaptureReceipt(
        stream_id="records",
        source_snapshot_token=request.source_snapshot_token,
        byte_count=23,
        content_sha256=document_by_field["payload_set_sha256"],
        canonical_manifest=canonical,
        manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest(),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"part_count": True},
        {"arrow_byte_count": -1},
        {"policy_sha256": _digest("other").hex()},
        {"statement_sha256": _digest("other").hex()},
    ],
)
def test_receipt_rejects_accounting_types_and_identity_drift(changes):
    request = _request()
    with pytest.raises(CaptureStoreError):
        pending._receipt_document(_receipt(request, **changes), _bundle(request), request.policy)


def test_bundle_receipt_is_compact_sorted_and_uses_raw_hashes():
    request = _request()
    bundle = _bundle(request)
    receipt = _receipt(request)
    canonical = pending._bundle_manifest(bundle, (("records", 1),), {"records": receipt}, request.policy)
    document = json.loads(canonical)
    assert document["streams"] == [{"manifest_sha256": receipt.manifest_sha256, "stream_slot": 1}]
    assert document["contract_version"] == pending.SEGMENTED_PAYLOAD_CONTRACT
    assert "parts" not in document and "canonical_manifest" not in canonical
    assert bytes.fromhex(receipt.manifest_sha256) == hashlib.sha256(receipt.canonical_manifest.encode()).digest()
    assert request.policy.digest != hashlib.sha256(request.policy.canonical.encode()).hexdigest()


async def test_expiry_is_checked_with_fresh_database_time(monkeypatch):
    request = _request()
    bundle = _bundle(request)
    expires = bundle.acquisition_deadline_at + dt.timedelta(seconds=30)
    session = _Session()
    session.execute = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: expires))
    now = AsyncMock(return_value=bundle.acquisition_deadline_at)
    monkeypatch.setattr(pending.lifecycle, "_database_now", now)
    with pytest.raises(CaptureBundleConflict, match="expired"):
        await pending._fresh_remaining_seconds(session, request, bundle)
    now.assert_awaited_once()


async def test_statement_timeout_is_armed_before_the_stream_seal_statement(monkeypatch):
    request = _request()
    bundle = _bundle(request)
    receipt = _receipt(request)
    document = json.loads(receipt.canonical_manifest)
    header = _stream_header(
        eof_at=dt.datetime.now(dt.UTC), **{"committed_" + name: document[name] for name in pending._ACCOUNTING_FIELDS}
    )
    monkeypatch.setattr(pending, "_fresh_remaining_seconds", AsyncMock(return_value=4))
    session = _Session()
    await pending._seal_stream(session, request, bundle, header, receipt)
    assert len(session.statements) == 2
    assert "set_config" in str(session.statements[0])
    assert "2000ms" in session.statements[0].compile().params.values()
    assert session.statements[1].is_update


@pytest.mark.parametrize("damage", ["boolean_slot", "boolean_count", "duplicate_slot", "extra_entry_field"])
def test_bundle_receipt_entries_are_closed_typed_and_strictly_sorted(damage):
    request = _request()
    canonical = pending._bundle_manifest(
        _bundle(request), (("records", 1),), {"records": _receipt(request)}, request.policy
    )
    document_by_field = json.loads(canonical)
    if damage == "boolean_slot":
        document_by_field["streams"][0]["stream_slot"] = True
    elif damage == "boolean_count":
        document_by_field["stream_count"] = True
    elif damage == "duplicate_slot":
        document_by_field["streams"] *= 2
        document_by_field["stream_count"] = 2
    else:
        document_by_field["streams"][0]["extra"] = "invalid"
    with pytest.raises(CaptureStoreError):
        pending._validate_bundle_receipt_shape(document_by_field)


def test_stream_receipt_requires_pinned_source_identity():
    request = _request()
    receipt = _receipt(request)
    document_by_field = json.loads(receipt.canonical_manifest)
    document_by_field.pop("source_request_sha256")
    canonical = pending.canonical_json(document_by_field)
    incomplete = replace(
        receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest()
    )
    with pytest.raises(CaptureBundleConflict, match="incomplete"):
        pending._receipt_document(incomplete, _bundle(request), request.policy)


def _segmented_read_case(monkeypatch):
    """Build sealed synthetic v2 headers over the accepted single-row cursor double."""

    case = _part_read_case(monkeypatch)
    request = _request(source_snapshot_token=case.bundle.snapshot_token)
    receipts_by_stream = {}
    for slot, legacy in enumerate(case.captures, 1):
        stream_rows = [part_row for part_row in case.source if part_row[1] == slot]
        manifest_digest = hashlib.sha256(pending._MANIFEST_SET_DOMAIN)
        manifest_bytes = 0
        for part_row in stream_rows:
            stream = replace(_stream(format_name="parquet"), stream_id=legacy.receipt.stream_id)
            capture = capture_stream(BytesIO(part_row[4]), stream, source_snapshot_token=request.source_snapshot_token)
            values_by_field = pending._part_values(capture, part_row[2], 0, 0, request.policy)
            canonical = values_by_field["canonical_capture_manifest"]
            manifest_bytes += len(canonical.encode())
            pending._add_payload_part_digest(
                manifest_digest, part_row[2], len(canonical.encode()), values_by_field["capture_manifest_sha256"]
            )
            for metric in (len(part_row[4]), 0, 0):
                manifest_digest.update(metric.to_bytes(8, "big"))
            index = case.source.index(part_row)
            case.source[index] = (
                *part_row,
                canonical,
                values_by_field["capture_manifest_sha256"],
                len(part_row[4]),
                0,
                0,
            )
        header = case.captures_by_slot[slot]
        document_by_field = dict(
            contract_version=pending.SEGMENTED_PAYLOAD_CONTRACT,
            policy_sha256=request.policy.digest,
            source_request_sha256=request.source_request_sha256.hex(),
            statement_sha256=request.statement_sha256.hex(),
            part_count=len(stream_rows),
            byte_count=legacy.receipt.byte_count,
            decoded_byte_count=legacy.receipt.byte_count,
            arrow_byte_count=0,
            record_count=0,
            manifest_byte_count=manifest_bytes,
            payload_set_sha256=bytes(header.payload_set_sha256).hex(),
            manifest_set_sha256=manifest_digest.hexdigest(),
        )
        canonical = pending.canonical_json(document_by_field)
        receipt = replace(
            legacy.receipt,
            canonical_manifest=canonical,
            manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest(),
            content_sha256=document_by_field["payload_set_sha256"],
        )
        _seal_read_header(header, receipt, document_by_field, manifest_digest.digest())
        receipts_by_stream[receipt.stream_id] = receipt
    _seal_read_bundle(case, request, receipts_by_stream)
    return case, request


def _seal_read_header(header, receipt, document_by_field, manifest_digest):
    header.payload_contract = pending.SEGMENTED_PAYLOAD_CONTRACT
    header.capture_state = "sealed"
    header.eof_at = dt.datetime.now(dt.UTC)
    header.canonical_manifest = receipt.canonical_manifest
    header.manifest_sha256 = bytes.fromhex(receipt.manifest_sha256)
    header.content_sha256 = bytes.fromhex(receipt.content_sha256)
    header.manifest_set_sha256 = manifest_digest
    for name in pending._ACCOUNTING_FIELDS:
        setattr(header, "committed_" + name, document_by_field[name])


def _seal_read_bundle(case, request, receipts_by_stream):
    for name, value in vars(_bundle(request)).items():
        if name not in {"capture_bundle_id", "snapshot_token_sha256"}:
            setattr(case.bundle, name, value)
    case.bundle.capture_state = "sealed"
    for name in pending._ACCOUNTING_FIELDS:
        setattr(
            case.bundle,
            "committed_" + name,
            sum(getattr(header, "committed_" + name) for header in case.captures_by_slot.values()),
        )
    canonical = pending._bundle_manifest(
        case.bundle, (("records", 1), ("details", 2)), receipts_by_stream, request.policy
    )
    case.bundle.canonical_manifest = canonical
    case.bundle.manifest_sha256 = hashlib.sha256(canonical.encode()).digest()


async def test_named_segmented_iterator_preserves_complete_manifests_and_cleanup(monkeypatch):
    case, _ = _segmented_read_case(monkeypatch)
    async with capture_store.open_segmented_parquet_parts(case.session, **_PART_IDS) as parts:
        assert case.session.rows.read_count == 0
        first = await anext(parts)
        assert first.capture.payload == b"first" and first.ordinal == 1
        assert first.capture.manifest.compressed_sha256 == hashlib.sha256(b"first").hexdigest()
        assert first.record_count == 0 and first.arrow_byte_count == 0
        assert len([part async for part in parts]) == 2
    assert case.session.rows.close_count == 1
    assert len(case.session.statement.selected_columns) == 11


async def test_eager_legacy_loader_rejects_segmented_capture_before_accumulating_parts(monkeypatch):
    case, _ = _segmented_read_case(monkeypatch)
    with pytest.raises(CaptureStoreError, match="incremental"):
        await capture_store.load_replayable_parquet_bundle(case.session, **_PART_IDS)
    assert case.session.rows.read_count == 1 and case.session.rows.close_count == 1


@pytest.mark.parametrize("damage", ["missing", "extra", "manifest_digest", "record_metric", "manifest_shape"])
async def test_segmented_accounting_rejects_complete_payload_drift(monkeypatch, damage):
    case, _ = _segmented_read_case(monkeypatch)
    if damage == "missing":
        case.source.pop()
    elif damage == "extra":
        case.source.append(case.source[-1])
    else:
        part_fields = list(case.source[1])
        index, value = {"manifest_digest": (7, b"x" * 32), "record_metric": (10, 1), "manifest_shape": (6, "{}")}[
            damage
        ]
        part_fields[index] = value
        case.source[1] = tuple(part_fields)
    with pytest.raises(CaptureStoreError):
        async with capture_store.open_segmented_parquet_parts(case.session, **_PART_IDS) as parts:
            received_parts = [part async for part in parts]
            pytest.fail(f"corrupted store returned {len(received_parts)} parts")
    assert case.session.rows.close_count == 1


async def test_segmented_early_exit_and_cancellation_close_owned_resources(monkeypatch):
    import asyncio

    case, _ = _segmented_read_case(monkeypatch)
    async with capture_store.open_segmented_parquet_parts(case.session, **_PART_IDS) as parts:
        await anext(parts)
    assert case.session.rows.read_count == 1 and case.session.rows.close_count == 1
    paused = asyncio.Event()

    async def consume():
        async with capture_store.open_segmented_parquet_parts(case.session, **_PART_IDS) as parts:
            case.session.rows.pause = paused
            await anext(parts)

    task = asyncio.create_task(consume())
    await paused.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert case.session.rows.close_count == 1


@pytest.mark.parametrize("operation", ["begin", "append", "eof", "seal"])
async def test_pending_mutations_require_a_caller_transaction_before_database_work(operation):
    class NoTransaction:
        def in_transaction(self):
            return False

    session = NoTransaction()
    request = _request()
    with pytest.raises(capture_store.CaptureStoreTransactionRequired):
        if operation == "begin":
            await pending.begin_pending_parquet_bundle(session, request=request)
        elif operation == "append":
            await pending.append_pending_parquet_part(
                session,
                request=request,
                capture_bundle_id=5,
                capture=_capture(request),
                ordinal=1,
                record_count=0,
                arrow_byte_count=0,
            )
        elif operation == "eof":
            await pending.mark_pending_parquet_eof(
                session, request=request, capture_bundle_id=5, stream_id="records", part_count=1, record_count=0
            )
        else:
            await pending.seal_pending_parquet_bundle(
                session, request=request, capture_bundle_id=5, receipts=(_receipt(request),)
            )


async def test_optional_binding_must_match_retained_execution_identity(monkeypatch):
    request = _request()
    execution = SimpleNamespace(request_identity_sha256=request.request_identity_sha256, source_binding_revision_id=7)
    monkeypatch.setattr(
        pending.lifecycle, "_lock_current_capture_binding", AsyncMock(return_value=(execution, "running", None))
    )
    with pytest.raises(CaptureBundleConflict, match="source binding"):
        await pending._lock_execution(_Session(), request)
