# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Current-part payload reads and payload-free final coverage checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import capture_store as store
from process.custom_import.capture import CaptureManifest
from process.custom_import.capture_pending import _ReplayAccounting
from tests.test_custom_import_capture_store import _PART_IDS, _build_receipt, _PartReadSession, _PartRows
from tests.test_custom_import_snowflake_capture import _policy


def _part_row(ordinal):
    payload = f"synthetic-part-{ordinal}".encode()
    digest = hashlib.sha256(payload).digest()
    manifest = CaptureManifest(
        "records",
        "parquet",
        "none",
        "synthetic-snapshot-1",
        "a" * 64,
        len(payload),
        len(payload),
        digest.hex(),
        digest.hex(),
        "b" * 64,
    )
    canonical = json.dumps(asdict(manifest), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (
        9,
        1,
        ordinal,
        len(payload),
        payload,
        digest,
        canonical,
        hashlib.sha256(canonical.encode()).digest(),
        len(payload),
        24,
        1,
    )


def _headers(monkeypatch, rows):
    receipt = _build_receipt(byte_count=sum(len(row[4]) for row in rows))
    payload_digest = hashlib.sha256(store._PARQUET_PART_SET_DOMAIN)
    expected_by_field = dict(
        part_count=len(rows),
        byte_count=receipt.byte_count,
        decoded_byte_count=receipt.byte_count,
        arrow_byte_count=24 * len(rows),
        record_count=len(rows),
        manifest_byte_count=sum(len(row[6].encode()) for row in rows),
    )
    baseline = _ReplayAccounting(_policy(), expected_by_field, bytes(32), receipt.source_snapshot_token)
    for row in rows:
        baseline.add(receipt, row[2], row[4], row[6:])
        store._add_payload_part_digest(payload_digest, row[2], row[3], row[5])

    async def load(*arguments):
        return (
            {1: receipt},
            {1: (len(rows), payload_digest.digest())},
            {
                1: _ReplayAccounting(
                    _policy(), expected_by_field, baseline.digest.digest(), receipt.source_snapshot_token
                )
            },
        )

    monkeypatch.setattr(store, "_load_parquet_part_metadata", load)
    return receipt, baseline


async def test_current_part_query_skips_all_previous_payloads_and_closes(monkeypatch):
    rows = [_part_row(1), _part_row(2)]
    _headers(monkeypatch, rows)
    session = _PartReadSession(rows[1:])
    async with store.open_segmented_cursor_part(session, **_PART_IDS, stream_slot=1, part_ordinal=2) as part:
        assert part.ordinal == 2 and part.record_count == 1
        assert part.capture.payload == rows[1][4]
    statement = str(session.statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
    assert "stream_slot = 1" in statement and "part_ordinal = 2" in statement and "LIMIT 2" in statement
    assert session.rows.close_count == 1


@pytest.mark.parametrize("fault", ["missing", "duplicate", "payload", "manifest", "slot", "record_count"])
async def test_current_part_drift_fails_closed_and_closes(monkeypatch, fault):
    original_parts = _part_row(1)
    _headers(monkeypatch, [original_parts])
    damaged_fields = list(original_parts)
    index, content = {
        "payload": (4, b"different"),
        "manifest": (7, bytes(32)),
        "slot": (1, 2),
        "record_count": (10, True),
    }.get(fault, (0, 9))
    damaged_fields[index] = content
    rows = (
        []
        if fault == "missing"
        else [original_parts, original_parts]
        if fault == "duplicate"
        else [tuple(damaged_fields)]
    )
    session = _PartReadSession(rows)
    with pytest.raises(store.CaptureStoreError):
        async with store.open_segmented_cursor_part(session, **_PART_IDS, stream_slot=1, part_ordinal=1):
            pytest.fail("invalid part was yielded")
    assert session.rows.close_count == 1


async def test_metadata_only_accounting_matches_existing_payload_verifier(monkeypatch):
    rows = [_part_row(1), _part_row(2)]
    receipt, baseline = _headers(monkeypatch, rows)
    metadata_rows = [row[:4] + row[5:] for row in rows]
    session = _PartReadSession(metadata_rows)
    await store.verify_segmented_stream_metadata(session, **_PART_IDS, stream_slot=1, deadline=time.monotonic() + 60)
    assert session.rows.close_count == 1
    columns = {column.name for column in session.statement.selected_columns}
    assert "payload" not in columns and "payload_sha256" in columns
    accounting = _ReplayAccounting(
        _policy(), baseline.expected_by_field, baseline.digest.digest(), receipt.source_snapshot_token
    )
    for row in metadata_rows:
        store._add_segmented_metadata(accounting, receipt, row[2], row[3], row[4], row[5:])
    assert accounting.totals_by_field == baseline.totals_by_field
    assert accounting.digest.digest() == baseline.digest.digest()
    accounting.finish()


@pytest.mark.parametrize("fault", ["gap", "missing", "duplicate", "digest", "counts", "expired"])
async def test_final_metadata_coverage_or_deadline_failure_never_grants_eof(monkeypatch, fault):
    original_parts = [_part_row(1), _part_row(2)]
    _headers(monkeypatch, original_parts)
    rows = [row[:4] + row[5:] for row in original_parts]
    if fault == "missing":
        rows.pop()
    elif fault == "duplicate":
        rows[1] = rows[0]
    elif fault == "gap":
        rows[1] = rows[1][:2] + (3,) + rows[1][3:]
    elif fault in ("digest", "counts"):
        damaged_fields = list(rows[1])
        damaged_fields[4 if fault == "digest" else 9] = bytes(32) if fault == "digest" else 2
        rows[1] = tuple(damaged_fields)
    session = _PartReadSession(rows)
    with pytest.raises(store.CaptureStoreError):
        await store.verify_segmented_stream_metadata(
            session, **_PART_IDS, stream_slot=1, deadline=time.monotonic() + (-1 if fault == "expired" else 60)
        )
    assert session.rows.close_count == 1


async def test_consumer_failure_still_closes_current_part(monkeypatch):
    row = _part_row(1)
    _headers(monkeypatch, [row])
    session = _PartReadSession([row])
    with pytest.raises(RuntimeError, match="synthetic consumer"):
        async with store.open_segmented_cursor_part(session, **_PART_IDS, stream_slot=1, part_ordinal=1):
            raise RuntimeError("synthetic consumer")
    assert session.rows.close_count == 1


@pytest.mark.parametrize("is_cancel", [False, True])
async def test_reader_close_failure_preserves_original_error_or_cancel(monkeypatch, is_cancel):
    row = _part_row(1)
    _headers(monkeypatch, [row])
    session = _PartReadSession([row])
    primary = asyncio.CancelledError("synthetic original cancel") if is_cancel else ValueError("synthetic original")

    async def fail_close(_result):
        raise RuntimeError("synthetic reader close")

    monkeypatch.setattr(_PartRows, "close", fail_close)
    with pytest.raises(type(primary)) as caught:
        async with store.open_segmented_cursor_part(session, **_PART_IDS, stream_slot=1, part_ordinal=1):
            raise primary
    assert caught.value is primary and isinstance(primary.__cause__, RuntimeError)
    assert primary._custom_import_retry_blocked


async def test_metadata_drift_and_close_failure_keep_drift_as_primary(monkeypatch):
    row = _part_row(1)
    _headers(monkeypatch, [row])
    metadata_row = row[:4] + row[5:]
    session = _PartReadSession([metadata_row[:2] + (2,) + metadata_row[3:]])

    async def fail_close(_result):
        raise RuntimeError("synthetic metadata close")

    monkeypatch.setattr(_PartRows, "close", fail_close)
    with pytest.raises(store.CaptureBundleConflict) as caught:
        await store.verify_segmented_stream_metadata(
            session,
            **_PART_IDS,
            stream_slot=1,
            deadline=time.monotonic() + 60,
        )
    assert isinstance(caught.value.__cause__, RuntimeError) and caught.value._custom_import_retry_blocked


async def test_cursor_result_cleanup_drains_repeated_cancellation():
    entered, release = asyncio.Event(), asyncio.Event()
    close_events = []

    async def close():
        entered.set()
        await release.wait()
        close_events.append(True)

    from types import SimpleNamespace

    task = asyncio.create_task(store._close_cursor_result(SimpleNamespace(close=close)))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert close_events == [True]
