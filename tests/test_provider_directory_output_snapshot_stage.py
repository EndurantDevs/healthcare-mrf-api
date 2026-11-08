# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound actual encoded COPY bytes and preserve the original incumbent proof."""

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_stage as stage
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS
from tests.test_provider_directory_output_snapshot_custody import relation


def test_native_scalar_accounting_keeps_timestamps_numeric_and_utf8_bounded():
    record = (None, True, 4, 2.5, Decimal("12.3456"), datetime(2026, 1, 1), "é🙂", b"abc")
    assert stage._record_bytes(record) > 2 + 4 * len(record)
    assert list(stage._record_batches([record])) == [[record]]
    with pytest.raises(RuntimeError, match="copy_value_invalid"):
        stage._record_bytes((object(),))


def test_copy_batches_bound_rows_and_bytes_without_splitting_or_reordering(monkeypatch):
    monkeypatch.setattr(stage, "OUTPUT_COPY_MAX_ROWS", 2)
    monkeypatch.setattr(stage, "OUTPUT_COPY_MAX_BYTES", 64)
    records = [("a" * 20,), ("b" * 20,), ("c",), ("d",)]
    batches = list(stage._record_batches(records))
    assert [len(batch) for batch in batches] == [1, 2, 1]
    assert [record for batch in batches for record in batch] == records
    assert all(stage.COPY_FRAME_BYTES + sum(stage._record_bytes(record) for record in batch) <= 64 for batch in batches)
    with pytest.raises(RuntimeError, match="copy_record_too_large"):
        list(stage._record_batches([("x" * 64,)]))


@pytest.mark.asyncio
async def test_wide_singleton_refuses_before_native_copy(monkeypatch):
    monkeypatch.setattr(stage, "_stage_row", AsyncMock())
    monkeypatch.setattr(stage, "OUTPUT_COPY_MAX_BYTES", 128)
    capability = SimpleNamespace(
        policy=SimpleNamespace(schema="source_schema"), connection=SimpleNamespace(copy_records_to_table=AsyncMock())
    )
    private = SimpleNamespace(target=OUTPUT_ROOTS[0])
    directory = SimpleNamespace(
        _schema=lambda: "source_schema",
        _json_columns=lambda table: set(),
        _copy_record=lambda row, columns, json_columns: (row["value"],),
    )
    with pytest.raises(RuntimeError, match="copy_record_too_large"):
        await stage.copy_output_rows(directory, capability, private, [{"value": "x" * 128}])
    capability.connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_copy_status_must_match_the_complete_batch(monkeypatch):
    monkeypatch.setattr(stage, "_stage_row", AsyncMock())
    capability = SimpleNamespace(
        policy=SimpleNamespace(schema="source_schema"),
        connection=SimpleNamespace(copy_records_to_table=AsyncMock(return_value="COPY 0")),
    )
    private = SimpleNamespace(target=OUTPUT_ROOTS[0], relation="private_a", schema="source_schema")
    directory = SimpleNamespace(
        _schema=lambda: "source_schema",
        _json_columns=lambda table: set(),
        _copy_record=lambda row, columns, json_columns: (row["value"],),
    )
    with pytest.raises(RuntimeError, match="copy_incomplete"):
        await stage.copy_output_rows(directory, capability, private, [{"value": "x"}])
    assert capability.connection.copy_records_to_table.await_count == 1


@pytest.mark.asyncio
async def test_late_capture_cannot_adopt_an_incumbent_that_changed_after_preservation(monkeypatch):
    old, changed = relation(OUTPUT_ROOTS[0], 101), relation(OUTPUT_ROOTS[0], 501)
    capability = SimpleNamespace(reference=None, targets=(OUTPUT_ROOTS[0],))
    capture = AsyncMock(return_value={OUTPUT_ROOTS[0]: changed})
    monkeypatch.setattr(custody, "_capture", capture)
    with pytest.raises(RuntimeError, match="incumbent_changed"):
        await custody.capture_prepared_output(capability, OUTPUT_ROOTS[0], "private_a", expected_incumbent=old)
    assert capture.await_count == 1


def test_private_descriptor_binds_the_original_incumbent_without_nested_receipts():
    old = relation(OUTPUT_ROOTS[0], 101)
    private = stage.OutputStage("source_schema", OUTPUT_ROOTS[0], "private_a", 201, "b" * 64, old)
    payload = private.to_payload()
    assert stage.OutputStage.from_payload(payload) == private
    descriptor = stage._descriptor(private, "sealed")
    assert descriptor["incumbent_oid"] == old.oid
    assert descriptor["incumbent_digest"] == custody._digest(old.to_payload())
    assert "incumbent" not in descriptor
