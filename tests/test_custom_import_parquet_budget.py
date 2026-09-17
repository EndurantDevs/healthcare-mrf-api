# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused resource-budget checks for custom-import Parquet decoding."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import process.custom_import.capture as capture_module
from process.custom_import import (
    CaptureError,
    CaptureLimits,
    CustomImportDefinition,
    capture_stream,
    iter_records,
    load_json_definition,
)

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


def _parquet_stream():
    """Build one synthetic custom-import stream configured for Parquet."""

    raw_definition = load_json_definition((FIXTURES / "v1_valid.json").read_text())
    stream_mapping = raw_definition["streams"][0]
    stream_mapping["format"] = "parquet"
    stream_mapping["compression"] = "none"
    return CustomImportDefinition.from_mapping(raw_definition).source_streams[0]


def _capture_parquet(parquet_bytes: bytes, source_stream, limits: CaptureLimits):
    """Seal synthetic Parquet bytes with the supplied resource limits."""

    return capture_stream(
        BytesIO(parquet_bytes),
        source_stream,
        source_snapshot_token="snapshot-20260917",
        limits=limits,
    )


def _write_parquet_bytes(column_values_by_name: dict[str, object]) -> bytes:
    """Write a small checksummed Parquet payload without filesystem access."""

    native_buffer = BytesIO()
    pq.write_table(
        pa.table(column_values_by_name),
        native_buffer,
        compression="zstd",
        write_page_checksum=True,
    )
    return native_buffer.getvalue()


def _declared_parquet_bytes(parquet_bytes: bytes) -> int:
    """Return footer-declared uncompressed bytes for one generated payload."""

    file_metadata = pq.ParquetFile(BytesIO(parquet_bytes)).metadata
    assert file_metadata is not None
    declared_bytes = 0
    for row_group_index in range(file_metadata.num_row_groups):
        row_group_metadata = file_metadata.row_group(row_group_index)
        for column_index in range(row_group_metadata.num_columns):
            declared_bytes += row_group_metadata.column(column_index).total_uncompressed_size
    return declared_bytes


def _assert_rejected_before_batches(monkeypatch, source_stream, parquet_bytes: bytes, limits: CaptureLimits) -> None:
    """Require page preflight to reject the resource budget before Arrow batches run."""

    monkeypatch.setattr(
        capture_module.pq.ParquetFile,
        "iter_batches",
        lambda *_args, **_kwargs: pytest.fail("resource preflight must run before batches"),
    )
    with pytest.raises(CaptureError, match="decoded-byte"):
        list(iter_records(_capture_parquet(parquet_bytes, source_stream, limits), source_stream, limits=limits))


def test_parquet_preflight_reserves_reader_and_column_buffers(monkeypatch):
    """A small payload cannot bypass the reader and active-column working budget."""

    source_stream = _parquet_stream()
    parquet_bytes = _write_parquet_bytes({"source_value": ["small value"]})
    restrictive_limits = CaptureLimits(maximum_decoded_bytes=64 * 1024, maximum_record_bytes=1024)

    assert len(parquet_bytes) < restrictive_limits.maximum_decoded_bytes
    assert _declared_parquet_bytes(parquet_bytes) < restrictive_limits.maximum_decoded_bytes
    _assert_rejected_before_batches(monkeypatch, source_stream, parquet_bytes, restrictive_limits)


def test_parquet_preflight_aggregates_active_column_buffers(monkeypatch):
    """Two active columns must reserve separate decoder working allowances."""

    source_stream = _parquet_stream()
    restrictive_limits = CaptureLimits(maximum_decoded_bytes=96 * 1024, maximum_record_bytes=1024)
    single_column_bytes = _write_parquet_bytes({"source_value": ["small value"]})
    assert _declared_parquet_bytes(single_column_bytes) < restrictive_limits.maximum_decoded_bytes
    assert (
        len(
            list(
                iter_records(
                    _capture_parquet(single_column_bytes, source_stream, restrictive_limits),
                    source_stream,
                    limits=restrictive_limits,
                )
            )
        )
        == 1
    )

    multi_column_bytes = _write_parquet_bytes({"source_value": ["small value"], "secondary_value": ["another value"]})
    assert _declared_parquet_bytes(multi_column_bytes) < restrictive_limits.maximum_decoded_bytes
    _assert_rejected_before_batches(monkeypatch, source_stream, multi_column_bytes, restrictive_limits)


def test_parquet_preflight_reserves_reader_buffer_for_empty_groups(monkeypatch):
    """Empty groups reserve reader state but no active-column decoder allowance."""

    source_stream = _parquet_stream()
    parquet_bytes = _write_parquet_bytes({"source_value": pa.array([], type=pa.string())})
    restrictive_limits = CaptureLimits(maximum_decoded_bytes=64 * 1024 - 1, maximum_record_bytes=1024)

    assert _declared_parquet_bytes(parquet_bytes) < restrictive_limits.maximum_decoded_bytes
    _assert_rejected_before_batches(monkeypatch, source_stream, parquet_bytes, restrictive_limits)
