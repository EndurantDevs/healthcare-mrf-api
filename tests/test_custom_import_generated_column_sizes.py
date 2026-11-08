# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native generated-record accounting against the previous scalar loop."""

from dataclasses import replace
from decimal import Decimal, localcontext

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from process.custom_import import snowflake_python
from process.custom_import.capture import CaptureError, _scalar_size
from process.custom_import.capture_limits import CaptureLimits


def _scalar_reference(table, limits):
    ordinal = logical_bytes = 0
    label_bytes = sum(len(label.encode("utf-8")) + 4 for label in table.column_names)
    for batch in table.to_batches(max_chunksize=min(snowflake_python._FETCH_ROWS, limits.maximum_records)):
        bitmap_labels = tuple(
            field.name
            for field, column in zip(batch.schema, batch.columns, strict=True)
            if not pa.types.is_integer(field.type) and column.buffers()[0] is not None
        )
        for row_index, values in enumerate(batch.to_pylist()):
            ordinal += 1
            logical_bytes += batch.slice(row_index, 1).nbytes - sum(
                values[label] is not None for label in bitmap_labels
            )
            if logical_bytes > limits.maximum_decoded_bytes:
                raise CaptureError("Parquet source payload exceeds the decoded-byte limit")
            if label_bytes + sum(_scalar_size(value) for value in values.values()) > limits.maximum_record_bytes:
                raise CaptureError(f"record {ordinal} exceeds the byte limit")


def _outcome(function, table, limits):
    try:
        function(table, limits)
    except CaptureError as error:
        return str(error)
    return None


@pytest.mark.parametrize("record_count", [7, 8, 9, 33])
@pytest.mark.parametrize("layout", ["plain", "slice", "chunks"])
@pytest.mark.parametrize("null_mode", ["none", "all", "mixed"])
def test_generated_native_sizes_match_scalar_prefixes(monkeypatch, record_count, layout, null_mode):
    monkeypatch.setattr(snowflake_python, "_FETCH_ROWS", 3)
    types = [pa.int64(), pa.bool_(), pa.string(), pa.large_string(), pa.decimal128(38, 12), pa.null()]
    values = [-(2**63), False, "🙂åλ中", "", Decimal("1E-12"), None]
    count = record_count + 3 if layout == "slice" else record_count
    columns = [
        pa.array(
            [None if null_mode == "all" or (null_mode == "mixed" and index % 2) else value for index in range(count)],
            type=scalar_type,
        )
        for value, scalar_type in zip(values, types, strict=True)
    ]
    if layout == "chunks":
        columns = [pa.chunked_array([column.slice(0, 1), column.slice(1, 3), column.slice(4)]) for column in columns]
    table = pa.Table.from_arrays(columns, names=["count", "flag", "text", "wide", "amount", "empty"])
    if layout == "slice":
        table = table.slice(2, record_count)
    for record_bytes in (1, 40, 256):
        for decoded_bytes in (record_bytes, 512, 2048):
            limits = CaptureLimits(maximum_record_bytes=record_bytes, maximum_decoded_bytes=decoded_bytes)
            assert _outcome(snowflake_python._validate_generated_landing_records, table, limits) == (
                _outcome(_scalar_reference, table, limits)
            )


@pytest.mark.parametrize("scale", [0, 6, 7, 12, 38])
def test_generated_decimal_native_lengths_are_python_scalar_lengths(scale):
    coefficients = [0, 1, 9, 10, 99, 100, 1234500, 10**18, 10**38 - 1]
    values = [
        Decimal((sign, tuple(map(int, str(coefficient))), -scale)) for coefficient in coefficients for sign in (0, 1)
    ] + [None]
    column = pa.array(values, type=pa.decimal128(38, scale))
    sizes = pc.fill_null(pc.binary_length(pc.cast(column, pa.string())), 4).to_pylist()
    for capitals in (0, 1):
        with localcontext() as context:
            context.capitals = capitals
            assert sizes == [_scalar_size(value) for value in column.to_pylist()]


@pytest.mark.parametrize(
    ("values", "limits", "expected"),
    [
        (
            ["x" * 41, "", "x" * 200],
            CaptureLimits(maximum_record_bytes=40, maximum_decoded_bytes=100),
            "record 1 exceeds the byte limit",
        ),
        (
            ["x" * 20] * 3 + ["x" * 100],
            CaptureLimits(maximum_record_bytes=40, maximum_decoded_bytes=50),
            "Parquet source payload exceeds the decoded-byte limit",
        ),
        (
            ["a", "x" * 41],
            CaptureLimits(maximum_record_bytes=40, maximum_decoded_bytes=49),
            "Parquet source payload exceeds the decoded-byte limit",
        ),
        (
            [""] * 7 + ["x" * 41],
            CaptureLimits(maximum_record_bytes=40, maximum_decoded_bytes=1000),
            "record 8 exceeds the byte limit",
        ),
    ],
)
def test_generated_native_first_failure_and_tie_precedence(monkeypatch, values, limits, expected):
    monkeypatch.setattr(snowflake_python, "_FETCH_ROWS", 3)
    table = pa.table({"text": values})
    assert _outcome(_scalar_reference, table, limits) == expected
    assert _outcome(snowflake_python._validate_generated_landing_records, table, limits) == expected
    snowflake_python._validate_generated_landing_records(
        table, replace(limits, maximum_record_bytes=2**80, maximum_decoded_bytes=2**80)
    )
