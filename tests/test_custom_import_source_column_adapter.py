# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded Arrow windows and ordered native SOURCE preparation."""

import struct
from contextlib import aclosing
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pyarrow as pa
import pytest

from process.custom_import import source_column_preparation as adapter
from process.custom_import.capture import CaptureError, CaptureLimits, _record_size
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source import _root
from tests.test_custom_import_build_source_bulk import _bulk_context


@pytest.mark.parametrize("link", ["__cause__", "__context__"])
@pytest.mark.parametrize("capability", [adapter.native_column_encoder, adapter.native_column_plan_factory])
def test_optional_capability_never_hides_wrapped_allocation_failure(monkeypatch, link, capability):
    import importlib

    failure = ImportError("extension initialization failed")
    setattr(failure, link, MemoryError("extension allocation"))
    monkeypatch.setattr(importlib, "import_module", Mock(side_effect=failure))
    with pytest.raises(ImportError) as caught:
        capability.__wrapped__()
    assert caught.value is failure


def test_missing_compiled_capability_binds_the_original_abi(monkeypatch):
    layout = adapter.compile_column_layout(_bulk_context())
    encoder = Mock(return_value="original result")
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    monkeypatch.setattr(adapter, "native_column_plan_factory", lambda: None)
    bound = adapter.bind_column_encoder(layout)
    assert bound(("descriptors",), 123, include_preparation_bounds=True) == "original result"
    encoder.assert_called_once_with(layout, ("descriptors",), 123, include_preparation_bounds=True)


def test_compiled_capability_binds_once_without_legacy_entry(monkeypatch):
    layout = adapter.compile_column_layout(_bulk_context())
    encoder = Mock(side_effect=AssertionError("compiled layout entered legacy ABI"))
    prepared = Mock(return_value="compiled result")
    factory = Mock(return_value=SimpleNamespace(prepare=prepared))
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    monkeypatch.setattr(adapter, "native_column_plan_factory", lambda: factory)
    bound = adapter.bind_column_encoder(layout)
    for _ in range(3):
        assert bound(("descriptors",), 123, include_preparation_bounds=True) == "compiled result"
    factory.assert_called_once_with(layout)
    assert prepared.call_count == 3
    encoder.assert_not_called()


@pytest.mark.parametrize("stage", ["compile", "prepare"])
@pytest.mark.parametrize("failure", [ValueError("native failure"), MemoryError("native allocation")])
def test_compiled_capability_failure_never_retries_legacy(monkeypatch, stage, failure):
    encoder = Mock(side_effect=AssertionError("failed native call retried"))
    prepared = Mock(side_effect=failure)
    factory = Mock(side_effect=failure) if stage == "compile" else Mock(return_value=SimpleNamespace(prepare=prepared))
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    monkeypatch.setattr(adapter, "native_column_plan_factory", lambda: factory)
    with pytest.raises(type(failure)) as caught:
        adapter.bind_column_encoder(adapter.compile_column_layout(_bulk_context()))((), 123)
    assert caught.value is failure
    factory.assert_called_once()
    encoder.assert_not_called()


def _batch(source_rows=None, *, score_type=None):
    source_rows = source_rows or [_root(), _root(score=Decimal("0.000000100000"))]
    return pa.record_batch(
        [
            pa.array([source_row["npi"] for source_row in source_rows], type=pa.string()),
            pa.array([source_row["score"] for source_row in source_rows], type=score_type or pa.decimal128(30, 12)),
            pa.array([source_row["enabled"] for source_row in source_rows], type=pa.bool_()),
        ],
        names=["npi", "score", "enabled"],
    )


def _accepted(prepared_rows, batch):
    accepted_outcomes = []
    for index, prepared_row in enumerate(prepared_rows):
        values_by_label = {name: batch.column(column)[index].as_py() for column, name in enumerate(batch.schema.names)}
        accepted_outcomes.append(
            (
                *prepared_row.raw_key,
                *prepared_row.typed_key,
                prepared_row.payload,
                prepared_row.payload_hash,
                prepared_row.child_key,
                prepared_row.child_hash,
                prepared_row.byte_count,
                _record_size(values_by_label),
            )
        )
    return accepted_outcomes


@pytest.mark.parametrize("physical", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
def test_fixed_width_slice_copies_only_logical_values(physical):
    array = pa.array([0, 1, 2, None, 4, 5, 6, 7, 8], type=physical).slice(3, 4)
    column = adapter._column_window("value", array)
    assert column[2:4] == (3, 4)
    assert len(column[-1]) == 4 * physical.bit_width // 8
    assert column[6].tobytes() == array.buffers()[0].slice(0, 1).to_pybytes()


@pytest.mark.parametrize("physical", [pa.string(), pa.large_string()])
def test_utf8_slice_keeps_original_offsets_and_only_logical_data(physical):
    array = pa.array(["unused prefix", "Ω", None, "line\n", "unused tail"], type=physical).slice(1, 3)
    column = adapter._column_window("value", array)
    assert column[2:4] == (1, 3)
    assert column[-1].tobytes() == "Ωline\n".encode()
    assert len(column[-2]) == (3 + 1) * (4 if physical == pa.string() else 8)


def test_boolean_slice_keeps_bitmap_offset_and_minimum_window():
    array = pa.array([True, False, True, None, False, True, False, True, False, True]).slice(3, 7)
    column = adapter._column_window("value", array)
    assert column[2:4] == (3, 7)
    assert len(column[-1]) == len(column[6]) == 2


def test_decimal_slice_retains_physical_precision_scale_and_exact_coefficient():
    array = pa.array([Decimal("1"), Decimal("0.0000001"), None], type=pa.decimal128(30, 12)).slice(1, 2)
    column = adapter._column_window("value", array)
    assert column[4:6] == (30, 12)
    assert len(column[-1]) == 32
    assert int.from_bytes(column[-1][:16], "little", signed=True) == 100000


def test_over_budget_column_selects_oracle_before_owned_buffer_copies(monkeypatch):
    context = _bulk_context()
    batch = _batch([_root(npi="x" * 131073)])
    copy = Mock(side_effect=AssertionError("oversize input was copied"))
    monkeypatch.setattr(adapter, "bytes", copy, raising=False)
    assert adapter.column_descriptors(adapter.compile_column_layout(context), batch) is None
    copy.assert_not_called()


@pytest.mark.asyncio
async def test_accepted_adapter_never_builds_python_rows(monkeypatch):
    context, batch, limits = _bulk_context(), _batch(), CaptureLimits()
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, limits)
    encoder = Mock(return_value=_accepted(expected, batch))
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    monkeypatch.setattr(
        adapter, "_decoded_arrow_row", Mock(side_effect=AssertionError("accepted row decoded in Python"))
    )
    actual = await adapter.prepare_arrow_batch_columns(context, adapter.compile_column_layout(context), batch, limits)
    assert actual == expected
    columns = encoder.call_args.args[1]
    assert tuple(column[1] for column in columns) == ("utf8", "decimal128", "bool")
    assert all(type(value) is bytes for column in columns for value in column[6:] if value is not None)


@pytest.mark.asyncio
async def test_unsupported_decimal256_uses_complete_existing_oracle(monkeypatch):
    context, batch, limits = _bulk_context(), _batch(score_type=pa.decimal256(50, 12)), CaptureLimits()
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, limits)
    encoder = Mock(side_effect=AssertionError("unsupported input entered native code"))
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    assert (
        await adapter.prepare_arrow_batch_columns(context, adapter.compile_column_layout(context), batch, limits)
        == expected
    )
    encoder.assert_not_called()


@pytest.mark.asyncio
async def test_native_exception_never_retries_oracle(monkeypatch):
    context, batch = _bulk_context(), _batch()
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: Mock(side_effect=ValueError("native failure")))
    fallback = Mock(side_effect=AssertionError("native exception retried"))
    monkeypatch.setattr(adapter, "iter_arrow_batch_oracle", fallback)
    with pytest.raises(ValueError, match="native failure"):
        await adapter.prepare_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits()
        )
    fallback.assert_not_called()


@pytest.mark.asyncio
async def test_native_transport_byte_count_preserves_capture_limit(monkeypatch):
    context, batch = _bulk_context(), _batch()
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, CaptureLimits())
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: Mock(return_value=_accepted(expected, batch)))
    limits = CaptureLimits(maximum_record_bytes=1)
    with pytest.raises(CaptureError, match="record 1 exceeds the byte limit"):
        await adapter.prepare_arrow_batch_columns(context, adapter.compile_column_layout(context), batch, limits)


@pytest.mark.asyncio
async def test_incomplete_native_result_is_fatal(monkeypatch):
    context, batch = _bulk_context(), _batch()
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: Mock(return_value=[]))
    with pytest.raises(CandidateRunnerError, match="incomplete batch"):
        await adapter.prepare_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits()
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind, code", [("transport-error", "integer_invalid"), ("preparation-error", "raw_key_byte_limit")]
)
async def test_capture_limit_precedes_expected_native_row_error(monkeypatch, kind, code):
    context, batch = _bulk_context(), _batch()
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: Mock(return_value=[(kind, code, 100)] * 2))
    with pytest.raises(CaptureError, match="record 1 exceeds the byte limit"):
        await adapter.prepare_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits(maximum_record_bytes=1)
        )


@pytest.mark.asyncio
async def test_transport_data_error_keeps_existing_exception_without_redecode(monkeypatch):
    from process.custom_import.snowflake_bundle import SnowflakeBundleError

    context, batch = _bulk_context(), _batch()
    monkeypatch.setattr(
        adapter, "native_column_encoder", lambda: Mock(return_value=[("transport-error", "integer_range", 100)] * 2)
    )
    monkeypatch.setattr(adapter, "_decoded_arrow_row", Mock(side_effect=AssertionError("error row decoded again")))
    with pytest.raises(SnowflakeBundleError, match="bundle replay integer result exceeds the signed 64-bit range"):
        await adapter.prepare_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits()
        )


@pytest.mark.asyncio
async def test_baseline_reuses_the_stream_layout(monkeypatch):
    context, batch, limits = _bulk_context(), _batch(), CaptureLimits()
    compiled = adapter.native_layout(context.request.definition, context.stream)
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, limits)
    monkeypatch.setattr(adapter, "native_layout", Mock(side_effect=AssertionError("stream layout rebuilt")))
    assert await adapter.prepare_arrow_batch_oracle(context, batch, limits, compiled_layout=compiled) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,error,message",
    [
        (("transport-error", "integer_invalid", 100), "transport", "bundle replay integer result is invalid"),
        (("preparation-error", "raw_key_byte_limit", 100), "raw", "raw family key exceeds"),
        (("preparation-error", "occurrence_byte_limit", 100), "occurrence", "one source occurrence exceeds"),
        (("transport-error", "integer_range", 1000), "capture", "record 2 exceeds the byte limit"),
    ],
)
async def test_native_iterator_yields_valid_prefix_before_later_row_error(monkeypatch, outcome, error, message):
    from process.custom_import.family_raw_key import RawFamilyKeyError
    from process.custom_import.snowflake_bundle import SnowflakeBundleError

    context, batch = _bulk_context(), _batch()
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, CaptureLimits())
    encoder = Mock(return_value=[_accepted(expected, batch)[0], outcome])
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    error_by_kind = {
        "transport": SnowflakeBundleError,
        "raw": RawFamilyKeyError,
        "occurrence": CandidateRunnerError,
        "capture": CaptureError,
    }
    async with aclosing(
        adapter.iter_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits(maximum_record_bytes=200)
        )
    ) as rows:
        assert await anext(rows) == expected[0]
        with pytest.raises(error_by_kind[error], match=message):
            await anext(rows)
    assert encoder.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("native_none", [False, True])
async def test_oracle_selection_preserves_valid_prefix_before_capture_error(monkeypatch, native_none):
    context, batch = _bulk_context(), _batch([_root(), _root(npi="x" * 300)])
    expected = await adapter.prepare_arrow_batch_oracle(context, batch.slice(0, 1), CaptureLimits())
    encoder = Mock(return_value=None) if native_none else None
    monkeypatch.setattr(adapter, "native_column_encoder", lambda: encoder)
    async with aclosing(
        adapter.iter_arrow_batch_columns(
            context, adapter.compile_column_layout(context), batch, CaptureLimits(maximum_record_bytes=200)
        )
    ) as rows:
        assert await anext(rows) == expected[0]
        with pytest.raises(CaptureError, match="record 2 exceeds the byte limit"):
            await anext(rows)
    if encoder is not None:
        assert encoder.call_count == 1


@pytest.mark.asyncio
async def test_oracle_iterator_translates_later_arrow_scalar_decode_failure(monkeypatch):
    context = _bulk_context()
    npi = pa.Array.from_buffers(
        pa.string(), 2, [None, pa.py_buffer(struct.pack("<iii", 0, 10, 11)), pa.py_buffer(b"1003000126\xff")]
    )
    batch = pa.record_batch(
        [npi, pa.array([Decimal("1"), Decimal("2")]), pa.array([True, True])], names=["npi", "score", "enabled"]
    )
    expected = await adapter.prepare_arrow_batch_oracle(context, batch.slice(0, 1), CaptureLimits())
    async with aclosing(adapter.iter_arrow_batch_oracle(context, batch, CaptureLimits())) as rows:
        assert await anext(rows) == expected[0]
        with pytest.raises(CaptureError, match="Parquet source payload is invalid") as caught:
            await anext(rows)
    assert isinstance(caught.value.__cause__, UnicodeDecodeError)


def test_arrow_scalar_allocation_failure_keeps_its_identity():
    failure = MemoryError("scalar allocation")
    batch = MagicMock()
    batch.schema.names = ("npi",)
    batch.column.return_value.__getitem__.return_value.as_py.side_effect = failure
    with pytest.raises(MemoryError) as caught:
        adapter._decoded_arrow_row(batch, 0, 1, CaptureLimits())
    assert caught.value is failure


@pytest.mark.asyncio
async def test_closing_native_none_iterator_closes_selected_oracle(monkeypatch):
    context, batch = _bulk_context(), _batch()
    expected = await adapter.prepare_arrow_batch_oracle(context, batch, CaptureLimits())
    events = []

    async def oracle(*_args, **_kwargs):
        try:
            yield expected[0]
            raise AssertionError("closed oracle was advanced")
        finally:
            events.append("closed")

    monkeypatch.setattr(adapter, "native_column_encoder", lambda: Mock(return_value=None))
    monkeypatch.setattr(adapter, "iter_arrow_batch_oracle", oracle)
    rows = adapter.iter_arrow_batch_columns(context, adapter.compile_column_layout(context), batch, CaptureLimits())
    assert await anext(rows) == expected[0]
    await rows.aclose()
    assert events == ["closed"]
