# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic bounded landing checks; no source connection or database."""

from __future__ import annotations

import weakref
from asyncio import CancelledError
from dataclasses import replace
from io import BytesIO

import pyarrow.parquet as pq
import pytest

from process.custom_import import snowflake_python
from process.custom_import.capture import CaptureError, iter_records
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_python import SnowflakeLandingEOF, SnowflakeLandingPart
from tests.test_custom_import_snowflake_bundle import _Credentials
from tests.test_custom_import_snowflake_python import _bundle_statement, _connect, _Cursor, _data_row, _metadata_row
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row


def _open(adapter, statement, *, part_limits=None, maximum_arrow_bytes=64 * 1024 * 1024):
    return adapter.open_bundle_landing(
        statement,
        _Credentials().load_key_pair(),
        part_limits=part_limits or CaptureLimits(),
        maximum_part_arrow_bytes=maximum_arrow_bytes,
        timeout_seconds=17,
    )


def _shared_open(monkeypatch, rows=(), **configuration):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, rows, **configuration)
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    result = _open(adapter, connector.build_statement(request))
    return result, cursor, connection


def test_shared_landing_emits_all_projections_before_next_fetch(monkeypatch):
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(), _shared_row(key="b")))
    with result:
        events = result.consume_events()
        parts = []
        for ordinal in (1, 2):
            root = next(events)
            fetch_count = cursor.fetchone.call_count
            child = next(events)
            assert cursor.fetchone.call_count == fetch_count
            assert (root.stream_id, child.stream_id) == ("root_source", "detail_source")
            assert root.ordinal == child.ordinal == ordinal
            assert root.record_count == child.record_count == 1
            root_table = pq.read_table(BytesIO(root.capture.payload))
            child_table = pq.read_table(BytesIO(child.capture.payload))
            assert root_table["npi"] == child_table["detail_npi"]
            assert root_table["score"] == child_table["amount"]
            assert root.arrow_byte_count == root_table.nbytes
            assert child.arrow_byte_count == child_table.nbytes
            parts.extend((root, child))
        assert list(events) == [SnowflakeLandingEOF("root_source", 2, 2), SnowflakeLandingEOF("detail_source", 2, 2)]
        assert cursor.fetchone.call_count == 5  # Two metadata, two data, actual EOF.
        assert len(cursor.executed) == 1
        for part in parts:
            stream = result._owner._streams[part.stream_id]
            assert part.capture.manifest.source_snapshot_token == result.source_snapshot_token
            assert len(list(iter_records(part.capture, stream))) == part.record_count
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("shared_rows", [(), (_shared_row(),)])
def test_interleaved_relations_keep_empty_parts_and_global_eof(monkeypatch, shared_rows):
    rows = (
        *(row + (None, None, None) for row in shared_rows),
        (1, 2, "other_source", None, None, None, None, None, None, None, "1003000126", "x", 2),
    )
    result, cursor, connection = _shared_open(monkeypatch, rows, interleaved=True)
    with result:
        events = result.consume_events()
        parts = [next(events) for _ in range(3)]
        assert all(isinstance(event, SnowflakeLandingPart) for event in parts)
        assert [part.stream_id for part in parts] == ["root_source", "detail_source", "other_source"]
        assert [part.record_count for part in parts] == [len(shared_rows), len(shared_rows), 1]
        for part in parts:
            assert pq.read_table(BytesIO(part.capture.payload)).num_columns > 0
        eof = next(events)
        assert isinstance(eof, SnowflakeLandingEOF)
        assert not cursor._rows
        assert len(list(events)) == 2
    assert cursor.closed and connection.closed


def test_empty_shared_landing_seals_one_schema_part_per_stream(monkeypatch):
    result, cursor, connection = _shared_open(monkeypatch)
    with result:
        events = list(result.consume_events())
    assert [type(event) for event in events] == [SnowflakeLandingPart] * 2 + [SnowflakeLandingEOF] * 2
    for part in events[:2]:
        assert part.ordinal == 1 and part.record_count == 0 and part.capture.payload
        assert pq.read_table(BytesIO(part.capture.payload)).num_rows == 0
    assert cursor.closed and connection.closed


def test_landing_retains_no_reader_or_previous_event_history(monkeypatch):
    references = []
    encode = snowflake_python._parquet_reader_with_metrics

    def track_reader(*arguments, **options):
        assert all(reference() is None or reference().closed for reference in references)
        reader, arrow_bytes = encode(*arguments, **options)
        references.append(weakref.ref(reader))
        return reader, arrow_bytes

    monkeypatch.setattr(snowflake_python, "_parquet_reader_with_metrics", track_reader)
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(key=str(index)) for index in range(500)))
    previous_event = None
    with result:
        for event in result.consume_events():
            assert previous_event is None or previous_event() is None
            previous_event = weakref.ref(event)
            assert result._owner._shared_partitions == {}
    assert len(references) == 1_000
    assert all(reference() is None or reference().closed for reference in references)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("advance", [False, True])
def test_landing_context_closes_before_eof_and_events_are_single_use(monkeypatch, advance):
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(), _shared_row()))
    with result:
        events = result.consume_events()
        if advance:
            assert isinstance(next(events), SnowflakeLandingPart)
        with pytest.raises(SnowflakeConnectorError, match="already consumed"):
            result.consume_events()
    result.close()
    assert cursor.closed and connection.closed
    assert list(events) == []


@pytest.mark.parametrize("failure", [RuntimeError("fetch"), CancelledError()])
def test_landing_fetch_failure_preserves_primary_and_closes(monkeypatch, failure):
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(),))
    events = result.consume_events()
    next(events)
    next(events)

    def fail_fetch():
        raise failure

    cursor.fetchone = fail_fetch
    cursor.close = lambda: (_ for _ in ()).throw(RuntimeError("close"))
    with pytest.raises(type(failure) if isinstance(failure, CancelledError) else SnowflakeConnectorError) as caught:
        next(events)
    assert caught.value is failure or caught.value.__cause__ is failure
    assert connection.closed


def test_suspended_landing_explicit_close_reports_cleanup_failure(monkeypatch):
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(),))
    events = result.consume_events()
    next(events)
    failure = RuntimeError("synthetic close failure")

    def fail_close():
        raise failure

    cursor.close = fail_close
    with pytest.raises(SnowflakeConnectorError, match="resource cleanup failed") as caught:
        result.close()
    assert caught.value.__cause__.__cause__ is failure
    assert connection.closed
    assert list(events) == []


def test_landing_uses_explicit_part_limits_not_legacy_aggregate_limits(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(
        monkeypatch,
        (_shared_row(key=str(index)) for index in range(5)),
        limits=CaptureLimits(maximum_compressed_bytes=1),
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    monkeypatch.setattr(snowflake_python, "MAX_BUNDLE_PARTITIONS", 1)
    monkeypatch.setattr(snowflake_python, "MAX_BUNDLE_CAPTURE_BYTES", 1)
    with _open(adapter, connector.build_statement(request)) as result:
        assert sum(isinstance(event, SnowflakeLandingPart) for event in result.consume_events()) == 10
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("maximum_arrow_bytes", [0, True, -1, 257 * 1024 * 1024])
def test_invalid_landing_limits_fail_before_connection(monkeypatch, maximum_arrow_bytes):
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader", warehouse="warehouse")
    monkeypatch.setattr(adapter, "_connect", lambda *_arguments, **_options: pytest.fail("must not connect"))
    with pytest.raises(SnowflakeConnectorError, match="Arrow limit"):
        _open(adapter, _bundle_statement(), maximum_arrow_bytes=maximum_arrow_bytes)


@pytest.mark.parametrize("limit_kind", ["fields", "record_bytes", "arrow_bytes", "encoded_bytes"])
def test_landing_enforces_per_part_limits_and_closes(monkeypatch, limit_kind):
    cursor = _Cursor((_metadata_row(), _data_row("1234567893", "Cardiology")))
    connection, _ = _connect(monkeypatch, cursor)
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader", warehouse="warehouse")
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    limits = CaptureLimits()
    arrow_bytes = 64 * 1024 * 1024
    if limit_kind == "fields":
        limits = replace(limits, maximum_fields_per_record=1)
    elif limit_kind == "record_bytes":
        limits = replace(limits, maximum_record_bytes=1)
    elif limit_kind == "arrow_bytes":
        arrow_bytes = 1
    else:
        limits = replace(limits, maximum_compressed_bytes=1)
    with pytest.raises(SnowflakeConnectorError):
        with _open(adapter, _bundle_statement(), part_limits=limits, maximum_arrow_bytes=arrow_bytes) as result:
            list(result.consume_events())
    if limit_kind == "fields":
        assert cursor.executed == []
    else:
        assert cursor.closed and connection.closed


@pytest.mark.parametrize("part_limits", [object(), CaptureLimits(maximum_fields_per_record=1_025)])
def test_invalid_part_limits_fail_before_connection(monkeypatch, part_limits):
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader", warehouse="warehouse")
    monkeypatch.setattr(adapter, "_connect", lambda *_arguments, **_options: pytest.fail("must not connect"))
    with pytest.raises(SnowflakeConnectorError, match="part limits"):
        _open(adapter, _bundle_statement(), part_limits=part_limits)


def test_landing_record_byte_limit_is_checked_before_encoding(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(),))
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    monkeypatch.setattr(
        snowflake_python,
        "_parquet_reader_with_metrics",
        lambda *_arguments, **_options: pytest.fail("must not encode"),
    )
    with _open(
        adapter, connector.build_statement(request), part_limits=CaptureLimits(maximum_record_bytes=1)
    ) as result:
        with pytest.raises(SnowflakeConnectorError) as failure:
            next(result.consume_events())
    assert isinstance(failure.value.__cause__, CaptureError)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("failure", [ValueError("encode"), CancelledError()])
def test_landing_encoding_failure_closes_untransferred_destination(monkeypatch, failure):
    readers = []

    def fail_encode(_table, destination, **_options):
        readers.append(destination)
        raise failure

    monkeypatch.setattr(snowflake_python.pq, "write_table", fail_encode)
    result, cursor, connection = _shared_open(monkeypatch, (_shared_row(),))
    with result:
        with pytest.raises(type(failure) if isinstance(failure, CancelledError) else SnowflakeConnectorError):
            next(result.consume_events())
    assert len(readers) == 1 and readers[0].closed
    assert cursor.closed and connection.closed


def test_landing_splits_by_explicit_part_record_count(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(
        monkeypatch,
        tuple(_shared_row(key=str(index)) for index in range(5)),
        partition_rows=4,
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    with _open(adapter, connector.build_statement(request), part_limits=CaptureLimits(maximum_records=2)) as result:
        parts = [event for event in result.consume_events() if isinstance(event, SnowflakeLandingPart)]
    assert [part.record_count for part in parts] == [2, 2, 2, 2, 1, 1]
    assert [part.ordinal for part in parts] == [1, 1, 2, 2, 3, 3]
    assert cursor.closed and connection.closed


def test_landing_early_close_releases_bounded_lookahead_row(monkeypatch):
    cursor = _Cursor((_metadata_row(), *(_data_row("1234567893", "Cardiology") for _ in range(3))))
    connection, _ = _connect(monkeypatch, cursor)
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader", warehouse="warehouse")
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    with _open(adapter, _bundle_statement(), maximum_arrow_bytes=70) as result:
        part = next(result.consume_events())
        assert part.record_count == 2
        assert result._owner._pending_row is not None
    assert result._owner._pending_row is None
    assert cursor.closed and connection.closed


def test_shared_union_can_exceed_admitted_per_projection_arrow_limit(monkeypatch):
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, (_shared_row(key="a" * 30),))
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    with _open(adapter, connector.build_statement(request), maximum_arrow_bytes=76) as result:
        owner = result._owner
        schema = owner._source_schemas[0]
        _, row_values = owner._split_row(owner._pending_row, 0)
        assert (
            snowflake_python._result_row_variable_bytes(row_values, schema)
            + (snowflake_python._fixed_batch_bytes(schema, 1))
            > 76
        )
        parts = [event for event in result.consume_events() if isinstance(event, SnowflakeLandingPart)]
    assert len(parts) == 2 and all(part.arrow_byte_count <= 76 for part in parts)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("corruption", ["query", "schema", "metadata", "row_identity", "row_order", "scalar"])
def test_landing_reuses_source_receipt_validation_and_closes(monkeypatch, corruption):
    rows = [_shared_row(), _shared_row(key="b")]
    if corruption in {"row_identity", "row_order", "scalar"}:
        row_values = list(rows[1])
        row_values[{"row_identity": 2, "row_order": 1, "scalar": 4}[corruption]] = {
            "row_identity": "unknown",
            "row_order": 2,
            "scalar": object(),
        }[corruption]
        rows[1] = tuple(row_values)
    connector, request, adapter, cursor, connection = _runtime(monkeypatch, tuple(rows))
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    if corruption == "query":
        cursor.sfqid = " "
    elif corruption == "schema":
        cursor.description = ()
    elif corruption == "metadata":
        cursor._rows[1] = (*cursor._rows[1][:3], "different-token", *cursor._rows[1][4:])
    observed_events = []
    with pytest.raises(SnowflakeConnectorError):
        with _open(adapter, connector.build_statement(request)) as result:
            for event in result.consume_events():
                observed_events.append(event)
    assert not any(isinstance(event, SnowflakeLandingEOF) for event in observed_events)
    assert cursor.closed and connection.closed
