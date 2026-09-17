# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused checks for sealed custom-import source capture and decoding."""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
import threading
from dataclasses import replace
from decimal import Decimal
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import process.custom_import.capture as capture_module
import process.custom_import.parquet_pages as parquet_pages
from process.custom_import import (
    CaptureError,
    CaptureLimits,
    CustomImportDefinition,
    capture_stream,
    iter_records,
    load_json_definition,
    verify_capture,
)

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"
_CAPTURE_EXPORTS = (
    "CaptureError",
    "CaptureLimits",
    "CaptureManifest",
    "DecodedRecord",
    "SealedCapture",
    "capture_stream",
    "iter_records",
    "verify_capture",
)


def test_capture_exports_match_the_compatibility_contract():
    """The package and compatibility surface expose the same capture boundary."""

    from process import custom_import
    from process.custom_import import contracts

    for name in _CAPTURE_EXPORTS:
        assert name in contracts.__all__
        assert getattr(contracts, name) is getattr(custom_import, name)


def _stream(
    *,
    format_name: str = "csv",
    compression: str = "none",
    record_path: str | None = None,
):
    """Build one valid root stream using the existing synthetic definition fixture."""

    raw = load_json_definition((FIXTURES / "v1_valid.json").read_text())
    source = raw["streams"][0]
    source["format"] = format_name
    source["compression"] = compression
    if record_path is not None:
        source["record_path"] = record_path
    return CustomImportDefinition.from_mapping(raw).source_streams[0]


def _capture(payload: bytes, stream, *, limits: CaptureLimits | None = None):
    """Seal a small synthetic source payload with a stable external snapshot token."""

    return capture_stream(
        BytesIO(payload),
        stream,
        source_snapshot_token="snapshot-20260914",
        limits=limits or CaptureLimits(),
    )


class _ParquetMetadataColumn:
    """Minimal flat column metadata for capture metadata checks."""

    def __init__(self, file_path: str | None) -> None:
        self.file_path = file_path
        self.num_values = 1
        self.total_uncompressed_size = 1


class _ParquetMetadataRowGroup:
    """Minimal one-column row group for capture metadata checks."""

    num_rows = 1
    num_columns = 1

    def __init__(self, file_path: str | None) -> None:
        self.column_metadata = _ParquetMetadataColumn(file_path)

    def column(self, column_index: int) -> _ParquetMetadataColumn:
        """Return the only metadata column after its index is checked."""

        assert column_index == 0
        return self.column_metadata


class _ParquetMetadataFixture:
    """Minimal one-row file metadata with a configurable column reference."""

    num_rows = 1
    num_row_groups = 1
    num_columns = 1

    def __init__(self, file_path: str | None) -> None:
        self.row_group_metadata = _ParquetMetadataRowGroup(file_path)

    def row_group(self, group_index: int) -> _ParquetMetadataRowGroup:
        """Return the only metadata row group after its index is checked."""

        assert group_index == 0
        return self.row_group_metadata


class _ExcessParquetGroupMetadata:
    """Metadata that must fail before any oversized row group is inspected."""

    num_rows = 0
    num_row_groups = 4_097
    num_columns = 1

    @staticmethod
    def row_group(_group_index: int) -> None:
        """Fail if the metadata preflight inspects row groups after its limit."""

        raise AssertionError("row groups must not be inspected after the fanout limit")


def _parquet_payload(
    columns: dict[str, object],
    *,
    row_group_size: int | None = None,
    use_dictionary: bool = True,
    column_encoding: dict[str, str] | None = None,
    data_page_version: str = "1.0",
    data_page_size: int | None = None,
    write_page_index: bool = False,
) -> bytes:
    """Encode a sealed-test Parquet payload with page checksums enabled."""

    return _write_parquet_table(
        pa.table(columns),
        row_group_size=row_group_size,
        use_dictionary=use_dictionary,
        column_encoding=column_encoding,
        data_page_version=data_page_version,
        data_page_size=data_page_size,
        write_page_index=write_page_index,
    )


def _write_parquet_table(
    table: pa.Table,
    *,
    row_group_size: int | None = None,
    use_dictionary: bool = True,
    column_encoding: dict[str, str] | None = None,
    data_page_version: str = "1.0",
    data_page_size: int | None = None,
    write_page_index: bool = False,
) -> bytes:
    """Build a small in-memory Parquet file without paths or ambient source state."""

    payload = BytesIO()
    pq.write_table(
        table,
        payload,
        compression="zstd",
        row_group_size=row_group_size,
        use_dictionary=use_dictionary,
        column_encoding=column_encoding,
        data_page_version=data_page_version,
        data_page_size=data_page_size,
        write_page_checksum=True,
        write_page_index=write_page_index,
    )
    return payload.getvalue()


def _compact_unsigned(value: int) -> bytes:
    """Encode one nonnegative Compact varint for a focused raw-header fixture."""

    assert value >= 0
    encoded = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            encoded.append(byte | 0x80)
        else:
            encoded.append(byte)
            return bytes(encoded)


def _compact_i32(value: int) -> bytes:
    """Encode one Compact ZigZag i32 for a focused raw-header fixture."""

    assert -(2**31) <= value < 2**31
    return _compact_unsigned((value << 1) ^ (value >> 31))


def _first_parquet_page_offset(column) -> int:
    """Return the physical start of a generated column's first data or dictionary page."""

    return column.dictionary_page_offset if column.dictionary_page_offset is not None else column.data_page_offset


def _replace_generated_page_i32(
    payload: bytearray,
    *,
    page_offset: int,
    field_id: int,
    replacement: int,
) -> None:
    """Replace one of PyArrow's ordered PageHeader i32 fields without shifting its body."""

    position = page_offset
    for expected_field_id in range(1, field_id + 1):
        assert payload[position] == 0x15
        position += 1
        value_start = position
        while payload[position] & 0x80:
            position += 1
        position += 1
        if expected_field_id == field_id:
            encoded = _compact_i32(replacement)
            assert len(encoded) == position - value_start
            payload[value_start:position] = encoded
            return
    raise AssertionError("PageHeader field was not found")


def _replace_generated_dictionary_num_values(
    payload: bytearray,
    *,
    page_offset: int,
    original: int,
    replacement: int,
) -> None:
    """Replace the ordered dictionary-entry count in a generated PageHeader without shifting bytes."""

    marker = b"\x15" + _compact_i32(original) + b"\x15" + _compact_i32(0)
    value_start = payload.index(marker, page_offset) + 1
    value_end = value_start + len(_compact_i32(original))
    encoded = _compact_i32(replacement)
    assert len(encoded) == value_end - value_start
    payload[value_start:value_end] = encoded


def _metadata_with_column_overrides(metadata, overrides):
    """Expose generated metadata with only the raw-page facts overridden for a direct preflight check."""

    class Column:
        def __init__(self, column, row_group_index: int, column_index: int) -> None:
            self._column = column
            self._row_group_index = row_group_index
            self._column_index = column_index

        def __getattr__(self, name: str):
            return overrides.get(
                (self._row_group_index, self._column_index, name),
                getattr(self._column, name),
            )

    class RowGroup:
        def __init__(self, row_group, row_group_index: int) -> None:
            self._row_group = row_group
            self._row_group_index = row_group_index

        @property
        def num_rows(self):
            return self._row_group.num_rows

        @property
        def num_columns(self):
            return self._row_group.num_columns

        def column(self, column_index: int):
            return Column(
                self._row_group.column(column_index),
                self._row_group_index,
                column_index,
            )

    class FileMetadata:
        num_row_groups = metadata.num_row_groups
        num_columns = metadata.num_columns

        @staticmethod
        def row_group(row_group_index: int):
            return RowGroup(metadata.row_group(row_group_index), row_group_index)

    return FileMetadata()


def test_capture_seals_and_replays_delimited_source_records():
    """A CSV capture retains exact bytes, hashes, snapshot provenance, and labels."""

    stream = _stream()
    payload = b"Provider ID,Provider Name\n1234567893,Synthetic Provider\n"
    capture = _capture(payload, stream)

    assert capture.payload == payload
    assert capture.manifest.stream_id == "providers"
    assert capture.manifest.source_snapshot_token == "snapshot-20260914"
    assert capture.manifest.compressed_bytes == len(payload)
    assert capture.manifest.decoded_bytes == len(payload)
    records = list(iter_records(capture, stream))
    assert records[0].ordinal == 1
    assert records[0].values == {
        "Provider ID": "1234567893",
        "Provider Name": "Synthetic Provider",
    }
    with pytest.raises(TypeError):
        records[0].values["Provider ID"] = "different"


def test_capture_supports_gzip_replay_and_enforces_transport_limits():
    """Gzip is replayed from its sealed bytes and both byte limits fail closed."""

    stream = _stream(compression="gzip")
    payload = gzip.compress(b"Provider ID,Provider Name\n1234567893,Synthetic Provider\n")
    capture = _capture(payload, stream)
    assert next(iter(iter_records(capture, stream))).values["Provider ID"] == "1234567893"

    with pytest.raises(CaptureError, match="compressed-byte"):
        _capture(payload, stream, limits=CaptureLimits(maximum_compressed_bytes=1))
    with pytest.raises(CaptureError, match="decoded-byte"):
        _capture(
            payload,
            stream,
            limits=CaptureLimits(maximum_decoded_bytes=32, maximum_record_bytes=16),
        )


def test_capture_normalizes_corrupt_gzip_deflate_bodies():
    """A valid gzip header with a corrupt deflate body remains a capture failure."""

    stream = _stream(compression="gzip")
    payload = gzip.compress(b"Provider ID,Provider Name\n1234567893,Synthetic Provider\n" * 2000)
    corrupt_payload = payload[:10] + bytes(byte ^ 0xFF for byte in payload[10:-8]) + payload[-8:]

    with pytest.raises(CaptureError, match="source stream compression is invalid"):
        _capture(corrupt_payload, stream)


def test_capture_detects_tampered_payload_or_manifest():
    """A persisted replay cannot substitute bytes or alter its source association."""

    stream = _stream()
    capture = _capture(b"Provider ID\n1234567893\n", stream)

    with pytest.raises(CaptureError, match="digest"):
        verify_capture(replace(capture, payload=b"Provider ID\n0000000000\n"), stream)
    with pytest.raises(CaptureError, match="does not belong"):
        verify_capture(capture, _stream(format_name="ndjson"))

    xml_stream = _stream(format_name="xml", record_path="provider")
    xml_capture = _capture(b"<providers><provider /></providers>", xml_stream)
    with pytest.raises(CaptureError, match="does not belong"):
        verify_capture(
            xml_capture,
            _stream(format_name="xml", record_path="different_provider"),
        )


def test_verify_capture_reapplies_stricter_replay_byte_limits():
    """A retained payload cannot bypass a smaller later replay limit."""

    stream = _stream()
    payload = b"Provider ID\n1234567893\n"
    capture = _capture(payload, stream)

    with pytest.raises(CaptureError, match="compressed byte count is invalid"):
        verify_capture(
            capture,
            stream,
            limits=CaptureLimits(maximum_compressed_bytes=len(payload) - 1),
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("compressed_bytes", True),
        ("compressed_bytes", 1.0),
        ("decoded_bytes", True),
        ("decoded_bytes", 1.0),
    ),
)
def test_verify_capture_rejects_non_integer_manifest_sizes(field_name, invalid_value):
    """Malformed manifest byte counts cannot become implicit replay limits."""

    stream = _stream()
    capture = _capture(b"Provider ID\n1234567893\n", stream)
    malformed = replace(
        capture,
        manifest=replace(capture.manifest, **{field_name: invalid_value}),
    )

    with pytest.raises(CaptureError, match="byte count is invalid"):
        verify_capture(malformed, stream)


def test_verify_capture_rejects_non_capture_objects_and_manifests():
    """The public replay boundary always reports malformed capture types safely."""

    stream = _stream()
    with pytest.raises(CaptureError, match="declared capture type"):
        verify_capture(object(), stream)

    capture = _capture(b"Provider ID\n1234567893\n", stream)
    malformed = replace(capture, manifest=object())
    with pytest.raises(CaptureError, match="declared manifest type"):
        verify_capture(malformed, stream)


@pytest.mark.parametrize(
    "payload, message",
    [
        (b"Provider ID,Provider ID\n1234567893,duplicate\n", "duplicate labels"),
        (b"Provider ID\n1234567893,unexpected\n", "field count"),
        (b"Provider ID,Provider Name\n1234567893\n", "field count"),
    ],
)
def test_delimited_decoder_rejects_ambiguous_source_columns(payload, message):
    """Delimited source shape cannot silently discard duplicated or extra values."""

    with pytest.raises(CaptureError, match=message):
        list(iter_records(_capture(payload, _stream()), _stream()))


def test_delimited_decoder_preserves_a_valid_overflow_named_source_column():
    """A source label that resembles an implementation sentinel remains ordinary data."""

    stream = _stream()
    payload = b"__overflow__,Provider ID\nretained,1234567893\n"
    records = list(iter_records(_capture(payload, stream), stream))
    assert records[0].values["__overflow__"] == "retained"


@pytest.mark.parametrize(
    ("source_payload", "supplied_line_sizes"),
    (
        (b"a," * 100_000 + b"a\n", []),
        (b"a\n" + b"x," * 100_000 + b"x\n", [2]),
    ),
)
def test_delimited_decoder_bounds_headers_and_many_short_fields_before_csv_parsing(
    monkeypatch,
    source_payload,
    supplied_line_sizes,
):
    """Oversized logical rows never reach ``csv.reader`` as complete strings."""

    stream = _stream()
    limits = CaptureLimits(
        maximum_compressed_bytes=len(source_payload),
        maximum_decoded_bytes=len(source_payload),
        maximum_record_bytes=16,
    )
    observed_line_sizes = []
    original_reader = capture_module.csv.reader

    def tracking_reader(lines, *args, **kwargs):
        def tracked_lines():
            for line in lines:
                observed_line_sizes.append(len(line.encode("utf-8")))
                yield line

        return original_reader(tracked_lines(), *args, **kwargs)

    monkeypatch.setattr(capture_module.csv, "reader", tracking_reader)
    sealed = _capture(source_payload, stream, limits=limits)

    with pytest.raises(CaptureError, match="delimited source record exceeds the byte limit"):
        list(iter_records(sealed, stream, limits=limits))

    assert observed_line_sizes == supplied_line_sizes


def test_delimited_decoder_accumulates_multiline_record_bytes_before_csv_parsing(monkeypatch):
    """Several bounded physical lines share one logical-record byte budget."""

    stream = _stream()
    payload = b'a,b\n1,"1234567890\n12345678901234567890"\n'
    limits = CaptureLimits(maximum_record_bytes=24, maximum_decoded_bytes=len(payload))
    observed_lines = []
    original_reader = capture_module.csv.reader

    def tracking_reader(lines, *args, **kwargs):
        def tracked_lines():
            for line in lines:
                observed_lines.append(line)
                yield line

        return original_reader(tracked_lines(), *args, **kwargs)

    monkeypatch.setattr(capture_module.csv, "reader", tracking_reader)
    sealed = _capture(payload, stream, limits=limits)

    with pytest.raises(CaptureError, match="delimited source record exceeds the byte limit"):
        list(iter_records(sealed, stream, limits=limits))

    assert observed_lines == ["a,b\n", '1,"1234567890\n']


def test_delimited_decoder_preserves_bounded_multiline_values():
    """The pre-parser byte guard retains standard multiline CSV behavior."""

    stream = _stream()
    payload = b'a,b\n1,"first line\nsecond line"\n'
    limits = CaptureLimits(maximum_record_bytes=64, maximum_decoded_bytes=64)

    records = list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))

    assert records[0].values == {"a": "1", "b": "first line\nsecond line"}


@pytest.mark.parametrize(
    ("format_name", "payload"),
    (
        ("csv", b'Provider ID,Provider Name\n1234567893,"unterminated\n'),
        ("tsv", b'Provider ID\tProvider Name\n1234567893\t"unterminated\n'),
    ),
)
def test_delimited_decoder_rejects_unterminated_quoted_values(format_name, payload):
    """Delimited sources cannot silently recover malformed quoted source cells."""

    stream = _stream(format_name=format_name)

    with pytest.raises(CaptureError, match="delimited source payload is invalid"):
        list(iter_records(_capture(payload, stream), stream))


def test_delimited_iterators_release_the_global_csv_limit_between_records():
    """Interleaved CSV iterators cannot retain the process-global parser limit."""

    stream = _stream()
    first = iter_records(_capture(b"Provider ID\n1234567893\n", stream), stream)
    second = iter_records(_capture(b"Provider ID\n1003000126\n", stream), stream)
    completed = threading.Event()
    second_records = []
    second_errors = []

    def advance_second_iterator():
        try:
            second_records.append(next(second))
        except CaptureError as exc:
            second_errors.append(exc)
        finally:
            completed.set()

    worker = threading.Thread(target=advance_second_iterator, daemon=True)
    has_started_thread = False
    try:
        assert next(first).values["Provider ID"] == "1234567893"
        worker.start()
        has_started_thread = True
        assert completed.wait(timeout=1)
        worker.join(timeout=1)
        assert not worker.is_alive()
        assert second_errors == []
        assert second_records[0].values["Provider ID"] == "1003000126"
    finally:
        first.close()
        if has_started_thread:
            worker.join(timeout=1)
        if not worker.is_alive():
            second.close()


def test_delimited_decoder_rejects_nonprintable_unicode_source_labels():
    """Valid UTF-8 controls cannot enter later exact source-label matching."""

    stream = _stream()
    payload = b"Provider\xc2\x85ID,Provider Name\n1234567893,Synthetic Provider\n"

    with pytest.raises(CaptureError, match="printable text"):
        list(iter_records(_capture(payload, stream), stream))


def test_json_decoder_is_incremental_strict_and_preserves_decimal_values():
    """Top-level JSON arrays stream flat objects without losing decimal precision."""

    stream = _stream(format_name="json")
    payload = b'[{"npi":"1234567893","amount":12.50},{"npi":"1003000126","amount":0.01}]'

    records = list(iter_records(_capture(payload, stream), stream))
    assert [record.values["amount"] for record in records] == [
        Decimal("12.50"),
        Decimal("0.01"),
    ]
    with pytest.raises(CaptureError, match="top-level array"):
        list(iter_records(_capture(b'{"npi":"1234567893"}', stream), stream))
    with pytest.raises(CaptureError, match="duplicate JSON"):
        list(iter_records(_capture(b'[{"npi":"one","npi":"two"}]', stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(b'[{"npi":"\\ud800"}]', stream), stream))


@pytest.mark.parametrize(
    ("payload", "message", "limits"),
    (
        (
            b'[{"value":"' + b"x" * 128 + b'"}]',
            "byte limit",
            CaptureLimits(maximum_record_bytes=32, maximum_decoded_bytes=512),
        ),
        (
            b'[{"value":{"nested":1}}]',
            "flat scalars",
            CaptureLimits(maximum_record_bytes=64, maximum_decoded_bytes=256),
        ),
        (
            b'[{"one":1,"two":2}]',
            "field limit",
            CaptureLimits(
                maximum_fields_per_record=1,
                maximum_record_bytes=64,
                maximum_decoded_bytes=256,
            ),
        ),
    ),
    ids=("oversized", "nested", "too-many-fields"),
)
def test_json_framer_rejects_before_record_materialization(monkeypatch, payload, message, limits):
    """Raw JSON bounds fail before a parser can allocate a record value."""

    from process.custom_import import capture

    stream = _stream(format_name="json")
    monkeypatch.setattr(
        capture.json,
        "loads",
        lambda *_args, **_kwargs: pytest.fail("bounded JSON rejected too late"),
    )
    with pytest.raises(CaptureError, match=message):
        list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))


def test_json_decoder_errors_do_not_echo_duplicate_source_labels():
    """Malformed source labels remain out of capture errors and downstream logs."""

    stream = _stream(format_name="json")
    source_label = "Unreported Source Label"
    payload = b'[{"' + source_label.encode("utf-8") + b'":"one","' + source_label.encode("utf-8") + b'":"two"}]'

    with pytest.raises(CaptureError) as error:
        list(iter_records(_capture(payload, stream), stream))

    assert source_label not in str(error.value)


def test_json_oversized_integer_fails_closed_without_terminating_python():
    """JSON integer conversion must remain a normal capture failure on Python 3.14."""

    digit_limit = sys.get_int_max_str_digits()
    if digit_limit == 0 or digit_limit > 100_000:
        pytest.skip("the interpreter has no practical integer digit cap")
    subprocess_script = _oversized_json_subprocess_script(digit_limit)
    subprocess_result = subprocess.run(
        [sys.executable, "-c", subprocess_script],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )
    assert subprocess_result.returncode == 0, subprocess_result.stderr


def _oversized_json_subprocess_script(digit_limit: int) -> str:
    """Build an isolated parser invocation without embedding an oversized literal."""

    return "\n".join(
        (
            "from io import BytesIO",
            "from pathlib import Path",
            "from process.custom_import import CaptureError, CustomImportDefinition",
            "from process.custom_import import capture_stream, iter_records, load_json_definition",
            "raw = load_json_definition(Path('tests/fixtures/custom_import/v1_valid.json').read_text())",
            "raw['streams'][0]['format'] = 'json'",
            "stream = CustomImportDefinition.from_mapping(raw).source_streams[0]",
            f"payload = b'[{{\"number\":' + b'9' * {digit_limit + 1} + b'}}]'",
            "capture = capture_stream(BytesIO(payload), stream, source_snapshot_token='snapshot-1')",
            "try:",
            "    list(iter_records(capture, stream))",
            "except CaptureError:",
            "    pass",
            "else:",
            "    raise SystemExit('oversized integer was accepted')",
        )
    )


def test_ndjson_decoder_rejects_nested_values_and_record_overruns():
    """NDJSON admits only flat scalar objects within the declared per-record bound."""

    stream = _stream(format_name="ndjson")
    payload = b'\n{"npi":"1234567893","amount":12.50}\n'
    records = list(iter_records(_capture(payload, stream), stream))
    assert records[0].values["amount"] == Decimal("12.50")

    with pytest.raises(CaptureError, match="nested"):
        list(iter_records(_capture(b'{"npi":["1234567893"]}\n', stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(b'{"npi":"\\ud800"}\n', stream), stream))
    tiny_limits = CaptureLimits(maximum_record_bytes=16, maximum_decoded_bytes=64)
    with pytest.raises(CaptureError, match="record exceeds"):
        list(
            iter_records(
                _capture(b'{"npi":"1234567893"}\n', stream, limits=tiny_limits),
                stream,
                limits=tiny_limits,
            )
        )


@pytest.mark.parametrize(
    "format_name, payload",
    (
        ("json", b'[{"value":1e9999999999999999999999999999999}]'),
        ("ndjson", b'{"value":1e9999999999999999999999999999999}\n'),
    ),
)
def test_json_decoders_normalize_extreme_decimal_exponents(format_name, payload):
    """Decimal conversion failures remain inside the public capture error contract."""

    stream = _stream(format_name=format_name)
    limits = CaptureLimits(maximum_record_bytes=256, maximum_decoded_bytes=512)
    with pytest.raises(CaptureError, match="invalid"):
        list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))


def test_xml_decoder_allows_flat_direct_child_records_and_clears_completed_roots():
    """XML records are streamed as direct root children without retaining prior children."""

    stream = _stream(format_name="xml", record_path="provider")
    xml_payload = (
        b"<providers>\n  <provider>\n    <npi>1234567893</npi>\n    <name>Synthetic</name>\n  </provider>\n</providers>"
    )
    xml_records = list(iter_records(_capture(xml_payload, stream), stream))
    assert xml_records[0].values == {"npi": "1234567893", "name": "Synthetic"}
    repeated_xml_payload = (
        b"<providers><provider><npi>1234567893</npi></provider><provider><npi>1003000126</npi></provider></providers>"
    )
    assert [record.values["npi"] for record in iter_records(_capture(repeated_xml_payload, stream), stream)] == [
        "1234567893",
        "1003000126",
    ]


@pytest.mark.parametrize(
    "payload",
    (
        b"<providers><provider>prefix<npi>1234567893</npi></provider></providers>",
        b"<providers><provider><npi>1234567893</npi>tail<name>Synthetic</name></provider></providers>",
    ),
    ids=("direct-prefix", "child-tail"),
)
def test_xml_decoder_rejects_mixed_record_prefix_and_child_tail_text(payload):
    """Only XML whitespace may surround flat scalar child elements in a record."""

    stream = _stream(format_name="xml", record_path="provider")

    with pytest.raises(CaptureError, match="non-XML-whitespace text"):
        list(iter_records(_capture(payload, stream), stream))


def test_xml_decoder_rejects_entities_and_non_utf8_encodings():
    """XML declaration preflight rejects entities across plain and gzip captures."""

    stream = _stream(format_name="xml", record_path="provider")
    entity_payload = b'<!DOCTYPE providers [<!ENTITY value "blocked">]><providers></providers>'
    with pytest.raises(CaptureError, match="entity declarations"):
        list(iter_records(_capture(entity_payload, stream), stream))
    split_limits = CaptureLimits(read_chunk_bytes=3)
    with pytest.raises(CaptureError, match="entity declarations"):
        list(
            iter_records(
                _capture(entity_payload, stream, limits=split_limits),
                stream,
                limits=split_limits,
            )
        )
    gzip_stream = _stream(format_name="xml", compression="gzip", record_path="provider")
    with pytest.raises(CaptureError, match="entity declarations"):
        list(iter_records(_capture(gzip.compress(entity_payload), gzip_stream), gzip_stream))
    encoded_entity_payload = entity_payload.decode().encode("utf-16")
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(iter_records(_capture(encoded_entity_payload, stream), stream))
    with pytest.raises(CaptureError, match="valid UTF-8"):
        list(
            iter_records(
                _capture(gzip.compress(encoded_entity_payload), gzip_stream),
                gzip_stream,
            )
        )


def test_xml_decoder_rejects_nested_or_unexpected_record_shapes():
    """Only flat records named by the direct-child selector can enter a stream."""

    stream = _stream(format_name="xml", record_path="provider")
    nested_payload = b"<providers><provider><npi><value>123</value></npi></provider></providers>"
    with pytest.raises(CaptureError, match="flat scalar"):
        list(iter_records(_capture(nested_payload, stream), stream))
    with pytest.raises(CaptureError, match="unexpected root child"):
        list(iter_records(_capture(b"<providers><metadata /></providers>", stream), stream))


@pytest.mark.parametrize(
    "payload",
    (
        b"<providers><!--" + b"x" * 5000,
        b'<providers><provider attr="' + b"x" * 5000,
        b"<providers><?pi " + b"x" * 5000,
    ),
    ids=("comment", "attribute", "processing-instruction"),
)
def test_xml_decoder_bounds_unfinished_markup_before_eof(payload):
    """Expat unfinished-token buffering cannot grow beyond the parser budget."""

    stream = _stream(format_name="xml", record_path="provider")
    limits = CaptureLimits(
        maximum_record_bytes=128,
        maximum_decoded_bytes=8192,
        read_chunk_bytes=16,
    )
    with pytest.raises(CaptureError, match="unfinished token"):
        list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))


@pytest.mark.parametrize("read_chunk_bytes", (1, 3, 16))
def test_xml_decoder_accepts_exact_comment_budget_after_bounded_reparse(read_chunk_bytes):
    """A complete comment at the parser budget remains valid for tiny feeds."""

    stream = _stream(format_name="xml", record_path="provider")
    payload = b"<providers><!--" + b"x" * 4080 + b"--><provider /></providers>"
    limits = CaptureLimits(
        maximum_record_bytes=128,
        maximum_decoded_bytes=8192,
        read_chunk_bytes=read_chunk_bytes,
    )
    records = list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))
    assert records[0].values == {}


def test_xml_decoder_bounds_document_name_vocabulary():
    """Distinct XML names are bounded across records, not only within one record."""

    stream = _stream(format_name="xml", record_path="provider")
    payload = (
        b"<providers>"
        + b"".join(f"<provider><field{index}>x</field{index}></provider>".encode() for index in range(10))
        + b"</providers>"
    )
    limits = CaptureLimits(
        maximum_fields_per_record=3,
        maximum_decoded_bytes=2048,
        maximum_record_bytes=128,
    )
    with pytest.raises(CaptureError, match="distinct-name"):
        list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))


def test_xml_decoder_rejects_cumulative_record_size_before_completion(monkeypatch):
    """Many individually valid fields cannot accumulate beyond one record bound."""

    from process.custom_import import capture

    stream = _stream(format_name="xml", record_path="provider")
    payload = b"<providers><provider><a>1234</a><b>1234</b></provider></providers>"
    limits = CaptureLimits(maximum_record_bytes=16, maximum_decoded_bytes=256)
    monkeypatch.setattr(
        capture,
        "_decoded_record",
        lambda *_args, **_kwargs: pytest.fail("record was materialized too late"),
    )
    with pytest.raises(CaptureError, match="byte limit"):
        list(iter_records(_capture(payload, stream, limits=limits), stream, limits=limits))


def test_decoder_limits_records_and_parquet_metadata_limits():
    """Every decoder, including Parquet metadata, honors the declared record cap."""

    json_stream = _stream(format_name="json")
    limits = CaptureLimits(maximum_records=1)
    payload = json.dumps([{"npi": "1234567893"}, {"npi": "1003000126"}]).encode()
    with pytest.raises(CaptureError, match="record limit"):
        list(
            iter_records(
                _capture(payload, json_stream, limits=limits),
                json_stream,
                limits=limits,
            )
        )

    parquet_stream = _stream(format_name="parquet")
    parquet_payload = _parquet_payload({"npi": ["1234567893", "1003000126"]})
    with pytest.raises(CaptureError, match="record limit"):
        list(
            iter_records(
                _capture(parquet_payload, parquet_stream, limits=limits),
                parquet_stream,
                limits=limits,
            )
        )


def test_parquet_decoder_streams_flat_scalar_rows_across_batches_and_row_groups():
    """Parquet retains exact source labels and scalar values without table materialization."""

    stream = _stream(format_name="parquet")
    payload = _parquet_payload(
        {
            "Provider ID": [f"{value:010d}" for value in range(257)],
            "Amount": pa.array([Decimal("12.50")] * 257, type=pa.decimal128(10, 2)),
            "Active": [value % 2 == 0 for value in range(257)],
        },
        row_group_size=64,
    )

    records = list(iter_records(_capture(payload, stream), stream))

    assert [records[0].ordinal, records[-1].ordinal] == [1, 257]
    assert records[0].values == {
        "Provider ID": "0000000000",
        "Amount": Decimal("12.50"),
        "Active": True,
    }
    assert records[-1].values["Provider ID"] == "0000000256"
    assert records[-1].values["Active"] is True


def test_parquet_dictionary_batches_use_one_row_and_preflight_the_working_budget(monkeypatch):
    """Dictionary output retains one-row streaming and rejects an over-budget working estimate."""

    stream = _stream(format_name="parquet")
    parquet_bytes = _parquet_payload({"source_value": ["x" * 16_384] * 256})
    metadata = pq.ParquetFile(BytesIO(parquet_bytes)).metadata
    assert metadata is not None
    column = metadata.row_group(0).column(0)
    assert column.has_dictionary_page is True

    restrictive_limits = CaptureLimits(
        maximum_decoded_bytes=32 * 1024,
        maximum_record_bytes=20 * 1024,
        maximum_records=256,
    )
    assert len(parquet_bytes) < restrictive_limits.maximum_decoded_bytes
    assert column.total_uncompressed_size < restrictive_limits.maximum_decoded_bytes

    requested_batch_sizes: list[int] = []
    materialized_batches: list[tuple[int, int]] = []
    original_iter_batches = capture_module.pq.ParquetFile.iter_batches

    def observing_iter_batches(parquet_file, *args, **kwargs):
        requested_batch_sizes.append(kwargs["batch_size"])
        for record_batch in original_iter_batches(parquet_file, *args, **kwargs):
            materialized_batches.append((record_batch.num_rows, record_batch.nbytes))
            yield record_batch

    monkeypatch.setattr(capture_module.pq.ParquetFile, "iter_batches", observing_iter_batches)

    permissive_limits = CaptureLimits(
        maximum_decoded_bytes=4 * 1024 * 1024 + 128 * 1024,
        maximum_record_bytes=20 * 1024,
        maximum_records=256,
    )
    decoded_records = list(
        iter_records(
            _capture(parquet_bytes, stream, limits=permissive_limits),
            stream,
            limits=permissive_limits,
        )
    )
    assert len(decoded_records) == 256

    with pytest.raises(CaptureError, match="decoded-byte"):
        list(
            iter_records(
                _capture(parquet_bytes, stream, limits=restrictive_limits),
                stream,
                limits=restrictive_limits,
            )
        )

    assert requested_batch_sizes == [1]
    assert materialized_batches
    assert all(batch_rows == 1 for batch_rows, _batch_bytes in materialized_batches)
    assert all(
        batch_bytes <= min(permissive_limits.maximum_decoded_bytes, permissive_limits.maximum_record_bytes)
        for _batch_rows, batch_bytes in materialized_batches
    )


def test_parquet_delta_byte_array_rejects_before_native_batches(monkeypatch):
    """Delta byte-array values are rejected from their raw data-page header before iteration."""

    stream = _stream(format_name="parquet")
    parquet_bytes = _parquet_payload(
        {"source_value": ["x" * 16_384] * 256},
        use_dictionary=False,
        column_encoding={"source_value": "DELTA_BYTE_ARRAY"},
    )
    metadata = pq.ParquetFile(BytesIO(parquet_bytes)).metadata
    assert metadata is not None
    column = metadata.row_group(0).column(0)
    assert column.has_dictionary_page is False
    assert "DELTA_BYTE_ARRAY" in column.encodings

    limits = CaptureLimits(
        maximum_decoded_bytes=128 * 1024,
        maximum_record_bytes=20 * 1024,
        maximum_records=256,
    )
    assert len(parquet_bytes) < limits.maximum_decoded_bytes
    assert column.total_uncompressed_size < limits.maximum_decoded_bytes
    monkeypatch.setattr(
        capture_module.pq.ParquetFile,
        "iter_batches",
        lambda *_args, **_kwargs: pytest.fail("raw page validation must run before batches"),
    )

    with pytest.raises(CaptureError, match="unsupported page encoding"):
        list(iter_records(_capture(parquet_bytes, stream, limits=limits), stream, limits=limits))


def test_parquet_page_header_total_mismatch_rejects_before_native_batches(monkeypatch):
    """A raw page size larger than its footer total is rejected before batches are requested."""

    stream = _stream(format_name="parquet")
    payload = bytearray(_parquet_payload({"source_value": ["x" * 16_384] * 256}))
    metadata = pq.ParquetFile(BytesIO(payload)).metadata
    assert metadata is not None
    column = metadata.row_group(0).column(0)
    assert column.total_uncompressed_size < 128 * 1024
    _replace_generated_page_i32(
        payload,
        page_offset=_first_parquet_page_offset(column),
        field_id=2,
        replacement=20_000,
    )
    limits = CaptureLimits(
        maximum_decoded_bytes=128 * 1024,
        maximum_record_bytes=20 * 1024,
        maximum_records=256,
    )
    monkeypatch.setattr(
        capture_module.pq.ParquetFile,
        "iter_batches",
        lambda *_args, **_kwargs: pytest.fail("raw page validation must run before batches"),
    )

    with pytest.raises(CaptureError, match="page metadata"):
        list(iter_records(_capture(bytes(payload), stream, limits=limits), stream, limits=limits))


def test_parquet_dictionary_entry_count_is_budgeted_before_native_batches(monkeypatch):
    """A dictionary entry count contributes to the preflight working estimate before batches."""

    stream = _stream(format_name="parquet")
    payload = bytearray(_parquet_payload({"source_value": [f"entry-{index:03d}" for index in range(100)]}))
    metadata = pq.ParquetFile(BytesIO(payload)).metadata
    assert metadata is not None
    column = metadata.row_group(0).column(0)
    assert column.has_dictionary_page is True
    _replace_generated_dictionary_num_values(
        payload,
        page_offset=_first_parquet_page_offset(column),
        original=100,
        replacement=8_191,
    )
    limits = CaptureLimits(maximum_decoded_bytes=128 * 1024, maximum_record_bytes=1024)
    monkeypatch.setattr(
        capture_module.pq.ParquetFile,
        "iter_batches",
        lambda *_args, **_kwargs: pytest.fail("raw page validation must run before batches"),
    )

    with pytest.raises(CaptureError, match="decoded-byte"):
        list(iter_records(_capture(bytes(payload), stream, limits=limits), stream, limits=limits))


def test_parquet_page_header_rejects_oversized_and_malformed_compact_values():
    """The bounded raw-header reader rejects oversized and unterminated Compact values."""

    oversized_binary = b"x" * (64 * 1024)
    oversized_header = (
        b"\x15\x00\x15\x00\x15\x00\x2c"
        b"\x15\x00\x15\x00\x15\x06\x15\x06\x00"
        b"\x48" + _compact_unsigned(len(oversized_binary)) + oversized_binary + b"\x00"
    )
    with pytest.raises(parquet_pages.ParquetPageError, match="invalid page header"):
        parquet_pages._read_parquet_page_header(
            oversized_header,
            0,
            len(oversized_header),
            64 * 1024,
        )
    with pytest.raises(parquet_pages.ParquetPageError, match="invalid page header"):
        parquet_pages._read_parquet_page_header(b"\x15\x80\x80\x80\x80\x80", 0, 6, 64)


def test_parquet_page_header_bounds_unknown_compact_collections_and_dictionary_encoding():
    """Unknown Compact collections stay bounded, and dictionary pages retain PLAIN values only."""

    data_page_prefix = b"\x15\x00\x15\x00\x15\x00\x2c\x15\x00\x15\x00\x15\x06\x15\x06\x00"
    collection_header = data_page_prefix + b"\x49\x21\x01\x02\x00"
    header = parquet_pages._read_parquet_page_header(collection_header, 0, len(collection_header), 64)
    assert header.page_type == 0

    nested_collections = data_page_prefix + b"\x49" + b"\x19" * 16 + b"\x13\x00\x00"
    with pytest.raises(parquet_pages.ParquetPageError, match="invalid page header"):
        parquet_pages._read_parquet_page_header(nested_collections, 0, len(nested_collections), 64)

    legacy_dictionary_header = b"\x15\x04\x15\x00\x15\x00\x4c\x15\x00\x15\x04\x00\x00"
    with pytest.raises(parquet_pages.ParquetPageError, match="invalid page header"):
        parquet_pages._read_parquet_page_header(
            legacy_dictionary_header,
            0,
            len(legacy_dictionary_header),
            64,
        )

    bit_packed_level_header = b"\x15\x00\x15\x00\x15\x00\x2c\x15\x00\x15\x00\x15\x08\x15\x06\x00\x00"
    with pytest.raises(parquet_pages.ParquetPageError, match="invalid page header"):
        parquet_pages._read_parquet_page_header(
            bit_packed_level_header,
            0,
            len(bit_packed_level_header),
            64,
        )


def test_parquet_page_layout_rejects_overlapping_offsets_and_inconsistent_counts():
    """Raw chunk intervals and page counts must agree with the generated footer metadata."""

    parquet_bytes = _parquet_payload(
        {"first": ["one", "two"], "second": ["three", "four"]},
        use_dictionary=False,
    )
    parquet_file = pq.ParquetFile(BytesIO(parquet_bytes))
    metadata = parquet_file.metadata
    assert metadata is not None
    schema = parquet_file.schema_arrow
    first = metadata.row_group(0).column(0)
    second = metadata.row_group(0).column(1)

    overlapping = _metadata_with_column_overrides(
        metadata,
        {(0, 1, "data_page_offset"): _first_parquet_page_offset(first)},
    )
    with pytest.raises(parquet_pages.ParquetPageError, match="overlapping"):
        parquet_pages.validate_page_layout(
            parquet_bytes,
            overlapping,
            schema,
            maximum_decoded_bytes=CaptureLimits().maximum_decoded_bytes,
        )

    shifted = _metadata_with_column_overrides(
        metadata,
        {(0, 0, "data_page_offset"): first.data_page_offset + 1},
    )
    with pytest.raises(parquet_pages.ParquetPageError, match="page"):
        parquet_pages.validate_page_layout(
            parquet_bytes,
            shifted,
            schema,
            maximum_decoded_bytes=CaptureLimits().maximum_decoded_bytes,
        )

    inconsistent_count = _metadata_with_column_overrides(
        metadata,
        {(0, 1, "num_values"): second.num_values + 1},
    )
    with pytest.raises(parquet_pages.ParquetPageError, match="inconsistent row metadata"):
        parquet_pages.validate_page_layout(
            parquet_bytes,
            inconsistent_count,
            schema,
            maximum_decoded_bytes=CaptureLimits().maximum_decoded_bytes,
        )


@pytest.mark.parametrize(
    ("use_dictionary", "data_page_version"),
    (
        pytest.param(False, "1.0", id="plain-v1"),
        pytest.param(True, "1.0", id="dictionary-v1"),
        pytest.param(False, "2.0", id="plain-v2"),
        pytest.param(True, "2.0", id="dictionary-v2"),
    ),
)
def test_parquet_page_preflight_accepts_default_scalar_layouts_across_row_groups(
    use_dictionary,
    data_page_version,
):
    """Default PyArrow scalar pages remain readable across V1, V2, and row-group boundaries."""

    stream = _stream(format_name="parquet")
    scalar_table = pa.table(
        {
            "source_value": [f"value-{index}" for index in range(8)],
            "count": list(range(8)),
            "active": [index % 2 == 0 for index in range(8)],
            "amount": pa.array([Decimal("12.50")] * 8, type=pa.decimal128(10, 2)),
        }
    )
    parquet_bytes = _write_parquet_table(
        scalar_table,
        row_group_size=2,
        use_dictionary=use_dictionary,
        data_page_version=data_page_version,
        data_page_size=128,
        write_page_index=True,
    )
    decoded_records = list(iter_records(_capture(parquet_bytes, stream), stream))

    assert len(decoded_records) == 8
    assert decoded_records[0].values == {
        "source_value": "value-0",
        "count": 0,
        "active": True,
        "amount": Decimal("12.50"),
    }
    assert decoded_records[-1].values["source_value"] == "value-7"


def test_parquet_page_preflight_accepts_zero_entry_dictionary_and_empty_row_group():
    """All-null dictionary pages and an empty row group retain normal streaming semantics."""

    stream = _stream(format_name="parquet")
    all_null_payload = _write_parquet_table(
        pa.table({"source_value": pa.array([None, None, None], type=pa.string())}),
    )
    all_null_metadata = pq.ParquetFile(BytesIO(all_null_payload)).metadata
    assert all_null_metadata is not None
    assert all_null_metadata.row_group(0).column(0).has_dictionary_page is True
    assert [record.values["source_value"] for record in iter_records(_capture(all_null_payload, stream), stream)] == [
        None,
        None,
        None,
    ]

    empty_payload = _write_parquet_table(pa.table({"source_value": pa.array([], type=pa.string())}))
    assert list(iter_records(_capture(empty_payload, stream), stream)) == []


def test_parquet_page_preflight_accepts_multiple_plain_data_pages():
    """The raw page walk accepts more than one plain page in a single row group."""

    stream = _stream(format_name="parquet")
    payload = _parquet_payload(
        {"source_value": [f"value-{index:08d}" for index in range(2_000)]},
        use_dictionary=False,
        data_page_size=64,
    )
    metadata = pq.ParquetFile(BytesIO(payload)).metadata
    assert metadata is not None
    column = metadata.row_group(0).column(0)
    position = column.data_page_offset
    end = position + column.total_compressed_size
    page_count = 0
    while position < end:
        header = parquet_pages._read_parquet_page_header(payload, position, end, 64 * 1024)
        position += header.header_size + header.compressed_page_size
        page_count += 1
    assert page_count > 1

    records = list(iter_records(_capture(payload, stream), stream))
    assert len(records) == 2_000
    assert records[-1].values["source_value"] == "value-00001999"


@pytest.mark.parametrize(
    "column",
    [
        pa.array([1.5], type=pa.float64()),
        pa.array([b"binary"], type=pa.binary()),
        pa.array([[1]], type=pa.list_(pa.int64())),
        pa.array([None], type=pa.date32()),
        pa.array([None], type=pa.timestamp("us", tz="UTC")),
    ],
)
def test_parquet_decoder_rejects_unsupported_or_nested_physical_types(column):
    """Only exact capture scalar types reach later declared-field mapping."""

    stream = _stream(format_name="parquet")
    payload = _parquet_payload({"source_value": column})

    with pytest.raises(CaptureError, match="unsupported scalar type"):
        list(iter_records(_capture(payload, stream), stream))


@pytest.mark.parametrize("label", ["", "Not\u0085Printable", "x" * 256])
def test_parquet_decoder_rejects_unsafe_source_labels(label):
    """Parquet column names use the same bounded source-label contract as other decoders."""

    stream = _stream(format_name="parquet")
    payload = _parquet_payload({label: ["value"]})

    with pytest.raises(CaptureError):
        list(iter_records(_capture(payload, stream), stream))


def test_parquet_decoder_rejects_duplicate_source_labels_without_collapsing_columns():
    """Positional Arrow columns cannot silently overwrite an earlier duplicate label."""

    stream = _stream(format_name="parquet")
    schema = pa.schema(
        [
            pa.field("source_value", pa.string()),
            pa.field("source_value", pa.int64()),
        ]
    )
    table = pa.Table.from_batches(
        [
            pa.record_batch(
                [pa.array(["first"]), pa.array([1])],
                schema=schema,
            )
        ],
        schema=schema,
    )
    payload = _write_parquet_table(table)

    with pytest.raises(CaptureError, match="duplicate labels"):
        list(iter_records(_capture(payload, stream), stream))


def test_parquet_decoder_supports_gzip_and_rejects_invalid_envelopes():
    """Random-access Parquet uses a bounded seekable replay buffer and fixed errors."""

    payload = _parquet_payload({"Synthetic Source Label": ["retained only in test payload"]})
    gzip_stream = _stream(format_name="parquet", compression="gzip")
    gzip_records = list(iter_records(_capture(gzip.compress(payload), gzip_stream), gzip_stream))
    assert gzip_records[0].values["Synthetic Source Label"] == "retained only in test payload"

    stream = _stream(format_name="parquet")
    invalid_envelope = b"BAD!" + payload[4:]
    with pytest.raises(CaptureError) as error:
        list(iter_records(_capture(invalid_envelope, stream), stream))
    assert str(error.value) == "Parquet source payload has an invalid envelope"
    assert "Synthetic Source Label" not in str(error.value)

    oversized_footer = payload[:-8] + (1024 * 1024 + 1).to_bytes(4, "little") + b"PAR1"
    with pytest.raises(CaptureError, match="invalid footer"):
        list(iter_records(_capture(oversized_footer, stream), stream))


def test_parquet_outer_gzip_materialization_rechecks_its_sealed_metrics():
    """The seekable gzip buffer cannot bypass the metrics verified before decoding."""

    from process.custom_import import capture

    payload = _parquet_payload({"source_value": ["value"]})
    with pytest.raises(CaptureError, match="sealed capture"):
        capture._bounded_parquet_payload(
            gzip.compress(payload),
            "gzip",
            CaptureLimits(maximum_decoded_bytes=4096, maximum_record_bytes=1024),
            expected_decoded_bytes=len(payload),
            expected_decoded_sha256="0" * 64,
        )


def test_parquet_metadata_preflight_rejects_external_references_and_excess_groups():
    """Metadata must remain bounded and point only inside the sealed source file."""

    limits = CaptureLimits()
    with pytest.raises(CaptureError, match="external file"):
        capture_module._validated_parquet_metadata(
            _ParquetMetadataFixture("another-file.parquet"),
            expected_columns=1,
            limits=limits,
        )
    with pytest.raises(CaptureError, match="row-group limit"):
        capture_module._validated_parquet_metadata(
            _ExcessParquetGroupMetadata(),
            expected_columns=1,
            limits=limits,
        )
    assert (
        capture_module._validated_parquet_metadata(
            _ParquetMetadataFixture(None),
            expected_columns=1,
            limits=limits,
        )
        == 1
    )


def test_parquet_decoder_enforces_record_and_logical_byte_limits():
    """Metadata and each emitted row remain bounded despite Parquet compression."""

    stream = _stream(format_name="parquet")
    record_payload = _parquet_payload({"source_value": ["x" * 64]})
    record_limits = CaptureLimits(maximum_record_bytes=32, maximum_decoded_bytes=128 * 1024)
    with pytest.raises(CaptureError, match="record 1 exceeds"):
        list(
            iter_records(
                _capture(record_payload, stream, limits=record_limits),
                stream,
                limits=record_limits,
            )
        )

    logical_payload = _parquet_payload(
        {"source_value": ["x" * 500 for _ in range(300)]},
    )
    logical_limits = CaptureLimits(maximum_record_bytes=1024, maximum_decoded_bytes=128 * 1024)
    with pytest.raises(CaptureError, match="decoded-byte"):
        list(
            iter_records(
                _capture(logical_payload, stream, limits=logical_limits),
                stream,
                limits=logical_limits,
            )
        )


def test_capture_rejects_unsafe_snapshot_tokens_and_nonbinary_sources():
    """Capture input never accepts control-bearing provenance or text-returning transports."""

    stream = _stream()
    with pytest.raises(CaptureError, match="control characters"):
        capture_stream(BytesIO(b"Provider ID\n1234567893\n"), stream, source_snapshot_token="bad\n")
    with pytest.raises(CaptureError, match="valid UTF-8"):
        capture_stream(
            BytesIO(b"Provider ID\n1234567893\n"),
            stream,
            source_snapshot_token="\ud800",
        )

    class TextSource:
        """Minimal invalid source used to exercise binary transport enforcement."""

        def read(self, _size: int) -> str:
            """Return text instead of the binary protocol required by capture_stream."""

            return "text"

    with pytest.raises(CaptureError, match="must yield bytes"):
        capture_stream(TextSource(), stream, source_snapshot_token="snapshot-20260914")


@pytest.mark.parametrize(
    "snapshot_token",
    (
        "snapshot-\u202e20260914",
        "snapshot-\u202820260914",
        "snapshot-\u202920260914",
    ),
    ids=("bidi-control", "line-separator", "paragraph-separator"),
)
def test_capture_rejects_nonprintable_snapshot_tokens_but_allows_printable_unicode(
    snapshot_token,
):
    """Snapshot provenance denies invisible text without narrowing ordinary Unicode."""

    stream = _stream()
    payload = b"Provider ID\n1234567893\n"

    with pytest.raises(CaptureError, match="control characters"):
        capture_stream(
            BytesIO(payload),
            stream,
            source_snapshot_token=snapshot_token,
        )

    capture = capture_stream(
        BytesIO(payload),
        stream,
        source_snapshot_token="snapshot caf\u00e9 20260914",
    )
    assert capture.manifest.source_snapshot_token == "snapshot caf\u00e9 20260914"
