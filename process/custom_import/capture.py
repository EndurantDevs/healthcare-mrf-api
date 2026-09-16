# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Replayable, bounded capture and record decoding for custom-import streams.

The capture boundary accepts a binary stream, never a path or an arbitrary
command.  It seals the exact acquired payload with content digests before any
field mapping occurs.  Decoders expose records one at a time and keep source
labels intact for the later declarative mapping stage.
"""

from __future__ import annotations

import codecs
import csv
import gzip
import hashlib
import json
import threading
import zlib
from collections import deque
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from io import BytesIO, TextIOWrapper
from types import MappingProxyType
from typing import Any, BinaryIO
from xml.parsers import expat

import pyarrow as pa
import pyarrow.parquet as pq

from process.custom_import.definition import CONTRACT_VERSION, SourceStream

_DEFAULT_READ_CHUNK_BYTES = 64 * 1024
_MAX_SNAPSHOT_TOKEN_BYTES = 1024
_MAX_SOURCE_LABEL_BYTES = 255
_MAX_PARQUET_FOOTER_BYTES = 1024 * 1024
_MAX_PARQUET_ROW_GROUPS = 4_096
_MAX_PARQUET_THRIFT_CONTAINER_ITEMS = 64 * 1024
_PARQUET_BATCH_ROWS = 256
_XML_UNFINISHED_TOKEN_FLOOR = 4 * 1024
_XML_UNFINISHED_TOKEN_CEILING = 64 * 1024
_XML_NAMESPACE_SEPARATOR = "\x1f"
# XML unfinished-token and parser-name ceilings are v1 safety invariants,
# rather than source-tuning knobs; keep them out of the public contract.
_CSV_FIELD_SIZE_LOCK = threading.Lock()

Scalar = str | int | Decimal | bool | None


class CaptureError(ValueError):
    """A source payload is malformed, exceeds limits, or fails replay checks."""


@dataclass(frozen=True)
class CaptureLimits:
    """Resource limits applied before custom-import source data is admitted.

    The per-record byte limit applies to raw delimited and JSON record bytes
    (including escapes) and to the cumulative scalar content of XML records.
    XML parser safety also applies fixed internal ceilings to unfinished markup
    and the document-wide expanded-name vocabulary.
    """

    maximum_compressed_bytes: int = 64 * 1024 * 1024
    maximum_decoded_bytes: int = 256 * 1024 * 1024
    maximum_record_bytes: int = 1024 * 1024
    maximum_records: int = 1_000_000
    maximum_fields_per_record: int = 1_024
    read_chunk_bytes: int = _DEFAULT_READ_CHUNK_BYTES

    def __post_init__(self) -> None:
        """Reject nonsensical or internally inconsistent resource limits."""

        for name, value in (
            ("maximum_compressed_bytes", self.maximum_compressed_bytes),
            ("maximum_decoded_bytes", self.maximum_decoded_bytes),
            ("maximum_record_bytes", self.maximum_record_bytes),
            ("maximum_records", self.maximum_records),
            ("maximum_fields_per_record", self.maximum_fields_per_record),
            ("read_chunk_bytes", self.read_chunk_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.maximum_decoded_bytes < self.maximum_record_bytes:
            raise ValueError("maximum_decoded_bytes must cover one complete record")


_DEFAULT_CAPTURE_LIMITS = CaptureLimits()


@dataclass(frozen=True)
class CaptureManifest:
    """Immutable provenance and integrity facts for one sealed source payload."""

    stream_id: str
    format: str
    compression: str
    source_snapshot_token: str
    stream_sha256: str
    compressed_bytes: int
    decoded_bytes: int
    compressed_sha256: str
    decoded_sha256: str
    capture_sha256: str


@dataclass(frozen=True)
class SealedCapture:
    """Replayable acquired bytes paired with their immutable capture manifest."""

    payload: bytes
    manifest: CaptureManifest


@dataclass(frozen=True)
class DecodedRecord:
    """One source-label record emitted by a bounded streaming decoder."""

    ordinal: int
    values: Mapping[str, Scalar]


def capture_stream(
    acquired_source: BinaryIO,
    source_stream: SourceStream,
    *,
    source_snapshot_token: str,
    limits: CaptureLimits = _DEFAULT_CAPTURE_LIMITS,
) -> SealedCapture:
    """Read, bound, hash, and seal a source stream without interpreting fields."""

    snapshot_token = _validated_snapshot_token(source_snapshot_token)
    sealed_payload = _read_source_payload(acquired_source, limits)
    decoded_bytes, decoded_sha256 = _decoded_metrics(
        sealed_payload,
        source_stream.compression,
        limits,
    )
    compressed_sha256 = hashlib.sha256(sealed_payload).hexdigest()
    manifest = CaptureManifest(
        stream_id=source_stream.stream_id,
        format=source_stream.format,
        compression=source_stream.compression,
        source_snapshot_token=snapshot_token,
        stream_sha256=_source_stream_sha256(source_stream),
        compressed_bytes=len(sealed_payload),
        decoded_bytes=decoded_bytes,
        compressed_sha256=compressed_sha256,
        decoded_sha256=decoded_sha256,
        capture_sha256=_capture_sha256(
            stream=source_stream,
            source_snapshot_token=snapshot_token,
            stream_sha256=_source_stream_sha256(source_stream),
            compressed_sha256=compressed_sha256,
            decoded_sha256=decoded_sha256,
        ),
    )
    return SealedCapture(payload=sealed_payload, manifest=manifest)


def verify_capture(
    capture: SealedCapture,
    stream: SourceStream,
    *,
    limits: CaptureLimits = _DEFAULT_CAPTURE_LIMITS,
) -> None:
    """Verify that a replay payload still matches its stream and sealed manifest."""

    if not isinstance(capture, SealedCapture):
        raise CaptureError("sealed capture must use the declared capture type")
    manifest = capture.manifest
    if not isinstance(manifest, CaptureManifest):
        raise CaptureError("sealed capture manifest must use the declared manifest type")
    if (
        manifest.stream_id != stream.stream_id
        or manifest.format != stream.format
        or manifest.compression != stream.compression
        or manifest.stream_sha256 != _source_stream_sha256(stream)
    ):
        raise CaptureError("capture manifest does not belong to the declared source stream")
    token = _validated_snapshot_token(manifest.source_snapshot_token)
    compressed_bytes = _validated_manifest_size(
        manifest.compressed_bytes,
        maximum=limits.maximum_compressed_bytes,
        label="compressed",
    )
    expected_decoded_bytes = _validated_manifest_size(
        manifest.decoded_bytes,
        maximum=limits.maximum_decoded_bytes,
        label="decoded",
    )
    if not isinstance(capture.payload, bytes):
        raise CaptureError("sealed capture payload must be bytes")
    if len(capture.payload) != compressed_bytes:
        raise CaptureError("capture payload length does not match the sealed manifest")
    compressed_sha256 = hashlib.sha256(capture.payload).hexdigest()
    if compressed_sha256 != manifest.compressed_sha256:
        raise CaptureError("capture payload digest does not match the sealed manifest")
    actual_decoded_bytes, decoded_sha256 = _decoded_metrics(capture.payload, stream.compression, limits)
    if actual_decoded_bytes != expected_decoded_bytes or decoded_sha256 != manifest.decoded_sha256:
        raise CaptureError("decoded capture digest does not match the sealed manifest")
    if (
        _capture_sha256(
            stream=stream,
            source_snapshot_token=token,
            stream_sha256=manifest.stream_sha256,
            compressed_sha256=compressed_sha256,
            decoded_sha256=decoded_sha256,
        )
        != manifest.capture_sha256
    ):
        raise CaptureError("capture manifest digest does not match its provenance")


def iter_records(
    capture: SealedCapture,
    stream: SourceStream,
    *,
    limits: CaptureLimits = _DEFAULT_CAPTURE_LIMITS,
) -> Iterator[DecodedRecord]:
    """Yield validated source-label records from a verified sealed capture."""

    verify_capture(capture, stream, limits=limits)
    yield from _iter_verified_records(capture, stream, limits=limits)


def _iter_verified_records(
    capture: SealedCapture,
    stream: SourceStream,
    *,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode one capture only after an internal caller has verified its sealed bytes."""

    if stream.format in {"csv", "tsv"}:
        yield from _iter_delimited_records(capture.payload, stream, limits)
        return
    if stream.format == "json":
        yield from _iter_json_records(capture.payload, stream, limits)
        return
    if stream.format == "ndjson":
        yield from _iter_ndjson_records(capture.payload, stream, limits)
        return
    if stream.format == "xml":
        yield from _iter_xml_records(capture.payload, stream, limits)
        return
    if stream.format == "parquet":
        yield from _iter_parquet_records(
            capture.payload,
            stream,
            limits,
            expected_decoded_bytes=capture.manifest.decoded_bytes,
            expected_decoded_sha256=capture.manifest.decoded_sha256,
        )
        return
    raise CaptureError("capture manifest declares an unsupported source format")


def _read_source_payload(source: BinaryIO, limits: CaptureLimits) -> bytes:
    """Read an acquired binary stream while enforcing the compressed-size limit."""

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = source.read(limits.read_chunk_bytes)
        if chunk == b"":
            return b"".join(chunks)
        if not isinstance(chunk, bytes):
            raise CaptureError("source stream must yield bytes")
        total += len(chunk)
        if total > limits.maximum_compressed_bytes:
            raise CaptureError("source stream exceeds the compressed-byte limit")
        chunks.append(chunk)


def _decoded_metrics(payload: bytes, compression: str, limits: CaptureLimits) -> tuple[int, str]:
    """Measure and hash decoded content without retaining a second payload copy."""

    digest = hashlib.sha256()
    total = 0
    try:
        with _open_decoded_payload(payload, compression) as decoded:
            while chunk := decoded.read(limits.read_chunk_bytes):
                total += len(chunk)
                if total > limits.maximum_decoded_bytes:
                    raise CaptureError("source stream exceeds the decoded-byte limit")
                digest.update(chunk)
    except (EOFError, OSError, zlib.error) as exc:
        raise CaptureError("source stream compression is invalid") from exc
    return total, digest.hexdigest()


@contextmanager
def _open_decoded_payload(payload: bytes, compression: str) -> Iterator[BinaryIO]:
    """Open an exact replayable decoded byte stream for one declared compression mode."""

    source = BytesIO(payload)
    if compression == "none":
        try:
            yield source
        finally:
            source.close()
        return
    if compression != "gzip":
        source.close()
        raise CaptureError("capture manifest declares an unsupported compression mode")
    decoded = gzip.GzipFile(fileobj=source, mode="rb")
    try:
        yield decoded
    finally:
        decoded.close()
        source.close()


def _capture_sha256(
    *,
    stream: SourceStream,
    source_snapshot_token: str,
    stream_sha256: str,
    compressed_sha256: str,
    decoded_sha256: str,
) -> str:
    """Bind source identity, snapshot, and both payload forms into one digest."""

    material = (
        f"{CONTRACT_VERSION}\x00capture\x00{stream.stream_id}\x00{stream.format}\x00"
        f"{stream.compression}\x00{source_snapshot_token}\x00{stream_sha256}\x00"
        f"{compressed_sha256}\x00{decoded_sha256}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _source_stream_sha256(stream: SourceStream) -> str:
    """Hash every declared stream-shape term used to decode a sealed capture."""

    material = "\x00".join(
        (
            CONTRACT_VERSION,
            "source-stream",
            stream.stream_id,
            stream.record_kind,
            stream.child_collection or "",
            stream.format,
            stream.compression,
            stream.snapshot_token,
            stream.record_path or "",
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _validated_snapshot_token(value: str) -> str:
    """Require a bounded opaque source snapshot token that is safe to retain."""

    if not isinstance(value, str) or not value:
        raise CaptureError("source snapshot token must be a non-empty string")
    if len(_utf8_bytes(value, "source snapshot token")) > _MAX_SNAPSHOT_TOKEN_BYTES:
        raise CaptureError("source snapshot token exceeds the byte limit")
    if not value.isprintable():
        raise CaptureError("source snapshot token contains control characters or non-printable text")
    return value


def _validated_manifest_size(value: object, *, maximum: int, label: str) -> int:
    """Require a bounded integer manifest size before replay hashes are calculated."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
        raise CaptureError(f"capture manifest {label} byte count is invalid")
    return value


def _iter_delimited_records(
    payload: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode a headered CSV or TSV source one row at a time."""

    try:
        with (
            _open_decoded_payload(payload, stream.compression) as binary,
            TextIOWrapper(binary, encoding="utf-8", newline="") as text,
        ):
            delimiter = "," if stream.format == "csv" else "\t"
            yield from _iter_delimited_text_records(text, delimiter, limits)
    except (UnicodeDecodeError, csv.Error) as exc:
        raise CaptureError("delimited source payload is invalid") from exc


def _iter_delimited_text_records(
    text: TextIOWrapper,
    delimiter: str,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Yield bounded rows from an already-open delimited text stream."""

    bounded_lines = _BoundedDelimitedLines(text, limits.maximum_record_bytes)
    reader = csv.reader(bounded_lines, delimiter=delimiter, strict=True)
    bounded_lines.begin_record()
    with _csv_field_limit(limits.maximum_record_bytes):
        headers = next(reader, None)
    source_labels = _validated_headers(headers, limits)
    ordinal = 0
    while True:
        bounded_lines.begin_record()
        with _csv_field_limit(limits.maximum_record_bytes):
            row = next(reader, None)
        if row is None:
            return
        ordinal += 1
        if len(row) != len(source_labels):
            raise CaptureError(f"record {ordinal} field count does not match its header")
        yield _decoded_record(ordinal, dict(zip(source_labels, row, strict=True)), limits)


class _BoundedDelimitedLines:
    """Limit raw UTF-8 bytes supplied to ``csv.reader`` for one logical row."""

    def __init__(self, text: TextIOWrapper, maximum_record_bytes: int) -> None:
        self.text = text
        self.maximum_record_bytes = maximum_record_bytes
        self.record_bytes = 0

    def __iter__(self) -> "_BoundedDelimitedLines":
        return self

    def __next__(self) -> str:
        remaining_bytes = self.maximum_record_bytes - self.record_bytes
        physical_line = self.text.readline(remaining_bytes + 1)
        if physical_line == "":
            raise StopIteration
        self.record_bytes += len(_utf8_bytes(physical_line, "delimited source record"))
        if self.record_bytes > self.maximum_record_bytes:
            raise CaptureError("delimited source record exceeds the byte limit")
        return physical_line

    def begin_record(self) -> None:
        """Reset the cumulative budget immediately before one reader advance."""

        self.record_bytes = 0


@contextmanager
def _csv_field_limit(limit: int) -> Iterator[None]:
    """Apply a process-global CSV field limit without racing concurrent decoders."""

    with _CSV_FIELD_SIZE_LOCK:
        previous = csv.field_size_limit(limit)
        try:
            yield
        finally:
            csv.field_size_limit(previous)


def _validated_headers(headers: list[str] | None, limits: CaptureLimits) -> tuple[str, ...]:
    """Require unique non-empty source labels before rows are emitted."""

    if not headers:
        raise CaptureError("delimited source payload requires a header row")
    if len(headers) > limits.maximum_fields_per_record:
        raise CaptureError("delimited source header exceeds the field limit")
    source_labels = tuple(_validated_source_label(header) for header in headers)
    if len(set(source_labels)) != len(source_labels):
        raise CaptureError("delimited source header contains duplicate labels")
    return source_labels


def _iter_json_records(
    payload: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode a bounded top-level JSON array without materializing raw items."""

    try:
        with _open_decoded_payload(payload, stream.compression) as binary:
            for ordinal, raw_record in enumerate(_iter_bounded_json_objects(binary, limits), start=1):
                try:
                    record = json.loads(
                        raw_record,
                        object_pairs_hook=_strict_json_object,
                        parse_float=Decimal,
                        parse_constant=_reject_json_constant,
                    )
                except CaptureError:
                    raise
                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                    DecimalException,
                ) as exc:
                    raise CaptureError("JSON source payload is invalid") from exc
                yield _decoded_record(ordinal, record, limits)
    except (EOFError, OSError, zlib.error) as exc:
        raise CaptureError("source stream compression is invalid") from exc


def _iter_bounded_json_objects(
    binary: BinaryIO,
    limits: CaptureLimits,
) -> Iterator[bytes]:
    """Frame raw bounded object items before a JSON parser can allocate values."""

    framer = _BoundedJsonObjectFramer(limits)
    while True:
        input_chunk = binary.read(limits.read_chunk_bytes)
        if not input_chunk:
            break
        yield from framer.feed(input_chunk)
    yield from framer.finish()


class _BoundedJsonObjectFramer:
    """Recognize a flat JSON record array without parsing record values."""

    def __init__(self, limits: CaptureLimits) -> None:
        self.limits = limits
        self.state = "before_array"
        self.has_seen_item = False
        self.record_count = 0
        self.raw_object = bytearray()
        self.is_inside_string = False
        self.is_escaped = False
        self.field_count = 0

    def feed(self, input_chunk: bytes) -> Iterator[bytes]:
        """Consume one bounded byte chunk and yield complete raw objects."""

        if not isinstance(input_chunk, bytes):
            raise CaptureError("JSON source stream must yield bytes")
        for input_byte in input_chunk:
            raw_object = self._consume(input_byte)
            if raw_object is not None:
                yield raw_object

    def finish(self) -> Iterator[bytes]:
        """Reject an empty, incomplete, or unterminated JSON array."""

        if self.state == "before_array":
            raise CaptureError("JSON source payload is empty")
        if self.state in {"before_item", "in_object", "after_item"}:
            raise CaptureError("JSON source payload is incomplete")
        yield from ()

    def _consume(self, input_byte: int) -> bytes | None:
        """Consume one byte according to the current top-level array state."""

        if self.state == "before_array":
            return self._consume_before_array(input_byte)
        if self.state == "before_item":
            return self._consume_before_item(input_byte)
        if self.state == "in_object":
            return self._consume_in_object(input_byte)
        if self.state == "after_item":
            return self._consume_after_item(input_byte)
        if input_byte not in b" \t\r\n":
            raise CaptureError("JSON source payload has trailing data")
        return None

    def _consume_before_array(self, input_byte: int) -> None:
        """Require one top-level array opener."""

        if input_byte in b" \t\r\n":
            return None
        if input_byte != ord("["):
            raise CaptureError("JSON source payload must be a top-level array")
        self.state = "before_item"
        return None

    def _consume_before_item(self, input_byte: int) -> None:
        """Require an object item or close an empty array."""

        if input_byte in b" \t\r\n":
            return None
        if input_byte == ord("]"):
            if self.has_seen_item:
                raise CaptureError("JSON source payload has a trailing comma")
            self.state = "after_array"
            return None
        if input_byte != ord("{"):
            raise CaptureError("JSON source records must be objects")
        if self.record_count >= self.limits.maximum_records:
            raise CaptureError("JSON source payload exceeds the record limit")
        self.raw_object = bytearray((input_byte,))
        self.field_count = 0
        self.is_inside_string = False
        self.is_escaped = False
        self.state = "in_object"
        return None

    def _consume_in_object(self, input_byte: int) -> bytes | None:
        """Consume one flat object byte and return it when complete."""

        if len(self.raw_object) >= self.limits.maximum_record_bytes:
            raise CaptureError("JSON source record exceeds the byte limit")
        self.raw_object.append(input_byte)
        if self.is_inside_string:
            self._consume_string_byte(input_byte)
            return None
        if input_byte == ord('"'):
            self.is_inside_string = True
            return None
        if input_byte in (ord("{"), ord("[")):
            raise CaptureError("JSON source records must contain flat scalars")
        if input_byte == ord(":"):
            self.field_count += 1
            if self.field_count > self.limits.maximum_fields_per_record:
                raise CaptureError("JSON source record exceeds the field limit")
        if input_byte != ord("}"):
            return None
        self.state = "after_item"
        self.has_seen_item = True
        self.record_count += 1
        return bytes(self.raw_object)

    def _consume_string_byte(self, input_byte: int) -> None:
        """Track JSON string escapes without interpreting their values."""

        if self.is_escaped:
            self.is_escaped = False
        elif input_byte == ord("\\"):
            self.is_escaped = True
        elif input_byte == ord('"'):
            self.is_inside_string = False

    def _consume_after_item(self, input_byte: int) -> None:
        """Require a comma, array close, or JSON whitespace after an item."""

        if input_byte in b" \t\r\n":
            return None
        if input_byte == ord(","):
            self.state = "before_item"
            return None
        if input_byte == ord("]"):
            self.state = "after_array"
            return None
        raise CaptureError("JSON source payload has trailing data")


def _iter_ndjson_records(
    captured_bytes: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode one strict JSON object per non-empty NDJSON line."""

    ordinal = 0
    try:
        with _open_decoded_payload(captured_bytes, stream.compression) as binary:
            while True:
                raw_line = binary.readline(limits.maximum_record_bytes + 1)
                if not raw_line:
                    break
                if len(raw_line) > limits.maximum_record_bytes:
                    raise CaptureError("NDJSON record exceeds the byte limit")
                line = raw_line.strip()
                if not line:
                    continue
                ordinal += 1
                try:
                    decoded_value = json.loads(
                        line.decode("utf-8"),
                        object_pairs_hook=_strict_json_object,
                        parse_float=Decimal,
                        parse_constant=_reject_json_constant,
                    )
                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                    DecimalException,
                ) as exc:
                    raise CaptureError(f"NDJSON record {ordinal} is invalid") from exc
                yield _decoded_record(ordinal, decoded_value, limits)
    except (EOFError, OSError, zlib.error) as exc:
        raise CaptureError("source stream compression is invalid") from exc


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while denying duplicate labels."""

    values_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in values_by_key:
            raise CaptureError("duplicate JSON object key")
        values_by_key[key] = value
    return values_by_key


def _reject_json_constant(value: str) -> None:
    """Deny non-finite JSON numeric constants at the raw source boundary."""

    raise ValueError(f"unsupported JSON numeric constant: {value}")


def _iter_xml_records(
    captured_bytes: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode flat direct-child XML records with bounded Expat input progress."""

    record_tag = stream.record_path
    if record_tag is None:
        raise CaptureError("XML streams require a declared record path")
    if _has_forbidden_xml_declaration(captured_bytes, stream.compression, limits):
        raise CaptureError("XML source payload cannot contain entity declarations")
    try:
        with _open_decoded_payload(captured_bytes, stream.compression) as binary:
            collector = _XmlRecordCollector(record_tag, limits)
            parser = _xml_parser(collector)
            # The raw bytes supplied while Expat is inside an unfinished
            # markup token are bounded independently of completed record size.
            token_budget = min(
                limits.maximum_decoded_bytes,
                max(
                    _XML_UNFINISHED_TOKEN_FLOOR,
                    min(limits.maximum_record_bytes, _XML_UNFINISHED_TOKEN_CEILING),
                ),
            )
            yield from _feed_xml_parser(binary, parser, collector, limits, token_budget)
            parser.Parse(b"", True)
            yield from collector.drain()
    except CaptureError:
        raise
    except (expat.ExpatError, UnicodeDecodeError, UnicodeEncodeError) as exc:
        raise CaptureError("XML source payload is invalid") from exc


def _feed_xml_parser(
    binary: BinaryIO,
    parser: Any,
    collector: _XmlRecordCollector,
    limits: CaptureLimits,
    token_budget: int,
) -> Iterator[DecodedRecord]:
    """Feed bounded chunks to Expat while draining completed records."""

    supplied = 0
    while True:
        outstanding = max(0, supplied - parser.CurrentByteIndex)
        if outstanding >= token_budget:
            parser.SetReparseDeferralEnabled(False)
            try:
                parser.Parse(b"", False)
            finally:
                parser.SetReparseDeferralEnabled(True)
            yield from collector.drain()
            outstanding = max(0, supplied - parser.CurrentByteIndex)
        if outstanding >= token_budget:
            raise CaptureError("XML source payload has an unfinished token over the limit")
        chunk = binary.read(min(limits.read_chunk_bytes, token_budget - outstanding))
        if not chunk:
            return
        if not isinstance(chunk, bytes):
            raise CaptureError("XML source stream must yield bytes")
        supplied += len(chunk)
        parser.Parse(chunk, False)
        yield from collector.drain()


def _iter_parquet_records(
    captured_bytes: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
    *,
    expected_decoded_bytes: int,
    expected_decoded_sha256: str,
) -> Iterator[DecodedRecord]:
    """Decode bounded, flat Parquet rows without permitting external file references."""

    decoded_payload = _bounded_parquet_payload(
        captured_bytes,
        stream.compression,
        limits,
        expected_decoded_bytes=expected_decoded_bytes,
        expected_decoded_sha256=expected_decoded_sha256,
    )
    _validate_parquet_envelope(decoded_payload)
    with _open_parquet_reader(decoded_payload, limits) as parquet_reader:
        source_labels = _validated_parquet_schema(parquet_reader.schema_arrow, limits)
        expected_record_count = _validated_parquet_metadata(
            parquet_reader.metadata,
            expected_columns=len(source_labels),
            limits=limits,
        )
        yield from _iter_parquet_batch_records(
            parquet_reader,
            source_labels,
            expected_record_count,
            limits,
        )


@contextmanager
def _open_parquet_reader(
    decoded_payload: bytes | memoryview,
    limits: CaptureLimits,
) -> Iterator[pq.ParquetFile]:
    """Open one in-memory Parquet reader with bounded native parser metadata."""

    native_buffer: pa.BufferReader | None = None
    parquet_reader: pq.ParquetFile | None = None
    try:
        native_buffer = pa.BufferReader(decoded_payload)
        parquet_reader = pq.ParquetFile(
            native_buffer,
            memory_map=False,
            buffer_size=0,
            pre_buffer=False,
            thrift_string_size_limit=min(_MAX_PARQUET_FOOTER_BYTES, limits.maximum_decoded_bytes),
            thrift_container_size_limit=_MAX_PARQUET_THRIFT_CONTAINER_ITEMS,
            page_checksum_verification=True,
            arrow_extensions_enabled=False,
        )
        yield parquet_reader
    except CaptureError:
        raise
    except (pa.ArrowException, EOFError, OSError, OverflowError, ValueError) as exc:
        raise CaptureError("Parquet source payload is invalid") from exc
    finally:
        if parquet_reader is not None:
            parquet_reader.close()
        if native_buffer is not None:
            native_buffer.close()


def _iter_parquet_batch_records(
    parquet_reader: pq.ParquetFile,
    source_labels: tuple[str, ...],
    expected_record_count: int,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Validate bounded record batches before exposing capture scalar mappings."""

    emitted_record_count = 0
    logical_batch_bytes = 0
    for record_batch in parquet_reader.iter_batches(
        batch_size=min(_PARQUET_BATCH_ROWS, limits.maximum_records),
        use_threads=False,
        use_pandas_metadata=False,
    ):
        _validate_parquet_batch_schema(record_batch, source_labels)
        logical_batch_bytes += _nonnegative_parquet_integer(record_batch.nbytes)
        if logical_batch_bytes > limits.maximum_decoded_bytes:
            raise CaptureError("Parquet source payload exceeds the decoded-byte limit")
        for row_index in range(record_batch.num_rows):
            if emitted_record_count >= expected_record_count:
                raise CaptureError("Parquet source payload has inconsistent row metadata")
            emitted_record_count += 1
            scalar_values_by_label = {
                source_labels[column_index]: record_batch.column(column_index)[row_index].as_py()
                for column_index in range(record_batch.num_columns)
            }
            yield _decoded_record(emitted_record_count, scalar_values_by_label, limits)
    if emitted_record_count != expected_record_count:
        raise CaptureError("Parquet source payload has inconsistent row metadata")


def _bounded_parquet_payload(
    captured_bytes: bytes,
    compression: str,
    limits: CaptureLimits,
    *,
    expected_decoded_bytes: int,
    expected_decoded_sha256: str,
) -> bytes | memoryview:
    """Materialize only the bounded seekable bytes required by Parquet random access."""

    if compression == "none":
        return captured_bytes
    if (
        isinstance(expected_decoded_bytes, bool)
        or not isinstance(expected_decoded_bytes, int)
        or expected_decoded_bytes < 0
        or expected_decoded_bytes > limits.maximum_decoded_bytes
        or not isinstance(expected_decoded_sha256, str)
    ):
        raise CaptureError("Parquet source payload does not match the sealed capture")
    decoded_bytes = bytearray(expected_decoded_bytes)
    offset = 0
    digest = hashlib.sha256()
    try:
        with _open_decoded_payload(captured_bytes, compression) as decoded:
            while offset < expected_decoded_bytes:
                chunk = decoded.read(min(limits.read_chunk_bytes, expected_decoded_bytes - offset))
                if not chunk:
                    break
                decoded_bytes[offset : offset + len(chunk)] = chunk
                digest.update(chunk)
                offset += len(chunk)
            has_extra_bytes = bool(decoded.read(1))
    except (EOFError, OSError, zlib.error) as exc:
        raise CaptureError("Parquet source payload compression is invalid") from exc
    if offset != expected_decoded_bytes or has_extra_bytes or digest.hexdigest() != expected_decoded_sha256:
        raise CaptureError("Parquet source payload does not match the sealed capture")
    return memoryview(decoded_bytes).toreadonly()


def _validate_parquet_envelope(payload: bytes | memoryview) -> None:
    """Reject malformed or oversized Parquet footers before native metadata parsing."""

    if len(payload) < 12 or payload[:4] != b"PAR1" or payload[-4:] != b"PAR1":
        raise CaptureError("Parquet source payload has an invalid envelope")
    footer_bytes = int.from_bytes(payload[-8:-4], byteorder="little")
    if footer_bytes == 0 or footer_bytes > _MAX_PARQUET_FOOTER_BYTES or footer_bytes > len(payload) - 8:
        raise CaptureError("Parquet source payload has an invalid footer")


def _validated_parquet_schema(
    schema: pa.Schema,
    limits: CaptureLimits,
) -> tuple[str, ...]:
    """Allow only labeled, flat source columns that map to capture scalar values."""

    if len(schema) == 0:
        raise CaptureError("Parquet source payload requires at least one source column")
    if len(schema) > limits.maximum_fields_per_record:
        raise CaptureError("Parquet source payload exceeds the field limit")
    source_labels = tuple(_validated_source_label(field.name) for field in schema)
    if len(set(source_labels)) != len(source_labels):
        raise CaptureError("Parquet source payload contains duplicate labels")
    if any(not _is_supported_parquet_scalar(field.type) for field in schema):
        raise CaptureError("Parquet source payload contains an unsupported scalar type")
    return source_labels


def _is_supported_parquet_scalar(data_type: pa.DataType) -> bool:
    """Keep the native decoder within the existing exact capture scalar contract."""

    return (
        pa.types.is_null(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_integer(data_type)
        or pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_decimal(data_type)
    )


def _validated_parquet_metadata(
    metadata: pq.FileMetaData | None,
    *,
    expected_columns: int,
    limits: CaptureLimits,
) -> int:
    """Bound metadata-controlled allocation before iterating Parquet data pages."""

    if metadata is None:
        raise CaptureError("Parquet source payload has no metadata")
    record_count = _nonnegative_parquet_integer(metadata.num_rows)
    row_group_count = _nonnegative_parquet_integer(metadata.num_row_groups)
    column_count = _nonnegative_parquet_integer(metadata.num_columns)
    if record_count > limits.maximum_records:
        raise CaptureError("Parquet source payload exceeds the record limit")
    if row_group_count > _MAX_PARQUET_ROW_GROUPS:
        raise CaptureError("Parquet source payload exceeds the row-group limit")
    if column_count != expected_columns:
        raise CaptureError("Parquet source payload has inconsistent column metadata")

    row_group_records = 0
    declared_uncompressed_bytes = 0
    for row_group_index in range(row_group_count):
        row_group = metadata.row_group(row_group_index)
        group_record_count = _nonnegative_parquet_integer(row_group.num_rows)
        group_column_count = _nonnegative_parquet_integer(row_group.num_columns)
        if group_column_count != expected_columns:
            raise CaptureError("Parquet source payload has inconsistent column metadata")
        row_group_records += group_record_count
        if row_group_records > limits.maximum_records:
            raise CaptureError("Parquet source payload exceeds the record limit")
        for column_index in range(group_column_count):
            column = row_group.column(column_index)
            if column.file_path not in (None, ""):
                raise CaptureError("Parquet source payload cannot reference an external file")
            if _nonnegative_parquet_integer(column.num_values) != group_record_count:
                raise CaptureError("Parquet source payload has inconsistent row metadata")
            declared_uncompressed_bytes += _nonnegative_parquet_integer(column.total_uncompressed_size)
            if declared_uncompressed_bytes > limits.maximum_decoded_bytes:
                raise CaptureError("Parquet source payload exceeds the decoded-byte limit")
    if row_group_records != record_count:
        raise CaptureError("Parquet source payload has inconsistent row metadata")
    return record_count


def _nonnegative_parquet_integer(value: object) -> int:
    """Reject malformed metadata numbers rather than allowing implicit coercion."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CaptureError("Parquet source payload has invalid metadata")
    return value


def _validate_parquet_batch_schema(
    batch: pa.RecordBatch,
    source_labels: tuple[str, ...],
) -> None:
    """Ensure batch columns remain positional matches for the validated source schema."""

    if batch.num_columns != len(source_labels) or tuple(batch.schema.names) != source_labels:
        raise CaptureError("Parquet source payload has an inconsistent batch schema")


def _xml_parser(collector: "_XmlRecordCollector") -> Any:
    """Create an Expat parser with explicit UTF-8 and entity-denial handlers."""

    parser = expat.ParserCreate("UTF-8", _XML_NAMESPACE_SEPARATOR)
    parser.buffer_text = False
    parser.StartElementHandler = collector.start_element
    parser.EndElementHandler = collector.end_element
    parser.CharacterDataHandler = collector.character_data
    parser.CommentHandler = collector.comment
    parser.StartNamespaceDeclHandler = collector.start_namespace
    parser.ProcessingInstructionHandler = collector.processing_instruction
    parser.StartDoctypeDeclHandler = collector.forbidden_declaration
    parser.EntityDeclHandler = collector.forbidden_declaration
    parser.ExternalEntityRefHandler = collector.forbidden_external_entity
    parser.SkippedEntityHandler = collector.forbidden_declaration
    collector.parser = parser
    return parser


class _XmlRecordCollector:
    """Collect one flat XML record at a time with document-wide name bounds."""

    def __init__(self, record_tag: str, limits: CaptureLimits) -> None:
        self.record_tag = record_tag
        self.limits = limits
        self.depth = 0
        self.root_seen = False
        self.in_record = False
        self.current_field: str | None = None
        self.current_field_text: list[str] = []
        self.current_field_bytes = 0
        self.values: dict[str, str] = {}
        self.record_bytes = 0
        self.field_count = 0
        self.ordinal = 0
        self.completed: deque[DecodedRecord] = deque()
        self.known_names: set[str] = set()
        self.known_namespace_declarations: set[tuple[str, str]] = set()

    def start_element(self, raw_name: str, attrs: dict[str, str]) -> None:
        """Validate element shape before Expat can build a nested element tree."""

        name = _xml_name(raw_name)
        self._observe_name(name)
        for attribute_name in attrs:
            self._observe_name(_xml_name(attribute_name))
        self.depth += 1
        if self.depth == 1:
            if self.root_seen:
                raise CaptureError("XML source payload has multiple root elements")
            self.root_seen = True
            return
        if self.depth == 2:
            if self.ordinal >= self.limits.maximum_records:
                raise CaptureError("source stream exceeds the record limit")
            if name != self.record_tag:
                raise CaptureError("XML source payload has an unexpected root child")
            if attrs:
                raise CaptureError("XML record elements cannot carry attributes")
            self.in_record = True
            self.values = {}
            self.record_bytes = 0
            self.field_count = 0
            return
        if self.depth == 3:
            if not self.in_record or attrs:
                raise CaptureError("XML records must contain flat scalar child elements")
            label = _validated_source_label(name)
            if label in self.values:
                raise CaptureError("XML record contains duplicate label")
            self.field_count += 1
            if self.field_count > self.limits.maximum_fields_per_record:
                raise CaptureError("XML record exceeds the field limit")
            self.record_bytes += len(_utf8_bytes(label, "source record label")) + 4
            if self.record_bytes > self.limits.maximum_record_bytes:
                raise CaptureError("XML record exceeds the byte limit")
            self.current_field = label
            self.current_field_text = []
            self.current_field_bytes = 0
            return
        raise CaptureError("XML records must contain flat scalar child elements")

    def end_element(self, _raw_name: str) -> None:
        """Finalize a scalar field or complete a direct-child record."""

        if self.depth == 3 and self.current_field is not None:
            self.values[self.current_field] = "".join(self.current_field_text)
            self.current_field = None
            self.current_field_text = []
            self.current_field_bytes = 0
        elif self.depth == 2 and self.in_record:
            self.ordinal += 1
            self.completed.append(_decoded_record(self.ordinal, self.values, self.limits))
            self.in_record = False
            self.values = {}
            self._clear_parser_intern()
        if self.depth <= 0:
            raise CaptureError("XML source payload is invalid")
        self.depth -= 1

    def character_data(self, text: str) -> None:
        """Bound field text and reject non-whitespace record-level mixed content."""

        if self.current_field is not None:
            try:
                text_bytes = len(text.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise CaptureError("XML source payload must be valid UTF-8") from exc
            self.current_field_bytes += text_bytes
            self.record_bytes += text_bytes
            if (
                self.current_field_bytes > self.limits.maximum_record_bytes
                or self.record_bytes > self.limits.maximum_record_bytes
            ):
                raise CaptureError("XML record exceeds the byte limit")
            self.current_field_text.append(text)
            return
        if self.in_record and not _is_xml_whitespace(text):
            raise CaptureError("XML records cannot contain non-XML-whitespace text")

    def comment(self, _text: str) -> None:
        """Consume comments so Expat advances past completed markup tokens."""

    def start_namespace(self, prefix: str | None, uri: str) -> None:
        """Bound namespace declarations that Expat retains outside normal attrs."""

        pair = (prefix or "", uri)
        if pair not in self.known_namespace_declarations:
            self._observe_name(f"namespace:{pair[0]}:{pair[1]}")
            self.known_namespace_declarations.add(pair)

    def processing_instruction(self, target: str, _data: str) -> None:
        """Count PI targets without retaining their untrusted data."""

        self._observe_name(f"pi:{target}")

    def forbidden_declaration(self, *_args: object) -> None:
        """Fail closed even when declaration preflight is bypassed or split."""

        raise CaptureError("XML source payload cannot contain entity declarations")

    def forbidden_external_entity(self, *_args: object) -> int:
        """Deny all external entity resolution."""

        raise CaptureError("XML source payload cannot resolve external entities")

    def drain(self) -> Iterator[DecodedRecord]:
        """Drain completions after each bounded parser feed."""

        while self.completed:
            yield self.completed.popleft()

    def _observe_name(self, name: str) -> None:
        """Apply the finite document-wide parser-name vocabulary limit."""

        if name in self.known_names:
            return
        # Leave a small fixed allowance for the root/record tags and namespace
        # or processing-instruction metadata while still bounding document-wide
        # Expat name retention independently of record count.
        if len(self.known_names) >= self.limits.maximum_fields_per_record + 8:
            raise CaptureError("XML source payload exceeds the distinct-name limit")
        self.known_names.add(name)

    def _clear_parser_intern(self) -> None:
        """Release completed-record names from Expat's Python intern table."""

        # Expat exposes this mutable mapping on CPython; keep the collector
        # compatible with alternate implementations that do not.
        parser = getattr(self, "parser", None)
        if parser is not None:
            intern = getattr(parser, "intern", None)
            if hasattr(intern, "clear"):
                intern.clear()


def _xml_name(raw_name: str) -> str:
    """Normalize Expat namespace names to ElementTree's expanded-name form."""

    if _XML_NAMESPACE_SEPARATOR not in raw_name:
        return raw_name
    namespace, local = raw_name.split(_XML_NAMESPACE_SEPARATOR, 1)
    return f"{{{namespace}}}{local}"


def _has_forbidden_xml_declaration(
    payload: bytes,
    compression: str,
    limits: CaptureLimits,
) -> bool:
    """Scan UTF-8 XML incrementally for declarations ElementTree must never expand.

    The scanner deliberately keeps only parsing state, rather than a fixed
    trailing buffer: XML permits arbitrary whitespace after ``<!``, so a
    boundary-spanning declaration must not evade the denial rule.  XML source
    bytes are deliberately limited to UTF-8 so alternate encodings cannot
    evade byte-level declaration recognition or change parser semantics.
    """

    state = _XmlDeclarationScanState()
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        with _open_decoded_payload(payload, compression) as binary:
            while chunk := binary.read(limits.read_chunk_bytes):
                decoder.decode(chunk)
                if state.has_forbidden_declaration(chunk):
                    return True
            decoder.decode(b"", final=True)
    except UnicodeDecodeError as exc:
        raise CaptureError("XML source payload must be valid UTF-8") from exc
    except (EOFError, OSError, zlib.error) as exc:
        raise CaptureError("source stream compression is invalid") from exc
    return False


@dataclass
class _XmlDeclarationScanState:
    """Constant-space recognizer for ``<!DOCTYPE`` and ``<!ENTITY`` markup."""

    state: int = 0
    target: bytes | None = None
    target_offset: int = 0

    def has_forbidden_declaration(self, chunk: bytes) -> bool:
        """Advance over a decoded XML chunk and report a prohibited declaration."""

        for character in chunk:
            if self.state == 0:
                self.state = 1 if character == ord("<") else 0
                continue
            if self.state == 1:
                if character == ord("!"):
                    self.state = 2
                else:
                    self.state = 1 if character == ord("<") else 0
                continue
            if self.state == 2:
                if character in b" \t\r\n":
                    continue
                lowered = _ascii_lower(character)
                if lowered == ord("d"):
                    self.target = b"doctype"
                elif lowered == ord("e"):
                    self.target = b"entity"
                else:
                    self.state = 1 if character == ord("<") else 0
                    continue
                self.target_offset = 1
                self.state = 3
                continue
            assert self.target is not None
            if _ascii_lower(character) != self.target[self.target_offset]:
                self.state = 1 if character == ord("<") else 0
                self.target = None
                self.target_offset = 0
                continue
            self.target_offset += 1
            if self.target_offset == len(self.target):
                return True
        return False


def _ascii_lower(character: int) -> int:
    """Lowercase one ASCII byte without treating non-ASCII XML data as markup."""

    if ord("A") <= character <= ord("Z"):
        return character + (ord("a") - ord("A"))
    return character


def _is_xml_whitespace(value: str | None) -> bool:
    """Return whether character data is absent or consists only of XML whitespace."""

    return value is None or all(character in " \t\r\n" for character in value)


def _decoded_record(ordinal: int, value: Any, limits: CaptureLimits) -> DecodedRecord:
    """Validate one raw object as a bounded flat scalar source record."""

    if ordinal > limits.maximum_records:
        raise CaptureError("source stream exceeds the record limit")
    if not isinstance(value, Mapping):
        raise CaptureError(f"record {ordinal} must be an object")
    if len(value) > limits.maximum_fields_per_record:
        raise CaptureError(f"record {ordinal} exceeds the field limit")
    values_by_label: dict[str, Scalar] = {}
    for raw_label, raw_scalar in value.items():
        label = _validated_source_label(raw_label)
        if label in values_by_label:
            raise CaptureError(f"record {ordinal} contains duplicate label")
        values_by_label[label] = _validated_scalar(raw_scalar, ordinal)
    if _record_size(values_by_label) > limits.maximum_record_bytes:
        raise CaptureError(f"record {ordinal} exceeds the byte limit")
    return DecodedRecord(ordinal=ordinal, values=MappingProxyType(values_by_label))


def _validated_source_label(value: Any) -> str:
    """Require a bounded, printable source label for deferred alias mapping."""

    if not isinstance(value, str) or not value:
        raise CaptureError("source record labels must be non-empty strings")
    if len(_utf8_bytes(value, "source record label")) > _MAX_SOURCE_LABEL_BYTES:
        raise CaptureError("source record label exceeds the byte limit")
    if not value.isprintable():
        raise CaptureError("source record label must be printable text")
    return value


def _validated_scalar(value: Any, ordinal: int) -> Scalar:
    """Accept only scalar JSON-compatible source values before field mapping."""

    if isinstance(value, str):
        _utf8_bytes(value, f"record {ordinal} scalar")
        return value
    if value is None or isinstance(value, (int, Decimal, bool)):
        return value
    raise CaptureError(f"record {ordinal} contains a nested or unsupported scalar value")


def _record_size(record: Mapping[str, Scalar]) -> int:
    """Estimate canonical scalar record bytes for the per-record decoder limit."""

    return sum(
        len(_utf8_bytes(label, "source record label")) + _scalar_size(value) + 4 for label, value in record.items()
    )


def _scalar_size(value: Scalar) -> int:
    """Measure a scalar's stable UTF-8 representation without serializing records."""

    if value is None:
        return 4
    if isinstance(value, bool):
        return 4 if value else 5
    return len(_utf8_bytes(str(value), "source record scalar"))


def _utf8_bytes(value: str, label: str) -> bytes:
    """Encode one retained source string or fail with the capture error contract."""

    try:
        return value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise CaptureError(f"{label} must be valid UTF-8") from exc
