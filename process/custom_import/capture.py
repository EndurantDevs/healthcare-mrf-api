# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Replayable, bounded capture and record decoding for custom-import streams.

The capture boundary accepts a binary stream, never a path or an arbitrary
command.  It seals the exact acquired payload with content digests before any
field mapping occurs.  Decoders expose records one at a time and keep source
labels intact for the later declarative mapping stage.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
import codecs
import csv
import gzip
import hashlib
from io import BytesIO, TextIOWrapper
import json
import threading
from types import MappingProxyType
from typing import Any, BinaryIO
import xml.etree.ElementTree as ElementTree

from ijson import common as ijson_common
from ijson.backends import python as ijson_python

from process.custom_import.definition import CONTRACT_VERSION, SourceStream


_DEFAULT_READ_CHUNK_BYTES = 64 * 1024
_MAX_SNAPSHOT_TOKEN_BYTES = 1024
_MAX_SOURCE_LABEL_BYTES = 255
_CSV_FIELD_SIZE_LOCK = threading.Lock()

Scalar = str | int | Decimal | bool | None


class CaptureError(ValueError):
    """A source payload is malformed, exceeds limits, or fails replay checks."""


@dataclass(frozen=True)
class CaptureLimits:
    """Resource limits applied before custom-import source data is admitted."""

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


class _DuplicateKeyDict(dict[str, Any]):
    """Record duplicate JSON keys without raising from a parser callback."""

    duplicate_key: str | None

    def __init__(self) -> None:
        """Initialize an empty mapping with no duplicate-key observation."""

        super().__init__()
        self.duplicate_key = None

    def __setitem__(self, key: str, value: Any) -> None:
        """Retain a first value and mark a duplicate for later boundary validation."""

        if key in self:
            self.duplicate_key = key
            return
        super().__setitem__(key, value)


def capture_stream(
    acquired_source: BinaryIO,
    source_stream: SourceStream,
    *,
    source_snapshot_token: str,
    limits: CaptureLimits = CaptureLimits(),
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
    limits: CaptureLimits = CaptureLimits(),
) -> None:
    """Verify that a replay payload still matches its stream and sealed manifest."""

    manifest = capture.manifest
    if (
        manifest.stream_id != stream.stream_id
        or manifest.format != stream.format
        or manifest.compression != stream.compression
        or manifest.stream_sha256 != _source_stream_sha256(stream)
    ):
        raise CaptureError("capture manifest does not belong to the declared source stream")
    token = _validated_snapshot_token(manifest.source_snapshot_token)
    if not isinstance(capture.payload, bytes):
        raise CaptureError("sealed capture payload must be bytes")
    if len(capture.payload) != manifest.compressed_bytes:
        raise CaptureError("capture payload length does not match the sealed manifest")
    compressed_sha256 = hashlib.sha256(capture.payload).hexdigest()
    if compressed_sha256 != manifest.compressed_sha256:
        raise CaptureError("capture payload digest does not match the sealed manifest")
    decoded_bytes, decoded_sha256 = _decoded_metrics(capture.payload, stream.compression, limits)
    if decoded_bytes != manifest.decoded_bytes or decoded_sha256 != manifest.decoded_sha256:
        raise CaptureError("decoded capture digest does not match the sealed manifest")
    if _capture_sha256(
        stream=stream,
        source_snapshot_token=token,
        stream_sha256=manifest.stream_sha256,
        compressed_sha256=compressed_sha256,
        decoded_sha256=decoded_sha256,
    ) != manifest.capture_sha256:
        raise CaptureError("capture manifest digest does not match its provenance")


def iter_records(
    capture: SealedCapture,
    stream: SourceStream,
    *,
    limits: CaptureLimits = CaptureLimits(),
) -> Iterator[DecodedRecord]:
    """Yield validated source-label records from a verified sealed capture."""

    verify_capture(capture, stream, limits=limits)
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
        raise CaptureError("parquet decoding is not enabled for custom-import/v1 captures")
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
    except (EOFError, OSError) as exc:
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

    material = "\x00".join(
        (
            CONTRACT_VERSION,
            "capture",
            stream.stream_id,
            stream.format,
            stream.compression,
            source_snapshot_token,
            stream_sha256,
            compressed_sha256,
            decoded_sha256,
        )
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
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise CaptureError("source snapshot token contains control characters")
    return value


def _iter_delimited_records(
    payload: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode a headered CSV or TSV source one row at a time."""

    try:
        with _open_decoded_payload(payload, stream.compression) as binary:
            with TextIOWrapper(binary, encoding="utf-8", newline="") as text:
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

    with _csv_field_limit(limits.maximum_record_bytes):
        reader = csv.reader(text, delimiter=delimiter)
        source_labels = _validated_headers(next(reader, None), limits)
        for ordinal, row in enumerate(reader, start=1):
            if len(row) != len(source_labels):
                raise CaptureError(f"record {ordinal} field count does not match its header")
            yield _decoded_record(ordinal, dict(zip(source_labels, row, strict=True)), limits)


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
    """Decode a top-level JSON array through the incremental JSON parser."""

    _require_json_array(payload, stream.compression)
    try:
        with _open_decoded_payload(payload, stream.compression) as binary:
            for ordinal, record in enumerate(
                ijson_python.items(binary, "item", map_type=_DuplicateKeyDict, use_float=False),
                start=1,
            ):
                yield _decoded_record(ordinal, record, limits)
    except (ijson_common.JSONError, UnicodeDecodeError) as exc:
        raise CaptureError("JSON source payload is invalid") from exc


def _require_json_array(payload: bytes, compression: str) -> None:
    """Reject JSON documents that are not one top-level record array."""

    try:
        with _open_decoded_payload(payload, compression) as binary:
            parser = ijson_python.parse(binary, use_float=False)
            prefix, event, _value = next(parser)
    except StopIteration as exc:
        raise CaptureError("JSON source payload is empty") from exc
    except (ijson_common.JSONError, UnicodeDecodeError) as exc:
        raise CaptureError("JSON source payload is invalid") from exc
    if prefix != "" or event != "start_array":
        raise CaptureError("JSON source payload must be a top-level array")


def _iter_ndjson_records(
    payload: bytes,
    stream: SourceStream,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Decode one strict JSON object per non-empty NDJSON line."""

    ordinal = 0
    try:
        with _open_decoded_payload(payload, stream.compression) as binary:
            while raw_line := binary.readline(limits.maximum_record_bytes + 1):
                if len(raw_line) > limits.maximum_record_bytes:
                    raise CaptureError("NDJSON record exceeds the byte limit")
                line = raw_line.strip()
                if not line:
                    continue
                ordinal += 1
                try:
                    value = json.loads(
                        line.decode("utf-8"),
                        object_pairs_hook=_strict_json_object,
                        parse_float=Decimal,
                        parse_constant=_reject_json_constant,
                    )
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                    raise CaptureError(f"NDJSON record {ordinal} is invalid") from exc
                yield _decoded_record(ordinal, value, limits)
    except (EOFError, OSError) as exc:
        raise CaptureError("source stream compression is invalid") from exc


def _strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while denying duplicate labels."""

    values_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in values_by_key:
            raise CaptureError(f"duplicate JSON object key: {key}")
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
    """Decode simple direct-child XML records while denying entity declarations."""

    record_tag = stream.record_path
    if record_tag is None:
        raise CaptureError("XML streams require a declared record path")
    if _has_forbidden_xml_declaration(captured_bytes, stream.compression, limits):
        raise CaptureError("XML source payload cannot contain entity declarations")
    try:
        with _open_decoded_payload(captured_bytes, stream.compression) as binary:
            with TextIOWrapper(binary, encoding="utf-8") as text:
                yield from _iter_xml_text_records(text, record_tag, limits)
    except (ElementTree.ParseError, UnicodeDecodeError) as exc:
        raise CaptureError("XML source payload is invalid") from exc


def _iter_xml_text_records(
    text: TextIOWrapper,
    record_tag: str,
    limits: CaptureLimits,
) -> Iterator[DecodedRecord]:
    """Stream direct children of one XML root while clearing completed records."""

    depth = 0
    ordinal = 0
    root: ElementTree.Element | None = None
    for event, element in ElementTree.iterparse(text, events=("start", "end")):
        if event == "start":
            depth += 1
            root = element if depth == 1 else root
            continue
        if _is_xml_record_end(event, depth, element, record_tag):
            ordinal += 1
            yield _decoded_record(ordinal, _xml_record_values(element), limits)
            _clear_xml_root(root)
        depth -= 1


def _is_xml_record_end(
    event: str,
    depth: int,
    element: ElementTree.Element,
    record_tag: str,
) -> bool:
    """Identify one complete direct-child record and reject shape drift immediately."""

    if event != "end" or depth != 2:
        return False
    if element.tag != record_tag:
        raise CaptureError("XML source payload has an unexpected root child")
    return True


def _clear_xml_root(root: ElementTree.Element | None) -> None:
    """Discard completed direct children so XML memory remains bounded by one record."""

    if root is None:
        raise CaptureError("XML source payload has no root element")
    root.clear()


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
    except (EOFError, OSError) as exc:
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


def _xml_record_values(element: ElementTree.Element) -> dict[str, str]:
    """Convert a simple XML record element into an exact source-label mapping."""

    if element.attrib:
        raise CaptureError("XML record elements cannot carry attributes")
    values_by_label: dict[str, str] = {}
    for child in element:
        if not isinstance(child.tag, str) or child.attrib or list(child):
            raise CaptureError("XML records must contain flat scalar child elements")
        label = _validated_source_label(child.tag)
        if label in values_by_label:
            raise CaptureError(f"XML record contains duplicate label: {label}")
        values_by_label[label] = child.text or ""
    return values_by_label


def _decoded_record(ordinal: int, value: Any, limits: CaptureLimits) -> DecodedRecord:
    """Validate one raw object as a bounded flat scalar source record."""

    if ordinal > limits.maximum_records:
        raise CaptureError("source stream exceeds the record limit")
    if isinstance(value, _DuplicateKeyDict) and value.duplicate_key is not None:
        raise CaptureError(f"duplicate JSON object key: {value.duplicate_key}")
    if not isinstance(value, Mapping):
        raise CaptureError(f"record {ordinal} must be an object")
    if len(value) > limits.maximum_fields_per_record:
        raise CaptureError(f"record {ordinal} exceeds the field limit")
    values_by_label: dict[str, Scalar] = {}
    for raw_label, raw_scalar in value.items():
        label = _validated_source_label(raw_label)
        if label in values_by_label:
            raise CaptureError(f"record {ordinal} contains duplicate label: {label}")
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
        len(_utf8_bytes(label, "source record label")) + _scalar_size(value) + 4
        for label, value in record.items()
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
