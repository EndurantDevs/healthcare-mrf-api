# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary contracts for canonical PTG values and retained artifact streams."""

from __future__ import annotations

import datetime
import io
import struct
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace

import pytest

from process.ptg_parts import artifact_streams, canonical


class _EvidenceKind(Enum):
    RATE = "rate"


@dataclass(frozen=True)
class _CanonicalEvidence:
    negotiated_rate: Decimal
    expiration_date: datetime.date
    modifiers: tuple[str, ...]
    evidence_kind: _EvidenceKind


def test_canonical_values_preserve_types_and_reject_ambiguous_money(monkeypatch):
    """Canonical hashes must not silently accept lossy or non-finite money."""

    assert canonical.normalize_money("  ") is None
    assert canonical.normalize_money(Decimal("12.3400")) == "12.34"
    assert canonical.money_number("12.34") == 12.34
    assert canonical.money_number("12") == 12
    assert canonical.money_number(None) is None
    assert canonical.money_number(object()) is None

    with pytest.raises(TypeError, match="Unsupported money"):
        canonical.normalize_money(object())
    with pytest.raises(ValueError, match="finite"):
        canonical.normalize_money(Decimal("NaN"))

    evidence = _CanonicalEvidence(
        negotiated_rate=Decimal("12.3400"),
        expiration_date=datetime.date(2026, 7, 31),
        modifiers=("TC", "26"),
        evidence_kind=_EvidenceKind.RATE,
    )
    assert canonical.canonical_json_dumps(evidence) == (
        '{"evidence_kind":"rate","expiration_date":"2026-07-31",'
        '"modifiers":["26","TC"],"negotiated_rate":"12.34"}'
    )
    assert canonical.semantic_sha256({"value": 1}) == canonical.sha256_bytes(
        b'{"value":1}'
    )

    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "sha256")
    assert len(canonical.semantic_hash(evidence)) == 64
    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", "blake2")
    assert len(canonical.semantic_hash(evidence)) == 32


def test_canonical_dates_and_months_fail_closed(monkeypatch):
    """Date adapters preserve calendar precision and reject blank periods."""

    timestamp = datetime.datetime(2026, 7, 29, 23, 59)
    day = datetime.date(2026, 7, 29)
    assert canonical.normalize_date(timestamp) == "2026-07-29"
    assert canonical.normalize_date(day) == "2026-07-29"
    assert canonical.normalize_date(" ") is None
    assert canonical.normalize_date("July 29, 2026") == "2026-07-29"
    with pytest.raises(ValueError, match="Invalid date"):
        canonical.normalize_date("not-a-date")

    monkeypatch.setattr(canonical, "parse_date", lambda _value: day)
    assert canonical.normalize_date("fallback-date") == "2026-07-29"

    assert canonical.normalize_import_month(timestamp) == datetime.date(2026, 7, 1)
    assert canonical.normalize_import_month(day) == datetime.date(2026, 7, 1)
    with pytest.raises(ValueError, match="cannot be blank"):
        canonical.normalize_import_month(" ")


def test_source_url_normalization_requires_complete_asr_coordinates():
    """ASR wrappers are rewritten only when every target coordinate exists."""

    complete = (
        "https://www.asrhealthbenefits.com/home/umbraco/surface/mrfdownload/index"
        "?groupNumber=group&fileId=file&fileType=in-network"
    )
    assert canonical.normalize_tic_source_url(complete) == (
        "https://www.asrhealthbenefits.com/umbraco/surface/mrfdownload"
        "?groupNumber=group&fileType=in-network&fileId=file"
    )

    incomplete = (
        "https://www.asrhealthbenefits.com/home/umbraco/surface/mrfdownload/index"
        "?groupNumber=group&fileId=file"
    )
    assert canonical.normalize_tic_source_url(incomplete) == incomplete
    assert canonical.canonicalize_url("HTTPS://EXAMPLE.TEST:443") == (
        "https://example.test/"
    )
    assert canonical.canonicalize_url("http://example.test:80/path") == (
        "http://example.test/path"
    )


def test_bounded_reader_enforces_zero_read_readall_and_readinto_contracts():
    """Every read shape must share the same decompressed-byte limit."""

    source = io.BytesIO(b"abcd")
    reader = artifact_streams._DecompressedByteLimitReader(
        source,
        limit=4,
        label="rates.json",
    )
    assert reader.is_readable()
    assert reader.read(0) == b""
    target = bytearray(2)
    assert reader.readinto(target) == 2
    assert bytes(target) == b"ab"
    assert reader.read() == b"cd"

    oversized = artifact_streams._DecompressedByteLimitReader(
        io.BytesIO(b"abc"),
        limit=2,
        label="oversized.json",
    )
    with pytest.raises(
        artifact_streams.DecompressedArtifactTooLargeError,
        match="oversized.json",
    ):
        oversized.read()


def test_bom_reader_preserves_non_bom_prefix_across_short_reads():
    """Prefix probing must neither consume nor reorder ordinary JSON bytes."""

    reader = artifact_streams._Utf8BomSkippingReader(io.BytesIO(b"abcdef"))
    assert reader.is_readable()
    assert reader.read(2) == b"ab"
    assert reader.read(2) == b"cd"
    target = bytearray(2)
    assert reader.readinto(target) == 2
    assert bytes(target) == b"ef"


def test_zip_headers_and_empty_archives_fail_closed(tmp_path):
    """Malformed or empty ZIP containers cannot be treated as logical JSON."""

    missing = tmp_path / "missing.json"
    assert artifact_streams._is_raw_file_gzip(missing) is False

    empty_zip = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty_zip, "w") as empty_zip_ref:
        assert empty_zip_ref.namelist() == []
    assert artifact_streams._first_zip_member(empty_zip) is None
    with pytest.raises(RuntimeError, match="No file members"):
        with artifact_streams.open_json_artifact_stream(empty_zip) as json_stream:
            json_stream.read()
    with pytest.raises(RuntimeError, match="No file members"):
        artifact_streams.stream_logical_artifact(empty_zip)

    info = SimpleNamespace(header_offset=0, filename="rates.json")
    with pytest.raises(RuntimeError, match="Invalid zip local header"):
        artifact_streams._zip_member_payload_offset(io.BytesIO(b"short"), info)

    invalid_header = struct.pack("<IHHHHHIIIHH", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    with pytest.raises(RuntimeError, match="signature"):
        artifact_streams._zip_member_payload_offset(
            io.BytesIO(invalid_header),
            info,
        )


def _deflate_reader(
    *,
    remaining: int = 0,
    expected_size: int = 0,
    expected_crc: int = 0,
    inflater=None,
):
    reader = artifact_streams._Deflate64ZipMemberReader.__new__(
        artifact_streams._Deflate64ZipMemberReader
    )
    reader._raw_fp = io.BytesIO()
    reader._remaining = remaining
    reader._chunk_size = 8
    reader._inflater = inflater or SimpleNamespace(eof=True, inflate=lambda _data: b"")
    reader._buffer = bytearray()
    reader._flushed = False
    reader._filename = "rates.json"
    reader._expected_size = expected_size
    reader._expected_crc = expected_crc
    reader._decompressed_size = 0
    reader._crc = 0
    return reader


def test_deflate64_metadata_and_truncation_checks_fail_closed(monkeypatch):
    """Deflate64 readers authenticate package, encryption, size, EOF, and CRC."""

    monkeypatch.setattr(artifact_streams, "inflate64", None)
    with pytest.raises(RuntimeError, match="require the inflate64 package"):
        artifact_streams._Deflate64ZipMemberReader(
            io.BytesIO(),
            SimpleNamespace(flag_bits=0, filename="rates.json"),
        )

    fake_module = SimpleNamespace(Inflater=lambda: SimpleNamespace())
    monkeypatch.setattr(artifact_streams, "inflate64", fake_module)
    with pytest.raises(RuntimeError, match="Encrypted"):
        artifact_streams._Deflate64ZipMemberReader(
            io.BytesIO(),
            SimpleNamespace(flag_bits=1, filename="rates.json"),
        )

    oversized = _deflate_reader(expected_size=1)
    assert oversized.is_readable()
    assert oversized.read(0) == b""
    with pytest.raises(zipfile.BadZipFile, match="exceeds declared size"):
        oversized._record_inflated(b"ab")
    with pytest.raises(EOFError, match="Truncated Deflate64 compressed data"):
        _deflate_reader(remaining=1)._inflate_next()

    early_eof = _deflate_reader(remaining=2)
    early_eof._raw_fp = io.BytesIO(b"x")
    early_eof._chunk_size = 1
    with pytest.raises(zipfile.BadZipFile, match="ended before"):
        early_eof._inflate_next()

    with pytest.raises(EOFError, match="Truncated Deflate64 compressed data"):
        _deflate_reader(remaining=1)._finish()

    no_eof = SimpleNamespace(eof=False, inflate=lambda _data: b"")
    with pytest.raises(EOFError, match="Truncated Deflate64 stream"):
        _deflate_reader(inflater=no_eof)._finish()

    with pytest.raises(zipfile.BadZipFile, match="has size"):
        _deflate_reader(expected_size=1)._finish()

    bad_crc = _deflate_reader(expected_size=1, expected_crc=1)
    bad_crc._record_inflated(b"a")
    with pytest.raises(zipfile.BadZipFile, match="Bad CRC"):
        bad_crc._finish()


def test_deflate64_read_shapes_preserve_buffer_and_tail_order():
    """Bounded and full reads emit buffered, inflated, then terminal bytes."""

    readall = _deflate_reader(remaining=2)
    readall._buffer.extend(b"a")
    inflated_chunks = iter((b"b", b""))

    def inflate_next():
        readall._remaining -= 1
        return next(inflated_chunks)

    readall._inflate_next = inflate_next
    readall._finish = lambda: b"c"
    assert readall.read() == b"abc"

    empty_tail = _deflate_reader(remaining=0)
    empty_tail._buffer.extend(b"a")
    empty_tail._finish = lambda: b""
    assert empty_tail.read() == b"a"

    bounded = _deflate_reader(remaining=1)

    def inflate_bounded():
        bounded._remaining = 0
        return b"x"

    bounded._inflate_next = inflate_bounded
    bounded._finish = lambda: b"y"
    assert bounded.read(2) == b"xy"
