from __future__ import annotations

import hashlib
import json
import os
import stat
import struct
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from process.ptg_parts import (
    ptg2_source_witness_locator_materialize as materialize,
)
from process.ptg_parts import ptg2_source_witness_locator_reader as reader
from process.ptg_parts import ptg2_source_witness_store as witness_store
from process.ptg_parts import (
    ptg2_source_witness_streaming_encode as streaming,
)
from process.ptg_parts.ptg2_shared_blocks import SharedLayoutBuildOwnership
from process.ptg_parts.ptg2_source_witness_contract import (
    CompressedSourceWitnessRecord,
    SourceWitnessBundleIdentity,
    SourceWitnessEvidenceLocator,
    SourceWitnessRecordLocator,
    WitnessPayloadLimitError,
)
from process.ptg_parts.ptg2_source_witness_persisted_encode import (
    SourceWitnessPayloadCounts,
)
from tests.test_ptg2_source_witness import SOURCE_A, _record
from tests.test_ptg2_source_witness_dictionary_bundle import (
    _write_dictionary_bundle,
)


U32 = struct.Struct(">I")


def _identity(path: Path) -> SourceWitnessBundleIdentity:
    file_stat = path.stat()
    return SourceWitnessBundleIdentity(
        path=str(path),
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        byte_count=file_stat.st_size,
        device=file_stat.st_dev,
        inode=file_stat.st_ino,
        mtime_ns=file_stat.st_mtime_ns,
    )


def _dummy_identity(*, byte_count: int = 1024) -> SourceWitnessBundleIdentity:
    return SourceWitnessBundleIdentity(
        path="/unused",
        sha256="a" * 64,
        byte_count=byte_count,
        device=1,
        inode=2,
        mtime_ns=3,
    )


def _dummy_locator(
    *,
    bundle: SourceWitnessBundleIdentity | None = None,
    evidence_by_sha256: dict[str, SourceWitnessEvidenceLocator] | None = None,
) -> SourceWitnessRecordLocator:
    return SourceWitnessRecordLocator(
        kind="rate_occurrence",
        priority=1,
        tie_breaker="b" * 64,
        raw_source_sha256=SOURCE_A,
        raw_sha256="c" * 64,
        linked_provider_sha256=None,
        bundle=bundle or _dummy_identity(),
        offset=0,
        length=1,
        compressed_sha256=hashlib.sha256(b"x").hexdigest(),
        evidence_by_sha256=evidence_by_sha256 or {},
    )


def _payload_counts(record_count: int = 1) -> SourceWitnessPayloadCounts:
    return SourceWitnessPayloadCounts(
        source_count=1,
        source_digest="d" * 64,
        occurrence_population=record_count,
        provider_population=0,
        emitted_rate_rows=record_count,
        unqueryable_rate_rows=0,
        occurrence_count=record_count,
        provider_count=0,
        record_count=record_count,
    )


def _one_locator(tmp_path: Path) -> SourceWitnessRecordLocator:
    bundle_entry = _write_dictionary_bundle(
        tmp_path,
        [
            _record(
                kind="rate_occurrence",
                priority=1,
                item_ordinal=1,
                raw_json=b'{"rate":1}',
                linked_provider_json=None,
            )
        ],
    )
    _header, locators = reader.read_scanner_bundle_locators(bundle_entry)
    assert isinstance(locators[0], SourceWitnessRecordLocator)
    return locators[0]


def test_bundle_open_failures_are_wrapped_and_close_owned_descriptor(
    monkeypatch,
) -> None:
    monkeypatch.setattr(reader.os, "open", lambda *_args: (_ for _ in ()).throw(OSError("no")))
    with pytest.raises(RuntimeError, match="bundle is missing"):
        reader._open_bundle_file(Path("/missing"))

    closed_descriptors: list[int] = []
    monkeypatch.setattr(reader.os, "open", lambda *_args: 73)
    monkeypatch.setattr(
        reader.os,
        "fdopen",
        lambda *_args: (_ for _ in ()).throw(ValueError("bad mode")),
    )
    monkeypatch.setattr(reader.os, "close", closed_descriptors.append)
    with pytest.raises(ValueError, match="bad mode"):
        reader._open_bundle_file(Path("/unused"))
    assert closed_descriptors == [73]


def test_reader_rejects_invalid_stats_and_stream_size_changes() -> None:
    regular_mode = stat.S_IFREG | 0o600
    base_stat_by_field = {
        "st_mode": regular_mode,
        "st_size": 3,
        "st_dev": 1,
        "st_ino": 2,
        "st_mtime_ns": 3,
    }
    bundle_entry_by_field = {"byte_count": 3}
    for changes in (
        {"st_mode": stat.S_IFDIR | 0o700},
        {"st_size": 0},
        {"st_size": reader.PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES + 1},
    ):
        with pytest.raises(RuntimeError, match="bundle size"):
            reader._validate_bundle_stat(
                bundle_entry_by_field,
                SimpleNamespace(**(base_stat_by_field | changes)),
            )
    with pytest.raises(RuntimeError, match="byte count"):
        reader._validate_bundle_stat(
            {"byte_count": 4},
            SimpleNamespace(**base_stat_by_field),
        )
    with pytest.raises(RuntimeError, match="changed while reading"):
        reader._stream_authenticated_digest(BytesIO(b"ab"), bundle_size=3)
    with pytest.raises(RuntimeError, match="changed while reading"):
        reader._stream_authenticated_digest(BytesIO(b"abcd"), bundle_size=3)
    assert reader._stream_authenticated_digest(BytesIO(b"abc"), bundle_size=3) == (
        hashlib.sha256(b"abc").hexdigest()
    )


def test_authenticated_reader_rejects_digest_magic_and_midread_stat_changes(
    tmp_path,
    monkeypatch,
) -> None:
    missing_bundle_entry_by_field = {
        "path": str(tmp_path / "missing"),
        "sha256": "0" * 64,
        "byte_count": 1,
    }
    with pytest.raises(RuntimeError, match="bundle is missing"):
        with reader._authenticated_bundle_file(missing_bundle_entry_by_field):
            pytest.fail("missing bundle unexpectedly authenticated")

    path = tmp_path / "bundle.bin"
    path.write_bytes(b"PTG2SW03payload")
    bundle_entry_by_field = {
        "path": str(path),
        "sha256": "0" * 64,
        "byte_count": path.stat().st_size,
    }
    with pytest.raises(RuntimeError, match="digest does not match"):
        with reader._authenticated_bundle_file(bundle_entry_by_field):
            pytest.fail("bundle with wrong digest unexpectedly authenticated")

    path.write_bytes(b"PTG2SW99payload")
    bundle_entry_by_field["byte_count"] = path.stat().st_size
    bundle_entry_by_field["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="magic is invalid"):
        with reader._authenticated_bundle_file(bundle_entry_by_field):
            pytest.fail("bundle with wrong magic unexpectedly authenticated")

    path.write_bytes(b"PTG2SW03payload")
    bundle_entry_by_field["byte_count"] = path.stat().st_size
    bundle_entry_by_field["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    actual_stat = path.stat()
    changed_stat = SimpleNamespace(
        st_mode=actual_stat.st_mode,
        st_size=actual_stat.st_size,
        st_dev=actual_stat.st_dev,
        st_ino=actual_stat.st_ino,
        st_mtime_ns=actual_stat.st_mtime_ns + 1,
    )
    file_stats = iter((actual_stat, changed_stat))
    monkeypatch.setattr(reader.os, "fstat", lambda _descriptor: next(file_stats))
    with pytest.raises(RuntimeError, match="changed while reading"):
        with reader._authenticated_bundle_file(bundle_entry_by_field):
            pytest.fail("mutated bundle unexpectedly authenticated")


@pytest.mark.parametrize("byte_count", [-1, 2])
def test_exact_file_reader_rejects_invalid_or_truncated_reads(byte_count: int) -> None:
    with pytest.raises(RuntimeError):
        reader._read_exact_file(BytesIO(b"x"), byte_count, field_name="field")


@pytest.mark.parametrize(
    "row_count",
    [True, -1, reader.PTG2_V3_SOURCE_WITNESS_TOTAL_TARGET + 1],
)
def test_bundle_row_count_is_strictly_bounded(row_count: object) -> None:
    with pytest.raises(RuntimeError, match="row count"):
        reader._validated_entry_row_count({"row_count": row_count})


def test_dictionary_locator_framing_rejects_counts_duplicates_and_missing_evidence() -> None:
    with pytest.raises(RuntimeError, match="dictionary count"):
        reader._read_dictionary_evidence_locators(
            BytesIO(U32.pack(2)),
            bundle_identity=_dummy_identity(),
            maximum_evidence_count=1,
        )

    malformed = U32.pack(1) + b"a" * 32 + U32.pack(0) + U32.pack(1) + b"x"
    with pytest.raises(RuntimeError, match="dictionary framing"):
        reader._read_dictionary_evidence_locators(
            BytesIO(malformed),
            bundle_identity=_dummy_identity(),
            maximum_evidence_count=1,
        )

    entry = b"a" * 32 + U32.pack(1) + U32.pack(1) + b"x"
    with pytest.raises(RuntimeError, match="dictionary framing"):
        reader._read_dictionary_evidence_locators(
            BytesIO(U32.pack(2) + entry + entry),
            bundle_identity=_dummy_identity(),
            maximum_evidence_count=2,
        )

    with pytest.raises(RuntimeError, match="evidence is missing"):
        reader._record_evidence_locator_map(
            raw_sha256="a" * 64,
            linked_provider_sha256=None,
            evidence_locator_by_sha256={},
        )


def test_dictionary_record_locator_rejects_count_and_frame_bounds() -> None:
    with pytest.raises(RuntimeError, match="record count does not match"):
        reader._read_dictionary_record_locators(
            BytesIO(U32.pack(2)),
            bundle_identity=_dummy_identity(),
            raw_source_sha256=SOURCE_A,
            expected_record_count=1,
            evidence_locator_by_sha256={},
        )
    with pytest.raises(RuntimeError, match="record framing"):
        reader._read_dictionary_record_locators(
            BytesIO(U32.pack(1) + U32.pack(0)),
            bundle_identity=_dummy_identity(),
            raw_source_sha256=SOURCE_A,
            expected_record_count=1,
            evidence_locator_by_sha256={},
        )


def test_current_bundle_rejects_bad_header_and_trailing_bytes(tmp_path) -> None:
    payload = b"PTG2SW03" + U32.pack(0)
    with pytest.raises(RuntimeError, match="header is truncated"):
        reader._read_current_dictionary_bundle(
            BytesIO(payload),
            replace(_dummy_identity(), byte_count=len(payload)),
            {"row_count": 0},
        )

    entry = _write_dictionary_bundle(
        tmp_path,
        [
            _record(
                kind="rate_occurrence",
                priority=1,
                item_ordinal=1,
                raw_json=b'{"rate":1}',
            )
        ],
    )
    path = Path(str(entry["path"]))
    path.write_bytes(path.read_bytes() + b"x")
    entry["byte_count"] = path.stat().st_size
    entry["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="record count does not match"):
        reader.read_scanner_bundle_locators(entry)


def test_locator_reader_legacy_fallback_and_defensive_magic_tail(
    tmp_path,
    monkeypatch,
) -> None:
    expected = ({"format_version": 2}, [])

    @contextmanager
    def legacy_context(_entry):
        yield BytesIO(), _dummy_identity(), reader.SOURCE_BUNDLE_MAGIC

    monkeypatch.setattr(reader, "_authenticated_bundle_file", legacy_context)
    monkeypatch.setattr(reader, "read_scanner_bundle", lambda _entry: expected)
    assert reader.read_scanner_bundle_locators({}) == expected

    @contextmanager
    def impossible_context(_entry):
        yield BytesIO(), _dummy_identity(), b"PTG2SW99"

    monkeypatch.setattr(reader, "_authenticated_bundle_file", impossible_context)
    with pytest.raises(RuntimeError, match="magic is truncated"):
        reader.read_scanner_bundle_locators({})


def test_materialization_file_identity_and_open_failures(tmp_path, monkeypatch) -> None:
    path = tmp_path / "bundle"
    path.write_bytes(b"x")
    identity = _identity(path)

    wrong = replace(identity, inode=identity.inode + 1)
    with pytest.raises(RuntimeError, match="changed before"):
        with materialize._materialization_bundle_file(wrong):
            pytest.fail("bundle with changed identity unexpectedly opened")

    monkeypatch.setattr(
        materialize,
        "_open_bundle_file",
        lambda _path: (_ for _ in ()).throw(RuntimeError("no follow")),
    )
    with pytest.raises(RuntimeError, match="changed before"):
        with materialize._materialization_bundle_file(identity):
            pytest.fail("bundle open failure unexpectedly recovered")


def test_materialization_detects_final_stat_change(tmp_path, monkeypatch) -> None:
    path = tmp_path / "bundle"
    path.write_bytes(b"x")
    identity = _identity(path)
    actual_stat = path.stat()
    changed_stat = SimpleNamespace(
        st_mode=actual_stat.st_mode,
        st_size=actual_stat.st_size,
        st_dev=actual_stat.st_dev,
        st_ino=actual_stat.st_ino,
        st_mtime_ns=actual_stat.st_mtime_ns + 1,
    )
    file_stats = iter((actual_stat, changed_stat))
    monkeypatch.setattr(
        materialize.os,
        "fstat",
        lambda _descriptor: next(file_stats),
    )
    with pytest.raises(RuntimeError, match="changed during"):
        with materialize._materialization_bundle_file(identity):
            pytest.fail("bundle mutation unexpectedly went undetected")


@pytest.mark.parametrize(
    ("offset", "length"),
    [(-1, 1), (0, 0), (0, materialize.PTG2_V3_SOURCE_WITNESS_MAX_RECORD_BYTES + 1), (2, 1)],
)
def test_materialization_locator_bounds_are_strict(offset: int, length: int) -> None:
    with pytest.raises(RuntimeError, match="locator is invalid"):
        materialize._read_locator_payload(
            BytesIO(b"x"),
            _dummy_identity(byte_count=2),
            offset=offset,
            length=length,
            field_name="record",
        )
    with pytest.raises(RuntimeError, match="is truncated"):
        materialize._read_locator_payload(
            BytesIO(b"x"),
            _dummy_identity(byte_count=2),
            offset=0,
            length=2,
            field_name="record",
        )


def test_evidence_locator_maps_reject_key_and_coordinate_inconsistency() -> None:
    first = SourceWitnessEvidenceLocator("a" * 64, 1, 0, 1)
    with pytest.raises(RuntimeError, match="locator is invalid"):
        materialize._evidence_locator_map(
            [(0, _dummy_locator(evidence_by_sha256={"b" * 64: first}))]
        )

    second = replace(first, offset=1)
    with pytest.raises(RuntimeError, match="locator is inconsistent"):
        materialize._evidence_locator_map(
            [
                (0, _dummy_locator(evidence_by_sha256={first.sha256: first})),
                (1, _dummy_locator(evidence_by_sha256={first.sha256: second})),
            ]
        )


def test_materialized_evidence_digest_consistency(
    monkeypatch,
) -> None:
    """Reject evidence bytes that disagree with authenticated locators."""
    evidence = SourceWitnessEvidenceLocator("a" * 64, 1, 0, 1)
    monkeypatch.setattr(materialize, "_decompress_evidence", lambda *_args: b"x")
    with pytest.raises(RuntimeError, match="evidence digest"):
        materialize._materialized_evidence_map(
            BytesIO(b"z"),
            _dummy_identity(byte_count=1),
            {evidence.sha256: evidence},
        )


def test_materialized_record_locator_consistency(monkeypatch) -> None:
    """Reject record bytes that disagree with authenticated locators."""
    locator = _dummy_locator()
    monkeypatch.setattr(
        materialize,
        "decode_record_locator_fields",
        lambda _record: SimpleNamespace(
            kind="provider_reference",
            priority=1,
            tie_breaker=locator.tie_breaker,
            raw_sha256=locator.raw_sha256,
            linked_provider_sha256=None,
        ),
    )
    with pytest.raises(RuntimeError, match="record locator is inconsistent"):
        materialize._materialize_record_locator(
            BytesIO(b"x"),
            _dummy_identity(byte_count=1),
            locator,
            {},
        )

    monkeypatch.setattr(
        materialize,
        "decode_record_locator_fields",
        lambda _record: SimpleNamespace(
            kind=locator.kind,
            priority=locator.priority,
            tie_breaker=locator.tie_breaker,
            raw_sha256=locator.raw_sha256,
            linked_provider_sha256=None,
        ),
    )
    monkeypatch.setattr(
        materialize,
        "decode_persisted_record",
        lambda *_args, **_kwargs: SimpleNamespace(
            kind=locator.kind,
            priority=locator.priority + 1,
            tie_breaker=locator.tie_breaker,
            raw_sha256=locator.raw_sha256,
            linked_provider_sha256=None,
        ),
    )
    with pytest.raises(RuntimeError, match="record locator is inconsistent"):
        materialize._materialize_record_locator(
            BytesIO(b"x"),
            _dummy_identity(byte_count=1),
            locator,
            {},
        )
