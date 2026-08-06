# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial proofs for the one-shot anonymous projection COPY lease."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
from io import BytesIO
import os
import stat
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from process.ptg_parts import ptg2_tax_identity_source_copy as source_copy
from process.ptg_parts.ptg2_tax_identity_source_copy import (
    _authenticated_projection_copy_stream,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.test_ptg2_tax_identity_source_artifact import _prepare, _record, _sidecar

_ERROR = "ptg2_tax_identity_source_projection_invalid"


def _prepared(tmp_path):
    sidecar = _sidecar(
        tmp_path,
        source_key=0,
        shard_id="file:a",
        identity_digit="8",
        sidecar_records=(_record(1, 1, 8), _record(2, 2)),
    )
    return _prepare(tmp_path, (sidecar,))


def _drain(copy_stream, *, read_size: int = 524_288) -> bytes:
    chunks: list[bytes] = []
    while copy_chunk := copy_stream.read(read_size):
        chunks.append(copy_chunk)
    return b"".join(chunks)


class _AlteredCopyFile:
    def __init__(self, payload: bytes, descriptor: int) -> None:
        altered_payload = bytearray(payload)
        altered_payload[len(altered_payload) // 2] ^= 1
        self._payload = bytes(altered_payload)
        self._descriptor = descriptor
        self._offset = 0

    def read(self, size: int) -> bytes:
        next_offset = min(self._offset + size, len(self._payload))
        chunk = self._payload[self._offset : next_offset]
        self._offset = next_offset
        return chunk

    def fileno(self) -> int:
        return self._descriptor


class _ReadResultFile:
    def __init__(self, result: object) -> None:
        self._result = result

    def read(self, _size: int) -> object:
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


class _CopyReadWrapper:
    def __init__(self, copy_file, read_result: object) -> None:
        self._copy_file = copy_file
        self._read_result = read_result

    def flush(self) -> None:
        self._copy_file.flush()

    def fileno(self) -> int:
        return self._copy_file.fileno()

    def seek(self, offset: int) -> int:
        return self._copy_file.seek(offset)

    def read(self, _size: int) -> object:
        if isinstance(self._read_result, BaseException):
            raise self._read_result
        return self._read_result


class _SeekFailureCopyFile:
    def __init__(self, copy_file) -> None:
        self._copy_file = copy_file

    def fileno(self) -> int:
        return self._copy_file.fileno()

    def seek(self, _offset: int) -> None:
        raise OSError("synthetic seek failure")

    def close(self) -> None:
        self._copy_file.close()


def _metadata_with(metadata, **changes):
    field_names = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_nlink",
        "st_uid",
        "st_gid",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    metadata_by_field = {name: getattr(metadata, name) for name in field_names}
    metadata_by_field.update(changes)
    return SimpleNamespace(**metadata_by_field)


def _anonymous_file(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir(exist_ok=True)
    return source_copy._open_anonymous_projection_copy(scratch_parent)


def test_asyncpg_sized_reads_authenticate_anonymous_bytes_once(tmp_path):
    prepared = _prepared(tmp_path)
    copy_file = prepared._copy_owner._copy_file
    assert copy_file is not None
    metadata = os.fstat(copy_file.fileno())
    assert metadata.st_nlink == 0
    assert stat.S_IMODE(metadata.st_mode) & 0o077 == 0
    assert list((tmp_path / "scratch").iterdir()) == []

    with _authenticated_projection_copy_stream(prepared) as copy_stream:
        copy_bytes = _drain(copy_stream)

    assert len(copy_bytes) == prepared.copy_byte_count
    assert hashlib.sha256(copy_bytes).hexdigest() == prepared.copy_sha256
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared):
            pytest.fail("closed lease was reused")


def test_cleanup_during_copy_defers_close_until_validated_exit(tmp_path):
    prepared = _prepared(tmp_path)

    with _authenticated_projection_copy_stream(prepared) as copy_stream:
        copy_bytes = copy_stream.read(7)
        prepared.cleanup()
        prepared.cleanup()
        copy_bytes += _drain(copy_stream, read_size=11)

    assert len(copy_bytes) == prepared.copy_byte_count
    assert hashlib.sha256(copy_bytes).hexdigest() == prepared.copy_sha256
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared):
            pytest.fail("closed lease was reused")


@pytest.mark.parametrize(
    "primary_error",
    [RuntimeError("copy-consumer-failure"), asyncio.CancelledError()],
)
def test_copy_consumer_failure_remains_primary(tmp_path, primary_error):
    prepared = _prepared(tmp_path)

    with pytest.raises(BaseException) as raised:
        with _authenticated_projection_copy_stream(prepared) as copy_stream:
            assert copy_stream.read(5)
            raise primary_error

    assert raised.value is primary_error
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared):
            pytest.fail("closed lease was reused")


@pytest.mark.skipif(
    not hasattr(os, "pread") or not hasattr(os, "pwrite"),
    reason="descriptor-preserving mutation proof requires POSIX pread/pwrite",
)
def test_consume_mutate_restore_is_rejected(tmp_path):
    prepared = _prepared(tmp_path)
    copy_file = prepared._copy_owner._copy_file
    assert copy_file is not None
    descriptor = copy_file.fileno()
    original = os.pread(descriptor, prepared.copy_byte_count, 0)
    mutation_offset = len(original) // 2
    mutation = bytes((original[mutation_offset] ^ 1,))

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared) as copy_stream:
            assert len(copy_stream.read(mutation_offset)) == mutation_offset
            try:
                assert os.pwrite(descriptor, mutation, mutation_offset) == 1
                _drain(copy_stream)
            finally:
                assert os.pwrite(
                    descriptor,
                    original[mutation_offset : mutation_offset + 1],
                    mutation_offset,
                ) == 1
                assert os.pread(descriptor, len(original), 0) == original


def test_exact_consumed_digest_rejects_altered_bytes_with_stable_file(tmp_path):
    prepared = _prepared(tmp_path)
    copy_file = prepared._copy_owner._copy_file
    assert copy_file is not None
    descriptor = copy_file.fileno()
    original = os.pread(descriptor, prepared.copy_byte_count, 0)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared) as copy_stream:
            copy_stream._copy_file = _AlteredCopyFile(original, descriptor)
            assert len(_drain(copy_stream)) == prepared.copy_byte_count
            assert os.pread(descriptor, len(original), 0) == original


def test_copy_and_scratch_metadata_must_remain_private(tmp_path):
    directory_metadata = os.stat(tmp_path)
    unsafe_directory = _metadata_with(
        directory_metadata,
        st_mode=directory_metadata.st_mode | 0o020,
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        source_copy._scratch_identity(unsafe_directory)

    with _anonymous_file(tmp_path) as copy_file:
        copy_metadata = os.fstat(copy_file.fileno())
        linked_copy = _metadata_with(copy_metadata, st_nlink=1)
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._anonymous_copy_identity(linked_copy)


def test_scratch_parent_open_rejects_unsupported_or_changed_state(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()

    with patch.object(source_copy.os, "name", "synthetic-non-posix"):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._open_scratch_parent(scratch_parent)

    identities = [(1, 2, 3, 4, 5), (6, 7, 8, 9, 10)]
    with patch.object(source_copy, "_scratch_identity", side_effect=identities):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._open_scratch_parent(scratch_parent)

    with patch.object(source_copy.os, "lstat", side_effect=OSError("synthetic")):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._open_scratch_parent(scratch_parent)


def test_scratch_parent_recheck_and_copy_creation_fail_closed(tmp_path):
    scratch_parent = tmp_path / "scratch"
    scratch_parent.mkdir()

    with patch.object(source_copy.os, "fstat", side_effect=OSError("synthetic")):
        assert not source_copy._is_scratch_parent_unchanged(
            scratch_parent,
            -1,
            (1, 2, 3, 4, 5),
        )

    with patch.object(
        source_copy,
        "_is_scratch_parent_unchanged",
        return_value=False,
    ):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._open_anonymous_projection_copy(scratch_parent)


def test_projection_copy_lease_requires_internal_authority():
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        source_copy._ProjectionCopyLease(
            BytesIO(),
            copy_identity=(0,) * 9,
            copy_sha256="0" * 64,
            copy_byte_count=1,
            token=object(),
        )


@pytest.mark.parametrize("invalid_count", [True, 0])
def test_projection_copy_seal_requires_positive_exact_count(tmp_path, invalid_count):
    with _anonymous_file(tmp_path) as copy_file:
        copy_file.write(b"x")
        copy_file.flush()
        metadata = os.fstat(copy_file.fileno())
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._seal_projection_copy_lease(
                copy_file,
                copy_metadata=metadata,
                copy_sha256=hashlib.sha256(b"x").hexdigest(),
                copy_byte_count=invalid_count,
            )


def test_projection_copy_seal_rejects_changed_identity_or_size(tmp_path):
    with _anonymous_file(tmp_path) as copy_file:
        copy_file.write(b"x")
        copy_file.flush()
        metadata = os.fstat(copy_file.fileno())
        changed_metadata = _metadata_with(metadata, st_ino=metadata.st_ino + 1)
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._seal_projection_copy_lease(
                copy_file,
                copy_metadata=changed_metadata,
                copy_sha256=hashlib.sha256(b"x").hexdigest(),
                copy_byte_count=1,
            )

        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._seal_projection_copy_lease(
                copy_file,
                copy_metadata=metadata,
                copy_sha256=hashlib.sha256(b"x").hexdigest(),
                copy_byte_count=2,
            )


@pytest.mark.parametrize(
    "read_result",
    [bytearray(b"x"), b"", OSError("synthetic read failure")],
)
def test_projection_copy_authentication_rejects_invalid_reads(tmp_path, read_result):
    with _anonymous_file(tmp_path) as copy_file:
        copy_file.write(b"x")
        wrapper = _CopyReadWrapper(copy_file, read_result)
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            source_copy._authenticate_and_seal_projection_copy(wrapper)


@pytest.mark.parametrize("invalid_size", [True, -2])
def test_authenticated_reader_requires_exact_bounded_sizes(invalid_size):
    reader = source_copy._AuthenticatedCopyReader(
        BytesIO(b"x"),
        expected_sha256=hashlib.sha256(b"x").hexdigest(),
        expected_byte_count=1,
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        reader.read(invalid_size)


def test_authenticated_reader_handles_zero_default_and_finished_reads():
    reader = source_copy._AuthenticatedCopyReader(
        BytesIO(b"x"),
        expected_sha256=hashlib.sha256(b"x").hexdigest(),
        expected_byte_count=1,
    )

    assert reader.read(0) == b""
    assert reader.read() == b"x"
    assert reader.read() == b""
    assert reader.read(1) == b""


@pytest.mark.parametrize(
    ("read_result", "read_size", "expected_byte_count"),
    [
        (bytearray(b"x"), 1, 1),
        (b"xx", 1, 2),
        (b"x", 1, 0),
        (OSError("synthetic read failure"), 1, 1),
    ],
)
def test_authenticated_reader_rejects_invalid_source_results(
    read_result,
    read_size,
    expected_byte_count,
):
    reader = source_copy._AuthenticatedCopyReader(
        _ReadResultFile(read_result),
        expected_sha256="0" * 64,
        expected_byte_count=expected_byte_count,
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        reader.read(read_size)


@pytest.mark.parametrize("mismatch", ["count", "digest", "identity"])
def test_authenticated_reader_finish_rejects_each_mismatch(tmp_path, mismatch):
    with _anonymous_file(tmp_path) as copy_file:
        copy_file.write(b"x")
        copy_file.flush()
        copy_file.seek(0)
        identity = source_copy._anonymous_copy_identity(os.fstat(copy_file.fileno()))
        expected_count = 2 if mismatch == "count" else 1
        expected_digest = "0" * 64 if mismatch == "digest" else hashlib.sha256(b"x").hexdigest()
        expected_identity = (
            (identity[0] + 1, *identity[1:]) if mismatch == "identity" else identity
        )
        reader = source_copy._AuthenticatedCopyReader(
            copy_file,
            expected_sha256=expected_digest,
            expected_byte_count=expected_count,
        )
        assert _drain(reader) == b"x"

        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            reader.finish(expected_identity)


def test_authenticated_reader_wraps_descriptor_failure():
    copy_file = _ReadResultFile(b"")
    reader = source_copy._AuthenticatedCopyReader(
        copy_file,
        expected_sha256=hashlib.sha256(b"").hexdigest(),
        expected_byte_count=0,
    )

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        reader.finish((0,) * 9)


def test_copy_stream_rejects_wrong_type_or_tampered_public_binding(tmp_path):
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(object()):
            pytest.fail("non-projection entered COPY stream")

    prepared = _prepared(tmp_path)
    tampered = replace(
        prepared,
        copy_byte_count=prepared.copy_byte_count + 1,
    )
    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(tampered):
            pytest.fail("tampered public binding entered COPY stream")
    prepared.cleanup()


def test_copy_stream_wraps_unexpected_binding_validation_failure(tmp_path):
    prepared = _prepared(tmp_path)
    with patch.object(source_copy, "_strict_sha256", side_effect=OSError("synthetic")):
        with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
            with _authenticated_projection_copy_stream(prepared):
                pytest.fail("unexpected validation failure was exposed")
    prepared.cleanup()


def test_copy_stream_rejects_changed_descriptor_identity(tmp_path):
    prepared = _prepared(tmp_path)
    owner = prepared._copy_owner
    owner._copy_identity = (owner._copy_identity[0] + 1, *owner._copy_identity[1:])

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared):
            pytest.fail("changed descriptor entered COPY stream")


def test_copy_stream_wraps_seek_failure_and_cleanup_ignores_other_objects(tmp_path):
    prepared = _prepared(tmp_path)
    owner = prepared._copy_owner
    copy_file = owner._copy_file
    assert copy_file is not None
    owner._copy_file = _SeekFailureCopyFile(copy_file)

    with pytest.raises(TaxIdentitySourceProjectionError, match=_ERROR):
        with _authenticated_projection_copy_stream(prepared):
            pytest.fail("unseekable descriptor entered COPY stream")

    source_copy._cleanup_projection_copy_owner(object())
