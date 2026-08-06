# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adversarial proofs for the one-shot anonymous projection COPY lease."""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat

import pytest

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
