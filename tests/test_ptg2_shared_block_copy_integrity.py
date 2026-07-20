from __future__ import annotations

import hashlib
import io
import struct
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_block_copy as block_copy
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_publish import copy_shared_block_binary_file
from process.ptg_parts.ptg2_shared_block_copy import (
    binary_copy_rows,
    scan_shared_block_copy,
)


_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)
_VALID_METADATA_FIELDS = (
    b"a" * 32,
    struct.pack(">h", 2),
    b"serving",
    struct.pack(">q", 1),
    struct.pack(">i", 0),
    struct.pack(">q", 1),
    b"none",
    struct.pack(">q", 1),
    struct.pack(">q", 1),
)


def _metadata_with(field_index: int, value: bytes) -> tuple[bytes, ...]:
    fields = list(_VALID_METADATA_FIELDS)
    fields[field_index] = value
    return tuple(fields)


def _field(value: bytes) -> bytes:
    return struct.pack(">i", len(value)) + value


def _block_row(block_hash: bytes, block_key: int, payload: bytes) -> bytes:
    fields = (
        block_hash,
        struct.pack(">h", 2),
        b"serving",
        struct.pack(">q", block_key),
        struct.pack(">i", 0),
        struct.pack(">q", 1),
        b"none",
        struct.pack(">q", len(payload)),
        struct.pack(">q", len(payload)),
        payload,
    )
    return struct.pack(">h", len(fields)) + b"".join(map(_field, fields))


def _block_copy(*rows: bytes) -> bytes:
    return _COPY_HEADER + b"".join(rows) + struct.pack(">h", -1)


def _copy_reader(payload: bytes) -> block_copy.SelectiveSharedBlockCopyReader:
    return block_copy.SelectiveSharedBlockCopyReader(
        io.BytesIO(payload),
        existing_hashes=set(),
        expected_source_bytes=len(payload),
        expected_source_sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.mark.asyncio
async def test_shared_block_copy_verifies_bytes_consumed_by_postgres(
    tmp_path,
    monkeypatch,
):
    block_payload = b"binary-copy-payload"
    path = tmp_path / "blocks.copy"
    path.write_bytes(block_payload)
    consumed = bytearray()

    async def copy_to_table(_table, *, source, **_kwargs):
        while chunk := source.read(3):
            consumed.extend(chunk)

    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(
            driver_connection=SimpleNamespace(copy_to_table=copy_to_table)
        )
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    await copy_shared_block_binary_file(
        path,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_test",
        expected_copy_bytes=len(block_payload),
        expected_copy_sha256=hashlib.sha256(block_payload).hexdigest(),
    )

    assert consumed == block_payload


@pytest.mark.asyncio
async def test_shared_block_copy_rejects_digest_change_during_publication(
    tmp_path,
    monkeypatch,
):
    block_payload = b"binary-copy-payload"
    path = tmp_path / "blocks.copy"
    path.write_bytes(block_payload)
    consumed = bytearray()

    async def copy_to_table(_table, *, source, **_kwargs):
        while chunk := source.read(4):
            consumed.extend(chunk)

    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(
            driver_connection=SimpleNamespace(copy_to_table=copy_to_table)
        )
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    with pytest.raises(RuntimeError, match="content changed"):
        await copy_shared_block_binary_file(
            path,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_test",
            expected_copy_bytes=len(block_payload),
            expected_copy_sha256="0" * 64,
        )

    assert consumed == block_payload


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_indexes", [(0,), (0, 1)])
async def test_shared_block_copy_omits_only_existing_payloads(
    tmp_path,
    monkeypatch,
    existing_indexes,
):
    hashes = (b"a" * 32, b"b" * 32)
    payloads = (b"first-payload", b"second-payload")
    block_payload = _block_copy(
        *(
            _block_row(block_hash, index + 1, payloads[index])
            for index, block_hash in enumerate(hashes)
        )
    )
    path = tmp_path / "blocks.copy"
    path.write_bytes(block_payload)
    consumed = bytearray()

    async def copy_to_table(_table, *, source, **_kwargs):
        while chunk := source.read(7):
            consumed.extend(chunk)

    async def all_rows(_statement, **_params):
        return [(hashes[index],) for index in existing_indexes]

    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(
            driver_connection=SimpleNamespace(copy_to_table=copy_to_table)
        )
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)
    monkeypatch.setattr(ptg2_shared_publish.db, "all", all_rows)

    await copy_shared_block_binary_file(
        path,
        schema_name="mrf",
        stage_table="ptg2_v3_block_stage_test",
        expected_copy_bytes=len(block_payload),
        expected_copy_sha256=hashlib.sha256(block_payload).hexdigest(),
        reuse_existing=True,
    )

    decoded_copy_rows = binary_copy_rows(bytes(consumed))
    assert len(decoded_copy_rows) == 2
    for index, decoded_copy_row in enumerate(decoded_copy_rows):
        assert decoded_copy_row[0] == hashes[index]
        assert decoded_copy_row[3] == struct.pack(">q", index + 1)
        assert decoded_copy_row[9] == (
            None if index in existing_indexes else payloads[index]
        )


def test_shared_block_copy_scan_rejects_truncated_payload(tmp_path):
    path = tmp_path / "truncated.copy"
    path.write_bytes(_block_copy(_block_row(b"a" * 32, 1, b"payload"))[:-4])

    with pytest.raises(RuntimeError, match="truncates"):
        scan_shared_block_copy(path)


@pytest.mark.parametrize(
    ("copy_payload", "error"),
    (
        (_COPY_HEADER + struct.pack(">h", 9), "row width changed"),
        (
            _COPY_HEADER
            + _block_row(b"a" * 32, 1, b"payload")[:-11]
            + struct.pack(">i", 8)
            + b"payload",
            "payload length is invalid",
        ),
        (_block_copy(), "contains no rows"),
    ),
)
def test_shared_block_copy_scan_rejects_corrupt_framing(
    tmp_path,
    copy_payload,
    error,
):
    path = tmp_path / "corrupt.copy"
    path.write_bytes(copy_payload)

    with pytest.raises(RuntimeError, match=error):
        scan_shared_block_copy(path)


def test_binary_copy_rows_rejects_wrong_row_width():
    with pytest.raises(RuntimeError, match="row width changed"):
        binary_copy_rows(_COPY_HEADER + struct.pack(">h", 9))


@pytest.mark.parametrize(
    ("fields", "error"),
    (
        (_VALID_METADATA_FIELDS[:-1], "metadata field count changed"),
        (_metadata_with(0, b"a"), "hash is not 32 bytes"),
        (_metadata_with(1, struct.pack(">h", 3)), "format version is incompatible"),
        (_metadata_with(2, b"\xff"), "metadata text is invalid"),
        (_metadata_with(2, b""), "object kind is invalid"),
        (_metadata_with(6, b"gzip"), "codec is invalid"),
        (_metadata_with(3, b"x"), "block key width changed"),
        (_metadata_with(3, struct.pack(">q", -1)), "block key is negative"),
    ),
)
def test_shared_block_copy_metadata_rejects_corrupt_contract(fields, error):
    with pytest.raises(RuntimeError, match=error):
        block_copy._validated_metadata(fields)


@pytest.mark.parametrize(
    ("operation", "error"),
    (
        (
            lambda: block_copy._read_exact(io.BytesIO(b""), 1, label="test"),
            "truncates test",
        ),
        (
            lambda: block_copy._read_metadata_fields(io.BytesIO(struct.pack(">i", -1))),
            "metadata cannot be NULL",
        ),
        (
            lambda: block_copy._validate_header(
                io.BytesIO(b"x" * len(block_copy._COPY_HEADER))
            ),
            "header is incompatible",
        ),
        (
            lambda: block_copy._validate_trailer(io.BytesIO(b"x")),
            "contains trailing bytes",
        ),
    ),
)
def test_shared_block_copy_framing_rejects_corrupt_contract(operation, error):
    with pytest.raises(RuntimeError, match=error):
        operation()


@pytest.mark.parametrize(
    ("operation", "error"),
    (
        (
            lambda: _copy_reader(struct.pack(">i", -1))._read_metadata(),
            "metadata cannot be NULL",
        ),
        (
            lambda: _copy_reader(b"x" * len(_COPY_HEADER)).read(),
            "header is incompatible",
        ),
        (
            lambda: _copy_reader(b"x")._verify_source(),
            "contains trailing bytes",
        ),
        (
            lambda: block_copy.SelectiveSharedBlockCopyReader(
                io.BytesIO(b""),
                existing_hashes=set(),
                expected_source_bytes=1,
                expected_source_sha256=hashlib.sha256(b"").hexdigest(),
            )._verify_source(),
            "content changed during publication",
        ),
        (
            lambda: _copy_reader(_COPY_HEADER + struct.pack(">h", 9)).read(),
            "row width changed",
        ),
        (
            lambda: _copy_reader(
                _COPY_HEADER
                + _block_row(b"a" * 32, 1, b"payload")[:-11]
                + struct.pack(">i", 8)
                + b"payload"
            ).read(),
            "payload length is invalid",
        ),
    ),
)
def test_selective_copy_reader_rejects_corrupt_framing(operation, error):
    with pytest.raises(RuntimeError, match=error):
        operation()


def test_selective_copy_reader_zero_size_does_not_consume_source():
    reader = _copy_reader(b"")

    assert reader.read(0) == b""
    assert reader.source_byte_count == 0
