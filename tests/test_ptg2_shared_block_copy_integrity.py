from __future__ import annotations

import hashlib
import struct
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_publish import copy_shared_block_binary_file
from process.ptg_parts.ptg2_shared_block_copy import (
    binary_copy_rows,
    scan_shared_block_copy,
)


_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)


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
