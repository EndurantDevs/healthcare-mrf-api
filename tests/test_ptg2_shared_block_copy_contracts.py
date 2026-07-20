from __future__ import annotations

import hashlib
import struct
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_publish import copy_shared_block_binary_file


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


def _install_copy_capture(monkeypatch, *, existing_hashes=()):
    captured_copy_bytes = bytearray()

    async def copy_to_table(_table, *, source, **_kwargs):
        while copy_chunk := source.read(7):
            captured_copy_bytes.extend(copy_chunk)

    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(
            driver_connection=SimpleNamespace(copy_to_table=copy_to_table)
        )
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)
    monkeypatch.setattr(
        ptg2_shared_publish.db,
        "all",
        AsyncMock(return_value=[(block_hash,) for block_hash in existing_hashes]),
    )
    return captured_copy_bytes


def _contract_options(case: str, digest: str) -> dict[str, object]:
    options_by_case = {
        "missing": {},
        "empty": {},
        "bytes-only": {"expected_copy_bytes": 1},
        "digest-only": {"expected_copy_sha256": digest},
        "boolean-bytes": {
            "expected_copy_bytes": True,
            "expected_copy_sha256": digest,
        },
        "zero-bytes": {
            "expected_copy_bytes": 0,
            "expected_copy_sha256": digest,
        },
        "changed-size": {
            "expected_copy_bytes": 2,
            "expected_copy_sha256": digest,
        },
        "invalid-digest": {
            "expected_copy_bytes": 1,
            "expected_copy_sha256": "not-a-digest",
        },
        "reuse-without-proof": {"reuse_existing": True},
    }
    return options_by_case[case]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("missing", "missing or empty"),
        ("empty", "missing or empty"),
        ("bytes-only", "requires both byte and digest"),
        ("digest-only", "requires both byte and digest"),
        ("boolean-bytes", "invalid expected bytes"),
        ("zero-bytes", "invalid expected bytes"),
        ("changed-size", "byte count changed before COPY"),
        ("invalid-digest", "invalid expected digest"),
        ("reuse-without-proof", "requires byte and digest expectations"),
    ),
)
async def test_shared_block_copy_rejects_invalid_publication_contract(
    tmp_path,
    case,
    message,
):
    path = tmp_path / "blocks.copy"
    content_by_case = {"missing": None, "empty": b""}
    file_content = content_by_case.get(case, b"x")
    if file_content is not None:
        path.write_bytes(file_content)

    copy_options_by_name = _contract_options(
        case,
        hashlib.sha256(b"x").hexdigest(),
    )
    with pytest.raises(RuntimeError, match=message):
        await copy_shared_block_binary_file(
            path,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_test",
            **copy_options_by_name,
        )


@pytest.mark.asyncio
async def test_shared_block_copy_requires_driver_binary_copy(tmp_path, monkeypatch):
    path = tmp_path / "blocks.copy"
    path.write_bytes(b"copy")
    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(driver_connection=object())
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(ptg2_shared_publish.db, "acquire", acquire)

    with pytest.raises(NotImplementedError, match="does not expose binary COPY"):
        await copy_shared_block_binary_file(
            path,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_test",
        )


@pytest.mark.asyncio
async def test_shared_block_copy_rejects_changed_selective_aggregates(
    tmp_path,
    monkeypatch,
):
    block_hash = b"a" * 32
    payload = b"payload"
    block_payload = _block_copy(_block_row(block_hash, 1, payload))
    path = tmp_path / "blocks.copy"
    path.write_bytes(block_payload)
    _install_copy_capture(monkeypatch, existing_hashes=(block_hash,))
    monkeypatch.setattr(
        ptg2_shared_publish,
        "scan_shared_block_copy",
        lambda _path: SimpleNamespace(
            block_hashes={block_hash},
            row_count=1,
            stored_payload_bytes=len(payload) + 1,
        ),
    )

    with pytest.raises(RuntimeError, match="filtering changed source aggregates"):
        await copy_shared_block_binary_file(
            path,
            schema_name="mrf",
            stage_table="ptg2_v3_block_stage_test",
            expected_copy_bytes=len(block_payload),
            expected_copy_sha256=hashlib.sha256(block_payload).hexdigest(),
            reuse_existing=True,
        )
