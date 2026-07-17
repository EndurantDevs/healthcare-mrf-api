from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts.ptg2_shared_publish import copy_shared_block_binary_file


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
