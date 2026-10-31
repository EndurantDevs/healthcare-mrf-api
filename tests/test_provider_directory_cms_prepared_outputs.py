# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen content streams retain their private baseline and propagate failures."""

import asyncio
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_outputs as outputs


def frozen_stream(chunks, *, status="COPY 2", keys=("id",), failure=None):
    async def export(query, *, output, **copy_options):
        assert query.startswith('SELECT * FROM ONLY "private_input"."frozen_resource"')
        assert query.endswith('ORDER BY "id"') and copy_options == {"format": "binary"}
        for chunk in chunks:
            await output(chunk)
        if failure is not None:
            raise failure
        return status

    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{"name": key} for key in keys]),
        copy_from_query=AsyncMock(side_effect=export),
    )
    workspace = SimpleNamespace(
        connection=connection,
        policy=SimpleNamespace(schema="private_input"),
        session=SimpleNamespace(),
    )
    handoff = SimpleNamespace(document={"tables": {"resource": {"oid": 42, "name": "frozen_resource"}}})
    return workspace, handoff.document["tables"]["resource"]


async def test_native_copy_chunks_do_not_change_complete_content_commitment():
    first = frozen_stream([b"header", b"row1", b"row2", b"end"])
    second = frozen_stream([b"headerrow1row2end"])
    expected_by_field = {
        "rows": 2,
        "bytes": len(b"headerrow1row2end"),
        "sha256": hashlib.sha256(b"headerrow1row2end").hexdigest(),
    }
    assert (
        await outputs._copy_frozen_content(*first) == await outputs._copy_frozen_content(*second) == expected_by_field
    )


@pytest.mark.parametrize("status", [None, "COPY", "COPY -1", "COPY 2\n", "SELECT 2"])
async def test_incomplete_copy_status_cannot_become_frozen_evidence(status):
    with pytest.raises(ValueError, match="frozen_copy_incomplete"):
        await outputs._copy_frozen_content(*frozen_stream([b"data"], status=status))


async def test_missing_native_primary_key_refuses_before_streaming():
    workspace, handoff = frozen_stream([], keys=())
    with pytest.raises(ValueError, match="frozen_key_required"):
        await outputs._copy_frozen_content(workspace, handoff)
    workspace.connection.copy_from_query.assert_not_awaited()


@pytest.mark.parametrize("failure", [RuntimeError("stream interrupted"), asyncio.CancelledError(), TimeoutError()])
async def test_interrupted_stream_cannot_return_partial_commitment(failure, monkeypatch):
    close, invalidate = AsyncMock(), AsyncMock()
    monkeypatch.setattr(outputs, "close_interrupted_copy", close)
    monkeypatch.setattr(outputs, "invalidate_copy_owner", invalidate)
    with pytest.raises(type(failure)):
        await outputs._copy_frozen_content(*frozen_stream([b"partial"], failure=failure))
    close.assert_awaited_once()
    invalidate.assert_awaited_once()


async def test_native_chunk_limit_refuses_before_hashing(monkeypatch):
    monkeypatch.setattr(outputs, "COPY_MAX_BYTES", 4)
    close, invalidate = AsyncMock(), AsyncMock()
    monkeypatch.setattr(outputs, "close_interrupted_copy", close)
    monkeypatch.setattr(outputs, "invalidate_copy_owner", invalidate)
    with pytest.raises(ValueError, match="frozen_chunk_unbounded"):
        await outputs._copy_frozen_content(*frozen_stream([b"too large"]))
    close.assert_awaited_once()
    invalidate.assert_awaited_once()
