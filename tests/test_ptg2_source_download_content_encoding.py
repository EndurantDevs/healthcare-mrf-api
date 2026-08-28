# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import aiohttp
import pytest

from process.ptg_parts import source_download
from process.ptg_parts.domain import PTG2HeadMetadata
from tests.ptg2_source_download_security_support import (
    _Response,
    _Session,
    _range_sidecar_path,
)


def test_download_session_uses_stable_product_user_agent(monkeypatch) -> None:
    session_options_by_name = {}
    connector = object()

    monkeypatch.setattr(source_download, "_public_connector", lambda: connector)
    monkeypatch.setattr(
        source_download.aiohttp,
        "ClientSession",
        lambda **kwargs: session_options_by_name.update(kwargs) or _Session(),
    )

    timeout = aiohttp.ClientTimeout(total=30)
    source_download._download_session(timeout)
    assert session_options_by_name == {
        "timeout": timeout,
        "connector": connector,
        "headers": {"User-Agent": "HealthPorta-MRF/1.0"},
        "max_field_size": 64 * 1024,
    }


def test_single_get_uses_decoded_byte_space_for_encoded_response(
    tmp_path, monkeypatch
) -> None:
    partial = tmp_path / "encoded"
    partial.write_bytes(b"stale")
    _range_sidecar_path(partial).write_text("stale", encoding="utf-8")
    state = source_download._single_get_download_state(
        partial,
        PTG2HeadMetadata(
            url="https://example.test/cms-hpt.txt",
            etag='"strong"',
            content_length=176,
            content_encoding="gzip",
        ),
    )
    assert state.total_bytes is None and state.byte_count == 0

    decoded = b"x" * 273
    response = _Response(
        status=200,
        headers={"Content-Encoding": "gzip", "Content-Length": "176"},
        chunks=[decoded],
    )

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        yield response

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    asyncio.run(
        source_download._run_single_get_attempt(
            url="https://example.test/cms-hpt.txt",
            state=state,
            max_bytes=273,
            started_at=0,
            timeout=aiohttp.ClientTimeout(total=None),
        )
    )
    assert partial.read_bytes() == decoded
    assert state.byte_count == 273
    assert state.total_bytes is None
    assert state.content_encoding == "gzip"

    state.byte_count = 1
    state.total_bytes = 2
    state.validator = '"strong"'
    with pytest.raises(
        source_download._UnsafeRangeResponseError, match="encoded content"
    ):
        source_download._prepare_single_get_response(
            response,
            state=state,
            url="https://example.test/cms-hpt.txt",
            is_resume_request=True,
        )
