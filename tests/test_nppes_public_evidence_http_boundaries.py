# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary matrices for official CMS HTTP streaming."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.nppes_public_evidence_http as http
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError


ARCHIVE_NAME = "NPPES_Data_Dissemination_July_2026_V2.zip"
ARCHIVE_URL = f"https://download.cms.gov/nppes/{ARCHIVE_NAME}"


class _Content:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


def _response(status=200, headers=None, chunks=(b"abc",)):
    return SimpleNamespace(
        status=status,
        headers=headers or {},
        content=_Content(chunks),
    )


@pytest.mark.asyncio
async def test_http_cancel_helper_and_etag_header_boundaries() -> None:
    events: list[str] = []

    def sync_cancel():
        events.append("sync")

    async def async_cancel():
        events.append("async")

    await http._invoke_cancel(sync_cancel)
    await http._invoke_cancel(async_cancel)
    assert events == ["sync", "async"]
    assert http._request_headers('"strong"')["If-None-Match"] == '"strong"'
    assert "If-None-Match" not in http._request_headers('W/"weak"')


@pytest.mark.parametrize(
    ("raw_length", "expected"),
    ((None, None), ("3", 3)),
)
def test_declared_content_length_accepts_absent_or_positive(
    raw_length,
    expected,
) -> None:
    response = _response(
        headers={} if raw_length is None else {"Content-Length": raw_length}
    )
    assert http._declared_content_length(response, 10) == expected


@pytest.mark.parametrize("raw_length", ("bad", "0", "11"))
def test_declared_content_length_rejects_invalid_or_over_limit(raw_length) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        http._declared_content_length(
            _response(headers={"Content-Length": raw_length}),
            10,
        )


def test_redirect_target_enforces_limit_and_canonical_location() -> None:
    response = _response(302, {"Location": f"./{ARCHIVE_NAME}"})
    assert http._redirect_target(response, ARCHIVE_URL, 0) == ARCHIVE_URL
    with pytest.raises(NppesPublicEvidenceArchiveError):
        http._redirect_target(response, ARCHIVE_URL, http._MAX_REDIRECTS)


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks", ((b"",), ("not-bytes",), (b"1234",)))
async def test_stream_body_rejects_empty_wrong_type_or_over_limit(
    tmp_path: Path,
    chunks,
) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await http._stream_identity_body(
            _response(chunks=chunks),
            tmp_path / f"body-{len(list(tmp_path.iterdir()))}",
            3,
        )


@pytest.mark.asyncio
async def test_terminal_response_handles_not_modified_success_and_error(
    tmp_path: Path,
) -> None:
    not_modified = await http._terminal_response_result(
        _response(304, {"ETag": '"fresh"'}),
        ARCHIVE_URL,
        tmp_path / "unused",
        10,
        '"old"',
    )
    assert not_modified.status == 304
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await http._terminal_response_result(
            _response(500), ARCHIVE_URL, tmp_path / "error", 10, None
        )
    success = await http._terminal_response_result(
        _response(200, {"Content-Length": "3"}),
        ARCHIVE_URL,
        tmp_path / "success",
        10,
        None,
    )
    assert success.byte_count == 3


@pytest.mark.asyncio
async def test_http_public_wrapper_preserves_cancellation_and_success(
    monkeypatch,
) -> None:
    async def cancelled(*_args, **_kwargs):
        raise ImportCancelledError("cancelled")

    monkeypatch.setattr(http, "_stream_official_url", cancelled)
    with pytest.raises(ImportCancelledError):
        await http.stream_official_url(
            ARCHIVE_URL, Path("unused"), max_bytes=10, etag=None
        )

    expected = http.HttpStreamResult(304, ARCHIVE_URL, None, None, None, None)
    monkeypatch.setattr(http, "_stream_official_url", AsyncMock(return_value=expected))
    assert await http.stream_official_url(
        ARCHIVE_URL, Path("unused"), max_bytes=10, etag=None
    ) == expected


@pytest.mark.asyncio
async def test_http_redirect_loop_is_bounded(tmp_path: Path, monkeypatch) -> None:
    class _ResponseContext:
        async def __aenter__(self):
            return _response(302, {"Location": f"./{ARCHIVE_NAME}"})

        async def __aexit__(self, *_args):
            return False

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def get(self, *_args, **_kwargs):
            return _ResponseContext()

    monkeypatch.setattr(http.aiohttp, "ClientSession", lambda **_kwargs: _Session())
    monkeypatch.setattr(
        http,
        "_redirect_target",
        lambda _response, current_url, _redirect_count: current_url,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await http._stream_official_url(
            ARCHIVE_URL,
            tmp_path / "redirected.zip",
            max_bytes=10,
            etag=None,
        )
