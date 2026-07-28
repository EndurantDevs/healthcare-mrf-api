# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retry contract for exact frozen-file GET evidence."""

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager

import pytest

from process.ptg_parts.domain import PTG2HeadMetadata
from process.ptg_parts.live_progress import (
    reset_live_progress_context,
    set_live_progress_context,
)


source_download = importlib.import_module(
    "process.ptg_parts.source_download"
)


class _FakeSession:
    def __init__(self, **_kwargs) -> None:
        self.is_open = False

    async def __aenter__(self):
        self.is_open = True
        return self

    async def __aexit__(self, *_args):
        self.is_open = False
        return None


class _TransientContent:
    def __init__(
        self,
        *,
        should_fail: bool,
        failure_text: str,
    ) -> None:
        self.should_fail = should_fail
        self.failure_text = failure_text

    async def iter_chunked(self, _chunk_size):
        yield b"{"
        if self.should_fail:
            raise RuntimeError(self.failure_text)
        yield b"}"


class _ExactGetResponse:
    status = 200

    def __init__(
        self,
        url: str,
        *,
        should_fail: bool,
        failure_text: str,
    ) -> None:
        self.url = url
        self.headers = {
            "Content-Length": "2",
            "ETag": '"exact-get"',
            "Last-Modified": "Mon, 27 Jul 2026 10:00:00 GMT",
            "Content-Type": "application/json",
        }
        self.content = _TransientContent(
            should_fail=should_fail,
            failure_text=failure_text,
        )

    def raise_for_status(self) -> None:
        return None


class _ExactGetRetryHarness:
    private_label = "frozen-part-001-of-001"

    def __init__(self, partial_path, failure_text: str) -> None:
        self.partial_path = partial_path
        self.failure_text = failure_text
        self.request_headers: list[dict[str, str] | None] = []
        self.pre_request_bytes: list[bytes | None] = []
        self.screen_lines: list[str] = []
        self.debug_lines: list[str] = []

    def install(self, monkeypatch) -> None:
        monkeypatch.setattr(
            source_download.aiohttp,
            "ClientSession",
            _FakeSession,
        )
        monkeypatch.setattr(
            source_download,
            "_validated_request",
            self.validated_request,
        )
        monkeypatch.setattr(
            source_download,
            "_emit_screen_line",
            lambda line, **_kwargs: self.screen_lines.append(line),
        )
        monkeypatch.setattr(
            source_download.logger,
            "debug",
            lambda line, *_args: self.debug_lines.append(line),
        )
        monkeypatch.setattr(
            source_download,
            "_download_retry_count",
            lambda: 1,
        )
        monkeypatch.setattr(
            source_download,
            "_download_retry_delay_seconds",
            lambda: 0,
        )

    async def download(self, canonical_url: str):
        token = set_live_progress_context(
            private_source=True,
            file_name=self.private_label,
        )
        try:
            return await source_download._download_raw_artifact_single_get(
                url=canonical_url,
                path=self.partial_path,
                head=PTG2HeadMetadata(
                    url=canonical_url,
                    etag='"stale"',
                    content_length=12,
                    supports_head=False,
                ),
                max_bytes=None,
                started_at=0,
                allow_resume=False,
            )
        finally:
            reset_live_progress_context(token)

    def assert_contract(
        self,
        download_result,
        sidecar_path,
        private_values,
    ) -> None:
        assert self.request_headers == [None, None]
        assert self.pre_request_bytes == [None, None]
        assert download_result[1] == 2
        assert self.partial_path.read_bytes() == b"{}"
        assert not sidecar_path.exists()
        rendered_retry = "\n".join(
            self.screen_lines + self.debug_lines
        )
        assert f"target={self.private_label}" in rendered_retry
        assert "error=RuntimeError" in rendered_retry
        for private_value in private_values:
            assert private_value not in rendered_retry

    @asynccontextmanager
    async def validated_request(
        self,
        _session,
        _method,
        url,
        *,
        headers=None,
    ):
        self.request_headers.append(headers)
        self.pre_request_bytes.append(
            self.partial_path.read_bytes()
            if self.partial_path.exists()
            else None
        )
        should_fail = len(self.request_headers) == 1
        yield _ExactGetResponse(
            url,
            should_fail=should_fail,
            failure_text=self.failure_text,
        )


@pytest.mark.asyncio
async def test_exact_get_retry_discards_sidecar_and_partial_prefix(
    monkeypatch,
    tmp_path,
):
    """Every frozen retry is a full GET with no retained partial prefix."""

    partial_path = tmp_path / "artifact.part"
    partial_path.write_bytes(b"stale-prefix")
    sidecar_path = source_download._range_sidecar_path(partial_path)
    sidecar_path.write_text('{"stale": true}', encoding="utf-8")
    canonical_url = "https://rates.example.test/part.json"
    private_values = (
        canonical_url,
        '"private-etag"',
        "f" * 64,
    )
    harness = _ExactGetRetryHarness(
        partial_path,
        " ".join(private_values),
    )
    harness.install(monkeypatch)
    download_result = await harness.download(canonical_url)
    harness.assert_contract(
        download_result,
        sidecar_path,
        private_values,
    )
