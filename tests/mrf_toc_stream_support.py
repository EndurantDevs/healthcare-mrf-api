# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic HTTP transport for bounded discovery TOC contracts."""

from __future__ import annotations

import asyncio
import io
import json
import zipfile
from unittest.mock import AsyncMock

from process import mrf_source_discovery as discovery


class TocResponse:
    def __init__(self, body, url, status=200):
        self.body = body
        self.offset = 0
        self.url = url
        self.status = status
        self.charset = "utf-8"
        self.headers = {"Content-Type": "application/json"}
        self.content = self

    async def read(self, size):
        """Expose only awaited, bounded bytes without any external connection."""
        await asyncio.sleep(0)
        chunk = self.body[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class TocSession:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status
        self.requests = []

    def get(self, url, **kwargs):
        """Record policy arguments and return an isolated response each time."""
        self.requests.append((url, kwargs))
        return TocResponse(self.body, url, self.status)


def toc_session(monkeypatch, tmp_path, body, *, status=200):
    """Keep worker-state files and every HTTP request within synthetic fixtures."""
    monkeypatch.setenv("HLTHPRT_MRF_CHUNK_WORKDIR", str(tmp_path))
    monkeypatch.delenv("HLTHPRT_MRF_DISCOVERY_SPOOL_BYTES", raising=False)
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    return TocSession(json.dumps(body).encode() if isinstance(body, dict) else body, status)


def zip_members(members):
    """Encode the same synthetic document members supplied to legacy ZIP tests."""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in members:
            archive.writestr(name, json.dumps(body))
    return stream.getvalue()
