# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import gzip

import hashlib

import sys

import types

import zipfile

from contextlib import asynccontextmanager

from pathlib import Path

import aiohttp

import pytest

from process.ptg_parts import source_download

from process.ptg_parts.artifacts import PTG2ArtifactStore, _range_sidecar_path

from process.ptg_parts.domain import (
    PTG2DownloadedJob,
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)

from process.url_security import UnsafeUrlError


class _Content:
    def __init__(self, chunks: list[bytes], *, read_payload: bytes | None = None):
        self.chunks = chunks
        self.read_payload = b"".join(chunks) if read_payload is None else read_payload

    async def read(self) -> bytes:
        return self.read_payload

    async def iter_chunked(self, _size: int):
        for chunk in self.chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        *,
        status: int = 200,
        url: str = "https://example.test/rates.json",
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
        read_payload: bytes | None = None,
        raise_error: Exception | None = None,
    ):
        self.status = status
        self.url = url
        self.headers = headers or {}
        self.content = _Content(chunks or [], read_payload=read_payload)
        self.raise_error = raise_error
        self.released = False

    def release(self) -> None:
        self.released = True

    def raise_for_status(self) -> None:
        if self.raise_error is not None:
            raise self.raise_error


class _Session:
    def __init__(self, responder=None, **_kwargs):
        self.responder = responder
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        if isinstance(self.responder, BaseException):
            raise self.responder
        if callable(self.responder):
            return self.responder(method, url, kwargs)
        return self.responder


def _raw(path: Path, *, reused: bool = False) -> PTG2RawArtifact:
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    return PTG2RawArtifact(
        original_url="https://example.test/rates.json",
        canonical_url="https://example.test/rates.json",
        raw_path=str(path),
        raw_storage_uri=f"file:{path}",
        raw_sha256=digest,
        byte_count=len(payload),
        reused=reused,
    )


def _logical(path: Path, *, reused: bool = False, deferred: bool = False) -> PTG2LogicalArtifact:
    payload = path.read_bytes()
    return PTG2LogicalArtifact(
        logical_path=str(path),
        logical_sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
        reused=reused,
        logical_hash_deferred=deferred,
    )


def _run_range_selection_download(
    url: str,
    *,
    store: PTG2ArtifactStore,
    max_bytes: int | None,
) -> PTG2RawArtifact:
    return asyncio.run(
        source_download._download_raw_artifact_locked(
            url,
            store=store,
            canonical_url=url,
            reuse_raw_artifacts=True,
            max_bytes=max_bytes,
            keep_partial_artifacts=True,
        )
    )


def _state(path: Path, **changes) -> source_download._SingleGetDownloadState:
    state_values_by_field = {
        "path": path,
        "digest": hashlib.sha256(),
        "byte_count": 0,
        "total_bytes": None,
        "validator": None,
        "last_modified": None,
        "next_progress_bytes": 1,
    }
    state_values_by_field.update(changes)
    return source_download._SingleGetDownloadState(**state_values_by_field)

