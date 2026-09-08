# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Completed URL caches require live metadata; frozen replay requires byte pins."""

import asyncio
import hashlib
from pathlib import Path

import pytest
from aiohttp import web

from process.ptg_parts import source_download
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from tests.test_ptg2_artifact_integrity import (
    _configure_single_get_downloads,
    _download_from_handler,
)
from tests.test_ptg_frozen_headless_download import _HeadlessDownloadHarness


MODIFIED = "Mon, 27 Jul 2026 10:00:00 GMT"


def _retain(store, url, body, *, etag='"old"', last_modified=MODIFIED):
    digest = hashlib.sha256(body).hexdigest()
    path = store.artifact_path(digest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    store.record_manifest({
        "artifact_kind": "raw", "canonical_url": url,
        "raw_storage_uri": store.storage_uri(path), "raw_sha256": digest,
        "content_length": len(body), "byte_count": len(body),
        "etag": etag, "last_modified": last_modified, "status": "available",
    })


@pytest.mark.parametrize(
    "etag,modified,status,longer,conflicting_latest,reuse",
    [
        ('"old"', MODIFIED, 200, False, False, True),
        ('"old"', "Tue, 28 Jul 2026 10:00:00 GMT", 200, False, False, True),
        (None, MODIFIED, 200, False, False, True),
        ('"new"', MODIFIED, 200, False, False, False),
        ('"old"', MODIFIED, 200, True, False, False),
        (None, None, 200, False, False, False),
        ('"old"', MODIFIED, 405, False, False, False),
        ('"old"', MODIFIED, 200, False, True, True),
    ],
)
def test_public_download_revalidates_each_retained_candidate(
    monkeypatch, tmp_path, etag, modified, status, longer, conflicting_latest, reuse,
):
    old = b'{"v":1}'
    current = old if reuse else (b'{"v":22}' if longer else b'{"v":2}')
    calls = []
    headers_by_name = {"Content-Length": str(len(current))}
    if etag is not None:
        headers_by_name["ETag"] = etag
    if modified is not None:
        headers_by_name["Last-Modified"] = modified

    async def handle(request):
        calls.append(request.method)
        if request.method == "HEAD":
            return web.Response(status=status, headers=headers_by_name)
        return web.Response(body=current, headers=headers_by_name)

    def prepare(store, url):
        _retain(store, url, old)
        if conflicting_latest:
            _retain(store, url, b'{"v":3}', etag='"different"')

    _configure_single_get_downloads(monkeypatch, retries=0)
    artifact = asyncio.run(_download_from_handler(tmp_path, handle, prepare))
    assert artifact.reused is reuse
    assert calls == (["HEAD"] if reuse else ["HEAD", "GET"])
    assert Path(artifact.raw_path).read_bytes() == current
    assert artifact.raw_sha256 == hashlib.sha256(current).hexdigest()
    assert artifact.byte_count == len(current)
    assert artifact.head.etag == etag
    assert artifact.head.content_length == len(current)
    assert artifact.verification_mode == (
        "strong_etag_length" if reuse and etag else
        "length_last_modified" if reuse else "downloaded"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("pin_change", [None, "raw_sha256", "content_length"])
async def test_frozen_job_reuses_only_its_explicit_headless_byte_pin(
    monkeypatch, tmp_path, pin_change,
):
    harness = _HeadlessDownloadHarness()
    store, descriptor, original_path = harness.prepared_store(tmp_path)
    _retain(store, harness.canonical_url, harness.body, etag='"from-get"')
    harness.install(monkeypatch)
    monkeypatch.setenv("HLTHPRT_PTG2_ARTIFACT_DIR", str(store.root))
    get_calls = []

    async def safe(_url):
        return None

    async def get(**kwargs):
        get_calls.append(True)
        return await harness.single_get(**kwargs)

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(source_download, "_download_raw_artifact_single_get", get)
    if pin_change == "raw_sha256":
        descriptor[pin_change] = "a" * 64
    elif pin_change == "content_length":
        descriptor[pin_change] += 1
    job_by_field = {"url": harness.canonical_url, "type": "in_network", "_frozen_rate_file": descriptor}
    arguments_by_name = dict(reuse_raw_artifacts=True, max_bytes=None, keep_partial_artifacts=False)
    if pin_change:
        with pytest.raises(FrozenRateFileMismatchError):
            await source_download._download_ptg_job_artifact(job_by_field, **arguments_by_name)
        assert get_calls == [True]
    else:
        downloaded_job = await source_download._download_ptg_job_artifact(job_by_field, **arguments_by_name)
        assert downloaded_job.error is None
        assert downloaded_job.raw_artifact.reused is True
        assert downloaded_job.raw_artifact.verification_mode == "verified_local_sha256"
        assert get_calls == []
    assert original_path.read_bytes() == harness.body


@pytest.mark.asyncio
async def test_exact_get_flag_does_not_authorize_headless_cache_reuse(monkeypatch, tmp_path):
    harness = _HeadlessDownloadHarness()
    store, _descriptor, _path = harness.prepared_store(tmp_path)
    _retain(store, harness.canonical_url, harness.body)
    harness.install(monkeypatch)
    artifact = await source_download._download_raw_artifact_locked(
        harness.canonical_url, store=store, canonical_url=harness.canonical_url,
        reuse_raw_artifacts=True, max_bytes=None, keep_partial_artifacts=False,
        exact_get_evidence=True,
    )
    assert artifact.reused is False
    assert artifact.verification_mode == "downloaded"
    assert artifact.head.status == 200
