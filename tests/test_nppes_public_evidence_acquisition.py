# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Proof for durable, fully reverified NPPES HTTP acquisition."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
import pytest

import process.nppes_public_evidence_acquisition as acquisition
import process.nppes_public_evidence_artifacts as artifact_contract
import process.nppes_public_evidence_http as acquisition_http
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import (
    NppesPublicEvidenceArchiveError,
    parse_official_nppes_listing,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.input_artifact_retention import (
    collect_ptg2_input_artifacts,
)


ARCHIVE_NAME = "NPPES_Data_Dissemination_July_2026_V2.zip"
ARCHIVE_URL = f"https://download.cms.gov/nppes/{ARCHIVE_NAME}"
LISTING = f'<a href="./{ARCHIVE_NAME}">monthly</a>'.encode()


def _candidate():
    return parse_official_nppes_listing(LISTING)[0]


def _artifact_store(tmp_path: Path) -> PTG2ArtifactStore:
    store = PTG2ArtifactStore(tmp_path / "store")
    store.root.chmod(0o700)
    return store


def test_failed_post_link_verification_removes_only_created_inode(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = _artifact_store(tmp_path)
    source = tmp_path / "source.zip"
    source.write_bytes(b"verified-source-bytes")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    final_path = artifact_contract.retained_path(store, digest, ".zip")
    monkeypatch.setattr(
        artifact_contract,
        "sha256_file",
        lambda _path: ("00" * 32, source.stat().st_size),
    )

    with pytest.raises(NppesPublicEvidenceArchiveError):
        artifact_contract.retain_verified_inode(
            store,
            source,
            digest,
            ".zip",
        )

    assert not final_path.exists()
    assert source.read_bytes() == b"verified-source-bytes"


class _FakeContent:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _size):
        for chunk in self._chunks:
            yield chunk


class _FakeResponse:
    def __init__(self, *, encoding="identity"):
        self.status = 200
        self.headers = {
            "Content-Length": "9",
            "Content-Encoding": encoding,
        }
        self.content = _FakeContent([b"short"])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _FakeSession:
    def __init__(self, *args, **kwargs):
        self.response = _FakeResponse(encoding=kwargs.pop("encoding", "identity"))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, *_args, **_kwargs):
        return self.response


class _EncodedSession(_FakeSession):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.response = _FakeResponse(encoding="gzip")


@pytest.mark.asyncio
async def test_listing_and_archive_publish_content_addressed_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    store = _artifact_store(tmp_path)
    archive_bytes = b"PK synthetic archive bytes"
    calls: list[tuple[str, str | None]] = []

    async def fake_stream(
        source_url,
        temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        calls.append((source_url, etag))
        payload = LISTING if source_url == acquisition.NPPES_LISTING_URL else archive_bytes
        temporary_path.write_bytes(payload)
        return acquisition._HttpStreamResult(
            status=200,
            final_url=source_url,
            sha256=hashlib.sha256(payload).hexdigest(),
            byte_count=len(payload),
            etag='"strong-etag"',
            last_modified="Mon, 13 Jul 2026 09:10:32 GMT",
        )

    monkeypatch.setattr(acquisition, "_stream_official_url", fake_stream)
    listing = await acquisition.acquire_nppes_listing(store)
    retained = await acquisition.acquire_nppes_archive(
        store, listing.candidates[0], listing.listing_sha256
    )
    assert listing.listing_sha256 == hashlib.sha256(LISTING).hexdigest()
    assert retained.artifact_sha256 == hashlib.sha256(archive_bytes).hexdigest()
    assert retained.path.read_bytes() == archive_bytes
    assert retained.path == acquisition._retained_path(
        store,
        retained.artifact_sha256,
        ".zip",
    )
    assert calls == [
        (acquisition.NPPES_LISTING_URL, None),
        (ARCHIVE_URL, None),
    ]
    assert "NPPES" not in repr(listing)
    assert "NPPES" not in repr(retained)


@pytest.mark.asyncio
async def test_retained_evidence_bytes_survive_generic_artifact_gc(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = _artifact_store(tmp_path)
    artifact_bytes = b"durable NPPES evidence"

    async def fake_stream(
        source_url,
        temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        temporary_path.write_bytes(artifact_bytes)
        return acquisition._HttpStreamResult(
            200,
            source_url,
            hashlib.sha256(artifact_bytes).hexdigest(),
            len(artifact_bytes),
            '"etag"',
            "date",
        )

    monkeypatch.setattr(acquisition, "_stream_official_url", fake_stream)
    retained = await acquisition.acquire_nppes_archive(
        store,
        _candidate(),
        "ab" * 32,
    )
    managed_path = store.artifact_path(retained.artifact_sha256, kind="raw")
    assert not managed_path.exists()
    assert retained.path.exists()

    observed_at = datetime.now(UTC) + timedelta(hours=1)
    for offset in (0, 1):
        collect_ptg2_input_artifacts(
            root=store.root,
            execute=True,
            now=observed_at + timedelta(hours=offset),
            retention_hours=0,
            min_age_hours=0,
            target_bytes=0,
            max_delete_bytes=None,
            max_delete_files=None,
        )
    assert not managed_path.exists()
    assert retained.path.read_bytes() == artifact_bytes


@pytest.mark.asyncio
async def test_valid_cache_uses_conditional_get_and_accepts_304(
    tmp_path: Path, monkeypatch
) -> None:
    store = _artifact_store(tmp_path)
    artifact_bytes = b"retained"
    request_etags: list[str | None] = []

    async def fake_stream(
        source_url,
        temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        request_etags.append(etag)
        if len(request_etags) == 1:
            temporary_path.write_bytes(artifact_bytes)
            return acquisition._HttpStreamResult(
                200,
                source_url,
                hashlib.sha256(artifact_bytes).hexdigest(),
                len(artifact_bytes),
                '"etag"',
                "date",
            )
        assert etag == '"etag"'
        return acquisition._HttpStreamResult(
            304, source_url, None, None, '"etag"', "date"
        )

    monkeypatch.setattr(acquisition, "_stream_official_url", fake_stream)
    first = await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    second = await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    assert first.path == second.path
    assert second.path.read_bytes() == artifact_bytes
    assert request_etags == [None, '"etag"']


@pytest.mark.asyncio
async def test_corrupt_durable_retention_fails_closed(tmp_path: Path, monkeypatch) -> None:
    store = _artifact_store(tmp_path)
    artifact_bytes = b"trusted"
    request_etags: list[str | None] = []

    async def fake_stream(
        source_url,
        temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        request_etags.append(etag)
        temporary_path.write_bytes(artifact_bytes)
        return acquisition._HttpStreamResult(
            200,
            source_url,
            hashlib.sha256(artifact_bytes).hexdigest(),
            len(artifact_bytes),
            None,
            None,
        )

    monkeypatch.setattr(acquisition, "_stream_official_url", fake_stream)
    first = await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    first.path.write_bytes(b"corrupt")
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    assert first.path.read_bytes() == b"corrupt"
    assert request_etags == [None, None]


@pytest.mark.parametrize(
    "configured",
    (None, "relative/path", "/", "/tmp/nppes-public-evidence"),
)
def test_artifact_root_fails_closed_for_unsafe_configuration(
    configured: str | None, monkeypatch
) -> None:
    if configured is None:
        monkeypatch.delenv(
            "HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT", raising=False
        )
    else:
        monkeypatch.setenv(
            "HLTHPRT_NPPES_PUBLIC_EVIDENCE_ARTIFACT_ROOT", configured
        )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        acquisition.resolve_nppes_artifact_root()


@pytest.mark.asyncio
async def test_acquisition_normalizes_private_fetch_failures(
    tmp_path: Path, monkeypatch
) -> None:
    store = _artifact_store(tmp_path)

    async def explode(*_args, **_kwargs):
        raise RuntimeError("PRIVATE-HTTP-MARKER")

    monkeypatch.setattr(acquisition, "_stream_official_url", explode)
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    assert str(caught.value) == "nppes_public_evidence_archive_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


@pytest.mark.asyncio
async def test_acquisition_rejects_a_different_canonical_redirect_target(
    tmp_path: Path, monkeypatch
) -> None:
    store = _artifact_store(tmp_path)

    async def redirected(
        _source_url,
        temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        payload = b"redirected"
        temporary_path.write_bytes(payload)
        return acquisition._HttpStreamResult(
            200,
            "https://download.cms.gov/nppes/different.zip",
            hashlib.sha256(payload).hexdigest(),
            len(payload),
            etag,
            None,
        )

    monkeypatch.setattr(acquisition, "_stream_official_url", redirected)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition.acquire_nppes_archive(store, _candidate(), "ab" * 32)
    assert not list((store.root / "nppes-url-index").glob("*.json"))


@pytest.mark.asyncio
async def test_acquisition_preserves_control_cancellation_before_publication(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = _artifact_store(tmp_path)

    async def cancelled_stream(
        _source_url,
        _temporary_path,
        *,
        max_bytes,
        etag,
        cancel_check=None,
    ):
        assert max_bytes > 0
        assert etag is None
        assert cancel_check is not None
        await cancel_check()
        raise AssertionError("cancellation callback returned")

    async def cancel() -> None:
        raise ImportCancelledError("cancelled")

    monkeypatch.setattr(acquisition, "_stream_official_url", cancelled_stream)
    with pytest.raises(ImportCancelledError):
        await acquisition.acquire_nppes_archive(
            store,
            _candidate(),
            "ab" * 32,
            cancel_check=cancel,
        )
    assert not list((store.root / "raw").glob("*"))


@pytest.mark.asyncio
async def test_stream_rejects_truncated_or_encoded_response(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(acquisition_http.aiohttp, "ClientSession", _FakeSession)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition_http.stream_official_url(
            ARCHIVE_URL,
            tmp_path / "truncated",
            max_bytes=100,
            etag=None,
        )

    monkeypatch.setattr(acquisition_http.aiohttp, "ClientSession", _EncodedSession)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await acquisition_http.stream_official_url(
            ARCHIVE_URL,
            tmp_path / "encoded",
            max_bytes=100,
            etag=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_url,precreate_target",
    (
        ("https://download.cms.gov:invalid/nppes/archive.zip", False),
        (ARCHIVE_URL, True),
    ),
)
async def test_http_boundary_normalizes_url_and_filesystem_failures(
    tmp_path: Path,
    monkeypatch,
    source_url: str,
    precreate_target: bool,
) -> None:
    target = tmp_path / "artifact.zip"
    if precreate_target:
        target.write_bytes(b"private-existing-value")
        monkeypatch.setattr(
            acquisition_http.aiohttp,
            "ClientSession",
            _FakeSession,
        )
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        await acquisition_http.stream_official_url(
            source_url,
            target,
            max_bytes=100,
            etag=None,
        )
    assert str(caught.value) == "nppes_public_evidence_archive_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
