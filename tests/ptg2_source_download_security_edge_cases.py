# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from tests.ptg2_source_download_security_support import (
    asyncio,
    gzip,
    hashlib,
    sys,
    types,
    zipfile,
    asynccontextmanager,
    Path,
    aiohttp,
    pytest,
    source_download,
    PTG2ArtifactStore,
    _range_sidecar_path,
    PTG2DownloadedJob,
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
    UnsafeUrlError,
    _Content,
    _Response,
    _Session,
    _raw,
    _logical,
    _run_range_selection_download,
    _state,
)

def test_corrupt_reuse_candidate_is_recorded_before_fresh_download(
    tmp_path,
    monkeypatch,
) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_bytes(b"new")
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_bytes(b"cached")
    candidate_by_field = {
        "raw_storage_uri": "candidate-uri",
        "raw_sha256": "a" * 64,
    }
    store = PTG2ArtifactStore(tmp_path / "store")
    manifests = []

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=3)

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(
        store,
        "find_candidates",
        lambda _url: [candidate_by_field],
    )
    monkeypatch.setattr(store, "path_from_uri", lambda _uri: candidate_path)
    monkeypatch.setattr(store, "record_manifest", manifests.append)
    monkeypatch.setattr(source_download, "protect_existing_artifact", lambda *_args: True)
    monkeypatch.setattr(source_download, "protect_artifact_path", lambda *_args: None)
    monkeypatch.setattr(
        source_download,
        "choose_reusable_raw_artifact",
        lambda *_args, **_kwargs: (candidate_by_field, "etag"),
    )
    downloaded_artifact = asyncio.run(
        source_download._download_raw_artifact_locked(
            str(source_path),
            store=store,
            canonical_url=str(source_path),
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=True,
        )
    )
    assert not downloaded_artifact.reused
    assert manifests[0]["status"] == "corrupt"


def test_fresh_truncated_gzip_fails_full_integrity_check(tmp_path, monkeypatch) -> None:
    source = tmp_path / "source.json.gz"
    source.write_bytes(b"\x1f\x8btruncated")
    store = PTG2ArtifactStore(tmp_path / "store")

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=source.stat().st_size)

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(source_download, "protect_artifact_path", lambda *_args: None)
    monkeypatch.setenv(source_download._GZIP_VALIDATE_FRESH_ENV, "true")
    with pytest.raises(
        source_download._UnexpectedArtifactContainerError,
        match="integrity check",
    ):
        asyncio.run(
            source_download._download_raw_artifact_locked(
                str(source),
                store=store,
                canonical_url=str(source),
                reuse_raw_artifacts=False,
                max_bytes=None,
                keep_partial_artifacts=True,
            )
        )

