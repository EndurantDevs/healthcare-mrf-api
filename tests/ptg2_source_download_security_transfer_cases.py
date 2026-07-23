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

def test_single_get_retry_and_terminal_error_branches(tmp_path, monkeypatch) -> None:
    attempts = []

    async def fail_then_succeed(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise RuntimeError("disconnect")

    monkeypatch.setattr(source_download, "_run_single_get_attempt", fail_then_succeed)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0)
    monkeypatch.setattr(source_download, "_preserve_single_get_partial", lambda *_args: None)
    download_summary = asyncio.run(
        source_download._download_raw_artifact_single_get(
            url="https://example.test",
            path=tmp_path / "retry",
            head=PTG2HeadMetadata(url="https://example.test"),
            max_bytes=None,
            started_at=0,
        )
    )
    assert len(attempts) == 2 and download_summary[1] == 0

    async def unsafe(**_kwargs):
        raise UnsafeUrlError("unsafe")

    monkeypatch.setattr(source_download, "_run_single_get_attempt", unsafe)
    with pytest.raises(UnsafeUrlError):
        asyncio.run(
            source_download._download_raw_artifact_single_get(
                url="https://example.test",
                path=tmp_path / "unsafe",
                head=PTG2HeadMetadata(url="https://example.test"),
                max_bytes=None,
                started_at=0,
            )
        )

    async def too_large(**_kwargs):
        raise source_download._DownloadSizeLimitError("large")

    monkeypatch.setattr(source_download, "_run_single_get_attempt", too_large)
    with pytest.raises(source_download._DownloadSizeLimitError):
        asyncio.run(
            source_download._download_raw_artifact_single_get(
                url="https://example.test",
                path=tmp_path / "large",
                head=PTG2HeadMetadata(url="https://example.test"),
                max_bytes=1,
                started_at=0,
            )
        )


@pytest.mark.parametrize("manifest_fails", [False, True])
def test_local_download_limit_preserves_partial_manifest(tmp_path, monkeypatch, manifest_fails) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_bytes(b"abc")
    store = PTG2ArtifactStore(tmp_path / "store")
    manifests = []

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=3)

    def record(value):
        if manifest_fails:
            raise RuntimeError("manifest unavailable")
        manifests.append(value)

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(source_download, "protect_artifact_path", lambda *_args: None)
    monkeypatch.setattr(store, "record_manifest", record)
    with pytest.raises(RuntimeError, match="max-bytes"):
        asyncio.run(
            source_download._download_raw_artifact_locked(
                str(source_path),
                store=store,
                canonical_url=str(source_path),
                reuse_raw_artifacts=False,
                max_bytes=1,
                keep_partial_artifacts=True,
            )
        )
    if manifest_fails:
        assert not manifests
    else:
        assert manifests[-1]["status"] == "partial"


def test_reuse_candidate_missing_external_unprotected_and_valid(tmp_path, monkeypatch) -> None:
    source_path = tmp_path / "download.json"
    source_path.write_bytes(b"new")
    store = PTG2ArtifactStore(tmp_path / "store")
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_bytes(b"cached")
    digest = hashlib.sha256(b"cached").hexdigest()
    candidate_by_field = {
        "raw_storage_uri": str(candidate_path),
        "raw_sha256": digest,
        "source_file_version_id": "source-1",
    }

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=len(b"cached"))

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(
        store,
        "find_candidates",
        lambda _url: [{}, {"raw_storage_uri": "outside"}, candidate_by_field],
    )
    monkeypatch.setattr(
        store,
        "path_from_uri",
        lambda uri: candidate_path if uri == str(candidate_path) else tmp_path / "outside",
    )
    protected_calls = []

    def is_existing_artifact_protected(_store, path):
        protected_calls.append(path)
        if path.name == "outside":
            raise ValueError("outside")
        return True

    monkeypatch.setattr(
        source_download,
        "protect_existing_artifact",
        is_existing_artifact_protected,
    )
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
    assert downloaded_artifact.reused and downloaded_artifact.raw_sha256 == digest
    assert any(path.name == "outside" for path in protected_calls)


def test_should_materialize_handles_oserror(monkeypatch) -> None:
    monkeypatch.setattr(source_download.zipfile, "is_zipfile", lambda _path: (_ for _ in ()).throw(OSError("bad")))
    assert source_download._should_materialize_logical_artifact("bad.zip") is False


def test_retained_logical_skips_invalid_size_and_external_candidates(tmp_path, monkeypatch) -> None:
    raw_path = tmp_path / "raw.zip"
    with zipfile.ZipFile(raw_path, "w") as archive:
        archive.writestr("rates.json", "{}")
    store = PTG2ArtifactStore(tmp_path / "store")
    raw = _raw(raw_path)
    external = tmp_path / "external.json"
    external.write_bytes(b"{}")
    digest = hashlib.sha256(b"{}").hexdigest()
    monkeypatch.setattr(
        store,
        "find_logical_candidates",
        lambda _digest: [
            {},
            {"logical_storage_uri": str(external), "logical_sha256": "bad"},
            {
                "logical_storage_uri": str(external),
                "logical_sha256": digest,
                "byte_count": "bad",
            },
            {
                "logical_storage_uri": str(external),
                "logical_sha256": digest,
                "byte_count": 2,
            },
        ],
    )
    monkeypatch.setattr(store, "path_from_uri", lambda _uri: external)
    monkeypatch.setattr(
        source_download,
        "protect_existing_artifact",
        lambda *_args: (_ for _ in ()).throw(ValueError("external")),
    )
    monkeypatch.setattr(source_download, "protect_artifact_prefix", lambda *_args: None)
    retained_artifact = asyncio.run(
        source_download._retained_logical_artifact(store, raw)
    )
    assert not retained_artifact.reused
    assert Path(retained_artifact.logical_path).read_bytes() == b"{}"


def test_facades_observers_and_worker_success_error_paths(tmp_path, monkeypatch) -> None:
    raw_path = tmp_path / "raw.json"
    raw_path.write_bytes(b"{}")
    raw = _raw(raw_path, reused=True)
    logical = _logical(raw_path, reused=True, deferred=True)
    observations = []
    source_download._observe_raw_artifact_stage(observations.append, raw)
    source_download._observe_logical_artifact_stage(observations.append, logical)
    source_download._observe_raw_artifact_stage(None, raw)
    source_download._observe_logical_artifact_stage(None, logical)
    assert len(observations) == 2 and observations[1].logical_hash_deferred

    module = types.SimpleNamespace(materialize_json_source=lambda: "facade")
    monkeypatch.setitem(sys.modules, "process.ptg", module)
    assert source_download._materialize_json_source_from_facade()() == "facade"
    del module.materialize_json_source
    assert source_download._materialize_json_source_from_facade() is source_download.materialize_json_source

    async def downloaded(*_args, **_kwargs):
        return raw

    monkeypatch.setattr(source_download, "download_raw_artifact", downloaded)
    monkeypatch.setattr(source_download, "logical_artifact_identity", lambda *_args, **_kwargs: logical)
    downloaded_job = asyncio.run(
        source_download._download_ptg_job_artifact(
            {"url": raw.original_url},
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=False,
            artifact_stage_observer=observations.append,
        )
    )
    assert downloaded_job.error is None and downloaded_job.logical_artifact is logical

    async def failed(*_args, **_kwargs):
        raise RuntimeError("download failed")

    monkeypatch.setattr(source_download, "download_raw_artifact", failed)
    downloaded_job = asyncio.run(
        source_download._download_ptg_job_artifact(
            {"url": raw.original_url},
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=False,
        )
    )
    assert downloaded_job.error == "download failed"


def test_sync_facade_resets_context_on_failure_and_plain_path(monkeypatch) -> None:
    module = types.SimpleNamespace()
    calls = []

    def downloader(job, **kwargs):
        calls.append((job, kwargs))
        if job.get("fail"):
            raise RuntimeError("boom")
        return PTG2DownloadedJob(job=job)

    module._download_ptg_job_artifact_sync = downloader
    monkeypatch.setitem(sys.modules, "process.ptg", module)
    resets = []
    monkeypatch.setattr(source_download, "set_live_progress_context", lambda **_kwargs: "token")
    monkeypatch.setattr(source_download, "reset_live_progress_context", resets.append)
    assert source_download._download_ptg_job_artifact_sync_from_facade({"url": "x"}).error is None
    with pytest.raises(RuntimeError, match="boom"):
        source_download._download_ptg_job_artifact_sync_from_facade(
            {"url": "x", "fail": True},
            live_progress_context={"file_index": 1},
        )
    assert resets == ["token"] and len(calls) == 2


def test_download_scheduler_assigns_monotonic_indexes_without_job_metadata(monkeypatch) -> None:
    monkeypatch.setenv(source_download.PTG2_DOWNLOAD_TASKS_ENV, "1")
    captured_contexts = []

    def download(job, **kwargs):
        captured_contexts.append(kwargs["live_progress_context"])
        return PTG2DownloadedJob(job=job)

    monkeypatch.setattr(
        source_download,
        "_download_ptg_job_artifact_sync_from_facade",
        download,
    )

    async def collect():
        return [
            item
            async for item in source_download._iter_downloaded_ptg_jobs(
                [{"url": f"https://example.test/{index}.json"} for index in range(3)],
                reuse_raw_artifacts=False,
                max_bytes=None,
                keep_partial_artifacts=False,
            )
        ]

    assert len(asyncio.run(collect())) == 3
    assert [context["file_index"] for context in captured_contexts] == [1, 2, 3]
    assert all(context["file_count"] == 3 for context in captured_contexts)


def test_stage_tracker_logical_and_validation_failures() -> None:
    tracker = source_download.PTG2FreshArtifactStageTracker()
    valid = source_download.PTG2ArtifactStageObservation(
        artifact_kind=source_download.PTG2_ARTIFACT_LOGICAL_JSON,
        identity_sha256="a" * 64,
        byte_count=1,
        logical_hash_deferred=True,
    )
    tracker.observe(valid)
    counts = tracker.snapshot()
    assert counts.logical_artifacts_total == 1
    assert counts.logical_artifacts_deferred_hashes == 1

    invalid_values = [
        {"artifact_kind": "unknown"},
        {"identity_sha256": "A" * 64},
        {"byte_count": True},
        {"reused": 1},
        {"logical_hash_deferred": 1},
    ]
    for changes in invalid_values:
        observation_values_by_field = {
            "artifact_kind": source_download.PTG2_ARTIFACT_RAW,
            "identity_sha256": "b" * 64,
            "byte_count": 1,
            "reused": False,
            "logical_hash_deferred": False,
        }
        observation_values_by_field.update(changes)
        with pytest.raises(ValueError):
            tracker.observe(
                source_download.PTG2ArtifactStageObservation(
                    **observation_values_by_field
                )
            )


def test_range_validation_etag_status_and_resume_sidecar(tmp_path) -> None:
    response = _Response(
        status=200,
        headers={"Content-Range": "bytes 0-0/1", "ETag": '"same"'},
    )
    with pytest.raises(source_download._UnsafeRangeResponseError, match="status"):
        source_download._validate_range_response(
            response,
            url="https://example.test",
            expected_start=0,
            expected_end=0,
            expected_total=1,
            expected_etag='"same"',
        )
    response.status = 206
    response.headers["ETag"] = '"other"'
    with pytest.raises(source_download._UnsafeRangeResponseError, match="changed ETag"):
        source_download._validate_range_response(
            response,
            url="https://example.test",
            expected_start=0,
            expected_end=0,
            expected_total=1,
            expected_etag='"same"',
        )

    path = tmp_path / "partial"
    path.write_bytes(b"ab")
    source_download._write_completed_ranges(
        _range_sidecar_path(path),
        total_bytes=4,
        etag='"strong"',
        completed={(0, 1)},
    )
    assert (
        source_download._validated_resume_offset(
            path,
            total_bytes=4,
            etag='"strong"',
        )
        == 2
    )
    path.write_bytes(b"abcd")
    assert source_download._validated_resume_offset(path, total_bytes=4, etag='"strong"') == 0


def test_environment_host_and_small_container_branches(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(
        source_download.INCOMPLETE_TLS_CHAIN_HOSTS_ENV,
        " First.test,second.test  ",
    )
    assert source_download._incomplete_tls_chain_hosts() == {"first.test", "second.test"}
    assert source_download._request_ssl_kwargs("https://first.test/rates") == {"ssl": False}

    not_gzip = tmp_path / "wrong.json.gz"
    not_gzip.write_bytes(b"no")
    assert "gzip header" in source_download._gzip_integrity_error(str(not_gzip), not_gzip)
    assert source_download._zip_container_error(str(tmp_path / "plain.json"), tmp_path / "plain.json") is None

