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

def test_range_download_resets_oversized_file_and_advances_completed_progress(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "artifact.part"
    path.write_bytes(b"oversized")
    _range_sidecar_path(path).write_text("stale", encoding="utf-8")

    @asynccontextmanager
    async def request(_session, _method, _url, *, headers):
        start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
        yield _Response(
            status=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/2",
                "ETag": '"strong"',
            },
            chunks=[b"ab"],
        )

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **_kwargs: None)
    asyncio.run(
        source_download._download_raw_artifact_ranges(
            url="https://example.test",
            partial_path=path,
            total_bytes=2,
            etag='"strong"',
            max_bytes=None,
            started_at=0,
        )
    )
    assert path.read_bytes() == b"ab"

    path.write_bytes(b"ab")
    source_download._write_completed_ranges(
        _range_sidecar_path(path),
        total_bytes=2,
        etag='"strong"',
        completed={(0, 1)},
    )
    asyncio.run(
        source_download._download_raw_artifact_ranges(
            url="https://example.test",
            partial_path=path,
            total_bytes=2,
            etag='"strong"',
            max_bytes=None,
            started_at=0,
        )
    )


def test_single_get_terminal_retry_sleep_and_incomplete_body(tmp_path, monkeypatch) -> None:
    original_run_single_get_attempt = source_download._run_single_get_attempt

    async def always_fails(**_kwargs):
        raise RuntimeError("offline")

    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(source_download, "_run_single_get_attempt", always_fails)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0.25)
    monkeypatch.setattr(source_download, "_preserve_single_get_partial", lambda *_args: None)
    monkeypatch.setattr(source_download.asyncio, "sleep", sleep)
    with pytest.raises(RuntimeError, match="offline"):
        asyncio.run(
            source_download._download_raw_artifact_single_get(
                url="https://example.test",
                path=tmp_path / "retry",
                head=PTG2HeadMetadata(url="https://example.test"),
                max_bytes=None,
                started_at=0,
            )
        )
    assert sleeps == [0.25]
    monkeypatch.setattr(
        source_download,
        "_run_single_get_attempt",
        original_run_single_get_attempt,
    )

    state = _state(tmp_path / "incomplete")
    response = _Response(status=200, headers={"Content-Length": "3"}, chunks=[b"ab"])

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        yield response

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    with pytest.raises(RuntimeError, match="ended at 2 bytes"):
        asyncio.run(
            source_download._run_single_get_attempt(
                url="https://example.test",
                state=state,
                max_bytes=None,
                started_at=0,
                timeout=aiohttp.ClientTimeout(total=None),
            )
        )


def test_publish_downloaded_raw_rejects_post_publish_checksum(tmp_path, monkeypatch) -> None:
    store = PTG2ArtifactStore(tmp_path / "store")
    staged = tmp_path / "staged"
    staged.write_bytes(b"abc")
    monkeypatch.setattr(source_download, "publish_artifact_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(source_download, "sha256_file", lambda _path: ("b" * 64, 3))
    with pytest.raises(RuntimeError, match="Checksum verification"):
        source_download._publish_downloaded_raw_artifact(
            store,
            staged,
            raw_sha256="a" * 64,
            byte_count=3,
            reuse_raw_artifacts=True,
        )


def test_reuse_candidate_second_protection_failure_disables_reuse(
    tmp_path,
    monkeypatch,
) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_bytes(b"new")
    candidate_path = tmp_path / "candidate"
    candidate_path.write_bytes(b"cached")
    digest = hashlib.sha256(b"cached").hexdigest()
    candidate_by_field = {
        "raw_storage_uri": str(candidate_path),
        "raw_sha256": digest,
    }
    store = PTG2ArtifactStore(tmp_path / "store")
    monkeypatch.setattr(store, "find_candidates", lambda _url: [candidate_by_field])
    monkeypatch.setattr(store, "path_from_uri", lambda _uri: candidate_path)
    monkeypatch.setattr(
        source_download,
        "choose_reusable_raw_artifact",
        lambda *_args, **_kwargs: (candidate_by_field, "etag"),
    )
    protection_call_counts = [0]

    def is_existing_artifact_protected(*_args):
        protection_call_counts[0] += 1
        if protection_call_counts[0] == 2:
            raise ValueError("outside")
        return True

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=3)

    monkeypatch.setattr(
        source_download,
        "protect_existing_artifact",
        is_existing_artifact_protected,
    )
    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    original_exists = Path.exists
    monkeypatch.setattr(
        Path,
        "exists",
        lambda self: False if self == candidate_path else original_exists(self),
    )
    monkeypatch.setattr(source_download, "protect_artifact_path", lambda *_args: None)
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
    assert not downloaded_artifact.reused and protection_call_counts[0] == 2


def test_local_download_emits_progress_and_nonpartial_failure_removes_stage(
    tmp_path,
    monkeypatch,
) -> None:
    source_path = tmp_path / "source.json"
    source_path.write_bytes(b"abc")
    store = PTG2ArtifactStore(tmp_path / "store")
    events = []

    async def head(url):
        return PTG2HeadMetadata(url=url, content_length=3)

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **value: events.append(value))
    monkeypatch.setattr(
        source_download,
        "_raise_for_unexpected_artifact_container",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("bad container")),
    )
    with pytest.raises(RuntimeError, match="bad container"):
        asyncio.run(
            source_download._download_raw_artifact_locked(
                str(source_path),
                store=store,
                canonical_url=str(source_path),
                reuse_raw_artifacts=True,
                max_bytes=None,
                keep_partial_artifacts=False,
            )
        )
    assert any(event["bytes_read"] == 3 for event in events)
    assert not list(store.tmp_dir.glob("*.part"))


@pytest.mark.parametrize("failure_kind", ["limit", "unsafe", "success"])
def test_range_selection_limit_unsafe_and_success(tmp_path, monkeypatch, failure_kind) -> None:
    """Exercise ranged-download selection across limit, safety, and success paths."""
    store = PTG2ArtifactStore(tmp_path / f"store-{failure_kind}")
    url = "https://example.test/rates.json"

    async def head(_url):
        return PTG2HeadMetadata(
            url=url,
            etag='"strong"',
            content_length=4,
            last_modified="now",
        )

    async def probe(*_args):
        return True, 4, '"strong"', url

    async def ranged(*, partial_path, **_kwargs):
        if failure_kind == "unsafe":
            raise UnsafeUrlError("unsafe")
        partial_path.write_bytes(b"data")

    async def single(*_args, **_kwargs):
        raise AssertionError("single GET should not run")

    monkeypatch.setattr(source_download, "fetch_head_metadata", head)
    monkeypatch.setattr(source_download, "_range_download_min_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_probe_http_range_support", probe)
    monkeypatch.setattr(source_download, "_download_raw_artifact_ranges", ranged)
    monkeypatch.setattr(source_download, "_download_raw_artifact_single_get", single)
    monkeypatch.setattr(source_download, "protect_artifact_path", lambda *_args: None)
    if failure_kind == "limit":
        with pytest.raises(RuntimeError, match="max-bytes"):
            _run_range_selection_download(url, store=store, max_bytes=3)
    elif failure_kind == "unsafe":
        with pytest.raises(UnsafeUrlError):
            _run_range_selection_download(url, store=store, max_bytes=None)
    else:
        downloaded_artifact = _run_range_selection_download(
            url,
            store=store,
            max_bytes=None,
        )
        assert Path(downloaded_artifact.raw_path).read_bytes() == b"data"
        assert downloaded_artifact.head.last_modified == "now"


def test_materialize_json_source_and_sync_wrapper(tmp_path, monkeypatch) -> None:
    raw_path = tmp_path / "raw.json"
    raw_path.write_bytes(b"{}")
    raw = _raw(raw_path)
    logical = _logical(raw_path)

    async def download(*_args, **_kwargs):
        return raw

    monkeypatch.setattr(source_download, "download_raw_artifact", download)
    monkeypatch.setattr(source_download, "stream_logical_artifact", lambda *_args, **_kwargs: logical)
    observations = []
    materialized_artifacts = asyncio.run(
        source_download.materialize_json_source(
            raw.original_url,
            tmp_path / "logical",
            artifact_stage_observer=observations.append,
        )
    )
    assert materialized_artifacts == (raw, logical) and len(observations) == 2

    monkeypatch.setattr(
        source_download,
        "_download_ptg_job_artifact",
        lambda *_args, **_kwargs: asyncio.sleep(
            0,
            result=PTG2DownloadedJob(job={"url": "x"}),
        ),
    )
    assert (
        source_download._download_ptg_job_artifact_sync(
            {"url": "x"},
            reuse_raw_artifacts=False,
            max_bytes=None,
            keep_partial_artifacts=False,
        ).error
        is None
    )


def test_gzip_stat_failure_and_misc_formatting_branches(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "rates.json.gz"
    artifact.write_bytes(gzip.compress(b"{}"))
    original_stat = Path.stat

    def stat(path, *args, **kwargs):
        if path == artifact:
            raise OSError("stat unavailable")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)
    assert "not readable" in source_download._gzip_integrity_error(
        str(artifact),
        artifact,
        max_bytes=1,
    )
    assert source_download._format_eta_seconds(float("nan")) == "unknown"
    assert source_download._coerce_download_float(float("inf")) is None

    events = []
    monkeypatch.setattr(source_download, "_emit_screen_line", lambda *args, **kwargs: events.append(args))
    monkeypatch.setattr(source_download, "write_live_progress", lambda **value: events.append(value))
    monkeypatch.setattr(source_download, "current_live_progress_context", lambda: {})
    source_download._emit_download_progress(
        url="opaque-label",
        bytes_read=2,
        total_bytes=None,
        started_at=source_download.time.monotonic(),
        done=False,
    )
    assert events


def test_range_download_retries_transient_failure(tmp_path, monkeypatch) -> None:
    attempt_counts = [0]
    sleeps = []

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        attempt_counts[0] += 1
        if attempt_counts[0] == 1:
            raise RuntimeError("transient")
        yield _Response(
            status=206,
            headers={"Content-Range": "bytes 0-1/2", "ETag": '"strong"'},
            chunks=[b"ab"],
        )

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_delay_seconds", lambda: 0.5)
    monkeypatch.setattr(source_download.asyncio, "sleep", sleep)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **_kwargs: None)
    asyncio.run(
        source_download._download_raw_artifact_ranges(
            url="https://example.test",
            partial_path=tmp_path / "part",
            total_bytes=2,
            etag='"strong"',
            max_bytes=None,
            started_at=0,
        )
    )
    assert attempt_counts[0] == 2 and sleeps == [0.5]


def test_single_get_resumes_complete_prefix_and_advances_threshold(tmp_path, monkeypatch) -> None:
    path = tmp_path / "part"
    path.write_bytes(b"ab")
    source_download._write_completed_ranges(
        _range_sidecar_path(path),
        total_bytes=4,
        etag='"strong"',
        completed={(0, 1)},
    )
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    state = source_download._single_get_download_state(
        path,
        PTG2HeadMetadata(
            url="https://example.test",
            etag='"strong"',
            content_length=4,
            last_modified="then",
        ),
    )
    assert state.byte_count == 2 and state.next_progress_bytes == 3

    response = _Response(
        status=206,
        headers={
            "Content-Range": "bytes 2-3/4",
            "ETag": '"strong"',
        },
        chunks=[b"cd"],
    )
    requested_headers = []

    @asynccontextmanager
    async def request(*_args, **kwargs):
        requested_headers.append(kwargs["headers"])
        yield response

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **_kwargs: None)
    asyncio.run(
        source_download._run_single_get_attempt(
            url="https://example.test",
            state=state,
            max_bytes=None,
            started_at=0,
            timeout=aiohttp.ClientTimeout(total=None),
        )
    )
    assert path.read_bytes() == b"abcd"
    assert requested_headers == [{"Range": "bytes=2-", "If-Match": '"strong"'}]
