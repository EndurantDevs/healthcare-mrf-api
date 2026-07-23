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

def test_container_validation_covers_unreadable_corrupt_empty_and_size_skip(tmp_path) -> None:
    plain = tmp_path / "rates.json"
    plain.write_bytes(b"{}")
    assert source_download._artifact_container_error(str(plain), plain) is None

    unreadable = tmp_path / "unreadable.json.gz"
    unreadable.mkdir()
    assert "not readable" in source_download._gzip_magic_error(str(unreadable), unreadable)

    corrupt = tmp_path / "corrupt.json.gz"
    corrupt.write_bytes(b"\x1f\x8bnot-a-valid-gzip")
    assert "integrity check" in source_download._gzip_integrity_error(str(corrupt), corrupt)
    assert source_download._gzip_integrity_error(str(corrupt), corrupt, max_bytes=1) is None

    empty_zip = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty_zip, "w") as empty_archive:
        assert empty_archive.namelist() == []
    assert "no file members" in source_download._zip_container_error(str(empty_zip), empty_zip)
    invalid_zip = tmp_path / "invalid.zip"
    invalid_zip.write_bytes(b"not zip")
    assert "not a readable ZIP" in source_download._zip_container_error(str(invalid_zip), invalid_zip)

    valid_gzip = tmp_path / "valid.json.gz"
    valid_gzip.write_bytes(gzip.compress(b"{}"))
    assert (
        source_download._artifact_container_error(
            str(valid_gzip),
            valid_gzip,
            validate_gzip_integrity=True,
        )
        is None
    )


def test_resume_offset_and_progress_coercion_failure_paths(tmp_path) -> None:
    path = tmp_path / "partial"
    path.write_bytes(b"abc")
    assert source_download._validated_resume_offset(path, total_bytes=None, etag='"x"') == 0
    assert source_download._validated_resume_offset(path, total_bytes=5, etag="weak") == 0
    assert source_download._progress_job_index({"_ptg_progress_index": object()}) == 0
    assert source_download._progress_job_total({"_ptg_progress_total": object()}, 0) == 1


def test_redirect_rewrites_303_and_rejects_missing_or_excessive_locations(monkeypatch) -> None:
    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    responses = iter(
        [
            _Response(status=303, headers={"Location": "/final"}),
            _Response(status=200, url="https://example.test/final"),
        ]
    )
    session = _Session(lambda *_args: next(responses))
    response = asyncio.run(
        source_download._open_validated_request(
            session,
            "POST",
            "https://example.test/start",
            data=b"x",
            json={"x": 1},
        )
    )
    assert response.status == 200
    assert [call[0] for call in session.calls] == ["POST", "GET"]
    assert "data" not in session.calls[1][2] and "json" not in session.calls[1][2]

    missing = _Session(_Response(status=302))
    with pytest.raises(RuntimeError, match="redirect target is missing"):
        asyncio.run(
            source_download._open_validated_request(
                missing,
                "GET",
                "https://example.test/start",
            )
        )

    looping = _Session(
        lambda _method, url, _kwargs: _Response(
            status=307,
            url=url,
            headers={"Location": "/again"},
        )
    )
    with pytest.raises(RuntimeError, match="too many redirects"):
        asyncio.run(
            source_download._open_validated_request(
                looping,
                "GET",
                "https://example.test/start",
            )
        )
    assert len(looping.calls) == source_download._MAX_REDIRECTS + 1


def test_validated_request_releases_response(monkeypatch) -> None:
    response = _Response()

    async def opened(*_args, **_kwargs):
        return response

    monkeypatch.setattr(source_download, "_open_validated_request", opened)

    async def use_request():
        async with source_download._validated_request(
            _Session(),
            "GET",
            "https://example.test/rates.json",
        ) as yielded:
            assert yielded is response

    asyncio.run(use_request())
    assert response.released


def test_fetch_head_metadata_handles_client_error(monkeypatch) -> None:
    async def safe(_url):
        return None

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(
        source_download.aiohttp,
        "ClientSession",
        lambda **_kwargs: _Session(aiohttp.ClientConnectionError("offline")),
    )
    result = asyncio.run(source_download.fetch_head_metadata("https://example.test/rates.json"))
    assert result.supports_head is False


@pytest.mark.parametrize(
    ("response", "etag", "expected"),
    [
        (_Response(status=200), None, (False, None, None, None)),
        (
            _Response(status=206, headers={"Content-Range": "bad"}, read_payload=b"x"),
            None,
            (False, None, None, None),
        ),
        (
            _Response(
                status=206,
                headers={"Content-Range": "bytes 1-1/3"},
                read_payload=b"x",
            ),
            None,
            (False, None, None, None),
        ),
        (
            _Response(
                status=206,
                headers={"Content-Range": "bytes 0-0/3", "ETag": '"changed"'},
                read_payload=b"x",
            ),
            '"expected"',
            (False, None, None, None),
        ),
        (
            _Response(
                status=206,
                url="https://cdn.example.test/rates",
                headers={"Content-Range": "bytes 0-0/3", "ETag": '"fresh"'},
                read_payload=b"x",
            ),
            None,
            (True, 3, '"fresh"', "https://cdn.example.test/rates"),
        ),
    ],
)
def test_probe_range_response_shapes(monkeypatch, response, etag, expected) -> None:
    async def safe(_url):
        return None

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        yield response

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    assert (
        asyncio.run(
            source_download._probe_http_range_support(
                "https://example.test/rates",
                etag,
            )
        )
        == expected
    )


def test_probe_range_swallows_request_failure(monkeypatch) -> None:
    async def safe(_url):
        return None

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        raise RuntimeError("network")
        yield

    monkeypatch.setattr(source_download, "assert_safe_url", safe)
    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    assert asyncio.run(source_download._probe_http_range_support("https://example.test")) == (
        False,
        None,
        None,
        None,
    )


def test_range_download_streams_progress_and_cleans_sidecar(tmp_path, monkeypatch) -> None:
    path = tmp_path / "artifact.part"
    progress = []

    @asynccontextmanager
    async def request(_session, _method, _url, *, headers):
        start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
        payload = b"abcd"[start : end + 1]
        yield _Response(
            status=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/4",
                "ETag": '"strong"',
            },
            chunks=[b"", payload],
        )

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **value: progress.append(value))
    asyncio.run(
        source_download._download_raw_artifact_ranges(
            url="https://example.test/rates",
            partial_path=path,
            total_bytes=4,
            etag='"strong"',
            max_bytes=4,
            started_at=0.0,
        )
    )
    assert path.read_bytes() == b"abcd"
    assert not _range_sidecar_path(path).exists()
    assert progress[-1]["bytes_read"] == 4


def test_range_download_guards_and_rolls_back_failed_count(tmp_path, monkeypatch) -> None:
    with pytest.raises(RuntimeError, match="strong ETag"):
        asyncio.run(
            source_download._download_raw_artifact_ranges(
                url="https://example.test/rates",
                partial_path=tmp_path / "weak",
                total_bytes=2,
                etag=None,
                max_bytes=None,
                started_at=0,
            )
        )
    with pytest.raises(RuntimeError, match="max-bytes"):
        asyncio.run(
            source_download._download_raw_artifact_ranges(
                url="https://example.test/rates",
                partial_path=tmp_path / "large",
                total_bytes=2,
                etag='"strong"',
                max_bytes=1,
                started_at=0,
            )
        )

    path = tmp_path / "short"

    @asynccontextmanager
    async def request(*_args, **_kwargs):
        yield _Response(
            status=206,
            headers={"Content-Range": "bytes 0-1/2", "ETag": '"strong"'},
            chunks=[b"x"],
        )

    monkeypatch.setattr(source_download, "_validated_request", request)
    monkeypatch.setattr(source_download.aiohttp, "ClientSession", _Session)
    monkeypatch.setattr(source_download, "_range_download_chunk_bytes", lambda: 2)
    monkeypatch.setattr(source_download, "_range_download_tasks", lambda: 1)
    monkeypatch.setattr(source_download, "_download_retry_count", lambda: 0)
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    with pytest.raises(RuntimeError, match="returned 1 bytes"):
        asyncio.run(
            source_download._download_raw_artifact_ranges(
                url="https://example.test/rates",
                partial_path=path,
                total_bytes=2,
                etag='"strong"',
                max_bytes=None,
                started_at=0,
            )
        )


def test_prepare_single_get_response_rejects_unsafe_resume_and_full_shapes(tmp_path) -> None:
    state = _state(
        tmp_path / "part",
        byte_count=2,
        total_bytes=4,
        validator='"same"',
    )
    with pytest.raises(source_download._UnsafeRangeResponseError, match="status"):
        source_download._prepare_single_get_response(
            _Response(status=200),
            state=state,
            url="https://example.test",
            is_resume_request=True,
        )
    with pytest.raises(source_download._UnsafeRangeResponseError, match="changed ETag"):
        source_download._prepare_single_get_response(
            _Response(
                status=206,
                headers={"Content-Range": "bytes 2-3/4", "ETag": '"other"'},
            ),
            state=state,
            url="https://example.test",
            is_resume_request=True,
        )
    assert (
        source_download._prepare_single_get_response(
            _Response(
                status=206,
                headers={"Content-Range": "bytes 2-3/4", "ETag": '"same"'},
            ),
            state=state,
            url="https://example.test",
            is_resume_request=True,
        )
        == "ab"
    )
    with pytest.raises(source_download._UnsafeRangeResponseError, match="unexpectedly"):
        source_download._prepare_single_get_response(
            _Response(status=206),
            state=state,
            url="https://example.test",
            is_resume_request=False,
        )


def test_stream_single_get_enforces_declared_and_streamed_limits_and_reports(tmp_path, monkeypatch) -> None:
    known = _state(tmp_path / "known", total_bytes=3)
    with pytest.raises(source_download._DownloadSizeLimitError, match="max-bytes"):
        asyncio.run(
            source_download._stream_single_get_response(
                _Response(chunks=[b"abc"]),
                state=known,
                url="https://example.test",
                file_mode="wb",
                max_bytes=2,
                started_at=0,
            )
        )

    progress = []
    streamed = _state(tmp_path / "streamed", next_progress_bytes=1)
    monkeypatch.setattr(source_download, "_download_progress_interval_bytes", lambda: 1)
    monkeypatch.setattr(source_download, "_emit_download_progress", lambda **value: progress.append(value))
    with pytest.raises(source_download._DownloadSizeLimitError, match="max-bytes"):
        asyncio.run(
            source_download._stream_single_get_response(
                _Response(chunks=[b"", b"a", b"bc"]),
                state=streamed,
                url="https://example.test",
                file_mode="wb",
                max_bytes=2,
                started_at=0,
            )
        )
    assert progress and streamed.byte_count == 3


def test_preserve_single_get_partial_writes_resume_or_resets(tmp_path) -> None:
    path = tmp_path / "part"
    path.write_bytes(b"ab")
    state = _state(path, byte_count=2, total_bytes=4, validator='"strong"')
    source_download._preserve_single_get_partial(state, RuntimeError("disconnect"))
    assert _range_sidecar_path(path).exists()

    source_download._preserve_single_get_partial(
        state,
        source_download._UnsafeRangeResponseError("bad range"),
    )
    assert not path.exists()
    assert state.byte_count == 0 and state.validator is None

