# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import binascii
import gzip
import hashlib
import io
import struct
import zipfile
from pathlib import Path

import pytest
from aiohttp import web

from process.ptg_parts import artifact_streams, artifacts, config, source_download
from process.ptg_parts.domain import PTG2_ARTIFACT_RAW, PTG2HeadMetadata


def _configure_range_downloads(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(config.PTG2_RANGE_DOWNLOADS_ENV, "true")
    monkeypatch.setenv(config.PTG2_RANGE_DOWNLOAD_MIN_BYTES_ENV, "1")
    monkeypatch.setenv(config.PTG2_RANGE_DOWNLOAD_TASKS_ENV, "1")
    monkeypatch.setenv(config.PTG2_DOWNLOAD_RETRIES_ENV, "0")
    monkeypatch.setenv(config.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV, "0")


async def _download_from_handler(tmp_path: Path, handler, prepare=None, *, query: str = ""):
    app = web.Application()
    app.router.add_route("*", "/artifact.bin", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}/artifact.bin{query}"
    store = artifacts.PTG2ArtifactStore(tmp_path)
    if prepare is not None:
        prepare(store, url)
    try:
        return await source_download.download_raw_artifact(url, store=store)
    finally:
        await runner.cleanup()


def _zip_bytes(payload: bytes = b'{"in_network":[]}') -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("rates.json", payload)
    return output.getvalue()


def _configure_single_get_downloads(monkeypatch, *, retries: int = 1) -> None:
    monkeypatch.setenv("HLTHPRT_FETCH_ALLOW_LOCAL", "true")
    monkeypatch.setenv(config.PTG2_RANGE_DOWNLOADS_ENV, "false")
    monkeypatch.setenv(config.PTG2_DOWNLOAD_RETRIES_ENV, str(retries))
    monkeypatch.setenv(config.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV, "0")


@pytest.mark.parametrize(
    ("filename", "artifact_bytes"),
    [
        ("rates.zip", _zip_bytes()),
        ("rates.json.gz", gzip.compress(b'{"in_network":[]}')),
    ],
)
def test_query_named_container_retries_http_200_html(
    monkeypatch,
    tmp_path,
    filename,
    artifact_bytes,
):
    html_bytes = b"<html><body>temporary upstream error</body></html>"
    get_requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": str(len(artifact_bytes))})
        get_requests.append(request.path_qs)
        if len(get_requests) == 1:
            return web.Response(body=html_bytes, content_type="text/html")
        return web.Response(body=artifact_bytes)

    _configure_single_get_downloads(monkeypatch)
    artifact = asyncio.run(
        _download_from_handler(
            tmp_path,
            handle,
            query=f"?FileName={filename}",
        )
    )

    assert Path(artifact.raw_path).read_bytes() == artifact_bytes
    assert len(get_requests) == 2
    available_manifest_entries = [
        entry
        for entry in artifacts.PTG2ArtifactStore(tmp_path)._manifest_entries()
        if entry.get("status") == "available"
    ]
    assert [entry["raw_sha256"] for entry in available_manifest_entries] == [
        hashlib.sha256(artifact_bytes).hexdigest()
    ]


def test_query_named_zip_rejects_cached_html(monkeypatch, tmp_path):
    html_bytes = b"<html><body>cached upstream error</body></html>"
    zip_bytes = _zip_bytes()
    get_requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": str(len(zip_bytes))})
        get_requests.append(request.path_qs)
        return web.Response(body=zip_bytes)

    def prepare(store, url):
        digest = hashlib.sha256(html_bytes).hexdigest()
        raw_path = store.artifact_path(digest, kind=PTG2_ARTIFACT_RAW)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(html_bytes)
        store.record_manifest(
            {
                "artifact_kind": PTG2_ARTIFACT_RAW,
                "canonical_url": source_download.canonicalize_url(url),
                "raw_storage_uri": store.storage_uri(raw_path),
                "raw_sha256": digest,
                "content_length": len(html_bytes),
                "status": "available",
            }
        )

    _configure_single_get_downloads(monkeypatch, retries=0)
    artifact = asyncio.run(
        _download_from_handler(
            tmp_path,
            handle,
            prepare=prepare,
            query="?FileName=rates.zip",
        )
    )

    assert Path(artifact.raw_path).read_bytes() == zip_bytes
    assert len(get_requests) == 1
    manifest_entries = artifacts.PTG2ArtifactStore(tmp_path)._manifest_entries()
    assert any(
        entry.get("status") == "corrupt"
        and entry.get("raw_sha256") == hashlib.sha256(html_bytes).hexdigest()
        for entry in manifest_entries
    )


def test_query_named_zip_rejects_ranges_before_full_get(monkeypatch, tmp_path):
    invalid_range_bytes = b"<html><body>temporary upstream error</body></html>"
    zip_bytes = _zip_bytes()
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={
                    "Content-Length": str(len(invalid_range_bytes)),
                    "ETag": '"stable"',
                }
            )
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header:
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text or len(invalid_range_bytes) - 1)
            return web.Response(
                status=206,
                body=invalid_range_bytes[start : end + 1],
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(invalid_range_bytes)}",
                    "ETag": '"stable"',
                },
            )
        return web.Response(body=zip_bytes)

    _configure_range_downloads(monkeypatch)
    artifact = asyncio.run(
        _download_from_handler(
            tmp_path,
            handle,
            query="?FileName=rates.zip",
        )
    )

    assert Path(artifact.raw_path).read_bytes() == zip_bytes
    assert requests == [
        "bytes=0-0",
        f"bytes=0-{len(invalid_range_bytes) - 1}",
        None,
    ]


def test_shifted_content_range_falls_back_to_full_get(monkeypatch, tmp_path):
    artifact_bytes = b"0123456789abcdef" * 64
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(
                headers={"Content-Length": str(len(artifact_bytes)), "ETag": '"stable"'}
            )
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header == "bytes=0-0":
            return web.Response(
                status=206,
                body=artifact_bytes[:1],
                headers={"Content-Range": f"bytes 0-0/{len(artifact_bytes)}", "ETag": '"stable"'},
            )
        if range_header:
            start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
            start = int(start_text)
            end = int(end_text)
            return web.Response(
                status=206,
                body=artifact_bytes[start : end + 1],
                headers={
                    "Content-Range": f"bytes {start + 1}-{end + 1}/{len(artifact_bytes)}",
                    "ETag": '"stable"',
                },
            )
        return web.Response(body=artifact_bytes, headers={"ETag": '"stable"'})

    _configure_range_downloads(monkeypatch)
    artifact = asyncio.run(_download_from_handler(tmp_path, handle))

    assert Path(artifact.raw_path).read_bytes() == artifact_bytes
    assert requests == ["bytes=0-0", f"bytes=0-{len(artifact_bytes) - 1}", None]


def test_no_etag_discards_stale_partial_and_uses_single_get(monkeypatch, tmp_path):
    old_payload = b"A" * 1024
    current_payload = b"B" * 1024
    requests = []

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": str(len(current_payload))})
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header == "bytes=0-0":
            return web.Response(
                status=206,
                body=old_payload[:1],
                headers={"Content-Range": f"bytes 0-0/{len(old_payload)}"},
            )
        return web.Response(body=current_payload)

    def prepare(store, url):
        partial_path = store.partial_path(
            source_download.canonicalize_url(url),
            suffix=artifacts._safe_url_suffix(url),
        )
        partial_path.write_bytes(old_payload[: len(old_payload) // 2])

    _configure_range_downloads(monkeypatch)
    artifact = asyncio.run(_download_from_handler(tmp_path, handle, prepare=prepare))

    assert Path(artifact.raw_path).read_bytes() == current_payload
    assert requests == ["bytes=0-0", None]


def test_no_etag_retry_restarts_full_get_after_version_change(monkeypatch, tmp_path):
    old_payload = b"A" * (1024 * 1024)
    current_payload = b"B" * (1024 * 1024)
    requests = []
    request_counts_by_kind = {"full": 0}

    async def handle(request):
        if request.method == "HEAD":
            return web.Response(headers={"Content-Length": str(len(current_payload))})
        range_header = request.headers.get("Range")
        requests.append(range_header)
        if range_header == "bytes=0-0":
            return web.Response(
                status=206,
                body=old_payload[:1],
                headers={"Content-Range": f"bytes 0-0/{len(old_payload)}"},
            )
        request_counts_by_kind["full"] += 1
        if request_counts_by_kind["full"] == 1:
            response = web.StreamResponse(headers={"Content-Length": str(len(old_payload))})
            await response.prepare(request)
            await response.write(old_payload[: len(old_payload) // 2])
            request.transport.close()
            return response
        return web.Response(body=current_payload)

    _configure_range_downloads(monkeypatch)
    monkeypatch.setenv(config.PTG2_DOWNLOAD_RETRIES_ENV, "1")
    artifact = asyncio.run(_download_from_handler(tmp_path, handle))

    assert Path(artifact.raw_path).read_bytes() == current_payload
    assert requests == ["bytes=0-0", None, None]


def _zip_end_record(central_size: int, central_offset: int) -> bytes:
    return struct.pack(
        "<IHHHHIIH",
        0x06054B50,
        0,
        0,
        1,
        1,
        central_size,
        central_offset,
        0,
    )


def _write_deflate64_zip(
    path: Path,
    member_payload: bytes,
    *,
    corrupt_crc: bool = False,
    truncate_stream: bool = False,
) -> None:
    inflate64 = pytest.importorskip("inflate64")
    member_name = b"rates.json"
    deflater = inflate64.Deflater()
    compressed = deflater.deflate(member_payload) + deflater.flush()
    if truncate_stream:
        compressed = compressed[:-1]
    crc = binascii.crc32(member_payload) & 0xFFFFFFFF
    if corrupt_crc:
        crc ^= 0xFFFFFFFF
    local_header = struct.pack(
        "<IHHHHHIIIHH",
        0x04034B50,
        45,
        0,
        artifact_streams.ZIP_DEFLATE64_METHOD,
        0,
        0,
        crc,
        len(compressed),
        len(member_payload),
        len(member_name),
        0,
    )
    central_header = struct.pack(
        "<IHHHHHHIIIHHHHHII",
        0x02014B50,
        45,
        45,
        0,
        artifact_streams.ZIP_DEFLATE64_METHOD,
        0,
        0,
        crc,
        len(compressed),
        len(member_payload),
        len(member_name),
        0,
        0,
        0,
        0,
        0,
        0,
    )
    central_offset = len(local_header) + len(member_name) + len(compressed)
    central_size = len(central_header) + len(member_name)
    end_record = _zip_end_record(central_size, central_offset)
    path.write_bytes(local_header + member_name + compressed + central_header + member_name + end_record)


@pytest.mark.parametrize(
    "archive_options, expected_error",
    [
        ({"corrupt_crc": True}, zipfile.BadZipFile),
        ({"truncate_stream": True}, EOFError),
    ],
)
def test_deflate64_rejects_corrupt_members(tmp_path, archive_options, expected_error):
    raw_path = tmp_path / "rates.zip"
    _write_deflate64_zip(raw_path, b'{"in_network":[]}', **archive_options)

    with pytest.raises(expected_error):
        with artifact_streams.open_json_artifact_stream(raw_path) as fp:
            fp.read()


@pytest.mark.parametrize("compression", ["gzip", "zip"])
def test_logical_artifact_decompression_limit(monkeypatch, tmp_path, compression):
    payload = b'{"in_network":[]}' * 16
    raw_path = tmp_path / f"rates.{compression}"
    if compression == "gzip":
        raw_path.write_bytes(gzip.compress(payload))
    else:
        with zipfile.ZipFile(raw_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_ref:
            zip_ref.writestr("rates.json", payload)
    monkeypatch.setenv(artifact_streams.PTG2_MAX_DECOMPRESSED_BYTES_ENV, str(len(payload) - 1))

    with pytest.raises(artifact_streams.DecompressedArtifactTooLargeError):
        artifact_streams.logical_artifact_identity(raw_path)
    with pytest.raises(artifact_streams.DecompressedArtifactTooLargeError):
        artifact_streams.stream_logical_artifact(raw_path, output_dir=tmp_path / "logical")

    assert not list((tmp_path / "logical").glob("*"))


@pytest.mark.parametrize(
    "hash_fields",
    [
        {},
        {"raw_sha256": "not-a-sha256"},
        {"sha256": "a" * 64},
    ],
)
def test_raw_artifact_without_valid_raw_sha256_is_not_reusable(tmp_path, hash_fields):
    raw_path = tmp_path / "raw.json"
    raw_path.write_bytes(b"{}")
    candidate_by_field = {
        "artifact_kind": PTG2_ARTIFACT_RAW,
        "raw_storage_uri": raw_path.resolve().as_uri(),
        "content_length": raw_path.stat().st_size,
        "etag": '"stable"',
        **hash_fields,
    }
    head = PTG2HeadMetadata(
        url="https://example.test/raw.json",
        content_length=raw_path.stat().st_size,
        etag='"stable"',
    )

    reused, mode = artifacts.choose_reusable_raw_artifact(
        [candidate_by_field],
        head,
        store=artifacts.PTG2ArtifactStore(tmp_path / "store"),
    )

    assert reused is None
    assert mode is None
