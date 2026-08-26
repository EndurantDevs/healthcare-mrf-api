# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import functools
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from process import uhc_official_file_acquisition as acquisition
from tests.test_uhc_official_file_acquisition import (
    _catalog_file,
    _DownloadSession,
    _FakeSession,
    _ResponseContext,
)


@pytest.mark.asyncio
async def test_reusable_binding_requires_both_verified_files(tmp_path):
    raw_file = tmp_path / "raw"
    manifest_file = tmp_path / "manifest"
    raw_file.write_bytes(b"[]")
    manifest_file.write_bytes(b"{}")
    raw_file.chmod(0o600)
    manifest_file.chmod(0o600)
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "raw_storage_uri": raw_file.as_uri(),
                "manifest_storage_uri": manifest_file.as_uri(),
                "byte_count": 2,
            }
        )
    )
    assert await acquisition._has_reusable_binding(
        connection,
        "a" * 64,
        "source-file",
    )
    connection.fetchrow.return_value = None
    assert not await acquisition._has_reusable_binding(
        connection,
        "a" * 64,
        "source-file",
    )


def test_download_directory_creates_private_non_symlink_root(monkeypatch, tmp_path):
    retained_root = tmp_path / "retained"
    monkeypatch.setattr(
        acquisition,
        "uhc_retained_artifact_root",
        lambda: retained_root,
    )
    download_root = acquisition._download_directory()
    assert download_root == retained_root / "downloads"
    assert download_root.stat().st_mode & 0o777 == 0o700

    download_root.rmdir()
    download_root.symlink_to(tmp_path)
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="staging storage is unavailable",
    ):
        acquisition._download_directory()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "response_url", "headers", "declared_length", "chunks", "match"),
    [
        (404, None, {}, 2, (b"[]",), "exact reviewed URL"),
        (200, "https://example.invalid/redirect", {}, 2, (b"[]",), "exact reviewed URL"),
        (200, None, {"Content-Encoding": "gzip"}, 2, (b"[]",), "content encoding"),
        (200, None, {}, 0, (), "response size is invalid"),
        (200, None, {}, 3, (b"[]",), "response size is invalid"),
        (200, None, {}, None, (b"", b"123"), "configured size limit"),
    ],
)
async def test_stream_download_rejects_transport_contract_drift(
    status,
    response_url,
    headers,
    declared_length,
    chunks,
    match,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    session = _DownloadSession(
        _ResponseContext(
            source_url=catalog_file["source_url"],
            status=status,
            response_url=response_url,
            headers=headers,
            content_length=declared_length,
            chunks=chunks,
        )
    )
    with pytest.raises(acquisition.UHCOfficialFileAcquisitionError, match=match):
        await acquisition._stream_download_response(
            session,
            catalog_file,
            BytesIO(),
            max_bytes=2,
        )


@pytest.mark.asyncio
async def test_stream_download_hashes_identity_encoded_chunks():
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    output = BytesIO()
    digest, byte_count, declared_length = (
        await acquisition._stream_download_response(
            _DownloadSession(
                _ResponseContext(
                    source_url=catalog_file["source_url"],
                    headers={"Content-Encoding": "identity"},
                    content_length=2,
                    chunks=(b"", b"[", b"]"),
                )
            ),
            catalog_file,
            output,
            max_bytes=2,
        )
    )
    assert output.getvalue() == b"[]"
    assert digest.hexdigest() == (
        "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )
    assert byte_count == declared_length == 2


@pytest.mark.parametrize(
    ("byte_count", "declared_length", "catalog_size", "match"),
    [
        (0, None, None, "truncated or empty"),
        (1, 2, None, "truncated or empty"),
        (2, 2, 3, "differs from its catalog identity"),
    ],
)
def test_download_byte_count_rejects_each_mismatch(
    byte_count,
    declared_length,
    catalog_size,
    match,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    catalog_file["size_bytes"] = catalog_size
    with pytest.raises(acquisition.UHCOfficialFileAcquisitionError, match=match):
        acquisition._validate_download_byte_count(
            catalog_file,
            byte_count=byte_count,
            declared_length=declared_length,
        )


@pytest.mark.asyncio
async def test_download_file_persists_valid_response(monkeypatch, tmp_path):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    monkeypatch.setattr(acquisition, "_download_directory", lambda: tmp_path)
    response_context = _ResponseContext(
        source_url=catalog_file["source_url"],
        content_length=2,
        chunks=(b"[]",),
    )
    path, digest, byte_count = await acquisition._download_file(
        _DownloadSession(response_context),
        catalog_file,
        max_bytes=2,
    )
    try:
        assert path.read_bytes() == b"[]"
        assert len(digest) == 64
        assert byte_count == 2
    finally:
        path.unlink()


@pytest.mark.asyncio
async def test_download_file_maps_transport_error_and_cleans_partial(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    monkeypatch.setattr(acquisition, "_download_directory", lambda: tmp_path)
    session = _DownloadSession(
        _ResponseContext(
            source_url=catalog_file["source_url"],
            enter_error=aiohttp.ClientError("offline"),
        )
    )
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="transport is unavailable",
    ):
        await acquisition._download_file(
            session,
            catalog_file,
            max_bytes=2,
        )
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.asyncio
async def test_download_file_cleans_partial_on_non_transport_failure(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    monkeypatch.setattr(acquisition, "_download_directory", lambda: tmp_path)
    monkeypatch.setattr(
        acquisition,
        "_stream_download_response",
        AsyncMock(side_effect=ValueError("malformed response")),
    )
    with pytest.raises(ValueError, match="malformed response"):
        await acquisition._download_file(
            _FakeSession(),
            catalog_file,
            max_bytes=2,
        )
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.asyncio
async def test_download_file_maps_failures_before_temporary_path_exists(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    monkeypatch.setattr(acquisition, "_download_directory", lambda: tmp_path)
    monkeypatch.setattr(
        acquisition.tempfile,
        "NamedTemporaryFile",
        Mock(side_effect=aiohttp.ClientError("unavailable")),
    )
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="transport is unavailable",
    ):
        await acquisition._download_file(
            _FakeSession(),
            catalog_file,
            max_bytes=2,
        )
    acquisition.tempfile.NamedTemporaryFile.side_effect = RuntimeError(
        "local failure"
    )
    with pytest.raises(RuntimeError, match="local failure"):
        await acquisition._download_file(
            _FakeSession(),
            catalog_file,
            max_bytes=2,
        )


@pytest.mark.asyncio
async def test_catalog_file_loop_accepts_absent_callbacks(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    source_path = tmp_path / "download.part"
    source_path.write_bytes(b"[]")
    monkeypatch.setattr(
        acquisition,
        "_has_reusable_binding",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        acquisition,
        "_download_file",
        AsyncMock(return_value=(source_path, "b" * 64, 2)),
    )
    monkeypatch.setattr(
        acquisition,
        "_admit_downloaded_catalog_file",
        AsyncMock(),
    )
    assert await acquisition._acquire_catalog_files(
        object(),
        _FakeSession(),
        "a" * 64,
        (catalog_file,),
        None,
        None,
    ) == (1, 0, 2)


@pytest.mark.asyncio
async def test_catalog_file_downloads_use_bounded_parallelism(
    monkeypatch,
    tmp_path,
):
    """Downloads overlap without exceeding the configured worker bound."""
    catalog_files = tuple(
        _catalog_file(f"JSON_Providers_A{i}IEX.json")
        for i in range(6)
    )
    counts_by_state = {"active": 0, "maximum_active": 0}

    async def download(_session, catalog_file, *, max_bytes):
        assert max_bytes > 0
        counts_by_state["active"] += 1
        counts_by_state["maximum_active"] = max(
            counts_by_state["maximum_active"],
            counts_by_state["active"],
        )
        await asyncio.sleep(0)
        source_path = tmp_path / f"{catalog_file['file_id']}.part"
        source_path.write_bytes(b"[]")
        counts_by_state["active"] -= 1
        return source_path, "b" * 64, 2

    monkeypatch.setenv(
        "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY",
        "3",
    )
    monkeypatch.setattr(
        acquisition,
        "_has_reusable_binding",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(acquisition, "_download_file", download)
    monkeypatch.setattr(
        acquisition,
        "_admit_downloaded_catalog_file",
        AsyncMock(),
    )

    acquisition_result = await acquisition._acquire_catalog_files(
        object(),
        _FakeSession(),
        "a" * 64,
        catalog_files,
        None,
        None,
    )

    assert acquisition_result == (6, 0, 12)
    assert 1 < counts_by_state["maximum_active"] <= 3


async def _admission_test_download(
    tmp_path,
    _session,
    catalog_file,
    *,
    max_bytes,
):
    assert max_bytes > 0
    source_path = tmp_path / f"{catalog_file['file_id']}.part"
    source_path.write_bytes(b"[]")
    return source_path, "b" * 64, 2


async def _counted_catalog_admission(
    counts_by_state,
    _connection,
    _catalog_hash,
    _catalog_file,
    _source_path,
    _artifact_hash,
    _byte_count,
):
    counts_by_state["active"] += 1
    counts_by_state["maximum_active"] = max(
        counts_by_state["maximum_active"],
        counts_by_state["active"],
    )
    await asyncio.sleep(0)
    counts_by_state["active"] -= 1


def _counted_connection_factory(counts_by_state):
    @contextlib.asynccontextmanager
    async def connection_factory():
        counts_by_state["connections"] += 1
        yield object()

    return connection_factory


@pytest.mark.asyncio
async def test_catalog_file_admission_uses_bounded_worker_connections(
    monkeypatch,
    tmp_path,
):
    """Admissions use independent connections within their worker bound."""
    catalog_files = tuple(
        _catalog_file(f"JSON_Providers_B{i}IEX.json")
        for i in range(5)
    )
    counts_by_state = {
        "active": 0,
        "maximum_active": 0,
        "connections": 0,
    }

    monkeypatch.setenv(
        "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY",
        "4",
    )
    monkeypatch.setenv(
        "HLTHPRT_UHC_PROVIDER_FILE_ADMISSION_CONCURRENCY",
        "2",
    )
    monkeypatch.setattr(
        acquisition,
        "_has_reusable_binding",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        acquisition,
        "_download_file",
        functools.partial(_admission_test_download, tmp_path),
    )
    monkeypatch.setattr(
        acquisition,
        "_admit_downloaded_catalog_file",
        functools.partial(_counted_catalog_admission, counts_by_state),
    )
    connection_factory = _counted_connection_factory(counts_by_state)

    acquisition_result = await acquisition._acquire_catalog_files(
        object(),
        _FakeSession(),
        "a" * 64,
        catalog_files,
        None,
        None,
        connection_factory=connection_factory,
    )

    assert acquisition_result == (5, 0, 10)
    assert counts_by_state["connections"] == len(catalog_files)
    assert counts_by_state["maximum_active"] == 2


@pytest.mark.asyncio
async def test_acquisition_closes_owned_session_and_rejects_bad_totals(
    monkeypatch,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    session = _FakeSession()
    client_session_kwargs_by_name = {}
    monkeypatch.setattr(
        acquisition,
        "_selected_catalog_files",
        AsyncMock(return_value=(catalog_file,)),
    )

    def fake_client_session(**kwargs):
        client_session_kwargs_by_name.update(kwargs)
        return session

    monkeypatch.setattr(
        acquisition.aiohttp,
        "ClientSession",
        fake_client_session,
    )
    monkeypatch.setattr(
        acquisition,
        "_acquire_catalog_files",
        AsyncMock(return_value=(0, 0, 0)),
    )
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="did not complete",
    ):
        await acquisition.acquire_complete_uhc_catalog_set(
            object(),
            "a" * 64,
        )
    assert session.closed is True
    assert client_session_kwargs_by_name["auto_decompress"] is False
    assert client_session_kwargs_by_name["headers"] == {
        "User-Agent": "HealthPorta-Official-Provider-Files/1.0",
        "Accept-Encoding": "identity",
    }
