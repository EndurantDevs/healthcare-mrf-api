# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import functools
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from process import uhc_official_file_acquisition as acquisition
from process.uhc_retained_registry_contract import (
    expected_catalog_file_hash_pair,
)


def _catalog_file(file_name: str) -> dict[str, object]:
    source_url = (
        "https://providermrf.uhc.com/api/stream/ui/ifp/providers/"
        + file_name
    )
    catalog_modified_at = "2026-07-28T08:00:00Z"
    catalog_entry_sha256, file_id = expected_catalog_file_hash_pair(
        family="ifp",
        collection_kind="provider_membership",
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        size_bytes=None,
    )
    return {
        "file_id": file_id,
        "family": "ifp",
        "collection_kind": "provider_membership",
        "file_name": file_name,
        "source_url": source_url,
        "catalog_modified_at": catalog_modified_at,
        "catalog_entry_sha256": catalog_entry_sha256,
        "size_bytes": None,
        "availability": "published",
        "catalog_support": "cataloged",
    }


class _FakeSession:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _ChunkStream:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _chunk_bytes):
        for chunk in self._chunks:
            yield chunk


class _ResponseContext:
    def __init__(
        self,
        *,
        source_url,
        status=200,
        response_url=None,
        headers=None,
        content_length=None,
        chunks=(),
        enter_error=None,
    ):
        self._enter_error = enter_error
        self.response = SimpleNamespace(
            status=status,
            url=response_url or source_url,
            headers=headers or {},
            content_length=content_length,
            content=_ChunkStream(chunks),
        )

    async def __aenter__(self):
        if self._enter_error is not None:
            raise self._enter_error
        return self.response

    async def __aexit__(self, *_args):
        return False


class _DownloadSession:
    def __init__(self, response_context):
        self.response_context = response_context

    def get(self, _source_url, *, allow_redirects):
        assert allow_redirects is False
        return self.response_context


def test_large_source_concurrency_rejects_unbounded_configuration(
    monkeypatch,
):
    monkeypatch.setenv(
        "HLTHPRT_UHC_PROVIDER_FILE_DOWNLOAD_CONCURRENCY",
        str(acquisition.MAX_DOWNLOAD_CONCURRENCY + 1),
    )
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="must not exceed",
    ):
        acquisition.uhc_provider_file_download_concurrency()

    monkeypatch.setenv(
        "HLTHPRT_UHC_PROVIDER_FILE_ADMISSION_CONCURRENCY",
        str(acquisition.MAX_ADMISSION_CONCURRENCY + 1),
    )
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="must not exceed",
    ):
        acquisition.uhc_provider_file_admission_concurrency()


@pytest.mark.asyncio
async def test_catalog_task_cleanup_cancels_work_and_removes_stages(tmp_path):
    staged_path = tmp_path / "download.part"
    staged_path.write_bytes(b"partial")
    pending_task = asyncio.create_task(asyncio.Event().wait())

    await acquisition._cleanup_catalog_acquisition_tasks(
        (pending_task,),
        {staged_path},
    )

    assert pending_task.cancelled()
    assert not staged_path.exists()


@pytest.mark.asyncio
async def test_missing_file_worker_uses_shared_or_independent_connection(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    shared_connection = object()
    worker_connection = object()
    admitted_connections: list[object] = []

    @contextlib.asynccontextmanager
    async def connection_factory():
        yield worker_connection

    async def admit(
        connection,
        _catalog_hash,
        _catalog_file,
        temporary_path,
        _artifact_hash,
        _byte_count,
    ):
        admitted_connections.append(connection)
        temporary_path.unlink()

    monkeypatch.setattr(
        acquisition,
        "_admit_downloaded_catalog_file",
        admit,
    )
    for ordinal, factory in enumerate((None, connection_factory)):
        temporary_path = tmp_path / f"download-{ordinal}.part"
        temporary_path.write_bytes(b"[]")
        monkeypatch.setattr(
            acquisition,
            "_download_file",
            AsyncMock(return_value=(temporary_path, "b" * 64, 2)),
        )
        context = acquisition._catalog_acquisition_context(
            shared_connection,
            factory,
            _FakeSession(),
            "a" * 64,
            set(),
        )
        await acquisition._acquire_missing_catalog_file(
            context,
            catalog_file,
        )

    assert admitted_connections == [shared_connection, worker_connection]


@pytest.mark.asyncio
async def test_acquisition_reuses_exact_binding_and_admits_missing_file(
    monkeypatch,
    tmp_path,
):
    catalog_hash = "a" * 64
    reused_file = _catalog_file("JSON_Providers_ALIEX.json")
    downloaded_file = _catalog_file("JSON_Providers_AZIEX.json")
    source_path = tmp_path / "download.part"
    source_path.write_bytes(b"[]")
    progress = AsyncMock()
    cancel = AsyncMock()
    admit = AsyncMock()
    reusable = AsyncMock(
        side_effect=lambda _connection, _catalog_hash, source_file_id: (
            source_file_id == reused_file["file_id"]
        )
    )
    monkeypatch.setattr(
        acquisition,
        "_selected_catalog_files",
        AsyncMock(return_value=(reused_file, downloaded_file)),
    )
    monkeypatch.setattr(acquisition, "_has_reusable_binding", reusable)
    monkeypatch.setattr(
        acquisition,
        "_download_file",
        AsyncMock(return_value=(source_path, "b" * 64, 2)),
    )
    monkeypatch.setattr(acquisition, "admit_retained_source", admit)
    session = _FakeSession()

    acquisition_result = await acquisition.acquire_complete_uhc_catalog_set(
        object(),
        catalog_hash,
        progress_callback=progress,
        cancel_check=cancel,
        session=session,
    )

    assert acquisition_result == acquisition.UHCOfficialFileAcquisitionResult(
        catalog_set_sha256=catalog_hash,
        file_count=2,
        downloaded_file_count=1,
        reused_file_count=1,
        downloaded_byte_count=2,
    )
    assert cancel.await_count == 2
    assert progress.await_count == 2
    admit.assert_awaited_once()
    assert admit.await_args.kwargs["binding"].source_file_id == (
        downloaded_file["file_id"]
    )
    assert not source_path.exists()
    assert session.closed is False


@pytest.mark.asyncio
async def test_acquisition_removes_download_when_native_admission_fails(
    monkeypatch,
    tmp_path,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    source_path = tmp_path / "download.part"
    source_path.write_bytes(b"[]")
    monkeypatch.setattr(
        acquisition,
        "_selected_catalog_files",
        AsyncMock(return_value=(catalog_file,)),
    )
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
        "admit_retained_source",
        AsyncMock(
            side_effect=acquisition.UHCRetainedAdmissionError(
                "native verifier rejected bytes"
            )
        ),
    )

    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="retained admission failed",
    ):
        await acquisition.acquire_complete_uhc_catalog_set(
            object(),
            "a" * 64,
            session=_FakeSession(),
        )

    assert not source_path.exists()


def test_catalog_binding_rejects_identity_or_availability_drift():
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    acquisition._catalog_file_binding_fields("a" * 64, catalog_file)

    catalog_file["availability"] = "missing"
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="identity or availability",
    ):
        acquisition._catalog_file_binding_fields("a" * 64, catalog_file)


@pytest.mark.parametrize("raw_value", ["nope", "0", "-1", str(2**63)])
def test_positive_environment_integer_rejects_invalid_values(
    monkeypatch,
    raw_value,
):
    monkeypatch.setenv("SYNTHETIC_POSITIVE_INTEGER", raw_value)
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="must be a positive integer",
    ):
        acquisition._positive_environment_integer(
            "SYNTHETIC_POSITIVE_INTEGER",
            7,
        )


def test_environment_integer_defaults_and_accepts_positive_value(monkeypatch):
    monkeypatch.delenv("SYNTHETIC_POSITIVE_INTEGER", raising=False)
    assert (
        acquisition._positive_environment_integer(
            "SYNTHETIC_POSITIVE_INTEGER",
            7,
        )
        == 7
    )
    monkeypatch.setenv("SYNTHETIC_POSITIVE_INTEGER", "11")
    assert (
        acquisition._positive_environment_integer(
            "SYNTHETIC_POSITIVE_INTEGER",
            7,
        )
        == 11
    )
    assert acquisition._file_max_bytes() > 0
    assert acquisition._download_timeout_seconds() > 0


def test_row_mapping_accepts_none_mapping_and_record_wrapper():
    assert acquisition._row_mapping(None) == {}
    assert acquisition._row_mapping({"a": 1}) == {"a": 1}
    assert acquisition._row_mapping(SimpleNamespace(_mapping={"b": 2})) == {
        "b": 2
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("catalog_entry_sha256", "0" * 64),
        ("file_id", "unexpected"),
        ("availability", "missing"),
        ("catalog_support", "unsupported"),
    ],
)
def test_catalog_binding_rejects_each_identity_dimension(field, value):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    catalog_file[field] = value
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="identity or availability",
    ):
        acquisition._catalog_file_binding_fields("a" * 64, catalog_file)


class _CatalogConnection:
    def __init__(self, catalog_row, catalog_records):
        self.catalog_row = catalog_row
        self.catalog_records = catalog_records

    async def fetchrow(self, *_args):
        return self.catalog_row

    async def fetch(self, *_args):
        return self.catalog_records


@pytest.mark.asyncio
async def test_selected_catalog_files_rejects_missing_set():
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="was not found",
    ):
        await acquisition._selected_catalog_files(
            _CatalogConnection(None, ()),
            "a" * 64,
        )


@pytest.mark.asyncio
async def test_selected_catalog_files_validates_and_returns_mapping_rows(
    monkeypatch,
):
    catalog_file = _catalog_file("JSON_Providers_ALIEX.json")
    monkeypatch.setattr(
        acquisition,
        "logical_scopes_for_current_census",
        lambda source_files: tuple(source_files),
    )
    result = await acquisition._selected_catalog_files(
        _CatalogConnection(
            {
                "file_count": 1,
                "provider_file_count": 1,
                "plan_reference_file_count": 0,
            },
            (catalog_file,),
        ),
        "a" * 64,
    )
    assert result == (catalog_file,)


@pytest.mark.parametrize(
    "catalog_row",
    [
        {
            "file_count": 2,
            "provider_file_count": 1,
            "plan_reference_file_count": 0,
        },
        {
            "file_count": 1,
            "provider_file_count": 0,
            "plan_reference_file_count": 0,
        },
        {
            "file_count": 1,
            "provider_file_count": 1,
            "plan_reference_file_count": 1,
        },
        {
            "file_count": 0,
            "provider_file_count": 1,
            "plan_reference_file_count": 0,
        },
    ],
)
def test_selected_catalog_count_validation_rejects_each_mismatch(catalog_row):
    with pytest.raises(
        acquisition.UHCOfficialFileAcquisitionError,
        match="catalog set is incomplete",
    ):
        acquisition._validate_selected_catalog_counts(
            catalog_row,
            (_catalog_file("JSON_Providers_ALIEX.json"),),
        )


@pytest.mark.parametrize(
    "storage_uri",
    [
        None,
        "https://example.invalid/file",
        "file://host/tmp/file",
        "file:///tmp/file?query=1",
        "file:///tmp/file#fragment",
        "file:///path/that/does/not/exist",
    ],
)
def test_retained_file_usability_rejects_invalid_locators(storage_uri):
    assert acquisition._is_retained_file_usable(storage_uri) is False


def test_retained_file_usability_checks_permissions_size_and_type(tmp_path):
    retained_file = tmp_path / "retained.json"
    retained_file.write_bytes(b"[]")
    retained_file.chmod(0o600)
    storage_uri = retained_file.as_uri()

    assert acquisition._is_retained_file_usable(storage_uri, 2)
    assert not acquisition._is_retained_file_usable(storage_uri, "2")
    assert not acquisition._is_retained_file_usable(storage_uri, 3)
    retained_file.chmod(0o622)
    assert not acquisition._is_retained_file_usable(storage_uri, 2)
    assert not acquisition._is_retained_file_usable(tmp_path.as_uri())


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
    client_session_kwargs = {}
    monkeypatch.setattr(
        acquisition,
        "_selected_catalog_files",
        AsyncMock(return_value=(catalog_file,)),
    )

    def fake_client_session(**kwargs):
        client_session_kwargs.update(kwargs)
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
    assert client_session_kwargs["auto_decompress"] is False
    assert client_session_kwargs["headers"] == {
        "User-Agent": "HealthPorta-Official-Provider-Files/1.0",
        "Accept-Encoding": "identity",
    }
