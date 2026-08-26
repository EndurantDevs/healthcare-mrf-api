# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

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
