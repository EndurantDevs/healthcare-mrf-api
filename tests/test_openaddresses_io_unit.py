import importlib
import asyncio
import json

import pytest

from process.control_cancel import ImportCancelledError


openaddresses = importlib.import_module("process.openaddresses")
control_imports = importlib.import_module("api.control_imports")

class _FakeDownloadContent:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_chunked(self, _chunk_size):
        for chunk in self._chunks:
            yield chunk


class _FakeDownloadResponse:
    def __init__(self, status, *, body="", chunks=()):
        self.status = status
        self._body = body
        self.content = _FakeDownloadContent(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def text(self):
        return self._body


class _FakeDownloadClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_openaddresses_download_retries_transient_http_status(monkeypatch, tmp_path):
    sleeps = []
    client = _FakeDownloadClient(
        [
            _FakeDownloadResponse(504, body="gateway timeout"),
            _FakeDownloadResponse(200, chunks=[b"abc", b"def"]),
        ]
    )

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(openaddresses.asyncio, "sleep", fake_sleep)
    path = tmp_path / "source.geojson.gz"

    await openaddresses._download_file(
        client,
        "https://openaddresses.test/source.geojson.gz",
        path,
        "token",
        task={"download_retries": 1},
    )

    assert path.read_bytes() == b"abcdef"
    assert len(client.calls) == 2
    assert sleeps == [openaddresses.DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS]


@pytest.mark.asyncio
async def test_openaddresses_download_does_not_retry_non_transient_status(tmp_path):
    client = _FakeDownloadClient([_FakeDownloadResponse(404, body="missing")])

    with pytest.raises(RuntimeError, match="HTTP 404"):
        await openaddresses._download_file(
            client,
            "https://openaddresses.test/source.geojson.gz",
            tmp_path / "source.geojson.gz",
            "token",
            task={"download_retries": 1},
        )

    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_openaddresses_flush_uses_copy_first(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, *, rewrite, use_copy):
        calls.append((list(rows), cls, rewrite, use_copy))

    rows = [{"row_hash": "a" * 64}]
    monkeypatch.setattr(openaddresses, "push_objects", fake_push_objects)

    accepted = await openaddresses._flush_rows(rows, object)

    assert accepted == 1
    assert rows == []
    assert calls == [([{"row_hash": "a" * 64}], object, True, True)]


@pytest.mark.asyncio
async def test_openaddresses_repairs_legacy_stage_row_hash_width(monkeypatch):
    statuses = []

    class FakeDb:
        async def first(self, _stmt, **params):
            assert params == {"schema": "mrf", "table_name": "openaddresses_geocode_202606151230024"}
            return {"data_type": "character varying", "character_maximum_length": 32}

        async def status(self, stmt, **_params):
            statuses.append(stmt)

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    await openaddresses._ensure_openaddresses_stage_schema(
        "openaddresses_geocode_202606151230024",
        "mrf",
    )

    assert statuses == [
        'ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ALTER COLUMN row_hash TYPE varchar(64);',
        'ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ADD COLUMN IF NOT EXISTS zip5_source text;',
        'ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ADD COLUMN IF NOT EXISTS zip5_restored_at timestamptz;',
    ]


@pytest.mark.asyncio
async def test_openaddresses_keeps_current_stage_row_hash_width(monkeypatch):
    statuses = []

    class FakeDb:
        async def first(self, _stmt, **_params):
            return {"data_type": "character varying", "character_maximum_length": 64}

        async def status(self, stmt, **_params):
            statuses.append(stmt)

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    await openaddresses._ensure_openaddresses_stage_schema(
        "openaddresses_geocode_202606151230024",
        "mrf",
    )

    assert statuses == [
        'ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ADD COLUMN IF NOT EXISTS zip5_source text;',
        'ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ADD COLUMN IF NOT EXISTS zip5_restored_at timestamptz;',
    ]


@pytest.mark.asyncio
async def test_openaddresses_local_files_load_in_parallel(monkeypatch, tmp_path):
    paths = []
    for index in range(3):
        path = tmp_path / f"source-{index}.geojson"
        path.write_text("{}", encoding="utf-8")
        paths.append(path)

    concurrency_by_metric = {"active": 0, "max_active": 0}

    async def fake_load_file(path, **_kwargs):
        concurrency_by_metric["active"] += 1
        concurrency_by_metric["max_active"] = max(
            concurrency_by_metric["max_active"],
            concurrency_by_metric["active"],
        )
        await asyncio.sleep(0.01)
        concurrency_by_metric["active"] -= 1
        return 10, 5, 1, {"missing_zip5": 1, "not_point": 4}

    monkeypatch.setattr(openaddresses, "_load_file", fake_load_file)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(
        {"context": {"test_mode": False}},
        {"local_files": [str(path) for path in paths], "source_concurrency": 3},
        object,
        object,
    )

    assert stats == {
        "processed_files": 3,
        "processed_rows": 30,
        "accepted_rows": 15,
        "zip_recovery_rows": 3,
        "rejected_rows": 12,
        "rejection_counts": {"missing_zip5": 3, "not_point": 12},
        "zip_restore_shards": 64,
        "zip_restore_concurrency": openaddresses.DEFAULT_ZIP_RESTORE_CONCURRENCY,
    }
    assert concurrency_by_metric["max_active"] > 1


@pytest.mark.asyncio
async def test_openaddresses_remote_sources_load_in_parallel(monkeypatch):
    source_entries = [
        {"source": f"us/tx/source-{index}", "layer": "addresses", "output": {"output": True}, "id": index, "job": index}
        for index in range(1, 4)
    ]
    concurrency_by_metric = {"active": 0, "max_active": 0}

    async def fake_fetch_json(_client, _url, _token):
        return source_entries

    async def fake_load_source_item(**kwargs):
        concurrency_by_metric["active"] += 1
        concurrency_by_metric["max_active"] = max(
            concurrency_by_metric["max_active"],
            concurrency_by_metric["active"],
        )
        await asyncio.sleep(0.01)
        concurrency_by_metric["active"] -= 1
        return kwargs["source_item"]["source"], 10, 5, 1, {
            "missing_zip5": 1,
            "not_point": 4,
        }

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(
        {"context": {"test_mode": False}},
        {"source_concurrency": 3, "max_files": 3},
        object,
        object,
    )

    assert stats == {
        "processed_files": 3,
        "processed_rows": 30,
        "accepted_rows": 15,
        "zip_recovery_rows": 3,
        "rejected_rows": 12,
        "rejection_counts": {"missing_zip5": 3, "not_point": 12},
        "zip_restore_shards": 64,
        "zip_restore_concurrency": openaddresses.DEFAULT_ZIP_RESTORE_CONCURRENCY,
    }
    assert concurrency_by_metric["max_active"] > 1


@pytest.mark.asyncio
async def test_openaddresses_remote_tempdir_ignores_cleanup_errors(monkeypatch, tmp_path):
    tempdir_kwargs = []
    source_entries = [
        {
            "source": "us/tx/source-1",
            "layer": "addresses",
            "output": {"output": True},
            "id": 1,
            "job": 1,
        }
    ]

    class FakeTemporaryDirectory:
        def __init__(self, **kwargs):
            tempdir_kwargs.append(kwargs)

        def __enter__(self):
            return str(tmp_path)

        def __exit__(self, *_args):
            return False

    async def fake_fetch_json(_client, _url, _token):
        return source_entries

    async def fake_load_source_item(**kwargs):
        return kwargs["source_item"]["source"], 10, 5, 1, {
            "missing_zip5": 1,
            "not_point": 4,
        }

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses.tempfile, "TemporaryDirectory", FakeTemporaryDirectory)
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(
        {"context": {"test_mode": False}},
        {"source_concurrency": 1, "max_files": 1},
        object,
        object,
    )

    assert stats == {
        "processed_files": 1,
        "processed_rows": 10,
        "accepted_rows": 5,
        "zip_recovery_rows": 1,
        "rejected_rows": 4,
        "rejection_counts": {"missing_zip5": 1, "not_point": 4},
        "zip_restore_shards": 64,
        "zip_restore_concurrency": openaddresses.DEFAULT_ZIP_RESTORE_CONCURRENCY,
    }
    assert tempdir_kwargs == [{"ignore_cleanup_errors": True}]


@pytest.mark.asyncio
async def test_openaddresses_remote_test_mode_honors_source_concurrency(monkeypatch):
    source_entries = [
        {"source": f"us/ca/test-{index}", "layer": "addresses", "output": {"output": True}, "id": index, "job": index}
        for index in range(1, 4)
    ]
    concurrency_by_metric = {"active": 0, "max_active": 0}

    async def fake_fetch_json(_client, _url, _token):
        return source_entries

    async def fake_load_source_item(**kwargs):
        assert kwargs["settings"].row_limit == 10
        concurrency_by_metric["active"] += 1
        concurrency_by_metric["max_active"] = max(
            concurrency_by_metric["max_active"],
            concurrency_by_metric["active"],
        )
        await asyncio.sleep(0.01)
        concurrency_by_metric["active"] -= 1
        return kwargs["source_item"]["source"], 10, 5, 1, {
            "missing_zip5": 1,
            "not_point": 4,
        }

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(
        {"context": {"test_mode": True}},
        {"source_concurrency": 2, "test_file_limit": 3, "test_row_limit": 10},
        object,
        object,
    )

    assert stats == {
        "processed_files": 3,
        "processed_rows": 30,
        "accepted_rows": 15,
        "zip_recovery_rows": 3,
        "rejected_rows": 12,
        "rejection_counts": {"missing_zip5": 3, "not_point": 12},
        "zip_restore_shards": 64,
        "zip_restore_concurrency": openaddresses.DEFAULT_ZIP_RESTORE_CONCURRENCY,
    }
    assert concurrency_by_metric["max_active"] > 1
