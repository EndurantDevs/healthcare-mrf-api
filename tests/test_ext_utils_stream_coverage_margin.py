# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from arq import Retry
from sqlalchemy.exc import SQLAlchemyError

from process.ext import utils
from tests.ext_utils_import_coverage_support import (
    _Client,
    _Request,
    _Response,
    _fixed_download_info,
    _fixed_download_timeout,
)

@pytest.mark.asyncio
async def test_stream_download_cache_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "_head_download_info", _fixed_download_info)
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)

    destination_path = tmp_path / "stream.bin"
    cache_dir = tmp_path / "cache"
    stream_client = _Client(
        get_requests=[
            _Request(
                _Response(
                    status=200,
                    body=b"abcdefgh",
                    headers={"Content-Encoding": "identity"},
                )
            )
        ]
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=stream_client),
    )
    await utils.download_it_and_save(
        "https://example.test/stream",
        str(destination_path),
        cache_dir=cache_dir,
    )
    assert destination_path.read_bytes() == b"abcdefgh"
    assert list(cache_dir.iterdir())

    cached_target = tmp_path / "cached.bin"
    await utils.download_it_and_save(
        "https://example.test/stream",
        str(cached_target),
        cache_dir=cache_dir,
    )
    assert cached_target.read_bytes() == b"abcdefgh"


@pytest.mark.asyncio
async def test_stream_download_resume_and_complete_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)

    resumable = tmp_path / "resume.bin"
    resumable.write_bytes(b"abcd")
    resume_client = _Client(
        get_requests=[
            _Request(
                _Response(
                    status=206,
                    body=b"efgh",
                    headers={"Content-Encoding": "identity"},
                )
            )
        ]
    )
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(8, True)),
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=resume_client),
    )
    await utils.download_it_and_save(
        "https://example.test/resume",
        str(resumable),
        prefer_stream=True,
    )
    assert resumable.read_bytes() == b"abcdefgh"

    complete = tmp_path / "complete.bin"
    complete.write_bytes(b"x" * 1024)
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(1024, True)),
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=_Client()),
    )
    await utils.download_it_and_save(
        "https://example.test/complete",
        str(complete),
    )
    assert complete.read_bytes() == b"x" * 1024


@pytest.mark.asyncio
async def test_stream_download_parallel_fallback(
    tmp_path,
    monkeypatch,
    capsys,
):
    destination_path = tmp_path / "fallback.bin"
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(1024, True)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_THRESHOLD_BYTES", 1)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_RANGE_SIZE", 512)
    monkeypatch.setattr(utils, "PARALLEL_DOWNLOAD_WORKERS", 2)
    monkeypatch.setattr(utils, "PARALLEL_CHUNK_RETRIES", 1)
    monkeypatch.setattr(utils, "LARGE_FILE_TIMEOUT_LOG_THRESHOLD_BYTES", 1)
    monkeypatch.setattr(utils, "PREFER_COMPRESSED_STREAM", False)
    monkeypatch.setattr(utils, "PROGRESS_INTERVAL_SECONDS", 0.0)

    fallback_response = _Response(
        body=b"fallback",
        headers={"Content-Encoding": "gzip"},
    )
    fallback_response.content_length = None
    fallback_client = _Client(
        get_requests=[
            _Request(_Response(status=206, body=b"r" * 512)),
            _Request(_Response(status=200, body=b"range refused")),
            _Request(fallback_response),
        ]
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=fallback_client),
    )
    await utils.download_it_and_save(
        "https://example.test/fallback",
        str(destination_path),
    )
    assert destination_path.read_bytes() == b"fallback"
    output = capsys.readouterr().out
    assert "computed aiohttp timeout for large file" in output
    assert "parallel download failed, falling back to stream" in output
    assert len(fallback_client.calls) == 3


@pytest.mark.asyncio
async def test_stream_download_network_failure_requests_retry(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        utils,
        "_head_download_info",
        AsyncMock(return_value=(8, True)),
    )
    monkeypatch.setattr(utils, "_determine_request_timeout", _fixed_download_timeout)
    log_error = AsyncMock()
    monkeypatch.setattr(utils, "log_error", log_error)
    failed_client = _Client(
        get_requests=[
            _Request(error=aiohttp.ClientError("connection reset")),
        ]
    )
    monkeypatch.setattr(
        utils,
        "get_http_client",
        AsyncMock(return_value=failed_client),
    )
    with pytest.raises(Retry):
        await utils.download_it_and_save(
            "https://example.test/fail",
            str(tmp_path / "failed.bin"),
            context={
                "issuer_array": [1],
                "source": "coverage-test",
            },
            logger=object(),
            prefer_stream=True,
        )
    log_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_error_buffer_flush_success_and_rollback(monkeypatch):
    utils.err_obj_list.clear()
    utils.err_obj_key.clear()
    push = AsyncMock()
    monkeypatch.setattr(utils, "push_objects", push)

    await utils.log_error(
        "error",
        "broken",
        [1, 1],
        "https://example.test",
        "source",
        "network",
        object(),
    )
    assert len(utils.err_obj_list) == 1
    await utils.flush_error_log(object())
    push.assert_awaited_once()
    assert not utils.err_obj_list
    await utils.flush_error_log(object())

    utils.err_obj_list.append(
        {
            "issuer_id": 2,
            "checksum": 22,
            "type": "error",
            "text": "broken",
            "url": "https://example.test",
            "source": "source",
            "level": "network",
        }
    )
    push.reset_mock(side_effect=True)
    push.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await utils.flush_error_log(object())
    assert utils.err_obj_list[0]["checksum"] == 22
    assert utils.err_obj_key[22]


@pytest.mark.asyncio
async def test_slow_insert_fallback_and_database_selection(monkeypatch):
    attempts = []

    class _Statement:
        def values(self, payload):
            self.payload = payload
            return self

        def on_conflict_do_nothing(self, *, index_elements):
            self.index_elements = index_elements
            return self

        async def status(self):
            attempts.append(self.payload)
            if isinstance(self.payload, list):
                raise SQLAlchemyError("batch failed")
            if self.payload["id"] == 2:
                raise SQLAlchemyError("row failed")

    fake_cls = SimpleNamespace(
        __my_index_elements__=["id"],
        __table__=object(),
    )
    monkeypatch.setattr(utils.db, "insert", lambda _target: _Statement())
    await utils.push_objects_slow(
        [{"id": 1}, {"id": 2}],
        fake_cls,
    )
    assert attempts == [
        [{"id": 1}, {"id": 2}],
        {"id": 1},
        {"id": 2},
    ]

    connect = AsyncMock()
    monkeypatch.setattr(utils.db, "connect", connect)
    monkeypatch.setattr(utils, "TEST_DATABASE_SUFFIX", "_test")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "source_db")
    await utils.ensure_database(test_mode=True)
    assert utils.db._database_override == "source_db_test"
    await utils.ensure_database(test_mode=False)
    assert utils.db._database_override is None
    assert connect.await_count == 2


def test_rows_per_insert_and_import_schema_fallbacks(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ROWS_PER_INSERT", "invalid")
    assert utils._default_rows_per_insert() == 1000
    monkeypatch.setenv("HLTHPRT_ROWS_PER_INSERT", "0")
    assert utils._default_rows_per_insert() == 1
    monkeypatch.delenv("COVERAGE_IMPORT_SCHEMA", raising=False)
    assert (
        utils.get_import_schema(
            "COVERAGE_IMPORT_SCHEMA",
            "mrf",
            test_mode=True,
        )
        == "mrf"
    )
