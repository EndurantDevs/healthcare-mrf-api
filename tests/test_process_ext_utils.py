# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime

import pytest

from process.ext import utils


class _Response:
    def __init__(self, headers):
        self.headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _Client:
    def __init__(self, head_result, probe_result=None):
        self.head_result = head_result
        self.probe_result = probe_result
        self.probe_headers = None

    def head(self, *_args, **_kwargs):
        if isinstance(self.head_result, BaseException):
            raise self.head_result
        return self.head_result

    def get(self, *_args, **kwargs):
        self.probe_headers = kwargs["headers"]
        if isinstance(self.probe_result, BaseException):
            raise self.probe_result
        return self.probe_result


def test_download_cache_dir_override_keeps_non_tmp_cache(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DOWNLOAD_CACHE_DIR", "/work/download-cache")

    assert utils._resolve_download_cache_dir("/var/cache/healthporta") == "/var/cache/healthporta"


def test_download_cache_dir_override_moves_tmp_cache(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DOWNLOAD_CACHE_DIR", "/work/download-cache")

    assert utils._resolve_download_cache_dir("/tmp") == "/work/download-cache"


def test_download_cache_dir_override_ignored_when_unset(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DOWNLOAD_CACHE_DIR", raising=False)

    assert utils._resolve_download_cache_dir("/tmp") == "/tmp"


@pytest.mark.parametrize(
    ("raw_datetime", "expected"),
    (
        ("2026-07-25T10:11:12", datetime.datetime(2026, 7, 25, 10, 11, 12)),
        ({"__type__": "datetime", "value": "2026-07-25"}, datetime.datetime(2026, 7, 25)),
        ({"__type__": "datetime", "value": "July 25, 2026"}, datetime.datetime(2026, 7, 25)),
        ({"__type__": "repr", "repr": "2026-07-25"}, datetime.datetime(2026, 7, 25)),
        ({"value": "2026-07-25"}, datetime.datetime(2026, 7, 25)),
        ("July 25, 2026", datetime.datetime(2026, 7, 25)),
    ),
)
def test_coerce_datetime_accepts_supported_serializations(raw_datetime, expected):
    assert utils._coerce_datetime(raw_datetime) == expected


def test_coerce_datetime_rejects_invalid_and_unknown_values():
    current = datetime.datetime.now(datetime.timezone.utc)
    assert utils._coerce_datetime(current) is current
    for raw_datetime in (
        {"__type__": "datetime", "value": "not-a-date"},
        {"__type__": "datetime", "value": 123},
        {"__type__": "unknown", "value": "2026-07-25"},
        "not-a-date",
        123,
        None,
    ):
        assert utils._coerce_datetime(raw_datetime) is None


@pytest.mark.asyncio
async def test_head_download_info_uses_valid_head_metadata(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS", "")
    sized_client = _Client(
        _Response({"Content-Length": "4096", "Accept-Ranges": "bytes"})
    )
    assert await utils._head_download_info(
        sized_client, "https://files.example/data"
    ) == (4096, True)

    no_range_client = _Client(_Response({"Content-Length": "2048"}))
    assert await utils._head_download_info(
        no_range_client, "https://files.example/data"
    ) == (2048, False)


@pytest.mark.asyncio
async def test_head_download_info_probes_small_or_invalid_lengths(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS", "")
    probed_client = _Client(
        _Response({"Content-Length": "bad", "Accept-Ranges": "bytes"}),
        _Response({"Content-Range": "bytes 0-0/12345"}),
    )
    assert await utils._head_download_info(
        probed_client, "https://files.example/data"
    ) == (12345, True)
    assert probed_client.probe_headers == {
        "Range": "bytes=0-0",
        "Accept-Encoding": "identity",
    }

    invalid_probe_client = _Client(
        _Response({"Content-Length": "500", "Accept-Ranges": "bytes"}),
        _Response({"Content-Range": "unknown"}),
    )
    assert await utils._head_download_info(
        invalid_probe_client, "https://files.example/data"
    ) == (500, True)


@pytest.mark.asyncio
async def test_head_download_info_falls_back_after_head_failure(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS", "")
    recovered_client = _Client(
        utils.aiohttp.ClientError("head failed"),
        _Response({"Content-Range": "bytes 0-0/9000"}),
    )
    assert await utils._head_download_info(
        recovered_client, "https://files.example/data"
    ) == (9000, True)

    failed_client = _Client(
        utils.aiohttp.ClientError("head failed"),
        asyncio.TimeoutError(),
    )
    assert await utils._head_download_info(
        failed_client, "https://files.example/data"
    ) == (None, False)


@pytest.mark.asyncio
async def test_head_download_info_respects_disabled_hosts(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PARALLEL_DOWNLOAD_DISABLED_HOSTS",
        "blocked.example,.suffix.example",
    )
    client = _Client(
        _Response({"Content-Length": "700", "Accept-Ranges": "bytes"})
    )
    assert await utils._head_download_info(
        client, "https://blocked.example/data"
    ) == (700, False)
    assert client.probe_headers is None
    assert utils._is_parallel_download_disabled_for_url(
        "https://child.suffix.example/data"
    )
    assert not utils._is_parallel_download_disabled_for_url(
        "https://allowed.example/data"
    )
