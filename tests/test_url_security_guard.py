import ipaddress
import socket
import urllib.request

import pytest

from process import mrf_source_discovery, url_security
from process.url_security import (
    UnsafeUrlError,
    assert_public_ip,
    assert_safe_url,
    assert_safe_url_sync,
    fetch_max_bytes,
    urlopen_safe,
)


NON_GLOBAL_ADDRESSES = [
    "100.64.1.1",  # CGNAT / Tailscale shared space (not caught by is_private)
    "198.18.0.1",  # benchmarking range
    "192.0.0.8",  # IETF protocol assignments
    "169.254.169.254",  # link-local / cloud metadata
    "127.0.0.1",
    "10.0.0.1",
    "0.0.0.0",
]


class _ResponseHeaders:
    def __init__(self, charset):
        self._charset = charset

    def get_content_charset(self):
        return self._charset


class _StreamingResponse:
    def __init__(self, *, final_url, chunks, charset=None):
        self._final_url = final_url
        self._chunks = list(chunks)
        self.headers = _ResponseHeaders(charset)
        self.read_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def geturl(self):
        return self._final_url

    def read(self, size):
        assert size == 64 * 1024
        self.read_count += 1
        return self._chunks.pop(0)


class _ResponseOpener:
    def __init__(self, response):
        self._response = response
        self.opened_request = None
        self.timeout = None

    def open(self, request, *, timeout):
        self.opened_request = request
        self.timeout = timeout
        return self._response


def _public_dns_answers(hostname, port, **lookup_options):
    assert hostname
    assert lookup_options == {"type": socket.SOCK_STREAM}
    return [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port)),
    ]


def _install_streaming_response(monkeypatch, response):
    opener = _ResponseOpener(response)
    installed_handlers = []

    def build_opener(*handlers):
        installed_handlers.extend(handlers)
        return opener

    monkeypatch.setattr(socket, "getaddrinfo", _public_dns_answers)
    monkeypatch.setattr(urllib.request, "build_opener", build_opener)
    return opener, installed_handlers


@pytest.mark.parametrize("address", NON_GLOBAL_ADDRESSES)
def test_assert_public_ip_rejects_non_global(monkeypatch, address):
    monkeypatch.delenv("HLTHPRT_FETCH_ALLOW_LOCAL", raising=False)
    with pytest.raises(UnsafeUrlError):
        assert_public_ip(ipaddress.ip_address(address))


@pytest.mark.parametrize("address", NON_GLOBAL_ADDRESSES)
def test_discovery_assert_public_ip_rejects_non_global(address):
    with pytest.raises(ValueError):
        mrf_source_discovery._assert_public_ip(ipaddress.ip_address(address))


def test_assert_public_ip_allows_global():
    assert_public_ip(ipaddress.ip_address("8.8.8.8"))
    mrf_source_discovery._assert_public_ip(ipaddress.ip_address("8.8.8.8"))


@pytest.mark.parametrize("configured_value", ["invalid", "0", "-1"])
def test_fetch_size_limit_falls_back_for_invalid_configuration(
    monkeypatch,
    configured_value,
):
    monkeypatch.setenv(url_security.FETCH_MAX_BYTES_ENV, configured_value)

    assert fetch_max_bytes(1024) == 1024


def test_fetch_size_limit_uses_call_site_default_when_unconfigured(monkeypatch):
    monkeypatch.delenv(url_security.FETCH_MAX_BYTES_ENV, raising=False)

    assert fetch_max_bytes(1024) == 1024


def test_fetch_size_limit_accepts_positive_configuration(monkeypatch):
    monkeypatch.setenv(url_security.FETCH_MAX_BYTES_ENV, "2048")

    assert fetch_max_bytes(1024) == 2048


def test_private_addresses_require_explicit_local_fetch_opt_in(monkeypatch):
    monkeypatch.setenv(url_security.FETCH_ALLOW_LOCAL_ENV, "yes")

    assert_public_ip(ipaddress.ip_address("127.0.0.1"))


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "",
        "file:///tmp/archive.csv",
        "https:///missing-host",
        "https://localhost/archive.csv",
        "https://worker.local/archive.csv",
    ],
)
def test_safe_url_requires_a_remote_http_destination(monkeypatch, unsafe_url):
    def unexpected_dns_lookup(*_args, **_kwargs):
        raise AssertionError("invalid hosts must be rejected before DNS lookup")

    monkeypatch.delenv(url_security.FETCH_ALLOW_LOCAL_ENV, raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", unexpected_dns_lookup)

    with pytest.raises(UnsafeUrlError):
        assert_safe_url_sync(unsafe_url)


def test_safe_url_rejects_dns_answer_set_containing_private_address(monkeypatch):
    def mixed_dns_answers(_hostname, port, **lookup_options):
        assert lookup_options == {"type": socket.SOCK_STREAM}
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.8", port)),
        ]

    monkeypatch.delenv(url_security.FETCH_ALLOW_LOCAL_ENV, raising=False)
    monkeypatch.setattr(socket, "getaddrinfo", mixed_dns_answers)

    with pytest.raises(UnsafeUrlError, match="10.0.0.8"):
        assert_safe_url_sync("https://downloads.example/archive.csv")


def test_safe_url_rejects_host_without_dns_answers(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *_args, **_kwargs: [])

    with pytest.raises(UnsafeUrlError, match="cannot be resolved"):
        assert_safe_url_sync("https://missing.example/archive.csv")


async def test_async_safe_url_accepts_public_literal():
    await assert_safe_url("https://8.8.8.8/archive.csv")


def test_redirect_validation_blocks_private_hop_and_accepts_public_hop(monkeypatch):
    monkeypatch.delenv(url_security.FETCH_ALLOW_LOCAL_ENV, raising=False)
    handler = url_security._ValidatingRedirectHandler()
    original_request = urllib.request.Request("https://8.8.8.8/start")

    with pytest.raises(UnsafeUrlError):
        handler.redirect_request(
            original_request,
            None,
            302,
            "Found",
            {},
            "http://127.0.0.1/internal",
        )

    redirected_request = handler.redirect_request(
        original_request,
        None,
        302,
        "Found",
        {},
        "https://8.8.8.8/archive.csv",
    )
    assert redirected_request.full_url == "https://8.8.8.8/archive.csv"


def test_safe_fetch_rejects_private_final_url_before_reading(monkeypatch):
    response = _StreamingResponse(
        final_url="http://127.0.0.1/internal",
        chunks=[b"must not be read"],
    )
    _install_streaming_response(monkeypatch, response)

    with pytest.raises(UnsafeUrlError):
        urlopen_safe(
            "https://downloads.example/archive.csv",
            timeout=5,
            max_bytes=1024,
        )

    assert response.read_count == 0


def test_safe_fetch_enforces_limit_on_streamed_bytes(monkeypatch):
    response = _StreamingResponse(
        final_url="https://downloads.example/archive.csv",
        chunks=[b"abcd", b"efgh", b""],
    )
    _install_streaming_response(monkeypatch, response)

    with pytest.raises(UnsafeUrlError, match="7 byte fetch limit"):
        urlopen_safe(
            "https://downloads.example/archive.csv",
            timeout=5,
            max_bytes=7,
        )

    assert response.read_count == 2


def test_safe_fetch_returns_body_charset_and_uses_redirect_guard(monkeypatch):
    response = _StreamingResponse(
        final_url="https://downloads.example/archive.csv",
        chunks=[b"first", b"-second", b""],
        charset="utf-8",
    )
    opener, installed_handlers = _install_streaming_response(monkeypatch, response)
    request = urllib.request.Request("https://downloads.example/archive.csv")

    body, charset = urlopen_safe(request, timeout=9, max_bytes=12)

    assert body == b"first-second"
    assert charset == "utf-8"
    assert opener.opened_request is request
    assert opener.timeout == 9
    assert len(installed_handlers) == 1
    assert isinstance(
        installed_handlers[0],
        url_security._ValidatingRedirectHandler,
    )
