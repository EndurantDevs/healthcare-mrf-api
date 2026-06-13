import ipaddress

import pytest

from process import mrf_source_discovery
from process.url_security import UnsafeUrlError, assert_public_ip


NON_GLOBAL_ADDRESSES = [
    "100.64.1.1",  # CGNAT / Tailscale shared space (not caught by is_private)
    "198.18.0.1",  # benchmarking range
    "192.0.0.8",  # IETF protocol assignments
    "169.254.169.254",  # link-local / cloud metadata
    "127.0.0.1",
    "10.0.0.1",
    "0.0.0.0",
]


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
