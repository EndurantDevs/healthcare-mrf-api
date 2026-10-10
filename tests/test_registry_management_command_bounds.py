# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Company assertion transport remains bounded before any command or database work."""

from types import SimpleNamespace

import pytest

from api.endpoint.registry_management import _body


def _request(kind, size):
    body = b'{"value":"' + b"x" * (size - 12) + b'"}'
    assert len(body) == size
    return SimpleNamespace(match_info={"kind": kind}, args={}, body=body)


@pytest.mark.parametrize("size", [65537, 131072])
def test_company_accepts_bounded_assertion_command(size):
    assert _body(_request("company", size))["value"]


def test_company_refuses_one_byte_over_transport_bound():
    with pytest.raises(ValueError, match="request_too_large"):
        _body(_request("company", 131073))


@pytest.mark.parametrize("kind", ["group", "network", "provider", "location", "site_binding", "network_binding"])
def test_other_records_retain_existing_transport_bound(kind):
    with pytest.raises(ValueError, match="request_too_large"):
        _body(_request(kind, 65537))


@pytest.mark.parametrize("kind", ["membership", "company_links"])
def test_existing_bulk_commands_keep_their_larger_bound(kind):
    assert _body(_request(kind, 131073))["value"]
