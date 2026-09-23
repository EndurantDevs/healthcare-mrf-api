# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic geo cursor binding, validation, and domain-separation checks."""

import hashlib
import hmac
import json
from dataclasses import replace

import pytest

from api import custom_import_provider_geo_cursor as geo
from process.custom_import.read_contracts import (
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    PinnedReadTarget,
    canonical_read_document,
)
from process.custom_import.read_cursor import _base64url_decode, _base64url_encode

SECRET = b"synthetic-geo-cursor-key-32-bytes!"
STATE = geo.GeoCursorState(
    target=PinnedReadTarget(1, 2, 3, 4, "default"),
    query_fingerprint="a" * 64,
    authorization_scope_sha256="b" * 64,
    anchor_npi="1000000001",
    anchor_address_key="00000000-0000-4000-8000-000000000001",
    issued_at=100,
    expires_at=200,
)


def _open(token, **overrides):
    return geo.open_geo_cursor(
        token,
        **{
            "secret": SECRET,
            "pinned_target": STATE.target,
            "query_fingerprint": STATE.query_fingerprint,
            "authorization_scope_sha256": STATE.authorization_scope_sha256,
            "trusted_now": 150,
            **overrides,
        },
    )


def test_anchor_round_trip_has_no_offset_or_metric_values():
    token = geo.issue_geo_cursor(STATE, secret=SECRET)
    assert "." not in token and "=" not in token
    assert _open(token) == STATE
    assert set(json.loads(_base64url_decode(token)[:-32])) == {
        "version",
        "target",
        "query_fingerprint",
        "authorization_scope_sha256",
        "anchor_npi",
        "anchor_address_key",
        "issued_at",
        "expires_at",
    }


@pytest.mark.parametrize(
    "override",
    [
        {"pinned_target": replace(STATE.target, generation_id=5)},
        {"pinned_target": replace(STATE.target, schema_revision_id=5)},
        {"pinned_target": replace(STATE.target, profile_id="other")},
        {"query_fingerprint": "c" * 64},
        {"authorization_scope_sha256": "c" * 64},
        {"trusted_now": 200},
        {"trusted_now": 99},
        {"trusted_now": True},
        {"secret": b"another-synthetic-key-32-bytes!!!!"},
    ],
)
def test_rejects_stale_or_differently_bound_cursor(override):
    with pytest.raises(CustomImportReadCursorError):
        _open(geo.issue_geo_cursor(STATE, secret=SECRET), **override)


@pytest.mark.parametrize(
    "override",
    [
        {"anchor_npi": "1"},
        {"anchor_npi": "١" * 10},
        {"anchor_address_key": "not-an-address"},
        {"anchor_address_key": None},
        {"issued_at": True},
        {"expires_at": 1001},
        {"query_fingerprint": "invalid"},
        {"target": None},
    ],
)
def test_invalid_state_cannot_be_issued(override):
    with pytest.raises(CustomImportReadCursorError):
        geo.issue_geo_cursor(replace(STATE, **override), secret=SECRET)


@pytest.mark.parametrize("token", [None, "", "a" * 2049, "a.b", "a=", "YQ", "Yg"])
def test_invalid_token_is_rejected(token):
    with pytest.raises(CustomImportReadCursorError):
        _open(token)


def test_mac_is_verified_before_json_parsing(monkeypatch):
    token = _base64url_encode(b"not-json" + bytes(32))
    monkeypatch.setattr(geo, "_state_from_payload", lambda _: pytest.fail("unauthenticated parse"))
    with pytest.raises(CustomImportReadCursorError):
        _open(token)


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"unexpected": "field"},
        {"expires_at": True},
        {"target": {"dataset_id": True}},
        {"anchor_npi": 1000000001},
    ],
)
def test_even_signed_invalid_payloads_are_rejected(change):
    document = json.loads(_base64url_decode(geo.issue_geo_cursor(STATE, secret=SECRET))[:-32])
    payload = canonical_read_document({**document, **change})
    token = _base64url_encode(payload + hmac.digest(SECRET, geo._DOMAIN + payload, hashlib.sha256))
    with pytest.raises(CustomImportReadCursorError):
        _open(token)


def test_foreign_domain_and_noncanonical_signed_json_are_rejected():
    token = geo.issue_geo_cursor(STATE, secret=SECRET)
    payload = _base64url_decode(token)[:-32]
    for altered_payload, domain in [(payload, b"other-domain"), (payload + b" ", geo._DOMAIN)]:
        signed = hmac.digest(SECRET, domain + altered_payload, hashlib.sha256)
        with pytest.raises(CustomImportReadCursorError):
            _open(_base64url_encode(altered_payload + signed))
    with pytest.raises(CustomImportReadRequestError):
        geo.issue_geo_cursor(STATE, secret=b"short")
