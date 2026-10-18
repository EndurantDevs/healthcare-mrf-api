# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The singleton body allowance does not expand other signed read modes."""

from types import SimpleNamespace

import pytest

from api import custom_import_read_http as transport
from api.custom_import_provider_batch import CUSTOM_IMPORT_PROVIDER_BATCH_PATH
from tests import test_custom_import_read_http as fixtures


def _verify(
    body, *, managed_detail=True, parsed_mode=True, path=CUSTOM_IMPORT_PROVIDER_BATCH_PATH, tampered=False, **changes
):
    headers = fixtures._resigned_headers(
        body=body,
        path=path,
        contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=transport.managed_provider_body_sha256,
        signature_message=transport.custom_import_provider_signature_message,
        **changes,
    )
    return transport._verify_provider_transport(
        headers=headers,
        body=b"y" * len(body) if tampered else body,
        request=SimpleNamespace(
            target=transport._parse_target(fixtures._TARGET), managed_detail=object() if parsed_mode else None
        ),
        trusted_now=fixtures._NOW,
        keyring=transport._load_keyring(fixtures._keyring_document()),
        path=path,
        managed_detail=managed_detail,
    )


@pytest.mark.parametrize("size", [1, 16384])
def test_managed_hash_keeps_exact_provider_v2_framing(size):
    body = b"x" * size
    assert transport.managed_provider_body_sha256(body) == transport.custom_import_provider_body_sha256(body)


@pytest.mark.parametrize("body", [b"", b"x" * (1024 * 1024 + 1), bytearray(b"x"), "x"])
def test_managed_hash_rejects_invalid_body_bounds_and_type(body):
    with pytest.raises(transport.CustomImportReadTransportError):
        transport.managed_provider_body_sha256(body)


@pytest.mark.parametrize("size", [16385, 1024 * 1024])
def test_large_body_requires_the_explicit_parsed_singleton_mode(size):
    body = b"x" * size
    assert _verify(body).scope == "a" * 64
    with pytest.raises(transport.CustomImportReadTransportError):
        _verify(body, managed_detail=False)
    with pytest.raises(transport.CustomImportReadTransportError):
        _verify(body, parsed_mode=False)
    with pytest.raises(transport.CustomImportReadTransportError):
        transport.custom_import_read_body_sha256(body)


@pytest.mark.parametrize("path", [transport.CUSTOM_IMPORT_READ_PATH, "/api/v1/extensions/custom-import/providers/list"])
def test_singleton_allowance_is_not_accepted_on_other_paths(path):
    with pytest.raises(transport.CustomImportReadTransportError):
        _verify(b"x" * 16385, path=path)


@pytest.mark.parametrize("mode", [None, 1, "true"])
def test_managed_mode_is_an_explicit_boolean(mode):
    with pytest.raises(transport.CustomImportReadTransportError):
        _verify(b"x", managed_detail=mode)


@pytest.mark.parametrize(
    "changes",
    [
        {"tampered": True},
        {"expires_at": fixtures._NOW},
        {"method": "GET"},
        {"authorization_scope_sha256": "invalid"},
        {"target": {**fixtures._TARGET, "generation_id": 102}},
    ],
)
def test_large_signed_mode_keeps_permit_expiry_target_body_and_scope_checks(changes):
    with pytest.raises(transport.CustomImportReadTransportError):
        _verify(b"x" * 16385, **changes)
