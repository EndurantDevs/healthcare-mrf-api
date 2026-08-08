# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundary cases for opaque FHIR formulary cursors."""

import base64
import json

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import pytest

from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_serving as serving


KEY_BYTES = b"z" * 32
KEY_TEXT = base64.urlsafe_b64encode(KEY_BYTES).decode("ascii").rstrip("=")
ENVIRONMENT = {cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: KEY_TEXT}
SCOPE = {"route": "drugs"}


def _sealed_bytes(plaintext: bytes) -> str:
    nonce = b"n" * 12
    sealed = AESGCM(KEY_BYTES).encrypt(nonce, plaintext, cursor._CURSOR_AAD)
    return base64.urlsafe_b64encode(nonce + sealed).decode("ascii").rstrip("=")


def _valid_payload(**changes) -> dict[str, object]:
    payload_by_field = {
        "kind": "drugs",
        "last": "ffm_" + "2" * 48,
        "marker": "2026-08-08T06:00:00Z",
        "scope": cursor._scope_digest(SCOPE),
        "version": 1,
    }
    payload_by_field.update(changes)
    return payload_by_field


def _decode(raw_cursor: object):
    return cursor.decode_fhir_formulary_cursor(
        raw_cursor,
        kind="drugs",
        scope_by_field=SCOPE,
        environment=ENVIRONMENT,
    )


@pytest.mark.parametrize(
    "scope_by_field",
    (
        {},
        [],
        {"": "value"},
        {1: "value"},
        {"field": []},
    ),
)
def test_scope_rejects_empty_non_mapping_and_non_scalar_values(scope_by_field):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        cursor._scope_digest(scope_by_field)


@pytest.mark.parametrize(
    "changes",
    (
        {"kind": "unknown"},
        {"marker": None},
        {"marker": ""},
        {"marker": "m" * 129},
        {"last_id": None},
        {"last_id": ""},
        {"last_id": "d" * 65},
    ),
)
def test_cursor_encoder_rejects_invalid_plaintext_contract(changes):
    arguments_by_name = {
        "kind": "drugs",
        "scope_by_field": SCOPE,
        "marker": "marker",
        "last_id": "drug",
        "environment": ENVIRONMENT,
    }
    arguments_by_name.update(changes)

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        cursor.encode_fhir_formulary_cursor(**arguments_by_name)


def test_duplicate_decrypted_fields_are_rejected():
    scope_digest = cursor._scope_digest(SCOPE)
    duplicate_json = (
        '{"kind":"drugs","kind":"aliases","last":"ffm_'
        + "2" * 48
        + '","marker":"marker","scope":"'
        + scope_digest
        + '","version":1}'
    ).encode("ascii")

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(_sealed_bytes(duplicate_json))


@pytest.mark.parametrize("plaintext", (b"{", b"\xff", b"[]"))
def test_decrypted_cursor_requires_ascii_json_object(plaintext):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(_sealed_bytes(plaintext))


@pytest.mark.parametrize(
    "changes",
    (
        {"kind": "aliases"},
        {"last": ""},
        {"last": "d" * 65},
        {"marker": ""},
        {"marker": "m" * 129},
    ),
)
def test_decrypted_cursor_rejects_wrong_kind_and_bounds(changes):
    plaintext = json.dumps(
        _valid_payload(**changes),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(_sealed_bytes(plaintext))


def test_noncanonical_and_truncated_base64_tokens_are_rejected():
    token_bytes = b"t" * 29
    canonical = base64.urlsafe_b64encode(token_bytes).decode("ascii").rstrip("=")
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    final_index = alphabet.index(canonical[-1])
    noncanonical = canonical[:-1] + alphabet[final_index + 1]
    assert base64.urlsafe_b64decode(noncanonical + "=") == token_bytes

    truncated = base64.urlsafe_b64encode(b"t" * 28).decode("ascii").rstrip("=")
    for raw_cursor in (noncanonical, truncated):
        with pytest.raises(serving.FHIRFormularyInvalidRequestError):
            _decode(raw_cursor)


def test_configuration_uses_process_environment(monkeypatch):
    monkeypatch.setenv(cursor.FHIR_FORMULARY_CURSOR_KEY_ENV, KEY_TEXT)

    assert cursor.require_fhir_formulary_cursor_configuration() == KEY_BYTES


def test_configuration_maps_decoder_failure_to_unavailable(monkeypatch):
    def fail_decode(*_args, **_kwargs):
        raise ValueError("synthetic decoder failure")

    monkeypatch.setattr(cursor.base64, "b64decode", fail_decode)

    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        cursor.require_fhir_formulary_cursor_configuration(ENVIRONMENT)
