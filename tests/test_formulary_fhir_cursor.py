# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated cursor contracts for source-hidden formulary pages."""

import base64
import json

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import pytest

from api import formulary_fhir_cursor as cursor
from api import formulary_fhir_serving as serving


KEY_TEXT = base64.urlsafe_b64encode(b"k" * 32).decode("ascii").rstrip("=")
OTHER_KEY_TEXT = base64.urlsafe_b64encode(b"q" * 32).decode("ascii").rstrip("=")
ENVIRONMENT = {cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: KEY_TEXT}
SCOPE = {
    "alias_id": "ffa_" + "1" * 48,
    "formulary_id": "fhir_" + "a" * 26,
    "prior_authorization": False,
    "route": "drugs",
    "tier": "Preferred",
}


def _encode(**changes):
    values_by_name = {
        "kind": "drugs",
        "scope_by_field": SCOPE,
        "marker": "2026-08-08T06:00:00Z",
        "last_id": "ffm_" + "2" * 48,
        "environment": ENVIRONMENT,
    }
    values_by_name.update(changes)
    return cursor.encode_fhir_formulary_cursor(**values_by_name)


def _decode(raw_cursor, **changes):
    values_by_name = {
        "kind": "drugs",
        "scope_by_field": SCOPE,
        "environment": ENVIRONMENT,
    }
    values_by_name.update(changes)
    return cursor.decode_fhir_formulary_cursor(raw_cursor, **values_by_name)


def _sealed_payload(payload_by_field: dict[str, object]) -> str:
    plaintext = json.dumps(
        payload_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    nonce = b"n" * 12
    sealed = AESGCM(b"k" * 32).encrypt(nonce, plaintext, cursor._CURSOR_AAD)
    return base64.urlsafe_b64encode(nonce + sealed).decode("ascii").rstrip("=")


def test_cursor_is_encrypted_authenticated_and_round_trips(monkeypatch):
    monkeypatch.setattr(cursor.os, "urandom", lambda size: b"n" * size)

    encoded_cursor = _encode()
    decoded_cursor = _decode(encoded_cursor)

    assert decoded_cursor == cursor.FHIRFormularyPageCursor(
        marker="2026-08-08T06:00:00Z",
        last_id="ffm_" + "2" * 48,
    )
    assert "fhir_" not in encoded_cursor
    assert "ffa_" not in encoded_cursor
    assert "ffm_" not in encoded_cursor
    assert len(encoded_cursor) <= 512


def test_maximum_filter_scope_stays_within_cursor_bound():
    encoded_cursor = _encode(
        scope_by_field={
            **SCOPE,
            "ndc11": "1" * 11,
            "quantity_limit": True,
            "rxnorm_id": "2" * 64,
            "step_therapy": False,
            "tier": "t" * 256,
        }
    )

    assert len(encoded_cursor) <= 512


@pytest.mark.parametrize(
    "mutator",
    (
        lambda value: ("A" if value[0] != "A" else "B") + value[1:],
        lambda value: value[:-1] + ("A" if value[-1] != "A" else "B"),
        lambda value: value + "=",
        lambda _value: "not.a.cursor",
        lambda value: value[:-12],
    ),
)
def test_tampered_or_noncanonical_cursor_is_rejected(mutator):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(mutator(_encode()))


@pytest.mark.parametrize(
    "changes",
    (
        {"kind": "aliases"},
        {"scope_by_field": {**SCOPE, "tier": "Other"}},
        {
            "environment": {
                cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: OTHER_KEY_TEXT
            }
        },
    ),
)
def test_cursor_is_bound_to_kind_scope_and_key(changes):
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(_encode(), **changes)


@pytest.mark.parametrize(
    "environment",
    (
        {},
        {cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: "short"},
        {cursor.FHIR_FORMULARY_CURSOR_KEY_ENV: "_" * 43},
    ),
)
def test_missing_or_invalid_key_fails_closed(environment):
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        cursor.require_fhir_formulary_cursor_configuration(environment)


@pytest.mark.parametrize("kind", ("formularies", "aliases", "drugs"))
def test_supported_cursor_kinds_round_trip(kind):
    scope_by_field = {"route": kind}
    encoded_cursor = _encode(kind=kind, scope_by_field=scope_by_field)

    assert _decode(
        encoded_cursor,
        kind=kind,
        scope_by_field=scope_by_field,
    ).last_id.startswith("ffm_")


@pytest.mark.parametrize(
    "payload_changes",
    (
        {"version": True},
        {"version": 2},
        {"last": None},
        {"marker": None},
        {"scope": "0" * 64},
        {"extra": "field"},
    ),
)
def test_decrypted_shape_must_match_the_exact_contract(payload_changes):
    payload_by_field = {
        "kind": "drugs",
        "last": "ffm_" + "2" * 48,
        "marker": "2026-08-08T06:00:00Z",
        "scope": cursor._scope_digest(SCOPE),
        "version": 1,
    }
    payload_by_field.update(payload_changes)

    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        _decode(_sealed_payload(payload_by_field))


def test_none_cursor_is_empty_but_page_preflight_requires_key():
    assert _decode(None) is None
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        cursor.require_fhir_formulary_cursor_configuration({})
