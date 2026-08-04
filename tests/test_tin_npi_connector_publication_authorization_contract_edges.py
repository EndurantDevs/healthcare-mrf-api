# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Defensive branch proofs for publication authorization and trust input."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from types import MappingProxyType

import pytest

from process import tin_npi_connector_publication_authorization as authorization
from process import (
    tin_npi_connector_publication_authorization_contract as contract,
)
from process.tin_npi_connector_publication_authorization_contract import (
    ConnectorPublicationAuthorizationError,
    ConnectorPublicationAuthorizationTrust,
    ConnectorPublicationAuthorizationTrustKey,
)
from tests.tin_npi_connector_publication_authorization_support import (
    ACTIVE_KEY_ID,
    AUTHORITY_ID,
    AUTHORITY_RELEASE_DIGEST,
    BASE_TIME,
    PUBLICATION_SCOPE_ID,
    PUBLIC_KEY_BYTES,
    ROTATED_PUBLIC_KEY_BYTES,
    SENSITIVE_SYNTHETIC_VALUE,
    authorization_trust,
    sealed_generation,
    signed_envelope,
    synthetic_bundle,
    verify,
)


def test_error_factory_replaces_unknown_code_without_echoing_it():
    error = contract.authorization_error(SENSITIVE_SYNTHETIC_VALUE)

    assert error.code == "verification_failed"
    assert str(error) == "connector publication authorization verification_failed"
    assert SENSITIVE_SYNTHETIC_VALUE not in repr(error)


def test_envelope_boundaries_require_exact_builtin_dicts(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    intent_fields_by_name = dict(envelope["intent"])
    intent_fields_by_name["expected_predecessor"] = _DictSubclass(
        intent_fields_by_name["expected_predecessor"]
    )
    candidates_and_codes = (
        (_DictSubclass(envelope), "invalid_envelope_fields"),
        (MappingProxyType(envelope), "invalid_envelope_fields"),
        (
            {
                "intent": _DictSubclass(envelope["intent"]),
                "signature": envelope["signature"],
            },
            "invalid_intent_fields",
        ),
        (
            {"intent": intent_fields_by_name, "signature": envelope["signature"]},
            "invalid_predecessor",
        ),
    )

    for candidate, expected_code in candidates_and_codes:
        with pytest.raises(
            ConnectorPublicationAuthorizationError,
            match=rf"authorization {expected_code}$",
        ):
            verify(candidate, bundle=bundle, sealed=sealed)


class _DictSubclass(dict):
    """Synthetic mapping subtype rejected at the signed-input boundary."""


def _trust_key(**overrides):
    fields_by_name = {
        "key_id": ACTIVE_KEY_ID,
        "public_key": PUBLIC_KEY_BYTES,
        "authority_release_digest": AUTHORITY_RELEASE_DIGEST,
        "public_source_ids": ("source-a", "source-b"),
        "status": "active",
        "retired_at": None,
        "verify_until": None,
    }
    fields_by_name.update(overrides)
    return ConnectorPublicationAuthorizationTrustKey(**fields_by_name)


@pytest.mark.parametrize(
    "overrides",
    (
        {"key_id": ""},
        {"public_key": b"x" * 31},
        {"authority_release_digest": "A" * 64},
        {"public_source_ids": ["source-a"]},
        {"public_source_ids": ()},
        {"public_source_ids": ("bad source",)},
        {"public_source_ids": ("source-b", "source-a")},
        {"retired_at": BASE_TIME},
        {"status": "unknown"},
        {
            "status": "retired",
            "retired_at": BASE_TIME.replace(tzinfo=None),
            "verify_until": BASE_TIME + dt.timedelta(minutes=1),
        },
        {
            "status": "retired",
            "retired_at": BASE_TIME,
            "verify_until": BASE_TIME + dt.timedelta(minutes=16),
        },
    ),
)
def test_invalid_trust_key_contracts_fail_closed(overrides):
    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_trust$",
    ):
        _trust_key(**overrides)


def test_trust_key_repr_hides_key_and_source_identities():
    trust_key = _trust_key()

    rendered = repr(trust_key)

    assert rendered == (
        "<connector-publication-authorization-key status=active sources=2>"
    )
    assert ACTIVE_KEY_ID not in rendered
    assert PUBLIC_KEY_BYTES.hex() not in rendered
    assert all(source_id not in rendered for source_id in trust_key.public_source_ids)


@pytest.mark.parametrize(
    "trust_fields",
    (
        {"publication_scope_id": ""},
        {"authority_id": ""},
        {"active_key_id": ""},
        {"keys": []},
        {"keys": ()},
        {"keys": (object(),)},
    ),
)
def test_invalid_trust_keyring_shapes_fail_closed(trust_fields):
    fields_by_name = {
        "publication_scope_id": PUBLICATION_SCOPE_ID,
        "authority_id": AUTHORITY_ID,
        "active_key_id": ACTIVE_KEY_ID,
        "keys": (_trust_key(),),
    }
    fields_by_name.update(trust_fields)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_trust$",
    ):
        ConnectorPublicationAuthorizationTrust(**fields_by_name)


def test_trust_rejects_unsorted_keys_and_active_key_mismatch():
    first_key = _trust_key(key_id="key-a")
    second_key = _trust_key(
        key_id="key-b",
        public_key=ROTATED_PUBLIC_KEY_BYTES,
    )

    for active_key_id, keys in (
        ("key-a", (second_key, first_key)),
        ("missing-key", (first_key,)),
        ("key-a", (first_key, second_key)),
    ):
        with pytest.raises(
            ConnectorPublicationAuthorizationError,
            match="authorization invalid_trust$",
        ):
            ConnectorPublicationAuthorizationTrust(
                publication_scope_id=PUBLICATION_SCOPE_ID,
                authority_id=AUTHORITY_ID,
                active_key_id=active_key_id,
                keys=keys,
            )


def test_trust_repr_hides_scope_authority_and_key_ids(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    trust = authorization_trust(bundle)

    rendered = repr(trust)

    assert rendered == "<connector-publication-authorization-trust keys=1>"
    assert PUBLICATION_SCOPE_ID not in rendered
    assert AUTHORITY_ID not in rendered
    assert ACTIVE_KEY_ID not in rendered


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_code"),
    (
        ("target_generation_key", True, "invalid_generation"),
        ("target_generation_key", -1, "invalid_generation"),
        ("expected_pointer_version", True, "invalid_pointer"),
        ("expected_predecessor", "absent", "invalid_predecessor"),
        (
            "expected_predecessor",
            {"state": "present"},
            "invalid_predecessor",
        ),
        (
            "expected_predecessor",
            {"state": "present", "generation_key": True},
            "invalid_predecessor",
        ),
        (
            "expected_predecessor",
            {"state": "present", "generation_key": 41},
            "invalid_predecessor",
        ),
        ("issued_at", None, "invalid_time"),
        ("issued_at", "2026-02-30T12:00:00Z", "invalid_time"),
        ("source_ids", None, "invalid_source_ids"),
        ("source_ids", [], "invalid_source_ids"),
        ("source_ids", ["bad source"], "invalid_source_ids"),
    ),
)
def test_invalid_intent_scalar_and_shape_contracts_fail_closed(
    tmp_path,
    field_name,
    invalid_value,
    expected_code,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    envelope["intent"][field_name] = invalid_value
    if field_name == "expected_predecessor" and isinstance(invalid_value, dict):
        envelope["intent"]["expected_pointer_version"] = 1

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match=rf"authorization {expected_code}$",
    ):
        contract.canonical_publication_authorization_json(envelope["intent"])


def test_nonmapping_intent_and_envelope_fail_closed():
    for candidate in (None, [], "intent"):
        with pytest.raises(ConnectorPublicationAuthorizationError):
            contract.canonical_publication_authorization_json(candidate)
        with pytest.raises(ConnectorPublicationAuthorizationError):
            contract.validated_publication_authorization_envelope(candidate)


def test_invalid_trust_type_and_validation_time_fail_closed(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_trust$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, trust=object())
    for invalid_now in ("now", BASE_TIME.replace(tzinfo=None)):
        with pytest.raises(
            ConnectorPublicationAuthorizationError,
            match="authorization invalid_validation_time$",
        ):
            verify(envelope, bundle=bundle, sealed=sealed, now=invalid_now)


def test_invalid_binding_types_fail_closed(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    trust = authorization_trust(bundle)

    for invalid_bundle, invalid_sealed in ((None, sealed), (bundle, None)):
        with pytest.raises(
            ConnectorPublicationAuthorizationError,
            match="authorization invalid_binding$",
        ):
            authorization.verify_connector_publication_authorization(
                envelope,
                trust=trust,
                bundle=invalid_bundle,
                sealed_generation=invalid_sealed,
                now=BASE_TIME,
            )


def test_canonical_base64_signature_with_changed_bytes_reaches_verifier(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    first_character = "A" if envelope["signature"][0] != "A" else "B"
    envelope["signature"] = first_character + envelope["signature"][1:]

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_signature$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed)


def test_signature_decoder_failure_is_detached_and_sanitized(tmp_path, monkeypatch):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)

    def reject_decode(*_arguments, **_options):
        raise ValueError("synthetic sensitive decoder detail")

    monkeypatch.setattr(authorization.base64, "b64decode", reject_decode)
    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_signature$",
    ) as captured_error:
        verify(envelope, bundle=bundle, sealed=sealed)

    assert captured_error.value.__context__ is None


def test_public_key_decoder_failure_is_detached_and_sanitized(tmp_path, monkeypatch):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)

    class RejectingPublicKey:
        @staticmethod
        def from_public_bytes(_candidate):
            raise ValueError("synthetic sensitive key detail")

    monkeypatch.setattr(authorization, "Ed25519PublicKey", RejectingPublicKey)
    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_trust$",
    ) as captured_error:
        verify(envelope, bundle=bundle, sealed=sealed)

    assert captured_error.value.__context__ is None
