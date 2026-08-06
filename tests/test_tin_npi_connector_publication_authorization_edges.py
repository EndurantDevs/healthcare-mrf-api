# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial, temporal, rotation, and redaction authorization proofs."""

from __future__ import annotations

import copy
import datetime as dt

import pytest

from process import tin_npi_connector_publication_authorization as authorization
from process.tin_npi_connector_publication_authorization_contract import (
    ConnectorPublicationAuthorizationError,
)
from tests.tin_npi_connector_publication_authorization_support import (
    BASE_TIME,
    SENSITIVE_SYNTHETIC_VALUE,
    authorization_trust,
    retired_key_trust,
    sealed_generation,
    sign_intent,
    signed_envelope,
    synthetic_bundle,
    unsigned_intent,
    verify,
)

SIGNED_FIELD_MUTATIONS = (
    ("action", "rollback"),
    ("authority_id", "mutated-authority"),
    ("authority_release_digest", "44" * 32),
    ("contract_id", "healthporta.synthetic.unsupported.v1"),
    ("expected_pointer_version", 1),
    ("expected_predecessor", {"state": "present", "generation_key": 7}),
    ("expires_at", "2026-08-04T12:09:59Z"),
    ("generation_id", "55" * 32),
    ("intent_id", "66" * 32),
    ("issued_at", "2026-08-04T12:00:01Z"),
    ("key_id", "mutated-key"),
    ("nonce", "77" * 32),
    ("publication_scope_id", "mutated-scope"),
    ("rights_profile", "restricted_sources_v1"),
    ("signature_algorithm", "synthetic"),
    ("source_ids", ["source-b", "source-a"]),
    ("source_vector_id", "88" * 32),
    ("target_generation_key", 42),
)


@pytest.mark.parametrize(("field_name", "mutated_value"), SIGNED_FIELD_MUTATIONS)
def test_mutating_any_signed_field_invalidates_authorization(
    tmp_path,
    field_name,
    mutated_value,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    mutated_envelope = copy.deepcopy(envelope)
    mutated_envelope["intent"][field_name] = mutated_value

    with pytest.raises(ConnectorPublicationAuthorizationError):
        verify(mutated_envelope, bundle=bundle, sealed=sealed)


@pytest.mark.parametrize(
    "mutator",
    (
        lambda envelope: envelope.update(extra="field"),
        lambda envelope: envelope.pop("signature"),
        lambda envelope: envelope["intent"].update(extra="field"),
        lambda envelope: envelope["intent"].pop("nonce"),
    ),
)
def test_closed_envelope_and_intent_reject_unknown_or_missing_fields(
    tmp_path,
    mutator,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    mutator(envelope)

    with pytest.raises(ConnectorPublicationAuthorizationError):
        verify(envelope, bundle=bundle, sealed=sealed)


@pytest.mark.parametrize(
    "signature",
    (None, "", "!" * 86, "a" * 85, "a" * 87),
)
def test_malformed_signature_fails_closed(tmp_path, signature):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    envelope["signature"] = signature

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_signature$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed)


def test_validly_signed_different_source_set_fails_exact_bundle_binding(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    intent = unsigned_intent(
        bundle,
        sealed,
        source_ids=["source-a", "source-c"],
    )
    envelope = sign_intent(intent)
    trust = authorization_trust(
        bundle,
        public_source_ids=("source-a", "source-b", "source-c"),
    )

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization source_binding_mismatch$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, trust=trust)


@pytest.mark.parametrize(
    "source_ids",
    (["source-b", "source-a"], ["source-a", "source-a"]),
)
def test_source_order_and_duplicates_fail_before_signature_use(tmp_path, source_ids):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    envelope["intent"]["source_ids"] = source_ids

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization invalid_source_ids$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed)


@pytest.mark.parametrize(
    ("intent_options", "now", "expected_code"),
    (
        (
            {
                "issued_at": BASE_TIME + dt.timedelta(seconds=6),
                "expires_at": BASE_TIME + dt.timedelta(minutes=5),
            },
            BASE_TIME,
            "issued_in_future",
        ),
        (
            {"expires_at": BASE_TIME},
            BASE_TIME,
            "invalid_validity_window",
        ),
        (
            {"expires_at": BASE_TIME + dt.timedelta(minutes=15, seconds=1)},
            BASE_TIME,
            "invalid_validity_window",
        ),
        (
            {"expires_at": BASE_TIME + dt.timedelta(minutes=1)},
            BASE_TIME + dt.timedelta(minutes=1),
            "expired",
        ),
    ),
)
def test_authorization_temporal_boundaries_fail_closed(
    tmp_path,
    intent_options,
    now,
    expected_code,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed, **intent_options)

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match=rf"authorization {expected_code}$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, now=now)


def test_five_second_future_skew_is_accepted(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(
        bundle,
        sealed,
        issued_at=BASE_TIME + dt.timedelta(seconds=5),
        expires_at=BASE_TIME + dt.timedelta(minutes=5),
    )

    verified = verify(envelope, bundle=bundle, sealed=sealed, now=BASE_TIME)

    assert verified.issued_at == BASE_TIME + dt.timedelta(seconds=5)


def test_retired_key_accepts_pre_retirement_unexpired_intent(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    retired_at = BASE_TIME
    verify_until = BASE_TIME + dt.timedelta(minutes=15)
    trust = retired_key_trust(
        bundle,
        retired_at=retired_at,
        verify_until=verify_until,
    )
    envelope = signed_envelope(
        bundle,
        sealed,
        issued_at=BASE_TIME - dt.timedelta(seconds=1),
    )

    verified = verify(
        envelope,
        bundle=bundle,
        sealed=sealed,
        trust=trust,
        now=BASE_TIME + dt.timedelta(minutes=1),
    )

    assert verified.key_id == "key-2026-a"


def test_retired_key_rejects_intent_issued_at_retirement_boundary(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    trust = retired_key_trust(
        bundle,
        retired_at=BASE_TIME,
        verify_until=BASE_TIME + dt.timedelta(minutes=15),
    )
    envelope = signed_envelope(
        bundle,
        sealed,
        issued_at=BASE_TIME,
        expires_at=BASE_TIME + dt.timedelta(minutes=5),
    )

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization retired_key_rejected$",
    ):
        verify(
            envelope,
            bundle=bundle,
            sealed=sealed,
            trust=trust,
            now=BASE_TIME + dt.timedelta(minutes=1),
        )


@pytest.mark.parametrize(
    ("issued_at", "expires_at", "now"),
    (
        (
            BASE_TIME + dt.timedelta(seconds=1),
            BASE_TIME + dt.timedelta(minutes=5),
            BASE_TIME + dt.timedelta(minutes=1),
        ),
        (
            BASE_TIME,
            BASE_TIME + dt.timedelta(minutes=11),
            BASE_TIME + dt.timedelta(minutes=1),
        ),
        (
            BASE_TIME,
            BASE_TIME + dt.timedelta(minutes=6),
            BASE_TIME + dt.timedelta(minutes=5),
        ),
    ),
)
def test_retired_key_rejects_outside_rotation_window(
    tmp_path,
    issued_at,
    expires_at,
    now,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    trust = retired_key_trust(
        bundle,
        retired_at=BASE_TIME,
        verify_until=BASE_TIME + dt.timedelta(minutes=5),
    )
    envelope = signed_envelope(
        bundle,
        sealed,
        issued_at=issued_at,
        expires_at=expires_at,
    )

    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization retired_key_rejected$",
    ):
        verify(envelope, bundle=bundle, sealed=sealed, trust=trust, now=now)


def test_invalid_signature_and_errors_have_detached_redacted_graph(tmp_path):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    envelope["signature"] = "a" * 86

    with pytest.raises(ConnectorPublicationAuthorizationError) as captured_error:
        verify(envelope, bundle=bundle, sealed=sealed)

    _assert_exception_graph_redacted(captured_error.value)


def test_verifier_uses_detached_signed_snapshot(tmp_path, monkeypatch):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)
    original_verify_signature = authorization._verify_signature

    def verify_then_mutate_original(intent, signature_bytes, trust_key):
        original_verify_signature(intent, signature_bytes, trust_key)
        envelope["intent"]["target_generation_key"] = 999
        envelope["intent"]["source_ids"].clear()
        envelope["intent"]["expected_predecessor"] = {
            "state": "present",
            "generation_key": 998,
        }

    monkeypatch.setattr(
        authorization,
        "_verify_signature",
        verify_then_mutate_original,
    )

    receipt = verify(envelope, bundle=bundle, sealed=sealed)

    assert receipt.target_generation_key == sealed.generation_key
    assert receipt.source_ids == bundle.generation.source_ordinal_map
    assert receipt.expected_predecessor_generation_key is None


def test_unexpected_verifier_failure_is_sanitized_without_context(
    tmp_path,
    monkeypatch,
):
    bundle = synthetic_bundle(tmp_path)
    sealed = sealed_generation(bundle)
    envelope = signed_envelope(bundle, sealed)

    def reject_envelope(_candidate):
        raise RuntimeError(SENSITIVE_SYNTHETIC_VALUE)

    monkeypatch.setattr(
        authorization,
        "validated_publication_authorization_envelope",
        reject_envelope,
    )
    with pytest.raises(
        ConnectorPublicationAuthorizationError,
        match="authorization verification_failed$",
    ) as captured_error:
        verify(envelope, bundle=bundle, sealed=sealed)

    _assert_exception_graph_redacted(captured_error.value)


def _assert_exception_graph_redacted(root_error: BaseException) -> None:
    assert root_error.__cause__ is None
    assert root_error.__context__ is None
    pending_errors: list[BaseException | None] = [root_error]
    seen_error_ids: set[int] = set()
    while pending_errors:
        current_error = pending_errors.pop()
        if current_error is None or id(current_error) in seen_error_ids:
            continue
        seen_error_ids.add(id(current_error))
        rendered_forms = (
            repr(current_error),
            str(current_error),
            repr(vars(current_error)),
        )
        assert all(
            SENSITIVE_SYNTHETIC_VALUE not in rendered for rendered in rendered_forms
        )
        pending_errors.extend((current_error.__cause__, current_error.__context__))
