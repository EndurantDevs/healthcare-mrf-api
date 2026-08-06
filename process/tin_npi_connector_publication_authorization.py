# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Verify source rights and exact cutover intent without database access."""

from __future__ import annotations

import base64
import datetime as dt
import hmac
import re
from collections.abc import Mapping
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.tin_npi_connector_generation_store import (
    SealedConnectorGeneration,
)
from process.tin_npi_connector_publication import ConnectorPublicationBundle
from process.tin_npi_connector_publication_authorization_contract import (
    PUBLICATION_AUTHORIZATION_FUTURE_SKEW_SECONDS,
    PUBLICATION_AUTHORIZATION_MAX_VALIDITY_SECONDS,
    ConnectorPublicationAuthorizationReceipt,
    ConnectorPublicationAuthorizationError,
    ConnectorPublicationAuthorizationTrust,
    ConnectorPublicationAuthorizationTrustKey,
    authorization_error,
    publication_authorization_signature_message,
    publication_authorization_timestamp,
    validated_publication_authorization_envelope,
)

_BASE64URL_SIGNATURE = re.compile(r"[A-Za-z0-9_-]{86}\Z")


def _decode_signature(candidate: Any) -> bytes:
    if type(candidate) is not str or _BASE64URL_SIGNATURE.fullmatch(candidate) is None:
        raise authorization_error("invalid_signature")
    signature_bytes = b""
    is_decoded = True
    try:
        signature_bytes = base64.b64decode(
            candidate + "==",
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, TypeError):
        is_decoded = False
    if not is_decoded:
        raise authorization_error("invalid_signature")
    canonical_signature = (
        base64.urlsafe_b64encode(signature_bytes).rstrip(b"=").decode("ascii")
    )
    if len(signature_bytes) != 64 or canonical_signature != candidate:
        raise authorization_error("invalid_signature")
    return signature_bytes


def _selected_trust_key(
    intent: Mapping[str, Any],
    trust: ConnectorPublicationAuthorizationTrust,
) -> ConnectorPublicationAuthorizationTrustKey:
    if type(trust) is not ConnectorPublicationAuthorizationTrust:
        raise authorization_error("invalid_trust")
    if not hmac.compare_digest(
        intent["publication_scope_id"],
        trust.publication_scope_id,
    ):
        raise authorization_error("scope_mismatch")
    if not hmac.compare_digest(intent["authority_id"], trust.authority_id):
        raise authorization_error("authority_mismatch")
    selected_key = next(
        (key for key in trust.keys if key.key_id == intent["key_id"]),
        None,
    )
    if selected_key is None:
        raise authorization_error("unknown_key")
    if not hmac.compare_digest(
        intent["authority_release_digest"],
        selected_key.authority_release_digest,
    ):
        raise authorization_error("authority_release_mismatch")
    return selected_key


def _public_key(candidate: bytes) -> Ed25519PublicKey:
    public_key: Ed25519PublicKey | None = None
    is_valid = True
    try:
        public_key = Ed25519PublicKey.from_public_bytes(candidate)
    except ValueError:
        is_valid = False
    if not is_valid or public_key is None:
        raise authorization_error("invalid_trust")
    return public_key


def _verify_signature(
    intent: Mapping[str, Any],
    signature_bytes: bytes,
    trust_key: ConnectorPublicationAuthorizationTrustKey,
) -> None:
    is_signature_valid = True
    try:
        _public_key(trust_key.public_key).verify(
            signature_bytes,
            publication_authorization_signature_message(intent),
        )
    except InvalidSignature:
        is_signature_valid = False
    if not is_signature_valid:
        raise authorization_error("invalid_signature")


def _validated_now(candidate: Any) -> dt.datetime:
    if (
        type(candidate) is not dt.datetime
        or candidate.tzinfo is None
        or candidate.utcoffset() != dt.timedelta(0)
    ):
        raise authorization_error("invalid_validation_time")
    return candidate


def _assert_temporal_authority(
    intent: Mapping[str, Any],
    trust_key: ConnectorPublicationAuthorizationTrustKey,
    *,
    now: dt.datetime,
) -> tuple[dt.datetime, dt.datetime]:
    issued_at = publication_authorization_timestamp(
        intent["issued_at"],
        code="invalid_time",
    )
    expires_at = publication_authorization_timestamp(
        intent["expires_at"],
        code="invalid_time",
    )
    maximum_validity = dt.timedelta(
        seconds=PUBLICATION_AUTHORIZATION_MAX_VALIDITY_SECONDS
    )
    future_skew = dt.timedelta(seconds=PUBLICATION_AUTHORIZATION_FUTURE_SKEW_SECONDS)
    if issued_at > now + future_skew:
        raise authorization_error("issued_in_future")
    if expires_at <= issued_at or expires_at - issued_at > maximum_validity:
        raise authorization_error("invalid_validity_window")
    if now >= expires_at:
        raise authorization_error("expired")
    _assert_key_lifecycle(
        trust_key, issued_at=issued_at, expires_at=expires_at, now=now
    )
    return issued_at, expires_at


def _assert_key_lifecycle(
    trust_key: ConnectorPublicationAuthorizationTrustKey,
    *,
    issued_at: dt.datetime,
    expires_at: dt.datetime,
    now: dt.datetime,
) -> None:
    if trust_key.status == "active":
        return
    retired_at = trust_key.retired_at
    verify_until = trust_key.verify_until
    if (
        retired_at is None
        or verify_until is None
        or issued_at >= retired_at
        or expires_at > verify_until
        or now >= verify_until
    ):
        raise authorization_error("retired_key_rejected")


def _assert_source_authority(
    intent: Mapping[str, Any],
    trust_key: ConnectorPublicationAuthorizationTrustKey,
    bundle: ConnectorPublicationBundle,
) -> tuple[str, ...]:
    signed_source_ids = tuple(intent["source_ids"])
    bundle_source_ids = bundle.generation.source_ordinal_map
    if signed_source_ids != bundle_source_ids:
        raise authorization_error("source_binding_mismatch")
    authorized_source_ids = set(trust_key.public_source_ids)
    if any(source_id not in authorized_source_ids for source_id in signed_source_ids):
        raise authorization_error("source_rights_denied")
    return signed_source_ids


def _assert_bundle_binding(
    intent: Mapping[str, Any],
    bundle: ConnectorPublicationBundle,
    sealed_generation: SealedConnectorGeneration,
) -> None:
    if (
        type(bundle) is not ConnectorPublicationBundle
        or type(sealed_generation) is not SealedConnectorGeneration
    ):
        raise authorization_error("invalid_binding")
    identity_matches = (
        hmac.compare_digest(
            intent["source_vector_id"],
            bundle.source_vector.source_vector_id,
        )
        and hmac.compare_digest(
            intent["source_vector_id"],
            sealed_generation.source_vector_id,
        )
        and hmac.compare_digest(
            intent["generation_id"],
            bundle.generation.generation_id,
        )
        and hmac.compare_digest(
            intent["generation_id"],
            sealed_generation.generation_id,
        )
    )
    if (
        not identity_matches
        or intent["target_generation_key"] != sealed_generation.generation_key
        or bundle.counts != sealed_generation.counts
    ):
        raise authorization_error("binding_mismatch")


def _predecessor_key(intent: Mapping[str, Any]) -> int | None:
    predecessor = intent["expected_predecessor"]
    if predecessor["state"] == "absent":
        return None
    return predecessor["generation_key"]


def _verify_connector_publication_authorization(
    envelope: Any,
    *,
    trust: ConnectorPublicationAuthorizationTrust,
    bundle: ConnectorPublicationBundle,
    sealed_generation: SealedConnectorGeneration,
    now: dt.datetime,
) -> ConnectorPublicationAuthorizationReceipt:
    intent, raw_signature = validated_publication_authorization_envelope(envelope)
    trust_key = _selected_trust_key(intent, trust)
    signature_bytes = _decode_signature(raw_signature)
    _verify_signature(intent, signature_bytes, trust_key)
    validation_time = _validated_now(now)
    issued_at, expires_at = _assert_temporal_authority(
        intent,
        trust_key,
        now=validation_time,
    )
    _assert_bundle_binding(intent, bundle, sealed_generation)
    source_ids = _assert_source_authority(intent, trust_key, bundle)
    return ConnectorPublicationAuthorizationReceipt(
        intent_id=intent["intent_id"],
        publication_scope_id=intent["publication_scope_id"],
        authority_id=intent["authority_id"],
        authority_release_digest=intent["authority_release_digest"],
        key_id=intent["key_id"],
        source_ids=source_ids,
        source_vector_id=intent["source_vector_id"],
        generation_id=intent["generation_id"],
        target_generation_key=intent["target_generation_key"],
        expected_pointer_version=intent["expected_pointer_version"],
        expected_predecessor_generation_key=_predecessor_key(intent),
        issued_at=issued_at,
        expires_at=expires_at,
        validated_at=validation_time,
    )


def verify_connector_publication_authorization(
    envelope: Any,
    *,
    trust: ConnectorPublicationAuthorizationTrust,
    bundle: ConnectorPublicationBundle,
    sealed_generation: SealedConnectorGeneration,
    now: dt.datetime,
) -> ConnectorPublicationAuthorizationReceipt:
    """Verify rights and binding, returning a non-authorizing receipt.

    ``now`` must come from a trusted process clock. A future cutover operation
    must perform this verification again immediately before its database CAS;
    it must never accept the returned receipt as authorization evidence.
    """

    try:
        return _verify_connector_publication_authorization(
            envelope,
            trust=trust,
            bundle=bundle,
            sealed_generation=sealed_generation,
            now=now,
        )
    except ConnectorPublicationAuthorizationError:
        raise
    except Exception:
        error_code = "verification_failed"
    raise authorization_error(error_code)
