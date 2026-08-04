# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed source-rights authorization contract for connector publication."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from process.tin_npi_connector_publication import (
    TinNpiConnectorPublicationError,
)

PUBLICATION_AUTHORIZATION_CONTRACT_ID = (
    "healthporta.tin-npi.publication-authorization.v1"
)
PUBLICATION_AUTHORIZATION_ACTION = "publish"
PUBLICATION_AUTHORIZATION_RIGHTS_PROFILE = "phase1_public_sources_only_v1"
PUBLICATION_AUTHORIZATION_SIGNATURE_ALGORITHM = "Ed25519"
PUBLICATION_AUTHORIZATION_MAX_VALIDITY_SECONDS = 15 * 60
PUBLICATION_AUTHORIZATION_FUTURE_SKEW_SECONDS = 5
PUBLICATION_AUTHORIZATION_MAX_TRUST_KEYS = 8
PUBLICATION_AUTHORIZATION_MAX_SOURCE_IDS = 1024

_AUTHORIZATION_ID_DOMAIN = b"healthporta.tin-npi.publication-authorization-id.v1\x00"
_AUTHORIZATION_SIGNATURE_DOMAIN = (
    b"healthporta.tin-npi.publication-authorization.v1\x00"
)
_LOWER_HEX_32_BYTES = re.compile(r"[0-9a-f]{64}\Z")
_OPAQUE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}\Z")
_UTC_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z")
_MAX_SIGNED_BIGINT = (1 << 63) - 1
_INTENT_FIELDS = frozenset(
    {
        "action",
        "authority_id",
        "authority_release_digest",
        "contract_id",
        "expected_pointer_version",
        "expected_predecessor",
        "expires_at",
        "generation_id",
        "intent_id",
        "issued_at",
        "key_id",
        "nonce",
        "publication_scope_id",
        "rights_profile",
        "signature_algorithm",
        "source_ids",
        "source_vector_id",
        "target_generation_key",
    }
)
_INTENT_ID_FIELDS = _INTENT_FIELDS.difference({"intent_id"})
_ENVELOPE_FIELDS = frozenset({"intent", "signature"})
_AUTHORIZATION_ERROR_CODES = frozenset(
    {
        "authority_mismatch",
        "authority_release_mismatch",
        "binding_mismatch",
        "expired",
        "intent_id_mismatch",
        "invalid_authority",
        "invalid_binding",
        "invalid_envelope_fields",
        "invalid_generation",
        "invalid_intent_fields",
        "invalid_intent_id",
        "invalid_key",
        "invalid_nonce",
        "invalid_pointer",
        "invalid_predecessor",
        "invalid_scope",
        "invalid_signature",
        "invalid_source_ids",
        "invalid_source_vector",
        "invalid_time",
        "invalid_trust",
        "invalid_validation_time",
        "invalid_validity_window",
        "issued_in_future",
        "retired_key_rejected",
        "scope_mismatch",
        "source_binding_mismatch",
        "source_rights_denied",
        "unauthorized_rights_profile",
        "unknown_key",
        "unsupported_action",
        "unsupported_contract",
        "unsupported_signature_algorithm",
        "verification_failed",
    }
)


class ConnectorPublicationAuthorizationError(TinNpiConnectorPublicationError):
    """Reject an authorization using only a stable, value-free code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(f"connector publication authorization {code}")


def authorization_error(code: str) -> ConnectorPublicationAuthorizationError:
    """Create one stable authorization error without rejected values."""
    safe_code = (
        code
        if type(code) is str and code in _AUTHORIZATION_ERROR_CODES
        else "verification_failed"
    )
    return ConnectorPublicationAuthorizationError(safe_code)


def _exact_mapping(
    candidate: Any,
    expected_fields: frozenset[str],
    *,
    code: str,
) -> dict[str, Any]:
    if type(candidate) is not dict:
        raise authorization_error(code)
    candidate_fields_by_name = dict(candidate)
    if set(candidate_fields_by_name) != expected_fields:
        raise authorization_error(code)
    return candidate_fields_by_name


def _opaque_id(candidate: Any, *, code: str) -> str:
    if type(candidate) is not str or _OPAQUE_ID.fullmatch(candidate) is None:
        raise authorization_error(code)
    return candidate


def _hex_digest(candidate: Any, *, code: str) -> str:
    if type(candidate) is not str or _LOWER_HEX_32_BYTES.fullmatch(candidate) is None:
        raise authorization_error(code)
    return candidate


def _bounded_integer(
    candidate: Any,
    *,
    minimum: int,
    code: str,
) -> int:
    if type(candidate) is not int or not minimum <= candidate <= _MAX_SIGNED_BIGINT:
        raise authorization_error(code)
    return candidate


def publication_authorization_timestamp(candidate: Any, *, code: str) -> dt.datetime:
    """Parse one exact UTC-second timestamp without echoing its value."""
    if type(candidate) is not str or _UTC_TIMESTAMP.fullmatch(candidate) is None:
        raise authorization_error(code)
    try:
        parsed = dt.datetime.strptime(candidate, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        is_timestamp_valid = False
    else:
        is_timestamp_valid = True
    if not is_timestamp_valid:
        raise authorization_error(code)
    return parsed.replace(tzinfo=dt.timezone.utc)


def _utc_second(candidate: Any, *, code: str) -> dt.datetime:
    if (
        type(candidate) is not dt.datetime
        or candidate.tzinfo is None
        or candidate.utcoffset() != dt.timedelta(0)
        or candidate.microsecond != 0
    ):
        raise authorization_error(code)
    return candidate


def _validated_source_ids(candidate: Any) -> tuple[str, ...]:
    if type(candidate) is not list or not 1 <= len(candidate) <= (
        PUBLICATION_AUTHORIZATION_MAX_SOURCE_IDS
    ):
        raise authorization_error("invalid_source_ids")
    source_ids = tuple(
        _source_id(source_id, code="invalid_source_ids") for source_id in candidate
    )
    if source_ids != tuple(sorted(set(source_ids), key=str.encode)):
        raise authorization_error("invalid_source_ids")
    return source_ids


def _source_id(candidate: Any, *, code: str) -> str:
    if type(candidate) is not str or _SOURCE_ID.fullmatch(candidate) is None:
        raise authorization_error(code)
    return candidate


def _validated_predecessor(
    candidate: Any,
    *,
    pointer_version: int,
    target_generation_key: int,
) -> int | None:
    if type(candidate) is not dict:
        raise authorization_error("invalid_predecessor")
    predecessor_fields_by_name = dict(candidate)
    state = predecessor_fields_by_name.get("state")
    if state == "absent" and set(predecessor_fields_by_name) == {"state"}:
        if pointer_version != 0:
            raise authorization_error("invalid_predecessor")
        return None
    if state != "present" or set(predecessor_fields_by_name) != {
        "state",
        "generation_key",
    }:
        raise authorization_error("invalid_predecessor")
    predecessor_key = _bounded_integer(
        predecessor_fields_by_name["generation_key"],
        minimum=1,
        code="invalid_predecessor",
    )
    if pointer_version <= 0 or predecessor_key == target_generation_key:
        raise authorization_error("invalid_predecessor")
    return predecessor_key


def _validated_intent_scalars(intent: Mapping[str, Any]) -> dict[str, Any]:
    if intent["contract_id"] != PUBLICATION_AUTHORIZATION_CONTRACT_ID:
        raise authorization_error("unsupported_contract")
    if intent["action"] != PUBLICATION_AUTHORIZATION_ACTION:
        raise authorization_error("unsupported_action")
    if intent["rights_profile"] != PUBLICATION_AUTHORIZATION_RIGHTS_PROFILE:
        raise authorization_error("unauthorized_rights_profile")
    if intent["signature_algorithm"] != PUBLICATION_AUTHORIZATION_SIGNATURE_ALGORITHM:
        raise authorization_error("unsupported_signature_algorithm")
    return {
        "authority_id": _opaque_id(intent["authority_id"], code="invalid_authority"),
        "authority_release_digest": _hex_digest(
            intent["authority_release_digest"], code="invalid_authority"
        ),
        "generation_id": _hex_digest(
            intent["generation_id"], code="invalid_generation"
        ),
        "key_id": _opaque_id(intent["key_id"], code="invalid_key"),
        "nonce": _hex_digest(intent["nonce"], code="invalid_nonce"),
        "publication_scope_id": _opaque_id(
            intent["publication_scope_id"], code="invalid_scope"
        ),
        "source_vector_id": _hex_digest(
            intent["source_vector_id"], code="invalid_source_vector"
        ),
    }


def _validated_intent(candidate: Any, *, includes_id: bool) -> dict[str, Any]:
    expected_fields = _INTENT_FIELDS if includes_id else _INTENT_ID_FIELDS
    intent = _exact_mapping(
        candidate,
        expected_fields,
        code="invalid_intent_fields",
    )
    scalar_fields = _validated_intent_scalars(intent)
    target_key = _bounded_integer(
        intent["target_generation_key"],
        minimum=1,
        code="invalid_generation",
    )
    pointer_version = _bounded_integer(
        intent["expected_pointer_version"],
        minimum=0,
        code="invalid_pointer",
    )
    predecessor_key = _validated_predecessor(
        intent["expected_predecessor"],
        pointer_version=pointer_version,
        target_generation_key=target_key,
    )
    publication_authorization_timestamp(intent["issued_at"], code="invalid_time")
    publication_authorization_timestamp(intent["expires_at"], code="invalid_time")
    source_ids = _validated_source_ids(intent["source_ids"])
    if includes_id:
        _hex_digest(intent["intent_id"], code="invalid_intent_id")
    predecessor = (
        {"state": "absent"}
        if predecessor_key is None
        else {"state": "present", "generation_key": predecessor_key}
    )
    intent_fields_by_name = {
        "action": PUBLICATION_AUTHORIZATION_ACTION,
        "authority_id": scalar_fields["authority_id"],
        "authority_release_digest": scalar_fields["authority_release_digest"],
        "contract_id": PUBLICATION_AUTHORIZATION_CONTRACT_ID,
        "expected_pointer_version": pointer_version,
        "expected_predecessor": predecessor,
        "expires_at": intent["expires_at"],
        "generation_id": scalar_fields["generation_id"],
        "issued_at": intent["issued_at"],
        "key_id": scalar_fields["key_id"],
        "nonce": scalar_fields["nonce"],
        "publication_scope_id": scalar_fields["publication_scope_id"],
        "rights_profile": PUBLICATION_AUTHORIZATION_RIGHTS_PROFILE,
        "signature_algorithm": PUBLICATION_AUTHORIZATION_SIGNATURE_ALGORITHM,
        "source_ids": list(source_ids),
        "source_vector_id": scalar_fields["source_vector_id"],
        "target_generation_key": target_key,
    }
    if includes_id:
        intent_fields_by_name["intent_id"] = intent["intent_id"]
    return intent_fields_by_name


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def publication_authorization_intent_id(intent_without_id: Any) -> str:
    """Derive the exact domain-separated identity of an unsigned intent."""
    validated_intent = _validated_intent(intent_without_id, includes_id=False)
    return hashlib.sha256(
        _AUTHORIZATION_ID_DOMAIN + _canonical_json(validated_intent).encode("ascii")
    ).hexdigest()


def _validated_complete_intent(candidate: Any) -> dict[str, Any]:
    validated_intent = _validated_intent(candidate, includes_id=True)
    intent_fields_by_name_without_id = {
        field_name: field_value
        for field_name, field_value in validated_intent.items()
        if field_name != "intent_id"
    }
    expected_intent_id = publication_authorization_intent_id(
        intent_fields_by_name_without_id
    )
    if not hmac.compare_digest(validated_intent["intent_id"], expected_intent_id):
        raise authorization_error("intent_id_mismatch")
    return validated_intent


def canonical_publication_authorization_json(candidate: Any) -> str:
    """Validate and canonically encode one complete signed intent."""

    return _canonical_json(_validated_complete_intent(candidate))


def publication_authorization_signature_message(candidate: Any) -> bytes:
    """Return the closed, domain-separated Ed25519 signature message."""

    return _AUTHORIZATION_SIGNATURE_DOMAIN + (
        canonical_publication_authorization_json(candidate).encode("ascii")
    )


def validated_publication_authorization_envelope(
    candidate: Any,
) -> tuple[dict[str, Any], Any]:
    """Return a detached normalized intent and signature from a closed envelope."""

    envelope = _exact_mapping(
        candidate,
        _ENVELOPE_FIELDS,
        code="invalid_envelope_fields",
    )
    normalized_intent = _validated_complete_intent(envelope["intent"])
    return normalized_intent, envelope["signature"]


@dataclass(frozen=True, repr=False)
class ConnectorPublicationAuthorizationTrustKey:
    """One public verification key and its exact public-source authority."""

    key_id: str
    public_key: bytes = field(repr=False)
    authority_release_digest: str = field(repr=False)
    public_source_ids: tuple[str, ...] = field(repr=False)
    status: str
    retired_at: dt.datetime | None = field(default=None, repr=False)
    verify_until: dt.datetime | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _opaque_id(self.key_id, code="invalid_trust")
        _hex_digest(self.authority_release_digest, code="invalid_trust")
        if type(self.public_key) is not bytes or len(self.public_key) != 32:
            raise authorization_error("invalid_trust")
        if (
            type(self.public_source_ids) is not tuple
            or not 1
            <= len(self.public_source_ids)
            <= PUBLICATION_AUTHORIZATION_MAX_SOURCE_IDS
        ):
            raise authorization_error("invalid_trust")
        source_ids = tuple(
            _source_id(source_id, code="invalid_trust")
            for source_id in self.public_source_ids
        )
        if source_ids != tuple(sorted(set(source_ids), key=str.encode)):
            raise authorization_error("invalid_trust")
        self._validate_lifecycle()

    def _validate_lifecycle(self) -> None:
        if self.status == "active":
            if self.retired_at is not None or self.verify_until is not None:
                raise authorization_error("invalid_trust")
            return
        if self.status != "retired":
            raise authorization_error("invalid_trust")
        retired_at = _utc_second(self.retired_at, code="invalid_trust")
        verify_until = _utc_second(self.verify_until, code="invalid_trust")
        maximum_window = dt.timedelta(
            seconds=PUBLICATION_AUTHORIZATION_MAX_VALIDITY_SECONDS
        )
        if not retired_at < verify_until <= retired_at + maximum_window:
            raise authorization_error("invalid_trust")

    def __repr__(self) -> str:
        return (
            "<connector-publication-authorization-key "
            f"status={self.status} sources={len(self.public_source_ids)}>"
        )


@dataclass(frozen=True, repr=False)
class ConnectorPublicationAuthorizationTrust:
    """Deployment-scoped offline authority and bounded rotation keyring."""

    publication_scope_id: str = field(repr=False)
    authority_id: str = field(repr=False)
    active_key_id: str = field(repr=False)
    keys: tuple[ConnectorPublicationAuthorizationTrustKey, ...] = field(repr=False)

    def __post_init__(self) -> None:
        _opaque_id(self.publication_scope_id, code="invalid_trust")
        _opaque_id(self.authority_id, code="invalid_trust")
        _opaque_id(self.active_key_id, code="invalid_trust")
        if (
            type(self.keys) is not tuple
            or not 1 <= len(self.keys) <= PUBLICATION_AUTHORIZATION_MAX_TRUST_KEYS
            or any(
                type(key) is not ConnectorPublicationAuthorizationTrustKey
                for key in self.keys
            )
        ):
            raise authorization_error("invalid_trust")
        key_ids = tuple(key.key_id for key in self.keys)
        active_ids = tuple(key.key_id for key in self.keys if key.status == "active")
        if key_ids != tuple(sorted(set(key_ids))) or active_ids != (
            self.active_key_id,
        ):
            raise authorization_error("invalid_trust")

    def __repr__(self) -> str:
        return f"<connector-publication-authorization-trust keys={len(self.keys)}>"


@dataclass(frozen=True, repr=False)
class ConnectorPublicationAuthorizationReceipt:
    """Redacted result of one verification, never a cutover capability.

    A publisher must verify the original signed envelope again with a trusted
    clock immediately before cutover and consume its CAS values in the same
    transaction. Merely possessing or constructing this receipt authorizes
    nothing.
    """

    intent_id: str = field(repr=False)
    publication_scope_id: str = field(repr=False)
    authority_id: str = field(repr=False)
    authority_release_digest: str = field(repr=False)
    key_id: str = field(repr=False)
    source_ids: tuple[str, ...] = field(repr=False)
    source_vector_id: str = field(repr=False)
    generation_id: str = field(repr=False)
    target_generation_key: int
    expected_pointer_version: int
    expected_predecessor_generation_key: int | None
    issued_at: dt.datetime = field(repr=False)
    expires_at: dt.datetime = field(repr=False)
    validated_at: dt.datetime = field(repr=False)

    def __repr__(self) -> str:
        predecessor_state = (
            "absent" if self.expected_predecessor_generation_key is None else "present"
        )
        return (
            "<connector-publication-authorization-receipt "
            f"target={self.target_generation_key} "
            f"pointer={self.expected_pointer_version} "
            f"predecessor={predecessor_state}>"
        )
