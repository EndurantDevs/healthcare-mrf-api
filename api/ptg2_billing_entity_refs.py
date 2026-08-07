# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable public pseudonyms for sealed PTG tax-identity tokens."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import struct
from dataclasses import dataclass

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

BILLING_ENTITY_REF_PREFIX = "be1_"
_BILLING_ENTITY_REF_DOMAIN = b"healthporta.billing-entity-ref.v1\x00"
_TIN_ID_BYTES = 16
_TIN_HMAC_BYTES = 32
_REFERENCE_TAG_BYTES = 32
_REFERENCE_PAYLOAD_BYTES = _TIN_ID_BYTES + _REFERENCE_TAG_BYTES
_REFERENCE_PAYLOAD_CHARACTERS = 64
_URLSAFE_BASE64_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)


class PTG2BillingAssociationDataError(PTG2ManifestArtifactError):
    """The sealed tax-identity sidecar cannot prove the requested mapping."""


class PTG2BillingAssociationProjectionUnavailable(PTG2BillingAssociationDataError):
    """The snapshot has no searchable tax-identity projection."""


@dataclass(frozen=True, slots=True, repr=False)
class DecodedBillingEntityRef:
    """Redacted locator and authentication tag from one canonical public ref."""

    tin_id_128: bytes
    reference_tag: bytes

    def __post_init__(self) -> None:
        if (
            type(self.tin_id_128) is not bytes
            or len(self.tin_id_128) != _TIN_ID_BYTES
            or type(self.reference_tag) is not bytes
            or len(self.reference_tag) != _REFERENCE_TAG_BYTES
        ):
            raise PTG2BillingAssociationDataError("billing entity reference is invalid")

    def __repr__(self) -> str:
        return "<decoded-billing-entity-ref token=<redacted>>"


def _validated_snapshot_key(snapshot_key: object) -> int:
    if type(snapshot_key) is not int or not 1 <= snapshot_key < 2**63:
        raise PTG2BillingAssociationDataError(
            "billing association snapshot key is invalid"
        )
    return snapshot_key


def _validated_full_tin_hmac(tin_hmac_sha256: object) -> bytes:
    if type(tin_hmac_sha256) is not bytes or len(tin_hmac_sha256) != _TIN_HMAC_BYTES:
        raise PTG2BillingAssociationDataError(
            "billing association tax-identity token is invalid"
        )
    return tin_hmac_sha256


def _decoded_reference_payload(reference: object) -> bytes:
    if type(reference) is not str or not reference.startswith(
        BILLING_ENTITY_REF_PREFIX
    ):
        raise PTG2BillingAssociationDataError("billing entity reference is invalid")
    encoded_payload = reference.removeprefix(BILLING_ENTITY_REF_PREFIX)
    if len(encoded_payload) != _REFERENCE_PAYLOAD_CHARACTERS or any(
        character not in _URLSAFE_BASE64_CHARACTERS for character in encoded_payload
    ):
        raise PTG2BillingAssociationDataError("billing entity reference is invalid")
    try:
        decoded_payload = base64.b64decode(
            encoded_payload.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError):
        raise PTG2BillingAssociationDataError(
            "billing entity reference is invalid"
        ) from None
    if len(decoded_payload) != _REFERENCE_PAYLOAD_BYTES:
        raise PTG2BillingAssociationDataError("billing entity reference is invalid")
    canonical_payload = base64.urlsafe_b64encode(decoded_payload).rstrip(b"=")
    if not hmac.compare_digest(canonical_payload, encoded_payload.encode("ascii")):
        raise PTG2BillingAssociationDataError("billing entity reference is invalid")
    return decoded_payload


def decode_billing_entity_ref(reference: object) -> DecodedBillingEntityRef:
    """Strictly decode one frozen be1 reference without exposing it in errors."""

    decoded_payload = _decoded_reference_payload(reference)
    return DecodedBillingEntityRef(
        tin_id_128=decoded_payload[:_TIN_ID_BYTES],
        reference_tag=decoded_payload[_TIN_ID_BYTES:],
    )


def is_billing_ref_valid_for_token(
    decoded_reference: DecodedBillingEntityRef,
    *,
    snapshot_key: object,
    tin_hmac_sha256: object,
) -> bool:
    """Verify the locator and snapshot tag against one authoritative full HMAC."""

    if type(decoded_reference) is not DecodedBillingEntityRef:
        raise PTG2BillingAssociationDataError("billing entity reference is invalid")
    validated_snapshot_key = _validated_snapshot_key(snapshot_key)
    validated_full_hmac = _validated_full_tin_hmac(tin_hmac_sha256)
    expected_tag = hmac.new(
        validated_full_hmac,
        _BILLING_ENTITY_REF_DOMAIN
        + struct.pack(">Q", validated_snapshot_key)
        + decoded_reference.tin_id_128,
        hashlib.sha256,
    ).digest()
    has_matching_locator = hmac.compare_digest(
        decoded_reference.tin_id_128,
        validated_full_hmac[:_TIN_ID_BYTES],
    )
    has_matching_tag = hmac.compare_digest(
        decoded_reference.reference_tag,
        expected_tag,
    )
    return has_matching_locator & has_matching_tag


def encode_billing_entity_ref(
    *,
    snapshot_key: int,
    tin_id_128: bytes,
    tin_hmac_sha256: bytes,
) -> str:
    """Encode an opaque, collision-verifiable snapshot-tagged billing reference."""

    validated_snapshot_key = _validated_snapshot_key(snapshot_key)
    if type(tin_id_128) is not bytes or type(tin_hmac_sha256) is not bytes:
        raise PTG2BillingAssociationDataError(
            "billing association tax-identity token is invalid"
        )
    if (
        len(tin_id_128) != _TIN_ID_BYTES
        or len(tin_hmac_sha256) != _TIN_HMAC_BYTES
        or not hmac.compare_digest(tin_id_128, tin_hmac_sha256[:_TIN_ID_BYTES])
    ):
        raise PTG2BillingAssociationDataError(
            "billing association tax-identity token is invalid"
        )
    reference_tag = hmac.new(
        tin_hmac_sha256,
        _BILLING_ENTITY_REF_DOMAIN
        + struct.pack(">Q", validated_snapshot_key)
        + tin_id_128,
        hashlib.sha256,
    ).digest()
    encoded_reference = base64.urlsafe_b64encode(tin_id_128 + reference_tag).rstrip(
        b"="
    )
    return BILLING_ENTITY_REF_PREFIX + encoded_reference.decode("ascii")
