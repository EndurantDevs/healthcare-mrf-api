# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable public pseudonyms for sealed PTG tax-identity tokens."""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


BILLING_ENTITY_REF_PREFIX = "be1_"
_BILLING_ENTITY_REF_DOMAIN = b"healthporta.billing-entity-ref.v1\x00"
_TIN_ID_BYTES = 16
_TIN_HMAC_BYTES = 32


class PTG2BillingAssociationDataError(PTG2ManifestArtifactError):
    """The sealed tax-identity sidecar cannot prove the requested mapping."""


def encode_billing_entity_ref(
    *,
    snapshot_key: int,
    tin_id_128: bytes,
    tin_hmac_sha256: bytes,
) -> str:
    """Encode an opaque, collision-verifiable snapshot-tagged billing reference."""

    if type(snapshot_key) is not int or not 1 <= snapshot_key < 2**63:
        raise PTG2BillingAssociationDataError(
            "billing association snapshot key is invalid"
        )
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
        _BILLING_ENTITY_REF_DOMAIN + struct.pack(">Q", snapshot_key) + tin_id_128,
        hashlib.sha256,
    ).digest()
    encoded_reference = base64.urlsafe_b64encode(
        tin_id_128 + reference_tag
    ).rstrip(b"=")
    return BILLING_ENTITY_REF_PREFIX + encoded_reference.decode("ascii")
