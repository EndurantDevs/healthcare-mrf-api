# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed controller mandates for explicit recovery of one original owner root."""

import base64
import datetime as dt
import hashlib
import hmac
import math
import re

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.custom_import import family_shard_handoff as handoff
from process.custom_import.build_continuation import authorization_bytes, parse_build_continuation_authorization

CONTRACT = "custom-import/owner-attempt/v1"
DOMAIN = b"custom-import/owner-attempt-signature/v1\x00"
FIELDS = frozenset(
    {
        "contract",
        "operation_id",
        "owner_handoff_id",
        "attempt",
        "previous_attempt_sha256",
        "previous_job_uid",
        "root_device",
        "root_inode",
        "kind",
        "authorization",
        "launch_expires_at",
        "owner_profile_sha256",
        "owner_image",
    }
)


def checked_descriptor(descriptor, parent):
    """Keep the original producer and root fixed across every explicit attempt."""
    if type(descriptor) is not dict or descriptor.keys() != FIELDS or descriptor["contract"] != CONTRACT:
        handoff.unavailable()
    original = handoff.checked_descriptor(parent["descriptor"])
    if (
        descriptor["owner_handoff_id"] != handoff.identity(parent)
        or descriptor["owner_profile_sha256"] != original["owner_profile_sha256"]
        or descriptor["owner_image"] != original["owner_image"]
        or type(descriptor["attempt"]) is not int
        or not 1 <= descriptor["attempt"] <= 32
        or descriptor["kind"] not in {"recover", "continue"}
        or any(type(descriptor[n]) is not int or descriptor[n] <= 0 for n in ("root_device", "root_inode"))
        or type(descriptor["previous_job_uid"]) is not str
        or handoff._OPAQUE.fullmatch(descriptor["previous_job_uid"]) is None
    ):
        handoff.unavailable()
    for name in ("operation_id", "previous_attempt_sha256"):
        digest = descriptor[name]
        if name == "previous_attempt_sha256" and descriptor["attempt"] == 1:
            if digest is not None:
                handoff.unavailable()
        elif type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            handoff.unavailable()
    expiry = handoff.timestamp(descriptor["launch_expires_at"])
    authorization = descriptor["authorization"]
    if authorization is None:
        if descriptor["kind"] != "recover" or expiry > min(
            handoff.timestamp(original["build_deadline_at"]), handoff.timestamp(original["authorization_expires_at"])
        ):
            handoff.unavailable()
    else:
        authorization = parse_build_continuation_authorization(authorization)
        expected_by_field = {
            "engine_execution_id": str(original["engine_execution_id"]),
            "capture_bundle_id": original["capture_bundle_id"],
            "producer_fence": original["fence"],
            "producer_token_sha256": original["bearer_sha256"],
            "request_identity_sha256": original["request_identity_sha256"],
            "base_generation_id": original["base_generation_id"],
            "base_pointer_version": original["base_pointer_version"],
        }
        if any(authorization[n] != expected_by_field[n] for n in expected_by_field):
            handoff.unavailable()
        if descriptor["kind"] == "continue" and expiry > dt.datetime.fromisoformat(authorization["launch_expires_at"]):
            handoff.unavailable()
    if len(handoff.canonical(descriptor)) > handoff.MAX_DOCUMENT_BYTES:
        handoff.unavailable()
    return descriptor


def verify(signed, parent, *, verifying_key, now=None, token=None):
    """Authenticate the child separately; an old parent never grants a new writer."""
    handoff.verify(parent, verifying_key=verifying_key)
    if type(signed) is not dict or signed.keys() != {"descriptor", "signature"}:
        handoff.unavailable()
    descriptor = checked_descriptor(signed["descriptor"], parent)
    try:
        signature = signed["signature"]
        if type(signature) is not str or re.fullmatch(r"[A-Za-z0-9_-]{86}", signature) is None:
            handoff.unavailable()
        raw = base64.b64decode(signature + "==", altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != signature:
            handoff.unavailable()
        Ed25519PublicKey.from_public_bytes(verifying_key).verify(raw, DOMAIN + handoff.canonical(descriptor))
    except ValueError, TypeError, InvalidSignature:
        handoff.unavailable()
    if now is not None and (
        type(now) not in (int, float)
        or not math.isfinite(now)
        or not now < handoff.timestamp(descriptor["launch_expires_at"]).timestamp() <= now + 900
    ):
        handoff.unavailable()
    if token is not None:
        authorization = descriptor["authorization"]
        expected = parent["descriptor"]["bearer_sha256"] if authorization is None else authorization["nonce_sha256"]
        if type(token) is not bytes or not hmac.compare_digest(hashlib.sha256(token).hexdigest(), expected):
            handoff.unavailable()
        if authorization is None:
            handoff.checked_bearer(token)
        elif len(token) != 32:
            handoff.unavailable()
    return descriptor


def identity(signed, parent):
    """Name the immutable signed attempt without changing producer data hashes."""
    checked_descriptor(signed["descriptor"], parent)
    return hashlib.sha256(handoff.canonical(signed)).hexdigest()


def authorization_sha256(descriptor):
    """Identify the existing exact continuation authorization when one is present."""
    authorization = descriptor["authorization"]
    return None if authorization is None else hashlib.sha256(authorization_bytes(authorization)).hexdigest()
