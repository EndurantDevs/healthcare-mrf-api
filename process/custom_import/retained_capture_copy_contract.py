# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed controller authorization for copying one sealed segmented capture."""

import base64
import hashlib
import hmac
import math
import re

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from process.custom_import import family_shard_handoff as custody

CONTRACT = "custom-import/retained-capture-copy/v1"
DOMAIN = b"custom-import/retained-capture-copy-signature/v1\x00"
FIELDS = frozenset(
    {
        "contract",
        "controller_execution_id",
        "source_capture_bundle_id",
        "source_capture_manifest_sha256",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
        "request_identity_sha256",
        "source_request_sha256",
        "statement_sha256",
        "policy_sha256",
        "bearer_sha256",
        "capture_secret_uid",
        "authorization_expires_at",
        "owner_profile_sha256",
        "owner_image",
    }
)
IDENTIFIERS = frozenset(
    {
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_capture_bundle_id",
    }
)
DIGESTS = frozenset(name for name in FIELDS if name.endswith("sha256"))


def checked_descriptor(value):
    """Accept only exact bounded scalar scope and immutable producer coordinates."""
    if type(value) is not dict or value.keys() != FIELDS or value["contract"] != CONTRACT:
        custody.unavailable()
    for name in IDENTIFIERS:
        if type(value[name]) is not int or not 0 < value[name] < 2**63:
            custody.unavailable()
    for name in DIGESTS:
        if type(value[name]) is not str or re.fullmatch(r"[0-9a-f]{64}", value[name]) is None:
            custody.unavailable()
    for name in ("controller_execution_id", "capture_secret_uid"):
        if type(value[name]) is not str or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value[name]) is None:
            custody.unavailable()
    custody.timestamp(value["authorization_expires_at"])
    if (
        type(value["owner_image"]) is not str
        or re.fullmatch(r"[a-z0-9][a-z0-9./_-]+@sha256:[0-9a-f]{64}", value["owner_image"]) is None
    ):
        custody.unavailable()
    return value


def verify(signed, *, verifying_key, bearer=None, now=None):
    """Authenticate before native work; historical observation never renews authority."""
    if type(signed) is not dict or signed.keys() != {"descriptor", "signature"}:
        custody.unavailable()
    descriptor = checked_descriptor(signed["descriptor"])
    try:
        signature = signed["signature"]
        if type(signature) is not str or re.fullmatch(r"[A-Za-z0-9_-]{86}", signature) is None:
            custody.unavailable()
        raw = base64.b64decode(signature + "==", altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != signature:
            custody.unavailable()
        Ed25519PublicKey.from_public_bytes(verifying_key).verify(raw, DOMAIN + custody.canonical(descriptor))
    except ValueError, TypeError, InvalidSignature:
        custody.unavailable()
    if bearer is not None and not hmac.compare_digest(
        hashlib.sha256(custody.checked_bearer(bearer)).hexdigest(), descriptor["bearer_sha256"]
    ):
        custody.unavailable()
    if now is not None and (
        type(now) not in (int, float)
        or not math.isfinite(now)
        or now >= custody.timestamp(descriptor["authorization_expires_at"]).timestamp()
    ):
        custody.unavailable()
    return descriptor


def identity(signed):
    """Keep authorization identity separate from unchanged capture content hashes."""
    checked_descriptor(signed["descriptor"])
    return hashlib.sha256(custody.canonical(signed)).hexdigest()
