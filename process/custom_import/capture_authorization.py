# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticate a closed capture envelope before the HTTP adapter reads any body.

The adapter supplies exactly the seven contract headers, preserving duplicates,
and rejects unknown capture-prefixed headers before selecting them. A verified
permit does not authorize the supplied lease or execution pins: the handler must
check retained scope, cancellation, fence and deadlines under its existing locks.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta

from process.custom_import import admission_authorization as admission
from process.custom_import.capture_limits import CaptureLimits

CAPTURE_PATH = "/control/v1/custom-import/capture"
PERMIT_CONTRACT = "custom-import-capture-permit/v1"
CONTEXT_HEADER = "X-Custom-Import-Capture-Context"
KEY_ID_HEADER = "X-Custom-Import-Capture-Key-Id"
SIGNATURE_HEADER = "X-Custom-Import-Capture-Signature"
EXECUTION_ID_HEADER = "X-Custom-Import-Capture-Execution-Id"
FENCE_HEADER = "X-Custom-Import-Capture-Fence"
MAX_CONTEXT_BYTES = 4_096
ERROR = "custom_import_capture_authorization_invalid"
_SIGNATURE_DOMAIN = b"CUSTOM_IMPORT_CAPTURE_PERMIT_V1\x00"
_PATHS = frozenset(CAPTURE_PATH + suffix for suffix in ("-preflight", "-bundle", "-retained"))
_DECIMAL_ID = re.compile(r"[1-9][0-9]{0,18}")
_DEFAULT_LIMITS = asdict(CaptureLimits())
_HEADER_LIMITS = {
    "content-type": len("application/octet-stream"),
    "authorization": len("Bearer ") + (admission.MAX_TOKEN_BYTES * 4 + 2) // 3,
    CONTEXT_HEADER.lower(): (MAX_CONTEXT_BYTES * 4 + 2) // 3,
    KEY_ID_HEADER.lower(): 32,
    SIGNATURE_HEADER.lower(): 43,
    EXECUTION_ID_HEADER.lower(): 19,
    FENCE_HEADER.lower(): 19,
}


class CaptureAuthorizationError(ValueError):
    """A fixed, redacted rejection of capture authority or configuration."""


def _fail() -> CaptureAuthorizationError:
    return CaptureAuthorizationError(ERROR)


@dataclass(frozen=True, slots=True)
class CapturePermit:
    audience: str
    contract: str
    dataset_id: int
    definition_revision_id: int
    definition_sha256: str
    expected_content_sha256: str
    expires_at: datetime
    idempotency_key: str = field(repr=False)
    issued_at: datetime
    issuer: str
    limits: CaptureLimits
    maximum_dataset_retained_bytes: int
    method: str
    origin: str
    path: str
    request_identity_sha256: str
    schema_revision_id: int
    timeout_seconds: int


@dataclass(frozen=True, slots=True)
class VerifiedCapture:
    permit: CapturePermit
    token: admission.LeaseToken = field(repr=False)
    execution_id: int
    fence: int


_PERMIT_FIELDS = frozenset(item.name for item in fields(CapturePermit))


def _permit(context: bytes, *, expected_origin: str, trusted_now: datetime) -> CapturePermit:
    """Parse the canonical document only; this does not authenticate its signature."""

    parsed = admission._object(context, MAX_CONTEXT_BYTES)
    if parsed.keys() != _PERMIT_FIELDS:
        raise _fail()
    expected_by_name = {
        "audience": "custom-import-engine",
        "contract": PERMIT_CONTRACT,
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "origin": admission._origin(expected_origin),
        "path": CAPTURE_PATH,
    }
    if any(
        type(parsed[name]) is not str or parsed[name] != expected_value
        for name, expected_value in expected_by_name.items()
    ):
        raise _fail()
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id"):
        admission._positive_id(parsed[name])
    for name in ("definition_sha256", "request_identity_sha256", "expected_content_sha256"):
        digest = parsed[name]
        if type(digest) is not str or admission._SHA256.fullmatch(digest) is None or digest == "0" * 64:
            raise _fail()
    if (
        type(parsed["idempotency_key"]) is not str
        or admission._IDEMPOTENCY_KEY.fullmatch(parsed["idempotency_key"]) is None
    ):
        raise _fail()
    if type(parsed["timeout_seconds"]) is not int or not 1 <= parsed["timeout_seconds"] <= 3_600:
        raise _fail()
    admission._positive_id(parsed["maximum_dataset_retained_bytes"], minimum=0)
    limits = parsed["limits"]
    if (
        type(limits) is not dict
        or limits.keys() != _DEFAULT_LIMITS.keys()
        or any(
            type(limits[name]) is not int or not 1 <= limits[name] <= maximum
            for name, maximum in _DEFAULT_LIMITS.items()
        )
    ):
        raise _fail()
    limits = CaptureLimits(**limits)
    issued, expires = admission._canonical_utc(parsed["issued_at"]), admission._canonical_utc(parsed["expires_at"])
    if not timedelta(0) < expires - issued <= timedelta(seconds=admission.MAX_TTL_SECONDS):
        raise _fail()
    if not issued <= admission._trusted_utc(trusted_now) < expires:
        raise _fail()
    return CapturePermit(**{**parsed, "limits": limits, "issued_at": issued, "expires_at": expires})


def _signature_message(key_id: str, context: bytes) -> bytes:
    encoded_id = key_id.encode("ascii")
    return (
        _SIGNATURE_DOMAIN + len(encoded_id).to_bytes(2, "big") + encoded_id + len(context).to_bytes(8, "big") + context
    )


def sign_permit(
    context: bytes,
    *,
    expected_origin: str,
    trusted_now: datetime,
    launch_expires_at: datetime,
    keyring: admission.AdmissionKeyring,
) -> admission.SignedPermit:
    """Issuer-only signing; a permit cannot outlive its trusted signed launch."""

    try:
        permit = _permit(context, expected_origin=expected_origin, trusted_now=trusted_now)
        if permit.expires_at > admission._trusted_utc(launch_expires_at):
            raise _fail()
        key_id = keyring.active_key_id
        signature = hmac.new(keyring.key_for(key_id), _signature_message(key_id, context), hashlib.sha256).digest()
        return admission.SignedPermit(
            admission._base64url_encode(context), key_id, admission._base64url_encode(signature)
        )
    except ValueError:
        failure = _fail()
    raise failure


def _headers(pairs: list[tuple[str, str]] | tuple[tuple[str, str], ...]) -> dict[str, str]:
    if type(pairs) not in (list, tuple) or len(pairs) != len(_HEADER_LIMITS):
        raise _fail()
    header_by_name = {}
    for pair in pairs:
        if type(pair) not in (tuple, list) or len(pair) != 2:
            raise _fail()
        name, value = pair
        if type(name) is not str or len(name) > 64 or not name.isascii():
            raise _fail()
        name = name.lower()
        if (
            name not in _HEADER_LIMITS
            or name in header_by_name
            or type(value) is not str
            or not 1 <= len(value) <= _HEADER_LIMITS[name]
            or not value.isascii()
            or not value.isprintable()
            or value != value.strip()
        ):
            raise _fail()
        header_by_name[name] = value
    if header_by_name.keys() != _HEADER_LIMITS.keys() or header_by_name["content-type"] != "application/octet-stream":
        raise _fail()
    return header_by_name


def verify_request(
    *,
    headers: list[tuple[str, str]] | tuple[tuple[str, str], ...],
    method: str,
    path: str,
    query_string: str,
    trusted_now: datetime,
    expected_origin: str,
    keyring: admission.AdmissionKeyring,
) -> VerifiedCapture:
    """Authenticate headers only; streaming bytes remain unread and untrusted."""

    try:
        if (
            type(method) is not str
            or method != "POST"
            or type(path) is not str
            or path not in _PATHS
            or type(query_string) is not str
            or query_string
        ):
            raise _fail()
        header_by_name = _headers(headers)
        key_id = header_by_name[KEY_ID_HEADER.lower()]
        if admission._KEY_ID.fullmatch(key_id) is None:
            raise _fail()
        context = admission._base64url_decode(header_by_name[CONTEXT_HEADER.lower()], MAX_CONTEXT_BYTES)
        signature = admission._base64url_decode(header_by_name[SIGNATURE_HEADER.lower()], 32)
        expected_signature = hmac.new(
            keyring.key_for(key_id), _signature_message(key_id, context), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(signature, expected_signature):
            raise _fail()
        permit = _permit(context, expected_origin=expected_origin, trusted_now=trusted_now)
        authorization = header_by_name["authorization"]
        if not authorization.startswith("Bearer "):
            raise _fail()
        token = admission.LeaseToken(
            admission._base64url_decode(authorization[len("Bearer ") :], admission.MAX_TOKEN_BYTES)
        )
        identifiers = []
        for name in (EXECUTION_ID_HEADER, FENCE_HEADER):
            identifier_text = header_by_name[name.lower()]
            if _DECIMAL_ID.fullmatch(identifier_text) is None:
                raise _fail()
            identifiers.append(admission._positive_id(int(identifier_text)))
        return VerifiedCapture(permit, token, *identifiers)
    except ValueError:
        failure = _fail()
    raise failure
