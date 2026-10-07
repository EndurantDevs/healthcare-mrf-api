# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure authorization envelopes for two fixed application-owned operations.

The HTTP adapter supplies exactly the five contract headers, preserving duplicate
names. It must reject unknown admission-prefixed headers before selecting them.
Ordinary HTTP transport headers are not inputs to this module. No environment,
network, database, or clock is consulted here.

Verification authenticates the immutable permit, not the lease or supplied run
pins. The handler must bind those pins to retained configuration and check the
original token, fence, cursor, cancellation state, and deadlines under its locks,
including immediately before commit. Never serialize these result objects into
logs or responses. The signer belongs only in the permit issuer, not the worker.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

ADMISSION_PATH = "/control/v1/custom-import/admission-batch"
PERMIT_CONTRACT = "custom-import-admission-permit/v1"
KEYRING_CONTRACT = "custom-import-admission-keyring/v1"
MAX_TTL_SECONDS = 90_000
MAX_TOKEN_BYTES = 4_096
MAX_BIGINT = 9_223_372_036_854_775_807
ERROR = "custom_import_admission_authorization_invalid"

CONTEXT_HEADER = "X-Custom-Import-Admission-Context"
KEY_ID_HEADER = "X-Custom-Import-Admission-Key-Id"
SIGNATURE_HEADER = "X-Custom-Import-Admission-Signature"
SOURCE_PATH = "/control/v1/custom-import/source-batch"
SOURCE_PERMIT_CONTRACT = "custom-import-source-permit/v1"
SOURCE_CONTEXT_HEADER = "X-Custom-Import-Source-Context"
SOURCE_KEY_ID_HEADER = "X-Custom-Import-Source-Key-Id"
SOURCE_SIGNATURE_HEADER = "X-Custom-Import-Source-Signature"
_SIGNATURE_DOMAIN = b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00"
_SOURCE_SIGNATURE_DOMAIN = b"CUSTOM_IMPORT_SOURCE_PERMIT_V1\x00"
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+")
_KEY_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_PERMIT_FIELDS = frozenset(
    {
        "audience",
        "contract",
        "dataset_id",
        "definition_revision_id",
        "expires_at",
        "idempotency_key",
        "issued_at",
        "issuer",
        "method",
        "origin",
        "path",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
    }
)
_BODY_FIELDS = frozenset({"build_id", "execution_id", "expected_after_occurrence_id", "fence"})
_HEADER_LIMITS = {
    "content-type": len("application/json"),
    "authorization": len("Bearer ") + (MAX_TOKEN_BYTES * 4 + 2) // 3,
    CONTEXT_HEADER.lower(): (2_048 * 4 + 2) // 3,
    KEY_ID_HEADER.lower(): 32,
    SIGNATURE_HEADER.lower(): 43,
}


class AdmissionAuthorizationError(ValueError):
    """A fixed, metadata-only rejection of the envelope or configuration."""


def _fail() -> AdmissionAuthorizationError:
    return AdmissionAuthorizationError(ERROR)


@dataclass(frozen=True, slots=True)
class AdmissionKeyring:
    active_key_id: str
    keys: tuple[tuple[str, bytes], ...] = field(repr=False)

    def key_for(self, key_id: str) -> bytes:
        """Return the retained key for key_id; reject unknown IDs without logging."""

        for candidate, key in self.keys:
            if hmac.compare_digest(candidate, key_id):
                return key
        raise _fail()


@dataclass(frozen=True, slots=True)
class AdmissionPermit:
    audience: str
    contract: str
    dataset_id: int
    definition_revision_id: int
    expires_at: datetime
    idempotency_key: str
    issued_at: datetime
    issuer: str
    method: str
    origin: str
    path: str
    schema_revision_id: int
    source_binding_revision_id: int
    source_binding_sha256: str


@dataclass(frozen=True, slots=True)
class BatchPins:
    build_id: int
    execution_id: int
    expected_after_occurrence_id: int
    fence: int


@dataclass(frozen=True, slots=True)
class LeaseToken:
    value: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class VerifiedAdmission:
    permit: AdmissionPermit
    pins: BatchPins
    token: LeaseToken = field(repr=False)


@dataclass(frozen=True, slots=True)
class SignedPermit:
    context: str = field(repr=False)
    key_id: str
    signature: str = field(repr=False)


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: object, maximum: int) -> bytes:
    if type(value) is not str or not 1 <= len(value) <= (maximum * 4 + 2) // 3 or _BASE64URL.fullmatch(value) is None:
        raise _fail()
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except binascii.Error, ValueError:
        decoded = None
    if decoded is None or not 1 <= len(decoded) <= maximum or _base64url_encode(decoded) != value:
        raise _fail()
    return decoded


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value_by_name = {}
    for name, value in pairs:
        if name in value_by_name:
            raise ValueError
        value_by_name[name] = value
    return value_by_name


def _reject_number(_value: str) -> None:
    raise ValueError


def _integer(value: str) -> int:
    if len(value) > 20:
        raise ValueError
    result = int(value)
    if not -(1 << 63) <= result <= MAX_BIGINT:
        raise ValueError
    return result


def _object(raw: bytes, maximum: int) -> dict[str, object]:
    if type(raw) is not bytes or not 1 <= len(raw) <= maximum:
        raise _fail()
    try:
        parsed = json.loads(
            raw.decode("ascii"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_number,
            parse_float=_reject_number,
            parse_int=_integer,
        )
        canonical = json.dumps(parsed, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    except UnicodeError, ValueError, RecursionError:
        parsed, canonical = None, None
    if type(parsed) is not dict or canonical.encode("ascii") != raw:
        raise _fail()
    return parsed


def _positive_id(value: object, *, minimum: int = 1) -> int:
    if type(value) is not int or not minimum <= value <= MAX_BIGINT:
        raise _fail()
    return value


def _canonical_utc(value: object) -> datetime:
    if type(value) is not str or _UTC.fullmatch(value) is None:
        raise _fail()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        parsed = None
    if parsed is None:
        raise _fail()
    return parsed


def _trusted_utc(value: datetime) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise _fail()
    return value


def _origin(origin_value: object) -> str:
    if (
        type(origin_value) is not str
        or not 1 <= len(origin_value) <= 512
        or not origin_value.isascii()
        or any(character.isspace() or not character.isprintable() for character in origin_value)
        or any(character in origin_value for character in "\\%?#")
    ):
        raise _fail()
    try:
        parsed = urlsplit(origin_value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        parsed, host, port = None, None, None
    if (
        parsed is None
        or not host
        or parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or port == 0
    ):
        raise _fail()
    if ":" not in host and any(_HOST_LABEL.fullmatch(label) is None for label in host.split(".")):
        raise _fail()
    authority = f"[{host}]" if ":" in host else host
    if port is not None:
        authority += f":{port}"
    if origin_value != f"https://{authority}":
        raise _fail()
    return origin_value


def load_keyring(document: bytes) -> AdmissionKeyring:
    """Load a bounded canonical document with a dedicated writer purpose."""

    parsed = _object(document, 4_096)
    if (
        parsed.keys() != {"active_key_id", "contract", "keys"}
        or parsed["contract"] != KEYRING_CONTRACT
        or type(parsed["keys"]) is not list
        or not 1 <= len(parsed["keys"]) <= 4
        or type(parsed["active_key_id"]) is not str
        or _KEY_ID.fullmatch(parsed["active_key_id"]) is None
    ):
        raise _fail()
    keys = []
    for entry in parsed["keys"]:
        if type(entry) is not dict or entry.keys() != {"key_id", "key_base64url"}:
            raise _fail()
        key_id = entry["key_id"]
        if type(key_id) is not str or _KEY_ID.fullmatch(key_id) is None:
            raise _fail()
        key = _base64url_decode(entry["key_base64url"], 32)
        if len(key) != 32:
            raise _fail()
        keys.append((key_id, key))
    if (
        len({key_id for key_id, _ in keys}) != len(keys)
        or len({key for _, key in keys}) != len(keys)
        or parsed["active_key_id"] not in {key_id for key_id, _ in keys}
    ):
        raise _fail()
    return AdmissionKeyring(parsed["active_key_id"], tuple(sorted(keys)))


def _permit(context: bytes, *, expected_origin: str, trusted_now: datetime, is_source: bool = False) -> AdmissionPermit:
    parsed = _object(context, 2_048)
    if parsed.keys() != _PERMIT_FIELDS:
        raise _fail()
    expected_by_name = {
        "contract": SOURCE_PERMIT_CONTRACT if is_source else PERMIT_CONTRACT,
        "issuer": "custom-import-execution-controller",
        "audience": "custom-import-engine",
        "method": "POST",
        "path": SOURCE_PATH if is_source else ADMISSION_PATH,
        "origin": _origin(expected_origin),
    }
    if any(type(parsed[name]) is not str or parsed[name] != value for name, value in expected_by_name.items()):
        raise _fail()
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "source_binding_revision_id"):
        _positive_id(parsed[name])
    if type(parsed["idempotency_key"]) is not str or _IDEMPOTENCY_KEY.fullmatch(parsed["idempotency_key"]) is None:
        raise _fail()
    digest = parsed["source_binding_sha256"]
    if type(digest) is not str or _SHA256.fullmatch(digest) is None or digest == "0" * 64:
        raise _fail()
    issued, expires = _canonical_utc(parsed["issued_at"]), _canonical_utc(parsed["expires_at"])
    if not timedelta(0) < expires - issued <= timedelta(seconds=MAX_TTL_SECONDS):
        raise _fail()
    if not issued <= _trusted_utc(trusted_now) < expires:
        raise _fail()
    return AdmissionPermit(**{**parsed, "issued_at": issued, "expires_at": expires})


def _signature_message(key_id: str, context: bytes, *, is_source: bool = False) -> bytes:
    encoded_id = key_id.encode("ascii")
    domain = _SOURCE_SIGNATURE_DOMAIN if is_source else _SIGNATURE_DOMAIN
    return domain + len(encoded_id).to_bytes(2, "big") + encoded_id + len(context).to_bytes(8, "big") + context


def sign_permit(
    context: bytes,
    *,
    expected_origin: str,
    trusted_now: datetime,
    launch_expires_at: datetime,
    keyring: AdmissionKeyring,
    is_source: bool = False,
) -> SignedPermit:
    """Issuer-only operation: cap the permit to the trusted signed launch expiry."""

    permit = _permit(context, expected_origin=expected_origin, trusted_now=trusted_now, is_source=is_source)
    if permit.expires_at > _trusted_utc(launch_expires_at):
        raise _fail()
    key_id = keyring.active_key_id
    signature = hmac.new(
        keyring.key_for(key_id), _signature_message(key_id, context, is_source=is_source), hashlib.sha256
    ).digest()
    return SignedPermit(_base64url_encode(context), key_id, _base64url_encode(signature))


def encode_lease_bearer(token: str | bytes | bytearray | memoryview) -> str:
    """Preserve the engine's text-to-UTF-8 or arbitrary byte token semantics."""

    if isinstance(token, str):
        try:
            value = token.encode("utf-8")
        except UnicodeError:
            value = b""
    elif isinstance(token, (bytes, bytearray, memoryview)):
        value = bytes(token)
    else:
        raise _fail()
    if not 1 <= len(value) <= MAX_TOKEN_BYTES:
        raise _fail()
    return "Bearer " + _base64url_encode(value)


def _headers(pairs: list[tuple[str, str]] | tuple[tuple[str, str], ...], *, is_source: bool = False) -> dict[str, str]:
    limits_by_name = {
        name.replace("-admission-", "-source-") if is_source else name: limit for name, limit in _HEADER_LIMITS.items()
    }
    if type(pairs) not in (list, tuple) or len(pairs) != len(limits_by_name):
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
            name not in limits_by_name
            or name in header_by_name
            or type(value) is not str
            or not 1 <= len(value) <= limits_by_name[name]
            or not value.isascii()
            or not value.isprintable()
            or value != value.strip()
        ):
            raise _fail()
        header_by_name[name] = value
    if header_by_name.keys() != limits_by_name.keys() or header_by_name["content-type"] != "application/json":
        raise _fail()
    return header_by_name


def _verify_authority(
    *,
    headers: list[tuple[str, str]] | tuple[tuple[str, str], ...],
    method: str,
    path: str,
    query_string: str,
    trusted_now: datetime,
    expected_origin: str,
    keyring: AdmissionKeyring,
    is_source: bool = False,
) -> tuple[AdmissionPermit, LeaseToken]:
    """Shared framing for the two fixed purposes, never caller-selected SQL."""

    expected_path = SOURCE_PATH if is_source else ADMISSION_PATH
    if method != "POST" or path != expected_path or type(query_string) is not str or query_string:
        raise _fail()
    header_by_name = _headers(headers, is_source=is_source)
    key_id = header_by_name[(SOURCE_KEY_ID_HEADER if is_source else KEY_ID_HEADER).lower()]
    if _KEY_ID.fullmatch(key_id) is None:
        raise _fail()
    context = _base64url_decode(header_by_name[(SOURCE_CONTEXT_HEADER if is_source else CONTEXT_HEADER).lower()], 2_048)
    signature = _base64url_decode(
        header_by_name[(SOURCE_SIGNATURE_HEADER if is_source else SIGNATURE_HEADER).lower()], 32
    )
    expected_signature = hmac.new(
        keyring.key_for(key_id), _signature_message(key_id, context, is_source=is_source), hashlib.sha256
    ).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise _fail()
    permit = _permit(context, expected_origin=expected_origin, trusted_now=trusted_now, is_source=is_source)
    authorization = header_by_name["authorization"]
    if not authorization.startswith("Bearer "):
        raise _fail()
    token = LeaseToken(_base64url_decode(authorization[len("Bearer ") :], MAX_TOKEN_BYTES))
    return permit, token


def verify_request(
    *,
    headers: list[tuple[str, str]] | tuple[tuple[str, str], ...],
    body: bytes,
    method: str,
    path: str,
    query_string: str,
    trusted_now: datetime,
    expected_origin: str,
    keyring: AdmissionKeyring,
) -> VerifiedAdmission:
    """Authenticate the permit and parse one closed admission request; do no writes."""

    permit, token = _verify_authority(
        headers=headers,
        method=method,
        path=path,
        query_string=query_string,
        trusted_now=trusted_now,
        expected_origin=expected_origin,
        keyring=keyring,
    )
    parsed = _object(body, 512)
    if parsed.keys() != _BODY_FIELDS:
        raise _fail()
    for name in _BODY_FIELDS:
        _positive_id(parsed[name], minimum=0 if name == "expected_after_occurrence_id" else 1)
    return VerifiedAdmission(permit, BatchPins(**parsed), token)
