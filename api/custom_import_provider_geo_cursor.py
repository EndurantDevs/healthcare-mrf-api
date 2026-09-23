# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed identity anchors for imported-field provider geo pagination."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict, dataclass
from uuid import UUID

from process.custom_import.read_contracts import (
    MAX_CURSOR_TTL_SECONDS,
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    PinnedReadTarget,
    canonical_read_document,
)
from process.custom_import.read_cursor import (
    MAX_CURSOR_CHARACTERS,
    _base64url_decode,
    _base64url_encode,
    _is_sha256_hex,
    _reject_duplicate_keys,
)

_DOMAIN = b"custom-import-provider-geo/v1\x00"


@dataclass(frozen=True, slots=True)
class GeoCursorState:
    """Bind an address identity, not exposed metric values, to one result set."""

    target: PinnedReadTarget
    query_fingerprint: str
    authorization_scope_sha256: str
    anchor_npi: str
    anchor_address_key: str
    issued_at: int
    expires_at: int


def issue_geo_cursor(state: GeoCursorState, *, secret: bytes) -> str:
    """Sign a bounded canonical anchor using a geo-specific MAC domain."""

    _validate_secret(secret)
    payload = canonical_read_document(_state_document(state))
    signature = hmac.digest(secret, _DOMAIN + payload, hashlib.sha256)
    token = _base64url_encode(payload + signature)
    if len(token) > MAX_CURSOR_CHARACTERS:
        raise _invalid_cursor()
    return token


def open_geo_cursor(
    token: object,
    *,
    secret: bytes,
    pinned_target: PinnedReadTarget,
    query_fingerprint: str,
    authorization_scope_sha256: str,
    trusted_now: int,
) -> GeoCursorState:
    """Authenticate before parsing and require fresh exact result-set identity."""

    _validate_secret(secret)
    if (
        type(token) is not str
        or not 1 <= len(token) <= MAX_CURSOR_CHARACTERS
        or type(pinned_target) is not PinnedReadTarget
        or not _is_sha256_hex(query_fingerprint)
        or not _is_sha256_hex(authorization_scope_sha256)
        or type(trusted_now) is not int
        or not 0 <= trusted_now < 2**63
    ):
        raise _invalid_cursor()
    decoded = _base64url_decode(token)
    if len(decoded) <= hashlib.sha256().digest_size:
        raise _invalid_cursor()
    cursor_payload, signature = decoded[:-32], decoded[-32:]
    if not hmac.compare_digest(signature, hmac.digest(secret, _DOMAIN + cursor_payload, hashlib.sha256)):
        raise _invalid_cursor()
    state = _state_from_payload(cursor_payload)
    if (
        state.target != pinned_target
        or not hmac.compare_digest(state.query_fingerprint, query_fingerprint)
        or not hmac.compare_digest(state.authorization_scope_sha256, authorization_scope_sha256)
        or not state.issued_at <= trusted_now < state.expires_at
    ):
        raise _invalid_cursor()
    return state


def _state_document(state: GeoCursorState) -> dict[str, object]:
    if type(state) is not GeoCursorState or type(state.target) is not PinnedReadTarget:
        raise _invalid_cursor()
    if (
        not _is_sha256_hex(state.query_fingerprint)
        or not _is_sha256_hex(state.authorization_scope_sha256)
        or type(state.anchor_npi) is not str
        or len(state.anchor_npi) != 10
        or not state.anchor_npi.isascii()
        or not state.anchor_npi.isdigit()
        or type(state.anchor_address_key) is not str
        or type(state.issued_at) is not int
        or type(state.expires_at) is not int
        or not 0 <= state.issued_at < state.expires_at < 2**63
        or state.expires_at - state.issued_at > MAX_CURSOR_TTL_SECONDS
    ):
        raise _invalid_cursor()
    try:
        if str(UUID(state.anchor_address_key)) != state.anchor_address_key:
            raise _invalid_cursor()
    except ValueError:
        raise _invalid_cursor() from None
    return {"version": 1, **asdict(state)}


def _state_from_payload(payload: bytes) -> GeoCursorState:
    try:
        document = json.loads(payload.decode("ascii"), object_pairs_hook=_reject_duplicate_keys)
        if (
            type(document) is not dict
            or type(document.get("version")) is not int
            or document.pop("version", None) != 1
            or type(document.get("target")) is not dict
        ):
            raise _invalid_cursor()
        document["target"] = PinnedReadTarget(**document["target"])
        state = GeoCursorState(**document)
        if canonical_read_document(_state_document(state)) != payload:
            raise _invalid_cursor()
        return state
    except UnicodeDecodeError, ValueError, TypeError:
        raise _invalid_cursor() from None


def _validate_secret(secret: object) -> None:
    if type(secret) is not bytes or not 32 <= len(secret) <= 128:
        raise CustomImportReadRequestError("read cursor secret is malformed")


def _invalid_cursor() -> CustomImportReadCursorError:
    return CustomImportReadCursorError("custom import geo cursor is invalid")
