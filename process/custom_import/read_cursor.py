# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Signed, bounded cursor codec for generic custom-import extension reads."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
from dataclasses import dataclass

from process.custom_import.read_contracts import (
    MAX_CURSOR_TTL_SECONDS,
    MAX_PAGE_OFFSET,
    READ_CORE_CONTRACT,
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    PinnedReadTarget,
    canonical_read_document,
)

MAX_CURSOR_CHARACTERS = 2_048

_CURSOR_PREFIX = "cir1"
_CURSOR_AAD = b"custom-import-read-core/v1\x00"
_TOKEN_PART = re.compile(r"^[A-Za-z0-9_-]+$", flags=re.ASCII)
_MAC_HEX = re.compile(r"^[0-9a-f]{64}$", flags=re.ASCII)


@dataclass(frozen=True, slots=True)
class ReadCursorState:
    """Opaque signed pagination state for one immutable normalized search."""

    target: PinnedReadTarget
    query_fingerprint: str
    authorization_scope_sha256: str
    offset: int
    issued_at: int
    expires_at: int


class ReadCursorCodec:
    """HMAC-authenticate bounded, canonical read-core page cursor state."""

    def __init__(self, secret: bytes) -> None:
        if type(secret) is not bytes or not 32 <= len(secret) <= 128:
            raise CustomImportReadRequestError("read cursor secret is malformed")
        self._secret = secret

    def issue(self, state: ReadCursorState) -> str:
        """Return a signed cursor after validating its exact bounded state."""

        encoded_payload = _base64url_encode(canonical_read_document(_cursor_payload(state)))
        signature = hmac.new(self._secret, _CURSOR_AAD + encoded_payload.encode("ascii"), hashlib.sha256).hexdigest()
        token = f"{_CURSOR_PREFIX}.{encoded_payload}.{signature}"
        if len(token) > MAX_CURSOR_CHARACTERS:
            raise CustomImportReadCursorError("custom import cursor is invalid")
        return token

    def open(
        self,
        token: object,
        *,
        pinned_target: PinnedReadTarget,
        query_fingerprint: str,
        authorization_scope_sha256: str,
        trusted_now: int,
    ) -> ReadCursorState:
        """Verify one cursor against the exact target, query, scope, and expiry."""

        _validate_open_request(token, pinned_target, query_fingerprint, authorization_scope_sha256, trusted_now)
        prefix, encoded_payload, signature = token.split(".")
        if (
            prefix != _CURSOR_PREFIX
            or _TOKEN_PART.fullmatch(encoded_payload) is None
            or _MAC_HEX.fullmatch(signature) is None
        ):
            raise CustomImportReadCursorError("custom import cursor is invalid")
        expected_signature = hmac.new(
            self._secret,
            _CURSOR_AAD + encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected_signature, signature):
            raise CustomImportReadCursorError("custom import cursor is invalid")
        state = _cursor_state_from_token_part(encoded_payload)
        if (
            state.target != pinned_target
            or not hmac.compare_digest(state.query_fingerprint, query_fingerprint)
            or not hmac.compare_digest(state.authorization_scope_sha256, authorization_scope_sha256)
            or state.expires_at <= trusted_now
        ):
            raise CustomImportReadCursorError("custom import cursor is invalid")
        return state


def _validate_open_request(
    token: object,
    pinned_target: PinnedReadTarget,
    query_fingerprint: str,
    authorization_scope_sha256: str,
    trusted_now: int,
) -> None:
    if (
        type(token) is not str
        or not 1 <= len(token) <= MAX_CURSOR_CHARACTERS
        or len(token.split(".")) != 3
        or type(pinned_target) is not PinnedReadTarget
        or not _is_sha256_hex(query_fingerprint)
        or not _is_sha256_hex(authorization_scope_sha256)
        or type(trusted_now) is not int
        or not 0 <= trusted_now < 2**63
    ):
        raise CustomImportReadCursorError("custom import cursor is invalid")


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: object) -> bytes:
    if type(value) is not str or _TOKEN_PART.fullmatch(value) is None:
        raise CustomImportReadCursorError("custom import cursor is invalid")
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except binascii.Error, ValueError:
        raise CustomImportReadCursorError("custom import cursor is invalid") from None
    if not hmac.compare_digest(_base64url_encode(decoded), value):
        raise CustomImportReadCursorError("custom import cursor is invalid")
    return decoded


def _cursor_payload(state: ReadCursorState) -> dict[str, object]:
    _validate_cursor_state(state)
    return {
        "auth": state.authorization_scope_sha256,
        "contract": READ_CORE_CONTRACT,
        "dataset": state.target.dataset_id,
        "definition": state.target.definition_revision_id,
        "expires": state.expires_at,
        "generation": state.target.generation_id,
        "issued": state.issued_at,
        "offset": state.offset,
        "profile": state.target.profile_id,
        "query": state.query_fingerprint,
        "schema": state.target.schema_revision_id,
    }


def _validate_cursor_state(state: object) -> None:
    if type(state) is not ReadCursorState:
        raise CustomImportReadCursorError("custom import cursor is invalid")
    if (
        not _is_sha256_hex(state.query_fingerprint)
        or not _is_sha256_hex(state.authorization_scope_sha256)
        or type(state.offset) is not int
        or not 0 <= state.offset <= MAX_PAGE_OFFSET
        or type(state.issued_at) is not int
        or type(state.expires_at) is not int
        or not 0 <= state.issued_at < state.expires_at < 2**63
        or state.expires_at - state.issued_at > MAX_CURSOR_TTL_SECONDS
    ):
        raise CustomImportReadCursorError("custom import cursor is invalid")


def _cursor_state_from_token_part(encoded_payload: str) -> ReadCursorState:
    try:
        decoded_payload = _base64url_decode(encoded_payload)
        cursor_document = json.loads(decoded_payload.decode("ascii"), object_pairs_hook=_reject_duplicate_keys)
    except UnicodeDecodeError, json.JSONDecodeError, CustomImportReadCursorError:
        raise CustomImportReadCursorError("custom import cursor is invalid") from None
    expected_fields = {
        "auth",
        "contract",
        "dataset",
        "definition",
        "expires",
        "generation",
        "issued",
        "offset",
        "profile",
        "query",
        "schema",
    }
    if (
        type(cursor_document) is not dict
        or set(cursor_document) != expected_fields
        or cursor_document.get("contract") != READ_CORE_CONTRACT
    ):
        raise CustomImportReadCursorError("custom import cursor is invalid")
    try:
        state = ReadCursorState(
            target=PinnedReadTarget(
                dataset_id=cursor_document["dataset"],
                generation_id=cursor_document["generation"],
                definition_revision_id=cursor_document["definition"],
                schema_revision_id=cursor_document["schema"],
                profile_id=cursor_document["profile"],
            ),
            query_fingerprint=cursor_document["query"],
            authorization_scope_sha256=cursor_document["auth"],
            offset=cursor_document["offset"],
            issued_at=cursor_document["issued"],
            expires_at=cursor_document["expires"],
        )
        _validate_cursor_state(state)
    except KeyError, CustomImportReadRequestError, TypeError:
        raise CustomImportReadCursorError("custom import cursor is invalid") from None
    return state


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    parsed_by_key: dict[str, object] = {}
    for key, value in pairs:
        if key in parsed_by_key:
            raise CustomImportReadCursorError("custom import cursor is invalid")
        parsed_by_key[key] = value
    return parsed_by_key


def _is_sha256_hex(value: object) -> bool:
    return type(value) is str and _MAC_HEX.fullmatch(value) is not None


__all__ = ("MAX_CURSOR_CHARACTERS", "ReadCursorCodec", "ReadCursorState")
