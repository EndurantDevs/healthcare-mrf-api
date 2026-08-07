# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Confidential keyset cursors for billing-identity pricing search."""

from __future__ import annotations

import base64
import binascii
import hmac
import json
import math
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from api.billing_search_sealed_cursor import (
    BillingSearchSealedPageCursor,
    _mint_billing_search_sealed_page_cursor,
)

BILLING_SEARCH_CURSOR_CONTRACT = "healthporta.billing-search-cursor.v1"
BILLING_SEARCH_CURSOR_PREFIX = "bsc1"
BILLING_SEARCH_CURSOR_MAX_CHARACTERS = 2048
BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS = 900

_AAD_DOMAIN = b"HEALTHPORTA_BILLING_SEARCH_CURSOR_V1\x00"
_NONCE_BYTES = 12
_KEY_BYTES = 32
_INVALID = "billing_search_cursor_invalid"
_REDACTED = "<redacted-billing-search-cursor>"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}", flags=re.ASCII)
_KEY_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_TOKEN_PAYLOAD_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_INVALID_JSON = object()
_STATE_FIELDS = frozenset(
    {
        "authorization_context_sha256",
        "contract",
        "expires_at",
        "generation_bundle_sha256",
        "issued_at",
        "request_fingerprint_sha256",
        "snapshot_set_sha256",
        "sort_key",
    }
)


class BillingSearchCursorError(ValueError):
    """Value-free cursor failure safe for an API boundary."""


class BillingSearchCursorGenerationExpired(BillingSearchCursorError):
    """The cursor's immutable serving generation is no longer current."""


def _fail() -> BillingSearchCursorError:
    return BillingSearchCursorError(_INVALID)


def _generation_expired() -> BillingSearchCursorGenerationExpired:
    return BillingSearchCursorGenerationExpired(
        "billing_search_cursor_generation_expired"
    )


def _canonical_sha256(value: object) -> str:
    if (
        type(value) is not str
        or _SHA256_PATTERN.fullmatch(value) is None
        or value == "0" * 64
    ):
        raise _fail()
    return value


def _canonical_key_id(value: object) -> str:
    if type(value) is not str or _KEY_ID_PATTERN.fullmatch(value) is None:
        raise _fail()
    return value


def _canonical_key(value: object) -> bytes:
    if type(value) is not bytes or len(value) != _KEY_BYTES:
        raise _fail()
    return value


def _canonical_timestamp(value: object) -> int:
    if type(value) is not int or not 0 <= value < 2**63:
        raise _fail()
    return value


def _canonical_sort_value(value: object) -> int | float | str:
    if type(value) is int and -(2**63) <= value < 2**63:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if (
        type(value) is str
        and 1 <= len(value) <= 256
        and value.isascii()
        and value.isprintable()
    ):
        return value
    raise _fail()


def _canonical_sort_key(value: object) -> tuple[int | float | str, ...]:
    if type(value) not in {tuple, list} or not 1 <= len(value) <= 16:
        raise _fail()
    return tuple(_canonical_sort_value(member) for member in value)


class BillingSearchCursorKeyring:
    """Immutable active-and-retained AES-256 keyring."""

    __slots__ = ("__active_key_id", "__keys")

    def __init__(
        self,
        *,
        active_key_id: str,
        keys_by_id: Mapping[str, bytes],
    ) -> None:
        normalized_active_id = _canonical_key_id(active_key_id)
        if type(keys_by_id) is not dict or not 1 <= len(keys_by_id) <= 8:
            raise _fail()
        normalized_keys = tuple(
            sorted(
                (
                    _canonical_key_id(key_id),
                    _canonical_key(key_value),
                )
                for key_id, key_value in keys_by_id.items()
            )
        )
        if len({key_id for key_id, _key in normalized_keys}) != len(
            normalized_keys
        ) or len({key for _key_id, key in normalized_keys}) != len(normalized_keys):
            raise _fail()
        if normalized_active_id not in {key_id for key_id, _key in normalized_keys}:
            raise _fail()
        object.__setattr__(
            self, "_BillingSearchCursorKeyring__active_key_id", normalized_active_id
        )
        object.__setattr__(self, "_BillingSearchCursorKeyring__keys", normalized_keys)

    def __setattr__(self, attribute_name: str, attribute_value: object) -> None:
        del attribute_name, attribute_value
        raise TypeError(_INVALID)

    def __delattr__(self, attribute_name: str) -> None:
        del attribute_name
        raise TypeError(_INVALID)

    @property
    def active_key_id(self) -> str:
        """Return the non-secret active key version."""

        return self.__active_key_id

    def key_for(self, key_id: object) -> bytes:
        """Return one retained key or fail without revealing key availability."""

        normalized_key_id = _canonical_key_id(key_id)
        for retained_key_id, retained_key in self.__keys:
            if hmac.compare_digest(retained_key_id, normalized_key_id):
                return retained_key
        raise _fail()

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __copy__(self) -> BillingSearchCursorKeyring:
        return self

    def __deepcopy__(
        self,
        memo: dict[int, object],
    ) -> BillingSearchCursorKeyring:
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        raise _fail()


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchCursorState:
    """Complete generation-bound position retained only inside ciphertext."""

    request_fingerprint_sha256: str
    authorization_context_sha256: str
    generation_bundle_sha256: str
    snapshot_set_sha256: str
    sort_key: tuple[int | float | str, ...]
    issued_at: int
    expires_at: int
    contract: str = BILLING_SEARCH_CURSOR_CONTRACT

    def __post_init__(self) -> None:
        _validated_state(self)

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__


def _state_values(state: BillingSearchCursorState) -> dict[str, Any]:
    if type(state.sort_key) is not tuple:
        raise _fail()
    return {
        "authorization_context_sha256": _canonical_sha256(
            state.authorization_context_sha256
        ),
        "contract": state.contract,
        "expires_at": _canonical_timestamp(state.expires_at),
        "generation_bundle_sha256": _canonical_sha256(state.generation_bundle_sha256),
        "issued_at": _canonical_timestamp(state.issued_at),
        "request_fingerprint_sha256": _canonical_sha256(
            state.request_fingerprint_sha256
        ),
        "snapshot_set_sha256": _canonical_sha256(state.snapshot_set_sha256),
        "sort_key": list(_canonical_sort_key(state.sort_key)),
    }


def _validated_state(state: object) -> dict[str, Any]:
    if type(state) is not BillingSearchCursorState:
        raise _fail()
    values = _state_values(state)
    if (
        type(state.contract) is not str
        or state.contract != BILLING_SEARCH_CURSOR_CONTRACT
        or values["expires_at"] <= values["issued_at"]
        or values["expires_at"] - values["issued_at"]
        > BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS
    ):
        raise _fail()
    return values


def _aad(key_id: str) -> bytes:
    return _AAD_DOMAIN + key_id.encode("ascii")


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _base64url_decode(value: object) -> bytes:
    if (
        type(value) is not str
        or not value
        or _TOKEN_PAYLOAD_PATTERN.fullmatch(value) is None
    ):
        raise _fail()
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise _fail() from exc
    if not hmac.compare_digest(_base64url_encode(decoded), value):
        raise _fail()
    return decoded


def _new_sealed_page_cursor(
    token: object,
    sort_key: object,
) -> BillingSearchSealedPageCursor:
    if (
        type(token) is not str
        or not 1 <= len(token) <= BILLING_SEARCH_CURSOR_MAX_CHARACTERS
    ):
        raise _fail()
    token_parts = token.split("_", 2)
    if len(token_parts) != 3 or token_parts[0] != BILLING_SEARCH_CURSOR_PREFIX:
        raise _fail()
    _canonical_key_id(token_parts[1])
    sealed_payload = _base64url_decode(token_parts[2])
    if len(sealed_payload) <= _NONCE_BYTES + 16:
        raise _fail()
    return _mint_billing_search_sealed_page_cursor(
        token,
        _canonical_sort_key(sort_key),
    )


def _canonical_json_bytes(json_object: object) -> bytes:
    return json.dumps(
        json_object,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _unique_json_object(
    member_pairs: list[tuple[str, object]],
) -> dict[str, object]:
    json_object_by_name: dict[str, object] = {}
    for member_name, member_value in member_pairs:
        if member_name in json_object_by_name:
            raise ValueError
        json_object_by_name[member_name] = member_value
    return json_object_by_name


def _reject_json_constant(_constant: str) -> None:
    raise ValueError


def _finite_json_float(encoded_number: str) -> float:
    decoded_number = float(encoded_number)
    if not math.isfinite(decoded_number):
        raise ValueError
    return decoded_number


def _parse_authenticated_json(plaintext: bytes) -> object:
    try:
        return json.loads(
            plaintext.decode("ascii"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
            parse_float=_finite_json_float,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return _INVALID_JSON


def seal_billing_search_cursor(
    state: BillingSearchCursorState,
    *,
    keyring: BillingSearchCursorKeyring,
    trusted_now: int,
) -> str:
    """Encrypt and authenticate one bounded keyset cursor."""

    state_fields = _validated_state(state)
    now = _canonical_timestamp(trusted_now)
    if not state_fields["issued_at"] <= now < state_fields["expires_at"]:
        raise _fail()
    key_id = keyring.active_key_id
    plaintext = _canonical_json_bytes(state_fields)
    nonce = secrets.token_bytes(_NONCE_BYTES)
    if type(nonce) is not bytes or len(nonce) != _NONCE_BYTES:
        raise _fail()
    ciphertext = AESGCM(keyring.key_for(key_id)).encrypt(
        nonce,
        plaintext,
        _aad(key_id),
    )
    token = f"{BILLING_SEARCH_CURSOR_PREFIX}_{key_id}_{_base64url_encode(nonce + ciphertext)}"
    if len(token) > BILLING_SEARCH_CURSOR_MAX_CHARACTERS:
        raise _fail()
    return token


def _decoded_state(plaintext: bytes) -> BillingSearchCursorState:
    raw_state = _parse_authenticated_json(plaintext)
    if (
        type(raw_state) is not dict
        or frozenset(raw_state) != _STATE_FIELDS
        or not hmac.compare_digest(_canonical_json_bytes(raw_state), plaintext)
    ):
        raise _fail()
    return BillingSearchCursorState(
        request_fingerprint_sha256=raw_state["request_fingerprint_sha256"],
        authorization_context_sha256=raw_state["authorization_context_sha256"],
        generation_bundle_sha256=raw_state["generation_bundle_sha256"],
        snapshot_set_sha256=raw_state["snapshot_set_sha256"],
        sort_key=_canonical_sort_key(raw_state["sort_key"]),
        issued_at=raw_state["issued_at"],
        expires_at=raw_state["expires_at"],
        contract=raw_state["contract"],
    )


def open_billing_search_cursor(
    token: object,
    *,
    keyring: BillingSearchCursorKeyring,
    trusted_now: int,
    request_fingerprint_sha256: str,
    authorization_context_sha256: str,
    generation_bundle_sha256: str,
    snapshot_set_sha256: str,
) -> BillingSearchCursorState:
    """Open one cursor and bind it to request, authority, and generations."""

    if (
        type(token) is not str
        or not 1 <= len(token) <= BILLING_SEARCH_CURSOR_MAX_CHARACTERS
    ):
        raise _fail()
    token_parts = token.split("_", 2)
    if len(token_parts) != 3 or token_parts[0] != BILLING_SEARCH_CURSOR_PREFIX:
        raise _fail()
    key_id = _canonical_key_id(token_parts[1])
    sealed_payload = _base64url_decode(token_parts[2])
    if len(sealed_payload) <= _NONCE_BYTES + 16:
        raise _fail()
    try:
        plaintext = AESGCM(keyring.key_for(key_id)).decrypt(
            sealed_payload[:_NONCE_BYTES],
            sealed_payload[_NONCE_BYTES:],
            _aad(key_id),
        )
    except (InvalidTag, ValueError) as exc:
        raise _fail() from exc
    state = _decoded_state(plaintext)
    now = _canonical_timestamp(trusted_now)
    expected_scope_digests = (
        (state.request_fingerprint_sha256, request_fingerprint_sha256),
        (state.authorization_context_sha256, authorization_context_sha256),
    )
    expected_generation_digests = (
        (state.generation_bundle_sha256, generation_bundle_sha256),
        (state.snapshot_set_sha256, snapshot_set_sha256),
    )
    if not state.issued_at <= now < state.expires_at or any(
        not hmac.compare_digest(actual, _canonical_sha256(expected))
        for actual, expected in expected_scope_digests
    ):
        raise _fail()
    if any(
        not hmac.compare_digest(actual, _canonical_sha256(expected))
        for actual, expected in expected_generation_digests
    ):
        raise _generation_expired()
    return state


__all__ = [
    "BILLING_SEARCH_CURSOR_CONTRACT",
    "BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS",
    "BillingSearchCursorError",
    "BillingSearchCursorGenerationExpired",
    "BillingSearchCursorKeyring",
    "BillingSearchSealedPageCursor",
    "BillingSearchCursorState",
    "open_billing_search_cursor",
    "seal_billing_search_cursor",
]
