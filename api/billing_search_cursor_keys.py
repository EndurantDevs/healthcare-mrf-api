# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded AES-256 keyring configuration for billing-search cursors."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
import hmac
import json
import re

from api.billing_search_cursor import BillingSearchCursorKeyring

BILLING_SEARCH_CURSOR_KEYRING_CONTRACT = "healthporta.billing-search-cursor-keyring.v1"
BILLING_SEARCH_CURSOR_KEYRING_ENV = "HLTHPRT_BILLING_SEARCH_CURSOR_KEYRING_JSON"
BILLING_SEARCH_CURSOR_MAX_KEYRING_BYTES = 8192

_INVALID = "billing_search_cursor_keyring_invalid"
_INVALID_JSON = object()
_KEY_BYTES = 32
_MAX_KEYS = 8
_KEY_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_BASE64URL_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_DOCUMENT_FIELDS = frozenset({"active_key_id", "contract", "keys"})
_KEY_FIELDS = frozenset({"key_base64url", "key_id"})


class BillingSearchCursorKeyringError(RuntimeError):
    """Value-free cursor-key configuration failure."""


def _fail() -> BillingSearchCursorKeyringError:
    return BillingSearchCursorKeyringError(_INVALID)


def _unique_json_object(
    member_pairs: list[tuple[str, object]],
) -> dict[str, object]:
    json_object_by_name: dict[str, object] = {}
    for member_name, member_value in member_pairs:
        if member_name in json_object_by_name:
            raise ValueError
        json_object_by_name[member_name] = member_value
    return json_object_by_name


def _reject_json_number(_encoded_number: str) -> None:
    raise ValueError


def _parse_json_bytes(encoded_json: bytes) -> object:
    try:
        return json.loads(
            encoded_json.decode("ascii"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_number,
            parse_float=_reject_json_number,
            parse_int=_reject_json_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return _INVALID_JSON


def _decoded_base64url(encoded_text: object) -> bytes | None:
    if (
        type(encoded_text) is not str
        or not encoded_text
        or _BASE64URL_PATTERN.fullmatch(encoded_text) is None
    ):
        return None
    try:
        decoded_bytes = base64.b64decode(
            encoded_text + "=" * (-len(encoded_text) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError):
        return None
    canonical_text = (
        base64.urlsafe_b64encode(decoded_bytes).rstrip(b"=").decode("ascii")
    )
    return decoded_bytes if hmac.compare_digest(canonical_text, encoded_text) else None


def _ascii_document(raw_document: object) -> bytes | None:
    if type(raw_document) is not str:
        return None
    try:
        return raw_document.encode("ascii")
    except UnicodeEncodeError:
        return None


def _keyring_from_environment(
    environment_map: Mapping[str, str],
) -> BillingSearchCursorKeyring | None:
    try:
        if not isinstance(environment_map, Mapping):
            raise _fail()
        document_bytes = _ascii_document(
            environment_map.get(BILLING_SEARCH_CURSOR_KEYRING_ENV)
        )
        if (
            document_bytes is None
            or not 1 <= len(document_bytes) <= BILLING_SEARCH_CURSOR_MAX_KEYRING_BYTES
        ):
            raise _fail()
        document = _parse_json_bytes(document_bytes)
        if (
            type(document) is not dict
            or frozenset(document) != _DOCUMENT_FIELDS
            or document.get("contract") != BILLING_SEARCH_CURSOR_KEYRING_CONTRACT
            or type(document.get("keys")) is not list
            or not 1 <= len(document["keys"]) <= _MAX_KEYS
        ):
            raise _fail()
        keys_by_id: dict[str, bytes] = {}
        for key_entry in document["keys"]:
            if type(key_entry) is not dict or frozenset(key_entry) != _KEY_FIELDS:
                raise _fail()
            key_id = key_entry.get("key_id")
            decoded_key = _decoded_base64url(key_entry.get("key_base64url"))
            if (
                type(key_id) is not str
                or _KEY_ID_PATTERN.fullmatch(key_id) is None
                or decoded_key is None
                or len(decoded_key) != _KEY_BYTES
                or key_id in keys_by_id
            ):
                raise _fail()
            keys_by_id[key_id] = decoded_key
        return BillingSearchCursorKeyring(
            active_key_id=document.get("active_key_id"),
            keys_by_id=keys_by_id,
        )
    except Exception:
        return None


def load_billing_search_cursor_keyring(
    environment_map: Mapping[str, str],
) -> BillingSearchCursorKeyring:
    """Load one closed cursor keyring from the healthcare environment."""

    keyring = _keyring_from_environment(environment_map)
    if keyring is None:
        raise _fail()
    return keyring


__all__ = [
    "BILLING_SEARCH_CURSOR_KEYRING_CONTRACT",
    "BILLING_SEARCH_CURSOR_KEYRING_ENV",
    "BILLING_SEARCH_CURSOR_MAX_KEYRING_BYTES",
    "BillingSearchCursorKeyringError",
    "load_billing_search_cursor_keyring",
]
