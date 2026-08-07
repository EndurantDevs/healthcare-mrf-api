# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded keyring configuration for signed billing-search transport."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
import hmac
import json
import re

BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT = (
    "healthporta.billing-search-transport-keyring.v1"
)
BILLING_SEARCH_TRANSPORT_KEYRING_ENV = "HLTHPRT_BILLING_SEARCH_TRANSPORT_KEYRING_JSON"
BILLING_SEARCH_TRANSPORT_MAX_KEYRING_BYTES = 4096

_INVALID = "billing_search_transport_keyring_invalid"
_REDACTED = "<redacted-billing-search-transport-keyring>"
_INVALID_JSON = object()
_KEY_BYTES = 32
_MAX_KEYS = 4
_KEY_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}", flags=re.ASCII)
_BASE64URL_PATTERN = re.compile(r"[A-Za-z0-9_-]+", flags=re.ASCII)
_DOCUMENT_FIELDS = frozenset({"active_key_id", "contract", "keys"})
_KEY_FIELDS = frozenset({"key_base64url", "key_id"})


class BillingSearchTransportKeyringError(RuntimeError):
    """Value-free signing-key configuration failure."""


def _fail() -> BillingSearchTransportKeyringError:
    return BillingSearchTransportKeyringError(_INVALID)


def _canonical_key_id(value: object) -> str:
    if type(value) is not str or _KEY_ID_PATTERN.fullmatch(value) is None:
        raise _fail()
    return value


def _canonical_key(value: object) -> bytes:
    if type(value) is not bytes or len(value) != _KEY_BYTES:
        raise _fail()
    return value


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


class BillingSearchTransportKeyring:
    """Immutable active-and-retained HMAC-SHA-256 keyring."""

    __slots__ = ("__active_key_id", "__keys")

    def __init__(
        self,
        *,
        active_key_id: str,
        keys_by_id: Mapping[str, bytes],
    ) -> None:
        normalized_active_id = _canonical_key_id(active_key_id)
        if type(keys_by_id) is not dict or not 1 <= len(keys_by_id) <= _MAX_KEYS:
            raise _fail()
        normalized_keys = tuple(
            sorted(
                (
                    _canonical_key_id(key_id),
                    _canonical_key(key_material),
                )
                for key_id, key_material in keys_by_id.items()
            )
        )
        if len({key_id for key_id, _key in normalized_keys}) != len(
            normalized_keys
        ) or len({key for _key_id, key in normalized_keys}) != len(normalized_keys):
            raise _fail()
        if normalized_active_id not in {key_id for key_id, _key in normalized_keys}:
            raise _fail()
        object.__setattr__(
            self,
            "_BillingSearchTransportKeyring__active_key_id",
            normalized_active_id,
        )
        object.__setattr__(
            self,
            "_BillingSearchTransportKeyring__keys",
            normalized_keys,
        )

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
        """Return one retained key without disclosing key availability."""

        normalized_key_id = _canonical_key_id(key_id)
        for retained_key_id, retained_key in self.__keys:
            if hmac.compare_digest(retained_key_id, normalized_key_id):
                return retained_key
        raise _fail()

    def __repr__(self) -> str:
        return _REDACTED

    __str__ = __repr__

    def __copy__(self) -> BillingSearchTransportKeyring:
        return self

    def __deepcopy__(
        self,
        memo: dict[int, object],
    ) -> BillingSearchTransportKeyring:
        memo[id(self)] = self
        return self

    def __reduce_ex__(self, protocol: int) -> object:
        del protocol
        raise _fail()


def _ascii_document(raw_document: object) -> bytes | None:
    if type(raw_document) is not str:
        return None
    try:
        return raw_document.encode("ascii")
    except UnicodeEncodeError:
        return None


def _keyring_from_environment(
    environment_map: Mapping[str, str],
) -> BillingSearchTransportKeyring | None:
    try:
        if not isinstance(environment_map, Mapping):
            raise _fail()
        document_bytes = _ascii_document(
            environment_map.get(BILLING_SEARCH_TRANSPORT_KEYRING_ENV)
        )
        if (
            document_bytes is None
            or not 1
            <= len(document_bytes)
            <= BILLING_SEARCH_TRANSPORT_MAX_KEYRING_BYTES
        ):
            raise _fail()
        document = _parse_json_bytes(document_bytes)
        if (
            type(document) is not dict
            or frozenset(document) != _DOCUMENT_FIELDS
            or document.get("contract") != BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT
            or type(document.get("keys")) is not list
            or not 1 <= len(document["keys"]) <= _MAX_KEYS
        ):
            raise _fail()
        key_material_by_id: dict[str, bytes] = {}
        for key_entry in document["keys"]:
            if type(key_entry) is not dict or frozenset(key_entry) != _KEY_FIELDS:
                raise _fail()
            key_id = _canonical_key_id(key_entry.get("key_id"))
            decoded_key = _decoded_base64url(key_entry.get("key_base64url"))
            if decoded_key is None or len(decoded_key) != _KEY_BYTES:
                raise _fail()
            if key_id in key_material_by_id:
                raise _fail()
            key_material_by_id[key_id] = decoded_key
        return BillingSearchTransportKeyring(
            active_key_id=document.get("active_key_id"),
            keys_by_id=key_material_by_id,
        )
    except Exception:
        return None


def load_billing_search_transport_keyring(
    environment_map: Mapping[str, str],
) -> BillingSearchTransportKeyring:
    """Load one closed keyring document from the healthcare environment."""

    keyring = _keyring_from_environment(environment_map)
    if keyring is None:
        raise _fail()
    return keyring


__all__ = [
    "BILLING_SEARCH_TRANSPORT_KEYRING_CONTRACT",
    "BILLING_SEARCH_TRANSPORT_KEYRING_ENV",
    "BillingSearchTransportKeyring",
    "BillingSearchTransportKeyringError",
    "load_billing_search_transport_keyring",
]
