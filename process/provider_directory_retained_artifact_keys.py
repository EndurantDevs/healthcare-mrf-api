# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Tagged encryption and HMAC identities for retained private capabilities."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from process.provider_directory_retained_artifact_base import (
    RetainedArtifactError,
    _CIPHER_DOMAIN,
    _CIPHER_PREFIX,
    _HMAC_DOMAIN,
    _HMAC_PREFIX,
    _KEY_ID_PATTERN,
    require_digest,
    sha256_json,
)


MAX_PRIVATE_VALUE_BYTES = 64 * 1024
MAX_PRIVATE_CIPHERTEXT_CHARS = 96 * 1024
MAX_PRIVATE_KEY_SECRET_BYTES = 4096
MAX_PRIVATE_KEYRING_BYTES = 128 * 1024
MAX_PRIVATE_KEYRING_ENTRIES = 32
_PRIVATE_ITEM_BINDING_DOMAIN = (
    "healthporta-provider-directory-retained-private-item-binding-v1"
)


class _PrivateCapabilitySnapshot:
    """One inert marker produced by copying or serializing private state."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<retained-private-capability-redacted>"

    __str__ = __repr__

    def __copy__(self) -> "_PrivateCapabilitySnapshot":
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_PrivateCapabilitySnapshot":
        return self

    def __reduce_ex__(self, _protocol: int):
        return (_PrivateCapabilitySnapshot, ())


class _PrivateKeyContext:
    """Internal key capability whose raw secret has no public accessor."""

    __slots__ = ("__key_id", "__secret")

    def __init__(self, key_id: str, secret: str):
        object.__setattr__(self, "_PrivateKeyContext__key_id", key_id)
        object.__setattr__(self, "_PrivateKeyContext__secret", secret)

    @property
    def key_id(self) -> str:
        """Return the non-secret configured key identifier."""

        return self.__key_id

    def __repr__(self) -> str:
        return "<retained-private-key-context>"

    __str__ = __repr__

    def __copy__(self) -> _PrivateCapabilitySnapshot:
        return _PrivateCapabilitySnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _PrivateCapabilitySnapshot:
        return _PrivateCapabilitySnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_PrivateCapabilitySnapshot, ())


class _PrivateValuePurpose:
    __slots__ = ("__name",)

    def __init__(self, name: str):
        object.__setattr__(self, "_PrivateValuePurpose__name", name)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("retained_private_purpose_immutable")

    def _domain_bytes(self) -> bytes:
        return self.__name.encode("ascii") + b"\x00"


LOCATOR_PRIVATE_VALUE = _PrivateValuePurpose("locator")
VALIDATOR_PRIVATE_VALUE = _PrivateValuePurpose("validator")


class _OpenedPrivateValue:
    """Purpose-bound private plaintext capability with inert serialization."""

    __slots__ = ("__key_id", "__purpose", "__value")

    def __init__(self, value: str, key_id: str, purpose: _PrivateValuePurpose):
        object.__setattr__(self, "_OpenedPrivateValue__value", value)
        object.__setattr__(self, "_OpenedPrivateValue__key_id", key_id)
        object.__setattr__(self, "_OpenedPrivateValue__purpose", purpose)

    @property
    def key_id(self) -> str:
        """Return the non-secret identifier of the opening key."""

        return self.__key_id

    def __repr__(self) -> str:
        return "<opened-retained-private-value>"

    __str__ = __repr__

    def __copy__(self) -> _PrivateCapabilitySnapshot:
        return _PrivateCapabilitySnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _PrivateCapabilitySnapshot:
        return _PrivateCapabilitySnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_PrivateCapabilitySnapshot, ())


def _consume_private_value(
    opened: _OpenedPrivateValue,
    *,
    purpose: _PrivateValuePurpose,
) -> str:
    if (
        type(opened) is not _OpenedPrivateValue
        or opened._OpenedPrivateValue__purpose is not purpose
    ):
        raise RetainedArtifactError("retained_private_capability_mismatch")
    return opened._OpenedPrivateValue__value


def _validated_key_context(key_id: Any, secret: Any) -> _PrivateKeyContext:
    if type(key_id) is not str or not _KEY_ID_PATTERN.fullmatch(key_id):
        raise RetainedArtifactError("retained_artifact_key_id_invalid")
    if type(secret) is not str or len(secret) > MAX_PRIVATE_KEY_SECRET_BYTES:
        raise RetainedArtifactError("retained_artifact_key_missing")
    normalized_secret = secret.strip()
    try:
        secret_byte_count = len(normalized_secret.encode("utf-8"))
    except UnicodeError:
        raise RetainedArtifactError("retained_artifact_key_missing") from None
    if not 32 <= secret_byte_count <= MAX_PRIVATE_KEY_SECRET_BYTES:
        raise RetainedArtifactError("retained_artifact_key_missing")
    return _PrivateKeyContext(key_id=key_id, secret=normalized_secret)


def _primary_key_context() -> _PrivateKeyContext:
    key_id = (
        os.getenv("HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID") or ""
    ).strip()
    secret = (
        os.getenv("HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY") or ""
    ).strip()
    return _validated_key_context(key_id, secret)


def _reject_duplicate_key_ids(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    key_secret_by_id: dict[str, object] = {}
    for key_id, secret in pairs:
        if key_id in key_secret_by_id:
            raise RetainedArtifactError("retained_artifact_keyring_invalid")
        key_secret_by_id[key_id] = secret
    return key_secret_by_id


def _previous_keyring() -> dict[str, _PrivateKeyContext]:
    raw = (
        os.getenv("HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_PREVIOUS_KEYRING") or ""
    ).strip()
    if not raw:
        return {}
    if len(raw) > MAX_PRIVATE_KEYRING_BYTES:
        raise RetainedArtifactError("retained_artifact_keyring_invalid")
    try:
        raw_byte_count = len(raw.encode("utf-8"))
    except UnicodeError:
        raise RetainedArtifactError("retained_artifact_keyring_invalid") from None
    if raw_byte_count > MAX_PRIVATE_KEYRING_BYTES:
        raise RetainedArtifactError("retained_artifact_keyring_invalid")
    candidates = None
    try:
        candidates = json.loads(raw, object_pairs_hook=_reject_duplicate_key_ids)
    except (json.JSONDecodeError, RetainedArtifactError):
        candidates = None
    if candidates is None:
        raise RetainedArtifactError("retained_artifact_keyring_invalid")
    if (
        not isinstance(candidates, dict)
        or len(candidates) > MAX_PRIVATE_KEYRING_ENTRIES
    ):
        raise RetainedArtifactError("retained_artifact_keyring_invalid")
    context_by_key_id: dict[str, _PrivateKeyContext] = {}
    for key_id, secret in candidates.items():
        context = None
        try:
            context = _validated_key_context(key_id, secret)
        except RetainedArtifactError:
            context = None
        if context is None:
            raise RetainedArtifactError("retained_artifact_keyring_invalid")
        context_by_key_id[context.key_id] = context
    return context_by_key_id


def _private_keyring() -> dict[str, _PrivateKeyContext]:
    primary = _primary_key_context()
    previous = _previous_keyring()
    if primary.key_id in previous:
        raise RetainedArtifactError("retained_artifact_keyring_invalid")
    return {primary.key_id: primary, **previous}


def active_private_key_id() -> str:
    """Return the safe ID of the key used for new private values."""

    return _primary_key_context().key_id


def configured_private_key_ids() -> tuple[str, ...]:
    """Return only safe configured IDs; secrets never leave this module."""

    return tuple(sorted(_private_keyring()))


def _context_secret_bytes(context: _PrivateKeyContext) -> bytes:
    return context._PrivateKeyContext__secret.encode("utf-8")


def private_item_binding(
    *,
    campaign_id: str,
    source_item_id: str,
    endpoint_id: str,
) -> str:
    """Bind one private value to its exact campaign, item, and endpoint."""

    return sha256_json(
        {
            "domain": _PRIVATE_ITEM_BINDING_DOMAIN,
            "campaign_id": require_digest(campaign_id, "campaign_id"),
            "source_item_id": require_digest(source_item_id, "source_item_id"),
            "endpoint_id": require_digest(endpoint_id, "endpoint_id"),
        }
    )


def _private_binding_bytes(binding_sha256: str) -> bytes:
    return require_digest(
        binding_sha256,
        "private_binding_sha256",
    ).encode("ascii")


def _fernet(
    context: _PrivateKeyContext,
    purpose: _PrivateValuePurpose,
    binding_sha256: str,
) -> Fernet:
    derived = hmac.new(
        _context_secret_bytes(context),
        _CIPHER_DOMAIN
        + b"\x00"
        + purpose._domain_bytes()
        + _private_binding_bytes(binding_sha256),
        hashlib.sha256,
    )
    return Fernet(base64.urlsafe_b64encode(derived.digest()))


def _validated_private_value_bytes(value: Any) -> bytes:
    if type(value) is not str or not value or len(value) > MAX_PRIVATE_VALUE_BYTES:
        raise RetainedArtifactError("retained_private_value_invalid")
    try:
        encoded_value = value.encode("utf-8")
    except UnicodeError:
        raise RetainedArtifactError("retained_private_value_invalid") from None
    if not value.isprintable() or len(encoded_value) > MAX_PRIVATE_VALUE_BYTES:
        raise RetainedArtifactError("retained_private_value_invalid")
    return encoded_value


def seal_private_value(
    value: str,
    *,
    purpose: _PrivateValuePurpose,
    binding_sha256: str,
) -> str:
    """Encrypt a private locator or validator before persistence."""

    encoded_value = _validated_private_value_bytes(value)
    context = _primary_key_context()
    token = _fernet(context, purpose, binding_sha256).encrypt(encoded_value)
    return f"{_CIPHER_PREFIX}{context.key_id}:{token.decode('ascii')}"


def _open_private_value(
    ciphertext: Any,
    *,
    purpose: _PrivateValuePurpose,
    binding_sha256: str,
) -> _OpenedPrivateValue:
    """Decrypt a tagged capability with the exact configured key context."""

    if (
        type(ciphertext) is not str
        or len(ciphertext) > MAX_PRIVATE_CIPHERTEXT_CHARS
        or not ciphertext.startswith(_CIPHER_PREFIX)
        or not ciphertext.isascii()
    ):
        raise RetainedArtifactError("retained_private_ciphertext_invalid")
    tagged = ciphertext.removeprefix(_CIPHER_PREFIX)
    key_id, separator, token_text = tagged.partition(":")
    if (
        not separator
        or not _KEY_ID_PATTERN.fullmatch(key_id)
        or not token_text
        or len(token_text) > MAX_PRIVATE_CIPHERTEXT_CHARS
    ):
        raise RetainedArtifactError("retained_private_ciphertext_invalid")
    context = _private_keyring().get(key_id)
    if context is None:
        raise RetainedArtifactError("retained_private_key_unavailable")
    decrypted_bytes = None
    try:
        decrypted_bytes = _fernet(context, purpose, binding_sha256).decrypt(
            token_text.encode("ascii")
        )
    except (InvalidToken, ValueError):
        decrypted_bytes = None
    if decrypted_bytes is None or len(decrypted_bytes) > MAX_PRIVATE_VALUE_BYTES:
        raise RetainedArtifactError("retained_private_decryption_failed")
    plaintext = None
    try:
        plaintext = decrypted_bytes.decode("utf-8")
    except UnicodeError:
        plaintext = None
    if plaintext is None:
        raise RetainedArtifactError("retained_private_decryption_failed")
    _validated_private_value_bytes(plaintext)
    return _OpenedPrivateValue(
        value=plaintext,
        key_id=context.key_id,
        purpose=purpose,
    )


def private_identity_hmac(
    value: str,
    *,
    purpose: _PrivateValuePurpose,
    binding_sha256: str,
    key_id: str | None = None,
) -> str:
    """Return a stable keyed identity without disclosing the capability."""

    encoded_value = _validated_private_value_bytes(value)
    contexts = _private_keyring()
    selected_key_id = key_id or _primary_key_context().key_id
    context = contexts.get(selected_key_id)
    if context is None:
        raise RetainedArtifactError("retained_private_key_unavailable")
    digest = hmac.new(
        _context_secret_bytes(context),
        _HMAC_DOMAIN
        + purpose._domain_bytes()
        + _private_binding_bytes(binding_sha256)
        + b"\x00"
        + encoded_value,
        hashlib.sha256,
    ).hexdigest()
    return f"{_HMAC_PREFIX}{context.key_id}:{digest}"


def _is_private_identity_valid(
    opened: _OpenedPrivateValue,
    identity: Any,
    *,
    purpose: _PrivateValuePurpose,
    binding_sha256: str,
) -> bool:
    """Return whether an HMAC identity matches its opened private value."""

    if not isinstance(identity, str):
        return False
    expected = private_identity_hmac(
        _consume_private_value(opened, purpose=purpose),
        purpose=purpose,
        binding_sha256=binding_sha256,
        key_id=opened.key_id,
    )
    return hmac.compare_digest(expected, identity)
