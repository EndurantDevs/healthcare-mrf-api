# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only keyring loading for the inactive encrypted raw-EIN vault."""

from __future__ import annotations

import base64
import json
import os
import re
import stat
from typing import Any


RAW_TIN_VAULT_KEYRING_CONTRACT = "healthporta.ptg.raw-tin-vault-keyring.v1"
RAW_TIN_VAULT_KEYRING_MAX_BYTES = 128 * 1024
RAW_TIN_VAULT_KEYRING_MAX_ENTRIES = 32

_KEY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}$")


class RawTinVaultError(ValueError):
    """Report a fail-closed vault contract error without private values."""


class _ImmutableVaultCapability:
    """Reject ordinary mutation of private cryptographic capabilities."""

    __slots__ = ()

    def __setattr__(self, _name: str, _value: object) -> None:
        raise TypeError("raw TIN vault capability is immutable")


class _InertVaultSnapshot(_ImmutableVaultCapability):
    """Redacted marker produced instead of copying private capabilities."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<raw-tin-vault-capability-redacted>"

    __str__ = __repr__

    def __copy__(self) -> "_InertVaultSnapshot":
        return self

    def __deepcopy__(self, _memo: dict[int, Any]) -> "_InertVaultSnapshot":
        return self

    def __reduce_ex__(self, _protocol: int):
        return (_InertVaultSnapshot, ())


class _VaultKeyContext(_ImmutableVaultCapability):
    """One immutable key ID and protected 32-byte master key."""

    __slots__ = ("__key_id", "__secret")

    def __init__(self, key_id: str, secret: bytes) -> None:
        object.__setattr__(self, "_VaultKeyContext__key_id", _validated_key_id(key_id))
        object.__setattr__(self, "_VaultKeyContext__secret", _validated_master_key(secret))
        self._validated_material()

    def _validated_material(self) -> tuple[str, bytes]:
        try:
            return _validated_key_id(self.__key_id), _validated_master_key(self.__secret)
        except (AttributeError, RawTinVaultError):
            raise RawTinVaultError("raw_tin_vault_key_context_invalid") from None

    @property
    def key_id(self) -> str:
        """Return the non-secret key identifier."""
        return self._validated_material()[0]

    def __repr__(self) -> str:
        return "<raw-tin-vault-key-context>"

    __str__ = __repr__


class _RawTinVaultKeyring(_ImmutableVaultCapability):
    """Opaque keyring whose active key seals and exact tagged keys open."""

    __slots__ = ("__active_key_id", "__contexts")

    def __init__(
        self,
        *,
        active_key_id: str,
        context_by_key_id: dict[str, _VaultKeyContext],
    ) -> None:
        if type(context_by_key_id) is not dict:
            raise RawTinVaultError("raw_tin_vault_keyring_invalid")
        for key_id, context in context_by_key_id.items():
            if type(context) is not _VaultKeyContext or key_id != context.key_id:
                raise RawTinVaultError("raw_tin_vault_keyring_invalid")
        object.__setattr__(self, "_RawTinVaultKeyring__active_key_id", _validated_key_id(active_key_id))
        object.__setattr__(self, "_RawTinVaultKeyring__contexts", tuple(context_by_key_id.values()))
        self._validated_context_by_key_id()

    def _validated_context_by_key_id(self) -> tuple[str, dict[str, _VaultKeyContext]]:
        try:
            active_key_id = _validated_key_id(self.__active_key_id)
            contexts = self.__contexts
        except (AttributeError, RawTinVaultError):
            raise RawTinVaultError("raw_tin_vault_keyring_state_invalid") from None
        if type(contexts) is not tuple or not 1 <= len(contexts) <= RAW_TIN_VAULT_KEYRING_MAX_ENTRIES:
            raise RawTinVaultError("raw_tin_vault_keyring_state_invalid")
        context_by_key_id: dict[str, _VaultKeyContext] = {}
        for context in contexts:
            if type(context) is not _VaultKeyContext or context.key_id in context_by_key_id:
                raise RawTinVaultError("raw_tin_vault_keyring_state_invalid")
            context_by_key_id[context.key_id] = context
        if active_key_id not in context_by_key_id:
            raise RawTinVaultError("raw_tin_vault_keyring_state_invalid")
        return active_key_id, context_by_key_id

    @property
    def active_key_id(self) -> str:
        """Return the key identifier used for new ciphertext."""
        return self._validated_context_by_key_id()[0]

    @property
    def configured_key_ids(self) -> tuple[str, ...]:
        """Return safe identifiers for all available opening keys."""
        return tuple(sorted(self._validated_context_by_key_id()[1]))

    def _active_context(self) -> _VaultKeyContext:
        active_key_id, context_by_key_id = self._validated_context_by_key_id()
        return context_by_key_id[active_key_id]

    def _context(self, key_id: str) -> _VaultKeyContext:
        context = self._validated_context_by_key_id()[1].get(key_id)
        if context is None:
            raise RawTinVaultError("raw_tin_vault_key_unavailable")
        return context

    def __repr__(self) -> str:
        return "<raw-tin-vault-keyring>"

    __str__ = __repr__

    def __copy__(self) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_InertVaultSnapshot, ())


def _validated_key_id(candidate: object) -> str:
    if type(candidate) is not str or _KEY_ID_PATTERN.fullmatch(candidate) is None:
        raise RawTinVaultError("raw_tin_vault_key_id_invalid")
    return candidate


def _validated_master_key(candidate: object) -> bytes:
    if type(candidate) is not bytes or len(candidate) != 32:
        raise RawTinVaultError("raw_tin_vault_master_key_invalid")
    return candidate


def _json_object_by_field(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document_by_field: dict[str, object] = {}
    for field_name, field_value in pairs:
        if type(field_name) is not str or field_name in document_by_field:
            raise RawTinVaultError("raw_tin_vault_keyring_invalid")
        document_by_field[field_name] = field_value
    return document_by_field


def _read_keyring_document(keyring_file: str | os.PathLike[str]) -> object:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(keyring_file, flags)
        with os.fdopen(descriptor, "rb") as keyring_stream:
            metadata = os.fstat(keyring_stream.fileno())
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o400
                or metadata.st_uid != os.geteuid()
            ):
                raise RawTinVaultError("raw_tin_vault_keyring_file_invalid")
            raw_document = keyring_stream.read(RAW_TIN_VAULT_KEYRING_MAX_BYTES + 1)
    except RawTinVaultError:
        raise
    except (OSError, TypeError, ValueError):
        raise RawTinVaultError("raw_tin_vault_keyring_file_unavailable") from None
    if not raw_document or len(raw_document) > RAW_TIN_VAULT_KEYRING_MAX_BYTES:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    try:
        return json.loads(
            raw_document.decode("utf-8"),
            object_pairs_hook=_json_object_by_field,
        )
    except (UnicodeError, json.JSONDecodeError, RawTinVaultError):
        raise RawTinVaultError("raw_tin_vault_keyring_invalid") from None


def _decoded_master_key(candidate: object) -> bytes:
    if type(candidate) is not str or len(candidate) > 128:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    try:
        encoded = candidate.encode("ascii")
        secret = base64.b64decode(encoded, altchars=b"-_", validate=True)
    except (UnicodeError, ValueError):
        raise RawTinVaultError("raw_tin_vault_keyring_invalid") from None
    if len(secret) != 32 or base64.urlsafe_b64encode(secret) != encoded:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    return secret


def load_raw_tin_vault_keyring(
    keyring_file: str | os.PathLike[str],
) -> _RawTinVaultKeyring:
    """Load one bounded, owner-only JSON keyring from an explicit path."""

    document = _read_keyring_document(keyring_file)
    if not isinstance(document, dict) or set(document) != {
        "contract",
        "active_key_id",
        "keys",
    }:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    if document["contract"] != RAW_TIN_VAULT_KEYRING_CONTRACT:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    active_key_id = _validated_key_id(document["active_key_id"])
    keys = document["keys"]
    if not isinstance(keys, dict) or not 1 <= len(keys) <= RAW_TIN_VAULT_KEYRING_MAX_ENTRIES:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    context_by_key_id: dict[str, _VaultKeyContext] = {}
    for candidate_key_id, encoded_secret in keys.items():
        key_id = _validated_key_id(candidate_key_id)
        context_by_key_id[key_id] = _VaultKeyContext(key_id, _decoded_master_key(encoded_secret))
    if active_key_id not in context_by_key_id:
        raise RawTinVaultError("raw_tin_vault_keyring_invalid")
    return _RawTinVaultKeyring(
        active_key_id=active_key_id,
        context_by_key_id=context_by_key_id,
    )


def _context_secret(context: _VaultKeyContext) -> bytes:
    return context._validated_material()[1]
