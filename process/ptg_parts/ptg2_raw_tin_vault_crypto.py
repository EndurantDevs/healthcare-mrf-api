# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Purpose-bound encryption for inactive raw-EIN vault entries.

Callers must hold a TIN token-policy capability and a separate vault keyring.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import struct
from dataclasses import dataclass
from typing import Any
from cryptography.fernet import Fernet, InvalidToken

from process.ptg_parts.ptg2_raw_tin_vault_keyring import (
    RAW_TIN_VAULT_KEYRING_CONTRACT,
    RawTinVaultError,
    _ImmutableVaultCapability,
    _InertVaultSnapshot,
    _KEY_ID_PATTERN,
    _RawTinVaultKeyring,
    _VaultKeyContext,
    _context_secret,
    _validated_key_id,
    load_raw_tin_vault_keyring,
)
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    TinTokenProjector,
    canonical_token_policy_id,
    normalize_ein,
    token_policy_descriptor_sha256,
)


RAW_TIN_VAULT_ENCRYPTION_CONTRACT = "fernet_hmac_sha256_bound_v1"
RAW_TIN_VAULT_BINDING_CONTRACT = "token_policy_full_hmac_ein_v1"
RAW_TIN_VAULT_CIPHERTEXT_PREFIX = "hptinv1:"
RAW_TIN_VAULT_CIPHERTEXT_MAX_CHARS = 256

_FERNET_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]+={0,2}$")
_BINDING_DOMAIN = b"healthporta.ptg.raw-tin-vault.binding.v1\x00"
_CIPHER_DOMAIN = b"healthporta.ptg.raw-tin-vault.cipher.v1\x00"


@dataclass(frozen=True, repr=False)
class SealedRawTinVaultEntry:
    """Ciphertext and authoritative policy-scoped identity for one EIN."""

    token_policy_id: str
    token_policy_descriptor_sha256: bytes
    tin_hmac_sha256: bytes
    encryption_key_id: str
    ciphertext: str

    def __post_init__(self) -> None:
        _validate_sealed_entry(self)

    @property
    def encryption_contract(self) -> str:
        """Return the fixed encryption contract stored with the row."""
        return RAW_TIN_VAULT_ENCRYPTION_CONTRACT

    @property
    def binding_contract(self) -> str:
        """Return the fixed identity-binding contract stored with the row."""
        return RAW_TIN_VAULT_BINDING_CONTRACT

    @property
    def tin_type(self) -> str:
        """Return the only raw tax-identifier type accepted by release 1."""
        return "ein"

    def __repr__(self) -> str:
        return "<sealed-raw-tin-vault-entry ciphertext=<redacted>>"

    __str__ = __repr__

    def __copy__(self) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_InertVaultSnapshot, ())


class _OpenedRawTinVaultEntry(_ImmutableVaultCapability):
    """Purpose-bound plaintext capability with inert serialization."""

    __slots__ = ("__normalized_ein", "__tin_hmac_sha256", "__token_policy_id")

    def __init__(
        self,
        normalized_ein: str,
        token_policy_id: str,
        tin_hmac_sha256: bytes,
    ) -> None:
        object.__setattr__(
            self,
            "_OpenedRawTinVaultEntry__normalized_ein",
            _validated_normalized_ein(normalized_ein),
        )
        object.__setattr__(
            self,
            "_OpenedRawTinVaultEntry__token_policy_id",
            canonical_token_policy_id(token_policy_id),
        )
        if type(tin_hmac_sha256) is not bytes or len(tin_hmac_sha256) != 32:
            raise RawTinVaultError("raw_tin_vault_capability_state_invalid")
        object.__setattr__(
            self,
            "_OpenedRawTinVaultEntry__tin_hmac_sha256",
            tin_hmac_sha256,
        )

    def __repr__(self) -> str:
        return "<opened-raw-tin-vault-entry value=<redacted>>"

    __str__ = __repr__

    def __copy__(self) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __deepcopy__(self, _memo: dict[int, Any]) -> _InertVaultSnapshot:
        return _InertVaultSnapshot()

    def __reduce_ex__(self, _protocol: int):
        return (_InertVaultSnapshot, ())


def _consume_opened_ein(
    opened: _OpenedRawTinVaultEntry,
    token_projector: TinTokenProjector,
) -> str:
    if type(opened) is not _OpenedRawTinVaultEntry:
        raise RawTinVaultError("raw_tin_vault_capability_mismatch")
    try:
        normalized_ein = _validated_normalized_ein(
            opened._OpenedRawTinVaultEntry__normalized_ein
        )
        token_policy_id = canonical_token_policy_id(
            opened._OpenedRawTinVaultEntry__token_policy_id
        )
        tin_hmac_sha256 = opened._OpenedRawTinVaultEntry__tin_hmac_sha256
        _, token = _tokenized_ein(token_projector, normalized_ein)
    except (AttributeError, RawTinVaultError, ValueError):
        raise RawTinVaultError("raw_tin_vault_capability_state_invalid") from None
    if (
        type(tin_hmac_sha256) is not bytes
        or len(tin_hmac_sha256) != 32
        or token.token_policy_id != token_policy_id
        or not token.has_matching_full_hmac(tin_hmac_sha256)
    ):
        raise RawTinVaultError("raw_tin_vault_capability_state_invalid")
    return normalized_ein


def _validated_normalized_ein(candidate: object) -> str:
    try:
        normalized_ein = normalize_ein(candidate)
    except ValueError:
        raise RawTinVaultError("raw_tin_vault_ein_invalid") from None
    if type(candidate) is not str or normalized_ein != candidate:
        raise RawTinVaultError("raw_tin_vault_ein_invalid")
    return normalized_ein


def _frame(field_value: bytes) -> bytes:
    return struct.pack(">I", len(field_value)) + field_value


def _binding_bytes(
    *,
    token_policy_id: str,
    token_policy_descriptor: bytes,
    tin_hmac_sha256: bytes,
) -> bytes:
    return b"".join(
        (
            _BINDING_DOMAIN,
            _frame(RAW_TIN_VAULT_BINDING_CONTRACT.encode("ascii")),
            _frame(token_policy_id.encode("ascii")),
            _frame(token_policy_descriptor),
            _frame(b"ein"),
            _frame(tin_hmac_sha256),
        )
    )


def _fernet(
    context: _VaultKeyContext,
    *,
    token_policy_id: str,
    token_policy_descriptor: bytes,
    tin_hmac_sha256: bytes,
) -> Fernet:
    derived_key = hmac.new(
        _context_secret(context),
        _CIPHER_DOMAIN
        + _frame(context.key_id.encode("ascii"))
        + _binding_bytes(
            token_policy_id=token_policy_id,
            token_policy_descriptor=token_policy_descriptor,
            tin_hmac_sha256=tin_hmac_sha256,
        ),
        hashlib.sha256,
    ).digest()
    return Fernet(base64.urlsafe_b64encode(derived_key))


def _descriptor_bytes(token_policy_id: str) -> bytes:
    return bytes.fromhex(token_policy_descriptor_sha256(token_policy_id))


def _tokenized_ein(
    token_projector: TinTokenProjector,
    candidate: object,
) -> tuple[str, TinTaxIdentityToken]:
    normalized_ein = normalize_ein(candidate)
    try:
        token_policy_id = canonical_token_policy_id(token_projector.token_policy_id)
        token = token_projector.tokenize_ein(normalized_ein)
    except (AttributeError, ValueError):
        raise RawTinVaultError("raw_tin_vault_token_policy_invalid") from None
    if type(token) is not TinTaxIdentityToken or token.token_policy_id != token_policy_id:
        raise RawTinVaultError("raw_tin_vault_token_policy_invalid")
    return normalized_ein, token


def _parse_ciphertext(ciphertext: object) -> tuple[str, str]:
    if (
        type(ciphertext) is not str
        or not ciphertext.isascii()
        or len(ciphertext) > RAW_TIN_VAULT_CIPHERTEXT_MAX_CHARS
        or not ciphertext.startswith(RAW_TIN_VAULT_CIPHERTEXT_PREFIX)
    ):
        raise RawTinVaultError("raw_tin_vault_ciphertext_invalid")
    tagged = ciphertext.removeprefix(RAW_TIN_VAULT_CIPHERTEXT_PREFIX)
    key_id, separator, token_text = tagged.partition(":")
    if (
        not separator
        or _KEY_ID_PATTERN.fullmatch(key_id) is None
        or _FERNET_TOKEN_PATTERN.fullmatch(token_text) is None
    ):
        raise RawTinVaultError("raw_tin_vault_ciphertext_invalid")
    return key_id, token_text


def _validate_sealed_entry(entry: SealedRawTinVaultEntry) -> None:
    try:
        policy_id = canonical_token_policy_id(entry.token_policy_id)
    except ValueError:
        raise RawTinVaultError("raw_tin_vault_identity_invalid") from None
    expected_descriptor = _descriptor_bytes(policy_id)
    if (
        type(entry.token_policy_descriptor_sha256) is not bytes
        or not hmac.compare_digest(
            entry.token_policy_descriptor_sha256,
            expected_descriptor,
        )
        or type(entry.tin_hmac_sha256) is not bytes
        or len(entry.tin_hmac_sha256) != 32
    ):
        raise RawTinVaultError("raw_tin_vault_identity_invalid")
    key_id = _validated_key_id(entry.encryption_key_id)
    tagged_key_id, _ = _parse_ciphertext(entry.ciphertext)
    if tagged_key_id != key_id:
        raise RawTinVaultError("raw_tin_vault_ciphertext_invalid")


def _seal_normalized_ein(
    keyring: _RawTinVaultKeyring,
    token_projector: TinTokenProjector,
    normalized_ein: str,
) -> SealedRawTinVaultEntry:
    normalized, token = _tokenized_ein(token_projector, normalized_ein)
    descriptor = _descriptor_bytes(token.token_policy_id)
    context = keyring._active_context()
    encrypted = _fernet(
        context,
        token_policy_id=token.token_policy_id,
        token_policy_descriptor=descriptor,
        tin_hmac_sha256=token.tin_hmac_sha256,
    ).encrypt(normalized.encode("ascii"))
    ciphertext = (
        f"{RAW_TIN_VAULT_CIPHERTEXT_PREFIX}{context.key_id}:"
        f"{encrypted.decode('ascii')}"
    )
    return SealedRawTinVaultEntry(
        token_policy_id=token.token_policy_id,
        token_policy_descriptor_sha256=descriptor,
        tin_hmac_sha256=token.tin_hmac_sha256,
        encryption_key_id=context.key_id,
        ciphertext=ciphertext,
    )


def seal_ein(
    keyring: _RawTinVaultKeyring,
    token_projector: TinTokenProjector,
    candidate: object,
) -> SealedRawTinVaultEntry:
    """Normalize, tokenize, and encrypt one EIN without retaining plaintext."""

    normalized_ein = normalize_ein(candidate)
    return _seal_normalized_ein(keyring, token_projector, normalized_ein)


def open_ein(
    keyring: _RawTinVaultKeyring,
    token_projector: TinTokenProjector,
    entry: SealedRawTinVaultEntry,
) -> _OpenedRawTinVaultEntry:
    """Open one exact identity-bound entry and revalidate its full HMAC."""

    if type(entry) is not SealedRawTinVaultEntry:
        raise RawTinVaultError("raw_tin_vault_entry_invalid")
    _validate_sealed_entry(entry)
    tagged_key_id, token_text = _parse_ciphertext(entry.ciphertext)
    context = keyring._context(tagged_key_id)
    try:
        plaintext_bytes = _fernet(
            context,
            token_policy_id=entry.token_policy_id,
            token_policy_descriptor=entry.token_policy_descriptor_sha256,
            tin_hmac_sha256=entry.tin_hmac_sha256,
        ).decrypt(token_text.encode("ascii"))
        plaintext = plaintext_bytes.decode("ascii")
        normalized_ein, token = _tokenized_ein(token_projector, plaintext)
    except (InvalidToken, UnicodeError, ValueError):
        raise RawTinVaultError("raw_tin_vault_decryption_failed") from None
    if (
        token.token_policy_id != entry.token_policy_id
        or not token.has_matching_full_hmac(entry.tin_hmac_sha256)
    ):
        raise RawTinVaultError("raw_tin_vault_identity_mismatch")
    return _OpenedRawTinVaultEntry(
        normalized_ein,
        entry.token_policy_id,
        entry.tin_hmac_sha256,
    )


def rewrap_ein(
    keyring: _RawTinVaultKeyring,
    token_projector: TinTokenProjector,
    entry: SealedRawTinVaultEntry,
) -> SealedRawTinVaultEntry:
    """Re-encrypt a verified entry with the active vault key."""

    opened = open_ein(keyring, token_projector, entry)
    normalized_ein = _consume_opened_ein(opened, token_projector)
    return _seal_normalized_ein(keyring, token_projector, normalized_ein)


__all__ = [
    "RAW_TIN_VAULT_BINDING_CONTRACT", "RAW_TIN_VAULT_CIPHERTEXT_PREFIX",
    "RAW_TIN_VAULT_ENCRYPTION_CONTRACT", "RAW_TIN_VAULT_KEYRING_CONTRACT",
    "RawTinVaultError", "SealedRawTinVaultEntry",
    "load_raw_tin_vault_keyring", "open_ein", "rewrap_ein", "seal_ein",
]
