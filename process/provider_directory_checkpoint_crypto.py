# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read retained checkpoint capabilities with current or previous cipher keys."""

from __future__ import annotations

import base64
import json
from itertools import chain
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

PREVIOUS_FERNET_KEYS_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_FERNET_KEYS"


def previous_fernet_ciphers(raw_keyring: str | None) -> tuple[Fernet, ...]:
    """Validate the entire optional JSON key ring without exposing its contents."""
    if not raw_keyring or not raw_keyring.strip():
        return ()
    try:
        encoded_keys = json.loads(raw_keyring)
        if not isinstance(encoded_keys, list) or any(not isinstance(key, str) for key in encoded_keys):
            raise ValueError
        ciphers = []
        for encoded_key in dict.fromkeys(encoded_keys):
            key = encoded_key.encode("ascii")
            decoded_key = base64.b64decode(key, altchars=b"-_", validate=True)
            if len(decoded_key) != 32 or base64.urlsafe_b64encode(decoded_key) != key:
                raise ValueError
            ciphers.append(Fernet(key))
        return tuple(ciphers)
    except ValueError, UnicodeError:
        raise RuntimeError("bulk_export_checkpoint_keyring_invalid") from None


def decrypt_bulk_capability(importer: Any, value: Any) -> str:
    """Use previous derived keys only for reads, preserving importer crypto hooks."""
    encrypted_value = importer._clean_text(value)
    if not encrypted_value or not encrypted_value.startswith(importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX):
        raise RuntimeError("bulk_export_checkpoint_ciphertext_invalid")
    token = encrypted_value.removeprefix(importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX)
    previous_ciphers = previous_fernet_ciphers(importer.os.getenv(PREVIOUS_FERNET_KEYS_ENV))
    try:
        decryption_secrets = importer._bulk_checkpoint_decryption_secrets()
    except RuntimeError as exc:
        if str(exc) != "bulk_export_checkpoint_encryption_key_missing" or not previous_ciphers:
            raise
        decryption_secrets = ()
    for key in chain(decryption_secrets, previous_ciphers):
        try:
            cipher = key if isinstance(key, Fernet) else importer._bulk_checkpoint_fernet(key)
            return cipher.decrypt(token.encode("ascii")).decode("utf-8")
        except InvalidToken, UnicodeError, ValueError:
            continue
    raise RuntimeError("bulk_export_checkpoint_decryption_failed")
