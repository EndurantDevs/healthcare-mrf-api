# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit S3 signing keys from one operator-provisioned local capability."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from process.custom_import.snowflake import (
    _close_descriptor_after_failure,
    _credential_directory_identity,
    _credential_file_identity,
    _open_fixed_credential_directory,
    _unique_json_object,
    _validate_fixed_credential_directory,
    _validate_fixed_credential_file,
)

FIXED_S3_CREDENTIAL_FILENAME = "s3-access-key.json"
_MAX_CREDENTIAL_BYTES = 16 * 1024


class S3CredentialError(ValueError):
    """A redacted local signing-credential failure."""


def _credential_text(value: object, maximum_bytes: int) -> str:
    if not isinstance(value, str) or not value.isascii() or not value.isprintable():
        raise ValueError
    if not 1 <= len(value) <= maximum_bytes or any(character.isspace() for character in value):
        raise ValueError
    return value


@dataclass(frozen=True)
class S3Credentials:
    """Explicit signing material, never an ambient profile or discovery request."""

    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        try:
            _credential_text(self.access_key_id, 256)
            _credential_text(self.secret_access_key, 1024)
            if self.session_token is not None:
                _credential_text(self.session_token, 8192)
        except Exception:
            raise S3CredentialError("s3_credentials_invalid") from None


class S3CredentialProvider(Protocol):
    """A composition-time provider; source requests cannot select credentials."""

    def load_credentials(self) -> S3Credentials:
        """Return explicit ephemeral keys without paths or selectors as arguments."""


def _parse_credentials(payload: bytes) -> S3Credentials:
    document_by_key = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_json_object)
    required_keys = {"access_key_id", "secret_access_key"}
    if not isinstance(document_by_key, dict) or not required_keys <= set(document_by_key) <= required_keys | {
        "session_token"
    }:
        raise ValueError
    return S3Credentials(**document_by_key)


def _read_credentials(directory_descriptor: int) -> bytes:
    flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(FIXED_S3_CREDENTIAL_FILENAME, flags, dir_fd=directory_descriptor)
    try:
        before = os.fstat(descriptor)
        _validate_fixed_credential_file(before)
        if before.st_size > _MAX_CREDENTIAL_BYTES:
            raise ValueError
        chunks = []
        byte_count = 0
        while chunk := os.read(descriptor, min(8192, _MAX_CREDENTIAL_BYTES + 1 - byte_count)):
            chunks.append(chunk)
            byte_count += len(chunk)
            if byte_count > _MAX_CREDENTIAL_BYTES:
                raise ValueError
        after = os.fstat(descriptor)
        _validate_fixed_credential_file(after)
        if byte_count != before.st_size or _credential_file_identity(before) != _credential_file_identity(after):
            raise ValueError
    except BaseException:
        _close_descriptor_after_failure(descriptor)
        raise
    os.close(descriptor)
    return b"".join(chunks)


class FixedLocalS3CredentialProvider:
    """Pin a local directory and read only its owner-read-only S3 JSON file.

    The operator composes this capability from a fixed absolute directory, never
    from a source request. The caller owns its lifetime and must close it.
    """

    def __init__(self, credential_directory: Path) -> None:
        try:
            if not isinstance(credential_directory, Path) or not credential_directory.is_absolute():
                raise ValueError
            self._descriptor, self._directory_identity = _open_fixed_credential_directory(credential_directory)
        except Exception:
            raise S3CredentialError("s3_credentials_unavailable") from None

    def __enter__(self) -> FixedLocalS3CredentialProvider:
        self._pinned_descriptor()
        return self

    def __exit__(self, exception_type, _exception, _traceback) -> None:
        if exception_type is None:
            self.close()
            return
        try:
            self.close()
        except BaseException:
            return None

    def close(self) -> None:
        """Release the pinned capability exactly once, without closing caller handles."""
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                raise S3CredentialError("s3_credentials_unavailable") from None

    def _pinned_descriptor(self) -> int:
        try:
            if self._descriptor is None:
                raise ValueError
            metadata = os.fstat(self._descriptor)
            _validate_fixed_credential_directory(metadata)
            if _credential_directory_identity(metadata) != self._directory_identity:
                raise ValueError
            return self._descriptor
        except Exception:
            raise S3CredentialError("s3_credentials_unavailable") from None

    def load_credentials(self) -> S3Credentials:
        """Read a bounded, unchanged, regular file without following its symlink."""
        try:
            return _parse_credentials(_read_credentials(self._pinned_descriptor()))
        except Exception:
            raise S3CredentialError("s3_credentials_unavailable") from None
