# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded declarative Snowflake acquisition for ``custom-import/v1``.

This module intentionally has no driver dependency or runtime registration.  A
composition root supplies a narrowly typed adapter and a fixed local
credential-file provider.  Requests can select only configured relations and
columns, and the connector generates the sole read-only statement shape.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, BinaryIO, Callable, Protocol

from process.custom_import._source_text import (
    _SourceTextValidationError,
    validate_snapshot_token,
)
from process.custom_import.capture import (
    CaptureError,
    CaptureLimits,
    CaptureManifest,
    SealedCapture,
    capture_stream,
    verify_capture,
)
from process.custom_import.definition import (
    CONTRACT_VERSION,
    CustomImportDefinition,
    SourceStream,
    canonical_json,
)

CONNECTOR_CONTRACT = "custom-import/snowflake-acquisition/v1"
PARQUET_RESULT_FORMAT = "parquet"
FIXED_KEY_PAIR_CREDENTIAL_FILENAME = "snowflake-key-pair.json"

MAX_APPROVED_RELATIONS = 128
MAX_DECLARED_COLUMNS = 128
MAX_SELECTED_COLUMNS = 128
MAX_RESULT_PARTITIONS = 4_096
MAX_RESULT_PARTITION_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 256 * 1024 * 1024
MAX_MANIFEST_CANONICAL_BYTES = 2 * 1024 * 1024
MAX_CREDENTIAL_FILE_BYTES = 128 * 1024
MAX_PRIVATE_KEY_BYTES = 96 * 1024
MAX_PRIVATE_KEY_PASSPHRASE_BYTES = 8 * 1024
MAX_CREDENTIAL_PRINCIPAL_BYTES = 255
MAX_RESULT_TYPE_BYTES = 255

_SNOWFLAKE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
_DECLARED_FIELD_ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_ENVIRONMENT_REFERENCE = re.compile(r"^(?:env:|\$\{|\$[A-Za-z_])")

# The adapter may return only raw Parquet bytes.  This fixed stream shape
# carries those bytes through the shared capture/replay boundary; it cannot be
# replaced by an adapter-supplied path, format, compression mode, or selector.
SNOWFLAKE_RESULT_STREAM = SourceStream(
    stream_id="snowflake_result",
    record_kind="root",
    child_collection=None,
    format=PARQUET_RESULT_FORMAT,
    compression="none",
    record_path=None,
    snapshot_token="source_snapshot",
)
DEFAULT_CAPTURE_LIMITS = CaptureLimits()


class SnowflakeConnectorError(ValueError):
    """A Snowflake connector input, response, or identity is unsafe or invalid."""


class SnowflakeCredentialError(SnowflakeConnectorError):
    """The fixed local key-pair credential material is unavailable or malformed."""


def _identifier(value: Any, label: str) -> str:
    """Accept one unquoted Snowflake identifier and normalize its safe form."""

    if not isinstance(value, str) or not _SNOWFLAKE_IDENTIFIER.fullmatch(value):
        raise SnowflakeConnectorError(f"{label} must be a simple Snowflake identifier")
    return value.upper()


def _field_id(value: Any, label: str) -> str:
    """Accept one custom-import field identifier shared with a declaration."""

    if not isinstance(value, str) or not _DECLARED_FIELD_ID.fullmatch(value):
        raise SnowflakeConnectorError(f"{label} must be a declared lower_snake_case field id")
    return value


def _sha256(value: Any, label: str) -> str:
    """Require one retained SHA-256 hexadecimal identity."""

    if not isinstance(value, str) or not _SHA256_HEX.fullmatch(value):
        raise SnowflakeConnectorError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _printable_text(value: Any, label: str, *, maximum_bytes: int) -> str:
    """Accept bounded printable text that is retained in a manifest or credential."""

    if not isinstance(value, str) or not value:
        raise SnowflakeConnectorError(f"{label} must be non-empty text")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SnowflakeConnectorError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > maximum_bytes:
        raise SnowflakeConnectorError(f"{label} exceeds the byte limit")
    if not value.isprintable():
        raise SnowflakeConnectorError(f"{label} must be printable text")
    return value


def _credential_text(value: Any, label: str, *, maximum_bytes: int) -> bytes:
    """Encode bounded key material without permitting environment indirection."""

    if not isinstance(value, str) or not value:
        raise SnowflakeCredentialError(f"{label} must be non-empty text")
    if _ENVIRONMENT_REFERENCE.match(value):
        raise SnowflakeCredentialError(f"{label} cannot reference an environment value")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SnowflakeCredentialError(f"{label} must be valid UTF-8") from exc
    if len(encoded) > maximum_bytes:
        raise SnowflakeCredentialError(f"{label} exceeds the byte limit")
    return encoded


def _credential_principal(value: Any, label: str) -> str:
    """Validate a non-secret account or user identifier from the fixed file."""

    try:
        result = _printable_text(value, label, maximum_bytes=MAX_CREDENTIAL_PRINCIPAL_BYTES)
    except SnowflakeConnectorError as exc:
        raise SnowflakeCredentialError(str(exc)) from exc
    if result != result.strip() or any(character.isspace() for character in result):
        raise SnowflakeCredentialError(f"{label} cannot contain whitespace")
    if _ENVIRONMENT_REFERENCE.match(result):
        raise SnowflakeCredentialError(f"{label} cannot reference an environment value")
    return result


def _snapshot_token(value: Any) -> str:
    """Map the shared source-token boundary to connector diagnostics."""

    try:
        return validate_snapshot_token(value)
    except _SourceTextValidationError as exc:
        message_by_reason = {
            "not_string": "source snapshot token must be non-empty text",
            "empty": "source snapshot token must be non-empty text",
            "invalid_utf8": "source snapshot token must be valid UTF-8",
            "byte_limit": "source snapshot token exceeds the byte limit",
            "non_printable": "source snapshot token must be printable text",
        }
        raise SnowflakeConnectorError(message_by_reason[exc.reason]) from exc


def _identity_sha256(domain: str, document: dict[str, Any]) -> tuple[str, str]:
    """Canonicalize and domain-separate one immutable connector identity document."""

    canonical = canonical_json(document)
    material = f"{CONTRACT_VERSION}\x00{CONNECTOR_CONTRACT}\x00{domain}\x00{canonical}".encode("utf-8")
    return canonical, hashlib.sha256(material).hexdigest()


def _manifest_identity_sha256(document_by_key: dict[str, Any]) -> tuple[str, str]:
    """Seal the bounded acquisition manifest without definition-node limits.

    Generic definition serialization intentionally rejects documents with more
    than 10,000 nodes.  A valid 4,096-partition acquisition manifest exceeds
    that structural ceiling, so this serializer admits only the already
    validated, internally assembled manifest shape and adds its own byte cap.
    """

    try:
        canonical = json.dumps(
            document_by_key,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        encoded = canonical.encode("utf-8")
    except (TypeError, UnicodeEncodeError, ValueError) as exc:
        raise SnowflakeConnectorError("acquisition manifest cannot be canonically serialized") from exc
    if len(encoded) > MAX_MANIFEST_CANONICAL_BYTES:
        raise SnowflakeConnectorError("acquisition manifest exceeds the canonical byte limit")
    material = f"{CONTRACT_VERSION}\x00{CONNECTOR_CONTRACT}\x00acquisition-manifest\x00".encode("ascii") + encoded
    return canonical, hashlib.sha256(material).hexdigest()


class _ResultContentHasher:
    """Incrementally bind ordered, already-sealed partition content receipts."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(f"{CONTRACT_VERSION}\x00{CONNECTOR_CONTRACT}\x00content\x00".encode("ascii"))

    def add(self, receipt: SnowflakeResultPartitionManifest) -> None:
        """Add one partition's bounded length and content identity in order."""

        self._digest.update(receipt.ordinal.to_bytes(4, byteorder="big", signed=False))
        self._digest.update(receipt.content_bytes.to_bytes(8, byteorder="big", signed=False))
        self._digest.update(bytes.fromhex(receipt.content_sha256))

    def hexdigest(self) -> str:
        """Return the immutable aggregate content digest."""

        return self._digest.hexdigest()


def _validated_capture_limits(value: Any) -> CaptureLimits:
    """Keep injected capture policy within this connector's hard result bounds."""

    if not isinstance(value, CaptureLimits):
        raise SnowflakeConnectorError("capture limits must use the declared capture-limit type")
    if (
        value.maximum_compressed_bytes > MAX_RESULT_BYTES
        or value.maximum_decoded_bytes > MAX_RESULT_BYTES
        or value.maximum_record_bytes > MAX_RESULT_PARTITION_BYTES
        or value.read_chunk_bytes > MAX_RESULT_PARTITION_BYTES
    ):
        raise SnowflakeConnectorError("capture limits exceed the connector result-byte bounds")
    return value


def _remaining_partition_limits(
    limits: CaptureLimits,
    *,
    compressed_bytes: int,
    decoded_bytes: int,
) -> CaptureLimits:
    """Derive a per-partition capture budget from the aggregate result budget."""

    compressed_remaining = limits.maximum_compressed_bytes - compressed_bytes
    decoded_remaining = limits.maximum_decoded_bytes - decoded_bytes
    if compressed_remaining <= 0 or decoded_remaining <= 0:
        raise SnowflakeConnectorError("result captures exceed the total-byte limit")
    maximum_decoded_bytes = min(decoded_remaining, MAX_RESULT_PARTITION_BYTES)
    return replace(
        limits,
        maximum_compressed_bytes=min(compressed_remaining, MAX_RESULT_PARTITION_BYTES),
        maximum_decoded_bytes=maximum_decoded_bytes,
        maximum_record_bytes=min(limits.maximum_record_bytes, maximum_decoded_bytes),
    )


@dataclass(frozen=True)
class SnowflakeRelation:
    """One fully qualified approved-relation identity with simple identifiers only."""

    database: str
    schema: str
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "database", _identifier(self.database, "relation database"))
        object.__setattr__(self, "schema", _identifier(self.schema, "relation schema"))
        object.__setattr__(self, "name", _identifier(self.name, "relation name"))

    @property
    def parts(self) -> tuple[str, str, str]:
        """Return the normalized three-part relation identity."""

        return (self.database, self.schema, self.name)

    @property
    def quoted_sql(self) -> str:
        """Return the only SQL spelling used by the connector's statement builder."""

        return ".".join(f'"{part}"' for part in self.parts)


@dataclass(frozen=True)
class SnowflakeDeclaredColumn:
    """One approved physical column mapped to a declared custom-import field."""

    field_id: str
    column_identifier: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "field_id", _field_id(self.field_id, "declared field id"))
        object.__setattr__(self, "column_identifier", _identifier(self.column_identifier, "column identifier"))


@dataclass(frozen=True)
class SnowflakeApprovedRelation:
    """A finite relation allowlist entry and its complete selectable column set."""

    relation: SnowflakeRelation
    columns: tuple[SnowflakeDeclaredColumn, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeConnectorError("approved relation must use the declared relation type")
        if not isinstance(self.columns, tuple) or not 1 <= len(self.columns) <= MAX_DECLARED_COLUMNS:
            raise SnowflakeConnectorError("approved relation must declare from 1 through 128 columns")
        if not all(isinstance(column, SnowflakeDeclaredColumn) for column in self.columns):
            raise SnowflakeConnectorError("approved relation columns must use declared column values")
        field_ids = tuple(column.field_id for column in self.columns)
        column_identifiers = tuple(column.column_identifier for column in self.columns)
        if len(field_ids) != len(set(field_ids)):
            raise SnowflakeConnectorError("approved relation field ids must be unique")
        if len(column_identifiers) != len(set(column_identifiers)):
            raise SnowflakeConnectorError("approved relation column identifiers must be unique")

    def column_for(self, field_id: str) -> SnowflakeDeclaredColumn | None:
        """Return the one configured physical-column mapping for a field id."""

        return next((column for column in self.columns if column.field_id == field_id), None)


@dataclass(frozen=True)
class SnowflakeKeyPairCredentials:
    """Ephemeral key-pair material passed only from a provider to an adapter."""

    account: str
    user: str
    private_key_pem: bytes = field(repr=False)
    private_key_passphrase: bytes | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "account", _credential_principal(self.account, "credential account"))
        object.__setattr__(self, "user", _credential_principal(self.user, "credential user"))
        if not isinstance(self.private_key_pem, bytes) or not 1 <= len(self.private_key_pem) <= MAX_PRIVATE_KEY_BYTES:
            raise SnowflakeCredentialError("private key material has an invalid byte length")
        if b"\x00" in self.private_key_pem or not self.private_key_pem.startswith(b"-----BEGIN "):
            raise SnowflakeCredentialError("private key material must be PEM text")
        if b"PRIVATE KEY-----" not in self.private_key_pem:
            raise SnowflakeCredentialError("private key material must contain a private key PEM header")
        if not self.private_key_pem.rstrip().endswith(b"PRIVATE KEY-----"):
            raise SnowflakeCredentialError("private key material must contain a private key PEM footer")
        if self.private_key_passphrase is not None:
            if (
                not isinstance(self.private_key_passphrase, bytes)
                or not 1 <= len(self.private_key_passphrase) <= MAX_PRIVATE_KEY_PASSPHRASE_BYTES
            ):
                raise SnowflakeCredentialError("private key passphrase has an invalid byte length")


class SnowflakeCredentialProvider(Protocol):
    """Load one key-pair credential without receiving a source-controlled path."""

    def load_key_pair(self) -> SnowflakeKeyPairCredentials:
        """Return ephemeral credentials for one adapter invocation."""


@dataclass(frozen=True)
class _CredentialDirectoryIdentity:
    """The exact directory inode held by one fixed credential provider."""

    device: int
    inode: int


class FixedLocalKeyPairCredentialProvider:
    """Read only a fixed key-pair JSON file under a composition-time directory.

    The public ``load_key_pair`` method accepts no file name, arbitrary path,
    environment reference, or credential selector.  Its fixed JSON object uses
    ``account``, ``user``, ``private_key_pem``, and optional
    ``private_key_passphrase`` keys.  Construction pins the directory inode;
    callers must explicitly close the provider (or use it as a context manager)
    once that composition-time capability is no longer needed.
    """

    def __init__(self, credential_directory: Path) -> None:
        if not isinstance(credential_directory, Path) or not credential_directory.is_absolute():
            raise SnowflakeCredentialError("credential directory must be an absolute pathlib path")
        descriptor, identity = _open_fixed_credential_directory(credential_directory)
        self._credential_directory_descriptor: int | None = descriptor
        self._credential_directory_identity = identity
        self._closed = False

    def __enter__(self) -> FixedLocalKeyPairCredentialProvider:
        """Return this still-open provider for composition-time scoped use."""

        self._pinned_directory_descriptor()
        return self

    def __exit__(self, exception_type, _exception, _traceback) -> None:
        """Close the pinned descriptor without masking a surrounding failure."""

        if exception_type is None:
            self.close()
            return
        try:
            self.close()
        except BaseException:
            return None

    def close(self) -> None:
        """Release the held directory descriptor exactly once."""

        if self._closed:
            return
        self._closed = True
        descriptor = self._credential_directory_descriptor
        self._credential_directory_descriptor = None
        if descriptor is None:
            return
        try:
            os.close(descriptor)
        except OSError as exc:
            raise SnowflakeCredentialError("credential directory descriptor cannot be closed") from exc

    def load_key_pair(self) -> SnowflakeKeyPairCredentials:
        """Load bounded key-pair material from the one configured local file."""

        raw = _read_fixed_credential_file(self._pinned_directory_descriptor())
        try:
            decoded = raw.decode("utf-8")
            wire_value = json.loads(decoded, object_pairs_hook=_unique_json_object)
        except (RecursionError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SnowflakeCredentialError("fixed credential file is not valid UTF-8 JSON") from exc
        if not isinstance(wire_value, dict):
            raise SnowflakeCredentialError("fixed credential file must contain one object")
        required_keys = {"account", "user", "private_key_pem"}
        allowed_keys = {*required_keys, "private_key_passphrase"}
        if set(wire_value) - allowed_keys or required_keys - set(wire_value):
            raise SnowflakeCredentialError("fixed credential file has an unsupported key set")
        private_key_pem = _credential_text(
            wire_value["private_key_pem"],
            "private key material",
            maximum_bytes=MAX_PRIVATE_KEY_BYTES,
        )
        passphrase_value = wire_value.get("private_key_passphrase")
        passphrase = (
            None
            if passphrase_value is None
            else _credential_text(
                passphrase_value,
                "private key passphrase",
                maximum_bytes=MAX_PRIVATE_KEY_PASSPHRASE_BYTES,
            )
        )
        return SnowflakeKeyPairCredentials(
            account=wire_value["account"],
            user=wire_value["user"],
            private_key_pem=private_key_pem,
            private_key_passphrase=passphrase,
        )

    def _pinned_directory_descriptor(self) -> int:
        """Return the still-held directory descriptor after checking its inode."""

        descriptor = self._credential_directory_descriptor
        if self._closed or descriptor is None:
            raise SnowflakeCredentialError("credential provider is closed")
        try:
            metadata = os.fstat(descriptor)
        except OSError as exc:
            raise SnowflakeCredentialError("credential directory is unavailable") from exc
        _validate_fixed_credential_directory(metadata)
        if _credential_directory_identity(metadata) != self._credential_directory_identity:
            raise SnowflakeCredentialError("credential directory identity changed")
        return descriptor


def _effective_user_id() -> int:
    """Return the local effective user identity required for private key files."""

    get_effective_user_id = getattr(os, "geteuid", None)
    if not callable(get_effective_user_id):
        raise SnowflakeCredentialError("fixed credential files require local owner identity support")
    return get_effective_user_id()


def _credential_directory_identity(metadata: os.stat_result) -> _CredentialDirectoryIdentity:
    """Return the immutable inode identity used to pin a credential directory."""

    return _CredentialDirectoryIdentity(device=metadata.st_dev, inode=metadata.st_ino)


def _validate_fixed_credential_directory(metadata: os.stat_result) -> None:
    """Require an owner-held directory that no group or other user can modify."""

    if not stat.S_ISDIR(metadata.st_mode):
        raise SnowflakeCredentialError("credential directory must be a real directory")
    if metadata.st_uid != _effective_user_id():
        raise SnowflakeCredentialError("credential directory must be owned by the effective user")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise SnowflakeCredentialError("credential directory mode permits untrusted replacement")


def _open_fixed_credential_directory(credential_directory: Path) -> tuple[int, _CredentialDirectoryIdentity]:
    """Open and pin a nofollow directory descriptor for provider lifetime."""

    directory_flag = getattr(os, "O_DIRECTORY", None)
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if directory_flag is None or no_follow is None or os.open not in os.supports_dir_fd:
        raise SnowflakeCredentialError("fixed credential files require descriptor-relative directory support")
    try:
        expected_metadata = os.stat(credential_directory, follow_symlinks=False)
    except OSError as exc:
        raise SnowflakeCredentialError("credential directory is unavailable") from exc
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | directory_flag | no_follow
        descriptor = os.open(credential_directory, flags)
        opened_metadata = os.fstat(descriptor)
        _validate_fixed_credential_directory(opened_metadata)
        if _credential_directory_identity(opened_metadata) != _credential_directory_identity(expected_metadata):
            raise SnowflakeCredentialError("credential directory identity changed while opening")
        if descriptor is None:  # Narrow the descriptor handoff after a successful open.
            raise SnowflakeCredentialError("credential directory is unavailable")
        held_descriptor = descriptor
        descriptor = None
        return held_descriptor, _credential_directory_identity(opened_metadata)
    except SnowflakeCredentialError:
        raise
    except OSError as exc:
        raise SnowflakeCredentialError("credential directory is unavailable") from exc
    finally:
        if descriptor is not None:
            _close_descriptor_after_failure(descriptor)


def _credential_file_identity(metadata: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    """Capture every file fact that must remain fixed while credential bytes are read."""

    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_uid,
        metadata.st_mode,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _validate_fixed_credential_file(metadata: os.stat_result) -> None:
    """Require one bounded, owner-only regular key-pair document."""

    if not stat.S_ISREG(metadata.st_mode):
        raise SnowflakeCredentialError("fixed credential file must be regular")
    if metadata.st_uid != _effective_user_id():
        raise SnowflakeCredentialError("fixed credential file must be owned by the effective user")
    if stat.S_IMODE(metadata.st_mode) != 0o400:
        raise SnowflakeCredentialError("fixed credential file must use owner-read-only mode")
    if not 1 <= metadata.st_size <= MAX_CREDENTIAL_FILE_BYTES:
        raise SnowflakeCredentialError("fixed credential file has an invalid byte length")


def _close_descriptor_after_failure(descriptor: int) -> None:
    """Best-effort descriptor cleanup that cannot hide an active primary error."""

    try:
        os.close(descriptor)
    except BaseException:
        return None


def _close_descriptor_after_success(descriptor: int) -> None:
    """Close a descriptor and surface a terminal cleanup failure deterministically."""

    try:
        os.close(descriptor)
    except OSError as exc:
        raise SnowflakeCredentialError("fixed credential file descriptor cannot be closed") from exc


def _read_fixed_credential_file(credential_directory_descriptor: int) -> bytes:
    """Read one owner-only fixed file relative to the pinned directory descriptor."""

    non_blocking = getattr(os, "O_NONBLOCK", None)
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if non_blocking is None or no_follow is None or os.open not in os.supports_dir_fd:
        raise SnowflakeCredentialError("fixed credential files require secure file-opening support")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | non_blocking | no_follow
    descriptor: int | None = None
    try:
        # Opening a FIFO or device before fstat can otherwise block forever.
        descriptor = os.open(
            FIXED_KEY_PAIR_CREDENTIAL_FILENAME,
            flags,
            dir_fd=credential_directory_descriptor,
        )
    except OSError as exc:
        raise SnowflakeCredentialError("fixed credential file is unavailable") from exc
    try:
        file_status = os.fstat(descriptor)
        _validate_fixed_credential_file(file_status)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 8 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_CREDENTIAL_FILE_BYTES:
                raise SnowflakeCredentialError("fixed credential file exceeds the byte limit")
            chunks.append(chunk)
        final_status = os.fstat(descriptor)
        _validate_fixed_credential_file(final_status)
        if total != file_status.st_size or _credential_file_identity(final_status) != _credential_file_identity(
            file_status
        ):
            raise SnowflakeCredentialError("fixed credential file changed while reading")
    except OSError as exc:
        _close_descriptor_after_failure(descriptor)
        raise SnowflakeCredentialError("fixed credential file is unavailable") from exc
    except BaseException:
        _close_descriptor_after_failure(descriptor)
        raise
    _close_descriptor_after_success(descriptor)
    return b"".join(chunks)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys before credential object validation."""

    credential_values_by_key: dict[str, Any] = {}
    for key, value in pairs:
        if key in credential_values_by_key:
            raise SnowflakeCredentialError("fixed credential file contains a duplicate key")
        credential_values_by_key[key] = value
    return credential_values_by_key


@dataclass(frozen=True)
class SnowflakeReadRequest:
    """The immutable relation, field, and declaration identities for one read."""

    relation: SnowflakeRelation
    selected_columns: tuple[SnowflakeDeclaredColumn, ...]
    definition_sha256: str
    schema_sha256: str
    canonical_request: str = field(init=False, repr=False)
    request_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.relation, SnowflakeRelation):
            raise SnowflakeConnectorError("read request relation must use the declared relation type")
        if not isinstance(self.selected_columns, tuple) or not 1 <= len(self.selected_columns) <= MAX_SELECTED_COLUMNS:
            raise SnowflakeConnectorError("read request must select from 1 through 128 declared columns")
        if not all(isinstance(column, SnowflakeDeclaredColumn) for column in self.selected_columns):
            raise SnowflakeConnectorError("read request columns must use declared column values")
        field_ids = tuple(column.field_id for column in self.selected_columns)
        if len(field_ids) != len(set(field_ids)):
            raise SnowflakeConnectorError("read request field ids must be unique")
        definition_sha256 = _sha256(self.definition_sha256, "definition digest")
        schema_sha256 = _sha256(self.schema_sha256, "schema digest")
        request_document_by_key = {
            "contract": CONNECTOR_CONTRACT,
            "definition_sha256": definition_sha256,
            "fields": [
                {"column_identifier": column.column_identifier, "field_id": column.field_id}
                for column in self.selected_columns
            ],
            "relation": list(self.relation.parts),
            "schema_sha256": schema_sha256,
        }
        canonical, digest = _identity_sha256("request", request_document_by_key)
        object.__setattr__(self, "definition_sha256", definition_sha256)
        object.__setattr__(self, "schema_sha256", schema_sha256)
        object.__setattr__(self, "canonical_request", canonical)
        object.__setattr__(self, "request_sha256", digest)

    @property
    def selected_field_ids(self) -> tuple[str, ...]:
        """Return source-independent field identities in statement order."""

        return tuple(column.field_id for column in self.selected_columns)


@dataclass(frozen=True)
class SnowflakeReadStatement:
    """The sole generated read-only SQL shape and its immutable identity."""

    request: SnowflakeReadRequest
    sql: str = field(init=False)
    canonical_statement: str = field(init=False, repr=False)
    statement_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.request, SnowflakeReadRequest):
            raise SnowflakeConnectorError("statement request must use the declared request type")
        selected = ", ".join(
            f'"{column.column_identifier}" AS "{column.field_id}"' for column in self.request.selected_columns
        )
        sql = f"SELECT {selected} FROM {self.request.relation.quoted_sql}"
        statement_document_by_key = {
            "contract": CONNECTOR_CONTRACT,
            "format": PARQUET_RESULT_FORMAT,
            "request_sha256": self.request.request_sha256,
            "sql": sql,
        }
        canonical, digest = _identity_sha256("statement", statement_document_by_key)
        object.__setattr__(self, "sql", sql)
        object.__setattr__(self, "canonical_statement", canonical)
        object.__setattr__(self, "statement_sha256", digest)


@dataclass(frozen=True)
class SnowflakeResultColumn:
    """One adapter-reported Parquet result column used in the schema fingerprint."""

    field_id: str
    source_type: str
    nullable: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "field_id", _field_id(self.field_id, "result field id"))
        object.__setattr__(
            self,
            "source_type",
            _printable_text(self.source_type, "result source type", maximum_bytes=MAX_RESULT_TYPE_BYTES),
        )
        if not isinstance(self.nullable, bool):
            raise SnowflakeConnectorError("result nullable must be a boolean")


class SnowflakePartitionSourceSet(Protocol):
    """A closeable single-pass owner of adapter-created binary partition readers.

    Once a reader is yielded, ownership transfers to ``SnowflakeParquetResult``
    and the set's ``close`` method must release only unyielded readers and its
    own transport state.  This prevents a cancellation from leaking either the
    current reader or unread adapter resources without pre-materializing them.
    ``close`` must be idempotent and must attempt every retained resource before
    it returns or raises; the connector invokes it exactly once per result.
    """

    def __iter__(self) -> Iterator[BinaryIO]:
        """Yield each binary result partition at most once."""

        raise NotImplementedError

    def close(self) -> None:
        """Release unyielded readers and the source-set transport state."""

        raise NotImplementedError


def _resource_closer(value: object) -> Callable[[], None] | None:
    """Return an explicit resource closer without assuming a concrete adapter type."""

    close = getattr(value, "close", None)
    return close if callable(close) else None


@dataclass
class SnowflakeParquetResult:
    """One single-use, explicitly owned adapter result and its schema facts.

    The response deliberately carries binary readers rather than already
    materialized partition bytes.  The connector admits each reader through
    ``capture_stream`` before moving to the next partition, then closes every
    owned resource even when a cancellation or ``KeyboardInterrupt`` occurs.
    """

    source_snapshot_token: str
    schema: tuple[SnowflakeResultColumn, ...]
    partition_sources: SnowflakePartitionSourceSet
    on_close: Callable[[], None] | None = field(default=None, repr=False)
    _partition_sources_consumed: bool = field(init=False, default=False, repr=False)
    _partition_iterator: Iterator[BinaryIO] | None = field(init=False, default=None, repr=False)
    _owned_readers_by_id: dict[int, BinaryIO] = field(init=False, default_factory=dict, repr=False)
    _closed_resource_ids: set[int] = field(init=False, default_factory=set, repr=False)
    _cleanup_errors: list[BaseException] = field(init=False, default_factory=list, repr=False)
    _closed: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        self.source_snapshot_token = _snapshot_token(self.source_snapshot_token)
        if not isinstance(self.schema, tuple) or not 1 <= len(self.schema) <= MAX_SELECTED_COLUMNS:
            raise SnowflakeConnectorError("result schema must contain from 1 through 128 columns")
        if not all(isinstance(column, SnowflakeResultColumn) for column in self.schema):
            raise SnowflakeConnectorError("result schema columns must use declared result-column values")
        field_ids = tuple(column.field_id for column in self.schema)
        if len(field_ids) != len(set(field_ids)):
            raise SnowflakeConnectorError("result schema field ids must be unique")
        if isinstance(self.partition_sources, (bytes, bytearray, memoryview, str)) or not isinstance(
            self.partition_sources, Iterable
        ):
            raise SnowflakeConnectorError("result partition sources must be an iterable of binary readers")
        if _resource_closer(self.partition_sources) is None:
            raise SnowflakeConnectorError("result partition sources must own an explicit close method")
        if self.on_close is not None and not callable(self.on_close):
            raise SnowflakeConnectorError("result cleanup callback must be callable")

    def consume_partition_sources(self) -> Iterator[BinaryIO]:
        """Return the owned source iterator once and retain it for deterministic closure."""

        if self._closed:
            raise SnowflakeConnectorError("adapter result is closed")
        if self._partition_sources_consumed:
            raise SnowflakeConnectorError("adapter result partition sources were already consumed")
        try:
            iterator = iter(self.partition_sources)
        except TypeError as exc:
            raise SnowflakeConnectorError("result partition sources must be iterable") from exc
        if _resource_closer(iterator) is None:
            raise SnowflakeConnectorError("result partition iterator must own an explicit close method")
        self._partition_sources_consumed = True
        self._partition_iterator = iterator
        return iterator

    def claim_partition_source(self, source: BinaryIO) -> None:
        """Take ownership of one yielded reader before its contents are inspected."""

        if self._closed:
            raise SnowflakeConnectorError("adapter result is closed")
        if _resource_closer(source) is None:
            raise SnowflakeConnectorError("result partition source must own an explicit close method")
        self._owned_readers_by_id.setdefault(id(source), source)

    def close_partition_source(self, source: BinaryIO) -> None:
        """Release one claimed reader now while retaining cleanup-failure evidence."""

        if id(source) not in self._owned_readers_by_id:
            raise SnowflakeConnectorError("result partition source was not claimed")
        self._close_owned_resource(source)

    def close_partition_iterator(self) -> None:
        """Release the retained partition iterator after iteration ends or aborts."""

        if self._partition_iterator is not None:
            self._close_owned_resource(self._partition_iterator)

    def close(self) -> None:
        """Release readers, iterator, source set, and adapter cleanup callback once."""

        if self._closed:
            return
        self._closed = True
        for reader in tuple(self._owned_readers_by_id.values()):
            self._close_owned_resource(reader)
        self.close_partition_iterator()
        self._close_owned_resource(self.partition_sources)
        if self.on_close is not None:
            try:
                self.on_close()
            except BaseException as exc:
                self._cleanup_errors.append(exc)
        if self._cleanup_errors:
            raise SnowflakeConnectorError("adapter result cleanup failed") from self._cleanup_errors[0]

    def _close_owned_resource(self, resource: object) -> None:
        """Attempt an idempotent close without replacing an active processing error."""

        resource_id = id(resource)
        if resource_id in self._closed_resource_ids:
            return
        self._closed_resource_ids.add(resource_id)
        close = _resource_closer(resource)
        if close is None:
            self._cleanup_errors.append(SnowflakeConnectorError("adapter resource has no close method"))
            return
        try:
            close()
        except BaseException as exc:
            self._cleanup_errors.append(exc)


class SnowflakeParquetAdapter(Protocol):
    """Execute the connector-generated statement and return bounded Parquet output."""

    def fetch_parquet(
        self,
        statement: SnowflakeReadStatement,
        credentials: SnowflakeKeyPairCredentials,
    ) -> SnowflakeParquetResult:
        """Fetch the generated statement's result without accepting arbitrary SQL."""


@dataclass(frozen=True)
class SnowflakeResultPartitionManifest:
    """One ordered result-partition content receipt without the partition payload."""

    ordinal: int
    content_bytes: int
    content_sha256: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or not 1 <= self.ordinal <= MAX_RESULT_PARTITIONS
        ):
            raise SnowflakeConnectorError("result partition ordinal is invalid")
        if (
            isinstance(self.content_bytes, bool)
            or not isinstance(self.content_bytes, int)
            or not 1 <= self.content_bytes <= MAX_RESULT_PARTITION_BYTES
        ):
            raise SnowflakeConnectorError("result partition byte count is invalid")
        object.__setattr__(self, "content_sha256", _sha256(self.content_sha256, "result partition digest"))


@dataclass(frozen=True)
class SnowflakeAcquisitionManifest:
    """Immutable source snapshot, schema, partition, and content identity facts."""

    request_sha256: str
    statement_sha256: str
    source_snapshot_token: str
    schema_fingerprint: str
    result_partitions: tuple[SnowflakeResultPartitionManifest, ...]
    content_sha256: str
    canonical_manifest: str = field(init=False, repr=False)
    manifest_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        request_sha256 = _sha256(self.request_sha256, "request digest")
        statement_sha256 = _sha256(self.statement_sha256, "statement digest")
        source_snapshot_token = _snapshot_token(self.source_snapshot_token)
        schema_fingerprint = _sha256(self.schema_fingerprint, "schema fingerprint")
        content_sha256 = _sha256(self.content_sha256, "content digest")
        if not isinstance(self.result_partitions, tuple) or len(self.result_partitions) > MAX_RESULT_PARTITIONS:
            raise SnowflakeConnectorError("result partition manifest exceeds the partition limit")
        if not all(isinstance(partition, SnowflakeResultPartitionManifest) for partition in self.result_partitions):
            raise SnowflakeConnectorError("result partition manifest has an invalid entry")
        if tuple(partition.ordinal for partition in self.result_partitions) != tuple(
            range(1, len(self.result_partitions) + 1)
        ):
            raise SnowflakeConnectorError("result partition manifest ordinals must be contiguous")
        manifest_document_by_key = {
            "content_sha256": content_sha256,
            "contract": CONNECTOR_CONTRACT,
            "format": PARQUET_RESULT_FORMAT,
            "request_sha256": request_sha256,
            "result_partitions": [
                {
                    "content_bytes": partition.content_bytes,
                    "content_sha256": partition.content_sha256,
                    "ordinal": partition.ordinal,
                }
                for partition in self.result_partitions
            ],
            "schema_fingerprint": schema_fingerprint,
            "source_snapshot_token": source_snapshot_token,
            "statement_sha256": statement_sha256,
        }
        canonical, digest = _manifest_identity_sha256(manifest_document_by_key)
        object.__setattr__(self, "request_sha256", request_sha256)
        object.__setattr__(self, "statement_sha256", statement_sha256)
        object.__setattr__(self, "source_snapshot_token", source_snapshot_token)
        object.__setattr__(self, "schema_fingerprint", schema_fingerprint)
        object.__setattr__(self, "content_sha256", content_sha256)
        object.__setattr__(self, "canonical_manifest", canonical)
        object.__setattr__(self, "manifest_sha256", digest)


@dataclass(frozen=True)
class SnowflakeAcquisition:
    """A sealed Parquet result whose captures match its immutable manifest."""

    statement: SnowflakeReadStatement
    manifest: SnowflakeAcquisitionManifest
    parquet_captures: tuple[SealedCapture, ...] = field(repr=False)
    capture_limits: CaptureLimits = field(default=DEFAULT_CAPTURE_LIMITS, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.statement, SnowflakeReadStatement):
            raise SnowflakeConnectorError("acquisition statement must use the declared statement type")
        if not isinstance(self.manifest, SnowflakeAcquisitionManifest):
            raise SnowflakeConnectorError("acquisition manifest must use the declared manifest type")
        if self.statement.request.request_sha256 != self.manifest.request_sha256:
            raise SnowflakeConnectorError("acquisition request identity does not match its manifest")
        if self.statement.statement_sha256 != self.manifest.statement_sha256:
            raise SnowflakeConnectorError("acquisition statement identity does not match its manifest")
        if not isinstance(self.parquet_captures, tuple) or len(self.parquet_captures) != len(
            self.manifest.result_partitions
        ):
            raise SnowflakeConnectorError("acquisition captures do not match the partition manifest")
        capture_limits = _validated_capture_limits(self.capture_limits)
        receipts, content_sha256 = _verified_capture_receipts(
            self.parquet_captures,
            source_snapshot_token=self.manifest.source_snapshot_token,
            capture_limits=capture_limits,
        )
        if receipts != self.manifest.result_partitions:
            raise SnowflakeConnectorError("acquisition captures do not match the partition manifest")
        if content_sha256 != self.manifest.content_sha256:
            raise SnowflakeConnectorError("acquisition content digest does not match its manifest")
        object.__setattr__(self, "capture_limits", capture_limits)


class SnowflakeAcquisitionConnector:
    """Prepare and execute only allowlisted declarative relation reads."""

    def __init__(
        self,
        *,
        approved_relations: tuple[SnowflakeApprovedRelation, ...],
        credential_provider: SnowflakeCredentialProvider,
        adapter: SnowflakeParquetAdapter,
        capture_limits: CaptureLimits = DEFAULT_CAPTURE_LIMITS,
    ) -> None:
        if not isinstance(approved_relations, tuple) or not 1 <= len(approved_relations) <= MAX_APPROVED_RELATIONS:
            raise SnowflakeConnectorError("approved relations must contain from 1 through 128 entries")
        if not all(isinstance(relation, SnowflakeApprovedRelation) for relation in approved_relations):
            raise SnowflakeConnectorError("approved relations must use declared relation values")
        relation_keys = tuple(relation.relation.parts for relation in approved_relations)
        if len(relation_keys) != len(set(relation_keys)):
            raise SnowflakeConnectorError("approved relation identifiers must be unique")
        if not callable(getattr(credential_provider, "load_key_pair", None)):
            raise SnowflakeConnectorError("credential provider must load a key pair")
        if not callable(getattr(adapter, "fetch_parquet", None)):
            raise SnowflakeConnectorError("adapter must fetch Parquet results")
        self._approved_by_relation = MappingProxyType(
            {relation.relation.parts: relation for relation in approved_relations}
        )
        self._credential_provider = credential_provider
        self._adapter = adapter
        self._capture_limits = _validated_capture_limits(capture_limits)

    def prepare_request(
        self,
        definition: CustomImportDefinition,
        *,
        relation: SnowflakeRelation,
        selected_field_ids: tuple[str, ...],
    ) -> SnowflakeReadRequest:
        """Bind one explicit declared-field subset to an approved relation."""

        if not isinstance(definition, CustomImportDefinition):
            raise SnowflakeConnectorError("read request requires a custom-import definition")
        if not isinstance(relation, SnowflakeRelation):
            raise SnowflakeConnectorError("read request relation must use the declared relation type")
        if not isinstance(selected_field_ids, tuple) or not 1 <= len(selected_field_ids) <= MAX_SELECTED_COLUMNS:
            raise SnowflakeConnectorError("selected fields must contain from 1 through 128 entries")
        normalized_field_ids = tuple(_field_id(field_id, "selected field id") for field_id in selected_field_ids)
        if len(normalized_field_ids) != len(set(normalized_field_ids)):
            raise SnowflakeConnectorError("selected field ids must be unique")
        approved_relation = self._approved_by_relation.get(relation.parts)
        if approved_relation is None:
            raise SnowflakeConnectorError("relation identifier is not approved")
        declared_fields = definition.fields_by_id
        selected_columns: list[SnowflakeDeclaredColumn] = []
        for field_id in normalized_field_ids:
            if field_id not in declared_fields:
                raise SnowflakeConnectorError("selected field is not declared by the custom-import definition")
            column = approved_relation.column_for(field_id)
            if column is None:
                raise SnowflakeConnectorError("selected field is not declared for the approved relation")
            selected_columns.append(column)
        return SnowflakeReadRequest(
            relation=approved_relation.relation,
            selected_columns=tuple(selected_columns),
            definition_sha256=definition.digest,
            schema_sha256=definition.schema_digest,
        )

    def build_statement(self, request: SnowflakeReadRequest) -> SnowflakeReadStatement:
        """Generate the sole read-only statement after rechecking approval identity."""

        self._validate_approved_request(request)
        return SnowflakeReadStatement(request=request)

    def acquire(self, request: SnowflakeReadRequest) -> SnowflakeAcquisition:
        """Fetch and seal an adapter result without decoding or registering it."""

        statement = self.build_statement(request)
        credentials = self._credential_provider.load_key_pair()
        if not isinstance(credentials, SnowflakeKeyPairCredentials):
            raise SnowflakeConnectorError("credential provider returned an invalid key-pair value")
        result: object | None = None
        acquisition_succeeded = False
        try:
            result = self._adapter.fetch_parquet(statement, credentials)
            if not isinstance(result, SnowflakeParquetResult):
                raise SnowflakeConnectorError("adapter returned an invalid Parquet result")
            acquisition = _seal_acquisition(statement, result, capture_limits=self._capture_limits)
            acquisition_succeeded = True
            return acquisition
        finally:
            if result is not None:
                if acquisition_succeeded:
                    _close_adapter_result_after_success(result)
                else:
                    _close_adapter_result_after_failure(result)

    def _validate_approved_request(self, request: SnowflakeReadRequest) -> None:
        """Reject manually constructed requests that differ from the allowlist mapping."""

        if not isinstance(request, SnowflakeReadRequest):
            raise SnowflakeConnectorError("statement request must use the declared request type")
        approved_relation = self._approved_by_relation.get(request.relation.parts)
        if approved_relation is None:
            raise SnowflakeConnectorError("relation identifier is not approved")
        expected_columns = tuple(approved_relation.column_for(field_id) for field_id in request.selected_field_ids)
        if any(column is None for column in expected_columns) or request.selected_columns != expected_columns:
            raise SnowflakeConnectorError("request columns do not match the approved relation declaration")


def _close_adapter_result_after_failure(result: object) -> None:
    """Best-effort adapter-result cleanup that never masks a primary BaseException."""

    close = _resource_closer(result)
    if close is None:
        return
    try:
        close()
    except BaseException:
        return None


def _close_adapter_result_after_success(result: SnowflakeParquetResult) -> None:
    """Close a successful adapter result and map any cleanup failure consistently."""

    try:
        result.close()
    except SnowflakeConnectorError:
        raise
    except BaseException as exc:
        raise SnowflakeConnectorError("adapter result cleanup failed") from exc


def _seal_acquisition(
    statement: SnowflakeReadStatement,
    adapter_result: SnowflakeParquetResult,
    *,
    capture_limits: CaptureLimits,
) -> SnowflakeAcquisition:
    """Bind an adapter response to the generated statement and immutable manifests."""

    if tuple(column.field_id for column in adapter_result.schema) != statement.request.selected_field_ids:
        raise SnowflakeConnectorError("result schema does not match the selected declared fields")
    schema_document_by_key = {
        "columns": [
            {
                "field_id": column.field_id,
                "nullable": column.nullable,
                "source_type": column.source_type,
            }
            for column in adapter_result.schema
        ],
        "contract": CONNECTOR_CONTRACT,
        "format": PARQUET_RESULT_FORMAT,
    }
    _, schema_fingerprint = _identity_sha256("result-schema", schema_document_by_key)
    parquet_captures, partition_manifest_entries, content_sha256 = _capture_result_partitions(
        adapter_result,
        capture_limits=capture_limits,
    )
    manifest = SnowflakeAcquisitionManifest(
        request_sha256=statement.request.request_sha256,
        statement_sha256=statement.statement_sha256,
        source_snapshot_token=adapter_result.source_snapshot_token,
        schema_fingerprint=schema_fingerprint,
        result_partitions=partition_manifest_entries,
        content_sha256=content_sha256,
    )
    return SnowflakeAcquisition(
        statement=statement,
        manifest=manifest,
        parquet_captures=parquet_captures,
        capture_limits=capture_limits,
    )


def _capture_result_partitions(
    adapter_result: SnowflakeParquetResult,
    *,
    capture_limits: CaptureLimits,
) -> tuple[tuple[SealedCapture, ...], tuple[SnowflakeResultPartitionManifest, ...], str]:
    """Capture adapter readers one at a time under one aggregate result budget."""

    captures: list[SealedCapture] = []
    receipts: list[SnowflakeResultPartitionManifest] = []
    content_hasher = _ResultContentHasher()
    compressed_bytes = 0
    decoded_bytes = 0
    partition_iterator = adapter_result.consume_partition_sources()
    try:
        for ordinal, acquired_source in enumerate(partition_iterator, start=1):
            adapter_result.claim_partition_source(acquired_source)
            try:
                if ordinal > MAX_RESULT_PARTITIONS:
                    raise SnowflakeConnectorError("result partitions exceed the manifest limit")
                if not callable(getattr(acquired_source, "read", None)):
                    raise SnowflakeConnectorError("result partition source must be a binary reader")
                partition_limits = _remaining_partition_limits(
                    capture_limits,
                    compressed_bytes=compressed_bytes,
                    decoded_bytes=decoded_bytes,
                )
                try:
                    capture = capture_stream(
                        acquired_source,
                        SNOWFLAKE_RESULT_STREAM,
                        source_snapshot_token=adapter_result.source_snapshot_token,
                        limits=partition_limits,
                    )
                except (CaptureError, OSError) as exc:
                    raise SnowflakeConnectorError("result partition cannot be captured within the byte limits") from exc
                receipt = _capture_receipt(
                    capture,
                    ordinal=ordinal,
                    source_snapshot_token=adapter_result.source_snapshot_token,
                )
                compressed_bytes += capture.manifest.compressed_bytes
                decoded_bytes += capture.manifest.decoded_bytes
                captures.append(capture)
                receipts.append(receipt)
                content_hasher.add(receipt)
            finally:
                adapter_result.close_partition_source(acquired_source)
    finally:
        adapter_result.close_partition_iterator()
    return tuple(captures), tuple(receipts), content_hasher.hexdigest()


def _capture_receipt(
    capture: SealedCapture,
    *,
    ordinal: int,
    source_snapshot_token: str,
) -> SnowflakeResultPartitionManifest:
    """Expose one sealed capture as a bounded content receipt for its result slot."""

    if not isinstance(capture, SealedCapture) or not isinstance(capture.manifest, CaptureManifest):
        raise SnowflakeConnectorError("result partition must use a sealed capture")
    capture_manifest = capture.manifest
    if capture_manifest.source_snapshot_token != source_snapshot_token:
        raise SnowflakeConnectorError("result capture snapshot token does not match the acquisition")
    if (
        isinstance(capture_manifest.compressed_bytes, bool)
        or not isinstance(capture_manifest.compressed_bytes, int)
        or not 1 <= capture_manifest.compressed_bytes <= MAX_RESULT_PARTITION_BYTES
    ):
        raise SnowflakeConnectorError("result partition has an invalid byte length")
    if (
        isinstance(capture_manifest.decoded_bytes, bool)
        or not isinstance(capture_manifest.decoded_bytes, int)
        or not 1 <= capture_manifest.decoded_bytes <= MAX_RESULT_PARTITION_BYTES
    ):
        raise SnowflakeConnectorError("result partition has an invalid decoded-byte length")
    return SnowflakeResultPartitionManifest(
        ordinal=ordinal,
        content_bytes=capture_manifest.compressed_bytes,
        content_sha256=capture_manifest.compressed_sha256,
    )


def _verified_capture_receipts(
    captures: tuple[SealedCapture, ...],
    *,
    source_snapshot_token: str,
    capture_limits: CaptureLimits,
) -> tuple[tuple[SnowflakeResultPartitionManifest, ...], str]:
    """Replay-verify capture payloads while enforcing the aggregate byte budget."""

    if len(captures) > MAX_RESULT_PARTITIONS:
        raise SnowflakeConnectorError("result partition manifest exceeds the partition limit")
    receipts: list[SnowflakeResultPartitionManifest] = []
    content_hasher = _ResultContentHasher()
    compressed_bytes = 0
    decoded_bytes = 0
    for ordinal, capture in enumerate(captures, start=1):
        receipt = _capture_receipt(
            capture,
            ordinal=ordinal,
            source_snapshot_token=source_snapshot_token,
        )
        capture_manifest = capture.manifest
        if (
            compressed_bytes + capture_manifest.compressed_bytes > capture_limits.maximum_compressed_bytes
            or decoded_bytes + capture_manifest.decoded_bytes > capture_limits.maximum_decoded_bytes
        ):
            raise SnowflakeConnectorError("result captures exceed the total-byte limit")
        partition_limits = _remaining_partition_limits(
            capture_limits,
            compressed_bytes=compressed_bytes,
            decoded_bytes=decoded_bytes,
        )
        try:
            verify_capture(capture, SNOWFLAKE_RESULT_STREAM, limits=partition_limits)
        except CaptureError as exc:
            raise SnowflakeConnectorError("result capture cannot be replayed within the byte limits") from exc
        compressed_bytes += capture_manifest.compressed_bytes
        decoded_bytes += capture_manifest.decoded_bytes
        receipts.append(receipt)
        content_hasher.add(receipt)
    return tuple(receipts), content_hasher.hexdigest()
