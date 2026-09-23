# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared bounded contracts for generic custom-import extension reads."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

READ_CORE_CONTRACT = "custom-import-read-core/v1"
MAX_FILTER_TERMS = 3
MAX_ORDER_TERMS = 3
MAX_PAGE_SIZE = 100
MAX_NPI_PAGE_SIZE = 200
MAX_PAGE_OFFSET = 1_000_000
MAX_CURSOR_TTL_SECONDS = 900
MAX_DETAIL_CHILDREN = 1_000
DEFAULT_READ_TIMEOUT_MS = 10_000
MAX_READ_TIMEOUT_MS = 30_000

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$", flags=re.ASCII)
_OPAQUE_VALUE = re.compile(r"^[A-Za-z0-9._~-]{1,512}$", flags=re.ASCII)
_SCOPE_VALUE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$", flags=re.ASCII)


class CustomImportReadError(ValueError):
    """Base error for a safe, value-free custom-import read failure."""


class CustomImportReadAuthorizationError(CustomImportReadError):
    """The caller did not present a verified extension-read authorization."""


class CustomImportReadCursorError(CustomImportReadError):
    """The page cursor is malformed, stale, or bound to another request."""


class CustomImportReadRequestError(CustomImportReadError):
    """The requested bounded read shape is not permitted by the definition."""


class CustomImportReadUnavailableError(CustomImportReadError):
    """The exact pinned materialization is incomplete or not eligible to serve."""


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or not 0 < value < 2**63:
        raise CustomImportReadRequestError(f"{label} must be a positive integer")
    return value


def _bounded_identifier(value: object, label: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise CustomImportReadRequestError(f"{label} must be a lower_snake_case identifier")
    return value


@dataclass(frozen=True, slots=True)
class PinnedReadTarget:
    """The immutable generation, schema, and selection profile to read."""

    dataset_id: int
    generation_id: int
    definition_revision_id: int
    schema_revision_id: int
    profile_id: str

    def __post_init__(self) -> None:
        _positive_integer(self.dataset_id, "dataset_id")
        _positive_integer(self.generation_id, "generation_id")
        _positive_integer(self.definition_revision_id, "definition_revision_id")
        _positive_integer(self.schema_revision_id, "schema_revision_id")
        _bounded_identifier(self.profile_id, "profile_id")


@dataclass(frozen=True, slots=True, repr=False)
class ExtensionReadAuthorization:
    """Opaque extension-specific material evaluated only by a host verifier."""

    credential: str

    def __post_init__(self) -> None:
        if type(self.credential) is not str or _OPAQUE_VALUE.fullmatch(self.credential) is None:
            raise CustomImportReadRequestError("extension read authorization is malformed")

    def __repr__(self) -> str:
        return "<extension-read-authorization>"


@dataclass(frozen=True, slots=True, repr=False)
class ExtensionReadScope:
    """A host-issued scope used to isolate cursors and cached read results."""

    value: str

    def __post_init__(self) -> None:
        if type(self.value) is not str or _SCOPE_VALUE.fullmatch(self.value) is None:
            raise CustomImportReadAuthorizationError("extension read scope is malformed")

    def __repr__(self) -> str:
        return "<extension-read-scope>"


class ExtensionReadAuthorizer(Protocol):
    """Host policy boundary for the distinct extension-read authorization value."""

    def authorize(
        self,
        authorization: ExtensionReadAuthorization,
        *,
        target: PinnedReadTarget,
    ) -> ExtensionReadScope | None:
        """Return an exact scope or ``None`` without exposing host policy details."""


class CustomImportReadCache(Protocol):
    """Optional async cache used only after extension authorization succeeds."""

    async def get(self, key: str) -> object | None:
        """Return one cached read result, if present."""

    async def set(self, key: str, value: object, *, expires_at: int) -> None:
        """Store one result until the supplied UNIX expiry time."""


def canonical_read_document(value: Mapping[str, object]) -> bytes:
    """Return deterministic ASCII JSON bytes for hash-bound internal read state."""

    return json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("ascii")


__all__ = (
    "CustomImportReadAuthorizationError",
    "CustomImportReadCache",
    "CustomImportReadCursorError",
    "CustomImportReadError",
    "CustomImportReadRequestError",
    "CustomImportReadUnavailableError",
    "DEFAULT_READ_TIMEOUT_MS",
    "ExtensionReadAuthorization",
    "ExtensionReadAuthorizer",
    "ExtensionReadScope",
    "MAX_CURSOR_TTL_SECONDS",
    "MAX_DETAIL_CHILDREN",
    "MAX_FILTER_TERMS",
    "MAX_NPI_PAGE_SIZE",
    "MAX_ORDER_TERMS",
    "MAX_PAGE_OFFSET",
    "MAX_PAGE_SIZE",
    "MAX_READ_TIMEOUT_MS",
    "PinnedReadTarget",
    "READ_CORE_CONTRACT",
    "canonical_read_document",
)
