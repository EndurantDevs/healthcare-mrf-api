# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Internal source-text contracts shared by custom-import entry points."""

from __future__ import annotations

from typing import Any, Final

_MAX_SNAPSHOT_TOKEN_BYTES: Final = 1024
_MAX_SOURCE_LABEL_BYTES: Final = 255


class _SourceTextValidationError(ValueError):
    """Internal reason code for mapping a text-contract failure at each boundary."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def validate_snapshot_token(value: Any) -> str:
    """Require a retained source snapshot token to be bounded, valid, and printable."""

    if not isinstance(value, str):
        raise _SourceTextValidationError("not_string")
    if not value:
        raise _SourceTextValidationError("empty")
    return _validate_printable_utf8(value, maximum_bytes=_MAX_SNAPSHOT_TOKEN_BYTES)


def validate_source_label(value: Any) -> str:
    """Require a deferred source label to be bounded, valid, and printable."""

    if not isinstance(value, str):
        raise _SourceTextValidationError("not_string")
    if not value:
        raise _SourceTextValidationError("empty")
    return _validate_printable_utf8(value, maximum_bytes=_MAX_SOURCE_LABEL_BYTES)


def _validate_printable_utf8(value: str, *, maximum_bytes: int) -> str:
    """Apply the common retained-text UTF-8 length and visibility contract."""

    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise _SourceTextValidationError("invalid_utf8") from exc
    if len(encoded) > maximum_bytes:
        raise _SourceTextValidationError("byte_limit")
    if not value.isprintable():
        raise _SourceTextValidationError("non_printable")
    return value
