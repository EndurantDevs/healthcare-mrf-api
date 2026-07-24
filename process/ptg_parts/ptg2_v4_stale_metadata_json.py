# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lossless JSON-envelope inspection for stale PTG V4 metadata."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def canonical_json_digest(payload: Any) -> str:
    """Hash one complete JSON value in canonical form."""

    payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(payload_bytes).hexdigest()


def json_mapping(value: Any) -> dict[str, Any]:
    """View one driver-decoded JSON object without reparsing JSON strings."""

    return dict(value) if isinstance(value, Mapping) else {}


def is_non_object_json_envelope(
    row_by_field: Mapping[str, Any],
    field_name: str,
) -> bool:
    """Detect a stored non-null JSON array, scalar, or JSON null."""

    database_value = row_by_field.get(field_name)
    if isinstance(database_value, Mapping):
        return False
    if database_value is not None:
        return True
    return row_by_field.get(f"{field_name}_is_sql_null") is False


def database_json_digest(value: Any) -> str:
    """Hash one already-decoded database JSON value without type collapse."""

    return canonical_json_digest(value)


def named_json_value_digest(field_name: str, value: Any) -> str:
    """Bind a scalar observation to its reviewed field name."""

    return canonical_json_digest({field_name: value})


__all__ = [
    "canonical_json_digest",
    "database_json_digest",
    "is_non_object_json_envelope",
    "json_mapping",
    "named_json_value_digest",
]
