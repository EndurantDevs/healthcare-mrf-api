# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database and JSON utilities for reviewed terminal disposition."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Mapping

from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    ReviewedSubsetTerminalDispositionError,
)


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def json_object(value: Any) -> dict[str, Any]:
    """Decode one exact JSON object from a driver or fake row."""

    if isinstance(value, Mapping):
        return dict(value)
    if type(value) is str:
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            raise ReviewedSubsetTerminalDispositionError("evidence") from None
        if isinstance(decoded, Mapping):
            return dict(decoded)
    raise ReviewedSubsetTerminalDispositionError("evidence")


def json_text_tuple(value: Any) -> tuple[str, ...]:
    """Decode one nonempty ordered JSON string vector."""

    if type(value) is str:
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            raise ReviewedSubsetTerminalDispositionError("evidence") from None
    if not isinstance(value, list) or not value or any(
        type(entry) is not str or not entry or entry != entry.strip()
        for entry in value
    ):
        raise ReviewedSubsetTerminalDispositionError("evidence")
    return tuple(value)


def row_mapping(row: object) -> dict[str, Any]:
    """Normalize one database row without accepting arbitrary objects."""

    if isinstance(row, Mapping):
        return dict(row)
    mapping = getattr(row, "_mapping", None)
    if isinstance(mapping, Mapping):
        return dict(mapping)
    raise ReviewedSubsetTerminalDispositionError("state")


def clean_text(value: object) -> str | None:
    """Return one nonempty already-trimmed string."""

    return value if type(value) is str and value and value == value.strip() else None


def schema_name() -> str:
    """Resolve one safe runtime schema without accepting conflicts."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise ReviewedSubsetTerminalDispositionError("state")
    selected_schema = runtime_schema or legacy_schema or "mrf"
    if _IDENTIFIER.fullmatch(selected_schema) is None:
        raise ReviewedSubsetTerminalDispositionError("state")
    return selected_schema


def quoted_relation(table_name: str) -> str:
    """Quote one allow-shaped relation in the resolved schema."""

    if _IDENTIFIER.fullmatch(table_name) is None:
        raise ReviewedSubsetTerminalDispositionError("state")
    return f'"{schema_name()}"."{table_name}"'


__all__ = (
    "clean_text",
    "json_object",
    "json_text_tuple",
    "quoted_relation",
    "row_mapping",
    "schema_name",
)
