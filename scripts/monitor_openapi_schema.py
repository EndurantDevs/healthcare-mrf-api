#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema checks for bounded OpenAPI monitoring values."""

from __future__ import annotations

import re
import uuid
from datetime import date
from typing import Any


def smallest_pagination_value(
    configured_minimum: int, schema: dict[str, Any]
) -> int | float:
    """Return the smallest schema-valid pagination value."""
    return max(configured_minimum, schema.get("minimum", configured_minimum))


def validate_schema_value(
    candidate_value: Any, schema: dict[str, Any], field_name: str
) -> None:
    """Reject a configured monitoring value that violates its OpenAPI schema."""
    expected_type = schema.get("type")
    is_type_valid_by_name = {
        "array": isinstance(candidate_value, list),
        "boolean": isinstance(candidate_value, bool),
        "integer": isinstance(candidate_value, int)
        and not isinstance(candidate_value, bool),
        "number": isinstance(candidate_value, (int, float))
        and not isinstance(candidate_value, bool),
        "object": isinstance(candidate_value, dict),
        "string": isinstance(candidate_value, str),
    }
    if expected_type in is_type_valid_by_name and not is_type_valid_by_name[expected_type]:
        raise _schema_error(field_name)
    if "enum" in schema and candidate_value not in schema["enum"]:
        raise _schema_error(field_name)
    if isinstance(candidate_value, str):
        _validate_string(candidate_value, schema, field_name)
    if isinstance(candidate_value, (int, float)) and not isinstance(
        candidate_value, bool
    ):
        _validate_number(candidate_value, schema, field_name)
    _validate_children(candidate_value, schema, field_name)


def _validate_string(
    candidate_value: str, schema: dict[str, Any], field_name: str
) -> None:
    if len(candidate_value) < schema.get("minLength", 0) or len(
        candidate_value
    ) > schema.get("maxLength", len(candidate_value)):
        raise _schema_error(field_name)
    if schema.get("pattern") and re.search(
        str(schema["pattern"]), candidate_value
    ) is None:
        raise _schema_error(field_name)
    try:
        if schema.get("format") == "uuid":
            uuid.UUID(candidate_value)
        elif schema.get("format") == "date":
            date.fromisoformat(candidate_value)
    except ValueError as exc:
        raise _schema_error(field_name) from exc


def _validate_number(
    candidate_value: int | float, schema: dict[str, Any], field_name: str
) -> None:
    if candidate_value < schema.get(
        "minimum", candidate_value
    ) or candidate_value > schema.get("maximum", candidate_value):
        raise _schema_error(field_name)


def _validate_children(
    candidate_value: Any, schema: dict[str, Any], field_name: str
) -> None:
    if isinstance(candidate_value, list) and schema.get("items"):
        for child_value in candidate_value:
            validate_schema_value(child_value, schema["items"], field_name)
    if isinstance(candidate_value, dict):
        required_keys = set(schema.get("required") or ())
        if required_keys - candidate_value.keys():
            raise _schema_error(field_name)
        for key, child_value in candidate_value.items():
            property_schema = (schema.get("properties") or {}).get(key)
            if property_schema:
                validate_schema_value(
                    child_value, property_schema, f"{field_name}.{key}"
                )


def _schema_error(field_name: str) -> ValueError:
    return ValueError(f"monitoring value for {field_name} does not satisfy its schema")


def query_text(candidate_value: Any) -> str:
    """Render one schema-valid query value."""
    if isinstance(candidate_value, bool):
        return "true" if candidate_value else "false"
    if isinstance(candidate_value, list):
        return ",".join(str(child_value) for child_value in candidate_value)
    return str(candidate_value)
