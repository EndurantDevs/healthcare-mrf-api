# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure validation of generated Snowflake preflight cursor metadata."""

from __future__ import annotations

import re
from collections.abc import Sequence

from process.custom_import.snowflake import SnowflakeConnectorError

_SUPPORTED_SOURCE_TYPES = frozenset({"BOOLEAN", "FIXED", "TEXT"})
_FIXED_SOURCE_TYPE = re.compile(r"^FIXED\(([1-9]|[1-2][0-9]|3[0-8]),([0-9]|[1-2][0-9]|3[0-7])\)$")


def validate_preflight_result_schema(
    statement: object,
    description: object,
    *,
    field_types: Sequence[object] = (),
) -> None:
    """Require actual cursor metadata to match one generated preflight statement.

    Hosts may inject their installed driver's field-type table when metadata
    provides numeric type codes instead of type names.  Missing or unsupported
    metadata fails closed.
    """

    try:
        column_ids = statement.column_ids
        fields = tuple(sorted(statement.definition.fields, key=lambda field: field.field_slot))
    except AttributeError as exc:
        raise SnowflakeConnectorError(
            "Snowflake preflight result schema does not match the generated statement"
        ) from exc
    if (
        not isinstance(column_ids, tuple)
        or not column_ids
        or not all(isinstance(column_id, str) and column_id for column_id in column_ids)
        or not isinstance(description, Sequence)
        or len(description) != len(column_ids)
        or tuple(getattr(metadata, "name", None) for metadata in description) != column_ids
    ):
        raise SnowflakeConnectorError("Snowflake preflight result schema does not match the generated statement")
    source_types = tuple(_source_type(metadata, field_types=field_types) for metadata in description)
    if (
        not all(_is_integral_fixed(source_types[index]) for index in (0, 1, 3, 5))
        or any(source_types[index] != "TEXT" for index in (2, 4))
        or any(
            not _supports_preflight_field_type(field.value_type, source_type)
            for field, source_type in zip(fields, source_types[6:], strict=True)
        )
    ):
        raise SnowflakeConnectorError("Snowflake preflight result schema does not match the generated statement")


def _is_integral_fixed(source_type: str) -> bool:
    return source_type.startswith("FIXED(") and _fixed_type_parts(source_type)[1] == 0


def _supports_preflight_field_type(value_type: str, source_type: str) -> bool:
    if value_type == "string":
        return source_type == "TEXT"
    if value_type == "integer":
        return _is_integral_fixed(source_type)
    if value_type == "decimal":
        return source_type.startswith("FIXED(")
    return value_type == "boolean" and source_type == "BOOLEAN"


def _source_type(metadata: object, *, field_types: Sequence[object] = ()) -> str:
    type_name = getattr(metadata, "type_name", None)
    type_code = getattr(metadata, "type_code", None)
    if (
        type_name is None
        and isinstance(type_code, int)
        and not isinstance(type_code, bool)
        and 0 <= type_code < len(field_types)
    ):
        type_name = getattr(field_types[type_code], "name", None)
    if isinstance(type_name, str) and type_name:
        normalized = type_name.upper()
        if normalized == "FIXED":
            precision = _metadata_integer(metadata, "precision", minimum=1, maximum=38)
            scale = _metadata_integer(metadata, "scale", minimum=0, maximum=min(precision, 37))
            return f"FIXED({precision},{scale})"
        if normalized in _SUPPORTED_SOURCE_TYPES:
            return normalized
        raise SnowflakeConnectorError("Snowflake result column type is not supported by the capture runtime")
    raise SnowflakeConnectorError("Snowflake result column type is unavailable")


def _metadata_integer(metadata: object, name: str, *, minimum: int, maximum: int) -> int:
    number = getattr(metadata, name, None)
    if isinstance(number, bool) or not isinstance(number, int) or not minimum <= number <= maximum:
        raise SnowflakeConnectorError(f"Snowflake result {name} is invalid")
    return number


def _fixed_type_parts(source_type: str) -> tuple[int, int]:
    fixed_match = _FIXED_SOURCE_TYPE.fullmatch(source_type)
    if fixed_match is None:
        raise SnowflakeConnectorError("Snowflake result column type is not supported by the Parquet encoder")
    return int(fixed_match.group(1)), int(fixed_match.group(2))
