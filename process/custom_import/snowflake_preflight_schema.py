# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure validation of generated Snowflake preflight cursor metadata."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from decimal import ROUND_HALF_EVEN, Context, Decimal, DivisionByZero, InvalidOperation, Overflow
from types import MappingProxyType

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import is_decimal_scalar_storage_valid, normalize_source_decimal
from process.custom_import.snowflake import MAX_APPROVED_RELATIONS, MAX_SELECTED_COLUMNS, SnowflakeConnectorError

_SUPPORTED_SOURCE_TYPES = frozenset({"BOOLEAN", "FIXED", "TEXT"})
_FIXED_SOURCE_TYPE = re.compile(r"^FIXED\(([1-9]|[1-2][0-9]|3[0-8]),([0-9]|[1-2][0-9]|3[0-7])\)$")
_CONVERSION_FIELD = re.compile(r"^[a-z][a-z0-9_]{0,62}$", flags=re.ASCII)
FLOAT_DECIMAL_CONVERSION = "float64_round_half_even_12"
_DECIMAL_LIMIT = Decimal("999999999999999999.999999999999")


def normalize_decimal_conversions(
    value: object, definition: CustomImportDefinition | None = None
) -> Mapping[str, str] | None:
    """Seal a closed, opt-in conversion map without changing legacy scalar semantics."""

    if value is None:
        return None
    if not isinstance(value, Mapping) or not 1 <= len(value) <= MAX_APPROVED_RELATIONS * MAX_SELECTED_COLUMNS:
        raise SnowflakeConnectorError("Snowflake decimal conversions must be a non-empty field map")
    if any(
        not isinstance(key, str) or _CONVERSION_FIELD.fullmatch(key) is None or mode != FLOAT_DECIMAL_CONVERSION
        for key, mode in value.items()
    ):
        raise SnowflakeConnectorError("Snowflake decimal conversion is unsupported")
    if definition is not None:
        identity_fields = {definition.entity_field, *definition.root_logical_key}
        for collection in definition.child_collections:
            identity_fields.update(collection.child_key)
            identity_fields.update(part.child_field for part in collection.parent_key)
            identity_fields.update(part.root_field for part in collection.parent_key)
        for profile in definition.selection_profiles:
            identity_fields.update(profile.context_dimensions)
        fields = definition.fields_by_id
        if any(key not in fields or fields[key].value_type != "decimal" or key in identity_fields for key in value):
            raise SnowflakeConnectorError("Snowflake decimal conversions require declared non-key decimal fields")
    return MappingProxyType(dict(sorted(value.items())))


def validate_decimal_conversion_sources(columns: Sequence[object], conversions: Mapping[str, str] | None) -> None:
    """Keep observed REAL metadata distinct from its explicitly converted transport."""

    configured = conversions or {}
    if any((column.source_type == "REAL") != (column.field_id in configured) for column in columns):
        raise SnowflakeConnectorError("Snowflake REAL source requires its declared decimal conversion")


def convert_snowflake_float(value: object) -> Decimal | None:
    """Round the exact binary64 value once, independently of ambient decimal context."""

    if value is None:
        return None
    if type(value) is not float:
        raise SnowflakeConnectorError("Snowflake REAL result must be a finite float")
    exact = Decimal.from_float(value)
    if not exact.is_finite() or exact.copy_abs() > _DECIMAL_LIMIT:
        raise SnowflakeConnectorError("Snowflake REAL result exceeds decimal storage bounds")
    context = Context(
        prec=31,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        traps=[InvalidOperation, DivisionByZero, Overflow],
    )
    rounded = exact.quantize(Decimal("0.000000000001"), context=context)
    normalized = normalize_source_decimal(rounded)
    if normalized is None or not is_decimal_scalar_storage_valid(normalized):
        raise SnowflakeConnectorError("Snowflake converted result exceeds decimal storage bounds")
    return normalized


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
    conversions = (
        getattr(getattr(getattr(statement, "bundle_statement", None), "request", None), "decimal_conversions", None)
        or {}
    )
    if (
        not all(_is_integral_fixed(source_types[index]) for index in (0, 1, 3, 5))
        or any(source_types[index] != "TEXT" for index in (2, 4))
        or any(
            not (
                field.value_type == "decimal" and source_type == "REAL"
                if field.field_id in conversions
                else _supports_preflight_field_type(field.value_type, source_type)
            )
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
        if normalized in {"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT4", "FLOAT8"}:
            return "REAL"
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
