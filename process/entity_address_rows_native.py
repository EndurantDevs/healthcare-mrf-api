# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared native row preparation and SQL adapters for both address build paths."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import re
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path
from typing import Any

CONTRACT = "entity_address_rows.v1"
MAX_ROWS = 5_000
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_COPY_BYTES = 16 * 1024 * 1024
MAX_FIELD_BYTES = 1024 * 1024
TEXT_COLUMNS = (
    "entity_name",
    "entity_subtype",
    "type",
    "base_address_version",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "place_id",
)
CONTACT_COLUMNS = ("phone_number", "phone_extension", "fax_number_digits", "fax_extension")
NORMALIZED_COLUMNS = (
    "first_line_norm",
    "second_line_norm",
    "city_norm",
    "state_code",
    "zip5",
    "country_norm",
    "telephone_digits",
)
INPUT_COLUMNS = ("row_id", *TEXT_COLUMNS)
OUTPUT_COLUMNS = (*INPUT_COLUMNS, *CONTACT_COLUMNS, *NORMALIZED_COLUMNS, "native_case_safe", "native_contact_safe")
OUTPUT_TYPES = ("bigint", *("text" for _ in range(24)), "boolean", "boolean")


def native_preparation_module():
    """Select the same versioned implementation for identity capture and execution."""
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ModuleNotFoundError as error:
        if error.name != "ptg2_address_canon":
            raise
        return None
    if not callable(getattr(module, "entity_address_rows_version", None)):
        return None
    if module.entity_address_rows_version() != CONTRACT or not callable(
        getattr(module, "prepare_entity_address_rows", None)
    ):
        raise RuntimeError("native address row preparation contract differs")
    return module


def native_preparation_identity() -> dict[str, str]:
    """Bind actual compiled artifact bytes once per mapper capture, without local paths."""
    module = native_preparation_module()
    if module is None:
        return {"implementation": "sql", "contract": CONTRACT}
    export = module.prepare_entity_address_rows
    if not inspect.isbuiltin(export):
        raise RuntimeError("native address row preparation export is not compiled")
    extension = importlib.import_module(export.__module__)
    origin = getattr(extension, "__file__", None)
    if not isinstance(origin, str) or not origin.endswith(tuple(EXTENSION_SUFFIXES)):
        raise RuntimeError("native address row preparation artifact is unavailable")
    with Path(origin).open("rb") as artifact:
        digest = hashlib.file_digest(artifact, "sha256").hexdigest()
    return {"implementation": "native", "contract": CONTRACT, "artifact_sha256": digest}


def source_value_expressions(sql: Any, base_alias: str | None = "b") -> dict[str, str]:
    """Retain the existing source expressions, including their column collations."""
    prefix = "" if base_alias is None else _alias(base_alias) + "."
    expression_by_field = {field: f"NULLIF(TRIM({prefix}{field}), '')" for field in TEXT_COLUMNS}
    expression_by_field["type"] = f"COALESCE(NULLIF(TRIM({prefix}type), ''), 'primary')"
    expression_by_field["country_code"] = f"COALESCE(NULLIF(TRIM({prefix}country_code), ''), 'US')"
    for raw_field, number_field, extension_field in (
        ("telephone_number", "phone_number", "phone_extension"),
        ("fax_number", "fax_number_digits", "fax_extension"),
    ):
        expression_by_field[raw_field] = sql._nullish_text_expr(f"{prefix}{raw_field}")
        expression_by_field[number_field] = sql._canonical_contact_number_expr(
            f"{prefix}{raw_field}", f"{prefix}country_code"
        )
        expression_by_field[extension_field] = sql._contact_extension_expr(f"{prefix}{raw_field}")
    return {field: expression_by_field[field] for field in (*TEXT_COLUMNS, *CONTACT_COLUMNS)}


def sanitized_value_expressions(sql: Any, *, native_prepared: bool = False) -> dict[str, str]:
    """Select already prepared values in both builders without repeating normalization."""
    if native_prepared:
        return {field: field for field in (*TEXT_COLUMNS, *CONTACT_COLUMNS)}
    return source_value_expressions(sql, None)


def prepare_entity_address_rows(input_bytes: bytes) -> tuple[bytes, int]:
    """Prepare one bounded COPY batch, retaining no expanded Python row objects."""
    if type(input_bytes) is not bytes or len(input_bytes) > MAX_INPUT_BYTES:
        raise ValueError("address row input must be bounded COPY bytes")
    module = native_preparation_module()
    if module is None:
        raise RuntimeError("native address row preparation contract differs")
    result = module.prepare_entity_address_rows(input_bytes)
    if (
        type(result) is not tuple
        or len(result) != 2
        or type(result[0]) is not bytes
        or not 21 <= len(result[0]) <= MAX_COPY_BYTES
        or type(result[1]) is not int
        or not 0 <= result[1] <= MAX_ROWS
    ):
        raise RuntimeError("native address row preparation result differs")
    return result


def _alias(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[a-z_][a-z0-9_]*", value) is None:
        raise ValueError("address row SQL alias differs")
    return value


def input_projection_sql(base_alias: str = "b", *, include_base_address_version: bool = True) -> str:
    """Export the fixed native schema from an already materialized candidate."""
    base = _alias(base_alias)
    return ", ".join(
        [f"{base}.row_id"]
        + [
            "NULL::text"
            if field == "base_address_version" and not include_base_address_version
            else f"{base}.{field}::text"
            for field in TEXT_COLUMNS
        ]
    )


def prepared_value_expressions(
    sql: Any,
    base_alias: str = "b",
    prepared_alias: str = "p",
    *,
    case_qualified: bool = False,
    contact_qualified: bool = False,
) -> dict[str, str]:
    """Use native ASCII results and retain database locale rules for Unicode rows."""
    base, prepared = _alias(base_alias), _alias(prepared_alias)
    if type(case_qualified) is not bool or type(contact_qualified) is not bool:
        raise ValueError("address row collation qualification differs")
    # ASCII case conversion still depends on the actual database column collation.
    case_condition = f"{prepared}.native_case_safe" if case_qualified else "FALSE"
    contact_condition = f"{prepared}.native_contact_safe" if contact_qualified else "FALSE"
    expression_by_field = {field: f"{prepared}.{field}" for field in TEXT_COLUMNS}
    for raw_field in ("telephone_number", "fax_number"):
        expression_by_field[raw_field] = (
            f"CASE WHEN {contact_condition} THEN {prepared}.{raw_field} "
            f"ELSE {sql._nullish_text_expr(f'{base}.{raw_field}')} END"
        )
    for raw_field, number_field, extension_field in (
        ("telephone_number", "phone_number", "phone_extension"),
        ("fax_number", "fax_number_digits", "fax_extension"),
    ):
        expression_by_field[number_field] = (
            f"CASE WHEN {contact_condition} THEN {prepared}.{number_field} "
            f"ELSE {sql._canonical_contact_number_expr(f'{base}.{raw_field}', f'{base}.country_code')} END"
        )
        expression_by_field[extension_field] = (
            f"CASE WHEN {contact_condition} THEN {prepared}.{extension_field} "
            f"ELSE {sql._contact_extension_expr(f'{base}.{raw_field}')} END"
        )
    for normalized, raw_field, normalizer in (
        ("first_line_norm", "first_line", sql._alnum_norm_expr),
        ("second_line_norm", "second_line", sql._alnum_norm_expr),
        ("city_norm", "city_name", sql._alnum_norm_expr),
        ("state_code", "state_name", sql._state_norm_expr),
        ("country_norm", "country_code", sql._state_norm_expr),
    ):
        source_expression = f"NULLIF(TRIM({base}.{raw_field}), '')"
        if raw_field == "country_code":
            source_expression = f"COALESCE({source_expression}, 'US')"
        expression_by_field[normalized] = (
            f"CASE WHEN {case_condition} THEN {prepared}.{normalized} ELSE {normalizer(source_expression)} END"
        )
    expression_by_field.update(
        zip5=f"{prepared}.zip5" if case_qualified else sql._zip5_norm_expr(f"{base}.postal_code"),
        telephone_digits=f"{prepared}.telephone_digits"
        if case_qualified
        else sql._phone_norm_expr(sql._nullish_text_expr(f"{base}.telephone_number")),
    )
    return {field: expression_by_field[field] for field in (*TEXT_COLUMNS, *CONTACT_COLUMNS, *NORMALIZED_COLUMNS)}


def direct_location_key_sql(sql: Any, *, native_prepared: bool = False) -> str:
    """Build the direct-materialization location key expression."""

    return sql._location_key_expr(
        {
            "entity_type": "entity_type",
            "entity_id": "entity_id",
            "npi": "npi",
            "inferred_npi": "inferred_npi",
            "address_role_id": sql._address_role_id_expr("type"),
            "row_origin": "'base'",
            "address_key": "address_key",
            "source_id": sql._source_id_expr("address_source"),
            "source_record_id": "source_record_id",
            "zip5": "prepared_zip5" if native_prepared else sql._zip5_norm_expr("postal_code"),
            "state_code": "prepared_state_code" if native_prepared else sql._state_norm_expr("state_name"),
            "city_norm": "prepared_city_norm" if native_prepared else sql._alnum_norm_expr("city_name"),
        }
    )


def address_checksum_sql(sql: Any, *, native_prepared: bool = False) -> str:
    """Build the direct-materialization address checksum expression."""

    return sql._address_checksum_expr(
        {
            "first_line": "prepared_first_line_norm" if native_prepared else sql._alnum_norm_expr("first_line"),
            "second_line": "prepared_second_line_norm" if native_prepared else sql._alnum_norm_expr("second_line"),
            "city_name": "prepared_city_norm" if native_prepared else sql._alnum_norm_expr("city_name"),
            "state_name": "prepared_state_code" if native_prepared else sql._state_norm_expr("state_name"),
            "postal_code": "prepared_zip5" if native_prepared else sql._zip5_norm_expr("postal_code"),
            "country_code": "prepared_country_norm" if native_prepared else sql._state_norm_expr("country_code"),
            "telephone_number": "prepared_telephone_digits"
            if native_prepared
            else sql._phone_norm_expr("telephone_number"),
        }
    )


def prepared_component_projection_sql() -> str:
    """Carry shared normalized components only through their immediate consumer."""
    return "".join(f",\n            prepared_{field}" for field in NORMALIZED_COLUMNS)


def direct_normalized_cte_sql(sql: Any, *, native_prepared: bool = False) -> str:
    """Build direct normalized location and checksum fields."""

    return f"""
    normalized AS (
        SELECT
            entity_type,
            entity_id,
            npi,
            inferred_npi,
            inference_confidence,
            inference_method,
            entity_name,
            entity_subtype,
            type,
            taxonomy_array,
            plans_network_array,
            procedures_array,
            medications_array,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension,
            lat,
            long,
            date_added,
            place_id,
            source_priority,
            address_source,
            source_record_id,
            updated_at,
            address_key,
            {sql._direct_materialize_location_key_sql(native_prepared=native_prepared)} AS location_key,
            {sql._direct_materialize_checksum_sql(native_prepared=native_prepared)} AS checksum
          FROM sanitized
    ),"""
