# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL snippet builders for PTG2 price payloads."""

from __future__ import annotations

from typing import Any


def _empty_price_array_sql(*, json_type: str = "json") -> str:
    is_jsonb = json_type == "jsonb"
    return "'[]'::jsonb" if is_jsonb else "CAST('[]' AS json)"


def _scalar_price_json_sql(alias: str = "ps", *, json_type: str = "json") -> str:
    is_jsonb = json_type == "jsonb"
    build_array = "jsonb_build_array" if is_jsonb else "json_build_array"
    build_object = "jsonb_build_object" if is_jsonb else "json_build_object"
    empty_array = _empty_price_array_sql(json_type=json_type)
    to_json_func = "to_jsonb" if is_jsonb else "to_json"
    return f"""
        CASE
            WHEN {alias}.negotiated_rate IS NULL THEN {empty_array}
            ELSE {build_array}(
                {build_object}(
                    'negotiated_type', {alias}.negotiated_type,
                    'negotiated_rate', {alias}.negotiated_rate,
                    'expiration_date', {alias}.expiration_date,
                    'service_code', COALESCE({to_json_func}({alias}.service_code), {empty_array}),
                    'billing_class', {alias}.billing_class,
                    'setting', {alias}.setting,
                    'billing_code_modifier', COALESCE({to_json_func}({alias}.billing_code_modifier), {empty_array}),
                    'additional_information', {alias}.additional_information
                )
            )
        END
    """


def _typed_price_json_sql(alias: str = "ps", *, json_type: str = "json") -> str:
    is_jsonb = json_type == "jsonb"
    canonical_payload = f"{alias}.canonical_payload::jsonb" if is_jsonb else f"{alias}.canonical_payload"
    return f"""
        COALESCE(
            {canonical_payload},
            {_scalar_price_json_sql(alias, json_type=json_type)}
        )
    """


def _normalized_price_json_sql(
    alias: str = "ps",
    *,
    payload_alias: str = "price_payload",
    json_type: str = "json",
) -> str:
    del alias
    payload = f"{payload_alias}.prices" if json_type == "jsonb" else f"{payload_alias}.prices::json"
    return f"COALESCE({payload}, {_empty_price_array_sql(json_type=json_type)})"


def _price_atom_payload_sql(alias: str = "pa", *, service_alias: str = "service_set", modifier_alias: str = "modifier_set") -> str:
    return f"""
        jsonb_build_object(
            'negotiated_type', {alias}.negotiated_type,
            'negotiated_rate',
                CASE
                    WHEN {alias}.negotiated_rate ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    THEN {alias}.negotiated_rate::numeric
                    ELSE NULL
                END,
            'expiration_date', {alias}.expiration_date,
            'service_code', COALESCE(to_jsonb({service_alias}.codes), '[]'::jsonb),
            'billing_class', {alias}.billing_class,
            'setting', {alias}.setting,
            'billing_code_modifier', COALESCE(to_jsonb({modifier_alias}.codes), '[]'::jsonb),
            'additional_information', {alias}.additional_information
        )
    """


def _normalized_price_join_sql(
    serving_tables: Any,
    *,
    rate_alias: str = "r",
    payload_alias: str = "price_payload",
    price_filter_sql: str = "",
) -> str:
    if not (serving_tables.price_atom_table and serving_tables.price_set_entry_table and serving_tables.price_code_set_table):
        return ""
    return f"""
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(
                {_price_atom_payload_sql("pa")}
                ORDER BY pse.price_atom_hash
            ) AS prices
              FROM {serving_tables.price_set_entry_table} pse
              JOIN {serving_tables.price_atom_table} pa
                ON pa.price_atom_hash = pse.price_atom_hash
              LEFT JOIN {serving_tables.price_code_set_table} service_set
                ON service_set.code_set_hash = pa.service_code_set_hash
              LEFT JOIN {serving_tables.price_code_set_table} modifier_set
                ON modifier_set.code_set_hash = pa.billing_code_modifier_set_hash
             WHERE pse.price_set_hash = {rate_alias}.price_set_hash
             {price_filter_sql}
        ) {payload_alias} ON TRUE
    """
