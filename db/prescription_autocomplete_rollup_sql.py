# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL shared by prescription autocomplete migration and staged publication."""

from __future__ import annotations


_ROLLUP_INSERT_SQL = """
    WITH variants AS (
        SELECT
            year,
            rx_code_system,
            rx_code,
            rx_name,
            generic_name,
            brand_name,
            SUM(total_claims) AS total_claims,
            SUM(total_drug_cost) AS total_drug_cost,
            SUM(total_benes) AS total_benes
        FROM {source_relation}
        GROUP BY
            year,
            rx_code_system,
            rx_code,
            rx_name,
            generic_name,
            brand_name
    ),
    ranked AS (
        SELECT
            variants.*,
            ROW_NUMBER() OVER (
                PARTITION BY year, rx_code_system, rx_code
                ORDER BY
                    rx_name ASC NULLS FIRST,
                    generic_name ASC NULLS FIRST,
                    brand_name ASC NULLS FIRST
            )::bigint AS variant_id
        FROM variants
    )
    INSERT INTO {target_relation} (
        year,
        rx_code_system,
        rx_code,
        variant_id,
        rx_name,
        generic_name,
        brand_name,
        total_claims,
        total_drug_cost,
        total_benes,
        source_relation_fingerprint
    )
    SELECT
        year,
        rx_code_system,
        rx_code,
        variant_id,
        rx_name,
        generic_name,
        brand_name,
        total_claims,
        total_drug_cost,
        total_benes,
        {fingerprint}
    FROM ranked
"""


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table(schema: str, name: str) -> str:
    return f"{_quote(schema)}.{_quote(name)}"


def prescription_autocomplete_source_fingerprint_sql(
    *,
    schema: str,
    provider_table: str,
) -> str:
    """Identify the provider relation summarized by one rollup generation."""

    relation = _table(schema, provider_table).replace("'", "''")
    return f"COALESCE(to_regclass('{relation}')::oid::text, '0')"


def prescription_autocomplete_rollup_insert_sql(
    *,
    schema: str,
    rollup_table: str,
    provider_table: str,
) -> str:
    """Aggregate exact provider name variants into the autocomplete rollup."""

    source_relation = _table(schema, provider_table)
    target_relation = _table(schema, rollup_table)
    fingerprint = prescription_autocomplete_source_fingerprint_sql(
        schema=schema,
        provider_table=provider_table,
    )
    return _ROLLUP_INSERT_SQL.format(
        source_relation=source_relation,
        target_relation=target_relation,
        fingerprint=fingerprint,
    )
