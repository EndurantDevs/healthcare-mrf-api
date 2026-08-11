# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Audit-only SQL for reviewed numeric-grid alias discovery."""

from __future__ import annotations

import re

from process.ext import address_alias_sql


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_ident(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f'"{value}"'


def numeric_grid_skipped_source_count_sql(*, schema: str, archive: str) -> str:
    """Count structurally incomplete sources excluded by existing alias topology."""
    qschema = _quote_ident(schema)
    aliases = f'{qschema}."{address_alias_sql.ADDRESS_ALIAS_TABLE}"'
    return f"""
        WITH parsed_sources AS MATERIALIZED (
            SELECT
                archived.address_key,
                archived.source_bits,
                archived.state_code,
                archived.zip5,
                {qschema}.addr_numeric_grid_parts_v1(
                    archived.first_line,
                    archived.second_line
                ) AS parts
            FROM {archive} AS archived
            WHERE archived.address_key IS NOT NULL
              AND archived.identity_key IS NOT NULL
              AND COALESCE(
                    archived.precision,
                    split_part(archived.identity_key, '|', 8)
                  ) = 'street'
              AND archived.merged_into IS NULL
        )
        SELECT count(*)::bigint
        FROM parsed_sources AS source
        WHERE source.parts IS NOT NULL
          AND (
                (source.parts[2] = '' AND source.parts[4] <> '')
             OR (source.parts[2] <> '' AND source.parts[4] = '')
          )
          AND (
                CAST(:scope_state_code AS varchar) IS NULL
                OR source.state_code = CAST(:scope_state_code AS varchar)
          )
          AND (
                CAST(:scope_zip_prefix AS varchar) IS NULL
                OR source.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
          )
          AND (
                EXISTS (
                    SELECT 1
                    FROM {aliases} AS active
                    WHERE active.source_address_key = source.address_key
                      AND active.revoked_at IS NULL
                      AND (
                            CAST(:retry_shadow_run_id AS uuid) IS NULL
                            OR active.shadow_run_id <> CAST(:retry_shadow_run_id AS uuid)
                      )
                )
                OR EXISTS (
                    SELECT 1
                    FROM {aliases} AS upstream
                    WHERE upstream.target_address_key = source.address_key
                      AND upstream.revoked_at IS NULL
                )
          );
    """
