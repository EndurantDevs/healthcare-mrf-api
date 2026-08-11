# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add deterministic formatted-address display metadata.

Revision ID: 20260811110000_address_formatted_display
Revises: 20260811100000_address_numeric_grid_alias
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260811110000_address_formatted_display"
down_revision = "20260811100000_address_numeric_grid_alias"
branch_labels = None
depends_on = None

_DISPLAY_SPACE_CODEPOINTS = (
    160,
    8239,
    8201,
    8202,
    8199,
    8198,
    8197,
    8196,
    8195,
    8194,
    8193,
    8192,
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _formatted_address_function_sql(schema: str) -> str:
    function = f"{_q(schema)}.{_q('addr_formatted_address_v1')}"
    translated_spaces = " || ".join(
        f"chr({codepoint})" for codepoint in _DISPLAY_SPACE_CODEPOINTS
    )
    return f"""
    CREATE OR REPLACE FUNCTION {function}(
        first_line text,
        second_line text,
        city_name text,
        state_name text,
        postal_code text,
        country_code text
    )
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    PARALLEL SAFE
    AS $function$
        WITH cleaned AS (
            SELECT
                NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(first_line, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '') AS line_one,
                NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(second_line, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '') AS line_two,
                NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(city_name, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '') AS city_value,
                NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(state_name, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '') AS state_value,
                NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(postal_code, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '') AS postal_value,
                upper(NULLIF(btrim(regexp_replace(
                    translate(
                        normalize(COALESCE(country_code, ''), NFC),
                        {translated_spaces},
                        '            '
                    ),
                    '[[:space:]]+', ' ', 'g'
                )), '')) AS country_value
        ), deduplicated AS (
            SELECT
                line_one,
                CASE
                    WHEN line_one = line_two THEN NULL
                    WHEN line_one IS NOT NULL
                         AND line_two IS NOT NULL
                         AND char_length(line_one) > char_length(line_two)
                         AND right(line_one, char_length(line_two)) = line_two
                         AND substring(
                             line_one
                             FROM char_length(line_one) - char_length(line_two)
                             FOR 1
                         ) IN (' ', ',', ';')
                    THEN NULL
                    ELSE line_two
                END AS line_two,
                city_value,
                state_value,
                postal_value,
                country_value
            FROM cleaned
        ), normalized AS (
            SELECT
                line_one,
                line_two,
                city_value,
                state_value,
                CASE
                    WHEN (country_value IS NULL OR country_value = 'US')
                         AND postal_value ~ '^[0-9]{{9}}$'
                    THEN substring(postal_value FROM 1 FOR 5)
                         || '-' || substring(postal_value FROM 6 FOR 4)
                    WHEN (country_value IS NULL OR country_value = 'US')
                         AND postal_value ~ '^[0-9]{{5}}[- ][0-9]{{4}}$'
                    THEN substring(postal_value FROM 1 FOR 5)
                         || '-' || substring(postal_value FROM 7 FOR 4)
                    ELSE postal_value
                END AS postal_value,
                country_value
            FROM deduplicated
        ), assembled AS (
            SELECT
                line_one,
                line_two,
                NULLIF(concat_ws(
                    ', ',
                    city_value,
                    NULLIF(concat_ws(' ', state_value, postal_value), '')
                ), '') AS locality_value,
                CASE
                    WHEN country_value IS NULL OR country_value = 'US'
                    THEN NULL
                    ELSE country_value
                END AS displayed_country
            FROM normalized
        ), result AS (
            SELECT NULLIF(concat_ws(
                ', ',
                line_one,
                line_two,
                locality_value,
                displayed_country
            ), '') AS rendered
            FROM assembled
        )
        SELECT CASE
            WHEN rendered IS NULL THEN NULL
            WHEN char_length(rendered) > 1024 THEN
                NULLIF(rtrim(
                    substring(rendered FROM 1 FOR 1024),
                    E' \t\n\r,;'
                ), '')
            ELSE rendered
        END
        FROM result
    $function$;
    """


def _metadata_columns_sql(schema: str, table: str) -> str:
    return f"""
    ALTER TABLE IF EXISTS {_qt(schema, table)}
        ADD COLUMN IF NOT EXISTS formatted_address_version smallint,
        ADD COLUMN IF NOT EXISTS formatted_address_source varchar(32);
    """


def _overlay_columns_sql(schema: str) -> str:
    return f"""
    ALTER TABLE IF EXISTS {_qt(schema, 'provider_directory_address_overlay')}
        ADD COLUMN IF NOT EXISTS formatted_address text,
        ADD COLUMN IF NOT EXISTS formatted_address_version smallint,
        ADD COLUMN IF NOT EXISTS formatted_address_source varchar(32);
    """


def upgrade() -> None:
    schema = _schema()
    op.execute(_formatted_address_function_sql(schema))
    op.execute(_metadata_columns_sql(schema, "address_archive_v2"))
    op.execute(_metadata_columns_sql(schema, "entity_address_unified"))
    op.execute(_overlay_columns_sql(schema))


def downgrade() -> None:
    schema = _schema()
    for table in ("address_archive_v2", "entity_address_unified"):
        op.execute(
            f"""
            ALTER TABLE IF EXISTS {_qt(schema, table)}
                DROP COLUMN IF EXISTS formatted_address_version,
                DROP COLUMN IF EXISTS formatted_address_source;
            """
        )
    op.execute(
        f"""
        ALTER TABLE IF EXISTS {_qt(schema, 'provider_directory_address_overlay')}
            DROP COLUMN IF EXISTS formatted_address,
            DROP COLUMN IF EXISTS formatted_address_version,
            DROP COLUMN IF EXISTS formatted_address_source;
        """
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('addr_formatted_address_v1')}"
        f"(text, text, text, text, text, text);"
    )
