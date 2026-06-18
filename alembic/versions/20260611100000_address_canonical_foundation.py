"""Add canonical address archive foundation"""

from __future__ import annotations

import os

from alembic import op

from process.ext.address_pub28 import (
    PUB28_DIRECTIONAL_MAP,
    PUB28_INVALID_UNIT_VALUES,
    PUB28_STATE_MAP,
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
    PUB28_UNIT_NO_RANGE,
)


revision = "20260611100000_address_canonical_foundation"
down_revision = "20260610143000_address_checksums_bigint"
branch_labels = None
depends_on = None


ADDRESS_KEY_TABLES = (
    "npi_address",
    "mrf_address",
    "mrf_address_evidence",
    "doctor_clinician_address",
    "provider_enrollment_ffs",
    "provider_enrollment_ffs_address",
    "facility_anchor",
    "pharmacy_license_record_stage_v1",
    "pharmacy_license_record_v1",
    "pharmacy_license_record_history_v1",
    "partd_pharmacy_activity_v2",
    "partd_pharmacy_activity_stage_v2",
    "entity_address_unified",
    "ptg_address",
    "entity_address_evidence",
)

ORDINAL_WORD_MAP = {
    "first": "1",
    "second": "2",
    "third": "3",
    "fourth": "4",
    "fifth": "5",
    "sixth": "6",
    "seventh": "7",
    "eighth": "8",
    "ninth": "9",
    "tenth": "10",
    "eleventh": "11",
    "twelfth": "12",
    "thirteenth": "13",
    "fourteenth": "14",
    "fifteenth": "15",
    "sixteenth": "16",
    "seventeenth": "17",
    "eighteenth": "18",
    "nineteenth": "19",
    "twentieth": "20",
    "thirtieth": "30",
}


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _schema_literal(schema: str) -> str:
    return schema.replace("'", "''")


def _q(schema: str, name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


def _exec_sql_batch(bind, sql: str) -> None:
    statements: list[str] = []
    start = 0
    index = 0
    in_dollar_quote = False
    while index < len(sql):
        if sql.startswith("$$", index):
            in_dollar_quote = not in_dollar_quote
            index += 2
            continue
        if sql[index] == ";" and not in_dollar_quote:
            statement = sql[start:index].strip()
            if statement:
                statements.append(statement)
            start = index + 1
        index += 1
    tail = sql[start:].strip()
    if tail:
        statements.append(tail)

    for statement in statements:
        bind.exec_driver_sql(statement)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _case_pairs(mapping: dict[str, str], *, indent: str = "        ") -> str:
    return "\n".join(
        f"{indent}WHEN {_sql_literal(key)} THEN {_sql_literal(value)}"
        for key, value in sorted(mapping.items())
    )


def _sql_in(values: set[str]) -> str:
    return ", ".join(_sql_literal(value) for value in sorted(values))


def _unit_regex() -> str:
    escaped = sorted((value.replace("'", "''") for value in PUB28_UNIT_DESIGNATOR_MAP), key=len, reverse=True)
    return "(" + "|".join(escaped) + ")"


def _create_functions_sql(schema: str) -> str:
    qschema = _quote_ident(schema)
    street_token_case = _case_pairs({**PUB28_STREET_SUFFIX_MAP, **PUB28_DIRECTIONAL_MAP}, indent="            ")
    ordinal_word_case = _case_pairs(ORDINAL_WORD_MAP, indent="            ")
    suffix_token_case = _case_pairs({key: "1" for key in PUB28_STREET_SUFFIX_MAP}, indent="            ")
    directional_token_case = _case_pairs({key: "1" for key in PUB28_DIRECTIONAL_MAP}, indent="            ")
    state_case = _case_pairs(PUB28_STATE_MAP)
    unit_case = _case_pairs(PUB28_UNIT_DESIGNATOR_MAP)
    unit_regex = _unit_regex()
    unit_no_range = _sql_in(PUB28_UNIT_NO_RANGE)
    invalid_unit_values = _sql_in(PUB28_INVALID_UNIT_VALUES)
    return rf"""
CREATE OR REPLACE FUNCTION {qschema}.addr_clean_alnum_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT NULLIF(regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g'), '')
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_space_norm_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT translate(
        COALESCE(value, ''),
        chr(160) || chr(8239) || chr(8201) || chr(8202) || chr(8199) ||
        chr(8198) || chr(8197) || chr(8196) || chr(8195) || chr(8194) ||
        chr(8193) || chr(8192),
        '            '
    )
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_zip5_norm_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH digits AS (
        SELECT regexp_replace(COALESCE(value, ''), '[^0-9]', '', 'g') AS v
    )
    SELECT CASE
        WHEN length(v) >= 5 THEN LEFT(v, 5)
        WHEN length(v) IN (3, 4) THEN lpad(v, 5, '0')
        ELSE NULL
    END
    FROM digits
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_country_code_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN NULLIF(upper(regexp_replace(COALESCE(value, ''), '[^A-Za-z]', '', 'g')), '') IS NULL THEN 'US'
        WHEN upper(regexp_replace(COALESCE(value, ''), '[^A-Za-z]', '', 'g')) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA') THEN 'US'
        ELSE upper(regexp_replace(COALESCE(value, ''), '[^A-Za-z]', '', 'g'))
    END
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_state_code_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT upper(regexp_replace(COALESCE(value, ''), '[^A-Za-z]', '', 'g')) AS v
    )
    SELECT CASE v
        WHEN '' THEN NULL
{state_case}
        ELSE NULL
    END
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_unit_prefix_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT CASE
            WHEN trim(COALESCE(value, '')) = '#' THEN '#'
            ELSE regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g')
        END AS v
    )
    SELECT CASE v
{unit_case}
        ELSE NULL
    END
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_unit_range_required_v1(prefix text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT COALESCE(prefix, '') NOT IN ({unit_no_range})
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_unit_value_valid_v1(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g') AS v
    )
    SELECT v <> '' AND v NOT IN ({invalid_unit_values})
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_unit_norm_v1(line1 text, line2 text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    m text[];
    prefix text;
    unit_value text;
BEGIN
    line1 := {qschema}.addr_space_norm_v1(line1);
    line2 := {qschema}.addr_space_norm_v1(line2);

    m := regexp_match(lower(COALESCE(line2, '')),
        '^\s*{unit_regex}\.?\s*(#\s*)?([a-z0-9][a-z0-9-]*)?[.,;:]*\s*$');
    IF m IS NOT NULL THEN
        prefix := {qschema}.addr_unit_prefix_v1(m[1]);
        unit_value := m[3];

        IF prefix IS NULL THEN
            RETURN '';
        END IF;

        unit_value := regexp_replace(lower(COALESCE(unit_value, '')), '[^a-z0-9]', '', 'g');
        IF unit_value = '' THEN
            IF NOT {qschema}.addr_unit_range_required_v1(prefix) THEN
                RETURN prefix;
            END IF;
            RETURN '';
        END IF;

        IF NOT {qschema}.addr_unit_value_valid_v1(unit_value) THEN
            RETURN '';
        END IF;

        RETURN prefix || unit_value;
    END IF;

    m := regexp_match(' ' || lower(COALESCE(line1, '')) || ' ' || lower(COALESCE(line2, '')) || ' ',
        '(^|[\s,]){unit_regex}\.?\s*(#\s*)?([a-z0-9][a-z0-9-]*)?[.,;:]*\s*$');
    IF m IS NOT NULL THEN
        prefix := {qschema}.addr_unit_prefix_v1(m[2]);
        unit_value := m[4];
    END IF;

    IF prefix IS NULL THEN
        RETURN '';
    END IF;

    unit_value := regexp_replace(lower(COALESCE(unit_value, '')), '[^a-z0-9]', '', 'g');
    IF unit_value = '' THEN
        IF trim(COALESCE(line2, '')) = '' AND NOT {qschema}.addr_unit_range_required_v1(prefix) THEN
            RETURN prefix;
        END IF;
        RETURN '';
    END IF;

    IF NOT {qschema}.addr_unit_value_valid_v1(unit_value) THEN
        RETURN '';
    END IF;

    RETURN prefix || unit_value;
END;
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_token_norm_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g') AS v
    )
    SELECT CASE
        WHEN v ~ '^0*[1-9][0-9]*(st|nd|rd|th)$'
            THEN regexp_replace(v, '^0*([1-9][0-9]*)(st|nd|rd|th)$', '\1')
        WHEN v = 'saint'
            THEN 'st'
        ELSE CASE v
{street_token_case}
            ELSE v
        END
    END
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_token_is_suffix_v1(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g') AS v
    )
    SELECT CASE v
{suffix_token_case}
        ELSE '0'
    END = '1'
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_token_is_directional_v1(value text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g') AS v
    )
    SELECT CASE v
{directional_token_case}
        ELSE '0'
    END = '1'
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_token_norm_context_v1(value text, next_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH cleaned AS (
        SELECT regexp_replace(lower(COALESCE(value, '')), '[^a-z0-9]', '', 'g') AS v
    )
    SELECT CASE
        WHEN {qschema}.addr_street_token_is_suffix_v1(next_value) THEN
            CASE
                WHEN v ~ '^0*[1-9][0-9]*h$'
                    THEN regexp_replace(v, '^0*([1-9][0-9]*)h$', '\1')
                ELSE CASE v
{ordinal_word_case}
                    ELSE {qschema}.addr_street_token_norm_v1(v)
                END
            END
        ELSE {qschema}.addr_street_token_norm_v1(v)
    END
    FROM cleaned
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_text_v1(line1 text, line2 text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    l1 text := lower({qschema}.addr_space_norm_v1(line1));
    l2 text := lower({qschema}.addr_space_norm_v1(line2));
    raw text;
    m text[];
    prefix text;
    unit_value text;
    normalized text;
BEGIN
    m := regexp_match(l2,
        '^\s*{unit_regex}\.?\s*(#\s*)?([a-z0-9][a-z0-9-]*)?[.,;:]*\s*$');
    IF m IS NOT NULL THEN
        prefix := {qschema}.addr_unit_prefix_v1(m[1]);
        unit_value := regexp_replace(lower(COALESCE(m[3], '')), '[^a-z0-9]', '', 'g');
        IF prefix IS NOT NULL AND (
            (unit_value <> '' AND {qschema}.addr_unit_value_valid_v1(unit_value))
            OR (unit_value = '' AND NOT {qschema}.addr_unit_range_required_v1(prefix))
        ) THEN
            raw := ' ' || l1 || ' ';
        ELSE
            raw := ' ' || l1 || ' ' || l2 || ' ';
        END IF;
    ELSE
        raw := ' ' || l1 || ' ' || l2 || ' ';
        m := regexp_match(raw,
            '(^|[\s,]){unit_regex}\.?\s*(#\s*)?([a-z0-9][a-z0-9-]*)?[.,;:]*\s*$');
        IF m IS NOT NULL THEN
            prefix := {qschema}.addr_unit_prefix_v1(m[2]);
            unit_value := regexp_replace(lower(COALESCE(m[4], '')), '[^a-z0-9]', '', 'g');
            IF prefix IS NOT NULL AND (
                (unit_value <> '' AND {qschema}.addr_unit_value_valid_v1(unit_value))
                OR (
                    unit_value = ''
                    AND trim(l2) = ''
                    AND NOT {qschema}.addr_unit_range_required_v1(prefix)
                )
            ) THEN
                raw := regexp_replace(raw,
                    '(^|[\s,]){unit_regex}\.?\s*(#\s*)?([a-z0-9][a-z0-9-]*)?[.,;:]*\s*$',
                    '',
                    '');
            END IF;
        END IF;
    END IF;

    IF raw IS NULL THEN
        raw := ' ' || l1 || ' ' || l2 || ' ';
    END IF;
    raw := regexp_replace(raw, '\mp\s*\.?\s*o\s*\.?\s*box\M', ' pobox ', 'gi');
    raw := regexp_replace(raw, '\mpob\M', ' pobox ', 'gi');
    RETURN raw;
END;
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_norm_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    context AS (
        SELECT token, ord, lead(token) OVER (ORDER BY ord) AS next_token
          FROM parts
    )
    SELECT NULLIF(
        string_agg({qschema}.addr_street_token_norm_context_v1(token, next_token), '' ORDER BY ord),
        ''
    )
    FROM context
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_suffix_token_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    last_part AS (
        SELECT token, count(*) OVER () AS token_count
          FROM parts
         ORDER BY ord DESC
         LIMIT 1
    )
    SELECT CASE
        WHEN token_count >= 2 AND {qschema}.addr_street_token_is_suffix_v1(token)
            THEN {qschema}.addr_street_token_norm_v1(token)
        ELSE NULL
    END
    FROM last_part
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_suffixless_norm_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    counted AS (
        SELECT
            token,
            ord,
            lead(token) OVER (ORDER BY ord) AS next_token,
            count(*) OVER () AS token_count,
            max(ord) OVER () AS last_ord
          FROM parts
    ),
    retained AS (
        SELECT token, ord, next_token
          FROM counted
         WHERE NOT (
            token_count >= 2
            AND ord = last_ord
            AND {qschema}.addr_street_token_is_suffix_v1(token)
         )
    )
    SELECT NULLIF(
        string_agg({qschema}.addr_street_token_norm_context_v1(token, next_token), '' ORDER BY ord),
        ''
    )
    FROM retained
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_direction_index_v1(line1 text, line2 text)
RETURNS integer
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    counted AS (
        SELECT token, ord, max(ord) OVER () AS last_ord
          FROM parts
    )
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM counted WHERE ord = 1 AND {qschema}.addr_street_token_is_directional_v1(token)
        ) THEN 1
        WHEN EXISTS (
            SELECT 1
              FROM counted first_part
              JOIN counted second_part ON second_part.ord = 2
             WHERE first_part.ord = 1
               AND regexp_replace(lower(first_part.token), '[^a-z0-9]', '', 'g') ~ '^[0-9]+[a-z]?$'
               AND {qschema}.addr_street_token_is_directional_v1(second_part.token)
        ) THEN 2
        WHEN EXISTS (
            SELECT 1 FROM counted WHERE ord = last_ord AND {qschema}.addr_street_token_is_directional_v1(token)
        ) THEN (SELECT max(ord)::integer FROM counted)
        ELSE NULL
    END
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_direction_token_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH direction AS (
        SELECT {qschema}.addr_street_direction_index_v1(line1, line2) AS direction_ord
    ),
    raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    )
    SELECT {qschema}.addr_street_token_norm_v1(parts.token)
      FROM parts, direction
     WHERE parts.ord = direction.direction_ord
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_directionless_norm_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH direction AS (
        SELECT {qschema}.addr_street_direction_index_v1(line1, line2) AS direction_ord
    ),
    raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    retained AS (
        SELECT token, ord, lead(token) OVER (ORDER BY ord) AS next_token
          FROM parts, direction
         WHERE direction.direction_ord IS NULL OR parts.ord <> direction.direction_ord
    )
    SELECT NULLIF(
        string_agg({qschema}.addr_street_token_norm_context_v1(token, next_token), '' ORDER BY ord),
        ''
    )
    FROM retained
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_street_completion_norm_v1(line1 text, line2 text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    WITH direction AS (
        SELECT {qschema}.addr_street_direction_index_v1(line1, line2) AS direction_ord
    ),
    raw_parts AS (
        SELECT token, ord AS raw_ord
          FROM regexp_split_to_table({qschema}.addr_street_text_v1(line1, line2), '[^a-z0-9]+')
               WITH ORDINALITY AS raw(token, ord)
         WHERE token <> ''
    ),
    parts AS (
        SELECT token, row_number() OVER (ORDER BY raw_ord) AS ord
          FROM raw_parts
    ),
    marked AS (
        SELECT
            parts.token,
            parts.ord,
            lead(parts.token) OVER (ORDER BY parts.ord) AS next_token,
            count(*) FILTER (WHERE direction.direction_ord IS NULL OR parts.ord <> direction.direction_ord)
                OVER () AS retained_count,
            max(parts.ord) FILTER (WHERE direction.direction_ord IS NULL OR parts.ord <> direction.direction_ord)
                OVER () AS retained_last_ord,
            direction.direction_ord
          FROM parts, direction
    ),
    retained AS (
        SELECT token, ord, next_token
          FROM marked
         WHERE (direction_ord IS NULL OR ord <> direction_ord)
           AND NOT (
                retained_count >= 2
                AND ord = retained_last_ord
                AND {qschema}.addr_street_token_is_suffix_v1(token)
           )
    )
    SELECT NULLIF(
        string_agg({qschema}.addr_street_token_norm_context_v1(token, next_token), '' ORDER BY ord),
        ''
    )
    FROM retained
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_city_norm_v1(value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT {qschema}.addr_clean_alnum_v1(value)
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_identity_key_v1(
    first_line text,
    second_line text,
    city text,
    state text,
    zip text,
    country text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    street_norm text := {qschema}.addr_street_norm_v1(first_line, second_line);
    unit_norm text := {qschema}.addr_unit_norm_v1(first_line, second_line);
    city_norm text := {qschema}.addr_city_norm_v1(city);
    state_code text := {qschema}.addr_state_code_v1(state);
    zip5 text := {qschema}.addr_zip5_norm_v1(zip);
    country_code text := {qschema}.addr_country_code_v1(country);
    precision_value text;
    identity_city text := COALESCE(city_norm, '');
BEGIN
    IF country_code <> 'US' OR state_code IS NULL OR zip5 IS NULL THEN
        RETURN NULL;
    END IF;

    IF street_norm IS NOT NULL THEN
        precision_value := 'street';
        identity_city := '';
    ELSIF city_norm IS NOT NULL THEN
        precision_value := 'city_zip';
    ELSE
        RETURN NULL;
    END IF;

    RETURN concat_ws('|',
        'v2',
        COALESCE(street_norm, ''),
        COALESCE(unit_norm, ''),
        identity_city,
        state_code,
        zip5,
        country_code,
        precision_value
    );
END;
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_key_from_identity_v1(identity_key text)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN identity_key IS NULL
        THEN NULL
        ELSE encode(substr(sha256(convert_to(identity_key, 'UTF8')), 1, 16), 'hex')::uuid
    END
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_key_v1(
    first_line text,
    second_line text,
    city text,
    state text,
    zip text,
    country text
)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT {qschema}.addr_key_from_identity_v1(
        {qschema}.addr_identity_key_v1(first_line, second_line, city, state, zip, country)
    )
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_premise_identity_key_v1(
    first_line text,
    second_line text,
    city text,
    state text,
    zip text,
    country text
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    street_norm text := {qschema}.addr_street_norm_v1(first_line, second_line);
    state_code text := {qschema}.addr_state_code_v1(state);
    zip5 text := {qschema}.addr_zip5_norm_v1(zip);
    country_code text := {qschema}.addr_country_code_v1(country);
BEGIN
    IF country_code <> 'US' OR street_norm IS NULL OR state_code IS NULL OR zip5 IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN concat_ws('|', 'v2', street_norm, '', '', state_code, zip5, country_code, 'street');
END;
$$;

CREATE OR REPLACE FUNCTION {qschema}.addr_premise_key_v1(
    first_line text,
    second_line text,
    city text,
    state text,
    zip text,
    country text
)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT {qschema}.addr_key_from_identity_v1(
        {qschema}.addr_premise_identity_key_v1(first_line, second_line, city, state, zip, country)
    )
$$;
"""


def _create_archive_sql(schema: str) -> str:
    archive = _q(schema, "address_archive_v2")
    checksum_map = _q(schema, "address_checksum_map")
    checksum_collision = _q(schema, "address_checksum_collision")
    return f"""
CREATE TABLE IF NOT EXISTS {archive} (
    address_key uuid PRIMARY KEY,
    identity_key text NOT NULL UNIQUE,
    identity_version smallint NOT NULL DEFAULT 2,
    precision text NOT NULL DEFAULT 'street'
        CHECK (precision IN ('street', 'city_zip')),
    premise_key uuid,
    line1_norm text,
    unit_norm text NOT NULL DEFAULT '',
    city_norm text,
    state_code varchar(32),
    zip5 varchar(5),
    zip4 varchar(4),
    country_code varchar(8) NOT NULL DEFAULT 'US',
    first_line text,
    second_line text,
    city_name text,
    state_name text,
    postal_code text,
    telephone_number text,
    fax_number text,
    formatted_address text,
    lat numeric(11,8),
    long numeric(11,8),
    place_id text,
    geocode_source text,
    geocode_quality text,
    postal_validation_status text,
    geocoded_at timestamptz,
    source_bits integer NOT NULL DEFAULT 0,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    date_added date,
    display_priority smallint NOT NULL DEFAULT 9,
    merged_into uuid REFERENCES {archive} (address_key)
) WITH (fillfactor = 90);

CREATE INDEX IF NOT EXISTS address_archive_v2_zip5_line1_norm_idx
    ON {archive} (zip5, line1_norm);
CREATE INDEX IF NOT EXISTS address_archive_v2_state_city_idx
    ON {archive} (state_code, city_norm);
CREATE INDEX IF NOT EXISTS address_archive_v2_premise_key_idx
    ON {archive} (premise_key)
    WHERE premise_key IS NOT NULL;

DO $$
BEGIN
    IF to_regtype('public.geography') IS NOT NULL
       AND to_regprocedure('public.st_makepoint(double precision, double precision)') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS address_archive_v2_geo_idx
            ON {archive}
            USING gist (public.Geography(public.ST_MakePoint((long)::double precision, (lat)::double precision)))
            WHERE lat IS NOT NULL AND long IS NOT NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS {checksum_map} (
    checksum bigint NOT NULL,
    address_key uuid NOT NULL REFERENCES {archive} (address_key),
    PRIMARY KEY (checksum)
);

CREATE TABLE IF NOT EXISTS {checksum_collision} (
    checksum bigint NOT NULL,
    address_key uuid NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (checksum, address_key)
);
"""


def _add_address_key_sql(schema: str, table: str) -> str:
    schema_lit = _schema_literal(schema)
    table_lit = table.replace("'", "''")
    qtable = _q(schema, table)
    return f"""
DO $$
BEGIN
    IF to_regclass('{schema_lit}.{table_lit}') IS NOT NULL THEN
        ALTER TABLE {qtable} ADD COLUMN IF NOT EXISTS address_key uuid;
    END IF;
END $$;
"""


def upgrade() -> None:
    schema = _schema()
    bind = op.get_bind()
    bind.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")
    _exec_sql_batch(bind, _create_functions_sql(schema))
    _exec_sql_batch(bind, _create_archive_sql(schema))
    for table in ADDRESS_KEY_TABLES:
        _exec_sql_batch(bind, _add_address_key_sql(schema, table))


def downgrade() -> None:
    schema = _schema()
    bind = op.get_bind()
    for table in ADDRESS_KEY_TABLES:
        bind.exec_driver_sql(
            f'ALTER TABLE IF EXISTS {_q(schema, table)} DROP COLUMN IF EXISTS "address_key";'
        )
    bind.exec_driver_sql(f"DROP TABLE IF EXISTS {_q(schema, 'address_checksum_collision')};")
    bind.exec_driver_sql(f"DROP TABLE IF EXISTS {_q(schema, 'address_checksum_map')};")
    bind.exec_driver_sql(f"DROP TABLE IF EXISTS {_q(schema, 'address_archive_v2')};")
    qschema = _quote_ident(schema)
    for function_name, args in (
        ("addr_premise_key_v1", "text, text, text, text, text, text"),
        ("addr_premise_identity_key_v1", "text, text, text, text, text, text"),
        ("addr_key_v1", "text, text, text, text, text, text"),
        ("addr_key_from_identity_v1", "text"),
        ("addr_identity_key_v1", "text, text, text, text, text, text"),
        ("addr_city_norm_v1", "text"),
        ("addr_street_completion_norm_v1", "text, text"),
        ("addr_street_directionless_norm_v1", "text, text"),
        ("addr_street_direction_token_v1", "text, text"),
        ("addr_street_direction_index_v1", "text, text"),
        ("addr_street_suffixless_norm_v1", "text, text"),
        ("addr_street_suffix_token_v1", "text, text"),
        ("addr_street_norm_v1", "text, text"),
        ("addr_street_text_v1", "text, text"),
        ("addr_street_token_norm_context_v1", "text, text"),
        ("addr_street_token_is_directional_v1", "text"),
        ("addr_street_token_is_suffix_v1", "text"),
        ("addr_street_token_norm_v1", "text"),
        ("addr_unit_norm_v1", "text, text"),
        ("addr_unit_value_valid_v1", "text"),
        ("addr_unit_range_required_v1", "text"),
        ("addr_unit_prefix_v1", "text"),
        ("addr_state_code_v1", "text"),
        ("addr_country_code_v1", "text"),
        ("addr_zip5_norm_v1", "text"),
        ("addr_space_norm_v1", "text"),
        ("addr_clean_alnum_v1", "text"),
    ):
        bind.exec_driver_sql(f"DROP FUNCTION IF EXISTS {qschema}.{function_name}({args});")
