# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Install the human-readable canonical formatted-address renderer.

Revision ID: 20260815010000_address_formatted_display_v2
Revises: 20260814010000_fhir_formulary_uhc_selected_receipt
"""

from __future__ import annotations

import os
import re

from alembic import op

from process.ext.address_format import ADDRESS_UNIT_PREFIX_DISPLAY
from process.ext.address_pub28 import PUB28_UNIT_DESIGNATOR_MAP


revision = "20260815010000_address_formatted_display_v2"
down_revision = "20260814010000_fhir_formulary_uhc_selected_receipt"
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


def _humanize_component_function_sql(schema: str) -> str:
    function = f"{_q(schema)}.{_q('addr_humanize_component_v2')}"
    unit_normalizer = f"{_q(schema)}.{_q('addr_unit_norm_v1')}"
    unit_stripper = f"{_q(schema)}.{_q('addr_strip_duplicate_tail_unit_v1')}"
    street_suffix_normalizer = (
        f"{_q(schema)}.{_q('addr_street_suffix_token_v1')}"
    )
    translated_spaces = " || ".join(
        f"chr({codepoint})" for codepoint in _DISPLAY_SPACE_CODEPOINTS
    )
    unit_designators = "|".join(
        re.escape(value).replace("'", "''")
        for value in sorted(PUB28_UNIT_DESIGNATOR_MAP, key=len, reverse=True)
    )
    ambiguous_unit_designators = "|".join(
        re.escape(value).replace("'", "''")
        for value in sorted(PUB28_UNIT_DESIGNATOR_MAP, key=len, reverse=True)
        if value != "#"
    )
    unit_prefix_cases = "\n".join(
        f"                    WHEN unit_norm = '{prefix}' OR ("
        f"unit_norm LIKE '{prefix}%' AND char_length(unit_norm) > {len(prefix)}"
        f") THEN '{display}'"
        for prefix, display in sorted(
            ADDRESS_UNIT_PREFIX_DISPLAY.items(),
            key=lambda item: -len(item[0]),
        )
    )
    unit_value_cases = "\n".join(
        f"                    WHEN unit_norm = '{prefix}' THEN ''\n"
        f"                    WHEN unit_norm LIKE '{prefix}%' "
        f"THEN substring(unit_norm FROM {len(prefix) + 1})"
        for prefix in sorted(
            ADDRESS_UNIT_PREFIX_DISPLAY,
            key=len,
            reverse=True,
        )
    )
    return f"""
    CREATE OR REPLACE FUNCTION {function}(
        input_value text,
        component_kind text,
        us_style boolean
    )
    RETURNS text
    LANGUAGE plpgsql
    IMMUTABLE
    PARALLEL SAFE
    AS $function$
    DECLARE
        cleaned text;
        original_cleaned text;
        parse_value text;
        pieces text[];
        rendered_parts text[] := ARRAY[]::text[];
        unit_parts text[] := ARRAY[]::text[];
        token text;
        upper_token text;
        rendered_token text;
        rendered text;
        unit_norm text;
        unit_prefix text;
        unit_value text;
        unit_source_value text;
        unit_match text[];
        unit_display text;
        street_value text;
        index_value integer;
        first_alpha integer;
        suffix_position integer;
        drop_suffix_period boolean := false;
    BEGIN
        cleaned := NULLIF(btrim(regexp_replace(
            translate(
                normalize(COALESCE(input_value, ''), NFC),
                {translated_spaces},
                '            '
            ),
            '[[:space:]]+', ' ', 'g'
        )), '');
        cleaned := NULLIF(rtrim(cleaned, ' ,;:'), '');
        IF cleaned IS NULL THEN
            RETURN NULL;
        END IF;
        IF us_style AND component_kind IN ('line1', 'line2', 'city') THEN
            cleaned := regexp_replace(
                cleaned,
                '\\m([NS])([[:space:]]*[.][[:space:]]*|[[:space:]]+)([EW])[.]?\\M',
                '\\1\\3',
                'gi'
            );
        END IF;
        IF component_kind IN ('line1', 'line2') THEN
            IF us_style THEN
                cleaned := regexp_replace(
                    cleaned,
                    '\\mBOX\\M[[:space:]]*#[[:space:]]*([[:alnum:]])',
                    'BOX \\1',
                    'gi'
                );
            END IF;
            original_cleaned := cleaned;
            IF us_style THEN
              LOOP
                unit_prefix := NULL;
                unit_value := NULL;
                unit_source_value := NULL;
                unit_display := NULL;
                street_value := NULL;
                parse_value := regexp_replace(
                    cleaned,
                    '(^|[[:space:],])({unit_designators})[-/]([#A-Za-z0-9])',
                    '\\1\\2 \\3',
                    'gi'
                );
                unit_norm := CASE component_kind
                    WHEN 'line1' THEN {unit_normalizer}(parse_value, NULL::text)
                    ELSE {unit_normalizer}(NULL::text, parse_value)
                END;
                unit_prefix := CASE
{unit_prefix_cases}
                    ELSE NULL
                END;
                unit_value := CASE
{unit_value_cases}
                    ELSE NULL
                END;
                IF unit_prefix IS NOT NULL
                   AND unit_value ~ '^[a-z]+$'
                   AND lower(parse_value) ~ (
                       '(^|[[:space:],])({ambiguous_unit_designators})'
                       || CASE
                           WHEN char_length(unit_value) > 1
                           THEN left(unit_value, -1)
                                || '[[:space:]]*'
                                || right(unit_value, 1)
                           ELSE unit_value
                       END
                       || '[.,;:]*[[:space:]]*$'
                   )
                THEN
                    unit_prefix := NULL;
                    unit_value := NULL;
                END IF;
                IF unit_prefix IS NOT NULL THEN
                    unit_match := regexp_match(
                        parse_value,
                        '(^|[[:space:],])({unit_designators})[.]?[[:space:]]*(#[[:space:]]*)?(([A-Za-z0-9][A-Za-z0-9-]*)([[:space:]]+[A-Za-z0-9])?)?[.,;:]*[[:space:]]*$',
                        'i'
                    );
                    unit_source_value := COALESCE(unit_match[4], unit_value);
                    unit_display := unit_prefix;
                    IF COALESCE(unit_source_value, '') <> '' THEN
                      unit_display := unit_prefix || ' ' || CASE
                        WHEN unit_source_value ~* '^[0-9]+(st|nd|rd|th)$'
                        THEN substring(unit_source_value FROM 1 FOR char_length(unit_source_value) - 2)
                             || lower(right(unit_source_value, 2))
                        WHEN unit_source_value ~ '[0-9]'
                             AND unit_source_value ~* '[a-z]'
                        THEN upper(unit_source_value)
                        WHEN upper(unit_source_value) = ANY(ARRAY[
                            'APO', 'CMR', 'CR', 'DPO', 'FM', 'FPO', 'HC', 'I',
                            'PMB', 'PO', 'PSC', 'RR', 'SH', 'SR', 'US'
                        ])
                        THEN upper(unit_source_value)
                        WHEN unit_source_value ~ '^[A-Z][a-z]+([A-Z][a-z]+)*$'
                        THEN unit_source_value
                        ELSE initcap(lower(unit_source_value COLLATE "und-x-icu"))
                      END;
                    END IF;
                    street_value := NULLIF(btrim(
                        {unit_stripper}(' ' || lower(parse_value) || ' ', unit_norm),
                        E' \t\n\r,;:'
                    ), '');
                    IF street_value IS NOT NULL THEN
                        street_value := NULLIF(btrim(
                            substring(cleaned FROM 1 FOR char_length(street_value)),
                            E' \t\n\r,;:'
                        ), '');
                    END IF;
                    IF component_kind = 'line1'
                       AND lower(COALESCE(unit_match[2], '')) = ANY(ARRAY[
                           'apartment', 'basement', 'building', 'department',
                           'floor', 'front', 'hanger', 'key', 'lobby', 'lot',
                           'lower', 'office', 'penthouse', 'pier', 'rear',
                           'room', 'side', 'slip', 'space', 'stop', 'suite',
                           'trailer', 'unit', 'upper'
                       ])
                       AND COALESCE(unit_match[1], '') <> ','
                       AND {street_suffix_normalizer}(
                           regexp_replace(
                               street_value,
                               '[[:space:]]+(N|S|E|W|NE|NW|SE|SW)[.]?$',
                               '',
                               'i'
                           ),
                           NULL::text
                       ) IS NULL
                    THEN
                        EXIT;
                    END IF;
                    IF street_value IS NOT NULL
                       AND char_length(street_value) >= char_length(cleaned) THEN
                        EXIT;
                    END IF;
                    unit_parts := array_prepend(unit_display, unit_parts);
                    cleaned := street_value;
                    EXIT WHEN cleaned IS NULL;
                ELSE
                    EXIT;
                END IF;
              END LOOP;
            END IF;
            IF component_kind = 'line2' THEN
                IF cleaned IS NULL AND cardinality(unit_parts) > 0 THEN
                    RETURN array_to_string(unit_parts, ', ');
                END IF;
                cleaned := original_cleaned;
                unit_parts := ARRAY[]::text[];
            END IF;
            IF cleaned IS NULL THEN
                RETURN NULLIF(array_to_string(unit_parts, ', '), '');
            END IF;
            cleaned := btrim(regexp_replace(
                cleaned,
                '^(P[[:space:]]*[.]?[[:space:]]*O[[:space:]]*[.]?|POST[[:space:]]+OFFICE)[[:space:]]+BOX[.]?([[:space:]]*#[[:space:]]*|[[:space:]]+|$)',
                'PO Box ',
                'i'
            ));
            cleaned := NULLIF(rtrim(cleaned, ' ,;:'), '');
            IF cleaned IS NULL THEN
                RETURN NULLIF(array_to_string(unit_parts, ', '), '');
            END IF;
        END IF;
        IF component_kind = 'postal' THEN
            RETURN upper(cleaned);
        END IF;
        IF component_kind = 'state'
           AND cleaned COLLATE "und-x-icu" ~ '^[[:alpha:]]{{1,3}}$' THEN
            RETURN upper(cleaned);
        END IF;

        SELECT array_agg(part[1] ORDER BY ordinality)
          INTO pieces
          FROM regexp_matches(
                   cleaned COLLATE "und-x-icu",
                   '([[:alnum:]]+|[^[:alnum:]]+)',
                   'g'
               ) WITH ORDINALITY AS token_parts(part, ordinality);
        IF pieces IS NULL THEN
            RETURN cleaned;
        END IF;

        FOR index_value IN 1..array_length(pieces, 1) LOOP
            token := pieces[index_value];
            IF first_alpha IS NULL
               AND token COLLATE "und-x-icu" ~ '[[:alpha:]]' THEN
                first_alpha := index_value;
            END IF;
        END LOOP;
        FOR index_value IN REVERSE array_length(pieces, 1)..1 LOOP
            token := pieces[index_value];
            IF token COLLATE "und-x-icu" ~ '[[:alpha:]]'
               AND char_length(token) > 1
               AND upper(token) <> ALL(ARRAY[
                   'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'
               ]) THEN
                suffix_position := index_value;
                EXIT;
            END IF;
        END LOOP;

        FOR index_value IN 1..array_length(pieces, 1) LOOP
            token := pieces[index_value];
            IF token COLLATE "und-x-icu" !~ '[[:alnum:]]' THEN
                IF drop_suffix_period THEN
                    rendered_token := regexp_replace(
                        token,
                        '^([[:space:]]*)[.]',
                        '\\1'
                    );
                    drop_suffix_period := rendered_token = token
                                          AND btrim(token) = '';
                    token := rendered_token;
                END IF;
                rendered_parts := array_append(rendered_parts, token);
                CONTINUE;
            END IF;
            upper_token := upper(token);
            rendered_token := NULL;
            IF index_value <> suffix_position THEN
                drop_suffix_period := false;
            END IF;
            IF us_style AND component_kind IN ('line1', 'line2') THEN
                IF index_value = suffix_position THEN
                    rendered_token := CASE upper_token
                        WHEN 'ALLEE' THEN 'Alley'
                        WHEN 'ALLEY' THEN 'Alley'
                        WHEN 'ALLY' THEN 'Alley'
                        WHEN 'ALY' THEN 'Alley'
                        WHEN 'AV' THEN 'Avenue'
                        WHEN 'AVE' THEN 'Avenue'
                        WHEN 'AVEN' THEN 'Avenue'
                        WHEN 'AVENU' THEN 'Avenue'
                        WHEN 'AVENUE' THEN 'Avenue'
                        WHEN 'AVN' THEN 'Avenue'
                        WHEN 'AVNUE' THEN 'Avenue'
                        WHEN 'BLVD' THEN 'Boulevard'
                        WHEN 'BOUL' THEN 'Boulevard'
                        WHEN 'BOULEVARD' THEN 'Boulevard'
                        WHEN 'BOULV' THEN 'Boulevard'
                        WHEN 'CEN' THEN 'Center'
                        WHEN 'CENT' THEN 'Center'
                        WHEN 'CENTER' THEN 'Center'
                        WHEN 'CENTR' THEN 'Center'
                        WHEN 'CENTRE' THEN 'Center'
                        WHEN 'CIR' THEN 'Circle'
                        WHEN 'CIRC' THEN 'Circle'
                        WHEN 'CIRCL' THEN 'Circle'
                        WHEN 'CIRCLE' THEN 'Circle'
                        WHEN 'CNTER' THEN 'Center'
                        WHEN 'CNTR' THEN 'Center'
                        WHEN 'COURT' THEN 'Court'
                        WHEN 'CRCL' THEN 'Circle'
                        WHEN 'CRCLE' THEN 'Circle'
                        WHEN 'CT' THEN 'Court'
                        WHEN 'CTR' THEN 'Center'
                        WHEN 'DR' THEN 'Drive'
                        WHEN 'DRIV' THEN 'Drive'
                        WHEN 'DRIVE' THEN 'Drive'
                        WHEN 'DRV' THEN 'Drive'
                        WHEN 'EXP' THEN 'Expressway'
                        WHEN 'EXPR' THEN 'Expressway'
                        WHEN 'EXPRESS' THEN 'Expressway'
                        WHEN 'EXPRESSWAY' THEN 'Expressway'
                        WHEN 'EXPW' THEN 'Expressway'
                        WHEN 'EXPY' THEN 'Expressway'
                        WHEN 'FREEWAY' THEN 'Freeway'
                        WHEN 'FREEWY' THEN 'Freeway'
                        WHEN 'FRWAY' THEN 'Freeway'
                        WHEN 'FRWY' THEN 'Freeway'
                        WHEN 'FWY' THEN 'Freeway'
                        WHEN 'HIGHWAY' THEN 'Highway'
                        WHEN 'HIGHWY' THEN 'Highway'
                        WHEN 'HIWAY' THEN 'Highway'
                        WHEN 'HIWY' THEN 'Highway'
                        WHEN 'HWAY' THEN 'Highway'
                        WHEN 'HWY' THEN 'Highway'
                        WHEN 'LANE' THEN 'Lane'
                        WHEN 'LN' THEN 'Lane'
                        WHEN 'PARKWAY' THEN 'Parkway'
                        WHEN 'PARKWAYS' THEN 'Parkway'
                        WHEN 'PARKWY' THEN 'Parkway'
                        WHEN 'PKWAY' THEN 'Parkway'
                        WHEN 'PKWY' THEN 'Parkway'
                        WHEN 'PKWYS' THEN 'Parkway'
                        WHEN 'PKY' THEN 'Parkway'
                        WHEN 'PL' THEN 'Place'
                        WHEN 'PLACE' THEN 'Place'
                        WHEN 'PLAZA' THEN 'Plaza'
                        WHEN 'PLZ' THEN 'Plaza'
                        WHEN 'PLZA' THEN 'Plaza'
                        WHEN 'RD' THEN 'Road'
                        WHEN 'ROAD' THEN 'Road'
                        WHEN 'ROUTE' THEN 'Route'
                        WHEN 'RTE' THEN 'Route'
                        WHEN 'SQ' THEN 'Square'
                        WHEN 'SQR' THEN 'Square'
                        WHEN 'SQRE' THEN 'Square'
                        WHEN 'SQU' THEN 'Square'
                        WHEN 'SQUARE' THEN 'Square'
                        WHEN 'ST' THEN 'Street'
                        WHEN 'STR' THEN 'Street'
                        WHEN 'STREET' THEN 'Street'
                        WHEN 'STRT' THEN 'Street'
                        WHEN 'TER' THEN 'Terrace'
                        WHEN 'TERR' THEN 'Terrace'
                        WHEN 'TERRACE' THEN 'Terrace'
                        WHEN 'TPKE' THEN 'Turnpike'
                        WHEN 'TRAIL' THEN 'Trail'
                        WHEN 'TRAILS' THEN 'Trail'
                        WHEN 'TRL' THEN 'Trail'
                        WHEN 'TRLS' THEN 'Trail'
                        WHEN 'TRNPK' THEN 'Turnpike'
                        WHEN 'TURNPIKE' THEN 'Turnpike'
                        WHEN 'TURNPK' THEN 'Turnpike'
                        WHEN 'WAY' THEN 'Way'
                        WHEN 'WY' THEN 'Way'
                        ELSE NULL
                    END;
                    drop_suffix_period := rendered_token IS NOT NULL;
                END IF;
                IF rendered_token IS NULL
                   AND upper_token = ANY(ARRAY[
                       'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'
                   ])
                   AND (
                       index_value = first_alpha
                       OR (
                           suffix_position IS NOT NULL
                           AND index_value > suffix_position
                       )
                   ) THEN
                    rendered_token := CASE upper_token
                        WHEN 'N' THEN 'North'
                        WHEN 'S' THEN 'South'
                        WHEN 'E' THEN 'East'
                        WHEN 'W' THEN 'West'
                        ELSE upper_token
                    END;
                    drop_suffix_period := rendered_token IS NOT NULL;
                END IF;
            ELSIF us_style
                  AND component_kind = 'city'
                  AND index_value = first_alpha THEN
                rendered_token := CASE upper_token
                    WHEN 'N' THEN 'North'
                    WHEN 'S' THEN 'South'
                    WHEN 'E' THEN 'East'
                    WHEN 'W' THEN 'West'
                    WHEN 'NE' THEN 'NE'
                    WHEN 'NW' THEN 'NW'
                    WHEN 'SE' THEN 'SE'
                    WHEN 'SW' THEN 'SW'
                    WHEN 'FT' THEN 'Fort'
                    WHEN 'MT' THEN 'Mount'
                    ELSE NULL
                END;
                drop_suffix_period := rendered_token IS NOT NULL;
            END IF;
            IF rendered_token IS NULL THEN
                rendered_token := CASE
                    WHEN token ~* '^[0-9]+(ST|ND|RD|TH)$'
                    THEN substring(upper_token FROM 1 FOR char_length(token) - 2)
                         || lower(right(upper_token, 2))
                    WHEN token COLLATE "und-x-icu" ~ '[[:digit:]]'
                         AND token COLLATE "und-x-icu" ~ '[[:alpha:]]'
                    THEN upper_token
                    WHEN upper_token = ANY(ARRAY[
                        'APO', 'CMR', 'CR', 'DPO', 'FM', 'FPO', 'HC', 'I',
                        'PMB', 'PO', 'PSC', 'RR', 'SH', 'SR', 'US'
                    ])
                    THEN upper_token
                    WHEN token ~ '^[A-Z][a-z]+([A-Z][a-z]+)*$'
                    THEN token
                    ELSE initcap(lower(token COLLATE "und-x-icu"))
                END;
            END IF;
            rendered_parts := array_append(rendered_parts, rendered_token);
        END LOOP;

        rendered := array_to_string(rendered_parts, '');
        IF component_kind = 'line1' AND cardinality(unit_parts) > 0 THEN
            unit_display := array_to_string(unit_parts, ', ');
            rendered := CASE
                WHEN COALESCE(rtrim(rendered, ' ,;:'), '') = '' THEN unit_display
                ELSE rtrim(rendered, ' ,;:') || ', ' || unit_display
            END;
        END IF;
        RETURN rendered;
    END
    $function$;
    """


def _formatted_address_function_sql(schema: str) -> str:
    humanizer = f"{_q(schema)}.{_q('addr_humanize_component_v2')}"
    function = f"{_q(schema)}.{_q('addr_formatted_address_v2')}"
    street_normalizer = f"{_q(schema)}.{_q('addr_street_norm_v1')}"
    unit_normalizer = f"{_q(schema)}.{_q('addr_unit_norm_v1')}"
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
    LANGUAGE plpgsql
    IMMUTABLE
    PARALLEL SAFE
    AS $function$
    DECLARE
        country_value text;
        country_key text;
        us_style boolean;
        line_one text;
        line_two text;
        city_value text;
        state_value text;
        postal_value text;
        locality_value text;
        displayed_country text;
        rendered text;
        suffix_start integer;
    BEGIN
        country_value := NULLIF(btrim(regexp_replace(
            translate(
                normalize(COALESCE(country_code, ''), NFC),
                {translated_spaces},
                '            '
            ),
            '[[:space:]]+', ' ', 'g'
        )), '');
        country_key := regexp_replace(
            upper(COALESCE(country_value, '')),
            '[^A-Z]', '', 'g'
        );
        us_style := country_value IS NULL OR country_key = ANY(ARRAY[
            'US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA'
        ]);

        line_one := {humanizer}(first_line, 'line1', us_style);
        line_two := {humanizer}(second_line, 'line2', us_style);
        city_value := {humanizer}(city_name, 'city', us_style);
        state_value := {humanizer}(state_name, 'state', us_style);
        postal_value := {humanizer}(postal_code, 'postal', us_style);
        IF us_style AND postal_value ~ '^[0-9]{{9}}$' THEN
            postal_value := substring(postal_value FROM 1 FOR 5)
                            || '-' || substring(postal_value FROM 6 FOR 4);
        ELSIF us_style AND postal_value ~ '^[0-9]{{5}}[- ][0-9]{{4}}$' THEN
            postal_value := substring(postal_value FROM 1 FOR 5)
                            || '-' || substring(postal_value FROM 7 FOR 4);
        END IF;

        IF line_one IS NOT NULL AND line_two IS NOT NULL THEN
            suffix_start := char_length(line_one) - char_length(line_two);
            IF lower(line_one) = lower(line_two)
               OR (
                   suffix_start > 0
                   AND lower(right(line_one, char_length(line_two))) = lower(line_two)
                   AND substring(line_one FROM suffix_start FOR 1) IN (' ', ',', ';')
               )
               OR (
                   NULLIF({unit_normalizer}(first_line, NULL::text), '') IS NOT NULL
                   AND {unit_normalizer}(first_line, NULL::text)
                       = {unit_normalizer}(second_line, NULL::text)
                   AND {street_normalizer}(first_line, NULL::text) IS NOT NULL
                   AND {street_normalizer}(first_line, NULL::text)
                       = {street_normalizer}(second_line, NULL::text)
               )
               OR (
                   NULLIF({unit_normalizer}(first_line, NULL::text), '') IS NOT NULL
                   AND {unit_normalizer}(first_line, NULL::text)
                       = {unit_normalizer}(first_line, second_line)
                   AND {street_normalizer}(first_line, NULL::text) IS NOT NULL
                   AND {street_normalizer}(first_line, NULL::text)
                       = {street_normalizer}(first_line, second_line)
               ) THEN
                line_two := NULL;
            END IF;
        END IF;

        locality_value := NULLIF(concat_ws(
            ', ',
            city_value,
            NULLIF(concat_ws(' ', state_value, postal_value), '')
        ), '');
        IF NOT us_style THEN
            IF country_value COLLATE "und-x-icu"
               ~ '^[[:alpha:]]{{1,3}}$' THEN
                displayed_country := upper(country_value);
            ELSE
                displayed_country := {humanizer}(
                    country_value,
                    'country',
                    false
                );
            END IF;
        END IF;
        rendered := NULLIF(concat_ws(
            ', ',
            line_one,
            line_two,
            locality_value,
            displayed_country
        ), '');
        IF rendered IS NULL THEN
            RETURN NULL;
        END IF;
        rendered := normalize(rendered, NFC);
        IF char_length(rendered) > 1024 THEN
            RETURN NULLIF(rtrim(
                substring(rendered FROM 1 FOR 1024),
                E' \t\n\r,;'
            ), '');
        END IF;
        RETURN rendered;
    END
    $function$;
    """


def upgrade() -> None:
    schema = _schema()
    op.execute(_humanize_component_function_sql(schema))
    op.execute(_formatted_address_function_sql(schema))


def downgrade() -> None:
    schema = _schema()
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('addr_formatted_address_v2')}"
        f"(text, text, text, text, text, text);"
    )
    op.execute(
        f"DROP FUNCTION IF EXISTS "
        f"{_q(schema)}.{_q('addr_humanize_component_v2')}"
        f"(text, text, boolean);"
    )
