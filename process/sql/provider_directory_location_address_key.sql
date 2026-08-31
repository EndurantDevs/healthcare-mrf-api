    WITH normalized AS (
        SELECT
            {{SOURCE_ALIAS}}.source_id,
            {{SOURCE_ALIAS}}.resource_id,
            {{SOURCE_ALIAS}}.first_line,
            {{SOURCE_ALIAS}}.second_line,
            {{SOURCE_ALIAS}}.city_name,
            {{SOURCE_ALIAS}}.state_name,
            {{SOURCE_ALIAS}}.state_code,
            {{SOURCE_ALIAS}}.postal_code,
            {{SOURCE_ALIAS}}.zip5,
            {{SOURCE_ALIAS}}.city_norm,
            {{SOURCE_ALIAS}}.country_code,
            {{SOURCE_ALIAS}}.address_key,
            {{SCHEMA_REF}}.addr_zip5_norm_v1({{SOURCE_ALIAS}}.postal_code)::varchar AS source_zip5,
            {{NORMALIZED_COUNTRY_SQL}} AS normalized_country,
            {{RAW_STATE_SQL}} AS raw_state,
            {{NORMALIZED_STATE_SQL}} AS normalized_state
          {{FROM_SQL}}
         WHERE {{WHERE_SQL}}
    ),
    resolved AS (
        SELECT
            normalized.*,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.normalized_state::varchar, '')), '') IS NULL
                    THEN {{ZIP_STATE_SQL}}
                ELSE NULL::varchar
            END AS zip_restored_state,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.city_name::varchar, '')), '') IS NULL
                    THEN {{ZIP_CITY_SQL}}
                ELSE NULL::varchar
            END AS zip_restored_city,
            COALESCE(NULLIF(BTRIM(normalized.normalized_state::varchar), ''), {{ZIP_STATE_SQL}}) AS resolved_state,
            COALESCE(NULLIF(BTRIM(normalized.city_name::varchar), ''), {{ZIP_CITY_SQL}}) AS resolved_city
          FROM normalized
          {{ZIP_STATE_JOIN_SQL}}
    ),
    keyed AS (
        SELECT
            source_id,
            resource_id,
            {{SCHEMA_REF}}.addr_key_v1(
                first_line,
                second_line,
                resolved_city,
                resolved_state,
                postal_code,
                COALESCE(NULLIF(normalized_country, ''), 'US')
            ) AS computed_address_key,
            source_zip5 AS computed_zip5,
            {{SCHEMA_REF}}.addr_state_code_v1(resolved_state)::varchar AS computed_state_code,
            {{SCHEMA_REF}}.addr_city_norm_v1(resolved_city)::varchar AS computed_city_norm,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(city_name::varchar, '')), '') IS NULL
                 AND zip_restored_city IS NOT NULL
                    THEN zip_restored_city::varchar
                ELSE NULL::varchar
            END AS restored_city_name,
            CASE
                WHEN raw_state ~ '^[0-9]{1,2}$'
                    THEN {{SCHEMA_REF}}.addr_state_code_v1(resolved_state)::varchar
                WHEN NULLIF(BTRIM(COALESCE(raw_state::varchar, '')), '') IS NULL
                 AND zip_restored_state IS NOT NULL
                    THEN {{SCHEMA_REF}}.addr_state_code_v1(zip_restored_state)::varchar
                ELSE NULL::varchar
            END AS restored_state_name,
            normalized_country
          FROM resolved
    )
    UPDATE {{LOCATION_REF}} AS loc
       SET address_key = keyed.computed_address_key::text,
           zip5 = COALESCE(keyed.computed_zip5, loc.zip5),
           city_name = COALESCE(keyed.restored_city_name, loc.city_name),
           state_name = COALESCE(keyed.restored_state_name, loc.state_name),
           state_code = COALESCE(keyed.computed_state_code, loc.state_code),
           city_norm = COALESCE(keyed.computed_city_norm, loc.city_norm),
           country_code = COALESCE(keyed.normalized_country, loc.country_code),
           updated_at = now()
      FROM keyed
     WHERE loc.source_id = keyed.source_id
       AND loc.resource_id = keyed.resource_id
       AND keyed.computed_address_key IS NOT NULL
       AND (
            loc.address_key IS DISTINCT FROM keyed.computed_address_key::text
         OR loc.zip5 IS DISTINCT FROM COALESCE(keyed.computed_zip5, loc.zip5)
         OR loc.city_name IS DISTINCT FROM COALESCE(keyed.restored_city_name, loc.city_name)
         OR loc.state_name IS DISTINCT FROM COALESCE(keyed.restored_state_name, loc.state_name)
         OR loc.state_code IS DISTINCT FROM COALESCE(keyed.computed_state_code, loc.state_code)
         OR loc.city_norm IS DISTINCT FROM COALESCE(keyed.computed_city_norm, loc.city_norm)
         OR loc.country_code IS DISTINCT FROM COALESCE(keyed.normalized_country, loc.country_code)
       );
