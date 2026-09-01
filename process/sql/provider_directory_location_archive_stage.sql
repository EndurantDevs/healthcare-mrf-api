    CREATE UNLOGGED TABLE {{STAGE_REF}} AS
    WITH eligible AS (
        SELECT
            loc.address_key::uuid AS address_key,
            NULLIF(BTRIM(loc.first_line), '')::text AS first_line,
            NULLIF(BTRIM(loc.second_line), '')::text AS second_line,
            NULLIF(BTRIM(loc.city_name), '')::text AS city_name,
            NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), '')::text AS state_name,
            NULLIF(BTRIM(COALESCE(NULLIF(loc.postal_code, ''), loc.zip5)), '')::text AS postal_code,
            COALESCE(NULLIF(BTRIM(loc.country_code), ''), 'US')::text AS country_code,
            loc.updated_at,
            loc.source_id,
            loc.resource_id
          FROM {{LOCATION_REF}} AS loc
         WHERE loc.address_key ~* '{{UUID_RE}}'
           AND NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), '') IS NOT NULL
           AND UPPER(NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), ''))
                NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')
           AND NULLIF(BTRIM(COALESCE(NULLIF(loc.postal_code, ''), loc.zip5)), '') IS NOT NULL
           AND (
                NULLIF(BTRIM(loc.first_line), '') IS NOT NULL
             OR NULLIF(BTRIM(loc.city_name), '') IS NOT NULL
           )
           AND COALESCE(
                NULLIF(
                    UPPER(regexp_replace(COALESCE(NULLIF(loc.country_code, ''), 'US'), '[^A-Z0-9]', '', 'g')),
                    ''
                ),
                'US'
           ) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')
           {{LOCATION_SCOPE_SQL}}
        UNION ALL
        SELECT
            {{SCHEMA_REF}}.addr_key_v1(
                NULLIF(BTRIM(addr.value->'line'->>0), ''),
                NULLIF(BTRIM(addr.value->'line'->>1), ''),
                NULLIF(BTRIM(addr.value->>'city'), ''),
                NULLIF(BTRIM(addr.value->>'state'), ''),
                NULLIF(BTRIM(addr.value->>'postalCode'), ''),
                COALESCE(NULLIF(BTRIM(addr.value->>'country'), ''), 'US')
            ) AS address_key,
            NULLIF(BTRIM(addr.value->'line'->>0), '')::text AS first_line,
            NULLIF(BTRIM(addr.value->'line'->>1), '')::text AS second_line,
            NULLIF(BTRIM(addr.value->>'city'), '')::text AS city_name,
            NULLIF(BTRIM(addr.value->>'state'), '')::text AS state_name,
            NULLIF(BTRIM(addr.value->>'postalCode'), '')::text AS postal_code,
            COALESCE(NULLIF(BTRIM(addr.value->>'country'), ''), 'US')::text AS country_code,
            organization.updated_at,
            organization.source_id,
            organization.resource_id
          FROM {{ORGANIZATION_REF}} AS organization
          JOIN LATERAL jsonb_array_elements(
                COALESCE(organization.address_json::jsonb, '[]'::jsonb)
          ) WITH ORDINALITY AS addr(value, ordinal) ON TRUE
         WHERE organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(addr.value->'line'->>0), '') IS NOT NULL
           AND NULLIF(BTRIM(addr.value->>'postalCode'), '') IS NOT NULL
           AND (
                NULLIF(BTRIM(addr.value->'line'->>0), '') IS NOT NULL
             OR NULLIF(BTRIM(addr.value->>'city'), '') IS NOT NULL
           )
           AND NULLIF(BTRIM(addr.value->>'state'), '') IS NOT NULL
           AND UPPER(NULLIF(BTRIM(addr.value->>'state'), ''))
                NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')
           AND COALESCE(
                NULLIF(
                    UPPER(regexp_replace(COALESCE(NULLIF(addr.value->>'country', ''), 'US'), '[^A-Z0-9]', '', 'g')),
                    ''
                ),
                'US'
           ) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')
           {{ORGANIZATION_SCOPE_SQL}}
    )
    SELECT DISTINCT ON (address_key)
        address_key,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code
      FROM eligible
     WHERE address_key IS NOT NULL
     ORDER BY
        address_key,
        first_line IS NULL,
        length(COALESCE(first_line, '')) DESC,
        city_name IS NULL,
        length(COALESCE(city_name, '')) DESC,
        updated_at DESC NULLS LAST,
        source_id,
        resource_id;
