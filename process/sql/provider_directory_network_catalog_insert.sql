    INSERT INTO {{STAGE_REF}} ({{COLUMNS_SQL}})
    WITH refs_raw AS MATERIALIZED (
        SELECT
            insurance_plan.source_id::varchar AS source_id,
            'InsurancePlan'::varchar AS source_resource_type,
            insurance_plan.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            insurance_plan.last_seen_run_id::varchar AS last_seen_run_id,
            insurance_plan.observed_at AS source_observed_at,
            insurance_plan.updated_at AS source_updated_at
          FROM {{INSURANCE_PLAN_REF}} AS insurance_plan
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {{INSURANCE_PLAN_SCOPE_SQL}}
        UNION ALL
        SELECT
            role.source_id::varchar AS source_id,
            'PractitionerRole'::varchar AS source_resource_type,
            role.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            role.last_seen_run_id::varchar AS last_seen_run_id,
            role.observed_at AS source_observed_at,
            role.updated_at AS source_updated_at
          FROM {{PRACTITIONER_ROLE_REF}} AS role
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(role.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE role.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {{PRACTITIONER_ROLE_SCOPE_SQL}}
        UNION ALL
        SELECT
            affiliation.source_id::varchar AS source_id,
            'OrganizationAffiliation'::varchar AS source_resource_type,
            affiliation.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            affiliation.last_seen_run_id::varchar AS last_seen_run_id,
            affiliation.observed_at AS source_observed_at,
            affiliation.updated_at AS source_updated_at
          FROM {{ORGANIZATION_AFFILIATION_REF}} AS affiliation
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE affiliation.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {{AFFILIATION_SCOPE_SQL}}
    ), refs AS MATERIALIZED (
        SELECT
            refs_raw.source_id,
            refs_raw.source_resource_type,
            refs_raw.source_resource_id,
            refs_raw.network_ref,
            {{NETWORK_RESOURCE_ID_SQL}}::varchar AS network_resource_id,
            refs_raw.last_seen_run_id,
            refs_raw.source_observed_at,
            refs_raw.source_updated_at
          FROM refs_raw
         WHERE NULLIF(BTRIM(refs_raw.network_ref), '') IS NOT NULL
    ), joined AS MATERIALIZED (
        SELECT
            refs.source_id,
            refs.source_resource_type,
            refs.source_resource_id,
            refs.network_ref,
            refs.network_resource_id,
            refs.last_seen_run_id,
            refs.source_observed_at,
            refs.source_updated_at,
            NULLIF(BTRIM(network_org.name), '')::varchar AS provider_directory_network_name,
            COALESCE(network_org.aliases::jsonb, '[]'::jsonb) AS aliases,
            NULLIF(regexp_replace(lower(COALESCE(network_org.name, '')), '[^a-z0-9]+', '', 'g'), '')
                AS provider_directory_network_key,
            NULLIF(regexp_replace(lower(COALESCE(NULLIF(src.org_name, ''), src.plan_name, '')), '[^a-z0-9]+', '', 'g'), '')
                AS provider_directory_issuer_key,
            src.org_name::varchar AS source_org_name,
            src.plan_name::varchar AS source_plan_name,
            src.canonical_api_base::text AS canonical_api_base,
            GREATEST(
                COALESCE(refs.source_observed_at, TIMESTAMP 'epoch'),
                COALESCE(refs.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(network_org.observed_at, TIMESTAMP 'epoch'),
                COALESCE(network_org.updated_at, TIMESTAMP 'epoch'),
                COALESCE(src.updated_at, TIMESTAMP 'epoch')
            ) AS observed_at
          FROM refs
          JOIN {{ORGANIZATION_REF}} AS network_org
            ON network_org.source_id = refs.source_id
           AND network_org.resource_id = refs.network_resource_id
          JOIN {{SOURCE_REF}} AS src
            ON src.source_id = refs.source_id
         WHERE refs.network_resource_id IS NOT NULL
           AND network_org.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_org.name), '') IS NOT NULL
    ), keyed AS MATERIALIZED (
        SELECT
            joined.*,
            CASE
                WHEN joined.provider_directory_issuer_key IS NOT NULL
                 AND joined.provider_directory_network_key IS NOT NULL
                    THEN joined.provider_directory_issuer_key || ':' || joined.provider_directory_network_key
                ELSE NULL
            END::varchar AS provider_directory_issuer_network_match_key
          FROM joined
         WHERE joined.provider_directory_network_key IS NOT NULL
    )
    SELECT
        keyed.source_id,
        keyed.network_resource_id,
        keyed.provider_directory_network_name,
        keyed.provider_directory_network_key,
        keyed.provider_directory_issuer_key,
        keyed.provider_directory_issuer_network_match_key,
        keyed.aliases,
        COALESCE(
            jsonb_agg(
                DISTINCT jsonb_build_object(
                    'resource_type', keyed.source_resource_type,
                    'resource_id', keyed.source_resource_id,
                    'ref', keyed.network_ref,
                    'last_seen_run_id', keyed.last_seen_run_id
                )
            ),
            '[]'::jsonb
        ) AS refs,
        jsonb_build_object(
            'InsurancePlan', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'InsurancePlan'),
            'PractitionerRole', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'PractitionerRole'),
            'OrganizationAffiliation', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'OrganizationAffiliation')
        ) AS source_resource_counts,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'InsurancePlan'))::bigint
            AS insurance_plan_ref_count,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'PractitionerRole'))::bigint
            AS practitioner_role_ref_count,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'OrganizationAffiliation'))::bigint
            AS organization_affiliation_ref_count,
        COUNT(DISTINCT keyed.source_resource_type || ':' || keyed.source_resource_id || ':' || keyed.network_ref)::bigint
            AS distinct_ref_count,
        keyed.source_org_name,
        keyed.source_plan_name,
        keyed.canonical_api_base,
        MAX(keyed.observed_at) AS observed_at,
        now() AS published_at
      FROM keyed
  GROUP BY
        keyed.source_id,
        keyed.network_resource_id,
        keyed.provider_directory_network_name,
        keyed.provider_directory_network_key,
        keyed.provider_directory_issuer_key,
        keyed.provider_directory_issuer_network_match_key,
        keyed.aliases,
        keyed.source_org_name,
        keyed.source_plan_name,
        keyed.canonical_api_base;
