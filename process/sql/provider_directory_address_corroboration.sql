    CREATE OR REPLACE VIEW {{VIEW_REF}} AS
    WITH address_candidates AS (
        SELECT
            overlay.source_record_id::varchar AS source_record_id,
            overlay.source_id::varchar AS source_id,
            overlay.resource_type::varchar AS resource_type,
            overlay.resource_id::varchar AS resource_id,
            CASE
                WHEN overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')
                    THEN NULLIF(split_part(overlay.source_record_id, ':', 5), '')
                ELSE NULL::varchar
            END AS location_resource_id,
            overlay.npi::bigint AS npi,
            NULL::varchar AS location_key,
            overlay.address_key::uuid AS address_key,
            NULLIF(LEFT(regexp_replace(COALESCE(overlay.postal_code, ''), '\D', '', 'g'), 5), '')::varchar AS zip5,
            overlay.state_code::varchar AS state_code,
            NULLIF(regexp_replace(upper(BTRIM(COALESCE(overlay.city_name, ''))), '[^A-Z0-9]+', '', 'g'), '')::varchar AS city_norm,
            overlay.telephone_number::varchar AS telephone_number,
            overlay.phone_number::varchar AS phone_number,
            overlay.fax_number::varchar AS fax_number,
            overlay.fax_number_digits::varchar AS fax_number_digits,
            overlay.source_updated_at AS source_updated_at
          FROM {{ADDRESS_OVERLAY_REF}} overlay
         WHERE overlay.npi IS NOT NULL
           AND overlay.address_key IS NOT NULL
           AND overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')
    ),
    practitioner_matches AS (
        SELECT
            NULL::varchar AS source_key,
            NULL::varchar AS snapshot_id,
            NULL::varchar AS plan_id,
            NULL::varchar AS ptg_plan_id,
            e.npi,
            e.location_key,
            e.address_key,
            e.zip5,
            e.state_code,
            e.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            practitioner.resource_id AS provider_directory_provider_resource_id,
            practitioner.full_name AS provider_directory_provider_name,
            role.resource_id AS provider_directory_role_resource_id,
            COALESCE(loc.resource_id, e.location_resource_id) AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            COALESCE(loc.telephone_number, e.telephone_number) AS provider_directory_telephone_number,
            COALESCE(loc.phone_number, e.phone_number) AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            COALESCE(loc.fax_number, e.fax_number) AS provider_directory_fax_number,
            COALESCE(loc.fax_number_digits, e.fax_number_digits) AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs,
            COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb) AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            false AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            COALESCE(plan_context.provider_directory_insurance_plan_matches, '[]'::jsonb)
                AS provider_directory_insurance_plan_matches,
            COALESCE(role.specialty_codes::jsonb, '[]'::jsonb) AS provider_directory_specialty_codes,
            COALESCE(role.code_codes::jsonb, '[]'::jsonb) AS provider_directory_role_codes,
            'practitioner_role'::varchar AS provider_directory_match_type,
            (role.active IS DISTINCT FROM false) AS provider_directory_active_role,
            (practitioner.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.resource_id IS NULL OR loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(e.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(role.observed_at, TIMESTAMP 'epoch'),
                COALESCE(practitioner.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM address_candidates e
          JOIN {{PRACTITIONER_ROLE_REF}} role
            ON e.resource_type = 'PractitionerRole'
           AND role.source_id = e.source_id
           AND role.resource_id = e.resource_id
          JOIN {{PRACTITIONER_REF}} practitioner
            ON practitioner.source_id = role.source_id
           AND practitioner.resource_id = NULLIF(regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''), '')
           AND COALESCE(practitioner.npi, role.npi) = e.npi
          LEFT JOIN {{LOCATION_REF}} loc
            ON loc.source_id = e.source_id
           AND loc.resource_id = e.location_resource_id
          LEFT JOIN LATERAL (
            SELECT
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', plan_ref.value,
                            'resource_id', insurance_plan.resource_id,
                            'plan_identifier', insurance_plan.plan_identifier,
                            'name', insurance_plan.name
                        )
                    ) FILTER (WHERE plan_ref.value IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_insurance_plan_matches
              FROM jsonb_array_elements_text(
                  COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb)
              ) AS plan_ref(value)
              LEFT JOIN {{INSURANCE_PLAN_REF}} insurance_plan
                ON insurance_plan.source_id = role.source_id
               AND insurance_plan.resource_id = {{PLAN_REF_RESOURCE_ID_SQL}}
          ) plan_context ON TRUE
          LEFT JOIN LATERAL (
            WITH network_ref_values AS (
                SELECT network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.network_refs::jsonb, '[]'::jsonb)
                  ) AS network_ref(value)
                UNION
                SELECT plan_network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb)
                  ) AS plan_ref(value)
                  JOIN {{INSURANCE_PLAN_REF}} insurance_plan
                    ON insurance_plan.source_id = role.source_id
                   AND insurance_plan.resource_id = {{PLAN_REF_RESOURCE_ID_SQL}}
                  CROSS JOIN LATERAL jsonb_array_elements_text(
                      COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
                  ) AS plan_network_ref(value)
            )
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_catalog.provider_directory_network_name)
                        FILTER (WHERE NULLIF(BTRIM(network_catalog.provider_directory_network_name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_catalog.network_resource_id,
                            'name', network_catalog.provider_directory_network_name,
                            'aliases', COALESCE(network_catalog.aliases::jsonb, '[]'::jsonb),
                            'provider_directory_network_key', network_catalog.provider_directory_network_key,
                            'provider_directory_source', 'provider_directory_fhir',
                            'provider_directory_source_id', role.source_id,
                            'provider_directory_org_name', network_catalog.source_org_name,
                            'provider_directory_plan_name', network_catalog.source_plan_name,
                            'provider_directory_issuer_key', network_catalog.provider_directory_issuer_key,
                            'provider_directory_issuer_network_match_key',
                                network_catalog.provider_directory_issuer_network_match_key
                        )
                    ) FILTER (WHERE network_catalog.network_resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM network_ref_values network_ref
              LEFT JOIN {{NETWORK_CATALOG_REF}} network_catalog
                ON network_catalog.source_id = role.source_id
               AND network_catalog.network_resource_id = {{NETWORK_REF_RESOURCE_ID_SQL}}
          ) network_context ON TRUE
          JOIN {{SOURCE_REF}} src
            ON src.source_id = role.source_id
         WHERE e.npi IS NOT NULL
           AND e.address_key IS NOT NULL
    ),
    organization_affiliation_matches AS (
        SELECT
            NULL::varchar AS source_key,
            NULL::varchar AS snapshot_id,
            NULL::varchar AS plan_id,
            NULL::varchar AS ptg_plan_id,
            e.npi,
            e.location_key,
            e.address_key,
            e.zip5,
            e.state_code,
            e.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            organization.resource_id AS provider_directory_provider_resource_id,
            organization.name AS provider_directory_provider_name,
            affiliation.resource_id AS provider_directory_role_resource_id,
            COALESCE(loc.resource_id, e.location_resource_id) AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            COALESCE(loc.telephone_number, e.telephone_number) AS provider_directory_telephone_number,
            COALESCE(loc.phone_number, e.phone_number) AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            COALESCE(loc.fax_number, e.fax_number) AS provider_directory_fax_number,
            COALESCE(loc.fax_number_digits, e.fax_number_digits) AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs,
            '[]'::jsonb AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            false AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            '[]'::jsonb AS provider_directory_insurance_plan_matches,
            COALESCE(affiliation.specialty_codes::jsonb, '[]'::jsonb) AS provider_directory_specialty_codes,
            COALESCE(affiliation.code_codes::jsonb, '[]'::jsonb) AS provider_directory_role_codes,
            'organization_affiliation'::varchar AS provider_directory_match_type,
            (affiliation.active IS DISTINCT FROM false) AS provider_directory_active_role,
            (organization.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.resource_id IS NULL OR loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(e.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(affiliation.observed_at, TIMESTAMP 'epoch'),
                COALESCE(organization.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM address_candidates e
          JOIN {{ORGANIZATION_AFFILIATION_REF}} affiliation
            ON e.resource_type = 'OrganizationAffiliation'
           AND affiliation.source_id = e.source_id
           AND affiliation.resource_id = e.resource_id
          JOIN LATERAL (
              SELECT DISTINCT normalized_ref AS resource_id
                FROM (
                    VALUES
                        (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                        (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                ) AS refs(normalized_ref)
               WHERE normalized_ref IS NOT NULL
          ) AS organization_ref ON TRUE
          JOIN {{ORGANIZATION_REF}} organization
            ON organization.source_id = affiliation.source_id
           AND organization.resource_id = organization_ref.resource_id
           AND organization.npi = e.npi
          LEFT JOIN {{LOCATION_REF}} loc
            ON loc.source_id = e.source_id
           AND loc.resource_id = e.location_resource_id
          LEFT JOIN LATERAL (
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_catalog.provider_directory_network_name)
                        FILTER (WHERE NULLIF(BTRIM(network_catalog.provider_directory_network_name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_catalog.network_resource_id,
                            'name', network_catalog.provider_directory_network_name,
                            'aliases', COALESCE(network_catalog.aliases::jsonb, '[]'::jsonb),
                            'provider_directory_network_key', network_catalog.provider_directory_network_key,
                            'provider_directory_source', 'provider_directory_fhir',
                            'provider_directory_source_id', affiliation.source_id,
                            'provider_directory_org_name', network_catalog.source_org_name,
                            'provider_directory_plan_name', network_catalog.source_plan_name,
                            'provider_directory_issuer_key', network_catalog.provider_directory_issuer_key,
                            'provider_directory_issuer_network_match_key',
                                network_catalog.provider_directory_issuer_network_match_key
                        )
                    ) FILTER (WHERE network_catalog.network_resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM jsonb_array_elements_text(
                  COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
              ) AS network_ref(value)
              LEFT JOIN {{NETWORK_CATALOG_REF}} network_catalog
                ON network_catalog.source_id = affiliation.source_id
               AND network_catalog.network_resource_id = {{NETWORK_REF_RESOURCE_ID_SQL}}
          ) network_context ON TRUE
          JOIN {{SOURCE_REF}} src
            ON src.source_id = affiliation.source_id
         WHERE e.npi IS NOT NULL
           AND e.address_key IS NOT NULL
    ),
    matches AS (
        SELECT * FROM practitioner_matches
        UNION ALL
        SELECT * FROM organization_affiliation_matches
    )
    SELECT
        source_key,
        snapshot_id,
        plan_id,
        ptg_plan_id,
        npi,
        location_key,
        address_key,
        zip5,
        state_code,
        city_norm,
        provider_directory_source_id,
        provider_directory_org_name,
        provider_directory_plan_name,
        provider_directory_provider_resource_id,
        provider_directory_provider_name,
        provider_directory_role_resource_id,
        provider_directory_location_resource_id,
        provider_directory_location_name,
        provider_directory_telephone_number,
        provider_directory_phone_number,
        provider_directory_phone_extension,
        provider_directory_fax_number,
        provider_directory_fax_number_digits,
        provider_directory_fax_extension,
        provider_directory_network_refs,
        provider_directory_insurance_plan_refs,
        provider_directory_specialty_codes,
        provider_directory_role_codes,
        provider_directory_match_type,
        provider_directory_active_role,
        provider_directory_active_provider,
        provider_directory_active_location,
        provider_directory_observed_at,
        (
            provider_directory_active_role
            AND provider_directory_active_provider
            AND provider_directory_active_location
        ) AS provider_directory_active_match,
        CASE
            WHEN provider_directory_active_role
             AND provider_directory_active_provider
             AND provider_directory_active_location
             AND provider_directory_plan_context_matched
                THEN 'payer_directory_corroborated_location'
            WHEN provider_directory_active_role
             AND provider_directory_active_provider
             AND provider_directory_active_location
                THEN 'provider_directory_address'
            ELSE 'payer_directory_corroborated_location_inactive_or_unknown'
        END::varchar AS address_network_binding,
        jsonb_build_object(
            'source', 'provider_directory_fhir',
            'matched_on',
                CASE
                    WHEN provider_directory_plan_context_matched
                        THEN 'npi_address_key_role_location_plan'
                    ELSE 'npi_address_key_role_location'
                END,
            'source_id', provider_directory_source_id,
            'org_name', provider_directory_org_name,
            'plan_name', provider_directory_plan_name,
            'provider_directory_source_id', provider_directory_source_id,
            'provider_directory_org_name', provider_directory_org_name,
            'provider_directory_plan_name', provider_directory_plan_name,
            'provider_resource_id', provider_directory_provider_resource_id,
            'role_resource_id', provider_directory_role_resource_id,
            'location_resource_id', provider_directory_location_resource_id,
            'match_type', provider_directory_match_type,
            'plan_context_matched', provider_directory_plan_context_matched,
            'network_context_present', provider_directory_network_context_present,
            'network_names', provider_directory_network_names,
            'network_matches', provider_directory_network_matches,
            'insurance_plan_matches', provider_directory_insurance_plan_matches
        ) AS address_verification_evidence,
        provider_directory_plan_context_matched,
        provider_directory_network_context_present,
        provider_directory_insurance_plan_matches,
        provider_directory_network_names,
        provider_directory_network_matches
      FROM matches;
