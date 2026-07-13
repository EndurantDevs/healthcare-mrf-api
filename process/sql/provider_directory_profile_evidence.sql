INSERT INTO {{TARGET_REF}} ("evidence_key", "npi", "fact_type", "fact_key", "value_json", "source_id", "endpoint_id", "dataset_id", "canonical_api_base", "source_org_name", "source_plan_name", "resource_type", "resource_id", "role_resource_id", "active", "effective_start", "effective_end", "observed_at")
        WITH selected_dataset(source_id, dataset_id) AS MATERIALIZED (
            SELECT *
              FROM unnest(
                    CAST(:source_ids AS varchar[]),
                    CAST(:dataset_ids AS varchar[])
              )
        ), source_context AS MATERIALIZED (
            SELECT source.source_id,
                   source.endpoint_id,
                   selected_dataset.dataset_id,
                   source.canonical_api_base,
                   source.org_name,
                   source.plan_name
              FROM {{SOURCE_REF}} AS source
              JOIN selected_dataset
                ON selected_dataset.source_id = source.source_id
        ), practitioner_rows AS MATERIALIZED (
            SELECT practitioner.*, source_context.endpoint_id,
                   source_context.dataset_id,
                   source_context.canonical_api_base,
                   source_context.org_name AS source_org_name,
                   source_context.plan_name AS source_plan_name
              FROM {{PRACTITIONER_REF}} AS practitioner
              JOIN source_context
                ON source_context.source_id = practitioner.source_id
             WHERE practitioner.npi IS NOT NULL
        ), role_rows AS MATERIALIZED (
            SELECT role.*,
                   COALESCE(role.npi, practitioner.npi) AS resolved_npi,
                   source_context.endpoint_id,
                   source_context.dataset_id,
                   source_context.canonical_api_base,
                   source_context.org_name AS source_org_name,
                   source_context.plan_name AS source_plan_name
              FROM {{ROLE_REF}} AS role
              JOIN source_context
                ON source_context.source_id = role.source_id
              LEFT JOIN {{PRACTITIONER_REF}} AS practitioner
                ON practitioner.source_id = role.source_id
               AND practitioner.resource_id = NULLIF(BTRIM(CASE WHEN role.practitioner_ref LIKE '%/Practitioner/%' THEN regexp_replace(role.practitioner_ref, '^.*/Practitioner/', '') WHEN role.practitioner_ref LIKE 'Practitioner/%' THEN regexp_replace(role.practitioner_ref, '^Practitioner/', '') ELSE role.practitioner_ref END), '')
             WHERE COALESCE(role.npi, practitioner.npi) IS NOT NULL
        ), qualification_values AS MATERIALIZED (
            SELECT practitioner_rows.*,
                   qualification.value AS qualification
              FROM practitioner_rows
              CROSS JOIN LATERAL jsonb_array_elements(
                   CASE
                     WHEN jsonb_typeof(COALESCE(practitioner_rows.qualifications::jsonb, 'null'::jsonb)) = 'array'
                      AND jsonb_array_length(COALESCE(practitioner_rows.qualifications::jsonb, '[]'::jsonb)) > 0
                     THEN practitioner_rows.qualifications::jsonb
                     ELSE COALESCE(
                          (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'code_codes', jsonb_build_array(legacy_code.value)
                                )
                            )
                              FROM jsonb_array_elements(
                                   COALESCE(practitioner_rows.qualification_codes::jsonb, '[]'::jsonb)
                              ) AS legacy_code(value)
                          ),
                          '[]'::jsonb
                     )
                   END
              ) AS qualification(value)
        ), qualification_codings AS MATERIALIZED (
            SELECT qualification_values.*,
                   coding.value AS coding,
                   CASE
                     WHEN lower(COALESCE(coding.value ->> 'system', '')) LIKE '%nucc%'
                       OR lower(COALESCE(coding.value ->> 'system', '')) LIKE '%taxonomy%'
                       OR upper(COALESCE(coding.value ->> 'code', '')) ~ '^[0-9A-Z]{10}$'
                     THEN 'taxonomy_qualification'
                     WHEN lower(COALESCE(coding.value ->> 'system', '')) LIKE '%v2-0360%'
                       OR upper(COALESCE(coding.value ->> 'code', '')) IN ('MD','DO','DDS','DMD','DPM','OD','DC','NP','APRN','APRN-CNP','PA','PA-C','RN','LPN','CRNA','CNM','CNS','FNP','FNP-C','MSN','PHD','PSYD','LCSW','LICSW','LMFT','LPC','PT','DPT','OT','OTR','RD','RDN','RPH','PHARMD')
                     THEN 'credential'
                     ELSE 'qualification'
                   END AS qualification_type
              FROM qualification_values
              CROSS JOIN LATERAL jsonb_array_elements(
                   CASE
                     WHEN jsonb_typeof(qualification_values.qualification -> 'code_codes') = 'array'
                      AND jsonb_array_length(qualification_values.qualification -> 'code_codes') > 0
                     THEN qualification_values.qualification -> 'code_codes'
                     WHEN NULLIF(qualification_values.qualification ->> 'code_text', '') IS NOT NULL
                     THEN jsonb_build_array(
                          jsonb_build_object(
                              'text', qualification_values.qualification ->> 'code_text'
                          )
                     )
                     ELSE jsonb_build_array(jsonb_build_object('text', 'Unspecified qualification'))
                   END
              ) AS coding(value)
        ), role_service_rows AS MATERIALIZED (
            SELECT role_rows.resolved_npi AS npi,
                   role_rows.source_id,
                   role_rows.endpoint_id,
                   role_rows.dataset_id,
                   role_rows.canonical_api_base,
                   role_rows.source_org_name,
                   role_rows.source_plan_name,
                   role_rows.resource_id AS role_resource_id,
                   service.resource_id, service.active, service.name,
                   service.type_codes, service.category_codes,
                   service.specialty_codes, service.program_codes,
                   service.characteristic_codes,
                   service.communication_codes,
                   service.referral_method_codes,
                   service.service_provision_codes, service.eligibility,
                   service.appointment_required, service.telecom,
                   service.available_time, service.not_available,
                   service.availability_exceptions, service.extra_details,
                   service.updated_at
              FROM role_rows
              CROSS JOIN LATERAL jsonb_array_elements_text(
                   COALESCE(role_rows.healthcare_service_refs::jsonb, '[]'::jsonb)
              ) AS service_reference(value)
              JOIN {{SERVICE_REF}} AS service
                ON service.source_id = role_rows.source_id
               AND service.resource_id = NULLIF(BTRIM(CASE WHEN service_reference.value LIKE '%/HealthcareService/%' THEN regexp_replace(service_reference.value, '^.*/HealthcareService/', '') WHEN service_reference.value LIKE 'HealthcareService/%' THEN regexp_replace(service_reference.value, '^HealthcareService/', '') ELSE service_reference.value END), '')
        ), direct_service_rows AS MATERIALIZED (
            SELECT service.npi,
                   service.source_id,
                   source_context.endpoint_id,
                   source_context.dataset_id,
                   source_context.canonical_api_base,
                   source_context.org_name AS source_org_name,
                   source_context.plan_name AS source_plan_name,
                   NULL::varchar AS role_resource_id,
                   service.resource_id, service.active, service.name,
                   service.type_codes, service.category_codes,
                   service.specialty_codes, service.program_codes,
                   service.characteristic_codes,
                   service.communication_codes,
                   service.referral_method_codes,
                   service.service_provision_codes, service.eligibility,
                   service.appointment_required, service.telecom,
                   service.available_time, service.not_available,
                   service.availability_exceptions, service.extra_details,
                   service.updated_at
              FROM {{SERVICE_REF}} AS service
              JOIN source_context
                ON source_context.source_id = service.source_id
             WHERE service.npi IS NOT NULL
        ), service_rows AS MATERIALIZED (
            SELECT * FROM role_service_rows
            UNION ALL
            SELECT * FROM direct_service_rows
        ), facts AS (
            SELECT practitioner.npi,
                   'name'::varchar AS fact_type,
                   md5(lower(COALESCE(name.value ->> 'text', practitioner.full_name, ''))) AS fact_key,
                   jsonb_strip_nulls(
                       name.value || jsonb_build_object(
                           'text', COALESCE(name.value ->> 'text', practitioner.full_name),
                           'family', COALESCE(name.value ->> 'family', practitioner.family_name),
                           'given', COALESCE(name.value -> 'given', practitioner.given_names::jsonb)
                       )
                   ) AS value_json,
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner'::varchar AS resource_type,
                   practitioner.resource_id, NULL::varchar AS role_resource_id,
                   practitioner.active, NULL::varchar AS effective_start,
                   NULL::varchar AS effective_end, practitioner.updated_at AS observed_at
              FROM practitioner_rows AS practitioner
              CROSS JOIN LATERAL jsonb_array_elements(
                   CASE
                     WHEN jsonb_typeof(COALESCE(practitioner.names::jsonb, 'null'::jsonb)) = 'array'
                      AND jsonb_array_length(COALESCE(practitioner.names::jsonb, '[]'::jsonb)) > 0
                     THEN practitioner.names::jsonb
                     ELSE jsonb_build_array(
                          jsonb_strip_nulls(
                              jsonb_build_object(
                                  'text', practitioner.full_name,
                                  'family', practitioner.family_name,
                                  'given', practitioner.given_names::jsonb
                              )
                          )
                     )
                   END
              ) AS name(value)
             WHERE NULLIF(COALESCE(name.value ->> 'text', practitioner.full_name, ''), '') IS NOT NULL

            UNION ALL
            SELECT practitioner.npi, 'administrative_gender',
                   md5(practitioner.administrative_gender),
                   jsonb_build_object(
                       'code', practitioner.administrative_gender,
                       'label', 'FHIR administrative gender'
                   ),
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner', practitioner.resource_id, NULL::varchar,
                   practitioner.active, NULL::varchar, NULL::varchar,
                   practitioner.updated_at
              FROM practitioner_rows AS practitioner
             WHERE practitioner.administrative_gender IS NOT NULL

            UNION ALL
            SELECT practitioner.npi, 'age',
                   md5(practitioner.age_years::text),
                   jsonb_build_object(
                       'years', practitioner.age_years,
                       'as_of', practitioner.age_as_of,
                       'derivation', 'FHIR Practitioner.birthDate'
                   ),
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner', practitioner.resource_id, NULL::varchar,
                   practitioner.active, NULL::varchar, NULL::varchar,
                   practitioner.updated_at
              FROM practitioner_rows AS practitioner
             WHERE practitioner.age_years IS NOT NULL
               AND practitioner.age_as_of IS NOT NULL

            UNION ALL
            SELECT practitioner.npi, 'years_of_practice',
                   md5(
                       concat_ws(
                           '|',
                           practitioner.years_of_practice::text,
                           practitioner.years_of_practice_basis,
                           practitioner.years_of_practice_start_date
                       )
                   ),
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'years', practitioner.years_of_practice,
                           'as_of', practitioner.years_of_practice_as_of,
                           'estimated', true,
                           'basis', practitioner.years_of_practice_basis,
                           'basis_start_date',
                               practitioner.years_of_practice_start_date
                       )
                   ),
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner', practitioner.resource_id, NULL::varchar,
                   practitioner.active, NULL::varchar, NULL::varchar,
                   practitioner.updated_at
              FROM practitioner_rows AS practitioner
             WHERE practitioner.years_of_practice IS NOT NULL
               AND practitioner.years_of_practice_as_of IS NOT NULL
               AND practitioner.years_of_practice_basis IS NOT NULL

            UNION ALL
            SELECT qualification.npi, qualification.qualification_type,
                   md5(
                       lower(
                           concat_ws('|',
                               qualification.coding ->> 'system',
                               qualification.coding ->> 'code',
                               qualification.coding ->> 'display',
                               qualification.coding ->> 'text'
                           )
                       )
                   ),
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'coding', qualification.coding,
                           'classification', qualification.qualification_type
                       )
                   ),
                   qualification.source_id, qualification.endpoint_id,
                   qualification.dataset_id, qualification.canonical_api_base,
                   qualification.source_org_name, qualification.source_plan_name,
                   'Practitioner', qualification.resource_id, NULL::varchar,
                   qualification.active, qualification.qualification ->> 'period_start',
                   qualification.qualification ->> 'period_end', qualification.updated_at
              FROM qualification_codings AS qualification

            UNION ALL
            SELECT qualification.npi, 'qualification_detail',
                   md5(qualification.qualification::text),
                   qualification.qualification,
                   qualification.source_id, qualification.endpoint_id,
                   qualification.dataset_id, qualification.canonical_api_base,
                   qualification.source_org_name, qualification.source_plan_name,
                   'Practitioner', qualification.resource_id, NULL::varchar,
                   qualification.active, qualification.qualification ->> 'period_start',
                   qualification.qualification ->> 'period_end', qualification.updated_at
              FROM qualification_values AS qualification
             WHERE qualification.qualification ?| ARRAY[
                       'issuer_ref', 'issuer_display', 'identifiers',
                       'period_start', 'period_end'
                   ]

            UNION ALL
            SELECT practitioner.npi, 'language',
                   md5(lower(communication.value::text)), communication.value,
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner', practitioner.resource_id, NULL::varchar,
                   practitioner.active, NULL::varchar, NULL::varchar,
                   practitioner.updated_at
              FROM practitioner_rows AS practitioner
              CROSS JOIN LATERAL jsonb_array_elements(
                   CASE
                     WHEN jsonb_typeof(COALESCE(practitioner.communications::jsonb, 'null'::jsonb)) = 'array'
                      AND jsonb_array_length(COALESCE(practitioner.communications::jsonb, '[]'::jsonb)) > 0
                     THEN practitioner.communications::jsonb
                     ELSE COALESCE(
                          (
                            SELECT jsonb_agg(
                                jsonb_build_object('codes', jsonb_build_array(language_code.value))
                            )
                              FROM jsonb_array_elements(
                                   COALESCE(practitioner.communication_codes::jsonb, '[]'::jsonb)
                              ) AS language_code(value)
                          ),
                          '[]'::jsonb
                     )
                   END
              ) AS communication(value)

            UNION ALL
            SELECT practitioner.npi, 'contact',
                   md5(
                       lower(
                           concat_ws('|',
                               contact.value ->> 'system',
                               regexp_replace(COALESCE(contact.value ->> 'value', ''), '[^0-9A-Za-z@.+]', '', 'g'),
                               contact.value ->> 'use'
                           )
                       )
                   ),
                   contact.value,
                   practitioner.source_id, practitioner.endpoint_id,
                   practitioner.dataset_id, practitioner.canonical_api_base,
                   practitioner.source_org_name, practitioner.source_plan_name,
                   'Practitioner', practitioner.resource_id, NULL::varchar,
                   practitioner.active, NULL::varchar, NULL::varchar,
                   practitioner.updated_at
              FROM practitioner_rows AS practitioner
              CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(practitioner.telecom::jsonb, '[]'::jsonb)
              ) AS contact(value)
             WHERE NULLIF(contact.value ->> 'value', '') IS NOT NULL

            UNION ALL
            SELECT role.resolved_npi, 'specialty',
                   md5(lower(specialty.value::text)), specialty.value,
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role
              CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(role.specialty_codes::jsonb, '[]'::jsonb)
              ) AS specialty(value)

            UNION ALL
            SELECT role.resolved_npi, 'role',
                   md5(lower(role_code.value::text)), role_code.value,
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role
              CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(role.code_codes::jsonb, '[]'::jsonb)
              ) AS role_code(value)

            UNION ALL
            SELECT role.resolved_npi, 'role_context',
                   md5(concat_ws('|', role.source_id, role.resource_id)),
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'organization_ref', role.organization_ref,
                           'location_refs', role.location_refs::jsonb,
                           'healthcare_service_refs', role.healthcare_service_refs::jsonb,
                           'network_refs', role.network_refs::jsonb,
                           'insurance_plan_refs', role.insurance_plan_refs::jsonb,
                           'specialty_codes', role.specialty_codes::jsonb,
                           'role_codes', role.code_codes::jsonb,
                           'telecom', role.telecom::jsonb,
                           'available_time', role.available_time::jsonb,
                           'not_available', role.not_available::jsonb,
                           'availability_exceptions', role.availability_exceptions,
                           'new_patient_acceptance', role.new_patient_acceptance::jsonb,
                           'telehealth', role.telehealth::jsonb,
                           'accepting_medicaid', role.accepting_medicaid,
                           'period_start', role.period_start,
                           'period_end', role.period_end
                       )
                   ),
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role

            UNION ALL
            SELECT role.resolved_npi, 'new_patient_acceptance',
                   md5(acceptance.value::text), acceptance.value,
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role
              CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(role.new_patient_acceptance::jsonb, '[]'::jsonb)
              ) AS acceptance(value)

            UNION ALL
            SELECT role.resolved_npi, 'telehealth',
                   md5(telehealth.value::text), telehealth.value,
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role
              CROSS JOIN LATERAL jsonb_array_elements(
                   COALESCE(role.telehealth::jsonb, '[]'::jsonb)
              ) AS telehealth(value)

            UNION ALL
            SELECT role.resolved_npi, 'accepting_medicaid',
                   md5(role.accepting_medicaid::text),
                   jsonb_build_object('accepted', role.accepting_medicaid),
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'PractitionerRole', role.resource_id,
                   role.resource_id, role.active, role.period_start,
                   role.period_end, role.updated_at
              FROM role_rows AS role
             WHERE role.accepting_medicaid IS NOT NULL

            UNION ALL
            SELECT role.resolved_npi, 'organization',
                   md5(lower(COALESCE(organization.name, role.organization_ref, ''))),
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'name', organization.name,
                           'active', organization.active,
                           'type_codes', organization.type_codes::jsonb
                       )
                   ),
                   role.source_id, role.endpoint_id, role.dataset_id,
                   role.canonical_api_base, role.source_org_name,
                   role.source_plan_name, 'Organization',
                   COALESCE(organization.resource_id, role.resource_id),
                   role.resource_id, organization.active, role.period_start,
                   role.period_end, GREATEST(role.updated_at, organization.updated_at)
              FROM role_rows AS role
              LEFT JOIN {{ORGANIZATION_REF}} AS organization
                ON organization.source_id = role.source_id
               AND organization.resource_id = NULLIF(BTRIM(CASE WHEN role.organization_ref LIKE '%/Organization/%' THEN regexp_replace(role.organization_ref, '^.*/Organization/', '') WHEN role.organization_ref LIKE 'Organization/%' THEN regexp_replace(role.organization_ref, '^Organization/', '') ELSE role.organization_ref END), '')
             WHERE NULLIF(COALESCE(organization.name, role.organization_ref, ''), '') IS NOT NULL

            UNION ALL
            SELECT service.npi, 'service',
                   md5(
                       jsonb_strip_nulls(
                           jsonb_build_object(
                               'name', service.name,
                               'type_codes', service.type_codes::jsonb,
                               'category_codes', service.category_codes::jsonb,
                               'specialty_codes', service.specialty_codes::jsonb,
                               'program_codes', service.program_codes::jsonb,
                               'characteristic_codes', service.characteristic_codes::jsonb,
                               'communication_codes', service.communication_codes::jsonb,
                               'referral_method_codes', service.referral_method_codes::jsonb,
                               'service_provision_codes', service.service_provision_codes::jsonb,
                               'eligibility', service.eligibility::jsonb,
                               'appointment_required', service.appointment_required,
                               'telecom', service.telecom::jsonb,
                               'available_time', service.available_time::jsonb,
                               'not_available', service.not_available::jsonb,
                               'availability_exceptions', service.availability_exceptions,
                               'extra_details', service.extra_details
                           )
                       )::text
                   ),
                   jsonb_strip_nulls(
                       jsonb_build_object(
                           'name', service.name,
                           'type_codes', service.type_codes::jsonb,
                           'category_codes', service.category_codes::jsonb,
                           'specialty_codes', service.specialty_codes::jsonb,
                           'program_codes', service.program_codes::jsonb,
                           'characteristic_codes', service.characteristic_codes::jsonb,
                           'communication_codes', service.communication_codes::jsonb,
                           'referral_method_codes', service.referral_method_codes::jsonb,
                           'service_provision_codes', service.service_provision_codes::jsonb,
                           'eligibility', service.eligibility::jsonb,
                           'appointment_required', service.appointment_required,
                           'telecom', service.telecom::jsonb,
                           'available_time', service.available_time::jsonb,
                           'not_available', service.not_available::jsonb,
                           'availability_exceptions', service.availability_exceptions,
                           'extra_details', service.extra_details
                       )
                   ),
                   service.source_id, service.endpoint_id, service.dataset_id,
                   service.canonical_api_base, service.source_org_name,
                   service.source_plan_name, 'HealthcareService',
                   service.resource_id, service.role_resource_id, service.active,
                   NULL::varchar, NULL::varchar, service.updated_at
              FROM service_rows AS service
        ), normalized_facts AS MATERIALIZED (
            SELECT DISTINCT ON (
                       npi, fact_type, fact_key, source_id,
                       resource_type, resource_id,
                       COALESCE(role_resource_id, '')
                   )
                   md5(
                       concat_ws('|',
                           npi::text, fact_type, fact_key, source_id,
                           resource_type, resource_id,
                           COALESCE(role_resource_id, '')
                       )
                   ) AS evidence_key,
                   npi, fact_type, fact_key, value_json, source_id,
                   endpoint_id, dataset_id, canonical_api_base,
                   source_org_name, source_plan_name, resource_type,
                   resource_id, role_resource_id, active, effective_start,
                   effective_end, observed_at
              FROM facts
             WHERE npi IS NOT NULL
               AND value_json IS NOT NULL
               AND value_json <> '{}'::jsonb
             ORDER BY npi, fact_type, fact_key, source_id,
                      resource_type, resource_id,
                      COALESCE(role_resource_id, ''), observed_at DESC NULLS LAST
        )
        SELECT evidence_key, npi, fact_type, fact_key, value_json,
               source_id, endpoint_id, dataset_id, canonical_api_base,
               source_org_name, source_plan_name, resource_type,
               resource_id, role_resource_id, active, effective_start,
               effective_end, observed_at
          FROM normalized_facts
        ON CONFLICT (evidence_key) DO NOTHING;
