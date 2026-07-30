CREATE TABLE "{{SCHEMA}}"."source" (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            canonical_api_base text,
            org_name varchar(256),
            plan_name varchar(512)
        );
        CREATE TABLE "{{SCHEMA}}"."practitioner" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            active boolean,
            names jsonb,
            full_name varchar(512),
            family_name varchar(256),
            given_names jsonb,
            administrative_gender varchar(32),
            age_years integer,
            age_as_of varchar(10),
            years_of_practice integer,
            years_of_practice_as_of varchar(10),
            years_of_practice_basis varchar(128),
            years_of_practice_start_date varchar(10),
            qualifications jsonb,
            qualification_codes jsonb,
            communications jsonb,
            communication_codes jsonb,
            telecom jsonb,
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );
        CREATE TABLE "{{SCHEMA}}"."role" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            practitioner_ref text,
            organization_ref text,
            healthcare_service_refs jsonb,
            endpoint_refs jsonb,
            identifiers jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            location_refs jsonb,
            network_refs jsonb,
            insurance_plan_refs jsonb,
            telecom jsonb,
            accepting_patients jsonb,
            available_time jsonb,
            not_available jsonb,
            availability_exceptions text,
            new_patient_acceptance jsonb,
            telehealth jsonb,
            accepting_medicaid boolean,
            active boolean,
            period_start varchar(64),
            period_end varchar(64),
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );
        CREATE TABLE "{{SCHEMA}}"."organization" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            tax_id varchar(64),
            tin_status varchar(64),
            name varchar(512),
            active boolean,
            identifiers jsonb,
            type_codes jsonb,
            telecom jsonb,
            address_json jsonb,
            source_lineage jsonb,
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );
        CREATE TABLE "{{SCHEMA}}"."service" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            active boolean,
            name varchar(512),
            identifiers jsonb,
            type_codes jsonb,
            category_codes jsonb,
            specialty_codes jsonb,
            program_codes jsonb,
            characteristic_codes jsonb,
            communication_codes jsonb,
            referral_method_codes jsonb,
            service_provision_codes jsonb,
            eligibility jsonb,
            appointment_required boolean,
            accepting_patients jsonb,
            telecom jsonb,
            available_time jsonb,
            not_available jsonb,
            availability_exceptions text,
            extra_details text,
            comment text,
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );

        CREATE TABLE "{{SCHEMA}}"."affiliation" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            active boolean,
            identifiers jsonb,
            organization_ref text,
            participating_organization_ref text,
            network_refs jsonb,
            insurance_plan_refs jsonb,
            location_refs jsonb,
            healthcare_service_refs jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            telecom jsonb,
            plan_scope jsonb,
            network_tier varchar(128),
            network_key_id varchar(64),
            relationship_type varchar(64),
            ownership_status varchar(64),
            source_lineage jsonb,
            period_start varchar(64),
            period_end varchar(64),
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );

        CREATE TABLE "{{SCHEMA}}"."affiliation_organization" (
            dataset_id varchar(96) NOT NULL,
            participating_organization_resource_id varchar(256) NOT NULL,
            affiliation_resource_id varchar(256) NOT NULL,
            PRIMARY KEY (
                dataset_id,
                participating_organization_resource_id,
                affiliation_resource_id
            )
        );

        CREATE TABLE "{{SCHEMA}}"."endpoint" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            status varchar(64),
            connection_type_system text,
            connection_type_code text,
            connection_type_display text,
            name varchar(512),
            managing_organization_ref text,
            contact jsonb,
            period_start varchar(64),
            period_end varchar(64),
            payload_type_codes jsonb,
            payload_mime_types jsonb,
            address text,
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );


        INSERT INTO "{{SCHEMA}}"."source" VALUES
            ('source_a', 'endpoint_a', 'https://a.example/fhir', 'Carrier A', 'Plan A'),
            ('source_b', 'endpoint_b', 'https://b.example/fhir', 'Carrier B', 'Plan B');

        INSERT INTO "{{SCHEMA}}"."practitioner" VALUES
        (
            'source_a', 'practitioner-a', 1588616783, true,
            '[{"text":"Alex Rivera, MD","family":"Rivera","given":["Alex"]}]',
            'Alex Rivera, MD', 'Rivera', '["Alex"]', 'female', 56,
            '2026-07-13', 25, '2026-07-13',
            'FHIR Practitioner.qualification.period.start', '2001-01-01',
            '[{"code_codes":[{"system":"http://terminology.hl7.org/CodeSystem/v2-0360","code":"MD","display":"Doctor of Medicine"}],"period_start":"2001-01-01","issuer_display":"Example Medical School"}]',
            '[]',
            '[{"codes":[{"system":"urn:ietf:bcp:47","code":"es","display":"Spanish"}],"text":"Spanish"}]',
            '[]',
            '[{"system":"phone","value":"312-555-0100","use":"work"}]',
            '2026-07-13 12:00:00'
        ),
        (
            'source_b', 'practitioner-b', 1588616783, true,
            '[{"text":"Alex Rivera, MD","family":"Rivera","given":["Alex"]}]',
            'Alex Rivera, MD', 'Rivera', '["Alex"]', 'female', 56,
            '2026-07-13', 25, '2026-07-13',
            'FHIR Practitioner.qualification.period.start', '2001-01-01',
            '[{"code_codes":[{"system":"http://nucc.org/provider-taxonomy","code":"207Q00000X","display":"Family Medicine"}],"period_start":"2001-01-01"}]',
            '[]',
            '[{"codes":[{"system":"urn:ietf:bcp:47","code":"es","display":"Spanish"}],"text":"Spanish"}]',
            '[]',
            '[{"system":"phone","value":"3125550100","use":"work"}]',
            '2026-07-13 12:05:00'
        );

        INSERT INTO "{{SCHEMA}}"."organization" (
            source_id, resource_id, name, active, type_codes, updated_at
        ) VALUES
            ('source_a', 'organization-a', 'Rivera Medical Group', true, '[]', '2026-07-13 12:00:00'),
            ('source_b', 'organization-b', 'Rivera Medical Group', true, '[]', '2026-07-13 12:05:00');

        INSERT INTO "{{SCHEMA}}"."role" (
            source_id, resource_id, npi, practitioner_ref,
            organization_ref, healthcare_service_refs, specialty_codes,
            code_codes, location_refs, network_refs, insurance_plan_refs,
            telecom, available_time, not_available,
            availability_exceptions, new_patient_acceptance, telehealth,
            active, period_start, period_end, updated_at
        ) VALUES
        (
            'source_a', 'role-a', 1588616783, 'Practitioner/practitioner-a',
            'Organization/organization-a', '["HealthcareService/service-a"]',
            '[{"system":"http://nucc.org/provider-taxonomy","code":"207Q00000X","display":"Family Medicine"}]',
            '[{"code":"doctor","display":"Doctor"}]', '[]',
            '["Organization/network-a"]', '["InsurancePlan/plan-a"]',
            '[{"system":"phone","value":"312-555-0100"}]', '[]', '[]',
            NULL, '[{"code":"accepting"}]', '[{"code":"video"}]',
            true, '2026-01-01', NULL, '2026-07-13 12:00:00'
        ),
        (
            'source_b', 'role-b', 1588616783, 'Practitioner/practitioner-b',
            'Organization/organization-b', '[]',
            '[{"system":"http://nucc.org/provider-taxonomy","code":"207Q00000X","display":"Family Medicine"}]',
            '[{"code":"doctor","display":"Doctor"}]', '[]',
            '["Organization/network-b"]', '["InsurancePlan/plan-b"]',
            '[{"system":"phone","value":"3125550100"}]', '[]', '[]',
            NULL, '[{"code":"accepting"}]', '[{"code":"video"}]',
            true, '2026-01-01', NULL, '2026-07-13 12:05:00'
        );

        INSERT INTO "{{SCHEMA}}"."service" (
            source_id, resource_id, npi, active, name, type_codes,
            category_codes, specialty_codes, program_codes,
            characteristic_codes, communication_codes,
            referral_method_codes, service_provision_codes, eligibility,
            appointment_required, telecom, available_time, not_available,
            availability_exceptions, extra_details, updated_at
        ) VALUES
        (
            'source_a', 'service-a', NULL, true, 'Primary Care',
            '[{"code":"primary-care"}]', '[]',
            '[{"code":"207Q00000X","display":"Family Medicine"}]',
            '[]', '[]', '[]', '[]', '[]', '[]', true,
            '[{"system":"phone","value":"312-555-0100"}]', '[]', '[]',
            NULL, 'Routine primary care', '2026-07-13 12:00:00'
        );
