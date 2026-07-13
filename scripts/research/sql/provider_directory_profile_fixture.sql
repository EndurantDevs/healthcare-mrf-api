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
            specialty_codes jsonb,
            code_codes jsonb,
            location_refs jsonb,
            network_refs jsonb,
            insurance_plan_refs jsonb,
            telecom jsonb,
            available_time jsonb,
            not_available jsonb,
            availability_exceptions text,
            new_patient_acceptance jsonb,
            telehealth jsonb,
            active boolean,
            period_start varchar(64),
            period_end varchar(64),
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );
        CREATE TABLE "{{SCHEMA}}"."organization" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            name varchar(512),
            active boolean,
            type_codes jsonb,
            updated_at timestamp without time zone,
            PRIMARY KEY (source_id, resource_id)
        );
        CREATE TABLE "{{SCHEMA}}"."service" (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            npi bigint,
            active boolean,
            name varchar(512),
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
            telecom jsonb,
            available_time jsonb,
            not_available jsonb,
            availability_exceptions text,
            extra_details text,
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

        INSERT INTO "{{SCHEMA}}"."organization" VALUES
            ('source_a', 'organization-a', 'Rivera Medical Group', true, '[]', '2026-07-13 12:00:00'),
            ('source_b', 'organization-b', 'Rivera Medical Group', true, '[]', '2026-07-13 12:05:00');

        INSERT INTO "{{SCHEMA}}"."role" VALUES
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

        INSERT INTO "{{SCHEMA}}"."service" VALUES
        (
            'source_a', 'service-a', NULL, true, 'Primary Care',
            '[{"code":"primary-care"}]', '[]',
            '[{"code":"207Q00000X","display":"Family Medicine"}]',
            '[]', '[]', '[]', '[]', '[]', '[]', true,
            '[{"system":"phone","value":"312-555-0100"}]', '[]', '[]',
            NULL, 'Routine primary care', '2026-07-13 12:00:00'
        );
