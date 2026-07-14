-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_source" (
    source_id varchar(64) PRIMARY KEY,
    endpoint_id varchar(64) NOT NULL,
    canonical_api_base text,
    org_name varchar(256) NOT NULL,
    plan_name varchar(512)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_practitioner" (
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
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_practitioner_role" (
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
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_organization" (
    source_id varchar(64) NOT NULL,
    resource_id varchar(256) NOT NULL,
    name varchar(512),
    active boolean,
    type_codes jsonb,
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_healthcare_service" (
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
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_organization_affiliation" (
    source_id varchar(64) NOT NULL,
    resource_id varchar(256) NOT NULL,
    active boolean,
    identifiers jsonb,
    organization_ref text,
    participating_organization_ref text,
    network_refs jsonb,
    location_refs jsonb,
    healthcare_service_refs jsonb,
    specialty_codes jsonb,
    code_codes jsonb,
    telecom jsonb,
    period_start varchar(64),
    period_end varchar(64),
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_endpoint" (
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
    updated_at timestamp,
    PRIMARY KEY (source_id, resource_id)
);
-- statement
CREATE TABLE "{{SCHEMA}}"."provider_directory_dataset_affiliation_organization" (
    dataset_id varchar(96) NOT NULL,
    participating_organization_resource_id varchar(256) NOT NULL,
    affiliation_resource_id varchar(256) NOT NULL,
    PRIMARY KEY (
        dataset_id,
        participating_organization_resource_id,
        affiliation_resource_id
    )
);
