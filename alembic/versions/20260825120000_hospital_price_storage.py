"""Add normalized hospital-price storage and publication state.

Revision ID: 20260825120000_hospital_price_storage
Revises: 20260820200000_provider_directory_projection_finalizer

The migration creates storage only. It neither registers hospitals nor starts
imports. Publications replace the per-hospital current pointer with an
optimistic generation predicate, leaving the prior pointer intact on failure.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260825120000_hospital_price_storage"
down_revision = "20260820200000_provider_directory_projection_finalizer"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _execute(sql: str) -> None:
    for statement in sql.split(";"):
        if statement.strip():
            op.execute(statement)


_CONTROL_DDL = """
CREATE TABLE {schema}.hospital_price_locator (
    locator_id varchar(64) PRIMARY KEY,
    cms_hpt_url text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT hospital_price_locator_url_key UNIQUE (cms_hpt_url),
    CONSTRAINT hospital_price_locator_id_check CHECK (
        locator_id = btrim(locator_id) AND locator_id <> ''
    )
);
CREATE TABLE {schema}.hospital_price_locator_observation (
    observation_id varchar(64) PRIMARY KEY, locator_id varchar(64) NOT NULL,
    registry_version integer NOT NULL, requested_url text NOT NULL,
    final_url text, result_status varchar(32) NOT NULL, http_status integer,
    response_sha256 varchar(64), response_byte_count bigint,
    checked_at timestamptz NOT NULL, error_code varchar(64), error_detail text,
    CONSTRAINT hospital_price_locator_observation_locator_fkey
        FOREIGN KEY (locator_id)
        REFERENCES {schema}.hospital_price_locator(locator_id),
    CONSTRAINT hospital_price_locator_observation_owner_key
        UNIQUE (locator_id, observation_id),
    CONSTRAINT hospital_price_locator_observation_shape_check CHECK (
        registry_version > 0 AND requested_url <> ''
        AND result_status = btrim(result_status) AND result_status <> ''
        AND (http_status IS NULL OR http_status BETWEEN 100 AND 599)
        AND ((response_sha256 IS NULL AND response_byte_count IS NULL) OR
             (response_sha256 ~ '^[0-9a-f]{{64}}$' AND response_byte_count > 0))
    )
);
CREATE INDEX hospital_price_locator_observation_checked_idx
    ON {schema}.hospital_price_locator_observation(locator_id, checked_at DESC);
CREATE TABLE {schema}.hospital_price_hospital (
    hospital_id varchar(64) PRIMARY KEY, facility_anchor_id varchar(128),
    locator_id varchar(64) NOT NULL, name varchar(256) NOT NULL,
    registry_version integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT hospital_price_hospital_facility_anchor_key
        UNIQUE (facility_anchor_id),
    CONSTRAINT hospital_price_hospital_locator_fkey
        FOREIGN KEY (locator_id)
        REFERENCES {schema}.hospital_price_locator(locator_id),
    CONSTRAINT hospital_price_hospital_identity_check CHECK (
        hospital_id = btrim(hospital_id) AND hospital_id <> ''
        AND name = btrim(name) AND name <> '' AND registry_version > 0
    )
);
CREATE INDEX hospital_price_hospital_locator_idx
    ON {schema}.hospital_price_hospital(locator_id);
CREATE TABLE {schema}.hospital_price_content (
    content_sha256 varchar(64) PRIMARY KEY, byte_count bigint NOT NULL,
    media_type varchar(128),
    acquired_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT hospital_price_content_identity_check CHECK (
        content_sha256 ~ '^[0-9a-f]{{64}}$' AND byte_count > 0
    )
);
CREATE TABLE {schema}.hospital_price_version (
    version_id varchar(64) PRIMARY KEY, content_sha256 varchar(64) NOT NULL,
    parser_contract_sha256 varchar(64) NOT NULL, semantic_sha256 varchar(64) NOT NULL,
    source_format varchar(16) NOT NULL, source_hospital_name text NOT NULL,
    last_updated_on date NOT NULL, template_version varchar(32) NOT NULL,
    attestation_text text NOT NULL, confirm_attestation boolean NOT NULL,
    attester_name text NOT NULL, location_count integer NOT NULL,
    npi_count integer NOT NULL, license_count integer NOT NULL,
    service_count bigint NOT NULL, charge_count bigint NOT NULL,
    payer_charge_count bigint NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    financial_aid_policy text,
    CONSTRAINT hospital_price_version_content_fkey
        FOREIGN KEY (content_sha256)
        REFERENCES {schema}.hospital_price_content(content_sha256),
    CONSTRAINT hospital_price_version_projection_key
        UNIQUE (content_sha256, parser_contract_sha256),
    CONSTRAINT hospital_price_version_shape_check CHECK (
        version_id ~ '^[0-9a-f]{{64}}$'
        AND parser_contract_sha256 ~ '^[0-9a-f]{{64}}$'
        AND semantic_sha256 ~ '^[0-9a-f]{{64}}$'
        AND source_format IN ('json', 'csv-tall', 'csv-wide')
        AND location_count > 0 AND npi_count > 0 AND license_count > 0
        AND service_count > 0 AND charge_count > 0
        AND payer_charge_count >= 0
    )
);
CREATE INDEX hospital_price_version_content_idx
    ON {schema}.hospital_price_version(content_sha256);
CREATE TABLE {schema}.hospital_price_version_location (
    version_id varchar(64) NOT NULL, location_ordinal integer NOT NULL,
    location_name text, hospital_address text,
    PRIMARY KEY (version_id, location_ordinal),
    CONSTRAINT hospital_price_version_location_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id) ON DELETE CASCADE,
    CONSTRAINT hospital_price_version_location_shape_check CHECK (
        location_ordinal >= 0 AND (
            (location_name IS NOT NULL AND btrim(location_name) <> '') OR
            (hospital_address IS NOT NULL AND btrim(hospital_address) <> '')
        )
    )
);
CREATE TABLE {schema}.hospital_price_version_npi (
    version_id varchar(64) NOT NULL, npi_ordinal integer NOT NULL,
    npi text NOT NULL,
    PRIMARY KEY (version_id, npi_ordinal),
    CONSTRAINT hospital_price_version_npi_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id) ON DELETE CASCADE,
    CONSTRAINT hospital_price_version_npi_shape_check CHECK (
        npi_ordinal >= 0 AND npi <> ''
    )
);
CREATE TABLE {schema}.hospital_price_version_license (
    version_id varchar(64) NOT NULL, license_ordinal integer NOT NULL,
    state varchar(2) NOT NULL, license_number text,
    PRIMARY KEY (version_id, license_ordinal),
    CONSTRAINT hospital_price_version_license_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id) ON DELETE CASCADE,
    CONSTRAINT hospital_price_version_license_shape_check CHECK (
        license_ordinal >= 0 AND state <> ''
    )
);
CREATE TABLE {schema}.hospital_price_version_hospital (
    version_id varchar(64) NOT NULL, hospital_id varchar(64) NOT NULL,
    source_location_ordinal integer,
    PRIMARY KEY (version_id, hospital_id),
    CONSTRAINT hospital_price_version_hospital_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id)
        ON DELETE CASCADE,
    CONSTRAINT hospital_price_version_hospital_hospital_fkey
        FOREIGN KEY (hospital_id)
        REFERENCES {schema}.hospital_price_hospital(hospital_id),
    CONSTRAINT hospital_price_version_hospital_location_fkey
        FOREIGN KEY (version_id, source_location_ordinal)
        REFERENCES {schema}.hospital_price_version_location(
            version_id, location_ordinal
        )
);
CREATE INDEX hospital_price_version_hospital_lookup_idx
    ON {schema}.hospital_price_version_hospital(hospital_id, version_id);
CREATE TABLE {schema}.hospital_price_contract_provision (
    version_id varchar(64) NOT NULL, provision_ordinal integer NOT NULL,
    payer_name text, plan_name text, provisions text NOT NULL,
    PRIMARY KEY (version_id, provision_ordinal),
    CONSTRAINT hospital_price_contract_provision_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id) ON DELETE CASCADE,
    CONSTRAINT hospital_price_contract_provision_shape_check CHECK (
        provision_ordinal >= 0 AND btrim(provisions) <> ''
        AND (payer_name IS NULL OR btrim(payer_name) <> '')
        AND (plan_name IS NULL OR btrim(plan_name) <> '')
    )
);
CREATE TABLE {schema}.hospital_price_import_attempt (
    attempt_id varchar(64) PRIMARY KEY, hospital_id varchar(64) NOT NULL,
    locator_id varchar(64) NOT NULL, locator_observation_id varchar(64) NOT NULL,
    registry_version integer NOT NULL, requested_source_url text NOT NULL,
    final_source_url text, source_http_status integer,
    expected_generation bigint NOT NULL, status varchar(16) NOT NULL,
    content_sha256 varchar(64), version_id varchar(64),
    started_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    lease_owner varchar(128) NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    finished_at timestamptz,
    error_code varchar(64),
    error_detail text,
    CONSTRAINT hospital_price_import_attempt_hospital_fkey
        FOREIGN KEY (hospital_id)
        REFERENCES {schema}.hospital_price_hospital(hospital_id),
    CONSTRAINT hospital_price_import_attempt_observation_owner_fkey
        FOREIGN KEY (locator_id, locator_observation_id)
        REFERENCES {schema}.hospital_price_locator_observation(
            locator_id, observation_id
        ),
    CONSTRAINT hospital_price_import_attempt_content_fkey
        FOREIGN KEY (content_sha256)
        REFERENCES {schema}.hospital_price_content(content_sha256),
    CONSTRAINT hospital_price_import_attempt_version_hospital_fkey
        FOREIGN KEY (version_id, hospital_id)
        REFERENCES {schema}.hospital_price_version_hospital(
            version_id, hospital_id
        ),
    CONSTRAINT hospital_price_import_attempt_owner_key
        UNIQUE (hospital_id, attempt_id),
    CONSTRAINT hospital_price_import_attempt_version_owner_key
        UNIQUE (hospital_id, version_id, attempt_id),
    CONSTRAINT hospital_price_import_attempt_state_check CHECK (
        registry_version > 0 AND expected_generation >= 0
        AND (source_http_status IS NULL OR source_http_status BETWEEN 100 AND 599)
        AND status IN (
            'queued', 'running', 'verified', 'published', 'unchanged', 'failed', 'superseded'
        )
        AND lease_owner = btrim(lease_owner) AND lease_owner <> ''
        AND started_at <= heartbeat_at AND heartbeat_at < lease_expires_at
        AND ((status IN ('queued', 'running', 'verified') AND finished_at IS NULL)
             OR (status IN ('published', 'unchanged', 'failed', 'superseded')
                 AND finished_at IS NOT NULL))
    )
);
CREATE INDEX hospital_price_import_attempt_hospital_started_idx
    ON {schema}.hospital_price_import_attempt(hospital_id, started_at DESC);
CREATE TABLE {schema}.hospital_price_current (
    hospital_id varchar(64) PRIMARY KEY, version_id varchar(64),
    generation bigint NOT NULL DEFAULT 0, published_attempt_id varchar(64),
    latest_attempt_id varchar(64), service_count bigint NOT NULL DEFAULT 0,
    charge_count bigint NOT NULL DEFAULT 0, payer_charge_count bigint NOT NULL DEFAULT 0,
    npi_count integer NOT NULL DEFAULT 0, tax_identity_count integer NOT NULL DEFAULT 0,
    last_success_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CONSTRAINT hospital_price_current_hospital_fkey
        FOREIGN KEY (hospital_id)
        REFERENCES {schema}.hospital_price_hospital(hospital_id),
    CONSTRAINT hospital_price_current_version_hospital_fkey
        FOREIGN KEY (version_id, hospital_id)
        REFERENCES {schema}.hospital_price_version_hospital(
            version_id, hospital_id
        ),
    CONSTRAINT hospital_price_current_published_attempt_fkey
        FOREIGN KEY (hospital_id, version_id, published_attempt_id)
        REFERENCES {schema}.hospital_price_import_attempt(
            hospital_id, version_id, attempt_id
        ),
    CONSTRAINT hospital_price_current_latest_attempt_fkey
        FOREIGN KEY (hospital_id, latest_attempt_id)
        REFERENCES {schema}.hospital_price_import_attempt(
            hospital_id, attempt_id
        ),
    CONSTRAINT hospital_price_current_state_check CHECK (
        generation >= 0 AND service_count >= 0 AND charge_count >= 0
        AND payer_charge_count >= 0 AND npi_count >= 0
        AND tax_identity_count >= 0
        AND ((version_id IS NULL AND generation = 0
              AND published_attempt_id IS NULL AND last_success_at IS NULL)
             OR (version_id IS NOT NULL AND generation > 0
                 AND published_attempt_id IS NOT NULL
                 AND last_success_at IS NOT NULL))
    )
);
CREATE TABLE {schema}.hospital_price_hospital_npi (
    hospital_id varchar(64) NOT NULL,
    version_id varchar(64) NOT NULL,
    source_ordinal integer NOT NULL,
    npi varchar(10) NOT NULL,
    source_kind varchar(32) NOT NULL,
    PRIMARY KEY (hospital_id, version_id, source_ordinal),
    CONSTRAINT hospital_price_hospital_npi_version_hospital_fkey
        FOREIGN KEY (version_id, hospital_id)
        REFERENCES {schema}.hospital_price_version_hospital(
            version_id, hospital_id
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_hospital_npi_shape_check CHECK (
        npi ~ '^[0-9]{{10}}$' AND source_ordinal >= 0
        AND source_kind = 'mrf_header_file'
    )
);
CREATE INDEX hospital_price_hospital_npi_lookup_idx
    ON {schema}.hospital_price_hospital_npi(npi, hospital_id);
CREATE TABLE {schema}.hospital_price_hospital_tax_identity (
    hospital_id varchar(64) NOT NULL,
    version_id varchar(64) NOT NULL,
    attempt_id varchar(64) NOT NULL,
    tin_type varchar(16) NOT NULL,
    tin_value varchar(64) NOT NULL,
    source_kind varchar(32) NOT NULL,
    source_ordinal integer NOT NULL,
    PRIMARY KEY (
        hospital_id, version_id, tin_type, tin_value,
        source_kind, source_ordinal
    ),
    CONSTRAINT hospital_price_hospital_tax_version_hospital_fkey
        FOREIGN KEY (version_id, hospital_id)
        REFERENCES {schema}.hospital_price_version_hospital(
            version_id, hospital_id
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_hospital_tax_attempt_fkey
        FOREIGN KEY (hospital_id, version_id, attempt_id)
        REFERENCES {schema}.hospital_price_import_attempt(
            hospital_id, version_id, attempt_id
        ),
    CONSTRAINT hospital_price_hospital_tax_shape_check CHECK (
        tin_type = btrim(tin_type) AND tin_type <> ''
        AND tin_value = btrim(tin_value) AND tin_value <> ''
        AND source_kind = btrim(source_kind) AND source_kind <> ''
        AND source_ordinal >= 0
    )
);
CREATE INDEX hospital_price_hospital_tax_lookup_idx
    ON {schema}.hospital_price_hospital_tax_identity(
        tin_type, tin_value, hospital_id
    );
"""


_FACT_DDL = """
CREATE TABLE {schema}.hospital_price_service (
    version_id varchar(64) NOT NULL, service_ordinal integer NOT NULL,
    description text NOT NULL, drug_unit numeric, drug_type varchar(2),
    PRIMARY KEY (version_id, service_ordinal),
    CONSTRAINT hospital_price_service_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id)
        ON DELETE CASCADE,
    CONSTRAINT hospital_price_service_shape_check CHECK (
        service_ordinal >= 0 AND description <> ''
        AND ((drug_unit IS NULL AND drug_type IS NULL)
             OR (drug_unit > 0 AND drug_type IN (
                 'GR', 'ML', 'ME', 'UN', 'F2', 'GM', 'EA'
             )))
    )
);
CREATE TABLE {schema}.hospital_price_service_code (
    version_id varchar(64) NOT NULL, service_ordinal integer NOT NULL,
    code_ordinal integer NOT NULL, code_type varchar(16) NOT NULL,
    code text NOT NULL,
    PRIMARY KEY (version_id, service_ordinal, code_ordinal),
    CONSTRAINT hospital_price_service_code_service_fkey
        FOREIGN KEY (version_id, service_ordinal)
        REFERENCES {schema}.hospital_price_service(
            version_id, service_ordinal
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_service_code_shape_check CHECK (
        code_ordinal >= 0 AND code <> '' AND code_type IN (
            'CPT', 'HCPCS', 'ICD', 'DRG', 'MS-DRG', 'R-DRG', 'S-DRG',
            'APS-DRG', 'AP-DRG', 'APR-DRG', 'TRIS-DRG', 'APC', 'NDC',
            'HIPPS', 'LOCAL', 'EAPG', 'CDT', 'RC', 'CDM', 'CMG',
            'MS-LTC-DRG'
        )
    )
);
CREATE INDEX hospital_price_service_code_lookup_idx
    ON {schema}.hospital_price_service_code(
        code_type, code, version_id, service_ordinal
    );
CREATE TABLE {schema}.hospital_price_charge (
    version_id varchar(64) NOT NULL, service_ordinal integer NOT NULL,
    charge_ordinal integer NOT NULL, setting varchar(16) NOT NULL,
    minimum numeric, maximum numeric, gross_charge numeric, discounted_cash numeric,
    modifier_codes text[],
    additional_generic_notes text,
    billing_class varchar(16),
    PRIMARY KEY (version_id, service_ordinal, charge_ordinal),
    CONSTRAINT hospital_price_charge_service_fkey
        FOREIGN KEY (version_id, service_ordinal)
        REFERENCES {schema}.hospital_price_service(
            version_id, service_ordinal
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_charge_shape_check CHECK (
        charge_ordinal >= 0
        AND setting IN ('inpatient', 'outpatient', 'both')
        AND (minimum IS NULL OR minimum > 0)
        AND (maximum IS NULL OR maximum > 0)
        AND (gross_charge IS NULL OR gross_charge > 0)
        AND (discounted_cash IS NULL OR discounted_cash > 0)
        AND (billing_class IS NULL OR billing_class IN (
            'professional', 'facility', 'both'
        ))
    )
);
CREATE TABLE {schema}.hospital_price_payer_charge (
    version_id varchar(64) NOT NULL,
    service_ordinal integer NOT NULL,
    charge_ordinal integer NOT NULL,
    payer_ordinal integer NOT NULL,
    payer_name text NOT NULL,
    plan_name text NOT NULL,
    methodology varchar(64) NOT NULL,
    standard_charge_dollar numeric,
    standard_charge_percentage numeric,
    standard_charge_algorithm text,
    median_amount numeric,
    percentile_10 numeric,
    percentile_90 numeric,
    allowed_count varchar(32),
    additional_payer_notes text,
    PRIMARY KEY (
        version_id, service_ordinal, charge_ordinal, payer_ordinal
    ),
    CONSTRAINT hospital_price_payer_charge_charge_fkey
        FOREIGN KEY (version_id, service_ordinal, charge_ordinal)
        REFERENCES {schema}.hospital_price_charge(
            version_id, service_ordinal, charge_ordinal
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_payer_charge_shape_check CHECK (
        payer_ordinal >= 0 AND payer_name <> '' AND plan_name <> ''
        AND methodology IN (
            'case rate', 'fee schedule', 'percent of total billed charges',
            'per diem', 'other'
        )
        AND (standard_charge_dollar IS NULL OR standard_charge_dollar > 0)
        AND (standard_charge_percentage IS NULL
             OR standard_charge_percentage > 0)
        AND (median_amount IS NULL OR median_amount > 0)
        AND (percentile_10 IS NULL OR percentile_10 > 0)
        AND (percentile_90 IS NULL OR percentile_90 > 0)
        AND (standard_charge_dollar IS NOT NULL
             OR standard_charge_percentage IS NOT NULL
             OR standard_charge_algorithm IS NOT NULL)
        AND (allowed_count IS NULL OR allowed_count ~
             '^(0|1 through 10|1[1-9]|[2-9][0-9]+|[1-9][0-9]{{2,}})$')
        AND ((standard_charge_percentage IS NULL
              AND standard_charge_algorithm IS NULL)
             OR allowed_count IS NOT NULL)
    )
);
CREATE INDEX hospital_price_payer_charge_lookup_idx
    ON {schema}.hospital_price_payer_charge(
        payer_name, plan_name, version_id, service_ordinal, charge_ordinal
    );
CREATE TABLE {schema}.hospital_price_modifier (
    version_id varchar(64) NOT NULL,
    modifier_ordinal integer NOT NULL,
    code text NOT NULL,
    description text NOT NULL,
    setting varchar(16),
    additional_generic_notes text,
    PRIMARY KEY (version_id, modifier_ordinal),
    CONSTRAINT hospital_price_modifier_version_fkey
        FOREIGN KEY (version_id)
        REFERENCES {schema}.hospital_price_version(version_id)
        ON DELETE CASCADE,
    CONSTRAINT hospital_price_modifier_shape_check CHECK (
        modifier_ordinal >= 0 AND code <> '' AND description <> ''
        AND (setting IS NULL
             OR setting IN ('inpatient', 'outpatient', 'both'))
        AND (additional_generic_notes IS NULL
             OR btrim(additional_generic_notes) <> '')
    )
);
CREATE TABLE {schema}.hospital_price_modifier_payer (
    version_id varchar(64) NOT NULL,
    modifier_ordinal integer NOT NULL,
    payer_ordinal integer NOT NULL,
    payer_name text NOT NULL,
    plan_name text NOT NULL,
    description text,
    standard_charge_dollar numeric,
    standard_charge_percentage numeric,
    standard_charge_algorithm text,
    PRIMARY KEY (version_id, modifier_ordinal, payer_ordinal),
    CONSTRAINT hospital_price_modifier_payer_modifier_fkey
        FOREIGN KEY (version_id, modifier_ordinal)
        REFERENCES {schema}.hospital_price_modifier(
            version_id, modifier_ordinal
        ) ON DELETE CASCADE,
    CONSTRAINT hospital_price_modifier_payer_shape_check CHECK (
        payer_ordinal >= 0 AND payer_name <> '' AND plan_name <> ''
        AND (description IS NULL OR btrim(description) <> '')
        AND (standard_charge_dollar IS NULL OR standard_charge_dollar > 0)
        AND (standard_charge_percentage IS NULL OR standard_charge_percentage > 0)
        AND (standard_charge_algorithm IS NULL
             OR btrim(standard_charge_algorithm) <> '')
        AND (description IS NOT NULL OR standard_charge_dollar IS NOT NULL
             OR standard_charge_percentage IS NOT NULL
             OR standard_charge_algorithm IS NOT NULL)
    )
);
"""


_DROP_ORDER = (
    "hospital_price_modifier_payer",
    "hospital_price_modifier",
    "hospital_price_payer_charge",
    "hospital_price_charge",
    "hospital_price_service_code",
    "hospital_price_service",
    "hospital_price_hospital_tax_identity",
    "hospital_price_hospital_npi",
    "hospital_price_current",
    "hospital_price_import_attempt",
    "hospital_price_contract_provision",
    "hospital_price_version_hospital",
    "hospital_price_version_license",
    "hospital_price_version_npi",
    "hospital_price_version_location",
    "hospital_price_version",
    "hospital_price_content",
    "hospital_price_hospital",
    "hospital_price_locator_observation",
    "hospital_price_locator",
)


def upgrade() -> None:
    """Create dormant normalized storage; no import or publication is started."""
    schema = _quote(_schema())
    _execute(_CONTROL_DDL.format(schema=schema))
    _execute(_FACT_DDL.format(schema=schema))


def downgrade() -> None:
    """Drop only hospital-price relations in dependency-safe order."""
    schema = _quote(_schema())
    for table in _DROP_ORDER:
        op.execute(f"DROP TABLE {schema}.{_quote(table)}")
