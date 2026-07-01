"""Add Provider Directory FHIR import tables."""

from __future__ import annotations

import os

from alembic import op


revision = "20260628100000_provider_directory_fhir_tables"
down_revision = "20260625123000_reapply_address_canonical_unit_parser"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _create_tables(bind, schema: str) -> None:
    statements = (
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_source")} (
            source_id varchar(64) PRIMARY KEY,
            org_tin varchar(64),
            org_name varchar(256) NOT NULL,
            plan_name varchar(512),
            portal_url text,
            api_base text,
            canonical_api_base text,
            endpoint_insurance_plan text,
            endpoint_practitioner text,
            endpoint_practitioner_role text,
            endpoint_organization text,
            endpoint_organization_affiliation text,
            endpoint_location text,
            endpoint_healthcare_service text,
            endpoint_network text,
            endpoint_endpoint text,
            requires_registration boolean NOT NULL DEFAULT false,
            requires_api_key boolean NOT NULL DEFAULT false,
            auth_type varchar(64),
            last_validated varchar(64),
            last_validated_status varchar(64),
            fhir_version varchar(32),
            compliance_flag varchar(64),
            violation_type varchar(128),
            violation_detail text,
            data_quality_flag varchar(64),
            data_quality_sample_npi varchar(32),
            data_quality_practitioner_count varchar(64),
            data_quality_checked text,
            is_medicare_advantage boolean,
            is_medicaid_mco boolean,
            is_chip boolean,
            is_qhp boolean,
            seed_source varchar(128),
            seed_source_detail text,
            seed_source_url text,
            seed_source_date varchar(64),
            seed_row_id varchar(64),
            id_provider_alt varchar(128),
            team_status varchar(128),
            last_probe_status varchar(64),
            last_probe_status_code integer,
            last_probe_error text,
            last_probe_run_id varchar(64),
            last_probed_at timestamp,
            metadata_json jsonb,
            created_at timestamp,
            updated_at timestamp
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_capability")} (
            source_id varchar(64) PRIMARY KEY,
            api_base text,
            metadata_url text,
            probe_status varchar(64) NOT NULL,
            http_status integer,
            response_time_ms integer,
            resource_type varchar(64),
            fhir_version varchar(32),
            software_name varchar(256),
            software_version varchar(128),
            implementation_url text,
            formats jsonb,
            supported_resources jsonb,
            search_params jsonb,
            auth_required boolean NOT NULL DEFAULT false,
            error text,
            capability_hash varchar(64),
            probed_at timestamp,
            run_id varchar(64),
            metadata_json jsonb
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_insurance_plan")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            plan_identifier varchar(256),
            status varchar(64),
            name varchar(512),
            aliases jsonb,
            type_codes jsonb,
            owned_by_ref text,
            administered_by_ref text,
            network_refs jsonb,
            coverage_area_refs jsonb,
            plan_json jsonb,
            period_start varchar(64),
            period_end varchar(64),
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_practitioner")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            npi bigint,
            active boolean,
            family_name varchar(256),
            given_names jsonb,
            full_name varchar(512),
            telecom jsonb,
            qualification_codes jsonb,
            communication_codes jsonb,
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_organization")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            npi bigint,
            tax_id varchar(64),
            active boolean,
            name varchar(512),
            aliases jsonb,
            type_codes jsonb,
            telecom jsonb,
            address_json jsonb,
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_location")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            status varchar(64),
            name varchar(512),
            mode varchar(64),
            type_codes jsonb,
            first_line varchar,
            second_line varchar,
            city_name varchar,
            state_name varchar,
            state_code varchar(2),
            postal_code varchar,
            zip5 varchar(5),
            city_norm varchar,
            country_code varchar,
            telephone_number varchar,
            fax_number varchar,
            telecom jsonb,
            latitude varchar(64),
            longitude varchar(64),
            address_key varchar(64),
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_practitioner_role")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            active boolean,
            practitioner_ref text,
            organization_ref text,
            location_refs jsonb,
            healthcare_service_refs jsonb,
            network_refs jsonb,
            insurance_plan_refs jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            telecom jsonb,
            accepting_patients jsonb,
            period_start varchar(64),
            period_end varchar(64),
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_healthcare_service")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            active boolean,
            name varchar(512),
            type_codes jsonb,
            category_codes jsonb,
            specialty_codes jsonb,
            location_refs jsonb,
            telecom jsonb,
            coverage_area_refs jsonb,
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {_qt(schema, "provider_directory_organization_affiliation")} (
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            resource_url text,
            active boolean,
            organization_ref text,
            participating_organization_ref text,
            network_refs jsonb,
            location_refs jsonb,
            healthcare_service_refs jsonb,
            specialty_codes jsonb,
            code_codes jsonb,
            period_start varchar(64),
            period_end varchar(64),
            last_seen_run_id varchar(64),
            observed_at timestamp,
            updated_at timestamp,
            PRIMARY KEY (source_id, resource_id)
        )
        """,
    )
    for statement in statements:
        bind.exec_driver_sql(statement)


def _create_indexes(bind, schema: str) -> None:
    statements = (
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_api_base_idx ON {_qt(schema, 'provider_directory_source')} (canonical_api_base)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_org_name_idx ON {_qt(schema, 'provider_directory_source')} (org_name)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_auth_type_idx ON {_qt(schema, 'provider_directory_source')} (auth_type)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_validation_idx ON {_qt(schema, 'provider_directory_source')} (last_validated_status)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_source_data_quality_idx ON {_qt(schema, 'provider_directory_source')} (data_quality_flag)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_capability_status_idx ON {_qt(schema, 'provider_directory_capability')} (probe_status)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_capability_fhir_version_idx ON {_qt(schema, 'provider_directory_capability')} (fhir_version)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_insurance_plan_identifier_idx ON {_qt(schema, 'provider_directory_insurance_plan')} (plan_identifier)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_insurance_plan_name_idx ON {_qt(schema, 'provider_directory_insurance_plan')} (name)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_practitioner_npi_idx ON {_qt(schema, 'provider_directory_practitioner')} (npi)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_practitioner_family_idx ON {_qt(schema, 'provider_directory_practitioner')} (family_name)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_organization_npi_idx ON {_qt(schema, 'provider_directory_organization')} (npi)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_organization_tax_id_idx ON {_qt(schema, 'provider_directory_organization')} (tax_id)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_organization_name_idx ON {_qt(schema, 'provider_directory_organization')} (name)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_location_address_key_idx ON {_qt(schema, 'provider_directory_location')} (address_key)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_location_zip5_idx ON {_qt(schema, 'provider_directory_location')} (zip5)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_location_state_city_idx ON {_qt(schema, 'provider_directory_location')} (state_code, city_norm)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_role_practitioner_idx ON {_qt(schema, 'provider_directory_practitioner_role')} (practitioner_ref)",
        f"CREATE INDEX IF NOT EXISTS provider_directory_role_organization_idx ON {_qt(schema, 'provider_directory_practitioner_role')} (organization_ref)",
    )
    for statement in statements:
        bind.exec_driver_sql(statement + ";")


def upgrade():
    bind = op.get_bind()
    schema = _schema()
    bind.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    _create_tables(bind, schema)
    _create_indexes(bind, schema)


def downgrade():
    return None
