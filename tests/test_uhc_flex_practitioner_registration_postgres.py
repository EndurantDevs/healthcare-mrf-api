# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for exact Flex source registration."""

from __future__ import annotations

import json
import uuid

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
import pytest

from db.connection import Database
from process import uhc_flex_practitioner_registration as registration
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import quoted


GENERIC_FLEX_SOURCE_ID = "pdfhir_0b5cfd565c53364a73981dcb"
OFFICIAL_FILE_SOURCE_ID = "pdfhir_2754e999dd691175821ec26e"
GENERIC_ENDPOINT_ID = "1" * 64
OFFICIAL_ENDPOINT_ID = "2" * 64


async def _create_registry_tables(database: Database, schema_name: str) -> None:
    schema = quoted(schema_name)
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY,
            canonical_api_base text NOT NULL,
            credential_descriptor_hash varchar(64) NOT NULL,
            endpoint_signature_hash varchar(64) NOT NULL,
            credential_descriptor_json jsonb,
            endpoint_signature_json jsonb,
            first_seen_at timestamp,
            last_seen_at timestamp,
            metadata_json jsonb,
            created_at timestamp,
            updated_at timestamp,
            UNIQUE (
                canonical_api_base,
                credential_descriptor_hash,
                endpoint_signature_hash
            )
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            org_tin varchar(64),
            org_name varchar(256) NOT NULL,
            plan_name varchar(512),
            portal_url text,
            api_base text,
            canonical_api_base text,
            endpoint_id varchar(64) REFERENCES
                {schema}.provider_directory_api_endpoint(endpoint_id),
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
        );
        """
    )


async def _seed_protected_sources(database: Database, schema_name: str) -> None:
    schema = quoted(schema_name)
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_api_endpoint (
            endpoint_id, canonical_api_base, credential_descriptor_hash,
            endpoint_signature_hash, credential_descriptor_json,
            endpoint_signature_json, metadata_json
        ) VALUES
            (:generic_endpoint_id, :flex_base, :generic_credential_hash,
             :generic_signature_hash, '{{}}'::jsonb,
             '{{"probe":"generic"}}'::jsonb,
             '{{"classification":"probe_only"}}'::jsonb),
            (:official_endpoint_id, :official_base, :official_credential_hash,
             :official_signature_hash, '{{}}'::jsonb,
             '{{"transport":"official_provider_files"}}'::jsonb,
             '{{"classification":"official_files"}}'::jsonb);
        """,
        generic_endpoint_id=GENERIC_ENDPOINT_ID,
        official_endpoint_id=OFFICIAL_ENDPOINT_ID,
        flex_base="https://flex.optum.com/fhirpublic/R4",
        official_base="https://providermrf.example.test",
        generic_credential_hash="3" * 64,
        generic_signature_hash="4" * 64,
        official_credential_hash="5" * 64,
        official_signature_hash="6" * 64,
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, org_name, canonical_api_base, endpoint_id,
            auth_type, metadata_json
        ) VALUES
            (:generic_source_id, 'Synthetic generic probe', :flex_base,
             :generic_endpoint_id, 'none',
             '{{"classification":"probe_only"}}'::jsonb),
            (:official_source_id, 'Synthetic official files', :official_base,
             :official_endpoint_id, 'none',
             '{{"classification":"official_files"}}'::jsonb);
        """,
        flex_base="https://flex.optum.com/fhirpublic/R4",
        official_base="https://providermrf.example.test",
        generic_endpoint_id=GENERIC_ENDPOINT_ID,
        official_endpoint_id=OFFICIAL_ENDPOINT_ID,
        generic_source_id=GENERIC_FLEX_SOURCE_ID,
        official_source_id=OFFICIAL_FILE_SOURCE_ID,
    )


async def _protected_fingerprint(
    database: Database,
    schema_name: str,
) -> dict[str, object]:
    schema = quoted(schema_name)
    fingerprint = await database.first(
        f"""
        SELECT (
                   SELECT jsonb_agg(to_jsonb(endpoint_record)
                                    ORDER BY endpoint_id)
                     FROM {schema}.provider_directory_api_endpoint
                          AS endpoint_record
                    WHERE endpoint_id IN (
                        :generic_endpoint_id,
                        :official_endpoint_id
                    )
               ) AS endpoints,
               (
                   SELECT jsonb_agg(to_jsonb(source_record)
                                    ORDER BY source_id)
                     FROM {schema}.provider_directory_source AS source_record
                    WHERE source_id IN (
                        :generic_source_id,
                        :official_source_id
                    )
               ) AS sources;
        """,
        generic_endpoint_id=GENERIC_ENDPOINT_ID,
        official_endpoint_id=OFFICIAL_ENDPOINT_ID,
        generic_source_id=GENERIC_FLEX_SOURCE_ID,
        official_source_id=OFFICIAL_FILE_SOURCE_ID,
    )
    assert fingerprint is not None
    return dict(fingerprint._mapping)


async def _dedicated_fingerprint(
    database: Database,
    schema_name: str,
    endpoint_id: str,
) -> dict[str, object]:
    schema = quoted(schema_name)
    row = await database.first(
        f"""
        SELECT (
                   SELECT to_jsonb(endpoint_record)
                     FROM {schema}.provider_directory_api_endpoint
                          AS endpoint_record
                    WHERE endpoint_id = :endpoint_id
               ) AS endpoint_record,
               (
                   SELECT to_jsonb(source_record)
                     FROM {schema}.provider_directory_source AS source_record
                    WHERE source_id = :source_id
               ) AS source_record;
        """,
        endpoint_id=endpoint_id,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
    )
    assert row is not None
    return dict(row._mapping)


@pytest.mark.asyncio
async def test_flex_registration_is_atomic_distinct_and_drift_rejecting(
    monkeypatch,
) -> None:
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = Database(
        engine=engine,
        session_factory=async_sessionmaker(
            engine,
            expire_on_commit=False,
            autoflush=False,
        ),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        await database.status(f"CREATE SCHEMA {quoted(schema_name)};")
        await _create_registry_tables(database, schema_name)
        await _seed_protected_sources(database, schema_name)
        protected_before = await _protected_fingerprint(database, schema_name)
        schema = quoted(schema_name)

        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source (
                source_id, org_name, canonical_api_base, endpoint_id,
                auth_type, metadata_json
            ) VALUES (
                :source_id, 'Synthetic collision',
                'https://collision.example.test', :endpoint_id,
                'none', CAST(:collision_metadata AS jsonb)
            );
            """,
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
            endpoint_id=GENERIC_ENDPOINT_ID,
            collision_metadata=json.dumps({"drift": True}),
        )
        expected_endpoint_id = (
            registration.uhc_flex_practitioner_endpoint_identity().endpoint_id
        )
        with pytest.raises(registration.UHCFlexPractitionerRegistrationError):
            await registration.register_uhc_flex_practitioner_source(
                database=database
            )
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_api_endpoint "
            "WHERE endpoint_id = :endpoint_id;",
            endpoint_id=expected_endpoint_id,
        ) == 0
        await database.status(
            f"DELETE FROM {schema}.provider_directory_source "
            "WHERE source_id = :source_id;",
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        )

        created = await registration.register_uhc_flex_practitioner_source(
            database=database
        )
        dedicated_before = await _dedicated_fingerprint(
            database,
            schema_name,
            created.endpoint_id,
        )
        replayed = await registration.register_uhc_flex_practitioner_source(
            database=database
        )
        dedicated_after = await _dedicated_fingerprint(
            database,
            schema_name,
            created.endpoint_id,
        )

        assert created.endpoint_created is True
        assert created.source_created is True
        assert replayed.created is False
        assert dedicated_after == dedicated_before
        assert created.endpoint_id not in {
            GENERIC_ENDPOINT_ID,
            OFFICIAL_ENDPOINT_ID,
        }
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_api_endpoint;"
        ) == 3
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}.provider_directory_source;"
        ) == 3
        assert await _protected_fingerprint(database, schema_name) == (
            protected_before
        )

        await database.status(
            f"""
            UPDATE {schema}.provider_directory_source
               SET metadata_json = jsonb_set(
                       metadata_json,
                       '{{provider_directory_profile_eligible}}',
                       'true'::jsonb
                   )
             WHERE source_id = :source_id;
            """,
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        )
        drifted_source = await database.scalar(
            f"SELECT metadata_json FROM {schema}.provider_directory_source "
            "WHERE source_id = :source_id;",
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        )
        with pytest.raises(registration.UHCFlexPractitionerRegistrationError):
            await registration.register_uhc_flex_practitioner_source(
                database=database
            )
        assert await database.scalar(
            f"SELECT metadata_json FROM {schema}.provider_directory_source "
            "WHERE source_id = :source_id;",
            source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        ) == drifted_source
        assert drifted_source["provider_directory_profile_eligible"] is True
        assert await _protected_fingerprint(database, schema_name) == (
            protected_before
        )
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()
