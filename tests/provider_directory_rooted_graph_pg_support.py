# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL fixtures for rooted provider-directory graph tests."""

from __future__ import annotations

import json
import os

from db.connection import Database
from process.provider_directory_dataset_scoped_publication import (
    exact_uhc_dataset_pair,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
)
from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS,
)
from process.provider_directory_rooted_graph_identity import (
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
)
from process.provider_directory_rooted_graph_store_contract import (
    build_provider_directory_rooted_graph_acquisition_identity,
)
from tests.formulary_fhir_twin_admission_pg_support import quoted


ENDPOINT_ID = "a" * 64
ENDPOINT_SIGNATURE = "b" * 64
DATASET_HASH = "c" * 64
ROOT_PROOF = "d" * 64
ROOT_DATASET_ID = "synthetic-practitioner-dataset-1"
ROOT_COHORT_ID = "synthetic-cohort-v1"
ROOT_RESOURCE_ID = "synthetic-1003821380"
API_BASE = PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE
WORK_TABLE = "provider_directory_rooted_graph_work"
ACQUISITION_TABLE = "provider_directory_rooted_graph_acquisition"
RESOURCE_TABLE = "provider_directory_rooted_graph_resource"
EDGE_TABLE = "provider_directory_rooted_graph_edge"
WORK_TRIGGER = "provider_directory_rooted_graph_work_row_guard"


async def extend_publication_foundation(connection, schema_name: str) -> None:
    """Add the production registry and serving-relation columns to the UHC fixture."""

    schema = quoted(schema_name)
    await _add_endpoint_columns(connection, schema)
    await _add_source_columns(connection, schema)
    await _bind_registry_fields(connection, schema)
    await _create_serving_relation_tables(connection, schema)


async def _add_endpoint_columns(connection, schema: str) -> None:
    await connection.execute(
        f"""
        ALTER TABLE {schema}.provider_directory_api_endpoint
            ADD COLUMN credential_descriptor_hash varchar(64),
            ADD COLUMN IF NOT EXISTS endpoint_signature_hash varchar(64),
            ADD COLUMN credential_descriptor_json jsonb,
            ADD COLUMN endpoint_signature_json jsonb,
            ADD COLUMN first_seen_at timestamp,
            ADD COLUMN last_seen_at timestamp,
            ADD COLUMN created_at timestamp,
            ADD COLUMN updated_at timestamp;
        """
    )


async def _add_source_columns(connection, schema: str) -> None:
    await connection.execute(
        f"""
        ALTER TABLE {schema}.provider_directory_source
            ADD COLUMN org_tin varchar(64),
            ADD COLUMN org_name varchar(256),
            ADD COLUMN plan_name varchar(512),
            ADD COLUMN portal_url text,
            ADD COLUMN api_base text,
            ADD COLUMN endpoint_insurance_plan text,
            ADD COLUMN endpoint_practitioner text,
            ADD COLUMN endpoint_practitioner_role text,
            ADD COLUMN endpoint_organization text,
            ADD COLUMN endpoint_organization_affiliation text,
            ADD COLUMN endpoint_location text,
            ADD COLUMN endpoint_healthcare_service text,
            ADD COLUMN endpoint_network text,
            ADD COLUMN endpoint_endpoint text,
            ADD COLUMN last_validated varchar(64),
            ADD COLUMN last_validated_status varchar(64),
            ADD COLUMN fhir_version varchar(32),
            ADD COLUMN compliance_flag varchar(64),
            ADD COLUMN violation_type varchar(128),
            ADD COLUMN violation_detail text,
            ADD COLUMN data_quality_flag varchar(64),
            ADD COLUMN data_quality_sample_npi varchar(32),
            ADD COLUMN data_quality_practitioner_count varchar(64),
            ADD COLUMN data_quality_checked text,
            ADD COLUMN is_medicare_advantage boolean,
            ADD COLUMN is_medicaid_mco boolean,
            ADD COLUMN is_chip boolean,
            ADD COLUMN is_qhp boolean,
            ADD COLUMN seed_source varchar(128),
            ADD COLUMN seed_source_detail text,
            ADD COLUMN seed_source_url text,
            ADD COLUMN seed_source_date varchar(64),
            ADD COLUMN seed_row_id varchar(64),
            ADD COLUMN id_provider_alt varchar(128),
            ADD COLUMN team_status varchar(128),
            ADD COLUMN last_probe_status varchar(64),
            ADD COLUMN last_probe_status_code integer,
            ADD COLUMN last_probe_error text,
            ADD COLUMN last_probe_run_id varchar(64),
            ADD COLUMN last_probed_at timestamp,
            ADD COLUMN created_at timestamp,
            ADD COLUMN updated_at timestamp;
        """
    )


async def _bind_registry_fields(connection, schema: str) -> None:
    pair = exact_uhc_dataset_pair()
    legacy_endpoint = uhc_flex_practitioner_endpoint_identity()
    await connection.execute(
        f"""
        UPDATE {schema}.provider_directory_api_endpoint
           SET credential_descriptor_hash = repeat('0', 64),
               endpoint_signature_hash = repeat('0', 64),
               credential_descriptor_json = '{{}}'::jsonb,
               endpoint_signature_json = '{{}}'::jsonb
        """
    )
    await connection.execute(
        f"""
        UPDATE {schema}.provider_directory_api_endpoint
           SET credential_descriptor_hash = $1,
               endpoint_signature_hash = $2,
               credential_descriptor_json = '{{}}'::jsonb,
               endpoint_signature_json = '{{}}'::jsonb
         WHERE endpoint_id = $3
        """,
        legacy_endpoint.credential_descriptor_hash,
        legacy_endpoint.endpoint_signature_hash,
        pair.legacy_endpoint_id,
    )
    await connection.execute(
        f"""
        UPDATE {schema}.provider_directory_api_endpoint
           SET endpoint_signature_hash = $1
         WHERE endpoint_id = $2
        """,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
        pair.rooted_endpoint_id,
    )
    await connection.execute(
        f"""
        ALTER TABLE {schema}.provider_directory_api_endpoint
            ALTER COLUMN credential_descriptor_hash SET NOT NULL,
            ALTER COLUMN endpoint_signature_hash SET NOT NULL;
        """
    )


async def _create_serving_relation_tables(connection, schema: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE {schema}.provider_directory_dataset_insurance_plan (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id)
                ON DELETE CASCADE,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_id)
        );
        CREATE TABLE {schema}.provider_directory_dataset_network_plan (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id)
                ON DELETE CASCADE,
            network_resource_id varchar(256) NOT NULL,
            insurance_plan_resource_id varchar(256) NOT NULL,
            PRIMARY KEY (
                dataset_id, network_resource_id, insurance_plan_resource_id
            )
        );
        CREATE TABLE {schema}.provider_directory_dataset_affiliation_organization (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id)
                ON DELETE CASCADE,
            participating_organization_resource_id varchar(256) NOT NULL,
            affiliation_resource_id varchar(256) NOT NULL,
            PRIMARY KEY (
                dataset_id, participating_organization_resource_id,
                affiliation_resource_id
            )
        );
        """
    )


def configure_database(monkeypatch, url) -> Database:
    """Build a database facade for an isolated synthetic schema."""

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    return Database()


async def create_foundation(connection, schema_name: str) -> None:
    """Create the minimum generic publication tables required by migration."""

    schema = quoted(schema_name)
    await connection.execute(f"CREATE SCHEMA {schema}")
    await connection.execute(
        f"""
        CREATE TABLE {schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY,
            canonical_api_base text NOT NULL,
            credential_descriptor_hash varchar(64) NOT NULL,
            endpoint_signature_hash varchar(64) NOT NULL
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL REFERENCES
                {schema}.provider_directory_api_endpoint(endpoint_id),
            dataset_hash varchar(64), status varchar(32) NOT NULL,
            is_current boolean NOT NULL, resource_count bigint NOT NULL,
            publication_metadata_json jsonb
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                {schema}.provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        )
        """
    )


async def seed_practitioner_dataset(connection, schema_name: str) -> None:
    """Seed one exact, published synthetic Practitioner root cohort."""

    schema = quoted(schema_name)
    metadata = {
        "cohort_complete": True,
        "cohort_id": ROOT_COHORT_ID,
        "dataset_id": ROOT_DATASET_ID,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "endpoint_id": ENDPOINT_ID,
        "expected_resources": ["Practitioner"],
        "resource_counts": {"Practitioner": 1},
        "selected_resources": ["Practitioner"],
        "terminal_set_sha256": ROOT_PROOF,
    }
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_api_endpoint "
        "(endpoint_id, canonical_api_base, credential_descriptor_hash, "
        "endpoint_signature_hash) VALUES ($1, $2, $3, $4)",
        ENDPOINT_ID,
        API_BASE,
        "e" * 64,
        ENDPOINT_SIGNATURE,
    )
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, endpoint_id, dataset_hash, status, is_current, "
        "resource_count, publication_metadata_json) "
        "VALUES ($1, $2, $3, 'published', true, 1, $4::jsonb)",
        ROOT_DATASET_ID,
        ENDPOINT_ID,
        DATASET_HASH,
        json.dumps(metadata),
    )
    practitioner_by_field = {
        "resourceType": "Practitioner",
        "id": ROOT_RESOURCE_ID,
    }
    await connection.execute(
        f"INSERT INTO {schema}.provider_directory_dataset_resource "
        "(dataset_id, resource_type, resource_id, payload_hash, payload_json) "
        "VALUES ($1, 'Practitioner', $2, $3, $4::jsonb)",
        ROOT_DATASET_ID,
        ROOT_RESOURCE_ID,
        "f" * 64,
        json.dumps(practitioner_by_field),
    )


def acquisition_identity(
    current,
    role: str,
    run_digit: str,
    intent_digit: str,
    *,
    max_payload_bytes: int = 1_000_000,
):
    """Build one role-specific acquisition over a shared comparison scope."""

    scope = build_provider_directory_rooted_graph_scope(
        root_dataset_variant=current.variant,
        root_publication_contract_id=current.root_publication_contract_id,
        root_source_id=current.root_source_id,
        root_endpoint_id=current.root_endpoint_id,
        acquisition_source_id=current.acquisition_source_id,
        acquisition_endpoint_id=current.acquisition_endpoint_id,
        source_authority_id=current.source_authority_id,
        root_dataset_id=current.dataset_id,
        root_dataset_hash=current.dataset_hash,
        root_content_proof_sha256=current.root_content_proof_sha256,
        root_resource_count=current.practitioner_resource_count,
        max_work_items=20,
        max_resource_rows=20,
        max_edge_rows=40,
        max_payload_bytes=max_payload_bytes,
    )
    return build_provider_directory_rooted_graph_acquisition_identity(
        scope,
        root_cohort_id=current.root_cohort_id,
        endpoint_signature_sha256=current.endpoint_signature_sha256,
        acquisition_role=role,
        run_id="pdrgr_" + run_digit * 48,
        dataset_intent_id="pdrgi_" + intent_digit * 48,
    )


async def work_rows(database: Database, acquisition_id: str):
    """Return the deterministic initial work census for one acquisition."""

    schema = quoted(os.environ["HLTHPRT_DB_SCHEMA"])
    return await database.all(
        f"SELECT query_id, kind FROM {schema}.{WORK_TABLE} "
        "WHERE acquisition_id = :acquisition_id ORDER BY kind",
        acquisition_id=acquisition_id,
    )


async def expire_claim(connection, schema_name: str, acquisition_id: str) -> None:
    """Simulate elapsed wall time without weakening the production trigger."""

    schema = quoted(schema_name)
    await connection.execute(
        f"ALTER TABLE {schema}.{WORK_TABLE} DISABLE TRIGGER {WORK_TRIGGER}"
    )
    try:
        await connection.execute(
            f"UPDATE {schema}.{WORK_TABLE} SET lease_expires_at = "
            "clock_timestamp() - interval '1 second' "
            "WHERE acquisition_id = $1 AND status = 'leased'",
            acquisition_id,
        )
    finally:
        await connection.execute(
            f"ALTER TABLE {schema}.{WORK_TABLE} ENABLE ALWAYS TRIGGER {WORK_TRIGGER}"
        )


def resources_for_kind(kind: str):
    """Return synthetic results for either initial rooted-graph query kind."""

    if kind == "full_insurance_plan_census":
        return [
            {
                "resourceType": "InsurancePlan",
                "id": "plan.synthetic-selected",
                "network": [{"reference": "Organization/org.synthetic-1"}],
            },
            {
                "resourceType": "InsurancePlan",
                "id": "plan.synthetic-unselected",
                "network": [{"reference": "Organization/org.synthetic-other"}],
            },
        ]
    return [
        {
            "resourceType": "PractitionerRole",
            "id": "role.synthetic-1",
            "practitioner": {"reference": f"Practitioner/{ROOT_RESOURCE_ID}"},
            "organization": {"reference": "Organization/org.synthetic-1"},
            "endpoint": [{"reference": "Endpoint/endpoint.synthetic-missing"}],
            "extension": [
                {
                    "url": (
                        PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[
                            0
                        ]
                    ),
                    "valueReference": {"reference": "Organization/org.synthetic-1"},
                }
            ],
        }
    ]
