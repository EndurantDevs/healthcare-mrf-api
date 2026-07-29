# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared disposable PostgreSQL support for connector migration proofs."""

from __future__ import annotations

import importlib.util
import json
import os
from dataclasses import dataclass
from pathlib import Path
import re
import uuid

import pytest

from process import tin_npi_connector as connector
from process.provider_directory_source_summary import (
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)


asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260729110000_tin_npi_connector.py"
POSTGRES_DSN_ENV = "HLTHPRT_TIN_NPI_CONNECTOR_POSTGRES_DSN"
TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)
ORGANIZATION_ROWS = (
    ("organization-a", "11" * 32),
    ("organization-b", "22" * 32),
)


class SqlCapture:
    """Capture Alembic operation statements for controlled replay."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "tin_npi_connector_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def open_test_connection():
    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    connection = await asyncpg.connect(database_dsn)
    database_name = str(await connection.fetchval("SELECT current_database()"))
    if TEST_DATABASE_PATTERN.search(database_name) is None:
        await connection.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return connection


async def run_migration(migration, action: str, connection) -> list[str]:
    sql_capture = SqlCapture()
    migration.op = sql_capture
    getattr(migration, action)()
    for sql_statement in sql_capture.statements:
        await connection.execute(sql_statement)
    return sql_capture.statements


async def create_fence_tables(connection, schema: str) -> None:
    await _create_directory_catalog_tables(connection, schema)
    await _create_dataset_resource_table(connection, schema)
    await _create_ptg_manifest_table(connection, schema)


async def _create_directory_catalog_tables(connection, schema: str) -> None:
    quoted_schema = f'"{schema}"'
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64)
        )
        """
    )
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            import_run_id varchar(64),
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL,
            resource_count bigint NOT NULL,
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb
        )
        """
    )


async def _create_dataset_resource_table(connection, schema: str) -> None:
    quoted_schema = f'"{schema}"'
    await connection.execute(
        f"""
        CREATE TABLE {quoted_schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL
                REFERENCES
                    {quoted_schema}.provider_directory_endpoint_dataset (
                        dataset_id
                    ),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        )
        """
    )


async def _create_ptg_manifest_table(connection, schema: str) -> None:
    await connection.execute(
        f"""
        CREATE TABLE "{schema}".ptg2_provider_tax_identity_manifest (
            snapshot_key bigint PRIMARY KEY,
            token_policy_id varchar(64) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL
        )
        """
    )


async def expect_postgres_error(
    connection,
    marker: str,
    statement: str,
    *parameters,
) -> None:
    try:
        async with connection.transaction():
            await connection.execute(statement, *parameters)
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker!r}")


def build_scan_proof(
    *,
    token_policy_id: str,
    identifier_rule: connector.FhirTinNpiIdentifierRule,
    evidence_rows: tuple[connector.FhirTinNpiEvidence, ...],
    source_summary_sha256: str,
    organization_resource_count: int,
    organization_resource_sha256: str,
    matched_organization_count: int,
    matched_evidence_counts: tuple[tuple[str, int], ...] | None = None,
) -> connector.FhirOrganizationScanProof:
    state_counts = tuple(
        (
            evidence_state.value,
            _state_count(
                evidence_state,
                organization_resource_count,
                matched_organization_count,
            ),
        )
        for evidence_state in sorted(
            connector.FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate_state: candidate_state.value,
        )
    )
    policy_counts = matched_evidence_counts or ((token_policy_id, len(evidence_rows)),)
    return connector.FhirOrganizationScanProof(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        source_summary_sha256=source_summary_sha256,
        identifier_rule_id=identifier_rule.rule_id,
        identifier_rule_sha256=identifier_rule.descriptor_sha256,
        organization_resource_count=organization_resource_count,
        organization_resource_sha256=organization_resource_sha256,
        state_counts=state_counts,
        matched_evidence_counts=policy_counts,
        matched_evidence_sha256=(
            connector.canonical_fhir_evidence_set_digest(evidence_rows).hex()
        ),
    )


def _state_count(evidence_state, organization_count, matched_count):
    if evidence_state is connector.FhirOrganizationEvidenceState.MATCHED:
        return matched_count
    if evidence_state is connector.FhirOrganizationEvidenceState.MISSING_IDENTIFIERS:
        return organization_count - matched_count
    return 0


def build_identifier_policy():
    identifier_rule = connector.FhirTinNpiIdentifierRule(
        rule_id="healthporta.test.source-a.endpoint-a.tax-as-ein.v1",
        source_id="source-a",
        endpoint_id="endpoint-a",
        npi_systems=("http://hl7.org/fhir/sid/us-npi",),
        npi_type_codings=(("http://terminology.hl7.org/CodeSystem/v2-0203", "NPI"),),
        ein_systems=(),
        ein_type_codings=(("http://terminology.hl7.org/CodeSystem/v2-0203", "TAX"),),
    )
    identifier_policy = connector.FhirTinNpiIdentifierPolicy(
        policy_id="healthporta.test.fhir-tax-as-ein.v2",
        rules=(identifier_rule,),
    )
    return identifier_rule, identifier_policy


def build_source_summary_payload():
    organization_digest = connector.canonical_fhir_organization_identity_sha256(
        ORGANIZATION_ROWS
    )
    source_summary = build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            dataset_id="dataset-a",
            endpoint_id="endpoint-a",
            acquisition_root_run_id="run-a",
            dataset_hash="ab" * 32,
        ),
        source_ids=("source-a",),
        selected_resources=("Organization",),
        count_by_resource={"Organization": len(ORGANIZATION_ROWS)},
        hash_by_resource={"Organization": organization_digest},
        count_by_field={"organization_resources": len(ORGANIZATION_ROWS)},
    )
    return organization_digest, source_summary


async def insert_published_directory(connection, schema: str):
    quoted_schema = f'"{schema}"'
    organization_digest, source_summary = build_source_summary_payload()
    await _insert_directory_identity(connection, quoted_schema, source_summary)
    await connection.executemany(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
            dataset_id,
            resource_type,
            resource_id,
            payload_hash,
            payload_json
        ) VALUES ('dataset-a', 'Organization', $1, $2, $3::jsonb)
        """,
        [
            (
                resource_id,
                payload_hash,
                json.dumps(
                    {"id": resource_id, "resourceType": "Organization"},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            for resource_id, payload_hash in ORGANIZATION_ROWS
        ],
    )
    return organization_digest, source_summary


async def _insert_directory_identity(connection, quoted_schema, source_summary):
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ('endpoint-a')
        """
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_source (
            source_id,
            endpoint_id
        ) VALUES ('source-a', 'endpoint-a')
        """
    )
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id,
            endpoint_id,
            acquisition_root_run_id,
            dataset_hash,
            status,
            is_current,
            resource_count,
            validated_at,
            published_at,
            publication_metadata_json
        ) VALUES (
            'dataset-a', 'endpoint-a', 'run-a', $1, 'published', TRUE, 2,
            timestamp '2026-07-27 00:00:00',
            timestamp '2026-07-27 00:01:00',
            $2::jsonb
        )
        """,
        "ab" * 32,
        json.dumps(
            {
                "expected_resources": ["Organization"],
                "selected_resources": ["Organization"],
                "source_ids": ["source-a"],
                "source_summary_v1": source_summary,
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


@dataclass
class TransactionalSchema:
    """One random schema inside a rollback-only outer transaction."""

    connection: object
    transaction: object
    schema: str
    migration: object

    @property
    def quoted_schema(self) -> str:
        return f'"{self.schema}"'

    @classmethod
    async def create(cls, monkeypatch):
        connection = await open_test_connection()
        transaction = connection.transaction()
        await transaction.start()
        schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        migration = load_migration()
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await create_fence_tables(connection, schema)
        return cls(connection, transaction, schema, migration)

    async def close(self):
        await self.transaction.rollback()
        await self.connection.close()

    async def upgrade(self):
        await run_migration(self.migration, "upgrade", self.connection)
