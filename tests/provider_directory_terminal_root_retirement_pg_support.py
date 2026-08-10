# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL fixture for terminal-root retirement proof."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import importlib.util
import json
import os
from pathlib import Path
from typing import Any, AsyncIterator
import uuid

import pytest
from sqlalchemy.engine import make_url

from db.connection import Database
from tests.tin_npi_connector_postgres_support import (
    POSTGRES_DSN_ENV,
    open_test_connection,
)


asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / ("20260810090000_provider_directory_terminal_root_retirement.py")
)
SOURCE_ID = "source-terminal"
ENDPOINT_ID = "endpoint-terminal"
CURRENT_DATASET_ID = "dataset-current"
TARGET_DATASET_ID = "dataset-terminal"
ROOT_RUN_ID = "run-terminal-0"
OWNER_RUN_ID = "run-terminal-3"
ORPHAN_DATASET_ID = "dataset-predecessorless"
RELATION_ORDERS = {
    "provider_directory_api_endpoint": "endpoint_id",
    "provider_directory_source": "source_id",
    "provider_directory_endpoint_dataset": "dataset_id",
    "import_run": "run_id",
    "provider_directory_bulk_acquisition_checkpoint": "checkpoint_id",
    "provider_directory_bulk_output_checkpoint": "checkpoint_id, output_id",
    "provider_directory_dataset_affiliation_organization": (
        "dataset_id, participating_organization_resource_id, affiliation_resource_id"
    ),
    "provider_directory_dataset_insurance_plan": "dataset_id, resource_id",
    "provider_directory_dataset_network_plan": (
        "dataset_id, network_resource_id, insurance_plan_resource_id"
    ),
    "provider_directory_dataset_proof_shard": "dataset_id, shard_id",
    "provider_directory_dataset_rehydration_checkpoint": (
        "dataset_id, source_id, acquisition_root_run_id, resource_type"
    ),
    "provider_directory_dataset_resource": "dataset_id, resource_type, resource_id",
    "provider_directory_pagination_checkpoint": (
        "dataset_id, canonical_api_base, resource_type, source_scope_hash"
    ),
    "provider_directory_uhc_flex_npi_cohort": "cohort_id",
    "provider_directory_uhc_flex_practitioner_dataset": "dataset_id",
    "provider_directory_uhc_flex_practitioner_dataset_resource": (
        "dataset_id, resource_id"
    ),
}


class SqlCapture:
    """Capture SQL emitted through the Alembic operations facade."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


@dataclass
class RetirementPostgres:
    """One isolated schema with setup and runtime database handles."""

    connection: Any
    database: Database
    schema_name: str
    migration: Any

    @property
    def schema(self) -> str:
        return quote(self.schema_name)

    async def migrate(self, action: str) -> None:
        capture = SqlCapture()
        self.migration.op = capture
        getattr(self.migration, action)()
        async with self.connection.transaction():
            for statement in capture.statements:
                await self.connection.execute(statement)

    async def rows(self, relation: str) -> tuple[str, ...]:
        order = RELATION_ORDERS[relation]
        records = await self.connection.fetch(
            f"SELECT pg_catalog.to_jsonb(row)::text AS value "
            f"FROM {self.schema}.{quote(relation)} AS row ORDER BY {order}"
        )
        return tuple(record["value"] for record in records)

    async def snapshot(self) -> dict[str, tuple[str, ...]]:
        return {relation: await self.rows(relation) for relation in RELATION_ORDERS}


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def load_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "terminal_root_retirement_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _configure_runtime(monkeypatch: pytest.MonkeyPatch, schema_name: str) -> None:
    url = make_url(os.environ[POSTGRES_DSN_ENV])
    setting_by_name = {
        "HLTHPRT_DB_DRIVER": "asyncpg",
        "HLTHPRT_DB_HOST": str(url.host),
        "HLTHPRT_DB_PORT": str(url.port or 5432),
        "HLTHPRT_DB_USER": str(url.username),
        "HLTHPRT_DB_PASSWORD": str(url.password or ""),
        "HLTHPRT_DB_DATABASE": str(url.database),
        "HLTHPRT_DB_SCHEMA": schema_name,
        "DB_SCHEMA": schema_name,
        "HLTHPRT_PROVIDER_DIRECTORY_TERMINAL_ROOT_RETIREMENT_ENABLED": "true",
    }
    for key, value in setting_by_name.items():
        monkeypatch.setenv(key, value)


async def _create_foundation(connection: Any, schema_name: str) -> None:
    schema = quote(schema_name)
    await connection.execute(f"CREATE SCHEMA {schema}")
    for statement in (
        _foundation_catalog_sql(schema),
        _foundation_evidence_sql(schema),
        _foundation_projection_sql(schema),
    ):
        await connection.execute(statement)


def _foundation_catalog_sql(schema: str) -> str:
    """Return the minimal endpoint, run, and parent relation DDL."""

    return f"""
    CREATE TABLE {schema}.provider_directory_api_endpoint (
        endpoint_id varchar(64) PRIMARY KEY, canonical_api_base text NOT NULL,
        metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb);
    CREATE TABLE {schema}.provider_directory_source (
        source_id varchar(64) PRIMARY KEY, endpoint_id varchar(64) NOT NULL,
        canonical_api_base text NOT NULL, metadata_json jsonb NOT NULL DEFAULT '{{}}');
    CREATE TABLE {schema}.import_run (
        run_id varchar(64) PRIMARY KEY, importer varchar(64) NOT NULL,
        status varchar(32) NOT NULL, retry_of_run_id varchar(64),
        created_at timestamptz NOT NULL, started_at timestamptz,
        finished_at timestamptz, summary_json jsonb NOT NULL DEFAULT '{{}}');
    CREATE TABLE {schema}.provider_directory_endpoint_dataset (
        dataset_id varchar(96) PRIMARY KEY, endpoint_id varchar(64) NOT NULL,
        import_run_id varchar(64), acquisition_root_run_id varchar(64),
        previous_dataset_id varchar(96), status varchar(40) NOT NULL,
        is_current boolean NOT NULL, resource_count bigint NOT NULL,
        dataset_hash varchar(64), validated_at timestamptz, published_at timestamptz,
        superseded_at timestamptz, completion_proof_required_version varchar(32),
        completion_proof_json jsonb, completion_proof_sha256 varchar(64),
        publication_metadata_json jsonb NOT NULL DEFAULT '{{}}',
        created_at timestamptz NOT NULL DEFAULT transaction_timestamp());
    """


def _foundation_evidence_sql(schema: str) -> str:
    """Return the resource, proof, and checkpoint relation DDL."""

    return f"""
    CREATE TABLE {schema}.provider_directory_dataset_resource (
        dataset_id varchar(96), resource_type varchar(64), resource_id varchar(256),
        payload_hash varchar(64), payload_json jsonb,
        PRIMARY KEY (dataset_id, resource_type, resource_id));
    CREATE TABLE {schema}.provider_directory_dataset_proof_shard (
        dataset_id varchar(96), shard_id integer, payload_bytes bytea,
        resource_count bigint, PRIMARY KEY (dataset_id, shard_id));
    CREATE TABLE {schema}.provider_directory_pagination_checkpoint (
        dataset_id varchar(96), canonical_api_base text, resource_type varchar(64),
        source_scope_hash varchar(64), state varchar(32),
        PRIMARY KEY (dataset_id, canonical_api_base, resource_type, source_scope_hash));
    CREATE TABLE {schema}.provider_directory_bulk_acquisition_checkpoint (
        checkpoint_id varchar(64) PRIMARY KEY, dataset_id varchar(96), state varchar(32));
    CREATE TABLE {schema}.provider_directory_bulk_output_checkpoint (
        checkpoint_id varchar(64), output_id varchar(64), state varchar(32),
        PRIMARY KEY (checkpoint_id, output_id));
    CREATE TABLE {schema}.provider_directory_dataset_rehydration_checkpoint (
        dataset_id varchar(96), source_id varchar(64), acquisition_root_run_id varchar(64),
        resource_type varchar(64), state varchar(32),
        PRIMARY KEY (dataset_id, source_id, acquisition_root_run_id, resource_type));
    """


def _foundation_projection_sql(schema: str) -> str:
    """Return the remaining zero-row evidence projection relation DDL."""

    return f"""
    CREATE TABLE {schema}.provider_directory_dataset_affiliation_organization (
        dataset_id varchar(96), participating_organization_resource_id varchar(256),
        affiliation_resource_id varchar(256), PRIMARY KEY (
            dataset_id, participating_organization_resource_id, affiliation_resource_id));
    CREATE TABLE {schema}.provider_directory_dataset_insurance_plan (
        dataset_id varchar(96), resource_id varchar(256),
        PRIMARY KEY (dataset_id, resource_id));
    CREATE TABLE {schema}.provider_directory_dataset_network_plan (
        dataset_id varchar(96), network_resource_id varchar(256),
        insurance_plan_resource_id varchar(256), PRIMARY KEY (
            dataset_id, network_resource_id, insurance_plan_resource_id));
    CREATE TABLE {schema}.provider_directory_uhc_flex_npi_cohort (
        cohort_id varchar(64) PRIMARY KEY, official_dataset_id varchar(96));
    CREATE TABLE {schema}.provider_directory_uhc_flex_practitioner_dataset (
        dataset_id varchar(96) PRIMARY KEY, previous_dataset_id varchar(96), state varchar(32));
    CREATE TABLE {schema}.provider_directory_uhc_flex_practitioner_dataset_resource (
        dataset_id varchar(96), resource_id varchar(256),
        PRIMARY KEY (dataset_id, resource_id));
    """


async def seed_exact_fixture(scenario: RetirementPostgres) -> None:
    await _seed_endpoint_pair(scenario)
    await _seed_terminal_lineage(scenario)
    await _seed_datasets(scenario)
    await _seed_target_evidence(scenario)


async def _seed_endpoint_pair(scenario: RetirementPostgres) -> None:
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_api_endpoint
            (endpoint_id, canonical_api_base) VALUES
            ($1, 'https://terminal.example.invalid/fhir'),
            ('endpoint-predecessorless', 'https://orphan.example.invalid/fhir')""",
        ENDPOINT_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_source
            (source_id, endpoint_id, canonical_api_base) VALUES
            ($2, $1, 'https://terminal.example.invalid/fhir'),
            ('source-predecessorless', 'endpoint-predecessorless',
             'https://orphan.example.invalid/fhir')""",
        ENDPOINT_ID,
        SOURCE_ID,
    )


async def _seed_terminal_lineage(scenario: RetirementPostgres) -> None:
    rows = [
        (ROOT_RUN_ID, None, "failed", 40),
        ("run-terminal-1", ROOT_RUN_ID, "canceled", 35),
        ("run-terminal-2", "run-terminal-1", "dead_letter", 30),
        (OWNER_RUN_ID, "run-terminal-2", "cancelled", 25),
        ("run-predecessorless", None, "failed", 20),
    ]
    await scenario.connection.executemany(
        f"""INSERT INTO {scenario.schema}.import_run
            (run_id, importer, status, retry_of_run_id, created_at, started_at,
             finished_at) VALUES ($1, 'provider-directory-fhir', $3, $2,
             transaction_timestamp() - make_interval(mins => $4 + 2),
             transaction_timestamp() - make_interval(mins => $4 + 1),
             transaction_timestamp() - make_interval(mins => $4))""",
        rows,
    )


async def _seed_datasets(scenario: RetirementPostgres) -> None:
    metadata = json.dumps({"source_ids": [SOURCE_ID]})
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_endpoint_dataset
            (dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
             previous_dataset_id, status, is_current, resource_count, dataset_hash,
             validated_at, published_at, publication_metadata_json) VALUES
            ($1, $2, 'run-current', 'run-current', NULL, 'published', true, 1,
             $3, transaction_timestamp() - interval '2 days',
             transaction_timestamp() - interval '2 days', '{{}}')""",
        CURRENT_DATASET_ID,
        ENDPOINT_ID,
        "c" * 64,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_endpoint_dataset
            (dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
             previous_dataset_id, status, is_current, resource_count, dataset_hash,
             validated_at, published_at, publication_metadata_json) VALUES
            ($3, $2, $4, $5, $1, 'acquiring', false, 3, NULL, NULL, NULL, $6),
            ($7, 'endpoint-predecessorless', 'run-predecessorless',
             'run-predecessorless', NULL, 'acquiring', false, 0, NULL, NULL,
             NULL, '{{"source_ids":["source-predecessorless"]}}')""",
        CURRENT_DATASET_ID,
        ENDPOINT_ID,
        TARGET_DATASET_ID,
        OWNER_RUN_ID,
        ROOT_RUN_ID,
        metadata,
        ORPHAN_DATASET_ID,
    )


async def _seed_target_evidence(scenario: RetirementPostgres) -> None:
    schema = scenario.schema
    await scenario.connection.execute(
        f"""INSERT INTO {schema}.provider_directory_dataset_resource VALUES
            ($1, 'Organization', 'org-1', $2, '{{"resourceType":"Organization"}}'),
            ($1, 'Organization', 'org-2', $2,
             '{{"resourceType":"Organization","active":true}}'),
            ($1, 'Practitioner', 'practitioner-1', $3,
             '{{"resourceType":"Practitioner"}}'),
            ($4, 'Organization', 'current-org', $2,
             '{{"resourceType":"Organization","active":true}}')""",
        TARGET_DATASET_ID,
        "a" * 64,
        "b" * 64,
        CURRENT_DATASET_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {schema}.provider_directory_dataset_proof_shard
            VALUES ('{TARGET_DATASET_ID}', 0, decode('abcd', 'hex'), 4);
        INSERT INTO {schema}.provider_directory_pagination_checkpoint
            VALUES ('{TARGET_DATASET_ID}', 'https://terminal.example.invalid/fhir',
                    'Practitioner', '{'d' * 64}', 'complete');
        INSERT INTO {schema}.provider_directory_bulk_acquisition_checkpoint
            VALUES ('bulk-1', '{TARGET_DATASET_ID}', 'complete');
        INSERT INTO {schema}.provider_directory_bulk_output_checkpoint
            VALUES ('bulk-1', 'output-1', 'committed');"""
    )


async def seed_ineligible(
    scenario: RetirementPostgres,
    slug: str,
    failure_kind: str,
) -> dict[str, str]:
    endpoint = f"endpoint-{slug}"
    source_id = f"source-{slug}"
    current = f"dataset-{slug}-current"
    target_dataset_id = f"dataset-{slug}-target"
    root = f"run-{slug}"
    contract = "semantic_bound_v4" if failure_kind == "v4" else "transport_bound_v1"
    run_status = "running" if failure_kind == "nonterminal" else "failed"
    finished = (
        "NULL"
        if failure_kind == "nonterminal"
        else (
            "transaction_timestamp()"
            if failure_kind == "young"
            else "transaction_timestamp() - interval '20 minutes'"
        )
    )
    await _seed_ineligible_catalog(
        scenario, endpoint, source_id, root, run_status, finished, slug
    )
    await _seed_ineligible_datasets(
        scenario, endpoint, source_id, root, current, target_dataset_id, contract
    )
    return {
        "source_id": source_id,
        "endpoint_id": endpoint,
        "dataset_id": target_dataset_id,
        "acquisition_root_run_id": root,
        "owner_run_id": root,
        "expected_current_dataset_id": current,
    }


async def _seed_ineligible_catalog(
    scenario: RetirementPostgres,
    endpoint: str,
    source_id: str,
    root: str,
    run_status: str,
    finished: str,
    slug: str,
) -> None:
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_api_endpoint "
        "VALUES ($1, $2, '{}')",
        endpoint,
        f"https://{slug}.example.invalid/fhir",
    )
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_source "
        "VALUES ($1, $2, $3, '{}')",
        source_id,
        endpoint,
        f"https://{slug}.example.invalid/fhir",
    )
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.import_run
            (run_id, importer, status, created_at, started_at, finished_at)
            VALUES ($1, 'provider-directory-fhir', $2,
                    transaction_timestamp() - interval '30 minutes',
                    transaction_timestamp() - interval '29 minutes', {finished})""",
        root,
        run_status,
    )


async def _seed_ineligible_datasets(
    scenario: RetirementPostgres,
    endpoint: str,
    source_id: str,
    root: str,
    current: str,
    target_dataset_id: str,
    contract: str,
) -> None:
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_endpoint_dataset
            (dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
             previous_dataset_id, status, is_current, resource_count, dataset_hash,
             validated_at, published_at, publication_metadata_json) VALUES
            ($1, $2, $3, $3, NULL, 'published', true, 1, $4,
             transaction_timestamp() - interval '1 hour',
             transaction_timestamp() - interval '1 hour', '{{}}'),
            ($5, $2, $3, $3, $1, 'acquiring', false, 1, NULL, NULL, NULL,
             jsonb_build_object(
                 'source_ids', jsonb_build_array(CAST($6 AS text)),
                 'resource_hash_contract', CAST($7 AS text)
             ))""",
        current,
        endpoint,
        root,
        "e" * 64,
        target_dataset_id,
        source_id,
        contract,
    )
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_dataset_resource "
        "VALUES ($1, 'Organization', 'org', $2, "
        '\'{"resourceType":"Organization"}\')',
        target_dataset_id,
        "e" * 64,
    )
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_dataset_proof_shard "
        "VALUES ($1, 0, decode('01', 'hex'), 1)",
        target_dataset_id,
    )


@asynccontextmanager
async def retirement_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RetirementPostgres]:
    connection = await open_test_connection()
    schema_name = f"provider_terminal_retirement_{uuid.uuid4().hex}"
    _configure_runtime(monkeypatch, schema_name)
    database = Database()
    scenario = RetirementPostgres(
        connection=connection,
        database=database,
        schema_name=schema_name,
        migration=load_migration(),
    )
    try:
        await _create_foundation(connection, schema_name)
        await seed_exact_fixture(scenario)
        await scenario.migrate("upgrade")
        await database.connect()
        yield scenario
    finally:
        await database.disconnect()
        await connection.execute(f"DROP SCHEMA IF EXISTS {quote(schema_name)} CASCADE")
        await connection.close()
