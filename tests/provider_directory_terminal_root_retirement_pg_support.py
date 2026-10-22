# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL fixture for terminal-root retirement proof."""

from __future__ import annotations

import importlib.util
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_source_checkpoint_catalog import (
    _GUARDS,
    CHECKPOINT_LAYOUTS,
    RETIRED_BULK_HOOKS,
    capture_checkpoint_controls,
)
from tests.provider_directory_dataset_rehydrate_checkpoint_support import _checkpoint_function, _guard, _layout
from tests.provider_directory_source_postgres_support import native_source_component
from tests.tin_npi_connector_postgres_support import (
    POSTGRES_DSN_ENV,
    open_test_connection,
)

asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic/versions" / ("20260810090000_provider_directory_terminal_root_retirement.py")
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
    "provider_directory_dataset_network_plan": ("dataset_id, network_resource_id, insurance_plan_resource_id"),
    "provider_directory_dataset_proof_shard": "dataset_id, shard_id",
    "provider_directory_dataset_rehydration_checkpoint": (
        "dataset_id, source_id, acquisition_root_run_id, resource_type"
    ),
    "provider_directory_dataset_resource": "dataset_id, resource_type, resource_id",
    "provider_directory_pagination_checkpoint": ("dataset_id, canonical_api_base, resource_type, source_scope_hash"),
    "provider_directory_uhc_flex_npi_cohort": "cohort_id",
    "provider_directory_uhc_flex_practitioner_dataset": "dataset_id",
    "provider_directory_uhc_flex_practitioner_dataset_resource": ("dataset_id, resource_id"),
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
    database: Database | None
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
    for root, layout in CHECKPOINT_LAYOUTS.items():
        await _layout(connection, schema_name, root, layout)


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
        f"INSERT INTO {schema}.provider_directory_dataset_proof_shard VALUES ($1, 0, decode('abcd', 'hex'), 4)",
        TARGET_DATASET_ID,
    )
    await _seed_checkpoint_witnesses(scenario)


async def _seed_checkpoint_witnesses(scenario: RetirementPostgres) -> None:
    """Bind complete checkpoints to the seeded endpoint, sources, URL and lineage."""
    api_base = "https://terminal.example.invalid/fhir"
    source_ids = json.dumps([SOURCE_ID], separators=(",", ":"))
    scope_hash = sha256(source_ids.encode()).hexdigest()
    start_hash = sha256((api_base + "/Practitioner").encode()).hexdigest()
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_pagination_checkpoint
        (dataset_id, canonical_api_base, resource_type, source_scope_hash,
         source_ids, owner_run_id, retry_of_run_id, start_url_hash, state,
         acquisition_root_run_id) VALUES
        ($1, $2, 'Practitioner', $3, $4::jsonb, $5, 'run-terminal-2', $6,
         'complete', $7)""",
        TARGET_DATASET_ID,
        api_base,
        scope_hash,
        source_ids,
        OWNER_RUN_ID,
        start_hash,
        ROOT_RUN_ID,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_bulk_acquisition_checkpoint
        (checkpoint_id, dataset_id, canonical_api_base, resource_type,
         source_scope_hash, strategy_version, acquisition_root_run_id,
         owner_run_id, retry_of_run_id, endpoint_id, start_url_hash, state)
        VALUES ('bulk-1', $1, $2, 'Practitioner', $3, 'terminal-fixture-v1',
         $4, $5, 'run-terminal-2', $6, $7, 'complete')""",
        TARGET_DATASET_ID,
        api_base,
        scope_hash,
        ROOT_RUN_ID,
        OWNER_RUN_ID,
        ENDPOINT_ID,
        start_hash,
    )
    await scenario.connection.execute(
        f"""INSERT INTO {scenario.schema}.provider_directory_bulk_output_checkpoint
        (checkpoint_id, output_id, output_index, resource_type, output_url_hash, state)
        VALUES ('bulk-1', 'output-1', 0, 'Practitioner', $1, 'committed')""",
        sha256((api_base + "/bulk/output-1.ndjson").encode()).hexdigest(),
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
            "transaction_timestamp()" if failure_kind == "young" else "transaction_timestamp() - interval '20 minutes'"
        )
    )
    await _seed_ineligible_catalog(scenario, endpoint, source_id, root, run_status, finished, slug)
    await _seed_ineligible_datasets(scenario, endpoint, source_id, root, current, target_dataset_id, contract)
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
        f"INSERT INTO {scenario.schema}.provider_directory_api_endpoint VALUES ($1, $2, '{{}}')",
        endpoint,
        f"https://{slug}.example.invalid/fhir",
    )
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.provider_directory_source VALUES ($1, $2, $3, '{{}}')",
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
        f"INSERT INTO {scenario.schema}.provider_directory_dataset_proof_shard VALUES ($1, 0, decode('01', 'hex'), 1)",
        target_dataset_id,
    )


async def _install_checkpoint_guards(scenario: RetirementPostgres) -> None:
    """Add only missing current guards around the unchanged v1 retirement corpus."""
    await scenario.connection.execute(_checkpoint_function(scenario.schema_name))
    for root, guards in _GUARDS.items():
        for name, guard in guards.items():
            if name in RETIRED_BULK_HOOKS:
                continue
            present = await scenario.connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2)",
                scenario.schema_name + "." + root,
                name,
            )
            if not present:
                await _guard(scenario.connection, scenario.schema_name, root, name, guard)


async def _adopt_checkpoint_fixture(scenario: RetirementPostgres, component: Any) -> None:
    """Give the issued Source owner the declared fixture corpus, then attest it."""
    owner = quote(component.owner)
    async with scenario.connection.transaction():
        for root in RELATION_ORDERS:
            await scenario.connection.execute(f"ALTER TABLE {scenario.schema}.{quote(root)} OWNER TO {owner}")
        signatures = (
            *scenario.migration._function_signatures(scenario.schema_name),
            f'{scenario.schema}."guard_provider_directory_subset_abandonment_checkpoint"()',
        )
        for signature in signatures:
            await scenario.connection.execute(f"ALTER FUNCTION {signature} OWNER TO {owner}")
            await scenario.connection.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC")
        closed_roles = ",".join(quote(role) for role in (component.publisher, *component.policy.runtime_roles))
        for root in CHECKPOINT_LAYOUTS:
            relation = f"{scenario.schema}.{quote(root)}"
            await scenario.connection.execute(f"REVOKE ALL ON TABLE {relation} FROM PUBLIC, {closed_roles}")
            await scenario.connection.execute(f"GRANT SELECT ON TABLE {relation} TO {quote(component.reader)}")
    async with source_transaction(component.database, isolation="read committed"):
        publisher = bound_publisher()
        assert publisher.policy is component.policy
        await capture_checkpoint_controls(publisher.connection, publisher.policy)


@asynccontextmanager
async def retirement_postgres(
    monkeypatch: pytest.MonkeyPatch,
    *,
    current_runtime: bool = True,
) -> AsyncIterator[RetirementPostgres]:
    connection = await open_test_connection()
    schema_name = f"provider_terminal_retirement_{uuid.uuid4().hex}"
    engine = None
    try:
        _configure_runtime(monkeypatch, schema_name)
        scenario = RetirementPostgres(connection, None, schema_name, load_migration())
        async with connection.transaction():
            await _create_foundation(connection, schema_name)
            await seed_exact_fixture(scenario)
            await scenario.migrate("upgrade")
            if current_runtime:
                await _install_checkpoint_guards(scenario)
        if not current_runtime:
            yield scenario
            return
        url = make_url(os.environ[POSTGRES_DSN_ENV]).set(drivername="postgresql+asyncpg")
        engine = create_async_engine(url)
        async with native_source_component(engine, schema_name, monkeypatch) as component:
            scenario.database = component.database
            await _adopt_checkpoint_fixture(scenario, component)
            yield scenario
    finally:
        try:
            if engine is not None:
                await engine.dispose()
        finally:
            try:
                await connection.execute(f"DROP SCHEMA IF EXISTS {quote(schema_name)} CASCADE")
            finally:
                await connection.close()


@asynccontextmanager
async def historical_retirement_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RetirementPostgres]:
    """Keep catalog-only historical migration proofs on their original administrator."""
    async with retirement_postgres(monkeypatch, current_runtime=False) as scenario:
        assert scenario.database is None
        yield scenario
