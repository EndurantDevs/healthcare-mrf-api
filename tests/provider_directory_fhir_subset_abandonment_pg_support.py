# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Committed PostgreSQL fixture for reviewed subset abandonment."""

from __future__ import annotations

from contextlib import suppress
import json
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_BLOCKED_ERROR,
    SERVER_ISSUED_SUBSET_FETCH_MODE,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    load_migration as load_subset_migration,
    replace_subset_source,
)
from tests.provider_directory_subset_completion_pg_support import (
    RESOURCE_TYPES,
    VALID_SOURCE_SCOPE_SHA256,
    valid_source_metadata,
)
from tests.tin_npi_connector_postgres_support import (
    load_admission_seal_migration,
    POSTGRES_DSN_ENV,
)

CANONICAL_API_BASE = "https://directory.example.test/fhir"
DATASET_ID = "dataset-abandoned"
ENDPOINT_ID = "endpoint-a"
SERVING_ENDPOINT_ID = "endpoint-serving"
OWNER_RUN_ID = "owner-abandoned"
ROOT_RUN_ID = "root-abandoned"
SOURCE_ID = "synthetic-source"
VERIFICATION_CAMPAIGN_ID = valid_source_metadata(
    "pending_two_matching_reviewed_subset_acquisitions"
)["provider_directory_verification_campaign_id"]


async def install_admission_query_surface(scenario) -> None:
    """Install current admission readers in the historical fixture."""

    subset = load_subset_migration()
    admission = load_admission_seal_migration()
    for statement in (
        subset._content_proof_valid_function_sql(scenario.schema),
        admission._add_columns_sql(scenario.schema),
        admission._digest_function_sql(scenario.schema),
    ):
        await scenario.connection.execute(statement)


def guard_handoff_context(importer):
    """Build the exact retained-root guard context used by the race proof."""

    return importer.PaginationCheckpointContext(
        canonical_api_base=CANONICAL_API_BASE,
        source_scope_hash="1" * 64,
        source_ids=(SOURCE_ID,),
        owner_run_id=OWNER_RUN_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
    )

_PROOF_RELATION_SQL = """
CREATE TABLE {schema}.provider_directory_dataset_proof_shard (
    dataset_id varchar(96) NOT NULL,
    shard_id varchar(64) NOT NULL,
    endpoint_id varchar(64) NOT NULL,
    acquisition_root_run_id varchar(64) NOT NULL,
    source_ids_json jsonb NOT NULL,
    resource_count bigint NOT NULL,
    resource_counts_json jsonb NOT NULL,
    first_identity_json jsonb NOT NULL,
    last_identity_json jsonb NOT NULL,
    input_sha256 varchar(64) NOT NULL,
    artifact_sha256 varchar(64) NOT NULL,
    artifact_byte_count bigint NOT NULL,
    payload_bytes bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, shard_id),
    FOREIGN KEY (dataset_id) REFERENCES
        {schema}.provider_directory_endpoint_dataset(dataset_id)
        ON DELETE CASCADE
)
"""
_PAGINATION_RELATION_SQL = """
CREATE TABLE {schema}.provider_directory_pagination_checkpoint (
    canonical_api_base text NOT NULL,
    resource_type varchar(64) NOT NULL,
    source_scope_hash varchar(64) NOT NULL,
    dataset_id varchar(96) REFERENCES
        {schema}.provider_directory_endpoint_dataset(dataset_id),
    source_ids jsonb NOT NULL,
    acquisition_root_run_id varchar(64) NOT NULL,
    owner_run_id varchar(64) NOT NULL,
    retry_of_run_id varchar(64),
    start_url_hash varchar(64) NOT NULL,
    next_url text,
    state varchar(32) NOT NULL,
    pages_processed bigint NOT NULL DEFAULT 0,
    rows_processed bigint NOT NULL DEFAULT 0,
    recent_cursor_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
    completeness_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    completed_at timestamp,
    PRIMARY KEY (
        canonical_api_base, resource_type, source_scope_hash,
        acquisition_root_run_id
    )
)
"""
_BULK_RELATION_SQL = """
CREATE TABLE {schema}.provider_directory_bulk_acquisition_checkpoint (
    checkpoint_id varchar(64) PRIMARY KEY,
    canonical_api_base text NOT NULL,
    resource_type varchar(64) NOT NULL,
    source_scope_hash varchar(64) NOT NULL,
    strategy_version varchar(64) NOT NULL,
    acquisition_root_run_id varchar(64) NOT NULL,
    owner_run_id varchar(64) NOT NULL,
    retry_of_run_id varchar(64),
    endpoint_id varchar(64) NOT NULL REFERENCES
        {schema}.provider_directory_api_endpoint(endpoint_id),
    dataset_id varchar(96) NOT NULL REFERENCES
        {schema}.provider_directory_endpoint_dataset(dataset_id),
    start_url_hash varchar(64) NOT NULL,
    status_url_ciphertext text,
    status_url_hash varchar(64),
    manifest_hash varchar(64),
    manifest_ciphertext text,
    manifest_json jsonb,
    state varchar(32) NOT NULL,
    lease_expires_at timestamp,
    rows_written bigint NOT NULL DEFAULT 0,
    error text,
    created_at timestamp NOT NULL,
    accepted_at timestamp,
    last_polled_at timestamp,
    next_poll_at timestamp,
    manifest_received_at timestamp,
    completed_at timestamp,
    failed_at timestamp,
    updated_at timestamp NOT NULL,
    CONSTRAINT provider_directory_bulk_acquisition_identity_key UNIQUE (
        canonical_api_base, resource_type, source_scope_hash,
        strategy_version, acquisition_root_run_id, dataset_id
    )
)
"""


def runtime_database() -> Database:
    database_dsn = os.environ[POSTGRES_DSN_ENV]
    async_database_dsn = database_dsn.replace(
        "postgresql://",
        "postgresql+asyncpg://",
        1,
    )
    engine = create_async_engine(
        async_database_dsn,
        pool_size=1,
        max_overflow=0,
    )
    return Database(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


async def create_abandonment_relations(scenario) -> None:
    """Create the exact proof, pagination, and bulk checkpoint relations."""

    for relation_template in (
        _PROOF_RELATION_SQL,
        _PAGINATION_RELATION_SQL,
        _BULK_RELATION_SQL,
    ):
        await scenario.connection.execute(
            relation_template.format(schema=scenario.quoted_schema)
        )


def terminal_diagnostics() -> dict[str, dict[str, object]]:
    return {
        resource_type: {
            "bounded": False,
            "complete": False,
            "error": f"{CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410",
            "fetch_mode": SERVER_ISSUED_SUBSET_FETCH_MODE,
        }
        for resource_type in RESOURCE_TYPES
    }


def authorize_operator(monkeypatch, enabled_env: str) -> None:
    """Bind the selector-free operator to the neutral reviewed fixture."""

    monkeypatch.setenv(enabled_env, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: SOURCE_ID,
    )


async def seed_expired_root(scenario) -> None:
    """Seed one exact seven-resource failed root before guard adoption."""

    await _seed_source_alias(scenario)
    await insert_subset_candidate(
        scenario,
        dataset_id=DATASET_ID,
        root_run_id=ROOT_RUN_ID,
        resource_count=0,
    )
    await _seed_serving_decoy(scenario)
    await _bind_expired_candidate_identity(scenario)
    await insert_valid_subset_resources(scenario, DATASET_ID)
    await _seed_proof_shard(scenario)
    await _seed_pagination_checkpoints(scenario)


async def _seed_source_alias(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ($1), ($2)
        """,
        ENDPOINT_ID,
        SERVING_ENDPOINT_ID,
    )
    await replace_subset_source(
        scenario,
        "pending_two_matching_reviewed_subset_acquisitions",
        last_resource_import={
            "run_id": OWNER_RUN_ID,
            "resources": terminal_diagnostics(),
        },
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET endpoint_id = $1
         WHERE source_id = $2
        """,
        SERVING_ENDPOINT_ID,
        SOURCE_ID,
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES (
            'synthetic-serving-sibling', $1, $2,
            false, false, 'none', $3::jsonb
        )
        """,
        SERVING_ENDPOINT_ID,
        CANONICAL_API_BASE,
        json.dumps(
            {
                "provider_directory_configured_endpoint_id": SERVING_ENDPOINT_ID
            }
        ),
    )


async def _seed_serving_decoy(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            is_current, resource_count, publication_metadata_json,
            completion_proof_required_version
        ) VALUES (
            'dataset-serving-decoy', $1, 'root-serving-decoy', 'failed',
            false, 0, $2::jsonb, NULL
        )
        """,
        SERVING_ENDPOINT_ID,
        json.dumps(
            {
                "source_ids": [SOURCE_ID],
                "selected_resources": list(RESOURCE_TYPES),
            }
        ),
    )


async def _bind_expired_candidate_identity(scenario) -> None:
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET import_run_id = $1,
               status = 'failed',
               publication_metadata_json = $2::jsonb
         WHERE dataset_id = $3
        """,
        OWNER_RUN_ID,
        json.dumps(
            {
                "source_ids": [SOURCE_ID],
                "selected_resources": list(RESOURCE_TYPES),
                "verification_source_scope_hash": VALID_SOURCE_SCOPE_SHA256,
                "verification_campaign_id": VERIFICATION_CAMPAIGN_ID,
            }
        ),
        DATASET_ID,
    )


async def _seed_proof_shard(scenario) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_proof_shard (
            dataset_id, shard_id, endpoint_id, acquisition_root_run_id,
            source_ids_json, resource_count, resource_counts_json,
            first_identity_json, last_identity_json, input_sha256,
            artifact_sha256, artifact_byte_count, payload_bytes
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7::jsonb,
            $8::jsonb, $9::jsonb, $10, $11, $12, $13
        )
        """,
        DATASET_ID,
        "a" * 64,
        ENDPOINT_ID,
        ROOT_RUN_ID,
        json.dumps([SOURCE_ID]),
        len(RESOURCE_TYPES),
        json.dumps(dict.fromkeys(RESOURCE_TYPES, 1)),
        json.dumps([RESOURCE_TYPES[0], "resource-a", "b" * 64]),
        json.dumps([RESOURCE_TYPES[-1], "resource-z", "c" * 64]),
        "d" * 64,
        "e" * 64,
        5,
        b"proof",
    )


async def _seed_pagination_checkpoints(scenario) -> None:
    await scenario.connection.executemany(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_pagination_checkpoint (
            canonical_api_base, resource_type, source_scope_hash, dataset_id,
            source_ids, acquisition_root_run_id, owner_run_id, retry_of_run_id,
            start_url_hash, next_url, state, pages_processed, rows_processed,
            recent_cursor_hashes, completeness_json
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
            $9, $10, 'active', 1, 1, $11::jsonb, $12::jsonb
        )
        """,
        [
            (
                CANONICAL_API_BASE,
                resource_type,
                "1" * 64,
                DATASET_ID,
                json.dumps([SOURCE_ID]),
                ROOT_RUN_ID,
                OWNER_RUN_ID,
                "owner-prior",
                f"{ordinal:064x}",
                f"https://directory.example.test/next/{ordinal}",
                json.dumps([f"{ordinal + 10:064x}"]),
                json.dumps({"verified": False}),
            )
            for ordinal, resource_type in enumerate(RESOURCE_TYPES, start=1)
        ],
    )


async def retained_evidence_snapshot(scenario) -> dict[str, str]:
    """Return stable JSON snapshots of every retained evidence relation."""

    queries_by_name = {
        "resources": f"""
            SELECT COALESCE(jsonb_agg(to_jsonb(resource) ORDER BY
                       resource.resource_type, resource.resource_id), '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_dataset_resource AS resource
             WHERE resource.dataset_id = $1
        """,
        "proofs": f"""
            SELECT COALESCE(jsonb_agg(to_jsonb(shard) ORDER BY shard.shard_id),
                            '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_dataset_proof_shard AS shard
             WHERE shard.dataset_id = $1
        """,
        "checkpoints": f"""
            SELECT COALESCE(jsonb_agg(
                       to_jsonb(checkpoint) - ARRAY['state','updated_at','completed_at']
                       ORDER BY checkpoint.resource_type), '[]'::jsonb)::text
              FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
                   AS checkpoint
             WHERE checkpoint.dataset_id = $1
        """,
        "source": f"""
            SELECT metadata_json::jsonb::text
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = $1
        """,
    }
    return {
        snapshot_name: await scenario.connection.fetchval(
            snapshot_query,
            SOURCE_ID if snapshot_name == "source" else DATASET_ID,
        )
        for snapshot_name, snapshot_query in queries_by_name.items()
    }


async def close_abandonment_scenario(scenario, *resources) -> None:
    for resource in resources:
        with suppress(Exception):
            await resource.disconnect()
        with suppress(Exception):
            await resource.close()
    with suppress(Exception):
        await scenario.connection.execute("ROLLBACK")
    await scenario.connection.execute(
        f"DROP SCHEMA IF EXISTS {scenario.quoted_schema} CASCADE"
    )
    await scenario.connection.close()
