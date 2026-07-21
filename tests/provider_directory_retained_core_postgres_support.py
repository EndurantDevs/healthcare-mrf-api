# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL fixtures for retained-acquisition core tests."""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import AsyncIterator

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from process.provider_directory_retained_artifact_contract import (
    BULK_NDJSON,
    FHIR_BUNDLE_PAGE,
    FIXED_CATALOG,
    ORDERED_STREAMS,
    PAYLOAD,
    TERMINAL_ZERO,
    ArtifactLayoutRange,
    ProducedArtifact,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    endpoint_request_fence_digest,
    expected_range_set_digest,
    produced_layout_digest,
)
from process.provider_directory_retained_store_support import database_table


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260721160000_provider_directory_retained_artifact_acquisition.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_RETAINED_ARTIFACT_POSTGRES_DSN"
DISPOSABLE_DATABASE = re.compile(r".*test.*", re.IGNORECASE)


def digest(label: str) -> str:
    """Return a stable fixture SHA-256 identity."""

    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def campaign_item(
    label: str,
    *,
    stream_identity: str | None = None,
    sequence_ordinal: int = 0,
    item_role: str = PAYLOAD,
) -> RetainedCampaignItem:
    """Build one valid payload or terminal-zero campaign member."""

    source_item_id = digest(f"item:{label}")
    is_terminal = item_role == TERMINAL_ZERO
    return RetainedCampaignItem(
        source_item_id=source_item_id,
        source_entry_sha256=digest(f"entry:{label}"),
        artifact_kind=(FHIR_BUNDLE_PAGE if stream_identity else BULK_NDJSON),
        family="PractitionerRole",
        collection_kind="fhir_resource",
        partition_metadata={"fixture_sha256": digest(label)},
        stream_identity_sha256=stream_identity or source_item_id,
        sequence_ordinal=sequence_ordinal,
        item_role=item_role,
        source_locator=None if is_terminal else f"fixture://{source_item_id}",
        declared_byte_count=0 if is_terminal else 13,
        terminal_proof_sha256=(digest(f"terminal:{label}") if is_terminal else None),
    ).validate()


def fixed_campaign_plan(
    label: str,
    retained_items: tuple[RetainedCampaignItem, ...],
) -> RetainedCampaignPlan:
    """Build one exact fixed-catalog fixture plan."""

    endpoint_id = digest(f"endpoint:fixed:{label}")
    return RetainedCampaignPlan(
        adapter_id="retained_core_fixture_v1",
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=digest("credential:public-fixture"),
        source_census_sha256=digest(f"census:{label}"),
        census_mode=FIXED_CATALOG,
        items=retained_items,
        per_item_byte_budget=1024,
        aggregate_byte_budget=8192,
    ).validate()


def ordered_campaign_plan(
    label: str,
    stream_identity: str,
    retained_items: tuple[RetainedCampaignItem, ...] = (),
) -> RetainedCampaignPlan:
    """Build one exact unknown-length ordered-stream fixture plan."""

    endpoint_id = digest(f"endpoint:stream:{label}")
    return RetainedCampaignPlan(
        adapter_id="retained_core_fixture_v1",
        endpoint_id=endpoint_id,
        request_fence_id=endpoint_request_fence_digest(endpoint_id),
        credential_descriptor_sha256=digest("credential:public-fixture"),
        source_census_sha256=digest(f"census:{label}"),
        census_mode=ORDERED_STREAMS,
        items=retained_items,
        per_item_byte_budget=1024,
        aggregate_byte_budget=8192,
        expected_stream_identities=(stream_identity,),
    ).validate()


def registry_artifact(label: str, artifact_kind: str) -> ProducedArtifact:
    """Build one immutable single-range artifact identity without filesystem I/O."""

    artifact_bytes = f'{{"id":"{label}"}}\n'.encode("utf-8")
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    layout_range = ArtifactLayoutRange(
        range_ordinal=0,
        raw_byte_start=0,
        raw_byte_end=len(artifact_bytes),
        raw_byte_count=len(artifact_bytes),
        raw_sha256=artifact_sha256,
        record_start=0,
        record_end=1,
        record_count=1,
        canonical_sha256=artifact_sha256,
        canonical_byte_count=len(artifact_bytes),
    )
    provisional_artifact = ProducedArtifact(
        artifact_sha256=artifact_sha256,
        artifact_kind=artifact_kind,
        artifact_byte_count=len(artifact_bytes),
        artifact_record_count=1,
        artifact_path="fixture://artifact",
        layout_contract_id="retained-core-fixture-layout-v1",
        layout_contract_version=1,
        range_set_sha256="0" * 64,
        canonical_byte_count=len(artifact_bytes),
        manifest_sha256=digest(f"manifest:{label}"),
        manifest_byte_count=64,
        manifest_path="fixture://manifest",
        producer_build_id="retained-core-fixture-v1",
        ranges=(layout_range,),
    )
    return replace(
        provisional_artifact,
        range_set_sha256=expected_range_set_digest(provisional_artifact),
    )


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_retained_core_test_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


def _upgrade(sync_connection, migration_module) -> None:
    migration_module.op = Operations(MigrationContext.configure(sync_connection))
    migration_module.upgrade()


def _downgrade(sync_connection, migration_module) -> None:
    migration_module.op = Operations(MigrationContext.configure(sync_connection))
    migration_module.downgrade()


async def _connect(database_url) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=str(database_url.host),
        port=int(database_url.port or 5432),
        user=str(database_url.username),
        password=str(database_url.password or ""),
        database=str(database_url.database),
    )


@asynccontextmanager
async def retained_peer_connection() -> AsyncIterator[asyncpg.Connection]:
    """Open a second connection to the configured disposable test database."""

    database_dsn = os.environ[POSTGRES_DSN_ENV]
    connection = await _connect(make_url(database_dsn))
    try:
        yield connection
    finally:
        await connection.close()


@asynccontextmanager
async def retained_database(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """Create, migrate twice, and remove one isolated disposable schema."""

    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for disposable PostgreSQL proofs")
    database_url = make_url(database_dsn)
    if not database_url.drivername.startswith(
        "postgresql"
    ) or not DISPOSABLE_DATABASE.fullmatch(str(database_url.database or "")):
        pytest.fail(f"{POSTGRES_DSN_ENV} must target a disposable test database")
    schema_name = f"mrf_pd_core_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY_ID", "core-test-v1"
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETAINED_ARTIFACT_KEY",
        "retained-core-postgres-test-key-material",
    )
    database_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    async with database_engine.begin() as migration_connection:
        await migration_connection.exec_driver_sql(f'CREATE SCHEMA "{schema_name}"')
        await migration_connection.run_sync(
            lambda sync_connection: _upgrade(sync_connection, _load_migration())
        )
        await migration_connection.run_sync(
            lambda sync_connection: _upgrade(sync_connection, _load_migration())
        )
    database_connection = await _connect(database_url)
    try:
        yield database_connection, schema_name
    finally:
        await database_connection.close()
        async with database_engine.begin() as cleanup_connection:
            await cleanup_connection.run_sync(
                lambda sync_connection: _downgrade(
                    sync_connection,
                    _load_migration(),
                )
            )
            await cleanup_connection.exec_driver_sql(
                f'DROP SCHEMA "{schema_name}" CASCADE'
            )
        await database_engine.dispose()


async def _insert_artifact_identity(
    connection: asyncpg.Connection,
    produced_artifact: ProducedArtifact,
) -> None:
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact')} (
                artifact_sha256, artifact_byte_count, artifact_locator,
                registry_status, verified_at, created_at
            ) VALUES ($1, $2, $3, 'verified', now(), now());""",
        produced_artifact.artifact_sha256,
        produced_artifact.artifact_byte_count,
        f"fixture://artifact/{produced_artifact.artifact_sha256}",
    )


async def _insert_layout_identity(
    connection: asyncpg.Connection,
    produced_artifact: ProducedArtifact,
    layout_sha256: str,
) -> None:
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact_layout')} (
                layout_sha256, artifact_sha256, artifact_record_count,
                layout_contract_id, layout_contract_version, layout_range_count,
                range_set_sha256, canonical_byte_count, manifest_sha256,
                manifest_byte_count, manifest_locator, producer_build_id,
                registry_status, verified_at, created_at
            ) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10, $11,
                      'verified', now(), now());""",
        layout_sha256,
        produced_artifact.artifact_sha256,
        produced_artifact.artifact_record_count,
        produced_artifact.layout_contract_id,
        produced_artifact.layout_contract_version,
        produced_artifact.range_set_sha256,
        produced_artifact.canonical_byte_count,
        produced_artifact.manifest_sha256,
        produced_artifact.manifest_byte_count,
        f"fixture://manifest/{produced_artifact.manifest_sha256}",
        produced_artifact.producer_build_id,
    )


async def _insert_layout_range(
    connection: asyncpg.Connection,
    produced_artifact: ProducedArtifact,
    layout_sha256: str,
) -> None:
    layout_range = produced_artifact.ranges[0]
    await connection.execute(
        f"""INSERT INTO {database_table('provider_directory_retained_artifact_range')} (
                layout_sha256, artifact_sha256, layout_contract_version,
                layout_range_count, range_ordinal, raw_byte_start, raw_byte_end,
                raw_byte_count, raw_sha256, record_start, record_end, record_count,
                canonical_sha256, canonical_byte_count, verified_at
            ) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $9, $10, $11,
                      $12, $13, now());""",
        layout_sha256,
        produced_artifact.artifact_sha256,
        produced_artifact.layout_contract_version,
        layout_range.range_ordinal,
        layout_range.raw_byte_start,
        layout_range.raw_byte_end,
        layout_range.raw_byte_count,
        layout_range.raw_sha256,
        layout_range.record_start,
        layout_range.record_end,
        layout_range.record_count,
        layout_range.canonical_sha256,
        layout_range.canonical_byte_count,
    )


async def admit_campaign_item(
    connection: asyncpg.Connection,
    campaign_id: str,
    retained_item: RetainedCampaignItem,
    produced_artifact: ProducedArtifact,
) -> str:
    """Insert verified registry identities and bind one admitted campaign member."""

    layout_sha256 = produced_layout_digest(produced_artifact)
    await _insert_artifact_identity(connection, produced_artifact)
    await _insert_layout_identity(connection, produced_artifact, layout_sha256)
    await _insert_layout_range(connection, produced_artifact, layout_sha256)
    await connection.execute(
        f"""UPDATE {database_table('provider_directory_retained_artifact_campaign_item')}
               SET status='admitted', observed_byte_count=$3,
                   acquisition_mode='producer_verified',
                   validator_kind='producer_proof', validator_sha256=NULL,
                   immutable_identity_sha256=$4, committed_byte_count=$3,
                   downloaded_artifact_sha256=$4, artifact_sha256=$4,
                   layout_sha256=$5, admitted_at=now(), updated_at=now()
             WHERE campaign_id=$1 AND source_item_id=$2;""",
        campaign_id,
        retained_item.source_item_id,
        produced_artifact.artifact_byte_count,
        produced_artifact.artifact_sha256,
        layout_sha256,
    )
    return layout_sha256
