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
    LeaseIdentity,
    ProducedArtifact,
    RetainedCampaignItem,
    RetainedCampaignPlan,
    endpoint_request_fence_digest,
    expected_range_set_digest,
)
from process.provider_directory_retained_lease_store import acquire_item_lease
from process.provider_directory_retained_producer_store import admit_produced_artifact
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
    declared_byte_count: int | None = 13,
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
        declared_byte_count=0 if is_terminal else declared_byte_count,
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

    artifact_bytes = registry_artifact_payload(label)
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
        artifact_path=f"fixture://artifact/{artifact_sha256}",
        layout_contract_id="retained-core-fixture-layout-v1",
        layout_contract_version=1,
        range_set_sha256="0" * 64,
        canonical_byte_count=len(artifact_bytes),
        manifest_sha256=digest(f"manifest:{label}"),
        manifest_byte_count=64,
        manifest_path=f"fixture://manifest/{digest(f'manifest:{label}')}",
        producer_build_id="retained-core-fixture-v1",
        ranges=(layout_range,),
    )
    return replace(
        provisional_artifact,
        range_set_sha256=expected_range_set_digest(provisional_artifact),
    )


def registry_artifact_payload(label: str) -> bytes:
    """Return the exact bytes described by ``registry_artifact``."""

    return digest(label)[:13].encode("ascii")


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


async def admit_campaign_item(
    connection: asyncpg.Connection,
    campaign_id: str,
    retained_item: RetainedCampaignItem,
    produced_artifact: ProducedArtifact,
) -> str:
    """Insert verified registry identities and bind one admitted campaign member."""

    campaign_state = await connection.fetchrow(
        f"""SELECT lease_owner, lease_epoch
               FROM {database_table('provider_directory_retained_artifact_campaign')}
              WHERE campaign_id=$1;""",
        campaign_id,
    )
    assert campaign_state is not None and campaign_state["lease_owner"] is not None
    campaign_lease = LeaseIdentity(
        owner=str(campaign_state["lease_owner"]),
        epoch=int(campaign_state["lease_epoch"]),
    )
    item_lease = await acquire_item_lease(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        owner=f"fixture-{retained_item.source_item_id[:16]}",
    )
    return await admit_produced_artifact(
        connection,
        campaign_id=campaign_id,
        source_item_id=retained_item.source_item_id,
        campaign_lease=campaign_lease,
        item_lease=item_lease,
        produced_artifact=produced_artifact,
    )
