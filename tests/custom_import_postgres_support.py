# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL fixtures for generic custom-import lifecycle tests."""

from __future__ import annotations

import hashlib
import os
import re
import uuid
import datetime as dt
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from db.connection import Base
from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPublicationEvent,
    CustomImportSchemaRevision,
)

POSTGRES_DSN_ENV = "HLTHPRT_CUSTOM_IMPORT_POSTGRES_DSN"
_TEST_DATABASE = re.compile(r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))", re.IGNORECASE)
_PUBLICATION_TABLES = (
    CustomImportDataset.__table__,
    CustomImportSchemaRevision.__table__,
    CustomImportDefinitionRevision.__table__,
    CustomImportCaptureBundle.__table__,
    CustomImportExecution.__table__,
    CustomImportLease.__table__,
    CustomImportGeneration.__table__,
    CustomImportCurrentGeneration.__table__,
    CustomImportPublicationEvent.__table__,
)


def digest(label: str) -> bytes:
    """Return one deterministic synthetic SHA-256 value."""

    return hashlib.sha256(label.encode("utf-8")).digest()


def lease_digest(token: str) -> bytes:
    """Return the digest persisted for a synthetic plaintext lease token."""

    return hashlib.sha256(token.encode("utf-8")).digest()


def _database_url():
    raw_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    database_url = make_url(raw_dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or not _TEST_DATABASE.search(database_name)
        or not database_url.host
        or not database_url.username
    ):
        pytest.fail(f"{POSTGRES_DSN_ENV} must identify an explicit PostgreSQL test database")
    return database_url.set(drivername="postgresql+asyncpg")


@asynccontextmanager
async def transaction_session() -> AsyncIterator[AsyncSession]:
    """Yield a caller-owned transaction whose committed savepoints are rolled back."""

    engine = create_async_engine(_database_url(), pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            outer_transaction = await connection.begin()
            relation = await connection.scalar(text("SELECT to_regclass('mrf.custom_import_dataset')"))
            if relation is None:
                pytest.fail("custom-import migration is not installed in the test database")
            session = AsyncSession(
                bind=connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            try:
                yield session
            finally:
                await session.close()
                if outer_transaction.is_active:
                    await outer_transaction.rollback()
    finally:
        await engine.dispose()


@dataclass(frozen=True)
class IsolatedPublicationCase:
    engine: AsyncEngine
    sessions: async_sessionmaker[AsyncSession]
    schema_name: str


def _quoted_publication_schema(schema_name: str) -> str:
    if not re.fullmatch(r"custom_import_publication_[0-9a-f]{16}", schema_name):
        raise RuntimeError("refusing unsafe synthetic publication schema")
    return f'"{schema_name}"'


@asynccontextmanager
async def isolated_publication_case() -> AsyncIterator[IsolatedPublicationCase]:
    """Create an exact disposable schema for committed multi-session races."""

    raw_engine = create_async_engine(_database_url(), pool_pre_ping=True)
    schema_name = f"custom_import_publication_{uuid.uuid4().hex[:16]}"
    quoted_schema = _quoted_publication_schema(schema_name)
    engine = raw_engine.execution_options(schema_translate_map={"mrf": schema_name})
    sessions = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    created_schema = False
    try:
        async with raw_engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {quoted_schema}"))
        created_schema = True
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: Base.metadata.create_all(
                    sync_connection,
                    tables=_PUBLICATION_TABLES,
                    checkfirst=False,
                )
            )
        yield IsolatedPublicationCase(
            engine=engine,
            sessions=sessions,
            schema_name=schema_name,
        )
    finally:
        if created_schema:
            async with raw_engine.begin() as connection:
                await connection.execute(text(f"DROP SCHEMA {quoted_schema} CASCADE"))
        await raw_engine.dispose()


@dataclass(frozen=True)
class PublicationGraph:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    first_execution_id: int
    first_generation_id: int
    first_generation_sha256: bytes
    second_execution_id: int
    second_generation_id: int
    no_change_execution_id: int
    no_change_token: str
    no_change_fence: int


async def seed_publication_graph(session: AsyncSession) -> PublicationGraph:
    """Insert a source-neutral two-generation graph and one running execution."""

    suffix = uuid.uuid4().hex
    dataset = CustomImportDataset(dataset_key=f"synthetic_{suffix[:24]}")
    session.add(dataset)
    await session.flush()

    schema = CustomImportSchemaRevision(
        dataset_id=dataset.dataset_id,
        revision_number=1,
        canonical_schema='{"synthetic":true}',
        schema_sha256=digest(f"schema:{suffix}"),
    )
    session.add(schema)
    await session.flush()

    definition = CustomImportDefinitionRevision(
        dataset_id=dataset.dataset_id,
        schema_revision_id=schema.schema_revision_id,
        revision_number=1,
        contract_version="custom-import/v1",
        refresh_mode="upsert",
        canonical_definition='{"synthetic":true}',
        definition_sha256=digest(f"definition:{suffix}"),
    )
    session.add(definition)
    await session.flush()

    capture_bundle = CustomImportCaptureBundle(
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition.definition_revision_id,
        schema_revision_id=schema.schema_revision_id,
        snapshot_token=f"synthetic-snapshot-{suffix}",
        snapshot_token_sha256=digest(f"snapshot:{suffix}"),
        canonical_manifest='{"synthetic":true}',
        manifest_sha256=digest(f"manifest:{suffix}"),
        stream_count=1,
    )
    session.add(capture_bundle)
    await session.flush()

    executions: list[CustomImportExecution] = []
    generations: list[CustomImportGeneration] = []
    for ordinal in (1, 2):
        execution = CustomImportExecution(
            dataset_id=dataset.dataset_id,
            definition_revision_id=definition.definition_revision_id,
            schema_revision_id=schema.schema_revision_id,
            capture_bundle_id=capture_bundle.capture_bundle_id,
            idempotency_key=f"synthetic-completed-{ordinal}-{suffix}",
            mechanism="local",
            state="completed",
        )
        session.add(execution)
        await session.flush()
        generation = CustomImportGeneration(
            dataset_id=dataset.dataset_id,
            definition_revision_id=definition.definition_revision_id,
            schema_revision_id=schema.schema_revision_id,
            execution_id=execution.execution_id,
            capture_bundle_id=capture_bundle.capture_bundle_id,
            base_generation_id=generations[-1].generation_id if generations else None,
            base_dataset_id=dataset.dataset_id if generations else None,
            source_bundle_sha256=digest(f"source-bundle:{ordinal}:{suffix}"),
            generation_sha256=digest(f"generation:{ordinal}:{suffix}"),
            root_count=ordinal,
            family_count=ordinal,
        )
        session.add(generation)
        await session.flush()
        executions.append(execution)
        generations.append(generation)

    no_change_token = f"synthetic-lease-{suffix}"
    lease_now = dt.datetime.now(dt.UTC)
    no_change = CustomImportExecution(
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition.definition_revision_id,
        schema_revision_id=schema.schema_revision_id,
        capture_bundle_id=capture_bundle.capture_bundle_id,
        idempotency_key=f"synthetic-no-change-{suffix}",
        mechanism="local",
        state="running",
    )
    session.add(no_change)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=no_change.execution_id,
            fence=1,
            token_sha256=lease_digest(no_change_token),
            heartbeat_at=lease_now,
            expires_at=lease_now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()

    return PublicationGraph(
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition.definition_revision_id,
        schema_revision_id=schema.schema_revision_id,
        first_execution_id=executions[0].execution_id,
        first_generation_id=generations[0].generation_id,
        first_generation_sha256=bytes(generations[0].generation_sha256),
        second_execution_id=executions[1].execution_id,
        second_generation_id=generations[1].generation_id,
        no_change_execution_id=no_change.execution_id,
        no_change_token=no_change_token,
        no_change_fence=1,
    )


async def execution_state(session: AsyncSession, execution_id: int) -> str:
    return (
        await session.execute(
            select(CustomImportExecution.state).where(CustomImportExecution.execution_id == execution_id)
        )
    ).scalar_one()


__all__ = (
    "POSTGRES_DSN_ENV",
    "PublicationGraph",
    "digest",
    "execution_state",
    "isolated_publication_case",
    "lease_digest",
    "seed_publication_graph",
    "transaction_session",
)
