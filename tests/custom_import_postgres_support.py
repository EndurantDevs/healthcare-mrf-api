# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL fixtures for generic custom-import lifecycle tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import os
import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportFieldSlot,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPack,
    CustomImportPublicationEvent,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
    CustomImportWinner,
)
from process.custom_import.publication import (
    _capture_source_bundle_digest,
    seal_generation,
)

POSTGRES_DSN_ENV = "HLTHPRT_CUSTOM_IMPORT_POSTGRES_DSN"
_TEST_DATABASE = re.compile(r"(?:^test(?:[_-]|$)|(?:^|[_-])test(?:[_-]|$))", re.IGNORECASE)
_ROOT = Path(__file__).resolve().parents[1]
_BASE_MIGRATION_PATH = _ROOT / "alembic" / "versions" / "20260914120000_custom_import_v1_schema.py"
_FINALITY_MIGRATION_PATH = _ROOT / "alembic" / "versions" / "20260917130000_custom_import_generation_finality.py"
_DURABLE_CAPTURE_MIGRATION_PATH = (
    _ROOT / "alembic" / "versions" / "20260922000000_custom_import_durable_parquet_capture.py"
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


def _migration(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _install_custom_import_migrations(sync_connection, schema_name: str) -> None:
    """Install the exact custom-import DDL needed by the focused PostgreSQL proofs."""

    for path, module_name in (
        (_BASE_MIGRATION_PATH, "custom_import_v1_test_migration"),
        (_FINALITY_MIGRATION_PATH, "custom_import_finality_test_migration"),
        (_DURABLE_CAPTURE_MIGRATION_PATH, "custom_import_durable_capture_test_migration"),
    ):
        migration = _migration(path, module_name)
        migration._schema = lambda: schema_name
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()


@asynccontextmanager
async def transaction_session() -> AsyncIterator[AsyncSession]:
    """Yield a rollback-only session over exact disposable migration DDL."""

    async with isolated_publication_case() as case:
        async with case.engine.connect() as connection:
            outer_transaction = await connection.begin()
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
    is_schema_created = False
    try:
        async with raw_engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {quoted_schema}"))
        is_schema_created = True
        async with raw_engine.begin() as connection:
            await connection.run_sync(_install_custom_import_migrations, schema_name)
        yield IsolatedPublicationCase(
            engine=engine,
            sessions=sessions,
            schema_name=schema_name,
        )
    finally:
        if is_schema_created:
            async with raw_engine.begin() as connection:
                await connection.execute(text(f"DROP SCHEMA {quoted_schema} CASCADE"))
        await raw_engine.dispose()


@dataclass(frozen=True)
class PublicationGraph:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int
    first_execution_id: int
    first_generation_id: int
    first_materialization_sha256: bytes
    second_execution_id: int
    second_generation_id: int
    no_change_execution_id: int
    no_change_candidate_generation_id: int
    no_change_token: str
    no_change_fence: int


@dataclass(frozen=True)
class _PublicationSeed:
    dataset: CustomImportDataset
    schema_revision: CustomImportSchemaRevision
    definition_revision: CustomImportDefinitionRevision
    capture_bundle: CustomImportCaptureBundle
    source_bundle_sha256: bytes


@dataclass(frozen=True)
class GenerationAttempt:
    """One live fenced generation that has not yet been terminalized."""

    execution_id: int
    generation_id: int
    token: str
    fence: int


@dataclass(frozen=True)
class FamilyMaterial:
    """Pre-seal family prerequisites that can be attached to a generation."""

    root_record_id: int
    root_revision_id: int
    family_revision_id: int
    entity_binding_id: int
    include_root_winner: bool
    child_revision_ids: tuple[int, ...]


@dataclass(frozen=True)
class FamilyMaterialSpec:
    """Describe optional child and winner material for one synthetic family."""

    suffix: str
    child_keys: tuple[str, ...] = ()
    child_payloads: tuple[str, ...] | None = None
    parent_mismatch: bool = False
    reverse_insertion: bool = False
    include_root_winner: bool = False
    include_child_scalars: bool = True
    root_record_id: int | None = None
    entity_binding_id: int | None = None
    root_source_ordinal: int = 0
    child_source_ordinals: tuple[int, ...] | None = None


async def _seed_publication_identity(
    session: AsyncSession,
    suffix: str,
    *,
    semantic_suffix: str | None = None,
    context_collection_slot: int | None = None,
    include_selection_profile: bool = True,
) -> _PublicationSeed:
    """Seed the synthetic definition, capture, and source identity graph."""

    semantic = semantic_suffix or suffix
    dataset, schema = await _seed_identity_schema(session, suffix, semantic)
    definition = await _seed_identity_definition(
        session,
        dataset,
        schema,
        semantic,
        context_collection_slot=context_collection_slot,
        include_selection_profile=include_selection_profile,
    )
    capture_bundle = await _seed_identity_capture_bundle(
        session,
        dataset,
        schema,
        definition,
        suffix,
        semantic,
    )
    source_bundle_sha256 = await _capture_source_bundle_digest(
        session,
        capture_bundle_id=capture_bundle.capture_bundle_id,
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition.definition_revision_id,
        schema_revision_id=schema.schema_revision_id,
    )
    return _PublicationSeed(
        dataset=dataset,
        schema_revision=schema,
        definition_revision=definition,
        capture_bundle=capture_bundle,
        source_bundle_sha256=source_bundle_sha256,
    )


async def _seed_identity_schema(
    session: AsyncSession,
    suffix: str,
    semantic: str,
    *,
    canonical_schema: str = '{"synthetic":true}',
    schema_sha256: bytes | None = None,
) -> tuple[CustomImportDataset, CustomImportSchemaRevision]:
    """Create the dataset, schema revision, and fixed field definitions."""

    dataset = CustomImportDataset(dataset_key=f"synthetic_{suffix[:24]}")
    session.add(dataset)
    await session.flush()
    schema = CustomImportSchemaRevision(
        dataset_id=dataset.dataset_id,
        revision_number=1,
        canonical_schema=canonical_schema,
        schema_sha256=digest(f"schema:{semantic}") if schema_sha256 is None else schema_sha256,
    )
    session.add(schema)
    await session.flush()
    await _add_identity_fields(session, dataset, schema, semantic)
    return dataset, schema


async def _add_identity_fields(
    session: AsyncSession,
    dataset: CustomImportDataset,
    schema: CustomImportSchemaRevision,
    semantic: str,
) -> None:
    """Add the fixed slots and fields used by synthetic family fixtures."""

    session.add_all(
        (
            CustomImportFieldSlot(dataset_id=dataset.dataset_id, field_slot=1, field_id="synthetic_alpha"),
            CustomImportFieldSlot(dataset_id=dataset.dataset_id, field_slot=2, field_id="synthetic_beta"),
            CustomImportChildCollection(
                schema_revision_id=schema.schema_revision_id,
                dataset_id=dataset.dataset_id,
                collection_slot=1,
                collection_name="synthetic_children",
                canonical_key_shape='{"synthetic":true}',
                key_shape_sha256=digest(f"child-key-shape:{semantic}"),
            ),
        )
    )
    await session.flush()
    session.add_all(
        (
            CustomImportField(
                schema_revision_id=schema.schema_revision_id,
                dataset_id=dataset.dataset_id,
                field_slot=1,
                collection_slot=1,
                field_name="synthetic_alpha",
                field_type="string",
                is_nullable=False,
                projection_slot=1,
            ),
            CustomImportField(
                schema_revision_id=schema.schema_revision_id,
                dataset_id=dataset.dataset_id,
                field_slot=2,
                collection_slot=1,
                field_name="synthetic_beta",
                field_type="string",
                is_nullable=False,
                projection_slot=2,
            ),
        )
    )
    await session.flush()


async def _seed_identity_definition(
    session: AsyncSession,
    dataset: CustomImportDataset,
    schema: CustomImportSchemaRevision,
    semantic: str,
    *,
    context_collection_slot: int | None = None,
    include_selection_profile: bool = True,
) -> CustomImportDefinitionRevision:
    """Create the definition revision and optionally its synthetic profile."""

    definition = CustomImportDefinitionRevision(
        dataset_id=dataset.dataset_id,
        schema_revision_id=schema.schema_revision_id,
        revision_number=1,
        contract_version="custom-import/v1",
        refresh_mode="upsert",
        canonical_definition='{"synthetic":true}',
        definition_sha256=digest(f"definition:{semantic}"),
    )
    session.add(definition)
    await session.flush()

    if not include_selection_profile:
        return definition
    session.add(
        CustomImportSelectionProfile(
            definition_revision_id=definition.definition_revision_id,
            dataset_id=dataset.dataset_id,
            schema_revision_id=schema.schema_revision_id,
            profile_slot=1,
            profile_id="synthetic_profile",
            context_collection_slot=context_collection_slot,
            canonical_profile=(
                '{"synthetic":true}'
                if context_collection_slot is None
                else f'{{"context_collection_slot":{context_collection_slot},"synthetic":true}}'
            ),
            profile_sha256=digest(
                f"profile:{semantic}"
                if context_collection_slot is None
                else f"profile:{semantic}:context:{context_collection_slot}"
            ),
        )
    )
    await session.flush()
    return definition


async def _seed_identity_capture_bundle(
    session: AsyncSession,
    dataset: CustomImportDataset,
    schema: CustomImportSchemaRevision,
    definition: CustomImportDefinitionRevision,
    suffix: str,
    semantic: str,
) -> CustomImportCaptureBundle:
    """Create one capture bundle with deterministic root and child streams."""

    capture_bundle = CustomImportCaptureBundle(
        dataset_id=dataset.dataset_id,
        definition_revision_id=definition.definition_revision_id,
        schema_revision_id=schema.schema_revision_id,
        snapshot_token=f"synthetic-snapshot-{suffix}",
        snapshot_token_sha256=digest(f"snapshot:{suffix}"),
        canonical_manifest='{"synthetic":true}',
        manifest_sha256=digest(f"manifest:{semantic}"),
        stream_count=2,
    )
    session.add(capture_bundle)
    await session.flush()
    await _add_identity_streams(session, dataset, schema, definition)
    await _add_identity_captures(session, dataset, schema, definition, capture_bundle, semantic)
    return capture_bundle


async def _add_identity_streams(
    session: AsyncSession,
    dataset: CustomImportDataset,
    schema: CustomImportSchemaRevision,
    definition: CustomImportDefinitionRevision,
) -> None:
    """Add the root and child stream definitions for synthetic captures."""

    session.add_all(
        (
            CustomImportSourceStream(
                definition_revision_id=definition.definition_revision_id,
                dataset_id=dataset.dataset_id,
                schema_revision_id=schema.schema_revision_id,
                stream_slot=1,
                stream_id="synthetic_root",
                record_kind="root",
                collection_slot=None,
                decoder="json",
                compression="none",
                snapshot_token_selector="synthetic_snapshot",
                record_path=None,
            ),
            CustomImportSourceStream(
                definition_revision_id=definition.definition_revision_id,
                dataset_id=dataset.dataset_id,
                schema_revision_id=schema.schema_revision_id,
                stream_slot=2,
                stream_id="synthetic_child",
                record_kind="child",
                collection_slot=1,
                decoder="json",
                compression="none",
                snapshot_token_selector="synthetic_snapshot",
                record_path=None,
            ),
        )
    )
    await session.flush()


async def _add_identity_captures(
    session: AsyncSession,
    dataset: CustomImportDataset,
    schema: CustomImportSchemaRevision,
    definition: CustomImportDefinitionRevision,
    capture_bundle: CustomImportCaptureBundle,
    semantic: str,
) -> None:
    """Add deterministic capture records beneath the synthetic bundle."""

    session.add_all(
        (
            CustomImportCapture(
                capture_bundle_id=capture_bundle.capture_bundle_id,
                dataset_id=dataset.dataset_id,
                definition_revision_id=definition.definition_revision_id,
                schema_revision_id=schema.schema_revision_id,
                stream_slot=1,
                content_sha256=digest(f"capture-root-content:{semantic}"),
                byte_count=0,
                canonical_manifest='{"synthetic":true}',
                manifest_sha256=digest(f"capture-root-manifest:{semantic}"),
            ),
            CustomImportCapture(
                capture_bundle_id=capture_bundle.capture_bundle_id,
                dataset_id=dataset.dataset_id,
                definition_revision_id=definition.definition_revision_id,
                schema_revision_id=schema.schema_revision_id,
                stream_slot=2,
                content_sha256=digest(f"capture-child-content:{semantic}"),
                byte_count=0,
                canonical_manifest='{"synthetic":true}',
                manifest_sha256=digest(f"capture-child-manifest:{semantic}"),
            ),
        )
    )
    await session.flush()


async def _seed_completed_generation(
    session: AsyncSession,
    seed: _PublicationSeed,
    suffix: str,
    ordinal: int,
    prior_generation: CustomImportGeneration | None,
) -> tuple[CustomImportExecution, CustomImportGeneration]:
    token = f"synthetic-generation-lease-{ordinal}-{suffix}"
    lease_now = dt.datetime.now(dt.UTC)
    execution = CustomImportExecution(
        dataset_id=seed.dataset.dataset_id,
        definition_revision_id=seed.definition_revision.definition_revision_id,
        schema_revision_id=seed.schema_revision.schema_revision_id,
        capture_bundle_id=seed.capture_bundle.capture_bundle_id,
        idempotency_key=f"synthetic-completed-{ordinal}-{suffix}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=lease_digest(token),
            heartbeat_at=lease_now,
            expires_at=lease_now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    generation = CustomImportGeneration(
        dataset_id=seed.dataset.dataset_id,
        definition_revision_id=seed.definition_revision.definition_revision_id,
        schema_revision_id=seed.schema_revision.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=seed.capture_bundle.capture_bundle_id,
        base_generation_id=prior_generation.generation_id if prior_generation else None,
        base_dataset_id=seed.dataset.dataset_id if prior_generation else None,
        source_bundle_sha256=seed.source_bundle_sha256,
        candidate_sha256=digest(f"generation:{ordinal}:{suffix}"),
        root_count=0,
        family_count=0,
        producing_fence=1,
        producing_token_sha256=lease_digest(token),
    )
    session.add(generation)
    await session.flush()
    await seal_generation(
        session,
        dataset_id=seed.dataset.dataset_id,
        generation_id=generation.generation_id,
        lease_fence=1,
        lease_token=token,
    )
    return execution, generation


async def _seed_no_change_execution(
    session: AsyncSession,
    seed: _PublicationSeed,
    suffix: str,
) -> tuple[CustomImportExecution, CustomImportGeneration, str]:
    no_change_token = f"synthetic-lease-{suffix}"
    lease_now = dt.datetime.now(dt.UTC)
    no_change = CustomImportExecution(
        dataset_id=seed.dataset.dataset_id,
        definition_revision_id=seed.definition_revision.definition_revision_id,
        schema_revision_id=seed.schema_revision.schema_revision_id,
        capture_bundle_id=seed.capture_bundle.capture_bundle_id,
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
    candidate = CustomImportGeneration(
        dataset_id=seed.dataset.dataset_id,
        definition_revision_id=seed.definition_revision.definition_revision_id,
        schema_revision_id=seed.schema_revision.schema_revision_id,
        execution_id=no_change.execution_id,
        capture_bundle_id=seed.capture_bundle.capture_bundle_id,
        source_bundle_sha256=seed.source_bundle_sha256,
        candidate_sha256=digest(f"no-change-candidate:{suffix}"),
        root_count=0,
        family_count=0,
        producing_fence=1,
        producing_token_sha256=lease_digest(no_change_token),
    )
    session.add(candidate)
    await session.flush()
    return no_change, candidate, no_change_token


async def seed_publication_graph(
    session: AsyncSession,
    *,
    semantic_suffix: str | None = None,
    context_collection_slot: int | None = None,
) -> PublicationGraph:
    """Insert a source-neutral graph, optionally with repeatable semantic rows."""

    suffix = uuid.uuid4().hex
    seed = await _seed_publication_identity(
        session,
        suffix,
        semantic_suffix=semantic_suffix,
        context_collection_slot=context_collection_slot,
    )
    first_execution, first_generation = await _seed_completed_generation(session, seed, suffix, 1, None)
    second_execution, second_generation = await _seed_completed_generation(
        session,
        seed,
        suffix,
        2,
        first_generation,
    )
    no_change, no_change_candidate, no_change_token = await _seed_no_change_execution(session, seed, suffix)
    first_seal = await session.get(CustomImportGenerationSeal, first_generation.generation_id)
    assert first_seal is not None

    return PublicationGraph(
        dataset_id=seed.dataset.dataset_id,
        definition_revision_id=seed.definition_revision.definition_revision_id,
        schema_revision_id=seed.schema_revision.schema_revision_id,
        capture_bundle_id=seed.capture_bundle.capture_bundle_id,
        first_execution_id=first_execution.execution_id,
        first_generation_id=first_generation.generation_id,
        first_materialization_sha256=bytes(first_seal.materialization_sha256),
        second_execution_id=second_execution.execution_id,
        second_generation_id=second_generation.generation_id,
        no_change_execution_id=no_change.execution_id,
        no_change_candidate_generation_id=no_change_candidate.generation_id,
        no_change_token=no_change_token,
        no_change_fence=1,
    )


async def seed_sealed_generation(
    session: AsyncSession,
    graph: PublicationGraph,
    *,
    suffix: str,
    base_generation_id: int | None,
) -> int:
    """Add one independently fenced, sealed synthetic output to a graph."""

    token = f"synthetic-competing-lease-{suffix}"
    now = dt.datetime.now(dt.UTC)
    execution = CustomImportExecution(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        capture_bundle_id=graph.capture_bundle_id,
        idempotency_key=f"synthetic-competing-{suffix}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=lease_digest(token),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    source_generation = await session.get(CustomImportGeneration, graph.first_generation_id)
    assert source_generation is not None
    generation = CustomImportGeneration(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=graph.capture_bundle_id,
        base_generation_id=base_generation_id,
        base_dataset_id=graph.dataset_id if base_generation_id is not None else None,
        source_bundle_sha256=source_generation.source_bundle_sha256,
        candidate_sha256=digest(f"synthetic-competing-generation:{suffix}"),
        root_count=0,
        family_count=0,
        producing_fence=1,
        producing_token_sha256=lease_digest(token),
    )
    session.add(generation)
    await session.flush()
    await seal_generation(
        session,
        dataset_id=graph.dataset_id,
        generation_id=generation.generation_id,
        lease_fence=1,
        lease_token=token,
    )
    return generation.generation_id


async def seed_running_generation(
    session: AsyncSession,
    graph: PublicationGraph,
    *,
    suffix: str,
    base_generation_id: int | None,
    root_count: int = 0,
    family_count: int = 0,
    candidate_sha256: bytes | None = None,
) -> GenerationAttempt:
    """Add a live-fence candidate without sealing or terminalizing it."""

    execution, token = await _seed_running_execution(session, graph, suffix)
    source_bundle_sha256 = await _capture_source_bundle_digest(
        session,
        capture_bundle_id=graph.capture_bundle_id,
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
    )
    generation = CustomImportGeneration(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=graph.capture_bundle_id,
        base_generation_id=base_generation_id,
        base_dataset_id=graph.dataset_id if base_generation_id is not None else None,
        source_bundle_sha256=source_bundle_sha256,
        candidate_sha256=candidate_sha256 or digest(f"synthetic-running-generation-sha:{suffix}"),
        root_count=root_count,
        family_count=family_count,
        producing_fence=1,
        producing_token_sha256=lease_digest(token),
    )
    session.add(generation)
    await session.flush()
    return GenerationAttempt(
        execution_id=execution.execution_id,
        generation_id=generation.generation_id,
        token=token,
        fence=1,
    )


async def _seed_running_execution(
    session: AsyncSession,
    graph: PublicationGraph,
    suffix: str,
) -> tuple[CustomImportExecution, str]:
    """Create the active execution and lease used by a candidate generation."""

    token = f"synthetic-running-generation-{suffix}"
    now = dt.datetime.now(dt.UTC)
    execution = CustomImportExecution(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        capture_bundle_id=graph.capture_bundle_id,
        idempotency_key=f"synthetic-running-generation-{suffix}",
        mechanism="local",
        state="running",
    )
    session.add(execution)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=lease_digest(token),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    return execution, token


async def seed_family_material(
    session: AsyncSession,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    material_spec: FamilyMaterialSpec,
) -> FamilyMaterial:
    """Create one exact pre-seal root family, optionally with child scalars."""

    root_pack, child_pack = await _seed_family_packs(
        session,
        graph,
        attempt,
        material_spec.suffix,
        len(material_spec.child_keys),
    )
    root_record, entity_binding, root_revision, family = await _seed_family_root(
        session,
        graph,
        root_pack,
        material_spec,
        len(material_spec.child_keys),
    )
    resolved_child_payloads = _resolved_child_payloads(
        material_spec.child_keys,
        material_spec.child_payloads,
    )
    child_revision_ids: tuple[int, ...] = ()
    if material_spec.child_keys:
        assert child_pack is not None
        child_revision_ids = await _seed_child_revisions(
            session,
            graph,
            child_pack,
            root_record,
            family,
            material_spec,
            resolved_child_payloads,
        )
    return FamilyMaterial(
        root_record_id=root_record.root_record_id,
        root_revision_id=root_revision.root_revision_id,
        family_revision_id=family.family_revision_id,
        entity_binding_id=entity_binding.entity_binding_id,
        include_root_winner=material_spec.include_root_winner,
        child_revision_ids=child_revision_ids,
    )


async def _seed_family_packs(
    session: AsyncSession,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    suffix: str,
    child_count: int,
) -> tuple[CustomImportPack, CustomImportPack | None]:
    """Create the root pack and, when needed, a matching child pack."""

    root_pack = CustomImportPack(
        execution_id=attempt.execution_id,
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        stream_slot=1,
        pack_ordinal=0,
        capture_bundle_id=graph.capture_bundle_id,
        record_count=1,
        pack_sha256=digest(f"synthetic-root-pack:{suffix}"),
        producing_fence=attempt.fence,
        producing_token_sha256=lease_digest(attempt.token),
    )
    session.add(root_pack)
    await session.flush()
    child_pack: CustomImportPack | None = None
    if child_count:
        child_pack = CustomImportPack(
            execution_id=attempt.execution_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            stream_slot=2,
            pack_ordinal=0,
            capture_bundle_id=graph.capture_bundle_id,
            record_count=child_count,
            pack_sha256=digest(f"synthetic-child-pack:{suffix}"),
            producing_fence=attempt.fence,
            producing_token_sha256=lease_digest(attempt.token),
        )
        session.add(child_pack)
        await session.flush()
    return root_pack, child_pack


async def _seed_family_root(
    session: AsyncSession,
    graph: PublicationGraph,
    root_pack: CustomImportPack,
    material_spec: FamilyMaterialSpec,
    child_count: int,
) -> tuple[
    CustomImportRootRecord,
    CustomImportEntityBinding,
    CustomImportRootRevision,
    CustomImportFamilyRevision,
]:
    """Create a root record, its revision, binding, and family revision."""

    suffix = material_spec.suffix
    root_record = await _seed_or_reuse_root_record(session, graph, material_spec, suffix)
    entity_binding = await _seed_or_reuse_entity_binding(session, graph, material_spec, suffix)
    root_revision = CustomImportRootRevision(
        dataset_id=graph.dataset_id,
        definition_revision_id=graph.definition_revision_id,
        schema_revision_id=graph.schema_revision_id,
        root_record_id=root_record.root_record_id,
        pack_id=root_pack.pack_id,
        source_ordinal=material_spec.root_source_ordinal,
        canonical_payload=f'{{"payload":"{suffix}"}}',
        payload_sha256=digest(f"synthetic-root-payload:{suffix}"),
    )
    session.add(root_revision)
    await session.flush()
    family = CustomImportFamilyRevision(
        dataset_id=graph.dataset_id,
        schema_revision_id=graph.schema_revision_id,
        root_record_id=root_record.root_record_id,
        root_revision_id=root_revision.root_revision_id,
        entity_binding_id=entity_binding.entity_binding_id,
        family_sha256=digest(f"synthetic-family:{suffix}"),
        child_count=child_count,
        producing_execution_id=root_pack.execution_id,
        producing_fence=root_pack.producing_fence,
        producing_token_sha256=root_pack.producing_token_sha256,
    )
    session.add(family)
    await session.flush()
    return root_record, entity_binding, root_revision, family


async def _seed_or_reuse_root_record(
    session: AsyncSession,
    graph: PublicationGraph,
    material_spec: FamilyMaterialSpec,
    suffix: str,
) -> CustomImportRootRecord:
    """Return one stable root identity, optionally reusing an earlier family key."""

    if material_spec.root_record_id is not None:
        root_record = await session.get(CustomImportRootRecord, material_spec.root_record_id)
        if root_record is None or root_record.dataset_id != graph.dataset_id:
            raise ValueError("reused root record must belong to the synthetic dataset")
        return root_record
    root_record = CustomImportRootRecord(
        dataset_id=graph.dataset_id,
        key_contract_sha256=digest(f"synthetic-root-contract:{suffix}"),
        canonical_logical_key=f'{{"root":"{suffix}"}}',
        logical_key_sha256=digest(f"synthetic-root-key:{suffix}"),
    )
    session.add(root_record)
    await session.flush()
    return root_record


async def _seed_or_reuse_entity_binding(
    session: AsyncSession,
    graph: PublicationGraph,
    material_spec: FamilyMaterialSpec,
    suffix: str,
) -> CustomImportEntityBinding:
    """Return one stable entity binding, optionally shared by equivalent families."""

    if material_spec.entity_binding_id is not None:
        entity_binding = await session.get(CustomImportEntityBinding, material_spec.entity_binding_id)
        if entity_binding is None or entity_binding.dataset_id != graph.dataset_id:
            raise ValueError("reused entity binding must belong to the synthetic dataset")
        return entity_binding
    entity_binding = CustomImportEntityBinding(
        dataset_id=graph.dataset_id,
        adapter_id="synthetic",
        canonical_value=f"binding-{suffix}",
        value_sha256=digest(f"synthetic-binding:{suffix}"),
    )
    session.add(entity_binding)
    await session.flush()
    return entity_binding


def _resolved_child_payloads(
    child_keys: tuple[str, ...],
    child_payloads: tuple[str, ...] | None,
) -> tuple[str, ...]:
    """Return child payloads after enforcing their one-to-one alignment."""

    resolved_payloads = child_keys if child_payloads is None else child_payloads
    if len(resolved_payloads) != len(child_keys):
        raise ValueError("child payloads must align with child keys")
    return resolved_payloads


async def _seed_child_revisions(
    session: AsyncSession,
    graph: PublicationGraph,
    child_pack: CustomImportPack,
    root_record: CustomImportRootRecord,
    family: CustomImportFamilyRevision,
    material_spec: FamilyMaterialSpec,
    child_payloads: tuple[str, ...],
) -> tuple[int, ...]:
    """Create child revisions, family links, and scalar projections in one order."""

    child_revision_ids: list[int] = []
    child_rows = _child_rows_for_material(material_spec, child_payloads)
    insertion_rows = tuple(reversed(child_rows)) if material_spec.reverse_insertion else child_rows
    for source_ordinal, child_key, child_payload in insertion_rows:
        canonical_parent_key = root_record.canonical_logical_key
        parent_key_sha256 = root_record.logical_key_sha256
        if material_spec.parent_mismatch:
            canonical_parent_key = '{"root":"wrong-parent"}'
            parent_key_sha256 = digest(f"synthetic-wrong-parent:{child_key}")
        child_revision = CustomImportChildRevision(
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            root_record_id=root_record.root_record_id,
            collection_slot=1,
            pack_id=child_pack.pack_id,
            source_ordinal=source_ordinal,
            canonical_parent_key=canonical_parent_key,
            parent_key_sha256=parent_key_sha256,
            canonical_child_key=f'{{"child":"{child_key}"}}',
            child_key_sha256=digest(f"synthetic-child-key:{child_key}"),
            canonical_payload=f'{{"child_payload":"{child_payload}"}}',
            payload_sha256=digest(f"synthetic-child-payload:{child_payload}"),
        )
        session.add(child_revision)
        await session.flush()
        child_revision_ids.append(child_revision.child_revision_id)
        session.add(
            CustomImportFamilyChild(
                family_revision_id=family.family_revision_id,
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=root_record.root_record_id,
                collection_slot=1,
                child_revision_id=child_revision.child_revision_id,
            )
        )
        if material_spec.include_child_scalars:
            _add_child_scalars(
                session,
                graph,
                root_record,
                child_revision,
                child_payload,
                material_spec.reverse_insertion,
            )
        await session.flush()
    return tuple(child_revision_ids)


def _child_rows_for_material(
    material_spec: FamilyMaterialSpec,
    child_payloads: tuple[str, ...],
) -> tuple[tuple[int, str, str], ...]:
    """Align child provenance positions with logical keys and payloads."""

    source_ordinals = material_spec.child_source_ordinals
    if source_ordinals is None:
        source_ordinals = tuple(range(len(material_spec.child_keys)))
    if len(source_ordinals) != len(material_spec.child_keys):
        raise ValueError("child source ordinals must align with child keys")
    return tuple(
        zip(
            source_ordinals,
            material_spec.child_keys,
            child_payloads,
            strict=True,
        )
    )


def _add_child_scalars(
    session: AsyncSession,
    graph: PublicationGraph,
    root_record: CustomImportRootRecord,
    child_revision: CustomImportChildRevision,
    child_payload: str,
    reverse_insertion: bool,
) -> None:
    """Add both scalar values for one child revision in the requested order."""

    scalar_slots = ((2, 2), (1, 1)) if reverse_insertion else ((1, 1), (2, 2))
    for field_slot, projection_slot in scalar_slots:
        session.add(
            CustomImportChildScalar(
                child_revision_id=child_revision.child_revision_id,
                dataset_id=graph.dataset_id,
                schema_revision_id=graph.schema_revision_id,
                root_record_id=root_record.root_record_id,
                collection_slot=1,
                field_slot=field_slot,
                field_collection_slot=1,
                projection_slot=projection_slot,
                field_type="string",
                value_state="value",
                string_value=f"{child_payload}-{field_slot}",
            )
        )


async def attach_generation_family(
    session: AsyncSession,
    graph: PublicationGraph,
    attempt: GenerationAttempt,
    material: FamilyMaterial,
) -> None:
    """Attach exactly one prepared family to the candidate generation."""

    session.add(
        CustomImportGenerationFamily(
            generation_id=attempt.generation_id,
            dataset_id=graph.dataset_id,
            definition_revision_id=graph.definition_revision_id,
            schema_revision_id=graph.schema_revision_id,
            root_record_id=material.root_record_id,
            family_revision_id=material.family_revision_id,
        )
    )
    await session.flush()
    if material.include_root_winner:
        session.add(
            CustomImportWinner(
                generation_id=attempt.generation_id,
                dataset_id=graph.dataset_id,
                definition_revision_id=graph.definition_revision_id,
                schema_revision_id=graph.schema_revision_id,
                profile_slot=1,
                entity_binding_id=material.entity_binding_id,
                family_revision_id=material.family_revision_id,
                context_collection_slot=0,
                context_key_sha256=digest("synthetic-root-context"),
                context_child_revision_id=None,
            )
        )
        await session.flush()


async def execution_state(session: AsyncSession, execution_id: int) -> str:
    return (
        await session.execute(
            select(CustomImportExecution.state).where(CustomImportExecution.execution_id == execution_id)
        )
    ).scalar_one()


__all__ = (
    "POSTGRES_DSN_ENV",
    "FamilyMaterial",
    "GenerationAttempt",
    "PublicationGraph",
    "attach_generation_family",
    "digest",
    "execution_state",
    "isolated_publication_case",
    "lease_digest",
    "seed_publication_graph",
    "seed_family_material",
    "seed_running_generation",
    "seed_sealed_generation",
    "transaction_session",
)
