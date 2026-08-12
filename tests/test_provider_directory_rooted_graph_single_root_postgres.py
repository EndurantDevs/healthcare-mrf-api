# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable-PostgreSQL proof for reviewed rooted-graph authority."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process.provider_directory_rooted_graph_registration import (
    register_provider_directory_rooted_graph_source,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    derive_single_root_identity,
)
from process.provider_directory_rooted_graph_twin_store import (
    admit_rooted_graph_single_root,
    ProviderDirectoryRootedGraphTwinError,
)
from tests.formulary_fhir_twin_admission_pg_support import (
    connect,
    database_url,
    drop_schema,
    load_migration,
    quoted,
    run_migration,
)
from tests.provider_directory_rooted_graph_rotation_pg_support import (
    publish_legacy_root,
)
from tests.test_provider_directory_rooted_graph_acquisition_postgres import (
    _complete_success,
    _configure_database,
    _extend_publication_foundation,
    _load_legacy_migrations,
    MIGRATION_PATH,
)
from tests.test_provider_directory_uhc_flex_practitioner_publication_postgres import (
    _prepare_publication_schema,
)


VERSIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
SINGLE_ROOT_PATH = VERSIONS / (
    "20260812030000_provider_directory_specialized_single_root_admission.py"
)
CANONICAL_JSON_PATH = VERSIONS / "20260810110000_ptg_wave_receipt_authority.py"


async def _install_single_root_schema(context: SimpleNamespace) -> None:
    connection = await connect(context.url)
    try:
        await _extend_publication_foundation(connection, context.schema_name)
    finally:
        await connection.close()
    await context.database.connect()
    await register_provider_directory_rooted_graph_source(database=context.database)
    await run_migration(context.engine, context.base_migration, "upgrade")
    await run_migration(context.engine, context.canonical_migration, "install")
    await run_migration(context.engine, context.single_root_migration, "upgrade")


@asynccontextmanager
async def _single_root_scope(monkeypatch):
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    context = SimpleNamespace(
        url=url,
        schema_name=schema_name,
        engine=create_async_engine(url.set(drivername="postgresql+asyncpg")),
        database=_configure_database(monkeypatch, url),
        base_migration=load_migration(MIGRATION_PATH, "rooted_single_root_base"),
        single_root_migration=load_migration(SINGLE_ROOT_PATH, "rooted_single_root"),
        canonical_migration=load_migration(CANONICAL_JSON_PATH, "rooted_json"),
    )
    context.canonical_migration.install = lambda: (
        context.canonical_migration._install_receipt_verification_functions(
            schema_name
        )
    )
    try:
        await _prepare_publication_schema(
            context.engine,
            url,
            schema_name,
            quoted(schema_name),
            _load_legacy_migrations("rooted_single_root_legacy"),
        )
        await _install_single_root_schema(context)
        yield context.database
    finally:
        await context.database.disconnect()
        await drop_schema(context.engine, schema_name)
        await context.engine.dispose()


async def _prove_authority(database) -> None:
    current = await publish_legacy_root(database)
    operation_key = "7" * 64
    identity = derive_single_root_identity(current, operation_key=operation_key)
    sealed = await _complete_success(database, identity.candidate)
    admission = await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    replay = await admit_rooted_graph_single_root(
        identity.candidate.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    assert admission == replay
    assert admission.rooted_graph_sha256 == sealed.rooted_graph_sha256
    rotated = await publish_legacy_root(database, "2" * 64)
    assert rotated.dataset_id != current.dataset_id
    with pytest.raises(ProviderDirectoryRootedGraphTwinError) as stale:
        await admit_rooted_graph_single_root(
            identity.candidate.acquisition_id,
            acquisition_operation_key=operation_key,
            database=database,
        )
    assert stale.value.code == "stale"


@pytest.mark.asyncio
async def test_reviewed_single_root_binds_the_current_dataset(monkeypatch) -> None:
    """Admit and replay one current root, then reject parent rotation."""

    async with _single_root_scope(monkeypatch) as database:
        await _prove_authority(database)
