# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the production CMS callers with native COPY and real transactions."""

import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_cms_npd as cms
from process import provider_directory_entity_identity as entity_writer
from process import provider_directory_insurance_network_batch as network_writer
from process import provider_directory_resource_identity as resource_writer
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_provider_directory_insurance_network_batch_postgres import (
    _counts,
    _org,
    _plan,
    _seed,
)
from tests.test_provider_directory_insurance_network_batch_postgres import (
    network_batch_db as network_batch_db,
)

pytestmark = pytest.mark.asyncio


def _fhir(db):
    _, schema, engine = db
    calls = []

    @asynccontextmanager
    async def session():
        async with AsyncSession(engine) as current:
            try:
                yield current
                if current.in_transaction():
                    await current.commit()
            except BaseException:
                if current.in_transaction():
                    await current.rollback()
                raise

    async def all_rows(statement, **parameters):
        calls.append(parameters)
        async with session() as current:
            return (await current.execute(text(statement), parameters)).all()

    return SimpleNamespace(
        db=SimpleNamespace(session=session, all=all_rows),
        _schema=lambda: schema,
        _qt=lambda namespace, table: f'"{namespace}"."{table}"',
        _raise_if_resource_import_cancelled=AsyncMock(),
    ), calls


async def _resource_migration(engine):
    filename = "20260930010000_provider_directory_resource_identity.py"
    path = Path(__file__).parents[1] / "alembic/versions" / filename
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def upgrade(connection):
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()

    async with engine.begin() as connection:
        await connection.run_sync(upgrade)


async def test_production_organization_and_plan_batches_keep_exact_evidence(network_batch_db, monkeypatch):
    db = network_batch_db
    connection, schema, engine = db
    monkeypatch.setenv("DB_SCHEMA", schema)
    await _resource_migration(engine)
    fhir, calls = _fhir(db)
    identity_by_field = {"vector_sha256": "release-one"}
    organizations = [_org(), _org("payer", role=False)]
    plan = _plan(refs=["Organization/network-one", "Organization/missing", "https://example.test/external"])
    for kind, resources in (("Organization", organizations), ("InsurancePlan", [plan])):
        await cms._write_identity_batch(
            fhir, entity_writer, network_writer, resource_writer, kind, resources, identity_by_field
        )
    first = await connection.fetch(
        f'SELECT resource_id,network_id FROM "{schema}".provider_directory_insurance_network_source_binding'
    )
    assert [binding["resource_id"] for binding in first] == ["network-one"]
    for kind, resources in (("Organization", organizations), ("InsurancePlan", [plan])):
        await cms._write_identity_batch(
            fhir, entity_writer, network_writer, resource_writer, kind, resources, identity_by_field
        )
    assert (
        await connection.fetch(
            f'SELECT resource_id,network_id FROM "{schema}".provider_directory_insurance_network_source_binding'
        )
        == first
    )
    assert await _counts(db) == (1, 1, 1)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".provider_directory_resource_identity') == 1
    assert calls == []


async def test_last_invalid_organization_rolls_back_entity_and_network_writes(network_batch_db):
    db = network_batch_db
    fhir, _ = _fhir(db)
    with pytest.raises(ValueError, match="insurance_network_batch_invalid"):
        await cms._write_identity_batch(
            fhir,
            entity_writer,
            network_writer,
            resource_writer,
            "Organization",
            [_org(), _org("invalid_id")],
            {"vector_sha256": "release-one"},
        )
    assert await _counts(db) == (0, 0, 0)
    assert await db[0].fetchval(f'SELECT count(*) FROM "{db[1]}".provider_directory_entity_source_binding') == 0


async def test_network_backfill_bounds_bytes_and_advances_only_committed_pages(network_batch_db, monkeypatch):
    db = network_batch_db
    organizations = [{**_org(f"network-{index}"), "name": "x" * 400} for index in range(3)]
    await _seed(db, organizations, source=cms.SOURCE_ID)
    fhir, calls = _fhir(db)
    monkeypatch.setattr(cms, "BATCH_MAX_DECODED_BYTES", 600)
    identity_by_field = {"vector_sha256": "release-one"}
    await cms._backfill_network_roles(fhir, identity_by_field, {}, {})
    assert await _counts(db) == (3, 3, 0)
    assert [call["after_id"] for call in calls] == ["", "network-0", "network-1", "network-2"]
    assert all(call["batch_bytes"] == 600 and call["batch_size"] == 1000 for call in calls)
    assert fhir._raise_if_resource_import_cancelled.await_count == 3
    assert tuple(
        await db[0].fetchrow(f'SELECT draft_revision,approved_revision FROM "{db[1]}".registry_revision_control')
    ) == (0, 0)
    await cms._backfill_network_roles(fhir, identity_by_field, {}, {})
    assert await _counts(db) == (3, 3, 0)


async def test_backfill_cancellation_stops_after_one_committed_native_batch(network_batch_db, monkeypatch):
    db = network_batch_db
    await _seed(db, [_org("network-one"), _org("network-two")], source=cms.SOURCE_ID)
    fhir, calls = _fhir(db)
    monkeypatch.setattr(cms, "IDENTITY_BATCH_SIZE", 1)
    fhir._raise_if_resource_import_cancelled.side_effect = RuntimeError("cancelled")
    with pytest.raises(RuntimeError, match="cancelled"):
        await cms._backfill_network_roles(fhir, {"vector_sha256": "release-one"}, {}, {})
    assert await _counts(db) == (1, 1, 0)
    assert len(calls) == 1
