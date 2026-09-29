# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib.util
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.models import ProviderDirectoryInsuranceNetworkPlanEvidence
from process.provider_directory_insurance_network_identity import (
    _plan_network_refs,
    record_insurance_network_plan,
)


def _plan(*, name="A plan", network="Organization/network-1"):
    return {
        "resourceType": "InsurancePlan",
        "id": "plan-1",
        "name": name,
        "network": [{"reference": network}],
        "ownedBy": {"reference": "Organization/insurer-1"},
        "administeredBy": {"reference": "Organization/administrator-1"},
    }


def test_network_refs_include_nested_plan_networks_without_guessing():
    plan = _plan()
    plan["plan"] = [{"network": [{"reference": "Organization/network-2"}]}]
    assert _plan_network_refs(plan) == ["Organization/network-1", "Organization/network-2"]


async def _migrate(connection, upgrade, revision="20260929020000_provider_directory_insurance_network_identity"):
    path = Path(__file__).resolve().parents[1] / f"alembic/versions/{revision}.py"
    spec = importlib.util.spec_from_file_location("network_identity_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def run(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        (migration.upgrade if upgrade else migration.downgrade)()

    await connection.run_sync(run)


async def _prepare_network_tables(connection):
    """Apply the actual parent migration so network foreign keys cannot drift."""
    await _migrate(connection, True, "20260929010000_provider_directory_entity_identity")
    await _migrate(connection, True)
    await connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_organization_identity VALUES "
            "('00000000-0000-4000-8000-000000000001', now()),"
            "('00000000-0000-4000-8000-000000000002', now()),"
            "('00000000-0000-4000-8000-000000000003', now())"
        )
    )
    await connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_entity_source_binding "
            "(source_id, resource_type, resource_id, organization_id, created_at) VALUES "
            "('source-a','Organization','network-1','00000000-0000-4000-8000-000000000001',now()),"
            "('source-a','Organization','network-2','00000000-0000-4000-8000-000000000002',now()),"
            "('source-b','Organization','network-1','00000000-0000-4000-8000-000000000003',now())"
        )
    )
    await connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_entity_release_evidence "
            "(source_id, resource_type, resource_id, release_id, payload_sha256, payload_json, observed_at) VALUES "
            "('source-a','Organization','network-1','release-1',repeat('a',64),'{}',now()),"
            "('source-a','Organization','network-2','release-1',repeat('a',64),'{}',now()),"
            "('source-b','Organization','network-1','release-1',repeat('a',64),'{}',now())"
        )
    )


def _test_database_url():
    """Require an explicitly disposable test database."""
    database_url = os.getenv("CMS_NETWORK_TEST_DATABASE")
    if not database_url:
        pytest.skip("set CMS_NETWORK_TEST_DATABASE to a disposable PostgreSQL database")
    parsed = make_url(database_url)
    if parsed.host not in {"localhost", "127.0.0.1"} or not re.fullmatch(
        r"cms_network_test_[0-9a-f]{32}", parsed.database or ""
    ):
        pytest.fail("CMS_NETWORK_TEST_DATABASE must identify a UUID-owned local test database")
    if (
        any((os.getenv(name) or "mrf") != "mrf" for name in ("HLTHPRT_DB_SCHEMA", "DB_SCHEMA"))
        or ProviderDirectoryInsuranceNetworkPlanEvidence.__table__.schema != "mrf"
    ):
        pytest.fail("network PostgreSQL tests require the mrf schema")
    return database_url


@pytest.mark.parametrize(
    "database_url",
    (
        "postgresql+asyncpg://nick@127.0.0.1:5432/shared_test",
        "postgresql+asyncpg://nick@example.test:5432/cms_network_test_" + "a" * 32,
    ),
)
def test_network_postgres_proof_rejects_non_disposable_database(monkeypatch, database_url):
    monkeypatch.setenv("CMS_NETWORK_TEST_DATABASE", database_url)
    with pytest.raises(pytest.fail.Exception, match="UUID-owned local test database"):
        _test_database_url()


@pytest.mark.parametrize("setting", ("HLTHPRT_DB_SCHEMA", "DB_SCHEMA"))
def test_network_postgres_proof_rejects_other_schema(monkeypatch, setting):
    monkeypatch.setenv(
        "CMS_NETWORK_TEST_DATABASE", "postgresql+asyncpg://nick@127.0.0.1:5432/cms_network_test_" + "a" * 32
    )
    monkeypatch.setenv(setting, "other")
    with pytest.raises(pytest.fail.Exception, match="require the mrf schema"):
        _test_database_url()


@asynccontextmanager
async def _network_test_engine():
    """Prepare and remove only this test's disposable schema."""
    engine = create_async_engine(_test_database_url())
    is_schema_created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE SCHEMA mrf"))
            is_schema_created = True
            await _prepare_network_tables(connection)
        yield engine
    finally:
        if is_schema_created:
            async with engine.begin() as connection:
                await connection.execute(text("DROP SCHEMA IF EXISTS mrf CASCADE"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_network_identity_is_source_scoped_and_release_evidence_is_immutable():
    """Replays retain IDs and facts; source and release boundaries are enforced."""
    async with _network_test_engine() as engine:
        async with AsyncSession(engine) as session:
            async with session.begin():
                first_id = await record_insurance_network_plan(
                    session,
                    source_id="source-a",
                    release_id="release-1",
                    network_resource_id="network-1",
                    plan=_plan(),
                )
                assert first_id == await record_insurance_network_plan(
                    session,
                    source_id="source-a",
                    release_id="release-1",
                    network_resource_id="network-1",
                    plan=_plan(),
                )
                second_id = await record_insurance_network_plan(
                    session,
                    source_id="source-b",
                    release_id="release-1",
                    network_resource_id="network-1",
                    plan=_plan(),
                )
                assert first_id != second_id
        async with AsyncSession(engine) as session:
            evidence_rows = (
                (await session.execute(select(ProviderDirectoryInsuranceNetworkPlanEvidence))).scalars().all()
            )
            assert len(evidence_rows) == 2
            assert all(entry.owned_by_ref == "Organization/insurer-1" for entry in evidence_rows)
            assert all(entry.administered_by_ref == "Organization/administrator-1" for entry in evidence_rows)
            assert all(entry.network_refs == ["Organization/network-1"] for entry in evidence_rows)
        await _assert_network_failures(engine)


async def _assert_network_failures(engine):
    """Reject changed facts, unsupported releases, and destructive downgrade."""
    async with AsyncSession(engine) as session:
        with pytest.raises(ValueError, match="release_plan_conflict"):
            await record_insurance_network_plan(
                session,
                source_id="source-a",
                release_id="release-1",
                network_resource_id="network-1",
                plan=_plan(name="Changed"),
            )
        with pytest.raises(ValueError, match="network_ref_missing"):
            await record_insurance_network_plan(
                session,
                source_id="source-a",
                release_id="release-2",
                network_resource_id="network-1",
                plan=_plan(network="Organization/other"),
            )
    async with AsyncSession(engine) as session:
        async with session.begin():
            with pytest.raises(ValueError, match="release_evidence_missing"):
                await record_insurance_network_plan(
                    session,
                    source_id="source-a",
                    release_id="release-2",
                    network_resource_id="network-1",
                    plan=_plan(),
                )
    async with engine.begin() as connection:
        with pytest.raises(RuntimeError, match="downgrade_requires_empty_tables"):
            await _migrate(connection, False)


@pytest.mark.asyncio
async def test_plan_payload_conflict_across_networks_is_rejected_sequentially():
    """One source, release and plan cannot acquire conflicting network facts."""
    async with _network_test_engine() as engine:
        async with AsyncSession(engine) as session:
            async with session.begin():
                await record_insurance_network_plan(
                    session,
                    source_id="source-a",
                    release_id="release-1",
                    network_resource_id="network-1",
                    plan=_plan(),
                )
        async with AsyncSession(engine) as session:
            async with session.begin():
                with pytest.raises(ValueError, match="release_plan_conflict"):
                    await record_insurance_network_plan(
                        session,
                        source_id="source-a",
                        release_id="release-1",
                        network_resource_id="network-2",
                        plan=_plan(name="Conflicting", network="Organization/network-2"),
                    )
        async with AsyncSession(engine) as session:
            evidence_rows = (
                (await session.execute(select(ProviderDirectoryInsuranceNetworkPlanEvidence))).scalars().all()
            )
            assert len(evidence_rows) == 1
            assert evidence_rows[0].network_resource_id == "network-1"
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM mrf.provider_directory_insurance_network_source_binding WHERE resource_id='network-2'"
                    )
                )
                == 0
            )


@pytest.mark.asyncio
async def test_network_identity_requires_exact_source_release_evidence():
    """No ID is made for an Organization absent from this source release."""
    async with _network_test_engine() as engine:
        with pytest.raises(ValueError, match="release_evidence_missing"):
            async with AsyncSession(engine) as session:
                async with session.begin():
                    await record_insurance_network_plan(
                        session,
                        source_id="source-b",
                        release_id="release-1",
                        network_resource_id="network-2",
                        plan=_plan(network="Organization/network-2"),
                    )
        async with AsyncSession(engine) as session:
            assert (
                await session.scalar(text("SELECT count(*) FROM mrf.provider_directory_insurance_network_identity"))
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM mrf.provider_directory_insurance_network_source_binding")
                )
                == 0
            )


@pytest.mark.asyncio
async def test_concurrent_conflicting_network_plan_payloads_serialize():
    """A plan lock permits one payload even when network IDs differ."""
    async with _network_test_engine() as engine:
        start = asyncio.Event()

        async def record(network_id, plan_name):
            async with AsyncSession(engine) as session:
                async with session.begin():
                    await start.wait()
                    return await record_insurance_network_plan(
                        session,
                        source_id="source-a",
                        release_id="release-1",
                        network_resource_id=network_id,
                        plan=_plan(name=plan_name, network=f"Organization/{network_id}"),
                    )

        tasks = [
            asyncio.create_task(record("network-1", "First")),
            asyncio.create_task(record("network-2", "Second")),
        ]
        start.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        assert sum(isinstance(outcome, UUID) for outcome in outcomes) == 1
        assert sum(isinstance(outcome, ValueError) for outcome in outcomes) == 1
        assert any("release_plan_conflict" in str(outcome) for outcome in outcomes if isinstance(outcome, ValueError))
        async with AsyncSession(engine) as session:
            evidence_rows = (
                (await session.execute(select(ProviderDirectoryInsuranceNetworkPlanEvidence))).scalars().all()
            )
            assert len(evidence_rows) == 1


@pytest.mark.asyncio
async def test_downgrade_waits_for_importer_and_preserves_committed_evidence():
    """A concurrent importer commit is visible before downgrade emptiness checks."""
    async with _network_test_engine() as engine:
        async with AsyncSession(engine) as writer:
            async with writer.begin():
                await record_insurance_network_plan(
                    writer,
                    source_id="source-a",
                    release_id="release-1",
                    network_resource_id="network-1",
                    plan=_plan(),
                )

                async def downgrade():
                    async with engine.begin() as connection:
                        await _migrate(connection, False)

                downgrade_task = asyncio.create_task(downgrade())
                await asyncio.sleep(0.1)
                assert not downgrade_task.done()
        with pytest.raises(RuntimeError, match="downgrade_requires_empty_tables"):
            await asyncio.wait_for(downgrade_task, timeout=5)
        async with AsyncSession(engine) as session:
            assert (await session.execute(select(ProviderDirectoryInsuranceNetworkPlanEvidence))).scalars().one()
