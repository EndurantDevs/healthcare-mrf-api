# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for reviewed CMS Organization to payer links."""

import asyncio
import importlib.util
import os
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.models import MRFPayer, ProviderDirectoryMRFPayerBinding, ProviderDirectoryMRFPayerReviewDecision
from process.provider_directory_mrf_payer_binding import ReviewedCMSPayerDecision, record_reviewed_cms_payer_decision


async def _migrate(connection, revision):
    path = Path(__file__).resolve().parents[1] / f"alembic/versions/{revision}.py"
    spec = importlib.util.spec_from_file_location("payer_binding_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    def run(sync_connection):
        migration.op = Operations(MigrationContext.configure(sync_connection))
        migration.upgrade()

    await connection.run_sync(run)


def _test_database_url():
    if any(os.getenv(name) not in (None, "mrf") for name in ("HLTHPRT_DB_SCHEMA", "DB_SCHEMA")):
        pytest.fail("CMS payer binding tests require the mrf schema")
    if MRFPayer.__table__.schema != "mrf":
        pytest.fail("CMS payer binding models must target the mrf schema")
    database_url = os.getenv("CMS_PAYER_BINDING_TEST_DATABASE")
    if not database_url:
        pytest.skip("set CMS_PAYER_BINDING_TEST_DATABASE to a disposable PostgreSQL database")
    parsed = make_url(database_url)
    if parsed.host not in {"localhost", "127.0.0.1"} or not re.fullmatch(
        r"cms_payer_binding_test_[0-9a-f]{32}", parsed.database or ""
    ):
        pytest.fail("CMS_PAYER_BINDING_TEST_DATABASE must identify a UUID-owned local test database")
    return database_url


@asynccontextmanager
async def _test_engine():
    engine = create_async_engine(_test_database_url())
    is_schema_created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE SCHEMA mrf"))
            is_schema_created = True
            await connection.run_sync(lambda sync_connection: MRFPayer.__table__.create(sync_connection))
            for revision in (
                "20260929010000_provider_directory_entity_identity",
                "20260929020000_provider_directory_insurance_network_identity",
                "20260929030000_provider_directory_mrf_payer_binding",
            ):
                await _migrate(connection, revision)
            await connection.execute(
                text(
                    "INSERT INTO mrf.mrf_payer (payer_id, canonical_name, lifecycle) VALUES "
                    "('mrfpayer_a', 'Synthetic payer A', 'active'),"
                    "('mrfpayer_b', 'Synthetic payer B', 'active')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO mrf.provider_directory_organization_identity VALUES "
                    "('00000000-0000-4000-8000-000000000001', now()),"
                    "('00000000-0000-4000-8000-000000000002', now())"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO mrf.provider_directory_entity_source_binding "
                    "(source_id, resource_type, resource_id, organization_id, created_at) VALUES "
                    "('cms-npd','Organization','org-1','00000000-0000-4000-8000-000000000001',now()),"
                    "('source-b','Organization','org-1','00000000-0000-4000-8000-000000000002',now())"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO mrf.provider_directory_entity_release_evidence "
                    "(source_id, resource_type, resource_id, release_id, payload_sha256, payload_json, observed_at) VALUES "
                    "('cms-npd','Organization','org-1','release-1',repeat('a',64),'{}',now()),"
                    "('cms-npd','Organization','org-1','release-2',repeat('b',64),'{}',now()),"
                    "('source-b','Organization','org-1','release-1',repeat('c',64),'{}',now())"
                )
            )
        yield engine
    finally:
        if is_schema_created:
            async with engine.begin() as connection:
                await connection.execute(text("DROP SCHEMA mrf CASCADE"))
        await engine.dispose()


def _decision(decision_id, *, action="bind", payer_id="mrfpayer_a", release_id="release-1", prior=None):
    return ReviewedCMSPayerDecision(
        decision_id=decision_id,
        action=action,
        source_id="cms-npd",
        resource_id="org-1",
        release_id=release_id,
        source_payload_sha256=("b" if release_id == "release-2" else "a") * 64,
        payer_id=payer_id,
        prior_decision_id=prior,
        review_receipt_id=f"synthetic-review-{decision_id}",
        review_receipt_sha256="d" * 64,
        review_actor="synthetic-reviewer",
        reviewed_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )


async def _record(engine, review_decision):
    async with AsyncSession(engine) as session:
        async with session.begin():
            await record_reviewed_cms_payer_decision(session, review_decision)


async def _reject(engine, review_decision, error_fragment):
    with pytest.raises(ValueError, match=error_fragment):
        await _record(engine, review_decision)


async def _assert_immutable_review(engine, first, closing):
    with pytest.raises(DBAPIError, match="review_immutable"):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE mrf.provider_directory_mrf_payer_review_decision "
                    "SET review_actor = 'changed' WHERE decision_id = :decision_id"
                ),
                {"decision_id": first},
            )
    with pytest.raises(DBAPIError, match="review_immutable"):
        async with engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM mrf.provider_directory_mrf_payer_review_decision WHERE decision_id = :decision_id"),
                {"decision_id": closing},
            )
    for table in ("provider_directory_mrf_payer_review_decision", "provider_directory_mrf_payer_binding"):
        with pytest.raises(DBAPIError, match="review_immutable"):
            async with engine.begin() as connection:
                await connection.execute(text(f"TRUNCATE mrf.{table} CASCADE"))


async def _assert_direct_delete_rejected(engine):
    with pytest.raises(DBAPIError, match="binding_requires_close_decision"):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "DELETE FROM mrf.provider_directory_mrf_payer_binding WHERE source_id='cms-npd' AND resource_id='org-1'"
                )
            )


async def _assert_binding_guards(engine, first, closing):
    with pytest.raises(DBAPIError, match="binding_immutable"):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE mrf.provider_directory_mrf_payer_binding SET payer_id='mrfpayer_a' "
                    "WHERE source_id='cms-npd' AND resource_id='org-1'"
                )
            )
    for decision_id in (first, closing):
        with pytest.raises(DBAPIError, match="binding_requires_bind_decision"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO mrf.provider_directory_mrf_payer_binding "
                        "(source_id, resource_type, resource_id, payer_id, binding_decision_id, created_at) "
                        "VALUES ('cms-npd','Organization','org-1','mrfpayer_a',:decision_id,now())"
                    ),
                    {"decision_id": decision_id},
                )


@pytest.mark.asyncio
async def test_reviewed_payer_binding_replay_conflict_closure_and_source_isolation():
    """Only a reviewed exact CMS Organization can point to one existing payer."""
    first = UUID("00000000-0000-4000-8000-000000000011")
    closing = UUID("00000000-0000-4000-8000-000000000012")
    second = UUID("00000000-0000-4000-8000-000000000013")
    async with _test_engine() as engine:
        await _record(engine, _decision(first))
        await _record(engine, _decision(first))
        await _assert_direct_delete_rejected(engine)
        await _reject(engine, _decision(second, payer_id="mrfpayer_b"), "active_binding_conflict")
        await _reject(engine, _decision(first, payer_id="mrfpayer_b"), "decision_replay_conflict")
        await _reject(engine, replace(_decision(second), source_id="source-b"), "source_invalid")
        await _reject(engine, _decision(second, release_id="release-absent"), "release_evidence_missing")
        await _reject(engine, replace(_decision(second), source_payload_sha256="0" * 64), "source_payload_conflict")
        await _reject(engine, _decision(second, payer_id="payer-absent"), "identity_missing")
        await _reject(engine, _decision(closing, action="close", prior=second), "close_target_invalid")
        async with AsyncSession(engine) as session:
            async with session.begin():
                await record_reviewed_cms_payer_decision(
                    session, _decision(closing, action="close", release_id="release-2", prior=first)
                )
                await record_reviewed_cms_payer_decision(
                    session, _decision(closing, action="close", release_id="release-2", prior=first)
                )
                await record_reviewed_cms_payer_decision(session, _decision(first))
                await record_reviewed_cms_payer_decision(
                    session, _decision(second, payer_id="mrfpayer_b", release_id="release-2")
                )
        async with AsyncSession(engine) as session:
            bindings = (await session.execute(select(ProviderDirectoryMRFPayerBinding))).scalars().all()
            decisions = (await session.execute(select(ProviderDirectoryMRFPayerReviewDecision))).scalars().all()
            assert len(bindings) == 1
            assert (bindings[0].source_id, bindings[0].payer_id, bindings[0].binding_decision_id) == (
                "cms-npd",
                "mrfpayer_b",
                second,
            )
            assert {(review_entry.decision_id, review_entry.action) for review_entry in decisions} == {
                (first, "bind"),
                (closing, "close"),
                (second, "bind"),
            }
        await _assert_immutable_review(engine, first, closing)
        await _assert_binding_guards(engine, first, closing)


@pytest.mark.asyncio
async def test_concurrent_reviews_cannot_bind_two_payers_to_one_organization():
    """The source/Organization lock serializes competing reviewed decisions."""
    async with _test_engine() as engine:
        outcomes = await asyncio.gather(
            _record(engine, _decision(UUID("00000000-0000-4000-8000-000000000021"))),
            _record(engine, _decision(UUID("00000000-0000-4000-8000-000000000022"), payer_id="mrfpayer_b")),
            return_exceptions=True,
        )
        assert sum(outcome is None for outcome in outcomes) == 1
        failures = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
        assert len(failures) == 1
        assert "active_binding_conflict" in str(failures[0])
        async with AsyncSession(engine) as session:
            assert len((await session.execute(select(ProviderDirectoryMRFPayerBinding))).scalars().all()) == 1
            assert len((await session.execute(select(ProviderDirectoryMRFPayerReviewDecision))).scalars().all()) == 1
