# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL checks for reviewed, source-scoped one-hop entity redirects."""

import asyncio
import importlib.util
import os
import re
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.models import (
    ProviderDirectoryEntityRedirect as Redirect,
)
from db.models import (
    ProviderDirectoryEntityRedirectDecision as Decision,
)
from db.models import (
    ProviderDirectoryEntityReleaseEvidence as Evidence,
)
from db.models import (
    ProviderDirectoryEntitySourceBinding as Binding,
)
from process.provider_directory_entity_redirect import ReviewedEntityRedirectDecision, record_reviewed_entity_redirect

_REVISION = "20260930070000_provider_directory_entity_redirect"


def _entity_id(index, kind="Organization", source="source-a"):
    return UUID(int=index + (100 if kind == "Location" else 0) + (1000 if source == "source-b" else 0))


def _decision(old=1, canonical=2, *, kind="Organization", source="source-a", action="redirect", prior=None):
    return ReviewedEntityRedirectDecision(
        decision_id=uuid4(),
        action=action,
        source_id=source,
        resource_type=kind,
        old_entity_id=_entity_id(old, kind, source),
        canonical_entity_id=_entity_id(canonical, kind, source),
        old_resource_id=f"record-{old}",
        canonical_resource_id=f"record-{canonical}",
        old_release_id="release-one",
        canonical_release_id="release-one",
        old_payload_sha256=str(old) * 64,
        canonical_payload_sha256=str(canonical) * 64,
        prior_decision_id=prior,
        review_receipt_id="synthetic-review",
        review_receipt_sha256="f" * 64,
        review_actor="synthetic-reviewer",
        reviewed_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
    )


def _database_url():
    name = os.getenv("HLTHPRT_DB_DATABASE", "")
    if not re.fullmatch(r"ptg2_v3_lifecycle_test_[a-z0-9_]{8,}", name):
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    host = os.getenv("HLTHPRT_DB_HOST", "127.0.0.1")
    if host not in {"localhost", "127.0.0.1"}:
        pytest.fail("entity redirect tests require a local disposable database")
    return URL.create(
        "postgresql+asyncpg",
        username=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        host=host,
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=name,
    )


def _migrate(connection, revision, *, downgrade=False):
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / f"{revision}.py"
    spec = importlib.util.spec_from_file_location(revision, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = Operations(MigrationContext.configure(connection))
    (module.downgrade if downgrade else module.upgrade)()


async def _seed_entities(connection, schema):
    for source_id in ("source-a", "source-b"):
        for kind, column in (("Organization", "organization_id"), ("Location", "site_id")):
            for index in range(1, 5):
                seed_by_column = {
                    "id": _entity_id(index, kind, source_id),
                    "source": source_id,
                    "kind": kind,
                    "resource": f"record-{index}",
                    "hash": str(index) * 64,
                }
                identity_table = "organization" if kind == "Organization" else "site"
                await connection.execute(
                    text(f'INSERT INTO "{schema}".provider_directory_{identity_table}_identity VALUES (:id, now())'),
                    seed_by_column,
                )
                await connection.execute(
                    text(
                        f'INSERT INTO "{schema}".provider_directory_entity_source_binding '
                        f"(source_id, resource_type, resource_id, {column}, created_at) "
                        "VALUES (:source, :kind, :resource, :id, now())"
                    ),
                    seed_by_column,
                )
                await connection.execute(
                    text(
                        f'INSERT INTO "{schema}".provider_directory_entity_release_evidence '
                        "(source_id, resource_type, resource_id, release_id, payload_sha256, payload_json, observed_at) "
                        "VALUES (:source, :kind, :resource, 'release-one', :hash, '{}', now())"
                    ),
                    seed_by_column,
                )


@asynccontextmanager
async def _test_engine():
    schema = f"entity_redirect_{uuid4().hex}"
    engine = create_async_engine(
        _database_url(), execution_options={"schema_translate_map": {Binding.__table__.schema: schema}}
    )
    is_created = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            is_created = True
            with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema}):
                for revision in ("20260929010000_provider_directory_entity_identity", _REVISION):
                    await connection.run_sync(lambda sync, revision=revision: _migrate(sync, revision))
            await _seed_entities(connection, schema)
        yield engine, schema
    finally:
        if is_created:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def _record(engine, decision):
    async with AsyncSession(engine) as session, session.begin():
        await record_reviewed_entity_redirect(session, decision)


async def _insert_pair(connection, decision, **overrides):
    await connection.execute(Decision.__table__.insert().values(**asdict(decision)))
    pointer_by_column = {
        "source_id": decision.source_id,
        "resource_type": decision.resource_type,
        "old_entity_id": decision.old_entity_id,
        "canonical_entity_id": decision.canonical_entity_id,
        "decision_id": decision.decision_id,
        "created_at": decision.reviewed_at,
    }
    await connection.execute(Redirect.__table__.insert().values(**(pointer_by_column | overrides)))


@pytest.mark.parametrize(
    "changes",
    (
        {"old_entity_id": "not-a-uuid"},
        {"resource_type": "Endpoint"},
        {"canonical_entity_id": _entity_id(1)},
        {"action": "merge"},
        {"prior_decision_id": "not-a-uuid"},
        {"action": "close"},
        {"source_id": " source-a"},
        {"review_receipt_id": ""},
        {"canonical_payload_sha256": "bad"},
        {"review_receipt_sha256": None},
        {"reviewed_at": datetime(2026, 9, 29)},
    ),
)
@pytest.mark.asyncio
async def test_invalid_review_is_rejected_before_database_use(changes):
    with pytest.raises(ValueError, match="entity_redirect_"):
        await record_reviewed_entity_redirect(None, replace(_decision(), **changes))


@pytest.mark.asyncio
async def test_review_requires_an_explicit_transaction():
    async with AsyncSession() as session:
        with pytest.raises(ValueError, match="requires_transaction"):
            await record_reviewed_entity_redirect(session, _decision())


@pytest.mark.asyncio
async def test_review_replay_closure_retarget_and_source_history():
    async with _test_engine() as (engine, _):
        first = _decision()
        await _record(engine, first)
        await _record(engine, first)
        with pytest.raises(ValueError, match="decision_replay_conflict"):
            await _record(engine, replace(first, review_actor="another-reviewer"))
        for bad, reason in (
            (_decision(1, 3), "active_conflict"),
            (_decision(2, 1), "one_hop_required"),
            (_decision(2, 3), "one_hop_required"),
            (_decision(3, 1), "one_hop_required"),
            (replace(_decision(3, 4), old_payload_sha256="0" * 64), "exact_evidence"),
            (replace(_decision(3, 4), canonical_entity_id=_entity_id(4, "Location")), "exact_evidence"),
            (replace(_decision(3, 4), canonical_entity_id=_entity_id(4, source="source-b")), "exact_evidence"),
            (replace(_decision(3, 4), canonical_release_id="absent"), "exact_evidence"),
            (_decision(action="close", prior=uuid4()), "close_target_invalid"),
        ):
            with pytest.raises(ValueError, match=reason):
                await _record(engine, bad)
        await _record(engine, _decision(3, 2))  # Several reviewed aliases may share a canonical target.
        await _record(engine, _decision(kind="Location"))
        await _record(engine, _decision(source="source-b"))
        closure = _decision(action="close", prior=first.decision_id)
        async with AsyncSession(engine) as session, session.begin():
            await record_reviewed_entity_redirect(session, closure)
            await record_reviewed_entity_redirect(session, closure)
            await record_reviewed_entity_redirect(session, first)
            replacement = _decision(1, 4)
            await record_reviewed_entity_redirect(session, replacement)
        with pytest.raises(ValueError, match="close_target_invalid"):
            await _record(engine, _decision(action="close", prior=first.decision_id))
        async with AsyncSession(engine) as session:
            pointers = (await session.execute(select(Redirect.__table__))).mappings().all()
            assert len(pointers) == 4
            assert any(pointer["decision_id"] == replacement.decision_id for pointer in pointers)
            assert not any(pointer["decision_id"] == first.decision_id for pointer in pointers)
            assert len((await session.execute(select(Decision.__table__))).all()) == 6
            assert len((await session.execute(select(Evidence.__table__))).all()) == 16
            for binding in (await session.execute(select(Binding.__table__))).mappings():
                assert (binding["organization_id"] or binding["site_id"]) == _entity_id(
                    int(binding["resource_id"][-1]), binding["resource_type"], binding["source_id"]
                )


@pytest.mark.parametrize("second", ((1, 3), (2, 1), (2, 3), (3, 1)))
@pytest.mark.asyncio
async def test_competing_redirects_and_concurrent_cycles_serialize(second):
    async with _test_engine() as (engine, _):
        outcomes = await asyncio.gather(
            _record(engine, _decision()), _record(engine, _decision(*second)), return_exceptions=True
        )
        assert sum(outcome is None for outcome in outcomes) == 1
        assert sum(isinstance(outcome, ValueError) for outcome in outcomes) == 1
        async with AsyncSession(engine) as session:
            assert len((await session.execute(select(Redirect.__table__))).all()) == 1
            assert len((await session.execute(select(Decision.__table__))).all()) == 1


@pytest.mark.asyncio
async def test_direct_sql_requires_exact_evidence_and_scope():
    async with _test_engine() as (engine, _):
        for invalid in (
            replace(_decision(), canonical_payload_sha256="0" * 64),
            replace(_decision(), old_entity_id=_entity_id(1, "Location")),
            replace(_decision(), canonical_entity_id=_entity_id(2, "Location")),
            replace(_decision(), old_entity_id=_entity_id(1, source="source-b")),
            replace(_decision(), canonical_entity_id=_entity_id(2, source="source-b")),
        ):
            with pytest.raises(DBAPIError, match="exact_evidence"):
                async with engine.begin() as connection:
                    await connection.execute(Decision.__table__.insert().values(**asdict(invalid)))
        first = _decision()
        await _record(engine, first)
        for closure in (
            _decision(kind="Location", action="close", prior=first.decision_id),
            _decision(source="source-b", action="close", prior=first.decision_id),
        ):
            with pytest.raises(DBAPIError, match="close_target_invalid"):
                async with engine.begin() as connection:
                    await connection.execute(Decision.__table__.insert().values(**asdict(closure)))
        for changed_scope in ({"resource_type": "Location"}, {"source_id": "source-b"}):
            with pytest.raises(DBAPIError, match="open_decision_required"):
                async with engine.begin() as connection:
                    await _insert_pair(connection, _decision(3, 4), **changed_scope)


@pytest.mark.asyncio
async def test_direct_sql_preserves_history_and_pointer_atomicity():
    async with _test_engine() as (engine, schema):
        with pytest.raises(DBAPIError, match="active_pointer_required"):
            async with engine.begin() as connection:
                await connection.execute(Decision.__table__.insert().values(**asdict(_decision())))
        first = _decision()
        await _record(engine, first)
        with pytest.raises(DBAPIError, match="closed_pointer_remains"):
            async with engine.begin() as connection:
                await connection.execute(
                    Decision.__table__.insert().values(**asdict(_decision(action="close", prior=first.decision_id)))
                )
        for command, reason in (
            (f"UPDATE {schema}.{Decision.__tablename__} SET review_actor='changed'", "review_immutable"),
            (f"DELETE FROM {schema}.{Decision.__tablename__}", "review_immutable"),
            (f"TRUNCATE {schema}.{Decision.__tablename__} CASCADE", "review_immutable"),
            (f"TRUNCATE {schema}.{Redirect.__tablename__}", "review_immutable"),
            (f"UPDATE {schema}.{Redirect.__tablename__} SET canonical_entity_id=old_entity_id", "active_immutable"),
            (f"DELETE FROM {schema}.{Redirect.__tablename__}", "close_decision_required"),
            (
                f"UPDATE {schema}.{Evidence.__tablename__} SET payload_sha256=repeat('f',64) "
                "WHERE source_id='source-a' AND resource_type='Organization' AND resource_id='record-1'",
                "parent_immutable",
            ),
            (
                f"UPDATE {schema}.{Binding.__tablename__} SET organization_id='{_entity_id(4)}' "
                "WHERE source_id='source-a' AND resource_type='Organization' AND resource_id='record-1'",
                "parent_immutable",
            ),
        ):
            with pytest.raises(DBAPIError, match=reason):
                async with engine.begin() as connection:
                    await connection.execute(text(command))
        with pytest.raises(DBAPIError, match="one_hop_required"):
            async with engine.begin() as connection:
                await _insert_pair(connection, _decision(2, 3))
        with pytest.raises(RuntimeError, match="requires_empty_history"):
            async with engine.begin() as connection:
                with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema}):
                    await connection.run_sync(lambda sync: _migrate(sync, _REVISION, downgrade=True))


@pytest.mark.asyncio
async def test_direct_sql_reciprocal_redirects_cannot_commit_a_cycle():
    async with _test_engine() as (engine, _):

        async def insert_pair(decision):
            async with engine.begin() as connection:
                await _insert_pair(connection, decision)

        outcomes = await asyncio.gather(insert_pair(_decision()), insert_pair(_decision(2, 1)), return_exceptions=True)
        assert sum(outcome is None for outcome in outcomes) == 1
        assert sum(isinstance(outcome, DBAPIError) for outcome in outcomes) == 1
        assert any("one_hop_required" in str(outcome) for outcome in outcomes if isinstance(outcome, DBAPIError))


@pytest.mark.parametrize("parent", (Binding, Evidence))
@pytest.mark.asyncio
async def test_review_locks_exact_parents_until_history_is_immutable(parent):
    async with _test_engine() as (engine, _):
        async with engine.connect() as mutation_connection:
            table = parent.__table__
            changes = {"organization_id": _entity_id(4)} if parent is Binding else {"payload_sha256": "f" * 64}
            statement = (
                table.update()
                .values(**changes)
                .where(
                    table.c.source_id == "source-a",
                    table.c.resource_type == "Organization",
                    table.c.resource_id == "record-1",
                )
            )
            async with AsyncSession(engine) as session, session.begin():
                await record_reviewed_entity_redirect(session, _decision())
                with pytest.raises(DBAPIError, match="lock timeout"):
                    await _mutate_parent(mutation_connection, statement)
            with pytest.raises(DBAPIError, match="parent_immutable"):
                await _mutate_parent(mutation_connection, statement)


@pytest.mark.parametrize("parent", (Binding, Evidence))
@pytest.mark.asyncio
async def test_parent_mutation_rejects_snapshot_started_before_review(parent):
    async with _test_engine() as (engine, _):
        table = parent.__table__
        changes = {"organization_id": _entity_id(4)} if parent is Binding else {"payload_sha256": "f" * 64}
        statement = (
            table.update()
            .values(**changes)
            .where(
                table.c.source_id == "source-a",
                table.c.resource_type == "Organization",
                table.c.resource_id == "record-1",
            )
        )
        async with AsyncSession(engine) as stale:
            with pytest.raises(DBAPIError, match="requires_read_committed"):
                async with stale.begin():
                    await stale.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                    await stale.execute(select(table.c.resource_id).limit(1))
                    await _record(engine, _decision())
                    await stale.execute(statement)


async def _mutate_parent(connection, statement):
    async with connection.begin():
        await connection.execute(text("SET LOCAL lock_timeout = '100ms'"))
        await connection.execute(statement)


@pytest.mark.asyncio
async def test_failure_rolls_back_both_review_and_pointer_and_rejects_stale_snapshots():
    async with _test_engine() as (engine, schema):
        with pytest.raises(RuntimeError, match="cancelled"):
            async with AsyncSession(engine) as session, session.begin():
                await record_reviewed_entity_redirect(session, _decision())
                raise RuntimeError("cancelled")
        async with AsyncSession(engine) as session, session.begin():
            assert not (await session.execute(select(Redirect.__table__))).all()
            assert not (await session.execute(select(Decision.__table__))).all()
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            with pytest.raises(ValueError, match="requires_read_committed"):
                await record_reviewed_entity_redirect(session, _decision())
        async with engine.begin() as connection:
            with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema}):
                await connection.run_sync(lambda sync: _migrate(sync, _REVISION, downgrade=True))
                await connection.run_sync(lambda sync: _migrate(sync, _REVISION))
