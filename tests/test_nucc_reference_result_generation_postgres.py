# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Taxonomy authority starts empty and advances only at actual writer boundaries."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import nucc_protected_source as source
from tests.test_nucc_protected_source import closed_marker, copying_marker, ready_input, source_run
from tests.test_reference_source_generation_postgres import (
    _observe,
    _read_authority,
    _run_migration,
    _source_family,
)

_MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20261006010000_nucc_reference_result_generation.py"


def test_taxonomy_migration_extends_one_linear_history():
    scripts = ScriptDirectory(str(_MIGRATION.parent.parent))
    assert len(scripts.get_heads()) == 1
    assert scripts.get_revision("20261006010000_nucc_reference_result_generation").down_revision == (
        "20261007000000_custom_import_rejection_anti_joins"
    )


async def _install_taxonomy(connection, schema):
    await connection.execute(text(f'CREATE TABLE "{schema}".nucc_taxonomy (id bigint, marker text)'))
    await connection.execute(text(f"INSERT INTO \"{schema}\".nucc_taxonomy VALUES (1,'legacy')"))
    await _run_migration(connection, _MIGRATION, "upgrade")


@pytest.mark.asyncio
async def test_taxonomy_seed_preserves_prior_rows_and_does_not_attest_legacy_data(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        table = f'"{schema}".reference_family_result_generation'
        async with engine.begin() as connection:
            before = (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all()
            await _install_taxonomy(connection, schema)
            after = (
                await connection.execute(text(f"SELECT * FROM {table} WHERE importer_id<>'nucc' ORDER BY importer_id"))
            ).all()
            assert after == before
            seed = await _read_authority(connection, schema, "nucc")
            assert seed.local_generation == 0 and seed.serving_generation is None and seed.relation_oids is None
            assert (
                await connection.scalar(text(f"SELECT source_revision_tracked FROM {table} WHERE importer_id='nucc'"))
                is False
            )
        with pytest.raises(RuntimeError, match="tracking is unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema, "nucc")
        async with engine.begin() as connection:
            await _run_migration(connection, _MIGRATION, "downgrade")
            assert (await connection.execute(text(f"SELECT * FROM {table} ORDER BY importer_id"))).all() == before
            assert await connection.scalar(text(f'SELECT marker FROM "{schema}".nucc_taxonomy')) == "legacy"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    (
        "INSERT INTO {table} VALUES (2,'inserted')",
        "UPDATE {table} SET marker='updated'",
        "DELETE FROM {table}",
        "TRUNCATE {table}",
    ),
)
async def test_taxonomy_source_boundary_installs_statement_guards_and_preserves_evidence(monkeypatch, statement):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with engine.begin() as connection:
            await _install_taxonomy(connection, schema)
        async with sessions.begin() as session:
            first = await _observe(session, schema, "nucc", bootstrap=True)
            assert first.local_generation == 1 and len(first.relation_oids) == 1
        async with engine.begin() as connection:
            guard_type = await connection.scalar(
                text(
                    "SELECT tgtype FROM pg_catalog.pg_trigger WHERE "
                    f"tgrelid='\"{schema}\".nucc_taxonomy'::regclass AND NOT tgisinternal"
                )
            )
            assert guard_type == 60  # One AFTER STATEMENT boundary, not a per-row trigger.
            await connection.execute(text(statement.format(table=f'"{schema}".nucc_taxonomy')))
        async with sessions.begin() as session:
            updated = await _observe(session, schema, "nucc")
            assert updated.local_generation == first.local_generation + 1
            assert updated.relation_oids == first.relation_oids
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(await session.connection(), _MIGRATION, "downgrade")


@pytest.mark.asyncio
async def test_taxonomy_generation_rejects_a_multi_relation_identity(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, _sessions):
        async with engine.begin() as connection:
            await _install_taxonomy(connection, schema)
        with pytest.raises(DBAPIError) as rejected:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f'UPDATE "{schema}".reference_family_result_generation SET origin_lineage_id=local_lineage_id, '
                        "origin_generation=1,published_at=transaction_timestamp(),relation_oids=ARRAY[1,2] "
                        "WHERE importer_id='nucc'"
                    )
                )
        assert rejected.value.orig.sqlstate == "23514"


@asynccontextmanager
async def _contended_poll_row(engine, lock_row, monkeypatch, attempts, failures):
    sleeps = []
    async with engine.connect() as holder:
        await holder.begin()
        assert await holder.scalar(lock_row) == 1

        async def backoff(seconds):
            sleeps.append(seconds)
            assert seconds == source.POLL_SECONDS and len(attempts) == 1
            session, (backend_pid, _transaction_id) = attempts[0]
            assert not session.in_transaction() and failures == ["55P03"]
            assert (
                await holder.scalar(
                    text("SELECT xact_start IS NULL FROM pg_stat_activity WHERE pid=:pid"), {"pid": backend_pid}
                )
                is True
            )
            await holder.rollback()

        monkeypatch.setattr(source, "asyncio", SimpleNamespace(timeout=asyncio.timeout, sleep=backoff))
        try:
            yield sleeps
        finally:
            await holder.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("poll", ["stage", "handoff"])
async def test_source_poll_retries_native_row_contention_after_rollback(monkeypatch, poll):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        marker = copying_marker() if poll == "stage" else closed_marker()
        ready = ready_input(marker)
        stage = replace(source.NuccSourceStage.from_wire(marker), ready=ready)
        run = source_run(marker, ready=ready, status="running" if poll == "stage" else "finalizing")
        if poll == "handoff":
            run["metrics"]["nucc_handoff"] = marker["handoff"]
        lock_row = text(f'SELECT id FROM "{schema}".geo_zip_census_profile WHERE id=1 FOR UPDATE NOWAIT')
        attempts, failures = [], []

        async def locked_row(session, *_arguments):
            identity = (await session.execute(text("SELECT pg_backend_pid(),pg_current_xact_id()::text"))).one()
            attempts.append((session, identity))
            try:
                assert await session.scalar(lock_row) == 1
            except DBAPIError as error:
                failures.append(error.orig.sqlstate)
                raise

        async def stage_state(session, received, **options):
            assert received.marker == marker
            assert options == (
                {"copying": True, "allow_pending_retry": True} if poll == "stage" else {"copying": False}
            )
            if poll == "handoff":
                await locked_row(session)
            return run, marker

        monkeypatch.setattr(source, "_source_stage_state", stage_state)
        if poll == "stage":
            monkeypatch.setattr(source.control, "_source_transaction", AsyncMock(return_value={}))
            monkeypatch.setattr(source.control, "reconcile_source_publication", locked_row)
            monkeypatch.setattr(source.control, "_locked_source_run", AsyncMock(return_value=run))
        async with _contended_poll_row(engine, lock_row, monkeypatch, attempts, failures) as sleeps:
            database = SimpleNamespace(transaction=sessions.begin)
            if poll == "stage":
                context_by_field = {
                    "control_run_id": marker["run_id"],
                    "context": {
                        "_control_attempt_id": marker["attempt_id"],
                        "_control_attempt_started_at": marker["attempt_started_at"],
                    },
                }
                assert (await source.wait_nucc_source_stage(database, context_by_field)).marker == marker
            else:
                assert await source.wait_nucc_source_handoff(database, stage) == marker["handoff"]
        assert sleeps == [source.POLL_SECONDS] and failures == ["55P03"] and len(attempts) == 2
        assert attempts[0][0] is not attempts[1][0] and attempts[0][1][1] != attempts[1][1][1]
        assert all(not session.in_transaction() for session, _identity in attempts)
    try:
        async with engine.connect() as observer:
            assert await observer.scalar(text("SELECT to_regnamespace(:schema) IS NULL"), {"schema": schema}) is True
    finally:
        await engine.dispose()
