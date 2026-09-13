# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL checks for caller-owned PTG candidate activation."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import re
import uuid

import pytest
from sqlalchemy.engine import make_url

from db.connection import db
from process.ptg_parts import source_pointers
from process.ptg_parts.ptg2_lifecycle_lock import PTG2LifecycleLockDeferred

_OPT_IN_ENV = "HLTHPRT_PTG_ARCHIVE_ACTIVATION_POSTGRES_TEST"
_DSN_ENV = "HLTHPRT_PTG_ARCHIVE_ACTIVATION_POSTGRES_DSN"
_DATABASE_PATTERN = re.compile(r"^ptg_archive_activation_test_[a-z0-9_]+$")


def _require_postgres(monkeypatch: pytest.MonkeyPatch) -> str:
    """Configure the explicitly named disposable PostgreSQL database."""

    if os.getenv(_OPT_IN_ENV) != "1":
        pytest.skip(f"set {_OPT_IN_ENV}=1 for the isolated PostgreSQL test")
    dsn = str(os.getenv(_DSN_ENV) or "").strip()
    if not dsn:
        pytest.fail(f"{_DSN_ENV} is required for the isolated PostgreSQL test")
    url = make_url(dsn)
    database_name = str(url.database or "")
    if (
        not url.drivername.startswith("postgresql")
        or not url.host
        or not url.username
        or not _DATABASE_PATTERN.fullmatch(database_name)
    ):
        pytest.fail(f"{_DSN_ENV} must identify this test's disposable database")
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    return database_name


def _quoted_identifier(name: str) -> str:
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


async def _create_schema(schema: str) -> None:
    """Create the exact minimal native activation relations."""

    statements = (
        f"CREATE SCHEMA {schema}",
        f"""CREATE FUNCTION {schema}.guard_ptg2_v4_attempt(
                snapshot_id text, internal_run_id text, allow_reconciled boolean
            ) RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$""",
        f"""CREATE TABLE {schema}.ptg2_snapshot (
                snapshot_id text PRIMARY KEY, import_run_id text NOT NULL,
                import_month date NOT NULL, status text NOT NULL,
                created_at timestamp NOT NULL, validated_at timestamp,
                published_at timestamp, previous_snapshot_id text,
                manifest jsonb NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                snapshot_key bigint PRIMARY KEY, state text NOT NULL,
                generation text NOT NULL, mapping_digest bytea
            )""",
        f"""CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
                snapshot_key bigint PRIMARY KEY, state text NOT NULL,
                map_digest bytea NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
                snapshot_id text PRIMARY KEY, snapshot_key bigint NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
                snapshot_id text PRIMARY KEY, plan_id text NOT NULL,
                plan_market_type text NOT NULL, coverage_scope_id bytea NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope (
                snapshot_id text NOT NULL, plan_id text NOT NULL,
                plan_market_type text NOT NULL,
                PRIMARY KEY (snapshot_id, plan_id, plan_market_type)
            )""",
        f"""CREATE TABLE {schema}.ptg2_current_source_snapshot (
                source_key text PRIMARY KEY, snapshot_id text NOT NULL,
                previous_snapshot_id text, import_month date NOT NULL,
                updated_at timestamp NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_current_plan_source (
                plan_source_key text PRIMARY KEY, plan_id text NOT NULL,
                plan_market_type text NOT NULL, import_month date NOT NULL,
                source_key text NOT NULL, snapshot_id text NOT NULL,
                previous_snapshot_id text, updated_at timestamp NOT NULL
            )""",
        f"""CREATE TABLE {schema}.ptg2_legacy_global_pointer_projection_queue (
                source_key text PRIMARY KEY, requested_generation bigint NOT NULL,
                applied_generation bigint NOT NULL, available_at timestamp NOT NULL,
                created_at timestamp NOT NULL, updated_at timestamp NOT NULL
            )""",
    )
    for statement in statements:
        await db.execute_ddl(statement)


async def _seed_candidate(schema: str) -> None:
    """Seed one candidate, predecessor, and two synthetic logical plans."""

    timestamp = dt.datetime(2026, 9, 13, 12, 0)
    await _seed_candidate_snapshot_rows(schema, timestamp)
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, state, generation, mapping_digest)
            VALUES (19, 'sealed', 'shared_blocks_v3', :digest)""",
        digest=b"m" * 32,
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_binding
                (snapshot_id, snapshot_key)
            VALUES ('synthetic-candidate', 19)"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_scope
                (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
            VALUES ('synthetic-candidate', 'synthetic-main', 'group', :scope)""",
        scope=b"s" * 32,
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope
                (snapshot_id, plan_id, plan_market_type)
            VALUES
                ('synthetic-candidate', 'synthetic-main', 'group'),
                ('synthetic-candidate', 'synthetic-transplant', 'group')"""
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_current_source_snapshot
                (source_key, snapshot_id, previous_snapshot_id, import_month, updated_at)
            VALUES ('synthetic-source', 'synthetic-previous', NULL,
                    :import_month, :timestamp)""",
        import_month=timestamp.date(),
        timestamp=timestamp,
    )
    await _seed_current_plan_pointers(schema, timestamp)


async def _seed_candidate_snapshot_rows(schema: str, timestamp: dt.datetime) -> None:
    """Seed the predecessor and sealed candidate snapshot rows."""

    activation = json.dumps(
        {
            "activation": {
                "contract": source_pointers.PTG2_CANDIDATE_ACTIVATION_CONTRACT,
                "state": "validated",
                "source_key": "synthetic-source",
                "expected_previous_snapshot_id": "synthetic-previous",
            }
        }
    )
    await db.status(
        f"""INSERT INTO {schema}.ptg2_snapshot
                (snapshot_id, import_run_id, import_month, status, created_at,
                 validated_at, published_at, previous_snapshot_id, manifest)
            VALUES
                ('synthetic-previous', 'synthetic-previous-run', :import_month,
                 'published', :timestamp, :timestamp, :timestamp, NULL, '{{}}'),
                ('synthetic-candidate', 'synthetic-candidate-run', :import_month,
                 'validated', :timestamp, :timestamp, NULL,
                 'synthetic-previous', CAST(:activation AS jsonb))""",
        import_month=timestamp.date(),
        timestamp=timestamp,
        activation=activation,
    )


async def _seed_current_plan_pointers(schema: str, timestamp: dt.datetime) -> None:
    """Seed source-local current pointers for two synthetic plan scopes."""

    for plan_id in ("synthetic-main", "synthetic-transplant"):
        await db.status(
            f"""INSERT INTO {schema}.ptg2_current_plan_source
                    (plan_source_key, plan_id, plan_market_type, import_month,
                     source_key, snapshot_id, previous_snapshot_id, updated_at)
                VALUES (:plan_source_key, :plan_id, 'group', :import_month,
                        'synthetic-source', 'synthetic-previous', NULL, :timestamp)""",
            plan_source_key=f"{plan_id}:synthetic-source",
            plan_id=plan_id,
            import_month=timestamp.date(),
            timestamp=timestamp,
        )


async def _pointer_state(
    schema: str,
) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    """Read candidate status plus source and plan pointers in stable order."""

    candidate_status = await db.scalar(
        f"""SELECT status FROM {schema}.ptg2_snapshot
             WHERE snapshot_id = 'synthetic-candidate'"""
    )
    source_snapshot_id = await db.scalar(
        f"""SELECT snapshot_id FROM {schema}.ptg2_current_source_snapshot
             WHERE source_key = 'synthetic-source'"""
    )
    plan_rows = await db.all(
        f"""SELECT plan_id, snapshot_id FROM {schema}.ptg2_current_plan_source
             WHERE source_key = 'synthetic-source' ORDER BY plan_id"""
    )
    return (
        str(candidate_status),
        str(source_snapshot_id),
        tuple((str(row[0]), str(row[1])) for row in plan_rows),
    )


async def _activate_and_force_rollback(schema_name: str) -> dict[str, object]:
    """Run the public bridge inside one caller transaction, then roll it back."""

    class CallerRollback(Exception):
        """Signal the caller's intentional transaction rollback."""

    result: dict[str, object] | None = None
    with pytest.raises(CallerRollback):
        async with db.transaction() as session:
            result = await source_pointers.activate_ptg2_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key="synthetic-source",
                snapshot_id="synthetic-candidate",
                expected_current_snapshot_id="synthetic-previous",
            )
            raise CallerRollback()
    assert result is not None
    return result


async def _assert_candidate_refusals(
    schema_name: str,
    schema: str,
    original_state: tuple[str, str, tuple[tuple[str, str], ...]],
) -> None:
    """Check rejected predecessor and missing-candidate calls preserve pointers."""

    async with db.transaction() as session:
        with pytest.raises(
            source_pointers.PTG2SourcePointerConflict,
            match="predecessor does not match",
        ):
            await source_pointers.activate_ptg2_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key="synthetic-source",
                snapshot_id="synthetic-candidate",
                expected_current_snapshot_id="synthetic-wrong-predecessor",
            )
    assert await _pointer_state(schema) == original_state

    async with db.transaction() as session:
        with pytest.raises(ValueError, match="candidate is unavailable"):
            await source_pointers.activate_ptg2_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key="synthetic-source",
                snapshot_id="synthetic-missing-candidate",
                expected_current_snapshot_id="synthetic-previous",
            )
    assert await _pointer_state(schema) == original_state


async def _assert_gc_lock_refusal(
    schema_name: str,
    schema: str,
    original_state: tuple[str, str, tuple[tuple[str, str], ...]],
) -> None:
    """Check an exclusive global GC lock refuses a source activation."""

    assert db.session_factory is not None
    async with db.session_factory() as session:
        async with session.begin():
            await session.execute(
                db.text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
                {"lock_name": source_pointers.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
            )
            with pytest.raises(PTG2LifecycleLockDeferred):
                await asyncio.wait_for(_activate_in_separate_transaction(schema_name), timeout=2)
    assert await _pointer_state(schema) == original_state


@pytest.mark.asyncio
async def test_caller_owned_activation_is_atomic_and_fenced(monkeypatch):
    """Prove rollback, predecessor refusal, and the shared GC fence natively."""

    _require_postgres(monkeypatch)
    schema_name = f"ptg_archive_activation_{uuid.uuid4().hex[:16]}"
    schema = _quoted_identifier(schema_name)
    await db.disconnect()
    await db.connect()
    try:
        await _create_schema(schema)
        await _seed_candidate(schema)
        monkeypatch.setattr(
            source_pointers,
            "verify_candidate_audit_attestation_in_transaction",
            lambda *_args, **_kwargs: asyncio.sleep(0, result=b"r" * 32),
        )
        monkeypatch.setattr(
            source_pointers,
            "consume_candidate_audit_attestation_in_transaction",
            lambda *_args, **_kwargs: asyncio.sleep(0),
        )

        original_state = await _pointer_state(schema)
        rollback_result = await _activate_and_force_rollback(schema_name)
        assert rollback_result["status"] == "promoted"
        assert rollback_result["plan_source_count"] == 2
        assert await _pointer_state(schema) == original_state

        await _assert_candidate_refusals(schema_name, schema, original_state)
        await _assert_gc_lock_refusal(schema_name, schema, original_state)
    finally:
        try:
            await db.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await db.disconnect()


async def _activate_in_separate_transaction(schema_name: str) -> None:
    """Attempt activation under a second transaction while GC is exclusive."""

    assert db.session_factory is not None
    async with db.session_factory() as session:
        async with session.begin():
            await source_pointers.activate_ptg2_candidate_in_transaction(
                session,
                schema_name=schema_name,
                source_key="synthetic-source",
                snapshot_id="synthetic-candidate",
                expected_current_snapshot_id="synthetic-previous",
            )
