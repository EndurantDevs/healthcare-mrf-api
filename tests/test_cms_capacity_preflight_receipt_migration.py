# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact contract widening and retained-history proofs for CMS preflight receipts."""

from __future__ import annotations

import asyncio
import datetime
import importlib.util
import io
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_cms_capacity_contract as contract
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_cms_capacity_contract import _cms_receipt_row
from tests.test_provider_directory_profile_capacity_preflight_postgres import _receipt_values

_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260930140000_cms_capacity_preflight_receipt.py"
_SPEC = importlib.util.spec_from_file_location("cms_preflight_receipt_migration_test", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
migration = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(migration)
original = migration._ORIGINAL


def test_receipt_predicate_preserves_original_suffix():
    predicate = migration._cms_values_check()
    suffix = original._values_check().split("AND limits_contract_id = ", 1)[1]
    assert predicate.split("AND limits_contract_id = ", 1)[1] == suffix
    assert f"({_pair(3)}) OR ({_pair(4)})" in predicate
    assert migration.down_revision == "20260930130000_cms_doctors_prepared_seal"


def _pair(version):
    return (
        f"contract_id = 'healthporta.provider-directory-profile-capacity-preflight.v{version}' "
        f"AND request_contract_id = 'healthporta.provider-directory-profile-capacity-preflight-request.v{version}'"
    )


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_migration_offline_orders_guards(monkeypatch, direction):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "receipt_test")
    monkeypatch.setenv("DB_SCHEMA", "receipt_test")
    output = io.StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        getattr(migration, direction)()
    statements = output.getvalue()
    assert statements.index("lock_timeout='5s'") < statements.index("LOCK TABLE")
    assert statements.index("LOCK TABLE") < statements.index("ADD CONSTRAINT")
    assert statements.count("NOT VALID") == 3
    assert statements.count("VALIDATE CONSTRAINT") == 1
    assert "conbin IS DISTINCT FROM probe_row.conbin" in statements
    assert "NOT live_row.convalidated" in statements
    assert "DROP TABLE" not in statements and "DELETE FROM" not in statements
    if direction == "downgrade":
        guard = statements.index("cms_capacity_preflight_v4_history_requires_retention")
        assert guard < statements.index("VALIDATE CONSTRAINT")


def test_schema_mismatch_fails_before_lock(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "receipt_test")
    monkeypatch.setenv("DB_SCHEMA", "other_test")
    with pytest.raises(RuntimeError, match="must match"):
        migration.upgrade()


def _apply(connection, schema, direction):
    with Operations.context(MigrationContext.configure(connection)):
        if direction == "bootstrap":
            original._create_table(schema)
            original._create_guards(schema)
        else:
            getattr(migration, direction)()


@asynccontextmanager
async def _ledger(monkeypatch):
    """Create only a task-owned schema within the strict UUID test database."""
    engine = create_async_engine(_database_url())
    schema = "cms_receipt_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
            await connection.run_sync(_apply, schema, "bootstrap")
        yield engine, schema
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()


async def _migrate(engine, schema, direction):
    async with engine.begin() as connection:
        await connection.run_sync(_apply, schema, direction)


def _row(label, version=3):
    issued_at = datetime.datetime.now(datetime.timezone.utc)
    row = _receipt_values(label, issued_at=issued_at, expires_at=issued_at + datetime.timedelta(hours=1))
    row["contract_id"] = f"healthporta.provider-directory-profile-capacity-preflight.v{version}"
    row["request_contract_id"] = f"healthporta.provider-directory-profile-capacity-preflight-request.v{version}"
    return row


async def _insert(engine, schema, row):
    values = ", ".join("CAST(:receipt_json AS jsonb)" if field == "receipt_json" else ":" + field for field in row)
    statement = f'INSERT INTO "{schema}"."{original._TABLE}" ({", ".join(row)}) VALUES ({values})'
    async with engine.begin() as connection:
        await connection.execute(sa.text(statement), row)


async def _snapshot(engine, schema):
    """Capture full retained rows, key/index identities, and immutable guard bytes."""
    table = original._qt(schema, original._TABLE)
    async with engine.connect() as connection:
        history = (
            (
                await connection.execute(
                    sa.text(f"SELECT to_jsonb(receipt) FROM {table} receipt ORDER BY receipt_sha256")
                )
            )
            .scalars()
            .all()
        )
        keys = (
            await connection.execute(
                sa.text("""SELECT conname,contype,conkey,conindid,convalidated
            FROM pg_constraint WHERE conrelid=to_regclass(:table) AND contype IN ('p','u') ORDER BY conname"""),
                {"table": table},
            )
        ).all()
        guards = (
            await connection.execute(
                sa.text("""SELECT trigger.oid,trigger.tgname,trigger.tgenabled::text,
            pg_get_triggerdef(trigger.oid),pg_get_functiondef(trigger.tgfoid)
            FROM pg_trigger trigger WHERE tgrelid=to_regclass(:table) AND NOT tgisinternal ORDER BY tgname"""),
                {"table": table},
            )
        ).all()
        indexes = (
            await connection.execute(
                sa.text("""SELECT indexrelid,pg_get_indexdef(indexrelid)
            FROM pg_index WHERE indrelid=to_regclass(:table) ORDER BY indexrelid"""),
                {"table": table},
            )
        ).all()
        return history, keys, guards, indexes


async def _check_definition(engine, schema):
    async with engine.connect() as connection:
        return (
            await connection.execute(
                sa.text("""SELECT pg_get_constraintdef(oid),convalidated
            FROM pg_constraint WHERE conrelid=to_regclass(:table) AND conname=:name"""),
                {"table": original._qt(schema, original._TABLE), "name": original._VALUES_CONSTRAINT},
            )
        ).one()


def test_native_upgrade_retains_ledger(monkeypatch):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _insert(engine, schema, _row("incumbent"))
            before = await _snapshot(engine, schema)
            await _migrate(engine, schema, "upgrade")
            assert await _snapshot(engine, schema) == before
            await _insert(engine, schema, _row("candidate", 4))
            after = await _snapshot(engine, schema)
            assert len(after[0]) == 2
            assert after[1:] == before[1:]
            assert len(after[1]) == 3 and len(after[2]) == 3 and len(after[3]) == 4
            assert all(guard[2] == "A" for guard in after[2])
            with pytest.raises(DBAPIError, match="history_immutable"):
                async with engine.begin() as connection:
                    await connection.execute(sa.text(f"DELETE FROM {original._qt(schema, original._TABLE)}"))
            assert await _snapshot(engine, schema) == after

    asyncio.run(exercise())


@pytest.mark.parametrize("receipt_version,request_version", [(3, 4), (4, 3), (5, 5)])
def test_native_rejects_crossed_contracts(monkeypatch, receipt_version, request_version):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _migrate(engine, schema, "upgrade")
            row = _row("crossed", receipt_version)
            row["request_contract_id"] = (
                f"healthporta.provider-directory-profile-capacity-preflight-request.v{request_version}"
            )
            with pytest.raises(DBAPIError, match=original._VALUES_CONSTRAINT):
                await _insert(engine, schema, row)
            assert (await _snapshot(engine, schema))[0] == []

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "field,value",
    [
        ("limits_contract_id", "unsupported"),
        ("receipt_sha256", "invalid"),
        ("materialization_mode", "full_swap"),
        ("consumed_run_id", "run_" + "a" * 32),
    ],
)
def test_native_keeps_original_predicates(monkeypatch, field, value):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _migrate(engine, schema, "upgrade")
            row = _row("invalid-suffix", 4)
            row[field] = value
            with pytest.raises(DBAPIError, match=original._VALUES_CONSTRAINT):
                await _insert(engine, schema, row)

    asyncio.run(exercise())


@pytest.mark.parametrize("drift", ["predicate", "unvalidated"])
def test_native_rejects_constraint_drift(monkeypatch, drift):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _insert(engine, schema, _row("retained"))
            table = original._qt(schema, original._TABLE)
            condition = "TRUE" if drift == "predicate" else original._values_check()
            validation = "" if drift == "predicate" else " NOT VALID"
            async with engine.begin() as connection:
                await connection.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT {original._VALUES_CONSTRAINT}"))
                await connection.execute(
                    sa.text(
                        f"ALTER TABLE {table} ADD CONSTRAINT {original._VALUES_CONSTRAINT} CHECK ({condition}){validation}"
                    )
                )
            before = await _snapshot(engine, schema)
            check = await _check_definition(engine, schema)
            with pytest.raises(DBAPIError, match="cms_capacity_preflight_constraint_drift"):
                await _migrate(engine, schema, "upgrade")
            assert await _snapshot(engine, schema) == before
            assert await _check_definition(engine, schema) == check

    asyncio.run(exercise())


def test_native_downgrade_retains_profile_history(monkeypatch):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _insert(engine, schema, _row("retained-profile"))
            before = await _snapshot(engine, schema)
            await _migrate(engine, schema, "upgrade")
            await _migrate(engine, schema, "downgrade")
            assert await _snapshot(engine, schema) == before
            with pytest.raises(DBAPIError, match=original._VALUES_CONSTRAINT):
                await _insert(engine, schema, _row("cms-not-allowed", 4))
            await _migrate(engine, schema, "upgrade")
            await _insert(engine, schema, _row("cms-allowed", 4))

    asyncio.run(exercise())


@pytest.mark.parametrize("consumed", [False, True])
def test_native_downgrade_preserves_cms_history(monkeypatch, consumed):
    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _migrate(engine, schema, "upgrade")
            row = _row("retained-cms", 4)
            await _insert(engine, schema, row)
            if consumed:
                async with engine.begin() as connection:
                    await connection.execute(
                        sa.text(f"""UPDATE {original._qt(schema, original._TABLE)}
                        SET consumed_at=:issued_at,consumed_run_id=:run,consumed_attestation_id=:attestation
                        WHERE receipt_sha256=:receipt_sha256"""),
                        {**row, "run": "run_" + "a" * 32, "attestation": "a" * 64},
                    )
            before = await _snapshot(engine, schema)
            check = await _check_definition(engine, schema)
            with pytest.raises(DBAPIError, match="cms_capacity_preflight_v4_history_requires_retention"):
                await _migrate(engine, schema, "downgrade")
            assert await _snapshot(engine, schema) == before
            assert await _check_definition(engine, schema) == check

    asyncio.run(exercise())


def test_native_signed_receipt_roundtrip(monkeypatch):
    """Read actual PostgreSQL storage types against the independently verified signature."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    request, receipt, lease, row = _cms_receipt_row()
    assert row == contract.capacity_preflight_receipt_row_values(request, receipt, issued_at=row["issued_at"])

    async def exercise():
        async with _ledger(monkeypatch) as (engine, schema):
            await _migrate(engine, schema, "upgrade")
            await _insert(engine, schema, row)
            async with engine.connect() as connection:
                result = await connection.execute(
                    sa.text(f"SELECT * FROM {original._qt(schema, original._TABLE)} WHERE receipt_sha256=:digest"),
                    {"digest": receipt["receipt_sha256"]},
                )
                stored = result.mappings().one()
            assert isinstance(stored["receipt_json"], dict)
            for field in ("issued_at", "expires_at", "created_at"):
                assert isinstance(stored[field], datetime.datetime)
                assert stored[field].tzinfo is not None
                assert stored[field] == row[field]
            assert contract.read_capacity_preflight_receipt(stored, lease) == receipt
            assert stored["receipt_json"] == lease.signing_preflight_guard["healthcare_receipt"]

    asyncio.run(exercise())
