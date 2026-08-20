"""Migration regressions for PTG JSON-null guard parity."""

from __future__ import annotations

import pytest

from tests.ptg_wave_v13_post_ready_guard_support import (
    JSON_NULL_GUARD_MIGRATION_PATH,
    MIGRATION_PATH,
    ROOT,
    add_v13_head_prerequisites,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _insert_and_assert_terminal_receipt,
    _install_receipt_migration,
    _prepare_ordinary_terminal_db_fixture,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
    asyncpg,
)


ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH = ROOT / "alembic" / (
    "versions/20260820020000_ptg_ordinary_terminal_json_null_guard.py"
)


def test_v13_json_null_patch_replaces_only_the_exact_predicate(monkeypatch):
    migration = _load_migration(JSON_NULL_GUARD_MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "v13_json_null_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    sql = " ".join(statements[0].split())
    assert migration.down_revision == (
        "202608170001_ptg_v13_post_ready_failure_guard"
    )
    assert '"v13_json_null_test".' in sql
    assert "AND admitted.error IS NULL" in sql
    assert "admitted.error::jsonb = ''null''::jsonb" in sql
    assert "pg_get_functiondef" in sql
    assert "PTG_IMPORT_WAVE_V13_JSON_NULL_PATCH_PRECONDITION_FAILED" in sql

    statements.clear()
    migration.downgrade()
    downgrade_sql = " ".join(statements[0].split())
    assert "pg_get_functiondef" in downgrade_sql
    assert "admitted.error::jsonb = ''null''::jsonb" in downgrade_sql
    assert "PTG_IMPORT_WAVE_V13_JSON_NULL_PATCH_PRECONDITION_FAILED" in (
        downgrade_sql
    )


def test_ordinary_terminal_json_null_patch_is_exact(monkeypatch):
    migration = _load_migration(
        ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH
    )
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ordinary_terminal_json_null_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    sql = " ".join(statements[0].split())
    assert migration.down_revision == (
        "20260820010000_prescription_autocomplete_trigram_index"
    )
    assert '"ordinary_terminal_json_null_test".' in sql
    assert "OR ordinary_run.error IS NOT NULL" in sql
    assert (
        "ordinary_run.error::jsonb IS DISTINCT FROM ''null''::jsonb" in sql
    )
    assert "pg_get_functiondef" in sql
    assert "PTG_ORDINARY_TERMINAL_JSON_NULL_PATCH_PRECONDITION_FAILED" in sql

    statements.clear()
    migration.downgrade()
    downgrade_sql = " ".join(statements[0].split())
    assert "pg_get_functiondef" in downgrade_sql
    assert (
        "ordinary_run.error::jsonb IS DISTINCT FROM ''null''::jsonb"
        in downgrade_sql
    )
    assert (
        "PTG_ORDINARY_TERMINAL_JSON_NULL_PATCH_PRECONDITION_FAILED"
        in downgrade_sql
    )


async def _execute(connection, statements: list[str]) -> None:
    async with connection.transaction():
        for statement in statements:
            await connection.execute(statement)


async def _set_ordinary_run_error(
    connection,
    quoted: str,
    run_id: str,
    value: str,
) -> None:
    await connection.execute(
        f"UPDATE {quoted}.import_run SET error = $2::jsonb WHERE run_id = $1",
        run_id,
        value,
    )


async def _assert_terminal_receipt_rejected(
    connection,
    schema: str,
    state: dict,
    receipt: dict,
) -> None:
    with pytest.raises(
        asyncpg.PostgresError,
        match="PTG_WAVE_ORDINARY_TERMINAL_BINDING_INVALID",
    ):
        await _insert_and_assert_terminal_receipt(
            connection,
            schema,
            state,
            receipt,
        )


@pytest.mark.asyncio
async def test_postgres_v13_json_null_patch_upgrades_and_downgrades(monkeypatch):
    schema = "v13_json_null_guard_migration"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(connection, monkeypatch, schema)
        await add_v13_head_prerequisites(connection, _quote(schema))

        base = _load_migration(MIGRATION_PATH)
        statements: list[str] = []
        monkeypatch.setattr(base.op, "execute", statements.append)
        base.upgrade()
        await _execute(connection, statements)

        patch = _load_migration(JSON_NULL_GUARD_MIGRATION_PATH)
        statements.clear()
        monkeypatch.setattr(patch.op, "execute", statements.append)
        patch.upgrade()
        await _execute(connection, statements)
        definition = await connection.fetchval(
            "SELECT pg_get_functiondef(to_regprocedure($1))",
            f"{schema}.ptg_import_wave_v13_abandonment_guard()",
        )
        assert patch._NEW_PREDICATE in definition
        assert patch._OLD_PREDICATE not in definition.replace(
            patch._NEW_PREDICATE, ""
        )

        statements.clear()
        patch.downgrade()
        await _execute(connection, statements)
        definition = await connection.fetchval(
            "SELECT pg_get_functiondef(to_regprocedure($1))",
            f"{schema}.ptg_import_wave_v13_abandonment_guard()",
        )
        assert patch._OLD_PREDICATE in definition
        assert patch._NEW_PREDICATE not in definition
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_ordinary_terminal_accepts_only_pristine_json_null(
    monkeypatch,
):
    schema = "ordinary_terminal_json_null_guard"
    connection = await asyncpg.connect(_dsn())
    try:
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
        )
        quoted = _quote(schema)
        run_id = state["request"]["run_id"]
        await _set_ordinary_run_error(connection, quoted, run_id, "null")
        assert await connection.fetchval(
            f"SELECT error IS NULL FROM {quoted}.import_run WHERE run_id = $1",
            run_id,
        ) is False
        await _assert_terminal_receipt_rejected(
            connection,
            schema,
            state,
            receipt,
        )

        patch = _load_migration(
            ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH
        )
        statements: list[str] = []
        monkeypatch.setattr(patch.op, "execute", statements.append)
        patch.upgrade()
        await _execute(connection, statements)

        await _set_ordinary_run_error(
            connection,
            quoted,
            run_id,
            '{"kind":"failed"}',
        )
        await _assert_terminal_receipt_rejected(
            connection,
            schema,
            state,
            receipt,
        )

        await _set_ordinary_run_error(connection, quoted, run_id, "null")
        await _insert_and_assert_terminal_receipt(
            connection,
            schema,
            state,
            receipt,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
