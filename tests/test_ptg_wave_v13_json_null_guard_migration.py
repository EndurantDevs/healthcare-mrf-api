"""Migration regression for V13 pristine JSON-null parity."""

from __future__ import annotations

import pytest

from tests.ptg_wave_v13_post_ready_guard_support import (
    JSON_NULL_GUARD_MIGRATION_PATH,
    MIGRATION_PATH,
    add_v13_head_prerequisites,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _install_receipt_migration,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
    asyncpg,
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


async def _execute(connection, statements: list[str]) -> None:
    async with connection.transaction():
        for statement in statements:
            await connection.execute(statement)


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
