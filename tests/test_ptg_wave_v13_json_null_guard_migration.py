"""Migration regressions for PTG JSON-null guard parity."""

from __future__ import annotations

import json

import pytest

from tests import test_ptg_wave_receipt_authority_migration as receipt_authority
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
ORDINARY_TERMINAL_JSON_CANONICAL_DIGEST_MIGRATION_PATH = ROOT / "alembic" / (
    "versions/20260820030000_ptg_ordinary_terminal_json_canonical_digest.py"
)
_BASE_ORDINARY_RESULT = receipt_authority._ordinary_result


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


def test_ordinary_terminal_json_canonical_digest_patch_is_exact(monkeypatch):
    migration = _load_migration(
        ORDINARY_TERMINAL_JSON_CANONICAL_DIGEST_MIGRATION_PATH
    )
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ordinary_terminal_digest_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    assert migration.down_revision == (
        "20260820020000_ptg_ordinary_terminal_json_null_guard"
    )
    assert len(statements) == 2
    function_sql, replacement_sql = statements
    assert (
        'CREATE FUNCTION "ordinary_terminal_digest_test".'
        '"ptg_wave_canonical_json_ascii_v1"(payload json)' in function_sql
    )
    assert "FROM json_each(payload)" in function_sql
    assert "FROM json_array_elements(payload) WITH ORDINALITY" in function_sql
    assert "RETURN btrim(payload::text)" in function_sql
    assert "RETURN \"ordinary_terminal_digest_test\"." in function_sql
    for old_value, new_value in migration._DOCUMENTS:
        assert migration._canonical_call(old_value) in replacement_sql
        assert migration._canonical_call(new_value) in replacement_sql
    assert "PTG_ORDINARY_TERMINAL_JSON_DIGEST_PATCH_PRECONDITION_FAILED" in (
        replacement_sql
    )

    statements.clear()
    migration.downgrade()
    assert len(statements) == 2
    assert all(
        migration._canonical_call(value) in statements[0]
        for pair in migration._DOCUMENTS
        for value in pair
    )
    assert statements[1].endswith(
        '"ptg_wave_canonical_json_ascii_v1"(json)'
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


def _ordinary_result_with_exponent(monkeypatch):
    state = _BASE_ORDINARY_RESULT(monkeypatch)
    for document in (
        state["run"].params,
        state["run"].metrics,
        state["engine_run"].options,
        state["engine_run"].report,
        state["engine_snapshot"].manifest,
    ):
        document["canonical_exponent_probe"] = 4.5280903577804565e-06
    return state


async def _restore_raw_exponent_documents(
    connection,
    quoted: str,
    state: dict,
) -> None:
    await connection.execute(
        f"""
        ALTER TABLE {quoted}.import_run
            ALTER COLUMN params TYPE json USING params::json,
            ALTER COLUMN metrics TYPE json USING metrics::json;
        ALTER TABLE {quoted}.ptg2_import_run
            ALTER COLUMN options TYPE json USING options::json,
            ALTER COLUMN report TYPE json USING report::json;
        ALTER TABLE {quoted}.ptg2_snapshot
            ALTER COLUMN manifest TYPE json USING manifest::json;
        """
    )
    await connection.execute("DISCARD PLANS")
    await connection.execute(
        f"UPDATE {quoted}.import_run SET params = $2::json, metrics = $3::json "
        "WHERE run_id = $1",
        state["run"].run_id,
        json.dumps(state["run"].params),
        json.dumps(state["run"].metrics),
    )
    await connection.execute(
        f"UPDATE {quoted}.ptg2_import_run SET options = $2::json, "
        "report = $3::json WHERE import_run_id = $1",
        state["engine_run"].import_run_id,
        json.dumps(state["engine_run"].options),
        json.dumps(state["engine_run"].report),
    )
    await connection.execute(
        f"UPDATE {quoted}.ptg2_snapshot SET manifest = $2::json "
        "WHERE snapshot_id = $1",
        state["engine_snapshot"].snapshot_id,
        json.dumps(state["engine_snapshot"].manifest),
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
async def test_postgres_ordinary_terminal_preserves_python_exponent_digests(
    monkeypatch,
):
    schema = "ordinary_terminal_exponent_digest"
    connection = await asyncpg.connect(_dsn())
    try:
        monkeypatch.setattr(
            receipt_authority,
            "_ordinary_result",
            _ordinary_result_with_exponent,
        )
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
        )
        quoted = _quote(schema)
        await _restore_raw_exponent_documents(
            connection,
            quoted,
            state,
        )
        assert "4.5280903577804565e-06" in await connection.fetchval(
            f"SELECT metrics::text FROM {quoted}.import_run WHERE run_id = $1",
            state["run"].run_id,
        )

        patch = _load_migration(
            ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH
        )
        statements: list[str] = []
        monkeypatch.setattr(patch.op, "execute", statements.append)
        patch.upgrade()
        await _execute(connection, statements)

        canonical_patch = _load_migration(
            ORDINARY_TERMINAL_JSON_CANONICAL_DIGEST_MIGRATION_PATH
        )
        statements.clear()
        monkeypatch.setattr(canonical_patch.op, "execute", statements.append)
        canonical_patch.upgrade()
        await _execute(connection, statements)

        statements.clear()
        canonical_patch.downgrade()
        await _execute(connection, statements)
        with pytest.raises(
            asyncpg.PostgresError,
            match="PTG_WAVE_ORDINARY_TERMINAL_RECEIPT_INVALID",
        ):
            await _insert_and_assert_terminal_receipt(
                connection,
                schema,
                state,
                receipt,
            )

        statements.clear()
        canonical_patch.upgrade()
        await _execute(connection, statements)
        await _insert_and_assert_terminal_receipt(
            connection,
            schema,
            state,
            receipt,
        )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


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
