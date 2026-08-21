"""PostgreSQL acceptance for authenticated empty terminal receipts."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.ptg_blank_terminal_support import blank_ordinary_result
from tests.ptg_wave_v13_post_ready_guard_support import (
    JSON_NULL_GUARD_MIGRATION_PATH,
    MIGRATION_PATH as V13_MIGRATION_PATH,
    add_v13_head_prerequisites,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _insert_and_assert_terminal_receipt,
    _prepare_ordinary_terminal_db_fixture,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _load_migration,
    _quote,
    asyncpg,
)
from tests.test_ptg_wave_v13_json_null_guard_migration import (
    ORDINARY_TERMINAL_JSON_CANONICAL_DIGEST_MIGRATION_PATH,
    ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH,
    _apply_test_migration,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260821010000_ptg_ordinary_terminal_blank_receipt.py"
)


def test_blank_receipt_patch_is_exact_and_reversible(monkeypatch):
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_blank_patch_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    assert migration.down_revision == (
        "20260820140000_prescription_autocomplete_rollup"
    )
    assert len(statements) == 1
    assert "pg_get_functiondef" in statements[0]
    assert "allowed_amount_provider_payments" in statements[0]
    assert "PTG_ORDINARY_TERMINAL_BLANK_PATCH_PRECONDITION_FAILED" in (
        statements[0]
    )

    statements.clear()
    migration.downgrade()
    assert len(statements) == 2
    assert "PTG_ORDINARY_TERMINAL_BLANK_DOWNGRADE_BLOCKED" in statements[0]
    assert "pg_get_functiondef" in statements[1]


async def _upgrade_terminal_guard(connection, monkeypatch, schema: str):
    await add_v13_head_prerequisites(connection, _quote(schema))
    for migration_path in (
        V13_MIGRATION_PATH,
        JSON_NULL_GUARD_MIGRATION_PATH,
        ORDINARY_TERMINAL_JSON_NULL_GUARD_MIGRATION_PATH,
        ORDINARY_TERMINAL_JSON_CANONICAL_DIGEST_MIGRATION_PATH,
        MIGRATION_PATH,
    ):
        migration = _load_migration(migration_path)
        try:
            await _apply_test_migration(
                connection,
                monkeypatch,
                migration,
                "upgrade",
            )
        except Exception as exc:
            raise AssertionError(migration_path.name) from exc
    return _load_migration(MIGRATION_PATH)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema", "state_factory"),
    (
        ("ptg_terminal_success_compat", None),
        ("ptg_terminal_blank_accept", blank_ordinary_result),
    ),
)
async def test_postgres_guard_accepts_success_and_exact_blank(
    monkeypatch,
    schema,
    state_factory,
):
    connection = await asyncpg.connect(_dsn())
    try:
        fixture_options = (
            {} if state_factory is None else {"state_factory": state_factory}
        )
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
            **fixture_options,
        )
        await _upgrade_terminal_guard(connection, monkeypatch, schema)
        await _insert_and_assert_terminal_receipt(
            connection,
            schema,
            state,
            receipt,
        )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_blank_receipt_rejects_payment_drift(monkeypatch):
    schema = "ptg_terminal_blank_drift"
    connection = await asyncpg.connect(_dsn())
    try:
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
            state_factory=blank_ordinary_result,
        )
        await _upgrade_terminal_guard(connection, monkeypatch, schema)
        quoted = _quote(schema)
        for table_name, document_name in (
            ("ptg2_import_run", "report"),
            ("ptg2_snapshot", "manifest"),
        ):
            await connection.execute(
                f"UPDATE {quoted}.{table_name} SET {document_name} = "
                f"jsonb_set({document_name}::jsonb, "
                "'{allowed_amount_lane,successful_files,0,summary,"
                "allowed_amount_payments}', '1'::jsonb)::json"
            )
        with pytest.raises(
            asyncpg.PostgresError,
            match=r"TERMINAL_RESULT_INVALID",
        ):
            await _insert_and_assert_terminal_receipt(
                connection,
                schema,
                state,
                receipt,
            )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("schema", "blank_receipt_present"),
    (
        ("ptg_terminal_blank_downgrade_blocked", True),
        ("ptg_terminal_blank_downgrade_restored", False),
    ),
)
async def test_postgres_blank_receipt_downgrade_boundary(
    monkeypatch,
    schema,
    blank_receipt_present,
):
    connection = await asyncpg.connect(_dsn())
    try:
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection,
            monkeypatch,
            schema,
            state_factory=blank_ordinary_result,
        )
        migration = await _upgrade_terminal_guard(
            connection,
            monkeypatch,
            schema,
        )
        if blank_receipt_present:
            await _insert_and_assert_terminal_receipt(
                connection,
                schema,
                state,
                receipt,
            )
            with pytest.raises(
                asyncpg.PostgresError,
                match=r"PTG_ORDINARY_TERMINAL_BLANK_DOWNGRADE_BLOCKED",
            ):
                await _apply_test_migration(
                    connection,
                    monkeypatch,
                    migration,
                    "downgrade",
                )
        else:
            await _apply_test_migration(
                connection,
                monkeypatch,
                migration,
                "downgrade",
            )
            with pytest.raises(
                asyncpg.PostgresError,
                match=r"PTG_WAVE_ORDINARY_TERMINAL_BINDING_INVALID",
            ):
                await _insert_and_assert_terminal_receipt(
                    connection,
                    schema,
                    state,
                    receipt,
                )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()
