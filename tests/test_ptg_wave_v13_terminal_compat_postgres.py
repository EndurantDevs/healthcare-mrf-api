"""Persisted PostgreSQL compatibility proofs for the V13 terminal guard."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import PTGImportWave, db
from process.ptg_wave_receipt_authority import (
    PTGWaveReceiptAuthorityError,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_key_coverage import (
    assert_nonterminal_receipt_key_coverage,
)
from tests.ptg_wave_v13_post_ready_guard_support import (
    MIGRATION_PATH,
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
from tests.test_ptg_wave_v13_post_ready_guard_migration import (
    _execute_sql_statements,
    _seed_v13_pending_terminal,
)


async def _upgrade_v13_guard(connection, monkeypatch, schema: str):
    await add_v13_head_prerequisites(connection, _quote(schema))
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    await _execute_sql_statements(connection, statements)
    return migration, statements


def _public_only_keyring(signer) -> PTGWaveReceiptKeyring:
    epoch = signer.public_by_key_id[signer.active_key_id]
    return PTGWaveReceiptKeyring(
        active_key_id=epoch.key_id,
        signing_by_key_id={},
        public_by_key_id={epoch.key_id: epoch},
    )


@pytest.mark.asyncio
async def test_postgres_v13_pending_signer_required(monkeypatch):
    """Persisted pending V13 members keep their private signer available."""

    schema = "v13_pending_terminal_signer"
    connection = await asyncpg.connect(_dsn())
    engine = None
    try:
        await _install_receipt_migration(connection, monkeypatch, schema)
        await _upgrade_v13_guard(connection, monkeypatch, schema)
        _state, signer = await _seed_v13_pending_terminal(
            connection, monkeypatch, schema
        )
        model_schema = PTGImportWave.__table__.schema
        assert model_schema
        engine = create_async_engine(
            _dsn().replace("postgresql://", "postgresql+asyncpg://", 1),
            execution_options={
                "schema_translate_map": {model_schema: schema},
            },
        )
        monkeypatch.setattr(
            db,
            "session",
            async_sessionmaker(engine, expire_on_commit=False),
        )
        with pytest.raises(
            PTGWaveReceiptAuthorityError,
            match="unavailable for signing",
        ):
            await assert_nonterminal_receipt_key_coverage(
                keyring=_public_only_keyring(signer)
            )
        await assert_nonterminal_receipt_key_coverage(keyring=signer)
    finally:
        if engine is not None:
            await engine.dispose()
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.parametrize("guard_state", ("upgraded", "downgraded"))
@pytest.mark.asyncio
async def test_postgres_v13_keeps_v12_receipt(monkeypatch, guard_state):
    """A signed V12 terminal first-write remains valid on both guard bodies."""

    schema = f"v13_v12_terminal_{guard_state}"
    connection = await asyncpg.connect(_dsn())
    try:
        state, receipt = await _prepare_ordinary_terminal_db_fixture(
            connection, monkeypatch, schema
        )
        migration, statements = await _upgrade_v13_guard(
            connection, monkeypatch, schema
        )
        if guard_state == "downgraded":
            statements.clear()
            migration.downgrade()
            await _execute_sql_statements(connection, statements)
        await _insert_and_assert_terminal_receipt(
            connection, schema, state, receipt
        )
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()
