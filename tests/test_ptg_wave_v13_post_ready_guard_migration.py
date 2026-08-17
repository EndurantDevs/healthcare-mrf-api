"""Focused PostgreSQL guard migration regression for V13 failed-wave quarantine."""

from __future__ import annotations

import pytest

from tests.ptg_wave_v13_post_ready_guard_support import (
    MIGRATION_PATH,
    add_v13_head_prerequisites,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _fixture,
    _install_receipt_migration,
    _insert_v12_quarantine,
    _seed_pristine_intents_and_runs,
    _seed_v6_wave,
)
from tests.test_ptg_wave_recovery_storage_postgres import _load_migration
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote, asyncpg


def test_v13_guard_migration_has_its_own_healthcare_head(monkeypatch):
    """Keep V13 isolated from the adjacent service's Alembic chain."""

    migration = _load_migration(MIGRATION_PATH)
    sql_statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "v13_failure_guard_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", sql_statements.append)

    migration.upgrade()
    sql = "\n".join(sql_statements)

    assert migration.down_revision == "20260816020000_address_evidence_alias"
    assert "ptg_import_wave_v13_abandonment_guard" in sql
    assert "ptg_import_wave_v13_abandoned_child_guard" in sql
    assert "ptg_import_wave_v13_abandoned_run_guard" in sql
    assert "ptg_import_wave_v13_abandoned_event_guard" in sql
    assert "v13_post_ready_unreleased_failure_cutover" in sql
    assert "healthporta.ptg-wave.v13-post-ready-unreleased-failure-abandonment-proof.v1" in sql
    assert "predecessor.kubernetes_manifest_identity\n                    ~" in sql
    assert "predecessor.kubernetes_config_identity\n                    ~" in sql
    assert "predecessor.pinned_image_reference\n                    ~" in sql
    assert "predecessor.pinned_image_digest\n                    ~" in sql
    assert "predecessor.runtime_image_identity\n                    ~" in sql
    assert "ADD COLUMN" not in sql
    assert (
        "recovery_basis IS NULL AND reason IN ( "
        "'legacy_uncertain_slots_waiting_pre_receipt', "
        "'materialized_preclaim_failure' )"
    ) in " ".join(sql.split())


@pytest.mark.asyncio
async def test_postgres_v13_guard_migration_installs_closed_fences(monkeypatch):
    """Compile the V13 trigger functions against the real healthcare head."""

    schema = "v13_post_ready_guard_install"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(connection, monkeypatch, schema)
        await add_v13_head_prerequisites(connection, _quote(schema))
        migration = _load_migration(MIGRATION_PATH)
        sql_statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", sql_statements.append)
        migration.upgrade()
        async with connection.transaction():
            for index, statement in enumerate(sql_statements):
                try:
                    await connection.execute(statement)
                except Exception as exc:  # pragma: no cover - preserves SQL receipt
                    raise AssertionError(
                        f"V13 guard migration statement {index} failed:\n"
                        f"{statement[:1_000]}"
                    ) from exc
        assert await connection.fetchval(
            "SELECT count(*) FROM pg_catalog.pg_trigger "
            "WHERE tgrelid = $1::regclass AND NOT tgisinternal",
            f"{schema}.ptg_import_wave_quarantine",
        ) >= 3
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_postgres_v13_migration_keeps_v12_quarantine_strict(monkeypatch):
    """The V13 constraint replacement keeps the closed V12 family intact."""

    schema = "v13_post_ready_guard_v12"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_receipt_migration(connection, monkeypatch, schema)
        await add_v13_head_prerequisites(connection, _quote(schema))
        migration = _load_migration(MIGRATION_PATH)
        sql_statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", sql_statements.append)
        migration.upgrade()
        async with connection.transaction():
            for statement in sql_statements:
                await connection.execute(statement)
        fixture = _fixture()
        admission = fixture["abandonment"]["proof"]["admission"]
        await _seed_v6_wave(
            connection,
            schema,
            admission,
            state="slots_waiting",
            materialized=True,
        )
        await _seed_pristine_intents_and_runs(connection, schema, admission)
        await _insert_v12_quarantine(connection, schema, fixture)
        assert await connection.fetchval(
            f"SELECT recovery_basis FROM {_quote(schema)}.ptg_import_wave_quarantine "
            "WHERE predecessor_wave_id = $1",
            admission["wave_id"],
        ) == "v12_pristine_materialized_cutover"
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
