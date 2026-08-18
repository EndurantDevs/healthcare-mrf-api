"""Focused PostgreSQL guard migration regression for V13 failed-wave quarantine."""

from __future__ import annotations

import asyncio

import pytest

from process.ptg_wave_ordinary_terminal_receipt import (
    ordinary_terminal_receipt_payload,
)
from process.ptg_wave_receipt_authority import (
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
)
from process.ptg_wave_receipt_contract import admission_receipt_mapping
from tests.ptg_wave_ordinary_terminal_receipt_support import (
    ISSUED_AT as TERMINAL_ISSUED_AT,
    keyring as _terminal_keyring,
    v13_ordinary_result,
)
from tests.ptg_wave_v13_post_ready_guard_support import (
    MIGRATION_PATH,
    add_v13_head_prerequisites,
)
from tests.test_ptg_wave_receipt_authority_migration import (
    _fixture,
    _insert_and_assert_terminal_receipt,
    _install_receipt_migration,
    _insert_v12_quarantine,
    _prepare_ordinary_terminal_db_fixture,
    _seed_direct_pristine_member,
    _seed_later_ordinary_result,
    _seed_pristine_intents_and_runs,
    _seed_v6_wave,
)
from tests.test_ptg_wave_recovery_storage_postgres import _load_migration
from tests.test_ptg_wave_recovery_storage_postgres import _dsn, _quote, asyncpg
from tests.test_ptg_wave_v13_post_ready_guard_postgres import (
    _insert_signed_quarantine,
)


def test_v13_guard_migration_has_its_own_healthcare_head(monkeypatch):
    """Keep V13 isolated from the adjacent service's Alembic chain."""

    migration = _load_migration(MIGRATION_PATH)
    sql_statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "v13_failure_guard_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", sql_statements.append)

    migration.upgrade()
    sql = "\n".join(sql_statements)
    upgrade_lock = next(
        statement for statement in sql_statements
        if statement.startswith("LOCK TABLE")
    )

    assert migration.down_revision == (
        "20260818020000_provider_directory_terminal_publication_compact_guard"
    )
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
    assert "ptg_wave_ordinary_terminal_receipt_guard" in sql
    assert "pg_get_functiondef" in sql
    assert "retired.reason IS DISTINCT FROM retired.recovery_basis" in sql
    assert "retired.recovery_basis IS NULL" in sql
    assert "retired.recovery_basis NOT IN" in sql
    assert sql.index("pg_advisory_xact_lock") < sql.index("LOCK TABLE")
    assert "ordinary_terminal_receipt" not in upgrade_lock
    assert "ADD COLUMN" not in sql
    assert (
        "recovery_basis IS NULL AND reason IN ( "
        "'legacy_uncertain_slots_waiting_pre_receipt', "
        "'materialized_preclaim_failure' )"
    ) in " ".join(sql.split())

    sql_statements.clear()
    migration.downgrade()
    downgrade_sql = "\n".join(sql_statements)
    assert downgrade_sql.index("pg_advisory_xact_lock") < downgrade_sql.index(
        "LOCK TABLE"
    )
    downgrade_lock = next(
        statement for statement in sql_statements
        if statement.startswith("LOCK TABLE")
    )
    assert "ordinary_terminal_receipt" not in downgrade_lock


async def _ordinary_terminal_guard_definition(connection, schema: str) -> str:
    return await connection.fetchval(
        "SELECT pg_get_functiondef(to_regprocedure($1))",
        f"{schema}.ptg_wave_ordinary_terminal_receipt_guard()",
    )


async def _execute_sql_statements(connection, statements) -> None:
    async with connection.transaction():
        for statement in statements:
            await connection.execute(statement)


async def _wait_for_advisory_lock(connection, backend_pid: int) -> None:
    for _attempt in range(100):
        lock_wait = await connection.fetchrow(
            "SELECT wait_event_type, wait_event FROM pg_stat_activity "
            "WHERE pid = $1",
            backend_pid,
        )
        if lock_wait is not None and tuple(lock_wait) == ("Lock", "advisory"):
            return
        await asyncio.sleep(0.01)
    pytest.fail("V13 migration never reached the admission advisory lock")


async def _wait_for_relation_lock(
    connection,
    backend_pid: int,
    relation: str,
    mode: str,
) -> None:
    for _attempt in range(500):
        if await connection.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks "
            "WHERE pid = $1 AND relation = to_regclass($2) "
            "AND mode = $3 AND NOT granted)",
            backend_pid,
            relation,
            mode,
        ):
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"V13 migration never waited on {relation}")


async def _assert_migration_serializes_with_abandonment(
    monkeypatch,
    direction: str,
) -> None:
    schema = f"v13_guard_{direction}_admission_lock"
    holder = await asyncpg.connect(_dsn())
    migrator = await asyncpg.connect(_dsn())
    holder_transaction = None
    migration_task = None
    try:
        await _install_receipt_migration(holder, monkeypatch, schema)
        quoted = _quote(schema)
        await add_v13_head_prerequisites(holder, quoted)
        migration = _load_migration(MIGRATION_PATH)
        statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        if direction == "downgrade":
            migration.upgrade()
            await _execute_sql_statements(holder, statements)
            statements.clear()
        getattr(migration, direction)()

        holder_transaction = holder.transaction()
        await holder_transaction.start()
        await holder.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            migration._ADMISSION_LOCK,
        )
        await holder.execute(
            f"LOCK TABLE {quoted}.ptg_import_wave_quarantine "
            "IN ROW EXCLUSIVE MODE"
        )
        migrator_pid = await migrator.fetchval("SELECT pg_backend_pid()")
        migration_task = asyncio.create_task(
            _execute_sql_statements(migrator, statements)
        )
        await _wait_for_advisory_lock(holder, migrator_pid)
        await asyncio.wait_for(
            holder.execute(
                f"LOCK TABLE {quoted}.ptg_import_wave "
                "IN SHARE ROW EXCLUSIVE MODE"
            ),
            timeout=1,
        )
        await holder_transaction.commit()
        await asyncio.wait_for(migration_task, timeout=10)
        migration_task = None
    finally:
        if migration_task is not None:
            migration_task.cancel()
            await asyncio.gather(migration_task, return_exceptions=True)
        if holder_transaction is not None and holder.is_in_transaction():
            await holder_transaction.rollback()
        await holder.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await migrator.close()
        await holder.close()


@pytest.mark.parametrize("direction", ("upgrade", "downgrade"))
@pytest.mark.asyncio
async def test_postgres_v13_migration_serializes_with_abandonment(
    monkeypatch,
    direction,
):
    """Migration authority precedes every relation lock in both directions."""

    await _assert_migration_serializes_with_abandonment(monkeypatch, direction)


async def _upgrade_while_terminal_receipt_finishes(
    holder,
    migrator,
    monkeypatch,
    schema: str,
    terminal,
):
    quoted = _quote(schema)
    receipt = f"{quoted}.ptg_import_wave_ordinary_terminal_receipt"
    quarantine = f"{quoted}.ptg_import_wave_quarantine"
    migration = _load_migration(MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    migration_task = None
    try:
        async with holder.transaction():
            await holder.execute(f"LOCK TABLE {receipt} IN ROW SHARE MODE")
            await holder.execute(
                f"LOCK TABLE {quarantine} IN ACCESS SHARE MODE"
            )
            migrator_pid = await migrator.fetchval("SELECT pg_backend_pid()")
            migration_task = asyncio.create_task(
                _execute_sql_statements(migrator, statements)
            )
            await _wait_for_relation_lock(
                holder,
                migrator_pid,
                f"{schema}.ptg_import_wave_quarantine",
                "AccessExclusiveLock",
            )
            await asyncio.wait_for(
                _insert_and_assert_terminal_receipt(
                    holder, schema, terminal[0], terminal[1]
                ),
                timeout=3,
            )
        await asyncio.wait_for(migration_task, timeout=10)
        migration_task = None
    finally:
        if migration_task is not None and not migration_task.done():
            migration_task.cancel()
            await asyncio.gather(migration_task, return_exceptions=True)
    assert migration._ORDINARY_TERMINAL_V13_PREDICATE in (
        await _ordinary_terminal_guard_definition(holder, schema)
    )
    return migration, statements


async def _downgrade_with_terminal_reader(
    holder,
    migrator,
    migration,
    statements,
    schema: str,
) -> None:
    receipt = (
        f"{_quote(schema)}.ptg_import_wave_ordinary_terminal_receipt"
    )
    statements.clear()
    migration.downgrade()
    migration_task = None
    try:
        async with holder.transaction():
            await holder.execute(f"LOCK TABLE {receipt} IN ROW SHARE MODE")
            migration_task = asyncio.create_task(
                _execute_sql_statements(migrator, statements)
            )
            await asyncio.wait_for(migration_task, timeout=10)
            migration_task = None
    finally:
        if migration_task is not None and not migration_task.done():
            migration_task.cancel()
            await asyncio.gather(migration_task, return_exceptions=True)
    assert migration._ORDINARY_TERMINAL_V12_PREDICATE in (
        await _ordinary_terminal_guard_definition(holder, schema)
    )


@pytest.mark.asyncio
async def test_postgres_v13_migration_does_not_lock_terminal_receipts(
    monkeypatch,
):
    """Emulate the app's receipt read-to-flush table lock sequence."""

    schema = "v13_guard_terminal_receipt_lock"
    holder = await asyncpg.connect(_dsn())
    migrator = await asyncpg.connect(_dsn())
    try:
        terminal = await _prepare_ordinary_terminal_db_fixture(
            holder, monkeypatch, schema
        )
        await add_v13_head_prerequisites(holder, _quote(schema))
        migration, statements = await _upgrade_while_terminal_receipt_finishes(
            holder, migrator, monkeypatch, schema, terminal
        )
        await _downgrade_with_terminal_reader(
            holder, migrator, migration, statements, schema
        )
    finally:
        await holder.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await migrator.close()
        await holder.close()


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
        definition = await _ordinary_terminal_guard_definition(
            connection, schema
        )
        assert migration._ORDINARY_TERMINAL_V13_PREDICATE in definition

        sql_statements.clear()
        migration.downgrade()
        async with connection.transaction():
            for statement in sql_statements:
                await connection.execute(statement)
        definition = await _ordinary_terminal_guard_definition(
            connection, schema
        )
        assert migration._ORDINARY_TERMINAL_V12_PREDICATE in definition
        assert migration._ORDINARY_TERMINAL_V13_PREDICATE not in definition
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


async def _seed_v13_pending_terminal(connection, monkeypatch, schema: str):
    state = v13_ordinary_result(monkeypatch)
    quoted = _quote(schema)
    wave, intent = state["wave"], state["intent"]
    proof = state["quarantine"].recovery_evidence
    admission = admission_receipt_mapping(wave, (intent,))
    assert proof["admission"] == admission
    job_receipt = proof["kubernetes"]["job_receipt"]
    await _seed_v6_wave(
        connection,
        schema,
        admission,
        state="slots_waiting",
        materialized={
            "kubernetes": {
                "job_uid": job_receipt["job_uid"],
                "job_receipt_digest": proof["kubernetes"][
                    "job_receipt_digest"
                ],
            }
        },
    )
    await connection.execute(
        f"UPDATE {quoted}.ptg_import_wave SET "
        "kubernetes_manifest = '{}'::json, "
        "kubernetes_manifest_bytes = convert_to('{}', 'UTF8'), "
        "kubernetes_manifest_sha256 = "
        "encode(sha256(convert_to('{}', 'UTF8')), 'hex') "
        "WHERE wave_id = $1",
        admission["wave_id"],
    )
    await _seed_direct_pristine_member(
        connection, schema, wave=wave, intent=intent
    )
    signer = _terminal_keyring(monkeypatch)
    await _insert_signed_quarantine(
        connection, quoted, admission, proof, signer
    )
    return state, signer


async def _insert_v13_terminal_receipt(connection, monkeypatch, schema: str) -> None:
    state, signer = await _seed_v13_pending_terminal(
        connection, monkeypatch, schema
    )
    await _seed_later_ordinary_result(connection, schema, state)
    receipt = signer.sign_receipt(
        schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
        key_id=state["request"]["key_id"],
        issued_at=TERMINAL_ISSUED_AT,
        receipt_payload=ordinary_terminal_receipt_payload(**state),
    )
    await _insert_and_assert_terminal_receipt(
        connection, schema, state, receipt
    )


@pytest.mark.asyncio
async def test_postgres_v13_terminal_receipt_accepts_exact_signed_family(
    monkeypatch,
):
    """A signed V13 quarantine can persist its later terminal witness."""

    schema = "v13_post_ready_terminal_receipt"
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
        await _insert_v13_terminal_receipt(
            connection, monkeypatch, schema
        )
        sql_statements.clear()
        migration.downgrade()
        with pytest.raises(asyncpg.PostgresError, match="V13_DOWNGRADE_BLOCKED"):
            async with connection.transaction():
                for statement in sql_statements:
                    await connection.execute(statement)
        definition = await _ordinary_terminal_guard_definition(
            connection, schema
        )
        assert migration._ORDINARY_TERMINAL_V13_PREDICATE in definition
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
