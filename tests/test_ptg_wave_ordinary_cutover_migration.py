"""Migration and PostgreSQL proof for non-wave ordinary cutover."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from db.models import PTGImportWaveQuarantine
from tests.ptg_wave_materialized_preclaim_postgres_support import (
    materialized_evidence,
    seed_materialized_predecessor,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    _dsn,
    _install_migration,
    _load_migration,
    _quote,
    asyncpg,
)


ROOT = Path(__file__).resolve().parents[1]
CUTOVER_MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260809040000_ptg_import_wave_ordinary_cutover.py"
)


async def _install_cutover(connection, monkeypatch, schema: str):
    await _install_migration(connection, monkeypatch, schema)
    migration = _load_migration(CUTOVER_MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    async with connection.transaction():
        for statement in statements:
            await connection.execute(statement)
    return migration


async def _abandon(connection, schema: str, descriptor: dict, cutover_id: str):
    proof, canonical = materialized_evidence(descriptor, cutover_id)
    await connection.execute(
        f"""
        INSERT INTO {_quote(schema)}.ptg_import_wave_quarantine (
            predecessor_wave_id, reason, successor_wave_id, recovery_basis,
            recovery_evidence, recovery_evidence_canonical,
            recovery_evidence_sha256
        ) VALUES (
            $1, 'materialized_preclaim_failure', $2,
            'materialized_preclaim_failure', $3::jsonb, $4, $5
        )
        """,
        descriptor["wave_id"],
        cutover_id,
        json.dumps(proof),
        canonical,
        proof["proof_digest"],
    )
    return proof


def test_migration_reuses_exact_guard_and_has_one_head_link(monkeypatch):
    migration = _load_migration(CUTOVER_MIGRATION_PATH)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ordinary_cutover_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()
    sql = "\n".join(statements)

    assert migration.down_revision == (
        "20260810090000_provider_directory_terminal_root_retirement"
    )
    assert "ADD COLUMN successor_wave_id varchar(64)" in sql
    assert "ADD COLUMN recovery_evidence jsonb" in sql
    assert "ptg_import_wave_materialized_preclaim_guard" in sql
    assert "NEW.recovery_basis = 'materialized_preclaim_failure'" in sql
    assert "admitted.node_id IS NOT NULL" in sql
    assert "ptg_import_wave_abandonment_run_guard" in sql
    assert (
        'CREATE TRIGGER "ptg_import_wave_abandonment_event_guard" '
        "BEFORE INSERT OR UPDATE OR DELETE"
    ) in sql
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        trigger_name = f'{table_name}_abandonment_truncate_guard'
        assert (
            f'CREATE TRIGGER "{trigger_name}" BEFORE TRUNCATE ON '
        ) in sql
        assert (
            f'ENABLE ALWAYS TRIGGER "{trigger_name}"'
        ) in sql
    assert "abandoned.recovery_basis = 'materialized_preclaim_failure'" in sql
    assert "REFERENCES" not in "\n".join(
        statement
        for statement in statements
        if "ADD COLUMN successor_wave_id" in statement
    )


def test_quarantine_model_exposes_nullable_cutover_audit_shape():
    table = PTGImportWaveQuarantine.__table__
    assert {
        "successor_wave_id",
        "recovery_basis",
        "recovery_evidence",
        "recovery_evidence_canonical",
        "recovery_evidence_sha256",
    }.issubset(table.columns.keys())
    assert table.columns.successor_wave_id.nullable is True
    assert PTGImportWaveQuarantine.cutover_id.property.columns[0].name == (
        "successor_wave_id"
    )


def _active_capacity_owner_sql(quoted_schema: str) -> str:
    return f"""
        SELECT count(*) FROM {quoted_schema}.ptg_import_wave AS candidate
         WHERE candidate.state IN (
            'admitted', 'materializing', 'slots_waiting',
            'redis_releasing', 'released', 'executing',
            'awaiting_linkage', 'terminalizing', 'cleaning', 'uncertain'
         )
           AND NOT EXISTS (
               SELECT 1 FROM {quoted_schema}.ptg_import_wave_supersession AS retired
                WHERE retired.predecessor_wave_id = candidate.wave_id
           )
           AND NOT EXISTS (
               SELECT 1 FROM {quoted_schema}.ptg_import_wave_quarantine AS abandoned
                WHERE abandoned.predecessor_wave_id = candidate.wave_id
                  AND abandoned.recovery_basis = 'materialized_preclaim_failure'
           )
    """


async def _assert_preserved_exact_rows(
    connection,
    quoted_schema: str,
    descriptor: dict,
    proof: dict,
) -> None:
    assert await connection.fetchval(
        f"SELECT count(*) FROM {quoted_schema}.ptg_import_wave_supersession "
        "WHERE predecessor_wave_id = $1",
        descriptor["wave_id"],
    ) == 0
    retained_evidence = await connection.fetchrow(
        f"""
        SELECT wave.state, run.status, run.node_id,
               quarantine.recovery_evidence_sha256
          FROM {quoted_schema}.ptg_import_wave AS wave
          JOIN {quoted_schema}.ptg_import_wave_intent AS member
            ON member.wave_id = wave.wave_id
          JOIN {quoted_schema}.import_run AS run ON run.run_id = member.run_id
          JOIN {quoted_schema}.ptg_import_wave_quarantine AS quarantine
            ON quarantine.predecessor_wave_id = wave.wave_id
         WHERE wave.wave_id = $1
        """,
        descriptor["wave_id"],
    )
    assert tuple(retained_evidence) == (
        "slots_waiting",
        "queued",
        None,
        proof["proof_digest"],
    )


async def _assert_quarantine_evidence_immutable(
    connection,
    quoted_schema: str,
) -> None:
    assert await connection.fetchval(
        """
        SELECT tgenabled = 'A'
          FROM pg_catalog.pg_trigger
         WHERE tgrelid = $1::regclass
           AND tgname = 'ptg_import_wave_quarantine_row_guard'
        """,
        f"{quoted_schema}.ptg_import_wave_quarantine",
    ) is True
    quarantine_mutations = (
        f"UPDATE {quoted_schema}.ptg_import_wave_quarantine "
        "SET successor_wave_id = 'tampered-cutover' "
        "WHERE predecessor_wave_id = 'materialized-wave'",
        f"DELETE FROM {quoted_schema}.ptg_import_wave_quarantine "
        "WHERE predecessor_wave_id = 'materialized-wave'",
    )
    for mutation_sql in quarantine_mutations:
        with pytest.raises(asyncpg.PostgresError, match="RECOVERY_IMMUTABLE"):
            await connection.execute(mutation_sql)


async def _assert_source_attempt_evidence_immutable(
    connection,
    quoted_schema: str,
) -> None:
    event_mutations = (
        f"UPDATE {quoted_schema}.ptg_source_attempt_event "
        "SET event_kind = 'tampered' "
        "WHERE outer_run_id = 'materialized-run'",
        f"DELETE FROM {quoted_schema}.ptg_source_attempt_event "
        "WHERE outer_run_id = 'materialized-run'",
    )
    for mutation_sql in event_mutations:
        with pytest.raises(asyncpg.PostgresError, match="ABANDONED_IMMUTABLE"):
            await connection.execute(mutation_sql)
    assert await connection.fetchval(
        f"SELECT event_kind FROM {quoted_schema}.ptg_source_attempt_event "
        "WHERE outer_run_id = 'materialized-run'"
    ) == "start_admitted"


async def _assert_abandonment_truncate_guards(
    connection,
    quoted_schema: str,
) -> None:
    guarded_tables = (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    )
    trigger_count = await connection.fetchval(
        """
        SELECT count(*)
          FROM pg_catalog.pg_trigger AS installed
          JOIN pg_catalog.pg_class AS relation
            ON relation.oid = installed.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace
            ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = $1
           AND relation.relname = ANY($2::text[])
           AND installed.tgname LIKE '%_abandonment_truncate_guard'
           AND installed.tgenabled = 'A'
        """,
        quoted_schema.strip('"'),
        list(guarded_tables),
    )
    assert trigger_count == len(guarded_tables)
    for table_name in (
        "ptg_import_wave_intent",
        "import_run",
        "ptg_source_attempt_event",
    ):
        with pytest.raises(
            asyncpg.PostgresError,
            match="ABANDONED_IMMUTABLE",
        ):
            await connection.execute(
                f"TRUNCATE TABLE {quoted_schema}.{table_name}"
            )

    # No pristine wave can have claims or outcomes.  Their guards remain
    # scoped: empty relations can still be truncated safely.
    await connection.execute(
        f"TRUNCATE TABLE {quoted_schema}.ptg_import_wave_claim, "
        f"{quoted_schema}.ptg_import_wave_outcome"
    )


async def _assert_abandoned_run_cannot_progress(
    connection,
    quoted_schema: str,
    wave_id: str,
) -> None:
    with pytest.raises(asyncpg.PostgresError, match="ABANDONED_IMMUTABLE"):
        await connection.execute(
            f"UPDATE {quoted_schema}.import_run SET status = 'running' "
            "WHERE run_id = 'materialized-run'"
        )
    with pytest.raises(asyncpg.PostgresError, match="ABANDONED_IMMUTABLE"):
        await connection.execute(
            f"INSERT INTO {quoted_schema}.ptg_import_wave_claim (wave_id) "
            "VALUES ($1)",
            wave_id,
        )


async def _assert_ordinary_run_remains_admissible(
    connection,
    quoted_schema: str,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.import_run (
            run_id, node_id, importer, status, source_file_import_id,
            import_id, params, metrics
        ) VALUES (
            'ordinary-run', NULL, 'ptg', 'queued', 'ordinary-source',
            'ordinary-source', '{{}}'::jsonb, '{{}}'::jsonb
        )
        """
    )
    assert await connection.fetchval(
        f"SELECT status FROM {quoted_schema}.import_run "
        "WHERE run_id = 'ordinary-run'"
    ) == "queued"


@pytest.mark.asyncio
async def test_cutover_clears_only_abandoned_capacity_and_preserves_rows(
    monkeypatch,
):
    """Prove abandonment releases capacity without deleting historical rows."""

    schema = "wave_ordinary_cutover_acceptance"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_cutover(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        quoted = _quote(schema)
        active_owner_sql = _active_capacity_owner_sql(quoted)
        assert await connection.fetchval(active_owner_sql) == 1
        await connection.execute(
            f"""
            INSERT INTO {quoted}.ptg_source_attempt_event (
                outer_run_id, event_kind
            ) VALUES ('materialized-run', 'start_admitted')
            """
        )

        proof = await _abandon(
            connection,
            schema,
            descriptor,
            "ordinary-cutover-operation",
        )

        assert await connection.fetchval(active_owner_sql) == 0
        await _assert_preserved_exact_rows(connection, quoted, descriptor, proof)
        await _assert_quarantine_evidence_immutable(connection, quoted)
        await _assert_source_attempt_evidence_immutable(connection, quoted)
        await _assert_abandonment_truncate_guards(
            connection,
            quoted,
        )
        await _assert_abandoned_run_cannot_progress(
            connection,
            quoted,
            descriptor["wave_id"],
        )
        await _assert_ordinary_run_remains_admissible(connection, quoted)
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
async def test_empty_cutover_migration_downgrades_cleanly(monkeypatch):
    schema = "wave_ordinary_cutover_empty_downgrade"
    connection = await asyncpg.connect(_dsn())
    try:
        migration = await _install_cutover(connection, monkeypatch, schema)
        statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        migration.downgrade()
        async with connection.transaction():
            for statement in statements:
                await connection.execute(statement)
        assert await connection.fetchval(
            """
            SELECT count(*)
              FROM information_schema.columns
             WHERE table_schema = $1
               AND table_name = 'ptg_import_wave_quarantine'
               AND column_name = 'recovery_basis'
            """,
            schema,
        ) == 0
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
async def test_recorded_cutover_blocks_downgrade(monkeypatch):
    schema = "wave_ordinary_cutover_blocked_downgrade"
    connection = await asyncpg.connect(_dsn())
    try:
        migration = await _install_cutover(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await _abandon(
            connection,
            schema,
            descriptor,
            "downgrade-cutover-operation",
        )
        statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        migration.downgrade()
        with pytest.raises(
            asyncpg.PostgresError,
            match="ABANDONMENT_DOWNGRADE_BLOCKED",
        ):
            async with connection.transaction():
                for statement in statements:
                    await connection.execute(statement)
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_sql,error_pattern",
    (
        (
            "UPDATE {schema}.import_run SET node_id = 'assigned-node' "
            "WHERE run_id = 'materialized-run'",
            "REQUIRES_UNASSIGNED_RUNS",
        ),
        (
            "UPDATE {schema}.import_run SET status = 'running' "
            "WHERE run_id = 'materialized-run'",
            "MATERIALIZED_PRECLAIM_REQUIRED",
        ),
        (
            "INSERT INTO {schema}.ptg_import_wave_claim (wave_id) "
            "VALUES ('materialized-wave')",
            "MATERIALIZED_PRECLAIM_REQUIRED",
        ),
        (
            "UPDATE {schema}.ptg_import_wave SET "
            "redis_release_ticket = 'released' "
            "WHERE wave_id = 'materialized-wave'",
            "MATERIALIZED_PRECLAIM_REQUIRED",
        ),
    ),
)
async def test_cutover_rejects_assigned_started_claimed_or_released_state(
    monkeypatch,
    unsafe_sql,
    error_pattern,
):
    suffix = hashlib.sha256(unsafe_sql.encode()).hexdigest()[:10]
    schema = "wave_ordinary_cutover_unsafe_" + suffix
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_cutover(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await connection.execute(unsafe_sql.format(schema=_quote(schema)))
        with pytest.raises(asyncpg.PostgresError, match=error_pattern):
            await _abandon(
                connection,
                schema,
                descriptor,
                "unsafe-cutover-operation",
            )
        assert await connection.fetchval(
            f"SELECT count(*) FROM {_quote(schema)}.ptg_import_wave_quarantine "
            "WHERE predecessor_wave_id = $1 "
            "AND recovery_basis = 'materialized_preclaim_failure'",
            descriptor["wave_id"],
        ) == 0
    finally:
        await connection.execute(
            f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE"
        )
        await connection.close()
