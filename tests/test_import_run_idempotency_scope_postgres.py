# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
import subprocess
import sys
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy import select, text, update

from api import control_imports
from api.control_imports import ACTIVE_STATUSES, TERMINAL_STATUSES
from api.ptg_membership_contract import IMPORTER as MEMBERSHIP_IMPORTER
from db.models import ImportRun, db
from tests.test_control_imports_db import (
    _drop_import_run_schema,
    _fake_enqueue,
    _require_disposable_database,
    _reset_import_run_schema,
)

pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings("ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"),
]


async def _create_run(run_id: str, importer: str):
    return await control_imports.create_import_run(
        {
            "run_id": run_id,
            "importer": importer,
            "idempotency_key": "idem-db",
        }
    )


async def _assert_same_importer_integrity_recovery(monkeypatch):
    real_find = control_imports.find_active_run_by_idempotency_key
    real_find_importer = control_imports.find_earliest_active_run_by_importer
    lookup_count_by_kind = {"active": 0}

    async def race_miss_then_real(importer: str, idempotency_key: str):
        lookup_count_by_kind["active"] += 1
        if lookup_count_by_kind["active"] == 1:
            return None
        return await real_find(importer, idempotency_key)

    async def race_importer_miss(importer: str):
        assert importer == "nucc"
        return None

    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        race_miss_then_real,
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        race_importer_miss,
    )
    replayed, created = await _create_run("run_replayed", "nucc")
    assert created is False
    assert replayed["run_id"] == "run_first"
    assert lookup_count_by_kind["active"] == 2
    return real_find, real_find_importer


async def _assert_active_index_definition() -> None:
    index_record_by_name = {
        str(index_record.index_name): index_record
        for index_record in (
            await db.execute(
                text(
                    """
                    SELECT index_record.relname AS index_name,
                           pg_get_indexdef(index_record.oid) AS index_definition,
                           index_state.indisunique AS is_unique,
                           pg_get_expr(
                               index_state.indpred,
                               index_state.indrelid
                           ) AS predicate
                      FROM pg_class AS index_record
                      JOIN pg_index AS index_state
                        ON index_state.indexrelid = index_record.oid
                      JOIN pg_namespace AS namespace_record
                        ON namespace_record.oid = index_record.relnamespace
                     WHERE namespace_record.nspname = :schema
                       AND index_record.relname IN (
                           'import_run_active_idempotency_idx',
                           'import_run_importer_active_idempotency_idx'
                       )
                    """
                ),
                schema=ImportRun.__table__.schema or "mrf",
            )
        ).all()
    }
    assert set(index_record_by_name) == {
        "import_run_importer_active_idempotency_idx",
    }
    composite_index = index_record_by_name["import_run_importer_active_idempotency_idx"]
    assert "(importer, idempotency_key)" in str(composite_index.index_definition)
    assert composite_index.is_unique is True
    predicate = str(composite_index._mapping["predicate"])
    assert set(re.findall(r"'([^']+)'", predicate)) == set(control_imports.ACTIVE_STATUSES), predicate


def _alembic_env(schema: str) -> dict[str, str]:
    environment = os.environ.copy()
    environment["HLTHPRT_DB_SCHEMA"] = schema
    environment.pop("DB_SCHEMA", None)
    return environment


def _run_alembic(schema: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        cwd=Path(__file__).resolve().parents[1],
        env=_alembic_env(schema),
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )


async def _index_state(
    connection: asyncpg.Connection,
    schema: str,
    index_name: str,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        """
        SELECT table_record.relname AS table_name,
               index_state.indisvalid,
               index_state.indisready,
               index_state.indislive
          FROM pg_class AS index_record
          JOIN pg_namespace AS index_namespace
            ON index_namespace.oid = index_record.relnamespace
          JOIN pg_index AS index_state
            ON index_state.indexrelid = index_record.oid
          JOIN pg_class AS table_record
            ON table_record.oid = index_state.indrelid
         WHERE index_namespace.nspname = $1
           AND index_record.relname = $2
        """,
        schema,
        index_name,
    )


async def _create_activation_schema(connection, schema: str) -> None:
    predicate = "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')"
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await connection.execute(
        f"""
        CREATE TABLE "{schema}".import_run (
            importer text NOT NULL,
            idempotency_key text,
            status text NOT NULL
        )
        """
    )
    await connection.execute(
        f"""
        CREATE UNIQUE INDEX import_run_active_idempotency_idx
            ON "{schema}".import_run (idempotency_key)
         WHERE {predicate}
        """
    )
    await connection.execute(
        f"""
        CREATE UNIQUE INDEX import_run_importer_active_idempotency_idx
            ON "{schema}".import_run (importer, idempotency_key)
         WHERE {predicate}
        """
    )


async def _assert_activation_upgrade(connection, schema: str) -> None:
    stamped = _run_alembic(
        schema,
        "stamp",
        "20260829090000_import_run_idempotency_scope",
    )
    assert stamped.returncode == 0, stamped.stdout + stamped.stderr
    upgraded = _run_alembic(
        schema,
        "upgrade",
        "20260829100000_activate_import_run_idempotency_scope",
    )
    assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
    assert (
        await _index_state(
            connection,
            schema,
            "import_run_active_idempotency_idx",
        )
        is None
    )


async def _assert_failed_restore_cleanup(connection, schema: str) -> None:
    await connection.execute(
        f"""
        INSERT INTO "{schema}".import_run (importer, idempotency_key, status)
        VALUES ('hospital-prices', 'shared', 'running'),
               ('npi', 'shared', 'running')
        """
    )
    failed = _run_alembic(
        schema,
        "downgrade",
        "20260829090000_import_run_idempotency_scope",
    )
    assert failed.returncode != 0
    assert (
        await _index_state(
            connection,
            schema,
            "import_run_active_idempotency_idx",
        )
        is None
    )
    composite_state = await _index_state(
        connection,
        schema,
        "import_run_importer_active_idempotency_idx",
    )
    assert tuple(composite_state) == ("import_run", True, True, True)
    assert (
        await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version')
        == "20260829100000_activate_import_run_idempotency_scope"
    )


async def _assert_restore_recovery(connection, schema: str) -> None:
    await connection.execute(f'DELETE FROM "{schema}".import_run')
    downgraded = _run_alembic(
        schema,
        "downgrade",
        "20260829090000_import_run_idempotency_scope",
    )
    assert downgraded.returncode == 0, downgraded.stdout + downgraded.stderr
    global_state = await _index_state(
        connection,
        schema,
        "import_run_active_idempotency_idx",
    )
    assert tuple(global_state) == ("import_run", True, True, True)
    assert (
        await _index_state(
            connection,
            schema,
            "import_run_importer_active_idempotency_idx",
        )
        is not None
    )


async def test_activation_migration_cleans_failed_global_restore():
    """Run the activation and failed downgrade against real PostgreSQL."""

    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.lower():
        pytest.skip("activation migration requires a disposable test database")
    schema = f"idempotency_activation_{uuid.uuid4().hex[:12]}"
    connection = await asyncpg.connect(
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        database=database,
    )
    try:
        await _create_activation_schema(connection, schema)
        await _assert_activation_upgrade(connection, schema)
        await _assert_failed_restore_cleanup(connection, schema)
        await _assert_restore_recovery(connection, schema)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


async def test_active_idempotency_key_is_unique_per_importer(
    monkeypatch,
):
    """Keep cross-import keys independent across races and terminal reuse."""

    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(control_imports, "_enqueue_selected_import", _fake_enqueue)
        first, first_created = await _create_run("run_first", "nucc")
        assert first_created is True and first["run_id"] == "run_first"

        cross_importer, cross_created = await _create_run(
            "run_cross_importer",
            "npi",
        )
        assert cross_created is True
        assert cross_importer["run_id"] == "run_cross_importer"
        await _assert_active_index_definition()

        real_find, real_find_importer = await _assert_same_importer_integrity_recovery(monkeypatch)
        await db.execute(
            update(ImportRun)
            .where(ImportRun.run_id == "run_first")
            .values(status="succeeded", finished_at=control_imports.utc_now())
        )
        monkeypatch.setattr(
            control_imports,
            "find_active_run_by_idempotency_key",
            real_find,
        )
        monkeypatch.setattr(
            control_imports,
            "find_earliest_active_run_by_importer",
            real_find_importer,
        )

        after_terminal, created = await _create_run("run_after_terminal", "nucc")
        assert created is True
        assert after_terminal["run_id"] == "run_after_terminal"
        import_runs = (await db.execute(select(ImportRun).order_by(ImportRun.run_id))).scalars().all()
        assert [run.run_id for run in import_runs] == [
            "run_after_terminal",
            "run_cross_importer",
            "run_first",
        ]
    finally:
        await _drop_import_run_schema()


MEMBERSHIP_REVISION = "20261021110000_ptg_membership_import_idempotency"
MEMBERSHIP_PREVIOUS_REVISION = "20261020200000_custom_import_manual_acceptance"
MEMBERSHIP_INDEX = "import_run_ptg_membership_idempotency_idx"


@asynccontextmanager
async def _migration_schema():
    _require_disposable_database()
    schema = "membership_idempotency_" + uuid.uuid4().hex
    connection = await asyncpg.connect(
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        user=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD", ""),
        database=os.environ["HLTHPRT_DB_DATABASE"],
    )
    is_created = False
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        is_created = True
        await connection.execute(
            f'CREATE TABLE "{schema}".import_run '
            "(run_id text PRIMARY KEY, importer text NOT NULL, "
            "idempotency_key text, status text NOT NULL)"
        )
        await connection.execute(f'CREATE INDEX existing_status_idx ON "{schema}".import_run (status)')
        stamped = _run_alembic(schema, "stamp", MEMBERSHIP_PREVIOUS_REVISION)
        assert stamped.returncode == 0, stamped.stdout + stamped.stderr
        yield connection, schema
    finally:
        try:
            if is_created:
                await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        finally:
            await connection.close()


async def _indexes(connection, schema):
    return await connection.fetch(
        "SELECT indexrelid, pg_get_indexdef(indexrelid), indisvalid, indisready, indislive "
        "FROM pg_index WHERE indrelid = to_regclass($1) ORDER BY indexrelid",
        f'"{schema}".import_run',
    )


async def _insert(connection, schema, *, run_id, importer=MEMBERSHIP_IMPORTER, key="shared", status="succeeded"):
    await connection.execute(
        f'INSERT INTO "{schema}".import_run (run_id, importer, idempotency_key, status) VALUES ($1,$2,$3,$4)',
        run_id,
        importer,
        key,
        status,
    )


async def _assert_all_statuses_unique(connection, schema):
    statuses = sorted(ACTIVE_STATUSES | TERMINAL_STATUSES)
    for existing_status in statuses:
        await _insert(connection, schema, run_id=existing_status, key=existing_status, status=existing_status)
        for new_status in statuses:
            with pytest.raises(asyncpg.UniqueViolationError) as rejected:
                await _insert(connection, schema, run_id="duplicate", key=existing_status, status=new_status)
            assert rejected.value.constraint_name == MEMBERSHIP_INDEX
    await _insert(connection, schema, run_id="distinct", key="distinct")
    with pytest.raises(asyncpg.UniqueViolationError) as rejected:
        await connection.execute(
            f'UPDATE "{schema}".import_run SET idempotency_key=$1 WHERE run_id=$2', statuses[0], "distinct"
        )
    assert rejected.value.constraint_name == MEMBERSHIP_INDEX


async def _assert_unrelated_runs_permitted(connection, schema):
    for status in sorted(ACTIVE_STATUSES | TERMINAL_STATUSES):
        for occurrence in range(2):
            suffix = f"{status}-{occurrence}"
            await _insert(connection, schema, run_id="null-" + suffix, key=None, status=status)
            await _insert(connection, schema, run_id="other-" + suffix, importer="npi", status=status)
    await _insert(connection, schema, run_id="shared-membership")


async def test_membership_migration_enforces_all_status_identity_and_reversible_index_scope():
    async with _migration_schema() as (connection, schema):
        before = await _indexes(connection, schema)
        upgraded = _run_alembic(schema, "upgrade", MEMBERSHIP_REVISION)
        assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
        assert await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version') == MEMBERSHIP_REVISION
        after = await _indexes(connection, schema)
        assert [row for row in after if row in before] == before
        assert len(after) == len(before) + 1
        assert tuple(next(row for row in after if row not in before)[2:]) == (True, True, True)
        await _assert_all_statuses_unique(connection, schema)
        await _assert_unrelated_runs_permitted(connection, schema)
        rows = await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id')

        downgraded = _run_alembic(schema, "downgrade", MEMBERSHIP_PREVIOUS_REVISION)
        assert downgraded.returncode == 0, downgraded.stdout + downgraded.stderr
        assert (
            await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version')
            == MEMBERSHIP_PREVIOUS_REVISION
        )
        assert await _indexes(connection, schema) == before
        assert await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id') == rows
        await _insert(connection, schema, run_id="after-downgrade")
        with pytest.raises(asyncpg.UniqueViolationError) as rejected:
            await _insert(connection, schema, run_id="after-downgrade", key="new-key")
        assert rejected.value.constraint_name == "import_run_pkey"


async def test_membership_migration_adopts_exact_existing_index_without_rebuilding():
    async with _migration_schema() as (connection, schema):
        upgraded = _run_alembic(schema, "upgrade", MEMBERSHIP_REVISION)
        assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
        await _insert(connection, schema, run_id="existing")
        before = await _indexes(connection, schema)
        rows = await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id')
        stamped = _run_alembic(schema, "stamp", MEMBERSHIP_PREVIOUS_REVISION)
        assert stamped.returncode == 0, stamped.stdout + stamped.stderr

        adopted = _run_alembic(schema, "upgrade", MEMBERSHIP_REVISION)

        assert adopted.returncode == 0, adopted.stdout + adopted.stderr
        assert await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version') == MEMBERSHIP_REVISION
        assert await _indexes(connection, schema) == before
        assert await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id') == rows
        with pytest.raises(asyncpg.UniqueViolationError) as rejected:
            await _insert(connection, schema, run_id="duplicate")
        assert rejected.value.constraint_name == MEMBERSHIP_INDEX


@pytest.mark.parametrize(
    "definition",
    [
        "INDEX {index} ON {table} (importer,idempotency_key) WHERE {predicate}",
        "UNIQUE INDEX {index} ON {table} (importer,idempotency_key,status) WHERE {predicate}",
        "UNIQUE INDEX {index} ON {table} (importer,idempotency_key) WHERE {predicate} AND status='queued'",
    ],
)
async def test_membership_migration_rejects_mismatched_existing_index(definition):
    async with _migration_schema() as (connection, schema):
        await connection.execute(
            "CREATE "
            + definition.format(
                index=MEMBERSHIP_INDEX,
                table=f'"{schema}".import_run',
                predicate="importer = 'ptg-provider-membership-projection' AND idempotency_key IS NOT NULL",
            )
        )
        before = await _indexes(connection, schema)

        rejected = _run_alembic(schema, "upgrade", MEMBERSHIP_REVISION)

        assert rejected.returncode != 0
        assert f"existing_schema_index_mismatch:{schema}.{MEMBERSHIP_INDEX}" in rejected.stderr
        assert (
            await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version')
            == MEMBERSHIP_PREVIOUS_REVISION
        )
        assert await _indexes(connection, schema) == before


async def test_membership_migration_rejects_invalid_index_without_replacing_it():
    async with _migration_schema() as (connection, schema):
        await _insert(connection, schema, run_id="first")
        await _insert(connection, schema, run_id="duplicate")
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY {MEMBERSHIP_INDEX} ON "{schema}".import_run (importer,idempotency_key) '
                "WHERE importer = 'ptg-provider-membership-projection' AND idempotency_key IS NOT NULL"
            )
        before = await _indexes(connection, schema)
        assert any(not row[2] for row in before)
        rows = await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id')

        rejected = _run_alembic(schema, "upgrade", MEMBERSHIP_REVISION)

        assert rejected.returncode != 0
        assert f"existing_schema_index_invalid:{schema}.{MEMBERSHIP_INDEX}" in rejected.stderr
        assert (
            await connection.fetchval(f'SELECT version_num FROM "{schema}".alembic_version')
            == MEMBERSHIP_PREVIOUS_REVISION
        )
        assert await _indexes(connection, schema) == before
        assert await connection.fetch(f'SELECT * FROM "{schema}".import_run ORDER BY run_id') == rows
