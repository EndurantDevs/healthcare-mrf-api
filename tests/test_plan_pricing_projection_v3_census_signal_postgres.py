# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real PostgreSQL process-signal proof for the projection-v3 census."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from scripts.research import (
    plan_pricing_projection_v3_census_transaction as transaction,
)
from tests.test_plan_pricing_projection_postgres import (
    POSTGRES_DSN_ENV,
    TEST_DATABASE_PATTERN,
    _sqlalchemy_async_dsn,
)

asyncpg = pytest.importorskip("asyncpg")
_RECEIPT_ENV = "HLTHPRT_TEST_CENSUS_SIGNAL_RECEIPT"
_RUN_TOKEN_ENV = "HLTHPRT_TEST_CENSUS_SIGNAL_RUN_TOKEN"
_SCHEMA_ENV = "HLTHPRT_TEST_CENSUS_SIGNAL_SCHEMA"
_QUERY_MARKER = "signal-cancel-proof"


async def _blocked_census_statement(session, run_token: str) -> dict:
    setup_name = transaction.census_database_application_name(run_token, "setup")
    await transaction.set_census_database_stage(
        session,
        run_token,
        "price_hydration",
        setup_name,
        1,
    )
    schema = os.environ[_SCHEMA_ENV]
    await session.execute(text(f'UPDATE "{schema}".marker SET value = 2'))
    await session.execute(text(f"/* {_QUERY_MARKER} */ SELECT pg_sleep(60)"))
    raise AssertionError("cancelled database statement returned")


def _run_signal_census_child() -> None:
    """Run the signal-aware wrapper in a real child process."""

    dsn = os.environ[POSTGRES_DSN_ENV]
    run_token = os.environ[_RUN_TOKEN_ENV]
    receipt_path = Path(os.environ[_RECEIPT_ENV])
    engine = create_async_engine(_sqlalchemy_async_dsn(dsn), poolclass=NullPool)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )
    transaction.db = SimpleNamespace(session=session_factory)

    async def no_lock(_session) -> None:
        return None

    async def runner(_args, receipt_by_field) -> int:
        await transaction.rollback_only(
            receipt_by_field,
            lambda session: _blocked_census_statement(session, run_token),
            run_token=run_token,
        )
        return 0

    transaction.lock_provider_generation = no_lock
    exit_code = diagnostics.run_census_process(
        SimpleNamespace(receipt=receipt_path),
        {},
        runner,
        lambda _args: {},
    )
    raise SystemExit(exit_code)


async def _start_signal_child(
    receipt_path: Path,
    run_token: str,
    schema: str,
) -> asyncio.subprocess.Process:
    child_env_by_name = {
        **os.environ,
        _RECEIPT_ENV: str(receipt_path),
        _RUN_TOKEN_ENV: run_token,
        _SCHEMA_ENV: schema,
    }
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        (
            "from tests.test_plan_pricing_projection_v3_census_signal_postgres "
            "import _run_signal_census_child; _run_signal_census_child()"
        ),
        env=child_env_by_name,
        cwd=Path(__file__).resolve().parents[1],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _wait_for_marked_backend(
    observer,
    process: asyncio.subprocess.Process,
    application_name: str,
) -> int:
    async with asyncio.timeout(30):
        while True:
            backend_pid = await observer.fetchval(
                "SELECT pid FROM pg_stat_activity "
                "WHERE application_name = $1 AND state = 'active' "
                "AND query LIKE '%' || $2 || '%'",
                application_name,
                _QUERY_MARKER,
            )
            if backend_pid is not None:
                return int(backend_pid)
            if process.returncode is not None:
                _stdout, stderr = await process.communicate()
                pytest.fail(
                    f"signal child exited {process.returncode} before PostgreSQL "
                    f"became active: {stderr.decode(errors='replace')}"
                )
            await asyncio.sleep(0.01)


async def _wait_for_backend_exit(observer, backend_pid: int) -> None:
    async with asyncio.timeout(5):
        while await observer.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)",
            backend_pid,
        ):
            await asyncio.sleep(0.01)


async def _connect_test_database():
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    observer = await asyncpg.connect(dsn)
    database_name = await observer.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await observer.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    return dsn, observer


@pytest.mark.asyncio
async def test_sigterm_cancels_sql_rolls_back_and_seals_receipt(tmp_path: Path) -> None:
    """SIGTERM must drain the active backend before sealing exit 143."""

    dsn, observer = await _connect_test_database()

    receipt_path = tmp_path / "signal-receipt.json"
    run_token = uuid.uuid4().hex[:12]
    application_name = transaction.census_database_application_name(
        run_token,
        "price_hydration",
        1,
    )
    schema = f"census_signal_{uuid.uuid4().hex}"
    backend_pid = None
    persistent_value = None
    process = None
    try:
        await observer.execute(f'CREATE SCHEMA "{schema}"')
        await observer.execute(
            f'CREATE TABLE "{schema}".marker (value integer NOT NULL)'
        )
        await observer.execute(f'INSERT INTO "{schema}".marker VALUES (1)')
        process = await _start_signal_child(receipt_path, run_token, schema)
        backend_pid = await _wait_for_marked_backend(
            observer,
            process,
            application_name,
        )
        process.send_signal(signal.SIGTERM)
        await asyncio.wait_for(process.communicate(), timeout=10)
        await _wait_for_backend_exit(observer, backend_pid)
        persistent_value = await observer.fetchval(
            f'SELECT value FROM "{schema}".marker'
        )
    finally:
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        await observer.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await observer.close()

    assert (process.returncode, persistent_value) == (143, 1)
    receipt_text = receipt_path.read_text(encoding="utf-8")
    receipt_by_field = json.loads(receipt_text)
    assert receipt_by_field["database_backend_pid"] == backend_pid
    assert receipt_by_field["status"] == "failed"
    assert receipt_by_field["rollback_complete"] is True
    assert receipt_by_field["temporary_relations_after_rollback"] == []
    assert "rollback_error" not in receipt_by_field
    assert "rollback_task_error" not in receipt_by_field
    assert receipt_by_field["error"] == {
        "type": "_CensusInterrupted",
        "signal": "SIGTERM",
    }
    assert receipt_by_field["accepted"] is False
    assert receipt_by_field["cap_calibration_admissible"] is False
    assert receipt_by_field["resource_proof_admissible"] is False
    assert _QUERY_MARKER not in receipt_text
    assert schema not in receipt_text
    assert dsn not in receipt_text
