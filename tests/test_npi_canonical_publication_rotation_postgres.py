# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-transaction race and rollback proof for canonical NPI rotation."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import NamedTuple

import asyncpg
import pytest

from process.npi_canonical_publication import (
    NpiCanonicalPublicationError,
    NpiCanonicalPublicationInput,
    receipt_metrics,
)
from process.npi_canonical_publication_store import (
    canonical_relation_oids,
    insert_npi_publication_receipt,
    lock_npi_publication_attempt,
    mark_npi_publication_succeeded,
)
from tests.npi_canonical_publication_postgres_support import (
    CANONICAL_TABLES,
    canonical_relation_state,
    create_canonical_stage_tables,
    npi_publication_schema,
    rotate_canonical_stage_tables,
)
from tests.public_evidence_nppes_admission_postgres_support import (
    admit_chain,
    admit_replay,
    finished_chain_receipt,
    prepared_replay,
    qualified,
)
from tests.public_evidence_storage_postgres_support import connect


RUN_ID = "run_npi_rotation_pg"
ATTEMPT_ID = RUN_ID + ":" + "d" * 32
ATTEMPT_STARTED_AT = "2026-08-09T03:04:05.678901+00:00"
STAGE_ROW_COUNTS = (11, 12, 13, 14, 15, 16)


class _RotationSetup(NamedTuple):
    chain_ref: str
    stage_table_by_live: dict[str, str]
    old_state: tuple[tuple[int, ...], tuple[int, ...]]
    stage_oids: tuple[int, ...]


async def _admitted_chain(
    connection: asyncpg.Connection,
    schema_name: str,
    root: Path,
):
    replay = await prepared_replay(root)
    admission = await admit_replay(connection, schema_name, replay)
    return await admit_chain(
        connection,
        schema_name,
        finished_chain_receipt(replay, admission),
    )


async def _insert_running_attempt(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    progress_by_name = {
        "attempt_id": ATTEMPT_ID,
        "attempt_started_at": ATTEMPT_STARTED_AT,
    }
    await connection.execute(
        f"INSERT INTO {qualified(schema_name, 'import_run')} "
        "(run_id, importer, status, phase_detail, heartbeat_at, progress, metrics) "
        "VALUES ($1, 'npi', 'running', 'process_data running', "
        "transaction_timestamp() AT TIME ZONE 'UTC', $2::json, '{}'::json)",
        RUN_ID,
        json.dumps(progress_by_name),
    )


async def _lock_attempt(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    await lock_npi_publication_attempt(
        connection,
        schema=schema_name,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        attempt_started_at=ATTEMPT_STARTED_AT,
    )


async def _finalize_publication(
    connection: asyncpg.Connection,
    schema_name: str,
    chain_ref: str,
    row_counts: tuple[int, ...],
):
    receipt = await insert_npi_publication_receipt(
        connection,
        schema=schema_name,
        publication_input=NpiCanonicalPublicationInput(
            RUN_ID,
            ATTEMPT_ID,
            ATTEMPT_STARTED_AT,
            chain_ref,
            "2026-08-09",
            await canonical_relation_oids(connection, schema=schema_name),
            row_counts,
        ),
    )
    await mark_npi_publication_succeeded(
        connection,
        schema=schema_name,
        receipt=receipt,
        progress_by_name={
            "unit": "rows",
            "done": row_counts[1],
            "total": row_counts[1],
            "pct": 100,
            "phase": "npi published",
            "message": "succeeded",
        },
        metrics_by_name={
            "npi_canonical_publication": receipt_metrics(receipt),
            "nppes_public_evidence": {"chain_ref": chain_ref},
        },
    )
    return receipt


async def _cancel_running_attempt(
    connection: asyncpg.Connection,
    schema_name: str,
) -> str | None:
    return await connection.fetchval(
        f"UPDATE {qualified(schema_name, 'import_run')} SET status='canceling' "
        "WHERE run_id=$1 AND status='running' RETURNING run_id",
        RUN_ID,
    )


async def _relation_oids(
    connection: asyncpg.Connection,
    schema_name: str,
    table_names: tuple[str, ...],
) -> tuple[int, ...]:
    """Resolve exact relation identities for a bounded table vector."""

    relation_oids: list[int] = []
    for table_name in table_names:
        relation_oids.append(await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint",
            f"{schema_name}.{table_name}",
        ))
    return tuple(relation_oids)


async def _wait_for_lock_wait(
    observer: asyncpg.Connection,
    backend_pid: int,
) -> None:
    """Wait until a competing backend is demonstrably blocked on a DB lock."""

    for _poll_ordinal in range(200):
        wait_event_type = await observer.fetchval(
            "SELECT wait_event_type FROM pg_catalog.pg_stat_activity WHERE pid=$1",
            backend_pid,
        )
        if wait_event_type == "Lock":
            return
        await asyncio.sleep(0.01)
    raise AssertionError("publication contender did not enter a database lock wait")


async def _drain_task(task: asyncio.Task | None) -> None:
    """Cancel and retrieve one unfinished race contender during test cleanup."""

    if task is None:
        return
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _assert_committed_rotation(
    connection: asyncpg.Connection,
    schema_name: str,
    old_oids: tuple[int, ...],
    stage_oids: tuple[int, ...],
    stage_tables: tuple[str, ...],
    receipt: object,
) -> None:
    """Require all promoted identities, censuses, archives, and terminal state."""

    live_oids, live_counts = await canonical_relation_state(connection, schema_name)
    assert live_oids == receipt.relation_oids
    assert live_oids == stage_oids and live_oids != old_oids
    assert live_counts == STAGE_ROW_COUNTS
    for table_name, old_oid in zip(CANONICAL_TABLES, old_oids, strict=True):
        assert await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint",
            f"{schema_name}.{table_name}_old",
        ) == old_oid
    for stage_table in stage_tables:
        assert await connection.fetchval(
            "SELECT to_regclass($1)", f"{schema_name}.{stage_table}"
        ) is None
    sealed_run = await connection.fetchrow(
        f"SELECT run.status, run.snapshot_id, count(sealed.publication_ref)::bigint "
        f"FROM {qualified(schema_name, 'npi_canonical_publication_receipt')} receipt "
        f"JOIN {qualified(schema_name, 'npi_canonical_publication_receipt_seal')} "
        "sealed USING (publication_ref) "
        f"JOIN {qualified(schema_name, 'import_run')} run USING (run_id) "
        "WHERE receipt.publication_ref=$1 GROUP BY run.status, run.snapshot_id",
        receipt.publication_ref,
    )
    assert tuple(sealed_run) == (
        "succeeded",
        receipt.publication_ref,
        1,
    )


async def _install_terminal_failure(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Install a test-only failure at the exact terminal control update."""

    await connection.execute(
        f"CREATE FUNCTION {qualified(schema_name, 'fail_npi_terminal')}() "
        "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "IF NEW.status='succeeded' THEN RAISE EXCEPTION 'synthetic terminal failure'; "
        "END IF; RETURN NEW; END $$; "
        f"CREATE TRIGGER fail_npi_terminal BEFORE UPDATE ON "
        f"{qualified(schema_name, 'import_run')} FOR EACH ROW EXECUTE FUNCTION "
        f"{qualified(schema_name, 'fail_npi_terminal')}();"
    )


async def _assert_rolled_back_rotation(
    connection: asyncpg.Connection,
    schema_name: str,
    old_state: tuple[tuple[int, ...], tuple[int, ...]],
    stage_tables: tuple[str, ...],
    stage_oids: tuple[int, ...],
) -> None:
    """Require no live, staged, receipt, seal, or control mutation survived."""

    assert await canonical_relation_state(connection, schema_name) == old_state
    await _assert_stage_relations_exist(
        connection,
        schema_name,
        stage_tables,
        expected_oids=stage_oids,
    )
    for table_name in CANONICAL_TABLES:
        assert await connection.fetchval(
            "SELECT to_regclass($1)", f"{schema_name}.{table_name}_old"
        ) is None
    run_state = await connection.fetchrow(
        f"SELECT status, snapshot_id FROM {qualified(schema_name, 'import_run')} "
        "WHERE run_id=$1",
        RUN_ID,
    )
    assert tuple(run_state) == ("running", None)
    assert await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{qualified(schema_name, 'npi_canonical_publication_receipt')}"
    ) == 0
    assert await connection.fetchval(
        f"SELECT count(*) FROM "
        f"{qualified(schema_name, 'npi_canonical_publication_receipt_seal')}"
    ) == 0


async def _assert_stage_relations_exist(
    connection: asyncpg.Connection,
    schema_name: str,
    stage_tables: tuple[str, ...],
    *,
    expected_oids: tuple[int, ...] | None = None,
) -> None:
    """Require every prepublication stage relation to remain available."""

    if expected_oids is not None:
        assert len(expected_oids) == len(stage_tables)
    for stage_ordinal, stage_table in enumerate(stage_tables):
        relation_oid = await connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint",
            f"{schema_name}.{stage_table}",
        )
        assert relation_oid is not None
        if expected_oids is not None:
            assert relation_oid == expected_oids[stage_ordinal]


async def _prepare_rotation(
    connection: asyncpg.Connection,
    schema_name: str,
    root: Path,
) -> _RotationSetup:
    """Create one admitted chain, running attempt, and six staged relations."""

    chain = await _admitted_chain(connection, schema_name, root)
    await _insert_running_attempt(connection, schema_name)
    stage_table_by_live = await create_canonical_stage_tables(
        connection,
        schema_name,
        STAGE_ROW_COUNTS,
    )
    old_state = await canonical_relation_state(connection, schema_name)
    stage_oids = await _relation_oids(
        connection,
        schema_name,
        tuple(stage_table_by_live.values()),
    )
    return _RotationSetup(
        chain.chain_ref,
        stage_table_by_live,
        old_state,
        stage_oids,
    )


@pytest.mark.asyncio
async def test_publisher_lock_wins_and_commits_six_rotations_atomically(tmp_path):
    """A waiting cancel cannot split the six swaps from receipt success."""

    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        publisher = await connect(database_url)
        canceller = await connect(database_url)
        transaction = None
        cancel_task = None
        try:
            setup = await _prepare_rotation(publisher, schema_name, tmp_path)
            old_oids, old_counts = setup.old_state
            assert old_counts == (1, 2, 3, 4, 5, 6)
            stage_tables = tuple(setup.stage_table_by_live.values())

            transaction = publisher.transaction()
            await transaction.start()
            await _lock_attempt(publisher, schema_name)
            cancel_task = asyncio.create_task(
                _cancel_running_attempt(canceller, schema_name)
            )
            await _wait_for_lock_wait(
                publisher,
                canceller.get_server_pid(),
            )
            await rotate_canonical_stage_tables(
                publisher,
                schema_name,
                setup.stage_table_by_live,
            )
            receipt = await _finalize_publication(
                publisher,
                schema_name,
                setup.chain_ref,
                STAGE_ROW_COUNTS,
            )
            await transaction.commit()
            transaction = None

            assert await asyncio.wait_for(cancel_task, timeout=2) is None
            await _assert_committed_rotation(
                publisher,
                schema_name,
                old_oids,
                setup.stage_oids,
                stage_tables,
                receipt,
            )
        finally:
            if transaction is not None:
                await transaction.rollback()
            await _drain_task(cancel_task)
            await publisher.close()
            await canceller.close()


@pytest.mark.asyncio
async def test_cancel_lock_wins_before_any_canonical_rotation(tmp_path):
    """A publisher blocked behind a committed cancel leaves every stage intact."""

    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        publisher = await connect(database_url)
        canceller = await connect(database_url)
        cancel_transaction = None
        publisher_task = None
        try:
            setup = await _prepare_rotation(publisher, schema_name, tmp_path)
            cancel_transaction = canceller.transaction()
            await cancel_transaction.start()
            assert await _cancel_running_attempt(canceller, schema_name) == RUN_ID

            async def attempt_rotation() -> None:
                async with publisher.transaction():
                    await _lock_attempt(publisher, schema_name)
                    await rotate_canonical_stage_tables(
                        publisher,
                        schema_name,
                        setup.stage_table_by_live,
                    )

            publisher_task = asyncio.create_task(attempt_rotation())
            await _wait_for_lock_wait(
                canceller,
                publisher.get_server_pid(),
            )
            await cancel_transaction.commit()
            cancel_transaction = None
            with pytest.raises(NpiCanonicalPublicationError):
                await asyncio.wait_for(publisher_task, timeout=2)

            assert await canonical_relation_state(publisher, schema_name) == setup.old_state
            await _assert_stage_relations_exist(
                publisher,
                schema_name,
                tuple(setup.stage_table_by_live.values()),
                expected_oids=setup.stage_oids,
            )
            assert await publisher.fetchval(
                f"SELECT count(*) FROM "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt')}"
            ) == 0
            assert await publisher.fetchval(
                f"SELECT status FROM {qualified(schema_name, 'import_run')} "
                "WHERE run_id=$1",
                RUN_ID,
            ) == "canceling"
        finally:
            if cancel_transaction is not None:
                await cancel_transaction.rollback()
            await _drain_task(publisher_task)
            await publisher.close()
            await canceller.close()


@pytest.mark.asyncio
async def test_injected_terminal_failure_rolls_back_all_six_rotations(tmp_path):
    """A terminal update failure restores live OIDs and all staged relations."""

    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            setup = await _prepare_rotation(connection, schema_name, tmp_path)
            stage_tables = tuple(setup.stage_table_by_live.values())
            await _install_terminal_failure(connection, schema_name)
            with pytest.raises(NpiCanonicalPublicationError) as caught:
                async with connection.transaction():
                    await _lock_attempt(connection, schema_name)
                    await rotate_canonical_stage_tables(
                        connection,
                        schema_name,
                        setup.stage_table_by_live,
                    )
                    await _finalize_publication(
                        connection,
                        schema_name,
                        setup.chain_ref,
                        STAGE_ROW_COUNTS,
                    )
            assert str(caught.value) == "npi_canonical_publication_invalid"
            assert "synthetic terminal failure" not in repr(caught.value)

            await _assert_rolled_back_rotation(
                connection,
                schema_name,
                setup.old_state,
                stage_tables,
                setup.stage_oids,
            )
        finally:
            await connection.close()
