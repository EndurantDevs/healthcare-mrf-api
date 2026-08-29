# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL rollback proofs for the projection-v3 census."""

from __future__ import annotations

import asyncio
from functools import partial
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import plan_pricing_projection_v3_census_support as support
from scripts.research import (
    plan_pricing_projection_v3_census_transaction as transaction,
)
from tests.test_plan_pricing_projection_v3_differential_postgres import (
    _insert_candidate,
    migrated_v3_database,
)

_RUN_TOKEN = "a" * 12


async def _run_blocked_statement(session, backend_pids, query_started) -> None:
    """Run one marked statement until the rollback owner cancels it."""

    await transaction.set_census_database_stage(
        session,
        _RUN_TOKEN,
        "price_hydration",
        transaction.census_database_application_name(_RUN_TOKEN, "setup"),
    )
    backend_pids.append(int(await session.scalar(text("SELECT pg_backend_pid()"))))
    query_started.set()
    await session.execute(text("SELECT pg_sleep(60)"))
    raise AssertionError("cancelled database statement returned")


def _assert_rollback_receipt(receipt_by_field: dict) -> None:
    backend_pid = receipt_by_field["database_backend_pid"]
    assert type(backend_pid) is int and backend_pid > 0
    assert receipt_by_field == {
        "database_run_token": _RUN_TOKEN,
        "database_backend_pid": backend_pid,
        "database_session_settings": (
            transaction.expected_census_database_settings(_RUN_TOKEN)
        ),
        "rollback_complete": True,
        "temporary_relations_after_rollback": [],
    }


async def _configure_census_database(
    monkeypatch,
    database,
    projection_id: str,
) -> None:
    async with database.engine.begin() as connection:
        await _insert_candidate(connection, database.schema, projection_id)
    session_factory = async_sessionmaker(
        database.engine,
        expire_on_commit=False,
        autoflush=False,
    )
    monkeypatch.setattr(transaction, "db", SimpleNamespace(session=session_factory))

    async def lock_repeatable_read(session) -> None:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))

    monkeypatch.setattr(transaction, "lock_provider_generation", lock_repeatable_read)


async def _temporary_relation_totals(session) -> tuple[int, int]:
    """Return exact table-only and all-relation temporary bytes."""

    size_result = await session.execute(text("""
            SELECT COALESCE(SUM(pg_total_relation_size(oid))
                                FILTER (WHERE relkind = 'r'), 0)::bigint
                       AS table_total,
                   COALESCE(SUM(pg_total_relation_size(oid)), 0)::bigint
                       AS all_relation_total
              FROM pg_class
             WHERE relnamespace = pg_my_temp_schema()
            """))
    size_by_field = size_result.mappings().one()
    return (
        int(size_by_field["table_total"]),
        int(size_by_field["all_relation_total"]),
    )


async def _census_operation(
    session,
    database,
    projection_id: str,
    persistent_write: bool,
):
    await session.execute(
        text(
            "INSERT INTO plan_pricing_provider_set_stage "
            "(binding_ordinal, provider_set_key, provider_set_id, membership_count) "
            "VALUES (1, 2, 'set_2', 3)"
        )
    )
    if persistent_write:
        await session.execute(
            text(f"""DELETE FROM "{database.schema}".
                    plan_pricing_projection_candidate
                   WHERE projection_id = :projection_id"""),
            {"projection_id": projection_id},
        )
    return {"measured": True}


async def _candidate_count(database, projection_id: str) -> int:
    async with database.engine.connect() as connection:
        return int(
            await connection.scalar(
                text(
                    f'SELECT COUNT(*) FROM "{database.schema}".'
                    "plan_pricing_projection_candidate "
                    "WHERE projection_id = :projection_id"
                ),
                {"projection_id": projection_id},
            )
        )


async def _stage_two_code_work(session, projection_id: str) -> dict:
    setup_statements = (
        "INSERT INTO plan_pricing_provider_set_stage VALUES (1, 2, 'set_2', 1)",
        "INSERT INTO plan_pricing_provider_member_stage VALUES (1, 2, 3)",
    )
    for setup_statement in setup_statements:
        await session.execute(text(setup_statement))
    await session.execute(
        text(
            "INSERT INTO plan_pricing_provider_cell_stage VALUES "
            "(:projection_id, '10001', 3, 2, ARRAY[]::varchar[], :fragment)"
        ),
        {"projection_id": projection_id, "fragment": b"{}"},
    )
    work_rows_list = []
    for ordinal, code_identity in enumerate(
        (("CPT", "27447"), ("HCPCS", "A0001")), start=1
    ):
        price_set_id = str(ordinal) * 32
        await session.execute(
            text(
                "TRUNCATE plan_pricing_code_occurrence_stage, "
                "plan_pricing_price_rate_stage"
            )
        )
        await session.execute(
            text(
                "INSERT INTO plan_pricing_code_occurrence_stage "
                "VALUES (1, 2, :price_set_id, 1)"
            ),
            {"price_set_id": price_set_id},
        )
        await session.execute(
            text(
                "INSERT INTO plan_pricing_price_rate_stage "
                "VALUES (1, :price_set_id, :rate, 1)"
            ),
            {"price_set_id": price_set_id, "rate": ordinal},
        )
        work = await census.projection._stage_code_work(
            session, projection_id, code_identity, 10, 10
        )
        work_rows_list.append(
            (work.membership_probe_rows, work.member_cell_rows, work.set_cell_rows)
        )
    return {"measured": work_rows_list}


@pytest.mark.asyncio
@pytest.mark.parametrize("persistent_write", (False, True))
async def test_census_transaction_always_rolls_back_temp_and_persistent_rows(
    monkeypatch,
    migrated_v3_database,
    persistent_write: bool,
) -> None:
    database = migrated_v3_database
    projection_id = "c" * 64
    await _configure_census_database(monkeypatch, database, projection_id)

    async def operation(session):
        return await _census_operation(
            session, database, projection_id, persistent_write
        )

    receipt_by_field: dict = {}
    assert await transaction.rollback_only(
        receipt_by_field,
        operation,
        run_token=_RUN_TOKEN,
    ) == {"measured": True}
    _assert_rollback_receipt(receipt_by_field)
    assert await _candidate_count(database, projection_id) == 1


@pytest.mark.asyncio
async def test_census_transaction_runs_two_real_code_stage_resets(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "e" * 64
    await _configure_census_database(monkeypatch, database, projection_id)

    receipt_by_field: dict = {}
    measured_result = await transaction.rollback_only(
        receipt_by_field,
        lambda session: _stage_two_code_work(session, projection_id),
        run_token=_RUN_TOKEN,
    )
    assert measured_result == {"measured": [(1, 1, 0), (1, 1, 1)]}
    _assert_rollback_receipt(receipt_by_field)


@pytest.mark.asyncio
async def test_census_stage_samples_own_backend_and_temp_relations(
    monkeypatch,
    migrated_v3_database,
) -> None:
    """Measure only table-owned temporary bytes on the census backend."""

    database = migrated_v3_database
    projection_id = "b" * 64
    await _configure_census_database(monkeypatch, database, projection_id)

    async def operation(session):
        setup_name = transaction.census_database_application_name(
            _RUN_TOKEN,
            "setup",
        )
        before = await transaction.set_census_database_stage(
            session,
            _RUN_TOKEN,
            "reset_code_work",
            setup_name,
        )
        await session.execute(
            text(
                "INSERT INTO plan_pricing_provider_set_stage "
                "VALUES (1, 2, 'set_2', 3)"
            )
        )
        after = await transaction.set_census_database_stage(
            session,
            _RUN_TOKEN,
            "membership_probe",
            str(before["application_name"]),
        )
        table_total, all_relation_total = await _temporary_relation_totals(session)
        return before, after, table_total, all_relation_total

    receipt_by_field: dict = {}
    before, after, table_total, all_relation_total = await transaction.rollback_only(
        receipt_by_field,
        operation,
        run_token=_RUN_TOKEN,
    )

    assert before["backend_pid"] == after["backend_pid"]
    assert before["backend_pid"] == receipt_by_field["database_backend_pid"]
    assert int(after["backend_memory_context_bytes"]) > 0
    assert int(after["temporary_relation_bytes"]) == table_total
    assert all_relation_total > table_total
    _assert_rollback_receipt(receipt_by_field)


@pytest.mark.asyncio
async def test_census_measures_exact_single_price_key_atom_peak(
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    async with database.engine.begin() as connection:
        await census.projection._create_stage_tables(connection)
        await connection.execute(text("""
                INSERT INTO plan_pricing_price_rate_stage VALUES
                    (0, 'a', 10, 3),
                    (0, 'a', 20, 4),
                    (0, 'b', 30, 5)
                """))
        measured_result = await connection.execute(
            text(census._STAGED_PRICE_METRICS_SQL)
        )

    assert dict(measured_result.mappings().one()) == {
        "staged_price_atom_membership_rows": 12,
        "maximum_price_key_atom_membership_rows": 7,
    }


@pytest.mark.asyncio
async def test_census_measures_exact_provider_stage_counts(
    migrated_v3_database,
) -> None:
    async with migrated_v3_database.engine.begin() as connection:
        await census.projection._create_stage_tables(connection)
        await connection.execute(
            text(
                "INSERT INTO plan_pricing_provider_set_stage VALUES "
                "(0, 1, 'set_1', 2), (0, 2, 'set_2', 0)"
            )
        )
        await connection.execute(
            text(
                "INSERT INTO plan_pricing_provider_member_stage VALUES "
                "(0, 1, 1), (0, 1, 2)"
            )
        )
        await connection.execute(
            text(
                "INSERT INTO plan_pricing_provider_cell_stage VALUES "
                "('projection', '10001', 1, 2, ARRAY[]::varchar[], :fragment)"
            ),
            {"fragment": b"{}"},
        )
        await connection.execute(
            text("INSERT INTO plan_pricing_provider_npi_materialized_stage VALUES (1)")
        )
        measured_counts = await census._projection_stage_counts(connection)

    assert measured_counts == {
        "provider_set_count": 2,
        "provider_membership_count": 2,
        "maximum_provider_set_membership_count": 2,
        "provider_cell_count": 1,
        "provider_fragment_byte_count": 2,
        "provider_npi_count": 1,
        "pending_npi_count": 0,
        "referenced_empty_provider_set_count": 1,
    }


@pytest.mark.asyncio
async def test_census_transaction_rolls_back_after_repeated_cancellation(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    projection_id = "d" * 64
    await _configure_census_database(monkeypatch, database, projection_id)
    operation_entered = asyncio.Event()
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    finish_rollback = transaction._finish_rollback

    async def gated_finish_rollback(session, active_transaction, receipt):
        cleanup_entered.set()
        await cleanup_release.wait()
        await finish_rollback(session, active_transaction, receipt)

    monkeypatch.setattr(transaction, "_finish_rollback", gated_finish_rollback)

    async def timed_out_operation(session):
        await _census_operation(session, database, projection_id, True)
        operation_entered.set()
        await asyncio.Event().wait()

    receipt_by_field: dict = {}
    census_task = asyncio.create_task(
        transaction.rollback_only(
            receipt_by_field,
            timed_out_operation,
            run_token=_RUN_TOKEN,
        )
    )
    await asyncio.wait_for(operation_entered.wait(), timeout=1)
    census_task.cancel()
    await asyncio.wait_for(cleanup_entered.wait(), timeout=1)
    census_task.cancel()
    census_task.cancel()
    await asyncio.sleep(0)
    assert not census_task.done()
    cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(census_task, timeout=2)

    _assert_rollback_receipt(receipt_by_field)
    assert await _candidate_count(database, projection_id) == 1


@pytest.mark.asyncio
async def test_census_cancellation_stops_marked_postgresql_statement(
    monkeypatch,
    migrated_v3_database,
) -> None:
    """Cancel the marked backend statement before rollback completes."""

    database = migrated_v3_database
    projection_id = "a" * 64
    await _configure_census_database(monkeypatch, database, projection_id)
    backend_pids = []
    query_started = asyncio.Event()

    receipt_by_field: dict = {}
    census_task = asyncio.create_task(
        transaction.rollback_only(
            receipt_by_field,
            partial(
                _run_blocked_statement,
                backend_pids=backend_pids,
                query_started=query_started,
            ),
            run_token=_RUN_TOKEN,
        )
    )
    await asyncio.wait_for(query_started.wait(), timeout=2)
    assert len(backend_pids) == 1
    application_name = transaction.census_database_application_name(
        _RUN_TOKEN, "price_hydration"
    )
    async with database.engine.connect() as observer:
        async with asyncio.timeout(2):
            while not await observer.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                    "WHERE pid = :pid AND state = 'active' "
                    "AND application_name = :application_name "
                    "AND query LIKE '%pg_sleep(60)%')"
                ),
                {
                    "pid": backend_pids[0],
                    "application_name": application_name,
                },
            ):
                await asyncio.sleep(0.01)

    census_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(census_task, timeout=5)

    async with database.engine.connect() as observer:
        assert not await observer.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                "WHERE pid = :pid AND (state = 'active' "
                "OR application_name LIKE 'hp-pv3-census:%'))"
            ),
            {"pid": backend_pids[0]},
        )
    _assert_rollback_receipt(receipt_by_field)
    assert await _candidate_count(database, projection_id) == 1


@pytest.mark.asyncio
async def test_census_postflight_drains_repeated_cancellation(
    monkeypatch,
    migrated_v3_database,
) -> None:
    database = migrated_v3_database
    session_factory = async_sessionmaker(
        database.engine,
        expire_on_commit=False,
        autoflush=False,
    )

    async def no_lock(_session):
        return None

    monkeypatch.setattr(support, "db", SimpleNamespace(session=session_factory))
    monkeypatch.setattr(support, "lock_provider_generation", no_lock)
    operation_entered = asyncio.Event()
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    finish_rollback = support._finish_postflight_rollback

    async def blocked_release_input(_session, _plan_release_id):
        operation_entered.set()
        await asyncio.Event().wait()

    async def gated_finish_rollback(session, active_transaction):
        cleanup_entered.set()
        await cleanup_release.wait()
        await finish_rollback(session, active_transaction)

    monkeypatch.setattr(support, "locked_release_input", blocked_release_input)
    monkeypatch.setattr(support, "_finish_postflight_rollback", gated_finish_rollback)
    postflight_task = asyncio.create_task(
        support.postflight("hprelease_test", {"projection_id": "f" * 64})
    )
    await asyncio.wait_for(operation_entered.wait(), timeout=1)
    postflight_task.cancel()
    await asyncio.wait_for(cleanup_entered.wait(), timeout=1)
    postflight_task.cancel()
    cleanup_release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(postflight_task, timeout=2)
