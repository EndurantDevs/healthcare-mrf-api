"""PostgreSQL transaction proof for PTG source-attempt admission."""

from __future__ import annotations

import datetime as dt
import json

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process.ptg_parts import ptg_source_attempt_actions as source_actions
from process.ptg_parts.ptg_source_worker_admission import (
    guard_ptg_worker_start,
)
from tests.ptg2_legacy_v3_reconcile_postgres_support import (
    OUTER_RUN_ID,
    SOURCE_IMPORT_ID,
    LegacyV3PostgresContext,
    legacy_v3_postgres_context,
    seed_ready_v3_target,
)


async def _make_outer_run_active(context: LegacyV3PostgresContext) -> None:
    await context.connection.execute(
        f"UPDATE {context.schema}.import_run "
        "SET status = 'running', phase_detail = 'active' "
        "WHERE run_id = $1",
        OUTER_RUN_ID,
    )


async def _outer_state(context: LegacyV3PostgresContext) -> asyncpg.Record:
    return await context.connection.fetchrow(
        f"SELECT status, phase_detail, progress, heartbeat_at "
        f"FROM {context.schema}.import_run WHERE run_id = $1",
        OUTER_RUN_ID,
    )


async def _event_count(context: LegacyV3PostgresContext) -> int:
    return int(
        await context.connection.fetchval(
            f"SELECT COUNT(*) FROM {context.schema}.ptg_source_attempt_event"
        )
    )


@pytest.mark.asyncio
async def test_action_update_and_event_commit_atomically(monkeypatch) -> None:
    """Commit the locked outer-state update with its admission event."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        await _make_outer_run_active(context)
        event_count_before = await _event_count(context)
        heartbeat_at = dt.datetime(2026, 8, 1, 1, 2, 3)
        admitted = await source_actions.admit_existing_outer_run_action(
            run_id=OUTER_RUN_ID,
            event_kind="ensure_admitted",
            expected_source_file_import_id=SOURCE_IMPORT_ID,
            state_updates={
                "status": "finalizing",
                "phase_detail": "ensure admitted",
                "heartbeat_at": heartbeat_at,
                "progress": {"done": 0, "total": 1},
                "import_id": SOURCE_IMPORT_ID,
            },
        )

        stored_state = await _outer_state(context)
        latest_event = await context.connection.fetchrow(
            f"SELECT source_file_import_id, event_kind, outer_run_id "
            f"FROM {context.schema}.ptg_source_attempt_event "
            "ORDER BY event_id DESC LIMIT 1"
        )
        assert admitted["status"] == "finalizing"
        assert tuple(stored_state[:2]) == ("finalizing", "ensure admitted")
        assert json.loads(stored_state[2]) == {"done": 0, "total": 1}
        assert stored_state[3] == heartbeat_at
        assert await _event_count(context) == event_count_before + 1
        assert tuple(latest_event) == (
            SOURCE_IMPORT_ID,
            "ensure_admitted",
            OUTER_RUN_ID,
        )


async def _install_event_insert_failure(
    context: LegacyV3PostgresContext,
) -> None:
    await context.connection.execute(
        f"""
        CREATE FUNCTION {context.schema}.reject_synthetic_event()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'SYNTHETIC_EVENT_INSERT_FAILURE';
        END;
        $$;
        CREATE TRIGGER reject_synthetic_event
        BEFORE INSERT ON {context.schema}.ptg_source_attempt_event
        FOR EACH ROW EXECUTE FUNCTION {context.schema}.reject_synthetic_event();
        """
    )


@pytest.mark.asyncio
async def test_event_insert_failure_rolls_back_outer_state(monkeypatch) -> None:
    """Roll back the state transition when durable event insertion fails."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        await _make_outer_run_active(context)
        state_before = await _outer_state(context)
        event_count_before = await _event_count(context)
        await _install_event_insert_failure(context)

        with pytest.raises(
            DBAPIError,
            match="SYNTHETIC_EVENT_INSERT_FAILURE",
        ):
            await source_actions.admit_existing_outer_run_action(
                run_id=OUTER_RUN_ID,
                event_kind="ensure_admitted",
                expected_source_file_import_id=SOURCE_IMPORT_ID,
                state_updates={
                    "status": "finalizing",
                    "phase_detail": "must roll back",
                },
            )

        assert await _outer_state(context) == state_before
        assert await _event_count(context) == event_count_before


@pytest.mark.asyncio
async def test_worker_start_accepts_exact_source_identity(monkeypatch) -> None:
    """Admit the production worker boundary for the exact durable source."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        await _make_outer_run_active(context)
        event_count_before = await _event_count(context)

        outcome = await guard_ptg_worker_start(
            {"source_file_import_id": SOURCE_IMPORT_ID},
            run_id=OUTER_RUN_ID,
            attempt_id=f"{OUTER_RUN_ID}:synthetic-attempt",
        )

        assert outcome is None
        assert await _event_count(context) == event_count_before + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_payload",
    (
        {},
        {"source_file_import_id": "different-synthetic-source"},
    ),
)
async def test_worker_start_rejects_missing_or_conflicting_identity(
    monkeypatch,
    task_payload,
) -> None:
    """Fail closed before work when queued and durable identities differ."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        await _make_outer_run_active(context)
        event_count_before = await _event_count(context)

        outcome = await guard_ptg_worker_start(
            task_payload,
            run_id=OUTER_RUN_ID,
            attempt_id=f"{OUTER_RUN_ID}:synthetic-attempt",
        )

        assert outcome == {
            "status": "skipped",
            "reason": "source_attempt_identity_mismatch",
            "run_id": OUTER_RUN_ID,
        }
        assert await _event_count(context) == event_count_before
