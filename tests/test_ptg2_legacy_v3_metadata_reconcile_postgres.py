"""Executable PostgreSQL proof for legacy PTG V3 metadata repair."""

from __future__ import annotations

import asyncio
import json

import asyncpg
import pytest

from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile
from process.ptg_parts import ptg_source_attempt_actions as source_actions
from process.ptg_parts.ptg_source_attempt_guard import source_attempt_lock_key
from process.ptg_parts.ptg_source_attempt_guard import (
    PTGSourceAttemptTerminalError,
)
from tests.ptg2_lifecycle_retry_test_support import settle_lifecycle_outcomes
from tests.ptg2_legacy_v3_reconcile_postgres_support import (
    INTERNAL_RUN_ID,
    OUTER_RUN_ID,
    SNAPSHOT_ID,
    SOURCE_IMPORT_ID,
    LegacyV3PostgresContext,
    legacy_v3_postgres_context,
    operational_absence,
    patch_operational_absence as _patch_operational_absence,
    row_versions,
    seed_ready_v3_target,
    seed_source_event,
    source_options,
)


async def _assert_preplan_rejections(
    context: LegacyV3PostgresContext,
) -> None:
    await context.connection.execute(
        f"INSERT INTO {context.schema}.ptg2_v3_snapshot_scope "
        "(snapshot_id) VALUES ($1)",
        SNAPSHOT_ID,
    )
    duplicate_plan = await reconcile.plan_legacy_v3_metadata_reconcile(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )
    assert "retained_attachment_cardinality_changed" in duplicate_plan[
        "reason_codes"
    ]
    await context.connection.execute(
        f"DELETE FROM {context.schema}.ptg2_v3_snapshot_scope "
        "WHERE ctid IN (SELECT ctid FROM "
        f"{context.schema}.ptg2_v3_snapshot_scope WHERE snapshot_id = $1 "
        "LIMIT 1)",
        SNAPSHOT_ID,
    )
    await context.connection.execute(
        f"UPDATE {context.schema}.ptg2_import_run SET heartbeat_at = NULL "
        "WHERE import_run_id = $1",
        INTERNAL_RUN_ID,
    )
    heartbeat_plan = await reconcile.plan_legacy_v3_metadata_reconcile(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )
    assert heartbeat_plan["plan_digest"] is None
    assert "stale_reference_missing" in heartbeat_plan["reason_codes"]
    await context.connection.execute(
        f"UPDATE {context.schema}.ptg2_import_run SET heartbeat_at = "
        "timezone('UTC', now()) - INTERVAL '8 hours' "
        "WHERE import_run_id = $1",
        INTERNAL_RUN_ID,
    )


async def _execute_concurrent_repair(
    context: LegacyV3PostgresContext,
) -> tuple[list[dict], tuple, tuple]:
    versions_before = await row_versions(context)
    plan = await reconcile.plan_legacy_v3_metadata_reconcile(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )
    assert plan["status"] == "ready"
    outcomes = await asyncio.gather(
        *(
            reconcile.reconcile_legacy_v3_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                outer_run_id=OUTER_RUN_ID,
                expected_plan_digest=plan["plan_digest"],
            )
            for _ordinal in range(2)
        ),
        return_exceptions=True,
    )
    reports = await settle_lifecycle_outcomes(
        outcomes,
        replay=lambda: reconcile.reconcile_legacy_v3_metadata(
                snapshot_id=SNAPSHOT_ID,
                internal_run_id=INTERNAL_RUN_ID,
                outer_run_id=OUTER_RUN_ID,
                expected_plan_digest=plan["plan_digest"],
        ),
    )
    return reports, versions_before, await row_versions(context)


async def _assert_applied_state(context: LegacyV3PostgresContext) -> None:
    state = await context.connection.fetchrow(
        f"""
        SELECT snapshot.status, internal_run.status,
               internal_run.report, internal_run.error,
               (SELECT COUNT(*) FROM {context.schema}.
                   ptg2_legacy_v3_metadata_reconcile_audit)
          FROM {context.schema}.ptg2_snapshot AS snapshot
          JOIN {context.schema}.ptg2_import_run AS internal_run
            ON internal_run.import_run_id = snapshot.import_run_id
         WHERE snapshot.snapshot_id = $1
        """,
        SNAPSHOT_ID,
    )
    assert tuple(state[:2]) == ("failed", "failed")
    assert json.loads(state[2]) == {"synthetic": "preserved"}
    assert state[3:] == ("synthetic-preserved-error", 1)


async def _assert_reconcile_guards(context: LegacyV3PostgresContext) -> None:
    for guard_statement, coordinates in (
        (
            f"SELECT {context.schema}.guard_ptg_source_attempt($1)",
            (SOURCE_IMPORT_ID,),
        ),
        (
            f"SELECT {context.schema}.guard_ptg2_v4_attempt($1, $2, false)",
            (SNAPSHOT_ID, INTERNAL_RUN_ID),
        ),
        (
            f"SELECT {context.schema}.guard_ptg2_v4_attempt($1, $2, false)",
            (SNAPSHOT_ID, None),
        ),
        (
            f"SELECT {context.schema}.guard_ptg2_v4_attempt($1, $2, false)",
            (None, INTERNAL_RUN_ID),
        ),
    ):
        with pytest.raises(
            asyncpg.RaiseError,
            match="PTG2_LEGACY_V3_ATTEMPT_RECONCILED",
        ):
            await context.connection.execute(guard_statement, *coordinates)


async def _assert_table_immutable(
    context: LegacyV3PostgresContext,
    table_name: str,
) -> None:
    for immutable_statement in (
        f"UPDATE {context.schema}.{table_name} SET "
        + (
            "outer_run_id = outer_run_id"
            if "audit" in table_name
            else "event_kind = event_kind"
        ),
        f"DELETE FROM {context.schema}.{table_name}",
        f"TRUNCATE {context.schema}.{table_name}",
    ):
        with pytest.raises(
            asyncpg.RaiseError,
            match="PTG_SOURCE_ATTEMPT_AUDIT_IMMUTABLE",
        ):
            await context.connection.execute(immutable_statement)


@pytest.mark.asyncio
async def test_repair_is_metadata_only_idempotent_and_fences_writers(
    monkeypatch,
) -> None:
    """Apply one exact repair while preserving attachments and evidence."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        with pytest.raises(PTGSourceAttemptTerminalError):
            await source_actions.admit_existing_outer_run_action(
                run_id=OUTER_RUN_ID,
                event_kind="ensure_admitted",
            )
        await _assert_preplan_rejections(context)
        reports, versions_before, versions_after = (
            await _execute_concurrent_repair(context)
        )
        assert {report["state"] for report in reports} == {
            "applied",
            "already_reconciled",
        }
        assert versions_after[2:] == versions_before[2:]
        assert versions_after[:2] != versions_before[:2]
        await _assert_applied_state(context)
        await _assert_reconcile_guards(context)
        await _assert_table_immutable(
            context,
            "ptg2_legacy_v3_metadata_reconcile_audit",
        )


@pytest.mark.asyncio
async def test_zero_event_repair_remains_idempotent(monkeypatch) -> None:
    """Allow a proven pre-event orphan and retain its zero watermark."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context, include_source_event=False)
        _patch_operational_absence(monkeypatch)
        plan_by_field = await reconcile.plan_legacy_v3_metadata_reconcile(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
        )
        assert plan_by_field["status"] == "ready"
        assert plan_by_field["event_high_water_mark"] == 0
        reports = []
        for _attempt in range(2):
            reports.append(
                await reconcile.reconcile_legacy_v3_metadata(
                    snapshot_id=SNAPSHOT_ID,
                    internal_run_id=INTERNAL_RUN_ID,
                    outer_run_id=OUTER_RUN_ID,
                    expected_plan_digest=plan_by_field["plan_digest"],
                )
            )
        assert [report["state"] for report in reports] == [
            "applied",
            "already_reconciled",
        ]
        assert await context.connection.fetchval(
            f"SELECT event_high_water_mark FROM {context.schema}."
            "ptg2_legacy_v3_metadata_reconcile_audit"
        ) == 0


@pytest.mark.asyncio
async def test_postcommit_red_reports_durable_applied_state(monkeypatch) -> None:
    """Distinguish a committed repair from a precommit conflict."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        plan_by_field = await reconcile.plan_legacy_v3_metadata_reconcile(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
        )
        green_evidence_by_field = operational_absence([])
        red_evidence_by_field = {
            **green_evidence_by_field,
            "worker_present_count": 1,
            "exact_external_absence": False,
        }
        evidence_by_call = iter(
            (green_evidence_by_field, red_evidence_by_field)
        )

        async def changing_absence(_outer_runs, _event_rows=None):
            return next(evidence_by_call)

        monkeypatch.setattr(
            reconcile,
            "load_exact_operational_absence",
            changing_absence,
        )
        report_by_field = await reconcile.reconcile_legacy_v3_metadata(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
            expected_plan_digest=plan_by_field["plan_digest"],
        )

        assert report_by_field["state"] == "applied_postcheck_red"
        assert report_by_field["acceptance"] == "red"
        assert report_by_field["retry_allowed"] is False
        assert report_by_field["operator_action"] == "stop_no_retry"
        assert report_by_field["postcheck_exact_external_absence"] is False
        assert report_by_field["reason_codes"] == [
            "postcommit_external_identity_present"
        ]
        assert report_by_field["reconciliation_id"]
        await _assert_applied_state(context)


@pytest.mark.asyncio
async def test_source_event_table_is_append_only(monkeypatch) -> None:
    """Reject UPDATE, DELETE, and TRUNCATE on durable source events."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        await _assert_table_immutable(context, "ptg_source_attempt_event")


@pytest.mark.asyncio
async def test_unknown_event_outer_fails_closed(monkeypatch) -> None:
    """Reject an event whose outer run is absent from the retry chain."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        await seed_source_event(
            context.connection,
            context.schema_name,
            outer_run_id="run_unknown_synthetic",
        )
        plan = await reconcile.plan_legacy_v3_metadata_reconcile(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
        )
        assert plan["plan_digest"] is None
        assert "source_event_outer_lineage_changed" in plan["reason_codes"]


@pytest.mark.asyncio
async def test_same_source_extra_pair_fails_closed(monkeypatch) -> None:
    """Reject a second internal run/snapshot under the reviewed source."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        extra_run_id = "ptg2:" + "e" * 32
        await context.connection.execute(
            f"INSERT INTO {context.schema}.ptg2_import_run "
            "(import_run_id, status, started_at, heartbeat_at, options) "
            "VALUES ($1, 'running', now(), now(), $2::json)",
            extra_run_id,
            source_options(),
        )
        await context.connection.execute(
            f"INSERT INTO {context.schema}.ptg2_snapshot "
            "(snapshot_id, import_run_id, status, created_at, manifest) "
            "VALUES ('ptg2:202607:extra-synthetic', $1, "
            "'building', now(), '{}'::json)",
            extra_run_id,
        )
        plan = await reconcile.plan_legacy_v3_metadata_reconcile(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
        )
        assert "source_internal_run_cardinality_changed" in plan[
            "reason_codes"
        ]
        assert "source_snapshot_cardinality_changed" in plan["reason_codes"]


@pytest.mark.asyncio
async def test_outer_snapshot_coordinate_mismatch_fails_closed(
    monkeypatch,
) -> None:
    """Reject an outer attempt that points at another snapshot."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        await context.connection.execute(
            f"UPDATE {context.schema}.import_run SET snapshot_id = $1 "
            "WHERE run_id = $2",
            "ptg2:202607:other-synthetic",
            OUTER_RUN_ID,
        )
        plan = await reconcile.plan_legacy_v3_metadata_reconcile(
            snapshot_id=SNAPSHOT_ID,
            internal_run_id=INTERNAL_RUN_ID,
            outer_run_id=OUTER_RUN_ID,
        )
        assert plan["plan_digest"] is None
        assert "outer_source_snapshot_changed" in plan["reason_codes"]


async def _wait_for_source_lock_waiters(
    context: LegacyV3PostgresContext,
    expected_count: int,
) -> None:
    query = """
        WITH lock_id AS (SELECT hashtextextended($1, 0) AS value)
        SELECT COUNT(*)
          FROM pg_locks, lock_id
         WHERE locktype = 'advisory' AND NOT granted
           AND classid = (((value >> 32) & 4294967295)::oid)
           AND objid = ((value & 4294967295)::oid)
           AND objsubid = 1
    """
    for _attempt in range(200):
        if await context.connection.fetchval(
            query,
            source_attempt_lock_key(SOURCE_IMPORT_ID),
        ) >= expected_count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("source advisory-lock waiter did not appear")


async def _record_guarded_event(
    context: LegacyV3PostgresContext,
    external_callbacks: list[str],
) -> None:
    action_connection = await asyncpg.connect(context.dsn)
    try:
        async with action_connection.transaction():
            await action_connection.execute(
                f"SELECT {context.schema}.guard_ptg_source_attempt($1)",
                SOURCE_IMPORT_ID,
            )
            await action_connection.execute(
                f"""
                INSERT INTO {context.schema}.ptg_source_attempt_event (
                    protocol_version, source_file_import_id, event_kind,
                    outer_run_id, state_digest
                ) VALUES (
                    'ptg_source_attempt_fence_v1', $1,
                    'ensure_admitted', $2, repeat('2', 64)
                )
                """,
                SOURCE_IMPORT_ID,
                OUTER_RUN_ID,
            )
        external_callbacks.append("launched")
    finally:
        await action_connection.close()


async def _run_source_lock_race(
    context: LegacyV3PostgresContext,
    *,
    first_contender: str,
) -> tuple[list[object], list[str]]:
    plan = await reconcile.plan_legacy_v3_metadata_reconcile(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )
    callbacks: list[str] = []
    repair = lambda: reconcile.reconcile_legacy_v3_metadata(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
        expected_plan_digest=plan["plan_digest"],
    )
    action = lambda: _record_guarded_event(context, callbacks)
    first_factory = action if first_contender == "action" else repair
    second_factory = repair if first_contender == "action" else action
    blocker = context.connection.transaction()
    await blocker.start()
    await context.connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        source_attempt_lock_key(SOURCE_IMPORT_ID),
    )
    first_task = asyncio.create_task(first_factory())
    await _wait_for_source_lock_waiters(context, 1)
    second_task = asyncio.create_task(second_factory())
    await _wait_for_source_lock_waiters(context, 2)
    await blocker.commit()
    return await asyncio.gather(
        first_task,
        second_task,
        return_exceptions=True,
    ), callbacks


@pytest.mark.asyncio
@pytest.mark.parametrize("first_contender", ["action", "repair"])
async def test_source_action_and_repair_share_exact_lock(
    monkeypatch,
    first_contender,
) -> None:
    """Make the lock winner durable and stop the loser before effects."""

    async with legacy_v3_postgres_context(monkeypatch) as context:
        await seed_ready_v3_target(context)
        _patch_operational_absence(monkeypatch)
        outcomes, callbacks = await _run_source_lock_race(
            context,
            first_contender=first_contender,
        )
        if first_contender == "action":
            assert outcomes[0] is None
            assert isinstance(outcomes[1], reconcile.LegacyV3MetadataConflict)
            assert callbacks == ["launched"]
        else:
            assert outcomes[0]["state"] == "applied"
            assert isinstance(outcomes[1], asyncpg.RaiseError)
            assert "PTG2_LEGACY_V3_ATTEMPT_RECONCILED" in str(outcomes[1])
            assert callbacks == []
