# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from process import control_lifecycle, live_progress, ptg_control
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis


async def _allow_active_run(_run_id: str) -> None:
    return None


async def _skip_terminal_flush() -> None:
    return None


def _statement_values_by_field(statement: Any) -> dict[str, Any]:
    return {
        getattr(key, "key", str(key)): getattr(field_value, "value", field_value)
        for key, field_value in statement._values.items()
    }


@dataclass
class OverlapAttemptHarness:
    run_id: str = "run_ptg_overlap"
    first_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_first: asyncio.Event = field(default_factory=asyncio.Event)
    main_attempts: list[tuple[str, str]] = field(default_factory=list)
    database_state_by_field: dict[str, str | None] = field(
        default_factory=lambda: {
            "attempt_id": None,
            "attempt_started_at": None,
            "status": "queued",
        }
    )
    status_events: list[dict[str, Any]] = field(default_factory=list)
    terminal_sql_statements: list[str] = field(default_factory=list)
    terminal_sql_parameters: list[dict[str, Any]] = field(default_factory=list)

    async def run_ptg_import(self, **arguments_by_name: Any) -> dict[str, Any]:
        self.main_attempts.append(
            (
                arguments_by_name["control_attempt_id"],
                arguments_by_name["control_attempt_started_at"],
            )
        )
        if len(self.main_attempts) == 1:
            self.first_started.set()
            await self.release_first.wait()
            raise RuntimeError("delayed attempt A failure")
        return {}

    async def execute_control_update(self, statement: Any) -> int:
        values_by_field = _statement_values_by_field(statement)
        status = str(values_by_field["status"])
        progress_by_field = values_by_field["progress"]
        incoming_attempt = str(progress_by_field.get("attempt_id") or "")
        incoming_started_at = str(
            progress_by_field.get("attempt_started_at") or ""
        )
        if status == "running":
            self.database_state_by_field.update(
                attempt_id=incoming_attempt,
                attempt_started_at=incoming_started_at,
                status=status,
            )
            return 1
        compiled_statement = statement.compile(dialect=postgresql.dialect())
        self.terminal_sql_statements.append(str(compiled_statement))
        self.terminal_sql_parameters.append(dict(compiled_statement.params))
        has_attempt_ownership = (
            incoming_attempt == self.database_state_by_field["attempt_id"]
            and incoming_started_at
            == self.database_state_by_field["attempt_started_at"]
        )
        if not has_attempt_ownership:
            return 0
        self.database_state_by_field["status"] = status
        return 1

    def is_live_progress_recorded(self, **progress_by_field: Any) -> bool:
        self.status_events.append(progress_by_field["status_event_payload"])
        return True

    def assert_sql_fencing(self) -> None:
        assert self.terminal_sql_statements
        assert all(
            "progress ->>" in statement
            for statement in self.terminal_sql_statements
        )
        assert all(
            "attempt_id" in parameters.values()
            for parameters in self.terminal_sql_parameters
        )
        assert all(
            "attempt_started_at" in parameters.values()
            for parameters in self.terminal_sql_parameters
        )
        assert all(
            "RETURNING" in statement
            for statement in self.terminal_sql_statements
        )


def _install_overlap_harness(monkeypatch, harness: OverlapAttemptHarness) -> None:
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "60")
    monkeypatch.setattr(
        control_lifecycle,
        "_claim_control_run_db_update_slot",
        lambda _key, _seconds: False,
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_ptg_import)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)
    monkeypatch.setattr(
        ptg_control,
        "_flush_terminal_status_events",
        _skip_terminal_flush,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        harness.execute_control_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "read_live_progress",
        lambda _run_id: None,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        harness.is_live_progress_recorded,
    )


@pytest.mark.asyncio
async def test_overlapping_ptg_attempts_fence_delayed_old_terminal(
    monkeypatch,
):
    """B owns the run after it starts, so delayed A cannot terminalize B."""

    harness = OverlapAttemptHarness()
    _install_overlap_harness(monkeypatch, harness)

    first_task = asyncio.create_task(
        ptg_control.ptg_control_start(
            {},
            {"run_id": harness.run_id, "params": {"source_key": "source_a"}},
        )
    )
    await harness.first_started.wait()
    second_result = await ptg_control.ptg_control_start(
        {},
        {"run_id": harness.run_id, "params": {"source_key": "source_a"}},
    )
    harness.release_first.set()
    with pytest.raises(RuntimeError, match="delayed attempt A"):
        await first_task

    assert second_result["status"] == "succeeded"
    assert harness.database_state_by_field == {
        "attempt_id": harness.main_attempts[1][0],
        "attempt_started_at": harness.main_attempts[1][1],
        "status": "succeeded",
    }
    assert all(
        not (
            event_by_field["status"] == "failed"
            and event_by_field["progress"]["attempt_id"]
            == harness.main_attempts[0][0]
        )
        for event_by_field in harness.status_events
    )
    harness.assert_sql_fencing()


@dataclass
class InvertedStartHarness:
    first_claim_entered: asyncio.Event = field(default_factory=asyncio.Event)
    release_first_claim: asyncio.Event = field(default_factory=asyncio.Event)
    current_attempt_by_field: dict[str, str | None] = field(
        default_factory=lambda: {
            "attempt_id": None,
            "attempt_started_at": None,
            "status": "queued",
        }
    )
    main_attempt_ids: list[str] = field(default_factory=list)
    start_claim_sql_statements: list[str] = field(default_factory=list)
    running_claim_count: int = 0

    async def execute_control_update(self, statement: Any) -> int:
        values_by_field = _statement_values_by_field(statement)
        status = str(values_by_field["status"])
        progress_by_field = values_by_field["progress"]
        incoming_attempt = str(progress_by_field.get("attempt_id") or "")
        incoming_started_at = str(
            progress_by_field.get("attempt_started_at") or ""
        )
        if status == "running":
            self.running_claim_count += 1
            self.start_claim_sql_statements.append(
                str(statement.compile(dialect=postgresql.dialect()))
            )
            if self.running_claim_count == 1:
                self.first_claim_entered.set()
                await self.release_first_claim.wait()
            stored_started_at = str(
                self.current_attempt_by_field["attempt_started_at"] or ""
            )
            if stored_started_at and incoming_started_at <= stored_started_at:
                return 0
            self.current_attempt_by_field.update(
                attempt_id=incoming_attempt,
                attempt_started_at=incoming_started_at,
                status=status,
            )
            return 1
        has_attempt_ownership = (
            incoming_attempt == self.current_attempt_by_field["attempt_id"]
            and incoming_started_at
            == self.current_attempt_by_field["attempt_started_at"]
        )
        if not has_attempt_ownership:
            return 0
        self.current_attempt_by_field["status"] = status
        return 1

    async def run_ptg_import(self, **arguments_by_name: Any) -> dict[str, Any]:
        self.main_attempt_ids.append(arguments_by_name["control_attempt_id"])
        return {}


@pytest.mark.asyncio
async def test_delayed_older_ptg_start_cannot_replace_newer_claim(
    monkeypatch,
):
    """An inverted SQL interleaving rejects A after B already claimed."""

    run_id = "run_ptg_inverted_start"
    harness = InvertedStartHarness()
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_ptg_import)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        harness.execute_control_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "read_live_progress",
        lambda _run_id: None,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **_progress_by_field: True,
    )

    control_task_by_field = {
        "run_id": run_id,
        "params": {"source_key": "source_a"},
    }
    older_task = asyncio.create_task(
        ptg_control.ptg_control_start({}, control_task_by_field)
    )
    await harness.first_claim_entered.wait()
    await asyncio.sleep(0.001)
    newer_result = await ptg_control.ptg_control_start(
        {},
        control_task_by_field,
    )
    harness.release_first_claim.set()
    older_result = await older_task

    assert newer_result["status"] == "succeeded"
    assert older_result["reason"] == "newer_attempt_active"
    assert harness.main_attempt_ids == [
        harness.current_attempt_by_field["attempt_id"]
    ]
    assert harness.current_attempt_by_field["status"] == "succeeded"
    assert len(harness.start_claim_sql_statements) == 2
    assert all(
        "RETURNING" in statement and "progress ->>" in statement
        for statement in harness.start_claim_sql_statements
    )
    assert all(
        " < " in statement for statement in harness.start_claim_sql_statements
    )


@dataclass
class TerminalStartHarness:
    claim_entered: asyncio.Event = field(default_factory=asyncio.Event)
    release_claim: asyncio.Event = field(default_factory=asyncio.Event)
    stored_status: str = "queued"
    compiled_parameter_by_name: dict[str, Any] = field(default_factory=dict)
    has_importer_run: bool = False

    async def execute_control_update(self, statement: Any) -> int:
        self.claim_entered.set()
        await self.release_claim.wait()
        compiled = statement.compile(dialect=postgresql.dialect())
        self.compiled_parameter_by_name = dict(compiled.params)
        return (
            0
            if self.stored_status in _terminal_claim_statuses()
            else 1
        )

    async def run_ptg_import(
        self,
        **_arguments_by_name: Any,
    ) -> dict[str, Any]:
        self.has_importer_run = True
        return {}


@pytest.mark.asyncio
async def test_delayed_start_cannot_resurrect_terminal_run(monkeypatch):
    """A start claim already in flight loses if the run becomes terminal."""

    harness = TerminalStartHarness()
    monkeypatch.setattr(
        control_lifecycle,
        "_execute_control_run_update",
        harness.execute_control_update,
    )
    monkeypatch.setattr(
        control_lifecycle,
        "write_live_progress",
        lambda **_progress_by_field: True,
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_ptg_import)
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    task = asyncio.create_task(
        ptg_control.ptg_control_start(
            {},
            {
                "run_id": "run_terminal_interleaving",
                "params": {"source_key": "source_a"},
            },
        )
    )
    await harness.claim_entered.wait()
    harness.stored_status = "succeeded"
    harness.release_claim.set()
    control_outcome = await task

    assert control_outcome["status"] == "skipped"
    assert not harness.has_importer_run
    parameter_strings = {
        nested_value
        for parameter_value in harness.compiled_parameter_by_name.values()
        for nested_value in (
            parameter_value
            if isinstance(parameter_value, (list, tuple, set))
            else (parameter_value,)
        )
        if isinstance(nested_value, str)
    }
    assert _terminal_claim_statuses() <= parameter_strings


def _terminal_claim_statuses() -> set[str]:
    return control_lifecycle._TERMINAL_STATUSES | {"canceling"}


@dataclass
class RetryProgressHarness:
    stored_by_key: dict[str, str] = field(default_factory=dict)
    attempts: list[tuple[str, str]] = field(default_factory=list)

    async def is_control_run_marked(
        self,
        *_arguments: Any,
        **_arguments_by_name: Any,
    ) -> bool:
        return True

    async def run_ptg_import(self, **arguments_by_name: Any) -> dict[str, Any]:
        self.attempts.append(
            (
                arguments_by_name["control_attempt_id"],
                arguments_by_name["control_attempt_started_at"],
            )
        )
        if len(self.attempts) == 1:
            live_progress.write_live_progress(
                stage_id="publish",
                stage_ordinal=5,
                pct=90,
                publish_event=False,
            )
        else:
            live_progress.write_live_progress(
                stage_id="scan",
                stage_ordinal=1,
                pct=5,
                publish_event=False,
            )
        return {}


@pytest.mark.asyncio
async def test_ptg_retry_accepts_low_stage_progress_with_new_attempt(
    monkeypatch,
):
    """A retry starts at its real low stage instead of inheriting A's 90%."""

    harness = RetryProgressHarness()
    monkeypatch.setenv("HLTHPRT_IMPORT_LIVE_PROGRESS_HEARTBEAT_SECONDS", "0")
    monkeypatch.setattr(
        live_progress,
        "_redis",
        lambda: AtomicLiveProgressRedis(harness.stored_by_key),
    )
    monkeypatch.setattr(ptg_control, "ptg_main", harness.run_ptg_import)
    monkeypatch.setattr(
        ptg_control,
        "mark_control_run",
        harness.is_control_run_marked,
    )
    monkeypatch.setattr(ptg_control, "_stale_ptg_job_result", _allow_active_run)

    control_task_by_field = {
        "run_id": "run_ptg_retry_progress",
        "params": {"source_key": "source_a"},
    }
    await ptg_control.ptg_control_start({}, control_task_by_field)
    await asyncio.sleep(0.001)
    await ptg_control.ptg_control_start({}, control_task_by_field)

    retained_by_field = json.loads(
        harness.stored_by_key[
            live_progress.live_progress_key("run_ptg_retry_progress")
        ]
    )
    assert harness.attempts[0][0] != harness.attempts[1][0]
    assert retained_by_field["attempt_id"] == harness.attempts[1][0]
    assert retained_by_field["attempt_started_at"] == harness.attempts[1][1]
    assert retained_by_field["stage_id"] == "scan"
    assert retained_by_field["stage_ordinal"] == 1
    assert retained_by_field["pct"] == 5


def test_run_id_fallback_uses_attempt_timestamp_to_detect_retry():
    run_id = "run_legacy_retry"

    assert (
        live_progress._attempt_disposition(
            {
                "run_id": run_id,
                "attempt_id": run_id,
                "attempt_started_at": "2026-07-23T12:00:00Z",
            },
            {
                "run_id": run_id,
                "attempt_id": run_id,
                "attempt_started_at": "2026-07-23T11:00:00Z",
            },
        )
        == live_progress._ATTEMPT_NEWER
    )
