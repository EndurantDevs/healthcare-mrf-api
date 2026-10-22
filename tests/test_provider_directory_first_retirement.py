# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit first-acquisition selection and fail-closed worker evidence."""

from unittest.mock import AsyncMock

import pytest

from process import provider_directory_terminal_root_retirement_operator as operator
from process import provider_directory_terminal_root_retirement_runtime as runtime
from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError
from process.provider_directory_terminal_root_retirement_selection import selected_terminal_root_retirement
from scripts.smoke import provider_directory_terminal_root_retirement as cli
from tests.test_provider_directory_terminal_root_retirement_cli import SELECTOR_ARGUMENTS
from tests.test_provider_directory_terminal_root_retirement_selection import SelectionDatabase, request


def first_request(**overrides):
    return request(expected_current_dataset_id=None, expect_no_current_dataset=True, **overrides)


@pytest.mark.parametrize("renderer", ["evidence_sql", "eligible_sql", "parent_sql"])
def test_rewritten_retirement_routines_keep_source_custody(renderer):
    from db import migration_provider_directory_first_acquisition_retirement as migration
    from process import network_fhir_source_custody as custody

    schema = "custody_contract"
    sql = getattr(migration, renderer)(schema)
    name = sql.split("CREATE FUNCTION ", 1)[1].split("(", 1)[0].split(".")[-1].strip('"')
    routine_by_field = {
        "proname": name,
        "prosrc": sql.split("AS $function$\n", 1)[1].rsplit("\n    $function$;", 1)[0],
        "lanname": "plpgsql" if "LANGUAGE plpgsql" in sql else "sql",
        "provolatile": "s" if "STABLE" in sql else "v",
        "prosecdef": True,
        "proconfig": ["search_path=pg_catalog", *(["TimeZone=UTC"] if "SET TimeZone = 'UTC'" in sql else [])],
        "proowner": 100,
    }
    custody._require_function(routine_by_field, schema, 100)
    with pytest.raises(custody.FHIRSourceCustodyError):
        custody._require_function(routine_by_field | {"prosrc": routine_by_field["prosrc"] + " changed"}, schema, 100)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["preview", "apply"])
@pytest.mark.parametrize("first_acquisition", [False, True])
async def test_operator_bounds_complete_first_acquisition_evidence(monkeypatch, operation, first_acquisition):
    selected_request = first_request() if first_acquisition else request()
    original_timeout = operator.asyncio.timeout
    deadlines = []

    def observed_timeout(delay):
        deadlines.append(delay)
        return original_timeout(delay)

    transaction = AsyncMock(return_value=object())
    monkeypatch.setattr(operator, "require_terminal_root_retirement_gate", lambda: None)
    monkeypatch.setattr(operator.asyncio, "timeout", observed_timeout)
    monkeypatch.setattr(operator, operation + "_terminal_root_retirement_transaction", transaction)
    database = object()
    command = getattr(operator, operation + "_terminal_root_retirement")
    assert await command(selected_request, database=database) is transaction.return_value
    assert deadlines == [3600 if first_acquisition else 180]
    transaction.assert_awaited_once_with(database, selected_request)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["preview", "apply"])
@pytest.mark.parametrize("wrapped", [False, True])
async def test_nowait_contention_reports_busy_for_both_operator_commands(monkeypatch, operation, wrapped):
    locked = RuntimeError("locked")
    locked.sqlstate = "55P03"
    if wrapped:
        failure = RuntimeError("database")
        failure.orig = locked
    else:
        failure = locked
    monkeypatch.setattr(operator, "require_terminal_root_retirement_gate", lambda: None)
    monkeypatch.setattr(operator, operation + "_terminal_root_retirement_transaction", AsyncMock(side_effect=failure))
    command = getattr(operator, operation + "_terminal_root_retirement")
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await command(first_request(), database=object())


def test_first_acquisition_requires_explicit_absence_and_closed_selector():
    assert first_request().expected_current_dataset_id is None
    for fields in (
        {"expected_current_dataset_id": None},
        {"expect_no_current_dataset": True},
        {"expected_current_dataset_id": None, "expect_no_current_dataset": 1},
    ):
        with pytest.raises(TerminalRootRetirementError, match="request_invalid"):
            request(**fields)
    parsed = cli._parser().parse_args(["preview", *SELECTOR_ARGUMENTS[:-2], "--expect-no-current-dataset"])
    assert cli._request(parsed).expect_no_current_dataset
    with pytest.raises(SystemExit):
        cli._parser().parse_args(["preview", *SELECTOR_ARGUMENTS, "--expect-no-current-dataset"])


class FirstDatabase(SelectionDatabase):
    def __init__(self):
        super().__init__()
        self.target["previous_dataset_id"] = None
        self.serving = []
        self.lock_results = [True] * 7

    async def all(self, sql, **params):
        if "dataset.is_current OR dataset.published_at" in sql:
            self.calls.append(("all", sql, params))
            return self.serving
        if sql.startswith("SELECT 1 FROM"):
            self.calls.append(("all", sql, params))
            return []
        return await super().all(sql, **params)


@pytest.mark.asyncio
async def test_first_selection_uses_root_locks_and_exact_runtime_absence(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(runtime, "require_stopped_lineage", stopped)
    database = FirstDatabase()
    selected = await selected_terminal_root_retirement(database, first_request())
    assert selected.prior_status == "acquiring"
    stopped.assert_awaited_once_with(tuple(database.lineage))
    assert not any("LOCK TABLE" in sql for _, sql, _ in database.calls)
    assert any("FOR UPDATE OF run NOWAIT" in sql for _, sql, _ in database.calls)
    locks = [params["lock_identity"] for _, sql, params in database.calls if "pg_try_advisory" in sql]
    assert locks == [
        "provider-directory-pagination:https://synthetic.invalid/fhir",
        "endpoint-synthetic",
        "provider-directory-terminal-root-retirement:run-root",
        "provider-directory-terminal-root-retirement:run-owner",
        "control-run-worker-action:v1:run-owner",
        "provider-directory-terminal-root-retirement:run-root",
        "control-run-worker-action:v1:run-root",
    ]


@pytest.mark.asyncio
async def test_first_selection_rejects_current_dataset_before_runtime_probe(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(runtime, "require_stopped_lineage", stopped)
    database = FirstDatabase()
    database.serving = [{"dataset_id": "new-current"}]
    with pytest.raises(TerminalRootRetirementError, match="evidence_changed"):
        await selected_terminal_root_retirement(database, first_request())
    stopped.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_selection_rechecks_children_committed_before_parent_fence(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(runtime, "require_stopped_lineage", stopped)
    database = FirstDatabase()
    scalar = database.scalar

    async def insert_late_child(sql, **params):
        if params.get("lock_identity") == "provider-directory-terminal-root-retirement:run-owner":
            database.lineage.append(
                database.lineage[-1] | {"run_id": "late-child", "retry_of_run_id": "run-owner", "depth": 2}
            )
        return await scalar(sql, **params)

    monkeypatch.setattr(database, "scalar", insert_late_child)
    with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(database, first_request())
    stopped.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_selection_rejects_other_hash_profile():
    database = FirstDatabase()
    database.target["publication_metadata_json"]["resource_hash_contract"] = "semantic_content_v4"
    with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
        await selected_terminal_root_retirement(database, first_request())


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["queue_member", "job", "retry", "in_progress"])
async def test_executable_queue_state_refuses_before_worker_probe(monkeypatch, field):
    from api import control_imports, control_workers

    state = dict.fromkeys(("queue_member", "job", "retry", "in_progress"), False)
    state[field] = True
    monkeypatch.setattr(control_imports, "_arq_worker_presence", AsyncMock(return_value=state))
    monkeypatch.setattr(control_workers, "exact_worker_presence", lambda _: pytest.fail("queue state was executable"))
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await runtime.require_stopped_lineage([{"run_id": "root"}])


@pytest.mark.asyncio
@pytest.mark.parametrize("presence", [{}, {"enabled": False}, {"enabled": True, "stopped": False}])
async def test_missing_or_active_worker_evidence_is_not_absence(monkeypatch, presence):
    from api import control_imports, control_workers

    state = dict.fromkeys(("queue_member", "job", "retry", "in_progress"), False)
    monkeypatch.setattr(control_imports, "_arq_worker_presence", AsyncMock(return_value=state))
    monkeypatch.setattr(control_workers, "exact_worker_presence", lambda _: presence)
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await runtime.require_stopped_lineage([{"run_id": "root"}])


@pytest.mark.asyncio
async def test_terminal_results_are_safe_but_missing_queue_evidence_is_not(monkeypatch):
    from api import control_imports, control_workers

    state = dict.fromkeys(("queue_member", "job", "retry", "in_progress"), False)
    state["result"] = True
    queue = AsyncMock(return_value=state)
    monkeypatch.setattr(control_imports, "_arq_worker_presence", queue)
    monkeypatch.setattr(control_workers, "exact_worker_presence", lambda _: {"enabled": True, "stopped": True})
    await runtime.require_stopped_lineage([{"run_id": "root"}, {"run_id": "retry"}])
    assert queue.await_count == 2
    queue.return_value = {}
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await runtime.require_stopped_lineage([{"run_id": "root"}])


@pytest.mark.asyncio
async def test_unavailable_queue_evidence_fails_without_a_write(monkeypatch):
    from api import control_imports

    monkeypatch.setattr(control_imports, "_arq_worker_presence", AsyncMock(side_effect=ConnectionError("unavailable")))
    with pytest.raises(ConnectionError):
        await runtime.require_stopped_lineage([{"run_id": "root"}])
