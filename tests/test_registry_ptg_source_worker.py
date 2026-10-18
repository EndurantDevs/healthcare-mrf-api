"""Closed event/configuration and real fixed importer/control adapter contracts."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner
from sqlalchemy.ext.asyncio import AsyncEngine

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process
from api import control_imports, control_workers
from process import registry_ptg_source_worker as worker
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from tests.test_registry_ptg_source_notifications import event as native_event

IMPORTER = "registry-ptg-source-worker"


def event():
    row = native_event()
    return {name: row[name] for name in ("capture_id", "approval_sha256", "hold_sha256")}


def configuration():
    return {
        "execution_dsn": "postgresql://executor:{password}@db.example.test/example",
        "notification_dsn": "postgresql://office_reader:{password}@db.example.test/example",
        "consumer": {"control_schema": "office_control", "execution_role": "executor", "owner_role": "physical_owner"},
        "reader_role": "office_reader",
        "ptg": {
            "owner_role": "office_owner",
            "approval_role": "office_approver",
            "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 10, "maximum_coordinates": 100},
        },
        "event": event(),
    }


def test_configuration_is_closed_and_keeps_separate_direct_read_and_execution_roles():
    parsed = worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
    assert parsed.reader_role == "office_reader" and parsed.consumer.execution_role == "executor"
    assert parsed.notification_store is parsed.ptg.store
    assert "{password}" not in repr(parsed)


@pytest.mark.parametrize(
    "damage", ["extra", "database", "reader", "privileged", "schema", "runtime", "event", "deadline"]
)
def test_bad_configuration_refuses_before_resources(damage):
    value = configuration()
    if damage == "extra":
        value["sql"] = "not permitted"
    if damage == "database":
        value["notification_dsn"] += "2"
    if damage == "reader":
        value["reader_role"] = "other"
    if damage == "privileged":
        value["consumer"]["execution_role"] = "office_approver"
    if damage == "schema":
        value["consumer"]["control_schema"] = "bad;schema"
    if damage == "runtime":
        value["ptg"]["runtime"] = {}
    if damage == "event":
        value["event"]["selection"] = []
    if damage == "deadline":
        value["consumer"]["operation_seconds"] = True
    with pytest.raises(ValueError, match="registry_ptg_source_worker_config_invalid"):
        worker.parse_registry_ptg_source_worker_config(json.dumps(value))


@pytest.mark.parametrize(
    "changes",
    [
        {"capture_id": str(0)},
        {"approval_sha256": "A" * 64},
        {"hold_sha256": True},
        {"coordinates": {}},
        {"execution_dsn": "synthetic"},
        {"run_id": "extra"},
    ],
)
def test_event_is_exact_identity_only(changes):
    with pytest.raises(ValueError):
        worker.validate_registry_ptg_source_event({**event(), **changes})


def test_registered_main_command_queue_metadata_and_worker_agree():
    item = next(item for item in control_imports.importer_registry() if item["name"] == IMPORTER)
    assert item["schedulable"] and item["cancelable"] and item["retryable"]
    assert item["queue"] == "arq:RegistryPTGSource" and item["function"] == "control_single_job_start"
    assert {p["name"] for p in item["params_schema"]} == set(event())
    assert all(p["required"] for p in item["params_schema"])
    assert control_workers._BY_QUEUE[item["queue"]].worker_class == "process.RegistryPTGSource"
    assert (
        process.RegistryPTGSource.max_jobs
        == process.RegistryPTGSource.queue_read_limit
        == process.RegistryPTGSource.max_tries
        == 1
    )
    assert process.RegistryPTGSource.job_timeout == 4200
    assert process.process_group.commands[IMPORTER] is worker.registry_ptg_source_worker_command


@pytest.mark.asyncio
async def test_real_adapter_only_enqueues_triple_and_genuine_run_id(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id="synthetic")))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=redis))
    result = await control_imports._enqueue_import_start(
        {"run_id": "synthetic-run", "importer": IMPORTER, "params": event()}
    )
    assert result["status"] == "queued"
    args, kwargs = redis.enqueue_job.await_args
    assert args[0] == "control_single_job_start"
    assert args[1]["target_module"] == "process.registry_ptg_source_worker"
    assert args[1]["task"] == event() and args[1]["run_id"] == "synthetic-run"
    assert kwargs == {"_queue_name": "arq:RegistryPTGSource", "_job_id": "registry_ptg_source_synthetic-run"}


@pytest.mark.asyncio
async def test_malformed_event_never_reaches_db_or_queue(monkeypatch):
    database = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected DB")))
    pool = AsyncMock(side_effect=AssertionError("unexpected queue"))
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "create_pool", pool)
    with pytest.raises(ValueError):
        await control_imports.create_import_run({"importer": IMPORTER, "params": {**event(), "sql": "invalid"}})
    result = await control_imports._enqueue_import_start(
        {"importer": IMPORTER, "run_id": "synthetic", "params": {**event(), "sql": "invalid"}}
    )
    assert result["status"] == "failed"
    database.execute.assert_not_called()
    pool.assert_not_awaited()


@pytest.mark.parametrize(
    "failure,code,status",
    [("config", "invalid_configuration", 2), ("work", "worker_failed", 1), ("cancel", "canceled", 130)],
)
def test_errors_never_echo_configuration_or_exception(monkeypatch, capsys, failure, code, status):
    monkeypatch.setenv(worker.CONFIG_ENV, "sensitive-value" if failure == "config" else json.dumps(configuration()))

    async def run(_config):
        if failure == "cancel":
            raise asyncio.CancelledError
        raise RuntimeError("sensitive-value")

    monkeypatch.setattr(worker, "run_registry_ptg_source_worker", run)
    assert worker.run_command() == status
    captured = capsys.readouterr()
    assert not captured.out and "sensitive-value" not in captured.err and "synthetic" not in captured.err
    assert json.loads(captured.err) == {"state": "error", "code": code}
    rejected = CliRunner().invoke(worker.registry_ptg_source_worker_command, ["--credential=sensitive-value"])
    assert rejected.exit_code == 2 and "sensitive-value" not in rejected.output


@pytest.mark.asyncio
async def test_control_uses_trusted_config_and_exact_event_and_drains_watcher(monkeypatch):
    from process import control_cancel

    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())
    run = AsyncMock(return_value={"state": "review_required"})
    monkeypatch.setattr(worker, "run_registry_ptg_source_worker", run)
    assert await worker.control_start({}, {**event(), "run_id": "synthetic-run"}) == {"state": "review_required"}
    assert run.await_args.kwargs["event"] == event()
    assert type(run.await_args.args[0]) is worker.RegistryPTGSourceWorkerConfig


@pytest.mark.asyncio
async def test_second_pool_failure_closes_first_without_consumer(monkeypatch):
    pool = SimpleNamespace(close=AsyncMock(), terminate=lambda: None)
    monkeypatch.setattr(worker.asyncpg, "create_pool", AsyncMock(side_effect=[pool, RuntimeError("synthetic")]))
    consume = AsyncMock()
    monkeypatch.setattr(worker, "consume_registry_ptg_source_notification_once", consume)
    with pytest.raises(RuntimeError, match="synthetic"):
        await worker.run_registry_ptg_source_worker(
            worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
        )
    pool.close.assert_awaited_once()
    consume.assert_not_awaited()


class Connection:
    def __init__(self, role, events):
        self.role, self.events, self.active_transaction = role, events, False
        self.identity = dict(
            database_name="example",
            database_oid=1,
            server_address="127.0.0.1",
            server_port=5432,
            server_started="synthetic-start",
            control_oid=2,
            inbox_oid=3,
            current_role=role,
            session_role=role,
            safe_login=True,
        )

    async def fetchrow(self, query, *args):
        assert query == worker._IDENTITY_SQL and args == ("office_control", "office_control")
        return dict(self.identity)

    def is_in_transaction(self):
        return self.active_transaction

    @asynccontextmanager
    async def transaction(self, **options):
        assert self.role == "office_reader"
        assert options == {"isolation": "repeatable_read", "readonly": True}
        self.active_transaction = True
        self.events.append("reader transaction")
        try:
            yield
        finally:
            self.active_transaction = False
            self.events.append("reader transaction ended")


class Pool:
    def __init__(self, connection, harness, options):
        self.connection, self.harness, self.options = connection, harness, options

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == 5
        role = self.connection.role
        self.harness.events.append(f"{role} checkout")
        if self.harness.failure == f"{role} checkout":
            raise RuntimeError("synthetic failure")
        await self.options["setup"](self.connection)
        try:
            yield self.connection
        finally:
            self.harness.events.append(f"{role} released")

    async def close(self):
        self.harness.events.append(f"{self.connection.role} closed")

    def terminate(self):
        raise AssertionError("normal pool cleanup must not terminate")


def resources(monkeypatch, failure=None):
    """Exercise typed orchestration with unit database/qualification doubles, without native proof."""
    harness = SimpleNamespace(events=[], options=[], failure=failure, identity=object())
    harness.execution = Connection("executor", harness.events)
    harness.reader = Connection("office_reader", harness.events)
    monkeypatch.setattr(worker.RegistryPublicationSQLLimits, "verify", AsyncMock())

    async def create_pool(**options):
        if failure == "startup":
            raise RuntimeError("synthetic failure")
        harness.options.append(options)
        connection = harness.execution if options["user"] == "executor" else harness.reader
        return Pool(connection, harness, options)

    async def qualify_notifications(connection, store, *, reader_role):
        assert connection is harness.reader and connection.is_in_transaction()
        assert store.control_schema == "office_control" and reader_role == "office_reader"
        harness.events.append("notification qualified")
        if failure == "notification qualification":
            raise RuntimeError("synthetic failure")
        return harness.identity

    async def qualify_runtime(runtime):
        assert type(runtime) is RegistryPublicationPTGRuntime
        harness.events.append("runtime qualified")
        if failure == "runtime qualification":
            raise RuntimeError("synthetic failure")

    original_dispose = AsyncEngine.dispose

    async def dispose(engine, *args, **kwargs):
        await original_dispose(engine, *args, **kwargs)
        harness.events.append("runtime disposed")

    monkeypatch.setattr(worker.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(worker, "qualify_registry_ptg_source_notifications", qualify_notifications)
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "qualify", qualify_runtime)
    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    return harness


@pytest.mark.asyncio
@pytest.mark.parametrize("state", sorted(worker._STATES))
@pytest.mark.parametrize("override", [False, True])
async def test_typed_event_consumption_qualifies_before_work_and_closes_owned_resources(monkeypatch, state, override):
    harness = resources(monkeypatch)
    configured = worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
    selected = {**event(), "approval_sha256": "b" * 64} if override else event()

    async def consume(**kwargs):
        assert kwargs["connection"] is harness.execution and not harness.execution.is_in_transaction()
        assert kwargs["notification_pool"].pool.connection is harness.reader
        assert kwargs["notification_pool"].timeout == configured.sql_limits.connect_timeout_seconds
        assert kwargs["store"] is configured.notification_store
        assert kwargs["reader_role"] == configured.reader_role and kwargs["identity"] is harness.identity
        assert kwargs["event"] == selected and kwargs["event"] is not configured.event
        assert type(kwargs["config"]) is worker.RegistryPTGSourceConsumerConfig
        assert kwargs["config"].composition_runtime.store is configured.notification_store
        harness.events.append("consume")
        return {"state": state}

    monkeypatch.setattr(worker, "consume_registry_ptg_source_notification_once", consume)
    options = {"event": selected} if override else {}
    assert await worker.run_registry_ptg_source_worker(configured, **options) == {"state": state}
    assert harness.events == [
        "executor checkout",
        "office_reader checkout",
        "reader transaction",
        "notification qualified",
        "reader transaction ended",
        "office_reader released",
        "runtime qualified",
        "consume",
        "executor released",
        "runtime disposed",
        "office_reader closed",
        "executor closed",
    ]
    assert [pool_options["user"] for pool_options in harness.options] == ["executor", "office_reader"]
    assert all(
        pool_options["min_size"] == pool_options["max_size"] == 1 and pool_options["timeout"] == 5
        for pool_options in harness.options
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "startup",
        "executor checkout",
        "office_reader checkout",
        "identity mismatch",
        "notification qualification",
        "runtime qualification",
        "consume",
        "cancel",
        "result",
    ],
)
async def test_runner_failures_drain_and_release_owned_pools_and_runtime(monkeypatch, failure):
    harness = resources(monkeypatch, failure)
    if failure == "identity mismatch":
        harness.reader.identity["server_started"] = "other-server-start"

    async def consume(**kwargs):
        try:
            if failure == "cancel":
                raise asyncio.CancelledError
            if failure == "consume":
                raise RuntimeError("synthetic failure")
            return {"state": "completed", "untrusted": "extra"}
        finally:
            await asyncio.sleep(0)
            harness.events.append("consumer drained")

    consumer = AsyncMock(side_effect=consume)
    monkeypatch.setattr(worker, "consume_registry_ptg_source_notification_once", consumer)
    with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
        await worker.run_registry_ptg_source_worker(
            worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
        )
    if failure == "startup":
        assert not harness.events and not harness.options
    else:
        assert harness.events[-2:] == ["office_reader closed", "executor closed"]
    if failure in {"runtime qualification", "consume", "cancel", "result"}:
        assert harness.events.index("executor released") < harness.events.index("runtime disposed")
        assert harness.events.index("runtime disposed") < harness.events.index("office_reader closed")
    if failure in {"consume", "cancel", "result"}:
        assert harness.events.index("consumer drained") < harness.events.index("executor released")
    else:
        consumer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"safe_login": False},
        {"current_role": "other"},
        {"session_role": "other"},
        {"control_oid": None},
        {"inbox_oid": None},
        {"active_transaction": True},
    ],
)
async def test_runner_rejects_unqualified_execution_before_notification_or_runtime(monkeypatch, changes):
    harness = resources(monkeypatch)
    if "active_transaction" in changes:
        harness.execution.active_transaction = True
    else:
        harness.execution.identity.update(changes)
    with pytest.raises(ValueError, match="registry_ptg_source_worker_config_invalid"):
        await worker.run_registry_ptg_source_worker(
            worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
        )
    assert harness.events == ["executor checkout", "executor released", "office_reader closed", "executor closed"]


@pytest.mark.parametrize("changes", [{"sql_limits": object()}, {"notification_store": object()}, {"ptg": None}])
def test_direct_configuration_requires_typed_limits_store_and_runtime(changes):
    configured = worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
    with pytest.raises(ValueError):
        replace(configured, **changes)


def test_direct_configuration_refuses_privileged_executor_even_with_matching_dsn():
    configured = worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
    role = configured.notification_store.approval_role
    with pytest.raises(ValueError, match="registry_ptg_source_worker_config_invalid"):
        replace(
            configured,
            execution_url=configured.execution_url.set(username=role),
            consumer=replace(configured.consumer, execution_role=role),
        )


@pytest.mark.parametrize("encoded", [None, json.dumps({**configuration(), "execution_dsn": False})])
def test_parser_refuses_nontext_configuration_and_dsn(encoded):
    with pytest.raises(ValueError, match="registry_ptg_source_worker_config_invalid"):
        worker.parse_registry_ptg_source_worker_config(encoded)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["config", "event"])
async def test_runner_rejects_wrong_type_or_event_before_creating_pools(monkeypatch, invalid):
    create = AsyncMock(side_effect=AssertionError("unexpected pool"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", create)
    configured = worker.parse_registry_ptg_source_worker_config(json.dumps(configuration()))
    with pytest.raises(ValueError):
        await worker.run_registry_ptg_source_worker(
            object() if invalid == "config" else configured, event={**event(), "untrusted": "extra"}
        )
    create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["work", "control cancel", "outer cancel"])
async def test_control_failures_drain_both_owned_tasks_before_return(monkeypatch, failure):
    from process import control_cancel

    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())
    started, watching = asyncio.Event(), asyncio.Event()
    tasks, drained = [], []

    async def run(config, *, event):
        tasks.append(asyncio.current_task())
        started.set()
        try:
            await watching.wait()
            if failure == "work":
                raise RuntimeError("sensitive failure")
            await asyncio.Future()
        finally:
            await asyncio.sleep(0)
            drained.append("work")

    async def watch(ctx, task):
        tasks.append(asyncio.current_task())
        await started.wait()
        watching.set()
        try:
            if failure == "control cancel":
                raise control_cancel.ImportCancelledError("synthetic cancellation")
            await asyncio.Future()
        finally:
            await asyncio.sleep(0)
            drained.append("watcher")

    monkeypatch.setattr(worker, "run_registry_ptg_source_worker", run)
    monkeypatch.setattr(worker, "_watch_control_cancel", watch)
    control = asyncio.create_task(worker.control_start({}, {**event(), "run_id": "synthetic-run"}))
    await watching.wait()
    if failure == "outer cancel":
        control.cancel()
    error = {
        "work": RuntimeError,
        "control cancel": control_cancel.ImportCancelledError,
        "outer cancel": asyncio.CancelledError,
    }[failure]
    with pytest.raises(error) as caught:
        await control
    assert len(tasks) == 2 and all(task.done() for task in tasks)
    assert sorted(drained) == ["watcher", "work"]
    if failure == "work":
        assert str(caught.value) == "registry_ptg_source_worker_failed" and caught.value.__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["run id", "already cancelled"])
async def test_control_refuses_before_creating_owned_tasks(monkeypatch, failure):
    from process import control_cancel

    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    check = AsyncMock(side_effect=control_cancel.ImportCancelledError("synthetic cancellation"))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", check)
    run, watch = AsyncMock(), AsyncMock()
    monkeypatch.setattr(worker, "run_registry_ptg_source_worker", run)
    monkeypatch.setattr(worker, "_watch_control_cancel", watch)
    expected = RuntimeError if failure == "run id" else control_cancel.ImportCancelledError
    task_by_name = {**event(), "run_id": " invalid " if failure == "run id" else "synthetic-run"}
    with pytest.raises(expected):
        await worker.control_start({}, task_by_name)
    run.assert_not_awaited()
    watch.assert_not_awaited()
    assert check.await_count == (failure == "already cancelled")
