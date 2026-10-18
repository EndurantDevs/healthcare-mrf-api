# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed configuration, safe command output, and bounded owned worker resources."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from click.testing import CliRunner

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process as process_cli
from process import registry_cms_source_worker as worker

_PRIVATE = "synthetic-sensitive-marker"


def configuration():
    coordinates_by_name = dict(
        source_system="fhir",
        source_id="cms-npd",
        dataset_schema="retained_new",
        dataset_id="dataset",
        producer_id="producer",
        edition_id="edition-new",
    )
    return dict(
        execution_dsn="postgresql://registry_executor:{synthetic-password}@db.example.test/example",
        notification_dsn="postgresql://source_reader:{synthetic-password}@db.example.test:5432/example",
        source_endpoint_id="endpoint-one",
        source_schema="source_origin",
        receipt_id="a" * 64,
        notification_sha256="b" * 64,
        consumer=dict(
            control_schema="registry",
            execution_role="registry_executor",
            owner_role="registry_owner",
            coordinates=coordinates_by_name,
            replaced_coordinates=coordinates_by_name | {"edition_id": "edition-old"},
        ),
        notification_store=dict(
            control_schema="inbox",
            owner_role="inbox_owner",
            publisher_role="source_publisher",
            reader_roles=["source_reader"],
            source_attempt=dict(
                run_id="synthetic-run",
                attempt_id="synthetic-run:" + "c" * 32,
                attempt_started_at="2026-10-17T00:00:00+00:00",
            ),
        ),
    )


def parsed():
    return worker.parse_registry_cms_source_worker_config(json.dumps(configuration()))


def test_distinct_schemas_preserve_attempt_and_redact_credentials():
    config = parsed()
    assert config.consumer.control_schema == "registry"
    assert config.notification_store.control_schema == "inbox"
    assert config.notification_store.source_attempt.as_dict() == configuration()["notification_store"]["source_attempt"]
    assert "synthetic-password" not in repr(config)
    assert config.consumer.operation_seconds == 300 and config.consumer.max_attempts == 2


@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "missing",
        "host",
        "database",
        "port",
        "tls",
        "role",
        "reader",
        "owner",
        "dsn_type",
        "options",
        "coordinates",
        "attempt",
        "roles",
        "duplicate_roles",
        "receipt",
        "digest",
        "schema",
        "deadline_bool",
        "deadline_zero",
        "deadline_large",
        "attempts_bool",
        "attempts_large",
        "limits",
        "unknown_store",
    ],
)
def test_closed_configuration_refuses_before_resources(damage):
    configured_by_name = configuration()
    dsn = configured_by_name["notification_dsn"]
    replacements_by_case = {
        "unknown": ((), "unknown", _PRIVATE),
        "missing": ((), "source_schema", None),
        "host": ((), "notification_dsn", dsn.replace("db.example.test", "other.example.test")),
        "database": ((), "notification_dsn", dsn.replace("/example", "/other")),
        "port": ((), "notification_dsn", dsn.replace(":5432", ":5433")),
        "tls": ((), "notification_dsn", dsn + "?ssl=require"),
        "role": ((), "notification_dsn", configured_by_name["execution_dsn"]),
        "dsn_type": ((), "notification_dsn", True),
        "options": ((), "notification_dsn", dsn + "?options=-crole=other"),
        "reader": (("notification_store",), "reader_roles", ["other_reader"]),
        "owner": (("consumer",), "owner_role", "registry_executor"),
        "coordinates": (("consumer", "coordinates"), "extra", _PRIVATE),
        "attempt": (("notification_store", "source_attempt"), "extra", _PRIVATE),
        "roles": (("consumer",), "reader_roles", True),
        "duplicate_roles": (("notification_store",), "reader_roles", ["source_reader", "source_reader"]),
        "receipt": ((), "receipt_id", "A" * 64),
        "digest": ((), "notification_sha256", True),
        "schema": ((), "source_schema", "invalid;schema"),
        "deadline_bool": (("consumer",), "operation_seconds", True),
        "deadline_zero": (("consumer",), "operation_seconds", 0),
        "deadline_large": (("consumer",), "operation_seconds", 3601),
        "attempts_bool": (("consumer",), "max_attempts", True),
        "attempts_large": (("consumer",), "max_attempts", 4),
        "limits": ((), "sql_limits", {"statement_timeout_ms": 0}),
        "unknown_store": (("notification_store",), "unknown", _PRIVATE),
    }
    path, field, replacement = replacements_by_case[damage]
    target_by_name = configured_by_name
    for name in path:
        target_by_name = target_by_name[name]
    if damage == "missing":
        target_by_name.pop(field)
    else:
        target_by_name[field] = replacement
    with pytest.raises(ValueError, match="^registry_cms_source_worker_config_invalid$"):
        worker.parse_registry_cms_source_worker_config(json.dumps(configured_by_name))


@pytest.mark.parametrize(
    "encoded",
    [None, "null", "[]", "NaN", "{", " " * 16385, '{"consumer":{},"consumer":{}}', "[" * 10000 + "0" + "]" * 10000],
)
def test_json_refuses_invalid_duplicate_oversized_and_recursive_inputs(encoded):
    with pytest.raises(ValueError):
        worker.parse_registry_cms_source_worker_config(encoded)


def test_nested_duplicates_and_utf8_bytes_refuse():
    encoded = json.dumps(configuration()).replace(
        '"control_schema": "inbox"', '"control_schema":"inbox","control_schema":"inbox"'
    )
    with pytest.raises(ValueError):
        worker.parse_registry_cms_source_worker_config(encoded)
    value = configuration()
    value["source_schema"] = "é" * 8500
    with pytest.raises(ValueError):
        worker.parse_registry_cms_source_worker_config(json.dumps(value, ensure_ascii=False))


def test_direct_config_refuses_invalid_limits_and_digest_before_work():
    for changes in ({"sql_limits": object()}, {"notification_sha256": True}):
        with pytest.raises(ValueError):
            replace(parsed(), **changes)


class Connection:
    def __init__(self, role, *, wrong_database=False, in_transaction=False):
        self.role, self.wrong_database, self.transaction = role, wrong_database, in_transaction

    def is_in_transaction(self):
        return self.transaction

    async def fetchrow(self, query, *args):
        assert args == ("registry", "inbox")
        return dict(
            database_name="other" if self.wrong_database else "example",
            database_oid=1,
            server_address="127.0.0.1",
            server_port=5432,
            server_started="synthetic-start",
            control_oid=2,
            inbox_oid=3,
            current_role=self.role,
            session_role=self.role,
            safe_login=True,
        )

    async def fetchval(self, query):
        return "on" if "read_only" in query else "repeatable read"


class Pool:
    def __init__(self, harness):
        self.harness = harness

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == 5
        self.harness.events.append("acquire")
        try:
            yield self.harness.execution
        finally:
            self.harness.events.append("release")

    async def close(self):
        self.harness.events.append("pool close")
        if self.harness.fail_close:
            raise RuntimeError(_PRIVATE)

    def terminate(self):
        self.harness.events.append("pool terminate")


class Engine:
    sync_engine = object()

    def __init__(self, harness):
        self.harness = harness

    @asynccontextmanager
    async def connect(self):
        harness = self.harness
        harness.events.append("reader connect")
        harness.options["listener"](SimpleNamespace(run_async=harness.listeners.append), None, None)
        await harness.listeners[-1](harness.reader)
        if harness.fail_connect:
            raise RuntimeError(_PRIVATE)
        yield SimpleNamespace(get_raw_connection=AsyncMock())

    async def dispose(self):
        self.harness.events.append("reader dispose")


def resources(monkeypatch, *, wrong_database=False, fail_connect=False, fail_close=False):
    """Model pool/engine lifecycle without claiming a native database qualification."""
    harness = SimpleNamespace(
        events=[],
        options={},
        listeners=[],
        fail_connect=fail_connect,
        fail_close=fail_close,
        execution=Connection("registry_executor"),
        reader=Connection("source_reader", wrong_database=wrong_database),
    )
    verify = AsyncMock()
    monkeypatch.setattr(worker.RegistryPublicationSQLLimits, "verify", verify)

    async def create_pool(**kwargs):
        harness.options["pool"] = kwargs
        await kwargs["setup"](harness.execution)
        return Pool(harness)

    def create_engine(*args, **kwargs):
        harness.options["engine"] = kwargs
        return Engine(harness)

    def listen(_engine, name, callback):
        assert name == "checkout"
        harness.options["listener"] = callback

    sessions = object()
    monkeypatch.setattr(worker.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(worker, "create_async_engine", create_engine)
    monkeypatch.setattr(worker.event, "listen", listen)
    monkeypatch.setattr(worker, "async_sessionmaker", lambda engine: sessions)
    return harness.events, harness.options, verify, sessions


@pytest.mark.asyncio
@pytest.mark.parametrize("state", sorted(worker._STATES))
async def test_one_consume_has_finite_actual_checkout_limits_and_state_only_output(monkeypatch, state):
    events, options, verify, sessions = resources(monkeypatch)

    async def consume(**kwargs):
        assert kwargs["notification_sessions"] is sessions
        assert kwargs["receipt_id"] == "a" * 64
        assert not kwargs["connection"].is_in_transaction()
        events.append("consume")
        return {"state": state, "event_id": _PRIVATE, "manifest": {"private": _PRIVATE}}

    monkeypatch.setattr(worker, "consume_registry_cms_source_notification_once", consume)
    assert await worker.run_registry_cms_source_worker(parsed()) == {"state": state}
    assert events == ["acquire", "reader connect", "consume", "release", "reader dispose", "pool close"]
    assert verify.await_count == 2
    assert options["pool"]["min_size"] == options["pool"]["max_size"] == 1
    assert options["engine"]["pool_size"] == 1 and options["engine"]["max_overflow"] == 0
    assert options["engine"]["connect_args"]["server_settings"]["default_transaction_read_only"] == "on"
    assert options["engine"]["isolation_level"] == "REPEATABLE READ"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["database", "connect", "consumer", "cancel", "close", "result"])
async def test_failure_and_cancellation_drain_before_owned_cleanup(monkeypatch, failure):
    events, _options, _verify, _sessions = resources(
        monkeypatch,
        wrong_database=failure == "database",
        fail_connect=failure == "connect",
        fail_close=failure == "close",
    )

    async def consume(**kwargs):
        try:
            if failure == "cancel":
                raise asyncio.CancelledError
            if failure == "consumer":
                raise RuntimeError(_PRIVATE)
            return {"state": _PRIVATE if failure == "result" else "completed"}
        finally:
            await asyncio.sleep(0)
            events.append("consumer drained")

    monkeypatch.setattr(worker, "consume_registry_cms_source_notification_once", consume)
    with pytest.raises((ValueError, RuntimeError, asyncio.CancelledError)):
        await worker.run_registry_cms_source_worker(parsed())
    assert events.index("release") < events.index("reader dispose") < events.index("pool close")
    if failure not in {"database", "connect"}:
        assert events.index("consumer drained") < events.index("release")
    if failure == "close":
        assert events[-1] == "pool terminate"


@pytest.mark.asyncio
@pytest.mark.parametrize("role,transaction", [("other", False), ("registry_executor", True)])
async def test_execution_identity_refuses_wrong_actor_or_open_transaction(role, transaction):
    with pytest.raises(ValueError):
        await worker._identity(Connection(role, in_transaction=transaction), parsed(), "registry_executor")


def test_actual_command_registration_and_unsupported_argument_redaction():
    assert process_cli.process_group.commands["registry-cms-source-worker"] is worker.registry_cms_source_worker_command
    result = CliRunner().invoke(worker.registry_cms_source_worker_command, [_PRIVATE])
    assert result.exit_code == 2 and _PRIVATE not in result.output
    assert json.loads(result.output) == {"state": "error", "code": "invalid_arguments"}


@pytest.mark.parametrize(
    "failure,expected", [("config", "invalid_configuration"), ("work", "worker_failed"), ("cancel", "canceled")]
)
def test_command_redacts_credentials_and_exception_body(monkeypatch, capsys, failure, expected):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()) if failure != "config" else _PRIVATE)

    async def run(_config):
        if failure == "cancel":
            raise asyncio.CancelledError
        raise RuntimeError(_PRIVATE)

    monkeypatch.setattr(worker, "run_registry_cms_source_worker", run)
    assert worker.run_command() == {"config": 2, "work": 1, "cancel": 130}[failure]
    output = capsys.readouterr()
    assert not output.out and _PRIVATE not in output.err and "synthetic-password" not in output.err
    assert json.loads(output.err) == {"state": "error", "code": expected}


@pytest.mark.parametrize("endpoint", ["", True, " endpoint", "x" * 65, "é" * 33, "line\nend"])
def test_operator_endpoint_is_typed_and_byte_bounded(endpoint):
    configured = configuration()
    configured["source_endpoint_id"] = endpoint
    with pytest.raises(ValueError):
        worker.parse_registry_cms_source_worker_config(json.dumps(configured))


def test_explicit_cli_remains_compatible_without_periodic_endpoint():
    configured = configuration()
    configured.pop("source_endpoint_id")
    assert worker.parse_registry_cms_source_worker_config(json.dumps(configured)).source_endpoint_id is None


@pytest.mark.asyncio
async def test_identity_refuses_unsafe_login_before_work():
    connection = Connection("registry_executor")
    original = connection.fetchrow

    async def unsafe(*args):
        return await original(*args) | {"safe_login": False}

    connection.fetchrow = unsafe
    with pytest.raises(ValueError, match="connection_invalid"):
        await worker._identity(connection, parsed(), "registry_executor")
    for flag in (
        "rolcanlogin",
        "rolsuper",
        "rolcreatedb",
        "rolcreaterole",
        "rolreplication",
        "rolbypassrls",
        "pg_has_role",
    ):
        assert flag in worker._IDENTITY_SQL


@pytest.mark.asyncio
async def test_periodic_event_varies_receipt_but_retains_operator_scope_and_limits(monkeypatch):
    resources(monkeypatch)
    consume = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(worker, "consume_registry_cms_source_notification_once", consume)
    event_by_name = dict(source_schema="source_origin", receipt_id="c" * 64, notification_sha256="d" * 64)
    assert await worker.run_registry_cms_source_worker(parsed(), event=event_by_name) == {"state": "completed"}
    supplied = consume.await_args.kwargs
    assert {name: supplied[name] for name in worker.REGISTRY_CMS_EVENT_FIELDS} == event_by_name
    assert supplied["source_endpoint_id"] == "endpoint-one"
    assert supplied["config"] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["source", "endpoint", "coordinates"])
async def test_periodic_scope_attack_refuses_before_resources(monkeypatch, damage):
    pool = AsyncMock(side_effect=AssertionError("Unexpected connection"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pool)
    config = parsed()
    event_by_name = dict(source_schema="source_origin", receipt_id="c" * 64, notification_sha256="d" * 64)
    if damage == "source":
        event_by_name["source_schema"] = "foreign_source"
    elif damage == "endpoint":
        config = replace(config, source_endpoint_id=None)
    else:
        event_by_name["coordinates"] = {"edition_id": "unreviewed"}
    with pytest.raises(ValueError):
        await worker.run_registry_cms_source_worker(config, event=event_by_name)
    pool.assert_not_awaited()


@pytest.mark.parametrize("identity", ("source_publisher", "inbox_owner", "source_reader"))
def test_custody_refuses_all_notification_writer_and_reader_identities(identity):
    from process.registry_publication_worker import _custody_roles

    config = parsed()
    assert identity in _custody_roles(config)
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        replace(config, custody_url=config.execution_url.set(username=identity))
