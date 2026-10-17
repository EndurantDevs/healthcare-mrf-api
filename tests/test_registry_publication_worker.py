# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict worker configuration, actual registration and owned resource cleanup."""

import asyncio
import hashlib
import json
import os
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process as process_cli
from process import registry_publication_worker as worker
from process.registry_publication_authority import RegistryPublicationAuthorityClient

_ID = "01234567-89ab-4cde-8123-456789abcdef"
_PRIVATE = "synthetic-sensitive-marker"


def configuration():
    return {
        "lifecycle_dsn": "postgresql://worker_lifecycle:{synthetic-password}@db.example.test/example",
        "execution_dsn": "postgresql://worker_execution:{synthetic-password}@db.example.test:5432/example",
        "app_origin": "https://authority.example.test",
        "service_token": "example-service-token",
        "consumer": {
            "control_schema": "registry",
            "lifecycle_role": "worker_lifecycle",
            "execution_role": "worker_execution",
            "api_role": "registry_api",
            "queue_owner_role": "registry_queue_owner",
            "physical_owner_role": "registry_snapshot_owner",
            "loader_roles": [],
            "reader_roles": ["registry_reader"],
        },
    }


@pytest.mark.parametrize("mode", [None, "require", "verify-full"])
def test_configuration_matches_database_roles_and_tls_without_exposing_credentials(mode):
    configured = configuration()
    if mode:
        for name in ("lifecycle_dsn", "execution_dsn"):
            configured[name] += "?ssl=" + mode
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
    options = worker._pool_options(parsed.execution_url, 1)
    assert options["ssl"] == (mode or "prefer") and options["port"] == 5432
    assert options["min_size"] == options["max_size"] == 1
    assert "{synthetic-password}" not in repr(parsed) and "example-service-token" not in repr(parsed)
    configured["lifecycle_dsn"] = "postgresql://worker_lifecycle@db.example.test/example" + (
        "?ssl=" + mode if mode else ""
    )
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert worker._pool_options(parsed.lifecycle_url, 2)["password"] == ""


@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "missing",
        "user",
        "host",
        "port",
        "database",
        "tls",
        "option",
        "duplicate_ssl",
        "driver",
        "dsn_type",
        "roles",
        "duplicate_roles",
        "role_array",
        "origin",
        "token",
        "initial",
        "pin",
    ],
)
def test_configuration_refuses_ambiguous_or_unsafe_connection_inputs(damage):
    configured = configuration()
    dsn = configured["execution_dsn"]
    changed_by_damage = {
        "unknown": {"extra": _PRIVATE},
        "user": {"execution_dsn": configured["lifecycle_dsn"]},
        "host": {"execution_dsn": dsn.replace("db.example.test", "other.example.test")},
        "port": {"execution_dsn": dsn.replace(":5432", ":5433")},
        "database": {"execution_dsn": dsn.replace("/example", "/other")},
        "tls": {"execution_dsn": dsn + "?ssl=require"},
        "option": {"execution_dsn": dsn + "?options=-csearch_path=other"},
        "duplicate_ssl": {"execution_dsn": dsn + "?ssl=require&ssl=verify-full"},
        "driver": {"execution_dsn": "sqlite:///example"},
        "dsn_type": {"execution_dsn": True},
        "roles": {"consumer": configured["consumer"] | {"unknown": _PRIVATE}},
        "duplicate_roles": {"consumer": configured["consumer"] | {"api_role": "worker_execution"}},
        "role_array": {"consumer": configured["consumer"] | {"reader_roles": "registry_reader"}},
        "origin": {"app_origin": "https://user:password@authority.example.test/private"},
        "token": {"service_token": "token\n" + _PRIVATE},
        "initial": {"initial_address_sources": {"base_source": {}, "office_scope_store": {}}},
        "pin": {"initial_address_sources": {"base_source": {"extra": _PRIVATE}}},
    }
    if damage == "missing":
        configured.pop("service_token")
    else:
        configured.update(changed_by_damage[damage])
    with pytest.raises(ValueError, match="^registry_publication_worker_config_invalid$"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.parametrize(
    "encoded",
    ["null", "[]", "{", "NaN", "{" + '"consumer":{},' * 2 + '"x":0}', "[" * 10000 + "0" + "]" * 10000, " " * 16385],
)
def test_json_bounds_duplicate_keys_and_recursion_refuse_before_resources(encoded):
    with pytest.raises(ValueError, match="^registry_publication_worker_config_invalid$"):
        worker.parse_registry_publication_worker_config(encoded)


def test_nested_duplicate_keys_and_utf8_byte_bound():
    encoded = json.dumps(configuration()).replace(
        '"control_schema": "registry"', '"control_schema":"other","control_schema":"registry"'
    )
    with pytest.raises(ValueError):
        worker.parse_registry_publication_worker_config(encoded)
    configured = configuration()
    configured["service_token"] = "é" * 9000
    with pytest.raises(ValueError):
        worker.parse_registry_publication_worker_config(json.dumps(configured, ensure_ascii=False))


def test_initial_source_pins_are_syntax_only_and_preserve_exact_generation():
    configured = configuration()
    pin_by_field = {
        "schema_name": "retained_addresses",
        "table_name": "entity_address_unified",
        "generation_id": "retained-edition/one",
    }
    configured["initial_address_sources"] = {
        "base_source": pin_by_field,
        "npi_source": {**pin_by_field, "table_name": "npi"},
    }
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert parsed.initial_address_sources.base_source.generation_id == pin_by_field["generation_id"]
    assert parsed.initial_address_sources.npi_source.table_name == "npi"
    assert parsed.initial_address_sources.office_scope_store is None


class AuthorityBody(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body

    async def __aiter__(self):
        yield self.body


class Pool:
    def __init__(self, name, events, fail_close=False):
        self.name, self.events, self.fail_close = name, events, fail_close

    async def close(self):
        self.events.append(self.name + " close")
        if self.fail_close:
            raise RuntimeError(_PRIVATE)

    def terminate(self):
        self.events.append(self.name + " terminate")


class Authority:
    def __init__(self, origin, token, events):
        self.events = events

    async def __aenter__(self):
        self.events.append("authority open")
        return self

    async def __aexit__(self, *details):
        self.events.append("authority close")

    async def __call__(self, **request):
        raise AssertionError("Unexpected authority request")


def resources(monkeypatch, *, second_failure=False, close_failure=False):
    events, options = [], []
    monkeypatch.setattr(
        worker, "RegistryPublicationAuthorityClient", lambda origin, token: Authority(origin, token, events)
    )

    async def create_pool(**configured):
        options.append(configured)
        name = "lifecycle" if len(options) == 1 else "execution"
        if second_failure and len(options) == 2:
            raise RuntimeError(_PRIVATE)
        events.append(name + " open")
        return Pool(name, events, fail_close=close_failure and name == "execution")

    monkeypatch.setattr(worker.asyncpg, "create_pool", create_pool)
    return events, options


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["idle", "completed", "error", "cancel", "close_error"])
async def test_actual_runner_owns_fixed_pools_and_cleans_every_exit(monkeypatch, outcome):
    events, options = resources(monkeypatch, close_failure=outcome == "close_error")

    async def consume(**arguments):
        assert arguments["lifecycle_pool"] is not arguments["execution_pool"]
        assert arguments["config"].execution_role == "worker_execution"
        if outcome == "error":
            raise RuntimeError(_PRIVATE)
        if outcome == "cancel":
            raise asyncio.CancelledError(_PRIVATE)
        return None if outcome == "idle" else {"request_id": _ID, "state": "completed", "result": {"private": _PRIVATE}}

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    if outcome in {"error", "cancel", "close_error"}:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else RuntimeError):
            await worker.run_registry_publication_worker(config)
    else:
        result = await worker.run_registry_publication_worker(config)
        assert result == ({"state": "no_work"} if outcome == "idle" else {"request_id": _ID, "state": "completed"})
    assert [(entry["min_size"], entry["max_size"]) for entry in options] == [(1, 2), (1, 1)]
    expected_events = ["authority open", "lifecycle open", "execution open", "execution close"]
    if outcome == "close_error":
        expected_events.append("execution terminate")
    assert events == expected_events + ["lifecycle close", "authority close"]


@pytest.mark.asyncio
async def test_second_pool_startup_failure_closes_first_and_authority(monkeypatch):
    events, _ = resources(monkeypatch, second_failure=True)
    with pytest.raises(RuntimeError):
        await worker.run_registry_publication_worker(
            worker.parse_registry_publication_worker_config(json.dumps(configuration()))
        )
    assert events == ["authority open", "lifecycle open", "lifecycle close", "authority close"]


@pytest.mark.asyncio
async def test_runner_uses_real_authority_transport_without_returning_private_receipt(monkeypatch):
    events, _ = resources(monkeypatch)
    actor_by_name = {"kind": "platform_admin", "user_id": str(UUID(_ID)), "client_id": "system"}
    selected_records = [{"record_kind": "network", "record_id": 42, "revision": 1}]

    def handle(request):
        assert request.url.path == "/internal/v1/registry/publication/authorize"
        digest = hashlib.sha256(
            json.dumps(selected_records, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return httpx.Response(
            200,
            headers={"cache-control": "private, no-store", "content-type": "application/json"},
            stream=AuthorityBody(
                json.dumps(
                    {"authorized": True, "actor": actor_by_name, "selection_sha256": digest, "policy_revision": 1}
                ).encode()
            ),
        )

    monkeypatch.setattr(
        worker,
        "RegistryPublicationAuthorityClient",
        lambda origin, token: RegistryPublicationAuthorityClient(origin, token, transport=httpx.MockTransport(handle)),
    )

    async def consume(**arguments):
        receipt = await arguments["authorize"](
            session_token_sha256="a" * 64, actor=actor_by_name, selection=selected_records
        )
        assert receipt["authorized"] is True
        return {"request_id": _ID, "state": "completed", "result": {"private": _PRIVATE}}

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    summary_by_name = await worker.run_registry_publication_worker(
        worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    )
    assert summary_by_name == {"request_id": _ID, "state": "completed"} and _PRIVATE not in json.dumps(summary_by_name)
    assert events == ["lifecycle open", "execution open", "execution close", "lifecycle close"]


@pytest.mark.parametrize(
    "failure,code,status",
    [("invalid", "invalid_configuration", 2), ("runtime", "worker_failed", 1), ("cancel", "canceled", 130)],
)
def test_command_errors_never_echo_configuration_or_exception(monkeypatch, capsys, failure, code, status):
    monkeypatch.setenv(worker.CONFIG_ENV, _PRIVATE if failure == "invalid" else json.dumps(configuration()))

    async def run(_config):
        if failure == "cancel":
            raise asyncio.CancelledError(_PRIVATE)
        raise RuntimeError(_PRIVATE)

    monkeypatch.setattr(worker, "run_registry_publication_worker", run)
    assert worker.run_command() == status
    captured = capsys.readouterr()
    assert (
        captured.out == ""
        and captured.err == json.dumps({"state": "error", "code": code}, separators=(",", ":")) + "\n"
    )


def test_actual_process_group_registration_and_argument_redaction(monkeypatch):
    calls = []
    monkeypatch.setattr(worker, "run_command", lambda: calls.append(True) or 0)
    assert (
        process_cli.process_group.commands["registry-publication-worker"] is worker.registry_publication_worker_command
    )
    result = CliRunner().invoke(process_cli.process_group, ["registry-publication-worker"])
    assert result.exit_code == 0 and calls == [True]
    result = CliRunner().invoke(process_cli.process_group, ["registry-publication-worker", "--credential=" + _PRIVATE])
    assert result.exit_code == 2 and _PRIVATE not in result.output and "invalid_arguments" in result.output
    assert calls == [True]


@pytest.mark.parametrize(
    "dsn",
    ["not-a-url-" + _PRIVATE, "postgresql://worker_execution:{synthetic-password}@db.example.test:invalid/example"],
)
def test_malformed_dsn_errors_are_redacted(dsn):
    configured = configuration()
    configured["execution_dsn"] = dsn
    with pytest.raises(ValueError, match="^registry_publication_worker_config_invalid$"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.parametrize(
    "bad_result",
    [
        {"request_id": "private", "state": "completed", "result": {}},
        {"request_id": _ID, "state": "unknown", "result": _PRIVATE},
        {"request_id": _ID, "state": "completed", "result": {}, "private": _PRIVATE},
    ],
)
def test_unknown_consumer_metadata_is_not_printed(bad_result):
    with pytest.raises(ValueError):
        worker._summary(bad_result)


def ptg_configuration():
    configured = configuration()
    configured["ptg"] = {
        "owner_role": "registry_queue_owner",
        "approval_role": "registry_scope_approver",
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100},
    }
    return configured


@pytest.mark.parametrize(
    "damage", [None, "null", "unknown", "missing", "role", "same_role", "limits", "bound", "runtime"]
)
def test_ptg_configuration_is_closed_bounded_and_cannot_supply_runtime(damage):
    configured = ptg_configuration()
    if damage == "null":
        configured["ptg"] = None
    if damage == "unknown":
        configured["ptg"]["dsn"] = _PRIVATE
    if damage == "missing":
        configured["ptg"].pop("approval_role")
    if damage == "role":
        configured["ptg"]["owner_role"] = "invalid role"
    if damage == "same_role":
        configured["ptg"]["approval_role"] = configured["ptg"]["owner_role"]
    if damage == "limits":
        configured["ptg"]["read_limits"]["extra"] = 1
    if damage == "bound":
        configured["ptg"]["read_limits"]["maximum_bytes"] = True
    if damage == "runtime":
        configured["consumer"]["composition_runtime"] = {}
    if damage is not None:
        with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
            worker.parse_registry_publication_worker_config(json.dumps(configured))
    else:
        parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
        assert parsed.consumer.composition_runtime is None
        assert parsed.ptg.store.control_schema == parsed.consumer.control_schema
        assert parsed.ptg.read_budget.maximum_bytes == 1048576


def _observe_ptg_engine(monkeypatch, config, events, password):
    """Inspect actual driver arguments while observing the owned engine lifetime."""
    from sqlalchemy.ext.asyncio import AsyncEngine

    original_create, original_dispose = worker.create_async_engine, AsyncEngine.dispose
    engines = []

    def create(url, **arguments):
        assert url == config.execution_url
        assert arguments == {
            "connect_args": {"password": password or ""},
            "pool_size": 1,
            "max_overflow": 0,
            "hide_parameters": True,
        }
        engine = original_create(url, **arguments)
        assert (engine.dialect.create_connect_args(engine.url)[1] | arguments["connect_args"])["password"] == (
            password or ""
        )
        engines.append(engine)
        events.append("sqlalchemy open")
        return engine

    async def dispose(engine, *arguments, **keywords):
        events.append("sqlalchemy close")
        await original_dispose(engine, *arguments, **keywords)

    monkeypatch.setattr(worker, "create_async_engine", create)
    monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    return engines


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["idle", "error", "cancel"])
@pytest.mark.parametrize("password", [None, "configured-password"])
async def test_ptg_assembly_uses_execution_url_and_disposes_genuine_single_engine(monkeypatch, outcome, password):
    """Keep explicit credentials and close the genuine engine for every terminal outcome."""
    from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime

    events, options = resources(monkeypatch)
    configured = ptg_configuration()
    configured["execution_dsn"] = (
        "postgresql://worker_execution" + (":" + password if password else "") + "@db.example.test:5432/example"
    )
    config = worker.parse_registry_publication_worker_config(json.dumps(configured))
    monkeypatch.setenv("PGPASSWORD", "unrelated-ambient-password")

    engines = _observe_ptg_engine(monkeypatch, config, events, password)

    async def consume(**arguments):
        runtime = arguments["config"].composition_runtime
        assert type(runtime) is RegistryPublicationPTGRuntime
        assert runtime.sessions.kw["bind"] is engines[0]
        assert runtime.sessions.kw["bind"].pool.size() == 1 and runtime.sessions.kw["bind"].pool._max_overflow == 0
        if outcome == "error":
            raise RuntimeError(_PRIVATE)
        if outcome == "cancel":
            raise asyncio.CancelledError
        return None

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    if outcome == "idle":
        assert await worker.run_registry_publication_worker(config) == {"state": "no_work"}
    else:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else RuntimeError):
            await worker.run_registry_publication_worker(config)
    assert [(entry["min_size"], entry["max_size"]) for entry in options] == [(1, 2), (1, 1)]
    assert events == [
        "authority open",
        "lifecycle open",
        "execution open",
        "sqlalchemy open",
        "sqlalchemy close",
        "execution close",
        "lifecycle close",
        "authority close",
    ]
