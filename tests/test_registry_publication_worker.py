# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict worker configuration, actual registration and owned resource cleanup."""

import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process as process_cli
from process import registry_publication_consumer as consumer
from process import registry_publication_ptg_runtime as ptg_runtime
from process import registry_publication_worker as worker
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource
from process.registry_publication_authority import RegistryPublicationAuthorityClient
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes

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


def test_publication_custody_census_matches_existing_role_contract():
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    expected = ("registry_api", "registry_reader", "registry_snapshot_owner", "worker_execution", "worker_lifecycle")
    assert parsed.consumer.custody_roles == worker._custody_roles(parsed) == expected


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


@pytest.mark.parametrize("compact", [False, True, 0, 1, None, "true", {}, []])
def test_initial_compact_selection_is_explicit_and_strict(compact):
    configured = configuration()
    configured["initial_address_sources"] = {
        "base_source": {
            "schema_name": "address_example",
            "table_name": "entity_address_unified",
            "generation_id": "a" * 64,
        },
        "compact": compact,
    }
    if type(compact) is not bool:
        with pytest.raises(ValueError):
            worker.parse_registry_publication_worker_config(json.dumps(configured))
    else:
        parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
        assert parsed.initial_address_sources.compact is compact


def _configured_recipe(kind="fhir"):
    coordinates = RegistryNetworkSourceCoordinates(
        kind, "cms-npd" if kind == "fhir" else "source-example", "source_example", "dataset-one", "producer", "edition"
    )
    if kind == "fhir":
        pin = PinnedFHIRMembershipSource(
            "source_example",
            "cms-npd",
            "endpoint",
            "dataset-one",
            "a" * 64,
            "e" * 64,
            123,
            "medical",
            "2026-10-01",
            custody_owner_role="source_owner",
            custody_runtime_roles=("source_reader",),
            custody_proof_sha256="b" * 64,
            custody_catalog_sha256="c" * 64,
        )
    else:
        pin = PinnedPTGOfficeMembershipSource(_ID, "example-client", "a" * 64, "b" * 64, "c" * 64, coordinates, 1, 1)
    return RegistrySourceMembershipRecipe(pin, coordinates)


@pytest.mark.parametrize("phase", ["initial_source_recipes", "replacement_source_recipes"])
@pytest.mark.parametrize("kind", ["fhir", "ptg"])
def test_configuration_decodes_closed_typed_source_recipes(phase, kind):
    recipe = _configured_recipe(kind)
    configured = configuration()
    configured["initial_address_sources"] = {
        "base_source": {"schema_name": "addresses", "table_name": "entity_address_unified", "generation_id": "a" * 64},
        phase: json.loads(canonical_registry_source_recipes((recipe,))),
    }
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert getattr(parsed.initial_address_sources, phase) == (recipe,)
    assert parsed.initial_address_sources.introduced_source_recipes == (recipe,)
    assert parsed.initial_address_sources.office_scope_store is None


@pytest.mark.parametrize("phase", ["initial_source_recipes", "replacement_source_recipes"])
@pytest.mark.parametrize(
    "damage",
    [
        "not_array",
        "null",
        "json_string",
        "unknown",
        "kind",
        "pin",
        "coordinates",
        "unprotected",
        "unsupported",
        "duplicate",
        "both",
    ],
)
def test_configuration_refuses_unqualified_or_cross_source_recipes(phase, damage):
    recipe = json.loads(canonical_registry_source_recipes((_configured_recipe(),)))[0]
    configured = configuration()
    sources_by_name = {
        "base_source": {"schema_name": "addresses", "table_name": "entity_address_unified", "generation_id": "a" * 64},
        phase: [recipe],
    }
    match damage:
        case "not_array":
            sources_by_name[phase] = recipe
        case "null":
            sources_by_name[phase] = None
        case "json_string":
            sources_by_name[phase] = json.dumps([recipe])
        case "unknown":
            recipe["unexpected"] = _PRIVATE
        case "kind":
            recipe["source_kind"] = "unknown"
        case "pin":
            recipe["source_pin"]["unexpected"] = _PRIVATE
        case "coordinates":
            recipe["binding_coordinates"]["dataset_id"] = "other-dataset"
        case "unprotected":
            for name in tuple(recipe["source_pin"]):
                if name.startswith("custody_"):
                    recipe["source_pin"].pop(name)
        case "unsupported":
            recipe["source_pin"]["source_id"] = recipe["binding_coordinates"]["source_id"] = "other-source"
        case "duplicate":
            sources_by_name[phase].append(recipe)
        case _:
            sources_by_name[
                "replacement_source_recipes" if phase == "initial_source_recipes" else "initial_source_recipes"
            ] = [recipe]
    configured["initial_address_sources"] = sources_by_name
    with pytest.raises(ValueError, match="^registry_publication_worker_config_invalid$"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


def test_empty_recipe_arrays_preserve_legacy_address_configuration():
    configured = configuration()
    configured["initial_address_sources"] = {
        "base_source": {"schema_name": "addresses", "table_name": "entity_address_unified", "generation_id": "a" * 64}
    }
    legacy = worker.parse_registry_publication_worker_config(json.dumps(configured))
    configured["initial_address_sources"].update(initial_source_recipes=[], replacement_source_recipes=[])
    assert worker.parse_registry_publication_worker_config(json.dumps(configured)) == legacy


class AuthorityBody(httpx.AsyncByteStream):
    def __init__(self, body):
        self.body = body

    async def __aiter__(self):
        yield self.body


class Pool:
    def __init__(self, name, events, fail_close=False):
        self.name, self.events, self.fail_close = name, events, fail_close

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == 5
        yield object()

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
    monkeypatch.setattr(worker.RegistryPublicationPTGRuntime, "qualify", AsyncMock())
    monkeypatch.setattr(
        worker, "_qualify_fresh_backends", AsyncMock(return_value=SimpleNamespace(verifier=lambda role: AsyncMock()))
    )
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


def _backend_connection(role="worker_execution", **changes):
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    observed_by_name = {
        "database_name": "example",
        "database_oid": 10,
        "server_address": "127.0.0.1",
        "server_port": 5432,
        "server_started": observed_at - timedelta(days=1),
        "control_oid": 20,
        "inbox_oid": 20,
        "current_role": role,
        "session_role": role,
        "safe_login": True,
        "assumed_role": "none",
        "pid": 100,
        "backend_start": observed_at,
        "observed_at": observed_at + timedelta(seconds=1),
        "backend_type": "client backend",
        "datid": 10,
        "usesysid": 30 if role == "worker_execution" else 31,
        "role_oids": json.dumps({"worker_execution": 30, "worker_lifecycle": 31}),
    }
    observed_by_name.update(changes)
    return SimpleNamespace(
        fetchrow=AsyncMock(return_value=observed_by_name), is_in_transaction=lambda: False, get_server_pid=lambda: 100
    )


async def _fresh_qualification():
    lifecycle = _backend_connection("worker_lifecycle")
    return await worker._qualify_fresh_backends(
        lifecycle, schema="registry", observer_role="worker_lifecycle", execution_roles=("worker_execution",)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "old",
        "server",
        "database",
        "control",
        "role_oid",
        "missing_role",
        "pid",
        "login_oid",
        "datid",
        "set_role",
        "session_role",
        "unsafe",
        "transaction",
        "missing",
        "start_missing",
        "future_start",
        "backend_type",
    ],
)
async def test_backend_verifier_refuses_changed_identity(damage):
    qualification = await _fresh_qualification()
    connection = _backend_connection(backend_start=qualification.not_before)
    observed_by_name = connection.fetchrow.return_value
    changes_by_case = {
        "old": {"backend_start": qualification.not_before - timedelta(microseconds=1)},
        "server": {"server_started": qualification.not_before},
        "database": {"database_oid": 11, "datid": 11},
        "control": {"control_oid": 21},
        "role_oid": {"role_oids": '{"worker_execution":32,"worker_lifecycle":31}', "usesysid": 32},
        "missing_role": {"role_oids": '{"worker_execution":30}'},
        "pid": {"pid": 101},
        "login_oid": {"usesysid": 31},
        "datid": {"datid": 11},
        "set_role": {"assumed_role": "worker_execution"},
        "session_role": {"session_role": "worker_lifecycle"},
        "unsafe": {"safe_login": False},
        "start_missing": {"backend_start": None},
        "future_start": {"backend_start": qualification.not_before + timedelta(days=1)},
        "backend_type": {"backend_type": "parallel worker"},
    }
    observed_by_name.update(changes_by_case.get(damage, {}))
    if damage == "transaction":
        connection.is_in_transaction = lambda: True
    if damage == "missing":
        connection.fetchrow.return_value = None
    with pytest.raises(ValueError, match="registry_publication_worker_backend_invalid"):
        await qualification.verifier("worker_execution")(connection)


@pytest.mark.asyncio
async def test_backend_verifier_accepts_new_reconnections():
    qualification = await _fresh_qualification()
    verify = qualification.verifier("worker_execution")
    for seconds in (0, 1):
        started = qualification.not_before + timedelta(seconds=seconds)
        connection = _backend_connection(backend_start=started, observed_at=started)
        await verify(connection)
        assert connection.fetchrow.await_args.args[1:] == (
            "registry",
            "registry",
            ("worker_execution", "worker_lifecycle"),
        )
    with pytest.raises(ValueError, match="backend_invalid"):
        qualification.verifier("unconfigured_actor")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unsafe", "assumed", "missing_role", "missing_schema"])
async def test_backend_anchor_requires_direct_lifecycle(damage):
    changes_by_case = {
        "unsafe": {"safe_login": False},
        "assumed": {"assumed_role": "worker_lifecycle"},
        "missing_role": {"role_oids": '{"worker_lifecycle":31}'},
        "missing_schema": {"control_oid": None},
    }
    with pytest.raises(ValueError, match="backend_invalid"):
        await worker._qualify_fresh_backends(
            _backend_connection("worker_lifecycle", **changes_by_case[damage]),
            schema="registry",
            observer_role="worker_lifecycle",
            execution_roles=("worker_execution",),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError, asyncio.CancelledError])
async def test_backend_anchor_failure_closes_before_execution(monkeypatch, failure):
    events, options = resources(monkeypatch)
    monkeypatch.setattr(worker, "_qualify_fresh_backends", AsyncMock(side_effect=failure("backend_invalid")))
    consume = AsyncMock()
    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    with pytest.raises(failure):
        await worker.run_registry_publication_worker(
            worker.parse_registry_publication_worker_config(json.dumps(configuration()))
        )
    assert len(options) == 1 and consume.await_count == 0
    assert events == ["authority open", "lifecycle open", "lifecycle close", "authority close"]


@pytest.mark.asyncio
async def test_backend_verification_retains_sql_limits():
    config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    calls = []

    async def verify_limits(connection):
        calls.append(("limits", connection))

    async def verify_backend(connection):
        calls.append(("backend", connection))

    limits = SimpleNamespace(
        verify=verify_limits, connect_timeout_seconds=5, statement_timeout_ms=1000, server_settings=lambda: {}
    )
    options = worker._pool_options(config.execution_url, 1, limits, backend_verifier=verify_backend)
    connection = object()
    await options["init"](connection)
    await options["setup"](connection)
    assert calls == [("limits", connection), ("backend", connection)] * 2


@pytest.mark.asyncio
async def test_postclaim_backend_refusal_keeps_the_same_durable_request_resumable(monkeypatch):
    from process import registry_publication_consumer as consumer

    qualification = await _fresh_qualification()
    checkout_verifier = AsyncMock(wraps=worker._connection_verifier(None, qualification.verifier("worker_execution")))
    checkout_numbers = count(1)

    @asynccontextmanager
    async def acquire():
        connection = _backend_connection(backend_start=qualification.not_before)
        if next(checkout_numbers) == 2:
            connection.fetchrow.return_value["server_started"] = qualification.not_before
        await checkout_verifier(connection)
        yield connection

    config = worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer
    request_by_field = {
        "request_id": UUID(_ID),
        "lease_id": UUID("11234567-89ab-4cde-8123-456789abcdef"),
        "command_json": {"operation": None},
    }
    original_request_by_field = deepcopy(request_by_field)
    claim = AsyncMock(return_value=request_by_field)
    reject = AsyncMock()
    complete = AsyncMock(return_value={"generation": 7})
    monkeypatch.setattr(consumer, "_verify_execution", AsyncMock())
    monkeypatch.setattr(consumer, "_claim", claim)
    monkeypatch.setattr(consumer, "_reject", reject)
    monkeypatch.setattr(consumer, "_ordinary_publication", complete)

    async def heartbeat(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(consumer, "_heartbeat", heartbeat)
    arguments_by_field = dict(
        lifecycle_pool=object(), execution_pool=SimpleNamespace(acquire=acquire), authorize=AsyncMock(), config=config
    )
    with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable"):
        await consumer.consume_registry_publication_once(**arguments_by_field)
    reject.assert_not_awaited()
    complete.assert_not_awaited()
    assert request_by_field == original_request_by_field and claim.await_count == 1
    assert checkout_verifier.await_count == 2
    completed_by_field = await consumer.consume_registry_publication_once(**arguments_by_field)
    assert completed_by_field == {"request_id": _ID, "state": "completed", "result": {"generation": 7}}
    assert (
        request_by_field == original_request_by_field and claim.await_count == 2 and checkout_verifier.await_count == 4
    )
    reject.assert_not_awaited()
    complete.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["initial_source_recipes", "replacement_source_recipes"])
async def test_worker_forwards_typed_recipes_without_opening_an_alternate_source_path(monkeypatch, phase):
    resources(monkeypatch)
    configured = configuration()
    recipe = _configured_recipe()
    configured["initial_address_sources"] = {
        "base_source": {"schema_name": "addresses", "table_name": "entity_address_unified", "generation_id": "a" * 64},
        phase: json.loads(canonical_registry_source_recipes((recipe,))),
    }
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))

    async def consume(**arguments):
        assert arguments["initial_address_sources"] is parsed.initial_address_sources
        assert getattr(arguments["initial_address_sources"], phase) == (recipe,)
        return None

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    assert await worker.run_registry_publication_worker(parsed) == {"state": "no_work"}


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
            "connect_args": {
                "password": password or "",
                "timeout": config.sql_limits.connect_timeout_seconds,
                "command_timeout": config.sql_limits.statement_timeout_ms / 1000
                + config.sql_limits.connect_timeout_seconds,
                "server_settings": config.sql_limits.server_settings(),
            },
            "pool_timeout": config.sql_limits.connect_timeout_seconds,
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
        assert arguments["config"].custody_roles == worker._custody_roles(config)
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


@pytest.mark.parametrize("seconds", [1, 3600, 86400])
def test_operation_deadline_preserves_config_and_positional_contract(seconds):
    configured = configuration()
    legacy = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert legacy.operation_seconds == 3600
    positional = worker.RegistryPublicationWorkerConfig(
        legacy.lifecycle_url,
        legacy.execution_url,
        legacy.app_origin,
        legacy.service_token,
        legacy.consumer,
        legacy.initial_address_sources,
        legacy.ptg,
    )
    assert positional.operation_seconds == 3600
    configured["operation_seconds"] = seconds
    parsed = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert parsed.operation_seconds == seconds


@pytest.mark.parametrize("seconds", [True, False, None, "1", 1.0, 0, -1, 86401])
def test_operation_deadline_rejects_invalid_direct_and_json_config(monkeypatch, seconds):
    config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    from dataclasses import replace

    pools = AsyncMock(side_effect=AssertionError("invalid deadline must not open resources"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pools)
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        replace(config, operation_seconds=seconds)
    configured = configuration()
    configured["operation_seconds"] = seconds
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))
    pools.assert_not_awaited()


@pytest.mark.asyncio
async def test_operation_timeout_drains_before_owned_pool_and_ptg_cleanup(monkeypatch):
    events, options = resources(monkeypatch)
    configured = ptg_configuration()
    configured["operation_seconds"] = 1
    config = worker.parse_registry_publication_worker_config(json.dumps(configured))
    _observe_ptg_engine(monkeypatch, config, events, "{synthetic-password}")
    completed = asyncio.Event()

    async def consume(**arguments):
        try:
            await asyncio.Event().wait()
        finally:
            events.append("work drain")
            await asyncio.sleep(0.02)
            completed.set()
            events.append("work drained")

    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    with pytest.raises(TimeoutError):
        await worker.run_registry_publication_worker(config)
    assert completed.is_set()
    assert events == [
        "authority open",
        "lifecycle open",
        "execution open",
        "sqlalchemy open",
        "work drain",
        "work drained",
        "sqlalchemy close",
        "execution close",
        "lifecycle close",
        "authority close",
    ]
    assert [(entry["min_size"], entry["max_size"]) for entry in options] == [(1, 2), (1, 1)]


@pytest.mark.parametrize("field", list(asdict(worker.RegistryPublicationSQLLimits())))
@pytest.mark.parametrize("value", [True, False, None, "5", 1.5, 0, -1, 2**63])
def test_closed_sql_limits_reject_invalid_values_before_resources(field, value):
    configured = configuration()
    configured["sql_limits"] = asdict(worker.RegistryPublicationSQLLimits()) | {field: value}
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))
    with pytest.raises(ValueError, match="registry_publication_worker_sql_limits_invalid"):
        worker.RegistryPublicationSQLLimits(**configured["sql_limits"])


@pytest.mark.parametrize("value", [None, [], {}, {"work_mem": "16MB"}])
def test_closed_sql_limits_reject_partial_or_arbitrary_guc_blocks(value):
    configured = configuration()
    configured["sql_limits"] = value
    with pytest.raises(ValueError):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


def test_sql_limits_default_explicit_and_eight_positional_compatibility():
    default = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    positional = worker.RegistryPublicationWorkerConfig(
        default.lifecycle_url,
        default.execution_url,
        default.app_origin,
        default.service_token,
        default.consumer,
        default.initial_address_sources,
        default.ptg,
        default.operation_seconds,
    )
    assert positional.sql_limits == default.sql_limits
    configured = configuration()
    configured["sql_limits"] = asdict(replace(default.sql_limits, statement_timeout_ms=1000))
    explicit = worker.parse_registry_publication_worker_config(json.dumps(configured))
    assert explicit.sql_limits.statement_timeout_ms == 1000
    options = worker._pool_options(explicit.execution_url, 1, explicit.sql_limits)
    assert options["server_settings"] == explicit.sql_limits.server_settings()
    assert "temp_file_limit" not in options["server_settings"]
    assert options["timeout"] == 5 and options["command_timeout"] == 6
    assert options["init"] == explicit.sql_limits.verify
    assert options["setup"] == explicit.sql_limits.verify
    with pytest.raises(ValueError):
        replace(default, sql_limits=asdict(default.sql_limits))


@pytest.mark.parametrize(
    "field",
    [
        "work_mem_bytes",
        "maintenance_work_mem_bytes",
        "temp_file_limit_bytes",
        "statement_timeout_ms",
        "lock_timeout_ms",
        "parallel_query",
        "parallel_maintenance",
    ],
)
@pytest.mark.parametrize("value", [-1, 0, 2**63])
async def test_sql_limits_verify_actual_effective_values(field, value):
    limits = worker.RegistryPublicationSQLLimits()
    values = asdict(limits)
    values.pop("connect_timeout_seconds")
    values.update(parallel_query=0, parallel_maintenance=0, replication_role="origin")
    values[field] = value
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=values))
    if field in {"parallel_query", "parallel_maintenance"} and value == 0:
        await limits.verify(connection)
    else:
        with pytest.raises(RuntimeError, match="registry_publication_worker_sql_limits_unavailable"):
            await limits.verify(connection)


@pytest.mark.parametrize("mode", ["replica", "local", "", None])
async def test_sql_limits_refuse_non_origin_backend(mode):
    limits = worker.RegistryPublicationSQLLimits()
    values = asdict(limits)
    values.pop("connect_timeout_seconds")
    values.update(parallel_query=0, parallel_maintenance=0, replication_role=mode)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=values))
    with pytest.raises(RuntimeError, match="registry_publication_worker_sql_limits_unavailable"):
        await limits.verify(connection)


async def test_bounded_pool_preserves_explicit_stricter_checkout():
    pool = SimpleNamespace(acquire=lambda **kwargs: kwargs)
    bounded = worker._BoundedPool(pool, 5)
    assert bounded.acquire() == dict(timeout=5)
    assert bounded.acquire(timeout=2) == dict(timeout=2)
    assert bounded.acquire(timeout=30) == dict(timeout=5)


class _CandidateSealConnection:
    def __init__(self, candidate=None):
        self.active = False
        self.candidate = candidate
        self.transaction_options = []
        self.active_options = None

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active
        self.transaction_options.append(dict(options))
        self.active_options = dict(options)
        self.active = True
        try:
            yield self
        finally:
            self.active = False
            self.active_options = None

    async def fetchrow(self, *args):
        return self.candidate


def _queued_publication_phases(monkeypatch, outcome):
    """Install the phase assertions used by queued custody success and refusal cases."""
    from process import registry_publication_execution as execution

    events = []
    connection = _CandidateSealConnection({"approved_custom_revision": 2, "expected_head": 1})
    request_by_name = {
        "request_id": UUID(_ID),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 1},
    }
    requests = AsyncMock(
        side_effect=[
            request_by_name,
            request_by_name,
            {**request_by_name, "request_sha256": "b" * 64} if outcome == "lease" else request_by_name,
        ]
    )
    monkeypatch.setattr(execution, "_request", requests)

    async def prepare(*args, **kwargs):
        assert not connection.active
        events.append("prepared")

    monkeypatch.setattr(execution, "prepare_network_candidate", prepare)

    async def authorize(*args):
        if not connection.active:
            events.append("authorized")
        elif outcome == "revoked":
            raise ValueError("authority_revoked")
        return 3

    monkeypatch.setattr(execution, "_fresh_authority", authorize)

    async def seal(target):
        assert not connection.active and events == ["prepared", "authorized"]
        events.append("sealed")
        if outcome == "failure":
            raise ValueError("custody_changed")
        if outcome == "cancel":
            raise asyncio.CancelledError

    monkeypatch.setattr(execution, "_lock_publication_controls", AsyncMock())
    monkeypatch.setattr(execution, "_locked_candidate", AsyncMock())
    monkeypatch.setattr(execution, "_approval", AsyncMock(return_value={"approved_revision": 2}))
    monkeypatch.setattr(execution, "_source_selection_counts", lambda row: {})

    async def publish(*args, **kwargs):
        assert connection.active and (outcome == "legacy" or events[-1] == "sealed")
        return {"generation_id": 2}

    publisher = AsyncMock(side_effect=publish)
    finish = AsyncMock()
    monkeypatch.setattr(execution, "publish_network_candidate", publisher)
    monkeypatch.setattr(execution, "finish_registry_publication_request", finish)
    return connection, publisher, finish, seal


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "failure", "cancel", "lease", "revoked"])
async def test_queued_publication_seals_after_commit_and_authority_before_atomic_finish(monkeypatch, outcome):
    from process import registry_publication_execution as execution

    connection, publisher, finish, seal = _queued_publication_phases(monkeypatch, outcome)
    operation = execution.complete_queued_registry_publication(
        connection,
        UUID(_ID),
        UUID(int=2),
        SimpleNamespace(candidate_id=_ID),
        object(),
        authorize=object(),
        writer_context=replace(
            worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer,
            control_schema="control",
        ),
        seal_candidate=None if outcome == "legacy" else seal,
    )
    if outcome in {"legacy", "complete"}:
        assert (await operation)["serving_generation"] == 2
        assert publisher.await_count == finish.await_count == 1
    else:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else ValueError):
            await operation
        publisher.assert_not_awaited()
        finish.assert_not_awaited()
    assert not connection.active


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_source_rollback_seals_before_review_hash_without_modifying_validation(monkeypatch, failure):
    from process import registry_publication_execution as execution

    connection = _CandidateSealConnection()
    events = []
    copy_target, address = object(), object()
    request_by_name = {"request_id": UUID(_ID)}
    command = SimpleNamespace(expected_approved_revision=2, expected_serving_generation=1)
    monkeypatch.setattr(execution, "_request", AsyncMock(return_value=request_by_name))
    monkeypatch.setattr(execution, "_fresh_retained_operation_authority", AsyncMock(return_value=command))
    monkeypatch.setattr(execution, "_source_rollback_pins", AsyncMock(return_value=(object(), object())))
    monkeypatch.setattr(execution, "_source_rollback_address_sources", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        execution, "compose_registry_membership_candidate", AsyncMock(return_value=(copy_target, address))
    )

    async def prepare(*args, **kwargs):
        assert not connection.active
        events.append("prepared")

    monkeypatch.setattr(execution, "prepare_network_candidate", prepare)

    async def seal(actual):
        assert actual is copy_target and not connection.active and events == ["prepared"]
        events.append("sealed")
        if failure:
            raise ValueError("custody_changed")

    async def preview(*args):
        assert connection.active and events == ["prepared", "sealed"]
        return {"candidate_sha256": "c" * 64}

    previewer = AsyncMock(side_effect=preview)
    finish = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(execution, "_source_rollback_preview_result", previewer)
    monkeypatch.setattr(execution, "_finish_source_rollback_preview", finish)
    operation = execution.complete_queued_registry_source_rollback_preview(
        connection,
        UUID(_ID),
        UUID(int=2),
        authorize=object(),
        writer_roles={},
        control_schema="control",
        seal_candidate=seal,
    )
    if failure:
        with pytest.raises(ValueError, match="custody_changed"):
            await operation
        previewer.assert_not_awaited()
        finish.assert_not_awaited()
    else:
        assert await operation == {"state": "completed"}


@pytest.mark.asyncio
@pytest.mark.parametrize("sealed", [False, True])
async def test_rollback_hash_binds_verified_final_custody_and_preserves_validation(monkeypatch, sealed):
    from process import network_candidate_custody
    from process import registry_publication_execution as execution

    validation_by_name = {"valid": True, "writer_closure": {"owner_role": "original_owner"}}
    candidate_by_name = {
        "state": "ready",
        "expected_head": 1,
        "approved_custom_revision": 2,
        "validation_json": validation_by_name,
        "source_recipes_json": [],
        "source_generations": {},
    }
    copy_target = SimpleNamespace(schema_name="candidate")
    command = SimpleNamespace(expected_serving_generation=1, expected_approved_revision=2)
    monkeypatch.setattr(execution, "_locked_candidate", AsyncMock(return_value=candidate_by_name))
    custody = {"seal_sha256": "a" * 64, "receipt": {"closure": {"owner_role": "final_owner"}}} if sealed else None

    async def verify_native(connection, supplied, *, control_schema, required=True):
        assert required is False and control_schema == "control" and supplied is copy_target
        return custody

    native = AsyncMock(side_effect=verify_native)
    monkeypatch.setattr(network_candidate_custody, "verify_network_candidate_custody", native)
    legacy = AsyncMock()
    monkeypatch.setattr(execution, "verify_network_candidate_writer_closure", legacy)
    monkeypatch.setattr(
        execution, "_publication_readiness", lambda candidate_by_name: ({}, None, {"index_definition_sha256": "b" * 64})
    )
    monkeypatch.setattr(execution, "_catalog_readiness", AsyncMock())
    monkeypatch.setattr(execution, "_index_digest", AsyncMock(return_value="b" * 64))
    before = json.dumps(validation_by_name, sort_keys=True)
    actual, first = await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "control")
    assert actual["validation_json"] is validation_by_name and json.dumps(validation_by_name, sort_keys=True) == before
    assert legacy.await_count == int(not sealed)
    if sealed:
        custody["receipt"]["closure"]["owner_role"] = "another_final_owner"
        _, second = await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "control")
        assert first != second and legacy.await_count == 0
        refusal = network_candidate_custody.NetworkCandidateCustodyError("changed_seal")
        native.side_effect = refusal
        with pytest.raises(network_candidate_custody.NetworkCandidateCustodyError, match="changed_seal") as error:
            await execution._verified_source_rollback_candidate(object(), copy_target, command, {}, "control")
        assert error.value is refusal
        legacy.assert_not_awaited()


def test_publication_custody_callback_is_runtime_only():
    configured = configuration()
    configured["consumer"]["seal_candidate"] = {}
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))
    cfg = worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer
    callback = AsyncMock()
    assert replace(cfg, seal_candidate=callback).seal_candidate is callback
    assert "AsyncMock" not in repr(replace(cfg, seal_candidate=callback))
    with pytest.raises(ValueError):
        replace(cfg, seal_candidate={})


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_seal", [False, True])
async def test_source_rollback_preview_finishes_in_repeatable_read_and_refuses_changed_seal(monkeypatch, changed_seal):
    from process import network_candidate_custody
    from process import registry_publication_execution as execution

    connection = _CandidateSealConnection()
    request_by_name = {"request_id": UUID(_ID), "request_sha256": "a" * 64}
    command = SimpleNamespace(expected_approved_revision=2, expected_serving_generation=1)
    copy_target = object()
    source_config, current = SimpleNamespace(generation_id=3), SimpleNamespace(generation_id=4)
    result_by_name = {"candidate_sha256": "c" * 64}
    monkeypatch.setattr(execution, "_request", AsyncMock(return_value=request_by_name))
    monkeypatch.setattr(execution, "_fresh_retained_operation_authority", AsyncMock(return_value=command))
    monkeypatch.setattr(execution, "_lock_publication_controls", AsyncMock(return_value=(2, 1)))

    async def retained(connection, *, generation_id, control_schema):
        return {3: source_config, 4: current}[generation_id]

    monkeypatch.setattr(execution, "_source_rollback_manifest", retained)
    refusal = network_candidate_custody.NetworkCandidateCustodyError("changed_seal")

    async def verify(connection, supplied, actual_command, roles, schema):
        assert supplied is copy_target and actual_command is command and schema == "control"
        assert connection.active and connection.active_options.get("isolation") == "repeatable_read"
        if changed_seal:
            raise refusal
        return {}, "c" * 64

    verifier = AsyncMock(side_effect=verify)
    monkeypatch.setattr(execution, "_verified_source_rollback_candidate", verifier)
    monkeypatch.setattr(execution, "_source_rollback_result", lambda *args: None)
    finish = AsyncMock()
    monkeypatch.setattr(execution, "finish_registry_publication_request", finish)
    operation = execution._finish_source_rollback_preview(
        connection,
        request_by_name,
        UUID(int=2),
        copy_target,
        (source_config, current, result_by_name),
        authorize=object(),
        writer_roles={},
        control_schema="control",
    )
    if changed_seal:
        with pytest.raises(network_candidate_custody.NetworkCandidateCustodyError) as error:
            await operation
        assert error.value is refusal
        finish.assert_not_awaited()
    else:
        assert await operation is result_by_name
        finish.assert_awaited_once()
    verifier.assert_awaited_once()
    assert connection.transaction_options == [{"isolation": "repeatable_read"}]
    assert not connection.active


@pytest.mark.parametrize(
    "changes",
    [
        {"membership_refresh_only": 1},
        {"membership_refresh_only": True},
        {"membership_refresh_runtime": {}, "only_request_id": UUID(int=1)},
        {"only_request_id": UUID(int=0)},
        {"only_request_id": str(UUID(int=1))},
        {"seal_candidate": {}},
        {"prepare_address_sources": {}},
        {"composition_runtime": {}},
        {"reader_roles": []},
    ],
)
def test_consumer_rejects_uninstalled_capabilities_and_ambiguous_request_identity(changes):
    configured = worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer
    with pytest.raises(ValueError, match="consumer_config_invalid"):
        replace(configured, **changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["shared_pool", "authority", "source", "configuration"])
async def test_consumer_refuses_invalid_host_context_before_acquiring_a_backend(fault):
    configured = worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer
    lifecycle = SimpleNamespace(acquire=AsyncMock())
    execution = lifecycle if fault == "shared_pool" else SimpleNamespace(acquire=AsyncMock())
    with pytest.raises(ValueError, match="consumer_config_invalid"):
        await consumer.consume_registry_publication_once(
            lifecycle_pool=lifecycle,
            execution_pool=execution,
            authorize=None if fault == "authority" else AsyncMock(),
            config={} if fault == "configuration" else configured,
            initial_address_sources={} if fault == "source" else None,
        )
    lifecycle.acquire.assert_not_called()
    execution.acquire.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["refresh_memberships", "unknown_operation"])
async def test_execution_dispatch_cannot_invent_a_refresh_runtime_or_operation(monkeypatch, operation):
    configured = worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer
    close_events = []

    @asynccontextmanager
    async def acquire():
        try:
            yield object()
        finally:
            close_events.append(True)

    monkeypatch.setattr(consumer, "_verify_execution", AsyncMock())
    execute = AsyncMock()
    monkeypatch.setattr(consumer, "complete_queued_membership_refresh", execute)
    with pytest.raises(ValueError, match="refresh_runtime_unavailable|publication_operation_invalid"):
        await consumer._execute_publication_request(
            SimpleNamespace(acquire=acquire), configured, {"command_json": {"operation": operation}}, AsyncMock(), None
        )
    assert close_events == [True]
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_address_preparation_cannot_continue_under_a_replaced_queue_request(monkeypatch):
    from process.registry_approval_store import RegistryApprovalConflict

    prepare = AsyncMock()
    configured = replace(
        worker.parse_registry_publication_worker_config(json.dumps(configuration())).consumer,
        prepare_address_sources=prepare,
    )
    monkeypatch.setattr(consumer, "_request", AsyncMock(return_value={"request_sha256": "b" * 64}))
    authorize = AsyncMock()
    with pytest.raises(RegistryApprovalConflict, match="queue_request_changed"):
        await consumer._prepared_address_sources(
            object(),
            configured,
            {"request_id": UUID(int=1), "lease_id": UUID(int=2), "request_sha256": "a" * 64},
            1,
            None,
            None,
            authorize,
        )
    prepare.assert_not_awaited()
    authorize.assert_not_awaited()


@pytest.fixture
async def isolated_ptg_runtime():
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

    engine = create_async_engine(
        "postgresql+asyncpg://publisher@localhost:1/worker_contract", pool_size=1, max_overflow=0
    )
    try:
        yield ptg_runtime.RegistryPublicationPTGRuntime(
            async_sessionmaker(engine),
            RegistryPTGProducerScopeStore("owner", "approver", "registry"),
            RegistryPTGGraphReadBudget(1048576),
        )
    finally:
        assert engine.pool.checkedout() == 0
        await engine.dispose()


@pytest.mark.parametrize("changes", [{"sql_limits": {}}, {"store": {}}, {"read_budget": {}}])
def test_ptg_runtime_requires_typed_host_limits_and_source_capabilities(isolated_ptg_runtime, changes):
    with pytest.raises(ValueError, match="ptg_runtime_invalid"):
        replace(isolated_ptg_runtime, **changes)


@pytest.mark.asyncio
async def test_ptg_runtime_refuses_nested_preparation_and_untyped_address_sources(isolated_ptg_runtime):
    with pytest.raises(ValueError, match="ptg_runtime_invalid"):
        await isolated_ptg_runtime.compose(
            SimpleNamespace(is_in_transaction=lambda: False), control_schema="registry", address_sources={}
        )
    with pytest.raises(ValueError, match="ptg_runtime_invalid"):
        await isolated_ptg_runtime.prepare(
            SimpleNamespace(is_in_transaction=lambda: True), None, None, writer_roles={}, control_schema="registry"
        )


@pytest.mark.asyncio
async def test_ptg_published_candidate_resume_never_reopens_raw_preparation(monkeypatch, isolated_ptg_runtime):
    from process import network_membership_pipeline as pipeline
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    candidate_by_field = {"state": "published", "source_generations": {COMPACT_FORMAT_KEY: COMPACT_FORMAT}}
    monkeypatch.setattr(ptg_runtime.RegistryPublicationPTGRuntime, "_same_execution_database", AsyncMock())
    inspect = AsyncMock(return_value=candidate_by_field)
    close = AsyncMock()
    monkeypatch.setattr(pipeline, "_inspect_candidate", inspect)
    monkeypatch.setattr(pipeline, "_close_raw_phase", close)
    assert (
        await isolated_ptg_runtime.prepare(
            SimpleNamespace(is_in_transaction=lambda: False), None, None, writer_roles={}, control_schema="registry"
        )
        is candidate_by_field
    )
    inspect.assert_awaited_once()
    close.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_fence_available", [True, False])
async def test_ptg_sealed_preparation_requires_fence_before_raw_driver(
    monkeypatch, isolated_ptg_runtime, is_fence_available
):
    """A failed fence must prevent raw preparation, even after the logical transaction begins."""
    from process import network_membership_pipeline as pipeline
    from process.registry_compact_composition import COMPACT_FORMAT, COMPACT_FORMAT_KEY

    events, driver, session = [], object(), SimpleNamespace()
    candidate_by_field = {"state": "sealed", "source_generations": {COMPACT_FORMAT_KEY: COMPACT_FORMAT}}
    prepared_by_field = {**candidate_by_field, "state": "ready"}

    async def require_fence(actual_session, schema):
        assert actual_session is session and schema == "registry"
        events.append("fence")
        if not is_fence_available:
            raise ValueError("registry_ptg_scope_company_fence_unavailable")

    async def raw_connection():
        assert events == ["begin", "fence"]
        events.append("raw")
        return SimpleNamespace(driver_connection=driver)

    session.connection = AsyncMock(return_value=SimpleNamespace(get_raw_connection=raw_connection))

    @asynccontextmanager
    async def transaction(sessions, *, control_schema):
        assert sessions is isolated_ptg_runtime.sessions and control_schema == "registry"
        events.append("begin")
        yield session

    inspect = AsyncMock(side_effect=[candidate_by_field, prepared_by_field])
    prepare = AsyncMock()
    monkeypatch.setattr(ptg_runtime.RegistryPublicationPTGRuntime, "_same_execution_database", AsyncMock())
    monkeypatch.setattr(ptg_runtime, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(ptg_runtime, "require_registry_company_approval_fence", require_fence, raising=False)
    monkeypatch.setattr(pipeline, "_inspect_candidate", inspect)
    monkeypatch.setattr(pipeline, "_close_raw_phase", AsyncMock())
    monkeypatch.setattr(pipeline, "_prepare_phase_snapshot", prepare)
    operation = isolated_ptg_runtime.prepare(
        SimpleNamespace(is_in_transaction=lambda: False), None, None, writer_roles={}, control_schema="registry"
    )
    if is_fence_available:
        assert await operation is prepared_by_field
        assert prepare.await_args.args[0] is driver
        assert prepare.await_args.kwargs["office_authority"][0] is session
        assert events == ["begin", "fence", "raw"]
    else:
        with pytest.raises(ValueError, match="company_fence_unavailable"):
            await operation
        session.connection.assert_not_awaited()
        prepare.assert_not_awaited()
