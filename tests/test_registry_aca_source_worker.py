# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed ACA configuration and real command/queue adapter contracts."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process
from api import control_imports, control_workers
from process import registry_aca_source_worker as worker
from process.provider_directory_generic_source_policy import GenericSourcePolicy

IMPORTER = "registry-aca-source-worker"


def event():
    return dict(capture_id=str(UUID(int=9)), recipe_sha256="a" * 64, notification_sha256="b" * 64)


def configuration():
    return dict(
        execution_dsn="postgresql://executor:{password}@db.example.test/example",
        notification_dsn="postgresql://reader:{password}@db.example.test/example",
        consumer=dict(
            control_schema="registry_control",
            execution_role="executor",
            owner_role="loading_owner",
            source_id="source-example",
            producer_id="producer-example",
            issuer_id=7,
            year=2026,
        ),
        reader_role="reader",
        notification_store=dict(
            control_schema="registry_control", publisher_role="retained_publisher", reader_roles=["reader"]
        ),
        ptg=dict(
            owner_role="office_owner",
            approval_role="office_approver",
            read_limits=dict(maximum_bytes=1048576, maximum_pages=10, maximum_coordinates=100),
        ),
        event=event(),
    )


def test_configuration_has_no_serialized_source_authority_or_callback():
    parsed = worker.parse_registry_aca_source_worker_config(json.dumps(configuration()))
    assert parsed.source_authority_roles == ()
    assert parsed.reader_role == "reader" and parsed.consumer.execution_role == "executor"
    assert "{password}" not in repr(parsed)


@pytest.mark.parametrize(
    "damage", ["extra", "database", "reader", "publisher", "schema", "runtime", "roles", "deadline"]
)
def test_bad_configuration_refuses_before_resources(damage):
    value = configuration()
    if damage == "extra":
        value["command"] = ["foreign"]
    if damage == "database":
        value["notification_dsn"] += "2"
    if damage == "reader":
        value["reader_role"] = "foreign"
    if damage == "publisher":
        value["notification_store"]["publisher_role"] = "executor"
    if damage == "schema":
        value["consumer"]["control_schema"] = "bad;schema"
    if damage == "runtime":
        value["consumer"]["seal_candidate"] = "foreign"
    if damage == "roles":
        value["consumer"]["source_authority_roles"] = ["source_owner"]
    if damage == "deadline":
        value["consumer"]["operation_seconds"] = True
    with pytest.raises(ValueError):
        worker.parse_registry_aca_source_worker_config(json.dumps(value))


def test_registered_command_queue_metadata_and_worker_agree():
    item = next(item for item in control_imports.importer_registry() if item["name"] == IMPORTER)
    assert item["schedulable"] and item["cancelable"] and item["retryable"]
    assert item["queue"] == "arq:RegistryACASource" and item["function"] == "control_single_job_start"
    assert {p["name"] for p in item["params_schema"]} == set(event())
    assert all(p["required"] for p in item["params_schema"])
    assert control_workers._BY_QUEUE[item["queue"]].worker_class == "process.RegistryACASource"
    assert (
        process.RegistryACASource.max_jobs
        == process.RegistryACASource.queue_read_limit
        == process.RegistryACASource.max_tries
        == 1
    )
    assert process.process_group.commands[IMPORTER] is worker.registry_aca_source_worker_command


@pytest.mark.asyncio
async def test_exact_adapter_enqueues_only_native_event_and_genuine_run_id(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id="synthetic")))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=redis))
    assert (
        await control_imports._enqueue_import_start(dict(run_id="synthetic-run", importer=IMPORTER, params=event()))
    )["status"] == "queued"
    args, kwargs = redis.enqueue_job.await_args
    assert args[1]["target_module"] == "process.registry_aca_source_worker"
    assert args[1]["task"] == event() and args[1]["run_id"] == "synthetic-run"
    assert kwargs == dict(_queue_name="arq:RegistryACASource", _job_id="registry_aca_source_synthetic-run")


@pytest.mark.asyncio
async def test_malformed_event_never_reaches_queue(monkeypatch):
    queue = AsyncMock(side_effect=AssertionError("unexpected queue"))
    monkeypatch.setattr(control_imports, "create_pool", queue)
    result = await control_imports._enqueue_import_start(
        dict(run_id="synthetic", importer=IMPORTER, params={**event(), "roles": []})
    )
    assert result["status"] == "failed"
    queue.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_genuine_source_factory_refuses_before_any_pool(monkeypatch):
    pool = AsyncMock(side_effect=AssertionError("unexpected pool"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pool)
    with pytest.raises(ValueError):
        await worker.run_registry_aca_source_worker(
            worker.parse_registry_aca_source_worker_config(json.dumps(configuration()))
        )
    pool.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_census_is_derived_from_genuine_policy_and_cannot_omit_owner(monkeypatch):
    config = worker.parse_registry_aca_source_worker_config(json.dumps(configuration()))
    policy = GenericSourcePolicy(
        "mrf", "source_owner", "source_publisher", ("control_reader", "source_reader"), ("control_reader",)
    )
    publisher = SimpleNamespace(policy=policy)
    identity_map = dict(
        database_name="example", database_oid=1, server_address="127.0.0.1", server_port=1, server_started="start"
    )
    native = SimpleNamespace(fetchrow=AsyncMock(return_value=identity_map))

    @asynccontextmanager
    async def source_transaction(*args, **kwargs):
        yield object()

    monkeypatch.setattr(worker.source_policy, "source_transaction", source_transaction)
    monkeypatch.setattr(worker.source_policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(worker.source_policy, "source_producer_identity", lambda: ("a" * 40, "sha256:" + "b" * 64))
    from process import network_cms_registry_address_capture

    monkeypatch.setattr(network_cms_registry_address_capture, "native_driver", AsyncMock(return_value=native))
    actual = await worker._source_config(config, identity_map)
    assert actual.source_authority_roles == ("control_reader", "source_owner", "source_publisher", "source_reader")
    omitted = replace(config, consumer=replace(config.consumer, source_authority_roles=("source_reader",)))
    with pytest.raises(ValueError):
        await worker._source_config(omitted, identity_map)


@pytest.mark.asyncio
async def test_control_cancellation_drains_worker_before_return(monkeypatch):
    from process import control_cancel

    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())
    started, drained = asyncio.Event(), asyncio.Event()

    async def run(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    monkeypatch.setattr(worker, "run_registry_aca_source_worker", run)
    task = asyncio.create_task(worker.control_start({}, {**event(), "run_id": "synthetic"}))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert drained.is_set()


def test_managed_worker_requires_native_admission_and_isolated_launcher(monkeypatch):
    from api import registry_aca_source_worker as launch

    spec = control_workers._BY_WORKER_CLASS["process.RegistryACASource"]
    assert control_workers._ensure_spec(spec, {"run_id": "synthetic"})["status"] == "failed"
    monkeypatch.setenv(launch.IMAGE_ENV, "registry.example.test/source@sha256:" + "a" * 64)
    admitted_run_map = dict(
        run_id="synthetic",
        importer=IMPORTER,
        metrics=dict(
            queue="arq:RegistryACASource", function="control_single_job_start", job_id="registry_aca_source_synthetic"
        ),
    )
    request = control_workers._AdmittedWorkerRequest(run_id="synthetic", importer=IMPORTER)
    request.admitted_job = (
        "synthetic",
        IMPORTER,
        "arq:RegistryACASource",
        "control_single_job_start",
        "registry_aca_source_synthetic",
    )
    request.aca_source_launch = launch.admit_aca_source_worker_launch(admitted_run_map)
    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "process")
    assert "isolated launcher" in control_workers._ensure_spec(spec, request)["message"]
    container, _, _ = control_workers._worker_job_container(spec, request, "foreign")
    assert container["image"] == request.aca_source_launch.image
    assert container["command"] == ["/usr/local/bin/aca-registry-source-worker"]
    assert container["workingDir"] == "/app"
    assert control_workers._single_job_worker_target(spec, request) == request.aca_source_launch.job_id


def test_controlled_config_rejects_unqualified_composer_objects():
    parsed = worker.parse_registry_aca_source_worker_config(json.dumps(configuration()))
    with pytest.raises(ValueError):
        replace(parsed, ptg=SimpleNamespace(store=SimpleNamespace(control_schema="registry_control")))


def batch_configuration():
    value = configuration()
    for key in ("source_id", "producer_id", "issuer_id", "year"):
        value["consumer"].pop(key)
    value["consumer"]["initial_batch"] = True
    value["consumer"]["initial_address_sources"] = {
        "base_source": {
            "schema_name": "retained_addresses",
            "table_name": "entity_address_unified",
            "generation_id": "a" * 64,
        }
    }
    return value


def test_initial_batch_configuration_uses_existing_small_event_and_address_pins():
    value = batch_configuration()
    parsed = worker.parse_registry_aca_source_worker_config(json.dumps(value))
    assert parsed.consumer.initial_batch and not parsed.consumer.global_epoch
    assert parsed.consumer.initial_address_sources.initial_source_recipes == ()
    assert parsed.event == event() and parsed.source_authority_roles == ()
    assert len(json.dumps(value).encode()) < 16384


@pytest.mark.parametrize("damage", ["false", "integer", "global", "logical", "callback", "recipes"])
def test_initial_batch_configuration_refuses_other_modes_and_embedded_authority(damage):
    value = batch_configuration()
    settings = value["consumer"]
    settings.update(
        {
            "false": {"initial_batch": False},
            "integer": {"initial_batch": 1},
            "global": {"global_epoch": True},
            "logical": {"source_id": "selected-subset"},
            "callback": {"on_prepared": "foreign"},
            "recipes": {"recipes": []},
        }[damage]
    )
    with pytest.raises(ValueError):
        worker.parse_registry_aca_source_worker_config(json.dumps(value))
