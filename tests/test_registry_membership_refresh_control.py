"""Exact existing operational admission; transport mocks do not establish native authority."""

import asyncio
import json
import os
import sys
from contextlib import AsyncExitStack
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

import process
from api import control_imports, control_workers
from process import registry_publication_worker as worker
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import SERVICE_ACTOR, RegistryMembershipRefreshUnavailable
from process.registry_membership_refresh_execution import RegistryMembershipRefreshRuntime
from process.registry_membership_refresh_queue import RegistryMembershipRefreshAddressProfile
from process.registry_publication_queue import _request_metadata
from tests.test_registry_membership_refresh_contract import command
from tests.test_registry_publication_worker import configuration, resources

IMPORTER = "registry-publication-worker"
REQUEST_ID = "d69a5cb2-4333-43eb-bba8-e03b6108599b"


def params():
    return {"request_id": REQUEST_ID, "operation": "refresh_memberships"}


def stored_request():
    selected = command()
    key = selected.pop("idempotency_key")
    return {
        "request_id": UUID(REQUEST_ID),
        "command_json": selected,
        "actor_json": SERVICE_ACTOR,
        "session_token_sha256": None,
        "idempotency_key": key,
        **_request_metadata(selected, SERVICE_ACTOR, None, key),
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": REQUEST_ID.upper()},
        {"request_id": "00000000-0000-0000-0000-000000000000"},
        {"request_id": "invalid"},
        {"request_id": UUID(REQUEST_ID)},
        {"operation": "publish"},
        {"actor": SERVICE_ACTOR},
        {"service_token": "not-permitted"},
        {"consumer": {}},
        {"run_id": "extra"},
    ],
)
def test_params_accept_only_canonical_request_and_exact_service_operation(changes):
    with pytest.raises(ValueError, match="worker_params_invalid"):
        worker.validate_membership_refresh_worker_params({**params(), **changes})
    assert worker.validate_membership_refresh_worker_params(params()) == UUID(REQUEST_ID)


def test_registry_worker_schema_and_existing_single_job_lane_agree():
    item = next(entry for entry in control_imports.importer_registry() if entry["name"] == IMPORTER)
    assert item["queue"] == "arq:PTGCandidateAudit" and item["function"] == "control_single_job_start"
    assert item["schedulable"] and item["cancelable"]
    assert {entry["name"] for entry in item["params_schema"]} == set(params())
    assert all(entry["required"] for entry in item["params_schema"])
    spec = control_workers._BY_IMPORTER_ROLE[(IMPORTER, "start")]
    assert spec.worker_class == "process.PTGCandidateAudit"
    assert control_workers._worker_command("python", spec)[-2:] == ["worker-once", "process.PTGCandidateAudit"]
    assert process.process_group.commands[IMPORTER] is worker.registry_publication_worker_command
    assert IMPORTER in control_imports.ALL_STATUS_IDEMPOTENCY_IMPORTERS


@pytest.mark.asyncio
async def test_real_adapter_enqueues_only_two_params_and_genuine_control_run(monkeypatch):
    redis = SimpleNamespace(enqueue_job=AsyncMock(return_value=SimpleNamespace(job_id="registry_publication_run-a")))
    monkeypatch.setattr(control_imports, "create_pool", AsyncMock(return_value=redis))
    receipt = await control_imports._enqueue_import_start({"run_id": "run-a", "importer": IMPORTER, "params": params()})
    assert receipt["status"] == "queued"
    args, kwargs = redis.enqueue_job.await_args
    assert args[0] == "control_single_job_start"
    assert args[1]["target_module"] == "process.registry_publication_worker"
    assert args[1]["target_function"] == "control_start" and args[1]["task"] == params()
    assert args[1]["run_id"] == "run-a" and args[1]["importer"] == IMPORTER
    assert kwargs == {"_queue_name": "arq:PTGCandidateAudit", "_job_id": "registry_publication_run-a"}


@pytest.mark.asyncio
async def test_bad_params_fail_before_database_or_queue_and_replay_is_exact(monkeypatch):
    database = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected DB")))
    queue = AsyncMock(side_effect=AssertionError("unexpected queue"))
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "create_pool", queue)
    with pytest.raises(ValueError):
        await control_imports.create_import_run({"importer": IMPORTER, "params": {**params(), "credentials": "no"}})
    rejected = await control_imports._enqueue_import_start({"importer": IMPORTER, "run_id": "run-a", "params": {}})
    assert rejected["status"] == "failed"
    database.execute.assert_not_called()
    queue.assert_not_awaited()
    control_imports._validate_registry_cms_source_params(IMPORTER, params(), {"importer": IMPORTER, "params": params()})
    with pytest.raises(ValueError, match="identity_mismatch"):
        control_imports._validate_registry_cms_source_params(
            IMPORTER,
            params(),
            {"importer": IMPORTER, "params": {**params(), "request_id": "e69a5cb2-4333-43eb-bba8-e03b6108599b"}},
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["absent", "actor", "operation", "digest", "session"])
async def test_native_queue_row_must_prove_service_operation_before_claim(damage):
    native = stored_request()
    if damage == "absent":
        native = None
    elif damage == "actor":
        native["actor_json"] = {"kind": "platform_admin"}
    elif damage == "operation":
        native["command_json"] = {**json.loads(native["command_json"]), "operation": "publish"}
    elif damage == "digest":
        native["request_sha256"] = "f" * 64
    else:
        native["session_token_sha256"] = "a" * 64
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=native))
    consumer = SimpleNamespace(
        control_schema="registry", only_request_id=UUID(REQUEST_ID), membership_refresh_only=True
    )
    with pytest.raises((ValueError, RegistryMembershipRefreshUnavailable)):
        await worker._require_membership_refresh_request(connection, consumer, UUID(REQUEST_ID))
    statement, identity = connection.fetchrow.await_args.args
    assert statement == 'SELECT * FROM "registry".registry_publication_request WHERE request_id=$1'
    assert identity == UUID(REQUEST_ID)


@pytest.mark.asyncio
async def test_current_native_row_is_accepted_without_using_stored_receipt():
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=stored_request()))
    consumer = SimpleNamespace(
        control_schema="registry", only_request_id=UUID(REQUEST_ID), membership_refresh_only=True
    )
    await worker._require_membership_refresh_request(connection, consumer, UUID(REQUEST_ID))


@pytest.mark.asyncio
async def test_control_target_loads_server_configuration_and_exact_uuid(monkeypatch):
    from process import control_cancel

    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())
    run = AsyncMock(return_value={"state": "no_work"})
    monkeypatch.setattr(worker, "run_registry_publication_worker", run)
    assert await worker.control_start({}, {"run_id": "run-a", **params()}) == {"state": "no_work"}
    assert run.await_args.kwargs == {"membership_refresh_request_id": UUID(REQUEST_ID)}
    assert type(run.await_args.args[0]) is worker.RegistryPublicationWorkerConfig
    with pytest.raises(ValueError):
        await worker.control_start({}, {"run_id": "run-a", **params(), "service_token": "no"})


@pytest.mark.asyncio
async def test_missing_native_runtime_fails_before_pools_or_consumer(monkeypatch):
    module = ModuleType("process.registry_membership_refresh_runtime")
    module.membership_refresh_worker_runtime = AsyncMock(side_effect=RegistryMembershipRefreshUnavailable("disabled"))
    monkeypatch.setitem(sys.modules, module.__name__, module)
    pools = AsyncMock(side_effect=AssertionError("unexpected pool"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pools)
    config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await worker.run_registry_publication_worker(config, membership_refresh_request_id=UUID(REQUEST_ID))
    pools.assert_not_awaited()


@pytest.mark.asyncio
async def test_fixed_bootstrap_installs_strong_runtime_and_exact_claim_flags(monkeypatch):
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example", "dedicated", enabled=True
    ) as authority:
        runtime = RegistryMembershipRefreshRuntime(
            authority,
            RegistryMembershipRefreshAddressProfile("addresses", ("address_reader",)),
            AsyncMock(),
            AsyncMock(),
        )
        module = ModuleType("process.registry_membership_refresh_runtime")
        module.membership_refresh_worker_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setitem(sys.modules, module.__name__, module)
        config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
        async with AsyncExitStack() as cleanup:
            configured = await worker._membership_refresh_config(config, cleanup, UUID(REQUEST_ID))
            module.membership_refresh_worker_runtime.assert_awaited_once_with(config, cleanup)
        assert configured.consumer.only_request_id == UUID(REQUEST_ID)
        assert configured.consumer.membership_refresh_only is True
        assert configured.consumer.membership_refresh_runtime is runtime


@pytest.mark.asyncio
async def test_control_cancellation_drains_owned_work_before_terminal_projection(monkeypatch):
    from process import control_cancel, registry_cms_source_worker

    work_started, work_drained = asyncio.Event(), asyncio.Event()
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configuration()))
    monkeypatch.setattr(control_cancel, "raise_if_cancelled", AsyncMock())

    async def run(_config, **_selectors):
        work_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            work_drained.set()

    async def canceled(_ctx, _task):
        await work_started.wait()
        raise control_cancel.ImportCancelledError("canceled")

    monkeypatch.setattr(worker, "run_registry_publication_worker", run)
    monkeypatch.setattr(registry_cms_source_worker, "_watch_control_cancel", canceled)
    with pytest.raises(control_cancel.ImportCancelledError):
        await asyncio.wait_for(worker.control_start({}, {"run_id": "run-a", **params()}), 5)
    assert work_drained.is_set()


def test_only_native_admitted_job_selects_importer_scoped_secret_and_target():
    spec = control_workers._BY_IMPORTER_ROLE[(IMPORTER, "start")]
    selector_by_field = {"workerClasses": [spec.worker_class], "importers": [IMPORTER]}
    untrusted_by_field = {"run_id": "run-a", "importer": IMPORTER, "job_id": "registry_publication_run-a"}
    with pytest.raises(ValueError, match="exact admitted job"):
        control_workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, untrusted_by_field)
    admitted = control_workers._AdmittedWorkerRequest(**untrusted_by_field)
    control_workers._bind_admitted_job(
        admitted,
        {
            **untrusted_by_field,
            "metrics": {
                "queue": spec.queue,
                "function": "control_single_job_start",
                "job_id": untrusted_by_field["job_id"],
            },
        },
    )
    assert control_workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, admitted)
    assert control_workers._single_job_worker_target(spec, admitted) == untrusted_by_field["job_id"]
    admitted.admitted_job = admitted.admitted_job[:-1] + ("foreign-job",)
    with pytest.raises(ValueError, match="exact admitted job"):
        control_workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, admitted)


@pytest.mark.asyncio
async def test_preclaim_refuses_any_changed_exact_request_selector():
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=AssertionError("unexpected native read")))
    consumer = SimpleNamespace(control_schema="registry", only_request_id=None, membership_refresh_only=False)
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await worker._require_membership_refresh_request(connection, consumer, UUID(REQUEST_ID))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_native", [True, False])
async def test_actual_runner_checks_exact_native_request_before_consuming(monkeypatch, valid_native):
    resources(monkeypatch)
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example", "dedicated", enabled=True
    ) as authority:
        runtime = RegistryMembershipRefreshRuntime(
            authority,
            RegistryMembershipRefreshAddressProfile("addresses", ("address_reader",)),
            AsyncMock(),
            AsyncMock(),
        )
        module = ModuleType("process.registry_membership_refresh_runtime")
        module.membership_refresh_worker_runtime = AsyncMock(return_value=runtime)
        monkeypatch.setitem(sys.modules, module.__name__, module)
        observations = []

        async def preclaim(_connection, consumer, request_id):
            assert consumer.only_request_id == request_id == UUID(REQUEST_ID)
            assert consumer.membership_refresh_only is True and consumer.membership_refresh_runtime is runtime
            observations.append("native_request")
            if not valid_native:
                raise RegistryMembershipRefreshUnavailable("native request changed")

        async def consume(**arguments):
            observations.append("consume")
            assert arguments["authorize"] is authority
            assert arguments["config"].only_request_id == UUID(REQUEST_ID)
            return None

        monkeypatch.setattr(worker, "_require_membership_refresh_request", preclaim)
        monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
        config = worker.parse_registry_publication_worker_config(json.dumps(configuration()))
        if valid_native:
            assert await worker.run_registry_publication_worker(
                config, membership_refresh_request_id=UUID(REQUEST_ID)
            ) == {"state": "no_work"}
            assert observations == ["native_request", "consume"]
        else:
            with pytest.raises(RegistryMembershipRefreshUnavailable):
                await worker.run_registry_publication_worker(config, membership_refresh_request_id=UUID(REQUEST_ID))
            assert observations == ["native_request"]
