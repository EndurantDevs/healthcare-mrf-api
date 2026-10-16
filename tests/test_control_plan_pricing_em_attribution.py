# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed importer requests, queue dispatch and exact shared-worker admission."""

from contextlib import asynccontextmanager
from copy import deepcopy
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_imports as imports
from api import control_workers as workers
from api.control_plan_pricing_em_attribution import ADAPTER, IMPORTER, validate_em_attribution_params
from process.control_lifecycle import _invoke_control_target


def _params():
    return {
        "plan_release_id": "hprelease_" + "1" * 26,
        "serving_revision_id": "hpserve_" + "2" * 26,
        "owner_role": "artifact_owner",
        "runtime_roles": ["a_reader", "z_reader"],
    }


def test_capability_matches_exact_worker_and_four_typed_parameters():
    entries = [entry for entry in imports.importer_registry() if entry["name"] == IMPORTER]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["family"] == imports._importer_family(IMPORTER) == "mrf"
    assert entry["queue"] == ADAPTER["queue"] == "arq:PTGCandidateAudit"
    assert entry["kind"] == "control" and entry["lifecycle"] == "single"
    assert entry["retryable"] and not entry["schedulable"] and not entry["cancelable"]
    assert entry["depends_on"] == [] and entry["dependency_authority"] == "scheduler"
    assert {parameter["name"]: parameter["type"] for parameter in entry["params_schema"]} == {
        "plan_release_id": "string",
        "serving_revision_id": "string",
        "owner_role": "string",
        "runtime_roles": "array",
    }
    assert all(parameter["required"] and not parameter["multiple"] for parameter in entry["params_schema"])
    assert IMPORTER in imports.ALL_STATUS_IDEMPOTENCY_IMPORTERS
    assert IMPORTER in imports.STALE_WORKER_RECONCILIATION_IMPORTERS
    assert IMPORTER in imports.TERMINAL_QUEUE_RESIDUE_IMPORTERS


@pytest.mark.parametrize("role_count", [1, 32])
def test_canonical_role_list_preserves_exact_request(role_count):
    params_by_name = _params()
    params_by_name["runtime_roles"] = [f"reader_{index:02}" for index in range(role_count)]
    original = deepcopy(params_by_name)
    validate_em_attribution_params(IMPORTER, params_by_name)
    assert params_by_name == original
    validate_em_attribution_params("other-importer", None)


def _invalid_params():
    valid = _params()
    return [
        None,
        [],
        {},
        {name: content for name, content in valid.items() if name != "owner_role"},
        valid | {"projection_id": "a" * 64},
        valid | {"test_mode": True},
        *[
            valid | {name: content}
            for name in ("plan_release_id", "serving_revision_id", "owner_role")
            for content in (None, False, "", " padded ")
        ],
        *[
            valid | {"runtime_roles": content}
            for content in (
                None,
                "reader",
                ("reader",),
                [],
                [False],
                [""],
                ["z_reader", "a_reader"],
                ["reader", "reader"],
                ["artifact_owner"],
                ["bad-role"],
                [f"reader_{index:02}" for index in range(33)],
            )
        ],
        valid | {"owner_role": "bad-role"},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("params_by_name", _invalid_params())
async def test_invalid_request_refuses_before_database_or_enqueue(monkeypatch, params_by_name):
    database = SimpleNamespace(
        execute=AsyncMock(side_effect=AssertionError("no database")),
        acquire=Mock(side_effect=AssertionError("no acquire")),
    )
    enqueue = AsyncMock(side_effect=AssertionError("no enqueue"))
    lookup = AsyncMock(side_effect=AssertionError("no lookup"))
    monkeypatch.setattr(imports, "db", database)
    monkeypatch.setattr(imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(imports, "find_earliest_active_run_by_importer", lookup)
    monkeypatch.setattr(imports, "_idempotent_import_run", lookup)
    with pytest.raises(ValueError):
        await imports.create_import_run(
            {"importer": IMPORTER, "params": params_by_name, "idempotency_key": "synthetic"}
        )
    database.execute.assert_not_awaited()
    database.acquire.assert_not_called()
    enqueue.assert_not_awaited()
    lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_run_queues_exact_kwargs_and_preserves_role_array(monkeypatch):
    queued_jobs = []
    statements = []

    async def enqueue(function, task_payload_by_field, **options_by_name):
        queued_jobs.append((function, task_payload_by_field, options_by_name))
        return SimpleNamespace(job_id=options_by_name["_job_id"])

    async def execute(statement):
        statements.append(statement)
        return SimpleNamespace(rowcount=1)

    monkeypatch.setattr(imports, "db", SimpleNamespace(execute=execute))
    monkeypatch.setattr(imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    monkeypatch.setattr(imports, "create_pool", AsyncMock(return_value=SimpleNamespace(enqueue_job=enqueue)))
    monkeypatch.setattr(imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(imports, "_write_run_live_progress", Mock())
    params_by_name = _params()
    result_by_field, created = await imports.create_import_run(
        {"importer": IMPORTER, "run_id": "run_synthetic", "params": params_by_name}
    )
    assert created and result_by_field["family"] == "mrf" and result_by_field["params"] == params_by_name
    assert len(statements) == 2 and len(queued_jobs) == 1
    function, task_payload_by_field, options_by_name = queued_jobs[0]
    assert function == ADAPTER["function"] == "control_single_job_start"
    assert options_by_name == {"_queue_name": ADAPTER["queue"], "_job_id": "plan_pricing_em_attribution_run_synthetic"}
    assert task_payload_by_field["target_module"] == "api.plan_pricing_em_attribution"
    assert (
        task_payload_by_field["target_function"] == "build_plan_pricing_em_attribution"
        and task_payload_by_field["call_style"] == "kwargs"
    )
    assert task_payload_by_field["task"] == {"test_mode": False, **params_by_name}
    assert result_by_field["metrics"]["job_id"] == options_by_name["_job_id"]


@pytest.mark.asyncio
async def test_terminal_idempotency_lookup_keeps_exact_importer(monkeypatch):
    retained_by_field = {"run_id": "run_retained", "importer": IMPORTER, "status": "succeeded"}
    find = AsyncMock(return_value=retained_by_field)
    active = AsyncMock(side_effect=AssertionError("terminal keys must remain retained_by_field"))
    monkeypatch.setattr(imports, "find_importer_run_by_idempotency_key", find)
    monkeypatch.setattr(imports, "find_active_run_by_idempotency_key", active)
    assert await imports._idempotent_import_run(IMPORTER, "key", params=_params()) == retained_by_field
    find.assert_awaited_once_with(IMPORTER, "key")
    active.assert_not_awaited()


def test_worker_spec_prefix_and_exact_admission_close_new_importer():
    request_by_field = {"importer": IMPORTER, "run_id": "run_synthetic"}
    spec = workers._BY_IMPORTER_ROLE[(IMPORTER, "start")]
    assert spec == workers._BY_WORKER_CLASS["process.PTGCandidateAudit"]
    assert workers._resolve_specs(request_by_field) == [spec]
    assert workers._exact_worker_spec(request_by_field) == spec
    assert workers._single_job_worker_target(spec, request_by_field) == "plan_pricing_em_attribution_run_synthetic"
    request_by_field["job_id"] = "plan_pricing_em_attribution_run_synthetic"
    recorded_by_field = {
        "importer": IMPORTER,
        "run_id": "run_synthetic",
        "metrics": {
            "queue": ADAPTER["queue"],
            "function": ADAPTER["function"],
            "job_id": request_by_field["job_id"],
        },
    }
    admitted = workers._AdmittedWorkerRequest(request_by_field)
    workers._bind_admitted_job(admitted, recorded_by_field)
    selector_by_field = {"workerClasses": [spec.worker_class], "importers": [IMPORTER]}
    assert workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, admitted)
    for field in ("run_id", "importer", "job_id"):
        changed = deepcopy(admitted)
        changed[field] = "different"
        with pytest.raises(ValueError, match="exact admitted job"):
            workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, changed)
    with pytest.raises(ValueError, match="exact admitted job"):
        workers._is_admitted_importer_selected(selector_by_field, spec.worker_class, dict(request_by_field))
    for field, content in (("queue", "arq:NPI"), ("worker_class", "process.NPI")):
        with pytest.raises(ValueError, match="conflicts"):
            workers._exact_worker_spec(request_by_field | {field: content})


@pytest.mark.asyncio
async def test_generic_dispatch_reaches_real_build_wrapper_signature(monkeypatch):
    module = import_module(ADAPTER["target_module"])
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield session

    prepare = AsyncMock(return_value=SimpleNamespace(as_dict=lambda: {"contract": module.ATTRIBUTION_CONTRACT}))
    monkeypatch.setattr(module.db, "transaction", transaction)
    monkeypatch.setattr(module, "prepare_em_attribution_in_session", prepare)
    assert await _invoke_control_target(
        module,
        target_function=ADAPTER["target_function"],
        call_style="kwargs",
        control_context={},
        target_task_by_field={"test_mode": False, **_params()},
    ) == {"contract": module.ATTRIBUTION_CONTRACT}
    assert session.execute.await_args.args[0].text == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
    prepare.assert_awaited_once_with(session, **(_params() | {"runtime_roles": tuple(_params()["runtime_roles"])}))
