# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit revision, request and launch identities cannot authorize a fallback."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api.control_workers import WorkerSpec, _AdmittedWorkerRequest
from api.plan_pricing_worker_policy import pricing_worker_policy_volumes
from process import plan_pricing_admission as admission
from process import plan_pricing_control_catalog as control_catalog
from process.plan_pricing_admission import (
    BULK_REVISION,
    HISTORICAL_REVISIONS,
    PricingAdmission,
    _dispatches,
    _require_lineage,
    is_managed_storage_installed,
)
from process.plan_pricing_control import pricing_control_digest, projection_admission_key, projection_retry_key
from process.plan_pricing_control_policy import PricingControlPolicy
from tests.test_plan_pricing_idempotency import _durable_import_request
from tests.test_plan_pricing_retained_inputs import _native_heap_observations, _retained_case


@pytest.mark.asyncio
@pytest.mark.parametrize("revision", (*sorted(HISTORICAL_REVISIONS), BULK_REVISION, "unknown", None))
@pytest.mark.parametrize("controls", ((None, None), (1, None), (1, 2)))
async def test_only_exact_complete_bulk_revision_activates(revision, controls, monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "control_catalog")
    present_rows = [{"oid": oid} for oid in (*controls, 3 if revision else None)]
    revisions = [{"version_num": revision}] if revision else []
    connection = AsyncMock()
    connection.fetch.side_effect = (present_rows, revisions)
    valid = (not any(controls) and revision in HISTORICAL_REVISIONS) or (all(controls) and revision == BULK_REVISION)
    if valid:
        assert await is_managed_storage_installed(connection) is (revision == BULK_REVISION)
    else:
        with pytest.raises(ValueError):
            await is_managed_storage_installed(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("controls", [(None, None), (1, None), (None, 2), (1, 2)])
@pytest.mark.parametrize(
    "revisions", [("scalar_successor",), ("unknown_revision",), (BULK_REVISION,), ("scalar_successor", "other")]
)
async def test_qualified_historical_layout_never_confers_managed_admission(tmp_path, monkeypatch, controls, revisions):
    from process import plan_pricing_control_policy as policy

    path = tmp_path / "historical.json"
    path.write_text(
        json.dumps(
            {"contract": policy.HISTORICAL_POLICY_CONTRACT, "schema": "control_catalog", "revision": "scalar_successor"}
        )
    )
    monkeypatch.setattr(policy, "HISTORICAL_POLICY_PATH", path)
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "control_catalog")
    connection = AsyncMock()
    connection.fetch.side_effect = (
        [{"oid": value} for value in (*controls, 3)],
        [{"version_num": value} for value in revisions],
    )
    if not any(controls) and revisions == ("scalar_successor",):
        assert await is_managed_storage_installed(connection) is False
    elif all(controls) and revisions == (BULK_REVISION,):
        assert await is_managed_storage_installed(connection) is True
    else:
        with pytest.raises(ValueError, match="schema_revision_invalid"):
            await is_managed_storage_installed(connection)


@pytest.fixture(params=(None, "parent-run"), ids=("reserved", "retry"))
def missing_control_storage(monkeypatch, request):
    from db.models import db
    from process import plan_pricing_control_transaction

    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "missing_control_storage")
    run_by_field = {
        "run_id": "managed-run",
        "importer": "plan-pricing-projection",
        "idempotency_key": "plan-pricing-projection" + ("-retry" if request.param else "") + ":" + "a" * 64,
        "retry_of_run_id": request.param,
        "params": {},
    }
    connection = AsyncMock()
    connection.fetch.return_value = [{"oid": None}] * 3
    connection.fetchrow.return_value = run_by_field

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(raw_connection=SimpleNamespace(driver_connection=connection))

    monkeypatch.setattr(db, "transaction", transaction)
    monkeypatch.setattr(db, "acquire", transaction)
    monkeypatch.setattr(db, "connect", AsyncMock())
    monkeypatch.setattr(db, "execute", AsyncMock(side_effect=AssertionError("unguarded database write")))
    monkeypatch.setattr(plan_pricing_control_transaction, "pricing_driver", AsyncMock(return_value=connection))
    return run_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("entry_point", ("preflight", "insert"))
async def test_missing_control_storage_refuses_before_creation(missing_control_storage, entry_point):
    from api import control_imports

    with pytest.raises(ValueError, match="schema_revision_missing"):
        if entry_point == "preflight":
            await control_imports._has_isolated_pricing_admission(missing_control_storage)
        else:
            await control_imports._admit_import_row(
                "plan-pricing-projection", missing_control_storage, is_ptg_source_file_admission=False
            )
    control_imports.db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_control_storage_refuses_before_worker_launch(missing_control_storage, monkeypatch):
    from api import control_workers

    for name in (
        "acquire_ptg_admission_lock",
        "acquire_control_run_worker_action_lock",
        "require_not_wave_owned_run",
        "require_no_capacity_owning_wave",
        "_admit_worker_ensure",
    ):
        monkeypatch.setattr(control_workers, name, AsyncMock(return_value=None))
    launch = Mock(side_effect=AssertionError("worker launched without managed policy"))
    monkeypatch.setattr(control_workers, "ensure_worker", launch)
    with pytest.raises(ValueError, match="schema_revision_missing"):
        await control_workers.guarded_ensure_worker(missing_control_storage)
    launch.assert_not_called()


@pytest.mark.asyncio
async def test_missing_control_storage_refuses_before_initial_target(missing_control_storage, monkeypatch):
    from process import control_lifecycle

    target = Mock(side_effect=AssertionError("unguarded target import"))
    monkeypatch.setattr(control_lifecycle, "import_module", target)
    monkeypatch.setattr(control_lifecycle, "bind_status_event_loop", Mock())
    monkeypatch.setattr(control_lifecycle, "set_live_progress_context", Mock(return_value=None))
    with pytest.raises(ValueError, match="schema_revision_missing"):
        await control_lifecycle.control_single_job_start({}, task=missing_control_storage)
    control_lifecycle.db.execute.assert_not_awaited()
    target.assert_not_called()


def _identity():
    request_by_field = {
        "binding_manifest_digest": "a" * 64,
        "projection_contract": "supported",
        "publication_identity": "b" * 64,
        "publication_attempt_identity": "c" * 64,
        "bindings": [{}],
    }
    key = projection_admission_key(request_by_field)
    record_by_field = {
        "request_digest": pricing_control_digest(request_by_field),
        "admission_key": key,
        "publication_identity": request_by_field["publication_identity"],
        "attempt_identity": request_by_field["publication_attempt_identity"],
        "status": "accepted",
        "engine_run_id": "parent",
        "dispatches": [{"key": key, "retry_of": None, "run_id": "parent"}],
    }
    return request_by_field, record_by_field


@pytest.mark.parametrize("damage", ("request", "parent", "key", "run", "state"))
def test_closed_dispatch_identity_includes_whole_request_and_lineage(damage):
    request_by_field, record_by_field = _identity()
    _dispatches(record_by_field, request_by_field)
    if damage == "request":
        request_by_field = {**request_by_field, "extra": True}
    elif damage == "parent":
        record_by_field["dispatches"][0]["retry_of"] = "foreign"
    elif damage == "key":
        record_by_field["dispatches"][0]["key"] = "foreign"
    elif damage == "run":
        record_by_field["engine_run_id"] = "foreign"
    else:
        record_by_field["status"] = "reserved"
    with pytest.raises(ValueError):
        _dispatches(record_by_field, request_by_field)


def test_managed_parent_rejects_unmanaged_child_key_or_changed_params():
    request_by_field, record_by_field = _identity()
    parent_by_field = {
        "importer": "plan-pricing-projection",
        "idempotency_key": record_by_field["admission_key"],
        "params": request_by_field,
        "node_id": "synthetic-node",
        "engine": "synthetic-engine",
        "retry_of_run_id": None,
        "status": "failed",
    }
    child_by_field = {
        **parent_by_field,
        "retry_of_run_id": "parent",
        "idempotency_key": projection_retry_key(record_by_field["admission_key"], "parent"),
    }
    _require_lineage(record_by_field["admission_key"], [child_by_field, parent_by_field])
    for changed in ({**child_by_field, "idempotency_key": "unmanaged"}, {**child_by_field, "params": {}}):
        with pytest.raises(ValueError):
            _require_lineage(record_by_field["admission_key"], [changed, parent_by_field])


def _worker():
    spec = WorkerSpec("arq:PTGCandidateAudit", "process.PTGCandidateAudit", ("plan-pricing-projection",))
    worker_payload = _AdmittedWorkerRequest(run_id="exact-run", importer="plan-pricing-projection")
    worker_payload.admitted_job = (
        "exact-run",
        "plan-pricing-projection",
        spec.queue,
        "control_single_job_start",
        "plan_pricing_projection_exact-run",
    )
    worker_payload.pricing_control_admission = PricingAdmission({}, {}, {}, object(), {})
    return spec, worker_payload


def test_only_native_admission_selects_fixed_nonoptional_policy_projection(monkeypatch):
    spec, worker_payload = _worker()
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_NAMESPACE_POLICY_CONFIGMAP", "namespace-policy")
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_PREPARATION_CONFIGMAP", "preparation-policy")
    volumes = pricing_worker_policy_volumes(spec, worker_payload)
    assert volumes == [
        {
            "volume": {
                "name": "pricing-control-policy",
                "projected": {
                    "defaultMode": 292,
                    "sources": [
                        {
                            "configMap": {
                                "name": "namespace-policy",
                                "optional": False,
                                "items": [{"key": "namespace-role-policy.json", "path": "namespace-role-policy.json"}],
                            }
                        },
                        {
                            "configMap": {
                                "name": "preparation-policy",
                                "optional": False,
                                "items": [{"key": "preparation.json", "path": "preparation.json"}],
                            }
                        },
                    ],
                },
            },
            "volumeMount": {"name": "pricing-control-policy", "mountPath": "/etc/pricing-control", "readOnly": True},
        }
    ]
    assert pricing_worker_policy_volumes(spec, dict(worker_payload, pricing_control_admission=True)) == []
    worker_payload.admitted_job = (*worker_payload.admitted_job[:-1], "different-job")
    with pytest.raises(ValueError):
        pricing_worker_policy_volumes(spec, worker_payload)


def test_managed_worker_receives_same_trusted_schema_only(monkeypatch):
    from api.plan_pricing_worker_policy import pricing_worker_policy_environment

    _spec, worker_payload = _worker()
    worker_payload["schema"] = "untrusted_request_value"
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "pricing_control")
    assert pricing_worker_policy_environment(worker_payload) == [
        {"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": "pricing_control"}
    ]
    assert pricing_worker_policy_environment(dict(worker_payload)) == []
    monkeypatch.delenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA")
    with pytest.raises(ValueError):
        pricing_worker_policy_environment(worker_payload)


def historical_worker(tmp_path, monkeypatch, *, revision="scalar_successor"):
    from api.plan_pricing_worker_policy import HistoricalPricingWorkerPolicy
    from process import plan_pricing_control_policy as policy

    path = tmp_path / "historical.json"
    path.write_text(
        json.dumps({"contract": policy.HISTORICAL_POLICY_CONTRACT, "schema": "control_catalog", "revision": revision})
    )
    monkeypatch.setattr(policy, "HISTORICAL_POLICY_PATH", path)
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "control_catalog")
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_HISTORICAL_CONFIGMAP", "historical-policy")
    spec, worker_payload = _worker()
    worker_payload.pricing_control_admission = None
    worker_payload.historical_pricing_policy = HistoricalPricingWorkerPolicy("control_catalog", revision, "exact-run")
    return spec, worker_payload, path


def test_historical_worker_receives_only_qualified_scalar_policy(tmp_path, monkeypatch):
    from api.plan_pricing_worker_policy import pricing_worker_policy_environment

    spec, worker_payload, _path = historical_worker(tmp_path, monkeypatch)
    worker_payload.update(revision="request_revision", policy_path="/untrusted", configmap="request-map")
    assert pricing_worker_policy_volumes(spec, worker_payload) == [
        {
            "volume": {
                "name": "pricing-control-policy",
                "projected": {
                    "defaultMode": 292,
                    "sources": [
                        {
                            "configMap": {
                                "name": "historical-policy",
                                "optional": False,
                                "items": [{"key": "historical.json", "path": "historical.json"}],
                            }
                        }
                    ],
                },
            },
            "volumeMount": {"name": "pricing-control-policy", "mountPath": "/etc/pricing-control", "readOnly": True},
        }
    ]
    assert pricing_worker_policy_environment(worker_payload) == [
        {"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": "control_catalog"}
    ]
    assert pricing_worker_policy_volumes(spec, dict(worker_payload)) == []
    assert pricing_worker_policy_environment(dict(worker_payload)) == []
    assert worker_payload.pricing_control_admission is None


@pytest.mark.parametrize("damage", ["run", "importer", "queue", "missing", "changed", "reference"])
def test_historical_worker_refuses_changed_identity_or_policy(tmp_path, monkeypatch, damage):
    spec, worker_payload, path = historical_worker(tmp_path, monkeypatch)
    match damage:
        case "run":
            worker_payload["run_id"] = "foreign-run"
        case "importer":
            worker_payload["importer"] = "unrelated-importer"
        case "queue":
            spec = WorkerSpec("arq:other", "process.Other", ("plan-pricing-projection",))
        case "missing":
            path.unlink()
        case "changed":
            declared = json.loads(path.read_text())
            declared["revision"] = "changed_revision"
            path.write_text(json.dumps(declared))
        case "reference":
            monkeypatch.delenv("HLTHPRT_PLAN_PRICING_HISTORICAL_CONFIGMAP")
    with pytest.raises(ValueError):
        pricing_worker_policy_volumes(spec, worker_payload)


def test_builtin_historical_worker_preserves_serialized_policy(tmp_path, monkeypatch):
    from api.plan_pricing_worker_policy import pricing_worker_policy_environment

    spec, worker_payload, path = historical_worker(tmp_path, monkeypatch, revision=min(HISTORICAL_REVISIONS))
    path.unlink()
    assert pricing_worker_policy_volumes(spec, worker_payload) == []
    assert pricing_worker_policy_environment(worker_payload) == [
        {"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": "control_catalog"}
    ]
    assert worker_payload.pricing_control_admission is None


@pytest.mark.asyncio
async def test_server_qualifies_historical_worker_without_managed_grant(monkeypatch):
    from api import plan_pricing_worker_policy as workers

    _spec, worker_payload = _worker()
    worker_payload.pricing_control_admission = None
    connection = AsyncMock()
    connection.fetchrow.return_value = {
        "importer": "plan-pricing-projection",
        "params": {},
        "idempotency_key": "plan-pricing-projection:" + "a" * 64,
    }
    monkeypatch.setattr(workers, "require_managed_pricing_admission", AsyncMock(return_value=None))
    monkeypatch.setattr(
        workers, "pricing_control_layout", AsyncMock(return_value=("control_catalog", "scalar_successor", False))
    )
    assert await workers.admit_pricing_worker(connection, "exact-run", worker_payload) is None
    assert worker_payload.historical_pricing_policy == workers.HistoricalPricingWorkerPolicy(
        "control_catalog", "scalar_successor", "exact-run"
    )


def test_job_container_includes_historical_policy_and_schema(tmp_path, monkeypatch):
    from api import control_workers

    spec, worker_payload, _path = historical_worker(tmp_path, monkeypatch)
    container, volumes, run_id = control_workers._worker_job_container(
        spec, worker_payload, "registry.example/worker:synthetic"
    )
    assert run_id == "exact-run"
    assert {"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": "control_catalog"} in container["env"]
    selected_volumes = [volume for volume in volumes if volume["volume"]["name"] == "pricing-control-policy"]
    assert selected_volumes == pricing_worker_policy_volumes(spec, worker_payload)
    assert selected_volumes[0]["volumeMount"] in container["volumeMounts"]


def test_process_worker_inherits_qualified_schema_and_fixed_policy(tmp_path, monkeypatch):
    from api import control_workers

    spec, worker_payload, path = historical_worker(tmp_path, monkeypatch)
    monkeypatch.setenv("HLTHPRT_WORKER_STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("HLTHPRT_WORKER_LOG_DIR", str(tmp_path / "logs"))
    started_environments = []

    def start(_command, **options):
        started_environments.append(options["env"])
        return SimpleNamespace(pid=42)

    monkeypatch.setattr(control_workers.subprocess, "Popen", start)
    assert control_workers._start_process(spec, worker_payload) == 42
    assert started_environments[0]["HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA"] == "control_catalog"
    assert started_environments[0]["HLTHPRT_CONTROL_RUN_ID"] == "exact-run"
    assert path.exists()


@pytest.mark.asyncio
async def test_unmanaged_request_uses_no_managed_schema_policy_or_connection(monkeypatch):
    from process.plan_pricing_admission import require_managed_pricing_admission

    monkeypatch.delenv("HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", raising=False)
    connection = AsyncMock()
    assert (
        await require_managed_pricing_admission(
            connection, {"importer": "plan-pricing-projection", "idempotency_key": "ordinary"}, action="create"
        )
        is None
    )
    assert not connection.mock_calls


@pytest.mark.parametrize("reference", ("", "UPPER", "has space", "../foreign", "a" * 254))
def test_managed_worker_refuses_missing_or_invalid_trusted_reference(monkeypatch, reference):
    spec, worker_payload = _worker()
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_NAMESPACE_POLICY_CONFIGMAP", reference)
    with pytest.raises(ValueError):
        pricing_worker_policy_volumes(spec, worker_payload)


def _control_catalog_rows():
    return [
        {
            "relname": name,
            "oid": oid,
            "relowner": 12,
            "relkind": "r",
            "relpersistence": "p",
            "parents": [],
            "relrowsecurity": False,
            "relforcerowsecurity": False,
            "inert": True,
            "native_checks": True,
            "direct_acl_closed": True,
            "readers_closed": True,
            "columns": deepcopy(shape["columns"]),
            "constraints": [
                {"name": key, "definition": definition, "valid": True, "type": "c"}
                for key, definition in shape["checks"].items()
            ],
            "indexes": [
                {
                    "keys": keys,
                    "primary": primary,
                    "unique": unique,
                    "valid": True,
                    "method": "btree",
                    "predicate": None,
                    "expression": None,
                }
                for keys, primary, unique in shape["indexes"]
            ],
        }
        for oid, (name, shape) in enumerate(control_catalog.CONTROL_SHAPES.items(), 41)
    ]


def _managed_case(monkeypatch, *, retry=False):
    case = _retained_case(monkeypatch)
    request_by_field, _record = _identity()
    request_by_field["publication_identity"] = case.admission.record_by_field["publication_identity"]
    root_key = projection_admission_key(request_by_field)
    params_by_field = {
        name: request_by_field[name] for name in ("binding_manifest_digest", "projection_contract", "bindings")
    }
    parent_by_field = {
        **_durable_import_request("plan-pricing-projection", "parent"),
        "idempotency_key": root_key,
        "params": params_by_field,
        "engine": "healthcare-mrf-api",
        "node_id": "synthetic_node",
        "triggered_by": "release",
        "retry_of_run_id": None,
        "status": "failed" if retry else "queued",
    }
    run_by_field = (
        {
            **parent_by_field,
            "run_id": "child",
            "retry_of_run_id": "parent",
            "idempotency_key": projection_retry_key(root_key, "parent"),
            "status": "queued",
        }
        if retry
        else parent_by_field
    )
    dispatches = [{"key": root_key, "retry_of": None, "run_id": "parent"}]
    if retry:
        dispatches.append({"key": run_by_field["idempotency_key"], "retry_of": "parent", "run_id": "child"})
    case.admission.record_by_field.update(
        request=json.dumps(request_by_field),
        request_digest=pricing_control_digest(request_by_field),
        admission_key=root_key,
        attempt_identity=request_by_field["publication_attempt_identity"],
        engine_run_id=run_by_field["run_id"],
        dispatches=json.dumps(dispatches),
        status="accepted",
        preparation_status="admitting",
        node_id="synthetic_node",
    )
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic_node")
    policy = PricingControlPolicy("storage_owner", "storage_publisher", ("storage_reader",), ("control_reader",))
    monkeypatch.setattr(admission, "load_pricing_control_policy", lambda: policy)
    verify = AsyncMock(return_value=case.admission.family)
    monkeypatch.setattr(admission, "verify_storage_family", verify)
    monkeypatch.setattr(admission, "storage_roles", AsyncMock())
    case.run, case.parent, case.policy, case.verify = run_by_field, parent_by_field, policy, verify
    return case


def _managed_connection(monkeypatch, case):
    connection, observe = _native_heap_observations(monkeypatch, case)
    heap_identity = connection.fetchrow.return_value
    admission_records = [deepcopy(case.admission.record_by_field), case.admission.record_by_field]

    async def fetchrow(sql, *args):
        if "session_user" in sql:
            return {"session": "control_reader", "current": "control_reader"}
        if "pg_relation_filenode" in sql:
            return heap_identity
        if "import_run" in sql:
            assert args == ("parent",)
            return {**case.parent, "params": json.dumps(case.parent["params"])}
        return admission_records.pop(0)

    async def fetch(sql, *args):
        if sql.startswith("SELECT to_regclass(name)"):
            return [{"oid": oid} for oid in (41, 42, 43)]
        if "version_num" in sql:
            return [{"version_num": BULK_REVISION}]
        return _control_catalog_rows()

    connection.fetchrow.side_effect = fetchrow
    connection.fetch = AsyncMock(side_effect=fetch)
    connection.fetchval = AsyncMock(
        side_effect=lambda sql, *args: "read committed" if sql.startswith("SHOW") else False
    )
    connection.execute = AsyncMock()
    connection.is_in_transaction = lambda: True
    return connection, observe


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("action", ["create", "launch", "claim", "replay", "inspect"])
async def test_managed_admission_keeps_exact_durable_parent_child_and_native_scope(monkeypatch, retry, action):
    case = _managed_case(monkeypatch, retry=retry)
    connection, observe = _managed_connection(monkeypatch, case)
    result = await admission.require_managed_pricing_admission(connection, case.run, action=action)
    assert type(result) is PricingAdmission and result.policy is case.policy and result.family is case.admission.family
    assert result.request["bindings"] == case.run["params"]["bindings"]
    assert result.dispatch == {
        "key": case.run["idempotency_key"],
        "retry_of": case.run["retry_of_run_id"],
        "run_id": case.run["run_id"],
    }
    assert observe.await_count == (0 if action in {"replay", "inspect"} else 2)
    locks = [call for call in connection.execute.await_args_list if "pg_advisory_xact_lock" in call.args[0]]
    assert len(locks) == (0 if action == "inspect" else 1)
    if locks:
        assert locks[0].args[1] == admission.projection_attempt_fence_key(
            result.request["publication_identity"], result.request["publication_attempt_identity"]
        )
    case.verify.assert_awaited_once_with(connection, "pricing_storage", **case.policy.storage_arguments())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,code",
    [
        ("attempt", "attempt_identity_changed"),
        ("node", "different request"),
        ("params", "different request"),
        ("parent", "retry_parent_not_terminal"),
        ("dispatch", "dispatch_run_changed"),
        ("closed", "admission_closed"),
        ("isolation", "fresh_control_snapshot_required"),
    ],
)
async def test_changed_protected_authority_never_reaches_retained_inputs(monkeypatch, damage, code):
    case = _managed_case(monkeypatch, retry=True)
    connection, observe = _managed_connection(monkeypatch, case)
    if damage == "attempt":
        case.admission.record_by_field["attempt_identity"] = "f" * 64
    if damage == "node":
        case.admission.record_by_field["node_id"] = "foreign_node"
    if damage == "params":
        case.run["params"] = {"changed": True}
    if damage == "parent":
        case.parent["status"] = "running"
    if damage == "dispatch":
        case.run["run_id"] = "foreign_run"
    if damage == "closed":
        case.admission.record_by_field["preparation_status"] = "published"
    if damage == "isolation":
        connection.fetchval.return_value = "repeatable read"
        connection.fetchval.side_effect = None
    with pytest.raises(ValueError, match=code):
        await admission.require_managed_pricing_admission(connection, case.run, action="claim")
    observe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,code",
    [
        ("owner", "protected_controls_open"),
        ("columns", "protected_columns_changed"),
        ("checks", "protected_constraints_changed"),
        ("index", "protected_indexes_changed"),
        ("operator", "protected_index_operator_changed"),
    ],
)
async def test_managed_control_catalog_refuses_changed_native_contract(damage, code, monkeypatch):
    _retained_case(monkeypatch)
    catalog_rows = _control_catalog_rows()
    if damage == "owner":
        catalog_rows[0]["relowner"] = 99
    elif damage == "columns":
        catalog_rows[0]["columns"] = []
    elif damage == "checks":
        catalog_rows[0]["constraints"][0]["valid"] = False
    elif damage == "index":
        catalog_rows[0]["indexes"][0]["predicate"] = "foreign_predicate"
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=catalog_rows), fetchval=AsyncMock(return_value=damage == "operator")
    )
    with pytest.raises(ValueError, match=code):
        await control_catalog.require_control_catalog(connection, {"owner_oid": 12, "reader_oids": [13]})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native_identity",
    [
        {"session": "storage_owner", "current": "storage_owner"},
        {"session": "storage_owner", "current": "control_reader"},
    ],
)
async def test_foreign_or_role_switched_control_actor_cannot_claim_admission(monkeypatch, native_identity):
    case = _managed_case(monkeypatch)
    connection, observe = _managed_connection(monkeypatch, case)
    connection.fetchrow.side_effect = None
    connection.fetchrow.return_value = native_identity
    with pytest.raises(ValueError, match="genuine_control_required"):
        await admission.require_managed_pricing_admission(connection, case.run, action="claim")
    case.verify.assert_not_awaited()
    observe.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_parent", [False, True])
async def test_missing_or_cyclic_retry_parent_refuses_before_native_authority(monkeypatch, missing_parent):
    case = _managed_case(monkeypatch, retry=True)
    connection, observe = _managed_connection(monkeypatch, case)
    case.parent["retry_of_run_id"] = "parent"
    if missing_parent:
        connection.fetchrow.side_effect = None
        connection.fetchrow.return_value = None
    with pytest.raises(ValueError, match="retry_parent_missing" if missing_parent else "retry_lineage_invalid"):
        await admission.require_managed_pricing_admission(connection, case.run, action="create")
    case.verify.assert_not_awaited()
    observe.assert_not_awaited()
    connection.execute.assert_not_awaited()
