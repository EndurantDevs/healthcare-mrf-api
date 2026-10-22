# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A settled witness stays fenced until its final evidence has been encoded."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_no_work_backends as backends
from api import plan_pricing_no_work_http as http
from db.models import ImportRun
from process import plan_pricing_no_work as native
from tests.test_plan_pricing_no_work import _terminal
from tests.test_plan_pricing_no_work_backends import _inventory, native_run


class ControlRows:
    def __init__(self, run):
        self.run = run
        self.children = []
        self.queries = []
        self.columns = [
            {
                "attname": name,
                "type": "timestamp without time zone" if name.endswith("_at") else "text",
            }
            for name in ImportRun.__table__.columns.keys()
        ]
        self.index = {
            "valid": True,
            "keys": ["retry_of_run_id", "run_id"],
            "predicate": native.RETRY_NATIVE_PREDICATE,
        }

    async def execute(self, query, *args):
        self.queries.append((query, args))

    async def fetchrow(self, query, *args):
        self.queries.append((query, args))
        return self.index if "pg_index" in query else self.run

    async def fetch(self, query, *args):
        self.queries.append((query, args))
        if "pg_attribute" in query:
            return self.columns
        return self.children if "LIMIT 4" in query else [self.run]

    async def fetchval(self, query, *args):
        self.queries.append((query, args))
        return "f" * 64


@pytest.mark.asyncio
async def test_native_lineage_reads_complete_request_and_exact_direct_children():
    admission, run = _terminal()
    rows = ControlRows(run)
    assert await native.native_run(rows, "exact") == run
    await native.require_native_control_shape(rows)
    runs, digest = await native.native_lineage_witness(rows, admission)
    assert runs == [run] and digest == "f" * 64
    child_query = next(args for query, args in rows.queries if "LIMIT 4" in query)
    assert child_query == (["exact"],)
    rows.children = [{"run_id": "unexpected", "retry_of_run_id": "exact"}]
    with pytest.raises(ValueError, match="native_lineage_extra_child"):
        await native.native_lineage_witness(rows, admission)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_run", "columns", "timestamp", "index", "keys", "predicate"])
async def test_native_witness_refuses_incomplete_or_changed_control_shape(damage):
    _admission, run = _terminal()
    rows = ControlRows(run)
    if damage == "missing_run":
        rows.run = None
        with pytest.raises(ValueError, match="native_run_missing"):
            await native.native_run(rows, "exact")
        return
    if damage == "columns":
        rows.columns.reverse()
    elif damage == "timestamp":
        next(row for row in rows.columns if row["attname"] == "created_at")["type"] = "timestamp with time zone"
    else:
        rows.index[{"index": "valid", "keys": "keys", "predicate": "predicate"}[damage]] = {
            "index": False,
            "keys": ["run_id"],
            "predicate": "true",
        }[damage]
    with pytest.raises(ValueError, match="native_(run|retry)_"):
        await native.require_native_control_shape(rows)


@pytest.fixture
def admitted_witness(monkeypatch):
    admission, run = _terminal()
    admission.record_by_field.update(preparation_id="synthetic-preparation", scope_digest="a" * 64)
    control_rows = ControlRows(run)
    events = []
    session = object()

    @asynccontextmanager
    async def transaction():
        events.append("transaction-open")
        try:
            yield session
        finally:
            events.append("transaction-closed")

    async def driver(actual_session):
        assert actual_session is session
        return control_rows

    async def lock(_connection, run_id):
        assert events[-1] != "transaction-closed"
        events.append(("lock", run_id))

    async def retained(_driver, _admission):
        events.append("retained")

    def kubernetes(actual_run):
        assert actual_run == run and events[-1] != "transaction-closed"
        events.append("kubernetes")
        return {"stopped": True}

    async def redis(actual_run):
        assert actual_run == run and events[-1] == "kubernetes"
        events.append("redis")
        return {"queue_member": False}

    monkeypatch.setattr(http, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(http, "pricing_driver", driver)
    monkeypatch.setattr(http, "ConnectionProxy", lambda *_args: object())
    monkeypatch.setattr(http, "acquire_control_run_worker_action_lock", lock)
    monkeypatch.setattr(http, "require_retained_pricing_inputs", retained)
    monkeypatch.setattr(http, "pricing_kubernetes_no_work", kubernetes)
    monkeypatch.setattr(http, "pricing_redis_no_work", redis)
    admission_read = AsyncMock(return_value=admission)
    monkeypatch.setattr(http, "require_managed_pricing_admission", admission_read)
    return SimpleNamespace(admission=admission, rows=control_rows, events=events, admission_read=admission_read)


@pytest.mark.asyncio
async def test_complete_witness_holds_transaction_through_caller_encoding(admitted_witness):
    state = admitted_witness
    async with http.managed_pricing_no_work("exact") as receipt:
        assert state.events == ["transaction-open", ("lock", "exact"), "retained", "kubernetes", "redis"]
        assert receipt["contract"] == native.CONTRACT
        assert receipt["lineage_digest"] == "f" * 64
        assert receipt["runs"][0]["run_id"] == "exact"
        assert receipt["runs"][0]["kubernetes"] == {"stopped": True}
        assert receipt["runs"][0]["redis"] == {"queue_member": False}
        assert "bindings" not in str(receipt)
    assert state.events[-1] == "transaction-closed"
    assert [call.kwargs["action"] for call in state.admission_read.await_args_list] == ["inspect", "replay", "replay"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["not_managed", "lineage", "final_record", "backend"])
async def test_changed_or_unavailable_evidence_closes_fence_without_receipt(admitted_witness, monkeypatch, damage):
    state = admitted_witness
    changed = deepcopy(state.admission)
    if damage == "not_managed":
        state.admission_read.return_value = None
    elif damage == "lineage":
        changed.record_by_field["dispatches"][0]["run_id"] = "different"
        changed.record_by_field["engine_run_id"] = "different"
        state.admission_read.side_effect = [state.admission, changed]
    elif damage == "final_record":
        changed.record_by_field["scope_digest"] = "b" * 64
        state.admission_read.side_effect = [state.admission, state.admission, changed]
    else:
        monkeypatch.setattr(http, "pricing_redis_no_work", AsyncMock(side_effect=ConnectionError("unavailable")))
    with pytest.raises(ConnectionError if damage == "backend" else ValueError):
        async with http.managed_pricing_no_work("exact"):
            pytest.fail("An unqualified witness was returned")
    assert state.events[-1] == "transaction-closed"


@pytest.mark.parametrize("outcome", ["terminal", "absent", "named_changed", "read_failure", "unconfigured"])
def test_backend_inventory_joins_fresh_list_and_exact_named_job(monkeypatch, outcome):
    labels, job, pod = _inventory()
    worker = backends.control_workers
    monkeypatch.setattr(worker, "_exact_worker_spec", lambda _payload: {})
    monkeypatch.setattr(worker, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(worker, "_is_kubernetes_configured", lambda: outcome != "unconfigured")
    monkeypatch.setattr(worker, "_kubernetes_namespace", lambda: "synthetic")
    monkeypatch.setattr(worker, "_kubernetes_label_selector", lambda *_args: "synthetic=exact")
    monkeypatch.setattr(worker, "_worker_job_name", lambda *_args: "exact-job")
    requests = []

    def request(method, path, *, timeout_seconds):
        assert method == "GET" and timeout_seconds == 2
        requests.append(path)
        if path.endswith("/jobs/exact-job"):
            if outcome in {"absent", "read_failure"}:
                raise worker._KubernetesApiError(404 if outcome == "absent" else 503, "unavailable")
            return {"metadata": {"uid": "changed"}} if outcome == "named_changed" else job
        return {
            "metadata": {},
            "items": [] if outcome == "absent" else [job if "/jobs?" in path else pod],
        }

    monkeypatch.setattr(worker, "_kubernetes_request", request)
    if outcome in {"terminal", "absent"}:
        receipt = backends.pricing_kubernetes_no_work(native_run())
        assert receipt["stopped"] is True
        assert receipt["job_count"] == receipt["pod_count"] == (outcome == "terminal")
    else:
        with pytest.raises(worker._KubernetesApiError if outcome == "read_failure" else ValueError):
            backends.pricing_kubernetes_no_work(native_run())
    assert len(requests) == (0 if outcome == "unconfigured" else 3)
    assert all("labelSelector=synthetic%3Dexact&limit=129" in path for path in requests[:2])
