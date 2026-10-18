# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh exact backend evidence rejects active, foreign or incomplete inventory."""

import hashlib
from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from arq.jobs import serialize_result

from api import plan_pricing_no_work_backends as backends


def native_run():
    return {
        "run_id": "synthetic-run",
        "importer": "plan-pricing-projection",
        "params": {},
        "family": "ptg",
        "metrics": {
            "enqueue_adapter": "arq_single_job",
            "queue": "arq:PTGCandidateAudit",
            "function": "control_single_job_start",
            "job_id": "plan_pricing_projection_synthetic-run",
        },
    }


def _inventory():
    labels_by_name = {"synthetic": "exact"}
    job_by_field = {
        "metadata": {"name": "exact-job", "namespace": "synthetic", "uid": "job-uid", "labels": labels_by_name},
        "status": {"active": 0, "conditions": [{"type": "Complete", "status": "True"}]},
    }
    pod_by_field = {
        "metadata": {
            "name": "pod",
            "namespace": "synthetic",
            "uid": "pod-uid",
            "labels": labels_by_name,
            "ownerReferences": [{"uid": "job-uid", "kind": "Job", "controller": True}],
        },
        "status": {"phase": "Succeeded"},
    }
    return labels_by_name, job_by_field, pod_by_field


def test_terminal_inventory_binds_native_uid_owner_and_phase():
    labels_by_name, job_by_field, pod_by_field = _inventory()
    receipt = backends._terminal_inventory("synthetic", "exact-job", labels_by_name, [job_by_field], [pod_by_field])
    assert receipt["stopped"] is True and receipt["job_count"] == receipt["pod_count"] == 1
    assert len(receipt["inventory_digest"]) == 64


@pytest.mark.parametrize(
    "damage", ("active_job", "active_pod", "owner", "deleting", "namespace", "labels", "duplicate_uid")
)
def test_terminal_inventory_refuses_foreign_or_active_members(damage):
    labels_by_name, job_by_field, pod_by_field = deepcopy(_inventory())
    target_by_damage = {
        "active_job": (job_by_field["status"], "active", 1),
        "active_pod": (pod_by_field["status"], "phase", "Running"),
        "owner": (pod_by_field["metadata"]["ownerReferences"][0], "uid", "foreign"),
        "deleting": (pod_by_field["metadata"], "deletionTimestamp", "now"),
        "namespace": (pod_by_field["metadata"], "namespace", "foreign"),
        "labels": (pod_by_field["metadata"], "labels", {}),
        "duplicate_uid": (pod_by_field["metadata"], "uid", job_by_field["metadata"]["uid"]),
    }
    target_by_field, field, changed = target_by_damage[damage]
    target_by_field[field] = changed
    with pytest.raises(ValueError):
        backends._terminal_inventory("synthetic", "exact-job", labels_by_name, [job_by_field], [pod_by_field])


@pytest.mark.parametrize(
    "inventory", ({}, {"items": [], "metadata": {"continue": "next"}}, {"items": [None], "metadata": {}})
)
def test_incomplete_inventory_never_means_absence(monkeypatch, inventory):
    monkeypatch.setattr(backends.control_workers, "_kubernetes_request", lambda *_, **_kwargs: inventory)
    with pytest.raises(ValueError):
        backends._worker_records("fixed", 32)


def test_missing_or_substituted_queue_identity_refuses():
    run_by_field = native_run()
    backends.canonical_pricing_job(run_by_field)
    for field in ("queue", "function", "job_id"):
        changed = deepcopy(run_by_field)
        changed["metrics"].pop(field)
        with pytest.raises(ValueError):
            backends.canonical_pricing_job(changed)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["absent", "terminal", "queued", "foreign_result", "read_failure"])
async def test_redis_proof_reads_exact_job_atomically_and_always_closes_owned_pool(monkeypatch, outcome):
    run = native_run()
    adapter, job_id, task = backends.canonical_pricing_job(run)
    serialized_result = None
    if outcome in {"terminal", "foreign_result"}:
        serialized_result = serialize_result(
            adapter["function"],
            (task,),
            {},
            1,
            0,
            True,
            {"opaque": True},
            1,
            2,
            job_id,
            "foreign" if outcome == "foreign_result" else adapter["queue"],
            job_id,
            serializer=backends.serialize_job,
        )
        assert type(serialized_result) is bytes
    pipe = MagicMock()
    pipe.__aenter__.return_value = pipe
    pipe.execute = AsyncMock(return_value=[0 if outcome == "queued" else None, 0, 0, 0, serialized_result])
    if outcome == "read_failure":
        pipe.execute.side_effect = ConnectionError("example read failed")
    pool = MagicMock()
    pool.pipeline.return_value = pipe
    pool.aclose = AsyncMock()
    monkeypatch.setattr(backends, "create_pool", AsyncMock(return_value=pool))
    monkeypatch.setattr(backends, "build_redis_settings", lambda: object())
    if outcome in {"absent", "terminal"}:
        receipt = await backends.pricing_redis_no_work(run)
        assert receipt["queue"] == adapter["queue"] and receipt["job_id"] == job_id
        assert receipt["result"] == {
            "present": serialized_result is not None,
            "digest": hashlib.sha256(serialized_result).hexdigest() if serialized_result is not None else None,
        }
        assert "opaque" not in str(receipt)
    else:
        with pytest.raises(ConnectionError if outcome == "read_failure" else ValueError):
            await backends.pricing_redis_no_work(run)
    pool.pipeline.assert_called_once_with(transaction=True)
    pipe.zscore.assert_called_once_with(adapter["queue"], job_id)
    assert {call[0] for call in pipe.method_calls} <= {"zscore", "exists", "get", "execute"}
    pool.aclose.assert_awaited_once_with(close_connection_pool=True)
