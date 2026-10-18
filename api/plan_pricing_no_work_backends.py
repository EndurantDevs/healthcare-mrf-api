# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh exact backend inventories, without queue or worker mutation."""

import hashlib
import urllib.parse

from arq.connections import create_pool
from arq.jobs import deserialize_result

from api import control_workers
from process.plan_pricing_admission import require
from process.plan_pricing_control import pricing_control_digest
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


def canonical_pricing_job(run_by_field):
    """Require the durable initial adapter identity rather than infer missing fields."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS, _adapter_payload, _enqueue_job_options

    adapter = _SINGLE_JOB_ADAPTERS["plan-pricing-projection"]
    job_id = _enqueue_job_options(adapter, run_by_field)["_job_id"]
    metrics = run_by_field["metrics"]
    require(
        all(
            metrics.get(name) == expected
            for name, expected in (
                ("enqueue_adapter", "arq_single_job"),
                ("queue", adapter["queue"]),
                ("function", adapter["function"]),
                ("job_id", job_id),
            )
        ),
        "backend_job_identity_changed",
    )
    return adapter, job_id, _adapter_payload(adapter, run_by_field, run_by_field["params"])


async def pricing_redis_no_work(run_by_field):
    """Atomically inspect the exact ARQ job without changing any Redis key."""
    from api.control_imports import _arq_evidence_keys

    adapter, job_id, expected_task = canonical_pricing_job(run_by_field)
    pool = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    try:
        async with pool.pipeline(transaction=True) as pipe:
            pipe.zscore(adapter["queue"], job_id)
            for field, key in _arq_evidence_keys(job_id):
                pipe.get(key) if field == "result" else pipe.exists(key)
            queue_score, job, retry, in_progress, result_bytes = await pipe.execute()
    finally:
        await pool.aclose(close_connection_pool=True)
    require(queue_score is None and not any((job, retry, in_progress)), "redis_work_present")
    result_digest = None
    if result_bytes is not None:
        require(type(result_bytes) is bytes and len(result_bytes) <= 16 * 1024 * 1024, "redis_result_invalid")
        decoded = deserialize_result(result_bytes, deserializer=deserialize_job)
        require(
            decoded.function == adapter["function"]
            and decoded.queue_name == adapter["queue"]
            and decoded.job_id == job_id
            and decoded.kwargs == {}
            and pricing_control_digest(decoded.args) == pricing_control_digest([expected_task]),
            "redis_result_identity_changed",
        )
        result_digest = hashlib.sha256(result_bytes).hexdigest()
    return {
        "queue": adapter["queue"],
        "job_id": job_id,
        "queue_member": False,
        "job": False,
        "retry": False,
        "in_progress": False,
        "result": {"present": result_bytes is not None, "digest": result_digest},
    }


def _worker_records(path, maximum):
    response = control_workers._kubernetes_request("GET", path, timeout_seconds=2)
    require(
        type(response) is dict
        and type(response.get("items")) is list
        and type(response.get("metadata")) is dict
        and not response["metadata"].get("continue")
        and len(response["items"]) <= maximum
        and all(type(item) is dict for item in response["items"]),
        "kubernetes_inventory_invalid",
    )
    return response["items"]


def _worker_identity(record, namespace, labels):
    metadata = record.get("metadata")
    require(
        type(metadata) is dict
        and type(metadata.get("uid")) is str
        and bool(metadata["uid"])
        and metadata.get("namespace") == namespace
        and not metadata.get("deletionTimestamp")
        and all(metadata.get("labels", {}).get(name) == value for name, value in labels.items()),
        "kubernetes_identity_changed",
    )
    return metadata


def pricing_kubernetes_no_work(run_by_field):
    """Require exact terminal Jobs and UID-owned terminal Pods from fresh API reads."""
    adapter, _job_id, _task = canonical_pricing_job(run_by_field)
    worker_payload_by_field = {
        "run_id": run_by_field["run_id"],
        "importer": run_by_field["importer"],
        "queue": adapter["queue"],
    }
    spec = control_workers._exact_worker_spec(worker_payload_by_field)
    require(
        control_workers._launcher_mode() == "kubernetes" and control_workers._is_kubernetes_configured(),
        "kubernetes_unavailable",
    )
    namespace = control_workers._kubernetes_namespace()
    selector = control_workers._kubernetes_label_selector(spec, worker_payload_by_field)
    query = urllib.parse.urlencode({"labelSelector": selector, "limit": 129})
    jobs = _worker_records(f"/apis/batch/v1/namespaces/{namespace}/jobs?{query}", 32)
    pods = _worker_records(f"/api/v1/namespaces/{namespace}/pods?{query}", 128)
    job_name = control_workers._worker_job_name(spec, worker_payload_by_field)
    try:
        named_job = control_workers._kubernetes_request(
            "GET", f"/apis/batch/v1/namespaces/{namespace}/jobs/{job_name}", timeout_seconds=2
        )
    except control_workers._KubernetesApiError as error:
        if error.status != 404:
            raise
        named_job = None
    require(
        (named_job is None and not jobs)
        or (
            len(jobs) == 1
            and named_job is not None
            and named_job.get("metadata", {}).get("uid") == jobs[0].get("metadata", {}).get("uid")
        ),
        "kubernetes_exact_job_changed",
    )
    return _terminal_inventory(
        namespace, job_name, dict(pair.split("=", 1) for pair in selector.split(",")), jobs, pods
    )


def _terminal_inventory(namespace, job_name, labels, jobs, pods):
    job_uids = set()
    coordinates = []
    for job in jobs:
        metadata = _worker_identity(job, namespace, labels)
        require(
            metadata.get("name") == job_name and control_workers._is_terminal_worker_job(job), "kubernetes_job_active"
        )
        job_uids.add(metadata["uid"])
        coordinates.append([metadata["uid"], "Job", "terminal", None])
    for pod in pods:
        metadata = _worker_identity(pod, namespace, labels)
        owners = metadata.get("ownerReferences", [])
        phase = (pod.get("status") or {}).get("phase")
        require(
            phase in {"Succeeded", "Failed"}
            and len(owners) == 1
            and owners[0].get("kind") == "Job"
            and owners[0].get("controller") is True
            and owners[0].get("uid") in job_uids,
            "kubernetes_pod_active_or_unowned",
        )
        coordinates.append([metadata["uid"], "Pod", phase, owners[0]["uid"]])
    require(len({coordinate[0] for coordinate in coordinates}) == len(coordinates), "kubernetes_uid_reused")
    return {
        "namespace": namespace,
        "exact_job_name": job_name,
        "job_count": len(jobs),
        "pod_count": len(pods),
        "inventory_digest": pricing_control_digest(sorted(coordinates)),
        "stopped": True,
    }
