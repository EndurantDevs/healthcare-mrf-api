# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One fixed trusted owner Job, using the existing Engine launcher identity."""

import asyncio
import hashlib
import json
import os
import time
import urllib.parse
from dataclasses import replace

from api import control_workers
from process.custom_import import family_shard_handoff as handoff
from process.custom_import.family_shard_owner_cli import (
    NODE_ENV,
    PROFILE_ENV,
    profile,
    require_capacity,
    require_profile,
)


def job_name(handoff_id):
    """Derive the only permitted Kubernetes Job name from the handoff digest."""
    if type(handoff_id) is not str or len(handoff_id) != 64 or any(c not in "0123456789abcdef" for c in handoff_id):
        handoff.unavailable()
    return "hci-owner-" + handoff_id[:40]


def manifest(signed, profile_document, *, with_reader=True):
    """Only public profile and custody references enter the Kubernetes spec."""
    require_profile(signed, profile_document)
    identifier = handoff.identity(signed)
    name = job_name(identifier)
    labels_by_key = {
        "app.kubernetes.io/name": "healthporta-import-worker",
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/owner-handoff": identifier[:40],
    }
    container_by_field = _owner_container(profile_document, identifier, with_reader=with_reader)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": profile_document["namespace"],
            "labels": labels_by_key,
            "annotations": {
                "healthporta.com/worker-class": "process.CustomImportFamilyOwner",
                "healthporta.com/owner-handoff": identifier,
            },
        },
        "spec": {
            "backoffLimit": 0,
            "parallelism": 1,
            "completions": 1,
            "template": {
                "metadata": {"labels": labels_by_key},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": profile_document["service_account"],
                    "nodeSelector": {"kubernetes.io/hostname": profile_document["node_name"]},
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "imagePullSecrets": [{"name": profile_document["image_pull_secret"]}],
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": control_workers._worker_job_pod_security_context(has_group_read_volume=False),
                    "containers": [container_by_field],
                    "volumes": [
                        {"name": "owner-storage", "persistentVolumeClaim": {"claimName": profile_document["pvc"]}}
                    ],
                },
            },
        },
    }


def _job_path(profile_document, identifier):
    return "/apis/batch/v1/namespaces/" + profile_document["namespace"] + "/jobs/" + job_name(identifier)


async def _request(method, path, body=None):
    return await asyncio.to_thread(control_workers._kubernetes_request, method, path, body)


async def observed(signed, profile_document):
    """Verify one exact owner Job while accepting only known server defaults."""
    identifier = handoff.identity(signed)
    return await _observed_job(manifest(signed, profile_document), profile_document, identifier)


async def _observed_job(expected, profile_document, identifier):
    """Use the same complete spec proof for original and explicitly signed child Jobs."""
    path = "/apis/batch/v1/namespaces/" + profile_document["namespace"] + "/jobs/" + expected["metadata"]["name"]
    try:
        job = await _request("GET", path)
    except control_workers._KubernetesApiError as error:
        if error.status == 404:
            return None
        raise
    metadata = job.get("metadata", {})
    if (
        not metadata.get("uid")
        or metadata.get("name") != expected["metadata"]["name"]
        or metadata.get("namespace") != profile_document["namespace"]
        or metadata.get("labels") != expected["metadata"]["labels"]
        or metadata.get("annotations") != expected["metadata"]["annotations"]
    ):
        handoff.unavailable()
    # Compare the complete authored spec, allowing only known API defaults.
    actual = job.get("spec", {})
    if set(actual) - {
        "backoffLimit",
        "parallelism",
        "completions",
        "template",
        "selector",
        "manualSelector",
        "completionMode",
        "suspend",
        "podReplacementPolicy",
    }:
        handoff.unavailable()
    if (
        actual.get("manualSelector", False) is not False
        or actual.get("suspend", False) is not False
        or actual.get("completionMode", "NonIndexed") != "NonIndexed"
        or actual.get("podReplacementPolicy", "TerminatingOrFailed") != "TerminatingOrFailed"
    ):
        handoff.unavailable()
    if actual.get("selector") != {"matchLabels": {"batch.kubernetes.io/controller-uid": metadata["uid"]}}:
        handoff.unavailable()
    template_metadata = actual.get("template", {}).get("metadata", {})
    expected_labels_by_key = dict(expected["spec"]["template"]["metadata"]["labels"])
    for prefix in ("batch.kubernetes.io/", ""):
        expected_labels_by_key[prefix + "controller-uid"] = metadata["uid"]
        expected_labels_by_key[prefix + "job-name"] = metadata["name"]
    if template_metadata.get("labels") != expected_labels_by_key or template_metadata.get(
        "annotations", {}
    ) != expected["spec"]["template"]["metadata"].get("annotations", {}):
        handoff.unavailable()
    for field in ("backoffLimit", "parallelism", "completions"):
        if actual.get(field) != expected["spec"][field]:
            handoff.unavailable()
    if actual.get("activeDeadlineSeconds") is not None or actual.get("ttlSecondsAfterFinished") is not None:
        handoff.unavailable()
    _require_owner_pod(actual, expected, profile_document)
    return _observed_receipt(job, identifier)


async def launch(signed, bearer, factory):
    """Admit and create one deterministic owner Job, retaining uncertain replies."""
    from process.custom_import.build_source import _page_session
    from process.custom_import.execution import heartbeat_execution
    from process.custom_import.family_shard_production import original_request, require_native
    from process.custom_import.snowflake_segmented_runner import _retained_base

    profile_document, key, _limits = profile()
    descriptor = handoff.verify(signed, verifying_key=key, bearer=bearer, now=time.time())
    require_profile(signed, profile_document)
    await _require_owner(factory, profile_document)
    original = await original_request(factory, descriptor, bearer)
    require_native(descriptor, original.native)
    bounded_request = replace(
        original.build_request, authorization_expires_at=handoff.timestamp(descriptor["authorization_expires_at"])
    )
    require_capacity(profile_document)
    identifier = handoff.identity(signed)
    existing = await observed(signed, profile_document)
    if existing is not None:
        handoff.is_launch_file_created(signed, "job.json", {"job_uid": existing["job_uid"], "handoff_id": identifier})
        return existing
    async with _page_session(factory, bounded_request) as (session, _build):
        retained = await _retained_base(session, bounded_request)
        if (
            retained.expected_base_generation_id != descriptor["base_generation_id"]
            or retained.expected_pointer_version != descriptor["base_pointer_version"]
        ):
            handoff.unavailable()
        # Native cancellation takes the same locks. It therefore observes either
        # no staging or the complete durable intent, never an unregistered gap.
        handoff.stage(signed, bearer, parent=handoff.OWNER_PARENT)
        if not handoff.is_launch_file_created(signed, "launch.json", {"handoff_id": identifier, "state": "attempted"}):
            return {"handoff_id": identifier, "status": "launch_pending"}
        grant = await heartbeat_execution(
            session,
            execution_id=descriptor["engine_execution_id"],
            fence=descriptor["fence"],
            token=bearer,
            lease_seconds=original.policy.build.lease_seconds,
        )
        if grant is None or grant.state != "running":
            handoff.unavailable()
    handoff.verify(signed, verifying_key=key, bearer=bearer, now=time.time())
    # No generic ensure/retry: a lost POST reply stays uncertain until exact GET.
    try:
        await _request(
            "POST",
            "/apis/batch/v1/namespaces/" + profile_document["namespace"] + "/jobs",
            manifest(signed, profile_document),
        )
    except control_workers._KubernetesApiError as error:
        if error.status != 409 and error.status != 0:
            raise
    job_receipt = await observed(signed, profile_document)
    if job_receipt is not None:
        handoff.is_launch_file_created(
            signed, "job.json", {"job_uid": job_receipt["job_uid"], "handoff_id": identifier}
        )
    return {"handoff_id": identifier, "status": "launch_pending"} if job_receipt is None else job_receipt


def custody_receipt(signed):
    """Return the protected root identity without reading it into logs."""
    identifier = handoff.identity(signed)
    custody_by_field, _bearer = handoff.reopen(identifier, parent=handoff.OWNER_PARENT)
    if custody_by_field != signed:
        handoff.unavailable()
    metadata = (handoff.OWNER_PARENT / identifier).lstat()
    return {"handoff_id": identifier, "root_device": metadata.st_dev, "root_inode": metadata.st_ino}


async def is_stopped(signed, profile_document, *, expected_uid, require_terminal=False):
    """Caller has already established native cancellation/finality."""
    identifier = handoff.identity(signed)
    return await _is_stopped_job(
        manifest(signed, profile_document),
        profile_document,
        identifier,
        expected_uid=expected_uid,
        require_terminal=require_terminal,
    )


async def _is_stopped_job(expected, profile_document, identifier, *, expected_uid, require_terminal=False):
    job = await _observed_job(expected, profile_document, identifier)
    if job is not None:
        if require_terminal and not (job["status"]["succeeded"] or job["status"]["failed"]):
            return False
        if expected_uid is not None and job["job_uid"] != expected_uid:
            handoff.unavailable()
        expected_uid = job["job_uid"]
        await _request(
            "DELETE",
            "/apis/batch/v1/namespaces/" + profile_document["namespace"] + "/jobs/" + expected["metadata"]["name"],
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": expected_uid},
                "propagationPolicy": "Foreground",
            },
        )
    if expected_uid is None:
        handoff.unavailable()
    selector = urllib.parse.urlencode({"labelSelector": "batch.kubernetes.io/controller-uid=" + expected_uid})
    pods = await _request("GET", "/api/v1/namespaces/" + profile_document["namespace"] + "/pods?" + selector)
    if type(pods.get("items")) is not list or len(pods["items"]) > 1:
        handoff.unavailable()
    return await _observed_job(expected, profile_document, identifier) is None and not pods["items"]


def _remove_empty_attempts(root):
    """Consume only an exact lock-only child directory after terminal custody proof."""
    from process.custom_import import family_shard_owner_attempt_files as files

    lock = files.attempt_lock(root)
    try:
        if set(os.listdir(root / "attempts")) != {files._NAME}:
            handoff.unavailable()
        lock.remove_terminal_root()
    finally:
        lock.close()


def cleanup(signed, receipt):
    """Remove only a terminal, empty, exact-inode custody tree after Pod absence."""
    from process.custom_import import family_shard_retirement_handoff as retired

    custody_by_field = {"handoff_id": handoff.identity(signed)}
    root = handoff.OWNER_PARENT / custody_by_field["handoff_id"]
    try:
        metadata = root.lstat()
    except FileNotFoundError:
        return
    custody_by_field |= {"root_device": metadata.st_dev, "root_inode": metadata.st_ino}
    if any(receipt.get(name) != profile_document for name, profile_document in custody_by_field.items()):
        handoff.unavailable()
    fd = handoff._directory(root)
    try:
        names = set(os.listdir(fd))
        retirement_names = {retired.INTENT, retired.SENT, retired.JOB, retired.COMPLETE, retired.SUCCEEDED}
        if names - {
            "delegation.json",
            "bearer",
            "committed",
            "work",
            "launch.json",
            "job.json",
            "attempts",
            *retirement_names,
        }:
            handoff.unavailable()
        if "committed" in names and handoff.document(handoff.read_file(fd, "committed", 512)) != {
            "handoff_id": custody_by_field["handoff_id"],
            "device": custody_by_field["root_device"],
            "inode": custody_by_field["root_inode"],
        }:
            handoff.unavailable()
        work = root / "work"
        if "work" in names:
            work_fd = handoff._directory(work)
            try:
                if os.listdir(work_fd):
                    handoff.unavailable()
            finally:
                os.close(work_fd)
        for name in names - {"work", "attempts"}:
            handoff.read_file(fd, name, handoff.MAX_DOCUMENT_BYTES)
        if "attempts" in names:
            _remove_empty_attempts(root)
            names.remove("attempts")
        for name in ("delegation.json", "bearer", "launch.json", "job.json", *sorted(retirement_names)):
            if name in names:
                os.unlink(name, dir_fd=fd)
        if "work" in names:
            os.rmdir("work", dir_fd=fd)
        if "committed" in names:
            os.unlink("committed", dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(fd)
    root.rmdir()


def _owner_container(profile_document, identifier, *, with_reader=False):
    """Render the fixed command and protected input/work mounts."""
    from process.custom_import.family_block_reader import activation_profile

    reader_profile = activation_profile(profile_document)
    if type(with_reader) is not bool:
        handoff.unavailable()
    cpu = (
        str(profile_document["cpu_millis"] // 1000)
        if profile_document["cpu_millis"] % 1000 == 0
        else str(profile_document["cpu_millis"]) + "m"
    )
    memory = (
        str(profile_document["memory_bytes"] // 1024**3) + "Gi"
        if profile_document["memory_bytes"] % 1024**3 == 0
        else str(profile_document["memory_bytes"] // 1024**2) + "Mi"
    )
    resources_by_kind = {"cpu": cpu, "memory": memory}
    container_by_field = {
        "name": "worker",
        "image": profile_document["image"],
        "imagePullPolicy": "IfNotPresent",
        "workingDir": "/opt",
        "command": ["/opt/venv/bin/python", "-m", "process.custom_import.family_shard_owner_cli", identifier],
        "env": [
            {"name": PROFILE_ENV, "value": handoff.canonical(profile_document).decode("ascii")},
            {"name": NODE_ENV, "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "spec.nodeName"}}},
            {"name": "HLTHPRT_DB_ECHO", "value": "false"},
            {"name": "HLTHPRT_DB_POOL_MIN_SIZE", "value": "1"},
            {"name": "HLTHPRT_DB_POOL_MAX_SIZE", "value": "4"},
            *(
                []
                if reader_profile is None or not with_reader
                else [
                    {"name": name, "valueFrom": {"secretKeyRef": {"name": reader_profile.secret, "key": name}}}
                    for name in ("HLTHPRT_DB_READER_USER", "HLTHPRT_DB_READER_PASSWORD")
                ]
            ),
        ],
        "envFrom": [
            {"configMapRef": {"name": profile_document["database_configmap"]}},
            {"secretRef": {"name": profile_document["database_secret"]}},
        ],
        "resources": {"requests": resources_by_kind, "limits": dict(resources_by_kind)},
        "securityContext": control_workers._worker_job_container_security_context() | {"readOnlyRootFilesystem": True},
        "volumeMounts": [
            {
                "name": "owner-storage",
                "mountPath": str(handoff.OWNER_INPUT),
                "subPath": "custom-import-owner/" + identifier,
                "readOnly": True,
            },
            {
                "name": "owner-storage",
                "mountPath": str(handoff.OWNER_PARENT),
                "subPath": "custom-import-owner/" + identifier + "/work",
            },
        ],
    }
    return container_by_field


def _require_owner_pod(actual, expected, profile_document):
    """Compare every authored Pod field and deny unregistered additions."""
    actual_pod = actual.get("template", {}).get("spec", {})
    for field, value_expected in expected["spec"]["template"]["spec"].items():
        if actual_pod.get(field) != value_expected:
            # Container termination-message defaults are the only extra fields.
            if field != "containers" or len(actual_pod.get(field, [])) != 1:
                handoff.unavailable()
            container_by_field = dict(actual_pod[field][0])
            if (
                container_by_field.pop("terminationMessagePath", "/dev/termination-log") != "/dev/termination-log"
                or container_by_field.pop("terminationMessagePolicy", "File") != "File"
                or container_by_field != value_expected[0]
            ):
                handoff.unavailable()
    permitted_fields = {
        *expected["spec"]["template"]["spec"],
        "dnsPolicy",
        "schedulerName",
        "terminationGracePeriodSeconds",
        "serviceAccount",
    }
    if actual_pod.get("serviceAccount", profile_document["service_account"]) != profile_document["service_account"]:
        handoff.unavailable()
    if (
        set(actual_pod) - permitted_fields
        or actual_pod.get("dnsPolicy", "ClusterFirst") != "ClusterFirst"
        or actual_pod.get("schedulerName", "default-scheduler") != "default-scheduler"
    ):
        handoff.unavailable()


def _observed_receipt(job, identifier):
    """Retain only the exact Job identity and bounded status counters."""
    return {
        "handoff_id": identifier,
        "job_name": job["metadata"]["name"],
        "job_uid": job["metadata"]["uid"],
        "status": {key: job.get("status", {}).get(key, 0) for key in ("active", "succeeded", "failed")},
    }


async def _require_owner(factory, profile_document):
    """Keep the existing actual SOURCE and block-table owner checks."""
    from process.custom_import.family_shard_owner_access import require_owner_connection

    async with factory() as session, session.begin():
        await require_owner_connection(session, profile_document["owner_role"])
