# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact trusted copy Job lifecycle through the Engine's existing Kubernetes identity."""

import urllib.parse

from sqlalchemy import select

from api import control_workers
from api import custom_import_owner_attempt as children
from api import custom_import_owner_worker as owner
from process.custom_import import execution as lifecycle
from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy as service
from process.custom_import import retained_capture_copy_contract as contract
from process.custom_import import retained_capture_copy_files as files
from process.custom_import import retained_capture_copy_native as native
from process.custom_import.family_shard_owner_cli import require_capacity


def manifest(signed, configuration):
    """Use the existing owner profile with one fixed copy command and read-only input."""
    identifier = contract.identity(signed)
    owner.require_profile(signed, configuration)
    labels_by_key = {
        "app.kubernetes.io/name": "healthporta-import-worker",
        "app.kubernetes.io/managed-by": "healthporta-worker-launcher",
        "healthporta.com/retained-copy": identifier[:40],
    }
    container = owner._owner_container(configuration, identifier)
    container["command"] = ["/opt/venv/bin/python", "-m", "process.custom_import.retained_capture_copy_cli", identifier]
    container["volumeMounts"] = [
        {
            "name": "owner-storage",
            "mountPath": str(files.INPUT),
            "subPath": "custom-import-owner/" + files.directory_name(identifier),
            "readOnly": True,
        }
    ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": "hci-copy-" + identifier[:40],
            "namespace": configuration["namespace"],
            "labels": labels_by_key,
            "annotations": {
                "healthporta.com/worker-class": "process.CustomImportRetainedCaptureCopy",
                "healthporta.com/retained-copy": identifier,
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
                    "serviceAccountName": configuration["service_account"],
                    "nodeSelector": {"kubernetes.io/hostname": configuration["node_name"]},
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "imagePullSecrets": [{"name": configuration["image_pull_secret"]}],
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": control_workers._worker_job_pod_security_context(has_group_read_volume=False),
                    "containers": [container],
                    "volumes": [
                        {"name": "owner-storage", "persistentVolumeClaim": {"claimName": configuration["pvc"]}}
                    ],
                },
            },
        },
    }


async def _observed(signed, configuration):
    result = await owner._observed_job(manifest(signed, configuration), configuration, contract.identity(signed))
    if result is not None and any(type(count) is not int or count not in (0, 1) for count in result["status"].values()):
        custody.unavailable()
    return result


async def _is_producer_stopped(configuration, uid):
    selector = urllib.parse.urlencode({"labelSelector": "batch.kubernetes.io/controller-uid=" + uid})
    result = await owner._request("GET", "/api/v1/namespaces/" + configuration["namespace"] + "/pods?" + selector)
    pods = result.get("items")
    if type(pods) is not list or len(pods) > 1:
        custody.unavailable()
    for pod in pods:
        if pod.get("metadata", {}).get("namespace") != configuration["namespace"]:
            custody.unavailable()
        if not children._is_terminal_pod(pod, uid):
            return False
    return True


def _root(row, signed):
    if row.root_device is None or row.root_inode is None:
        custody.unavailable()
    if row.cleanup_started_at is None:
        retained, _bearer, observed = files.reopen(row.copy_id)
        if retained != signed or (observed["root_device"], observed["root_inode"]) != (row.root_device, row.root_inode):
            custody.unavailable()
    return {"root_device": row.root_device, "root_inode": row.root_inode}


async def observe(factory, signed, configuration):
    """Bind first actual UID and real terminal Pod evidence before reporting success."""
    current = await _observed(signed, configuration)
    uid = None if current is None else current["job_uid"]
    stopped = uid is not None and await _is_producer_stopped(configuration, uid)
    async with factory() as session, session.begin():
        copy_record = await service.locked_copy(session, signed)
        if copy_record is None or copy_record.root_device is None:
            if current is not None:
                custody.unavailable()
            return {"copy_id": contract.identity(signed), "status": "absent"}
        if uid is not None:
            if copy_record.launch_sent_at is None or copy_record.first_job_uid not in (None, uid):
                custody.unavailable()
            copy_record.first_job_uid = uid
            if stopped and (current["status"]["succeeded"] or current["status"]["failed"]):
                terminal = "succeeded" if current["status"]["succeeded"] else "failed"
                if copy_record.terminal_job_state not in (None, terminal):
                    custody.unavailable()
                copy_record.terminal_job_state = terminal
                copy_record.pods_stopped_at = copy_record.pods_stopped_at or await lifecycle._database_now(session)
        execution = await service.bound_execution(session, copy_record, signed)
        observation = service.snapshot(copy_record, execution.state)
        observation |= _root(copy_record, signed)
        observation["producer_namespace"] = configuration["namespace"]
        observation["status"] = "absent" if copy_record.launch_sent_at is not None else "launch_pending"
        if copy_record.first_job_uid is not None:
            observation["job_name"] = "hci-copy-" + copy_record.copy_id[:40]
            if current is not None:
                observation["status"] = current["status"]
            elif copy_record.terminal_job_state is not None and copy_record.pods_stopped_at is not None:
                observation["status"] = {
                    "active": 0,
                    "succeeded": int(copy_record.terminal_job_state == "succeeded"),
                    "failed": int(copy_record.terminal_job_state == "failed"),
                }
        else:
            observation.pop("job_uid")
        for name in ("terminal_job_state", "launch_sent", "cleaned", "cleanup_started"):
            observation.pop(name)
        return observation


async def launch(factory, signed, bearer, configuration):
    """Persist the sent marker before the sole create; lost replies only observe."""
    await owner._require_owner(factory, configuration)
    require_capacity(configuration)
    await service.reserve_copy(factory, signed, bearer, configuration)
    current = await _observed(signed, configuration)
    async with factory() as session, session.begin():
        copy_input = await native.load_source(session, signed["descriptor"], bearer)
        copy_record, _pending = await service.current_copy(session, signed, copy_input)
        if copy_record.cleaned_at is not None:
            custody.unavailable()
        if copy_record.launch_sent_at is not None:
            should_send = False
        else:
            if current is not None:
                custody.unavailable()
            root = files.stage(signed, bearer)
            if copy_record.root_device is not None and (copy_record.root_device, copy_record.root_inode) != (
                root["root_device"],
                root["root_inode"],
            ):
                custody.unavailable()
            copy_record.root_device, copy_record.root_inode = root["root_device"], root["root_inode"]
            now = await lifecycle._database_now(session)
            if now >= custody.timestamp(signed["descriptor"]["authorization_expires_at"]):
                custody.unavailable()
            copy_record.launch_sent_at = now
            should_send = True
    if should_send:
        try:
            await owner._request(
                "POST",
                "/apis/batch/v1/namespaces/" + configuration["namespace"] + "/jobs",
                manifest(signed, configuration),
            )
        except control_workers._KubernetesApiError as error:
            if error.status not in (0, 409):
                raise
    return await observe(factory, signed, configuration)


def _require_receipt(row, receipt, configuration):
    expected_by_field = {
        "copy_id": row.copy_id,
        "engine_execution_id": row.execution_id,
        "capture_bundle_id": row.capture_bundle_id,
        "producer_namespace": configuration["namespace"],
    }
    if row.root_device is not None:
        expected_by_field |= {"root_device": row.root_device, "root_inode": row.root_inode}
    if row.first_job_uid is not None:
        expected_by_field["job_uid"] = row.first_job_uid
    if (
        receipt == {"copy_id": row.copy_id, "status": "absent"}
        and row.launch_sent_at is None
        and row.first_job_uid is None
    ):
        return
    if type(receipt) is not dict or any(
        receipt.get(name) != expected_value for name, expected_value in expected_by_field.items()
    ):
        custody.unavailable()


async def stop(factory, signed, receipt, configuration, *, cleanup=False):
    """Require native stop authority and exact UID before deleting any Job or file."""
    await observe(factory, signed, configuration)
    async with factory() as session, session.begin():
        copy_record = await service.locked_copy(session, signed)
        if copy_record is None:
            return await _absent_terminal(session, signed, receipt, cleanup=cleanup)
        _require_receipt(copy_record, receipt, configuration)
        execution = await service.bound_execution(session, copy_record, signed)
        is_successful = (
            copy_record.phase == "sealed"
            and copy_record.terminal_job_state == "succeeded"
            and copy_record.pods_stopped_at is not None
        )
        if (
            execution is None
            or not is_successful
            and execution.state not in {"canceling", "canceled", "failed", "completed", "no_change"}
        ):
            custody.unavailable()
        uid, has_sent = copy_record.first_job_uid, copy_record.launch_sent_at is not None
    if has_sent:
        if uid is None:
            custody.unavailable()
        is_absent = await owner._is_stopped_job(
            manifest(signed, configuration),
            configuration,
            contract.identity(signed),
            expected_uid=uid,
            require_terminal=is_successful or cleanup,
        )
    else:
        is_absent = await _observed(signed, configuration) is None
    if not is_absent:
        return {"copy_id": contract.identity(signed), "pods_absent": False, "cleaned": False}
    await _record_stopped(factory, signed, receipt, configuration, uid, cleanup)
    return {"copy_id": contract.identity(signed), "pods_absent": True, "cleaned": cleanup}


def _request_pins(signed):
    descriptor = signed["descriptor"]
    return {
        "dataset_id": descriptor["dataset_id"],
        "definition_revision_id": descriptor["definition_revision_id"],
        "schema_revision_id": descriptor["schema_revision_id"],
        "source_binding_revision_id": descriptor["source_binding_revision_id"],
        "idempotency_key": descriptor["controller_execution_id"],
        "request_identity_sha256": bytes.fromhex(descriptor["request_identity_sha256"]),
    }


async def _absent_terminal(session, signed, receipt, *, cleanup):
    identifier = contract.identity(signed)
    if receipt != {"copy_id": identifier, "status": "absent"}:
        custody.unavailable()
    descriptor = signed["descriptor"]
    execution = await session.scalar(
        select(service.CustomImportExecution).where(
            service.CustomImportExecution.dataset_id == descriptor["dataset_id"],
            service.CustomImportExecution.definition_revision_id == descriptor["definition_revision_id"],
            service.CustomImportExecution.idempotency_key == descriptor["controller_execution_id"],
        )
    )
    if (
        execution is None
        or execution.state not in {"canceled", "failed", "completed", "no_change"}
        or (
            execution.schema_revision_id != descriptor["schema_revision_id"]
            or execution.source_binding_revision_id != descriptor["source_binding_revision_id"]
            or bytes(execution.request_identity_sha256) != bytes.fromhex(descriptor["request_identity_sha256"])
        )
    ):
        custody.unavailable()
    root = custody.OWNER_PARENT / files.directory_name(identifier)
    try:
        root.lstat()
    except FileNotFoundError:
        return {"copy_id": identifier, "pods_absent": True, "cleaned": cleanup}
    custody.unavailable()


def _pin_staged_root(row, signed):
    if row.root_device is not None:
        return
    root = custody.OWNER_PARENT / files.directory_name(row.copy_id)
    try:
        root.lstat()
    except FileNotFoundError:
        return
    retained, _bearer, metadata = files.reopen(row.copy_id)
    if retained != signed:
        custody.unavailable()
    row.root_device, row.root_inode = metadata["root_device"], metadata["root_inode"]


async def _record_stopped(factory, signed, receipt, configuration, uid, cleanup):
    """Commit native stopped finality and exact cleanup intent before unlinking inputs."""
    async with factory() as session, session.begin():
        copy_record = await service.locked_copy(session, signed)
        _require_receipt(copy_record, receipt, configuration)
        execution = await service.bound_execution(session, copy_record, signed)
        if copy_record.phase == "active" and execution.state == "canceling":
            await lifecycle.finalize_stopped_execution_request(session, **_request_pins(signed))
        if uid is not None:
            if copy_record.first_job_uid != uid:
                custody.unavailable()
            copy_record.pods_stopped_at = copy_record.pods_stopped_at or await lifecycle._database_now(session)
        if copy_record.phase == "active":
            copy_record.phase, copy_record.source_pin_active = "closed", False
        if cleanup and copy_record.cleaned_at is None:
            _pin_staged_root(copy_record, signed)
            if copy_record.root_device is not None:
                _root(copy_record, signed)
            copy_record.cleanup_started_at = (
                await lifecycle._database_now(session) if copy_record.root_device is not None else None
            )
    if cleanup:
        async with factory() as session, session.begin():
            copy_record = await service.locked_copy(session, signed)
            _require_receipt(copy_record, receipt, configuration)
            if copy_record.cleaned_at is None:
                if copy_record.root_device is not None:
                    files.cleanup(
                        signed, {"root_device": copy_record.root_device, "root_inode": copy_record.root_inode}
                    )
                copy_record.cleaned_at = await lifecycle._database_now(session)
