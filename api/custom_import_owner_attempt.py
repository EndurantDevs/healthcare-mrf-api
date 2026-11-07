# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit owner child Jobs after exact predecessor termination and native proof."""

import base64
import os
import time
import urllib.parse

from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as contract
from process.custom_import import family_shard_owner_attempt_files as files
from process.custom_import.family_shard_owner_cli import profile


def manifest(parent, signed, configuration):
    """Keep the original pinned Pod shape, adding only its signed attempt locator."""
    descriptor = contract.checked_descriptor(signed["descriptor"], parent)
    digest = contract.identity(signed, parent)
    value = owner.manifest(parent, configuration)
    value["metadata"]["name"] = "hci-owner-" + digest[:40]
    value["metadata"]["annotations"]["healthporta.com/owner-attempt"] = digest
    value["metadata"]["labels"]["healthporta.com/owner-attempt"] = digest[:40]
    value["spec"]["template"]["metadata"]["labels"]["healthporta.com/owner-attempt"] = digest[:40]
    worker = value["spec"]["template"]["spec"]["containers"][0]
    worker["command"] += ["--attempt-sha256", digest]
    if descriptor["owner_image"] != worker["image"]:
        handoff.unavailable()
    return value


async def observed(parent, signed, configuration):
    """Return exact Job/root custody after complete authored-spec comparison."""
    descriptor = contract.checked_descriptor(signed["descriptor"], parent)
    root, _bearer = files.parent_root(parent, descriptor)
    current = await owner._observed_job(
        manifest(parent, signed, configuration), configuration, descriptor["owner_handoff_id"]
    )
    if current is None:
        return None
    return current | {
        "attempt_sha256": contract.identity(signed, parent),
        "root_device": root.stat().st_dev,
        "root_inode": root.stat().st_ino,
    }


def _token(parent, descriptor, root, encoded):
    _root, original = files.parent_root(parent, descriptor, root=root)
    authorization = descriptor["authorization"]
    if descriptor["kind"] == "continue":
        if type(encoded) is not str or len(encoded) != 43:
            handoff.unavailable()
        try:
            token = base64.b64decode(encoded + "=", altchars=b"-_", validate=True)
        except ValueError:
            handoff.unavailable()
        if len(token) != 32 or base64.urlsafe_b64encode(token).decode("ascii").rstrip("=") != encoded:
            handoff.unavailable()
        return token
    if encoded is not None:
        handoff.unavailable()
    if authorization is None:
        return original
    if descriptor["previous_attempt_sha256"] is None:
        handoff.unavailable()
    previous, token = files.reopen(parent, descriptor["previous_attempt_sha256"], root=root)
    if previous["descriptor"]["authorization"] != authorization:
        handoff.unavailable()
    return token


async def _native(factory, parent, descriptor, token, configuration):
    from process.custom_import.family_shard_production import (
        original_request,
        owner_continuation_request,
        require_native,
    )

    await owner._require_owner(factory, configuration)
    if descriptor["authorization"] is None:
        original = await original_request(factory, parent["descriptor"], token)
        require_native(parent["descriptor"], original.native)
    else:
        await owner_continuation_request(
            factory,
            parent["descriptor"],
            descriptor["authorization"],
            token,
            start=descriptor["kind"] == "continue",
            inspect_only=True,
        )


def _is_terminal_pod(pod, uid):
    metadata = pod.get("metadata", {})
    references = metadata.get("ownerReferences", [])
    if not any(
        reference.get("uid") == uid and reference.get("controller") is True and reference.get("kind") == "Job"
        for reference in references
    ):
        handoff.unavailable()
    state, spec = pod.get("status", {}), pod.get("spec", {})
    if state.get("phase") not in {"Failed", "Succeeded"}:
        return False
    for containers, statuses in (
        ("containers", "containerStatuses"),
        ("initContainers", "initContainerStatuses"),
        ("ephemeralContainers", "ephemeralContainerStatuses"),
    ):
        names = {container["name"] for container in spec.get(containers, [])}
        actual = state.get(statuses, [])
        if (
            len(actual) != len(names)
            or {container.get("name") for container in actual} != names
            or any("terminated" not in container.get("state", {}) for container in actual)
        ):
            return False
    return True


async def predecessor_stopped(parent, descriptor, configuration, root):
    """Prove the actual prior UID and every container have stopped without deletion."""
    previous_sha = descriptor["previous_attempt_sha256"]
    if previous_sha is None:
        expected = owner.manifest(parent, configuration)
        directory = root
    else:
        previous, _nonce = files.reopen(parent, previous_sha, root=root)
        contract.verify(previous, parent, verifying_key=profile()[1])
        if previous["descriptor"]["attempt"] != descriptor["attempt"] - 1:
            handoff.unavailable()
        expected = manifest(parent, previous, configuration)
        directory = root / "attempts" / previous_sha
    fd = handoff._directory(directory)
    try:
        known = handoff.document(handoff.read_file(fd, "job.json", 512))
        if known.get("job_uid") != descriptor["previous_job_uid"]:
            handoff.unavailable()
    finally:
        os.close(fd)
    current = await owner._observed_job(expected, configuration, descriptor["owner_handoff_id"])
    if current is not None and (
        current["job_uid"] != descriptor["previous_job_uid"]
        or current["status"]["active"]
        or not (current["status"]["succeeded"] or current["status"]["failed"])
    ):
        return False
    selector = urllib.parse.urlencode(
        {"labelSelector": "batch.kubernetes.io/controller-uid=" + descriptor["previous_job_uid"]}
    )
    response = await owner._request("GET", "/api/v1/namespaces/" + configuration["namespace"] + "/pods?" + selector)
    pods = response.get("items")
    if type(pods) is not list or len(pods) > 1:
        handoff.unavailable()
    return all(_is_terminal_pod(pod, descriptor["previous_job_uid"]) for pod in pods)


async def launch(parent, signed, encoded_nonce, factory):
    """Send once after stopped predecessor, native authority and protected commit."""
    configuration, key, _limits = profile()
    descriptor = contract.verify(signed, parent, verifying_key=key, now=time.time())
    owner.require_profile(parent, configuration)
    root, _bearer = files.parent_root(parent, descriptor)
    lock = files.attempt_lock(root)
    try:
        digest = contract.identity(signed, parent)
        current = await observed(parent, signed, configuration)
        if current is not None:
            stored, token = files.reopen(parent, digest, root=root)
            if stored != signed:
                handoff.unavailable()
            contract.verify(stored, parent, verifying_key=key, token=token)
            files.is_marker_created(
                root / "attempts" / digest, "job.json", {"attempt_sha256": digest, "job_uid": current["job_uid"]}
            )
            return current
        path = root / "attempts" / digest
        if path.exists() and (path / "launch.json").exists():
            return {"attempt_sha256": digest, "status": "launch_pending"}
        if not await predecessor_stopped(parent, descriptor, configuration, root):
            handoff.unavailable()
        token = _token(parent, descriptor, root, encoded_nonce)
        contract.verify(signed, parent, verifying_key=key, now=time.time(), token=token)
        await _native(factory, parent, descriptor, token, configuration)
        owner.require_capacity(configuration)
        path = files.stage(parent, signed, token, root=root)
        if not files.is_marker_created(path, "launch.json", {"attempt_sha256": digest, "state": "sent"}):
            return {"attempt_sha256": digest, "status": "launch_pending"}
        contract.verify(signed, parent, verifying_key=key, now=time.time(), token=token)
        try:
            await owner._request(
                "POST",
                "/apis/batch/v1/namespaces/" + configuration["namespace"] + "/jobs",
                manifest(parent, signed, configuration),
            )
        except owner.control_workers._KubernetesApiError as error:
            if error.status not in {0, 409}:
                raise
        current = await observed(parent, signed, configuration)
        if current is None:
            return {"attempt_sha256": digest, "status": "launch_pending"}
        files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": current["job_uid"]})
        return current
    finally:
        lock.close()


async def observe(parent, signed):
    """Adopt only the exact late Job UID; absence never permits another send."""
    configuration, key, _limits = profile()
    descriptor = contract.verify(signed, parent, verifying_key=key)
    root, _bearer = files.parent_root(parent, descriptor)
    lock = files.attempt_lock(root)
    try:
        digest = contract.identity(signed, parent)
        current = await observed(parent, signed, configuration)
        if current is not None:
            path = root / "attempts" / digest
            stored, _token_bytes = files.reopen(parent, digest, root=root)
            if stored != signed:
                handoff.unavailable()
            files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": current["job_uid"]})
            return current
        return {"attempt_sha256": digest, "status": "launch_pending"}
    finally:
        lock.close()


def is_cleaned_files(parent, signed, receipt):
    """Remove known child inputs only when all original work is already gone."""
    root, _bearer = files.parent_root(parent, signed["descriptor"])
    work = handoff._directory(root / "work")
    try:
        if os.listdir(work):
            return False
    finally:
        os.close(work)
    lock = files.attempt_lock(root)
    try:
        digest = contract.identity(signed, parent)
        path = root / "attempts" / digest
        if path.exists():
            actual, _nonce = files.reopen(parent, digest, root=root)
            if actual != signed:
                handoff.unavailable()
            fd = handoff._directory(path)
            try:
                names = set(os.listdir(fd))
                if names != {"delegation.json", "nonce", "committed", "launch.json", "job.json"}:
                    handoff.unavailable()
                if handoff.document(handoff.read_file(fd, "job.json", 512)) != {
                    "attempt_sha256": digest,
                    "job_uid": receipt["job_uid"],
                }:
                    handoff.unavailable()
                for name in names:
                    handoff.read_file(fd, name, handoff.MAX_DOCUMENT_BYTES)
                for name in names:
                    os.unlink(name, dir_fd=fd)
                os.fsync(fd)
            finally:
                os.close(fd)
            path.rmdir()
        if {entry.name for entry in (root / "attempts").iterdir()} == {files._NAME}:
            lock.remove_terminal_root()
        return True
    finally:
        lock.close()


async def _terminal_operation(factory, parent, signed, receipt, operation, configuration):
    """Require native finality and exact stopped child custody before input cleanup."""
    from api import custom_import_owner_retirement as retired

    descriptor = signed["descriptor"]
    root, _bearer = files.parent_root(parent, descriptor)
    digest = contract.identity(signed, parent)
    state = await retired.terminal_state(factory, parent, configuration)
    expected_by_field = {
        "attempt_sha256": digest,
        "handoff_id": descriptor["owner_handoff_id"],
        "root_device": descriptor["root_device"],
        "root_inode": descriptor["root_inode"],
    }
    if (
        state not in {"canceling", "canceled", "failed", "completed", "no_change", "retired"}
        or type(receipt) is not dict
        or (
            any(receipt.get(n) != expected_value for n, expected_value in expected_by_field.items())
            or type(receipt.get("job_uid")) is not str
            or handoff._OPAQUE.fullmatch(receipt["job_uid"]) is None
        )
    ):
        handoff.unavailable()
    path = root / "attempts" / digest
    if path.exists():
        fd = handoff._directory(path)
        try:
            if handoff.document(handoff.read_file(fd, "job.json", 512)) != {
                "attempt_sha256": digest,
                "job_uid": receipt["job_uid"],
            }:
                handoff.unavailable()
        finally:
            os.close(fd)
    absent = await owner._is_stopped_job(
        manifest(parent, signed, configuration),
        configuration,
        descriptor["owner_handoff_id"],
        expected_uid=receipt["job_uid"],
        require_terminal=operation == "cleanup",
    )
    cleaned = (
        operation == "cleanup"
        and absent
        and state != "canceling"
        and await retired.ensure_files_cleaned(factory, parent, configuration)
        and is_cleaned_files(parent, signed, receipt)
    )
    return {"attempt_sha256": digest, "pods_absent": absent, "cleaned": bool(cleaned)}


async def serve(request, factory):
    """Keep one closed controller route separate from every hosted batch permit."""
    from api.control_execution_evidence import _error, _is_authorized, _reply

    if not _is_authorized(getattr(request, "headers", None)):
        return _error("forbidden", 403)
    try:
        if getattr(request, "query_string", ""):
            handoff.unavailable()
        body = handoff.document(request.body)
        operation = body.get("operation") if type(body) is dict else None
        if operation not in {"check", "launch", "observe", "stop", "cleanup"}:
            handoff.unavailable()
        wanted = {"operation", "signed", "attempt"} | (
            {"nonce"} if operation == "launch" else {"receipt"} if operation in {"stop", "cleanup"} else set()
        )
        if body.keys() != wanted:
            handoff.unavailable()
        configuration, key, _limits = profile()
        parent, signed = body["signed"], body["attempt"]
        descriptor = contract.verify(
            signed, parent, verifying_key=key, now=time.time() if operation in {"check", "launch"} else None
        )
        owner.require_profile(parent, configuration)
        if operation == "launch":
            return _reply(await launch(parent, signed, body["nonce"], factory))
        if operation == "observe":
            return _reply(await observe(parent, signed))
        root, _bearer = files.parent_root(parent, descriptor)
        digest = contract.identity(signed, parent)
        if operation == "check":
            lock = files.attempt_lock(root)
            try:
                return _reply(
                    {
                        "attempt_sha256": digest,
                        "predecessor_stopped": await predecessor_stopped(parent, descriptor, configuration, root),
                    }
                )
            finally:
                lock.close()
        return _reply(await _terminal_operation(factory, parent, signed, body["receipt"], operation, configuration))
    except Exception:
        return _error("owner_attempt_unavailable", 503)
