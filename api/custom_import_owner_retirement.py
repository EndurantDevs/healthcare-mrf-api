# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One exact terminal cleanup Job after permanent native retirement and Pod absence."""

import asyncio
import os

from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as attempt
from process.custom_import import family_shard_owner_attempt_files as attempts
from process.custom_import import family_shard_retirement_files as inventory
from process.custom_import import family_shard_retirement_handoff as custody
from process.custom_import import family_shard_terminal_files as producer
from process.custom_import.family_shard_owner_cli import profile
from process.custom_import.family_shard_retirement_native import read_retired_scope


def manifest(parent, document, configuration):
    """Preserve the original profile, credentials and absolute mounts byte-for-byte."""
    digest = custody.identity(document)
    if document["handoff_id"] != handoff.identity(parent):
        handoff.unavailable()
    value = owner.manifest(parent, configuration, with_reader=False)
    value["metadata"]["name"] = "hci-clean-" + digest[:40]
    value["metadata"]["annotations"]["healthporta.com/owner-retirement"] = digest
    value["metadata"]["labels"]["healthporta.com/owner-retirement"] = digest[:40]
    value["spec"]["template"]["metadata"]["labels"]["healthporta.com/owner-retirement"] = digest[:40]
    value["spec"]["template"]["metadata"]["annotations"] = {"healthporta.com/owner-retirement": digest}
    value["spec"]["template"]["spec"]["containers"][0]["command"] = [
        "/opt/venv/bin/python",
        "-m",
        "process.custom_import.family_shard_retirement_cli",
        handoff.identity(parent),
        digest,
    ]
    return value


async def terminal_state(factory, parent, configuration):
    """A retired original epoch permits terminal cleanup only, never another writer."""
    from process.custom_import.family_shard_production import retained_state

    try:
        return await retained_state(factory, parent["descriptor"])
    except Exception:
        await read_retired_scope(factory, parent, configuration)
        return "retired"


def _known_job(root, name):
    value = custody.read(root, name)
    uid = value.get("job_uid") if type(value) is dict else None
    if type(uid) is not str or handoff._OPAQUE.fullmatch(uid) is None:
        handoff.unavailable()
    return value


async def _is_owner_cleanup_ready(parent, root, configuration, key):
    """Every actually sent owner retains its first recorded UID until Pod absence."""
    from api import custom_import_owner_attempt as children

    known = _known_job(root, "job.json")
    if known != {"handoff_id": handoff.identity(parent), "job_uid": known["job_uid"]}:
        handoff.unavailable()
    if not await owner.is_stopped(parent, configuration, expected_uid=known["job_uid"], require_terminal=True):
        return False
    if not (root / "attempts").exists():
        return True
    lock = attempts.attempt_lock(root)
    try:
        directory = handoff._directory(root / "attempts")
        try:
            names = set(os.listdir(directory)) - {attempts._NAME}
        finally:
            os.close(directory)
        if len(names) > 32:
            handoff.unavailable()
        for digest in sorted(names):
            child, token = attempts.reopen(parent, digest, root=root)
            attempt.verify(child, parent, verifying_key=key, token=token)
            child_root = root / "attempts" / digest
            sent = custody.read(child_root, "launch.json", optional=True)
            if sent is None:
                continue
            if sent != {"attempt_sha256": digest, "state": "sent"}:
                handoff.unavailable()
            known = _known_job(child_root, "job.json")
            if known != {"attempt_sha256": digest, "job_uid": known["job_uid"]}:
                handoff.unavailable()
            if not await owner._is_stopped_job(
                children.manifest(parent, child, configuration),
                configuration,
                handoff.identity(parent),
                expected_uid=known["job_uid"],
                require_terminal=True,
            ):
                return False
        return True
    finally:
        lock.close()


def _is_work_empty(root):
    directory = handoff._directory(root / "work")
    try:
        return not os.listdir(directory)
    finally:
        os.close(directory)


def _remove_completed(root, document):
    """Consume only the successful exact Job's fixed, authenticated state files."""
    work, state = root / "work", root / "work" / custody.STATE
    inventory._directory(state, tuple(document["state_identity"]))
    directory = handoff._directory(state)
    try:
        names = {"inventory.mpk", "sealed", "drained", "done", custody.COMPLETION}
        actual_names = set(os.listdir(directory))
        if actual_names - names or set(os.listdir(work)) != {custody.STATE}:
            handoff.unavailable()
        completion = custody.read(root, custody.COMPLETE, optional=True)
        if completion is None:
            if actual_names != names:
                handoff.unavailable()
            completion = handoff.document(inventory._read_file(directory, custody.COMPLETION, maximum=2048)[2])
        if type(completion) is not dict or completion.keys() != {"intent_sha256", "native_sha256", "inventory_sha256"}:
            handoff.unavailable()
        if (
            completion["intent_sha256"] != custody.identity(document)
            or completion["native_sha256"] != document["native_sha256"]
        ):
            handoff.unavailable()
        digest = bytes.fromhex(completion["inventory_sha256"])
        if len(digest) != 32 or completion["inventory_sha256"] != digest.hex():
            handoff.unavailable()
        for name in actual_names & {"sealed", "drained", "done"}:
            if inventory._read_file(directory, name, maximum=32)[2] != digest:
                handoff.unavailable()
        if (
            "inventory.mpk" in actual_names
            and inventory._read_file(directory, "inventory.mpk", maximum=inventory.MAX_INVENTORY_BYTES)[1] != digest
        ):
            handoff.unavailable()
        if (
            custody.COMPLETION in actual_names
            and handoff.document(inventory._read_file(directory, custody.COMPLETION, maximum=2048)[2]) != completion
        ):
            handoff.unavailable()
        custody.is_marker_created(root, custody.COMPLETE, completion)
        for name in ("inventory.mpk", "sealed", "drained", "done", custody.COMPLETION):
            if name in actual_names:
                os.unlink(name, dir_fd=directory)
        os.fsync(directory)
    finally:
        os.close(directory)
    state.rmdir()


async def _is_cleanup_finished(parent, document, configuration, root):
    expected = manifest(parent, document, configuration)
    digest = custody.identity(document)
    known = custody.read(root, custody.JOB, optional=True)
    observed = await owner._observed_job(expected, configuration, handoff.identity(parent))
    if observed is not None:
        receipt_by_field = {"intent_sha256": digest, "job_uid": observed["job_uid"]}
        custody.is_marker_created(root, custody.JOB, receipt_by_field)
        known = receipt_by_field
        if observed["status"]["failed"] or not observed["status"]["succeeded"]:
            return False
        custody.is_marker_created(root, custody.SUCCEEDED, receipt_by_field)
    if known is None:
        return False
    if (
        known.keys() != {"intent_sha256", "job_uid"}
        or known["intent_sha256"] != digest
        or (type(known["job_uid"]) is not str or handoff._OPAQUE.fullmatch(known["job_uid"]) is None)
    ):
        handoff.unavailable()
    if custody.read(root, custody.SUCCEEDED, optional=True) != known:
        return False
    if not await owner._is_stopped_job(
        expected, configuration, handoff.identity(parent), expected_uid=known["job_uid"], require_terminal=True
    ):
        return False
    await asyncio.to_thread(_remove_completed, root, document)
    return True


async def ensure_files_cleaned(factory, parent, configuration):
    """Existing cleanup stays pending until this separately trusted consumer finishes."""
    identifier = handoff.identity(parent)
    stored, _bearer = handoff.reopen(identifier, parent=handoff.OWNER_PARENT)
    if stored != parent:
        handoff.unavailable()
    root = handoff.OWNER_PARENT / identifier
    if _is_work_empty(root):
        return True
    native = await read_retired_scope(factory, parent, configuration)
    if not await _is_owner_cleanup_ready(parent, root, configuration, profile()[1]):
        return False
    if await asyncio.to_thread(producer.is_empty_residue_removed, root):
        return True
    document = custody.stage(root, parent, native, configuration)
    digest = custody.identity(document)
    if custody.is_marker_created(root, custody.SENT, {"intent_sha256": digest, "state": "sent"}):
        try:
            await owner._request(
                "POST",
                "/apis/batch/v1/namespaces/" + configuration["namespace"] + "/jobs",
                manifest(parent, document, configuration),
            )
        except owner.control_workers._KubernetesApiError as error:
            if error.status not in {0, 409}:
                raise
    return await _is_cleanup_finished(parent, document, configuration, root)
