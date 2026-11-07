# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retired cleanup keeps exact Job custody, first UID and observation-only replies."""

import os
import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_execution_evidence as control
from api import custom_import_owner_attempt as children
from api import custom_import_owner_retirement as api
from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as attempt
from process.custom_import import family_shard_owner_attempt_files as attempts
from process.custom_import import family_shard_retirement_handoff as custody
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from tests.owner_attempt_support import recovery, server_job, stage_child


@pytest.fixture
def retired(tmp_path, monkeypatch):
    value = recovery(tmp_path, monkeypatch)
    root = value.root / "work/owner"
    root.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    lock.close()
    value.native = SimpleNamespace(sha256="e" * 64)
    monkeypatch.setattr(api, "profile", lambda: (value.profile, value.public, None))
    return value


def _document(value):
    return custody.stage(value.root, value.parent, value.native, value.profile)


def test_all_job_labels_obey_native_limits_and_preserve_annotations(retired):
    custody_case = retired
    document = _document(custody_case)
    manifests = [
        owner.manifest(custody_case.parent, custody_case.profile),
        children.manifest(custody_case.parent, custody_case.signed, custody_case.profile),
        api.manifest(custody_case.parent, document, custody_case.profile),
    ]
    identities = [
        handoff.identity(custody_case.parent),
        attempt.identity(custody_case.signed, custody_case.parent),
        custody.identity(document),
    ]
    identity_keys = ["owner-handoff", "owner-attempt", "owner-retirement"]
    for job, digest, identity_key in zip(manifests, identities, identity_keys, strict=True):
        for metadata in (job["metadata"], job["spec"]["template"]["metadata"]):
            for name, label in metadata["labels"].items():
                assert len(label) <= 63, name
                assert re.fullmatch(r"(?:[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?)?", label)
        assert job["metadata"]["labels"]["healthporta.com/" + identity_key] == digest[:40]
        assert job["metadata"]["annotations"]["healthporta.com/" + identity_key] == digest
        assert len(job["metadata"]["name"]) <= 63
    original = manifests[0]["spec"]["template"]["spec"]
    cleanup = manifests[2]["spec"]["template"]["spec"]
    expected = deepcopy(original)
    expected["containers"][0]["command"] = [
        "/opt/venv/bin/python",
        "-m",
        "process.custom_import.family_shard_retirement_cli",
        handoff.identity(custody_case.parent),
        custody.identity(document),
    ]
    assert cleanup == expected


async def test_missing_retirement_refuses_before_intent_or_create(retired, monkeypatch):
    monkeypatch.setattr(api, "read_retired_scope", AsyncMock(side_effect=ValueError("native not retired")))
    request = AsyncMock()
    monkeypatch.setattr(owner, "_request", request)
    with pytest.raises(ValueError):
        await api.ensure_files_cleaned(object(), retired.parent, retired.profile)
    request.assert_not_awaited()
    assert not (retired.root / custody.INTENT).exists()


async def test_active_or_unknown_predecessor_retains_work_without_cleanup_job(retired, monkeypatch):
    monkeypatch.setattr(api, "read_retired_scope", AsyncMock(return_value=retired.native))
    monkeypatch.setattr(owner, "is_stopped", AsyncMock(return_value=False))
    request = AsyncMock()
    monkeypatch.setattr(owner, "_request", request)
    assert not await api.ensure_files_cleaned(object(), retired.parent, retired.profile)
    request.assert_not_awaited()
    assert not (retired.root / custody.INTENT).exists()


async def test_lost_cleanup_create_reply_never_posts_again_and_pins_first_uid(retired, monkeypatch):
    value = retired
    monkeypatch.setattr(api, "read_retired_scope", AsyncMock(return_value=value.native))
    monkeypatch.setattr(api, "_is_owner_cleanup_ready", AsyncMock(return_value=True))
    request = AsyncMock(side_effect=owner.control_workers._KubernetesApiError(0, "synthetic reply lost"))
    monkeypatch.setattr(owner, "_request", request)
    observed = AsyncMock(return_value=None)
    monkeypatch.setattr(owner, "_observed_job", observed)
    assert not await api.ensure_files_cleaned(object(), value.parent, value.profile)
    assert request.await_count == 1 and request.await_args.args[0] == "POST"
    assert (value.root / custody.SENT).exists()
    request.reset_mock()
    assert not await api.ensure_files_cleaned(object(), value.parent, value.profile)
    request.assert_not_awaited()
    current_by_field = {"job_uid": "cleanup-first", "status": {"failed": 0, "succeeded": 0, "active": 1}}
    observed.return_value = current_by_field
    assert not await api.ensure_files_cleaned(object(), value.parent, value.profile)
    document = custody.read(value.root, custody.INTENT)
    assert custody.read(value.root, custody.JOB) == {
        "intent_sha256": custody.identity(document),
        "job_uid": "cleanup-first",
    }
    observed.return_value = current_by_field | {"job_uid": "replacement"}
    with pytest.raises(ValueError):
        await api.ensure_files_cleaned(object(), value.parent, value.profile)
    request.assert_not_awaited()


async def test_sent_child_without_first_uid_is_preserved_as_unknown(retired, monkeypatch):
    value = retired
    path = stage_child(value)
    attempts.is_marker_created(
        path, "launch.json", {"attempt_sha256": attempt.identity(value.signed, value.parent), "state": "sent"}
    )
    monkeypatch.setattr(owner, "is_stopped", AsyncMock(return_value=True))
    with pytest.raises(FileNotFoundError):
        await api._is_owner_cleanup_ready(value.parent, value.root, value.profile, value.public)
    assert path.exists()


async def test_successful_job_still_waits_for_exact_uid_pod_absence(retired, monkeypatch):
    value = retired
    document = _document(value)
    custody.is_marker_created(value.root, custody.SENT, {"intent_sha256": custody.identity(document), "state": "sent"})
    monkeypatch.setattr(
        owner,
        "_observed_job",
        AsyncMock(return_value={"job_uid": "cleanup-first", "status": {"failed": 0, "succeeded": 1}}),
    )
    stopped = AsyncMock(return_value=False)
    monkeypatch.setattr(owner, "_is_stopped_job", stopped)
    remove = AsyncMock()
    monkeypatch.setattr(api.asyncio, "to_thread", remove)
    assert not await api._is_cleanup_finished(value.parent, document, value.profile, value.root)
    remove.assert_not_awaited()
    assert stopped.await_args.kwargs["expected_uid"] == "cleanup-first"


async def test_absent_job_without_durable_success_cannot_consume_files(retired, monkeypatch):
    document = _document(retired)
    custody.is_marker_created(
        retired.root, custody.JOB, {"intent_sha256": custody.identity(document), "job_uid": "cleanup-first"}
    )
    monkeypatch.setattr(owner, "_observed_job", AsyncMock(return_value=None))
    stopped = AsyncMock()
    monkeypatch.setattr(owner, "_is_stopped_job", stopped)
    assert not await api._is_cleanup_finished(retired.parent, document, retired.profile, retired.root)
    stopped.assert_not_awaited()
    assert (retired.root / "work/owner").exists()


def _finished_inventory(retired, monkeypatch):
    from process.custom_import import family_shard_retirement_cli as cli
    from process.custom_import import family_shard_retirement_files as files
    from process.custom_import.family_shard_owner_cli import source_limits
    from process.custom_import.family_shard_retirement_policy import RetiredFilePolicy

    document = _document(retired)
    context = custody.context(document, work=retired.root / "work")
    lock = acquire_owner_job_lock(context.root, identity=context.root_identity[:2])
    try:
        files.seal_inventory(context, RetiredFilePolicy(context.root), lock.check)
    finally:
        lock.close()
    with monkeypatch.context() as scoped:
        scoped.setattr(handoff, "OWNER_PARENT", retired.root / "work")
        cli._consume(
            document, retired.native, source_limits(retired.profile["source_limits"]), retired.profile["work_mem_mb"]
        )
    return document


def test_completed_state_partial_unlink_retries_exact_remaining_files(retired, monkeypatch):
    document = _finished_inventory(retired, monkeypatch)
    original = os.unlink

    def interrupted(name, *args, **kwargs):
        if name == "sealed":
            raise OSError("synthetic state cleanup interruption")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", interrupted)
    with pytest.raises(OSError):
        api._remove_completed(retired.root, document)
    assert (retired.root / custody.COMPLETE).exists()
    assert not (retired.root / "work/retirement/inventory.mpk").exists()
    monkeypatch.setattr(os, "unlink", original)
    api._remove_completed(retired.root, document)
    assert not list((retired.root / "work").iterdir())


def test_completed_state_refuses_extra_files_before_any_unlink(retired, monkeypatch):
    document = _finished_inventory(retired, monkeypatch)
    state = retired.root / "work/retirement"
    (state / "unknown").write_bytes(b"retain")
    with pytest.raises(ValueError):
        api._remove_completed(retired.root, document)
    assert (state / "inventory.mpk").exists() and (state / "unknown").exists()


async def test_actual_cleanup_observation_refuses_authored_spec_drift(retired, monkeypatch):
    value = retired
    document = _document(value)
    job = server_job(api.manifest(value.parent, document, value.profile), "cleanup-first")
    job["spec"]["template"]["spec"]["containers"][0]["envFrom"][1]["secretRef"]["name"] = "foreign-secret"
    monkeypatch.setattr(owner, "_request", AsyncMock(return_value=job))
    with pytest.raises(ValueError):
        await owner._observed_job(
            api.manifest(value.parent, document, value.profile), value.profile, handoff.identity(value.parent)
        )


async def test_existing_original_cleanup_reports_pending_until_consumer_finishes(retired, monkeypatch):
    value = retired
    monkeypatch.setattr(api, "terminal_state", AsyncMock(return_value="retired"))
    monkeypatch.setattr(owner, "is_stopped", AsyncMock(return_value=True))
    consumer = AsyncMock(return_value=False)
    monkeypatch.setattr(api, "ensure_files_cleaned", consumer)
    cleanups = []
    monkeypatch.setattr(owner, "cleanup", lambda *_args: cleanups.append(True))
    receipt_by_field = {"handoff_id": handoff.identity(value.parent), "job_uid": "synthetic-first"}
    response = await control._signed_owner_handoff(
        object(), {"signed": value.parent, "receipt": receipt_by_field}, value.profile, value.public, "cleanup"
    )
    assert response.status == 200 and b'"cleaned":false' in response.body
    assert not cleanups
    consumer.return_value = True
    response = await control._signed_owner_handoff(
        object(), {"signed": value.parent, "receipt": receipt_by_field}, value.profile, value.public, "cleanup"
    )
    assert response.status == 200 and b'"cleaned":true' in response.body
    assert cleanups == [True]


async def test_child_cleanup_uses_same_consumer_before_deleting_inputs(retired, monkeypatch):
    value = retired
    path = stage_child(value)
    digest = attempt.identity(value.signed, value.parent)
    attempts.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": "child-first"})
    monkeypatch.setattr(api, "terminal_state", AsyncMock(return_value="retired"))
    monkeypatch.setattr(owner, "_is_stopped_job", AsyncMock(return_value=True))
    monkeypatch.setattr(api, "ensure_files_cleaned", AsyncMock(return_value=False))
    receipt_by_field = {
        "attempt_sha256": digest,
        "handoff_id": handoff.identity(value.parent),
        "root_device": value.root.stat().st_dev,
        "root_inode": value.root.stat().st_ino,
        "job_uid": "child-first",
    }
    result = await children._terminal_operation(
        object(), value.parent, value.signed, receipt_by_field, "cleanup", value.profile
    )
    assert result == {"attempt_sha256": digest, "pods_absent": True, "cleaned": False}
    assert path.exists() and os.listdir(value.root / "work") == ["owner"]


@pytest.mark.parametrize("gate", ["not_retired", "owner_present", "retired_stopped"])
async def test_empty_producer_state_still_requires_actual_retirement_and_stopped_owner(retired, monkeypatch, gate):
    value = retired
    root = value.root / "work/owner"
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    lock.remove_terminal_root()
    state = value.root / "work/retirement"
    state.mkdir(mode=0o700)
    observed = AsyncMock(return_value=value.native)
    if gate == "not_retired":
        observed.side_effect = ValueError("native not retired")
    monkeypatch.setattr(api, "read_retired_scope", observed)
    stopped = AsyncMock(return_value=gate == "retired_stopped")
    monkeypatch.setattr(api, "_is_owner_cleanup_ready", stopped)
    request = AsyncMock()
    monkeypatch.setattr(owner, "_request", request)
    if gate == "not_retired":
        with pytest.raises(ValueError):
            await api.ensure_files_cleaned(object(), value.parent, value.profile)
        stopped.assert_not_awaited()
    else:
        assert await api.ensure_files_cleaned(object(), value.parent, value.profile) == (gate == "retired_stopped")
    assert state.exists() == (gate != "retired_stopped")
    request.assert_not_awaited()
