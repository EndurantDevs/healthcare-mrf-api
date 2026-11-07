# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit attempts preserve original identity and reject ambiguous custody."""

import base64
import hashlib
import os
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api import custom_import_owner_attempt as api
from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_attempt as contract
from process.custom_import import family_shard_owner_attempt_files as files
from tests.owner_attempt_support import recovery, server_job, signed_attempt, stage_child
from tests.test_custom_import_build_continuation import _test_authorization


@pytest.fixture
def recovered(tmp_path, monkeypatch):
    return recovery(tmp_path, monkeypatch)


def test_fresh_child_signature_preserves_parent_and_has_separate_domain(recovered):
    value = recovered
    assert (
        contract.verify(value.signed, value.parent, verifying_key=value.public, now=value.now, token=b"x" * 43)
        == value.descriptor
    )
    assert contract.identity(value.signed, value.parent) != handoff.identity(value.parent)
    wrong_domain = deepcopy(value.signed)
    wrong_domain["signature"] = (
        base64.urlsafe_b64encode(value.key.sign(handoff.DOMAIN + handoff.canonical(value.descriptor)))
        .decode("ascii")
        .rstrip("=")
    )
    with pytest.raises(ValueError):
        contract.verify(wrong_domain, value.parent, verifying_key=value.public)


@pytest.mark.parametrize(
    "change",
    [
        {"attempt": True},
        {"attempt": 33},
        {"previous_attempt_sha256": "f" * 64},
        {"root_inode": 0},
        {"owner_image": "registry.example.test/engine:latest"},
        {"owner_handoff_id": "f" * 64},
        {"extra": None},
        {"authorization": {}},
    ],
)
def test_signed_changes_cannot_leave_closed_original_scope(recovered, change):
    value = recovered
    signed = signed_attempt(value.key, value.descriptor | change)
    with pytest.raises((ValueError, TypeError)):
        contract.verify(signed, value.parent, verifying_key=value.public, now=value.now)


@pytest.mark.parametrize("now,token", [(None, b"y" * 43), (float("nan"), None), (0, None), (float("inf"), None)])
def test_wrong_nonce_or_invalid_launch_clock_is_refused(recovered, now, token):
    with pytest.raises(ValueError):
        contract.verify(recovered.signed, recovered.parent, verifying_key=recovered.public, now=now, token=token)


def test_continued_child_binds_original_producer_and_current_nonce(recovered):
    import datetime as dt

    value, nonce = recovered, b"n" * 32
    authorization = _test_authorization(
        hashlib.sha256(b"x" * 43).digest(), nonce, dt.datetime.fromtimestamp(value.now, dt.UTC)
    )
    original = value.parent["descriptor"]
    authorization |= {
        "engine_execution_id": str(original["engine_execution_id"]),
        "capture_bundle_id": original["capture_bundle_id"],
        "producer_fence": original["fence"],
        "request_identity_sha256": original["request_identity_sha256"],
    }
    signed = signed_attempt(value.key, value.descriptor | {"kind": "continue", "authorization": authorization})
    contract.verify(signed, value.parent, verifying_key=value.public, now=value.now, token=nonce)
    with pytest.raises(ValueError):
        contract.verify(signed, value.parent, verifying_key=value.public, token=b"x" * 43)
    changed = signed_attempt(value.key, signed["descriptor"] | {"authorization": authorization | {"producer_fence": 8}})
    with pytest.raises(ValueError):
        contract.verify(changed, value.parent, verifying_key=value.public)


def test_protected_stage_reopens_exact_bytes_and_refuses_rebinding_or_unknown_state(recovered):
    value = recovered
    path = stage_child(value)
    digest = contract.identity(value.signed, value.parent)
    assert files.reopen(value.parent, digest, root=value.root) == (value.signed, b"x" * 43)
    assert (path / "nonce").stat().st_mode & 0o777 == 0o400
    assert files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": "synthetic-child"})
    assert not files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": "synthetic-child"})
    with pytest.raises(ValueError):
        files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": "foreign"})
    (path / "unknown").touch()
    with pytest.raises(ValueError):
        files.reopen(value.parent, digest, root=value.root)
    assert (path / "nonce").read_bytes() == b"x" * 43


def test_original_root_inode_and_concurrent_launch_lock_are_preserved(recovered):
    value = recovered
    with pytest.raises(ValueError):
        files.parent_root(
            value.parent, value.descriptor | {"root_inode": value.descriptor["root_inode"] + 1}, root=value.root
        )
    lock = files.attempt_lock(value.root)
    try:
        with pytest.raises((ValueError, BlockingIOError)):
            files.attempt_lock(value.root)
    finally:
        lock.close()


async def test_child_observation_checks_complete_spec_and_first_uid(recovered, monkeypatch):
    value = recovered
    stage_child(value)
    expected = api.manifest(value.parent, value.signed, value.profile)
    job = server_job(expected, "synthetic-child")
    monkeypatch.setattr(owner, "_request", AsyncMock(return_value=job))
    receipt = await api.observed(value.parent, value.signed, value.profile)
    assert receipt["job_uid"] == "synthetic-child"
    assert receipt["job_name"] == expected["metadata"]["name"]
    assert receipt["handoff_id"] == handoff.identity(value.parent)
    assert receipt["root_inode"] == value.descriptor["root_inode"]
    job["spec"]["template"]["spec"]["containers"][0]["command"].remove("--attempt-sha256")
    with pytest.raises(ValueError):
        await api.observed(value.parent, value.signed, value.profile)


@pytest.mark.parametrize("status", [{"active": 1}, {}, {"succeeded": 1, "active": 1}])
async def test_predecessor_must_be_terminal_before_any_native_or_create(recovered, monkeypatch, status):
    value = recovered
    job = server_job(owner.manifest(value.parent, value.profile), "synthetic-first")
    job["status"] = status
    monkeypatch.setattr(owner, "_request", AsyncMock(return_value=job))
    assert not await api.predecessor_stopped(value.parent, value.descriptor, value.profile, value.root)


@pytest.mark.parametrize(
    "pod",
    [
        {
            "metadata": {"ownerReferences": [{"kind": "Job", "uid": "synthetic-first", "controller": True}]},
            "spec": {"containers": [{"name": "owner"}]},
            "status": {"phase": "Running"},
        },
        {
            "metadata": {"ownerReferences": [{"kind": "Job", "uid": "synthetic-first", "controller": True}]},
            "spec": {"containers": [{"name": "owner"}]},
            "status": {"phase": "Failed", "containerStatuses": []},
        },
    ],
)
async def test_terminal_job_with_unstopped_or_unproved_pod_is_not_recoverable(recovered, monkeypatch, pod):
    value = recovered
    job = server_job(owner.manifest(value.parent, value.profile), "synthetic-first")
    monkeypatch.setattr(owner, "_request", AsyncMock(side_effect=[job, {"items": [pod]}]))
    assert not await api.predecessor_stopped(value.parent, value.descriptor, value.profile, value.root)


async def test_lost_create_reply_never_posts_again_and_exact_observation_adopts_uid(recovered, monkeypatch):
    custody = recovered
    original = server_job(owner.manifest(custody.parent, custody.profile), "synthetic-first")
    child = server_job(api.manifest(custody.parent, custody.signed, custody.profile), "synthetic-child")
    state_by_field = {"child": None, "posts": 0}

    async def request(method, path, body=None):
        if method == "POST":
            state_by_field["posts"] += 1
            assert body == api.manifest(custody.parent, custody.signed, custody.profile)
            raise owner.control_workers._KubernetesApiError(0, "reply unavailable")
        assert method == "GET"
        if "/pods?" in path:
            return {"items": []}
        if path.endswith(original["metadata"]["name"]):
            return original
        if state_by_field["child"] is None:
            raise owner.control_workers._KubernetesApiError(404, "absent")
        return state_by_field["child"]

    native = AsyncMock()
    monkeypatch.setattr(owner, "_request", request)
    monkeypatch.setattr(api, "_native", native)
    monkeypatch.setattr(owner, "require_capacity", lambda _configuration: None)
    first = await api.launch(custody.parent, custody.signed, None, object())
    assert first["status"] == "launch_pending" and state_by_field["posts"] == 1
    assert (await api.launch(custody.parent, custody.signed, None, object()))["status"] == "launch_pending"
    assert state_by_field["posts"] == 1
    native.assert_awaited_once()
    state_by_field["child"] = child
    receipt = await api.observe(custody.parent, custody.signed)
    assert receipt["job_uid"] == "synthetic-child" and state_by_field["posts"] == 1
    assert (await api.launch(custody.parent, custody.signed, None, object()))["job_uid"] == "synthetic-child"
    fd = handoff._directory(custody.root / "attempts" / receipt["attempt_sha256"])
    try:
        assert handoff.document(handoff.read_file(fd, "job.json", 512))["job_uid"] == "synthetic-child"
    finally:
        os.close(fd)


async def test_native_refusal_cannot_leave_send_marker_or_create(recovered, monkeypatch):
    value = recovered
    original = server_job(owner.manifest(value.parent, value.profile), "synthetic-first")
    missing = owner.control_workers._KubernetesApiError(404, "absent")
    request = AsyncMock(side_effect=[missing, original, {"items": []}])
    monkeypatch.setattr(owner, "_request", request)
    monkeypatch.setattr(api, "_native", AsyncMock(side_effect=ValueError("native epoch changed")))
    with pytest.raises(ValueError):
        await api.launch(value.parent, value.signed, None, object())
    assert all(call.args[0] == "GET" for call in request.await_args_list)
    assert not (value.root / "attempts" / contract.identity(value.signed, value.parent)).exists()


def test_cleanup_preserves_continuable_or_unknown_work(recovered):
    value = recovered
    path = stage_child(value)
    digest = contract.identity(value.signed, value.parent)
    files.is_marker_created(path, "launch.json", {"attempt_sha256": digest, "state": "sent"})
    files.is_marker_created(path, "job.json", {"attempt_sha256": digest, "job_uid": "synthetic-child"})
    work = value.root / "work" / "owner"
    work.mkdir()
    assert not api.is_cleaned_files(value.parent, value.signed, {"job_uid": "synthetic-child"})
    assert path.exists() and work.exists()
    work.rmdir()
    assert api.is_cleaned_files(value.parent, value.signed, {"job_uid": "synthetic-child"})
    assert not (value.root / "attempts").exists()
    assert (value.root / "bearer").read_bytes() == b"x" * 43
