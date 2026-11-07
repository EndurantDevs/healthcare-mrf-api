# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed owner Job admission, reply-loss observation and exact file cleanup."""

import base64
import hashlib
import json
import runpy
import sys
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from api import control_execution_evidence as api
from api import custom_import_owner_worker as owner
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_cli as cli
from process.custom_import import family_shard_production as production


def _profile():
    """Construct one synthetic deployment profile and signed original request."""
    key = Ed25519PrivateKey.generate()
    fact_limits_by_name = dict(max_spool_bytes=4096, max_record_bytes=256, memory_bytes=256)
    limits_by_kind = dict(
        input=dict(max_spool_bytes=4096, shard_count=1, chunk_bytes=256),
        plan=fact_limits_by_name,
        allocation=dict(spool=fact_limits_by_name, maximum_batch_records=4, maximum_batch_bytes=1024),
        facts=fact_limits_by_name,
        worker=dict(
            facts=fact_limits_by_name,
            output=dict(max_spool_bytes=4096, max_record_bytes=256, fragment_bytes=256, memory_bytes=256),
            audit=fact_limits_by_name,
            maximum_metadata_rows=4,
            maximum_metadata_bytes=1024,
        ),
        maximum_workers=1,
        checkpoint=fact_limits_by_name,
    )
    profile_by_field = dict(
        contract="custom-import/owner-profile/v1",
        image="registry.example.test/engine@sha256:" + "b" * 64,
        namespace="synthetic-engine",
        node_name="synthetic-node",
        pvc="synthetic-work",
        service_account="synthetic-worker",
        image_pull_secret="synthetic-pull",
        database_configmap="synthetic-config",
        database_secret="synthetic-secret",
        owner_role="synthetic_owner",
        verifying_key=base64.urlsafe_b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
        .decode()
        .rstrip("="),
        cpu_millis=1000,
        memory_bytes=64 * 1024**2,
        disk_bytes=1024**2,
        sql_memory_bytes=4 * 1024**2,
        reserve_memory_bytes=8 * 1024**2,
        work_mem_mb=1,
        maintenance_work_mem_mb=1,
        source_limits=limits_by_kind,
    )
    return profile_by_field, _signed_profile(profile_by_field, key)


def _server_job(profile_document, signed):
    job = json.loads(json.dumps(owner.manifest(signed, profile_document)))
    job["metadata"]["uid"] = "synthetic-owner-job"
    job["spec"]["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": "synthetic-owner-job"}}
    labels_by_key = job["spec"]["template"]["metadata"]["labels"]
    for prefix in ("batch.kubernetes.io/", ""):
        labels_by_key[prefix + "controller-uid"] = "synthetic-owner-job"
        labels_by_key[prefix + "job-name"] = job["metadata"]["name"]
    job["status"] = {"succeeded": 1}
    return job


def test_fixed_profile_admits_complete_limits_and_rejects_underbudget_or_extra_fields():
    profile_document, _signed = _profile()
    assert (
        cli.profile(
            {cli.PROFILE_ENV: handoff.canonical(profile_document).decode(), cli.NODE_ENV: profile_document["node_name"]}
        )[0]
        == profile_document
    )
    for changed in (
        profile_document | {"disk_bytes": 1},
        profile_document | {"memory_bytes": 1024**2},
        profile_document | {"extra": True},
        profile_document | {"cpu_millis": True},
    ):
        with pytest.raises(ValueError):
            cli.profile(
                {cli.PROFILE_ENV: handoff.canonical(changed).decode(), cli.NODE_ENV: profile_document["node_name"]}
            )


def test_job_contains_no_bearer_and_mounts_only_its_custody_and_work():
    profile_document, signed = _profile()
    job = owner.manifest(signed, profile_document)
    assert b"x" * 43 not in handoff.canonical(job)
    pod = job["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert "fsGroup" not in pod["securityContext"]  # Do not chmod protected PVC inputs.
    assert pod["containers"][0]["volumeMounts"][0]["readOnly"] is True
    assert pod["containers"][0]["volumeMounts"][1]["subPath"].endswith("/work")
    assert job["spec"]["backoffLimit"] == 0 and "ttlSecondsAfterFinished" not in job["spec"]


@pytest.mark.parametrize(
    "change", ["command", "environment", "volume", "token", "selector", "manual", "container", "image"]
)
async def test_exact_observation_rejects_spec_or_identity_change(monkeypatch, change):
    profile_document, signed = _profile()
    job = _server_job(profile_document, signed)
    pod = job["spec"]["template"]["spec"]
    match change:
        case "command":
            pod["containers"][0]["command"][-1] = "f" * 64
        case "environment":
            pod["containers"][0]["env"].append({"name": "UNTRUSTED", "value": "1"})
        case "volume":
            pod["volumes"][0]["persistentVolumeClaim"]["claimName"] = "foreign"
        case "token":
            pod["automountServiceAccountToken"] = True
        case "selector":
            job["spec"]["selector"]["matchLabels"]["batch.kubernetes.io/controller-uid"] = "foreign"
        case "manual":
            job["spec"]["manualSelector"] = True
        case "container":
            pod["containers"].append(deepcopy(pod["containers"][0]))
        case _:
            pod["containers"][0]["image"] = "registry.example.test/engine:latest"
    monkeypatch.setattr(owner, "_request", AsyncMock(return_value=job))
    with pytest.raises(ValueError):
        await owner.observed(signed, profile_document)


async def test_known_server_defaults_are_accepted(monkeypatch):
    profile_document, signed = _profile()
    job = _server_job(profile_document, signed)
    job["spec"]["completionMode"] = "NonIndexed"
    pod = job["spec"]["template"]["spec"]
    pod |= dict(
        dnsPolicy="ClusterFirst",
        schedulerName="default-scheduler",
        terminationGracePeriodSeconds=30,
        serviceAccount=profile_document["service_account"],
    )
    pod["containers"][0] |= dict(terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File")
    monkeypatch.setattr(owner, "_request", AsyncMock(return_value=job))
    job_receipt = await owner.observed(signed, profile_document)
    assert job_receipt["job_uid"] == "synthetic-owner-job"


async def test_cleanup_does_not_interrupt_active_owner_or_remove_files(monkeypatch):
    profile_document, signed = _profile()
    monkeypatch.setattr(
        owner,
        "_observed_job",
        AsyncMock(return_value=dict(job_uid="synthetic-owner-job", status=dict(active=1, succeeded=0, failed=0))),
    )
    request = AsyncMock()
    monkeypatch.setattr(owner, "_request", request)
    assert not await owner.is_stopped(
        signed, profile_document, expected_uid="synthetic-owner-job", require_terminal=True
    )
    request.assert_not_awaited()


def test_cleanup_requires_original_inode_empty_work_and_preserves_other_roots(tmp_path, monkeypatch):
    tmp_path.chmod(0o700)
    profile_document, signed = _profile()
    monkeypatch.setattr(handoff, "OWNER_PARENT", tmp_path)
    identifier = handoff.stage(signed, b"x" * 43, parent=tmp_path)
    receipt = owner.custody_receipt(signed)
    child = tmp_path / identifier / "work" / "owner"
    child.mkdir()
    with pytest.raises(ValueError):
        owner.cleanup(signed, receipt)
    child.rmdir()
    with pytest.raises(ValueError):
        owner.cleanup(signed, receipt | {"root_inode": receipt["root_inode"] + 1})
    other = tmp_path / "unrelated"
    other.mkdir()
    owner.cleanup(signed, receipt)
    assert other.exists() and not (tmp_path / identifier).exists()
    owner.cleanup(signed, receipt)  # Readback after lost cleanup acknowledgement.


async def test_invalid_signed_body_is_not_echoed_and_never_launches(monkeypatch):
    profile_document, signed = _profile()
    monkeypatch.setenv(cli.PROFILE_ENV, handoff.canonical(profile_document).decode())
    monkeypatch.setenv(cli.NODE_ENV, profile_document["node_name"])
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-read")
    signed["descriptor"]["fence"] += 1
    launch = AsyncMock()
    monkeypatch.setattr(owner, "launch", launch)
    request = SimpleNamespace(
        headers={"Authorization": "Bearer synthetic-read"},
        query_string="",
        body=handoff.canonical({"signed": signed, "bearer": "x" * 43}),
    )
    job_receipt = await api.serve_owner_handoff(request, object(), operation="launch")
    assert job_receipt.status == 503 and b"x" * 43 not in job_receipt.body
    launch.assert_not_awaited()


async def test_lost_create_reply_observes_exact_job_without_second_post(tmp_path, monkeypatch):
    """Exercise reply loss and verify that repeated admission never posts a second Job."""
    from process.custom_import import build_source, execution, family_shard_owner_access, snowflake_segmented_runner

    profile_document, signed = _profile()
    monkeypatch.setenv(cli.PROFILE_ENV, handoff.canonical(profile_document).decode())
    monkeypatch.setenv(cli.NODE_ENV, profile_document["node_name"])
    tmp_path.chmod(0o700)
    monkeypatch.setattr(handoff, "OWNER_PARENT", tmp_path)
    monkeypatch.setattr(owner, "require_capacity", lambda _value: None)

    session, factory = _synthetic_factory(profile_document)

    @dataclass(frozen=True)
    class Request:
        expected_base_generation_id: int | None = None
        expected_pointer_version: int = 0
        authorization_expires_at: object = None

    request = Request()
    original = SimpleNamespace(
        native={key: signed["descriptor"][key] for key in production.NATIVE_FIELDS},
        build_request=request,
        policy=SimpleNamespace(build=SimpleNamespace(lease_seconds=300)),
    )
    monkeypatch.setattr(production, "original_request", AsyncMock(return_value=original))
    monkeypatch.setattr(family_shard_owner_access, "require_owner_connection", AsyncMock())
    pages = []

    @asynccontextmanager
    async def page(_factory, bounded_request):
        pages.append(bounded_request)
        yield session, None

    monkeypatch.setattr(build_source, "_page_session", page)
    monkeypatch.setattr(snowflake_segmented_runner, "_retained_base", AsyncMock(return_value=request))
    monkeypatch.setattr(execution, "heartbeat_execution", AsyncMock(return_value=SimpleNamespace(state="running")))
    identifier = handoff.identity(signed)
    observed_by_field = {
        "handoff_id": identifier,
        "job_name": owner.job_name(identifier),
        "job_uid": "synthetic-owner-job",
        "status": {"active": 1, "succeeded": 0, "failed": 0},
    }
    observation = AsyncMock(side_effect=[None, observed_by_field])
    monkeypatch.setattr(owner, "observed", observation)
    post = AsyncMock(side_effect=owner.control_workers._KubernetesApiError(0, "synthetic reply lost"))
    monkeypatch.setattr(owner, "_request", post)
    assert await owner.launch(signed, b"x" * 43, factory) == observed_by_field
    assert post.await_count == 1 and post.await_args.args[0] == "POST"
    assert pages[0].authorization_expires_at == handoff.timestamp(signed["descriptor"]["authorization_expires_at"])
    assert (tmp_path / identifier / "launch.json").exists()
    # A repeated signed request cannot turn later absence into another POST.
    monkeypatch.setattr(owner, "observed", AsyncMock(return_value=None))
    post.reset_mock()
    assert (await owner.launch(signed, b"x" * 43, factory))["status"] == "launch_pending"
    post.assert_not_awaited()


async def test_cli_passes_authorized_memory_to_same_owner_factory_without_global_setting(monkeypatch):
    from process.custom_import import family_shard_owner_access, family_shard_owner_job, family_shard_sessions

    profile_document, signed = _profile()
    profile_document["work_mem_mb"] = 1024
    profile_document["sql_memory_bytes"] = 1025 * 1024**2
    # This test supplies already signature-checked custody; native joins remain
    # exercised separately and no host database is opened.
    limits = cli.source_limits(profile_document["source_limits"])
    monkeypatch.setattr(cli, "profile", lambda: (profile_document, b"k" * 32, limits))
    monkeypatch.setattr(handoff, "reopen_job", lambda _identifier: (signed, b"x" * 43))
    monkeypatch.setattr(handoff, "verify", lambda *_args, **_kwargs: signed["descriptor"])
    monkeypatch.setattr(cli, "require_profile", lambda *_args: None)
    monkeypatch.setattr(cli, "require_capacity", lambda *_args: None)

    @asynccontextmanager
    async def begin():
        yield

    session = SimpleNamespace(scalar=AsyncMock(return_value=profile_document["owner_role"]), begin=begin)

    @asynccontextmanager
    async def import_factory():
        yield session

    wrapped = Mock(return_value=import_factory)
    monkeypatch.setattr(family_shard_sessions, "import_session_factory", wrapped)
    monkeypatch.setattr(family_shard_owner_access, "require_owner_connection", AsyncMock())

    @dataclass(frozen=True)
    class Request:
        build_deadline_at: object
        authorization_expires_at: object = None

    request = Request(handoff.timestamp(signed["descriptor"]["build_deadline_at"]))
    original = SimpleNamespace(
        build_request=request, native={name: signed["descriptor"][name] for name in production.NATIVE_FIELDS}
    )
    monkeypatch.setattr(production, "original_request", AsyncMock(return_value=original))
    run_owner = AsyncMock(return_value="actual-adapter-result")
    monkeypatch.setattr(production, "run_original_owner", run_owner)
    job = Mock(return_value=SimpleNamespace(root=handoff.OWNER_PARENT / "owner", _terminal_custody=None))
    monkeypatch.setattr(family_shard_owner_job, "OwnerShardJob", job)
    database = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock(), session_factory=object())
    assert await cli.run_owner("a" * 64, database) == "actual-adapter-result"
    wrapped.assert_called_once_with(database.session_factory, work_mem_mb=1024, maintenance_work_mem_mb=1)
    assert run_owner.await_args.args[0] is import_factory
    assert run_owner.await_args.args[1] is original
    assert original.build_request is request
    assert job.call_args.args[0] is request
    assert job.return_value._terminal_custody.parent is signed
    assert job.call_args.kwargs["projection_work_mem_mb"] == 1024
    assert request.authorization_expires_at is None
    assert request.build_deadline_at == handoff.timestamp(signed["descriptor"]["build_deadline_at"])
    database.disconnect.assert_awaited_once()


async def test_expired_owner_start_refuses_before_database_or_capacity(monkeypatch):
    profile_document, signed = _profile()
    key = base64.urlsafe_b64decode(profile_document["verifying_key"] + "=")
    limits = cli.source_limits(profile_document["source_limits"])
    monkeypatch.setattr(cli, "profile", lambda: (profile_document, key, limits))
    monkeypatch.setattr(handoff, "reopen_job", lambda _identifier: (signed, b"x" * 43))
    monkeypatch.setattr(
        cli.time, "time", lambda: handoff.timestamp(signed["descriptor"]["authorization_expires_at"]).timestamp()
    )
    capacity = Mock()
    monkeypatch.setattr(cli, "require_capacity", capacity)
    database = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock())

    with pytest.raises(ValueError, match="^owner handoff is unavailable$"):
        await cli.run_owner(handoff.identity(signed), database)

    database.connect.assert_not_awaited()
    database.disconnect.assert_not_awaited()
    capacity.assert_not_called()


def test_fresh_recovery_cannot_extend_expired_parent_authority(tmp_path, monkeypatch):
    import datetime as dt

    from process.custom_import import family_shard_owner_attempt as attempts
    from tests.owner_attempt_support import recovery, signed_attempt

    custody = recovery(tmp_path, monkeypatch)
    parent = deepcopy(custody.parent)
    parent["descriptor"]["authorization_expires_at"] = (
        dt.datetime.fromtimestamp(custody.now - 1, dt.UTC).isoformat().replace("+00:00", "Z")
    )
    parent["signature"] = (
        base64.urlsafe_b64encode(custody.key.sign(handoff.DOMAIN + handoff.canonical(parent["descriptor"])))
        .decode()
        .rstrip("=")
    )
    child = signed_attempt(custody.key, custody.descriptor | {"owner_handoff_id": handoff.identity(parent)})

    with pytest.raises(ValueError, match="^owner handoff is unavailable$"):
        attempts.verify(child, parent, verifying_key=custody.public, now=custody.now, token=b"x" * 43)


def test_cli_main_awaits_owner_and_renders_original_receipt(monkeypatch, capsys):
    from db.connection import db
    from process.custom_import.runner_types import CandidateRunResult

    run_owner = AsyncMock(return_value=CandidateRunResult("activated", 7, 11))
    monkeypatch.setattr(cli, "run_owner", run_owner)

    assert cli.main(["a" * 64]) == 0
    run_owner.assert_awaited_once_with("a" * 64, db)
    captured = capsys.readouterr()
    assert captured.out == '{"execution_id":7,"generation_id":11,"status":"activated"}\n'
    assert captured.err == ""


def test_cli_main_redacts_owner_failure(monkeypatch, capsys):
    run_owner = AsyncMock(side_effect=ValueError("synthetic-private-input"))
    monkeypatch.setattr(cli, "run_owner", run_owner)

    assert cli.main(["a" * 64]) == 1
    run_owner.assert_awaited_once()
    captured = capsys.readouterr()
    assert captured.out == '{"status":"owner_unavailable"}\n'
    assert captured.err == ""


def test_owner_module_entry_validates_profile_before_reopening_custody(monkeypatch, capsys):
    profile_document, _signed = _profile()
    monkeypatch.setenv(cli.PROFILE_ENV, handoff.canonical(profile_document).decode())
    monkeypatch.setenv(cli.NODE_ENV, profile_document["node_name"])
    reopen = Mock(side_effect=ValueError("synthetic-private-input"))
    monkeypatch.setattr(handoff, "reopen_job", reopen)
    monkeypatch.setattr(sys, "argv", [cli.__name__, "a" * 64])
    monkeypatch.delitem(sys.modules, cli.__name__)

    with pytest.raises(SystemExit) as exited:
        runpy.run_module(cli.__name__, run_name="__main__", alter_sys=True)

    assert exited.value.code == 1
    reopen.assert_called_once_with("a" * 64)
    captured = capsys.readouterr()
    assert captured.out == '{"status":"owner_unavailable"}\n'
    assert captured.err == ""


def test_cli_main_passes_exact_attempt_identity_and_preserves_receipt(monkeypatch, capsys):
    from db.connection import db
    from process.custom_import.runner_types import CandidateRunResult

    run = AsyncMock(return_value=CandidateRunResult("activated", 7, 11))
    monkeypatch.setattr(cli, "run_owner", run)
    assert cli.main(["a" * 64, "--attempt-sha256", "b" * 64]) == 0
    run.assert_awaited_once_with("a" * 64, db, attempt_sha256="b" * 64)
    assert capsys.readouterr().out == '{"execution_id":7,"generation_id":11,"status":"activated"}\n'


async def test_cli_continued_attempt_uses_original_custody_and_trusted_continuation_dispatch(tmp_path, monkeypatch):
    import datetime as dt

    from process.custom_import import family_shard_owner_access, family_shard_owner_job, family_shard_sessions
    from process.custom_import import family_shard_owner_attempt as attempts
    from process.custom_import import family_shard_owner_attempt_files as files
    from tests.owner_attempt_support import recovery, signed_attempt
    from tests.test_custom_import_build_continuation import _test_authorization

    custody = recovery(tmp_path, monkeypatch)
    nonce = b"n" * 32
    authorization = _test_authorization(
        hashlib.sha256(b"x" * 43).digest(), nonce, dt.datetime.fromtimestamp(custody.now, dt.UTC)
    )
    original = custody.parent["descriptor"]
    authorization |= {
        "engine_execution_id": str(original["engine_execution_id"]),
        "capture_bundle_id": original["capture_bundle_id"],
        "producer_fence": original["fence"],
        "request_identity_sha256": original["request_identity_sha256"],
    }
    child = signed_attempt(custody.key, custody.descriptor | {"kind": "continue", "authorization": authorization})
    lock = files.attempt_lock(custody.root)
    try:
        files.stage(custody.parent, child, nonce, root=custody.root)
    finally:
        lock.close()
    monkeypatch.setattr(handoff, "OWNER_INPUT", custody.root)
    monkeypatch.setenv(cli.PROFILE_ENV, handoff.canonical(custody.profile).decode())
    monkeypatch.setenv(cli.NODE_ENV, custody.profile["node_name"])
    monkeypatch.setattr(cli, "require_capacity", lambda _profile: None)
    _session, factory = _synthetic_factory(custody.profile)
    monkeypatch.setattr(family_shard_sessions, "import_session_factory", lambda *_args, **_kwargs: factory)
    monkeypatch.setattr(family_shard_owner_access, "require_owner_connection", AsyncMock())
    request = object()
    reconstruct = AsyncMock(return_value=request)
    old_path = AsyncMock(side_effect=AssertionError("continued owner cannot enter original capture reconstruction"))
    run = AsyncMock(return_value="continued-result")
    monkeypatch.setattr(production, "owner_continuation_request", reconstruct)
    monkeypatch.setattr(production, "original_request", old_path)
    monkeypatch.setattr(production, "run_continued_owner", run)
    job = Mock(return_value=SimpleNamespace(root=handoff.OWNER_PARENT / "owner", _terminal_custody=None))
    monkeypatch.setattr(family_shard_owner_job, "OwnerShardJob", job)
    database = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock(), session_factory=object())
    assert (
        await cli.run_owner(
            handoff.identity(custody.parent), database, attempt_sha256=attempts.identity(child, custody.parent)
        )
        == "continued-result"
    )
    reconstruct.assert_awaited_once_with(factory, original, authorization, nonce, start=True)
    old_path.assert_not_awaited()
    assert job.call_args.args[0] is request
    assert job.return_value._terminal_custody.parent == custody.parent
    run.assert_awaited_once_with(factory, request, job.return_value)
    database.disconnect.assert_awaited_once()


def _signed_profile(profile_by_field, key):
    """Sign the synthetic independently prepared native descriptor."""
    descriptor_by_field = {name: "a" * 64 for name in handoff.DESCRIPTOR_FIELDS if name.endswith("sha256")}
    descriptor_by_field |= {name: 7 for name in handoff._IDS}
    descriptor_by_field |= dict(
        contract=handoff.CONTRACT,
        controller_execution_id="synthetic-execution",
        capture_job_uid="synthetic-job",
        capture_secret_uid="synthetic-secret",
        bearer_sha256=hashlib.sha256(b"x" * 43).hexdigest(),
        base_generation_id=None,
        base_pointer_version=0,
        build_deadline_at="2099-01-02T00:00:00.123456Z",
        authorization_expires_at="2099-01-02T00:00:00Z",
        owner_profile_sha256=hashlib.sha256(handoff.canonical(profile_by_field)).hexdigest(),
        owner_image=profile_by_field["image"],
    )
    signed_by_field = dict(
        descriptor=descriptor_by_field,
        signature=base64.urlsafe_b64encode(key.sign(handoff.DOMAIN + handoff.canonical(descriptor_by_field)))
        .decode()
        .rstrip("="),
    )
    return signed_by_field


def _synthetic_factory(profile_document):
    """Provide the original session owner for the private launch tests."""

    @asynccontextmanager
    async def transaction():
        yield

    session = SimpleNamespace(scalar=AsyncMock(return_value=profile_document["owner_role"]), begin=transaction)

    @asynccontextmanager
    async def factory():
        yield session

    return session, factory
