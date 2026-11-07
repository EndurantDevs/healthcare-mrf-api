# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected native copies preserve original bytes, scope, hashes and lease guards."""

import base64
import datetime as dt
import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, text, update

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
    CustomImportExecution,
    CustomImportLease,
)
from db.models.custom_import_retained_copy import CustomImportRetainedCaptureCopy
from process.custom_import import family_shard_handoff as custody
from process.custom_import import family_shard_production
from process.custom_import import retained_capture_copy as service
from process.custom_import import retained_capture_copy_contract as contract
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.capture_store import open_segmented_parquet_parts
from process.custom_import.publication import _capture_source_bundle_digest, _materialization_document
from process.custom_import.snowflake_bundle import DEFAULT_BUNDLE_ENCODING
from tests.custom_import_postgres_support import _ROOT, _migration, isolated_publication_case
from tests.test_custom_import_retained_copy import signed_copy
from tests.test_custom_import_retirement_block_postgres import _install_blocks
from tests.test_custom_import_segmented_runner_postgres import _registered_runtime


def _install(connection, schema):
    _install_blocks(connection, schema)
    migration = _migration(
        _ROOT / "alembic/versions/20261107010000_custom_import_retained_copy.py", "retained_copy_fixture"
    )
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


def _signed(descriptor, key):
    signature = base64.urlsafe_b64encode(key.sign(contract.DOMAIN + custody.canonical(descriptor))).decode().rstrip("=")
    return {"descriptor": descriptor, "signature": signature}


@asynccontextmanager
async def _case(monkeypatch, *, tight_quota=False):
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        if tight_quota:
            from tests import test_custom_import_segmented_runner_postgres as registered

            cap = _quota_policy(registered._capture_policy())
            monkeypatch.setattr(registered, "_capture_policy", lambda: cap)
        connector, request, policy, cursor, _connection = await _registered_runtime(case, monkeypatch)
        request = replace(request, bundle_request=replace(request.bundle_request, encoding=DEFAULT_BUNDLE_ENCODING))
        old = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, request, processing_policy=policy
        )
        assert old.status == "activated"
        configuration, _old_signed, bearer, key = signed_copy()
        async with case.sessions() as session, session.begin():
            execution = await session.get(CustomImportExecution, old.execution_id)
            bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id)
            locator_by_field = dict(
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                source_binding_revision_id=request.source_binding_revision_id,
                controller_execution_id="synthetic-retained-copy",
                source_capture_bundle_id=bundle.capture_bundle_id,
                source_capture_manifest_sha256=bytes(bundle.manifest_sha256).hex(),
            )
            old_usage = (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes
            configuration["owner_role"] = await session.scalar(text("SELECT CURRENT_USER"))
        prepared = await service.prepare(
            case.sessions,
            {
                "operation": "prepare",
                "locator": locator_by_field,
                "bearer": bearer.decode(),
                "capture_secret_uid": "synthetic-hosted-copy-secret",
                "authorization_expires_at": (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=600))
                .isoformat()
                .replace("+00:00", "Z"),
            },
            configuration,
        )
        signed = _signed(prepared["descriptor"], key)
        source_io = AsyncMock(side_effect=AssertionError("copy cannot enter acquisition"))
        monkeypatch.setattr(runner, "acquire_segmented_snowflake_capture", source_io)
        yield case, request, configuration, signed, bearer, old_usage, source_io


async def _parts(case, capture, request):
    async with case.sessions() as session, session.begin():
        async with open_segmented_parquet_parts(
            session,
            capture_bundle_id=capture,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        ) as parts:
            return [
                (part.ordinal, part.capture, part.record_count, part.arrow_byte_count, part.receipt)
                async for part in parts
            ]


async def test_full_copy_preserves_original_capture_and_semantic_materialization(monkeypatch):
    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        copy_receipt = await service.copy_capture(case.sessions, signed, bearer, configuration)
        assert copy_receipt["copy_state"] == "sealed" and copy_receipt["engine_state"] == "running"
        old_id = signed["descriptor"]["source_capture_bundle_id"]
        assert copy_receipt["capture_bundle_id"] != old_id
        assert await _parts(case, old_id, request) == await _parts(case, copy_receipt["capture_bundle_id"], request)
        scope_by_field = dict(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        )
        async with case.sessions() as session, session.begin():
            old = await session.get(CustomImportCaptureBundle, old_id)
            copied = await session.get(CustomImportCaptureBundle, copy_receipt["capture_bundle_id"])
            assert _materialization_document(old) == _materialization_document(copied)
            assert old.producing_execution_id != copied.producing_execution_id
            assert copied.producing_fence == 1 and old.sealed_at <= copied.sealed_at
            assert (await session.get(CustomImportExecution, old.producing_execution_id)).state == "completed"
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == 2 * old_usage
            assert await _capture_source_bundle_digest(
                session, capture_bundle_id=old_id, **scope_by_field
            ) == await _capture_source_bundle_digest(
                session, capture_bundle_id=copied.capture_bundle_id, **scope_by_field
            )
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.source_execution_id == old.producing_execution_id and audit.source_fence == old.producing_fence
            assert audit.source_token_sha256 == old.producing_token_sha256 and audit.source_pin_active is False
        original = await family_shard_production.original_request(case.sessions, signed["descriptor"], bearer)
        assert original.native["engine_execution_id"] == copy_receipt["engine_execution_id"]
        assert original.native["capture_bundle_id"] == copy_receipt["capture_bundle_id"]
        assert original.native["request_identity_sha256"] == signed["descriptor"]["request_identity_sha256"]
        source_io.assert_not_awaited()


async def test_committed_part_interruption_resumes_same_producer_without_double_charge(monkeypatch):
    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        append = service.append_pending_parquet_part
        attempted_parts = []

        async def interrupt(*args, **kwargs):
            attempted_parts.append((kwargs["capture"].manifest.stream_id, kwargs["ordinal"]))
            if len(attempted_parts) == 2:
                raise OSError("synthetic interruption after committed first part")
            return await append(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(service, "append_pending_parquet_part", interrupt)
            with pytest.raises(ExceptionGroup):
                await service.copy_capture(case.sessions, signed, bearer, configuration)
        pending = await service.inspect_copy(case.sessions, signed)
        assert pending["copy_state"] == "active"
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.source_pin_active is True
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(CustomImportCaptureParquetPart)
                    .where(CustomImportCaptureParquetPart.capture_bundle_id == pending["capture_bundle_id"])
                )
                == 1
            )
        copy_receipt = await service.copy_capture(case.sessions, signed, bearer, configuration)
        assert copy_receipt["engine_execution_id"] == pending["engine_execution_id"]
        async with case.sessions() as session, session.begin():
            assert (await session.get(CustomImportLease, copy_receipt["engine_execution_id"])).fence == 1
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == 2 * old_usage
        source_io.assert_not_awaited()


async def test_expired_native_lease_is_never_reclaimed_or_extended(monkeypatch):
    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        allocated = await service.reserve_copy(case.sessions, signed, bearer, configuration)
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == allocated["engine_execution_id"])
                .values(expires_at=dt.datetime(2000, 1, 1, tzinfo=dt.UTC))
            )
        with pytest.raises(ValueError):
            await service.copy_capture(case.sessions, signed, bearer, configuration)
        async with case.sessions() as session, session.begin():
            assert (await session.get(CustomImportLease, allocated["engine_execution_id"])).fence == 1
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage
            assert (
                await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            ).source_pin_active is True
        source_io.assert_not_awaited()


class _Kubernetes:
    """One exact synthetic API-server object; all database and file operations remain real."""

    def __init__(self):
        self.job = None
        self.posts = 0
        self.deletes = 0
        self.stopped = False
        self.hide_once = False

    async def request(self, method, path, body=None):
        import json

        from api.control_workers import _KubernetesApiError

        if method == "POST":
            self.posts += 1
            assert self.job is None
            self.job = json.loads(json.dumps(body))
            metadata = self.job["metadata"]
            metadata["uid"] = "synthetic-real-dev-copy-job"
            spec = self.job["spec"]
            spec["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": metadata["uid"]}}
            labels = spec["template"]["metadata"]["labels"]
            for prefix in ("batch.kubernetes.io/", ""):
                labels[prefix + "controller-uid"] = metadata["uid"]
                labels[prefix + "job-name"] = metadata["name"]
            self.job["status"] = {"active": 1}
            self.hide_once = True
            raise _KubernetesApiError(0, "synthetic lost create reply")
        if "/pods?" in path:
            if self.job is None:
                return {"items": []}
            uid = self.job["metadata"]["uid"]
            is_failed = self.job["status"].get("failed", 0) > 0
            state = {"terminated": {"exitCode": int(is_failed)}} if self.stopped else {"running": {}}
            return {
                "items": [
                    {
                        "metadata": {
                            "namespace": self.job["metadata"]["namespace"],
                            "ownerReferences": [{"uid": uid, "controller": True, "kind": "Job"}],
                        },
                        "spec": {"containers": [{"name": "worker"}]},
                        "status": {
                            "phase": ("Failed" if is_failed else "Succeeded") if self.stopped else "Running",
                            "containerStatuses": [{"name": "worker", "state": state}],
                        },
                    }
                ]
            }
        if method == "DELETE":
            assert body["preconditions"] == {"uid": self.job["metadata"]["uid"]}
            self.deletes += 1
            self.job = None
            return {}
        if self.hide_once:
            self.hide_once = False
            raise _KubernetesApiError(404, "synthetic observation delay")
        if self.job is None:
            raise _KubernetesApiError(404, "synthetic absent object")
        return self.job


async def test_private_job_reply_loss_actual_cli_copy_and_exact_partial_cleanup(monkeypatch, tmp_path):
    import json

    from api import custom_import_retained_copy as endpoint
    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import retained_capture_copy_files as files

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, _usage, source_io):
        kube = _job_setup(monkeypatch, tmp_path, configuration)
        launched = await endpoint.serve(
            _http({"operation": "launch", "signed": signed, "bearer": bearer.decode()}), case.sessions
        )
        assert launched.status == 200, launched.body
        initial = json.loads(launched.body)
        assert "job_uid" not in initial and initial["status"] == "absent"
        assert initial["producer_namespace"] == configuration["namespace"]
        observed = await endpoint.serve(_http({"operation": "observe", "signed": signed}), case.sessions)
        receipt = json.loads(observed.body)
        assert receipt["job_uid"] == "synthetic-real-dev-copy-job" and receipt["pods_stopped"] is False
        # Even an explicit duplicate request is observation-only after native SENT.
        again = await endpoint.serve(
            _http({"operation": "launch", "signed": signed, "bearer": bearer.decode()}), case.sessions
        )
        assert again.status == 200 and kube.posts == 1
        await _run_cli(case, signed, monkeypatch, tmp_path)
        kube.job["status"] = {"succeeded": 1}
        unsafe = await worker.observe(case.sessions, signed, configuration)
        assert unsafe["copy_state"] == "sealed" and unsafe["pods_stopped"] is False
        kube.stopped = True
        complete = await worker.observe(case.sessions, signed, configuration)
        assert complete["pods_stopped"] is True and complete["status"]["succeeded"] == 1
        await _owner_scope(case, signed, bearer, configuration, complete)
        with pytest.raises(ValueError):
            await worker.stop(case.sessions, signed, complete | {"job_uid": "foreign-uid"}, configuration, cleanup=True)
        assert kube.deletes == 0
        await _cleanup_retry(case, signed, configuration, complete, monkeypatch, kube)
        assert not (tmp_path / files.directory_name(contract.identity(signed))).exists()
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.cleaned_at is not None and audit.phase == "sealed" and audit.source_pin_active is False
            assert audit.first_job_uid == "synthetic-real-dev-copy-job"
            assert (await session.get(CustomImportExecution, audit.execution_id)).state == "running"
            assert (await session.get(CustomImportExecution, audit.source_execution_id)).state == "completed"
        source_io.assert_not_awaited()


async def test_retained_quota_refusal_keeps_original_and_pending_source_pin(monkeypatch):
    async with _case(monkeypatch, tight_quota=True) as (
        case,
        request,
        configuration,
        signed,
        bearer,
        old_usage,
        source_io,
    ):
        assert 2560 < old_usage <= 5120
        with pytest.raises(ExceptionGroup):
            await service.copy_capture(case.sessions, signed, bearer, configuration)
        allocated = await service.inspect_copy(case.sessions, signed)
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            execution = await session.get(CustomImportExecution, audit.execution_id)
            assert audit.phase == "active" and audit.source_pin_active is True
            assert execution.capture_bundle_id is None
            assert old_usage <= (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes <= 5120
            assert (
                await session.get(CustomImportCaptureBundle, signed["descriptor"]["source_capture_bundle_id"])
            ).capture_state == "sealed"
        source_io.assert_not_awaited()


async def test_closed_stream_resume_reverifies_existing_parts_before_eof_and_seal(monkeypatch):
    import json

    from process.custom_import import retained_capture_copy_native as native
    from process.custom_import.capture_pending import append_pending_parquet_part, mark_pending_parquet_eof

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        allocated = await service.reserve_copy(case.sessions, signed, bearer, configuration)
        original = await _parts(case, signed["descriptor"]["source_capture_bundle_id"], request)
        first = original[0]
        async with case.sessions() as session, session.begin():
            copy_input = await native.load_source(session, signed["descriptor"], bearer)
            pending = copy_input.pending(allocated["engine_execution_id"], 1)
            await append_pending_parquet_part(
                session,
                request=pending,
                capture_bundle_id=allocated["capture_bundle_id"],
                capture=first[1],
                ordinal=first[0],
                record_count=first[2],
                arrow_byte_count=first[3],
            )
            receipt = first[4]
            accounting = json.loads(receipt.canonical_manifest)
            await mark_pending_parquet_eof(
                session,
                request=pending,
                capture_bundle_id=allocated["capture_bundle_id"],
                stream_id=receipt.stream_id,
                part_count=accounting["part_count"],
                record_count=accounting["record_count"],
            )
        copied = await service.copy_capture(case.sessions, signed, bearer, configuration)
        assert copied["copy_state"] == "sealed"
        async with case.sessions() as session, session.begin():
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage * 2
        source_io.assert_not_awaited()


@pytest.mark.parametrize(
    "field", ["request_identity_sha256", "statement_sha256", "policy_sha256", "source_capture_manifest_sha256"]
)
async def test_even_valid_controller_signature_cannot_override_native_source_identity(monkeypatch, field):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        key = Ed25519PrivateKey.generate()
        configuration = configuration | {
            "verifying_key": base64.urlsafe_b64encode(key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw))
            .decode()
            .rstrip("=")
        }
        descriptor = signed["descriptor"] | {
            field: "f" * 64,
            "owner_profile_sha256": hashlib.sha256(custody.canonical(configuration)).hexdigest(),
        }
        bad = _signed(descriptor, key)
        with pytest.raises(ValueError):
            await service.reserve_copy(case.sessions, bad, bearer, configuration)
        async with case.sessions() as session, session.begin():
            assert await session.scalar(select(func.count()).select_from(CustomImportRetainedCaptureCopy)) == 0
            assert await session.scalar(select(func.count()).select_from(CustomImportExecution)) == 1
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage
        source_io.assert_not_awaited()


def _quota_policy(cap):
    """Keep native reader memory valid while leaving insufficient room for a second copy."""
    budget_by_limit = dict(
        maximum_parts=4,
        maximum_compressed_bytes=1536,
        maximum_decoded_bytes=1024 * 1024,
        maximum_arrow_bytes=2048,
        maximum_records=8,
        maximum_manifest_bytes=1024,
    )
    cap = replace(
        cap,
        part_limits=replace(
            cap.part_limits,
            maximum_compressed_bytes=1536,
            maximum_decoded_bytes=1024 * 1024,
            maximum_record_bytes=1024,
            read_chunk_bytes=1024,
        ),
        stream_budget=budget_by_limit,
        bundle_budget={name: limit * 2 for name, limit in budget_by_limit.items()},
        maximum_part_arrow_bytes=2048,
        maximum_part_manifest_bytes=1024,
        maximum_dataset_retained_bytes=5120,
    )
    return cap


def _job_setup(monkeypatch, tmp_path, configuration):
    """Double Kubernetes and Linux capacity; preserve real profile, files and PostgreSQL."""
    from unittest.mock import Mock

    from api import custom_import_owner_worker as owner
    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import retained_capture_copy_cli as cli
    from process.custom_import import retained_capture_copy_files as files
    from process.custom_import.family_shard_owner_cli import NODE_ENV, PROFILE_ENV

    kube = _Kubernetes()
    tmp_path.chmod(0o700)
    monkeypatch.setattr(custody, "OWNER_PARENT", tmp_path)
    # Default filesystem arguments are fixed at definition time; bind the actual protected parent explicitly.
    stage = files.stage
    reopen = files.reopen
    cleanup = files.cleanup
    monkeypatch.setattr(files, "stage", lambda signed, bearer: stage(signed, bearer, parent=tmp_path))
    monkeypatch.setattr(files, "reopen", lambda identifier, *, parent=tmp_path: reopen(identifier, parent=parent))
    monkeypatch.setattr(files, "cleanup", lambda signed, receipt: cleanup(signed, receipt, parent=tmp_path))
    monkeypatch.setattr(owner, "_request", kube.request)
    monkeypatch.setattr(worker, "require_capacity", Mock())
    monkeypatch.setattr(cli, "require_capacity", Mock())
    monkeypatch.setenv(PROFILE_ENV, custody.canonical(configuration).decode())
    monkeypatch.setenv(NODE_ENV, configuration["node_name"])
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-private-token")
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)

    return kube


async def _run_cli(case, signed, monkeypatch, tmp_path):
    """Use real native preflight and copying with only the mounted path and connector doubled."""
    from types import SimpleNamespace

    from process.custom_import import retained_capture_copy_cli as cli
    from process.custom_import import retained_capture_copy_files as files

    database = SimpleNamespace(session_factory=case.sessions, connect=AsyncMock(), disconnect=AsyncMock())
    with monkeypatch.context() as mounted:
        mounted.setattr(files, "INPUT", tmp_path / files.directory_name(contract.identity(signed)))
        assert await cli.run_copy(contract.identity(signed), database) == {"status": "capture_ready"}
    database.connect.assert_awaited_once()
    database.disconnect.assert_awaited_once()


async def _owner_scope(case, signed, bearer, configuration, complete):
    """Ordinary owner preparation must join the genuinely fresh execution and capture."""
    import json

    from api import control_execution_evidence as evidence

    locator_by_field = {
        name: signed["descriptor"][name]
        for name in (
            "dataset_id",
            "definition_revision_id",
            "source_binding_revision_id",
            "controller_execution_id",
        )
    }
    prepared = await evidence._prepare_owner_handoff(
        case.sessions, {"locator": locator_by_field, "bearer": bearer.decode()}, configuration
    )
    native = json.loads(prepared.body)["native"]
    assert (native["engine_execution_id"], native["capture_bundle_id"]) == (
        complete["engine_execution_id"],
        complete["capture_bundle_id"],
    )


async def _cleanup_retry(case, signed, configuration, complete, monkeypatch, kube):
    """An interrupted unlink must retry exact custody without another Job delete."""
    import os

    from api import custom_import_retained_copy_worker as worker

    unlink = os.unlink

    def interrupt(name, **kwargs):
        if name == "bearer":
            raise OSError("synthetic interrupted cleanup")
        return unlink(name, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", interrupt)
        with pytest.raises(OSError):
            await worker.stop(case.sessions, signed, complete, configuration, cleanup=True)
    assert kube.deletes == 1
    cleanup_receipt = await worker.stop(case.sessions, signed, complete, configuration, cleanup=True)
    assert cleanup_receipt == {"copy_id": contract.identity(signed), "pods_absent": True, "cleaned": True}


def _http(document):
    from types import SimpleNamespace

    return SimpleNamespace(
        headers={"Authorization": "Bearer synthetic-private-token"}, query_string="", body=custody.canonical(document)
    )


async def test_native_cancel_finalizes_only_stopped_active_copy(monkeypatch, tmp_path):
    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import execution

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        kube = _job_setup(monkeypatch, tmp_path, configuration)
        await worker.launch(case.sessions, signed, bearer, configuration)
        receipt = await worker.observe(case.sessions, signed, configuration)
        async with case.sessions() as session, session.begin():
            transition = await execution.request_bound_execution_cancellation(session, **worker._request_pins(signed))
            assert transition.state == "canceling"
        stopped = await worker.stop(case.sessions, signed, receipt, configuration)
        assert stopped == {"copy_id": contract.identity(signed), "pods_absent": True, "cleaned": False}
        cleaned = await worker.stop(case.sessions, signed, receipt, configuration, cleanup=True)
        assert cleaned["cleaned"] is True and kube.deletes == 1
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.phase == "closed" and audit.source_pin_active is False
            assert (await session.get(CustomImportExecution, audit.execution_id)).state == "canceled"
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage
        source_io.assert_not_awaited()


@pytest.mark.parametrize("allocated", [False, True])
async def test_canceled_unsent_copy_closes_without_fabricated_uid(monkeypatch, tmp_path, allocated):
    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import execution

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        kube = _job_setup(monkeypatch, tmp_path, configuration)
        if allocated:
            await service.reserve_copy(case.sessions, signed, bearer, configuration)
        absent_receipt_by_field = {"copy_id": contract.identity(signed), "status": "absent"}
        assert await worker.observe(case.sessions, signed, configuration) == absent_receipt_by_field
        with pytest.raises(ValueError):
            await worker.stop(case.sessions, signed, absent_receipt_by_field, configuration, cleanup=True)
        async with case.sessions() as session, session.begin():
            await execution.request_bound_execution_cancellation(session, **worker._request_pins(signed))
        cleaned = await worker.stop(case.sessions, signed, absent_receipt_by_field, configuration, cleanup=True)
        assert cleaned == {"copy_id": contract.identity(signed), "pods_absent": True, "cleaned": True}
        assert kube.posts == kube.deletes == 0
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            if allocated:
                assert audit.cleaned_at is not None and audit.phase == "closed" and audit.first_job_uid is None
                assert (await session.get(CustomImportExecution, audit.execution_id)).state == "canceled"
            else:
                assert audit is None
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage
        source_io.assert_not_awaited()


async def test_legacy_segment_grants_do_not_enable_raw_originals(monkeypatch):
    from process.custom_import.capture_authority import require_raw_capture_authority
    from process.custom_import.capture_store import CaptureStoreError

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        # Only the isolated synthetic schema changes; preserve the production privilege contract.
        async with case.engine.begin() as connection:
            await connection.execute(
                text(f'GRANT INSERT ON "{case.schema_name}".custom_import_capture_parquet_part TO pg_monitor')
            )
        async with case.sessions() as session, session.begin():
            with pytest.raises(CaptureStoreError):
                await require_raw_capture_authority(session)
        copied = await service.copy_capture(case.sessions, signed, bearer, configuration)
        assert copied["copy_state"] == "sealed"
        async with case.sessions() as session, session.begin():
            assert (await session.get(CustomImportCaptureUsage, request.dataset_id)).retained_bytes == old_usage * 2
        source_io.assert_not_awaited()


async def test_sealed_copy_retains_native_cancellation(monkeypatch, tmp_path):
    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import execution

    async with _case(monkeypatch) as (case, request, configuration, signed, bearer, old_usage, source_io):
        kube = _job_setup(monkeypatch, tmp_path, configuration)
        await worker.launch(case.sessions, signed, bearer, configuration)
        await service.copy_capture(case.sessions, signed, bearer, configuration)
        kube.job["status"] = {"succeeded": 1}
        kube.stopped = True
        receipt = await worker.observe(case.sessions, signed, configuration)
        async with case.sessions() as session, session.begin():
            await execution.request_bound_execution_cancellation(session, **worker._request_pins(signed))
        cleaned = await worker.stop(case.sessions, signed, receipt, configuration, cleanup=True)
        assert cleaned["cleaned"] is True
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.phase == "sealed" and audit.cleaned_at is not None
            # Copy cleanup cannot finalize a later owner/epoch's native cancellation.
            assert (await session.get(CustomImportExecution, audit.execution_id)).state == "canceling"
        source_io.assert_not_awaited()


async def test_cli_failure_retains_pin_until_stopped(monkeypatch, tmp_path):
    from types import SimpleNamespace

    from api import custom_import_retained_copy_worker as worker
    from process.custom_import import retained_capture_copy_cli as cli
    from process.custom_import import retained_capture_copy_files as files

    async with _case(monkeypatch, tight_quota=True) as (
        case,
        request,
        configuration,
        signed,
        bearer,
        old_usage,
        source_io,
    ):
        kube = _job_setup(monkeypatch, tmp_path, configuration)
        await worker.launch(case.sessions, signed, bearer, configuration)
        await worker.observe(case.sessions, signed, configuration)
        database = SimpleNamespace(session_factory=case.sessions, connect=AsyncMock(), disconnect=AsyncMock())
        with monkeypatch.context() as mounted:
            mounted.setattr(files, "INPUT", tmp_path / files.directory_name(contract.identity(signed)))
            with pytest.raises(ExceptionGroup):
                await cli.run_copy(contract.identity(signed), database)
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.source_pin_active is True and audit.phase == "active"
            assert (await session.get(CustomImportExecution, audit.execution_id)).state == "failed"
        kube.job["status"] = {"failed": 1}
        kube.stopped = True
        receipt = await worker.observe(case.sessions, signed, configuration)
        assert receipt["pods_stopped"] is True
        cleaned = await worker.stop(case.sessions, signed, receipt, configuration, cleanup=True)
        assert cleaned["cleaned"] is True and kube.deletes == 1
        async with case.sessions() as session, session.begin():
            audit = await session.get(CustomImportRetainedCaptureCopy, contract.identity(signed))
            assert audit.source_pin_active is False and audit.phase == "closed" and audit.cleaned_at is not None
            assert (await session.get(CustomImportExecution, audit.source_execution_id)).state == "completed"
        source_io.assert_not_awaited()
