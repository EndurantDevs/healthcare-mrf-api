# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL lifecycle proof using only sealed synthetic HTTPS bytes."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import aiohttp
import pyarrow as pa
import pytest
from sqlalchemy import func, select, update

from db.models.custom_import import (
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportRejection,
)
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject
from process.custom_import import capture_handoff, execution, https_candidate, runner
from process.custom_import.capture import capture_stream
from process.custom_import.capture_store import load_replayable_parquet_bundle
from process.custom_import.definition_store import register_definition
from process.custom_import.https_candidate import https_request_identity_sha256, run_https_candidate
from process.custom_import.read_core import ExtensionReadAuthorization, PinnedReadTarget, SearchRequest
from process.custom_import.runner_types import LeaseAuthorityLost
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.capture_handoff_test_support import fixture as capture_handoff_fixture
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_https_candidate import _request as source_request
from tests.test_custom_import_read_core_postgres import _service


@pytest.fixture(autouse=True)
def forbid_https_transport(monkeypatch):
    attempts = []

    def forbidden_transport(*_args, **_kwargs):
        attempts.append(True)
        raise AssertionError("real HTTPS is forbidden")

    monkeypatch.setattr(aiohttp.ClientSession, "_request", forbidden_transport)
    yield
    assert not attempts


def _request_for_seed(seed, suffix, *, tables_by_stream=None):
    request, payloads = source_request(definition=seed.definition, tables_by_stream=tables_by_stream)
    return replace(
        request,
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        source_snapshot_token=f"synthetic-{suffix}-snapshot",
        idempotency_key=f"synthetic-{suffix}-execution",
        lease_token=f"synthetic-{suffix}-owner",
        timeout_seconds=30,
    ), payloads


def _install_source(monkeypatch, request, payloads):
    acquired_streams = []

    async def acquire(url, stream, **kwargs):
        expected = next(item for item in request.objects if item.stream_id == stream.stream_id)
        assert url == expected.url
        assert kwargs["expected_sha256"] == expected.expected_sha256
        assert kwargs["expected_bytes"] == expected.expected_bytes
        acquired_streams.append(stream.stream_id)
        with BytesIO(payloads[stream.stream_id]) as source:
            return capture_stream(
                source, stream, source_snapshot_token=kwargs["source_snapshot_token"], limits=kwargs["limits"]
            )

    monkeypatch.setattr(https_candidate.https, "acquire_https", acquire)
    return acquired_streams


async def _current_read(case, seed):
    async with case.sessions() as session:
        pointer = await session.get(CustomImportCurrentGeneration, seed.dataset_id)
        assert pointer is not None
        target = PinnedReadTarget(
            seed.dataset_id, pointer.generation_id, seed.definition_revision_id, seed.schema_revision_id, "default"
        )
        page = await _service().search(
            session,
            authorization=ExtensionReadAuthorization("synthetic-read"),
            request=SearchRequest(target=target),
        )
    return target, page


@pytest.mark.asyncio
async def test_https_execute_reloads_durable_bundle_and_serves_sealed_current_snapshot(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_execute", definition.definition)
        request, payloads = _request_for_seed(seed, "https-execute")
        acquired_streams = _install_source(monkeypatch, request, payloads)
        run_result = await run_https_candidate(case.sessions, request)
        assert run_result.status == "activated" and run_result.accepted_family_count == 1
        assert acquired_streams == ["providers", "rates"]
        async with case.sessions() as session, session.begin():
            execution = await session.get(CustomImportExecution, run_result.execution_id)
            assert execution is not None and execution.capture_bundle_id is not None
            retained = await load_replayable_parquet_bundle(
                session,
                capture_bundle_id=execution.capture_bundle_id,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
            )
            seal = await session.get(CustomImportGenerationSeal, run_result.generation_id)
            assert seal is not None and seal.root_count == seal.family_count == 1
        assert {capture.receipt.stream_id: capture.parts[0] for capture in retained} == payloads
        replay = await https_candidate._replay(request, retained)
        assert replay["rates"][0]["amount"] is None
        current_target, page = await _current_read(case, seed)
        assert current_target.generation_id == run_result.generation_id and page.total == 1
        assert (
            next(field.value for field in page.items[0].root_fields if field.field_id == "display_name")
            == "Synthetic Provider"
        )


@pytest.mark.asyncio
async def test_https_expired_bound_execution_resumes_offline_without_refetch(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_resume", definition.definition)
        request, payloads = _request_for_seed(seed, "https-resume")
        acquired_streams = _install_source(monkeypatch, request, payloads)
        original_materialize = runner._materialize_candidate

        async def expire_before_materialization(session_factory, candidate, grant, admitted):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == grant.execution_id)
                    .values(expires_at=func.clock_timestamp())
                )
            return await original_materialize(session_factory, candidate, grant, admitted)

        monkeypatch.setattr(runner, "_materialize_candidate", expire_before_materialization)
        interrupted = await run_https_candidate(case.sessions, request)
        assert interrupted.status == "lease_lost"
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, interrupted.execution_id)
            assert execution is not None and execution.capture_bundle_id is not None
            original_bundle_id = execution.capture_bundle_id
        monkeypatch.setattr(runner, "_materialize_candidate", original_materialize)

        async def forbidden_acquisition(*_args, **_kwargs):
            pytest.fail("offline resume must not fetch")

        monkeypatch.setattr(https_candidate.https, "acquire_https", forbidden_acquisition)
        for changed in (
            replace(request, timeout_seconds=60),
            replace(
                request,
                objects=tuple(
                    replace(source_object, url=source_object.url + "&renewed=true") for source_object in request.objects
                ),
            ),
        ):
            with pytest.raises(https_candidate.HttpsCandidateError, match="^https_candidate_failed$"):
                await run_https_candidate(case.sessions, changed, resume=True)
        resumed = await run_https_candidate(
            case.sessions, replace(request, lease_token="synthetic-recovery-owner"), resume=True
        )
        assert resumed.status == "activated" and resumed.execution_id == interrupted.execution_id
        assert acquired_streams == ["providers", "rates"]
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, resumed.execution_id)
            lease = await session.get(CustomImportLease, resumed.execution_id)
            assert execution.capture_bundle_id == original_bundle_id
            assert lease.fence == 2
        current_target, page = await _current_read(case, seed)
        assert current_target.generation_id == resumed.generation_id and page.total == 1


@pytest.mark.asyncio
async def test_https_new_execution_reuses_same_content_after_transport_refresh(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_transport_refresh", definition.definition)
        request, payloads = _request_for_seed(seed, "https-transport-refresh")
        _install_source(monkeypatch, request, payloads)
        initial = await run_https_candidate(case.sessions, request)
        assert initial.status == "activated"
        old_target, _ = await _current_read(case, seed)
        refreshed = replace(
            request,
            objects=tuple(
                replace(source_object, url=source_object.url + "&renewed=true") for source_object in request.objects
            ),
            timeout_seconds=60,
            idempotency_key="synthetic-refreshed-execution",
            lease_token="synthetic-refreshed-owner",
        )
        acquired_streams = _install_source(monkeypatch, refreshed, payloads)
        repeated = await run_https_candidate(case.sessions, refreshed)
        assert repeated.status == "no_change" and repeated.execution_id != initial.execution_id
        assert repeated.generation_id is not None
        assert acquired_streams == ["providers", "rates"]
        async with case.sessions() as session:
            original = await session.get(CustomImportExecution, initial.execution_id)
            replayed = await session.get(CustomImportExecution, repeated.execution_id)
            no_change = await session.get(CustomImportNoChangeSeal, repeated.execution_id)
            assert replayed.capture_bundle_id == original.capture_bundle_id
            assert bytes(replayed.request_identity_sha256) != bytes(original.request_identity_sha256)
            assert replayed.state == "no_change" and no_change.candidate_generation_id == repeated.generation_id
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureBundle)) == 2
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureParquetPart)) == 2
            assert await session.scalar(select(func.count()).select_from(CustomImportGeneration)) == 2
        current_target, page = await _current_read(case, seed)
        assert current_target == old_target and page.total == 1


@pytest.mark.asyncio
async def test_https_new_execution_refuses_changed_content_for_sealed_snapshot(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_content_conflict", definition.definition)
        request, payloads = _request_for_seed(seed, "https-content-conflict")
        _install_source(monkeypatch, request, payloads)
        initial = await run_https_candidate(case.sessions, request)
        assert initial.status == "activated"
        old_target, _ = await _current_read(case, seed)
        changed, changed_payloads = _request_for_seed(
            seed, "https-content-conflict", tables_by_stream=_family_tables("Changed", Decimal("10.00"))
        )
        changed = replace(changed, idempotency_key="synthetic-changed-execution", lease_token="synthetic-changed-owner")
        acquired_streams = _install_source(monkeypatch, changed, changed_payloads)
        with pytest.raises(https_candidate.HttpsCandidateError, match="^https_candidate_failed$"):
            await run_https_candidate(case.sessions, changed)
        assert acquired_streams == ["providers", "rates"]
        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureBundle)) == 2
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureParquetPart)) == 2
            assert await session.scalar(select(func.count()).select_from(CustomImportGeneration)) == 1
        current_target, page = await _current_read(case, seed)
        assert current_target == old_target
        assert (
            next(field.value for field in page.items[0].root_fields if field.field_id == "display_name")
            == "Synthetic Provider"
        )


@pytest.mark.asyncio
async def test_https_stale_bind_rolls_back_all_new_durable_parts(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_stale_bind", definition.definition)
        request, payloads = _request_for_seed(seed, "https-stale-bind")
        _install_source(monkeypatch, request, payloads)
        original_store = https_candidate._store

        async def expired_store(session_factory, candidate, grant, captures):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == grant.execution_id)
                    .values(expires_at=func.clock_timestamp())
                )
            return await original_store(session_factory, candidate, grant, captures)

        monkeypatch.setattr(https_candidate, "_store", expired_store)
        result = await run_https_candidate(case.sessions, request)
        assert result.status == "lease_lost"
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, result.execution_id)
            assert execution is not None and execution.capture_bundle_id is None
            bundle_ids = (await session.scalars(select(CustomImportCaptureBundle.capture_bundle_id))).all()
            assert bundle_ids == [seed.capture_bundle_id]
            assert await session.scalar(select(func.count()).select_from(CustomImportCaptureParquetPart)) == 0
            assert await session.scalar(select(func.count()).select_from(CustomImportGeneration)) == 0


def _family_tables(name, amount):
    return {
        "providers": pa.table({"Provider ID": ["1234567893"], "Provider Name": [name]}),
        "rates": pa.table({"Provider ID": ["1234567893"], "Service Code": ["A100"], "Amount": [amount]}),
    }


@pytest.mark.asyncio
async def test_https_invalid_child_rejects_whole_family_and_retains_previous_snapshot(monkeypatch):
    async with isolated_publication_case() as case:
        definition, _ = source_request()
        seed = await runner_fixture._seed_case(case, "https_invalid_child", definition.definition)
        initial_request, payloads = _request_for_seed(
            seed, "https-prior", tables_by_stream=_family_tables("Prior", Decimal("10.00"))
        )
        _install_source(monkeypatch, initial_request, payloads)
        initial = await run_https_candidate(case.sessions, initial_request)
        assert initial.status == "activated"
        old_target, _ = await _current_read(case, seed)
        invalid_request, invalid_payloads = _request_for_seed(
            seed, "https-invalid", tables_by_stream=_family_tables("Rejected Replacement", "invalid")
        )
        _install_source(monkeypatch, invalid_request, invalid_payloads)
        rejected = await run_https_candidate(case.sessions, invalid_request)
        assert rejected.status == "no_change" and rejected.accepted_family_count == 0 and rejected.rejection_count == 1
        current_target, current_page = await _current_read(case, seed)
        assert current_target == old_target
        assert (
            next(field.value for field in current_page.items[0].root_fields if field.field_id == "display_name")
            == "Prior"
        )
        async with case.sessions() as session:
            models_by_type = await runner_fixture._generation_models(session, rejected.generation_id)
            rejection = models_by_type[CustomImportRejection]
            rejections = (
                await session.scalars(select(rejection).where(rejection.execution_id == rejected.execution_id))
            ).all()
            assert [rejection_row.code for rejection_row in rejections] == ["field_type_invalid"]
            detail = await _service().root_detail(
                session,
                authorization=ExtensionReadAuthorization("synthetic-read"),
                target=old_target,
                winner=current_page.items[0].winner,
            )
        assert next(field.value for field in detail.children[0].fields if field.field_id == "amount") == Decimal(
            "10.00"
        )


async def _claimed_handoff_capture(case):
    """Persist exactly the revisions and request hash the capture boundary checks."""

    state = capture_handoff_fixture()
    async with case.sessions() as session, session.begin():
        seed = await register_definition(session, "synthetic_capture_" + uuid4().hex[:16], state.request.definition)
    request = replace(
        state.request,
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        timeout_seconds=300,
    )
    identity = https_request_identity_sha256(request, raw_originals=True)
    async with case.sessions() as session, session.begin():
        submission = await execution.reserve_execution(
            session,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism="local",
            request_identity_sha256=identity,
        )
        grant = await execution.claim_execution(
            session, execution_id=submission.execution_id, token=request.lease_token
        )
        now = await execution._database_now(session)
    permit = replace(
        state.verified.permit,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        request_identity_sha256=identity.hex(),
        timeout_seconds=300,
        issued_at=now - timedelta(seconds=1),
        expires_at=now + timedelta(minutes=15),
    )
    verified = replace(state.verified, permit=permit, execution_id=grant.execution_id, fence=grant.fence)
    return SimpleNamespace(verified=verified, captures=state.captures)


async def _raw_capture_counts(case):
    async with case.sessions() as session:
        return tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (CustomImportCaptureBundle, CustomImportCaptureRawObject)
            ]
        )


async def test_complete_capture_and_exact_execution_binding_commit_together():
    async with isolated_publication_case() as case:
        state = await _claimed_handoff_capture(case)
        await capture_handoff.preflight_capture(case.sessions, state.verified)
        bundle_id = await capture_handoff.register_capture(case.sessions, state.verified, state.captures)
        assert await _raw_capture_counts(case) == (1, 2)
        async with case.sessions() as session:
            retained = await session.get(CustomImportExecution, state.verified.execution_id)
            assert retained.capture_bundle_id == bundle_id
        assert await capture_handoff.retained_capture(case.sessions, state.verified) == (bundle_id, state.captures)


async def test_lease_loss_after_native_copy_rolls_back_payload_and_binding(monkeypatch):
    async with isolated_publication_case() as case:
        state = await _claimed_handoff_capture(case)
        monkeypatch.setattr(execution, "bind_execution_capture_bundle", AsyncMock(return_value=None))
        with pytest.raises(LeaseAuthorityLost):
            await capture_handoff.register_capture(case.sessions, state.verified, state.captures)
        assert await _raw_capture_counts(case) == (0, 0)
        async with case.sessions() as session:
            retained = await session.get(CustomImportExecution, state.verified.execution_id)
            assert retained.capture_bundle_id is None
