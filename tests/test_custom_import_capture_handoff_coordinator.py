# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic remote capture coordination without source or database access."""

import asyncio
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import https_candidate, object_candidate, upload
from process.custom_import.capture import SealedCapture, capture_stream
from process.custom_import.execution import ExecutionSubmission
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network
from tests.test_custom_import_object_candidate import _request as _object_request


@pytest.fixture(params=("https", "s3", "upload"))
def capture_case(request, coordinator, monkeypatch):
    source_kind = request.param
    if source_kind == "https":
        candidate = replace(coordinator.request, maximum_dataset_retained_bytes=1024 * 1024)
        payloads = coordinator.payloads_by_stream
        run = https_candidate.run_https_candidate
        failure = https_candidate.HttpsCandidateError
        acquire = coordinator.acquire
    else:
        candidate, payloads = _object_request(source_kind, "parquet")
        run = object_candidate.run_object_candidate
        failure = object_candidate.ObjectCandidateError
        acquire = AsyncMock()
        monkeypatch.setattr(object_candidate, "_acquire_object", acquire)
    captures = tuple(
        capture_stream(
            BytesIO(payloads[stream.stream_id]), stream, source_snapshot_token=candidate.source_snapshot_token
        )
        for stream in candidate.definition.source_streams
    )

    def acquired(*arguments, **_options):
        stream = arguments[-1]
        return next(sealed for sealed in captures if sealed.manifest.stream_id == stream.stream_id)

    acquire.side_effect = acquired
    handoff = SimpleNamespace(
        require_candidate_binding=Mock(),
        preflight=AsyncMock(side_effect=lambda *_args: asyncio.get_running_loop().time() + candidate.timeout_seconds),
        register=AsyncMock(return_value=23),
        retained=AsyncMock(return_value=captures),
    )
    forbidden_operations = []
    for name in ("register_raw_capture_bundle", "load_replayable_raw_bundle"):
        operation = AsyncMock(side_effect=AssertionError("remote capture must not use local storage"))
        monkeypatch.setattr(https_candidate, name, operation)
        forbidden_operations.append(operation)
    for name in (
        "require_raw_capture_authority",
        "register_replayable_parquet_bundle",
        "load_replayable_parquet_bundle",
        "bind_execution_capture_bundle",
    ):
        operation = coordinator.mocks_by_name[name]
        operation.side_effect = AssertionError("remote capture must not use local capture authority")
        forbidden_operations.append(operation)
    yield SimpleNamespace(
        request=candidate,
        run=run,
        failure=failure,
        handoff=handoff,
        captures=captures,
        acquire=acquire,
    )
    for operation in forbidden_operations:
        operation.assert_not_awaited()


async def test_handoff_preflights_before_acquisition_and_replays_raw_parquet(coordinator, capture_case):
    case = capture_case
    acquire = case.acquire.side_effect

    def checked_acquire(*arguments, **options):
        case.handoff.preflight.assert_awaited_once_with(case.request, coordinator.grant)
        return acquire(*arguments, **options)

    case.acquire.side_effect = checked_acquire
    result = await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    assert result.status == "no_change"
    case.handoff.require_candidate_binding.assert_called_once_with(case.request)
    case.handoff.register.assert_awaited_once_with(case.request, coordinator.grant, case.captures)
    assert all(isinstance(sealed, SealedCapture) for sealed in case.handoff.register.call_args.args[2])
    case.handoff.retained.assert_awaited_once_with(case.request, coordinator.grant, 23)
    assert case.acquire.await_count == len(case.request.objects)
    runner = coordinator.mocks_by_name["run_claimed_candidate"]
    assert runner.call_args.args[1].roots[0]["display_name"] == "Synthetic Provider"
    assert len(runner.call_args.args[3].families) == 1
    identity = coordinator.mocks_by_name["reserve_execution"].call_args.kwargs["request_identity_sha256"]
    expected = (
        https_candidate.https_request_identity_sha256(case.request, raw_originals=True)
        if isinstance(case.request, https_candidate.HttpsCandidateRequest)
        else object_candidate.object_request_identity_sha256(case.request)
    )
    assert identity == expected


async def test_rejected_candidate_binding_cannot_claim_or_access_source(coordinator, capture_case):
    case = capture_case
    case.handoff.require_candidate_binding.side_effect = ValueError("synthetic binding refusal")
    with pytest.raises(case.failure, match="candidate_failed"):
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    assert not coordinator.sessions
    case.acquire.assert_not_awaited()
    case.handoff.preflight.assert_not_awaited()
    case.handoff.register.assert_not_awaited()
    case.handoff.retained.assert_not_awaited()


async def test_denied_preflight_never_opens_source_or_registers(coordinator, capture_case):
    case = capture_case
    case.handoff.preflight.side_effect = ValueError("synthetic authorization refusal")
    with pytest.raises(case.failure, match="candidate_failed"):
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    case.acquire.assert_not_awaited()
    case.handoff.register.assert_not_awaited()
    case.handoff.retained.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


async def test_preflight_deadline_bounds_source_acquisition(coordinator, capture_case):
    case = capture_case
    case.handoff.preflight.side_effect = lambda *_args: asyncio.get_running_loop().time() + 1
    assert (
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    ).status == "no_change"
    assert all(0 < call.kwargs["timeout_seconds"] <= 1 for call in case.acquire.await_args_list)


async def test_expired_preflight_deadline_cannot_open_source(coordinator, capture_case):
    case = capture_case
    case.handoff.preflight.side_effect = lambda *_args: asyncio.get_running_loop().time() - 1
    with pytest.raises(case.failure, match="candidate_failed"):
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    case.acquire.assert_not_awaited()
    case.handoff.register.assert_not_awaited()
    case.handoff.retained.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.parametrize("resume", (False, True))
async def test_bound_handoff_uses_only_remote_retained_originals(coordinator, capture_case, monkeypatch, resume):
    case = capture_case

    def forbidden_provider(*_arguments, **_options):
        pytest.fail("bound replay must not construct a source provider")

    monkeypatch.setattr(object_candidate, "FixedLocalS3CredentialProvider", forbidden_provider)
    monkeypatch.setattr(upload, "StagedUploadProvider", forbidden_provider)
    if not resume:
        coordinator.mocks_by_name["reserve_execution"].return_value = ExecutionSubmission(17, "running", False, 23)
        coordinator.mocks_by_name["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    result = await case.run(coordinator.session_factory, case.request, resume=resume, capture_handoff=case.handoff)
    assert result.status == "no_change"
    case.acquire.assert_not_awaited()
    case.handoff.preflight.assert_not_awaited()
    case.handoff.register.assert_not_awaited()
    case.handoff.retained.assert_awaited_once_with(case.request, replace(coordinator.grant, fence=2), 23)


async def test_uncertain_registration_does_not_finish_or_refetch_and_bound_resume_recovers(coordinator, capture_case):
    case = capture_case
    case.handoff.register.side_effect = TimeoutError("synthetic lost acknowledgement")
    with pytest.raises(case.failure, match="candidate_failed"):
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    assert case.acquire.await_count == len(case.request.objects)
    case.handoff.register.assert_awaited_once()
    case.handoff.retained.assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()

    case.acquire.reset_mock()
    result = await case.run(coordinator.session_factory, case.request, resume=True, capture_handoff=case.handoff)
    assert result.status == "no_change"
    case.acquire.assert_not_awaited()
    case.handoff.register.assert_awaited_once()
    case.handoff.preflight.assert_awaited_once()
    case.handoff.retained.assert_awaited_once_with(case.request, replace(coordinator.grant, fence=2), 23)
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()


async def test_registration_cancellation_does_not_finish_or_refetch(coordinator, capture_case):
    case = capture_case
    case.handoff.register.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await case.run(coordinator.session_factory, case.request, capture_handoff=case.handoff)
    assert case.acquire.await_count == len(case.request.objects)
    case.handoff.register.assert_awaited_once()
    case.handoff.retained.assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.parametrize("cap", (None, True, -1, 1.5, 2**63))
async def test_raw_parquet_handoff_requires_bounded_retention_before_claim(coordinator, cap):
    handoff = SimpleNamespace(preflight=AsyncMock())
    request = replace(coordinator.request, maximum_dataset_retained_bytes=cap)
    with pytest.raises(https_candidate.HttpsCandidateError, match="candidate_failed"):
        await https_candidate.run_https_candidate(coordinator.session_factory, request, capture_handoff=handoff)
    assert not coordinator.sessions
    handoff.preflight.assert_not_awaited()
    coordinator.acquire.assert_not_awaited()


def test_raw_parquet_identity_binds_retention_without_changing_legacy_validation(coordinator):
    legacy = coordinator.request
    assert https_candidate.https_request_identity_sha256(legacy) == https_candidate.https_request_identity_sha256(
        legacy, raw_originals=False
    )
    raw = replace(legacy, maximum_dataset_retained_bytes=1024 * 1024)
    with pytest.raises(https_candidate.HttpsCandidateError, match="https_request_invalid"):
        https_candidate.https_request_identity_sha256(raw)
    identity = https_candidate.https_request_identity_sha256(raw, raw_originals=True)
    assert identity != https_candidate.https_request_identity_sha256(
        replace(raw, maximum_dataset_retained_bytes=raw.maximum_dataset_retained_bytes + 1), raw_originals=True
    )
    assert identity == https_candidate.https_request_identity_sha256(
        replace(raw, lease_token="synthetic-replacement", objects=raw.objects[::-1]), raw_originals=True
    )


async def test_none_handoff_keeps_legacy_registration_and_readback(coordinator):
    result = await https_candidate.run_https_candidate(
        coordinator.session_factory, coordinator.request, capture_handoff=None
    )
    assert result.status == "no_change"
    coordinator.mocks_by_name["require_raw_capture_authority"].assert_not_awaited()
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_awaited_once()
    coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_awaited_once()
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].assert_awaited_once()
