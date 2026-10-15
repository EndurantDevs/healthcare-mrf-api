# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native source lifecycle parity with synthetic objects and no external I/O."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import https_candidate, object_candidate, s3, upload
from process.custom_import.execution import ExecutionSubmission
from process.custom_import.object_candidate import (
    ObjectCandidateError,
    ObjectCandidateRequest,
    object_request_identity_sha256,
    run_object_candidate,
)
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network
from tests.test_custom_import_https_originals import _original_io, _original_request


def _request(source_kind, format_name="ndjson", compression="none"):
    base, payloads = _original_request(format_name, compression)
    objects = []
    for item in base.objects:
        identity_by_key = dict(
            stream_id=item.stream_id, expected_sha256=item.expected_sha256, expected_bytes=item.expected_bytes
        )
        objects.append(
            s3.S3Object(
                **identity_by_key,
                endpoint="https://objects.example",
                region="us-east-1",
                bucket="synthetic-captures",
                key=f"originals/{item.stream_id}",
                version_id="version-one",
            )
            if source_kind == "s3"
            else upload.UploadObject(**identity_by_key, object_id=f"source_{item.stream_id}")
        )
    return ObjectCandidateRequest(source_kind=source_kind, **(vars(base) | {"objects": tuple(objects)})), payloads


def _native_io(monkeypatch, case, request, payloads):
    register, load, captures = _original_io(monkeypatch, case, request, payloads)
    acquire = AsyncMock(
        side_effect=lambda _item, stream, **_options: next(
            sealed for sealed in captures if sealed.manifest.stream_id == stream.stream_id
        )
    )
    owner, name = (s3, "acquire_s3") if request.source_kind == "s3" else (upload, "acquire_upload")
    monkeypatch.setattr(owner, name, acquire)
    return register, load, captures, acquire


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("format_name", ("csv", "tsv", "json", "ndjson", "xml", "parquet"))
@pytest.mark.parametrize("compression", ("none", "gzip"))
async def test_native_formats_bind_originals_and_use_same_family_runner(
    coordinator, monkeypatch, source_kind, format_name, compression
):
    request, payloads = _request(source_kind, format_name, compression)
    register, load, captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
    provider = SimpleNamespace()
    result = await run_object_candidate(
        coordinator.session_factory, request, credential_provider=provider, upload_provider=provider
    )
    assert result.status == "no_change"
    assert acquire.await_count == len(request.objects)
    assert register.call_args.kwargs["captures"] == captures
    assert register.call_args.kwargs["maximum_dataset_retained_bytes"] == request.maximum_dataset_retained_bytes
    bound = coordinator.mocks_by_name["bind_execution_capture_bundle"]
    assert register.call_args.args[0] is bound.call_args.args[0]
    assert register.call_args.args[0].outcome == "commit"
    assert load.call_args.args[0] is not bound.call_args.args[0]
    assert load.call_args.args[0].outcome == "commit"
    arguments = coordinator.mocks_by_name["run_claimed_candidate"].call_args.args
    assert arguments[1].roots[0]["display_name"] == "Synthetic Provider"
    assert arguments[1].children_by_collection["rates"][0]["amount"] == "12.50"
    assert len(arguments[3].families) == 1 and not arguments[3].rejections
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].assert_not_awaited()
    coordinator.acquire.assert_not_awaited()


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("resume", (False, True))
async def test_bound_native_replay_never_opens_signs_or_recharges(coordinator, monkeypatch, source_kind, resume):
    request, payloads = _request(source_kind, "csv", "gzip")
    register, load, _captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)

    def forbidden_provider(*_arguments, **_options):
        pytest.fail("bound replay must not construct a source provider")

    monkeypatch.setattr(object_candidate, "FixedLocalS3CredentialProvider", forbidden_provider)
    monkeypatch.setattr(upload, "StagedUploadProvider", forbidden_provider)
    mocks = coordinator.mocks_by_name
    if not resume:
        mocks["reserve_execution"].return_value = ExecutionSubmission(17, "running", False, 23)
        mocks["claim_execution"].return_value = replace(coordinator.grant, fence=2)
    assert (await run_object_candidate(coordinator.session_factory, request, resume=resume)).status == "no_change"
    acquire.assert_not_awaited()
    register.assert_not_awaited()
    mocks["bind_execution_capture_bundle"].assert_not_awaited()
    assert load.call_args.kwargs["capture_bundle_id"] == 23
    assert mocks["run_claimed_candidate"].call_args.args[2].fence == 2


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
def test_native_identity_is_stable_and_binds_complete_bundle_policy(source_kind):
    request, _payloads = _request(source_kind)
    identity = object_request_identity_sha256(request)
    assert len(identity) == 32
    assert identity == object_request_identity_sha256(replace(request, objects=request.objects[::-1]))
    assert identity == object_request_identity_sha256(replace(request, lease_token="new-owner"))
    assert identity == object_request_identity_sha256(replace(request, idempotency_key="new-attempt"))
    first = request.objects[0]
    for changed in (
        replace(request, dataset_id=request.dataset_id + 1),
        replace(request, definition_revision_id=request.definition_revision_id + 1),
        replace(request, schema_revision_id=request.schema_revision_id + 1),
        replace(request, source_snapshot_token="different-publication"),
        replace(request, timeout_seconds=request.timeout_seconds + 1),
        replace(request, maximum_dataset_retained_bytes=request.maximum_dataset_retained_bytes + 1),
        replace(request, limits=replace(request.limits, maximum_records=3)),
        replace(request, objects=(replace(first, expected_sha256="0" * 64), *request.objects[1:])),
    ):
        assert identity != object_request_identity_sha256(changed)


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("cap", (None, True, -1, 1.5, 2**63))
async def test_native_cap_rejected_before_database_or_provider_access(coordinator, source_kind, cap):
    request, _payloads = _request(source_kind, "parquet")
    with pytest.raises(ObjectCandidateError, match="^object_candidate_failed$"):
        await run_object_candidate(coordinator.session_factory, replace(request, maximum_dataset_retained_bytes=cap))
    assert not coordinator.sessions


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
@pytest.mark.parametrize("invalid_request_kind", ("request_type", "source_kind", "objects_type", "object_type"))
async def test_invalid_native_bundle_is_rejected_before_database_or_source_access(
    coordinator, monkeypatch, source_kind, invalid_request_kind
):
    request, _payloads = _request(source_kind)
    other_request, _other_payloads = _request("upload" if source_kind == "s3" else "s3")
    invalid_requests_by_kind = {
        "request_type": None,
        "source_kind": replace(request, source_kind="unsupported"),
        "objects_type": replace(request, objects=list(request.objects)),
        "object_type": replace(request, objects=other_request.objects),
    }
    invalid_request = invalid_requests_by_kind[invalid_request_kind]
    acquire = AsyncMock(side_effect=AssertionError("unexpected source access"))
    monkeypatch.setattr(object_candidate, "_acquire_object", acquire)
    with pytest.raises(ObjectCandidateError, match="^object_request_invalid$") as identity_error:
        object_request_identity_sha256(invalid_request)
    with pytest.raises(ObjectCandidateError, match="^object_candidate_failed$") as candidate_error:
        await run_object_candidate(coordinator.session_factory, invalid_request)
    assert identity_error.value.__cause__ is None and candidate_error.value.__cause__ is None
    assert not coordinator.sessions
    acquire.assert_not_awaited()


@pytest.mark.parametrize("invalid_argument", ("session_factory", "resume"))
async def test_invalid_native_execution_arguments_do_not_open_database_or_source(
    coordinator, monkeypatch, invalid_argument
):
    request, _payloads = _request("s3")
    session_factory = None if invalid_argument == "session_factory" else coordinator.session_factory
    resume = 1 if invalid_argument == "resume" else False
    acquire = AsyncMock(side_effect=AssertionError("unexpected source access"))
    monkeypatch.setattr(object_candidate, "_acquire_object", acquire)
    with pytest.raises(ObjectCandidateError, match="^object_candidate_failed$") as caught:
        await run_object_candidate(session_factory, request, resume=resume)
    assert caught.value.__cause__ is None
    assert not coordinator.sessions
    acquire.assert_not_awaited()


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
async def test_native_partial_acquisition_never_registers_or_publishes(coordinator, monkeypatch, source_kind):
    request, payloads = _request(source_kind)
    register, load, captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
    acquire.side_effect = [captures[0], ValueError("private-source-marker")]
    with pytest.raises(ObjectCandidateError, match="^object_candidate_failed$") as raised:
        await run_object_candidate(
            coordinator.session_factory, request, credential_provider=object(), upload_provider=object()
        )
    assert raised.value.__cause__ is None
    register.assert_not_awaited()
    load.assert_not_awaited()
    coordinator.mocks_by_name["finish_execution"].assert_awaited_once()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
async def test_native_binding_failure_rolls_back_complete_registration(coordinator, monkeypatch, source_kind):
    request, payloads = _request(source_kind)
    register, load, _captures, _acquire = _native_io(monkeypatch, coordinator, request, payloads)
    coordinator.mocks_by_name["bind_execution_capture_bundle"].return_value = None
    result = await run_object_candidate(
        coordinator.session_factory, request, credential_provider=object(), upload_provider=object()
    )
    assert result.status == "lease_lost"
    assert register.call_args.args[0].outcome == "rollback"
    load.assert_not_awaited()


@pytest.mark.parametrize("source_kind", ("s3", "upload"))
async def test_native_cancellation_propagates_before_any_registration(coordinator, monkeypatch, source_kind):
    request, payloads = _request(source_kind)
    register, load, _captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
    acquire.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await run_object_candidate(
            coordinator.session_factory, request, credential_provider=object(), upload_provider=object()
        )
    register.assert_not_awaited()
    load.assert_not_awaited()
    assert all(not session.is_active for session in coordinator.sessions)
