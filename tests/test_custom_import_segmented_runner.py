# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-claim composition, immutable bounds and ordinary finality outcomes."""

from __future__ import annotations

import datetime as dt
from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process.custom_import import build_output as output
from process.custom_import import execution as lifecycle
from process.custom_import import operator as operator_evidence
from process.custom_import import scalar_digest
from process.custom_import import snowflake_operator_cli as operator_cli
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.snowflake_capture import SnowflakeCaptureResult
from tests.test_custom_import_build_graph import _registry
from tests.test_custom_import_build_output import _generation
from tests.test_custom_import_build_source import _request as _source_request
from tests.test_custom_import_execution import _SyntheticSession
from tests.test_custom_import_output_bulk_verification import (
    _digests,
    _projection_records,
    _projection_responses,
    _read_session,
    _twenty_projection_family,
)
from tests.test_custom_import_processing_policy import _policy_document


def _policy():
    return ProcessingPolicy.from_mapping(_policy_document())


def test_deadline_is_sealed_time_not_retry_time():
    sealed = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    expected = sealed + dt.timedelta(seconds=300)
    assert runner._build_deadline(sealed, sealed, 300) == expected
    assert runner._build_deadline(sealed, sealed + dt.timedelta(seconds=299), 300) == expected
    with pytest.raises(CandidateRunnerError, match="build deadline elapsed"):
        runner._build_deadline(sealed, expected, 300)


@pytest.mark.parametrize("sealed", [None, "2026-01-01", dt.datetime(2026, 1, 1)])
def test_deadline_rejects_missing_or_naive_timestamp(sealed):
    with pytest.raises(CandidateRunnerError):
        runner._build_deadline(sealed, dt.datetime(2026, 1, 1, tzinfo=dt.UTC), 300)


def test_deadline_rejects_future_and_overflow():
    now = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    with pytest.raises(CandidateRunnerError, match="future"):
        runner._build_deadline(now + dt.timedelta(seconds=1), now, 300)
    maximum = dt.datetime.max.replace(tzinfo=dt.UTC)
    with pytest.raises(CandidateRunnerError, match="overflows"):
        runner._build_deadline(maximum, maximum, 300)


def _install_flow(monkeypatch, *, capture_status="capture_sealed", no_change=False):
    from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
    from tests.test_custom_import_snowflake_capture import _Harness

    capture_harness = _Harness(monkeypatch)
    request = replace(
        capture_harness.request,
        bundle_request=replace(capture_harness.request.bundle_request, processing_policy=_policy()),
        source_binding_revision_id=4,
        source_binding_sha256=b"x" * 32,
    )
    assert isinstance(request, SnowflakeBundleCandidateRequest)
    build_request = object()
    output = SimpleNamespace(generation_id=17, seal=object(), no_change=object() if no_change else None)
    calls_by_name = {
        "acquire_segmented_snowflake_capture": AsyncMock(return_value=SnowflakeCaptureResult(capture_status, 9, 23, 7)),
        "_build_request": AsyncMock(return_value=build_request),
        "stage_segmented_source": AsyncMock(return_value=SimpleNamespace(build_id=19)),
        "count_source_outcomes": AsyncMock(return_value=runner.SourceOutcomeCounts(2, 3)),
        "build_graph": AsyncMock(return_value=17),
        "build_output": AsyncMock(return_value=output),
        "_activate": AsyncMock(return_value=object()),
        "_finish": AsyncMock(return_value="candidate_rejected"),
    }
    for name, call in calls_by_name.items():
        monkeypatch.setattr(runner, name, call)
    return SimpleNamespace(request=request, connector=capture_harness.builder, calls=calls_by_name, output=output)


async def _run(flow):
    return await runner.run_segmented_snowflake_candidate(
        object(), flow.connector, flow.request, processing_policy=_policy()
    )


@pytest.mark.parametrize("capture_status", ["capture_sealed", "capture_bound"])
async def test_single_capture_claim_is_carried_to_all_build_stages(monkeypatch, capture_status):
    flow = _install_flow(monkeypatch, capture_status=capture_status)
    result = await _run(flow)
    assert (result.status, result.execution_id, result.generation_id) == ("activated", 9, 17)
    assert (result.accepted_family_count, result.rejection_count) == (2, 3)
    captured = flow.calls["_build_request"].await_args.args[2]
    assert captured.fence == 7 and captured.capture_bundle_id == 23
    build_request = flow.calls["_build_request"].return_value
    for name in ("stage_segmented_source", "count_source_outcomes", "build_graph", "build_output", "_activate"):
        assert flow.calls[name].await_args.args[1] is build_request
    flow.calls["acquire_segmented_snowflake_capture"].assert_awaited_once()
    flow.calls["_finish"].assert_not_awaited()


@pytest.mark.parametrize("status", ["not_claimed", "lease_lost", "canceled"])
async def test_unavailable_capture_never_enters_build(monkeypatch, status):
    flow = _install_flow(monkeypatch, capture_status=status)
    assert (await _run(flow)).status == status
    flow.calls["_build_request"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


async def test_no_change_does_not_activate_again(monkeypatch):
    flow = _install_flow(monkeypatch, no_change=True)
    result = await _run(flow)
    assert result.status == "no_change" and result.publication is flow.output.no_change
    flow.calls["_activate"].assert_not_awaited()


async def test_pointer_conflict_retains_sealed_candidate(monkeypatch):
    flow = _install_flow(monkeypatch)
    flow.calls["_activate"].return_value = None
    result = await _run(flow)
    assert result.status == "sealed_unpublished" and result.seal is flow.output.seal


async def test_candidate_rejection_finishes_without_output(monkeypatch):
    flow = _install_flow(monkeypatch)
    flow.calls["build_graph"].return_value = None
    result = await _run(flow)
    assert result.status == "candidate_rejected" and result.rejection_count == 3
    assert flow.calls["_finish"].await_args.args[-1] == "failed"
    flow.calls["build_output"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


@pytest.mark.parametrize(
    "stage", ["_build_request", "stage_segmented_source", "count_source_outcomes", "build_graph", "build_output"]
)
@pytest.mark.parametrize("error", [CancellationRequested, LeaseAuthorityLost])
async def test_authority_failure_is_not_publication(monkeypatch, stage, error):
    flow = _install_flow(monkeypatch)
    flow.calls[stage].side_effect = error("synthetic authority failure")
    flow.calls["_finish"].return_value = "canceled"
    result = await _run(flow)
    assert result.status == ("canceled" if error is CancellationRequested else "lease_lost")
    flow.calls["_activate"].assert_not_awaited()
    if error is CancellationRequested:
        assert flow.calls["_finish"].await_args.args[-1] == "canceled"
    else:
        flow.calls["_finish"].assert_not_awaited()


@pytest.mark.parametrize("token", [bytearray(b"synthetic"), memoryview(b"synthetic")])
async def test_mutable_lease_token_is_snapshotted_before_capture(monkeypatch, token):
    flow = _install_flow(monkeypatch, capture_status="not_claimed")
    flow.request = replace(flow.request, lease_token=token)
    await _run(flow)
    assert type(flow.calls["acquire_segmented_snowflake_capture"].await_args.args[1].lease_token) is bytes


async def test_unknown_storage_error_is_not_relabelled_as_lease_loss(monkeypatch):
    flow = _install_flow(monkeypatch)
    flow.calls["build_output"].side_effect = RuntimeError("synthetic storage failure")
    with pytest.raises(RuntimeError, match="storage failure"):
        await _run(flow)
    flow.calls["_finish"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


async def test_writer_transports_is_checked_before_capture_and_forwarded_unchanged(monkeypatch):
    flow = _install_flow(monkeypatch)
    transport = SimpleNamespace(require_candidate_binding=Mock())
    await runner.run_segmented_snowflake_candidate(
        object(),
        flow.connector,
        flow.request,
        processing_policy=_policy(),
        writer_transports=transport,
    )
    transport.require_candidate_binding.assert_called_once_with(flow.request)
    for stage in ("_build_request", "stage_segmented_source"):
        assert flow.calls[stage].await_args.kwargs == {"writer_transports": transport}


async def test_invalid_admission_launch_does_not_claim_capture_or_finish_execution(monkeypatch):
    from process.custom_import.admission_worker import AdmissionTransportError

    flow = _install_flow(monkeypatch)
    transport = SimpleNamespace(require_candidate_binding=Mock(side_effect=AdmissionTransportError("unavailable")))
    with pytest.raises(AdmissionTransportError):
        await runner.run_segmented_snowflake_candidate(
            object(),
            flow.connector,
            flow.request,
            processing_policy=_policy(),
            writer_transports=transport,
        )
    flow.calls["acquire_segmented_snowflake_capture"].assert_not_awaited()
    flow.calls["_finish"].assert_not_awaited()


async def test_uncertain_admission_response_never_finishes_or_publishes(monkeypatch):
    from process.custom_import.admission_worker import AdmissionTransportError

    flow = _install_flow(monkeypatch)
    flow.calls["stage_segmented_source"].side_effect = AdmissionTransportError("synthetic uncertain response")
    transport = SimpleNamespace(require_candidate_binding=Mock())
    with pytest.raises(AdmissionTransportError):
        await runner.run_segmented_snowflake_candidate(
            object(), flow.connector, flow.request, processing_policy=_policy(), writer_transports=transport
        )
    for stage in ("_finish", "count_source_outcomes", "build_graph", "build_output", "_activate"):
        flow.calls[stage].assert_not_awaited()


def _transaction_mock(monkeypatch, session):
    context = Mock(side_effect=lambda *_args, **_kwargs: nullcontext(session))
    monkeypatch.setattr(runner, "_session", context)
    monkeypatch.setattr(runner, "_set_timeout", AsyncMock())
    return context


def _sealed_capture(request, captured, policy):
    return SimpleNamespace(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        producing_execution_id=captured.execution_id,
        capture_state="sealed",
        payload_contract=runner.SEGMENTED_PAYLOAD_CONTRACT,
        canonical_policy=policy.capture.canonical,
        policy_sha256=bytes.fromhex(policy.capture.digest),
        source_binding_revision_id=request.source_binding_revision_id,
        source_binding_sha256=request.source_binding_sha256,
        sealed_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        (None, None),
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("producing_execution_id", 99),
        ("capture_state", "pending"),
        ("payload_contract", "other-contract"),
        ("canonical_policy", "{}"),
        ("policy_sha256", b"y" * 32),
        ("source_binding_revision_id", 99),
        ("source_binding_sha256", b"y" * 32),
    ],
)
async def test_build_request_denies_unbound_capture(monkeypatch, field, replacement):
    build_request = runner._build_request
    flow = _install_flow(monkeypatch)
    policy = _policy()
    captured = flow.calls["acquire_segmented_snowflake_capture"].return_value
    bundle = _sealed_capture(flow.request, captured, policy)
    if field is not None:
        setattr(bundle, field, replacement)
    capture_row = None if field is None else (bundle, bundle.sealed_at)
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: capture_row)))
    _transaction_mock(monkeypatch, session)
    lock_page, retained_base = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runner, "_lock_page", lock_page)
    monkeypatch.setattr(runner, "_retained_base", retained_base)
    with pytest.raises(CandidateRunnerError, match="sealed capture"):
        await build_request(object(), flow.request, captured, policy)
    lock_page.assert_not_awaited()
    retained_base.assert_not_awaited()


async def test_expired_admission_stage_denies_before_initial_page_lock(monkeypatch):
    from process.custom_import.admission_worker import AdmissionTransportError

    build_request = runner._build_request
    flow = _install_flow(monkeypatch)
    policy = _policy()
    captured = flow.calls["acquire_segmented_snowflake_capture"].return_value
    bundle = _sealed_capture(flow.request, captured, policy)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: (bundle, bundle.sealed_at)))
    )
    _transaction_mock(monkeypatch, session)
    lock_page, retained_base = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runner, "_lock_page", lock_page)
    monkeypatch.setattr(runner, "_retained_base", retained_base)
    admission = SimpleNamespace(bind_request=Mock(side_effect=AdmissionTransportError("expired")))
    source = SimpleNamespace(bind_request=Mock())
    transport = SimpleNamespace(admission=admission, source=source)
    with pytest.raises(AdmissionTransportError):
        await build_request(object(), flow.request, captured, policy, writer_transports=transport)
    admission.bind_request.assert_called_once()
    source.bind_request.assert_not_called()
    lock_page.assert_not_awaited()
    retained_base.assert_not_awaited()


@pytest.mark.parametrize("expired", [False, True])
async def test_writer_expiry_stays_at_boundaries(monkeypatch, expired):
    """Keep the early permit fence without shortening unrelated build phases."""
    build_request = runner._build_request
    flow = _install_flow(monkeypatch)
    policy = _policy()
    captured = flow.calls["acquire_segmented_snowflake_capture"].return_value
    bundle = _sealed_capture(flow.request, captured, policy)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: (bundle, bundle.sealed_at)))
    )
    _transaction_mock(monkeypatch, session)
    expires_at = bundle.sealed_at + dt.timedelta(seconds=30)

    def bind_writer_expiry(request):
        return replace(request, authorization_expires_at=expires_at)

    transport = SimpleNamespace(
        require_candidate_binding=Mock(),
        admission=SimpleNamespace(bind_request=Mock(side_effect=bind_writer_expiry)),
        source=SimpleNamespace(bind_request=Mock(side_effect=bind_writer_expiry)),
    )
    lock_page = AsyncMock(side_effect=LeaseAuthorityLost("writer expired") if expired else None)
    retained_base = AsyncMock(side_effect=lambda session, request: request)
    monkeypatch.setattr(runner, "_lock_page", lock_page)
    monkeypatch.setattr(runner, "_retained_base", retained_base)
    monkeypatch.setattr(runner, "_build_request", build_request)
    candidate_result = await runner.run_segmented_snowflake_candidate(
        object(), flow.connector, flow.request, processing_policy=policy, writer_transports=transport
    )
    assert lock_page.await_args.args[1].authorization_expires_at == expires_at
    transport.admission.bind_request.assert_called_once()
    transport.source.bind_request.assert_called_once()
    stages = ("stage_segmented_source", "count_source_outcomes", "build_graph", "build_output", "_activate")
    if expired:
        assert candidate_result.status == "lease_lost"
        retained_base.assert_not_awaited()
        for stage in stages:
            flow.calls[stage].assert_not_awaited()
    else:
        assert candidate_result.status == "activated"
        retained_request = retained_base.await_args.args[1]
        assert retained_request.authorization_expires_at is None
        assert lock_page.await_args.args[1] == replace(retained_request, authorization_expires_at=expires_at)
        for stage in stages:
            assert flow.calls[stage].await_args.args[1] is retained_request


@pytest.mark.parametrize(("generation_id", "version"), [(None, 0), (17, 3)])
async def test_same_fence_preserves_original_base(monkeypatch, generation_id, version):
    request = _source_request(fence=7)
    build = SimpleNamespace(base_generation_id=generation_id, base_pointer_version=version)
    session = SimpleNamespace(scalars=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: build)))
    pointer = AsyncMock(return_value=SimpleNamespace(generation_id=99, version=12))
    monkeypatch.setattr(runner, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(runner, "load_current_pointer", pointer)
    retained = await runner._retained_base(session, request)
    assert retained == replace(request, expected_base_generation_id=generation_id, expected_pointer_version=version)
    assert session.scalars.await_args.args[0].compile().params == {
        "execution_id_1": request.execution_id,
        "producing_fence_1": request.fence,
    }
    pointer.assert_not_awaited()


@pytest.mark.parametrize(("acknowledged", "status"), [("canceled", "canceled"), ("running", "lease_lost")])
async def test_cancellation_wins_candidate_rejection(monkeypatch, acknowledged, status):
    finish = runner._finish
    flow = _install_flow(monkeypatch)
    monkeypatch.setattr(runner, "_finish", finish)
    flow.calls["build_graph"].return_value = None
    session = object()
    context = _transaction_mock(monkeypatch, session)
    transition = AsyncMock(
        side_effect=[
            SimpleNamespace(changed=False, state="canceling"),
            SimpleNamespace(changed=acknowledged == "canceled", state=acknowledged),
        ]
    )
    monkeypatch.setattr(runner, "finish_execution", transition)
    outcome = await _run(flow)
    assert outcome.status == status and outcome.generation_id is None
    assert transition.await_args_list == [
        call(
            session,
            execution_id=9,
            fence=7,
            token=flow.request.lease_token,
            terminal_state=terminal_state,
            terminal_reason=reason,
        )
        for terminal_state, reason in [("failed", "candidate_rejected"), ("canceled", "cancellation_requested")]
    ]
    assert context.call_count == 2
    flow.calls["build_output"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


async def test_activation_conflict_preserves_pinned_compare_and_swap(monkeypatch):
    activate = runner._activate
    flow = _install_flow(monkeypatch)
    monkeypatch.setattr(runner, "_activate", activate)
    request = _source_request(expected_base_generation_id=41, expected_pointer_version=5)
    flow.calls["_build_request"].return_value = request
    session = object()
    _transaction_mock(monkeypatch, session)
    publication = AsyncMock(side_effect=runner.PublicationConflict("synthetic moved pointer"))
    monkeypatch.setattr(runner, "activate_generation", publication)
    outcome = await _run(flow)
    assert outcome.status == "sealed_unpublished" and outcome.seal is flow.output.seal
    assert outcome.generation_id == flow.output.generation_id and outcome.publication is None
    publication.assert_awaited_once_with(
        session,
        dataset_id=request.dataset_id,
        target_generation_id=flow.output.generation_id,
        expected_generation_id=41,
        expected_pointer_version=5,
    )
    flow.calls["_finish"].assert_not_awaited()


@pytest.mark.parametrize("invalid", ["policy", "source_binding_revision_id", "source_binding_sha256"])
async def test_invalid_configuration_denied_before_capture(monkeypatch, invalid):
    flow = _install_flow(monkeypatch)
    policy = _policy()
    if invalid == "policy":
        policy = policy.to_mapping()
    else:
        flow.request = replace(flow.request, **{invalid: None})
    with pytest.raises(CandidateRunnerError):
        await runner.run_segmented_snowflake_candidate(object(), flow.connector, flow.request, processing_policy=policy)
    flow.calls["acquire_segmented_snowflake_capture"].assert_not_awaited()
    flow.calls["stage_segmented_source"].assert_not_awaited()


async def _claimed_validation_flow(monkeypatch, *, stage="_build_request"):
    """Reuse real claim and finish rules around the isolated coordinator flow."""

    finish = runner._finish
    flow = _install_flow(monkeypatch, capture_status="capture_bound")
    session = _SyntheticSession()
    monkeypatch.setattr(lifecycle, "_database_now", AsyncMock(side_effect=lambda session: session.now))
    submission = await lifecycle.create_execution(
        session,
        dataset_id=flow.request.dataset_id,
        definition_revision_id=flow.request.definition_revision_id,
        schema_revision_id=flow.request.schema_revision_id,
        idempotency_key=flow.request.idempotency_key,
        mechanism="local",
        capture_bundle_id=23,
    )
    grant = await lifecycle.claim_execution(
        session, execution_id=submission.execution_id, token=flow.request.lease_token
    )
    captured = SnowflakeCaptureResult("capture_bound", submission.execution_id, 23, grant.fence)
    flow.calls["acquire_segmented_snowflake_capture"].return_value = captured
    flow.calls[stage].side_effect = CandidateRunnerError("synthetic private captured build detail")
    monkeypatch.setattr(runner, "_finish", finish)
    _transaction_mock(monkeypatch, session)
    return flow, session, captured


async def test_expired_capture_finishes_and_cannot_resume_again(monkeypatch):
    build_request = runner._build_request
    flow, session, captured = await _claimed_validation_flow(monkeypatch)
    policy = _policy()
    bundle = _sealed_capture(flow.request, captured, policy)
    bundle.sealed_at = session.now - dt.timedelta(seconds=policy.build.build_deadline_seconds)
    capture_session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: (bundle, session.now)))
    )
    context = _transaction_mock(monkeypatch, session)
    context.side_effect = [nullcontext(capture_session), nullcontext(session)]
    monkeypatch.setattr(runner, "_build_request", build_request)
    lock_page = AsyncMock()
    monkeypatch.setattr(runner, "_lock_page", lock_page)

    with pytest.raises(CandidateRunnerError, match="^segmented build failed$"):
        await _run(flow)

    execution = session.executions[captured.execution_id]
    lease = session.leases[captured.execution_id]
    assert execution.state == "failed" and execution.finished_at == session.now
    assert execution.terminal_reason == "segmented_build_failed" and execution.capture_bundle_id == 23
    assert operator_evidence._failure_class(execution.state, execution.terminal_reason) == "other_failed"
    assert lease.fence == captured.fence and lease.expires_at == session.now
    for _attempt in range(2):
        assert (
            await lifecycle.resume_execution(session, execution_id=execution.execution_id, token=b"synthetic-next")
            is None
        )
    assert lease.fence == captured.fence
    lock_page.assert_not_awaited()
    flow.calls["stage_segmented_source"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


@pytest.mark.parametrize(
    "stage", ["_build_request", "stage_segmented_source", "count_source_outcomes", "build_graph", "build_output"]
)
async def test_post_capture_validation_finishes_current_owner(monkeypatch, stage):
    flow, session, captured = await _claimed_validation_flow(monkeypatch, stage=stage)
    with pytest.raises(CandidateRunnerError, match="^segmented build failed$") as caught:
        await _run(flow)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    execution = session.executions[captured.execution_id]
    assert execution.state == "failed" and execution.finished_at == session.now
    assert execution.terminal_reason == "segmented_build_failed"
    assert operator_evidence._failure_class(execution.state, execution.terminal_reason) == "other_failed"
    lease = session.leases[captured.execution_id]
    assert lease.fence == captured.fence and lease.expires_at == session.now
    assert lease.token_sha256 == lifecycle.lease_token_sha256(flow.request.lease_token)
    flow.calls["_activate"].assert_not_awaited()


@pytest.mark.parametrize("adapter", ["encoder", "verifier"])
@pytest.mark.parametrize("error_type", [ValueError, TypeError, OverflowError])
async def test_native_scalar_rejection_durably_fails_without_activation(monkeypatch, adapter, error_type):
    flow, session, captured = await _claimed_validation_flow(monkeypatch, stage="build_output")
    request, family = _twenty_projection_family(child=False)
    projection_records = _projection_records(request, [family], child=False)
    read_session = _read_session(monkeypatch, _projection_responses(request, projection_records, child=False))
    native = Mock(side_effect=error_type("synthetic private scalar detail"))
    fallback = Mock(side_effect=AssertionError("native failure must not fall back"))
    monkeypatch.setattr(scalar_digest, "native_verifier", lambda: native if adapter == "verifier" else None)
    monkeypatch.setattr(scalar_digest, "native_encoder", lambda: native if adapter == "encoder" else fallback)
    digests = _digests()
    flow.calls["build_output"].side_effect = lambda *_args: output._scalar_material(
        read_session, request, _registry(request.definition), 7, _generation(request), digests, child=False
    )

    with pytest.raises(CandidateRunnerError, match="^segmented build failed$") as caught:
        await _run(flow)

    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    execution = session.executions[captured.execution_id]
    assert execution.state == "failed" and execution.finished_at == session.now
    assert execution.terminal_reason == "segmented_build_failed"
    lease = session.leases[captured.execution_id]
    assert lease.fence == captured.fence and lease.expires_at == session.now
    assert (
        await lifecycle.resume_execution(session, execution_id=execution.execution_id, token=b"synthetic-next") is None
    )
    assert read_session.transactions == read_session.closed
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]
    native.assert_called_once()
    fallback.assert_not_called()
    flow.calls["_activate"].assert_not_awaited()


async def test_graph_rejection_retains_candidate_failure_class(monkeypatch):
    flow, session, captured = await _claimed_validation_flow(monkeypatch, stage="build_graph")
    flow.calls["build_graph"].side_effect = None
    flow.calls["build_graph"].return_value = None

    outcome = await _run(flow)

    assert outcome.status == "candidate_rejected" and outcome.generation_id is None
    assert (outcome.accepted_family_count, outcome.rejection_count) == (2, 3)
    execution = session.executions[captured.execution_id]
    assert execution.state == "failed" and execution.finished_at == session.now
    assert execution.terminal_reason == "candidate_rejected"
    assert operator_evidence._failure_class(execution.state, execution.terminal_reason) == "candidate_rejected"
    flow.calls["build_output"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()


@pytest.mark.parametrize("command", ["execute", "resume"])
def test_operator_reports_post_capture_failure_without_private_details(monkeypatch, capsys, command):
    async def operation(**_arguments):
        flow, _session, _captured = await _claimed_validation_flow(monkeypatch, stage="build_output")
        return await _run(flow)

    operation_name = "_run_retained_snowflake_binding" if command == "execute" else "_run_resumed_snowflake_binding"
    monkeypatch.setattr(operator_cli, operation_name, operation)
    assert (
        operator_cli.run_command(
            [
                command,
                "--definition-revision-id",
                "1",
                "--source-binding-revision-id",
                "4",
                "--idempotency-key",
                "synthetic",
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert output.out == "" and output.err == '{"code":"failed","status":"error"}\n'


@pytest.mark.parametrize("authority", ["expired", "preempted", "wrong_token", "canceling_stale"])
async def test_invalid_capture_cannot_finish_stale_authority(monkeypatch, authority):
    flow, session, captured = await _claimed_validation_flow(monkeypatch)
    lease = session.leases[captured.execution_id]
    if authority == "wrong_token":
        flow.request = replace(flow.request, lease_token=b"synthetic-other")
    else:
        if authority == "canceling_stale":
            await lifecycle.request_cancellation(session, execution_id=captured.execution_id)
        session.now = lease.expires_at
        if authority == "preempted":
            takeover = await lifecycle.resume_execution(
                session, execution_id=captured.execution_id, token=b"synthetic-next"
            )
            assert takeover.fence == captured.fence + 1
    before = (lease.fence, lease.token_sha256, lease.expires_at)

    outcome = await _run(flow)

    assert outcome.status == "lease_lost" and outcome.generation_id is None
    execution = session.executions[captured.execution_id]
    assert execution.state == ("canceling" if authority == "canceling_stale" else "running")
    assert execution.finished_at is None
    assert (lease.fence, lease.token_sha256, lease.expires_at) == before
    flow.calls["_activate"].assert_not_awaited()


async def test_cancellation_wins_post_capture_validation(monkeypatch):
    flow, session, captured = await _claimed_validation_flow(monkeypatch)
    await lifecycle.request_cancellation(session, execution_id=captured.execution_id)

    outcome = await _run(flow)

    assert outcome.status == "canceled" and outcome.generation_id is None
    execution = session.executions[captured.execution_id]
    assert execution.state == "canceled" and execution.finished_at == session.now
    assert execution.terminal_reason == "cancellation_requested"
    lease = session.leases[captured.execution_id]
    assert lease.fence == captured.fence and lease.expires_at == session.now
    assert lease.token_sha256 == lifecycle.lease_token_sha256(flow.request.lease_token)
    flow.calls["_activate"].assert_not_awaited()
