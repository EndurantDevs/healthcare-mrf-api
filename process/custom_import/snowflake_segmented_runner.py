# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compose retained-policy capture and bounded family builds under one claim."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

from sqlalchemy import func, select
from sqlalchemy.orm import load_only

from db.models.custom_import import CustomImportBuildAttempt, CustomImportCaptureBundle, CustomImportExecution
from process.custom_import.build_counts import SourceOutcomeCounts, count_source_outcomes
from process.custom_import.build_graph import _session, build_graph
from process.custom_import.build_output import build_output
from process.custom_import.build_source import (
    SourceBuildRequest,
    _lock_page,
    _prepare_statement,
    _set_timeout,
    stage_segmented_source,
)
from process.custom_import.capture_pending import SEGMENTED_PAYLOAD_CONTRACT
from process.custom_import.execution import finish_execution
from process.custom_import.failure_diagnostics import FailurePhase, failure_identifiers, failure_phase
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.publication import PublicationConflict, activate_generation
from process.custom_import.runner_registry import load_current_pointer
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRunnerError,
    CandidateRunResult,
    LeaseAuthorityLost,
)
from process.custom_import.snowflake_candidate import bundle_request_identity_sha256
from process.custom_import.snowflake_capture import (
    acquire_segmented_snowflake_capture,
    segmented_bundle_request_identity_sha256,
)


def configured_request_identity(request, statement, *, source_binding_sha256, processing_policy=None):
    """Keep legacy identity byte-exact; pin all opted-in bounds through the binding."""

    if request.processing_policy != processing_policy:
        raise CandidateRunnerError("request processing policy differs from the retained policy")
    if processing_policy is None:
        return bundle_request_identity_sha256(request, statement, source_binding_sha256=source_binding_sha256)
    policy = _validated_policy(processing_policy)
    return segmented_bundle_request_identity_sha256(
        request, statement, policy.capture, source_binding_sha256=source_binding_sha256
    )


def _validated_policy(policy):
    if not isinstance(policy, ProcessingPolicy):
        raise CandidateRunnerError("segmented execution requires a retained processing policy")
    return ProcessingPolicy.from_mapping(policy.to_mapping())


def _build_deadline(sealed_at, now, seconds):
    """Derive an immutable deadline, never a fresh retry-relative time window."""

    if not isinstance(sealed_at, dt.datetime) or sealed_at.tzinfo is None or sealed_at.utcoffset() is None:
        raise CandidateRunnerError("sealed capture timestamp is unavailable")
    if sealed_at > now:
        raise CandidateRunnerError("sealed capture timestamp is in the future")
    try:
        deadline = sealed_at + dt.timedelta(seconds=seconds)
    except OverflowError as exc:
        raise CandidateRunnerError("sealed capture deadline overflows") from exc
    if deadline <= now:
        raise CandidateRunnerError("sealed capture build deadline elapsed")
    return deadline


async def _build_request(session_factory, request, captured, policy, *, writer_transports=None):
    """Read one immutable capture, then retain the same-fence base and bounds."""

    async with _session(session_factory, transaction=True) as session:
        await _set_timeout(session, policy.build.statement_timeout_ms)
        capture_row = (
            await session.execute(
                select(CustomImportCaptureBundle, func.clock_timestamp())
                .join(
                    CustomImportExecution,
                    CustomImportExecution.capture_bundle_id == CustomImportCaptureBundle.capture_bundle_id,
                )
                .where(
                    CustomImportCaptureBundle.capture_bundle_id == captured.capture_bundle_id,
                    CustomImportExecution.execution_id == captured.execution_id,
                )
                .options(load_only(CustomImportCaptureBundle.sealed_at, *_capture_columns()))
            )
        ).one_or_none()
        if capture_row is None:
            raise CandidateRunnerError("sealed capture is unavailable")
        bundle, now = capture_row
        _require_capture(request, captured, policy, bundle)
        deadline = _build_deadline(bundle.sealed_at, now, policy.build.build_deadline_seconds)
        bounds = policy.build.to_mapping()
        bounds.pop("build_deadline_seconds")
        build_request = SourceBuildRequest(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=captured.execution_id,
            lease_token=request.lease_token,
            fence=captured.fence,
            definition=request.definition,
            expected_base_generation_id=None,
            expected_pointer_version=0,
            complete_scope=True,
            build_deadline_at=deadline,
            **bounds,
        )
        # Writer expiry bounds the initial lock, not subsequent build phases.
        lock_request = build_request
        if writer_transports is not None:
            lock_request = writer_transports.source.bind_request(
                writer_transports.admission.bind_request(build_request)
            )
        await _lock_page(session, lock_request)
        return await _retained_base(session, build_request)


def _require_capture(request, captured, policy, bundle):
    """Do not promote a pending, foreign or differently configured capture to scope proof."""

    expected_by_field = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "producing_execution_id": captured.execution_id,
        "capture_state": "sealed",
        "payload_contract": SEGMENTED_PAYLOAD_CONTRACT,
        "canonical_policy": policy.capture.canonical,
        "policy_sha256": bytes.fromhex(policy.capture.digest),
        "source_binding_revision_id": request.source_binding_revision_id,
        "source_binding_sha256": request.source_binding_sha256,
    }
    if any(getattr(bundle, name) != value for name, value in expected_by_field.items()):
        raise CandidateRunnerError("sealed capture differs from the configured execution")


def _capture_columns():
    return tuple(
        getattr(CustomImportCaptureBundle, name)
        for name in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "producing_execution_id",
            "capture_state",
            "payload_contract",
            "canonical_policy",
            "policy_sha256",
            "source_binding_revision_id",
            "source_binding_sha256",
        )
    )


async def _retained_base(session, request):
    """Resume a build's captured pointer; only a new fence may select a new base."""

    await _prepare_statement(session)
    build = (
        await session.scalars(
            select(CustomImportBuildAttempt).where(
                CustomImportBuildAttempt.execution_id == request.execution_id,
                CustomImportBuildAttempt.producing_fence == request.fence,
            )
        )
    ).one_or_none()
    if build is not None:
        return replace(
            request,
            expected_base_generation_id=build.base_generation_id,
            expected_pointer_version=build.base_pointer_version,
        )
    await _prepare_statement(session)
    pointer = await load_current_pointer(session, request.dataset_id)
    return replace(
        request,
        expected_base_generation_id=None if pointer is None else pointer.generation_id,
        expected_pointer_version=0 if pointer is None else pointer.version,
    )


def _result(status, captured, counts, output=None, publication=None):
    return CandidateRunResult(
        status=status,
        execution_id=captured.execution_id,
        generation_id=None if output is None else output.generation_id,
        accepted_family_count=counts.accepted_family_count,
        rejection_count=counts.rejection_count,
        seal=None if output is None else output.seal,
        publication=publication,
    )


async def _finish(session_factory, request, captured, policy, terminal_state, *, failure_reason="candidate_rejected"):
    """Commit a fenced outcome before reporting failure; cancellation wins."""

    async with _session(session_factory, transaction=True) as session:
        await _set_timeout(session, policy.build.statement_timeout_ms)
        transition = await finish_execution(
            session,
            execution_id=captured.execution_id,
            fence=captured.fence,
            token=request.lease_token,
            terminal_state=terminal_state,
            terminal_reason=failure_reason if terminal_state == "failed" else "cancellation_requested",
        )
    if transition.changed and transition.state == "failed":
        if failure_reason != "candidate_rejected":
            raise CandidateRunnerError("segmented build failed") from None
        return "candidate_rejected"
    if transition.state == "canceling" and terminal_state != "canceled":
        return await _finish(session_factory, request, captured, policy, "canceled")
    return "canceled" if transition.state == "canceled" else "lease_lost"


async def _activate(session_factory, request, output):
    """Use the ordinary pointer CAS only after terminal sealing has committed."""

    try:
        async with _session(session_factory, transaction=True) as session:
            await _set_timeout(session, request.statement_timeout_ms)
            return await activate_generation(
                session,
                dataset_id=request.dataset_id,
                target_generation_id=output.generation_id,
                expected_generation_id=request.expected_base_generation_id,
                expected_pointer_version=request.expected_pointer_version,
            )
    except PublicationConflict:
        return None


async def _capture_for_build(session_factory, connector, request, policy):
    """Record a normally obtained execution identifier before entering the build."""

    with failure_phase(FailurePhase.CAPTURE):
        captured = await acquire_segmented_snowflake_capture(
            session_factory,
            request,
            statement_builder=connector,
            adapter=connector._adapter,
            credential_provider=connector._credential_provider,
            policy=policy.capture,
            driver_timeout_seconds=policy.driver_timeout_seconds,
            processing_policy=policy,
        )
        failure_identifiers(execution_id=captured.execution_id)
        return captured


async def run_segmented_snowflake_candidate(
    session_factory,
    connector,
    request,
    *,
    processing_policy,
    writer_transports=None,
    direct_compact: bool = False,
):
    """Carry one source claim through bounded replay, verified sealing and activation."""

    policy = _validated_policy(processing_policy)
    if request.source_binding_revision_id is None or request.source_binding_sha256 is None:
        raise CandidateRunnerError("segmented execution requires an immutable source binding")
    if writer_transports is not None:
        writer_transports.require_candidate_binding(request)
    if isinstance(request.lease_token, (bytearray, memoryview)):
        request = replace(request, lease_token=bytes(request.lease_token))
    captured = await _capture_for_build(session_factory, connector, request, policy)
    return await build_segmented_capture(
        session_factory,
        request,
        captured,
        policy,
        writer_transports=writer_transports,
        direct_compact=direct_compact,
    )


async def _build_captured_output(
    session_factory, request, build_id, counts, *, compact_limits, writer_transports, staged_phase=None
):
    """Dispatch owned snapshots after SOURCE and leave existing builds on the legacy path."""
    if staged_phase == "verified":
        from process.custom_import.build_output import _replay

        replay = await _replay(session_factory, request, build_id)
        if replay is not None:
            return replay
    with failure_phase(FailurePhase.GRAPH):
        if compact_limits is not None:
            from process.custom_import.family_block_finalize import try_build_block_output

            compact = await try_build_block_output(session_factory, request, build_id, counts, limits=compact_limits)
            if compact is not None:
                return compact
        generation_id = await build_graph(session_factory, request, build_id)
    if generation_id is None:
        return None
    with failure_phase(FailurePhase.OUTPUT):
        return await build_output(
            session_factory,
            request,
            build_id,
            projection_transport=None if writer_transports is None else writer_transports.projection,
        )


async def _load_sealed_direct_request(session_factory, request, captured, policy):
    """Load the retained build request and id for an already-verified direct build."""
    from sqlalchemy import text

    async with session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await _set_timeout(session, policy.build.statement_timeout_ms)
        bundle = (
            await session.scalars(
                select(CustomImportCaptureBundle)
                .join(
                    CustomImportExecution,
                    CustomImportExecution.capture_bundle_id == CustomImportCaptureBundle.capture_bundle_id,
                )
                .where(
                    CustomImportCaptureBundle.capture_bundle_id == captured.capture_bundle_id,
                    CustomImportExecution.execution_id == captured.execution_id,
                )
                .options(load_only(CustomImportCaptureBundle.sealed_at, *_capture_columns()))
            )
        ).one_or_none()
        if bundle is None:
            return None
        _require_capture(request, captured, policy, bundle)
        build = (
            await session.scalars(
                select(CustomImportBuildAttempt).where(
                    CustomImportBuildAttempt.execution_id == captured.execution_id,
                    CustomImportBuildAttempt.producing_fence == captured.fence,
                )
            )
        ).one_or_none()
        if build is None or build.phase != "verified" or build.generation_id is None:
            return None
        bounds = policy.build.to_mapping()
        bounds.pop("build_deadline_seconds")
        build_request = SourceBuildRequest(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=captured.execution_id,
            lease_token=request.lease_token,
            fence=captured.fence,
            definition=request.definition,
            expected_base_generation_id=build.base_generation_id,
            expected_pointer_version=build.base_pointer_version,
            complete_scope=True,
            build_deadline_at=build.build_deadline_at,
            **bounds,
        )
        return build_request, build.build_id


async def _replay_sealed_direct_capture(session_factory, request, captured, policy, retained_base):
    """Recover a committed direct terminal seal on a same-fence retry after lease expiry."""
    if not callable(session_factory):
        return None
    from process.custom_import.build_output import _replay

    loaded = await _load_sealed_direct_request(session_factory, request, captured, policy)
    if loaded is None:
        return None
    build_request, build_id = loaded
    if (
        retained_base is not None
        and (build_request.expected_base_generation_id, build_request.expected_pointer_version) != retained_base
    ):
        raise CandidateRunnerError("retained replay base has changed")
    output = await _replay(session_factory, build_request, build_id)
    if output is None:
        return None
    counts = await count_source_outcomes(session_factory, build_request, build_id)
    return build_request, counts, output


async def _finalize_captured_output(session_factory, build_request, captured, counts, output, retained_base):
    if output.no_change is not None:
        return _result("no_change", captured, counts, output, output.no_change)
    if retained_base is not None:
        return _result("sealed_unpublished", captured, counts, output)
    with failure_phase(FailurePhase.ACTIVATE):
        publication = await _activate(session_factory, build_request, output)
    return _result("sealed_unpublished" if publication is None else "activated", captured, counts, output, publication)


async def _begin_or_replay_build(session_factory, request, captured, policy, *, handoff, retained_base):
    try:
        build_request = await _build_request(session_factory, request, captured, policy, **handoff)
    except LeaseAuthorityLost:
        replayed = await _replay_sealed_direct_capture(session_factory, request, captured, policy, retained_base)
        if replayed is not None:
            return None, replayed
        raise
    if (
        retained_base is not None
        and (build_request.expected_base_generation_id, build_request.expected_pointer_version) != retained_base
    ):
        raise CandidateRunnerError("retained replay base has changed")
    return build_request, None


async def build_segmented_capture(
    session_factory,
    request,
    captured,
    policy,
    *,
    writer_transports=None,
    retained_base=None,
    compact_limits=None,
    direct_compact: bool = False,
):
    """Build a bound capture. Replays require the original base and explicit activation."""
    if compact_limits is not None and (writer_transports is not None or retained_base is None):
        raise CandidateRunnerError("compact retained builds require the local owner and explicit base")
    if direct_compact and writer_transports is not None:
        raise CandidateRunnerError("direct compact option cannot be combined with writer transports")
    counts = SourceOutcomeCounts(0, 0)
    if captured.status not in {"capture_sealed", "capture_bound"}:
        return _result(captured.status, captured, counts)
    handoff = {} if writer_transports is None else {"writer_transports": writer_transports}
    stage_options = handoff if not direct_compact else {"direct_compact": True}
    try:
        with failure_phase(FailurePhase.BEGIN_BUILD):
            build_request, replayed = await _begin_or_replay_build(
                session_factory, request, captured, policy, handoff=handoff, retained_base=retained_base
            )
            if replayed is not None:
                return await _finalize_captured_output(
                    session_factory, replayed[0], captured, replayed[1], replayed[2], retained_base
                )
        staged_source = await stage_segmented_source(session_factory, build_request, **stage_options)
        with failure_phase(FailurePhase.COUNTS):
            failure_identifiers(build_id=staged_source.build_id)
            counts = await count_source_outcomes(session_factory, build_request, staged_source.build_id)
        output = await _build_captured_output(
            session_factory,
            build_request,
            staged_source.build_id,
            counts,
            compact_limits=compact_limits,
            writer_transports=writer_transports,
            staged_phase=getattr(staged_source, "phase", None),
        )
        if output is None:
            status = await _finish(session_factory, request, captured, policy, "failed")
            return _result(status, captured, counts)
    except CancellationRequested:
        return _result(await _finish(session_factory, request, captured, policy, "canceled"), captured, counts)
    except LeaseAuthorityLost:
        return _result("lease_lost", captured, counts)
    except CandidateRunnerError:
        status = await _finish(
            session_factory, request, captured, policy, "failed", failure_reason="segmented_build_failed"
        )
        return _result(status, captured, counts)
    return await _finalize_captured_output(session_factory, build_request, captured, counts, output, retained_base)
