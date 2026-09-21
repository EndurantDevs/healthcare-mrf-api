# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional orchestration for a generic custom-import candidate.

This is deliberately not a worker registration point.  A host supplies a
persisted execution, a lease token, and an ``AsyncSession`` factory; this
module owns only the bounded candidate lifecycle.  Lifecycle, graph-write,
and finality operations use distinct caller-owned transactions so execution
state never becomes an implicit publication transaction.
"""

from __future__ import annotations

import datetime as dt
import hmac
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportExecution, CustomImportLease
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import (
    LeaseGrant,
    claim_execution,
    finish_execution,
    heartbeat_execution,
    lease_token_sha256,
)
from process.custom_import.family import FamilyBuildResult, FamilyRejection, assemble_root_families
from process.custom_import.publication import (
    GenerationSealReceipt,
    PublicationConflict,
    PublicationReceipt,
    activate_generation,
    record_no_change,
    seal_generation,
)
from process.custom_import.runner_codec import root_key_evidence_from_tuple, root_key_hash
from process.custom_import.runner_graph import materialize_candidate as _materialize_candidate
from process.custom_import.runner_registry import database_now, validate_revision_identity
from process.custom_import.runner_types import (
    CancellationRequested as _CancellationRequested,
)
from process.custom_import.runner_types import (
    CandidateRunnerError,
    CandidateRunRequest,
    CandidateRunResult,
    RunStatus,
    SessionFactory,
)
from process.custom_import.runner_types import (
    CurrentGenerationPointer as _Pointer,
)
from process.custom_import.runner_types import (
    LeaseAuthorityLost as _LeaseLost,
)
from process.custom_import.runner_types import (
    MaterializedCandidate as _MaterializedCandidate,
)

__all__ = (
    "CandidateRunRequest",
    "CandidateRunResult",
    "CandidateRunnerError",
    "run_candidate",
)


_NO_CHANGE_DIFFERENCE = "no-change candidate effective output differs from the current generation"


async def run_candidate(session_factory: SessionFactory, request: CandidateRunRequest) -> CandidateRunResult:
    """Run one admitted candidate from fenced claim through terminal finality.

    Unknown persistence and graph-validation exceptions propagate after their
    graph transaction rolls back.  Only observed cancellation and stale-lease
    conditions become terminal runner results.
    """

    validate_candidate_request(session_factory, request)
    admitted = reject_duplicate_canonical_root_keys(
        request.definition,
        assemble_root_families(request.definition, request.roots, request.children_by_collection),
    )
    grant = await _claim(session_factory, request)
    if grant is None:
        return admission_result("not_claimed", request, admitted)
    return await run_claimed_candidate(session_factory, request, grant, admitted)


def validate_candidate_request(session_factory: object, request: object) -> None:
    """Reject malformed host inputs before attempting any lifecycle mutation."""

    if not callable(session_factory):
        raise CandidateRunnerError("candidate runner requires a session factory")
    if not isinstance(request, CandidateRunRequest):
        raise CandidateRunnerError("candidate runner request is malformed")
    validate_candidate_identifiers(request)
    if not isinstance(request.definition, CustomImportDefinition):
        raise CandidateRunnerError("candidate definition is malformed")
    validate_definition_canonical(request.definition)
    if not isinstance(request.complete_scope, bool):
        raise CandidateRunnerError("complete_scope must be boolean")
    if not isinstance(request.roots, (list, tuple)) or not isinstance(request.children_by_collection, Mapping):
        raise CandidateRunnerError("candidate records must use bounded root and child collections")
    try:
        lease_token_sha256(request.lease_token)
    except ValueError as exc:
        raise CandidateRunnerError("candidate lease token is malformed") from exc


def validate_candidate_identifiers(request: CandidateRunRequest) -> None:
    """Require positive immutable IDs before opening a lifecycle transaction."""

    identity_values = (
        (request.dataset_id, "dataset_id"),
        (request.definition_revision_id, "definition_revision_id"),
        (request.schema_revision_id, "schema_revision_id"),
        (request.execution_id, "execution_id"),
    )
    for identifier, label in identity_values:
        if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier <= 0:
            raise CandidateRunnerError(f"{label} must be a positive integer")


def validate_definition_canonical(definition: CustomImportDefinition) -> None:
    """Require the supplied parsed definition to exactly match its canonical form."""

    try:
        canonical_definition = CustomImportDefinition.from_json(definition.canonical)
    except (TypeError, UnicodeError, ValueError) as exc:
        raise CandidateRunnerError("candidate definition is not canonical") from exc
    if canonical_definition != definition:
        raise CandidateRunnerError("candidate definition does not match its canonical form")


def reject_duplicate_canonical_root_keys(
    definition: CustomImportDefinition,
    admitted: FamilyBuildResult,
) -> FamilyBuildResult:
    """Reject every accepted family sharing one canonical root identity."""

    hashed_families = tuple((root_key_hash(definition, family.root), family) for family in admitted.families)
    count_by_root_hash: dict[bytes, int] = {}
    for root_hash, _family in hashed_families:
        count_by_root_hash[root_hash] = count_by_root_hash.get(root_hash, 0) + 1
    rejected_root_hashes = {
        evidence[1]
        for rejection in admitted.rejections
        if (evidence := root_key_evidence_from_tuple(definition, rejection.root_key)) is not None
    }
    duplicate_hashes = {
        root_hash for root_hash, count in count_by_root_hash.items() if count > 1 or root_hash in rejected_root_hashes
    }
    if not duplicate_hashes:
        return admitted
    duplicate_rejections = tuple(
        FamilyRejection(family.root_key, "duplicate_root_key")
        for root_hash, family in hashed_families
        if root_hash in duplicate_hashes
    )
    return FamilyBuildResult(
        families=tuple(family for root_hash, family in hashed_families if root_hash not in duplicate_hashes),
        rejections=(*admitted.rejections, *duplicate_rejections),
        candidate_errors=admitted.candidate_errors,
    )


async def run_claimed_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> CandidateRunResult:
    """Apply admission policy and materialize only while the lease remains live."""

    if is_candidate_rejected(request, admitted):
        return await _finish_rejected_candidate(session_factory, request, grant, admitted)
    try:
        materialized = await _materialize_candidate(session_factory, request, grant, admitted)
    except _CancellationRequested:
        return await _finish_canceled_candidate(session_factory, request, grant, admitted)
    except _LeaseLost:
        return lease_lost_result(request, admitted)
    return await finalize_materialized_candidate(session_factory, request, grant, admitted, materialized)


def is_candidate_rejected(request: CandidateRunRequest, admitted: FamilyBuildResult) -> bool:
    """Return whether admission cannot safely form a complete candidate."""

    return admitted.is_candidate_rejected or (
        request.definition.refresh_mode == "snapshot" and not request.complete_scope
    )


async def finalize_materialized_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate,
) -> CandidateRunResult:
    """Renew authority before finality and stop if cancellation won the race."""

    renewed_grant = await _heartbeat(session_factory, request, grant)
    if renewed_grant is None:
        return lease_lost_result(request, admitted, materialized)
    if renewed_grant.state != "running":
        return await _finish_canceled_candidate(session_factory, request, renewed_grant, admitted, materialized)
    return await finalize_live_candidate(session_factory, request, renewed_grant, admitted, materialized)


async def finalize_live_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate,
) -> CandidateRunResult:
    """Prefer atomic no-change proof, then seal and conditionally activate."""

    if can_attempt_no_change(materialized.pointer, request):
        no_change_result = await no_change_result_or_none(session_factory, request, grant, admitted, materialized)
        if no_change_result is not None:
            return no_change_result
    return await seal_and_activate_candidate(session_factory, request, grant, admitted, materialized)


async def no_change_result_or_none(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate,
) -> CandidateRunResult | None:
    """Return atomic no-change, or preserve a live candidate for sealing."""

    try:
        receipt = await _record_no_change_or_none(session_factory, request, grant, materialized)
    except PublicationConflict:
        terminal_outcome = await _finality_conflict_outcome(
            session_factory,
            request,
            grant,
            admitted,
            materialized,
        )
        if terminal_outcome is not None:
            return terminal_outcome
        return None
    if receipt is None:
        return None
    return materialized_result("no_change", request, materialized, publication=receipt)


async def seal_and_activate_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate,
) -> CandidateRunResult:
    """Seal a distinct candidate and activate it only at the captured pointer."""

    try:
        seal = await _seal(session_factory, request, grant, materialized)
    except PublicationConflict:
        terminal_outcome = await _finality_conflict_outcome(
            session_factory,
            request,
            grant,
            admitted,
            materialized,
        )
        if terminal_outcome is not None:
            return terminal_outcome
        raise
    publication = await _activate(session_factory, request, materialized)
    if publication is None:
        return materialized_result("sealed_unpublished", request, materialized, seal=seal)
    return materialized_result("activated", request, materialized, seal=seal, publication=publication)


def admission_result(
    status: RunStatus,
    request: CandidateRunRequest,
    admitted: FamilyBuildResult,
) -> CandidateRunResult:
    """Build an outcome for work that never formed a generation graph."""

    return CandidateRunResult(
        status=status,
        execution_id=request.execution_id,
        accepted_family_count=len(admitted.families),
        rejection_count=len(admitted.rejections),
    )


def lease_lost_result(
    request: CandidateRunRequest,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate | None = None,
) -> CandidateRunResult:
    """Build a retryable stale-fence outcome without relabeling it as failure."""

    if materialized is None:
        return admission_result("lease_lost", request, admitted)
    return materialized_result("lease_lost", request, materialized)


def materialized_result(
    status: RunStatus,
    request: CandidateRunRequest,
    materialized: _MaterializedCandidate,
    *,
    seal: GenerationSealReceipt | None = None,
    publication: PublicationReceipt | None = None,
) -> CandidateRunResult:
    """Build one terminal result using persisted candidate graph counts."""

    return CandidateRunResult(
        status=status,
        execution_id=request.execution_id,
        generation_id=materialized.generation_id,
        accepted_family_count=materialized.accepted_family_count,
        rejection_count=materialized.rejection_count,
        seal=seal,
        publication=publication,
    )


async def _claim(session_factory: SessionFactory, request: CandidateRunRequest) -> LeaseGrant | None:
    """Validate immutable identity before claiming in one lifecycle transaction."""

    async with session_factory() as session, session.begin():
        await validate_execution_identity_before_claim(session, request)
        await validate_revision_identity(session, request)
        return await claim_execution(session, execution_id=request.execution_id, token=request.lease_token)


async def validate_execution_identity_before_claim(session: AsyncSession, request: CandidateRunRequest) -> None:
    """Reject a mismatched host request before any lifecycle row can change.

    This intentionally reads without a row lock: ``claim_execution`` takes the
    durable dataset → execution → lease lock order immediately afterward.  The
    immutable identity check and the claim therefore share one transaction
    without introducing a lower-order pre-claim lock.
    """

    with session.no_autoflush:
        execution = await session.scalar(
            select(CustomImportExecution)
            .where(CustomImportExecution.execution_id == request.execution_id)
            .execution_options(populate_existing=True)
        )
    if not has_immutable_request_identity(execution, request):
        raise CandidateRunnerError("candidate execution identity does not match the request")


async def _heartbeat(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
) -> LeaseGrant | None:
    """Renew the exact candidate fence before opening finality work."""

    async with session_factory() as session, session.begin():
        return await heartbeat_execution(
            session,
            execution_id=request.execution_id,
            fence=grant.fence,
            token=request.lease_token,
        )


async def _finish_rejected_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
) -> CandidateRunResult:
    """Fence a candidate-wide admission failure without appending a generation."""

    async with session_factory() as session, session.begin():
        transition = await finish_execution(
            session,
            execution_id=request.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="failed",
            terminal_reason="candidate_rejected",
        )
    if transition.changed:
        return admission_result("candidate_rejected", request, admitted)
    if transition.state == "canceling":
        return await _finish_canceled_candidate(session_factory, request, grant, admitted)
    if transition.state == "canceled":
        return admission_result("canceled", request, admitted)
    return lease_lost_result(request, admitted)


async def _finish_canceled_candidate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate | None = None,
) -> CandidateRunResult:
    """Acknowledge a known cancellation only while the original fence is live."""

    async with session_factory() as session, session.begin():
        transition = await finish_execution(
            session,
            execution_id=request.execution_id,
            fence=grant.fence,
            token=request.lease_token,
            terminal_state="canceled",
            terminal_reason="cancellation_requested",
        )
    if transition.changed or transition.state == "canceled":
        if materialized is None:
            return admission_result("canceled", request, admitted)
        return materialized_result("canceled", request, materialized)
    return lease_lost_result(request, admitted, materialized)


async def _finality_conflict_outcome(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    admitted: FamilyBuildResult,
    materialized: _MaterializedCandidate,
) -> CandidateRunResult | None:
    """Classify only an observed cancellation or stale lease after finality races."""

    observed_state = await observed_finality_state(session_factory, request, grant)
    if observed_state == "canceling":
        return await _finish_canceled_candidate(session_factory, request, grant, admitted, materialized)
    if observed_state == "canceled":
        return materialized_result("canceled", request, materialized)
    if observed_state == "lease_lost":
        return lease_lost_result(request, admitted, materialized)
    return None


async def observed_finality_state(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
) -> str | None:
    """Read the mutable state needed to classify a finality conflict safely."""

    async with session_factory() as session, session.begin():
        execution = await session.get(CustomImportExecution, request.execution_id)
        if not is_execution_request_match(execution, request):
            return None
        if execution.state in {"canceling", "canceled"}:
            return execution.state
        lease = await session.get(CustomImportLease, request.execution_id)
        now = await database_now(session)
        token_sha256 = lease_token_sha256(request.lease_token)
        if not has_matching_live_grant(execution, lease, grant, token_sha256, now):
            return "lease_lost"
    return None


def is_execution_request_match(
    execution: CustomImportExecution | None,
    request: CandidateRunRequest,
) -> bool:
    """Return whether a mutable execution still has the request's identity."""

    return has_immutable_request_identity(execution, request)


def has_immutable_request_identity(
    execution: CustomImportExecution | None,
    request: CandidateRunRequest,
) -> bool:
    """Return whether an execution can ever be claimed for this request."""

    return execution is not None and (
        execution.dataset_id == request.dataset_id
        and execution.definition_revision_id == request.definition_revision_id
        and execution.schema_revision_id == request.schema_revision_id
        and execution.capture_bundle_id is not None
    )


def has_matching_live_grant(
    execution: CustomImportExecution,
    lease: CustomImportLease | None,
    grant: LeaseGrant,
    token_sha256: bytes,
    now: dt.datetime,
) -> bool:
    """Return whether a finality observer still holds the exact live fence."""

    return (
        execution.state == "running"
        and lease is not None
        and lease.fence == grant.fence
        and lease.token_sha256 is not None
        and hmac.compare_digest(bytes(lease.token_sha256), token_sha256)
        and lease.expires_at is not None
        and lease.expires_at > now
    )


def can_attempt_no_change(pointer: _Pointer | None, request: CandidateRunRequest) -> bool:
    """Return whether the captured pointer uses the candidate's immutable schema."""

    return (
        pointer is not None
        and pointer.definition_revision_id == request.definition_revision_id
        and pointer.schema_revision_id == request.schema_revision_id
    )


async def _record_no_change_or_none(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    materialized: _MaterializedCandidate,
) -> PublicationReceipt | None:
    """Atomically complete only equal output; return ``None`` for a difference."""

    assert materialized.pointer is not None
    try:
        async with session_factory() as session, session.begin():
            return await record_no_change(
                session,
                dataset_id=request.dataset_id,
                execution_id=request.execution_id,
                expected_generation_id=materialized.pointer.generation_id,
                expected_pointer_version=materialized.pointer.version,
                candidate_generation_id=materialized.generation_id,
                lease_fence=grant.fence,
                lease_token=request.lease_token,
            )
    except PublicationConflict as exc:
        if str(exc) == _NO_CHANGE_DIFFERENCE:
            return None
        raise


async def _seal(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    grant: LeaseGrant,
    materialized: _MaterializedCandidate,
) -> GenerationSealReceipt:
    """Seal the current candidate in its own clean finality transaction."""

    async with session_factory() as session, session.begin():
        return await seal_generation(
            session,
            dataset_id=request.dataset_id,
            generation_id=materialized.generation_id,
            lease_fence=grant.fence,
            lease_token=request.lease_token,
        )


async def _activate(
    session_factory: SessionFactory,
    request: CandidateRunRequest,
    materialized: _MaterializedCandidate,
) -> PublicationReceipt | None:
    """Activate at the captured pointer or preserve a sealed retryable candidate."""

    pointer = materialized.pointer
    try:
        async with session_factory() as session, session.begin():
            return await activate_generation(
                session,
                dataset_id=request.dataset_id,
                target_generation_id=materialized.generation_id,
                expected_generation_id=None if pointer is None else pointer.generation_id,
                expected_pointer_version=0 if pointer is None else pointer.version,
            )
    except PublicationConflict:
        return None
