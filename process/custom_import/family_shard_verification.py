# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Record completed owner verification after exact candidate durability."""

from sqlalchemy import func, select
from sqlalchemy.orm import undefer

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildVerification, CustomImportGeneration
from process.custom_import.build_source import _flush_page, _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_materialization import _COUNTS, automated_proof
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import _authority, _call, _candidate, _execute, _validation_identity
from process.custom_import.family_block_validate import ClosedCandidateIdentity

_CONTRACT = "custom-import/build-structure/v1"
_FROZEN = ("source_frozen_at", "graph_frozen_at", "output_frozen_at")


def _count_values(request, counts):
    if type(counts) is not tuple or len(counts) != len(_COUNTS):
        raise ValueError("owner verification requires all eight original counts")
    for name, value in zip(_COUNTS, counts, strict=True):
        _integer(value, "owner verification " + name, zero=True)
    if counts[0] != counts[1] or counts[1] != counts[2] or counts[5] != len(request.definition.selection_profiles):
        raise FamilyBlockUnavailable("owner verification selected-family or profile counts differ")
    return dict(zip(_COUNTS, counts, strict=True))


def _control_counts(build, generation, counts):
    expected = counts["family_count"]
    if (
        build.phase not in ("verifying", "verified")
        or build.plan_stage != "complete"
        or build.plan_complete_at is None
        or build.candidate_error_count != 0
        or (build.selected_family_count, build.completed_family_count) != (expected, expected)
        or build.generation_family_count != counts["generation_family_count"]
        or build.winner_count != counts["winner_count"]
        or (generation.root_count, generation.family_count) != (counts["root_count"], expected)
        or any(getattr(build, name) is None for name in _FROZEN)
    ):
        raise FamilyBlockUnavailable("owner verification frozen aggregate counts differ")


def _proof_identity(build, generation, proof):
    if (
        proof.build_id != build.build_id
        or proof.generation_id != generation.generation_id
        or proof.verification_contract != _CONTRACT
        or proof.manual_acceptance is not None
        or any(getattr(proof, name) != getattr(build, name) for name in _FROZEN)
    ):
        raise FamilyBlockUnavailable("owner verification immutable proof identity differs")


def _complete_proof(build, generation, proof, counts, stamp):
    if proof is None:
        if build.phase != "verifying" or build.verified_at is not None:
            raise FamilyBlockUnavailable("owner verification is missing its completed proof")
        return CustomImportBuildVerification(
            build_id=build.build_id,
            generation_id=generation.generation_id,
            verification_contract=_CONTRACT,
            manual_acceptance=None,
            verification_state="complete",
            scan_stage="complete",
            # One completed owner composition checkpoint; not an invariant count.
            page_sequence=1,
            verified_at=stamp,
            **{name: getattr(build, name) for name in _FROZEN},
            **counts,
        )
    _proof_identity(build, generation, proof)
    if proof.verification_state != "scanning" or build.phase != "verifying" or build.verified_at is not None:
        raise FamilyBlockUnavailable("owner verification progress cannot be replaced")
    if proof.scan_stage not in ("capture", "profiles", "families", "root_scalars", "child_scalars", "winners"):
        raise FamilyBlockUnavailable("owner verification progress stage differs")
    _integer(proof.page_sequence, "owner verification checkpoint", zero=True)
    if proof.page_sequence >= 2**63 - 1 or proof.verified_at is not None:
        raise FamilyBlockUnavailable("owner verification checkpoint is already complete")
    for name, completed_count in counts.items():
        previous = getattr(proof, name)
        _integer(previous, "owner verification previous " + name, zero=True)
        if previous > completed_count:
            raise FamilyBlockUnavailable("owner verification progress exceeds completed originals")
        setattr(proof, name, completed_count)
    proof.page_sequence += 1
    proof.verification_state, proof.scan_stage, proof.verified_at = "complete", "complete", stamp
    return proof


async def _controls(session, build_id, identity):
    scope = identity.scope
    build = (
        await _execute(
            session,
            select(CustomImportBuildAttempt)
            .where(
                CustomImportBuildAttempt.build_id == build_id,
                CustomImportBuildAttempt.execution_id == scope.execution_id,
                CustomImportBuildAttempt.producing_fence == scope.producing_fence,
            )
            .execution_options(populate_existing=True),
        )
    ).scalar_one()
    generation = (
        await _execute(
            session,
            select(CustomImportGeneration)
            .where(CustomImportGeneration.generation_id == scope.generation_id)
            .execution_options(populate_existing=True),
        )
    ).scalar_one()
    proof = (
        await _execute(
            session,
            select(CustomImportBuildVerification)
            .where(CustomImportBuildVerification.build_id == build_id)
            .options(undefer(CustomImportBuildVerification.manual_acceptance))
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    return build, generation, proof


async def _logged_candidate(session, request, layout, identity, validation_sha256):
    await _call(session, "lock_custom_import_snapshot_attempt", _authority(request))
    descriptor, binding = await _candidate(session, request, identity.scope.family_id, layout, {"logged"})
    await _validation_identity(session, descriptor, binding, identity)
    if bytes(descriptor.validation_sha256) != validation_sha256:
        raise FamilyBlockUnavailable("owner verification accepted commitment differs")


async def record_shard_verification(session_factory, request, build_id, layout, identity, *, validation_sha256, counts):
    """Record the existing automated proof for a genuinely verified logged closure.

    The caller must compose all original semantic obligations and native/original
    comparisons before accepting this exact digest and census in the store.
    This operation binds that already accepted proof to the normal build row;
    it does not derive semantic evidence from a descriptor or page sequence.
    """
    if type(identity) is not ClosedCandidateIdentity or type(validation_sha256) is not bytes:
        raise ValueError("owner verification requires its immutable closed proof identity")
    if len(validation_sha256) != 32:
        raise ValueError("owner verification commitment differs")
    _integer(build_id, "owner verification build")
    expected = _count_values(request, counts)
    async with _page_session(session_factory, request, build_id) as (session, _build):
        await _logged_candidate(session, request, layout, identity, validation_sha256)
        build, generation, proof = await _controls(session, build_id, identity)
        _control_counts(build, generation, expected)
        if proof is None or proof.verification_state != "complete":
            stamp = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
            proof = _complete_proof(build, generation, proof, expected, stamp)
            session.add(proof)
            # The existing UPDATE guard still requires the build to be verifying.
            await _flush_page(session)
            build.phase, build.verified_at = "verified", stamp
            await _flush_page(session)
        _proof_identity(build, generation, proof)
        if tuple(getattr(proof, name) for name in _COUNTS) != counts:
            raise FamilyBlockUnavailable("owner verification completed counts differ")
        await _prepare_statement(session)
        current = await automated_proof(session, generation)
        if current.verified_at != proof.verified_at or tuple(getattr(current, name) for name in _COUNTS) != counts:
            raise FamilyBlockUnavailable("owner verification replay evidence differs")
        await _logged_candidate(session, request, layout, identity, validation_sha256)
        return current.verified_at
