# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Proof recording under a synthetic logged-owner transaction boundary."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildVerification, CustomImportGeneration
from process.custom_import import family_shard_verification as verification
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_validate import CandidateScope, ClosedCandidateIdentity
from process.custom_import.publication import PublicationConflict
from tests.test_custom_import_build_source import _request

_STAMP = datetime(2030, 1, 2, tzinfo=timezone.utc)


def _controls_for(scope):
    scope_by_name = {
        name: getattr(scope, name)
        for name in (
            "dataset_id",
            "definition_revision_id",
            "schema_revision_id",
            "execution_id",
            "capture_bundle_id",
            "producing_fence",
            "producing_token_sha256",
        )
    }
    build = SimpleNamespace(
        **scope_by_name,
        build_id=23,
        generation_id=scope.generation_id,
        phase="verifying",
        plan_stage="complete",
        plan_complete_at=_STAMP,
        candidate_error_count=0,
        selected_family_count=1,
        completed_family_count=1,
        generation_family_count=1,
        winner_count=1,
        verified_at=None,
        **{name: _STAMP for name in verification._FROZEN},
    )
    generation = SimpleNamespace(**scope_by_name, generation_id=scope.generation_id, root_count=1, family_count=1)
    return build, generation


@pytest.fixture
def candidate(monkeypatch):
    request = _request()
    layout = FamilyBlockLayout(request.definition, {"details": 7})
    scope = CandidateScope(
        family_id=17,
        generation_id=31,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=41,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
        base_generation_id=request.expected_base_generation_id,
    )
    identity = ClosedCandidateIdentity(scope, _STAMP, layout.digest, 71, (("family", 101),))
    counts = (1, 1, 1, 2, 1, len(request.definition.selection_profiles), 3, 6)
    build, generation = _controls_for(scope)
    state = SimpleNamespace(
        request=request,
        layout=layout,
        identity=identity,
        counts=counts,
        build=build,
        generation=generation,
        proof=None,
        descriptor=SimpleNamespace(validation_sha256=b"v" * 32),
        flushes=[],
        additions=[],
        clocks=[],
        authority_locked=False,
        attempts=[],
        identity_checks=[],
        refuse_attempt=None,
    )
    state.session = _Session(state)

    monkeypatch.setattr(verification, "_page_session", state.session.page)
    monkeypatch.setattr(verification, "_candidate", state.session.candidate_scope)
    monkeypatch.setattr(verification, "_validation_identity", state.session.identity_check)
    monkeypatch.setattr(verification, "_call", state.session.attempt)
    monkeypatch.setattr(verification, "_execute", lambda session, statement: session.execute(statement))
    monkeypatch.setattr(verification, "_flush_page", _Session.flush)
    monkeypatch.setattr(verification, "_prepare_statement", _Session.prepare)
    return state


class _Session:
    def __init__(self, state):
        self.state = state

    @asynccontextmanager
    async def page(self, factory, request, build_id):
        assert factory is self.state and request is self.state.request and build_id == 23
        self.state.authority_locked = True
        try:
            yield self, self.state.build
        finally:
            self.state.authority_locked = False

    async def candidate_scope(self, session, request, family_id, layout, phases):
        state = self.state
        assert state.authority_locked and session is self and request is state.request
        assert (family_id, layout, phases) == (17, state.layout, {"logged"})
        return state.descriptor, None

    async def identity_check(self, session, descriptor, binding, expected):
        state = self.state
        assert state.authority_locked and session is self and descriptor is state.descriptor
        assert binding is None and expected is state.identity
        state.identity_checks.append(expected)

    async def attempt(self, session, name, arguments):
        state = self.state
        assert state.authority_locked and session is self
        assert (name, arguments) == ("lock_custom_import_snapshot_attempt", verification._authority(state.request))
        state.attempts.append(True)
        if state.refuse_attempt == len(state.attempts):
            raise FamilyBlockUnavailable("current native attempt refused")

    async def flush(self):
        state = self.state
        assert state.authority_locked
        state.flushes.append((state.build.phase, state.proof.verification_state, state.proof.page_sequence))

    async def prepare(self):
        assert self.state.authority_locked

    async def execute(self, statement):
        state = self.state
        assert state.authority_locked
        if "clock_timestamp" in str(statement):
            state.clocks.append(True)
            return SimpleNamespace(scalar_one=lambda: _STAMP + timedelta(seconds=1))
        if len(statement.column_descriptions) == 2:
            return SimpleNamespace(one_or_none=lambda: (state.build, state.proof))
        model = statement.column_descriptions[0]["entity"]
        selected = {
            CustomImportBuildAttempt: state.build,
            CustomImportGeneration: state.generation,
            CustomImportBuildVerification: state.proof,
        }[model]
        return SimpleNamespace(scalar_one=lambda: selected, scalar_one_or_none=lambda: selected)

    def add(self, proof):
        assert self.state.authority_locked
        self.state.additions.append(proof)
        self.state.proof = proof


def _proof(candidate, *, complete=False):
    proof = CustomImportBuildVerification(
        build_id=candidate.build.build_id,
        generation_id=candidate.identity.scope.generation_id,
        verification_contract=verification._CONTRACT,
        manual_acceptance=None,
        verification_state="complete" if complete else "scanning",
        scan_stage="complete" if complete else "families",
        page_sequence=7,
        verified_at=_STAMP if complete else None,
        **{name: _STAMP for name in verification._FROZEN},
        **dict(zip(verification._COUNTS, candidate.counts if complete else (0,) * 8, strict=True)),
    )
    candidate.proof = proof
    if complete:
        candidate.build.phase, candidate.build.verified_at = "verified", _STAMP
    return proof


async def _record(candidate, **changes):
    parameters_by_name = dict(validation_sha256=b"v" * 32, counts=candidate.counts)
    parameters_by_name.update(changes)
    return await verification.record_shard_verification(
        candidate,
        candidate.request,
        candidate.build.build_id,
        candidate.layout,
        candidate.identity,
        **parameters_by_name,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scanning", [False, True])
async def test_proof_flush_precedes_verified_build_phase(candidate, scanning):
    if scanning:
        _proof(candidate)
    stamp = await _record(candidate)
    sequence = 8 if scanning else 1
    assert candidate.flushes == [("verifying", "complete", sequence), ("verified", "complete", sequence)]
    assert candidate.build.verified_at == candidate.proof.verified_at == stamp
    assert candidate.proof.manual_acceptance is None
    assert len(candidate.attempts) == len(candidate.identity_checks) == 2
    assert tuple(getattr(candidate.proof, name) for name in verification._COUNTS) == candidate.counts


@pytest.mark.asyncio
async def test_completed_replay_preserves_proof_without_writes(candidate):
    proof = _proof(candidate, complete=True)
    assert await _record(candidate) == _STAMP
    assert candidate.proof is proof and proof.page_sequence == 7
    assert not candidate.flushes and not candidate.additions and not candidate.clocks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["selected_family_count", "completed_family_count", "generation_family_count", "winner_count"]
)
async def test_changed_frozen_build_totals_refuse(candidate, field):
    setattr(candidate.build, field, 99)
    with pytest.raises(FamilyBlockUnavailable, match="aggregate"):
        await _record(candidate)
    assert not candidate.flushes and candidate.proof is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["manual", "contract", "frozen", "generation", "count", "missing", "phase"])
async def test_completed_history_cannot_be_overwritten(candidate, damage):
    proof = _proof(candidate, complete=True)
    match damage:
        case "manual":
            proof.manual_acceptance = {"contract": "custom-import/manual-acceptance/v1"}
        case "contract":
            proof.verification_contract = "custom-import/manual-acceptance/v1"
        case "frozen":
            proof.source_frozen_at += timedelta(seconds=1)
        case "generation":
            proof.generation_id += 1
        case "count":
            proof.child_scalar_count += 1
        case "missing":
            candidate.proof = None
        case "phase":
            candidate.build.phase = "verifying"
    with pytest.raises((FamilyBlockUnavailable, PublicationConflict)):
        await _record(candidate)
    assert not candidate.flushes and not candidate.additions


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["sequence", "complete_stamp", "count", "stage", "contract"])
async def test_incompatible_scanning_progress_is_preserved(candidate, damage):
    proof = _proof(candidate)
    match damage:
        case "sequence":
            proof.page_sequence = 2**63 - 1
        case "complete_stamp":
            proof.verified_at = _STAMP
        case "count":
            proof.root_count = 2
        case "stage":
            proof.scan_stage = "unsupported"
        case "contract":
            proof.verification_contract = "custom-import/manual-acceptance/v1"
    with pytest.raises(FamilyBlockUnavailable):
        await _record(candidate)
    assert candidate.proof is proof and not candidate.flushes and not candidate.additions


@pytest.mark.asyncio
@pytest.mark.parametrize("counts", [(True,) * 8, (1, 1, 1), (1, 1, 2, 2, 1, 1, 3, 6), (1, 1, 1, -1, 1, 1, 3, 6)])
async def test_complete_count_tuple_is_required(candidate, counts):
    with pytest.raises((ValueError, FamilyBlockUnavailable)):
        await _record(candidate, counts=counts)
    assert not candidate.flushes


@pytest.mark.asyncio
async def test_changed_accepted_digest_cannot_create_verification(candidate):
    candidate.descriptor.validation_sha256 = b"x" * 32
    with pytest.raises(FamilyBlockUnavailable, match="commitment"):
        await _record(candidate)
    assert candidate.proof is None and not candidate.flushes


@pytest.mark.asyncio
@pytest.mark.parametrize("step", [1, 2])
async def test_current_native_attempt_check_is_preserved(candidate, step):
    candidate.refuse_attempt = step
    with pytest.raises(FamilyBlockUnavailable, match="native attempt"):
        await _record(candidate)
    assert len(candidate.attempts) == step and not candidate.authority_locked
    if step == 1:
        assert not candidate.flushes and not candidate.additions
