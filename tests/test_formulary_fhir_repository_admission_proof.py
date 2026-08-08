# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused proof tests for immutable formulary twin admission."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository_admission_proof
from process.formulary_fhir.repository_admission_proof import DatasetEvidence
from process.formulary_fhir.repository_admission_proof import dataset_evidence_hash
from process.formulary_fhir.repository_admission_proof import recompute_alternative_proof
from process.formulary_fhir.repository_admission_proof import require_full_checkpoints
from process.formulary_fhir.repository_admission_proof import require_matching_pair
from process.formulary_fhir.repository_admission_types import AlternativeProof
from process.formulary_fhir.repository_admission_types import TwinAdmissionError
from process.formulary_fhir.repository_shared import DatasetRef
from process.formulary_fhir.repository_shared import DatasetVerification


CUTOFF = dt.datetime(2026, 8, 8, 1, tzinfo=dt.UTC)
BASELINE_VERIFIED = CUTOFF + dt.timedelta(minutes=1)
CANDIDATE_VERIFIED = CUTOFF + dt.timedelta(minutes=2)


def _dataset(identity: str = "a", **overrides) -> DatasetRef:
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": "ffd_" + identity * 48,
        "run_id": "run-" + identity,
        "previous_dataset_id": None,
        "cutoff_at": CUTOFF,
        "acquisition_contract_hash": "c" * 64,
        "intent": "none" if identity == "a" else "requested",
        "status": "verified",
    }
    values_by_field.update(overrides)
    return DatasetRef(**values_by_field)


def _alternative(**overrides) -> dict[str, object]:
    values_by_field: dict[str, object] = {
        "alias_id": "alias-a",
        "alias_version_id": "version-a",
        "upstream_medication_id": "med-a",
        "raw_reference": "MedicationKnowledge/med-b",
        "corrected_reference": "MedicationKnowledge/med-b",
        "resolved_medication_id": "med-b",
        "resolved": True,
        "rule_version": "rule-v1",
        "evidence_json": {"same_alias": True},
    }
    values_by_field.update(overrides)
    return values_by_field


def _checkpoint(alias_id: str = "alias-a", **overrides) -> dict[str, object]:
    values_by_field: dict[str, object] = {
        "alias_id": alias_id,
        "linked_alias_id": alias_id,
        "cutoff_at": CUTOFF,
        "acquisition_mode": "full",
        "expected_count": 2,
        "processed_count": 2,
        "membership_hash": "b" * 64,
        "completed": True,
    }
    values_by_field.update(overrides)
    return values_by_field


def _verification(dataset_id: str, **overrides) -> DatasetVerification:
    values_by_field = {
        "source_id": "source-a",
        "dataset_id": dataset_id,
        "list_count": 1,
        "alias_count": 1,
        "medication_membership_count": 2,
        "coverage_hash": "d" * 64,
        "membership_hash": "e" * 64,
    }
    values_by_field.update(overrides)
    return DatasetVerification(**values_by_field)


def _evidence(
    dataset: DatasetRef,
    verified_at: dt.datetime,
    **verification_overrides,
) -> DatasetEvidence:
    return DatasetEvidence(
        {"dataset_id": dataset.dataset_id, "status": dataset.status},
        _verification(dataset.dataset_id, **verification_overrides),
        AlternativeProof(1, "f" * 64),
        verified_at,
    )


@pytest.mark.asyncio
async def test_alternative_proof_is_keyset_paged_and_domain_separated(monkeypatch):
    monkeypatch.setattr(repository_admission_proof, "WRITE_BATCH_SIZE", 2)
    first = _alternative()
    second = _alternative(alias_id="alias-b", alias_version_id="version-b")
    third = _alternative(alias_id="alias-c", alias_version_id="version-c")
    database = SimpleNamespace(all=AsyncMock(side_effect=[[first, second], [third]]))

    proof = await recompute_alternative_proof(database, "source-a", _dataset().dataset_id)

    assert proof.count == 3
    assert proof.evidence_hash != hashlib.sha256(b"").hexdigest()
    first_call, second_call = database.all.await_args_list
    assert first_call.kwargs["last_alias_id"] == ""
    assert second_call.kwargs["last_alias_id"] == "alias-b"
    assert second_call.kwargs["last_version_id"] == "version-b"
    assert ":last_raw_reference" in first_call.args[0]
    assert "ORDER BY link.alias_id" in first_call.args[0]


@pytest.mark.asyncio
async def test_alternative_proof_hashes_every_stored_evidence_field():
    alternatives = [
        _alternative(),
        _alternative(alias_id="alias-z"),
        _alternative(alias_version_id="version-z"),
        _alternative(upstream_medication_id="med-z"),
        _alternative(raw_reference="MedicationKnowledge/med-z"),
        _alternative(corrected_reference="MedicationKnowledge/med-z"),
        _alternative(resolved=False, resolved_medication_id=None),
        _alternative(rule_version="rule-v2"),
        _alternative(evidence_json={"same_alias": False}),
    ]
    hashes: set[str] = set()
    for alternative_by_field in alternatives:
        database = SimpleNamespace(
            all=AsyncMock(return_value=[alternative_by_field])
        )
        proof = await recompute_alternative_proof(
            database,
            "source-a",
            _dataset().dataset_id,
        )
        hashes.add(proof.evidence_hash)

    assert len(hashes) == len(alternatives)


@pytest.mark.asyncio
async def test_empty_alternative_proof_has_one_stable_domain_hash():
    database = SimpleNamespace(all=AsyncMock(return_value=[]))

    proof = await recompute_alternative_proof(database, "source-a", _dataset().dataset_id)

    expected = hashlib.sha256(
        b"fhir-formulary-alternative-evidence-v1\n"
    ).hexdigest()
    assert proof == AlternativeProof(0, expected)


@pytest.mark.asyncio
async def test_alternative_proof_sanitizes_storage_and_order_failures(monkeypatch):
    secret_database = SimpleNamespace(
        all=AsyncMock(side_effect=RuntimeError("private endpoint"))
    )
    with pytest.raises(TwinAdmissionError) as secret_error:
        await recompute_alternative_proof(
            secret_database,
            "source-a",
            _dataset().dataset_id,
        )
    assert secret_error.value.code == "evidence"
    assert "private endpoint" not in str(secret_error.value)
    assert secret_error.value.__cause__ is None

    monkeypatch.setattr(repository_admission_proof, "WRITE_BATCH_SIZE", 1)
    repeated = _alternative()
    repeated_database = SimpleNamespace(
        all=AsyncMock(side_effect=[[repeated], [repeated]])
    )
    with pytest.raises(TwinAdmissionError, match="content evidence"):
        await recompute_alternative_proof(
            repeated_database,
            "source-a",
            _dataset().dataset_id,
        )

    invalid_database = SimpleNamespace(
        all=AsyncMock(return_value=[_alternative(resolved=False)])
    )
    with pytest.raises(TwinAdmissionError, match="content evidence"):
        await recompute_alternative_proof(
            invalid_database,
            "source-a",
            _dataset().dataset_id,
        )


@pytest.mark.asyncio
async def test_full_checkpoint_proof_accepts_only_exact_independent_rows():
    dataset = _dataset()
    database = SimpleNamespace(
        all=AsyncMock(return_value=[_checkpoint("alias-a"), _checkpoint("alias-b")])
    )

    await require_full_checkpoints(database, dataset, 2)

    statement = database.all.await_args.args[0]
    assert "LEFT JOIN" in statement
    assert "checkpoint.run_id = :run_id" in statement
    assert database.all.await_args.kwargs["run_id"] == dataset.run_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "checkpoint_by_field",
    [
        _checkpoint(acquisition_mode="reuse"),
        _checkpoint(completed=False),
        _checkpoint(linked_alias_id=None),
        _checkpoint(processed_count=1),
        _checkpoint(cutoff_at=CUTOFF + dt.timedelta(seconds=1)),
        _checkpoint(membership_hash="invalid"),
    ],
)
async def test_full_checkpoint_proof_rejects_non_independent_rows(
    checkpoint_by_field,
):
    database = SimpleNamespace(all=AsyncMock(return_value=[checkpoint_by_field]))

    with pytest.raises(TwinAdmissionError) as admission_error:
        await require_full_checkpoints(database, _dataset(), 1)

    assert admission_error.value.code == "independence"


@pytest.mark.asyncio
async def test_full_checkpoint_proof_rejects_missing_and_duplicate_aliases():
    empty_database = SimpleNamespace(all=AsyncMock(return_value=[]))
    with pytest.raises(TwinAdmissionError, match="independent acquisition"):
        await require_full_checkpoints(empty_database, _dataset(), 1)

    duplicate_database = SimpleNamespace(
        all=AsyncMock(return_value=[_checkpoint(), _checkpoint()])
    )
    with pytest.raises(TwinAdmissionError, match="independent acquisition"):
        await require_full_checkpoints(duplicate_database, _dataset(), 2)


@pytest.mark.asyncio
async def test_pair_locks_are_deterministic_before_semantic_recompute(monkeypatch):
    baseline = _dataset("z")
    candidate = _dataset("a", intent="requested")
    locked_dataset_ids: list[str] = []

    async def _lock(_database, _source_id, dataset, *, allowed_statuses):
        locked_dataset_ids.append(dataset.dataset_id)
        return {"dataset_id": dataset.dataset_id, "allowed": allowed_statuses}

    async def _proof(_database, dataset, _dataset_row):
        verified_at = (
            BASELINE_VERIFIED
            if dataset.dataset_id == baseline.dataset_id
            else CANDIDATE_VERIFIED
        )
        return _evidence(dataset, verified_at)

    monkeypatch.setattr(repository_admission_proof, "lock_dataset", _lock)
    monkeypatch.setattr(repository_admission_proof, "_dataset_evidence", _proof)

    pair_evidence = await repository_admission_proof.lock_pair_evidence(
        object(),
        baseline,
        candidate,
        candidate_statuses={"verified", "published"},
    )

    assert locked_dataset_ids == sorted((baseline.dataset_id, candidate.dataset_id))
    assert pair_evidence[0].verified_at == BASELINE_VERIFIED
    assert pair_evidence[1].verified_at == CANDIDATE_VERIFIED


def test_pair_match_requires_equal_nonempty_evidence_and_run_order():
    baseline = _dataset("a")
    candidate = _dataset("b")
    baseline_evidence = _evidence(baseline, BASELINE_VERIFIED)
    candidate_evidence = _evidence(candidate, CANDIDATE_VERIFIED)

    require_matching_pair(baseline_evidence, candidate_evidence)

    mismatches = (
        replace(
            candidate_evidence,
            verification=_verification(candidate.dataset_id, coverage_hash="0" * 64),
        ),
        replace(candidate_evidence, alternative=AlternativeProof(0, "f" * 64)),
    )
    for mismatched_evidence in mismatches:
        with pytest.raises(TwinAdmissionError, match="do not match"):
            require_matching_pair(baseline_evidence, mismatched_evidence)

    inadmissible = (
        replace(candidate_evidence, verified_at=CUTOFF),
        _evidence(candidate, CANDIDATE_VERIFIED, medication_membership_count=0),
    )
    for inadmissible_evidence in inadmissible:
        with pytest.raises(TwinAdmissionError, match="content evidence"):
            require_matching_pair(baseline_evidence, inadmissible_evidence)


def test_dataset_evidence_hash_excludes_root_identity_and_verification_time():
    baseline = _dataset("a")
    candidate = _dataset("b")
    baseline_evidence = _evidence(baseline, BASELINE_VERIFIED)
    candidate_evidence = _evidence(candidate, CANDIDATE_VERIFIED)

    baseline_hash = dataset_evidence_hash(
        baseline,
        "9" * 64,
        baseline_evidence,
    )
    candidate_hash = dataset_evidence_hash(
        candidate,
        "9" * 64,
        candidate_evidence,
    )

    assert baseline_hash == candidate_hash
    assert len(baseline_hash) == 64


@pytest.mark.parametrize(
    "dataset_overrides,configuration_hash",
    [
        ({"source_id": "source-b"}, "9" * 64),
        ({"previous_dataset_id": "ffd_" + "p" * 48}, "9" * 64),
        ({"cutoff_at": CUTOFF + dt.timedelta(seconds=1)}, "9" * 64),
        ({"acquisition_contract_hash": "0" * 64}, "9" * 64),
        ({}, "8" * 64),
    ],
)
def test_dataset_evidence_hash_binds_source_and_acquisition_contract(
    dataset_overrides,
    configuration_hash,
):
    dataset = _dataset("a")
    evidence = _evidence(dataset, BASELINE_VERIFIED)
    baseline_hash = dataset_evidence_hash(dataset, "9" * 64, evidence)
    changed_dataset = replace(dataset, **dataset_overrides)
    changed_evidence = evidence
    if "source_id" in dataset_overrides:
        changed_evidence = replace(
            evidence,
            verification=replace(
                evidence.verification,
                source_id=dataset_overrides["source_id"],
            ),
        )

    changed_hash = dataset_evidence_hash(
        changed_dataset,
        configuration_hash,
        changed_evidence,
    )

    assert changed_hash != baseline_hash


@pytest.mark.parametrize(
    "verification_overrides",
    [
        {"list_count": 2},
        {"alias_count": 2},
        {"medication_membership_count": 3},
        {"coverage_hash": "0" * 64},
        {"membership_hash": "0" * 64},
    ],
)
def test_dataset_evidence_hash_binds_verification(verification_overrides):
    dataset = _dataset("a")
    evidence = _evidence(dataset, BASELINE_VERIFIED)
    changed_evidence = replace(
        evidence,
        verification=_verification(dataset.dataset_id, **verification_overrides),
    )

    assert dataset_evidence_hash(dataset, "9" * 64, changed_evidence) != (
        dataset_evidence_hash(dataset, "9" * 64, evidence)
    )


@pytest.mark.parametrize(
    "alternative",
    [AlternativeProof(0, "f" * 64), AlternativeProof(1, "0" * 64)],
)
def test_dataset_evidence_hash_binds_alternatives(alternative):
    dataset = _dataset("a")
    evidence = _evidence(dataset, BASELINE_VERIFIED)
    changed_evidence = replace(evidence, alternative=alternative)

    assert dataset_evidence_hash(dataset, "9" * 64, changed_evidence) != (
        dataset_evidence_hash(dataset, "9" * 64, evidence)
    )


def test_dataset_evidence_hash_sanitizes_identity_mismatch():
    dataset = _dataset("a")
    mismatched_evidence = _evidence(_dataset("b"), BASELINE_VERIFIED)

    with pytest.raises(TwinAdmissionError) as admission_error:
        dataset_evidence_hash(dataset, "9" * 64, mismatched_evidence)

    assert admission_error.value.code == "evidence"
