# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundary proof for the UHC repository synchronization contract."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

import pytest

import process.formulary_fhir.uhc_drug_parser as parser
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import CompletedAliasCheckpoint
from process.formulary_fhir.repository import CoveragePlanWriteResult
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.source_artifact_contract import artifact_set_sha256
from process.formulary_fhir.source_artifact_contract import artifact_sort_key
from process.formulary_fhir.uhc_drug_parser import load_spooled_uhc_plan
from process.formulary_fhir.uhc_drug_parser import spooled_uhc_plan_keys
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugMembershipProof,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    UHCDrugSynchronizationResult,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    _canonical_catalog_timestamp,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_alias_write,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_completed_checkpoint,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_coverage_write,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    require_exact_verification,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    uhc_drug_membership_proof,
)
from process.formulary_fhir.uhc_drug_sync_contract import (
    validate_uhc_drug_sync_inputs,
)
from tests.test_uhc_drug_sync import _prepared_source


CUTOFF = dt.datetime(2026, 8, 10, 12, tzinfo=dt.UTC)


def _materialized_plan(monkeypatch, tmp_path):
    binding, artifacts, spool_path, evidence = _prepared_source(
        monkeypatch,
        tmp_path,
    )
    with parser.open_verified_uhc_drug_spool(
        spool_path,
        evidence,
        artifacts,
    ) as spool_snapshot:
        plan_key = spooled_uhc_plan_keys(spool_snapshot)[0]
        materialized_plan = load_spooled_uhc_plan(
            spool_snapshot,
            plan_key,
            source_id=binding.source_id,
            canonical_base=binding.config.canonical_base,
            evidence=evidence,
        )
    return binding, artifacts, evidence, materialized_plan


def _dataset(source_id: str) -> DatasetRef:
    return DatasetRef(
        source_id=source_id,
        dataset_id="ffd_" + "1" * 48,
        run_id="uhc-contract-boundary",
        previous_dataset_id=None,
        cutoff_at=CUTOFF,
        acquisition_contract_hash="2" * 64,
        intent="none",
        status="verified",
    )


def _alias(dataset: DatasetRef, materialized_plan) -> AliasRef:
    return AliasRef(
        source_id=dataset.source_id,
        public_id=materialized_plan.coverage_plan.public_id,
        alias_id=stable_id(
            "ffa_",
            dataset.source_id,
            materialized_plan.coverage_plan.public_id,
            materialized_plan.key.source_plan_identifier,
        ),
        source_plan_identifier=materialized_plan.key.source_plan_identifier,
    )


def _repacked_artifacts(artifacts, changed_artifact):
    exact_artifacts = tuple(
        sorted(
            (changed_artifact, *artifacts.artifacts[1:]),
            key=artifact_sort_key,
        )
    )
    return replace(
        artifacts,
        artifacts=exact_artifacts,
        artifact_set_sha256=artifact_set_sha256(exact_artifacts),
    )


@pytest.mark.parametrize("medication_count", (0, -1, True, "1"))
def test_membership_proof_rejects_invalid_counts(medication_count) -> None:
    with pytest.raises(ValueError, match="medication count"):
        UHCDrugMembershipProof(medication_count, "a" * 64)


@pytest.mark.parametrize(
    "raw_timestamp",
    (object(), "not-a-timestamp", "2026-08-10T00:00:00", "2026-08-10T00:00:00+00:00"),
)
def test_catalog_timestamp_requires_canonical_utc(raw_timestamp) -> None:
    with pytest.raises(ValueError, match="catalog timestamp"):
        _canonical_catalog_timestamp(raw_timestamp)


def test_sync_inputs_require_exact_family_census(monkeypatch, tmp_path) -> None:
    binding, artifacts, _spool_path, evidence = _prepared_source(monkeypatch, tmp_path)
    first = artifacts.artifacts[0]
    changed = replace(first, identity=replace(first.identity, family="ifp"))
    changed_set = _repacked_artifacts(artifacts, changed)
    changed_evidence = replace(
        evidence,
        artifact_set_sha256=changed_set.artifact_set_sha256,
    )

    with pytest.raises(ValueError, match="census is incomplete"):
        validate_uhc_drug_sync_inputs(
            binding,
            changed_set,
            changed_evidence,
            CUTOFF,
        )


def test_sync_inputs_reject_future_catalog_observation(monkeypatch, tmp_path) -> None:
    binding, artifacts, _spool_path, evidence = _prepared_source(monkeypatch, tmp_path)
    first = artifacts.artifacts[0]
    future_identity = replace(
        first.identity,
        catalog_modified_at="2026-08-10T12:00:01Z",
    )
    changed_set = _repacked_artifacts(
        artifacts,
        replace(first, identity=future_identity),
    )
    changed_evidence = replace(
        evidence,
        artifact_set_sha256=changed_set.artifact_set_sha256,
    )

    with pytest.raises(ValueError, match="after the cutoff"):
        validate_uhc_drug_sync_inputs(
            binding,
            changed_set,
            changed_evidence,
            CUTOFF,
        )


def test_membership_proof_rejects_type_and_order_drift(monkeypatch, tmp_path) -> None:
    _binding, _artifacts, _evidence, materialized_plan = _materialized_plan(
        monkeypatch,
        tmp_path,
    )
    with pytest.raises(ValueError, match="materialization is invalid"):
        uhc_drug_membership_proof(object())
    duplicated = replace(
        materialized_plan,
        medications=(materialized_plan.medications[0],) * 2,
    )
    with pytest.raises(ValueError, match="medication order"):
        uhc_drug_membership_proof(duplicated)


def test_synchronization_result_rejects_count_drift(monkeypatch, tmp_path) -> None:
    binding, _artifacts, evidence, unused_plan = _materialized_plan(
        monkeypatch,
        tmp_path,
    )
    assert unused_plan.medications
    dataset = _dataset(binding.source_id)
    verification = DatasetVerification(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        list_count=evidence.plan_count,
        alias_count=evidence.plan_count,
        medication_membership_count=evidence.medication_membership_count,
        coverage_hash="3" * 64,
        membership_hash="4" * 64,
    )
    with pytest.raises(ValueError, match="synchronization result"):
        UHCDrugSynchronizationResult(
            dataset=dataset,
            verification=verification,
            evidence=evidence,
            full_alias_count=evidence.plan_count,
            resumed_alias_count=evidence.plan_count + 1,
        )


def test_repository_write_guards_bind_exact_aliases(monkeypatch, tmp_path) -> None:
    binding, _artifacts, _evidence, materialized_plan = _materialized_plan(
        monkeypatch,
        tmp_path,
    )
    dataset = _dataset(binding.source_id)
    alias = _alias(dataset, materialized_plan)
    coverage_write = CoveragePlanWriteResult(
        dataset=dataset,
        coverage_version_id=stable_id(
            "ffcv_",
            dataset.source_id,
            materialized_plan.coverage_plan.public_id,
            materialized_plan.coverage_plan.content_hash,
        ),
        aliases=(alias,),
    )
    assert require_exact_coverage_write(
        coverage_write,
        dataset,
        materialized_plan,
    ) == alias
    with pytest.raises(RuntimeError, match="coverage write"):
        require_exact_coverage_write(object(), dataset, materialized_plan)
    with pytest.raises(RuntimeError, match="plan alias"):
        require_exact_coverage_write(
            replace(coverage_write, aliases=(replace(alias, alias_id="ffa_" + "9" * 48),)),
            dataset,
            materialized_plan,
        )


def test_checkpoint_and_alias_write_guards_bind_membership(monkeypatch, tmp_path) -> None:
    binding, _artifacts, _evidence, materialized_plan = _materialized_plan(
        monkeypatch,
        tmp_path,
    )
    dataset = _dataset(binding.source_id)
    alias = _alias(dataset, materialized_plan)
    proof = uhc_drug_membership_proof(materialized_plan)
    alias_version_id = stable_id(
        "ffav_",
        dataset.source_id,
        alias.alias_id,
        proof.membership_sha256,
    )
    checkpoint = CompletedAliasCheckpoint(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        alias_id=alias.alias_id,
        alias_version_id=alias_version_id,
        expected_count=proof.medication_count,
        membership_hash=proof.membership_sha256,
        acquisition_mode="full",
    )
    require_exact_completed_checkpoint(checkpoint, dataset, alias, proof)
    with pytest.raises(RuntimeError, match="completed checkpoint"):
        require_exact_completed_checkpoint(object(), dataset, alias, proof)
    with pytest.raises(RuntimeError, match="completed checkpoint"):
        require_exact_completed_checkpoint(
            replace(checkpoint, expected_count=proof.medication_count + 1),
            dataset,
            alias,
            proof,
        )
    write_result = AliasVersionResult(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        alias_id=alias.alias_id,
        alias_version_id=alias_version_id,
        membership_count=proof.medication_count,
        membership_hash=proof.membership_sha256,
        acquisition_mode="full",
    )
    require_exact_alias_write(write_result, dataset, alias, proof)
    with pytest.raises(RuntimeError, match="alias write"):
        require_exact_alias_write(object(), dataset, alias, proof)
    with pytest.raises(RuntimeError, match="alias write"):
        require_exact_alias_write(
            replace(write_result, membership_count=proof.medication_count + 1),
            dataset,
            alias,
            proof,
        )


def test_verification_guard_rejects_count_drift(monkeypatch, tmp_path) -> None:
    binding, _artifacts, evidence, unused_plan = _materialized_plan(
        monkeypatch,
        tmp_path,
    )
    assert unused_plan.medications
    dataset = _dataset(binding.source_id)
    verification = DatasetVerification(
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        list_count=evidence.plan_count,
        alias_count=evidence.plan_count,
        medication_membership_count=evidence.medication_membership_count,
        coverage_hash="5" * 64,
        membership_hash="6" * 64,
    )
    require_exact_verification(dataset, evidence, verification)
    with pytest.raises(RuntimeError, match="verification"):
        require_exact_verification(dataset, evidence, object())
    with pytest.raises(RuntimeError, match="verification"):
        require_exact_verification(
            dataset,
            evidence,
            replace(verification, alias_count=evidence.plan_count + 1),
        )
