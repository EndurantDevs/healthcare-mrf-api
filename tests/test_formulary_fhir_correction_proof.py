# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused proof that source correction reaches storage and alias identity."""

from __future__ import annotations

import datetime as dt

import pytest

from process.formulary_fhir import repository_batch
from process.formulary_fhir import synchronizer as sync_module
from process.formulary_fhir.planner import AliasCensusPlan
from process.formulary_fhir.planner import CoverageWork
from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository_proof import source_medication_variant_hash
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.types import AlternativeCorrection
from process.formulary_fhir.types import CoveragePlanRecord
from process.formulary_fhir.types import MedicationRecord


CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)
CORRECTION = AlternativeCorrection(
    prefix="PRE-",
    rule_version="prefix-rule-v1",
)


def _medication(
    medication_id: str,
    *,
    alternatives: tuple[str, ...] = (),
) -> MedicationRecord:
    return MedicationRecord(
        upstream_medication_id=medication_id,
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="active",
        drug_name="Synthetic medication",
        rxnorm_id=None,
        ndc11=None,
        codings=(),
        raw_extensions=(),
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        drug_tier="preferred",
        prior_authorization=False,
        step_therapy=False,
        quantity_limit=False,
        alternative_references=alternatives,
        content_hash="a" * 64,
    )


def _dataset() -> DatasetRef:
    return DatasetRef(
        source_id="source-alpha",
        dataset_id="ffd_" + "1" * 48,
        run_id="synthetic-run",
        previous_dataset_id=None,
        cutoff_at=CUTOFF,
        acquisition_contract_hash="b" * 64,
        intent="none",
        status="building",
    )


def _work_item() -> sync_module._AliasWorkItem:
    plan = CoveragePlanRecord(
        upstream_list_id="list-a",
        public_id="fhir_" + "1" * 26,
        canonical_identity="https://synthetic.invalid/fhir/List/list-a",
        upstream_version_id="1",
        upstream_last_updated=CUTOFF,
        status="current",
        title="Synthetic plan",
        name="Synthetic plan",
        upstream_date=CUTOFF,
        period_start=None,
        period_end=None,
        source_plan_identifiers=("SYNTHETIC-PLAN",),
        raw_identifiers=(),
        raw_extensions=(),
        content_hash="c" * 64,
    )
    work = CoverageWork(
        plan=plan,
        source_plan_identifier="SYNTHETIC-PLAN",
        search_contract_hash="d" * 64,
    )
    alias = AliasRef(
        source_id="source-alpha",
        public_id=plan.public_id,
        alias_id="ffa_" + "2" * 48,
        source_plan_identifier="SYNTHETIC-PLAN",
    )
    return sync_module._AliasWorkItem(work, alias)


class _Repository:
    def __init__(self) -> None:
        self.alias_write = None

    async def put_alias_version(self, alias_write):
        self.alias_write = alias_write
        return AliasVersionResult(
            source_id=alias_write.dataset.source_id,
            dataset_id=alias_write.dataset.dataset_id,
            alias_id=alias_write.alias.alias_id,
            alias_version_id="ffav_" + "3" * 48,
            membership_count=alias_write.expected_count,
            membership_hash=membership_hash(
                {
                    medication.upstream_medication_id: (
                        source_medication_variant_hash(
                            medication,
                            alias_write.alternative_correction,
                        )
                    )
                    for medication in alias_write.medications
                }
            ),
            acquisition_mode="full",
        )


def test_correction_changes_source_proof_without_changing_legacy_hash():
    medication = _medication("med-1")

    legacy_hash = medication_variant_hash(medication)
    absent_policy_hash = source_medication_variant_hash(medication, None)
    corrected_policy_hash = source_medication_variant_hash(
        medication,
        CORRECTION,
    )

    assert absent_policy_hash == legacy_hash
    assert corrected_policy_hash != legacy_hash


def test_alternative_batch_applies_explicit_correction_evidence():
    referencing_medication = _medication(
        "med-1",
        alternatives=("MedicationKnowledge/med-2",),
    )
    corrected_medication = _medication("PRE-med-2")

    evidence_rows = repository_batch._alternative_rows(
        (referencing_medication, corrected_medication),
        {"med-1", "PRE-med-2"},
        correction=CORRECTION,
    )

    assert len(evidence_rows) == 1
    _medication_id, evidence = evidence_rows[0]
    assert evidence.corrected_reference == "MedicationKnowledge/PRE-med-2"
    assert evidence.resolved_medication_id == "PRE-med-2"
    assert evidence.rule_version == "prefix-rule-v1"


@pytest.mark.asyncio
async def test_alias_write_receives_correction_bound_to_planner_hash():
    medication = _medication("med-1")
    work_item = _work_item()
    dataset = _dataset()
    corrected_hash = membership_hash(
        {
            medication.upstream_medication_id: source_medication_variant_hash(
                medication,
                CORRECTION,
            )
        }
    )
    alias_plan = AliasCensusPlan((medication,), 1, corrected_hash, "full")
    repository = _Repository()

    await sync_module._write_alias_plan(
        repository,
        dataset,
        work_item,
        alias_plan,
        None,
        1,
        CORRECTION,
    )

    assert repository.alias_write.alternative_correction == CORRECTION
