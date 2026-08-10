# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact full-alias writes for one UHC formulary repository root."""

from __future__ import annotations

import datetime as dt
from typing import Any, Literal

from process.formulary_fhir.repository import AliasCompletionFence
from process.formulary_fhir.repository import AliasVersionWrite
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository_shared import PublicationIntent
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.uhc_drug_parser_contract import (
    UHCDrugPlanMaterialization,
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
    uhc_drug_membership_proof,
)


PlanWriteOutcome = Literal["resumed", "written"]


def require_exact_uhc_dataset(
    dataset: DatasetRef,
    *,
    binding: EnabledSourceBinding,
    run_id: str,
    cutoff_at: dt.datetime,
    contract_hash: str,
    intent: PublicationIntent,
) -> None:
    """Reject any repository root that differs from the exact sync request."""

    expected_values = (
        binding.source_id,
        run_id,
        cutoff_at,
        contract_hash,
        intent,
    )
    observed_values = (
        dataset.source_id,
        dataset.run_id,
        dataset.cutoff_at,
        dataset.acquisition_contract_hash,
        dataset.intent,
    )
    if (
        type(dataset) is not DatasetRef
        or observed_values != expected_values
        or dataset.status not in {"building", "verified"}
    ):
        raise RuntimeError("UHC drug repository dataset is inconsistent")


async def write_or_resume_uhc_plan(
    repository: Any,
    dataset: DatasetRef,
    materialized_plan: UHCDrugPlanMaterialization,
) -> PlanWriteOutcome:
    """Write one full alias or prove its exact completed checkpoint."""

    membership_proof = uhc_drug_membership_proof(materialized_plan)
    plan_write = await repository.put_coverage_plan(
        dataset=dataset,
        plan=materialized_plan.coverage_plan,
    )
    alias = require_exact_coverage_write(
        plan_write,
        dataset,
        materialized_plan,
    )
    checkpoint = await repository.completed_alias_checkpoint(
        dataset=dataset,
        alias=alias,
    )
    if checkpoint is not None:
        require_exact_completed_checkpoint(
            checkpoint,
            dataset,
            alias,
            membership_proof,
        )
        return "resumed"
    completion_fence = await repository.next_alias_completion_fence(
        dataset=dataset,
        alias=alias,
    )
    if type(completion_fence) is not AliasCompletionFence:
        raise RuntimeError("UHC drug alias completion fence is invalid")
    alias_write = await repository.put_alias_version(
        AliasVersionWrite(
            dataset=dataset,
            alias=alias,
            expected_count=membership_proof.medication_count,
            medications=materialized_plan.medications,
            fence_token=completion_fence.fence_token,
            alternative_correction=None,
        )
    )
    require_exact_alias_write(
        alias_write,
        dataset,
        alias,
        membership_proof,
    )
    return "written"


__all__ = (
    "PlanWriteOutcome",
    "require_exact_uhc_dataset",
    "write_or_resume_uhc_plan",
)
