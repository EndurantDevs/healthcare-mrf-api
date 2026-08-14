# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure repository-sync contracts for the exact UHC drug artifact set."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field

from process.formulary_fhir.repository import AliasRef
from process.formulary_fhir.repository import AliasVersionResult
from process.formulary_fhir.repository import CompletedAliasCheckpoint
from process.formulary_fhir.repository import CoveragePlanWriteResult
from process.formulary_fhir.repository import DatasetRef
from process.formulary_fhir.repository import DatasetVerification
from process.formulary_fhir.repository_proof import source_medication_variant_hash
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.source import LIBRARY_ONLY_LAUNCH_MODE
from process.formulary_fhir.source_artifact_contract import (
    VerifiedSourceArtifactSet,
)
from process.formulary_fhir.uhc_drug_normalization import SPOOL_CONTRACT
from process.formulary_fhir.uhc_drug_parser import MEDICATION_PROJECTION_CONTRACT
from process.formulary_fhir.uhc_drug_parser import PLAN_PROJECTION_CONTRACT
from process.formulary_fhir.uhc_drug_parser_contract import PLAN_ALIAS_DOMAIN
from process.formulary_fhir.uhc_drug_parser_contract import (
    UHCDrugPlanMaterialization,
)
from process.formulary_fhir.uhc_drug_parser_contract import UHCDrugSpoolEvidence
from process.formulary_fhir.uhc_source import UHC_FORMULARY_CANONICAL_BASE
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


UHC_DRUG_SYNC_CONTRACT = "uhc-official-formulary-repository-sync-v1"
UHC_DRUG_PARTIAL_SYNC_CONTRACT = (
    "uhc-official-formulary-repository-sync-partial-v1"
)


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugMembershipProof:
    """Bind one sorted plan membership to its repository hash."""

    medication_count: int
    membership_sha256: str = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.medication_count) is not int or self.medication_count <= 0:
            raise ValueError("UHC drug medication count is invalid")
        strict_hash(self.membership_sha256, "UHC drug membership hash")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugSynchronizationResult:
    """Expose one verified UHC repository root without publishing it."""

    dataset: DatasetRef = field(repr=False)
    verification: DatasetVerification = field(repr=False)
    evidence: UHCDrugSpoolEvidence = field(repr=False)
    full_alias_count: int
    resumed_alias_count: int

    def __post_init__(self) -> None:
        if (
            type(self.dataset) is not DatasetRef
            or self.dataset.status != "verified"
            or type(self.verification) is not DatasetVerification
            or type(self.evidence) is not UHCDrugSpoolEvidence
            or self.verification.source_id != self.dataset.source_id
            or self.verification.dataset_id != self.dataset.dataset_id
            or self.verification.list_count != self.evidence.plan_count
            or self.verification.alias_count != self.evidence.plan_count
            or self.verification.medication_membership_count
            != self.evidence.medication_membership_count
            or self.full_alias_count != self.evidence.plan_count
            or type(self.resumed_alias_count) is not int
            or not 0 <= self.resumed_alias_count <= self.full_alias_count
        ):
            raise ValueError("UHC drug synchronization result is invalid")


def _canonical_catalog_timestamp(raw_timestamp: str) -> dt.datetime:
    try:
        parsed_timestamp = dt.datetime.fromisoformat(
            raw_timestamp[:-1] + "+00:00"
            if raw_timestamp.endswith("Z")
            else raw_timestamp
        )
    except (AttributeError, ValueError):
        raise ValueError("UHC drug catalog timestamp is invalid") from None
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise ValueError("UHC drug catalog timestamp is invalid")
    normalized_timestamp = parsed_timestamp.astimezone(dt.UTC)
    if normalized_timestamp.isoformat().replace("+00:00", "Z") != raw_timestamp:
        raise ValueError("UHC drug catalog timestamp is invalid")
    return normalized_timestamp


def validate_uhc_drug_sync_inputs(
    binding: EnabledSourceBinding,
    artifacts: VerifiedSourceArtifactSet,
    evidence: UHCDrugSpoolEvidence,
    cutoff_at: dt.datetime,
) -> dt.datetime:
    """Bind exact source, artifacts, spool evidence, and as-of boundary."""

    normalized_cutoff = utc_timestamp(cutoff_at, "UHC drug cutoff")
    if (
        type(binding) is not EnabledSourceBinding
        or binding.source_id != UHC_FORMULARY_SOURCE_ID
        or binding.config.canonical_base != UHC_FORMULARY_CANONICAL_BASE
        or binding.launch_mode != LIBRARY_ONLY_LAUNCH_MODE
        or binding.alternative_correction is not None
        or type(artifacts) is not VerifiedSourceArtifactSet
        or type(evidence) is not UHCDrugSpoolEvidence
        or artifacts.source_id != binding.source_id
        or evidence.source_id != binding.source_id
        or evidence.source_file_set_sha256
        != artifacts.source_file_set_sha256
        or evidence.artifact_set_sha256 != artifacts.artifact_set_sha256
        or evidence.file_count != len(artifacts.artifacts)
        or evidence.expected_file_count != 48
        or evidence.excluded_file_count != 48 - len(artifacts.artifacts)
        or evidence.max_last_updated_at is None
        or evidence.max_last_updated_at > normalized_cutoff
        or any(
            artifact.verified_at > normalized_cutoff
            for artifact in artifacts.artifacts
        )
    ):
        raise ValueError("UHC drug synchronization input is invalid")
    family_count_by_name = {
        family: sum(
            artifact.identity.family == family
            for artifact in artifacts.artifacts
        )
        for family in ("cs", "ifp")
    }
    if any(count > 24 for count in family_count_by_name.values()):
        raise ValueError("UHC drug artifact census is incomplete")
    if any(
        _canonical_catalog_timestamp(artifact.identity.catalog_modified_at)
        > normalized_cutoff
        for artifact in artifacts.artifacts
    ):
        raise ValueError("UHC drug artifact observation is after the cutoff")
    return normalized_cutoff


def uhc_drug_sync_contract_hash(
    binding: EnabledSourceBinding,
    artifacts: VerifiedSourceArtifactSet,
    evidence: UHCDrugSpoolEvidence,
    cutoff_at: dt.datetime,
) -> str:
    """Hash every source, parser, projection, and exact-census input."""

    normalized_cutoff = validate_uhc_drug_sync_inputs(binding, artifacts, evidence, cutoff_at)
    sync_contract = (
        UHC_DRUG_SYNC_CONTRACT
        if evidence.is_coverage_complete
        else UHC_DRUG_PARTIAL_SYNC_CONTRACT
    )
    evidence_by_field = {
        "duplicate_count": evidence.duplicate_count,
        "file_count": evidence.file_count,
        "max_last_updated_at": utc_timestamp(
            evidence.max_last_updated_at,
            "UHC drug maximum update timestamp",
        ).isoformat(),
        "medication_membership_count": evidence.medication_membership_count,
        "plan_count": evidence.plan_count,
        "raw_plan_entry_count": evidence.raw_plan_entry_count,
        "raw_record_count": evidence.raw_record_count,
        "spool_content_sha256": evidence.spool_content_sha256,
        "superseded_count": evidence.superseded_count,
    }
    if not evidence.is_coverage_complete:
        evidence_by_field.update(
            {
                "excluded_file_count": evidence.excluded_file_count,
                "expected_file_count": evidence.expected_file_count,
            }
        )
    contract_by_field = {
        "artifact_set_sha256": artifacts.artifact_set_sha256,
        "cutoff_at": normalized_cutoff.isoformat(),
        "evidence": evidence_by_field,
        "medication_projection_contract": MEDICATION_PROJECTION_CONTRACT,
        "plan_alias_domain": PLAN_ALIAS_DOMAIN,
        "plan_projection_contract": PLAN_PROJECTION_CONTRACT,
        "raw_listing_projection_sha256": (
            artifacts.raw_listing_projection_sha256
        ),
        "source_configuration_hash": binding.configuration_hash,
        "source_file_set_sha256": artifacts.source_file_set_sha256,
        "source_id": binding.source_id,
        "spool_contract": SPOOL_CONTRACT,
        "sync_contract": sync_contract,
    }
    digest = hashlib.sha256()
    digest.update(sync_contract.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(contract_by_field).encode("utf-8"))
    return digest.hexdigest()


def uhc_drug_membership_proof(
    materialized_plan: UHCDrugPlanMaterialization,
) -> UHCDrugMembershipProof:
    """Compute the repository's exact full-membership hash before writing."""

    if type(materialized_plan) is not UHCDrugPlanMaterialization:
        raise ValueError("UHC drug plan materialization is invalid")
    medication_ids = tuple(
        medication.upstream_medication_id
        for medication in materialized_plan.medications
    )
    if medication_ids != tuple(sorted(set(medication_ids))):
        raise ValueError("UHC drug medication order is invalid")
    variants_by_medication_id = {
        medication.upstream_medication_id: source_medication_variant_hash(
            medication,
            None,
        )
        for medication in materialized_plan.medications
    }
    return UHCDrugMembershipProof(
        medication_count=len(materialized_plan.medications),
        membership_sha256=membership_hash(variants_by_medication_id),
    )


def require_exact_coverage_write(
    write_result: CoveragePlanWriteResult,
    dataset: DatasetRef,
    materialized_plan: UHCDrugPlanMaterialization,
) -> AliasRef:
    """Require one exact repository plan version and sole source alias."""

    expected_coverage_version_id = stable_id(
        "ffcv_",
        dataset.source_id,
        materialized_plan.coverage_plan.public_id,
        materialized_plan.coverage_plan.content_hash,
    )
    if (
        type(write_result) is not CoveragePlanWriteResult
        or write_result.dataset != dataset
        or write_result.coverage_version_id != expected_coverage_version_id
        or type(write_result.aliases) is not tuple
        or len(write_result.aliases) != 1
    ):
        raise RuntimeError("UHC drug coverage write is inconsistent")
    alias = write_result.aliases[0]
    expected_alias = AliasRef(
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
    if alias != expected_alias:
        raise RuntimeError("UHC drug plan alias is inconsistent")
    return alias


def require_exact_completed_checkpoint(
    checkpoint: CompletedAliasCheckpoint,
    dataset: DatasetRef,
    alias: AliasRef,
    membership_proof: UHCDrugMembershipProof,
) -> None:
    """Require a restart checkpoint for the same full source materialization."""

    if type(checkpoint) is not CompletedAliasCheckpoint:
        raise RuntimeError("UHC drug completed checkpoint is inconsistent")
    expected_alias_version_id = stable_id(
        "ffav_",
        dataset.source_id,
        alias.alias_id,
        membership_proof.membership_sha256,
    )
    expected_values = (
        dataset.source_id,
        dataset.dataset_id,
        alias.alias_id,
        expected_alias_version_id,
        membership_proof.medication_count,
        membership_proof.membership_sha256,
        "full",
    )
    observed_values = (
        checkpoint.source_id,
        checkpoint.dataset_id,
        checkpoint.alias_id,
        checkpoint.alias_version_id,
        checkpoint.expected_count,
        checkpoint.membership_hash,
        checkpoint.acquisition_mode,
    )
    if observed_values != expected_values:
        raise RuntimeError("UHC drug completed checkpoint is inconsistent")


def require_exact_alias_write(
    write_result: AliasVersionResult,
    dataset: DatasetRef,
    alias: AliasRef,
    membership_proof: UHCDrugMembershipProof,
) -> None:
    """Require the atomic full alias write to match its precomputed proof."""

    if type(write_result) is not AliasVersionResult:
        raise RuntimeError("UHC drug alias write is inconsistent")
    expected_values = (
        dataset.source_id,
        dataset.dataset_id,
        alias.alias_id,
        stable_id(
            "ffav_",
            dataset.source_id,
            alias.alias_id,
            membership_proof.membership_sha256,
        ),
        membership_proof.medication_count,
        membership_proof.membership_sha256,
        "full",
    )
    observed_values = (
        write_result.source_id,
        write_result.dataset_id,
        write_result.alias_id,
        write_result.alias_version_id,
        write_result.membership_count,
        write_result.membership_hash,
        write_result.acquisition_mode,
    )
    if observed_values != expected_values:
        raise RuntimeError("UHC drug alias write is inconsistent")


def require_exact_verification(
    dataset: DatasetRef,
    evidence: UHCDrugSpoolEvidence,
    verification: DatasetVerification,
) -> None:
    """Require exact repository census for the independently built root."""

    if (
        type(verification) is not DatasetVerification
        or verification.source_id != dataset.source_id
        or verification.dataset_id != dataset.dataset_id
        or verification.list_count != evidence.plan_count
        or verification.alias_count != evidence.plan_count
        or verification.medication_membership_count
        != evidence.medication_membership_count
    ):
        raise RuntimeError("UHC drug repository verification is inconsistent")


def require_exact_predecessor(
    dataset: DatasetRef,
    current_dataset: DatasetRef | None,
) -> None:
    """Reject a pointer race between candidate creation and repository reads."""

    current_dataset_id = (
        current_dataset.dataset_id if current_dataset is not None else None
    )
    if dataset.previous_dataset_id != current_dataset_id:
        raise RuntimeError("UHC drug repository predecessor changed")


def validate_uhc_drug_run_id(run_id: str) -> str:
    """Validate one bounded external run identity before repository mutation."""

    return strict_text(run_id, "UHC drug run id", 64)


__all__ = (
    "UHC_DRUG_SYNC_CONTRACT",
    "UHC_DRUG_PARTIAL_SYNC_CONTRACT",
    "UHCDrugMembershipProof",
    "UHCDrugSynchronizationResult",
    "require_exact_alias_write",
    "require_exact_completed_checkpoint",
    "require_exact_coverage_write",
    "require_exact_predecessor",
    "require_exact_verification",
    "uhc_drug_membership_proof",
    "uhc_drug_sync_contract_hash",
    "validate_uhc_drug_run_id",
    "validate_uhc_drug_sync_inputs",
)
