# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure exact-census plans for dormant formulary synchronization."""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field
from typing import Literal

from process.formulary_fhir.continuation import canonical_cutoff
from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.continuation import medication_search_contract
from process.formulary_fhir.parser import parse_coverage_plan
from process.formulary_fhir.parser import parse_medication_knowledge
from process.formulary_fhir.repository_shared import PriorAliasState
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import medication_variant_hash
from process.formulary_fhir.repository_shared import membership_hash
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.types import CoveragePlanRecord
from process.formulary_fhir.types import CurrentVersionCensus
from process.formulary_fhir.types import MedicationRecord


ACQUISITION_CONTRACT_DOMAIN = "fhir-formulary-sync-contract-v1"
AliasPlanMode = Literal["full", "reuse"]


@dataclass(frozen=True, slots=True, repr=False)
class CoverageWork:
    """Bind one parsed plan alias to its deterministic search contract."""

    plan: CoveragePlanRecord = field(repr=False)
    source_plan_identifier: str = field(repr=False)
    search_contract_hash: str = field(repr=False)

    def __repr__(self) -> str:
        return (
            "CoverageWork("
            f"public_id={self.plan.public_id!r}, alias=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class CoverageCensusPlan:
    """Retain deterministic plan work while exposing only bounded evidence."""

    plans: tuple[CoveragePlanRecord, ...] = field(repr=False)
    work_items: tuple[CoverageWork, ...] = field(repr=False)
    exact_total: int
    search_contract_hash: str = field(repr=False)
    acquisition_contract_hash: str = field(repr=False)

    def __repr__(self) -> str:
        return (
            "CoverageCensusPlan("
            f"exact_total={self.exact_total}, aliases={len(self.work_items)})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class AliasCensusPlan:
    """Describe one complete alias materialization or immutable reuse."""

    medications: tuple[MedicationRecord, ...] = field(repr=False)
    expected_count: int
    membership_hash: str = field(repr=False)
    mode: AliasPlanMode

    def __repr__(self) -> str:
        return (
            "AliasCensusPlan("
            f"expected_count={self.expected_count}, mode={self.mode!r})"
        )


def _require_census(
    census: CurrentVersionCensus,
    *,
    resource_type: str,
    cutoff_at: dt.datetime,
    search_contract_hash: str,
    allow_empty: bool,
) -> None:
    is_exact = bool(
        type(census) is CurrentVersionCensus
        and census.resource_type == resource_type
        and census.cutoff_at == cutoff_at
        and type(census.resources) is tuple
        and type(census.exact_total) is int
        and census.exact_total == len(census.resources)
        and census.search_contract_hash == search_contract_hash
    )
    if not is_exact or (not allow_empty and census.exact_total == 0):
        raise RuntimeError("FHIR formulary current-version census is inconsistent")


def _parsed_plans(
    binding: EnabledSourceBinding,
    census: CurrentVersionCensus,
) -> tuple[CoveragePlanRecord, ...]:
    plans = tuple(
        sorted(
            (
                parse_coverage_plan(
                    resource,
                    canonical_base=binding.config.canonical_base,
                )
                for resource in census.resources
            ),
            key=lambda plan: plan.public_id,
        )
    )
    public_ids: set[str] = set()
    canonical_identities: set[str] = set()
    alias_owner_by_identifier: dict[str, str] = {}
    for plan in plans:
        if (
            plan.public_id in public_ids
            or plan.canonical_identity in canonical_identities
        ):
            raise RuntimeError("FHIR formulary coverage census contains duplicates")
        public_ids.add(plan.public_id)
        canonical_identities.add(plan.canonical_identity)
        for source_plan_identifier in plan.source_plan_identifiers:
            if source_plan_identifier in alias_owner_by_identifier:
                raise RuntimeError("FHIR formulary plan alias ownership is ambiguous")
            alias_owner_by_identifier[source_plan_identifier] = plan.public_id
    return plans


def _coverage_work(
    binding: EnabledSourceBinding,
    plans: tuple[CoveragePlanRecord, ...],
    cutoff_at: dt.datetime,
) -> tuple[CoverageWork, ...]:
    return tuple(
        CoverageWork(
            plan=plan,
            source_plan_identifier=source_plan_identifier,
            search_contract_hash=medication_search_contract(
                binding.config,
                source_plan_identifier,
                cutoff_at,
            ).contract_hash,
        )
        for plan in plans
        for source_plan_identifier in plan.source_plan_identifiers
    )


def _acquisition_hash(
    binding: EnabledSourceBinding,
    cutoff_text: str,
    coverage_contract_hash: str,
    plans: tuple[CoveragePlanRecord, ...],
    work_items: tuple[CoverageWork, ...],
) -> str:
    evidence_by_field = {
        "coverage_search_contract_hash": coverage_contract_hash,
        "cutoff": cutoff_text,
        "medication_search_contracts": [
            {
                "public_id": work.plan.public_id,
                "search_contract_hash": work.search_contract_hash,
            }
            for work in work_items
        ],
        "plans": [
            {
                "aliases": list(plan.source_plan_identifiers),
                "content_hash": plan.content_hash,
                "public_id": plan.public_id,
            }
            for plan in plans
        ],
        "source_configuration_hash": binding.configuration_hash,
    }
    digest = hashlib.sha256()
    digest.update(ACQUISITION_CONTRACT_DOMAIN.encode("ascii"))
    digest.update(b"\n")
    digest.update(json_text(evidence_by_field).encode("utf-8"))
    return digest.hexdigest()


def plan_coverage_census(
    binding: EnabledSourceBinding,
    census: CurrentVersionCensus,
    cutoff: object,
) -> CoverageCensusPlan:
    """Validate and deterministically plan one exact CoveragePlan census."""

    cutoff_at, cutoff_text = canonical_cutoff(cutoff)
    coverage_contract_hash = coverage_plan_search_contract(
        binding.config,
        cutoff_at,
    ).contract_hash
    _require_census(
        census,
        resource_type="List",
        cutoff_at=cutoff_at,
        search_contract_hash=coverage_contract_hash,
        allow_empty=False,
    )
    plans = _parsed_plans(binding, census)
    work_items = _coverage_work(binding, plans, cutoff_at)
    return CoverageCensusPlan(
        plans=plans,
        work_items=work_items,
        exact_total=census.exact_total,
        search_contract_hash=coverage_contract_hash,
        acquisition_contract_hash=_acquisition_hash(
            binding,
            cutoff_text,
            coverage_contract_hash,
            plans,
            work_items,
        ),
    )


def _parsed_medications(
    census: CurrentVersionCensus,
    source_plan_identifier: str,
) -> tuple[MedicationRecord, ...]:
    medications = tuple(
        sorted(
            (parse_medication_knowledge(resource) for resource in census.resources),
            key=lambda medication: medication.upstream_medication_id,
        )
    )
    medication_ids: set[str] = set()
    for medication in medications:
        if medication.upstream_medication_id in medication_ids:
            raise RuntimeError("FHIR formulary alias census contains duplicates")
        if (
            medication.source_plan_identifiers
            and source_plan_identifier not in medication.source_plan_identifiers
        ):
            raise RuntimeError("FHIR formulary alias census crossed plan membership")
        medication_ids.add(medication.upstream_medication_id)
    return medications


def _require_prior_ownership(
    binding: EnabledSourceBinding,
    work: CoverageWork,
    prior: PriorAliasState,
) -> None:
    expected_owner = (
        binding.source_id,
        work.plan.public_id,
        work.source_plan_identifier,
    )
    actual_owner = (
        prior.source_id,
        prior.public_id,
        prior.source_plan_identifier,
    )
    if actual_owner != expected_owner:
        raise RuntimeError("FHIR formulary prior alias ownership is invalid")


def plan_alias_census(
    binding: EnabledSourceBinding,
    work: CoverageWork,
    census: CurrentVersionCensus,
    cutoff: object,
    prior: PriorAliasState | None,
) -> AliasCensusPlan:
    """Choose exact immutable reuse only after full membership comparison."""

    cutoff_at, _cutoff_text = canonical_cutoff(cutoff)
    expected_contract_hash = medication_search_contract(
        binding.config,
        work.source_plan_identifier,
        cutoff_at,
    ).contract_hash
    if expected_contract_hash != work.search_contract_hash:
        raise RuntimeError("FHIR formulary alias search plan is inconsistent")
    _require_census(
        census,
        resource_type="MedicationKnowledge",
        cutoff_at=cutoff_at,
        search_contract_hash=expected_contract_hash,
        allow_empty=True,
    )
    medications = _parsed_medications(census, work.source_plan_identifier)
    variants_by_medication_id = {
        medication.upstream_medication_id: medication_variant_hash(medication)
        for medication in medications
    }
    computed_membership_hash = membership_hash(variants_by_medication_id)
    mode: AliasPlanMode = "full"
    if prior is not None:
        _require_prior_ownership(binding, work, prior)
        if (
            prior.expected_count == census.exact_total
            and prior.membership_hash == computed_membership_hash
        ):
            mode = "reuse"
    return AliasCensusPlan(
        medications=medications,
        expected_count=census.exact_total,
        membership_hash=computed_membership_hash,
        mode=mode,
    )


__all__ = (
    "AliasCensusPlan",
    "CoverageCensusPlan",
    "CoverageWork",
    "plan_alias_census",
    "plan_coverage_census",
)
