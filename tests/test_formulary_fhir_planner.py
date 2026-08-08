# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tests for deterministic exact-census formulary synchronization plans."""

from __future__ import annotations

import datetime as dt
import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from process.formulary_fhir.continuation import coverage_plan_search_contract
from process.formulary_fhir.continuation import medication_search_contract
from process.formulary_fhir.planner import plan_alias_census
from process.formulary_fhir.planner import plan_coverage_census
from process.formulary_fhir.repository import PriorAliasState
from process.formulary_fhir.source import EnabledSourceBinding
from process.formulary_fhir.types import AlternativeCorrection
from process.formulary_fhir.types import CurrentVersionCensus
from process.formulary_fhir.types import enabled_source_config


FIXTURES = Path(__file__).parent / "fixtures" / "formulary_fhir"
CUTOFF = dt.datetime(2026, 8, 7, 12, tzinfo=dt.UTC)


def _fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _binding(
    correction: AlternativeCorrection | None = None,
) -> EnabledSourceBinding:
    config = enabled_source_config(
        canonical_base="https://synthetic.invalid/fhir",
        enabled=True,
        runtime_config_json={
            "timeout_seconds": 30,
            "max_attempts": 2,
            "page_size": 50,
            "max_pages": 100,
            "max_total_resources": 5_000,
            "max_response_bytes": 1_048_576,
        },
    )
    return EnabledSourceBinding(
        "source-alpha",
        config,
        "a" * 64,
        alternative_correction=correction,
    )


def _coverage_census(
    resources: tuple[dict[str, object], ...],
) -> CurrentVersionCensus:
    binding = _binding()
    return CurrentVersionCensus(
        resource_type="List",
        cutoff_at=CUTOFF,
        exact_total=len(resources),
        resources=resources,
        search_contract_hash=coverage_plan_search_contract(
            binding.config,
            CUTOFF,
        ).contract_hash,
    )


def _medication_census(
    alias: str,
    resources: tuple[dict[str, object], ...],
) -> CurrentVersionCensus:
    binding = _binding()
    return CurrentVersionCensus(
        resource_type="MedicationKnowledge",
        cutoff_at=CUTOFF,
        exact_total=len(resources),
        resources=resources,
        search_contract_hash=medication_search_contract(
            binding.config,
            alias,
            CUTOFF,
        ).contract_hash,
    )


def _one_alias_plan(alias: str = "SYNTH-A") -> dict[str, object]:
    resource = _fixture("coverage_plan.json")
    resource["extension"] = [
        extension
        for extension in resource["extension"]
        if extension.get("valueString") == alias
        or "PlanID-extension" not in str(extension.get("url"))
    ]
    return resource


def _medication(alias: str, medication_id: str = "synthetic-drug-a"):
    resource = _fixture("medication_a.json")
    resource["id"] = medication_id
    for extension in resource["extension"]:
        if "PlanID-extension" in str(extension.get("url")):
            extension["valueString"] = alias
    return resource


def test_coverage_plan_is_deterministic_and_binds_all_search_contracts():
    first = _one_alias_plan("SYNTH-A")
    second = _one_alias_plan("SYNTH-B")
    second["id"] = "synthetic-coverage-b"
    forward = plan_coverage_census(
        _binding(),
        _coverage_census((first, second)),
        CUTOFF,
    )
    reverse = plan_coverage_census(
        _binding(),
        _coverage_census((second, first)),
        CUTOFF,
    )

    assert forward.acquisition_contract_hash == reverse.acquisition_contract_hash
    assert forward.plans == reverse.plans
    assert {plan.upstream_list_id for plan in forward.plans} == {
        "synthetic-coverage-a",
        "synthetic-coverage-b",
    }
    assert len(forward.work_items) == 2
    assert "SYNTH-A" not in repr(forward.work_items[0])


@pytest.mark.parametrize(
    "changed_census",
    [
        lambda census: replace(census, resource_type="MedicationKnowledge"),
        lambda census: replace(census, cutoff_at=CUTOFF + dt.timedelta(seconds=1)),
        lambda census: replace(census, exact_total=2),
        lambda census: replace(census, search_contract_hash="b" * 64),
    ],
)
def test_coverage_plan_rejects_inexact_census(changed_census):
    census = _coverage_census((_one_alias_plan(),))

    with pytest.raises(RuntimeError, match="census"):
        plan_coverage_census(_binding(), changed_census(census), CUTOFF)


def test_coverage_plan_rejects_empty_duplicate_and_cross_plan_aliases():
    with pytest.raises(RuntimeError, match="census"):
        plan_coverage_census(_binding(), _coverage_census(()), CUTOFF)

    duplicate = _one_alias_plan()
    with pytest.raises(RuntimeError, match="duplicates"):
        plan_coverage_census(
            _binding(),
            _coverage_census((duplicate, deepcopy(duplicate))),
            CUTOFF,
        )

    second_owner = _one_alias_plan()
    second_owner["id"] = "synthetic-coverage-b"
    with pytest.raises(RuntimeError, match="ownership"):
        plan_coverage_census(
            _binding(),
            _coverage_census((_one_alias_plan(), second_owner)),
            CUTOFF,
        )


def test_alias_plan_uses_full_membership_hash_for_reuse():
    coverage_plan = plan_coverage_census(
        _binding(),
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    work = coverage_plan.work_items[0]
    census = _medication_census("SYNTH-A", (_medication("SYNTH-A"),))
    full_plan = plan_alias_census(_binding(), work, census, CUTOFF, None)
    prior = PriorAliasState(
        source_id="source-alpha",
        public_id=work.plan.public_id,
        alias_id="ffa_" + "1" * 48,
        source_plan_identifier="SYNTH-A",
        alias_version_id="ffav_" + "2" * 48,
        expected_count=1,
        cutoff_at=CUTOFF - dt.timedelta(days=1),
        variants_by_medication_id={},
        membership_hash=full_plan.membership_hash,
    )

    reuse_plan = plan_alias_census(_binding(), work, census, CUTOFF, prior)
    changed_prior = replace(prior, membership_hash="b" * 64)
    changed_plan = plan_alias_census(
        _binding(), work, census, CUTOFF, changed_prior
    )

    assert full_plan.mode == "full"
    assert reuse_plan.mode == "reuse"
    assert changed_plan.mode == "full"


def test_correction_policy_is_bound_to_membership_reuse_proof():
    uncorrected_binding = _binding()
    corrected_binding = _binding(
        AlternativeCorrection(prefix="PRE-", rule_version="prefix-rule-v1")
    )
    coverage_plan = plan_coverage_census(
        corrected_binding,
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    work = coverage_plan.work_items[0]
    census = _medication_census("SYNTH-A", (_medication("SYNTH-A"),))
    uncorrected_plan = plan_alias_census(
        uncorrected_binding,
        work,
        census,
        CUTOFF,
        None,
    )
    prior = PriorAliasState(
        source_id="source-alpha",
        public_id=work.plan.public_id,
        alias_id="ffa_" + "1" * 48,
        source_plan_identifier="SYNTH-A",
        alias_version_id="ffav_" + "2" * 48,
        expected_count=1,
        cutoff_at=CUTOFF - dt.timedelta(days=1),
        variants_by_medication_id={},
        membership_hash=uncorrected_plan.membership_hash,
    )

    corrected_plan = plan_alias_census(
        corrected_binding,
        work,
        census,
        CUTOFF,
        prior,
    )

    assert corrected_plan.membership_hash != uncorrected_plan.membership_hash
    assert corrected_plan.mode == "full"


def test_alias_plan_rejects_crossed_duplicates_and_contract_mismatch():
    coverage_plan = plan_coverage_census(
        _binding(),
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    work = coverage_plan.work_items[0]
    crossed = _medication_census("SYNTH-A", (_medication("SYNTH-B"),))
    duplicate_medication = _medication("SYNTH-A")
    duplicates = _medication_census(
        "SYNTH-A",
        (duplicate_medication, deepcopy(duplicate_medication)),
    )
    wrong_hash = replace(
        _medication_census("SYNTH-A", ()),
        search_contract_hash="b" * 64,
    )

    with pytest.raises(RuntimeError, match="crossed"):
        plan_alias_census(_binding(), work, crossed, CUTOFF, None)
    with pytest.raises(RuntimeError, match="duplicates"):
        plan_alias_census(_binding(), work, duplicates, CUTOFF, None)
    with pytest.raises(RuntimeError, match="census"):
        plan_alias_census(_binding(), work, wrong_hash, CUTOFF, None)


def test_alias_plan_allows_exact_empty_census():
    coverage_plan = plan_coverage_census(
        _binding(),
        _coverage_census((_one_alias_plan(),)),
        CUTOFF,
    )
    alias_plan = plan_alias_census(
        _binding(),
        coverage_plan.work_items[0],
        _medication_census("SYNTH-A", ()),
        CUTOFF,
        None,
    )

    assert alias_plan.expected_count == 0
    assert alias_plan.mode == "full"
    assert len(alias_plan.membership_hash) == 64
