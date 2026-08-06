# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from process.formulary_fhir.identity import (
    canonical_list_identity,
    public_formulary_id,
)
from process.formulary_fhir.parser import (
    CA_ALTERNATIVE_RULE_VERSION,
    parse_coverage_plan,
    parse_medication_knowledge,
    resolve_alternative_references,
)


FIXTURES = Path(__file__).parent / "fixtures" / "formulary_fhir"
BASE = "https://fhir.example.invalid/r4"


def _fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_public_id_has_130_base32_bits_and_canonical_identity():
    public_id = public_formulary_id("https://FHIR.EXAMPLE.invalid:443/r4/", "abc")

    assert re.fullmatch(r"fhir_[a-z2-7]{26}", public_id)
    assert canonical_list_identity(BASE, "abc") == "https://fhir.example.invalid/r4/List/abc"
    assert public_id == public_formulary_id(BASE, "abc")


def test_public_id_rejects_ambiguous_or_non_https_identity():
    with pytest.raises(ValueError):
        public_formulary_id("http://fhir.example.invalid/r4", "abc")
    with pytest.raises(ValueError):
        public_formulary_id(BASE, "nested/abc")


def test_coverage_plan_preserves_identifiers_and_enumerates_every_alias():
    parsed = parse_coverage_plan(_fixture("coverage_plan.json"), canonical_base=BASE)

    assert parsed.source_plan_identifiers == ("SYNTH-NCAL-A", "SYNTH-NCAL-B")
    assert parsed.raw_identifiers[0]["value"] == "SYNTH-NCAL-A"
    assert parsed.upstream_date == "2026-08-01T10:00:00Z"
    assert parsed.raw_extensions[-1]["valueUri"].endswith("synthetic-plan")
    assert parsed.public_id.startswith("fhir_")


def test_medication_parser_preserves_all_codings_and_only_unambiguous_ndc11():
    parsed_a = parse_medication_knowledge(_fixture("medication_a.json"))
    parsed_b = parse_medication_knowledge(_fixture("medication_b.json"))

    assert parsed_a.rxnorm_id == "100001"
    assert parsed_a.ndc11 == "12345678901"
    assert len(parsed_a.codings) == 3
    assert parsed_a.drug_tier == "Tier 1"
    assert parsed_a.step_therapy is True
    assert parsed_b.ndc11 is None


def test_conflicting_rxnorm_codings_remain_evidence_and_scalar_is_null():
    resource = _fixture("medication_a.json")
    resource["code"]["coding"].append(
        {
            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
            "code": "200002",
        }
    )

    parsed = parse_medication_knowledge(resource)

    assert parsed.rxnorm_id is None
    assert len(parsed.codings) == 4


def test_invalid_rxnorm_code_is_preserved_but_not_normalized():
    resource = _fixture("medication_a.json")
    resource["code"]["coding"][0]["code"] = "not-a-concept-id"

    parsed = parse_medication_knowledge(resource)

    assert parsed.rxnorm_id is None
    assert parsed.codings[0].code == "not-a-concept-id"


def test_california_alternative_correction_only_resolves_known_prefixed_target():
    evidence = resolve_alternative_references(
        ["MedicationKnowledge/synthetic-drug-b", "MedicationKnowledge/missing"],
        known_medication_ids={"MI-synthetic-drug-b"},
        apply_california_rule=True,
    )

    assert evidence[1].corrected_reference == "MedicationKnowledge/MI-synthetic-drug-b"
    assert evidence[1].rule_version == CA_ALTERNATIVE_RULE_VERSION
    assert evidence[0].resolved is False


def test_non_california_alternative_never_applies_prefix_rule():
    evidence = resolve_alternative_references(
        ["MedicationKnowledge/synthetic-drug-b"],
        known_medication_ids={"MI-synthetic-drug-b"},
        apply_california_rule=False,
    )

    assert evidence[0].resolved is False
    assert evidence[0].corrected_reference is None
