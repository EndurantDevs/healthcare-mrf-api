# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json

import pytest

from scripts.research import hospital_hpt_experiment as experiment


def test_three_source_formats_have_one_semantic_digest(tmp_path):
    result = experiment.generate(
        tmp_path, hospitals=2, facts_per_hospital=7, payers=3
    )

    assert result["status"] == "passed"
    assert {
        (value["hospitals"], value["facts"], value["semantic_sha256"])
        for value in result["formats"].values()
    } == {(2, 14, next(iter(result["formats"].values()))["semantic_sha256"])}
    assert '"tier 0"' in next((tmp_path / "tall_csv").iterdir()).read_text()


def test_corpus_exercises_v3_charge_shapes():
    _, facts = experiment.build_corpus(
        hospitals=1, facts_per_hospital=36, payers=3
    )

    assert {fact.code_system for fact in facts} >= {"CPT", "MS-DRG", "NDC"}
    assert any(fact.negotiated_dollar is not None for fact in facts)
    assert any(fact.negotiated_percentage is not None for fact in facts)
    assert any(fact.negotiated_algorithm is not None for fact in facts)
    assert any(fact.allowed_count == "1 through 10" for fact in facts)
    assert any(fact.drug_unit and fact.drug_type for fact in facts)
    assert any(fact.modifiers for fact in facts)
    assert any(fact.additional_payer_notes for fact in facts)
    assert {fact.billing_class for fact in facts} == {
        "professional", "facility", "both"
    }
    assert all(fact.gross_amount and fact.discounted_cash for fact in facts)

    hospitals, _ = experiment.build_corpus(
        hospitals=1, facts_per_hospital=1, payers=1
    )
    assert hospitals[0].financial_aid_policy
    assert hospitals[0].contract_provisions == (
        (None, None, "Synthetic contract provision 1"),
    )


def test_verify_fails_when_one_format_loses_a_fact(tmp_path):
    experiment.generate(tmp_path, hospitals=1, facts_per_hospital=3, payers=2)
    path = next((tmp_path / "json").iterdir())
    payload = json.loads(path.read_text())
    payload["standard_charge_information"][0]["standard_charges"][0][
        "payers_information"
    ].pop()
    path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="json does not match"):
        experiment.verify(tmp_path / "manifest.json")


@pytest.mark.parametrize(
    "arguments", [{"hospitals": 0}, {"facts_per_hospital": 0}, {"payers": 0}]
)
def test_corpus_dimensions_must_be_positive(arguments):
    dimensions_by_name = {
        "hospitals": 1,
        "facts_per_hospital": 1,
        "payers": 1,
        **arguments,
    }
    with pytest.raises(ValueError, match="must be positive"):
        experiment.build_corpus(**dimensions_by_name)
