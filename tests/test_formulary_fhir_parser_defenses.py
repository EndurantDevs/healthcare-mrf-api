# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

import process.formulary_fhir.parser as parser_module
from process.formulary_fhir.types import (
    ALTERNATIVES_EXTENSION_URI,
    DRUG_TIER_EXTENSION_URI,
    AlternativeCorrection,
)


def test_extension_and_identifier_evidence_requires_exact_container_shapes():
    for resource in ({"extension": {}}, {"extension": [None]}):
        with pytest.raises(ValueError, match="extension"):
            parser_module._extension_rows(resource)
    with pytest.raises(ValueError, match="approved FHIR extension fields"):
        parser_module._extension_values(
            ({"url": "approved", "valueString": "x", "extra": True},),
            "approved",
            "valueString",
        )
    for resource in ({"identifier": {}}, {"identifier": [None]}):
        with pytest.raises(ValueError, match="identifier"):
            parser_module._raw_identifiers(resource)


def test_coding_shapes_and_user_selected_primitive_are_strict():
    for resource in (
        {"code": None},
        {"code": {"coding": [], "extra": True}},
        {"code": {"coding": []}},
    ):
        with pytest.raises(ValueError, match="code|codings"):
            parser_module._coding_rows(resource)
    for coding in (
        None,
        {"system": "s", "code": "c", "extra": True},
        {"system": "s", "code": "c", "userSelected": 1},
    ):
        with pytest.raises(ValueError, match="coding"):
            parser_module._coding_from_object(coding)

    parsed = parser_module._coding_rows(
        {
            "code": {
                "text": "Synthetic medication",
                "coding": [{"system": "synthetic", "code": "code"}],
            }
        }
    )
    assert parsed[0].code == "code"


def _tier_extension(value: object) -> dict[str, object]:
    return {
        "url": DRUG_TIER_EXTENSION_URI,
        "valueCodeableConcept": value,
    }


def test_tier_extension_rejects_ambiguous_and_malformed_evidence():
    assert parser_module._tier_value(()) is None
    with pytest.raises(ValueError, match="extension is ambiguous"):
        parser_module._tier_value(
            (_tier_extension({"coding": [{"code": "one"}]}),) * 2
        )
    for concept in (None, {"coding": [], "extra": True}, {"coding": []}):
        with pytest.raises(ValueError, match="tier concept|tier coding"):
            parser_module._tier_value((_tier_extension(concept),))
    with pytest.raises(ValueError, match="coding is ambiguous"):
        parser_module._tier_value(
            (
                _tier_extension(
                    {"coding": [{"code": "one"}, {"code": "two"}]}
                ),
            )
        )
    with pytest.raises(ValueError, match="coding fields"):
        parser_module._tier_coding_name(None)
    with pytest.raises(ValueError, match="has no value"):
        parser_module._tier_coding_name({"system": "synthetic"})
    assert parser_module._tier_coding_name(
        {"system": "synthetic", "display": "Tier One"}
    ) == "Tier One"


def test_alternative_reference_parser_rejects_malformed_evidence():
    for value_reference in (None, {"reference": "bad"}, {"other": "bad"}):
        extension_by_field = {
            "url": ALTERNATIVES_EXTENSION_URI,
            "valueReference": value_reference,
        }
        with pytest.raises(ValueError, match="alternative reference"):
            parser_module._alternative_references((extension_by_field,))


def test_alternative_resolution_covers_direct_unresolved_and_policy_rejections():
    direct = parser_module.resolve_alternative_references(
        ["MedicationKnowledge/direct"],
        known_medication_ids={"direct"},
    )
    unresolved = parser_module.resolve_alternative_references(
        ["MedicationKnowledge/PRE-missing"],
        known_medication_ids={"other"},
        correction=AlternativeCorrection(prefix="PRE-", rule_version="v1"),
    )
    candidate_missing = parser_module.resolve_alternative_references(
        ["MedicationKnowledge/missing"],
        known_medication_ids={"other"},
        correction=AlternativeCorrection(prefix="PRE-", rule_version="v1"),
    )

    assert direct[0].resolved_medication_id == "direct"
    assert unresolved[0].is_resolved is False
    assert candidate_missing[0].is_resolved is False
    with pytest.raises(ValueError, match="collection"):
        parser_module.resolve_alternative_references(
            "MedicationKnowledge/direct",
            known_medication_ids={"direct"},
        )
    with pytest.raises(ValueError, match="collection"):
        parser_module.resolve_alternative_references(
            [],
            known_medication_ids=frozenset(),
        )
    with pytest.raises(ValueError, match="reference is invalid"):
        parser_module.resolve_alternative_references(
            ["invalid"],
            known_medication_ids=set(),
        )
    with pytest.raises(ValueError, match="correction policy"):
        parser_module.resolve_alternative_references(
            [],
            known_medication_ids=set(),
            correction=SimpleNamespace(prefix="bad prefix", rule_version="v1"),
        )


def test_validated_correction_accepts_none_and_rejects_invalid_exact_type():
    assert parser_module._validated_correction(None) is None
    with pytest.raises(ValueError, match="correction policy"):
        parser_module._validated_correction(
            SimpleNamespace(prefix="PRE-", rule_version="v1")
        )
