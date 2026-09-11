# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import json

import pytest

from process import rhode_island_profile_binding as binding
from tests.test_rhode_island_profile_rows import evidence, occurrence


def candidate(**changes):
    return {
        "npi": 1003000126,
        "joined_npi": 1003000126,
        "taxonomy_occurrence_checksum": 1,
        "license_number": "MD00001",
        "license_state": "RI",
        "taxonomy": "207Q00000X",
        "primary_taxonomy_switch": "Y",
        "entity_type_code": 1,
        "first_name": "Alex",
        "middle_name": "",
        "last_name": "Example",
        "suffix": "",
        "joined_taxonomy_code": "207Q00000X",
        "taxonomy_grouping": "Allopathic & Osteopathic Physicians",
        **changes,
    }


def bind(candidates, license_number="MD00001", **changes):
    payload = json.dumps(occurrence(license_number, **changes) * 2).encode()
    return binding.bind_profile(
        payload, license_number=license_number, evidence=evidence(payload, license_number), candidates=candidates
    )


@pytest.mark.parametrize("license_number", ["MD00001", "DO00001"])
def test_exact_prefixed_license_name_and_repeated_taxonomy_bind_once(license_number):
    supplied_candidates = [
        candidate(license_number=license_number),
        candidate(
            license_number=license_number,
            taxonomy_occurrence_checksum=2,
            primary_taxonomy_switch="N",
            first_name="  ALEX  ",
        ),
    ]
    original = copy.deepcopy(supplied_candidates)
    record, facts = bind(supplied_candidates, license_number)
    decision = record["match_evidence"]["registry_binding"]
    assert record["match_status"] == "deterministic" and record["matched_npi"] == 1003000126
    assert decision["exact_candidate_indexes"] == [0, 1] and decision["candidate_rows"] == original
    assert not decision["registry_completeness_verified"]
    assert record["match_evidence"]["npi_binding"] == "supplied_registry_occurrences"
    assert all(fact["npi"] == 1003000126 and fact["published_at"] is None for fact in facts)
    assert all(fact["source_json"]["occurrence_indexes"] == [0, 1] for fact in facts)
    decision["candidate_rows"][0]["first_name"] = "changed"
    assert supplied_candidates == original


@pytest.mark.parametrize(
    "license_number", ["00001", "1", "MD1", "md00001", " MD00001", "MD00001 ", "DO00001", "MD００００１", None]
)
def test_no_prefix_inference_padding_or_license_normalization(license_number):
    record, facts = bind([candidate(license_number=license_number)])
    assert record["matched_npi"] is None and record["match_status"] == "unmatched"
    assert all(fact["npi"] is None for fact in facts)
    assert record["match_evidence"]["registry_binding"]["candidate_rows"][0]["license_number"] == license_number


def test_opposite_board_and_bare_numbers_are_excluded_with_evidence():
    record, _ = bind(
        [
            candidate(license_number="DO00001", npi=1003000134, joined_npi=1003000134),
            candidate(license_number="00001"),
            candidate(),
            candidate(license_state="MA"),
        ]
    )
    assert record["matched_npi"] == 1003000126
    assert record["match_evidence"]["registry_binding"]["exact_candidate_indexes"] == [2]
    assert len(record["match_evidence"]["registry_binding"]["candidate_rows"]) == 4


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": 123},
        {"npi": "1003000126"},
        {"npi": True},
        {"joined_npi": None},
        {"joined_npi": 1003000134},
        {"entity_type_code": 2},
        {"entity_type_code": True},
        {"taxonomy_occurrence_checksum": None},
        {"taxonomy": ""},
        {"joined_taxonomy_code": None},
        {"taxonomy_grouping": "Other Providers"},
        {"first_name": "A"},
        {"last_name": "Different"},
        {"middle_name": "B"},
        {"first_name": None},
        {"suffix": "Jr"},
        {"unrecognized": "field"},
    ],
)
def test_invalid_or_conflicting_exact_occurrence_blocks_other_matching_row(changes):
    record, facts = bind([candidate(), candidate(**changes)])
    assert record["matched_npi"] is None and record["match_status"] == "identity_conflict"
    assert all(fact["npi"] is None for fact in facts)
    assert len(record["match_evidence"]["registry_binding"]["candidate_rows"]) == 2


def test_multiple_valid_npis_are_ambiguous():
    record, _ = bind([candidate(), candidate(npi=1003000134, joined_npi=1003000134)])
    assert record["match_status"] == "ambiguous" and record["matched_npi"] is None


@pytest.mark.parametrize("changes", [{"First_Name": ""}, {"First_Name": "A"}, {"Last_Name": "E"}])
def test_source_initials_cannot_establish_identity(changes):
    record, _ = bind([candidate()], **changes)
    assert record["match_status"] == "identity_conflict"


def test_middle_name_is_exact_and_null_empty_is_compatible():
    record, _ = bind([candidate(middle_name=None)])
    assert record["match_status"] == "deterministic"
    record, _ = bind([candidate(middle_name=" B ")], Middle_Name="b")
    assert record["match_status"] == "deterministic"
    record, _ = bind([candidate(middle_name="B")], Middle_Name="Blair")
    assert record["match_status"] == "identity_conflict"


@pytest.mark.parametrize(
    "supplied_candidates",
    [
        None,
        {},
        [None],
        [{}],
        [{"license_number": 1, "license_state": "RI"}],
        [{"license_number": "MD00001", "license_state": None}],
    ],
)
def test_invalid_candidate_envelope_is_rejected(supplied_candidates):
    with pytest.raises(ValueError, match="rhode_island_binding_invalid_candidates"):
        bind(supplied_candidates)


def test_no_candidates_retains_profile_without_npi():
    record, facts = bind([])
    assert record["match_status"] == "unmatched" and facts
    assert record["raw_payload"]["values"] == occurrence() * 2
