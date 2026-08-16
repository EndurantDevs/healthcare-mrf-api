# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import io
import runpy
import sys
from dataclasses import replace

import pytest

from process.ext import address_canon, address_match
from process.ext.address_match import AddressRecord, match_address_candidates


NPI = "1234567893"


def record(
    first_line: str,
    second_line: str = "",
    *,
    npi: str = NPI,
    city: str = "Austin",
    state: str = "TX",
    zip_code: str = "78701",
    visible: bool = False,
    formatted_address: str = "",
) -> AddressRecord:
    key = address_canon.address_key_v1(first_line, second_line, city, state, zip_code, "US")
    premise = address_canon.key_from_identity(
        address_canon.premise_identity_key_v1(first_line, second_line, city, state, zip_code, "US")
    )
    return AddressRecord(
        npi=npi,
        first_line=first_line,
        second_line=second_line,
        city=city,
        state=state,
        zip_code=zip_code,
        country="US",
        address_key=str(key) if visible and key else None,
        premise_key=str(premise) if visible and premise else None,
        formatted_address=formatted_address,
        is_healthporta_visible=visible,
    )


def matched(source: AddressRecord, *targets: AddressRecord):
    result = match_address_candidates(source, targets)
    assert result is not None
    return result


def test_internal_match_guards_fail_closed():
    source = record("10 Main St")
    target = record("10 Main St", visible=True)

    assert address_match._street_relation("", "", target.first_line, target.second_line) is None
    assert address_match._is_route_number_removed(["10"], 0) is False
    assert address_match.compare_address_pair(
        source, replace(target, is_healthporta_visible=False)
    ) is None
    assert address_match._select_parser_match(source, [], {}, []) is None


def test_canonical_full_and_premise_only_matches_stay_distinct():
    exact_source = record("123 Main Street", "Suite 10")
    exact_target = record("123 MAIN ST", "STE 10", visible=True)
    exact = matched(exact_source, exact_target)

    premise_source = record("123 Main Street", "Suite 11")
    premise = matched(premise_source, exact_target)

    assert (exact.classification, exact.rule) == ("exact", "canonical_address_key")
    assert (premise.classification, premise.rule) == ("premise_only", "canonical_premise_key")


def test_direction_relocation_is_pairwise_only_and_preserves_unit_classification():
    source = record("902 7th Street North", "Suite 4")
    target = record("902 N 7TH ST", "Ste 4", visible=True)
    premise_target = record("902 N 7TH ST", "Ste 5", visible=True)

    assert address_canon.address_key_v1(
        source.first_line, source.second_line, source.city, source.state, source.zip_code, source.country
    ) != address_canon.address_key_v1(
        target.first_line, target.second_line, target.city, target.state, target.zip_code, target.country
    )
    assert (matched(source, target).classification, matched(source, target).rule) == (
        "exact",
        "direction_relocation",
    )
    assert matched(source, premise_target).classification == "premise_only"


def test_exact_direction_match_precedes_a_premise_only_candidate():
    source = record("902 7th Street North", "Suite 4")
    exact_target = record("902 N 7TH ST", "Suite 4", visible=True)
    premise_target = record("902 7th St N", "Suite 5", visible=True)

    result = matched(source, premise_target, exact_target)

    assert (result.classification, result.rule) == ("exact", "direction_relocation")
    assert result.target_address_key == exact_target.address_key


def test_one_sided_suffix_omission_is_pairwise_only_and_preserves_unit_classification():
    source = record("15101 Glenwood", "Suite 7")
    target = record("15101 Glenwood Ave", "Ste 7", visible=True)
    premise_target = record("15101 Glenwood Ave", "Ste 8", visible=True)

    assert address_canon.address_key_v1(
        source.first_line, source.second_line, source.city, source.state, source.zip_code, source.country
    ) != address_canon.address_key_v1(
        target.first_line, target.second_line, target.city, target.state, target.zip_code, target.country
    )
    assert (matched(source, target).classification, matched(source, target).rule) == (
        "exact",
        "terminal_suffix_omission",
    )
    assert matched(source, premise_target).classification == "premise_only"


@pytest.mark.parametrize(
    ("source_line", "target_line", "target_unit"),
    [
        ("4007 Clarksville Pike 301", "4007 Clarksville Pike", "Suite 301"),
        ("11061 Broadway b", "11061 Broadway", "Ste B"),
        ("7200 State Highway 161 230", "7200 State Highway 161", "Ste 230"),
    ],
)
def test_candidate_confirmed_bare_unit_matches_exactly(source_line, target_line, target_unit):
    source = record(source_line)
    target = record(target_line, target_unit, visible=True)
    result = matched(source, target)

    assert result.classification == "exact"
    assert result.rule == "candidate_confirmed_bare_unit"
    assert result.target_address_key == target.address_key


def test_loop_street_suffix_can_precede_an_unlabeled_unit():
    source = record("8200 Crafters Loop 1103")
    target = record("8200 Crafters Loop", "Apt 1103", visible=True)

    assert matched(source, target).rule == "candidate_confirmed_bare_unit"


@pytest.mark.parametrize(
    ("source_line", "target_line"),
    [
        ("919 South Winton Road 220", "919 Winton Rd S"),
        ("370 E South Temple Street 325", "370 E South Temple"),
    ],
)
def test_candidate_confirmed_bare_unit_can_use_one_safe_street_relation(source_line, target_line):
    source = record(source_line)
    target = record(target_line, "Suite 220" if source_line.endswith("220") else "Suite 325", visible=True)

    assert matched(source, target).rule == "candidate_confirmed_bare_unit"


def test_unlabeled_unit_does_not_choose_between_distinct_explicit_unit_types():
    source = record("39 Broadway 2115")
    suite = record("39 Broadway", "Suite 2115", visible=True)
    room = record("39 Broadway", "Room 2115", visible=True)

    assert match_address_candidates(source, [suite, room]) is None


def test_bare_unit_prefers_an_exact_base_over_a_suffix_relaxation():
    source = record("500 Rue de la Vie 510")
    exact_base = record("500 Rue de la Vie", "Suite 510", visible=True)
    suffix_relaxed = record("500 Rue de la Vie St", "Suite 510", visible=True)

    assert matched(source, suffix_relaxed, exact_base).target_address_key == exact_base.address_key


def test_bare_unit_requires_candidate_confirmation_and_exact_unit():
    source = record("4007 Clarksville Pike 301")
    wrong_unit = record("4007 Clarksville Pike", "Suite 302", visible=True)

    assert match_address_candidates(source, []) is None
    assert match_address_candidates(source, [wrong_unit]) is None


def test_bare_unit_does_not_discard_a_source_second_line():
    source = record("10 Main 301", "North Campus")
    target = record("10 Main", "Suite 301", visible=True)

    assert match_address_candidates(source, [target]) is None


def test_numbered_road_number_is_not_reinterpreted_as_a_unit():
    source = record("123 US Highway 64")
    false_target = record("123 US Highway", "Suite 64", visible=True)

    assert match_address_candidates(source, [false_target]) is None


@pytest.mark.parametrize(
    ("source_line", "target_line", "target_unit"),
    [
        ("7200 State Highway 161 230", "7200 State Highway", "Suite 161230"),
        ("123 Highway64", "123 Highway", "Suite 64"),
        ("123 US-64", "123 US", "Suite 64"),
        ("123 County Road 64", "123 County Road", "Suite 64"),
        ("123 Old Highway 64", "123 Old Highway", "Suite 64"),
        ("123 Interstate 64", "123 Interstate", "Suite 64"),
        ("123 Business Loop 64", "123 Business Loop", "Suite 64"),
        ("123 SR 64", "123 SR", "Suite 64"),
        ("123 I-64", "123 I", "Suite 64"),
        ("123 Hiway 64", "123 Hiway", "Suite 64"),
        ("123 U.S. 64", "123 U.S.", "Suite 64"),
        ("123 County Rd 64", "123 County Rd", "Suite 64"),
        ("123 State Rd 64", "123 State Rd", "Suite 64"),
        ("123 CR 64", "123 CR", "Suite 64"),
        ("123 SH 64", "123 SH", "Suite 64"),
        ("123 Highway No 64", "123 Highway", "Suite 64"),
        ("123 Route No 64", "123 Route", "Suite 64"),
        ("123 State Loop 12 4", "123 State Loop", "Suite 124"),
    ],
)
def test_route_numbers_are_not_combined_or_removed_as_bare_units(source_line, target_line, target_unit):
    source = record(source_line)
    target = record(target_line, target_unit, visible=True)

    assert match_address_candidates(source, [target]) is None


def test_unit_punctuation_is_local_and_invalid_values_remain_unmatched():
    source = record("3009 North Ballas Road Suite: 141A")
    target = record("3009 N Ballas Rd", "Ste 141A", visible=True)
    invalid_source = record("3009 North Ballas Road Suite: Road")

    assert matched(source, target).rule == "unit_designator_punctuation"
    assert match_address_candidates(invalid_source, [target]) is None


def test_unit_punctuation_rejects_a_different_canonical_target():
    source = record("3009 North Ballas Road Suite: 141A")
    target = record("3010 N Ballas Rd", "Ste 141A", visible=True)

    assert address_match._punctuation_result(source, target) is None


def test_candidate_confirmed_spaced_unit_only_second_line():
    source = record("7108 De Soto Avenue unit 105c")
    target = record("7108 DE SOTO AVE", "105 C", visible=True)

    result = matched(source, target)
    assert (result.classification, result.rule) == ("exact", "candidate_confirmed_spaced_unit")


def test_candidate_confirmed_spaced_unit_allows_safe_suffix_omission():
    source = record("7108 De Soto Avenue unit 105c")
    target = record("7108 DE SOTO", "105 C", visible=True)

    assert matched(source, target).rule == "candidate_confirmed_spaced_unit"


def test_serving_formatted_address_can_prove_descriptor_is_non_address_metadata():
    source = record("2241 Geary Blvd")
    target = record(
        "2241 GEARY BLVD",
        "HEALTH EDUCATION DEPT",
        formatted_address="2241 Geary Boulevard, Austin, TX 78701",
        visible=True,
    )
    unproven = replace(target, formatted_address="")

    assert matched(source, target).rule == "formatted_address_omits_descriptor"
    assert match_address_candidates(source, [unproven]) is None


@pytest.mark.parametrize(
    ("source_line", "formatted_address"),
    [
        ("2241 Geary Blvd", "999 Other Street, Austin, TX 78701"),
        (
            "2241 Geary Blvd",
            "2241 Geary Boulevard, Health Education Dept, Austin, TX 78701",
        ),
        ("2242 Geary Blvd", "2241 Geary Boulevard, Austin, TX 78701"),
    ],
)
def test_descriptor_evidence_rejects_unproved_target(source_line, formatted_address):
    source = record(source_line)
    target = record(
        "2241 GEARY BLVD",
        "HEALTH EDUCATION DEPT",
        formatted_address=formatted_address,
        visible=True,
    )

    assert address_match._descriptor_result(source, target) is None


@pytest.mark.parametrize(
    ("source_line", "source_unit", "target_line", "target_unit"),
    [
        ("504 S New Florissant Rd", "", "504 North New Florissant Road", ""),
        ("10 N Main St", "", "10 S Main St", ""),
        ("10 Main St", "", "10 Main Rd", ""),
        ("6 Old Fremont Road", "", "6 Old Fremont Road Ext", ""),
        ("24 White Bridge Road", "", "24 White Bridge Pike", ""),
        ("213 West 4th Street North", "", "213 W 4th North St", ""),
        ("4550 Cobb Parkway North", "Suite 101", "4550 Cobb Pkwy N NW", "Suite 201A"),
    ],
)
def test_conflicting_street_evidence_stays_unmatched(source_line, source_unit, target_line, target_unit):
    source = record(source_line, source_unit)
    target = record(target_line, target_unit, visible=True)

    assert match_address_candidates(source, [target]) is None


def test_mandatory_npi_zip_state_country_and_visibility_gates():
    source = record("10 Main St")
    target = record("10 Main St", visible=True)

    assert match_address_candidates(source, [replace(target, npi="9876543210")]) is None
    assert match_address_candidates(source, [replace(target, zip_code="78702")]) is None
    assert match_address_candidates(source, [replace(target, state="CA")]) is None
    assert match_address_candidates(source, [replace(target, country="CA")]) is None
    assert match_address_candidates(
        source,
        [replace(target, address_key="00000000-0000-0000-0000-000000000001")],
    ) is None
    assert match_address_candidates(
        source,
        [replace(target, premise_key="00000000-0000-0000-0000-000000000001")],
    ) is None
    assert match_address_candidates(source, [replace(target, is_healthporta_visible=False)]) is None
    assert match_address_candidates(replace(source, npi="123"), [target]) is None
    assert match_address_candidates(replace(source, npi="9999999995"), [replace(target, npi="9999999995")]) is None


def test_city_text_difference_is_reported_but_not_a_gate():
    source = record("10 Main St", city="West Lake Hills")
    target = record("10 Main St", city="Austin", visible=True)

    assert matched(source, target).city_differs is True


def test_conflicting_direction_candidates_veto_relaxed_match():
    source = record("10 Main St N")
    north = record("10 N Main St", visible=True)
    south = record("10 S Main St", visible=True)

    assert match_address_candidates(source, [north, south]) is None


def test_conflicting_suffix_candidates_veto_relaxed_match():
    source = record("10 Main")
    street = record("10 Main St", visible=True)
    road = record("10 Main Rd", visible=True)

    assert match_address_candidates(source, [street, road]) is None


def test_direction_and_suffix_relaxations_cannot_select_different_candidates():
    source = record("10 N Main")
    relocated = record("10 Main N", visible=True)
    suffixed = record("10 N Main Rd", visible=True)

    assert match_address_candidates(source, [relocated, suffixed]) is None


@pytest.mark.parametrize(
    ("source_line", "first_target", "second_target"),
    [
        ("10 N Main St", "10 Main St N", "10 N Main Rd"),
        ("10 N Main", "10 N Main St", "10 S Main"),
    ],
)
def test_every_relaxed_match_checks_direction_and_suffix_conflicts(source_line, first_target, second_target):
    source = record(source_line)
    first = record(first_target, visible=True)
    second = record(second_target, visible=True)

    assert match_address_candidates(source, [first, second]) is None


def test_relaxed_match_vetoes_a_candidate_conflicting_in_both_dimensions():
    source = record("10 N Main St")
    relocated = record("10 Main St N", visible=True)
    conflicting = record("10 S Main Rd", visible=True)

    assert match_address_candidates(source, [relocated, conflicting]) is None


def test_spaced_unit_lines_cannot_hide_street_conflicts():
    source = record("10 Main St N", "Unit 301")
    matching = record("10 N Main St", "301", visible=True)
    conflicting = record("10 S Main Rd", "302", visible=True)

    assert match_address_candidates(source, [matching, conflicting]) is None


def test_exact_matches_across_different_relaxed_rules_require_one_target_key():
    source = record("4800 N Galloway Ave", "Ste #300")
    explicit = record("4800 N Galloway", "Suite 300", visible=True)
    spaced = record("4800 North Galloway Avenue", "300", visible=True)

    assert match_address_candidates(source, [explicit, spaced]) is None


def test_bare_unit_relaxation_obeys_explicit_suffix_conflicts():
    source = record("10 Main 301")
    matching_unit = record("10 Main St", "Suite 301", visible=True)
    conflicting_street = record("10 Main Rd", "Suite 302", visible=True)

    assert match_address_candidates(source, [matching_unit, conflicting_street]) is None


def test_exact_key_precedes_conflicting_relaxed_candidates():
    source = record("10 N Main St")
    exact = record("10 North Main Street", visible=True)
    conflict = record("10 S Main St", visible=True)

    assert matched(source, conflict, exact).target_address_key == exact.address_key


def test_duplicate_candidate_rows_are_deduplicated_by_stored_address_key():
    source = record("15101 Glenwood")
    target = record("15101 Glenwood Ave", visible=True)
    duplicate = replace(target, formatted_address="15101 Glenwood Avenue, Austin, TX 78701")

    assert matched(source, duplicate, target).target_address_key == target.address_key


def test_json_cli_emits_a_no_match_result(monkeypatch):
    input_stream = io.StringIO(
        '[{"source":{"npi":"1234567893","first_line":"10 Main St",'
        '"second_line":"","city":"Austin","state":"TX","zip_code":"78701"},'
        '"candidates":[]}]'
    )
    output_stream = io.StringIO()
    monkeypatch.setattr(sys, "stdin", input_stream)
    monkeypatch.setattr(sys, "stdout", output_stream)

    runpy.run_path(address_match.__file__, run_name="__main__")

    assert output_stream.getvalue() == "[null]"
