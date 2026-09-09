# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic licensing rows exercise the Illinois profile identity boundary."""

import copy

import pytest

from process.illinois_profile_roster import PHYSICIAN_DESCRIPTION, match_profile, parse_roster


def evidence():
    return {"run_id": "synthetic-run", "artifact_id": "synthetic-roster",
            "source_url": "https://data.illinois.gov/resource/pzzh-kp68.json?$limit=25",
            "downloaded_at": "2026-09-08T00:00:00+00:00", "content_sha256": "a" * 64}


def physician(**changes):
    return {"license_type": "MEDICAL BOARD", "description": PHYSICIAN_DESCRIPTION,
            "license_number": "036000007", "license_status": "ACTIVE", "business": "N",
            "first_name": "ALEX", "middle": "QUINN", "last_name": "EXAMPLE", "title": "MD",
            "prefix": "", "suffix": "", "original_issue_date": "04/28/2014",
            "expiration_date": "07/31/2029", "effective_date": "06/05/2026",
            "lastmodifieddate": "06/05/2026", **changes}


def profile(**changes):
    return {"display_name": "ALEX QUINN EXAMPLE MD", "original_issue_date": "04/28/2014",
            "license_status": "ACTIVE", "expiration_date": "07/31/2023", **changes}


def bridge(rows, identity=None):
    return match_profile(profile() if identity is None else identity, parse_roster(rows, evidence=evidence()))


def test_physician_description_separates_same_name_date_licenses():
    rows = [physician(), physician(description="LICENSED PHYSICIAN CONTROLLED SUBSTANCE", license_number="33*****07"),
            physician(description="TEMPORARY MEDICAL PERMIT", license_number="125000007")]
    roster = parse_roster(rows, evidence=evidence())
    result = match_profile(profile(), roster)
    assert result["status"] == "matched"
    assert result["license_number"] == "036000007"
    assert [row["status"] for row in roster["records"]] == ["eligible", "excluded", "excluded"]
    assert [row["originals"][0]["raw_payload"] for row in roster["records"]] == rows
    assert result["candidate_records"][0]["identity"]["original_issue_date"] == "2014-04-28"
    assert result["profile_identity"]["expiration_date"] == "07/31/2023"
    assert result["candidate_records"][0]["originals"][0]["raw_payload"]["expiration_date"] == "07/31/2029"
    assert "npi" not in result and "graduation_year" not in result and "freshness" not in result


def test_exact_duplicate_rows_keep_all_original_occurrences_and_detach_inputs():
    first = physician()
    second_by_field = dict(reversed(list(first.items())))
    rows, provenance = [first, second_by_field], evidence()
    roster = parse_roster(rows, evidence=provenance)
    assert len(roster["records"]) == 1
    record = roster["records"][0]
    assert record["originals"] == [{"row_number": 1, "raw_payload": first}, {"row_number": 2, "raw_payload": second_by_field}]
    assert match_profile(profile(), roster)["status"] == "matched"
    first["first_name"] = "CHANGED"
    provenance["run_id"] = "changed"
    assert record["originals"][0]["raw_payload"]["first_name"] == "ALEX"
    assert roster["evidence"]["run_id"] == "synthetic-run"
    result = match_profile(profile(), roster)
    result["candidate_records"][0]["originals"].clear()
    assert len(record["originals"]) == 2


@pytest.mark.parametrize("changes", [
    {"license_number": "036000008"},
    {"lastmodifieddate": "06/06/2026"},
    {"first_name": "ALEX QUINN", "middle": ""},
])
def test_nonidentical_matching_rows_are_ambiguous_even_with_the_same_license(changes):
    result = bridge([physician(), physician(**changes)])
    assert result["status"] == "ambiguous"
    assert result["reason"] == "multiple_matching_source_rows"
    assert result["license_number"] is None
    assert len(result["candidate_records"]) == 2


@pytest.mark.parametrize("changes", [
    {"original_issue_date": "04/29/2014"},
    {"first_name": "OTHER"},
    {"description": "TEMPORARY MEDICAL PERMIT"},
])
def test_conflicting_rows_for_the_selected_license_block_the_bridge(changes):
    result = bridge([physician(), physician(**changes)])
    assert result["status"] == "identity_conflict"
    assert result["reason"] == "conflicting_license_rows"
    assert result["license_number"] is None
    assert len(result["candidate_records"]) == 2


def test_a_different_issue_date_disambiguates_a_separate_complete_license():
    result = bridge([physician(), physician(license_number="036000008", original_issue_date="04/29/2014")])
    assert result["status"] == "matched"
    assert result["license_number"] == "036000007"
    assert len(result["candidate_records"]) == 2
    conflict = bridge([physician(original_issue_date="04/29/2014")])
    assert conflict["status"] == "identity_conflict"
    assert conflict["reason"] == "original_issue_date_conflict"


@pytest.mark.parametrize("identity", [
    {}, None, "not an identity", profile(display_name=""), profile(original_issue_date=""),
    profile(original_issue_date="2014-04-28"), profile(original_issue_date="02/30/2014"),
    profile(original_issue_date="4/28/2014"), profile(license_status=None),
])
def test_missing_or_invalid_profile_identity_never_yields_a_license(identity):
    result = match_profile(identity, parse_roster([physician()], evidence=evidence()))
    assert result["status"] == "identity_conflict"
    assert result["reason"] == "profile_identity_invalid"
    assert result["license_number"] is None
    assert result["profile_identity"] == identity


@pytest.mark.parametrize("changes", [
    {"middle": None}, {"first_name": ""}, {"title": None}, {"prefix": "DR"}, {"suffix": "JR"},
    {"license_number": "36.000007"}, {"license_number": 36000007}, {"license_number": "036000007 "},
    {"original_issue_date": ""}, {"original_issue_date": "02/30/2014"}, {"business": "Y"},
    {"description": ""}, {"license_type": ""}, {"unexpected": "field"},
])
def test_invalid_physician_rows_are_retained_and_block_a_potential_match(changes):
    raw = physician(**changes)
    roster = parse_roster([physician(), raw], evidence=evidence())
    assert roster["records"][1]["status"] == "invalid"
    assert roster["records"][1]["originals"][0]["raw_payload"] == raw
    result = match_profile(profile(), roster)
    assert result["status"] == "identity_conflict"
    assert result["license_number"] is None


@pytest.mark.parametrize("raw", [None, [], "error", 7, {}])
def test_unidentified_malformed_rows_are_retained_and_block_unverified_candidate_completeness(raw):
    roster = parse_roster([physician(), raw], evidence=evidence())
    assert roster["records"][1]["originals"][0]["raw_payload"] == raw
    assert match_profile(profile(), roster)["reason"] == "relevant_roster_identity_invalid"


def test_unrelated_invalid_row_is_retained_without_blocking():
    raw = physician(first_name="OTHER", last_name="PERSON", original_issue_date="", license_number="036000008")
    roster = parse_roster([raw, physician()], evidence=evidence())
    assert match_profile(profile(), roster)["license_number"] == "036000007"
    assert roster["records"][0]["status"] == "invalid"
    assert roster["records"][0]["originals"][0]["raw_payload"] == raw


@pytest.mark.parametrize("changes", [
    {"first_name": "", "last_name": ""}, {"first_name": ""}, {"business": "Y", "first_name": "", "last_name": ""},
])
def test_known_other_issue_date_does_not_make_missing_names_globally_relevant(changes):
    raw = physician(license_number="036000008", original_issue_date="04/29/2014", **changes)
    roster = parse_roster([physician(), raw], evidence=evidence())
    result = match_profile(profile(), roster)
    assert result["status"] == "matched" and result["license_number"] == "036000007"
    assert roster["records"][1]["status"] == "invalid"
    assert roster["records"][1]["originals"][0]["raw_payload"] == raw
    for conflict in ({"original_issue_date": "04/28/2014"}, {"original_issue_date": ""},
                     {"license_number": "036000007"}):
        assert bridge([physician(), {**raw, **conflict}])["status"] == "identity_conflict"


def test_malformed_rows_preserve_name_split_conflicts():
    raw = physician(first_name="ALEX QUINN", middle="", title=None, license_number="036000008")
    assert bridge([physician(), raw])["reason"] == "relevant_roster_identity_invalid"
    raw["title"] = "MD"
    raw["original_issue_date"] = ""
    assert bridge([physician(), raw])["reason"] == "relevant_roster_identity_invalid"


@pytest.mark.parametrize("display_name", ["ALEX Q EXAMPLE MD", "ALEX QUINN EXAMPLE M.D.", "ALEX QUINN EXAMPLE", "ALEX QUINN EXÁMPLE MD"])
def test_initials_punctuation_credentials_and_accents_are_not_fuzzy_equivalents(display_name):
    assert bridge([physician()], profile(display_name=display_name))["status"] == "unmatched"


def test_case_whitespace_and_blank_components():
    raw = physician(first_name=" Alex ", middle="", last_name="Example", title="")
    assert bridge([raw], profile(display_name=" alex  example "))["license_number"] == "036000007"
    assert bridge([], profile())["status"] == "unmatched"
    assert bridge([physician(license_type="OTHER BOARD")])["status"] == "unmatched"


@pytest.mark.parametrize("credential", ["MD", "DO"])
def test_observed_credential_suffix_preserves_original_identity_and_conflicts(credential):
    raw = physician(title="", suffix=credential)
    identity = profile(display_name=f"ALEX QUINN EXAMPLE {credential}")
    roster = parse_roster([raw], evidence=evidence())
    result = match_profile(identity, roster)
    assert result["status"] == "matched" and result["license_number"] == raw["license_number"]
    assert roster["records"][0]["originals"] == [{"row_number": 1, "raw_payload": raw}]
    assert roster["records"][0]["identity"]["title"] == ""
    assert roster["records"][0]["identity"]["suffix"] == credential
    assert result["profile_identity"] == identity and "npi" not in result
    assert bridge([raw], profile(display_name="ALEX QUINN EXAMPLE"))["status"] == "unmatched"
    assert bridge([raw, physician(title=credential)], identity)["status"] == "ambiguous"
    split_by_field = {**raw, "first_name": "ALEX QUINN", "middle": "", "license_number": "invalid"}
    conflict = bridge([raw, split_by_field], identity)
    assert conflict["reason"] == "relevant_roster_identity_invalid" and conflict["license_number"] is None
    assert len(conflict["candidate_records"]) == 2
    for changes in ({"suffix": "D.O."}, {"suffix": "M.D."}, {"suffix": "JR"}, {"title": credential}, {"prefix": "DR"}):
        unsupported_by_field = {**raw, **changes}
        retained = parse_roster([raw, unsupported_by_field], evidence=evidence())
        assert retained["records"][1]["status"] == "invalid"
        assert retained["records"][1]["originals"][0]["raw_payload"] == unsupported_by_field
        assert match_profile(identity, retained)["status"] == "identity_conflict"


@pytest.mark.parametrize(("changes", "reason"), [
    ({"source_url": "https://other.example/resource/pzzh-kp68.json"}, "illinois_roster_source_url_invalid"),
    ({"source_url": "https://data.illinois.gov/api/views/pzzh-kp68.json"}, "illinois_roster_source_url_invalid"),
    ({"source_url": "https://data.illinois.gov/resource/pzzh-kp68.json#wrong"}, "illinois_roster_source_url_invalid"),
    ({"source_url": "http://data.illinois.gov/resource/pzzh-kp68.json"}, "illinois_roster_source_url_invalid"),
    ({"downloaded_at": "2026-09-08"}, "illinois_roster_observation_timezone_missing"),
    ({"downloaded_at": "invalid"}, "illinois_roster_observation_timestamp_invalid"),
    ({"content_sha256": "bad"}, "illinois_roster_content_hash_invalid"),
    ({"run_id": ""}, "illinois_roster_evidence_missing"),
])
def test_provenance_must_bind_an_official_roster_response_and_aware_observation(changes, reason):
    with pytest.raises(ValueError, match=reason):
        parse_roster([physician()], evidence={**evidence(), **changes})


def test_invalid_programmatic_envelopes_fail_without_mutating_inputs():
    rows = [physician()]
    original = copy.deepcopy(rows)
    for invalid in (None, [], {"schema_version": "wrong"}):
        with pytest.raises(ValueError, match="illinois_roster_normalized_input_required"):
            match_profile(profile(), invalid)
    with pytest.raises(ValueError, match="illinois_roster_row_sequence_required"):
        parse_roster({}, evidence=evidence())
    with pytest.raises(ValueError, match="illinois_roster_evidence_missing"):
        parse_roster(rows, evidence=None)
    with pytest.raises(ValueError, match="illinois_roster_json_row_required"):
        parse_roster([{"bad": object()}], evidence=evidence())
    assert rows == original
