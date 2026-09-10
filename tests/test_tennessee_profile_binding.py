# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import hashlib

import pytest

from process import tennessee_profile_binding as binding
from process.massachusetts_profile_acquisition import encoded_json
from process.tennessee_profile_rows import parse_report
from tests.test_tennessee_profile_rows import EVIDENCE, PRACTICE_HEADERS, malformed_row, report_bytes, source_row


def _do_row(**changes):
    return source_row(**{"Board": "Osteopathy", "Profession": "Osteopathic Physician", "Rank": "Osteopathic Physician",
                         "FirstName": "Casey", "MiddleName": "", "LastName": "Sample", "LicenseNumber": "456",
                         "DegreeEarned": "DO", **changes})


def _reports(md_rows=None, do_rows=None):
    reports = {}
    for profession, source_rows in (("1606", [source_row(LicenseNumber="123")] if md_rows is None else md_rows),
                                   ("1907", [_do_row()] if do_rows is None else do_rows)):
        content = report_bytes(source_rows)
        reports[profession] = {"content": content, "evidence": {
            **EVIDENCE, "artifact_id": "synthetic-artifact-" + profession,
            "content_sha256": hashlib.sha256(content).hexdigest()}}
    return reports


def _candidate(**changes):
    return {"npi": 1000000004, "taxonomy_occurrence_checksum": 17, "license_number": "123",
            "license_state": "TN", "taxonomy": "207R00000X", "primary_taxonomy_switch": "Y",
            "joined_npi": 1000000004, "entity_type_code": 1, "first_name": "Alex", "middle_name": "Morgan",
            "last_name": "Example", "suffix": None, "joined_taxonomy_code": "207R00000X",
            "taxonomy_grouping": "Allopathic & Osteopathic Physicians", **changes}


def _do_candidate(**changes):
    return _candidate(**{"npi": 1000000012, "joined_npi": 1000000012, "license_number": "456",
                         "first_name": "Casey", "middle_name": None, "last_name": "Sample", **changes})


def _snapshot(registry_rows=None):
    registry_rows = [_candidate(), _do_candidate()] if registry_rows is None else registry_rows
    registry_rows = sorted(registry_rows, key=lambda row: tuple(
        row.get(field) if type(row.get(field)) is int else 0 for field in ("npi", "taxonomy_occurrence_checksum")))
    registry_bytes = encoded_json(registry_rows)
    return {"schema_version": binding.SNAPSHOT_SCHEMA, "coverage_scope": binding.COVERAGE_SCOPE,
            "source_schema": "mrf", "source_state": "TN", "query_sha256": binding.QUERY_SHA256,
            "columns": list(binding.REGISTRY_COLUMNS), "status": "passed", "all_rows_received": True,
            "connection_closed": True, "snapshot": {"read_only": "on", "isolation": "repeatable read",
                "snapshot_id": "100:101:", "backend_pid": 123, "snapshot_started_at": "2026-09-09 00:00:00+00",
                "server_version": "18", "database_name": "synthetic"},
            "registry_relations": {"mrf.npi": 11, "mrf.npi_taxonomy": 12, "mrf.nucc_taxonomy": 13},
            "expected_source_row_count": len(registry_rows), "row_count": len(registry_rows),
            "registry_rows_bytes": len(registry_bytes), "registry_rows_sha256": hashlib.sha256(registry_bytes).hexdigest(),
            "registry_rows": registry_rows}


def _save_snapshot(tmp_path, snapshot):
    content = encoded_json(snapshot)
    path = tmp_path / "registry.json"
    path.write_bytes(content)
    return {"snapshot_path": path, "snapshot_sha256": hashlib.sha256(content).hexdigest()}


def _bind(tmp_path, *, reports=None, candidates=None):
    reports = _reports() if reports is None else reports
    return binding.bind_reports(reports, reports_sha256=binding.reports_content_sha256(reports),
                                **_save_snapshot(tmp_path, _snapshot(candidates)))


def _decision(result, profession="1606"):
    return next(record["match_evidence"]["registry_binding"] for record in result["source_records"]
                if record["profession_code"] == profession)


def test_both_reports_bind_without_changing_assertions_or_source_identity(tmp_path):
    reports = _reports(md_rows=[source_row(LicenseNumber="123"), source_row(LicenseNumber="123"),
                               source_row(LicenseNumber="123", EducationProvider="Synthetic Second School")])
    inputs_before = copy.deepcopy(reports)
    original_records, original_facts = [], []
    for report in reports.values():
        records, facts = parse_report(report["content"], evidence=report["evidence"])
        original_records.extend(records)
        original_facts.extend(facts)
    candidates = [_candidate(), _candidate(), _candidate(taxonomy_occurrence_checksum=18), _do_candidate()]
    result = _bind(tmp_path, reports=reports, candidates=candidates)
    assert reports == inputs_before
    assert [record["matched_npi"] for record in result["source_records"]] == [1000000004, 1000000012]
    assert len(_decision(result)["candidate_rows"]) == 3
    assert _decision(result)["candidate_rows"][0] == _decision(result)["candidate_rows"][1]
    for actual, original in zip(result["source_records"], original_records, strict=True):
        actual = copy.deepcopy(actual)
        assert actual.pop("matched_npi") is not None
        assert actual.pop("match_status") == "deterministic"
        actual["match_evidence"].pop("registry_binding")
        original.pop("matched_npi")
        original.pop("match_status")
        assert actual == original
    for actual, original in zip(result["facts"], original_facts, strict=True):
        assert actual["npi"] is not None and actual["published_at"] is None
        assert {**actual, "npi": None} == original
    assert len([fact for fact in result["facts"] if fact["category"] == "education"]) == 3
    assert result == _bind(tmp_path, reports=reports, candidates=candidates)


def test_cross_board_number_resolves_only_with_both_explained_identities(tmp_path):
    reports = _reports(do_rows=[_do_row(LicenseNumber="123")])
    result = _bind(tmp_path, reports=reports, candidates=[_candidate(), _do_candidate(license_number="123")])
    records = result["source_records"]
    assert {record["source_record_key"] for record in records} == {"tennessee-tdh:1606:123", "tennessee-tdh:1907:123"}
    assert [record["matched_npi"] for record in records] == [1000000004, 1000000012]
    for record in records:
        decision = record["match_evidence"]["registry_binding"]
        assert len(decision["candidate_rows"]) == len(decision["source_groups"]) == 2
        assert decision["candidate_source_record_keys"] == ["tennessee-tdh:1606:123", "tennessee-tdh:1907:123"]
        assert decision["registry_license_profession"] == "not_supplied_by_nppes"
    assert {fact["npi"] for fact in result["facts"]} == {1000000004, 1000000012}


@pytest.mark.parametrize("peer_rows,peer_candidates", [
    ([_do_row(LicenseNumber="123", FirstName="Alex", MiddleName="Morgan", LastName="Example")], [_candidate()]),
    ([_do_row(LicenseNumber="123", Title="M.D.")], [_do_candidate(license_number="123", suffix="M.D.")]),
    ([_do_row(LicenseNumber="123"), _do_row(LicenseNumber="123", Status="Expired")], [_do_candidate(license_number="123")]),
    ([_do_row(LicenseNumber="123")], []),
    ([_do_row(LicenseNumber="123")], [_do_candidate(license_number="123", joined_npi=None)]),
    ([_do_row(LicenseNumber="123")], [_do_candidate(license_number="123", npi=1000000004, joined_npi=1000000004)]),
    ([_do_row(LicenseNumber="123")], [_do_candidate(license_number="123"),
        _do_candidate(license_number="123", npi=1000000020, joined_npi=1000000020)]),
])
def test_unresolved_cross_board_identity_stays_held(tmp_path, peer_rows, peer_candidates):
    result = _bind(tmp_path, reports=_reports(do_rows=peer_rows), candidates=[_candidate(), *peer_candidates])
    assert all(record["matched_npi"] is None for record in result["source_records"])
    assert all(fact["npi"] is None and fact["published_at"] is None for fact in result["facts"])
    assert len(_decision(result)["candidate_rows"]) == 1 + len(peer_candidates)


@pytest.mark.parametrize("changes", [
    {"npi": 0}, {"npi": "1000000004"}, {"npi": True}, {"joined_npi": None}, {"joined_npi": 1000000012},
    {"joined_npi": "1000000004"}, {"taxonomy_occurrence_checksum": None}, {"taxonomy_occurrence_checksum": True},
    {"license_state": "FL"}, {"entity_type_code": None}, {"entity_type_code": 2}, {"entity_type_code": True},
    {"taxonomy": None}, {"joined_taxonomy_code": None}, {"taxonomy_grouping": None},
    {"taxonomy_grouping": "Physician Assistants & Advanced Practice Nursing Providers"},
    {"first_name": None}, {"first_name": "Someone"}, {"middle_name": "M"}, {"last_name": "Other"}, {"suffix": "Jr."},
])
def test_conflicting_occurrence_is_never_filtered_away(tmp_path, changes):
    conflicting = _candidate(**changes)
    result = _bind(tmp_path, candidates=[_candidate(), conflicting, _do_candidate()])
    decision = _decision(result)
    assert decision["npi"] is None and decision["reason"] == "registry_identity_conflict"
    assert len(decision["candidate_rows"]) == 2 and conflicting in decision["candidate_rows"]
    assert _decision(result, "1907")["npi"] == 1000000012


@pytest.mark.parametrize("field", sorted(set(binding.REGISTRY_COLUMNS) - {"license_number"}))
def test_missing_occurrence_field_remains_a_conflict(tmp_path, field):
    candidate = _candidate()
    candidate.pop(field)
    result = _bind(tmp_path, candidates=[_candidate(), candidate, _do_candidate()])
    assert _decision(result)["reason"] == "registry_identity_conflict"


def test_multiple_npis_remain_ambiguous(tmp_path):
    result = _bind(tmp_path, candidates=[_candidate(), _candidate(npi=1000000020, joined_npi=1000000020), _do_candidate()])
    assert _decision(result)["status"] == "ambiguous"
    assert _decision(result)["npi"] is None


@pytest.mark.parametrize("license_number", [None, "", "0123", " 123", "123 ", "MD123", "123MD", "１２３"])
def test_registry_license_is_literal_without_rewriting(tmp_path, license_number):
    result = _bind(tmp_path, candidates=[_candidate(license_number=license_number), _do_candidate()])
    assert _decision(result)["status"] == "unmatched"
    assert _decision(result)["candidate_rows"] == []


@pytest.mark.parametrize("title,suffix,accepted", [("", None, True), ("Jr.", "Jr.", True), ("III", "iii", True), ("VI", "VI", True),
    ("Jr.", "Jr", False), ("M.D.", "M.D.", False), ("DO", None, False), ("Unknown", "Unknown", False)])
def test_title_is_preserved_and_never_guessed(tmp_path, title, suffix, accepted):
    reports = _reports(md_rows=[source_row(LicenseNumber="123", Title=title)])
    result = _bind(tmp_path, reports=reports, candidates=[_candidate(suffix=suffix), _do_candidate()])
    assert (_decision(result)["npi"] is not None) is accepted
    assert result["source_records"][0]["raw_payload"]["rows"][0]["fields"]["Title"] == title


def test_name_components_allow_only_case_whitespace_and_optional_null(tmp_path):
    reports = _reports(md_rows=[source_row(LicenseNumber="123", MiddleName="")])
    result = _bind(tmp_path, reports=reports, candidates=[_candidate(first_name=" ALEX ", middle_name=None), _do_candidate()])
    assert _decision(result)["npi"] == 1000000004


def test_blank_license_source_holds_are_not_rebound(tmp_path):
    reports = _reports(md_rows=[source_row(LicenseNumber=""), source_row(LicenseNumber="", FirstName="Other")])
    result = _bind(tmp_path, reports=reports, candidates=[_candidate(license_number=None), _do_candidate()])
    md_records = [record for record in result["source_records"] if record["profession_code"] == "1606"]
    assert len({record["source_record_key"] for record in md_records}) == 2
    assert all(record["normalized_payload"]["visibility"] == "held_identity" for record in md_records)
    assert all(record["matched_npi"] is None and record["match_evidence"]["reason"] == "license_number_missing"
               for record in md_records)
    assert {fact["source_record_id"] for fact in result["facts"]}.isdisjoint({record["record_id"] for record in md_records})


def test_malformed_source_rows_remain_held_with_original_coordinates(tmp_path):
    reports = _reports()
    content = report_bytes([source_row(LicenseNumber="123")], PRACTICE_HEADERS) + malformed_row(LicenseNumber="123")
    reports["1606"]["content"] = content
    reports["1606"]["evidence"]["content_sha256"] = hashlib.sha256(content).hexdigest()
    original, _ = parse_report(content, evidence=reports["1606"]["evidence"])
    result = _bind(tmp_path, reports=reports)
    actual = result["source_records"][0]
    assert actual["raw_payload"] == original[0]["raw_payload"]
    assert actual["normalized_payload"] == original[0]["normalized_payload"]
    assert _decision(result)["reason"] == "malformed_source_row" and actual["matched_npi"] is None


@pytest.mark.parametrize("changes", [{"source_state": "NY"}, {"source_schema": "other"}, {"schema_version": "other"},
    {"coverage_scope": "physicians_only"}, {"query_sha256": "0" * 64}, {"columns": binding.REGISTRY_COLUMNS[:-1]},
    {"status": "failed"}, {"all_rows_received": False}, {"connection_closed": False}, {"row_count": 1},
    {"expected_source_row_count": True}, {"registry_rows_bytes": 1}, {"registry_rows_sha256": "0" * 64},
    {"registry_relations": {}}, {"registry_relations": {"mrf.npi": True, "mrf.npi_taxonomy": 12, "mrf.nucc_taxonomy": 13}},
])
def test_incomplete_or_changed_snapshot_contract_rejected(tmp_path, changes):
    options = _save_snapshot(tmp_path, {**_snapshot(), **changes})
    with pytest.raises(ValueError, match="tennessee_binding_snapshot_"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize("changes", [{"read_only": "off"}, {"isolation": "read committed"}, {"snapshot_id": "invalid"},
    {"backend_pid": True}, {"backend_pid": 0}, {"snapshot_started_at": ""}, {"server_version": None}, {"database_name": ""}])
def test_capture_transaction_metadata_required(tmp_path, changes):
    snapshot = _snapshot()
    snapshot["snapshot"].update(changes)
    options = _save_snapshot(tmp_path, snapshot)
    with pytest.raises(ValueError, match="snapshot_transaction_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize("row", [{"npi": 1000000004}, {"license_number": 123}, {"license_number": "123", "profession_code": "1606"}])
def test_snapshot_cannot_hide_a_license_or_invent_profession(tmp_path, row):
    options = _save_snapshot(tmp_path, _snapshot([row]))
    with pytest.raises(ValueError, match="snapshot_rows_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


def test_independent_snapshot_pin_detects_rehashed_removed_conflict(tmp_path):
    options = _save_snapshot(tmp_path, _snapshot([_candidate(), _candidate(first_name="Other")]))
    options["snapshot_path"].write_bytes(encoded_json(_snapshot([_candidate()])))
    with pytest.raises(ValueError, match="snapshot_changed"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


def test_snapshot_preserves_occurrence_order(tmp_path):
    snapshot = _snapshot()
    snapshot["registry_rows"].reverse()
    content = encoded_json(snapshot["registry_rows"])
    snapshot.update(registry_rows_bytes=len(content), registry_rows_sha256=hashlib.sha256(content).hexdigest())
    options = _save_snapshot(tmp_path, snapshot)
    with pytest.raises(ValueError, match="snapshot_order_changed"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize("change", ["content", "evidence"])
def test_independent_reports_pin_rejects_changed_capture(tmp_path, change):
    reports = _reports()
    original_pin = binding.reports_content_sha256(reports)
    if change == "content":
        reports["1606"]["content"] = reports["1606"]["content"].replace(b"Alex", b"Other")
        reports["1606"]["evidence"]["content_sha256"] = hashlib.sha256(reports["1606"]["content"]).hexdigest()
    else:
        reports["1606"]["evidence"]["artifact_id"] = "changed"
    with pytest.raises(ValueError, match="reports_changed"):
        binding.bind_reports(reports, reports_sha256=original_pin, **_save_snapshot(tmp_path, _snapshot()))


def test_caller_changes_cannot_replace_already_pinned_report_inputs(tmp_path, monkeypatch):
    reports = _reports()
    originals = copy.deepcopy(reports)
    options = _save_snapshot(tmp_path, _snapshot())
    read_snapshot = binding.read_registry_snapshot

    def mutate_inputs(*args, **kwargs):
        reports["1606"]["content"] = reports["1606"]["content"].replace(b"Alex", b"Other")
        reports["1606"]["evidence"].update(artifact_id="changed", content_sha256=hashlib.sha256(reports["1606"]["content"]).hexdigest())
        return read_snapshot(*args, **kwargs)

    monkeypatch.setattr(binding, "read_registry_snapshot", mutate_inputs)
    result = binding.bind_reports(reports, reports_sha256=binding.reports_content_sha256(reports), **options)
    assert result["source_records"][0]["artifact_id"] == originals["1606"]["evidence"]["artifact_id"]
    assert _decision(result)["npi"] == 1000000004
    assert all(fact["source_json"]["content_sha256"] == originals["1606"]["evidence"]["content_sha256"]
               for fact in result["facts"] if fact["npi"] == 1000000004)


@pytest.mark.parametrize("pin", [None, "", "f" * 63, "z" * 64])
def test_independent_pins_are_required(tmp_path, pin):
    reports = _reports()
    options = _save_snapshot(tmp_path, _snapshot())
    with pytest.raises(ValueError, match="reports_pin_invalid"):
        binding.bind_reports(reports, reports_sha256=pin, **options)
    with pytest.raises(ValueError, match="snapshot_pin_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=pin)


def test_both_nonempty_profession_reports_are_required(tmp_path):
    reports = _reports()
    reports.pop("1907")
    with pytest.raises(ValueError, match="report_professions_invalid"):
        binding.reports_content_sha256(reports)
    with pytest.raises(ValueError, match="report_profession_mismatch"):
        _bind(tmp_path, reports=_reports(do_rows=[]))
    with pytest.raises(ValueError, match="report_profession_mismatch"):
        _bind(tmp_path, reports=_reports(do_rows=[source_row()]))


def test_both_reports_must_belong_to_the_same_import_run(tmp_path):
    reports = _reports()
    reports["1907"]["evidence"]["run_id"] = "different-run"
    with pytest.raises(ValueError, match="report_run_id_mismatch"):
        _bind(tmp_path, reports=reports)


def test_snapshot_file_bound_and_symlink_are_rejected(tmp_path, monkeypatch):
    options = _save_snapshot(tmp_path, _snapshot())
    link = tmp_path / "linked.json"
    link.symlink_to(options["snapshot_path"])
    with pytest.raises(ValueError, match="artifact_symlink"):
        binding.read_registry_snapshot(link, snapshot_sha256=options["snapshot_sha256"])
    monkeypatch.setattr(binding, "MAX_SNAPSHOT_BYTES", 16)
    with pytest.raises(ValueError, match="artifact_file_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])
