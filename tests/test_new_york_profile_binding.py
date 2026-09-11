# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import hashlib

import pytest

from process import new_york_profile_binding as binding
from process.new_york_profile_acquisition import encoded_json
from process.new_york_profile_retained import read_acquisition
from tests.test_new_york_profile_acquisition import SourceResponse, _acquire, _education, _search, source_session
from tests.test_new_york_profile_retained import _body


def _candidate(**changes):
    return {
        "npi": 1000000004,
        "taxonomy_occurrence_checksum": 17,
        "license_number": "654321",
        "license_state": "NY",
        "taxonomy": "207R00000X",
        "primary_taxonomy_switch": "Y",
        "joined_npi": 1000000004,
        "entity_type_code": 1,
        "first_name": "Alex",
        "middle_name": None,
        "last_name": "Example",
        "suffix": None,
        "joined_taxonomy_code": "207R00000X",
        "taxonomy_grouping": "Allopathic & Osteopathic Physicians",
        **changes,
    }


def _snapshot(registry_rows=None):
    registry_rows = [_candidate()] if registry_rows is None else registry_rows
    registry_bytes = encoded_json(registry_rows)
    return {
        "schema_version": binding.SNAPSHOT_SCHEMA,
        "coverage_scope": binding.COVERAGE_SCOPE,
        "source_schema": "mrf",
        "source_state": "NY",
        "query_sha256": binding.QUERY_SHA256,
        "columns": list(binding.REGISTRY_COLUMNS),
        "status": "passed",
        "all_rows_received": True,
        "connection_closed": True,
        "snapshot": {
            "read_only": "on",
            "isolation": "repeatable read",
            "snapshot_id": "100:101:",
            "backend_pid": 123,
            "snapshot_started_at": "2026-09-09 00:00:00+00",
            "server_version": "18",
            "database_name": "synthetic",
        },
        "registry_relations": {"mrf.npi": 11, "mrf.npi_taxonomy": 12, "mrf.nucc_taxonomy": 13},
        "expected_source_row_count": len(registry_rows),
        "row_count": len(registry_rows),
        "registry_rows_bytes": len(registry_bytes),
        "registry_rows_sha256": hashlib.sha256(registry_bytes).hexdigest(),
        "registry_rows": registry_rows,
    }


def _save_snapshot(tmp_path, snapshot_by_field):
    path = tmp_path / "registry.json"
    snapshot_bytes = encoded_json(snapshot_by_field)
    path.write_bytes(snapshot_bytes)
    return {"snapshot_path": path, "snapshot_sha256": hashlib.sha256(snapshot_bytes).hexdigest()}


@pytest.fixture
async def retained_case(source_session):
    session = source_session(SourceResponse(_search()), SourceResponse(_education(nationalProviderId="")))
    await _acquire(session)
    return _pinned_case(session)


def _pinned_case(session):
    manifest_sha256 = hashlib.sha256((session.destination / "manifest.json").read_bytes()).hexdigest()
    return session, manifest_sha256, binding.acquisition_content_sha256(session.destination)


def _bind(retained_case, tmp_path, registry_rows=None):
    session, manifest_sha256, acquisition_sha256 = retained_case
    snapshot_options = _save_snapshot(tmp_path, _snapshot(registry_rows))
    return binding.bind_retained_acquisition(
        session.destination, manifest_sha256=manifest_sha256, acquisition_sha256=acquisition_sha256, **snapshot_options
    )


async def test_binding_preserves_duplicates_and_fact_provenance(retained_case, tmp_path):
    session, manifest_sha256, acquisition_sha256 = retained_case
    original = read_acquisition(session.destination, manifest_sha256=manifest_sha256)
    candidates = [_candidate(), _candidate(), _candidate(taxonomy_occurrence_checksum=18)]
    bound = _bind(retained_case, tmp_path, candidates)
    assert bound["outcome"] == "accepted" and bound["reason"] == "unique_exact_license_name"
    source_record = bound["source_record"]
    assert source_record["matched_npi"] == 1000000004 and source_record["match_status"] == "deterministic"
    assert source_record["profession_code"] is None
    assert source_record["match_evidence"]["registry_binding"]["acquisition_sha256"] == acquisition_sha256
    assert source_record["match_evidence"]["registry_binding"]["registry_license_profession"] == "not_supplied_by_nppes"
    assert source_record["match_evidence"]["registry_binding"]["candidate_rows"] == candidates
    for field in original["source_record"]:
        if field not in {"matched_npi", "match_status", "match_evidence"}:
            assert source_record[field] == original["source_record"][field]
    assert (
        source_record["match_evidence"]["license_search"]
        == original["source_record"]["match_evidence"]["license_search"]
    )
    expected_facts = copy.deepcopy(original["facts"])
    for fact in expected_facts:
        fact["npi"] = 1000000004
    assert bound["facts"] == expected_facts
    assert all(fact["published_at"] is None and "candidate_rows" not in fact["source_json"] for fact in bound["facts"])
    assert read_acquisition(session.destination, manifest_sha256=manifest_sha256) == original
    assert len(session.requests) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": 1234567890},
        {"npi": "1000000004"},
        {"joined_npi": None},
        {"joined_npi": 1000000012},
        {"joined_npi": True},
        {"taxonomy_occurrence_checksum": None},
        {"taxonomy_occurrence_checksum": True},
        {"license_state": "MA"},
        {"entity_type_code": 2},
        {"entity_type_code": True},
        {"entity_type_code": None},
        {"taxonomy": ""},
        {"taxonomy": None},
        {"joined_taxonomy_code": None},
        {"joined_taxonomy_code": "Other"},
        {"taxonomy_grouping": "Other"},
        {"taxonomy_grouping": None},
        {"first_name": "A"},
        {"first_name": "Example", "last_name": "Alex"},
        {"first_name": None},
        {"last_name": None},
        {"middle_name": "Morgan"},
        {"suffix": "Jr"},
        {"middle_name": False},
        {"suffix": 0},
    ],
)
async def test_conflicting_candidate_is_not_filtered_away(retained_case, tmp_path, changes):
    candidates = [_candidate(), _candidate(**changes)]
    bound = _bind(retained_case, tmp_path, candidates)
    assert bound["outcome"] == "held" and bound["reason"] == "registry_identity_conflict"
    assert bound["source_record"]["matched_npi"] is None
    assert bound["source_record"]["match_evidence"]["registry_binding"]["candidate_rows"] == candidates
    assert all(fact["npi"] is None for fact in bound["facts"])


@pytest.mark.parametrize(
    "field",
    [
        "first_name",
        "middle_name",
        "last_name",
        "suffix",
        "joined_npi",
        "joined_taxonomy_code",
        "entity_type_code",
        "taxonomy_occurrence_checksum",
    ],
)
async def test_missing_candidate_columns_hold_the_literal_root(retained_case, tmp_path, field):
    incomplete = _candidate()
    del incomplete[field]
    bound = _bind(retained_case, tmp_path, [_candidate(), incomplete])
    assert bound["outcome"] == "held" and bound["source_record"]["matched_npi"] is None
    assert bound["source_record"]["match_evidence"]["registry_binding"]["candidate_rows"][1] == incomplete


@pytest.mark.parametrize("middle,suffix", [(None, None), ("", ""), ("  ", " ")])
async def test_present_optional_blank_names_and_case_match(retained_case, tmp_path, middle, suffix):
    bound = _bind(
        retained_case,
        tmp_path,
        [_candidate(first_name=" ALEX ", last_name="example", middle_name=middle, suffix=suffix)],
    )
    assert bound["outcome"] == "accepted"


async def test_multiple_valid_npis_remain_ambiguous(retained_case, tmp_path):
    assert binding.is_valid_npi(1000000012)
    bound = _bind(retained_case, tmp_path, [_candidate(), _candidate(npi=1000000012, joined_npi=1000000012)])
    assert bound["outcome"] == "held" and bound["reason"] == "multiple_matching_npis"
    assert bound["source_record"]["match_status"] == "ambiguous"


@pytest.mark.parametrize("license_number", [None, "", " 654321 ", "060654321", "654321-1", "６５４３２１", "123456"])
async def test_other_literal_formats_are_retained_without_binding(retained_case, tmp_path, license_number):
    candidates = [_candidate(license_number=license_number)]
    snapshot_by_field = _snapshot(candidates)
    options = _save_snapshot(tmp_path, snapshot_by_field)
    assert (
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])
        == snapshot_by_field
    )
    bound = _bind(retained_case, tmp_path, candidates)
    assert bound["outcome"] == "held" and bound["reason"] == "no_exact_license_candidates"


@pytest.mark.parametrize(
    "source_npi,expected",
    [
        ("", "accepted"),
        ("1000000004", "accepted"),
        ("1000000012", "held"),
        ("1234567890", "held"),
        ("not-an-npi", "held"),
    ],
)
async def test_explicit_source_npi_must_agree(source_session, tmp_path, source_npi, expected):
    session = source_session(SourceResponse(_search()), SourceResponse(_education(nationalProviderId=source_npi)))
    await _acquire(session)
    bound = _bind(_pinned_case(session), tmp_path)
    assert bound["outcome"] == expected
    if expected == "held":
        assert bound["reason"] == "source_npi_identity_conflict" and all(fact["npi"] is None for fact in bound["facts"])


@pytest.mark.parametrize(
    "registry_rows",
    [
        [_candidate(), _candidate(taxonomy_grouping=None)],
        [_candidate(), _candidate(npi=1000000012, joined_npi=1000000012)],
    ],
)
async def test_source_npi_cannot_bypass_candidate_conflicts(source_session, tmp_path, registry_rows):
    session = source_session(SourceResponse(_search()), SourceResponse(_education()))
    await _acquire(session)
    assert _bind(_pinned_case(session), tmp_path, registry_rows)["outcome"] == "held"


async def test_known_middle_suffix_and_search_disagreement_stay_held(source_session, tmp_path):
    session = source_session(
        SourceResponse(_search(physicianFirstName="Example", physicianLastName="Alex")),
        SourceResponse(_education(middleName="Morgan", suffix="Jr")),
    )
    await _acquire(session)
    bound = _bind(_pinned_case(session), tmp_path, [_candidate(middle_name="Morgan", suffix="Jr")])
    assert bound["outcome"] == "held" and bound["reason"] == "search_header_name_disagreement"
    assert bound["source_record"]["normalized_payload"]["quality_flags"] == ["search_header_name_disagreement"]
    assert bound["facts"][0]["source_json"]["quality_flags"] == ["search_header_name_disagreement"]


@pytest.mark.parametrize(
    "candidate", [_candidate(), _candidate(middle_name="M", suffix="Jr"), _candidate(middle_name="Morgan", suffix="Jr")]
)
async def test_known_optional_components_require_exact_match(source_session, tmp_path, candidate):
    session = source_session(SourceResponse(_search()), SourceResponse(_education(middleName="Morgan", suffix="Jr")))
    await _acquire(session)
    bound = _bind(_pinned_case(session), tmp_path, [candidate])
    assert (bound["outcome"] == "accepted") is (candidate["middle_name"] == "Morgan")


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": "other"},
        {"coverage_scope": "physicians_only"},
        {"source_state": "MA"},
        {"source_schema": "other"},
        {"query_sha256": "0" * 64},
        {"columns": ["npi", "license_number"]},
        {"status": "failed"},
        {"all_rows_received": False},
        {"all_rows_received": 1},
        {"connection_closed": False},
        {"snapshot": {}},
        {"registry_relations": {}},
        {"expected_source_row_count": 2},
        {"row_count": True},
        {"registry_rows_bytes": 1},
        {"registry_rows_sha256": "0" * 64},
        {"registry_rows": []},
    ],
)
def test_pinned_incomplete_or_wrong_scope_snapshots_fail(tmp_path, changes):
    snapshot_by_field = {**_snapshot(), **changes}
    options = _save_snapshot(tmp_path, snapshot_by_field)
    with pytest.raises(ValueError):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize(
    "changes",
    [
        {"read_only": "off"},
        {"isolation": "read committed"},
        {"snapshot_id": ""},
        {"backend_pid": True},
        {"backend_pid": 0},
        {"database_name": ""},
        {"server_version": None},
        {"snapshot_started_at": ""},
    ],
)
def test_snapshot_transaction_metadata_is_required(tmp_path, changes):
    snapshot_by_field = _snapshot()
    snapshot_by_field["snapshot"].update(changes)
    options = _save_snapshot(tmp_path, snapshot_by_field)
    with pytest.raises(ValueError, match="snapshot_transaction_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize(
    "rows",
    [
        [None],
        [{}],
        [{"license_number": "654321", "profession_code": "060"}],
        [{"license_number": 654321}],
        [{"license_number": ["654321"]}],
        [{"license_number": True}],
    ],
)
def test_uninterpretable_rows_cannot_hide_a_license(tmp_path, rows):
    options = _save_snapshot(tmp_path, _snapshot(rows))
    with pytest.raises(ValueError, match="snapshot_rows_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


def test_snapshot_pin_rejects_rehashed_removed_conflict(tmp_path):
    options = _save_snapshot(tmp_path, _snapshot([_candidate(), _candidate(first_name="Other")]))
    _save_snapshot(tmp_path, _snapshot([_candidate()]))
    with pytest.raises(ValueError, match="snapshot_changed"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


def test_snapshot_occurrence_order_is_preserved(tmp_path):
    options = _save_snapshot(tmp_path, _snapshot([_candidate(taxonomy_occurrence_checksum=18), _candidate()]))
    with pytest.raises(ValueError, match="snapshot_order_changed"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize("oid", [None, 0, -1, True, "12"])
def test_registry_relations_require_actual_oids(tmp_path, oid):
    snapshot_by_field = _snapshot()
    snapshot_by_field["registry_relations"]["mrf.npi"] = oid
    options = _save_snapshot(tmp_path, snapshot_by_field)
    with pytest.raises(ValueError, match="snapshot_relations_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


@pytest.mark.parametrize("pin", [None, "", "g" * 64, "0" * 64])
def test_snapshot_requires_external_pin(tmp_path, pin):
    options = _save_snapshot(tmp_path, _snapshot())
    with pytest.raises(ValueError):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=pin)


async def test_binder_rechecks_retained_acquisition(retained_case, tmp_path):
    session, _, _ = retained_case
    path = session.destination / "education.response.json"
    path.write_bytes(b'{"complete": false}')
    with pytest.raises(ValueError):
        _bind(retained_case, tmp_path)


async def test_binder_requires_original_manifest_pin(retained_case, tmp_path):
    session, _, acquisition_sha256 = retained_case
    with pytest.raises(ValueError, match="manifest_changed"):
        _bind((session, "0" * 64, acquisition_sha256), tmp_path)


def test_snapshot_file_limit_and_symlink_are_rejected(tmp_path, monkeypatch):
    options = _save_snapshot(tmp_path, _snapshot())
    alias = tmp_path / "alias.json"
    alias.symlink_to(options["snapshot_path"])
    with pytest.raises(ValueError, match="symlink"):
        binding.read_registry_snapshot(alias, snapshot_sha256=options["snapshot_sha256"])
    monkeypatch.setattr(binding, "MAX_SNAPSHOT_BYTES", 1)
    with pytest.raises(ValueError, match="artifact_file_invalid"):
        binding.read_registry_snapshot(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])


async def test_network_plan_npi_never_fills_header(source_session, tmp_path, monkeypatch):
    education = _education(nationalProviderId="")
    education["data"]["pndsHealthPlans"] = [{"physicianNPI": "1000000012", "physicianLicense": "654321"}]
    session = source_session(SourceResponse(_search()), SourceResponse(education))
    await _acquire(session)
    pinned_case = _pinned_case(session)

    def no_session(*args, **kwargs):
        raise AssertionError("Offline binding cannot open a source session")

    from process import new_york_profile_acquisition as acquisition

    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", no_session)
    bound = _bind(pinned_case, tmp_path)
    assert bound["outcome"] == "accepted" and bound["source_record"]["matched_npi"] == 1000000004
    assert bound["source_record"]["raw_payload"]["data"] == education["data"]
    assert bound["source_record"]["match_evidence"]["registry_binding"]["source_npi"] == ""


@pytest.mark.parametrize("stage,change", [("education", "school"), ("education", "header"), ("search", "search")])
async def test_rehashed_replacement_requires_original_content_pin(retained_case, tmp_path, stage, change):
    session, manifest_sha256, _ = retained_case
    content = _search() if stage == "search" else _education(nationalProviderId="")
    if change == "school":
        content["data"]["medSchools"][0]["schoolName"] = "Different Synthetic School"
    elif change == "header":
        content["data"]["phyInfo"]["licenseDate"] = "2000-01-01"
    else:
        content["data"]["physicians"][0]["statusCode"] = "I"
    _body(session.destination, stage, content)
    # The replacement is internally consistent and keeps the original summary.
    assert read_acquisition(session.destination, manifest_sha256=manifest_sha256)["outcome"] == "acquired"
    with pytest.raises(ValueError, match="acquisition_changed"):
        _bind(retained_case, tmp_path)


@pytest.mark.parametrize("name", [name for name, _ in binding.ACQUISITION_FILES])
async def test_every_consumed_file_has_a_byte_pin(retained_case, tmp_path, name):
    session, manifest_sha256, _ = retained_case
    path = session.destination / name
    path.write_bytes(path.read_bytes() + b"\n")
    assert read_acquisition(session.destination, manifest_sha256=manifest_sha256)["outcome"] == "acquired"
    with pytest.raises(ValueError, match="acquisition_changed"):
        _bind(retained_case, tmp_path)


@pytest.mark.parametrize("stage", ["education", "search"])
async def test_replay_must_consume_pinned_response(retained_case, tmp_path, monkeypatch, stage):
    session, _, _ = retained_case
    actual_read = binding.read_acquisition
    response_path = session.destination / f"{stage}.response.json"
    original_bytes = response_path.read_bytes()

    def replace_during_replay(destination, **options):
        content = (
            _education(nationalProviderId="", licenseDate="2000-01-01")
            if stage == "education"
            else _search(statusCode="I")
        )
        try:
            _body(destination, stage, content)
            return actual_read(destination, **options)
        finally:
            response_path.write_bytes(original_bytes)

    monkeypatch.setattr(binding, "read_acquisition", replace_during_replay)
    with pytest.raises(ValueError, match="acquisition_replay_changed"):
        _bind(retained_case, tmp_path)


@pytest.mark.parametrize("pin", [None, "", "g" * 64, "0" * 64])
async def test_acquisition_requires_external_content_pin(retained_case, tmp_path, pin):
    session, manifest_sha256, _ = retained_case
    with pytest.raises(ValueError, match="acquisition_(pin_invalid|changed)"):
        _bind((session, manifest_sha256, pin), tmp_path)


@pytest.mark.parametrize("total", [0, 2, 11])
async def test_nonsingleton_search_remains_held(source_session, tmp_path, total):
    session = source_session(SourceResponse(_search(total=total)))
    result = await _acquire(session)
    assert result == {
        "outcome": "held",
        "reason": "search_not_singleton",
        "reported_total": total,
        "source_record": None,
        "facts": [],
    }
    retained_bytes_by_name = {path.name: path.read_bytes() for path in session.destination.iterdir()}
    with pytest.raises(ValueError, match="acquisition_file_invalid"):
        binding.bind_retained_acquisition(
            session.destination,
            manifest_sha256=hashlib.sha256(retained_bytes_by_name["manifest.json"]).hexdigest(),
            acquisition_sha256="0" * 64,
            **_save_snapshot(tmp_path, _snapshot()),
        )
    assert len(session.requests) == 1
    assert {path.name: path.read_bytes() for path in session.destination.iterdir()} == retained_bytes_by_name
