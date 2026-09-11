# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import copy
import base64
import hashlib
import json
import socket
from pathlib import Path

import pytest

from process import new_york_profile_binding as binding
from process import new_york_nysed_profile as nysed
from process import new_york_nysed_profile_acquisition as nysed_acquisition
from process.new_york_profile_acquisition import encoded_json
from process.new_york_profile_retained import read_acquisition
from tests.test_new_york_profile_acquisition import SourceResponse, _acquire, _education, _search, source_session
from tests.test_new_york_profile_retained import _body
from tests.test_new_york_nysed_profile import (
    PUBLIC_HEADER,
    SourceResponse as NysedResponse,
    SourceSession as NysedSession,
    _profile_body,
)


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


@pytest.fixture
def offline_binding(monkeypatch):
    def deny(*_args, **_kwargs):
        raise AssertionError("Corroborated binding checks forbid network access")

    monkeypatch.setattr(socket.socket, "connect", deny)
    monkeypatch.setattr(socket.socket, "connect_ex", deny)
    monkeypatch.setattr(socket, "create_connection", deny)


async def _paired_case(
    source_session, monkeypatch, tmp_path, *, source_names=None, search_names=None, nysed_fields=None
):
    source_by_field = {
        "firstName": "Alex",
        "middleName": "Morgan",
        "lastName": "Example",
        "suffix": "",
        "nationalProviderId": "",
        **(source_names or {}),
    }
    search_by_field = {"physicianFirstName": "Example", "physicianLastName": "Alex", **(search_names or {})}
    session = source_session(SourceResponse(_search(**search_by_field)), SourceResponse(_education(**source_by_field)))
    await _acquire(session)
    _, manifest_sha256, acquisition_sha256 = _pinned_case(session)
    legal_name = " ".join(source_by_field[field] for field in ("lastName", "firstName", "middleName", "suffix")).strip()
    nysed_body = _profile_body(**{"name": legal_name, **(nysed_fields or {})})
    nysed_session = NysedSession(NysedResponse(nysed_body))
    nysed_directory = tmp_path.resolve() / "nysed"
    monkeypatch.setattr(nysed_acquisition.aiohttp, "ClientSession", lambda **_options: nysed_session)
    acquired = await nysed_acquisition.acquire_license(
        nysed_body["licenseNumber"]["value"], nysed_directory, run_id="synthetic-nysed", api_key=PUBLIC_HEADER
    )
    return {
        "destination": session.destination,
        "manifest_sha256": manifest_sha256,
        "acquisition_sha256": acquisition_sha256,
        "nysed_destination": nysed_directory,
        "nysed_receipt_sha256": acquired["receipt_sha256"],
    }


@pytest.fixture
async def paired_case(source_session, monkeypatch, tmp_path, offline_binding):
    return await _paired_case(source_session, monkeypatch, tmp_path)


def _corroborated_bind(paired_case, tmp_path, registry_rows=None):
    return binding.bind_corroborated_acquisition(**paired_case, **_save_snapshot(tmp_path, _snapshot(registry_rows)))


@pytest.mark.parametrize(
    "middle,relationship",
    [
        (None, "registry_unreported"),
        ("", "registry_unreported"),
        ("  ", "registry_unreported"),
        ("M", "registry_initial"),
        ("m", "registry_initial"),
        ("Morgan", "equal"),
    ],
)
async def test_corroborated_link_preserves_source_evidence(paired_case, tmp_path, middle, relationship):
    original = read_acquisition(paired_case["destination"], manifest_sha256=paired_case["manifest_sha256"])
    nysed_original = nysed.read_acquisition(
        paired_case["nysed_destination"], receipt_sha256=paired_case["nysed_receipt_sha256"]
    )
    candidates = [_candidate(middle_name=middle), _candidate(middle_name=middle)]
    bytes_by_path = {
        str(path): path.read_bytes()
        for directory in (paired_case["destination"], paired_case["nysed_destination"])
        for path in directory.iterdir()
    }
    bound = _corroborated_bind(paired_case, tmp_path, candidates)
    assert bound["outcome"] == "accepted" and bound["reason"] == "unique_exact_license_corroborated_name"
    decision = bound["source_record"]["match_evidence"]["registry_binding"]
    assert decision["method"] == binding.CORROBORATED_METHOD and decision["npi"] == 1000000004
    assert decision["npi_verification"] == "not_independently_verified"
    assert decision["candidate_rows"] == candidates
    assert decision["registry_middle_name_relationships"] == [relationship, relationship]
    assert decision["search_header_name_relationship"] == "first_last_transposed"
    assert decision["nysed_corroboration"]["receipt_sha256"] == paired_case["nysed_receipt_sha256"]
    assert decision["nysed_corroboration"]["artifact_id"] == nysed_original["source_record"]["artifact_id"]
    assert decision["nysed_corroboration"]["license_matches"] is True
    assert decision["nysed_corroboration"]["header_legal_name_matches"] is True
    expected_facts = copy.deepcopy(original["facts"])
    for fact in expected_facts:
        fact["npi"] = 1000000004
    assert bound["facts"] == expected_facts
    assert "search_header_name_disagreement" in bound["source_record"]["normalized_payload"]["quality_flags"]
    assert bound["source_record"]["profession_code"] is None
    assert bound["source_record"]["raw_payload"] == original["source_record"]["raw_payload"]
    assert (
        bound["source_record"]["match_evidence"]["license_search"]
        == original["source_record"]["match_evidence"]["license_search"]
    )
    assert all(fact["published_at"] is None for fact in bound["facts"])
    assert {path: Path(path).read_bytes() for path in bytes_by_path} == bytes_by_path
    assert (
        nysed.read_acquisition(paired_case["nysed_destination"], receipt_sha256=paired_case["nysed_receipt_sha256"])
        == nysed_original
    )
    strict_by_field = {key: setting for key, setting in paired_case.items() if not key.startswith("nysed_")}
    strict = binding.bind_retained_acquisition(**strict_by_field, **_save_snapshot(tmp_path, _snapshot(candidates)))
    assert strict["outcome"] == "held" and strict["reason"] == "search_header_name_disagreement"


@pytest.mark.parametrize(
    "changes",
    [
        {"middle_name": "Martha"},
        {"middle_name": "Mo"},
        {"middle_name": "N"},
        {"middle_name": "M."},
        {"middle_name": "M G"},
        {"middle_name": False},
        {"middle_name": 0},
        {"first_name": "A"},
        {"last_name": "Different"},
        {"first_name": "Example", "last_name": "Alex"},
        {"first_name": None},
        {"suffix": "Jr"},
        {"suffix": False},
        {"npi": 1234567890},
        {"joined_npi": None},
        {"joined_npi": True},
        {"joined_npi": 1000000012},
        {"entity_type_code": 2},
        {"entity_type_code": True},
        {"taxonomy_occurrence_checksum": None},
        {"taxonomy_occurrence_checksum": True},
        {"taxonomy": ""},
        {"joined_taxonomy_code": None},
        {
            "taxonomy_grouping": "Nursing Service Providers",
            "taxonomy": "164W00000X",
            "joined_taxonomy_code": "164W00000X",
        },
    ],
)
async def test_corroboration_never_discards_conflicts(paired_case, tmp_path, changes):
    candidates = [_candidate(), _candidate(**changes)]
    bound = _corroborated_bind(paired_case, tmp_path, candidates)
    assert bound["outcome"] == "held" and bound["reason"] == "registry_identity_conflict"
    assert bound["source_record"]["match_evidence"]["registry_binding"]["candidate_rows"] == candidates
    assert all(fact["npi"] is None for fact in bound["facts"])


@pytest.mark.parametrize(
    "field",
    [
        "first_name",
        "last_name",
        "middle_name",
        "suffix",
        "joined_npi",
        "entity_type_code",
        "joined_taxonomy_code",
        "taxonomy_occurrence_checksum",
    ],
)
async def test_corroboration_requires_present_columns(paired_case, tmp_path, field):
    incomplete = _candidate()
    del incomplete[field]
    bound = _corroborated_bind(paired_case, tmp_path, [_candidate(), incomplete])
    assert bound["outcome"] == "held" and bound["reason"] == "registry_identity_conflict"


@pytest.mark.parametrize(
    "source_middle,registry_middle",
    [("", "Morgan"), ("M", "Morgan"), ("M", "Martha"), ("Mary Ann", "M"), ("Martha", "Morgan")],
)
async def test_middle_compatibility_is_asymmetric(
    source_session, monkeypatch, tmp_path, offline_binding, source_middle, registry_middle
):
    paired = await _paired_case(source_session, monkeypatch, tmp_path, source_names={"middleName": source_middle})
    bound = _corroborated_bind(paired, tmp_path, [_candidate(middle_name=registry_middle)])
    assert bound["outcome"] == "held" and bound["reason"] == "registry_identity_conflict"


async def test_multiple_npis_cannot_be_overridden(source_session, monkeypatch, tmp_path, offline_binding):
    paired = await _paired_case(
        source_session, monkeypatch, tmp_path, source_names={"nationalProviderId": "1000000004"}
    )
    candidates = [_candidate(), _candidate(npi=1000000012, joined_npi=1000000012)]
    bound = _corroborated_bind(paired, tmp_path, candidates)
    assert bound["outcome"] == "held" and bound["reason"] == "multiple_matching_npis"
    assert bound["source_record"]["match_status"] == "ambiguous"


@pytest.mark.parametrize(
    "source_npi,accepted",
    [("1000000004", True), ("", True), ("1000000012", False), ("1234567890", False), ("unexpected", False)],
)
async def test_corroboration_checks_reported_npi(
    source_session, monkeypatch, tmp_path, offline_binding, source_npi, accepted
):
    paired = await _paired_case(source_session, monkeypatch, tmp_path, source_names={"nationalProviderId": source_npi})
    bound = _corroborated_bind(paired, tmp_path)
    assert (bound["outcome"] == "accepted") is accepted
    if not accepted:
        assert bound["reason"] == "source_npi_identity_conflict"


@pytest.mark.parametrize(
    "search_names,accepted,relationship",
    [
        ({"physicianFirstName": "Alex", "physicianLastName": "Example"}, True, "equal"),
        ({"physicianFirstName": "Example", "physicianLastName": "Alex"}, True, "first_last_transposed"),
        ({"physicianFirstName": "A", "physicianLastName": "Example"}, False, "conflict"),
        ({"physicianFirstName": "Other", "physicianLastName": "Person"}, False, "conflict"),
    ],
)
async def test_only_exact_search_relationships_are_accepted(
    source_session, monkeypatch, tmp_path, offline_binding, search_names, accepted, relationship
):
    paired = await _paired_case(source_session, monkeypatch, tmp_path, search_names=search_names)
    bound = _corroborated_bind(paired, tmp_path)
    assert (bound["outcome"] == "accepted") is accepted
    assert (
        bound["source_record"]["match_evidence"]["registry_binding"]["search_header_name_relationship"] == relationship
    )
    if not accepted:
        assert bound["reason"] == "search_header_name_disagreement"


@pytest.mark.parametrize(
    "nysed_fields",
    [
        {"name": "EXAMPLE ALEX MARTHA"},
        {"name": "ALEX EXAMPLE MORGAN"},
        {"name": "EXAMPLE ALEX MORGAN JR"},
        {"license_number": "123456"},
    ],
)
async def test_corroborating_record_must_match(source_session, monkeypatch, tmp_path, offline_binding, nysed_fields):
    paired = await _paired_case(source_session, monkeypatch, tmp_path, nysed_fields=nysed_fields)
    bound = _corroborated_bind(paired, tmp_path)
    assert bound["outcome"] == "held" and bound["reason"] == "nysed_identity_conflict"


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [_candidate(license_number="123456")],
        [_candidate(license_number="060654321")],
        [_candidate(license_number=" 654321 ")],
    ],
)
async def test_corroboration_requires_exact_literal_root(paired_case, tmp_path, rows):
    bound = _corroborated_bind(paired_case, tmp_path, rows)
    assert bound["outcome"] == "held" and bound["reason"] == "no_exact_license_candidates"


@pytest.mark.parametrize("field", ["nysed_receipt_sha256", "manifest_sha256", "acquisition_sha256"])
async def test_corroborated_replay_requires_external_pins(paired_case, tmp_path, field):
    invalid_by_field = {**paired_case, field: "0" * 64}
    with pytest.raises(ValueError):
        _corroborated_bind(invalid_by_field, tmp_path)


@pytest.mark.parametrize("artifact", ["manifest.json", "request.json", "response.json", "result.json"])
async def test_corroboration_rejects_changed_nysed_evidence(paired_case, tmp_path, artifact):
    path = paired_case["nysed_destination"] / artifact
    content_by_field = json.loads(path.read_bytes())
    content_by_field["unexpected"] = True
    path.write_bytes(encoded_json(content_by_field))
    with pytest.raises(ValueError):
        _corroborated_bind(paired_case, tmp_path)


@pytest.mark.parametrize(
    "mutation",
    [
        {"all_rows_received": False},
        {"coverage_scope": "selected_physicians"},
        {"connection_closed": False},
        {"expected_source_row_count": 2},
    ],
)
async def test_corroboration_rejects_incomplete_registry(paired_case, tmp_path, mutation):
    snapshot_by_field = {**_snapshot(), **mutation}
    with pytest.raises(ValueError):
        binding.bind_corroborated_acquisition(**paired_case, **_save_snapshot(tmp_path, snapshot_by_field))


@pytest.mark.parametrize("change", ["profession", "incomplete"])
async def test_corroboration_replays_nysed_semantics(paired_case, tmp_path, change):
    directory = paired_case["nysed_destination"]
    response_by_field = json.loads((directory / "response.json").read_bytes())
    if change == "profession":
        body_by_field = json.loads(base64.b64decode(response_by_field["body_base64"]))
        body_by_field["professionCode"] = "040"
        body_by_field["profession"]["value"] = "Pharmacy (040)"
        body = encoded_json(body_by_field)
        response_by_field.update(
            body_base64=base64.b64encode(body).decode("ascii"),
            received_bytes=len(body),
            content_sha256=hashlib.sha256(body).hexdigest(),
        )
    else:
        response_by_field["complete"] = False
    (directory / "response.json").write_bytes(encoded_json(response_by_field))
    receipt_by_field = json.loads((directory / "result.json").read_bytes())
    receipt_by_field["response_sha256"] = nysed._hash(response_by_field)
    (directory / "result.json").write_bytes(encoded_json(receipt_by_field))
    repinned_by_field = {**paired_case, "nysed_receipt_sha256": nysed._hash(receipt_by_field)}
    with pytest.raises(ValueError, match="new_york_nysed_(identity_mismatch|response_incomplete)"):
        _corroborated_bind(repinned_by_field, tmp_path)


async def test_source_id_disagreement_is_held(paired_case, tmp_path, monkeypatch):
    original_replay = binding._validated_acquisition

    def changed_source_id(*args):
        acquired = original_replay(*args)
        acquired["source_record"]["match_evidence"]["license_search"]["raw_identity"]["physicianID"] = "91002"
        return acquired

    monkeypatch.setattr(binding, "_validated_acquisition", changed_source_id)
    bound = _corroborated_bind(paired_case, tmp_path)
    assert bound["outcome"] == "held" and bound["reason"] == "search_header_name_disagreement"


async def test_blank_middle_keeps_strict_default(source_session, monkeypatch, tmp_path, offline_binding):
    paired = await _paired_case(
        source_session,
        monkeypatch,
        tmp_path,
        source_names={"middleName": ""},
        search_names={"physicianFirstName": "Alex", "physicianLastName": "Example"},
    )
    strict_by_field = {key: setting for key, setting in paired.items() if not key.startswith("nysed_")}
    strict = binding.bind_retained_acquisition(**strict_by_field, **_save_snapshot(tmp_path, _snapshot()))
    explicit = _corroborated_bind(paired, tmp_path)
    assert strict["outcome"] == explicit["outcome"] == "accepted"
    assert strict["source_record"]["match_evidence"]["registry_binding"]["method"] == "exact_ny_license_name_components"
    assert explicit["source_record"]["match_evidence"]["registry_binding"]["registry_middle_name_relationships"] == [
        "equal"
    ]


@pytest.fixture(params=["strict", "corroborated"])
async def indexed_case(request, source_session, monkeypatch, tmp_path, offline_binding):
    if request.param == "strict":
        paired = await _paired_case(
            source_session,
            monkeypatch,
            tmp_path,
            source_names={"middleName": ""},
            search_names={"physicianFirstName": "Alex", "physicianLastName": "Example"},
        )
        return "bind_retained_acquisition", {key: item for key, item in paired.items() if not key.startswith("nysed_")}
    return "bind_corroborated_acquisition", await _paired_case(source_session, monkeypatch, tmp_path)


@pytest.mark.parametrize(
    "candidates",
    [
        [],
        [_candidate(), _candidate(), _candidate(taxonomy_occurrence_checksum=18)],
        [_candidate(), _candidate(taxonomy=None)],
        [_candidate(), {"license_number": "654321"}],
        [_candidate(), _candidate(npi=1000000012, joined_npi=1000000012)],
        [_candidate(), _candidate(middle_name="M")],
        [_candidate(), _candidate(last_name="Different")],
        [_candidate(), _candidate(entity_type_code=2)],
        [_candidate(), _candidate(taxonomy_grouping="Nursing Service Providers")],
        [_candidate(license_number=literal) for literal in (None, "", "060654321", " 654321 ", "123456")],
    ],
)
async def test_loaded_snapshot_preserves_one_shot_decisions(indexed_case, tmp_path, monkeypatch, candidates):
    method, options = indexed_case
    saved = _save_snapshot(tmp_path, _snapshot(candidates))
    expected = getattr(binding, method)(**options, **saved)
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    saved["snapshot_path"].unlink()

    def no_reread(*_args, **_kwargs):
        raise AssertionError("Loaded binding must not reread or validate the registry again")

    monkeypatch.setattr(binding, "read_registry_snapshot", no_reread)
    for _ in range(3):
        assert getattr(loaded, method)(**options) == expected
    exact_candidates = [candidate for candidate in candidates if candidate["license_number"] == "654321"]
    assert expected["source_record"]["match_evidence"]["registry_binding"]["candidate_rows"] == exact_candidates


async def test_loaded_snapshot_result_mutation_cannot_change_later_decisions(indexed_case, tmp_path):
    method, options = indexed_case
    rows = [_candidate(), _candidate(taxonomy={"reported": ["unexpected"]})]
    saved = _save_snapshot(tmp_path, _snapshot(rows))
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    first = getattr(loaded, method)(**options)
    expected = copy.deepcopy(first)
    assert first["outcome"] == "held" and first["reason"] == "registry_identity_conflict"
    decision = first["source_record"]["match_evidence"]["registry_binding"]
    decision["candidate_rows"][1]["taxonomy"]["reported"].clear()
    decision["candidate_rows"].clear()
    decision["snapshot_sha256"] = "0" * 64
    first["facts"][0]["npi"] = 1000000012
    first["source_record"]["raw_payload"].clear()
    assert getattr(loaded, method)(**options) == expected
    with pytest.raises(AttributeError):
        loaded._snapshot_sha256 = "0" * 64
    with pytest.raises(AttributeError):
        loaded._rows_by_license = {}


@pytest.mark.parametrize("artifact", [name for name, _ in binding.ACQUISITION_FILES])
async def test_loaded_snapshot_revalidates_every_acquisition(indexed_case, tmp_path, artifact):
    method, options = indexed_case
    saved = _save_snapshot(tmp_path, _snapshot())
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    expected = getattr(loaded, method)(**options)
    path = options["destination"] / artifact
    original = path.read_bytes()
    path.write_bytes(original + b"\n")
    with pytest.raises(ValueError, match="acquisition_changed"):
        getattr(loaded, method)(**options)
    path.write_bytes(original)
    assert getattr(loaded, method)(**options) == expected


@pytest.mark.parametrize("artifact", ["manifest.json", "request.json", "response.json", "result.json"])
async def test_loaded_snapshot_revalidates_nysed_evidence(paired_case, tmp_path, artifact):
    saved = _save_snapshot(tmp_path, _snapshot())
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    assert loaded.bind_corroborated_acquisition(**paired_case)["outcome"] == "accepted"
    path = paired_case["nysed_destination"] / artifact
    changed_by_field = json.loads(path.read_bytes())
    changed_by_field["unexpected"] = True
    path.write_bytes(encoded_json(changed_by_field))
    with pytest.raises(ValueError):
        loaded.bind_corroborated_acquisition(**paired_case)


@pytest.mark.parametrize(
    "changes",
    [{"all_rows_received": False}, {"row_count": 2}, {"registry_rows": [{"license_number": []}]}],
)
def test_loaded_snapshot_requires_full_pinned_validation(tmp_path, changes):
    saved = _save_snapshot(tmp_path, {**_snapshot(), **changes})
    with pytest.raises(ValueError, match="snapshot_(incomplete|count_changed|rows_invalid)"):
        binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])


async def test_loaded_snapshot_keeps_original_capture_when_file_changes(retained_case, tmp_path):
    session, manifest_sha256, acquisition_sha256 = retained_case
    options_by_field = {
        "destination": session.destination,
        "manifest_sha256": manifest_sha256,
        "acquisition_sha256": acquisition_sha256,
    }
    saved = _save_snapshot(tmp_path, _snapshot())
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    expected = loaded.bind_retained_acquisition(**options_by_field)
    saved["snapshot_path"].write_bytes(encoded_json(_snapshot([_candidate(entity_type_code=2)])))
    with pytest.raises(ValueError, match="snapshot_changed"):
        binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    assert loaded.bind_retained_acquisition(**options_by_field) == expected
    assert (
        expected["source_record"]["match_evidence"]["registry_binding"]["snapshot_sha256"] == saved["snapshot_sha256"]
    )


async def test_one_snapshot_binds_different_literal_roots(source_session, tmp_path, monkeypatch, offline_binding):
    cases = []
    for license_number, first_name in [("654321", "Alex"), ("000123", "Jordan")]:
        session = source_session(
            SourceResponse(_search(physicianFirstName=first_name)),
            SourceResponse(_education(license_number, firstName=first_name, nationalProviderId="")),
        )
        session.destination = tmp_path / license_number
        await _acquire(session, license_number)
        _, manifest_sha256, acquisition_sha256 = _pinned_case(session)
        cases.append(
            {
                "destination": session.destination,
                "manifest_sha256": manifest_sha256,
                "acquisition_sha256": acquisition_sha256,
            }
        )
    registry_candidates = [
        _candidate(),
        _candidate(license_number="000123", first_name="Jordan", npi=1000000012, joined_npi=1000000012),
    ]
    saved = _save_snapshot(tmp_path, _snapshot(registry_candidates))
    reads = []
    original_reader = binding.read_registry_snapshot

    def counted_read(*args, **kwargs):
        reads.append(True)
        return original_reader(*args, **kwargs)

    monkeypatch.setattr(binding, "read_registry_snapshot", counted_read)
    loaded = binding.RegistrySnapshot(saved["snapshot_path"], snapshot_sha256=saved["snapshot_sha256"])
    outcomes = [loaded.bind_retained_acquisition(**case) for case in cases]
    assert [outcome["source_record"]["matched_npi"] for outcome in outcomes] == [1000000004, 1000000012]
    assert [
        outcome["source_record"]["match_evidence"]["registry_binding"]["candidate_rows"] for outcome in outcomes
    ] == [[candidate] for candidate in registry_candidates]
    assert reads == [True]
