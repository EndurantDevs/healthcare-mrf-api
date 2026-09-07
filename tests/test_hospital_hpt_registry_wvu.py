# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preserve reviewed WVU identities without grouping hospitals by system name."""

from copy import deepcopy
import datetime as dt

import pytest

from api import hospital_price_serving as serving
from api import hospital_price_status as status
from api.hospital_price_serving_sql import VERSION_SQL
from process import hospital_hpt_registry as registry
from process.hospital_hpt_locator import HospitalHptLocatorRecord
from process.hospital_price_acquisition import LocatorResult, candidates_from_locators
from tests.test_hospital_price_serving import _query, _Result, _Session, _version


WVU_LOCATOR = "https://wvumedicine.org/cms-hpt.txt"
REVIEWED_GROUPS = (
    ("hospital-007211", ("hospital-007116", "hospital-007117", "hospital-007211"), "West Virginia University Hospitals"),
    ("hospital-007212", ("hospital-002546", "hospital-002547", "hospital-007212"), "Harrison Community Hospital"),
    ("hospital-007215", ("hospital-005389", "hospital-005390", "hospital-007215"), "Reynolds Memorial Hospital"),
    ("hospital-007218", ("hospital-007131", "hospital-007132", "hospital-007218"), "Wetzel County Hospital"),
    ("hospital-007219", ("hospital-006740", "hospital-007219"), "United Hospital Center"),
    ("hospital-007220", ("hospital-007133", "hospital-007134", "hospital-007220"), "Wheeling Hospital"),
    ("hospital-007222", ("hospital-006737", "hospital-007222"), "Uniontown Hospital"),
    ("hospital-007225", ("hospital-002440", "hospital-002441", "hospital-007225"), "Grant Memorial Hospital"),
    ("hospital-007226", ("hospital-000967", "hospital-007226"), "Braxton County Memorial Hospital"),
    ("hospital-007227", ("hospital-003226", "hospital-007227"), "Jefferson Medical Center"),
    ("hospital-007228", ("hospital-007049", "hospital-007050", "hospital-007228"), "Weirton Medical Center"),
)
LEGAL_CLINICAL_PAIRS = (
    ("hospital-007209", "hospital-006189", "hospital-006184", "St. Joseph's Hospital"),
    ("hospital-007210", "hospital-001050", "hospital-001049", "Camden Clark Medical Center"),
    ("hospital-007213", "hospital-001524", "hospital-003182", "Jackson General Hospital"),
    ("hospital-007214", "hospital-001395", "hospital-000865", "Berkeley Medical Center"),
    ("hospital-007216", "hospital-005213", "hospital-005212", "Princeton Community Hospital"),
    ("hospital-007217", "hospital-005193", "hospital-005192", "Potomac Valley Hospital"),
    ("hospital-007221", "hospital-007115", "hospital-006348", "Summersville Regional Medical Center"),
    ("hospital-007223", "hospital-002898", "hospital-006552", "Thomas Hospitals"),
    ("hospital-007224", "hospital-000715", "hospital-000714", "Barnesville Hospital"),
)
SOURCE_GROUPS = REVIEWED_GROUPS + tuple(
    (alias, (legal_id, alias), name) for alias, legal_id, _clinical_id, name in LEGAL_CLINICAL_PAIRS
)
EXPECTED_GROUPS = tuple((alias, group) for alias, group, _name in SOURCE_GROUPS)
WVU_IDS = {f"hospital-{number:06d}" for number in range(7209, 7229)}
PUBLISHED_IDS = tuple(group[0] for _alias, group, _name in REVIEWED_GROUPS) + tuple(
    member_id for _hospital_id, legal_id, clinical_id, _name in LEGAL_CLINICAL_PAIRS
    for member_id in (legal_id, clinical_id)
)


@pytest.mark.parametrize("hospital_id,expected_group", EXPECTED_GROUPS)
def test_wvu_selection_preserves_exact_identity(hospital_id, expected_group):
    """Selecting any member reaches one reviewed identity, not the system group."""
    assert len(EXPECTED_GROUPS) == 20
    assert {raw_id for raw_id, _group in EXPECTED_GROUPS} == WVU_IDS
    for member_id in expected_group:
        assert registry.hospital_hpt_group_ids(member_id) == expected_group
        selected = registry.selected_hospital_hpt_registry({"hospital_id": member_id})
        assert {hospital["hospital_id"] for hospital in selected} == set(expected_group)
        assert {hospital["hospital_id"] for hospital in selected} & WVU_IDS == {hospital_id}
    hospital = next(hospital for hospital in selected if hospital["hospital_id"] == hospital_id)
    assert hospital["name"] == "WVU Medicine"
    assert hospital["cms_hpt_url"] == WVU_LOCATOR
    assert not {"locator_mrf_url", "fallback_mrf_url"} & hospital.keys()
    assert hospital["alias_of"] == expected_group[0]


@pytest.mark.parametrize("hospital_id,legal_id,clinical_id,_name", LEGAL_CLINICAL_PAIRS)
def test_wvu_legal_clinical_counterparts_remain_canonical(hospital_id, legal_id, clinical_id, _name):
    """Reconstruct the original legal row without merging its clinical counterpart."""
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in registry.load_hospital_hpt_registry()}
    assert all("alias_of" not in hospital_by_id[member_id] for member_id in (legal_id, clinical_id))
    assert registry.hospital_hpt_group_ids(legal_id) == (legal_id, hospital_id)
    assert registry.hospital_hpt_group_ids(clinical_id) == (clinical_id,)
    selected = registry.selected_hospital_hpt_registry({"hospital_id": clinical_id})
    assert tuple(hospital["hospital_id"] for hospital in selected) == (clinical_id,)


@pytest.mark.parametrize("record_case,record_index", [("bound", None), ("shared_url", None)] + [
    (record_case, index) for record_case in ("missing", "ambiguous") for index in range(20)
])
def test_wvu_candidates_require_exact_records(record_case, record_index):
    """Every source group needs its exact record, even when MRF URLs are shared."""
    hospitals = tuple(hospital for hospital in registry.load_hospital_hpt_registry()
                      if hospital["cms_hpt_url"] == WVU_LOCATOR)
    names = tuple(name for _alias, _group, name in SOURCE_GROUPS) + ("Garrett Regional Medical Center",)
    locator_records = tuple(HospitalHptLocatorRecord(name, f"https://files.example/{index}.csv")
                            for index, name in enumerate(names))
    if record_case == "missing":
        locator_records = tuple(locator_record for index, locator_record in enumerate(locator_records) if index != record_index)
    elif record_case == "ambiguous":
        locator_records += (HospitalHptLocatorRecord(names[record_index], "https://files.example/conflict.csv"),)
    elif record_case == "shared_url":
        locator_records = tuple(HospitalHptLocatorRecord(name, "https://files.example/shared.csv") for name in names)
    candidates = candidates_from_locators((LocatorResult(
        WVU_LOCATOR, "synthetic-locator", "synthetic-observation", hospitals, locator_records,
    ),))
    candidate_by_id = {candidate.hospital_id: candidate for candidate in candidates}
    assert len(hospitals) == len(candidate_by_id) == 58
    expected_failed_ids = set()
    if record_index is not None:
        expected_failed_ids.update(SOURCE_GROUPS[record_index][1])
        expected_failed_ids.update(clinical_id for _alias, _legal_id, clinical_id, name in LEGAL_CLINICAL_PAIRS
                                   if name == names[record_index])
    assert {candidate.hospital_id for candidate in candidates if candidate.initial_error_code} == expected_failed_ids
    for index, (alias, group, name) in enumerate(SOURCE_GROUPS):
        for member_id in group:
            candidate = candidate_by_id[member_id]
            if index == record_index:
                assert candidate.initial_error_code == ("locator_unmatched" if record_case == "missing" else "locator_ambiguous")
                assert candidate.source_url == WVU_LOCATOR and candidate.locator_name == name
            else:
                assert candidate.initial_error_code is None
                assert candidate.locator_name == name
                assert candidate.source_url == ("https://files.example/shared.csv" if record_case == "shared_url" else f"https://files.example/{index}.csv")
        assert candidate_by_id[alias].hospital_name == "WVU Medicine"


@pytest.mark.asyncio
@pytest.mark.parametrize("hospital_id,expected_group", EXPECTED_GROUPS)
@pytest.mark.parametrize("has_publication", (False, True))
async def test_wvu_serving_is_publication_scoped(monkeypatch, hospital_id, expected_group, has_publication):
    """Read only the exact group's publication, never another canonical's prices."""
    session = _Session()
    publications_by_id = {member_id: _version(version_id=f"{index:064x}")
                          for index, member_id in enumerate(PUBLISHED_IDS, 1)}
    if not has_publication:
        del publications_by_id[expected_group[0]]
    original_publications = deepcopy(publications_by_id)

    async def execute_scoped(statement, params=None):
        if statement is VERSION_SQL:
            session.statements.append((statement, params))
            return _Result(publications_by_id[member_id] for member_id in params["hospital_ids"]
                           if member_id in publications_by_id)
        return session._statement_result(statement, params)

    monkeypatch.setattr(session, "execute", execute_scoped)
    monkeypatch.setattr(serving, "_NATIVE", session.native)
    query = _query(hospital_id=hospital_id, payer_name=None, plan_name=None)
    if not has_publication:
        with pytest.raises(serving.HospitalPriceNotFoundError):
            await serving.read_hospital_price_page(session, query)
    else:
        page = await serving.read_hospital_price_page(session, query)
        assert page["hospital_id"] == hospital_id
        assert page["version"]["version_id"] == publications_by_id[expected_group[0]]["version_id"]
    assert next(params["hospital_ids"] for statement, params in session.statements
                if statement is VERSION_SQL) == expected_group
    assert publications_by_id == original_publications


@pytest.mark.parametrize("hospital_id,expected_group", EXPECTED_GROUPS)
def test_wvu_status_preserves_publication_history(hospital_id, expected_group):
    """A newer alias failure cannot erase publication or leak into other groups."""
    published_at = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    rows_by_id = {member_id: {"version_id": f"{index:064x}", "generation": 1,
                            "last_success_at": published_at, "facility_anchor_id": member_id}
                  for index, member_id in enumerate(PUBLISHED_IDS, 1)}
    rows_by_id[hospital_id] = {"attempt_id": "synthetic-failed-attempt", "attempt_status": "failed",
                               "started_at": published_at + dt.timedelta(days=1),
                               "error_code": "locator_unmatched"}
    original_rows = deepcopy(rows_by_id)
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in registry.load_hospital_hpt_registry()}
    group_hospitals = tuple(hospital_by_id[member_id] for member_id in registry.hospital_hpt_group_ids(hospital_id))
    item = status._status_item(group_hospitals, rows_by_id)
    assert item["hospital_id"] == expected_group[0]
    assert item["alias_hospital_ids"] == list(expected_group[1:])
    assert item["latest_attempt"]["error_code"] == "locator_unmatched"
    assert item["publication"]["version_id"] == rows_by_id[expected_group[0]]["version_id"]
    assert item["publication"]["generation"] == 1
    assert item["facility_anchor_id"] == expected_group[0]
    assert rows_by_id == original_rows
