# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared-ID hospital registry behavior."""

from process import hospital_hpt_locator as locator
from process import hospital_hpt_registry as registry


AVERA_LOCATOR = "https://www.avera.org/cms-hpt.txt"
AVERA_LOCATION_NAMES = (
    "Avera Behavioral Health Hospital",
    "Avera Creighton Hospital",
    "Avera De Smet Memorial Hospital",
    "Avera Dells Area Hospital",
    "Avera Eureka Health Care Center",
    "Avera Flandreau Hospital",
    "Avera Granite Falls Health Center",
    "Avera Gregory Hospital",
    "Avera Hand County Memorial Hospital",
    "Avera Heart Hospital of South Dakota",
    "Avera Holy Family Hospital",
    "Avera Marshall Regional Medical Center",
    "Avera McKennan Hospital and University Health Center",
    "Avera Merrill Pioneer Hospital",
    "Avera Missouri River Health Center",
    "Avera Queen of Peace Hospital",
    "Avera Sacred Heart Hospital",
    "Avera Specialty Hospital",
    "Avera St. Anthony's Hospital",
    "Avera St. Benedict Health Center",
    "Avera St. Luke's Hospital",
    "Avera St. Mary's Hospital",
    "Avera Tyler Hospital",
    "Avera Weskota Memorial Hospital",
    "Bowdle Healthcare Center Avera",
    "Community Memorial Hospital Avera",
    "Floyd Valley Healthcare",
    "Freeman Regional Health Services",
    "Hegg Health Center Avera",
    "Lakes Regional Healthcare",
    "Landmann-Jungman Memorial Hospital Avera",
    "Marshall County Healthcare Center Avera",
    "Milbank Area Health Care Campus",
    "Osceola Regional Health Center",
    "Pipestone County Medical Center",
    "Platte Health Center Avera",
    "Regional Health Services of Howard County",
    "Sioux Center Health Avera",
    "St. Michael's Hospital Avera",
    "Wagner Community Memorial Hospital Avera",
)


def test_shared_ids_expand_as_one_canonical_group(tmp_path, monkeypatch):
    locator = "http://hospital.example:8080/nonstandard/path?view=current"
    path = tmp_path / "registry.yaml"
    path.write_text(
        f"""version: 1
hospitals:
  - hospital_ids:
      - hospital-000001
      - hospital-000002
    name: Example Hospital
    cms_hpt_url: {locator}
""",
        encoding="utf-8",
    )
    hospitals = registry._load_hospital_hpt_registry_path(path)

    assert {(entry["cms_hpt_url"], entry["name"]) for entry in hospitals} == {
        (locator, "Example Hospital")
    }
    assert [entry.get("alias_of") for entry in hospitals] == [None, "hospital-000001"]
    monkeypatch.setattr(registry, "load_hospital_hpt_registry", lambda: hospitals)
    assert registry.hospital_hpt_registry_groups() == (hospitals,)
    assert registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-000002"}
    ) == hospitals


def test_reviewed_duplicate_aliases_preserve_selector_identity():
    hospital_by_id = {
        row["hospital_id"]: row for row in registry.load_hospital_hpt_registry()
    }
    for alias, canonical in (
        ("hospital-002614", "hospital-002613"),
        ("hospital-002615", "hospital-002616"),
        ("hospital-002618", "hospital-002617"),
    ):
        assert hospital_by_id[alias]["locator_mrf_url"] == hospital_by_id[canonical][
            "locator_mrf_url"
        ]
    keller_mrf_url = (
        "https://mrfs.hyvehealthcare.com/Emerus/"
        "364755936_EBD-BEMC-Burleson-LLC_standardcharges.csv"
    )
    assert hospital_by_id["hospital-000763"]["locator_mrf_url"] == keller_mrf_url
    assert hospital_by_id["hospital-000767"]["locator_mrf_url"] == keller_mrf_url


def test_rice_legal_name_alias_preserves_ids_and_locator_closure():
    """Resolve Rice's legal-name alias without merging neighboring facilities."""
    group = ("hospital-001033", "hospital-001032")
    locator_url = "https://ricemedicalcenter.net/cms-hpt.txt"
    records = (locator.HospitalHptLocatorRecord(
        "CAHRMC, dba Rice Medical Center", "https://hospital.example/rice.csv"
    ),)
    hospitals = registry.load_hospital_hpt_registry()
    cohort_hospitals = tuple(row for row in hospitals if row["cms_hpt_url"] == locator_url)
    assert {row["hospital_id"] for row in cohort_hospitals} == set(group)
    assert {row.get("alias_of", row["hospital_id"]) for row in cohort_hospitals} == {group[0]}
    assert [row["name"] for row in cohort_hospitals] == ["CAHRMC", "CAHRMC, dba Rice Medical Center"]
    assert all("locator_mrf_url" not in row for row in cohort_hospitals)
    for hospital_id in group:
        assert registry.hospital_hpt_group_ids(hospital_id) == group
        selected = registry.selected_hospital_hpt_registry({"hospital_id": hospital_id})
        assert selected == cohort_hospitals
        match = locator.match_hospital_hpt_locator(selected, locator_url, records)
        assert [(item.hospital_id, item.record_index) for item in match.bindings] == [
            ("hospital-001032", 0), ("hospital-001033", 0),
        ]
        assert match.content_targets == (records[0].mrf_url,)
        assert not match.unmatched_hospital_ids and not match.ambiguous_hospital_ids
        assert not match.unmatched_record_indexes and not match.ambiguous_record_indexes
    for hospital_id in ("hospital-001031", "hospital-001034"):
        assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)


def test_ohio_valley_alias_preserves_ids_and_locator_closure():
    """Group the reviewed Ohio identity without merging neighboring hospitals."""
    group = ("hospital-004819", "hospital-004818")
    locator_url = "https://www.ovsurgical.com/cms-hpt.txt"
    locator_records = (locator.HospitalHptLocatorRecord(
        "OHIO VALLEY SURGICAL HOSPITAL", "https://hospital.example/ohio.csv"
    ),)
    hospitals = registry.load_hospital_hpt_registry()
    cohort_hospitals = tuple(hospital for hospital in hospitals if hospital["cms_hpt_url"] == locator_url)
    assert {hospital["hospital_id"] for hospital in cohort_hospitals} == set(group)
    assert [hospital.get("alias_of") for hospital in cohort_hospitals] == [group[0], None]
    assert [hospital["name"] for hospital in cohort_hospitals] == [
        "Ohio Valley Medical Center", "OHIO VALLEY SURGICAL HOSPITAL",
    ]
    assert cohort_hospitals[0]["locator_name"] == "OHIO VALLEY SURGICAL HOSPITAL"
    assert all("locator_mrf_url" not in hospital and "fallback_mrf_url" not in hospital for hospital in cohort_hospitals)
    for hospital_id in group:
        assert registry.hospital_hpt_group_ids(hospital_id) == group
        selected = registry.selected_hospital_hpt_registry({"hospital_id": hospital_id})
        assert selected == cohort_hospitals
        match = locator.match_hospital_hpt_locator(selected, locator_url, locator_records)
        assert [(binding.hospital_id, binding.record_index) for binding in match.bindings] == [
            ("hospital-004818", 0), ("hospital-004819", 0),
        ]
        assert match.content_targets == (locator_records[0].mrf_url,)
        assert not match.unmatched_hospital_ids
        assert not match.ambiguous_hospital_ids
        assert not match.unmatched_record_indexes
        assert not match.ambiguous_record_indexes
    for hospital_id in ("hospital-004817", "hospital-004820"):
        assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)


def test_xavier_alias_preserves_rsfh_hospital_boundaries():
    """Bind the reviewed former name without combining other system hospitals."""
    locator_url = "https://www.rsfh.com/cms-hpt.txt"
    groups = (
        ("hospital-000934", "hospital-006029"),
        ("hospital-005491",),
        ("hospital-005492",),
        ("hospital-005493", "hospital-004361"),
    )
    names = (
        "Bon Secours St Francis Hospital", "Roper Hospital",
        "Roper St Francis Berkeley Hospital", "Roper St Francis Mt Pleasant",
    )
    hospitals = tuple(hospital for hospital in registry.load_hospital_hpt_registry()
                      if hospital["cms_hpt_url"] == locator_url)
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    assert set(hospital_by_id) == {hospital_id for group in groups for hospital_id in group}
    assert hospital_by_id["hospital-006029"] == {
        "hospital_id": "hospital-006029", "name": "St Francis Xavier",
        "cms_hpt_url": locator_url, "alias_of": "hospital-000934", "locator_name": names[0],
    }
    locator_records = tuple(locator.HospitalHptLocatorRecord(name, f"https://files.example/{index}.csv")
                            for index, name in enumerate(names))
    match = locator.match_hospital_hpt_locator(hospitals, locator_url, locator_records)
    binding_by_id = {binding.hospital_id: binding for binding in match.bindings}
    for index, group in enumerate(groups):
        for hospital_id in group:
            assert registry.hospital_hpt_group_ids(hospital_id) == group
            selected = registry.selected_hospital_hpt_registry({"hospital_id": hospital_id})
            assert {hospital["hospital_id"] for hospital in selected} == set(group)
            assert binding_by_id[hospital_id].record_index == index
    assert len(match.content_targets) == 4
    assert set(match.content_targets) == {locator_record.mrf_url for locator_record in locator_records}
    assert not match.unmatched_hospital_ids and not match.ambiguous_hospital_ids
    assert not match.unmatched_record_indexes and not match.ambiguous_record_indexes


def test_avera_shared_locator_closes_every_reviewed_id():
    hospitals = tuple(
        hospital for hospital in registry.load_hospital_hpt_registry()
        if hospital["cms_hpt_url"] == AVERA_LOCATOR
    )
    locator_records = tuple(
        locator.HospitalHptLocatorRecord(
            name, f"https://files.example/{index:02d}.csv"
        )
        for index, name in enumerate(AVERA_LOCATION_NAMES)
    )

    match_summary = locator.match_hospital_hpt_locator(
        hospitals, AVERA_LOCATOR, locator_records
    )

    assert len(hospitals) == 74
    assert len({
        hospital.get("alias_of", hospital["hospital_id"])
        for hospital in hospitals
    }) == 71
    assert len(locator_records) == len({
        locator_record.mrf_url for locator_record in locator_records
    }) == 40
    assert {binding.hospital_id for binding in match_summary.bindings} == {
        hospital["hospital_id"] for hospital in hospitals
    }
    assert len(match_summary.bindings) == 74
    assert set(match_summary.content_targets) == {
        locator_record.mrf_url for locator_record in locator_records
    }
    assert len(match_summary.content_targets) == 40
    assert match_summary.unmatched_hospital_ids == ()
    assert match_summary.ambiguous_hospital_ids == ()
    assert match_summary.unmatched_record_indexes == ()
    assert match_summary.ambiguous_record_indexes == ()
