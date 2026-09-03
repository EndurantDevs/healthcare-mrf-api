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
