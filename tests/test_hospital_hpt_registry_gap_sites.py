# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep newly registered physical sites distinct from shared price content."""

from process.hospital_hpt_locator import HospitalHptLocatorRecord, match_hospital_hpt_locator
from process.hospital_hpt_registry import load_hospital_hpt_registry, selected_hospital_hpt_registry


def test_publisher_fallback_preserves_existing_shared_source_identity():
    """An official replacement changes neither IDs nor existing locator names."""
    hospitals = selected_hospital_hpt_registry({"hospital_ids": ["hospital-001130", "hospital-001131"]})
    assert [row["hospital_id"] for row in hospitals] == ["hospital-001130", "hospital-001131"]
    assert {row["cms_hpt_url"] for row in hospitals} == {"https://www.carsontahoe.com/cms-hpt.txt"}
    assert {row.get("locator_name", row["name"]) for row in hospitals} == {"Carson Tahoe Regional Healthcare"}
    assert {row["fallback_mrf_url"] for row in hospitals} == {
        "https://apim.services.craneware.com/api-pricing-transparency/api/public/633d776663e0de8f00d1b829ca9ce867/charges/mrf",
    }
    assert all("alias_of" not in row and "locator_mrf_url" not in row for row in hospitals)


def test_reviewed_gap_sites_preserve_locator_closure_and_shared_content():
    """Two campuses bind separate records while downloading one shared MRF."""
    ids = ("hospital-007357", "hospital-007358", "hospital-007359")
    hospitals = selected_hospital_hpt_registry({"hospital_ids": list(ids)})
    assert tuple(row["hospital_id"] for row in hospitals) == ids
    assert tuple(row["name"] for row in hospitals) == (
        "College Hospital Costa Mesa",
        "El Camino Hospital (Mountain View)",
        "El Camino Hospital (Los Gatos)",
    )
    assert tuple(row["cms_hpt_url"] for row in hospitals) == (
        "https://chcm.us/cms-hpt.txt",
        "https://www.elcaminohealth.org/cms-hpt.txt",
        "https://www.elcaminohealth.org/cms-hpt.txt",
    )
    assert all("alias_of" not in row for row in hospitals)
    shared_url = "https://hospital.example/shared.json"
    records = tuple(HospitalHptLocatorRecord(name, shared_url) for name in (
        "El Camino Hospital (Los Gatos)", "El Camino Hospital (Mountain View)",
    ))
    match = match_hospital_hpt_locator(hospitals, hospitals[1]["cms_hpt_url"], records)
    assert [(binding.hospital_id, binding.record_index) for binding in match.bindings] == [
        (ids[1], 1), (ids[2], 0),
    ]
    assert match.content_targets == (shared_url,)
    assert not match.unmatched_hospital_ids
    assert not match.unmatched_record_indexes
    assert not match.ambiguous_hospital_ids


def test_reviewed_gap_hospitals_bind_their_own_locator_records():
    """Separate official domains keep independently identified hospitals distinct."""
    hospitals = selected_hospital_hpt_registry({
        "hospital_ids": ["hospital-007360", "hospital-007361", "hospital-007362"],
    })
    assert [(row["hospital_id"], row["name"], row["cms_hpt_url"]) for row in hospitals] == [
        ("hospital-007360", "Andalusia Health", "https://www.andalusiahealth.com/cms-hpt.txt"),
        ("hospital-007361", "Oroville Hospital", "https://www.orovillehospital.com/cms-hpt.txt"),
        ("hospital-007362", "Evergreen Medical Center", "https://www.evergreenmedical.org/cms-hpt.txt"),
    ]
    assert hospitals[2]["fallback_mrf_url"] == (
        "https://irp.cdn-website.com/1a9867e2/files/uploaded/"
        "208057151_evergreenmedicalcenter_standardcharges-8969ef83.csv"
    )
    for hospital in hospitals:
        assert "alias_of" not in hospital
        record = HospitalHptLocatorRecord(hospital["name"], "https://hospital.example/source.csv")
        match = match_hospital_hpt_locator(hospitals, hospital["cms_hpt_url"], (record,))
        assert [(binding.hospital_id, binding.record_index) for binding in match.bindings] == [
            (hospital["hospital_id"], 0),
        ]
        assert not match.unmatched_hospital_ids
        assert not match.unmatched_record_indexes
        assert not match.ambiguous_hospital_ids
        assert not match.ambiguous_record_indexes


def test_state_specific_locators_preserve_same_named_hospitals():
    """Separate state files must not redirect the unrelated published hospital."""
    hospitals = selected_hospital_hpt_registry({
        "hospital_ids": ["hospital-003393", "hospital-003394", "hospital-003395"],
    })
    assert [(row["hospital_id"], row["cms_hpt_url"]) for row in hospitals] == [
        ("hospital-003393", "https://www.kdmc.org/cms-hpt.txt"),
        ("hospital-003394", "https://search.hospitalpriceindex.com/7804/cms-hpt.txt"),
        ("hospital-003395", "https://search.hospitalpriceindex.com/7805/cms-hpt.txt"),
    ]
    assert all("alias_of" not in row and "locator_name" not in row for row in hospitals)
    for hospital in hospitals[1:]:
        record = HospitalHptLocatorRecord(hospital["name"], "https://hospital.example/source.csv")
        match = match_hospital_hpt_locator(hospitals, hospital["cms_hpt_url"], (record,))
        assert [(binding.hospital_id, binding.record_index) for binding in match.bindings] == [
            (hospital["hospital_id"], 0),
        ]
        assert not match.unmatched_hospital_ids
        assert not match.unmatched_record_indexes
        assert not match.ambiguous_hospital_ids
        assert not match.ambiguous_record_indexes


def test_new_hospital_locators_keep_duplicate_records_and_clinics_separate():
    """One reviewed selector resolves a duplicate name, without adding clinics."""
    hospitals = selected_hospital_hpt_registry({
        "hospital_ids": ["hospital-007363", "hospital-007364", "hospital-007365"],
    })
    helen, red_bay, hale = hospitals
    assert [hospital["name"] for hospital in hospitals] == [
        "Helen Keller Hospital", "Red Bay Hospital", "Hale County Hospital",
    ]
    assert all("alias_of" not in hospital and "fallback_mrf_url" not in hospital for hospital in hospitals)
    assert helen["cms_hpt_url"] == red_bay["cms_hpt_url"] == "https://hh.health/cms-hpt.txt"
    assert red_bay["locator_mrf_url"] == (
        "https://hh.health/wp-content/uploads/472323163_red-bay-hospital_standardcharges.csv"
    )
    hh_records = (
        HospitalHptLocatorRecord(helen["name"], "https://hospital.example/helen.csv"),
        HospitalHptLocatorRecord(red_bay["name"], "https://alternate.example/red.csv"),
        HospitalHptLocatorRecord(red_bay["name"], red_bay["locator_mrf_url"]),
    )
    hh_match = match_hospital_hpt_locator(hospitals, helen["cms_hpt_url"], hh_records)
    assert [(binding.hospital_id, binding.record_index) for binding in hh_match.bindings] == [
        (helen["hospital_id"], 0), (red_bay["hospital_id"], 2),
    ]
    assert not hh_match.ambiguous_hospital_ids and hh_match.unmatched_record_indexes == (1,)
    assert hale["cms_hpt_url"] == "https://www.halecountyhospital.com/cms-hpt.txt"
    hale_records = tuple(HospitalHptLocatorRecord(name, "https://hospital.example/shared.csv")
        for name in (hale["name"], "Hale County Hospital Clinic", "Moundville Medical Associates"))
    hale_match = match_hospital_hpt_locator(hospitals, hale["cms_hpt_url"], hale_records)
    assert [(binding.hospital_id, binding.record_index) for binding in hale_match.bindings] == [(hale["hospital_id"], 0)]
    assert hale_match.unmatched_record_indexes == (1, 2)
    assert hale_match.content_targets == ("https://hospital.example/shared.csv",)
    registered = load_hospital_hpt_registry()
    assert [hospital for hospital in registered if hospital["cms_hpt_url"] == helen["cms_hpt_url"]] == [helen, red_bay]
    assert [hospital for hospital in registered if hospital["cms_hpt_url"] == hale["cms_hpt_url"]] == [hale]
