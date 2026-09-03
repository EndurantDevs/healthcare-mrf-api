# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-bound Spring Valley specialty facility registry behavior."""

from process import hospital_hpt_locator as locator
from process import hospital_hpt_registry as registry


CMS_HPT_URL = "https://www.springvalleyhospital.com/cms-hpt.txt"
MRF_URL = (
    "https://uhsfilecdn.eskycity.net/ac/"
    "721549752_spring-valley-hospital-medical-center_standardcharges.csv"
)


def test_spring_valley_specialty_aliases_select_one_shared_mrf():
    hospitals = registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-006936"}
    )
    records = tuple(
        locator.HospitalHptLocatorRecord(name, MRF_URL)
        for name in (
            "Spring Valley Hospital Medical Center",
            "ER at Blue Diamond, an Extension of Spring Valley Hospital",
            "Spring Mountain Treatment Center, an Extension of Spring Valley Hospital",
            "Spring Mountain Sahara, an Extension of Spring Valley Hospital",
        )
    )

    result = locator.match_hospital_hpt_locator(hospitals, CMS_HPT_URL, records)

    assert tuple(binding.hospital_id for binding in result.bindings) == (
        "hospital-005975",
        "hospital-006936",
    )
    assert {hospital["locator_mrf_url"] for hospital in hospitals} == {MRF_URL}
    assert result.content_targets == (MRF_URL,)
    assert result.unmatched_hospital_ids == ()
