# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from process import hospital_hpt_registry as registry
from process.hospital_hpt_locator import HospitalHptLocatorRecord, match_hospital_hpt_locator
from tests.hospital_hpt_registry_fallbacks import (
    FALLBACK_URL_SHA256_BY_HOSPITAL_ID as _FALLBACK_URL_SHA256_BY_HOSPITAL_ID,
)
from tests.hospital_price_control_support import acquisition_module, store_module
_REVIEWED_ALIAS_SAMPLES = {
    f"hospital-{alias}": f"hospital-{canonical}"
    for alias, canonical in (pair.split(":") for pair in "000061:000060 000064:000063 000123:000122 000162:000161 001486:001483 002520:002519 004667:004666 005329:005328 005429:001811 005563:001678 005564:001678 005565:001678 006233:005566 007207:007206 007272:000586 000121:000120 000342:000343 000593:000592 000654:000604 000655:000600 000656:000592 000657:000606 000745:000744 002911:000189 005797:005798 005077:005063 006650:006649 003017:003012 003068:003013 003069:003014 003070:003015 003071:003016 003072:003019 003073:003018 003074:003020 003075:003021 003076:003022 003077:003023 003078:003024 003079:003025 002432:002433 006299:006300 005971:005970 005973:005972 005975:006936 001882:001881 005163:005162 003238:005914 002844:004555 006900:006899 000905:000904 001851:006405 006402:002912 006403:006404 006987:006406 007167:007168 000229:000231 000230:000232 000806:000807 001263:006172 001264:006171 001265:006173 001266:006174 001267:006175 001270:000805 001272:006200 001273:006207 001274:006208 001275:006209 001276:006203 001280:001277 001533:001535 002319:002318 002377:002378 006164:006161 006190:006191 006212:006205 006215:006201 006225:006204 006226:006206 006237:006234 006263:005494 006264:005641 006265:005787 006549:001253 007234:007237 007235:007236 000902:000901 003410:003409 004869:004870 005186:005187 005357:005358 006266:005582 006285:006284 006330:005919 006331:005920 006651:006652 003161:003159 003172:003160 001586:001587 001589:001590 001591:001592 001593:001594 001598:001596 001599:001597 001600:001612 001601:001602 001603:001604 001606:001607 001608:001609 001610:001611 001613:001614 001615:001616 000514:000511 004534:000825 004535:000826 001433:001435 001434:006080 006396:003096 006397:006395 000191:000190 000556:001528 000557:000558 000571:004809 000575:000577 000915:000916 001432:001416 002159:002158 004470:004236 004471:004237 004472:004238 005459:005427 006362:006363 006041:005093 000957:000956 000369:004907 000791:000790 001451:001452 001198:007129 001200:001773 001202:002924 001203:005479 001204:006589 005113:005116 005471:005469 006390:006573 006484:001013 006595:006613 006597:006598 006611:006608 006617:006614 006694:006693 006747:006538 007006:007005 007177:006691 006865:006864 006870:006869 000827:004573 000828:003656 001025:001024 004155:004156 005819:005820 006477:006475 004316:004315 004988:004987 003246:003245 003970:003969 006564:006565 006815:006692 001078:007241 004517:007241 007046:007241 007242:007241 007243:007241 007244:007241 007245:007241 007246:007241 007247:007241 007248:007241 007249:007241 007250:007241 007251:007241".split())
} | {f"hospital-{alias}": f"hospital-{canonical}" for alias, canonical in (pair.split(":") for pair in "000030:002931 005457:002930 006332:002934 006354:002932 006639:002933 006738:006688 000767:000763 001835:001836 001837:001834 002614:002613 002615:002616 002618:002617 005625:005624 004759:004756 004769:004747 004770:004760".split())}
_REVIEWED_LOCATOR_NAMES = {
    "hospital-000047": "Adair County Memorial Hospital",
    "hospital-000126": "HANFORD COMMUNITY HOSPITAL",
    "hospital-000188": "Amberwell Atchison Association",
    "hospital-000207": "UTMB Health Angleton Danbury Hospital",
    "hospital-000342": "Ashland Health Center",
    "hospital-000600": "Baptist Health Hardin",
    "hospital-000833": "Beckett Springs",
    **dict(pair.split(":", 1) for pair in "hospital-000685:39-3714515 BMH Oktibbeha County|hospital-000825:New Orleans Hospital|hospital-000826:New Orleans Westbank Hospital".split("|")),
    "hospital-001199": "Cottonwood Springs",
    "hospital-001415": "UTMB Health Clear Lake Hospital",
    "hospital-001458": "North Central Kansas Medical Center",
    **dict(pair.split(":", 1) for pair in "hospital-001587:Corewell Health Big Rapids|hospital-001590:Corewell Health Gerber|hospital-001592:Corewell Health Greenville|hospital-001594:Corewell Health Gross Pointe|hospital-001596:Corewell Health Lakeland Niles|hospital-001597:Corewell Health Lakeland St. Joseph|hospital-001602:Corewell Health Ludington|hospital-001604:Corewell Health Reed City|hospital-001607:Corewell Health Taylor|hospital-001609:Corewell Health Trenton|hospital-001611:Corewell Health Troy|hospital-001612:Corewell Health Lakeland Watervliet|hospital-001614:Corewell Health Wayne|hospital-001616:Corewell Health Zeeland".split("|")),
    "hospital-001880": "Edgerton Hospital and Health Services - Fulton Square Clinic",
    "hospital-001881": "Edgerton Hospital and Health Services - Milton Clinic",
    "hospital-002260": "Franciscan Health Orthopedic-Carmel",
    "hospital-002332": "Garfield County Hospital District",
    "hospital-002421": "Grady Health System",
    "hospital-002914": "Highland-Clarksburg Hospital, Inc.",
    "hospital-003007": "Memorial Health System Abilene",
    "hospital-003238": "Southern Humboldt Community Hospital",
    "hospital-003145": "Intermountain Health Good Samaritan Medical Center",
    "hospital-003148": "Holy Rosary Healthcare",
    "hospital-003157": "Platte Valley Medical Center",
    "hospital-003163": "Saint Joseph Hospital",
    "hospital-003168": "St. James Healthcare",
    "hospital-003169": "St. Mary's Medical Center",
    "hospital-003170": "St. Vincent Healthcare",
    "hospital-003240": "Jersey Community Hospital",
    "hospital-003517": "UTMB Health League City Hospital",
    "hospital-003587": "Lindsborg Community Hospital",
    "hospital-003588": "Lindsborg Community Hospital",
    "hospital-003592": "Little River Medical Center, INC DBA Little River Memorial Hospital",
    "hospital-005162": "Pioneer Memorial Hospital & Health Services",
    "hospital-005086": "Philadelphia Post-Acute Partners LLC",
    "hospital-005821": "Slidell Memorial Hospital - Main Campus",
    "hospital-005304": "Ramapo Ridge Behavioral Health",
    "hospital-005915": "Mee Memorial Hospital",
    "hospital-006345": "Summa Rehab Hospital, LLC",
    "hospital-003109": "Mesa Springs",
    "hospital-003110": "Mesa Springs Changes",
    "hospital-004749": "Ochsner Behavioral Health Acadiana - Broussard",
    "hospital-006677": "UCSF Parnassus",
    "hospital-006918": "USMD Hospital at Arlington LLC",
}
_NORTHSHORE_ALIAS_GROUPS = (
    ("hospital-002062", "hospital-002163", "Evanston Hospital"),
    ("hospital-002063", "hospital-002375", "Glenbrook Hospital"),
    ("hospital-002064", "hospital-002922", "Highland Park Hospital"),
    ("hospital-002067", "hospital-005814", "Skokie Hospital"),
)

def _load(tmp_path: Path, text: str) -> tuple[dict[str, str], ...]:
    path = tmp_path / "registry.yaml"
    path.write_text(text, encoding="utf-8")
    return registry._load_hospital_hpt_registry_path(path)


def _document(locator: str = "https://hospital.example/cms-hpt.txt") -> str:
    return f"""\
version: 1
hospitals:
  - hospital_id: hospital-000001
    name: Example Hospital
    cms_hpt_url: {locator}
"""


def test_checked_in_registry_has_exact_source_neutral_shape():
    """Keep the checked-in registry shape and reviewed counts stable."""
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    assert len(hospitals) == registry.EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT
    assert len(registry.hospital_hpt_registry_groups()) == 6_900
    assert len({entry["hospital_id"] for entry in hospitals}) == len(hospitals)
    assert sum("locator_name" in entry for entry in hospitals) == 1_713
    assert sum("locator_mrf_url" in entry for entry in hospitals) == 683
    assert sum("fallback_mrf_url" in entry for entry in hospitals) == 137
    assert "alias_of" not in hospital_by_id["hospital-001271"]
    assert hospital_by_id["hospital-001271"]["locator_mrf_url"] == (
        "https://www.commonspirit.org/content/dam/commonspiritorg/en/bslmc/soho/"
        "finance/price-transparency/741161938-1184622847_chi-st-lukes-health-"
        "baylor-college-of-medicine-medical-center_standardcharges.json"
    )
    assert {entry["hospital_id"] for entry in hospitals if "fallback_mrf_url" in entry} == set(
        _FALLBACK_URL_SHA256_BY_HOSPITAL_ID
    )
    assert {
        hospital_id: hospital_by_id[hospital_id]["locator_name"]
        for hospital_id in _REVIEWED_LOCATOR_NAMES
    } == _REVIEWED_LOCATOR_NAMES
    assert hospital_by_id["hospital-007141"]["name"] == "Stone County Medical Center"
    assert {
        hospital_id: hashlib.sha256(
            hospital_by_id[hospital_id]["fallback_mrf_url"].encode()
        ).hexdigest()
        for hospital_id in _FALLBACK_URL_SHA256_BY_HOSPITAL_ID
    } == _FALLBACK_URL_SHA256_BY_HOSPITAL_ID
    assert all(
        {"hospital_id", "name", "cms_hpt_url"} <= set(entry)
        <= {
            "alias_of", "fallback_mrf_url", "hospital_id", "name",
            "cms_hpt_url", "locator_name", "locator_mrf_url",
        }
        for entry in hospitals
    )


def test_reviewed_publisher_replacement_preserves_singleton_identity():
    """A reviewed file replaces a matched stale source without changing scope."""
    hospitals = registry.selected_hospital_hpt_registry({"hospital_id": "hospital-007229"})
    assert len(hospitals) == 1
    hospital = hospitals[0]
    assert set(hospital) == {"hospital_id", "name", "cms_hpt_url", "fallback_mrf_url"}
    assert (hospital["hospital_id"], hospital["name"], hospital["cms_hpt_url"]) == (
        "hospital-007229", "Wyandot Memorial Hospital",
        "https://www.wyandotmemorial.org/cms-hpt.txt",
    )
    assert tuple(row for row in registry.load_hospital_hpt_registry()
                 if row["cms_hpt_url"] == hospital["cms_hpt_url"]) == hospitals
    acquisition = acquisition_module()
    result = acquisition.LocatorResult(
        hospital["cms_hpt_url"], "synthetic-locator", "synthetic-observation", hospitals,
        (acquisition.HospitalHptLocatorRecord(hospital["name"], "https://files.example/previous.csv"),),
    )
    candidate, = acquisition.candidates_from_locators((result,))
    assert candidate.initial_error_code is None
    assert candidate.source_url == hospital["fallback_mrf_url"]
    assert (candidate.hospital_id, candidate.hospital_name, candidate.locator_name) == (
        hospital["hospital_id"], hospital["name"], hospital["name"],
    )
    assert candidate.locator_url == hospital["cms_hpt_url"]
    assert candidate.observation_id == "synthetic-observation"


@pytest.mark.parametrize("hospital_id,name,locator_url,location_names,ordinal", (
    ("hospital-002218", "First Care Health Center", "https://www.firstcarehc.com/cms-hpt.txt",
     ("First Care Health Center",), 0),
    ("hospital-004390", "Mountainview Medical Center", "https://www.mvmc.org/cms-hpt.txt",
     ("Mountainview Medical Center",), 0),
    ("hospital-005166", "Pioneers Medical Center", "https://www.pioneershospital.org/cms-hpt.txt",
     ("PIONEERS MEDICAL", "PIONEERS MEDICAL"), None),
    ("hospital-005866", "South Lyon Medical Center", "https://slmcnv.org/cms-hpt.txt",
     ("South Lyon Medical Center",), 0),
    ("hospital-006469", "Texas Institute for Surgery at Texas Health Presbyterian Dallas",
     "https://www.texasinstituteforsurgery.com/cms-hpt.txt",
     ("Texas Institute for Surgery at Texas Health Dallas",) * 2, None),
    ("hospital-007197", "Wood County Hospital", "https://www.woodcountyhospital.org/cms-hpt.txt",
     ("Wood County Hospital",), 0),
))
def test_reviewed_sources_preserve_location_binding(hospital_id, name, locator_url, location_names, ordinal):
    """Replace reviewed stale files without changing singleton or file-wide scope."""
    hospital, = registry.selected_hospital_hpt_registry({"hospital_id": hospital_id})
    assert set(hospital) == {"hospital_id", "name", "cms_hpt_url", "fallback_mrf_url"}
    assert (hospital["name"], hospital["cms_hpt_url"]) == (name, locator_url)
    assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)
    assert tuple(entry for entry in registry.load_hospital_hpt_registry()
                 if entry["cms_hpt_url"] == locator_url
                 or entry.get("fallback_mrf_url") == hospital["fallback_mrf_url"]) == (hospital,)
    acquisition = acquisition_module()
    candidate, = acquisition.candidates_from_locators((acquisition.LocatorResult(
        locator_url, "synthetic-locator", "synthetic-observation", (hospital,),
        (HospitalHptLocatorRecord(name, "https://files.example/previous.csv"),),
    ),))
    assert candidate.source_url == hospital["fallback_mrf_url"]
    assert candidate.initial_error_code is None
    assert (candidate.hospital_id, candidate.hospital_name, candidate.locator_name) == (hospital_id, name, name)
    assert (candidate.locator_url, candidate.observation_id) == (locator_url, "synthetic-observation")
    store, _native = store_module()
    locations = tuple(enumerate(location_names))
    assert store._location_ordinals((candidate,), locations) == {hospital_id: ordinal}


def test_unproven_timeout_keeps_no_reviewed_source():
    """An uncertain locator timeout does not authorize a reviewed source override."""
    hospital, = registry.selected_hospital_hpt_registry({"hospital_id": "hospital-003267"})
    assert hospital == {"hospital_id": "hospital-003267", "name": "Johnson County Hospital",
                        "cms_hpt_url": "https://jchosp.com/cms-hpt.txt"}
    acquisition = acquisition_module()
    candidate, = acquisition.candidates_from_locators((acquisition.LocatorResult(
        hospital["cms_hpt_url"], "synthetic-locator", "synthetic-observation", (hospital,),
        None, error_code="timeout", fetch_failed=False,
    ),))
    assert candidate.initial_error_code == "timeout"
    assert candidate.source_url == hospital["cms_hpt_url"]


def test_checked_in_registry_has_reviewed_cms_hpt_urls():
    """Keep source-proven locator changes separate from catalog shape checks."""
    hospital_by_id = {
        entry["hospital_id"]: entry for entry in registry.load_hospital_hpt_registry()
    }
    assert [hospital_by_id[hospital_id]["cms_hpt_url"] for hospital_id in (
        "hospital-000047", "hospital-000188", "hospital-000600", "hospital-002332",
        "hospital-005162", "hospital-005163", "hospital-006475",
        "hospital-006476", "hospital-006477", "hospital-007140",
        "hospital-007141",
        "hospital-001458", "hospital-003007", "hospital-003587", "hospital-003588",
        "hospital-003117", "hospital-003118", "hospital-003119",
        "hospital-003824",
    )] == [
        "https://www.achsiowa.org/cms-hpt.txt",
        "https://amberwellhealth.org/cms-hpt.txt",
        "https://www.baptisthealth.com/cms-hpt.txt",
        "https://www.garfieldcountyhospital.com/cms-hpt.txt",
        "https://www.pioneermemorial.org/cms-hpt.txt",
        "https://www.pioneermemorial.org/cms-hpt.txt",
        "https://scottishriteforchildren.org/cms-hpt.txt",
        "https://scottishriteforchildren.org/cms-hpt.txt",
        "https://scottishriteforchildren.org/cms-hpt.txt",
        "https://whiteriverhealth.org/cms-hpt.txt",
        "https://whiteriverhealth.org/cms-hpt.txt",
        "https://nckmed.com/cms-hpt.txt",
        "https://mhsks.org/cms-hpt.txt",
        "https://lindsborghospital.org/cms-hpt.txt",
        "https://lindsborghospital.org/cms-hpt.txt",
        "https://estimator.myinsightcare.com/cms-hpt.txt",
        "https://insightcoldwater.org/cms-hpt.txt",
        "https://insightsurgicalhospital.com/cms-hpt.txt",
        "https://www.massgeneralbrigham.org/cms-hpt.txt",
    ]


def test_mclean_shared_locator_keeps_fourteen_facilities_separate():
    """Correct one locator without aliasing its other hospital records."""
    locator_url = "https://www.massgeneralbrigham.org/cms-hpt.txt"
    hospitals = tuple(row for row in registry.load_hospital_hpt_registry()
                      if row["cms_hpt_url"] == locator_url)
    assert len(hospitals) == 14 and all("alias_of" not in row for row in hospitals)
    mclean = next(row for row in hospitals if row["hospital_id"] == "hospital-003824")
    assert mclean == {"hospital_id": "hospital-003824", "name": "McLean Hospital",
                      "cms_hpt_url": locator_url}
    records = tuple(HospitalHptLocatorRecord(
        row.get("locator_name", row["name"]),
        f"https://files.example/{row['hospital_id']}.zip",
    ) for row in hospitals)
    match = match_hospital_hpt_locator(hospitals, locator_url, records)
    assert len(match.bindings) == len(match.content_targets) == 14
    assert not match.unmatched_hospital_ids and not match.unmatched_record_indexes
    assert not match.ambiguous_hospital_ids and not match.ambiguous_record_indexes
    assert {binding.hospital_id: binding.mrf_url for binding in match.bindings} == {
        row["hospital_id"]: f"https://files.example/{row['hospital_id']}.zip"
        for row in hospitals
    }


def test_slidell_name_binding_preserves_distinct_campus_sources():
    """Bind the reviewed Main name without sharing East's source or identity."""
    hospitals = registry.selected_hospital_hpt_registry({"hospital_ids": [
        "hospital-005821", "hospital-005822", "hospital-005823",
    ]})
    records = (
        HospitalHptLocatorRecord(
            "Slidell Memorial Hospital - Main Campus", "https://hospital.example/main.csv"
        ),
        HospitalHptLocatorRecord(
            "Slidell Memorial Hospital- East Campus", "https://hospital.example/east.csv"
        ),
    )
    match = match_hospital_hpt_locator(hospitals, hospitals[0]["cms_hpt_url"], records)
    assert [(item.hospital_id, item.record_index) for item in match.bindings] == [
        ("hospital-005821", 0), ("hospital-005822", 0), ("hospital-005823", 1),
    ]
    assert match.content_targets == tuple(record.mrf_url for record in records)
    assert not match.unmatched_hospital_ids and not match.ambiguous_hospital_ids
    assert all(registry.hospital_hpt_group_ids(item["hospital_id"]) == (
        item["hospital_id"],
    ) for item in hospitals)


@pytest.mark.parametrize("record_case", ("matching", "unmatched", "ambiguous"))
def test_fulton_fallback_preserves_separate_hospital_sources(record_case):
    """Use the reviewed file for one exact facility without borrowing main-site prices."""
    acquisition = acquisition_module()
    fallback_url = "https://www.fultoncountyhospital.org/plugins/show_image.php?id=68"
    hospitals = registry.load_hospital_hpt_registry()
    fulton, = registry.selected_hospital_hpt_registry({"hospital_id": "hospital-000743"})
    assert fulton == {
        "hospital_id": "hospital-000743", "name": "Baxter Health Fulton County Hospital",
        "cms_hpt_url": "https://baxterregional.org/cms-hpt.txt", "fallback_mrf_url": fallback_url,
    }
    assert {hospital["hospital_id"] for hospital in hospitals if hospital.get("fallback_mrf_url") == fallback_url} == {fulton["hospital_id"]}
    main_hospitals = registry.selected_hospital_hpt_registry({"hospital_ids": [
        "hospital-000741", "hospital-000742",
    ]})
    for hospital in (fulton, *main_hospitals):
        assert registry.hospital_hpt_group_ids(hospital["hospital_id"]) == (hospital["hospital_id"],)
    record_names = (fulton["name"], fulton["name"]) if record_case == "ambiguous" else (
        "Baxter Health" if record_case == "unmatched" else fulton["name"],
    )
    candidates = acquisition.candidates_from_locators((
        acquisition.LocatorResult(fulton["cms_hpt_url"], "fulton-locator", "fulton-observation", (fulton,), tuple(
            HospitalHptLocatorRecord(name, f"https://files.example/stale-{index}.csv")
            for index, name in enumerate(record_names)
        )),
        acquisition.LocatorResult(main_hospitals[0]["cms_hpt_url"], "main-locator", "main-observation", main_hospitals,
                      (HospitalHptLocatorRecord("Baxter Health", "https://files.example/main.csv"),)),
    ))
    candidate_by_id = {candidate.hospital_id: candidate for candidate in candidates}
    assert set(candidate_by_id) == {"hospital-000741", "hospital-000742", "hospital-000743"}
    candidate = candidate_by_id[fulton["hospital_id"]]
    assert candidate.source_url == (fulton["cms_hpt_url"] if record_case == "ambiguous" else fallback_url)
    assert candidate.initial_error_code == ("locator_ambiguous" if record_case == "ambiguous" else None)
    assert candidate.locator_name == fulton["name"] and candidate.locator_url == fulton["cms_hpt_url"]
    for hospital in main_hospitals:
        assert "fallback_mrf_url" not in hospital
        candidate = candidate_by_id[hospital["hospital_id"]]
        assert candidate.source_url == "https://files.example/main.csv" and candidate.initial_error_code is None


@pytest.mark.parametrize("record_case", ("matching", "unmatched", "ambiguous"))
def test_reviewed_pair_preserves_shared_locator(record_case):
    """Replace two exact sources without changing their shared locator's other bindings."""
    acquisition = acquisition_module()
    locator_url = "https://www.kindredhospitals.com/cms-hpt.txt"
    hospitals = tuple(hospital for hospital in registry.load_hospital_hpt_registry()
                      if hospital["cms_hpt_url"] == locator_url)
    replacements = registry.selected_hospital_hpt_registry({"hospital_ids": [
        "hospital-004860", "hospital-004861",
    ]})
    replacement_by_id = {hospital["hospital_id"]: hospital for hospital in replacements}
    assert len(hospitals) == 53
    assert {hospital["hospital_id"] for hospital in hospitals if "fallback_mrf_url" in hospital} == set(replacement_by_id)
    assert len({hospital["fallback_mrf_url"] for hospital in replacements}) == 2
    for hospital in replacements:
        assert set(hospital) == {"hospital_id", "name", "cms_hpt_url", "fallback_mrf_url"}
        assert registry.hospital_hpt_group_ids(hospital["hospital_id"]) == (hospital["hospital_id"],)
    locator_records = tuple(HospitalHptLocatorRecord(
        hospital.get("locator_name", hospital["name"]),
        f"https://files.example/{hospital['hospital_id']}.json",
    ) for hospital in hospitals if hospital["hospital_id"] not in replacement_by_id)
    if record_case != "unmatched":
        locator_records += tuple(HospitalHptLocatorRecord(
            hospital["name"], f"https://files.example/stale-{index}.json",
        ) for hospital in replacements for index in range(2 if record_case == "ambiguous" else 1))
    previous_hospitals = tuple({field: field_value for field, field_value in hospital.items()
                               if field != "fallback_mrf_url"} for hospital in hospitals)
    candidate_snapshots = []
    for cohort in (previous_hospitals, hospitals):
        candidates = acquisition.candidates_from_locators((acquisition.LocatorResult(
            locator_url, "synthetic-locator", "synthetic-observation", cohort, locator_records,
        ),))
        candidate_snapshots.append({candidate.hospital_id: candidate for candidate in candidates})
    previous_by_id, candidate_by_id = candidate_snapshots
    assert set(candidate_by_id) == set(previous_by_id) == {hospital["hospital_id"] for hospital in hospitals}
    for hospital_id, candidate in candidate_by_id.items():
        if hospital_id not in replacement_by_id:
            assert candidate == previous_by_id[hospital_id]
            continue
        hospital = replacement_by_id[hospital_id]
        assert candidate.hospital_name == candidate.locator_name == hospital["name"]
        assert candidate.locator_url == locator_url and candidate.observation_id == "synthetic-observation"
        assert candidate.source_url == (locator_url if record_case == "ambiguous" else hospital["fallback_mrf_url"])
        assert candidate.initial_error_code == ("locator_ambiguous" if record_case == "ambiguous" else None)


@pytest.mark.parametrize("record_case", (
    "matching", "unmatched", "fetch_failed", "body_failed", "ambiguous",
))
def test_reviewed_singleton_preserves_location_scope(record_case):
    """A reviewed file keeps its exact hospital binding without assigning its clinic."""
    acquisition = acquisition_module()
    hospital, = registry.selected_hospital_hpt_registry({"hospital_id": "hospital-001409"})
    assert set(hospital) == {"hospital_id", "name", "cms_hpt_url", "fallback_mrf_url"}
    assert (hospital["name"], hospital["cms_hpt_url"]) == (
        "Clarke County Hospital", "https://clarkehosp.org/cms-hpt.txt",
    )
    assert registry.hospital_hpt_group_ids(hospital["hospital_id"]) == (hospital["hospital_id"],)
    assert tuple(entry for entry in registry.load_hospital_hpt_registry()
                 if entry["cms_hpt_url"] == hospital["cms_hpt_url"]
                 or entry.get("fallback_mrf_url") == hospital["fallback_mrf_url"]) == (hospital,)
    names = (hospital["name"], "Clarke County Clinic")
    if record_case == "unmatched":
        names = names[1:]
    elif record_case == "ambiguous":
        names = (hospital["name"], *names)
    has_failed = record_case in {"fetch_failed", "body_failed"}
    candidate, = acquisition.candidates_from_locators((acquisition.LocatorResult(
        hospital["cms_hpt_url"], "synthetic-locator", "synthetic-observation", (hospital,),
        None if has_failed else tuple(HospitalHptLocatorRecord(
            name, f"https://files.example/previous-{index}.csv",
        ) for index, name in enumerate(names)),
        error_code="clientresponse" if has_failed else None,
        fetch_failed=record_case == "fetch_failed",
    ),))
    expected_error = {"ambiguous": "locator_ambiguous", "body_failed": "clientresponse"}.get(record_case)
    assert candidate.initial_error_code == expected_error
    assert candidate.source_url == (hospital["cms_hpt_url"] if expected_error else hospital["fallback_mrf_url"])
    assert (candidate.hospital_id, candidate.hospital_name, candidate.locator_name) == (
        hospital["hospital_id"], hospital["name"], hospital["name"],
    )
    assert candidate.locator_url == hospital["cms_hpt_url"]
    assert candidate.observation_id == "synthetic-observation"
    if not expected_error:
        store, _native = store_module()
        assert store._location_ordinals((candidate,), (
            (0, hospital["name"]), (1, "Clarke County Clinic"),
        )) == {hospital["hospital_id"]: 0}
        assert store._location_ordinals((candidate,), (
            (1, "Clarke County Clinic"),
        )) == {hospital["hospital_id"]: None}


def test_checked_in_registry_has_reviewed_canonical_aliases():
    """Keep reviewed alias identities explicit while preserving every raw ID."""
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    aliases_by_id = {
        entry["hospital_id"]: entry["alias_of"]
        for entry in hospitals
        if "alias_of" in entry
    }
    assert len(aliases_by_id) == 465
    assert not {"hospital-000833", "hospital-001199", "hospital-006476"} & aliases_by_id.keys()
    assert {
        hospital_id: aliases_by_id[hospital_id]
        for hospital_id in _REVIEWED_ALIAS_SAMPLES
    } == _REVIEWED_ALIAS_SAMPLES
    assert hospital_by_id["hospital-000063"]["name"] == "Advanced Specialty Hospitals of Toledo"


def test_northshore_aliases_preserve_four_facilities():
    """Preserve old names and IDs without merging neighboring physical sites."""
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    locator_url = "https://www.endeavorhealth.org/cms-hpt.txt"
    canonical_ids = {"hospital-001877"} | {f"hospital-{number:06d}" for number in range(2060, 2069)}
    alias_ids = {alias for _canonical, alias, _name in _NORTHSHORE_ALIAS_GROUPS}
    assert len(hospitals) == len(hospital_by_id) == 7_365
    assert {row["hospital_id"] for row in hospitals if row["cms_hpt_url"] == locator_url} == canonical_ids | alias_ids
    for canonical, alias, name in _NORTHSHORE_ALIAS_GROUPS:
        assert hospital_by_id[alias] == {
            "hospital_id": alias, "name": name, "cms_hpt_url": locator_url,
            "alias_of": canonical, "locator_name": "Endeavor Health " + name,
        }
        assert hospital_by_id[canonical] == {
            "hospital_id": canonical, "name": "Endeavor Health " + name,
            "cms_hpt_url": locator_url,
        }
        for hospital_id in (canonical, alias):
            assert registry.hospital_hpt_group_ids(hospital_id) == (canonical, alias)
            assert registry.selected_hospital_hpt_registry({"hospital_id": hospital_id}) == (
                hospital_by_id[canonical], hospital_by_id[alias],
            )
    for hospital_id in canonical_ids - {group[0] for group in _NORTHSHORE_ALIAS_GROUPS}:
        assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)
    assert hospital_by_id["hospital-001877"]["locator_mrf_url"] == (
        "https://www.endeavorhealth.org/363297173_1427069632_edward-hospital_standardcharges.json"
    )


@pytest.mark.parametrize("record_case", ("bound", "missing", "ambiguous"))
def test_northshore_aliases_require_exact_locator_records(record_case):
    """Share content across sites, never their facility or locator identity."""
    locator_url = "https://www.endeavorhealth.org/cms-hpt.txt"
    hospitals = tuple(dict(row) for row in registry.load_hospital_hpt_registry() if row["cms_hpt_url"] == locator_url)
    shared_ids = {"hospital-002062", "hospital-002063", "hospital-002067"}
    records = tuple(HospitalHptLocatorRecord(
        row["name"], "https://files.example/shared.json" if row["hospital_id"] in shared_ids
        else f"https://files.example/{row['hospital_id']}.json",
    ) for row in hospitals if "alias_of" not in row)
    next(row for row in hospitals if row["hospital_id"] == "hospital-001877")["locator_mrf_url"] = records[0].mrf_url
    affected_ids = {"hospital-002062", "hospital-002163"}
    if record_case == "missing":
        records = tuple(record for record in records if record.location_name != "Endeavor Health Evanston Hospital")
    elif record_case == "ambiguous":
        records += (HospitalHptLocatorRecord("Endeavor Health Evanston Hospital", "https://files.example/conflict.json"),)
    match = match_hospital_hpt_locator(hospitals, locator_url, records)
    binding_by_id = {binding.hospital_id: binding for binding in match.bindings}
    if record_case == "bound":
        assert len(binding_by_id) == 14 and len(match.content_targets) == 8
        for canonical, alias, _name in _NORTHSHORE_ALIAS_GROUPS:
            assert binding_by_id[canonical].record_index == binding_by_id[alias].record_index
        assert len({binding_by_id[hospital_id].record_index for hospital_id in shared_ids}) == 3
        assert {binding_by_id[hospital_id].mrf_url for hospital_id in shared_ids} == {"https://files.example/shared.json"}
        assert not match.unmatched_hospital_ids and not match.ambiguous_hospital_ids
        assert not match.unmatched_record_indexes and not match.ambiguous_record_indexes
    else:
        assert len(binding_by_id) == 12 and not affected_ids & binding_by_id.keys()
        assert set(match.unmatched_hospital_ids) == (affected_ids if record_case == "missing" else set())
        assert set(match.ambiguous_hospital_ids) == (affected_ids if record_case == "ambiguous" else set())


def test_philadelphia_branding_aliases_preserve_distinct_good_shepherd_facilities():
    """Bind one Philadelphia specialty hospital without merging distinct campuses."""
    hospital_by_id = {
        entry["hospital_id"]: entry for entry in registry.load_hospital_hpt_registry()
    }
    group = ("hospital-005086", "hospital-002405", "hospital-005085")
    for hospital_id in group:
        assert registry.hospital_hpt_group_ids(hospital_id) == group
        assert hospital_by_id[hospital_id]["locator_name"] == "Philadelphia Post-Acute Partners LLC"
    for hospital_id in ("hospital-002406", "hospital-002407"):
        assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)


def test_primary_childrens_campuses_use_distinct_locator_records():
    """Keep each reviewed campus bound to its own locator record."""
    hospital_by_id = {
        entry["hospital_id"]: entry for entry in registry.load_hospital_hpt_registry()
    }
    prefix = (
        "https://intermountainhealthcare.org/-/media/files/intermountain-health/"
        "locations/hospital-prices/"
    )
    assert {
        suffix: hospital_by_id[f"hospital-0031{suffix}"]["locator_mrf_url"].removeprefix(prefix)
        for suffix in ("58", "59", "60", "61", "72")
    } == {
        "58": "942854057_primary-childrens-hospital_lehi_standardcharges.ashx",
        "59": "942854057_primary-childrens-hospital_taylorsville_standardcharges.ashx",
        "60": "942854057_primary-childrens-hospital_standardcharges.ashx",
        "61": "942854057_primary-childrens-hospital_taylorsville_standardcharges.ashx",
        "72": "942854057_primary-childrens-hospital_standardcharges.ashx",
    }


def test_shared_sources_preserve_lindsborg_identities_and_freeman_campuses():
    """Share reviewed content without merging identities or unrelated campuses."""
    hospital_by_id = {
        entry["hospital_id"]: entry for entry in registry.load_hospital_hpt_registry()
    }
    for suffix in ("003587", "003588", "001853", "001854", "002305", "002306",
                   "002307", "002308", "002309", "002310", "002311", "002312", "002313"):
        hospital_id = f"hospital-{suffix}"
        assert registry.hospital_hpt_group_ids(hospital_id) == (hospital_id,)
    assert [hospital_by_id[f"hospital-{suffix}"]["name"] for suffix in ("003587", "003588")] == [
        "Lindsborg Community Hospital", "LINDSBORG COMMUNITY HOSPITAL ASSOCIATION",
    ]
    assert hospital_by_id["hospital-002306"]["fallback_mrf_url"] != (
        hospital_by_id["hospital-002311"]["fallback_mrf_url"]
    )
    for suffix in ("002307", "002308", "002309", "002310"):
        assert hospital_by_id[f"hospital-{suffix}"]["fallback_mrf_url"] == (
            hospital_by_id["hospital-002306"]["fallback_mrf_url"]
        )
    assert {
        entry["hospital_id"] for entry in hospital_by_id.values()
        if entry["cms_hpt_url"] == hospital_by_id["hospital-002306"]["cms_hpt_url"]
    } == {
        f"hospital-{suffix}" for suffix in (
            "001853", "001854", "002305", "002306", "002307", "002308", "002309",
            "002310", "002311", "002312", "002313",
        )
    }


def test_checked_in_registry_has_reviewed_wvu_legal_name_aliases():
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    aliases_by_id = {
        "hospital-000715": "Barnesville Hospital",
        "hospital-001050": "Camden Clark Medical Center",
        "hospital-001395": "Berkeley Medical Center",
        "hospital-001524": "Jackson General Hospital",
        "hospital-002441": "Grant Memorial Hospital",
        "hospital-002466": "Garrett Regional Medical Center",
        "hospital-002547": "Harrison Community Hospital",
        "hospital-002898": "Thomas Hospitals",
        "hospital-005193": "Potomac Valley Hospital",
        "hospital-005213": "Princeton Community Hospital",
        "hospital-005390": "Reynolds Memorial Hospital",
        "hospital-006189": "St. Joseph's Hospital",
        "hospital-007050": "Weirton Medical Center",
        "hospital-007115": "Summersville Regional Medical Center",
        "hospital-007117": "West Virginia University Hospitals",
        "hospital-007132": "Wetzel County Hospital",
        "hospital-007134": "Wheeling Hospital",
    }
    assert {
        hospital_id: hospital_by_id[hospital_id]["locator_name"]
        for hospital_id in aliases_by_id
    } == aliases_by_id


def test_checked_in_registry_is_checksum_gated(tmp_path, monkeypatch):
    path = tmp_path / "registry.yaml"
    path.write_text(_document(), encoding="utf-8")
    monkeypatch.setattr(registry, "HOSPITAL_HPT_REGISTRY_PATH", path)
    registry._cached_hospital_hpt_registry.cache_clear()
    try:
        with pytest.raises(registry.HospitalHptRegistryError, match="checksum"):
            registry.load_hospital_hpt_registry()
    finally:
        registry._cached_hospital_hpt_registry.cache_clear()


def test_reviewed_alias_groups_and_selection_expand_both_ids(tmp_path, monkeypatch):
    locator = "https://hospital.example/cms-hpt.txt"
    hospitals = _load(
        tmp_path,
        _document(locator).replace(
            "    cms_hpt_url:", "    locator_mrf_url: https://f.test/a\n    fallback_mrf_url: https://f.test/fallback\n    cms_hpt_url:"
        )
        + f"""\
  - hospital_id: hospital-000002
    name: Example Hospital Alias
    cms_hpt_url: {locator}
    alias_of: hospital-000001
""",
    )
    monkeypatch.setattr(registry, "load_hospital_hpt_registry", lambda: hospitals)

    assert hospitals[1]["locator_name"] == "Example Hospital"
    assert hospitals[1]["locator_mrf_url"] == "https://f.test/a"
    assert hospitals[1]["fallback_mrf_url"] == "https://f.test/fallback"
    assert registry.hospital_hpt_registry_groups() == (hospitals,)
    assert registry.selected_hospital_hpt_registry({"hospital_id": "hospital-000001"}) == hospitals
    assert registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-000002"}
    ) == hospitals

@pytest.mark.parametrize(
    "extra_rows",
    [
        """\
  - hospital_id: hospital-000002
    name: Alias
    cms_hpt_url: https://hospital.example/cms-hpt.txt
    alias_of: hospital-missing
""",
        """\
  - hospital_id: hospital-000002
    name: Alias
    cms_hpt_url: https://hospital.example/cms-hpt.txt
    alias_of: hospital-000002
""",
        """\
  - hospital_id: hospital-000002
    name: Alias
    cms_hpt_url: https://other.example/cms-hpt.txt
    alias_of: hospital-000001
""",
        """\
  - hospital_id: hospital-000002
    name: Alias
    cms_hpt_url: https://hospital.example/cms-hpt.txt
    alias_of: hospital-000001
  - hospital_id: hospital-000003
    name: Chained Alias
    cms_hpt_url: https://hospital.example/cms-hpt.txt
    alias_of: hospital-000002
""",
    ],
)
def test_invalid_alias_relationships_fail_closed(tmp_path, extra_rows):
    with pytest.raises(registry.HospitalHptRegistryError, match="alias_of_invalid"):
        _load(tmp_path, _document() + extra_rows)


def test_optional_locator_name_is_strict_and_preserved(tmp_path):
    text = _document().replace(
        "    cms_hpt_url:",
        "    locator_name: Exact Locator Entry\n    cms_hpt_url:",
    )

    assert _load(tmp_path, text)[0]["locator_name"] == "Exact Locator Entry"

    with pytest.raises(registry.HospitalHptRegistryError, match="locator_name_invalid"):
        _load(tmp_path, text.replace("Exact Locator Entry", '" Exact Locator Entry"'))


def test_optional_locator_mrf_url_is_validated_and_preserved(tmp_path):
    text = _document().replace(
        "    cms_hpt_url:",
        "    locator_mrf_url: https://files.example/current.csv\n    cms_hpt_url:",
    )

    assert _load(tmp_path, text)[0]["locator_mrf_url"].endswith("current.csv")

    with pytest.raises(
        registry.HospitalHptRegistryError, match="locator_mrf_url_invalid"
    ):
        _load(tmp_path, text.replace("https://files.example/current.csv", "file.csv"))


def test_optional_fallback_mrf_url_preserves_stable_query(tmp_path):
    fallback = "https://files.example/report?facility=one&type=csv"
    text = _document().replace(
        "    cms_hpt_url:",
        f"    fallback_mrf_url: {fallback}\n    cms_hpt_url:",
    )

    assert _load(tmp_path, text)[0]["fallback_mrf_url"] == fallback

    with pytest.raises(
        registry.HospitalHptRegistryError, match="fallback_mrf_url_invalid"
    ):
        _load(tmp_path, text.replace("https://files.example", "file://local"))


@pytest.mark.parametrize(
    "selector",
    (
        "https://files.example/current.csv?sig=credential",
        "HTTPS://files.example/current.csv",
        "https://FILES.example/current.csv",
        "https://files.example:443/current.csv",
    ),
)
def test_optional_locator_mrf_selector_must_be_queryless_and_canonical(
    tmp_path, selector
):
    text = _document().replace(
        "    cms_hpt_url:",
        f"    locator_mrf_url: {selector}\n    cms_hpt_url:",
    )

    with pytest.raises(
        registry.HospitalHptRegistryError, match="locator_mrf_url_invalid"
    ):
        _load(tmp_path, text)


def test_duplicate_hospital_id_is_rejected(tmp_path):
    text = _document().replace(
        "hospital_id: hospital-000001",
        "hospital_ids:\n    - hospital-000001\n    - hospital-000001",
    )

    with pytest.raises(registry.HospitalHptRegistryError, match="duplicate_hospital_id"):
        _load(tmp_path, text)


@pytest.mark.parametrize(
    "text",
    [
        "version: true\nhospitals: []\n",
        "version: 2\nhospitals: []\n",
        "version: 1\nhospitals: []\n",
        "version: 1\nhospitals: value\n",
        "version: 1\nhospitals: []\nunexpected: value\n",
        "[]\n",
    ],
)
def test_invalid_document_shapes_are_rejected(tmp_path, text):
    with pytest.raises(registry.HospitalHptRegistryError, match="document_shape"):
        _load(tmp_path, text)


@pytest.mark.parametrize(
    "replacement",
    [
        "    unexpected: value\n",
        "",
    ],
)
def test_inexact_hospital_fields_are_rejected(tmp_path, replacement):
    text = _document().replace(
        "    cms_hpt_url: https://hospital.example/cms-hpt.txt\n",
        replacement,
    )

    with pytest.raises(registry.HospitalHptRegistryError, match="hospital_fields"):
        _load(tmp_path, text)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (_document().replace("hospital-000001", "hospital 000001"), "hospital_id_invalid"),
        (
            _document().replace("name: Example Hospital", 'name: " Example Hospital"'),
            "name_invalid",
        ),
        (_document().replace("cms-hpt.txt", "cms hpt.txt"), "cms_hpt_url_invalid"),
    ],
)
def test_invalid_text_is_rejected(tmp_path, text, message):
    with pytest.raises(registry.HospitalHptRegistryError, match=message):
        _load(tmp_path, text)


@pytest.mark.parametrize(
    "locator",
    [
        "ftp://hospital.example/locator",
        "https:///locator",
        "https://user:password@hospital.example/locator",
        "https://hospital.example/locator#fragment",
        "https://hospital.example:invalid/locator",
    ],
)
def test_invalid_locators_are_rejected(tmp_path, locator):
    with pytest.raises(registry.HospitalHptRegistryError, match="cms_hpt_url_invalid"):
        _load(tmp_path, _document(locator))


@pytest.mark.parametrize(
    "text",
    [
        "version: 1\nversion: 1\nhospitals: []\n",
        _document().replace(
            "    name: Example Hospital\n",
            "    name: Example Hospital\n    name: Another Hospital\n",
        ),
    ],
)
def test_duplicate_yaml_fields_are_rejected(tmp_path, text):
    with pytest.raises(registry.HospitalHptRegistryError, match="duplicate_field"):
        _load(tmp_path, text)


def test_malformed_yaml_is_rejected(tmp_path):
    with pytest.raises(registry.HospitalHptRegistryError, match="document_unreadable"):
        _load(tmp_path, "version: [\n")


def test_unhashable_yaml_key_is_rejected(tmp_path):
    with pytest.raises(registry.HospitalHptRegistryError, match="document_unreadable"):
        _load(tmp_path, "? [unhashable]\n: value\n")


def test_checked_in_registry_read_failure_is_normalized(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "HOSPITAL_HPT_REGISTRY_PATH", tmp_path / "missing")
    registry._cached_hospital_hpt_registry.cache_clear()
    try:
        with pytest.raises(registry.HospitalHptRegistryError, match="document_unreadable"):
            registry.load_hospital_hpt_registry()
    finally:
        registry._cached_hospital_hpt_registry.cache_clear()


def test_checked_in_registry_count_is_gated(tmp_path, monkeypatch):
    path = tmp_path / "registry.yaml"
    path.write_text(_document(), encoding="utf-8")
    monkeypatch.setattr(registry, "HOSPITAL_HPT_REGISTRY_PATH", path)
    monkeypatch.setattr(
        registry,
        "EXPECTED_HOSPITAL_HPT_REGISTRY_SHA256",
        hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(registry, "EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT", 2)
    registry._cached_hospital_hpt_registry.cache_clear()
    try:
        with pytest.raises(registry.HospitalHptRegistryError, match="count"):
            registry.load_hospital_hpt_registry()
    finally:
        registry._cached_hospital_hpt_registry.cache_clear()


def test_runtime_selection_is_exact_and_source_neutral(monkeypatch):
    hospitals = (
        {"hospital_id": "hospital-000001", "name": "One", "cms_hpt_url": "https://one.example/cms-hpt.txt"},
        {"hospital_id": "hospital-000002", "name": "Two", "cms_hpt_url": "https://two.example/cms-hpt.txt"},
    )
    monkeypatch.setattr(registry, "load_hospital_hpt_registry", lambda: hospitals)
    assert registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-000002", "test_mode": False, "run_id": "run-1"},
        runtime=True,
    ) == (hospitals[1],)
    assert registry.selected_hospital_hpt_registry(
        {"hospital_ids": ["hospital-000002", "hospital-000001", "hospital-000002"]}
    ) == hospitals
    assert registry.selected_hospital_hpt_registry({"all_hospitals": True}) == hospitals


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"hospital_id": "hospital-000001", "all_hospitals": True},
        {"hospital_ids": []},
        {"hospital_ids": ["hospital-000001"], "hospital_id": "hospital-000001"},
        {"hospital_ids": ["missing"]},
        {"hospital_ids": ["hospital-000001"] * 201},
        {"hospital_id": "missing"},
        {"all_hospitals": 1},
        {"all_hospitals": True, "test_mode": True},
        {"all_hospitals": True, "unexpected": True},
    ],
)
def test_invalid_runtime_selection_fails_closed(monkeypatch, params):
    monkeypatch.setattr(
        registry, "load_hospital_hpt_registry", lambda: ({"hospital_id": "hospital-000001"},)
    )
    with pytest.raises(registry.HospitalHptRegistryError):
        registry.selected_hospital_hpt_registry(params)
