# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from process import hospital_hpt_registry as registry
from tests.hospital_hpt_registry_fallbacks import (
    FALLBACK_URL_SHA256_BY_HOSPITAL_ID as _FALLBACK_URL_SHA256_BY_HOSPITAL_ID,
)
_REVIEWED_ALIAS_SAMPLES = {
    f"hospital-{alias}": f"hospital-{canonical}"
    for alias, canonical in (pair.split(":") for pair in "000061:000060 000064:000063 000123:000122 000162:000161 001486:001483 002520:002519 004667:004666 005329:005328 005563:001678 005564:001678 005565:001678 006233:005566 007207:007206 007272:000586 000121:000120 000342:000343 000593:000592 000654:000604 000655:000600 000656:000592 000657:000606 000745:000744 002911:000189 005797:005798 005077:005063 006650:006649 003017:003012 003068:003013 003069:003014 003070:003015 003071:003016 003072:003019 003073:003018 003074:003020 003075:003021 003076:003022 003077:003023 003078:003024 003079:003025 002432:002433 006299:006300 005971:005970 005973:005972 001882:001881 005163:005162 003238:005914 002844:004555 006900:006899 000905:000904 001851:006405 006402:002912 006403:006404 006987:006406 007167:007168 000229:000231 000230:000232 000806:000807 001263:006172 001264:006171 001265:006173 001266:006174 001267:006175 001270:000805 001272:006200 001273:006207 001274:006208 001275:006209 001276:006203 001280:001277 001533:001535 002319:002318 002377:002378 006164:006161 006190:006191 006212:006205 006215:006201 006225:006204 006226:006206 006237:006234 006263:005494 006264:005641 006265:005787 006549:001253 007234:007237 007235:007236 000902:000901 003410:003409 004869:004870 005186:005187 005357:005358 006266:005582 006285:006284 006330:005919 006331:005920 006651:006652 003161:003159 003172:003160 001586:001587 001589:001590 001591:001592 001593:001594 001598:001596 001599:001597 001600:001612 001601:001602 001603:001604 001606:001607 001608:001609 001610:001611 001613:001614 001615:001616 000514:000513 004534:000825 004535:000826 001433:001435 001434:006080 006396:003096 006397:006395".split())
}
_REVIEWED_LOCATOR_NAMES = {
    "hospital-000047": "Adair County Memorial Hospital",
    "hospital-000126": "HANFORD COMMUNITY HOSPITAL",
    "hospital-000188": "Amberwell Atchison Association",
    "hospital-000342": "Ashland Health Center",
    "hospital-000600": "Baptist Health Hardin",
    "hospital-000833": "Beckett Springs",
    "hospital-001199": "Cottonwood Springs",
    **dict(pair.split(":", 1) for pair in "hospital-001587:Corewell Health Big Rapids|hospital-001590:Corewell Health Gerber|hospital-001592:Corewell Health Greenville|hospital-001594:Corewell Health Gross Pointe|hospital-001596:Corewell Health Lakeland Niles|hospital-001597:Corewell Health Lakeland St. Joseph|hospital-001602:Corewell Health Ludington|hospital-001604:Corewell Health Reed City|hospital-001607:Corewell Health Taylor|hospital-001609:Corewell Health Trenton|hospital-001611:Corewell Health Troy|hospital-001612:Corewell Health Lakeland Watervliet|hospital-001614:Corewell Health Wayne|hospital-001616:Corewell Health Zeeland".split("|")),
    "hospital-001880": "Edgerton Hospital and Health Services - Fulton Square Clinic",
    "hospital-001881": "Edgerton Hospital and Health Services - Milton Clinic",
    "hospital-002260": "Franciscan Health Orthopedic-Carmel",
    "hospital-002421": "Grady Health System",
    "hospital-003238": "Southern Humboldt Community Hospital",
    "hospital-003145": "Intermountain Health Good Samaritan Medical Center",
    "hospital-003148": "Holy Rosary Healthcare",
    "hospital-003157": "Platte Valley Medical Center",
    "hospital-003163": "Saint Joseph Hospital",
    "hospital-003168": "St. James Healthcare",
    "hospital-003169": "St. Mary's Medical Center",
    "hospital-003170": "St. Vincent Healthcare",
    "hospital-003240": "Jersey Community Hospital",
    "hospital-003592": "Little River Medical Center, INC DBA Little River Memorial Hospital",
    "hospital-005162": "Pioneer Memorial Hospital & Health Services",
    "hospital-005304": "Ramapo Ridge Behavioral Health",
    "hospital-005915": "Mee Memorial Hospital",
    "hospital-006345": "Summa Rehab Hospital, LLC",
}

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
    assert len(registry.hospital_hpt_registry_groups()) == 7_005
    assert len({entry["hospital_id"] for entry in hospitals}) == len(hospitals)
    assert "alias_of" not in hospital_by_id["hospital-005625"]
    assert sum("locator_name" in entry for entry in hospitals) == 1_557
    assert sum("locator_mrf_url" in entry for entry in hospitals) == 646
    assert sum("fallback_mrf_url" in entry for entry in hospitals) == 60
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
    assert [hospital_by_id[hospital_id]["cms_hpt_url"] for hospital_id in (
        "hospital-000047", "hospital-000188", "hospital-000600",
        "hospital-005162", "hospital-005163",
    )] == [
        "https://www.achsiowa.org/cms-hpt.txt",
        "https://amberwellhealth.org/cms-hpt.txt",
        "https://www.baptisthealth.com/cms-hpt.txt",
        "https://www.pioneermemorial.org/cms-hpt.txt",
        "https://www.pioneermemorial.org/cms-hpt.txt",
    ]
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


def test_checked_in_registry_has_reviewed_canonical_aliases():
    """Keep reviewed alias identities explicit while preserving every raw ID."""
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    aliases_by_id = {
        entry["hospital_id"]: entry["alias_of"]
        for entry in hospitals
        if "alias_of" in entry
    }
    assert len(aliases_by_id) == 351
    assert not {"hospital-000833", "hospital-001199"} & aliases_by_id.keys()
    assert {
        hospital_id: aliases_by_id[hospital_id]
        for hospital_id in _REVIEWED_ALIAS_SAMPLES
    } == _REVIEWED_ALIAS_SAMPLES
    assert hospital_by_id["hospital-000063"]["name"] == (
        "Advanced Specialty Hospitals of Toledo"
    )


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


def test_duplicate_names_and_locators_are_preserved(tmp_path):
    locator = "http://hospital.example:8080/nonstandard/path?view=current"
    hospitals = _load(
        tmp_path,
        _document(locator).replace(
            "hospital_id: hospital-000001",
            "hospital_ids:\n    - hospital-000001\n    - hospital-000002",
        ),
    )
    assert [entry["cms_hpt_url"] for entry in hospitals] == [locator, locator]
    assert [entry["name"] for entry in hospitals] == [
        "Example Hospital",
        "Example Hospital",
    ]


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
