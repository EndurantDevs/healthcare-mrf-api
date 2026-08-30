# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from process import hospital_hpt_registry as registry


_FALLBACK_URL_SHA256_BY_HOSPITAL_ID = {
    "hospital-000188": "a6632acf6862be3c7ff2ae19b51a2fa0a5da283b7d6a0954ced59e323408f1bc",
    "hospital-000189": "dff5b72c99a19b8989184eb65b37de4246c4dd3649097b697830fcd8b22638ab",
    "hospital-000290": "6c50dd498f71ee43bf7c440e1af0d793fdd27a9c593e0adda4121df714cd7740",
    "hospital-001163": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-001503": "6c134e170f5dfe9aa4ac2ab2dae9f0523bfa17262bb97813e16f07d2bba14615",
    "hospital-002491": "f7e5a19c5a44e85520da233d3d17025d9c834a8f7b817d9f5b7c4ef6596b6e64",
    "hospital-002911": "dff5b72c99a19b8989184eb65b37de4246c4dd3649097b697830fcd8b22638ab",
    "hospital-003312": "75d4e626daf0db2c1e53cc903f3669d04339bbffb8891f92dd588c7fa3d0316f",
    "hospital-004979": "dee8d2ff24f723f64f41aa8c576ad18113657d7e11b0b59a392c58fd8acb765d",
    "hospital-005156": "180a1ae8dfcb952d7189c1c9ffb03ad121835699375b1e4ef9734c0764151192",
    "hospital-005608": "00f218c51149bc2237e87924d78a7e244607d53421e5cd18943a26a9f9e7c9c5",
    "hospital-005609": "00f218c51149bc2237e87924d78a7e244607d53421e5cd18943a26a9f9e7c9c5",
    "hospital-006053": "4d983a60e26bb91ee01c54bf194c91bc6a5d2b6215807af651dc96950ff55a6b",
    "hospital-006471": "dc7b9213c55ff2a6d9626a7841c532b5e7ebf1dce51e17efda59ead2c3f17de4",
    "hospital-006488": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-006502": "75d4e626daf0db2c1e53cc903f3669d04339bbffb8891f92dd588c7fa3d0316f",
    "hospital-006547": "4001360464d0b094a10df3bd688d3879f0bc0d6c07ee966021772d689f0aebf7",
    "hospital-006620": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-006621": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-006622": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-006635": "587c428f38fdc873612470c48e12b13a0405f0a63fe572f61a8c2702d208c6df",
    "hospital-007195": "4001360464d0b094a10df3bd688d3879f0bc0d6c07ee966021772d689f0aebf7",
    "hospital-007318": "923f885b47e27d492d193557d2cd671f0d0a63becb3c971ad9092f794fea2b36",
    "hospital-007340": "0e057fbfbc92c7cdfa98bfdd02a06c7f2ce7eb5fe9b48eddac785ed1686fe27c",
}

_REVIEWED_ALIAS_SAMPLES = {
    "hospital-000064": "hospital-000063",
    "hospital-000123": "hospital-000122", "hospital-000162": "hospital-000161",
    "hospital-001486": "hospital-001483", "hospital-002520": "hospital-002519",
    "hospital-004667": "hospital-004666", "hospital-005329": "hospital-005328",
    "hospital-005563": "hospital-001678", "hospital-005564": "hospital-001678",
    "hospital-005565": "hospital-001678", "hospital-006233": "hospital-005566",
    "hospital-007207": "hospital-007206", "hospital-007272": "hospital-000586",
    "hospital-000121": "hospital-000120", "hospital-000342": "hospital-000343",
    "hospital-000593": "hospital-000592", "hospital-000654": "hospital-000604",
    "hospital-000655": "hospital-000600", "hospital-000656": "hospital-000592",
    "hospital-000657": "hospital-000606", "hospital-000745": "hospital-000744",
    "hospital-002911": "hospital-000189", "hospital-005797": "hospital-005798",
    "hospital-006650": "hospital-006649",
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
    hospitals = registry.load_hospital_hpt_registry()
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}

    assert len(hospitals) == registry.EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT
    assert len(registry.hospital_hpt_registry_groups()) == 7_256
    assert len({entry["hospital_id"] for entry in hospitals}) == len(hospitals)
    assert "alias_of" not in hospital_by_id["hospital-005625"]
    assert sum("locator_name" in entry for entry in hospitals) == 1_258
    assert sum("locator_mrf_url" in entry for entry in hospitals) == 636
    assert sum("fallback_mrf_url" in entry for entry in hospitals) == 24
    assert {
        entry["hospital_id"] for entry in hospitals if "fallback_mrf_url" in entry
    } == set(_FALLBACK_URL_SHA256_BY_HOSPITAL_ID)
    assert {
        hospital_id: hospital_by_id[hospital_id]["locator_name"]
        for hospital_id in (
            "hospital-000047",
            "hospital-000126",
            "hospital-000188",
            "hospital-000342",
            "hospital-000600",
        )
    } == {
        "hospital-000047": "Adair County Memorial Hospital",
        "hospital-000126": "HANFORD COMMUNITY HOSPITAL",
        "hospital-000188": "Amberwell Atchison Association",
        "hospital-000342": "Ashland Health Center",
        "hospital-000600": "Baptist Health Hardin",
    }
    assert [hospital_by_id[hospital_id]["cms_hpt_url"] for hospital_id in (
        "hospital-000047", "hospital-000188", "hospital-000600",
    )] == [
        "https://www.achsiowa.org/cms-hpt.txt",
        "https://amberwellhealth.org/cms-hpt.txt",
        "https://www.baptisthealth.com/cms-hpt.txt",
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

    assert len(aliases_by_id) == 100
    assert {
        hospital_id: aliases_by_id[hospital_id]
        for hospital_id in _REVIEWED_ALIAS_SAMPLES
    } == _REVIEWED_ALIAS_SAMPLES
    assert hospital_by_id["hospital-000063"]["name"] == (
        "Advanced Specialty Hospitals of Toledo"
    )


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
        _document(locator)
        + f"""\
  - hospital_id: hospital-000002
    name: Example Hospital
    cms_hpt_url: {locator}
""",
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
        _document(locator)
        + f"""\
  - hospital_id: hospital-000002
    name: Example Hospital Alias
    cms_hpt_url: {locator}
    alias_of: hospital-000001
""",
    )
    monkeypatch.setattr(registry, "load_hospital_hpt_registry", lambda: hospitals)

    assert registry.hospital_hpt_registry_groups() == (hospitals,)
    assert registry.selected_hospital_hpt_registry(
        {"hospital_id": "hospital-000001"}
    ) == hospitals
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
    text = _document() + """\
  - hospital_id: hospital-000001
    name: Another Hospital
    cms_hpt_url: https://another.example/locator
"""

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
        with pytest.raises(
            registry.HospitalHptRegistryError, match="document_unreadable"
        ):
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
        registry,
        "load_hospital_hpt_registry",
        lambda: ({"hospital_id": "hospital-000001"},),
    )
    with pytest.raises(registry.HospitalHptRegistryError):
        registry.selected_hospital_hpt_registry(params)
