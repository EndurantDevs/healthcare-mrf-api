# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from process import hospital_hpt_registry as registry


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

    assert len(hospitals) == registry.EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT
    assert len({entry["hospital_id"] for entry in hospitals}) == len(hospitals)
    assert sum("locator_name" in entry for entry in hospitals) == 1_194
    assert sum("locator_mrf_url" in entry for entry in hospitals) == 637
    assert sum("fallback_mrf_url" in entry for entry in hospitals) == 1
    assert all(
        {"hospital_id", "name", "cms_hpt_url"} <= set(entry)
        <= {
            "fallback_mrf_url", "hospital_id", "name", "cms_hpt_url",
            "locator_name", "locator_mrf_url",
        }
        for entry in hospitals
    )


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
