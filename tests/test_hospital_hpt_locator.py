# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).parents[1] / "process/hospital_hpt_locator.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "hospital_hpt_locator_isolated",
    MODULE_PATH,
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
locator = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = locator
MODULE_SPEC.loader.exec_module(locator)


def _record(name: str, url: str = "https://files.example/mrf.json"):
    return locator.HospitalHptLocatorRecord(name, url)


def test_parser_accepts_bom_blank_and_repeated_location_boundaries():
    payload = (
        "\ufefflocation-name: Hôpital One\r\n"
        "source-page-url: https://hospital.example/prices\r\n"
        "mrf-url: https://files.example/one.json?token=a:b\r\n"
        "\r\n"
        "location-name: Hospital Two\r\n"
        "mrf-url: http://files.example/two.csv?download=1\r\n"
        "location-name: Hospital Three\r\n"
        "mrf-url: https://files.example/three.json\r\n"
    ).encode("utf-8")

    records = locator.parse_hospital_hpt_locator(payload)

    assert records == (
        _record("Hôpital One", "https://files.example/one.json?token=a:b"),
        _record("Hospital Two", "http://files.example/two.csv?download=1"),
        _record("Hospital Three", "https://files.example/three.json"),
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (b"", "empty"),
        (b"location-name: Hospital\n", "mrf_url"),
        (b"mrf-url: https://files.example/mrf.json\n", "location_name"),
        (
            b"location-name: Hospital\nmrf-url: relative.json\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\nmrf-url: ftp://files.example/mrf\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\nmrf-url: https://u:p@files.example/mrf\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\nmrf-url: https://files.example/mrf#\n",
            "mrf_url",
        ),
        (b"location-name: Hos\tpital\n", "control_character"),
        (b"location-name Hospital\n", "line"),
        (b"\xff", "utf8"),
    ],
)
def test_parser_rejects_invalid_payloads(payload, reason):
    with pytest.raises(locator.HospitalHptLocatorError, match=reason):
        locator.parse_hospital_hpt_locator(payload)


def test_parser_rejects_duplicate_fields_case_insensitively():
    payload = b"""\
location-name: Hospital
mrf-url: https://files.example/one.json
MRF-URL: https://files.example/two.json
"""

    with pytest.raises(locator.HospitalHptLocatorError, match="duplicate_field"):
        locator.parse_hospital_hpt_locator(payload)


def test_parser_rejects_oversize_and_non_bytes_payloads():
    with pytest.raises(locator.HospitalHptLocatorError, match="payload_too_large"):
        locator.parse_hospital_hpt_locator(
            b"x" * (locator.MAX_HOSPITAL_HPT_LOCATOR_BYTES + 1)
        )
    with pytest.raises(locator.HospitalHptLocatorError, match="payload_type"):
        locator.parse_hospital_hpt_locator("location-name: Hospital")


def test_parser_rejects_control_characters_embedded_bom_and_invalid_port():
    for payload in (
        b"location-name: Hospital\rmrf-url: https://files.example/mrf.json\n",
        b"location-name: Hospital\n\xef\xbb\xbfmrf-url: https://files.example/mrf.json\n",
        b"location-name: Hospital\nmrf-url: https://files.example:invalid/mrf\n",
    ):
        with pytest.raises(locator.HospitalHptLocatorError):
            locator.parse_hospital_hpt_locator(payload)


def test_parser_flushes_final_record_without_trailing_newline():
    assert locator.parse_hospital_hpt_locator(
        b"location-name: Hospital\nmrf-url: https://files.example/mrf.json"
    ) == (_record("Hospital"),)


def test_matcher_normalizes_exact_names_and_deduplicates_content_targets():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    registry_hospitals = (
        {
            "hospital_id": "hospital-1",
            "name": "Café & Medical Center",
            "cms_hpt_url": shared_locator,
        },
        {
            "hospital_id": "hospital-2",
            "name": "Branch   Hospital",
            "cms_hpt_url": shared_locator,
        },
        {
            "hospital_id": "other-locator",
            "name": "Outside Hospital",
            "cms_hpt_url": "https://other.example/cms-hpt.txt",
        },
    )
    shared_mrf = "https://files.example/shared.json?version=2"
    locator_records = (
        _record("Cafe\u0301 &amp; Medical\u00a0Center", shared_mrf),
        _record("branch hospital", shared_mrf),
        _record("Unregistered Hospital", "https://files.example/extra.json"),
    )

    match_summary = locator.match_hospital_hpt_locator(
        registry_hospitals,
        shared_locator,
        locator_records,
    )

    assert [
        (binding.hospital_id, binding.record_index)
        for binding in match_summary.bindings
    ] == [
        ("hospital-1", 0),
        ("hospital-2", 1),
    ]
    assert match_summary.content_targets == (shared_mrf,)
    assert match_summary.unmatched_hospital_ids == ()
    assert match_summary.unmatched_record_indexes == (2,)
    assert match_summary.ambiguous_hospital_ids == ()
    assert match_summary.ambiguous_record_indexes == ()


def test_matcher_allows_many_hospitals_per_record_but_rejects_repeated_records():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    registry_hospitals = (
        {
            "hospital_id": "duplicate-1",
            "name": "Same Hospital",
            "cms_hpt_url": shared_locator,
        },
        {
            "hospital_id": "duplicate-2",
            "name": "same hospital",
            "cms_hpt_url": shared_locator,
        },
        {
            "hospital_id": "missing",
            "name": "Missing Hospital",
            "cms_hpt_url": shared_locator,
        },
        {
            "hospital_id": "repeated-record",
            "name": "Repeated Record",
            "cms_hpt_url": shared_locator,
        },
    )
    locator_records = (
        _record("Same Hospital"),
        _record("Repeated Record", "https://files.example/a.json"),
        _record("repeated record", "https://files.example/b.json"),
    )

    match_summary = locator.match_hospital_hpt_locator(
        registry_hospitals,
        shared_locator,
        locator_records,
    )

    assert [
        (binding.hospital_id, binding.record_index)
        for binding in match_summary.bindings
    ] == [("duplicate-1", 0), ("duplicate-2", 0)]
    assert match_summary.content_targets == ("https://files.example/mrf.json",)
    assert match_summary.unmatched_hospital_ids == ("missing",)
    assert match_summary.unmatched_record_indexes == ()
    assert match_summary.ambiguous_hospital_ids == (
        "repeated-record",
    )
    assert match_summary.ambiguous_record_indexes == (1, 2)


def test_matcher_does_not_fuzzy_match_names():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    hospitals = (
        {
            "hospital_id": "hospital-1",
            "name": "North Medical Center",
            "cms_hpt_url": shared_locator,
        },
    )

    result = locator.match_hospital_hpt_locator(
        hospitals,
        shared_locator,
        (_record("North Medical Ctr"),),
    )

    assert result.unmatched_hospital_ids == ("hospital-1",)
    assert result.unmatched_record_indexes == (0,)
    assert result.bindings == ()


def test_matcher_uses_explicit_exact_locator_name_for_a_sublocation():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    hospital_by_field = {
        "hospital_id": "sublocation",
        "name": "South Outpatient Center",
        "locator_name": "Parent Hospital - South Campus",
        "cms_hpt_url": shared_locator,
    }

    result = locator.match_hospital_hpt_locator(
        (hospital_by_field,),
        shared_locator,
        (_record("Parent Hospital - South Campus"),),
    )

    assert result.bindings[0].hospital_id == "sublocation"
    assert result.unmatched_hospital_ids == ()


def test_matcher_uses_exact_mrf_selector_for_repeated_locator_names():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    hospital_by_field = {
        "hospital_id": "selected",
        "name": "Same Hospital",
        "locator_mrf_url": "https://files.example/b.json",
        "cms_hpt_url": shared_locator,
    }

    result = locator.match_hospital_hpt_locator(
        (hospital_by_field,),
        shared_locator,
        (
            _record("Same Hospital", "https://files.example/a.json"),
            _record("Same Hospital", "https://files.example/b.json"),
        ),
    )

    assert [(binding.record_index, binding.mrf_url) for binding in result.bindings] == [
        (1, "https://files.example/b.json")
    ]
    assert result.ambiguous_hospital_ids == ()


def test_matcher_collapses_identical_repeated_locator_records():
    shared_locator = "https://hospital.example/cms-hpt.txt"
    hospital_by_field = {
        "hospital_id": "selected",
        "name": "Same Hospital",
        "cms_hpt_url": shared_locator,
    }
    repeated = _record("Same Hospital", "https://files.example/same.json")

    result = locator.match_hospital_hpt_locator(
        (hospital_by_field,), shared_locator, (repeated, repeated)
    )

    assert result.bindings[0].mrf_url == repeated.mrf_url
    assert result.ambiguous_hospital_ids == ()
