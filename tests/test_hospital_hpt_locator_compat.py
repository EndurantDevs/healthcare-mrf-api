# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from tests.test_hospital_hpt_locator import locator


def _record(name: str, url: str):
    return locator.HospitalHptLocatorRecord(name, url)


def test_reviewed_locator_compatibility_variants():
    assert locator.parse_hospital_hpt_locator(
        b"location-name: Hospital\n"
        b"source-page_url: https://hospital.example/prices\n"
        b"mrf_url: https://files.example/mrf.json\n"
    ) == (_record("Hospital", "https://files.example/mrf.json"),)

    payload = (
        b"location-name: Hospital One\n"
        b"mrf-url: https://files.example/one.csv\n\n"
        b"System Two\n"
        b"location-name: Hospital Two\n"
        b"mrf-url: https://files.example/two.csv\n"
    )
    assert locator.parse_hospital_hpt_locator(payload) == (
        _record("Hospital One", "https://files.example/one.csv"),
        _record("Hospital Two", "https://files.example/two.csv"),
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    (
        (b"location-name: Hospital\nmrf_url: relative.json\n", "mrf_url"),
        (
            b"location-name: Hospital\n"
            b"mrf-url: https://files.example/one.json\n"
            b"mrf_url: https://files.example/two.json\n",
            "duplicate_field",
        ),
    ),
)
def test_underscore_aliases_keep_existing_validation(payload, reason):
    with pytest.raises(locator.HospitalHptLocatorError, match=reason):
        locator.parse_hospital_hpt_locator(payload)


@pytest.mark.parametrize(
    "malformed_line",
    (
        b"mrf-url https://files.example/ignored.csv",
        "mrf-url\N{NO-BREAK SPACE}https://files.example/ignored.csv".encode(),
        b"mrf_url https://files.example/ignored.csv",
        b"location-name Fake heading",
        b"mrf-url",
        b"mrf_url",
        b"location-name",
    ),
)
@pytest.mark.parametrize("is_after_record", (False, True))
def test_reserved_field_like_lines_never_become_headings(
    malformed_line, is_after_record
):
    prefix = (
        b"location-name: Hospital One\n"
        b"mrf-url: https://files.example/one.csv\n\n"
        if is_after_record
        else b""
    )
    payload = (
        prefix
        + malformed_line
        + b"\nlocation-name: Hospital Two\n"
        b"mrf-url: https://files.example/two.csv\n"
    )
    with pytest.raises(locator.HospitalHptLocatorError):
        locator.parse_hospital_hpt_locator(payload)


@pytest.mark.parametrize(
    "payload",
    (
        b"location-name: One\n\nSystem Two\n"
        b"location-name: Two\nmrf-url: https://files.example/two.csv\n",
        b"location-name: One\nmrf-url: https://files.example/one.csv\n\n"
        b"System Two\nSystem Three\n"
        b"location-name: Two\nmrf-url: https://files.example/two.csv\n",
        b"location-name: One\nmrf-url: https://files.example/one.csv\n\n"
        b"System Two\n",
    ),
)
def test_inter_record_headings_require_one_complete_boundary(payload):
    with pytest.raises(locator.HospitalHptLocatorError, match="line"):
        locator.parse_hospital_hpt_locator(payload)
