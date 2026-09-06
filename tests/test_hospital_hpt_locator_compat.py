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
    "payload",
    (
        b"location-name: Hospital\rmrf-url: https://files.example/mrf.csv\r",
        b"location-name: Hospital\n"
        b"mrf-url: https://files.example/mrf.csv\n"
        b".csv\ncontact-name: Price Team\n",
        b"location-name: Hospital\n"
        b"mrf-url: mrf-url: https://files.example/mrf.csv\n",
        b"location-name: Hospital\n"
        b"source-page-url: https://hospital.example/patients\n"
        b"  /billing?price-transparency\n"
        b"mrf-url: https://files.example/mrf.csv\n",
    ),
)
def test_reviewed_locator_syntax_normalizations(payload):
    assert locator.parse_hospital_hpt_locator(payload) == (
        _record("Hospital", "https://files.example/mrf.csv"),
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    (
        (
            b"location-name: Hospital\rmrf-url: https://files.example/mrf.csv\n",
            "control_character",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: https://files.example/mrf.csv\n\n.csv\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: https://files.example/mrf.csv\n.csv \n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: https://files.example/mrf.csv\n.CSV\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: https://files.example/mrf.csv?download=1\n.csv\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: mrf-url: mrf-url: https://files.example/mrf.csv\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: MRF-URL: https://files.example/mrf.csv\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: mrf_url: https://files.example/mrf.csv\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\n"
            b"mrf-url: mrf-url: relative.csv\n",
            "mrf_url",
        ),
        (
            b"location-name: Hospital\n"
            b"source-page-url: https://hospital.example/patients\n\n"
            b"/billing\n"
            b"mrf-url: https://files.example/mrf.csv\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"source-page-url: https://hospital.example/patients\n"
            b"billing\n"
            b"mrf-url: https://files.example/mrf.csv\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"source-page-url:\n/billing\n"
            b"mrf-url: https://files.example/mrf.csv\n",
            "line",
        ),
        (
            b"location-name: Hospital\n"
            b"source-page-url: https://hospital.example/patients\n"
            b"/billing\n/charges\n"
            b"mrf-url: https://files.example/mrf.csv\n",
            "line",
        ),
    ),
)
def test_reviewed_locator_syntax_normalizations_fail_closed(payload, reason):
    with pytest.raises(locator.HospitalHptLocatorError, match=reason):
        locator.parse_hospital_hpt_locator(payload)


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


@pytest.mark.parametrize("position", ("before", "after", "inline"))
@pytest.mark.parametrize(
    "contacts",
    (
        b"contact-name: Price Team\ncontact-name: Price Team\n",
        b"contact-name: Price Team\ncontact-name: billing@example.com\n",
        b"contact-name: Price Team\n CONTACT-NAME : Another Team\n",
    ),
)
def test_repeated_contact_names_preserve_the_binding(position, contacts):
    mrf = b"mrf-url: https://files.example/mrf.csv\n"
    if position == "before":
        fields = contacts + mrf
    elif position == "after":
        fields = mrf + contacts
    else:
        fields = mrf.rstrip(b"\n") + b" contact-name: Inline Team\n" + contacts

    assert locator.parse_hospital_hpt_locator(b"location-name: Hospital\n" + fields) == (
        _record("Hospital", "https://files.example/mrf.csv"),
    )


def test_repeated_contact_names_preserve_record_order():
    payload = (
        b"location-name: Hospital One\nmrf-url: https://files.example/one.csv\n"
        b"contact-name: First Team\ncontact-name: Other Team\n\n"
        b"location-name: Hospital Two\ncontact-name: First Team\n"
        b"contact-name: Other Team\nmrf-url: https://files.example/two.csv"
    )
    assert locator.parse_hospital_hpt_locator(payload) == (
        _record("Hospital One", "https://files.example/one.csv"),
        _record("Hospital Two", "https://files.example/two.csv"),
    )


@pytest.mark.parametrize(
    ("fields", "reason"),
    (
        (b"mrf-url: https://files.example/mrf.csv\nMRF-URL: https://files.example/mrf.csv\n", "duplicate_field"),
        (b"mrf-url: https://files.example/mrf.csv\nmrf_url: https://files.example/other.csv\n", "duplicate_field"),
        (b"source-page-url: https://hospital.example/prices\nsource-page_url: https://hospital.example/other\n", "duplicate_field"),
        (b"contact-email: one@example.com\ncontact-email: two@example.com\n", "duplicate_field"),
        (b"unknown-field: one\nunknown-field: two\n", "duplicate_field"),
        (b"mrf-url: https://user:password@files.example/mrf.csv\n", "mrf_url"),
        (b"mrf-url: https://files.example/mrf.csv#fragment\n", "mrf_url"),
        (b"contact-name: Invalid\x00Team\n", "control_character"),
        (b"", "mrf_url"),
    ),
)
def test_repeated_contacts_do_not_relax_other_validation(fields, reason):
    payload = b"location-name: Hospital\ncontact-name: One\ncontact-name: Two\n" + fields
    with pytest.raises(locator.HospitalHptLocatorError, match=reason):
        locator.parse_hospital_hpt_locator(payload)


def test_contact_names_still_require_a_location_first():
    with pytest.raises(locator.HospitalHptLocatorError, match="location_name"):
        locator.parse_hospital_hpt_locator(
            b"contact-name: One\ncontact-name: Two\n"
            b"location-name: Hospital\nmrf-url: https://files.example/mrf.csv\n"
        )
