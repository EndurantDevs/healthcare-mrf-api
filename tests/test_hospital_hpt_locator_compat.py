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


@pytest.mark.parametrize("position", ["before", "after", "inline"])
@pytest.mark.parametrize(
    "contacts",
    [
        b"contact-name: Price Team\ncontact-name: Price Team\n",
        b"contact-name: Price Team\ncontact-name: billing@example.com\n",
        b"contact-name: Price Team\n CONTACT-NAME : Another Team\n",
    ],
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
    [
        (b"mrf-url: https://files.example/mrf.csv\nMRF-URL: https://files.example/mrf.csv\n", "duplicate_field"),
        (b"mrf-url: https://files.example/mrf.csv\nmrf_url: https://files.example/other.csv\n", "duplicate_field"),
        (b"source-page-url: https://hospital.example/prices\nsource-page_url: https://hospital.example/other\n", "duplicate_field"),
        (b"contact-email: one@example.com\ncontact-email: two@example.com\n", "duplicate_field"),
        (b"unknown-field: one\nunknown-field: two\n", "duplicate_field"),
        (b"mrf-url: https://user:password@files.example/mrf.csv\n", "mrf_url"),
        (b"mrf-url: https://files.example/mrf.csv#fragment\n", "mrf_url"),
        (b"contact-name: Invalid\x00Team\n", "control_character"),
        (b"", "mrf_url"),
    ],
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


def _html_wrapper(fields: bytes) -> bytes:
    return (
        b'<!DOCTYPE html>\r\n<html lang="en"><head>\n'
        b'<link rel="preconnect" href="https://files.example">\n'
        b'<meta name="robots" content="index,follow">\n'
        b'<style>.error{display:none}</style>\n'
        b'<script>location-name: Decoy\nmrf-url: https://ignored.example/file</script>\n'
        b'</head>\r\n<body>\r\n\t\t' + fields
        + b'\n<script src="/email-decode.js"></script></body>\r\n</html>'
    )


def test_html_wrapper_preserves_literal_records_and_query_bytes():
    url = "https://files.example/Reports.aspx?dbName=dbTEST&type=CDMWithoutLabel&fileType=CSV"
    fields = (
        b"location-name: Hospital\nsource-page-url: https://hospital.example/prices\n"
        + f"mrf-url: {url}\n".encode()
        + b'contact-name: Price Team\ncontact-email: <a href="/email-protection" '
        b'class="__cf_email__" data-cfemail="001122">[email&#160;protected]</a>\n'
        b"location-name: Hospital Annex\nmrf-url: https://files.example/annex.csv\n"
    )
    expected = (_record("Hospital", url), _record("Hospital Annex", "https://files.example/annex.csv"))
    assert locator.parse_hospital_hpt_locator(_html_wrapper(fields)) == expected
    assert locator.parse_hospital_hpt_locator(fields.replace(b"\n", b"\r\n\r\n")) == expected


@pytest.mark.parametrize(
    "fields",
    (
        b"location-name: Hospital\nmrf-url: <a href='https://files.example/a.csv'>Download</a>\n",
        b"location-name: Hospital\nmrf-url: https://files.example/<a>file</a>.csv\n",
        b"location-name: Hos<b>pital</b>\nmrf-url: https://files.example/file.csv\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a&#10;contact-name:x\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a?x=1&amp;y=2\n",
        b"location-name: Hospital\nsource-page-url: https://hospital.example\n"
        b"mrf-url:\nhttps://files.example/a?x=1&amp;y=2\n",
        b"location-name: Hospital\nsource-page-url: https://hospital.example\n"
        b"/prices?x=1&amp;y=2\nmrf-url: https://files.example/a\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a<script></script>.csv\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a<!--x-->.csv\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a<broken\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a\nmalformed trailing text\n",
        b"<div>location-name: Hospital\nmrf-url: https://files.example/a\n</div>",
        b"Service unavailable\nlocation-name: Hospital\nmrf-url: https://files.example/a\n",
        b"",
    ),
)
def test_html_wrapper_never_discovers_or_reconstructs_binding_fields(fields):
    with pytest.raises(locator.HospitalHptLocatorError):
        locator.parse_hospital_hpt_locator(_html_wrapper(fields))


@pytest.mark.parametrize(
    "change",
    (
        lambda body: body.replace(b"</body>", b""),
        lambda body: body.replace(b"</html>", b""),
        lambda body: body.replace(b"</body>", b"</body><body></body>"),
        lambda body: body.replace(b"<body>", b"<body><body>"),
        lambda body: body.replace(b"</html>", b"</html>unexpected"),
        lambda body: body.replace(b"</html>", b"</html>&amp;"),
        lambda body: body.replace(b"<head>", b"<head>\x00"),
        lambda body: body.replace(b"<head>", b"<head><template>"),
        lambda body: body.replace(b"<head>", b"<head><template/>"),
        lambda body: body.replace(b"<head>", b"<head></script>"),
        lambda body: body.replace(b"</style>", b""),
        lambda body: body.replace(b"<body>", b"<body><?unexpected?>"),
    ),
)
def test_html_wrapper_requires_complete_safe_envelope(change):
    payload = _html_wrapper(b"location-name: Hospital\nmrf-url: https://files.example/a\n")
    with pytest.raises(locator.HospitalHptLocatorError):
        locator.parse_hospital_hpt_locator(change(payload))


@pytest.mark.parametrize(
    "fields",
    (
        b"location-name: Hospital\nmrf-url: relative.csv\n",
        b"location-name: Hospital\nmrf-url: https://user:password@files.example/a\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a#fragment\n",
        b"location-name: Hospital\nmrf-url: https://files.example:invalid/a\n",
        b"location-name: Hospital\nmrf-url: https://files.example/a\nMRF-URL: https://files.example/b\n",
        b"location-name: Hospital\ncontact-name: Price Team\n",
    ),
)
def test_html_wrapper_retains_existing_field_validation(fields):
    with pytest.raises(locator.HospitalHptLocatorError):
        locator.parse_hospital_hpt_locator(_html_wrapper(fields))
