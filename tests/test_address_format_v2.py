# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.ext.address_format import render_formatted_address_v2


@pytest.mark.parametrize(
    ("first_line", "city_name", "postal_code", "expected"),
    (
        (
            "3800 S WHITNEY AVE",
            "INDEPENDENCE",
            "64055",
            "3800 South Whitney Avenue, Independence, MO 64055",
        ),
        (
            "11525 OLDE CABIN RD",
            "SAINT LOUIS",
            "631417146",
            "11525 Olde Cabin Road, Saint Louis, MO 63141-7146",
        ),
    ),
)
def test_renderer_formats_uppercase_us_sources_for_humans(
    first_line: str,
    city_name: str,
    postal_code: str,
    expected: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        None,
        city_name,
        "MO",
        postal_code,
        "US",
    ) == expected


@pytest.mark.parametrize(
    ("first_line", "second_line"),
    (
        ("4007 Clarksville Pike Suite 301", "Ste 301"),
        ("4007 CLARKSVILLE PIKE STE 301", "suite #301"),
    ),
)
def test_v2_renderer_uses_only_structured_unit_components(
    first_line: str,
    second_line: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        second_line,
        "NASHVILLE",
        "TN",
        "37218",
        "United States",
    ) == "4007 Clarksville Pike, Suite 301, Nashville, TN 37218"


@pytest.mark.parametrize(
    ("first_line", "second_line", "city_name", "expected"),
    (
        (
            "100 MAIN ST",
            "100 MAIN ST",
            "CITY",
            "100 Main Street, City, MO 64055",
        ),
        (
            "BLDG I",
            None,
            "CITY",
            "Building I, City, MO 64055",
        ),
        (
            "12B MAIN ST STE McDonald",
            None,
            ",;:",
            "12B Main Street, Suite McDonald, MO 64055",
        ),
    ),
)
def test_v2_renderer_handles_display_edge_contracts(
    first_line: str,
    second_line: str | None,
    city_name: str,
    expected: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        second_line,
        city_name,
        "MO",
        "64055",
        "US",
    ) == expected


@pytest.mark.parametrize(
    ("first_line", "second_line", "expected"),
    (
        ("123 UNIT RD", None, "123 Unit Road, City, MO 64055"),
        ("1 BUILDING WAY", None, "1 Building Way, City, MO 64055"),
        (
            "123 STE GENEVIEVE DR",
            None,
            "123 Ste Genevieve Drive, City, MO 64055",
        ),
        (
            "100 MAIN ST 2ND FLOOR",
            "FLOOR 2",
            "100 Main Street, Floor 2, City, MO 64055",
        ),
        (
            "100 MAIN ST floor A",
            None,
            "100 Main Street, Floor A, City, MO 64055",
        ),
        (
            "100 MAIN ST STE. 301",
            "STE 301",
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 MAIN ST STE#301",
            "SUITE #301",
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 MAIN ST APT 2ND",
            None,
            "100 Main Street, Apartment 2nd, City, MO 64055",
        ),
        (
            "100 MAIN ST STE MCDONALD",
            None,
            "100 Main Street, Suite Mcdonald, City, MO 64055",
        ),
        (
            "1110 CALLE FLAMBOYAN",
            None,
            "1110 Calle Flamboyan, City, MO 64055",
        ),
        ("123 STEWART", None, "123 Stewart, City, MO 64055"),
        ("STEWART A", None, "Stewart A, City, MO 64055"),
        (
            "COMMANDING OFFICER",
            None,
            "Commanding Officer, City, MO 64055",
        ),
        ("123 OCEAN FRONT", None, "123 Ocean Front, City, MO 64055"),
        ("GME OFFICE", None, "Gme Office, City, MO 64055"),
        (
            "MEDICAL OFFICE BUILDING",
            None,
            "Medical Office Building, City, MO 64055",
        ),
        (
            "UNIVERSITY DEPARTMENT PEDIATRICS",
            None,
            "University Department Pediatrics, City, MO 64055",
        ),
        (
            "EMERGENCY ROOM PHYSICIANS",
            None,
            "Emergency Room Physicians, City, MO 64055",
        ),
        ("POST OFFICE BOX", None, "PO Box, City, MO 64055"),
        ("ST OFFICE", None, "St Office, City, MO 64055"),
        (
            "100 MAIN ST SUITE E",
            None,
            "100 Main Street, Suite E, City, MO 64055",
        ),
        (
            "100 MAIN ST STE301",
            None,
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 MAIN ST #LA",
            None,
            "100 Main Street, Suite La, City, MO 64055",
        ),
        (
            "100 MAIN ST BLDG A STE 2",
            None,
            "100 Main Street, Building A, Suite 2, City, MO 64055",
        ),
        (
            "100 MAIN ST APT 2 OFFICE",
            None,
            "100 Main Street, Apartment 2, Office, City, MO 64055",
        ),
        (
            "100 MAIN ST",
            "BLDG A STE 2",
            "100 Main Street, Building A, Suite 2, City, MO 64055",
        ),
        (
            "100 MAIN ST STE-301",
            "STE 301",
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 MAIN ST STE/301-A",
            None,
            "100 Main Street, Suite 301-A, City, MO 64055",
        ),
        ("100 S. MAIN ST", None, "100 South Main Street, City, MO 64055"),
        (
            "100 MAIN ST N.",
            None,
            "100 Main Street North, City, MO 64055",
        ),
        ("100 N.E. MAIN ST", None, "100 NE Main Street, City, MO 64055"),
        ("100 N E MAIN ST", None, "100 NE Main Street, City, MO 64055"),
        (
            "100 MAIN ST N.W.",
            None,
            "100 Main Street NW, City, MO 64055",
        ),
        ("100 S MAIN ST.", None, "100 South Main Street, City, MO 64055"),
        ("100 MAIN ST,", None, "100 Main Street, City, MO 64055"),
        ("POST OFFICE BOX. 42", None, "PO Box 42, City, MO 64055"),
        ("P.O. BOX#42", None, "PO Box 42, City, MO 64055"),
        ("P.O. BOX #42", None, "PO Box 42, City, MO 64055"),
        ("RR 2 BOX #42", None, "RR 2 Box 42, City, MO 64055"),
        ("100 MAIN ST OFC", None, "100 Main Street, Office, City, MO 64055"),
        (
            "100 MAIN ST OFFICE",
            None,
            "100 Main Street, Office, City, MO 64055",
        ),
        (
            "100 MAIN ST OFFICE 200",
            None,
            "100 Main Street, Office 200, City, MO 64055",
        ),
        (
            "100 MAIN ST N, OFFICE",
            None,
            "100 Main Street North, Office, City, MO 64055",
        ),
        (
            "100 MAIN ST DEPT 2",
            None,
            "100 Main Street, Department 2, City, MO 64055",
        ),
        ("100 MAIN ST SPC 4", None, "100 Main Street, Space 4, City, MO 64055"),
        (
            "100 MAIN ST PH",
            None,
            "100 Main Street, Penthouse, City, MO 64055",
        ),
        ("1 BUILDING-WAY", None, "1 Building-Way, City, MO 64055"),
        (
            "100 BUILDING-WAY STE-2",
            None,
            "100 Building-Way, Suite 2, City, MO 64055",
        ),
        ("100 MAIN ST STE A B", None, "100 Main Street, Suite A B, City, MO 64055"),
        ("100 MAIN ST STE A-B", None, "100 Main Street, Suite A-B, City, MO 64055"),
        (
            "100 MAIN ST STE AB-301",
            None,
            "100 Main Street, Suite AB-301, City, MO 64055",
        ),
        (
            "100 MAIN ST STE ABC-1D",
            None,
            "100 Main Street, Suite ABC-1D, City, MO 64055",
        ),
        (
            "100 MAIN ST STE 1",
            "100 Main Street Suite 1",
            "100 Main Street, Suite 1, City, MO 64055",
        ),
        (
            "100 MAIN ST STE 301",
            "301",
            "100 Main Street, Suite 301, City, MO 64055",
        ),
    ),
)
def test_v2_renderer_handles_unit_and_punctuation_boundaries(
    first_line: str,
    second_line: str | None,
    expected: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        second_line,
        "CITY",
        "MO",
        "64055",
        "US",
    ) == expected


def test_v2_renderer_treats_explicit_non_ascii_country_as_non_us() -> None:
    assert render_formatted_address_v2(
        "1 ST",
        None,
        "北京",
        None,
        "100000",
        "中国",
    ) == "1 St, 北京, 100000, 中国"


@pytest.mark.parametrize(
    ("first_line", "second_line", "expected"),
    (
        ("STRASSE STE-301", None, "Strasse Ste-301, Berlin, 10115, DE"),
        (
            "İSTANBUL SUITE 1",
            "SUITE 1",
            "İstanbul Suite 1, Berlin, 10115, DE",
        ),
    ),
)
def test_v2_renderer_preserves_non_us_unit_punctuation(
    first_line: str,
    second_line: str | None,
    expected: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        second_line,
        "BERLIN",
        None,
        "10115",
        "DE",
    ) == expected


def test_v2_renderer_cleans_locality_abbreviation_punctuation() -> None:
    assert render_formatted_address_v2(
        "100 MAIN ST",
        None,
        "FT. WORTH,",
        "TX,",
        "76102,",
        "US,",
    ) == "100 Main Street, Fort Worth, TX 76102"


@pytest.mark.parametrize(
    ("first_line", "city_name", "expected"),
    (
        (
            "123 NE 1ST ST NW",
            "O'FALLON",
            "123 NE 1st Street NW, O'Fallon, MO 63366",
        ),
        (
            "P.O. BOX 42",
            "ST LOUIS",
            "PO Box 42, St Louis, MO 63366",
        ),
        (
            "1 US HWY 101",
            "SAN JOSÉ",
            "1 US Highway 101, San José, MO 63366",
        ),
    ),
)
def test_v2_renderer_polishes_common_us_address_tokens(
    first_line: str,
    city_name: str,
    expected: str,
) -> None:
    assert render_formatted_address_v2(
        first_line,
        None,
        city_name,
        "MO",
        "63366",
        "USA",
    ) == expected
