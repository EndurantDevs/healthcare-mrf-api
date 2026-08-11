# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic display rendering for structured address components.

Version one intentionally performs no address parsing, fuzzy matching, source
selection, or geocoding. Callers must pass the structured components chosen by
their offline canonicalization/materialization path.
"""

from __future__ import annotations

import re
import unicodedata


ADDRESS_FORMAT_VERSION = 1
ADDRESS_FORMAT_SOURCE = "canonical_v1"
ADDRESS_FORMAT_MAX_LENGTH = 1024

_DISPLAY_SPACE_CODEPOINTS = (
    "\u00a0",
    "\u202f",
    "\u2009",
    "\u200a",
    "\u2007",
    "\u2006",
    "\u2005",
    "\u2004",
    "\u2003",
    "\u2002",
    "\u2001",
    "\u2000",
)
_DISPLAY_SPACE_TRANSLATION = str.maketrans(
    {codepoint: " " for codepoint in _DISPLAY_SPACE_CODEPOINTS}
)
_ASCII_WHITESPACE_RE = re.compile(r"[ \t\n\r\f\v]+")
_US_ZIP9_RE = re.compile(r"^([0-9]{5})([0-9]{4})$")
_US_ZIP4_SEPARATED_RE = re.compile(r"^([0-9]{5})[- ]([0-9]{4})$")
_TRUNCATION_SUFFIX = " \t\n\r,;"


def _clean_component(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFC", str(value)).translate(
        _DISPLAY_SPACE_TRANSLATION
    )
    cleaned = _ASCII_WHITESPACE_RE.sub(" ", normalized).strip()
    return cleaned or None


def _us_postal_code(value: str | None) -> str | None:
    if value is None:
        return None
    match = _US_ZIP9_RE.fullmatch(value) or _US_ZIP4_SEPARATED_RE.fullmatch(
        value
    )
    if match is None:
        return value
    return f"{match.group(1)}-{match.group(2)}"


def _bounded_display(value: str) -> str | None:
    if len(value) <= ADDRESS_FORMAT_MAX_LENGTH:
        return value
    bounded = value[:ADDRESS_FORMAT_MAX_LENGTH].rstrip(_TRUNCATION_SUFFIX)
    return bounded or None


def _display_second_line(
    line_one: str | None,
    line_two: str | None,
) -> str | None:
    """Suppress only an exact normalized unit already present in line one."""
    if line_one is None or line_two is None:
        return line_two
    if line_one == line_two:
        return None
    suffix_start = len(line_one) - len(line_two)
    if (
        suffix_start > 0
        and line_one.endswith(line_two)
        and line_one[suffix_start - 1] in " ,;"
    ):
        return None
    return line_two


def render_formatted_address_v1(
    first_line: str | None,
    second_line: str | None,
    city_name: str | None,
    state_name: str | None,
    postal_code: str | None,
    country_code: str | None,
) -> str | None:
    """Render one stable display string from structured address components."""

    line_one = _clean_component(first_line)
    line_two = _display_second_line(
        line_one,
        _clean_component(second_line),
    )
    city = _clean_component(city_name)
    state = _clean_component(state_name)
    postal = _clean_component(postal_code)
    country = _clean_component(country_code)
    if country is not None:
        country = country.upper()
    if country in (None, "US"):
        postal = _us_postal_code(postal)

    state_postal = " ".join(
        component for component in (state, postal) if component
    ) or None
    locality = ", ".join(
        component for component in (city, state_postal) if component
    ) or None
    displayed_country = country if country not in (None, "US") else None
    rendered = ", ".join(
        component
        for component in (line_one, line_two, locality, displayed_country)
        if component
    )
    if not rendered:
        return None
    return _bounded_display(rendered)
