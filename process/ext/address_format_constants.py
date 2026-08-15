# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Static data and token primitives for human-readable addresses."""

from __future__ import annotations

import re

from process.ext.address_pub28 import (
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
)


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
_TOKEN_RE = re.compile(r"[^\W_]+|_+|[^\w]+", re.UNICODE)
_ORDINAL_RE = re.compile(r"^[0-9]+(?:ST|ND|RD|TH)$", re.IGNORECASE)
_SAFE_MIXED_CASE_RE = re.compile(r"^[A-Z][a-z]+(?:[A-Z][a-z]+)*$")
_DEFAULT_US_COUNTRIES = {
    "US",
    "USA",
    "UNITEDSTATES",
    "UNITEDSTATESOFAMERICA",
}
_DIRECTION_DISPLAY = {
    "N": "North",
    "S": "South",
    "E": "East",
    "W": "West",
    "NE": "NE",
    "NW": "NW",
    "SE": "SE",
    "SW": "SW",
}
_CITY_PREFIX_DISPLAY = {**_DIRECTION_DISPLAY, "FT": "Fort", "MT": "Mount"}
ADDRESS_UNIT_PREFIX_DISPLAY = {
    "apt": "Apartment",
    "bldg": "Building",
    "bsmt": "Basement",
    "dept": "Department",
    "fl": "Floor",
    "frnt": "Front",
    "hngr": "Hangar",
    "key": "Key",
    "lbby": "Lobby",
    "lot": "Lot",
    "lowr": "Lower",
    "ofc": "Office",
    "ph": "Penthouse",
    "pier": "Pier",
    "rear": "Rear",
    "rm": "Room",
    "side": "Side",
    "slip": "Slip",
    "spc": "Space",
    "ste": "Suite",
    "stop": "Stop",
    "trlr": "Trailer",
    "unit": "Unit",
    "uppr": "Upper",
}
_UNIT_PREFIX_DISPLAY = tuple(
    sorted(ADDRESS_UNIT_PREFIX_DISPLAY.items(), key=lambda pair: -len(pair[0]))
)
_ADDRESS_ACRONYMS = frozenset(
    {
        "APO",
        "CMR",
        "CR",
        "DPO",
        "FM",
        "FPO",
        "HC",
        "I",
        "PMB",
        "PO",
        "PSC",
        "RR",
        "SH",
        "SR",
        "US",
    }
)
_STREET_SUFFIX_DISPLAY = {
    "ALY": "Alley",
    "AVE": "Avenue",
    "BLVD": "Boulevard",
    "CIR": "Circle",
    "CT": "Court",
    "CTR": "Center",
    "DR": "Drive",
    "EXPY": "Expressway",
    "FWY": "Freeway",
    "HWY": "Highway",
    "LN": "Lane",
    "PKWY": "Parkway",
    "PL": "Place",
    "PLZ": "Plaza",
    "RD": "Road",
    "RTE": "Route",
    "SQ": "Square",
    "ST": "Street",
    "TER": "Terrace",
    "TPKE": "Turnpike",
    "TRL": "Trail",
    "WAY": "Way",
}
_PO_BOX_RE = re.compile(
    r"^(?:P\s*\.?\s*O\s*\.?|POST\s+OFFICE)\s+BOX\.?(?:\s*#\s*|\s+|$)",
    re.IGNORECASE,
)
_COMPOUND_DIRECTION_RE = re.compile(
    r"(?<![A-Za-z])([NS])(?:\s*\.\s*|\s+)([EW])\.?(?![A-Za-z])",
    re.IGNORECASE,
)
_UNIT_DESIGNATOR_PATTERN = "|".join(
    re.escape(designator)
    for designator in sorted(PUB28_UNIT_DESIGNATOR_MAP, key=len, reverse=True)
)
_UNIT_MARKER_SEPARATOR_RE = re.compile(
    rf"(^|[\s,])({_UNIT_DESIGNATOR_PATTERN})[-/]([#A-Za-z0-9])",
    re.IGNORECASE,
)
_BOX_HASH_RE = re.compile(r"\bBOX\s*#\s*([A-Za-z0-9])", re.IGNORECASE)
_AMBIGUOUS_UNIT_WORDS = frozenset(
    {
        "apartment",
        "basement",
        "building",
        "department",
        "floor",
        "front",
        "hanger",
        "key",
        "lobby",
        "lot",
        "lower",
        "office",
        "penthouse",
        "pier",
        "rear",
        "room",
        "side",
        "slip",
        "space",
        "stop",
        "suite",
        "trailer",
        "unit",
        "upper",
    }
)


def _display_street_suffix(token: str) -> str | None:
    canonical_suffix = PUB28_STREET_SUFFIX_MAP.get(token.lower())
    if canonical_suffix is None:
        return None
    return _STREET_SUFFIX_DISPLAY.get(canonical_suffix.upper())


def _humanize_word(token: str) -> str:
    upper_token = token.upper()
    if _ORDINAL_RE.fullmatch(token):
        return upper_token[:-2] + upper_token[-2:].lower()
    if any(character.isdigit() for character in token) and any(
        character.isalpha() for character in token
    ):
        return upper_token
    if upper_token in _ADDRESS_ACRONYMS:
        return upper_token
    if _SAFE_MIXED_CASE_RE.fullmatch(token):
        return token
    return token[:1].upper() + token[1:].lower()


def _humanize_text(text: str) -> str:
    return "".join(
        _humanize_word(token)
        if any(character.isalnum() for character in token)
        else token
        for token in _TOKEN_RE.findall(text)
    )


def _humanize_unit_value(unit_value: str) -> str:
    upper_value = unit_value.upper()
    if _ORDINAL_RE.fullmatch(unit_value):
        return upper_value[:-2] + upper_value[-2:].lower()
    if any(character.isdigit() for character in unit_value) and any(
        character.isalpha() for character in unit_value
    ):
        return upper_value
    if upper_value in _ADDRESS_ACRONYMS:
        return upper_value
    if _SAFE_MIXED_CASE_RE.fullmatch(unit_value):
        return unit_value
    return _humanize_text(unit_value)
