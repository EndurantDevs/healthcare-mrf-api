# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Parse the national finder JSON response without inferring plan participation."""

import json
import re

from process.delta_dental_directory_capture import RESULT_CAP, FinderCard, FinderPage
from process.provider_directory_projection_fhir_values import is_valid_npi

SOURCE_ID = "delta-dental-national-finder"
SOURCE_ENDPOINT = "https://portal.deltadental.com/portal/api/v1/providers/dentistSearch"
SELECTOR_VALUES = {
    "Delta Dental PPO": "PPO",
    "Delta Dental Premier": "Premier",
    "Delta Dental PPO Plus Premier": "ppoPlusPremier",
    "Delta Dental Medicare Advantage": "MedicareAdvantage",
    "DeltaCare USA": "Deltacare",
    "Delta Dental Patient Direct": "PatientDirect",
    "All networks": "NationalNetworks",
}
_DEFAULT_FILTERS = {
    "sortType": 0,
    "websiteMc": "DDPA",
    "specialtyCode": None,
    "lastName": "",
    "gender": "",
    "secondaryLanguageType": "",
    "raceEthnicity": None,
    "businessName": "",
    "acceptsNewPatients": None,
    "handicappedAccessible": None,
    "serviceIntellectuallyDisabledChildren": None,
    "serviceIntellectuallyDisabledAdults": None,
    "servicePhysicallyDisabledChildren": None,
    "servicePhysicallyDisabledAdults": None,
    "publicTransitAvailable": None,
    "offersTelehealthServices": None,
    "maximumNumberOfRecordsToReturn": RESULT_CAP,
    "extendedHoursType": "",
}


def finder_payload(search):
    """Serialize one exact unfiltered ZIP, network, and radius query."""
    radius = search.radius_miles
    return {
        **_DEFAULT_FILTERS,
        "address": search.zip_code,
        "productType": SELECTOR_VALUES[search.selector],
        "searchRadiusMiles": int(radius) if radius == int(radius) else radius,
    }


def _text(value):
    if isinstance(value, str):
        return " ".join(value.split()) or None
    return None


def canonical_office_zip(value):
    """Keep ZIP5 and spell either ZIP+4 format with a hyphen."""
    postal = _text(value)
    if re.fullmatch(r"[0-9]{9}", postal or ""):
        return f"{postal[:5]}-{postal[5:]}"
    return postal if re.fullmatch(r"[0-9]{5}(?:-[0-9]{4})?", postal or "") else None


def _address(office):
    location = office.get("location")
    if not isinstance(location, dict):
        return None
    street = _text(location.get("streetAddress"))
    city = _text(location.get("city"))
    state = _text(location.get("stateType"))
    postal = canonical_office_zip(location.get("zipCode"))
    if not street or not city or not re.fullmatch("[A-Z]{2}", state or "") or not postal:
        return None
    second_line = _text(location.get("streetAddressLine2"))
    return ", ".join(part for part in (street, second_line, city, f"{state} {postal}") if part)


def _publisher_number(value):
    return str(value) if type(value) is int and value > 0 else None


def _npi(value):
    return value if isinstance(value, str) and is_valid_npi(value) else None


def _networks(products):
    if not isinstance(products, list):
        return (), frozenset({"invalid_product_observation"})
    labels = []
    gaps = set()
    for product in products:
        label = _text(product.get("productType")) if isinstance(product, dict) else None
        if not label:
            gaps.add("invalid_product_observation")
            continue
        if label not in labels:
            labels.append(label)
    return tuple(labels), gaps


def _office_card(record):
    if not isinstance(record, dict) or not isinstance(record.get("provider"), dict):
        return None, {"invalid_provider_office_observation"}
    provider = record["provider"]
    first_name, last_name = _text(provider.get("firstName")), _text(provider.get("lastName"))
    if not first_name or not last_name:
        return None, {"invalid_provider_office_observation"}
    office = record.get("serviceOffice")
    office = office if isinstance(office, dict) else {}
    labels, gaps = _networks(record.get("products"))
    provider_npi = _npi(record.get("providerNPI"))
    if provider_npi is None:
        gaps = set(gaps) | {"provider_npi_unavailable"}
    return FinderCard(
        dentist=f"{first_name} {last_name}",
        office=_text(office.get("businessName")),
        address=_address(office),
        network_labels=labels,
        office_id=_publisher_number(office.get("serviceOfficeNumber")),
        practitioner_npi=provider_npi,
        specialty=_text(provider.get("specialtyType")),
        provider_office_id=_publisher_number(record.get("providerServiceOfficeId")),
        office_npi=_npi(record.get("serviceOfficeNPI")),
    ), gaps


def parse_finder_json_page(raw, search, number):
    """Retain one publisher response, including positive rows in an incomplete window."""
    try:
        response = json.loads(raw)
    except ValueError, RecursionError:
        raise ValueError("delta_dental_finder_response_json_invalid") from None
    rows = response.get("providerResults") if isinstance(response, dict) else None
    if not isinstance(rows, list) or len(rows) > RESULT_CAP:
        raise ValueError("delta_dental_finder_response_envelope_invalid")
    cards = []
    gaps = set()
    for record in rows:
        card, row_gaps = _office_card(record)
        if card is not None:
            cards.append(card)
        gaps.update(row_gaps)
    return FinderPage(search, number, len(rows), tuple(cards), single_response=True, gaps=frozenset(gaps))
