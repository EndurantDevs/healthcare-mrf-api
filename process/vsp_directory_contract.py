# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publisher identities and bounded request contracts for the VSP directory."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date
from urllib.parse import parse_qs, urlsplit

API_BASE = "https://api.vspglobal.com/"
ENDPOINT_SCOPE = API_BASE.rstrip("/")
SEARCH_URL = API_BASE + "fadbusiness-api/publiclocationsummarybusextensions"
PROFILE_PATH = "/provider-web/publicdoctorprofile"
OFFICE_DOCTORS_INVALID = "vsp_office_doctors_invalid"
ADAPTER_ID = "vsp-directory-json-v1"
TRANSPORT = "official_directory_json"
RESOURCE_TYPES = (
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)
NETWORKS = {"1000": "Signature", "2000": "Choice", "4000": "Advantage"}
MAX_LOCATIONS = 50
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MIN_REQUEST_INTERVAL = 1.0


def canonical_json(value):
    """Encode request and capture identities without nonfinite JSON numbers."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


SOURCE_ID = (
    "pdfhir_"
    + hashlib.sha256(
        canonical_json(
            {
                "adapter": ADAPTER_ID,
                "api_base": API_BASE,
                "publisher": "https://www.vsp.com/",
            }
        ).encode()
    ).hexdigest()[:24]
)


class VSPDirectoryError(ValueError):
    """Expose static source errors without URLs, credentials or provider data."""


def require(condition, code):
    """Reject invalid source evidence before it can enter a candidate dataset."""
    if not condition:
        raise VSPDirectoryError(code)


def public_id(value):
    """Keep the publisher's numeric identifiers exact across JSON representations."""
    require(type(value) in (int, str), "vsp_public_id_invalid")
    text = str(value)
    require(re.fullmatch(r"[0-9]{1,32}", text) is not None, "vsp_public_id_invalid")
    return text


def request_key(method, url, payload=None):
    """Bind response reuse to the exact approved method, URL and request body."""
    validated_url(url, method=method)
    return hashlib.sha256(canonical_json([method, url, payload]).encode()).hexdigest()


def validated_url(url, *, method):
    """Allow only publisher search, exact-office and linked-profile requests."""
    require(type(url) is str and not re.search(r"[\s\\%#]", url), "vsp_request_url_invalid")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and parsed.netloc == "api.vspglobal.com", "vsp_request_url_invalid")
    search_path = urlsplit(SEARCH_URL).path
    if method == "POST":
        require(url == SEARCH_URL, "vsp_request_url_invalid")
    elif method == "GET" and parsed.path == PROFILE_PATH:
        query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        require(
            set(query) == {"officepublicid", "asofdate"} and all(len(entries) == 1 for entries in query.values()),
            "vsp_profile_url_invalid",
        )
        public_id(query["officepublicid"][0])
        try:
            observed_on = date.fromisoformat(query["asofdate"][0])
        except TypeError, ValueError:
            raise VSPDirectoryError("vsp_profile_url_invalid") from None
        require(observed_on.isoformat() == query["asofdate"][0], "vsp_profile_url_invalid")
    elif method == "GET" and parsed.path.startswith(search_path + "/"):
        public_id(parsed.path[len(search_path) + 1 :])
        require(set(parse_qs(parsed.query)) <= {"networkNames", "productNetworkIDs"}, "vsp_office_url_invalid")
    else:
        raise VSPDirectoryError("vsp_request_url_invalid")
    return url


def profile_url(office):
    """Require the linked doctor response to belong to this exact office."""
    require(type(office) is dict, "vsp_office_invalid")
    link = office.get("locationDrProfileLink")
    require(type(link) is dict and type(link.get("href")) is str, "vsp_profile_url_invalid")
    url = validated_url(link["href"], method="GET")
    query = parse_qs(urlsplit(url).query)
    require(query["officepublicid"] == [public_id(office.get("pid"))], "vsp_profile_office_mismatch")
    return url


def office_position(office):
    """Validate source coordinates without deriving a geocode from an address."""
    require(type(office) is dict and type(office.get("addr")) is dict, "vsp_office_invalid")
    address = office["addr"]
    values = []
    for name, bound in (("latitude", 90), ("longitude", 180)):
        value = address.get(name)
        require(type(value) in (int, float, str), "vsp_office_position_invalid")
        try:
            number = float(value)
        except TypeError, ValueError, OverflowError:
            raise VSPDirectoryError("vsp_office_position_invalid") from None
        require(math.isfinite(number) and -bound <= number <= bound, "vsp_office_position_invalid")
        values.append(number)
    return tuple(values)


def search_result(payload):
    """Reject HTTP-200 source errors, clamped pages and ambiguous office counts."""
    require(type(payload) is dict and not payload.get("errors"), "vsp_search_source_error")
    total, locations = payload.get("totalCounts"), payload.get("locations")
    require(type(total) is int and total >= 0 and type(locations) is list, "vsp_search_response_invalid")
    identifiers = [public_id(x.get("pid")) for x in locations if type(x) is dict]
    require(
        len(identifiers) == len(locations)
        and len(set(identifiers)) == len(identifiers)
        and len(locations) <= MAX_LOCATIONS
        and len(locations) <= total,
        "vsp_search_response_invalid",
    )
    require(len(locations) == min(total, MAX_LOCATIONS), "vsp_search_response_incomplete")
    validated_offices = []
    for office in locations:
        office_position(office)
        profile_url(office)
        summaries = office.get("providerSummaries", [])
        require(type(summaries) is list, OFFICE_DOCTORS_INVALID)
        # Search can omit summaries; the linked profile still determines membership.
        validated_offices.append(
            office if "providerSummaries" in office else {**office, "providerSummaries": summaries}
        )
    return total, validated_offices
