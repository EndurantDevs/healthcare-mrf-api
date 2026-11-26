# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project Nomi directory search observations to the shared FHIR resource model."""

import hashlib
import math
import re

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import canonical_json

NETWORK_ID = "nomi-health-network"
NETWORK_REFERENCE = {"reference": f"Organization/{NETWORK_ID}"}
NETWORK = {
    "resourceType": "Organization",
    "id": NETWORK_ID,
    "active": True,
    "name": "Nomi Health Network",
    "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "ntwk"}]}],
}
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
_PROVIDER_TYPES = {1: "Physical", 2: "Mobile", 3: "Virtual"}


def _text(row, field):
    value = row.get(field)
    return " ".join(value.split()) if type(value) is str else ""


def _source_id(row, field):
    value = row.get(field)
    if type(value) not in (str, int) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", str(value)):
        raise ValueError("nomi_directory_row_identity_invalid")
    return str(value)


def _id(kind, *values):
    digest = hashlib.sha256(canonical_json(values).encode()).hexdigest()[:40]
    return f"nomi-{kind}-{digest}"


def _npi(row):
    value = row.get("npiNumber")
    value = str(value).strip() if type(value) in (str, int) else ""
    return value if is_valid_npi(value) else None


def _position(row):
    try:
        lat, lon = float(row["latitude"]), float(row["longitude"])
    except (KeyError, TypeError, ValueError, OverflowError) as _error:
        return None
    if math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180 and (lat or lon):
        return {"latitude": lat, "longitude": lon}
    return None


def _taxonomy(row):
    label = _text(row, "taxonomy")
    taxonomy_id = row.get("taxonomyId")
    specialty_id = row.get("specialtyId")
    if type(taxonomy_id) is int and taxonomy_id >= 0:
        coding_by_field = {"system": "urn:nomi:taxonomy-id", "code": str(taxonomy_id)}
        if label:
            coding_by_field["display"] = label
        return (taxonomy_id, specialty_id), [{"coding": [coding_by_field], **({"text": label} if label else {})}]
    return (label.casefold(), specialty_id), [{"text": label}] if label else []


def _is_virtual(value):
    if value is True or type(value) in (str, int) and str(value).strip().lower() in {"1", "true", "yes", "y"}:
        return True
    if (
        value is None
        or value is False
        or type(value) in (str, int)
        and str(value).strip().lower() in {"", "0", "false", "no", "n"}
    ):
        return False
    raise ValueError("nomi_directory_virtual_flag_invalid")


def _location(row, location_id, virtual):
    identifier = _id("location", location_id, virtual)
    payload = {"resourceType": "Location", "id": identifier, "status": "active"}
    if virtual:
        payload["physicalType"] = {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/location-physical-type", "code": "vi"}]
        }
    address_by_field = {
        fhir_field: value
        for fhir_field, source_field in (("city", "city"), ("state", "state"), ("postalCode", "zipCode"))
        if (value := _text(row, source_field))
    }
    if lines := [line for key in ("addressLine1", "addressLine2") if (line := _text(row, key))]:
        address_by_field["line"] = lines
    if address_by_field:
        payload["address"] = address_by_field
    if name := _text(row, "facilityName"):
        payload["name"] = name
    if position := _position(row):
        payload["position"] = position
    if phone := _text(row, "phoneNumber"):
        payload["telecom"] = [{"system": "phone", "value": phone}]
    return payload


def _accepting_patients(is_accepting_patients):
    """Keep the publisher's explicit acceptance flag on the scoped role."""
    if type(is_accepting_patients) is bool:
        return [
            {
                "url": "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/newpatients",
                "extension": [
                    {
                        "url": "acceptingPatients",
                        "valueCodeableConcept": {"coding": [{"code": "newpt" if is_accepting_patients else "nopt"}]},
                    },
                    {"url": "fromNetwork", "valueReference": NETWORK_REFERENCE},
                ],
            }
        ]
    return []


def _provider_resources(observation, location, site_key, npi, npi_identifiers, specialty):
    """Build a practitioner and a network role tied to the observed location."""
    name = _text(observation, "name")
    site_references = [{"reference": "Location/" + location["id"]}]
    provider_id = _source_id(observation, "providerId")
    provider_types = observation.get("providerType") or []
    if type(provider_types) is not list or any(type(type_id) is not int for type_id in provider_types):
        raise ValueError("nomi_directory_provider_type_invalid")
    key = (provider_id, *site_key)
    practitioner_id = f"nomi-npi-{npi}" if npi else _id("practitioner", provider_id)
    practitioner_by_field = {
        "resourceType": "Practitioner",
        "id": practitioner_id,
        "active": True,
        "identifier": npi_identifiers + [{"system": "urn:nomi:provider-id", "value": provider_id}],
        "name": [{"text": name}] if name else [],
    }
    role_by_field = {
        "resourceType": "PractitionerRole",
        "id": _id("role", practitioner_id, location["id"], site_key[-1]),
        "active": True,
        "practitioner": {"reference": "Practitioner/" + practitioner_id},
        "location": site_references,
        "network": [NETWORK_REFERENCE],
        "specialty": specialty,
        "code": [
            {
                "coding": [
                    {"system": "urn:nomi:provider-type", "code": str(type_id), "display": _PROVIDER_TYPES[type_id]}
                ]
            }
            for type_id in sorted(set(provider_types) & _PROVIDER_TYPES.keys())
        ],
    }
    if extensions := _accepting_patients(observation.get("IsAcceptNewPatient")):
        role_by_field["extension"] = extensions
    return key, (location, practitioner_by_field, role_by_field)


def _facility_resources(observation, location, site_key, npi, npi_identifiers, specialty):
    """Build one facility and its observed network affiliation."""
    name = _text(observation, "name")
    site_references = [{"reference": "Location/" + location["id"]}]
    name = _text(observation, "facilityName") or name
    if not name:
        raise ValueError("nomi_directory_facility_name_missing")
    identity = npi or name.casefold()
    key = (*site_key, identity)
    organization_id = _id("facility", site_key[0], identity)
    organization_by_field = {
        "resourceType": "Organization",
        "id": organization_id,
        "active": True,
        "identifier": npi_identifiers,
        "name": name,
    }
    affiliation_by_field = {
        "resourceType": "OrganizationAffiliation",
        "id": _id("affiliation", organization_id, location["id"], site_key[-1]),
        "active": True,
        "participatingOrganization": {"reference": "Organization/" + organization_id},
        "location": site_references,
        "network": [NETWORK_REFERENCE],
        "specialty": specialty,
    }
    return key, (location, organization_by_field, affiliation_by_field)


def normalize_row(kind, observation):
    """Return the stable row key and its linked network resources."""
    location_id = _source_id(observation, "locationId")
    is_virtual = _is_virtual(observation.get("isVirtualAddress"))
    location = _location(observation, location_id, is_virtual)
    npi = _npi(observation)
    npi_identifiers = [{"system": _NPI_SYSTEM, "value": npi}] if npi else []
    taxonomy, specialty = _taxonomy(observation)
    site_key = (location_id, is_virtual, taxonomy)
    if kind == "providers":
        return _provider_resources(observation, location, site_key, npi, npi_identifiers, specialty)
    return _facility_resources(observation, location, site_key, npi, npi_identifiers, specialty)
