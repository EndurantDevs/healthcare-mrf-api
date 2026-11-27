# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normalize network-scoped EyeMed offices and their listed practitioners."""

import hashlib
import re

from process.nomi_directory_normalization import _position, _text
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import canonical_json

LOCATION_ARRAYS = ("zipClassSortedLocations", "independentLocations", "retailLocations", "preferredLocations")
_PLAN_NET = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"


def _identifier(row, field):
    text_value = row.get(field) if isinstance(row, dict) else None
    if type(text_value) not in (int, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", str(text_value)):
        raise ValueError("eyemed_directory_publisher_id_invalid")
    return str(text_value)


def _id(kind, *keys):
    digest = hashlib.sha256(canonical_json(keys).encode()).hexdigest()[:40]
    return f"eyemed-{kind}-{digest}"


def network_resource(row):
    """Represent a publisher network as a Plan-Net organization."""
    code = _identifier(row, "networkId")
    name = _text(row, "networkName")
    if not re.fullmatch(r"[0-9]{1,32}", code) or not name:
        raise ValueError("eyemed_directory_network_invalid")
    return {
        "resourceType": "Organization",
        "id": f"eyemed-network-{code}",
        "active": True,
        "meta": {"profile": [_PLAN_NET + "plannet-Network"]},
        "identifier": [{"system": "urn:eyemed:network-id", "value": code}],
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "ntwk"}]}],
        "name": name,
    }


def _location(site, site_id):
    address = site.get("address")
    if not isinstance(address, dict):
        raise ValueError("eyemed_directory_address_invalid")
    address_by_field = {
        dest: text_value
        for field, dest in (("city", "city"), ("county", "district"), ("state", "state"), ("zip", "postalCode"))
        if (text_value := _text(address, field))
    }
    if lines := [text_value for key in ("streetAddress1", "streetAddress2") if (text_value := _text(address, key))]:
        address_by_field["line"] = lines
    location_by_field = {
        "resourceType": "Location",
        "id": _id("loc", site_id),
        "status": "active",
        **({"address": address_by_field} if address_by_field else {}),
        "identifier": [{"system": "urn:eyemed:location-id", "value": site_id}],
    }
    if name := _text(site, "businessName"):
        location_by_field["name"] = name
    if position := _position(site):
        location_by_field["position"] = position
    if contact_points := [
        {"system": kind, "value": text_value}
        for key, kind in (("phoneNumber", "phone"), ("faxNumber", "fax"), ("locationUrl", "url"))
        if (text_value := _text(site, key))
    ]:
        location_by_field["telecom"] = contact_points
    return location_by_field


def _doctor_resources(doctor, code, site_id, location_ref_by_field, network_ref_by_field):
    provider_id = _identifier(doctor, "providerId")
    identifier = _id("pr", provider_id)
    ids = [{"system": "urn:eyemed:provider-id", "value": provider_id}]
    npi = _text(doctor, "npiLicenseNumber")
    if is_valid_npi(npi):
        ids.append({"system": _NPI_SYSTEM, "value": npi})
    given_names = [text_value for key in ("firstName", "middleName") if (text_value := _text(doctor, key))]
    family = _text(doctor, "lastName")
    practitioner_by_field = {"resourceType": "Practitioner", "id": identifier, "active": True, "identifier": ids}
    if family or given_names:
        practitioner_by_field["name"] = [
            {**({"family": family} if family else {}), **({"given": given_names} if given_names else {})}
        ]
    if designation := _text(doctor, "professionalDesignation"):
        practitioner_by_field["qualification"] = [{"code": {"text": designation}}]
    role_by_field = {
        "resourceType": "PractitionerRole",
        "id": _id("role", code, site_id, provider_id),
        "active": True,
        "practitioner": {"reference": "Practitioner/" + identifier},
        "location": [location_ref_by_field],
        "extension": [{"url": _PLAN_NET + "network-reference", "valueReference": network_ref_by_field}],
    }
    accepting = _text(doctor, "acceptingNewPatients").lower()
    if accepting in {"yes", "no", "y", "n"}:
        role_by_field["extension"].append(
            {
                "url": _PLAN_NET + "newpatients",
                "extension": [
                    {
                        "url": "acceptingPatients",
                        "valueCodeableConcept": {
                            "coding": [{"code": "newpt" if accepting in {"yes", "y"} else "nopt"}]
                        },
                    },
                    {"url": "fromNetwork", "valueReference": network_ref_by_field},
                ],
            }
        )
    return practitioner_by_field, role_by_field


def site_resources(site, code):
    """Preserve every office and clinician membership within the requested network."""
    site_id = _identifier(site, "locationId")
    location_by_field = _location(site, site_id)
    yield location_by_field
    org_id = _id("org", site_id)
    network_ref_by_field = {"reference": "Organization/eyemed-network-" + code}
    location_ref_by_field = {"reference": "Location/" + location_by_field["id"]}
    organization_by_field = {
        "resourceType": "Organization",
        "id": org_id,
        "active": True,
        "identifier": [{"system": "urn:eyemed:location-id", "value": site_id}],
    }
    if "name" in location_by_field:
        organization_by_field["name"] = location_by_field["name"]
    yield organization_by_field
    yield {
        "resourceType": "OrganizationAffiliation",
        "id": _id("aff", code, site_id),
        "active": True,
        "participatingOrganization": {"reference": "Organization/" + org_id},
        "location": [location_ref_by_field],
        "network": [network_ref_by_field],
    }
    doctors = site.get("doctors")
    if not isinstance(doctors, list):
        raise ValueError("eyemed_directory_doctor_list_invalid")
    for doctor in doctors:
        yield from _doctor_resources(doctor, code, site_id, location_ref_by_field, network_ref_by_field)
