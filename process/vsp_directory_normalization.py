# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Map public VSP office and doctor records to directory resources."""

from collections import Counter

from process.provider_directory_projection_fhir_values import is_valid_npi

_NETWORK_NAMES = {"1000": "VSP Signature", "2000": "VSP Choice", "4000": "VSP Advantage"}
_PLAN_NET = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"


class VSPDirectoryNormalizationError(ValueError):
    """A static reason for rejecting a malformed or ambiguous office capture."""


class VSPOfficeResources(list[dict]):
    """Selected resources and the coverage accounting for this office."""

    def __init__(self, *, outside_profile_count: int, outside_summary_count: int, other_network_ids: set[str]):
        super().__init__()
        self.outside_profile_count = outside_profile_count
        self.outside_summary_count = outside_summary_count
        self.other_network_ids = tuple(sorted(other_network_ids))


def vsp_network_organizations() -> tuple[dict, ...]:
    """Declare the three public directory networks; they are not insurance products."""
    return tuple(
        {
            "resourceType": "Organization",
            "id": f"vsp-network-{code}",
            "meta": {"profile": [_PLAN_NET + "plannet-Network"]},
            "identifier": [{"system": "urn:vsp:network-id", "value": code}],
            "type": [
                {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "ntwk"}]}
            ],
            "name": name,
        }
        for code, name in _NETWORK_NAMES.items()
    )


def _public_id(record, field):
    public_id = record.get(field) if isinstance(record, dict) else None
    if type(public_id) is not int or not 0 < public_id < 2**63:
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_public_id")
    return public_id


def _by_doctor(records, id_field):
    if not isinstance(records, list):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_doctor_list")
    records_by_id = {}
    for record in records:
        doctor_id = _public_id(record, id_field)
        if doctor_id in records_by_id:
            raise VSPDirectoryNormalizationError("vsp_directory_duplicate_doctor_id")
        records_by_id[doctor_id] = record
    return records_by_id


def _network_ids(values):
    if not isinstance(values, list):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_network_ids")
    for code in values:
        if not isinstance(code, str) or not 0 < len(code) <= 64 or not code.isprintable() or code.strip() != code:
            raise VSPDirectoryNormalizationError("vsp_directory_invalid_network_ids")
    return set(values)


def _profile_networks(profile):
    codes = _network_ids(profile.get("networkIds"))
    if "providerNetworks" in profile:
        entries = profile["providerNetworks"]
        if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
            raise VSPDirectoryNormalizationError("vsp_directory_invalid_network_ids")
        if _network_ids([entry.get("networkId") for entry in entries]) != codes:
            raise VSPDirectoryNormalizationError("vsp_directory_conflicting_network_ids")
    return codes


def _text(record, field):
    value = record.get(field)
    if value is None:
        return ""
    if not isinstance(value, str) or len(value) > 2048:
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_text")
    return value.strip()


def _coordinate(address, key, limit):
    value = address.get(key)
    if type(value) not in (int, float) or not -limit <= value <= limit:
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_position")
    return value


def _location(office, office_id):
    address = office.get("addr")
    if not isinstance(address, dict):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_address")
    lines = [_text(address, "street1")]
    if line_two := _text(address, "street2"):
        lines.append(line_two)
    address_map = {
        "city": _text(address, "city"),
        "state": _text(address, "stateCode"),
        "postalCode": _text(address, "zipCode"),
    }
    if not lines[0] or not all(address_map.values()):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_address")
    if country := _text(address, "countryCode"):
        address_map["country"] = country
    if county := _text(address, "countyName"):
        address_map["district"] = county
    location_map = {
        "resourceType": "Location",
        "id": f"vsp-office-{office_id}",
        "identifier": [{"system": "urn:vsp:office-id", "value": str(office_id)}],
        "address": {"line": lines, **address_map},
        "position": {
            "latitude": _coordinate(address, "latitude", 90),
            "longitude": _coordinate(address, "longitude", 180),
        },
    }
    if name := _text(office, "name"):
        location_map["name"] = name
    if phone := _text(office, "phone"):
        location_map["telecom"] = [{"system": "phone", "value": phone}]
    return location_map


def _practitioner(summary, profile, npi, doctor_id):
    payload = {
        "resourceType": "Practitioner",
        "id": f"vsp-npi-{npi}",
        "identifier": [{"system": _NPI_SYSTEM, "value": npi}, {"system": "urn:vsp:doctor-id", "value": str(doctor_id)}],
    }
    name_map = {}
    if family := _text(profile, "lastName") or _text(summary, "last"):
        name_map["family"] = family
    given_names = [_text(profile, "firstName") or _text(summary, "first"), _text(summary, "middle")]
    if any(given_names):
        name_map["given"] = [part for part in given_names if part]
    if name_map:
        payload["name"] = [name_map]
    return payload


def _selected_resources(doctor_id, office_id, summary, profile, networks, npi_counts):
    npi = summary.get("npi")
    if not isinstance(npi, str) or len(npi) != 10 or not is_valid_npi(npi):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_npi")
    if npi_counts[npi] != 1 or profile.get("npi") not in (None, npi):
        raise VSPDirectoryNormalizationError("vsp_directory_conflicting_npi")
    return _practitioner(summary, profile, npi, doctor_id), {
        "resourceType": "PractitionerRole",
        "id": f"vsp-role-{doctor_id}-{office_id}-{npi}",
        "practitioner": {"reference": f"Practitioner/vsp-npi-{npi}"},
        "location": [{"reference": f"Location/vsp-office-{office_id}"}],
        "extension": [
            {
                "url": _PLAN_NET + "network-reference",
                "valueReference": {"reference": f"Organization/vsp-network-{code}"},
            }
            for code in sorted(networks)
        ],
    }


def normalize_vsp_office(office: dict, *, profiles: list[dict]) -> VSPOfficeResources:
    """Join summaries to this office's profiles by public doctor ID, never by name."""
    office_id = _public_id(office, "pid")
    summaries = _by_doctor(office.get("providerSummaries"), "pid")
    doctors = _by_doctor(profiles, "doctorPublicId")
    networks_by_doctor = {doctor_id: _profile_networks(profile) for doctor_id, profile in doctors.items()}
    selected_by_doctor = {
        doctor_id: members
        for doctor_id, codes in networks_by_doctor.items()
        if (members := codes & _NETWORK_NAMES.keys())
    }
    if summaries.keys() - doctors.keys() or selected_by_doctor.keys() - summaries.keys():
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    resources = VSPOfficeResources(
        outside_profile_count=len(doctors) - len(selected_by_doctor),
        outside_summary_count=len(summaries) - len(selected_by_doctor),
        other_network_ids={
            code for codes in networks_by_doctor.values() for code in codes if code not in _NETWORK_NAMES
        },
    )
    if selected_by_doctor:
        resources.append(_location(office, office_id))
        npi_counts = Counter(npi for summary in summaries.values() if isinstance(npi := summary.get("npi"), str))
        for doctor_id, codes in sorted(selected_by_doctor.items()):
            resources.extend(
                _selected_resources(doctor_id, office_id, summaries[doctor_id], doctors[doctor_id], codes, npi_counts)
            )
    return resources
