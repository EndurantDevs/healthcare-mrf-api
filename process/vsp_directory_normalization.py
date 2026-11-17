# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Map public VSP office and doctor records to directory resources."""

from collections import Counter

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import NETWORKS

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
        if not isinstance(code, str) or not 0 < len(code) <= 64 or not code.isprintable() or not code.strip():
            raise VSPDirectoryNormalizationError("vsp_directory_invalid_network_ids")
    return {code.strip() for code in values}


def _profile_networks(profile):
    codes = _network_ids(profile.get("networkIds"))
    if "providerNetworks" in profile:
        entries = profile["providerNetworks"]
        if not isinstance(entries, list) or not all(isinstance(entry, dict) for entry in entries):
            raise VSPDirectoryNormalizationError("vsp_directory_invalid_network_ids")
        if _network_ids([entry.get("networkId") for entry in entries]) != codes:
            raise VSPDirectoryNormalizationError("vsp_directory_conflicting_network_ids")
    return codes


def _selected_networks(office, networks_by_doctor, search_memberships):
    selected_by_doctor = {}
    for doctor_id, codes in networks_by_doctor.items():
        members = codes & _NETWORK_NAMES.keys()
        if not members and office.get("locationType") == "Affiliate":
            members = {code for code, name in NETWORKS.items() if str(doctor_id) in search_memberships.get(name, ())}
        if members:
            selected_by_doctor[doctor_id] = members
    return selected_by_doctor


def vsp_profile_memberships(office, profiles, search_memberships):
    """Resolve profiles and NPI-backed summaries using exact office/search joins."""
    doctors = _by_doctor(profiles, "doctorPublicId")
    networks_by_doctor = {doctor_id: _profile_networks(profile) for doctor_id, profile in doctors.items()}
    retail_ids = retail_profile_ids(office, profiles)
    selected_by_doctor = _selected_networks(
        office, {did: codes for did, codes in networks_by_doctor.items() if did not in retail_ids}, search_memberships
    )
    summaries = _by_doctor(office.get("providerSummaries"), "pid")
    for doctor_id in summaries.keys() - doctors.keys():
        if is_company_summary(office, summaries[doctor_id]):
            raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
        npi = summaries[doctor_id].get("npi")
        if not isinstance(npi, str) or len(npi) != 10 or not is_valid_npi(npi):
            continue
        codes = {code for code, name in NETWORKS.items() if str(doctor_id) in search_memberships.get(name, ())}
        if codes:
            selected_by_doctor[doctor_id] = codes
    if any(is_company_summary(office, summaries.get(did, {})) for did in selected_by_doctor):
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    return selected_by_doctor


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
        "id": f"vsp-npi-{npi}" if npi else f"vsp-doctor-{doctor_id}",
        "identifier": ([{"system": _NPI_SYSTEM, "value": npi}] if npi else [])
        + [{"system": "urn:vsp:doctor-id", "value": str(doctor_id)}],
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
    if npi is None:
        npi = profile.get("npi")
    if npi is not None and not isinstance(npi, str):
        raise VSPDirectoryNormalizationError("vsp_directory_invalid_npi")
    npi = npi if npi and len(npi) == 10 and is_valid_npi(npi) else None
    if (npi is not None and npi_counts[npi] != 1) or profile.get("npi") not in (None, npi):
        raise VSPDirectoryNormalizationError("vsp_directory_conflicting_npi")
    practitioner = _practitioner(summary, profile, npi, doctor_id)
    return practitioner, {
        "resourceType": "PractitionerRole",
        "id": f"vsp-role-{doctor_id}-{office_id}" + (f"-{npi}" if npi else ""),
        "practitioner": {"reference": "Practitioner/" + practitioner["id"]},
        "location": [{"reference": f"Location/vsp-office-{office_id}"}],
        "extension": [
            {
                "url": _PLAN_NET + "network-reference",
                "valueReference": {"reference": f"Organization/vsp-network-{code}"},
            }
            for code in sorted(networks)
        ],
    }


def _name_words(record, first_field, last_field):
    return (_text(record, first_field) + " " + _text(record, last_field)).casefold().split()


def _company_office(office):
    return (
        office.get("locationType") in {"VSP", "Affiliate"}
        and _text(office, "serviceTypeCd") in {"DL", "EC", "FS"}
        and bool(_text(office, "name"))
        and bool(_text(office, "parentCompanyCd"))
        and bool(_text(office, "droStoreNumber"))
    )


def company_summary_name(summary):
    """Compare the publisher's company display without treating its suffix as a site ID."""
    return "".join(_name_words(summary, "first", "last"))


def is_company_summary(office, summary):
    """Recognize a source-typed company row independently of its display or legal name."""
    return _company_office(office) and summary.get("degree") == "NA" and bool(company_summary_name(summary))


def _company_npi(office, profile, summary):
    npi = summary.get("npi")
    if not (
        _public_id(summary, "pid") == _public_id(profile, "doctorPublicId")
        and is_company_summary(office, summary)
        and company_summary_name(summary) == "".join(_name_words(profile, "firstName", "lastName"))
        and (npi is None or isinstance(npi, str) and len(npi) == 10 and is_valid_npi(npi))
    ):
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    return npi


def retail_profile_ids(office, profiles):
    """Separate store displays from clinicians sharing the same office."""
    service = _text(office, "serviceTypeCd")
    if not _company_office(office):
        return set()
    summaries = _by_doctor(office.get("providerSummaries"), "pid")
    retail_ids = set()
    for profile in profiles:
        if _text(profile, "doctorDegreeCode") != "NA":
            continue
        did = _public_id(profile, "doctorPublicId")
        if (
            _text(profile, "serviceTypeCode") != service
            or not _name_words(profile, "firstName", "lastName")
            or profile.get("npi") is not None
        ):
            raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
        if did in summaries:
            _company_npi(office, profile, summaries[did])
        retail_ids.add(did)
    return retail_ids


def is_retail_location(office, profiles):
    """Recognize an office containing a verified store display."""
    return bool(retail_profile_ids(office, profiles))


def retail_company_npis(office, profiles, company_summaries_by_doctor=None):
    """Resolve each company profile independently from matching source identities."""
    retail_ids = retail_profile_ids(office, profiles)
    summaries = _by_doctor(office.get("providerSummaries"), "pid")
    company_summaries_by_doctor = company_summaries_by_doctor or {}
    if not company_summaries_by_doctor.keys() <= retail_ids:
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    npis_by_doctor = {}
    for profile in profiles:
        did = profile["doctorPublicId"]
        if did not in retail_ids:
            continue
        npis = {
            _company_npi(office, profile, summary)
            for summary in (summaries.get(did), company_summaries_by_doctor.get(did))
            if summary is not None
        }
        npis.discard(None)
        if len(npis) > 1:
            raise VSPDirectoryNormalizationError("vsp_directory_conflicting_npi")
        if npis:
            npis_by_doctor[did] = next(iter(npis))
    return npis_by_doctor


def _retail_company_resources(office_id, profiles, npis_by_doctor):
    """Keep distinct company identities linked to their shared source site."""
    for profile in profiles:
        did = profile["doctorPublicId"]
        if did not in npis_by_doctor:
            continue
        organization_id = f"vsp-site-company-{office_id}-{did}"
        yield {
            "resourceType": "Organization",
            "id": organization_id,
            "identifier": [
                {"system": "urn:vsp:doctor-id", "value": str(did)},
                {"system": _NPI_SYSTEM, "value": npis_by_doctor[did]},
            ],
            "name": " ".join(filter(None, (_text(profile, "firstName"), _text(profile, "lastName")))),
        }
        yield {
            "resourceType": "OrganizationAffiliation",
            "id": f"vsp-site-company-link-{office_id}-{did}",
            "organization": {"reference": f"Organization/vsp-site-org-{office_id}"},
            "participatingOrganization": {"reference": "Organization/" + organization_id},
            "location": [{"reference": f"Location/vsp-office-{office_id}"}],
        }


def _retail_resources(office, office_id, profiles, network_names, npis_by_doctor):
    if not is_retail_location(office, profiles) or not network_names <= set(NETWORKS.values()):
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    resources = VSPOfficeResources(
        outside_profile_count=len(profiles),
        outside_summary_count=0,
        other_network_ids={
            code for profile in profiles for code in _profile_networks(profile) if code not in _NETWORK_NAMES
        },
    )
    site = _location(office, office_id)
    organization_id = f"vsp-site-org-{office_id}"
    company_npis = set(npis_by_doctor.values())
    company_npi = next(iter(company_npis)) if len(company_npis) == 1 else None
    site["managingOrganization"] = {"reference": "Organization/" + organization_id}
    resources.extend(
        (
            site,
            {
                "resourceType": "Organization",
                "id": organization_id,
                "identifier": [{"system": "urn:vsp:office-id", "value": str(office_id)}]
                + ([{"system": _NPI_SYSTEM, "value": company_npi}] if company_npi is not None else []),
                "name": _text(office, "name"),
            },
            {
                "resourceType": "OrganizationAffiliation",
                "id": f"vsp-site-network-{office_id}",
                "participatingOrganization": {"reference": "Organization/" + organization_id},
                "location": [{"reference": site["resourceType"] + "/" + site["id"]}],
                "network": [
                    {"reference": "Organization/vsp-network-" + code}
                    for code, name in sorted(NETWORKS.items())
                    if name in network_names
                ],
            },
        )
    )
    if len(company_npis) > 1:
        resources.extend(_retail_company_resources(office_id, profiles, npis_by_doctor))
    return resources


def normalize_vsp_office(
    office: dict,
    *,
    profiles: list[dict],
    search_memberships=None,
    retail_networks=None,
    company_summaries_by_doctor=None,
) -> VSPOfficeResources:
    """Join summaries to this office's profiles by public doctor ID, never by name."""
    office_id = _public_id(office, "pid")
    summaries = _by_doctor(office.get("providerSummaries"), "pid")
    doctors = _by_doctor(profiles, "doctorPublicId")
    networks_by_doctor = {doctor_id: _profile_networks(profile) for doctor_id, profile in doctors.items()}
    retail_ids = retail_profile_ids(office, profiles)
    selected_by_doctor = vsp_profile_memberships(office, profiles, search_memberships or {})
    if summaries.keys() - doctors.keys() - selected_by_doctor.keys() or bool(retail_ids) != bool(retail_networks):
        raise VSPDirectoryNormalizationError("vsp_directory_unresolved_doctor")
    resources = VSPOfficeResources(
        outside_profile_count=len(doctors.keys() - selected_by_doctor.keys()),
        outside_summary_count=len(summaries.keys() - selected_by_doctor.keys()),
        other_network_ids={
            code for codes in networks_by_doctor.values() for code in codes if code not in _NETWORK_NAMES
        },
    )
    if retail_ids:
        resources.extend(
            _retail_resources(
                office,
                office_id,
                [doctors[did] for did in sorted(retail_ids)],
                set(retail_networks),
                retail_company_npis(office, profiles, company_summaries_by_doctor),
            )
        )
    elif selected_by_doctor:
        resources.append(_location(office, office_id))
    if selected_by_doctor:
        npi_counts = Counter(npi for summary in summaries.values() if isinstance(npi := summary.get("npi"), str))
        npi_counts.update(
            npi
            for doctor_id, profile in doctors.items()
            if summaries.get(doctor_id, {}).get("npi") is None and isinstance(npi := profile.get("npi"), str)
        )
        for doctor_id, codes in sorted(selected_by_doctor.items()):
            resources.extend(
                _selected_resources(
                    doctor_id, office_id, summaries.get(doctor_id, {}), doctors.get(doctor_id, {}), codes, npi_counts
                )
            )
    return resources
