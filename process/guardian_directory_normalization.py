# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Translate Guardian detail records and their explicit network memberships."""

import hashlib
import re

from process.provider_directory_projection_fhir_values import is_valid_npi
from process.vsp_directory_contract import canonical_json

_PLAN_NET = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"


def _id(kind, *parts):
    digest = hashlib.sha256(canonical_json(parts).encode()).hexdigest()[:40]
    return f"guardian-{kind}-{digest}"


def _text(record, key):
    value = record.get(key)
    if value is None:
        return ""
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("guardian_detail_text_invalid")
    text = value.strip()
    return "" if text == "No Information Available" else text


def _source_id(record, key):
    value = record.get(key)
    if type(value) not in (int, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", str(value).strip()):
        raise ValueError("guardian_detail_identity_invalid")
    return str(value).strip()


def _network(name):
    return {
        "resourceType": "Organization",
        "id": _id("net", name),
        "name": name,
        "meta": {"profile": [_PLAN_NET + "plannet-Network"]},
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type", "code": "ntwk"}]}],
        "identifier": [{"system": "urn:guardian:directory:network-label", "value": name}],
    }


def _location(detail, practice):
    lines = []
    for key in ("address1", "address2", "suite"):
        text = _text(detail, key)
        if text and text not in lines:
            lines.append(text)
    address_map = {
        "line": lines,
        "city": _text(detail, "city"),
        "state": _text(detail, "state"),
        "postalCode": _text(detail, "zip"),
    }
    if not all(address_map.values()):
        raise ValueError("guardian_detail_address_invalid")
    site_map = {
        "resourceType": "Location",
        "id": _id("site", practice, address_map),
        "identifier": [{"system": "urn:guardian:directory:practice-id", "value": practice}],
        "address": address_map,
    }
    if name := _text(detail, "ddsPracticeName"):
        site_map["name"] = name
    if phone := _text(detail, "phone"):
        site_map["telecom"] = [{"system": "phone", "value": phone}]
    coords = [detail.get(key) for key in ("latitude", "longitude")]
    if all(coordinate not in (None, "") for coordinate in coords):
        if any(type(coordinate) not in (int, float, str) for coordinate in coords):
            raise ValueError("guardian_detail_position_invalid")
        latitude, longitude = map(float, coords)
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError("guardian_detail_position_invalid")
        if latitude or longitude:
            site_map["position"] = {"latitude": latitude, "longitude": longitude}
    return site_map


def _practitioner(detail):
    npi = detail.get("npi")
    npi = str(npi).strip() if is_valid_npi(npi) else None
    identifiers = []
    raw_id = detail.get("denMastId")
    if raw_id is not None and str(raw_id).strip() not in {"", "0", "-1", "No Information Available"}:
        doctor = _source_id(detail, "denMastId")
        identifier = _id("dentist", doctor)
        identifiers.append({"system": "urn:guardian:directory:dentist-id", "value": doctor})
    elif npi:
        identifier = f"guardian-npi-{npi}"
    else:
        raise ValueError("guardian_detail_identity_invalid")
    if npi:
        identifiers.append({"system": _NPI_SYSTEM, "value": npi})
    person_map = {"resourceType": "Practitioner", "id": identifier, "identifier": identifiers}
    name_map = {}
    if family := _text(detail, "ddsLastName"):
        name_map["family"] = family
    if given_names := [part for key in ("ddsFirstName", "ddsMiddleName") if (part := _text(detail, key))]:
        name_map["given"] = given_names
    if suffix := _text(detail, "ddsSuffixName"):
        name_map["suffix"] = [suffix]
    if name_map:
        person_map["name"] = [name_map]
    gender = _text(detail, "gender").lower()
    if gender in {"male", "female", "other", "unknown"}:
        person_map["gender"] = gender
    if degree := _text(detail, "ddsDegree"):
        person_map["qualification"] = [{"code": {"text": degree}}]
    return person_map


def _membership(entity, site, network, *, specialty=()):
    is_person = entity["resourceType"] == "Practitioner"
    family, kind = ("PractitionerRole", "role") if is_person else ("OrganizationAffiliation", "aff")
    relation_map = {
        "resourceType": family,
        "id": _id(kind, entity["id"], site["id"], network and network["id"]),
        "practitioner" if is_person else "participatingOrganization": {
            "reference": entity["resourceType"] + "/" + entity["id"]
        },
        "location": [{"reference": "Location/" + site["id"]}],
    }
    if network:
        network_reference_map = {"reference": "Organization/" + network["id"]}
        if is_person:
            relation_map["extension"] = [
                {"url": _PLAN_NET + "network-reference", "valueReference": network_reference_map}
            ]
        else:
            relation_map["network"] = [network_reference_map]
    if specialty:
        relation_map["specialty"] = [{"text": value} for value in specialty]
    return relation_map


def normalize_guardian_detail(detail):
    """Emit the scoped clinician/site relationships and separately declared networks."""
    if not isinstance(detail, dict) or detail.get("cardType") not in ("P", "F") or "networks" not in detail:
        raise ValueError("guardian_detail_shape_invalid")
    practice = _source_id(detail, "practiceId")
    site = _location(detail, practice)
    is_facility = detail["cardType"] == "F"
    if is_facility:
        if not (name := _text(detail, "ddsPracticeName")):
            raise ValueError("guardian_facility_name_missing")
        entity_map = {
            "resourceType": "Organization",
            "id": _id("office", practice),
            "name": name,
            "identifier": [{"system": "urn:guardian:directory:practice-id", "value": practice}],
        }
    else:
        entity_map = _practitioner(detail)
    resources = [site, *_site_resources(entity_map, site, detail)]
    providers = detail.get("providers")
    if providers is None:
        return tuple(resources)
    if not isinstance(providers, list) or (not is_facility and providers):
        raise ValueError("guardian_nested_provider_shape_invalid")
    for provider in providers:
        if not isinstance(provider, dict) or (
            provider.get("practiceId") is not None and _source_id(provider, "practiceId") != practice
        ):
            raise ValueError("guardian_nested_provider_shape_invalid")
        resources.extend(_site_resources(_practitioner(provider), site, provider))
    return tuple(resources)


def _site_resources(entity, site, source):
    networks = [_network(label) for label in _labels(_text(source, "networks"))]
    specialties = _labels(_text(source, "specialties"))
    yield entity
    yield from networks
    for network in networks or [None]:
        yield _membership(entity, site, network, specialty=specialties)


def _labels(text):
    return sorted({name for item in text.split("~") if (name := item.strip()) and name != "No Information Available"})


def merge_guardian_resources(prior, incoming):
    """Merge complementary observations for the same publisher-scoped identity."""
    if prior == incoming:
        return prior
    if (prior["resourceType"], prior["id"]) != (incoming["resourceType"], incoming["id"]):
        raise ValueError("guardian_resource_payload_conflict")
    if prior["resourceType"] == "Location":
        return _merge_site(prior, incoming)
    if prior["resourceType"] in {"OrganizationAffiliation", "PractitionerRole"}:
        return _merge_specialties(prior, incoming)
    if prior["resourceType"] != "Practitioner":
        raise ValueError("guardian_resource_payload_conflict")
    merged_map = {**prior, **incoming}
    for key in (prior.keys() & incoming.keys()) - {"identifier", "name"}:
        if prior[key] != incoming[key]:
            raise ValueError("guardian_resource_payload_conflict")
    by_system = {}
    for identifier_entry in [*prior["identifier"], *incoming["identifier"]]:
        if identifier_entry["system"] in by_system and identifier_entry != by_system[identifier_entry["system"]]:
            raise ValueError("guardian_resource_payload_conflict")
        by_system[identifier_entry["system"]] = identifier_entry
    merged_map["identifier"] = sorted(
        by_system.values(), key=lambda identifier_entry: identifier_entry["system"] == _NPI_SYSTEM
    )
    if "name" in prior and "name" in incoming:
        left, right = prior["name"][0], incoming["name"][0]
        name_map = {**left, **right}
        for key in left.keys() & right.keys():
            if left[key] == right[key]:
                continue
            if key == "given":
                shorter, longer = sorted((left[key], right[key]), key=len)
                if longer[: len(shorter)] == shorter:
                    name_map[key] = longer
                    continue
            raise ValueError("guardian_resource_payload_conflict")
        merged_map["name"] = [name_map]
    return merged_map


def _merge_site(prior, incoming):
    site_map = {**prior, **incoming}
    for field in (prior.keys() & incoming.keys()) - {"name", "alias", "telecom"}:
        if prior[field] != incoming[field]:
            raise ValueError("guardian_resource_payload_conflict")
    names = sorted({name for site in (prior, incoming) for name in (site.get("name"), *site.get("alias", ())) if name})
    if names:
        site_map["name"] = names[0]
        if len(names) > 1:
            site_map["alias"] = names[1:]
    contacts = sorted(
        {(contact["system"], contact["value"]) for site in (prior, incoming) for contact in site.get("telecom", ())}
    )
    if contacts:
        site_map["telecom"] = [{"system": system, "value": number} for system, number in contacts]
    return site_map


def _merge_specialties(prior, incoming):
    if {k: v for k, v in prior.items() if k != "specialty"} != {k: v for k, v in incoming.items() if k != "specialty"}:
        raise ValueError("guardian_resource_payload_conflict")
    names = sorted({entry["text"] for rel in (prior, incoming) for entry in rel.get("specialty", ())})
    return {**prior, "specialty": [{"text": name} for name in names]}
