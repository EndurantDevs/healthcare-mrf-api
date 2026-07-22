# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral FHIR payload to physical projection row transformation."""
from __future__ import annotations
import re
from typing import Any, Iterable, Mapping
from process.provider_directory_projection_fhir_values import (
    fhir_list,
    invalid_fhir_field,
    normalized_address_map,
    normalized_period_map,
    normalized_position_map,
    normalized_text,
    optional_field_text,
    profile_concept_maps,
    profile_contact_maps,
    profile_name_maps,
)
from process.provider_directory_projection_semantic_evidence import (
    normalized_semantic_contribution,
)
from process.provider_directory_projection_typed_evidence import (
    SEMANTIC_GEOCODE_EVIDENCE_KEY,
    SEMANTIC_RELATIONSHIP_EVIDENCE_KEY,
    normalized_semantic_evidence,
)
from process.provider_directory_projection_types import (
    ProjectionShardClaim,
    ProviderDirectoryProjectionError,
    stable_json,
)


_SUPPORTED_RESOURCE_TYPES = frozenset({
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
})
_NPI_RESOURCE_TYPES = frozenset({"HealthcareService", "Organization", "Practitioner", "PractitionerRole"})
_NPI_ID_RESOURCE_TYPES = frozenset({"Organization", "Practitioner"})
_FHIR_ID = re.compile(r"^[A-Za-z0-9\-.]{1,64}$")
_REFERENCE_FIELDS_BY_RESOURCE = {
    "Endpoint": ("managingOrganization",),
    "HealthcareService": ("providedBy", "location", "coverageArea", "endpoint"),
    "InsurancePlan": ("ownedBy", "administeredBy", "network", "coverageArea", "endpoint"),
    "Location": ("managingOrganization", "partOf", "endpoint"),
    "Organization": ("partOf", "endpoint"),
    "OrganizationAffiliation": (
        "organization",
        "participatingOrganization",
        "network",
        "location",
        "healthcareService",
        "endpoint",
    ),
    "PractitionerRole": (
        "practitioner",
        "organization",
        "location",
        "healthcareService",
        "endpoint",
        "network",
        "insurancePlan",
    ),
}
_RELATIONSHIP_FIELD_BY_RESOURCE = {
    "InsurancePlan": {"network": ("network", "Organization")},
    "OrganizationAffiliation": {
        "organization": ("organization", "Organization"),
        "participatingOrganization": ("participating_organization", "Organization"),
        "network": ("network", "Organization"),
        "location": ("location", "Location"),
        "healthcareService": ("healthcare_service", "HealthcareService"),
        "endpoint": ("endpoint", "Endpoint"),
    },
}
_SPECIALTY_FIELDS_BY_RESOURCE = {
    "HealthcareService": ("specialty",),
    "OrganizationAffiliation": ("specialty",),
    "PractitionerRole": ("specialty",),
}
_TAXONOMY_CODE = re.compile(r"^[A-Za-z0-9]{10}$")
_STATUS_OUTCOME_BY_RESOURCE = {
    "Endpoint": {
        "active": True,
        "suspended": False,
        "error": False,
        "off": False,
        "entered-in-error": False,
        "test": False,
    },
    "InsurancePlan": {
        "active": True,
        "draft": False,
        "retired": False,
        "unknown": False,
    },
    "Location": {"active": True, "suspended": False, "inactive": False},
}
_PLAN_NET_NETWORK_EXTENSION_URLS = frozenset(
    {
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference",
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-participatingnetwork-extension",
    }
)


def _profile_address_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> list[dict[str, Any]]:
    if resource_type not in {
        "HealthcareService", "Location", "Organization", "Practitioner"
    }:
        return []
    raw_addresses = fhir_resource_map.get("address")
    if raw_addresses is None:
        address_entries: list[Any] = []
    elif isinstance(raw_addresses, list):
        address_entries = raw_addresses
    elif isinstance(raw_addresses, Mapping):
        address_entries = [raw_addresses]
    else:
        raise invalid_fhir_field("address")
    address_maps = [normalized_address_map(address_data) for address_data in address_entries]
    position_map = normalized_position_map(fhir_resource_map.get("position"))
    if resource_type == "Location" and address_maps and position_map is not None:
        address_maps[0] = {
            **address_maps[0],
            SEMANTIC_GEOCODE_EVIDENCE_KEY: position_map,
        }
    return address_maps


def _reference_entries(reference_data: Any) -> Iterable[Mapping[str, Any]]:
    if reference_data is None:
        return ()
    if isinstance(reference_data, Mapping):
        return (reference_data,)
    if isinstance(reference_data, list):
        if not all(isinstance(reference_entry, Mapping) for reference_entry in reference_data):
            raise invalid_fhir_field("reference")
        return tuple(reference_data)
    raise invalid_fhir_field("reference")


def _normalized_reference_map(reference_data: Mapping[str, Any]) -> dict[str, Any]:
    reference_map: dict[str, Any] = {}
    for field_name in ("reference", "type", "display", "id"):
        field_text = optional_field_text(reference_data, field_name)
        if field_text is not None:
            reference_map[field_name] = field_text
    raw_identifier = reference_data.get("identifier")
    if raw_identifier is not None:
        if not isinstance(raw_identifier, Mapping):
            raise invalid_fhir_field("reference_identifier")
        identifier_map = {
            field_name: field_text
            for field_name in ("system", "value")
            if (
                field_text := optional_field_text(raw_identifier, field_name)
            )
            is not None
        }
        if "value" not in identifier_map:
            raise invalid_fhir_field("reference_identifier")
        reference_map["identifier"] = identifier_map
    if not (
        "reference" in reference_map
        or "identifier" in reference_map
        or {"type", "id"} <= set(reference_map)
    ):
        raise invalid_fhir_field("reference")
    return reference_map


def _reference_target_id(reference_text: Any, expected_type: str) -> str | None:
    normalized_reference = normalized_text(reference_text)
    if normalized_reference is None:
        return None
    clean_reference = normalized_reference.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    path_segments = [segment for segment in clean_reference.split("/") if segment]
    if len(path_segments) == 1 and _FHIR_ID.fullmatch(path_segments[0]):
        return path_segments[0]
    if len(path_segments) >= 2 and path_segments[-2] == "_history":
        path_segments = path_segments[:-2]
    for segment_index in range(len(path_segments) - 2, -1, -1):
        if (
            path_segments[segment_index] == expected_type
            and _FHIR_ID.fullmatch(path_segments[segment_index + 1])
        ):
            return path_segments[segment_index + 1]
    return None


def _normalized_extension_url(extension_url: Any) -> str | None:
    url_text = normalized_text(extension_url)
    if url_text is None:
        return None
    normalized_url = url_text.split("|", 1)[0].rstrip("/").lower()
    if normalized_url.startswith("https://"):
        normalized_url = "http://" + normalized_url[len("https://") :]
    return normalized_url


def _extension_network_reference_maps(
    fhir_resource_map: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    reference_maps: list[Mapping[str, Any]] = []

    def visit_extensions(extension_data: Any) -> None:
        """Visit nested Plan-Net extensions without interpreting other URLs."""

        if extension_data is None:
            return
        extension_entries = extension_data if isinstance(extension_data, list) else [extension_data]
        if not all(isinstance(extension_entry, Mapping) for extension_entry in extension_entries):
            raise invalid_fhir_field("extension")
        for extension_map in extension_entries:
            if _normalized_extension_url(extension_map.get("url")) in _PLAN_NET_NETWORK_EXTENSION_URLS:
                network_reference_maps = list(
                    _reference_entries(extension_map.get("valueReference"))
                )
                if not network_reference_maps:
                    raise invalid_fhir_field("network_extension")
                reference_maps.extend(network_reference_maps)
            visit_extensions(extension_map.get("extension"))

    visit_extensions(fhir_resource_map.get("extension"))
    return reference_maps


def _relationship_reference_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
    field_name: str,
) -> list[Mapping[str, Any]]:
    reference_maps = list(_reference_entries(fhir_resource_map.get(field_name)))
    if field_name == "network":
        reference_maps.extend(_extension_network_reference_maps(fhir_resource_map))
    if resource_type == "InsurancePlan" and field_name == "network":
        for backbone_name in ("plan", "coverage"):
            for backbone_map in fhir_list(fhir_resource_map, backbone_name):
                if not isinstance(backbone_map, Mapping):
                    raise invalid_fhir_field(backbone_name)
                reference_maps.extend(_reference_entries(backbone_map.get("network")))
    return reference_maps


def _relationship_evidence_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> list[dict[str, Any]]:
    relationship_maps: list[dict[str, Any]] = []
    seen_relationships: set[tuple[str, str, str]] = set()
    field_contract_map = _RELATIONSHIP_FIELD_BY_RESOURCE.get(resource_type, {})
    for field_name, (relationship_kind, target_type) in field_contract_map.items():
        for reference_map in _relationship_reference_maps(
            fhir_resource_map, resource_type, field_name
        ):
            target_id = _reference_target_id(reference_map.get("reference"), target_type)
            if target_id is None:
                raise invalid_fhir_field("relationship_reference")
            relationship_key = (relationship_kind, target_type, target_id)
            if relationship_key in seen_relationships:
                continue
            seen_relationships.add(relationship_key)
            relationship_maps.append(
                {
                    SEMANTIC_RELATIONSHIP_EVIDENCE_KEY: {
                        "kind": relationship_kind,
                        "target_resource_type": target_type,
                        "target_resource_id": target_id,
                    }
                }
            )
    return relationship_maps


def _profile_reference_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> list[dict[str, Any]]:
    reference_maps: list[dict[str, Any]] = []
    for field_name in _REFERENCE_FIELDS_BY_RESOURCE.get(resource_type, ()):
        reference_maps.extend(
            _normalized_reference_map(reference_data)
            for reference_data in _reference_entries(fhir_resource_map.get(field_name))
        )
    reference_maps.extend(
        _normalized_reference_map(reference_data)
        for reference_data in _extension_network_reference_maps(fhir_resource_map)
    )
    reference_maps.extend(_relationship_evidence_maps(fhir_resource_map, resource_type))
    deduplicated_maps: list[dict[str, Any]] = []
    seen_references: set[str] = set()
    for reference_map in reference_maps:
        reference_key = stable_json(reference_map)
        if reference_key not in seen_references:
            deduplicated_maps.append(reference_map)
            seen_references.add(reference_key)
    return deduplicated_maps


def _is_taxonomy_coding(coding_map: Mapping[str, Any]) -> bool:
    coding_system = str(coding_map.get("system") or "").lower()
    coding_code = str(coding_map.get("code") or "")
    return bool(
        "nucc" in coding_system
        or "taxonomy" in coding_system
        or _TAXONOMY_CODE.fullmatch(coding_code)
    )


def _practitioner_taxonomy_maps(
    fhir_resource_map: Mapping[str, Any],
) -> list[dict[str, Any]]:
    specialty_maps: list[dict[str, Any]] = []
    for qualification_map in fhir_list(fhir_resource_map, "qualification"):
        if not isinstance(qualification_map, Mapping):
            raise invalid_fhir_field("qualification")
        for concept_map in profile_concept_maps(qualification_map.get("code")):
            taxonomy_codings = [
                coding_map
                for coding_map in concept_map.get("coding", ())
                if _is_taxonomy_coding(coding_map)
            ]
            if taxonomy_codings:
                specialty_maps.append(
                    {
                        **({"text": concept_map["text"]} if "text" in concept_map else {}),
                        "coding": taxonomy_codings,
                    }
                )
    return specialty_maps


def _profile_specialty_maps(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> list[dict[str, Any]]:
    specialty_maps: list[dict[str, Any]] = []
    for field_name in _SPECIALTY_FIELDS_BY_RESOURCE.get(resource_type, ()):
        specialty_maps.extend(profile_concept_maps(fhir_resource_map.get(field_name)))
    if resource_type == "Practitioner":
        specialty_maps.extend(_practitioner_taxonomy_maps(fhir_resource_map))
    return specialty_maps


def _identifier_type_text(identifier_map: Mapping[str, Any]) -> str:
    identifier_type = identifier_map.get("type")
    if identifier_type is None:
        return ""
    if not isinstance(identifier_type, Mapping):
        raise invalid_fhir_field("identifier_type")
    type_parts = [str(identifier_type.get("text") or "")]
    for coding_map in fhir_list(identifier_type, "coding"):
        if not isinstance(coding_map, Mapping):
            raise invalid_fhir_field("identifier_type")
        type_parts.extend(str(coding_map.get(field_name) or "") for field_name in ("system", "code", "display"))
    return " ".join(type_parts).lower()


def _summary_npi(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
    resource_id: str,
) -> int | None:
    if resource_type not in _NPI_RESOURCE_TYPES:
        return None
    ranked_candidates: list[tuple[int, int, int]] = []
    for identifier_ordinal, identifier_map in enumerate(fhir_list(fhir_resource_map, "identifier")):
        if not isinstance(identifier_map, Mapping):
            raise invalid_fhir_field("identifier")
        identifier_text = optional_field_text(identifier_map, "value", limit=64)
        if identifier_text is None:
            continue
        identifier_system = str(identifier_map.get("system") or "").lower()
        descriptor = f"{identifier_system} {_identifier_type_text(identifier_map)}"
        if "npi" not in descriptor and "national provider" not in descriptor:
            continue
        digits = "".join(character for character in identifier_text if character.isdigit())
        if len(digits) == 10 and 1_000_000_000 <= int(digits) <= 2_999_999_999:
            system_rank = int(identifier_system != "http://hl7.org/fhir/sid/us-npi")
            ranked_candidates.append((system_rank, identifier_ordinal, int(digits)))
    if ranked_candidates:
        return min(ranked_candidates)[2]
    resource_id_npi = (
        int(resource_id)
        if len(resource_id) == 10 and resource_id.isdigit()
        else 0
    )
    if (
        resource_type in _NPI_ID_RESOURCE_TYPES
        and 1_000_000_000 <= resource_id_npi <= 2_999_999_999
    ):
        return resource_id_npi
    return None


def _active_status(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> bool | None:
    status_outcome_map = _STATUS_OUTCOME_BY_RESOURCE.get(resource_type)
    if status_outcome_map is not None:
        status_text = optional_field_text(fhir_resource_map, "status", limit=64)
        if status_text is None:
            return None
        if status_text not in status_outcome_map:
            raise invalid_fhir_field("status")
        return status_outcome_map[status_text]
    if "active" not in fhir_resource_map:
        return None
    if type(fhir_resource_map["active"]) is not bool:
        raise invalid_fhir_field("active")
    return fhir_resource_map["active"]


def _profile_evidence_map(
    fhir_resource_map: Mapping[str, Any],
    resource_type: str,
) -> dict[str, Any]:
    evidence_map = {
        "names": profile_name_maps(fhir_resource_map),
        "specialties": _profile_specialty_maps(fhir_resource_map, resource_type),
        "contacts": profile_contact_maps(fhir_resource_map, resource_type),
        "addresses": _profile_address_maps(fhir_resource_map, resource_type),
        "references": _profile_reference_maps(fhir_resource_map, resource_type),
    }
    return {
        evidence_name: evidence_entries
        for evidence_name, evidence_entries in evidence_map.items()
        if evidence_entries
    }


def projection_resource_row(
    fhir_resource_map: Mapping[str, Any],
    *,
    claim: ProjectionShardClaim,
    input_ordinal: int,
    payload_hash: str,
) -> dict[str, Any]:
    """Derive and validate one exact physical row from canonical FHIR JSON."""

    resource_type = fhir_resource_map.get("resourceType")
    resource_id = fhir_resource_map.get("id")
    if (
        resource_type not in _SUPPORTED_RESOURCE_TYPES
        or not isinstance(resource_id, str)
        or _FHIR_ID.fullmatch(resource_id) is None
        or type(input_ordinal) is not int
        or input_ordinal < 0
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_record_invalid"
        )
    profile_evidence_map = _profile_evidence_map(fhir_resource_map, resource_type)
    expanded_evidence_map = {
        evidence_name: list(profile_evidence_map.get(evidence_name, ()))
        for evidence_name in ("names", "specialties", "contacts", "addresses", "references")
    }
    typed_evidence_map = normalized_semantic_evidence(resource_type, expanded_evidence_map)
    address_count = len(expanded_evidence_map["addresses"])
    relationship_count = len(typed_evidence_map["relationships"])
    period_map = normalized_period_map(fhir_resource_map.get("period")) or {}
    raw_metadata = fhir_resource_map.get("meta")
    if raw_metadata is not None and not isinstance(raw_metadata, Mapping):
        raise invalid_fhir_field("meta")
    metadata_map = raw_metadata or {}
    projection_row_map = {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "proof_partition_id": claim.shard.partition_id,
        "payload_hash": payload_hash,
        "source_rank": f"{claim.shard.partition_ordinal:020d}:{payload_hash}:{input_ordinal:020d}",
        "summary_npi": _summary_npi(fhir_resource_map, resource_type, resource_id),
        "summary_address_count": address_count,
        "summary_addressed_location": resource_type == "Location" and address_count > 0,
        "summary_geocoded_location": bool(typed_evidence_map["geocodes"]),
        "summary_network_link_count": relationship_count if resource_type == "InsurancePlan" else 0,
        "summary_affiliation_link_count": relationship_count if resource_type == "OrganizationAffiliation" else 0,
        "active": _active_status(fhir_resource_map, resource_type),
        "effective_start": normalized_text(period_map.get("start"), limit=64),
        "effective_end": normalized_text(period_map.get("end"), limit=64),
        "observed_at": optional_field_text(metadata_map, "lastUpdated", limit=64),
        "profile_evidence_json": profile_evidence_map or None,
    }
    normalized_semantic_contribution(projection_row_map)
    return projection_row_map


__all__ = ("projection_resource_row",)
