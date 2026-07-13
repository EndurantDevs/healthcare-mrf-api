"""Typed Provider Directory role, plan, and network evidence checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class NetworkWitness:
    """One expected typed Organization network and its populated fields."""

    resource_id: str
    has_name: bool
    has_reference: bool


@dataclass(frozen=True)
class MappedEvidenceWitness:
    """One exact mapped role or affiliation witness for a provider/source pair."""

    source_id: str
    npi: int
    resource_type: str
    resource_id: str
    insurance_plan_ids: tuple[str, ...] = ()
    networks: tuple[NetworkWitness, ...] = ()
    address_key: str | None = None

    @property
    def supports_completion(self) -> bool:
        """Return whether this exact resource witness completes its capability."""
        return self.resource_type in {"PractitionerRole", "OrganizationAffiliation"}

    @property
    def supports_plan_network_context(self) -> bool:
        """Return whether the witness additionally exposes plan/network context."""
        return bool(self.insurance_plan_ids or self.networks)


def matching_source_summary_maps(
    provider_row: Any, source_id: str
) -> list[Mapping[str, Any]]:
    """Return exact FHIR source summaries from one consumer API provider row."""
    if not isinstance(provider_row, Mapping):
        return []
    raw_summary_list = provider_row.get("provider_directory_sources")
    if not isinstance(raw_summary_list, list):
        return []
    summary_map_list = []
    for summary_map in raw_summary_list:
        if not _is_exact_fhir_source_summary(summary_map):
            continue
        if _has_summary_source_id(summary_map, source_id):
            summary_map_list.append(summary_map)
    return summary_map_list


def _is_exact_fhir_source_summary(summary_map: Any) -> bool:
    return bool(
        isinstance(summary_map, Mapping)
        and summary_map.get("source") == "provider_directory_fhir"
        and summary_map.get("catalog_aliases_verified") is False
    )


def _has_summary_source_id(summary_map: Mapping[str, Any], source_id: str) -> bool:
    source_id_list = summary_map.get("source_ids")
    if isinstance(source_id_list, list) and source_id in {
        str(source_value) for source_value in source_id_list
    }:
        return True
    alias_list = summary_map.get("catalog_aliases")
    return isinstance(alias_list, list) and any(
        isinstance(alias_map, Mapping)
        and str(alias_map.get("source_id") or "") == source_id
        for alias_map in alias_list
    )


def has_detail_witness(
    response_payload: Mapping[str, Any] | None,
    witness: MappedEvidenceWitness,
) -> bool:
    """Return whether provider detail exposes one exact typed witness."""
    if not isinstance(response_payload, Mapping):
        return False
    data_map = response_payload.get("data")
    npi_map = data_map.get("npi") if isinstance(data_map, Mapping) else None
    address_list = npi_map.get("address_list") if isinstance(npi_map, Mapping) else None
    if not isinstance(address_list, list):
        return False
    return any(
        _has_provider_row_witness(provider_row, witness)
        for provider_row in address_list
    )


def has_provider_search_witness(
    response_payload: Mapping[str, Any] | None,
    witness: MappedEvidenceWitness,
) -> bool:
    """Return whether the provider collection exposes one exact witness."""
    if not isinstance(response_payload, Mapping):
        return False
    data_map = response_payload.get("data")
    response_map = data_map if isinstance(data_map, Mapping) else response_payload
    provider_item_list = response_map.get("items")
    if not isinstance(provider_item_list, list):
        provider_item_list = response_map.get("rows")
    if not isinstance(provider_item_list, list):
        return False
    return any(
        _has_provider_item_witness(provider_item, witness)
        for provider_item in provider_item_list
    )


def _has_provider_item_witness(
    provider_item: Any, witness: MappedEvidenceWitness
) -> bool:
    if _has_provider_row_witness(provider_item, witness):
        return True
    if not isinstance(provider_item, Mapping):
        return False
    address_list = provider_item.get("address_list")
    return isinstance(address_list, list) and any(
        _has_provider_row_witness(address_map, witness) for address_map in address_list
    )


def _has_provider_row_witness(
    provider_row: Any, witness: MappedEvidenceWitness
) -> bool:
    return any(
        _has_source_summary_witness(summary_map, witness)
        for summary_map in matching_source_summary_maps(provider_row, witness.source_id)
    )


def _has_source_summary_witness(
    summary_map: Mapping[str, Any], witness: MappedEvidenceWitness
) -> bool:
    if witness.resource_type == "PractitionerRole":
        if not _has_summary_role(summary_map, witness.resource_id):
            return False
    elif witness.resource_type == "OrganizationAffiliation":
        if not _has_summary_affiliation(summary_map, witness.resource_id):
            return False
    else:
        return False
    if not set(witness.insurance_plan_ids).issubset(
        _summary_resource_id_set(summary_map, "insurance_plans")
    ):
        return False
    return all(
        _has_summary_network(summary_map, expected_network)
        for expected_network in witness.networks
    )


def _has_summary_role(summary_map: Mapping[str, Any], role_id: str) -> bool:
    role_id_list = summary_map.get("practitioner_role_ids")
    if not isinstance(role_id_list, list) or role_id not in map(str, role_id_list):
        return False
    typed_role_list = summary_map.get("practitioner_roles")
    return isinstance(typed_role_list, list) and any(
        isinstance(role_map, Mapping)
        and role_map.get("resource_type") == "PractitionerRole"
        and str(role_map.get("resource_id") or "") == role_id
        for role_map in typed_role_list
    )


def _has_summary_affiliation(
    summary_map: Mapping[str, Any], affiliation_id: str
) -> bool:
    affiliation_id_list = summary_map.get("organization_affiliation_ids")
    return isinstance(affiliation_id_list, list) and affiliation_id in map(
        str, affiliation_id_list
    )


def _summary_resource_id_set(summary_map: Mapping[str, Any], field: str) -> set[str]:
    resource_map_list = summary_map.get(field)
    if not isinstance(resource_map_list, list):
        return set()
    return {
        str(resource_map.get("resource_id") or "")
        for resource_map in resource_map_list
        if isinstance(resource_map, Mapping) and resource_map.get("resource_id")
    }


def _has_summary_network(
    summary_map: Mapping[str, Any], expected_network: NetworkWitness
) -> bool:
    network_map_list = summary_map.get("networks")
    if not isinstance(network_map_list, list):
        return False
    matching_network_list = [
        network_map
        for network_map in network_map_list
        if isinstance(network_map, Mapping)
        and network_map.get("resource_type") == "Organization"
        and str(network_map.get("resource_id") or "") == expected_network.resource_id
    ]
    if not matching_network_list:
        return False
    if expected_network.has_name and not any(
        str(network_map.get("name") or "").strip()
        for network_map in matching_network_list
    ):
        return False
    return not expected_network.has_reference or any(
        str(network_map.get("reference") or "").strip()
        for network_map in matching_network_list
    )
