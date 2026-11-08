# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed opt-in metadata selectors, separate from legacy catalog requests."""

import re
from dataclasses import dataclass

from db.registry_fhir_catalog import VIEW
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_read import FHIRCatalogQuery


@dataclass(frozen=True)
class FHIRCatalogSelection:
    query: FHIRCatalogQuery
    network_id: int | None = None


def _generation(value):
    if type(value) is not str or re.fullmatch(r"[1-9][0-9]{0,18}", value) is None or int(value) >= 2**63:
        raise ValueError("invalid metadata generation selector")
    return int(value)


def parse_fhir_catalog_query(document, kind):
    """Require explicit negotiation and retain exact policy-independent pins."""
    fields = {
        "view",
        "network_generation",
        "metadata_generation",
        "continuation_scope_sha256",
        "search",
        "archived",
        "source",
        "benefit_code",
        "company_id",
        "group_id",
        "company_search",
        "plan_search",
    }
    if kind == "list":
        fields |= {"limit", "offset"}
    elif kind == "detail":
        fields |= {"network_id", "relationship_offset", "binding_offset"}
    else:
        raise ValueError("unsupported metadata catalog operation")
    if type(document) is not dict or set(document) - fields or document.get("view") != VIEW:
        raise ValueError("invalid metadata catalog query")
    arguments_by_name = {
        key: query_value
        for key, query_value in document.items()
        if key not in {"view", "network_id", "network_generation"}
    }
    arguments_by_name["generation_id"] = (
        _generation(document["network_generation"]) if "network_generation" in document else None
    )
    if "source" in arguments_by_name:
        if type(arguments_by_name["source"]) is not dict:
            raise ValueError("invalid metadata catalog source")
        arguments_by_name["source"] = RegistryNetworkSourceCoordinates(**arguments_by_name["source"])
    network_id = document.get("network_id")
    if kind == "detail":
        if type(network_id) is not int or not 0 < network_id < 2**31:
            raise ValueError("invalid metadata catalog identity")
        arguments_by_name.update(limit=1, offset=0)
    return FHIRCatalogSelection(FHIRCatalogQuery(**arguments_by_name), network_id), arguments_by_name["generation_id"]
