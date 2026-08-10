# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed registry identity for dormant rooted Provider Directory acquisition."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-source.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE = (
    "official-practitioner-rooted-graph-enrichment"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT = "fhir_rest_rooted_reference_closure"
PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE = UHC_FLEX_PRACTITIONER_API_BASE
PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID = UHC_FLEX_OFFICIAL_AUTHORITY_ID


class RootedGraphSourceContractError(ValueError):
    """Reject drift from the reviewed dormant graph source identity."""


def _canonical_identity_json(identity_by_field: dict[str, Any]) -> str:
    return json.dumps(
        identity_by_field,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _identity_sha256(identity_by_field: dict[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_identity_json(identity_by_field).encode("utf-8")
    ).hexdigest()


def _canonical_json_sha256(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def rooted_graph_source_identity_payload() -> dict[str, Any]:
    """Return the immutable identity used for the graph acquisition source."""

    return {
        "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "connector_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
        "identity_contract": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID
        ),
        "source_role": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE,
    }


PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID = (
    "pdfhir_" + _identity_sha256(rooted_graph_source_identity_payload())[:24]
)
_PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_JSON = _canonical_identity_json(
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT.endpoint_signature()
)
_PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_JSON = "{}"
PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_SHA256 = _canonical_json_sha256(
    _PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_JSON
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256 = _canonical_json_sha256(
    _PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_JSON
)


def provider_directory_rooted_graph_credential_descriptor() -> dict[str, Any]:
    """Return a fresh exact no-auth descriptor."""

    return json.loads(_PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_JSON)


def provider_directory_rooted_graph_endpoint_signature() -> dict[str, Any]:
    """Return a fresh exact connector signature."""

    return json.loads(_PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_JSON)


PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID = _identity_sha256(
    {
        "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
        "credential_descriptor": (
            provider_directory_rooted_graph_credential_descriptor()
        ),
        "endpoint_signature": provider_directory_rooted_graph_endpoint_signature(),
    }
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_LOCK_IDENTITY = (
    "provider-directory-rooted-graph-publication:"
    + PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
)


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphSourceContract:
    """Bind one disjoint registry pair to the exact rooted graph contract."""

    source_identity_contract_id: str = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID
    )
    source_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
    endpoint_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
    connector_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    graph_contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
    authority_id: str = UHC_FLEX_OFFICIAL_AUTHORITY_ID
    source_role: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE
    canonical_api_base: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE
    transport: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT
    resource_types: tuple[str, ...] = PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        expected_by_field = {
            "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
            "canonical_api_base": PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE,
            "connector_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
            "endpoint_collection_complete": False,
            "endpoint_complete": False,
            "endpoint_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
            "graph_contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
            "resource_types": PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES,
            "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            "source_identity_contract_id": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID
            ),
            "source_role": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE,
            "transport": PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT,
        }
        if any(
            getattr(self, field_name) != expected_value
            for field_name, expected_value in expected_by_field.items()
        ):
            raise RootedGraphSourceContractError(
                "provider_directory_rooted_graph_source_contract_inconsistent"
            )

    def endpoint_signature(self) -> dict[str, Any]:
        """Return a fresh exact graph signature for registry validation."""

        return provider_directory_rooted_graph_endpoint_signature()


PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_CONTRACT = (
    ProviderDirectoryRootedGraphSourceContract()
)

ProviderDirectoryRootedGraphSourceContractError = RootedGraphSourceContractError
provider_directory_rooted_graph_source_identity_payload = (
    rooted_graph_source_identity_payload
)


__all__ = (
    "provider_directory_rooted_graph_source_identity_payload",
    "rooted_graph_source_identity_payload",
    "provider_directory_rooted_graph_credential_descriptor",
    "provider_directory_rooted_graph_endpoint_signature",
    "ProviderDirectoryRootedGraphSourceContract",
    "ProviderDirectoryRootedGraphSourceContractError",
    "RootedGraphSourceContractError",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_API_BASE",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_CREDENTIAL_DESCRIPTOR_SHA256",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_LOCK_IDENTITY",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_CONTRACT",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_IDENTITY_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ROLE",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_TRANSPORT",
)
