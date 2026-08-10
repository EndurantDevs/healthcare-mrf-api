# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed source and connector identity for exact Flex Practitioner reads."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any

from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_AUTHORITY_ID,
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)


UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1"
)
UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID = (
    "healthporta.provider-directory.derived-enrichment-source.v1"
)
UHC_FLEX_PRACTITIONER_API_BASE = "https://flex.optum.com/fhirpublic/R4"
UHC_FLEX_PRACTITIONER_SOURCE_ROLE = "official-practitioner-npi-enrichment"
UHC_FLEX_PRACTITIONER_TRANSPORT = "fhir_rest_exact_identifier"
UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER = "identifier"
UHC_FLEX_PRACTITIONER_COHORT_SCOPE = "official_practitioner_npi_cohort"
UHC_FLEX_PRACTITIONER_QUERY_VALUES_PER_REQUEST = 1
UHC_FLEX_PRACTITIONER_QUERY_COUNT = 16
UHC_FLEX_PRACTITIONER_PAGINATION = "forbidden"


class UHCFlexPractitionerContractError(ValueError):
    """Reject drift from the reviewed exact-identifier connector."""


def _canonical_identity_json(identity_by_field: dict[str, Any]) -> str:
    return json.dumps(
        identity_by_field,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _derived_identity(
    prefix: str,
    identity_by_field: dict[str, Any],
    digest_length: int,
) -> str:
    digest = hashlib.sha256(
        _canonical_identity_json(identity_by_field).encode("utf-8")
    ).hexdigest()
    return prefix + digest[:digest_length]


def uhc_flex_practitioner_connector_identity_payload() -> dict[str, Any]:
    """Return the static connector identity without dynamic cohort lineage."""

    return {
        "cohort_scope": UHC_FLEX_PRACTITIONER_COHORT_SCOPE,
        "contract_id": UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
        "identifier_system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
        "pagination": UHC_FLEX_PRACTITIONER_PAGINATION,
        "query_count": UHC_FLEX_PRACTITIONER_QUERY_COUNT,
        "query_values_per_request": (
            UHC_FLEX_PRACTITIONER_QUERY_VALUES_PER_REQUEST
        ),
        "resource_type": UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
        "search_parameter": UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER,
        "transport": UHC_FLEX_PRACTITIONER_TRANSPORT,
    }


UHC_FLEX_PRACTITIONER_CONNECTOR_ID = _derived_identity(
    "pdufpc_",
    uhc_flex_practitioner_connector_identity_payload(),
    48,
)


def uhc_flex_practitioner_source_identity_payload() -> dict[str, Any]:
    """Return the identity used to derive the dedicated enrichment source."""

    return {
        "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
        "canonical_api_base": UHC_FLEX_PRACTITIONER_API_BASE,
        "connector_id": UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
        "identity_contract": (
            UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID
        ),
        "source_role": UHC_FLEX_PRACTITIONER_SOURCE_ROLE,
    }


UHC_FLEX_PRACTITIONER_SOURCE_ID = _derived_identity(
    "pdfhir_",
    uhc_flex_practitioner_source_identity_payload(),
    24,
)
UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY = (
    "provider-directory-uhc-flex-practitioner-publication:"
    + UHC_FLEX_PRACTITIONER_SOURCE_ID
)


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerQueryContract:
    """Bind one dedicated source to one exact one-NPI query contract."""

    contract_id: str = UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID
    source_identity_contract_id: str = (
        UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID
    )
    source_id: str = UHC_FLEX_PRACTITIONER_SOURCE_ID
    connector_id: str = UHC_FLEX_PRACTITIONER_CONNECTOR_ID
    authority_id: str = UHC_FLEX_OFFICIAL_AUTHORITY_ID
    source_role: str = UHC_FLEX_PRACTITIONER_SOURCE_ROLE
    canonical_api_base: str = UHC_FLEX_PRACTITIONER_API_BASE
    transport: str = UHC_FLEX_PRACTITIONER_TRANSPORT
    resource_type: str = UHC_FLEX_OFFICIAL_RESOURCE_TYPE
    search_parameter: str = UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER
    identifier_system: str = UHC_FLEX_OFFICIAL_NPI_SYSTEM
    cohort_scope: str = UHC_FLEX_PRACTITIONER_COHORT_SCOPE
    query_values_per_request: int = (
        UHC_FLEX_PRACTITIONER_QUERY_VALUES_PER_REQUEST
    )
    query_count: int = UHC_FLEX_PRACTITIONER_QUERY_COUNT
    pagination: str = UHC_FLEX_PRACTITIONER_PAGINATION
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        expected_by_field = {
            "authority_id": UHC_FLEX_OFFICIAL_AUTHORITY_ID,
            "canonical_api_base": UHC_FLEX_PRACTITIONER_API_BASE,
            "cohort_scope": UHC_FLEX_PRACTITIONER_COHORT_SCOPE,
            "connector_id": UHC_FLEX_PRACTITIONER_CONNECTOR_ID,
            "contract_id": UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
            "endpoint_collection_complete": False,
            "endpoint_complete": False,
            "identifier_system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
            "pagination": UHC_FLEX_PRACTITIONER_PAGINATION,
            "query_count": UHC_FLEX_PRACTITIONER_QUERY_COUNT,
            "query_values_per_request": (
                UHC_FLEX_PRACTITIONER_QUERY_VALUES_PER_REQUEST
            ),
            "resource_type": UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
            "search_parameter": UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER,
            "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
            "source_identity_contract_id": (
                UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID
            ),
            "source_role": UHC_FLEX_PRACTITIONER_SOURCE_ROLE,
            "transport": UHC_FLEX_PRACTITIONER_TRANSPORT,
        }
        if any(
            getattr(self, field_name) != expected_value
            for field_name, expected_value in expected_by_field.items()
        ):
            raise UHCFlexPractitionerContractError(
                "UHC Flex Practitioner query contract is inconsistent"
            )

    def endpoint_signature(self) -> dict[str, Any]:
        """Return a fresh signature that stays distinct from the generic probe."""

        return {
            "connector_acquisition_contract": {
                "connector_id": self.connector_id,
                **uhc_flex_practitioner_connector_identity_payload(),
            }
        }


UHC_FLEX_PRACTITIONER_QUERY_CONTRACT = UHCFlexPractitionerQueryContract()


__all__ = (
    "UHCFlexPractitionerContractError",
    "UHCFlexPractitionerQueryContract",
    "UHC_FLEX_PRACTITIONER_API_BASE",
    "UHC_FLEX_PRACTITIONER_COHORT_SCOPE",
    "UHC_FLEX_PRACTITIONER_CONNECTOR_ID",
    "UHC_FLEX_PRACTITIONER_PAGINATION",
    "UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY",
    "UHC_FLEX_PRACTITIONER_QUERY_CONTRACT",
    "UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_QUERY_COUNT",
    "UHC_FLEX_PRACTITIONER_QUERY_VALUES_PER_REQUEST",
    "UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER",
    "UHC_FLEX_PRACTITIONER_SOURCE_ID",
    "UHC_FLEX_PRACTITIONER_SOURCE_IDENTITY_CONTRACT_ID",
    "UHC_FLEX_PRACTITIONER_SOURCE_ROLE",
    "UHC_FLEX_PRACTITIONER_TRANSPORT",
    "uhc_flex_practitioner_connector_identity_payload",
    "uhc_flex_practitioner_source_identity_payload",
)
