# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed, source-neutral contract for rooted Provider Directory graphs."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any


PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-identity.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE = 100
PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_VALUES_PER_REQUEST = 1
PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION = "same-origin-source-issued-until-terminal"
PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_PAGINATION = "forbidden"
PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION = (
    "full-finite-census-local-network-intersection"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NETWORK_QUERY = "forbidden"
PROVIDER_DIRECTORY_ROOTED_GRAPH_COMPLETION_SCOPE = "rooted-reference-closure"
PROVIDER_DIRECTORY_ROOTED_GRAPH_ROLE_EXPANSION = "once-per-root-practitioner"
PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_EXPANSION = (
    "reachable-participating-organization-fixed-point"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_EXPANSION = "deduplicated-reference-closure"
PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_INITIALIZATION = (
    "set-wise-sql-canonical-query-identity"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_DERIVED_REGISTRATION = (
    "same-transaction-as-terminal-witness"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_CENSUS_ADMISSION = (
    "database-proven-root-reference-fixed-point"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_MISSING_HTTP_STATUSES = (404, 410)
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES = 20 * 1024 * 1024
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES = 64 * 1024 * 1024
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES = 4096
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES = 100_000
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES = 8192
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES = 1 << 20
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES = 64 * 1024
PROVIDER_DIRECTORY_ROOTED_GRAPH_RESPONSE_VALIDATION_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-response-validation.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_MISSING_OUTCOME_ISSUE_SHAPES = (
    (
        404,
        (
            (("error", "not-found"),),
            (("error", "processing"), ("information", "informational")),
        ),
    ),
    (
        410,
        (
            (("error", "deleted"),),
            (("error", "processing"), ("information", "informational")),
        ),
    ),
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS = 16_500_000
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS = 25_000_000
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS = 100_000_000
PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES = 274_877_906_944
PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_VARIANT = "uhc_flex_practitioner"
PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_VARIANT = "rooted_combined"
PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID = (
    "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-publication.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_PUBLICATION_BY_VARIANT = {
    PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_VARIANT: (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_LEGACY_ROOT_PUBLICATION_CONTRACT_ID
    ),
    PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_VARIANT: (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_COMBINED_ROOT_PUBLICATION_CONTRACT_ID
    ),
}
PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_DEPTH = 6
PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_NODES = 4096
PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS = (
    (
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/network-reference"
    ),
    (
        "https://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/network-reference"
    ),
    (
        "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
        "StructureDefinition/plannet-ParticipatingNetwork-extension"
    ),
)

PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES = (
    "PractitionerRole",
    "OrganizationAffiliation",
    "Organization",
    "Location",
    "HealthcareService",
    "InsurancePlan",
    "Endpoint",
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES = (
    "Organization",
    "Location",
    "HealthcareService",
    "Endpoint",
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT = (
    (
        "PractitionerRole",
        (
            ("practitioner", "single", "Practitioner"),
            ("organization", "single", "Organization"),
            ("network", "repeated", "Organization"),
            ("location", "repeated", "Location"),
            ("healthcareService", "repeated", "HealthcareService"),
            ("endpoint", "repeated", "Endpoint"),
        ),
    ),
    (
        "OrganizationAffiliation",
        (
            ("organization", "single", "Organization"),
            ("participatingOrganization", "single", "Organization"),
            ("network", "repeated", "Organization"),
            ("location", "repeated", "Location"),
            ("healthcareService", "repeated", "HealthcareService"),
            ("endpoint", "repeated", "Endpoint"),
        ),
    ),
    (
        "Organization",
        (
            ("partOf", "single", "Organization"),
            ("endpoint", "repeated", "Endpoint"),
        ),
    ),
    (
        "Location",
        (
            ("managingOrganization", "single", "Organization"),
            ("partOf", "single", "Location"),
            ("endpoint", "repeated", "Endpoint"),
        ),
    ),
    (
        "HealthcareService",
        (
            ("providedBy", "single", "Organization"),
            ("location", "repeated", "Location"),
            ("coverageArea", "repeated", "Location"),
            ("endpoint", "repeated", "Endpoint"),
        ),
    ),
    (
        "InsurancePlan",
        (
            ("ownedBy", "single", "Organization"),
            ("administeredBy", "single", "Organization"),
            ("coverageArea", "repeated", "Location"),
            ("network", "repeated", "Organization"),
        ),
    ),
    (
        "Endpoint",
        (("managingOrganization", "single", "Organization"),),
    ),
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_NETWORK_EXTENSION_ALLOWED_FIELDS = (
    "id",
    "url",
    "valueReference",
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_UNKNOWN_EXTENSION_REFERENCE_POLICY = (
    "reject-recursive-reference-shaped-values-and-enforce-ext-1"
)


class ProviderDirectoryRootedGraphContractError(ValueError):
    """Reject drift from the reviewed rooted-graph acquisition contract."""


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphExactSearch:
    """Describe one allowed exact reference search in the rooted graph."""

    resource_type: str
    search_parameter: str
    reference_type: str
    expansion: str
    query_values_per_request: int = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_VALUES_PER_REQUEST
    )
    page_size: int = PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE
    pagination: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION

    def __post_init__(self) -> None:
        allowed_search_shapes = {
            (
                "PractitionerRole",
                "practitioner",
                "Practitioner",
                PROVIDER_DIRECTORY_ROOTED_GRAPH_ROLE_EXPANSION,
            ),
            (
                "OrganizationAffiliation",
                "participating-organization",
                "Organization",
                PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_EXPANSION,
            ),
        }
        if (
            (
                self.resource_type,
                self.search_parameter,
                self.reference_type,
                self.expansion,
            )
            not in allowed_search_shapes
            or self.query_values_per_request
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_VALUES_PER_REQUEST
            or self.page_size != PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE
            or self.pagination != PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION
        ):
            raise ProviderDirectoryRootedGraphContractError(
                "provider_directory_rooted_graph_exact_search_invalid"
            )

    def document(self) -> dict[str, Any]:
        """Return a fresh, JSON-ready contract document."""

        return {
            "expansion": self.expansion,
            "pagination": self.pagination,
            "page_size": self.page_size,
            "query_values_per_request": self.query_values_per_request,
            "reference_type": self.reference_type,
            "resource_type": self.resource_type,
            "search_parameter": self.search_parameter,
        }


PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES = (
    ProviderDirectoryRootedGraphExactSearch(
        resource_type="PractitionerRole",
        search_parameter="practitioner",
        reference_type="Practitioner",
        expansion=PROVIDER_DIRECTORY_ROOTED_GRAPH_ROLE_EXPANSION,
    ),
    ProviderDirectoryRootedGraphExactSearch(
        resource_type="OrganizationAffiliation",
        search_parameter="participating-organization",
        reference_type="Organization",
        expansion=PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_EXPANSION,
    ),
)


def _reference_field_payload() -> list[dict[str, Any]]:
    return [
        {
            "fields": [
                {
                    "cardinality": cardinality,
                    "field_name": field_name,
                    "target_type": target_type,
                }
                for field_name, cardinality, target_type in field_contracts
            ],
            "resource_type": resource_type,
        }
        for resource_type, field_contracts in (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_REFERENCE_FIELD_CONTRACT
        )
    ]


def _response_validation_payload() -> dict[str, Any]:
    return {
        "bundle_entries": "searchset-match-only-no-includes-or-duplicates",
        "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_RESPONSE_VALIDATION_CONTRACT_ID,
        "continuation": (
            "effective-https-origin-exact-collection-path-opaque-query-"
            "normalized-cycle-reject-credentials-fragment-traversal"
        ),
        "direct_response": "exact-resource-type-and-id-no-total",
        "hard_caps": {
            "max_missing_response_bytes": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES
            ),
            "max_page_bytes": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGE_BYTES,
            "max_pages": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAGES,
            "max_query_bytes": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_QUERY_BYTES,
            "max_resource_json_bytes": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_JSON_BYTES
            ),
            "max_resources": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCES,
            "max_url_bytes": PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_URL_BYTES,
        },
        "json": {
            "duplicate_keys": "reject",
            "encoding": "strict-utf-8",
            "nonfinite_numbers": "reject",
            "number_semantics": "binary64-exact-lexical-roundtrip-or-reject",
        },
        "media": "application-fhir-json-identity-encoding-no-redirects",
        "missing": {
            "issue_shapes_by_status": [
                {
                    "issue_shapes": [
                        [
                            {"code": code, "severity": severity}
                            for severity, code in issue_shape
                        ]
                        for issue_shape in issue_shapes
                    ],
                    "status": status,
                }
                for status, issue_shapes in (
                    PROVIDER_DIRECTORY_ROOTED_GRAPH_MISSING_OUTCOME_ISSUE_SHAPES
                )
            ],
            "payload": "strict-json-operation-outcome-retained-hash-and-bytes",
        },
        "search_total": (
            "optional-exact-but-all-pages-stable-and-final-count-equal;"
            "required-stable-census-and-final-count-equal"
        ),
    }


def provider_directory_rooted_graph_contract_payload() -> dict[str, Any]:
    """Return the static acquisition identity without endpoint or root IDs."""

    return {
        "completion": {
            "endpoint_collection_complete": False,
            "endpoint_complete": False,
            "rooted_graph_complete": True,
            "scope": PROVIDER_DIRECTORY_ROOTED_GRAPH_COMPLETION_SCOPE,
        },
        "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID,
        "direct_reads": {
            "expansion": PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_EXPANSION,
            "missing_http_statuses": list(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_MISSING_HTTP_STATUSES
            ),
            "pagination": (PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_PAGINATION),
            "resource_types": list(PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES),
        },
        "exact_searches": [
            search.document()
            for search in PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES
        ],
        "insurance_plan": {
            "admission": PROVIDER_DIRECTORY_ROOTED_GRAPH_CENSUS_ADMISSION,
            "network_query": (PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NETWORK_QUERY),
            "page_size": PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_PAGE_SIZE,
            "pagination": PROVIDER_DIRECTORY_ROOTED_GRAPH_PAGINATION,
            "selection": PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION,
        },
        "network_references": {
            "extension_max_depth": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_DEPTH
            ),
            "extension_max_nodes": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_EXTENSION_MAX_NODES
            ),
            "practitioner_role_extension_urls": list(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS
            ),
            "practitioner_role_top_level_field": "network",
            "reference_type": "Organization",
            "reviewed_node_allowed_fields": list(
                PROVIDER_DIRECTORY_ROOTED_GRAPH_NETWORK_EXTENSION_ALLOWED_FIELDS
            ),
            "unknown_extension_policy": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_UNKNOWN_EXTENSION_REFERENCE_POLICY
            ),
        },
        "persistence": {
            "derived_registration": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_DERIVED_REGISTRATION
            ),
            "root_initialization": PROVIDER_DIRECTORY_ROOTED_GRAPH_ROOT_INITIALIZATION,
        },
        "resource_types": list(PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES),
        "reference_fields": _reference_field_payload(),
        "response_validation": _response_validation_payload(),
    }


def _contract_sha256() -> str:
    canonical_payload = json.dumps(
        provider_directory_rooted_graph_contract_payload(),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256 = _contract_sha256()


def _contract_id() -> str:
    return "pdrgc_" + PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256[:48]


PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID = _contract_id()


@dataclass(frozen=True, slots=True)
class ProviderDirectoryRootedGraphContract:
    """Freeze graph families, query shapes, and bounded completion claims."""

    contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
    identity_contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID
    connector_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    resource_types: tuple[str, ...] = PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
    direct_read_types: tuple[str, ...] = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES
    )
    exact_searches: tuple[ProviderDirectoryRootedGraphExactSearch, ...] = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES
    )
    insurance_plan_selection: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION
    insurance_plan_network_query: str = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NETWORK_QUERY
    )
    completion_scope: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_COMPLETION_SCOPE
    rooted_graph_complete: bool = True
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        if (
            self.contract_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_ID
            or self.identity_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_IDENTITY_CONTRACT_ID
            or self.connector_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
            or self.resource_types != PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_TYPES
            or self.direct_read_types
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_READ_TYPES
            or self.exact_searches != PROVIDER_DIRECTORY_ROOTED_GRAPH_EXACT_SEARCHES
            or self.insurance_plan_selection
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_SELECTION
            or self.insurance_plan_network_query
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NETWORK_QUERY
            or self.completion_scope != PROVIDER_DIRECTORY_ROOTED_GRAPH_COMPLETION_SCOPE
            or self.rooted_graph_complete is not True
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ProviderDirectoryRootedGraphContractError(
                "provider_directory_rooted_graph_contract_inconsistent"
            )

    def endpoint_signature(self) -> dict[str, Any]:
        """Return a shallow signature committing the full semantic contract."""

        return {
            "connector_acquisition_contract": {
                "connector_id": self.connector_id,
                "contract_id": self.contract_id,
                "graph_contract_sha256": (
                    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
                ),
                "identity_contract_id": self.identity_contract_id,
                "response_validation_contract_id": (
                    PROVIDER_DIRECTORY_ROOTED_GRAPH_RESPONSE_VALIDATION_CONTRACT_ID
                ),
            }
        }


PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT = ProviderDirectoryRootedGraphContract()
