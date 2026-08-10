# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed identities and immutable witnesses for rooted-graph storage."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS,
    provider_directory_rooted_graph_contract_payload,
)
from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_QUERY_PATTERN,
    ROOTED_GRAPH_SCOPE_PATTERN,
    SHA256_PATTERN,
    ProviderDirectoryRootedGraphScope,
    canonical_fhir_resource_id,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
    ROOTED_GRAPH_QUERY_EXACT_SEARCH,
    ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
    ProviderDirectoryRootedGraphQuery,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_store_identity import (
    _canonical_json,
    _sha256_text,
    _strict_hash,
    _strict_identifier,
    _strict_text,
)

PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-acquisition.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-query.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ROLES = frozenset({"baseline", "candidate"})
PROVIDER_DIRECTORY_ROOTED_GRAPH_CLOSURE_SCOPES = frozenset({"root", "plan", "census"})
ACQUISITION_PATTERN = re.compile(r"pdrga_[0-9a-f]{48}\Z")
RUN_PATTERN = re.compile(r"pdrgr_[0-9a-f]{48}\Z")
INTENT_PATTERN = re.compile(r"pdrgi_[0-9a-f]{48}\Z")


class ProviderDirectoryRootedGraphStoreError(RuntimeError):
    """Expose bounded storage failures without endpoint or payload details."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "identity": "rooted graph acquisition identity is invalid",
            "lease_lost": "rooted graph work lease was lost",
            "state": "rooted graph acquisition state is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256 = _sha256_text(
    _canonical_json(
        {
            "contract_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_ID,
            "graph": provider_directory_rooted_graph_contract_payload(),
        }
    )
)


def _acquisition_id(
    scope: ProviderDirectoryRootedGraphScope,
    root_cohort_id: str,
    endpoint_signature_sha256: str,
    acquisition_role: str,
    run_id: str,
    dataset_intent_id: str,
) -> str:
    parts = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID,
        scope.scope_id,
        _strict_text(root_cohort_id, 128),
        _strict_hash(endpoint_signature_sha256),
        PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256,
        PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256,
        acquisition_role,
        _strict_identifier(run_id, RUN_PATTERN),
        _strict_identifier(dataset_intent_id, INTENT_PATTERN),
    )
    return "pdrga_" + _sha256_text("\x1f".join(parts))[:48]


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphAcquisitionIdentity:
    """Bind one resumable role to an exact published Practitioner root."""

    acquisition_id: str
    scope_id: str
    root_dataset_variant: str
    root_publication_contract_id: str
    root_source_id: str
    root_endpoint_id: str
    acquisition_source_id: str
    acquisition_endpoint_id: str
    source_authority_id: str
    root_dataset_id: str
    root_dataset_hash: str
    root_content_proof_sha256: str
    root_resource_count: int
    max_work_items: int
    max_resource_rows: int
    max_edge_rows: int
    max_payload_bytes: int
    root_cohort_id: str
    endpoint_signature_sha256: str
    acquisition_role: str
    run_id: str
    dataset_intent_id: str
    storage_contract_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
    connector_id: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
    graph_contract_sha256: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
    query_contract_sha256: str = PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256
    rooted_graph_complete: bool = False
    endpoint_collection_complete: bool = False
    endpoint_complete: bool = False

    def __post_init__(self) -> None:
        scope = ProviderDirectoryRootedGraphScope(
            scope_id=self.scope_id,
            root_dataset_variant=self.root_dataset_variant,
            root_publication_contract_id=self.root_publication_contract_id,
            root_source_id=self.root_source_id,
            root_endpoint_id=self.root_endpoint_id,
            acquisition_source_id=self.acquisition_source_id,
            acquisition_endpoint_id=self.acquisition_endpoint_id,
            source_authority_id=self.source_authority_id,
            root_dataset_id=self.root_dataset_id,
            root_dataset_hash=self.root_dataset_hash,
            root_content_proof_sha256=self.root_content_proof_sha256,
            root_resource_count=self.root_resource_count,
            max_work_items=self.max_work_items,
            max_resource_rows=self.max_resource_rows,
            max_edge_rows=self.max_edge_rows,
            max_payload_bytes=self.max_payload_bytes,
        )
        expected_id = _acquisition_id(
            scope,
            self.root_cohort_id,
            self.endpoint_signature_sha256,
            self.acquisition_role,
            self.run_id,
            self.dataset_intent_id,
        )
        if (
            self.acquisition_id != expected_id
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or self.acquisition_role
            not in PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ROLES
            or self.storage_contract_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID
            or self.connector_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONNECTOR_ID
            or self.acquisition_source_id != PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID
            or self.acquisition_endpoint_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
            or self.endpoint_signature_sha256
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
            or self.source_authority_id
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID
            or self.graph_contract_sha256
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256
            or self.query_contract_sha256
            != PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256
            or type(self.max_work_items) is not int
            or not 1 <= self.max_work_items <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_WORK_ITEMS
            or type(self.max_resource_rows) is not int
            or not 1 <= self.max_resource_rows <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_RESOURCE_ROWS
            or type(self.max_edge_rows) is not int
            or not 1 <= self.max_edge_rows <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_EDGE_ROWS
            or type(self.max_payload_bytes) is not int
            or not 1 <= self.max_payload_bytes <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_PAYLOAD_BYTES
            or self.rooted_graph_complete is not False
            or self.endpoint_collection_complete is not False
            or self.endpoint_complete is not False
        ):
            raise ValueError("provider_directory_rooted_graph_identity_invalid")


def build_rooted_graph_acquisition(
    scope: ProviderDirectoryRootedGraphScope,
    *,
    root_cohort_id: str,
    endpoint_signature_sha256: str,
    acquisition_role: str,
    run_id: str,
    dataset_intent_id: str,
) -> ProviderDirectoryRootedGraphAcquisitionIdentity:
    """Build one independently replayable baseline or candidate identity."""

    if type(scope) is not ProviderDirectoryRootedGraphScope:
        raise ValueError("provider_directory_rooted_graph_scope_invalid")
    acquisition_id = _acquisition_id(
        scope,
        root_cohort_id,
        endpoint_signature_sha256,
        acquisition_role,
        run_id,
        dataset_intent_id,
    )
    return ProviderDirectoryRootedGraphAcquisitionIdentity(
        acquisition_id=acquisition_id,
        scope_id=scope.scope_id,
        root_dataset_variant=scope.root_dataset_variant,
        root_publication_contract_id=scope.root_publication_contract_id,
        root_source_id=scope.root_source_id,
        root_endpoint_id=scope.root_endpoint_id,
        acquisition_source_id=scope.acquisition_source_id,
        acquisition_endpoint_id=scope.acquisition_endpoint_id,
        source_authority_id=scope.source_authority_id,
        root_dataset_id=scope.root_dataset_id,
        root_dataset_hash=scope.root_dataset_hash,
        root_content_proof_sha256=scope.root_content_proof_sha256,
        root_resource_count=scope.root_resource_count,
        max_work_items=scope.max_work_items,
        max_resource_rows=scope.max_resource_rows,
        max_edge_rows=scope.max_edge_rows,
        max_payload_bytes=scope.max_payload_bytes,
        root_cohort_id=root_cohort_id,
        endpoint_signature_sha256=endpoint_signature_sha256,
        acquisition_role=acquisition_role,
        run_id=run_id,
        dataset_intent_id=dataset_intent_id,
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphWorkSpec:
    """Persist one endpoint-neutral query and its immutable discovery proof."""

    query_id: str
    scope_id: str
    query_identity_sha256: str
    query_identity_json_text: str = field(repr=False)
    kind: str
    resource_type: str
    search_parameter: str | None
    reference_type: str | None
    reference_id: str | None = field(repr=False)
    closure_scope: str
    discovered_by_query_id: str | None = field(default=None, repr=False)
    discovered_source_type: str | None = None
    discovered_source_id: str | None = field(default=None, repr=False)
    discovered_edge_sha256: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        try:
            identity = json.loads(self.query_identity_json_text)
        except (TypeError, ValueError):
            raise ValueError("provider_directory_rooted_graph_work_invalid") from None
        reference = identity.get("reference") if type(identity) is dict else None
        expected_reference = (
            f"{self.reference_type}/{self.reference_id}"
            if self.reference_type is not None and self.reference_id is not None
            else None
        )
        if (
            ROOTED_GRAPH_QUERY_PATTERN.fullmatch(self.query_id) is None
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or self.query_identity_json_text != _canonical_json(identity)
            or self.query_identity_sha256 != _sha256_text(self.query_identity_json_text)
            or self.kind != identity.get("kind")
            or self.resource_type != identity.get("resource_type")
            or self.search_parameter != identity.get("search_parameter")
            or reference != expected_reference
            or self.closure_scope not in PROVIDER_DIRECTORY_ROOTED_GRAPH_CLOSURE_SCOPES
        ):
            raise ValueError("provider_directory_rooted_graph_work_invalid")
        self._validate_discovery()

    def _validate_discovery(self) -> None:
        is_initial = self.kind in {
            ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS,
        } or (
            self.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH
            and self.resource_type == "PractitionerRole"
        )
        discovery_values = (
            self.discovered_by_query_id,
            self.discovered_source_type,
            self.discovered_source_id,
            self.discovered_edge_sha256,
        )
        if is_initial:
            expected_scope = (
                "census"
                if self.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
                else "root"
            )
            if self.closure_scope != expected_scope or any(discovery_values):
                raise ValueError("provider_directory_rooted_graph_work_invalid")
            return
        if (
            self.closure_scope not in {"root", "plan"}
            or type(self.discovered_by_query_id) is not str
            or ROOTED_GRAPH_QUERY_PATTERN.fullmatch(self.discovered_by_query_id) is None
            or self.discovered_source_type
            not in {
                "PractitionerRole",
                "OrganizationAffiliation",
                "Organization",
                "Location",
                "HealthcareService",
                "InsurancePlan",
                "Endpoint",
            }
            or canonical_fhir_resource_id(self.discovered_source_id)
            != self.discovered_source_id
        ):
            raise ValueError("provider_directory_rooted_graph_work_invalid")
        if self.kind == ROOTED_GRAPH_QUERY_DIRECT_READ:
            if (
                type(self.discovered_edge_sha256) is not str
                or SHA256_PATTERN.fullmatch(self.discovered_edge_sha256) is None
            ):
                raise ValueError("provider_directory_rooted_graph_work_invalid")
        elif self.discovered_edge_sha256 is not None:
            raise ValueError("provider_directory_rooted_graph_work_invalid")


def build_rooted_graph_work_spec(
    scope_id: str,
    query: ProviderDirectoryRootedGraphQuery,
    *,
    closure_scope: str,
    discovered_by_query_id: str | None = None,
    discovered_source_type: str | None = None,
    discovered_source_id: str | None = None,
    discovered_edge_sha256: str | None = None,
) -> ProviderDirectoryRootedGraphWorkSpec:
    """Canonicalize one initial or discovered query for durable replay."""

    if type(query) is not ProviderDirectoryRootedGraphQuery:
        raise ValueError("provider_directory_rooted_graph_query_invalid")
    identity = query.identity_document()
    identity_json = _canonical_json(identity)
    reference = identity["reference"]
    reference_type, reference_id = (
        reference.split("/", 1) if type(reference) is str else (None, None)
    )
    return ProviderDirectoryRootedGraphWorkSpec(
        query_id=query.query_id(scope_id),
        scope_id=scope_id,
        query_identity_sha256=_sha256_text(identity_json),
        query_identity_json_text=identity_json,
        kind=query.kind,
        resource_type=query.resource_type,
        search_parameter=query.search_parameter,
        reference_type=reference_type,
        reference_id=reference_id,
        closure_scope=closure_scope,
        discovered_by_query_id=discovered_by_query_id,
        discovered_source_type=discovered_source_type,
        discovered_source_id=discovered_source_id,
        discovered_edge_sha256=discovered_edge_sha256,
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphWorkClaim:
    """Identify one exact query lease generation without exposing its URL."""

    acquisition_id: str
    scope_id: str
    query_id: str
    query_identity_sha256: str
    kind: str
    resource_type: str
    reference_type: str | None
    reference_id: str | None = field(repr=False)
    closure_scope: str
    attempt: int
    lease_token: str = field(repr=False)

    def __post_init__(self) -> None:
        try:
            reference_id = (
                canonical_fhir_resource_id(self.reference_id)
                if self.reference_id is not None
                else None
            )
        except ValueError:
            raise ValueError("provider_directory_rooted_graph_claim_invalid") from None
        is_exact_shape = self.kind == ROOTED_GRAPH_QUERY_EXACT_SEARCH and (
            (
                self.resource_type == "PractitionerRole"
                and self.reference_type == "Practitioner"
                and reference_id is not None
                and self.closure_scope == "root"
            )
            or (
                self.resource_type == "OrganizationAffiliation"
                and self.reference_type == "Organization"
                and reference_id is not None
                and self.closure_scope in {"root", "plan"}
            )
        )
        is_direct_shape = (
            self.kind == ROOTED_GRAPH_QUERY_DIRECT_READ
            and self.resource_type
            in {"Organization", "Location", "HealthcareService", "Endpoint"}
            and self.reference_type == self.resource_type
            and reference_id is not None
            and self.closure_scope in {"root", "plan"}
        )
        is_census_shape = (
            self.kind == ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
            and self.resource_type == "InsurancePlan"
            and self.reference_type is None
            and self.reference_id is None
            and self.closure_scope == "census"
        )
        if (
            ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or ROOTED_GRAPH_SCOPE_PATTERN.fullmatch(self.scope_id) is None
            or ROOTED_GRAPH_QUERY_PATTERN.fullmatch(self.query_id) is None
            or SHA256_PATTERN.fullmatch(self.query_identity_sha256) is None
            or not (is_exact_shape or is_direct_shape or is_census_shape)
            or (not is_census_shape and reference_id != self.reference_id)
            or type(self.attempt) is not int
            or self.attempt < 1
            or SHA256_PATTERN.fullmatch(self.lease_token) is None
        ):
            raise ValueError("provider_directory_rooted_graph_claim_invalid")


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphCensusClaim:
    """Bind the dedicated plan census lease to DB-derived root network anchors."""

    work_claim: ProviderDirectoryRootedGraphWorkClaim
    root_network_references: tuple[str, ...]

    def __post_init__(self) -> None:
        try:
            canonical_references = tuple(
                "Organization/" + canonical_fhir_resource_id(reference.split("/", 1)[1])
                for reference in self.root_network_references
                if reference.startswith("Organization/")
            )
        except (AttributeError, IndexError, TypeError, ValueError):
            raise ValueError(
                "provider_directory_rooted_graph_census_claim_invalid"
            ) from None
        if (
            type(self.work_claim) is not ProviderDirectoryRootedGraphWorkClaim
            or self.work_claim.kind != ROOTED_GRAPH_QUERY_INSURANCE_PLAN_CENSUS
            or type(self.root_network_references) is not tuple
            or len(canonical_references) != len(self.root_network_references)
            or canonical_references != self.root_network_references
            or tuple(sorted(set(canonical_references))) != canonical_references
        ):
            raise ValueError("provider_directory_rooted_graph_census_claim_invalid")


build_provider_directory_rooted_graph_acquisition_identity = (
    build_rooted_graph_acquisition
)
build_provider_directory_rooted_graph_work_spec = build_rooted_graph_work_spec


__all__ = (
    "build_provider_directory_rooted_graph_acquisition_identity",
    "build_provider_directory_rooted_graph_work_spec",
    "build_rooted_graph_acquisition",
    "build_rooted_graph_work_spec",
    "ProviderDirectoryRootedGraphAcquisitionIdentity",
    "ProviderDirectoryRootedGraphCensusClaim",
    "ProviderDirectoryRootedGraphStoreError",
    "ProviderDirectoryRootedGraphWorkClaim",
    "ProviderDirectoryRootedGraphWorkSpec",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ROLES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_CONTRACT_SHA256",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_QUERY_CONTRACT_SHA256",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_STORAGE_CONTRACT_ID",
)
