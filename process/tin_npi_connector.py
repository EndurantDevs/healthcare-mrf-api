# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public compatibility facade for the token-only TIN-to-NPI connector."""

from process.tin_npi_connector_build import build_compact_tin_npi_generation
from process.tin_npi_connector_adapters import (
    extract_normalized_fhir_organization_tin_npi_evidence,
    extract_normalized_fhir_organization_tin_npi_evidence_for_policies,
)
from process.tin_npi_connector_evidence import (
    FhirOrganizationEvidenceResult,
    FhirTinNpiEvidence,
    canonical_fhir_organization_identity_sha256,
    canonical_provider_directory_payload_hash,
)
from process.tin_npi_connector_extract import (
    extract_fhir_organization_tin_npi_evidence,
    extract_fhir_organization_tin_npi_evidence_for_policies,
)
from process.tin_npi_connector_generation import (
    CompactTinNpiGeneration,
    assert_generation_reuse_compatible,
)
from process.tin_npi_connector_lookup import (
    NpiTinLookupReference,
    NpiTinLookupRow,
    TinNpiLookupRow,
    _generation_id,
    _lookup_digest,
)
from process.tin_npi_connector_policy import (
    DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
    FhirTinNpiIdentifierPolicy,
    FhirTinNpiIdentifierRule,
)
from process.tin_npi_connector_scan import (
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    canonical_fhir_evidence_set_digest,
    canonical_fhir_organization_scan_proof_digest,
    canonical_fhir_organization_scan_proof_json,
)
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    TinTokenPolicyDescriptor,
    TinTokenProjector,
    canonical_token_policy_id,
    load_tin_token_policy,
    normalize_ein,
    token_policy_descriptor_sha256,
)
from process.tin_npi_connector_source import (
    ConnectorRelationIdentity,
    FhirDatasetFenceIdentity,
    TinNpiConnectorSourceVector,
    canonical_source_ordinal_map_digest,
    canonical_source_ordinal_map_json,
)
from process.tin_npi_connector_support import (
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
    FHIR_TIN_NPI_IDENTIFIER_POLICY_ID,
    TIN_NPI_FHIR_INPUT_RELATION,
    TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID,
    TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    TIN_NPI_LOOKUP_CONTRACT_ID,
    TIN_NPI_LOOKUP_SCHEMA_VERSION,
    TIN_NPI_PROJECTION_POLICY_ID,
    TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
    TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID,
    TIN_NPI_SOURCE_SCOPE_CONTRACT_ID,
    TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
    TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID,
    TIN_TOKEN_FULL_HMAC_CONTRACT_ID,
    TIN_TOKEN_HMAC_CONTRACT_ID,
    TIN_TOKEN_ID_128_CONTRACT_ID,
    TIN_TOKEN_MESSAGE_DOMAIN,
    TIN_TOKEN_MESSAGE_FORMAT_ID,
    TIN_TOKEN_NORMALIZATION_CONTRACT_ID,
    TIN_TOKEN_POLICY_DESCRIPTOR_DOMAIN,
    TIN_TOKEN_POLICY_ID_MAX_BYTES,
    TIN_TOKEN_POLICY_PREFIX,
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
)
from process.tin_npi_connector_temporal import (
    canonical_evidence_as_of,
)


__all__ = [
    "DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY",
    "CompactTinNpiGeneration",
    "ConnectorRelationIdentity",
    "FHIR_SAME_ORGANIZATION_RELATIONSHIP",
    "FHIR_ORGANIZATION_SCAN_TERMINAL_STATES",
    "FHIR_TIN_NPI_IDENTIFIER_POLICY_ID",
    "FhirDatasetFenceIdentity",
    "FhirOrganizationEvidenceResult",
    "FhirOrganizationEvidenceState",
    "FhirOrganizationScanProof",
    "FhirOrganizationScanRecord",
    "FhirTinNpiEvidence",
    "FhirTinNpiIdentifierPolicy",
    "FhirTinNpiIdentifierRule",
    "NpiTinLookupReference",
    "NpiTinLookupRow",
    "TIN_NPI_LOOKUP_SCHEMA_VERSION",
    "TIN_NPI_LOOKUP_CONTRACT_ID",
    "TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID",
    "TIN_NPI_FHIR_INPUT_RELATION",
    "TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID",
    "TIN_NPI_PROJECTION_POLICY_ID",
    "TIN_NPI_SITE_RESOLUTION_CONTRACT_ID",
    "TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID",
    "TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION",
    "TIN_TOKEN_MESSAGE_DOMAIN",
    "TIN_TOKEN_MESSAGE_FORMAT_ID",
    "TIN_TOKEN_POLICY_ID_MAX_BYTES",
    "TIN_TOKEN_POLICY_PREFIX",
    "TinNpiConnectorError",
    "TinNpiConnectorSourceVector",
    "TinNpiLookupRow",
    "TinTaxIdentityToken",
    "TinTokenProjector",
    "assert_generation_reuse_compatible",
    "build_compact_tin_npi_generation",
    "canonical_evidence_as_of",
    "canonical_fhir_organization_identity_sha256",
    "canonical_fhir_evidence_set_digest",
    "canonical_fhir_organization_scan_proof_digest",
    "canonical_fhir_organization_scan_proof_json",
    "canonical_provider_directory_payload_hash",
    "canonical_source_ordinal_map_digest",
    "canonical_source_ordinal_map_json",
    "canonical_token_policy_id",
    "extract_fhir_organization_tin_npi_evidence",
    "extract_fhir_organization_tin_npi_evidence_for_policies",
    "extract_normalized_fhir_organization_tin_npi_evidence",
    "extract_normalized_fhir_organization_tin_npi_evidence_for_policies",
    "load_tin_token_policy",
    "normalize_ein",
]
